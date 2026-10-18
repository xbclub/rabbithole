// Copyright (c) 2018-2019 Parity Technologies (UK) Ltd.
//
// Licensed under the Apache License, Version 2.0 or MIT license, at your option.
//
// A copy of the Apache License, Version 2.0 is included in the software as
// LICENSE-APACHE and a copy of the MIT license is included in the software as
// LICENSE-MIT. You may also obtain a copy of the Apache License at
// https://www.apache.org/licenses/LICENSE-2.0
// or https://opensource.org/licenses/MIT.

//! 流级分片(Round-5,规格 benchmark/runs/tcp-multiflow-frp-20261015/
//! coding-spec-round5-stream-sharding.md §2):Framer→Worker 有界轻量通道
//! 与分片 Worker。
//!
//! halves 接收侧数据面按 `sid % N` 拆到 N 个 Worker:流级解包/派发/
//! 流控记账/唤醒/FIN/RST 全部在各 Worker 内闭环(D1:同流必同 Worker,
//! 流内顺序免协调)。Worker 是裸 Future(futures-only 红线),由
//! rh-transport 侧 tokio::spawn 物理并行。
//!
//! 本模块是 halves 的子模块,可直接触达 [`super::ConnShared`] 的私有字段。

use crate::connection::{Id, Mode, State, StreamCommand, STREAM_COMMAND_CAP};
use crate::frame::header::{self, Header, StreamId, Tag};
use crate::frame::Frame;
use crate::tagged_stream::TaggedStream;
use crate::{Config, ConnectionError, Stream, DEFAULT_CREDIT};
use futures::channel::mpsc;
use futures::stream::{SelectAll, StreamExt};
use nohash_hasher::IntMap;
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use super::{make_shadow, ConnShared};

/// Framer → Worker 的入站消息(§2.1,D2:经有界轻量通道)。
pub(super) enum ShardMsg {
    /// 无 SYN 的 Data/WindowUpdate/Ping(sid≠0)帧,原样转发。
    Frame(Frame<()>),
    /// 带 SYN 的 Data|SYN / WU|SYN:由 Worker 执行建流全流程。
    Syn(Frame<()>),
}

/// SendHalf → Worker 的冷路径控制(仅流删除,§2.5-D ②路径)。
pub(super) enum ShardCtrl {
    Remove(StreamId),
}

/// Framer/SendHalf 的路由句柄:每 Worker 一对(帧通道 + ctrl 通道)生产者。
pub(super) type ShardRoute = (ShardProducer<ShardMsg>, ShardProducer<ShardCtrl>);

/// 分片通道容量(D2:cap 128;临界区 ~50ns,当前帧率下无争用问题)。
pub(super) const SHARD_CHANNEL_CAP: usize = 128;

/// SendWorker → FlushTask 帧通道容量(round-6 §2.1:每 Worker 一条
/// `ShardChannel<Frame<()>>`,cap 128 帧;帧体 ≤ 单帧最大 body,内存上界
/// 有界)。满即停:SendWorker 停止消费命令(pending_frame 单槽滞留),
/// FlushTask 写穿后逐帧让位唤醒。
pub(super) const SEND_OUT_CAP: usize = 128;

/// SendWorker 单轮 poll 的命令消费预算(round-6 §2.3:防单 Worker 独占
/// 线程;命令已在通道内,break 后下轮继续,不丢)。
const SEND_CMDS_PER_POLL: usize = 64;

/// Worker→Framer 建流交付通道容量(D6:accept 背压链的一环)。
pub(super) const NEW_STREAMS_CAP: usize = 16;

/// 通道共享状态:有界队列 + 双 waker 槽 + 消费者存活标志 + 出队水位。
struct ChannelShared<T> {
    queue: Mutex<VecDeque<T>>,
    cap: usize,
    producer_waker: Mutex<Option<Waker>>,
    consumer_waker: Mutex<Option<Waker>>,
    consumer_gone: AtomicBool,
    /// 累计出队条数(单调;消费侧 Release 递增、生产侧 Acquire 读取)。
    /// SPSC 下生产者第 n 次成功 push 的条目恰在 `popped ≥ n` 时已出队,
    /// 生产侧据此做"某条目已出队"的精确判定,不必观察队列是否为空——
    /// send_out 的 ctrl Remove 顺序门控以此替代"整队取空"(复核修复:
    /// FlushTask 瓶颈态下队列几乎永不为空,整队门控会无限期饥饿 Remove)。
    popped: AtomicU64,
}

/// 有界轻量通道(§2.1,D2):`parking_lot::Mutex<VecDeque<T>>` + 双 waker
/// 槽,单生产者/单消费者(帧通道 Framer→Worker,ctrl 通道反向同理)。
///
/// **waker elision 契约(红线级)**:唤醒必须且只能发生在"队列从空变
/// 非空(唤醒消费者)"或"从满变非满(唤醒生产者)"且对端**已登记
/// waker** 的时刻;唤醒后 waker 槽即清空(take 语义),对端重新 poll 时
/// 重新登记。非空期间连续 push 不得重复唤醒。丢唤醒防线(消费/生产侧
/// park 前 register 之后必须复查队列状态)由调用方(Worker/Framer/
/// SendHalf 的 park 路径)遵守。
pub(super) struct ShardChannel<T> {
    shared: Arc<ChannelShared<T>>,
}

impl<T> ShardChannel<T> {
    pub(super) fn with_capacity(cap: usize) -> (ShardProducer<T>, ShardConsumer<T>) {
        let channel = ShardChannel {
            shared: Arc::new(ChannelShared {
                queue: Mutex::new(VecDeque::new()),
                cap,
                producer_waker: Mutex::new(None),
                consumer_waker: Mutex::new(None),
                consumer_gone: AtomicBool::new(false),
                popped: AtomicU64::new(0),
            }),
        };
        (
            ShardProducer {
                shared: channel.shared.clone(),
            },
            ShardConsumer {
                shared: channel.shared,
            },
        )
    }
}

/// 通道生产者句柄(克隆即共享同一队列)。
pub(super) struct ShardProducer<T> {
    shared: Arc<ChannelShared<T>>,
}

// 手写 Clone:derive 会给 T 加 Clone 约束,这里只克隆 Arc。
impl<T> Clone for ShardProducer<T> {
    fn clone(&self) -> Self {
        ShardProducer {
            shared: self.shared.clone(),
        }
    }
}

impl<T> ShardProducer<T> {
    /// 非阻塞尝试压入;满 → Err(T) 原样返还(调用方 park 重试)。
    pub(super) fn try_push(&self, msg: T) -> Result<(), T> {
        let mut queue = self.shared.queue.lock();
        if queue.len() >= self.shared.cap {
            return Err(msg);
        }
        let was_empty = queue.is_empty();
        queue.push_back(msg);
        drop(queue);
        if was_empty {
            // 空变非空:消费侧已登记才唤醒(take 语义;未登记说明消费侧
            // 尚未 park,其登记后的复查会看到非空,不丢唤醒)
            if let Some(w) = self.shared.consumer_waker.lock().take() {
                w.wake();
            }
        }
        Ok(())
    }

    /// park 点登记生产者 waker(消费者让出空间时唤醒;登记后调用方必须
    /// 复查队列非满,见 §2.1 防线)。
    pub(super) fn register_producer_waker(&self, w: &Waker) {
        *self.shared.producer_waker.lock() = Some(w.clone());
    }

    /// 消费者已 drop → true(生产者据此走 fatal,§2.6 防御路径)。
    pub(super) fn consumer_gone(&self) -> bool {
        self.shared.consumer_gone.load(Ordering::Acquire)
    }

    /// 消费侧累计出队条数(SeqCst,与消费侧 SeqCst 递增配对)。SPSC
    /// 下生产者第 n 次成功 `try_push` 的条目在 `popped_count() ≥ n` 时
    /// 必已出队——send_out 的 ctrl Remove 顺序门控据此做"本流末帧已
    /// 出队"的精确判定(见 [`SendWorker::flush_ctrl_pending`]),不再
    /// 要求整队取空。
    ///
    /// SeqCst 约束(round-6.1 复核修复):本读点(SendWorker park 复查)
    /// 与 ctrl_wait 门读(connection.rs next_frame)构成跨线程
    /// store-buffering 对——Release/Acquire 下"Worker 复查未见取空
    /// 推进 && FlushTask 门读未见置位"的双陈旧交织无任何同步边禁止,
    /// 抽象机允许(aarch64 等非 TSO 机器上真实可达),空闲连接的 Remove
    /// 将无限期滞留;四点(本读 / 取空 fetch_add / flag store / flag
    /// load)全 SeqCst 后,SC 全序下"双陈旧"成环被禁——要么复查已见
    /// 取空(水位追平,立即投递不 park),要么门读到 true(走 take+
    /// wake),防线可见性与机器无关。x86 上 load/fetch_add 与
    /// Acquire/Release 同指令,aarch64 上 ldar/stlr 同指令,零额外开销。
    pub(super) fn popped_count(&self) -> u64 {
        self.shared.popped.load(Ordering::SeqCst)
    }
}

/// 通道消费者句柄(Worker 侧;drop 即向生产者宣告消亡)。
pub(super) struct ShardConsumer<T> {
    shared: Arc<ChannelShared<T>>,
}

impl<T> ShardConsumer<T> {
    /// 批量弹出至多 min(队列长度, 接收缓冲空闲容量)条;从满变非满时
    /// 唤醒生产者(elision 契约的镜像)。返回实际弹出条数。
    fn drain_into(&self, buf: &mut VecDeque<T>) -> usize {
        let mut queue = self.shared.queue.lock();
        let was_full = queue.len() >= self.shared.cap;
        let room = buf.capacity().saturating_sub(buf.len());
        let n = queue.len().min(room);
        if n > 0 {
            buf.extend(queue.drain(..n));
        }
        let still = queue.len();
        drop(queue);
        if n > 0 {
            // 出队水位先于生产者唤醒推进:被唤醒的生产侧复查即见新水位。
            // SeqCst:与 popped_count 的 SC 对约束(见其文档)统一本计数
            // 器全部读写的序——send_out 消费路径(try_pop)是 SC 对的
            // 写侧,此处的 msg/ctrl 通道虽无人读 popped,同协议免混序。
            self.shared.popped.fetch_add(n as u64, Ordering::SeqCst);
        }
        if was_full && still < self.shared.cap {
            // 满变非满:生产侧已登记才唤醒(take 语义)
            if let Some(w) = self.shared.producer_waker.lock().take() {
                w.wake();
            }
        }
        n
    }

    /// 弹出单条;从满变非满时唤醒生产者(elision 契约镜像;round-6 §2.1)。
    /// FlushTask 的 next_frame 逐帧轮转取(不整段 drain_into——为 D3 严格
    /// 轮转公平:每取一帧即切换游标,任一 Worker 持续满队列时后面的
    /// Worker 每批仍分得份额)。
    pub(super) fn try_pop(&self) -> Option<T> {
        let mut queue = self.shared.queue.lock();
        let was_full = queue.len() >= self.shared.cap;
        let item = queue.pop_front();
        let still = queue.len();
        drop(queue);
        if item.is_some() {
            // 出队水位先于生产者唤醒推进(同 drain_into)。SeqCst:本路径
            // 是 send_out 取空通知门控 SC 对的写侧——FlushTask 取空
            // (本 fetch_add)与门读 ctrl_wait 之间须与 Worker 侧
            // (store ctrl_wait + 复查读 popped)共同服从 SC 全序,
            // 双陈旧(store-buffering)交织才被抽象机禁止(见
            // popped_count 文档)
            self.shared.popped.fetch_add(1, Ordering::SeqCst);
        }
        if was_full && still < self.shared.cap {
            // 满变非满:生产侧已登记才唤醒(take 语义)
            if let Some(w) = self.shared.producer_waker.lock().take() {
                w.wake();
            }
        }
        item
    }

    /// 空闲时登记消费者 waker(生产者压入时唤醒;登记后调用方必须复查
    /// 队列非空,见 §2.1 防线)。round-6 起 FlushTask(发送半)亦经
    /// send_out 通道消费,提升至 halves 可见。
    pub(super) fn register_consumer_waker(&self, w: &Waker) {
        *self.shared.consumer_waker.lock() = Some(w.clone());
    }

    /// 复查探测(丢唤醒防线):队列当前非空。FlushTask 的 park 复查用
    /// (round-6 §3.2,受 sink_idle 门控)。
    pub(super) fn has_pending(&self) -> bool {
        !self.shared.queue.lock().is_empty()
    }
}

impl<T> Drop for ShardConsumer<T> {
    fn drop(&mut self) {
        self.shared.consumer_gone.store(true, Ordering::Release);
    }
}

/// 分片 Worker:独占 `sid % N == index` 的流集合(§2.2)。
///
/// Future 语义:`Ready(())` 仅出现在共享错误/连接终结之后;`Pending` =
/// 两队列皆空且无待交付建流。Worker 内部致命(建流通道断)等价
/// set_error 路径(§2.6)。
pub struct ShardWorker {
    index: usize,
    /// 分片总数 N:opening 条目按 `sid % N == index` 归属本 Worker(收编权
    /// 分片所有,§2.5-C),终结/弃置排空只清本分片子集。
    shard_count: usize,
    shared: Arc<ConnShared>,
    mode: Mode,
    config: Arc<Config>,
    id: Id,
    /// 本分片流表(D1:同一 sid 恒路由本 Worker,本地表即该 sid 全集)。
    streams: IntMap<StreamId, Arc<Mutex<crate::connection::stream::Shared>>>,
    msg_rx: ShardConsumer<ShardMsg>,
    ctrl_rx: ShardConsumer<ShardCtrl>,
    /// 建流交付通道(Worker→RecvHalf,D6:cap 16)。
    new_streams: mpsc::Sender<Stream>,
    /// new_streams 满时的暂存(不得丢句柄)。
    pending_new: VecDeque<Stream>,
    msg_buf: VecDeque<ShardMsg>,
    ctrl_buf: VecDeque<ShardCtrl>,
    finished: bool,
    /// send_dropped 的一次性排空标记(§2.2:排空后继续服务,新 SYN 照常
    /// 建流,不因标志常驻反复清表)。
    send_drop_drained: bool,
}

impl ShardWorker {
    pub(super) fn new(
        index: usize,
        shard_count: usize,
        shared: Arc<ConnShared>,
        msg_rx: ShardConsumer<ShardMsg>,
        ctrl_rx: ShardConsumer<ShardCtrl>,
        new_streams: mpsc::Sender<Stream>,
    ) -> Self {
        let mode = shared.mode;
        let config = shared.config.clone();
        let id = shared.id;
        ShardWorker {
            index,
            shard_count,
            shared,
            mode,
            config,
            id,
            streams: IntMap::default(),
            msg_rx,
            ctrl_rx,
            new_streams,
            pending_new: VecDeque::new(),
            msg_buf: VecDeque::with_capacity(SHARD_CHANNEL_CAP),
            ctrl_buf: VecDeque::with_capacity(SHARD_CHANNEL_CAP),
            finished: false,
            send_drop_drained: false,
        }
    }

    /// opening 条目是否归属本分片(与 Framer 路由 `sid % N` 同式)。
    fn owns(&self, id: StreamId) -> bool {
        (id.val() as usize) % self.shard_count == self.index
    }

    /// 排空本地流表:逐流 Closed + 唤醒 reader/writer + 摘表(§2.2/§2.6)。
    /// D4 精确性:排空条数同步释放存活流计数——基线 SendHalf::drop →
    /// drop_all_streams 清全局表(len 随之归零),分片形态由各 Worker
    /// 自减;未确认出站流的计数经 update_state(Closed) 内的令牌递减。
    /// (复核修复:修前只 Closed 不减计数,存活期新 SYN 提前误触
    /// max_num_streams 上限发 GoAway internal_error。)
    fn drain_local_streams(&mut self) {
        let n = self.streams.len();
        for (id, s) in self.streams.drain() {
            close_stream(&self.shared, id, &s);
        }
        if n > 0 {
            self.shared.stream_count.fetch_sub(n, Ordering::SeqCst);
        }
    }

    /// 及时关闭面(复核修复,§2.6/§0.3-3):只关本地表流的**状态**并唤醒,
    /// **不摘表、不减计数**。错误一旦可见,读端立即观察到 Closed + 缓冲
    /// 数据(read_after_close),不依赖 RecvHalf 后续 poll/drop(基线
    /// SendHalf::fatal 同步关全表);表项保留,后到帧(pending_route 冲洗)
    /// 仍经命中落缓冲,直至终结排空摘表。幂等(Closed 上 update_state 无
    /// 迁移)。
    fn close_local_states(&self) {
        for (id, s) in self.streams.iter() {
            close_stream(&self.shared, *id, s);
        }
    }

    /// 及时关闭面之二:opening 本分片条目的状态关闭(不摘条目、不减
    /// 计数)——条目保留供后到的待收编帧收编(摘除会使其按 unknown
    /// stream 丢弃,即本轮复核发现 1)。
    fn close_opening_shard_states(&self) {
        let mine: Vec<(StreamId, Arc<Mutex<crate::connection::stream::Shared>>)> = {
            let opening = self.shared.opening.lock();
            opening
                .iter()
                .filter(|(id, _)| self.owns(**id))
                .map(|(id, s)| (*id, s.clone()))
                .collect()
        };
        for (id, s) in &mine {
            close_stream(&self.shared, *id, s);
        }
    }

    /// 终结/弃置排空的收尾:摘除 opening 本分片残留 + 计数回收。此刻
    /// 已路由帧全部处理完(有帧的条目已被收编进本地表,随本地表排空),
    /// 残留条目必无在途帧。只清本分片:跨分片清会与仍在终结排空的其他
    /// Worker 竞争收编权。
    fn drain_opening_shard(&mut self) {
        let drained: Vec<(StreamId, Arc<Mutex<crate::connection::stream::Shared>>)> = {
            let mut opening = self.shared.opening.lock();
            let ids: Vec<StreamId> = opening
                .keys()
                .copied()
                .filter(|id| self.owns(*id))
                .collect();
            ids.into_iter()
                .filter_map(|id| opening.remove(&id).map(|s| (id, s)))
                .collect()
        };
        if !drained.is_empty() {
            self.shared
                .stream_count
                .fetch_sub(drained.len(), Ordering::SeqCst);
        }
        for (id, s) in &drained {
            close_stream(&self.shared, *id, s);
        }
    }

    /// 本地/收编双查(§2.5-C):本地表命中即得;未命中且为本侧出站奇偶
    /// 时从 opening 收编(对端任何帧只可能在我们 SYN 上线后到达,遇
    /// 未知+本侧奇偶 sid 查 opening 即得)。
    fn lookup_stream(
        &mut self,
        id: StreamId,
    ) -> Option<Arc<Mutex<crate::connection::stream::Shared>>> {
        if let Some(s) = self.streams.get(&id) {
            return Some(s.clone());
        }
        if !self.is_local_outbound(id) {
            return None;
        }
        let s = self.shared.opening.lock().remove(&id)?;
        self.streams.insert(id, s.clone());
        Some(s)
    }

    fn is_local_outbound(&self, id: StreamId) -> bool {
        match self.mode {
            Mode::Client => id.is_client(),
            Mode::Server => id.is_server(),
        }
    }

    // Check if the given stream ID is valid w.r.t. the provided tag and our connection mode.
    fn is_valid_remote_id(&self, id: StreamId, tag: Tag) -> bool {
        if tag == Tag::Ping || tag == Tag::GoAway {
            return id.is_session();
        }
        match self.mode {
            Mode::Client => id.is_server(),
            Mode::Server => id.is_client(),
        }
    }

    fn make_new_inbound_stream(&self, id: StreamId, credit: u32) -> Stream {
        let (sender, receiver) = mpsc::channel(STREAM_COMMAND_CAP);
        self.shared.push_new_receiver(id, receiver);
        // round-9:halves 路径传入 ConnShared,纯 WU 帧走 to_send 快车道
        Stream::new_inbound(
            id,
            self.id,
            self.config.clone(),
            credit,
            sender,
            Some(self.shared.clone()),
        )
    }

    /// 建流计数回滚(§2.3 ②③④失败路径:未建成流不得占用全局上界)。
    fn rollback_stream_count(&self) {
        self.shared.stream_count.fetch_sub(1, Ordering::SeqCst);
    }

    fn push_goaway(&self, f: Frame<crate::frame::header::GoAway>) {
        // 现行 Terminate 语义:只发帧不断连(连接终结由对端 GoAway 响应/EOF 驱动)
        self.shared.push_to_send(f.into());
    }

    /// 冷路径:ctrl(流删除)排空 + 逐条 §2.5-D Worker 侧逻辑。
    fn drain_ctrl(&mut self) -> bool {
        self.ctrl_rx.drain_into(&mut self.ctrl_buf);
        if self.ctrl_buf.is_empty() {
            return false;
        }
        while let Some(ShardCtrl::Remove(sid)) = self.ctrl_buf.pop_front() {
            log::trace!("{}: removing dropped stream {}", self.id, sid);
            let Some(s) = self.streams.remove(&sid) else {
                // 健康状态下不可达(opening 未命中 ⟺ 已收编于本表);防御忽略
                log::trace!("{}: shard remove for unknown stream {}", self.id, sid);
                continue;
            };
            if let Some(frame) = finalize_dropped_stream(&self.shared, sid, &s) {
                self.shared.push_to_send(frame);
            }
        }
        true
    }

    /// 热路径:ring 排空(单轮 ≤ 通道容量,天然有界)+ 逐条 §2.3 处理。
    fn drain_msgs(&mut self) -> bool {
        self.msg_rx.drain_into(&mut self.msg_buf);
        if self.msg_buf.is_empty() {
            return false;
        }
        while let Some(msg) = self.msg_buf.pop_front() {
            self.process_msg(msg);
        }
        true
    }

    /// new_streams 交付重试(§2.2 步骤 3:try_send 成功一条清一条)。
    fn deliver_pending_new(&mut self) -> bool {
        let mut progress = false;
        while let Some(s) = self.pending_new.pop_front() {
            match self.new_streams.try_send(s) {
                Ok(()) => progress = true,
                Err(e) => {
                    let full = e.is_full();
                    let s = e.into_inner();
                    if full {
                        self.pending_new.push_front(s);
                    } else {
                        // 建流通道断(RecvHalf 已亡):等价内部致命(§2.6)。
                        // RecvHalf Drop 已置错误,此处兜底置位(首错生效)。
                        self.shared.set_error(ConnectionError::Closed);
                        progress = true;
                    }
                    break;
                }
            }
        }
        progress
    }

    fn process_msg(&mut self, msg: ShardMsg) {
        let is_syn = matches!(msg, ShardMsg::Syn(_));
        let frame = match msg {
            ShardMsg::Frame(f) => f,
            ShardMsg::Syn(f) => f,
        };
        log::trace!("{}: received: {}", self.id, frame.header());

        // ACK 头部处理(现行 on_frame 头部:Data/WU 共用、先 ACK 后 tag)。
        // 未确认计数的递减由流 Shared 的令牌在 update_state 内完成——
        // 离开 Open{ack:false} 恰递减一次(D4,与基线实时表扫描等价),
        // 此处只迁移状态 + 唤醒开流门控等待者。
        if frame.header().flags().contains(header::ACK)
            && matches!(frame.header().tag(), Tag::Data | Tag::WindowUpdate)
        {
            let id = frame.header().stream_id();
            if let Some(s) = self.lookup_stream(id) {
                s.lock()
                    .update_state(self.id, id, State::Open { acknowledged: true });
            }
            if let Some(waker) = self.shared.new_outbound_stream_waker.lock().take() {
                waker.wake();
            }
        }

        if is_syn {
            self.on_syn(frame);
        } else {
            match frame.header().tag() {
                Tag::Data => self.on_data(frame.into_data()),
                Tag::WindowUpdate => self.on_window_update(frame.into_window_update()),
                Tag::Ping => self.on_ping(frame.into_ping()),
                // GoAway 由 Framer 本地 fatal,不路由至此(防御丢弃)
                Tag::GoAway => {}
            }
        }
    }

    /// SYN 建流(§2.3 首行:Data|SYN / WU|SYN 建流全流程)。
    fn on_syn(&mut self, frame: Frame<()>) {
        match frame.header().tag() {
            Tag::Data => self.on_syn_data(frame.into_data()),
            Tag::WindowUpdate => self.on_syn_window_update(frame.into_window_update()),
            // Framer 只路由 Data|SYN / WU|SYN(防御丢弃)
            _ => {}
        }
    }

    fn on_syn_data(&mut self, frame: Frame<crate::frame::header::Data>) {
        let stream_id = frame.header().stream_id();
        let is_finish = frame.header().flags().contains(header::FIN); // half-close
        if !self.is_valid_remote_id(stream_id, Tag::Data) {
            log::error!("{}: invalid stream id {}", self.id, stream_id);
            self.push_goaway(Frame::protocol_error());
            return;
        }
        // 存在性检查只需本地表(D1:同一 sid 恒路由本 Worker)
        if self.streams.contains_key(&stream_id) {
            log::error!("{}/{}: stream already exists", self.id, stream_id);
            self.push_goaway(Frame::protocol_error());
            return;
        }
        // ② 全局计数器精确上界(跨分片精确,T-W4)
        if self.shared.stream_count.fetch_add(1, Ordering::SeqCst) + 1 > self.config.max_num_streams
        {
            self.rollback_stream_count();
            log::error!("{}: maximum number of streams reached", self.id);
            self.push_goaway(Frame::internal_error());
            return;
        }
        if frame.body().len() > DEFAULT_CREDIT as usize {
            self.rollback_stream_count();
            log::error!(
                "{}/{}: 1st body of stream exceeds default credit",
                self.id,
                stream_id
            );
            self.push_goaway(Frame::protocol_error());
            return;
        }
        let stream = self.make_new_inbound_stream(stream_id, DEFAULT_CREDIT);
        {
            let mut shared = stream.shared();
            if is_finish {
                shared.update_state(self.id, stream_id, State::RecvClosed);
            }
            if let Err(_err) = shared.consume_receive_window(frame.body_len()) {
                log::error!(
                    "{}/{}: 1st body of stream exceeds default credit",
                    self.id,
                    stream_id
                );
                drop(shared);
                // 未入表:回滚计数(现行路径此分支为防御性死代码,结构等价)
                self.rollback_stream_count();
                self.push_goaway(Frame::protocol_error());
                return;
            }
            shared.buffer.push(frame.into_body());
        }
        self.streams.insert(stream_id, stream.clone_shared());
        // ④ 帧句柄入 new_streams 待交付(不丢句柄)
        self.pending_new.push_back(stream);
    }

    fn on_syn_window_update(&mut self, frame: Frame<crate::frame::header::WindowUpdate>) {
        let stream_id = frame.header().stream_id();
        let is_finish = frame.header().flags().contains(header::FIN); // half-close
        if !self.is_valid_remote_id(stream_id, Tag::WindowUpdate) {
            log::error!("{}: invalid stream id {}", self.id, stream_id);
            self.push_goaway(Frame::protocol_error());
            return;
        }
        if self.streams.contains_key(&stream_id) {
            log::error!("{}/{}: stream already exists", self.id, stream_id);
            self.push_goaway(Frame::protocol_error());
            return;
        }
        if self.shared.stream_count.fetch_add(1, Ordering::SeqCst) + 1 > self.config.max_num_streams
        {
            self.rollback_stream_count();
            log::error!("{}: maximum number of streams reached", self.id);
            self.push_goaway(Frame::internal_error());
            return;
        }

        let Some(credit) = frame.header().credit().checked_add(DEFAULT_CREDIT) else {
            self.rollback_stream_count();
            log::error!("{}: header contains invalid credit", self.id);
            self.push_goaway(Frame::protocol_error());
            return;
        };
        let stream = self.make_new_inbound_stream(stream_id, credit);

        if is_finish {
            stream
                .shared()
                .update_state(self.id, stream_id, State::RecvClosed);
        }
        self.streams.insert(stream_id, stream.clone_shared());
        self.pending_new.push_back(stream);
    }

    /// 非 SYN Data 帧(§2.3 "Data(其余)" 行:与现行 on_data 非 SYN 分支
    /// 逐行为等价,操作对象换成本地表 + opening 收编)。
    fn on_data(&mut self, frame: Frame<crate::frame::header::Data>) {
        let stream_id = frame.header().stream_id();

        if frame.header().flags().contains(header::RST) {
            // stream reset
            if let Some(s) = self.lookup_stream(stream_id) {
                let mut shared = s.lock();
                shared.update_state(self.id, stream_id, State::Closed);
                if let Some(w) = shared.reader.take() {
                    w.wake()
                }
                if let Some(w) = shared.writer.take() {
                    w.wake()
                }
            }
            return;
        }

        let is_finish = frame.header().flags().contains(header::FIN); // half-close

        if let Some(s) = self.lookup_stream(stream_id) {
            let mut shared = s.lock();

            if let Err(_err) = shared.consume_receive_window(frame.body_len()) {
                log::error!(
                    "{}/{}: frame body larger than window of stream",
                    self.id,
                    stream_id
                );
                drop(shared);
                self.push_goaway(Frame::protocol_error());
                return;
            }

            if is_finish {
                shared.update_state(self.id, stream_id, State::RecvClosed);
            }

            shared.buffer.push(frame.into_body());
            if let Some(w) = shared.reader.take() {
                w.wake()
            }
        } else {
            log::trace!(
                "{}/{}: data frame for unknown stream, possibly dropped earlier: {:?}",
                self.id,
                stream_id,
                frame
            );
            // 与未拆分路径一致:不视为协议违规、不回 RST(仍可能有该流
            // 在途命令,见 paritytech/yamux#110)。
        }
    }

    /// 非 SYN WU 帧(§2.3 "WU" 行:与现行 on_window_update 非 SYN 分支
    /// 逐行为等价)。
    fn on_window_update(&mut self, frame: Frame<crate::frame::header::WindowUpdate>) {
        let stream_id = frame.header().stream_id();

        if frame.header().flags().contains(header::RST) {
            // stream reset
            if let Some(s) = self.lookup_stream(stream_id) {
                let mut shared = s.lock();
                shared.update_state(self.id, stream_id, State::Closed);
                if let Some(w) = shared.reader.take() {
                    w.wake()
                }
                if let Some(w) = shared.writer.take() {
                    w.wake()
                }
            }
            return;
        }

        let is_finish = frame.header().flags().contains(header::FIN); // half-close

        if let Some(s) = self.lookup_stream(stream_id) {
            let mut shared = s.lock();
            if let Err(err) = shared.increase_send_window_by(frame.header().credit()) {
                log::error!(
                    "{}/{}: could not increase the send window, {err}",
                    self.id,
                    stream_id
                );
                drop(shared);
                self.push_goaway(Frame::protocol_error());
                return;
            }
            if is_finish {
                shared.update_state(self.id, stream_id, State::RecvClosed);

                if let Some(w) = shared.reader.take() {
                    w.wake()
                }
            }
            if let Some(w) = shared.writer.take() {
                w.wake()
            }
        } else {
            log::trace!(
                "{}/{}: window update for unknown stream, possibly dropped earlier: {:?}",
                self.id,
                stream_id,
                frame
            );
            // 与未拆分路径一致:不视为协议违规、不回 RST(paritytech/yamux#110)。
        }
    }

    /// 流级 Ping(§2.3:Ping|ACK 由 Framer 本地处理,不路由至此)。
    fn on_ping(&mut self, frame: Frame<crate::frame::header::Ping>) {
        let stream_id = frame.header().stream_id();
        if frame.header().flags().contains(header::ACK) {
            return; // 防御:Framer 已本地 rtt.handle_pong
        }
        if self.lookup_stream(stream_id).is_some() {
            let mut hdr = Header::ping(frame.header().id());
            hdr.ack();
            self.shared.push_to_send(Frame::new(hdr).into());
        } else {
            log::debug!(
                "{}/{}: ping for unknown stream, possibly dropped earlier: {:?}",
                self.id,
                stream_id,
                frame
            );
            // 与未拆分路径一致:不视为协议违规、不回 RST(paritytech/yamux#110)。
        }
    }
}

impl Future for ShardWorker {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let this = &mut *self;
        loop {
            if this.finished {
                return Poll::Ready(());
            }
            // §2.4/§2.6 生命周期:共享错误(含其粘性影子 workers_shutdown,
            // 防 take_error 先行移除后 Worker 漏终结)→ 终结排空后 Ready
            // 终结。
            //
            // 终结排空(§0.3-2/§0.3-3,复核修复):见到错误标志**不得**
            // 立即丢消息终结——Framer 在 EOF/GoAway/对端错误之前已
            // try_push 进分片通道的 Data/FIN 是已读出的在途数据,必须
            // 全部处理落进流缓冲(read_after_close 契约;基线单任务逐帧
            // 同步处理完才见 EOF,分片后的等价语义)。此态排空不受
            // D6 背压门控约束(终结优先落地,建流句柄经暂存走交付)。
            if this.shared.workers_shutdown.load(Ordering::Acquire)
                || this.shared.error.lock().is_some()
            {
                this.drain_ctrl();
                this.drain_msgs();
                this.deliver_pending_new();
                // 及时关闭面(复核修复,§2.6):已路由帧处理完毕后立即关闭
                // 本地表与 opening 本分片流的**状态**(不摘表、不减计数)——
                // 读端此刻即得 Closed + 缓冲数据,不再依赖 RecvHalf 的后续
                // poll/drop(其可能阻塞在 accept 背压上);表项/条目保留,
                // 后到帧(pending_route 冲洗)仍能命中/收编落缓冲,数据不丢
                // (§0.3-2/§0.3-3)。
                this.close_local_states();
                this.close_opening_shard_states();
                // 终结条件:建流暂存全部交付(RecvHalf 终结排空持续消费
                // new_streams,容量恢复即经 poll_ready 唤醒本 Worker)
                // + Framer 路由终结宣告(此后不再有新消息入通道——防
                // "RecvHalf 冲洗 pending_route 的帧落入已终结 Worker"的
                // 竞态丢失)。等待期间流状态已 Closed,只延后摘表/计数。
                if this.pending_new.is_empty() && this.shared.framer_flushed.load(Ordering::Acquire)
                {
                    this.drain_local_streams();
                    this.drain_opening_shard();
                    this.finished = true;
                    // 建流通道就此关闭:接收侧排空余量后观察到 None,其
                    // 终结排空随之收敛——不依赖本 Future 被 drop(测试
                    // 夹具与生产 spawn 均成立)
                    this.new_streams.close_channel();
                    return Poll::Ready(());
                }
                // 终结条件未满足:park(丢唤醒防线:登记后复查)。复查项
                // 必须是"可做的工作":滞留消息可排空、容量恢复可交付、
                // (暂存已清空时)framer_flushed 置位使终结条件达成——
                // 单独的 framer_flushed 电平不可行动(暂存未清时终结
                // 条件仍不满足),置入复查会永久 continue 空转。
                if !this.pending_new.is_empty() && this.new_streams.poll_ready(cx).is_ready() {
                    continue; // 容量恢复:先交付再重评
                }
                this.shared.worker_wakers.lock()[this.index] = Some(cx.waker().clone());
                this.msg_rx.register_consumer_waker(cx.waker());
                this.ctrl_rx.register_consumer_waker(cx.waker());
                if this.msg_rx.has_pending()
                    || this.ctrl_rx.has_pending()
                    || (this.shared.framer_flushed.load(Ordering::Acquire)
                        && this.pending_new.is_empty())
                    || (!this.pending_new.is_empty() && this.new_streams.poll_ready(cx).is_ready())
                {
                    continue;
                }
                return Poll::Pending;
            }
            // §2.6 SendHalf Drop:不置错误——先处理完已路由消息(数据落
            // 缓冲,§0.3-3),再一次排空本地表与 opening 本分片子集(基线
            // SendHalf::drop → drop_all_streams 清全局表;SendHalf 已亡,
            // opening 不再新增,无帧条目直接摘除);此后继续服务新 SYN(不
            // 终结,对齐现行"发送半弃置后接收半仍可读/建流")。
            if this.shared.send_dropped.load(Ordering::Acquire) && !this.send_drop_drained {
                this.drain_ctrl();
                this.drain_msgs();
                this.drain_local_streams();
                this.drain_opening_shard();
                this.send_drop_drained = true;
            }
            let mut progress = false;
            // 1) 冷路径:ctrl(流删除)
            if this.drain_ctrl() {
                progress = true;
            }
            // 2) 热路径:ring 排空(单轮 ≤ 通道容量,天然有界)。
            //    D6 背压门控(复核修复):new_streams 满(暂存非空)时
            //    不再消费分片通道——通道随之填满 → Framer 停读 → TCP
            //    背压,端到端有界;修前无条件排空把建流句柄无限堆进
            //    无界暂存,背压链在此断裂。
            if this.pending_new.is_empty() && this.drain_msgs() {
                progress = true;
            }
            // 3) new_streams 交付重试(try_send 成功一条清一条)
            if this.deliver_pending_new() {
                progress = true;
            }
            if progress {
                continue;
            }
            // park:new_streams 满 → 发送侧 poll_ready 登记(futures mpsc
            // 自带容量唤醒;Ready 即容量恢复,继续交付)
            if !this.pending_new.is_empty() && this.new_streams.poll_ready(cx).is_ready() {
                continue;
            }
            // 登记 worker_wakers 槽(set_error/排空信号广播)与两通道
            // 消费者 waker,随后复查(丢唤醒防线:两侧都遵守)。分片
            // 通道的复查以门控开启为前提:门控挂起时滞留消息不是本
            // 轮可做的工作,把它当作"有工作"会空转(容量恢复的唤醒
            // 由 poll_ready 保证,届时门开即排空)。
            this.shared.worker_wakers.lock()[this.index] = Some(cx.waker().clone());
            this.msg_rx.register_consumer_waker(cx.waker());
            this.ctrl_rx.register_consumer_waker(cx.waker());
            if (this.pending_new.is_empty() && this.msg_rx.has_pending())
                || this.ctrl_rx.has_pending()
                || this.shared.error.lock().is_some()
                || this.shared.workers_shutdown.load(Ordering::Acquire)
                || (this.shared.send_dropped.load(Ordering::Acquire) && !this.send_drop_drained)
            {
                continue;
            }
            return Poll::Pending;
        }
    }
}

/// 发送侧分片 Worker(round-6 §2.3,规格 benchmark/runs/
/// tcp-multiflow-frp-20261015/coding-spec-round6-send-sharding.md):
/// 独占 `sid % N == index` 的流命令接收器,分摊原 send_driver 的计算
/// 密集面——多流命令轮询、帧转换、on_drop 选帧、ctrl 投递;跨 Tokio
/// 线程物理并行。产出完整 `Frame<()>` 压入本分片的 `send_out[k]` 有界
/// 通道(cap [`SEND_OUT_CAP`]),由 FlushTask(发送半)轮转消费写出。
/// 裸 Future(futures-only 红线),由 rh-transport 侧 spawn。
///
/// **顺序契约(红线 5 的机制保证)**:同一流全部事件(数据帧、
/// CloseStream、on_drop 产出的 FIN/RST)只被本 Worker 按命令到达顺序
/// 处理,产出帧按处理顺序 push 进同一条 `send_out[k]` FIFO ⟹ wire 上
/// FIN/RST 必在该流全部已产出数据之后;`pending_frame` 单槽 + "满即停"
/// (send_out[k] 满时不消费新命令)保证 on_drop 不越过滞留的数据命令。
/// 例外(与现行语义一致,非本流):to_send 里的接收侧 RST/FIN/Pong/
/// GoAway 优先级高于全部流帧——现行行为,不是回退。
///
/// Future 语义:`Ready(())` 仅出现在共享错误(含粘性影子
/// workers_shutdown)或 `send_dropped` 之后(D8:发送面已死则命令通道
/// 应断,流侧 sender 报错,与现行 flush 任务终结等效);`Pending` =
/// 无命令可消费、无滞留帧/ctrl、注册队列空。
pub struct SendWorker {
    /// k = sid % N(与接收侧 RecvWorker 同 N 同模数,D1:on_drop 的 ctrl
    /// Remove 恰路由到**同一个** RecvWorker,零额外路由表)。
    index: usize,
    shared: Arc<ConnShared>,
    streams: SelectAll<TaggedStream<StreamId, mpsc::Receiver<StreamCommand>>>,
    /// send_out[k] 生产句柄(本分片流全部产出帧的唯一出口,顺序契约)。
    out: ShardProducer<Frame<()>>,
    /// ctrl Remove 投递句柄(构造时预存 workers[k].1,D1 保证 k 即目标
    /// RecvWorker;clone 预存免每轮取锁,语义与现行 SendHalf 同款)。
    ctrl_tx: ShardProducer<ShardCtrl>,
    /// send_out[k] 满时的暂存帧(单槽:满即停止消费,顺序契约)。
    pending_frame: Option<Frame<()>>,
    /// 成功压入 send_out[k] 的累计帧数(推入序;SPSC 下与通道内部的
    /// 出队水位同一计量)。ctrl Remove 的门控水位取自此:drop 命令被
    /// 消费时本流全部数据帧已压入(命令通道 FIFO + 满即停),此刻的推入
    /// 序即"本流末帧序"的上界。
    out_pushed: u64,
    /// ctrl Remove 未投递暂存:(流 id, 门控水位)。水位 = drop 时的
    /// `out_pushed`,`popped_count() ≥ 水位` 即本流在途帧已全部出队,方可
    /// 投递(复核修复:精确水位替代"整队取空"——FlushTask 瓶颈态下
    /// 队列几乎永不为空,整队门控会无限期饥饿 Remove,对端收不到
    /// RST/FIN、stream_count 不减)。上界 = 存活流数;队首阻塞即止,其余
    /// Remove 在其后保序——多流并发 drop 时通道可能满,单槽会丢。
    ctrl_pending: VecDeque<(StreamId, u64)>,
    finished: bool,
}

impl SendWorker {
    pub(super) fn new(
        index: usize,
        shared: Arc<ConnShared>,
        out: ShardProducer<Frame<()>>,
        ctrl_tx: ShardProducer<ShardCtrl>,
    ) -> Self {
        SendWorker {
            index,
            shared,
            streams: SelectAll::default(),
            out,
            ctrl_tx,
            pending_frame: None,
            out_pushed: 0,
            ctrl_pending: VecDeque::new(),
            finished: false,
        }
    }

    /// 产出帧压入 send_out[k](顺序契约唯一出口):满则登记生产者 waker
    /// 并复查一次(§2.1 防线);仍满返回帧由调用方存 pending_frame。
    /// 成功即推进推入序 `out_pushed`(ctrl Remove 门控水位的计量)。
    fn push_out_frame(&mut self, frame: Frame<()>, cx: &mut Context<'_>) -> Result<(), Frame<()>> {
        let pushed = match self.out.try_push(frame) {
            Ok(()) => Ok(()),
            Err(f) => {
                self.out.register_producer_waker(cx.waker());
                // 复查(丢唤醒防线):登记后让出空间即唤醒
                self.out.try_push(f)
            }
        };
        if pushed.is_ok() {
            self.out_pushed += 1;
        }
        pushed
    }

    /// ctrl Remove 投递(§2.5-D ②:经 workers[k].1 路由,D1 保证 k 即
    /// 目标 RecvWorker):成功 → true;满 → 登记生产者 waker + 复查
    /// (§2.1 防线),仍满由调用方暂存 ctrl_pending(park 后重试)。
    /// 与现行 SendHalf 同款语义,整体迁入。
    fn try_push_ctrl(&self, stream_id: StreamId, cx: &mut Context<'_>) -> bool {
        match self.ctrl_tx.try_push(ShardCtrl::Remove(stream_id)) {
            Ok(()) => true,
            Err(_) => {
                self.ctrl_tx.register_producer_waker(cx.waker());
                matches!(self.ctrl_tx.try_push(ShardCtrl::Remove(stream_id)), Ok(()))
            }
        }
    }

    /// ctrl_pending 排空重试(队首阻塞即止——其余 Remove 在其后保序)。
    /// **顺序门控(红线 5,精确水位)**:Remove 只在 `popped_count() ≥
    /// 本条水位`(drop 时的推入序)后投递——此前本流仍有数据帧滞留
    /// send_out[k],RecvWorker finalize 的 RST/FIN 经 to_send(优先级
    /// 最高)会越过它们插队上线(分片前单任务天然保序,分片后此门控
    /// 恢复等价);出队水位追平即本流在途帧已全部弹出,且 FlushTask 的
    /// 弹出与 start_send 在 fill_batch 内严格交替,后到的 to_send 帧只会
    /// 排在其后。门控不要求整队取空:水位之后压入的他流帧不计,持续
    /// 负载下迟到以 drop 时的队列占用(≤ cap)为界,不饥饿(复核修复)。
    /// 唤醒来源:满变非满的生产者 waker(队列曾满)/ FlushTask 取空
    /// 通知 send_worker_wakers[k](取空时水位必已追平)/ 任一命令到达。
    fn flush_ctrl_pending(&mut self, cx: &mut Context<'_>) -> bool {
        let mut progress = false;
        while let Some(&(sid, watermark)) = self.ctrl_pending.front() {
            if self.out.popped_count() < watermark {
                log::trace!(
                    "{}/{}: stream removal parked on in-flight send_out frames",
                    self.shared.id,
                    sid
                );
                break;
            }
            if !self.try_push_ctrl(sid, cx) {
                log::trace!(
                    "{}/{}: stream removal parked on full ctrl channel",
                    self.shared.id,
                    sid
                );
                break;
            }
            self.ctrl_pending.pop_front();
            progress = true;
        }
        // round-6.1 F2:暂存清空(全部投递成功)即无滞留,清零取空通知
        // 前置门——FlushTask 恢复"一次原子读即跳过两把锁"的廉价路径。
        // 置位与清零同在本 Worker 线程,Read 时序竞缝至多一次伪唤醒
        // (FlushTask 读到 true 而唤已无滞留者),park 复查后重 Pending,
        // 无害(规格 §1.2/§1.3)。SeqCst:与置位/门读构成 SC 对写侧
        // (约束见 popped_count 文档),此处清零同理入对。
        if self.ctrl_pending.is_empty() {
            self.shared.ctrl_wait[self.index].store(false, Ordering::SeqCst);
        }
        progress
    }

    /// §2.5-D(on_drop 全量迁入):命令通道关闭 `(id, None)` 时的流删除
    /// 双路径。① opening 命中 → `finalize_dropped_stream`(选帧、唤醒、
    /// 双计数器递减三联),产出帧同样过 send_out[k] 与本流数据同 FIFO
    /// (红线 5);② 未命中(已被 RecvWorker 收编)→ ctrl Remove 直送
    /// RecvWorker k(帧经 to_send 汇入,现行 Round-5 行为;投递受本流
    /// 在途帧出队水位门控,见 [`Self::flush_ctrl_pending`];Remove 迟到
    /// 无害且有界)。返回 `Some(frame)` = send_out 满滞留帧(调用方存
    /// pending_frame 并停止消费——on_drop 不得越过本流滞留数据)。
    fn on_drop_stream(&mut self, stream_id: StreamId, cx: &mut Context<'_>) -> Option<Frame<()>> {
        // 先释放 opening 锁再 finalize/压帧:不跨 stream::Shared 与通道锁持有
        let opening_hit = self.shared.opening.lock().remove(&stream_id);
        if let Some(s) = opening_hit {
            log::trace!("{}: removing dropped stream {}", self.shared.id, stream_id);
            if let Some(frame) = finalize_dropped_stream(&self.shared, stream_id, &s) {
                if let Err(f) = self.push_out_frame(frame, cx) {
                    return Some(f);
                }
            }
            return None;
        }
        // round-6.1 F2:置位取空通知前置门(先于 push:紧随的
        // flush_ctrl_pending 与对端观察都看到一致"非空"状态;置位
        // 先后于 push 均不漏唤醒,见规格 round-6.1 §1.3——flag 咨询性,
        // Worker 未 park 时通知本就多余)。SeqCst(复核修复):本 store
        // 与紧随 park 复查的 popped 读同线程程序序,对端(FlushTask)
        // 则以取空 fetch_add → 门读 flag 的顺序观察——双侧任一读到
        // 陈旧值在 Release/Acquire 下可同时成立(store-buffering 形态,
        // 空闲连接 Remove 滞留);四点全 SeqCst 后 SC 全序禁"双陈旧",
        // 约束详见 popped_count 文档
        self.shared.ctrl_wait[self.index].store(true, Ordering::SeqCst);
        // 水位 = 此刻推入序:drop 命令在本流全部数据命令之后被消费(命令
        // 通道 FIFO),且步骤 3 仅在 pending_frame 为空时运行(满即停),
        // 故本流数据帧此刻已全部成功压入,推入序即其末帧序的上界
        self.ctrl_pending.push_back((stream_id, self.out_pushed));
        // 立即尝试一次(在途帧已出队且通道有余量时免等下一轮;否则仅
        // 暂存,不阻塞数据命令消费——Remove 迟到无害)
        self.flush_ctrl_pending(cx);
        None
    }
}

impl Future for SendWorker {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let this = &mut *self;
        // round-6.1 F1:命令预算是 poll 级的(单次 poll 全程共用)——在
        // loop 外声明,`progress → continue` 不再重建,并发补货下单次
        // poll 消费量有上界(公平性,round-6 §2.3「单轮 poll ≤64」的
        // 字面兑现)。耗尽即让出(budget_exhausted → wake_by_ref +
        // Pending);park 复查(pending_frame/flush_ctrl/注册)不是命令
        // 消费,不受预算约束。
        let mut budget = SEND_CMDS_PER_POLL;
        loop {
            if this.finished {
                return Poll::Ready(());
            }
            // D8 终结:共享错误(含粘性影子 workers_shutdown,防 take_error
            // 先行移除后漏终结)或 send_dropped(§2.5:发送面弃置,命令
            // 通道应断)→ drop 全部接收器(命令通道断,流侧 sender 报错)
            // + 丢弃 pending 帧 + 清 ctrl 暂存 + Ready。错误槽非空时回填
            // make_shadow(现行 take-error 观察方语义:SendHalf 只凭错误槽
            // 观察错误,本 Worker 取走后必须回填,否则 FlushTask 看不到
            // 已发生的中止)。
            if this.shared.workers_shutdown.load(Ordering::Acquire)
                || this.shared.error.lock().is_some()
                || this.shared.send_dropped.load(Ordering::Acquire)
            {
                if let Some(e) = this.shared.take_error() {
                    this.shared.set_error(make_shadow(&e));
                }
                this.streams.clear();
                this.pending_frame = None;
                this.ctrl_pending.clear();
                this.finished = true;
                return Poll::Ready(());
            }
            let mut progress = false;
            // 1) 注册收编(D4:send_regs[k] 全量 drain——入站建流
            //    (push_new_receiver)与出站开流(make_new_outbound_stream)
            //    的命令接收器都按 sid%N 压入;条目上界 = max_num_streams)
            {
                let mut regs = this.shared.send_regs.lock();
                if !regs[this.index].is_empty() {
                    for (id, r) in regs[this.index].drain(..) {
                        this.streams.push(TaggedStream::new(id, r));
                    }
                    progress = true;
                }
            }
            // 2) pending_frame 重试:成功 → 清槽 + progress;失败经
            //    push_out_frame 以**当前** waker 重新登记(register-then-
            //    retry:跨 poll 场景下槽内可能是上一 poll 的旧 waker,
            //    不刷新则消费侧会唤醒到失效上下文)+ 复查一次,仍满则
            //    跳过 3——满即停,顺序契约:未消费命令保留在命令通道/
            //    SelectAll 中不被消费,on_drop 不越过滞留的数据命令
            if let Some(f) = this.pending_frame.take() {
                match this.push_out_frame(f, cx) {
                    Ok(()) => progress = true,
                    Err(f) => this.pending_frame = Some(f),
                }
            }
            // 3) 命令消费(单次 poll ≤ SEND_CMDS_PER_POLL 条,超出保留
            //    下次 poll;公平性:防单 Worker 独占线程——tokio 协作调度
            //    只在 poll 返回时让出,预算是 poll 级的(见 poll 入口),
            //    loop 的 progress → continue 不重置;预算耗尽后本轮 poll
            //    不得再消费命令,pending_frame 满滞留时同样跳过)
            let mut budget_exhausted = false;
            if this.pending_frame.is_none() && budget > 0 {
                while budget > 0 {
                    match this.streams.poll_next_unpin(cx) {
                        Poll::Ready(Some((_, Some(StreamCommand::SendFrame(frame))))) => {
                            budget -= 1;
                            log::trace!(
                                "{}/{}: sending: {}",
                                this.shared.id,
                                frame.header().stream_id(),
                                frame.header()
                            );
                            let frame: Frame<()> = frame.into();
                            match this.push_out_frame(frame, cx) {
                                Ok(()) => progress = true,
                                Err(f) => {
                                    this.pending_frame = Some(f);
                                    break; // send_out 满:停止消费(顺序契约)
                                }
                            }
                        }
                        Poll::Ready(Some((id, Some(StreamCommand::CloseStream { ack, syn })))) => {
                            budget -= 1;
                            log::trace!("{}: sending close {}", this.shared.id, id);
                            let frame: Frame<()> = Frame::close_stream(id, ack, syn).into();
                            match this.push_out_frame(frame, cx) {
                                Ok(()) => progress = true,
                                Err(f) => {
                                    this.pending_frame = Some(f);
                                    break; // send_out 满:停止消费(顺序契约)
                                }
                            }
                        }
                        Poll::Ready(Some((id, None))) => {
                            budget -= 1;
                            log::trace!("{}: consuming drop of stream {}", this.shared.id, id);
                            match this.on_drop_stream(id, cx) {
                                Some(f) => {
                                    this.pending_frame = Some(f);
                                    break; // opening 命中的 FIN/RST 滞留:停止消费
                                }
                                None => progress = true,
                            }
                        }
                        // SelectAll 空(无接收器可挂 waker——新接收器到达
                        // 经 send_worker_wakers[k] 唤醒,park 复查 a 捕获)
                        // 或命令暂无就绪(mpsc 已登记 waker)
                        Poll::Ready(None) | Poll::Pending => break,
                    }
                }
                budget_exhausted = budget == 0;
            }
            // ctrl 暂存重试(不阻塞数据命令:Remove 迟到无害)
            if this.flush_ctrl_pending(cx) {
                progress = true;
            }
            // 4) progress → continue;预算耗尽的让出例外(§2.3 预算说明
            //    的实现形态:此刻命令通道可能仍有剩余,mpsc waker 未必
            //    登记过——直接 park 会滞留到无关事件。自唤醒 + 返回
            //    Pending:任务重新排队、其他任务插队(防独占),剩余命令
            //    下次 poll 继续,不丢)
            if progress {
                if budget_exhausted {
                    cx.waker().wake_by_ref();
                    return Poll::Pending;
                }
                continue;
            }
            // park(register-then-recheck 防线,§2.3;全部满足才 Pending)。
            // a. 注册 send_worker_wakers[k](注册压入 / set_error /
            //    send_dropped 唤醒)→ 复查 send_regs[k] 非空
            this.shared.send_worker_wakers.lock()[this.index] = Some(cx.waker().clone());
            let mut rerun = !this.shared.send_regs.lock()[this.index].is_empty();
            // 终结条件复查:与 waker 登记的竞缝由 set_error 广播兜住
            // (标志粘性,continue 即进终结分支返回,不自旋)
            if !rerun
                && (this.shared.workers_shutdown.load(Ordering::Acquire)
                    || this.shared.error.lock().is_some()
                    || this.shared.send_dropped.load(Ordering::Acquire))
            {
                rerun = true;
            }
            // b. pending_frame 非空 → 经 push_out_frame 以当前 waker 登记
            //    + 复查 try_push 一次(登记跨 poll 刷新,消费侧 take 后
            //    槽空,此处重登是唯一可靠的唤醒来源)
            if !rerun {
                if let Some(f) = this.pending_frame.take() {
                    match this.push_out_frame(f, cx) {
                        Ok(()) => rerun = true,
                        Err(f) => this.pending_frame = Some(f),
                    }
                }
            }
            // c. ctrl_pending 非空 → 复查投递。两种阻塞各有已登记的唤醒:
            //    ctrl 通道满 → ctrl producer waker 已在失败时登记;在途帧
            //    水位未追平 → 队列曾满时 b 已登记 send_out producer waker
            //    (满变非满即醒),队列未满时 FlushTask 取空通知经 a 登记
            //    的 send_worker_wakers[k] 唤醒(取空 ⟹ 水位追平);a 在
            //    本复查之前登记,登记后的取空由本复查捕获(防线同款)
            if !rerun && this.flush_ctrl_pending(cx) {
                rerun = true;
            }
            // d. streams 非空:SelectAll 的 poll_next 已登记 waker(命令
            //    到达即唤醒);streams 空:新接收器靠 a 的注册唤醒
            if rerun {
                continue;
            }
            return Poll::Pending;
        }
    }
}

/// 关闭单条流的状态并唤醒两端 waker(数据保留在缓冲,read_after_close
/// 仍可读;Closed 上重复调用无迁移,幂等)。Worker 排空/及时关闭面共用。
fn close_stream(
    shared: &ConnShared,
    id: StreamId,
    s: &Arc<Mutex<crate::connection::stream::Shared>>,
) {
    let mut st = s.lock();
    st.update_state(shared.id, id, State::Closed);
    if let Some(w) = st.reader.take() {
        w.wake()
    }
    if let Some(w) = st.writer.take() {
        w.wake()
    }
}

/// 现行 on_drop_stream 的等价三联(§2.5-D):按关前状态选帧
/// (Open→RST、RecvClosed→FIN、SendClosed/Closed→无帧)+ 唤醒流侧
/// reader/writer waker + 存活流计数递减(未确认出站计数由 update_state
/// 内的令牌递减,D4)。SendHalf 的 opening 命中路径与 Worker 的
/// ctrl Remove 路径共用,保证两条路径行为一致。
pub(super) fn finalize_dropped_stream(
    shared: &ConnShared,
    stream_id: StreamId,
    s: &Arc<Mutex<crate::connection::stream::Shared>>,
) -> Option<Frame<()>> {
    let mut stream_shared = s.lock();
    let frame = match stream_shared.update_state(shared.id, stream_id, State::Closed) {
        // The stream was dropped without calling `poll_close`.
        // We reset the stream to inform the remote of the closure.
        State::Open { .. } => {
            let mut header = Header::data(stream_id, 0);
            header.rst();
            Some(Frame::new(header))
        }
        // The stream was dropped without calling `poll_close`.
        // We have already received a FIN from remote and send one
        // back which closes the stream for good.
        State::RecvClosed => {
            let mut header = Header::data(stream_id, 0);
            header.fin();
            Some(Frame::new(header))
        }
        // The stream was properly closed. We already sent our FIN frame.
        State::SendClosed => None,
        // The stream was properly closed. We already have sent our FIN frame.
        // The remote end has already done so in the past.
        State::Closed => None,
    };
    if let Some(w) = stream_shared.reader.take() {
        w.wake()
    }
    if let Some(w) = stream_shared.writer.take() {
        w.wake()
    }
    drop(stream_shared);
    shared.stream_count.fetch_sub(1, Ordering::SeqCst);
    frame.map(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connection::Connection;
    use futures::io::{AsyncRead, AsyncWrite};
    use futures::task::{noop_waker, waker, ArcWake};
    use futures::FutureExt;
    use std::cell::RefCell;
    use std::collections::HashMap;
    use std::rc::Rc;
    use std::sync::atomic::AtomicUsize;

    // ===== §5.1 通道(D2 轻量通道)=====

    /// 唤醒计数 waker(elision 断言用)。
    #[derive(Default)]
    struct CountWaker(AtomicUsize);
    impl ArcWake for CountWaker {
        fn wake_by_ref(me: &Arc<Self>) {
            me.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    /// 唤醒置位 waker(round-6 T-S/T-F 系:单次唤醒观察)。
    #[derive(Default)]
    struct WakeFlag(AtomicBool);
    impl ArcWake for WakeFlag {
        fn wake_by_ref(me: &Arc<Self>) {
            me.0.store(true, Ordering::SeqCst);
        }
    }

    /// 构造 Data 帧(测试 helper,round-6 T-S/T-F 系)。
    fn data_frame(id: u32, body: Vec<u8>) -> Frame<()> {
        Frame::data(StreamId::new(id), body.into()).unwrap().into()
    }

    /// 按帧走读线上字节(tag 感知:只有 Data 帧的 len 是帧体长度,控制帧
    /// 的 len 字段是信用/opaque id、无帧体;round-6 T-S/T-F 系)。
    fn frames_on_wire(st: &Rc<RefCell<ScriptState>>) -> Vec<(Header<()>, Vec<u8>)> {
        let bytes = st.borrow().written.clone();
        frames_from_bytes(&bytes)
    }

    /// [`frames_on_wire`] 的字节级入口(T-F3:ThrottleSocket 的 written
    /// 不经 ScriptState)。
    fn frames_from_bytes(bytes: &[u8]) -> Vec<(Header<()>, Vec<u8>)> {
        let mut out = Vec::new();
        let mut off = 0;
        while off + header::HEADER_SIZE <= bytes.len() {
            let block: [u8; header::HEADER_SIZE] =
                bytes[off..off + header::HEADER_SIZE].try_into().unwrap();
            let hdr = header::decode(&block).expect("written bytes must be valid frames");
            let body_len = if hdr.tag() == Tag::Data {
                hdr.len().val() as usize
            } else {
                0
            };
            let body =
                bytes[off + header::HEADER_SIZE..off + header::HEADER_SIZE + body_len].to_vec();
            off += header::HEADER_SIZE + body_len;
            out.push((hdr, body));
        }
        assert_eq!(off, bytes.len(), "wire must end on a frame boundary");
        out
    }

    /// 解析 written 字节为 header 序列(T-F3 反向断言用)。
    fn written_headers_from_bytes(bytes: &[u8]) -> Vec<Header<()>> {
        frames_from_bytes(bytes)
            .into_iter()
            .map(|(h, _)| h)
            .collect()
    }

    /// T-C1:cap=4 压 5 条 → 第 5 条 Err 原样返还;消费后可再压;FIFO。
    #[test]
    fn shard_channel_bounded_and_fifo() {
        let (prod, cons) = ShardChannel::with_capacity(4);
        for i in 0..4 {
            assert_eq!(prod.try_push(i), Ok(()), "cap 内必须可压");
        }
        match prod.try_push(4) {
            Err(returned) => assert_eq!(returned, 4, "满时必须原样返还"),
            Ok(()) => panic!("cap=4 压第 5 条必须 Err"),
        }
        // FIFO + 消费后可再压
        let mut buf = VecDeque::with_capacity(8);
        assert_eq!(cons.drain_into(&mut buf), 4);
        assert_eq!(buf.into_iter().collect::<Vec<_>>(), vec![0, 1, 2, 3]);
        let mut buf = VecDeque::with_capacity(8);
        assert_eq!(prod.try_push(5), Ok(()));
        assert_eq!(cons.drain_into(&mut buf), 1);
        assert_eq!(buf.front(), Some(&5));
    }

    /// T-C2(红→绿):waker elision——唤醒只允许发生在"队列从空变非空
    /// 且消费者已登记"的时刻。竞缝形态:消费者把 waker 登记在队列
    /// **非空**期间(复核前的窗口)——此后连续 push 不得唤醒(修前
    /// 无 was_empty 门、push 即 take+wake,count ≥ 1,红);消费排空
    /// 后重新登记,下一次空→非空恰 1 次 wake,再压不重复。
    #[test]
    fn shard_channel_waker_elision_on_empty_to_nonempty() {
        let (prod, cons) = ShardChannel::<u8>::with_capacity(8);
        let cw = Arc::new(CountWaker::default());
        let w = waker(cw.clone());
        // 竞缝:先 push(无登记,不唤醒)再登记(队列非空期间)
        assert_eq!(prod.try_push(1), Ok(()));
        assert_eq!(cw.0.load(Ordering::SeqCst), 0);
        cons.register_consumer_waker(&w);
        assert_eq!(cw.0.load(Ordering::SeqCst), 0);
        // 非空期间连续 push:不得唤醒(elision 契约)
        assert_eq!(prod.try_push(2), Ok(()));
        assert_eq!(prod.try_push(3), Ok(()));
        assert_eq!(
            cw.0.load(Ordering::SeqCst),
            0,
            "非空期间 push 不得唤醒(waker 登记于非空竞缝)"
        );
        // 消费排空 → 重新登记 → 空变非空恰 1 次 wake
        let mut buf = VecDeque::with_capacity(8);
        assert_eq!(cons.drain_into(&mut buf), 3);
        cons.register_consumer_waker(&w);
        assert_eq!(prod.try_push(4), Ok(()));
        assert_eq!(cw.0.load(Ordering::SeqCst), 1, "空→非空恰 1 次 wake");
        // 唤醒后槽已清空(take 语义):非空期间再压不再唤醒
        assert_eq!(prod.try_push(5), Ok(()));
        assert_eq!(cw.0.load(Ordering::SeqCst), 1, "非空期间 push 不得重复唤醒");
    }

    /// T-C3(红→绿):丢唤醒防线——生产者侧对称(满→非满恰 1 次
    /// wake);登记与对端 push 的竞缝由"登记后复查"兜住:登记时队列
    /// 已非空则 has_pending 必须为真,消费侧不得 park。
    #[test]
    fn shard_channel_lost_wakeup_defense_recheck() {
        // (a) 生产者侧 elision:填满 → 登记 → 消费 → 恰 1 次 wake
        let (prod, cons) = ShardChannel::<u8>::with_capacity(2);
        let pw = Arc::new(CountWaker::default());
        let w = waker(pw.clone());
        assert_eq!(prod.try_push(1), Ok(()));
        assert_eq!(prod.try_push(2), Ok(()));
        prod.register_producer_waker(&w);
        let mut buf = VecDeque::with_capacity(4);
        assert_eq!(cons.drain_into(&mut buf), 2);
        assert_eq!(pw.0.load(Ordering::SeqCst), 1, "满→非满恰 1 次 wake");
        // 唤醒后槽已清空:再次填满不重复唤醒
        assert_eq!(prod.try_push(3), Ok(()));
        assert_eq!(prod.try_push(4), Ok(()));
        assert_eq!(pw.0.load(Ordering::SeqCst), 1, "已唤醒过不再重复");

        // (b) 竞缝:push 先于消费者登记发生 → 登记后复查必须发现非空
        let (prod2, cons2) = ShardChannel::<u8>::with_capacity(4);
        let cw2 = Arc::new(CountWaker::default());
        let w2 = waker(cw2.clone());
        assert_eq!(prod2.try_push(9), Ok(())); // 竞缝内:push 时无人登记
        assert_eq!(cw2.0.load(Ordering::SeqCst), 0);
        cons2.register_consumer_waker(&w2); // 消费侧随后登记
        assert!(
            cons2.has_pending(),
            "登记后复查必须发现非空(丢唤醒防线:不得 park)"
        );
        let mut buf2 = VecDeque::with_capacity(4);
        assert_eq!(
            cons2.drain_into(&mut buf2),
            1,
            "复查通过即继续处理,消息不滞留"
        );
    }

    /// T-C4:drop 消费者 → producer 视角 closed。
    #[test]
    fn shard_channel_consumer_gone() {
        let (prod, cons) = ShardChannel::<u8>::with_capacity(4);
        assert!(!prod.consumer_gone());
        drop(cons);
        assert!(prod.consumer_gone());
    }

    /// T-O1(round-6 §5.1,红→绿:try_pop 修前不存在):try_pop 语义——
    /// FIFO;空 → None;满 → pop 后生产者被唤醒恰一次(elision 契约镜像:
    /// 只发生在"从满变非满"且生产者已登记的时刻;非满期间 pop 不重复
    /// 唤醒)。
    #[test]
    fn shard_channel_try_pop_fifo_and_producer_elision() {
        let (prod, cons) = ShardChannel::<u8>::with_capacity(4);
        // FIFO
        for i in 0..4 {
            assert_eq!(prod.try_push(i), Ok(()));
        }
        let pw = Arc::new(CountWaker::default());
        let w = waker(pw.clone());
        prod.register_producer_waker(&w);
        assert_eq!(
            cons.try_pop(),
            Some(0),
            "try_pop must be FIFO (front first)"
        );
        assert_eq!(
            pw.0.load(Ordering::SeqCst),
            1,
            "full→non-full must wake the registered producer exactly once"
        );
        // 非满期间 pop:不重复唤醒(镜像 elision)
        assert_eq!(cons.try_pop(), Some(1));
        assert_eq!(
            pw.0.load(Ordering::SeqCst),
            1,
            "pop while non-full must not re-wake the producer"
        );
        // 排空 → 空 → None;消费者侧镜像:登记后空变非空恰一次唤醒
        assert_eq!(cons.try_pop(), Some(2));
        assert_eq!(cons.try_pop(), Some(3));
        assert_eq!(cons.try_pop(), None, "empty channel must pop None");
        let cw = Arc::new(CountWaker::default());
        let cw_waker = waker(cw.clone());
        cons.register_consumer_waker(&cw_waker);
        assert_eq!(prod.try_push(9), Ok(()));
        assert_eq!(
            cw.0.load(Ordering::SeqCst),
            1,
            "empty→non-empty must wake the registered consumer exactly once"
        );
        assert_eq!(cons.try_pop(), Some(9));
    }

    // ===== §5.2/§5.3 测试基建 =====

    /// 分片测试载体:读侧按脚本块返回(耗尽 Pending;eof → Ok(0)),
    /// 写侧收集 written(单线程 Rc,noop waker 手动驱动)。
    struct ScriptState {
        read_blocks: std::collections::VecDeque<Vec<u8>>,
        cur: Vec<u8>,
        cur_off: usize,
        written: Vec<u8>,
        eof: bool,
    }

    struct ScriptSocket(Rc<RefCell<ScriptState>>);

    impl ScriptSocket {
        fn new() -> (Self, Rc<RefCell<ScriptState>>) {
            let st = Rc::new(RefCell::new(ScriptState {
                read_blocks: Default::default(),
                cur: Vec::new(),
                cur_off: 0,
                written: Vec::new(),
                eof: false,
            }));
            (ScriptSocket(st.clone()), st)
        }
    }

    impl crate::Writev2 for ScriptSocket {
        fn poll_write2(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            a: &[u8],
            b: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            let mut st = self.0.borrow_mut();
            st.written.extend_from_slice(a);
            st.written.extend_from_slice(b);
            Poll::Ready(Ok(a.len() + b.len()))
        }
    }

    impl futures::io::AsyncRead for ScriptSocket {
        fn poll_read(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &mut [u8],
        ) -> Poll<std::io::Result<usize>> {
            let mut st = self.0.borrow_mut();
            if st.cur_off >= st.cur.len() {
                st.cur = st.read_blocks.pop_front().unwrap_or_default();
                st.cur_off = 0;
            }
            if st.cur_off < st.cur.len() {
                let n = (st.cur.len() - st.cur_off).min(buf.len());
                buf[..n].copy_from_slice(&st.cur[st.cur_off..st.cur_off + n]);
                st.cur_off += n;
                return Poll::Ready(Ok(n));
            }
            if st.eof {
                return Poll::Ready(Ok(0));
            }
            Poll::Pending
        }
    }

    impl futures::io::AsyncWrite for ScriptSocket {
        fn poll_write(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            crate::Writev2::poll_write2(self, cx, &[], buf)
        }
        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }
        fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    fn noop_cx() -> Context<'static> {
        Context::from_waker(Box::leak(Box::new(noop_waker())))
    }

    /// Data|SYN 块(建流 + 首帧体)。
    fn syn_data_block(id: u32, body: &[u8]) -> Vec<u8> {
        let mut h = Header::data(StreamId::new(id), body.len() as u32);
        h.syn();
        let mut v = header::encode(&h).to_vec();
        v.extend_from_slice(body);
        v
    }

    /// Data 块(无 SYN/FIN)。
    fn data_block(id: u32, body: &[u8]) -> Vec<u8> {
        let h = Header::data(StreamId::new(id), body.len() as u32);
        let mut v = header::encode(&h).to_vec();
        v.extend_from_slice(body);
        v
    }

    /// Data+FIN 块(半关闭)。
    fn data_fin_block(id: u32, body: &[u8]) -> Vec<u8> {
        let mut h = Header::data(StreamId::new(id), body.len() as u32);
        h.fin();
        let mut v = header::encode(&h).to_vec();
        v.extend_from_slice(body);
        v
    }

    /// Data+RST 块(复位)。
    fn data_rst_block(id: u32) -> Vec<u8> {
        let mut h = Header::data(StreamId::new(id), 0);
        h.rst();
        header::encode(&h).to_vec()
    }

    /// Data+ACK 块(对端对本侧出站流的首个响应帧:确认 + 数据)。
    fn data_ack_block(id: u32, body: &[u8]) -> Vec<u8> {
        let mut h = Header::data(StreamId::new(id), body.len() as u32);
        h.ack();
        let mut v = header::encode(&h).to_vec();
        v.extend_from_slice(body);
        v
    }

    /// WU+ACK(+credit)块。
    fn wu_ack_credit_block(id: u32, credit: u32) -> Vec<u8> {
        let mut h = Header::window_update(StreamId::new(id), credit);
        h.ack();
        header::encode(&h).to_vec()
    }

    /// WU(credit)块(无 SYN/ACK/FIN)。
    fn wu_credit_block(id: u32, credit: u32) -> Vec<u8> {
        let h = Header::window_update(StreamId::new(id), credit);
        header::encode(&h).to_vec()
    }

    /// 流级 Ping 原始块(手工 12B:sid≠0、无 ACK)。
    fn ping_sid_block(id: u32, opaque: u32) -> Vec<u8> {
        let mut v = vec![0u8, 2, 0, 0];
        v.extend_from_slice(&id.to_be_bytes());
        v.extend_from_slice(&opaque.to_be_bytes());
        v
    }

    /// 解析 written 为 header 序列(按 12B header + Data 帧体步进)。
    fn written_headers(st: &Rc<RefCell<ScriptState>>) -> Vec<Header<()>> {
        let bytes = st.borrow().written.clone();
        let mut out = Vec::new();
        let mut off = 0;
        while off + header::HEADER_SIZE <= bytes.len() {
            let block: [u8; header::HEADER_SIZE] =
                bytes[off..off + header::HEADER_SIZE].try_into().unwrap();
            let hdr = header::decode(&block).expect("written bytes must be valid frames");
            off += header::HEADER_SIZE
                + if hdr.tag() == Tag::Data {
                    hdr.len().val() as usize
                } else {
                    0
                };
            out.push(hdr);
        }
        out
    }

    /// 分片 halves 测试夹具:N 个 RecvWorker + N 个 SendWorker 的 halves
    /// 四元组 + 脚本载体 + 共享状态句柄(计数器/开流表断言用)。
    struct Fixture {
        recv: RecvHalfForTest,
        send: SendHalfForTest,
        workers: Vec<ShardWorker>,
        send_workers: Vec<SendWorker>,
        st: Rc<RefCell<ScriptState>>,
        shared: Arc<ConnShared>,
        cx: Context<'static>,
    }

    type RecvHalfForTest = super::super::RecvHalf<super::super::SplitSocket<ScriptSocket>>;
    type SendHalfForTest = super::super::SendHalf<super::super::SplitSocket<ScriptSocket>>;

    impl Fixture {
        /// client 模式(入站流 id 为偶数 → worker0;出站流 id 为奇数)。
        /// 分片数强制写入 cfg(默认 4,测试假定为 N)。
        fn client(n: usize, mut cfg: Config) -> Self {
            cfg.set_shard_workers(n);
            let (socket, st) = ScriptSocket::new();
            let conn = Connection::new(socket, cfg, Mode::Client);
            let (recv, send, workers, send_workers) = conn.into_halves();
            let shared = recv.shared_for_test();
            debug_assert_eq!(workers.len(), n);
            debug_assert_eq!(send_workers.len(), n);
            Fixture {
                recv,
                send,
                workers,
                send_workers,
                st,
                shared,
                cx: noop_cx(),
            }
        }

        fn feed(&self, block: Vec<u8>) {
            self.st.borrow_mut().read_blocks.push_back(block);
        }

        /// 驱动 recv + 双侧 Worker 一轮收敛(单线程手动推进,等价生产侧
        /// tokio::spawn 的并行):recv 排空路由 → RecvWorker 全处理 →
        /// SendWorker 同 poll(round-6 §5.4:驱动面覆盖)→ recv 再探
        /// (new_streams 交付)。
        fn drive_recv(&mut self) -> Poll<Option<crate::Result<Stream>>> {
            let cx = &mut self.cx;
            match self.recv.poll(cx) {
                Poll::Pending => {}
                other => return other,
            }
            for w in self.workers.iter_mut() {
                let _ = w.poll_unpin(cx);
            }
            for w in self.send_workers.iter_mut() {
                let _ = w.poll_unpin(cx);
            }
            self.recv.poll(cx)
        }

        /// 驱动 send + 双侧 Worker 一轮收敛:send(FlushTask)冲刷 →
        /// RecvWorker 全处理(建流授信命令等)→ SendWorker 消费流命令、
        /// 产出帧压 send_out(round-6:发送面数据命令的唯一消费者)→
        /// send 终轮把 send_out/to_send 新帧写上 wire。
        fn drive_send(&mut self) -> Poll<crate::Result<()>> {
            let cx = &mut self.cx;
            match self.send.poll(cx) {
                Poll::Pending => {}
                other => return other,
            }
            for w in self.workers.iter_mut() {
                let _ = w.poll_unpin(cx);
            }
            for w in self.send_workers.iter_mut() {
                let _ = w.poll_unpin(cx);
            }
            self.send.poll(cx)
        }

        /// 只驱动 SendWorker 一轮(不发车 FlushTask/RecvWorker):T-S 系
        /// 对 SendWorker 自身行为(收编/消费/满停/park)的白盒驱动。
        fn poll_send_workers(&mut self) {
            let cx = &mut self.cx;
            for w in self.send_workers.iter_mut() {
                let _ = w.poll_unpin(cx);
            }
        }
    }

    /// 取流读尽当前就绪缓冲(noop waker 下 Pending = 本轮无更多数据;
    /// FIN/EOF 时返回 Ready(Ok(0)) 收尾)。
    fn drain_stream(stream: &mut Stream, cx: &mut Context<'_>) -> std::io::Result<Vec<u8>> {
        let mut out = Vec::new();
        let mut buf = [0u8; 8192];
        loop {
            match Pin::new(&mut *stream).poll_read(cx, &mut buf) {
                Poll::Ready(Ok(0)) => return Ok(out),
                Poll::Ready(Ok(n)) => out.extend_from_slice(&buf[..n]),
                Poll::Ready(Err(e)) => return Err(e),
                Poll::Pending => return Ok(out),
            }
        }
    }

    // ===== §5.2 Worker 派发(N=2 构造)=====

    /// T-W1:同一 sid 连续 Data 帧跨多次 poll → buffer 顺序逐字节一致。
    #[test]
    fn worker_stream_order_across_polls() {
        let mut f = Fixture::client(2, Config::default());
        // 第一批:建流 + 3 帧
        f.feed(syn_data_block(2, &[0xA0]));
        for i in 1..=3u8 {
            f.feed(data_block(2, &[0xA0 + i; 100]));
        }
        let mut stream = match f.drive_recv() {
            Poll::Ready(Some(Ok(s))) => s,
            other => panic!("worker must deliver the inbound stream, got {other:?}"),
        };
        // 第二批:跨 poll 再 4 帧
        for i in 4..=7u8 {
            f.feed(data_block(2, &[0xA0 + i; 100]));
        }
        match f.drive_recv() {
            Poll::Pending => {}
            other => panic!("no new stream expected, got {other:?}"),
        }
        let data = drain_stream(&mut stream, &mut f.cx).expect("ordered read");
        let mut expect = vec![0xA0u8];
        for i in 1..=7u8 {
            expect.extend_from_slice(&[0xA0 + i; 100]);
        }
        assert_eq!(data, expect, "流内顺序必须逐字节一致(FIFO)");
    }

    /// T-W2:SYN 建流交付 + wire 侧恰一个 ACK|WU 建流授信帧(对齐现行
    /// G2 语义,不重复授信)。
    #[test]
    fn worker_syn_delivers_and_grants_ack_wu() {
        let mut cfg = Config::default();
        cfg.set_initial_stream_receive_window(6 * 1024 * 1024);
        cfg.set_shard_workers(2);
        let (socket, st) = ScriptSocket::new();
        let conn = Connection::new(socket, cfg, Mode::Client);
        let (mut recv, mut send, mut workers, mut send_workers) = conn.into_halves();

        st.borrow_mut()
            .read_blocks
            .push_back(syn_data_block(2, b"x"));
        let mut cx = noop_cx();
        match recv.poll(&mut cx) {
            Poll::Pending => {} // 路由后 socket 空 → park
            other => panic!("unexpected recv outcome, got {other:?}"),
        }
        for w in workers.iter_mut() {
            let _ = w.poll_unpin(&mut cx);
        }
        let _stream = match recv.poll(&mut cx) {
            Poll::Ready(Some(Ok(s))) => s,
            other => panic!("worker-built stream must be delivered, got {other:?}"),
        };
        // round-6:入站流的授信命令(ACK|WU)经 send_regs 路由 SendWorker
        // 收编消费、压 send_out,再由 send(FlushTask)写上 wire
        for w in send_workers.iter_mut() {
            let _ = w.poll_unpin(&mut cx);
        }
        match send.poll(&mut cx) {
            Poll::Pending => {} // flush 后 park
            other => panic!("send half must park, got {other:?}"),
        }
        // 再驱动一轮双侧 worker/send 确认无第二帧
        for w in workers.iter_mut() {
            let _ = w.poll_unpin(&mut cx);
        }
        for w in send_workers.iter_mut() {
            let _ = w.poll_unpin(&mut cx);
        }
        match send.poll(&mut cx) {
            Poll::Pending => {}
            other => panic!("send half must stay parked, got {other:?}"),
        }

        let wus: Vec<_> = written_headers(&st)
            .into_iter()
            .filter(|h| h.tag() == Tag::WindowUpdate && h.stream_id() == StreamId::new(2))
            .collect();
        assert_eq!(wus.len(), 1, "exactly one WU for stream 2, got {wus:?}");
        assert!(
            wus[0].flags().contains(header::ACK),
            "creation grant must carry ACK (ACK|WU), got {:?}",
            wus[0].flags()
        );
        assert_eq!(
            wus[0].len().val(),
            6 * 1024 * 1024 - DEFAULT_CREDIT,
            "grant size must be the pregrant delta 6MiB − 256KiB"
        );
    }

    /// T-W3:重复 SYN → to_send 出现 GoAway::protocol_error;连接不断
    /// (后续帧仍处理)。
    #[test]
    fn worker_duplicate_syn_goaway_continues() {
        let mut f = Fixture::client(2, Config::default());
        f.feed(syn_data_block(2, b"one"));
        f.feed(syn_data_block(2, b"dup"));
        match f.drive_recv() {
            Poll::Ready(Some(Ok(_))) => {}
            other => panic!("first SYN must create the stream, got {other:?}"),
        }
        // 第二个 SYN 被 worker0 判重复 → GoAway(protocol_error)入 to_send
        match f.drive_recv() {
            Poll::Pending => {}
            other => panic!("duplicate SYN must not deliver a stream, got {other:?}"),
        }
        match f.drive_send() {
            Poll::Pending => {}
            other => panic!("send half must park after flushing GoAway, got {other:?}"),
        }
        let goaways: Vec<_> = written_headers(&f.st)
            .into_iter()
            .filter(|h| h.tag() == Tag::GoAway)
            .collect();
        assert_eq!(goaways.len(), 1, "one GoAway expected, got {goaways:?}");
        assert_eq!(
            goaways[0].len().val(),
            1,
            "GoAway must carry protocol_error code (1)"
        );

        // 连接不断:后续 SYN(新 sid)照常建流交付
        f.feed(syn_data_block(4, b"next"));
        match f.drive_recv() {
            Poll::Ready(Some(Ok(_))) => {}
            other => panic!("connection must keep serving after GoAway, got {other:?}"),
        }
    }

    /// T-W4:流数上限跨分片精确——N=2、max=4,2 出站(opening 计数)+
    /// 2 入站(worker0 计数)后,第 5 个 SYN → GoAway::internal_error +
    /// stream_count 回滚为 4。
    #[test]
    fn worker_stream_cap_exact_across_shards() {
        let mut cfg = Config::default();
        cfg.set_shard_workers(2);
        cfg.set_max_num_streams(4);
        let mut f = Fixture::client(2, cfg);

        // 2 条出站流(奇 sid → worker1 路由域;计数器全局 +2;存活到
        // 断言之后——立即 drop 会触发 on_drop 递减)
        let mut outbound = Vec::new();
        for _ in 0..2 {
            match f.send.poll_new_outbound(&mut f.cx) {
                Poll::Ready(Ok(s)) => outbound.push(s),
                other => panic!("outbound open must succeed, got {other:?}"),
            }
        }
        // 2 条入站流(偶 sid → worker0)
        let mut inbound = Vec::new();
        f.feed(syn_data_block(2, b"a"));
        f.feed(syn_data_block(4, b"b"));
        for _ in 0..2 {
            match f.drive_recv() {
                Poll::Ready(Some(Ok(s))) => inbound.push(s),
                other => panic!("inbound SYN must deliver, got {other:?}"),
            }
        }
        assert_eq!(f.shared.stream_count.load(Ordering::SeqCst), 4);

        // 第 5 个 SYN:fetch_add 后超界 → 回滚 + internal_error GoAway
        f.feed(syn_data_block(6, b"c"));
        match f.drive_recv() {
            Poll::Pending => {}
            other => panic!("over-cap SYN must not deliver, got {other:?}"),
        }
        match f.drive_send() {
            Poll::Pending => {}
            other => panic!("send half must park, got {other:?}"),
        }
        let goaways: Vec<_> = written_headers(&f.st)
            .into_iter()
            .filter(|h| h.tag() == Tag::GoAway)
            .collect();
        assert_eq!(goaways.len(), 1, "one GoAway expected");
        assert_eq!(
            goaways[0].len().val(),
            2,
            "GoAway must carry internal_error code (2), got {goaways:?}"
        );
        assert_eq!(
            f.shared.stream_count.load(Ordering::SeqCst),
            4,
            "rejected SYN must roll the counter back to 4"
        );
    }

    /// T-W5:ACK 收编——SendHalf 开流(opening 注册)→ ACK|WU(credit)
    /// 经真实路由到 worker → 收编、send_window 增、is_pending_ack 清、
    /// ack_backlog_count 减、new_outbound_stream_waker 被唤醒;后续该流
    /// 的 Data 帧直接命中本地表。
    #[test]
    fn worker_ack_adopts_opening_stream() {
        let mut cfg = Config::default();
        cfg.set_shard_workers(2);
        let mut f = Fixture::client(2, cfg);

        let mut stream = match f.send.poll_new_outbound(&mut f.cx) {
            Poll::Ready(Ok(s)) => s,
            other => panic!("outbound open must succeed, got {other:?}"),
        };
        assert_eq!(stream.id(), StreamId::new(1));
        // 开流后再登记 backlog 等待 waker(开流自身会覆盖槽位)
        let cw = Arc::new(CountWaker::default());
        let w = waker(cw.clone());
        *f.shared.new_outbound_stream_waker.lock() = Some(w);
        assert_eq!(f.shared.ack_backlog_count.load(Ordering::SeqCst), 1);

        // ACK|WU(credit=4096)→ 路由 worker[1%2=1] → 收编 + 确认 + 授信
        f.feed(wu_ack_credit_block(1, 4096));
        match f.recv.poll(&mut f.cx) {
            Poll::Pending => {}
            other => panic!("recv must park after routing, got {other:?}"),
        }
        for w2 in f.workers.iter_mut() {
            let _ = w2.poll_unpin(&mut f.cx);
        }
        assert!(
            !stream.is_pending_ack(),
            "ACK must clear pending-ack on the adopted stream"
        );
        assert_eq!(
            f.shared.ack_backlog_count.load(Ordering::SeqCst),
            0,
            "ack_backlog_count must decrement on ACK"
        );
        assert_eq!(
            stream.shared().send_window(),
            DEFAULT_CREDIT + 4096,
            "WU credit must land on the send window"
        );
        assert!(
            cw.0.load(Ordering::SeqCst) >= 1,
            "new_outbound_stream_waker must be woken by the ACK"
        );

        // 收编后该流数据直接命中 worker1 本地表(可读)
        f.feed(data_block(1, b"adopted"));
        match f.recv.poll(&mut f.cx) {
            Poll::Pending => {}
            other => panic!("recv must park after dispatch, got {other:?}"),
        }
        for w2 in f.workers.iter_mut() {
            let _ = w2.poll_unpin(&mut f.cx);
        }
        let mut buf = [0u8; 16];
        let n = match Pin::new(&mut stream).poll_read(&mut f.cx, &mut buf) {
            Poll::Ready(Ok(n)) => n,
            other => panic!("data must be readable after adoption, got {other:?}"),
        };
        assert_eq!(&buf[..n], b"adopted");
    }

    /// T-W6:半关闭——Data+FIN → 读半先得数据再 EOF;写半仍可写
    /// (RecvClosed 下 can_write)→ 写命令入队;RST → Closed + 双 waker
    /// 唤醒。
    #[test]
    fn worker_half_close_fin_and_rst() {
        let mut f = Fixture::client(2, Config::default());
        f.feed(syn_data_block(2, b"head"));
        let mut stream = match f.drive_recv() {
            Poll::Ready(Some(Ok(s))) => s,
            other => panic!("inbound stream expected, got {other:?}"),
        };
        // Data+FIN:数据先行、随后 EOF
        f.feed(data_fin_block(2, b"tail"));
        match f.drive_recv() {
            Poll::Pending => {}
            other => panic!("no new stream expected, got {other:?}"),
        }
        // 写半仍可写:poll_write 入队成功(RecvClosed 不影响 can_write)
        match Pin::new(&mut stream).poll_write(&mut f.cx, b"reply") {
            Poll::Ready(Ok(5)) => {}
            other => panic!("write half must stay writable after remote FIN, got {other:?}"),
        }
        let data = drain_stream(&mut stream, &mut f.cx).expect("read after FIN");
        assert_eq!(data, b"headtail".to_vec(), "FIN 前数据必须先于 EOF 交付");

        // RST → Closed + 双 waker 唤醒(预置 reader/writer waker 观察)
        #[derive(Default)]
        struct FlagWaker(AtomicBool);
        impl ArcWake for FlagWaker {
            fn wake_by_ref(me: &Arc<Self>) {
                me.0.store(true, Ordering::SeqCst);
            }
        }
        let rw = Arc::new(FlagWaker::default());
        let ww = Arc::new(FlagWaker::default());
        {
            let mut shared = stream.shared();
            shared.reader = Some(waker(rw.clone()));
            shared.writer = Some(waker(ww.clone()));
        }
        f.feed(data_rst_block(2));
        match f.drive_recv() {
            Poll::Pending => {}
            other => panic!("no new stream expected on RST, got {other:?}"),
        }
        assert!(stream.is_closed(), "RST must close the stream");
        assert!(rw.0.load(Ordering::SeqCst), "reader waker must be woken");
        assert!(ww.0.load(Ordering::SeqCst), "writer waker must be woken");
    }

    /// T-W7:未知流静默丢弃——对端奇偶 sid 无流 → Data/WU/Ping 丢弃,
    /// 无 GoAway、无 RST、无 Pong(paritytech#110)。
    #[test]
    fn worker_unknown_stream_silent_drop() {
        let mut f = Fixture::client(2, Config::default());
        f.feed(data_block(6, b"x"));
        f.feed(wu_credit_block(6, 100));
        f.feed(ping_sid_block(6, 7));
        match f.drive_recv() {
            Poll::Pending => {}
            other => panic!("unknown-stream frames must not deliver, got {other:?}"),
        }
        match f.drive_send() {
            Poll::Pending => {}
            other => panic!("send half must park with nothing to answer, got {other:?}"),
        }
        let hdrs = written_headers(&f.st);
        assert!(
            hdrs.iter().all(|h| h.tag() != Tag::GoAway),
            "no GoAway allowed for unknown streams, got {hdrs:?}"
        );
        assert!(
            hdrs.iter().all(|h| h.stream_id() != StreamId::new(6)),
            "no RST/Pong for stream 6 allowed, got {hdrs:?}"
        );
    }

    /// T-W8:Remove(ctrl)三态矩阵——Open→RST、RecvClosed→FIN、
    /// SendClosed→无帧;本地表移除;计数器递减。
    #[test]
    fn worker_ctrl_remove_frame_matrix() {
        let mut f = Fixture::client(2, Config::default());
        // 三条入站流:sid2(Open)、sid4(RecvClosed)、sid6(SendClosed)
        f.feed(syn_data_block(2, b"a"));
        f.feed(syn_data_block(4, b"b"));
        f.feed(syn_data_block(6, b"c"));
        let s2 = match f.drive_recv() {
            Poll::Ready(Some(Ok(s))) => s,
            other => panic!("stream 2 expected, got {other:?}"),
        };
        let s4 = match f.drive_recv() {
            Poll::Ready(Some(Ok(s))) => s,
            other => panic!("stream 4 expected, got {other:?}"),
        };
        let mut s6 = match f.drive_recv() {
            Poll::Ready(Some(Ok(s))) => s,
            other => panic!("stream 6 expected, got {other:?}"),
        };
        assert_eq!(f.shared.stream_count.load(Ordering::SeqCst), 3);

        // sid4 → RecvClosed(对端 FIN)
        f.feed(data_fin_block(4, b""));
        match f.drive_recv() {
            Poll::Pending => {}
            other => panic!("unexpected delivery, got {other:?}"),
        }
        // sid6 → SendClosed(本端 poll_close 排 FIN;状态即刻 SendClosed)
        match Pin::new(&mut s6).poll_close(&mut f.cx) {
            Poll::Ready(Ok(())) => {}
            other => panic!("close must be ready, got {other:?}"),
        }
        match f.drive_send() {
            Poll::Pending => {}
            other => panic!("send half must flush the FIN, got {other:?}"),
        }

        // 逐流 drop → (id,None) → SendHalf on_drop → ctrl Remove → Worker
        let baseline = f.shared.stream_count.load(Ordering::SeqCst);
        assert_eq!(baseline, 3);
        drop(s2); // Open → RST
        for _ in 0..4 {
            match f.drive_send() {
                Poll::Pending => {}
                other => panic!("send half must park, got {other:?}"),
            }
        }
        let rst: Vec<_> = written_headers(&f.st)
            .into_iter()
            .filter(|h| h.stream_id() == StreamId::new(2) && h.flags().contains(header::RST))
            .collect();
        assert_eq!(rst.len(), 1, "Open → exactly one RST, got {rst:?}");
        assert_eq!(
            f.shared.stream_count.load(Ordering::SeqCst),
            baseline - 1,
            "stream_count must decrement on Remove"
        );

        let before = written_headers(&f.st).len();
        drop(s4); // RecvClosed → FIN
        for _ in 0..4 {
            match f.drive_send() {
                Poll::Pending => {}
                other => panic!("send half must park, got {other:?}"),
            }
        }
        let fin: Vec<_> = written_headers(&f.st)
            .into_iter()
            .skip(before)
            .filter(|h| h.stream_id() == StreamId::new(4) && h.flags().contains(header::FIN))
            .collect();
        assert_eq!(fin.len(), 1, "RecvClosed → exactly one FIN, got {fin:?}");
        assert_eq!(f.shared.stream_count.load(Ordering::SeqCst), baseline - 2);

        let before = written_headers(&f.st).len();
        drop(s6); // SendClosed → 无帧
        for _ in 0..4 {
            match f.drive_send() {
                Poll::Pending => {}
                other => panic!("send half must park, got {other:?}"),
            }
        }
        let after: Vec<_> = written_headers(&f.st).into_iter().skip(before).collect();
        assert!(
            after.is_empty(),
            "SendClosed → no frame expected, got {after:?}"
        );
        assert_eq!(
            f.shared.stream_count.load(Ordering::SeqCst),
            baseline - 3,
            "all removes must decrement the counter"
        );
    }

    /// T-W9:错误广播——set_error → Worker 排空本地表(全部 Closed+
    /// 唤醒)→ Future Ready 终结。终结条件含 Framer 路由终结宣告
    /// (framer_flushed;生产侧由 RecvHalf 终结排空/Drop 置位,本测试
    /// 直接驱动 Worker,模拟其已完成冲洗)。
    #[test]
    fn worker_error_broadcast_drains_and_finishes() {
        let mut f = Fixture::client(2, Config::default());
        f.feed(syn_data_block(2, b"x"));
        let stream = match f.drive_recv() {
            Poll::Ready(Some(Ok(s))) => s,
            other => panic!("inbound stream expected, got {other:?}"),
        };
        // 预置 reader waker 观察排空唤醒
        #[derive(Default)]
        struct FlagWaker(AtomicBool);
        impl ArcWake for FlagWaker {
            fn wake_by_ref(me: &Arc<Self>) {
                me.0.store(true, Ordering::SeqCst);
            }
        }
        let rw = Arc::new(FlagWaker::default());
        stream.shared().reader = Some(waker(rw.clone()));

        f.shared.set_error(ConnectionError::Closed);
        // Framer 路由终结宣告 + 广播(等价 RecvHalf 终结排空序:
        // 置位后唤醒等待中的 Worker)
        f.shared.framer_flushed.store(true, Ordering::SeqCst);
        f.shared.wake_all_workers();
        // workers 被 worker_wakers 唤醒(此处手动驱动):排空 + Ready
        let mut finished = 0;
        for w in f.workers.iter_mut() {
            match w.poll_unpin(&mut f.cx) {
                Poll::Ready(()) => finished += 1,
                Poll::Pending => {}
            }
        }
        assert_eq!(
            finished,
            f.workers.len(),
            "all workers must finish on error"
        );
        assert!(stream.is_closed(), "local table must be drained Closed");
        assert!(rw.0.load(Ordering::SeqCst), "reader waker must be woken");
        // 终结幂等
        for w in f.workers.iter_mut() {
            assert!(w.poll_unpin(&mut f.cx).is_ready());
        }
    }

    /// T-W10:send_dropped——置位 → 排空本地表但**不终结**,后续 SYN
    /// 仍建流。
    #[test]
    fn worker_send_dropped_drains_but_serves() {
        let mut f = Fixture::client(2, Config::default());
        f.feed(syn_data_block(2, b"x"));
        let stream = match f.drive_recv() {
            Poll::Ready(Some(Ok(s))) => s,
            other => panic!("inbound stream expected, got {other:?}"),
        };
        // 等价 SendHalf::drop 的信号面:置位 + wake_all_workers 广播
        f.shared.send_dropped.store(true, Ordering::SeqCst);
        f.shared.wake_all_workers();
        for w in f.workers.iter_mut() {
            match w.poll_unpin(&mut f.cx) {
                Poll::Pending => {}
                Poll::Ready(()) => panic!("worker must NOT finish on send_dropped alone"),
            }
        }
        assert!(stream.is_closed(), "local table must be drained once");

        // 后续 SYN 照常建流
        f.feed(syn_data_block(4, b"y"));
        let mut stream2 = match f.drive_recv() {
            Poll::Ready(Some(Ok(s))) => s,
            other => panic!("new SYN must still create a stream, got {other:?}"),
        };
        let data = drain_stream(&mut stream2, &mut f.cx).expect("read new stream");
        assert_eq!(data, b"y".to_vec());
    }

    /// T-W11:流级 Ping——命中 → Pong;未命中 → 丢弃。
    #[test]
    fn worker_stream_ping_pong() {
        let mut f = Fixture::client(2, Config::default());
        f.feed(syn_data_block(2, b"x"));
        match f.drive_recv() {
            Poll::Ready(Some(Ok(_))) => {}
            other => panic!("inbound stream expected, got {other:?}"),
        }
        // 命中:Pong(带 ACK、回显 opaque id);未命中(偶 sid 无流):丢弃
        f.feed(ping_sid_block(2, 0x1234));
        f.feed(ping_sid_block(8, 0x5678));
        match f.drive_recv() {
            Poll::Pending => {}
            other => panic!("pings must not deliver streams, got {other:?}"),
        }
        match f.drive_send() {
            Poll::Pending => {}
            other => panic!("send half must park after Pong, got {other:?}"),
        }
        let pongs: Vec<_> = written_headers(&f.st)
            .into_iter()
            .filter(|h| h.tag() == Tag::Ping && h.flags().contains(header::ACK))
            .collect();
        assert_eq!(pongs.len(), 1, "exactly one Pong, got {pongs:?}");
        // Pong 由 Header::ping 构造(现行行为):sid 恒为 0,opaque id
        // 回显在 length 字段
        assert_eq!(pongs[0].stream_id(), StreamId::new(0));
        assert_eq!(pongs[0].len().val(), 0x1234, "Pong must echo the opaque id");
    }

    /// T-W12(复核修复 1,D4 精确性):未确认出站流离开 Open{ack:false}
    /// 的每条路径都必须释放 ack_backlog_count——本地 poll_close 半关闭
    /// (中继收尾常态:先于对端 ACK 到达)、对端无 ACK 的 FIN、对端 RST。
    /// 修前仅 ACK 分支与删除路径递减,本地半关闭泄漏 MAX_ACK_BACKLOG
    /// 条后 poll_new_outbound 永久 Pending。
    #[test]
    fn worker_ack_backlog_released_without_ack() {
        // (a) 本地 poll_close_half(u_w.shutdown 收尾路径)先于对端 ACK
        {
            let mut f = Fixture::client(2, Config::default());
            let s = match f.send.poll_new_outbound(&mut f.cx) {
                Poll::Ready(Ok(s)) => s,
                other => panic!("outbound open must succeed, got {other:?}"),
            };
            assert_eq!(f.shared.ack_backlog_count.load(Ordering::SeqCst), 1);
            let (_read_half, mut write_half) = s.into_halves();
            match write_half.poll_close_half(&mut f.cx) {
                Poll::Ready(Ok(())) => {}
                other => panic!("close half must be ready, got {other:?}"),
            }
            assert_eq!(
                f.shared.ack_backlog_count.load(Ordering::SeqCst),
                0,
                "local close must release the pending-ack slot"
            );
        }
        // (b) 对端无 ACK 的 FIN → 收编 + RecvClosed,同样释放
        {
            let mut f = Fixture::client(2, Config::default());
            let _s = match f.send.poll_new_outbound(&mut f.cx) {
                Poll::Ready(Ok(s)) => s,
                other => panic!("outbound open must succeed, got {other:?}"),
            };
            assert_eq!(f.shared.ack_backlog_count.load(Ordering::SeqCst), 1);
            f.feed(data_fin_block(1, b""));
            match f.recv.poll(&mut f.cx) {
                Poll::Pending => {}
                other => panic!("recv must park after routing, got {other:?}"),
            }
            for w in f.workers.iter_mut() {
                let _ = w.poll_unpin(&mut f.cx);
            }
            assert_eq!(
                f.shared.ack_backlog_count.load(Ordering::SeqCst),
                0,
                "peer FIN without ACK must release the pending-ack slot"
            );
        }
        // (c) 对端 RST → 收编 + Closed,同样释放
        {
            let mut f = Fixture::client(2, Config::default());
            let _s = match f.send.poll_new_outbound(&mut f.cx) {
                Poll::Ready(Ok(s)) => s,
                other => panic!("outbound open must succeed, got {other:?}"),
            };
            f.feed(data_rst_block(1));
            match f.recv.poll(&mut f.cx) {
                Poll::Pending => {}
                other => panic!("recv must park after routing, got {other:?}"),
            }
            for w in f.workers.iter_mut() {
                let _ = w.poll_unpin(&mut f.cx);
            }
            assert_eq!(
                f.shared.ack_backlog_count.load(Ordering::SeqCst),
                0,
                "peer RST must release the pending-ack slot"
            );
        }
    }

    /// T-W13(复核修复 2,§0.3-2/§0.3-3):Framer 读到 EOF 前已路由的帧
    /// 必须在 Worker 终结排空中全部处理——数据落缓冲、建流(含超出
    /// new_streams 容量的暂存)全部交付,recv 才返回终结。修前 Worker
    /// 见错误标志即丢整批在途帧、recv 先 finished 致建流句柄丢失。
    #[test]
    fn worker_eof_flushes_routed_frames_and_streams() {
        let mut f = Fixture::client(2, Config::default());
        // 20 个 SYN(> new_streams cap 16:终结排空必须把暂存也全部交付)
        for k in 1..=20u32 {
            f.feed(syn_data_block(2 * k, b"m"));
        }
        f.feed(data_fin_block(2, b"fin-tail"));
        f.st.borrow_mut().eof = true;
        let mut streams = Vec::new();
        let mut done = false;
        for _ in 0..64 {
            match f.recv.poll(&mut f.cx) {
                Poll::Ready(Some(Ok(s))) => streams.push(s),
                Poll::Ready(None) => {
                    done = true;
                    break;
                }
                Poll::Pending => {}
                other => panic!("unexpected recv outcome, got {other:?}"),
            }
            for w in f.workers.iter_mut() {
                let _ = w.poll_unpin(&mut f.cx);
            }
        }
        assert!(done, "recv must converge to Ready(None)");
        assert_eq!(
            streams.len(),
            20,
            "all routed SYNs must be delivered before the terminal"
        );
        // read_after_close:终结排空(Closed)后缓冲数据仍可读
        let s2 = &mut streams[0];
        assert_eq!(s2.id(), StreamId::new(2));
        let data = drain_stream(s2, &mut f.cx).expect("read after close");
        assert_eq!(
            data,
            b"mfin-tail".to_vec(),
            "routed data+FIN must land in the buffer"
        );
        // 全部 Worker 已终结(终结排空收敛的另一半)
        for w in f.workers.iter_mut() {
            assert!(w.poll_unpin(&mut f.cx).is_ready(), "workers must finish");
        }
    }

    /// T-W14(复核修复 3,D4 stream_count 精确性):SendHalf Drop 排空
    /// 窗口内 Worker 本地表的流计数必须回收——修前 drain_local_streams
    /// 只 Closed 不减计数,存活期新 SYN 提前误触 max_num_streams。
    #[test]
    fn worker_send_drop_releases_stream_count() {
        let mut cfg = Config::default();
        cfg.set_shard_workers(2);
        cfg.set_max_num_streams(3);
        let mut f = Fixture::client(2, cfg);
        // 3 条入站流打满上限(计数 3)
        f.feed(syn_data_block(2, b"a"));
        f.feed(syn_data_block(4, b"b"));
        f.feed(syn_data_block(6, b"c"));
        let mut inbound = Vec::new();
        for _ in 0..3 {
            match f.drive_recv() {
                Poll::Ready(Some(Ok(s))) => inbound.push(s),
                other => panic!("inbound SYN must deliver, got {other:?}"),
            }
        }
        assert_eq!(f.shared.stream_count.load(Ordering::SeqCst), 3);
        // 等价 SendHalf::drop 的信号面:置位 + wake_all_workers 广播;
        // Worker 排空本地表必须同步释放计数
        f.shared.send_dropped.store(true, Ordering::SeqCst);
        f.shared.wake_all_workers();
        for w in f.workers.iter_mut() {
            let _ = w.poll_unpin(&mut f.cx);
        }
        assert_eq!(
            f.shared.stream_count.load(Ordering::SeqCst),
            0,
            "drained local tables must release stream_count"
        );
        // 继续服务:上限 3 下新 SYN 必须照常建流(计数虚高 3 时第 4 个
        // SYN 会 fetch_add 越界 → GoAway internal_error,不交付)
        f.feed(syn_data_block(8, b"d"));
        match f.drive_recv() {
            Poll::Ready(Some(Ok(_))) => {}
            other => panic!("new SYN must still be accepted after send drop, got {other:?}"),
        }
    }

    /// T-W15(复核修复 4,D6 背压链):new_streams 满(暂存非空)时
    /// Worker 不得继续排空分片通道——通道滞留 → Framer 停读 → TCP 背压,
    /// 端到端有界。修前 Worker 无条件排空,建流句柄无限堆进无界暂存。
    /// 门控态经真实路径建立(20 SYN → 17 入通道、3 暂存);随后直接经
    /// 生产者句柄压入新消息(RecvHalf 每次 poll 先交付一条流,真实路径
    /// 在门控未解除时到不了 socket 读,故白盒直压)断言不被消费。
    #[test]
    fn worker_new_streams_full_gates_shard_drain() {
        let mut cfg = Config::default();
        cfg.set_shard_workers(1);
        let mut f = Fixture::client(1, cfg);
        // 首批 20 个 SYN:17 进 new_streams(16 buffer + 1 sender 在途槽),
        // 3 滞暂存 → 门控生效
        for k in 1..=20u32 {
            f.feed(syn_data_block(2 * k, b"x"));
        }
        match f.recv.poll(&mut f.cx) {
            Poll::Pending => {}
            other => panic!("recv must park after routing, got {other:?}"),
        }
        for w in f.workers.iter_mut() {
            let _ = w.poll_unpin(&mut f.cx);
        }
        assert_eq!(
            f.workers[0].pending_new.len(),
            3,
            "17 delivered into new_streams, 3 parked"
        );
        // 门控下直压 5 条消息:分片通道必须滞留(Worker 不消费)
        let mk_msg = |sid: u32| -> ShardMsg {
            let h = Header::data(StreamId::new(sid), 0);
            ShardMsg::Frame(Frame::new(h).into())
        };
        {
            let workers = f.shared.workers.lock();
            for i in 0..5u32 {
                match workers[0].0.try_push(mk_msg(2 + i)) {
                    Ok(()) => {}
                    Err(_) => panic!("channel has room (cap 128)"),
                }
            }
        }
        for _ in 0..4 {
            for w in f.workers.iter_mut() {
                let _ = w.poll_unpin(&mut f.cx);
            }
        }
        assert!(
            f.workers[0].msg_rx.has_pending(),
            "gated worker must leave the shard channel unconsumed"
        );
        assert_eq!(
            f.workers[0].pending_new.len(),
            3,
            "pending_new must not grow while gated"
        );
        // 消费侧恢复(recv 交付)后门控解除:通道排空、暂存清空;直压的
        // 5 条 Data 帧不建流(消费即释),全部 20 条流最终逐条交付
        let mut collected = Vec::new();
        let mut guard = 0;
        loop {
            guard += 1;
            assert!(guard < 512, "drain must converge");
            let mut progressed = false;
            match f.recv.poll(&mut f.cx) {
                Poll::Ready(Some(Ok(s))) => {
                    collected.push(s);
                    progressed = true;
                }
                Poll::Pending => {}
                other => panic!("unexpected recv outcome, got {other:?}"),
            }
            for w in f.workers.iter_mut() {
                let _ = w.poll_unpin(&mut f.cx);
            }
            if !progressed
                && !f.workers[0].msg_rx.has_pending()
                && f.workers[0].pending_new.is_empty()
            {
                break;
            }
        }
        assert_eq!(collected.len(), 20, "all streams must eventually deliver");
    }

    /// T-W16(复核修复 R2-1,§0.3-3):已路由进分片通道、其流尚未被
    /// Worker 收编的**出站流首帧**(对端响应 Data|ACK 与 EOF 同一读突发
    /// 到达)必须在终结排空中完成收编落缓冲——修前 fatal 入口同步排空
    /// opening,Worker 终结排空时 lookup 落空按 unknown stream 丢弃,应用
    /// 读该流得 EOF 却无响应数据(基线全局表命中 → buffer.push 后才
    /// Closed)。
    #[test]
    fn worker_fatal_adopts_pending_opening_frames() {
        let mut f = Fixture::client(2, Config::default());
        let mut s = match f.send.poll_new_outbound(&mut f.cx) {
            Poll::Ready(Ok(s)) => s,
            other => panic!("outbound open must succeed, got {other:?}"),
        };
        assert_eq!(s.id(), StreamId::new(1));
        assert_eq!(f.shared.ack_backlog_count.load(Ordering::SeqCst), 1);
        // 对端响应首帧(Data|ACK + 体)与 EOF 同一读突发
        f.feed(data_ack_block(1, b"resp"));
        f.st.borrow_mut().eof = true;
        let mut done = false;
        for _ in 0..16 {
            match f.recv.poll(&mut f.cx) {
                Poll::Ready(None) => {
                    done = true;
                    break;
                }
                Poll::Pending => {}
                other => panic!("unexpected recv outcome, got {other:?}"),
            }
            for w in f.workers.iter_mut() {
                let _ = w.poll_unpin(&mut f.cx);
            }
        }
        assert!(done, "recv must converge to Ready(None)");
        // 收编成功:响应体落缓冲(read_after_close),ACK 释放 backlog,
        // 终结后状态 Closed,opening 已清、计数归零
        let data = drain_stream(&mut s, &mut f.cx).expect("read after close");
        assert_eq!(
            data,
            b"resp".to_vec(),
            "routed Data|ACK for an un-adopted outbound stream must land in its buffer"
        );
        assert!(s.is_closed(), "stream must be Closed after teardown");
        assert_eq!(f.shared.ack_backlog_count.load(Ordering::SeqCst), 0);
        assert!(
            f.shared.opening.lock().is_empty(),
            "opening must be drained at finish"
        );
        assert_eq!(f.shared.stream_count.load(Ordering::SeqCst), 0);
    }

    /// T-W17(复核修复 R2-2,§2.6):错误由 SendHalf 产出、RecvHalf 既不
    /// poll 也不 drop(阻塞在 accept 背压)时,Worker 仍须在首次 poll 即
    /// 关闭本地表与 opening 本分片流的状态——读端立即得 EOF(缓冲数据
    /// 仍可读),不依赖 framer_flushed(基线 SendHalf::fatal 同步关全表);
    /// 摘表/计数/Future 终结可延后到 RecvHalf 收敛。
    #[test]
    fn worker_closes_states_on_error_without_recv_progress() {
        let mut cfg = Config::default();
        cfg.set_shard_workers(2);
        let (socket, st) = ScriptSocket::new();
        let conn = Connection::new(socket, cfg, Mode::Client);
        let (mut recv, mut send, mut workers, _send_workers) = conn.into_halves();
        let shared = recv.shared_for_test();
        let mut cx = noop_cx();

        // 一条入站流(本地表)+ 一条出站流(opening,未收编)
        st.borrow_mut()
            .read_blocks
            .push_back(syn_data_block(2, b"in"));
        match recv.poll(&mut cx) {
            Poll::Pending => {}
            other => panic!("recv must park after routing, got {other:?}"),
        }
        for w in workers.iter_mut() {
            let _ = w.poll_unpin(&mut cx);
        }
        let mut inbound = match recv.poll(&mut cx) {
            Poll::Ready(Some(Ok(s))) => s,
            other => panic!("inbound stream expected, got {other:?}"),
        };
        let outbound = match send.poll_new_outbound(&mut cx) {
            Poll::Ready(Ok(s)) => s,
            other => panic!("outbound open must succeed, got {other:?}"),
        };
        #[derive(Default)]
        struct FlagWaker(AtomicBool);
        impl ArcWake for FlagWaker {
            fn wake_by_ref(me: &Arc<Self>) {
                me.0.store(true, Ordering::SeqCst);
            }
        }
        let rw_in = Arc::new(FlagWaker::default());
        let rw_out = Arc::new(FlagWaker::default());
        inbound.shared().reader = Some(waker(rw_in.clone()));
        outbound.shared().reader = Some(waker(rw_out.clone()));

        // 等价 SendHalf::fatal 的信号面:只置错误;RecvHalf 全程不 poll、
        // 不 drop(framer_flushed 保持 false)
        shared.set_error(ConnectionError::Closed);
        for w in workers.iter_mut() {
            match w.poll_unpin(&mut cx) {
                Poll::Pending => {} // 等 framer_flushed,但状态已关
                Poll::Ready(()) => panic!("worker must not finish before framer_flushed"),
            }
        }
        assert!(
            !shared.framer_flushed.load(Ordering::SeqCst),
            "precondition: recv side made no progress"
        );
        assert!(
            inbound.is_closed(),
            "local-table stream must be Closed promptly"
        );
        assert!(
            outbound.is_closed(),
            "opening stream must be Closed promptly"
        );
        assert!(
            rw_in.0.load(Ordering::SeqCst),
            "inbound reader must be woken"
        );
        assert!(
            rw_out.0.load(Ordering::SeqCst),
            "outbound reader must be woken"
        );
        // read_after_close:缓冲数据仍可读,随后 EOF
        let data = drain_stream(&mut inbound, &mut cx).expect("read after close");
        assert_eq!(data, b"in".to_vec());
        // 及时关闭面不摘表/不减计数(延后到终结);opening 条目仍在供收编
        assert_eq!(
            shared.opening.lock().len(),
            1,
            "opening entry kept for adoption"
        );
        assert_eq!(shared.stream_count.load(Ordering::SeqCst), 2);

        // RecvHalf 收敛(以 Drop 模拟 recv_driver 退出):Worker 终结,
        // 摘表 + opening 本分片清空 + 计数归零
        drop(recv);
        let mut finished = 0;
        for w in workers.iter_mut() {
            if w.poll_unpin(&mut cx).is_ready() {
                finished += 1;
            }
        }
        assert_eq!(
            finished,
            workers.len(),
            "workers finish once recv converges"
        );
        assert!(shared.opening.lock().is_empty());
        assert_eq!(shared.stream_count.load(Ordering::SeqCst), 0);
    }

    // ===== round-6 §5.2 SendWorker(单测,N=2 构造跨分片)=====

    /// T-S1:注册路由——8 条出站流 → 驱动 SendWorker → 各 Worker 的
    /// SelectAll 恰含 sid%N==k 的接收器;命令不丢(收编后首条命令经
    /// 通道 FIFO 消费、帧上线)。
    #[test]
    fn send_worker_registration_routes_by_sid_mod_n() {
        let mut f = Fixture::client(2, Config::default());
        let mut outbound = Vec::new();
        for _ in 0..8 {
            let s = match f.send.poll_new_outbound(&mut f.cx) {
                Poll::Ready(Ok(s)) => s,
                other => panic!("outbound open must succeed, got {other:?}"),
            };
            outbound.push(s);
        }
        // client 出站 sid 恒奇数:1,3,…,15 → % 2 == 1 全归 SendWorker 1
        f.poll_send_workers();
        assert_eq!(
            f.send_workers[0].streams.len(),
            0,
            "even shard must adopt no receivers"
        );
        assert_eq!(
            f.send_workers[1].streams.len(),
            8,
            "odd shard must adopt all 8 receivers"
        );
        // 命令不丢:收编后的首条写命令经通道 FIFO 消费、帧经 send_out 上线
        match Pin::new(&mut outbound[0]).poll_write(&mut f.cx, b"cmd") {
            Poll::Ready(Ok(3)) => {}
            other => panic!("write after adoption must be ready, got {other:?}"),
        }
        match f.drive_send() {
            Poll::Pending => {}
            other => panic!("flush task must park after flushing, got {other:?}"),
        }
        let data: Vec<_> = written_headers(&f.st)
            .into_iter()
            .filter(|h| h.stream_id() == StreamId::new(1) && h.tag() == Tag::Data)
            .collect();
        assert_eq!(
            data.len(),
            1,
            "the adopted stream's command must reach the wire via send_out"
        );
    }

    /// T-S2(红→绿):流内顺序——同一流 8 个数据帧 + CloseStream,跨多轮
    /// poll(FlushTask 先行空转穿插)→ wire 字节序:8 帧体逐字节一致且
    /// FIN 在最后(顺序契约:同流事件只经 send_out[k] 单 FIFO,红线 5)。
    #[test]
    fn send_worker_stream_order_across_polls() {
        let mut f = Fixture::client(2, Config::default());
        let mut s = match f.send.poll_new_outbound(&mut f.cx) {
            Poll::Ready(Ok(s)) => s,
            other => panic!("outbound open must succeed, got {other:?}"),
        };
        assert_eq!(s.id(), StreamId::new(1));
        let payloads: Vec<Vec<u8>> = (0..8u8).map(|i| vec![0xA0 + i; 256]).collect();
        for p in &payloads {
            match Pin::new(&mut s).poll_write(&mut f.cx, &p[..]) {
                Poll::Ready(Ok(256)) => {}
                other => panic!("stream write must be ready, got {other:?}"),
            }
        }
        match Pin::new(&mut s).poll_close(&mut f.cx) {
            Poll::Ready(Ok(())) => {}
            other => panic!("close must be ready, got {other:?}"),
        }
        // 多轮交替驱动(FlushTask 先行空转 → SendWorker 消费 → FlushTask
        // 写穿),模拟跨轮 poll 的交错;每轮终态必须 park(不空转)
        for _ in 0..4 {
            match f.send.poll(&mut f.cx) {
                Poll::Pending => {}
                other => panic!("flush task must park between rounds, got {other:?}"),
            }
            f.poll_send_workers();
            match f.send.poll(&mut f.cx) {
                Poll::Pending => {}
                other => panic!("flush task must park after draining, got {other:?}"),
            }
        }
        let s1: Vec<_> = frames_on_wire(&f.st)
            .into_iter()
            .filter(|(h, _)| h.stream_id() == StreamId::new(1))
            .collect();
        assert_eq!(s1.len(), 9, "8 data frames + FIN expected, got {s1:?}");
        for i in 0..8 {
            assert_eq!(s1[i].0.tag(), Tag::Data, "frame #{i} must be data");
            assert_eq!(
                &s1[i].1,
                &payloads[i][..],
                "frame #{i} body must be byte-identical in order"
            );
        }
        let (h_fin, b_fin) = &s1[8];
        assert!(
            h_fin.flags().contains(header::FIN),
            "FIN must come last on the wire, got {h_fin:?}"
        );
        assert_eq!(b_fin.len(), 0);
        assert!(
            s1[0].0.flags().contains(header::SYN),
            "lazy SYN must ride the first frame"
        );
    }

    /// T-S3(红→绿):send_out 有界——FlushTask 不消费时 SendWorker 压满
    /// cap(128)+ 单槽 pending 后 poll Pending(不丢帧);FlushTask 排空
    /// 后生产者被唤醒(WakeFlag),滞留帧与命令最终全部上线且顺序一致。
    #[test]
    fn send_worker_send_out_bounded_and_recovery() {
        let mut cfg = Config::default();
        cfg.set_shard_workers(1);
        let mut f = Fixture::client(1, cfg);
        let mut s = match f.send.poll_new_outbound(&mut f.cx) {
            Poll::Ready(Ok(s)) => s,
            other => panic!("outbound open must succeed, got {other:?}"),
        };
        let payload = [7u8; 64];
        // 阶段 1(饱和):只驱动 SendWorker(FlushTask 全程不消费),应用
        // 侧写至通道满,直至 pending_frame 单槽滞留
        let mut queued = 0usize;
        let mut rounds = 0usize;
        loop {
            rounds += 1;
            assert!(rounds < 512, "must saturate send_out within bounded rounds");
            loop {
                match Pin::new(&mut s).poll_write(&mut f.cx, &payload) {
                    Poll::Ready(Ok(64)) => queued += 1,
                    Poll::Pending => break,
                    other => panic!("unexpected stream write: {other:?}"),
                }
            }
            match f.send_workers[0].poll_unpin(&mut f.cx) {
                Poll::Pending => {}
                other => panic!("worker must park once saturated, got {other:?}"),
            }
            if f.send_workers[0].pending_frame.is_some() {
                break;
            }
        }
        // 饱和证据:send_out 恰 cap + pending 单槽(≥ 129 条命令已产出)
        assert!(
            queued > SEND_OUT_CAP,
            "saturation must cover a full channel plus the pending slot, queued {queued}"
        );
        let wire_before = frames_on_wire(&f.st)
            .iter()
            .filter(|(h, _)| h.tag() == Tag::Data)
            .count();
        assert_eq!(
            wire_before, 0,
            "flush task must not have consumed anything yet"
        );
        // 阶段 2(park 防线):以 WakeFlag 复 poll 登记 producer waker
        let fw = Arc::new(WakeFlag::default());
        let w = waker(fw.clone());
        let mut cx2 = Context::from_waker(&w);
        match f.send_workers[0].poll_unpin(&mut cx2) {
            Poll::Pending => {}
            other => panic!("worker must stay parked while send_out is full, got {other:?}"),
        }
        assert!(
            !fw.0.load(Ordering::SeqCst),
            "no spurious wake before the drain"
        );
        // FlushTask 排空 send_out:满变非满 → 生产者被唤醒(elision 镜像)
        match f.send.poll(&mut f.cx) {
            Poll::Pending => {}
            other => panic!("flush task must park after draining send_out, got {other:?}"),
        }
        assert!(
            fw.0.load(Ordering::SeqCst),
            "producer waker must be woken when send_out drains (lost-wakeup defense)"
        );
        // 阶段 3(恢复):滞留帧与命令通道剩余全部上线,顺序逐字节一致
        let mut guard = 0usize;
        loop {
            guard += 1;
            assert!(guard < 512, "recovery must converge");
            f.poll_send_workers();
            match f.send.poll(&mut f.cx) {
                Poll::Pending => {}
                other => panic!("flush task must park after draining, got {other:?}"),
            }
            let n = frames_on_wire(&f.st)
                .iter()
                .filter(|(h, _)| h.tag() == Tag::Data && h.stream_id() == StreamId::new(1))
                .count();
            if n == queued {
                break;
            }
        }
        let frames = frames_on_wire(&f.st);
        let bodies: Vec<&Vec<u8>> = frames
            .iter()
            .filter(|(h, _)| h.tag() == Tag::Data && h.stream_id() == StreamId::new(1))
            .map(|(_, b)| b)
            .collect();
        assert_eq!(bodies.len(), queued);
        assert!(
            bodies.iter().all(|b| b.as_slice() == &payload[..]),
            "every frame body must be intact and in order after recovery"
        );
    }

    /// T-S4:on_drop 三态矩阵——opening 命中路径 Open→RST、SendClosed→
    /// 无帧;ctrl Remove 路径(已被 RecvWorker 收编)RecvClosed→FIN 与
    /// 现行行为一致;stream_count 递减、ack_backlog_count 由令牌递减。
    #[test]
    fn send_worker_on_drop_frame_matrix() {
        let mut f = Fixture::client(2, Config::default());
        // (a) opening 命中,Open{ack:false} → RST 经 send_out;令牌递减
        let s1 = match f.send.poll_new_outbound(&mut f.cx) {
            Poll::Ready(Ok(s)) => s,
            other => panic!("outbound open must succeed, got {other:?}"),
        };
        assert_eq!(f.shared.ack_backlog_count.load(Ordering::SeqCst), 1);
        drop(s1);
        for _ in 0..4 {
            match f.drive_send() {
                Poll::Pending => {}
                other => panic!("flush task must park after the RST, got {other:?}"),
            }
        }
        let rst: Vec<_> = written_headers(&f.st)
            .into_iter()
            .filter(|h| h.stream_id() == StreamId::new(1) && h.flags().contains(header::RST))
            .collect();
        assert_eq!(
            rst.len(),
            1,
            "Open → exactly one RST via send_out, got {rst:?}"
        );
        assert_eq!(
            f.shared.stream_count.load(Ordering::SeqCst),
            0,
            "opening hit must decrement stream_count"
        );
        assert_eq!(
            f.shared.ack_backlog_count.load(Ordering::SeqCst),
            0,
            "the ack-backlog token must release the slot"
        );

        // (b) opening 命中,poll_close 半关闭(SendClosed)→ drop 无帧
        let mut s3 = match f.send.poll_new_outbound(&mut f.cx) {
            Poll::Ready(Ok(s)) => s,
            other => panic!("outbound open must succeed, got {other:?}"),
        };
        assert_eq!(s3.id(), StreamId::new(3));
        match Pin::new(&mut s3).poll_close(&mut f.cx) {
            Poll::Ready(Ok(())) => {}
            other => panic!("close must be ready, got {other:?}"),
        }
        for _ in 0..4 {
            match f.drive_send() {
                Poll::Pending => {}
                other => panic!("flush task must park after the FIN, got {other:?}"),
            }
        }
        let before = written_headers(&f.st).len();
        drop(s3);
        for _ in 0..4 {
            match f.drive_send() {
                Poll::Pending => {}
                other => panic!("flush task must park, got {other:?}"),
            }
        }
        let after: Vec<_> = written_headers(&f.st)
            .into_iter()
            .skip(before)
            .filter(|h| h.stream_id() == StreamId::new(3))
            .collect();
        assert!(
            after.is_empty(),
            "SendClosed → no extra frame on drop, got {after:?}"
        );

        // (c) ctrl 路径(RecvWorker 已收编):RecvClosed → FIN(现行行为)
        f.feed(syn_data_block(2, b"a"));
        let s2 = match f.drive_recv() {
            Poll::Ready(Some(Ok(s))) => s,
            other => panic!("inbound stream expected, got {other:?}"),
        };
        f.feed(data_fin_block(2, b""));
        match f.drive_recv() {
            Poll::Pending => {}
            other => panic!("no new stream expected on FIN, got {other:?}"),
        }
        let baseline = f.shared.stream_count.load(Ordering::SeqCst);
        let before = written_headers(&f.st).len();
        drop(s2);
        for _ in 0..6 {
            match f.drive_send() {
                Poll::Pending => {}
                other => panic!("flush task must park after the FIN, got {other:?}"),
            }
        }
        let fin: Vec<_> = written_headers(&f.st)
            .into_iter()
            .skip(before)
            .filter(|h| h.stream_id() == StreamId::new(2) && h.flags().contains(header::FIN))
            .collect();
        assert_eq!(
            fin.len(),
            1,
            "RecvClosed → exactly one FIN via the ctrl Remove path, got {fin:?}"
        );
        assert_eq!(
            f.shared.stream_count.load(Ordering::SeqCst),
            baseline - 1,
            "ctrl Remove must decrement stream_count"
        );
    }

    /// T-S5:错误收敛——set_error → SendWorker Ready 终结(粘性影子);
    /// 其接收器消亡 → 流侧 sender 报错(命令通道断,现行等效)。
    #[test]
    fn send_worker_error_terminates_and_breaks_channels() {
        let mut f = Fixture::client(2, Config::default());
        let mut s = match f.send.poll_new_outbound(&mut f.cx) {
            Poll::Ready(Ok(s)) => s,
            other => panic!("outbound open must succeed, got {other:?}"),
        };
        f.poll_send_workers(); // 收编(sid 1 → SendWorker 1)
        f.shared.set_error(ConnectionError::Closed);
        for w in f.send_workers.iter_mut() {
            assert!(
                w.poll_unpin(&mut f.cx).is_ready(),
                "error must finish every SendWorker"
            );
        }
        // 终结幂等
        for w in f.send_workers.iter_mut() {
            assert!(w.poll_unpin(&mut f.cx).is_ready());
        }
        // 命令通道断:流侧写报错(WriteZero)
        match Pin::new(&mut s).poll_write(&mut f.cx, b"x") {
            Poll::Ready(Err(e)) => assert_eq!(
                e.kind(),
                std::io::ErrorKind::WriteZero,
                "stream write must fail with WriteZero after receivers dropped"
            ),
            other => {
                panic!("stream write must fail after the worker dropped receivers, got {other:?}")
            }
        }
    }

    /// T-S6:send_dropped——置位 → SendWorker Ready 终结;RecvWorker 不
    /// 终结(现行 T-W10 语义不回归,§2.5 生命周期矩阵)。
    #[test]
    fn send_worker_send_dropped_terminates_but_recv_workers_serve() {
        let mut f = Fixture::client(2, Config::default());
        f.feed(syn_data_block(2, b"x"));
        let stream = match f.drive_recv() {
            Poll::Ready(Some(Ok(s))) => s,
            other => panic!("inbound stream expected, got {other:?}"),
        };
        // 等价 SendHalf::drop 的信号面:置位 + 双侧广播(现行
        // wake_all_workers + 新增 wake_all_send_workers)
        f.shared.send_dropped.store(true, Ordering::SeqCst);
        f.shared.wake_all_workers();
        f.shared.wake_all_send_workers();
        for w in f.send_workers.iter_mut() {
            assert!(
                w.poll_unpin(&mut f.cx).is_ready(),
                "send_dropped must finish SendWorker (D8)"
            );
        }
        for w in f.workers.iter_mut() {
            match w.poll_unpin(&mut f.cx) {
                Poll::Pending => {}
                other => panic!("RecvWorker must NOT finish on send_dropped alone, got {other:?}"),
            }
        }
        assert!(
            stream.is_closed(),
            "RecvWorker must still drain its local table (T-W10 semantics)"
        );
    }

    /// T-S7(红→绿):命令预算——单轮 poll 70 条命令(7 流 × 10 帧)只
    /// 消费 64(SEND_CMDS_PER_POLL 上界的直接证据),剩余保留下轮、
    /// 零丢失;两轮后全部上线且顺序逐字节一致。
    #[test]
    fn send_worker_command_budget_splits_rounds() {
        let mut cfg = Config::default();
        cfg.set_shard_workers(1);
        let mut f = Fixture::client(1, cfg);
        // 7 流(client 出站 sid 1..13 奇数,N=1 全归 worker0)× 10 帧
        // = 70 条命令 > 64;单流命令通道 cap `STREAM_COMMAND_CAP` 内全就绪
        const STREAMS: usize = 7;
        const FRAMES: usize = 10;
        let mut streams = Vec::new();
        let payload = [3u8; 64];
        for _ in 0..STREAMS {
            let mut s = match f.send.poll_new_outbound(&mut f.cx) {
                Poll::Ready(Ok(s)) => s,
                other => panic!("outbound open must succeed, got {other:?}"),
            };
            for _ in 0..FRAMES {
                match Pin::new(&mut s).poll_write(&mut f.cx, &payload) {
                    Poll::Ready(Ok(64)) => {}
                    other => panic!("stream write must be ready, got {other:?}"),
                }
            }
            streams.push(s);
        }
        // 第一轮:预算 64 恰耗尽(自唤醒 + Pending,noop 下手动续驱)
        f.poll_send_workers();
        match f.send.poll(&mut f.cx) {
            Poll::Pending => {}
            other => panic!("flush task must park after the first round, got {other:?}"),
        }
        let after_round1 = frames_on_wire(&f.st)
            .iter()
            .filter(|(h, _)| h.tag() == Tag::Data)
            .count();
        assert_eq!(
            after_round1, SEND_CMDS_PER_POLL,
            "a single SendWorker poll must consume exactly the 64-command budget"
        );
        // 第二轮:剩余 6 条消费,零丢失
        f.poll_send_workers();
        match f.send.poll(&mut f.cx) {
            Poll::Pending => {}
            other => panic!("flush task must park after the second round, got {other:?}"),
        }
        let frames = frames_on_wire(&f.st);
        let total = frames.iter().filter(|(h, _)| h.tag() == Tag::Data).count();
        assert_eq!(
            total,
            STREAMS * FRAMES,
            "all commands must be consumed across rounds without loss"
        );
        // 每流恰 10 帧、体逐字节一致(顺序契约)
        for k in 0..STREAMS {
            let sid = (2 * k + 1) as u32;
            let bodies: Vec<&Vec<u8>> = frames
                .iter()
                .filter(|(h, _)| h.tag() == Tag::Data && h.stream_id() == StreamId::new(sid))
                .map(|(_, b)| b)
                .collect();
            assert_eq!(bodies.len(), FRAMES, "stream {sid} frame count");
            assert!(
                bodies.iter().all(|b| b.as_slice() == &payload[..]),
                "stream {sid} bodies must be intact and in order"
            );
        }
    }

    /// T-S8(红→绿,复核修复 medium):ctrl Remove 在持续负载下不得
    /// 饥饿——已收编入站流 sid2 写 K 帧后 drop;同分片出站流 sid1 每轮
    /// 先灌一帧再驱动,使 SendWorker 的每个门控检查点(步骤 3 之后与
    /// park 复查 c)看到的 send_out[0] 都非空(FlushTask 瓶颈态下队列
    /// 几乎永不为空的确定性复现)。修前门控 =「整队取空」→ Remove 永不
    /// 投递(RST 不上线、stream_count 不减,有限 max_num_streams 下最终
    /// TooManyStreams / GoAway);修后门控 =「本流末帧已出队」的水位 →
    /// 有界轮数内投递,且 RST 必在 sid2 全部在途数据之后上线(红线 5
    /// 不回退)。
    #[test]
    fn send_worker_ctrl_remove_not_starved_under_sustained_load() {
        let mut cfg = Config::default();
        cfg.set_shard_workers(1);
        let mut f = Fixture::client(1, cfg);
        // 已收编入站流(on_drop 走 ctrl Remove → RecvWorker finalize 路径)
        f.feed(syn_data_block(2, b"a"));
        let mut inbound = match f.drive_recv() {
            Poll::Ready(Some(Ok(s))) => s,
            other => panic!("inbound stream expected, got {other:?}"),
        };
        // 同分片持续负载源(N=1:全部流归 SendWorker 0)
        let mut load = match f.send.poll_new_outbound(&mut f.cx) {
            Poll::Ready(Ok(s)) => s,
            other => panic!("outbound open must succeed, got {other:?}"),
        };
        // K 帧在途数据(命令通道 cap `STREAM_COMMAND_CAP`:授信 WU|ACK 已
        // 消费,8 帧全就绪)
        const K: usize = 8;
        let payload = [0x5Au8; 128];
        for _ in 0..K {
            match Pin::new(&mut inbound).poll_write(&mut f.cx, &payload) {
                Poll::Ready(Ok(128)) => {}
                other => panic!("inbound stream write must be ready, got {other:?}"),
            }
        }
        let baseline = f.shared.stream_count.load(Ordering::SeqCst);
        assert_eq!(baseline, 2, "one inbound + one outbound stream");
        drop(inbound); // Open → RST(ctrl Remove → RecvWorker finalize → to_send)

        let sid2 = StreamId::new(2);
        let mut removed_at = None;
        for round in 0..8 {
            // 每轮先灌一帧负载:本轮 SendWorker 必有新帧压入 send_out,
            // 其门控检查点看到的队列恒非空
            match Pin::new(&mut load).poll_write(&mut f.cx, &[1u8; 32]) {
                Poll::Ready(Ok(32)) => {}
                other => panic!("load write must be ready, got {other:?}"),
            }
            match f.drive_send() {
                Poll::Pending => {}
                other => panic!("flush task must park each round, got {other:?}"),
            }
            if f.shared.stream_count.load(Ordering::SeqCst) == baseline - 1 {
                removed_at = Some(round);
                break;
            }
        }
        let removed_at = removed_at.unwrap_or_else(|| {
            panic!(
                "ctrl Remove starved under sustained load: stream_count still {} after 8 rounds",
                f.shared.stream_count.load(Ordering::SeqCst)
            )
        });
        // 有界迟到:drop 时在途帧于首轮终态写穿 → 次轮投递 Remove → 第三轮
        // RecvWorker finalize 递减计数(轮序确定,不因负载推迟)
        assert!(
            removed_at <= 3,
            "Remove lateness must be bounded by the in-flight drain, got round {removed_at}"
        );
        match f.drive_send() {
            Poll::Pending => {}
            other => panic!("flush task must park, got {other:?}"),
        }
        let wire = frames_on_wire(&f.st);
        let rst_pos: Vec<usize> = wire
            .iter()
            .enumerate()
            .filter(|(_, (h, _))| h.stream_id() == sid2 && h.flags().contains(header::RST))
            .map(|(i, _)| i)
            .collect();
        assert_eq!(rst_pos.len(), 1, "Open → exactly one RST, got {rst_pos:?}");
        let data_pos: Vec<usize> = wire
            .iter()
            .enumerate()
            .filter(|(_, (h, b))| {
                h.stream_id() == sid2 && h.tag() == Tag::Data && b.len() == payload.len()
            })
            .map(|(i, _)| i)
            .collect();
        assert_eq!(data_pos.len(), K, "all in-flight data must reach the wire");
        assert!(
            data_pos.iter().all(|&i| i < rst_pos[0]),
            "RST must follow every in-flight data frame: data {data_pos:?}, RST {rst_pos:?}"
        );
    }

    /// T-S9(复核修复,红线 5 守卫):水位门控不得回退保序——已收编入站
    /// 流写 K 帧后 drop;以「SendWorker → RecvWorker → FlushTask」轮序
    /// 驱动(RecvWorker 先于 FlushTask 看到 Remove:若 Remove 在本流数据
    /// 尚滞留 send_out 时投递,finalize 的 RST 经 to_send 最高优先级会先于
    /// 数据写穿——无门控即插队,数据丢失)。断言:RST 上线且 K 帧数据
    /// 全部在其前。
    #[test]
    fn send_worker_ctrl_remove_waits_for_in_flight_frames() {
        let mut cfg = Config::default();
        cfg.set_shard_workers(1);
        let mut f = Fixture::client(1, cfg);
        f.feed(syn_data_block(2, b"a"));
        let mut inbound = match f.drive_recv() {
            Poll::Ready(Some(Ok(s))) => s,
            other => panic!("inbound stream expected, got {other:?}"),
        };
        // 先把授信 WU|ACK 写穿:随后 send_out 内只剩本流数据帧
        match f.drive_send() {
            Poll::Pending => {}
            other => panic!("flush task must park, got {other:?}"),
        }
        const K: usize = 8;
        let payload = [0xC3u8; 96];
        for _ in 0..K {
            match Pin::new(&mut inbound).poll_write(&mut f.cx, &payload) {
                Poll::Ready(Ok(96)) => {}
                other => panic!("inbound stream write must be ready, got {other:?}"),
            }
        }
        assert_eq!(f.shared.stream_count.load(Ordering::SeqCst), 1);
        drop(inbound); // Open → RST

        let sid2 = StreamId::new(2);
        let mut rounds = 0;
        loop {
            rounds += 1;
            assert!(rounds <= 8, "RST must land within bounded rounds");
            // 轮序:SendWorker(消费数据 + drop)→ RecvWorker(Remove 已
            // 投递则 finalize → to_send)→ FlushTask 写穿(to_send 优先)
            f.poll_send_workers();
            for w in f.workers.iter_mut() {
                let _ = w.poll_unpin(&mut f.cx);
            }
            match f.send.poll(&mut f.cx) {
                Poll::Pending => {}
                other => panic!("flush task must park, got {other:?}"),
            }
            if frames_on_wire(&f.st)
                .iter()
                .any(|(h, _)| h.stream_id() == sid2 && h.flags().contains(header::RST))
            {
                break;
            }
        }
        let wire = frames_on_wire(&f.st);
        let rst_pos = wire
            .iter()
            .position(|(h, _)| h.stream_id() == sid2 && h.flags().contains(header::RST))
            .expect("RST present");
        let data_pos: Vec<usize> = wire
            .iter()
            .enumerate()
            .filter(|(_, (h, b))| {
                h.stream_id() == sid2 && h.tag() == Tag::Data && b.len() == payload.len()
            })
            .map(|(i, _)| i)
            .collect();
        assert_eq!(data_pos.len(), K, "all in-flight data must reach the wire");
        assert!(
            data_pos.iter().all(|&i| i < rst_pos),
            "RST overtook in-flight data: data {data_pos:?}, RST {rst_pos}"
        );
        assert_eq!(
            f.shared.stream_count.load(Ordering::SeqCst),
            0,
            "ctrl Remove must decrement stream_count"
        );
    }

    // ===== round-6.1 §3(F2 取空通知门控 / F1 命令预算 poll 级)=====

    /// T-G1(round-6.1 F2,红→绿):取空通知门控——ctrl_wait[k] 为假
    /// (SendWorker k 无滞留 ctrl Remove)时,FlushTask 取空 send_out[k]
    /// 不得触碰 send_worker_wakers[k](一次原子读跳过 has_pending/
    /// wakers 两把锁与唤醒;通知唯一服务对象是"因水位门控滞留 ctrl
    /// Remove 的 SendWorker",无滞留即纯开销);置位后取空恰一次唤醒
    /// (take 语义,槽清空);清零后恢复跳过(§1.2 清零路径的效果面)。
    #[test]
    fn flush_task_empty_notification_gated_by_ctrl_wait() {
        let mut f = Fixture::client(1, Config::default());
        // 模拟 SendWorker 0 park:计数 waker 登记进 waker 槽(真实形态
        // 为 park 复查 a 的登记点,此处白盒直置)
        let wf = Arc::new(CountWaker::default());
        let w = waker(wf.clone());
        f.shared.send_worker_wakers.lock()[0] = Some(w.clone());
        // Worker 未驱动:无 ctrl_pending,ctrl_wait[0] 初始为假——反复
        // 取空 send_out[0],waker 槽必须保持已登记、计数零唤醒
        for i in 0..3u8 {
            f.send_workers[0]
                .out
                .try_push(data_frame(1, vec![i; 8]))
                .expect("send_out has room (cap 128)");
            match f.send.poll(&mut f.cx) {
                Poll::Pending => {}
                other => panic!("flush task must park after draining, got {other:?}"),
            }
        }
        assert_eq!(
            wf.0.load(Ordering::SeqCst),
            0,
            "ctrl_wait 为假时取空通知必须被前置门跳过(零唤醒)"
        );
        assert!(
            f.shared.send_worker_wakers.lock()[0].is_some(),
            "waker 槽必须保持已登记(未被 take)"
        );
        // 置位(模拟 Worker 滞留 ctrl Remove)→ 取空恰一次唤醒、槽被 take
        f.shared.ctrl_wait[0].store(true, Ordering::SeqCst);
        f.send_workers[0]
            .out
            .try_push(data_frame(1, vec![9u8; 8]))
            .expect("send_out has room (cap 128)");
        match f.send.poll(&mut f.cx) {
            Poll::Pending => {}
            other => panic!("flush task must park, got {other:?}"),
        }
        assert_eq!(
            wf.0.load(Ordering::SeqCst),
            1,
            "ctrl_wait 置位后取空必须恰一次唤醒"
        );
        assert!(
            f.shared.send_worker_wakers.lock()[0].is_none(),
            "唤醒后 waker 槽已清空(take 语义)"
        );
        // 清零(模拟 flush_ctrl_pending 排空后清门)→ 再取空恢复跳过
        f.shared.ctrl_wait[0].store(false, Ordering::SeqCst);
        f.shared.send_worker_wakers.lock()[0] = Some(w);
        f.send_workers[0]
            .out
            .try_push(data_frame(1, vec![0xAu8; 8]))
            .expect("send_out has room (cap 128)");
        match f.send.poll(&mut f.cx) {
            Poll::Pending => {}
            other => panic!("flush task must park, got {other:?}"),
        }
        assert_eq!(
            wf.0.load(Ordering::SeqCst),
            1,
            "ctrl_wait 清零后取空通知必须再次被门跳过"
        );
    }

    /// T-G2(round-6.1 F1):命令预算 poll 级——通道内预置 >64 条全就绪
    /// 命令(20 流 × 10 帧 = 200),单次 poll 消费恰 SEND_CMDS_PER_POLL
    /// 的量级;预算耗尽的 wake_by_ref 自唤醒(noop waker 下无效)由驱动
    /// 循环显式再 poll;每轮 wire 增量 ≤64、总消费对齐 200、零丢失、
    /// 流内顺序逐字节一致。
    ///
    /// 说明(规格 §3 预留的退化形态):"单次 poll 消费 >64"的红形态需
    /// 同一 poll 内迭代间并发补货——单线程手动驱动下预置命令全就绪,
    /// 迭代要么耗尽预算让出(budget_exhausted → wake_by_ref + Pending)、
    /// 要么无命令可消费,迭代间预算重置无法被观察;故本测与 T-S7 一起
    /// 作为 poll 级上界的直接守卫(预算若回退为迭代级,任何迭代间
    /// progress → continue 的补货窗口都会令单轮消费突破 64)。
    #[test]
    fn send_worker_command_budget_poll_level() {
        let mut cfg = Config::default();
        cfg.set_shard_workers(1);
        let mut f = Fixture::client(1, cfg);
        // 20 流(client 出站 sid 1..40 奇数,N=1 全归 worker0)× 10 帧
        // = 200 条命令 > 3×64;单流命令通道 cap `STREAM_COMMAND_CAP` 内全就绪
        const STREAMS: usize = 20;
        const FRAMES: usize = 10;
        let mut streams = Vec::new();
        let payload = [0x7Bu8; 32];
        for _ in 0..STREAMS {
            let mut s = match f.send.poll_new_outbound(&mut f.cx) {
                Poll::Ready(Ok(s)) => s,
                other => panic!("outbound open must succeed, got {other:?}"),
            };
            for _ in 0..FRAMES {
                match Pin::new(&mut s).poll_write(&mut f.cx, &payload) {
                    Poll::Ready(Ok(32)) => {}
                    other => panic!("stream write must be ready, got {other:?}"),
                }
            }
            streams.push(s);
        }
        // 驱动循环:每轮一次 SendWorker poll + FlushTask 写穿;每轮 wire
        // 的 Data 帧增量 ≤ SEND_CMDS_PER_POLL(单 poll 命令消费上界),
        // 直至 200 条全部上线(预算耗尽即让出,轮数 > 200/64)
        let mut on_wire = 0usize;
        let mut rounds = 0usize;
        while on_wire < STREAMS * FRAMES {
            rounds += 1;
            assert!(rounds <= 8, "all commands must land within bounded rounds");
            f.poll_send_workers();
            match f.send.poll(&mut f.cx) {
                Poll::Pending => {}
                other => panic!("flush task must park each round, got {other:?}"),
            }
            let now = frames_on_wire(&f.st)
                .iter()
                .filter(|(h, _)| h.tag() == Tag::Data)
                .count();
            let delta = now - on_wire;
            assert!(
                delta <= SEND_CMDS_PER_POLL,
                "round {rounds}: single poll must consume at most \
                 {SEND_CMDS_PER_POLL} commands, got {delta}"
            );
            on_wire = now;
        }
        assert_eq!(
            on_wire,
            STREAMS * FRAMES,
            "all commands must reach the wire across poll rounds without loss"
        );
        // 每流恰 10 帧、体逐字节一致(顺序契约)
        let frames = frames_on_wire(&f.st);
        for k in 0..STREAMS {
            let sid = (2 * k + 1) as u32;
            let bodies: Vec<&Vec<u8>> = frames
                .iter()
                .filter(|(h, _)| h.tag() == Tag::Data && h.stream_id() == StreamId::new(sid))
                .map(|(_, b)| b)
                .collect();
            assert_eq!(bodies.len(), FRAMES, "stream {sid} frame count");
            assert!(
                bodies.iter().all(|b| b.as_slice() == &payload[..]),
                "stream {sid} bodies must be intact and in order"
            );
        }
    }

    // ===== round-6 §5.3 FlushTask(halves)=====

    /// T-F1(红→绿):轮转公平——4 Worker 各灌 32 帧(白盒直压 send_out,
    /// 真实 SPSC 生产路径)→ 全部上线且每流恰 32 ≥ 总量/N−2(无饿死);
    /// wire 的 Data 序列按 worker 严格交错(out_cursor 推进的直接证据)。
    #[test]
    fn flush_task_round_robin_fairness() {
        let mut f = Fixture::client(4, Config::default());
        for (k, w) in f.send_workers.iter_mut().enumerate() {
            for _ in 0..32 {
                let sid = (2 * k + 1) as u32;
                match w.out.try_push(data_frame(sid, vec![k as u8 + 0xA0; 16])) {
                    Ok(()) => {}
                    Err(_) => panic!("send_out has room (cap 128)"),
                }
            }
        }
        match f.send.poll(&mut f.cx) {
            Poll::Pending => {}
            other => panic!("flush task must park after draining, got {other:?}"),
        }
        let frames = frames_on_wire(&f.st);
        let data: Vec<_> = frames
            .iter()
            .filter(|(h, _)| h.tag() == Tag::Data)
            .collect();
        assert_eq!(data.len(), 128, "all frames must reach the wire");
        for k in 0..4usize {
            let sid = (2 * k + 1) as u32;
            let n = data
                .iter()
                .filter(|(h, _)| h.stream_id() == StreamId::new(sid))
                .count();
            assert!(
                n >= 128 / 4 - 2,
                "stream {sid} starved with {n} frames (round-robin broken)"
            );
            assert_eq!(n, 32, "strict round robin must give each shard exactly 32");
        }
        // 严格轮转序:Data 帧的 sid 序列周期 4(1,3,5,7,1,…;批间
        // out_cursor 不复位,全局单周期)
        let seq: Vec<u32> = data.iter().map(|(h, _)| h.stream_id().val()).collect();
        for (i, &sid) in seq.iter().enumerate() {
            assert_eq!(
                sid,
                (2 * (i % 4) + 1) as u32,
                "wire order must follow the strict round-robin cursor at #{i}"
            );
        }
    }

    /// T-F2:优先级——to_send 有 Pong + send_out 有数据(真实命令链灌入)
    /// → Pong 字节先上线(T-B4 同型,路径改 send_out)。
    #[test]
    fn flush_task_tosend_precedes_send_out() {
        let mut f = Fixture::client(2, Config::default());
        let payload = [7u8; 1024];
        let mut streams = Vec::new();
        for _ in 0..2 {
            let mut s = match f.send.poll_new_outbound(&mut f.cx) {
                Poll::Ready(Ok(s)) => s,
                other => panic!("outbound open must succeed, got {other:?}"),
            };
            for _ in 0..8 {
                match Pin::new(&mut s).poll_write(&mut f.cx, &payload) {
                    Poll::Ready(Ok(1024)) => {}
                    other => panic!("stream write must be ready, got {other:?}"),
                }
            }
            streams.push(s);
        }
        // 16 个流帧先进 send_out(SendWorker 消费真实命令)
        f.poll_send_workers();
        // to_send 压入 Pong(生产 push_to_send 语义)
        let mut hdr = Header::ping(9);
        hdr.ack();
        f.shared.push_to_send_for_test(Frame::new(hdr).into());

        match f.send.poll(&mut f.cx) {
            Poll::Pending => {}
            other => panic!("flush task must park after draining, got {other:?}"),
        }

        let frames = frames_on_wire(&f.st);
        let (h0, _) = &frames[0];
        assert_eq!(
            h0.tag(),
            Tag::Ping,
            "first frame on the wire must be the pong (to_send > send_out)"
        );
        assert!(h0.flags().contains(header::ACK), "pong must carry ACK");
        assert_eq!(h0.len().val(), 9, "pong keeps its opaque id");
        let data = frames.iter().filter(|(h, _)| h.tag() == Tag::Data).count();
        assert_eq!(data, 16, "all 16 stream frames must follow the pong");
    }

    /// T-F3(红→绿,D3-2 镜像,T7 同型):载体 writev 阻塞(半写)+
    /// send_out[k] 有帧 → SendHalf::poll 必须返回 Pending(修前把
    /// send_out 当"有工作"复查 → continue 无限自旋,poll 永不返回)。
    /// 看门狗:poll 在工作线程执行,主线程 3s 收不到返回即判自旋失败;
    /// poison 让自旋线程有界退出。
    #[test]
    fn flush_task_park_returns_pending_when_send_out_blocked_by_partial_write() {
        // T-F3 专用载体:写容量受 cap 限制(cap==0 → Pending 并登记写
        // waker;每次成功写扣减容量),读侧恒 Pending;poison 后写 Err。
        // 状态经 Arc<std::sync::Mutex>:Send 绑定,可移入工作线程。
        struct ThrottleState {
            cap: usize,
            written: Vec<u8>,
            write_waker: Option<Waker>,
            poison: bool,
        }
        struct ThrottleSocket(Arc<std::sync::Mutex<ThrottleState>>);
        impl crate::Writev2 for ThrottleSocket {
            fn poll_write2(
                self: Pin<&mut Self>,
                cx: &mut Context<'_>,
                a: &[u8],
                b: &[u8],
            ) -> Poll<std::io::Result<usize>> {
                crate::Writev2::poll_writev(
                    self,
                    cx,
                    &[std::io::IoSlice::new(a), std::io::IoSlice::new(b)],
                )
            }
            fn poll_writev(
                self: Pin<&mut Self>,
                cx: &mut Context<'_>,
                bufs: &[std::io::IoSlice<'_>],
            ) -> Poll<std::io::Result<usize>> {
                let mut st = self.0.lock().unwrap();
                if st.poison {
                    return Poll::Ready(Err(std::io::Error::other("throttle poison")));
                }
                if st.cap == 0 {
                    st.write_waker = Some(cx.waker().clone());
                    return Poll::Pending;
                }
                let mut budget = st.cap;
                let mut n = 0usize;
                for b in bufs {
                    let take = b.len().min(budget);
                    if take == 0 {
                        continue;
                    }
                    st.written.extend_from_slice(&b[..take]);
                    n += take;
                    budget -= take;
                    if budget == 0 {
                        break;
                    }
                }
                st.cap -= n;
                Poll::Ready(Ok(n))
            }
        }
        impl futures::io::AsyncRead for ThrottleSocket {
            fn poll_read(
                self: Pin<&mut Self>,
                _cx: &mut Context<'_>,
                _buf: &mut [u8],
            ) -> Poll<std::io::Result<usize>> {
                Poll::Pending
            }
        }
        impl futures::io::AsyncWrite for ThrottleSocket {
            fn poll_write(
                self: Pin<&mut Self>,
                cx: &mut Context<'_>,
                buf: &[u8],
            ) -> Poll<std::io::Result<usize>> {
                crate::Writev2::poll_writev(self, cx, &[std::io::IoSlice::new(buf)])
            }
            fn poll_flush(
                self: Pin<&mut Self>,
                _cx: &mut Context<'_>,
            ) -> Poll<std::io::Result<()>> {
                Poll::Ready(Ok(()))
            }
            fn poll_close(
                self: Pin<&mut Self>,
                _cx: &mut Context<'_>,
            ) -> Poll<std::io::Result<()>> {
                Poll::Ready(Ok(()))
            }
        }

        let state = Arc::new(std::sync::Mutex::new(ThrottleState {
            cap: 8, // 首帧(12B 帧头)只写穿 8B → 半写
            written: Vec::new(),
            write_waker: None,
            poison: false,
        }));
        let sock = ThrottleSocket(state.clone());
        let mut cfg = Config::default();
        cfg.set_shard_workers(2);
        let conn = Connection::new(sock, cfg, Mode::Client);
        let (_recv, mut send_half, _workers, mut send_workers) = conn.into_halves();

        let mut cx = noop_cx();
        let mut stream = match send_half.poll_new_outbound(&mut cx) {
            Poll::Ready(Ok(s)) => s,
            other => panic!("open outbound via send half, got {other:?}"),
        };
        match Pin::new(&mut stream).poll_write(&mut cx, b"payload") {
            Poll::Ready(Ok(7)) => {}
            other => panic!("queue data frame, got {other:?}"),
        }
        // 数据帧进 send_out[1](FlushTask 尚未 poll,半写未发生)
        for w in send_workers.iter_mut() {
            let _ = w.poll_unpin(&mut cx);
        }

        // 工作线程 poll 发送半:修前 park 复查命中 send_out 非空 →
        // 无限自旋(看门狗超时判失败);修后 send_out 复查受 sink_idle
        // 门控(半写态 sink_idle=false),立即返回 Pending。
        let (tx, rx) = std::sync::mpsc::channel::<String>();
        let worker = std::thread::spawn(move || {
            let w = futures::task::noop_waker();
            let mut wcx = Context::from_waker(&w);
            let outcome = send_half.poll(&mut wcx);
            let _ = tx.send(format!("{outcome:?}"));
            send_half
        });
        match rx.recv_timeout(std::time::Duration::from_secs(3)) {
            Ok(outcome) => {
                assert_eq!(
                    outcome, "Pending",
                    "flush task must return Pending (not spin) while a partial write \
                     blocks the carrier and send_out holds a frame"
                );
                let mut send_half = worker.join().expect("worker must finish cleanly");
                assert_eq!(
                    state.lock().unwrap().written.len(),
                    8,
                    "exactly 8B of the first frame header must be written"
                );
                // 反向断言:写侧恢复可写 → 半写帧写穿 + send_out 帧上线
                {
                    let mut st = state.lock().unwrap();
                    st.cap = 4096;
                    if let Some(w) = st.write_waker.take() {
                        w.wake();
                    }
                }
                match send_half.poll(&mut cx) {
                    Poll::Pending => {}
                    other => panic!("flush task must re-park after draining, got {other:?}"),
                }
                let written = state.lock().unwrap().written.clone();
                let hdrs = written_headers_from_bytes(&written);
                assert!(
                    hdrs.iter()
                        .any(|h| h.tag() == Tag::Data && h.stream_id() == StreamId::new(1)),
                    "the send_out frame must reach the wire after the drain: {hdrs:?}"
                );
            }
            Err(_) => {
                // 看门狗超时:poison 让自旋中的 poll_writev 返回 Err,
                // 工作线程有界退出,然后判失败
                state.lock().unwrap().poison = true;
                let _ = worker.join();
                panic!(
                    "spin detected: flush task did not return within 3s while send_out \
                     holds a frame blocked by a partial write"
                );
            }
        }
    }

    /// T-F4(红→绿):丢唤醒防线——FlushTask poll 至 Pending(已登记
    /// send_out consumer waker)→ Worker push send_out[k](空变非空)→
    /// FlushTask 被唤醒并写穿(WakeFlag 断言,§3.2 无条件登记的兑现)。
    #[test]
    fn flush_task_woken_by_send_out_push_after_park() {
        let mut f = Fixture::client(2, Config::default());
        // 预驱动冲掉 Rtt::new 立即到期的 ping,让 FlushTask 干净 park
        match f.send.poll(&mut f.cx) {
            Poll::Pending => {}
            other => panic!("flush task must park after the rtt ping, got {other:?}"),
        }
        // 以 WakeFlag 复 poll:park 序列把 consumer waker 登记到 fw
        let fw = Arc::new(WakeFlag::default());
        let w = waker(fw.clone());
        let mut cx2 = Context::from_waker(&w);
        match f.send.poll(&mut cx2) {
            Poll::Pending => {}
            other => panic!("flush task must park, got {other:?}"),
        }
        assert!(
            !fw.0.load(Ordering::SeqCst),
            "no spurious wake before the push"
        );
        // Worker push(白盒直压 send_out[1],空变非空恰一次唤醒)
        match f.send_workers[1].out.try_push(data_frame(3, vec![9u8; 32])) {
            Ok(()) => {}
            Err(_) => panic!("send_out has room"),
        }
        assert!(
            fw.0.load(Ordering::SeqCst),
            "consumer waker must be woken by the empty→non-empty push (lost-wakeup defense)"
        );
        // 复 poll 写穿
        match f.send.poll(&mut f.cx) {
            Poll::Pending => {}
            other => panic!("flush task must park after draining, got {other:?}"),
        }
        assert!(
            frames_on_wire(&f.st)
                .iter()
                .any(|(h, b)| h.stream_id() == StreamId::new(3) && b.as_slice() == &[9u8; 32][..]),
            "the pushed frame must reach the wire after the wake"
        );
    }

    /// T-F5(红→绿):开流路由——poll_new_outbound + 立即写 → 首帧带 SYN
    /// 经 send_out 上线(对齐现行 G2/S 系断言;D4:出站流命令接收器经
    /// push_new_receiver 路由 SendWorker 收编)。
    #[test]
    fn flush_task_first_write_carries_syn_via_send_out() {
        let mut f = Fixture::client(2, Config::default());
        let mut s = match f.send.poll_new_outbound(&mut f.cx) {
            Poll::Ready(Ok(s)) => s,
            other => panic!("outbound open must succeed, got {other:?}"),
        };
        assert_eq!(s.id(), StreamId::new(1));
        match Pin::new(&mut s).poll_write(&mut f.cx, b"first") {
            Poll::Ready(Ok(5)) => {}
            other => panic!("first write must be ready, got {other:?}"),
        }
        // 跨分片驱动:SendWorker 收编(注册路由)→ 消费 → FlushTask 写穿
        match f.drive_send() {
            Poll::Pending => {}
            other => panic!("flush task must park after flushing, got {other:?}"),
        }
        let s1: Vec<_> = frames_on_wire(&f.st)
            .into_iter()
            .filter(|(h, _)| h.stream_id() == StreamId::new(1))
            .collect();
        assert_eq!(s1.len(), 1, "exactly one frame for stream 1");
        assert_eq!(s1[0].0.tag(), Tag::Data);
        assert!(
            s1[0].0.flags().contains(header::SYN),
            "lazy SYN must ride the first frame via send_out, got {:?}",
            s1[0].0.flags()
        );
        assert_eq!(&s1[0].1, b"first");
    }

    // ===== §5.3 halves 端到端 =====

    /// T-H1:双向并发——双连接对泳(手工 wire 同步)16 流双向打流,
    /// 数据逐字节校验 + 双侧 FIN/EOF 收敛。
    #[test]
    fn halves_bidirectional_16_streams() {
        let mut cfg = Config::default();
        cfg.set_shard_workers(2);
        cfg.set_initial_stream_receive_window(6 * 1024 * 1024);
        const STREAMS: usize = 16;
        const PAYLOAD: usize = 4096;

        let (socket_a, st_a) = ScriptSocket::new();
        let conn_a = Connection::new(socket_a, cfg.clone(), Mode::Client);
        let (mut recv_a, mut send_a, mut workers_a, mut send_workers_a) = conn_a.into_halves();
        let (socket_b, st_b) = ScriptSocket::new();
        let conn_b = Connection::new(socket_b, cfg, Mode::Server);
        let (mut recv_b, mut send_b, mut workers_b, mut send_workers_b) = conn_b.into_halves();
        let mut cx = noop_cx();

        let payload_for = |sid: u32| -> Vec<u8> {
            (0..PAYLOAD)
                .map(|i| ((sid as usize + i) % 251) as u8)
                .collect()
        };

        // 双侧各开 16 流并整写载荷 + 半关闭(FIN)
        let mut local_a = Vec::new();
        for _ in 0..STREAMS {
            let mut s = match send_a.poll_new_outbound(&mut cx) {
                Poll::Ready(Ok(s)) => s,
                other => panic!("A open failed, got {other:?}"),
            };
            let payload = payload_for(s.id().val());
            match Pin::new(&mut s).poll_write(&mut cx, &payload) {
                Poll::Ready(Ok(n)) => assert_eq!(n, PAYLOAD),
                other => panic!("A write failed, got {other:?}"),
            }
            match Pin::new(&mut s).poll_close(&mut cx) {
                Poll::Ready(Ok(())) => {}
                other => panic!("A close failed, got {other:?}"),
            }
            local_a.push(s);
        }
        let mut local_b = Vec::new();
        for _ in 0..STREAMS {
            let mut s = match send_b.poll_new_outbound(&mut cx) {
                Poll::Ready(Ok(s)) => s,
                other => panic!("B open failed, got {other:?}"),
            };
            let payload = payload_for(s.id().val());
            match Pin::new(&mut s).poll_write(&mut cx, &payload) {
                Poll::Ready(Ok(n)) => assert_eq!(n, PAYLOAD),
                other => panic!("B write failed, got {other:?}"),
            }
            match Pin::new(&mut s).poll_close(&mut cx) {
                Poll::Ready(Ok(())) => {}
                other => panic!("B close failed, got {other:?}"),
            }
            local_b.push(s);
        }

        // 对泳:accepted 流按 sid 校验(读进度表)
        let mut accepted_a: Vec<Stream> = Vec::new();
        let mut accepted_b: Vec<Stream> = Vec::new();
        let mut progress_a: HashMap<u32, usize> = HashMap::new();
        let mut progress_b: HashMap<u32, usize> = HashMap::new();
        let mut synced_a = 0usize; // A 已同步给 B 的 written 前缀
        let mut synced_b = 0usize;

        // 单流一步读:校验字节 + 返回是否收满并见 EOF(FIN 收敛)
        fn step_read(
            s: &mut Stream,
            want: &[u8],
            progress: &mut HashMap<u32, usize>,
            cx: &mut Context<'_>,
        ) -> bool {
            let sid = s.id().val();
            let done = progress.get(&sid).copied().unwrap_or(0);
            if done < want.len() {
                let mut buf = [0u8; 8192];
                match Pin::new(&mut *s).poll_read(cx, &mut buf) {
                    Poll::Ready(Ok(0)) => {
                        panic!("stream {}: premature EOF at {}/{}", sid, done, want.len())
                    }
                    Poll::Ready(Ok(n)) => {
                        assert_eq!(
                            &buf[..n],
                            &want[done..done + n],
                            "payload mismatch on stream {sid}"
                        );
                        progress.insert(sid, done + n);
                    }
                    Poll::Ready(Err(e)) => panic!("stream {sid} read error: {e}"),
                    Poll::Pending => {}
                }
            }
            let done = progress.get(&sid).copied().unwrap_or(0);
            if done < want.len() {
                return false;
            }
            // 载荷收满:等 EOF(FIN)
            let mut eof = [0u8; 1];
            matches!(
                Pin::new(&mut *s).poll_read(cx, &mut eof),
                Poll::Ready(Ok(0))
            )
        }

        let mut rounds = 0;
        loop {
            rounds += 1;
            assert!(rounds < 500, "T-H1 did not converge in {rounds} rounds");

            // wire 同步:A 写 → B 读,B 写 → A 读
            {
                let written_a = st_a.borrow().written.clone();
                if written_a.len() > synced_a {
                    st_b.borrow_mut()
                        .read_blocks
                        .push_back(written_a[synced_a..].to_vec());
                    synced_a = written_a.len();
                }
            }
            {
                let written_b = st_b.borrow().written.clone();
                if written_b.len() > synced_b {
                    st_a.borrow_mut()
                        .read_blocks
                        .push_back(written_b[synced_b..].to_vec());
                    synced_b = written_b.len();
                }
            }

            // 驱动双侧:send(FlushTask)flush → RecvWorker 全处理 →
            // SendWorker 消费流命令(round-6 驱动面)→ recv 交付(×2 轮
            // 让 ctrl/交付链走穿)
            for r in 0..2 {
                let _ = send_a.poll(&mut cx);
                let _ = send_b.poll(&mut cx);
                for w in workers_a.iter_mut() {
                    let _ = w.poll_unpin(&mut cx);
                }
                for w in workers_b.iter_mut() {
                    let _ = w.poll_unpin(&mut cx);
                }
                for w in send_workers_a.iter_mut() {
                    let _ = w.poll_unpin(&mut cx);
                }
                for w in send_workers_b.iter_mut() {
                    let _ = w.poll_unpin(&mut cx);
                }
                // send_out 里 Worker 刚压入的帧由再一轮 send poll 写穿
                let _ = send_a.poll(&mut cx);
                let _ = send_b.poll(&mut cx);
                if r == 0 {
                    if let Poll::Ready(Some(Ok(s))) = recv_a.poll(&mut cx) {
                        accepted_a.push(s);
                    }
                    if let Poll::Ready(Some(Ok(s))) = recv_b.poll(&mut cx) {
                        accepted_b.push(s);
                    }
                }
            }

            // 应用读:逐流校验直至收满 + EOF
            let mut done_b = accepted_b.len() == STREAMS;
            for s in accepted_b.iter_mut() {
                if !step_read(s, &payload_for(s.id().val()), &mut progress_b, &mut cx) {
                    done_b = false;
                }
            }
            let mut done_a = accepted_a.len() == STREAMS;
            for s in accepted_a.iter_mut() {
                if !step_read(s, &payload_for(s.id().val()), &mut progress_a, &mut cx) {
                    done_a = false;
                }
            }
            if done_a && done_b {
                break;
            }
        }

        // 全部 accepted、双侧 FIN/EOF 收敛(内容已在泵中断言)
        assert_eq!(accepted_a.len(), STREAMS);
        assert_eq!(accepted_b.len(), STREAMS);
        drop(local_a);
        drop(local_b);
    }

    /// T-H2:建流风暴——256 连续 SYN(跨分片)→ 全部 accept、顺序句柄
    /// 数正确、stream_count 终值正确、全关后计数归零。
    #[test]
    fn halves_syn_storm_256_streams() {
        let mut cfg = Config::default();
        cfg.set_shard_workers(2);
        let (socket, st) = ScriptSocket::new();
        let conn = Connection::new(socket, cfg, Mode::Client);
        let (mut recv, mut send, mut workers, mut send_workers) = conn.into_halves();
        let shared = recv.shared_for_test();
        let mut cx = noop_cx();

        const N: u32 = 256;
        {
            let mut st_mut = st.borrow_mut();
            for k in 1..=N {
                st_mut
                    .read_blocks
                    .push_back(syn_data_block(2 * k, &[k as u8]));
            }
        }

        let mut streams = Vec::with_capacity(N as usize);
        let mut guard = 0;
        while (streams.len() as u32) < N {
            guard += 1;
            assert!(guard < 8192, "T-H2 delivery stalled at {}", streams.len());
            // recv 路由(遇背压 park)→ workers 建流交付 → recv 再探
            match recv.poll(&mut cx) {
                Poll::Ready(Some(Ok(s))) => {
                    streams.push(s);
                    continue;
                }
                Poll::Pending => {}
                other => panic!(
                    "unexpected recv outcome at #{}, got {other:?}",
                    streams.len()
                ),
            }
            for w in workers.iter_mut() {
                let _ = w.poll_unpin(&mut cx);
            }
            if let Poll::Ready(Some(Ok(s))) = recv.poll(&mut cx) {
                streams.push(s);
            }
        }

        // 顺序句柄:sids 2,4,…,512(N=2 下入站偶 sid 全在 worker0,
        // 交付顺序即 wire 顺序)
        for (i, s) in streams.iter().enumerate() {
            assert_eq!(
                s.id(),
                StreamId::new(2 * (i as u32 + 1)),
                "handle #{} out of order",
                i
            );
        }
        assert_eq!(
            shared.stream_count.load(Ordering::SeqCst),
            N as usize,
            "stream_count must equal 256"
        );

        // 全关 → (id,None) → SendWorker 消费 → ctrl Remove → RecvWorker
        // finalize → 计数归零(round-6:命令消费与 on_drop 都在 SendWorker)
        drop(streams);
        let mut guard = 0;
        while shared.stream_count.load(Ordering::SeqCst) != 0 {
            guard += 1;
            assert!(
                guard < 8192,
                "T-H2 removal stalled at {}",
                shared.stream_count.load(Ordering::SeqCst)
            );
            let _ = send.poll(&mut cx);
            for w in workers.iter_mut() {
                let _ = w.poll_unpin(&mut cx);
            }
            for w in send_workers.iter_mut() {
                let _ = w.poll_unpin(&mut cx);
            }
            // ctrl Remove 经 RecvWorker finalize 入 to_send,终轮写穿并
            // 释放计数;多轮驱动让 (id,None)→Remove→finalize 链走穿
            for w in workers.iter_mut() {
                let _ = w.poll_unpin(&mut cx);
            }
            let _ = send.poll(&mut cx);
        }
        assert_eq!(
            shared.ack_backlog_count.load(Ordering::SeqCst),
            0,
            "ack_backlog must be zero (no outbound streams)"
        );
    }
}
