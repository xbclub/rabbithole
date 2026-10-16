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

use crate::connection::{Id, Mode, State};
use crate::frame::header::{self, Header, StreamId, Tag};
use crate::frame::Frame;
use crate::{Config, ConnectionError, Stream, DEFAULT_CREDIT};
use futures::channel::mpsc;
use nohash_hasher::IntMap;
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use super::ConnShared;

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

/// Worker→Framer 建流交付通道容量(D6:accept 背压链的一环)。
pub(super) const NEW_STREAMS_CAP: usize = 16;

/// 通道共享状态:有界队列 + 双 waker 槽 + 消费者存活标志。
struct ChannelShared<T> {
    queue: Mutex<VecDeque<T>>,
    cap: usize,
    producer_waker: Mutex<Option<Waker>>,
    consumer_waker: Mutex<Option<Waker>>,
    consumer_gone: AtomicBool,
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
        if was_full && still < self.shared.cap {
            // 满变非满:生产侧已登记才唤醒(take 语义)
            if let Some(w) = self.shared.producer_waker.lock().take() {
                w.wake();
            }
        }
        n
    }

    /// 空闲时登记消费者 waker(生产者压入时唤醒;登记后调用方必须复查
    /// 队列非空,见 §2.1 防线)。
    fn register_consumer_waker(&self, w: &Waker) {
        *self.shared.consumer_waker.lock() = Some(w.clone());
    }

    /// 复查探测(丢唤醒防线):队列当前非空。
    fn has_pending(&self) -> bool {
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
        let (sender, receiver) = mpsc::channel(10); // 10 is an arbitrary number.
        self.shared.push_new_receiver(id, receiver);
        Stream::new_inbound(id, self.id, self.config.clone(), credit, sender)
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

    /// 分片 halves 测试夹具:N 个 Worker 的 halves 三件套 + 脚本载体 +
    /// 共享状态句柄(计数器/开流表断言用)。
    struct Fixture {
        recv: RecvHalfForTest,
        send: SendHalfForTest,
        workers: Vec<ShardWorker>,
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
            let (recv, send, workers) = conn.into_halves();
            let shared = recv.shared_for_test();
            debug_assert_eq!(workers.len(), n);
            Fixture {
                recv,
                send,
                workers,
                st,
                shared,
                cx: noop_cx(),
            }
        }

        fn feed(&self, block: Vec<u8>) {
            self.st.borrow_mut().read_blocks.push_back(block);
        }

        /// 驱动 recv + workers 一轮收敛(单线程手动推进,等价生产侧
        /// tokio::spawn 的并行):recv 排空路由 → workers 全处理 →
        /// recv 再探(new_streams 交付)。
        fn drive_recv(&mut self) -> Poll<Option<crate::Result<Stream>>> {
            let cx = &mut self.cx;
            match self.recv.poll(cx) {
                Poll::Pending => {}
                other => return other,
            }
            for w in self.workers.iter_mut() {
                let _ = w.poll_unpin(cx);
            }
            self.recv.poll(cx)
        }

        /// 驱动 send + workers 一轮收敛(Worker 压入 to_send 的 RST/FIN
        /// 由终轮 send poll 写上 wire)。
        fn drive_send(&mut self) -> Poll<crate::Result<()>> {
            let cx = &mut self.cx;
            match self.send.poll(cx) {
                Poll::Pending => {}
                other => return other,
            }
            for w in self.workers.iter_mut() {
                let _ = w.poll_unpin(cx);
            }
            self.send.poll(cx)
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
        let (mut recv, mut send, mut workers) = conn.into_halves();

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
        match send.poll(&mut cx) {
            Poll::Pending => {} // flush 后 park
            other => panic!("send half must park, got {other:?}"),
        }
        // 再驱动一轮 worker/send 确认无第二帧
        for w in workers.iter_mut() {
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
        let (mut recv, mut send, mut workers) = conn.into_halves();
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
        let (mut recv_a, mut send_a, mut workers_a) = conn_a.into_halves();
        let (socket_b, st_b) = ScriptSocket::new();
        let conn_b = Connection::new(socket_b, cfg, Mode::Server);
        let (mut recv_b, mut send_b, mut workers_b) = conn_b.into_halves();
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

            // 驱动双侧:send flush → workers 全处理 → recv 交付(×2 轮
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
        let (mut recv, mut send, mut workers) = conn.into_halves();
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

        // 全关 → (id,None) → ctrl Remove → 计数归零
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
            let _ = send.poll(&mut cx);
        }
        assert_eq!(
            shared.ack_backlog_count.load(Ordering::SeqCst),
            0,
            "ack_backlog must be zero (no outbound streams)"
        );
    }
}
