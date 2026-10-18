// Copyright (c) 2018-2019 Parity Technologies (UK) Ltd.
//
// Licensed under the Apache License, Version 2.0 or MIT license, at your option.
//
// A copy of the Apache License, Version 2.0 is included in the software as
// LICENSE-APACHE and a copy of the MIT license is included in the software
// as LICENSE-MIT. You may also obtain a copy of the Apache License, Version 2.0
// at https://www.apache.org/licenses/LICENSE-2.0 and a copy of the MIT license
// at https://opensource.org/licenses/MIT.

use crate::frame::header::ACK;
use crate::ConnectionError;
use crate::{
    chunks::Chunks,
    connection::{self, StreamCommand},
    frame::{
        header::{Data, Header, StreamId, WindowUpdate},
        Frame,
    },
    Config, DEFAULT_CREDIT,
};
use bytes::BufMut;
use flow_control::FlowController;
// WU 快车道(round-9 §2):流持连接级共享状态句柄,纯 WU 帧直压 to_send。
// halves 是 connection 的私有子模块,stream 同为其后代,可见性成立。
use super::halves::ConnShared;
use futures::{
    channel::mpsc,
    future::Either,
    io::{AsyncRead, AsyncWrite},
    ready, SinkExt,
};
use parking_lot::{Mutex, MutexGuard};
use std::{
    fmt, io,
    pin::Pin,
    sync::atomic::{AtomicUsize, Ordering},
    sync::Arc,
    task::{Context, Poll, Waker},
};

mod flow_control;

/// The state of a Yamux stream.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum State {
    /// Open bidirectionally.
    Open {
        /// Whether the stream is acknowledged.
        ///
        /// For outbound streams, this tracks whether the remote has acknowledged our stream.
        /// For inbound streams, this tracks whether we have acknowledged the stream to the remote.
        ///
        /// This starts out with `false` and is set to `true` when we receive or send an `ACK` flag for this stream.
        /// We may also directly transition:
        /// - from `Open` to `RecvClosed` if the remote immediately sends `FIN`.
        /// - from `Open` to `Closed` if the remote immediately sends `RST`.
        acknowledged: bool,
    },
    /// Open for incoming messages.
    SendClosed,
    /// Open for outgoing messages.
    RecvClosed,
    /// Closed (terminal state).
    Closed,
}

impl State {
    /// Can we receive messages over this stream?
    pub fn can_read(self) -> bool {
        !matches!(self, State::RecvClosed | State::Closed)
    }

    /// Can we send messages over this stream?
    pub fn can_write(self) -> bool {
        !matches!(self, State::SendClosed | State::Closed)
    }
}

/// Indicate if a flag still needs to be set on an outbound header.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub(crate) enum Flag {
    /// No flag needs to be set.
    None,
    /// The stream was opened lazily, so set the initial SYN flag.
    Syn,
    /// The stream still needs acknowledgement, so set the ACK flag.
    Ack,
}

/// A multiplexed Yamux stream.
///
/// Streams are created either outbound via [`crate::Connection::poll_new_outbound`]
/// or inbound via [`crate::Connection::poll_next_inbound`].
///
/// `Stream` implements [`AsyncRead`] and [`AsyncWrite`] and also
/// [`futures::stream::Stream`].
pub struct Stream {
    id: StreamId,
    conn: connection::Id,
    config: Arc<Config>,
    /// Sends the commands of the writing side of this stream to the connection.
    sender: mpsc::Sender<StreamCommand>,
    /// Sends the window updates of the reading side of this stream to the connection.
    ///
    /// This is a second handle to the same channel as [`Stream::sender`]. A
    /// [`mpsc::Sender`] has room for exactly one waker, which it overwrites whenever the
    /// channel is full. Sharing a single handle between the two halves of a split stream
    /// therefore makes the half polled last discard the waker of the other one, which is then
    /// never woken again.
    window_update_sender: mpsc::Sender<StreamCommand>,
    flag: Flag,
    shared: Arc<Mutex<Shared>>,
    /// 连接级共享状态(round-9 WU 快车道):Some = halves 路径,无 flag 的
    /// 纯 WU 帧经 `ConnShared::push_to_send` 直入 to_send(FlushTask 下一轮
    /// 即取,消除流级 mpsc 排队 + SendWorker 中间跳);None = 未拆分路径
    /// (Active,无 ConnShared)与单测直构形态,保持旧 mpsc 通路。
    /// 带 flag(Syn/Ack)的建流授信帧与任何 Data/FIN/RST 永不走快车道
    /// (红线:流内 FIFO 顺序契约)。
    conn_shared: Option<Arc<ConnShared>>,
}

impl fmt::Debug for Stream {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Stream")
            .field("id", &self.id.val())
            .field("connection", &self.conn)
            .finish()
    }
}

impl fmt::Display for Stream {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "(Stream {}/{})", self.conn, self.id.val())
    }
}

impl Stream {
    pub(crate) fn new_inbound(
        id: StreamId,
        conn: connection::Id,
        config: Arc<Config>,
        send_window: u32,
        sender: mpsc::Sender<StreamCommand>,
        conn_shared: Option<Arc<ConnShared>>,
    ) -> Self {
        let mut stream = Self {
            id,
            conn,
            config: config.clone(),
            window_update_sender: sender.clone(),
            sender,
            flag: Flag::Ack,
            shared: Arc::new(Mutex::new(Shared::new(DEFAULT_CREDIT, send_window, config))),
            conn_shared,
        };
        stream.queue_creation_grant();
        stream
    }

    pub(crate) fn new_outbound(
        id: StreamId,
        conn: connection::Id,
        config: Arc<Config>,
        sender: mpsc::Sender<StreamCommand>,
        conn_shared: Option<Arc<ConnShared>>,
    ) -> Self {
        let mut stream = Self {
            id,
            conn,
            config: config.clone(),
            window_update_sender: sender.clone(),
            sender,
            flag: Flag::Syn,
            shared: Arc::new(Mutex::new(Shared::new(
                DEFAULT_CREDIT,
                DEFAULT_CREDIT,
                config,
            ))),
            conn_shared,
        };
        stream.queue_creation_grant();
        stream
    }

    /// 建流即授信(round-4/B1,对齐 frp OpenStream/AcceptStream 时机):
    /// 构造完成后立即把预授差额(max_receive_window − DEFAULT_CREDIT)以带
    /// 初始 flag 的窗口更新帧排队授出——outbound SYN|WU 合帧(hashicorp
    /// OpenStream 的 initialStreamFlags 行为)、inbound ACK|WU(镜像
    /// `into_halves` 的 ACK 副作用记账),均为 upstream 合法组合,不再等
    /// 读路径首个窗口更新 poll。通道在 make_new_*_stream 皆为容量
    /// `STREAM_COMMAND_CAP` 的全新通道,建流时必空,try_send 必成;失败兜底
    /// (测试以容量 0 通道直接构造流的用法)不得丢授信——flag 恢复原值
    /// (Syn/Ack)、pregrant_pending 恢复 true,行为退回旧路径(读 poll
    /// 授信 / into_halves 补 ACK)。
    fn queue_creation_grant(&mut self) {
        let delta = self.shared.lock().flow_controller.take_creation_grant();
        if delta == 0 {
            // 无预授(预算钳制回退/已授出):flag 不动,维持现状——
            // lazy SYN 由首数据帧携带、ACK 由 into_halves 补。
            return;
        }
        let prev_flag = self.flag;
        let mut frame = Frame::window_update(self.id, delta).right();
        self.add_flag(frame.header_mut());
        let cmd = StreamCommand::SendFrame(frame);
        if self.window_update_sender.try_send(cmd).is_ok() {
            if prev_flag == Flag::Ack {
                self.shared()
                    .update_state(self.conn, self.id, State::Open { acknowledged: true });
            }
        } else {
            // 回滚:flag 与预授记账复原,授信交还旧路径
            self.flag = prev_flag;
            self.shared
                .lock()
                .flow_controller
                .restore_creation_grant(delta);
            log::debug!(
                "{}/{}: creation grant frame not queued, deferring to read-poll grant",
                self.conn,
                self.id
            );
        }
    }

    /// Get this stream's identifier.
    pub fn id(&self) -> StreamId {
        self.id
    }

    pub fn is_write_closed(&self) -> bool {
        matches!(self.shared().state(), State::SendClosed)
    }

    pub fn is_closed(&self) -> bool {
        matches!(self.shared().state(), State::Closed)
    }

    /// Whether we are still waiting for the remote to acknowledge this stream.
    pub fn is_pending_ack(&self) -> bool {
        self.shared().is_pending_ack()
    }

    pub(crate) fn shared(&self) -> MutexGuard<'_, Shared> {
        self.shared.lock()
    }

    pub(crate) fn clone_shared(&self) -> Arc<Mutex<Shared>> {
        self.shared.clone()
    }

    fn write_zero_err(&self) -> io::Error {
        let msg = format!("{}/{}: connection is closed", self.conn, self.id);
        io::Error::new(io::ErrorKind::WriteZero, msg)
    }

    /// Set ACK or SYN flag if necessary.
    fn add_flag(&mut self, header: &mut Header<Either<Data, WindowUpdate>>) {
        match self.flag {
            Flag::None => (),
            Flag::Syn => {
                header.syn();
                self.flag = Flag::None
            }
            Flag::Ack => {
                header.ack();
                self.flag = Flag::None
            }
        }
    }

    /// Send new credit to the sending side via a window update message if
    /// permitted.
    fn send_window_update(&mut self, cx: &mut Context) -> Poll<io::Result<()>> {
        if !self.shared.lock().state.can_read() {
            return Poll::Ready(Ok(()));
        }

        // 快车道(round-9 §2):无 flag 的纯 WU 帧直压 to_send——纯信用
        // 增量不携带数据/控制语义,不属于流内保序范畴,且 push_to_send
        // 压入即取 send_waker 槽唤醒 FlushTask(优先级最高,下一轮即出),
        // 无丢唤醒;产生速率受接收速率限制(收满半窗才一帧),天然有界。
        // 带 flag(Syn/Ack)的 WU(建流授信余量)必须走旧 mpsc 通路保
        // 流内 FIFO;next_window_update 的记账副作用紧跟 infallible 的
        // 压入,无"已记账未发出"窗口。
        if self.flag == Flag::None {
            if let Some(shared) = &self.conn_shared {
                let Some(credit) = self.shared.lock().next_window_update() else {
                    return Poll::Ready(Ok(()));
                };
                let frame: Frame<Either<Data, WindowUpdate>> =
                    Frame::window_update(self.id, credit).right();
                shared.push_to_send(frame.into());
                return Poll::Ready(Ok(()));
            }
        }

        ready!(self
            .window_update_sender
            .poll_ready(cx)
            .map_err(|_| self.write_zero_err())?);

        let Some(credit) = self.shared.lock().next_window_update() else {
            return Poll::Ready(Ok(()));
        };

        let mut frame = Frame::window_update(self.id, credit).right();
        self.add_flag(frame.header_mut());
        let cmd = StreamCommand::SendFrame(frame);
        self.window_update_sender
            .start_send(cmd)
            .map_err(|_| self.write_zero_err())?;

        Poll::Ready(Ok(()))
    }

    /// 零拷贝读:取出接收缓冲链首一整块 [`bytes::Bytes`](引用计数切片)。
    /// 语义与 [`futures::stream::Stream`] 实现一致(含窗口更新与 EOF);
    /// `Ok(None)` = EOF/流复位。rabbithole vendor 转发路径专用。
    pub fn poll_read_chunk(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
    ) -> Poll<io::Result<Option<bytes::Bytes>>> {
        if !self.config.read_after_close && self.sender.is_closed() {
            return Poll::Ready(Ok(None));
        }
        match self.send_window_update(cx) {
            Poll::Ready(Ok(())) => {}
            // C3:命令通道已断(连接关闭收尾)。已缓冲数据必须仍可读
            // (read_after_close 契约):缓冲非空时继续交付,耗尽才传播错误。
            Poll::Ready(Err(e)) => {
                if self.shared.lock().buffer.is_empty() {
                    return Poll::Ready(Err(e));
                }
            }
            // 窗口更新暂不可发:继续读已缓冲数据
            Poll::Pending => {}
        }
        let mut shared = self.shared();
        if let Some(bytes) = shared.buffer.pop() {
            return Poll::Ready(Ok(Some(bytes)));
        }
        if !shared.state().can_read() {
            log::debug!("{}/{}: eof", self.conn, self.id);
            return Poll::Ready(Ok(None));
        }
        shared.reader = Some(cx.waker().clone());
        Poll::Pending
    }

    /// 零拷贝写:把 `chunk` 整块(或受窗口/分片上限截断的前缀)作为帧体,
    /// 不经 [`crate::pool`] 拷入。所有权经 `chunk: &mut Option<Bytes>` 交接:
    /// * 成功消费 k 字节:整块消费时置 `None`(保持引用计数 1,帧写穿后
    ///   可归池);部分消费时置 `Some(剩余)`(slice 视图,本轮不归池);
    /// * `Poll::Pending`(窗口耗尽/通道满):`chunk` 原样保留。
    ///
    /// 返回本次消费字节数(0 = 空块)。rabbithole vendor 转发路径专用。
    pub fn poll_write_chunk(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
        chunk: &mut Option<bytes::Bytes>,
    ) -> Poll<io::Result<usize>> {
        let Some(bytes) = chunk.as_ref() else {
            return Poll::Ready(Ok(0));
        };
        ready!(self
            .sender
            .poll_ready(cx)
            .map_err(|_| self.write_zero_err())?);
        let body = {
            let mut shared = self.shared();
            if !shared.state().can_write() {
                log::debug!("{}/{}: can no longer write", self.conn, self.id);
                return Poll::Ready(Err(self.write_zero_err()));
            }
            if shared.send_window() == 0 {
                log::trace!("{}/{}: no more credit left", self.conn, self.id);
                shared.writer = Some(cx.waker().clone());
                return Poll::Pending;
            }
            let k = std::cmp::min(shared.send_window() as usize, bytes.len());
            let k = std::cmp::min(k, self.config.split_send_size);
            shared
                .consume_send_window(k as u32)
                .expect("not exceed receive window");
            if k == bytes.len() {
                chunk.take().expect("checked above")
            } else {
                let rest = bytes.slice(k..);
                let head = bytes.slice(..k);
                *chunk = Some(rest);
                head
            }
        };
        let n = body.len();
        let mut frame = Frame::data(self.id, body).expect("body <= u32::MAX").left();
        self.add_flag(frame.header_mut());
        log::trace!("{}/{}: write chunk {} bytes", self.conn, self.id, n);
        if frame.header().flags().contains(ACK) {
            self.shared()
                .update_state(self.conn, self.id, State::Open { acknowledged: true });
        }
        let cmd = StreamCommand::SendFrame(frame);
        self.sender
            .start_send(cmd)
            .map_err(|_| self.write_zero_err())?;
        Poll::Ready(Ok(n))
    }

    /// 拆分为读半/写半(各归一个任务; halves 只应使用对应方向的方法)。
    /// 共享同一条流的底层状态;发送通道/窗口更新通道各持独立句柄
    /// (同 `Stream` 字段注释:waker 不互踩)。rabbithole vendor 专用。
    pub fn into_halves(self) -> (StreamReadHalf, StreamWriteHalf) {
        let Stream {
            id,
            conn,
            config,
            sender,
            mut window_update_sender,
            flag,
            shared,
            conn_shared,
        } = self;
        // C2:拆半后 ACK flag 随写半,而读半的窗口更新不携带 ACK——
        // 小流量流迟迟不触发窗口更新、写半首写又未到来时,ACK 憋在
        // 写半,对端 ack_backlog(MAX_ACK_BACKLOG)打满会使 open_stream
        // 挂起。拆半时立即经既有命令通道排队一个 **0-credit 窗口更新
        // + ACK**(ACK-on-WindowUpdate 是 upstream 既有合法组合,入站流
        // 本就以该机制回 ACK;0-credit 经对端 increase_send_window_by
        // 的 checked_add(0) 恒不溢出,不会触发 Terminate)。
        // 补修:try_send 失败(通道满/已断)时**不得**回落为"仅写半可
        // 发"的旧缺陷形态——ACK 转入 shared.ack_pending,读半窗口更新
        // /补发路径与写半后续任何外发帧都会携带它(见各消费点)。
        let mut flag = flag;
        if flag == Flag::Ack {
            let mut header = Header::window_update(id, 0);
            header.ack();
            let cmd = StreamCommand::SendFrame(Frame::new(header).right());
            let sent = window_update_sender.try_send(cmd).is_ok();
            if sent {
                shared
                    .lock()
                    .update_state(conn, id, State::Open { acknowledged: true });
            } else {
                shared.lock().ack_pending = true;
            }
            flag = Flag::None;
        }
        (
            StreamReadHalf {
                id,
                conn,
                config: config.clone(),
                sender: sender.clone(),
                window_update_sender,
                shared: shared.clone(),
                conn_shared: conn_shared.clone(),
            },
            StreamWriteHalf {
                id,
                conn,
                config,
                sender,
                flag,
                shared,
                conn_shared,
            },
        )
    }
}

/// [`Stream`] 读半:仅读方向(chunk/窗口更新/EOF)。
pub struct StreamReadHalf {
    id: StreamId,
    conn: connection::Id,
    config: Arc<Config>,
    sender: mpsc::Sender<StreamCommand>,
    window_update_sender: mpsc::Sender<StreamCommand>,
    shared: Arc<Mutex<Shared>>,
    /// 同 [`Stream::conn_shared`]:读半是运行期 WU 帧的唯一产生侧。
    conn_shared: Option<Arc<ConnShared>>,
}

impl StreamReadHalf {
    /// 同 [`Stream::poll_read_chunk`]。
    pub fn poll_read_chunk(&mut self, cx: &mut Context) -> Poll<io::Result<Option<bytes::Bytes>>> {
        let __sd = crate::sched_diag::enabled();
        let skip_wu = crate::sched_diag::skip_wu();
        if __sd { crate::sched_diag::on_poll(); crate::sched_diag::begin_poll(); }
        if !self.config.read_after_close && self.sender.is_closed() {
            if __sd { crate::sched_diag::end_poll(); }
            return Poll::Ready(Ok(None));
        }
        match if skip_wu { Poll::Ready(Ok(())) } else { self.send_window_update(cx) } {
            Poll::Ready(Ok(())) => {}
            // C3:同 Stream::poll_read_chunk——缓冲优先于通道错误交付。
            Poll::Ready(Err(e)) => {
                if self.shared.lock().buffer.is_empty() {
                    if __sd { crate::sched_diag::end_poll(); }
                    return Poll::Ready(Err(e));
                }
            }
            Poll::Pending => {}
        }
        let mut shared = self.shared();
        if let Some(bytes) = shared.buffer.pop() {
            if __sd { crate::sched_diag::end_poll(); }
            return Poll::Ready(Ok(Some(bytes)));
        }
        if !shared.state().can_read() {
            log::debug!("{}/{}: eof", self.conn, self.id);
            if __sd { crate::sched_diag::end_poll(); }
            return Poll::Ready(Ok(None));
        }
        // round-11:park 前在**同一把锁内**完成"注册 waker + 复查 buffer"。
        // 原实现先 `buffer.pop()`(已持有 shared 锁,put 后释放),
        // 再 `shared.reader = waker` —— 两步之间若 ShardWorker 已 push
        // 并 take 走 reader(那时还没注册),本任务会永 park(丢失唤醒)。
        // 这里改为:注册 waker 后立刻在同一锁内复查 buffer,非空则
        // 撤销注册并直接交付。既修丢失唤醒,也减少空唤醒后的无效 park。
        shared.reader = Some(cx.waker().clone());
        if let Some(bytes) = shared.buffer.pop() {
            shared.reader = None; // 撤销注册:本 poll 已直接交付
            if __sd { crate::sched_diag::end_poll(); }
            return Poll::Ready(Ok(Some(bytes)));
        }
        if crate::sched_diag::enabled() { crate::sched_diag::on_park(); }
        if __sd { crate::sched_diag::end_poll(); }
        Poll::Pending
    }

    fn send_window_update(&mut self, cx: &mut Context) -> Poll<io::Result<()>> {
        if !self.shared.lock().state.can_read() {
            return Poll::Ready(Ok(()));
        }
        // 快车道(round-9 §2):无 flag 且无待补 ACK 的纯 WU 帧直压
        // to_send(ack_pending 的 0-credit WU 携带 ACK,属带 flag 帧,
        // 必须走旧 mpsc 通路——见 C2 补修注释)。记账副作用紧跟
        // infallible 压入,无"已记账未发出"窗口。
        let ack_pending = self.shared.lock().ack_pending;
        if !ack_pending {
            if let Some(shared) = &self.conn_shared {
                let Some(credit) = self.shared.lock().next_window_update() else {
                    return Poll::Ready(Ok(()));
                };
                let frame: Frame<Either<Data, WindowUpdate>> =
                    Frame::window_update(self.id, credit).right();
                shared.push_to_send(frame.into());
                return Poll::Ready(Ok(()));
            }
        }
        // C2 补修:除常规窗口更新外,共享待 ACK 状态也由此路径补发——
        // 即使无 credit 可授(小流量流),也发 0-credit WU+ACK,使 ACK
        // 不依赖写半的活动。队列满时 poll_ready Pending 注册 waker,
        // 容量释放后被唤醒重试(正确重试,不丢状态)。
        // 注意 `next_window_update` 有记账副作用(receive_window 推进),
        // 必须在容量确认(poll_ready)之后调用,否则队列满时 credit
        // 已记账而帧未发出,窗口永久丢失。
        ready!(self
            .window_update_sender
            .poll_ready(cx)
            .map_err(|_| self.write_zero_err())?);
        let credit = self.shared.lock().next_window_update();
        if credit.is_none() && !ack_pending {
            return Poll::Ready(Ok(()));
        }
        let frame = {
            let mut shared = self.shared();
            let mut header = Header::window_update(self.id, credit.unwrap_or(0));
            if shared.ack_pending {
                shared.ack_pending = false;
                header.ack();
            }
            header
        };
        if frame.flags().contains(ACK) {
            self.shared()
                .update_state(self.conn, self.id, State::Open { acknowledged: true });
        }
        let cmd = StreamCommand::SendFrame(Frame::new(frame).right());
        self.window_update_sender
            .start_send(cmd)
            .map_err(|_| self.write_zero_err())?;
        Poll::Ready(Ok(()))
    }

    fn shared(&self) -> MutexGuard<'_, Shared> {
        self.shared.lock()
    }

    fn write_zero_err(&self) -> io::Error {
        io::Error::new(
            io::ErrorKind::WriteZero,
            format!("{}/{}: connection is closed", self.conn, self.id),
        )
    }
}

/// [`Stream`] 写半:仅写方向(chunk 写/FIN 半关闭)。
pub struct StreamWriteHalf {
    id: StreamId,
    conn: connection::Id,
    config: Arc<Config>,
    sender: mpsc::Sender<StreamCommand>,
    flag: Flag,
    shared: Arc<Mutex<Shared>>,
    /// 同 [`Stream::conn_shared`]:写半不产生 WU 帧,仅为 halves 形态
    /// 对称持有(随拆半继承,保持两半共享同一连接级状态句柄)。
    #[allow(dead_code)] // 快车道只服务读侧;写侧无消费点(规格 §2.2 要求三结构同字段)
    conn_shared: Option<Arc<ConnShared>>,
}

impl StreamWriteHalf {
    /// 同 [`Stream::poll_write_chunk`]。
    pub fn poll_write_chunk(
        &mut self,
        cx: &mut Context,
        chunk: &mut Option<bytes::Bytes>,
    ) -> Poll<io::Result<usize>> {
        let Some(bytes) = chunk.as_ref() else {
            return Poll::Ready(Ok(0));
        };
        ready!(self
            .sender
            .poll_ready(cx)
            .map_err(|_| self.write_zero_err())?);
        let body = {
            let mut shared = self.shared();
            if !shared.state().can_write() {
                return Poll::Ready(Err(self.write_zero_err()));
            }
            if shared.send_window() == 0 {
                shared.writer = Some(cx.waker().clone());
                return Poll::Pending;
            }
            let k = std::cmp::min(shared.send_window() as usize, bytes.len());
            let k = std::cmp::min(k, self.config.split_send_size);
            shared
                .consume_send_window(k as u32)
                .expect("not exceed receive window");
            if k == bytes.len() {
                chunk.take().expect("checked above")
            } else {
                let rest = bytes.slice(k..);
                let head = bytes.slice(..k);
                *chunk = Some(rest);
                head
            }
        };
        let n = body.len();
        let mut frame = Frame::data(self.id, body).expect("body <= u32::MAX").left();
        self.add_flag(frame.header_mut());
        // C2 补修:共享待 ACK 随数据帧携带(拆半时未排出的 ACK)
        {
            let mut shared = self.shared();
            if shared.ack_pending {
                shared.ack_pending = false;
                frame.header_mut().ack();
            }
        }
        if frame.header().flags().contains(ACK) {
            self.shared()
                .update_state(self.conn, self.id, State::Open { acknowledged: true });
        }
        let cmd = StreamCommand::SendFrame(frame);
        self.sender
            .start_send(cmd)
            .map_err(|_| self.write_zero_err())?;
        Poll::Ready(Ok(n))
    }

    /// FIN 半关闭(对应 [`AsyncWrite::poll_close`])。
    pub fn poll_close_half(&mut self, cx: &mut Context) -> Poll<io::Result<()>> {
        if matches!(self.shared().state(), State::Closed) {
            return Poll::Ready(Ok(()));
        }
        if let State::SendClosed = self.shared().state() {
            // 重复 close 幂等:写半已关不再排队第二个 FIN
            return Poll::Ready(Ok(()));
        }
        ready!(self
            .sender
            .poll_ready(cx)
            .map_err(|_| self.write_zero_err())?);
        let mut ack = if self.flag == Flag::Ack {
            self.flag = Flag::None;
            true
        } else {
            false
        };
        // final-gaps 补修(close-before-first-write):lazy 开流的 SYN 若尚未
        // 随任何数据帧发出,必须随 FIN 合帧发出(SYN|FIN 是标准 yamux wire
        // 组合),否则对端从未见到开流,accept 永不就绪。flag 仅在容量确认
        // (poll_ready)之后消费——背压(Pending)/错误下不丢 SYN、不重复消费。
        let syn = if self.flag == Flag::Syn {
            self.flag = Flag::None;
            true
        } else {
            false
        };
        // C2 补修:共享待 ACK 随 FIN 携带(CloseStream 的 ack 形态,
        // 即 Frame::close_stream(id, ack) 的 Data+FIN+ACK 既有组合)
        {
            let mut shared = self.shared();
            if shared.ack_pending {
                shared.ack_pending = false;
                ack = true;
            }
        }
        if ack {
            self.shared()
                .update_state(self.conn, self.id, State::Open { acknowledged: true });
        }
        log::trace!("{}/{}: close", self.conn, self.id);
        let cmd = StreamCommand::CloseStream { ack, syn };
        self.sender
            .start_send(cmd)
            .map_err(|_| self.write_zero_err())?;
        self.shared()
            .update_state(self.conn, self.id, State::SendClosed);
        Poll::Ready(Ok(()))
    }

    fn add_flag(&mut self, header: &mut Header<Either<Data, WindowUpdate>>) {
        match self.flag {
            Flag::None => (),
            Flag::Syn => {
                header.syn();
                self.flag = Flag::None
            }
            Flag::Ack => {
                header.ack();
                self.flag = Flag::None
            }
        }
    }

    fn shared(&self) -> MutexGuard<'_, Shared> {
        self.shared.lock()
    }

    fn write_zero_err(&self) -> io::Error {
        io::Error::new(
            io::ErrorKind::WriteZero,
            format!("{}/{}: connection is closed", self.conn, self.id),
        )
    }
}

/// Byte data produced by the [`futures::stream::Stream`] impl of [`Stream`].
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Packet(bytes::Bytes);

impl AsRef<[u8]> for Packet {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl From<Packet> for bytes::Bytes {
    fn from(p: Packet) -> Self {
        p.0
    }
}

impl futures::stream::Stream for Stream {
    type Item = io::Result<Packet>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        if !self.config.read_after_close && self.sender.is_closed() {
            return Poll::Ready(None);
        }

        match self.send_window_update(cx) {
            Poll::Ready(Ok(())) => {}
            // C3:缓冲优先于通道错误交付(read_after_close 契约)。
            Poll::Ready(Err(e)) => {
                if self.shared.lock().buffer.is_empty() {
                    return Poll::Ready(Some(Err(e)));
                }
            }
            // Continue reading buffered data even though sending a window update blocked.
            Poll::Pending => {}
        }

        let mut shared = self.shared();

        if let Some(bytes) = shared.buffer.pop() {
            return Poll::Ready(Some(Ok(Packet(bytes))));
        }

        // Buffer is empty, let's check if we can expect to read more data.
        if !shared.state().can_read() {
            log::debug!("{}/{}: eof", self.conn, self.id);
            return Poll::Ready(None); // stream has been reset
        }

        // Since we have no more data at this point, we want to be woken up
        // by the connection when more becomes available for us.
        shared.reader = Some(cx.waker().clone());

        Poll::Pending
    }
}

// Like the `futures::stream::Stream` impl above, but copies bytes into the
// provided mutable slice.
impl AsyncRead for Stream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        if !self.config.read_after_close && self.sender.is_closed() {
            return Poll::Ready(Ok(0));
        }

        match self.send_window_update(cx) {
            Poll::Ready(Ok(())) => {}
            // C3:缓冲优先于通道错误交付(read_after_close 契约)。
            Poll::Ready(Err(e)) => {
                if self.shared.lock().buffer.is_empty() {
                    return Poll::Ready(Err(e));
                }
            }
            // Continue reading buffered data even though sending a window update blocked.
            Poll::Pending => {}
        }

        // Copy data from stream buffer.
        let mut shared = self.shared();
        let mut n = 0;
        while let Some(chunk) = shared.buffer.front() {
            if chunk.is_empty() {
                shared.buffer.pop();
                continue;
            }
            let k = std::cmp::min(chunk.len(), buf.len() - n);
            buf[n..n + k].copy_from_slice(&chunk[..k]);
            n += k;
            shared.buffer.advance_front(k);
            if n == buf.len() {
                break;
            }
        }

        if n > 0 {
            log::trace!("{}/{}: read {} bytes", self.conn, self.id, n);
            return Poll::Ready(Ok(n));
        }

        // Buffer is empty, let's check if we can expect to read more data.
        if !shared.state().can_read() {
            log::debug!("{}/{}: eof", self.conn, self.id);
            return Poll::Ready(Ok(0)); // stream has been reset
        }

        // Since we have no more data at this point, we want to be woken up
        // by the connection when more becomes available for us.
        shared.reader = Some(cx.waker().clone());

        Poll::Pending
    }
}

impl AsyncWrite for Stream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        ready!(self
            .sender
            .poll_ready(cx)
            .map_err(|_| self.write_zero_err())?);
        let body = {
            let mut shared = self.shared();
            if !shared.state().can_write() {
                log::debug!("{}/{}: can no longer write", self.conn, self.id);
                return Poll::Ready(Err(self.write_zero_err()));
            }
            if shared.send_window() == 0 {
                log::trace!("{}/{}: no more credit left", self.conn, self.id);
                shared.writer = Some(cx.waker().clone());
                return Poll::Pending;
            }
            let k = std::cmp::min(shared.send_window() as usize, buf.len());
            let k = std::cmp::min(k, self.config.split_send_size);
            shared
                .consume_send_window(k as u32)
                .expect("not exceed receive window");
            // 池化缓冲:消除逐帧 64KiB 分配/释放 + 清零
            let mut bm = crate::pool::get(k);
            bm.put_slice(&buf[..k]);
            bm.freeze()
        };
        let n = body.len();
        let mut frame = Frame::data(self.id, body).expect("body <= u32::MAX").left();
        self.add_flag(frame.header_mut());
        log::trace!("{}/{}: write {} bytes", self.conn, self.id, n);

        // technically, the frame hasn't been sent yet on the wire but from the perspective of this data structure, we've queued the frame for sending
        // We are tracking this information:
        // a) to be consistent with outbound streams
        // b) to correctly test our behaviour around timing of when ACKs are sent. See `ack_timing.rs` test.
        if frame.header().flags().contains(ACK) {
            self.shared()
                .update_state(self.conn, self.id, State::Open { acknowledged: true });
        }

        let cmd = StreamCommand::SendFrame(frame);
        self.sender
            .start_send(cmd)
            .map_err(|_| self.write_zero_err())?;
        Poll::Ready(Ok(n))
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<io::Result<()>> {
        self.sender
            .poll_flush_unpin(cx)
            .map_err(|_| self.write_zero_err())
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<io::Result<()>> {
        if self.is_closed() {
            return Poll::Ready(Ok(()));
        }
        // final-gaps 补修:重复 close 幂等——写半已关即返回,不再排队
        // 第二个 FIN(与 poll_close_half 语义对齐)。
        if let State::SendClosed = self.shared().state() {
            return Poll::Ready(Ok(()));
        }
        ready!(self
            .sender
            .poll_ready(cx)
            .map_err(|_| self.write_zero_err())?);
        let mut ack = if self.flag == Flag::Ack {
            self.flag = Flag::None;
            true
        } else {
            false
        };
        // final-gaps 补修(close-before-first-write):未随数据帧发出的 SYN
        // 必须随 FIN 合帧发出(SYN|FIN 是标准 yamux wire 组合),否则对端
        // 从未见到开流、accept 永不就绪。flag 仅在容量确认(poll_ready)
        // 之后消费——背压(Pending)/错误下不丢 SYN、不重复消费。
        let syn = if self.flag == Flag::Syn {
            self.flag = Flag::None;
            true
        } else {
            false
        };
        // C2 补修:共享待 ACK 随 FIN 携带(CloseStream 的 ack 形态,
        // 即 Frame::close_stream(id, ack) 的 Data+FIN+ACK 既有组合)
        {
            let mut shared = self.shared();
            if shared.ack_pending {
                shared.ack_pending = false;
                ack = true;
            }
        }
        if ack {
            self.shared()
                .update_state(self.conn, self.id, State::Open { acknowledged: true });
        }
        log::trace!("{}/{}: close", self.conn, self.id);
        let cmd = StreamCommand::CloseStream { ack, syn };
        self.sender
            .start_send(cmd)
            .map_err(|_| self.write_zero_err())?;
        self.shared()
            .update_state(self.conn, self.id, State::SendClosed);
        Poll::Ready(Ok(()))
    }
}

#[derive(Debug)]
pub(crate) struct Shared {
    state: State,
    flow_controller: FlowController,
    pub(crate) buffer: Chunks,
    pub(crate) reader: Option<Waker>,
    pub(crate) writer: Option<Waker>,
    /// C2 补修:拆半后仍未发出的 ACK(入站流回执)。共享于两个半:
    /// 读半窗口更新/补发路径与写半数据帧/FIN 路径都会消费它,
    /// ACK 交付不再依赖拆半时 try_send 的一次性成功(失败回落
    /// "仅写半可发"即旧缺陷形态)。完整 Stream 不使用(其 flag
    /// 机制已覆盖全部外发路径)。
    pub(crate) ack_pending: bool,
    /// halves 路径的未确认开流计数令牌(D4 精确性):Some ⟺ 该**出站**
    /// 流已计入全局 ack_backlog_count 且尚未递减。基线 ack_backlog() 是
    /// 全表实时扫描——流离开 Open{ack:false} 的**任何**迁移(对端 ACK
    /// 确认 / 本地 poll_close 半关闭 / 对端 FIN、RST / 删除 / 连接排空)
    /// 即刻不再计入;分片形态没有全局表可扫,由本令牌在 update_state
    /// 的迁移点上恰递减一次(消费即置 None)。未拆分路径与非出站流恒
    /// None,零行为影响。
    pub(crate) ack_backlog_token: Option<Arc<AtomicUsize>>,
}

impl Shared {
    fn new(receive_window: u32, send_window: u32, config: Arc<Config>) -> Self {
        Shared {
            state: State::Open {
                acknowledged: false,
            },
            flow_controller: FlowController::new(receive_window, send_window, config),
            buffer: Chunks::new(),
            reader: None,
            writer: None,
            ack_pending: false,
            ack_backlog_token: None,
        }
    }

    pub(crate) fn state(&self) -> State {
        self.state
    }

    /// Update the stream state and return the state before it was updated.
    pub(crate) fn update_state(
        &mut self,
        cid: connection::Id,
        sid: StreamId,
        next: State,
    ) -> State {
        use self::State::*;

        let current = self.state;

        // D4 令牌消费:离开 Open{ack:false} 的迁移点。基线靠全表实时扫描
        // 自然"不再计入";分片形态在此恰递减一次全局未确认计数——所有
        // 路径(对端 ACK 确认、本地 poll_close 半关闭、对端 FIN/RST、
        // on_drop 删除、连接排空)都经本方法迁移状态,单点收口。
        if matches!(
            current,
            Open {
                acknowledged: false
            }
        ) && !matches!(
            next,
            Open {
                acknowledged: false
            }
        ) {
            if let Some(counter) = self.ack_backlog_token.take() {
                counter.fetch_sub(1, Ordering::SeqCst);
            }
        }

        match (current, next) {
            (Closed, _) => {}
            (Open { .. }, _) => self.state = next,
            (RecvClosed, Closed) => self.state = Closed,
            (RecvClosed, Open { .. }) => {}
            (RecvClosed, RecvClosed) => {}
            (RecvClosed, SendClosed) => self.state = Closed,
            (SendClosed, Closed) => self.state = Closed,
            (SendClosed, Open { .. }) => {}
            (SendClosed, RecvClosed) => self.state = Closed,
            (SendClosed, SendClosed) => {}
        }

        log::trace!(
            "{}/{}: update state: (from {:?} to {:?} -> {:?})",
            cid,
            sid,
            current,
            next,
            self.state
        );

        current // Return the previous stream state for informational purposes.
    }

    pub(crate) fn next_window_update(&mut self) -> Option<u32> {
        self.flow_controller.next_window_update(self.buffer.len())
    }

    /// Whether we are still waiting for the remote to acknowledge this stream.
    pub fn is_pending_ack(&self) -> bool {
        matches!(
            self.state(),
            State::Open {
                acknowledged: false
            }
        )
    }

    pub(crate) fn send_window(&self) -> u32 {
        self.flow_controller.send_window()
    }

    pub(crate) fn consume_send_window(&mut self, i: u32) -> Result<(), ConnectionError> {
        self.flow_controller.consume_send_window(i)
    }

    pub(crate) fn increase_send_window_by(&mut self, i: u32) -> Result<(), ConnectionError> {
        self.flow_controller.increase_send_window_by(i)
    }

    pub(crate) fn consume_receive_window(&mut self, i: u32) -> Result<(), ConnectionError> {
        self.flow_controller.consume_receive_window(i)
    }
}

impl Drop for Shared {
    /// D4 防御网:所有表内路径理论上都经 update_state(Closed) 收尾并
    /// 消费令牌;若仍有令牌滞留到整体析构(未来新增路径漏迁移),在此
    /// 补递减,保证"计入一次 ⟺ 递减一次"的不变量无泄漏。令牌消费即
    /// 置 None,与 update_state 的消费互斥,不会双重递减。
    fn drop(&mut self) {
        if let Some(counter) = self.ack_backlog_token.take() {
            counter.fetch_sub(1, Ordering::SeqCst);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::task::{waker, ArcWake};
    use std::sync::atomic::{AtomicBool, Ordering};

    /// A [`Waker`] that only records whether it has been woken.
    #[derive(Default)]
    struct FlagWaker(AtomicBool);

    impl FlagWaker {
        fn woken(&self) -> bool {
            self.0.load(Ordering::SeqCst)
        }
    }

    impl ArcWake for FlagWaker {
        fn wake_by_ref(arc_self: &Arc<Self>) {
            arc_self.0.store(true, Ordering::SeqCst);
        }
    }

    /// Both halves of a split [`Stream`] send commands to the connection. If they shared a
    /// single [`mpsc::Sender`], the one polled last would overwrite the waker of the other
    /// one, whose wake-up would then be lost forever.
    ///
    /// See https://github.com/libp2p/rust-yamux/issues/232.
    #[test]
    fn read_half_does_not_steal_the_write_half_wakeup() {
        // A channel accepts `buffer + num_senders` commands before the sending handle parks
        // itself, so with a buffer of zero a single frame suffices to fill it up.
        let (sender, mut receiver) = mpsc::channel(0);

        let mut stream = Stream::new_outbound(
            StreamId::new(1),
            connection::Id::next(),
            Arc::new(Config::default()),
            sender,
            None,
        );

        let write_waker = Arc::new(FlagWaker::default());
        let read_waker = Arc::new(FlagWaker::default());
        let (write_waker_ref, read_waker_ref) =
            (waker(write_waker.clone()), waker(read_waker.clone()));
        let mut write_cx = Context::from_waker(&write_waker_ref);
        let mut read_cx = Context::from_waker(&read_waker_ref);

        // The write half queues a frame, filling up the channel.
        assert!(Pin::new(&mut stream)
            .poll_write(&mut write_cx, b"hello")
            .is_ready());

        // It wants to send more, hence it waits for capacity and registers `write_waker`.
        assert!(Pin::new(&mut stream)
            .poll_write(&mut write_cx, b"world")
            .is_pending());

        // In the meantime the read half is polled from another task. It sends window updates
        // over the very same channel and thus must not clobber `write_waker`.
        assert!(Pin::new(&mut stream)
            .poll_read(&mut read_cx, &mut [0u8; 16])
            .is_pending());

        // The connection consumes one command, making room for the write half again.
        receiver.try_recv().unwrap();

        assert!(
            write_waker.woken(),
            "the write half must be woken once the channel has capacity again"
        );
    }

    /// C2(红→绿):入站流在拆半前若仍持 ACK flag,`into_halves` 必须立即
    /// 经既有命令通道排队一个 **0-credit 窗口更新 + ACK** 帧(ACK-on-WU
    /// 是 upstream 既有合法组合),否则读半的窗口更新不携带 ACK、写半
    /// 首写又迟迟不来时,对端 ack_backlog 打满会使 open_stream 挂起。
    /// 同时锁定 R7:该外发帧的 header 经 encode/decode 往返保持合法。
    #[test]
    fn split_before_ack_emits_standard_ack() {
        let (sender, mut receiver) = mpsc::channel(10);
        let stream = Stream::new_inbound(
            StreamId::new(2),
            connection::Id::next(),
            Arc::new(Config::default()),
            DEFAULT_CREDIT,
            sender,
            None,
        );

        let (_read_half, mut write_half) = stream.into_halves();

        // 拆半即应排出 ACK(0-credit WU+ACK)
        let cmd = receiver
            .try_recv()
            .expect("into_halves must flush a pending ACK (C2)");
        match cmd {
            StreamCommand::SendFrame(frame) => {
                let hdr = frame.header();
                assert_eq!(hdr.tag(), crate::frame::header::Tag::WindowUpdate);
                assert_eq!(
                    hdr.len().val(),
                    0,
                    "synthetic window update must carry 0 credit"
                );
                assert!(hdr.flags().contains(crate::frame::header::ACK));
                assert!(!hdr.flags().contains(crate::frame::header::SYN));
                assert!(!hdr.flags().contains(crate::frame::header::FIN));
                assert!(!hdr.flags().contains(crate::frame::header::RST));
                assert_eq!(hdr.stream_id(), StreamId::new(2));

                // R7:header 线格式往返(encode 对任意 header 泛型)
                let enc = crate::frame::header::encode(hdr);
                let dec = crate::frame::header::decode(&enc).expect("WU+ACK must round-trip");
                assert_eq!(dec.tag(), crate::frame::header::Tag::WindowUpdate);
                assert!(dec.flags().contains(crate::frame::header::ACK));
                assert_eq!(dec.len().val(), 0);
            }
            other => panic!("expected SendFrame, got {other:?}"),
        }

        // 写半后续帧不再携带 ACK(flag 已消费)
        let w = futures::task::noop_waker();
        let mut cx = Context::from_waker(&w);
        let mut chunk = Some(bytes::Bytes::from_static(b"payload"));
        let n = match write_half.poll_write_chunk(&mut cx, &mut chunk) {
            Poll::Ready(Ok(n)) => n,
            other => panic!("write should be ready, got {other:?}"),
        };
        assert_eq!(n, 7);
        match receiver.try_recv().expect("data frame queued") {
            StreamCommand::SendFrame(frame) => {
                assert!(!frame.header().flags().contains(crate::frame::header::ACK));
            }
            other => panic!("expected SendFrame, got {other:?}"),
        }
    }

    /// 构造入站流并拆半的公共夹具。
    fn split_inbound() -> (
        StreamReadHalf,
        StreamWriteHalf,
        mpsc::Receiver<StreamCommand>,
    ) {
        let (sender, receiver) = mpsc::channel(10);
        let stream = Stream::new_inbound(
            StreamId::new(2),
            connection::Id::next(),
            Arc::new(Config::default()),
            DEFAULT_CREDIT,
            sender,
            None,
        );
        let (r, w) = stream.into_halves();
        (r, w, receiver)
    }

    /// C2 补修(共享待 ACK 状态缝):拆半时 ACK 因队列满未排出的等价
    /// 终态(ack_pending=true;futures 0.3.34 的 try_send Full 为
    /// per-handle park 语义,拆半时刻该 handle 尚未 park,不可自然
    /// 构造——probe 证据见补修日志)下,**写半全程静默**,仅 poll 读半,
    /// ACK 必须由读半补发(0-credit WU+ACK),不得滞留到写半首写。
    #[test]
    fn ack_via_read_half_when_write_half_silent() {
        let (mut read_half, _write_half, mut receiver) = split_inbound();
        // 模拟拆半 try_send 失败的终态:ACK 在共享状态
        read_half.shared().ack_pending = true;

        let w = futures::task::noop_waker();
        let mut cx = Context::from_waker(&w);
        // 写半不被 poll(静默);读半 poll(缓冲空 → 窗口更新先行)
        match read_half.poll_read_chunk(&mut cx) {
            Poll::Pending => {}
            other => panic!("expected pending read, got {other:?}"),
        }

        match receiver
            .try_recv()
            .expect("read half must flush the pending ACK")
        {
            StreamCommand::SendFrame(frame) => {
                let hdr = frame.header();
                assert_eq!(hdr.tag(), crate::frame::header::Tag::WindowUpdate);
                assert_eq!(
                    hdr.len().val(),
                    0,
                    "no credit to grant — synthetic 0-credit WU"
                );
                assert!(hdr.flags().contains(crate::frame::header::ACK));
            }
            other => panic!("expected SendFrame, got {other:?}"),
        }
        assert!(
            !read_half.shared().is_pending_ack(),
            "ACK must be recorded as sent (state acknowledged)"
        );
        assert!(
            !read_half.shared().ack_pending,
            "pending flag must be cleared"
        );
    }

    /// C2 补修(队列满 + 正确重试):背压(poll_ready Pending,parked)
    /// 期间待发 ACK 保持;容量释放后读半被唤醒重试并发出。
    #[test]
    fn ack_flushes_after_backpressure_clears() {
        // buffer=0:首条消息入队即触发发送 handle park(背压语义)
        let (sender, mut receiver) = mpsc::channel(0);
        let stream = Stream::new_inbound(
            StreamId::new(2),
            connection::Id::next(),
            Arc::new(Config::default()),
            DEFAULT_CREDIT,
            sender,
            None,
        );
        let (mut read_half, _write_half) = stream.into_halves();
        // 拆半 ACK 已排队(干净 handle try_send 成功),且该 handle 因
        // num(1) > buffer(0) 进入 parked——**未消费前**下一次 poll_ready
        // 将 Pending(真实背压窗口)。

        // 新的待 ACK(等价 ACK 需求滞留的场景)
        read_half.shared().ack_pending = true;

        let fw = Arc::new(FlagWaker::default());
        let w = waker(fw.clone());
        let mut cx = Context::from_waker(&w);
        // 背压:poll_ready Pending(parked),ACK 不丢、不误发
        match read_half.poll_read_chunk(&mut cx) {
            Poll::Pending => {}
            other => panic!("expected pending under backpressure, got {other:?}"),
        }
        // 背压期间无任何新帧:ACK 未被消费(消费点与发送在锁内配套,
        // 若发出过帧 ack_pending 必已被清),且队列中仅有拆半时排入的
        // 那一条 ACK(单线程同步,消费后立即再查即证明无第二帧)。
        assert!(
            read_half.shared().ack_pending,
            "ACK must survive backpressure"
        );
        match receiver
            .try_recv()
            .expect("only the split-time ACK may be queued")
        {
            StreamCommand::SendFrame(f) => {
                assert!(f.header().flags().contains(crate::frame::header::ACK));
            }
            other => panic!("expected SendFrame, got {other:?}"),
        }
        assert!(
            receiver.try_recv().is_err(),
            "no additional frame may be sent while the queue is full (parked)"
        );
        assert!(
            fw.woken(),
            "backpressure must register a waker and get woken on capacity"
        );

        // 唤醒后重试:ACK 发出
        match read_half.poll_read_chunk(&mut cx) {
            Poll::Pending => {}
            other => panic!("expected pending read, got {other:?}"),
        }
        match receiver
            .try_recv()
            .expect("ACK must flush after backpressure clears")
        {
            StreamCommand::SendFrame(f) => {
                assert_eq!(f.header().len().val(), 0);
                assert!(f.header().flags().contains(crate::frame::header::ACK));
            }
            other => panic!("expected SendFrame, got {other:?}"),
        }
    }

    /// C2 补修:写半数据帧携带共享待 ACK(消费途径之二)。
    #[test]
    fn ack_via_write_half_data_frame() {
        let (read_half, mut write_half, mut receiver) = split_inbound();
        // 取走拆半时排出的 ACK(成功路径)
        assert!(matches!(
            receiver.try_recv(),
            Ok(StreamCommand::SendFrame(_))
        ));
        read_half.shared().ack_pending = true;

        let w = futures::task::noop_waker();
        let mut cx = Context::from_waker(&w);
        let mut chunk = Some(bytes::Bytes::from_static(b"payload"));
        match write_half.poll_write_chunk(&mut cx, &mut chunk) {
            Poll::Ready(Ok(7)) => {}
            other => panic!("write must succeed, got {other:?}"),
        }
        match receiver.try_recv().expect("data frame") {
            StreamCommand::SendFrame(f) => {
                assert_eq!(f.header().tag(), crate::frame::header::Tag::Data);
                assert!(f.header().flags().contains(crate::frame::header::ACK));
            }
            other => panic!("expected SendFrame, got {other:?}"),
        }
        assert!(!write_half.shared().ack_pending);
    }

    /// C2 补修:写半 FIN 携带共享待 ACK(CloseStream{ack:true},
    /// 即 Frame::close_stream 的 Data+FIN+ACK 既有合法组合)。
    #[test]
    fn ack_via_write_half_close() {
        let (read_half, mut write_half, mut receiver) = split_inbound();
        assert!(matches!(
            receiver.try_recv(),
            Ok(StreamCommand::SendFrame(_))
        ));
        read_half.shared().ack_pending = true;

        let w = futures::task::noop_waker();
        let mut cx = Context::from_waker(&w);
        match write_half.poll_close_half(&mut cx) {
            Poll::Ready(Ok(())) => {}
            other => panic!("close must succeed, got {other:?}"),
        }
        match receiver.try_recv().expect("close command") {
            StreamCommand::CloseStream { ack, .. } => {
                assert!(ack, "FIN must carry the pending ACK");
            }
            other => panic!("expected CloseStream, got {other:?}"),
        }
        assert!(!write_half.shared().ack_pending);
    }

    // ===== round-9 §2:WU 帧极速快车道 =====

    /// 构造带连接级共享状态的入站流(halves 路径的生产等价形态:
    /// ShardWorker::make_new_inbound_stream 传 Some(shared))与独立命令通道。
    /// 6MiB 初窗 = 生产 mux_config 形态:建流授信(ACK|WU)经 try_send 自然
    /// 消费 Ack flag,后续运行期 WU 即为纯 WU。
    fn inbound_with_shared(
        config: Arc<Config>,
    ) -> (Stream, Arc<ConnShared>, mpsc::Receiver<StreamCommand>) {
        let (sender, receiver) = mpsc::channel(10);
        let conn_shared = Arc::new(ConnShared::new_for_test(
            connection::Id::next(),
            crate::Mode::Server,
            Config::default(),
            1,
        ));
        let stream = Stream::new_inbound(
            StreamId::new(2),
            connection::Id::next(),
            config,
            DEFAULT_CREDIT,
            sender,
            Some(conn_shared.clone()),
        );
        (stream, conn_shared, receiver)
    }

    /// 模拟接收侧入账(连接侧 on_data 的记账形态):仅消耗接收窗(缓冲
    /// 只会缩减授出额度,不影响本组断言),使下一次读 poll 越过半窗阈值。
    fn consume_received(stream: &mut Stream, n: u32) {
        stream.shared().consume_receive_window(n).expect("window");
    }

    /// 同 [`consume_received`],拆半读侧形态。
    fn consume_received_half(r: &StreamReadHalf, n: u32) {
        r.shared().consume_receive_window(n).expect("window");
    }

    /// 取出建流授信帧(6MiB 配置下 queue_creation_grant 必然排队)。
    fn take_creation_grant(receiver: &mut mpsc::Receiver<StreamCommand>) {
        match receiver.try_recv().expect("creation grant queued via mpsc") {
            StreamCommand::SendFrame(f) => {
                assert_eq!(f.header().tag(), crate::frame::header::Tag::WindowUpdate);
                assert!(f.header().flags().contains(ACK));
                assert!(!f.header().flags().contains(crate::frame::header::SYN));
                assert_eq!(f.header().len().val(), 6 * 1024 * 1024 - DEFAULT_CREDIT);
            }
            other => panic!("expected SendFrame, got {other:?}"),
        }
    }

    fn six_mib_config() -> Arc<Config> {
        let mut cfg = Config::default();
        cfg.set_initial_stream_receive_window(6 * 1024 * 1024);
        Arc::new(cfg)
    }

    /// T-W1:无 flag 的纯 WU 帧走 to_send 快车道——读满半窗后 WU 帧在
    /// to_send 中、线格式无任何 flag,且不经过流级 mpsc(通道保持空)。
    /// 修前(纯 mpsc 路径)红:WU 落在通道里,to_send 恒空。
    #[test]
    fn wu_fastlane_pure_wu_goes_to_to_send() {
        let (mut stream, conn_shared, mut receiver) = inbound_with_shared(six_mib_config());
        take_creation_grant(&mut receiver); // Ack flag 已随授信消费
        consume_received(&mut stream, 4 * 1024 * 1024);

        let w = futures::task::noop_waker();
        let mut cx = Context::from_waker(&w);
        match Pin::new(&mut stream).poll_read_chunk(&mut cx) {
            Poll::Pending => {} // 缓冲为空:仅触发窗口更新后挂起
            other => panic!("expected pending read, got {other:?}"),
        }

        assert_eq!(
            conn_shared.to_send_len_for_test(),
            1,
            "纯 WU 帧必须进入 to_send 快车道"
        );
        assert!(
            receiver.try_recv().is_err(),
            "纯 WU 帧不得经过流级 mpsc 通道"
        );
        let frame = conn_shared.pop_to_send_for_test().expect("WU frame queued");
        let hdr = frame.header();
        assert_eq!(hdr.tag(), crate::frame::header::Tag::WindowUpdate);
        assert_eq!(hdr.stream_id(), StreamId::new(2));
        assert_eq!(hdr.len().val(), 4 * 1024 * 1024, "回补额度 == 已耗额度");
        assert_eq!(hdr.flags().val(), 0, "快车道仅限无 flag 的纯 WU 帧");
    }

    /// T-W2:带 flag 的 WU 帧(建流授信 ACK|WU)仍走 mpsc 路径,严禁
    /// 进入 to_send;读半的 ack_pending 补发(0-credit WU+ACK)同理。
    /// flag 消费完毕后的运行期纯 WU 才切回快车道。
    #[test]
    fn wu_fastlane_flagged_grant_stays_on_mpsc() {
        let (stream, conn_shared, mut receiver) = inbound_with_shared(six_mib_config());

        // 建流授信:6MiB − 256KiB 差额,带 ACK,必须经 try_send 入 mpsc
        assert_eq!(
            conn_shared.to_send_len_for_test(),
            0,
            "建流授信帧严禁走 to_send 快车道"
        );
        take_creation_grant(&mut receiver);

        // 拆半 + ack_pending(ACK 未发出的等价终态):补发帧携带 ACK,
        // 即使读半持 conn_shared 也必须走 mpsc,不得入 to_send
        let (mut read_half, _write_half) = stream.into_halves();
        read_half.shared().ack_pending = true;
        let w = futures::task::noop_waker();
        let mut cx = Context::from_waker(&w);
        match read_half.poll_read_chunk(&mut cx) {
            Poll::Pending => {}
            other => panic!("expected pending read, got {other:?}"),
        }
        assert_eq!(
            conn_shared.to_send_len_for_test(),
            0,
            "携带 ACK 的补发 WU 不得走快车道"
        );
        match receiver.try_recv().expect("ACK resend via mpsc") {
            StreamCommand::SendFrame(f) => {
                assert!(f.header().flags().contains(ACK));
                assert_eq!(f.header().len().val(), 0, "无 credit 可授,0-credit WU");
            }
            other => panic!("expected SendFrame, got {other:?}"),
        }

        // ack_pending 清零后:运行期纯 WU 恢复快车道(6MiB 窗,授信已入账,
        // 再耗 4MiB 越过半窗阈值)
        consume_received_half(&read_half, 4 * 1024 * 1024);
        match read_half.poll_read_chunk(&mut cx) {
            Poll::Pending => {}
            other => panic!("expected pending read, got {other:?}"),
        }
        assert_eq!(conn_shared.to_send_len_for_test(), 1, "纯 WU 走快车道");
        assert!(receiver.try_recv().is_err(), "通道无新帧");
    }

    /// T-W3:16 流并发读满窗口,交叉 poll——快车道 WU 帧全数到达 to_send,
    /// 无一丢失、无一错流(逐帧校验 stream_id 与无 flag)。
    #[test]
    fn wu_fastlane_sixteen_streams_no_loss() {
        const N: u32 = 16;
        let conn_shared = Arc::new(ConnShared::new_for_test(
            connection::Id::next(),
            crate::Mode::Server,
            Config::default(),
            1,
        ));
        let cfg = six_mib_config();
        let mut streams = Vec::new();
        let mut receivers = Vec::new();
        for i in 0..N {
            let (sender, mut receiver) = mpsc::channel(10);
            let sid = StreamId::new(1 + 2 * i);
            let mut stream = Stream::new_inbound(
                sid,
                connection::Id::next(),
                cfg.clone(),
                DEFAULT_CREDIT,
                sender,
                Some(conn_shared.clone()),
            );
            take_creation_grant(&mut receiver); // Ack 随授信消费,聚焦纯 WU
            consume_received(&mut stream, 4 * 1024 * 1024);
            streams.push(stream);
            receivers.push(receiver);
        }
        let w = futures::task::noop_waker();
        let mut cx = Context::from_waker(&w);
        // 交叉 poll:逐流触发各自的窗口更新(快车道直压 to_send)
        for stream in streams.iter_mut() {
            match Pin::new(stream).poll_read_chunk(&mut cx) {
                Poll::Pending => {}
                other => panic!("expected pending read, got {other:?}"),
            }
        }
        assert_eq!(
            conn_shared.to_send_len_for_test(),
            N as usize,
            "16 流的 WU 帧必须全数到达 to_send"
        );
        let mut ids = std::collections::HashSet::new();
        for _ in 0..N {
            let frame = conn_shared
                .pop_to_send_for_test()
                .expect("WU frame present");
            let hdr = frame.header();
            assert_eq!(hdr.tag(), crate::frame::header::Tag::WindowUpdate);
            assert_eq!(hdr.flags().val(), 0, "纯 WU:不得携带任何 flag");
            assert_eq!(hdr.len().val(), 4 * 1024 * 1024);
            assert!(
                ids.insert(hdr.stream_id().val()),
                "不得重复/丢失任何流的 WU"
            );
        }
        assert_eq!(ids.len(), N as usize);
        for r in receivers.iter_mut() {
            assert!(r.try_recv().is_err(), "任何流都不得经 mpsc 发 WU");
        }
    }
}
