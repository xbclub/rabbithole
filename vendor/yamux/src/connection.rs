// Copyright (c) 2018-2019 Parity Technologies (UK) Ltd.
//
// Licensed under the Apache License, Version 2.0 or MIT license, at your option.
//
// A copy of the Apache License, Version 2.0 is included in the software as
// LICENSE-APACHE and a copy of the MIT license is included in the software
// as LICENSE-MIT. You may also obtain a copy of the Apache License, Version 2.0
// at https://www.apache.org/licenses/LICENSE-2.0 and a copy of the MIT license
// at https://opensource.org/licenses/MIT.

//! This module contains the `Connection` type and associated helpers.
//! A `Connection` wraps an underlying (async) I/O resource and multiplexes
//! `Stream`s over it.

mod cleanup;
mod closing;
mod rtt;
mod stream;

use std::sync::atomic::{AtomicU32, Ordering};

use crate::tagged_stream::TaggedStream;
use crate::{
    error::ConnectionError,
    frame::header::{self, Data, GoAway, Header, Ping, StreamId, Tag, WindowUpdate, CONNECTION_ID},
    frame::{self, Frame},
    Config, DEFAULT_CREDIT,
};
use crate::{Result, MAX_ACK_BACKLOG};
use cleanup::Cleanup;
use closing::Closing;
use futures::stream::SelectAll;
use futures::{channel::mpsc, future::Either, prelude::*, sink::SinkExt, stream::Fuse};
use nohash_hasher::IntMap;
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::task::{Context, Waker};
use std::{fmt, sync::Arc, task::Poll};

// SplitSocket 需公开:round-E2 起 into_halves 的返回签名直指
// RecvHalf<SplitSocket<T>>(锁路径的 halves socket 类型),类型名必须
// 对外可见;字段/构造器(pub(crate) split_socket)仍不外露,行为不变。
// ShardWorker(round-5 分片)同理:halves 三元组的第三件,由调用方
// (rh-transport)spawn;SendWorker(round-6 发送侧分片)为第四件。
pub use halves::{RecvHalf, SendHalf, SendWorker, ShardWorker, SplitSocket};
pub use stream::{Packet, State, Stream, StreamReadHalf, StreamWriteHalf};

/// Next connection identifier, used for debug logging.
static NEXT_ID: AtomicU32 = AtomicU32::new(1);

/// 两次入站读探测之间允许的最大载体写操作数。
///
/// 写优先有界化:`Active::poll` 的写分支 `continue` 会在写洪峰期间饿死
/// 已就绪的入站帧(WU/ACK/SYN/DATA),推高对端写阻塞与建流时延。累计
/// 载体写操作数达到本值即强制做一次入站读探测,给 WU/ACK 的处理时延
/// 一个上界;探测成本被两次探测之间 ≥ 本值的真实载体写摊薄。
const FAIRNESS_MAX_WRITE_OPS_BETWEEN_READ_PROBES: usize = 16;

/// 两次入站读探测之间允许的最大载体写字节数(帧体计)。
///
/// 与 [`FAIRNESS_MAX_WRITE_OPS_BETWEEN_READ_PROBES`] 同理,按字节体量对
/// 写优先做有界化:大帧写洪峰下(操作数增长慢、字节增长快)同样保证
/// 入站读探测的有界时延。计数只记帧体长度,帧头固定开销不计。
const FAIRNESS_MAX_WRITE_BYTES_BETWEEN_READ_PROBES: usize = 256 * 1024;

/// How the connection is used.
#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq)]
pub enum Mode {
    /// Client to server connection.
    Client,
    /// Server to client connection.
    Server,
}

/// The connection identifier.
///
/// Sequentially generated, this is mainly intended to improve log output.
#[derive(Clone, Copy)]
pub(crate) struct Id(u32);

impl Id {
    /// Create a new connection ID.
    pub(crate) fn next() -> Self {
        Id(NEXT_ID.fetch_add(1, Ordering::Relaxed))
    }
}

impl fmt::Debug for Id {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:08x}", self.0)
    }
}

impl fmt::Display for Id {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:08x}", self.0)
    }
}

/// A Yamux connection object.
///
/// Wraps the underlying I/O resource and makes progress via its
/// [`Connection::poll_next_inbound`] method which must be called repeatedly
/// until `Ok(None)` signals EOF or an error is encountered.
#[derive(Debug)]
pub struct Connection<T> {
    inner: ConnectionState<T>,
}

impl<T: AsyncRead + AsyncWrite + Unpin + crate::Writev2> Connection<T> {
    pub fn new(socket: T, cfg: Config, mode: Mode) -> Self {
        // Round-D:延迟物化——新建连接先持原始载体,首个 poll 才构造
        // `Active`(未拆分路径行为不变:物化即走既有 Active::new)。
        // `into_halves` 由此取回原始载体做收/发拆分(Active 的 fused
        // socket 不可逆,拆分必须在物化前)。
        Self {
            inner: ConnectionState::Init(socket, cfg, mode),
        }
    }

    /// Poll for a new outbound stream.
    ///
    /// This function will fail if the current state does not allow opening new outbound streams.
    pub fn poll_new_outbound(&mut self, cx: &mut Context<'_>) -> Poll<Result<Stream>> {
        loop {
            match std::mem::replace(&mut self.inner, ConnectionState::Poisoned) {
                ConnectionState::Init(socket, cfg, mode) => {
                    self.inner = ConnectionState::Active(Active::new(socket, cfg, mode));
                    continue;
                }
                ConnectionState::Active(mut active) => match active.poll_new_outbound(cx) {
                    Poll::Ready(Ok(stream)) => {
                        self.inner = ConnectionState::Active(active);
                        return Poll::Ready(Ok(stream));
                    }
                    Poll::Pending => {
                        self.inner = ConnectionState::Active(active);
                        return Poll::Pending;
                    }
                    Poll::Ready(Err(e)) => {
                        self.inner = ConnectionState::Cleanup(active.cleanup(e));
                        continue;
                    }
                },
                ConnectionState::Closing(mut inner) => match inner.poll_unpin(cx) {
                    Poll::Ready(Ok(())) => {
                        self.inner = ConnectionState::Closed;
                        return Poll::Ready(Err(ConnectionError::Closed));
                    }
                    Poll::Ready(Err(e)) => {
                        self.inner = ConnectionState::Closed;
                        return Poll::Ready(Err(e));
                    }
                    Poll::Pending => {
                        self.inner = ConnectionState::Closing(inner);
                        return Poll::Pending;
                    }
                },
                ConnectionState::Cleanup(mut inner) => match inner.poll_unpin(cx) {
                    Poll::Ready(e) => {
                        self.inner = ConnectionState::Closed;
                        return Poll::Ready(Err(e));
                    }
                    Poll::Pending => {
                        self.inner = ConnectionState::Cleanup(inner);
                        return Poll::Pending;
                    }
                },
                ConnectionState::Closed => {
                    self.inner = ConnectionState::Closed;
                    return Poll::Ready(Err(ConnectionError::Closed));
                }
                ConnectionState::Poisoned => unreachable!(),
            }
        }
    }

    /// Poll for the next inbound stream.
    ///
    /// If this function returns `None`, the underlying connection is closed.
    pub fn poll_next_inbound(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<Stream>>> {
        loop {
            match std::mem::replace(&mut self.inner, ConnectionState::Poisoned) {
                ConnectionState::Init(socket, cfg, mode) => {
                    self.inner = ConnectionState::Active(Active::new(socket, cfg, mode));
                    continue;
                }
                ConnectionState::Active(mut active) => match active.poll(cx) {
                    Poll::Ready(Ok(stream)) => {
                        self.inner = ConnectionState::Active(active);
                        return Poll::Ready(Some(Ok(stream)));
                    }
                    Poll::Ready(Err(e)) => {
                        self.inner = ConnectionState::Cleanup(active.cleanup(e));
                        continue;
                    }
                    Poll::Pending => {
                        self.inner = ConnectionState::Active(active);
                        return Poll::Pending;
                    }
                },
                ConnectionState::Closing(mut closing) => match closing.poll_unpin(cx) {
                    Poll::Ready(Ok(())) => {
                        self.inner = ConnectionState::Closed;
                        return Poll::Ready(None);
                    }
                    Poll::Ready(Err(e)) => {
                        self.inner = ConnectionState::Closed;
                        return Poll::Ready(Some(Err(e)));
                    }
                    Poll::Pending => {
                        self.inner = ConnectionState::Closing(closing);
                        return Poll::Pending;
                    }
                },
                ConnectionState::Cleanup(mut cleanup) => match cleanup.poll_unpin(cx) {
                    Poll::Ready(ConnectionError::Closed) => {
                        self.inner = ConnectionState::Closed;
                        return Poll::Ready(None);
                    }
                    Poll::Ready(other) => {
                        self.inner = ConnectionState::Closed;
                        return Poll::Ready(Some(Err(other)));
                    }
                    Poll::Pending => {
                        self.inner = ConnectionState::Cleanup(cleanup);
                        return Poll::Pending;
                    }
                },
                ConnectionState::Closed => {
                    self.inner = ConnectionState::Closed;
                    return Poll::Ready(None);
                }
                ConnectionState::Poisoned => unreachable!(),
            }
        }
    }

    /// Close the connection.
    pub fn poll_close(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        loop {
            match std::mem::replace(&mut self.inner, ConnectionState::Poisoned) {
                ConnectionState::Init(socket, cfg, mode) => {
                    self.inner = ConnectionState::Active(Active::new(socket, cfg, mode));
                    continue;
                }
                ConnectionState::Active(active) => {
                    self.inner = ConnectionState::Closing(active.close());
                }
                ConnectionState::Closing(mut inner) => match inner.poll_unpin(cx) {
                    Poll::Ready(Ok(())) => {
                        self.inner = ConnectionState::Closed;
                    }
                    Poll::Ready(Err(e)) => {
                        log::warn!("Failure while closing connection: {e}");
                        self.inner = ConnectionState::Closed;
                        return Poll::Ready(Err(e));
                    }
                    Poll::Pending => {
                        self.inner = ConnectionState::Closing(inner);
                        return Poll::Pending;
                    }
                },
                ConnectionState::Cleanup(mut cleanup) => match cleanup.poll_unpin(cx) {
                    Poll::Ready(reason) => {
                        log::warn!("Failure while closing connection: {reason}");
                        self.inner = ConnectionState::Closed;
                        return Poll::Ready(Ok(()));
                    }
                    Poll::Pending => {
                        self.inner = ConnectionState::Cleanup(cleanup);
                        return Poll::Pending;
                    }
                },
                ConnectionState::Closed => {
                    self.inner = ConnectionState::Closed;
                    return Poll::Ready(Ok(()));
                }
                ConnectionState::Poisoned => {
                    unreachable!()
                }
            }
        }
    }
}

impl<T> Drop for Connection<T> {
    fn drop(&mut self) {
        match &mut self.inner {
            ConnectionState::Init(..) => {}
            ConnectionState::Active(active) => active.drop_all_streams(),
            ConnectionState::Closing(_) => {}
            ConnectionState::Cleanup(_) => {}
            ConnectionState::Closed => {}
            ConnectionState::Poisoned => {}
        }
    }
}

enum ConnectionState<T> {
    /// 新建未激活:载体/配置/模式原样持有,首个 poll 物化为 `Active`
    /// (Round-D 拆分支持,见 [`Connection::into_halves`])。
    Init(T, Config, Mode),
    /// The connection is alive and healthy.
    Active(Active<T>),
    /// Our user requested to shutdown the connection, we are working on it.
    Closing(Closing<T>),
    /// An error occurred and we are cleaning up our resources.
    Cleanup(Cleanup),
    /// The connection is closed.
    Closed,
    /// Something went wrong during our state transitions. Should never happen unless there is a bug.
    Poisoned,
}

impl<T> fmt::Debug for ConnectionState<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConnectionState::Init(..) => write!(f, "Init"),
            ConnectionState::Active(_) => write!(f, "Active"),
            ConnectionState::Closing(_) => write!(f, "Closing"),
            ConnectionState::Cleanup(_) => write!(f, "Cleanup"),
            ConnectionState::Closed => write!(f, "Closed"),
            ConnectionState::Poisoned => write!(f, "Poisoned"),
        }
    }
}

/// The active state of [`Connection`].
struct Active<T> {
    id: Id,
    mode: Mode,
    config: Arc<Config>,
    socket: Fuse<frame::Io<T>>,
    next_id: u32,

    streams: IntMap<StreamId, Arc<Mutex<stream::Shared>>>,
    stream_receivers: SelectAll<TaggedStream<StreamId, mpsc::Receiver<StreamCommand>>>,
    no_streams_waker: Option<Waker>,

    pending_read_frame: Option<Frame<()>>,
    pending_write_frame: Option<Frame<()>>,
    new_outbound_stream_waker: Option<Waker>,

    rtt: rtt::Rtt,

    /// 公平调度:自上次入站读探测以来累计的载体写操作数(连接生命周期
    /// 内累计,不跨重启)。达到 [`FAIRNESS_MAX_WRITE_OPS_BETWEEN_READ_PROBES`]
    /// 即在写路径循环顶部强制做一次入站读探测。
    writes_since_read_probe: usize,
    /// 公平调度:自上次入站读探测以来累计的载体写帧体字节数,达到
    /// [`FAIRNESS_MAX_WRITE_BYTES_BETWEEN_READ_PROBES`] 即强制读探测。
    bytes_since_read_probe: usize,
}
/// `Stream` to `Connection` commands.
#[derive(Debug)]
pub(crate) enum StreamCommand {
    /// A new frame should be sent to the remote.
    SendFrame(Frame<Either<Data, WindowUpdate>>),
    /// Close a stream (send FIN; `syn` = 该流尚未发出 SYN,需 SYN|FIN 合帧).
    CloseStream { ack: bool, syn: bool },
}

/// 流命令通道容量(round-7):入站/出站建流点统一引用。
/// 64 ≈ 单流 4MiB(64 × 64KiB 帧)在途命令缓冲,对齐 frp 会话级
/// 缓冲深度;通道打满是本地调度背压,不是流控(流控见 Round-E1 窗口)。
pub(crate) const STREAM_COMMAND_CAP: usize = 64;

/// Possible actions as a result of incoming frame handling.
#[derive(Debug)]
pub(crate) enum Action {
    /// Nothing to be done.
    None,
    /// A new stream has been opened by the remote.
    New(Stream),
    /// A ping should be answered.
    Ping(Frame<Ping>),
    /// The connection should be terminated.
    Terminate(Frame<GoAway>),
}

/// [`Active::service_inbound`] 的一次入站读服务结果。
///
/// 常规读分支与写预算耗尽后的强制读探测共用;`NewStream`/`Closed` 在
/// 两条路径的语义一致(立即交付新流 / 返回 `ConnectionError::Closed`)。
enum InboundOutcome {
    /// 读侧暂无数据(`poll_next` 已登记读 waker)。
    Pending,
    /// 载体 EOF。
    Closed,
    /// 已处理一帧(Ping/Terminate 已入 `pending_read_frame`,保持其发送优先级)。
    Progress,
    /// 远端开出新流。
    NewStream(Stream),
}

impl<T> fmt::Debug for Active<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Connection")
            .field("id", &self.id)
            .field("mode", &self.mode)
            .field("streams", &self.streams.len())
            .field("next_id", &self.next_id)
            .finish()
    }
}

impl<T> fmt::Display for Active<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "(Connection {} {:?} (streams {}))",
            self.id,
            self.mode,
            self.streams.len()
        )
    }
}

impl<T: AsyncRead + AsyncWrite + Unpin + crate::Writev2> Active<T> {
    /// Create a new `Connection` from the given I/O resource.
    fn new(socket: T, cfg: Config, mode: Mode) -> Self {
        let id = Id::next();
        log::debug!("new connection: {id} ({mode:?})");
        let socket = frame::Io::new(id, socket).fuse();
        Active {
            id,
            mode,
            config: Arc::new(cfg),
            socket,
            streams: IntMap::default(),
            stream_receivers: SelectAll::default(),
            no_streams_waker: None,
            next_id: match mode {
                Mode::Client => 1,
                Mode::Server => 2,
            },
            pending_read_frame: None,
            pending_write_frame: None,
            new_outbound_stream_waker: None,
            rtt: rtt::Rtt::new(),
            writes_since_read_probe: 0,
            bytes_since_read_probe: 0,
        }
    }

    /// Gracefully close the connection to the remote.
    fn close(mut self) -> Closing<T> {
        // C3:进入 Closing 后驻留流不再被本连接服务,挂起在
        // reader/writer waker 上的任务必须被唤醒(仅唤醒,不推进
        // `State::Closed`——那会使读半在缓冲有数据时判 EOF,
        // 破坏 read_after_close 契约;已缓冲数据必须仍可读)。
        // Closing 完成/丢弃时会二次唤醒并先断开命令通道,保证被唤醒
        // 的任务重新 poll 时错误立即可见、不会重挂 waker 死等。
        let streams = std::mem::take(&mut self.streams);
        for s in streams.values() {
            let mut shared = s.lock();
            if let Some(w) = shared.reader.take() {
                w.wake()
            }
            if let Some(w) = shared.writer.take() {
                w.wake()
            }
        }
        let pending_frames = self
            .pending_read_frame
            .into_iter()
            .chain(self.pending_write_frame)
            .collect::<VecDeque<Frame<()>>>();
        Closing::new(self.stream_receivers, pending_frames, self.socket, streams)
    }

    /// Cleanup all our resources.
    ///
    /// This should be called in the context of an unrecoverable error on the connection.
    fn cleanup(mut self, error: ConnectionError) -> Cleanup {
        self.drop_all_streams();

        Cleanup::new(self.stream_receivers, error)
    }

    fn poll(&mut self, cx: &mut Context<'_>) -> Poll<Result<Stream>> {
        loop {
            // Round-A 公平调度(强制读探测):写优先的 A/B 分支持续 `continue`
            // 会饿死已就绪的入站帧(WU/ACK/SYN/DATA),写洪峰下读只在载体
            // sink Pending(背压)时才被服务。载体写累计(操作数或帧体字节)
            // 达到预算即先做一次入站服务,再回到写路径。探测无论 Ready/Pending
            // 都复位计数,保证任意两次入站读服务之间的载体写 ≤ 预算;预算从
            // 0 起步,空闲连接(无写入)不探测;`pending_read_frame` 在途
            // (Pong/Terminate)时跳过探测,保持其发送优先级不变。
            // Round-C:探测为批量排空至 Pending/交付/批量上界——同一探测内
            // 连续服务入站,对齐「收发对等排空」语义(写侧两次探测间 ≤ 预算
            // 次写,读侧单次探测 ≤ 预算帧读,对称操作预算)。
            if self.pending_read_frame.is_none()
                && (self.writes_since_read_probe >= FAIRNESS_MAX_WRITE_OPS_BETWEEN_READ_PROBES
                    || self.bytes_since_read_probe >= FAIRNESS_MAX_WRITE_BYTES_BETWEEN_READ_PROBES)
            {
                self.writes_since_read_probe = 0;
                self.bytes_since_read_probe = 0;
                // 批量排空:直至读侧 Pending、NewStream/Closed 交付,或本次
                // 探测已消费 FAIRNESS_MAX_WRITE_OPS_BETWEEN_READ_PROBES 个
                // 入站帧(帧体 ≤64KiB,16 帧上界 1MiB,不做字节预算)。
                // 循环内显式复查 `pending_read_frame`:本轮 service_inbound
                // 置入 Ping/Terminate 待发帧时立即终止批量,防止 Pong 未
                // 发出时继续吞读。
                let mut probed_frames = 0usize;
                while self.pending_read_frame.is_none()
                    && probed_frames < FAIRNESS_MAX_WRITE_OPS_BETWEEN_READ_PROBES
                {
                    match self.service_inbound(cx)? {
                        InboundOutcome::NewStream(stream) => return Poll::Ready(Ok(stream)),
                        InboundOutcome::Closed => return Poll::Ready(Err(ConnectionError::Closed)),
                        InboundOutcome::Pending => break,
                        InboundOutcome::Progress => {}
                    }
                    probed_frames += 1;
                }
                continue;
            }

            // B4:`Poll::is_ready` 对 `Ready(Err)` 同样为真,会把写错误当
            // 就绪吞掉,随后 `start_send` 覆盖 Poisoned 半写状态续发新帧。
            // 这里区分三态:Err 以**原始 IO 错误**立即传播并触发 cleanup。
            match self.socket.poll_ready_unpin(cx) {
                Poll::Ready(Ok(())) => {
                    // Note `next_ping` does not register a waker and thus if not called regularly (idle
                    // connection) no ping is sent. This is deliberate as an idle connection does not
                    // need RTT measurements to increase its stream receive window.
                    if let Some(frame) = self.rtt.next_ping() {
                        let body_len = frame.header().len().val() as usize;
                        self.socket.start_send_unpin(frame.into())?;
                        self.writes_since_read_probe += 1;
                        self.bytes_since_read_probe += body_len;
                        continue;
                    }

                    // Privilege pending `Pong` and `GoAway` `Frame`s
                    // over `Frame`s from the receivers.
                    if let Some(frame) = self
                        .pending_read_frame
                        .take()
                        .or_else(|| self.pending_write_frame.take())
                    {
                        let body_len = frame.header().len().val() as usize;
                        self.socket.start_send_unpin(frame)?;
                        self.writes_since_read_probe += 1;
                        self.bytes_since_read_probe += body_len;
                        continue;
                    }
                }
                // 写失败:立即传播(上层转入 Cleanup),不再续帧
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e.into())),
                Poll::Pending => {}
            }

            match self.socket.poll_flush_unpin(cx)? {
                Poll::Ready(()) => {}
                Poll::Pending => {}
            }

            if self.pending_write_frame.is_none() {
                match self.stream_receivers.poll_next_unpin(cx) {
                    Poll::Ready(Some((_, Some(StreamCommand::SendFrame(frame))))) => {
                        log::trace!(
                            "{}/{}: sending: {}",
                            self.id,
                            frame.header().stream_id(),
                            frame.header()
                        );
                        self.pending_write_frame.replace(frame.into());
                        continue;
                    }
                    Poll::Ready(Some((id, Some(StreamCommand::CloseStream { ack, syn })))) => {
                        log::trace!("{}/{}: sending close", self.id, id);
                        self.pending_write_frame
                            .replace(Frame::close_stream(id, ack, syn).into());
                        continue;
                    }
                    Poll::Ready(Some((id, None))) => {
                        if let Some(frame) = self.on_drop_stream(id) {
                            log::trace!("{}/{}: sending: {}", self.id, id, frame.header());
                            self.pending_write_frame.replace(frame);
                        };
                        continue;
                    }
                    Poll::Ready(None) => {
                        self.no_streams_waker = Some(cx.waker().clone());
                    }
                    Poll::Pending => {}
                }
            }

            if self.pending_read_frame.is_none() {
                match self.service_inbound(cx)? {
                    InboundOutcome::NewStream(stream) => return Poll::Ready(Ok(stream)),
                    InboundOutcome::Closed => return Poll::Ready(Err(ConnectionError::Closed)),
                    InboundOutcome::Progress => continue,
                    InboundOutcome::Pending => {}
                }
            }

            // If we make it this far, at least one of the above must have registered a waker.
            return Poll::Pending;
        }
    }

    /// 服务一次入站读:读单帧并经 `on_frame` 分派。
    ///
    /// [`Active::poll`] 的常规读分支与写预算耗尽后的强制读探测共用本方法,
    /// 行为与去重前的读分支逐分支等价(含 `NewStream`/EOF 的返回语义与
    /// Ping/Terminate 入 `pending_read_frame` 的优先级)。
    fn service_inbound(&mut self, cx: &mut Context<'_>) -> Result<InboundOutcome> {
        match self.socket.poll_next_unpin(cx) {
            Poll::Ready(Some(frame)) => match self.on_frame(frame?)? {
                Action::None => Ok(InboundOutcome::Progress),
                Action::New(stream) => {
                    log::trace!("{}: new inbound {} of {}", self.id, stream, self);
                    Ok(InboundOutcome::NewStream(stream))
                }
                Action::Ping(f) => {
                    log::trace!("{}/{}: pong", self.id, f.header().stream_id());
                    self.pending_read_frame.replace(f.into());
                    Ok(InboundOutcome::Progress)
                }
                Action::Terminate(f) => {
                    log::trace!("{}: sending term", self.id);
                    self.pending_read_frame.replace(f.into());
                    Ok(InboundOutcome::Progress)
                }
            },
            Poll::Ready(None) => Ok(InboundOutcome::Closed),
            Poll::Pending => Ok(InboundOutcome::Pending),
        }
    }

    fn poll_new_outbound(&mut self, cx: &mut Context<'_>) -> Poll<Result<Stream>> {
        if self.streams.len() >= self.config.max_num_streams {
            log::error!("{}: maximum number of streams reached", self.id);
            return Poll::Ready(Err(ConnectionError::TooManyStreams));
        }

        if self.ack_backlog() >= MAX_ACK_BACKLOG {
            log::debug!("{MAX_ACK_BACKLOG} streams waiting for ACK, registering task for wake-up until remote acknowledges at least one stream");
            self.new_outbound_stream_waker = Some(cx.waker().clone());
            return Poll::Pending;
        }

        log::trace!("{}: creating new outbound stream", self.id);

        let id = self.next_stream_id()?;
        let stream = self.make_new_outbound_stream(id);

        log::debug!("{}: new outbound {} of {}", self.id, stream, self);
        self.streams.insert(id, stream.clone_shared());

        Poll::Ready(Ok(stream))
    }

    fn on_drop_stream(&mut self, stream_id: StreamId) -> Option<Frame<()>> {
        let s = self.streams.remove(&stream_id).expect("stream not found");

        log::trace!("{}: removing dropped stream {}", self.id, stream_id);
        let frame = {
            let mut shared = s.lock();
            let frame = match shared.update_state(self.id, stream_id, State::Closed) {
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
                // The remote may be out of credit though and blocked on
                // writing more data. We may need to reset the stream.
                State::SendClosed => {
                    // The remote has either still credit or will be given more
                    // due to an enqueued window update or we already have
                    // inbound frames in the socket buffer which will be
                    // processed later. In any case we will reply with an RST in
                    // `Connection::on_data` because the stream will no longer
                    // be known.
                    None
                }
                // The stream was properly closed. We already have sent our FIN frame. The
                // remote end has already done so in the past.
                State::Closed => None,
            };
            if let Some(w) = shared.reader.take() {
                w.wake()
            }
            if let Some(w) = shared.writer.take() {
                w.wake()
            }
            frame
        };
        frame.map(Into::into)
    }

    /// Process the result of reading from the socket.
    ///
    /// Unless `frame` is `Ok(Some(_))` we will assume the connection got closed
    /// and return a corresponding error, which terminates the connection.
    /// Otherwise we process the frame and potentially return a new `Stream`
    /// if one was opened by the remote.
    fn on_frame(&mut self, frame: Frame<()>) -> Result<Action> {
        log::trace!("{}: received: {}", self.id, frame.header());

        if frame.header().flags().contains(header::ACK)
            && matches!(frame.header().tag(), Tag::Data | Tag::WindowUpdate)
        {
            let id = frame.header().stream_id();
            if let Some(stream) = self.streams.get(&id) {
                stream
                    .lock()
                    .update_state(self.id, id, State::Open { acknowledged: true });
            }
            if let Some(waker) = self.new_outbound_stream_waker.take() {
                waker.wake();
            }
        }

        let action = match frame.header().tag() {
            Tag::Data => self.on_data(frame.into_data()),
            Tag::WindowUpdate => self.on_window_update(&frame.into_window_update()),
            Tag::Ping => self.on_ping(&frame.into_ping()),
            Tag::GoAway => return Err(ConnectionError::Closed),
        };
        Ok(action)
    }

    fn on_data(&mut self, frame: Frame<Data>) -> Action {
        let stream_id = frame.header().stream_id();

        if frame.header().flags().contains(header::RST) {
            // stream reset
            if let Some(s) = self.streams.get_mut(&stream_id) {
                let mut shared = s.lock();
                shared.update_state(self.id, stream_id, State::Closed);
                if let Some(w) = shared.reader.take() {
                    w.wake()
                }
                if let Some(w) = shared.writer.take() {
                    w.wake()
                }
            }
            return Action::None;
        }

        let is_finish = frame.header().flags().contains(header::FIN); // half-close

        if frame.header().flags().contains(header::SYN) {
            // new stream
            if !self.is_valid_remote_id(stream_id, Tag::Data) {
                log::error!("{}: invalid stream id {}", self.id, stream_id);
                return Action::Terminate(Frame::protocol_error());
            }
            if self.streams.contains_key(&stream_id) {
                log::error!("{}/{}: stream already exists", self.id, stream_id);
                return Action::Terminate(Frame::protocol_error());
            }
            if self.streams.len() == self.config.max_num_streams {
                log::error!("{}: maximum number of streams reached", self.id);
                return Action::Terminate(Frame::internal_error());
            }
            if frame.body().len() > DEFAULT_CREDIT as usize {
                log::error!(
                    "{}/{}: 1st body of stream exceeds default credit",
                    self.id,
                    stream_id
                );
                return Action::Terminate(Frame::protocol_error());
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

                    return Action::Terminate(Frame::protocol_error());
                }
                shared.buffer.push(frame.into_body());
            }
            self.streams.insert(stream_id, stream.clone_shared());
            return Action::New(stream);
        }

        if let Some(s) = self.streams.get_mut(&stream_id) {
            let mut shared = s.lock();

            if let Err(_err) = shared.consume_receive_window(frame.body_len()) {
                log::error!(
                    "{}/{}: frame body larger than window of stream",
                    self.id,
                    stream_id
                );

                return Action::Terminate(Frame::protocol_error());
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
            // We do not consider this a protocol violation and thus do not send a stream reset
            // because we may still be processing pending `StreamCommand`s of this stream that were
            // sent before it has been dropped and "garbage collected". Such a stream reset would
            // interfere with the frames that still need to be sent, causing premature stream
            // termination for the remote.
            //
            // See https://github.com/paritytech/yamux/issues/110 for details.
        }

        Action::None
    }

    fn on_window_update(&mut self, frame: &Frame<WindowUpdate>) -> Action {
        let stream_id = frame.header().stream_id();

        if frame.header().flags().contains(header::RST) {
            // stream reset
            if let Some(s) = self.streams.get_mut(&stream_id) {
                let mut shared = s.lock();
                shared.update_state(self.id, stream_id, State::Closed);
                if let Some(w) = shared.reader.take() {
                    w.wake()
                }
                if let Some(w) = shared.writer.take() {
                    w.wake()
                }
            }
            return Action::None;
        }

        let is_finish = frame.header().flags().contains(header::FIN); // half-close

        if frame.header().flags().contains(header::SYN) {
            // new stream
            if !self.is_valid_remote_id(stream_id, Tag::WindowUpdate) {
                log::error!("{}: invalid stream id {}", self.id, stream_id);
                return Action::Terminate(Frame::protocol_error());
            }
            if self.streams.contains_key(&stream_id) {
                log::error!("{}/{}: stream already exists", self.id, stream_id);
                return Action::Terminate(Frame::protocol_error());
            }
            if self.streams.len() == self.config.max_num_streams {
                log::error!("{}: maximum number of streams reached", self.id);
                return Action::Terminate(Frame::protocol_error());
            }

            let Some(credit) = frame.header().credit().checked_add(DEFAULT_CREDIT) else {
                log::error!("{}: header contains invalid credit", self.id);
                return Action::Terminate(Frame::protocol_error());
            };
            let stream = self.make_new_inbound_stream(stream_id, credit);

            if is_finish {
                stream
                    .shared()
                    .update_state(self.id, stream_id, State::RecvClosed);
            }
            self.streams.insert(stream_id, stream.clone_shared());
            return Action::New(stream);
        }

        if let Some(s) = self.streams.get_mut(&stream_id) {
            let mut shared = s.lock();
            if let Err(err) = shared.increase_send_window_by(frame.header().credit()) {
                log::error!(
                    "{}/{}: could not increase the send window, {err}",
                    self.id,
                    stream_id
                );
                return Action::Terminate(Frame::protocol_error());
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
            // We do not consider this a protocol violation and thus do not send a stream reset
            // because we may still be processing pending `StreamCommand`s of this stream that were
            // sent before it has been dropped and "garbage collected". Such a stream reset would
            // interfere with the frames that still need to be sent, causing premature stream
            // termination for the remote.
            //
            // See https://github.com/paritytech/yamux/issues/110 for details.
        }

        Action::None
    }

    fn on_ping(&mut self, frame: &Frame<Ping>) -> Action {
        let stream_id = frame.header().stream_id();
        if frame.header().flags().contains(header::ACK) {
            return self.rtt.handle_pong(frame.id());
        }
        if stream_id == CONNECTION_ID || self.streams.contains_key(&stream_id) {
            let mut hdr = Header::ping(frame.header().id());
            hdr.ack();
            return Action::Ping(Frame::new(hdr));
        }
        log::debug!(
            "{}/{}: ping for unknown stream, possibly dropped earlier: {:?}",
            self.id,
            stream_id,
            frame
        );
        // We do not consider this a protocol violation and thus do not send a stream reset because
        // we may still be processing pending `StreamCommand`s of this stream that were sent before
        // it has been dropped and "garbage collected". Such a stream reset would interfere with the
        // frames that still need to be sent, causing premature stream termination for the remote.
        //
        // See https://github.com/paritytech/yamux/issues/110 for details.

        Action::None
    }

    fn make_new_inbound_stream(&mut self, id: StreamId, credit: u32) -> Stream {
        let config = self.config.clone();

        let (sender, receiver) = mpsc::channel(STREAM_COMMAND_CAP);
        self.stream_receivers.push(TaggedStream::new(id, receiver));
        if let Some(waker) = self.no_streams_waker.take() {
            waker.wake();
        }

        // 未拆分路径无 ConnShared:conn_shared = None,WU 恒走流级 mpsc。
        Stream::new_inbound(id, self.id, config, credit, sender, None)
    }

    fn make_new_outbound_stream(&mut self, id: StreamId) -> Stream {
        let config = self.config.clone();

        let (sender, receiver) = mpsc::channel(STREAM_COMMAND_CAP);
        self.stream_receivers.push(TaggedStream::new(id, receiver));
        if let Some(waker) = self.no_streams_waker.take() {
            waker.wake();
        }

        // 同上:未拆分路径无 ConnShared。
        Stream::new_outbound(id, self.id, config, sender, None)
    }

    fn next_stream_id(&mut self) -> Result<StreamId> {
        let proposed = StreamId::new(self.next_id);
        self.next_id = self
            .next_id
            .checked_add(2)
            .ok_or(ConnectionError::NoMoreStreamIds)?;
        match self.mode {
            Mode::Client => assert!(proposed.is_client()),
            Mode::Server => assert!(proposed.is_server()),
        }
        Ok(proposed)
    }

    /// The ACK backlog is defined as the number of outbound streams that have not yet been acknowledged.
    fn ack_backlog(&mut self) -> usize {
        self.streams
            .iter()
            // Whether this is an outbound stream.
            //
            // Clients use odd IDs and servers use even IDs.
            // A stream is outbound if:
            //
            // - Its ID is odd and we are the client.
            // - Its ID is even and we are the server.
            .filter(|(id, _)| match self.mode {
                Mode::Client => id.is_client(),
                Mode::Server => id.is_server(),
            })
            .filter(|(_, s)| s.lock().is_pending_ack())
            .count()
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
}

impl<T> Active<T> {
    /// Close and drop all `Stream`s and wake any pending `Waker`s.
    fn drop_all_streams(&mut self) {
        for (id, s) in self.streams.drain() {
            let mut shared = s.lock();
            shared.update_state(self.id, id, State::Closed);
            if let Some(w) = shared.reader.take() {
                w.wake()
            }
            if let Some(w) = shared.writer.take() {
                w.wake()
            }
        }
    }
}

/// Connection 拆分收/发双半(Round-D,rabbithole 专用;先例
/// [`Stream::into_halves`])。
///
/// 对齐 Go yamux 的 `go s.recv()` / `go s.send()` 双 goroutine 模型:收
/// (解帧+派发)与发(取命令+编帧+写出)由两个任务独立 poll,物理并行。
/// 未拆分路径([`Active`])不受影响;拆分后的发送侧 poll **不含** Round-A/C
/// 的公平调度读探测与批量排空——读任务物理独立,无需写侧让位。
///
/// 共享状态经 [`halves`::ConnShared]:锁只护数据结构,临界区内无 await、
/// 无载体 IO;载体访问经 [`halves`::SplitSocket] 的自研 BiLock(双句柄
/// 共享 `Mutex<BiLockState>` + waker 槽,等待 waker 存于锁、不随临时
/// future drop 丢失)串行,锁粒度 = 单次载体操作。错误双向传播(要点 1):
/// 任一半的 IO 错误/EOF 进入共享错误槽并唤醒对端 park 点;接收侧压来的
/// Pong/GoAway 帧与 rtt ping 的发送优先级不低于未拆分路径(要点 2)。
mod halves {
    use super::*;
    use crate::frame::{MAX_BATCH_BYTES, MAX_BATCH_FRAMES};
    use crate::{FrameDecodeError, Writev2};
    use futures::ready;
    use futures::{AsyncRead, AsyncWrite};
    use std::io;
    use std::pin::Pin;
    use std::sync::atomic::{AtomicBool, AtomicUsize};

    // round-5 流级分片:通道/Worker/收编三联(§2);子模块可直接触达本
    // 模块私有状态。round-6 起同文件另有发送侧 SendWorker(§2.3)。
    mod shard;
    pub use shard::{SendWorker, ShardWorker};

    /// 自研 BiLock 共享状态(规格 Round-D2 要点 2:futures 的 `BiLock`
    /// 需要未启用的 `bilock`+`unstable` feature,Cargo.toml 不在本次改动
    /// 范围,按规格以「双句柄共享 `Mutex<BiLockState>` + waker 槽」等价
    /// 自研)。`value` 为 `None` 表示锁被某一侧持有;锁等待者的 waker
    /// 存于锁共享状态,**不随任何临时 future 的 drop 丢失**——这正是
    /// `futures::lock::Mutex` 的缺陷:`lock()` 每次调用创建临时
    /// `MutexLockFuture`,Pending 时把等待节点(内含已注册 waker)入队,
    /// 该 future 随语句结束被 drop 时节点被移出队列,持锁对端释放时
    /// 队列已空、无人被唤醒,等待侧永久挂起(Round-D2 根因)。
    struct BiLockState<T> {
        value: Option<T>,
        waker: Option<Waker>,
    }

    /// `SplitSocket` 底层锁句柄:克隆即共享同一把锁(收/发两半恰好
    /// 两个句柄、至多一个等待者,单一 waker 槽足够;等待者必为未持锁
    /// 的那一半)。
    struct SplitBiLock<T> {
        shared: Arc<Mutex<BiLockState<T>>>,
    }

    impl<T> SplitBiLock<T> {
        fn new(value: T) -> Self {
            SplitBiLock {
                shared: Arc::new(Mutex::new(BiLockState {
                    value: Some(value),
                    waker: None,
                })),
            }
        }

        /// 取锁(poll 语义):空闲即取(`value` 移入守卫,临界区开始);
        /// 被对端持有则把当前 waker 存入锁共享状态并返回 Pending——
        /// waker 在锁里,对端释放([`SplitBiLockGuard::drop`])时唤醒,
        /// 最坏情形只是多余虚假唤醒,无丢失唤醒路径(BiLock 语义)。
        fn poll_lock(&self, cx: &mut Context<'_>) -> Poll<SplitBiLockGuard<T>> {
            let mut st = self.shared.lock();
            match st.value.take() {
                Some(value) => Poll::Ready(SplitBiLockGuard {
                    shared: self.shared.clone(),
                    value: Some(value),
                }),
                None => {
                    st.waker = Some(cx.waker().clone());
                    Poll::Pending
                }
            }
        }
    }

    // 手写 Clone:derive 会给 T 加 Clone 约束,这里只克隆 Arc。
    impl<T> Clone for SplitBiLock<T> {
        fn clone(&self) -> Self {
            SplitBiLock {
                shared: self.shared.clone(),
            }
        }
    }

    /// `SplitSocket` 底层锁守卫:Deref 到载体;drop 即释放(value 放回
    /// + 唤醒等待者)。临界区内只做单次载体 IO,不做 IO 之外的等待。
    struct SplitBiLockGuard<T> {
        shared: Arc<Mutex<BiLockState<T>>>,
        value: Option<T>,
    }

    impl<T> std::ops::Deref for SplitBiLockGuard<T> {
        type Target = T;
        fn deref(&self) -> &T {
            self.value.as_ref().expect("bilock guard holds the carrier")
        }
    }

    impl<T> std::ops::DerefMut for SplitBiLockGuard<T> {
        fn deref_mut(&mut self) -> &mut T {
            self.value.as_mut().expect("bilock guard holds the carrier")
        }
    }

    impl<T> Drop for SplitBiLockGuard<T> {
        fn drop(&mut self) {
            let mut st = self.shared.lock();
            debug_assert!(st.value.is_none(), "bilock invariant: one guard at a time");
            st.value = self.value.take();
            if let Some(w) = st.waker.take() {
                w.wake();
            }
        }
    }

    /// 载体拆分插座:单一 IO 经自研 BiLock(锁等待 waker 存于锁共享
    /// 状态)串行,供收/发两个任务分别做读/写。`poll_write2` 在锁内
    /// 委托 `T::poll_write2`,保持「帧头+帧体一次向量写」(要点 6)。
    /// pub:锁路径 halves 的 socket 类型(见 connection.rs 顶部 re-export
    /// 注释);字段与构造器不外露。
    pub struct SplitSocket<T> {
        io: SplitBiLock<T>,
    }

    // 手写 Clone:derive 会给 T 加 Clone 约束,这里只克隆 Arc。
    impl<T> Clone for SplitSocket<T> {
        fn clone(&self) -> Self {
            SplitSocket {
                io: self.io.clone(),
            }
        }
    }
    pub(crate) fn split_socket<T>(io: T) -> SplitSocket<T> {
        SplitSocket {
            io: SplitBiLock::new(io),
        }
    }

    impl<T: AsyncRead + Unpin> AsyncRead for SplitSocket<T> {
        fn poll_read(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut [u8],
        ) -> Poll<io::Result<usize>> {
            let this = self.get_mut();
            let mut guard = ready!(this.io.poll_lock(cx));
            Pin::new(&mut *guard).poll_read(cx, buf)
        }
    }

    impl<T: AsyncWrite + Writev2 + Unpin> AsyncWrite for SplitSocket<T> {
        fn poll_write(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            Writev2::poll_write2(self, cx, &[], buf)
        }
        fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            let this = self.get_mut();
            let mut guard = ready!(this.io.poll_lock(cx));
            Pin::new(&mut *guard).poll_flush(cx)
        }
        fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            let this = self.get_mut();
            let mut guard = ready!(this.io.poll_lock(cx));
            Pin::new(&mut *guard).poll_close(cx)
        }
    }

    impl<T: AsyncWrite + Writev2 + Unpin> Writev2 for SplitSocket<T> {
        fn poll_write2(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            a: &[u8],
            b: &[u8],
        ) -> Poll<io::Result<usize>> {
            let this = self.get_mut();
            let mut guard = ready!(this.io.poll_lock(cx));
            Pin::new(&mut *guard).poll_write2(cx, a, b)
        }

        /// round-5 §3.1:多段批量写与 `poll_write2` 同式——取锁后委托
        /// 内层(锁粒度仍 = 单次载体操作,临界区内只做这一次 writev)。
        fn poll_writev(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            bufs: &[io::IoSlice<'_>],
        ) -> Poll<io::Result<usize>> {
            let this = self.get_mut();
            let mut guard = ready!(this.io.poll_lock(cx));
            Pin::new(&mut *guard).poll_writev(cx, bufs)
        }
    }

    /// 命令接收器注册条目(round-6 D4):流 id + 其命令通道接收端,
    /// 压入 `send_regs[k]` 由 SendWorker k 收编。
    type SendRegistration = (StreamId, mpsc::Receiver<StreamCommand>);

    /// halves 四元组(round-6 D7):接收半(Framer)、发送半(FlushTask)、
    /// N 个接收侧分片 Worker、N 个发送侧分片 Worker——四者由调用方
    /// (rh-transport)分别 spawn。类型缩写仅供签名可读。
    type Halves<R, W> = (RecvHalf<R>, SendHalf<W>, Vec<ShardWorker>, Vec<SendWorker>);

    /// 双半共享的连接状态(round-5 分片改造:全局流表删除,换 opening
    /// 预注册表 + 双全局计数器,§2.4;round-6 起 `new_receivers` 全局注册
    /// 队列删除,命令接收器按 `sid % N` 路由进 `send_regs[k]`,§2.2)。
    ///
    /// 锁纪律(要点 3):所有 parking_lot Mutex 临界区内只操作数据结构,
    /// 无 await、无载体 IO、无跨锁回调;固定加锁顺序
    /// `opening → stream::Shared / send_regs → send_worker_wakers /
    /// send_waker`,任何路径不嵌套持双锁,不构成环路。
    pub(crate) struct ConnShared {
        id: Id,
        mode: Mode,
        config: Arc<Config>,
        /// D3 出站预注册表:SendHalf 开流即插入,Worker 首帧"按需收编"
        /// (消除跨任务注册顺序竞态:对端任何帧只可能在我们 SYN 上线后
        /// 到达);从未收到帧的出站流不占 Worker 表,由 on_drop 直接清。
        opening: Mutex<IntMap<StreamId, Arc<Mutex<stream::Shared>>>>,
        /// D4 存活流数(入站建流 + 出站开流各 +1;fetch_add 回滚保证
        /// 跨分片上界精确)。
        stream_count: AtomicUsize,
        /// D4 未确认出站流数(§2.5-A 开流 fetch_add;递减由流 Shared 持有
        /// 的计数令牌在离开 Open{ack:false} 的任何路径恰一次完成——对端
        /// ACK 确认 / 本地 poll_close 半关闭 / 对端 FIN、RST / 删除 / 连接
        /// 排空,与基线 ack_backlog() 实时表扫描语义等价)。Arc 形态供
        /// 令牌跨 ConnShared 共享同一计数器。
        ack_backlog_count: Arc<AtomicUsize>,
        /// SendHalf Drop 信号(§2.6:不置错误;Worker 排空本地表后
        /// 继续服务,对齐现行"发送半弃置后接收半仍可读/建流")。
        send_dropped: AtomicBool,
        /// Framer 路由终结宣告(复核修复,Worker fatal 排空的终结条件
        /// 之一):RecvHalf 在冲洗完 pending_route(或 Drop 弃置)后
        /// 置位,此后不再有新消息进入任何分片通道。若 Worker 凭错误
        /// 标志即终结,RecvHalf 在对端错误唤醒后冲洗的 pending_route 帧
        /// 会落入已终结 Worker 而丢失(§0.3-3 在途数据)。
        framer_flushed: AtomicBool,
        /// 错误粘性影子:set_error 即置位且永不清除。RecvHalf/SendHalf
        /// take_error 会移除首错,Worker 被唤醒时若首错已被取走,凭本
        /// 标志仍可观测"已致命"并终结(§2.6 Workers 收敛路径的竞态缝)。
        workers_shutdown: AtomicBool,
        /// recv → send 帧队列(Pong/GoAway/Worker 的 RST/FIN):发送
        /// 优先级最高(要点 2)。
        to_send: Mutex<VecDeque<Frame<()>>>,
        /// 每 SendWorker 一条的命令接收器注册队列(round-6 D4:入站建流
        /// 与出站开流都按 `sid % N` 压入 `send_regs[k]`,压入即唤醒
        /// SendWorker k;条目上界 = max_num_streams(每条活流恰注册一次,
        /// 天然有界,不设容量上限不违反红线 6)。drain 无背压(条目小、
        /// 速率 = 建流率)。
        send_regs: Mutex<Vec<VecDeque<SendRegistration>>>,
        /// 错误槽:首个错误生效;存重建副本(原始错误由产生侧直接返回,
        /// 见 [`make_shadow`])。
        error: Mutex<Option<ConnectionError>>,
        /// 发送半 park 点:压帧/置错时唤醒(round-6:注册路由改走
        /// send_worker_wakers,此槽只服务 to_send 帧压入)。
        send_waker: Mutex<Option<Waker>>,
        /// 接收半 park 点:发送侧置错时唤醒(要点 1)。
        recv_waker: Mutex<Option<Waker>>,
        /// ACK backlog 门控的 open_stream 等待者(发侧登记,Worker ACK
        /// 唤醒)。
        new_outbound_stream_waker: Mutex<Option<Waker>>,
        /// N 槽 RecvWorker park 点:set_error/排空信号广播(§2.4)。
        worker_wakers: Mutex<Vec<Option<Waker>>>,
        /// N 槽 SendWorker park 点(round-6 §2.2:注册压入 / set_error /
        /// send_dropped 唤醒;take 语义)。
        send_worker_wakers: Mutex<Vec<Option<Waker>>>,
        /// 每分片"SendWorker k 有滞留 ctrl Remove"咨询标志(round-6.1
        /// F2):FlushTask 取空通知的廉价前置门——为假即一次原子读跳过
        /// has_pending/wakers 两把锁与唤醒(该通知唯一的服务对象是"因
        /// 水位门控滞留 ctrl Remove 的 SendWorker",无滞留即纯开销)。
        /// 置位/清零仅 SendWorker k 自己(同一线程),FlushTask 只读;
        /// flag 不承担正确性,任何时序下至多多一次伪唤醒,不丢唤醒。
        /// 全部读写 SeqCst:与 popped 计数器构成跨线程 SC 对,机器无关
        /// 地封死"门读陈旧 false + Worker 复查读陈旧水位"的双陈旧
        /// 交织(复核修复,详述见 halves/shard.rs popped_count 文档)。
        ctrl_wait: Vec<AtomicBool>,
        /// Framer/SendHalf 路由句柄:每 Worker 一对(帧通道 + ctrl 通道)
        /// 生产者(§2.4 workers 字段)。
        workers: Mutex<Vec<shard::ShardRoute>>,
    }

    impl ConnShared {
        fn new(id: Id, mode: Mode, config: Config, worker_count: usize) -> Self {
            ConnShared {
                id,
                mode,
                config: Arc::new(config),
                opening: Mutex::new(IntMap::default()),
                stream_count: AtomicUsize::new(0),
                ack_backlog_count: Arc::new(AtomicUsize::new(0)),
                send_dropped: AtomicBool::new(false),
                framer_flushed: AtomicBool::new(false),
                workers_shutdown: AtomicBool::new(false),
                to_send: Mutex::new(VecDeque::new()),
                send_regs: Mutex::new((0..worker_count).map(|_| VecDeque::new()).collect()),
                error: Mutex::new(None),
                send_waker: Mutex::new(None),
                recv_waker: Mutex::new(None),
                new_outbound_stream_waker: Mutex::new(None),
                worker_wakers: Mutex::new(vec![None; worker_count]),
                send_worker_wakers: Mutex::new(vec![None; worker_count]),
                ctrl_wait: (0..worker_count).map(|_| AtomicBool::new(false)).collect(),
                workers: Mutex::new(Vec::new()),
            }
        }

        /// 置共享错误(首错生效)并唤醒两个半的 park 点、backlog 等待者
        /// 与全部 RecvWorker/SendWorker(要点 1:另一半个下次 poll 立即
        /// 可见,不得死等;§2.4:worker_wakers 广播 + 粘性影子置位;
        /// round-6 §2.2:send_worker_wakers 同被唤醒,SendWorker 由此
        /// 进入 D8 终结)。
        fn set_error(&self, e: ConnectionError) {
            let mut slot = self.error.lock();
            if slot.is_some() {
                return;
            }
            *slot = Some(e);
            drop(slot);
            self.workers_shutdown.store(true, Ordering::Release);
            if let Some(w) = self.send_waker.lock().take() {
                w.wake()
            }
            if let Some(w) = self.recv_waker.lock().take() {
                w.wake()
            }
            if let Some(w) = self.new_outbound_stream_waker.lock().take() {
                w.wake()
            }
            self.wake_all_workers();
            self.wake_all_send_workers();
        }

        /// 唤醒全部 RecvWorker park 点(take 语义,§2.4 排空信号广播)。
        fn wake_all_workers(&self) {
            for slot in self.worker_wakers.lock().iter_mut() {
                if let Some(w) = slot.take() {
                    w.wake()
                }
            }
        }

        /// 唤醒全部 SendWorker park 点(take 语义;round-6 §2.2:set_error
        /// 扩散 / SendHalf Drop 置 send_dropped 后广播)。
        fn wake_all_send_workers(&self) {
            for slot in self.send_worker_wakers.lock().iter_mut() {
                if let Some(w) = slot.take() {
                    w.wake()
                }
            }
        }

        /// 观察侧取错误(一次性)。错误产生侧自身直接返回原始错误、不经此
        /// 槽;最多一个对端观察者,取走即空(Worker 侧凭 workers_shutdown
        /// 粘性影子收敛,不依赖此槽)。
        fn take_error(&self) -> Option<ConnectionError> {
            self.error.lock().take()
        }

        /// 接收侧压来的待发帧(Pong/GoAway/Worker 的 RST/FIN;round-9 起
        /// 另含流读半快车道压入的纯 WU 帧):压入即唤醒发送半。
        /// pub(crate)(round-9):WU 快车道由 stream.rs(connection 的后代
        /// 模块)调用,仅可见性提升,行为不变。
        pub(crate) fn push_to_send(&self, f: Frame<()>) {
            self.to_send.lock().push_back(f);
            if let Some(w) = self.send_waker.lock().take() {
                w.wake()
            }
        }

        fn pop_to_send(&self) -> Option<Frame<()>> {
            self.to_send.lock().pop_front()
        }

        /// 命令接收器注册路由(round-6 D4:替换旧"压全局 new_receivers +
        /// 唤醒发送半"——flush 侧已不消费注册)。入站建流(Worker 内)与
        /// 出站开流(SendHalf)都按 `sid % N` 压入 `send_regs[k]` 并唤醒
        /// SendWorker k(take 语义);**不再唤醒 send_waker**。
        /// D4 正确性:出站流首命令缓冲在通道 cap `STREAM_COMMAND_CAP` 内,
        /// Worker 收编早晚不影响——命令不丢、SYN 在首帧自带 flag,顺序由
        /// 通道 FIFO 保证。
        fn push_new_receiver(&self, id: StreamId, r: mpsc::Receiver<StreamCommand>) {
            let index = (id.val() as usize) % self.send_regs.lock().len();
            self.send_regs.lock()[index].push_back((id, r));
            if let Some(w) = self.send_worker_wakers.lock()[index].take() {
                w.wake();
            }
        }

        /// Round-D3-2 回归测试专用(不得用于生产):直压 to_send,走生产
        /// `push_to_send` 语义(压入即取 send_waker 槽唤醒)。规格变更 4
        /// 预判的"tests 模块内直接调 push_to_send"路线的可见性缝。
        #[cfg(test)]
        pub(crate) fn push_to_send_for_test(&self, f: Frame<()>) {
            self.push_to_send(f)
        }

        /// round-9 WU 快车道测试缝(不得用于生产):独立构造一份连接级
        /// 共享状态——快车道只触达 to_send/send_waker,无 Worker 参与,
        /// 单 worker_count 即可支撑断言。
        #[cfg(test)]
        pub(crate) fn new_for_test(
            id: Id,
            mode: Mode,
            config: Config,
            worker_count: usize,
        ) -> Self {
            Self::new(id, mode, config, worker_count)
        }

        /// round-9 WU 快车道测试缝:观察 to_send 深度(帧是否入队)。
        #[cfg(test)]
        pub(crate) fn to_send_len_for_test(&self) -> usize {
            self.to_send.lock().len()
        }

        /// round-9 WU 快车道测试缝:弹出 to_send 队首帧(校验线格式)。
        #[cfg(test)]
        pub(crate) fn pop_to_send_for_test(&self) -> Option<Frame<()>> {
            self.pop_to_send()
        }

        /// 登记发送半 park 点 waker。唤醒方(唤醒语义,勿丢):
        /// [`ConnShared::push_to_send`](接收侧压来 Pong/GoAway 帧即唤醒)、
        /// [`ConnShared::set_error`](任一半 IO 错误即唤醒)。
        fn set_send_waker(&self, w: &Waker) {
            *self.send_waker.lock() = Some(w.clone());
        }

        fn set_recv_waker(&self, w: &Waker) {
            *self.recv_waker.lock() = Some(w.clone());
        }

        /// 排空 `opening` 残留(逐流 Closed + 唤醒 + `stream_count` 按条数
        /// 回收)。**仅作 RecvHalf 终结排空完成时的兜底**(Workers 消亡/
        /// abort 防御路径,无人清的场景)。
        ///
        /// 复核修复(§0.3-2/§0.3-3):规格 §2.4 原把 opening 排空放在
        /// drop_all_streams 里、由各 fatal 入口**同步**执行——但已路由进分片
        /// 通道、其流尚未被 Worker 收编的出站流首帧(对端响应的 Data|ACK /
        /// WU|ACK 与 EOF 同一读突发到达),会在 Worker 终结排空时因
        /// opening 已空而按"unknown stream"丢弃,应用读该流得 EOF 却无
        /// 响应数据(基线全局表命中 → buffer.push 后才 Closed)。红线优先:
        /// opening 的正常排空移到各 Worker 终结排空之后、按分片子集执行
        /// (收编权分片所有,§2.5-C),此处只兜底。
        fn drain_opening_leftovers(&self) {
            let drained: Vec<_> = self.opening.lock().drain().collect();
            if !drained.is_empty() {
                self.stream_count.fetch_sub(drained.len(), Ordering::SeqCst);
            }
            for (id, s) in drained {
                let mut shared = s.lock();
                shared.update_state(self.id, id, State::Closed);
                if let Some(w) = shared.reader.take() {
                    w.wake()
                }
                if let Some(w) = shared.writer.take() {
                    w.wake()
                }
            }
        }
    }

    /// 非错误持有侧观察到的重建副本:`io::Error` 不可 Clone,以 Display
    /// 语义等价的方式重建;单元变体原样保留(EOF → Closed 精确传递)。
    /// 原始错误始终由产生侧直接返回其调用者(对齐 B4)。
    fn make_shadow(e: &ConnectionError) -> ConnectionError {
        match e {
            ConnectionError::Io(err) => ConnectionError::Io(io::Error::other(err.to_string())),
            ConnectionError::Decode(err) => {
                ConnectionError::Decode(FrameDecodeError::Io(io::Error::other(err.to_string())))
            }
            ConnectionError::Closed => ConnectionError::Closed,
            ConnectionError::NoMoreStreamIds => ConnectionError::NoMoreStreamIds,
            ConnectionError::TooManyStreams => ConnectionError::TooManyStreams,
            ConnectionError::InvalidWindowUpdate => ConnectionError::InvalidWindowUpdate,
        }
    }

    impl<T: AsyncRead + AsyncWrite + Unpin + crate::Writev2> Connection<T> {
        /// 拆分收/发双半 + N 个接收侧分片 Worker + N 个发送侧分片
        /// Worker(round-6 D7 四元组;rabbithole 专用;先例
        /// [`Stream::into_halves`])。收半是纯切帧的 Framer,发半是唯一
        /// 写 socket 的 FlushTask,RecvWorker 独占 `sid % N` 流集合的接收
        /// 派发,SendWorker 独占同模数的命令消费/帧转换——四者可由不同
        /// 任务独立 poll,对齐 Go yamux 的 `go s.recv()` / `go s.send()`
        /// + frp 多 goroutine 分摊。
        ///
        /// 仅支持未激活的连接(`Connection::new` 之后、任何 poll 之前):
        /// 激活后载体已并入未拆分路径的 fused socket,不可取回。
        ///
        /// 锁路径:单一载体经 [`SplitSocket`](自研 BiLock)串行,两个半
        /// 持同一 socket 的 clone——TLS/WS/QUIC/KCP 等不可 fd 复制的载体
        /// 走此路径(泛型兜底,行为与 Round-D 起完全一致)。
        ///
        /// Worker 是裸 Future(futures-only 红线):yamux 不 spawn,由
        /// 调用方(rh-transport)spawn。
        pub fn into_halves(mut self) -> Halves<SplitSocket<T>, SplitSocket<T>> {
            let inner = std::mem::replace(&mut self.inner, ConnectionState::Poisoned);
            drop(self); // Poisoned 分支无 drop_all_streams 副作用
            match inner {
                ConnectionState::Init(socket, cfg, mode) => {
                    let split = split_socket(socket);
                    Self::halves_from_sockets(mode, cfg, split.clone(), split, "split halves")
                }
                _ => panic!(
                    "into_halves requires an unpolled connection \
                     (between Connection::new and the first poll)"
                ),
            }
        }

        /// 真·载体拆分(round-E2,裸 TCP fd-dup 双载体):`self` 的载体归
        /// 收半(读侧),`write_io` 归发半(写侧)——两个独立 IO 对象,
        /// 无 [`SplitSocket`]、无锁,读∥写在载体层物理并行。对齐
        /// frp/fmux 收发双 goroutine 各持 conn 读写的事实形态(Go 侧
        /// net.Conn 天然并发安全,Rust 侧以同 socket 的 fd 副本等价)。
        ///
        /// 调用契约:`write_io` 与 `self` 的载体必须指向同一全双工 socket
        /// (如同一 `TcpStream` 的两次 `try_clone`;双 fd 均满足本方法的
        /// trait 界)。方向纪律:写侧绝不读、读侧绝不写——halves 的调用面
        /// 天然如此(收半只经 `poll_next` 解帧,发半只经
        /// `poll_ready`/`start_send`/`poll_flush` 编帧写出),fd 各归一侧,
        /// 无消费冲突;入站数据(含 peek 前传字节)只从读侧 fd 消费。
        /// shutdown 语义:双 fd 指向同一 socket,写侧 `poll_close` 即对端
        /// 可见的 shutdown(WR) 半关闭,读侧 EOF 语义不变;socket 完全
        /// 释放需两侧 fd 同时 drop(与锁路径 halves 的持有语义一致)。
        ///
        /// 仅支持未激活的连接(同 [`Connection::into_halves`])。
        pub fn into_halves_duplex(mut self, write_io: T) -> Halves<T, T> {
            let inner = std::mem::replace(&mut self.inner, ConnectionState::Poisoned);
            drop(self); // Poisoned 分支无 drop_all_streams 副作用
            match inner {
                ConnectionState::Init(socket, cfg, mode) => {
                    Self::halves_from_sockets(mode, cfg, socket, write_io, "split halves duplex")
                }
                _ => panic!(
                    "into_halves_duplex requires an unpolled connection \
                     (between Connection::new and the first poll)"
                ),
            }
        }

        /// halves 组装公共体:读/写载体各自归半。锁路径两侧为同一
        /// [`SplitSocket`] 的 clone(共享 BiLock);duplex 路径为两个
        /// 独立 fd,零共享。round-5 起 N 个接收侧分片 Worker 与两半同时
        /// 构造(§2.4:通道/ctrl/建流通道三件套 + 路由表挂入 ConnShared);
        /// round-6 起每 Worker 另辟一条 send_out 通道(cap
        /// [`shard::SEND_OUT_CAP`],§2.4),producer 入 SendWorker、
        /// consumer 汇入 FlushTask(发送半),四元组返回(D7)。
        fn halves_from_sockets<R, W>(
            mode: Mode,
            cfg: Config,
            read_socket: R,
            write_socket: W,
            tag: &str,
        ) -> Halves<R, W>
        where
            R: AsyncRead + AsyncWrite + Unpin + crate::Writev2,
            W: AsyncRead + AsyncWrite + Unpin + crate::Writev2,
        {
            let id = Id::next();
            log::debug!("new connection: {id} ({mode:?}) [{tag}]");
            let shard_n = cfg.shard_workers;
            let shared = Arc::new(ConnShared::new(id, mode, cfg, shard_n));
            let rtt = rtt::Rtt::new();
            let mut workers = Vec::with_capacity(shard_n);
            let mut send_workers = Vec::with_capacity(shard_n);
            let mut new_streams_rxs = Vec::with_capacity(shard_n);
            let mut routes = Vec::with_capacity(shard_n);
            let mut out_consumers = Vec::with_capacity(shard_n);
            for index in 0..shard_n {
                let (msg_producer, msg_consumer) =
                    shard::ShardChannel::with_capacity(shard::SHARD_CHANNEL_CAP);
                let (ctrl_producer, ctrl_consumer) =
                    shard::ShardChannel::with_capacity(shard::SHARD_CHANNEL_CAP);
                let (new_streams_tx, new_streams_rx) = mpsc::channel(shard::NEW_STREAMS_CAP);
                new_streams_rxs.push(new_streams_rx);
                // send_out[k]:SendWorker k → FlushTask(SPSC,每 Worker 一条,
                // D2;复用 Round-5 轻量通道,elision 契约原样)
                let (out_producer, out_consumer) =
                    shard::ShardChannel::with_capacity(shard::SEND_OUT_CAP);
                out_consumers.push(out_consumer);
                // SendWorker 的 ctrl Remove 路由:本分片流 sid%N==k 恰路由
                // RecvWorker k(D1),构造时预存该 producer 免每轮取锁
                send_workers.push(SendWorker::new(
                    index,
                    shared.clone(),
                    out_producer,
                    ctrl_producer.clone(),
                ));
                routes.push((msg_producer, ctrl_producer));
                workers.push(ShardWorker::new(
                    index,
                    shard_n,
                    shared.clone(),
                    msg_consumer,
                    ctrl_consumer,
                    new_streams_tx,
                ));
            }
            // round-8 优化项 2:RecvHalf 本地缓存各 Worker 帧通道生产者,
            // 解包热路径免锁免克隆(shared.workers 仅供测试断言回看)
            let route_producers: Vec<_> = routes.iter().map(|(p, _)| p.clone()).collect();
            *shared.workers.lock() = routes;
            (
                RecvHalf {
                    socket: frame::Io::new(id, read_socket).fuse(),
                    shared: shared.clone(),
                    route_producers,
                    rtt: rtt.clone(),
                    new_streams_rxs,
                    pending_route: None,
                    teardown: None,
                    flush_signaled: false,
                    finished: false,
                },
                SendHalf {
                    socket: frame::Io::new(id, write_socket).fuse(),
                    shared,
                    send_out_consumers: out_consumers,
                    out_cursor: 0,
                    next_id: match mode {
                        Mode::Client => 1,
                        Mode::Server => 2,
                    },
                    rtt,
                    finished: false,
                },
                workers,
                send_workers,
            )
        }
    }

    /// Connection 的接收半:纯切帧的 Framer(round-5 重写,§2.4)。
    /// 只做 12B header 解析 + 按 `sid % N` 路由:连接级帧(GoAway、
    /// sid=0 Ping、Ping|ACK 的 rtt 处理)本地处理;Data/WU/Ping(sid≠0)
    /// 路由进对应 Worker 的分片通道(带 SYN → ShardMsg::Syn)。新流经
    /// 各 Worker 的 new_streams 通道回流交付;路由遇满 → 登记生产者
    /// waker 后 park(背压:TCP 停读)。EOF/错误 → 记共享错误并唤醒
    /// 发送侧与全部 Worker。
    ///
    /// 泛型参数直指 socket 类型(round-E2):锁路径 `S = SplitSocket<T>`
    /// (单一载体 + BiLock),duplex 路径 `S = T`(读侧独占 fd;
    /// [`Connection::into_halves_duplex`])。
    pub struct RecvHalf<S> {
        socket: Fuse<frame::Io<S>>,
        shared: Arc<ConnShared>,
        /// 分片 Worker 路由句柄本地缓存(round-8 优化项 2):每 Worker 的
        /// 帧通道生产者克隆,构造时一次性填充。解包热路径(`route_index` /
        /// `try_route` / `workers_gone`)原本每帧对 `shared.workers` 互斥锁
        /// 加锁 2 次 + 1 次 `Arc::clone`;Worker 集合在 halves 构造后
        /// 永不变更(生产者句柄克隆共享同一队列),缓存后热路径零锁零
        /// 分配。`shared.workers` 字段保留供 halves 构造与测试断言。
        route_producers: Vec<shard::ShardProducer<shard::ShardMsg>>,
        rtt: rtt::Rtt,
        /// 各 Worker 的建流通道接收器(D6/§2.4:实施者定由 RecvHalf 直接
        /// 持有;空闲时各接收器登记本任务 waker,futures mpsc 自带唤醒)。
        new_streams_rxs: Vec<mpsc::Receiver<Stream>>,
        /// 路由背压暂存:分片通道满时持有未路由帧,唤醒后最先重试
        /// (不丢帧,§2.1 防线)。
        pending_route: Option<(usize, shard::ShardMsg)>,
        /// 终结排空态(§0.3-2/§0.3-3,复核修复):fatal 已决但尚未返回
        /// ——先冲洗 pending_route、交付 Workers 的全部建流产出(含
        /// fatal 排空新建的),全部落地后才返回终结结果。基线单任务逐帧
        /// 同步处理完才见 EOF,分片后的等价语义。
        teardown: Option<TearDown>,
        /// 本侧已宣告"路由终结"(framer_flushed 已置位)的本地镜像,
        /// 防重复广播。
        flush_signaled: bool,
        finished: bool,
    }

    /// RecvHalf 的终结结果(§2.4 终结分支):Err = 对外一次性报告错误;
    /// Eof = 对端正常关闭,收敛为 Ready(None)。
    enum TearDown {
        Err(ConnectionError),
        Eof,
    }

    /// on_frame 的路由结果:Done = 已路由/已本地处理;Backpressured =
    /// 分片通道满(生产者 waker 已登记 + 复查),消息暂存由调用方 park。
    enum Routed {
        Done,
        Backpressured { index: usize, msg: shard::ShardMsg },
    }

    impl<S: AsyncRead + AsyncWrite + Unpin + crate::Writev2> RecvHalf<S> {
        /// 驱动接收:交付新入站流(经 Worker 建流通道回流);`Ready(None)`
        /// = EOF;`Ready(Some(Err))` = IO/协议错误。错误只交付一次,此后
        /// 收敛为 `Ready(None)`(对齐未拆分 `Connection::poll_next_inbound`
        /// 的状态机语义)。
        ///
        /// 终结排空(§0.3-2/§0.3-3):fatal 决定后不立即返回——基线单任务
        /// 逐帧同步处理完才见 EOF,分片后的等价语义是先冲洗 pending_route
        /// (该帧已从载体读出)、交付 Workers 的全部建流产出(含其 fatal
        /// 排空新建的),全部落地后才返回终结结果。
        pub fn poll(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<Stream>>> {
            loop {
                if self.finished {
                    return Poll::Ready(None);
                }
                if self.teardown.is_some() {
                    // 排空①:pending_route 冲洗。fatal 态 Worker 无条件
                    // 排空分片通道,空间必恢复;try_route 已登记生产者
                    // waker 并复查(§2.1 防线),park 必被推进。
                    if let Some((index, msg)) = self.pending_route.take() {
                        match self.try_route(index, msg, cx) {
                            Ok(()) => {}
                            Err((index, msg)) => {
                                self.pending_route = Some((index, msg));
                                return Poll::Pending;
                            }
                        }
                    }
                    if !self.flush_signaled {
                        self.flush_signaled = true;
                        // 排空②:路由终结宣告——此后不再有新消息入通道
                        // (Worker fatal 排空的终结条件,防"冲洗帧落入已
                        // 终结 Worker"的竞态丢失),广播唤醒等待中的 Worker
                        self.shared.framer_flushed.store(true, Ordering::Release);
                        self.shared.wake_all_workers();
                    }
                    // 排空③:交付 Workers 的建流产出,直至全部接收器耗尽
                    // (sender close_channel/drop = 对应 Worker 已终结)
                    let mut exhausted = true;
                    for rx in self.new_streams_rxs.iter_mut() {
                        match rx.poll_next_unpin(cx) {
                            Poll::Ready(Some(stream)) => {
                                log::trace!("{}: new inbound {}", self.shared.id, stream);
                                return Poll::Ready(Some(Ok(stream)));
                            }
                            Poll::Ready(None) => {}
                            Poll::Pending => exhausted = false,
                        }
                    }
                    if exhausted {
                        self.finished = true;
                        // 兜底清 opening 残留:全部 Worker 已终结时各自清过
                        // 本分片子集(此处必空);Workers 消亡(abort)路径
                        // 无人清,在此收口(状态 Closed + 计数回收)
                        self.shared.drain_opening_leftovers();
                        return match self.teardown.take().expect("teardown set") {
                            TearDown::Err(e) => Poll::Ready(Some(Err(e))),
                            TearDown::Eof => Poll::Ready(None),
                        };
                    }
                    // 尚有 Worker 在 fatal 排空(建流暂存交付中):挂起
                    // 等待——其 try_send 或终结(close_channel)都会唤醒
                    // 本任务;Worker fatal 排空必达终结,无死锁
                    return Poll::Pending;
                }
                if let Some(e) = self.shared.take_error() {
                    // 消费对端产出的错误(错误槽一次性;RecvHalf Drop 兜底
                    // 回填 Closed 供 SendHalf 收敛,现行不变量)。Workers 已
                    // 由产出侧 set_error 唤醒进入终结排空;opening 不在此
                    // 排空(待收编帧须能收编,见 drain_opening_leftovers)
                    self.teardown = Some(TearDown::Err(e));
                    continue;
                }
                // §2.6 防御路径:Worker 消亡(spawn 句柄被 abort)→
                // 生产者侧 consumer_gone → fatal(Closed)
                if self.workers_gone() {
                    self.begin_fatal(ConnectionError::Closed, true);
                    continue;
                }
                // 1) new_streams 交付:轮询 N 条接收器,有新流 → 立即交付
                //    (空闲时各接收器登记本任务 waker,futures mpsc 自带)。
                //    必须先于 pending_route 重试:D6 背压链——分片通道满
                //    (路由受阻)时 Worker 因 new_streams 满而门控,唯有
                //    本任务消费建流通道释放容量后 Worker 才继续排空分片
                //    通道;若先重试路由,满通道 park 永远到不了这里 → 互锁。
                for rx in self.new_streams_rxs.iter_mut() {
                    if let Poll::Ready(Some(stream)) = rx.poll_next_unpin(cx) {
                        log::trace!("{}: new inbound {}", self.shared.id, stream);
                        return Poll::Ready(Some(Ok(stream)));
                    }
                }
                // 2) 背压暂存重试(§2.4:唤醒后重试,不丢帧)
                if let Some((index, msg)) = self.pending_route.take() {
                    match self.try_route(index, msg, cx) {
                        Ok(()) => continue, // 路由成功,回到常规循环
                        Err((index, msg)) => {
                            self.pending_route = Some((index, msg));
                            self.shared.set_recv_waker(cx.waker());
                            if self.shared.error.lock().is_some() || self.workers_gone() {
                                continue;
                            }
                            return Poll::Pending; // 背压:TCP 停读
                        }
                    }
                }
                // 3) 载体读一帧(frame::Io 零拷贝路径不动):连接级帧本地
                //    处理;流级帧按 sid % N 路由。注:连续路由不 yield
                //    (规格 §2.4 注记:本轮 ≥ ROUTE_BATCH=64 条也继续,
                //    tokio 任务内循环本就不抢占);帧循环自然受通道容量
                //    节流(cap 128)。
                match self.socket.poll_next_unpin(cx) {
                    Poll::Ready(Some(Ok(frame))) => match self.on_frame(frame, cx) {
                        Ok(Routed::Done) => {}
                        Ok(Routed::Backpressured { index, msg }) => {
                            self.pending_route = Some((index, msg));
                            self.shared.set_recv_waker(cx.waker());
                            // 复查(要点 1 防线):错误槽/Worker 消亡不丢唤醒
                            if self.shared.error.lock().is_some() || self.workers_gone() {
                                continue;
                            }
                            return Poll::Pending; // 背压:TCP 停读
                        }
                        Err(e) => {
                            self.begin_fatal(e, true);
                            continue;
                        }
                    },
                    Poll::Ready(Some(Err(e))) => {
                        let e: ConnectionError = e.into();
                        self.begin_fatal(e, true);
                        continue;
                    }
                    Poll::Ready(None) => {
                        // EOF:错误槽置位(唤醒 Workers fatal 排空)但不
                        // 对外报错,终结收敛为 Ready(None)(现行语义)
                        self.begin_fatal(ConnectionError::Closed, false);
                        continue;
                    }
                    Poll::Pending => {
                        // park 前登记 waker 并复查:发送侧错误/Worker 消亡
                        // 不丢唤醒(要点 1 防线;new_streams 由 futures mpsc
                        // 的登记原子性兜住,无复查缝)
                        self.shared.set_recv_waker(cx.waker());
                        if self.shared.error.lock().is_some() || self.workers_gone() {
                            continue;
                        }
                        return Poll::Pending;
                    }
                }
            }
        }

        /// 进入终结排空(产出侧):置共享错误(shadow 供 SendHalf/Workers
        /// 观察 + 粘性 shutdown 广播,Workers 由此唤醒进入终结排空),
        /// 记录待返回的终结结果;返回前先完成终结排空(见 poll 顶部)。
        /// opening **不在此排空**:已路由、待收编的出站流首帧须能在 Worker
        /// 终结排空中收编落缓冲(§0.3-3),排空交各 Worker 终结后按分片
        /// 执行 + [`ConnShared::drain_opening_leftovers`] 兜底。
        fn begin_fatal(&mut self, e: ConnectionError, report_err: bool) {
            self.shared.set_error(make_shadow(&e));
            self.teardown = Some(if report_err {
                TearDown::Err(e)
            } else {
                TearDown::Eof
            });
        }

        /// Workers 消亡探测(§2.6 防御):任一分片通道消费者已 drop。
        /// round-8 优化项 2:经本地缓存句柄探测,免取 `shared.workers` 锁。
        fn workers_gone(&self) -> bool {
            self.route_producers.iter().any(|p| p.consumer_gone())
        }

        /// 纯切帧分派(§2.4):GoAway → fatal;Ping|ACK → rtt;Ping
        /// (sid=0)→ Pong;其余按 sid % N 路由(带 SYN → ShardMsg::Syn,
        /// 由 Worker 执行建流全流程)。
        fn on_frame(
            &mut self,
            frame: Frame<()>,
            cx: &mut Context<'_>,
        ) -> std::result::Result<Routed, ConnectionError> {
            log::trace!("{}: received: {}", self.shared.id, frame.header());
            match frame.header().tag() {
                Tag::GoAway => Err(ConnectionError::Closed),
                Tag::Ping => {
                    let ping = frame.into_ping();
                    if ping.header().flags().contains(header::ACK) {
                        // Pong:本地 rtt 处理(意外 pong → Terminate 语义:
                        // 只发 GoAway 帧不断连,现行行为)
                        match self.rtt.handle_pong(ping.id()) {
                            Action::None => {}
                            Action::Terminate(f) => self.shared.push_to_send(f.into()),
                            _ => unreachable!("rtt 只产生 None/Terminate"),
                        }
                        return Ok(Routed::Done);
                    }
                    if ping.header().stream_id() == CONNECTION_ID {
                        let mut hdr = Header::ping(ping.header().id());
                        hdr.ack();
                        self.shared.push_to_send(Frame::new(hdr).into());
                        return Ok(Routed::Done);
                    }
                    // 流级 Ping(sid≠0):路由(§2.3 Ping 行)
                    let index = self.route_index(ping.header().stream_id());
                    let msg = shard::ShardMsg::Frame(Frame::from(ping));
                    self.route(index, msg, cx)
                }
                Tag::Data | Tag::WindowUpdate => {
                    let index = self.route_index(frame.header().stream_id());
                    let msg = if frame.header().flags().contains(header::SYN) {
                        shard::ShardMsg::Syn(frame)
                    } else {
                        shard::ShardMsg::Frame(frame)
                    };
                    self.route(index, msg, cx)
                }
            }
        }

        /// round-8 优化项 2:直接按缓存句柄数取模,免 `shared.workers`
        /// 互斥锁——每帧数据路由的原热路径。
        fn route_index(&self, sid: StreamId) -> usize {
            (sid.val() as usize) % self.route_producers.len()
        }

        /// 路由一帧到 worker[index](§2.4):try_push 失败 → 登记生产者
        /// waker + 复查(§2.1 防线)→ 仍满 → 返回消息由调用方 park 背压
        /// (TCP 停读)。
        /// round-8 优化项 2:直接借用本地缓存句柄(免锁 + 免 `Arc::clone`,
        /// 原实现每帧 2 次取锁 + 1 次克隆)。
        fn try_route(
            &self,
            index: usize,
            msg: shard::ShardMsg,
            cx: &mut Context<'_>,
        ) -> std::result::Result<(), (usize, shard::ShardMsg)> {
            let producer = &self.route_producers[index];
            match producer.try_push(msg) {
                Ok(()) => Ok(()),
                Err(msg) => {
                    producer.register_producer_waker(cx.waker());
                    // 复查(丢唤醒防线):登记后让出空间即唤醒
                    match producer.try_push(msg) {
                        Ok(()) => Ok(()),
                        Err(msg) => Err((index, msg)),
                    }
                }
            }
        }

        fn route(
            &mut self,
            index: usize,
            msg: shard::ShardMsg,
            cx: &mut Context<'_>,
        ) -> std::result::Result<Routed, ConnectionError> {
            match self.try_route(index, msg, cx) {
                Ok(()) => Ok(Routed::Done),
                Err((index, msg)) => Ok(Routed::Backpressured { index, msg }),
            }
        }
    }

    /// Round-5 回归测试专用(不得用于生产):暴露共享状态句柄,供测试
    /// 断言计数器/开流表(镜像 SendHalf::shared_for_test)。
    #[cfg(test)]
    impl<S> RecvHalf<S> {
        pub(crate) fn shared_for_test(&self) -> Arc<ConnShared> {
            self.shared.clone()
        }
    }

    impl<S> Drop for RecvHalf<S> {
        fn drop(&mut self) {
            // 要点 4:接收半弃置 → 共享错误置 Closed,唤醒发送侧与 Worker
            //(set_error 广播 worker_wakers + 粘性影子,Worker 收敛终结)。
            // 未冲洗的 pending_route 随弃置丢弃(不再路由),路由终结宣告
            // 必须补上——Worker fatal 排空的终结条件,否则 Worker 永挂。
            self.shared.set_error(ConnectionError::Closed);
            self.shared.framer_flushed.store(true, Ordering::Release);
            self.shared.wake_all_workers();
        }
    }

    /// Connection 的发送半:FlushTask(round-6 §3 改造;round-5 曾持
    /// `stream_receivers` SelectAll 直接消费流命令)。职责二分(D6):
    /// 计算(多流命令轮询/帧转换/on_drop 选帧)分摊给 N 个 SendWorker,
    /// 本任务只剩**唯一写者**面——`poll_new_outbound` 开流(opening 预
    /// 注册 + 双计数器,冷路径且依赖 next_id 串行点)、接收侧压来的
    /// Pong/GoAway(to_send)、rtt ping、send_out[k] 轮转取帧、批量聚合
    /// writev(≤16 帧/≤256KiB)、部分写/flush。
    ///
    /// `poll` 帧源优先级:to_send(控制,不变)> `rtt.next_ping()`(不变)
    /// \> send_out 轮转(D3 严格轮转游标 `out_cursor`:自游标起扫 N 条取
    /// 第一条非空的一帧,取出后 `out_cursor = (k+1) % N`——固定顺序在
    /// Worker 0 持续满队列时会饿死后面的 Worker)。不含 Round-A/C 公平
    /// 调度读探测(读任务物理独立,无需写侧让位)。
    ///
    /// 泛型参数直指 socket 类型(round-E2):锁路径 `S = SplitSocket<T>`
    /// (单一载体 + BiLock),duplex 路径 `S = T`(写侧独占 fd;
    /// [`Connection::into_halves_duplex`])。
    pub struct SendHalf<S> {
        socket: Fuse<frame::Io<S>>,
        shared: Arc<ConnShared>,
        /// 每 SendWorker 一条的 send_out 通道消费句柄(§2.1:ShardChannel
        /// 是 SPSC 契约,单一共享队列会互相覆盖丢唤醒,故每 Worker 一条)。
        send_out_consumers: Vec<shard::ShardConsumer<Frame<()>>>,
        /// 轮转游标(D3:每取一帧 `cursor = (k+1) % N`,严格轮转保证每个
        /// 非空队列每批至少分得份额)。
        out_cursor: usize,
        next_id: u32,
        rtt: rtt::Rtt,
        finished: bool,
    }

    impl<S: AsyncRead + AsyncWrite + Unpin + crate::Writev2> SendHalf<S> {
        /// 开新出站流(§2.5-A:opening 预注册 + 双计数器;ACK backlog
        /// 门控经共享 waker 由 Worker 的 ACK 处理唤醒——与未拆分路径
        /// 同语义)。
        pub fn poll_new_outbound(&mut self, cx: &mut Context<'_>) -> Poll<Result<Stream>> {
            if self.finished {
                return Poll::Ready(Err(ConnectionError::Closed));
            }
            if let Some(e) = self.shared.take_error() {
                // 产出侧 set_error 已唤醒 Workers 进入终结排空(opening 由其
                // 终结后按分片排空,此处不得抢先清——待收编帧须能收编)
                self.finished = true;
                return Poll::Ready(Err(e));
            }
            // D4:全局计数器精确上界(fetch_add 回滚保证跨分片精确)
            if self.shared.stream_count.fetch_add(1, Ordering::SeqCst) + 1
                > self.shared.config.max_num_streams
            {
                self.shared.stream_count.fetch_sub(1, Ordering::SeqCst);
                log::error!("{}: maximum number of streams reached", self.shared.id);
                return Poll::Ready(Err(ConnectionError::TooManyStreams));
            }
            // 先登记 waker 再查 backlog:ACK 恰在登记与检查之间到达时由
            // 复查兜住(至多多一次虚假唤醒,不丢唤醒)——现行顺序保留
            *self.shared.new_outbound_stream_waker.lock() = Some(cx.waker().clone());
            if self.shared.ack_backlog_count.load(Ordering::SeqCst) >= MAX_ACK_BACKLOG {
                log::debug!(
                    "{MAX_ACK_BACKLOG} streams waiting for ACK, registering task for wake-up \
                     until remote acknowledges at least one stream"
                );
                // 未建流:回滚计数(重试不得重复累计)
                self.shared.stream_count.fetch_sub(1, Ordering::SeqCst);
                return Poll::Pending;
            }

            log::trace!("{}: creating new outbound stream", self.shared.id);

            let id = match self.next_stream_id() {
                Ok(id) => id,
                Err(e) => {
                    self.shared.stream_count.fetch_sub(1, Ordering::SeqCst);
                    return Poll::Ready(Err(e));
                }
            };
            let stream = self.make_new_outbound_stream(id);

            log::debug!("{}: new outbound {}", self.shared.id, stream);
            // D4:未确认计数 +1 并挂"递减令牌"(必须在 opening 插入前
            // 完成——opening 一旦可见,Worker 即可能收编并发起状态迁移,
            // 令牌须先就位)。令牌保证该流离开 Open{ack:false} 的**任何**
            // 路径(对端 ACK 确认 / 本地 poll_close 半关闭 / 对端 FIN、
            // RST / 删除 / 连接排空)恰递减一次——与基线 ack_backlog()
            // 实时表扫描语义等价(复核修复:仅靠 ACK 分支与删除路径递减
            // 会漏掉本地半关闭等常态路径,泄漏 256 条后开流永久 Pending)。
            {
                let mut ss = stream.shared();
                ss.ack_backlog_token = Some(self.shared.ack_backlog_count.clone());
                self.shared.ack_backlog_count.fetch_add(1, Ordering::SeqCst);
                if !matches!(
                    ss.state(),
                    State::Open {
                        acknowledged: false
                    }
                ) {
                    // 防御(理论上不可达:SYN 尚未上线,对端无从发来该流
                    // 任何帧):挂令牌前已被并发迁出未确认态 → 令牌无人
                    // 消费,就地撤销
                    ss.ack_backlog_token = None;
                    self.shared.ack_backlog_count.fetch_sub(1, Ordering::SeqCst);
                }
            }
            // D3:出站预注册进 opening(不再插全局表);Worker 首帧按需
            // 收编,从未收到帧的出站流不占 Worker 表
            self.shared.opening.lock().insert(id, stream.clone_shared());

            Poll::Ready(Ok(stream))
        }

        /// 驱动发送路径,尽可能推进到 Pending。`Ready(Err(e))` = 本侧或
        /// 对侧错误(终结态);此后 poll 恒 `Err(Closed)`(对齐未拆分
        /// `Connection` 的 Closed 语义)。`Ready(Ok(()))` 不会出现
        /// (防御:调用方按终结处理)。
        pub fn poll(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
            loop {
                if self.finished {
                    return Poll::Ready(Err(ConnectionError::Closed));
                }
                if let Some(e) = self.shared.take_error() {
                    // 同 poll_new_outbound:不抢先清 opening
                    self.finished = true;
                    return Poll::Ready(Err(e));
                }
                // B4 同款三态:Ready(Err) 以原始 IO 错误传播并终结。
                // round-5 §3.3:Sink 的 poll_ready Ready(Ok) ⟺ 批队列 +
                // 半写帧已全部写穿(§3.2 契约);随后按优先级连续取帧入队
                // (`fill_batch`),有帧入队即 continue——下轮 poll_ready 把
                // 整批一次 writev 写出;无帧可取 ⟹ 队列此刻为空。
                // `sink_idle`(Round-D3-2 门控的重推导)= 本轮 poll_ready 曾
                // Ready(Ok) 且当前队列 + current 为空;park 复查对 to_send/
                // send_out 的保留以此为门,见下方 park 段注释。
                let sink_idle = match self.socket.poll_ready_unpin(cx) {
                    Poll::Ready(Ok(())) => match self.fill_batch() {
                        Ok(0) => true,
                        Ok(_) => continue,
                        Err(e) => return Poll::Ready(Err(e)),
                    },
                    Poll::Ready(Err(e)) => return Poll::Ready(Err(self.fatal(e.into()))),
                    Poll::Pending => false,
                };

                match self.socket.poll_flush_unpin(cx) {
                    Poll::Ready(Ok(())) | Poll::Pending => {}
                    Poll::Ready(Err(e)) => return Poll::Ready(Err(self.fatal(e.into()))),
                }

                self.shared.set_send_waker(cx.waker());
                // round-6 §3.2:每轮循环**无条件**为全部 send_out[k] 登记
                // 消费者 waker(与 set_send_waker 同点;防线不依赖"只在
                // park 时登记"——Worker push 空变非空即唤醒,登记前压入
                // 的帧由下方复查捕获)。
                for consumer in &self.send_out_consumers {
                    consumer.register_consumer_waker(cx.waker());
                }
                // Round-D3(自旋修复)+ Round-D3-2(丢唤醒修复)+ round-6
                // §3.2(send_out 项):复查项 = 无需载体即可推进的错误,
                // **加** to_send 与 send_out——但两者都以 sink_idle 为门
                // (Round-D3 论证对 send_out 逐字适用):
                //
                // - sink_idle == false(批队列/半写帧未写穿,poll_ready 恒
                //   Pending):to_send/send_out 帧无法出队,把它们当作
                //   "有工作"会让 poll 永不返回 Pending(单核自旋,三机实测
                //   260 万次迭代);此形态下 park 时 EPOLLOUT 必已武装
                //   (批 writev 阻塞必经载体写的 WouldBlock),可写即唤醒,
                //   不存在丢唤醒——to_send/send_out 不参与,**不得复活自旋**。
                // - sink_idle == true(空 sink 型 park):本轮 poll_ready 曾
                //   Ready(Ok) 且 fill_batch 取到 0 帧(队列与 send_out 全
                //   空),未武装任何载体写 waker;若当次 poll 由消耗
                //   send_waker/consumer waker 槽的唤醒启动,则在 pop_to_send
                //   /try_pop 与本登记之间压入的 Pong/GoAway/RST/FIN 既不
                //   触发唤醒、也无人复查,滞留到下一个无关事件——必须复查;
                //   门保证了 continue 后下一轮真正入队写穿。
                //
                // ctrl_pending 已随 on_drop_stream 迁入 SendWorker(§3.3),
                // 其唤醒由 ctrl 通道生产者 waker 保证,与本任务无关。
                if (sink_idle && !self.shared.to_send.lock().is_empty())
                    || (sink_idle && self.send_out_consumers.iter().any(|c| c.has_pending()))
                    || self.shared.error.lock().is_some()
                {
                    continue;
                }
                return Poll::Pending;
            }
        }

        /// §3.3.2 批量取帧:按优先级连续取帧 `start_send` 入 Sink 批队列,
        /// 直至聚合上限([`MAX_BATCH_FRAMES`] 帧 / [`MAX_BATCH_BYTES`]
        /// 字节)或帧源 Pending。返回本轮入队帧数;Err = 载体 start_send
        /// 失败(已 fatal)。
        ///
        /// 字节上限按**已入队**字节判定:帧源不可窥探下一帧大小(rtt/
        /// 命令通道取出即消费、不可退回),取出即入队,最后一帧可越过
        /// 上限 < 1 帧——与 §3.2"队列为空时单帧无条件入队、上限只约束
        /// 聚合"同理;帧数上限精确。调用方拿到 Ready(Ok) 时队列必为空
        /// (§3.2),故本轮入队批即队列全量,批有界 ⟹ 队列有界。
        fn fill_batch(&mut self) -> Result<usize> {
            let mut frames = 0usize;
            let mut bytes = 0usize;
            while frames < MAX_BATCH_FRAMES && bytes < MAX_BATCH_BYTES {
                let Some(frame) = self.next_frame() else {
                    break;
                };
                bytes += frame.wire_len();
                frames += 1;
                if let Err(e) = self.socket.start_send_unpin(frame) {
                    return Err(self.fatal(e.into()));
                }
            }
            Ok(frames)
        }

        /// 取下一待发帧(帧优先级,要点 2 / §3.3.1 + round-6 §3.1):
        /// 接收侧压来的 Pong/GoAway(含 RecvWorker 经 to_send 的 RST/FIN)
        /// \> rtt ping > send_out[k] 严格轮转——不低于未拆分路径
        /// (pending_read_frame > ping/pending_write_frame 再 > 流命令)。
        /// `None` = 帧源全部空;各 send_out[k] 的消费者 waker 已在 poll
        /// 主循环每轮登记(§3.2),Worker push 空变非空即唤醒本任务。
        fn next_frame(&mut self) -> Option<Frame<()>> {
            if let Some(frame) = self.shared.pop_to_send() {
                return Some(frame);
            }
            // Note `next_ping` does not register a waker and thus if not called
            // regularly (idle connection) no ping is sent. This is deliberate
            // as an idle connection does not need RTT measurements.
            if let Some(frame) = self.rtt.next_ping() {
                return Some(frame.into());
            }
            // D3 严格轮转:自 out_cursor 起扫描 N 条,取第一条非空的一帧,
            // 取出后 out_cursor = (k + 1) % N——固定顺序在 Worker 0 持续
            // 满队列时会饿死后面的 Worker;逐帧 try_pop(不整段 drain)
            // 保证每个非空队列每批至少分得份额。
            let n = self.send_out_consumers.len();
            for i in 0..n {
                let k = (self.out_cursor + i) % n;
                if let Some(f) = self.send_out_consumers[k].try_pop() {
                    self.out_cursor = (k + 1) % n;
                    // 取空通知(顺序契约的唤醒半边):SendWorker 滞留的 ctrl
                    // Remove 以"本流末帧已出队"的水位门控(shard.rs
                    // flush_ctrl_pending),出队水位由 try_pop 推进;队列
                    // 取空 ⟹ 水位必已追平任何已记录门控,此刻唤醒 Worker k
                    // 重评。这是队列**未满**期间 FlushTask 对 Worker 的唯一
                    // 唤醒(未满无满变非满唤醒;逐帧唤醒会给热路径加每帧
                    // 取锁),配合 Worker 自身被命令到达/满变非满唤醒时的
                    // 逐轮复查,水位追平后有界时间内必投递。take 语义,未
                    // 登记则其 park 复查兜住,不丢唤醒。
                    // round-6.1 F2 前置门:ctrl_wait[k] 为假 ⟹ Worker k 无
                    // 滞留 Remove(置位/清零仅其自身,见 ConnShared 字段
                    // 注释),一次原子读即跳过 has_pending/wakers 两把锁与
                    // 唤醒——受限/环回工况下几乎每帧出队即取空,无门则
                    // 每帧两把锁 + 伪唤醒。flag 仅咨询性,不漏唤醒的时序
                    // 枚举见规格 round-6.1 §1.3。
                    // SeqCst(复核修复):本读与上方 try_pop 的取空推进
                    // (popped fetch_add)须同 Worker 侧(store ctrl_wait
                    // + park 复查读 popped)服从 SC 全序——否则"门读到
                    // 陈旧 false 跳过唤醒 && Worker 复查读到陈旧水位而
                    // park"的双陈旧 store-buffering 交织在抽象机上成立
                    // (x86 锁指令偶然封死,aarch64 不保证),空闲连接的
                    // Remove 无限期滞留;SC 下两者互斥,要么复查已见取空
                    // 立即投递、要么本门读到 true 走 take+wake,与机器
                    // 无关。x86 上 SeqCst load 与 Acquire 同为 mov,热路径
                    // 零额外开销(约束详述见 shard.rs popped_count 文档)。
                    if self.shared.ctrl_wait[k].load(Ordering::SeqCst)
                        && !self.send_out_consumers[k].has_pending()
                    {
                        if let Some(w) = self.shared.send_worker_wakers.lock()[k].take() {
                            w.wake();
                        }
                    }
                    return Some(f);
                }
            }
            None
        }

        /// 发送半 fatal:置共享错误(shadow + 粘性 shutdown,Workers 由此
        /// 唤醒进入终结排空——各自先处理完已路由帧、及时关闭流状态、再按
        /// 分片排空 opening)+ 自身终结。不在此清 opening(待收编帧须能
        /// 收编,§0.3-3)。
        fn fatal(&mut self, e: ConnectionError) -> ConnectionError {
            self.shared.set_error(make_shadow(&e));
            self.finished = true;
            e
        }

        fn make_new_outbound_stream(&mut self, id: StreamId) -> Stream {
            let (sender, receiver) = mpsc::channel(STREAM_COMMAND_CAP);
            // round-6 D4:注册路由改经 push_new_receiver(压 send_regs[k] +
            // 唤醒 SendWorker k;出站流首命令缓冲在通道 cap
            // `STREAM_COMMAND_CAP` 内,Worker 收编早晚不影响正确性——命令
            // 不丢、SYN 在首帧自带 flag,顺序由通道 FIFO 保证)
            self.shared.push_new_receiver(id, receiver);

            // round-9:halves 路径传入 ConnShared,纯 WU 帧走 to_send 快车道
            Stream::new_outbound(
                id,
                self.shared.id,
                self.shared.config.clone(),
                sender,
                Some(self.shared.clone()),
            )
        }

        fn next_stream_id(&mut self) -> Result<StreamId> {
            let proposed = StreamId::new(self.next_id);
            self.next_id = self
                .next_id
                .checked_add(2)
                .ok_or(ConnectionError::NoMoreStreamIds)?;
            match self.shared.mode {
                Mode::Client => assert!(proposed.is_client()),
                Mode::Server => assert!(proposed.is_server()),
            }
            Ok(proposed)
        }
    }

    /// Round-D3-2 回归测试专用(不得用于生产):暴露共享状态句柄,供测试
    /// 在丢唤醒窗口内直压 to_send(测试闭包在载体 flush 钩子内同步执行,
    /// 此刻 SendHalf 正被 &mut 借用,须持有独立 Arc 句柄)。
    #[cfg(test)]
    impl<S> SendHalf<S> {
        pub(crate) fn shared_for_test(&self) -> Arc<ConnShared> {
            self.shared.clone()
        }
    }

    impl<S> Drop for SendHalf<S> {
        fn drop(&mut self) {
            // 要点 4/§2.6:发送半弃置**不置错误**(现行)——置 send_dropped
            // + 广播唤醒 RecvWorker(先处理完已路由帧,再排空本地表与
            // opening 本分片子集,但不 finished,继续服务新 SYN,语义对齐
            // 现行"发送半弃置后接收半仍可读缓冲/建流");round-6 D9:
            // **同时唤醒全部 SendWorker**——发送面已弃置,命令通道应断
            // (SendWorker park 在 SelectAll/注册队列上,不唤醒则滞留至
            // 无关事件),其 D8 终结 drop 接收器后流侧 sender 报
            // WriteZero。opening 不在此清:已路由待收编帧须能收编
            // (§0.3-3)。
            self.shared.send_dropped.store(true, Ordering::SeqCst);
            self.shared.wake_all_workers();
            self.shared.wake_all_send_workers();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::frame::{MAX_BATCH_BYTES, MAX_BATCH_FRAMES};
    use futures::future::poll_fn;
    use std::cell::RefCell;
    use std::pin::Pin;
    use std::rc::Rc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    /// 测试载体:读侧按脚本块返回(支持部分读;耗尽 Pending),写侧可注入失败并计数。
    #[derive(Default)]
    struct TestSocketState {
        read_blocks: Mutex<std::collections::VecDeque<Vec<u8>>>,
        cur_block: Vec<u8>,
        cur_off: usize,
        written: Mutex<Vec<u8>>,
        fail_writes: AtomicBool,
        /// Arc 包装:公平调度 T3 的 waker 需跨 `Rc` 句柄快照「唤醒时刻的
        /// 载体写次数」;计数语义不变(经 Deref 照常 fetch_add/load)。
        write_calls: Arc<AtomicUsize>,
        /// writev 调用计数(round-5 §5.4 T-B1:批量写 syscall 汇聚断言;
        /// 每次 `poll_writev` +1,与单段/两段 `poll_write2` 的 write_calls
        /// 分列)。
        writev_calls: Arc<AtomicUsize>,
        /// T5 事件序列(测试基建):载体写成功记 'w',读返回 Ready(Ok(n>0))
        /// 记 'r'(0 字节 EOF 不记)——用于强制读探测批量排空的断言。
        events: Mutex<Vec<char>>,
        closed: AtomicBool,
        eof: AtomicBool,
    }

    struct TestSocket(Rc<RefCell<TestSocketState>>);

    impl TestSocket {
        fn new() -> (Self, Rc<RefCell<TestSocketState>>) {
            let st = Rc::new(RefCell::new(TestSocketState::default()));
            (TestSocket(st.clone()), st)
        }
    }

    impl crate::Writev2 for TestSocket {
        fn poll_write2(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            a: &[u8],
            b: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            let st = self.0.borrow();
            if st.fail_writes.load(Ordering::SeqCst) {
                return Poll::Ready(Err(std::io::Error::other("mock-io-write-failure")));
            }
            st.write_calls.fetch_add(1, Ordering::SeqCst);
            st.written.lock().unwrap().extend_from_slice(a);
            st.written.lock().unwrap().extend_from_slice(b);
            st.events.lock().unwrap().push('w');
            Poll::Ready(Ok(a.len() + b.len()))
        }

        /// 真·向量写(round-5 §3.1 测试侧覆盖):全部段一次写穿并计
        /// writev 次数,一次调用同时计一次载体写('w' 事件/write_calls
        /// 口径 = 载体写操作数,与 poll_write2 同级)。
        fn poll_writev(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            bufs: &[std::io::IoSlice<'_>],
        ) -> Poll<std::io::Result<usize>> {
            let st = self.0.borrow();
            if st.fail_writes.load(Ordering::SeqCst) {
                return Poll::Ready(Err(std::io::Error::other("mock-io-write-failure")));
            }
            st.write_calls.fetch_add(1, Ordering::SeqCst);
            st.writev_calls.fetch_add(1, Ordering::SeqCst);
            let mut written = st.written.lock().unwrap();
            let mut n = 0;
            for b in bufs {
                written.extend_from_slice(b);
                n += b.len();
            }
            st.events.lock().unwrap().push('w');
            Poll::Ready(Ok(n))
        }
    }

    impl futures::io::AsyncRead for TestSocket {
        fn poll_read(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &mut [u8],
        ) -> Poll<std::io::Result<usize>> {
            let mut st = self.0.borrow_mut();
            if st.cur_off >= st.cur_block.len() {
                let next = st
                    .read_blocks
                    .lock()
                    .unwrap()
                    .pop_front()
                    .unwrap_or_default();
                st.cur_block = next;
                st.cur_off = 0;
            }
            if st.cur_off < st.cur_block.len() {
                let n = (st.cur_block.len() - st.cur_off).min(buf.len());
                buf[..n].copy_from_slice(&st.cur_block[st.cur_off..st.cur_off + n]);
                st.cur_off += n;
                if n > 0 {
                    st.events.lock().unwrap().push('r');
                }
                return Poll::Ready(Ok(n));
            }
            if st.eof.load(Ordering::SeqCst) {
                return Poll::Ready(Ok(0));
            }
            Poll::Pending
        }
    }

    impl futures::io::AsyncWrite for TestSocket {
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
            self.0.borrow().closed.store(true, Ordering::SeqCst);
            Poll::Ready(Ok(()))
        }
    }

    fn syn_data_block(id: u32, body: &[u8]) -> Vec<u8> {
        let mut h = header::Header::data(StreamId::new(id), body.len() as u32);
        h.syn();
        let mut v = header::encode(&h).to_vec();
        v.extend_from_slice(body);
        v
    }

    fn wu_ack_block(id: u32) -> Vec<u8> {
        let mut h = header::Header::window_update(StreamId::new(id), 0);
        h.ack();
        header::encode(&h).to_vec()
    }

    /// 普通入站数据块(无 SYN/FIN):读洪峰预置用,目标流未知时被静默丢弃。
    fn data_block(id: u32, body: &[u8]) -> Vec<u8> {
        let h = header::Header::data(StreamId::new(id), body.len() as u32);
        let mut v = header::encode(&h).to_vec();
        v.extend_from_slice(body);
        v
    }

    /// 对指定流授信的入站窗口更新块(无 SYN/ACK/FIN)。
    fn wu_credit_block(id: u32, credit: u32) -> Vec<u8> {
        let h = header::Header::window_update(StreamId::new(id), credit);
        header::encode(&h).to_vec()
    }

    /// 首次唤醒时刻的载体写次数快照(公平调度 T3 用:断言被阻塞的
    /// writer 在有界载体写内被 WU 唤醒)。
    struct WakeAtSnapshot {
        writes: Arc<AtomicUsize>,
        wake_at: Mutex<Option<usize>>,
    }

    impl futures::task::ArcWake for WakeAtSnapshot {
        fn wake_by_ref(me: &Arc<Self>) {
            let mut slot = me.wake_at.lock().unwrap();
            if slot.is_none() {
                *slot = Some(me.writes.load(Ordering::SeqCst));
            }
        }
    }

    /// B4(红→绿):连接层写错误必须以**原始 IO 错误**传播并使连接进入
    /// cleanup,而不是被 `is_ready()` 吞掉、由后续 flush 报泛化的
    /// poisoned 错误、甚至经 start_send 覆盖 Poisoned 续发新帧。
    #[test]
    fn connection_write_error_propagates() {
        let (socket, st) = TestSocket::new();
        let mut conn = Connection::new(socket, Config::default(), Mode::Client);

        let mut stream = futures::executor::block_on(poll_fn(|cx| conn.poll_new_outbound(cx)))
            .expect("open outbound stream");
        futures::executor::block_on(poll_fn(|cx| {
            Pin::new(&mut stream).poll_write(cx, b"payload")
        }))
        .expect("queue data frame");

        st.borrow().fail_writes.store(true, Ordering::SeqCst);

        let err = match futures::executor::block_on(poll_fn(|cx| conn.poll_next_inbound(cx))) {
            Some(Err(e)) => e,
            other => panic!("write error must propagate as connection error, got {other:?}"),
        };
        assert!(
            matches!(&err, ConnectionError::Io(e) if e.to_string().contains("mock-io-write-failure")),
            "original io error must propagate (B4), got: {err:?}"
        );
        // 写侧只应被尝试一次;错误后不得续发新帧
        assert_eq!(
            st.borrow().write_calls.load(Ordering::SeqCst),
            0,
            "no further frame writes after the error"
        );
    }

    /// C2 对端侧(锁,修复前即绿):0-credit 窗口更新 + ACK 是 upstream
    /// 合法组合——接收端必须把流标记 acknowledged、唤醒阻塞中的
    /// open_stream,且不得 Terminate/回 GoAway。
    #[test]
    fn peer_accepts_zero_credit_wu_ack() {
        let (socket, st) = TestSocket::new();
        st.borrow_mut()
            .read_blocks
            .lock()
            .unwrap()
            .push_back(wu_ack_block(1));

        let mut cfg = Config::default();
        cfg.set_max_num_streams(MAX_ACK_BACKLOG * 4);
        let mut conn = Connection::new(socket, cfg, Mode::Client);

        // 打满 ACK backlog(256 个未确认 outbound 流)
        let mut streams = Vec::new();
        for _ in 0..MAX_ACK_BACKLOG {
            let s = futures::executor::block_on(poll_fn(|cx| conn.poll_new_outbound(cx)))
                .expect("open stream");
            streams.push(s);
        }
        // 下一次 open 注册 waker 后 Pending
        #[derive(Default)]
        struct CountWaker(AtomicUsize);
        impl futures::task::ArcWake for CountWaker {
            fn wake_by_ref(me: &Arc<Self>) {
                me.0.fetch_add(1, Ordering::SeqCst);
            }
        }
        let cw = Arc::new(CountWaker::default());
        let w = futures::task::waker(cw.clone());
        let mut cx = Context::from_waker(&w);
        match poll_fn(|cx| conn.poll_new_outbound(cx)).poll_unpin(&mut cx) {
            Poll::Pending => {}
            other => panic!("expected pending open_stream at backlog cap, got {other:?}"),
        }
        assert_eq!(cw.0.load(Ordering::SeqCst), 0);

        // 驱动连接处理 0-credit WU+ACK 帧
        match poll_fn(|cx| conn.poll_next_inbound(cx)).poll_unpin(&mut cx) {
            Poll::Pending => {}
            other => panic!("frame processing must not error/terminate, got {other:?}"),
        }

        // 流被 acknowledged,阻塞的 open_stream 被唤醒
        assert!(
            !streams[0].is_pending_ack(),
            "0-credit WU+ACK must acknowledge the stream"
        );
        assert!(
            cw.0.load(Ordering::SeqCst) >= 1,
            "backlogged open must be woken"
        );
        // backlog 释放后可以继续开流
        assert!(
            poll_fn(|cx| conn.poll_new_outbound(cx))
                .poll_unpin(&mut cx)
                .is_ready(),
            "open_stream must succeed after ack"
        );
        // 未发 GoAway(不 Terminate)
        let written = st.borrow().written.lock().unwrap().clone();
        let goaway = written.chunks(12).any(|c| c.len() == 12 && c[1] == 3);
        assert!(
            !goaway,
            "connection must not send GoAway, wrote {written:?}"
        );
    }

    /// R6b(锁,修复前即绿):载体 EOF 后,已缓冲数据仍可读
    /// (read_after_close 契约,cleanup 路径)。
    #[test]
    fn read_after_close_buffered_data_readable() {
        let (socket, st) = TestSocket::new();
        st.borrow_mut()
            .read_blocks
            .lock()
            .unwrap()
            .push_back(syn_data_block(2, b"pending"));

        let mut conn = Connection::new(socket, Config::default(), Mode::Client);
        let mut stream = futures::executor::block_on(poll_fn(|cx| conn.poll_next_inbound(cx)))
            .expect("inbound stream")
            .expect("ok stream");

        // EOF → 连接收敛为 None(cleanup 已 drop_all_streams:Closed+唤醒)
        st.borrow().eof.store(true, Ordering::SeqCst);
        match futures::executor::block_on(poll_fn(|cx| conn.poll_next_inbound(cx))) {
            None => {}
            other => panic!("expected EOF close, got {other:?}"),
        }

        // 已缓冲数据仍可读
        let mut buf = [0u8; 16];
        let n = futures::executor::block_on(poll_fn(|cx| {
            Pin::new(&mut stream).poll_read(cx, &mut buf)
        }))
        .expect("buffered data after close");
        assert_eq!(&buf[..n], b"pending");
        // 耗尽后 EOF
        let n = futures::executor::block_on(poll_fn(|cx| {
            Pin::new(&mut stream).poll_read(cx, &mut buf)
        }))
        .expect("eof after drain");
        assert_eq!(n, 0);
    }

    // ===== final-gaps 补修:lazy 流 close-before-first-write 的 SYN 语义 =====
    // 主代理独立复现(vendor API 缺陷):open_stream 后不 write 立即
    // shutdown,对端 accept 超时——poll_close/poll_close_half 只消费
    // Flag::Ack,未消费 Flag::Syn,FIN 无 SYN 上线,对端从未见到开流。

    /// 解析 TestSocket 已写字节为 header 序列(按 12B header + body 步进)。
    fn written_headers(st: &Rc<RefCell<TestSocketState>>) -> Vec<header::Header<()>> {
        let bytes = st.borrow().written.lock().unwrap().clone();
        let mut out = Vec::new();
        let mut off = 0;
        while off + header::HEADER_SIZE <= bytes.len() {
            let block: [u8; header::HEADER_SIZE] =
                bytes[off..off + header::HEADER_SIZE].try_into().unwrap();
            let hdr = header::decode(&block).expect("written bytes must be valid frames");
            off += header::HEADER_SIZE + hdr.len().val() as usize;
            out.push(hdr);
        }
        out
    }

    /// 目标流(id=1)的 Data 帧——过滤掉 RTT Ping(header::ping 按上游
    /// 惯例带 SYN、stream id 0)与窗口更新等无关帧。
    fn stream1_data_headers(st: &Rc<RefCell<TestSocketState>>) -> Vec<header::Header<()>> {
        written_headers(st)
            .into_iter()
            .filter(|h| h.tag() == Tag::Data && h.stream_id() == StreamId::new(1))
            .collect()
    }

    /// 驱动连接(命令队列写穿、读脚本为空 → Pending)。
    fn drive_to_pending<T>(conn: &mut Connection<T>, cx: &mut Context<'_>)
    where
        T: AsyncRead + AsyncWrite + Unpin + crate::Writev2,
    {
        match poll_fn(|cx| conn.poll_next_inbound(cx)).poll_unpin(cx) {
            Poll::Pending => {}
            other => panic!("expected pending connection drive, got {other:?}"),
        }
    }

    fn noop_cx() -> Context<'static> {
        // noop waker 测试专用:leak 一次,测试进程生命周期内使用
        Context::from_waker(Box::leak(Box::new(futures::task::noop_waker())))
    }

    // ===== round-5 分片 halves 驱动夹具(§5.3):单线程 noop waker 下手动
    // 轮询推进,等价生产侧 tokio::spawn 的多任务并行 =====

    /// 驱动 recv + workers 一轮收敛:recv 排空路由 → RecvWorker 全处理 →
    /// SendWorker 同 poll(round-6 §5.4:驱动面覆盖 SendWorker,入站建流
    /// 的命令接收器注册进 send_regs,由其收编)→ recv 再探(new_streams
    /// 交付)。返回 recv 的最终 poll 结果。
    fn drive_recv_workers<S>(
        recv: &mut RecvHalf<S>,
        workers: &mut [ShardWorker],
        send_workers: &mut [SendWorker],
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Stream>>>
    where
        S: AsyncRead + AsyncWrite + Unpin + crate::Writev2,
    {
        match recv.poll(cx) {
            Poll::Pending => {}
            other => return other,
        }
        for w in workers.iter_mut() {
            let _ = w.poll_unpin(cx);
        }
        for w in send_workers.iter_mut() {
            let _ = w.poll_unpin(cx);
        }
        recv.poll(cx)
    }

    /// 驱动 send + 双侧 Worker 一轮收敛:send(FlushTask)冲刷 → RecvWorker
    /// 全处理(建流授信命令等)→ SendWorker 消费流命令、产出帧压
    /// send_out(round-6:发送面数据命令的唯一消费者)→ send 终轮把
    /// send_out/to_send 新帧写上 wire。
    fn drive_send_workers<S>(
        send: &mut SendHalf<S>,
        workers: &mut [ShardWorker],
        send_workers: &mut [SendWorker],
        cx: &mut Context<'_>,
    ) -> Poll<Result<()>>
    where
        S: AsyncRead + AsyncWrite + Unpin + crate::Writev2,
    {
        match send.poll(cx) {
            Poll::Pending => {}
            other => return other,
        }
        for w in workers.iter_mut() {
            let _ = w.poll_unpin(cx);
        }
        for w in send_workers.iter_mut() {
            let _ = w.poll_unpin(cx);
        }
        send.poll(cx)
    }

    /// (红→绿)unsplit API:lazy 流首写前 close,线上必须是 Data len0 的
    /// SYN|FIN 合帧(标准 yamux wire 允许),不得只发 FIN。
    #[test]
    fn close_before_write_wire_carries_syn_fin() {
        let (socket, st) = TestSocket::new();
        let mut conn = Connection::new(socket, Config::default(), Mode::Client);
        let mut stream = futures::executor::block_on(poll_fn(|cx| conn.poll_new_outbound(cx)))
            .expect("open outbound stream");

        let mut cx = noop_cx();
        match Pin::new(&mut stream).poll_close(&mut cx) {
            Poll::Ready(Ok(())) => {}
            other => panic!("close must be ready, got {other:?}"),
        }
        drive_to_pending(&mut conn, &mut cx);

        let hdrs = stream1_data_headers(&st);
        assert!(!hdrs.is_empty(), "close frame must reach the wire");
        let h = &hdrs[0];
        assert_eq!(h.len().val(), 0, "close-before-write frame must be empty");
        assert!(
            h.flags().contains(header::SYN),
            "lazy stream close must carry SYN (SYN|FIN), got {:?}",
            h.flags()
        );
        assert!(
            h.flags().contains(header::FIN),
            "close must carry FIN, got {:?}",
            h.flags()
        );
    }

    /// (红→绿)split API:写半 poll_close_half 首写前关闭,同样必须
    /// SYN|FIN 合帧。
    #[test]
    fn split_close_half_before_write_wire_carries_syn_fin() {
        let (socket, st) = TestSocket::new();
        let mut conn = Connection::new(socket, Config::default(), Mode::Client);
        let stream = futures::executor::block_on(poll_fn(|cx| conn.poll_new_outbound(cx)))
            .expect("open outbound stream");

        let (_read_half, mut write_half) = stream.into_halves();
        let mut cx = noop_cx();
        match write_half.poll_close_half(&mut cx) {
            Poll::Ready(Ok(())) => {}
            other => panic!("close half must be ready, got {other:?}"),
        }
        drive_to_pending(&mut conn, &mut cx);

        let hdrs = stream1_data_headers(&st);
        assert!(!hdrs.is_empty(), "close frame must reach the wire");
        let h = &hdrs[0];
        assert_eq!(h.len().val(), 0);
        assert!(
            h.flags().contains(header::SYN) && h.flags().contains(header::FIN),
            "split close-before-write must be SYN|FIN, got {:?}",
            h.flags()
        );
    }

    /// (红→绿)重复 close 幂等:两次 poll_close 只允许一个 FIN 上线。
    #[test]
    fn repeated_close_sends_single_fin() {
        let (socket, st) = TestSocket::new();
        let mut conn = Connection::new(socket, Config::default(), Mode::Client);
        let mut stream = futures::executor::block_on(poll_fn(|cx| conn.poll_new_outbound(cx)))
            .expect("open outbound stream");

        let mut cx = noop_cx();
        for i in 0..2 {
            match Pin::new(&mut stream).poll_close(&mut cx) {
                Poll::Ready(Ok(())) => {}
                other => panic!("close #{i} must be ready, got {other:?}"),
            }
        }
        drive_to_pending(&mut conn, &mut cx);

        let hdrs = stream1_data_headers(&st);
        let fins = hdrs
            .iter()
            .filter(|h| h.flags().contains(header::FIN))
            .count();
        assert_eq!(
            fins, 1,
            "repeated close must be idempotent, headers: {hdrs:?}"
        );
        // SYN 也只能出现一次
        let syns = hdrs
            .iter()
            .filter(|h| h.flags().contains(header::SYN))
            .count();
        assert_eq!(syns, 1, "SYN must appear exactly once, headers: {hdrs:?}");
    }

    /// (锁,修复前即绿)空 write 先行:SYN 由空数据帧携带(Data len0+SYN),
    /// 随后 close 只补 FIN——SYN 恰好一次、不重复开流。
    #[test]
    fn empty_write_then_close_single_syn() {
        let (socket, st) = TestSocket::new();
        let mut conn = Connection::new(socket, Config::default(), Mode::Client);
        let mut stream = futures::executor::block_on(poll_fn(|cx| conn.poll_new_outbound(cx)))
            .expect("open outbound stream");

        let mut cx = noop_cx();
        match Pin::new(&mut stream).poll_write(&mut cx, &[]) {
            Poll::Ready(Ok(0)) => {}
            other => panic!("empty write must return Ok(0), got {other:?}"),
        }
        match Pin::new(&mut stream).poll_close(&mut cx) {
            Poll::Ready(Ok(())) => {}
            other => panic!("close must be ready, got {other:?}"),
        }
        drive_to_pending(&mut conn, &mut cx);

        let hdrs = stream1_data_headers(&st);
        assert_eq!(
            hdrs.len(),
            2,
            "empty write + close = 2 frames, got {hdrs:?}"
        );
        assert!(
            hdrs[0].flags().contains(header::SYN) && !hdrs[0].flags().contains(header::FIN),
            "empty write carries SYN only, got {:?}",
            hdrs[0].flags()
        );
        assert!(
            hdrs[1].flags().contains(header::FIN) && !hdrs[1].flags().contains(header::SYN),
            "close after empty write carries FIN only, got {:?}",
            hdrs[1].flags()
        );
        let syns = hdrs
            .iter()
            .filter(|h| h.flags().contains(header::SYN))
            .count();
        assert_eq!(syns, 1);
    }

    /// (红→绿)真实对端回环:client close-before-write 的线上字节喂给
    /// server 连接,必须产出入站流且立即可读 EOF(半关闭),而非被
    /// 当作未知流丢弃(accept 永不就绪的主代理复现形态)。
    #[test]
    fn close_before_write_bytes_open_stream_at_peer() {
        let (socket, st) = TestSocket::new();
        let mut conn = Connection::new(socket, Config::default(), Mode::Client);
        let mut stream = futures::executor::block_on(poll_fn(|cx| conn.poll_new_outbound(cx)))
            .expect("open outbound stream");

        let mut cx = noop_cx();
        match Pin::new(&mut stream).poll_close(&mut cx) {
            Poll::Ready(Ok(())) => {}
            other => panic!("close must be ready, got {other:?}"),
        }
        drive_to_pending(&mut conn, &mut cx);
        let bytes = st.borrow().written.lock().unwrap().clone();
        assert!(!bytes.is_empty(), "close bytes must be on the wire");

        let (srv_socket, srv_st) = TestSocket::new();
        srv_st
            .borrow_mut()
            .read_blocks
            .lock()
            .unwrap()
            .push_back(bytes);
        srv_st.borrow().eof.store(true, Ordering::SeqCst);
        let mut srv = Connection::new(srv_socket, Config::default(), Mode::Server);

        let inbound = match poll_fn(|cx| srv.poll_next_inbound(cx)).poll_unpin(&mut cx) {
            Poll::Ready(Some(Ok(s))) => s,
            other => panic!("peer must accept the SYN|FIN stream, got {other:?}"),
        };
        let mut inbound = inbound;
        let mut buf = [0u8; 8];
        match poll_fn(|cx| Pin::new(&mut inbound).poll_read(cx, &mut buf)).poll_unpin(&mut cx) {
            Poll::Ready(Ok(0)) => {}
            other => panic!("peer must see immediate EOF, got {other:?}"),
        }
    }

    // ===== Round-A 公平调度守卫:写优先有界化(先红后绿) =====

    /// T1(红→绿):16 条 outbound 流 × 8 × 4096B 的写洪峰中,已就绪的
    /// 入站 SYN|DATA 必须在有界载体写(≤ 预算+2)内被服务并交付新流
    /// (修前恒 ~129 次载体写入后读才被服务,读饿死)。
    #[test]
    fn fair_inbound_stream_serviced_under_write_storm() {
        let (socket, st) = TestSocket::new();
        let mut conn = Connection::new(socket, Config::default(), Mode::Client);

        // 16 条 outbound 流各 queue 8 次 4096B 写:单流 32KiB ≤ 256KiB 信用、
        // 8 条命令 < 通道容量(buffer 10 + 2 发送句柄),poll_write 全 Ready
        let mut cx = noop_cx();
        let payload = [7u8; 4096];
        let mut streams = Vec::new();
        for _ in 0..16 {
            let mut s = futures::executor::block_on(poll_fn(|cx| conn.poll_new_outbound(cx)))
                .expect("open outbound stream");
            for _ in 0..8 {
                match Pin::new(&mut s).poll_write(&mut cx, &payload) {
                    Poll::Ready(Ok(4096)) => {}
                    other => panic!("storm write must be ready, got {other:?}"),
                }
            }
            streams.push(s);
        }

        // 入站 SYN|DATA 在驱动前已就绪
        st.borrow_mut()
            .read_blocks
            .lock()
            .unwrap()
            .push_back(syn_data_block(2, b"x"));

        let inbound = match futures::executor::block_on(poll_fn(|cx| conn.poll_next_inbound(cx))) {
            Some(Ok(s)) => s,
            other => panic!("inbound stream must be serviced under write storm, got {other:?}"),
        };
        assert_eq!(inbound.id(), StreamId::new(2));

        let calls = st.borrow().write_calls.load(Ordering::SeqCst);
        assert!(
            calls <= FAIRNESS_MAX_WRITE_OPS_BETWEEN_READ_PROBES + 2,
            "first inbound service must happen within bounded carrier writes, got {calls}"
        );
    }

    /// T2(锁现有为):≥100 个入站数据块的读洪峰中,已 queue 的出站 Data 帧
    /// 仍须在 read_blocks 未耗尽前上 wire——读侧持续就绪不得饿死待写
    /// (公平调度只给写侧加预算,不引入反向偏置)。
    #[test]
    fn fair_pending_write_progresses_under_read_storm() {
        let (socket, st) = TestSocket::new();
        let mut conn = Connection::new(socket, Config::default(), Mode::Client);

        let mut stream = futures::executor::block_on(poll_fn(|cx| conn.poll_new_outbound(cx)))
            .expect("open outbound stream");
        let mut cx = noop_cx();
        match Pin::new(&mut stream).poll_write(&mut cx, b"outbound") {
            Poll::Ready(Ok(8)) => {}
            other => panic!("queue data frame, got {other:?}"),
        }

        // 读洪峰:50 块丢弃数据 → SYN|DATA 检查点(驱动在此返回新流)→ 50 块剩余
        {
            let st_mut = st.borrow_mut();
            let mut blocks = st_mut.read_blocks.lock().unwrap();
            for _ in 0..50 {
                blocks.push_back(data_block(6, b"storm"));
            }
            blocks.push_back(syn_data_block(4, b"probe"));
            for _ in 0..50 {
                blocks.push_back(data_block(6, b"storm"));
            }
        }

        let inbound = match futures::executor::block_on(poll_fn(|cx| conn.poll_next_inbound(cx))) {
            Some(Ok(s)) => s,
            other => panic!("checkpoint inbound stream expected, got {other:?}"),
        };
        assert_eq!(inbound.id(), StreamId::new(4));

        // 检查点返回时读洪峰未耗尽(还有整整 50 块未读)
        let remaining = st.borrow().read_blocks.lock().unwrap().len();
        assert!(
            remaining >= 50,
            "read storm must not be exhausted at checkpoint, {remaining} blocks left"
        );

        // 出站 Data 帧已在检查点之前上 wire
        let hdrs = stream1_data_headers(&st);
        assert!(
            !hdrs.is_empty(),
            "outbound data frame must reach wire during read storm"
        );
    }

    /// T3(红→绿):outbound 流 1 写到信用耗尽(pending)后,其他流写洪峰中
    /// 预置的对 流 1 窗口更新必须在 ≤ 预算+2 次载体写内唤醒流 1 的 writer
    /// (WU 处理时延有界;修前须等全部 120 帧洪峰写完才读到 WU)。
    #[test]
    fn fair_wu_reaches_blocked_writer_under_write_storm() {
        let (socket, st) = TestSocket::new();
        let mut conn = Connection::new(socket, Config::default(), Mode::Client);

        let mut streams = Vec::new();
        for _ in 0..16 {
            let s = futures::executor::block_on(poll_fn(|cx| conn.poll_new_outbound(cx)))
                .expect("open outbound stream");
            streams.push(s);
        }
        let mut blocked = streams.remove(0); // 流 1

        // 流 1 写到信用耗尽:256KiB / 4KiB = 64 次。通道容量为
        // buffer(`STREAM_COMMAND_CAP`)+2 发送句柄,每 8 次驱动一次连接清空
        // 队列,保证后续 Pending 只可能因信用耗尽(而非通道满)。
        let mut cx = noop_cx();
        let payload = [7u8; 4096];
        let mut total = 0usize;
        loop {
            match Pin::new(&mut blocked).poll_write(&mut cx, &payload) {
                Poll::Ready(Ok(4096)) => {
                    total += 4096;
                    if total.is_multiple_of(4096 * 8) {
                        drive_to_pending(&mut conn, &mut cx);
                    }
                }
                Poll::Pending => break,
                other => panic!("unexpected stream-1 write: {other:?}"),
            }
        }
        assert_eq!(
            total, DEFAULT_CREDIT as usize,
            "stream 1 must block on exhausted credit, not channel capacity"
        );

        // 用「唤醒时刻载体写次数」快照 waker 重新挂起流 1 的写
        let writes = st.borrow().write_calls.clone();
        let rec = Arc::new(WakeAtSnapshot {
            writes: writes.clone(),
            wake_at: Mutex::new(None),
        });
        let w = futures::task::waker(rec.clone());
        let mut block_cx = Context::from_waker(&w);
        match Pin::new(&mut blocked).poll_write(&mut block_cx, &payload) {
            Poll::Pending => {}
            other => panic!("stream 1 must stay credit-blocked, got {other:?}"),
        }

        // 其他 15 条流写洪峰(15 × 8 × 4096B = 120 帧)
        for s in streams.iter_mut() {
            for _ in 0..8 {
                match Pin::new(&mut *s).poll_write(&mut cx, &payload) {
                    Poll::Ready(Ok(4096)) => {}
                    other => panic!("storm write must be ready, got {other:?}"),
                }
            }
        }

        // 洪峰中预置对 流 1 的窗口更新(授 4096 信用)
        st.borrow_mut()
            .read_blocks
            .lock()
            .unwrap()
            .push_back(wu_credit_block(1, 4096));

        let before = writes.load(Ordering::SeqCst);
        drive_to_pending(&mut conn, &mut cx);

        // WU 必须在有界载体写内被处理(唤醒快照与洪峰起点之差 ≤ 预算+2)
        let wake_at = rec
            .wake_at
            .lock()
            .unwrap()
            .expect("stream-1 writer must be woken by the window update");
        assert!(
            wake_at - before <= FAIRNESS_MAX_WRITE_OPS_BETWEEN_READ_PROBES + 2,
            "WU must be processed within bounded carrier writes, woke at {wake_at} (+{})",
            wake_at - before
        );

        // 解锁后可继续写(授信已生效)
        match Pin::new(&mut blocked).poll_write(&mut cx, &payload) {
            Poll::Ready(Ok(4096)) => {}
            other => panic!("stream 1 must be writable again after WU, got {other:?}"),
        }
    }

    // ===== Round-C 公平调度守卫:探测批量排空(先红后绿) =====

    /// T5(红→绿):强制读探测必须**批量排空**入站——同一探测内连续消费
    /// 多个已就绪入站帧(对称操作预算,单次探测 ≤16 帧),而不是每探测
    /// 仅 1 帧即回写路径(×16 写洪峰下读侧成批消化能力被稀释)。修前
    /// 事件序列中 'r' 的连续运行恒为 1。
    #[test]
    fn fair_probe_drains_inbound_batch() {
        let (socket, st) = TestSocket::new();
        let mut conn = Connection::new(socket, Config::default(), Mode::Client);

        // 与 T1 同构:16 条 outbound 流各 queue 8 × 4096B(单流 32KiB ≤ 256KiB
        // 信用、8 条命令 < 通道容量,TestSocket 写侧恒 Ready → 写洪峰持续就绪)
        let mut cx = noop_cx();
        let payload = [7u8; 4096];
        let mut streams = Vec::new();
        for _ in 0..16 {
            let mut s = futures::executor::block_on(poll_fn(|cx| conn.poll_new_outbound(cx)))
                .expect("open outbound stream");
            for _ in 0..8 {
                match Pin::new(&mut s).poll_write(&mut cx, &payload) {
                    Poll::Ready(Ok(4096)) => {}
                    other => panic!("storm write must be ready, got {other:?}"),
                }
            }
            streams.push(s);
        }

        // 读侧预置 5 块:前置 4 个纯数据块(目标流 2 未知,被静默丢弃但
        // 照常消费,保证批量运行可观测),syn_data_block 放最后——NewStream
        // 交付即返回,不打断前面的批量运行
        {
            let st_mut = st.borrow_mut();
            let mut blocks = st_mut.read_blocks.lock().unwrap();
            for _ in 0..4 {
                blocks.push_back(data_block(2, &payload));
            }
            blocks.push_back(syn_data_block(2, b"syn"));
        }

        let inbound = match futures::executor::block_on(poll_fn(|cx| conn.poll_next_inbound(cx))) {
            Some(Ok(s)) => s,
            other => panic!("batch-draining probe must deliver the inbound stream, got {other:?}"),
        };
        assert_eq!(inbound.id(), StreamId::new(2));

        // 冲掉剩余写洪峰,事件序列覆盖完整(流存活到断言之后,避免 drop
        // 触发 RST 污染观察)
        drive_to_pending(&mut conn, &mut cx);

        let events = st.borrow().events.lock().unwrap().clone();
        // 连续 'r' 运行长度序列
        let mut runs = Vec::new();
        let mut cur = 0usize;
        for &e in &events {
            if e == 'r' {
                cur += 1;
            } else if cur > 0 {
                runs.push(cur);
                cur = 0;
            }
        }
        if cur > 0 {
            runs.push(cur);
        }

        assert!(
            runs.iter().any(|&r| r >= 3),
            "probe must drain inbound in batches (some 'r' run >= 3), events: {events:?}, runs: {runs:?}"
        );
        assert!(
            runs.iter().all(|&r| r <= FAIRNESS_MAX_WRITE_OPS_BETWEEN_READ_PROBES),
            "probe batch must respect the symmetric op budget (run <= 16), events: {events:?}, runs: {runs:?}"
        );
        let writes = events.iter().filter(|&&e| e == 'w').count();
        assert!(
            writes >= 100,
            "write storm must actually happen, got {writes} 'w' events, events: {events:?}"
        );
        assert!(
            st.borrow().read_blocks.lock().unwrap().is_empty(),
            "all 5 read blocks must be consumed"
        );
    }

    // ===== Round-B1 建流即授信守卫(对齐 frp OpenStream/AcceptStream 时机) =====

    /// G1(红→绿):outbound 建流即授信——`poll_new_outbound` 开流后不写任何
    /// 数据,drive 后 wire 上流 1 的首个帧必须是 WindowUpdate 且携带 SYN
    /// (SYN|WU 合帧,即 frp OpenStream 的 initialStreamFlags 行为),
    /// len == 6MiB − 256KiB(预授差额);该帧之前不得有流 1 的其他帧
    /// (过滤 stream id 0 的 RTT ping)。修前:SYN 为 lazy(随首数据帧),
    /// 建流后 wire 上没有任何流 1 的帧。
    #[test]
    fn creation_grant_outbound_syn_wu_on_open() {
        let (socket, st) = TestSocket::new();
        let mut cfg = Config::default();
        cfg.set_initial_stream_receive_window(6 * 1024 * 1024);
        let mut conn = Connection::new(socket, cfg, Mode::Client);

        // 存活到断言之后:避免 drop 触发 RST 污染 wire 观察
        let _stream = futures::executor::block_on(poll_fn(|cx| conn.poll_new_outbound(cx)))
            .expect("open outbound stream");

        let mut cx = noop_cx();
        drive_to_pending(&mut conn, &mut cx);

        // 只看流 1 的帧(过滤 stream id 0 的 RTT ping)
        let hdrs: Vec<_> = written_headers(&st)
            .into_iter()
            .filter(|h| h.stream_id() == StreamId::new(1))
            .collect();
        assert!(
            !hdrs.is_empty(),
            "creation grant must reach the wire without any write"
        );
        let h = &hdrs[0];
        assert_eq!(
            h.tag(),
            Tag::WindowUpdate,
            "first stream-1 frame must be a window update, got {h:?}"
        );
        assert!(
            h.flags().contains(header::SYN),
            "creation grant must carry SYN (SYN|WU), got {:?}",
            h.flags()
        );
        assert_eq!(
            h.len().val(),
            6 * 1024 * 1024 - DEFAULT_CREDIT,
            "grant size must be the pregrant delta 6MiB − 256KiB"
        );
    }

    /// G2(红→绿):inbound 建流即授信——SYN|DATA 交付流后 `into_halves`,
    /// 再 drive,wire 上流 2 恰有**一个** WU 帧:携带 ACK 且
    /// len == 6MiB − 256KiB。授信帧已在构造时排队并消费 ACK flag,
    /// `into_halves` 不得再补发 ACK|WU(0) 第二帧。修前:仅有
    /// `into_halves` 的 ACK|WU(0),len 为 0。
    #[test]
    fn creation_grant_inbound_ack_wu_no_duplicate() {
        let (socket, st) = TestSocket::new();
        st.borrow_mut()
            .read_blocks
            .lock()
            .unwrap()
            .push_back(syn_data_block(2, b"x"));
        let mut cfg = Config::default();
        cfg.set_initial_stream_receive_window(6 * 1024 * 1024);
        let mut conn = Connection::new(socket, cfg, Mode::Client);

        let stream = futures::executor::block_on(poll_fn(|cx| conn.poll_next_inbound(cx)))
            .expect("inbound stream")
            .expect("ok stream");
        // 半壳存活到断言之后:避免 drop 触发 RST 污染 wire 观察
        let (_read_half, _write_half) = stream.into_halves();

        let mut cx = noop_cx();
        drive_to_pending(&mut conn, &mut cx);

        let wus: Vec<_> = written_headers(&st)
            .into_iter()
            .filter(|h| h.tag() == Tag::WindowUpdate && h.stream_id() == StreamId::new(2))
            .collect();
        assert_eq!(
            wus.len(),
            1,
            "exactly one WU frame for stream 2 (no duplicate ACK|WU(0)), got {wus:?}"
        );
        let h = &wus[0];
        assert!(
            h.flags().contains(header::ACK),
            "creation grant must carry ACK (ACK|WU), got {:?}",
            h.flags()
        );
        assert_eq!(
            h.len().val(),
            6 * 1024 * 1024 - DEFAULT_CREDIT,
            "grant size must be the pregrant delta 6MiB − 256KiB, got {}",
            h.len().val()
        );
    }

    // round-E1:原 creation_grant_budget_exhausted_no_frame(预算归 0 时
    // 预授信自然回退、无 WU 帧)随 256MiB 共享预算一并删除——恒定窗
    // 语义下建流授信不再有预算回退路径(见 flow_control.rs)。

    // ===== Round-D 拆分守卫:收/发双半(先红后绿) =====
    // 形态:`Connection::into_halves() -> (RecvHalf<T>, SendHalf<T>)`;
    // `RecvHalf::poll -> Poll<Option<Result<Stream>>>`;
    // `SendHalf::poll -> Poll<Result<()>>`、`SendHalf::poll_new_outbound`。

    /// S1(红→绿):16 流 ×8×4096B 写命令已入队 + 入站 SYN|DATA 预置时,
    /// 仅驱动接收侧(recv+Worker)即可交付入站流——读推进不依赖写侧
    /// 让位(未拆分路径须等写预算耗尽后的强制读探测)。断言交付发生在
    /// 载体尚未写完风暴帧(written < 风暴总量)之时。round-5 起分片:
    /// SYN 经 Framer 路由进 Worker 建流、经 new_streams 回流交付。
    #[test]
    fn split_recv_delivers_while_send_commands_pending() {
        let (socket, st) = TestSocket::new();
        let conn = Connection::new(socket, Config::default(), Mode::Client);
        let (mut recv_half, mut send_half, mut workers, mut send_workers) = conn.into_halves();

        let mut cx = noop_cx();
        let payload = [7u8; 4096];
        let mut streams = Vec::new();
        for _ in 0..16 {
            let mut s = match send_half.poll_new_outbound(&mut cx) {
                Poll::Ready(Ok(s)) => s,
                other => panic!("open outbound via send half, got {other:?}"),
            };
            for _ in 0..8 {
                match Pin::new(&mut s).poll_write(&mut cx, &payload) {
                    Poll::Ready(Ok(4096)) => {}
                    other => panic!("storm write must be ready, got {other:?}"),
                }
            }
            streams.push(s);
        }

        st.borrow_mut()
            .read_blocks
            .lock()
            .unwrap()
            .push_back(syn_data_block(2, b"x"));

        let inbound =
            match drive_recv_workers(&mut recv_half, &mut workers, &mut send_workers, &mut cx) {
                Poll::Ready(Some(Ok(s))) => s,
                other => {
                    panic!(
                        "recv half must deliver inbound without send-side progress, got {other:?}"
                    )
                }
            };
        assert_eq!(inbound.id(), StreamId::new(2));

        let storm_total = 16 * 8 * (4096 + header::HEADER_SIZE);
        let written = st.borrow().written.lock().unwrap().len();
        assert!(
            written < storm_total,
            "inbound delivery must precede the storm drain on the wire, wrote {written}"
        );
    }

    /// S2(红→绿):G1 等价——SendHalf 开流后驱动发送半,wire 上流 1 首帧
    /// = SYN|WU(6MiB−256KiB),建流即授信在拆分路径照旧(规格要点 5)。
    #[test]
    fn split_creation_grant_syn_wu() {
        let (socket, st) = TestSocket::new();
        let mut cfg = Config::default();
        cfg.set_initial_stream_receive_window(6 * 1024 * 1024);
        let conn = Connection::new(socket, cfg, Mode::Client);
        let (_recv_half, mut send_half, mut workers, mut send_workers) = conn.into_halves();

        let mut cx = noop_cx();
        let _stream = match send_half.poll_new_outbound(&mut cx) {
            Poll::Ready(Ok(s)) => s,
            other => panic!("open outbound via send half, got {other:?}"),
        };
        match drive_send_workers(&mut send_half, &mut workers, &mut send_workers, &mut cx) {
            Poll::Pending => {}
            other => panic!("send half must park after flushing the grant, got {other:?}"),
        }

        let hdrs: Vec<_> = written_headers(&st)
            .into_iter()
            .filter(|h| h.stream_id() == StreamId::new(1))
            .collect();
        assert!(
            !hdrs.is_empty(),
            "creation grant must reach the wire without any write"
        );
        let h = &hdrs[0];
        assert_eq!(
            h.tag(),
            Tag::WindowUpdate,
            "first stream-1 frame must be a window update, got {h:?}"
        );
        assert!(
            h.flags().contains(header::SYN),
            "creation grant must carry SYN (SYN|WU), got {:?}",
            h.flags()
        );
        assert_eq!(
            h.len().val(),
            6 * 1024 * 1024 - DEFAULT_CREDIT,
            "grant size must be the pregrant delta 6MiB − 256KiB"
        );
    }

    /// S3(红→绿):读侧 EOF → 共享错误;SendHalf 再 poll 立即错误返回,
    /// 不再产出新写(规格要点 1 的 EOF 方向)。
    #[test]
    fn split_recv_eof_stops_sender() {
        let (socket, st) = TestSocket::new();
        let conn = Connection::new(socket, Config::default(), Mode::Client);
        let (mut recv_half, mut send_half, mut workers, mut send_workers) = conn.into_halves();

        let mut cx = noop_cx();
        let mut stream = match send_half.poll_new_outbound(&mut cx) {
            Poll::Ready(Ok(s)) => s,
            other => panic!("open outbound via send half, got {other:?}"),
        };
        match Pin::new(&mut stream).poll_write(&mut cx, b"payload") {
            Poll::Ready(Ok(7)) => {}
            other => panic!("queue data frame, got {other:?}"),
        }

        st.borrow().eof.store(true, Ordering::SeqCst);
        match drive_recv_workers(&mut recv_half, &mut workers, &mut send_workers, &mut cx) {
            Poll::Ready(None) => {}
            other => panic!("recv half must observe EOF, got {other:?}"),
        }

        let before = st.borrow().write_calls.load(Ordering::SeqCst);
        match poll_fn(|cx| send_half.poll(cx)).poll_unpin(&mut cx) {
            Poll::Ready(Err(e)) => {
                assert!(
                    matches!(e, ConnectionError::Closed),
                    "EOF must surface as Closed, got {e:?}"
                );
            }
            other => panic!("send half must fail fast after recv EOF, got {other:?}"),
        }
        assert_eq!(
            st.borrow().write_calls.load(Ordering::SeqCst),
            before,
            "no carrier writes after recv-side EOF"
        );
    }

    /// S4(红→绿):SendHalf 开流 → 写载荷 → 双半+Worker 驱动 → 从该流
    /// 读到载荷(单连接自环,验证派发入 staging 全链)。自环回喂剥除
    /// SYN 的流 1 Data 帧(带 SYN 会被 is_valid_remote_id 判非法——本端
    /// 流 id);round-5 起该帧由 Worker 从 opening 收编后入缓冲。
    #[test]
    fn split_open_write_read_roundtrip() {
        let (socket, st) = TestSocket::new();
        let conn = Connection::new(socket, Config::default(), Mode::Client);
        let (mut recv_half, mut send_half, mut workers, mut send_workers) = conn.into_halves();

        let mut cx = noop_cx();
        let mut stream = match send_half.poll_new_outbound(&mut cx) {
            Poll::Ready(Ok(s)) => s,
            other => panic!("open outbound via send half, got {other:?}"),
        };
        match Pin::new(&mut stream).poll_write(&mut cx, b"roundtrip") {
            Poll::Ready(Ok(9)) => {}
            other => panic!("queue data frame, got {other:?}"),
        }
        match drive_send_workers(&mut send_half, &mut workers, &mut send_workers, &mut cx) {
            Poll::Pending => {}
            other => panic!("send half must park, got {other:?}"),
        }

        // 自环:把已写的流 1 Data 帧剥除 SYN 后回喂读侧
        {
            let bytes = st.borrow().written.lock().unwrap().clone();
            let st_mut = st.borrow_mut();
            let mut blocks = st_mut.read_blocks.lock().unwrap();
            let mut off = 0;
            while off + header::HEADER_SIZE <= bytes.len() {
                let block: [u8; header::HEADER_SIZE] =
                    bytes[off..off + header::HEADER_SIZE].try_into().unwrap();
                let hdr = header::decode(&block).expect("written bytes must be valid frames");
                let body_end = off + header::HEADER_SIZE + hdr.len().val() as usize;
                if hdr.stream_id() == StreamId::new(1) && hdr.tag() == Tag::Data {
                    let clean = header::Header::data(StreamId::new(1), hdr.len().val());
                    let mut blk = header::encode(&clean).to_vec();
                    blk.extend_from_slice(&bytes[off + header::HEADER_SIZE..body_end]);
                    blocks.push_back(blk);
                }
                off = body_end;
            }
        }

        match drive_recv_workers(&mut recv_half, &mut workers, &mut send_workers, &mut cx) {
            Poll::Pending => {}
            other => panic!("recv half must park after dispatch, got {other:?}"),
        }

        let mut buf = [0u8; 16];
        let n = match Pin::new(&mut stream).poll_read(&mut cx, &mut buf) {
            Poll::Ready(Ok(n)) => n,
            other => panic!("roundtrip payload must be readable from the stream, got {other:?}"),
        };
        assert_eq!(&buf[..n], b"roundtrip");
    }

    /// S5(红→绿):接收半不 poll(模拟 accept 背压满,Worker 也不建流),
    /// 发送半仍能持续写出(载体 written 增长)——接收停摆不再拖死发送
    /// (旧单任务 driver 的结构问题)。
    #[test]
    fn split_accept_backpressure_does_not_stall_sender() {
        let (socket, st) = TestSocket::new();
        let conn = Connection::new(socket, Config::default(), Mode::Client);
        let (recv_half, mut send_half, _workers, mut send_workers) = conn.into_halves();

        let mut cx = noop_cx();
        let mut stream = match send_half.poll_new_outbound(&mut cx) {
            Poll::Ready(Ok(s)) => s,
            other => panic!("open outbound via send half, got {other:?}"),
        };
        let payload = [7u8; 4096];
        for _ in 0..8 {
            match Pin::new(&mut stream).poll_write(&mut cx, &payload) {
                Poll::Ready(Ok(4096)) => {}
                other => panic!("storm write must be ready, got {other:?}"),
            }
        }
        // 预置未读的入站块(接收半全程不 poll)
        st.borrow_mut()
            .read_blocks
            .lock()
            .unwrap()
            .push_back(syn_data_block(2, b"x"));

        let before = st.borrow().written.lock().unwrap().len();
        // round-6:流命令由 SendWorker 消费(注册路由 D4),驱动面必须
        // 覆盖之;接收半照旧全程不 poll
        for w in send_workers.iter_mut() {
            let _ = w.poll_unpin(&mut cx);
        }
        match poll_fn(|cx| send_half.poll(cx)).poll_unpin(&mut cx) {
            Poll::Pending => {}
            other => panic!("send half must park after draining commands, got {other:?}"),
        }
        let after = st.borrow().written.lock().unwrap().len();
        assert!(
            after - before >= 8 * (4096 + header::HEADER_SIZE),
            "sender must keep writing while the receiver stalls, delta {}",
            after - before
        );
        assert!(
            !st.borrow().read_blocks.lock().unwrap().is_empty(),
            "receiver side must stay untouched"
        );
        drop(recv_half); // 收尾:接收半弃置不影响已完成观察
    }

    // ===== Round-D2 守卫:SplitSocket 锁丢失唤醒修复(先红后绿) =====

    /// T6 专用载体:TestSocket 同款脚本行为 + 一次性重入钩子。首次
    /// `poll_write2` 执行时写半已持有 `SplitSocket` 的底层锁(正处于载体
    /// 临界区内),钩子先以读半的 waker 同步重入 poll 一次读半
    /// `poll_read`(读侧空 → 必然 Pending 在取锁上),再执行本侧写——
    /// 把真实场景中收/发双任务并行的临界区交错(竞态 ~50%)变成确定性
    /// 必现,无需真实线程。
    struct T6Socket {
        base: TestSocket,
        hook: Rc<RefCell<T6Hook>>,
    }

    struct T6Hook {
        armed: bool,
        reader: Option<halves::SplitSocket<T6Socket>>,
        reader_waker: Option<Waker>,
        nested_pending: Option<bool>,
    }

    impl crate::Writev2 for T6Socket {
        fn poll_write2(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            a: &[u8],
            b: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            let this = self.get_mut();
            {
                let mut hook = this.hook.borrow_mut();
                if hook.armed {
                    hook.armed = false;
                    let mut reader = hook.reader.take().expect("T6 reader half installed");
                    let waker = hook.reader_waker.take().expect("T6 reader waker installed");
                    drop(hook); // 重入 poll 期间不持有 hook 借用
                    let mut cx1 = Context::from_waker(&waker);
                    let mut buf = [0u8; 8];
                    let nested = Pin::new(&mut reader).poll_read(&mut cx1, &mut buf);
                    let mut hook = this.hook.borrow_mut();
                    hook.nested_pending = Some(matches!(nested, Poll::Pending));
                    hook.reader = Some(reader);
                }
            }
            crate::Writev2::poll_write2(Pin::new(&mut this.base), cx, a, b)
        }
    }

    impl futures::io::AsyncRead for T6Socket {
        fn poll_read(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut [u8],
        ) -> Poll<std::io::Result<usize>> {
            let this = self.get_mut();
            futures::io::AsyncRead::poll_read(Pin::new(&mut this.base), cx, buf)
        }
    }

    impl futures::io::AsyncWrite for T6Socket {
        fn poll_write(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            crate::Writev2::poll_write2(self, cx, &[], buf)
        }
        fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            let this = self.get_mut();
            futures::io::AsyncWrite::poll_flush(Pin::new(&mut this.base), cx)
        }
        fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            let this = self.get_mut();
            futures::io::AsyncWrite::poll_close(Pin::new(&mut this.base), cx)
        }
    }

    /// T6(红→绿,确定性):`SplitSocket` 底层锁的等待 waker 必须存于锁
    /// 共享状态——读半在锁被写半持有期间(写半正处于载体临界区)
    /// `poll_read` 返回 Pending 后,写半完成写并释放锁必须唤醒读半的
    /// waker。修前(futures::lock::Mutex):`lock()` 临时 future 随
    /// `poll_read` 返回被 drop,已入队的等待节点连同注册的 waker 一并被
    /// 移出队列,释放时队列已空,w1 永不被唤醒(红;对应真实场景中
    /// ~50% 概率的收侧永久挂起)。(d) 复 poll 读半(预置数据)→ Ready
    /// 读到数据。CountWaker 计数断言,不需要真实线程。
    #[test]
    fn split_lock_pending_reader_woken_by_writer_release() {
        let (base, st) = TestSocket::new();
        let hook = Rc::new(RefCell::new(T6Hook {
            armed: true,
            reader: None,
            reader_waker: None,
            nested_pending: None,
        }));
        let sock = T6Socket {
            base,
            hook: hook.clone(),
        };
        let split = halves::split_socket(sock);
        let mut writer = split.clone();
        hook.borrow_mut().reader = Some(split);

        #[derive(Default)]
        struct CountWaker(AtomicUsize);
        impl futures::task::ArcWake for CountWaker {
            fn wake_by_ref(me: &Arc<Self>) {
                me.0.fetch_add(1, Ordering::SeqCst);
            }
        }
        let w1 = Arc::new(CountWaker::default());
        let w2 = Arc::new(CountWaker::default());
        hook.borrow_mut().reader_waker = Some(futures::task::waker(w1.clone()));

        // (b) 以 w2 poll 写半 poll_write2 小载荷 → Ready(取锁/写/释放)。
        // (a) 嵌套于其临界区内:读侧空 → 读半 poll_read 必 Pending
        //     (此时等待注册)。
        let waker2 = futures::task::waker(w2.clone());
        let mut cx2 = Context::from_waker(&waker2);
        match crate::Writev2::poll_write2(Pin::new(&mut writer), &mut cx2, &[], b"x") {
            Poll::Ready(Ok(n)) => assert_eq!(n, 1),
            other => panic!("(b) writer poll_write2 must be ready, got {other:?}"),
        }
        assert_eq!(
            w2.0.load(Ordering::SeqCst),
            0,
            "writer side is never parked"
        );
        assert_eq!(
            hook.borrow().nested_pending,
            Some(true),
            "(a) reader must pend (on the lock) inside the writer critical section"
        );

        // (c) w1 已被唤醒:写半释放锁时必须唤醒等待中的读半 waker
        //     (修前:Mutex 临时 future drop 丢节点,w1 永不被唤醒,红)。
        assert!(
            w1.0.load(Ordering::SeqCst) >= 1,
            "(c) pending reader's waker must be woken by the writer's lock release"
        );

        // (d) 复 poll 读半(预置数据)→ Ready 读到数据。
        st.borrow_mut()
            .read_blocks
            .lock()
            .unwrap()
            .push_back(b"y".to_vec());
        let mut reader = hook
            .borrow_mut()
            .reader
            .take()
            .expect("reader half parked back in the hook");
        let waker1 = futures::task::waker(w1.clone());
        let mut cx1 = Context::from_waker(&waker1);
        let mut buf = [0u8; 8];
        match Pin::new(&mut reader).poll_read(&mut cx1, &mut buf) {
            Poll::Ready(Ok(n)) => assert_eq!(&buf[..n], b"y"),
            other => panic!("(d) reader must read preset data, got {other:?}"),
        }
    }

    // ===== Round-D3 守卫:SendHalf park 复查自旋修复(先红后绿) =====

    /// T7 专用载体(工作线程模式):写侧固定小容量(耗尽 → Pending 并
    /// 登记写 waker;`poison` 置位后返回 Err,让看门狗超时后的自旋线程
    /// 有界退出,不烧核到进程结束),读侧按脚本块返回、耗尽 Pending。
    /// 状态经 `Arc<std::sync::Mutex>` 持有:`Send` 绑定,poll 可移入工作
    /// 线程执行。
    struct SpinSocketState {
        cap: usize,
        written: Vec<u8>,
        write_waker: Option<Waker>,
        read_blocks: std::collections::VecDeque<Vec<u8>>,
        cur: Vec<u8>,
        cur_off: usize,
        poison: bool,
    }

    struct SpinSocket(Arc<Mutex<SpinSocketState>>);

    impl crate::Writev2 for SpinSocket {
        fn poll_write2(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            a: &[u8],
            b: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            let mut st = self.0.lock().unwrap();
            if st.poison {
                return Poll::Ready(Err(std::io::Error::other("spin-watchdog poison")));
            }
            if st.cap == 0 {
                st.write_waker = Some(cx.waker().clone());
                return Poll::Pending;
            }
            let total = a.len() + b.len();
            let n = total.min(st.cap);
            let (na, nb) = if n <= a.len() {
                (n, 0)
            } else {
                (a.len(), n - a.len())
            };
            st.written.extend_from_slice(&a[..na]);
            st.written.extend_from_slice(&b[..nb]);
            st.cap -= n;
            Poll::Ready(Ok(n))
        }
    }

    impl futures::io::AsyncRead for SpinSocket {
        fn poll_read(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &mut [u8],
        ) -> Poll<std::io::Result<usize>> {
            let mut st = self.0.lock().unwrap();
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
            Poll::Pending
        }
    }

    impl futures::io::AsyncWrite for SpinSocket {
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

    /// T7(红→绿,工作线程 + 超时看门狗):半写帧(poll_ready 恒 Pending)+
    /// to_send 非空(接收侧压入 Pong)时,SendHalf::poll 的 park 复查
    /// 不得把 to_send 当作"无需载体即可推进的工作"——修前
    /// `has_send_work` 命中 to_send 非空 → `continue` 无限自旋,poll 永不
    /// 返回 Pending(三机实测 260 万次迭代、烧 1 核)。看门狗:poll 在
    /// 工作线程执行,主线程 3s 收不到返回即判自旋失败(红)。
    /// 反向断言:写侧恢复可写后(waker 唤醒 + 复 poll)应写穿半写帧并
    /// 把 Pong 发上 wire。
    #[test]
    fn send_park_returns_pending_when_tosend_blocked_by_partial_write() {
        let state = Arc::new(Mutex::new(SpinSocketState {
            cap: 8, // 首帧(12B 帧头)只写穿 8B → 半写帧
            written: Vec::new(),
            write_waker: None,
            read_blocks: std::collections::VecDeque::new(),
            cur: Vec::new(),
            cur_off: 0,
            poison: false,
        }));
        let sock = SpinSocket(state.clone());
        let conn = Connection::new(sock, Config::default(), Mode::Client);
        let (mut recv_half, mut send_half, _workers, _send_workers) = conn.into_halves();

        // (1) 首次驱动发送半:Rtt::new 的 next_ping 立即到期(Round-B 后
        //     next=now),8B 容量只写穿 12B 帧头的前 8B → 半写帧,
        //     此后 poll_ready 恒 Pending(载体写 Pending 登记了写 waker)。
        let mut cx = noop_cx();
        match poll_fn(|cx| send_half.poll(cx)).poll_unpin(&mut cx) {
            Poll::Pending => {}
            other => panic!("send half must park after the partial write, got {other:?}"),
        }
        assert_eq!(
            state.lock().unwrap().written.len(),
            8,
            "exactly 8B of the 12B frame header must be written"
        );

        // (2) 接收半收到连接级 Ping(id=7,无 ACK)→ Pong 压入 to_send;
        //     读侧脚本耗尽 → 接收半 park(对齐线上形态:client 空闲 ping)。
        state
            .lock()
            .unwrap()
            .read_blocks
            .push_back(header::encode(&header::Header::ping(7)).to_vec());
        match poll_fn(|cx| recv_half.poll(cx)).poll_unpin(&mut cx) {
            Poll::Pending => {}
            other => panic!("recv half must park after answering the ping, got {other:?}"),
        }

        // (3) 工作线程 poll 发送半:修前 park 复查命中 to_send 非空 →
        //     无限自旋,poll 永不返回 → 看门狗超时判失败(红);修后
        //     复查只看"无需载体即可推进"项,立即返回 Pending(绿)。
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
                    "send half must return Pending (not spin) while the partial-write \
                     frame blocks the carrier and to_send holds a Pong"
                );
                let mut send_half = worker.join().expect("worker must finish cleanly");

                // (4) 反向断言:写侧恢复可写(唤醒 + 复 poll)→ 半写帧写穿,
                //     Pong 发上 wire,随后重新 park。
                {
                    let mut st = state.lock().unwrap();
                    st.cap = 4096;
                    if let Some(w) = st.write_waker.take() {
                        w.wake(); // EPOLLOUT 就绪唤醒的对齐(此处 noop,复 poll 推进)
                    }
                }
                match poll_fn(|cx| send_half.poll(cx)).poll_unpin(&mut cx) {
                    Poll::Pending => {}
                    other => panic!("send half must re-park after draining, got {other:?}"),
                }
                let written = state.lock().unwrap().written.clone();
                assert_eq!(
                    written.len(),
                    24,
                    "rtt ping (12B) + pong (12B) must be fully written, got {written:?}"
                );
                let h0 = header::decode(&written[0..12].try_into().unwrap())
                    .expect("first frame is the rtt ping");
                assert_eq!(h0.tag(), Tag::Ping, "first frame is the rtt ping");
                let h1 = header::decode(&written[12..24].try_into().unwrap())
                    .expect("second frame is the pong");
                assert_eq!(h1.tag(), Tag::Ping, "second frame is the pong");
                assert!(
                    h1.flags().contains(header::ACK),
                    "pong must carry ACK, got {:?}",
                    h1.flags()
                );
                assert_eq!(
                    h1.len().val(),
                    7,
                    "pong must echo the ping id (opaque id in the length field)"
                );
                drop(recv_half); // 收尾
            }
            Err(_) => {
                // 看门狗超时:poison 让自旋中的 poll_write2 返回 Err,
                // 工作线程有界退出(不烧核到进程结束),然后判失败。
                state.lock().unwrap().poison = true;
                let _ = worker.join();
                panic!(
                    "spin detected: SendHalf::poll did not return within 3s while \
                     to_send holds a Pong blocked by a partial write"
                );
            }
        }
    }

    // ===== Round-D3-2 守卫:park 复查丢唤醒窗口(独立复核修复,先红后绿) =====

    /// T8 专用载体:TestSocket 同款行为 + `poll_flush` 一次性注入钩子。
    /// 发送半 park 前必经一次 flush(poll_ready 无帧可发而落到 flush 的
    /// 路径),钩子在该点同步执行——精确落在 pop_to_send(本轮已空)与
    /// set_send_warker 之间的丢唤醒窗口内。
    struct FlushInjectSocket {
        base: TestSocket,
        armed: Rc<std::cell::Cell<bool>>,
        inject: Rc<dyn Fn()>,
    }

    impl crate::Writev2 for FlushInjectSocket {
        fn poll_write2(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            a: &[u8],
            b: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            let this = self.get_mut();
            crate::Writev2::poll_write2(Pin::new(&mut this.base), cx, a, b)
        }
    }

    impl futures::io::AsyncRead for FlushInjectSocket {
        fn poll_read(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut [u8],
        ) -> Poll<std::io::Result<usize>> {
            let this = self.get_mut();
            futures::io::AsyncRead::poll_read(Pin::new(&mut this.base), cx, buf)
        }
    }

    impl futures::io::AsyncWrite for FlushInjectSocket {
        fn poll_write(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            crate::Writev2::poll_write2(self, cx, &[], buf)
        }
        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            let this = self.get_mut();
            if this.armed.get() {
                this.armed.set(false);
                (this.inject)();
            }
            Poll::Ready(Ok(()))
        }
        fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    /// T8(红→绿):sink 空闲型 park 的丢唤醒窗口——当次 poll 内,在
    /// `pop_to_send`(返回空)与 `set_send_waker` 之间压入的 Pong,此刻
    /// send_waker 槽为空(push 不产生唤醒)、sink 空闲(本轮 poll_ready
    /// 曾 Ready(Ok),未武装任何载体写 waker),park 复查必须把 to_send
    /// 纳入(以"本轮 poll_ready 未被阻塞"为门),否则帧滞留 to_send
    /// 直到下一个无关事件。修前(Round-D3 首版)复查只看
    /// new_receivers/error,窗口内的 Pong 无人认领(红)。
    /// 注意与 T7 的边界:T7 守卫半写阻塞型 park(poll_ready 恒 Pending)
    /// 不得因 to_send 复活自旋——本修复以 sink 空闲为门,两类 park 分别
    /// 守卫。
    #[test]
    fn send_park_rechecks_tosend_pushed_before_waker_registration() {
        let (base, st) = TestSocket::new();
        let armed = Rc::new(std::cell::Cell::new(false));
        // 共享状态句柄经测试缝取回:注入闭包在载体 flush 钩子内执行时,
        // SendHalf 正处于 poll 的 &mut 借用中,须持独立 Arc 句柄。
        let shared_slot: Rc<RefCell<Option<Arc<halves::ConnShared>>>> = Rc::new(RefCell::new(None));
        let slot = shared_slot.clone();
        let inject = Rc::new(move || {
            // 窗口内压入 Pong:走生产 push_to_send 语义——此刻 send_waker
            // 槽为空,take() 无 waker 可取,不产生任何唤醒。
            let mut hdr = header::Header::ping(9);
            hdr.ack();
            let pong: Frame<()> = Frame::new(hdr).into();
            slot.borrow()
                .as_ref()
                .expect("shared handle installed before poll")
                .push_to_send_for_test(pong);
        });
        let sock = FlushInjectSocket {
            base,
            armed: armed.clone(),
            inject,
        };
        let conn = Connection::new(sock, Config::default(), Mode::Client);
        let (recv_half, mut send_half, _workers, _send_workers) = conn.into_halves();
        *shared_slot.borrow_mut() = Some(send_half.shared_for_test());

        // 单次 poll:sink 空闲(写恒 Ready),rtt ping 立即到期发出并写穿,
        // 随后 pop_to_send 空 → flush 钩子在窗口内压入 Pong → park 复查。
        let mut cx = noop_cx();
        armed.set(true);
        match poll_fn(|cx| send_half.poll(cx)).poll_unpin(&mut cx) {
            Poll::Pending => {}
            other => panic!("send half must eventually park, got {other:?}"),
        }

        // 窗口内压入的 Pong 不得滞留:必须在同一轮 poll 内被复查出队并
        // 写上 wire(rtt ping 12B + Pong 12B)。修前:written 只有 12B 的
        // rtt ping,Pong 滞留 to_send(红)。
        let written = st.borrow().written.lock().unwrap().clone();
        assert_eq!(
            written.len(),
            24,
            "rtt ping (12B) + window-injected pong (12B) must be written, got {written:?}"
        );
        let h0 = header::decode(&written[0..12].try_into().unwrap())
            .expect("first frame is the rtt ping");
        assert_eq!(h0.tag(), Tag::Ping, "first frame is the rtt ping");
        let h1 = header::decode(&written[12..24].try_into().unwrap())
            .expect("second frame must be the injected pong");
        assert_eq!(h1.tag(), Tag::Ping, "second frame is the injected pong");
        assert!(
            h1.flags().contains(header::ACK),
            "injected pong must carry ACK, got {:?}",
            h1.flags()
        );
        assert_eq!(
            h1.len().val(),
            9,
            "injected pong keeps its opaque id (length field)"
        );
        drop(recv_half); // 收尾
    }

    // ===== Round-E2:真·载体拆分(fd-dup 双载体,零锁读∥写)=====

    /// futures IO 双工内存管道对(round-E2 测试基建;futures 0.3 无内建
    /// duplex——那是 tokio::io 的 API,本 crate 不依赖 tokio)。
    /// (a, b):a 写 → b 读,b 写 → a 读。poll_close = 写半关闭(对端
    /// 读队列耗尽后 EOF);任一端 drop = 对端见 EOF(对端写侧不测错,
    /// 本测试只依赖读侧 EOF)。
    struct DuplexPipeState {
        queue: std::collections::VecDeque<Vec<u8>>,
        cur: Vec<u8>,
        cur_off: usize,
        writer_gone: bool,
        waker: Option<Waker>,
    }

    struct DuplexPipe {
        /// 本端读队列。
        own: Rc<RefCell<DuplexPipeState>>,
        /// 对端读队列(本端写目标)。
        peer: Rc<RefCell<DuplexPipeState>>,
    }

    fn duplex_pipe() -> (DuplexPipe, DuplexPipe) {
        let a = Rc::new(RefCell::new(DuplexPipeState {
            queue: Default::default(),
            cur: Vec::new(),
            cur_off: 0,
            writer_gone: false,
            waker: None,
        }));
        let b = Rc::new(RefCell::new(DuplexPipeState {
            queue: Default::default(),
            cur: Vec::new(),
            cur_off: 0,
            writer_gone: false,
            waker: None,
        }));
        (
            DuplexPipe {
                own: a.clone(),
                peer: b.clone(),
            },
            DuplexPipe { own: b, peer: a },
        )
    }

    impl Drop for DuplexPipe {
        fn drop(&mut self) {
            // 本端整体弃置:对端读队列耗尽后 EOF
            let mut peer = self.peer.borrow_mut();
            peer.writer_gone = true;
            if let Some(w) = peer.waker.take() {
                w.wake();
            }
        }
    }

    impl futures::io::AsyncRead for DuplexPipe {
        fn poll_read(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut [u8],
        ) -> Poll<std::io::Result<usize>> {
            let mut own = self.own.borrow_mut();
            if own.cur_off >= own.cur.len() {
                if let Some(next) = own.queue.pop_front() {
                    own.cur = next;
                    own.cur_off = 0;
                }
            }
            if own.cur_off < own.cur.len() {
                let n = (own.cur.len() - own.cur_off).min(buf.len());
                buf[..n].copy_from_slice(&own.cur[own.cur_off..own.cur_off + n]);
                own.cur_off += n;
                return Poll::Ready(Ok(n));
            }
            if own.writer_gone {
                return Poll::Ready(Ok(0));
            }
            own.waker = Some(cx.waker().clone());
            Poll::Pending
        }
    }

    impl futures::io::AsyncWrite for DuplexPipe {
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
            // 写半关闭:对端读队列耗尽后 EOF
            let mut peer = self.peer.borrow_mut();
            peer.writer_gone = true;
            if let Some(w) = peer.waker.take() {
                w.wake();
            }
            Poll::Ready(Ok(()))
        }
    }

    impl crate::Writev2 for DuplexPipe {
        fn poll_write2(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            a: &[u8],
            b: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            // 帧头+帧体并成单块入队(读者按需部分读)
            let mut block = Vec::with_capacity(a.len() + b.len());
            block.extend_from_slice(a);
            block.extend_from_slice(b);
            let n = block.len();
            let mut peer = self.peer.borrow_mut();
            peer.queue.push_back(block);
            if let Some(w) = peer.waker.take() {
                w.wake();
            }
            Poll::Ready(Ok(n))
        }
    }

    /// E2-1(round-E2,新能力无修前红证):fd-dup 双载体全链回环——两条
    /// 独立读/写载体交叉接线两个 Connection,`into_halves_duplex` 拆分;
    /// 开流(建流授信 SYN|WU / ACK|WU)、双向发收、半关闭(FIN → 对端
    /// EOF)、连接级 EOF 传播(对端双半 drop → 本端 Ready(None) →
    /// 发半快速收敛)全部走通。
    #[test]
    fn duplex_halves_stream_roundtrip() {
        // 初窗 6MiB:建流授信差额(6MiB−256KiB)> 0,SYN|WU / ACK|WU
        // 建流授信帧真实上线(默认初窗差额为 0 时 SYN 惰性随首数据帧)。
        let mut cfg = Config::default();
        cfg.set_initial_stream_receive_window(6 * 1024 * 1024);
        // pipe1: A.write → B.read;pipe2: B.write → A.read
        let (a_write, b_read) = duplex_pipe();
        let (b_write, a_read) = duplex_pipe();
        let conn_a = Connection::new(a_read, cfg.clone(), Mode::Client);
        let (mut recv_a, mut send_a, mut workers_a, mut send_workers_a) =
            conn_a.into_halves_duplex(a_write);
        let conn_b = Connection::new(b_read, cfg, Mode::Server);
        let (mut recv_b, mut send_b, mut workers_b, mut send_workers_b) =
            conn_b.into_halves_duplex(b_write);

        let mut cx = noop_cx();

        // 开流:A 发半开流(建流授信 SYN|WU 随命令通道)→ 写穿
        let mut stream_a = match send_a.poll_new_outbound(&mut cx) {
            Poll::Ready(Ok(s)) => s,
            other => panic!("open outbound on duplex send half, got {other:?}"),
        };
        match drive_send_workers(&mut send_a, &mut workers_a, &mut send_workers_a, &mut cx) {
            Poll::Pending => {}
            other => panic!("duplex send half must park after flush, got {other:?}"),
        }
        // B 收半解帧:SYN|WU 经 Framer 路由进 Worker 建流、new_streams
        // 回流交付(其建流授信 ACK|WU 已排入命令通道)
        let mut stream_b =
            match drive_recv_workers(&mut recv_b, &mut workers_b, &mut send_workers_b, &mut cx) {
                Poll::Ready(Some(Ok(s))) => s,
                other => panic!("duplex recv half must deliver inbound stream, got {other:?}"),
            };
        match drive_send_workers(&mut send_b, &mut workers_b, &mut send_workers_b, &mut cx) {
            Poll::Pending => {}
            other => panic!("duplex peer send half must park after flushing ACK|WU, got {other:?}"),
        }
        // A 收半处理 ACK(Worker 收编 opening 流 1 并确认;无新流 → Pending)
        match drive_recv_workers(&mut recv_a, &mut workers_a, &mut send_workers_a, &mut cx) {
            Poll::Pending => {}
            other => panic!("duplex recv half must park after ACK, got {other:?}"),
        }

        // 双向发收:A → B
        match Pin::new(&mut stream_a).poll_write(&mut cx, b"ping") {
            Poll::Ready(Ok(4)) => {}
            other => panic!("duplex stream write must be ready, got {other:?}"),
        }
        match drive_send_workers(&mut send_a, &mut workers_a, &mut send_workers_a, &mut cx) {
            Poll::Pending => {}
            other => panic!("send half must flush data frame, got {other:?}"),
        }
        match drive_recv_workers(&mut recv_b, &mut workers_b, &mut send_workers_b, &mut cx) {
            Poll::Pending => {}
            other => panic!("recv half must dispatch data, got {other:?}"),
        }
        let mut buf = [0u8; 8];
        let n = match Pin::new(&mut stream_b).poll_read(&mut cx, &mut buf) {
            Poll::Ready(Ok(n)) => n,
            other => panic!("read from duplex peer stream, got {other:?}"),
        };
        assert_eq!(&buf[..n], b"ping");

        // 双向发收:B → A
        match Pin::new(&mut stream_b).poll_write(&mut cx, b"pong") {
            Poll::Ready(Ok(4)) => {}
            other => panic!("peer duplex stream write must be ready, got {other:?}"),
        }
        match drive_send_workers(&mut send_b, &mut workers_b, &mut send_workers_b, &mut cx) {
            Poll::Pending => {}
            other => panic!("peer send half must flush data frame, got {other:?}"),
        }
        match drive_recv_workers(&mut recv_a, &mut workers_a, &mut send_workers_a, &mut cx) {
            Poll::Pending => {}
            other => panic!("recv half must dispatch peer data, got {other:?}"),
        }
        let n = match Pin::new(&mut stream_a).poll_read(&mut cx, &mut buf) {
            Poll::Ready(Ok(n)) => n,
            other => panic!("read from duplex stream, got {other:?}"),
        };
        assert_eq!(&buf[..n], b"pong");

        // 半关闭:A 关写半 → FIN 上线 → B 侧流 EOF
        match Pin::new(&mut stream_a).poll_close(&mut cx) {
            Poll::Ready(Ok(())) => {}
            other => panic!("duplex stream close must be ready, got {other:?}"),
        }
        match drive_send_workers(&mut send_a, &mut workers_a, &mut send_workers_a, &mut cx) {
            Poll::Pending => {}
            other => panic!("send half must flush FIN, got {other:?}"),
        }
        match drive_recv_workers(&mut recv_b, &mut workers_b, &mut send_workers_b, &mut cx) {
            Poll::Pending => {}
            other => panic!("recv half must dispatch FIN, got {other:?}"),
        }
        match Pin::new(&mut stream_b).poll_read(&mut cx, &mut buf) {
            Poll::Ready(Ok(0)) => {}
            other => panic!("peer stream must see EOF after half-close, got {other:?}"),
        }
        // B 也半关闭 → A 侧流同样 EOF
        match Pin::new(&mut stream_b).poll_close(&mut cx) {
            Poll::Ready(Ok(())) => {}
            other => panic!("peer stream close must be ready, got {other:?}"),
        }
        match drive_send_workers(&mut send_b, &mut workers_b, &mut send_workers_b, &mut cx) {
            Poll::Pending => {}
            other => panic!("peer send half must flush FIN, got {other:?}"),
        }
        match drive_recv_workers(&mut recv_a, &mut workers_a, &mut send_workers_a, &mut cx) {
            Poll::Pending => {}
            other => panic!("recv half must dispatch peer FIN, got {other:?}"),
        }
        match Pin::new(&mut stream_a).poll_read(&mut cx, &mut buf) {
            Poll::Ready(Ok(0)) => {}
            other => panic!("stream must see EOF after peer half-close, got {other:?}"),
        }

        // 连接级 EOF 传播:B 双半 drop → A 读侧 Ready(None) → 发半快速收敛
        drop(recv_b);
        drop(send_b);
        match drive_recv_workers(&mut recv_a, &mut workers_a, &mut send_workers_a, &mut cx) {
            Poll::Ready(None) => {}
            other => panic!("recv half must observe peer EOF as Ready(None), got {other:?}"),
        }
        match poll_fn(|cx| send_a.poll(cx)).poll_unpin(&mut cx) {
            Poll::Ready(Err(e)) => {
                assert!(
                    matches!(e, ConnectionError::Closed),
                    "peer EOF must surface as Closed, got {e:?}"
                );
            }
            other => panic!("send half must fail fast after peer EOF, got {other:?}"),
        }
    }

    /// E2-2(round-E2 机制断言):载体读侧挂起(无数据)时发半 poll 写
    /// 照常推进——读等待不阻塞写路径。duplex 下读/写是两个独立载体对象
    /// (结构上无共享锁):断言建流授信与数据帧全部落在写载体、读载体
    /// 零写入(write_calls=0)且读侧保持 Pending。
    #[test]
    fn duplex_read_write_progress_concurrently() {
        // 读载体:无脚本数据 → poll_read 恒 Pending;写载体:独立记录。
        // 初窗 6MiB:建流授信 SYN|WU 真实上线(默认初窗差额为 0 时
        // SYN 惰性随首数据帧,无独立授信帧可断言)。
        let mut cfg = Config::default();
        cfg.set_initial_stream_receive_window(6 * 1024 * 1024);
        let (read_socket, read_st) = TestSocket::new();
        let (write_socket, write_st) = TestSocket::new();
        let conn = Connection::new(read_socket, cfg, Mode::Client);
        let (mut recv_half, mut send_half, _workers, mut send_workers) =
            conn.into_halves_duplex(write_socket);

        let mut cx = noop_cx();
        // 先挂起读侧(waker 登记在读载体上)
        match poll_fn(|cx| recv_half.poll(cx)).poll_unpin(&mut cx) {
            Poll::Pending => {}
            other => panic!("recv half must park on empty read carrier, got {other:?}"),
        }

        // 读侧挂起状态下:开流 + 排数据帧 + 驱动发半,写路径必须照常完成
        let mut stream = match send_half.poll_new_outbound(&mut cx) {
            Poll::Ready(Ok(s)) => s,
            other => panic!("open outbound while read parked, got {other:?}"),
        };
        match Pin::new(&mut stream).poll_write(&mut cx, b"concurrent") {
            Poll::Ready(Ok(10)) => {}
            other => panic!("queue data frame while read parked, got {other:?}"),
        }
        // round-6:流命令由 SendWorker 消费(注册路由 D4),驱动面覆盖之
        for w in send_workers.iter_mut() {
            let _ = w.poll_unpin(&mut cx);
        }
        match poll_fn(|cx| send_half.poll(cx)).poll_unpin(&mut cx) {
            Poll::Pending => {}
            other => panic!("send half must park after flushing writes, got {other:?}"),
        }

        // 写照常落地:写载体上必有建流授信(SYN|WU)+ 数据帧。
        // 按帧走读 written(控制帧 len 字段是信用/opaque,无帧体;
        // Data 的 len 才是帧体长度)。
        let bytes = write_st.borrow().written.lock().unwrap().clone();
        let mut hdrs = Vec::new();
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
            off += header::HEADER_SIZE + body_len;
            hdrs.push(hdr);
        }
        assert!(
            hdrs.iter().any(|h| h.tag() == Tag::WindowUpdate && h.flags().contains(header::SYN)),
            "creation grant (SYN|WU) must reach the write carrier while the read side parks: {hdrs:?}"
        );
        assert!(
            hdrs.iter()
                .any(|h| h.tag() == Tag::Data && h.len().val() == 10),
            "data frame must reach the write carrier while the read side parks: {hdrs:?}"
        );
        // 读载体零写入:读/写两载体结构独立,读侧挂起不触碰写路径
        assert_eq!(
            read_st.borrow().write_calls.load(Ordering::SeqCst),
            0,
            "the read carrier must never be written by the send half"
        );
        assert!(
            read_st.borrow().read_blocks.lock().unwrap().is_empty(),
            "no scripted data consumed: the read side stays parked"
        );
    }

    // ===== round-5 §5.4 跨帧批量写(发送半级:§3.3 集成)=====

    /// 按帧走读线上字节(tag 感知:只有 Data 帧的 len 是帧体长度,控制帧
    /// 的 len 字段是信用/opaque id、无帧体)。`written_headers` 对非 Data
    /// 帧按 len 跳字节,Pong(len = ping id)会错位,此处不复用。
    fn frames_on_wire(bytes: &[u8]) -> Vec<(header::Header<()>, Vec<u8>)> {
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

    /// T-B1(红→绿):3 帧 × 4KiB 一次驱动 → TestSocket writev 调用数
    /// == 1(修前每帧一次 writev = 3);线上字节序 = 帧序、逐字节一致。
    /// 预驱动一次冲掉 `Rtt::new` 立即到期的 rtt ping(独立一批),使
    /// 断言只计量 3 个数据帧。
    #[test]
    fn batch_three_frames_one_writev() {
        let (socket, st) = TestSocket::new();
        let conn = Connection::new(socket, Config::default(), Mode::Client);
        let (_recv_half, mut send_half, _workers, mut send_workers) = conn.into_halves();
        let mut cx = noop_cx();

        match poll_fn(|cx| send_half.poll(cx)).poll_unpin(&mut cx) {
            Poll::Pending => {}
            other => panic!("send half must park after the rtt ping, got {other:?}"),
        }
        st.borrow().writev_calls.store(0, Ordering::SeqCst);
        let before = st.borrow().written.lock().unwrap().len();

        let mut stream = match send_half.poll_new_outbound(&mut cx) {
            Poll::Ready(Ok(s)) => s,
            other => panic!("open outbound via send half, got {other:?}"),
        };
        let payloads = [[1u8; 4096], [2u8; 4096], [3u8; 4096]];
        for p in &payloads {
            match Pin::new(&mut stream).poll_write(&mut cx, p) {
                Poll::Ready(Ok(4096)) => {}
                other => panic!("queue data frame, got {other:?}"),
            }
        }
        // round-6:流命令由 SendWorker 消费进 send_out,再由 FlushTask
        // 一次批量写出(writev 汇聚断言口径不变)
        for w in send_workers.iter_mut() {
            let _ = w.poll_unpin(&mut cx);
        }
        match poll_fn(|cx| send_half.poll(cx)).poll_unpin(&mut cx) {
            Poll::Pending => {}
            other => panic!("send half must park after the batch, got {other:?}"),
        }

        assert_eq!(
            st.borrow().writev_calls.load(Ordering::SeqCst),
            1,
            "3 frames must coalesce into exactly one writev (修前每帧一次 = 3)"
        );
        let bytes = st.borrow().written.lock().unwrap()[before..].to_vec();
        assert_eq!(
            bytes.len(),
            3 * (header::HEADER_SIZE + 4096),
            "exactly the three data frames reach the wire"
        );
        let frames = frames_on_wire(&bytes);
        assert_eq!(frames.len(), 3);
        for (i, (hdr, body)) in frames.iter().enumerate() {
            assert_eq!(hdr.tag(), Tag::Data);
            assert_eq!(hdr.stream_id(), StreamId::new(1));
            assert_eq!(body, &payloads[i][..], "frame #{i} body must be in order");
        }
    }

    /// T-B4:to_send 有 Pong + 流帧(2 流 × 8 帧 = 16)→ Pong 字节先上线
    /// (帧优先级 to_send > rtt ping > 流命令);全部 16 个流帧随后上线。
    /// round-7:pong + rtt ping + 16 流帧 = 18 帧 ≤ [`MAX_BATCH_FRAMES`] 且
    /// 总字节 < [`MAX_BATCH_BYTES`] → 合成**单批** writev(round-5 的 16 帧
    /// 上限下恰分两批)。
    #[test]
    fn batch_pong_precedes_full_batch_of_stream_frames() {
        let (socket, st) = TestSocket::new();
        let conn = Connection::new(socket, Config::default(), Mode::Client);
        let (_recv_half, mut send_half, _workers, mut send_workers) = conn.into_halves();
        let mut cx = noop_cx();

        let payload = [7u8; 4096];
        let mut streams = Vec::new();
        for _ in 0..2 {
            let mut s = match send_half.poll_new_outbound(&mut cx) {
                Poll::Ready(Ok(s)) => s,
                other => panic!("open outbound via send half, got {other:?}"),
            };
            for _ in 0..8 {
                match Pin::new(&mut s).poll_write(&mut cx, &payload) {
                    Poll::Ready(Ok(4096)) => {}
                    other => panic!("queue data frame, got {other:?}"),
                }
            }
            streams.push(s);
        }
        // round-6:16 个流帧先由 SendWorker 消费进 send_out(轮转路径)
        for w in send_workers.iter_mut() {
            let _ = w.poll_unpin(&mut cx);
        }
        // 接收侧压入 Pong(走生产 push_to_send 语义)
        let mut hdr = header::Header::ping(9);
        hdr.ack();
        send_half
            .shared_for_test()
            .push_to_send_for_test(Frame::new(hdr).into());

        match poll_fn(|cx| send_half.poll(cx)).poll_unpin(&mut cx) {
            Poll::Pending => {}
            other => panic!("send half must park after draining, got {other:?}"),
        }

        let bytes = st.borrow().written.lock().unwrap().clone();
        let frames = frames_on_wire(&bytes);
        let (h0, _) = &frames[0];
        assert_eq!(
            h0.tag(),
            Tag::Ping,
            "first frame on the wire must be the pong"
        );
        assert!(
            h0.flags().contains(header::ACK),
            "pong must carry ACK, got {:?}",
            h0.flags()
        );
        assert_eq!(h0.len().val(), 9, "pong keeps its opaque id");
        let data = frames.iter().filter(|(h, _)| h.tag() == Tag::Data).count();
        assert_eq!(data, 16, "all 16 stream frames must follow the pong");
        // 18 帧落入单批的前提(fill_batch 口径:取帧前检查帧数 < 上限、
        // 累计字节 < 上限;pong/ping 仅帧头),引用常量使下一轮扩容不炸
        let total_frames = 2 + 16;
        let total_bytes = 2 * header::HEADER_SIZE + 16 * (header::HEADER_SIZE + 4096);
        assert!(
            total_frames <= MAX_BATCH_FRAMES && total_bytes < MAX_BATCH_BYTES,
            "caps must admit pong + rtt ping + 16 stream frames in one batch"
        );
        assert_eq!(
            st.borrow().writev_calls.load(Ordering::SeqCst),
            1,
            "pong + rtt ping + 16 stream frames = 18 frames → one batch under round-7 caps"
        );
    }

    /// T-B7(round-7,红→绿):生产帧形态下的批聚合——`set_split_send_size`
    /// 对齐应用层 64KiB 步进(rh-transport mux.rs `CHUNK_CAP`),单流连写
    /// 16 个 64KiB 帧(round-7 目标工况"16 流 × 64KiB 帧单次 writev 覆盖
    /// 全部 16 流"的单流同型)→ 驱动 SendWorker + 发送半 → 恰 **1 次**
    /// writev、线上恰 16 帧、帧序与字节逐一相符。修前双红:256KiB 字节
    /// 上限下 fill_batch 4 帧即止 → 4 次 writev;流命令通道 cap 10(+2
    /// 发送句柄)在第 13 次 poll_write 即满 → Pending。第二阶段再补写
    /// 1 帧(共 17):前 16 帧已成批上线,末帧独立成批 → 总 2 次 writev。
    ///
    /// 工况帧数固定为 16——它是扩容的**目标**,不从上限反推(反推会在旧
    /// 上限下自适应为 4 帧而失去红→绿判别力);上限与工况的联动以常量
    /// 推导断言(§1.6),下一轮扩容不炸、缩容则明确报出。
    ///
    /// 写侧授信:出站流本地 send_window 初值为 [`DEFAULT_CREDIT`](256KiB,
    /// stream.rs `new_outbound`),17 个 64KiB 帧需 1088KiB,故先经**生产
    /// 入站路径**(RecvHalf → ShardWorker::on_window_update)注入对端窗口
    /// 更新补齐差额——等价对端建流授信(S2:对端初窗 6MiB 时 SYN|WU 授
    /// 6MiB−256KiB),不触碰流内部状态。
    #[test]
    fn batch_64k_frames_fill_byte_cap_one_writev() {
        /// round-7 目标工况的帧数(16 流 × 64KiB 帧)。
        const SCENARIO_FRAMES: usize = 16;
        const FRAME_BODY: usize = 64 * 1024;
        const WIRE: usize = header::HEADER_SIZE + FRAME_BODY;
        // 上限一批可容纳的 64KiB 帧数(fill_batch 口径:取帧前检查累计
        // 线上字节 < 字节上限,故取满 n 帧 ⟺ 前 n−1 帧累计 < 上限,即
        // n = ceil(上限 / 单帧线上长);再受帧数上限封顶)。
        let per_batch = MAX_BATCH_FRAMES.min(MAX_BATCH_BYTES.div_ceil(WIRE));

        let (socket, st) = TestSocket::new();
        let mut cfg = Config::default();
        cfg.set_split_send_size(FRAME_BODY);
        let conn = Connection::new(socket, cfg, Mode::Client);
        let (mut recv_half, mut send_half, mut workers, mut send_workers) = conn.into_halves();
        let mut cx = noop_cx();

        // 预驱动冲掉 Rtt::new 立即到期的 rtt ping(独立一批,不计入断言)
        match poll_fn(|cx| send_half.poll(cx)).poll_unpin(&mut cx) {
            Poll::Pending => {}
            other => panic!("send half must park after the rtt ping, got {other:?}"),
        }

        let mut stream = match send_half.poll_new_outbound(&mut cx) {
            Poll::Ready(Ok(s)) => s,
            other => panic!("open outbound via send half, got {other:?}"),
        };
        // 对端授信:补齐 17 帧所需 send_window 与初始 DEFAULT_CREDIT 的
        // 差额,经入站路径处理(WU 不交付新流 → Pending)
        let need = ((SCENARIO_FRAMES + 1) * FRAME_BODY) as u32;
        assert!(
            need > DEFAULT_CREDIT,
            "test premise: the write volume must exceed the initial credit"
        );
        st.borrow_mut()
            .read_blocks
            .lock()
            .unwrap()
            .push_back(wu_credit_block(1, need - DEFAULT_CREDIT));
        match drive_recv_workers(&mut recv_half, &mut workers, &mut send_workers, &mut cx) {
            Poll::Pending => {}
            other => panic!("a window update must not deliver a stream, got {other:?}"),
        }

        st.borrow().writev_calls.store(0, Ordering::SeqCst);
        let before = st.borrow().written.lock().unwrap().len();

        // 第一阶段:16 个 64KiB 帧 → 恰 1 次 writev
        let payloads: Vec<Vec<u8>> = (0..SCENARIO_FRAMES)
            .map(|i| vec![(i + 1) as u8; FRAME_BODY])
            .collect();
        for p in &payloads {
            match Pin::new(&mut stream).poll_write(&mut cx, p) {
                Poll::Ready(Ok(n)) if n == FRAME_BODY => {}
                other => panic!("queue 64KiB data frame, got {other:?}"),
            }
        }
        // round-6:流命令由 SendWorker 消费进 send_out,再由 FlushTask
        // 一次批量写出(与 T-B1 同型)
        for w in send_workers.iter_mut() {
            let _ = w.poll_unpin(&mut cx);
        }
        match poll_fn(|cx| send_half.poll(cx)).poll_unpin(&mut cx) {
            Poll::Pending => {}
            other => panic!("send half must park after the batch, got {other:?}"),
        }
        assert_eq!(
            st.borrow().writev_calls.load(Ordering::SeqCst),
            1,
            "{SCENARIO_FRAMES} × 64KiB frames must coalesce into exactly one writev \
             (caps admit {per_batch} such frames per batch; 修前 256KiB 上限 → 4 帧/批 = 4 次)"
        );
        // §1.6 常量联动:单批成立 ⟺ 上限容纳整个工况
        assert!(
            SCENARIO_FRAMES <= per_batch,
            "caps must admit the whole scenario in one batch, admit {per_batch}"
        );
        let bytes = st.borrow().written.lock().unwrap()[before..].to_vec();
        assert_eq!(
            bytes.len(),
            SCENARIO_FRAMES * WIRE,
            "exactly the {SCENARIO_FRAMES} data frames reach the wire"
        );
        let frames = frames_on_wire(&bytes);
        assert_eq!(frames.len(), SCENARIO_FRAMES);
        for (i, (hdr, body)) in frames.iter().enumerate() {
            assert_eq!(hdr.tag(), Tag::Data);
            assert_eq!(hdr.stream_id(), StreamId::new(1));
            assert_eq!(body, &payloads[i], "frame #{i} body must be in order");
        }

        // 第二阶段:补写第 17 帧 → 独立成批,总 2 次 writev
        let tail = vec![0xEEu8; FRAME_BODY];
        match Pin::new(&mut stream).poll_write(&mut cx, &tail) {
            Poll::Ready(Ok(n)) if n == FRAME_BODY => {}
            other => panic!("queue the tail 64KiB data frame, got {other:?}"),
        }
        for w in send_workers.iter_mut() {
            let _ = w.poll_unpin(&mut cx);
        }
        match poll_fn(|cx| send_half.poll(cx)).poll_unpin(&mut cx) {
            Poll::Pending => {}
            other => panic!("send half must park after the tail batch, got {other:?}"),
        }
        assert_eq!(
            st.borrow().writev_calls.load(Ordering::SeqCst),
            2,
            "frame #{} lands in its own batch: exactly one more writev",
            SCENARIO_FRAMES + 1
        );
        let bytes = st.borrow().written.lock().unwrap()[before..].to_vec();
        let frames = frames_on_wire(&bytes);
        assert_eq!(
            frames.len(),
            SCENARIO_FRAMES + 1,
            "all {} frames reach the wire",
            SCENARIO_FRAMES + 1
        );
        let (hdr, body) = &frames[SCENARIO_FRAMES];
        assert_eq!(hdr.tag(), Tag::Data);
        assert_eq!(hdr.stream_id(), StreamId::new(1));
        assert_eq!(body, &tail, "tail frame body must be intact");
    }
}
