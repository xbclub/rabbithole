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
        Self {
            inner: ConnectionState::Active(Active::new(socket, cfg, mode)),
        }
    }

    /// Poll for a new outbound stream.
    ///
    /// This function will fail if the current state does not allow opening new outbound streams.
    pub fn poll_new_outbound(&mut self, cx: &mut Context<'_>) -> Poll<Result<Stream>> {
        loop {
            match std::mem::replace(&mut self.inner, ConnectionState::Poisoned) {
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
            ConnectionState::Active(active) => active.drop_all_streams(),
            ConnectionState::Closing(_) => {}
            ConnectionState::Cleanup(_) => {}
            ConnectionState::Closed => {}
            ConnectionState::Poisoned => {}
        }
    }
}

enum ConnectionState<T> {
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

    /// A stream's `max_stream_receive_window` can grow beyond [`DEFAULT_CREDIT`], see
    /// [`Stream::next_window_update`]. This field is the sum of the bytes by which all streams'
    /// `max_stream_receive_window` have each exceeded [`DEFAULT_CREDIT`]. Used to enforce
    /// [`Config::max_connection_receive_window`].
    accumulated_max_stream_windows: Arc<Mutex<usize>>,
}
/// `Stream` to `Connection` commands.
#[derive(Debug)]
pub(crate) enum StreamCommand {
    /// A new frame should be sent to the remote.
    SendFrame(Frame<Either<Data, WindowUpdate>>),
    /// Close a stream (send FIN; `syn` = 该流尚未发出 SYN,需 SYN|FIN 合帧).
    CloseStream { ack: bool, syn: bool },
}

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
            accumulated_max_stream_windows: Default::default(),
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
            if self.pending_read_frame.is_none()
                && (self.writes_since_read_probe >= FAIRNESS_MAX_WRITE_OPS_BETWEEN_READ_PROBES
                    || self.bytes_since_read_probe >= FAIRNESS_MAX_WRITE_BYTES_BETWEEN_READ_PROBES)
            {
                self.writes_since_read_probe = 0;
                self.bytes_since_read_probe = 0;
                match self.service_inbound(cx)? {
                    InboundOutcome::NewStream(stream) => return Poll::Ready(Ok(stream)),
                    InboundOutcome::Closed => return Poll::Ready(Err(ConnectionError::Closed)),
                    InboundOutcome::Progress | InboundOutcome::Pending => {}
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

        let (sender, receiver) = mpsc::channel(10); // 10 is an arbitrary number.
        self.stream_receivers.push(TaggedStream::new(id, receiver));
        if let Some(waker) = self.no_streams_waker.take() {
            waker.wake();
        }

        Stream::new_inbound(
            id,
            self.id,
            config,
            credit,
            sender,
            self.rtt.clone(),
            self.accumulated_max_stream_windows.clone(),
        )
    }

    fn make_new_outbound_stream(&mut self, id: StreamId) -> Stream {
        let config = self.config.clone();

        let (sender, receiver) = mpsc::channel(10); // 10 is an arbitrary number.
        self.stream_receivers.push(TaggedStream::new(id, receiver));
        if let Some(waker) = self.no_streams_waker.take() {
            waker.wake();
        }

        Stream::new_outbound(
            id,
            self.id,
            config,
            sender,
            self.rtt.clone(),
            self.accumulated_max_stream_windows.clone(),
        )
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

#[cfg(test)]
mod tests {
    use super::*;
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
            Poll::Ready(Ok(a.len() + b.len()))
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
        // buffer(10)+2 发送句柄,每 8 次驱动一次连接清空队列,保证
        // 后续 Pending 只可能因信用耗尽(而非通道满)。
        let mut cx = noop_cx();
        let payload = [7u8; 4096];
        let mut total = 0usize;
        loop {
            match Pin::new(&mut blocked).poll_write(&mut cx, &payload) {
                Poll::Ready(Ok(4096)) => {
                    total += 4096;
                    if total % (4096 * 8) == 0 {
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
}
