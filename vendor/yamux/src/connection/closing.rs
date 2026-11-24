use crate::connection::cmd_queue::CmdGroup;
use crate::connection::stream::Shared;
use crate::connection::StreamCommand;
use crate::frame::Frame;
use crate::Result;
use crate::{frame, StreamId};
use futures::stream::Fuse;
use futures::{ready, AsyncRead, AsyncWrite, SinkExt, StreamExt};
use nohash_hasher::IntMap;
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

/// A [`Future`] that gracefully closes the yamux connection.
#[must_use]
pub struct Closing<T> {
    state: State,
    stream_receivers: CmdGroup<StreamCommand>,
    pending_frames: VecDeque<Frame<()>>,
    socket: Fuse<frame::Io<T>>,
    /// C3:进入 Closing 时驻留流的共享状态。关闭完成/丢弃时先断开
    /// 命令通道再二次唤醒,使挂起的读/写任务重新 poll 时错误立即可见
    /// (不会重挂 waker 死等);唤醒不推进流状态,已缓冲数据仍可读。
    streams: IntMap<StreamId, Arc<Mutex<Shared>>>,
}

impl<T> Closing<T>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    pub(crate) fn new(
        stream_receivers: CmdGroup<StreamCommand>,
        pending_frames: VecDeque<Frame<()>>,
        socket: Fuse<frame::Io<T>>,
        streams: IntMap<StreamId, Arc<Mutex<Shared>>>,
    ) -> Self {
        Self {
            state: State::ClosingStreamReceiver,
            stream_receivers,
            pending_frames,
            socket,
            streams,
        }
    }
}

impl<T> Closing<T> {
    /// C3:唤醒全部驻留流的挂起 reader/writer(仅唤醒,不改状态)。
    fn wake_all_streams(&mut self) {
        for s in self.streams.values() {
            let mut shared = s.lock();
            if let Some(w) = shared.reader.take() {
                w.wake()
            }
            if let Some(w) = shared.writer.take() {
                w.wake()
            }
        }
    }
}

impl<T> Drop for Closing<T> {
    fn drop(&mut self) {
        // C3 兜底:无论 close 流程是否 poll 到完成,丢弃时都必须解除
        // 驻留流的挂起。顺序关键——先断开命令通道(丢掉全部 receiver),
        // 再唤醒:被唤醒的任务重新 poll 时通道已关闭,窗口更新的
        // `poll_ready` 立即报错,任务以错误终结,不会重挂 waker 死等;
        // 已缓冲数据仍可读(读路径缓冲优先,见 stream.rs C3 配套)。
        self.stream_receivers.clear();
        self.wake_all_streams();
    }
}

impl<T: AsyncRead + AsyncWrite + Unpin + crate::Writev2> Future for Closing<T>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    type Output = Result<()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        loop {
            match this.state {
                State::ClosingStreamReceiver => {
                    this.stream_receivers.close_receivers();
                    this.state = State::DrainingStreamReceiver;
                }

                State::DrainingStreamReceiver => {
                    match this.stream_receivers.poll_next_unpin(cx) {
                        Poll::Ready(Some((_, Some(StreamCommand::SendFrame(frame))))) => {
                            this.pending_frames.push_back(frame.into());
                        }
                        Poll::Ready(Some((id, Some(StreamCommand::CloseStream { ack, syn })))) => {
                            this.pending_frames
                                .push_back(Frame::close_stream(id, ack, syn).into());
                        }
                        Poll::Ready(Some((_, None))) => {}
                        Poll::Pending | Poll::Ready(None) => {
                            // No more frames from streams, append `Term` frame and flush them all.
                            this.pending_frames.push_back(Frame::term().into());
                            this.state = State::FlushingPendingFrames;
                            continue;
                        }
                    }
                }
                State::FlushingPendingFrames => {
                    ready!(this.socket.poll_ready_unpin(cx))?;

                    match this.pending_frames.pop_front() {
                        Some(frame) => this.socket.start_send_unpin(frame)?,
                        None => this.state = State::ClosingSocket,
                    }
                }
                State::ClosingSocket => {
                    ready!(this.socket.poll_close_unpin(cx))?;

                    return Poll::Ready(Ok(()));
                }
            }
        }
    }
}

enum State {
    ClosingStreamReceiver,
    DrainingStreamReceiver,
    FlushingPendingFrames,
    ClosingSocket,
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::future::poll_fn;
    use futures::FutureExt;

    /// C3(红→绿):graceful close 必须唤醒驻留流的挂起 reader/writer
    /// (不推进 State::Closed),且 Closing 完成后已缓冲数据仍可读、
    /// 缓冲耗尽才得到终结错误(不提前 EOF、不挂死)。
    #[test]
    fn closing_wakes_parked_streams() {
        use crate::frame::header;
        use crate::{Config, Connection, Mode, StreamId};
        use futures::io::AsyncRead;
        use std::pin::Pin;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::{Arc, Mutex};
        use std::task::{Context, Poll};

        #[derive(Default)]
        struct Shared {
            read_blocks: Mutex<std::collections::VecDeque<Vec<u8>>>,
            written: Mutex<Vec<u8>>,
        }
        struct MockSocket(Arc<Shared>);
        impl futures::io::AsyncRead for MockSocket {
            fn poll_read(
                self: Pin<&mut Self>,
                _cx: &mut Context<'_>,
                buf: &mut [u8],
            ) -> Poll<std::io::Result<usize>> {
                let blk = self.0.read_blocks.lock().unwrap().pop_front();
                match blk {
                    Some(b) => {
                        buf[..b.len()].copy_from_slice(&b);
                        Poll::Ready(Ok(b.len()))
                    }
                    None => Poll::Ready(Ok(0)),
                }
            }
        }
        impl futures::io::AsyncWrite for MockSocket {
            fn poll_write(
                self: Pin<&mut Self>,
                _cx: &mut Context<'_>,
                buf: &[u8],
            ) -> Poll<std::io::Result<usize>> {
                self.0.written.lock().unwrap().extend_from_slice(buf);
                Poll::Ready(Ok(buf.len()))
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
        impl crate::Writev2 for MockSocket {
            fn poll_write2(
                self: Pin<&mut Self>,
                _cx: &mut Context<'_>,
                a: &[u8],
                b: &[u8],
            ) -> Poll<std::io::Result<usize>> {
                let mut w = self.0.written.lock().unwrap();
                w.extend_from_slice(a);
                w.extend_from_slice(b);
                Poll::Ready(Ok(a.len() + b.len()))
            }
        }

        fn syn_data_block(id: u32, body: &[u8]) -> Vec<u8> {
            let mut h = header::Header::data(StreamId::new(id), body.len() as u32);
            h.syn();
            let mut v = header::encode(&h).to_vec();
            v.extend_from_slice(body);
            v
        }
        fn data_block(id: u32, body: &[u8]) -> Vec<u8> {
            let h = header::Header::data(StreamId::new(id), body.len() as u32);
            let mut v = header::encode(&h).to_vec();
            v.extend_from_slice(body);
            v
        }

        #[derive(Default)]
        struct CountWaker(AtomicUsize);
        impl futures::task::ArcWake for CountWaker {
            fn wake_by_ref(me: &Arc<Self>) {
                me.0.fetch_add(1, Ordering::SeqCst);
            }
        }

        let shared = Arc::new(Shared::default());
        {
            let mut blocks = shared.read_blocks.lock().unwrap();
            blocks.push_back(syn_data_block(2, b"a"));
            blocks.push_back(data_block(2, b"tail"));
            blocks.push_back(syn_data_block(4, b"x"));
        }
        let mut conn = Connection::new(MockSocket(shared.clone()), Config::default(), Mode::Client);

        // 流 2(缓冲将有 "tail")与流 4(将挂起)
        let mut stream2 = futures::executor::block_on(poll_fn(|cx| conn.poll_next_inbound(cx)))
            .expect("inbound stream 2")
            .expect("ok");
        // 读掉 "a"(把控制权交回,让连接读入后续块)
        let mut buf = [0u8; 8];
        let _ = futures::executor::block_on(poll_fn(|cx| {
            Pin::new(&mut stream2).poll_read(cx, &mut buf)
        }))
        .expect("read a");

        let mut stream4 = futures::executor::block_on(poll_fn(|cx| conn.poll_next_inbound(cx)))
            .expect("inbound stream 4")
            .expect("ok");
        // 流 4:读完缓冲后挂起,注册 waker
        let mut buf4 = [0u8; 8];
        let _ = futures::executor::block_on(poll_fn(|cx| {
            Pin::new(&mut stream4).poll_read(cx, &mut buf4)
        }))
        .expect("read x");
        let cw = Arc::new(CountWaker::default());
        let wk = futures::task::waker(cw.clone());
        let mut cx = Context::from_waker(&wk);
        assert!(matches!(
            poll_fn(|cx| Pin::new(&mut stream4).poll_read(cx, &mut buf4)).poll_unpin(&mut cx),
            Poll::Pending
        ));
        assert_eq!(cw.0.load(Ordering::SeqCst), 0);

        // graceful close:进入 Closing 前必须唤醒驻留流的挂起 reader
        match poll_fn(|cx| conn.poll_close(cx)).poll_unpin(&mut cx) {
            Poll::Ready(Ok(())) => {}
            other => panic!("graceful close should complete, got {other:?}"),
        }
        assert!(
            cw.0.load(Ordering::SeqCst) >= 1,
            "C3: graceful close must wake parked stream readers"
        );

        // 已缓冲数据仍可读(不提前 Closed/EOF;Closing 完成后通道已断,
        // 缓冲交付必须优先于通道错误)
        let mut buf2 = [0u8; 8];
        let n = futures::executor::block_on(poll_fn(|cx| {
            Pin::new(&mut stream2).poll_read(cx, &mut buf2)
        }))
        .expect("buffered data must remain readable after graceful close");
        assert_eq!(&buf2[..n], b"tail");

        // 缓冲耗尽后得到明确的终结错误,而不是永远 Pending
        match futures::executor::block_on(poll_fn(|cx| {
            Pin::new(&mut stream2).poll_read(cx, &mut buf2)
        })) {
            Err(e) if e.kind() == std::io::ErrorKind::WriteZero => {}
            other => panic!("expected terminal error after drain, got {other:?}"),
        }
    }

    struct Socket {
        written: Vec<u8>,
        closed: bool,
    }
    impl AsyncRead for Socket {
        fn poll_read(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
            _: &mut [u8],
        ) -> Poll<std::io::Result<usize>> {
            unimplemented!()
        }
    }
    impl AsyncWrite for Socket {
        fn poll_write(
            mut self: Pin<&mut Self>,
            _: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            assert!(!self.closed);
            self.written.extend_from_slice(buf);
            Poll::Ready(Ok(buf.len()))
        }

        fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            unimplemented!()
        }

        fn poll_close(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            assert!(!self.closed);
            self.closed = true;
            Poll::Ready(Ok(()))
        }
    }

    #[test]
    fn pending_frames() {
        let frame_pending = Frame::data(StreamId::new(1), vec![2].into())
            .unwrap()
            .into();
        let frame_data = Frame::data(StreamId::new(3), vec![4].into())
            .unwrap()
            .into();
        let frame_close = Frame::close_stream(StreamId::new(5), false, false).into();
        let frame_close_ack = Frame::close_stream(StreamId::new(6), true, false).into();
        let frame_term = Frame::term().into();
        fn encode(buf: &mut Vec<u8>, frame: &Frame<()>) {
            buf.extend_from_slice(&frame::header::encode(frame.header()));
            if frame.header().tag() == frame::header::Tag::Data {
                buf.extend_from_slice(frame.clone().into_data().body());
            }
        }
        let mut expected_written = vec![];
        encode(&mut expected_written, &frame_pending);
        encode(&mut expected_written, &frame_data);
        encode(&mut expected_written, &frame_close);
        encode(&mut expected_written, &frame_close_ack);
        encode(&mut expected_written, &frame_term);

        use crate::connection::cmd_queue;

        let mut stream_receivers = CmdGroup::new();
        let mut push_cmd = |frame: &Frame<_>, command: StreamCommand| {
            let (mut tx, rx) = cmd_queue::channel(frame.header().stream_id(), 1);
            tx.try_send(command).unwrap();
            stream_receivers.push(rx);
        };
        push_cmd(
            &frame_data,
            StreamCommand::SendFrame(frame_data.clone().into_data().left()),
        );
        push_cmd(
            &frame_close,
            StreamCommand::CloseStream {
                ack: false,
                syn: false,
            },
        );
        push_cmd(
            &frame_close_ack,
            StreamCommand::CloseStream {
                ack: true,
                syn: false,
            },
        );
        let pending_frames = vec![frame_pending];
        let mut socket = Socket {
            written: vec![],
            closed: false,
        };
        let mut closing = Closing::new(
            stream_receivers,
            pending_frames.into(),
            frame::Io::new(crate::connection::Id(0), crate::Sequential(&mut socket)).fuse(),
            IntMap::default(),
        );
        futures::executor::block_on(async { poll_fn(|cx| closing.poll_unpin(cx)).await.unwrap() });
        assert!(closing.pending_frames.is_empty());
        drop(closing); // C3: Closing 新增 Drop(清 receivers+唤醒),先释放 socket 借用
        assert!(socket.closed);
        assert_eq!(socket.written, expected_written);
    }
}
