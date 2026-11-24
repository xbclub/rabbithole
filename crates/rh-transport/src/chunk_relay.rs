//! 跨平台 chunk 级用户态高速中继(形态 C):双侧无锁拆半 + 批量 copy 泵 + 共享收敛骨架。

use crate::{copy, mux, split_boxed_io, BoxedIo};
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt as _};

fn split_side(
    io: BoxedIo,
) -> (
    Box<dyn AsyncRead + Send + Unpin>,
    Box<dyn AsyncWrite + Send + Unpin>,
) {
    match split_boxed_io(io) {
        Ok((r, w)) => (Box::new(r), Box::new(w)),
        Err(io) => {
            let (r, w) = tokio::io::split(io);
            (Box::new(r), Box::new(w))
        }
    }
}

/// 跨平台 chunk 级双向中继:双侧优先 [`split_boxed_io`] 无锁拆半(回落 `tokio::io::split`),
/// 各向运行 [`copy::copy_batched`],收尾共享 [`mux::run_relay_pair`] 收敛骨架
/// (clean EOF 半关闭 + 迟到取消 + 30s 有界兜底;错误/取消 1s 有界关闭并立即中止对向)。
pub async fn chunk_relay(
    a: BoxedIo,
    b: BoxedIo,
    limiter: Option<Arc<dyn crate::ChunkLimiter>>,
    cancel: Option<crate::CancelFuture>,
) {
    let (mut ar, aw) = split_side(a);
    let (mut br, bw) = split_side(b);
    let lim_ab = limiter.clone();
    let lim_ba = limiter;

    mux::run_relay_pair(
        bw,
        move |bw| Box::pin(async move { copy::copy_batched(&mut ar, bw, lim_ab).await.is_ok() }),
        |bw| Box::pin(bw.shutdown()),
        aw,
        move |aw| Box::pin(async move { copy::copy_batched(&mut br, aw, lim_ba).await.is_ok() }),
        |aw| Box::pin(aw.shutdown()),
        cancel,
    )
    .await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        compress_stream, plain_tcp, wrap_stream_as_client, wrap_stream_as_server, CancelFuture,
        ChunkLimiter, ChunkReservation, PrependedIo, MUX_STREAMS_UNLIMITED,
    };
    use futures::FutureExt as _;
    use std::io::{self, IoSlice};
    use std::pin::Pin;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Mutex;
    use std::task::{Context, Poll};
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt, ReadBuf};

    struct DropIo {
        inner: tokio::io::DuplexStream,
        dropped: Arc<AtomicBool>,
    }

    impl Drop for DropIo {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::SeqCst);
        }
    }

    impl AsyncRead for DropIo {
        fn poll_read(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            Pin::new(&mut self.inner).poll_read(cx, buf)
        }
    }

    impl AsyncWrite for DropIo {
        fn poll_write(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            Pin::new(&mut self.inner).poll_write(cx, buf)
        }
        fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Pin::new(&mut self.inner).poll_flush(cx)
        }
        fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Pin::new(&mut self.inner).poll_shutdown(cx)
        }
    }

    struct PendingShutdownIo {
        inner: tokio::io::DuplexStream,
        dropped: Arc<AtomicBool>,
    }

    impl Drop for PendingShutdownIo {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::SeqCst);
        }
    }

    impl AsyncRead for PendingShutdownIo {
        fn poll_read(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            Pin::new(&mut self.inner).poll_read(cx, buf)
        }
    }

    impl AsyncWrite for PendingShutdownIo {
        fn poll_write(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            Pin::new(&mut self.inner).poll_write(cx, buf)
        }
        fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Pin::new(&mut self.inner).poll_flush(cx)
        }
        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Pending
        }
    }

    struct WriteErrShutdownPendIo {
        inner: tokio::io::DuplexStream,
        dropped: Arc<AtomicBool>,
    }

    impl Drop for WriteErrShutdownPendIo {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::SeqCst);
        }
    }

    impl AsyncRead for WriteErrShutdownPendIo {
        fn poll_read(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            Pin::new(&mut self.inner).poll_read(cx, buf)
        }
    }

    impl AsyncWrite for WriteErrShutdownPendIo {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            _buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "injected write error",
            )))
        }
        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Pending
        }
    }

    struct CountingLimiter {
        charged: AtomicUsize,
        refunded: Arc<AtomicUsize>,
    }

    impl CountingLimiter {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                charged: AtomicUsize::new(0),
                refunded: Arc::new(AtomicUsize::new(0)),
            })
        }
        fn net_consumed(&self) -> usize {
            self.charged.load(Ordering::SeqCst) - self.refunded.load(Ordering::SeqCst)
        }
    }

    struct RefundingReservation {
        remaining: usize,
        refunded: Arc<AtomicUsize>,
    }

    impl ChunkReservation for RefundingReservation {
        fn consume(&mut self, n: usize) {
            self.remaining = self.remaining.saturating_sub(n);
        }
    }

    impl Drop for RefundingReservation {
        fn drop(&mut self) {
            if self.remaining > 0 {
                self.refunded.fetch_add(self.remaining, Ordering::SeqCst);
            }
        }
    }

    impl ChunkLimiter for CountingLimiter {
        fn reserve(
            &self,
            n: usize,
        ) -> futures::future::BoxFuture<'static, Box<dyn ChunkReservation>> {
            self.charged.fetch_add(n, Ordering::SeqCst);
            let refunded = self.refunded.clone();
            Box::pin(async move {
                Box::new(RefundingReservation {
                    remaining: n,
                    refunded,
                }) as Box<dyn ChunkReservation>
            })
        }
    }

    async fn tcp_pair() -> (tokio::net::TcpStream, tokio::net::TcpStream) {
        let ln = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = ln.local_addr().unwrap();
        let c = tokio::net::TcpStream::connect(addr).await.unwrap();
        let (s, _) = ln.accept().await.unwrap();
        (c, s)
    }

    #[tokio::test]
    async fn chunk_relay_bidirectional_transfer_and_half_close() {
        let (mut a_peer, a_relay) = tcp_pair().await;
        let (b_relay, mut b_peer) = tcp_pair().await;

        let relay = tokio::spawn(chunk_relay(
            Box::new(a_relay),
            Box::new(b_relay),
            None,
            None,
        ));

        let payload_ab: Vec<u8> = (0..256 * 1024).map(|i| (i % 251) as u8).collect();
        let payload_ba: Vec<u8> = (0..128 * 1024).map(|i| ((i * 7 + 3) % 247) as u8).collect();

        let expected_ab = payload_ab.clone();
        let expected_ba = payload_ba.clone();

        let (mut a_r, mut a_w) = a_peer.split();
        let (mut b_r, mut b_w) = b_peer.split();

        let side_a = async {
            a_w.write_all(&payload_ab).await.unwrap();
            a_w.shutdown().await.unwrap();
            let mut got = Vec::new();
            a_r.read_to_end(&mut got).await.unwrap();
            got
        };
        let side_b = async {
            let mut got = Vec::new();
            b_r.read_to_end(&mut got).await.unwrap();
            b_w.write_all(&payload_ba).await.unwrap();
            b_w.shutdown().await.unwrap();
            got
        };

        let (got_ba, got_ab) = tokio::time::timeout(Duration::from_secs(5), async {
            tokio::join!(side_a, side_b)
        })
        .await
        .expect("chunk_relay bidirectional half-close timeout");

        assert_eq!(got_ab, expected_ab);
        assert_eq!(got_ba, expected_ba);
        tokio::time::timeout(Duration::from_secs(5), relay)
            .await
            .expect("chunk_relay finish timeout")
            .unwrap();
    }

    #[tokio::test]
    async fn chunk_relay_cancel_converges_bounded() {
        // 1) 运行中取消(含 PendingShutdownIo 永久卡住 shutdown)
        let (mut a_peer, a_relay) = tokio::io::duplex(4096);
        let (_b_peer, b_relay) = tokio::io::duplex(4096);
        let drop_a = Arc::new(AtomicBool::new(false));
        let drop_b = Arc::new(AtomicBool::new(false));
        let io_a: BoxedIo = Box::new(DropIo {
            inner: a_relay,
            dropped: drop_a.clone(),
        });
        let io_b: BoxedIo = Box::new(PendingShutdownIo {
            inner: b_relay,
            dropped: drop_b.clone(),
        });
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let cancel: CancelFuture = async move {
            let _ = rx.await;
        }
        .boxed()
        .shared();

        let mut relay = tokio::spawn(chunk_relay(io_a, io_b, None, Some(cancel)));
        a_peer.write_u8(1).await.unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
        tx.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(3), &mut relay)
            .await
            .expect("cancel 后必须在 CANCEL_CLOSE_GRACE 内收敛")
            .unwrap();
        assert!(drop_a.load(Ordering::SeqCst) && drop_b.load(Ordering::SeqCst));

        // 2) clean EOF 进入 shutdown 收尾后才抵达取消(cancel-after-eof)
        let (mut a2_peer, a2_relay) = tokio::io::duplex(4096);
        let (_b2_peer, b2_relay) = tokio::io::duplex(4096);
        let drop_b2 = Arc::new(AtomicBool::new(false));
        let io_b2: BoxedIo = Box::new(PendingShutdownIo {
            inner: b2_relay,
            dropped: drop_b2.clone(),
        });
        let (tx2, rx2) = tokio::sync::oneshot::channel::<()>();
        let cancel2: CancelFuture = async move {
            let _ = rx2.await;
        }
        .boxed()
        .shared();

        let mut relay2 = tokio::spawn(chunk_relay(Box::new(a2_relay), io_b2, None, Some(cancel2)));
        a2_peer.shutdown().await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        tx2.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(3), &mut relay2)
            .await
            .expect("EOF 收尾期间的取消必须收敛")
            .unwrap();
        assert!(drop_b2.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn chunk_relay_error_converges_and_aborts_sibling() {
        let (mut a_peer, a_relay) = tokio::io::duplex(4096);
        let (_b_peer, b_relay) = tokio::io::duplex(4096);
        let drop_a = Arc::new(AtomicBool::new(false));
        let drop_b = Arc::new(AtomicBool::new(false));
        let io_a: BoxedIo = Box::new(DropIo {
            inner: a_relay,
            dropped: drop_a.clone(),
        });
        let io_b: BoxedIo = Box::new(WriteErrShutdownPendIo {
            inner: b_relay,
            dropped: drop_b.clone(),
        });

        let mut relay = tokio::spawn(chunk_relay(io_a, io_b, None, None));
        a_peer.write_all(b"boom").await.unwrap();
        tokio::time::timeout(Duration::from_secs(3), &mut relay)
            .await
            .expect("写错误且 shutdown Pending 时必须在 CANCEL_CLOSE_GRACE 内收敛并中止静默对向")
            .unwrap();
        assert!(drop_a.load(Ordering::SeqCst));
        assert!(drop_b.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn chunk_relay_clean_eof_allows_late_peer_data() {
        let (mut a_peer, a_relay) = tokio::io::duplex(64 * 1024);
        let (b_relay, mut b_peer) = tokio::io::duplex(64 * 1024);

        let relay = tokio::spawn(chunk_relay(
            Box::new(a_relay),
            Box::new(b_relay),
            None,
            None,
        ));

        // A 侧立即发送 EOF,B 侧延迟 120ms 后才写入迟到响应并关闭
        a_peer.shutdown().await.unwrap();
        let mut b_buf = Vec::new();
        b_peer.read_to_end(&mut b_buf).await.unwrap();
        assert!(b_buf.is_empty());

        tokio::time::sleep(Duration::from_millis(120)).await;
        b_peer.write_all(b"late-peer-response").await.unwrap();
        b_peer.shutdown().await.unwrap();

        let mut a_buf = Vec::new();
        tokio::time::timeout(Duration::from_secs(2), a_peer.read_to_end(&mut a_buf))
            .await
            .expect("迟到对端数据必须在半关闭期间完整送达")
            .unwrap();
        assert_eq!(&a_buf, b"late-peer-response");
        tokio::time::timeout(Duration::from_secs(2), relay)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn chunk_relay_clean_eof_bounds_never_ending_peer() {
        let (mut a_peer, a_relay) = tokio::io::duplex(4096);
        let (b_relay, mut b_peer) = tokio::io::duplex(4096);
        let drop_b = Arc::new(AtomicBool::new(false));
        let io_b: BoxedIo = Box::new(DropIo {
            inner: b_relay,
            dropped: drop_b.clone(),
        });

        let mut relay = tokio::spawn(chunk_relay(Box::new(a_relay), io_b, None, None));
        a_peer.shutdown().await.unwrap();
        let mut sink = Vec::new();
        b_peer.read_to_end(&mut sink).await.unwrap();

        // b_peer 永不关闭写端(模拟半关闭泄漏场景);start_paused 下必须在有界 grace 内收敛
        tokio::time::timeout(Duration::from_secs(45), &mut relay)
            .await
            .expect("对端永不关闭时 chunk_relay 必须在 halfclose_grace 内有界收敛")
            .unwrap();
        assert!(drop_b.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn chunk_relay_limiter_metering_equivalence() {
        async fn copy_64k_baseline<R: AsyncRead + Unpin, W: AsyncWrite + Unpin>(
            r: &mut R,
            w: &mut W,
            lim: Arc<CountingLimiter>,
        ) {
            let mut buf = vec![0u8; 64 * 1024];
            loop {
                match r.read(&mut buf).await {
                    Ok(0) | Err(_) => break,
                    Ok(n) => {
                        let mut res = lim.reserve(n).await;
                        res.consume(n);
                        if w.write_all(&buf[..n]).await.is_err() {
                            break;
                        }
                    }
                }
            }
            let _ = w.shutdown().await;
        }

        let payload_ab: Vec<u8> = (0..180 * 1024).map(|i| (i % 251) as u8).collect();
        let payload_ba: Vec<u8> = (0..95 * 1024).map(|i| ((i * 3 + 1) % 239) as u8).collect();

        // 1) 新泵 chunk_relay
        let lim_new = CountingLimiter::new();
        let (a1, a1_r) = tokio::io::duplex(64 * 1024);
        let (b1_r, b1) = tokio::io::duplex(64 * 1024);
        let r1 = tokio::spawn(chunk_relay(
            Box::new(a1_r),
            Box::new(b1_r),
            Some(lim_new.clone()),
            None,
        ));
        let p_ab = payload_ab.clone();
        let p_ba = payload_ba.clone();
        let (mut a1_rd, mut a1_wr) = tokio::io::split(a1);
        let (mut b1_rd, mut b1_wr) = tokio::io::split(b1);
        let (_, _, got_ba, got_ab) = tokio::join!(
            async {
                a1_wr.write_all(&p_ab).await.unwrap();
                a1_wr.shutdown().await.unwrap();
            },
            async {
                b1_wr.write_all(&p_ba).await.unwrap();
                b1_wr.shutdown().await.unwrap();
            },
            async {
                let mut v = Vec::new();
                a1_rd.read_to_end(&mut v).await.unwrap();
                v
            },
            async {
                let mut v = Vec::new();
                b1_rd.read_to_end(&mut v).await.unwrap();
                v
            }
        );
        r1.await.unwrap();
        assert_eq!(got_ab, payload_ab);
        assert_eq!(got_ba, payload_ba);

        // 2) 旧 64KiB copy 泵对照
        let lim_old = CountingLimiter::new();
        let (a2, a2_r) = tokio::io::duplex(64 * 1024);
        let (b2_r, b2) = tokio::io::duplex(64 * 1024);
        let (mut a2_rr, mut a2_rw) = tokio::io::split(a2_r);
        let (mut b2_rr, mut b2_rw) = tokio::io::split(b2_r);
        let l_old_1 = lim_old.clone();
        let l_old_2 = lim_old.clone();
        let t_old_1 = tokio::spawn(async move {
            copy_64k_baseline(&mut a2_rr, &mut b2_rw, l_old_1).await;
        });
        let t_old_2 = tokio::spawn(async move {
            copy_64k_baseline(&mut b2_rr, &mut a2_rw, l_old_2).await;
        });
        let (mut a2_rd, mut a2_wr) = tokio::io::split(a2);
        let (mut b2_rd, mut b2_wr) = tokio::io::split(b2);
        let _ = tokio::join!(
            async {
                a2_wr.write_all(&payload_ab).await.unwrap();
                a2_wr.shutdown().await.unwrap();
            },
            async {
                b2_wr.write_all(&payload_ba).await.unwrap();
                b2_wr.shutdown().await.unwrap();
            },
            async {
                let mut v = Vec::new();
                a2_rd.read_to_end(&mut v).await.unwrap();
                v
            },
            async {
                let mut v = Vec::new();
                b2_rd.read_to_end(&mut v).await.unwrap();
                v
            }
        );
        t_old_1.await.unwrap();
        t_old_2.await.unwrap();

        assert_eq!(
            lim_new.net_consumed(),
            lim_old.net_consumed(),
            "chunk_relay 限速净扣费必须与 64KiB 旧泵等价"
        );
        assert_eq!(lim_new.net_consumed(), payload_ab.len() + payload_ba.len());
    }

    #[tokio::test]
    async fn chunk_relay_coalesces_writev() {
        struct ReadyReader(Vec<Vec<u8>>);
        impl AsyncRead for ReadyReader {
            fn poll_read(
                mut self: Pin<&mut Self>,
                _cx: &mut Context<'_>,
                buf: &mut ReadBuf<'_>,
            ) -> Poll<io::Result<()>> {
                if self.0.is_empty() {
                    return Poll::Ready(Ok(()));
                }
                let head = self.0.remove(0);
                let n = head.len().min(buf.remaining());
                buf.put_slice(&head[..n]);
                if n < head.len() {
                    self.0.insert(0, head[n..].to_vec());
                }
                Poll::Ready(Ok(()))
            }
        }
        impl AsyncWrite for ReadyReader {
            fn poll_write(
                self: Pin<&mut Self>,
                _cx: &mut Context<'_>,
                buf: &[u8],
            ) -> Poll<io::Result<usize>> {
                Poll::Ready(Ok(buf.len()))
            }
            fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
                Poll::Ready(Ok(()))
            }
            fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
                Poll::Ready(Ok(()))
            }
        }

        #[derive(Default)]
        struct SinkLog {
            max_segs: usize,
            written: Vec<u8>,
        }
        struct RecordingSink(Arc<Mutex<SinkLog>>);
        impl AsyncRead for RecordingSink {
            fn poll_read(
                self: Pin<&mut Self>,
                _cx: &mut Context<'_>,
                _buf: &mut ReadBuf<'_>,
            ) -> Poll<io::Result<()>> {
                Poll::Ready(Ok(()))
            }
        }
        impl AsyncWrite for RecordingSink {
            fn poll_write(
                self: Pin<&mut Self>,
                _cx: &mut Context<'_>,
                buf: &[u8],
            ) -> Poll<io::Result<usize>> {
                self.0.lock().unwrap().written.extend_from_slice(buf);
                Poll::Ready(Ok(buf.len()))
            }
            fn poll_write_vectored(
                self: Pin<&mut Self>,
                _cx: &mut Context<'_>,
                bufs: &[IoSlice<'_>],
            ) -> Poll<io::Result<usize>> {
                let mut st = self.0.lock().unwrap();
                st.max_segs = st.max_segs.max(bufs.len());
                let mut n = 0;
                for s in bufs {
                    st.written.extend_from_slice(s);
                    n += s.len();
                }
                Poll::Ready(Ok(n))
            }
            fn is_write_vectored(&self) -> bool {
                true
            }
            fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
                Poll::Ready(Ok(()))
            }
            fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
                Poll::Ready(Ok(()))
            }
        }

        let log = Arc::new(Mutex::new(SinkLog::default()));
        let src: BoxedIo = Box::new(ReadyReader(vec![
            vec![1u8; 512],
            vec![2u8; 512],
            vec![3u8; 512],
        ]));
        let dst: BoxedIo = Box::new(RecordingSink(log.clone()));
        chunk_relay(src, dst, None, None).await;

        let st = log.lock().unwrap();
        assert_eq!(st.written.len(), 1536);
        assert!(
            st.max_segs >= 3,
            "多段应聚合进单次 writev(max_segs={})",
            st.max_segs
        );
    }

    #[tokio::test]
    async fn plain_tcp_truth_table() {
        let (tcp, _peer) = tcp_pair().await;
        let tcp_box: BoxedIo = Box::new(tcp);
        assert!(
            plain_tcp(&tcp_box).is_some(),
            "裸 TcpStream 必须命中 plain_tcp"
        );

        // PrependedIo(TcpStream) -> None
        let prepended = PrependedIo::wrap(tcp_box, b"abc".to_vec());
        assert!(
            plain_tcp(&prepended).is_none(),
            "PrependedIo 包装不得命中 plain_tcp"
        );

        // DuplexStream -> None
        let (dup, _dup_peer) = tokio::io::duplex(64);
        let dup_box: BoxedIo = Box::new(dup);
        assert!(
            plain_tcp(&dup_box).is_none(),
            "DuplexStream 不得命中 plain_tcp"
        );

        // compress_stream(TcpStream) -> None
        let (tcp2, _peer2) = tcp_pair().await;
        let compressed = compress_stream(Box::new(tcp2)).await;
        assert!(
            plain_tcp(&compressed).is_none(),
            "compress_stream 产物不得命中 plain_tcp"
        );

        // RhMuxStream -> None
        let (m1, m2) = tokio::io::duplex(64 * 1024);
        let srv = wrap_stream_as_server(Box::new(m2), true, MUX_STREAMS_UNLIMITED);
        let cli = wrap_stream_as_client(Box::new(m1), true, MUX_STREAMS_UNLIMITED);
        let mux_stream = cli.open_stream().await.unwrap();
        assert!(
            plain_tcp(&mux_stream).is_none(),
            "RhMuxStream 不得命中 plain_tcp"
        );
        drop(srv);

        #[cfg(unix)]
        {
            let (u1, _u2) = tokio::net::UnixStream::pair().unwrap();
            let u_box: BoxedIo = Box::new(u1);
            assert!(plain_tcp(&u_box).is_none(), "UnixStream 不得命中 plain_tcp");
        }
    }
}
