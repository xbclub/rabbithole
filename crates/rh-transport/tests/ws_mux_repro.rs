//! P0 回归:ws 载体上跑 yamux,批量数据必须完整送达。
//! 对应 benchmark/runs/peer/REPORT.md 第六节(80KiB 处数据通道关闭)。
//! 管道带 0.4ms RTT 与逐块背压,严格 FIFO(字节流不允许乱序)。

use rh_transport::mux::{wrap_stream_as_client, wrap_stream_as_server, MUX_STREAMS_UNLIMITED};
use rh_transport::ws;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::{mpsc, Notify};

struct DelayPipe {
    deliver_rx: mpsc::Receiver<Vec<u8>>,
    queue: Arc<Mutex<VecDeque<Vec<u8>>>>,
    notify: Arc<Notify>,
    buf: Vec<u8>,
    pos: usize,
}

impl DelayPipe {
    /// pair() 返回 (a, b):a 的写延迟后到达 b 的读,反之亦然。
    fn pair(half_rtt: Duration) -> (Self, Self) {
        let (aq_tx, a_deliver_rx) = mpsc::channel::<Vec<u8>>(64);
        let (bq_tx, b_deliver_rx) = mpsc::channel::<Vec<u8>>(64);
        let aq: Arc<Mutex<VecDeque<Vec<u8>>>> = Arc::default();
        let bq: Arc<Mutex<VecDeque<Vec<u8>>>> = Arc::default();
        let an = Arc::new(Notify::new());
        let bn = Arc::new(Notify::new());

        // a 的写 → (延迟) → b 的读
        {
            let q = aq.clone();
            let n = an.clone();
            let tx = bq_tx;
            tokio::spawn(async move {
                loop {
                    let chunk = {
                        let mut g = q.lock().unwrap();
                        g.pop_front()
                    };
                    match chunk {
                        Some(c) => {
                            if c.is_empty() {
                                let _ = tx.send(c).await;
                                break;
                            }
                            tokio::time::sleep(half_rtt).await;
                            if tx.send(c).await.is_err() {
                                break;
                            }
                        }
                        None => n.notified().await,
                    }
                }
            });
        }
        // b 的写 → (延迟) → a 的读
        {
            let q = bq.clone();
            let n = bn.clone();
            let tx = aq_tx;
            tokio::spawn(async move {
                loop {
                    let chunk = {
                        let mut g = q.lock().unwrap();
                        g.pop_front()
                    };
                    match chunk {
                        Some(c) => {
                            if c.is_empty() {
                                let _ = tx.send(c).await;
                                break;
                            }
                            tokio::time::sleep(half_rtt).await;
                            if tx.send(c).await.is_err() {
                                break;
                            }
                        }
                        None => n.notified().await,
                    }
                }
            });
        }

        let a = DelayPipe {
            deliver_rx: a_deliver_rx,
            queue: aq,
            notify: an,
            buf: Vec::new(),
            pos: 0,
        };
        let b = DelayPipe {
            deliver_rx: b_deliver_rx,
            queue: bq,
            notify: bn,
            buf: Vec::new(),
            pos: 0,
        };
        (a, b)
    }
}

impl AsyncRead for DelayPipe {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        out: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        loop {
            if self.pos < self.buf.len() {
                let n = (self.buf.len() - self.pos).min(out.remaining());
                out.put_slice(&self.buf[self.pos..self.pos + n]);
                self.pos += n;
                return std::task::Poll::Ready(Ok(()));
            }
            match self.deliver_rx.poll_recv(cx) {
                std::task::Poll::Ready(Some(v)) => {
                    if v.is_empty() {
                        return std::task::Poll::Ready(Ok(())); // EOF
                    }
                    self.buf = v;
                    self.pos = 0;
                }
                std::task::Poll::Ready(None) => {
                    return std::task::Poll::Ready(Ok(()));
                }
                std::task::Poll::Pending => return std::task::Poll::Pending,
            }
        }
    }
}

impl AsyncWrite for DelayPipe {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        const MSS: usize = 1440;
        let n = buf.len().min(MSS);
        if n == 0 {
            return std::task::Poll::Ready(Ok(0));
        }
        let mut q = self.queue.lock().unwrap();
        q.push_back(buf[..n].to_vec());
        drop(q);
        self.notify.notify_one();
        std::task::Poll::Ready(Ok(n))
    }
    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let mut q = self.queue.lock().unwrap();
        q.push_back(Vec::new());
        drop(q);
        self.notify.notify_one();
        std::task::Poll::Ready(Ok(()))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ws_mux_bulk_transfer_survives() {
    let half = Duration::from_micros(200); // 0.4ms RTT
    let use_duplex = std::env::var("REPRO_DUPLEX").is_ok();
    let (a, b): (rh_transport::BoxedIo, rh_transport::BoxedIo) = if use_duplex {
        let (a, b) = tokio::io::duplex(64 * 1024);
        (Box::new(a), Box::new(b))
    } else {
        let (a, b) = DelayPipe::pair(half);
        (Box::new(a), Box::new(b))
    };

    let server = tokio::spawn(async move {
        let io = ws::upgrade_server(b, ws::WS_PATH).await.expect("upgrade");
        let mux = wrap_stream_as_server(io, true, MUX_STREAMS_UNLIMITED);
        let mut s = mux.accept().await.expect("accept");
        let mut expected: u64 = 0;
        let mut buf = vec![0u8; 64 * 1024];
        loop {
            match s.read(&mut buf).await {
                Ok(0) => break,
                Ok(n) => {
                    for (i, byte) in buf[..n].iter().enumerate() {
                        assert_eq!(
                            *byte,
                            ((expected + i as u64) & 0xff) as u8,
                            "corrupt at stream offset {} (got {:02x}, want {:02x})",
                            expected + i as u64,
                            *byte,
                            ((expected + i as u64) & 0xff) as u8
                        );
                    }
                    expected += n as u64;
                }
                Err(e) => {
                    panic!("server stream read error after {expected} bytes: {e}");
                }
            }
        }
        expected
    });

    let c = ws::client_handshake(a, "127.0.0.1", ws::WS_PATH)
        .await
        .expect("handshake");
    let mux = wrap_stream_as_client(Box::new(c), true, MUX_STREAMS_UNLIMITED);
    let mut s = mux.open_stream().await.expect("open stream");

    const TOTAL: u64 = 8 * 1024 * 1024;
    let mut sent: u64 = 0;
    let mut chunk = vec![0u8; 64 * 1024];
    while sent < TOTAL {
        for (i, b) in chunk.iter_mut().enumerate() {
            *b = ((sent + i as u64) & 0xff) as u8;
        }
        s.write_all(&chunk)
            .await
            .unwrap_or_else(|e| panic!("client write failed after {sent} bytes: {e}"));
        sent += chunk.len() as u64;
    }
    s.shutdown().await.expect("shutdown");
    let got = tokio::time::timeout(Duration::from_secs(120), server)
        .await
        .expect("server task timeout")
        .expect("server join");
    assert_eq!(got, TOTAL, "server received {got}, expected {TOTAL}");
}
