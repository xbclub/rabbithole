//! goal-repair P2 回归:mux chunk 中继生命周期与计量。
//!
//! 覆盖(方案 §6a):
//! * A1(a) `mux_relay_parent_cancel_kills_children`:父任务取消必须终止
//!   双向子泵(修前:spawn 后 join! 把 JoinHandle 移进 join future,
//!   父取消即 detach;审计 /tmp/rh-readonly-review.eFSs9d repro abort-parent
//!   ×3:io_dropped=false,abort 后写入的第 9 字节仍被转发)。
//! * A3 `pump_down_no_double_charge`:down 聚合块按 len 扣费后
//!   push_front 回退余量,外层循环对同一余量再扣(修前 ×3 复现:
//!   实际 98304B,扣费 131072B,多扣 32768B)。
//! * R6 `yamux_open_handles_released_serves_existing_streams`(红线 5 锁):
//!   全部开流请求句柄释放(open_rx 转 Ready(None))后,存量流仍可双向传输。
//! * `mux_relay_cancel_converges_when_close_queue_full`:载体写楔死 →
//!   yamux 命令队列满 → poll_close_half 永久 Pending;规则取消必须有界
//!   收敛;并锁 up 侧 fill 计费口径(楔死时 charged>0 而送达=0)。
//! * `mux_relay_error_path_converges_when_shutdown_pends`:mux 连接死亡
//!   (错误路径,非取消)且用户侧 poll_shutdown 永久 Pending 时有界收敛,
//!   对向泵不得在已死连接上永存。
//! * `mux_relay_sibling_aborted_on_pump_panic`:单向子泵 panic 时对向泵
//!   立即终止(守卫吸收 JoinError,但不允许对向永存)。

use futures::FutureExt;
use rh_transport::{mux, BoxedIo, CancelFuture, ChunkLimiter, ChunkReservation};
use std::io;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};

/// 计数限速器(预留/消费/退款三态;净扣费 = charged − refunded)。
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
}

/// 预留句柄:consume 减 remaining;drop 把未消费余量计为退款。
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
    fn reserve(&self, n: usize) -> futures::future::BoxFuture<'static, Box<dyn ChunkReservation>> {
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

/// Drop 探针 IO:被中继持有的连接在子泵终止(所有权释放)时翻转标志。
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
        b: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, b)
    }
}

impl AsyncWrite for DropIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        b: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, b)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

/// 带截止时间的轮询等待探针翻转。
async fn wait_flag(f: &Arc<AtomicBool>, what: &str) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while !f.load(Ordering::SeqCst) {
        if tokio::time::Instant::now() >= deadline {
            panic!("{what}: 探针在 2s 内未翻转(子泵未随父取消终止)");
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

// A1(a):父任务取消 → 双向子泵终止 → 中继持有的用户侧 IO 释放;
/// 随后写入的探测字节不得再被转发。
#[tokio::test]
async fn mux_relay_parent_cancel_kills_children() {
    let (wire_a, wire_b) = tokio::io::duplex(1024 * 1024);
    let client = rh_transport::wrap_stream_as_client(Box::new(wire_a), true, 100);
    let server = rh_transport::wrap_stream_as_server(Box::new(wire_b), true, 100);
    let mut c = client.open_stream().await.unwrap();
    c.write_u8(1).await.unwrap();
    let mut s = server.accept().await.unwrap();
    assert_eq!(s.read_u8().await.unwrap(), 1);
    let (r, w) = mux::steal_mux_halves(&mut s).unwrap();
    drop(s); // 壳

    let (mut local, relay_io) = tokio::io::duplex(4096);
    let dropped = Arc::new(AtomicBool::new(false));
    let io: BoxedIo = Box::new(DropIo {
        inner: relay_io,
        dropped: dropped.clone(),
    });
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let cancel: CancelFuture = async move {
        let _ = rx.await;
    }
    .boxed()
    .shared();
    let relay = tokio::spawn(mux::mux_relay(io, r, w, None, Some(cancel)));

    // 先建立流(第 7 字节经中继到达),确保子泵已运行
    local.write_u8(7).await.unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), c.read_u8())
            .await
            .unwrap()
            .unwrap(),
        7
    );

    // 父任务取消(会话拆除形态)
    relay.abort();
    assert!(relay.await.unwrap_err().is_cancelled());
    // 先等探针翻转(子泵所有权释放),再写探测字节
    wait_flag(&dropped, "mux_relay io").await;

    // 窗口期断言:取消后写入的第 9 字节不得被转发。
    // 探针已翻转 ⇒ 中继侧 duplex 已释放 ⇒ 写入应 BrokenPipe
    // (比"写入成功但不被转发"更强的保证);防御性兼容写入成功路径。
    match local.write_u8(9).await {
        Err(e) if e.kind() == std::io::ErrorKind::BrokenPipe => {}
        Err(e) => panic!("探测写异常: {e}"),
        Ok(()) => match tokio::time::timeout(Duration::from_millis(300), c.read_u8()).await {
            Err(_) => {}
            Ok(Ok(b)) => panic!("取消后字节 {b} 仍被转发(detach 泄漏)"),
            Ok(Err(e)) => panic!("读侧异常: {e}"),
        },
    }
    let _ = tx.send(());
}

// A3:down 聚合回退不得重复扣费(无取消全量运行:总扣费==载荷)。
// 修前证据(before-fix/rh-transport-tests.log):扣费 131072 vs 载荷 98304。
#[tokio::test]
async fn pump_down_no_double_charge() {
    let (wire_a, wire_b) = tokio::io::duplex(1024 * 1024);
    let client = rh_transport::wrap_stream_as_client(Box::new(wire_a), true, 100);
    let server = rh_transport::wrap_stream_as_server(Box::new(wire_b), true, 100);
    let mut c = client.open_stream().await.unwrap();
    c.write_all(&vec![11u8; 48 * 1024]).await.unwrap();
    c.write_all(&vec![22u8; 48 * 1024]).await.unwrap();
    c.shutdown().await.unwrap();
    let mut s = server.accept().await.unwrap();
    // 等帧到达(48KiB+48KiB 两个写 → 多帧),确保聚合回退路径可触发
    tokio::time::sleep(Duration::from_millis(30)).await;
    let (r, w) = mux::steal_mux_halves(&mut s).unwrap();
    drop(s);

    let (mut local, relay_io) = tokio::io::duplex(1024 * 1024);
    let limiter = CountingLimiter::new();
    let relay = tokio::spawn(mux::mux_relay(
        Box::new(relay_io),
        r,
        w,
        Some(limiter.clone() as Arc<dyn ChunkLimiter>),
        None,
    ));
    let mut output = Vec::new();
    tokio::time::timeout(Duration::from_secs(2), local.read_to_end(&mut output))
        .await
        .unwrap()
        .unwrap();
    use tokio::io::AsyncWriteExt as _;
    local.shutdown().await.unwrap(); // 让 up 泵收敛(与审计 main.rs 同款)
    tokio::time::timeout(Duration::from_secs(2), relay)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(output.len(), 96 * 1024, "载荷必须完整送达");
    assert!(output[..48 * 1024].iter().all(|&b| b == 11));
    assert!(output[48 * 1024..].iter().all(|&b| b == 22));
    let charged = limiter.charged.load(Ordering::SeqCst);
    let refunded = limiter.refunded.load(Ordering::SeqCst);
    assert_eq!(
        charged - refunded,
        output.len(),
        "净扣费必须等于已送达载荷(不得重复扣费;charged={charged} refunded={refunded})"
    );
}

// A5(限速等待取消,down 送达计费合同):预留等待期间取消 →
// 在途预留全额退款,净扣费 == 已送达字节(此处 == 0,尚无送达)。
#[tokio::test]
async fn mux_rate_wait_cancel_refunds() {
    let refunded = Arc::new(AtomicUsize::new(0));
    struct NeverLimiter(Arc<AtomicUsize>);
    impl ChunkLimiter for NeverLimiter {
        fn reserve(
            &self,
            n: usize,
        ) -> futures::future::BoxFuture<'static, Box<dyn ChunkReservation>> {
            let refunded = self.0.clone();
            let r2 = refunded.clone();
            Box::pin(async move {
                // 已"扣" n 但永不返回(模拟极小令牌桶的等待)
                struct Never(usize, Arc<AtomicUsize>);
                impl ChunkReservation for Never {
                    fn consume(&mut self, _n: usize) {}
                }
                impl Drop for Never {
                    fn drop(&mut self) {
                        self.1.fetch_add(self.0, Ordering::SeqCst);
                    }
                }
                Box::new(Never(n, r2)) as Box<dyn ChunkReservation>
            })
        }
    }

    let (wire_a, wire_b) = tokio::io::duplex(256 * 1024);
    let client = rh_transport::wrap_stream_as_client(Box::new(wire_a), true, 100);
    let server = rh_transport::wrap_stream_as_server(Box::new(wire_b), true, 100);
    let mut c = client.open_stream().await.unwrap();
    c.write_u8(0xAB).await.unwrap();
    let mut s = server.accept().await.unwrap();
    let (r, w) = mux::steal_mux_halves(&mut s).unwrap();
    drop(s);

    let (_local, relay_io) = tokio::io::duplex(1024);
    let relay = tokio::spawn(mux::mux_relay(
        Box::new(relay_io),
        r,
        w,
        Some(Arc::new(NeverLimiter(refunded.clone()))),
        None,
    ));
    // 字节已 fill(down 读到块)但预留永不完成为等待;父取消必须生效
    tokio::time::sleep(Duration::from_millis(50)).await;
    relay.abort();
    tokio::time::timeout(Duration::from_secs(1), relay)
        .await
        .unwrap()
        .unwrap_err();
    // A5 退款合同(兑现注释声明):等待中取消 → 在途预留全额退还,
    // 净扣费 == 已送达字节 == 0 —— NeverLimiter 的 Drop 记 refunded
    assert_eq!(
        refunded.load(Ordering::SeqCst),
        1,
        "取消时在途预留必须全额退款(净扣费=已送达=0)"
    );
}

// R6(红线 5 锁):open_tx 尽(open_rx.poll_recv → Ready(None))后,
/// driver 仍服务存量流直至载体/IO 结束。
#[tokio::test]
async fn yamux_open_handles_released_serves_existing_streams() {
    use rh_transport::MUX_STREAMS_UNLIMITED;

    let (a, b) = tokio::io::duplex(256 * 1024);
    let server = rh_transport::wrap_stream_as_server(Box::new(b), true, MUX_STREAMS_UNLIMITED);
    let client = rh_transport::wrap_stream_as_client(Box::new(a), true, MUX_STREAMS_UNLIMITED);

    let mut cli = client.open_stream().await.unwrap();
    cli.write_all(b"hello").await.unwrap();
    let mut srv = server.accept().await.unwrap();
    let mut buf = [0u8; 5];
    srv.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf, b"hello");

    // 释放全部 Multiplexer 句柄(开流/收流请求通道随之耗尽)
    drop(client);
    drop(server);
    tokio::time::sleep(Duration::from_millis(20)).await;

    // 存量流必须仍可双向传数据
    cli.write_all(b"ping2").await.unwrap();
    let mut buf2 = [0u8; 5];
    srv.read_exact(&mut buf2).await.unwrap();
    assert_eq!(&buf2, b"ping2");
    srv.write_all(b"pong2").await.unwrap();
    let mut buf3 = [0u8; 5];
    cli.read_exact(&mut buf3).await.unwrap();
    assert_eq!(&buf3, b"pong2");
}

/// poll_shutdown 永久 Pending 的用户侧 IO(主代理 cancel-close 复现同款:
/// /tmp/rh-main-independent-review/src/main.rs PendingShutdownIo)——模拟
/// 对端不读 + 发送缓冲满等 shutdown 永不就绪的真实形态。
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
        b: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, b)
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

// 规则取消(禁用/删除级联 CancelFuture)后中继必须收敛退出并释放 IO,
// 即使收尾的 shutdown/close 永久 Pending。父 abort 守卫(A1a)不覆盖本路径:
// 父任务仍在 join 等待,卡死的是子泵收尾。
// 修前证据:/tmp/rh-main-independent-review/cancel-close-before.log
// (cancel_close: timed_out=true io_dropped=false,真实退出 101)。
#[tokio::test]
async fn mux_relay_cancel_close_converges() {
    let (wire_a, wire_b) = tokio::io::duplex(4096);
    let client = rh_transport::wrap_stream_as_client(Box::new(wire_a), true, 100);
    let server = rh_transport::wrap_stream_as_server(Box::new(wire_b), true, 100);
    let mut c = client.open_stream().await.unwrap();
    c.write_u8(1).await.unwrap();
    let mut s = server.accept().await.unwrap();
    assert_eq!(s.read_u8().await.unwrap(), 1);
    let (r, w) = mux::steal_mux_halves(&mut s).unwrap();
    drop(s); // 壳

    let (mut local, relay_io) = tokio::io::duplex(4096);
    let dropped = Arc::new(AtomicBool::new(false));
    let io: BoxedIo = Box::new(PendingShutdownIo {
        inner: relay_io,
        dropped: dropped.clone(),
    });
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let cancel: CancelFuture = async move {
        let _ = rx.await;
    }
    .boxed()
    .shared();
    let mut relay = tokio::spawn(mux::mux_relay(io, r, w, None, Some(cancel)));

    // 先建立流(第 7 字节经中继到达),确保子泵已运行
    local.write_u8(7).await.unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), c.read_u8())
            .await
            .unwrap()
            .unwrap(),
        7
    );

    // 触发规则取消
    tx.send(()).unwrap();
    // 3s 内必须收敛退出(修前:down 泵收尾 a_w.shutdown() 永久 Pending,
    // join 永不完成、IO 不释放)
    tokio::time::timeout(Duration::from_secs(3), &mut relay)
        .await
        .expect("规则取消后 mux_relay 必须收敛退出(即使 poll_shutdown 永久 Pending)")
        .unwrap();
    assert!(dropped.load(Ordering::SeqCst), "规则取消必须释放用户侧 IO");
}

// 故障注入:取消在 EOF 收尾期间才抵达(cancel-after-eof,主代理复现同款:
// /tmp/rh-main-independent-review/src/main.rs cancel_after_eof)。泵先以
// clean EOF 结束进入 shutdown/close 收尾后,规则取消必须仍被听见——
// 收尾在 CANCEL_CLOSE_GRACE 内收敛并释放 IO;未取消的正常 EOF 不加
// 硬超时(半关闭红线)。修前:by_cancel 只在泵循环 select 记录,收尾
// 阶段永不再监听取消 → relay 卡死、IO 不释放(修前 3s 超时 exit 101)。
#[tokio::test]
async fn mux_relay_cancel_after_eof_converges() {
    let (wire_a, wire_b) = tokio::io::duplex(4096);
    let client = rh_transport::wrap_stream_as_client(Box::new(wire_a), true, 100);
    let server = rh_transport::wrap_stream_as_server(Box::new(wire_b), true, 100);
    let mut c = client.open_stream().await.unwrap();
    c.write_u8(1).await.unwrap();
    let mut s = server.accept().await.unwrap();
    assert_eq!(s.read_u8().await.unwrap(), 1);
    let (r, w) = mux::steal_mux_halves(&mut s).unwrap();
    drop(s); // 壳

    let (mut local, relay_io) = tokio::io::duplex(4096);
    let dropped = Arc::new(AtomicBool::new(false));
    let io: BoxedIo = Box::new(PendingShutdownIo {
        inner: relay_io,
        dropped: dropped.clone(),
    });
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let cancel: CancelFuture = async move {
        let _ = rx.await;
    }
    .boxed()
    .shared();
    let mut relay = tokio::spawn(mux::mux_relay(io, r, w, None, Some(cancel)));

    // 中继建立(第 7 字节经中继到达)
    local.write_u8(7).await.unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), c.read_u8())
            .await
            .unwrap()
            .unwrap(),
        7
    );
    // 对端 FIN → down 泵 clean EOF 进入收尾(a_w.shutdown 永久 Pending);
    // 等收尾确实挂起后再触发取消(与主代理复现同节奏)
    c.shutdown().await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    tx.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(3), &mut relay)
        .await
        .expect("EOF 收尾期间抵达的取消必须仍被听见并收敛")
        .unwrap();
    assert!(
        dropped.load(Ordering::SeqCst),
        "EOF 收尾取消必须释放用户侧 IO"
    );
}

/// 可楔死载体:置位 wedged 后 poll_write 恒 Pending(读透传)——模拟
/// 内核发送缓冲满 + 对端不读的真实形态。yamux 驱动写帧楔死后不再消费
/// 流命令,命令队列必然填满,poll_close_half 的 poll_ready 永不就绪
///(close 无法入队)。注:必须连接建立后再楔死——vendor poll 循环在
/// pending 控制帧(Pong 等)未写穿前不读入站帧,自始楔死会饿死 accept。
struct WedgeableIo {
    inner: tokio::io::DuplexStream,
    wedged: Arc<AtomicBool>,
}

impl AsyncRead for WedgeableIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        b: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, b)
    }
}

impl AsyncWrite for WedgeableIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        if self.wedged.load(Ordering::SeqCst) {
            return Poll::Pending;
        }
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

// 故障注入:close 命令队列满(poll_close_half 永久 Pending)+ 规则取消。
// 修前(无 CANCEL_CLOSE_GRACE):取消后 b_write.close() 无界等待,
// mux_relay 永不返回、IO 永不释放。
// 同时锁 up 侧 fill 计费口径:楔死时已读字节已扣费(charged>0)而送达为 0
// ——读侧计量(fill)≠ 目的端送达,二者不得混同。
#[tokio::test]
async fn mux_relay_cancel_converges_when_close_queue_full() {
    let (wire_a, wire_b) = tokio::io::duplex(1024 * 1024);
    let client = rh_transport::wrap_stream_as_client(Box::new(wire_a), true, 100);
    let wedged = Arc::new(AtomicBool::new(false));
    let server = rh_transport::wrap_stream_as_server(
        Box::new(WedgeableIo {
            inner: wire_b,
            wedged: wedged.clone(),
        }),
        true,
        100,
    );
    let mut c = client.open_stream().await.unwrap();
    c.write_u8(1).await.unwrap();
    let mut s = server.accept().await.unwrap();
    assert_eq!(s.read_u8().await.unwrap(), 1);
    let (r, w) = mux::steal_mux_halves(&mut s).unwrap();
    drop(s); // 壳

    let (mut local, relay_io) = tokio::io::duplex(1024 * 1024);
    let dropped = Arc::new(AtomicBool::new(false));
    let io: BoxedIo = Box::new(DropIo {
        inner: relay_io,
        dropped: dropped.clone(),
    });
    let limiter = CountingLimiter::new();
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let cancel: CancelFuture = async move {
        let _ = rx.await;
    }
    .boxed()
    .shared();
    let mut relay = tokio::spawn(mux::mux_relay(
        io,
        r,
        w,
        Some(limiter.clone()),
        Some(cancel),
    ));

    // 连接与中继已建立,现在楔死服务端载体写(此后 server→client
    // 帧永不落地,驱动写帧即堵)
    wedged.store(true, Ordering::SeqCst);

    // 逐块喂养直到写侧楔死:charge 停止增长 = write_chunk 已在
    // poll_ready 上 Pending(命令队列满、驱动卡在载体写)。
    let mut fed = 0usize;
    let mut stalled = false;
    for _ in 0..64 {
        local.write_all(&[7u8; 100]).await.unwrap();
        fed += 100;
        let deadline = tokio::time::Instant::now() + Duration::from_millis(500);
        while limiter.charged.load(Ordering::SeqCst) < fed {
            if tokio::time::Instant::now() >= deadline {
                stalled = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        if stalled {
            break;
        }
    }
    assert!(stalled, "64 轮喂养必须触发写侧楔死(命令队列有限)");
    let charged = limiter.charged.load(Ordering::SeqCst);
    assert!(
        charged >= 1000,
        "楔死前必须已有多个 fill 计费块(charged={charged})"
    );

    // 触发规则取消:close 永久 Pending(队列满)也必须收敛
    tx.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(3), &mut relay)
        .await
        .expect(
            "规则取消后 mux_relay 必须收敛(即使 close 命令队列满、poll_close_half 永久 Pending)",
        )
        .unwrap();
    assert!(dropped.load(Ordering::SeqCst), "规则取消必须释放用户侧 IO");
    // fill 口径锁定:楔死时已扣费(charged>0)但客户端零送达;
    // 净扣费 == charged(全量消费、无退款)
    assert_eq!(
        limiter.refunded.load(Ordering::SeqCst),
        0,
        "up 侧 fill 计费即时消费,不应有退款"
    );
    let mut delivered = Vec::new();
    let _ = tokio::time::timeout(Duration::from_millis(300), c.read_to_end(&mut delivered)).await;
    assert!(
        delivered.len() < charged,
        "fill 计费 {charged} 字节必须大于客户端实际送达 {} 字节(读侧计量≠目的端送达)",
        delivered.len()
    );
}

/// 写路径已坏且 shutdown 永久 Pending 的用户侧 IO:poll_write 恒 Err、
/// poll_shutdown 恒 Pending(模拟连接已 reset 但 shutdown 楔死的形态)。
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
        b: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, b)
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

// 故障注入:错误路径(非取消、非 clean EOF)。down 泵写用户侧得 Err
//(用户连接已坏)→ 收尾 a_w.shutdown() 永久 Pending 也必须有界收敛
//(CANCEL_CLOSE_GRACE);同时 up 泵(停在读等待)必须被终止,不得永存。
// 修前(错误收尾无界 + 无对向终止):relay 永不返回、IO 永不释放。
#[tokio::test]
async fn mux_relay_error_path_converges_when_shutdown_pends() {
    let (wire_a, wire_b) = tokio::io::duplex(256 * 1024);
    let client = rh_transport::wrap_stream_as_client(Box::new(wire_a), true, 100);
    let server = rh_transport::wrap_stream_as_server(Box::new(wire_b), true, 100);
    let mut c = client.open_stream().await.unwrap();
    c.write_u8(1).await.unwrap();
    let mut s = server.accept().await.unwrap();
    assert_eq!(s.read_u8().await.unwrap(), 1);
    let (r, w) = mux::steal_mux_halves(&mut s).unwrap();
    drop(s); // 壳

    let (_local, relay_io) = tokio::io::duplex(64 * 1024);
    let dropped = Arc::new(AtomicBool::new(false));
    let io: BoxedIo = Box::new(WriteErrShutdownPendIo {
        inner: relay_io,
        dropped: dropped.clone(),
    });
    let mut relay = tokio::spawn(mux::mux_relay(io, r, w, None, None));

    // down 数据抵达 → pump_down 写用户侧得 Err → 错误路径
    c.write_all(b"boom").await.unwrap();
    // 无取消信号:错误路径自身必须有界收敛
    tokio::time::timeout(Duration::from_secs(3), &mut relay)
        .await
        .expect("down 写错误后 mux_relay 必须收敛(即使 poll_shutdown 永久 Pending)")
        .unwrap();
    assert!(dropped.load(Ordering::SeqCst), "错误路径必须释放用户侧 IO");
}

/// 注入 panic 的写 IO:poll_write 直接 panic(模拟用户侧/适配层 bug)。
struct PanicWriteIo {
    inner: tokio::io::DuplexStream,
    dropped: Arc<AtomicBool>,
}

impl Drop for PanicWriteIo {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::SeqCst);
    }
}

impl AsyncRead for PanicWriteIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        b: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, b)
    }
}

impl AsyncWrite for PanicWriteIo {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        _buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        panic!("injected poll_write panic")
    }
    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

// 故障注入:单向子泵 panic(down 写用户侧)。守卫吸收 JoinError,
// 但对向泵(up,停在读等待)不得因此永存——mux_relay 必须收敛并释放 IO。
#[tokio::test]
async fn mux_relay_sibling_aborted_on_pump_panic() {
    let (wire_a, wire_b) = tokio::io::duplex(256 * 1024);
    let client = rh_transport::wrap_stream_as_client(Box::new(wire_a), true, 100);
    let server = rh_transport::wrap_stream_as_server(Box::new(wire_b), true, 100);
    let mut c = client.open_stream().await.unwrap();
    c.write_u8(1).await.unwrap();
    let mut s = server.accept().await.unwrap();
    assert_eq!(s.read_u8().await.unwrap(), 1);
    let (r, w) = mux::steal_mux_halves(&mut s).unwrap();
    drop(s); // 壳

    let (_local, relay_io) = tokio::io::duplex(64 * 1024);
    let dropped = Arc::new(AtomicBool::new(false));
    let io: BoxedIo = Box::new(PanicWriteIo {
        inner: relay_io,
        dropped: dropped.clone(),
    });
    let mut relay = tokio::spawn(mux::mux_relay(io, r, w, None, None));

    // down 数据抵达 → pump_down 写用户侧 → 注入 panic
    c.write_all(b"boom").await.unwrap();
    tokio::time::timeout(Duration::from_secs(3), &mut relay)
        .await
        .expect("单向子泵 panic 后 mux_relay 必须收敛(对向泵不得永存)")
        .unwrap();
    assert!(
        dropped.load(Ordering::SeqCst),
        "panic 收敛必须释放用户侧 IO"
    );
}

// ---------------------------------------------------------------------------
// G2/G6 单流瓶颈归因回归(2026-10-13 round-2):yamux 流窗口预授信。
//
// 【实测】round-1 三机测试床(benchmark/runs/goal-repair/round-1/
//   single-stream-medians.txt):tcp/tls/ws/wss mux 单流全部卡在
//   1051-1225 Mbps 上行 / 1468-1962 Mbps 下行,与是否 TLS 无关;
//   同路径 16 流 2160-2970 Mbps——每字节成本解释不了,瓶颈随流数摊薄。
// 【假设,本轮未闭合】钳制因素是 yamux 流控信用回路(规约初窗 256KiB,
//   WU 仅在接收端消费 ≥ 半窗后授出)而非每字节成本;测试床信用回路
//   时延与 ping RTT 的关系待下一轮实测。本地机制复现见下方合成 RTT 实验。
//
// 修复:接收端建流即把 max_receive_window 提到配置初值
//   (vendor Config::set_initial_stream_receive_window;rabbithole 接线见
//   mux.rs YamuxMultiplexer::new,受 256MiB 共享预算钳制),首个读 poll
//   以**标准窗口更新帧**(WU;入站流带 ACK flag,upstream 既有合法组合)
//   预授差额信用,且该首 WU 不触发 BDP 倍增。发送端初值仍 256KiB 不变
//   ——混合版本部署时对端无论新旧均只看到普通 WU,线路兼容(方案红线 8)。
//
// 红取证:01-red-initial-window.log 是修前**测试自身缺陷**的记录——
//   先 open_stream 再 accept,而 yamux SYN 是 lazy 的(Flag::Syn 随首个
//   外发帧置位,vendor stream.rs),accept 永久 Pending,测试挂死 60s+,
//   **没有**任何 262144 断言失败输出。修前实现的真红见
//   02-red-initial-window.log(修正后的测试:驱动握手 + 全流程 deadline,
//   对 upstream 无预授信实现恒 written=196608,断言 FAILED)与
//   02-red-flowcontrol-budget.log(对本轮评审起点实现:预算钳制/
//   首 WU 不倍增/溢出饱和 3 项单元回归 FAILED)。
//
// 判据(确定性,无计时竞态、无自增窗混入):接收端只读**一次**
// (弹出随 SYN 到达的 1 字节)即永久停读——全程只发过一个窗口更新
// (即预授信 WU),自增窗(BDP 倍增)只会在后续 next_window_update 里
// 触发,永不介入。发送端逐 64KiB 写入,被窗口卡住(2s 超时)前的累计
// 可写字节数 == 规约初窗 256KiB + 首个 WU 授出的预授信差额。
// 修前(无预授信,upstream 行为):首 poll 消费 1 字节 < 半窗阈值,
// 无 WU,恒 3×64KiB = 196608;修后(6MiB 初窗,round-3 对齐 frp):
// 首个 WU 授出 6MiB−256KiB+1,恒 1+96×64KiB = 6291457(6MiB+1)。
// 断言取 ≥ 5.5MiB 下界。
// (注:接收端持续读的形态下,本地 duplex 的自增窗会把修前形态也抬到
// ~1.75MiB——回路时延 < 2×RTT 时倍增合法触发;故必须用单次读设计
// 把预授信与自增窗解耦。)
#[tokio::test]
async fn mux_initial_window_pregrant_unlocks_single_stream() {
    use rh_transport::MUX_STREAMS_UNLIMITED;

    // 全流程 deadline:任何阶段挂起(建流/读/写)都转为确定失败,
    // 不再出现 01-red 日志里 "running for over 60 seconds" 的挂死形态。
    tokio::time::timeout(Duration::from_secs(30), async {
        // 载体 duplex 容量 8MiB:大于一切窗口,保证先被流控卡住的是
        // yamux 信用而非 harness 缓冲。
        let (wire_a, wire_b) = tokio::io::duplex(8 * 1024 * 1024);
        let client =
            rh_transport::wrap_stream_as_client(Box::new(wire_a), true, MUX_STREAMS_UNLIMITED);
        let server =
            rh_transport::wrap_stream_as_server(Box::new(wire_b), true, MUX_STREAMS_UNLIMITED);

        let mut c = client.open_stream().await.unwrap();
        // 驱动 lazy SYN:SYN flag 随首个外发帧置位(vendor stream.rs
        // Flag::Syn),必须先写再 accept——反序 accept 永久 Pending。
        c.write_u8(0xAA).await.unwrap();
        let mut s = server.accept().await.unwrap();
        let (mut s_r, _s_w) = mux::steal_mux_halves(&mut s).unwrap();
        drop(s); // 壳

        // 接收端:只读一次(1 字节)即挂起不读——半壳不 drop(避免任何
        // 收尾帧),持有 s_r 停在 pending:唯一 WU 已随首读发出。
        let reader = tokio::spawn(async move {
            let first = s_r.read_chunk().await;
            let got = match &first {
                Ok(Some(b)) => b.len(),
                _ => 0,
            };
            // 永久停读(保持读半存活,不再触发任何窗口更新)
            std::future::pending::<()>().await;
            #[allow(unreachable_code)]
            got
        });

        // 发送端:64KiB 步进写入,直到被窗口卡住(单次写 2s 超时视作卡住)。
        const CHUNK: usize = 64 * 1024;
        let payload = vec![0x5au8; CHUNK];
        let mut written = 0usize;
        loop {
            let r = tokio::time::timeout(Duration::from_secs(2), c.write_all(&payload)).await;
            match r {
                Ok(Ok(())) => written += CHUNK,
                _ => break, // Elapsed(窗口卡住)或 IO 错误:停止计数
            }
            assert!(written <= 8 * 1024 * 1024, "窗口不得越过 harness 上界");
        }
        assert!(
            written > 0,
            "初窗 256KiB 至少应容纳 3×64KiB(实际 {written},载体/实现异常)"
        );
        eprintln!("pregrant: written={written} (唯一 WU 应授 6MiB-256KiB+1)");

        // 断言后回收 reader(abort 挂起任务,读半随任务 drop)
        reader.abort();
        assert!(
            written >= CHUNK * 88, // ≥ 5.5MiB:钉住 6MiB 接线(回退 2MiB 接线恒 2MiB+1 必须 FAIL;修前无预授信恒 196608)
            "单流初信用必须包含预授信差额(修前 3×64KiB=196608;实际 {written})"
        );
    })
    .await
    .expect("全流程必须在 30s 内完成(挂起=失败,不作窗口证据)");
}

/// 顺序延迟泵:读一块 → sleep(delay) → 写对端。两个方向各一条,
/// 构成 RTT ≈ 2×delay 的合成载体(每向传播时延 delay)。
async fn delay_pump<R, W>(mut from: R, mut to: W, delay: Duration)
where
    R: tokio::io::AsyncRead + Unpin,
    W: tokio::io::AsyncWrite + Unpin,
{
    delay_pump_buf(from, to, delay, 256 * 1024).await;
}

/// 可调缓冲的延迟泵。buf 越大,泵服务速率上限(chunk/delay)越高——
/// 泵不得成为被测路径的瓶颈。
async fn delay_pump_buf<R, W>(mut from: R, mut to: W, delay: Duration, buf_len: usize)
where
    R: tokio::io::AsyncRead + Unpin,
    W: tokio::io::AsyncWrite + Unpin,
{
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    let mut buf = vec![0u8; buf_len];
    loop {
        match from.read(&mut buf).await {
            Ok(0) | Err(_) => break,
            Ok(n) => {
                if !delay.is_zero() {
                    tokio::time::sleep(delay).await;
                }
                if to.write_all(&buf[..n]).await.is_err() {
                    break;
                }
                let _ = to.flush().await;
            }
        }
    }
    let _ = to.shutdown().await;
}

/// 64KiB copy 泵(refrelay-copy 同形:read→write_all,EOF 半关闭)。
async fn copy_pump_64k<R, W>(mut from: R, mut to: W)
where
    R: tokio::io::AsyncRead + Unpin,
    W: tokio::io::AsyncWrite + Unpin,
{
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        match from.read(&mut buf).await {
            Ok(0) | Err(_) => break,
            Ok(n) => {
                if to.write_all(&buf[..n]).await.is_err() {
                    break;
                }
            }
        }
    }
    let _ = to.shutdown().await;
}

/// 本地两跳中继仿真(诊断,#[ignore] 显式运行):
/// 预检诊断项 ②「mux chunk 上行每字节/节律成本」能否在本地复现——
/// 测试床数据(tcp-mux up 1396 vs nomux up 1920,down 双双 ~2.0-2.1G 到
/// 两跳天花板;diagnosis-after-round-2/analysis.txt 阶段C)的归因需要
/// 远端 RELAY=copy A/B,本仿真先验证该缺口是否脱离真实 TCP 拥塞也能
/// 出现(tokio 唤醒链 + 帧边界 + 延迟泵节律)。
///
/// 拓扑(两路径同构,仅中继形态不同):
///   sender → hop1(duplex 4MiB + 双向 delay 泵) → [中继] →
///   hop2(duplex 4MiB + 双向 delay 泵) → sink(持续读)
/// - A 对照(copy 形态):中继 = 64KiB copy 泵(refrelay-copy 同形)
/// - B 实验(mux chunk 形态):中继 = yamux 载体 + mux_relay(chunk 路径)
/// delay=200µs/向 → 稳态 RTT ≈ 0.4ms(测试床 minrtt 0.13-0.28ms 同量级;
/// 缓冲排队膨胀由 duplex 容量自然产生,与测试床 4MiB 满载发送缓冲同形)。
/// 另跑 delay=0 轮给出两种形态的纯软件吞吐上限参照。
/// 保真度限制(如实):duplex 为无丢失内存管道,无真实 TCP cwnd/pacing/
/// fq_codel;结论仅约束"纯用户态节律"这一假设族。
///
/// 运行: cargo test -p rh-transport --test goal_repair_mux \
///   two_hop_relay_upload_emulator -- --ignored --nocapture
#[tokio::test]
#[ignore = "诊断实验:本地两跳仿真,需显式运行并记录证据"]
async fn two_hop_relay_upload_emulator() {
    use rh_transport::MUX_STREAMS_UNLIMITED;
    use std::time::Instant;

    const TOTAL: u64 = 384 * 1024 * 1024; // 每路径搬运总量(3GiB 轮为剖析临时值,已还原)
    const CHUNK: usize = 64 * 1024;
    const HOP_BUF: usize = 4 * 1024 * 1024; // 模拟满载 4MiB 发送缓冲
    const PUMP_BUF: usize = 2 * 1024 * 1024; // 泵服务上限 2MiB/200µs = 80Gbps,非瓶颈

    /// 组装一跳:返回 (本侧端点, 对侧端点),两向各一条 delay 泵。
    fn hop(buf: usize, delay: Duration) -> (tokio::io::DuplexStream, tokio::io::DuplexStream) {
        let (a1, a2) = tokio::io::duplex(buf);
        let (b1, b2) = tokio::io::duplex(buf);
        let (a2r, a2w) = tokio::io::split(a2);
        let (b1r, b1w) = tokio::io::split(b1);
        tokio::spawn(delay_pump_buf(a2r, b1w, delay, PUMP_BUF));
        tokio::spawn(delay_pump_buf(b1r, a2w, delay, PUMP_BUF));
        (a1, b2) // a1 = 本侧(发送端写入),b2 = 对侧(中继读取)
    }

    async fn sink_count(mut r: tokio::io::DuplexStream) -> (u64, std::time::Duration) {
        use tokio::io::AsyncReadExt as _;
        let mut buf = vec![0u8; 512 * 1024];
        let mut got: u64 = 0;
        let start = Instant::now();
        loop {
            match r.read(&mut buf).await {
                Ok(0) | Err(_) => break,
                Ok(n) => got += n as u64,
            }
        }
        (got, start.elapsed())
    }

    async fn sender_write<W>(mut w: W, total: u64, chunk: usize)
    where
        W: tokio::io::AsyncWrite + Unpin,
    {
        use tokio::io::AsyncWriteExt as _;
        let payload = vec![0x5au8; chunk];
        let mut sent: u64 = 0;
        while sent < total {
            let n = ((total - sent) as usize).min(chunk);
            w.write_all(&payload[..n]).await.unwrap();
            sent += n as u64;
        }
        let _ = w.shutdown().await;
    }

    // ---- 路径 A:copy 形态中继(refrelay 同形:双向各一 64KiB copy 泵)----
    // forward(上行形态):sender 在 hop1 近端,sink 在 hop2 远端;
    // reverse(下行形态):sender 在 hop2 远端,sink 在 hop1 近端。
    async fn run_copy(delay: Duration, reverse: bool) -> f64 {
        let (x1, y2) = hop(HOP_BUF, delay); // hop1: x1=近端, y2=relay 侧
        let (p1, q2) = hop(HOP_BUF, delay); // hop2: p1=relay 侧, q2=远端
        let (y2r, y2w) = tokio::io::split(y2);
        let (p1r, p1w) = tokio::io::split(p1);
        tokio::spawn(copy_pump_64k(y2r, p1w)); // hop1 → hop2
        tokio::spawn(copy_pump_64k(p1r, y2w)); // hop2 → hop1
        let (got, dur) = if reverse {
            let sink = tokio::spawn(sink_count(x1));
            sender_write(q2, TOTAL, CHUNK).await;
            sink.await.unwrap()
        } else {
            let sink = tokio::spawn(sink_count(q2));
            sender_write(x1, TOTAL, CHUNK).await;
            sink.await.unwrap()
        };
        assert_eq!(got, TOTAL, "copy 路径字节必须完整送达");
        got as f64 * 8.0 / dur.as_secs_f64() / 1e6
    }

    // ---- 路径 B:mux chunk 形态中继(双向 mux_relay;方向由 sender/sink 位置定义)----
    async fn run_mux(delay: Duration, reverse: bool) -> f64 {
        let (x1, y2) = hop(HOP_BUF, delay); // hop1 载体:x1=client, y2=server
        let (p1, q2) = hop(HOP_BUF, delay); // hop2:p1=relay 用户侧, q2=远端
        let client = rh_transport::wrap_stream_as_client(Box::new(x1), true, MUX_STREAMS_UNLIMITED);
        let server = rh_transport::wrap_stream_as_server(Box::new(y2), true, MUX_STREAMS_UNLIMITED);
        let mut c = client.open_stream().await.unwrap();
        c.write_u8(0xAA).await.unwrap(); // 驱动 lazy SYN
        let mut s = server.accept().await.unwrap();
        let (r, w) = mux::steal_mux_halves(&mut s).unwrap();
        drop(s); // 壳
        let relay =
            tokio::spawn(async move { mux::mux_relay(Box::new(p1), r, w, None, None).await });
        let (got, dur) = if reverse {
            // 下行形态:sender 在 hop2 远端(q2),client 流 c 为 sink
            use tokio::io::AsyncReadExt as _;
            let sink = tokio::spawn(async move {
                let mut got: u64 = 0;
                let mut buf = vec![0u8; 512 * 1024];
                let start = Instant::now();
                loop {
                    match c.read(&mut buf).await {
                        Ok(0) | Err(_) => break,
                        Ok(n) => got += n as u64,
                    }
                }
                (got, start.elapsed())
            });
            // 首字节 0xAA 随 SYN 发往 server(留在 hop2 缓冲,无人读),
            // 下行计数只含 sender 写入的 TOTAL 字节
            sender_write(q2, TOTAL, CHUNK).await;
            sink.await.unwrap()
        } else {
            let sink = tokio::spawn(sink_count(q2));
            sender_write(c, TOTAL - 1, CHUNK).await; // 首字节已发
            sink.await.unwrap()
        };
        let _ = tokio::time::timeout(Duration::from_secs(5), relay).await;
        assert_eq!(got, TOTAL, "mux 路径字节必须完整送达");
        got as f64 * 8.0 / dur.as_secs_f64() / 1e6
    }

    let out = |label: &str, mbps: f64| eprintln!("two-hop emulator {label}: {mbps:.0} Mbps");

    tokio::time::timeout(Duration::from_secs(240), async {
        // delay=200µs(测试床 minrtt 0.13-0.28ms 同量级),双向
        let copy_up = run_copy(Duration::from_micros(200), false).await;
        let mux_up = run_mux(Duration::from_micros(200), false).await;
        let copy_down = run_copy(Duration::from_micros(200), true).await;
        let mux_down = run_mux(Duration::from_micros(200), true).await;
        out("copy up  @rtt≈0.4ms", copy_up);
        out("mux  up  @rtt≈0.4ms", mux_up);
        out("copy down@rtt≈0.4ms", copy_down);
        out("mux  down@rtt≈0.4ms", mux_down);
        // delay=0(纯软件上限参照,仅上行)
        let copy_fast = run_copy(Duration::ZERO, false).await;
        let mux_fast = run_mux(Duration::ZERO, false).await;
        out("copy up  @no-delay", copy_fast);
        out("mux  up  @no-delay", mux_fast);
        eprintln!(
            "mux/copy 比:up {:+.1}% / down {:+.1}% \
             (测试床同窗:up 1396/1920=72.7%,down 2110/2010=105%)",
            mux_up / copy_up * 100.0,
            mux_down / copy_down * 100.0,
        );
        assert!(
            copy_up > 300.0 && mux_up > 300.0 && copy_down > 300.0 && mux_down > 300.0,
            "四条路径必须都 >300Mbps(harness 自检)"
        );
    })
    .await
    .expect("仿真必须在 240s 内完成");
}

// 合成 RTT 信用钳制实验(诊断,#[ignore] 显式运行,不作 CI 判据):
// 在 RTT ≈ 2ms 的合成载体上重复预授信测试的判据(接收端单次读后停读,
// 发送端可写总量 == 初窗 + 唯一 WU 授信)。修前(无预授信)恒
// 196608(256KiB 初窗);修后(6MiB 初窗,round-3 对齐 frp)恒 6291457
// (6MiB+1)——即 RTT 载体上单流在途窗口被预授信抬高 24 倍,持续态
// 吞吐钳制相应从 256KiB/RTT ≈ 1.0Gbps 抬到 6MiB/RTT ≈ 24Gbps
// (每轮窗口/RTT 静态算术;修前 1.0Gbps 与 round-1 测试床 mux 上行
// 1051-1225 Mbps 同量级,是一致性佐证而非因果证明——测试床回路时延
// 与 RTT 的实测关系待下轮)。
// 运行: cargo test -p rh-transport --test goal_repair_mux \
//   mux_single_stream_credit_stall -- --ignored --nocapture
#[tokio::test]
#[ignore = "诊断实验:合成 RTT 载体,需显式运行并记录窗口证据"]
async fn mux_single_stream_credit_stall_under_synthetic_rtt() {
    use rh_transport::MUX_STREAMS_UNLIMITED;

    const DELAY: Duration = Duration::from_millis(1); // RTT ≈ 2ms
    tokio::time::timeout(Duration::from_secs(60), async {
        // 两条 duplex + 每向延迟泵:a1=client, b2=server。
        // 泵缓冲 16MiB > 一切窗口:先被卡住的只会是 yamux 信用。
        let (a1, a2) = tokio::io::duplex(16 * 1024 * 1024);
        let (b1, b2) = tokio::io::duplex(16 * 1024 * 1024);
        let (a2r, a2w) = tokio::io::split(a2);
        let (b1r, b1w) = tokio::io::split(b1);
        tokio::spawn(delay_pump(a2r, b1w, DELAY)); // client → server
        tokio::spawn(delay_pump(b1r, a2w, DELAY)); // server → client
        let client = rh_transport::wrap_stream_as_client(Box::new(a1), true, MUX_STREAMS_UNLIMITED);
        let server = rh_transport::wrap_stream_as_server(Box::new(b2), true, MUX_STREAMS_UNLIMITED);

        let mut c = client.open_stream().await.unwrap();
        c.write_u8(0xAA).await.unwrap(); // 驱动 lazy SYN(同上)
        let mut s = server.accept().await.unwrap();
        let (mut s_r, _s_w) = mux::steal_mux_halves(&mut s).unwrap();
        drop(s);

        // 单次读(1 字节)后永久停读:唯一 WU 即预授信,无自增窗混入
        let reader = tokio::spawn(async move {
            let first = s_r.read_chunk().await;
            let got = match &first {
                Ok(Some(b)) => b.len(),
                _ => 0,
            };
            std::future::pending::<()>().await;
            #[allow(unreachable_code)]
            got
        });

        const CHUNK: usize = 64 * 1024;
        let payload = vec![0x5au8; CHUNK];
        let mut written = 0usize;
        let start = std::time::Instant::now();
        loop {
            let r = tokio::time::timeout(Duration::from_secs(5), c.write_all(&payload)).await;
            match r {
                Ok(Ok(())) => written += CHUNK,
                _ => break,
            }
            assert!(written <= 16 * 1024 * 1024, "窗口不得越过 harness 上界");
        }
        let elapsed = start.elapsed();
        // written = 初信用总量;持续态单流钳制 ≈ 每轮在途窗口/RTT
        //(静态算术,非本实验测量;持续吞吐另受自增窗/泵速率影响)。
        eprintln!(
            "credit-stall@rtt≈{}us: written={written} ({:.2} MiB), fill-elapsed={elapsed:?}, \
             per-round-clamp: 256KiB/RTT≈{:.0}Mbps, 6MiB/RTT≈{:.0}Mbps",
            DELAY.as_micros() * 2,
            written as f64 / (1024.0 * 1024.0),
            256.0 * 1024.0 * 8.0 / (2.0 * DELAY.as_secs_f64()) / 1e6,
            6.0 * 1024.0 * 1024.0 * 8.0 / (2.0 * DELAY.as_secs_f64()) / 1e6,
        );
        reader.abort();
        assert!(
            written >= CHUNK * 88, // ≥ 5.5MiB:预授信生效且接线为 6MiB(修前恒 196608;2MiB 接线恒 2MiB+1)
            "合成 RTT 载体上单流初信用必须跟随预授信窗口(修前 3×64KiB=196608;实际 {written})"
        );
    })
    .await
    .expect("实验必须在 60s 内完成");
}
