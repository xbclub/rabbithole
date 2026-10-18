//! yamux 多路复用驱动:yamux(vendor 零拷贝版)是 poll 风格 + futures IO,
//! 这里封装为 async open/accept(返回 tokio IO)+ 收/发双任务驱动
//! (Round-D:连接拆分 RecvHalf/SendHalf,对齐 Go yamux 的
//! `go s.recv()` / `go s.send()` 双 goroutine 模型——收(解帧+派发)与
//! 发(取命令+编帧+写出)物理并行;接收侧 accept 背压只停读侧任务,
//! 不再拖死发送)+ N 个分片 Worker 任务(Round-5:接收侧数据面按
//! `sid % N` 拆到 N 个 Worker,流级解包/派发/流控记账/唤醒在各 Worker
//! 内闭环,多核分摊——对齐 frp 多 goroutine;yamux 侧 futures-only,
//! Worker 是裸 Future,spawn 在此完成)。载体经 [`YamuxCarrier`] 适配:
//! 帧头+帧体由 writev 一次下发,发送半聚合的整批帧亦一次 writev。

use crate::{BoxedIo, MultiplexerInner};
use bytes::{BufMut as _, Bytes};
use std::io;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::Poll;
use tokio::io::ReadBuf;
use tokio::io::{AsyncRead as _, AsyncWrite as _};
use tokio::sync::{mpsc, oneshot};

/// yamux 流的 tokio 视图 + 零拷贝 chunk 通道(vendor yamux 专用)。
/// 普通消费者按 AsyncRead/AsyncWrite 使用(语义与旧 Compat 路径等价,
/// 读侧一次块内拷贝);转发路径可经 [`RhMuxStream::steal_halves`] 拆出
/// 读/写半走 read_chunk/write_chunk,数据全程 `Bytes` 引用传递、零额外拷贝
/// (未被 QuotaIo/PrependedIo 包装时——包装即回落拷贝路径)。
pub struct RhMuxStream {
    /// steal_halves 后为 None(壳保留在 BoxedIo 内随后丢弃)。
    inner: Option<yamux::Stream>,
    /// 兼容读路径:poll_read_chunk 取出后未消费完的余块
    /// (转发偷取时随读半前传,不丢数据)。
    pending: Option<Bytes>,
}

impl RhMuxStream {
    fn new(inner: yamux::Stream) -> Self {
        Self {
            inner: Some(inner),
            pending: None,
        }
    }

    /// 经 &mut 引用拆出读/写半(独占所有权场景;调用方随后丢弃壳)。
    /// 非 mux 流(返回 None)或已被偷取时由调用方回落。
    pub fn steal_halves(&mut self) -> Option<(RhMuxStreamRead, RhMuxStreamWrite)> {
        let inner = self.inner.take()?;
        let (r, w) = inner.into_halves();
        let mut pending = std::collections::VecDeque::new();
        if let Some(b) = self.pending.take() {
            pending.push_back(b);
        }
        Some((
            RhMuxStreamRead { inner: r, pending },
            RhMuxStreamWrite { inner: w },
        ))
    }
}

impl tokio::io::AsyncRead for RhMuxStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let Some(inner) = this.inner.as_mut() else {
            // 已被偷取:壳不应再被读,按 EOF 处理
            return Poll::Ready(Ok(()));
        };
        loop {
            if let Some(b) = this.pending.take() {
                if b.is_empty() {
                    continue;
                }
                let k = b.len().min(buf.remaining());
                buf.put_slice(&b[..k]);
                if k < b.len() {
                    this.pending = Some(b.slice(k..));
                }
                return Poll::Ready(Ok(()));
            }
            match Pin::new(&mut *inner).poll_read_chunk(cx) {
                Poll::Ready(Ok(Some(b))) => this.pending = Some(b),
                Poll::Ready(Ok(None)) => return Poll::Ready(Ok(())),
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

impl tokio::io::AsyncWrite for RhMuxStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let Some(inner) = this.inner.as_mut() else {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "mux stream halves stolen",
            )));
        };
        futures::AsyncWrite::poll_write(Pin::new(inner), cx, buf)
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let Some(inner) = this.inner.as_mut() else {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "mux stream halves stolen",
            )));
        };
        futures::AsyncWrite::poll_flush(Pin::new(inner), cx)
    }
    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let Some(inner) = this.inner.as_mut() else {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "mux stream halves stolen",
            )));
        };
        futures::AsyncWrite::poll_close(Pin::new(inner), cx)
    }
}

/// mux 流读半:零拷贝取块(帧体 Bytes 引用切片)。
pub struct RhMuxStreamRead {
    inner: yamux::StreamReadHalf,
    /// 先行交付队列(FIFO):剥壳注入的残留前缀(B1)、偷取时前传的
    /// 兼容读余量、聚合截断回退块。全部先于 inner 的后续数据交付。
    pending: std::collections::VecDeque<Bytes>,
}

impl RhMuxStreamRead {
    /// 取一块;`Ok(None)` = EOF/复位。
    pub async fn read_chunk(&mut self) -> io::Result<Option<Bytes>> {
        if let Some(b) = self.pending.pop_front() {
            return Ok(Some(b));
        }
        std::future::poll_fn(|cx| self.inner.poll_read_chunk(cx)).await
    }

    /// 立即取一块:无就绪数据返回 `Ok(None)`(不挂起)。供聚合写侧
    /// 批量收割已到达的帧;可能多注册一次 waker,无害。
    pub async fn try_read_chunk(&mut self) -> io::Result<Option<Bytes>> {
        if let Some(b) = self.pending.pop_front() {
            return Ok(Some(b));
        }
        let inner = &mut self.inner;
        std::future::poll_fn(|cx| match inner.poll_read_chunk(cx) {
            Poll::Ready(r) => Poll::Ready(r),
            Poll::Pending => Poll::Ready(Ok(None)),
        })
        .await
    }

    /// 放回一块(下一轮 read_chunk 最先交付);聚合写截断时用。
    pub fn push_front(&mut self, b: Bytes) {
        self.pending.push_front(b);
    }

    /// 注入先行前缀(goal-repair B1:PrependedIo 剥壳残留字节),
    /// 先于一切已有 pending 与 inner 数据交付(线路字节顺序保持)。
    pub fn set_front_prefix(&mut self, b: Bytes) {
        self.pending.push_front(b);
    }
}

/// mux 流写半:零拷贝整块写入(块直接作帧体,不经池拷贝)+ FIN 半关闭。
pub struct RhMuxStreamWrite {
    inner: yamux::StreamWriteHalf,
}

impl RhMuxStreamWrite {
    /// 整块写入:窗口/分片不足时内部续写或挂起,返回时已全部入帧。
    /// 整块消费保持 Bytes 引用计数 1,帧写穿后由 vendor 归池循环利用。
    pub async fn write_chunk(&mut self, chunk: Bytes) -> io::Result<()> {
        let mut cur = Some(chunk);
        std::future::poll_fn(|cx| loop {
            match self.inner.poll_write_chunk(cx, &mut cur) {
                Poll::Ready(Ok(n)) => {
                    let drained = n == 0 || cur.as_ref().is_none_or(|c| c.is_empty());
                    if drained {
                        return Poll::Ready(Ok(()));
                    }
                    // 窗口尚有余量:继续写剩余
                }
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Pending => return Poll::Pending,
            }
        })
        .await
    }

    /// FIN 半关闭(上行 EOF 传递)。
    pub async fn close(&mut self) -> io::Result<()> {
        std::future::poll_fn(|cx| self.inner.poll_close_half(cx)).await
    }
}

fn pool_get(cap: usize) -> bytes::BytesMut {
    yamux::pool::get(cap)
}

/// 初始相本地缓冲容量(goal-repair C4):< vendor 池 MIN_RECYCLE(16KiB),
/// 初始相冻结 Bytes 写穿后 vendor `pool::put` 被拒收,池零扰动;
/// 空闲中继驻留 = 12KiB/条(4096 条 ≈ 48MiB,不钉池的 64KiB 类)。
const INIT_BUF_CAP: usize = 12 * 1024;

/// 从任意 tokio 读端直接读入池化缓冲:内核 → 池 BytesMut 单次拷贝(不可省),
/// 返回 freeze 的 Bytes;`Ok(None)` = EOF。与 [`RhMuxStreamWrite::write_chunk`]
/// 组成「socket → yamux 帧」零额外拷贝链;cap 取 64KiB(读缓冲上限红线)。
/// A4 修复:读入 spare 的前 `cap` 字节切片——池中存在超限缓冲(如外部播种
/// 256KiB)时单次读入也不得超过 cap(红线 2)。
pub async fn read_chunk_from<R: tokio::io::AsyncRead + Unpin>(
    r: &mut R,
    cap: usize,
) -> io::Result<Option<Bytes>> {
    let bm = pool_get(cap);
    read_chunk_into(r, cap, bm).await
}

/// 读块核心:把 `r` 读入 `bm` spare 的前 `cap` 字节(A4 上限收口;测试缝:
/// 可注入任意容量的缓冲断言每块 ≤ cap)。返回 freeze 的 Bytes;`Ok(None)`=EOF。
async fn read_chunk_into<R: tokio::io::AsyncRead + Unpin>(
    r: &mut R,
    cap: usize,
    mut bm: bytes::BytesMut,
) -> io::Result<Option<Bytes>> {
    let n = std::future::poll_fn(|cx| {
        let spare = bm.spare_capacity_mut();
        let limit = spare.len().min(cap);
        let mut rb = ReadBuf::uninit(&mut spare[..limit]);
        match Pin::new(&mut *r).poll_read(cx, &mut rb) {
            Poll::Ready(Ok(())) => Poll::Ready(Ok(rb.filled().len())),
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Pending => Poll::Pending,
        }
    })
    .await?;
    if n == 0 {
        return Ok(None);
    }
    // SAFETY(advance_mut):上面 poll_read 只把读端已写入的 n 字节标为
    // filled 并仅暴露该前缀;声称已初始化的前 n 字节确已初始化。
    unsafe { bm.advance_mut(n) };
    Ok(Some(bm.freeze()))
}

/// up 读侧两相缓冲(goal-repair C4):
/// * 初始相:每次读分配本地 12KiB(< MIN_RECYCLE,不经池;滴流/空闲连接
///   不钉池缓冲),读满一次即升级;
/// * 池相:照旧 `pool::get(CHUNK_CAP)`(批量流保持池复用)。
#[derive(Default)]
struct UpReader {
    upgraded: bool,
    /// 本读取器经池相取缓冲的次数(cfg(test) 探针:C4 断言滴流阶段 == 0)。
    #[cfg(test)]
    pool_borrows: usize,
}

impl UpReader {
    async fn read_chunk<R: tokio::io::AsyncRead + Unpin>(
        &mut self,
        r: &mut R,
    ) -> io::Result<Option<Bytes>> {
        if self.upgraded {
            #[cfg(test)]
            {
                self.pool_borrows += 1;
            }
            return read_chunk_from(r, CHUNK_CAP).await;
        }
        let local = bytes::BytesMut::with_capacity(INIT_BUF_CAP);
        let chunk = read_chunk_into(r, INIT_BUF_CAP, local).await?;
        if chunk.as_ref().is_some_and(|c| c.len() >= INIT_BUF_CAP) {
            self.upgraded = true;
        }
        Ok(chunk)
    }
}

type MuxConn = yamux::Connection<YamuxCarrier>;

/// chunk 中继读块上限(读缓冲 64KiB 红线:yamux 窗口更新按读步进,
/// 超限会打破与对端发送窗的锁步节奏)。
const CHUNK_CAP: usize = 64 * 1024;

/// round-11 诊断:pump_down 的读/写耗时分解。仅当 `RH_PUMP_STATS=1` 时
/// 在 pump 结束时向 stderr 输出一行。用于判定下行瓶颈是"等数据"还是
/// "写不出"。
pub mod pump_diag {
    use std::sync::atomic::{AtomicU64, Ordering};
    pub static READ_NS: AtomicU64 = AtomicU64::new(0);
    pub static WRITE_NS: AtomicU64 = AtomicU64::new(0);
    pub static READS: AtomicU64 = AtomicU64::new(0);
    pub static WRITES: AtomicU64 = AtomicU64::new(0);
    pub static BYTES: AtomicU64 = AtomicU64::new(0);
    pub static CHUNKS: AtomicU64 = AtomicU64::new(0);
    /// 单次读等待 > 1ms 的次数(判定是否在等上游)
    pub static SLOW_READS: AtomicU64 = AtomicU64::new(0);
    /// 单次读等待 > 10ms 的次数
    pub static VERY_SLOW_READS: AtomicU64 = AtomicU64::new(0);
    pub static SLOW_WRITES: AtomicU64 = AtomicU64::new(0);
    pub static UP_BYTES: AtomicU64 = AtomicU64::new(0);
    pub static UP_CHUNKS: AtomicU64 = AtomicU64::new(0);
    pub fn enabled() -> bool {
        static V: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
        *V.get_or_init(|| std::env::var("RH_PUMP_STATS").map(|v| v == "1").unwrap_or(false))
    }
    pub fn dump(tag: &str) {
        if !enabled() { return; }
        let r = READ_NS.load(Ordering::Relaxed) as f64 / 1e6;
        let w = WRITE_NS.load(Ordering::Relaxed) as f64 / 1e6;
        let rd = READS.load(Ordering::Relaxed);
        let wr = WRITES.load(Ordering::Relaxed);
        let by = BYTES.load(Ordering::Relaxed);
        let ch = CHUNKS.load(Ordering::Relaxed);
        eprintln!(
            "PUMP_STATS {tag} read_ms={r:.0} write_ms={w:.0} reads={rd} writes={wr} bytes={by} chunks={ch} avg_chunk={:.0} slow1ms={} slow10ms={} slowwrites={}",
            if ch > 0 { by as f64 / ch as f64 } else { 0.0 },
            SLOW_READS.load(Ordering::Relaxed),
            VERY_SLOW_READS.load(Ordering::Relaxed),
            SLOW_WRITES.load(Ordering::Relaxed),
        );
        eprintln!(
            "PUMP_UP {tag} up_bytes={} up_chunks={}",
            UP_BYTES.load(Ordering::Relaxed),
            UP_CHUNKS.load(Ordering::Relaxed)
        );
    }
}

/// 规则取消后的收尾(FIN/close/shutdown)等待上限。
/// 收尾可能永久 Pending:对端不读且发送缓冲满、对端接收窗耗尽、
/// 自定义 IO 直接 Pending(主代理 cancel-close 复现:PendingShutdownIo)、
/// yamux 命令队列满(载体写死锁时 poll_ready 永不就绪——close 无法入队)。
/// 取消(规则禁用/删除)与错误路径(读/写已 Err,连接已坏,等 FIN 无意义)
/// 必须收敛释放 IO;**clean EOF 路径不设上限**,正常半关闭仍等待完整
/// FIN 传递(红线)。
const CANCEL_CLOSE_GRACE: std::time::Duration = std::time::Duration::from_secs(1);

/// 收尾等待(FIN/shutdown)与规则取消的合流:
/// * 未取消:无界等待完整半关闭传递(红线——不对正常关闭硬加短超时,
///   否则 FIN 尚未写穿即放弃,截断半关闭);
/// * 收尾期间取消抵达:仅在 [`CANCEL_CLOSE_GRACE`] 内继续等待已推进的
///   close/shutdown,超时即放弃并释放 IO(主代理 cancel-after-eof 复现:
///   泵先 EOF 进入 shutdown 后取消永不再被监听 → relay 卡死、IO 不释放)。
async fn close_with_late_cancel<F>(close: F, cancel: impl std::future::Future<Output = ()>)
where
    F: std::future::Future<Output = io::Result<()>>,
{
    let mut close = std::pin::pin!(close);
    tokio::select! {
        _ = &mut close => {}
        _ = cancel => {
            let _ = tokio::time::timeout(CANCEL_CLOSE_GRACE, &mut close).await;
        }
    }
}

/// 零拷贝 mux 双向中继:a 为对端普通 IO(用户连接/本地目标),b 为 mux 流半。
/// * up:a 读 → 池化块(内核单次拷贝)→ mux 帧体(零拷贝);
/// * down:mux 块(Bytes 引用)→ a 写(内核单次拷贝)。
///
/// 限速(goal-repair A5/A3):经 [`crate::ChunkLimiter`] 预留式扣减——
/// up=读侧计费(fill 完成即预留并即时消费);down=送达计费(冲刷前预留、
/// 按实际写出消费,取消/部分写自动退还未消费预留)。双桶皆空时调用方传
/// None,零热路径开销。
///
/// cancel/错误路径完成即双向有界收尾(收尾 close/shutdown 永久 Pending 时
/// 受 CANCEL_CLOSE_GRACE 约束);clean EOF 不设上限,半关闭语义与 copy 循环
/// 一致(单向 EOF → 对向写端关闭:mux 侧 FIN、a 侧 shutdown)。
/// 单向子泵 panic 时对向泵立即终止(不永存,见 [`crate::join_relay_pumps`])。
/// 父任务取消(A1a):子泵句柄由 [`crate::AbortOnDropHandle`] 守卫,
/// 父 future 被 drop 即 abort 双向子泵(不再 detach)。
pub async fn mux_relay(
    a: BoxedIo,
    b_read: RhMuxStreamRead,
    b_write: RhMuxStreamWrite,
    limiter: Option<std::sync::Arc<dyn crate::ChunkLimiter>>,
    cancel: Option<crate::CancelFuture>,
) {
    use tokio::io::AsyncWriteExt as _;

    let (mut a_r, mut a_w) = tokio::io::split(a);

    // 本向以错误(非 EOF、非取消)结束时置位:对向数据通路已死
    //(mux 连接断 / 用户侧错误),继续等对向只会在已死的通路上永存。
    let up_errored = Arc::new(AtomicBool::new(false));
    let dn_errored = Arc::new(AtomicBool::new(false));

    let lim_up = limiter.clone();
    let c1_tail = cancel.clone();
    let c1 = cancel.clone();
    let up_err = up_errored.clone();
    let t1 = tokio::spawn(async move {
        let mut b_write = b_write;
        let up = pump_up(&mut a_r, &mut b_write, &lim_up);
        let cancel_wait = async {
            match c1 {
                Some(c) => c.await,
                None => std::future::pending().await,
            }
        };
        // clean=true:up 泵以 EOF 干净结束(半关闭语义,等完整 FIN);
        // clean=false:读/写已 Err(连接已坏,有界收尾);
        // by_cancel:规则取消(有界收尾)。
        let (by_cancel, clean) = tokio::select! {
            clean = up => (false, clean),
            _ = cancel_wait => (true, false),
        };
        if !clean {
            up_err.store(true, Ordering::SeqCst);
        }
        // EOF 传 FIN(对端读侧收敛)。取消/错误路径有界等待:
        // poll_close 永久 Pending(含 yamux 命令队列满)时不得卡死收尾、
        // 不得扣住 IO(rule-cancel 复现:join 永不完成、io_dropped=false)。
        if by_cancel || !clean {
            let _ = tokio::time::timeout(CANCEL_CLOSE_GRACE, b_write.close()).await;
        } else {
            // clean EOF:无界等完整 FIN(红线,不加硬超时防截断);但收尾
            // 期间规则取消仍必须被听见(cancel-after-eof 复现:EOF 进入
            // close 后取消永不再被监听 → relay 卡死、IO 不释放)。
            let cancel_tail = async {
                match c1_tail {
                    Some(c) => c.await,
                    None => std::future::pending().await,
                }
            };
            close_with_late_cancel(b_write.close(), cancel_tail).await;
        }
    });

    let lim_dn = limiter;
    let c2 = cancel.clone();
    let c2_tail = c2.clone();
    let dn_err = dn_errored.clone();
    let t2 = tokio::spawn(async move {
        let mut b_read = b_read;
        let down = pump_down(&mut a_w, &mut b_read, &lim_dn);
        let cancel_wait = async {
            match c2 {
                Some(c) => c.await,
                None => std::future::pending().await,
            }
        };
        let (by_cancel, clean) = tokio::select! {
            clean = down => (false, clean),
            _ = cancel_wait => (true, false),
        };
        if !clean {
            dn_err.store(true, Ordering::SeqCst);
        }
        // 同上:取消/错误路径 shutdown 有界等待,clean EOF 路径保持
        // 完整半关闭语义(红线);收尾期间取消仍要被听见
        if by_cancel || !clean {
            let _ = tokio::time::timeout(CANCEL_CLOSE_GRACE, a_w.shutdown()).await;
        } else {
            let cancel_tail = async {
                match c2_tail {
                    Some(c) => c.await,
                    None => std::future::pending().await,
                }
            };
            close_with_late_cancel(a_w.shutdown(), cancel_tail).await;
        }
    });

    // 收尾 join(A1a + 收敛保证):
    // * 父取消(本 future 被 drop)→ 守卫 Drop → abort 双向子泵;
    // * 单向 panic 或单向错误退出 → 立即终止对向(对向可能在等一个
    //   永远不来的数据/EOF,不得永存);
    // * 单向 clean EOF → 等对向自然结束(半关闭语义,红线)。
    let mut g1 = crate::AbortOnDropHandle::new(t1);
    let mut g2 = crate::AbortOnDropHandle::new(t2);
    let first = tokio::select! {
        _ = &mut g1 => 1,
        _ = &mut g2 => 2,
    };
    let abnormal = match first {
        1 => g1.panicked() || up_errored.load(Ordering::SeqCst),
        _ => g2.panicked() || dn_errored.load(Ordering::SeqCst),
    };
    if abnormal {
        return; // 守卫 Drop → abort 对向(及未完成侧)
    }
    let _ = tokio::join!(g1, g2);
}

/// up 侧搬运:a 读 → mux 帧体。限速=读侧计费:fill 完成即预留并即时消费
/// (预留等待期间父/取消经外层 select 释放本 future → 预留自动退还,不丢令牌)。
/// 返回 true = 以 clean EOF 结束;false = 读/写错误(调用方有界收尾)。
async fn pump_up<R: tokio::io::AsyncRead + Unpin>(
    a_r: &mut R,
    b_write: &mut RhMuxStreamWrite,
    limiter: &Option<std::sync::Arc<dyn crate::ChunkLimiter>>,
) -> bool {
    let mut reader = UpReader::default();
    loop {
        let chunk = match reader.read_chunk(a_r).await {
            Ok(Some(c)) => c,
            Ok(None) => return true,
            Err(_) => return false,
        };
        if let Some(l) = limiter {
            let mut r = l.reserve(chunk.len()).await;
            r.consume(chunk.len());
        }
        if pump_diag::enabled() {
            pump_diag::UP_BYTES.fetch_add(chunk.len() as u64, std::sync::atomic::Ordering::Relaxed);
            pump_diag::UP_CHUNKS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        if b_write.write_chunk(chunk).await.is_err() {
            return false;
        }
    }
}

/// down 侧小块聚合的段数上限(round-8 优化项 4):一次冲刷的 writev
/// 至多聚合这么多小块。128 段栈上 IoSlice ≈ 2KB,远低于 Linux
/// IOV_MAX=1024;小块极端碎片化(每块 <512B)时由本上限先于字节上限
/// 触发冲刷,保证 writev 段数组有界。
const SMALL_CHUNK_SEGS: usize = 128;

/// down 侧搬运:mux 块 → a 写。满块(≥64KiB)直写(零拷贝单次 syscall);
/// 小块聚合(round-8 优化项 4:改向量写——小块以 `Bytes` 引用计数暂存
/// `Vec<Bytes>` 收集数组,0 次内存拷贝;冲刷时构造 IoSlice 段数组经
/// `write_vectored` 一次下发,彻底消除旧 `BytesMut::extend_from_slice`
/// 聚合槽的用户态二次 memcpy),聚合上限 64KiB / 128 段先到先冲,
/// 无后续就绪块即冲刷保延迟(节奏与旧 copy 路径一致:降低 syscall/
/// 唤醒频率)。
///
/// 限速=送达计费(A3/A5):字节入收集数组不扣费;冲刷/直写前按写出量
/// 预留,按实际写出逐步消费——取消/部分写时未消费预留自动退还,
/// 总扣费恒等于已送达字节(修前:聚合块先按 len 扣费,push_front 回退
/// 余量后被再次扣费,×3 复现多扣 32768B)。
///
/// 返回 true = 以 clean EOF 结束(b_read Ok(None));false = 读/写错误
/// (调用方收尾走有界 grace)。
async fn pump_down<W: tokio::io::AsyncWrite + Unpin>(
    a_w: &mut W,
    b_read: &mut RhMuxStreamRead,
    limiter: &Option<std::sync::Arc<dyn crate::ChunkLimiter>>,
) -> bool {
    use std::io::IoSlice;

    /// 预留 n 字节并写出 b,按实际写出消费;取消/写失败退款未消费部分。
    /// 返回 false = 写侧不可用(本轮结束)。
    async fn write_metered<W: tokio::io::AsyncWrite + Unpin>(
        w: &mut W,
        b: &Bytes,
        limiter: &Option<std::sync::Arc<dyn crate::ChunkLimiter>>,
    ) -> bool {
        use tokio::io::AsyncWriteExt as _;
        let mut res = match limiter {
            Some(l) => Some(l.reserve(b.len()).await),
            None => None,
        };
        let mut off = 0usize;
        while off < b.len() {
            match w.write(&b[off..]).await {
                Ok(0) => return false, // WriteZero:写侧不可用
                Ok(k) => {
                    off += k;
                    if let Some(r) = res.as_mut() {
                        r.consume(k);
                    }
                }
                Err(_) => return false,
            }
        }
        true
        // res(若有未消费余量)在返回时 drop → 自动退还
    }

    /// 多段向量写(优化项 4 核心):预留剩余总量,跨段推进部分写,按实际
    /// 写出逐步消费;取消/写失败退款未消费部分。返回 false = 写侧不可用。
    async fn write_metered_vectored<W: tokio::io::AsyncWrite + Unpin>(
        w: &mut W,
        chunks: &[Bytes],
        limiter: &Option<std::sync::Arc<dyn crate::ChunkLimiter>>,
    ) -> bool {
        use tokio::io::AsyncWriteExt as _;
        let total: usize = chunks.iter().map(|c| c.len()).sum();
        debug_assert!(total > 0, "caller flushes only non-empty batches");
        let mut res = match limiter {
            Some(l) => Some(l.reserve(total).await),
            None => None,
        };
        // (seg, off):当前未写起点(段索引 + 段内偏移);每轮从该点拼段
        // 一次 writev,写出 k 后跨段烧穿推进。段数组栈上构造,零堆分配。
        let mut seg = 0usize;
        let mut off = 0usize;
        let mut written = 0usize;
        while written < total {
            let mut slices = [IoSlice::new(&[]); SMALL_CHUNK_SEGS];
            let mut nsegs = 0usize;
            for (i, c) in chunks[seg..].iter().enumerate() {
                if nsegs == SMALL_CHUNK_SEGS {
                    break;
                }
                let s = if i == 0 { &c[off..] } else { &c[..] };
                slices[nsegs] = IoSlice::new(s);
                nsegs += 1;
            }
            match w.write_vectored(&slices[..nsegs]).await {
                Ok(0) => return false, // WriteZero:写侧不可用
                Ok(k) => {
                    written += k;
                    if let Some(r) = res.as_mut() {
                        r.consume(k);
                    }
                    let mut rem = k;
                    while rem > 0 {
                        let left = chunks[seg].len() - off;
                        if rem >= left {
                            seg += 1;
                            off = 0;
                            rem -= left;
                        } else {
                            off += rem;
                            rem = 0;
                        }
                    }
                }
                Err(_) => return false,
            }
        }
        true
        // res(若有未消费余量)在返回时 drop → 自动退还
    }

    /// 冲刷小块收集数组(空数组为 no-op;Vec capacity 跨轮复用)。
    async fn flush<W: tokio::io::AsyncWrite + Unpin>(
        w: &mut W,
        smalls: &mut Vec<Bytes>,
        smalls_bytes: &mut usize,
        limiter: &Option<std::sync::Arc<dyn crate::ChunkLimiter>>,
    ) -> bool {
        if smalls.is_empty() {
            return true;
        }
        let ok = write_metered_vectored(w, smalls, limiter).await;
        smalls.clear();
        *smalls_bytes = 0;
        ok
    }

    let mut smalls: Vec<Bytes> = Vec::new();
    let mut smalls_bytes = 0usize;
    let clean = 'outer: loop {
        let __t0 = if pump_diag::enabled() { Some(std::time::Instant::now()) } else { None };
        let chunk = match b_read.read_chunk().await {
            Ok(Some(c)) => c,
            Ok(None) => break 'outer true,
            Err(_) => break 'outer false,
        };
        if let Some(t) = __t0 {
            let el = t.elapsed();
            if el.as_millis() >= 1 {
                pump_diag::SLOW_READS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            if el.as_millis() >= 10 {
                pump_diag::VERY_SLOW_READS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            pump_diag::READ_NS.fetch_add(el.as_nanos() as u64, std::sync::atomic::Ordering::Relaxed);
            pump_diag::READS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            pump_diag::CHUNKS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            pump_diag::BYTES.fetch_add(chunk.len() as u64, std::sync::atomic::Ordering::Relaxed);
        }
        if chunk.len() >= CHUNK_CAP {
            if !flush(a_w, &mut smalls, &mut smalls_bytes, limiter).await {
                break 'outer false;
            }
            let __w0 = if pump_diag::enabled() { Some(std::time::Instant::now()) } else { None };
            let __ok = write_metered(a_w, &chunk, limiter).await;
            if let Some(t) = __w0 {
                let el = t.elapsed();
                pump_diag::WRITE_NS.fetch_add(el.as_nanos() as u64, std::sync::atomic::Ordering::Relaxed);
                pump_diag::WRITES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                if el.as_millis() >= 1 {
                    pump_diag::SLOW_WRITES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
            }
            if !__ok {
                break 'outer false;
            }
            continue;
        }
        if chunk.is_empty() {
            continue;
        }
        // 小块:仅增加 Bytes 引用计数暂存(0 拷贝)
        smalls_bytes += chunk.len();
        smalls.push(chunk);
        if smalls_bytes >= CHUNK_CAP || smalls.len() >= SMALL_CHUNK_SEGS {
            if !flush(a_w, &mut smalls, &mut smalls_bytes, limiter).await {
                break 'outer false;
            }
            continue;
        }
        // 聚合后续立即就绪的块(入收集数组的字节冲刷时统一预留/消费)
        loop {
            match b_read.try_read_chunk().await {
                Ok(Some(c)) if !c.is_empty() => {
                    smalls_bytes += c.len();
                    smalls.push(c);
                    if smalls_bytes >= CHUNK_CAP || smalls.len() >= SMALL_CHUNK_SEGS {
                        if !flush(a_w, &mut smalls, &mut smalls_bytes, limiter).await {
                            break 'outer false;
                        }
                        break;
                    }
                }
                Ok(Some(_)) => {}
                Ok(None) | Err(_) => {
                    if !flush(a_w, &mut smalls, &mut smalls_bytes, limiter).await {
                        break 'outer false;
                    }
                    break;
                }
            }
        }
    };
    let _ = flush(a_w, &mut smalls, &mut smalls_bytes, limiter).await;
    pump_diag::dump("down");
    clean
}

/// BoxedIo 是否未包装的 mux 流(转发路径零拷贝资格判定);
/// 被 QuotaIo/PrependedIo/压缩包装时为 false,调用方回落拷贝路径。
pub fn is_mux_stream(io: &BoxedIo) -> bool {
    io.as_any().downcast_ref::<RhMuxStream>().is_some()
}

/// 从未包装 mux 流中偷出读/写半(经 &mut,壳由调用方丢弃);
/// 非 mux 流返回 None。读半自带兼容路径余量,不丢数据。
pub fn steal_mux_halves(io: &mut BoxedIo) -> Option<(RhMuxStreamRead, RhMuxStreamWrite)> {
    io.as_any_mut()
        .downcast_mut::<RhMuxStream>()?
        .steal_halves()
}

/// yamux 连接载体:futures IO 视图的 BoxedIo + 向量写(writev)。
/// poll_write2 把 [帧头, 帧体] 以 writev 单 syscall 下发;poll_writev
/// (round-5 §3 跨帧批量写)把发送半聚合的整批帧(≤33 段)一次下发;
/// 不支持向量写的载体(tokio 默认实现)自动退化为顺序写。
pub struct YamuxCarrier {
    inner: BoxedIo,
}

impl YamuxCarrier {
    fn new(inner: BoxedIo) -> Self {
        Self { inner }
    }
}

impl futures::AsyncRead for YamuxCarrier {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        let mut rb = ReadBuf::new(buf);
        match Pin::new(&mut self.inner).poll_read(cx, &mut rb) {
            Poll::Ready(Ok(())) => Poll::Ready(Ok(rb.filled().len())),
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl futures::AsyncWrite for YamuxCarrier {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }
    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }
    fn poll_close(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

impl yamux::Writev2 for YamuxCarrier {
    fn poll_write2(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        a: &[u8],
        b: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = &mut *self;
        if a.is_empty() {
            return Pin::new(&mut this.inner).poll_write(cx, b);
        }
        if b.is_empty() {
            return Pin::new(&mut this.inner).poll_write(cx, a);
        }
        let bufs = [std::io::IoSlice::new(a), std::io::IoSlice::new(b)];
        Pin::new(&mut this.inner).poll_write_vectored(cx, &bufs)
    }

    /// round-5 §3.1:多段批量写直接走 tokio 的 `poll_write_vectored`
    /// (writev 单 syscall 下发整批);不支持向量写的载体(tokio 默认
    /// 实现)自动退化为顺序写,语义不变。
    fn poll_writev(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        bufs: &[std::io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let this = &mut *self;
        Pin::new(&mut this.inner).poll_write_vectored(cx, bufs)
    }
}

/// "不限制"复用流的工程上限(配置值 0 折算到此)。
/// yamux 流 ID 为 u32 奇偶分侧,理论极限 2^31;实际并发由内存兜底,
/// 此值只为让 0=不限 有一个有限的落点。64 位限定:256KiB×此值 = 4TiB。
pub const MUX_STREAMS_UNLIMITED: usize = 1 << 24;

/// 默认 shard 数的配置入口(仅测试/既有断言使用)。
///
/// round-10 起生产调用点一律显式传 shard 数,本函数只保留"默认 4"的
/// 既有语义供 [`mux_config_consumes_default_four_shard_workers`] 使用;
/// `#[cfg(test)]` 避免 -D warnings 下成为死代码(生产等价路径为
/// `mux_config_with_shards(max, DEFAULT_SHARD_WORKERS)`)。
#[cfg(test)]
fn mux_config(max_streams: usize) -> yamux::Config {
    mux_config_with_shards(max_streams, DEFAULT_SHARD_WORKERS)
}

/// 分片 Worker 默认数(vendor 默认,round-5 起被三机验收锁定)。
pub const DEFAULT_SHARD_WORKERS: usize = 4;

/// 池规模 → 每载体 shard 数(round-10):总 Worker 预算按 4 计,
/// K 越大每载体分得越少;下限 1。K=1 必须仍为 4(红线:K=1 零回退)。
pub fn shard_workers_for_k(k: usize) -> usize {
    (DEFAULT_SHARD_WORKERS / k.max(1)).max(1)
}

fn mux_config_with_shards(max_streams: usize, shard_workers: usize) -> yamux::Config {
    let mut cfg = yamux::Config::default();
    let streams = if max_streams == 0 {
        MUX_STREAMS_UNLIMITED
    } else {
        max_streams
    };
    // ---- 流控对齐 frp v0.2.0(round-E1,2026-10-16 源码级核对)----
    // fatedier/yamux(frp 实际锁定 v0.2.0)三条事实:
    // 1. OpenStream(session.go L200)/AcceptStream(L247)建流即
    //    sendWindowUpdate——我方 Round-B1 已对齐(SYN|WU / ACK|WU
    //    建流授信),保留;
    // 2. MaxStreamWindowSize 恒定(stream.go L284
    //    `max := config.MaxStreamWindowSize`),frp 两端 fmux 同设
    //    6MiB,窗口永不自增——删除 paritytech 上游继承的 BDP 自增窗
    //    (其与预算钳制/预授信守卫的相互作用是本 bug 类来源;我方
    //    Round-3 +9.1% 单流增益只依赖 6MiB 初窗 + B1 建流授信,
    //    不依赖自增);
    // 3. 无连接级共享预算、无 BDP/auto-tune(全源码无此逻辑)——删除
    //    自研 256MiB 共享预算(多流上已被证实"先到先得 + 耗尽冻结":
    //    ≥45 并发流后新流被钳死在 256KiB,是整类记账 bug 的温床)。
    // 每流接收窗恒等于初窗 6MiB,与流数无关(4096 流同窗,见
    // flow_control.rs every_stream_gets_full_window_regardless_of_count)。
    //
    // 内存护栏:聚合在途敞口 = 并发流数 × 6MiB,唯一护栏是
    // max_num_streams(rh-server 的 limits.mux_streams;0=不限时折算
    // MUX_STREAMS_UNLIMITED,敞口无界,与 frp 行为一致,生产建议
    // 显式配置流上限)。
    //
    // 连接级接收窗上限解除(None,对齐 frp"无连接级共享预算");
    // 上游 API 兼容保留该字段(vendor lib.rs 注释)。setter 断言相互
    // 校验当前值:必须先解除连接窗上限(None 对默认 512 流断言成立)
    // 再提高流数(对 usize::MAX 恒成立;反序:默认 1GiB 总窗对
    // 2^24 流保底乘积 4TiB 断言必炸)。断言语义零放宽。
    cfg.set_max_connection_receive_window(None);
    cfg.set_max_num_streams(streams);
    // 分片帽对齐 frp(2026-10-15):frp 的 fmux 无分片发送概念,帧大小
    // 上限即发送窗(见 benchmark/runs/tcp-multiflow-frp-20261015/
    // FRP-YAMUX-ANALYSIS.md §2.1)。上游默认 16KiB 曾提到 64KiB,
    // 现抬到 6MiB=单流窗口界,即"不再人为分片"。数据面经 CHUNK_CAP
    // =64KiB 块写入(mux.rs 中继路径),实际上线帧大小不变,此改动
    // 只移除人为帽,行为等价 frp 的"写多少发多少,上限受 sendWindow"。
    cfg.set_split_send_size(6 * 1024 * 1024);
    // 单流初始接收窗 = 恒定接收窗(round-2,goal-repair G2/G6;round-3
    // 对齐 frp):
    // 【实测】round-1 三机测试床(benchmark/runs/goal-repair/round-1/
    // single-stream-medians.txt):tcp/tls/ws/wss mux 单流全部卡在
    // 1051-1225 Mbps 上行 / 1468-1962 Mbps 下行,与是否 TLS 无关
    // (tcp-mux 无 TLS 与 tls-mux 同卡);同路径 16 流 2160-2970 Mbps
    // ——每字节成本解释不了,瓶颈随"流数"摊薄,指向每流独立约束
    // (yamux 流控信用回路)。
    // 【假设,未闭合】信用回路时延 > 2×ping RTT,RTT 门控的自增窗
    // 不触发,单流被钳在 初窗/回路时延;本地合成 RTT 实验
    // (goal_repair_mux.rs mux_single_stream_credit_stall_under_synthetic_rtt)
    // 复现钳制机制,测试床 RTT/回路时延待下一轮实测。
    // 【修复】round-2 接收端建流即从 2MiB 起步;round-3(2026-10-15)
    // 抬到 6MiB,对齐 frp 两端同设的 fmux MaxStreamWindowSize=6MiB
    // (FRP-YAMUX-ANALYSIS.md §2.1)。round-4/B1(2026-10-15)起差额
    // 在建流时以带初始 flag 的窗口更新帧授出(outbound SYN|WU /
    // inbound ACK|WU,upstream 合法组合,对齐 frp OpenStream/
    // AcceptStream),不再等首个读 poll;round-E1(2026-10-16)起该
    // 窗口恒定——不再受共享预算钳制、不再 BDP 自增(frp 对照与删除
    // 理由见上)。发送端初窗仍 256KiB——对端只见标准 WU,线路兼容。
    // 本改动只覆盖 yamux(mux 路径),不声称修复 nomux/QUIC/KCP。
    cfg.set_initial_stream_receive_window(6 * 1024 * 1024);
    // 分片 Worker 数(round-5 §2.7 锁默认 4;round-10 起由调用方按池规模
    // 传入)。该值只作用于 halves 路径(本文件两路入口均走 halves),未
    // 拆分 Active 路径忽略。`shard_workers == DEFAULT_SHARD_WORKERS` 时与
    // round-5 行为逐字节等价(既有验收断言原样绿);K>1 客户端经
    // [`shard_workers_for_k`] 缩减,使总 Worker 数 ≈ 4 而非 4K。
    cfg.set_shard_workers(shard_workers.clamp(1, 64));
    cfg
}

pub struct YamuxMultiplexer {
    open_tx: mpsc::Sender<oneshot::Sender<io::Result<yamux::Stream>>>,
    accept_rx: tokio::sync::Mutex<mpsc::Receiver<yamux::Stream>>,
    dead: Arc<AtomicBool>,
    /// 收发 driver 的句柄(round-9.1):仅 [`YamuxMultiplexer::shutdown`]
    /// 消费——Drop 仍保持"句柄丢弃即分离、driver 继续服务存量流"的既有
    /// 语义,不在此 abort(红线:正常路径零漂移)。
    drivers: std::sync::Mutex<Vec<tokio::task::JoinHandle<()>>>,
}

impl YamuxMultiplexer {
    /// 在给定字节流上创建 yamux(锁路径);mode:客户端(开流)/服务端
    /// (收流)。max_streams:并发流硬顶,0 = 不限制(折算
    /// MUX_STREAMS_UNLIMITED)。shard_workers:收发两侧分片 Worker 数
    /// (round-10 参数化;单载体传 [`DEFAULT_SHARD_WORKERS`] 保持 round-5
    /// 行为)。
    /// 注:yamux 0.14 无内建 keepalive,会话存活依赖协议层心跳(< 会话空闲超时)。
    pub fn new(io: BoxedIo, mode: yamux::Mode, max_streams: usize, shard_workers: usize) -> Self {
        let conn: MuxConn = yamux::Connection::new(
            YamuxCarrier::new(io),
            mux_config_with_shards(max_streams, shard_workers),
            mode,
        );
        // Round-D:收/发双任务驱动(对齐 Go yamux recv/send 双 goroutine)。
        // 单一载体经 SplitSocket/BiLock 串行——TLS/WS/QUIC/KCP 等不可
        // fd 复制的载体走此路径(泛型兜底)。Round-5 起三元组扩为
        // round-6 四元组:第三件是 N 个接收侧分片 Worker、第四件是 N 个
        // 发送侧分片 Worker(命令消费/帧转换/on_drop 分摊,数量由
        // Config.shard_workers 决定,见 [`mux_config`]),与两半一同 spawn。
        let (recv_half, send_half, workers, send_workers) = conn.into_halves();
        Self::spawn_drivers(recv_half, send_half, workers, send_workers)
    }

    /// 双载体入口(round-E2,裸 TCP fd-dup):read_io 归接收任务、write_io
    /// 归发送任务,halves 之间零锁,读∥写在载体层物理并行(对齐 frp/fmux
    /// 收发双 goroutine 各持 conn 读写的事实形态)。driver 的 spawn、
    /// open_tx(64)/accept_tx(256) 容量与背压、dead 语义与锁路径
    /// 完全一致(共用 [`YamuxMultiplexer::spawn_drivers`])。
    /// 调用契约:read_io/write_io 必须指向同一全双工 socket(如同一
    /// TcpStream 的两次 try_clone;见 vendor `Connection::into_halves_duplex`
    /// 的方向纪律与 shutdown 语义注释)。
    pub fn new_duplex(
        read_io: BoxedIo,
        write_io: BoxedIo,
        mode: yamux::Mode,
        max_streams: usize,
        shard_workers: usize,
    ) -> Self {
        let conn: MuxConn = yamux::Connection::new(
            YamuxCarrier::new(read_io),
            mux_config_with_shards(max_streams, shard_workers),
            mode,
        );
        let (recv_half, send_half, workers, send_workers) =
            conn.into_halves_duplex(YamuxCarrier::new(write_io));
        Self::spawn_drivers(recv_half, send_half, workers, send_workers)
    }

    /// 收/发双任务 + N 个接收侧分片 Worker + N 个发送侧分片 Worker 任务
    /// 驱动(两路径共用):接收任务解帧+路由,新流入 accept_tx(容量 256
    /// 背压——接收阻塞只停读侧,不影响发送任务);发送任务(FlushTask)
    /// 处理开流请求 + 写路径(唯一 socket 写者:send_out 轮转取帧、批量
    /// writev);RecvWorker 独占 `sid % N` 流集合的接收派发(Round-5),
    /// SendWorker 独占同模数分片的流命令消费/帧转换/on_drop(round-6,
    /// 发送面计算分摊)。S: Send + 'static 供 tokio::spawn(halves 的
    /// Send/生命周期随 socket 类型);[`yamux::ShardWorker`] 与
    /// [`yamux::SendWorker`] 均不带 socket 泛型参数,其 `Send + 'static`
    /// 由 vendor 侧结构保证(Arc/Mutex/原子,无 Rc)。
    ///
    /// Worker 生命周期:JoinHandle 丢弃即分离(同两半的 driver);Worker
    /// 在观察到共享错误槽(任一半 fatal / RecvHalf Drop 置 Closed)后自行
    /// `Ready(())` 终结并排空本地流表(SendWorker 另在 send_dropped 后
    /// 终结),无需 abort——与 vendor §2.6 生命周期矩阵一致,不存在
    /// Worker 永存泄漏面。
    ///
    /// round-9.1 起两个 driver 的 JoinHandle 存入
    /// [`YamuxMultiplexer::drivers`]:正常路径语义不变(Drop 不 abort),
    /// 池关停/会话拆除经 [`YamuxMultiplexer::shutdown`] 显式消费。
    fn spawn_drivers<S>(
        recv_half: yamux::RecvHalf<S>,
        send_half: yamux::SendHalf<S>,
        workers: Vec<yamux::ShardWorker>,
        send_workers: Vec<yamux::SendWorker>,
    ) -> Self
    where
        S: futures::AsyncRead + futures::AsyncWrite + Unpin + yamux::Writev2 + Send + 'static,
    {
        let (open_tx, open_rx) = mpsc::channel::<oneshot::Sender<io::Result<yamux::Stream>>>(64);
        let (accept_tx, accept_rx) = mpsc::channel::<yamux::Stream>(256);
        let dead = Arc::new(AtomicBool::new(false));
        // 双侧 Worker 先于两半 spawn(与 RecvWorker 同批):消费侧未起即
        // 满 → 生产侧 park(RecvWorker 满 → Framer 停读;send_out 满 →
        // SendWorker 停止消费命令),顺序不影响正确性(丢唤醒防线保证
        // 消费侧起后必被唤醒),只是让首批帧少一次 park。
        for w in workers {
            tokio::spawn(w);
        }
        for w in send_workers {
            tokio::spawn(w);
        }
        let send = tokio::spawn(send_driver(send_half, open_rx, dead.clone()));
        let recv = tokio::spawn(recv_driver(recv_half, accept_tx, dead.clone()));
        Self {
            open_tx,
            accept_rx: tokio::sync::Mutex::new(accept_rx),
            dead,
            drivers: std::sync::Mutex::new(vec![send, recv]),
        }
    }

    pub async fn open_stream(&self) -> io::Result<BoxedIo> {
        let (tx, rx) = oneshot::channel();
        self.open_tx
            .send(tx)
            .await
            .map_err(|_| other("mux driver gone"))?;
        let stream = rx
            .await
            .map_err(|_| other("mux driver dropped request"))??;
        Ok(Box::new(RhMuxStream::new(stream)))
    }

    pub async fn accept_stream(&self) -> io::Result<BoxedIo> {
        let mut guard = self.accept_rx.lock().await;
        match guard.recv().await {
            Some(s) => Ok(Box::new(RhMuxStream::new(s))),
            None => Err(other("mux connection closed")),
        }
    }

    pub fn is_dead(&self) -> bool {
        self.dead.load(Ordering::Relaxed)
    }

    /// 显式关停(round-9.1):中止收发 driver——物理连接随 socket drop
    /// 关闭,`recv_driver` 中止后 `accept_rx` 生产端消失,`accept_stream`
    /// 立即返回错误,转发任务据此自然收敛。
    ///
    /// 幂等:`std::mem::take` 消费句柄,重复/并发调用只会 abort 一次。
    /// **只由池关停 / 会话拆除调用**;Drop 不调用(既有语义是"mux 句柄
    /// 丢弃后 driver 继续服务存量流",见 `drivers` 字段注释)。
    pub fn shutdown(&self) {
        self.dead.store(true, Ordering::Release);
        let handles: Vec<_> = std::mem::take(&mut *self.drivers.lock().unwrap());
        for h in handles {
            h.abort();
        }
    }

    /// 取出 driver 句柄但不 abort(round-9.1 池 Drop 延迟收尾专用):
    /// 置 dead 阻止新开流,句柄交由调用方在宽限期后 abort。与
    /// [`YamuxMultiplexer::shutdown`] 共享"take 后为空"的幂等性。
    fn take_drivers(&self) -> Vec<tokio::task::JoinHandle<()>> {
        self.dead.store(true, Ordering::Release);
        std::mem::take(&mut *self.drivers.lock().unwrap())
    }
}

// ===== 多物理载体连接池聚合(round-8 优化项 5) =====
//
// 架构:池持有 K 个独立物理载体(每载体一套完整 YamuxMultiplexer——独立
// 收发 driver、独立分片 Worker、独立 TCP/TLS 会话)。业务流按全局原子
// 计数轮转分派(i = stream_seq mod K):K 条连接具有不同本地源端口
// (不同四元组),网卡 RSS 硬件自动散列到不同 CPU 核处理软中断;TLS
// 模式下 K 条连接 = K 组完全并行的 Record 加解密流水线,突破单连接
// 串行加解密上限。某载体异常断开时从活跃池剔除并自动重拨,其余载体
// 上的存量流不受影响(消除单 TCP 物理拥塞丢包的全局队头阻塞)。

/// 池推荐默认载体数(任务规格:K=4)。
pub const YAMUX_POOL_DEFAULT_CARRIERS: usize = 4;

/// 重拨配方:重新建立一条物理载体并包装为 YamuxMultiplexer(客户端侧;
/// 服务端成员不可主动拨号,死亡即永久剔除)。
pub type PoolRedial = Arc<
    dyn Fn() -> futures::future::BoxFuture<'static, io::Result<YamuxMultiplexer>> + Send + Sync,
>;

/// 重拨连续失败上限:退避 200ms 起步 ×2 封顶 5s,累计约 2 分钟后放弃
/// (池成员表空则置死——open_stream 的等待因此有界,不永挂)。
const REDIAL_MAX_ATTEMPTS: u32 = 16;
const REDIAL_BACKOFF_START: std::time::Duration = std::time::Duration::from_millis(200);
const REDIAL_BACKOFF_MAX: std::time::Duration = std::time::Duration::from_secs(5);

/// 服务端 accept 合流通道容量(对齐单 mux accept_rx 的 256 背压)。
const POOL_ACCEPT_CAP: usize = 256;

/// detach 的两种定位方式(round-10.1):Round-10 的下标入口保留原语义,
/// 新增的身份入口不受成员表压缩影响。
#[derive(Clone, Copy)]
enum DetachBy {
    Index(usize),
    Id(u64),
}

/// 单个物理载体成员。
struct PoolMember {
    /// 池内稳定身份(round-10.1):成员创建时分配,单调递增;重拨补位的
    /// 新成员拿新 id。用于在成员表压缩/重拨并发下精确定位载体(下标会
    /// 漂移)。
    id: u64,
    mux: YamuxMultiplexer,
    /// 重拨配方(None = 服务端成员/已 detach:不可重拨,死亡即永久剔除)。
    ///
    /// round-10 起为 `Mutex`:detach 必须在成员表写锁内先清配方再
    /// shutdown——`reap_member` 在另一个任务里读同一字段,顺序颠倒会被
    /// 视为载体死亡并触发重拨(5s 重拨环)。锁内只做 Option 读写,无 await。
    redial: std::sync::Mutex<Option<PoolRedial>>,
}

impl PoolMember {
    fn new(id: u64, mux: YamuxMultiplexer, redial: Option<PoolRedial>) -> Self {
        Self {
            id,
            mux,
            redial: std::sync::Mutex::new(redial),
        }
    }

    /// 取重拨配方的克隆(shutdown 后仍可能被已在途的 redial_loop 观察)。
    fn redial(&self) -> Option<PoolRedial> {
        self.redial.lock().unwrap().clone()
    }

    /// 永久关闭该成员的重拨(幂等);返回是否此前存在配方。
    fn clear_redial(&self) -> bool {
        self.redial.lock().unwrap().take().is_some()
    }
}

struct PoolInner {
    /// 活跃成员表(死亡即剔除、重拨成功即追加)。open_stream 只取读锁
    /// 快照;剔除/追加走写锁。
    members: tokio::sync::RwLock<Vec<Arc<PoolMember>>>,
    /// 全局流分派轮转计数(open_stream 热路径原子自增,无锁)。
    rr: AtomicUsize,
    /// 池死:成员表空且无在途重拨。
    dead: AtomicBool,
    /// 成员表变更通知(剔除/追加后唤醒等待中的 open_stream/accept)。
    changed: tokio::sync::Notify,
    /// 池关停(终止重拨循环;不再接受新成员)。
    shutdown: AtomicBool,
    /// 在途重拨任务数(全灭时的死亡判定依据)。
    redialing: AtomicUsize,
    /// 成员身份分配器(round-10.1):初始 1,单调递增;重拨/追加补位
    /// 不复用旧 id(摘除按 id 定位,复用会把新成员误认成旧成员)。
    next_member_id: AtomicU64,
    /// 轮转分派前缀(round-10 自动定容):open_stream 只在成员表前
    /// `active_prefix` 个成员中分派;`usize::MAX` = 不限(默认,零漂移)。
    /// 只影响客户端 open_stream;服务端 accept 合流不受影响。
    active_prefix: AtomicUsize,
    /// 重拨补位目标数(round-10.1):成员表长度已达到该值时,`redial_loop`
    /// 在入表前丢弃本次拨号。`set_active_prefix` 同步设置(取未按当前表长
    /// 钳制的请求值);`usize::MAX` = 不限(默认,既有语义零漂移)。
    ///
    /// 存在理由:收敛前的 `reap_member` 可能已经 spawn 了重拨,而拨号受
    /// `connect_timeout`(默认 10s)约束。仅在 detach 时清配方无法失效这条
    /// 已克隆的重拨;在入表处按目标数拦截,才能在任意慢拨下都不把物理
    /// 载体数补回 `final_k + 1`。
    redial_target: AtomicUsize,
    /// 服务端合流通道的生产端(round-9 attach_carrier:新成员的 accept
    /// 转发任务复用同一合流通道;None = 客户端池,无 accept 语义,
    /// attach_carrier 不适用)。原本是 new_server 的局部 tx,提升到
    /// inner 以支持运行中追加成员。
    accept_tx: Option<mpsc::Sender<BoxedIo>>,
    /// accept 转发任务句柄(round-9.1):shutdown 时全部 abort,并随池
    /// 释放兜底。追加成员时在成员表写锁内同步登记(临界区无 await),
    /// 消除"已 spawn 未登记"窗口。
    ///
    /// 用 `std::sync::Mutex` 而非 tokio Mutex:池 `shutdown()` 是同步方法
    /// (Drop 路径必须可调用),锁内只做 Vec push/take,无 await。
    forward_tasks: std::sync::Mutex<Vec<tokio::task::JoinHandle<()>>>,
    /// 转发任务已退出条数(round-9.1 测试缝,T-L1):退出守卫在任务正常
    /// 结束或 abort 丢弃 future 时自增。生产构建无此字段,零成本。
    #[cfg(test)]
    forward_exited: AtomicUsize,
}

impl PoolInner {
    /// 成员表(快照 ms)空且无在途重拨 → 置死。
    fn mark_dead_if_exhausted(&self, ms: &[Arc<PoolMember>]) {
        if ms.is_empty() && self.redialing.load(Ordering::Acquire) == 0 {
            self.dead.store(true, Ordering::Release);
        }
    }

    /// 关停竞态兜底(round-9.1 复核):**写锁路径释放锁后**复查 shutdown
    /// 标志并补做收敛。`YamuxMultiplexerPool::shutdown` 的成员快照走
    /// `try_read`,撞上写锁持有者(attach/redial/reap)时只能拿到空表;
    /// 若该写路径不再复查,显式拆除的立即收敛会退化为等 [`PoolInner`]
    /// 最后一次 `Arc` 释放(可能被在途流的 QuotaIo 长期续持)。
    ///
    /// 幂等:重复关停/abort 无害。自身也拿不到读锁时留给下一个写路径
    /// 或 [`PoolInner::Drop`]——写锁临界区均无 await,窗口极短且有界。
    fn shutdown_sweep(&self) {
        if !self.shutdown.load(Ordering::Acquire) {
            return;
        }
        let Ok(ms) = self.members.try_read() else {
            return;
        };
        for m in ms.iter() {
            m.mux.shutdown();
        }
        // 二次保险同 shutdown():转发任务可能停在合流发送或 accept 上。
        let tasks: Vec<_> = std::mem::take(&mut *self.forward_tasks.lock().unwrap());
        for t in tasks {
            t.abort();
        }
    }
}

/// 池本体释放兜底(round-9.1):转发任务被 abort 后它们持有的
/// `Arc<PoolInner>` 随任务结束释放,此时才可能走到这里。若
/// [`YamuxMultiplexerPool::shutdown`] 因成员表写锁被占而没能拿到快照
/// (`try_read` 失败分支),成员表里残留的载体 driver 由本 Drop 关停
/// ——保证"池已释放但物理连接仍活着"不成立。重复 shutdown 幂等
/// (句柄 take 后为空)。
impl Drop for PoolInner {
    fn drop(&mut self) {
        let members = self.members.get_mut();
        for m in members.iter() {
            m.mux.shutdown();
        }
    }
}

/// 池 Drop 的收尾宽限期(round-9.1):Drop 不能 await,若立即 abort 收发
/// driver,已进入 yamux 发送队列但未写穿 socket 的最后一帧会被丢弃(实测:
/// 对端 write_all 后紧随池释放会读到 early eof)。宽限期让发送侧把在途帧
/// 冲刷到内核;超时后无条件 abort,保证 driver 不滞留。取值远小于 T-L3
/// 的 1s EOF 断言,且只在"池整体释放"路径生效(显式 shutdown 立即中止)。
const POOL_DROP_GRACE: std::time::Duration = std::time::Duration::from_millis(250);

/// 多物理载体连接池:open_stream 按全局原子计数轮转分派到第
/// `seq mod K` 个载体;accept(服务端池)自合流通道交付;载体故障自动
/// 剔除 + 退避重拨,其余载体存量流不受影响。
pub struct YamuxMultiplexerPool {
    inner: Arc<PoolInner>,
    /// 服务端合流接收器(None = 客户端池,accept 返回错误)。
    accept_rx: Option<tokio::sync::Mutex<mpsc::Receiver<BoxedIo>>>,
}

/// 转发任务退出探针(round-9.1 测试缝,T-L1/T-L3):Drop 即计数——
/// 无论任务是自然收敛还是被 abort(future 被丢弃),都恰好记一次。
#[cfg(test)]
struct ForwardExitProbe(Arc<PoolInner>);

#[cfg(test)]
impl Drop for ForwardExitProbe {
    fn drop(&mut self) {
        self.0.forward_exited.fetch_add(1, Ordering::AcqRel);
    }
}

impl YamuxMultiplexerPool {
    /// 客户端池:members 为 K 个已建立连接的客户端 YamuxMultiplexer
    /// (≥1);redial 为重拨配方(任一载体死亡时按此重建,新成员继承)。
    pub fn new_client(members: Vec<YamuxMultiplexer>, redial: Option<PoolRedial>) -> Self {
        assert!(!members.is_empty(), "pool needs at least one carrier");
        let next_member_id = AtomicU64::new(1);
        let members: Vec<_> = members
            .into_iter()
            .map(|mux| {
                let id = next_member_id.fetch_add(1, Ordering::Relaxed);
                Arc::new(PoolMember::new(id, mux, redial.clone()))
            })
            .collect();
        let inner = Arc::new(PoolInner {
            members: tokio::sync::RwLock::new(members),
            rr: AtomicUsize::new(0),
            dead: AtomicBool::new(false),
            changed: tokio::sync::Notify::new(),
            shutdown: AtomicBool::new(false),
            redialing: AtomicUsize::new(0),
            next_member_id,
            active_prefix: AtomicUsize::new(usize::MAX),
            redial_target: AtomicUsize::new(usize::MAX),
            accept_tx: None,
            forward_tasks: std::sync::Mutex::new(Vec::new()),
            #[cfg(test)]
            forward_exited: AtomicUsize::new(0),
        });
        spawn_health_watch(&inner);
        Self {
            inner,
            accept_rx: None,
        }
    }

    /// 服务端池:members 为 K 个已 accept 连接的服务端 YamuxMultiplexer;
    /// accept_stream 自全部成员合流交付(FIFO)。成员死亡(该物理连接
    /// 断开)即剔除,不影响其余成员上的存量流。
    pub fn new_server(members: Vec<YamuxMultiplexer>) -> Self {
        assert!(!members.is_empty(), "pool needs at least one carrier");
        let (tx, rx) = mpsc::channel::<BoxedIo>(POOL_ACCEPT_CAP);
        let next_member_id = AtomicU64::new(1);
        let members: Vec<_> = members
            .into_iter()
            .map(|mux| {
                let id = next_member_id.fetch_add(1, Ordering::Relaxed);
                Arc::new(PoolMember::new(id, mux, None))
            })
            .collect();
        let inner = Arc::new(PoolInner {
            members: tokio::sync::RwLock::new(members.clone()),
            rr: AtomicUsize::new(0),
            dead: AtomicBool::new(false),
            changed: tokio::sync::Notify::new(),
            shutdown: AtomicBool::new(false),
            redialing: AtomicUsize::new(0),
            next_member_id,
            active_prefix: AtomicUsize::new(usize::MAX),
            redial_target: AtomicUsize::new(usize::MAX),
            // round-9:tx 提升入 inner(attach_carrier 复用同一合流通道)
            accept_tx: Some(tx.clone()),
            forward_tasks: std::sync::Mutex::new(Vec::new()),
            #[cfg(test)]
            forward_exited: AtomicUsize::new(0),
        });
        for m in members {
            // 句柄登记与成员表构造同属 new_server 的初始化临界区:池尚未
            // 返回,无并发观察者,直接 push 即消除"已 spawn 未登记"窗口。
            let h = Self::spawn_accept_forward(&inner, m, tx.clone());
            inner.forward_tasks.lock().unwrap().push(h);
        }
        spawn_health_watch(&inner);
        Self {
            inner,
            accept_rx: Some(tokio::sync::Mutex::new(rx)),
        }
    }

    /// 运行中追加一个载体成员(round-9 服务端会话聚合):追加 + 接管该
    /// 成员 accept 转发 + 唤醒等待方,三步全部在成员表写锁内完成——
    /// 与 open_stream 的读锁快照互斥,追加后的轮转/合流立即对后续调用
    /// 可见,不出现"已报到但分派不到"的窗口。仅服务端池(有合流通道);
    /// 成员表空转导致的 dead 标志随非空表一并清除(镜像 redial_loop 补位
    /// 语义)。客户端池无 accept 语义,不适用(附加载体由服务端聚合)。
    pub async fn attach_carrier(&self, mux: YamuxMultiplexer) {
        let Some(tx) = self.inner.accept_tx.clone() else {
            return;
        };
        let id = self.inner.next_member_id.fetch_add(1, Ordering::Relaxed);
        let member = Arc::new(PoolMember::new(id, mux, None));
        let mut ms = self.inner.members.write().await;
        if self.inner.shutdown.load(Ordering::Acquire) {
            // 池已关停:新载体直接关停(不能只靠 drop——mux 的 Drop 保持
            // "driver 继续服务存量流"语义,不 abort;见 YamuxMultiplexer
            // 字段注释)。释放写锁后补扫:本次持锁可能正是池 shutdown 的
            // try_read 快照落空的原因(round-9.1 复核)。
            drop(ms);
            member.mux.shutdown();
            self.inner.shutdown_sweep();
            return;
        }
        ms.push(member.clone());
        // 表已非空:清除"表空置死"标志(镜像 redial_loop 补位语义)
        self.inner.dead.store(false, Ordering::Release);
        // spawn 与登记同处成员表写锁临界区内(临界区无 await),消除
        // "已 spawn 未登记"窗口;关停路径必然能看到该句柄。
        let h = Self::spawn_accept_forward(&self.inner, member, tx);
        self.inner.forward_tasks.lock().unwrap().push(h);
        self.inner.changed.notify_waiters();
        drop(ms);
        // 关停竞态复查(round-9.1 复核):上面的标志检查与池 shutdown 的
        // try_read 快照可能交错——shutdown 在检查之后置位、快照又撞上本
        // 写锁时,刚追加的成员不在其快照里。释放写锁后补做一次:shutdown
        // 已置位则关停全部成员(含刚追加者)并 abort 已登记转发任务。
        self.inner.shutdown_sweep();
    }

    /// 池关停:终止重拨循环;进行中的存量流随各自 mux 自然终结(不在池
    /// 层强杀)。关停后新拨载体直接丢弃。
    ///
    /// round-9.1(F1):除置标志外必须把**载体 driver 与 accept 转发任务**
    /// 一并收敛——否则 Session 移除/替换后,空闲载体的物理连接、driver、
    /// 转发任务与 PoolInner 会永久滞留(转发任务强持 PoolInner 与成员)。
    /// 顺序:先关各载体 driver(物理连接随 socket drop 关闭,accept_rx
    /// 随之关闭,转发任务本会自然退出),再 abort 转发任务作二次保险。
    ///
    /// 幂等且并发安全:标志重复置位无害;成员快照取不到与句柄集合都用
    /// `take`/快照语义,重复调用不 panic。
    ///
    /// 成员快照失败(`try_read` 撞上写锁持有者,如 attach_carrier 的追加
    /// 临界区)时的兜底收敛路径:本次只置标志 + abort 已登记转发任务;
    /// 该写锁持有者在释放锁前经 [`PoolInner::shutdown_sweep`] 复查标志并
    /// 补做载体关停(attach_carrier / redial_loop / reap_member 三处),
    /// 且 [`PoolInner`] 最终释放时的 Drop 会关停成员表中残留的全部 driver
    /// ——三条路径保证不出现"标志已置但连接仍活着"的永久泄漏。
    pub fn shutdown(&self) {
        self.inner.shutdown.store(true, Ordering::Release);
        self.inner.changed.notify_waiters();
        let members: Vec<_> = match self.inner.members.try_read() {
            Ok(ms) => ms.clone(),
            Err(_) => Vec::new(),
        };
        for m in members {
            m.mux.shutdown();
        }
        // 二次保险:被 abort 的任务不会再执行成员剔除,但成员 mux 已置 dead,
        // live_carriers 归零;残留的合流接收端随池释放关闭。
        let tasks: Vec<_> = std::mem::take(&mut *self.inner.forward_tasks.lock().unwrap());
        for t in tasks {
            t.abort();
        }
        // try_read 落败:写锁持有者与本次调用并发,它释放锁前后各自会
        // 复查/收敛;此处再试一次把常见的短临界区窗口直接补上。
        self.inner.shutdown_sweep();
    }

    /// 服务端成员 accept 转发:该成员的每个新流入合流通道;载体终结
    /// (accept 返回错误 = 连接断开)时从成员表剔除并通知等待方。
    ///
    /// 返回 JoinHandle 供调用方在成员表写锁内登记(round-9.1 红线:
    /// 不得 spawn 后丢弃句柄又声称能停止)。循环双重收敛:池 shutdown
    /// 标志置位即退出;driver 被中止后 accept_stream 必然返回错误。
    fn spawn_accept_forward(
        inner: &Arc<PoolInner>,
        member: Arc<PoolMember>,
        tx: mpsc::Sender<BoxedIo>,
    ) -> tokio::task::JoinHandle<()> {
        let inner = inner.clone();
        tokio::spawn(async move {
            // 退出守卫(round-9.1 测试缝):正常退出与被 abort 都计数,
            // T-L1 据此断言转发任务已收敛(不需要 await 句柄)。
            #[cfg(test)]
            let _exit_probe = ForwardExitProbe(inner.clone());
            loop {
                if inner.shutdown.load(Ordering::Acquire) {
                    break;
                }
                match member.mux.accept_stream().await {
                    Ok(s) => {
                        if tx.send(s).await.is_err() {
                            break; // 池侧已释放
                        }
                    }
                    Err(_) => break,
                }
            }
            // 本任务因池关停而退出时必须亲自关停载体 driver(round-9.1):
            // 本任务可能是关停瞬间才登记进来的(attach_carrier 窗口),
            // 池 shutdown 的成员快照里没有它;不在此关停则 driver 随成员
            // 表剔除而失去唯一可关停的引用(mux 的 Drop 不 abort)。
            if inner.shutdown.load(Ordering::Acquire) {
                member.mux.shutdown();
            }
            // 该物理载体终结:从成员表剔除并通知等待方
            let mut ms = inner.members.write().await;
            ms.retain(|x| !Arc::ptr_eq(x, &member));
            inner.mark_dead_if_exhausted(&ms);
            drop(ms);
            inner.changed.notify_waiters();
            // 关停竞态复查(round-9.1 复核):本路径也是写锁持有者,池
            // shutdown 的 try_read 可能在此期间落空;释放锁后补做一次
            // (非关停路径为空操作)。sweep 可能 abort 本任务,但此后已
            // 无 await,abort 不改变语义。
            inner.shutdown_sweep();
        })
    }

    /// 剔除死亡成员并(若可)触发重拨;唤醒等待中的 open_stream。
    async fn reap(&self, member: Arc<PoolMember>) {
        reap_member(&self.inner, member).await
    }

    /// 客户端:打开一个新流——按全局原子计数轮转分派到活跃载体
    /// (i = stream_seq mod K)。选中载体死亡 → 剔除并自动重试其余载体;
    /// 全部载体同时死亡时等待重拨落地(全部不可重拨/重拨放弃 → 池死报错)。
    ///
    /// round-10:分派集合先按 [`Self::set_active_prefix`] 收缩到成员表前 n 个
    /// (默认 usize::MAX = 全表,零漂移)。成员被剔除后表会压缩,后续成员
    /// 因此前移进入前缀——这是"前缀"而非"载体身份"的既有语义。
    pub async fn open_stream(&self) -> io::Result<BoxedIo> {
        self.open_stream_inner().await.map(|(s, _id)| s)
    }

    /// 打开一个流并返回其落点成员身份(round-10.1):报到循环据此精确摘除
    /// 失败的载体,不依赖易漂移的下标。语义与 open_stream 完全一致(同轮转/
    /// 同死亡重试),仅在成功返回时附带成员 id(非池/无成员时不可能出现)。
    pub async fn open_stream_tagged(&self) -> io::Result<(BoxedIo, u64)> {
        self.open_stream_inner().await
    }

    /// open_stream 的实现本体:成功返回时附带落点成员 id。
    async fn open_stream_inner(&self) -> io::Result<(BoxedIo, u64)> {
        loop {
            let (alive, prefix_pending) = {
                let ms = self.inner.members.read().await;
                let limit = self
                    .inner
                    .active_prefix
                    .load(Ordering::Acquire)
                    .min(ms.len());
                let prefix = &ms[..limit];
                (
                    prefix
                        .iter()
                        .filter(|m| !m.mux.is_dead())
                        .cloned()
                        .collect::<Vec<_>>(),
                    prefix.iter().any(|m| m.mux.is_dead()),
                )
            };
            match alive.len() {
                0 => {
                    if self.inner.dead.load(Ordering::Acquire) {
                        return Err(other("yamux pool: no live carrier"));
                    }
                    // 前缀内全死但尚未剔除:等健康监视/开流收敛路径 reap 后
                    // 重读(表压缩可能把前缀外成员前移进来)。不得查看前缀外
                    // 的存活成员——那会让本循环在"前缀全死"时空转。
                    if prefix_pending {
                        self.inner.changed.notified().await;
                        continue;
                    }
                    // 有成员待收敛或重拨在途:等成员表变更(notified 先
                    // 创建再复查,防通知落入创建前的间隙)。
                    let notified = self.inner.changed.notified();
                    if self.inner.dead.load(Ordering::Acquire) {
                        continue;
                    }
                    {
                        let ms = self.inner.members.read().await;
                        let limit = self
                            .inner
                            .active_prefix
                            .load(Ordering::Acquire)
                            .min(ms.len());
                        if ms[..limit].iter().any(|m| !m.mux.is_dead()) {
                            continue;
                        }
                    }
                    notified.await;
                    continue;
                }
                n => {
                    let seq = self.inner.rr.fetch_add(1, Ordering::Relaxed);
                    let m = alive[seq % n].clone();
                    // 先取身份再移动成员:成功即死的分支会在返回前消费 m。
                    let member_id = m.id;
                    match m.mux.open_stream().await {
                        Ok(s) => {
                            if m.mux.is_dead() {
                                // 成功即死(连接在开流瞬间终结):顺手收敛,
                                // 流本身随错误语义由上层处理
                                self.reap(m).await;
                            }
                            return Ok((s, member_id));
                        }
                        Err(e) => {
                            if m.mux.is_dead() {
                                self.reap(m).await;
                                continue; // 换下一个活跃载体
                            }
                            // 非死亡性错误(如服务端流上限):原样上抛
                            return Err(e);
                        }
                    }
                }
            }
        }
    }

    /// 收缩轮转分派前缀(round-10 自动定容):仅前 n 个成员参与 open_stream
    /// 轮转;accept(服务端)不受影响。幂等;n 按目标数存储(下界 1),
    /// 读取侧统一按当前表长钳制。
    /// 只收缩分派集合,不摘除成员——需要真正回收多余载体时用
    /// [`Self::detach_carrier`]。
    ///
    /// 语义是"前缀计数"而非固定成员集合:成员因死亡被剔除后成员表压缩,
    /// 原前缀外成员前移进入前缀;若表长小于 n,全部剩余成员参与分派。
    ///
    /// round-10.1:同时把 n(未按当前表长钳制)登记为重拨补位目标数。收敛
    /// 前已 spawn 的在途重拨可能因拨号超时(默认 10s)晚于收敛落地;
    /// [`redial_loop`] 入表前按该目标数丢弃多余补位,因此无论如何都不会
    /// 把物理载体数补回 `final_k + 1`。
    pub async fn set_active_prefix(&self, n: usize) {
        // 持成员表读锁完成两次原子写:redial_loop 在写锁内读取补位目标并
        // 入表,二者互斥——不存在"重拨读到旧目标后入表"的窗口。
        //
        // round-10.2:两次 store 必须同口径——都存目标数,不按当前表长
        // 钳制。若此处把 active_prefix 钳到当时的 len,重拨补位后表变长而
        // 前缀不扩,会出现"物理数 = final_k 但只有前 len 个参与分派"的
        // 冻结(读取侧已统一 min(表长),无需在此收缩)。
        let ms = self.inner.members.read().await;
        let want = n.max(1);
        self.inner.redial_target.store(want, Ordering::Release);
        self.inner.active_prefix.store(want, Ordering::Release);
        drop(ms);
    }

    /// 当前生效的分派前缀上限(诊断/测试缝:与成员数取较小值)。
    pub async fn active_prefix(&self) -> usize {
        let len = self.inner.members.read().await.len();
        self.inner
            .active_prefix
            .load(Ordering::Acquire)
            .min(len.max(1))
    }

    /// 移除一个成员(round-10,任务书 §2.4 语义):**先清 redial 配方**,
    /// 再 shutdown 该成员 driver(物理连接随 socket drop 关闭),最后从
    /// 成员表移除。顺序不可颠倒——先 shutdown 会让 `reap_member` /
    /// 健康监视把该成员当作故障载体并触发重拨,形成 5s 周期重拨环。
    ///
    /// index 为成员表下标(报到顺序)。幂等:越界或成员已被并发收敛即
    /// 返回。若移除后只剩 1 个成员,同时关闭该剩余成员的重拨配方——
    /// `final_k == 1` 时池退化为单载体,重拨不再需要(任务书 §2.4)。
    ///
    /// accept 转发任务:`forward_tasks` 未按成员索引登记,按规格允许的
    /// 简化实现 abort 全部后为剩余成员重新 spawn(仅服务端池有该语义)。
    pub async fn detach_carrier(&self, index: usize) {
        let _ = self.detach_inner(DetachBy::Index(index)).await;
    }

    /// 按身份摘除成员(round-10.1):先失效 redial(防 5s 重拨环),再
    /// shutdown 其 driver,最后出表并 abort 其转发任务。幂等(id 不存在
    /// 即返回 false)。身份定位不受成员表压缩影响,因此报到失败的载体
    /// 可以在重拨补位/并发剔除之后仍被精确摘除。
    pub async fn detach_carrier_by_id(&self, id: u64) -> bool {
        self.detach_inner(DetachBy::Id(id)).await
    }

    /// detach 两入口的共享实现;锁内只做查找/移除/清配方/shutdown,
    /// 无 await(与 Round-10 的临界区形状一致)。
    async fn detach_inner(&self, by: DetachBy) -> bool {
        let (target, remain, tx) = {
            let mut ms = self.inner.members.write().await;
            if self.inner.shutdown.load(Ordering::Acquire) {
                return false;
            }
            let target = match by {
                DetachBy::Index(index) => {
                    if index >= ms.len() {
                        return false;
                    }
                    ms.remove(index)
                }
                DetachBy::Id(id) => {
                    let Some(pos) = ms.iter().position(|m| m.id == id) else {
                        return false;
                    };
                    ms.remove(pos)
                }
            };
            // 先清配方再 shutdown(见方法注释;顺序是正确性的一部分)
            target.clear_redial();
            target.mux.shutdown();
            if ms.len() == 1 {
                ms[0].clear_redial();
            }
            self.inner.mark_dead_if_exhausted(&ms);
            let remain: Vec<Arc<PoolMember>> = ms.clone();
            (target, remain, self.inner.accept_tx.clone())
        };
        self.inner.changed.notify_waiters();
        // 服务端池:转发任务重建。abort 全部 → 为剩余成员重新 spawn;
        // 新任务在成员表读锁前创建并登记,不存在"已 spawn 未登记"窗口
        // (登记在同步闭包内完成,期间无 await 让出成员表增删)。
        if let Some(tx) = tx {
            let old: Vec<_> = std::mem::take(&mut *self.inner.forward_tasks.lock().unwrap());
            for t in old {
                t.abort();
            }
            if !self.inner.shutdown.load(Ordering::Acquire) {
                let mut handles = Vec::with_capacity(remain.len());
                for m in remain {
                    handles.push(Self::spawn_accept_forward(&self.inner, m, tx.clone()));
                }
                self.inner.forward_tasks.lock().unwrap().extend(handles);
            }
        }
        // 该成员可能正是池 shutdown 的 try_read 快照落空的原因:释放
        // 锁后补扫一次(非关停路径为空操作)。
        self.inner.shutdown_sweep();
        drop(target);
        true
    }

    /// 成员身份快照(round-10.1,成员表顺序)。只读;调用方据此计算
    /// 超出分派前缀的尾部成员,再按 id 逆序摘除(下标会随表压缩漂移)。
    pub async fn member_ids(&self) -> Vec<u64> {
        self.inner
            .members
            .read()
            .await
            .iter()
            .map(|m| m.id)
            .collect()
    }

    /// 当前成员表中**存活**成员的 id(按表序,round-10.2)。终态收敛按此
    /// 计数保留额,避免把额度算给 dead-but-unreaped 成员后误摘健康成员
    /// (健康监视 500ms 窗口内 `member_ids` 仍含已死未剔除的成员)。
    pub async fn live_member_ids(&self) -> Vec<u64> {
        self.inner
            .members
            .read()
            .await
            .iter()
            .filter(|m| !m.mux.is_dead())
            .map(|m| m.id)
            .collect()
    }

    /// 成员表长度(**含 dead-but-unreaped 成员**,round-10.2)。服务端 cap
    /// 检查用它而非 [`Self::live_carriers`]:已死未 reap 的成员仍占用物理
    /// 连接与池内名额,按其计数才能覆盖"dead 未 reap 占位"的窗口(该窗口
    /// 内按存活数计会多放行 1 条)。已披露的"检查→聚合非原子"窗口仍在。
    pub async fn pool_member_count(&self) -> usize {
        self.inner.members.read().await.len()
    }

    /// 服务端:接受一个新流(自全部成员合流)。
    pub async fn accept_stream(&self) -> io::Result<BoxedIo> {
        let Some(rx) = self.accept_rx.as_ref() else {
            return Err(other("yamux pool: accept on client-side pool"));
        };
        let mut guard = rx.lock().await;
        match guard.recv().await {
            Some(s) => Ok(s),
            None => Err(other("yamux pool closed")),
        }
    }

    /// 池是否已死(无法再产生新流:成员表空且无在途重拨)。
    pub fn is_dead(&self) -> bool {
        self.inner.dead.load(Ordering::Acquire)
    }

    /// 活跃载体数(诊断/测试缝:观察故障收敛与重拨补位进度)。
    pub async fn live_carriers(&self) -> usize {
        self.inner
            .members
            .read()
            .await
            .iter()
            .filter(|m| !m.mux.is_dead())
            .count()
    }
}

/// 池整体释放(round-9.1 F1):转发任务与载体连接不得滞留——Session
/// 被替换/移除时 `Arc<Multiplexer>` 的最后一个引用可能在任何任务上下文
/// 里 drop,显式收敛让 driver 与转发任务立即开始收尾,而不是等健康监视
/// 或对端超时。
///
/// 与显式 [`YamuxMultiplexerPool::shutdown`] 的唯一差别:Drop 对**载体
/// driver** 有 [`POOL_DROP_GRACE`] 的有界宽限(先置 shutdown/dead、停
/// 转发任务,再延迟 abort driver),让已排入 yamux 发送队列的在途帧写穿
/// socket。立即 abort 会截断最后一帧(round-9 既有集成测试
/// pool_wiring:对端 write 后紧随池释放即读到 early eof)。宽限有界且由
/// 一个持有句柄的看门任务完成 abort,不存在"spawn 后丢弃句柄又声称能
/// 停止";无运行时时退化为立即 abort。幂等:标志重复置位无害,句柄
/// take 后为空。
impl Drop for YamuxMultiplexerPool {
    fn drop(&mut self) {
        self.inner.shutdown.store(true, Ordering::Release);
        self.inner.changed.notify_waiters();
        // 转发任务立即 abort:它们只停在 accept_stream 上,不承载在途数据。
        let tasks: Vec<_> = std::mem::take(&mut *self.inner.forward_tasks.lock().unwrap());
        for t in tasks {
            t.abort();
        }
        // 成员快照拿到则消费其 driver 句柄;拿不到(写锁被占)由
        // PoolInner 的 Drop 兜底。
        let mut drivers = Vec::new();
        if let Ok(ms) = self.inner.members.try_read() {
            for m in ms.iter() {
                drivers.extend(m.mux.take_drivers());
            }
        }
        if drivers.is_empty() {
            return;
        }
        if tokio::runtime::Handle::try_current().is_ok() {
            // 看门任务持有全部句柄并在宽限期后 abort;即使它自身被取消,
            // 句柄随 future drop 分离——与直接 abort 相比不会更差。
            tokio::spawn(async move {
                tokio::time::sleep(POOL_DROP_GRACE).await;
                for d in drivers {
                    d.abort();
                }
            });
        } else {
            // 无运行时上下文:driver 已无法被调度,直接 abort 无副作用。
            for d in drivers {
                d.abort();
            }
        }
    }
}

/// 测试缝(round-9.1):观测转发任务退出与 PoolInner 是否仍被任务持有。
#[cfg(test)]
impl YamuxMultiplexerPool {
    /// 已退出的 accept 转发任务数。
    fn forward_tasks_exited(&self) -> usize {
        self.inner.forward_exited.load(Ordering::Acquire)
    }

    /// PoolInner 的弱引用(断言任务/成员释放后强计数归零)。
    fn inner_weak(&self) -> std::sync::Weak<PoolInner> {
        Arc::downgrade(&self.inner)
    }

    /// 在途重拨任务数(复核回归:关停分支必须归还该计数)。
    fn redialing(&self) -> usize {
        self.inner.redialing.load(Ordering::Acquire)
    }
}

/// 剔除死亡成员并(若可)触发重拨;唤醒等待中的 open_stream。
/// 幂等:并发收敛(open_stream 撞死/健康监视/accept 转发退出)下按
/// 指针复查,至多剔除一次、至多重拨一次。
async fn reap_member(inner: &Arc<PoolInner>, member: Arc<PoolMember>) {
    let mut ms = inner.members.write().await;
    if !ms.iter().any(|x| Arc::ptr_eq(x, &member)) {
        // 已被并发路径收敛:仍需复查关停标志——shutdown() 的成员快照
        // 可能恰在本写锁被持有时两次 try_read 落空,本次是最后一条会
        // 释放该锁的路径(round-9.2)。必须先释放写锁再 sweep:其内部
        // try_read 在持锁时必然失败,成为无效调用。
        drop(ms);
        inner.shutdown_sweep();
        return;
    }
    // round-10:redial 必须在写锁内读取。detach_carrier 在写锁内先清
    // 配方再 shutdown;若在锁外提前读,可能读到刚被清掉的旧值并把
    // 已 detach 的载体重拨回来。
    let redial = member.redial();
    ms.retain(|x| !Arc::ptr_eq(x, &member));
    inner.mark_dead_if_exhausted(&ms);
    drop(ms);
    inner.changed.notify_waiters();
    // 关停竞态复查(round-9.1 复核):本路径持写锁期间池可能已 shutdown
    // 且其 `try_read` 快照落空;释放锁后补做载体关停,避免"标志已置而
    // 连接仍活着"直到 PoolInner 最后一次释放。
    inner.shutdown_sweep();
    if let Some(redial) = redial {
        if !inner.shutdown.load(Ordering::Acquire) {
            inner.redialing.fetch_add(1, Ordering::AcqRel);
            tokio::spawn(redial_loop(inner.clone(), redial));
        }
    }
}

/// 健康监视(优化项 5"故障收敛与健康探测"):周期扫描成员表,死亡载体
/// 主动剔除并触发重拨——不依赖下一次 open_stream 撞上死成员才收敛
/// (空闲池同样自愈)。持 Weak:池释放后自动退出,不泄漏任务。
fn spawn_health_watch(inner: &Arc<PoolInner>) {
    /// 扫描周期 = 空闲池的死亡→重拨感知延迟上限。
    const WATCH_INTERVAL: std::time::Duration = std::time::Duration::from_millis(500);
    let weak = Arc::downgrade(inner);
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(WATCH_INTERVAL).await;
            let Some(inner) = weak.upgrade() else {
                break; // 池已释放
            };
            if inner.shutdown.load(Ordering::Acquire) {
                break;
            }
            let dead: Vec<Arc<PoolMember>> = inner
                .members
                .read()
                .await
                .iter()
                .filter(|m| m.mux.is_dead())
                .cloned()
                .collect();
            for m in dead {
                reap_member(&inner, m).await;
            }
        }
    });
}

/// 重拨循环:退避重试至成功/放弃/池关停。成功 → 追加新成员(继承配方)
/// 并清除池死标志;放弃(连续失败超上限)→ 成员表空则置死。
async fn redial_loop(inner: Arc<PoolInner>, redial: PoolRedial) {
    let mut backoff = REDIAL_BACKOFF_START;
    for _ in 0..REDIAL_MAX_ATTEMPTS {
        match redial().await {
            Ok(mux) => {
                let mut ms = inner.members.write().await;
                if inner.shutdown.load(Ordering::Acquire) {
                    // 池已关停:该 mux 从未入表,没有任何其它路径会关停它
                    // ——`YamuxMultiplexer` 的 Drop 保持"driver 继续服务
                    // 存量流"语义、不 abort,直接 return 会令新拨通的
                    // driver + 物理连接永久驻留。此处显式 shutdown,并
                    // 归还 in-flight 计数(否则 mark_dead_if_exhausted 永久
                    // 失真)。
                    drop(ms);
                    mux.shutdown();
                    inner.redialing.fetch_sub(1, Ordering::AcqRel);
                    // 本次写锁可能正是 shutdown 的 try_read 落空的原因:
                    // 释放后补扫其余成员。
                    inner.shutdown_sweep();
                    return;
                }
                // round-10.1:收敛已把补位目标数压到 final_k 时,本次拨号
                // 若晚于收敛落地就丢弃——它可能是收敛前 `reap_member` 已
                // spawn 的在途重拨,入表会让物理载体数长期多出
                // `final_k + 1` 条(detach 只认身份,无法收回这条新成员)。
                // 目标数在成员表读锁内写入(见 set_active_prefix),本检查在
                // 写锁内读取,二者互斥,不存在"先读旧目标后入表"的窗口。
                let target = inner.redial_target.load(Ordering::Acquire);
                if ms.len() >= target {
                    drop(ms);
                    mux.shutdown();
                    inner.redialing.fetch_sub(1, Ordering::AcqRel);
                    inner.changed.notify_waiters();
                    return;
                }
                ms.push(Arc::new(PoolMember::new(
                    inner.next_member_id.fetch_add(1, Ordering::Relaxed),
                    mux,
                    Some(redial.clone()),
                )));
                inner.mark_dead_if_exhausted(&ms);
                drop(ms);
                inner.dead.store(false, Ordering::Release);
                inner.changed.notify_waiters();
                inner.redialing.fetch_sub(1, Ordering::AcqRel);
                // 关停竞态复查(round-9.1 复核):shutdown 的 try_read 可能
                // 在本写锁持有期间落空;入表成功也补做一次,保证关停后
                // 新载体连接立即收敛。
                inner.shutdown_sweep();
                return;
            }
            Err(_) => {
                if inner.shutdown.load(Ordering::Acquire) {
                    break;
                }
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(REDIAL_BACKOFF_MAX);
            }
        }
    }
    // 放弃/关停:在途计数回收;成员表空则置死(等待方得到错误而非永挂)
    inner.redialing.fetch_sub(1, Ordering::AcqRel);
    let ms = inner.members.read().await;
    inner.mark_dead_if_exhausted(&ms);
    drop(ms);
    inner.changed.notify_waiters();
}

fn other(msg: &str) -> io::Error {
    io::Error::other(msg.to_string())
}

/// 发送任务:开流请求 → `SendHalf::poll_new_outbound`;否则驱动写路径
/// (`SendHalf::poll`:Pong/GoAway、rtt ping、流命令帧、部分写/flush)。
/// 错误 → 置 `dead`(共享错误槽同时唤醒接收任务,双侧收敛)。
/// round-E2 起对 socket 泛型:锁路径 S=SplitSocket<YamuxCarrier>,
/// duplex 路径 S=YamuxCarrier(写侧独占 fd),两路共用本实现。
async fn send_driver<S>(
    mut send_half: yamux::SendHalf<S>,
    mut open_rx: mpsc::Receiver<oneshot::Sender<io::Result<yamux::Stream>>>,
    dead: Arc<AtomicBool>,
) where
    S: futures::AsyncRead + futures::AsyncWrite + Unpin + yamux::Writev2,
{
    let mut pending_open: Option<oneshot::Sender<io::Result<yamux::Stream>>> = None;
    loop {
        // 返回 true = 终结(错误);false 不会出现,park 以 Pending 表达
        let fatal = std::future::poll_fn(|cx| {
            // 批量收集开流请求:高并发建连(1024+)时逐请求一轮循环会把
            // 每次开流摊到一次驱动上,建连 p99 恶化到秒级;一轮最多取
            // 64 个请求逐个开流,摊薄驱动循环开销(同旧单任务 driver)
            let mut batch = 0;
            while pending_open.is_none() && batch < 64 {
                match open_rx.poll_recv(cx) {
                    Poll::Ready(Some(req)) => {
                        match send_half.poll_new_outbound(cx) {
                            Poll::Ready(Ok(s)) => {
                                let _ = req.send(Ok(s));
                            }
                            Poll::Ready(Err(e)) => {
                                // 先置 dead 再回 Err:池(round-8 优化项 5)
                                // 依据 is_dead 决定换载体重试,回 Err 时
                                // 死亡标志必须已可见
                                dead.store(true, Ordering::Relaxed);
                                let _ = req.send(Err(other(&format!("yamux: {e}"))));
                                return Poll::Ready(true);
                            }
                            Poll::Pending => {
                                pending_open = Some(req);
                            }
                        }
                        batch += 1;
                    }
                    // open_tx 全部释放:流句柄不持有 mux 引用,连接仍需
                    // 继续服务存量流直至对端/IO 结束 —— 与旧语义一致,忽略
                    Poll::Ready(None) | Poll::Pending => break,
                }
            }
            // ACK backlog 门控的挂起请求:每轮重试(ACK 到达经共享 waker
            // 唤醒本任务)
            if let Some(req) = pending_open.take() {
                match send_half.poll_new_outbound(cx) {
                    Poll::Ready(Ok(s)) => {
                        let _ = req.send(Ok(s));
                    }
                    Poll::Ready(Err(e)) => {
                        // 先置 dead 再回 Err(同上,池的重试判定依赖)
                        dead.store(true, Ordering::Relaxed);
                        let _ = req.send(Err(other(&format!("yamux: {e}"))));
                        return Poll::Ready(true);
                    }
                    Poll::Pending => {
                        pending_open = Some(req);
                    }
                }
            }
            match send_half.poll(cx) {
                Poll::Ready(Err(e)) => {
                    tracing::debug!(error = %e, "yamux send driver error");
                    dead.store(true, Ordering::Relaxed);
                    return Poll::Ready(true);
                }
                // Ready(Ok(())) 防御分支:发送半不会正常终结,按终结处理
                Poll::Ready(Ok(())) => {
                    dead.store(true, Ordering::Relaxed);
                    return Poll::Ready(true);
                }
                Poll::Pending => {}
            }
            Poll::Pending
        })
        .await;
        if fatal {
            break;
        }
    }
    dead.store(true, Ordering::Relaxed);
    if let Some(req) = pending_open.take() {
        let _ = req.send(Err(other("mux connection closed")));
    }
    while let Ok(req) = open_rx.try_recv() {
        let _ = req.send(Err(other("mux connection closed")));
    }
}

/// 接收任务:解帧+派发;新流入 `accept_tx`(容量 256 背压——接收阻塞
/// 只停读侧任务,不影响发送任务)。EOF/错误 → 置 `dead`。
/// round-E2 起对 socket 泛型(同 [`send_driver`]:锁路径/duplex 共用)。
async fn recv_driver<S>(
    mut recv_half: yamux::RecvHalf<S>,
    accept_tx: mpsc::Sender<yamux::Stream>,
    dead: Arc<AtomicBool>,
) where
    S: futures::AsyncRead + futures::AsyncWrite + Unpin + yamux::Writev2,
{
    loop {
        let inbound = match std::future::poll_fn(|cx| recv_half.poll(cx)).await {
            Some(Ok(s)) => s,
            Some(Err(e)) => {
                tracing::debug!(error = %e, "yamux inbound error");
                break;
            }
            None => break,
        };
        if accept_tx.send(inbound).await.is_err() {
            break;
        }
    }
    dead.store(true, Ordering::Relaxed);
}

/// 服务端:把已建立的字节流包装为 Multiplexer(multiplex=true → yamux 服务端)。
/// max_streams:并发流硬顶,0 = 不限制。
/// 锁路径(单载体 + SplitSocket/BiLock);双载体裸 TCP 走
/// [`wrap_stream_as_server_duplex`]。
pub fn wrap_stream_as_server(
    io: BoxedIo,
    multiplex: bool,
    max_streams: usize,
) -> Arc<crate::Multiplexer> {
    if multiplex {
        Arc::new(crate::Multiplexer {
            inner: MultiplexerInner::Yamux(YamuxMultiplexer::new(
                io,
                yamux::Mode::Server,
                max_streams,
                DEFAULT_SHARD_WORKERS,
            )),
        })
    } else {
        Arc::new(crate::Multiplexer {
            inner: MultiplexerInner::AcceptSingle {
                stream: tokio::sync::Mutex::new(Some(io)),
            },
        })
    }
}

/// 服务端双载体变体(round-E2):read_io/write_io 为同一裸 TCP socket 的
/// 两个 fd(tcp.rs verify 的 try_clone 产物),halves 零锁并行;
/// 语义/背压与 [`wrap_stream_as_server`] 完全一致。
pub fn wrap_stream_as_server_duplex(
    read_io: BoxedIo,
    write_io: BoxedIo,
    max_streams: usize,
) -> Arc<crate::Multiplexer> {
    Arc::new(crate::Multiplexer {
        inner: MultiplexerInner::Yamux(YamuxMultiplexer::new_duplex(
            read_io,
            write_io,
            yamux::Mode::Server,
            max_streams,
            DEFAULT_SHARD_WORKERS,
        )),
    })
}

/// 客户端:同上(yamux 客户端)。锁路径;双载体裸 TCP 走
/// [`wrap_stream_as_client_duplex`]。
pub fn wrap_stream_as_client(
    io: BoxedIo,
    multiplex: bool,
    max_streams: usize,
) -> Arc<crate::Multiplexer> {
    if multiplex {
        Arc::new(crate::Multiplexer {
            inner: MultiplexerInner::Yamux(YamuxMultiplexer::new(
                io,
                yamux::Mode::Client,
                max_streams,
                DEFAULT_SHARD_WORKERS,
            )),
        })
    } else {
        Arc::new(crate::Multiplexer {
            inner: MultiplexerInner::AcceptSingle {
                stream: tokio::sync::Mutex::new(Some(io)),
            },
        })
    }
}

/// 客户端双载体变体(round-E2):同 [`wrap_stream_as_client`],载体为
/// 同一裸 TCP socket 的两个 fd(tcp.rs dial_io 的 try_clone 产物)。
pub fn wrap_stream_as_client_duplex(
    read_io: BoxedIo,
    write_io: BoxedIo,
    max_streams: usize,
) -> Arc<crate::Multiplexer> {
    Arc::new(crate::Multiplexer {
        inner: MultiplexerInner::Yamux(YamuxMultiplexer::new_duplex(
            read_io,
            write_io,
            yamux::Mode::Client,
            max_streams,
            DEFAULT_SHARD_WORKERS,
        )),
    })
}

/// tcpmux 服务端终结:yamux 服务端接受多路流(供 vhost/tcpmux 使用)。
pub fn tcpmux_server_mux(io: crate::BoxedIo, max_streams: usize) -> Arc<crate::Multiplexer> {
    wrap_stream_as_server(io, true, max_streams)
}

#[cfg(test)]
mod tests {
    use super::*;

    // R2(红线 2 锁):读块上限常量必须保持 64KiB。
    #[test]
    fn chunk_cap_is_64kib() {
        assert_eq!(CHUNK_CAP, 64 * 1024, "读缓冲上限红线(64KiB)不得放宽");
    }

    // ===== round-8 优化项 5:多物理载体连接池 =====

    /// 关键 await 的统一时限(防断言性挂死拖垮测试套件)。
    async fn deadline<F: std::future::Future>(f: F) -> F::Output {
        tokio::time::timeout(std::time::Duration::from_secs(5), f)
            .await
            .expect("pool test deadline (5s)")
    }

    /// 可杀载体 IO:kill() 置位后读侧立刻 BrokenPipe 并唤醒挂起的
    /// driver(模拟物理连接断开)。写侧在下次 poll 时亦立即失败。
    struct KillState {
        dead: AtomicBool,
        read_waker: std::sync::Mutex<Option<std::task::Waker>>,
    }

    impl KillState {
        fn kill(&self) {
            self.dead.store(true, Ordering::SeqCst);
            if let Some(w) = self.read_waker.lock().unwrap().take() {
                w.wake();
            }
        }
    }

    struct KillIo {
        inner: tokio::io::DuplexStream,
        state: Arc<KillState>,
    }

    impl tokio::io::AsyncRead for KillIo {
        fn poll_read(
            self: Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
            buf: &mut tokio::io::ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            let this = self.get_mut();
            if this.state.dead.load(Ordering::SeqCst) {
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "carrier killed",
                )));
            }
            let r = Pin::new(&mut this.inner).poll_read(cx, buf);
            if r.is_pending() {
                *this.state.read_waker.lock().unwrap() = Some(cx.waker().clone());
            }
            if this.state.dead.load(Ordering::SeqCst) {
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "carrier killed",
                )));
            }
            r
        }
    }

    impl tokio::io::AsyncWrite for KillIo {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            _buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            if self.state.dead.load(Ordering::SeqCst) {
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "carrier killed",
                )));
            }
            Pin::new(&mut self.get_mut().inner).poll_write(_cx, _buf)
        }
        fn poll_flush(
            self: Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> Poll<io::Result<()>> {
            if self.state.dead.load(Ordering::SeqCst) {
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "carrier killed",
                )));
            }
            Pin::new(&mut self.get_mut().inner).poll_flush(cx)
        }
        fn poll_shutdown(
            self: Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> Poll<io::Result<()>> {
            Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
        }
    }

    /// 建一条内存载体对:客户端 mux + 服务端 mux(服务端 IO 经 KillIo
    /// 包装,可模拟物理断开)。
    fn carrier_pair_killable(
        max_streams: usize,
    ) -> (YamuxMultiplexer, YamuxMultiplexer, Arc<KillState>) {
        let (a, server, state) = carrier_pair_peer_killable(max_streams);
        (
            YamuxMultiplexer::new(
                Box::new(a),
                yamux::Mode::Client,
                max_streams,
                DEFAULT_SHARD_WORKERS,
            ),
            server,
            state,
        )
    }

    /// 同 [`carrier_pair_killable`],但对端保持裸 duplex(不包客户端 mux)
    /// ——round-9.1 T-L1 需要直接从对端观察 EOF(socket 是否真的关闭)。
    fn carrier_pair_peer_killable(
        max_streams: usize,
    ) -> (tokio::io::DuplexStream, YamuxMultiplexer, Arc<KillState>) {
        let (a, b) = tokio::io::duplex(1024 * 1024);
        let state = Arc::new(KillState {
            dead: AtomicBool::new(false),
            read_waker: std::sync::Mutex::new(None),
        });
        let server = KillIo {
            inner: b,
            state: state.clone(),
        };
        (
            a,
            YamuxMultiplexer::new(
                Box::new(server),
                yamux::Mode::Server,
                max_streams,
                DEFAULT_SHARD_WORKERS,
            ),
            state,
        )
    }

    /// 服务端 echo 泵:accept 全部流并逐字节回显(数据面活性探针)。
    fn spawn_echo_server(server: YamuxMultiplexer) {
        tokio::spawn(async move {
            while let Ok(mut st) = server.accept_stream().await {
                tokio::spawn(async move {
                    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
                    let mut buf = [0u8; 1024];
                    loop {
                        match st.read(&mut buf).await {
                            Ok(0) | Err(_) => break,
                            Ok(n) => {
                                if st.write_all(&buf[..n]).await.is_err() {
                                    break;
                                }
                            }
                        }
                    }
                });
            }
        });
    }

    /// 轮转分派:K=3 池开 6 流,恰每载体 2 条(SYN|WU 建流即发,
    /// 服务端 accept 立即就绪)。
    #[tokio::test]
    async fn yamux_pool_round_robin_across_carriers() {
        const K: usize = 3;
        const N: usize = 6;
        let mut clients = Vec::new();
        let mut servers = Vec::new();
        for _ in 0..K {
            let (a, b) = tokio::io::duplex(1024 * 1024);
            clients.push(YamuxMultiplexer::new(
                Box::new(a),
                yamux::Mode::Client,
                16,
                DEFAULT_SHARD_WORKERS,
            ));
            servers.push(YamuxMultiplexer::new(
                Box::new(b),
                yamux::Mode::Server,
                16,
                DEFAULT_SHARD_WORKERS,
            ));
        }
        let pool = YamuxMultiplexerPool::new_client(clients, None);
        for _ in 0..N {
            deadline(pool.open_stream()).await.unwrap();
        }
        for s in &mut servers {
            for _ in 0..(N / K) {
                deadline(s.accept_stream()).await.unwrap();
            }
        }
        // 恰 N/K:若某载体分派过多,下一行 accept 会挂起触发 deadline。
        // 若某载体分派过少,上方循环已先行挂起。均匀性由此双向锁死。
    }

    /// 故障隔离:杀载体 0 后,池剔除该载体(收敛为 live=K-1),
    /// 后续开流全部落在其余载体且数据面(echo)可用;被杀载体的
    /// 服务端不再收到任何新流。
    #[tokio::test]
    async fn yamux_pool_carrier_failure_isolated() {
        const K: usize = 3;
        let mut clients = Vec::new();
        let mut kills = Vec::new();
        for _ in 0..K {
            let (c, s, kill) = carrier_pair_killable(16);
            clients.push(c);
            kills.push(kill);
            spawn_echo_server(s);
        }
        let pool = YamuxMultiplexerPool::new_client(clients, None);
        // 杀前:每载体 1 流 echo 通
        for i in 0..K {
            let mut st = deadline(pool.open_stream()).await.unwrap();
            use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
            st.write_all(format!("ping{i}").as_bytes()).await.unwrap();
            let mut buf = [0u8; 5];
            deadline(st.read_exact(&mut buf)).await.unwrap();
            assert_eq!(&buf, format!("ping{i}").as_bytes());
        }
        // 杀载体 0 → 池收敛为 2 活载体
        kills[0].kill();
        let t0 = std::time::Instant::now();
        while pool.live_carriers().await != K - 1 {
            assert!(
                t0.elapsed() < std::time::Duration::from_secs(3),
                "carrier 0 must be reaped"
            );
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        // 后续 8 条流全部落在其余载体,echo 全通
        for i in 0..8u32 {
            let mut st = deadline(pool.open_stream()).await.unwrap();
            use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
            let probe = format!("after{i}").into_bytes();
            st.write_all(&probe).await.unwrap();
            let mut buf = vec![0u8; probe.len()];
            deadline(st.read_exact(&mut buf)).await.unwrap();
            assert_eq!(buf, probe);
        }
        assert!(!pool.is_dead());
    }

    /// 重拨恢复:K=1,载体死亡 → 池按配方自动重拨 → live 回到 1,
    /// 新流在新载体上 echo 通;重拨恰发生 1 次。
    #[tokio::test]
    async fn yamux_pool_redial_recovers() {
        let (c0, s0, kill) = carrier_pair_killable(16);
        spawn_echo_server(s0);
        let redials = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let redials2 = redials.clone();
        let redial: PoolRedial = Arc::new(move || {
            redials2.fetch_add(1, Ordering::SeqCst);
            Box::pin(async {
                let (a, b) = tokio::io::duplex(1024 * 1024);
                let server = YamuxMultiplexer::new(
                    Box::new(b),
                    yamux::Mode::Server,
                    16,
                    DEFAULT_SHARD_WORKERS,
                );
                spawn_echo_server(server);
                Ok(YamuxMultiplexer::new(
                    Box::new(a),
                    yamux::Mode::Client,
                    16,
                    DEFAULT_SHARD_WORKERS,
                ))
            })
        });
        let pool = YamuxMultiplexerPool::new_client(vec![c0], Some(redial));
        let mut st = deadline(pool.open_stream()).await.unwrap();
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
        st.write_all(b"before").await.unwrap();
        let mut buf = [0u8; 6];
        deadline(st.read_exact(&mut buf)).await.unwrap();
        assert_eq!(&buf, b"before");

        kill.kill();
        // 等自愈完成:健康监视(≤500ms 周期)剔除死载体并触发重拨,
        // 重拨成功补位。联合条件消除"死亡未传播时 live 仍为 1"的假阳性
        // 窗口——redials ≥ 1 证明旧载体已被剔除(redial 只由收敛路径
        // 触发),live == 1 证明新载体已入表。
        let t0 = std::time::Instant::now();
        loop {
            let live = pool.live_carriers().await;
            if live == 1 && redials.load(Ordering::SeqCst) >= 1 {
                break;
            }
            assert!(
                t0.elapsed() < std::time::Duration::from_secs(5),
                "redial must restore the carrier (live={live}, redials={})",
                redials.load(Ordering::SeqCst)
            );
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        // 重拨期间 open_stream 会被暂堵直到补位——deadline 兜底
        let mut st = deadline(pool.open_stream()).await.unwrap();
        st.write_all(b"after").await.unwrap();
        let mut buf = [0u8; 5];
        deadline(st.read_exact(&mut buf)).await.unwrap();
        assert_eq!(&buf, b"after");
        assert_eq!(redials.load(Ordering::SeqCst), 1, "exactly one redial");
        assert!(!pool.is_dead());
    }

    // T-C4(round-10):set_active_prefix(1) 后所有 open_stream 落在成员 0。
    // 用 3 个客户端 mux 各自 accept 计数观测分派落点(客户端开流 →
    // 对端 server mux accept;哪个 server 收到即哪个成员被选中)。
    #[tokio::test]
    async fn set_active_prefix_restricts_round_robin() {
        const K: usize = 3;
        let mut clients = Vec::new();
        let mut servers = Vec::new();
        for _ in 0..K {
            let (a, b) = tokio::io::duplex(1024 * 1024);
            clients.push(YamuxMultiplexer::new(
                Box::new(a),
                yamux::Mode::Client,
                16,
                DEFAULT_SHARD_WORKERS,
            ));
            servers.push(YamuxMultiplexer::new(
                Box::new(b),
                yamux::Mode::Server,
                16,
                DEFAULT_SHARD_WORKERS,
            ));
        }
        let pool = YamuxMultiplexerPool::new_client(clients, None);
        pool.set_active_prefix(1).await;
        assert_eq!(pool.active_prefix().await, 1);

        for _ in 0..6 {
            deadline(pool.open_stream()).await.unwrap();
        }
        // 全部 6 条必须落在成员 0:成员 0 的 server accept 6 条
        for _ in 0..6 {
            deadline(servers[0].accept_stream()).await.unwrap();
        }
        // 其余成员一条都不该有;用超时确认(有流即返回 Ok)
        for s in &servers[1..] {
            let r = tokio::time::timeout(std::time::Duration::from_millis(100), s.accept_stream())
                .await;
            assert!(r.is_err(), "前缀外成员不得参与分派");
        }

        // 幂等与越界钳制:n=0 / n>K / 重复调用都不 panic,且上限被钳到 [1,K]
        pool.set_active_prefix(0).await;
        assert_eq!(pool.active_prefix().await, 1);
        pool.set_active_prefix(1).await;
        assert_eq!(pool.active_prefix().await, 1);
        pool.set_active_prefix(usize::MAX).await;
        assert_eq!(pool.active_prefix().await, K);
        pool.set_active_prefix(usize::MAX).await;
        assert_eq!(pool.active_prefix().await, K);
    }

    // detach_carrier(round-10):移除后 live 减 1、对端读到 EOF、且不触发
    // 重拨——跨过 REDIAL_BACKOFF_START 与健康监视周期后成员数不回升。
    #[tokio::test]
    async fn detach_carrier_closes_and_never_redials() {
        // 两个可杀载体对:peer 端保持裸 duplex 以观察 EOF。
        let mut peers = Vec::new();
        let mut members = Vec::new();
        for _ in 0..2 {
            let (peer, mux, _kill) = carrier_pair_peer_killable(16);
            peers.push(peer);
            members.push(mux);
        }
        let redials = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let r2 = redials.clone();
        let redial: PoolRedial = Arc::new(move || {
            r2.fetch_add(1, Ordering::SeqCst);
            Box::pin(async {
                let (a, _b) = tokio::io::duplex(1024 * 1024);
                Ok(YamuxMultiplexer::new(
                    Box::new(a),
                    yamux::Mode::Client,
                    16,
                    DEFAULT_SHARD_WORKERS,
                ))
            })
        });
        let pool = YamuxMultiplexerPool::new_client(members, Some(redial));
        assert_eq!(pool.live_carriers().await, 2);

        pool.detach_carrier(1).await;
        assert_eq!(pool.live_carriers().await, 1, "detach 后只剩 1 条");
        assert_eq!(pool.active_prefix().await, 1);

        // 对端读到 EOF:detach 已 shutdown driver → socket drop
        let mut p = peers.pop().unwrap();
        use tokio::io::AsyncReadExt as _;
        let mut buf = [0u8; 64];
        let eof = tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                match p.read(&mut buf).await {
                    Ok(0) => break,
                    Ok(_) => continue,
                    Err(e) => panic!("对端读不得报错:{e}"),
                }
            }
        })
        .await;
        assert!(eof.is_ok(), "detach 必须关闭物理连接(对端 1s 内 EOF)");

        // 不重拨:跨过 200ms 退避起点 + 500ms 健康监视周期 ×2
        tokio::time::sleep(std::time::Duration::from_millis(1200)).await;
        assert_eq!(redials.load(Ordering::SeqCst), 0, "detach 不得触发重拨");
        assert_eq!(pool.live_carriers().await, 1, "成员数不得回升");

        // 幂等:越界/重复 detach 不 panic,也不再减少
        pool.detach_carrier(5).await;
        pool.detach_carrier(0).await; // 移除最后一个 → 池空
        assert_eq!(pool.live_carriers().await, 0);
        pool.detach_carrier(0).await;
        assert_eq!(pool.live_carriers().await, 0);
    }

    // detach_carrier:移除后剩余成员的轮转仍正确(K=3 → detach 1 →
    // 2 条各分 2 流,均能被各自 server accept)。
    #[tokio::test]
    async fn detach_carrier_keeps_remaining_rr_correct() {
        const K: usize = 3;
        let mut clients = Vec::new();
        let mut servers = Vec::new();
        for _ in 0..K {
            let (a, b) = tokio::io::duplex(1024 * 1024);
            clients.push(YamuxMultiplexer::new(
                Box::new(a),
                yamux::Mode::Client,
                16,
                DEFAULT_SHARD_WORKERS,
            ));
            servers.push(YamuxMultiplexer::new(
                Box::new(b),
                yamux::Mode::Server,
                16,
                DEFAULT_SHARD_WORKERS,
            ));
        }
        let pool = YamuxMultiplexerPool::new_client(clients, None);
        pool.detach_carrier(1).await;
        assert_eq!(pool.live_carriers().await, 2);

        for _ in 0..4 {
            deadline(pool.open_stream()).await.unwrap();
        }
        for s in [&servers[0], &servers[2]] {
            for _ in 0..2 {
                deadline(s.accept_stream()).await.unwrap();
            }
        }
        // 已 detach 的成员不得再接流:对端 mux 随物理连接关闭,要么
        // accept 返回错误,要么超时;唯独不得返回一条流。
        let r = tokio::time::timeout(
            std::time::Duration::from_millis(100),
            servers[1].accept_stream(),
        )
        .await;
        assert!(!matches!(r, Ok(Ok(_))), "已 detach 的成员不得再接流");
    }

    // T-R1(round-10.1):open_stream_tagged 返回的 id 属于当前成员且与轮转
    // 顺序一致。new_client 按表顺序分配 1..K,轮转从 seq=0 起步,故 6 条
    // 流的 id 序列必须是 1,2,3,1,2,3;member_ids 快照同序。
    #[tokio::test]
    async fn open_stream_tagged_reports_round_robin_member() {
        const K: usize = 3;
        let mut clients = Vec::new();
        let mut servers = Vec::new();
        for _ in 0..K {
            let (a, b) = tokio::io::duplex(1024 * 1024);
            clients.push(YamuxMultiplexer::new(
                Box::new(a),
                yamux::Mode::Client,
                16,
                DEFAULT_SHARD_WORKERS,
            ));
            servers.push(YamuxMultiplexer::new(
                Box::new(b),
                yamux::Mode::Server,
                16,
                DEFAULT_SHARD_WORKERS,
            ));
        }
        let pool = YamuxMultiplexerPool::new_client(clients, None);
        let ids = pool.member_ids().await;
        assert_eq!(ids, vec![1, 2, 3], "身份按成员表顺序分配");

        let mut got = Vec::new();
        for _ in 0..6 {
            let (_s, id) = deadline(pool.open_stream_tagged()).await.unwrap();
            got.push(id);
        }
        assert_eq!(got, vec![1, 2, 3, 1, 2, 3], "id 必须与轮转落点一致");
        // 对端各 accept 2 条(落点归属的独立佐证)
        for s in &mut servers {
            for _ in 0..2 {
                deadline(s.accept_stream()).await.unwrap();
            }
        }
    }

    // T-R2(round-10.1):detach_carrier_by_id 后 live 减 1、对端 EOF、跨过
    // 重拨退避起点(200ms)后成员数不回升(无重拨环)。
    #[tokio::test]
    async fn detach_carrier_by_id_closes_and_never_redials() {
        use tokio::io::AsyncReadExt as _;
        let (mut peer0, mux0, _k0) = carrier_pair_peer_killable(16);
        let (_peer1, mux1, _k1) = carrier_pair_peer_killable(16);
        let redials = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let r2 = redials.clone();
        let redial: PoolRedial = Arc::new(move || {
            r2.fetch_add(1, Ordering::SeqCst);
            Box::pin(async {
                let (a, _b) = tokio::io::duplex(1024 * 1024);
                Ok(YamuxMultiplexer::new(
                    Box::new(a),
                    yamux::Mode::Client,
                    16,
                    DEFAULT_SHARD_WORKERS,
                ))
            })
        });
        let pool = YamuxMultiplexerPool::new_client(vec![mux0, mux1], Some(redial));
        assert_eq!(pool.live_carriers().await, 2);
        let before = pool.member_ids().await;

        assert!(pool.detach_carrier_by_id(before[0]).await, "身份命中");
        assert_eq!(pool.live_carriers().await, 1, "detach 后只剩 1 条");
        assert_eq!(pool.member_ids().await, vec![before[1]], "只剩另一成员");

        // 对端读到 EOF:detach 已 shutdown driver → socket drop
        let mut buf = [0u8; 64];
        let eof = tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                match peer0.read(&mut buf).await {
                    Ok(0) => break,
                    Ok(_) => continue,
                    Err(e) => panic!("对端读不得报错:{e}"),
                }
            }
        })
        .await;
        assert!(eof.is_ok(), "按 id 摘除必须关闭物理连接(对端 1s 内 EOF)");

        // 不重拨:300ms 已跨过 REDIAL_BACKOFF_START(200ms),重拨环若存在
        // 会在该窗口内补回一条。
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        assert_eq!(redials.load(Ordering::SeqCst), 0, "按 id 摘除不得触发重拨");
        assert_eq!(pool.live_carriers().await, 1, "成员数不得回升");
    }

    // T-R3(round-10.1):幂等/未知 id 返回 false 且不 panic;重拨补位的新
    // 成员拿新 id,绝不复用旧 id(复用会让收敛阶段误摘新载体)。
    #[tokio::test]
    async fn detach_carrier_by_id_is_idempotent_and_redial_gets_fresh_id() {
        let (c0, _s0, kill) = carrier_pair_killable(16);
        let redials = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let r2 = redials.clone();
        let redial: PoolRedial = Arc::new(move || {
            r2.fetch_add(1, Ordering::SeqCst);
            Box::pin(async {
                let (a, b) = tokio::io::duplex(1024 * 1024);
                // 对端保持存活(否则新载体建即死,live 计数抖动)
                tokio::spawn(async move {
                    let _peer = b;
                    futures::future::pending::<()>().await;
                });
                Ok(YamuxMultiplexer::new(
                    Box::new(a),
                    yamux::Mode::Client,
                    16,
                    DEFAULT_SHARD_WORKERS,
                ))
            })
        });
        let pool = YamuxMultiplexerPool::new_client(vec![c0], Some(redial));
        let old_id = pool.member_ids().await[0];

        // 未知 id:false 且不 panic;池成员表不变
        assert!(!pool.detach_carrier_by_id(0).await, "0 不是成员身份");
        assert!(
            !pool.detach_carrier_by_id(u64::MAX).await,
            "未知 id 返回 false"
        );
        assert_eq!(pool.member_ids().await, vec![old_id]);

        // 杀载体 → 健康监视剔除并重拨补位,新成员 id 不得复用
        kill.kill();
        let t0 = std::time::Instant::now();
        loop {
            if pool.live_carriers().await == 1 && redials.load(Ordering::SeqCst) >= 1 {
                break;
            }
            assert!(
                t0.elapsed() < std::time::Duration::from_secs(5),
                "redial must restore the carrier"
            );
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        let ids = pool.member_ids().await;
        assert_eq!(ids.len(), 1);
        assert_ne!(ids[0], old_id, "重拨补位必须拿新 id,不得复用");

        // 旧 id 已不在表:重复摘除返回 false(幂等);新 id 可摘
        assert!(!pool.detach_carrier_by_id(old_id).await, "旧 id 已失效");
        assert!(pool.detach_carrier_by_id(ids[0]).await, "新 id 可摘除");
        assert!(
            !pool.detach_carrier_by_id(ids[0]).await,
            "重复摘除返回 false"
        );
    }

    /// T-R3b(round-10.1 复核 regression):`reap_member` 可能在收敛前就已
    /// spawn 重拨,拨号受 `connect_timeout`(默认 10s)约束——迟到的补位
    /// 必须按补位目标数在入表前丢弃,物理载体数不得补回 `final_k + 1`,
    /// 且被丢弃的载体连接必须真正关停(不能只是出表)。
    #[tokio::test]
    async fn slow_redial_landing_after_convergence_is_discarded() {
        use tokio::io::AsyncReadExt as _;
        let (c0, _s0, kill) = carrier_pair_killable(16);
        let (c1, _s1, _k1) = carrier_pair_killable(16);
        let redials = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let r2 = redials.clone();
        // 慢拨:把"已 spawn 但尚未落地"的重拨具象化为 300ms 后才建好载体。
        let peer_slot: Arc<tokio::sync::Mutex<Option<tokio::io::DuplexStream>>> =
            Arc::new(tokio::sync::Mutex::new(None));
        let slot = peer_slot.clone();
        let redial: PoolRedial = Arc::new(move || {
            r2.fetch_add(1, Ordering::SeqCst);
            let slot = slot.clone();
            Box::pin(async move {
                tokio::time::sleep(std::time::Duration::from_millis(300)).await;
                let (a, b) = tokio::io::duplex(1024 * 1024);
                *slot.lock().await = Some(b);
                Ok(YamuxMultiplexer::new(
                    Box::new(a),
                    yamux::Mode::Client,
                    16,
                    DEFAULT_SHARD_WORKERS,
                ))
            })
        });
        let pool = YamuxMultiplexerPool::new_client(vec![c0, c1], Some(redial));

        // 杀成员 0 → 健康监视剔除并 spawn 重拨(重拨进入慢拨等待)。
        kill.kill();
        let t0 = std::time::Instant::now();
        while pool.member_ids().await.len() != 1 || redials.load(Ordering::SeqCst) == 0 {
            assert!(
                t0.elapsed() < std::time::Duration::from_secs(5),
                "reaper must reap and spawn redial"
            );
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }

        // 收敛:目标 1(控制载体保留)。此刻重拨已在途,无法再被 detach
        // 失效——只能由入表处的目标数检查拦下。
        pool.set_active_prefix(1).await;

        // 跨过慢拨完成点(300ms)+ 余量。旧实现会在 300ms 时补回一条,
        // 使物理载体数 = final_k + 1 永久驻留。
        tokio::time::sleep(std::time::Duration::from_millis(600)).await;
        assert_eq!(redials.load(Ordering::SeqCst), 1, "只应有一次重拨");
        assert_eq!(
            pool.member_ids().await.len(),
            1,
            "迟到的重拨补位必须在入表前丢弃"
        );
        assert_eq!(pool.live_carriers().await, 1);

        // 被丢弃的载体不得只"出表"而泄漏物理连接:对端必须读到 EOF。
        let mut peer = peer_slot
            .lock()
            .await
            .take()
            .expect("slow redial must have produced a carrier");
        let mut buf = [0u8; 64];
        let eof = tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                match peer.read(&mut buf).await {
                    Ok(0) => break,
                    Ok(_) => continue,
                    Err(e) => panic!("对端读不得报错:{e}"),
                }
            }
        })
        .await;
        assert!(eof.is_ok(), "被丢弃的补位载体必须关闭物理连接");
    }

    /// round-10.2 §2.1 regression:收敛时成员表因成员死亡未补位而只有 2 条,
    /// `set_active_prefix(3)` 必须存**目标数**而非当时的表长;随后重拨补位
    /// 使表回到 3 条时,分派前缀必须扩展到 3。修复前 `active_prefix` 被钳
    /// 到 2,补位后仍冻结为 2 → 3 条流只落在 2 个成员上。
    #[tokio::test]
    async fn prefix_expands_when_redial_lands_after_convergence() {
        let (c0, _s0, _k0) = carrier_pair_killable(16);
        let (c1, _s1, k1) = carrier_pair_killable(16);
        let (c2, _s2, _k2) = carrier_pair_killable(16);
        let redials = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let r2 = redials.clone();
        // 慢拨 300ms:保证 set_active_prefix 执行时表长仍为 2(补位未落地)。
        let redial: PoolRedial = Arc::new(move || {
            r2.fetch_add(1, Ordering::SeqCst);
            Box::pin(async {
                tokio::time::sleep(std::time::Duration::from_millis(300)).await;
                let (a, b) = tokio::io::duplex(1024 * 1024);
                // 对端保持存活:新载体不得建即死。
                tokio::spawn(async move {
                    let _peer = b;
                    futures::future::pending::<()>().await;
                });
                Ok(YamuxMultiplexer::new(
                    Box::new(a),
                    yamux::Mode::Client,
                    16,
                    DEFAULT_SHARD_WORKERS,
                ))
            })
        });
        let pool = YamuxMultiplexerPool::new_client(vec![c0, c1, c2], Some(redial));
        assert_eq!(pool.member_ids().await.len(), 3);

        // 杀成员 1 → 健康监视剔除(表长 2)并 spawn 慢重拨。
        k1.kill();
        let t0 = std::time::Instant::now();
        while pool.member_ids().await.len() != 2 || redials.load(Ordering::SeqCst) == 0 {
            assert!(
                t0.elapsed() < std::time::Duration::from_secs(5),
                "carrier 1 must be reaped and redial spawned"
            );
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }

        // 收敛:此刻表长 2,目标 final_k=3。
        pool.set_active_prefix(3).await;

        // 等慢重拨补位落地(表长回到 3)。
        let t0 = std::time::Instant::now();
        while pool.member_ids().await.len() != 3 {
            assert!(
                t0.elapsed() < std::time::Duration::from_secs(5),
                "redial must land after convergence"
            );
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        assert_eq!(
            pool.active_prefix().await,
            3,
            "补位后前缀必须扩展到目标数(不得冻结在收敛时的表长)"
        );

        // 连续 3 次分派必须覆盖 3 个不同成员(修复前只有前 2 个参与轮转)。
        let mut landed = Vec::new();
        for _ in 0..3 {
            let (_s, id) = deadline(pool.open_stream_tagged()).await.unwrap();
            landed.push(id);
        }
        landed.sort_unstable();
        landed.dedup();
        assert_eq!(landed.len(), 3, "3 条流必须覆盖 3 个载体(前缀不得冻结)");
    }

    /// T-P1(round-9):运行中 attach_carrier 追加的成员立即参与
    /// open_stream 轮转——1 成员池附加第 2 成员后开 2 流,恰每载体 1 条
    /// (双向锁死均匀性,同 yamux_pool_round_robin_across_carriers)。
    #[tokio::test]
    async fn yamux_pool_attach_carrier_joins_round_robin() {
        // 载体对 A(池初始成员)与 B(附加成员),各带对端客户端 mux
        let (a_client, a_server) = tokio::io::duplex(1024 * 1024);
        let (b_client, b_server) = tokio::io::duplex(1024 * 1024);
        let client_a = YamuxMultiplexer::new(
            Box::new(a_client),
            yamux::Mode::Client,
            16,
            DEFAULT_SHARD_WORKERS,
        );
        let client_b = YamuxMultiplexer::new(
            Box::new(b_client),
            yamux::Mode::Client,
            16,
            DEFAULT_SHARD_WORKERS,
        );
        let server_a = YamuxMultiplexer::new(
            Box::new(a_server),
            yamux::Mode::Server,
            16,
            DEFAULT_SHARD_WORKERS,
        );
        let server_b = YamuxMultiplexer::new(
            Box::new(b_server),
            yamux::Mode::Server,
            16,
            DEFAULT_SHARD_WORKERS,
        );

        let pool = YamuxMultiplexerPool::new_server(vec![server_a]);
        pool.attach_carrier(server_b).await;
        assert_eq!(pool.live_carriers().await, 2, "附加成员必须在成员表中");

        // 服务端池开 2 流(服务端开偶数 id 流,客户端 mux accept):
        // 轮转恰覆盖初始成员 + 附加成员各 1 条
        for _ in 0..2 {
            deadline(pool.open_stream()).await.unwrap();
        }
        deadline(client_a.accept_stream()).await.unwrap();
        deadline(client_b.accept_stream()).await.unwrap();
        // 恰各 1 条:某载体多分派则下一条 accept 挂起触发 deadline,
        // 少分派则上方循环已挂起。
    }

    /// T-P2(round-9):attach_carrier 后 accept_stream 合流新成员——
    /// 两个客户端 mux 各开 2 流,池 accept 恰收 4 条(跨载体合流)。
    #[tokio::test]
    async fn yamux_pool_attach_carrier_accept_merges() {
        let (a_client, a_server) = tokio::io::duplex(1024 * 1024);
        let (b_client, b_server) = tokio::io::duplex(1024 * 1024);
        let client_a = YamuxMultiplexer::new(
            Box::new(a_client),
            yamux::Mode::Client,
            16,
            DEFAULT_SHARD_WORKERS,
        );
        let client_b = YamuxMultiplexer::new(
            Box::new(b_client),
            yamux::Mode::Client,
            16,
            DEFAULT_SHARD_WORKERS,
        );
        let server_a = YamuxMultiplexer::new(
            Box::new(a_server),
            yamux::Mode::Server,
            16,
            DEFAULT_SHARD_WORKERS,
        );
        let server_b = YamuxMultiplexer::new(
            Box::new(b_server),
            yamux::Mode::Server,
            16,
            DEFAULT_SHARD_WORKERS,
        );

        let pool = YamuxMultiplexerPool::new_server(vec![server_a]);
        pool.attach_carrier(server_b).await;

        for c in [&client_a, &client_b] {
            for _ in 0..2 {
                deadline(c.open_stream()).await.unwrap();
            }
        }
        for _ in 0..4 {
            deadline(pool.accept_stream()).await.unwrap();
        }
        assert!(!pool.is_dead());
    }

    /// 服务端合流:3 条连接的 server mux 合成一池,3 个客户端各开 2 流,
    /// 池 accept 恰收 6 条。
    #[tokio::test]
    async fn yamux_pool_server_accept_merges() {
        const K: usize = 3;
        let mut clients = Vec::new();
        let mut servers = Vec::new();
        for _ in 0..K {
            let (a, b) = tokio::io::duplex(1024 * 1024);
            clients.push(YamuxMultiplexer::new(
                Box::new(a),
                yamux::Mode::Client,
                16,
                DEFAULT_SHARD_WORKERS,
            ));
            servers.push(YamuxMultiplexer::new(
                Box::new(b),
                yamux::Mode::Server,
                16,
                DEFAULT_SHARD_WORKERS,
            ));
        }
        let pool = YamuxMultiplexerPool::new_server(servers);
        for c in &clients {
            for _ in 0..2 {
                deadline(c.open_stream()).await.unwrap();
            }
        }
        for _ in 0..(K * 2) {
            deadline(pool.accept_stream()).await.unwrap();
        }
        assert!(!pool.is_dead());
    }

    /// T-L1(round-9.1 F1):服务端池 shutdown 必须收敛整套资源——
    /// 载体物理连接关闭(对端读到 EOF)、live_carriers 归零、accept
    /// 转发任务 1s 内全部退出。
    #[tokio::test]
    async fn pool_shutdown_closes_carriers_and_forward_tasks() {
        use tokio::io::AsyncReadExt as _;
        let mut members = Vec::new();
        let mut peers = Vec::new();
        for _ in 0..2 {
            // 对端保持裸 duplex:直接从物理层观察 EOF(socket 是否真关)
            let (peer, mux, _kill) = carrier_pair_peer_killable(16);
            peers.push(peer);
            members.push(mux);
        }
        let pool = YamuxMultiplexerPool::new_server(members);
        assert_eq!(pool.live_carriers().await, 2);
        // 让两个转发任务先 park 在各自载体的 accept_stream 上
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        pool.shutdown();

        // 1) 对端读立即 EOF:driver 中止 → socket drop → 物理连接关闭。
        //    建连初期可能残留 12B 帧头(建流授信/ACK),读到 EOF 为准。
        for mut p in peers {
            let mut buf = [0u8; 64];
            let eof = tokio::time::timeout(std::time::Duration::from_secs(1), async {
                loop {
                    match p.read(&mut buf).await {
                        Ok(0) => break,
                        Ok(_) => continue,
                        Err(e) => panic!("对端读不得报错:{e}"),
                    }
                }
            })
            .await;
            assert!(eof.is_ok(), "对端必须在 1s 内读到 EOF(物理连接已关闭)");
        }
        // 2) 成员 mux 全部置 dead → live_carriers 归零
        assert_eq!(pool.live_carriers().await, 0, "shutdown 后无存活载体");
        // 3) 转发任务 1s 内退出(自然收敛或 abort 均计数)
        let t0 = std::time::Instant::now();
        while pool.forward_tasks_exited() < 2 {
            assert!(
                t0.elapsed() < std::time::Duration::from_secs(1),
                "转发任务必须收敛:已退出 {}/2",
                pool.forward_tasks_exited()
            );
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    }

    /// T-L2(round-9.1):shutdown 幂等——连续两次显式调用不 panic,
    /// 池 Drop(内部再调一次)同样安全。
    #[tokio::test]
    async fn pool_shutdown_is_idempotent() {
        let (c0, _s0, _k0) = carrier_pair_killable(16);
        let pool = YamuxMultiplexerPool::new_client(vec![c0], None);
        pool.shutdown();
        pool.shutdown();
        // Drop 路径再调一次 shutdown(impl Drop for YamuxMultiplexerPool)
        drop(pool);
    }

    /// T-L3(round-9.1 F1):客户端池**无显式 shutdown**,整体 Drop 后其
    /// driver 必须收敛——对端读到 EOF,且 PoolInner 最终释放(弱引用
    /// 失效,不留转发任务/健康监视强持有)。
    #[tokio::test]
    async fn client_pool_drop_converges_drivers() {
        use tokio::io::AsyncReadExt as _;
        let (a, mut peer) = tokio::io::duplex(1024 * 1024);
        let mux =
            YamuxMultiplexer::new(Box::new(a), yamux::Mode::Client, 16, DEFAULT_SHARD_WORKERS);
        let pool = YamuxMultiplexerPool::new_client(vec![mux], None);
        let weak = pool.inner_weak();
        drop(pool); // 无显式 shutdown:仅 Drop 兜底
                    // 物理连接随 driver 中止关闭 → 对端读至 EOF(建连初期可能
                    // 残留 12B 帧头,读到 EOF 为准)
        let mut buf = [0u8; 64];
        let eof = tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                match peer.read(&mut buf).await {
                    Ok(0) => break,
                    Ok(_) => continue,
                    Err(e) => panic!("对端读不得报错:{e}"),
                }
            }
        })
        .await;
        assert!(eof.is_ok(), "客户端池 Drop 后对端必须读到 EOF");
        // PoolInner 释放:转发任务(客户端池无)与健康监视(Weak)不滞留
        let t0 = std::time::Instant::now();
        while weak.upgrade().is_some() {
            assert!(
                t0.elapsed() < std::time::Duration::from_secs(1),
                "PoolInner 必须随池 Drop 释放(无任务强持有)"
            );
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    }

    /// 复核回归(round-9.1 F1 补丁):池关停后 in-flight 重拨拨通的载体
    /// 从未入表,必须由 redial_loop 显式关停——否则其 driver 与物理连接
    /// 永久驻留(`YamuxMultiplexer` 的 Drop 不 abort),且 redialing 计数
    /// 不归还。断言:对端 1s 内 EOF、计数归零、成员表仍为空。
    #[tokio::test]
    async fn redial_after_pool_shutdown_closes_fresh_carrier() {
        use tokio::io::AsyncReadExt as _;
        let (c0, _s0, _k0) = carrier_pair_killable(16);
        let pool = YamuxMultiplexerPool::new_client(vec![c0], None);
        let (peer_tx, mut peer_rx) = mpsc::unbounded_channel::<tokio::io::DuplexStream>();
        let redial: PoolRedial = Arc::new(move || {
            let tx = peer_tx.clone();
            Box::pin(async move {
                let (a, b) = tokio::io::duplex(1024 * 1024);
                // 只传对端给测试;客户端 mux 随 redial 返回值交给 redial_loop
                let _ = tx.send(b);
                Ok(YamuxMultiplexer::new(
                    Box::new(a),
                    yamux::Mode::Client,
                    16,
                    DEFAULT_SHARD_WORKERS,
                ))
            })
        });
        // 模拟"重拨在途时池关停":reap_member 已 +1 并 spawn
        pool.inner.redialing.fetch_add(1, Ordering::AcqRel);
        pool.shutdown();
        redial_loop(pool.inner.clone(), redial).await;

        let mut peer = peer_rx.recv().await.expect("redial must produce a carrier");
        let mut buf = [0u8; 64];
        let eof = tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                match peer.read(&mut buf).await {
                    Ok(0) => break,
                    Ok(_) => continue,
                    Err(e) => panic!("对端读不得报错:{e}"),
                }
            }
        })
        .await;
        assert!(eof.is_ok(), "池关停后重拨载体必须在 1s 内关闭(对端 EOF)");
        assert_eq!(pool.redialing(), 0, "关停分支必须归还 in-flight 重拨计数");
        assert_eq!(pool.live_carriers().await, 0, "关停后不得再入表");
    }

    /// 复核回归(round-9.1 F1 补丁):`shutdown()` 的成员快照撞上写锁
    /// 持有者时只能置标志;写锁释放后的复查([`PoolInner::shutdown_sweep`],
    /// attach/redial/reap 三个写路径均已接入)必须补做载体关停——否则
    /// 显式拆除的立即收敛会退化为等 PoolInner 最后一次 Arc 释放。
    #[tokio::test]
    async fn shutdown_snapshot_miss_converges_after_write_unlock() {
        use tokio::io::AsyncReadExt as _;
        let mut members = Vec::new();
        let mut peers = Vec::new();
        for _ in 0..2 {
            let (peer, mux, _kill) = carrier_pair_peer_killable(16);
            peers.push(peer);
            members.push(mux);
        }
        let pool = YamuxMultiplexerPool::new_server(members);
        assert_eq!(pool.live_carriers().await, 2);
        // 持写锁模拟 attach/reap 临界区:shutdown 的 try_read 必然落败,
        // 成员快照为空(这正是发现所指的窗口)。
        let guard = pool.inner.members.write().await;
        pool.shutdown();
        drop(guard); // 写路径释放锁
        pool.inner.shutdown_sweep(); // 写路径释放锁后的复查

        for mut p in peers {
            let mut buf = [0u8; 64];
            let eof = tokio::time::timeout(std::time::Duration::from_secs(1), async {
                loop {
                    match p.read(&mut buf).await {
                        Ok(0) => break,
                        Ok(_) => continue,
                        Err(e) => panic!("对端读不得报错:{e}"),
                    }
                }
            })
            .await;
            assert!(eof.is_ok(), "快照落空后经复查必须关闭载体(对端 1s 内 EOF)");
        }
    }

    /// round-9.2 回归:`reap_member` 的早退分支(成员已被并发路径收敛)
    /// 释放写锁后必须复查关停标志并补做收敛。
    ///
    /// 时序:同一 member 连续两次走生产 [`reap_member`]——第一次把它移出
    /// 成员表(模拟并发收敛);两次之间测试持成员表写锁调用
    /// [`YamuxMultiplexerPool::shutdown`],其两次 `try_read`(成员快照 +
    /// 末尾 sweep)必然落空,只置标志并 abort 转发任务,载体 driver 未关停;
    /// 随后第二次 reap 必走早退分支。
    ///
    /// 修复前该测试必红:早退分支在返回前不调用
    /// [`PoolInner::shutdown_sweep`],而 shutdown 的快照又为空 → 仍在表中
    /// 的载体 driver 永不 abort,对端 1s 内读不到 EOF,`live_carriers()`
    /// 停在 1。修复后:早退分支先 drop 写锁再 sweep → 表中载体被关停,
    /// 对端 1s 内 EOF、`live_carriers()` 归零。
    #[tokio::test]
    async fn reap_early_return_sweeps_after_shutdown() {
        use tokio::io::AsyncReadExt as _;
        let (_p0, m0, _k0) = carrier_pair_peer_killable(16);
        let (mut p1, m1, _k1) = carrier_pair_peer_killable(16);
        let pool = YamuxMultiplexerPool::new_server(vec![m0, m1]);
        assert_eq!(pool.live_carriers().await, 2);

        // 第一次 reap:成员 0 被移出成员表(生产语义:已被并发路径收敛)。
        let member = pool.inner.members.read().await[0].clone();
        reap_member(&pool.inner, member.clone()).await;
        assert_eq!(pool.live_carriers().await, 1, "第一次 reap 应移出成员 0");

        // 持写锁关停:shutdown 的两次 try_read 均落空,只剩关停标志;
        // 释放锁后不再有其他写路径会复查。
        {
            let guard = pool.inner.members.write().await;
            pool.shutdown();
            drop(guard);
        }

        // 第二次 reap 走早退分支 → 必须复查关停标志并关停表中残留载体。
        reap_member(&pool.inner, member).await;

        // 断言对象是仍在成员表里的载体 1;成员 0 已被测试移出,生产中被
        // reap 时必已 dead,不在本断言范围。
        let mut buf = [0u8; 64];
        let eof = tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                match p1.read(&mut buf).await {
                    Ok(0) => break,
                    Ok(_) => continue,
                    Err(e) => panic!("对端读不得报错:{e}"),
                }
            }
        })
        .await;
        assert!(
            eof.is_ok(),
            "早退分支必须补做关停:表中载体需在 1s 内关闭(对端 EOF)"
        );
        assert_eq!(pool.live_carriers().await, 0, "关停后不得残留活载体");
    }

    /// 复核回归:attach_carrier 与 shutdown 并发——写锁等待期间池关停,
    /// 新载体不得入表,且其 driver 必须被关停(对端 EOF)。
    #[tokio::test]
    async fn attach_carrier_during_shutdown_is_closed() {
        use tokio::io::AsyncReadExt as _;
        let (c0, _s0, _k0) = carrier_pair_killable(16);
        let pool = Arc::new(YamuxMultiplexerPool::new_server(vec![c0]));
        let (mut peer, mux, _kill) = carrier_pair_peer_killable(16);

        // 先占写锁,把 attach 排到写锁等待队列,再关停池。
        let guard = pool.inner.members.write().await;
        let p2 = pool.clone();
        let attach = tokio::spawn(async move { p2.attach_carrier(mux).await });
        // 让 attach 进入等待(写到 semaphore 队列),再关停
        tokio::task::yield_now().await;
        pool.shutdown();
        drop(guard);
        deadline(attach).await.unwrap();

        // 池已关停:初始成员与新附加载体都不得存活(入表即会被计数)
        assert_eq!(pool.live_carriers().await, 0, "关停后 attach 不得入表");
        let mut buf = [0u8; 64];
        let eof = tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                match peer.read(&mut buf).await {
                    Ok(0) => break,
                    Ok(_) => continue,
                    Err(e) => panic!("对端读不得报错:{e}"),
                }
            }
        })
        .await;
        assert!(eof.is_ok(), "关停后附加载体必须立即关闭(对端 EOF)");
    }

    // round-6 复核修复回归(发送侧分片):停读-恢复 + 写完即 drop 的流,
    // 其 on_drop RST(ctrl Remove → RecvWorker finalize → to_send 最高
    // 优先级)不得插队到本流滞留在 send_out 的回显数据之前——分片前
    // SendHalf 单任务先消费完流命令才处理 (id,None) 天然保序;分片后
    // FlushTask 与 SendWorker 并发,若无顺序门控,客户端会在回显中途
    // 收到 RST → Closed → read_exact "early eof"(数据丢失)。修复:
    // SendWorker 的 ctrl Remove 投递受"本流末帧已出队"的水位门控
    // (send_out[k] 出队计数 ≥ drop 时的推入序;不要求整队取空,持续
    // 负载下不饥饿)+ FlushTask 取空通知唤醒。
    #[tokio::test]
    async fn mux_pause_resume_echo_stream_drop_rst_not_overtaking() {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
        const TOTAL: usize = 2 * 1024 * 1024;
        let (a, b) = tokio::io::duplex(64 * 1024);
        let server = wrap_stream_as_server(Box::new(b), true, MUX_STREAMS_UNLIMITED);
        let client = wrap_stream_as_client(Box::new(a), true, MUX_STREAMS_UNLIMITED);

        let echo = {
            let server = server.clone();
            tokio::spawn(async move {
                let mut ss = server.accept().await.expect("accept");
                // 停读 2s:让客户端的发送数据积压在背压链上(打开插队
                // 窗口),再读全部并回显;写完即 drop(Open 态 → RST)
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                let mut buf = vec![0u8; 64 * 1024];
                let mut got = 0usize;
                while got < TOTAL {
                    let want = buf.len().min(TOTAL - got);
                    ss.read_exact(&mut buf[..want]).await.expect("srv read");
                    ss.write_all(&buf[..want]).await.expect("echo");
                    got += want;
                }
            })
        };
        let cs = client.open_stream().await.expect("open");
        let (mut cr, mut cw) = tokio::io::split(cs);
        let verifier = tokio::spawn(async move {
            let mut got = 0usize;
            let mut buf = vec![0u8; 64 * 1024];
            while got < TOTAL {
                let want = buf.len().min(TOTAL - got);
                cr.read_exact(&mut buf[..want])
                    .await
                    .expect("echo read must not see EOF before all echoed data");
                assert!(buf[..want].iter().all(|&b| b == 7));
                got += want;
            }
        });
        let mut pos = 0usize;
        let chunk = vec![7u8; 64 * 1024];
        while pos < TOTAL {
            cw.write_all(&chunk).await.expect("write");
            pos += chunk.len();
        }
        tokio::time::timeout(std::time::Duration::from_secs(30), verifier)
            .await
            .expect("verifier timeout: echo must complete")
            .expect("verifier join");
        let _ = tokio::time::timeout(std::time::Duration::from_secs(10), echo)
            .await
            .expect("echo join");
    }

    // round-5 §2.7/§4.3 + round-6 §4.4 锁:mux_config 保持默认 shard_workers,
    // halves 四元组的两个 Worker Vec 长度必须都等于 vendor 默认 4(三机
    // 验收以此为准;调参须另立规格)。经 into_halves 返回的 Vec 长度观测,
    // 不依赖 vendor 暴露 getter;纯构造不 poll,无需运行时(两半/Worker
    // 丢弃只走 Drop 收敛)。
    #[test]
    fn mux_config_consumes_default_four_shard_workers() {
        let (a, _b) = tokio::io::duplex(64);
        let conn: MuxConn = yamux::Connection::new(
            YamuxCarrier::new(Box::new(a)),
            mux_config(MUX_STREAMS_UNLIMITED),
            yamux::Mode::Client,
        );
        let (_recv, _send, workers, send_workers) = conn.into_halves();
        assert_eq!(
            workers.len(),
            4,
            "shard_workers 默认 4 不得被 mux_config 覆盖(round-5 §2.7)"
        );
        assert_eq!(
            send_workers.len(),
            4,
            "send workers 与 RecvWorker 同 N 同模数(round-6 §4.4)"
        );
    }

    // T-C5(round-10):池规模 → 每载体 shard 映射。K=1 必须回到 4
    // (红线:K=1 路径零回退),K≥4 收敛到 1,总量 ≤ 4。
    #[test]
    fn shard_workers_for_pool_size() {
        assert_eq!(shard_workers_for_k(1), 4);
        assert_eq!(shard_workers_for_k(2), 2);
        assert_eq!(shard_workers_for_k(3), 1);
        assert_eq!(shard_workers_for_k(4), 1);
        assert_eq!(shard_workers_for_k(8), 1);
        assert_eq!(shard_workers_for_k(0), 4, "0 按 1 处理,不 panic");
        for k in 1..=8 {
            assert!(
                (1..=DEFAULT_SHARD_WORKERS).contains(&shard_workers_for_k(k)),
                "K={k} 的 shard 必须在 1..=4"
            );
        }
    }

    // T-C5:参数化入口必须真实生效——K=2 时两侧各 2 个 Worker,
    // 且与默认入口(4)可区分。
    #[test]
    fn mux_config_with_shards_is_effective() {
        let (a, _b) = tokio::io::duplex(64);
        let conn: MuxConn = yamux::Connection::new(
            YamuxCarrier::new(Box::new(a)),
            mux_config_with_shards(MUX_STREAMS_UNLIMITED, shard_workers_for_k(2)),
            yamux::Mode::Client,
        );
        let (_recv, _send, workers, send_workers) = conn.into_halves();
        assert_eq!(workers.len(), 2);
        assert_eq!(send_workers.len(), 2);
    }

    // A4 长期锁(read_chunk_from_cap_lock):注入超限缓冲(256KiB,镜像池
    // 播种形态;B3 合入后 pool 拒收 oversized,直接播种会恒真,故走
    // read_chunk_into 测试缝)——每块必须 ≤ CHUNK_CAP,载荷逐字节完整。
    #[tokio::test]
    async fn read_chunk_from_cap_lock() {
        let input = vec![5u8; 200 * 1024];
        let mut reader = &input[..];
        let mut total = 0usize;
        loop {
            let oversized = bytes::BytesMut::with_capacity(256 * 1024);
            let chunk = read_chunk_into(&mut reader, CHUNK_CAP, oversized)
                .await
                .unwrap()
                .expect("input not exhausted");
            assert!(
                chunk.len() <= CHUNK_CAP,
                "块 {} 超过 64KiB 上限(A4)",
                chunk.len()
            );
            total += chunk.len();
            if total == input.len() {
                break;
            }
        }
        assert_eq!(total, input.len());
    }

    // C4:慢启动(滴流)阶段读路径不得借用池缓冲——UpReader 初始相本地
    // 缓冲(12KiB < vendor MIN_RECYCLE=16KiB,不经池、写穿归池被拒收);
    // 读满一次即升级池相。逐字节验证两相切换(按读取器实例计数,不受
    // 并行测试的共享池活动影响)。
    #[tokio::test]
    async fn idle_chunk_relay_releases_buffer() {
        use tokio::io::AsyncWriteExt as _;
        const _: () = assert!(INIT_BUF_CAP < 16 * 1024, "初始相缓冲必须小于 MIN_RECYCLE");

        // 滴流对端:每 10ms 抵达 1 字节,共 5 字节
        let (mut local, mut relay_io) = tokio::io::duplex(4096);
        let feeder = tokio::spawn(async move {
            for v in 1u8..=5 {
                if local.write_u8(v).await.is_err() {
                    return;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        });

        let mut reader = UpReader::default();
        for v in 1u8..=5 {
            let chunk = reader
                .read_chunk(&mut relay_io)
                .await
                .unwrap()
                .expect("trickle byte");
            assert_eq!(chunk.len(), 1);
            assert_eq!(chunk[0], v);
        }
        assert_eq!(
            reader.pool_borrows, 0,
            "滴流(初始相)不得借用池缓冲(C4:空闲中继驻留 12KiB 本地缓冲)"
        );
        feeder.abort();
        let _ = feeder.await;

        // 升级:一次读满 INIT_BUF_CAP 即进入池相(后续读经池)
        let data = vec![7u8; INIT_BUF_CAP * 3];
        let mut mem = &data[..];
        let c1 = reader
            .read_chunk(&mut mem)
            .await
            .unwrap()
            .expect("full init read");
        assert_eq!(c1.len(), INIT_BUF_CAP);
        assert_eq!(reader.pool_borrows, 0, "恰好读满初始相的那次读仍走本地缓冲");
        let c2 = reader
            .read_chunk(&mut mem)
            .await
            .unwrap()
            .expect("pool phase read");
        assert!(!c2.is_empty());
        assert_eq!(reader.pool_borrows, 1, "升级后读路径进入池相");
    }

    // Y1 chunk 中继:真实 echo 目标 + 偷取 halves → mux_relay;
    // 大载荷(> 64KiB 分片,覆盖 write_chunk 内部续写/部分消费路径)、
    // 双向完整性、FIN 半关闭逐跳传递。
    #[tokio::test]
    async fn mux_relay_echo_and_half_close() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let echo = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let echo_addr = echo.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let Ok((c, _)) = echo.accept().await else {
                    break;
                };
                tokio::spawn(async move {
                    let (mut r, mut w) = c.into_split();
                    let _ = tokio::io::copy(&mut r, &mut w).await;
                });
            }
        });

        let (a, b) = tokio::io::duplex(256 * 1024);
        let server = wrap_stream_as_server(Box::new(b), true, MUX_STREAMS_UNLIMITED);
        let client = wrap_stream_as_client(Box::new(a), true, MUX_STREAMS_UNLIMITED);

        // 服务端角色:accept → 偷 halves → mux_relay 对 echo 目标
        let srv = server.clone();
        tokio::spawn(async move {
            let mut s = srv.accept().await.unwrap();
            assert!(is_mux_stream(&s), "accepted stream should be RhMuxStream");
            let halves = steal_mux_halves(&mut s).expect("steal");
            drop(s); // 壳
            let echo_conn = tokio::net::TcpStream::connect(echo_addr).await.unwrap();
            mux_relay(Box::new(echo_conn), halves.0, halves.1, None, None).await;
        });

        let mut c = client.open_stream().await.unwrap();
        // 300KiB 模式载荷:跨多个 64KiB 帧
        let payload: Vec<u8> = (0..300 * 1024).map(|i| (i % 253) as u8).collect();
        c.write_all(&payload).await.unwrap();
        c.shutdown().await.unwrap(); // → FIN → 中继 → echo 关 → 回程 EOF
        let mut got = Vec::new();
        c.read_to_end(&mut got).await.unwrap();
        assert_eq!(got.len(), payload.len());
        assert!(got.iter().zip(&payload).all(|(x, y)| x == y));
    }

    // 兼容读余量前传:compat poll_read 留在 pending 的字节必须在偷取后
    // 由读半最先交付,不丢数据。
    #[tokio::test]
    async fn mux_relay_delivers_compat_pending_leftover() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let (a, b) = tokio::io::duplex(64 * 1024);
        let server = wrap_stream_as_server(Box::new(b), true, MUX_STREAMS_UNLIMITED);
        let client = wrap_stream_as_client(Box::new(a), true, MUX_STREAMS_UNLIMITED);

        let mut c = client.open_stream().await.unwrap();
        c.write_all(&[1, 2, 3, 4, 5, 6, 7, 8]).await.unwrap();

        let mut s = server.accept().await.unwrap();
        let mut probe = [0u8; 3];
        s.read_exact(&mut probe).await.unwrap();
        assert_eq!(&probe, &[1, 2, 3]);

        // 偷取后读半先回吐 pending 的 5 字节
        let (mut r_half, _w_half) = steal_mux_halves(&mut s).expect("steal");
        drop(s);
        let first = r_half.read_chunk().await.unwrap().expect("pending chunk");
        assert_eq!(first.as_ref(), &[4, 5, 6, 7, 8]);
    }

    // 回落判定:非 mux 流(裸 duplex Box)不是 RhMuxStream
    #[tokio::test]
    async fn mux_detection_rejects_plain_io() {
        let (a, _b) = tokio::io::duplex(64);
        let io: BoxedIo = Box::new(a);
        assert!(!is_mux_stream(&io));
        let mut io = io;
        assert!(steal_mux_halves(&mut io).is_none());
    }

    #[tokio::test]
    async fn yamux_open_accept_echo() {
        let (a, b) = tokio::io::duplex(4096);
        let server = wrap_stream_as_server(Box::new(b), true, MUX_STREAMS_UNLIMITED);
        let client = wrap_stream_as_client(Box::new(a), true, MUX_STREAMS_UNLIMITED);

        let srv = server.clone();
        tokio::spawn(async move {
            let mut s = srv.accept().await.unwrap();
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let mut buf = [0u8; 16];
            let n = s.read(&mut buf).await.unwrap();
            s.write_all(&buf[..n]).await.unwrap();
        });

        let mut c = client.open_stream().await.unwrap();
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        c.write_all(b"ping").await.unwrap();
        let mut out = [0u8; 4];
        c.read_exact(&mut out).await.unwrap();
        assert_eq!(&out, b"ping");
    }

    #[tokio::test]
    async fn yamux_many_streams() {
        let (a, b) = tokio::io::duplex(8192);
        let server = wrap_stream_as_server(Box::new(b), true, MUX_STREAMS_UNLIMITED);
        let client = wrap_stream_as_client(Box::new(a), true, MUX_STREAMS_UNLIMITED);
        let srv = server.clone();
        let echo = tokio::spawn(async move {
            for _ in 0..8 {
                let mut s = match srv.accept().await {
                    Ok(s) => s,
                    Err(_) => return,
                };
                tokio::spawn(async move {
                    use tokio::io::{AsyncReadExt, AsyncWriteExt};
                    let mut buf = [0u8; 64];
                    if let Ok(n) = s.read(&mut buf).await {
                        let _ = s.write_all(&buf[..n]).await;
                    }
                });
            }
        });
        let mut handles = Vec::new();
        for i in 0..8u8 {
            let cli = client.clone();
            handles.push(tokio::spawn(async move {
                let mut s = cli.open_stream().await.unwrap();
                use tokio::io::{AsyncReadExt, AsyncWriteExt};
                let payload = vec![i; 32];
                s.write_all(&payload).await.unwrap();
                let mut out = vec![0u8; 32];
                s.read_exact(&mut out).await.unwrap();
                assert_eq!(out, payload);
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
        echo.abort();
    }

    // 回归:单条复用连接上的并发存活流必须能超过 256。
    // 旧实现把流上限焊死在 256,multiplex 会话的并发数据通道全部被它卡死
    //(且服务端侧超限 SYN 会触发 yamux 终止整条连接)。
    #[tokio::test]
    async fn yamux_concurrent_streams_over_256() {
        let (a, b) = tokio::io::duplex(256 * 1024);
        let server = wrap_stream_as_server(Box::new(b), true, MUX_STREAMS_UNLIMITED);
        let client = wrap_stream_as_client(Box::new(a), true, MUX_STREAMS_UNLIMITED);
        let srv = server.clone();
        tokio::spawn(async move {
            while let Ok(mut s) = srv.accept().await {
                tokio::spawn(async move {
                    use tokio::io::{AsyncReadExt, AsyncWriteExt};
                    let mut buf = [0u8; 32];
                    if s.read_exact(&mut buf).await.is_ok() {
                        let _ = s.write_all(&buf).await;
                    }
                });
            }
        });
        const N: usize = 300;
        // 逐条开流并写载荷:ack backlog 随回显流动,开流不会滞死;
        // 流保持打开 → N 条同时存活(旧上限在第 257 条 open 即报错)
        let mut streams = Vec::with_capacity(N);
        for i in 0..N {
            let mut s = client.open_stream().await.unwrap();
            use tokio::io::AsyncWriteExt;
            s.write_all(&[i as u8; 32]).await.unwrap();
            streams.push(s);
        }
        for mut s in streams {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let mut out = [0u8; 32];
            s.read_exact(&mut out).await.unwrap();
            s.shutdown().await.unwrap();
        }
    }

    // tcpmux 标准模式全链路:用户侧 yamux 客户端在一条通道上并发多开流;
    // 服务端(= tcpmux_server_mux)accept 每流 → 拨号本地 TCP 目标 + 裸双向搬运
    // (与 vhost.rs 的 accept 循环、rh-client handle_start_forward 的每流转发同构)。
    // 校验:并发流隔离、大负载完整、半关闭(写 EOF → 目标回 EOF)。
    #[tokio::test]
    async fn tcpmux_stream_path_over_real_target() {
        let echo = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let echo_addr = echo.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let Ok((c, _)) = echo.accept().await else {
                    break;
                };
                tokio::spawn(async move {
                    let (mut r, mut w) = c.into_split();
                    let _ = tokio::io::copy(&mut r, &mut w).await;
                });
            }
        });

        let (a, b) = tokio::io::duplex(64 * 1024);
        let server = tcpmux_server_mux(Box::new(b), MUX_STREAMS_UNLIMITED);
        let client = wrap_stream_as_client(Box::new(a), true, MUX_STREAMS_UNLIMITED);

        let srv = server.clone();
        tokio::spawn(async move {
            loop {
                let Ok(mut stream) = srv.accept().await else {
                    break;
                };
                tokio::spawn(async move {
                    if let Ok(mut local) = tokio::net::TcpStream::connect(echo_addr).await {
                        let _ = tokio::io::copy_bidirectional(&mut stream, &mut local).await;
                    }
                });
            }
        });

        const N: usize = 8;
        let mut handles = Vec::new();
        for i in 0..N {
            let cli = client.clone();
            handles.push(tokio::spawn(async move {
                use tokio::io::{AsyncReadExt, AsyncWriteExt};
                let mut s = cli.open_stream().await.unwrap();
                let payload = vec![i as u8; 4096];
                s.write_all(&payload).await.unwrap();
                s.shutdown().await.unwrap();
                let mut out = Vec::new();
                s.read_to_end(&mut out).await.unwrap();
                assert_eq!(out.len(), payload.len());
                assert!(out.iter().all(|&b| b == i as u8));
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
    }

    // round-E2 duplex 冒烟:tokio 内存 TcpListener 对拨,server/client
    // 两侧均 new_duplex(裸 TCP 双 fd,镜像 tcp.rs try_dup_tcp 的
    // AsFd/try_clone_to_owned + from_std 路径),open_stream/accept/echo
    // 全链往返——真实 TCP socket 上验证读∥写零锁路径的组网。
    #[tokio::test]
    async fn duplex_mux_tcp_smoke_roundtrip() {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
        use tokio::net::{TcpListener, TcpStream};

        fn try_dup_tcp(stream: &TcpStream) -> TcpStream {
            use std::os::fd::AsFd as _;
            let fd = stream.as_fd().try_clone_to_owned().expect("dup fd");
            TcpStream::from_std(std::net::TcpStream::from(fd)).expect("register dup with runtime")
        }

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("server accept");
            let write_dup = try_dup_tcp(&stream);
            let mux = YamuxMultiplexer::new_duplex(
                Box::new(stream),
                Box::new(write_dup),
                yamux::Mode::Server,
                MUX_STREAMS_UNLIMITED,
                DEFAULT_SHARD_WORKERS,
            );
            // echo:收 5 字节回 5 字节
            let mut s = mux.accept_stream().await.expect("server accept stream");
            let mut buf = [0u8; 5];
            s.read_exact(&mut buf).await.expect("server read");
            s.write_all(&buf).await.expect("server echo");
        });

        let client_stream = TcpStream::connect(addr).await.expect("client connect");
        let write_dup = try_dup_tcp(&client_stream);
        let mux = YamuxMultiplexer::new_duplex(
            Box::new(client_stream),
            Box::new(write_dup),
            yamux::Mode::Client,
            MUX_STREAMS_UNLIMITED,
            DEFAULT_SHARD_WORKERS,
        );
        let mut c = mux.open_stream().await.expect("client open stream");
        c.write_all(b"hello").await.expect("client write");
        let mut buf = [0u8; 5];
        tokio::time::timeout(std::time::Duration::from_secs(5), c.read_exact(&mut buf))
            .await
            .expect("echo must return within 5s")
            .expect("client read");
        assert_eq!(&buf, b"hello");
        tokio::time::timeout(std::time::Duration::from_secs(5), server)
            .await
            .expect("server task must finish")
            .unwrap();
    }
}
