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
use std::sync::atomic::{AtomicBool, Ordering};
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

fn pool_put(b: bytes::BytesMut) {
    yamux::pool::put(b)
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
        if b_write.write_chunk(chunk).await.is_err() {
            return false;
        }
    }
}

/// down 侧搬运:mux 块 → a 写。满块(≥64KiB)直写(零拷贝单次 syscall);
/// 小块聚合到 64KiB 再写——帧不满时恢复旧 copy 路径「多帧合并一次写」
/// 的节奏(降低 syscall/唤醒频率),无后续就绪块即冲刷保延迟。
/// 聚合缓冲取自/归还 vendor 池。
///
/// 限速=送达计费(A3/A5):字节入聚合槽不扣费;冲刷/直写前按写出量预留,
/// 按实际写出逐步消费——取消/部分写时未消费预留自动退还,
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

    async fn flush<W: tokio::io::AsyncWrite + Unpin>(
        w: &mut W,
        acc: &mut Option<bytes::BytesMut>,
        limiter: &Option<std::sync::Arc<dyn crate::ChunkLimiter>>,
    ) -> bool {
        let Some(bm) = acc.take() else {
            return true;
        };
        if bm.is_empty() {
            pool_put(bm);
            return true;
        }
        let b = bm.freeze();
        let ok = write_metered(w, &b, limiter).await;
        if let Ok(mut m) = b.try_into_mut() {
            m.clear();
            pool_put(m);
        }
        ok
    }

    let mut acc: Option<bytes::BytesMut> = None;
    let clean = 'outer: loop {
        let chunk = match b_read.read_chunk().await {
            Ok(Some(c)) => c,
            Ok(None) => break 'outer true,
            Err(_) => break 'outer false,
        };
        if chunk.len() >= CHUNK_CAP {
            if !flush(a_w, &mut acc, limiter).await {
                break 'outer false;
            }
            if !write_metered(a_w, &chunk, limiter).await {
                break 'outer false;
            }
            continue;
        }
        let slot = acc.get_or_insert_with(|| pool_get(CHUNK_CAP));
        let room = CHUNK_CAP - slot.len();
        let take = chunk.len().min(room);
        slot.extend_from_slice(&chunk[..take]);
        if take < chunk.len() {
            // 余量回退(未入槽、未扣费;下一轮作为新块处理)
            b_read.push_front(chunk.slice(take..));
            if !flush(a_w, &mut acc, limiter).await {
                break 'outer false;
            }
            continue;
        }
        if acc.as_ref().is_some_and(|b| b.len() >= CHUNK_CAP) {
            if !flush(a_w, &mut acc, limiter).await {
                break 'outer false;
            }
            continue;
        }
        // 聚合后续立即就绪的块(入槽字节下一轮冲刷时统一预留/消费)
        loop {
            match b_read.try_read_chunk().await {
                Ok(Some(c)) if !c.is_empty() => {
                    let slot = acc.get_or_insert_with(|| pool_get(CHUNK_CAP));
                    let room = CHUNK_CAP - slot.len();
                    let take = c.len().min(room);
                    slot.extend_from_slice(&c[..take]);
                    if take < c.len() {
                        b_read.push_front(c.slice(take..));
                        if !flush(a_w, &mut acc, limiter).await {
                            break 'outer false;
                        }
                        break;
                    }
                    if slot.len() >= CHUNK_CAP {
                        if !flush(a_w, &mut acc, limiter).await {
                            break 'outer false;
                        }
                        break;
                    }
                }
                Ok(Some(_)) => {}
                Ok(None) | Err(_) => {
                    if !flush(a_w, &mut acc, limiter).await {
                        break 'outer false;
                    }
                    break;
                }
            }
        }
    };
    let _ = flush(a_w, &mut acc, limiter).await;
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

/// yamux 连接配置(锁路径 [`YamuxMultiplexer::new`] 与双载体路径
/// [`YamuxMultiplexer::new_duplex`] 共用,round-E2 起提取以免两路漂移)。
fn mux_config(max_streams: usize) -> yamux::Config {
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
    // 分片 Worker 数(round-5 §2.7):**不覆盖**,消费 vendor 默认 4。
    // 该值只作用于 halves 路径(本文件两路入口均走 halves),未拆分
    // Active 路径忽略;三机验收 ×16 下行目标 ≥3100 Mbps / 数据面
    // 2.0~3.5 核以默认 4 为准,调参须另立规格并回归三机。
    cfg
}

pub struct YamuxMultiplexer {
    open_tx: mpsc::Sender<oneshot::Sender<io::Result<yamux::Stream>>>,
    accept_rx: tokio::sync::Mutex<mpsc::Receiver<yamux::Stream>>,
    dead: Arc<AtomicBool>,
}

impl YamuxMultiplexer {
    /// 在给定字节流上创建 yamux(锁路径);mode:客户端(开流)/服务端
    /// (收流)。max_streams:并发流硬顶,0 = 不限制(折算
    /// MUX_STREAMS_UNLIMITED)。
    /// 注:yamux 0.14 无内建 keepalive,会话存活依赖协议层心跳(< 会话空闲超时)。
    pub fn new(io: BoxedIo, mode: yamux::Mode, max_streams: usize) -> Self {
        let conn: MuxConn =
            yamux::Connection::new(YamuxCarrier::new(io), mux_config(max_streams), mode);
        // Round-D:收/发双任务驱动(对齐 Go yamux recv/send 双 goroutine)。
        // 单一载体经 SplitSocket/BiLock 串行——TLS/WS/QUIC/KCP 等不可
        // fd 复制的载体走此路径(泛型兜底)。Round-5 起三元组:第三件
        // 是 N 个分片 Worker(数量由 Config.shard_workers 决定,见
        // [`mux_config`]),与两半一同 spawn。
        let (recv_half, send_half, workers) = conn.into_halves();
        Self::spawn_drivers(recv_half, send_half, workers)
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
    ) -> Self {
        let conn: MuxConn =
            yamux::Connection::new(YamuxCarrier::new(read_io), mux_config(max_streams), mode);
        let (recv_half, send_half, workers) = conn.into_halves_duplex(YamuxCarrier::new(write_io));
        Self::spawn_drivers(recv_half, send_half, workers)
    }

    /// 收/发双任务 + N 个分片 Worker 任务驱动(两路径共用):接收任务
    /// 解帧+路由,新流入 accept_tx(容量 256 背压——接收阻塞只停读侧,
    /// 不影响发送任务);发送任务处理开流请求 + 写路径;Worker 任务独占
    /// `sid % N` 的流集合,做流级派发/流控记账/唤醒(Round-5)。
    /// S: Send + 'static 供 tokio::spawn(halves 的 Send/生命周期随
    /// socket 类型);[`yamux::ShardWorker`] 不带 socket 泛型参数,其
    /// `Send + 'static` 由 vendor 侧结构保证(Arc/Mutex/原子,无 Rc)。
    ///
    /// Worker 生命周期:JoinHandle 丢弃即分离(同两半的 driver);Worker
    /// 在观察到共享错误槽(任一半 fatal / RecvHalf Drop 置 Closed)后自行
    /// `Ready(())` 终结并排空本地流表,无需 abort——与 vendor §2.6 生命周期
    /// 矩阵一致,不存在 Worker 永存泄漏面。
    fn spawn_drivers<S>(
        recv_half: yamux::RecvHalf<S>,
        send_half: yamux::SendHalf<S>,
        workers: Vec<yamux::ShardWorker>,
    ) -> Self
    where
        S: futures::AsyncRead + futures::AsyncWrite + Unpin + yamux::Writev2 + Send + 'static,
    {
        let (open_tx, open_rx) = mpsc::channel::<oneshot::Sender<io::Result<yamux::Stream>>>(64);
        let (accept_tx, accept_rx) = mpsc::channel::<yamux::Stream>(256);
        let dead = Arc::new(AtomicBool::new(false));
        // Worker 先于两半 spawn:接收任务路由的分片通道有界(cap 128),
        // 消费侧未起即满 → 接收任务 park 停读;顺序不影响正确性(丢唤醒
        // 防线保证消费侧起后必被唤醒),只是让首批帧少一次 park。
        for w in workers {
            tokio::spawn(w);
        }
        tokio::spawn(send_driver(send_half, open_rx, dead.clone()));
        tokio::spawn(recv_driver(recv_half, accept_tx, dead.clone()));
        Self {
            open_tx,
            accept_rx: tokio::sync::Mutex::new(accept_rx),
            dead,
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
                                let _ = req.send(Err(other(&format!("yamux: {e}"))));
                                dead.store(true, Ordering::Relaxed);
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
                        let _ = req.send(Err(other(&format!("yamux: {e}"))));
                        dead.store(true, Ordering::Relaxed);
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

    // round-5 §2.7/§4.3 锁:mux_config 不覆盖 shard_workers,halves 三元组
    // 的 Worker 数必须等于 vendor 默认 4(三机验收以此为准;调参须另立
    // 规格)。经 into_halves 返回的 Vec 长度观测,不依赖 vendor 暴露
    // getter;纯构造不 poll,无需运行时(两半/Worker 丢弃只走 Drop 收敛)。
    #[test]
    fn mux_config_consumes_default_four_shard_workers() {
        let (a, _b) = tokio::io::duplex(64);
        let conn: MuxConn = yamux::Connection::new(
            YamuxCarrier::new(Box::new(a)),
            mux_config(MUX_STREAMS_UNLIMITED),
            yamux::Mode::Client,
        );
        let (_recv, _send, workers) = conn.into_halves();
        assert_eq!(
            workers.len(),
            4,
            "shard_workers 默认 4 不得被 mux_config 覆盖(round-5 §2.7)"
        );
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
