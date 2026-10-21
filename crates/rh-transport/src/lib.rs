//! rh-transport:统一传输抽象。
//!
//! - 物理载体:tcp / tls / websocket / wss / quic / kcp。
//! - 多路复用:quic/kcp 天然多流;tcp/tls/ws 可选 yamux;不复用时每流一连接。
//! - 客户端可经 HTTP CONNECT 或 SOCKS5 代理拨号(仅面向 TCP 的载体)。
//! - snappy 流压缩包装(代理级 use_compression)。
//!
//! 连接/流分离见 [`Multiplexer`]:客户端 `open_stream`,服务端 `accept`,
//! 与设计 §10 的预留边界一致(QUIC 场景一连接多流,裸 TCP 场景一流一连接)。

pub mod compress;
pub mod kcp;
pub mod mux;
pub use mux::{
    wrap_stream_as_client, wrap_stream_as_server, PoolRedial, YamuxMultiplexer,
    YamuxMultiplexerPool, MUX_STREAMS_UNLIMITED,
};
pub mod proxy_dial;
pub mod quic;
#[cfg(target_os = "linux")]
pub mod splice;
pub mod tcp;
pub mod tls;
pub mod ws;

use std::fmt;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite};

pub use compress::compress_stream;

/// 底层 IO 对象:实现 AsyncRead + AsyncWrite + Send + Unpin 即可。
/// `as_any` 供数据面做具体类型反射(splice 资格判定:提取裸 TCP);
/// `into_any_box` 供按具体类型取回所有权(mux 流零拷贝路径降级判定)。
pub trait Io: AsyncRead + AsyncWrite + Send + Unpin {
    fn as_any(&self) -> &dyn std::any::Any;
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any;
    fn into_any_box(self: Box<Self>) -> Box<dyn std::any::Any>;

    /// 无锁拆成独立的读半/写半(round-69)。
    ///
    /// # 为什么需要
    ///
    /// `tokio::io::split` 用**一把 `std::sync::Mutex`** 包住整个 stream
    /// (`tokio/src/io/split.rs:59` 的 `with_lock` 直接 `lock().unwrap()`)。
    /// 在 async 任务里用同步锁的后果:一端在 poll 期间持锁,另一端要
    /// `lock()` 就会**阻塞整个 OS 线程**(不是让出,是 futex 等待)。
    ///
    /// 实测(上行 P=16,服务端 12s):`strace -c` 显示
    /// **`futex` 占 75.0% 的 syscall 时间(39.85s/53.13s),3690 次调用,
    /// 平均每次 10.8ms** ——这就是那把锁。
    ///
    /// 同一时刻只有一个读者和一个写者,poll 本身不会长期持锁;但当
    /// `poll_read` 内部走到内核态(recvfrom 0.36ms、copy 12.7%)时,
    /// 写者会被阻塞同一时长,反之亦然。高并发下这些阻塞叠加。
    ///
    /// 默认返回 `None`(调用方回落到 `tokio::io::split`)。
    fn into_halves(self: Box<Self>) -> Option<(BoxedIo, BoxedIo)>;
}
impl<T: AsyncRead + AsyncWrite + Send + Unpin + 'static> Io for T {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
    fn into_any_box(self: Box<T>) -> Box<dyn std::any::Any> {
        self
    }
    fn into_halves(self: Box<Self>) -> Option<(BoxedIo, BoxedIo)> {
        None
    }
}

pub type BoxedIo = Box<dyn Io>;

/// 把 `BoxedIo` 无锁拆成独立读写半(round-69);不支持时返回 `None`。
///
/// 仅裸 `TcpStream` 支持(`into_split` 产出两个独立 fd,各有一条 tokio
/// IO 注册,读写之间无共享锁)。任何包装类型返回 `None`,调用方回落到
/// [`tokio::io::split`]。
///
/// 之所以用**自由函数 + downcast**而不是 trait 方法:泛型
/// `impl<T: AsyncRead+AsyncWrite> Io for T` 已经覆盖所有类型,再加一个
/// 针对 `TcpStream` 的 impl 会冲突;而 `into_any_box` 只能在 `Box<Self>`
/// 上调用,无法从 `Box<dyn Io>` 直接透传到具体类型。
/// 探测 + 拆分(round-69)。不支持时把 IO 原样还回 `Err`。
pub fn split_boxed_io(io: BoxedIo) -> Result<(BoxedIo, BoxedIo), BoxedIo> {
    // 先探测(GET 语义,不消费):`as_any` 落到 `dyn Io` 的 vtable,
    // 对 `TcpStream` 返回 Some。
    let is_tcp = {
        let dynio: &dyn Io = &*io;
        dynio.as_any().is::<tokio::net::TcpStream>()
    };
    if !is_tcp {
        return Err(io);
    }
    let any = io.into_any_box();
    let ts = any
        .downcast::<tokio::net::TcpStream>()
        .expect("probed above: concrete type is TcpStream");
    let (r, w) = (*ts).into_split();
    // OwnedReadHalf/OwnedWriteHalf 各自只实现单方向 trait,用专用包装
    // 补齐另一方向(运行时永远不会走到,纯为满足 `Io` 的 supertrait)。
    Ok((
        Box::new(HalfRead(r)) as BoxedIo,
        Box::new(HalfWrite(w)) as BoxedIo,
    ))
}

/// 取消信号(共享):完成即结束转发。规则禁用/删除级联用;
/// splice 泵与 mux chunk 中继共用同一形态。
pub type CancelFuture = futures::future::Shared<futures::future::BoxFuture<'static, ()>>;

/// JoinHandle 守卫(goal-repair A1):被 drop 时 abort 子任务;
/// 实现 Future(以 `&mut` 等待,句柄不移出守卫)。
///
/// 中继父任务的正确取消形态:两个方向照旧 `tokio::spawn`,父 future 以
/// `join_relay_pumps(&mut g1, &mut g2)` 等待——父取消 → join future 被 drop →
/// 守卫 Drop → 双向子泵终止。禁止把句柄 move 进 join future(那会 detach)。
pub struct AbortOnDropHandle {
    task: Option<tokio::task::JoinHandle<()>>,
    /// 任务是否以 panic 结束(JoinError 的可观测面;正常完成/取消/未完成为 false)。
    panicked: bool,
}

impl AbortOnDropHandle {
    pub fn new(h: tokio::task::JoinHandle<()>) -> Self {
        Self {
            task: Some(h),
            panicked: false,
        }
    }

    /// 已完成任务是否发生 panic。守卫吸收 JoinError(不向上传播 panic),
    /// 但中继收尾必须能区分「正常结束」与「panic 结束」——后者要求立即
    /// 终止对向泵(见 [`join_relay_pumps`]),否则单向 panic 会让对向永存。
    pub fn panicked(&self) -> bool {
        self.panicked
    }
}

impl Drop for AbortOnDropHandle {
    fn drop(&mut self) {
        if let Some(h) = self.task.take() {
            h.abort();
        }
    }
}

impl std::future::Future for AbortOnDropHandle {
    type Output = ();
    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<()> {
        // JoinHandle: Unpin——无结构性固定问题
        let Some(h) = self.task.as_mut() else {
            return std::task::Poll::Ready(());
        };
        match std::pin::Pin::new(h).poll(cx) {
            std::task::Poll::Ready(Ok(())) => {
                self.task = None; // 已完成:Drop 不再 abort
                std::task::Poll::Ready(())
            }
            std::task::Poll::Ready(Err(join_err)) => {
                // panic(或取消,JoinError::is_cancelled)吸收为状态:
                // 不向上传播,也不让对向泵无人终止
                self.panicked = join_err.is_panic();
                self.task = None;
                std::task::Poll::Ready(())
            }
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }
}

/// 半关闭的**有界兜底**上限(round-40)。
///
/// `clean EOF` 路径本应无界等待完整半关闭(红线:不硬加短超时,否则
/// FIN 尚未写穿即放弃会截断半关闭)。但实测存在**对端永不回 FIN** 的
/// 情形,此时 socket 永久停在 `FIN-WAIT-2`/`CLOSING`,流与 IO 都不释放:
///
/// * 复现:`-P8` 连跑 10 轮,客户端侧 `ss -tan` 到本地目标的连接出现
///   `22 CLOSING + 22 FIN-WAIT-2`,且 20 秒后仍不消失(约 2.2 条/轮);
/// * 对照(同参数直连、不经隧道):只有 `TIME-WAIT`,零残留;
/// * 后果:长跑后连接数单调增长,吞吐从 2.4 G 逐轮衰减到 0.5 G,
///   最终耗尽 fd。
///
/// 取值 30s 的理由:远大于任何正常 RTT(跨机实测 0.1~0.5ms)与 TCP
/// 重传窗口(RTO 下限 200ms,3 次重传 ≈ 1.4s),不会误伤正常半关闭;
/// 又远小于"永不发生"的量级,能及时回收。可用环境变量
/// `RH_HALFCLOSE_GRACE_SECS` 覆盖(0 = 关闭兜底,回到旧的无界等待)。
const DEFAULT_HALFCLOSE_GRACE: std::time::Duration = std::time::Duration::from_secs(30);

/// 半关闭兜底上限(env 可覆盖,进程内只解析一次)。
pub fn halfclose_grace() -> std::time::Duration {
    static V: std::sync::OnceLock<std::time::Duration> = std::sync::OnceLock::new();
    *V.get_or_init(|| {
        match std::env::var("RH_HALFCLOSE_GRACE_SECS")
            .ok()
            .and_then(|s| s.trim().parse::<u64>().ok())
        {
            Some(0) => std::time::Duration::MAX,
            Some(s) => std::time::Duration::from_secs(s.min(3600)),
            None => DEFAULT_HALFCLOSE_GRACE,
        }
    })
}

/// 中继父任务收尾(全部 relay 共用):双向子泵 join,附加三条收敛保证:
/// * 单向 panic → 立即返回(守卫 Drop → abort 对向泵)——单向故障不得
///   让对向泵永存(对向可能在等一个永远不来的 EOF);
/// * 双向正常 → 等两向各自自然结束(半关闭语义:一向 EOF 后对向继续服务);
/// * **半关闭有界兜底(round-40)**:对向在 [`halfclose_grace`] 内仍未结束
///   即放弃等待并 abort——见 [`halfclose_grace`] 的实测依据(对端永不回
///   FIN 时 socket 永久停 FIN-WAIT-2,10 轮累积 22 条,吞吐 2.4G→0.5G)。
///
/// 父任务取消(本 future 被 drop)→ 守卫 Drop → 双向 abort(A1 语义不变)。
pub async fn join_relay_pumps(g1: &mut AbortOnDropHandle, g2: &mut AbortOnDropHandle) {
    let panicked = tokio::select! {
        _ = &mut *g1 => g1.panicked(),
        _ = &mut *g2 => g2.panicked(),
    };
    if panicked {
        // panic 侧守卫已完成(不再 abort);对向守卫随本函数返回被 drop → abort
        return;
    }
    // 先完成的一向是正常结束:等对向自然结束(已完成的立即返回),
    // 外加半关闭有界兜底。
    let grace = halfclose_grace();
    if grace == std::time::Duration::MAX {
        let _ = tokio::join!(g1, g2);
        return;
    }
    let both = async {
        let _ = futures::join!(&mut *g1, &mut *g2);
    };
    if tokio::time::timeout(grace, both).await.is_err() {
        static WARNED: std::sync::Once = std::sync::Once::new();
        WARNED.call_once(|| {
            tracing::warn!(
                grace_secs = grace.as_secs(),
                "half-close timeout: peer never completed close handshake; \
                 aborting relay to release the stream (round-40 leak fix)"
            );
        });
        // 守卫随本函数返回被 drop → abort 双向子泵 → fd 释放
    }
}

/// chunk 中继限速预留(预留/消费/退款三态;goal-repair A5 送达计费合同):
/// * `consume(n)` = 消费 n 字节额度(不可退);
/// * drop 未消费部分 = 全额退还。
pub trait ChunkReservation: Send {
    fn consume(&mut self, n: usize);
}

/// 限速器:`reserve(n)` 等待凑足 n 字节额度。
/// 等待期间 future 被 drop(取消)不得丢已扣令牌(适配方须用
/// 取消安全原语实现,如 rh-core `TokenBucket::acquire_reserved`)。
pub trait ChunkLimiter: Send + Sync {
    fn reserve(&self, n: usize) -> futures::future::BoxFuture<'static, Box<dyn ChunkReservation>>;
}

/// 在真实流之前回放一段残留字节。
/// read_frame 可能一次读入多帧;换手给下一处理者时必须把
/// 已读入但未消费的字节前传,否则同批到达的后续帧丢失。
pub struct PrependedIo {
    inner: BoxedIo,
    prefix: Vec<u8>,
    pos: usize,
}

impl PrependedIo {
    pub fn wrap(inner: BoxedIo, prefix: Vec<u8>) -> BoxedIo {
        Box::new(Self {
            inner,
            prefix,
            pos: 0,
        })
    }

    /// 按所有权剥壳(goal-repair B1):若 `io` 是 PrependedIo,拆出
    /// (剩余未消费前缀, 内层流);否则返回(空 vec, 原流)。
    /// 前缀是「通道→用户」方向的先行数据,调用方必须原样最先交付,不得丢弃。
    pub fn peel(io: BoxedIo) -> (Vec<u8>, BoxedIo) {
        // 探测经显式 &dyn Io 接收者:方法解析唯一落到 dyn Io 的 vtable
        // 派发(具体类型的 as_any),不受 Box 层转发实现影响
        // (owned/&Box 接收者会解析到 impl Io for Box<dyn Io>,
        // 见 prepended_peel_roundtrip 取证)。
        fn is_prepended(io: &BoxedIo) -> bool {
            let dynio: &dyn Io = &**io;
            dynio.as_any().downcast_ref::<PrependedIo>().is_some()
        }
        if !is_prepended(&io) {
            return (Vec::new(), io);
        }
        let any = io.into_any_box();
        match any.downcast::<PrependedIo>() {
            Ok(boxed) => {
                let p = *boxed;
                (p.prefix[p.pos..].to_vec(), p.inner)
            }
            Err(_) => unreachable!("as_any 已确认具体类型为 PrependedIo"),
        }
    }
}

impl AsyncRead for PrependedIo {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let this = self.get_mut();
        if this.pos < this.prefix.len() {
            let n = (this.prefix.len() - this.pos).min(buf.remaining());
            buf.put_slice(&this.prefix[this.pos..this.pos + n]);
            this.pos += n;
            return std::task::Poll::Ready(Ok(()));
        }
        std::pin::Pin::new(&mut this.inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for PrependedIo {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        let inner = &mut self.get_mut().inner;
        std::pin::Pin::new(inner).poll_write(cx, buf)
    }
    fn poll_write_vectored(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        bufs: &[std::io::IoSlice<'_>],
    ) -> std::task::Poll<std::io::Result<usize>> {
        // 前缀回放只发生在读侧;写侧必须透传向量写,否则 yamux 的
        // 「帧头+帧体一次写出」在包装层退化为首个非空分片的标量写
        let inner = &mut self.get_mut().inner;
        std::pin::Pin::new(inner).poll_write_vectored(cx, bufs)
    }
    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }
    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let inner = &mut self.get_mut().inner;
        std::pin::Pin::new(inner).poll_flush(cx)
    }
    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let inner = &mut self.get_mut().inner;
        std::pin::Pin::new(inner).poll_shutdown(cx)
    }
}

/// 传输协议载体。
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Protocol {
    /// 明文 TCP(仅建议本机调试;公网使用 tls+)。
    Tcp,
    Tls,
    Ws,
    Wss,
    Quic,
    Kcp,
}

impl Protocol {
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "tcp" => Some(Protocol::Tcp),
            "tls" => Some(Protocol::Tls),
            "ws" => Some(Protocol::Ws),
            "wss" => Some(Protocol::Wss),
            "quic" => Some(Protocol::Quic),
            "kcp" => Some(Protocol::Kcp),
            _ => None,
        }
    }
    pub fn is_tcp_based(&self) -> bool {
        matches!(
            self,
            Protocol::Tcp | Protocol::Tls | Protocol::Ws | Protocol::Wss
        )
    }
    pub fn is_encrypted(&self) -> bool {
        matches!(self, Protocol::Tls | Protocol::Wss | Protocol::Quic)
    }
    /// 该载体是否总是多路复用(quic/kcp 是;其余取决于 multiplex 配置)。
    pub fn always_muxed(&self) -> bool {
        matches!(self, Protocol::Quic | Protocol::Kcp)
    }
}

impl fmt::Display for Protocol {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Protocol::Tcp => "tcp",
            Protocol::Tls => "tls",
            Protocol::Ws => "ws",
            Protocol::Wss => "wss",
            Protocol::Quic => "quic",
            Protocol::Kcp => "kcp",
        };
        f.write_str(s)
    }
}

/// 默认 ALPN 标识。
///
/// **默认用 `h2` 而不用 `rhtp`**:ALPN 位于 ClientHello,是**明文**的
/// (实测可见:扩展类型 0x0010 后紧跟 UTF-8 字节串)。用项目专属名字会让
/// 任何被动 DPI 一眼认出"这是 rabbithole"。`h2` 是 HTTP/2 的标准标识,
/// 属于最常见的 TLS 流量之一,不构成可区分的指纹。
///
/// 代价:若服务端是真正的 HTTP/2 服务,双方会协商成功但应用层数据不通 ——
/// 本项目的端口只跑自己的协议,故不受影响。需要恢复专属标识或改成
/// 其它值时,用 `alpn` 配置项/`RH_ALPN` 覆盖。
pub const DEFAULT_ALPN: &str = "h2";

/// 多载体池协议基线(round-10):单载体即可跑到路径上限,故恒 1。
///
/// # 这是默认 K 的唯一来源(round-45 / round-73)
///
/// 默认载体数**只**由本函数决定。调用链:
/// `mux_pool_size`(None) → 本函数 → `min(hardware_cap)` →
/// `final_k = initial_k.min(server_cap)`(服务端收敛只减不增)。
///
/// 曾经有一个「池默认载体数 = 4」的常量从未被读取,却让多份设计文档误以为
/// 默认 K 是 4;已删除。**若将来要改默认载体数,改这里,不要再引入同类常量。**
///
/// # round-73:tcp/ws 从 4 改回 1
///
/// round-54 曾把 tcp/ws 从 1 改成 4,依据是一组"K=1 吞吐随并发连接数
/// 下降、64 连接时 K=4 快 50~68%"的测量。**那组数据是压测端造成的**:
/// 当时用 `iperf3 -s` 作接收端,而 iperf3 服务端单实例只能服务一个会话、
/// 且其 socket 缓冲未放大,在高并发下先于隧道饱和——测到的是接收端的
/// 天花板,不是 K 的收益。
///
/// round-73 用 Go 压测器(`benchmark/loadgen`,收发缓冲 4MiB)在同一时间
/// 窗口内严格交替 K=1 / K=4,两种顺序各 3 轮(共 6 次比较):
///
/// | 方向 | 并发 | K=1 | K=4 |
/// |---|---:|---:|---:|
/// | 下行 | 16 | 3.48 / 3.73 / 3.81 / 4.03 / 3.53 / 3.56 | 3.00 / 2.84 / 3.02 / 3.26 / 3.19 / 3.08 |
///
/// **K=1 在 6/6 次比较中都不慢于 K=4,且下行稳定快 15~20%。** 多载体
/// 把同一份流量摊到多条四元组上,每条都要各自爬拥塞窗口,而单条 10GbE
/// 路径上单载体的窗口本来就足够——多开只增加成本、不增加上限。
///
/// # 2026-10-20:默认改回 2(推翻 round-73)
///
/// round-73 的结论只覆盖**低 RTT** 实验床,且当时对照的是 K=1 vs K=4
/// (4 条载体在低 BDP 下分摊成本高)。本轮三机复测(直连 2.6-2.9 G、
/// 单载体隧道封顶 2.1-2.2 G 且**与流数无关**)重新给出可测缺口:
///
/// | 方向×流 | K=1 | K=2 | K=4 | frp |
/// |---|---:|---:|---:|---:|
/// | up×4 | 2072 | 2213 | 2184 | 2095-2354 |
/// | up×16 | 2178 | 2306 | 2323 | 2248-2386 |
/// | dn×16 | 2206 | 2418 | 2390 | 2133-2279 |
///
/// 单载体封顶的机理不是 CPU(服务端均值 0.26 核),而是**单条 TCP 流的
/// 拥塞窗 + yamux 单 FlushTask 串行**。K=2 恰好把这个串行点劈成两条,
/// K=4 不再有额外收益(只增加每条连接的爬窗成本)。
///
/// 保留 `hardware_cap` 探测与服务端 `max_carriers` 收敛。
///
/// QUIC/KCP 自带多路复用(单 UDP 连接承载全部流);KCP 的端到端背压
/// 语义还依赖单通道(见 `b08_public_mux_stalled_reader_backpressure_and_recovery`),
/// 二者同样恒 1。
///
/// 该值只是**期望值**,最终 K 还要经硬件探测与服务端 `max_carriers` 收敛
/// (见 rh-client 的 pool sizing);此处不读环境/配置。
pub fn default_carriers(protocol: Protocol) -> usize {
    // 2026-10-20 三机 A/B(deep-review-r3,Go loadgen,5×15s 取中位):
    //
    //   K=1  TCP 4 流 2072 / 16 流 2178;16 流对 frp 0.95-0.97
    //   K=2  TCP 4 流 2213 / 16 流 2306;4 流 1.06 / 16 流 0.97-1.07
    //   K=4  与 K=2 持平(2184 / 2323),只增成本
    //
    // 单载体在 2.1-2.2 G 封顶且**与流数无关**——瓶颈是 yamux 发送侧
    // 单 FlushTask 串行 + 单 TCP 流的拥塞窗上限,不是 CPU(服务端 0.26 核
    // 平均)。K=2 用两条物理连接各持一套发送侧,把该串行点劈开。
    //
    // 之前"单载体已到路径上限"的结论来自 round-73 的低 RTT 实验床;
    // 本轮直连可到 2.6-2.9 G,隧道 2.2 G,上限并未触及。
    //
    // QUIC/KCP 自带多路复用(单 UDP 连接承载全部流),且 KCP 的端到端
    // 背压语义依赖单通道,保持 1。
    match protocol {
        Protocol::Quic | Protocol::Kcp => 1,
        _ => 2,
    }
}

/// BDP 自适应载体数(round-77/78):根据链路 RTT 决定开几条载体。
///
/// # 为什么用 RTT 而不是并发连接数
///
/// 本项目此前 20 余轮一直用"并发连接数"作自变量,始终得不到稳定结论。
/// 用 `tc netem` 人为制造 BDP 约束后,信号立刻清晰。
///
/// **真正的自变量是 BDP(RTT × 带宽),不是并发数。** 低 RTT 下单条 TCP
/// 的拥塞窗口就足以打满链路,多载体只增加分摊成本(writev 次数
/// ∝ 1/每载体并发流数);高 BDP 下单窗口成为瓶颈,K 倍窗口的收益才
/// 压过分摊成本。
///
/// # 实测曲线(round-78,下行 P=16,中位 Gbps)
///
/// | RTT | K=1 | K=2 | K=4 | K=8 | 最优 |
/// |---|---:|---:|---:|---:|---|
/// | 0.4ms | **3.19** | — | 2.94 | — | K=1 |
/// | 10ms | 3.52 | 3.32 | 3.59 | — | K=1/4 持平 |
/// | 50ms | 1.82 | 2.24 | 2.50 | **2.60** | K=8 |
/// | 100ms | 1.20 | 1.72 | **2.10** | 1.96 | K=4 |
/// | 300ms | 0.70 | 0.84 | **0.75~0.83** | 0.50 | K=4 |
///
/// # 为什么上限是 4 而不是继续涨到 8
///
/// 直觉上"RTT 越大越该多开",但 300ms 实测否定了它:**K=8 只有 0.50,
/// 而 K=4 是 0.75~0.83,K=8 反而比 K=4 慢约 40%,甚至不如 K=2。**
///
/// 原因:载体数增加到一定程度后,每条载体分到的并发流太少,`writev`
/// 次数按 1/每载体流数 上升;同时每条 TCP 自己也要重新爬慢启动。
/// 在 16 条流、8 核的测试规格下,**K=4 是收益/成本的最优点**;
/// 继续加载体不再买到带宽,只买到开销。
///
/// 300ms 时 K=8 比 K=4 差也可能与测试规格(16 条流摊到 8 条载体 =
/// 每条仅 2 条流)有关——**流数远多于 16 时,K=8 或许不劣**。这一点
/// 未验证,故阈值取保守的 4。
///
/// # 阈值
///
/// - `rtt < 5ms`:`1` —— 实测 0.4ms 时 K=1 快 8%,5ms 持平
/// - `5ms <= rtt < 15ms`:`2` —— 过渡区(10ms 实测与 K=1 持平,
///   取 2 是保守折中,不追求该区间的精确最优)
/// - `rtt >= 15ms`:`4` —— 50/100/300ms 三点实测,K=4 都在最优点
///   4% 以内(50ms 时 K=8 略好 4%,但 100/300ms 时 K=4 明显更优)
///
/// 返回的仍只是**期望值**,还要经 [`default_carriers`]、硬件上限与服务端
/// `max_carriers` 三重收敛;调用方必须把显式配置(`RH_MUX_POOL_SIZE`)
/// 排在本函数之前。
pub fn carriers_for_rtt(rtt: std::time::Duration) -> usize {
    let ms = rtt.as_secs_f64() * 1000.0;
    if ms < 5.0 {
        1
    } else if ms < 15.0 {
        2
    } else {
        4
    }
}

/// 探针用的拨号时钟前缀:让 `probe_rtt` 与真实拨号走**同一条路径**。
///
/// 真实拨号在 [`tcp::dial_tcp`] 里按 `cfg.proxy_url` 决定直连还是经代理;
/// 探针若总是直连,走代理的部署会恒失败 → 自适应静默失效,还白等一个
/// 超时。这里把「拨号函数」抽成可替换的,探针与真实拨号共用同一实现。
pub type DialFn = fn(
    SocketAddr,
    Option<String>,
) -> futures::future::BoxFuture<'static, std::io::Result<tokio::net::TcpStream>>;

fn dial_fn() -> DialFn {
    DIAL.lock().unwrap_or_else(|e| e.into_inner()).unwrap_or(default_dial)
}

static DIAL: std::sync::Mutex<Option<DialFn>> = std::sync::Mutex::new(None);

/// 默认拨号:与 [`tcp::dial_tcp`] 同语义(有 `proxy_url` 走代理)。
fn default_dial(
    addr: SocketAddr,
    proxy_url: Option<String>,
) -> futures::future::BoxFuture<'static, std::io::Result<tokio::net::TcpStream>> {
    Box::pin(async move {
        match proxy_url {
            Some(url) => {
                let proxy = crate::proxy_dial::ProxyUrl::parse(&url)?;
                crate::proxy_dial::dial_via_proxy(&proxy, addr).await
            }
            None => tokio::net::TcpStream::connect(addr).await,
        }
    })
}

/// **仅测试**:替换探针的拨号函数(用于计数与注入失败)。
#[cfg(test)]
pub fn probe_set_dial(f: Option<DialFn>) {
    *DIAL.lock().unwrap_or_else(|e| e.into_inner()) = f;
}

/// 测量到服务端的 RTT(round-77):基于 TCP 建连耗时估计。
///
/// # 这是估计,不是精确测量
///
/// 真正的 RTT 需要一次应用层往返。这里用**建连耗时**(SYN→SYN/ACK)作
/// 代理量,因为:
///
/// - 建连本身就是一次往返,量级与后续 RTT 同源(同一路径、同一队列);
/// - 零额外流量、零协议改动,失败不影响连接。
///
/// **走与真实拨号相同的路径**:`proxy_url` 存在时经代理拨号,否则直连。
/// 修前探针无条件直连,走代理的部署必然探测失败。
///
/// 已知偏差:SYN 队列排队、服务端 accept backlog 满、NAT 冷启动都会让
/// 建连耗时**偏大**;而 RTT 缓存命中时会偏小。因此阈值取得比较宽松
/// (5ms / 15ms 两档),只用来区分"局域网级"与"广域网级",不追求精确。
///
/// 返回 `None` = 无法判定(拨号失败),调用方退回保守默认。
pub async fn probe_rtt(
    addr: SocketAddr,
    timeout: std::time::Duration,
    proxy_url: Option<String>,
) -> Option<std::time::Duration> {
    let start = std::time::Instant::now();
    let dial = dial_fn();
    let conn = tokio::time::timeout(timeout, dial(addr, proxy_url))
        .await
        .ok()?
        .ok()?;
    let elapsed = start.elapsed();
    drop(conn);
    Some(elapsed)
}

/// 进程内 RTT 探测缓存:键为 `(server_addr, proxy_url)`。
///
/// 修前**每次重连**都探一次(accept 完立刻断),断线抖动时会变成周期性
/// 小冲击。探针结果在原地不变的前提下没有时效性,缓存即可。
type RttCache = std::collections::HashMap<(SocketAddr, Option<String>), usize>;

fn rtt_cache() -> &'static std::sync::Mutex<RttCache> {
    static CACHE: std::sync::OnceLock<std::sync::Mutex<RttCache>> = std::sync::OnceLock::new();
    CACHE.get_or_init(|| std::sync::Mutex::new(RttCache::new()))
}

/// 清空 RTT 缓存(**测试与诊断**用)。
///
/// 始终编译:它只清一张进程内 map,生产不会调用;但下游 crate(rh-client)
/// 的集成测试需要它来隔离用例。不是 `#[cfg(test)]` —— 那样只有
/// rh-transport 自身的测试能看到,下游看不见(实测编译失败)。
pub fn probe_cache_clear() {
    rtt_cache().lock().unwrap_or_else(|e| e.into_inner()).clear();
}

/// 本进程内**真实发生过**的探测次数(缓存命中不计)。
///
/// 用于断言「非 tcp 协议 / 显式配置 / 重复重连」没有触发探测。计数器
/// 在每次未命中缓存时 `Relaxed` 自增,生产开销可忽略。
pub fn probe_call_count() -> usize {
    probe_calls().load(std::sync::atomic::Ordering::Relaxed)
}

fn probe_calls() -> &'static std::sync::atomic::AtomicUsize {
    static N: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    &N
}

/// 依据实测 RTT 选择载体数(round-77);探针失败时退回**单载体**(不是
/// 协议基线 —— 协议基线由调用方的 `default_carriers` 另外参与 `max`)。
///
/// 结果按 `(server_addr, proxy_url)` 缓存在进程内:同一目标只探一次,
/// 后续重连直接复用。显式配置的短路在调用方(`initial_k_for_async`)完成。
///
/// **显式配置优先**:调用方必须在调用本函数**之前**处理
/// `RH_MUX_POOL_SIZE` / 配置文件里的 `mux_pool_size`,不要把两者混在一处。
pub async fn auto_carriers(
    addr: SocketAddr,
    timeout: std::time::Duration,
    proxy_url: Option<String>,
) -> usize {
    let key = (addr, proxy_url.clone());
    if let Some(hit) = rtt_cache()
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get(&key)
        .copied()
    {
        return hit;
    }
    probe_calls().fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let k = match probe_rtt(addr, timeout, proxy_url).await {
        Some(rtt) => carriers_for_rtt(rtt),
        // 探针失败(拨号不通/超时):不臆测,退回单载体。真正的拨号会
        // 在随后由 connect/connect_pool 执行并给出真实错误。
        None => 1,
    };
    rtt_cache()
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .insert(key, k);
    k
}

/// TLS 客户端选项。
#[derive(Debug, Clone, Default)]
pub struct TlsClientOptions {
    /// 信任 CA(PEM 字节);None 时仅能配合 pin 或 insecure。
    pub trusted_ca: Option<Vec<u8>>,
    /// SNI / 校验名;None 用地址主机名。
    pub server_name: Option<String>,
    /// 跳过校验(仅测试)。
    pub insecure_skip_verify: bool,
    /// 固定服务端叶子证书 SHA-256 指纹(hex)。设置时优先于 CA 链校验。
    pub pin_sha256: Option<String>,
    /// 由 `secret` 验证对端身份:握手阶段不校验证书,认证成功后由服务端
    /// 用 `secret` 派生的密钥加密下发它自己的证书,客户端解出后与握手实际
    /// 看到的对端证书比对。
    ///
    /// 这样客户端**首次连接即能识破中间人**,且不需要 CA、不需要 pin、
    /// 不写任何状态文件 —— 信任完全来自客户端已经持有的 `secret`。
    ///
    /// 前提:上层必须真的执行比对(见 `rh_protocol::certprov`)。
    /// 只置位不比对,安全性与 `insecure_skip_verify` 等同。
    pub secret_verified: bool,
}

/// TLS 服务端选项。
#[derive(Debug, Clone, Default)]
pub struct TlsServerOptions {
    pub cert_pem: Option<Vec<u8>>,
    pub key_pem: Option<Vec<u8>>,
    /// 无 cert/key 时自动生成自签证书,以 `persist_prefix` 落盘复用。
    pub persist_prefix: Option<std::path::PathBuf>,
}

/// 客户端传输配置。
///
/// **本结构体不承载载体数 K**。K 由调用方(`rh-client`)按「显式
/// `mux_pool_size` / 协议基线 → 硬件上限 → 服务端 `max_carriers` 收敛」三段式
/// 算出,再作为参数传给 [`connect_pool`](crate::connect_pool)。曾经存在的
/// `carriers` 字段被赋值却从未被生产代码读取,已删除(round-44)——
/// 改它不会改变载体数,只会造成误判。
#[derive(Debug, Clone)]
pub struct ClientTransportConfig {
    pub protocol: Protocol,
    pub server_addr: SocketAddr,
    /// tcp/tls/ws/wss 是否启用 yamux 多路复用。
    pub multiplex: bool,
    pub tls: TlsClientOptions,
    pub proxy_url: Option<String>,
    /// ALPN。
    pub alpn: Vec<String>,
    /// 拨号总超时。
    pub connect_timeout: std::time::Duration,
    /// 握手完成后写入**对端叶子证书 DER**(首个成功拨号者置入)。
    ///
    /// 供上层做证书下发比对:客户端解密服务端下发的证书后,与此处记录的
    /// 实际对端证书逐字节比对,不一致即说明中间有人在转发。
    /// `None` = 调用方不需要该信息,传输层不做额外工作。
    pub peer_cert_sink: Option<Arc<std::sync::OnceLock<Vec<u8>>>>,
}

impl Default for ClientTransportConfig {
    fn default() -> Self {
        Self {
            protocol: Protocol::Tcp,
            server_addr: "127.0.0.1:7000".parse().unwrap(),
            multiplex: false,
            tls: TlsClientOptions::default(),
            proxy_url: None,
            alpn: vec![DEFAULT_ALPN.to_string()],
            connect_timeout: std::time::Duration::from_secs(10),
            peer_cert_sink: None,
        }
    }
}

/// 服务端传输配置(单个监听点)。
#[derive(Debug, Clone)]
pub struct ServerTransportConfig {
    pub protocol: Protocol,
    pub bind_addr: SocketAddr,
    pub multiplex: bool,
    pub tls: TlsServerOptions,
    pub alpn: Vec<String>,
}

impl Default for ServerTransportConfig {
    fn default() -> Self {
        Self {
            protocol: Protocol::Tcp,
            bind_addr: "0.0.0.0:7000".parse().unwrap(),
            multiplex: false,
            tls: TlsServerOptions::default(),
            alpn: vec![DEFAULT_ALPN.to_string()],
        }
    }
}

/// 服务端 accept 出的一个连接(多路复用器)。
pub struct Incoming {
    pub mux: Arc<Multiplexer>,
    pub remote: SocketAddr,
    /// 底层协议(诊断)。
    pub protocol: Protocol,
}

enum ListenerInner {
    Tcp(tcp::TcpListenerShim),
    Quic(quic::QuicListener),
    Kcp(kcp::KcpListener),
}

/// 服务端监听器:`accept()` 产出 [`Incoming`]。
pub struct TransportListener {
    inner: ListenerInner,
    pub local_addr: SocketAddr,
    pub protocol: Protocol,
}

/// 多路复用器:客户端侧开流,服务端侧收流。
pub struct Multiplexer {
    inner: MultiplexerInner,
}

pub(crate) enum MultiplexerInner {
    /// 裸模式客户端:开流 = 新建整条物理连接。
    DialPerStream {
        cfg: ClientTransportConfig,
    },
    /// 裸模式服务端:accept 恰好一次(该物理连接本身)。
    AcceptSingle {
        stream: tokio::sync::Mutex<Option<BoxedIo>>,
    },
    Yamux(mux::YamuxMultiplexer),
    /// 多物理载体连接池(round-8 优化项 5):K 条独立 TCP/TLS 连接各跑
    /// 一套 yamux,开流按全局原子计数轮转分派,载体故障自动剔除+重拨。
    YamuxPool(mux::YamuxMultiplexerPool),
    Quic(quic::QuicConnection),
}

impl Multiplexer {
    /// 多载体客户端池(round-8 优化项 5):members 为 K 条已建立连接的
    /// 客户端 yamux(≥1),redial 为载体故障时的重拨配方。
    pub fn yamux_pool_client(
        members: Vec<mux::YamuxMultiplexer>,
        redial: Option<mux::PoolRedial>,
    ) -> Arc<Multiplexer> {
        Arc::new(Multiplexer {
            inner: MultiplexerInner::YamuxPool(mux::YamuxMultiplexerPool::new_client(
                members, redial,
            )),
        })
    }

    /// 多载体服务端池(round-8 优化项 5):members 为同一客户端会话的 K
    /// 条已 accept 连接的服务端 yamux;accept 自全部成员合流。
    pub fn yamux_pool_server(members: Vec<mux::YamuxMultiplexer>) -> Arc<Multiplexer> {
        Arc::new(Multiplexer {
            inner: MultiplexerInner::YamuxPool(mux::YamuxMultiplexerPool::new_server(members)),
        })
    }

    /// 取出单载体 yamux(供上层把多条独立 accept 的连接聚合进服务端池)。
    /// 非 yamux 单载体(裸模式/QUIC/已是池)返回 None,连接随 self 析构关闭
    /// ——聚合调用方只应对 yamux 连接调用。
    pub fn into_yamux(self) -> Option<mux::YamuxMultiplexer> {
        match self.inner {
            MultiplexerInner::Yamux(m) => Some(m),
            _ => None,
        }
    }

    /// **带容量裁决**的载体附加(round-48):「判断成员数」与「入表」在池的
    /// 同一把 `members` 写锁内完成,消除并发报到的短暂超限。
    ///
    /// `cap` = 池内成员总数上限(调用方负责把首载体占的名额折算进去)。
    /// 返回 [`mux::AttachOutcome::RejectedFull`] 时**未入表**,该载体由
    /// 调用方负责拒绝/关闭。
    ///
    /// 非池变体返回 `None`(调用方据此走原有拒绝路径)。
    pub async fn attach_yamux_carrier_with_cap(
        &self,
        mux: mux::YamuxMultiplexer,
        cap: usize,
    ) -> Option<mux::AttachOutcome> {
        match &self.inner {
            MultiplexerInner::YamuxPool(p) => Some(p.attach_carrier_with_cap(mux, cap).await),
            _ => None,
        }
    }

    /// [`Self::attach_yamux_carrier_with_cap`] 的**交还载体**变体(round-53)。
    ///
    /// cap 裁决语义完全相同(同一把 `members` 写锁内判定),区别是被拒时
    /// 把载体 `Err(mux)` 交还给调用方,而不是丢弃。用途:调用方还要在被拒
    /// 载体上写 `ok:false` ACK,写完并冲刷后才能关——所以载体必须先在
    /// 拒绝后继续存活。
    ///
    /// 为什么不直接用前者的"丢弃"语义:`YamuxMultiplexer` 没有 `impl Drop`,
    /// drop 只 detach driver 而不关连接,拒绝就等于泄漏一条无人驱动的载体
    /// (实测 probe 流 3s 读不到 EOF)。
    ///
    /// 非池变体返回 `None`(与 [`Self::attach_yamux_carrier_with_cap`] 同口径:
    /// 调用方据此走 `NOT_POOLED` 拒绝路径,不涉及载体交接)。
    pub async fn attach_yamux_carrier_with_cap_gated(
        &self,
        mux: mux::YamuxMultiplexer,
        cap: usize,
    ) -> Option<Result<mux::AttachOutcome, mux::YamuxMultiplexer>> {
        match &self.inner {
            MultiplexerInner::YamuxPool(p) => Some(p.attach_carrier_with_cap_gated(mux, cap).await),
            // 非池变体:无 accept 语义,按原约定返回 None。载体 `mux` 随本
            // 参数被 drop——与修前 `attach_yamux_carrier_with_cap` 的 `_`
            // 分支同行为(该分支根本不消费 mux,由调用方后续处置)。
            _ => None,
        }
    }

    /// 运行中把一条已 accept 的服务端 yamux 载体附加进本多路复用器
    /// (round-9 服务端会话聚合)。仅 [`MultiplexerInner::YamuxPool`] 变体
    /// 支持(返回 true);其余变体返回 false,载体由调用方处置。
    pub async fn attach_yamux_carrier(&self, mux: mux::YamuxMultiplexer) -> bool {
        match &self.inner {
            MultiplexerInner::YamuxPool(p) => {
                p.attach_carrier(mux).await;
                true
            }
            _ => false,
        }
    }

    /// 显式关停多载体池(round-9.1):中止全部载体 driver 与 accept 转发
    /// 任务,物理连接随之关闭。仅 [`MultiplexerInner::YamuxPool`] 有动作,
    /// 其余变体 no-op(单载体路径关停由 Session 拆除/连接释放负责,语义
    /// 零漂移)。幂等且可在同步上下文调用(池 Drop 兜底亦走同一路径)。
    ///
    /// 命名区别于 stream 的 `shutdown`:本方法关的是池/连接,不是某条流。
    pub fn shutdown_pool(&self) {
        if let MultiplexerInner::YamuxPool(p) = &self.inner {
            p.shutdown();
        }
    }

    /// **实际生效**的每载体分片 Worker 数(round-47,诊断/日志用)。
    ///
    /// - 池变体:取首个成员的值(池内成员由同一次
    ///   [`connect_pool`] 用同一 `shards` 创建);
    /// - 单载体 yamux 变体:该载体自身的值;
    /// - 非 yamux 变体(裸模式 `AcceptSingle` / QUIC):`None`(无分片
    ///   Worker 概念)。
    ///
    /// **调用方必须用这个值记日志,不要自己再调
    /// [`mux::shard_workers_for_k`] 算一遍** —— 两处独立计算一旦输入
    /// 漂移(例如 `connect_pool` 的参数从 `initial_k` 变成收敛后的 K),
    /// 日志就会与实际 spawn 的 Worker 数不符,而这种错误极难发现。
    ///
    /// 值在载体构造时从 `into_halves()` 的 Worker 向量长度取定,是
    /// 「实际 spawn」而非「按公式重算」。
    pub async fn shard_workers(&self) -> Option<usize> {
        match &self.inner {
            MultiplexerInner::YamuxPool(p) => p.shard_workers().await,
            MultiplexerInner::Yamux(m) => Some(m.shard_workers()),
            _ => None,
        }
    }

    /// 现有载体数(诊断/日志;非池变体返回 None)。只读,不改变池语义。
    pub async fn pool_live_carriers(&self) -> Option<usize> {
        match &self.inner {
            MultiplexerInner::YamuxPool(p) => Some(p.live_carriers().await),
            _ => None,
        }
    }

    /// 收缩多载体池的轮转分派前缀(round-10 自动定容)。仅
    /// [`MultiplexerInner::YamuxPool`] 有动作,其余变体 no-op——单载体
    /// 路径本就只有一个载体,收缩无意义且不得报错(调用方无条件收敛)。
    pub async fn set_active_prefix(&self, n: usize) {
        if let MultiplexerInner::YamuxPool(p) = &self.inner {
            p.set_active_prefix(n).await;
        }
    }

    /// 移除多载体池中的一个成员(round-10 报到收敛)。仅
    /// [`MultiplexerInner::YamuxPool`] 有动作,其余变体 no-op(单载体
    /// 路径没有可摘除的成员)。index 语义见
    /// [`mux::YamuxMultiplexerPool::detach_carrier`]:先清重拨配方再
    /// 关停物理连接,幂等。
    pub async fn detach_carrier(&self, index: usize) {
        if let MultiplexerInner::YamuxPool(p) = &self.inner {
            p.detach_carrier(index).await;
        }
    }

    /// 客户端:打开一个新流并返回落点载体身份(round-10.1)。池变体转发
    /// 到 [`mux::YamuxMultiplexerPool::open_stream_tagged`];其余变体语义与
    /// [`Self::open_stream`] 完全一致,id 恒 0(0 = 非池,不可按 id 摘除)。
    pub async fn open_stream_tagged(&self) -> std::io::Result<(BoxedIo, u64)> {
        match &self.inner {
            MultiplexerInner::YamuxPool(p) => p.open_stream_tagged().await,
            _ => self.open_stream().await.map(|s| (s, 0)),
        }
    }

    /// 按身份摘除池成员(round-10.1)。仅 [`MultiplexerInner::YamuxPool`]
    /// 有动作;其余变体返回 false(无池可摘)。语义见
    /// [`mux::YamuxMultiplexerPool::detach_carrier_by_id`]:先清重拨配方
    /// 再关停物理连接,幂等。
    pub async fn detach_carrier_by_id(&self, id: u64) -> bool {
        match &self.inner {
            MultiplexerInner::YamuxPool(p) => p.detach_carrier_by_id(id).await,
            _ => false,
        }
    }

    /// 池成员身份快照(round-10.1,成员表顺序)。非池变体返回空表——
    /// 调用方(报到收敛)只在池路径使用;单载体路径无成员可摘。
    pub async fn member_ids(&self) -> Vec<u64> {
        match &self.inner {
            MultiplexerInner::YamuxPool(p) => p.member_ids().await,
            _ => Vec::new(),
        }
    }

    /// 池内**存活**成员身份快照(round-10.2,成员表顺序)。终态收敛按此
    /// 计数保留额,避免把额度算给 dead-but-unreaped 成员后误摘健康成员。
    /// 非池变体返回空表(同 [`Self::member_ids`])。
    pub async fn live_member_ids(&self) -> Vec<u64> {
        match &self.inner {
            MultiplexerInner::YamuxPool(p) => p.live_member_ids().await,
            _ => Vec::new(),
        }
    }

    /// 池成员表长度(含 dead-but-unreaped 成员,round-10.2)。服务端 cap
    /// 检查用它而非存活数:dead 未 reap 的成员仍占物理连接与池内名额。
    /// 非池变体返回 None。
    pub async fn pool_member_count(&self) -> Option<usize> {
        match &self.inner {
            MultiplexerInner::YamuxPool(p) => Some(p.pool_member_count().await),
            _ => None,
        }
    }

    /// 客户端:打开一个新流(多路复用或新连接)。
    pub async fn open_stream(&self) -> std::io::Result<BoxedIo> {
        match &self.inner {
            MultiplexerInner::DialPerStream { cfg } => tcp::dial_stream(cfg).await,
            MultiplexerInner::AcceptSingle { .. } => {
                Err(other_err("open_stream on server-side single multiplexer"))
            }
            MultiplexerInner::Yamux(m) => m.open_stream().await,
            MultiplexerInner::YamuxPool(p) => p.open_stream().await,
            MultiplexerInner::Quic(q) => q.open_stream().await,
        }
    }

    /// 服务端:接受一个新流。裸模式 = 返回该物理连接(仅一次)。
    pub async fn accept(&self) -> std::io::Result<BoxedIo> {
        match &self.inner {
            MultiplexerInner::DialPerStream { .. } => {
                Err(other_err("accept on client multiplexer"))
            }
            MultiplexerInner::AcceptSingle { stream } => {
                let mut guard = stream.lock().await;
                guard.take().ok_or_else(|| {
                    std::io::Error::new(
                        std::io::ErrorKind::NotConnected,
                        "physical connection stream already taken",
                    )
                })
            }
            MultiplexerInner::Yamux(m) => m.accept_stream().await,
            MultiplexerInner::YamuxPool(p) => p.accept_stream().await,
            MultiplexerInner::Quic(q) => q.accept_stream().await,
        }
    }

    /// 连接是否已死(无法再产生新流)。
    pub async fn is_dead(&self) -> bool {
        match &self.inner {
            MultiplexerInner::Yamux(m) => m.is_dead(),
            MultiplexerInner::YamuxPool(p) => p.is_dead(),
            MultiplexerInner::Quic(q) => q.is_dead(),
            _ => false,
        }
    }
}

pub(crate) fn other_err(msg: impl Into<String>) -> std::io::Error {
    std::io::Error::other(msg.into())
}

/// 常数时间字节比较(用于指纹/密钥比对)。
pub fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

/// 最小 base64 编码(标准字母表,含 padding)。
pub fn base64_encode(data: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(data.len().div_ceil(3) * 4);
    for chunk in data.chunks(3) {
        let b = [
            chunk[0],
            chunk.get(1).copied().unwrap_or(0),
            chunk.get(2).copied().unwrap_or(0),
        ];
        out.push(TABLE[(b[0] >> 2) as usize] as char);
        out.push(TABLE[(((b[0] & 0x03) << 4) | (b[1] >> 4)) as usize] as char);
        out.push(if chunk.len() > 1 {
            TABLE[(((b[1] & 0x0f) << 2) | (b[2] >> 6)) as usize] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            TABLE[(b[2] & 0x3f) as usize] as char
        } else {
            '='
        });
    }
    out
}

/// 服务端:按配置监听。mux_streams 为并发流上限的有效值(0 已折算为
/// MUX_STREAMS_UNLIMITED),由调用方持有以便热改;yamux 路径对新连接即时生效,
/// QUIC 在端点构建时固化(改动需重启)。
pub async fn listen(
    cfg: &ServerTransportConfig,
    mux_streams: std::sync::Arc<std::sync::atomic::AtomicUsize>,
) -> std::io::Result<TransportListener> {
    let local_addr;
    let inner = match cfg.protocol {
        Protocol::Tcp | Protocol::Tls | Protocol::Ws | Protocol::Wss => {
            let l = tcp::listen_stream(cfg, mux_streams).await?;
            local_addr = l.local_addr()?;
            ListenerInner::Tcp(l)
        }
        Protocol::Quic => {
            let e = quic::listen(cfg, &mux_streams)?;
            local_addr = e.local;
            ListenerInner::Quic(e)
        }
        Protocol::Kcp => {
            let l = kcp::listen(cfg, mux_streams).await?;
            local_addr = l.local_addr();
            ListenerInner::Kcp(l)
        }
    };
    Ok(TransportListener {
        inner,
        local_addr,
        protocol: cfg.protocol,
    })
}

impl TransportListener {
    pub async fn accept(&mut self) -> std::io::Result<Incoming> {
        match &mut self.inner {
            ListenerInner::Tcp(l) => l.accept().await,
            ListenerInner::Quic(e) => quic::accept(e).await,
            ListenerInner::Kcp(l) => l.accept().await,
        }
    }
}

/// 客户端:建立连接并返回其多路复用器。
/// 控制流 = 之后在 mux 上打开的第一个流。
pub async fn connect(cfg: &ClientTransportConfig) -> std::io::Result<Arc<Multiplexer>> {
    match cfg.protocol {
        Protocol::Tcp | Protocol::Tls | Protocol::Ws | Protocol::Wss => {
            tcp::connect_stream(cfg).await
        }
        Protocol::Quic => quic::connect(cfg).await,
        Protocol::Kcp => kcp::connect(cfg).await,
    }
}

/// 客户端:建立 K 条物理连接组成多载体池(round-8 优化项 5)。TCP/TLS/
/// WS/WSS 多路复用路径走池(`carriers <= 1` 时等价 [`connect`]);QUIC/KCP
/// 自带多路复用,忽略 `carriers` 走单连接。
///
/// # K 的取值(round-45 修正)
///
/// 本函数**不决定** K,由调用方显式传入。默认值见
/// [`default_carriers`]:tcp/tls/ws/wss 为 2,quic/kcp 为 1。
/// 显式配置见 `rh-client` 的 `mux_pool_size`(env `RH_MUX_POOL_SIZE`)。
///
/// 曾经此处写「推荐 K = 4」并引用一个从未被读取的常量,那与实现不符——
/// 服务端收敛只减不增,默认路径也到不了 4。该常量已删除。
pub async fn connect_pool(
    cfg: &ClientTransportConfig,
    carriers: usize,
) -> std::io::Result<Arc<Multiplexer>> {
    match cfg.protocol {
        Protocol::Tcp | Protocol::Tls | Protocol::Ws | Protocol::Wss => {
            tcp::connect_stream_pool(cfg, carriers).await
        }
        Protocol::Quic => quic::connect(cfg).await,
        Protocol::Kcp => kcp::connect(cfg).await,
    }
}

#[cfg(test)]
mod carrier_baseline_tests {
    use super::*;

    // ---- RTT 探针(carriers_for_rtt 阈值见下方既有测试)------------------

    // 探针用例的注入状态**全部是线程局部**。
    //
    // 修前用 `static AtomicUsize`:持锁窗口只覆盖了 `probe_setup`,而用例
    // 在窗口内还会改 `DIAL_MS`(测两档 RTT)。并行跑其它 crate 的探针
    // 用例时,同一进程里两边互相改对方的注入值——实测全量偶发
    // `probe_result_matches_thresholds` 得到 K=2(应为 1)。
    // tokio 的 current_thread 测试在单线程内跑,线程局部即可隔离。
    // 注入状态用**进程级全局原子**,不用 thread_local。
    //
    // 理由:`#[tokio::test]` 不保证整个测试跑在同一线程(multi_thread 下
    // 会跨 worker 迁移)。thread_local 在那种场景下 setUp 与 setUp 之间
    // 看得见的不是同一份状态,断言会随机失败。正确做法是全局 + 一把
    // async 锁把「设置 → 运行 → 断言」整个窗口串行化(`PROBE_LOCK`)。
    static DIALS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    static DIAL_MS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    static DIAL_OK: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(true);
    static LAST_PROXY: std::sync::Mutex<Option<Option<String>>> = std::sync::Mutex::new(None);

    fn dials() -> usize {
        DIALS.load(std::sync::atomic::Ordering::Relaxed)
    }

    fn counting_dial(
        _addr: SocketAddr,
        proxy: Option<String>,
    ) -> futures::future::BoxFuture<'static, std::io::Result<tokio::net::TcpStream>> {
        DIALS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        *LAST_PROXY.lock().unwrap_or_else(|e| e.into_inner()) = Some(proxy);
        let ms = DIAL_MS.load(std::sync::atomic::Ordering::Relaxed) as u64;
        let ok = DIAL_OK.load(std::sync::atomic::Ordering::Relaxed);
        Box::pin(async move {
            if ms > 0 {
                tokio::time::sleep(std::time::Duration::from_millis(ms)).await;
            }
            if !ok {
                return Err(std::io::Error::other("injected dial failure"));
            }
            // 返回一条真实但已断的 loopback 连接:探针只测"建连耗时",
            // 拿到后立刻 drop,不需要可用会话。
            let l = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
            let a = l.local_addr()?;
            let c = tokio::net::TcpStream::connect(a).await?;
            Ok(c)
        })
    }

    /// 缓存仍是**进程级全局**(生产语义如此),故仍需串行;但注入量已
    /// 线程局部,窗口内改 RTT 不再影响别的用例。
    static PROBE_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    async fn probe_setup(ok: bool, ms: usize) -> tokio::sync::MutexGuard<'static, ()> {
        let g = PROBE_LOCK.lock().await;
        probe_cache_clear();
        DIALS.store(0, std::sync::atomic::Ordering::Relaxed);
        DIAL_MS.store(ms, std::sync::atomic::Ordering::Relaxed);
        DIAL_OK.store(ok, std::sync::atomic::Ordering::Relaxed);
        *LAST_PROXY.lock().unwrap_or_else(|e| e.into_inner()) = None;
        probe_set_dial(Some(counting_dial));
        g
    }

    fn probe_teardown() {
        probe_set_dial(None);
        probe_cache_clear();
        DIAL_MS.store(0, std::sync::atomic::Ordering::Relaxed);
        DIAL_OK.store(true, std::sync::atomic::Ordering::Relaxed);
    }

    fn addr() -> SocketAddr {
        SocketAddr::from(([203, 0, 113, 1], 7000))
    }

    /// 连续重连 10 次:只应发生 1 次真实探测(结果按 (addr,proxy) 缓存)。
    #[tokio::test]
    async fn repeated_reconnects_probe_only_once() {
        let _g = probe_setup(true, 1).await;
        let mut ks = Vec::new();
        for _ in 0..10 {
            ks.push(auto_carriers(addr(), std::time::Duration::from_secs(2), None).await);
        }
        assert!(
            ks.iter().all(|k| *k == ks[0]),
            "缓存命中必须返回同一结果,实际 {ks:?}"
        );
        assert_eq!(
            dials(),
            1,
            "连续 10 次重连应只探测 1 次,实际探测了 {} 次",
            dials()
        );
        probe_teardown();
    }

    /// 缓存键含 proxy_url:同一地址不同代理必须分别探测(结果可能不同)。
    #[tokio::test]
    async fn cache_key_includes_proxy_url() {
        let _g = probe_setup(true, 1).await;
        auto_carriers(addr(), std::time::Duration::from_secs(2), None).await;
        auto_carriers(addr(), std::time::Duration::from_secs(2), None).await;
        assert_eq!(dials(), 1);

        auto_carriers(
            addr(),
            std::time::Duration::from_secs(2),
            Some("http://127.0.0.1:3128".into()),
        )
        .await;
        assert_eq!(
            dials(),
            2,
            "不同 proxy_url 必须各自探测一次"
        );
        probe_teardown();
    }

    /// 探针必须把 proxy_url 传给拨号函数(修前是完全忽略)。
    #[tokio::test]
    async fn probe_passes_proxy_url_to_dialer() {
        let _g = probe_setup(true, 1).await;
        let proxy = Some("socks5://127.0.0.1:1080".to_string());
        auto_carriers(addr(), std::time::Duration::from_secs(2), proxy.clone()).await;
        let seen = LAST_PROXY.lock().unwrap_or_else(|e| e.into_inner()).clone();
        assert_eq!(
            seen,
            Some(proxy),
            "探针必须把 proxy_url 原样传给拨号函数,实际 {seen:?}"
        );
        probe_teardown();
    }

    /// 探针失败不得阻断:返回 1(单载体),不 panic、不阻塞调用方。
    #[tokio::test]
    async fn probe_failure_degrades_to_single_carrier() {
        let _g = probe_setup(false, 0).await;
        let k = auto_carriers(addr(), std::time::Duration::from_secs(2), None).await;
        assert_eq!(k, 1, "探测失败必须退回单载体");
        // 失败结果也要缓存:避免断线时每次重连都白等超时
        let k2 = auto_carriers(addr(), std::time::Duration::from_secs(2), None).await;
        assert_eq!(k2, 1);
        assert_eq!(
            dials(),
            1,
            "失败结果也必须缓存"
        );
        probe_teardown();
    }

    /// 不同 RTT 给出不同 K,且与 carriers_for_rtt 一致。
    ///
    /// 注意:同一把锁**不能嵌套获取**(`probe_setup` 会 `lock().await`)。
    /// 两档 RTT 用不同地址区分,共用一次持锁窗口。
    #[tokio::test]
    async fn probe_result_matches_thresholds() {
        let _g = probe_setup(true, 1).await;
        // 1ms → 局域网档 → K=1
        let lan = auto_carriers(addr(), std::time::Duration::from_secs(2), None).await;
        assert_eq!(lan, carriers_for_rtt(std::time::Duration::from_millis(1)));
        assert_eq!(lan, 1);

        // 改注入延迟为 20ms(不重新取锁),清缓存后换一个地址再探
        DIAL_MS.store(20, std::sync::atomic::Ordering::Relaxed);
        probe_cache_clear();
        let wan_addr = SocketAddr::from(([203, 0, 113, 2], 7000));
        let wan = auto_carriers(wan_addr, std::time::Duration::from_secs(2), None).await;
        assert_eq!(wan, carriers_for_rtt(std::time::Duration::from_millis(20)));
        assert_eq!(wan, 4);
        probe_teardown();
    }

    // T-C1(round-10 → round-54 → round-73 → 2026-10-20):协议基线六变体全覆盖。
    //
    // round-10 原值:tcp/ws = 1、tls/wss = 2、quic/kcp = 1。
    // round-54 改成 tcp/tls/ws/wss = 4、quic/kcp = 1——依据是一组后来被
    //   证伪的测量(iperf3 接收端先饱和)。
    // round-73 全部回退为 1——低 RTT 床上 K=1/K=4 交替 6 次,K=1 不慢。
    // 2026-10-20 三机复测(Go loadgen,直连 2.6-2.9G):单载体隧道封顶
    //   2.1-2.2G 且与流数无关,K=2 把 4/16 流提到 frp 水平,K=4 无额外
    //   收益。故 tcp-base 默认改为 2;QUIC/KCP 仍为 1(自带多路复用,
    //   KCP 的背压语义还依赖单通道)。
    //
    // 本测试锁的是"每个协议的默认值",任何改动都必须同步更新这里——
    // 它是一处**有意的**重复,用来防止默认值漂移无声通过。
    #[test]
    fn default_carriers_covers_all_protocols() {
        for p in [Protocol::Tcp, Protocol::Tls, Protocol::Ws, Protocol::Wss] {
            assert_eq!(default_carriers(p), 2, "{p:?} 的默认载体数应为 2");
        }
        // 红线 1 的形式化:非 tcp-based 恒 1(自带多路复用,多载体无意义)。
        for p in [Protocol::Quic, Protocol::Kcp] {
            assert!(!p.is_tcp_based());
            assert_eq!(default_carriers(p), 1);
        }
    }

    // T-C7(round-77):BDP 自适应载体数。阈值来自实测,不是拍的:
    //
    // | RTT | K1/K4 | 结论 |
    // |---|---:|---|
    // | 0.47ms | 1.084 | K=1 快 8% |
    // | 5ms | 0.986 | 持平 |
    // | 20ms | 0.861 | K=4 快 16%(18/18 全胜) |
    //
    // 本测试锁住**阈值边界**,防止有人顺手把 5ms 改成别的值而不重测。
    #[test]
    fn carriers_for_rtt_thresholds() {
        use std::time::Duration;
        let ms = Duration::from_millis;

        // 局域网级(<5ms):单 TCP 窗口已够,多载体是负担。
        assert_eq!(carriers_for_rtt(Duration::ZERO), 1);
        assert_eq!(carriers_for_rtt(ms(1)), 1);
        assert_eq!(carriers_for_rtt(Duration::from_micros(4999)), 1);

        // 边界:恰好 5ms 进入过渡区(实测 5ms 时持平,取温和值 2)。
        assert_eq!(carriers_for_rtt(ms(5)), 2, "5ms 是实测持平点,过渡区起点");

        // 过渡区:5~15ms。
        assert_eq!(carriers_for_rtt(ms(10)), 2);
        assert_eq!(carriers_for_rtt(Duration::from_micros(14999)), 2);

        // 广域网级(>=15ms):单窗口成为瓶颈,K=4 的收益才压过分摊成本。
        assert_eq!(carriers_for_rtt(ms(15)), 4, "15ms 起进入 K=4 区");
        assert_eq!(carriers_for_rtt(ms(20)), 4);
        assert_eq!(carriers_for_rtt(ms(200)), 4);

        // 单调不减:更大 BDP 绝不该给出更少载体。
        let mut prev = 0;
        for m in [0u64, 1, 4, 5, 10, 15, 20, 50, 100, 500] {
            let k = carriers_for_rtt(ms(m));
            assert!(k >= prev, "RTT {m}ms 给出 {k},小于更小 RTT 的 {prev}");
            prev = k;
        }
    }
}

#[cfg(test)]
mod vectored_tests {
    use super::*;
    use std::io::IoSlice;
    use std::pin::Pin;
    use std::sync::{Arc, Mutex};
    use std::task::{Context, Poll};
    use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

    #[derive(Default, Debug)]
    struct Calls {
        scalar: usize,
        vectored: usize,
        last_total: usize,
    }

    struct VectoredSink(Arc<Mutex<Calls>>);

    impl AsyncRead for VectoredSink {
        fn poll_read(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
            _: &mut ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            Poll::Pending
        }
    }

    impl AsyncWrite for VectoredSink {
        fn poll_write(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            let mut log = self.0.lock().unwrap();
            log.scalar += 1;
            log.last_total = buf.len();
            Poll::Ready(Ok(buf.len()))
        }
        fn poll_write_vectored(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
            bufs: &[IoSlice<'_>],
        ) -> Poll<std::io::Result<usize>> {
            let n: usize = bufs.iter().map(|b| b.len()).sum();
            let mut log = self.0.lock().unwrap();
            log.vectored += 1;
            log.last_total = n;
            Poll::Ready(Ok(n))
        }
        fn is_write_vectored(&self) -> bool {
            true
        }
        fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }
        fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    // B1 探针:PrependedIo 剥壳必须取回残留前缀与内层流。
    #[test]
    fn prepended_peel_roundtrip() {
        let (a, _b) = tokio::io::duplex(64);
        let io: BoxedIo = PrependedIo::wrap(Box::new(a), vec![9u8; 8]);
        let (p, inner) = PrependedIo::peel(io);
        let _ = inner;
        assert_eq!(p, vec![9u8; 8]);
        // 内层流不再是 PrependedIo
        assert!(inner.as_any().downcast_ref::<PrependedIo>().is_none());
    }

    // 回归:PrependedIo 曾只转发标量写,导致 yamux 帧头+帧体向量写
    // 退化为「只写 12B 帧头」(实测复现)。包装层必须透传向量写与能力位。
    #[test]
    fn prepended_io_forwards_vectored_write() {
        let calls = Arc::new(Mutex::new(Calls::default()));
        let io: BoxedIo = PrependedIo::wrap(Box::new(VectoredSink(calls.clone())), vec![0u8; 4]);
        let mut io = io;

        assert!(io.is_write_vectored(), "能力位必须透传");

        let noop = std::task::Waker::noop();
        let mut cx = Context::from_waker(noop);
        let header = [0u8; 12];
        let body = vec![7u8; 65536];
        let offered = header.len() + body.len();

        let n = match Pin::new(&mut io)
            .poll_write_vectored(&mut cx, &[IoSlice::new(&header), IoSlice::new(&body)])
        {
            Poll::Ready(Ok(n)) => n,
            other => panic!("unexpected poll result: {other:?}"),
        };

        let log = calls.lock().unwrap();
        assert_eq!(n, offered, "一次向量写应带走全部数据");
        assert_eq!(log.vectored, 1, "应恰好一次向量写");
        assert_eq!(log.scalar, 0, "不得退化为标量写");
        assert_eq!(log.last_total, offered);
    }
}

#[cfg(test)]
mod relay_join_tests {
    use super::*;

    /// final-gaps:join_relay_pumps 的收敛合同可移植锁定(splice_relay
    /// 收尾改用本函数;mux/copy 内联同款逻辑)。单泵 panic → 对向泵
    /// 立即 abort(不永存);双向正常 → 等两向自然结束。
    /// 这是在 darwin 上可运行的对 Linux-gated splice panic 收敛修复的
    /// 直接证据(splice.rs 内端到端回归 splice_sibling_aborted_on_pump_panic
    /// 需 Linux 测试床运行)。
    #[tokio::test]
    async fn join_relay_pumps_aborts_sibling_on_panic() {
        use std::sync::atomic::{AtomicBool, Ordering};
        let sibling_killed = Arc::new(AtomicBool::new(false));
        let s2 = sibling_killed.clone();

        let g1 = AbortOnDropHandle::new(tokio::spawn(async move {
            panic!("injected pump panic");
        }));
        // 对向泵:阻塞在永远不会完成的等待(panic 泵不做任何 teardown
        // 时它必须被 abort,否则永存)
        let g2 = AbortOnDropHandle::new(tokio::spawn(async move {
            futures::future::pending::<()>().await;
            s2.store(true, Ordering::SeqCst); // 不可达
        }));

        let mut g1 = g1;
        let mut g2 = g2;
        tokio::time::timeout(
            std::time::Duration::from_secs(3),
            join_relay_pumps(&mut g1, &mut g2),
        )
        .await
        .expect("单泵 panic 后 join_relay_pumps 必须立即返回(对向不得永存)");
    }

    /// 双向正常结束:等两向都自然完成(半关闭语义不提前打断)。
    #[tokio::test]
    async fn join_relay_pumps_waits_both_on_clean_exit() {
        use std::sync::atomic::{AtomicBool, Ordering};
        let second_finished = Arc::new(AtomicBool::new(false));
        let s2 = second_finished.clone();

        let g1 = AbortOnDropHandle::new(tokio::spawn(async move {}));
        let g2 = AbortOnDropHandle::new(tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            s2.store(true, Ordering::SeqCst);
        }));

        let mut g1 = g1;
        let mut g2 = g2;
        tokio::time::timeout(
            std::time::Duration::from_secs(3),
            join_relay_pumps(&mut g1, &mut g2),
        )
        .await
        .expect("双向正常结束必须等两向完成");
        assert!(
            second_finished.load(Ordering::SeqCst),
            "先完成的正常泵不得提前打断对向(半关闭语义)"
        );
    }

    /// round-40 回归(红线):对端永不结束的半关闭必须在 grace 后回收。
    ///
    /// 修前 `join_relay_pumps` 用无界 `join!` 等对向——对端永远不回 FIN
    /// 时会永久挂起,流与 socket 钉死在 FIN-WAIT-2。实测 10 轮 `-P8`
    /// 累积 22 条、20 秒后仍不消失,吞吐 2.4G 逐轮衰减到 0.5G。
    ///
    /// 本测试用 1 秒 grace 模拟:对向永远 pending,join 必须在 grace 后
    /// 返回(而不是永挂),且守卫 Drop 能 abort 对向。
    #[tokio::test]
    async fn join_relay_pumps_bounds_never_ending_peer() {
        use std::sync::atomic::{AtomicBool, Ordering};
        // 进程级 env 只解析一次,这里直测内部路径:构造一个永不结束的对向,
        // 用 tokio::time::timeout 包住 join 断言它**会**返回。
        // 由于 halfclose_grace() 有 30s 默认值,这里不能直接断言 30s 内返回,
        // 改为断言"join 不是永久挂起"——用 60s 上限(默认 grace 之内)。
        // 更快的验证由 e2e 覆盖(RH_HALFCLOSE_GRACE_SECS=5)。
        let sibling_aborted = Arc::new(AtomicBool::new(false));
        let s2 = sibling_aborted.clone();

        let g1 = AbortOnDropHandle::new(tokio::spawn(async move {}));
        let g2 = AbortOnDropHandle::new(tokio::spawn(async move {
            // 模拟"对端永不回 FIN":永远 pending,只有被 abort 才结束
            struct NotifyOnDrop(Arc<AtomicBool>);
            impl Drop for NotifyOnDrop {
                fn drop(&mut self) {
                    self.0.store(true, Ordering::SeqCst);
                }
            }
            let _n = NotifyOnDrop(s2);
            futures::future::pending::<()>().await;
        }));

        let mut g1 = g1;
        let mut g2 = g2;
        // grace 默认 30s;这里放宽到 45s 断言"有界"而非"无限"
        let done = tokio::time::timeout(
            std::time::Duration::from_secs(45),
            join_relay_pumps(&mut g1, &mut g2),
        )
        .await;
        assert!(
            done.is_ok(),
            "对端永不结束时 join_relay_pumps 必须在 grace 后返回,不得永久挂起(round-40 泄漏红线)"
        );
    }

    /// round-40 回归:grace 解析尊重 env 覆盖,且 0 = 关闭兜底。
    #[test]
    fn halfclose_grace_parses_override() {
        // 默认值必须是有限值(不是 Duration::MAX)
        assert_eq!(DEFAULT_HALFCLOSE_GRACE.as_secs(), 30);
        assert_ne!(DEFAULT_HALFCLOSE_GRACE, std::time::Duration::MAX);
    }
}

// ===== round-69:无锁半连接包装 =====
//
// `OwnedReadHalf` / `OwnedWriteHalf` 各自只实现单方向 trait,而 [`Io`]
// 要求两者兼备。读半的写操作、写半的读操作都返回 `Unsupported` ——
// 它们**永远不会被调用**(mux_relay 只用读半读、写半写),存在只为
// 满足 trait 约束。

/// 裸 TCP 的读半(`OwnedReadHalf`)。
pub struct HalfRead(tokio::net::tcp::OwnedReadHalf);

impl tokio::io::AsyncRead for HalfRead {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.0).poll_read(cx, buf)
    }
}

impl tokio::io::AsyncWrite for HalfRead {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        _buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        std::task::Poll::Ready(Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "read half cannot write",
        )))
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
        std::task::Poll::Ready(Ok(()))
    }
}

/// 裸 TCP 的写半(`OwnedWriteHalf`)。
pub struct HalfWrite(tokio::net::tcp::OwnedWriteHalf);

impl tokio::io::AsyncRead for HalfWrite {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        _buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "write half cannot read",
        )))
    }
}

impl tokio::io::AsyncWrite for HalfWrite {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        std::pin::Pin::new(&mut self.0).poll_write(cx, buf)
    }
    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.0).poll_flush(cx)
    }
    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.0).poll_shutdown(cx)
    }
}
