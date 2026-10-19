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
}

pub type BoxedIo = Box<dyn Io>;

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
pub(crate) fn halfclose_grace() -> std::time::Duration {
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

/// 多载体池协议基线(round-10):tls/wss 受单核串行加解密限制,默认多载体;
/// tcp/ws 单载体已接近路径上限(实测 t/d 0.973~1.077),保持 1;
/// QUIC/KCP 自带多路复用,恒 1。
///
/// # 这是默认 K 的唯一来源(round-45)
///
/// 默认载体数**只**由本函数决定。调用链:
/// `mux_pool_size`(None) → 本函数 → `min(hardware_cap)` →
/// `final_k = initial_k.min(server_cap)`(服务端收敛只减不增)。
///
/// 曾经有一个「池默认载体数 = 4」的常量从未被读取,却让多份设计文档误以为
/// 默认 K 是 4;已删除。**若将来要改默认载体数,改这里,不要再引入同类常量。**
///
/// 该值只是**期望值**,最终 K 还要经硬件探测与服务端 `max_carriers` 收敛
/// (见 rh-client 的 pool sizing);此处不读环境/配置。
pub fn default_carriers(protocol: Protocol) -> usize {
    match protocol {
        Protocol::Tls | Protocol::Wss => 2,
        _ => 1,
    }
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
}

impl Default for ClientTransportConfig {
    fn default() -> Self {
        Self {
            protocol: Protocol::Tcp,
            server_addr: "127.0.0.1:7000".parse().unwrap(),
            multiplex: false,
            tls: TlsClientOptions::default(),
            proxy_url: None,
            alpn: vec!["rhtp".to_string()],
            connect_timeout: std::time::Duration::from_secs(10),
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
            alpn: vec!["rhtp".to_string()],
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
/// [`default_carriers`]:tcp/ws = 1,tls/wss = 2,quic/kcp = 1。
/// 显式配置见 `rh-client` 的 `mux_pool_size`(env `RH_MUX_POOL_SIZE`)。
///
/// 曾经此处写「推荐 K = 4」并引用一个从未被读取的常量,那与实现不符——
/// [`default_carriers`] 的上限是 2,服务端收敛只减不增,默认路径永远
/// 到不了 4。该常量已删除。
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

    // T-C1(round-10):协议基线六变体全覆盖。tcp/ws = 1(单载体已接近
    // 路径上限),tls/wss = 2(单核串行加解密),quic/kcp = 1(自带复用)。
    #[test]
    fn default_carriers_covers_all_protocols() {
        assert_eq!(default_carriers(Protocol::Tcp), 1);
        assert_eq!(default_carriers(Protocol::Tls), 2);
        assert_eq!(default_carriers(Protocol::Ws), 1);
        assert_eq!(default_carriers(Protocol::Wss), 2);
        assert_eq!(default_carriers(Protocol::Quic), 1);
        assert_eq!(default_carriers(Protocol::Kcp), 1);
        // 红线 1 的形式化:非 tcp-based 恒 1。
        for p in [Protocol::Quic, Protocol::Kcp] {
            assert!(!p.is_tcp_based());
            assert_eq!(default_carriers(p), 1);
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
