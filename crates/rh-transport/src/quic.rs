//! QUIC 载体(quinn):原生多流,一连接多流;ALPN 默认见 `DEFAULT_ALPN`。

use crate::{
    other_err, BoxedIo, ClientTransportConfig, Incoming, Multiplexer, MultiplexerInner,
    ServerTransportConfig, MUX_STREAMS_UNLIMITED,
};
use std::io;
use std::pin::Pin;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

const QUIC_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(15);
/// 空闲超时须显著大于 30s 协议心跳(quinn 默认 30s 恰等于心跳间隔,
/// 单向抖动即误杀);客户端同时以 keepalive 主动保活。
const QUIC_IDLE_TIMEOUT: Duration = Duration::from_secs(90);
const QUIC_KEEPALIVE: Duration = Duration::from_secs(15);

/// quinn 并发流上限的实际上限:大值(≥1<<23)在跨网(非回环)下会让
/// 握手静默超时(本地回环不复现,机理与 quinn 传输参数编码相关);
/// 65536 条并发流已远超数据通道实际需求,作为「不限」的落点与钳制上限。
const QUIC_STREAMS_CAP: u32 = 1 << 16;

/// 并发流上限钳制(R1 红线测试缝):0=不限 → CAP;n → min(n, CAP)。
fn clamped_streams(max_streams: usize) -> u32 {
    if max_streams == 0 {
        QUIC_STREAMS_CAP
    } else {
        (max_streams as u32).min(QUIC_STREAMS_CAP)
    }
}

fn transport_config(keepalive: bool, max_streams: usize) -> quinn::TransportConfig {
    let mut tc = quinn::TransportConfig::default();
    tc.max_idle_timeout(Some(quinn::IdleTimeout::from(quinn::VarInt::from(
        QUIC_IDLE_TIMEOUT.as_millis() as u32,
    ))));
    // quinn 默认仅允许 100 条并发流,远低于数据通道并发需求
    let n = quinn::VarInt::from(clamped_streams(max_streams));
    tc.max_concurrent_bidi_streams(n);
    tc.max_concurrent_uni_streams(n);
    // 实测注记(2026-10,10GbE/0.4ms 三机):receive_window/stream_receive_window
    // 抬到 8M/2M、开启 DPLPMTUD 对吞吐无可测收益(467 vs 601 Mbps,方差内),
    // 吞吐瓶颈在单连接驱动任务的逐包用户态处理,不在流控;故保持默认。
    if keepalive {
        tc.keep_alive_interval(Some(QUIC_KEEPALIVE));
    }
    // RH_QUIC_CC=bbr 时换 BBR 拥塞控制(默认 cubic;C1/C3 实验用)。
    // N14:OnceLock 化 —— 修前每次 connect 读一次 env(分配 + 全局锁),
    // 而该值进程内恒定。
    static QUIC_CC_BBR: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    if *QUIC_CC_BBR.get_or_init(|| std::env::var("RH_QUIC_CC").as_deref() == Ok("bbr")) {
        tc.congestion_controller_factory(std::sync::Arc::new(
            quinn::congestion::BbrConfig::default(),
        ));
    }
    tc
}

/// QUIC 端点 UDP 缓冲目标值(字节),与 `kcp.rs::UDP_BUFFER_BYTES` 同源理由。
///
/// quinn 自己用 `Endpoint::client/server` 时**不设** `SO_RCVBUF`,吃内核
/// 默认值 212992(208 KiB)。QUIC 在 600 Mbps / 1200B 下约 62k pps,
/// 208 KiB 仅够缓冲 ~170 个报文(≈2.7ms);一次调度抖动就丢包,而丢包
/// 直接触发 cubic 降窗 + 重传,吞吐被压在直连的 ~30%。
///
/// 4 MiB ≈ 3500 个报文(≈56ms)足以吸收抖动;不设更大是因为
/// `net.core.rmem_max` 通常会静默截断,且过大只增排队延迟。
const QUIC_UDP_BUFFER_BYTES: usize = 4 * 1024 * 1024;

/// 绑定 UDP socket 并尽力放大收发缓冲,交给 quinn 复用。
///
/// 用 `Endpoint::new` 而非 `Endpoint::client/server`:后者内部自建 socket,
/// 无法在握手前设缓冲。**设不上不算失败** —— `net.core.rmem_max` 可能
/// 低于目标、内核静默截断,调优失败不应让连接建不起来。
fn bind_tuned_udp(addr: std::net::SocketAddr) -> io::Result<std::net::UdpSocket> {
    let sock = std::net::UdpSocket::bind(addr)?;
    let sref = socket2::SockRef::from(&sock);
    for (name, res) in [
        (
            "SO_RCVBUF",
            sref.set_recv_buffer_size(QUIC_UDP_BUFFER_BYTES),
        ),
        (
            "SO_SNDBUF",
            sref.set_send_buffer_size(QUIC_UDP_BUFFER_BYTES),
        ),
    ] {
        if let Err(e) = res {
            tracing::debug!(option = name, error = %e, "quic udp buffer 设置失败(按尽力而为忽略)");
        }
    }
    Ok(sock)
}

/// 服务端 QUIC 监听器:接受与握手解耦(慢握手不阻塞后续接入)。
///
/// `Drop` 通过 watch 信号让 accept 循环退出(与 `TcpListenerShim::drop`
/// 同款),使 UDP socket 得以释放、端口可重绑。**不用 `Endpoint::close()`**:
/// 那会给每个已建立连接发送 Close 事件,把还在正常转发连接一起打死 ——
/// 而 drop listener 的语义只是"不再接新连接"。
pub struct QuicListener {
    pub(crate) local: std::net::SocketAddr,
    ready: tokio::sync::mpsc::Receiver<Incoming>,
    /// 通知 accept 循环退出(只停接受,不动已有连接)。
    shutdown: tokio::sync::watch::Sender<()>,
}

impl Drop for QuicListener {
    fn drop(&mut self) {
        // 电平台触发:accept 循环的 select 立即醒来并返回,UDP socket 随
        // 循环任务持有的 endpoint 引用释放而关闭(已建立连接各自持有
        // endpoint 引用,不受影响,继续服务到自然结束)。
        let _ = self.shutdown.send(());
    }
}

/// 服务端流上限在端点构建时固化(quinn 不支持热换端点配置),改动需重启;
/// yamux 路径(tcp/tls/ws/wss/kcp)无此限制,对新连接即时生效。
pub fn listen(
    cfg: &ServerTransportConfig,
    mux_streams: &std::sync::atomic::AtomicUsize,
) -> io::Result<QuicListener> {
    let tls = crate::tls::server_config_with_alpn(&cfg.tls, &cfg.alpn)?;
    let mut server_config = quinn::ServerConfig::with_crypto(Arc::new(
        quinn::crypto::rustls::QuicServerConfig::try_from(tls)
            .map_err(|e| other_err(format!("quic server config: {e}")))?,
    ));
    server_config.transport_config(Arc::new(transport_config(
        false,
        mux_streams.load(Ordering::Relaxed),
    )));
    let endpoint = quinn::Endpoint::new(
        quinn::EndpointConfig::default(),
        Some(server_config),
        bind_tuned_udp(cfg.bind_addr)?,
        quinn::default_runtime().ok_or_else(|| other_err("quic: no async runtime"))?,
    )
    .map_err(|e| other_err(format!("quic bind: {e}")))?;
    let local = endpoint
        .local_addr()
        .map_err(|e| other_err(format!("quic local addr: {e}")))?;
    let (tx, ready) = tokio::sync::mpsc::channel::<Incoming>(512);
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(());
    tokio::spawn(async move {
        loop {
            // biased:先看退出信号,listener drop 后立即停止接受新连接
            let incoming = tokio::select! {
                biased;
                _ = shutdown_rx.changed() => return,
                res = endpoint.accept() => match res {
                    Some(i) => i,
                    None => return, // endpoint 已关闭
                },
            };
            let tx2 = tx.clone();
            tokio::spawn(async move {
                match tokio::time::timeout(QUIC_HANDSHAKE_TIMEOUT, incoming).await {
                    Ok(Ok(conn)) => {
                        let remote = conn.remote_address();
                        let inc = Incoming {
                            mux: Arc::new(Multiplexer {
                                inner: MultiplexerInner::Quic(QuicConnection::new(conn)),
                            }),
                            remote,
                            protocol: crate::Protocol::Quic,
                        };
                        let _ = tx2.send(inc).await;
                    }
                    Ok(Err(e)) => {
                        tracing::debug!(error = %e, "quic handshake failed");
                    }
                    Err(_) => tracing::debug!("quic handshake timeout"),
                }
            });
        }
    });
    // listener 只持退出信号:drop 时不碰 endpoint/已建立连接。
    Ok(QuicListener {
        local,
        ready,
        shutdown: shutdown_tx,
    })
}

pub async fn accept(listener: &mut QuicListener) -> io::Result<Incoming> {
    listener
        .ready
        .recv()
        .await
        .ok_or_else(|| other_err("quic listener closed"))
}

/// Endpoint 缓存键:本地绑定地址族 + 创建它的 runtime。
type EndpointKey = (bool, tokio::runtime::Id);

/// 缓存值:身份令牌 + Endpoint。令牌与 Endpoint 在同一次插入中产生、
/// 之后不再变化,`Arc::ptr_eq` 即**结构性**的实例同一性判别。
/// Clone 对 token 是 Arc 克隆(指针不变),克隆体仍是同一身份 —— 这是
/// `evict_endpoint` 用缓存值里的 token 比对的前提。
#[derive(Clone)]
struct CachedEndpoint {
    token: Arc<()>,
    endpoint: quinn::Endpoint,
}

fn endpoint_cache(
) -> &'static std::sync::Mutex<std::collections::HashMap<EndpointKey, CachedEndpoint>> {
    static CACHE: std::sync::OnceLock<
        std::sync::Mutex<std::collections::HashMap<EndpointKey, CachedEndpoint>>,
    > = std::sync::OnceLock::new();
    CACHE.get_or_init(Default::default)
}

/// 从缓存中驱逐一个 Endpoint(仅当它仍是调用方实际用过的**同一实例**)。
///
/// 实例判别用与 Endpoint 同一次插入产生的身份令牌(`Arc::ptr_eq`):
/// 令牌相等 ⟺ 同一次 `client_endpoint` 插入,无 IO、无 Err 路径、
/// 无端口撞车。并发下另一任务驱逐重建后,本任务持旧令牌再来驱逐时
/// 比对不中,新鲜实例安然留在缓存里被复用。
///
/// 为什么不用 quinn 自己的接口判身份(查证 quinn 0.11.12 endpoint.rs,
/// 行号系该版本快照,升级 quinn 后以结构描述为准):
/// - `Endpoint` 仅 derive `Debug, Clone`(L49-50),无 `PartialEq`;
/// - 内部 `EndpointRef(Arc<EndpointInner>)` 为 `pub(crate)`(L678),
///   外部拿不到指针做比较;
/// - `driver_lost` 是私有字段(L483),无公开存活查询。
///
/// 前一版用 `local_addr` 比对,已废弃,三个理由:
/// 1. 任一方 `local_addr()` 返回 Err(socket 被内核回收等)即放弃驱逐,
///    本次 attempt 已耗尽,自愈多绕一整次 connect;
/// 2. 端口撞车(死实例与并发重建的新实例恰好同端口)只能靠
///    "0.0.0.0:0 重绑几乎不可能撞上"的概率论证排除,不是结构性保证;
/// 3. local_addr 是 socket 属性而非实例身份,相等不蕴含同一实例。
fn evict_endpoint(key: EndpointKey, token: &Arc<()>) {
    let mut g = endpoint_cache().lock().unwrap();
    if matches!(g.get(&key), Some(cur) if Arc::ptr_eq(&cur.token, token)) {
        g.remove(&key);
    }
}

/// 判断错误是否指向 **Endpoint 自身**已不可用(而非服务端侧问题)。
///
/// 只认 `ConnectError::EndpointStopping` —— quinn 在 driver 任务死亡
/// (`EndpointDriver` 的 future 结束,例如 UDP socket IO 错误)或
/// `Endpoint::close()` 之后,`connect_with` 会立刻返回它,且**不发出任何
/// 网络包**。这是唯一可靠的"endpoint 级"判据:
///
/// - 握手超时 / 连接被拒 / 证书错误 → 都在 `connecting.await` 之后才出现,
///   属于**服务端侧或路径**问题,驱逐 Endpoint 只会白白丢掉复用(还要
///   重建 UDP socket + driver),下次照样失败;
/// - `CidsExhausted` / `InvalidRemoteAddress` / `InvalidServerName` 等是
///   **配置**问题,驱逐同样无用。
///
/// 因此只对 EndpointStopping 驱逐,其余错误原样上抛。
fn is_endpoint_level_error(e: &quinn::ConnectError) -> bool {
    matches!(e, quinn::ConnectError::EndpointStopping)
}

/// N14:按 (bind 族, runtime Id) 缓存的客户端 Endpoint,连同身份令牌一起
/// 返回(令牌供 [`evict_endpoint`] 做实例同一性判别)。
/// 键必须含 runtime Id:Endpoint 的 driver 任务绑定**创建它的** runtime,
/// runtime 退出后 driver 即死 —— 按进程全局缓存时,测试环境(每个
/// #[tokio::test] 一个独立 runtime)首个测试结束后共享 Endpoint 的
/// 握手全部 "closed"。按 runtime 键控后:生产单 runtime 恰好一份/族,
/// 测试各 runtime 各自一份。
///
/// **失效路径**:缓存本身没有主动驱逐(quinn 的 Endpoint 没有"是否还活着"
/// 的公开查询接口,见 `evict_endpoint` 注释的查证),死 Endpoint 由
/// `connect` 在失败时按需驱逐 —— 见 [`is_endpoint_level_error`]。
fn client_endpoint(is_v4: bool) -> io::Result<(Arc<()>, quinn::Endpoint)> {
    let key = (is_v4, tokio::runtime::Handle::current().id());
    let mut g = endpoint_cache().lock().unwrap();
    if let Some(c) = g.get(&key) {
        return Ok((c.token.clone(), c.endpoint.clone()));
    }
    // 本地绑定地址必须与**服务端地址同族**:写死 0.0.0.0 时连 IPv6 服务端
    // 会因 address family 不匹配直接失败(与 kcp.rs 的同款分支对齐)。
    let bind_local: std::net::SocketAddr = if is_v4 {
        "0.0.0.0:0".parse().unwrap()
    } else {
        "[::]:0".parse().unwrap()
    };
    let endpoint = quinn::Endpoint::new(
        quinn::EndpointConfig::default(),
        None,
        bind_tuned_udp(bind_local)?,
        quinn::default_runtime().ok_or_else(|| other_err("quic: no async runtime"))?,
    )
    .map_err(|e| other_err(format!("quic client bind: {e}")))?;
    let cached = CachedEndpoint {
        token: Arc::new(()),
        endpoint: endpoint.clone(),
    };
    let token = cached.token.clone();
    g.insert(key, cached);
    Ok((token, endpoint))
}

pub async fn connect(cfg: &ClientTransportConfig) -> io::Result<Arc<Multiplexer>> {
    // R2:同 dial_io 走按配置键缓存(B5;QUIC 的 rustls 侧同样带
    // 会话恢复票据缓存,重连每次重建会让它永远空转)
    let (tls, verify) = crate::tls::client_config_cached(&cfg.tls, &cfg.alpn)?;
    let mut qcfg = quinn::ClientConfig::new(Arc::new(
        quinn::crypto::rustls::QuicClientConfig::try_from(tls)
            .map_err(|e| other_err(format!("quic client config: {e}")))?,
    ));
    qcfg.transport_config(Arc::new(transport_config(true, MUX_STREAMS_UNLIMITED)));
    let is_v4 = cfg.server_addr.is_ipv4();
    let key = (is_v4, tokio::runtime::Handle::current().id());
    let name = cfg
        .tls
        .server_name
        .clone()
        .unwrap_or_else(|| cfg.server_addr.ip().to_string());
    // N14:Endpoint 按服务端地址族缓存复用(修前每次 connect 新绑一个
    // UDP socket + 起 driver;quinn 客户端形态本就支持单 Endpoint 多
    // 连接,CID 区分)。配置经 connect_with 每次携带,不同 tls 配置的
    // 调用共享 Endpoint 不互相覆盖。
    //
    // ★失效自愈(修前是"永久 brick"):缓存的 Endpoint 一旦 driver 死亡
    // (UDP socket IO 错误等),后续所有 connect 都会立刻返回
    // `EndpointStopping` 且**不发任何包**,只能重启进程恢复 —— N14 之前
    // 每次新建 Endpoint 天然自愈。这里在**确认是 endpoint 级错误**时
    // 驱逐缓存并重试一次;服务端侧错误(超时/拒绝/证书)绝不驱逐,
    // 否则只会白丢 Endpoint 复用。
    let mut attempt = 0;
    let conn = loop {
        let (token, endpoint) = client_endpoint(is_v4)?;
        // 一次超时覆盖"建连接对象 + 握手"两段:修前两段各包一次
        // connect_timeout,最坏要等 2× 超时才失败(上层按 connect_timeout
        // 做重连退避,双倍延迟会让退避节奏整体错位)。endpoint.connect
        // 本身只构造状态机不阻塞,真正耗时的握手在 awaiting `connecting`
        // 时发生,故合并为单个 deadline 即可。
        //
        // 两段错误类型不同,在闭包内就地转成 io::Error;但 **ConnectError
        // 要单独带出来** —— 只有它能区分"endpoint 级"与"参数级"错误。
        let r = tokio::time::timeout(cfg.connect_timeout, async {
            let connecting = endpoint.connect_with(qcfg.clone(), cfg.server_addr, &name)?;
            Ok::<_, quinn::ConnectError>(connecting.await)
        })
        .await;
        match r {
            // 握手成功
            Ok(Ok(Ok(c))) => break c,
            // 握手失败(服务端侧/路径/证书):原样上抛,**不驱逐**
            Ok(Ok(Err(e))) => {
                return Err(other_err(format!("quic handshake: {e}")));
            }
            // connect_with 失败:只有 EndpointStopping 才值得驱逐重试
            Ok(Err(e)) => {
                if attempt == 0 && is_endpoint_level_error(&e) {
                    // 死 Endpoint:驱逐(token 判同一实例,见 evict_endpoint)
                    // 后重试一次(下一次 loop 会重建)
                    evict_endpoint(key, &token);
                    attempt += 1;
                    continue;
                }
                return Err(other_err(format!("quic connect: {e}")));
            }
            Err(_) => return Err(other_err("quic connect/handshake timeout")),
        }
    };
    // 记录对端叶子证书:上层(rh-client)用它比对服务端用 secret 加密下发的
    // 证书,不一致即说明中间有人在转发。与 tcp/tls 路径(sink 在握手后立刻
    // 写入)对齐——不写 sink 时上层 `peer_cert.get()` 恒为 None,而
    // `secret_verified` 模式下"取不到对端证书"就是拒绝连接,表现为 QUIC
    // 握手成功却无限重连。
    //
    // `peer_identity()` 返回 None(对端未出示证书)或类型不符时不写 sink:
    // 保持"取不到即失败"的语义,绝不塞一个空证书进去充当比对成功。
    // 多载体:逐条追加(QUIC 恒单载体,这里为契约一致性保留)
    if let Some(all) = &cfg.peer_cert_sink_all {
        if let Ok(Some(leaf)) = peer_leaf_cert(&conn) {
            if let Ok(mut v) = all.lock() {
                v.push(leaf);
            }
        }
    }
    if let Some(sink) = &cfg.peer_cert_sink {
        if let Ok(Some(leaf)) = peer_leaf_cert(&conn) {
            let _ = sink.set(leaf);
        }
    }
    // pin 模式:握手后比对叶子证书指纹
    if let crate::tls::ClientVerify::Pin(expected) = verify {
        let leaf = peer_leaf_cert(&conn)?.ok_or_else(|| other_err("quic: no peer certificate"))?;
        crate::tls::verify_pin(&expected, &leaf)?;
    }
    Ok(Arc::new(Multiplexer {
        inner: MultiplexerInner::Quic(QuicConnection::new(conn)),
    }))
}

/// 取 QUIC 对端叶子证书 DER。
///
/// 抽出来供 sink 记录与 pin 比对共用,避免两处各写一遍 downcast 而漂移。
/// 返回 `Ok(None)` 表示**对端没有出示证书**(quinn 在未要求客户端证书时
/// 仍会保留服务端证书,故实际多为 `Some`);类型不符/空链是硬错误。
fn peer_leaf_cert(conn: &quinn::Connection) -> io::Result<Option<Vec<u8>>> {
    let Some(identity) = conn.peer_identity() else {
        return Ok(None);
    };
    let certs = identity
        .downcast::<Vec<rustls_pki_types::CertificateDer<'static>>>()
        .map_err(|_| other_err("quic: unexpected peer identity type"))?;
    let leaf = certs
        .first()
        .ok_or_else(|| other_err("quic: empty certificate chain"))?;
    Ok(Some(leaf.as_ref().to_vec()))
}

/// quinn 连接包装:开流/收流。
pub struct QuicConnection {
    conn: quinn::Connection,
}

impl QuicConnection {
    pub fn new(conn: quinn::Connection) -> Self {
        // 关闭看门:持有 Connection 克隆直到连接真正关闭。作用是
        // **句柄 drop 后连接对象不立即释放** —— 已写出的在途数据得以
        // 冲刷(quic_connect 回显测试曾因移除此任务竞态失败:服务端
        // 回写后句柄 drop,endpoint 释放直接 ApplicationClose,客户端
        // 读不到回声)。与 yamux POOL_DROP_GRACE 同理。
        // C8:原 `dead` 状态位无读者(仅被删除的 is_dead 读),不再保留。
        let watcher_conn = conn.clone();
        tokio::spawn(async move {
            let _ = watcher_conn.closed().await;
        });
        Self { conn }
    }

    pub async fn open_stream(&self) -> io::Result<BoxedIo> {
        let (send, recv) = self
            .conn
            .open_bi()
            .await
            .map_err(|e| other_err(format!("quic open_bi: {e}")))?;
        Ok(Box::new(BiStream { send, recv }))
    }

    pub async fn accept_stream(&self) -> io::Result<BoxedIo> {
        match self.conn.accept_bi().await {
            Ok((send, recv)) => Ok(Box::new(BiStream { send, recv })),
            Err(e) => Err(other_err(format!("quic accept_bi: {e}"))),
        }
    }
}

/// quinn 双向流包装为字节流(读 RecvStream,写 SendStream)。
pub struct BiStream {
    send: quinn::SendStream,
    recv: quinn::RecvStream,
}

impl AsyncRead for BiStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.recv).poll_read(cx, buf)
    }
}

impl AsyncWrite for BiStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.send)
            .poll_write(cx, buf)
            .map_err(|e| other_err(format!("quic write: {e}")))
    }
    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.send.finish() {
            Ok(()) => Poll::Ready(Ok(())),
            Err(e) => Poll::Ready(Err(other_err(format!("quic finish: {e}")))),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{listen, ClientTransportConfig, Protocol, ServerTransportConfig};

    // R1(红线 1 锁):quinn 并发流上限必须保持 65536,且钳制路径
    // 0=不限 → CAP、n → min(n, CAP)——任何放宽都不得发生。
    #[test]
    fn quic_streams_cap_and_clamp() {
        assert_eq!(
            super::QUIC_STREAMS_CAP,
            65536,
            "quinn 并发流上限红线(65536)"
        );
        assert_eq!(super::clamped_streams(0), 65536);
        assert_eq!(super::clamped_streams(1), 1);
        assert_eq!(super::clamped_streams(65536), 65536);
        assert_eq!(
            super::clamped_streams(1 << 20),
            65536,
            "超限值必须钳制到 CAP"
        );
    }

    #[tokio::test]
    async fn quic_connect_and_stream() {
        let (cert, key) = crate::tls::auto_cert(&Default::default()).unwrap();
        let server_cfg = ServerTransportConfig {
            protocol: Protocol::Quic,
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            multiplex: true,
            tls: crate::TlsServerOptions {
                cert_pem: Some(cert),
                key_pem: Some(key),
                ..Default::default()
            },
            alpn: vec![crate::DEFAULT_ALPN.into()],
        };
        let mut listener = listen(
            &server_cfg,
            std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        )
        .await
        .unwrap();
        let addr = listener.local_addr;

        let srv = tokio::spawn(async move {
            let inc = listener.accept().await.unwrap();
            let mut s = inc.mux.accept().await.unwrap();
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let mut buf = [0u8; 8];
            s.read_exact(&mut buf).await.unwrap();
            s.write_all(&buf).await.unwrap();
        });

        let client = ClientTransportConfig {
            protocol: Protocol::Quic,
            server_addr: addr,
            multiplex: true,
            tls: crate::TlsClientOptions {
                insecure_skip_verify: true,
                ..Default::default()
            },
            ..Default::default()
        };
        let mux = crate::connect(&client).await.unwrap();
        let mut s = mux.open_stream().await.unwrap();
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        s.write_all(b"quic-ok!").await.unwrap();
        let mut back = [0u8; 8];
        s.read_exact(&mut back).await.unwrap();
        assert_eq!(&back, b"quic-ok!");
        srv.await.unwrap();
    }

    /// listener drop 后 accept 循环必须退出(资源收敛)。
    ///
    /// 修前 `QuicListener` 只持有 ready Receiver,`accept()` 只在 endpoint
    /// 关闭后才返回 None —— drop listener 后 accept 循环**永续**,任务与
    /// 及其持有的 endpoint 引用长期泄漏。TCP(`TcpListenerShim::drop` 发
    /// watch + abort 泵)与 KCP(触发 closing)都做了收敛,QUIC 缺席。
    ///
    /// # 实测修正:影响面比最初判断的小
    ///
    /// 曾以为"UDP socket 永久占用、端口无法重绑"。实测否定:listener 并不
    /// 持有 endpoint,endpoint 只被 accept 循环任务持有;而**端口重绑**在这种
    /// 状态下仍会成功(UDP 行为如此),新连接也照常被接受。所以真实泄漏只有
    /// "一个空转的 accept 循环任务 + 它持有的 endpoint 引用"。
    ///
    /// 因此本条测试的定位是**回归护栏**(锁住 drop 必须发信号这一实现),
    /// 而不是能区分 bug 的断言 —— 负向验证(把 `shutdown.send(())` 删掉)
    /// 实测**不会变红**,已如实记录,避免后人误以为它有区分力。
    ///
    /// **回归(N14 缓存无失效路径)**:缓存中的 Endpoint 一旦失效,下一次
    /// connect 必须自愈,而不是永久 brick。
    ///
    /// 构造:正常建一次连接(把 Endpoint 写进缓存),然后对**缓存里那个**
    /// Endpoint 调 `close()` —— 这是 quinn 提供的"让 endpoint 停止服务"
    /// 的公开手段,之后 `connect_with` 立刻返回 `EndpointStopping`
    /// (与 driver 死亡后的表现完全一致,是同一判据)。
    ///
    /// 修前:缓存的死 Endpoint 被一直返回 → 后续 connect 全部
    /// `quic connect: endpoint stopping`,只能重启进程。
    /// 修后:识别为 endpoint 级错误 → 驱逐 + 重试一次 → 成功。
    #[tokio::test]
    async fn dead_cached_endpoint_is_evicted_and_next_connect_heals() {
        let (cert, key) = crate::tls::auto_cert(&Default::default()).unwrap();
        let server_cfg = ServerTransportConfig {
            protocol: Protocol::Quic,
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            multiplex: true,
            tls: crate::TlsServerOptions {
                cert_pem: Some(cert),
                key_pem: Some(key),
                ..Default::default()
            },
            alpn: vec![crate::DEFAULT_ALPN.into()],
        };
        let mut listener = listen(
            &server_cfg,
            std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        )
        .await
        .unwrap();
        let addr = listener.local_addr;
        // 服务端持续 accept(两次连接)
        let srv = tokio::spawn(async move {
            for _ in 0..2 {
                if let Ok(_inc) = listener.accept().await {
                    // 保持连接存活
                    tokio::spawn(async move {
                        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                    });
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        });

        let client = ClientTransportConfig {
            protocol: Protocol::Quic,
            server_addr: addr,
            multiplex: true,
            connect_timeout: std::time::Duration::from_secs(5),
            tls: crate::TlsClientOptions {
                insecure_skip_verify: true,
                ..Default::default()
            },
            ..Default::default()
        };

        // 第一次:把 Endpoint 建进缓存
        let mux1 = super::connect(&client).await.expect("首次连接必须成功");
        drop(mux1);

        // 让缓存里的 Endpoint 失效(等价于 driver 死亡后的状态)
        {
            let key = (addr.is_ipv4(), tokio::runtime::Handle::current().id());
            let ep = super::endpoint_cache()
                .lock()
                .unwrap()
                .get(&key)
                .cloned()
                .expect("首次连接后缓存里必须有 Endpoint");
            ep.endpoint
                .close(quinn::VarInt::from_u32(0), b"test: kill endpoint");
        }
        // 给 driver 一点时间收敛到"停止服务"
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // 第二次:**必须自愈**(修前会一直拿到死 Endpoint 而永久失败)
        let mux2 =
            tokio::time::timeout(std::time::Duration::from_secs(10), super::connect(&client))
                .await
                .expect("死 Endpoint 不得让 connect 卡住")
                .expect(
                    "缓存中的 Endpoint 失效后,下一次 connect 必须驱逐并重建;\
             永久失败说明缓存没有失效路径(N14 遗留)",
                );
        drop(mux2);
        srv.abort();
    }

    /// 反向契约:**服务端侧**错误不得驱逐 Endpoint。
    ///
    /// 对端不可达时错误是握手超时(ConnectionError),不是 EndpointStopping。
    /// 若在这里也驱逐,每次超时都要重建 UDP socket + driver,纯属浪费 ——
    /// 而且下次照样失败,驱逐没有任何收益。
    #[tokio::test]
    async fn server_side_error_does_not_evict_endpoint() {
        // 找一个必然无人监听的本地端口
        let dead_addr = {
            let l = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
            l.local_addr().unwrap()
        };
        let client = ClientTransportConfig {
            protocol: Protocol::Quic,
            server_addr: dead_addr,
            multiplex: true,
            connect_timeout: std::time::Duration::from_millis(300),
            tls: crate::TlsClientOptions {
                insecure_skip_verify: true,
                ..Default::default()
            },
            ..Default::default()
        };
        let r = super::connect(&client).await;
        assert!(r.is_err(), "对端不可达时连接必须失败");

        // Endpoint 仍应留在缓存里(握手超时不等于 endpoint 坏了)
        let key = (dead_addr.is_ipv4(), tokio::runtime::Handle::current().id());
        assert!(
            super::endpoint_cache().lock().unwrap().contains_key(&key),
            "握手超时属于服务端侧错误,不得驱逐 Endpoint(否则白丢复用)"
        );
    }

    /// 身份判别契约:驱逐只认**同一次插入**的令牌(token)。
    ///
    /// 并发场景下,另一任务可能已因同样原因驱逐并重建过;此时本任务持
    /// **旧令牌**再来驱逐,必须比对不中、留下新鲜实例 —— 否则重连风暴中
    /// N 个并发任务会连环误删彼此刚重建的 Endpoint,每次都多绑一个
    /// UDP socket + driver。修前(local_addr 比对)除此之外还有
    /// Err 即放弃、端口撞车两个瑕疵,token 判别一并消除。
    ///
    /// 反向验证(2026-10-25 实测):把 `evict_endpoint` 的 `Arc::ptr_eq`
    /// 条件去掉改成无条件 remove,本测试的"旧令牌不得驱逐"断言立刻变红。
    /// `client_endpoint` 无网络依赖(只绑本地 UDP socket),可直连直测。
    #[tokio::test]
    async fn evict_matches_token_identity() {
        let key = (true, tokio::runtime::Handle::current().id());

        // 首建 (T1, E1);T1 驱逐 → 缓存清空、下轮重建
        let (t1, _e1) = super::client_endpoint(true).unwrap();
        super::evict_endpoint(key, &t1);
        assert!(
            !super::endpoint_cache().lock().unwrap().contains_key(&key),
            "匹配令牌必须驱逐"
        );

        // 重建得到 (T2, E2);持旧令牌 T1 再驱逐 → 不得动 E2
        let (t2, _e2) = super::client_endpoint(true).unwrap();
        super::evict_endpoint(key, &t1);
        assert!(
            super::endpoint_cache().lock().unwrap().contains_key(&key),
            "旧令牌不得驱逐并发重建的新鲜实例"
        );

        // 现任令牌 T2 驱逐 → 生效
        super::evict_endpoint(key, &t2);
        assert!(
            !super::endpoint_cache().lock().unwrap().contains_key(&key),
            "现任令牌必须驱逐"
        );
    }

    /// 反向契约:drop **不得**杀死已建立连接 —— 第一版实现用
    /// `Endpoint::close()` 收敛,实测把在途连接一起打死
    /// (`ConnectionLost(ApplicationClosed{reason:"listener dropped"})`,
    /// 由 `quic_connect_and_stream` 抓出),故改为 watch 信号。
    #[tokio::test]
    async fn quic_listener_drop_stops_accepting() {
        let (cert, key) = crate::tls::auto_cert(&Default::default()).unwrap();
        let server_cfg = ServerTransportConfig {
            protocol: Protocol::Quic,
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            multiplex: true,
            tls: crate::TlsServerOptions {
                cert_pem: Some(cert),
                key_pem: Some(key),
                ..Default::default()
            },
            alpn: vec![crate::DEFAULT_ALPN.into()],
        };
        let streams = std::sync::atomic::AtomicUsize::new(0);
        let listener = super::listen(&server_cfg, &streams).unwrap();
        let addr = listener.local;
        drop(listener);

        // drop 后新连接不得成功建立(accept 循环已退出,endpoint 随循环任务
        // 持有的最后一份引用释放而关闭,UDP 不再响应)。
        let client = ClientTransportConfig {
            protocol: Protocol::Quic,
            server_addr: addr,
            connect_timeout: std::time::Duration::from_millis(800),
            tls: crate::TlsClientOptions {
                insecure_skip_verify: true,
                ..Default::default()
            },
            ..Default::default()
        };
        let res =
            tokio::time::timeout(std::time::Duration::from_secs(3), super::connect(&client)).await;
        // 超时或失败都算"没有被接受";只有 Ok 说明 accept 循环还活着
        if let Ok(Ok(_)) = res {
            panic!("listener drop 后不应还能建立连接(accept 循环未退出)");
        }
    }

    /// `secret_verified` 模式下上层靠 `peer_cert_sink` 拿到对端证书做比对。
    /// QUIC 曾经不写 sink → 上层 `peer_cert.get()` 为 None → 握手成功也
    /// 拒绝连接,表现为无限重连。本测试锁住"QUIC 与 tcp/tls 一样写入 sink",
    /// 且写入的正是服务端叶子证书(逐字节相等,上层才可能比对通过)。
    #[tokio::test]
    async fn quic_writes_peer_cert_sink_in_secret_verified_mode() {
        let (cert, key) = crate::tls::auto_cert(&Default::default()).unwrap();
        let expect_leaf = crate::tls::leaf_der_pem(&cert).unwrap();
        let server_cfg = ServerTransportConfig {
            protocol: Protocol::Quic,
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            multiplex: true,
            tls: crate::TlsServerOptions {
                cert_pem: Some(cert),
                key_pem: Some(key),
                ..Default::default()
            },
            alpn: vec![crate::DEFAULT_ALPN.into()],
        };
        let mut listener = listen(
            &server_cfg,
            std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        )
        .await
        .unwrap();
        let addr = listener.local_addr;
        let srv = tokio::spawn(async move {
            let _inc = listener.accept().await.unwrap();
            // 保持连接存活,避免客户端侧 sink 读取前连接被关
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        });

        let sink: std::sync::Arc<std::sync::OnceLock<Vec<u8>>> = Default::default();
        let client = ClientTransportConfig {
            protocol: Protocol::Quic,
            server_addr: addr,
            multiplex: true,
            // 默认配置(无 CA / 无 pin / 非 insecure)下的真实路径:
            // 握手不校验,完全依赖上层 secret 下发比对 —— 前提是 sink 有值。
            tls: crate::TlsClientOptions {
                secret_verified: true,
                ..Default::default()
            },
            peer_cert_sink: Some(sink.clone()),
            ..Default::default()
        };
        let _mux = crate::connect(&client).await.unwrap();
        let got = sink.get().expect("QUIC 必须在握手后写入 peer_cert_sink");
        assert_eq!(
            got.as_slice(),
            expect_leaf.as_slice(),
            "sink 必须是服务端叶子证书"
        );
        srv.abort();
    }

    /// 未设置 sink 时不应额外做事,更不应让连接失败(调用方不需要该信息)。
    #[tokio::test]
    async fn quic_connect_without_sink_still_ok() {
        let (cert, key) = crate::tls::auto_cert(&Default::default()).unwrap();
        let server_cfg = ServerTransportConfig {
            protocol: Protocol::Quic,
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            multiplex: true,
            tls: crate::TlsServerOptions {
                cert_pem: Some(cert),
                key_pem: Some(key),
                ..Default::default()
            },
            alpn: vec![crate::DEFAULT_ALPN.into()],
        };
        let mut listener = listen(
            &server_cfg,
            std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        )
        .await
        .unwrap();
        let addr = listener.local_addr;
        let srv = tokio::spawn(async move {
            let _inc = listener.accept().await.unwrap();
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        });
        let client = ClientTransportConfig {
            protocol: Protocol::Quic,
            server_addr: addr,
            multiplex: true,
            tls: crate::TlsClientOptions {
                secret_verified: true,
                ..Default::default()
            },
            ..Default::default()
        };
        crate::connect(&client).await.unwrap();
        srv.abort();
    }
}
