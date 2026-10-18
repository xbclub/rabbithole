//! TCP/TLS/WS/WSS 载体:共用的拨号、监听与握手管线。
//!
//! 客户端:`connect_stream`(建立首连接)与 `dial_stream`(开一条新流)。
//! 服务端:`listen_stream` + `TcpListenerShim::accept`(完成各层握手后
//! 返回该物理连接的 Multiplexer)。

use crate::ws;
use crate::{
    other_err, ClientTransportConfig, Incoming, Multiplexer, MultiplexerInner,
    ServerTransportConfig, MUX_STREAMS_UNLIMITED,
};
use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio_rustls::TlsConnector;

const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);

/// 服务端 TCP/TLS/WS/WSS 监听器。
/// 接受与握手解耦:后台泵逐个 accept TCP,每连接的握手(TLS/WS/peek)
/// 在独立任务完成,慢握手不得阻塞后续接入(DoS 缓解);就绪连接入队供消费。
///
/// 所有权契约:
/// - 句柄(`TcpListenerShim`)是监听资源的唯一所有者:它持有 accept 泵任务
///   (JoinHandle)与关停信号;泵持有底层 `TcpListener` 与全部在途验证 child
///   (JoinSet)。
/// - drop 句柄 = 关停信号 + abort 泵:泵立即停止 accept(不等下一个连接),
///   监听 socket 随之释放,原地址可重绑;在途验证 child 随 JoinSet 析构被
///   中止——它们的结果已无消费者,连同行文 socket 一并回收,有界不泄漏。
/// - ready 队列里排的是"已验证未交付"的裸 IO(见 [`VerifiedConn`]),mux
///   延迟到 `accept()` 交付时才构造:未交付连接没有任何 yamux driver,
///   句柄 drop 时随队列/背压 child 析构直接关闭,不会出现 detached driver
///   拖住 socket(不触碰 mux 全局析构语义/open_rx 红线)。
/// - 已通过 `accept()` 交付的 mux 归消费者所有,不受句柄 drop 影响(与
///   yamux open_rx 释放不杀存量流的约定一致)。
pub struct TcpListenerShim {
    local: SocketAddr,
    ready: tokio::sync::mpsc::Receiver<VerifiedConn>,
    /// 电平触发关停信号;Sender 随句柄析构也会令泵的 `changed()` 立即返回。
    shutdown: tokio::sync::watch::Sender<()>,
    /// accept 泵任务句柄:唯一所有权,drop 时 abort(有界回收的硬兜底)。
    pump: Option<tokio::task::JoinHandle<()>>,
}

impl TcpListenerShim {
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        Ok(self.local)
    }

    pub async fn accept(&mut self) -> io::Result<Incoming> {
        let v = self
            .ready
            .recv()
            .await
            .ok_or_else(|| other_err("tcp listener closed"))?;
        // 交付即构造 mux:所有权与消费者同步建立。此前连接只是已验证裸 IO
        // (含 peek 前传字节),accept 之前客户端已发的帧仍缓冲在 IO 里,
        // 构造后由 driver 正常处理;未交付路径则从不存在 driver。
        // round-E2:yamux 连接且持有写侧 fd 副本 → 双载体 mux(零锁
        // 读∥写);否则(try_clone 失败 / TLS/WS / 非 yamux)锁路径。
        let mux = match (v.yamux, v.write_dup) {
            (true, Some(dup)) => crate::mux::wrap_stream_as_server_duplex(v.io, dup, v.max_streams),
            _ => crate::mux::wrap_stream_as_server(v.io, v.yamux, v.max_streams),
        };
        Ok(Incoming {
            mux,
            remote: v.remote,
            protocol: v.protocol,
        })
    }

    /// 已验证待交付连接的排队数(测试缝:监听生命周期回归用它在 drop 前
    /// 取得"已入队未消费"的因果证据,替代纯时间猜测;不参与任何运行时逻辑)。
    #[doc(hidden)]
    pub fn ready_len(&self) -> usize {
        self.ready.len()
    }
}

impl Drop for TcpListenerShim {
    fn drop(&mut self) {
        // 1) 电平触发关停:泵的 select 立即唤醒退出,绝不等待下一个连接
        //    (Sender 析构本身也会触发同样的唤醒,这里显式发送以表达意图)。
        let _ = self.shutdown.send(());
        // 2) 有界硬兜底:即便泵因内部缺陷未能观察信号,abort 也会让泵 future
        //    被析构——监听 socket 与 JoinSet(连同在途握手 child)随之释放。
        //    回收只需一次取消调度,与是否有新连接到来无关。
        if let Some(pump) = self.pump.take() {
            pump.abort();
        }
    }
}

/// 已通过全部验证(TLS/WS/peek)、等待 accept 交付的连接。
/// mux 延迟到 accept() 交付时才构造(见 `TcpListenerShim::accept`):
/// 未交付期间队列中只有裸 IO,监听句柄 drop 时随队列/背压 child 析构
/// 直接关闭;yamux driver 只随交付产生、归消费者所有。
struct VerifiedConn {
    io: crate::BoxedIo,
    /// 裸 TCP 分支的写侧 fd 副本(round-E2,verify 的 try_clone 产物):
    /// accept 交付时若有则走 duplex mux(halves 零锁);无则锁路径。
    /// 非 yamux 交付(RHTP 前缀/裸模式)不消费,随本结构析构关闭 fd
    ///(socket 本体仍由 io 持有,不受影响)。
    write_dup: Option<crate::BoxedIo>,
    remote: SocketAddr,
    protocol: crate::Protocol,
    /// peek 判定:按 yamux 处理(true)或裸 RHTP/非 multiplex 监听(false)。
    yamux: bool,
    /// 物理 accept 时读取的 mux_streams 热改值(保持"对新连接生效"时机不变)。
    max_streams: usize,
}

/// 裸 TCP fd 复制(round-E2,真·载体拆分的写侧载体):tokio TcpStream
/// 无 try_clone(1.53 仅 fs::File 有),经 std fd 等价实现——
/// F_DUPFD_CLOEXEC 复制 fd,与原 fd 共享同一打开文件描述(O_NONBLOCK
/// 等 fd 状态同源),`from_std` 重新注册到当前 runtime 的 IO driver
///(独立注册,与原 fd 无冲突)。任一步失败 → None → 调用方降级锁路径
///(不报错、不失败建连)。
fn try_dup_tcp(stream: &tokio::net::TcpStream) -> Option<crate::BoxedIo> {
    use std::os::fd::AsFd as _;
    let fd = stream.as_fd().try_clone_to_owned().ok()?;
    let std_stream = std::net::TcpStream::from(fd);
    let dup = tokio::net::TcpStream::from_std(std_stream).ok()?;
    Some(Box::new(dup) as crate::BoxedIo)
}

/// 单连接验证管线:TLS → WS 升级 → 首字节窥探。
/// 只做验证与分类,不构造 mux;验证流程/超时与原实现完全一致。
/// 返回 (io, write_dup, yamux):write_dup 仅在裸 TCP 分支产生
/// (round-E2;TLS/WS 会话层载体不可 fd 复制,恒 None)。
async fn verify(
    stream: tokio::net::TcpStream,
    tls: Option<tokio_rustls::TlsAcceptor>,
    ws_upgrade: bool,
    multiplex: bool,
) -> io::Result<(crate::BoxedIo, Option<crate::BoxedIo>, bool)> {
    stream.set_nodelay(true).ok();
    set_tcp_keepalive(&stream);
    // round-E2:裸 TCP 分支(tls.is_none() && !ws_upgrade)在装箱前产生
    // 写侧 fd 副本,供 accept 交付时走 duplex mux。peek_prefix 只从主
    //(读)侧消费 4 字节——双 fd 共享同一内核收队列,但写侧绝不读
    //(halves 方向纪律,见 vendor Connection::into_halves_duplex),无
    // 消费冲突。try_clone 失败 → None → 锁路径。multiplex 监听才需要
    // 副本(非 mux 监听永不消费,避免每连接白开一个 fd)。
    let write_dup = if multiplex && tls.is_none() && !ws_upgrade {
        try_dup_tcp(&stream)
    } else {
        None
    };
    let io: crate::BoxedIo = match tls {
        Some(acceptor) => {
            let tls_stream = tokio::time::timeout(HANDSHAKE_TIMEOUT, acceptor.accept(stream))
                .await
                .map_err(|_| other_err("tls handshake timeout"))??;
            Box::new(tls_stream)
        }
        None => Box::new(stream),
    };
    post_tls(io, write_dup, ws_upgrade, multiplex).await
}

async fn post_tls(
    io: crate::BoxedIo,
    write_dup: Option<crate::BoxedIo>,
    ws_upgrade: bool,
    multiplex: bool,
) -> io::Result<(crate::BoxedIo, Option<crate::BoxedIo>, bool)> {
    let io = if ws_upgrade {
        let io = tokio::time::timeout(HANDSHAKE_TIMEOUT, ws::upgrade_server(io, ws::WS_PATH))
            .await
            .map_err(|_| other_err("ws handshake timeout"))??;
        io
    } else {
        io
    };
    if !multiplex {
        return Ok((io, write_dup, false));
    }
    // 混合接入:multiplex 监听允许裸模式客户端。
    // 窥探首 4 字节:RHTP 魔数 = 裸连接(每连接一流);否则按 yamux 处理。
    // 窥探字节必须前传(两种包装都需要)。前传只挂在主(读)侧 IO 上,
    // write_dup 不涉及(方向纪律,同上)。
    let (prefix, io) = peek_prefix(io).await?;
    let yamux = prefix != *b"RHTP";
    Ok((
        crate::PrependedIo::wrap(io, prefix.to_vec()),
        write_dup,
        yamux,
    ))
}

/// 窥探首 4 字节(带超时,防慢连接占资源);返回字节与可继续读取的流。
async fn peek_prefix(mut io: crate::BoxedIo) -> io::Result<([u8; 4], crate::BoxedIo)> {
    use tokio::io::AsyncReadExt as _;
    let mut b = [0u8; 4];
    tokio::time::timeout(HANDSHAKE_TIMEOUT, io.read_exact(&mut b))
        .await
        .map_err(|_| other_err("peek timeout"))??;
    Ok((b, io))
}

#[allow(unused)]
fn set_tcp_keepalive(stream: &tokio::net::TcpStream) {
    use socket2::{SockRef, TcpKeepalive};
    let s = SockRef::from(stream);
    let ka = TcpKeepalive::new().with_time(Duration::from_secs(30));
    let _ = s.set_tcp_keepalive(&ka);
}

/// mux_streams:并发流上限的有效值(0 已折算),每连接 accept 时读取 → 热改对新连接生效。
pub async fn listen_stream(
    cfg: &ServerTransportConfig,
    mux_streams: Arc<AtomicUsize>,
) -> io::Result<TcpListenerShim> {
    let listener = TcpListener::bind(cfg.bind_addr).await?;
    let local = listener.local_addr()?;
    let tls = if matches!(cfg.protocol, crate::Protocol::Tls | crate::Protocol::Wss) {
        Some(tokio_rustls::TlsAcceptor::from(crate::tls::server_config(
            &cfg.tls,
        )?))
    } else {
        None
    };
    if !cfg.protocol.is_encrypted() {
        tracing::warn!(protocol = %cfg.protocol, "plaintext transport (dev/debug only)");
    }
    let ws_upgrade = matches!(cfg.protocol, crate::Protocol::Ws | crate::Protocol::Wss);
    let multiplex = cfg.multiplex;
    let protocol = cfg.protocol;
    let mux_limit = mux_streams;
    // 握手泵:accept 与验证解耦,慢验证不阻塞后续接入。
    // 队列 512:高并发建连(1024+)时 64 会排队,建连 p99 恶化
    let (tx, ready) = tokio::sync::mpsc::channel::<VerifiedConn>(512);
    // 句柄持有 watch Sender:句柄 drop → 泵的 changed() 立即返回 → 停止 accept。
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(());
    // 握手 child 由泵的 JoinSet 持有:完成即回收(panic/错误可观测,不悬空);
    // 泵退出时中止全部在途 child(结果已无消费者),连同行文 socket 一并释放。
    let pump = tokio::spawn(async move {
        let mut handshakes = tokio::task::JoinSet::new();
        loop {
            tokio::select! {
                biased;
                // 句柄已 drop(显式信号或 Sender 析构):立即停止,不等下一个连接
                _ = shutdown_rx.changed() => break,
                // 回收已完成的握手 child:JoinError(panic/abort)必须被观测,不得悬空
                Some(res) = handshakes.join_next(), if !handshakes.is_empty() => {
                    if let Err(e) = res {
                        if e.is_panic() {
                            tracing::error!(error = %e, "tcp handshake task panicked");
                        } else {
                            tracing::warn!(error = %e, "tcp handshake task aborted");
                        }
                    }
                }
                res = listener.accept() => {
                    let (stream, remote) = match res {
                        Ok(v) => v,
                        Err(e) => {
                            tracing::warn!(error = %e, "tcp accept error");
                            tokio::time::sleep(Duration::from_millis(50)).await;
                            continue;
                        }
                    };
                    let tx2 = tx.clone();
                    let tls2 = tls.clone();
                    let max_streams = mux_limit.load(Ordering::Relaxed);
                    handshakes.spawn(async move {
                        match verify(stream, tls2, ws_upgrade, multiplex).await {
                            Ok((io, write_dup, yamux)) => {
                                let vc = VerifiedConn {
                                    io,
                                    write_dup,
                                    remote,
                                    protocol,
                                    yamux,
                                    max_streams,
                                };
                                // 消费者已走:send 立即失败,已验证裸 IO 随 child
                                // 析构而关闭(此阶段不存在 mux/driver),不泄漏
                                let _ = tx2.send(vc).await;
                            }
                            Err(e) => {
                                tracing::debug!(error = %e, %remote, "tcp handshake failed");
                            }
                        }
                    });
                }
            }
        }
        // 停止接新连接后,中止并 join 收尸在途握手(取消有界:child 只在 await
        // 点被取消,无阻塞段);ready 队列随接收端析构而消亡。
        handshakes.abort_all();
        while let Some(res) = handshakes.join_next().await {
            if let Err(e) = res {
                if e.is_panic() {
                    tracing::error!(error = %e, "tcp handshake task panicked");
                }
            }
        }
        // listener 在此析构 → 监听端口释放
    });
    Ok(TcpListenerShim {
        local,
        ready,
        shutdown: shutdown_tx,
        pump: Some(pump),
    })
}

/// 客户端:建立一条物理连接并握手。多路复用时返回 yamux mux;
/// 裸模式下连接本体即一条流(AcceptSingle),后续流由 DialPerStream 拨号。
pub async fn connect_stream(cfg: &ClientTransportConfig) -> io::Result<Arc<Multiplexer>> {
    if cfg.multiplex || cfg.protocol.always_muxed() {
        let (io, write_dup) = dial_io(cfg).await?;
        // 客户端为开流方:超限是单流报错(不杀会话),取"不限制"即可,
        // 真正的策略上限由服务端在 DATA_CHANNEL_HELLO 处执行。
        // round-E2:裸 TCP 持写侧 fd 副本 → 双载体 mux(零锁读∥写);
        // None(非 Tcp 协议 / try_clone 失败)→ 锁路径。
        Ok(match write_dup {
            Some(dup) => crate::mux::wrap_stream_as_client_duplex(io, dup, MUX_STREAMS_UNLIMITED),
            None => crate::mux::wrap_stream_as_client(io, true, MUX_STREAMS_UNLIMITED),
        })
    } else {
        // 裸模式:开流即拨号
        Ok(Arc::new(Multiplexer {
            inner: MultiplexerInner::DialPerStream { cfg: cfg.clone() },
        }))
    }
}

/// 客户端:为裸模式拨一条新的物理连接(= 一条新流)。
/// 裸模式(AcceptSingle/DialPerStream)不涉及双载体:每连接一流,
/// write_dup 不消费,随返回丢弃。
pub async fn dial_stream(cfg: &ClientTransportConfig) -> io::Result<crate::BoxedIo> {
    let (io, _write_dup) = dial_io(cfg).await?;
    Ok(io)
}

/// 客户端:建立 K 条独立物理连接组成多载体池(round-8 优化项 5)。
/// `carriers <= 1` 或非 mux 配置 → 等价 [`connect_stream`](单载体,
/// 行为零漂移)。K 条连接并行拨号,任一失败即整体失败(已建连接随 drop
/// 关闭,不残留 driver);池持有按同一 cfg 的重拨配方,载体故障时自动
/// 补位。每条连接各自独立的本地源端口 → 不同四元组 → 网卡 RSS 散列到
/// 不同 CPU 核;TLS 下 K 组并行加解密流水线。
pub async fn connect_stream_pool(
    cfg: &ClientTransportConfig,
    carriers: usize,
) -> io::Result<Arc<Multiplexer>> {
    if carriers <= 1 || !(cfg.multiplex || cfg.protocol.always_muxed()) {
        return connect_stream(cfg).await;
    }
    // round-10:每载体 shard 数按 K 缩减(总预算 ≈ 4),避免 K 条连接各自
    // 4 个收发 Worker 使数据面线程总量随 K 线性膨胀。
    let shards = crate::mux::shard_workers_for_k(carriers);
    let members =
        futures::future::try_join_all((0..carriers).map(|_| dial_client_mux(cfg, shards))).await?;
    let redial_cfg = cfg.clone();
    let redial: crate::mux::PoolRedial = Arc::new(move || {
        let cfg = redial_cfg.clone();
        Box::pin(async move { dial_client_mux(&cfg, shards).await })
    });
    Ok(Multiplexer::yamux_pool_client(members, Some(redial)))
}

/// 拨一条物理连接并包装为客户端 yamux(双载体优先,与 [`connect_stream`]
/// 同构:裸 TCP 持写侧 fd 副本 → duplex 零锁;否则锁路径)。shards 为
/// 该载体的收发分片 Worker 数(round-10 由池规模推导)。
async fn dial_client_mux(
    cfg: &ClientTransportConfig,
    shards: usize,
) -> io::Result<crate::mux::YamuxMultiplexer> {
    let (io, write_dup) = dial_io(cfg).await?;
    Ok(match write_dup {
        Some(dup) => crate::mux::YamuxMultiplexer::new_duplex(
            io,
            dup,
            yamux::Mode::Client,
            MUX_STREAMS_UNLIMITED,
            shards,
        ),
        None => crate::mux::YamuxMultiplexer::new(
            io,
            yamux::Mode::Client,
            MUX_STREAMS_UNLIMITED,
            shards,
        ),
    })
}

/// 客户端拨号:返回 (主 IO, 写侧 fd 副本)。副本仅 Protocol::Tcp 裸分支
/// 产生(round-E2,`try_dup_tcp`;失败 → None → 调用方锁路径),
/// TLS/WS 会话层载体不可 fd 复制,恒 None。
async fn dial_io(
    cfg: &ClientTransportConfig,
) -> io::Result<(crate::BoxedIo, Option<crate::BoxedIo>)> {
    let fut = async {
        let tcp = dial_tcp(cfg).await?;
        match cfg.protocol {
            crate::Protocol::Tcp => {
                let write_dup = try_dup_tcp(&tcp);
                Ok::<(crate::BoxedIo, Option<crate::BoxedIo>), io::Error>((
                    Box::new(tcp) as crate::BoxedIo,
                    write_dup,
                ))
            }
            crate::Protocol::Tls | crate::Protocol::Wss => {
                let (rust, verify) = crate::tls::client_config(&cfg.tls)?;
                let connector = TlsConnector::from(rust);
                let name = rustls_pki_types::ServerName::try_from(
                    cfg.tls
                        .server_name
                        .clone()
                        .unwrap_or_else(|| cfg.server_addr.ip().to_string()),
                )
                .map_err(|e| other_err(format!("bad tls server name: {e}")))?
                .to_owned();
                let tls_stream = connector.connect(name, tcp).await?;
                // pin 模式:握手后、发任何数据前比对叶子证书指纹
                if let crate::tls::ClientVerify::Pin(expected) = verify {
                    let certs = tls_stream
                        .get_ref()
                        .1
                        .peer_certificates()
                        .ok_or_else(|| other_err("tls: no peer certificate"))?;
                    let leaf = certs
                        .first()
                        .ok_or_else(|| other_err("tls: empty certificate chain"))?;
                    crate::tls::verify_pin(&expected, leaf.as_ref())?;
                }
                let io: crate::BoxedIo = Box::new(tls_stream);
                let io = if cfg.protocol == crate::Protocol::Wss {
                    ws::client_handshake(
                        io,
                        &cfg.tls
                            .server_name
                            .clone()
                            .unwrap_or_else(|| cfg.server_addr.to_string()),
                        ws::WS_PATH,
                    )
                    .await?
                } else {
                    io
                };
                Ok((io, None))
            }
            crate::Protocol::Ws => {
                let io =
                    ws::client_handshake(Box::new(tcp), &cfg.server_addr.to_string(), ws::WS_PATH)
                        .await?;
                Ok((io, None))
            }
            crate::Protocol::Quic | crate::Protocol::Kcp => Err(other_err("use quic/kcp connect")),
        }
    };
    tokio::time::timeout(cfg.connect_timeout, fut)
        .await
        .map_err(|_| other_err("transport connect timeout"))?
}

async fn dial_tcp(cfg: &ClientTransportConfig) -> io::Result<tokio::net::TcpStream> {
    let stream = match &cfg.proxy_url {
        Some(url) => {
            let proxy = crate::proxy_dial::ProxyUrl::parse(url)?;
            crate::proxy_dial::dial_via_proxy(&proxy, cfg.server_addr).await?
        }
        None => tokio::net::TcpStream::connect(cfg.server_addr).await?,
    };
    stream.set_nodelay(true).ok();
    set_tcp_keepalive(&stream);
    Ok(stream)
}
