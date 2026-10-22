//! QUIC 载体(quinn):原生多流,一连接多流;ALPN 默认见 `DEFAULT_ALPN`。

use crate::{
    other_err, BoxedIo, ClientTransportConfig, Incoming, Multiplexer, MultiplexerInner,
    ServerTransportConfig, MUX_STREAMS_UNLIMITED,
};
use std::io;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
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
    // RH_QUIC_CC=bbr 时换 BBR 拥塞控制(默认 cubic;C1/C3 实验用)
    if std::env::var("RH_QUIC_CC").as_deref() == Ok("bbr") {
        tc.congestion_controller_factory(std::sync::Arc::new(
            quinn::congestion::BbrConfig::default(),
        ));
    }
    tc
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
    let endpoint = quinn::Endpoint::server(server_config, cfg.bind_addr)
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

pub async fn connect(cfg: &ClientTransportConfig) -> io::Result<Arc<Multiplexer>> {
    let (tls, verify) = crate::tls::client_config_with_alpn(&cfg.tls, &cfg.alpn)?;
    let mut qcfg = quinn::ClientConfig::new(Arc::new(
        quinn::crypto::rustls::QuicClientConfig::try_from(tls)
            .map_err(|e| other_err(format!("quic client config: {e}")))?,
    ));
    qcfg.transport_config(Arc::new(transport_config(true, MUX_STREAMS_UNLIMITED)));
    // 本地绑定地址必须与**服务端地址同族**:写死 0.0.0.0 时连 IPv6 服务端
    // 会因 address family 不匹配直接失败(与 kcp.rs 的同款分支对齐)。
    let bind_local: std::net::SocketAddr = if cfg.server_addr.is_ipv4() {
        "0.0.0.0:0".parse().unwrap()
    } else {
        "[::]:0".parse().unwrap()
    };
    let mut endpoint = quinn::Endpoint::client(bind_local)
        .map_err(|e| other_err(format!("quic client bind: {e}")))?;
    endpoint.set_default_client_config(qcfg);
    let name = cfg
        .tls
        .server_name
        .clone()
        .unwrap_or_else(|| cfg.server_addr.ip().to_string());
    // 一次超时覆盖"建连接对象 + 握手"两段:修前两段各包一次 connect_timeout,
    // 最坏情况要等 2× 超时才失败(上层按 connect_timeout 做重连退避,双倍延迟
    // 会让退避节奏整体错位)。endpoint.connect 本身只构造状态机不阻塞,真正
    // 耗时的握手在 awaiting `connecting` 时发生,故合并为单个 deadline 即可。
    // 两段错误类型不同(ConnectError / ConnectionError),在闭包内就地转成
    // io::Error,让外层只需处理超时。
    let conn = tokio::time::timeout(cfg.connect_timeout, async {
        let connecting = endpoint
            .connect(cfg.server_addr, &name)
            .map_err(|e| other_err(format!("quic connect: {e}")))?;
        connecting
            .await
            .map_err(|e| other_err(format!("quic handshake: {e}")))
    })
    .await
    .map_err(|_| other_err("quic connect/handshake timeout"))??;
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

/// quinn 连接包装:开流/收流 + 关闭状态位。
pub struct QuicConnection {
    conn: quinn::Connection,
    dead: Arc<AtomicBool>,
}

impl QuicConnection {
    pub fn new(conn: quinn::Connection) -> Self {
        let dead = Arc::new(AtomicBool::new(false));
        let watcher_conn = conn.clone();
        let watcher_dead = dead.clone();
        tokio::spawn(async move {
            let _ = watcher_conn.closed().await;
            watcher_dead.store(true, Ordering::Relaxed);
        });
        Self { conn, dead }
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

    pub fn is_dead(&self) -> bool {
        self.dead.load(Ordering::Relaxed)
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
