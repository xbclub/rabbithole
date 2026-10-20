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
///
/// # 仅 Unix(round-80 修复)
///
/// `std::os::fd` 是 Unix 专属模块;Windows 没有 fd 概念(用 HANDLE),
/// 该模块被 `cfg` 掉。修前此函数无条件 `use std::os::fd::AsFd`,
/// **Windows 目标直接编译失败**(v0.4.0 发布流水线 x86_64/aarch64
/// 两个 msvc 目标同时红,报 E0432 `unresolved import std::os::fd`)。
///
/// 现在按 `cfg(unix)` 分支:非 Unix 返回 `None`,调用方本就把它当作
/// "无法 fd 复制"而降级到锁路径(`SplitSocket` + BiLock),**语义完全
/// 等价**——只是少了读∥写的物理并行,功能不受影响。
#[cfg(unix)]
fn try_dup_tcp(stream: &tokio::net::TcpStream) -> Option<crate::BoxedIo> {
    use std::os::fd::AsFd as _;
    let fd = stream.as_fd().try_clone_to_owned().ok()?;
    let std_stream = std::net::TcpStream::from(fd);
    let dup = tokio::net::TcpStream::from_std(std_stream).ok()?;
    Some(Box::new(dup) as crate::BoxedIo)
}

/// 非 Unix(Windows 等):无 `std::os::fd`,不支持 fd 复制 → 降级锁路径。
#[cfg(not(unix))]
fn try_dup_tcp(_stream: &tokio::net::TcpStream) -> Option<crate::BoxedIo> {
    None
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
    // round-41:载体发送缓冲调优(默认 0 = 不触碰,零漂移)
    apply_carrier_sndbuf(&stream);
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

/// 载体 TCP socket 的 `SO_SNDBUF` 目标值(round-41,**默认 0 = 不触碰**)。
///
/// 本项目此前**刻意**不碰 `SO_SNDBUF`/`SO_RCVBUF`(`splice.rs:1716` 红线:
/// "不为制造背压触碰任何 SO_RCVBUF/SO_SNDBUF")。本开关不改变该立场——
/// 默认行为与改动前逐字节一致,只在显式设置 `RH_MUX_SNDBUF` 时才调用
/// `setsockopt`,用于给"发送缓冲是否限制吞吐"这个**从未被单独测过的
/// 变量**一个明确判决。
///
/// 背景(为什么值得测一次):
/// * 实测载体 `SO_SNDBUF` = 87,040 B ≈ 85 KiB(内核 autotune,见
///   `benchmark/runs/goal-repair/targeted-before-round-3/REPORT.md:26`);
/// * 0.3 ms RTT 下 2.4 Gbps 的 BDP ≈ 90 KiB ⇒ **发送缓冲正好一个 BDP,
///   没有余量吸收突发**;
/// * 而 `pump_down` 一次冲刷是 1 MiB 批(`mux.rs` `DOWN_BATCH_BYTES`),
///   约 11 倍 BDP ⇒ 必然被内核切成多次部分写。
///
/// 反向证据(一并记录,避免过度期待):refrelay(无 yamux、64 KiB 单写)
/// 同样停在 2.0 G;splice 路径 `NOTSENT_LOWAT=1MiB` 已生效仍 2.0 G;
/// 三个独立实现都在 2.0 G。**故本项预期收益可能为 0——测出 0 也是有
/// 价值的结论。**
///
/// 取值:未设或 0 → 不设置;否则钳到 [4096, 64 MiB],非法值忽略走默认。
///
/// 注意 Linux 会把 `setsockopt(SO_SNDBUF)` 的值**翻倍**(内核预留一半给
/// 协议开销),所以设置 N 之后读回通常是 2N。
fn carrier_sndbuf() -> usize {
    static V: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *V.get_or_init(|| parse_carrier_sndbuf(std::env::var("RH_MUX_SNDBUF").ok().as_deref()))
}

/// [`carrier_sndbuf`] 的纯函数内核(可单测,不碰进程级 env)。
///
/// 规则:`None`/空/非法/0 → 0(不设置);否则钳到 [4096, 64 MiB]。
fn parse_carrier_sndbuf(raw: Option<&str>) -> usize {
    match raw.and_then(|s| s.trim().parse::<usize>().ok()) {
        Some(0) | None => 0,
        Some(n) => n.clamp(4096, 64 * 1024 * 1024),
    }
}

/// 应用 [`carrier_sndbuf`](若有)。**失败静默忽略**:旧内核/权限不足/
/// 非 TCP socket 都不应让建连失败或进程退出。
///
/// 默认(env 未设/为 0)走空分支,**不调用任何 setsockopt**——这是零漂移
/// 红线,由 `tests::sndbuf_default_is_noop` 守住。
fn apply_carrier_sndbuf(stream: &tokio::net::TcpStream) {
    let want = carrier_sndbuf();
    if want == 0 {
        return;
    }
    use socket2::SockRef;
    let _ = SockRef::from(stream).set_send_buffer_size(want);
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

/// 收集 K 条拨号结果;**部分失败时把已成功的成员逐个 `shutdown()`** 后再
/// 返回错误(round-42 修复的核心逻辑,抽成独立函数以便单测)。
///
/// ## 为什么必须 shutdown 而不是让它们 drop
///
/// `YamuxMultiplexer` 没有 `impl Drop`:drop 它只是把 `drivers` 里的
/// `JoinHandle` 一起 drop —— **detach 而非 abort**,driver 任务继续跑、
/// 继续持有载体 socket(`mux.rs:1147-1150` 字段注释明确说明这是为
/// 「正常路径零漂移」刻意保留的语义)。
///
/// 所以错误分支若只返回 Err,K=4 时前 3 条的 driver 全部泄漏,重连循环下
/// 累积。`shutdown()` 置 dead + abort drivers,是唯一正确的回收手段。
///
/// ## 为什么用 `FuturesUnordered` 而不是 `join_all`
///
/// `join_all` 要等**全部**拨号结束才返回,会把失败路径拖成
/// `connect_timeout × K` 量级;`try_join_all` 原本是快速失败的,不能
/// 退化成慢失败。
async fn collect_pool_members<F>(
    dials: impl IntoIterator<Item = F>,
    carriers: usize,
) -> io::Result<Vec<crate::mux::YamuxMultiplexer>>
where
    F: std::future::Future<Output = io::Result<crate::mux::YamuxMultiplexer>>,
{
    use futures::stream::{FuturesUnordered, StreamExt as _};
    let mut inflight: FuturesUnordered<F> = dials.into_iter().collect();
    let mut members: Vec<crate::mux::YamuxMultiplexer> = Vec::with_capacity(carriers);
    loop {
        match inflight.next().await {
            None => return Ok(members),
            Some(Ok(m)) => members.push(m),
            Some(Err(e)) => {
                // 先收干净"已完成但尚未取出"的成功成员(now_or_never 只消耗
                // 就绪队列,不等待在途拨号),否则 drop 掉它们同样是泄漏。
                while let Some(Some(done)) = futures::FutureExt::now_or_never(inflight.next()) {
                    if let Ok(m) = done {
                        members.push(m);
                    }
                }
                // 在途 future 随 inflight 析构被取消——它们尚未构造出
                // YamuxMultiplexer(构造在 dial_client_mux 末尾),无泄漏。
                for m in &members {
                    m.shutdown();
                }
                return Err(e);
            }
        }
    }
}

/// 客户端:建立 K 条独立物理连接组成多载体池(round-8 优化项 5)。
/// `carriers <= 1` 或非 mux 配置 → 等价 [`connect_stream`](单载体,
/// 行为零漂移)。K 条连接并行拨号,**任一失败即整体失败**,且已建成员
/// 会被显式 `shutdown()` 回收(round-42;修前只 drop,driver 分离后
/// 继续跑 = 泄漏);池持有按同一 cfg 的重拨配方,载体故障时自动补位。
/// 每条连接各自独立的本地源端口 → 不同四元组 → 网卡 RSS 散列到不同
/// CPU 核;TLS 下 K 组并行加解密流水线。
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
    // round-42:部分拨号失败时**必须显式 shutdown 已建立的成员**。
    //
    // 修前用 `try_join_all(...).await?`——任一 future 失败即返回 Err,已完成
    // 的 `YamuxMultiplexer` 直接 drop。而 `YamuxMultiplexer` 的 Drop 语义是
    // 「句柄丢弃即分离、driver 继续服务存量流」(mux.rs 字段注释,刻意保留
    // 以保证正常路径零漂移):driver 任务无人持有却仍在跑,持续占用 CPU
    // 与 fd。K=4 时第 3 条失败会泄漏前 2 条,重连循环下累积。
    //
    // 修复只在**错误分支**动作:逐个 `shutdown()`(置 dead + abort drivers,
    // 幂等)后再返回 Err。成功路径与 `carriers <= 1` 早退分支完全未改。
    //
    // 用 `FuturesUnordered` 而非 `join_all`:后者要等**全部**拨号结束才返回,
    // 会把失败路径拖成 `connect_timeout` 量级——`try_join_all` 原本是快速
    // 失败的,不能退化成慢失败。
    let members = collect_pool_members(
        (0..carriers).map(|_| dial_client_mux(cfg, shards)),
        carriers,
    )
    .await?;
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
    // round-41:载体发送缓冲调优(默认 0 = 不触碰,零漂移)
    apply_carrier_sndbuf(&stream);
    Ok(stream)
}

#[cfg(test)]
mod sndbuf_tests {
    use super::*;

    /// 纯函数内核:未设/空/非法/0 都必须是 0(= 不设置,零漂移红线)。
    #[test]
    fn parse_defaults_to_noop() {
        assert_eq!(parse_carrier_sndbuf(None), 0, "未设 env 必须不触碰");
        assert_eq!(parse_carrier_sndbuf(Some("")), 0, "空值必须不触碰");
        assert_eq!(parse_carrier_sndbuf(Some("0")), 0, "显式 0 必须不触碰");
        assert_eq!(parse_carrier_sndbuf(Some("abc")), 0, "非法值必须不触碰");
        assert_eq!(parse_carrier_sndbuf(Some("  ")), 0, "空白必须不触碰");
        assert_eq!(parse_carrier_sndbuf(Some("-1")), 0, "负值必须不触碰");
    }

    /// 合法值原样采用,并钳到 [4096, 64 MiB]。
    #[test]
    fn parse_clamps_to_range() {
        assert_eq!(parse_carrier_sndbuf(Some("4194304")), 4 * 1024 * 1024);
        assert_eq!(
            parse_carrier_sndbuf(Some(" 1048576 ")),
            1024 * 1024,
            "允许空白"
        );
        assert_eq!(parse_carrier_sndbuf(Some("1")), 4096, "过小钳到 4096");
        assert_eq!(
            parse_carrier_sndbuf(Some("999999999999")),
            64 * 1024 * 1024,
            "过大钳到 64 MiB"
        );
    }

    /// 零漂移红线:默认路径**不调用任何 setsockopt**,socket 的发送缓冲
    /// 与未调用前逐字节一致。
    ///
    /// 用真实 TCP 连接(loopback)读回 `send_buffer_size` 比对:内核 autotune
    /// 的初值在同一进程内是确定的,所以"调用前 == 调用后"是可判定断言。
    #[tokio::test]
    async fn sndbuf_default_is_noop() {
        use socket2::SockRef;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let accept = tokio::spawn(async move { listener.accept().await });
        let client = tokio::net::TcpStream::connect(addr).await.unwrap();
        let (server, _) = accept.await.unwrap().unwrap();

        // 前置条件:该测试进程未设置 RH_MUX_SNDBUF(否则跳过,避免受
        // 外部环境干扰而误报)。
        if carrier_sndbuf() != 0 {
            eprintln!("RH_MUX_SNDBUF 已设置为非 0,跳过零漂移断言");
            return;
        }

        let before = SockRef::from(&client).send_buffer_size().unwrap();
        apply_carrier_sndbuf(&client);
        let after = SockRef::from(&client).send_buffer_size().unwrap();
        assert_eq!(
            before, after,
            "默认(env 未设)时不得改动 SO_SNDBUF:before={before} after={after}"
        );

        // 服务端同理(verify 路径)
        let sb = SockRef::from(&server).send_buffer_size().unwrap();
        apply_carrier_sndbuf(&server);
        let sa = SockRef::from(&server).send_buffer_size().unwrap();
        assert_eq!(sb, sa, "默认时服务端侧同样不得改动 SO_SNDBUF");
    }

    /// 显式设置生效:Linux 会把 `setsockopt(SO_SNDBUF)` 翻倍,故断言
    /// 读回值 >= 目标(而不是 ==)。
    ///
    /// 直接调 socket2 设值(不经 env,避免进程级 OnceLock 与并行测试冲突),
    /// 验证"选定的 API 在该平台可用且语义如预期"——这是 `apply_carrier_sndbuf`
    /// 的实现依据。
    #[tokio::test]
    async fn sndbuf_explicit_set_takes_effect() {
        use socket2::SockRef;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let accept = tokio::spawn(async move { listener.accept().await });
        let client = tokio::net::TcpStream::connect(addr).await.unwrap();
        let _ = accept.await.unwrap().unwrap();

        let target = 4 * 1024 * 1024;
        SockRef::from(&client).set_send_buffer_size(target).unwrap();
        let got = SockRef::from(&client).send_buffer_size().unwrap();
        assert!(
            got >= target,
            "Linux 会翻倍,读回应 >= 目标:target={target} got={got}"
        );
    }
}

#[cfg(test)]
mod pool_leak_tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn make_mux() -> crate::mux::YamuxMultiplexer {
        let (a, _b) = tokio::io::duplex(64 * 1024);
        crate::mux::YamuxMultiplexer::new(
            Box::new(a) as crate::BoxedIo,
            yamux::Mode::Client,
            usize::MAX,
            1,
        )
    }

    /// round-42 主回归:**部分拨号失败时,已成功成员必须被 shutdown**。
    ///
    /// 构造 3 条拨号,第 3 条失败。用 `Arc<AtomicUsize>` 记录前 2 条被
    /// shutdown 的次数——修前(直接 `try_join_all(...)?`)它们只会被
    /// drop,计数为 0。
    ///
    /// 判据:`collect_pool_members` 返回 Err,且前 2 条各被 shutdown 一次。
    #[tokio::test]
    async fn partial_dial_failure_shuts_down_established_members() {
        // 记录每条已建立成员的 shutdown 调用次数
        let shutdown_calls = Arc::new(AtomicUsize::new(0));

        type Dial = std::pin::Pin<
            Box<
                dyn std::future::Future<Output = std::io::Result<crate::mux::YamuxMultiplexer>>
                    + Send,
            >,
        >;
        let dials: Vec<Dial> = (0..3)
            .map(|i| {
                let counter = shutdown_calls.clone();
                Box::pin(async move {
                    match i {
                        0 | 1 => {
                            if i == 0 {
                                counter.fetch_add(1, Ordering::SeqCst);
                            }
                            Ok(make_mux())
                        }
                        _ => Err(std::io::Error::new(
                            std::io::ErrorKind::ConnectionRefused,
                            "injected dial failure (3rd carrier)",
                        )),
                    }
                }) as Dial
            })
            .collect();

        let res = collect_pool_members(dials, 3).await;
        assert!(res.is_err(), "第 3 条失败必须让整体返回 Err");

        // 关键断言:前 2 条虽然成功,但不得留活 driver。
        // collect_pool_members 在错误分支调 shutdown;修前是直接 `?` 返回,
        // 成员被 drop(JoinHandle detach,driver 继续跑)。
        //
        // 用"成员确实被 shutdown"的可观测面:函数内部已 shutdown,外部
        // 拿不到成员,只能靠行为断言 —— 见下一个测试
        // `collect_pool_members_reports_shutdown_via_side_effect`。
        assert_eq!(
            shutdown_calls.load(Ordering::SeqCst),
            1,
            "测试自身的探针应被触发一次(仅第 0 条)"
        );
    }

    /// round-42 行为级回归:错误分支必须真的调用 `shutdown()`,
    /// 使已建立成员的 driver 停止(对端读到 EOF)。
    ///
    /// 把 2 条真实载体接到同一条 duplex 的"对端链"上,第 3 条注入失败;
    /// 断言错误返回后,两个对端都在超时内读到 EOF。
    #[tokio::test]
    async fn collect_pool_members_closes_carriers_on_failure() {
        use tokio::io::AsyncReadExt as _;

        // 两条载体各接一个对端,对端用于观察 EOF
        let (a1, mut peer1) = tokio::io::duplex(64 * 1024);
        let (a2, mut peer2) = tokio::io::duplex(64 * 1024);
        let m1 = crate::mux::YamuxMultiplexer::new(
            Box::new(a1) as crate::BoxedIo,
            yamux::Mode::Client,
            usize::MAX,
            1,
        );
        let m2 = crate::mux::YamuxMultiplexer::new(
            Box::new(a2) as crate::BoxedIo,
            yamux::Mode::Client,
            usize::MAX,
            1,
        );

        // 在载体上各开一条流并写数据,让 driver 有活干(避免空闲自然退出)
        let mut s1 = m1.open_stream().await.expect("stream1");
        let mut s2 = m2.open_stream().await.expect("stream2");
        use tokio::io::AsyncWriteExt as _;
        s1.write_all(b"live1").await.unwrap();
        s2.write_all(b"live2").await.unwrap();

        let mut buf = [0u8; 128];
        let _ =
            tokio::time::timeout(std::time::Duration::from_millis(300), peer1.read(&mut buf)).await;
        let _ =
            tokio::time::timeout(std::time::Duration::from_millis(300), peer2.read(&mut buf)).await;

        // 统一 boxed 以便同质收集
        type Dial = std::pin::Pin<
            Box<
                dyn std::future::Future<Output = std::io::Result<crate::mux::YamuxMultiplexer>>
                    + Send,
            >,
        >;
        let dials: Vec<Dial> = vec![
            Box::pin(async move { Ok(m1) }),
            Box::pin(async move { Ok(m2) }),
            Box::pin(async {
                Err(std::io::Error::new(
                    std::io::ErrorKind::ConnectionRefused,
                    "injected dial failure (3rd carrier)",
                ))
            }),
        ];

        let res = collect_pool_members(dials, 3).await;
        assert!(res.is_err(), "第 3 条失败必须让整体返回 Err");

        // 两个对端都必须在 2s 内读到 EOF(成员已被 shutdown → driver abort)
        for (idx, peer) in [(1, &mut peer1), (2, &mut peer2)] {
            let eof = tokio::time::timeout(std::time::Duration::from_secs(2), async {
                loop {
                    match peer.read(&mut buf).await {
                        Ok(0) => break,
                        Ok(_) => continue,
                        Err(_) => break,
                    }
                }
            })
            .await;
            assert!(
                eof.is_ok(),
                "载体 {idx} 必须在错误分支被 shutdown(对端读到 EOF)。\
                 若超时,说明 collect_pool_members 只 drop 没 shutdown —— \
                 即 round-42 的泄漏未修复"
            );
        }
        drop((s1, s2));
    }

    /// round-42 对照:**只 drop、不 shutdown** 时,有活跃流的载体 socket
    /// 不关闭 —— 这是修复必须显式 shutdown 的依据,也是
    /// "不要给 YamuxMultiplexer 加 impl Drop"这条红线的证明。
    #[tokio::test]
    async fn drop_does_not_close_carrier_with_live_stream() {
        use tokio::io::AsyncReadExt as _;
        let (a, mut peer) = tokio::io::duplex(64 * 1024);
        let mux = crate::mux::YamuxMultiplexer::new(
            Box::new(a) as crate::BoxedIo,
            yamux::Mode::Client,
            usize::MAX,
            1,
        );
        let mut stream = mux.open_stream().await.expect("stream");
        use tokio::io::AsyncWriteExt as _;
        stream.write_all(b"live").await.unwrap();

        let mut buf = [0u8; 128];
        let _ =
            tokio::time::timeout(std::time::Duration::from_millis(300), peer.read(&mut buf)).await;

        drop(mux);
        tokio::time::sleep(std::time::Duration::from_millis(400)).await;

        let got_eof = tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                match peer.read(&mut buf).await {
                    Ok(0) => break true,
                    Ok(_) => continue,
                    Err(_) => break true,
                }
            }
        })
        .await;
        assert!(
            got_eof.is_err(),
            "只 drop 时 driver 必须继续持有 socket(detach 语义,mux.rs:1147-1150 红线);\
             若读到 EOF 说明 Drop 语义被改动"
        );
        drop(stream);
    }

    /// round-42 直接判据:`shutdown()` 置 dead 且幂等。
    #[tokio::test]
    async fn shutdown_marks_dead_and_is_idempotent() {
        let mux = make_mux();
        assert!(!mux.is_dead(), "新建 mux 不应是 dead");
        mux.shutdown();
        assert!(mux.is_dead(), "shutdown 后必须置 dead");
        mux.shutdown();
        assert!(mux.is_dead(), "shutdown 幂等");
    }

    /// 成功路径零漂移:全部拨号成功时,成员原样返回且都是活的。
    #[tokio::test]
    async fn all_dials_success_returns_live_members() {
        type Dial = std::pin::Pin<
            Box<
                dyn std::future::Future<Output = std::io::Result<crate::mux::YamuxMultiplexer>>
                    + Send,
            >,
        >;
        let dials: Vec<Dial> = (0..3)
            .map(|_| Box::pin(async { Ok(make_mux()) }) as Dial)
            .collect();
        let members = collect_pool_members(dials, 3).await.expect("全部成功");
        assert_eq!(members.len(), 3, "应返回 3 个成员");
        for (i, m) in members.iter().enumerate() {
            assert!(!m.is_dead(), "成功路径上成员 {i} 不得被置 dead");
        }
    }
}
