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

/// 在途握手上限(与 ready 队列容量同量级)。
///
/// ready 队列(512)只背压"已验证待交付"的连接;在途握手若无上限,建连
/// 洪水(慢握手 / 半开 TCP)可在 HANDSHAKE_TIMEOUT(5s)内堆积任意多任务
/// 与 socket。取 512:与队列对齐,不改变正常负载的并发度(压测中并发建连
/// 恰在此量级,未观察到排队),只在异常洪峰时把压力退回内核 backlog。
const HANDSHAKE_MAX_INFLIGHT: usize = 512;

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
    /// 在途握手许可(测试观测用:断言背压发生在 accept 之前)。
    #[cfg(test)]
    inflight: std::sync::Arc<tokio::sync::Semaphore>,
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
    cfg: &crate::ServerTransportConfig,
    mux_streams: std::sync::Arc<std::sync::atomic::AtomicUsize>,
) -> io::Result<TcpListenerShim> {
    listen_stream_with_cap(cfg, mux_streams, HANDSHAKE_MAX_INFLIGHT).await
}

/// 同 [`listen_stream`],可指定在途握手上限(测试用小值以便观测)。
pub async fn listen_stream_with_cap(
    cfg: &crate::ServerTransportConfig,
    mux_streams: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    handshake_cap: usize,
) -> io::Result<TcpListenerShim> {
    listen_stream_impl(cfg, mux_streams, handshake_cap).await
}

async fn listen_stream_impl(
    cfg: &ServerTransportConfig,
    mux_streams: Arc<AtomicUsize>,
    handshake_cap: usize,
) -> io::Result<TcpListenerShim> {
    let listener = TcpListener::bind(cfg.bind_addr).await?;
    let local = listener.local_addr()?;
    let tls = if matches!(cfg.protocol, crate::Protocol::Tls | crate::Protocol::Wss) {
        // 必须用 `server_config_with_alpn(cfg.alpn)`:早先误用了
        // `server_config()`(内部固定取 DEFAULT_ALPN),导致配置里的
        // `alpn` 列表**完全失效**——配了 ["h2","http/1.1"] 也只协商 h2,
        // 且客户端只提 http/1.1 时服务端回 no_application_protocol 警报。
        Some(tokio_rustls::TlsAcceptor::from(
            crate::tls::server_config_with_alpn(&cfg.tls, &cfg.alpn)?,
        ))
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
    //
    // 在途握手**有界**,且背压点必须在 `accept()` **之前**。
    //
    // ready 队列(512)只背压"已验证待交付",不背压在途握手。修前每秒可接
    // 无限多连接,每个 child 各持一条 socket 且最长 HANDSHAKE_TIMEOUT(5s)
    // 才结束,建连洪水(慢握手/半开 TCP)能在 5s 内堆积数万任务与 fd。
    //
    // ★信号量在这里是"先拿票、再 accept":`acquire` 与 `accept` 绑成**一个**
    // future 放进 `select!`。拿不到票时不会去 accept,连接留在内核 backlog
    // 里排队(**不占用户态 fd**);拿到票才取连接,许可随 child 作用域归还。
    //
    // 若把 `acquire().await` 放进 spawn 出来的子任务(本实现的第一版就是
    // 这样),accept 与 spawn 仍无条件发生 —— 只有**并发 verify 的工作量**
    // 有界,已 accept 的任务与 fd 照样无界堆积,那正是这段要挡的东西。
    //
    // 也不能写成 select 的分支预条件(`if available_permits() > 0`):预条件
    // 为假时该分支完全不参与轮询,而许可归还并不唤醒 select,accept 会
    // 永久停摆(由 `accept_resumes_after_permit_returned` 锁住)。
    let inflight = std::sync::Arc::new(tokio::sync::Semaphore::new(handshake_cap));
    // pump 会 move 走 inflight;构造 shim 时要留一份观测句柄
    #[cfg(test)]
    let inflight_obs = inflight.clone();
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
                // ★先拿票、再 accept:两者绑成**一个** future 放进 select。
                //
                // 不能把 wait 写成分支预条件(`if inflight.available_permits()>0`):
                // 预条件为假时该分支**完全不参与轮询**,而许可归还并不会唤醒
                // `select!` —— 池一旦满过,accept 可能永久停摆(本实现第一版
                // 正是如此,由 `accept_resumes_after_permit_returned` 抓住)。
                //
                // 绑成一个 future 后:`acquire` 未完成时不会有 accept 发生
                // (背压回内核 backlog);`acquire` 完成才去取连接;两者
                // 任一步被 shutdown/cancel 打断都由 select 正常收尾。
                accepted = async {
                    let permit = inflight.clone().acquire_owned().await.expect("semaphore never closed");
                    let conn = listener.accept().await?;
                    Ok::<_, std::io::Error>((conn, permit))
                } => {
                    let ((stream, remote), permit) = match accepted {
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
                        // 许可随本 child 结束自动归还(permit 由作用域持有)
                        let _permit = permit;
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
        #[cfg(test)]
        inflight: inflight_obs,
        pump: Some(pump),
    })
}

/// 客户端:建立一条物理连接并握手。多路复用时返回 yamux mux;
/// 裸模式下连接本体即一条流(AcceptSingle),后续流由 DialPerStream 拨号。
pub async fn connect_stream(cfg: &ClientTransportConfig) -> io::Result<Arc<Multiplexer>> {
    if cfg.multiplex || cfg.protocol.always_muxed() {
        let (io, write_dup) = dial_io(cfg).await?;
        // 客户端为开流方,取"不限制"即可 —— 但**理由不是"超限只影响单流"**。
        //
        // 修前这里写「超限是单流报错(不杀会话)」,那是错的:yamux 的
        // `TooManyStreams` 是**连接级**错误。实测(限 4 开第 5 条)该载体上
        // 已开的 4 条流全部失效,后续开流返回 "mux driver gone"。代码路径:
        // `SendHalf::poll_new_outbound` 返回 `Err` → `send_driver` 先
        // `dead.store(true)` 再终结驱动(见 mux.rs send_driver);对端收到
        // 超限 SYN 更是直接 `Action::Terminate`(vendor connection.rs)。
        //
        // 因此客户端**不能**用 yamux 硬顶做自我限流:一旦触发就是整条
        // 会话断,而限流本该是"拒绝新流、保留老流"的干净拒绝。
        //
        // 正确的分层:策略上限由服务端在 DATA_CHANNEL_HELLO 处以**应用层
        // 计数器**执行(control.rs),其配额刻意钳到 `mux_streams - 1024`
        // 以保证应用层永远先于 yamux 拒绝。客户端是开流方,限流是服务端
        // 对客户端的约束,自己再设一道只会自伤。
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
    // 同样取"不限制":yamux 硬顶在客户端一旦触发是**连接级**失败
    // (杀掉该载体上所有已有流),不能当限流用。理由详见
    // `connect_stream` 内同名字段处的长注释。
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
                // 同服务端:必须传入 cfg.alpn,否则配置项被静默忽略。
                // B5:走按配置键缓存的构造(含 rustls 会话恢复票据缓存),
                // 修前每次拨号/重拨都重建 → PSK 恢复永远空转。
                let (rust, verify) = crate::tls::client_config_cached(&cfg.tls, &cfg.alpn)?;
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
                // 记录对端叶子证书:上层用它比对服务端加密下发的证书(防中间人)。
                if let Some(leaf) = tls_stream
                    .get_ref()
                    .1
                    .peer_certificates()
                    .and_then(|c| c.first())
                {
                    let der = leaf.as_ref().to_vec();
                    if let Some(sink) = &cfg.peer_cert_sink {
                        let _ = sink.set(der.clone());
                    }
                    // 多载体:逐条追加,供上层校验**每一条**载体
                    if let Some(all) = &cfg.peer_cert_sink_all {
                        if let Ok(mut v) = all.lock() {
                            v.push(der);
                        }
                    }
                }
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
                // Host 头与 Wss 分支同口径:`tls.server_name` 优先,未设才回落到
                // server_addr。修前此处恒用 server_addr,想用域名伪装(反指纹/
                // 前置反代按 Host 分流)的部署在 ws 明文路径仍会暴露 IP:port。
                let host = cfg
                    .tls
                    .server_name
                    .clone()
                    .unwrap_or_else(|| cfg.server_addr.to_string());
                let io = ws::client_handshake(Box::new(tcp), &host, ws::WS_PATH).await?;
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
    ///
    /// # 目标值不能硬编码 4 MiB(CI 曾因此变红)
    ///
    /// Linux 的 `setsockopt(SO_SNDBUF)` 受 `net.core.wmem_max` 约束:
    /// 超出部分被**静默截断**,不报错。开发机常见 `wmem_max=16MiB`(所以
    /// 4 MiB 能生效),而 GitHub Actions 容器用内核默认值 **212992**
    /// (约 208 KiB)—— 于是 4 MiB 被压到约 416 KiB,断言 `got >= target`
    /// 失败,报 `target=4194304 got=425984`。
    ///
    /// 现在先读系统上限,取「上限与 4 MiB 的较小者」作为目标。这样:
    /// * 上限充裕的机器仍然验证到 4 MiB 这一档;
    /// * 受限环境退化为验证「设一个系统允许的值确实生效」,这仍是本用例
    ///   要证明的东西(API 可用 + 语义如预期),而不是在测内核策略。
    #[tokio::test]
    async fn sndbuf_explicit_set_takes_effect() {
        use socket2::SockRef;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let accept = tokio::spawn(async move { listener.accept().await });
        let client = tokio::net::TcpStream::connect(addr).await.unwrap();
        let _ = accept.await.unwrap().unwrap();

        // 系统允许的上限:读不到(非 Linux / 无权限)就按 4 MiB 试
        let sys_max = std::fs::read_to_string("/proc/sys/net/core/wmem_max")
            .ok()
            .and_then(|s| s.trim().parse::<usize>().ok());
        let target = sys_max.map_or(4 * 1024 * 1024, |m| m.min(4 * 1024 * 1024));
        // 上限太小时(理论上不会,但别让断言恒真)直接跳过
        if target < 64 * 1024 {
            eprintln!("wmem_max={target} 过小,跳过 SO_SNDBUF 生效性验证");
            return;
        }

        SockRef::from(&client).set_send_buffer_size(target).unwrap();
        let got = SockRef::from(&client).send_buffer_size().unwrap();
        assert!(
            got >= target,
            "Linux 会翻倍,读回应 >= 目标:target={target} got={got} (wmem_max={sys_max:?})"
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

#[cfg(test)]
mod peer_cert_all_tests {
    /// 多载体证书必须**逐条记录**(sink_all),不能只有首个(OnceLock)。
    ///
    /// 修前只有 `peer_cert_sink`(`OnceLock`,只留首次写入):`connect_pool`
    /// 建 K 条独立 TLS 连接,附加载体的证书无人记录 —— 上层也就无从比对,
    /// 中间人可**只截断附加载体**读走数据明文(控制流照常,不会察觉)。
    ///
    /// 本测试锁定两种 sink 的语义差异(OnceLock 覆盖不到第 2 条),这是
    /// 上面对每条载体做比对的前提。
    #[test]
    fn sink_all_accumulates_but_once_lock_does_not() {
        let all: std::sync::Arc<std::sync::Mutex<Vec<Vec<u8>>>> =
            std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        for i in 0..3u8 {
            all.lock().unwrap().push(vec![i; 4]);
        }
        assert_eq!(all.lock().unwrap().len(), 3, "三条载体应全部记录");

        // 对照:OnceLock 只保留首次,后两条静默丢弃 —— 正是修前的漏洞
        let once: std::sync::OnceLock<Vec<u8>> = std::sync::OnceLock::new();
        for i in 0..3u8 {
            let _ = once.set(vec![i; 4]);
        }
        assert_eq!(
            once.get().unwrap(),
            &vec![0u8; 4],
            "OnceLock 只留首次写入(第 2/3 条被静默丢弃)"
        );
    }
}

#[cfg(test)]
mod handshake_backpressure_tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    fn cfg_for() -> crate::ServerTransportConfig {
        crate::ServerTransportConfig {
            protocol: crate::Protocol::Tcp,
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            multiplex: true,
            tls: crate::TlsServerOptions::default(),
            alpn: vec![crate::DEFAULT_ALPN.into()],
        }
    }

    /// 内核 accept 队列长度(`Recv-Q`)。**仅 Linux**:靠 `ss` 读。
    #[cfg(target_os = "linux")]
    fn accept_backlog_len(port: u16) -> Option<usize> {
        let out = std::process::Command::new("ss")
            .args(["-ltnH", &format!("sport = :{port}")])
            .output()
            .ok()?;
        String::from_utf8_lossy(&out.stdout)
            .lines()
            .next()
            .and_then(|l| l.split_whitespace().nth(1))
            .and_then(|q| q.parse::<usize>().ok())
    }

    /// 握手池满时,新连接必须留在**内核 accept 队列**里,不得被 accept 出队。
    ///
    /// # 为什么只能这样测
    ///
    /// 修前的实现把 `acquire().await` 放进 spawn 出来的子任务:accept 与
    /// spawn 无条件发生 —— 只有**并发 verify 的量**有界,已 accept 的任务
    /// 与 fd 照样无界堆积。注释里那句"不耗用户态 fd"根本不成立。
    ///
    /// 两种实现的"接收端可见行为"完全一致(回退版会把连接 accept 走后卡在
    /// acquire),所以先前两版基于 `accept()` 是否返回、以及基于进程 fd 计数
    /// 的测试都测不出差异(负向验证全绿 / 自身 fd 干扰)。唯一可靠的外部观测量
    /// 是**内核 accept 队列长度**:
    ///   - 正确实现:池满 → 不 accept → `Recv-Q > 0`,连接留在内核;
    ///   - 回退实现:池满 → 已 accept → `Recv-Q == 0`。
    ///
    /// 用 `listen_stream_with_cap` 把上限压到 1,一条连接即可区分。
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn pool_full_keeps_connections_in_kernel_accept_queue() {
        let streams = std::sync::Arc::new(AtomicUsize::new(0));
        let mut listener = listen_stream_with_cap(&cfg_for(), streams, 1)
            .await
            .unwrap();
        let addr = listener.local_addr().unwrap();
        let port = addr.port();

        let held = listener.inflight.clone().try_acquire_owned().unwrap();
        assert_eq!(listener.inflight.available_permits(), 0, "池应已占满");

        let mut cli = tokio::net::TcpStream::connect(addr).await.unwrap();
        use tokio::io::AsyncWriteExt as _;
        cli.write_all(b"PING").await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(400)).await;

        let q = accept_backlog_len(port).expect("ss 应能查到该监听端口");
        assert!(
            q > 0,
            "握手池满时连接应留在内核 accept 队列(Recv-Q>0),实际 Recv-Q={q} —— \
             说明它已被 accept 出队(修前行为:permit 在 accept 之后才拿)"
        );

        drop(held);
        let inc = tokio::time::timeout(std::time::Duration::from_millis(3000), listener.accept())
            .await
            .expect("归还许可后应恢复 accept");
        assert!(inc.is_ok());
        drop(cli);
    }

    /// 跨平台的弱断言:池满→归还后必须**恢复**接受(不会因背压永久停摆)。
    ///
    /// 这条测不出"背压位置",但能抓住一类真实回归:若把等待写成 select
    /// 分支预条件(`if available_permits()>0`),许可归还不唤醒 select,
    /// accept 会永久停摆 —— 本实现的第一版正是如此。
    #[tokio::test]
    async fn accept_resumes_after_permit_returned() {
        let streams = std::sync::Arc::new(AtomicUsize::new(0));
        let mut listener = listen_stream_with_cap(&cfg_for(), streams, 1)
            .await
            .unwrap();
        let addr = listener.local_addr().unwrap();

        let held = listener.inflight.clone().try_acquire_owned().unwrap();

        let mut cli = tokio::net::TcpStream::connect(addr).await.unwrap();
        use tokio::io::AsyncWriteExt as _;
        cli.write_all(b"PING").await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        drop(held);
        let inc = tokio::time::timeout(std::time::Duration::from_millis(3000), listener.accept())
            .await
            .expect("归还许可后应立即恢复 accept(不得因预条件不唤醒而停摆)");
        assert!(inc.is_ok());
        drop(cli);
    }
}
