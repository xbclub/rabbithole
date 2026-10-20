//! TCP 监听生命周期回归(独占文件,不与 kcp/mux/ws 回归耦合)。
//!
//! 契约(对应 benchmark/runs/goal-repair/kcp-main-review/SHUTDOWN-ADDENDUM.md
//! "公共 listener 在零会话时 drop 仍占端口"与 TCP-QUEUED-REVIEW.md
//! "已握手但未 accept 的 mux 泄漏"):
//! 1. `tcp::listen_stream` 返回的句柄是监听资源的唯一所有者:drop 句柄后
//!    不再永久监听、不继续接新连接,原地址可重绑(有界,不等待下一个连接)。
//! 2. 已成功 accept 出去交付给消费者的 stream/mux 不受监听句柄 drop 影响,
//!    仍可双向交换(与 yamux open_rx 不杀存量流的红线一致)。
//! 3. 挂起中的握手任务和 ready 队列在无人消费时不泄漏:drop 后在途连接被
//!    及时回收(远早于 5s 握手超时,证明是取消而非等超时),socket 释放。
//! 4. 已完成全部验证(TLS/WS/peek)进入 ready 队列、但从未被 accept 消费的
//!    连接,drop 句柄后必须回收——未交付的只有裸 IO,不得存在 detached
//!    yamux driver 拖住 socket。
//!
//! 测试全部有 deadline,默认参与 `cargo test`(无 #[ignore]);
//! 首轮三项在 pump 脱离所有者的原实现下为红;第四/五项(未交付回收)在
//! "post_tls 提前 wrap 出 detached driver"的首版补修下为红。

use rh_transport::tcp;
use rh_transport::{connect, ClientTransportConfig, Protocol, ServerTransportConfig};
use std::io;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

fn tcp_cfg() -> ServerTransportConfig {
    ServerTransportConfig {
        protocol: Protocol::Tcp,
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        multiplex: false,
        ..Default::default()
    }
}

fn mux_streams() -> Arc<AtomicUsize> {
    Arc::new(AtomicUsize::new(1024))
}

/// 有界重试 bind:修复后毫秒级成功;原实现永远 AddrInUse,超 deadline 即失败。
async fn bind_with_deadline(addr: std::net::SocketAddr, budget: Duration) -> TcpListener {
    let deadline = tokio::time::Instant::now() + budget;
    loop {
        match TcpListener::bind(addr).await {
            Ok(l) => return l,
            Err(e) if e.kind() == io::ErrorKind::AddrInUse => {
                if tokio::time::Instant::now() >= deadline {
                    panic!(
                        "bind {addr} still AddrInUse after {budget:?}: \
                         listener socket not released on drop"
                    );
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            Err(e) => panic!("unexpected bind error on {addr}: {e}"),
        }
    }
}

/// 对端被关闭的观察:EOF(0)或 RST(Reset/Aborted)都算"已被回收",
/// 真正的失败是一直读到 deadline 还挂着。
async fn expect_closed_soon(
    what: &str,
    read: impl std::future::Future<Output = io::Result<usize>>,
    budget: Duration,
) {
    match tokio::time::timeout(budget, read).await {
        Ok(Ok(0)) => {}
        Ok(Err(e))
            if matches!(
                e.kind(),
                io::ErrorKind::ConnectionReset | io::ErrorKind::ConnectionAborted
            ) =>
        {
            // RST 也是及时回收的表现
        }
        Ok(Ok(n)) => panic!("{what}: expected closure, read {n} bytes of stale data"),
        Ok(Err(e)) => panic!("{what}: unexpected read error: {e}"),
        Err(_) => panic!(
            "{what}: connection still open after {budget:?} — \
             pending handshake not reclaimed on listener drop"
        ),
    }
}

/// post-drop 探针的源端口守卫(LINUX-LISTENER-PROBE-REVIEW.md 的平台事实)。
///
/// Linux 上不带显式 bind 的 `connect` 由内核自动分配临时源端口,该端口可能
/// 恰好等于刚随 listener 释放的目的端口,形成 127.0.0.1:P ↔ 127.0.0.1:P 的
/// TCP 自连接:connect 成功、读到自己写的数据、永不 EOF。这会让回归 3 的
/// "post-drop probe 2s 无 EOF"在 listener 已正确拆除时也 panic,把平台混杂
/// 误判为产品泄漏(真实失败与自连接混杂无法区分,故原失败保留为未定归因)。
///
/// 这里显式 `TcpSocket::new_v4` + bind 127.0.0.1:0,确认源端口 != 服务端口
/// (bind0 撞上刚释放的服务端口概率极小,有界重选),使 4 元组 local != peer,
/// 自连接在构造上不可能。connect 的结果语义不变:被拒(Refused)或 Ok 后仍
/// 必须在原 2s 内观察到关闭。
async fn connect_with_distinct_source(server_addr: std::net::SocketAddr) -> io::Result<TcpStream> {
    const MAX_ATTEMPTS: usize = 8;
    let mut last_collision: Option<io::Error> = None;
    for _ in 0..MAX_ATTEMPTS {
        let socket = tokio::net::TcpSocket::new_v4()?;
        socket.bind("127.0.0.1:0".parse().unwrap())?;
        let local = socket.local_addr()?;
        if local.port() == server_addr.port() {
            // 极小概率:bind0 恰好选中刚释放的服务端口,销毁重选
            drop(socket);
            last_collision = Some(io::Error::other(format!(
                "probe source port collided with server port {} ({} attempts)",
                server_addr.port(),
                MAX_ATTEMPTS
            )));
            continue;
        }
        return socket.connect(server_addr).await;
    }
    Err(last_collision.expect("bounded retry loop must record a collision"))
}

/// 回归 1:零会话 public bind → drop 句柄 → 原地址重绑。
/// 复刻主代理确定性复现(listener_drop_probe.rs 的 TCP 部分):
/// 原实现 pump 脱离所有者持有 listener,AddrInUse 永不解除 → 红。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn zero_session_drop_releases_bind_addr() {
    let cfg = tcp_cfg();
    let listener = tcp::listen_stream(&cfg, mux_streams())
        .await
        .expect("listen");
    let addr = listener.local_addr().expect("local_addr");
    // 零会话:不做任何连接,直接 drop 唯一句柄
    drop(listener);
    tokio::time::timeout(Duration::from_secs(5), async move {
        let rebound = bind_with_deadline(addr, Duration::from_secs(3)).await;
        assert_eq!(rebound.local_addr().expect("rebind local_addr"), addr);
    })
    .await
    .expect("test deadline (5s)");
}

/// 回归 2:accept 成功交付后 drop 监听,已交付连接仍可双向交换;
/// 全部拆除后端口可重绑(原实现最后一步 AddrInUse → 红;
/// 若修复错误地关闭已交付连接,双向交换立即失败 → 红)。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn accepted_stream_survives_listener_drop() {
    let cfg = tcp_cfg();
    let mut listener = tcp::listen_stream(&cfg, mux_streams())
        .await
        .expect("listen");
    let addr = listener.local_addr().expect("local_addr");

    let body = async {
        let mut client = TcpStream::connect(addr).await.expect("client connect");
        let incoming = tokio::time::timeout(Duration::from_secs(2), listener.accept())
            .await
            .expect("accept deadline (2s)")
            .expect("accept ok");
        // 消费者已拿到该物理连接(AcceptSingle 的唯一一条流)
        let mut server_io = incoming.mux.accept().await.expect("take accepted stream");

        // 此刻才 drop 监听句柄:已交付的 stream 不属于监听器
        drop(listener);
        drop(incoming);

        // 双向交换 1:client → server(drop 监听之后)
        const C2S: &[u8] = b"ping-after-listener-drop";
        const S2C: &[u8] = b"pong-after-listener-drop";
        client
            .write_all(C2S)
            .await
            .expect("client write after listener drop");
        let mut buf = [0u8; 32];
        server_io
            .read_exact(&mut buf[..C2S.len()])
            .await
            .expect("server read after listener drop");
        assert_eq!(&buf[..C2S.len()], C2S);

        // 双向交换 2:server → client
        server_io
            .write_all(S2C)
            .await
            .expect("server write after listener drop");
        client
            .read_exact(&mut buf[..S2C.len()])
            .await
            .expect("client read after listener drop");
        assert_eq!(&buf[..S2C.len()], S2C);

        // 全部拆除后,监听地址必须可重绑(原实现在此 AddrInUse → 红)
        drop(server_io);
        drop(client);
        let rebound = bind_with_deadline(addr, Duration::from_secs(3)).await;
        assert_eq!(rebound.local_addr().expect("rebind local_addr"), addr);
    };
    tokio::time::timeout(Duration::from_secs(8), body)
        .await
        .expect("test deadline (8s)");
}

/// 回归 3:握手挂在 peek(multiplex 监听等待首 4 字节)时 drop 句柄——
/// 无任务/socket 残留:
///   a) 不再接新连接(新连接要么被拒,要么立即被关,绝不能挂 5s);
///   b) 在途静默连接被及时回收(deadline 2s « 5s peek 超时,证明是取消);
///   c) 端口可重绑(pump 不再持有 listener)。
/// 原实现:新连接照常接入并挂满握手超时、在途连接等满 5s、端口 AddrInUse → 红。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn drop_with_pending_handshake_leaves_no_residue() {
    let mut cfg = tcp_cfg();
    cfg.multiplex = true; // 服务端会 peek 首 4 字节,静默客户端 → 握手挂起
    let listener = tcp::listen_stream(&cfg, mux_streams())
        .await
        .expect("listen");
    let addr = listener.local_addr().expect("local_addr");

    // 静默客户端:连接但不发任何字节 → 服务端握手停在 peek_prefix
    let mut silent = TcpStream::connect(addr)
        .await
        .expect("silent client connect");
    // 给泵一个调度窗口把该连接 accept 出去并进入 peek
    tokio::time::sleep(Duration::from_millis(300)).await;

    let body = async {
        drop(listener);

        // a) 不再接新连接:被拒(Refused)可接受;极端竞态下被 accept 也必须
        //    立即被关闭,而不是当成正常接入等握手。
        //    探针显式 bind 源端口且 != 服务端口,排除 Linux TCP 自连接混杂;
        //    失败消息记录 local/peer tuple,便于区分真泄漏与平台行为。
        match connect_with_distinct_source(addr).await {
            Err(e) => {
                assert!(
                    matches!(
                        e.kind(),
                        io::ErrorKind::ConnectionRefused
                            | io::ErrorKind::ConnectionReset
                            | io::ErrorKind::ConnectionAborted
                    ),
                    "new connection after listener drop must be refused/reset, got: {e}"
                );
            }
            Ok(mut probe) => {
                let local = probe.local_addr().ok();
                let peer = probe.peer_addr().ok();
                assert_ne!(
                    local, peer,
                    "post-drop probe must never be a TCP self-connect (local={local:?} peer={peer:?})"
                );
                // 连接已建立但必须拆除。只读不足以证明泄漏:drop 到泵 future
                // 实际析构(监听 fd 关闭)之间存在调度窗口,SYN 在窗口内完成握手
                // 的连接此后只归内核——Linux 5.15 对这类"拆除竞态孤儿"可能不向
                // 对端发任何段(inet_csk_listen_stop 的 ANK 注释/inet_child_forget
                // 的半开 req 路径),只读会静默挂起,把平台行为误判为产品持有。
                // 先写后读:写入强制内核裁决四元组——对端 socket 已不存在必回
                // RST(2s 内观察到关闭);若真有用户空间持有(泵未被 abort/child
                // 泄漏/外进程重绑同端口存活),写入被持有方缓冲/消费,读仍挂 →
                // 仍红。不 sleep、不放大超时、不改变拒绝语义。
                let wrote = probe.write_all(b"probe123").await;
                let mut b = [0u8; 8];
                let what = format!(
                    "post-drop probe connection after write (local={local:?} peer={peer:?} wrote={wrote:?})"
                );
                match wrote {
                    // 写阶段即观察到对端拆除(如 darwin 对 backlog 残留发 RST):
                    // closed,与读到 Reset/EOF 等价
                    Err(e)
                        if matches!(
                            e.kind(),
                            io::ErrorKind::ConnectionReset
                                | io::ErrorKind::BrokenPipe
                                | io::ErrorKind::ConnectionAborted
                        ) => {}
                    Err(e) => panic!("{what}: unexpected write error (not a close signal): {e}"),
                    Ok(()) => {
                        expect_closed_soon(&what, probe.read(&mut b), Duration::from_secs(2)).await;
                    }
                }
            }
        }

        // b) 在途握手连接被及时回收(2s 远小于 5s peek 超时)
        let mut b = [0u8; 8];
        expect_closed_soon(
            "pending handshake connection",
            silent.read(&mut b),
            Duration::from_secs(2),
        )
        .await;

        // c) 端口可重绑
        let rebound = bind_with_deadline(addr, Duration::from_secs(3)).await;
        assert_eq!(rebound.local_addr().expect("rebind local_addr"), addr);
    };
    tokio::time::timeout(Duration::from_secs(8), body)
        .await
        .expect("test deadline (8s)");
}

// ===== 第二轮回归(主代理 TCP-QUEUED-REVIEW.md):已验证但未交付的回收 =====

/// 回归 4:完成全部验证(TLS/WS/peek)进入 ready 队列、但从未被 accept
/// 消费的连接,在监听句柄 drop 后必须被回收。
///
/// 因果前置(按评审允许的方式适配):复用主代理 tcp_queued_mux_probe_r2.rs
/// 的标准 yamux Ping 帧(v0/Ping/SYN/id=123,首 4 字节非 RHTP → peek 判定
/// yamux),随后直接观测 ready 队列深度==1——证明该连接已越过全部验证并
/// 入队、从未交付。延迟构造 mux 后 accept 前不存在 yamux driver,旧探针
/// "先收到 Pong"的前置不再适用;ready 观测是更直接的前置(证明队列状态,
/// 而非 driver 存在)。
/// 最终断言不弱化:drop 后客户端 2s 内 EOF/复位、端口 3s 内可重绑。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn completed_handshake_unconsumed_reclaimed_on_drop() {
    let mut cfg = tcp_cfg();
    cfg.multiplex = true;
    let listener = tcp::listen_stream(&cfg, mux_streams())
        .await
        .expect("listen");
    let addr = listener.local_addr().expect("local_addr");

    // 标准 yamux Ping(SYN,id=123),与主代理 r2 探针逐字节一致
    let mut client = TcpStream::connect(addr).await.expect("client connect");
    let ping: [u8; 12] = [0, 2, 0, 1, 0, 0, 0, 0, 0, 0, 0, 123];
    client.write_all(&ping).await.expect("client ping");

    // 因果前置:ready 队列观测到该已验证连接(未消费、无消费者)
    let queued_by = tokio::time::Instant::now() + Duration::from_secs(3);
    while listener.ready_len() == 0 {
        assert!(
            tokio::time::Instant::now() < queued_by,
            "verified conn never reached ready queue within 3s (ready_len={})",
            listener.ready_len()
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(listener.ready_len(), 1, "exactly one queued conn expected");

    let body = async {
        // 从未调用 accept():队列中的对象没有任何消费者
        drop(listener);
        let mut one = [0u8; 1];
        expect_closed_soon(
            "unaccepted verified conn",
            client.read(&mut one),
            Duration::from_secs(2),
        )
        .await;
        let rebound = bind_with_deadline(addr, Duration::from_secs(3)).await;
        assert_eq!(rebound.local_addr().expect("rebind local_addr"), addr);
    };
    tokio::time::timeout(Duration::from_secs(8), body)
        .await
        .expect("test deadline (8s)");
}

/// 回归 5:ready 队列满(容量 512)时 drop——已排队与背压停车两路未交付
/// 连接都必须回收。
///
/// 因果链:
/// 1) 512 个客户端各发 4 字节非 RHTP 前缀,观测 ready_len==512(队列确定满);
/// 2) 再接入 16 个 extras:其握手 child 完成验证后只能停在 send 背压上
///    (队列满,无人消费);
/// 3) 排空 513 个 accept(前 512 个按 FIFO 全是 fillers——它们的 send 在
///    ready_len==512 观测时已全部完成,严格早于 extras 任何 send),第 513
///    个必须来自 extras——证明确有 child 在满队列上停车等待;
/// 4) drop 监听:所有未交付客户端(extras 剩余 15 个,含背压停车与腾位后
///    入队者)必须 2s 内回收,端口 3s 内可重绑。
/// 需要约 1100 个 fd(528 连接 ×2 端)。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn queue_full_drop_reclaims_queued_and_backpressured() {
    let mut cfg = tcp_cfg();
    cfg.multiplex = true;
    let mut listener = tcp::listen_stream(&cfg, mux_streams())
        .await
        .expect("listen");
    let addr = listener.local_addr().expect("local_addr");

    const CAP: usize = 512; // tcp.rs ready 通道容量
    const EXTRAS: usize = 16;
    let prefix: [u8; 4] = [0, 2, 0, 1]; // 非 RHTP → peek 判定 yamux

    let mut clients: std::collections::HashMap<std::net::SocketAddr, TcpStream> =
        Default::default();

    // 1) 填满队列:全部完成验证并入队
    for _ in 0..CAP {
        let mut c = TcpStream::connect(addr).await.expect("filler connect");
        c.write_all(&prefix).await.expect("filler write");
        let a = c.local_addr().expect("filler local_addr");
        clients.insert(a, c);
    }
    let filled_by = tokio::time::Instant::now() + Duration::from_secs(15);
    while listener.ready_len() < CAP {
        assert!(
            tokio::time::Instant::now() < filled_by,
            "ready queue never filled: ready_len={} after 15s",
            listener.ready_len()
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    // 2) 满队列后接入 extras:child 完成验证后停在 send 背压
    let mut extra_addrs: std::collections::HashSet<std::net::SocketAddr> = Default::default();
    for _ in 0..EXTRAS {
        let mut c = TcpStream::connect(addr).await.expect("extra connect");
        c.write_all(&prefix).await.expect("extra write");
        let a = c.local_addr().expect("extra local_addr");
        extra_addrs.insert(a);
        clients.insert(a, c);
    }
    // 让 extras 的 child 完成验证并停车(正常情况微秒级)
    tokio::time::sleep(Duration::from_millis(300)).await;

    // 3) 排空 513 个,第 513 个必须来自 extras(背压停车的因果证明)
    let mut delivered: std::collections::HashSet<std::net::SocketAddr> = Default::default();
    let mut last_remote: Option<std::net::SocketAddr> = None;
    for _ in 0..=CAP {
        let inc = tokio::time::timeout(Duration::from_secs(5), listener.accept())
            .await
            .expect("drain accept deadline (5s)")
            .expect("drain accept ok");
        last_remote = Some(inc.remote);
        delivered.insert(inc.remote);
        drop(inc); // 已交付归消费者(本测试)所有,正常丢弃
    }
    let last = last_remote.expect("drained at least one conn");
    assert!(
        extra_addrs.contains(&last),
        "513th accept must come from an extra parked on the full queue, got filler/unknown {last}"
    );

    // 4) drop 监听:全部未交付连接必须回收
    let body = async {
        drop(listener);
        let mut undelivered = 0usize;
        for (a, mut c) in clients {
            if delivered.contains(&a) {
                continue; // 已交付:归消费者,不在此断言(交付路径由回归 2/6 覆盖)
            }
            undelivered += 1;
            let mut one = [0u8; 1];
            expect_closed_soon(
                &format!("undelivered conn {a}"),
                c.read(&mut one),
                Duration::from_secs(2),
            )
            .await;
        }
        assert!(
            undelivered >= EXTRAS - 1,
            "expected at least {} undelivered extras to reclaim, got {undelivered}",
            EXTRAS - 1
        );
        let rebound = bind_with_deadline(addr, Duration::from_secs(3)).await;
        assert_eq!(rebound.local_addr().expect("rebind local_addr"), addr);
    };
    tokio::time::timeout(Duration::from_secs(60), body)
        .await
        .expect("test deadline (60s)");
}

/// 守卫(附加,非主代理点名的两项红回归之一):延迟构造 mux 后,真实
/// yamux 客户端经 accept 交付的 mux 必须照常工作——包括 accept 之前已
/// 缓冲的帧(客户端先开流写数据,服务端才 accept),且交付后 drop 监听
/// 不影响该 mux(存量不杀红线)。本项在首版(急切构造)同样为绿,用于
/// 锁定重构不得破坏的交付路径语义。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delivered_yamux_mux_built_at_accept_still_works() {
    let mut cfg = tcp_cfg();
    cfg.multiplex = true;
    let mut listener = tcp::listen_stream(&cfg, mux_streams())
        .await
        .expect("listen");
    let addr = listener.local_addr().expect("local_addr");

    let c_cfg = ClientTransportConfig {
        protocol: Protocol::Tcp,
        server_addr: addr,
        multiplex: true,
        ..Default::default()
    };
    let client_mux = connect(&c_cfg).await.expect("client connect");
    let mut c_stream = client_mux.open_stream().await.expect("client open stream");
    // 先写:帧在服务端 accept 之前就已缓冲(PrependedIo + TCP 缓冲)
    c_stream
        .write_all(b"lazy-mux-echo")
        .await
        .expect("client write");

    let body = async {
        let inc = tokio::time::timeout(Duration::from_secs(3), listener.accept())
            .await
            .expect("accept deadline (3s)")
            .expect("accept ok");
        // 交付之后才 drop 监听:已交付 mux 归消费者,必须继续可用
        drop(listener);
        let mut s_stream = inc.mux.accept().await.expect("server accept stream");
        let mut buf = [0u8; 32];
        s_stream
            .read_exact(&mut buf[..13])
            .await
            .expect("server read pre-accept-buffered bytes");
        assert_eq!(&buf[..13], b"lazy-mux-echo");
        s_stream
            .write_all(b"lazy-mux-pong")
            .await
            .expect("server write");
        c_stream
            .read_exact(&mut buf[..13])
            .await
            .expect("client read echo");
        assert_eq!(&buf[..13], b"lazy-mux-pong");
    };
    tokio::time::timeout(Duration::from_secs(8), body)
        .await
        .expect("test deadline (8s)");
}

/// 守卫(机制锁定,确定性构造,无竞态):回归 3 的 post-drop 探针以
/// "先写后读"判定关闭,依据是平台事实——握手已完成、但从未被用户空间
/// accept 的连接,在其监听 socket 关闭后,对端可能得不到任何通知
/// (Linux 5.15 `inet_csk_listen_stop` 的 ANK 注释:既未按规格发 FIN 也
/// 未实现 abort 变体;darwin 则对 backlog 残留发 RST)。这类"拆除竞态
/// 孤儿"一旦被写入,内核必须裁决四元组:对端 socket 已不存在 → RST
/// (RFC 793 未知连接语义);若仍被用户空间持有则数据被缓冲/消费。
///
/// 本测试用裸 std listener 确定性构造该类:connect 仅入 kernel backlog、
/// 永不 accept → close listener → 写 8 字节 → 必须在 2s 内观察到关闭
/// (写 Reset/BrokenPipe/Aborted,或读 EOF/Reset)。无 sleep、无竞态窗口:
/// connect 在 close 之前完成,连接归属内核 backlog 而非任何用户任务。
/// 绿色前提对 darwin/RST-at-close 与 Linux/拆除竞态孤儿两类内核行为均成立;
/// 红 = 写入后连接仍被"某个用户空间持有者"保活 → 真持有,不是平台混杂。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn backlog_orphan_close_is_observable_after_write() {
    // 裸 std listener:确知该连接永远不会被 accept(没有泵、没有任务)
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("std bind");
    let addr = listener.local_addr().expect("std local_addr");

    let body = async {
        // 握手完成即止:连接只存在于 kernel backlog
        let mut probe = TcpStream::connect(addr).await.expect("backlog connect");
        let local = probe.local_addr().ok();
        let peer = probe.peer_addr().ok();
        assert_ne!(
            local, peer,
            "must not be a self-connect (local={local:?} peer={peer:?})"
        );

        drop(listener); // fd 关闭:此后连接的存续只可能是内核孤儿或外部持有

        let wrote = probe.write_all(b"probe123").await;
        let what =
            format!("backlog orphan after write (local={local:?} peer={peer:?} wrote={wrote:?})");
        let mut b = [0u8; 8];
        match wrote {
            Err(e)
                if matches!(
                    e.kind(),
                    io::ErrorKind::ConnectionReset
                        | io::ErrorKind::BrokenPipe
                        | io::ErrorKind::ConnectionAborted
                ) => {}
            Err(e) => panic!("{what}: unexpected write error (not a close signal): {e}"),
            Ok(()) => {
                expect_closed_soon(&what, probe.read(&mut b), Duration::from_secs(2)).await;
            }
        }
    };
    tokio::time::timeout(Duration::from_secs(8), body)
        .await
        .expect("test deadline (8s)");
}

/// 回归:`listen_stream` 必须使用 `cfg.alpn`,而不是内部默认值。
///
/// 由来:服务端曾用 `server_config()`(固定取 DEFAULT_ALPN)构建 acceptor,
/// 导致配置里的 alpn 列表被**静默忽略** —— 配 ["h2","http/1.1"] 实际只
/// 协商 h2,客户端只提 http/1.1 时服务端回 no_application_protocol。
/// 配置项形同虚设且无任何报错,只能靠真实握手发现,故在此锁死。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn listen_stream_honors_configured_alpn() {
    use tokio_rustls::rustls::pki_types::ServerName;
    use tokio_rustls::TlsConnector;

    let cfg = ServerTransportConfig {
        protocol: Protocol::Tls,
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        multiplex: false,
        alpn: vec!["h2".into(), "http/1.1".into()],
        ..Default::default()
    };
    let listener = tcp::listen_stream(&cfg, mux_streams())
        .await
        .expect("listen");
    let addr = listener.local_addr().expect("local_addr");

    // 只提非首项 http/1.1,若服务端忽略配置就会握手失败
    let mut roots = tokio_rustls::rustls::RootCertStore::empty();
    let _ = &mut roots;
    let client = tokio_rustls::rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(NoVerify))
        .with_no_client_auth();
    let mut client = client;
    client.alpn_protocols = vec![b"http/1.1".to_vec()];

    let tcp = TcpStream::connect(addr).await.expect("connect");
    let connector = TlsConnector::from(Arc::new(client));
    let name = ServerName::try_from("localhost").unwrap();
    let stream = tokio::time::timeout(
        Duration::from_secs(5),
        connector.connect(name, tcp),
    )
    .await
    .expect("handshake deadline")
    .expect("handshake must succeed when server offers http/1.1");
    assert_eq!(
        stream.get_ref().1.alpn_protocol(),
        Some(&b"http/1.1"[..]),
        "协商结果必须是客户端提出的 http/1.1"
    );
}

#[derive(Debug)]
struct NoVerify;
impl tokio_rustls::rustls::client::danger::ServerCertVerifier for NoVerify {
    fn verify_server_cert(
        &self,
        _e: &tokio_rustls::rustls::pki_types::CertificateDer<'_>,
        _i: &[tokio_rustls::rustls::pki_types::CertificateDer<'_>],
        _s: &tokio_rustls::rustls::pki_types::ServerName<'_>,
        _o: &[u8],
        _n: tokio_rustls::rustls::pki_types::UnixTime,
    ) -> Result<tokio_rustls::rustls::client::danger::ServerCertVerified, tokio_rustls::rustls::Error>
    {
        Ok(tokio_rustls::rustls::client::danger::ServerCertVerified::assertion())
    }
    fn verify_tls12_signature(
        &self,
        _m: &[u8],
        _c: &tokio_rustls::rustls::pki_types::CertificateDer<'_>,
        _d: &tokio_rustls::rustls::DigitallySignedStruct,
    ) -> Result<tokio_rustls::rustls::client::danger::HandshakeSignatureValid, tokio_rustls::rustls::Error>
    {
        Ok(tokio_rustls::rustls::client::danger::HandshakeSignatureValid::assertion())
    }
    fn verify_tls13_signature(
        &self,
        _m: &[u8],
        _c: &tokio_rustls::rustls::pki_types::CertificateDer<'_>,
        _d: &tokio_rustls::rustls::DigitallySignedStruct,
    ) -> Result<tokio_rustls::rustls::client::danger::HandshakeSignatureValid, tokio_rustls::rustls::Error>
    {
        Ok(tokio_rustls::rustls::client::danger::HandshakeSignatureValid::assertion())
    }
    fn supported_verify_schemes(&self) -> Vec<tokio_rustls::rustls::SignatureScheme> {
        vec![
            tokio_rustls::rustls::SignatureScheme::RSA_PKCS1_SHA256,
            tokio_rustls::rustls::SignatureScheme::ECDSA_NISTP256_SHA256,
            tokio_rustls::rustls::SignatureScheme::ED25519,
            tokio_rustls::rustls::SignatureScheme::RSA_PSS_SHA256,
        ]
    }
}
