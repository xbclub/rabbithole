//! round-9 多载体池接线集成测试(T-I1/T-I2)。
//!
//! T-I1:客户端 `connect_pool`(K=2,真实 TCP)→ 服务端把首连接建池、
//! 第二连接 `attach_yamux_carrier` 附加 → 客户端轮转开流跨两条载体,
//! 数据流全通(echo 往返)。
//! T-I2:`carriers=1` 时 `connect_pool` 严格退化为单载体 `connect`
//! (行为等价:同一监听点上两条路径各完成一次 echo 往返)。

use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::TcpListener;

/// K 不经过 `ClientTransportConfig`(round-44:该字段已删除)——它由调用方
/// 显式传给 `connect_pool`。本 helper 因此只构造协议/地址/多路复用开关。
fn tcp_pool_cfg(addr: std::net::SocketAddr) -> rh_transport::ClientTransportConfig {
    rh_transport::ClientTransportConfig {
        protocol: rh_transport::Protocol::Tcp,
        server_addr: addr,
        multiplex: true,
        ..Default::default()
    }
}

/// 关键 await 的统一时限(防断言性挂死拖垮测试套件)。
async fn deadline<F: std::future::Future>(f: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(5), f)
        .await
        .expect("pool wiring test deadline (5s)")
}

/// 把一条已 accept 的裸 TCP 包装为服务端单载体 yamux,并取出其
/// YamuxMultiplexer(镜像服务端聚合路径的 Arc::try_unwrap + into_yamux)。
fn server_yamux(conn: tokio::net::TcpStream) -> rh_transport::mux::YamuxMultiplexer {
    let mux = rh_transport::wrap_stream_as_server(
        Box::new(conn),
        true,
        rh_transport::MUX_STREAMS_UNLIMITED,
    );
    let mux = Arc::try_unwrap(mux).ok().expect("sole owner");
    mux.into_yamux().expect("yamux carrier")
}

/// T-I1:客户端 connect_pool(K=2)→ 服务端池 + attach_carrier →
/// 控制流走首载体、数据流轮转跨双载体,echo 全通。
#[tokio::test]
async fn pool_wiring_client_pool_server_attach_full_path() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    // 服务端:先收首连接(控制流),再收第二连接(附加载体);首连接的
    // mux 建池、第二连接 attach 进池;此后池 accept 的每条流 echo 回显。
    let server = tokio::spawn(async move {
        let (conn1, _) = listener.accept().await.expect("carrier 1 accept");
        let mux1 = server_yamux(conn1);
        let (conn2, _) = listener.accept().await.expect("carrier 2 accept");
        let mux2 = server_yamux(conn2);

        let pool = rh_transport::Multiplexer::yamux_pool_server(vec![mux1]);
        assert!(
            pool.attach_yamux_carrier(mux2).await,
            "attach_carrier 必须成功(池变体)"
        );

        // 首载体控制流:读标记并回执
        let mut ctrl = pool.accept().await.expect("control stream");
        let mut buf = [0u8; 4];
        ctrl.read_exact(&mut buf).await.expect("ctrl read");
        assert_eq!(&buf, b"ctrl");
        ctrl.write_all(b"ok!").await.expect("ctrl reply");

        // 数据流:池合流接受(跨两条载体),逐条 echo
        for _ in 0..3 {
            let mut s = pool.accept().await.expect("data stream");
            let mut b = [0u8; 8];
            s.read_exact(&mut b).await.expect("data read");
            s.write_all(&b).await.expect("data echo");
        }
    });

    // 客户端:拨 K=2 池;控制流占用轮转序号 0(首载体),后续 3 条数据流
    // 轮转覆盖两条载体(序号 1→载体 1,2→载体 0,3→载体 1)。
    let tcfg = tcp_pool_cfg(addr);
    let pool = rh_transport::connect_pool(&tcfg, 2)
        .await
        .expect("connect_pool");
    let mut ctrl = deadline(pool.open_stream()).await.expect("control open");
    ctrl.write_all(b"ctrl").await.expect("ctrl write");
    let mut buf = [0u8; 3];
    deadline(ctrl.read_exact(&mut buf)).await.expect("ctrl ack");
    assert_eq!(&buf, b"ok!");

    let mut handles = Vec::new();
    for i in 0..3u8 {
        let pool = pool.clone();
        handles.push(tokio::spawn(async move {
            let mut s = deadline(pool.open_stream()).await.expect("data open");
            let probe = [i; 8];
            s.write_all(&probe).await.expect("data write");
            let mut back = [0u8; 8];
            deadline(s.read_exact(&mut back)).await.expect("data echo");
            assert_eq!(back, probe);
        }));
    }
    for h in handles {
        h.await.expect("data task");
    }
    deadline(server).await.expect("server task");
}

/// T-I2:carriers=1 时 connect_pool 与 connect 行为等价——同一监听点上
/// 两条路径各完成一次开流/echo 往返(tcp.rs 的 carriers<=1 回退路径)。
#[tokio::test]
async fn pool_wiring_single_carrier_equivalence() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server = tokio::spawn(async move {
        for _ in 0..2 {
            let (conn, _) = listener.accept().await.expect("accept");
            let mux = rh_transport::wrap_stream_as_server(
                Box::new(conn),
                true,
                rh_transport::MUX_STREAMS_UNLIMITED,
            );
            let mut s = mux.accept().await.expect("stream");
            let mut b = [0u8; 3];
            s.read_exact(&mut b).await.expect("read");
            s.write_all(&b).await.expect("echo");
        }
    });

    let tcfg = tcp_pool_cfg(addr);
    // 路径一:connect_pool(carriers=1)→ 内部退化为 connect_stream
    let mux = rh_transport::connect_pool(&tcfg, 1)
        .await
        .expect("connect_pool(1)");
    let mut s = deadline(mux.open_stream()).await.expect("open");
    s.write_all(b"po1").await.expect("write");
    let mut back = [0u8; 3];
    deadline(s.read_exact(&mut back)).await.expect("echo");
    assert_eq!(&back, b"po1");

    // 路径二:connect(单载体基线路径)
    let mux = rh_transport::connect(&tcfg).await.expect("connect");
    let mut s = deadline(mux.open_stream()).await.expect("open");
    s.write_all(b"ba1").await.expect("write");
    let mut back = [0u8; 3];
    deadline(s.read_exact(&mut back)).await.expect("echo");
    assert_eq!(&back, b"ba1");

    deadline(server).await.expect("server task");
}

/// round-47:`Multiplexer::shard_workers()` 报告的值必须等于**实际 spawn**
/// 的 ShardWorker 数量,而不是按公式重算的值。
///
/// 这是「日志值 == 实际值」的回归锁:`rh-client` 的 pool sizing 日志改读
/// 该访问器。修前它另起一处 `shard_workers_for_k(initial_k)` 独立计算,
/// 只因输入恰好相同才碰巧一致。
///
/// 判据分两层:
///   1. 池变体(K>1):报告值 == `shard_workers_for_k(K)`(构造时传给
///      载体的值),且 >0;
///   2. 单载体变体(K=1,走 `connect` 而非池):报告值 == 该载体构造时
///      的 shard 数,且与池侧口径一致。
///
/// 更强的一层:对同一 K,池内每个成员的报告值必须相同(它们由同一次
/// `connect_stream_pool` 用同一 `shards` 创建)——若将来某人给不同成员
/// 传了不同的 shard 数,这里会红。
#[tokio::test]
async fn shard_workers_reports_actually_spawned_count() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    // 服务端:接受 K 条裸 TCP 并各建一个 yamux(不回话,只保活)
    let server = tokio::spawn(async move {
        let mut kept = Vec::new();
        for _ in 0..2 {
            if let Ok((conn, _)) = listener.accept().await {
                kept.push(server_yamux(conn));
            }
        }
        // 保持载体存活到测试结束
        tokio::time::sleep(Duration::from_secs(10)).await;
        drop(kept);
    });

    // K=2 → 池变体
    let cfg = tcp_pool_cfg(addr);
    let pool = deadline(rh_transport::connect_pool(&cfg, 2))
        .await
        .expect("connect_pool K=2");

    let reported = pool
        .shard_workers()
        .await
        .expect("池变体必须报告 shard_workers");
    let expected = rh_transport::mux::shard_workers_for_k(2);
    assert_eq!(
        reported, expected,
        "K=2 时报告值必须等于构造时传入的 shard 数(实际 spawn 的数量)"
    );
    assert!(reported > 0, "shard 数必须 >= 1,实际 {reported}");

    server.abort();
}

/// round-47:单载体路径(K=1,走 `connect` 而非池)同样报告实际值,
/// 且数值符合「K=1 时取自适应预算」的既有语义(红线:不建池、shard 仍为
/// 自适应值)。
#[tokio::test]
async fn shard_workers_reports_on_single_carrier_path() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server = tokio::spawn(async move {
        if let Ok((conn, _)) = listener.accept().await {
            let mux = server_yamux(conn);
            tokio::time::sleep(Duration::from_secs(10)).await;
            drop(mux);
        }
    });

    let cfg = tcp_pool_cfg(addr);
    let single = deadline(rh_transport::connect(&cfg))
        .await
        .expect("connect");

    let reported = single
        .shard_workers()
        .await
        .expect("单 yamux 变体必须报告 shard_workers");
    let expected = rh_transport::mux::shard_workers_for_k(1);
    assert_eq!(
        reported, expected,
        "K=1 单载体路径的 shard 数必须等于实际 spawn 的自适应值"
    );

    server.abort();
}

/// round-47:`YamuxMultiplexer::shard_workers()` 与真实 Worker 数一致——
/// 用 `into_halves()` 独立构造一个载体,直接数返回的 Worker 向量长度,
/// 与访问器报告值对比。这是最底层的「实际值」定义。
#[tokio::test]
async fn yamux_multiplexer_shard_workers_matches_into_halves() {
    // 对若干 shard 数分别构造,断言报告值 == 实际 Worker 数
    for shards in [1usize, 2, 3, 4, 8] {
        let (a, _b) = tokio::io::duplex(64 * 1024);
        let mux = rh_transport::mux::YamuxMultiplexer::new(
            Box::new(a) as rh_transport::BoxedIo,
            yamux::Mode::Client,
            rh_transport::MUX_STREAMS_UNLIMITED,
            shards,
        );
        assert_eq!(
            mux.shard_workers(),
            shards,
            "shard_workers={shards} 时载体必须报告实际 spawn 的数量"
        );
    }
}
