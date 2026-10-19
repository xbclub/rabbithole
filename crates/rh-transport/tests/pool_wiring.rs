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
