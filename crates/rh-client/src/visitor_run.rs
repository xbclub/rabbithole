//! visitor 本地运行:stcp/xtcp TCP 监听、sudp UDP 监听、配对交付。

use crate::session::ClientSession;
use rh_protocol::message::{
    forward_flags, ClientMessage, StartForward, StartVisitorRelay, UserConnect,
};
use rh_protocol::{VisitorKind, VisitorSpec};
use rh_transport::BoxedIo;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::warn;

/// 启动一个 visitor 的本地监听。
pub async fn start(sess: Arc<ClientSession>, visitor_id: u64, spec: VisitorSpec) {
    let cancel = CancellationToken::new();
    sess.visitor_cancels
        .lock()
        .await
        .insert(visitor_id, cancel.clone());
    match spec.kind {
        VisitorKind::Stcp | VisitorKind::Xtcp => {
            let addr = format!("{}:{}", spec.bind_addr, spec.bind_port);
            let l = match tokio::net::TcpListener::bind(&addr).await {
                Ok(l) => l,
                Err(e) => {
                    warn!(visitor = %spec.name, error = %e, "visitor bind failed");
                    return;
                }
            };
            tokio::spawn(run_tcp(sess, visitor_id, spec, l, cancel));
        }
        VisitorKind::Sudp => {
            let addr = format!("{}:{}", spec.bind_addr, spec.bind_port);
            let s = match tokio::net::UdpSocket::bind(&addr).await {
                Ok(s) => s,
                Err(e) => {
                    warn!(visitor = %spec.name, error = %e, "visitor bind failed");
                    return;
                }
            };
            tokio::spawn(run_udp(sess, visitor_id, Arc::new(s), cancel));
        }
    }
}

/// stcp/xtcp:本地 TCP 接入。xtcp 优先复用已打洞直连;否则 USER_CONNECT
/// 由服务端中继(stcp)或发起打洞(xtcp)。
async fn run_tcp(
    sess: Arc<ClientSession>,
    visitor_id: u64,
    spec: VisitorSpec,
    l: tokio::net::TcpListener,
    cancel: CancellationToken,
) {
    loop {
        tokio::select! {
            acc = l.accept() => {
                let Ok((conn, peer)) = acc else { return };
                // C10:用户侧连接必须关闭 Nagle 并开启 keepalive。
                // 修前 visitor 用户侧是全仓唯一不设这两个选项的 accept 路径,
                // stcp/xtcp 的小包交互会被 Nagle 合并出额外 RTT。
                let _ = conn.set_nodelay(true);
                let s2 = sess.clone();
                let sp = spec.clone();
                let vid = visitor_id;
                tokio::spawn(async move {
                    handle_user_conn(s2, vid, sp, conn, peer.to_string()).await;
                });
            }
            _ = cancel.cancelled() => return,
            _ = sess.cancel.cancelled() => return,
        }
    }
}

async fn handle_user_conn(
    sess: Arc<ClientSession>,
    visitor_id: u64,
    spec: VisitorSpec,
    mut conn: tokio::net::TcpStream,
    src: String,
) {
    let user_conn_id = sess.next_conn_id().await;
    // xtcp:已有直连 mux 则直接开流
    if spec.kind == VisitorKind::Xtcp {
        let mux = sess.punched.lock().await.get(&visitor_id).cloned();
        let auth = sess
            .punched_auth
            .lock()
            .await
            .get(&visitor_id)
            .cloned()
            .unwrap_or_default();
        if let Some(mux) = mux {
            match xtcp_direct_stream(&sess, &mux, &spec, auth, user_conn_id, conn).await {
                Some(conn_back) => {
                    // 直连失败:移除失效 mux(与其凭据同生命周期),落入常规路径
                    sess.punched.lock().await.remove(&visitor_id);
                    sess.punched_auth.lock().await.remove(&visitor_id);
                    conn = conn_back;
                }
                None => return,
            }
        }
    }
    // 常规:登记等待 + USER_CONNECT;TTL 10s
    sess.waiting
        .lock()
        .await
        .insert(user_conn_id, (conn, visitor_id));
    {
        let s2 = sess.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(10)).await;
            s2.waiting.lock().await.remove(&user_conn_id);
        });
    }
    tracing::debug!(visitor_id, user_conn_id, "user connect registered, sending");
    let uc = UserConnect {
        visitor_id,
        user_conn_id,
        src_addr: src,
        udp: false,
    };
    if !sess.send_msg(ClientMessage::UserConnect(uc)).await {
        sess.waiting.lock().await.remove(&user_conn_id);
    }
}

/// 经打洞直连 mux 开一路流(首帧 START_FORWARD 供对端识别),拼本地连接。
/// Some(conn) = 失败返还连接;None = 已消费(成功)。
async fn xtcp_direct_stream(
    sess: &Arc<ClientSession>,
    mux: &Arc<rh_transport::Multiplexer>,
    spec: &VisitorSpec,
    xtcp_auth: String,
    _user_conn_id: u64,
    conn: tokio::net::TcpStream,
) -> Option<tokio::net::TcpStream> {
    let mut stream = match mux.open_stream().await {
        Ok(s) => s,
        // 开流失败(mux 已死):返还连接,由调用方移除失效 mux 并走常规路径
        Err(_) => return Some(conn),
    };
    // A7:直连首帧凭据由调用方按 visitor_id 从 punched_auth 取。
    let fwd = StartForward {
        proxy_id: 0,
        user_src: String::new(),
        target_host: String::new(),
        target_port: 0,
        flags: 0,
        proxy_proto: rh_protocol::message::ProxyProto::None,
        proxy_name: spec.target_proxy_name.clone(),
        xtcp_auth,
    };
    let Ok(payload) = serde_json::to_vec(&fwd) else {
        return Some(conn);
    };
    if rh_protocol::write_frame(&mut stream, rh_protocol::FrameType::START_FORWARD, &payload)
        .await
        .is_err()
    {
        return Some(conn);
    }
    crate::forward::splice(stream, Box::new(conn), None).await;
    let _ = sess;
    None
}

/// nathole_cli 复用:经打洞直连 mux 开流并拼接。
pub async fn xtcp_direct_stream_pub(
    sess: &Arc<ClientSession>,
    mux: &Arc<rh_transport::Multiplexer>,
    spec: &VisitorSpec,
    auth: String,
    user_conn_id: u64,
    conn: tokio::net::TcpStream,
) -> Option<tokio::net::TcpStream> {
    xtcp_direct_stream(sess, mux, spec, auth, user_conn_id, conn).await
}

/// 中继通道配对交付(session.rs 调用)。
pub async fn deliver_relay(
    sess: Arc<ClientSession>,
    relay: StartVisitorRelay,
    channel: BoxedIo,
    _vspec: VisitorSpec,
    prefix: Vec<u8>,
) {
    let Some((mut conn, _)) = sess.waiting.lock().await.remove(&relay.user_conn_id) else {
        tracing::warn!(
            conn = relay.user_conn_id,
            "relay delivered but user conn gone"
        );
        return; // 本地连接已超时:丢弃通道
    };
    // 压缩时残留字节是压缩流首块,须回灌给解压器;未压缩时直接写给本地连接
    let channel = if relay.flags & forward_flags::COMPRESSED != 0 {
        let inner = if prefix.is_empty() {
            channel
        } else {
            rh_transport::PrependedIo::wrap(channel, prefix)
        };
        rh_transport::compress_stream(inner).await
    } else {
        if !prefix.is_empty() {
            use tokio::io::AsyncWriteExt as _;
            let _ = conn.write_all(&prefix).await;
        }
        channel
    };
    crate::forward::splice(channel, Box::new(conn), None).await;
}

/// sudp:本地 UDP 监听;首包触发 USER_CONNECT(udp=true);通道到位后
/// 由通道任务接管 socket(带首包缓冲),死亡后重新武装。
async fn run_udp(
    sess: Arc<ClientSession>,
    visitor_id: u64,
    sock: Arc<tokio::net::UdpSocket>,
    cancel: CancellationToken,
) {
    let (tx, mut rx) = mpsc::channel::<BoxedIo>(1);
    sess.sudp_deliver.lock().await.insert(visitor_id, tx);
    let armed = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut pending: Vec<(std::net::SocketAddr, Vec<u8>)> = Vec::new();
    let mut pkt = vec![0u8; 65536];
    loop {
        tokio::select! {
            r = sock.recv_from(&mut pkt) => {
                let Ok((n, peer)) = r else { return };
                if !armed.load(std::sync::atomic::Ordering::Relaxed) {
                    armed.store(true, std::sync::atomic::Ordering::Relaxed);
                    let uc = UserConnect {
                        visitor_id,
                        user_conn_id: 0,
                        src_addr: peer.to_string(),
                        udp: true,
                    };
                    if !sess.send_msg(ClientMessage::UserConnect(uc)).await {
                        armed.store(false, std::sync::atomic::Ordering::Relaxed);
                    }
                }
                pending.push((peer, pkt[..n].to_vec()));
                if pending.len() > 512 {
                    pending.remove(0);
                }
            }
            c = rx.recv() => {
                let Some(channel) = c else { return };
                let s2 = sess.clone();
                let aw = armed.clone();
                let sk = sock.clone();
                let c2 = cancel.clone();
                let p = std::mem::take(&mut pending);
                let vid = visitor_id;
                let mut handle = tokio::spawn(async move {
                    crate::udpchan::run_visitor_side(channel, sk, c2, p).await;
                    // 通道死亡:重新武装(下一本地包触发重建)
                    aw.store(false, std::sync::atomic::Ordering::Relaxed);
                    let uc = UserConnect {
                        visitor_id: vid,
                        user_conn_id: 0,
                        src_addr: String::new(),
                        udp: true,
                    };
                    let _ = s2.send_msg(ClientMessage::UserConnect(uc)).await;
                });
                // 通道在位:等待其结束(不与本循环竞争 socket)
                tokio::select! {
                    _ = &mut handle => {}
                    _ = cancel.cancelled() => { handle.abort(); return; }
                    _ = sess.cancel.cancelled() => { handle.abort(); return; }
                }
            }
            _ = cancel.cancelled() => return,
            _ = sess.cancel.cancelled() => return,
        }
    }
}
