//! XTCP 打洞客户端侧:STUN 候选上报、dial/listen 打洞、直连会话服务。
//!
//! 直连会话协议(kcp+yamux 之上,双端均为 rabbithole 客户端):
//! - proxy 侧(listen 角色)接受流,首帧 START_FORWARD(proxy_name 定位本地规则),
//!   忽略帧内目标字段,以本地 spec 拨号;
//! - visitor 侧(dial 角色)开流并写 START_FORWARD{proxy_name},随后裸流。

use crate::session::ClientSession;
use rh_protocol::message::{
    ClientMessage, NatHoleConnect, NatHolePrepare, NatHoleReport, StartForward,
};
use rh_protocol::{frame, FrameType, ProxySpec};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

/// NAT_HOLE_PREPARE:绑定 socket → STUN(可选)→ 候选上报。socket 保留至 CONNECT。
pub async fn on_prepare(sess: Arc<ClientSession>, m: NatHolePrepare) {
    let bind_local = "0.0.0.0:0";
    let Ok(sock) = tokio::net::UdpSocket::bind(bind_local).await else {
        return;
    };
    let sock = Arc::new(sock);
    // 通配绑定在本机报 0.0.0.0(不可拨):以 127.0.0.1 代替本机候选
    let local = sock
        .local_addr()
        .ok()
        .map(|a| {
            if a.ip().is_unspecified() {
                std::net::SocketAddr::new(std::net::IpAddr::from([127, 0, 0, 1]), a.port())
            } else {
                a
            }
        })
        .map(|a| a.to_string())
        .unwrap_or_default();
    let mut candidates = vec![local.clone()];
    let mut behavior = String::new();
    if let Some(stun) = sess.cfg.stun_server.clone() {
        if let Ok(server) = stun.parse::<SocketAddr>() {
            match tokio::time::timeout(
                std::time::Duration::from_secs(m.timeout_seconds.clamp(2, 5)),
                rh_core::stun::stun_binding(&sock, server),
            )
            .await
            {
                Ok(Ok(mapped)) => {
                    // ★保留本机候选,把映射地址放在最先尝试。
                    //
                    // 修前直接 `candidates = vec![mapped]` —— 把 lan 候选丢了。
                    // 同 NAT 后的双端本可以走内网直连(延迟远低于打洞-中继),
                    // 丢了这个候选就只剩公网路径。
                    // 顺序:映射地址优先(跨 NAT 最可能通),本机地址兜底。
                    if !candidates.contains(&mapped.to_string()) {
                        candidates.insert(0, mapped.to_string());
                    }
                    behavior = format!("mapped={mapped}");
                }
                Ok(Err(e)) => behavior = format!("stun error: {e}"),
                Err(_) => behavior = "stun timeout".into(),
            }
        }
    }
    let report = NatHoleReport {
        transaction_id: m.transaction_id.clone(),
        candidates,
        nat_behavior: behavior,
    };
    // 有界发送(2s):send_tx 的唯一消费者是 run_loop,队列满时无界
    // send 会 park 在"等自己消费"上(A1 同形)。超时放弃本次上报,
    // 服务端按事务超时回收。
    let sent = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        sess.send_msg(ClientMessage::NatHoleReport(report)),
    )
    .await
    .unwrap_or(false);
    if sent {
        sess.nat_socks
            .lock()
            .await
            .entry(m.transaction_id.clone())
            .or_default()
            .push_back(sock);
        // 事务残留清理
        let s2 = sess.clone();
        let txid = m.transaction_id;
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_secs(120)).await;
            s2.nat_socks.lock().await.remove(&txid);
        });
    }
}

/// NAT_HOLE_CONNECT:执行打洞;visitor 侧成功后直连开流,proxy 侧服务直连。
pub async fn on_connect(sess: Arc<ClientSession>, m: NatHoleConnect, shutdown: CancellationToken) {
    // socket 池按 prepare 到达顺序排列:首 = visitor 侧上报源,次 = proxy 侧。
    // dial 取首、listen 取尾,与两条指令的到达顺序无关。
    let pop_back = m.role == "listen";
    let sock = {
        let mut guard = sess.nat_socks.lock().await;
        match guard.get_mut(&m.transaction_id) {
            Some(q) => {
                if q.len() <= 1 {
                    q.pop_front()
                } else if pop_back {
                    q.pop_back()
                } else {
                    q.pop_front()
                }
            }
            None => None,
        }
    };
    let sock = match sock {
        Some(s) => s,
        None => {
            // 无准备 socket(超时清理/竞态):需要走中继的路径直接重发 USER_CONNECT,
            // 否则等待中的用户连接只能在 TTL 超时后被静默丢弃
            if m.role == "fallback" || (m.role == "dial" && m.fallback_relay) {
                reissue_user_connect(&sess, &m).await;
            }
            return;
        }
    };
    let punch_secs = m.punch_seconds.max(3);
    match m.role.as_str() {
        "dial" => {
            let cands: Vec<SocketAddr> =
                m.candidates.iter().filter_map(|c| c.parse().ok()).collect();
            // 候选来源必须可观测:测试要能区分「真的经 STUN 映射地址打通」与
            // 「走了本机 127.0.0.1 兜底候选(绕开 NAT)」。后者在真实部署里
            // 无意义(对端不可达),但同机测试会让它"通过",掩盖真实路径。
            // 排障时也需要知道实际拨了哪个地址。
            let cands_str: Vec<String> = cands.iter().map(|c| c.to_string()).collect();
            match rh_transport::kcp::punch_dial(sock, &cands, punch_secs).await {
                Ok(mux) => {
                    info!(tx = %m.transaction_id, candidates = ?cands_str, "punch dial ok");
                    replay_waiting(&sess, &m, mux).await;
                }
                Err(e) => {
                    warn!(tx = %m.transaction_id, error = %e, "punch dial failed; fallback relay");
                    if m.fallback_relay {
                        reissue_user_connect(&sess, &m).await;
                    }
                }
            }
        }
        "listen" => {
            // 打洞窗口与拨号侧对齐:超出 2×punch 仍无对端报文即放弃,
            // 避免每次失败打洞泄漏一个永久等待任务与 socket
            let listen_wait = std::time::Duration::from_secs(punch_secs.saturating_mul(2));
            // A7:listen 侧只接受服务端下发的 dial 候选来源。
            let allow: Vec<SocketAddr> =
                m.candidates.iter().filter_map(|c| c.parse().ok()).collect();
            match tokio::time::timeout(
                listen_wait,
                rh_transport::kcp::punch_listen(sock.clone(), &allow),
            )
            .await
            {
                Ok(Ok(mux)) => {
                    info!(tx = %m.transaction_id, "punch listen ok; serving direct");
                    serve_direct(sess, mux, shutdown, m.auth.clone(), m.nonce.clone()).await;
                }
                Ok(Err(e)) => {
                    warn!(tx = %m.transaction_id, error = %e, "punch listen failed");
                }
                Err(_) => {
                    warn!(tx = %m.transaction_id, "punch listen window expired");
                }
            }
        }
        "fallback" => {
            // 服务端判定回退中继:visitor 侧重发触发中继
            reissue_user_connect(&sess, &m).await;
        }
        _ => {}
    }
}

/// 打洞成功后:把等待中的首连接经直连发送;成功后缓存 mux 备后续连接复用。
async fn replay_waiting(
    sess: &Arc<ClientSession>,
    m: &NatHoleConnect,
    mux: Arc<rh_transport::Multiplexer>,
) {
    // 保留整条 entry(含登记时刻):回退重放时原样放回,不重置 TTL。
    let entry = sess.take_waiting(m.user_conn_id).await;
    let Some((conn, visitor_id, registered_at)) = entry else {
        return;
    };
    let vspec = sess.visitors.lock().await.get(&visitor_id).cloned();
    let Some(vspec) = vspec else { return };
    match crate::visitor_run::xtcp_direct_stream_pub(
        sess,
        &mux,
        &vspec,
        m.auth.clone(),
        m.user_conn_id,
        conn,
    )
    .await
    {
        None => {
            // A7:凭据与 mux 同步登记,后续连接复用同一凭据。
            sess.punched_auth
                .lock()
                .await
                .insert(visitor_id, m.auth.clone());
            sess.punched.lock().await.insert(visitor_id, mux);
        }
        Some(_conn) => {
            // 直连失败:登记回等待并回退中继(沿用原登记时刻,不续命)
            sess.requeue_waiting(m.user_conn_id, (_conn, visitor_id, registered_at))
                .await;
            reissue_user_connect(sess, m).await;
        }
    }
}

async fn reissue_user_connect(sess: &Arc<ClientSession>, m: &NatHoleConnect) {
    // 等待连接重新可见:直接以原 user_conn_id 重发
    // 找回等待项所属 visitor(未消费的重放/回退场景)
    let vid = sess.waiting_visitor(m.user_conn_id).await;
    let Some(vid) = vid else {
        warn!(
            conn = m.user_conn_id,
            "fallback reissue skipped: user conn no longer waiting"
        );
        return;
    };
    let vspec = sess.visitors.lock().await.get(&vid).cloned();
    let Some(vspec) = vspec else { return };
    let uc = rh_protocol::message::UserConnect {
        visitor_id: vspec.id,
        user_conn_id: m.user_conn_id,
        src_addr: String::new(),
        udp: false,
    };
    let _ = sess.send_msg(ClientMessage::UserConnect(uc)).await;
}

/// proxy 侧:直连会话服务(接受流 → 首帧 START_FORWARD → 本地拨号)。
async fn serve_direct(
    sess: Arc<ClientSession>,
    mux: Arc<rh_transport::Multiplexer>,
    shutdown: CancellationToken,
    expect_auth: String,
    nonce: String,
) {
    loop {
        tokio::select! {
            acc = mux.accept() => {
                let Ok(mut stream) = acc else { return };
                let s2 = sess.clone();
                let expect_auth = expect_auth.clone();
                let nonce = nonce.clone();
                tokio::spawn(async move {
                    let mut buf = bytes::BytesMut::with_capacity(512);
                    let Ok((ty, payload)) = frame::read_frame(&mut stream, &mut buf).await else { return };
                    let leftover: Vec<u8> = buf.to_vec();
                    if ty != FrameType::START_FORWARD {
                        let _ = stream.shutdown().await;
                        return;
                    }
                    let Ok(fwd) = serde_json::from_slice::<StartForward>(&payload) else { return };
                    let spec = {
                        let proxies = s2.proxies.lock().await;
                        find_by_name(&proxies, &fwd.proxy_name)
                    };
                    let Some(spec) = spec else {
                        let _ = stream.shutdown().await;
                        return;
                    };
                    // A7:直连首帧必须携带本次打洞事务的凭据。期望值来自
                    // 服务端 NAT_HOLE_CONNECT(按目标规则密钥计算);缺失或
                    // 不符立即断开,不拨本地目标。
                    if !direct_auth_ok(&fwd.xtcp_auth, &expect_auth, &nonce) {
                        warn!(
                            proxy = %spec.name,
                            "direct forward rejected: missing/mismatched xtcp auth"
                        );
                        let _ = stream.shutdown().await;
                        return;
                    }
                    let fwd = StartForward {
                        proxy_id: spec.id,
                        user_src: fwd.user_src,
                        target_host: spec.target_host.clone(),
                        target_port: spec.target_port,
                        flags: 0,
                        proxy_proto: rh_protocol::message::ProxyProto::None,
                        proxy_name: spec.name.clone(),
                        xtcp_auth: String::new(),
                    };
                    if let Err(e) = crate::forward::handle_start_forward(s2, stream, &spec, fwd, leftover).await {
                        warn!(error = %e, "direct forward failed");
                    }
                });
            }
            _ = shutdown.cancelled() => return,
            _ = sess.cancel.cancelled() => return,
        }
    }
}

fn find_by_name(
    proxies: &std::collections::HashMap<u64, std::sync::Arc<ProxySpec>>,
    name: &str,
) -> Option<std::sync::Arc<ProxySpec>> {
    proxies.values().find(|p| p.name == name).cloned()
}

/// A7 直连首帧凭据比对:恒定时间比较,长度不同直接失败。
fn direct_auth_ok(got: &str, expect_auth: &str, nonce: &str) -> bool {
    if expect_auth.is_empty() || nonce.is_empty() || got.is_empty() {
        // 旧服务端不会下发凭据;此时拒绝直连(不静默降级认证语义)。
        return false;
    }
    rh_transport::constant_time_eq(got.as_bytes(), expect_auth.as_bytes())
}
