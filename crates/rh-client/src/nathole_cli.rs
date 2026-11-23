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
use std::net::{IpAddr, Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

/// 默认路由探测目标(RFC 5737 TEST-NET-1,仅触发内核路由表查询,不发任何报文)。
const DEFAULT_ROUTE_PROBE: SocketAddr =
    SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(192, 0, 2, 1), 9));

/// 判定本机 IPv4 是否可作为局域网直连候选:排除未指定/环回/多播/链路本地/广播地址。
fn is_usable_lan_ipv4(ip: Ipv4Addr) -> bool {
    !ip.is_unspecified()
        && !ip.is_loopback()
        && !ip.is_multicast()
        && !ip.is_link_local()
        && !ip.is_broadcast()
}

/// 通过临时 UDP socket `connect` 查询内核路由表选出的本机源 IPv4(0 发包,
/// 跨 Linux/macOS/Windows 零额外依赖、无 unsafe)。
fn route_probe_ipv4(target: SocketAddr) -> Option<Ipv4Addr> {
    let canon = SocketAddr::new(target.ip().to_canonical(), target.port());
    let SocketAddr::V4(v4_target) = canon else {
        return None;
    };
    if v4_target.ip().is_unspecified()
        || v4_target.ip().is_loopback()
        || v4_target.ip().is_multicast()
    {
        return None;
    }
    let s = std::net::UdpSocket::bind((Ipv4Addr::UNSPECIFIED, 0)).ok()?;
    s.connect(SocketAddr::V4(v4_target)).ok()?;
    match s.local_addr().ok()?.ip() {
        IpAddr::V4(ip) if is_usable_lan_ipv4(ip) => Some(ip),
        _ => None,
    }
}

/// 探测本机可用于局域网直连的 IPv4 地址(按 STUN → 服务端 → 默认路由 顺序去重)。
fn discover_local_ipv4s(
    stun_addr: Option<SocketAddr>,
    server_addr: Option<SocketAddr>,
) -> Vec<Ipv4Addr> {
    let mut ips = Vec::new();
    for target in [stun_addr, server_addr, Some(DEFAULT_ROUTE_PROBE)]
        .into_iter()
        .flatten()
    {
        if let Some(ip) = route_probe_ipv4(target) {
            if !ips.contains(&ip) {
                ips.push(ip);
            }
        }
    }
    ips
}

/// 组装上报给服务端的候选地址列表:
/// 1. STUN 映射地址优先(跨 NAT 最可能连通);
/// 2. 探测到的本机非环回 LAN IPv4(同 NAT 下内网直连,去重);
/// 3. 仅当无任何可用 LAN IPv4 或服务端本身解析为环回地址(本机自测场景)时,
///    附加 `127.0.0.1:<port>` 兜底,避免跨主机部署在不可达环回地址上浪费拨号轮询槽位。
fn build_candidates(
    bound_port: u16,
    mapped: Option<SocketAddr>,
    lan_ips: &[Ipv4Addr],
    server_is_loopback: bool,
) -> Vec<String> {
    let mut addrs: Vec<SocketAddr> = Vec::new();
    let mut push_unique = |addr: SocketAddr| {
        if !addrs.contains(&addr) {
            addrs.push(addr);
        }
    };
    if let Some(m) = mapped {
        push_unique(m);
    }
    let mut has_lan = false;
    for &ip in lan_ips {
        if is_usable_lan_ipv4(ip) {
            push_unique(SocketAddr::new(IpAddr::V4(ip), bound_port));
            has_lan = true;
        }
    }
    if !has_lan || server_is_loopback {
        push_unique(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), bound_port));
    }
    addrs.into_iter().map(|a| a.to_string()).collect()
}

/// NAT_HOLE_PREPARE:绑定 socket → STUN(可选)→ 候选上报。socket 保留至 CONNECT。
pub async fn on_prepare(sess: Arc<ClientSession>, m: NatHolePrepare) {
    let bind_local = "0.0.0.0:0";
    let Ok(sock) = tokio::net::UdpSocket::bind(bind_local).await else {
        return;
    };
    let sock = Arc::new(sock);
    let Ok(bound_addr) = sock.local_addr() else {
        return;
    };
    let stun_addr = sess
        .cfg
        .stun_server
        .as_deref()
        .and_then(|s| s.parse::<SocketAddr>().ok());
    let server_addr = tokio::time::timeout(
        std::time::Duration::from_millis(500),
        sess.cfg.resolve_server_addr(),
    )
    .await
    .ok()
    .and_then(Result::ok)
    .map(|r| r.addr);
    let server_is_loopback = server_addr
        .map(|a| a.ip().to_canonical().is_loopback())
        .unwrap_or(false);

    let mut mapped: Option<SocketAddr> = None;
    let mut behavior = String::new();
    if let Some(server) = stun_addr {
        match tokio::time::timeout(
            std::time::Duration::from_secs(m.timeout_seconds.clamp(2, 5)),
            rh_core::stun::stun_binding(&sock, server),
        )
        .await
        {
            Ok(Ok(addr)) => {
                mapped = Some(addr);
                behavior = format!("mapped={addr}");
            }
            Ok(Err(e)) => behavior = format!("stun error: {e}"),
            Err(_) => behavior = "stun timeout".into(),
        }
    }
    let lan_ips = if bound_addr.ip().is_unspecified() {
        discover_local_ipv4s(stun_addr, server_addr)
    } else if let IpAddr::V4(v4) = bound_addr.ip().to_canonical() {
        if is_usable_lan_ipv4(v4) {
            vec![v4]
        } else {
            Vec::new()
        }
    } else {
        Vec::new()
    };
    let candidates = build_candidates(bound_addr.port(), mapped, &lan_ips, server_is_loopback);
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_candidates_orders_mapped_first_and_omits_loopback_for_remote_server() {
        let mapped: SocketAddr = "203.0.113.10:50000".parse().unwrap();
        let lan = [Ipv4Addr::new(192, 168, 1, 20)];
        let got = build_candidates(40000, Some(mapped), &lan, false);
        assert_eq!(got, vec!["203.0.113.10:50000", "192.168.1.20:40000"]);
    }

    #[test]
    fn build_candidates_dedups_and_filters_unusable_lan_ips() {
        let mapped: SocketAddr = "192.168.1.20:40000".parse().unwrap();
        let lan = [
            Ipv4Addr::UNSPECIFIED,
            Ipv4Addr::LOCALHOST,
            Ipv4Addr::new(169, 254, 10, 2),
            Ipv4Addr::new(224, 0, 0, 1),
            Ipv4Addr::BROADCAST,
            Ipv4Addr::new(192, 168, 1, 20),
            Ipv4Addr::new(10, 0, 0, 5),
            Ipv4Addr::new(10, 0, 0, 5),
        ];
        let got = build_candidates(40000, Some(mapped), &lan, false);
        assert_eq!(got, vec!["192.168.1.20:40000", "10.0.0.5:40000"]);
    }

    #[test]
    fn build_candidates_falls_back_to_loopback_when_no_lan_ip_found() {
        let got_no_stun = build_candidates(40000, None, &[], false);
        assert_eq!(got_no_stun, vec!["127.0.0.1:40000"]);

        let mapped: SocketAddr = "203.0.113.10:50000".parse().unwrap();
        let got_with_stun = build_candidates(40000, Some(mapped), &[], false);
        assert_eq!(got_with_stun, vec!["203.0.113.10:50000", "127.0.0.1:40000"]);
    }

    #[test]
    fn build_candidates_keeps_loopback_when_server_is_loopback() {
        let mapped: SocketAddr = "127.0.0.1:50000".parse().unwrap();
        let lan = [Ipv4Addr::new(192, 168, 1, 20)];
        let got = build_candidates(40000, Some(mapped), &lan, true);
        assert_eq!(
            got,
            vec!["127.0.0.1:50000", "192.168.1.20:40000", "127.0.0.1:40000"]
        );

        // 映射地址与环回同端口时必须去重
        let same_mapped: SocketAddr = "127.0.0.1:40000".parse().unwrap();
        let got_dedup = build_candidates(40000, Some(same_mapped), &lan, true);
        assert_eq!(got_dedup, vec!["127.0.0.1:40000", "192.168.1.20:40000"]);
    }

    #[test]
    fn discover_local_ipv4s_filters_invalid_and_skips_loopback_targets() {
        assert_eq!(
            route_probe_ipv4("127.0.0.1:7100".parse().unwrap()),
            None,
            "环回目标不得用于探测局域网 IP"
        );
        assert_eq!(
            route_probe_ipv4("[::1]:7100".parse().unwrap()),
            None,
            "IPv6 环回目标应跳过"
        );
        let ips = discover_local_ipv4s(None, None);
        for ip in &ips {
            assert!(
                is_usable_lan_ipv4(*ip),
                "探测结果不得包含未指定/环回/多播/链路本地地址: {ip}"
            );
        }
        // 结果必须去重
        let unique: std::collections::HashSet<_> = ips.iter().copied().collect();
        assert_eq!(unique.len(), ips.len());
    }
}
