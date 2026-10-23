//! visitor 中继:stcp/sudp 访问端在服务端的中继配对与 xtcp 协调入口。
//!
//! 配对模型:服务端为一次访问在两侧各签发一条数据通道
//! (visitor 侧 VisitorRelay / 代理侧 RelayProxy),到达后:
//! - TCP:代理侧写 START_FORWARD(自包含目标)后裸流,visitor 侧写
//!   START_VISITOR_RELAY 后裸流,服务端 splice 两条流;
//! - UDP(SUDP):两侧均为 UDP_PACKET 帧通道,帧对帧转发;
//! - XTCP:先打洞协调,失败回退以上中继。

use crate::session::{RequestPurpose, Session};
use crate::state::AppState;
use crate::state::VisitorResolution;
use rh_protocol::message::{ChannelEntity, StartForward, StartVisitorRelay, UserConnect};
use rh_protocol::{forward_flags, FrameType, ProxyKind, ProxySpec, VisitorSpec};
use rh_transport::BoxedIo;
use std::sync::Arc;
use std::time::Duration;
use tracing::warn;

/// USER_CONNECT:visitor 本地新连接(stcp/xtcp TCP;sudp 首包触发)。
pub async fn on_user_connect(state: &Arc<AppState>, sess: &Arc<Session>, u: UserConnect) {
    // C1:解析结果按 (client_id, config_revision) 缓存。
    //
    // 修前每条用户连接两次全表 DB 读 + 全量 JSON 反序列化,并且内联在
    // **控制读循环**里(visitor 连接建立速率一高就把控制面拖住)。配置只在
    // revision 变化时更新,缓存命中即为最新。
    let cache_key = (sess.client_id.clone(), u.visitor_id);
    // R2:双 gen 复核 —— 访问器属主与目标客户端任一配置变更都失效
    let resolve = match state.visitor_cache.get(&cache_key) {
        Some(c)
            if c.gen == state.cfg_gen(&sess.client_id)
                && c.target_gen == state.cfg_gen(&c.target_client) =>
        {
            c.clone()
        }
        _ => {
            let visitors = state
                .db
                .list_visitors(Some(&sess.client_id))
                .await
                .unwrap_or_default();
            let Some((_vid, _v_client, v_target_client, vspec)) = visitors
                .into_iter()
                .find(|(id, _, _, s)| *id as u64 == u.visitor_id && s.enabled)
            else {
                return;
            };
            let proxies = state
                .db
                .list_proxies(Some(&v_target_client))
                .await
                .unwrap_or_default();
            let Some((_pid, _pcid, pspec)) = proxies
                .into_iter()
                .find(|(_, _, s)| s.name == vspec.target_proxy_name && s.enabled)
            else {
                warn!(visitor = vspec.name, "target proxy not found/enabled");
                return;
            };
            let entry = Arc::new(VisitorResolution {
                gen: state.cfg_gen(&sess.client_id),
                target_gen: state.cfg_gen(&v_target_client),
                visitor_id: vspec.id,
                target_client: v_target_client,
                vspec,
                pspec,
            });
            state.visitor_cache.insert(cache_key.clone(), entry.clone());
            entry
        }
    };
    // 键已含 visitor_id,这里的复核退化为"配置是否仍启用"的兜底。
    if resolve.visitor_id != u.visitor_id || !resolve.vspec.enabled || !resolve.pspec.enabled {
        return;
    }
    // R1(第 1 轮扫描):Arc 借用即可,修前每连接三次深克隆(vspec/pspec/
    // target_client)——secret_ok/relay_* 全部按引用使用。pspec 在 xtcp/relay
    // 分支也只需引用;relay_tcp 内部的 spawn 深克隆另修(传 proxy_id)。
    let vspec = &resolve.vspec;
    let pspec = &resolve.pspec;
    let v_target_client = &resolve.target_client;
    if !secret_ok(pspec, vspec) {
        return;
    }
    let Some(target_sess) = state.sessions.get(v_target_client) else {
        warn!(visitor = vspec.name, target = %v_target_client, "target client offline");
        return;
    };

    if u.udp {
        relay_udp(state, sess, vspec, pspec, &target_sess).await;
        return;
    }
    if vspec.kind == rh_protocol::VisitorKind::Xtcp {
        // 同一 user_conn_id 的重发 UserConnect = 客户端打洞失败的回退信号:
        // 首次尝试打洞;重发直接走服务端中继。
        let already = state
            .nathole
            .mark_attempted(&sess.session_id, u.user_conn_id)
            .await;
        tracing::info!(
            visitor = vspec.name,
            conn = u.user_conn_id,
            reissue = already,
            "xtcp user connect: punch or relay"
        );
        if !already {
            let fallback =
                crate::nathole::start(state, sess, vspec, pspec, &target_sess, u.user_conn_id)
                    .await;
            if !fallback {
                return; // 打洞协调已发起
            }
        }
    }
    tracing::info!(
        visitor = vspec.name,
        conn = u.user_conn_id,
        "visitor relay established"
    );
    relay_tcp(state, sess, vspec, pspec, &target_sess, u.user_conn_id).await;
}

/// 密钥校验。访问授权由访问器记录本身保证(仅管理员可建,记录绑定客户端与
/// 目标规则,协议无客户端上行注册通道),不再比对规则侧客户端白名单。
fn secret_ok(pspec: &ProxySpec, vspec: &rh_protocol::VisitorSpec) -> bool {
    let Some(secret_cfg) = pspec.secret_cfg() else {
        return false;
    };
    // 密钥单一事实源在规则侧:访问器密钥留空 = 继承目标规则当前密钥
    // (访问器记录仅管理员可建,选了规则即视为授权,无需重复录入同一凭据;
    // 规则轮换密钥也不会留下过期副本)。非空仍比对,兼容两侧显式双写的旧部署。
    if !secret_cfg.secret_key.is_empty()
        && !vspec.secret_key.is_empty()
        && secret_cfg.secret_key != vspec.secret_key
    {
        warn!(visitor = vspec.name, "visitor secret mismatch");
        return false;
    }
    true
}

/// TCP 中继。
async fn relay_tcp(
    state: &Arc<AppState>,
    sess: &Arc<Session>,
    vspec: &VisitorSpec,
    pspec: &ProxySpec,
    target_sess: &Arc<Session>,
    user_conn_id: u64,
) {
    // 压缩必须两端一致(服务端裸 splice):任一侧未开启则整体不压缩
    let compress = vspec.use_compression && pspec.transport.use_compression;
    let Some((_visitor_rid, _proxy_rid, vr, pr)) = state
        .issue_relay_pair(
            sess,
            target_sess,
            ChannelEntity::Visitor {
                visitor_id: vspec.id,
                user_conn_id,
            },
            RequestPurpose::VisitorRelay {
                visitor_id: vspec.id,
                user_conn_id,
                compress,
            },
            ChannelEntity::Proxy { proxy_id: pspec.id },
            RequestPurpose::RelayProxy {
                proxy_id: pspec.id,
                compress,
            },
        )
        .await
    else {
        return;
    };
    let st = state.clone();
    let pid = pspec.id; // R1:spawn 只需 id,修前深克隆整个 ProxySpec
    let vid_sess = sess.session_id.clone();
    let pid_sess = target_sess.session_id.clone();
    tokio::spawn(async move {
        // 通道签发后 ≤15s 内必须两侧都到达;超时清理配对槽位,防任务与表项泄漏。
        // R1:两侧并发等待(互不依赖;修前串行,失败路径最坏 2×15s)。
        let (vr_res, pr_res) = tokio::join!(
            tokio::time::timeout(Duration::from_secs(15), vr),
            tokio::time::timeout(Duration::from_secs(15), pr)
        );
        let (visitor_stream, proxy_stream) = match (vr_res, pr_res) {
            (Ok(Ok(v)), Ok(Ok(p))) => (v, p),
            _ => {
                cleanup_pair(&st, &vid_sess, _visitor_rid, &pid_sess, _proxy_rid).await;
                return;
            }
        };
        splice_relay(st, pid, proxy_stream, visitor_stream).await;
    });
}

/// 超时/失败路径:移除两条配对表登记(deliver_pair 只处理"抵达"路径)。
async fn cleanup_pair(
    state: &Arc<AppState>,
    vid_sess: &str,
    visitor_rid: u64,
    pid_sess: &str,
    proxy_rid: u64,
) {
    let mut pairs = state.relay_pairs.lock().await;
    pairs.remove(&(vid_sess.to_string(), visitor_rid));
    pairs.remove(&(pid_sess.to_string(), proxy_rid));
}

/// SUDP 帧中继(两侧常驻通道)。
async fn relay_udp(
    state: &Arc<AppState>,
    sess: &Arc<Session>,
    vspec: &VisitorSpec,
    pspec: &ProxySpec,
    target_sess: &Arc<Session>,
) {
    // 已有常驻中继则复用(每 visitor 一条)。
    // 先占位后签发:登记与检查之间不得隔 await,否则并发首包可建出第二条
    {
        let mut relays = state.udp_relays.lock().await;
        if relays.contains(&vspec.id) {
            return;
        }
        relays.insert(vspec.id);
    }
    // 压缩两端一致(同 relay_tcp)
    let compress = vspec.use_compression && pspec.transport.use_compression;
    let Some((_visitor_rid, _proxy_rid, vr, pr)) = state
        .issue_relay_pair(
            sess,
            target_sess,
            ChannelEntity::Visitor {
                visitor_id: vspec.id,
                user_conn_id: 0,
            },
            RequestPurpose::VisitorRelay {
                visitor_id: vspec.id,
                user_conn_id: 0,
                compress,
            },
            ChannelEntity::Proxy { proxy_id: pspec.id },
            RequestPurpose::RelayProxy {
                proxy_id: pspec.id,
                compress,
            },
        )
        .await
    else {
        state.udp_relays.lock().await.remove(&vspec.id);
        return;
    };
    let st = state.clone();
    let vid = vspec.id;
    let prt = state.proxies.get(pspec.id);
    tokio::spawn(async move {
        // 通道签发后 ≤15s 内必须到达;否则清理 relay 槽位避免死占。
        // R1:两侧并发等待(修前串行,失败路径最坏 2×15s)。
        let (pr_res, vr_res) = tokio::join!(
            tokio::time::timeout(Duration::from_secs(15), pr),
            tokio::time::timeout(Duration::from_secs(15), vr)
        );
        let (a, b) = match (pr_res, vr_res) {
            (Ok(Ok(x)), Ok(Ok(y))) => (x, y),
            _ => {
                st.udp_relays.lock().await.remove(&vid);
                return;
            }
        };
        let mut a = a;
        // 计量归因到目标代理(visitor 流量按目标归属,design-parity 决策 3)
        let _conn_guard = match crate::data::try_open_user_conn(&st, prt.as_ref()) {
            Some(g) => g,
            None => {
                st.udp_relays.lock().await.remove(&vid);
                return;
            }
        };
        let mut b = crate::data::CountedUserIo::user(b, &st, prt);
        let _ = crate::data::copy_bidirectional_bounded(&mut a, &mut b).await;
        st.udp_relays.lock().await.remove(&vid);
    });
}

/// visitor 侧通道抵达(route_data_stream 调用):
/// TCP 写 START_VISITOR_RELAY(最后一帧)→ 交付配对;UDP(user_conn_id=0)直接交付。
/// compress 来自签发时的两端一致性决策(purpose 携带,免再查库)。
pub async fn on_visitor_channel(
    state: &Arc<AppState>,
    session_id: &str,
    request_id: u64,
    visitor_id: u64,
    user_conn_id: u64,
    compress: bool,
    mut stream: BoxedIo,
) {
    if user_conn_id != 0 {
        let mut flags = 0u8;
        if compress {
            flags |= forward_flags::COMPRESSED;
        }
        let relay = StartVisitorRelay {
            visitor_id,
            user_conn_id,
            flags,
        };
        let payload = serde_json::to_vec(&relay).unwrap();
        if rh_protocol::write_frame(&mut stream, FrameType::START_VISITOR_RELAY, &payload)
            .await
            .is_err()
        {
            return;
        }
    }
    deliver_pair(state, session_id, request_id, stream).await;
}

/// 代理侧中继通道抵达:TCP 写 START_FORWARD 后交付;UDP 直接交付。
pub async fn on_relay_proxy_channel(
    state: &Arc<AppState>,
    session_id: &str,
    request_id: u64,
    proxy_id: u64,
    src_hint: &str,
    compress: bool,
    mut stream: BoxedIo,
) {
    let Some(rt) = state.proxies.get(proxy_id) else {
        warn!(proxy_id, "relay proxy channel: proxy runtime missing");
        return;
    };
    // R1:锁内只取 START_FORWARD 需要的 6 个字段,修前 `spec.clone()`
    // 深克隆整个 ProxySpec(每条被中继的连接一次)。
    let fwd = {
        let spec = rt.spec.read().await;
        if spec.kind == ProxyKind::Sudp {
            // UDP 语义:无指派帧,直接交付(客户端按 UDP 通道处理)
            drop(spec);
            deliver_pair(state, session_id, request_id, stream).await;
            return;
        }
        let mut flags = 0u8;
        if compress {
            flags |= forward_flags::COMPRESSED;
        }
        StartForward {
            proxy_id: spec.id,
            user_src: src_hint.to_string(),
            target_host: spec.target_host.clone(),
            target_port: spec.target_port,
            flags,
            proxy_proto: conv_proxy_proto(spec.transport.proxy_proto),
            proxy_name: spec.name.clone(),
            xtcp_auth: String::new(),
        }
    };
    let payload = serde_json::to_vec(&fwd).unwrap();
    if rh_protocol::write_frame(&mut stream, FrameType::START_FORWARD, &payload)
        .await
        .is_err()
    {
        warn!(proxy_id, "relay proxy channel: start forward write failed");
        return;
    }
    tracing::debug!(proxy_id, request_id, "relay proxy channel assigned");
    deliver_pair(state, session_id, request_id, stream).await;
}

async fn deliver_pair(state: &Arc<AppState>, session_id: &str, request_id: u64, stream: BoxedIo) {
    let slot = state
        .relay_pairs
        .lock()
        .await
        .remove(&(session_id.to_string(), request_id));
    if let Some(tx) = slot {
        let _ = tx.send(stream);
    } else {
        warn!(
            session_id,
            request_id, "relay pair slot missing (expired or collision)"
        );
    }
}

/// 中继拼管(TCP):裸双向搬运。指派帧(START_FORWARD/START_VISITOR_RELAY)
/// 已在两侧通道抵达时写入并带压缩 flags,压缩由两端客户端各自完成。
async fn splice_relay(
    state: Arc<AppState>,
    proxy_id: u64,
    proxy_stream: BoxedIo,
    visitor_stream: BoxedIo,
) {
    let mut a = proxy_stream;
    // 计量取 visitor 侧(用户口径),归因到目标代理(design-parity 决策 3)
    let rt = state.proxies.get(proxy_id);
    let _conn_guard = match crate::data::try_open_user_conn(&state, rt.as_ref()) {
        Some(g) => g,
        None => return, // F2 配额超限:中继不建立
    };
    let mut b = crate::data::CountedUserIo::user(visitor_stream, &state, rt);
    let _ = crate::data::copy_bidirectional_bounded(&mut a, &mut b).await;
}

fn conv_proxy_proto(v: rh_protocol::ProxyProtoVersion) -> rh_protocol::message::ProxyProto {
    match v {
        rh_protocol::ProxyProtoVersion::None => rh_protocol::message::ProxyProto::None,
        rh_protocol::ProxyProtoVersion::V1 => rh_protocol::message::ProxyProto::V1,
        rh_protocol::ProxyProtoVersion::V2 => rh_protocol::message::ProxyProto::V2,
    }
}
