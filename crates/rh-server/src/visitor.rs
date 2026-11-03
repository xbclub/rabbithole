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
            // R4:seqlock 校验半边 —— 先捕获 gen,DB 读之后再复核;期间
            // 任何 bump_cfg_gen(属主或目标客户端)都令本次放弃入缓存
            //(结果照常返回,只是不缓存)。修前 gen 在 DB 读之后才读,
            // 「读完旧数据 → 管理 bump → 读到新 gen」的交错会把旧数据
            // 钉死在新 gen 上,直到下一次 bump 才自愈。
            let gen0_owner = state.cfg_gen(&sess.client_id);
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
            // R5:目标侧同样先捕获 gen(pspec 属目标客户端;R4 只做了
            // 属主半边 —— 「读 pspec(旧)→ 目标 bump → 读 gen_target(新)」
            // 仍可把旧 pspec 钉进新 gen)
            let gen0_target = state.cfg_gen(&v_target_client);
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
            let gen_owner = state.cfg_gen(&sess.client_id);
            let gen_target = state.cfg_gen(&v_target_client);
            let entry = Arc::new(VisitorResolution {
                gen: gen_owner,
                target_gen: gen_target,
                visitor_id: vspec.id,
                target_client: v_target_client,
                vspec,
                pspec,
            });
            if gen_owner == gen0_owner && gen_target == gen0_target {
                state.visitor_cache.insert(cache_key.clone(), entry.clone());
            }
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
        // 计量归因到目标代理(visitor 流量按目标归属,design-parity 决策 3)
        let _conn_guard = match crate::data::try_open_user_conn(&st, prt.as_ref()) {
            Some(g) => g,
            None => {
                st.udp_relays.lock().await.remove(&vid);
                return;
            }
        };
        // S3c:sudp 接线,与 splice_relay(S3b)同式:user 位=b(visitor 侧
        // 计量流)、channel 位=a(代理侧通道,剥 QuotaIo 后偷 halves)。
        // prt 为 None(proxy 已删)保持现状回落 copy。
        let b = crate::data::CountedUserIo::user(b, &st, prt.clone());
        if let Some(rt) = prt {
            crate::data::relay_user_channel(&st, &rt, b, a).await;
        } else {
            let _ =
                crate::data::copy_bidirectional_bounded(a, Box::new(b) as rh_transport::BoxedIo)
                    .await;
        }
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
    // 计量取 visitor 侧(用户口径),归因到目标代理(design-parity 决策 3)
    let rt = state.proxies.get(proxy_id);
    let _conn_guard = match crate::data::try_open_user_conn(&state, rt.as_ref()) {
        Some(g) => g,
        None => return, // F2 配额超限:中继不建立
    };
    // S3b:stcp 接线 —— 计量流 b 必须在 user 位(mux_relay 的 a 侧,读计
    // in/写计 out 与 CountedUserIo::user 语义不变)、proxy_stream 在
    // channel 位(剥 QuotaIo 后偷 RhMuxStream halves)。参数位相对现状
    // copy_bidirectional_bounded(proxy, b) 互换是设计要求:现状 proxy 在
    // user 位、计量流在 channel 位;两向泵语义逐向一致(现状 up=proxy读→
    // b写、down=b读→proxy写 = chunk 泵 b_read(proxy读)→a写 / a读(b读)
    // →b_write(proxy写))。快路径需要 rt 的 rule_cancel/bucket/归因,
    // None=proxy 已删,保持现状回落 copy。
    let b = crate::data::CountedUserIo::user(visitor_stream, &state, rt.clone());
    if let Some(rt) = rt {
        crate::data::relay_user_channel(&state, &rt, b, proxy_stream).await;
    } else {
        let _ = crate::data::copy_bidirectional_bounded(
            proxy_stream,
            Box::new(b) as rh_transport::BoxedIo,
        )
        .await;
    }
}

fn conv_proxy_proto(v: rh_protocol::ProxyProtoVersion) -> rh_protocol::message::ProxyProto {
    match v {
        rh_protocol::ProxyProtoVersion::None => rh_protocol::message::ProxyProto::None,
        rh_protocol::ProxyProtoVersion::V1 => rh_protocol::message::ProxyProto::V1,
        rh_protocol::ProxyProtoVersion::V2 => rh_protocol::message::ProxyProto::V2,
    }
}

#[cfg(test)]
mod tests {
    //! S3b 接线点直驱:splice_relay 是 stcp 中继的唯一拼管入口(relay_tcp
    //! spawn 内调用),sudp 点(relay_udp spawn 内)依赖 issue_relay_pair
    //! 签发链不可单测,由本测试 + data.rs relay_user_channel 入口直测的
    //! 同构覆盖 + 编译期位次护栏覆盖(盲区如实声明)。
    use super::*;

    async fn state() -> Arc<AppState> {
        let db = crate::db::Db::open("sqlite::memory:").await.unwrap();
        Arc::new(AppState::new(crate::config::ServerConfig::default(), db))
    }

    fn tcp_spec() -> ProxySpec {
        serde_json::from_value(serde_json::json!({
            "name": "visitor-splice",
            "kind": "stcp",
            "client_id": "cid",
            "bind_ip": "127.0.0.1",
            "bind_port": 0,
            "target_host": "127.0.0.1",
            "target_port": 9,
        }))
        .unwrap()
    }

    fn relay_path_count(state: &Arc<AppState>, path: &str) -> u64 {
        state
            .metrics
            .relay_path_total
            .with_label_values(&[path])
            .get()
    }

    fn pseudo_random(n: usize, seed: u64) -> Vec<u8> {
        let mut out = Vec::with_capacity(n);
        let mut x = seed | 1;
        for _ in 0..n {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            out.push((x >> 24) as u8);
        }
        out
    }

    /// proxy_stream 形态:QuotaIo 包 yamux server accept 流(route_data_stream
    /// 包装链复刻);测试持 client open 侧作对端。visitor_stream 为独立 duplex。
    /// 注意 open_stream/accept 已返回 BoxedIo,不得再 Box::new(会得到
    /// Box<Box<dyn Io>>,is_mux_stream 必 miss → 静默回落 copy)。
    async fn proxy_channel_pair() -> (BoxedIo, BoxedIo) {
        let (a, b) = tokio::io::duplex(256 * 1024);
        let server = rh_transport::wrap_stream_as_server(Box::new(b), true, 64);
        let client = rh_transport::wrap_stream_as_client(Box::new(a), true, 64);
        let c = client.open_stream().await.unwrap();
        let s = server.accept().await.unwrap();
        (s, c)
    }

    /// 接线点直驱:rt 已注册 → splice_relay → relay_user_channel 命中 chunk;
    /// 断言 chunk +1 与双向穿透(visitor→proxy / proxy→visitor)。
    #[tokio::test]
    async fn visitor_splice_relay_chunk_engages() {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
        let state = state().await;
        let rt = Arc::new(crate::state::ProxyRuntime::new(
            4001,
            "cid".to_string(),
            tcp_spec(),
        ));
        state.proxies.runtimes.insert(4001, rt.clone());
        let chunk0 = relay_path_count(&state, "chunk");

        let (s, c) = proxy_channel_pair().await;
        let (tx, _rx) = tokio::sync::mpsc::channel(1);
        let sess = crate::session::Session::new(
            "cid".into(),
            "sid".into(),
            1,
            tokio_util::sync::CancellationToken::new(),
            tx,
        );
        sess.data_conn_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let proxy_stream = crate::control::QuotaIo::new(s, sess);

        let (mut vp, vs) = tokio::io::duplex(256 * 1024);
        let st = state.clone();
        let mut relay = tokio::spawn(splice_relay(
            st,
            4001,
            proxy_stream,
            Box::new(vs) as BoxedIo,
        ));

        // up:visitor(vp)写 → relay user 读计 in → proxy 通道 → c 收
        let up = pseudo_random(64 * 1024 + 21, 0x0f0f_0f0f_0f0f_0f0f);
        let (mut c_r, mut c_w) = tokio::io::split(c);
        vp.write_all(&up).await.unwrap();
        let mut got = vec![0u8; up.len()];
        c_r.read_exact(&mut got).await.unwrap();
        assert_eq!(got, up, "visitor→proxy 方向必须穿透");

        // down:c 写 → relay mux 读 → user 写计 out → vp 收
        let down = pseudo_random(64 * 1024 + 43, 0xf0f0_f0f0_f0f0_f0f0);
        c_w.write_all(&down).await.unwrap();
        let mut got = vec![0u8; down.len()];
        vp.read_exact(&mut got).await.unwrap();
        assert_eq!(got, down, "proxy→visitor 方向必须穿透");

        c_w.shutdown().await.unwrap();
        vp.shutdown().await.unwrap();
        tokio::time::timeout(Duration::from_secs(10), &mut relay)
            .await
            .expect("双向 EOF 后 splice_relay 必须收敛")
            .unwrap();

        assert_eq!(
            relay_path_count(&state, "chunk"),
            chunk0 + 1,
            "stcp 接线点必须命中 chunk(剥 QuotaIo 后偷 halves)"
        );
    }

    /// rt=None 分支(proxy 已删):保持现状回落 copy_bidirectional_bounded,
    /// 不计任何 relay_path(与修前一致),数据照常穿透。
    #[tokio::test]
    async fn visitor_splice_relay_proxy_missing_falls_back() {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
        let state = state().await;
        let chunk0 = relay_path_count(&state, "chunk");
        let copy0 = relay_path_count(&state, "copy");

        let (s, c) = proxy_channel_pair().await;
        let (tx, _rx) = tokio::sync::mpsc::channel(1);
        let sess = crate::session::Session::new(
            "cid".into(),
            "sid".into(),
            1,
            tokio_util::sync::CancellationToken::new(),
            tx,
        );
        // proxy 缺失形态:splice_relay 不再包 QuotaIo 也成立;保留包装以
        // 忠实复刻交付链(QuotaIo::Drop 在 copy 内随流释放)
        sess.data_conn_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let proxy_stream = crate::control::QuotaIo::new(s, sess);

        let (mut vp, vs) = tokio::io::duplex(256 * 1024);
        let st = state.clone();
        let mut relay = tokio::spawn(splice_relay(
            st,
            4999,
            proxy_stream,
            Box::new(vs) as BoxedIo,
        ));

        let up = pseudo_random(8192, 1);
        let (mut c_r, mut c_w) = tokio::io::split(c);
        vp.write_all(&up).await.unwrap();
        let mut got = vec![0u8; up.len()];
        c_r.read_exact(&mut got).await.unwrap();
        assert_eq!(got, up, "proxy 缺失时数据仍必须穿透(现状回落)");

        let down = pseudo_random(8192, 2);
        c_w.write_all(&down).await.unwrap();
        let mut got = vec![0u8; down.len()];
        vp.read_exact(&mut got).await.unwrap();
        assert_eq!(got, down, "proxy 缺失时 down 方向仍必须穿透");

        c_w.shutdown().await.unwrap();
        vp.shutdown().await.unwrap();
        tokio::time::timeout(Duration::from_secs(10), &mut relay)
            .await
            .expect("proxy 缺失回落 copy 也必须收敛")
            .unwrap();

        assert_eq!(
            relay_path_count(&state, "chunk"),
            chunk0,
            "rt=None 不进快路径(现状)"
        );
        assert_eq!(
            relay_path_count(&state, "copy"),
            copy0,
            "rt=None 回落不计数(与修前 copy_bidirectional_bounded 一致)"
        );
    }

    /// S3b copy 反向:relay_mode=copy 时接线点必须回落 copy(检查点 4 的
    /// 「且有 copy 模式反向断言走 copy」半句;此前的 rt=None 用例不是 copy 档)。
    #[tokio::test]
    async fn visitor_splice_relay_copy_mode_falls_back() {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
        let cfg = crate::config::ServerConfig {
            relay_mode: "copy".to_string(),
            ..crate::config::ServerConfig::default()
        };
        let db = crate::db::Db::open("sqlite::memory:").await.unwrap();
        let state = Arc::new(AppState::new(cfg, db));
        let rt = Arc::new(crate::state::ProxyRuntime::new(
            4002,
            "cid".to_string(),
            tcp_spec(),
        ));
        state.proxies.runtimes.insert(4002, rt.clone());
        let chunk0 = relay_path_count(&state, "chunk");
        let copy0 = relay_path_count(&state, "copy");

        let (s, c) = proxy_channel_pair().await;
        let (tx, _rx) = tokio::sync::mpsc::channel(1);
        let sess = crate::session::Session::new(
            "cid".into(),
            "sid".into(),
            1,
            tokio_util::sync::CancellationToken::new(),
            tx,
        );
        sess.data_conn_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let proxy_stream = crate::control::QuotaIo::new(s, sess);

        let (mut vp, vs) = tokio::io::duplex(256 * 1024);
        let st = state.clone();
        let mut relay = tokio::spawn(splice_relay(
            st,
            4002,
            proxy_stream,
            Box::new(vs) as BoxedIo,
        ));

        let up = pseudo_random(8192, 3);
        let (mut c_r, mut c_w) = tokio::io::split(c);
        vp.write_all(&up).await.unwrap();
        let mut got = vec![0u8; up.len()];
        c_r.read_exact(&mut got).await.unwrap();
        assert_eq!(got, up, "copy 模式 visitor→proxy 必须照常穿透");

        let down = pseudo_random(8192, 4);
        c_w.write_all(&down).await.unwrap();
        let mut got = vec![0u8; down.len()];
        vp.read_exact(&mut got).await.unwrap();
        assert_eq!(got, down, "copy 模式 proxy→visitor 必须照常穿透");

        c_w.shutdown().await.unwrap();
        vp.shutdown().await.unwrap();
        tokio::time::timeout(Duration::from_secs(10), &mut relay)
            .await
            .expect("copy 模式 splice_relay 必须收敛")
            .unwrap();

        assert_eq!(
            relay_path_count(&state, "chunk"),
            chunk0,
            "copy 模式接线点不得走 chunk(红线 1)"
        );
        assert_eq!(
            relay_path_count(&state, "copy"),
            copy0 + 1,
            "copy 模式接线点必须回落 copy"
        );
    }

    /// S3c 调用点直驱(sudp):真 relay_udp 入口(issue_relay_pair 签发 +
    /// deliver_pair 交付生产形态通道),驱动到接线点。锁三件事:
    /// ① 生产交付链(QuotaIo 包装 mux 流)必须命中 chunk——任何交付形态
    ///    漂移(多一层壳 → is_mux_stream miss)在此显红;
    /// ② 双向数据完整性;
    /// ③ 收敛(spawn 尾部 udp_relays 槽位回收)。
    /// 关于 a/b 取位(复审关注点)的如实结论:接线点直接互换
    /// relay_user_channel(&st,&rt,a,b) **编译不过**(b 已先包 CountedUserIo,
    /// a 是裸 BoxedIo,位次护栏 E0308 拦截——mutation 实测);而交付端
    /// (pr_res, vr_res) 处的互换虽可编译,但 SUDP 两侧均为无协议帧的
    /// 对称透传流(START_FORWARD 不经此路;计量归属同一 rt,双向字节
    /// 相同),数据面行为**构造上等价**——mutation 实测方向断言不红,
    /// 且不存在可观测差异可供断言,故不为它虚构用例。
    async fn sudp_call_site_case(relay_mode: &'static str) -> (u64, u64) {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
        let cfg = crate::config::ServerConfig {
            relay_mode: relay_mode.to_string(),
            ..crate::config::ServerConfig::default()
        };
        let db = crate::db::Db::open("sqlite::memory:").await.unwrap();
        let state = Arc::new(AppState::new(cfg, db));
        // sudp 目标规则(prt 来源;relay_user_channel 内部自读其 spec)
        let pspec: ProxySpec = serde_json::from_value(serde_json::json!({
            "id": 6001,
            "name": "sudp-target",
            "kind": "sudp",
            "client_id": "pcid",
            "bind_ip": "127.0.0.1",
            "bind_port": 0,
            "target_host": "127.0.0.1",
            "target_port": 9,
        }))
        .unwrap();
        let rt = Arc::new(crate::state::ProxyRuntime::new(
            6001,
            "pcid".to_string(),
            pspec.clone(),
        ));
        state.proxies.runtimes.insert(6001, rt.clone());
        let chunk0 = relay_path_count(&state, "chunk");
        let copy0 = relay_path_count(&state, "copy");

        // relay_udp 不查 sessions 注册表(issue_relay_pair 直接用传入会话;
        // 签发的 CreateDataChannel 落入各自 queue_tx,测试不消费即可)
        let (vtx, _vrx) = tokio::sync::mpsc::channel(16);
        let vsess = crate::session::Session::new(
            "vcid".into(),
            "vsid".into(),
            1,
            tokio_util::sync::CancellationToken::new(),
            vtx,
        );
        let (ptx, _prx) = tokio::sync::mpsc::channel(16);
        let psess = crate::session::Session::new(
            "pcid".into(),
            "psid".into(),
            1,
            tokio_util::sync::CancellationToken::new(),
            ptx,
        );
        let vspec: rh_protocol::VisitorSpec = serde_json::from_value(serde_json::json!({
            "name": "sudp-visitor",
            "kind": "sudp",
            "bind_port": 0,
            "target_proxy_name": "sudp-target",
        }))
        .unwrap();

        relay_udp(&state, &vsess, &vspec, &pspec, &psess).await;

        // 两条独立 mux 流对:proxy 侧(交付给 RelayProxy 请求 → a)、
        // visitor 侧(交付给 VisitorRelay 请求 → b);QuotaIo 包装复刻
        // route_data_stream 交付形态
        let (s_p, mut c_p) = proxy_channel_pair().await;
        let (s_v, mut c_v) = proxy_channel_pair().await;
        psess
            .data_conn_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        vsess
            .data_conn_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        // 从配对表按 session_id 摘 rid,经生产交付入口 deliver_pair 投递
        let (v_rid, p_rid) = {
            let pairs = state.relay_pairs.lock().await;
            let find = |sid: &str| {
                pairs
                    .keys()
                    .find(|(s, _)| s == sid)
                    .map(|(_, r)| *r)
                    .unwrap_or_else(|| panic!("配对表缺 {sid} 的槽位"))
            };
            (find(&vsess.session_id), find(&psess.session_id))
        };
        deliver_pair(
            &state,
            &psess.session_id,
            p_rid,
            crate::control::QuotaIo::new(s_p, psess.clone()),
        )
        .await;
        deliver_pair(
            &state,
            &vsess.session_id,
            v_rid,
            crate::control::QuotaIo::new(s_v, vsess.clone()),
        )
        .await;

        // ---- 双向数据完整性(经生产交付链)----
        // visitor 对端(c_v)写 → user 位(b)读 → channel 位(a)写 →
        // proxy 对端(c_p)收;反向同理。
        let up = pseudo_random(32 * 1024 + 7, 0x5ec5_0001);
        c_v.write_all(&up).await.unwrap();
        let mut got = vec![0u8; up.len()];
        c_p.read_exact(&mut got).await.unwrap();
        assert_eq!(got, up, "visitor→proxy 方向必须穿透(生产交付链)");

        let down = pseudo_random(32 * 1024 + 9, 0x5ec5_0002);
        c_p.write_all(&down).await.unwrap();
        let mut got = vec![0u8; down.len()];
        c_v.read_exact(&mut got).await.unwrap();
        assert_eq!(got, down, "proxy→visitor 方向必须穿透(生产交付链)");

        // 半关闭 + 收敛:relay 在 relay_udp 的 spawn 内,以计数器到达与
        // udp_relays 槽位回收(spawn 尾部语句)判定收敛
        c_v.shutdown().await.unwrap();
        c_p.shutdown().await.unwrap();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while state.udp_relays.lock().await.contains(&vspec.id) {
            assert!(
                tokio::time::Instant::now() < deadline,
                "relay 完成后必须回收 udp_relays 槽位(收敛证明)"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        (
            relay_path_count(&state, "chunk") - chunk0,
            relay_path_count(&state, "copy") - copy0,
        )
    }

    /// S3c:sudp 接线点必须命中 chunk(生产 issue_relay_pair→deliver_pair 交付链)。
    #[tokio::test]
    async fn relay_udp_call_site_chunk_engages() {
        let (chunk, copy) =
            tokio::time::timeout(Duration::from_secs(20), sudp_call_site_case("splice"))
                .await
                .expect("用例 deadline");
        assert_eq!(
            chunk, 1,
            "S3c 接线点必须走 chunk(chunk={chunk}, copy={copy})"
        );
        assert_eq!(copy, 0, "S3c 不得回落 copy(chunk={chunk}, copy={copy})");
    }

    /// S3c copy 反向:relay_mode=copy 关闭 chunk(红线 1)。
    #[tokio::test]
    async fn relay_udp_call_site_copy_mode_falls_back() {
        let (chunk, copy) =
            tokio::time::timeout(Duration::from_secs(20), sudp_call_site_case("copy"))
                .await
                .expect("用例 deadline");
        assert_eq!(
            chunk, 0,
            "copy 模式不得走 chunk(红线 1)(chunk={chunk}, copy={copy})"
        );
        assert_eq!(
            copy, 1,
            "copy 模式必须回落 copy(chunk={chunk}, copy={copy})"
        );
    }
}
