//! XTCP 打洞协调:双端 STUN 探测 → 候选交换 → 打洞指令;失败回退中继。

use crate::session::Session;
use crate::state::AppState;
use rh_protocol::message::{NatHoleConnect, NatHolePrepare, NatHoleReport, ServerMessage};
use rh_protocol::{ProxySpec, VisitorSpec};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tracing::info;
use uuid::Uuid;

#[derive(Clone)]
struct NatSession {
    visitor_client: String,
    proxy_client: String,
    user_conn_id: u64,
    visitor_report: Option<NatHoleReport>,
    proxy_report: Option<NatHoleReport>,
    created: Instant,
    /// A7:一次性随机数(随 PREPARE/CONNECT 下发)。
    nonce: String,
    /// A7:直连首帧凭据 HMAC(目标规则 secret_key, nonce);规则未配置密钥
    /// 时等于 nonce(能力令牌:拿到 nonce 才能通过)。
    auth: String,
}

/// 打洞事务表。
pub struct NatCoordinator {
    txs: Mutex<HashMap<String, NatSession>>,
    /// 已发起过打洞的 (visitor 会话 id, user_conn_id):重发 UserConnect = 打洞失败
    /// 的回退信号,直接走中继,不再重复打洞(否则形成 打洞→失败→重发→打洞 循环,
    /// 等待连接在 10s TTL 内耗尽)。以 session_id 为键:user_conn_id 是客户端进程内
    /// 计数器,客户端重启后会重新从 1 开始,按 client_id 键会跨重启误判。
    attempted: Mutex<HashMap<(String, u64), Instant>>,
}

impl NatCoordinator {
    pub fn new() -> Self {
        Self {
            txs: Mutex::new(HashMap::new()),
            attempted: Mutex::new(HashMap::new()),
        }
    }

    /// 该连接是否已尝试过打洞(并登记本次尝试)。TTL 60s 防泄漏。
    pub async fn mark_attempted(&self, visitor_session_id: &str, user_conn_id: u64) -> bool {
        let key = (visitor_session_id.to_string(), user_conn_id);
        let mut m = self.attempted.lock().await;
        m.retain(|_, t| t.elapsed() < Duration::from_secs(60));
        m.insert(key, Instant::now()).is_some()
    }
}

impl Default for NatCoordinator {
    fn default() -> Self {
        Self::new()
    }
}

/// 发起打洞协调。返回 true = 应回退中继;false = 已发起协调(或失败踢回)。
pub async fn start(
    state: &Arc<AppState>,
    visitor_sess: &Arc<Session>,
    vspec: &VisitorSpec,
    pspec: &ProxySpec,
    proxy_sess: &Arc<Session>,
    user_conn_id: u64,
) -> bool {
    let txid = Uuid::new_v4().to_string();
    // A7:直连首帧凭据。nonce 每次打洞唯一;auth 用目标规则当前密钥计算,
    // 使打洞窗口内的第三方(未收到 CONNECT)无法伪造合法首帧。
    let nonce = Uuid::new_v4().simple().to_string();
    let auth = match pspec.secret_cfg().map(|c| c.secret_key.clone()) {
        Some(k) if !k.is_empty() => crate::control::hmac_hex(k.as_bytes(), &nonce),
        _ => nonce.clone(),
    };
    let timeout = state.cfg.nathole.prepare_timeout_s.max(3);
    {
        let mut txs = state.nathole.txs.lock().await;
        txs.retain(|_, s| s.created.elapsed() < Duration::from_secs(timeout + 10));
        txs.insert(
            txid.clone(),
            NatSession {
                visitor_client: visitor_sess.client_id.clone(),
                proxy_client: proxy_sess.client_id.clone(),
                user_conn_id,
                visitor_report: None,
                proxy_report: None,
                created: Instant::now(),
                nonce: nonce.clone(),
                auth: auth.clone(),
            },
        );
    }
    let prep = ServerMessage::NatHolePrepare(NatHolePrepare {
        transaction_id: txid.clone(),
        timeout_seconds: timeout,
        nonce: nonce.clone(),
    });
    let ok1 = visitor_sess.send(prep.clone()).await;
    let ok2 = proxy_sess.send(prep).await;
    if !ok1 || !ok2 {
        state.nathole.txs.lock().await.remove(&txid);
        return true;
    }
    info!(tx = %txid, visitor = vspec.name, proxy = pspec.name, "nathole prepare sent");
    // 超时守护:未齐备则回退中继
    let st = state.clone();
    let tx2 = txid.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(timeout)).await;
        decide(&st, &tx2).await;
    });
    false
}

/// 双端候选齐备(或超时)→ 下发打洞指令或回退。
/// 读取即原子移除:齐备触发的 decide 与超时守护的 decide 只有一个能拿到事务,
/// 避免双端各收到两条重复的打洞指令。
async fn decide(state: &Arc<AppState>, txid: &str) {
    let (vr, pr, v_client, p_client, uc, nonce, auth) = {
        let mut txs = state.nathole.txs.lock().await;
        let Some(ns) = txs.remove(txid) else { return };
        (
            ns.visitor_report,
            ns.proxy_report,
            ns.visitor_client,
            ns.proxy_client,
            ns.user_conn_id,
            ns.nonce,
            ns.auth,
        )
    };
    let Some(v_sess) = state.sessions.get(&v_client) else {
        return;
    };
    let Some(p_sess) = state.sessions.get(&p_client) else {
        return;
    };
    match (vr.as_ref(), pr.as_ref()) {
        (Some(v), Some(p)) => {
            // 下发:visitor 拨号方,proxy 监听方
            let punch = state.cfg.nathole.punch_timeout_s.max(5);
            let connect_v = ServerMessage::NatHoleConnect(NatHoleConnect {
                transaction_id: txid.to_string(),
                user_conn_id: uc,
                candidates: p.candidates.clone(),
                role: "dial".into(),
                punch_seconds: punch,
                fallback_relay: true,
                relay_mode: false,
                nonce: nonce.clone(),
                auth: auth.clone(),
            });
            let connect_p = ServerMessage::NatHoleConnect(NatHoleConnect {
                transaction_id: txid.to_string(),
                user_conn_id: uc,
                candidates: v.candidates.clone(),
                role: "listen".into(),
                punch_seconds: punch,
                fallback_relay: true,
                relay_mode: false,
                nonce: nonce.clone(),
                auth: auth.clone(),
            });
            let _ = v_sess.send(connect_v).await;
            let _ = p_sess.send(connect_p).await;
            info!(tx = txid, "nathole connect instructions sent");
        }
        _ => {
            // 超时未齐:双端回退通知(visitor 侧转中继)
            let fb = ServerMessage::NatHoleConnect(NatHoleConnect {
                transaction_id: txid.to_string(),
                user_conn_id: uc,
                candidates: vec![],
                role: "fallback".into(),
                punch_seconds: 0,
                fallback_relay: true,
                relay_mode: true,
                nonce: String::new(),
                auth: String::new(),
            });
            let _ = v_sess.send(fb).await;
        }
    }
}

/// 客户端候选上报。
pub async fn on_report(state: &Arc<AppState>, sess: &Arc<Session>, r: NatHoleReport) {
    let txid = r.transaction_id.clone();
    let ready;
    {
        let mut txs = state.nathole.txs.lock().await;
        let Some(ns) = txs.get_mut(&txid) else { return };
        if sess.client_id == ns.visitor_client && sess.client_id == ns.proxy_client {
            // 同一客户端同时持有两端:首报计 visitor,次报计 proxy
            if ns.visitor_report.is_none() {
                ns.visitor_report = Some(r);
            } else {
                ns.proxy_report = Some(r);
            }
        } else if sess.client_id == ns.visitor_client {
            ns.visitor_report = Some(r);
        } else if sess.client_id == ns.proxy_client {
            ns.proxy_report = Some(r);
        }
        ready = ns.visitor_report.is_some() && ns.proxy_report.is_some();
    }
    if ready {
        decide(state, &txid).await;
    }
}
