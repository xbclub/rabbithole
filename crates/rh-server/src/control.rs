//! 控制面:传输层 accept、首帧分流、nonce 挑战认证、会话生命周期、
//! 控制消息处理与数据通道四重校验。

use crate::session::{RequestPurpose, Session};
use crate::state::AppState;
use hmac::{Hmac, Mac};
use rh_protocol::message::{ClientMessage, Hello, ServerMessage};
use rh_protocol::{read_frame, write_frame, FrameType, PROTOCOL_VERSION};
use rh_transport::{BoxedIo, Incoming};
use sha2::Sha256;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::AsyncWriteExt;
use tokio::sync::{mpsc, Mutex};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

/// nonce 表:nonce_hex → (client_id, expires)。
/// 绑定签发连接:仅在签发连接上被消费。
pub struct ChallengeTable {
    inner: Mutex<HashMap<String, ChallengeEntry>>,
}

/// 表容量上限:防止 连接→HELLO→收挑战→断开 的慢速积累耗尽内存。
const CHALLENGE_TABLE_CAP: usize = 65536;

struct ChallengeEntry {
    client_id: String,
    expires: Instant,
}

impl ChallengeTable {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(HashMap::new()),
        }
    }

    pub async fn issue(&self, client_id: &str) -> String {
        let nonce =
            uuid::Uuid::new_v4().simple().to_string() + &uuid::Uuid::new_v4().simple().to_string();
        let mut g = self.inner.lock().await;
        // 顺手清理过期项(定期的清扫兜底,见 sweeper)
        let now = Instant::now();
        g.retain(|_, e| e.expires > now);
        if g.len() >= CHALLENGE_TABLE_CAP {
            // 拒绝新挑战:空 nonce 使后续 consume 必然失败,连接被 AUTH_FAILED 拒绝
            return String::new();
        }
        g.insert(
            nonce.clone(),
            ChallengeEntry {
                client_id: client_id.to_string(),
                expires: Instant::now() + Duration::from_secs(30),
            },
        );
        nonce
    }

    /// 取出即删;返回归属 client_id。
    pub async fn consume(&self, nonce: &str) -> Option<String> {
        let mut g = self.inner.lock().await;
        let e = g.remove(nonce)?;
        if e.expires < Instant::now() {
            return None;
        }
        Some(e.client_id)
    }

    /// 定期清扫过期 nonce(持有有效 client_id 的对端反复连接后断开
    /// 不会触发 consume,必须靠周期清扫回收)。
    pub async fn sweeper(self: Arc<Self>) {
        let mut tick = tokio::time::interval(Duration::from_secs(30));
        loop {
            tick.tick().await;
            let now = Instant::now();
            let removed = {
                let mut g = self.inner.lock().await;
                let before = g.len();
                g.retain(|_, e| e.expires > now);
                before - g.len()
            };
            if removed > 0 {
                tracing::debug!(removed, "challenge table swept");
            }
        }
    }
}

impl Default for ChallengeTable {
    fn default() -> Self {
        Self::new()
    }
}

/// 控制端口 accept 循环。
pub async fn serve_transport(state: Arc<AppState>, cfg: rh_transport::ServerTransportConfig) {
    let listener = match rh_transport::listen(&cfg, state.mux_streams.clone()).await {
        Ok(l) => l,
        Err(e) => {
            tracing::error!(error = %e, proto = %cfg.protocol, "control listen failed");
            return;
        }
    };
    info!(addr = %listener.local_addr, proto = %listener.protocol, "control listener up");
    let mut listener = listener;
    loop {
        let inc = match listener.accept().await {
            Ok(i) => i,
            Err(e) => {
                warn!(error = %e, "accept error");
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
        };
        let st = state.clone();
        tokio::spawn(async move {
            let remote = inc.remote;
            if let Err(e) = handle_incoming(st, inc).await {
                tracing::debug!(error = %e, %remote, "incoming closed");
            }
        });
    }
}

async fn handle_incoming(state: Arc<AppState>, inc: Incoming) -> anyhow::Result<()> {
    // 首流(或裸连接本体)
    let stream = tokio::time::timeout(Duration::from_secs(5), inc.mux.accept()).await??;
    // 首帧 5s
    let mut stream = stream;
    let mut buf = bytes::BytesMut::with_capacity(1024);
    let (ty, payload) =
        tokio::time::timeout(Duration::from_secs(5), read_frame(&mut stream, &mut buf))
            .await
            .map_err(|_| anyhow::anyhow!("first frame timeout"))??;
    match ty {
        FrameType::HELLO => {
            let hello: Hello = serde_json::from_slice(&payload)?;
            // 未认证并发上限(每 IP):认证完成或失败即释放
            let ip = inc.remote.ip();
            if !state.enter_unauth(&ip) {
                let _ = send_error(&mut stream, "AUTH_FAILED", "too many pending auth").await;
                return Ok(());
            }
            let r = auth_and_run(state.clone(), inc, stream, hello, buf).await;
            state.exit_unauth(&ip);
            r
        }
        FrameType::DATA_CHANNEL_HELLO => {
            // mux 模式下数据通道为后续流;裸模式每连接一流,此处即数据连接。
            let stream = if !buf.is_empty() {
                rh_transport::PrependedIo::wrap(stream, buf.to_vec())
            } else {
                stream
            };
            handle_data_channel_hello(state, inc, stream, &payload).await
        }
        _ => Err(anyhow::anyhow!("unexpected first frame {ty:?}")),
    }
}

// ---------------- 认证 ----------------

async fn auth_and_run(
    state: Arc<AppState>,
    inc: Incoming,
    mut stream: BoxedIo,
    hello: Hello,
    mut read_buf: bytes::BytesMut,
) -> anyhow::Result<()> {
    // 附加载体报到(round-9 §3.4):HELLO 携带 session_id = 多载体会话的
    // 后续物理连接,走聚合快速路径;None = 首条连接,走完整挑战应答。
    if let Some(sid) = hello.session_id.clone() {
        return attach_additional_carrier(state, inc, stream, hello, &sid).await;
    }
    if hello.version.split('.').next() != PROTOCOL_VERSION.split('.').next() {
        let _ = send_error(&mut stream, "VERSION_MISMATCH", "incompatible protocol").await;
        return Ok(());
    }
    let client = match state.db.get_client(&hello.client_id).await? {
        Some(c) => c,
        None => {
            let _ = send_error(&mut stream, "AUTH_FAILED", "unknown client").await;
            return Ok(());
        }
    };
    if !client.enabled {
        let _ = send_error(&mut stream, "CLIENT_DISABLED", "client disabled").await;
        return Ok(());
    }
    // 挑战应答(HMAC-SHA256,常量时间比较)
    let nonce = state.challenge.issue(&client.id).await;
    write_frame(
        &mut stream,
        FrameType::AUTH_CHALLENGE,
        &serde_json::to_vec(&rh_protocol::message::AuthChallenge {
            nonce: nonce.clone(),
        })?,
    )
    .await?;
    let (ty, payload) = tokio::time::timeout(
        Duration::from_secs(30),
        read_frame(&mut stream, &mut read_buf),
    )
    .await
    .map_err(|_| anyhow::anyhow!("auth response timeout"))??;
    if ty == FrameType::ERROR {
        anyhow::bail!("client aborted during auth");
    }
    if ty != FrameType::AUTH_RESPONSE {
        anyhow::bail!("expected AUTH_RESPONSE, got {ty:?}");
    }
    let resp: rh_protocol::message::AuthResponse = serde_json::from_slice(&payload)?;
    // nonce 一次性消费,且必须归属本次连接认证的客户端
    let owner = match state.challenge.consume(&nonce).await {
        Some(o) => o,
        None => {
            let _ = send_error(&mut stream, "AUTH_FAILED", "challenge expired").await;
            return Ok(());
        }
    };
    if owner != client.id {
        let _ = send_error(&mut stream, "AUTH_FAILED", "challenge mismatch").await;
        return Ok(());
    }
    let mut mac = Hmac::<Sha256>::new_from_slice(client.secret.as_bytes()).expect("hmac key");
    mac.update(format!("rh2:{}:{}", client.id, nonce).as_bytes());
    let sig = hex::decode(resp.auth.trim()).unwrap_or_default();
    if mac.verify_slice(&sig).is_err() {
        warn!(client = %client.id, "challenge auth failed");
        let _ = send_error(&mut stream, "AUTH_FAILED", "bad credentials").await;
        return Ok(());
    }
    let lock = state.serial_lock(&client.id);
    let _g = lock.lock().await;
    let latest = state
        .db
        .get_client(&client.id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("client gone"))?;
    if !latest.enabled {
        let _ = send_error(&mut stream, "CLIENT_DISABLED", "client disabled").await;
        return Ok(());
    }
    let session_id = uuid::Uuid::new_v4().to_string();
    let epoch = state.sessions.next_epoch();
    let cancel = CancellationToken::new();
    let (qtx, qrx) =
        mpsc::channel::<ServerMessage>(state.limits.read().unwrap().control_send_queue);
    let sess = Session::new(latest.id.clone(), session_id.clone(), epoch, cancel, qtx);
    if let Some(old) = state.sessions.register(sess.clone()) {
        info!(client = %latest.name, old_epoch = old.epoch, "session replaced");
        old.cancel.cancel();
    }
    drop(_g);
    let ack = rh_protocol::message::HelloAck {
        ok: true,
        error_code: None,
        session_id: Some(session_id),
        heartbeat_interval_s: 30,
        server_version: PROTOCOL_VERSION.to_string(),
    };
    write_frame(
        &mut stream,
        FrameType::HELLO_ACK,
        &serde_json::to_vec(&ack)?,
    )
    .await?;
    let _ = state
        .db
        .update_client_meta(&latest.id, &hello.version, &hello.hostname, &hello.os)
        .await;
    let entities = state.build_sync_entities(&latest.id).await;
    let rev = latest.config_revision.max(0) as u64;
    sess.mark_unconfirmed(rev).await;
    sess.send(ServerMessage::ServiceSync(
        rh_protocol::message::ServiceSync {
            revision: rev,
            entities,
        },
    ))
    .await;
    crate::runtime::on_session_up(&state, &sess).await;
    run_session_loop(state, sess, inc, stream, read_buf, qrx).await
}

/// 附加载体报到快速路径(round-9 §3.3/§3.4):校验归属(client_id 的
/// 活跃 Session 且 session_id 匹配,红线 ⑤)后把该物理连接的 yamux 聚合
/// 进会话池,此后该连接上的数据流由 [`run_session_loop`] 的合流接收路由。
/// 认证口径(规格 §3.4 方案 A 简化):session_id 为仅下发给已认证客户端
/// 的 UUIDv4,持有即视为同一客户端;DB 侧仍复查 enabled。附加为尽力而为,
/// 校验失败只拒本条连接,不影响主会话。
async fn attach_additional_carrier(
    state: Arc<AppState>,
    inc: Incoming,
    mut stream: BoxedIo,
    hello: Hello,
    session_id: &str,
) -> anyhow::Result<()> {
    // round-10:每条返回路径都回 CarrierAttachAck(客户端据此定容)。
    // 回包尽力而为:写失败只 warn,不影响会话;拒绝路径仍按原语义
    // 返回 Ok(本连接关闭,主会话不受影响)。
    let cap = state.cfg.max_carriers();
    if hello.version.split('.').next() != PROTOCOL_VERSION.split('.').next() {
        reject_carrier(
            &mut stream,
            cap,
            "VERSION_MISMATCH",
            &hello.client_id,
            session_id,
        )
        .await;
        let _ = send_error(&mut stream, "VERSION_MISMATCH", "incompatible protocol").await;
        return Ok(());
    }
    let Some(sess) = state.sessions.get(&hello.client_id) else {
        reject_carrier(&mut stream, cap, "NO_SESSION", &hello.client_id, session_id).await;
        let _ = send_error(&mut stream, "AUTH_FAILED", "no active session").await;
        return Ok(());
    };
    if sess.session_id != session_id {
        reject_carrier(
            &mut stream,
            cap,
            "SESSION_MISMATCH",
            &hello.client_id,
            session_id,
        )
        .await;
        let _ = send_error(&mut stream, "AUTH_FAILED", "session mismatch").await;
        return Ok(());
    }
    match state.db.get_client(&hello.client_id).await {
        Ok(Some(c)) if c.enabled => {}
        Ok(_) => {
            reject_carrier(
                &mut stream,
                cap,
                "CLIENT_DISABLED",
                &hello.client_id,
                session_id,
            )
            .await;
            let _ = send_error(&mut stream, "CLIENT_DISABLED", "client disabled").await;
            return Ok(());
        }
        Err(e) => {
            // round-10.1 §2.4:DB 错误也必须回 CarrierAttachAck(客户端只认
            // ACK 帧;缺包会让该载体白等 2s 超时)。
            warn!(client_id = %hello.client_id, error = %e, "carrier attach db lookup failed");
            reject_carrier(&mut stream, cap, "INTERNAL", &hello.client_id, session_id).await;
            return Ok(());
        }
    }
    // 上限强制(round-10.1 §2.3):cap 含首载体,池内只装附加载体,故允许
    // 本次附加的条件是 `现有附加载体数 + 1(首载体)+ 1(本次) <= cap`。
    // round-10.2 §2.3:计数用**成员表长度**(含 dead-but-unreaped)而非
    // 存活数——已死未 reap 的成员仍占用物理连接与池内名额;按存活数计会
    // 在健康监视 reap 前的窗口内多放行 1 条。
    // **检查非原子**:两个附加连接并发报到时可能都通过检查并各自聚合,
    // 短暂超出 1 条;严格串行化需把池锁与 attach 耦合,超出本任务范围。
    // 拒绝路径不聚合(红线:必须走 ACK 而非 send_error)。
    let existing = {
        let g = sess.mux_pool.lock().await;
        g.clone()
    };
    let pool_members = match existing.as_ref() {
        Some(p) => p.pool_member_count().await.unwrap_or(0),
        None => 0,
    };
    if pool_members + 2 > cap {
        reject_carrier(&mut stream, cap, "POOL_FULL", &hello.client_id, session_id).await;
        info!(
            client_id = %hello.client_id,
            session_id,
            pool_members,
            cap,
            "pool carrier rejected (cap)"
        );
        return Ok(());
    }
    // 取出该连接的 yamux 载体:此路径独占 Incoming(未与任何 accept 循环
    // 共享),try_unwrap 必成;裸模式连接(AcceptSingle)无 yamux,不聚合。
    let mux_owned = match Arc::try_unwrap(inc.mux) {
        Ok(m) => m,
        Err(_) => {
            reject_carrier(
                &mut stream,
                cap,
                "CARRIER_SHARED",
                &hello.client_id,
                session_id,
            )
            .await;
            return Ok(());
        }
    };
    let Some(yamux) = mux_owned.into_yamux() else {
        reject_carrier(&mut stream, cap, "NOT_MUXED", &hello.client_id, session_id).await;
        let _ = send_error(&mut stream, "AUTH_FAILED", "carrier not muxed").await;
        return Ok(());
    };
    // 聚合进会话池:首个附加载体建池,后续成员走 attach_carrier(追加 +
    // accept 转发 + 唤醒,池内写锁完成)。锁内无 await 之外的旁路,Session
    // 移除时池随 Session drop,附加载体连接随之关闭(§3.7)。
    let pool_size = {
        let mut pool = sess.mux_pool.lock().await;
        match pool.take() {
            None => {
                *pool = Some(rh_transport::Multiplexer::yamux_pool_server(vec![yamux]));
                // None→Some 升级必须补通知:数据接受循环此刻可能 park 在
                // 单源分支(仅首载体),池自身的 changed 只唤醒其订阅者,
                // 不通知则池侧合流通道的新流无人接收(§3.6 唤醒缝隙)
                sess.mux_pool_changed.notify_waiters();
            }
            Some(existing) => {
                let attached = existing.attach_yamux_carrier(yamux).await;
                *pool = Some(existing);
                if !attached {
                    // round-10.1 §2.4:池变体意外不支持聚合也要回 ACK。
                    drop(pool);
                    reject_carrier(&mut stream, cap, "NOT_POOLED", &hello.client_id, session_id)
                        .await;
                    return Ok(());
                }
            }
        }
        match pool.as_ref() {
            Some(p) => p.pool_live_carriers().await.unwrap_or(1),
            None => 1,
        }
    };
    // 成功应答必须在 shutdown 之前写出(客户端在报到流上等 2s)。
    let ack = rh_protocol::message::CarrierAttachAck {
        ok: true,
        error_code: None,
        max_carriers: cap,
    };
    // round-10.1 §2.4:序列化失败不得 `?` 早退(那样客户端收不到任何帧),
    // 改用静态回退 JSON(字面量;round-10.2 §2.4 提取为可单测的构造函数)。
    let payload = match serde_json::to_vec(&ack) {
        Ok(p) => p,
        Err(e) => {
            warn!(error = %e, "carrier attach ack encode failed");
            ack_fallback_json(cap)
        }
    };
    match tokio::time::timeout(
        Duration::from_secs(1),
        write_frame(&mut stream, FrameType::CARRIER_ATTACH_ACK, &payload),
    )
    .await
    {
        Ok(Ok(())) => {}
        Ok(Err(e)) => warn!(
            client_id = %hello.client_id,
            session_id = %session_id,
            error = %e,
            "carrier attach ack write failed"
        ),
        Err(_) => warn!(
            client_id = %hello.client_id,
            session_id = %session_id,
            "carrier attach ack write timeout"
        ),
    }
    info!(
        client_id = %hello.client_id,
        session_id = %session_id,
        pool_size,
        "pool carrier attached"
    );
    // 报到流礼貌半关闭(客户端读到应答后自行 FIN)
    let _ = stream.shutdown().await;
    Ok(())
}

/// CarrierAttachAck 序列化失败时的静态回退载荷(round-10.2 §2.4):客户端
/// 只认 ACK 帧,序列化失败也必须发出可解析的 `ok:false` 报文而非静默断开。
fn ack_fallback_json(cap: usize) -> Vec<u8> {
    format!(r#"{{"ok":false,"error_code":"INTERNAL","max_carriers":{cap}}}"#).into_bytes()
}

/// 拒绝载体报到:先回 `ok:false` 应答(客户端据此把该载体计入失败),
/// 再记 warn。写应答超时/失败不阻断——随后仍按原路径发 ERROR 并关连接。
async fn reject_carrier(
    stream: &mut BoxedIo,
    cap: usize,
    error_code: &str,
    client_id: &str,
    session_id: &str,
) {
    let ack = rh_protocol::message::CarrierAttachAck {
        ok: false,
        error_code: Some(error_code.to_string()),
        max_carriers: cap,
    };
    let payload = match serde_json::to_vec(&ack) {
        Ok(p) => p,
        Err(e) => {
            warn!(error = %e, "carrier attach ack encode failed");
            return;
        }
    };
    match tokio::time::timeout(
        Duration::from_secs(1),
        write_frame(stream, FrameType::CARRIER_ATTACH_ACK, &payload),
    )
    .await
    {
        Ok(Ok(())) => {}
        Ok(Err(e)) => {
            warn!(error = %e, "carrier attach reject write failed")
        }
        Err(_) => warn!("carrier attach reject write timeout"),
    }
    warn!(
        client_id,
        session_id, error_code, "pool carrier attach rejected"
    );
}

pub fn hmac_hex(secret: &[u8], msg: &str) -> String {
    let mut mac = Hmac::<Sha256>::new_from_slice(secret).expect("hmac key");
    mac.update(msg.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

async fn send_error(stream: &mut BoxedIo, code: &str, message: &str) -> std::io::Result<()> {
    let e = ServerMessage::Error(rh_protocol::message::ErrorMessage {
        code: code.to_string(),
        message: message.to_string(),
    });
    write_frame(stream, FrameType::ERROR, &serde_json::to_vec(&e)?).await?;
    stream.shutdown().await
}

// ---------------- 会话循环 ----------------

async fn run_session_loop(
    state: Arc<AppState>,
    sess: Arc<Session>,
    inc: Incoming,
    mut stream: BoxedIo,
    mut read_buf: bytes::BytesMut,
    mut qrx: mpsc::Receiver<ServerMessage>,
) -> anyhow::Result<()> {
    // 数据流接受循环(mux 模式:同物理连接的后续流均为数据通道;
    // round-9:附加载体聚合进 sess.mux_pool 后,池侧新流经同一合流路由)。
    // 首条载体由本循环直读:其 YamuxMultiplexer 迁入池需独占所有权,而
    // 本循环持有其 Arc,故采用"首载体 + 池"双源合流——每条流恰从一个
    // 源到达,语义与单池等价;mux_pool 为 None(无附加载体)时单源分支
    // 仅多一项永不触发的池升级等待,行为与旧路径零漂移。两 accept 均
    // 取消安全(mpsc recv),select 落败侧的流不丢失。
    let mux = inc.mux.clone();
    let data_accept = tokio::spawn({
        let st = state.clone();
        let sess = sess.clone();
        async move {
            loop {
                let s = {
                    let pool = sess.mux_pool.lock().await.clone();
                    match pool {
                        Some(p) => tokio::select! {
                            r = mux.accept() => match r { Ok(s) => s, Err(_) => break },
                            r = p.accept() => match r { Ok(s) => s, Err(_) => break },
                        },
                        None => {
                            // 单源路径与池升级等待并跑:park 期间附加载体
                            // (报到/重拨补位)可能把 mux_pool 升级为 Some,
                            // 只 park 在首载体 accept 的话,池侧合流通道的
                            // 新流无人接收。先 enable 订阅再复查池态,升级
                            // 落入复查后间隙也能被唤醒;唤醒即重读走 Some
                            // 分支(池一旦建立即复用同一合流通道,后续附加
                            // 载体靠通道 recv 自行唤醒,无需再通知)。
                            let upgraded = sess.mux_pool_changed.notified();
                            tokio::pin!(upgraded);
                            upgraded.as_mut().enable();
                            if sess.mux_pool.lock().await.is_some() {
                                continue;
                            }
                            tokio::select! {
                                r = mux.accept() => match r { Ok(s) => s, Err(_) => break },
                                _ = &mut upgraded => continue,
                            }
                        }
                    }
                };
                let st2 = st.clone();
                let sess2 = sess.clone();
                tokio::spawn(async move {
                    let mut s = s;
                    let mut b = bytes::BytesMut::with_capacity(256);
                    let r =
                        tokio::time::timeout(Duration::from_secs(10), read_frame(&mut s, &mut b))
                            .await;
                    if let Ok(Ok((FrameType::DATA_CHANNEL_HELLO, p))) = r {
                        // read_frame 可能把同批到达的后续帧留在 b;必须前传
                        let s = if !b.is_empty() {
                            rh_transport::PrependedIo::wrap(s, b.to_vec())
                        } else {
                            s
                        };
                        if let Err(e) =
                            crate::control::route_data_stream(st2, sess2, s, &p, None).await
                        {
                            tracing::debug!(error = %e, "data channel route failed");
                        }
                    }
                });
            }
        }
    });
    // 心跳:3×30s 未收到任何客户端帧则判死(30s 检查一次,非无条件下线)
    let mut hb = tokio::time::interval(Duration::from_secs(30));
    hb.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let _ = hb.tick().await;
    let mut last_frame = std::time::Instant::now();
    let result: anyhow::Result<()> = loop {
        tokio::select! {
            r = read_frame(&mut stream, &mut read_buf) => {
                last_frame = std::time::Instant::now();
                match r {
                    Ok((ty, payload)) => {
                        if ty == FrameType::PING {
                            let p: rh_protocol::message::Ping = match serde_json::from_slice(&payload) {
                                Ok(p) => p,
                                Err(_) => continue,
                            };
                            let _ = sess.send(ServerMessage::Pong(rh_protocol::message::Pong {
                                ts_ms: p.ts_ms,
                                probe: false,
                            })).await;
                            continue;
                        }
                        let msg: ClientMessage = match serde_json::from_slice(&payload) {
                            Ok(m) => m,
                            Err(e) => break Err(anyhow::anyhow!("bad message: {e}")),
                        };
                        if matches!(msg, ClientMessage::DataChannelHello(_)) {
                            continue; // 数据通道不经过控制流
                        }
                        let q = sess.queue_tx.clone();
                        handle_client_message(state.clone(), sess.clone(), msg, &q).await;
                    }
                    Err(e) => break Err(anyhow::anyhow!("control read: {e}")),
                }
            }
            out = qrx.recv() => {
                match out {
                    Some(msg) => {
                        let payload = serde_json::to_vec(&msg)?;
                        if write_frame(&mut stream, frame_type_of(&msg), &payload).await.is_err() {
                            break Err(anyhow::anyhow!("control write failed"));
                        }
                    }
                    None => break Err(anyhow::anyhow!("queue closed")),
                }
            }
            _ = sess.cancel.cancelled() => {
                let _ = stream.shutdown().await;
                break Ok(());
            }
            _ = hb.tick() => {
                if last_frame.elapsed() >= Duration::from_secs(90) {
                    break Err(anyhow::anyhow!("heartbeat timeout"));
                }
            }
            _ = state.shutdown.cancelled() => {
                sess.cancel.cancel();
                break Ok(());
            }
        }
    };
    data_accept.abort();
    // round-9.1:会话拆除必须显式关停多载体池——池 Drop 虽兜底,但 Session
    // 可能被在途数据流短暂续持,显式关停让附加载体连接/driver/转发任务
    // 立即收敛(不随 Session 引用计数延后)。
    if let Some(pool) = sess.mux_pool.lock().await.as_ref() {
        pool.shutdown_pool();
    }
    // 下线清理:仅当注册表仍是本会话时
    state.sessions.remove_if_current(&sess.client_id, &sess);
    state
        .metrics
        .client_online
        .set(state.sessions.count_online() as i64);
    sess.cancel.cancel();
    crate::runtime::on_session_down(&state, &sess).await;
    let name = state
        .db
        .get_client(&sess.client_id)
        .await
        .ok()
        .flatten()
        .map(|c| c.name)
        .unwrap_or_else(|| sess.client_id.clone());
    state
        .events
        .emit("info", "client", &format!("客户端已离线:{name}"))
        .await;
    result
}

fn frame_type_of(msg: &ServerMessage) -> FrameType {
    match msg {
        ServerMessage::AuthChallenge(_) => FrameType::AUTH_CHALLENGE,
        ServerMessage::HelloAck(_) => FrameType::HELLO_ACK,
        ServerMessage::ServiceSync(_) => FrameType::SERVICE_SYNC,
        ServerMessage::ServiceUpdate(_) => FrameType::SERVICE_UPDATE,
        ServerMessage::ServiceRemove(_) => FrameType::SERVICE_REMOVE,
        ServerMessage::CreateDataChannel(_) => FrameType::CREATE_DATA_CHANNEL,
        ServerMessage::StartForward(_) => FrameType::START_FORWARD,
        ServerMessage::StartVisitorRelay(_) => FrameType::START_VISITOR_RELAY,
        ServerMessage::Pong(_) => FrameType::PONG,
        ServerMessage::NatHolePrepare(_) => FrameType::NAT_HOLE_PREPARE,
        ServerMessage::NatHoleConnect(_) => FrameType::NAT_HOLE_CONNECT,
        ServerMessage::Error(_) => FrameType::ERROR,
    }
}

// ---------------- 消息处理 ----------------

/// 处理一条来自客户端的控制消息。
pub async fn handle_client_message(
    state: Arc<AppState>,
    sess: Arc<Session>,
    msg: ClientMessage,
    reply: &mpsc::Sender<ServerMessage>,
) {
    match msg {
        ClientMessage::Ping(p) => {
            let _ = reply
                .send(ServerMessage::Pong(rh_protocol::message::Pong {
                    ts_ms: p.ts_ms,
                    probe: p.probe,
                }))
                .await;
        }
        ClientMessage::ApplyResult(r) => {
            crate::runtime::on_apply_result(&state, &sess, &r).await;
        }
        ClientMessage::SyncRequest {} => {
            // 全量重发
            let entities = state.build_sync_entities(&sess.client_id).await;
            let client = state.db.get_client(&sess.client_id).await.ok().flatten();
            let rev = client.map(|c| c.config_revision.max(0) as u64).unwrap_or(0);
            sess.mark_unconfirmed(rev).await;
            let _ = reply
                .send(ServerMessage::ServiceSync(
                    rh_protocol::message::ServiceSync {
                        revision: rev,
                        entities,
                    },
                ))
                .await;
        }
        ClientMessage::UserConnect(u) => {
            crate::visitor::on_user_connect(&state, &sess, u).await;
        }
        ClientMessage::HealthReport(h) => {
            crate::runtime::on_health_report(&state, &sess, h).await;
        }
        ClientMessage::NatHoleReport(r) => {
            crate::nathole::on_report(&state, &sess, r).await;
        }
        ClientMessage::Hello(_) | ClientMessage::AuthResponse(_) => {
            // 非首帧出现:协议违例,忽略(由读循环决定是否断开)
        }
        ClientMessage::DataChannelHello(_) => {
            // mux 模式下不会出现在控制流;忽略
        }
    }
}

// ---------------- 数据通道 ----------------

/// 数据通道流路由:校验 DATA_CHANNEL_HELLO 后按 purpose 分发。
/// raw 模式下由 accept 循环直接调用;mux 模式由数据接受任务调用。
pub async fn route_data_stream(
    state: Arc<AppState>,
    sess: Arc<Session>,
    stream: BoxedIo,
    payload: &[u8],
    _extra: Option<()>,
) -> anyhow::Result<()> {
    let hello: rh_protocol::message::DataChannelHello = serde_json::from_slice(payload)?;
    if hello.session_id != sess.session_id {
        return Err(anyhow::anyhow!("session mismatch"));
    }
    // 四重校验之 4:request_id 一次性
    let req = sess
        .take_pending_request(hello.request_id)
        .await
        .ok_or_else(|| anyhow::anyhow!("request_id invalid/expired"))?;
    // 限额:占用期 = 通道存活期(随流释放,避免只增不减把会话饿死)
    let count = sess
        .data_conn_count
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        + 1;
    // 有效上限已钳制到 mux 顶-1024:应用层必然先于 yamux 拒绝,
    // 不会触发 yamux 的超限 SYN → 终止整条会话
    if count as usize > state.limits.read().unwrap().data_conn_limit() {
        sess.data_conn_count
            .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
        return Err(anyhow::anyhow!("data connection limit"));
    }
    let stream = QuotaIo::new(stream, sess.clone());
    match req.purpose {
        RequestPurpose::TcpPool { proxy_id } => {
            crate::data::tcp_on_channel(&state, &sess, proxy_id, hello.request_id, stream).await;
        }
        RequestPurpose::UdpChannel { proxy_id } => {
            crate::data::udp_on_channel(&state, &sess, proxy_id, stream).await;
        }
        RequestPurpose::VisitorRelay {
            visitor_id,
            user_conn_id,
            compress,
        } => {
            crate::visitor::on_visitor_channel(
                &state,
                &sess.session_id,
                hello.request_id,
                visitor_id,
                user_conn_id,
                compress,
                stream,
            )
            .await;
        }
        RequestPurpose::RelayProxy { proxy_id, compress } => {
            crate::visitor::on_relay_proxy_channel(
                &state,
                &sess.session_id,
                hello.request_id,
                proxy_id,
                "",
                compress,
                stream,
            )
            .await;
        }
    }
    Ok(())
}

/// 数据通道限额守卫流:流被丢弃即释放该通道的限额占用。
pub(crate) struct QuotaIo {
    inner: rh_transport::BoxedIo,
    sess: Arc<Session>,
}

impl QuotaIo {
    // 返回装箱 Io 而非 Self(直接可用作 BoxedIo)
    #[allow(clippy::new_ret_no_self)] // 返回装箱 Io(直接可用作 BoxedIo)
    pub(crate) fn new(inner: rh_transport::BoxedIo, sess: Arc<Session>) -> rh_transport::BoxedIo {
        Box::new(Self { inner, sess })
    }

    /// 内部流引用(splice 快路径:剥壳提取裸 TCP;限额语义随原对象生命周期)。
    /// 唯一调用方是 data.rs 的 linux-only splice 分支,与
    /// `CountedUserIo::inner_ref` 同款按平台裁剪,非 linux 平台不留死代码。
    #[cfg(target_os = "linux")]
    pub(crate) fn inner_ref(&self) -> &rh_transport::BoxedIo {
        &self.inner
    }

    /// 按所有权剥壳(goal-repair B1:仅 TcpPool→assign_forward 转发路径):
    /// 返回(内层流, 限额守卫)。守卫释放(或本体 Drop)即递减
    /// data_conn_count,与 QuotaIo::Drop 完全等价——限额占用期 = 转发期,
    /// 不得因剥壳提前释放。非 QuotaIo 包装时原样返回(守卫 None)。
    pub(crate) fn peel(io: rh_transport::BoxedIo) -> (rh_transport::BoxedIo, Option<QuotaGuard>) {
        // 探测经显式 &dyn Io 接收者(vtable 派发到具体类型;Box 层
        // 转发实现会让 owned/&Box 接收者解析到 Box<dyn Io> 本体)
        fn is_quota_io(io: &rh_transport::BoxedIo) -> bool {
            let dynio: &dyn rh_transport::Io = &**io;
            dynio.as_any().downcast_ref::<QuotaIo>().is_some()
        }
        if !is_quota_io(&io) {
            return (io, None);
        }
        let any = io.into_any_box();
        let boxed = match any.downcast::<QuotaIo>() {
            Ok(b) => b,
            Err(_) => unreachable!("as_any 已确认具体类型为 QuotaIo"),
        };
        // 整体移出 Box(移动 Drop 类型的整体是允许的),再经 ManuallyDrop
        // 拆字段:两字段所有权都转交给返回值,原对象不再 Drop,
        // data_conn_count 由 QuotaGuard 恰好递减一次。
        let md = std::mem::ManuallyDrop::new(*boxed);
        // SAFETY:md 置于 ManuallyDrop,绝不运行 QuotaIo::Drop;
        // 两个 ptr::read 各自独占地移出字段,无重复释放/双重递减。
        let inner = unsafe { std::ptr::read(&md.inner) };
        let sess = unsafe { std::ptr::read(&md.sess) };
        (inner, Some(QuotaGuard { sess }))
    }
}

/// 剥壳后的数据通道限额守卫(goal-repair B1):复刻 QuotaIo::Drop 的
/// data_conn_count 递减;随转发任务存活,结束即释放占用。
pub(crate) struct QuotaGuard {
    sess: Arc<Session>,
}

impl Drop for QuotaGuard {
    fn drop(&mut self) {
        self.sess
            .data_conn_count
            .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
    }
}

impl Drop for QuotaIo {
    fn drop(&mut self) {
        self.sess
            .data_conn_count
            .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
    }
}

impl tokio::io::AsyncRead for QuotaIo {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let inner = &mut self.get_mut().inner;
        std::pin::Pin::new(inner).poll_read(cx, buf)
    }
}

impl tokio::io::AsyncWrite for QuotaIo {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        let inner = &mut self.get_mut().inner;
        std::pin::Pin::new(inner).poll_write(cx, buf)
    }
    fn poll_write_vectored(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        bufs: &[std::io::IoSlice<'_>],
    ) -> std::task::Poll<std::io::Result<usize>> {
        // 限额守卫是透传包装:向量写必须透传,避免上层 writev 优化退化
        let inner = &mut self.get_mut().inner;
        std::pin::Pin::new(inner).poll_write_vectored(cx, bufs)
    }
    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }
    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let inner = &mut self.get_mut().inner;
        std::pin::Pin::new(inner).poll_flush(cx)
    }
    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let inner = &mut self.get_mut().inner;
        std::pin::Pin::new(inner).poll_shutdown(cx)
    }
}

/// 裸(非 mux)模式:每条新物理连接 = 数据连接,首帧 DATA_CHANNEL_HELLO。
pub async fn handle_data_channel_hello(
    state: Arc<AppState>,
    inc: Incoming,
    stream: BoxedIo,
    payload: &[u8],
) -> anyhow::Result<()> {
    let hello: rh_protocol::message::DataChannelHello = serde_json::from_slice(payload)?;
    // 查找会话
    let sess = state
        .sessions
        .online_clients()
        .into_iter()
        .find_map(|cid| {
            state
                .sessions
                .get(&cid)
                .filter(|s| s.session_id == hello.session_id)
        })
        .ok_or_else(|| anyhow::anyhow!("no session"))?;
    // round-10 重拨补位:客户端池重拨的新载体未再发报到 HELLO,其首流
    // 直接是数据通道——把该连接聚合进会话池,后续流不再搁浅。单载体
    // (mux 非池化)与裸模式/QUIC/KCP(into_yamux 为 None)零影响:
    // 单载体模式下数据通道不会出现在新物理连接的首流(必经控制连接)。
    //
    // round-10 复核修正(red line §2.3.3-2):此路径**不得**回
    // `CarrierAttachAck`。这里的 `stream` 就是数据通道本身(首帧
    // DATA_CHANNEL_HELLO),随后要交给 `route_data_stream`;客户端在
    // HELLO 之后只接受 START_FORWARD(UDP 语义下是 UDP_PACKET),任何
    // 额外帧都会让该通道被判协议错误关闭——新客户端与旧客户端皆然。
    // 报到应答只发给真正的报到流(见 `attach_additional_carrier`)。
    //
    // round-10.1 §2.3:补位路径同样强制 max_carriers。超限则**不聚合**,
    // 该载体不在池内,但其数据流仍按原路径服务(仅此一条)。超限时不能
    // shutdown 该载体——那会连带杀掉马上要路由的这条数据流;改为在
    // route_data_stream 期间持有 mux(保活该连接上的读写 driver),路由
    // 结束后随作用域释放。
    let mut rejected_mux: Option<rh_transport::mux::YamuxMultiplexer> = None;
    if let Some(yamux) = Arc::try_unwrap(inc.mux).ok().and_then(|m| m.into_yamux()) {
        let cap = state.cfg.max_carriers();
        let existing = {
            let g = sess.mux_pool.lock().await;
            g.clone()
        };
        // round-10.2 §2.3:与报到路径同口径——按成员表长度(含 dead 未
        // reap 的占位成员)计数,而非存活数;检查非原子窗口依然存在。
        let pool_members = match existing.as_ref() {
            Some(p) => p.pool_member_count().await.unwrap_or(0),
            None => 0,
        };
        if pool_members + 2 > cap {
            info!(
                client_id = %sess.client_id,
                session_id = %sess.session_id,
                pool_members,
                cap,
                "pool carrier rejected (cap, data-channel refill)"
            );
            rejected_mux = Some(yamux);
        } else {
            let mut pool = sess.mux_pool.lock().await;
            match pool.take() {
                None => {
                    *pool = Some(rh_transport::Multiplexer::yamux_pool_server(vec![yamux]));
                    // 同 attach_additional_carrier:升级补通知,唤醒可能仍
                    // park 在单源分支的数据接受循环
                    sess.mux_pool_changed.notify_waiters();
                }
                Some(existing) => {
                    let attached = existing.attach_yamux_carrier(yamux).await;
                    *pool = Some(existing);
                    if !attached {
                        warn!(
                            client_id = %sess.client_id,
                            "session mux pool is not a yamux pool (refill)"
                        );
                    }
                }
            }
        }
    }
    let routed = route_data_stream(state, sess, stream, payload, None).await;
    drop(rejected_mux);
    routed
}

#[cfg(test)]
mod carrier_ack_tests {
    //! round-10 报到应答(T-S1/T-S2)直连单测:不经完整客户端,直接构造
    //! 「已 accept 的 yamux 物理连接 + 报到流」,驱动
    //! `attach_additional_carrier` 并读回报文。覆盖成功(ok:true +
    //! max_carriers)与 session 不匹配(ok:false + SESSION_MISMATCH)。
    use super::*;
    use crate::config::ServerConfig;
    use rh_transport::Protocol;

    const CID: &str = "carrier-ack-client";

    async fn state_with(cap: usize) -> Arc<AppState> {
        let db = crate::db::Db::open("sqlite::memory:").await.unwrap();
        db.create_client(CID, "carrier-ack", &"0".repeat(64), "")
            .await
            .unwrap();
        let cfg = ServerConfig {
            max_carriers: cap,
            ..ServerConfig::default()
        };
        Arc::new(AppState::new(cfg, db))
    }

    fn register_session(state: &Arc<AppState>, sid: &str) -> Arc<Session> {
        let (tx, _rx) = mpsc::channel(8);
        let sess = Session::new(CID.into(), sid.into(), 1, CancellationToken::new(), tx);
        state.sessions.register(sess.clone());
        sess
    }

    fn attach_hello(session_id: &str) -> Hello {
        Hello {
            client_id: CID.into(),
            version: PROTOCOL_VERSION.into(),
            hostname: "h".into(),
            os: "test".into(),
            arch: "test".into(),
            capabilities: Vec::new(),
            session_id: Some(session_id.into()),
        }
    }

    /// 构造一对连通的 yamux 载体并完成"报到流已 accept":
    /// 返回 (客户端侧报到流, 服务端 Incoming, 服务端已 accept 的报到流)。
    async fn carrier_pair() -> (BoxedIo, Incoming, BoxedIo) {
        let (a, b) = tokio::io::duplex(256 * 1024);
        let client = rh_transport::wrap_stream_as_client(Box::new(a), true, 64);
        let server = rh_transport::wrap_stream_as_server(Box::new(b), true, 64);
        let client_stream = client.open_stream().await.expect("client report stream");
        let server_stream = tokio::time::timeout(Duration::from_secs(2), server.accept())
            .await
            .expect("accept timeout")
            .expect("server report stream");
        // 持有客户端 mux 直到会话结束(否则物理连接关闭,服务端写 ACK 失败)
        let inc = Incoming {
            mux: server,
            remote: "127.0.0.1:1".parse().unwrap(),
            protocol: Protocol::Tcp,
        };
        // 客户端 mux 句柄随返回的 client_stream 的持有者一起泄漏一个
        // 后台任务:开流后 driver 仍需存活才能读到 ACK。
        tokio::spawn(async move {
            let _keep = client;
            futures::future::pending::<()>().await;
        });
        (client_stream, inc, server_stream)
    }

    async fn read_ack(stream: &mut BoxedIo) -> rh_protocol::message::CarrierAttachAck {
        let mut buf = bytes::BytesMut::with_capacity(512);
        let (ty, p) = tokio::time::timeout(Duration::from_secs(2), read_frame(stream, &mut buf))
            .await
            .expect("ack timeout")
            .expect("ack frame");
        assert_eq!(ty, FrameType::CARRIER_ATTACH_ACK, "必须是新帧类型");
        serde_json::from_slice(&p).expect("ack json")
    }

    // T-S1:成功报到 → ok:true + max_carriers;成员进入会话池。
    #[tokio::test]
    async fn attach_success_acks_max_carriers() {
        let state = state_with(3).await;
        let sess = register_session(&state, "sid-ok");
        let (mut client_stream, inc, report) = carrier_pair().await;
        attach_additional_carrier(state.clone(), inc, report, attach_hello("sid-ok"), "sid-ok")
            .await
            .unwrap();

        let ack = read_ack(&mut client_stream).await;
        assert!(ack.ok, "成功报到必须 ok:true");
        assert_eq!(ack.error_code, None);
        assert_eq!(ack.max_carriers, 3, "回写服务端上限");
        assert!(sess.mux_pool.lock().await.is_some(), "成功报到必须建池");
    }

    // T-S2:session 不匹配 → ok:false + SESSION_MISMATCH,且不建池。
    #[tokio::test]
    async fn attach_session_mismatch_acks_reject() {
        let state = state_with(8).await;
        let sess = register_session(&state, "sid-real");
        let (mut client_stream, inc, report) = carrier_pair().await;
        attach_additional_carrier(
            state.clone(),
            inc,
            report,
            attach_hello("sid-wrong"),
            "sid-wrong",
        )
        .await
        .unwrap();

        let ack = read_ack(&mut client_stream).await;
        assert!(!ack.ok, "session 不匹配必须拒绝");
        assert_eq!(ack.error_code.as_deref(), Some("SESSION_MISMATCH"));
        assert_eq!(ack.max_carriers, 8, "拒绝时也回写上限(客户端据此收敛)");
        assert!(sess.mux_pool.lock().await.is_none(), "拒绝不得建池");
    }

    // T-S2(变体):无活跃会话 → NO_SESSION。
    #[tokio::test]
    async fn attach_no_session_acks_reject() {
        let state = state_with(8).await;
        let (mut client_stream, inc, report) = carrier_pair().await;
        attach_additional_carrier(
            state.clone(),
            inc,
            report,
            attach_hello("sid-none"),
            "sid-none",
        )
        .await
        .unwrap();

        let ack = read_ack(&mut client_stream).await;
        assert!(!ack.ok);
        assert_eq!(ack.error_code.as_deref(), Some("NO_SESSION"));
    }

    // T-R5(round-10.1 §2.3):cap=1(首载体已占满)时附加载体必须收到
    // ok:false + POOL_FULL,且**不聚合**(会话池仍为 None)。红线:拒绝走
    // ACK 帧而非 send_error(客户端只认 ACK)。
    #[tokio::test]
    async fn attach_over_cap_rejects_pool_full_without_aggregating() {
        let state = state_with(1).await;
        let sess = register_session(&state, "sid-full");
        let (mut client_stream, inc, report) = carrier_pair().await;
        attach_additional_carrier(
            state.clone(),
            inc,
            report,
            attach_hello("sid-full"),
            "sid-full",
        )
        .await
        .unwrap();

        let ack = read_ack(&mut client_stream).await;
        assert!(!ack.ok, "超限必须拒绝");
        assert_eq!(ack.error_code.as_deref(), Some("POOL_FULL"));
        assert_eq!(ack.max_carriers, 1);
        assert!(
            sess.mux_pool.lock().await.is_none(),
            "拒绝路径不得把载体聚合进池"
        );
    }

    // T-R5(边界):cap=2 时第一条附加载体(0+2<=2)允许,第二条(1+2>2)拒绝。
    // 证明"live + 2 <= cap"的计数口径(首载体计入 cap)。
    #[tokio::test]
    async fn attach_cap_counts_first_carrier() {
        let state = state_with(2).await;
        let sess = register_session(&state, "sid-two");
        let (mut c1, inc1, r1) = carrier_pair().await;
        attach_additional_carrier(state.clone(), inc1, r1, attach_hello("sid-two"), "sid-two")
            .await
            .unwrap();
        let ack1 = read_ack(&mut c1).await;
        assert!(ack1.ok, "cap=2 时首条附加载体允许");
        assert_eq!(sess.mux_pool.lock().await.as_ref().map(|_| ()), Some(()));

        let (mut c2, inc2, r2) = carrier_pair().await;
        attach_additional_carrier(state.clone(), inc2, r2, attach_hello("sid-two"), "sid-two")
            .await
            .unwrap();
        let ack2 = read_ack(&mut c2).await;
        assert!(!ack2.ok, "cap=2 时第二条附加载体必须拒绝");
        assert_eq!(ack2.error_code.as_deref(), Some("POOL_FULL"));
    }

    // §2.4:载体 mux 被外部持有时 `Arc::try_unwrap` 失败 → 必须回
    // ok:false + CARRIER_SHARED(修复前为 `?` 早退、客户端收不到任何帧)。
    #[tokio::test]
    async fn attach_shared_carrier_acks_carrier_shared() {
        let state = state_with(8).await;
        register_session(&state, "sid-shared");
        let (a, b) = tokio::io::duplex(256 * 1024);
        let client = rh_transport::wrap_stream_as_client(Box::new(a), true, 64);
        let server = rh_transport::wrap_stream_as_server(Box::new(b), true, 64);
        let mut client_stream = client.open_stream().await.expect("client stream");
        let report = tokio::time::timeout(Duration::from_secs(2), server.accept())
            .await
            .expect("accept timeout")
            .expect("server report stream");
        // 故意保留一个 Arc 克隆 → try_unwrap 必败
        let _shared = server.clone();
        let inc = Incoming {
            mux: server,
            remote: "127.0.0.1:1".parse().unwrap(),
            protocol: Protocol::Tcp,
        };
        tokio::spawn(async move {
            let _keep = client;
            futures::future::pending::<()>().await;
        });
        attach_additional_carrier(
            state.clone(),
            inc,
            report,
            attach_hello("sid-shared"),
            "sid-shared",
        )
        .await
        .unwrap();

        let ack = read_ack(&mut client_stream).await;
        assert!(!ack.ok);
        assert_eq!(ack.error_code.as_deref(), Some("CARRIER_SHARED"));
    }

    // §2.4:非 mux 载体(AcceptSingle)无法聚合 → 回 ok:false + NOT_MUXED。
    #[tokio::test]
    async fn attach_non_muxed_carrier_acks_not_muxed() {
        let state = state_with(8).await;
        register_session(&state, "sid-plain");
        let (peer, b) = tokio::io::duplex(256 * 1024);
        let server = rh_transport::wrap_stream_as_server(Box::new(b), false, 64);
        let report = server.accept().await.expect("single stream");
        let inc = Incoming {
            mux: server,
            remote: "127.0.0.1:1".parse().unwrap(),
            protocol: Protocol::Tcp,
        };
        attach_additional_carrier(
            state.clone(),
            inc,
            report,
            attach_hello("sid-plain"),
            "sid-plain",
        )
        .await
        .unwrap();
        // AcceptSingle 的 ack 写回物理流的对端
        let mut peer_io: BoxedIo = Box::new(peer);
        let ack = read_ack(&mut peer_io).await;
        assert!(!ack.ok);
        assert_eq!(ack.error_code.as_deref(), Some("NOT_MUXED"));
    }

    // ===== round-10.2 §2.4:三条此前无测试的回包路径 =====

    // DB 查询错误 → 必须回 ok:false + INTERNAL(修复前无覆盖;缺包会让
    // 客户端在报到流上白等 2s 超时)。
    #[tokio::test]
    async fn db_error_returns_internal_ack() {
        let state = state_with(8).await;
        register_session(&state, "sid-db");
        let (mut client_stream, inc, report) = carrier_pair().await;
        // 关闭连接池:后续 get_client 返回 PoolClosed 错误。
        state.db.close_for_test().await;
        attach_additional_carrier(state.clone(), inc, report, attach_hello("sid-db"), "sid-db")
            .await
            .unwrap();

        let ack = read_ack(&mut client_stream).await;
        assert!(!ack.ok, "DB 错误必须回 ok:false");
        assert_eq!(ack.error_code.as_deref(), Some("INTERNAL"));
        assert_eq!(ack.max_carriers, 8, "回写服务端上限");
        assert!(state.sessions.get(CID).is_some(), "主会话不受影响");
    }

    // 会话池已存在但不是 YamuxPool 变体(attach_yamux_carrier == false)→
    // 必须回 ok:false + NOT_POOLED,且不把载体聚合进去。
    #[tokio::test]
    async fn not_pooled_returns_ack() {
        let state = state_with(8).await;
        let sess = register_session(&state, "sid-notpooled");
        // 预置一个单载体 Yamux(非池)变体:attach_yamux_carrier 返回 false。
        let (x, _y) = tokio::io::duplex(256 * 1024);
        let single = rh_transport::wrap_stream_as_server(Box::new(x), true, 64);
        let single_weak = Arc::downgrade(&single);
        *sess.mux_pool.lock().await = Some(single);

        let (mut client_stream, inc, report) = carrier_pair().await;
        attach_additional_carrier(
            state.clone(),
            inc,
            report,
            attach_hello("sid-notpooled"),
            "sid-notpooled",
        )
        .await
        .unwrap();

        let ack = read_ack(&mut client_stream).await;
        assert!(!ack.ok, "非池变体必须拒绝聚合");
        assert_eq!(ack.error_code.as_deref(), Some("NOT_POOLED"));
        assert_eq!(ack.max_carriers, 8);
        // 原池保持原样(仍是同一个 Arc,未被替换或追加成员)。
        let same = sess
            .mux_pool
            .lock()
            .await
            .as_ref()
            .is_some_and(|p| single_weak.upgrade().is_some_and(|s| Arc::ptr_eq(&s, p)));
        assert!(same, "不得把载体聚合进非池变体");
    }

    // 序列化失败回退:CarrierAttachAck 字段均为 serde 可序列化类型,实际
    // 无法注入编码失败;按规格对回退常量做字节级断言——必须是合法 JSON、
    // ok:false 且带 INTERNAL 与 cap(修复前回退是内联字面量,无命名常量
    // 可测;改为函数后本测试才成立)。
    #[test]
    fn ack_serialize_failure_falls_back() {
        let raw = ack_fallback_json(7);
        let ack: rh_protocol::message::CarrierAttachAck =
            serde_json::from_slice(&raw).expect("fallback must be valid JSON");
        assert!(!ack.ok);
        assert_eq!(ack.error_code.as_deref(), Some("INTERNAL"));
        assert_eq!(ack.max_carriers, 7, "回退载荷必须携带服务端上限");
        assert_eq!(
            raw.as_slice(),
            br#"{"ok":false,"error_code":"INTERNAL","max_carriers":7}"#,
            "回退字节必须稳定(客户端按帧解析)"
        );
    }
}
