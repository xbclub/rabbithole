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

/// 拒绝载体时,**关闭整条载体前**给发送链路的有界冲刷窗口(round-53)。
///
/// ## 为什么需要(以及为什么不再是 yield)
///
/// 修前这里是 `write_frame(...).await` → `stream.shutdown().await` →
/// `tokio::task::yield_now().await` → `carrier.shutdown()`,注释写作
/// "给 driver 一次调度机会把 ACK 编帧并写穿(实测 1 次 yield 足够)"。
///
/// `write_frame` 只把字节写进 **mux 流的用户态缓冲**;真正落到 socket 要
/// 经过 `stream command 通道 → SendWorker → send_out → FlushTask →
/// poll_write`——**三级跨任务跳转**。`yield_now` 只让出一次执行权,既不
/// 保证这三个任务被调度,更不保证 poll_write 成功。
///
/// 实测(载体写被人为延迟 1ms × N 次,见 `slow_carrier_write_*` 回归):
/// `N <= 1` 时 ACK 能送达;`N >= 3` 时**必然丢失**,客户端读到
/// `UnexpectedEof` 而非 `ok:false` ACK——它无法区分"被拒"与"连接断了"。
///
/// 因此把"猜"换成"有界等待":交给
/// [`YamuxMultiplexer::shutdown_after_flush`](rh_transport::mux::YamuxMultiplexer::shutdown_after_flush),
/// 它置 dead(不再开新流,存量帧仍排空)→ 在该窗口内多次让出 → 再 abort。
///
/// 取值 100ms:远大于"几十字节帧走完三级任务跳转"的正常耗时(跨机实测
/// 正常路径 <1ms),又远小于客户端等 ACK 的 2s 超时,不会把拒绝路径拖慢。
const REJECT_FLUSH_GRACE: Duration = Duration::from_millis(100);

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
    // 本监听器的叶子证书:与 `rh_transport::listen` 用的是同一份 TLS 选项
    // (同源:显式 cert 或 persist_prefix 落盘的 .crt),故与握手实际出示的
    // 证书逐字节相同。
    //
    // **必须按监听器各自解析**。多监听器(`[[extra_listeners]]`)各有独立的
    // 自签证书;曾经这里是 AppState 上的全局单值、只由首个监听器注入,
    // 客户端连第 2 个及以后的加密监听器时会收到别人的证书,比对必然失败,
    // 而且报错文案是"可能存在中间人"——把安全问题排查引向错误方向。
    let leaf_cert: Option<Arc<[u8]>> = rh_transport::tls::leaf_der_of(&cfg.tls).map(Arc::from);
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
        let leaf = leaf_cert.clone();
        tokio::spawn(async move {
            let remote = inc.remote;
            if let Err(e) = handle_incoming(st, inc, leaf).await {
                tracing::debug!(error = %e, %remote, "incoming closed");
            }
        });
    }
}

/// `leaf_cert`:本监听器的叶子证书 DER(见 [`serve_transport`])。随连接一路
/// 传到 [`send_server_cert`],保证"客户端连哪个端口就下发哪个证书"。
async fn handle_incoming(
    state: Arc<AppState>,
    inc: Incoming,
    leaf_cert: Option<Arc<[u8]>>,
) -> anyhow::Result<()> {
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
            let r = auth_and_run(state.clone(), inc, stream, hello, buf, leaf_cert).await;
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
    leaf_cert: Option<Arc<[u8]>>,
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
    // 证书下发:仅加密协议。客户端解密后与 TLS 对端证书比对,不一致即断开。
    // 失败只 warn(不影响会话):客户端在缺失该帧时退回改动前的行为。
    if inc.protocol.is_encrypted() {
        if let Err(e) = send_server_cert(
            &mut stream,
            &latest.id,
            &latest.secret,
            leaf_cert.as_deref(),
        )
        .await
        {
            warn!(client = %latest.id, error = %e, "下发服务端证书失败");
        }
    }
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
    // 上限强制(round-10.1 §2.3 / round-48 **原子化**):cap 含首载体,池内
    // 只装附加载体,故允许本次附加的条件是
    // 「池内成员数 + 1(首载体) + 1(本次) <= cap」,即**池内成员数 < cap**。
    // round-10.2 §2.3:计数用**成员表长度**(含 dead-but-unreaped)而非
    // 存活数——已死未 reap 的成员仍占用物理连接与池内名额;按存活数计会
    // 在健康监视 reap 前的窗口内多放行 1 条。
    //
    // **本处不再做锁外预检(round-48)**:修前是「读成员数(一次锁)→ 判断
    // → 聚合(另一次锁)」三步分属不同临界区,两个附加连接并发报到时可能
    // 都通过检查并各自聚合,让 K 短暂达到 cap+1(旧注释自认"检查非原子")。
    // 现在裁决点下沉到
    // [`attach_yamux_carrier_with_cap`](rh_transport::Multiplexer::attach_yamux_carrier_with_cap)
    // 的 `members` 写锁内 —— 判断与 push 在同一临界区,窗口为零。
    // 超限由聚合调用返回 `RejectedFull`,再走本文件末尾的拒绝路径。
    // 拒绝路径不聚合(红线:必须走 ACK 而非 send_error)。
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
    // 聚合进会话池:首个附加载体建池,后续成员走
    // `attach_yamux_carrier_with_cap`(成员数判断与 push 同处池的 members
    // 写锁,round-48 原子化)。锁内无 await 之外的旁路;Session 移除时池随
    // Session drop,附加载体连接随之关闭(§3.7)。
    //
    // 返回 `RejectedFull` ⟹ 超限:该载体**未入表**,走拒绝路径。
    // 聚合进会话池:首个附加载体建池,后续成员走
    // `attach_yamux_carrier_with_cap`(成员数判断与 push 同处池的 members
    // 写锁,round-48 原子化)。锁内无 await 之外的旁路;Session 移除时池随
    // Session drop,附加载体连接随之关闭(§3.7)。
    //
    // 超限时把**尚未入表的载体**交回(`rejected`),由下方统一拒绝路径
    // 在 ACK 写出之后 shutdown —— 顺序反了客户端会收不到拒绝原因。
    //
    // 注:池变体拒绝时不支持聚合(`attach_yamux_carrier_with_cap` 返回
    // None)走 NOT_POOLED 就地拒绝;`RejectedFull` 时载体已被该函数丢弃,
    // 拿不回来,拒绝路径靠关报到流让客户端感知。
    let rejected: Option<rh_transport::mux::YamuxMultiplexer> = {
        let mut pool = sess.mux_pool.lock().await;
        match pool.take() {
            None => {
                // cap 含首载体:建池要占「首载体 + 本次」两个名额。
                if cap < 2 {
                    Some(yamux)
                } else {
                    *pool = Some(rh_transport::Multiplexer::yamux_pool_server(vec![yamux]));
                    // None→Some 升级必须补通知:数据接受循环此刻可能 park 在
                    // 单源分支(仅首载体),池自身的 changed 只唤醒其订阅者,
                    // 不通知则池侧合流通道的新流无人接收(§3.6 唤醒缝隙)
                    sess.mux_pool_changed.notify_waiters();
                    None
                }
            }
            Some(existing) => {
                // round-53:用**交还载体**的变体。它把"超限"的载体原样
                // 交回(`Err`),而不是像 `attach_yamux_carrier_with_cap`
                // 那样按值丢弃——后者因 `YamuxMultiplexer` 无 impl Drop
                // 等于泄漏一条无人驱动的连接(实测 probe 流 3s 读不到 EOF)。
                //
                // 交回后统一走下方 `rejected` 路径:先回 ACK,等它写穿,
                // 再 `shutdown_after_flush` 关整条载体。顺序反了客户端会
                // 收到 UnexpectedEof 而非 ok:false。
                let outcome = existing
                    .attach_yamux_carrier_with_cap_gated(yamux, cap)
                    .await;
                *pool = Some(existing);
                match outcome {
                    // 池变体不支持聚合(理论上不可达):就地拒绝。
                    // 载体 `mux` 已被 gated 变体交还,但本路径无需再关它
                    // ——`existing` 是池变体,说明承载报到流的连接不属于
                    // 该池,拒绝后由会话拆除收回。
                    None => {
                        drop(pool);
                        reject_carrier(
                            &mut stream,
                            cap,
                            "NOT_POOLED",
                            &hello.client_id,
                            session_id,
                        )
                        .await;
                        return Ok(());
                    }
                    // 超限:载体被交回,交给下方 `rejected` 统一处理
                    Some(Err(rejected_carrier)) => Some(rejected_carrier),
                    Some(Ok(_)) => None,
                }
            }
        }
    };
    if let Some(y) = rejected {
        // 超限且载体仍在手中:先回 ACK,再关整条载体。
        //
        // round-43 红线(不得退回"无人驱动的连接"):关的是**载体 mux**
        // (下面的 `shutdown_after_flush`),不是只关报到流——`YamuxMultiplexer`
        // 没有 impl Drop,drop 只 detach driver 的 JoinHandle,driver 会继续
        // 持有 socket。
        //
        // round-53:末帧送达不再靠 `yield_now` 猜(实测 3 次写延迟即必丢,
        // 见 [`REJECT_FLUSH_GRACE`] 的推导),改用有界冲刷关停。
        reject_carrier(&mut stream, cap, "POOL_FULL", &hello.client_id, session_id).await;
        info!(
            client_id = %hello.client_id,
            session_id,
            cap,
            "pool carrier rejected (cap, atomic)"
        );
        let _ = stream.shutdown().await;
        y.shutdown_after_flush(REJECT_FLUSH_GRACE).await;
        return Ok(());
    }
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
    // 池内当前成员数(诊断口径,含 dead-but-unreaped——与 cap 检查同口径)。
    // 成功路径到此必然已入表,故至少为 1。
    let pool_size = {
        let g = sess.mux_pool.lock().await;
        match g.as_ref() {
            Some(p) => p.pool_member_count().await.unwrap_or(1),
            None => 1,
        }
    };
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

/// 认证成功后下发服务端 TLS 证书(见 [`rh_protocol::certprov`])。
///
/// 客户端解密后与 TLS 握手实际看到的对端证书比对 —— 不一致说明有人在
/// 中间转发,客户端会断开。密钥由 `secret` 经 HKDF 派生,攻击者不知道
/// `secret` 就既解不开也伪造不了。
///
/// 证书缺失(纯明文控制面 / 尚未注入)时静默跳过:此帧是附加证明,
/// 不是认证前提,缺了不应导致连接失败。
async fn send_server_cert(
    stream: &mut BoxedIo,
    client_id: &str,
    secret: &str,
    leaf: Option<&[u8]>,
) -> anyhow::Result<()> {
    let Some(leaf) = leaf else {
        return Ok(());
    };
    let nonce = rh_protocol::certprov::random_nonce()
        .ok_or_else(|| anyhow::anyhow!("CSPRNG 不可用,跳过证书下发"))?;
    let sealed = rh_protocol::certprov::seal_cert(secret.as_bytes(), nonce, leaf)
        .ok_or_else(|| anyhow::anyhow!("证书加密失败"))?;
    // 与 AUTH_CHALLENGE 等一致:帧体内直接放内层结构,不套 ServerMessage 信封
    let msg = rh_protocol::message::ServerCert {
        nonce_b64: b64::encode(&sealed.nonce),
        cert_der_b64: b64::encode(&sealed.ciphertext),
    };
    write_frame(stream, FrameType::SERVER_CERT, &serde_json::to_vec(&msg)?).await?;
    tracing::debug!(client = %client_id, "已下发服务端证书(加密)");
    Ok(())
}

/// base64(标准、无填充容忍差异;仅编码用)。
mod b64 {
    use base64::Engine as _;
    pub fn encode(b: &[u8]) -> String {
        base64::engine::general_purpose::STANDARD.encode(b)
    }
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
    // round-80:gauge 已由 `remove_if_current` 内部上报(见 SessionRegistry
    // 的 `on_change`),此处不再重复写——重复写不仅冗余,在"新会话已替换
    // 旧会话"的时序下还会掩盖真实计数。
    state.sessions.remove_if_current(&sess.client_id, &sess);
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
        // round-48:本处与报到路径同口径,**裁决点下沉到池的 members 写锁**
        // (由 `attach_yamux_carrier_with_cap` 完成),不再有「读成员数 →
        // 判断 → 聚合」三步分属不同临界区的非原子窗口(旧注释自认
        // "检查非原子窗口依然存在")。
        // round-10.2 §2.3:计数按成员表长度(含 dead 未 reap 的占位成员),
        // 而非存活数——该口径在 `ms.len()` 里天然成立。
        //
        // 与报到路径的差异(有意):本路径超限时**不能 shutdown 该载体**
        // ——那会连带杀掉马上要 `route_data_stream` 的这条数据流。因此
        // 拒绝时把载体保活到路由结束(`rejected_mux`),随作用域释放。
        let mut pool = sess.mux_pool.lock().await;
        match pool.take() {
            None => {
                // cap 含首载体:建池要占「首载体 + 本次」两个名额
                if cap < 2 {
                    rejected_mux = Some(yamux);
                    info!(
                        client_id = %sess.client_id,
                        session_id = %sess.session_id,
                        cap,
                        "pool carrier rejected (cap, data-channel refill, atomic)"
                    );
                } else {
                    *pool = Some(rh_transport::Multiplexer::yamux_pool_server(vec![yamux]));
                    // 同 attach_additional_carrier:升级补通知,唤醒可能仍
                    // park 在单源分支的数据接受循环
                    sess.mux_pool_changed.notify_waiters();
                }
            }
            Some(existing) => {
                let outcome = existing.attach_yamux_carrier_with_cap(yamux, cap).await;
                *pool = Some(existing);
                match outcome {
                    Some(rh_transport::mux::AttachOutcome::RejectedFull) => {
                        // 载体已被 attach 内部丢弃,拿不回来。本路径**不需要**
                        // 保活它:数据流由 `stream` 承载,而 `stream` 已从该
                        // 载体 accept 出来,路由不依赖载体 mux 的持有。
                        info!(
                            client_id = %sess.client_id,
                            session_id = %sess.session_id,
                            cap,
                            "pool carrier rejected (cap, data-channel refill, atomic)"
                        );
                    }
                    None => {
                        warn!(
                            client_id = %sess.client_id,
                            "session mux pool is not a yamux pool (refill)"
                        );
                    }
                    Some(_) => {}
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

    // round-43 回归:超限拒绝后**必须关闭整条载体连接**。
    //
    // ## 缺陷
    //
    // 修前 `reject_carrier` 写完 `ok:false` ACK 就 `return Ok(())`。`stream`
    // 只是载体上的第一条流(承载 HELLO/ACK),而**真正持有物理连接的是
    // `inc.mux`**。`YamuxMultiplexer` 没有 `impl Drop`——drop 它只是 detach
    // driver 的 JoinHandle,driver 继续持有 socket 并运行
    // (mux.rs:1147-1150:为「正常路径零漂移」刻意保留)。
    //
    // 于是被拒载体的 driver 永远活着,客户端在其上开的新流无人接收 ——
    // 表现为偶发流挂起(REPORT-round10.md「## 6. 遗留」第 2 条已承认)。
    //
    // ## 判据
    //
    // 拒绝后客户端在**同一载体**上的另一条流必须能读到 EOF/错误(载体已关)。
    // 修复前该读会一直挂到超时。
    #[tokio::test]
    async fn attach_over_cap_closes_carrier_after_ack() {
        use tokio::io::AsyncReadExt as _;

        let state = state_with(1).await;
        let _sess = register_session(&state, "sid-close");

        // 手搭载体:把 server mux 装进 Incoming 交给被测函数,
        // 另留一条 probe 流观察载体是否被关。
        let (client_raw, server_raw) = tokio::io::duplex(256 * 1024);
        let client = rh_transport::wrap_stream_as_client(Box::new(client_raw), true, 64);
        let server = rh_transport::wrap_stream_as_server(Box::new(server_raw), true, 64);

        // 报到流(承载 HELLO + ACK)
        let mut report_client = client.open_stream().await.expect("report stream");
        let report_server = tokio::time::timeout(Duration::from_secs(2), server.accept())
            .await
            .expect("accept timeout")
            .expect("server report stream");

        // probe 流:用于观察载体是否被关闭
        let mut probe_client = client.open_stream().await.expect("probe stream");
        let _probe_server = tokio::time::timeout(Duration::from_secs(2), server.accept())
            .await
            .expect("accept timeout")
            .expect("server probe stream");

        // 客户端 mux 必须存活(driver 需要它)
        tokio::spawn(async move {
            let _keep = client;
            futures::future::pending::<()>().await;
        });

        let inc = Incoming {
            mux: server,
            remote: "127.0.0.1:1".parse().unwrap(),
            protocol: Protocol::Tcp,
        };

        // 走生产路径:cap=1、池空 → 0+2 > 1 → 拒绝
        attach_additional_carrier(
            state.clone(),
            inc,
            report_server,
            attach_hello("sid-close"),
            "sid-close",
        )
        .await
        .unwrap();

        // 1) ACK 必须送达(关闭在写完之后)
        let ack = read_ack(&mut report_client).await;
        assert!(!ack.ok, "超限必须拒绝");
        assert_eq!(ack.error_code.as_deref(), Some("POOL_FULL"));
        assert_eq!(ack.max_carriers, 1, "ACK 内容不得改动");

        // 2) 载体必须被关闭:同一载体上的 probe 流读到 EOF/错误。
        //    修复前 driver 仍活着,该读会一直挂到超时。
        let mut buf = [0u8; 64];
        let closed = tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                match probe_client.read(&mut buf).await {
                    Ok(0) => break true,
                    Ok(_) => continue,
                    Err(_) => break true,
                }
            }
        })
        .await;
        assert!(
            closed.is_ok(),
            "超限拒绝后整条载体必须被关闭(同载体的 probe 流应读到 EOF);\
             若超时,说明 driver 仍存活 —— round-43 的缺陷"
        );
        assert!(closed.unwrap(), "读端必须观察到 EOF 或错误");
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

    // round-53 回归:RejectedFull 分支必须**关掉整条载体**。
    //
    // 修前 `attach_carrier_with_cap` 在拒绝分支按值 drop 载体;
    // `YamuxMultiplexer` 无 impl Drop,drop 只 detach driver 不关 socket
    // ⟹ 泄漏一条无人驱动的连接(round-43 声称修好的缺陷在另一条分支的残留)。
    #[tokio::test]
    async fn rejected_full_closes_carrier() {
        use tokio::io::AsyncReadExt as _;
        let state = state_with(2).await;
        let _sess = register_session(&state, "sid-rf");

        // 第一次报到:cap=2、池空 → 建池(1 成员),ok:true
        let (mut c1, inc1, r1) = carrier_pair().await;
        attach_additional_carrier(state.clone(), inc1, r1, attach_hello("sid-rf"), "sid-rf")
            .await
            .unwrap();
        let a1 = read_ack(&mut c1).await;
        assert!(a1.ok, "首次报到应成功(CAP=2 允许 1 条附加载体)");

        // 第二次报到:池内 1 成员、cap=2 → 1+2 > 2 → RejectedFull。
        // 在**同一条载体**上开 probe 流,观察它是否被关。
        let (mut c2, inc2, r2) = carrier_pair().await;
        let mut probe = inc2.mux.open_stream().await.expect("probe stream");
        attach_additional_carrier(state.clone(), inc2, r2, attach_hello("sid-rf"), "sid-rf")
            .await
            .unwrap();
        let a2 = read_ack(&mut c2).await;
        assert!(!a2.ok, "超限必须拒绝");
        assert_eq!(a2.error_code.as_deref(), Some("POOL_FULL"));
        assert_eq!(a2.max_carriers, 2, "ACK 内容不得改动");

        let mut buf = [0u8; 64];
        let closed = tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                match probe.read(&mut buf).await {
                    Ok(0) => break true,
                    Ok(_) => continue,
                    Err(_) => break true,
                }
            }
        })
        .await;
        assert!(
            closed.is_ok(),
            "RejectedFull 后整条载体必须被关闭(同载体 probe 流应读到 EOF);\
             修前 driver 仍活着,该读会挂到超时"
        );
    }

    /// 写半可被"卡住 N 次"的载体:前 `delays` 次 poll_write 返回 Pending,
    /// 后台 1ms 后唤醒。模拟 driver 恰好在等其它资源 / 内核发送缓冲满——
    /// 即 `yield_now` 一次不足以让末帧写穿的场景。
    struct SlowWriteIo {
        inner: tokio::io::DuplexStream,
        delays: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl tokio::io::AsyncRead for SlowWriteIo {
        fn poll_read(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
            buf: &mut tokio::io::ReadBuf<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::pin::Pin::new(&mut self.inner).poll_read(cx, buf)
        }
    }

    impl tokio::io::AsyncWrite for SlowWriteIo {
        fn poll_write(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
            buf: &[u8],
        ) -> std::task::Poll<std::io::Result<usize>> {
            use std::sync::atomic::Ordering as O;
            if self.delays.load(O::SeqCst) > 0 {
                self.delays.fetch_sub(1, O::SeqCst);
                let w = cx.waker().clone();
                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                    w.wake();
                });
                return std::task::Poll::Pending;
            }
            std::pin::Pin::new(&mut self.inner).poll_write(cx, buf)
        }
        fn poll_flush(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::pin::Pin::new(&mut self.inner).poll_flush(cx)
        }
        fn poll_shutdown(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::pin::Pin::new(&mut self.inner).poll_shutdown(cx)
        }
    }

    // round-53(红):载体写被人为延迟时,拒绝 ACK 仍必须送达客户端。
    //
    // 修前该路径是 `write_frame` → `stream.shutdown` → **一次** `yield_now`
    // → `carrier.shutdown()`。yield 只让出一次执行权,而 ACK 要走
    // `stream command → SendWorker → FlushTask → poll_write` 三级跨任务
    // 跳转;driver 若在等其它资源,一次让出不足以让它推到 poll_write 成功。
    //
    // 实测(本测试):delay=0/1 时旧实现能过;delay>=3 时**必然**丢 ACK,
    // 客户端读到 UnexpectedEof 而非 ok:false——无法区分"被拒"与"断连"。
    #[tokio::test]
    async fn slow_carrier_write_still_delivers_reject_ack() {
        for delays in [0usize, 3, 10] {
            let state = state_with(1).await;
            let _sess = register_session(&state, "sid-slow");

            let (client_raw, server_raw) = tokio::io::duplex(256 * 1024);
            let client = rh_transport::wrap_stream_as_client(Box::new(client_raw), true, 64);
            let server = rh_transport::wrap_stream_as_server(
                Box::new(SlowWriteIo {
                    inner: server_raw,
                    delays: Arc::new(std::sync::atomic::AtomicUsize::new(delays)),
                }),
                true,
                64,
            );
            let mut report_client = client.open_stream().await.expect("report stream");
            let report_server = tokio::time::timeout(Duration::from_secs(2), server.accept())
                .await
                .expect("accept timeout")
                .expect("server report stream");
            tokio::spawn(async move {
                let _keep = client;
                futures::future::pending::<()>().await;
            });
            let inc = Incoming {
                mux: server,
                remote: "127.0.0.1:1".parse().unwrap(),
                protocol: Protocol::Tcp,
            };
            attach_additional_carrier(
                state.clone(),
                inc,
                report_server,
                attach_hello("sid-slow"),
                "sid-slow",
            )
            .await
            .unwrap();

            // cap=1 ⟹ 首次附加即被拒(0+2 > 1),走 `rejected` 分支。
            let ack = read_ack(&mut report_client).await;
            assert!(!ack.ok, "delays={delays}:必须收到 ok:false");
            assert_eq!(
                ack.error_code.as_deref(),
                Some("POOL_FULL"),
                "delays={delays}:ACK 内容不得改动"
            );
            assert_eq!(ack.max_carriers, 1, "delays={delays}:cap 回写不得改动");
        }
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

#[cfg(test)]
mod carrier_attach_concurrency_tests {
    //! round-48:并发报到下的 cap 原子性回归。
    //!
    //! 修前「读成员数(一次锁)→ 判断 → 聚合(另一次锁)」三步分属不同
    //! 临界区,N 个附加连接并发报到时可能都通过检查并各自聚合,让池内
    //! 成员数短暂达到 cap+1。修后裁决点下沉到池的 `members` 写锁内
    //! (`attach_yamux_carrier_with_cap`),判断与 push 同处一个临界区。
    use super::*;
    use crate::config::ServerConfig;
    use rh_transport::Protocol;

    const CIDC: &str = "concurrent-cap-client";

    async fn state_with_cap(cap: usize) -> Arc<AppState> {
        let db = crate::db::Db::open("sqlite::memory:").await.unwrap();
        db.create_client(CIDC, "cc", &"0".repeat(64), "")
            .await
            .unwrap();
        let cfg = ServerConfig {
            max_carriers: cap,
            ..ServerConfig::default()
        };
        Arc::new(AppState::new(cfg, db))
    }

    fn register(state: &Arc<AppState>, sid: &str) -> Arc<Session> {
        let (tx, _rx) = mpsc::channel(8);
        let sess = Session::new(CIDC.into(), sid.into(), 1, CancellationToken::new(), tx);
        state.sessions.register(sess.clone());
        sess
    }

    fn hello_for(sid: &str) -> Hello {
        Hello {
            client_id: CIDC.into(),
            version: PROTOCOL_VERSION.into(),
            hostname: "h".into(),
            os: "test".into(),
            arch: "test".into(),
            capabilities: Vec::new(),
            session_id: Some(sid.into()),
        }
    }

    /// 构造一对连通载体,返回 (客户端报到流, 服务端 Incoming, 服务端报到流)。
    async fn pair() -> (BoxedIo, Incoming, BoxedIo) {
        let (a, b) = tokio::io::duplex(256 * 1024);
        let client = rh_transport::wrap_stream_as_client(Box::new(a), true, 64);
        let server = rh_transport::wrap_stream_as_server(Box::new(b), true, 64);
        let cs = client.open_stream().await.expect("client report stream");
        let ss = tokio::time::timeout(Duration::from_secs(2), server.accept())
            .await
            .expect("accept timeout")
            .expect("server report stream");
        // 客户端 mux 必须存活(否则物理连接关闭,服务端写 ACK 失败)
        tokio::spawn(async move {
            let _keep = client;
            futures::future::pending::<()>().await;
        });
        let inc = Incoming {
            mux: server,
            remote: "127.0.0.1:1".parse().unwrap(),
            protocol: Protocol::Tcp,
        };
        (cs, inc, ss)
    }

    /// **核心回归**:cap=3 时并发发起 6 条附加报到,最终池内成员数必须
    /// <= cap-1(池内成员 = 附加载体;总载体 = 成员 + 首载体 <= cap)。
    ///
    /// 修前:多个报到可能同时通过锁外检查并各自聚合 → 成员数可达 5
    /// (总载体 6 > cap=3)。
    /// 修后:裁决在 members 写锁内,成员数不可能超过 cap-1 = 2。
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_attach_never_exceeds_cap() {
        const CAP: usize = 3;
        // 池内成员上限 = cap - 1(首载体占一名)
        const MAX_MEMBERS: usize = CAP - 1;

        let state = state_with_cap(CAP).await;
        let sess = register(&state, "sid-conc");

        // 并发发起 6 条附加报到
        const N: usize = 6;
        let mut handles = Vec::with_capacity(N);
        for _ in 0..N {
            let st = state.clone();
            let (mut cs, inc, ss) = pair().await;
            handles.push(tokio::spawn(async move {
                let r =
                    attach_additional_carrier(st, inc, ss, hello_for("sid-conc"), "sid-conc").await;
                // 读 ACK,判定该次报到是成功还是被拒
                let mut buf = bytes::BytesMut::with_capacity(512);
                let mut accepted = false;
                if let Ok(Ok((ty, payload))) = tokio::time::timeout(
                    Duration::from_secs(2),
                    rh_protocol::read_frame(&mut cs, &mut buf),
                )
                .await
                {
                    if ty == FrameType::CARRIER_ATTACH_ACK {
                        if let Ok(a) = serde_json::from_slice::<
                            rh_protocol::message::CarrierAttachAck,
                        >(&payload)
                        {
                            accepted = a.ok;
                        }
                    }
                }
                (r, accepted)
            }));
        }
        let mut accepted = 0usize;
        for h in handles {
            if let Ok((_, ok)) = h.await {
                if ok {
                    accepted += 1;
                }
            }
        }
        assert!(
            accepted <= MAX_MEMBERS,
            "并发报到有 {accepted} 条收到 ok:true,超过上限 {MAX_MEMBERS}             (cap={CAP},含首载体)—— round-48 的原子性裁决失效"
        );

        // 裁决:池内成员数不得超过 cap-1
        let members = {
            let g = sess.mux_pool.lock().await;
            match g.as_ref() {
                Some(p) => p.pool_member_count().await.unwrap_or(0),
                None => 0,
            }
        };
        assert!(
            members <= MAX_MEMBERS,
            "并发报到后池内成员数 {members} 超过上限 {MAX_MEMBERS}(cap={CAP},\
             含首载体)—— round-48 的原子性裁决失效"
        );
        // 且至少聚合了一条(cap=3 时至少允许 2 条)
        assert!(members >= 1, "cap={CAP} 时至少应聚合 1 条附加载体");
    }

    /// cap=1 时任何附加载体都不得聚合(连建池都不允许)。
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_attach_with_cap_one_aggregates_nothing() {
        let state = state_with_cap(1).await;
        let sess = register(&state, "sid-one");

        let mut handles = Vec::new();
        for _ in 0..4 {
            let st = state.clone();
            let (mut cs, inc, ss) = pair().await;
            handles.push(tokio::spawn(async move {
                let _ =
                    attach_additional_carrier(st, inc, ss, hello_for("sid-one"), "sid-one").await;
                let mut buf = bytes::BytesMut::with_capacity(512);
                let _ = tokio::time::timeout(
                    Duration::from_secs(2),
                    rh_protocol::read_frame(&mut cs, &mut buf),
                )
                .await;
            }));
        }
        for h in handles {
            let _ = h.await;
        }

        assert!(
            sess.mux_pool.lock().await.is_none(),
            "cap=1 时不得建池、不得聚合任何附加载体"
        );
    }
}

#[cfg(test)]
mod cert_per_listener_tests {
    //! 证书下发必须"连哪个端口就下发哪个证书"。
    //!
    //! 多监听器(`[[extra_listeners]]`)各有独立的自签证书。曾经
    //! `AppState.control_leaf_cert` 是全局单值、只由首个监听器注入,于是
    //! 客户端连第 2 个及以后的加密监听器时会收到别人的证书,逐字节比对
    //! 必然失败,还报"可能存在中间人"——把安全问题排查引向错误方向。
    //!
    //! 本模块锁住两点:
    //! 1. [`rh_transport::tls::leaf_der_of`] 对两份不同证书解析出不同结果;
    //! 2. [`send_server_cert`] 使用的就是**传入的那份**,而非任何全局状态。
    use super::*;
    use base64::Engine as _;

    /// 造一份独立的 PEM 自签证书,返回 (leaf DER, PEM)。
    fn make_cert() -> (Vec<u8>, Vec<u8>) {
        let (pem, _key) = rh_transport::tls::auto_cert(&Default::default()).unwrap();
        let der = rh_transport::tls::leaf_der_pem(&pem).unwrap();
        (der, pem)
    }

    /// 两份独立证书必须解析出两份不同的 DER —— 这是"多监听器证书不同"
    /// 这个前提本身,也是本缺陷成立的根因。
    #[test]
    fn two_listeners_yield_different_leaf_certs() {
        let (der_a, pem_a) = make_cert();
        let (der_b, pem_b) = make_cert();
        assert_ne!(der_a, der_b, "两次生成的自签证书必须不同");

        // leaf_der_of 走"显式 cert_pem"分支,与应用 acceptor 同源
        let opts_a = rh_transport::TlsServerOptions {
            cert_pem: Some(pem_a),
            ..Default::default()
        };
        let opts_b = rh_transport::TlsServerOptions {
            cert_pem: Some(pem_b),
            ..Default::default()
        };
        assert_eq!(
            rh_transport::tls::leaf_der_of(&opts_a).as_deref(),
            Some(der_a.as_slice()),
            "监听器 A 必须解析出自己的证书"
        );
        assert_eq!(
            rh_transport::tls::leaf_der_of(&opts_b).as_deref(),
            Some(der_b.as_slice()),
            "监听器 B 必须解析出自己的证书"
        );
    }

    /// `leaf_der_of` 也必须能走 persist_prefix 分支(生产路径:证书落盘在
    /// `<配置目录>/certs/rh-<proto>-<port>.crt`,启动时从磁盘读)。
    #[test]
    fn leaf_der_of_reads_persisted_cert() {
        let dir = std::env::temp_dir().join(format!(
            "rh-leaf-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let prefix = dir.join("rh-tls-1234");
        let opts = rh_transport::TlsServerOptions {
            persist_prefix: Some(prefix.clone()),
            ..Default::default()
        };
        let (pem, _key) = rh_transport::tls::auto_cert(&opts).unwrap();
        let expect = rh_transport::tls::leaf_der_pem(&pem).unwrap();
        // 此时 cert_pem 仍是 None,只能从落盘读
        let got = rh_transport::tls::leaf_der_of(&opts).expect("必须能从 .crt 读出");
        assert_eq!(got, expect, "落盘分支必须与 auto_cert 的证书一致");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// **核心回归**:`send_server_cert` 下发的必须是"传进来的那份"证书。
    ///
    /// 修前它读 `state.control_leaf_cert` 全局单值,与连接实际来自哪个
    /// 监听器无关;修后由 `serve_transport` 按 `cfg.tls` 解析并逐层传参。
    /// 这里用两份不同证书分别调用,断言解出来的就是各自那份。
    #[tokio::test]
    async fn send_server_cert_uses_the_passed_leaf() {
        let secret = "0123456789abcdef0123456789abcdef";
        let (der_a, _pem_a) = make_cert();
        let (der_b, _pem_b) = make_cert();
        assert_ne!(der_a, der_b);

        for (label, leaf) in [("A", &der_a), ("B", &der_b)] {
            let (server, mut client) = tokio::io::duplex(64 * 1024);
            let mut server: BoxedIo = Box::new(server);
            let leaf_for_task = leaf.clone();
            let task = tokio::spawn(async move {
                send_server_cert(&mut server, "cid", secret, Some(&leaf_for_task))
                    .await
                    .unwrap();
            });

            let mut buf = bytes::BytesMut::with_capacity(4096);
            let (ty, payload) =
                tokio::time::timeout(Duration::from_secs(2), read_frame(&mut client, &mut buf))
                    .await
                    .expect("读帧超时")
                    .expect("读帧失败");
            task.await.unwrap();

            assert_eq!(ty, FrameType::SERVER_CERT, "{label}: 帧类型");
            let msg: rh_protocol::message::ServerCert = serde_json::from_slice(&payload).unwrap();
            let nonce: [u8; 12] = base64::engine::general_purpose::STANDARD
                .decode(msg.nonce_b64.as_bytes())
                .unwrap()
                .as_slice()
                .try_into()
                .unwrap();
            let ct = base64::engine::general_purpose::STANDARD
                .decode(msg.cert_der_b64.as_bytes())
                .unwrap();
            let sealed = rh_protocol::certprov::SealedCert {
                nonce,
                ciphertext: ct,
            };
            let opened = rh_protocol::certprov::open_cert(secret.as_bytes(), &sealed)
                .expect("必须能用 secret 解开");
            assert_eq!(
                opened.as_slice(),
                leaf.as_slice(),
                "{label}: 下发的必须是本监听器的证书,不能是别人的"
            );
        }
    }

    /// 没有证书(纯明文控制面)时静默跳过,不得报错、不得发帧。
    #[tokio::test]
    async fn send_server_cert_skips_without_leaf() {
        let (server, mut client) = tokio::io::duplex(4096);
        let mut server: BoxedIo = Box::new(server);
        send_server_cert(&mut server, "cid", "s", None)
            .await
            .expect("无证书不是错误");
        let mut buf = bytes::BytesMut::with_capacity(512);
        let r = tokio::time::timeout(
            Duration::from_millis(200),
            read_frame(&mut client, &mut buf),
        )
        .await;
        assert!(r.is_err(), "无证书时不得下发任何帧");
    }
}
