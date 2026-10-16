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
    // 数据流接受循环(mux 模式:同物理连接的后续流均为数据通道)
    let mux = inc.mux.clone();
    let data_accept = tokio::spawn({
        let st = state.clone();
        let sess = sess.clone();
        async move {
            while let Ok(s) = mux.accept().await {
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
    _inc: Incoming,
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
    route_data_stream(state, sess, stream, payload, None).await
}
