//! 客户端会话:认证、配置同步应用、控制读写循环、数据通道建立。

use crate::config::ClientConfig;
use rh_protocol::message::{
    ApplyResult, ChannelEntity, ClientMessage, CreateDataChannel, DataChannelHello, Hello,
    ServerMessage, StartForward, StartVisitorRelay,
};
use rh_protocol::{frame, FrameType, ProxyKind, ProxySpec, VisitorSpec};
use rh_transport::BoxedIo;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::sync::{mpsc, Mutex};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

/// 一次已建立会话的全部运行态。
pub struct ClientSession {
    pub cfg: Arc<ClientConfig>,
    pub session_id: String,
    pub heartbeat_s: u32,
    /// 控制写队列。
    pub send_tx: mpsc::Sender<(FrameType, Vec<u8>)>,
    pub cancel: CancellationToken,
    // 实体表
    pub proxies: Mutex<HashMap<u64, ProxySpec>>,
    pub visitors: Mutex<HashMap<u64, VisitorSpec>>,
    /// visitor 监听任务取消器。
    pub visitor_cancels: Mutex<HashMap<u64, CancellationToken>>,
    /// 健康检查任务取消器。
    pub health_cancels: Mutex<HashMap<u64, CancellationToken>>,
    /// stcp/xtcp 等待配对的本地连接:user_conn_id → (连接本体, visitor_id)。
    pub waiting: Mutex<HashMap<u64, (tokio::net::TcpStream, u64)>>,
    /// XTCP 打洞成功后的直连多路复用:visitor_id → mux。
    pub punched: Mutex<HashMap<u64, Arc<rh_transport::Multiplexer>>>,
    /// 打洞事务保留 socket(候选与打洞同源;双端各自入队)。
    pub nat_socks: Mutex<HashMap<String, std::collections::VecDeque<Arc<tokio::net::UdpSocket>>>>,
    /// sudp 通道交付:visitor_id → sender。
    pub sudp_deliver: Mutex<HashMap<u64, mpsc::Sender<BoxedIo>>>,
    pub mux: Arc<rh_transport::Multiplexer>,
    pub conn_counter: AtomicU64,
}

/// 在真实流之前回放一段残留字节(read_frame 缓冲丢失问题)。
pub use rh_transport::PrependedIo;

/// 当前自定义 DNS 服务器(见 dnsquery)。
pub fn dns_server() -> Option<String> {
    crate::dnsquery::server()
}

pub fn frame_type_of_client(m: &ClientMessage) -> FrameType {
    match m {
        ClientMessage::Hello(_) => FrameType::HELLO,
        ClientMessage::AuthResponse(_) => FrameType::AUTH_RESPONSE,
        ClientMessage::Ping(_) => FrameType::PING,
        ClientMessage::SyncRequest {} => FrameType::SYNC_REQUEST,
        ClientMessage::DataChannelHello(_) => FrameType::DATA_CHANNEL_HELLO,
        ClientMessage::ApplyResult(_) => FrameType::APPLY_RESULT,
        ClientMessage::UserConnect(_) => FrameType::USER_CONNECT,
        ClientMessage::HealthReport(_) => FrameType::HEALTH_REPORT,
        ClientMessage::NatHoleReport(_) => FrameType::NAT_HOLE_REPORT,
    }
}

impl ClientSession {
    pub async fn send_msg(&self, m: ClientMessage) -> bool {
        let payload = serde_json::to_vec(&m).unwrap_or_default();
        self.send_tx
            .send((frame_type_of_client(&m), payload))
            .await
            .is_ok()
    }

    pub async fn next_conn_id(&self) -> u64 {
        self.conn_counter.fetch_add(1, Ordering::Relaxed) + 1
    }
}

/// 自动定容(round-10):协议基线 → 硬件上限三段中的前两段。
/// 返回 `(initial_k, hardware_cap)`;期望值来源优先级:显式配置 /
/// `RH_MUX_POOL_SIZE`(已由 config 归并为 `Some(n)`)> 自动基线。
/// 服务端上限在报到后收敛,srv 之外 K 恒在 [1,8]。
fn initial_k_for(cfg: &ClientConfig, protocol: rh_transport::Protocol) -> (usize, usize) {
    initial_k_for_with(cfg, protocol, crate::config::detect_hardware_cap())
}

/// 纯函数(可单测):硬件上限由调用方注入,测试不依赖跑测机器的核数与
/// 网卡队列(≤3 核机器 cpu_cap=1,硬编码 tls K=2 的断言会假红)。
/// 生产入口 [`initial_k_for`] 传入真实探测值。
fn initial_k_for_with(
    cfg: &ClientConfig,
    protocol: rh_transport::Protocol,
    hardware_cap: usize,
) -> (usize, usize) {
    let protocol_baseline = rh_transport::default_carriers(protocol);
    let desired = cfg.mux_pool_size.unwrap_or(protocol_baseline);
    (desired.min(hardware_cap).clamp(1, 8), hardware_cap)
}

/// 第三段收敛的纯函数(可单测):由成功报到载体的 `max_carriers` 计算
/// 最终 K;第二个返回值是"报到顺序尾部"的成员表下标(仅诊断/兼容既有
/// 断言——真正的摘除按身份走 [`plan_detach_set`],下标会随成员表压缩
/// 漂移)。
///
/// - `server_cap`:成功应答的最小上限;一个都没成功(旧服务端不回 ACK /
///   全部超时)时为 None → 按 1 处理(有意的保守行为:宁单不双)。
/// - 第二个返回值是"报到顺序尾部"的成员表下标,仅诊断/兼容既有断言——
///   真正的摘除按身份走 [`plan_detach_set`](保留额按表内成功数计,不按
///   位置取尾部),下标会随成员表压缩漂移。
fn plan_convergence(initial_k: usize, server_caps: &[usize]) -> (usize, Vec<usize>) {
    let server_cap = server_caps.iter().copied().min().unwrap_or(1);
    let final_k = initial_k.min(server_cap).clamp(1, 8);
    // 前缀始终保留至少一条(成员 0 = 控制流所在载体)
    (final_k, (final_k..initial_k).collect())
}

/// 待摘除身份集合(round-10.1,纯函数):失败身份 ∪ 超出保留额的成功载体。
///
/// 保留额 = `final_k` 条载体,其中控制载体(`control_id`,必须存活)占一条,
/// 因此**非失败、非控制**的成员按成员表顺序保留前 `final_k - 1` 个,其余
/// 摘除。不能按 `member_ids.skip(final_k)` 取尾部:前缀内出现失败时
/// (initial_k=3、final_k=2、成员 2 失败而成员 3 成功)该式会把成功的
/// 成员 3 一并摘掉,物理载体只剩控制载体一条,且收敛只减不增无法补回。
/// 成员表顺序可能因 reaper 压缩/重拨补位与报到顺序不同,故按"当前表内
/// 成功数"计数而不用报到下标。`control_id` 同时从失败集合中剔除:成员表
/// 压缩后报到流可能落到控制载体上,该次报到必失败,摘除它等于关闭整条
/// 会话。结果去重并升序;调用方按逆序摘除。
///
/// 调用方传入的应是**存活**成员表(round-10.2 复核修复):dead-but-unreaped
/// 成员不得占用保留额,否则其后的健康成员会被误摘,而收敛只减不增、无法
/// 补回。两次收敛(初次/终态)都按同一口径取
/// [`rh_transport::Multiplexer::live_member_ids`]。
fn plan_detach_set(
    member_ids: &[u64],
    final_k: usize,
    failed_ids: &[u64],
    control_id: u64,
) -> Vec<u64> {
    let mut set: Vec<u64> = failed_ids.to_vec();
    // 控制载体若仍在表内,它已占一个保留名额(front 里的顺序不影响计数)。
    let control_present = member_ids.contains(&control_id);
    let keep_others = final_k.saturating_sub(usize::from(control_present));
    let mut kept = 0usize;
    for id in member_ids {
        if *id == control_id || failed_ids.contains(id) {
            continue;
        }
        if kept < keep_others {
            kept += 1;
        } else {
            set.push(*id);
        }
    }
    set.retain(|id| *id != control_id);
    set.sort_unstable();
    set.dedup();
    set
}

/// 初次收敛(round-10.2 复核修复):按 `final_k` 摘除失败身份与超出保留额
/// 的成功载体。保留额按**存活**成员计数([`rh_transport::Multiplexer::live_member_ids`]),
/// 与终态收敛同口径——dead-but-unreaped 成员不得占保留额,否则其后的健康
/// 成员会在初次收敛就被摘掉,而收敛只减不增,终态无法补回。返回实际成功
/// 摘除数。
async fn initial_convergence(
    mux: &rh_transport::Multiplexer,
    final_k: usize,
    failed_ids: &[u64],
    control_member_id: u64,
) -> usize {
    let to_detach = plan_detach_set(
        &mux.live_member_ids().await,
        final_k,
        failed_ids,
        control_member_id,
    );
    let mut detached = 0usize;
    for id in to_detach.into_iter().rev() {
        if mux.detach_carrier_by_id(id).await {
            detached += 1;
        }
    }
    detached
}

/// 终态收敛(round-10.2):detach 期间可能仍有补位落地(例如收敛前已在途、
/// 目标数检查前后脚到达的重拨)。重读成员表,按同一保留规则再摘除,最多
/// 3 轮;每轮留 50ms 给在途补位落地。返回实际成功摘除数。
///
/// 保留额必须按**存活**成员计数([`rh_transport::Multiplexer::live_member_ids`]):
/// 健康监视的 500ms 窗口内成员表可能仍含 dead-but-unreaped 成员;若按
/// `member_ids` 计数,保留额会被算给已死成员,从而误摘其后的健康成员
/// (终态 live 载体数 < final_k)。
async fn terminal_convergence(
    mux: &rh_transport::Multiplexer,
    final_k: usize,
    control_member_id: u64,
) -> usize {
    let mut detached = 0usize;
    for _ in 0..3 {
        let ids = mux.live_member_ids().await;
        let excess = plan_detach_set(&ids, final_k, &[], control_member_id);
        if excess.is_empty() {
            break;
        }
        for id in excess.iter().rev() {
            if mux.detach_carrier_by_id(*id).await {
                detached += 1;
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    detached
}

/// 报到应答载荷判定(纯函数,便于单测):只有 `ok:true` 才算成功并返回
/// 服务端上限;`ok:false` 保留错误码用于日志;非法 JSON 归为坏报文。
/// 超时/非 ACK 帧/读错误在调用侧记日志并记录该次报到的载体身份,收敛时
/// 按身份摘除(round-10.1 起不再按成员下标摘除)。
#[derive(Debug, PartialEq)]
enum AckPayload {
    Ok(usize),
    Rejected(Option<String>),
    Bad(String),
}

fn parse_attach_ack_payload(p: &[u8]) -> AckPayload {
    match serde_json::from_slice::<rh_protocol::message::CarrierAttachAck>(p) {
        Ok(a) if a.ok => AckPayload::Ok(a.max_carriers.clamp(1, 8)),
        Ok(a) => AckPayload::Rejected(a.error_code),
        Err(e) => AckPayload::Bad(e.to_string()),
    }
}

/// 建立:连接 → 控制流 → HELLO/挑战/应答 → 运行会话循环。
/// Err 返回触发外层重连。
pub async fn connect_and_run(
    cfg: Arc<ClientConfig>,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    let tcfg = cfg.transport_cfg()?;
    // 三段式定容(round-10):显式值/协议基线先过硬件上限,报到后再按
    // 服务端应答收敛。QUIC/KCP 与非 multiplex 不建池(恒单载体)。
    let (sized_k, hardware_cap) = initial_k_for(&cfg, tcfg.protocol);
    let initial_k = if tcfg.protocol.is_tcp_based() && tcfg.multiplex {
        sized_k
    } else {
        1
    };
    // 多载体拨池(round-9 §3.2):carriers>1 时拨 K 条物理连接轮转分派;
    // K=1 走原 connect(红线:K=1 路径零回退,不建池、shard 仍为 4)。
    let mux = if initial_k > 1 {
        rh_transport::connect_pool(&tcfg, initial_k).await?
    } else {
        rh_transport::connect(&tcfg).await?
    };
    // 控制流的落点身份(round-10.1):收敛阶段绝不可摘除该载体——成员表
    // 压缩后轮转集合收缩,报到流可能落到控制载体上(服务端不会把它当报到流,
    // 该次报到必超时),若按失败身份摘除就会关闭整条会话。
    let (mut control, control_member_id) = mux.open_stream_tagged().await?;
    // HELLO
    let hostname = hostname_or_unknown();
    let hello = Hello {
        client_id: cfg.client_id.clone(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        hostname,
        os: std::env::consts::OS.to_string(),
        arch: std::env::consts::ARCH.to_string(),
        capabilities: vec![
            "tcp", "udp", "http", "https", "stcp", "sudp", "xtcp", "tcpmux",
        ]
        .into_iter()
        .map(String::from)
        .collect(),
        // 首条连接:无会话归属,走完整挑战应答认证(附加载体报到见下方)
        session_id: None,
    };
    frame::write_frame(&mut control, FrameType::HELLO, &serde_json::to_vec(&hello)?).await?;
    let mut buf = bytes::BytesMut::with_capacity(1024);
    // 挑战或错误
    let (ty, payload) = tokio::time::timeout(
        Duration::from_secs(10),
        frame::read_frame(&mut control, &mut buf),
    )
    .await
    .map_err(|_| anyhow::anyhow!("hello timeout"))??;
    match ty {
        FrameType::ERROR => {
            let e: ServerMessage = serde_json::from_slice(&payload).unwrap_or(
                ServerMessage::Error(rh_protocol::message::ErrorMessage {
                    code: "UNKNOWN".into(),
                    message: String::from_utf8_lossy(&payload).into_owned(),
                }),
            );
            let (code, msg) = match e {
                ServerMessage::Error(e) => (e.code, e.message),
                _ => ("UNKNOWN".to_string(), "unexpected".to_string()),
            };
            anyhow::bail!("server rejected: {code}: {msg}");
        }
        FrameType::AUTH_CHALLENGE => {
            let ch: rh_protocol::message::AuthChallenge = serde_json::from_slice(&payload)?;
            let auth = hmac_hex(
                cfg.secret.as_bytes(),
                &format!("rh2:{}:{}", cfg.client_id, ch.nonce),
            );
            // 裸帧携带 AuthResponse 本体(非 ClientMessage 信封)
            let resp = rh_protocol::message::AuthResponse {
                client_id: cfg.client_id.clone(),
                auth,
            };
            frame::write_frame(
                &mut control,
                FrameType::AUTH_RESPONSE,
                &serde_json::to_vec(&resp)?,
            )
            .await?;
        }
        other => anyhow::bail!("unexpected first frame {other:?}"),
    }
    // HELLO_ACK(或错误)
    let (ty, payload) = tokio::time::timeout(
        Duration::from_secs(10),
        frame::read_frame(&mut control, &mut buf),
    )
    .await
    .map_err(|_| anyhow::anyhow!("ack timeout"))??;
    if ty == FrameType::ERROR {
        let e: rh_protocol::message::ErrorMessage = serde_json::from_slice(&payload)
            .map_err(|_| anyhow::anyhow!("server error (unparsable)"))?;
        anyhow::bail!("server rejected: {}: {}", e.code, e.message);
    }
    if ty != FrameType::HELLO_ACK {
        anyhow::bail!("expected HELLO_ACK, got {ty:?}");
    }
    let ack: rh_protocol::message::HelloAck = serde_json::from_slice(&payload)?;
    if !ack.ok {
        anyhow::bail!("hello rejected: {:?}", ack.error_code);
    }
    let session_id = ack.session_id.clone().unwrap_or_default();
    info!(server = %cfg.server_addr, session = %session_id, "connected");

    // 附加载体报到(round-9 §3.5 / round-10 §2.3.4):池化多载体时,其余
    // K-1 条物理连接需向服务端声明归属(各自首流写 HELLO{session_id},
    // 服务端据此把连接聚合进会话池),并在同一流上读服务端应答以确定
    // 最终 K。控制流已占用轮转序号 0,此后第 i 次 open_stream 通常落在
    // 第 i 个成员;但报到窗口内若某成员死亡被剔除/重拨,轮转集合收缩,
    // 第 i 次 open 可能落到别的成员——因此**失败下标不能当作成员表下标**
    // (复核 finding 4)。失败按身份记录,收敛阶段据此精确摘除,并额外
    // 保留 final_k 条成功载体(见 [`plan_detach_set`])。
    // 尽力而为:单条失败/超时不阻断主会话。
    let mut server_caps: Vec<usize> = Vec::new();
    // 报到失败/被拒/超时的载体身份(round-10.1):身份在下标漂移(成员表
    // 压缩、重拨补位)下仍稳定,收敛阶段据此精确摘除。open_stream_tagged
    // 本身失败时无法得知落点,不记 id(该载体可能仍健康)。
    let mut failed_ids: Vec<u64> = Vec::new();
    if initial_k > 1 {
        let attach = Hello {
            client_id: cfg.client_id.clone(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            hostname: hostname_or_unknown(),
            os: std::env::consts::OS.to_string(),
            arch: std::env::consts::ARCH.to_string(),
            capabilities: Vec::new(),
            session_id: Some(session_id.clone()),
        };
        let payload = serde_json::to_vec(&attach)?;
        for i in 1..initial_k {
            let (mut s, member_id) = match mux.open_stream_tagged().await {
                Ok(v) => v,
                Err(e) => {
                    warn!(carrier = i, error = %e, "pool carrier attach failed (open stream)");
                    continue;
                }
            };
            if frame::write_frame(&mut s, FrameType::HELLO, &payload)
                .await
                .is_err()
            {
                warn!(
                    carrier = i,
                    member_id, "pool carrier attach failed (hello write)"
                );
                failed_ids.push(member_id);
                let _ = s.shutdown().await;
                continue;
            }
            let mut abuf = bytes::BytesMut::with_capacity(512);
            let ack =
                tokio::time::timeout(Duration::from_secs(2), frame::read_frame(&mut s, &mut abuf))
                    .await;
            match ack {
                Ok(Ok((FrameType::CARRIER_ATTACH_ACK, p))) => match parse_attach_ack_payload(&p) {
                    AckPayload::Ok(cap) => server_caps.push(cap),
                    AckPayload::Rejected(code) => {
                        warn!(
                            carrier = i,
                            member_id,
                            error_code = ?code,
                            "pool carrier attach rejected"
                        );
                        failed_ids.push(member_id);
                    }
                    AckPayload::Bad(e) => {
                        warn!(carrier = i, member_id, error = %e, "bad carrier attach ack");
                        failed_ids.push(member_id);
                    }
                },
                other => {
                    // 旧服务端(不回 ACK)走这里:该载体无法确认已聚合,
                    // 按失败处理并摘除(§2.2);不阻断主会话。
                    warn!(
                        carrier = i,
                        member_id,
                        ?other,
                        "carrier attach ack missing/timeout"
                    );
                    failed_ids.push(member_id);
                }
            }
            // 报到完成即 FIN;物理载体在收敛后按需 detach。
            let _ = s.shutdown().await;
        }
    }
    // 服务端上限 = 成功报到载体的最小值;一个都没成功(旧服务端/全拒)
    // 按 1 处理——有意的保守行为(见兼容说明)。
    let (final_k, _tail) = plan_convergence(initial_k, &server_caps);
    let server_cap = server_caps.iter().copied().min().unwrap_or(1);
    // 每载体 shard 在拨号时按 initial_k 固化(见 connect_stream_pool);
    // 收敛保留控制载体 + (final_k-1) 个成功载体、不重建,故此处记录实际
    // 生效值。从 initial_k>1 收敛到 1 时沿用初始 shard(max(1, 4/initial_k)):
    // 载体 Worker 已 spawn,重建会拆掉控制流所在的载体 0,代价远超收益
    // (README 定容段)。
    let shard_workers = rh_transport::mux::shard_workers_for_k(initial_k);
    if initial_k > 1 {
        // 先收缩分派集合,再摘除多余成员。待摘除集合 = 失败身份 ∪ 超出
        // 保留额的成功载体,见 [`plan_detach_set`]。`set_active_prefix` 同时
        // 把 final_k 登记为重拨补位目标:收敛前已 spawn 的在途重拨即使
        // 晚于本循环落地(拨号受默认 10s connect_timeout 约束),也会在
        // 入表前被 [`redial_loop`] 丢弃。
        mux.set_active_prefix(final_k).await;
        // 初次收敛与终态收敛同口径按**存活**成员计保留额(round-10.2 复核
        // 修复):dead-but-unreaped 成员若占掉 keep_others 名额,其后的健康
        // 成员会在此就被摘掉,而收敛只减不增,终态无法补回。
        let mut detached = initial_convergence(&mux, final_k, &failed_ids, control_member_id).await;
        // 终态收敛(L2/round-10.2):按**存活**成员重读并按同一保留规则再
        // 摘,见 [`terminal_convergence`];dead-but-unreaped 成员不占保留额。
        detached += terminal_convergence(&mux, final_k, control_member_id).await;
        info!(
            initial_k,
            final_k,
            server_cap,
            failed = failed_ids.len(),
            detached,
            "pool carriers converged"
        );
    }
    info!(
        protocol = %tcfg.protocol,
        protocol_baseline = rh_transport::default_carriers(tcfg.protocol),
        hardware_cap,
        server_cap,
        initial_k,
        final_k,
        shard_workers,
        pooled = initial_k > 1,
        "pool sizing"
    );

    let (send_tx, send_rx) = mpsc::channel::<(FrameType, Vec<u8>)>(256);
    let sess = Arc::new(ClientSession {
        cfg: cfg.clone(),
        session_id: session_id.clone(),
        heartbeat_s: ack.heartbeat_interval_s.max(10),
        send_tx,
        cancel: CancellationToken::new(),
        proxies: Mutex::new(HashMap::new()),
        visitors: Mutex::new(HashMap::new()),
        visitor_cancels: Mutex::new(HashMap::new()),
        health_cancels: Mutex::new(HashMap::new()),
        waiting: Mutex::new(HashMap::new()),
        punched: Mutex::new(HashMap::new()),
        nat_socks: Mutex::new(HashMap::new()),
        sudp_deliver: Mutex::new(HashMap::new()),
        mux: mux.clone(),
        conn_counter: AtomicU64::new(0),
    });
    let _ = shutdown.clone();
    run_loop(sess.clone(), control, send_rx, shutdown).await;
    sess.cancel.cancel();
    Ok(())
}

fn hostname_or_unknown() -> String {
    #[cfg(unix)]
    {
        if let Ok(n) = std::fs::read_to_string("/proc/sys/kernel/hostname") {
            return n.trim().to_string();
        }
    }
    std::env::var("HOSTNAME").unwrap_or_else(|_| "unknown".into())
}

pub fn hmac_hex(secret: &[u8], msg: &str) -> String {
    use hmac::Mac;
    let mut mac = hmac::Hmac::<sha2::Sha256>::new_from_slice(secret).expect("hmac key");
    mac.update(msg.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

// ---------------- 主循环 ----------------

async fn run_loop(
    sess: Arc<ClientSession>,
    mut control: BoxedIo,
    mut send_rx: mpsc::Receiver<(FrameType, Vec<u8>)>,
    shutdown: CancellationToken,
) {
    let mut buf = bytes::BytesMut::with_capacity(4096);
    let mut hb = tokio::time::interval(Duration::from_secs(sess.heartbeat_s as u64));
    hb.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let _ = hb.tick().await;
    // 活性看门狗:死链(NAT 老化/黑洞路由)下 TCP 写要 15-20 分钟才报错,
    // 3× 心跳周期无任何服务端帧即主动断开触发重连(与服务端 90s 看门狗对称)
    let mut last_rx = std::time::Instant::now();
    let watchdog = Duration::from_secs(sess.heartbeat_s as u64 * 3);
    loop {
        tokio::select! {
            r = frame::read_frame(&mut control, &mut buf) => {
                match r {
                    Ok((ty, payload)) => {
                        last_rx = std::time::Instant::now();
                        if let Err(e) = dispatch_frame(&sess, &mut control, &mut buf, ty, &payload, &shutdown).await {
                            if e.to_string().starts_with("session rejected") {
                                sess.cancel.cancel();
                            }
                            warn!(error = %e, "control dispatch error");
                            break;
                        }
                    }
                    Err(e) => {
                        warn!(error = %e, "control read error");
                        break;
                    }
                }
            }
            out = send_rx.recv() => {
                match out {
                    Some((ty, payload)) => {
                        if frame::write_frame(&mut control, ty, &payload).await.is_err() {
                            break;
                        }
                    }
                    None => break,
                }
            }
            _ = hb.tick() => {
                if last_rx.elapsed() > watchdog {
                    warn!("control link watchdog: no server frames; reconnecting");
                    break;
                }
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_millis() as i64)
                    .unwrap_or(0);
                if !sess
                    .send_msg(ClientMessage::Ping(rh_protocol::message::Ping {
                        ts_ms: now,
                        probe: false,
                    }))
                    .await
                {
                    break;
                }
            }
            _ = sess.cancel.cancelled() => {
                let _ = control.shutdown().await;
                break;
            }
            _ = shutdown.cancelled() => {
                let _ = control.shutdown().await;
                break;
            }
        }
    }
    // 会话结束:停全部实体任务
    let ids: Vec<u64> = sess.visitor_cancels.lock().await.keys().copied().collect();
    for id in ids {
        stop_visitor(&sess, id).await;
    }
    let ids: Vec<u64> = sess.health_cancels.lock().await.keys().copied().collect();
    for id in ids {
        stop_proxy(&sess, id).await;
    }
}

/// 供 main 循环调用的读分发(见 run_session)。
pub async fn dispatch_frame(
    sess: &Arc<ClientSession>,
    control: &mut BoxedIo,
    buf: &mut bytes::BytesMut,
    ty: FrameType,
    payload: &[u8],
    shutdown: &CancellationToken,
) -> anyhow::Result<()> {
    // 控制流消息经会话队列下发,均为 ServerMessage 信封;
    // 裸帧(握手/数据通道指派)另见 open_data_channel。
    if matches!(
        ty,
        FrameType::SERVICE_SYNC
            | FrameType::SERVICE_UPDATE
            | FrameType::SERVICE_REMOVE
            | FrameType::CREATE_DATA_CHANNEL
            | FrameType::NAT_HOLE_PREPARE
            | FrameType::NAT_HOLE_CONNECT
            | FrameType::ERROR
    ) {
        let m: ServerMessage = serde_json::from_slice(payload)?;
        match m {
            ServerMessage::ServiceSync(s) => {
                apply_sync(sess, s.revision, s.entities).await;
                ack(sess, s.revision).await;
            }
            ServerMessage::ServiceUpdate(u) => {
                upsert(sess, u.entity).await;
                ack(sess, u.revision).await;
            }
            ServerMessage::ServiceRemove(r) => {
                remove_entity(sess, &r.entity_type, r.entity_id).await;
                ack(sess, r.revision).await;
            }
            ServerMessage::CreateDataChannel(c) => {
                tracing::debug!(request_id = c.request_id, entity = ?c.entity, "create data channel received");
                let sess2 = sess.clone();
                tokio::spawn(async move {
                    if let Err(e) = open_data_channel(sess2, c).await {
                        warn!(error = %e, "data channel failed");
                    }
                });
            }
            ServerMessage::NatHolePrepare(p) => {
                crate::nathole_cli::on_prepare(sess.clone(), p).await;
            }
            ServerMessage::NatHoleConnect(c) => {
                let s2 = sess.clone();
                let sd = shutdown.clone();
                tokio::spawn(async move {
                    crate::nathole_cli::on_connect(s2, c, sd).await;
                });
            }
            ServerMessage::Error(e) => {
                warn!(code = %e.code, msg = %e.message, "server error frame");
                if e.code == "AUTH_FAILED" || e.code == "CLIENT_DISABLED" {
                    return Err(anyhow::anyhow!("session rejected: {}", e.code));
                }
            }
            _ => warn!("unexpected envelope frame {ty:?}"),
        }
        return Ok(());
    }
    match ty {
        FrameType::PING => {
            let p: rh_protocol::message::Ping =
                serde_json::from_slice(payload).unwrap_or(rh_protocol::message::Ping {
                    ts_ms: 0,
                    probe: false,
                });
            let _ = sess
                .send_msg(ClientMessage::Ping(rh_protocol::message::Ping {
                    ts_ms: p.ts_ms,
                    probe: false,
                }))
                .await;
        }
        FrameType::PONG => {}
        other => {
            let _ = (control, buf);
            warn!(frame = ?other, "ignored frame");
        }
    }
    Ok(())
}

async fn ack(sess: &Arc<ClientSession>, revision: u64) {
    let _ = sess
        .send_msg(ClientMessage::ApplyResult(ApplyResult {
            revision,
            ok: true,
            error: None,
        }))
        .await;
}

// ---------------- 同步应用 ----------------

async fn apply_sync(
    sess: &Arc<ClientSession>,
    revision: u64,
    entities: Vec<rh_protocol::message::SyncEntity>,
) {
    // 幂等全量替换:停掉不在新集合/已变更的,启动新增/变更的
    let mut new_proxies: HashMap<u64, ProxySpec> = HashMap::new();
    let mut new_visitors: HashMap<u64, VisitorSpec> = HashMap::new();
    for e in entities {
        match e {
            rh_protocol::message::SyncEntity::Proxy(p) => {
                new_proxies.insert(p.id, *p);
            }
            rh_protocol::message::SyncEntity::Visitor(v) => {
                new_visitors.insert(v.id, v);
            }
        }
    }
    // proxies
    {
        let mut cur = sess.proxies.lock().await;
        let old: Vec<(u64, ProxySpec)> = cur.drain().collect();
        for (id, old_spec) in old {
            match new_proxies.get(&id) {
                Some(new) if *new == old_spec => {
                    cur.insert(id, old_spec);
                }
                _ => stop_proxy(sess, id).await,
            }
        }
        let existing: Vec<u64> = cur.keys().copied().collect();
        for (id, spec) in &new_proxies {
            if existing.contains(id) {
                continue;
            }
            let mut spec = spec.clone();
            spec.normalize_kind_configs();
            cur.insert(*id, spec.clone());
            start_health(sess, *id, &spec).await;
        }
    }
    // visitors
    {
        let mut cur = sess.visitors.lock().await;
        let old: Vec<(u64, VisitorSpec)> = cur.drain().collect();
        for (id, old_spec) in old {
            match new_visitors.get(&id) {
                Some(new) if *new == old_spec => {
                    cur.insert(id, old_spec);
                }
                _ => stop_visitor(sess, id).await,
            }
        }
        let existing: Vec<u64> = cur.keys().copied().collect();
        for (id, spec) in &new_visitors {
            if existing.contains(id) {
                continue;
            }
            let spec = spec.clone();
            cur.insert(*id, spec.clone());
            crate::visitor_run::start(sess.clone(), *id, spec).await;
        }
    }
    info!(revision, "sync applied");
}

async fn upsert(sess: &Arc<ClientSession>, e: rh_protocol::message::SyncEntity) {
    match e {
        rh_protocol::message::SyncEntity::Proxy(p) => {
            let mut cur = sess.proxies.lock().await;
            let changed = cur.get(&p.id).map(|o| *o != *p).unwrap_or(true);
            if changed {
                if let Some(old) = cur.remove(&p.id) {
                    let id = p.id;
                    cur.insert(id, old); // 供 stop 读取
                    stop_proxy(sess, p.id).await;
                    cur.remove(&p.id);
                }
                let mut p = p;
                p.normalize_kind_configs();
                cur.insert(p.id, (*p).clone());
                start_health(sess, p.id, &p).await;
            }
        }
        rh_protocol::message::SyncEntity::Visitor(v) => {
            let mut cur = sess.visitors.lock().await;
            let changed = cur.get(&v.id).map(|o| *o != v).unwrap_or(true);
            if changed {
                stop_visitor(sess, v.id).await;
                cur.insert(v.id, v.clone());
                crate::visitor_run::start(sess.clone(), v.id, v).await;
            }
        }
    }
}

async fn remove_entity(sess: &Arc<ClientSession>, entity_type: &str, id: u64) {
    if entity_type == "proxy" {
        sess.proxies.lock().await.remove(&id);
        stop_proxy(sess, id).await;
    } else {
        sess.visitors.lock().await.remove(&id);
        stop_visitor(sess, id).await;
    }
}

async fn stop_proxy(sess: &Arc<ClientSession>, id: u64) {
    if let Some(c) = sess.health_cancels.lock().await.remove(&id) {
        c.cancel();
    }
}

async fn stop_visitor(sess: &Arc<ClientSession>, id: u64) {
    if let Some(c) = sess.visitor_cancels.lock().await.remove(&id) {
        c.cancel();
    }
    sess.sudp_deliver.lock().await.remove(&id);
}

async fn start_health(sess: &Arc<ClientSession>, id: u64, spec: &ProxySpec) {
    let Some(hc) = spec.health_check.clone() else {
        return;
    };
    if sess.health_cancels.lock().await.contains_key(&id) {
        return;
    }
    let c = CancellationToken::new();
    sess.health_cancels.lock().await.insert(id, c.clone());
    let s2 = sess.clone();
    let spec = spec.clone();
    tokio::spawn(async move {
        crate::health::run(s2, id, spec, hc, c).await;
    });
}

// ---------------- 数据通道 ----------------

async fn open_data_channel(sess: Arc<ClientSession>, m: CreateDataChannel) -> anyhow::Result<()> {
    let mut stream = sess.mux.open_stream().await?;
    let mut buf = bytes::BytesMut::with_capacity(512);
    let hello = DataChannelHello {
        session_id: sess.session_id.clone(),
        request_id: m.request_id,
    };
    frame::write_frame(
        &mut stream,
        FrameType::DATA_CHANNEL_HELLO,
        &serde_json::to_vec(&hello)?,
    )
    .await
    .map_err(|e| anyhow::anyhow!("hello write: {e}"))?;
    match m.entity {
        ChannelEntity::Proxy { proxy_id } => {
            let spec = sess.proxies.lock().await.get(&proxy_id).cloned();
            let Some(spec) = spec else {
                anyhow::bail!("unknown proxy {proxy_id}");
            };
            if spec.kind == ProxyKind::Udp || spec.kind == ProxyKind::Sudp {
                // UDP 语义:无指派帧,直接进入 UDP 帧模式(代理侧)
                // hello 之后可能紧跟已抵达的 UDP 帧:预置进帧缓冲
                crate::udpchan::run_proxy_side(stream, spec, buf.to_vec()).await;
                return Ok(());
            }
            // TCP 语义:等待 START_FORWARD(可能延迟到用户连接);
            // 帧后可能紧随用户数据(read_frame 缓冲),必须前传
            let (ty, payload) = frame::read_frame(&mut stream, &mut buf).await?;
            let leftover: Vec<u8> = buf.to_vec();
            if ty != FrameType::START_FORWARD {
                anyhow::bail!("expected START_FORWARD, got {ty:?}");
            }
            let fwd: StartForward = serde_json::from_slice(&payload)?;
            crate::forward::handle_start_forward(sess.clone(), stream, &spec, fwd, leftover)
                .await?;
        }
        ChannelEntity::Visitor {
            visitor_id,
            user_conn_id,
        } => {
            let vspec = sess.visitors.lock().await.get(&visitor_id).cloned();
            let Some(vspec) = vspec else {
                anyhow::bail!("unknown visitor {visitor_id}");
            };
            if user_conn_id == 0 {
                // sudp:无指派帧;交给 visitor 的 UDP 交付槽(预置已到帧)
                let tx = sess.sudp_deliver.lock().await.get(&visitor_id).cloned();
                if let Some(tx) = tx {
                    let _ = tx.send(PrependedIo::wrap(stream, buf.to_vec())).await;
                }
                return Ok(());
            }
            let (ty, payload) = frame::read_frame(&mut stream, &mut buf).await?;
            let leftover: Vec<u8> = buf.to_vec();
            if ty != FrameType::START_VISITOR_RELAY {
                anyhow::bail!("expected START_VISITOR_RELAY, got {ty:?}");
            }
            let relay: StartVisitorRelay = serde_json::from_slice(&payload)?;
            crate::visitor_run::deliver_relay(sess.clone(), relay, stream, vspec, leftover).await;
        }
    }
    Ok(())
}

#[cfg(test)]
mod sizing_tests {
    //! round-10 定容公式的单测:T-E2/T-E3/T-S3 的判定核心是
    //! `plan_convergence` 的纯逻辑,不依赖真实网络即可锁定。
    use super::*;

    // T-E2:tcp 默认(无显式配置)基线 1 → initial_k=1,不收敛、不建池。
    // initial_k_for_with 注入硬件上限:本套件不得依赖跑测机器的核数
    // (≤3 核时 cpu_cap=1,tls 的 2 会假红)。
    #[test]
    fn baseline_one_stays_single() {
        let cfg = ClientConfig::default();
        let (k, _hw) = initial_k_for_with(&cfg, rh_transport::Protocol::Tcp, 4);
        assert_eq!(k, 1, "tcp 默认必须单载体");
        let (final_k, drops) = plan_convergence(k, &[]);
        assert_eq!(final_k, 1);
        assert!(drops.is_empty(), "K=1 无成员可摘");
    }

    // 红线 1:tls/wss 基线 2(硬件上限足够时);quic/kcp 恒 1。
    // 硬件上限 1(≤3 逻辑核)时 tls/wss 同样被压到 1——公式前置条件。
    #[test]
    fn baseline_two_for_tls_wss() {
        let cfg = ClientConfig::default();
        assert_eq!(
            initial_k_for_with(&cfg, rh_transport::Protocol::Tls, 4).0,
            2
        );
        assert_eq!(
            initial_k_for_with(&cfg, rh_transport::Protocol::Wss, 4).0,
            2
        );
        assert_eq!(
            initial_k_for_with(&cfg, rh_transport::Protocol::Quic, 4).0,
            1
        );
        assert_eq!(
            initial_k_for_with(&cfg, rh_transport::Protocol::Kcp, 4).0,
            1
        );
        for p in [
            rh_transport::Protocol::Tls,
            rh_transport::Protocol::Wss,
            rh_transport::Protocol::Quic,
            rh_transport::Protocol::Kcp,
        ] {
            assert_eq!(
                initial_k_for_with(&cfg, p, 1).0,
                1,
                "硬件上限 1 时任何协议都必须单载体"
            );
        }
    }

    // 红线 2:显式 Some(n) 压过基线;并仍受硬件上限与 [1,8] 钳制。
    #[test]
    fn explicit_value_overrides_baseline() {
        let cfg = ClientConfig {
            mux_pool_size: Some(3),
            ..ClientConfig::default()
        };
        assert_eq!(
            initial_k_for_with(&cfg, rh_transport::Protocol::Tcp, 4).0,
            3
        );
        assert_eq!(
            initial_k_for_with(&cfg, rh_transport::Protocol::Tcp, 2).0,
            2,
            "显式值仍受硬件上限钳制"
        );

        let cfg = ClientConfig {
            mux_pool_size: Some(99),
            ..ClientConfig::default()
        };
        assert_eq!(
            initial_k_for_with(&cfg, rh_transport::Protocol::Tcp, 4).0,
            4,
            "显式超大值受硬件上限钳制"
        );
        assert_eq!(
            initial_k_for_with(&cfg, rh_transport::Protocol::Tcp, 64).0,
            8,
            "硬件上限再大也钳制到 8"
        );

        // 服务端上限路径的 8 钳制(与硬件无关,纯函数可判定)
        assert_eq!(plan_convergence(8, &[99]), (8, Vec::new()));

        let cfg = ClientConfig {
            mux_pool_size: Some(1),
            ..ClientConfig::default()
        };
        assert_eq!(
            initial_k_for_with(&cfg, rh_transport::Protocol::Tls, 4).0,
            1,
            "显式单载体必须压过 tls 基线 2"
        );
    }

    // T-E3:服务端 cap=1 → final_k=1,超出部分全部摘除。
    #[test]
    fn server_cap_converges_to_one() {
        let (final_k, drops) = plan_convergence(2, &[1]);
        assert_eq!(final_k, 1);
        assert_eq!(drops, vec![1], "只摘成员 1(成员 0 = 控制流)");
    }

    // 旧服务端不回 ACK(server_caps 空)→ 保守收敛到 1,摘除报到顺序的
    // 尾部(不按失败下标摘——失败可能是轮转落点漂移造成的假失败)。
    #[test]
    fn missing_acks_conservatively_converge() {
        let (final_k, drops) = plan_convergence(3, &[]);
        assert_eq!(final_k, 1);
        assert_eq!(drops, vec![1, 2], "尾部全部摘除");

        // 部分成功:取成功者最小值,只摘尾部
        let (final_k, drops) = plan_convergence(4, &[8, 2]);
        assert_eq!(final_k, 2);
        assert_eq!(drops, vec![2, 3]);
    }

    // T-S3(报文判定部分):ok:true 计入上限并钳制 [1,8];ok:false 保留
    // 错误码;非法 JSON 归为坏报文;三者都不 panic、不阻断后续载体。
    #[test]
    fn attach_ack_payload_classification() {
        assert_eq!(
            parse_attach_ack_payload(br#"{"ok":true,"max_carriers":8}"#),
            AckPayload::Ok(8)
        );
        assert_eq!(
            parse_attach_ack_payload(br#"{"ok":true,"max_carriers":99}"#),
            AckPayload::Ok(8),
            "服务端上限必须钳制到 8"
        );
        assert_eq!(
            parse_attach_ack_payload(
                br#"{"ok":false,"error_code":"SESSION_MISMATCH","max_carriers":8}"#
            ),
            AckPayload::Rejected(Some("SESSION_MISMATCH".into()))
        );
        assert!(matches!(
            parse_attach_ack_payload(b"not-json"),
            AckPayload::Bad(_)
        ));
        // 超时/非 ACK 帧/读错误在调用侧归入"未确认"——不代表该成员死亡,
        // 但会记录该次报到的载体身份,收敛时按身份摘除(不按下标摘除);
        // 保守上限仍由 server_caps 为空时的 1 兜住。
    }

    // 服务端 cap 大于 initial_k:不补拨(红线 3),drop_set 为空。
    #[test]
    fn larger_server_cap_never_adds_carriers() {
        let (final_k, drops) = plan_convergence(2, &[8]);
        assert_eq!(final_k, 2, "不因 max_carriers 变大而补拨");
        assert!(drops.is_empty());
    }

    // T-R4(round-10.1):待摘除集合 = 失败身份 ∪ 超出保留额的成功载体。注入
    // 成员表模拟"报到窗口内 reaper 已压缩成员表"与"重拨补位拿新 id"两种
    // 形态:保留额按**当前表内成功数**计,失败身份在表的任何位置都照样摘除。
    #[test]
    fn detach_set_merges_failed_ids_and_table_tail() {
        // 表未压缩:4 成员(1..=4),final_k=2 → 控制载体 1 占一个名额,成功
        // 成员 3 保留,成员 4 超出;失败身份 2 合并。成员 3 必须保留——
        // 按 skip(final_k) 取尾部会把它误摘,物理载体只剩控制载体一条。
        assert_eq!(plan_detach_set(&[1, 2, 3, 4], 2, &[2], 1), vec![2, 4]);
        // 复核 finding 的最小复现:initial_k=3、final_k=2、第 1 次报到失败
        // (成员 2)、第 2 次成功(成员 3)→ 只摘失败的 2。
        assert_eq!(plan_detach_set(&[1, 2, 3], 2, &[2], 1), vec![2]);
        // 表已压缩(成员 2 被 reaper 剔除,表 [1,3,4,5]):控制载体 + 成功
        // 成员 3 保留,尾部 4,5 摘除
        assert_eq!(plan_detach_set(&[1, 3, 4, 5], 2, &[], 1), vec![4, 5]);
        // 重拨补位:失败身份 7 已在表中(位置靠后),与尾部去重
        assert_eq!(plan_detach_set(&[1, 7, 8], 1, &[7], 1), vec![7, 8]);
        // 失败身份已不在表(被并发剔除):集合只含尾部,摘除时该 id 返回
        // false(幂等),不 panic 也不误摘其它成员
        assert_eq!(plan_detach_set(&[1, 2], 1, &[99], 1), vec![2, 99]);
        // 无失败且表长 == final_k:无需摘除
        assert!(plan_detach_set(&[1, 2], 2, &[], 1).is_empty());
        // failed_ids 重复去重
        assert_eq!(plan_detach_set(&[1, 2, 3], 1, &[2, 2], 1), vec![2, 3]);
        // 控制载体永不摘除:即便它落在尾部或出现在失败身份里
        assert_eq!(plan_detach_set(&[1, 2, 3], 1, &[1], 1), vec![2, 3]);
        assert_eq!(plan_detach_set(&[4, 1], 1, &[4], 1), vec![4]);
        // 控制载体已被 reaper 剔除时它不再占保留名额:保留成功的前 final_k 个
        assert_eq!(plan_detach_set(&[2, 3, 4], 2, &[], 1), vec![4]);
        // 前缀内失败多于名额:失败全摘;成功且非控制的只剩 2 条,少于保留
        // 额(final_k - 1)时不再多摘——不误杀任何一个成功载体
        assert_eq!(plan_detach_set(&[1, 2, 3, 4, 5], 3, &[2, 3], 1), vec![2, 3]);
    }

    // ===== round-10.2 §2.2:终态收敛存活过滤 =====

    use std::pin::Pin;
    use std::sync::atomic::AtomicBool;
    use std::task::{Context, Poll, Waker};

    struct KillState {
        dead: AtomicBool,
        waker: std::sync::Mutex<Option<Waker>>,
    }

    impl KillState {
        fn kill(&self) {
            self.dead.store(true, Ordering::SeqCst);
            if let Some(w) = self.waker.lock().unwrap().take() {
                w.wake();
            }
        }
    }

    /// 可杀载体 IO:kill 后读侧立即 BrokenPipe 并唤醒挂起的 driver。
    struct KillIo {
        inner: tokio::io::DuplexStream,
        state: Arc<KillState>,
    }

    impl tokio::io::AsyncRead for KillIo {
        fn poll_read(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut tokio::io::ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            let this = self.get_mut();
            if this.state.dead.load(Ordering::SeqCst) {
                return Poll::Ready(Err(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "carrier killed",
                )));
            }
            let r = Pin::new(&mut this.inner).poll_read(cx, buf);
            if r.is_pending() {
                *this.state.waker.lock().unwrap() = Some(cx.waker().clone());
            }
            if this.state.dead.load(Ordering::SeqCst) {
                return Poll::Ready(Err(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "carrier killed",
                )));
            }
            r
        }
    }

    impl tokio::io::AsyncWrite for KillIo {
        fn poll_write(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            if self.state.dead.load(Ordering::SeqCst) {
                return Poll::Ready(Err(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "carrier killed",
                )));
            }
            Pin::new(&mut self.get_mut().inner).poll_write(cx, buf)
        }
        fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Pin::new(&mut self.get_mut().inner).poll_flush(cx)
        }
        fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
        }
    }

    /// 建一条可杀客户端载体,返回 (池成员, 对端保活, kill 开关)。
    fn killable_client_carrier() -> (
        rh_transport::mux::YamuxMultiplexer,
        tokio::io::DuplexStream,
        Arc<KillState>,
    ) {
        let (a, b) = tokio::io::duplex(256 * 1024);
        let state = Arc::new(KillState {
            dead: AtomicBool::new(false),
            waker: std::sync::Mutex::new(None),
        });
        let io = KillIo {
            inner: a,
            state: state.clone(),
        };
        let mux = rh_transport::wrap_stream_as_client(Box::new(io), true, 16);
        let mux = Arc::try_unwrap(mux)
            .ok()
            .expect("sole owner")
            .into_yamux()
            .unwrap();
        (mux, b, state)
    }

    /// round-10.2 §2.2 regression:成员表 `[健康, dead-but-unreaped, 健康]`、
    /// final_k=2 时,终态收敛按存活表计保留额——必须**不摘**第三个健康
    /// 成员(修复前按全表计数会把保留额算给 dead 成员,误摘健康成员,
    /// 终态 live 载体只剩 1)。断言终态 live_carriers == 2。
    #[tokio::test]
    async fn termina_convergence_skips_dead_member() {
        let (m0, _peer0, _k0) = killable_client_carrier();
        let (m1, _peer1, k1) = killable_client_carrier();
        let (m2, _peer2, _k2) = killable_client_carrier();
        let pool = rh_transport::Multiplexer::yamux_pool_client(vec![m0, m1, m2], None);
        let ids = pool.member_ids().await;
        assert_eq!(ids.len(), 3);
        let control_id = ids[0];

        // 杀中间成员:等它被标记 dead,但健康监视(500ms 周期)尚未 reap
        // ——这正是"dead-but-unreaped"窗口。
        k1.kill();
        let t0 = std::time::Instant::now();
        while pool.member_ids().await.len() != 3 || pool.pool_live_carriers().await != Some(2) {
            assert!(
                t0.elapsed() < std::time::Duration::from_millis(400),
                "中间成员必须 dead 但未被 reap(窗口内)"
            );
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }

        // final_k=2:控制成员 1 + 一个健康成员应保留;第三个健康成员不得被摘。
        terminal_convergence(&pool, 2, control_id).await;

        let live_ids = pool.live_member_ids().await;
        assert_eq!(
            live_ids.len(),
            2,
            "终态存活载体必须为 2(不得把保留额算给 dead 成员后误摘健康成员)"
        );
        assert_eq!(pool.pool_live_carriers().await, Some(2));
        assert!(
            live_ids.contains(&ids[2]),
            "第三个健康成员必须保留(修复前会被误摘)"
        );
        assert!(live_ids.contains(&control_id), "控制成员必须保留");
    }

    /// round-10.2 复核回归:按**生产顺序**走初次收敛 + 终态收敛。成员表
    /// `[健康, dead-but-unreaped, 健康]`、final_k=2 时,初次收敛若按未过滤
    /// 的 `member_ids` 计保留额,会把 dead 成员算进 keep_others,第三个
    /// 健康成员在此就被摘掉;终态收敛只减不增,补不回来,终态 live=1。
    /// 修复后两次收敛同按存活表计额,终态 live=2。
    #[tokio::test]
    async fn initial_then_terminal_convergence_skips_dead_member() {
        let (m0, _peer0, _k0) = killable_client_carrier();
        let (m1, _peer1, k1) = killable_client_carrier();
        let (m2, _peer2, _k2) = killable_client_carrier();
        let pool = rh_transport::Multiplexer::yamux_pool_client(vec![m0, m1, m2], None);
        let ids = pool.member_ids().await;
        assert_eq!(ids.len(), 3);
        let control_id = ids[0];

        k1.kill();
        let t0 = std::time::Instant::now();
        while pool.member_ids().await.len() != 3 || pool.pool_live_carriers().await != Some(2) {
            assert!(
                t0.elapsed() < std::time::Duration::from_millis(400),
                "中间成员必须 dead 但未被 reap(窗口内)"
            );
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }

        // 生产顺序:初次收敛 → 终态收敛。
        initial_convergence(&pool, 2, &[], control_id).await;
        terminal_convergence(&pool, 2, control_id).await;

        let live_ids = pool.live_member_ids().await;
        assert_eq!(
            live_ids.len(),
            2,
            "生产顺序必须保留 2 条存活载体(初次收敛不得把额度算给 dead 成员)"
        );
        assert_eq!(pool.pool_live_carriers().await, Some(2));
        assert!(live_ids.contains(&ids[2]), "第三个健康成员必须保留");
        assert!(live_ids.contains(&control_id), "控制成员必须保留");
    }
}
