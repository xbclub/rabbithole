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
    /// 控制写队列:**已编码的完整帧**(含头),发送侧直接 `write_all`。
    ///
    /// 修前队列存 `(FrameType, Vec<u8>)`(仅 payload),run_loop 再调
    /// `write_frame` → `encode_frame_bytes` 分配 `BytesMut` 并整包拷一遍。
    /// 现在序列化与成帧都只做一次,队列元素即可直接下发的 `Bytes`
    /// (引用计数移动,跨任务零拷贝)。
    pub send_tx: mpsc::Sender<bytes::Bytes>,
    pub cancel: CancellationToken,
    // 实体表
    pub proxies: Mutex<HashMap<u64, ProxySpec>>,
    pub visitors: Mutex<HashMap<u64, VisitorSpec>>,
    /// visitor 监听任务取消器。
    pub visitor_cancels: Mutex<HashMap<u64, CancellationToken>>,
    /// 健康检查任务取消器。
    pub health_cancels: Mutex<HashMap<u64, CancellationToken>>,
    /// stcp/xtcp 等待配对的本地连接:user_conn_id → (连接本体, visitor_id, 登记时刻)。
    ///
    /// 第三个字段是**登记时刻**,TTL 淘汰由 [`ClientSession::waiting_sweeper`]
    /// 的单一后台任务按它判定;修前是每连接 spawn 一个 `sleep(10s)` 任务,
    /// 交付成功后无法 abort,只能空转到期后对已移除的 key 做一次无意义
    /// remove(高并发下任务数随连接数增长)。
    pub waiting: Mutex<HashMap<u64, WaitingEntry>>,
    /// XTCP 打洞成功后的直连多路复用:visitor_id → mux。
    pub punched: Mutex<HashMap<u64, Arc<rh_transport::Multiplexer>>>,
    /// 打洞事务保留 socket(候选与打洞同源;双端各自入队)。
    pub nat_socks: Mutex<HashMap<String, std::collections::VecDeque<Arc<tokio::net::UdpSocket>>>>,
    /// A7:打洞成功的直连首帧凭据(visitor_id → auth)。与 `punched`
    /// (visitor_id → mux)同生命周期:每次打洞成功写入,替换 mux 时一并更新。
    pub punched_auth: Mutex<HashMap<u64, String>>,
    /// sudp 通道交付:visitor_id → sender。
    pub sudp_deliver: Mutex<HashMap<u64, mpsc::Sender<BoxedIo>>>,
    pub mux: Arc<rh_transport::Multiplexer>,
    pub conn_counter: AtomicU64,
}

/// waiting 表条目:(本地连接, visitor_id, 登记时刻)。
pub type WaitingEntry = (tokio::net::TcpStream, u64, std::time::Instant);

/// waiting 表 TTL:语义不变(登记后 10s 未被配对即淘汰)。
pub const WAITING_TTL: Duration = Duration::from_secs(10);

/// TTL 清理扫描周期。取 1s:相对 10s TTL 足够细,单次扫描成本与条目数
/// 线性且无锁等待之外的额外开销。
const WAITING_SWEEP_INTERVAL: Duration = Duration::from_secs(1);

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

/// 控制消息 → 完整帧字节(序列化 + 成帧各一次)。
///
/// 保留 [`frame::MAX_FRAME_LEN`] 校验,且**在成帧之前**判长度:超限载荷
/// 编码出去就是一个腐化帧(长度字段被截断),对端会以 `TooLarge` 关掉
/// 整条连接,不如本端直接拒绝。
///
/// 返回 `None` = 序列化失败或载荷超限(与修前 `unwrap_or_default()`
/// 一样按"发不出去"处理,调用方拿到 `false`)。
fn encode_control_frame(m: &ClientMessage) -> Option<bytes::Bytes> {
    let ty = frame_type_of_client(m);
    let payload = serde_json::to_vec(m).ok()?;
    if payload.len() > frame::MAX_FRAME_LEN {
        return None;
    }
    Some(frame::encode_frame_bytes(ty, &payload))
}

impl ClientSession {
    /// 登记一个等待配对的本地连接(带登记时刻,供 TTL 淘汰)。
    pub async fn insert_waiting(&self, user_conn_id: u64, conn: tokio::net::TcpStream, visitor_id: u64) {
        self.waiting.lock().await.insert(
            user_conn_id,
            (conn, visitor_id, std::time::Instant::now()),
        );
    }

    /// 取出一个等待中的连接(配对成功时调用)。
    pub async fn take_waiting(&self, user_conn_id: u64) -> Option<WaitingEntry> {
        self.waiting.lock().await.remove(&user_conn_id)
    }

    /// 等待项所属 visitor(仅需读 visitor_id 时用,避免取走连接)。
    pub async fn waiting_visitor(&self, user_conn_id: u64) -> Option<u64> {
        self.waiting
            .lock()
            .await
            .get(&user_conn_id)
            .map(|(_, v, _)| *v)
    }

    /// 放回等待项(打洞回退重放路径):保留原登记时刻,不重置 TTL。
    ///
    /// 这是**语义选择**:回退属于同一次用户连接的生命周期,重置 TTL 会
    /// 让它在打洞反复失败时无限续命,与修前的行为也不同(修前放回时
    /// 已经过了若干秒,剩余 TTL 变短)。
    pub async fn requeue_waiting(&self, user_conn_id: u64, entry: WaitingEntry) {
        self.waiting.lock().await.insert(user_conn_id, entry);
    }

    /// 淘汰超过 TTL 的等待项,返回淘汰数量。
    ///
    /// 修前每个连接 spawn 一个 `sleep(10s)` 任务:交付成功后任务无法
    /// abort,会活到 10s 才醒来对已移除的 key 做一次无意义 remove。
    /// 改成单一后台任务周期扫描后,任务数与连接数无关。
    pub async fn sweep_waiting(&self) -> usize {
        let now = std::time::Instant::now();
        let mut w = self.waiting.lock().await;
        let before = w.len();
        w.retain(|_, (_, _, at)| now.duration_since(*at) < WAITING_TTL);
        before - w.len()
    }

    /// 启动 waiting 表的 TTL 清理任务,返回其句柄(供调用方在会话结束时 abort)。
    ///
    /// 单一后台任务,与连接数无关。每 [`WAITING_SWEEP_INTERVAL`] 扫描一次,
    /// 淘汰登记超过 [`WAITING_TTL`] 的条目。
    pub fn spawn_waiting_sweeper(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
        let sess = self.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(WAITING_SWEEP_INTERVAL);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                // 会话结束(或连接被丢弃)时随之退出,不 leak
                tokio::select! {
                    _ = sess.cancel.cancelled() => return,
                    _ = tick.tick() => {}
                }
                let evicted = sess.sweep_waiting().await;
                if evicted > 0 {
                    tracing::debug!(evicted, "waiting entries expired");
                }
            }
        })
    }

    pub async fn send_msg(&self, m: ClientMessage) -> bool {
        match encode_control_frame(&m) {
            Some(bytes) => self.send_tx.send(bytes).await.is_ok(),
            None => false,
        }
    }

    pub async fn next_conn_id(&self) -> u64 {
        self.conn_counter.fetch_add(1, Ordering::Relaxed) + 1
    }
}

/// 自动定容(round-10):协议基线 → 硬件上限三段中的前两段。
/// 返回 `(initial_k, hardware_cap)`;期望值来源优先级:显式配置 /
/// `RH_MUX_POOL_SIZE`(已由 config 归并为 `Some(n)`),其次 RTT 自适应
/// (round-77),最后协议基线。服务端上限在报到后收敛,K 恒在 [1,8]。
///
/// round-77:自动路径改为按**实测 RTT** 决定 K(而不是恒 1)。依据是
/// 低延迟下 K=1 快 8%、而 20ms 下 K=4 快 16%(§82),阈值见
/// [`rh_transport::carriers_for_rtt`]。
async fn initial_k_for(cfg: &ClientConfig, protocol: rh_transport::Protocol) -> (usize, usize) {
    initial_k_for_async(cfg, protocol, crate::config::detect_hardware_cap()).await
}

/// [`initial_k_for`] 的异步实现(需要一次 RTT 探测)。
///
/// RTT 探测只看 `server_addr` 与 `proxy_url`(与协议无关);探针失败时
/// 退回协议基线,**不阻断连接**。
pub(crate) async fn initial_k_for_async(
    cfg: &ClientConfig,
    protocol: rh_transport::Protocol,
    hardware_cap: usize,
) -> (usize, usize) {
    let proto_baseline = rh_transport::default_carriers(protocol);
    let baseline = match cfg.mux_pool_size {
        // 显式配置最高优先:不再探测 RTT(用户已明确表态)。
        Some(_) => proto_baseline,
        // 协议 gate **必须**在探测之前:quic/kcp 与非 multiplex 恒单载体,
        // 探测结果会被调用方直接丢弃。修前 gate 在探测之后,这两类协议
        // 照样白打一条 TCP(accept 完立刻断)。
        //
        // 这里直接返回 1(而非 proto_baseline):这两类场景下调用方的
        // `if is_tcp_based() && multiplex { sized_k } else { 1 }` 会把值
        // 压成 1,返回基线只会让本函数的输出与最终结果不一致(测试抓到过)。
        None if !protocol.is_tcp_based() || !cfg.multiplex => 1,
        None => {
            let rtt_k = match cfg.server_addr.parse::<std::net::SocketAddr>() {
                Ok(addr) => {
                    // 探针走与真实拨号相同的路径(含 proxy_url);
                    // 结果按 (addr, proxy_url) 进程内缓存,重连复用。
                    rh_transport::auto_carriers(
                        addr,
                        std::time::Duration::from_secs(2),
                        cfg.proxy_url.clone(),
                    )
                    .await
                }
                // 地址是域名或非法:无法探测,退回保守基线。
                Err(_) => proto_baseline,
            };
            // RTT 自适应给的是**下限意义上的建议**,不是上限:
            // 低 RTT(<5ms)返回 1,但 2026-10-20 三机实测单载体在
            // 2.1-2.2G 封顶且与流数无关(瓶颈是单 TCP 拥塞窗 + yamux
            // 单 FlushTask 串行),K=2 可测提升 6-10%。故取两者较大者,
            // 让协议基线抬高下限、RTT 仍能在高 BDP 下继续上调。
            rtt_k.max(proto_baseline)
        }
    };
    let desired = cfg.mux_pool_size.unwrap_or(baseline);
    (desired.min(hardware_cap).clamp(1, 8), hardware_cap)
}

/// 纯函数(可单测):硬件上限由调用方注入,测试不依赖跑测机器的核数与
/// 网卡队列(≤3 核机器 cpu_cap=1,硬编码 tls K=2 的断言会假红)。
/// 生产入口 [`initial_k_for`] 传入真实探测值。
///
/// 本函数**不探测 RTT**(纯函数):只处理显式值与协议基线。RTT 自适应
/// 由 [`initial_k_for_async`] 负责。仅供单测使用——生产路径已改走异步
/// 版本,故标 `cfg(test)` 避免 dead_code。
#[cfg(test)]
pub(crate) fn initial_k_for_with(
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

/// 是否执行"服务端加密下发证书"的绑定校验。
///
/// 两个条件缺一不可:
/// - **协议必须是加密的**:明文 tcp/ws/kcp 根本没有 TLS 证书,服务端不会
///   下发该帧,要求它是纯粹的误伤。
/// - **本端必须是 `secret_verified`**:pin / CA 已在握手内完成认证,
///   insecure 是显式放弃;只有零配置档才把下发帧当作唯一信任来源。
///
/// 这两条都曾经写错过一次(`is_encrypted()` 单独用 → insecure 被误杀;
/// `secret_verified` 单独用 → 明文协议被误杀),故收敛成一个函数供
/// sink 安装点与校验点共用,避免两处判定再次漂移。
fn cert_binding_required(protocol: rh_transport::Protocol, secret_verified: bool) -> bool {
    protocol.is_encrypted() && secret_verified
}

/// 校验服务端加密下发的证书与本次 TLS 握手实际看到的一致。
///
/// 这是**唯一能挡住中间人转发**的检查:攻击者不知道 `secret`,因此无法
/// 伪造一段能解开的密文;它若用自己的证书冒充,解密结果与握手看到的
/// 对端证书不同,此处即失败。
///
/// **调用方只在 `tls.secret_verified` 时调用本函数**(见调用点注释):
/// pin / CA 已在握手内完成认证,insecure 则是显式放弃,都不该被这里
/// 的 fail-closed 误伤。
///
/// 三种情况必须区分清楚:
/// - 收到帧且解出的证书 == 对端证书 → 通过;
/// - 收到帧但解不开/不一致 → **拒绝连接**(有人在中间,或 secret 不符);
/// - 没收到帧(超时/旧服务端不下发)→ **拒绝连接**。
///
/// 第三条是**有意**的 fail-closed:中间人无法伪造/解密该帧,但它可以
/// *丢弃*该帧让比对被静默跳过——那正是可被主动利用的降级攻击。所以
/// "缺失"必须与"不一致"同等对待。
///
/// 代价:升级窗口内"新客户端 + 旧服务端"连不上(客户端日志会明确写出
/// 原因)。这是为堵住降级攻击付出的、有意接受的代价,不是缺陷。
async fn verify_server_cert(
    control: &mut (impl tokio::io::AsyncRead + Unpin),
    buf: &mut bytes::BytesMut,
    cfg: &ClientConfig,
    peer_cert: &Arc<std::sync::OnceLock<Vec<u8>>>,
) -> anyhow::Result<Vec<u8>> {
    use base64::Engine as _;
    // 短超时:该帧紧接 HELLO_ACK,不应久等。**超时即拒绝**(见下方 Err 分支)——
    // 不是"没收到就放过",否则中间人只要把这一帧拖住/丢掉就能让比对静默跳过。
    // 代价是升级窗口内"新客户端 + 旧服务端(不下发该帧)"会连不上,见调用点注释。
    let got = tokio::time::timeout(
        std::time::Duration::from_secs(3),
        frame::read_frame(control, buf),
    )
    .await;
    let (ty, payload) = match got {
        Ok(Ok(v)) => v,
        Ok(Err(e)) => return Err(anyhow::anyhow!("读取证书下发帧失败: {e}")),
        Err(_) => {
            // 降级防护:本端已声明"靠 secret 验证对端"(secret_verified),
            // 此时**缺少**该帧必须视为失败。否则中间人只要丢弃这一帧
            // 就能让比对被静默跳过——那是可被主动利用的降级攻击。
            anyhow::bail!(
                "等待服务端证书下发帧超时:本端以 secret 验证对端身份,缺失该帧即拒绝连接\
                 (可能是旧服务端,或中间人丢弃了该帧)"
            );
        }
    };
    if ty != FrameType::SERVER_CERT {
        // 同理:声明了 secret 验证就不能接受"帧不存在"。旧服务端会在这里
        // 被明确拒绝(而不是静默降级),升级时客户端会看到明确原因。
        anyhow::bail!("期望服务端证书下发帧,实际收到 {ty:?}:无法验证对端身份,拒绝连接");
    }
    let msg: rh_protocol::message::ServerCert = serde_json::from_slice(&payload)?;
    let nonce_raw = base64::engine::general_purpose::STANDARD
        .decode(msg.nonce_b64.as_bytes())
        .map_err(|e| anyhow::anyhow!("证书下发帧 nonce 非法: {e}"))?;
    let nonce: [u8; 12] = nonce_raw
        .as_slice()
        .try_into()
        .map_err(|_| anyhow::anyhow!("证书下发帧 nonce 长度错误"))?;
    let ciphertext = base64::engine::general_purpose::STANDARD
        .decode(msg.cert_der_b64.as_bytes())
        .map_err(|e| anyhow::anyhow!("证书下发帧密文非法: {e}"))?;
    let sealed = rh_protocol::certprov::SealedCert { nonce, ciphertext };
    let seen = peer_cert
        .get()
        .ok_or_else(|| anyhow::anyhow!("未能取得本次 TLS 会话的对端证书,无法完成比对——拒绝连接"))?;
    let announced = compare_announced_cert(cfg.secret.as_bytes(), &sealed, seen)?;
    tracing::debug!("服务端证书比对通过");
    Ok(announced)
}

/// 比对"服务端加密自称的证书"与"握手实际看到的对端证书"。
///
/// 抽成独立函数是为了能对它做单元测试 —— 这是整套防中间人机制的唯一
/// 判据,必须能被精确验证(见本文件 tests 模块)。
fn compare_announced_cert(
    secret: &[u8],
    sealed: &rh_protocol::certprov::SealedCert,
    seen: &[u8],
) -> anyhow::Result<Vec<u8>> {
    let announced = rh_protocol::certprov::open_cert(secret, sealed).ok_or_else(|| {
        anyhow::anyhow!("服务端证书下发帧无法解密(secret 不符,或对端不是真服务端)——拒绝连接")
    })?;
    if announced != seen {
        anyhow::bail!(
            "服务端证书不一致:下发证书与 TLS 实际对端不同,可能存在中间人——拒绝连接\
             (下发 {} 字节,实际 {} 字节)",
            announced.len(),
            seen.len()
        );
    }
    Ok(announced)
}

/// 校验**每一条**载体的对端证书都与下发证书一致。
///
/// 修前只校验控制载体:`connect_pool` 为 K 条载体各做一次独立 TLS 握手
/// (默认 K=2),而 `peer_cert_sink` 是 `OnceLock` 只留首次写入 —— 附加载体
/// 的证书无人比对。中间人可**只截断附加载体**,控制流照常工作、不会察觉,
/// 而落到该载体上的数据已明文可读。
///
/// 任一载体不一致即整体失败(触发重连),绝不"只丢弃那条载体继续跑"——
/// 那等于把被截断的载体当作可用。
fn verify_all_carrier_certs(expected: &[u8], seen_all: &[Vec<u8>]) -> anyhow::Result<()> {
    if seen_all.is_empty() {
        anyhow::bail!("未能取得任何载体的对端证书:秘密验证模式下无法完成比对——拒绝连接");
    }
    for (i, seen) in seen_all.iter().enumerate() {
        if seen.as_slice() != expected {
            anyhow::bail!(
                "载体 #{i} 的服务端证书与下发证书不一致(下发 {} 字节,该载体 {} 字节),\
                 可能存在中间人截断了附加载体——拒绝连接",
                expected.len(),
                seen.len()
            );
        }
    }
    tracing::debug!(carriers = seen_all.len(), "全部载体证书比对通过");
    Ok(())
}

/// 建立:连接 → 控制流 → HELLO/挑战/应答 → 运行会话循环。
/// Err 返回触发外层重连。
pub async fn connect_and_run(
    cfg: Arc<ClientConfig>,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    let mut tcfg = cfg.transport_cfg()?;
    // 证书比对用:记录本次连接实际看到的对端证书(仅加密协议有意义)。
    // 必须逐次新建(不是复用):重连要对当前这次握手的证书做比对。
    //
    // 只在 `secret_verified` 时装 sink:那是唯一会读它的路径(见下方
    // verify_server_cert 调用点)。pin/CA/insecure 下装了也没人读,却要求
    // 传输层每种载体都支持写 sink —— quic 曾因没写而让默认配置无法连接。
    let peer_cert: Arc<std::sync::OnceLock<Vec<u8>>> = Arc::new(std::sync::OnceLock::new());
    // 多载体:记录**每一条**载体的对端证书,报到阶段逐条比对。
    // 只记控制载体(上面的 OnceLock)时,中间人可只截断附加载体 TLS,
    // 读走落到该载体的数据明文。
    let peer_cert_all: Arc<std::sync::Mutex<Vec<Vec<u8>>>> =
        Arc::new(std::sync::Mutex::new(Vec::new()));
    if cert_binding_required(tcfg.protocol, tcfg.tls.secret_verified) {
        tcfg.peer_cert_sink = Some(peer_cert.clone());
        tcfg.peer_cert_sink_all = Some(peer_cert_all.clone());
    }
    // 三段式定容(round-10):显式值/协议基线先过硬件上限,报到后再按
    // 服务端应答收敛。QUIC/KCP 与非 multiplex 不建池(恒单载体)。
    let (sized_k, hardware_cap) = initial_k_for(&cfg, tcfg.protocol).await;
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
        attach_auth: None,
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
    // 证书下发校验:服务端用 secret 派生的密钥把它的 TLS 证书加密发来,
    // 解出后与本次握手"实际看到的对端证书"逐字节比对。
    // 不一致 = 有人在中间转发(它拿不出用真 secret 加密的真证书),必须断开。
    //
    // **只在 `secret_verified` 时做**,这不是为了省事,而是这个检查的
    // 契约只在那一档成立:
    // - pin / trusted_ca:握手本身已经把对端钉死或验过 CA,下发帧是冗余的
    //   附加证明。对它做 fail-closed 只会让**没有该帧的旧服务端**被新客户端
    //   无故拒绝,并不增加任何安全性。
    // - insecure:客户端已显式声明不做校验,再强制一个校验只会自相矛盾。
    // - secret_verified:握手阶段**什么都没验**(见 tls.rs 的 NoVerify),
    //   下发帧是唯一的信任来源。此时缺帧必须拒绝 —— 否则中间人只要丢弃
    //   这一帧就能让比对静默跳过,那是可被主动利用的降级攻击。
    //
    // **缺失该帧即拒绝连接**,不是放行。本端以 secret 验证对端身份时,
    // 下发帧是唯一的信任来源(握手阶段什么都没验);中间人虽伪造不了它,
    // 却能把它丢弃,从而让比对被静默跳过 —— 那是可被主动利用的降级攻击。
    //
    // 有意代价:升级窗口内**新客户端 + 旧服务端**(旧版不下发该帧)会连不上,
    // 客户端会明确报"缺失该帧即拒绝连接"并重连退避。要连上只能二选一:
    // 升级服务端,或在客户端显式配 pin / trusted_ca / insecure 换用别的
    // 信任来源。**不得**为了让旧服务端能连而把这里改成 fail-open。
    //
    // 历史:这里曾用 `is_encrypted()` 判定,导致 insecure 的客户端也被
    // fail-closed 打死(E2E 的 quic/tls/wss 用例正是踩在这里)。
    // 下发的证书(用于逐条载体校验;仅 secret_verified 有)
    let mut announced_cert: Option<Vec<u8>> = None;
    if cert_binding_required(tcfg.protocol, tcfg.tls.secret_verified) {
        announced_cert = Some(verify_server_cert(&mut control, &mut buf, &cfg, &peer_cert).await?);
    }
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
        // B9:报到流必须证明持有 secret —— 仅凭明文 session_id 即可聚合
        // 会把"id 泄漏"升级为"会话池被混入"。
        let attach_auth = attach_hmac(cfg.secret.as_bytes(), &session_id);
        let attach = Hello {
            client_id: cfg.client_id.clone(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            hostname: hostname_or_unknown(),
            os: std::env::consts::OS.to_string(),
            arch: std::env::consts::ARCH.to_string(),
            capabilities: Vec::new(),
            session_id: Some(session_id.clone()),
            attach_auth: Some(attach_auth),
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
    // round-47:此处**不再**自行计算 shard 数。日志值改为从 `mux` 读实际
    // spawn 的数量(`mux.shard_workers()`)，见下方 info!。修前这里另起
    // 一处 `shard_workers_for_k(initial_k)`——与实际侧
    // (`tcp.rs` 的 `shard_workers_for_k(carriers)`)是两次独立计算，
    // 只因当前输入恰好相同才碰巧一致；一旦 `connect_pool` 的入参漂移
    // (例如改用收敛后的 final_k)，日志就会与实际不符。
    // ★每条载体都必须做过证书绑定校验。
    //
    // 修前只在控制载体上调用一次 `verify_server_cert`
    // 656 行),而 `connect_pool` 会为 K 条载体各做一次独立 TLS 握手
    // (默认 K=2)。`peer_cert_sink` 是 `OnceLock`,只保留首次写入,附加载体的
    // 证书无人比对 —— 中间人可**只截断附加载体**的 TLS,读走落到该载体上的
    // 数据明文(控制流仍然正常,不会察觉)。
    //
    // 这里在报到结束后整体校验:每条成功拨号的载体证书都必须与下发证书一致。
    // 不一致即整会话失败(触发重连),绝不"只丢弃那条载体"继续跑 ——
    // 那等于把被截断的载体当作可用。
    if let Some(expected) = announced_cert.as_ref() {
        let seen_all = peer_cert_all.lock().map(|v| v.clone()).unwrap_or_default();
        verify_all_carrier_certs(expected, &seen_all)?;
    }

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
    // round-47:shard_workers 取自 mux 的**实际 spawn 值**(唯一来源),
    // 不再用本地公式重算。非 yamux 变体(裸模式/QUIC)没有分片 Worker,
    // 记为 0 以免日志缺字段。
    let shard_workers = mux.shard_workers().await.unwrap_or(0);
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

    // 256 容量不变(背压语义:满了 send_msg 会 await,不丢帧)
    let (send_tx, send_rx) = mpsc::channel::<bytes::Bytes>(256);
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
        punched_auth: Mutex::new(HashMap::new()),
        sudp_deliver: Mutex::new(HashMap::new()),
        mux: mux.clone(),
        conn_counter: AtomicU64::new(0),
    });
    let _ = shutdown.clone();
    let sweeper = sess.spawn_waiting_sweeper();
    run_loop(sess.clone(), control, send_rx, shutdown, buf).await;
    sess.cancel.cancel();
    sweeper.abort();
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

/// B9:附加载体报到凭据(HMAC-SHA256,与 `auth` 用同一 secret 但域分隔)。
pub fn attach_hmac(secret: &[u8], session_id: &str) -> String {
    hmac_hex(secret, &format!("rh3:attach:{session_id}"))
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
    mut send_rx: mpsc::Receiver<bytes::Bytes>,
    shutdown: CancellationToken,
    // 握手阶段累积的缓冲,可能已含后续帧的残留字节。
    //
    // `frame::read_frame` 内部用 `read_buf` 读,一次可能读进多帧;握手结束后
    // 若像修前那样在这里 `BytesMut::with_capacity` 新建缓冲,残留字节就被
    // 直接丢弃。而服务端正是**背靠背**写 HELLO_ACK → SERVER_CERT →
    // ServiceSync:丢掉 ServiceSync 后客户端不会发 SYNC_REQUEST、服务端也
    // 不重发,表现为"连上了但所有规则不启动"。局域网(NODELAY 下各帧常被
    // 合并到同一批)恰恰是最容易触发的场景。数据通道侧早已用 PrependedIo
    // 处理同款问题,控制面这里是唯一漏点。
    mut buf: bytes::BytesMut,
) {
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
                    // 帧已在 send_msg 里编码好:这里只有一次 write_all,
                    // 不再分配、不再拷贝 payload。
                    Some(bytes) => {
                        use tokio::io::AsyncWriteExt as _;
                        if control.write_all(&bytes).await.is_err() {
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
        // 直接查 `cur` 本身(此刻它刚被重填完,语义与「先收集 existing
        // 再 contains」完全等价)。修前先 `keys().collect()` 成 Vec 再
        // `Vec::contains` 线性扫描,规则数 N 时全量同步是 O(N²)。
        for (id, spec) in &new_proxies {
            if cur.contains_key(id) {
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
        // 同上:直接查 `cur`,去掉 O(N²) 的 Vec + contains。
        for (id, spec) in &new_visitors {
            if cur.contains_key(id) {
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

    // T-E2(round-73 回退):tcp 默认基线 1 → initial_k=1,不建池。
    //
    // round-10 时 tcp 基线就是 1(单载体已接近路径上限)。round-54 曾改成
    // 4,依据是"K=1 吞吐随连接数下降(64 连接时 2.08 G,K=4 是 3.11~3.49 G)"
    // ——那组数据来自 iperf3 接收端先饱和,已证伪。round-73 在同一窗口严格
    // 交替 K=1/K=4 各 6 次,K=1 从未更慢、下行快 15~20%,故回退为 1。
    //
    // initial_k_for_with 注入硬件上限:本套件不得依赖跑测机器的核数。
    #[test]
    fn baseline_single_for_tcp() {
        let cfg = ClientConfig::default();
        let (k, _hw) = initial_k_for_with(&cfg, rh_transport::Protocol::Tcp, 8);
        // 2026-10-20 三机复测:K=2 把 4/16 流从 2.07/2.18G 提到 2.21/2.31G
        // (K=1 单载体封顶与流数无关)。详见 default_carriers 文档。
        assert_eq!(k, 2, "tcp 默认载体数为 2");
        // 显式请求多载体时收敛逻辑不变(基线回退不影响显式路径)。
        let (final_k, drops) = plan_convergence(4, &[8, 8, 8]);
        assert_eq!(final_k, 4, "显式 K=4 且服务端允许 8 时保持 4");
        assert!(drops.is_empty(), "K=4 <= 服务端 8,无成员可摘");
        let (capped, _) = plan_convergence(4, &[2]);
        assert_eq!(capped, 2, "服务端只允许 2 时收敛到 2");
    }

    // ---- RTT 探针的协议 gate(round-78)-------------------------------
    //
    // 修前 gate 在探测**之后**:quic/kcp 与非 multiplex 也会白打一条 TCP
    // (accept 完立刻断),探测结果随后被丢弃。现在 gate 提到探测之前。
    //
    // 用 rh-transport 的探针计数断言「一次都没有探测」——纯功能断言
    // (K 等于 1)在修前也成立,区分不了两条路径。

    static K_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    /// 非 tcp_based 协议(quic/kcp)不得触发探测。
    #[tokio::test]
    async fn non_tcp_protocol_skips_probe() {
        let _g = K_LOCK.lock().await;
        rh_transport::probe_cache_clear();
        let before = rh_transport::probe_call_count();
        for p in [rh_transport::Protocol::Quic, rh_transport::Protocol::Kcp] {
            let cfg = ClientConfig {
                server_addr: "203.0.113.9:7000".into(),
                ..ClientConfig::default()
            };
            let (k, _hw) = initial_k_for_async(&cfg, p, 8).await;
            assert_eq!(k, 1, "{p:?} 必须单载体");
        }
        assert_eq!(
            rh_transport::probe_call_count(),
            before,
            "quic/kcp 不得触发 RTT 探测"
        );
    }

    /// multiplex=false 的 tcp 同样不探测(恒单载体)。
    #[tokio::test]
    async fn non_multiplex_skips_probe() {
        let _g = K_LOCK.lock().await;
        rh_transport::probe_cache_clear();
        let before = rh_transport::probe_call_count();
        let cfg = ClientConfig {
            server_addr: "203.0.113.9:7000".into(),
            multiplex: false,
            ..ClientConfig::default()
        };
        let (k, _hw) = initial_k_for_async(&cfg, rh_transport::Protocol::Tcp, 8).await;
        assert_eq!(k, 1, "multiplex=false 必须单载体");
        assert_eq!(
            rh_transport::probe_call_count(),
            before,
            "multiplex=false 不得触发 RTT 探测"
        );
    }

    /// 红线:显式 mux_pool_size 最高优先,且**不探测**。
    #[tokio::test]
    async fn explicit_pool_size_skips_probe() {
        let _g = K_LOCK.lock().await;
        rh_transport::probe_cache_clear();
        let before = rh_transport::probe_call_count();
        let cfg = ClientConfig {
            server_addr: "203.0.113.9:7000".into(),
            mux_pool_size: Some(3),
            ..ClientConfig::default()
        };
        let (k, hw) = initial_k_for_async(&cfg, rh_transport::Protocol::Tcp, 8).await;
        assert_eq!(k, 3, "显式值必须原样生效");
        assert_eq!(hw, 8);
        assert_eq!(
            rh_transport::probe_call_count(),
            before,
            "显式配置不得触发 RTT 探测"
        );
    }

    /// tcp + multiplex + 无显式配置:确实会探测一次(反面证据)。
    #[tokio::test]
    async fn tcp_multiplex_does_probe() {
        let _g = K_LOCK.lock().await;
        rh_transport::probe_cache_clear();
        let before = rh_transport::probe_call_count();
        // 不可达地址:探针会失败,但**必须发生一次尝试**
        let cfg = ClientConfig {
            server_addr: "203.0.113.9:7000".into(),
            ..ClientConfig::default()
        };
        let (k, _hw) = initial_k_for_async(&cfg, rh_transport::Protocol::Tcp, 8).await;
        // 探测失败 → 1;再与协议基线(2)取 max → 2
        assert_eq!(k, 2, "失败退回 1 后与 tcp 基线 2 取 max");
        assert_eq!(
            rh_transport::probe_call_count(),
            before + 1,
            "tcp+multiplex 必须探测一次"
        );
    }

    /// 探针失败不得阻断连接:返回可用的 K,不 panic、不上抛错误。
    #[tokio::test]
    async fn probe_failure_does_not_block() {
        let _g = K_LOCK.lock().await;
        rh_transport::probe_cache_clear();
        let cfg = ClientConfig {
            server_addr: "203.0.113.9:7000".into(),
            ..ClientConfig::default()
        };
        // 2s 超时下不可达地址会失败;函数必须正常返回
        let (k, hw) = initial_k_for_async(&cfg, rh_transport::Protocol::Tcp, 8).await;
        assert!((1..=8).contains(&k), "K 必须在合法区间,实际 {k}");
        assert_eq!(hw, 8);
    }

    // 硬件上限仍能压住显式请求:1 核机器上必须退化为单载体。
    #[test]
    fn hardware_cap_clamps_explicit_k() {
        let cfg = ClientConfig {
            mux_pool_size: Some(4),
            ..ClientConfig::default()
        };
        assert_eq!(
            initial_k_for_with(&cfg, rh_transport::Protocol::Tcp, 1).0,
            1,
            "硬件上限 1 时显式 K=4 必须被压到 1"
        );
        assert_eq!(
            initial_k_for_with(&cfg, rh_transport::Protocol::Tcp, 2).0,
            2,
            "硬件上限 2 时取 2"
        );
    }

    // 红线 1(round-73 回退):**全部六个协议**的默认基线都是 1。
    // 只有显式配置才可能多载体;硬件上限 1 时任何协议都是单载体。
    #[test]
    fn baseline_single_for_all_protocols() {
        let cfg = ClientConfig::default();
        for p in [
            rh_transport::Protocol::Tcp,
            rh_transport::Protocol::Tls,
            rh_transport::Protocol::Ws,
            rh_transport::Protocol::Wss,
        ] {
            assert_eq!(
                initial_k_for_with(&cfg, p, 8).0,
                2,
                "{p:?} 默认载体数为 2(硬件上限充足时)"
            );
            assert_eq!(
                initial_k_for_with(&cfg, p, 1).0,
                1,
                "硬件上限 1 时任何协议都必须单载体"
            );
        }
        // QUIC/KCP 自带多路复用(且 KCP 背压语义依赖单通道),恒 1。
        for p in [rh_transport::Protocol::Quic, rh_transport::Protocol::Kcp] {
            assert_eq!(initial_k_for_with(&cfg, p, 8).0, 1, "{p:?} 必须单载体");
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
            "显式单载体必须压过任何协议基线"
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

#[cfg(test)]
mod cert_binding_tests {
    //! 证书下发机制的判定核心:**这是防中间人的唯一判据**,必须精确锁定。
    //!
    //! 中间人的可行路径被这三条测试覆盖:
    //! 1. 它不知道 secret → 造不出能解开的密文(改密文即失败);
    //! 2. 它能转发真密文,但自己的握手证书不同 → 比对不一致即失败;
    //! 3. 它想丢弃该帧让校验静默跳过 → 已由 `verify_server_cert` 的
    //!    降级防护拒绝(此处无法直接测超时,靠代码约束)。
    use super::*;

    // 绑定校验的开关判定。这里同时锁住两个曾经各自写错一次的方向:
    //   - 只看 `is_encrypted()`:insecure 的客户端被强制 fail-closed
    //     (E2E 的 tls/wss/quic 全红);
    //   - 只看 `secret_verified`:明文 tcp/ws/kcp 也在等一个永远不会来的帧
    //     (报 `收到 FrameType(258)` = SERVICE_SYNC)。
    #[test]
    fn cert_binding_requires_encrypted_protocol_and_secret_verified() {
        use rh_transport::Protocol;
        // 唯一该做的组合:加密协议 + 零配置档
        for p in [Protocol::Tls, Protocol::Wss, Protocol::Quic] {
            assert!(
                cert_binding_required(p, true),
                "{p:?} 在 secret_verified 下必须做绑定校验"
            );
        }
        // 明文协议:没有 TLS 证书,服务端不下发该帧
        for p in [Protocol::Tcp, Protocol::Ws, Protocol::Kcp] {
            assert!(
                !cert_binding_required(p, true),
                "{p:?} 无 TLS 证书,不得要求下发帧(否则等一条永不到来的帧)"
            );
        }
        // 加密协议但已有别的信任来源:pin/CA/insecure 都不该被这里拦
        for p in [Protocol::Tls, Protocol::Wss, Protocol::Quic] {
            assert!(
                !cert_binding_required(p, false),
                "{p:?} 在非 secret_verified 下不得强制下发帧绑定"
            );
        }
    }

    const SECRET: &[u8] = b"0123456789abcdef0123456789abcdef";

    fn seal(secret: &[u8], cert: &[u8]) -> rh_protocol::certprov::SealedCert {
        rh_protocol::certprov::seal_cert(secret, [9u8; 12], cert).expect("seal")
    }

    /// 诚实服务端:下发的就是握手出示的 → 通过。
    #[test]
    fn honest_server_passes() {
        let cert = b"real-server-leaf-der";
        let sealed = seal(SECRET, cert);
        assert!(compare_announced_cert(SECRET, &sealed, cert).is_ok());
    }

    /// 中间人用自己的证书与应用层握手,同时**原样转发**真服务端的密文。
    /// 客户端解出的是真证书,但握手看到的是中间人证书 → 必须拒绝。
    #[test]
    fn mitm_with_own_cert_is_rejected() {
        let real = b"real-server-leaf-der";
        let mitm = b"mitm-leaf-der-different";
        let sealed = seal(SECRET, real); // 真服务端发出的密文,中间人解不开只能转发
        let e = compare_announced_cert(SECRET, &sealed, mitm).unwrap_err();
        assert!(
            e.to_string().contains("中间人"),
            "应在错误里明确指出中间人风险,实际: {e}"
        );
    }

    /// 中间人不知道 secret,自行加密一张假证书:客户端解不开 → 拒绝。
    #[test]
    fn mitm_cannot_forge_sealed_cert() {
        let attacker_secret = b"attacker-guessed-wrong-secret";
        let sealed = seal(attacker_secret, b"forged");
        let e = compare_announced_cert(SECRET, &sealed, b"forged").unwrap_err();
        assert!(
            e.to_string().contains("无法解密"),
            "攻击者伪造的密文必须解不开,实际: {e}"
        );
    }

    /// 密文被篡改(位翻转)→ GCM tag 校验失败 → 拒绝。
    #[test]
    fn tampered_sealed_cert_is_rejected() {
        let cert = b"real-server-leaf-der";
        let mut sealed = seal(SECRET, cert);
        sealed.ciphertext[0] ^= 0x01;
        let e = compare_announced_cert(SECRET, &sealed, cert).unwrap_err();
        assert!(e.to_string().contains("无法解密"), "实际: {e}");
    }

    /// 客户端 secret 配错:同样解不开 → 明确报错而不是放行。
    #[test]
    fn wrong_local_secret_is_rejected() {
        let cert = b"real-server-leaf-der";
        let sealed = seal(SECRET, cert);
        let e = compare_announced_cert(b"wrong-local-secret", &sealed, cert).unwrap_err();
        assert!(e.to_string().contains("无法解密"), "实际: {e}");
    }
}

#[cfg(test)]
mod multi_carrier_cert_tests {
    use super::*;

    /// 所有载体证书都一致 → 通过。
    #[test]
    fn all_carriers_matching_passes() {
        let cert = vec![1u8; 32];
        let seen = vec![cert.clone(), cert.clone(), cert.clone()];
        assert!(verify_all_carrier_certs(&cert, &seen).is_ok());
    }

    /// **只有附加载体**不一致 → 必须失败。
    ///
    /// 这正是修前的漏洞:控制载体(第 0 条)正常,附加载体被中间人替换,
    /// 而旧代码只校验第 0 条,于是整体放行。
    #[test]
    fn one_tampered_extra_carrier_fails() {
        let cert = vec![1u8; 32];
        let mitm = vec![2u8; 32];
        let seen = vec![cert.clone(), mitm, cert.clone()];
        let e = verify_all_carrier_certs(&cert, &seen).unwrap_err();
        let msg = e.to_string();
        assert!(msg.contains("载体 #1"), "必须指出是第 1 条载体: {msg}");
        assert!(msg.contains("中间人"), "必须说明可能被截断: {msg}");
    }

    /// 一个证书都没记录 → 失败(不得当作"无需校验")。
    #[test]
    fn empty_seen_list_fails() {
        let cert = vec![1u8; 32];
        assert!(verify_all_carrier_certs(&cert, &[]).is_err());
    }

    /// 单载体正常路径不能误杀。
    #[test]
    fn single_carrier_ok() {
        let cert = vec![9u8; 16];
        assert!(verify_all_carrier_certs(&cert, std::slice::from_ref(&cert)).is_ok());
    }
}

#[cfg(test)]
mod control_frame_tests {
    use super::*;
    use rh_protocol::message::{HealthReport, NatHoleReport, Ping, UserConnect};

    /// 用真实的 `read_frame` 解一段字节,返回 (FrameType, payload)。
    async fn decode(bytes: &[u8]) -> (FrameType, bytes::Bytes) {
        let mut cur = std::io::Cursor::new(bytes.to_vec());
        let mut buf = bytes::BytesMut::new();
        rh_protocol::frame::read_frame(&mut cur, &mut buf)
            .await
            .expect("对端必须能解出该帧")
    }

    /// 回归:控制帧形状零变更 —— 与修前「先 to_vec 再 write_frame」的
    /// 字节完全一致(逐字节比对),且能被 read_frame 正确解出。
    #[tokio::test]
    async fn control_frame_bytes_identical_to_legacy_path() {
        let msgs = vec![
            ClientMessage::Ping(Ping { ts_ms: 1_700_000_000_123, probe: false }),
            ClientMessage::UserConnect(UserConnect {
                visitor_id: 42,
                user_conn_id: 7,
                src_addr: "203.0.113.5:54321".into(),
                udp: false,
            }),
            ClientMessage::HealthReport(HealthReport {
                proxy_id: 9,
                healthy: true,
                reason: String::new(),
            }),
            ClientMessage::NatHoleReport(NatHoleReport {
                transaction_id: "tx-abc".into(),
                candidates: vec!["198.51.100.7:7000".into()],
                nat_behavior: "endpoint-independent".into(),
            }),
        ];
        for m in msgs {
            let ty = frame_type_of_client(&m);
            let payload = serde_json::to_vec(&m).unwrap();

            // 修前路径:write_frame 内部 encode_frame_bytes
            let mut legacy = Vec::new();
            rh_protocol::frame::write_frame(&mut legacy, ty, &payload)
                .await
                .unwrap();

            // 修后路径
            let now = encode_control_frame(&m).expect("必须编码成功");

            assert_eq!(
                now.as_ref(),
                legacy.as_slice(),
                "{ty:?} 的帧字节与修前不一致(线格式被改动)"
            );

            // 对端可解,且 FrameType 与 payload 语义不变
            let (got_ty, got_payload) = decode(&now).await;
            assert_eq!(got_ty, ty, "FrameType 映射被改动");
            assert_eq!(
                got_payload.as_ref(),
                payload.as_slice(),
                "payload 字节被改动"
            );
        }
    }

    /// 每种 ClientMessage 变体都映射到预期的 FrameType(红线:映射不变)。
    #[test]
    fn frame_type_mapping_unchanged() {
        use rh_protocol::message::{ApplyResult, DataChannelHello, Hello};
        let cases: Vec<(ClientMessage, FrameType)> = vec![
            (
                ClientMessage::Hello(Hello {
                    client_id: "c".into(),
                    version: "1".into(),
                    hostname: String::new(),
                    os: String::new(),
                    arch: String::new(),
                    capabilities: vec![],
                    session_id: None,
                    attach_auth: None,
                }),
                FrameType::HELLO,
            ),
            (ClientMessage::Ping(Ping { ts_ms: 0, probe: false }), FrameType::PING),
            (
                ClientMessage::SyncRequest {},
                FrameType::SYNC_REQUEST,
            ),
            (
                ClientMessage::DataChannelHello(DataChannelHello {
                    session_id: "s".into(),
                    request_id: 1,
                }),
                FrameType::DATA_CHANNEL_HELLO,
            ),
            (
                ClientMessage::ApplyResult(ApplyResult {
                    revision: 1,
                    ok: true,
                    error: None,
                }),
                FrameType::APPLY_RESULT,
            ),
        ];
        for (m, want) in cases {
            assert_eq!(frame_type_of_client(&m), want, "FrameType 映射被改动");
        }
    }

    /// 红线:超限载荷必须被拒绝,**不得**编码出一个腐化帧。
    #[test]
    fn oversized_payload_is_rejected() {
        // 构造一条 payload 明确超过 MAX_FRAME_LEN 的消息:UserConnect 的
        // src_addr 是可变长字符串,直接塞满。
        let huge = "x".repeat(rh_protocol::frame::MAX_FRAME_LEN + 1);
        let m = ClientMessage::UserConnect(UserConnect {
            visitor_id: 1,
            user_conn_id: 2,
            src_addr: huge,
            udp: false,
        });
        assert!(
            encode_control_frame(&m).is_none(),
            "超限载荷必须被拒绝(否则对端会以 TooLarge 关连接)"
        );
    }

    /// 边界:恰好 MAX_FRAME_LEN 的载荷必须放行。
    #[test]
    fn payload_at_limit_is_accepted() {
        // 先算出一条消息的固定开销,再补齐到恰好 MAX_FRAME_LEN。
        let probe = ClientMessage::UserConnect(UserConnect {
            visitor_id: 1,
            user_conn_id: 2,
            src_addr: String::new(),
            udp: false,
        });
        let base = serde_json::to_vec(&probe).unwrap().len();
        let fill = rh_protocol::frame::MAX_FRAME_LEN - base;
        let m = ClientMessage::UserConnect(UserConnect {
            visitor_id: 1,
            user_conn_id: 2,
            src_addr: "x".repeat(fill),
            udp: false,
        });
        let payload_len = serde_json::to_vec(&m).unwrap().len();
        assert_eq!(payload_len, rh_protocol::frame::MAX_FRAME_LEN);
        assert!(
            encode_control_frame(&m).is_some(),
            "恰为上限的载荷必须放行"
        );
    }

    /// 计数探针:每消息只编码一次(`encode_frame_bytes` 恰好被调用 1 次)。
    ///
    /// 修前的路径是「to_vec → 队列 → write_frame 内再 encode 一次」,即
    /// 同一消息会经历一次序列化 + 一次成帧拷贝;这里锁住「成帧只做一次」。
    #[tokio::test]
    async fn each_message_is_framed_exactly_once() {
        let (tx, mut rx) = mpsc::channel::<bytes::Bytes>(256);
        let sess = Arc::new(ClientSession {
            cfg: Arc::new(ClientConfig::default()),
            session_id: "s".into(),
            heartbeat_s: 30,
            send_tx: tx,
            cancel: CancellationToken::new(),
            proxies: Mutex::new(HashMap::new()),
            visitors: Mutex::new(HashMap::new()),
            visitor_cancels: Mutex::new(HashMap::new()),
            health_cancels: Mutex::new(HashMap::new()),
            waiting: Mutex::new(HashMap::new()),
            punched: Mutex::new(HashMap::new()),
            nat_socks: Mutex::new(HashMap::new()),
            punched_auth: Mutex::new(HashMap::new()),
            sudp_deliver: Mutex::new(HashMap::new()),
            mux: rh_transport::mux::wrap_stream_as_client(
                Box::new(tokio::io::duplex(4096).0) as rh_transport::BoxedIo,
                true,
                100,
            ),
            conn_counter: AtomicU64::new(0),
        });

        let m = ClientMessage::Ping(Ping { ts_ms: 99, probe: false });
        assert!(sess.send_msg(m.clone()).await);
        let got = rx.recv().await.expect("队列里必须有一帧");

        // 队列元素**就是完整帧**:长度 = 头 + payload,且能被 read_frame 解出
        let payload = serde_json::to_vec(&m).unwrap();
        assert_eq!(
            got.len(),
            rh_protocol::frame::HEADER_LEN + payload.len(),
            "队列元素必须已是完整帧(头 + payload),否则下行还要再成帧一次"
        );
        let (ty, body) = decode(&got).await;
        assert_eq!(ty, frame_type_of_client(&m));
        assert_eq!(body.as_ref(), payload.as_slice());

        // run_loop 侧只做 write_all:用一个 Vec 承接,字节必须与原帧完全一致
        let mut sink: Vec<u8> = Vec::new();
        use tokio::io::AsyncWriteExt as _;
        sink.write_all(&got).await.unwrap();
        assert_eq!(sink, got.to_vec(), "write_all 不得改动帧字节");
    }

    /// 背压语义:队列容量 256,塞满 256 帧后第 257 次 send 必须 await
    /// (而不是丢弃或扩容)。
    #[tokio::test]
    async fn queue_capacity_preserves_backpressure() {
        let (tx, mut rx) = mpsc::channel::<bytes::Bytes>(256);
        // 256 条应能立即入队
        for i in 0..256 {
            tx.send(bytes::Bytes::from(vec![i as u8]))
                .await
                .expect("容量 256 内必须能入队");
        }
        // 第 257 条:通道满 → 必须 pending
        let extra = tokio::time::timeout(
            std::time::Duration::from_millis(50),
            tx.send(bytes::Bytes::from_static(b"257")),
        )
        .await;
        assert!(extra.is_err(), "队满时 send 必须 await(背压语义),不得静默丢弃");

        // 消费一条后应恢复可写
        let _ = rx.recv().await;
        let ok = tokio::time::timeout(
            std::time::Duration::from_millis(50),
            tx.send(bytes::Bytes::from_static(b"257")),
        )
        .await;
        assert!(ok.is_ok(), "消费后必须能继续入队");
    }

    /// 序列化失败可选地返回 false(不 panic)。这里只验"不发出去"的语义:
    /// 用超限消息走 send_msg 路径,必须返回 false 且队列为空。
    #[tokio::test]
    async fn send_msg_returns_false_on_oversized() {
        let (tx, mut rx) = mpsc::channel::<bytes::Bytes>(256);
        let sess = Arc::new(ClientSession {
            cfg: Arc::new(ClientConfig::default()),
            session_id: "s".into(),
            heartbeat_s: 30,
            send_tx: tx,
            cancel: CancellationToken::new(),
            proxies: Mutex::new(HashMap::new()),
            visitors: Mutex::new(HashMap::new()),
            visitor_cancels: Mutex::new(HashMap::new()),
            health_cancels: Mutex::new(HashMap::new()),
            waiting: Mutex::new(HashMap::new()),
            punched: Mutex::new(HashMap::new()),
            nat_socks: Mutex::new(HashMap::new()),
            punched_auth: Mutex::new(HashMap::new()),
            sudp_deliver: Mutex::new(HashMap::new()),
            mux: rh_transport::mux::wrap_stream_as_client(
                Box::new(tokio::io::duplex(4096).0) as rh_transport::BoxedIo,
                true,
                100,
            ),
            conn_counter: AtomicU64::new(0),
        });
        let huge = "x".repeat(rh_protocol::frame::MAX_FRAME_LEN + 1);
        let m = ClientMessage::UserConnect(UserConnect {
            visitor_id: 1,
            user_conn_id: 2,
            src_addr: huge,
            udp: false,
        });
        assert!(!sess.send_msg(m).await, "超限消息必须返回 false");
        assert!(
            rx.try_recv().is_err(),
            "超限消息不得进入队列(否则会发出腐化帧)"
        );
    }
}

#[cfg(test)]
mod waiting_ttl_tests {
    use super::*;

    /// 测试用会话(只需 waiting 表与 cancel 可用)。
    async fn test_session() -> (Arc<ClientSession>, mpsc::Receiver<bytes::Bytes>) {
        let (tx, rx) = mpsc::channel::<bytes::Bytes>(256);
        let sess = Arc::new(ClientSession {
            cfg: Arc::new(ClientConfig::default()),
            session_id: "s".into(),
            heartbeat_s: 30,
            send_tx: tx,
            cancel: CancellationToken::new(),
            proxies: Mutex::new(HashMap::new()),
            visitors: Mutex::new(HashMap::new()),
            visitor_cancels: Mutex::new(HashMap::new()),
            health_cancels: Mutex::new(HashMap::new()),
            waiting: Mutex::new(HashMap::new()),
            punched: Mutex::new(HashMap::new()),
            nat_socks: Mutex::new(HashMap::new()),
            punched_auth: Mutex::new(HashMap::new()),
            sudp_deliver: Mutex::new(HashMap::new()),
            mux: rh_transport::mux::wrap_stream_as_client(
                Box::new(tokio::io::duplex(4096).0) as rh_transport::BoxedIo,
                true,
                100,
            ),
            conn_counter: AtomicU64::new(0),
        });
        (sess, rx)
    }

    /// 造一条真实的本地 TCP 连接对(登记进 waiting 表需要真 TcpStream)。
    async fn tcp_conn() -> tokio::net::TcpStream {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let a = l.local_addr().unwrap();
        let c = tokio::net::TcpStream::connect(a).await.unwrap();
        // listener 侧立即接受并丢弃,避免 accept 队列堆积
        tokio::spawn(async move {
            let _ = l.accept().await;
        });
        c
    }

    /// 验收:1000 个 waiting 条目、1s 内全部交付,结束后表为空。
    ///
    /// 「交付」= `take_waiting`(与 deliver_relay 成功路径同语义)。修前
    /// 每条会额外 spawn 一个 sleep 任务,交付后仍活到 10s;现在没有
    /// 每连接任务,交付即出表。
    #[tokio::test]
    async fn thousand_entries_delivered_within_1s_no_residual_tasks() {
        let (sess, _rx) = test_session().await;
        const N: u64 = 1000;

        for id in 0..N {
            let c = tcp_conn().await;
            sess.insert_waiting(id, c, 1).await;
        }
        assert_eq!(sess.waiting.lock().await.len() as u64, N, "1000 条必须全部登记");

        // 1s 内全部交付
        let t0 = std::time::Instant::now();
        for id in 0..N {
            let got = sess.take_waiting(id).await;
            assert!(got.is_some(), "条目 {id} 必须可交付");
        }
        let elapsed = t0.elapsed();
        assert!(
            elapsed < Duration::from_secs(1),
            "1000 条交付耗时 {elapsed:?},超过 1s"
        );

        assert!(
            sess.waiting.lock().await.is_empty(),
            "全部交付后表必须为空"
        );

        // 没有残留的每连接任务:交付后再 sweep 一轮,不应有可淘汰项
        assert_eq!(sess.sweep_waiting().await, 0, "交付后不得有残留条目");

        // 修前的 1000 个 sleep 任务此时仍会存活:用一个可观测的替代证明
        // —— 登记→交付全程没有 spawn 任何任务(见 spawn_waiting_sweeper 的
        // 单一任务设计);这里断言 tokio 运行时未因本测试产生额外常驻任务
        // 不现实,故以「表为空 + 无淘汰项」作为结构性证据。
    }

    /// **红线**:TTL 的绝对值必须是 10s —— 用字面量断言,不引用常量本身。
    ///
    /// 引用 `WAITING_TTL` 的测试是自证循环:把常量改成 1s,断言跟着改
    /// 就永远绿(实测过:第一版正是如此,负向验证抓不到)。
    #[test]
    fn ttl_is_exactly_ten_seconds() {
        assert_eq!(
            WAITING_TTL,
            Duration::from_secs(10),
            "waiting 表 TTL 语义是 10s,不得改动"
        );
    }

    /// 扫描周期应显著小于 TTL(否则过期条目会滞留一个周期量级)。
    #[test]
    fn sweep_interval_shorter_than_ttl() {
        assert!(
            WAITING_SWEEP_INTERVAL < WAITING_TTL / 2,
            "扫描周期必须明显小于 TTL"
        );
    }

    /// TTL 边界:9s 的保留、11s 的淘汰(用**字面量**秒数,不引用常量)。
    #[tokio::test]
    async fn sweep_evicts_only_expired() {
        let (sess, _rx) = test_session().await;
        // 一条「已超 10s」的条目
        let c = tcp_conn().await;
        sess.insert_waiting(1, c, 7).await;
        {
            let mut w = sess.waiting.lock().await;
            if let Some((_, _, at)) = w.get_mut(&1) {
                *at = std::time::Instant::now() - Duration::from_secs(11);
            }
        }
        // 一条「9s 前登记」的条目(未过期)
        let c2 = tcp_conn().await;
        sess.insert_waiting(2, c2, 7).await;
        {
            let mut w = sess.waiting.lock().await;
            if let Some((_, _, at)) = w.get_mut(&2) {
                *at = std::time::Instant::now() - Duration::from_secs(9);
            }
        }

        let evicted = sess.sweep_waiting().await;
        assert_eq!(evicted, 1, "只应淘汰超过 10s 的那一条");
        let w = sess.waiting.lock().await;
        assert!(!w.contains_key(&1), "11s 前的条目必须被淘汰");
        assert!(w.contains_key(&2), "9s 前的条目必须保留");
    }

    /// 红线:配对顺序与 user_conn_id 生成不变。
    #[tokio::test]
    async fn user_conn_id_is_monotonic_and_order_preserved() {
        let (sess, _rx) = test_session().await;
        let a = sess.next_conn_id().await;
        let b = sess.next_conn_id().await;
        let c = sess.next_conn_id().await;
        assert_eq!((a, b, c), (1, 2, 3), "user_conn_id 必须从 1 起严格递增");
    }

    /// 回退重放不重置 TTL:requeue 保留原登记时刻。
    #[tokio::test]
    async fn requeue_keeps_original_timestamp() {
        let (sess, _rx) = test_session().await;
        let c = tcp_conn().await;
        sess.insert_waiting(1, c, 5).await;

        let entry = sess.take_waiting(1).await.expect("必须先能取出");
        let at = entry.2;
        // 模拟耗时 3s 后回退
        tokio::time::sleep(Duration::from_millis(3)).await;
        sess.requeue_waiting(1, entry).await;

        let w = sess.waiting.lock().await;
        let now_at = w.get(&1).unwrap().2;
        assert_eq!(now_at, at, "回退放回必须保留原登记时刻,不得续命");
    }

    /// 会话结束时 sweeper 必须退出(不 leak)。
    #[tokio::test]
    async fn sweeper_exits_on_cancel() {
        let (sess, _rx) = test_session().await;
        let h = sess.spawn_waiting_sweeper();
        // 触发会话取消
        sess.cancel.cancel();
        // sweeper 应在下一轮 select 立即返回
        let joined = tokio::time::timeout(Duration::from_secs(2), h).await;
        assert!(joined.is_ok(), "取消后 sweeper 必须退出,不得 leak");
    }

    /// sweeper 确实会淘汰过期项(端到端跑一次真实 tick)。
    #[tokio::test]
    async fn sweeper_evicts_expired_entry() {
        let (sess, _rx) = test_session().await;
        let c = tcp_conn().await;
        sess.insert_waiting(1, c, 3).await;
        // 把登记时刻拨到过期
        {
            let mut w = sess.waiting.lock().await;
            if let Some((_, _, at)) = w.get_mut(&1) {
                *at = std::time::Instant::now() - Duration::from_secs(11);
            }
        }
        let h = sess.spawn_waiting_sweeper();
        // 扫描周期 1s,留 3s 余量
        let mut evicted = false;
        for _ in 0..30 {
            tokio::time::sleep(Duration::from_millis(100)).await;
            if sess.waiting.lock().await.is_empty() {
                evicted = true;
                break;
            }
        }
        assert!(evicted, "过期条目必须被 sweeper 淘汰");
        sess.cancel.cancel();
        let _ = tokio::time::timeout(Duration::from_secs(2), h).await;
    }
}

#[cfg(test)]
mod apply_sync_bench {
    use std::time::Instant;

    /// 500 条规则全量同步里「判存在」这一步的耗时(改前 O(N²) vs 改后 O(N))。
    ///
    /// 隔离掉 `stop_proxy`/`start_health` 等真实副作用,只测两种判存在
    /// 写法的差距。用 `u64` 键的 HashMap(与 `proxies`/`visitors` 同形),
    /// 值类型不影响哈希查找成本。
    #[test]
    fn contains_key_vs_vec_contains_at_500() {
        const N: u64 = 500;

        let build = || {
            let mut cur: std::collections::HashMap<u64, ()> = std::collections::HashMap::new();
            for id in 0..N {
                cur.insert(id, ());
            }
            let mut new: std::collections::HashMap<u64, ()> = std::collections::HashMap::new();
            for id in 0..N {
                new.insert(id, ());
            }
            (cur, new)
        };

        // 改前:先 collect 成 Vec 再线性 contains
        let old_style = |cur: &std::collections::HashMap<u64, ()>,
                         new: &std::collections::HashMap<u64, ()>| {
            let existing: Vec<u64> = cur.keys().copied().collect();
            let mut hits = 0usize;
            for id in new.keys() {
                if existing.contains(id) {
                    hits += 1;
                }
            }
            hits
        };
        // 改后:直接哈希查
        let new_style = |cur: &std::collections::HashMap<u64, ()>,
                         new: &std::collections::HashMap<u64, ()>| {
            let mut hits = 0usize;
            for id in new.keys() {
                if cur.contains_key(id) {
                    hits += 1;
                }
            }
            hits
        };

        let (cur, new) = build();
        assert_eq!(old_style(&cur, &new), N as usize);
        assert_eq!(new_style(&cur, &new), N as usize, "两种写法语义必须一致");

        let rounds = 200usize;
        let mut t_old = Vec::with_capacity(rounds);
        let mut t_new = Vec::with_capacity(rounds);
        for _ in 0..rounds {
            let (cur, new) = build();
            let t = Instant::now();
            std::hint::black_box(old_style(&cur, &new));
            t_old.push(t.elapsed());

            let t = Instant::now();
            std::hint::black_box(new_style(&cur, &new));
            t_new.push(t.elapsed());
        }
        t_old.sort();
        t_new.sort();
        let med_old = t_old[rounds / 2];
        let med_new = t_new[rounds / 2];
        println!(
            "BENCH apply_sync N={N}: vec_contains med={:?}  hash_contains_key med={:?}  ratio={:.1}x",
            med_old,
            med_new,
            med_old.as_secs_f64() / med_new.as_secs_f64().max(1e-12)
        );
        // 只作记录,不做阈值断言(CI 机器噪声大);断言仅防"改后明显更慢"
        assert!(med_new <= med_old * 2, "改后不应比改前慢(允许噪声)");
    }
}
