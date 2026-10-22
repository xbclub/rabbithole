//! AppState:全局运行态(会话注册表、代理运行时、端口分配、组、vhost 路由)
//! 与每客户端串行域。

use crate::config::ServerConfig;
use crate::db::Db;
use crate::events::EventBus;
use crate::session::{Session, SessionRegistry};
use rh_core::net::{bind_conflicts, PortAllowList, PortsRange};
use rh_protocol::{
    message::{ChannelEntity, CreateDataChannel, ServerMessage, SyncEntity},
    ProxySpec,
};
use rh_transport::BoxedIo;

/// 控制面内联路径的发送上限。
///
/// 与 control.rs 的 `CONTROL_REPLY_TIMEOUT` 同量级:正常下发是微秒级,
/// 卡满 2s 说明对端已不读控制流,继续等只会拖死控制读循环(自锁)。
const CONTROL_SEND_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);
use std::collections::{HashMap, VecDeque};
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64};
use std::sync::Arc;
use std::time::Instant;
use tokio::net::{TcpListener, UdpSocket};
use tokio::sync::{Mutex, RwLock};
use tokio_util::sync::CancellationToken;

/// 中继配对表:((会话 id, request_id) → 通道交付 oneshot)。
pub type RelayPairMap = HashMap<(String, u64), tokio::sync::oneshot::Sender<BoxedIo>>;

pub struct AppState {
    pub cfg: Arc<ServerConfig>,
    pub db: Db,
    pub events: EventBus,
    pub challenge: Arc<crate::control::ChallengeTable>,
    pub sessions: Arc<SessionRegistry>,
    pub proxies: Arc<ProxyManager>,
    pub client_locks: Arc<dashmap::DashMap<String, Arc<Mutex<()>>>>,
    pub metrics: Arc<Metrics>,
    pub shutdown: CancellationToken,
    /// 中继配对表。键含会话 id:request_id 仅在会话内单调,两个会话各自的 rid=1 会撞号。
    pub relay_pairs: Arc<Mutex<RelayPairMap>>,
    /// 活跃 SUDP 常驻中继(visitor_id 集合)。
    pub udp_relays: Arc<Mutex<std::collections::HashSet<u64>>>,
    /// XTCP 打洞协调器。
    pub nathole: Arc<crate::nathole::NatCoordinator>,
    /// C7:登录限速第一级 —— 每 IP(防用户名轮换绕过)。
    pub login_limiter: Arc<governor::DefaultKeyedRateLimiter<String>>,
    /// C7:登录限速第二级 —— 每 IP+用户名。
    pub login_limiter_user: Arc<governor::DefaultKeyedRateLimiter<String>>,
    /// JWT 密钥(启动时定;空则启动生成随机值)。
    pub web_jwt_secret: Arc<Mutex<Option<String>>>,
    /// 每 IP 认证中并发连接数(控制面未认证窗口)。
    unauth_conns: Arc<std::sync::Mutex<std::collections::HashMap<IpAddr, usize>>>,
    /// 热生效配置子集(网页可管:端口策略/日志级别/SSH 网关)。
    pub hot: Arc<RwLock<HotConfig>>,
    /// C1:visitor 解析缓存((client_id, visitor_id) → 解析结果)。键内含
    /// 配置版本,版本前进即视为失效。条目在插拔时按容量淘汰。
    ///
    /// ★键必须含 **visitor_id**。修前只按 client_id 存单条,而注释自己就写着
    /// "同一会话可能有多个访问器":第二个 visitor 的连接会命中第一条缓存,
    /// 随后 `resolve.visitor_id != u.visitor_id` 直接 return —— 无日志丢弃,
    /// 表现为"同客户端的多个访问器只有一个能用"。
    pub visitor_cache: dashmap::DashMap<(String, u64), std::sync::Arc<VisitorResolution>>,
    /// C1:每个客户端的本地配置版本(任何规则/访问器变更 +1)。比 DB 的
    /// config_revision 更廉价:纯内存原子,不需要每次解析都读 DB。
    pub client_cfg_gen: Arc<dashmap::DashMap<String, u64>>,
    /// 热生效 limits(控制台可改):并发/限流上限的运行时唯一真源。
    /// 同步锁:enter_unauth 等非 async 路径也要读。
    pub limits: Arc<std::sync::RwLock<crate::config::LimitsConfig>>,
    /// 复用流上限有效值(limits.max_mux_streams 折算,0 → 不限制),
    /// transport accept 泵每连接读取 → 热改对新会话生效。
    pub mux_streams: Arc<std::sync::atomic::AtomicUsize>,
    /// F2 每客户端用量/配额缓存。
    pub client_usage: Arc<dashmap::DashMap<String, Arc<ClientUsage>>>,
    /// M3 服务端终结证书(SNI 匹配;certs 表变更后整体重建)。
    pub certs: Arc<RwLock<Vec<Arc<CertEntry>>>>,
    /// SSH 网关配置变更通知(supervisor 据此热启停监听)。
    pub ssh_watch: (
        tokio::sync::watch::Sender<u64>,
        Arc<tokio::sync::watch::Receiver<u64>>,
    ),
    /// Prometheus 独立监听任务(热开关启停;None = 未监听)。
    pub metrics_task: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
    /// metrics 实际在用的监听地址(metrics_listen_start 绑定成功时记录,stop 清除)。
    /// 2007/3005 自身占用归属只看这里,不读配置文件/hot 里的 metrics 字段——
    /// 热改(含回滚)后配置值可能与实际监听不一致。
    pub metrics_listen_addr: Arc<std::sync::Mutex<Option<SocketAddr>>>,
}

/// 2007/3005 自身监听占用命中(诊断提示用)。
pub struct SelfPortHit {
    /// 来源说明(如 "Web 管理后台"、"控制面主监听(kcp)")。
    pub label: String,
    /// 该监听的配置地址或(metrics)实际监听地址。
    pub addr: SocketAddr,
    /// true = 来自实际在跑的监听(metrics 绑定成功时记录);
    /// false = 静态配置保留口径,不冒充实时进程检测。
    pub live: bool,
}

/// 网页可管且即时生效的配置子集;其余字段改动标记需重启。
#[derive(Debug, Clone)]
pub struct HotConfig {
    pub allow_ports: Vec<String>,
    pub max_services_per_client: usize,
    pub log_level: String,
    pub ssh_gateway: crate::config::SshGatewayConfig,
    /// Prometheus 指标端点(独立监听;地址/端口均可热改,保存时热重启监听器)。
    pub metrics: crate::config::MetricsConfig,
}

impl HotConfig {
    pub fn from_cfg(cfg: &ServerConfig) -> Self {
        Self {
            allow_ports: cfg.ports.allow_ports.clone(),
            max_services_per_client: cfg.ports.max_services_per_client,
            log_level: cfg.log.level.clone(),
            ssh_gateway: cfg.ssh_gateway.clone(),
            metrics: cfg.metrics.clone(),
        }
    }

    pub fn port_allow_list(&self) -> PortAllowList {
        PortAllowList {
            ranges: self
                .allow_ports
                .iter()
                .filter_map(|s| PortsRange::parse(s))
                .collect(),
        }
    }
}

/// M3 服务端终结证书(预构建 rustls 配置,按 SNI 精确/通配匹配)。
pub struct CertEntry {
    pub domains: Vec<String>,
    pub cfg: Arc<rustls::ServerConfig>,
}

impl CertEntry {
    /// 域名匹配:精确或单层通配(*.example.com 覆盖 sub.example.com)。
    pub fn matches(&self, sni: &str) -> bool {
        let sni = sni.to_lowercase();
        self.domains.iter().any(|d| {
            let d = d.to_lowercase();
            if let Some(suffix) = d.strip_prefix("*.") {
                sni.ends_with(&format!(".{suffix}"))
                    && sni.len() > suffix.len() + 1
                    && !sni[..sni.len() - suffix.len() - 1].contains('.')
            } else {
                d == sni
            }
        })
    }
}

/// F2 每客户端配额(DB 为准,内存缓存;API 改动即刷新)。
#[derive(Debug, Clone, Default)]
pub struct ClientQuota {
    pub rate_limit: String,
    pub max_conns: i64,
    pub flow_limit: i64,
}

/// 每客户端运行用量(进程内存量,重启归零)。
pub struct ClientUsage {
    pub conns: AtomicI64,
    pub bytes: AtomicU64,
    /// 流量上限击穿标记(置位后相关连接在下一轮 IO 即关)。
    pub flow_cut: AtomicBool,
    pub quota: std::sync::RwLock<ClientQuota>,
    /// flow_limit 的原子缓存(配额写入时同步):数据面每块字节都要读,
    /// 避免 RwLock 热读;与 quota RwLock 由 set_quota 写侧保持一致。
    flow_limit_cache: AtomicI64,
    /// 服务端侧客户端级限速桶(随 rate_limit 重建)。
    pub rate_bucket: std::sync::RwLock<Option<Arc<rh_core::TokenBucket>>>,
}

impl Default for ClientUsage {
    fn default() -> Self {
        Self {
            conns: AtomicI64::new(0),
            bytes: AtomicU64::new(0),
            flow_cut: AtomicBool::new(false),
            quota: std::sync::RwLock::new(ClientQuota::default()),
            flow_limit_cache: AtomicI64::new(0),
            rate_bucket: std::sync::RwLock::new(None),
        }
    }
}

impl ClientUsage {
    /// 应用新配额;rate_limit 变化时重建限速桶。
    pub fn set_quota(&self, q: ClientQuota) {
        let mut cur = self.quota.write().unwrap();
        if cur.rate_limit != q.rate_limit {
            let bucket = rh_core::BandwidthQuantity::parse(&q.rate_limit)
                .map(|b| Arc::new(rh_core::TokenBucket::new(b.bytes_per_sec() as f64)));
            *self.rate_bucket.write().unwrap() = bucket;
        }
        // ★`flow_cut` 只在"新上限尚未被超出"时才解除。
        //
        // 修前只有 `flow_limit == 0`(改成不限)才清标志:管理员把 1GB 调到
        // 2GB 这种"加量"操作后,已置位的 flow_cut 会**永久**保持,所有连接
        // 继续被杀(客户端侧 check flow_cut 直接 continue),只能重启进程
        // 或先改 0 再改回来。改成按实际用量判定:加量到超过已用量即恢复。
        let limit_unreached =
            q.flow_limit == 0 || self.bytes.load(std::sync::atomic::Ordering::Relaxed) < q.flow_limit as u64;
        if limit_unreached {
            self.flow_cut
                .store(false, std::sync::atomic::Ordering::Relaxed);
        }
        self.flow_limit_cache
            .store(q.flow_limit, std::sync::atomic::Ordering::Relaxed);
        *cur = q;
    }

    /// 字节计量 + 流量上限判定。
    pub fn add_bytes(&self, n: u64) {
        let total = self
            .bytes
            .fetch_add(n, std::sync::atomic::Ordering::Relaxed)
            + n;
        let limit = self
            .flow_limit_cache
            .load(std::sync::atomic::Ordering::Relaxed);
        if limit > 0 && total >= limit as u64 {
            self.flow_cut
                .store(true, std::sync::atomic::Ordering::Relaxed);
        }
    }

    /// 服务端侧限速桶(无配置返回 None)。
    pub fn bucket(&self) -> Option<Arc<rh_core::TokenBucket>> {
        self.rate_bucket.read().unwrap().clone()
    }
}

impl Clone for AppState {
    fn clone(&self) -> Self {
        Self {
            cfg: self.cfg.clone(),
            db: self.db.clone(),
            events: self.events.clone(),
            challenge: self.challenge.clone(),
            sessions: self.sessions.clone(),
            proxies: self.proxies.clone(),
            client_locks: self.client_locks.clone(),
            metrics: self.metrics.clone(),
            shutdown: self.shutdown.clone(),
            hot: self.hot.clone(),
            limits: self.limits.clone(),
            mux_streams: self.mux_streams.clone(),
            relay_pairs: self.relay_pairs.clone(),
            udp_relays: self.udp_relays.clone(),
            nathole: self.nathole.clone(),
            login_limiter: self.login_limiter.clone(),
            login_limiter_user: self.login_limiter_user.clone(),
            web_jwt_secret: self.web_jwt_secret.clone(),
            unauth_conns: self.unauth_conns.clone(),
            client_usage: self.client_usage.clone(),
            certs: self.certs.clone(),
            ssh_watch: (self.ssh_watch.0.clone(), self.ssh_watch.1.clone()),
            metrics_task: self.metrics_task.clone(),
            metrics_listen_addr: self.metrics_listen_addr.clone(),
            visitor_cache: self.visitor_cache.clone(),
            client_cfg_gen: self.client_cfg_gen.clone(),
        }
    }
}

impl AppState {
    /// C1:推进客户端本地配置版本(规则/访问器任何变更后调用)。
    pub fn bump_cfg_gen(&self, client_id: &str) {
        let mut e = self
            .client_cfg_gen
            .entry(client_id.to_string())
            .or_insert(0);
        *e = e.wrapping_add(1);
        // 键现在是 (client_id, visitor_id),按 client_id 前缀逐条摘除
        self.visitor_cache.retain(|(cid, _), _| cid != client_id);
    }

    /// C1:读取客户端本地配置版本(0 = 从未变更)。
    pub fn cfg_gen(&self, client_id: &str) -> u64 {
        self.client_cfg_gen
            .get(client_id)
            .map(|e| *e.value())
            .unwrap_or(0)
    }

    /// 取(或建)客户端用量条目。
    pub fn client_usage(&self, id: &str) -> Arc<ClientUsage> {
        self.client_usage
            .entry(id.to_string())
            .or_insert_with(|| Arc::new(ClientUsage::default()))
            .clone()
    }

    /// M3:从 certs 表重建服务端终结证书集(启动与证书 CRUD 后调用)。
    /// 解析失败的行跳过并告警(不阻塞其余证书)。
    pub async fn rebuild_certs(&self) {
        let mut out = Vec::new();
        if let Ok(rows) = self.db.list_certs().await {
            for c in rows {
                match crate::vhost::build_cert_entry(&c.cert_pem, &c.key_pem) {
                    Ok((domains, fingerprint, not_after, cfg)) => {
                        let mut doms: Vec<String> =
                            serde_json::from_str(&c.domains).unwrap_or_default();
                        if doms.is_empty() {
                            doms = domains;
                        }
                        let _ = (fingerprint, not_after); // 由 /api/certs 从 DB 读取展示
                        out.push(Arc::new(CertEntry {
                            domains: doms,
                            cfg: Arc::new(cfg),
                        }));
                    }
                    Err(e) => {
                        tracing::warn!(cert = %c.name, error = %e, "certs: skip bad pem");
                    }
                }
            }
        }
        *self.certs.write().await = out;
    }

    /// 从 DB 刷新全部客户端配额缓存(启动与客户端 CRUD 后调用)。
    pub async fn refresh_client_quotas(&self) {
        if let Ok(rows) = self.db.list_clients().await {
            let live: std::collections::HashSet<&str> =
                rows.iter().map(|c| c.id.as_str()).collect();
            for c in &rows {
                self.client_usage(&c.id).set_quota(ClientQuota {
                    rate_limit: c.rate_limit.clone(),
                    max_conns: c.max_conns,
                    flow_limit: c.flow_limit,
                });
            }
            // 清理已删客户端的残留条目
            self.client_usage.retain(|k, _| live.contains(k.as_str()));
        }
    }

    pub fn new(cfg: ServerConfig, db: Db) -> Self {
        let events = EventBus::new(db.clone());
        let metrics = Arc::new(Metrics::new());
        let cfg = Arc::new(cfg);
        let hot = Arc::new(RwLock::new(HotConfig::from_cfg(&cfg)));
        let limits = Arc::new(std::sync::RwLock::new(cfg.limits.clone()));
        let mux_streams = Arc::new(std::sync::atomic::AtomicUsize::new(
            cfg.limits.mux_streams(),
        ));
        let proxies = Arc::new(ProxyManager::new(cfg.clone(), hot.clone()));
        let (ssh_tx, ssh_rx) = tokio::sync::watch::channel(0u64);
        let jwt_secret = cfg
            .web
            .jwt_secret
            .clone()
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

        Self {
            challenge: Arc::new(crate::control::ChallengeTable::new()),
            cfg,
            db,
            events,
            // round-80:在线数 gauge 由注册表在每次成员变更时直接上报。
            // 修前 gauge 只在拆除路径写、上线路径从不写,导致
            // `/metrics` 的 clients_online 恒为 0。
            sessions: Arc::new(SessionRegistry::with_online_gauge({
                let g = metrics.client_online.clone();
                Arc::new(move |n: usize| g.set(n as i64))
            })),
            proxies,
            client_locks: Arc::new(dashmap::DashMap::new()),
            metrics,
            shutdown: CancellationToken::new(),
            hot,
            limits,
            mux_streams,
            client_usage: Arc::new(dashmap::DashMap::new()),
            certs: Arc::new(RwLock::new(Vec::new())),
            ssh_watch: (ssh_tx, Arc::new(ssh_rx)),
            metrics_task: Arc::new(Mutex::new(None)),
            metrics_listen_addr: Arc::new(std::sync::Mutex::new(None)),
            relay_pairs: Arc::new(Mutex::new(HashMap::new())),
            udp_relays: Arc::new(Mutex::new(std::collections::HashSet::new())),
            nathole: Arc::new(crate::nathole::NatCoordinator::new()),
            // 每 IP 30 次/分钟(宽桶,容纳办公室 NAT 后多人同时输错)
            login_limiter: Arc::new(governor::RateLimiter::keyed(governor::Quota::per_minute(
                std::num::NonZeroU32::new(30).unwrap(),
            ))),
            // 每 IP+用户名 5 次/分钟(窄桶,原有语义)
            login_limiter_user: Arc::new(governor::RateLimiter::keyed(
                governor::Quota::per_minute(std::num::NonZeroU32::new(5).unwrap()),
            )),
            web_jwt_secret: Arc::new(Mutex::new(Some(jwt_secret))),
            unauth_conns: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
            visitor_cache: dashmap::DashMap::new(),
            client_cfg_gen: Arc::new(dashmap::DashMap::new()),
        }
    }

    /// 启动 Prometheus 独立监听(幂等)。地址由调用方传入(热更新时取自 HotConfig,
    /// 启动时取自启动配置),本方法不读 hot——避免与持有 hot 写锁的调用方互锁。
    /// 绑定失败返回错误,由调用方决定是否落配置。
    pub async fn metrics_listen_start(
        self: &Arc<Self>,
        m: &crate::config::MetricsConfig,
    ) -> anyhow::Result<()> {
        let mut guard = self.metrics_task.lock().await;
        if guard.is_some() {
            return Ok(());
        }
        let addr: SocketAddr = format!("{}:{}", m.bind_addr, m.port)
            .parse()
            .map_err(|e| anyhow::anyhow!("metrics bind_addr/port 非法:{e}"))?;
        let listener = TcpListener::bind(addr)
            .await
            .map_err(|e| anyhow::anyhow!("metrics 端口监听失败 {addr}:{e}"))?;
        // 绑定成功才记录实际监听地址(2007/3005 自身占用归属用)
        *self.metrics_listen_addr.lock().unwrap() = Some(addr);
        tracing::info!(%addr, "prometheus metrics up");
        let app = axum::Router::new()
            .route(
                "/metrics",
                axum::routing::get(crate::api::prometheus_metrics),
            )
            .with_state(self.clone());
        *guard = Some(tokio::spawn(async move {
            if let Err(e) = axum::serve(listener, app).await {
                tracing::warn!(error = %e, "prometheus metrics serve stopped");
            }
        }));
        Ok(())
    }

    /// 停止 Prometheus 独立监听(幂等)。abort 后等待任务真正退出,
    /// 确保监听 fd 已释放再返回(否则立即按新/旧地址重绑会 Address already in use)。
    pub async fn metrics_listen_stop(&self) {
        if let Some(t) = self.metrics_task.lock().await.take() {
            t.abort();
            let _ = t.await;
            tracing::info!("prometheus metrics down");
        }
        // 无论任务是否在跑都清除记录,避免残留陈旧地址
        *self.metrics_listen_addr.lock().unwrap() = None;
    }

    /// Prometheus 独立监听是否在运行。
    ///
    /// ★除了"持有任务句柄"还要判 `is_finished()`:修前只看 `is_some()`,
    /// 任务异常退出(panic / socket 错误)后句柄仍在,状态会**永远报 true**
    /// —— 控制台显示 metrics 在跑,实际端点已死。
    pub async fn metrics_listening(&self) -> bool {
        let g = self.metrics_task.lock().await;
        g.as_ref().is_some_and(|h| !h.is_finished())
    }

    /// 2007/3005 端口占用归属分类:按协议族(tcp/udp)+端口+IP 重叠
    /// (复用 rh_core::net::bind_conflicts 口径,通配/具体双向)匹配本进程
    /// 自身配置/在用的监听。命中返回来源说明;未命中视为外部进程占用
    /// (调用方保持通用文案),OS 试绑仍是最终裁定。
    ///
    /// 覆盖范围只含实际启用的字段:控制面主监听/附加监听(bind_port>0 且协议
    /// 可解析)、Web 管理后台、vhost http/https/tcpmux(0=关闭)、SSH 网关
    /// (热配置 enabled)、metrics(实际监听地址,热改即时反映)。
    /// 未启用的字段不构成占用;合法的规则组复用/异地址不在此判定(由
    /// port_in_use 与试绑负责),不会误报。
    pub async fn classify_port_occupation(
        &self,
        proto: &str,
        ip: IpAddr,
        port: u16,
    ) -> Option<SelfPortHit> {
        if port == 0 {
            return None;
        }
        // (标签, 协议族, 监听 IP, 端口, 是否实际在跑的监听)
        let mut cand: Vec<(String, &'static str, IpAddr, u16, bool)> = Vec::new();
        let any_v4 = IpAddr::from([0, 0, 0, 0]);
        let cfg = &self.cfg;
        let control_ip = rh_core::net::parse_bind_ip(&cfg.bind_addr).unwrap_or(any_v4);
        // 控制面主监听 + 附加传输监听(tcp/tls/ws/wss=TCP 族;quic/kcp=UDP 族)
        if cfg.bind_port > 0 {
            if let Some(p) = rh_transport::Protocol::parse(&cfg.bind_protocol) {
                let family = if p.is_tcp_based() { "tcp" } else { "udp" };
                cand.push((
                    format!("控制面主监听({p})"),
                    family,
                    control_ip,
                    cfg.bind_port,
                    false,
                ));
            }
        }
        for l in &cfg.extra_listeners {
            if l.bind_port == 0 {
                continue;
            }
            let Some(p) = rh_transport::Protocol::parse(&l.protocol) else {
                continue;
            };
            let lip = l
                .bind_addr
                .as_deref()
                .and_then(rh_core::net::parse_bind_ip)
                .unwrap_or(control_ip);
            let family = if p.is_tcp_based() { "tcp" } else { "udp" };
            cand.push((
                format!("附加传输监听({p})"),
                family,
                lip,
                l.bind_port,
                false,
            ));
        }
        // Web 管理后台(启动即绑定;bind_addr 缺省 127.0.0.1)
        if cfg.web.port > 0 {
            let wip = rh_core::net::parse_bind_ip(&cfg.web.bind_addr)
                .unwrap_or(IpAddr::from([127, 0, 0, 1]));
            cand.push(("Web 管理后台".into(), "tcp", wip, cfg.web.port, false));
        }
        // vhost 共享监听(与控制面同 bind_addr;端口 0 = 关闭,不占用)
        if cfg.vhost.http_port > 0 {
            cand.push((
                "vhost HTTP 共享监听".into(),
                "tcp",
                control_ip,
                cfg.vhost.http_port,
                false,
            ));
        }
        if cfg.vhost.https_port > 0 {
            cand.push((
                "vhost HTTPS(SNI) 共享监听".into(),
                "tcp",
                control_ip,
                cfg.vhost.https_port,
                false,
            ));
        }
        if cfg.vhost.tcpmux_port > 0 {
            cand.push((
                "vhost tcpmux 共享监听".into(),
                "tcp",
                control_ip,
                cfg.vhost.tcpmux_port,
                false,
            ));
        }
        // SSH 网关(热配置;enabled 才占用,bind_addr 空 = 0.0.0.0)
        {
            let ssh = self.hot.read().await.ssh_gateway.clone();
            if ssh.enabled && ssh.bind_port > 0 {
                let sip = rh_core::net::parse_bind_ip(&ssh.bind_addr).unwrap_or(any_v4);
                cand.push(("SSH 网关".into(), "tcp", sip, ssh.bind_port, false));
            }
        }
        // metrics:实际监听地址(metrics_listen_start 成功时记录;
        // 热改/回滚即时反映,不读配置文件或 hot 的 metrics 字段)
        let maddr = *self.metrics_listen_addr.lock().unwrap();
        if let Some(maddr) = maddr {
            cand.push((
                "Prometheus 指标监听".into(),
                "tcp",
                maddr.ip(),
                maddr.port(),
                true,
            ));
        }
        cand.into_iter()
            .find(|(_, family, cip, cport, _)| {
                bind_conflicts(proto, ip, port, family, *cip, *cport)
            })
            .map(|(label, _, cip, cport, live)| SelfPortHit {
                label,
                addr: SocketAddr::new(cip, cport),
                live,
            })
    }

    /// 认证窗口进入:超过每 IP 上限返回 false(0 = 不限)。
    pub fn enter_unauth(&self, ip: &IpAddr) -> bool {
        let limit = self.limits.read().unwrap().unauth_conns_per_ip;
        if limit == 0 {
            return true;
        }
        let mut g = self.unauth_conns.lock().unwrap();
        let c = g.entry(*ip).or_insert(0);
        if *c >= limit {
            return false;
        }
        *c += 1;
        true
    }

    /// 认证窗口退出。
    pub fn exit_unauth(&self, ip: &IpAddr) {
        let mut g = self.unauth_conns.lock().unwrap();
        if let Some(c) = g.get_mut(ip) {
            *c = c.saturating_sub(1);
            if *c == 0 {
                g.remove(ip);
            }
        }
    }

    /// A3:未认证名额的 RAII 守卫。
    ///
    /// 名额本是"认证窗口"的并发上限,但修前 `exit_unauth` 放在
    /// `auth_and_run(...).await` **之后**,而该函数以 `run_session_loop`
    /// 结尾 —— 于是一个已认证的会话会把名额**终身占住**。同一 NAT 出口
    /// 第 65 个客户端永远收到 `too many pending auth`,而 config 注释声称
    /// "不限制已认证业务并发",实现与之相反。
    ///
    /// 用守卫把配对关系交给 Drop:认证成功点显式 `disarm()`(名额立即归还),
    /// 其余任何早退路径(读失败、认证失败、版本不符…)由 Drop 归还。
    /// 手写 enter/exit 在两处 twenty 多条 return 路径上极易漏。
    pub fn enter_unauth_guard(self: &Arc<Self>, ip: IpAddr) -> Option<UnauthGuard> {
        if !self.enter_unauth(&ip) {
            return None;
        }
        Some(UnauthGuard {
            state: self.clone(),
            ip,
            armed: true,
        })
    }

    /// 每客户端串行域锁(创建/更新/启停/恢复共用)。
    pub fn serial_lock(&self, client_id: &str) -> Arc<Mutex<()>> {
        self.client_locks
            .entry(client_id.to_string())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }
}

// ---------------- 指标 ----------------

pub struct Metrics {
    pub registry: prometheus::Registry,
    pub connections: prometheus::IntGaugeVec,
    pub client_online: prometheus::IntGauge,
    /// F1 全局字节计量(用户侧口径,含请求/响应头)。
    pub bytes_in_total: prometheus::IntCounter,
    pub bytes_out_total: prometheus::IntCounter,
    /// goal-repair B1/B7 可观测性:数据面路径选择计数
    /// (label path = chunk | splice | copy;命中/回落诊断)。
    pub relay_path_total: prometheus::IntCounterVec,
    /// c84 G5 1/N 事件归因(candidate-r0 diagnostics README:36-40):
    /// 会话级失败计数——建立阶段静默丢弃(健康摘除/通道不可得/配额拒绝/
    /// START_FORWARD 写失败)与 copy 路径 relay 异常终止的唯一常驻留痕。
    /// label reason 为固定小集合(见 data.rs 调用点),无逐连接标签;
    /// 正常 EOF 与规则取消不计,避免淹没 1/N 信号。
    pub session_failures_total: prometheus::IntCounterVec,
    /// A1:控制回包队列满时的丢弃计数。
    ///
    /// 心跳回执(PONG)绝不能阻塞控制读循环 —— 那个循环同时是队列的**唯一
    /// 消费者**,一旦它 park 在 `send().await` 上等自己消费,就是自锁:
    /// 队列永满、select 不再回顶、心跳超时与 `sess.cancel` 全部失联。
    /// 故 PONG 改为 `try_send`,满即丢(丢 PONG 只让客户端那一次探测超时,
    /// 下一拍自愈;远比整会话卡死轻)。
    pub dropped_pong: prometheus::IntCounter,
    /// B7:UDP 会话表满导致的拒收计数(每个被拒新源地址 +1)。
    pub udp_sessions_rejected: prometheus::IntCounter,
    /// 汇总(控制台 overview)。原子化:数据面每块字节都要经过这里,
    /// 全局互斥锁曾是所有转发连接的串行点。
    pub conns_current: std::sync::atomic::AtomicI64,
    pub console_bytes_in: std::sync::atomic::AtomicU64,
    pub console_bytes_out: std::sync::atomic::AtomicU64,
}

#[derive(Default, Clone, Copy)]
pub struct Totals {
    pub conns_current: i64,
    pub bytes_in: u64,
    pub bytes_out: u64,
}

impl Metrics {
    pub fn new() -> Self {
        use prometheus::{self, IntCounter, IntGauge, IntGaugeVec, Opts, Registry};
        let registry = Registry::new();
        let connections = IntGaugeVec::new(
            Opts::new("rabbithole_connections", "active forwarded connections"),
            &["proxy", "kind"],
        )
        .unwrap();
        let client_online = IntGauge::new("rabbithole_clients_online", "online clients").unwrap();
        let bytes_in_total =
            IntCounter::new("rabbithole_bytes_in_total", "bytes received from users").unwrap();
        let bytes_out_total =
            IntCounter::new("rabbithole_bytes_out_total", "bytes sent to users").unwrap();
        let relay_path_total = prometheus::IntCounterVec::new(
            Opts::new(
                "rabbithole_relay_path_total",
                "data-plane relay path selection (chunk/splice/copy)",
            ),
            &["path"],
        )
        .unwrap();
        let session_failures_total = prometheus::IntCounterVec::new(
            Opts::new(
                "rabbithole_session_failures_total",
                "per-session failures (establishment drop / relay abnormal end)",
            ),
            &["reason"],
        )
        .unwrap();
        let _ = registry.register(Box::new(connections.clone()));
        let _ = registry.register(Box::new(client_online.clone()));
        let _ = registry.register(Box::new(bytes_in_total.clone()));
        let _ = registry.register(Box::new(bytes_out_total.clone()));
        let _ = registry.register(Box::new(relay_path_total.clone()));
        let dropped_pong = IntCounter::new(
            "rabbithole_dropped_pong_total",
            "PONG replies dropped because the control reply queue was full (A1 self-lock guard)",
        )
        .unwrap();
        let udp_sessions_rejected = IntCounter::new(
            "rabbithole_udp_sessions_rejected_total",
            "new UDP sources dropped because the per-proxy session table is full (B7)",
        )
        .unwrap();
        let _ = registry.register(Box::new(session_failures_total.clone()));
        let _ = registry.register(Box::new(dropped_pong.clone()));
        let _ = registry.register(Box::new(udp_sessions_rejected.clone()));
        Self {
            registry,
            connections,
            client_online,
            bytes_in_total,
            bytes_out_total,
            relay_path_total,
            session_failures_total,
            dropped_pong,
            udp_sessions_rejected,
            conns_current: std::sync::atomic::AtomicI64::new(0),
            console_bytes_in: std::sync::atomic::AtomicU64::new(0),
            console_bytes_out: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// 控制台 overview 快照。
    pub fn totals_snapshot(&self) -> Totals {
        use std::sync::atomic::Ordering;
        Totals {
            conns_current: self.conns_current.load(Ordering::Relaxed),
            bytes_in: self.console_bytes_in.load(Ordering::Relaxed),
            bytes_out: self.console_bytes_out.load(Ordering::Relaxed),
        }
    }

    /// 数据面路径选择计数(chunk/splice/copy;goal-repair B1/B7 诊断)。
    pub fn count_relay_path(&self, path: &str) {
        self.relay_path_total.with_label_values(&[path]).inc();
    }

    /// 会话级失败计数(c84 G5 1/N 归因):建立失败/relay 异常终止各调用点
    /// 自带单条 debug!() 明细,这里是常驻 /metrics 信号(reason 固定小集合)。
    pub fn count_session_failure(&self, reason: &str) {
        self.session_failures_total
            .with_label_values(&[reason])
            .inc();
    }

    /// 用户→服务端方向 n 字节(入站):totals 供控制台,counter 供 /metrics。
    pub fn count_user_in(&self, n: usize) {
        if n == 0 {
            return;
        }
        self.bytes_in_total.inc_by(n as u64);
        self.console_bytes_in
            .fetch_add(n as u64, std::sync::atomic::Ordering::Relaxed);
    }

    /// 服务端→用户方向 n 字节(出站)。
    pub fn count_user_out(&self, n: usize) {
        if n == 0 {
            return;
        }
        self.bytes_out_total.inc_by(n as u64);
        self.console_bytes_out
            .fetch_add(n as u64, std::sync::atomic::Ordering::Relaxed);
    }
}

// ---------------- 代理运行时 ----------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProxyState {
    Disabled,
    BindError,
    Offline,
    Pending,
    Running,
    ApplyError,
    Unhealthy,
}

impl ProxyState {
    pub fn as_str(&self) -> &'static str {
        match self {
            ProxyState::Disabled => "disabled",
            ProxyState::BindError => "bind_error",
            ProxyState::Offline => "offline",
            ProxyState::Pending => "pending",
            ProxyState::Running => "running",
            ProxyState::ApplyError => "apply_error",
            ProxyState::Unhealthy => "unhealthy",
        }
    }
}

/// TCP 数据通道池(每代理)。
pub struct TcpPool {
    /// 就绪通道(已完成 DATA_CHANNEL_HELLO,等待 START_FORWARD)。
    /// 元组:(建立该通道的会话 epoch, request_id, io)
    ///
    /// round-34:`Vec` → `VecDeque`,取用从 `pop()`(LIFO)改为
    /// `pop_front()`(FIFO)。修前每次取走的是**最后**建好的那条通道,
    /// 而"最后建好"的那条往往正是刚刚预热完成、尚未被任何流量验证过的;
    /// 先建好的通道则长期沉底,直到过期才被驱逐。FIFO 让每条通道都被
    /// 均匀使用一次,连接年龄分布稳定、不易出现"陈旧通道被突然启用"。
    ///
    /// 注:池内每条通道都是一条独立 yamux 流,单条流的在途字节数在服务端
    /// 这一侧不可观测(流是透明的 `BoxedIo`,不带 carrier 归属),故本轮
    /// 采用任务给出的 FIFO 选项,而非按在途字节做最小负载选择。
    pub ready: Mutex<VecDeque<(u64, u64, BoxedIo)>>,
    /// 等待用户连接的等待者(用户连接先到)。带唯一 id:超时按 id 精确移除,
    /// 不得误删其他等待者(LIFO pop 会删错对象)。
    pub waiters: Mutex<Vec<(u64, tokio::sync::oneshot::Sender<BoxedIo>)>>,
    /// 池容量(池中+在途 ≤ pool_size)。
    pub size: usize,
    /// 在途请求数。
    pub inflight: AtomicU64,
    /// 池当前归属会话的 epoch(全局单调递增;0 = 尚未认领)。
    /// 会话重连/替换后旧条目全部失效,由认领方驱逐。
    pub epoch: AtomicU64,
    /// 等待者 id 发号器。
    pub waiter_seq: AtomicU64,
}

impl TcpPool {
    pub fn new(size: usize) -> Self {
        Self {
            ready: Mutex::new(VecDeque::new()),
            waiters: Mutex::new(Vec::new()),
            size,
            inflight: AtomicU64::new(0),
            epoch: AtomicU64::new(0),
            waiter_seq: AtomicU64::new(1),
        }
    }

    /// 移除指定 id 的等待者(超时/签发失败路径)。
    pub async fn remove_waiter(&self, id: u64) {
        self.waiters.lock().await.retain(|(wid, _)| *wid != id);
    }
}

pub struct ProxyRuntime {
    pub id: u64,
    pub client_id: String,
    pub spec: RwLock<ProxySpec>,
    pub state: RwLock<ProxyState>,
    pub state_reason: RwLock<String>,
    pub confirmed_revision: AtomicU64,
    pub latest_revision: AtomicU64,
    pub healthy: AtomicBool,
    /// 独享端口类规则:listener 绑定成功标记(running 的必要条件,§4.1)。
    pub bound: AtomicBool,
    pub rule_cancel: CancellationToken,
    /// tcp/tcpmux(独享或组内成员):Listener 由本运行时或 Group 持有。
    pub tcp_listener: Mutex<Option<TcpListener>>,
    /// 本规则的独享 serve loop 句柄(A6 顺带项)。
    ///
    /// teardown→spawn 重建同一端口时,旧 serve loop 可能还没把监听 fd 释放,
    /// 新绑定就撞 `EADDRINUSE`(api.rs 拆完立刻重建的路径实测偶发)。
    /// 重建前带超时 await 它收敛,消除这个窗口。
    pub serve_handle: Mutex<Option<tokio::task::JoinHandle<()>>>,
    pub udp_socket: Mutex<Option<Arc<UdpSocket>>>,
    pub pool: Mutex<Option<Arc<TcpPool>>>,
    /// UDP pump 通道投递。
    pub udp_new_channel_tx: Mutex<Option<tokio::sync::mpsc::Sender<BoxedIo>>>,
    /// F1 分规则计量(用户侧口径;进程内存量,重启归零)。
    pub bytes_in: AtomicU64,
    pub bytes_out: AtomicU64,
    pub conns_current: AtomicI64,
    /// M2 HTTP 静态缓存(仅 http 规则使用)。
    pub http_cache: std::sync::Mutex<HttpCache>,
    /// 本规则是否属于负载均衡分组(无锁快路径)。
    ///
    /// `group_pick()` 每请求都要读 `spec.read().await` 才能拿到 group_key,
    /// 而绝大多数规则**没有分组**(group 为空),那次 async 读锁纯属浪费,
    /// 且在高并发下是明确的争用点。这里用 AtomicBool 做无锁预判:
    /// 非分组规则直接跳过整个 group_pick。
    /// 与 spec 由同一写入点(runtime::publish_spec_in_place)同步维护。
    pub has_group: AtomicBool,
    /// HTTP vhost 上游连接池(仅 http/https 规则使用)。
    ///
    /// 修前每个 HTTP 请求都 `hyper::client::conn::http1::handshake` 新建
    /// 一份完整 HTTP/1.1 状态机 + spawn 一个 conn 任务,请求结束即丢弃 ——
    /// 池化数据通道只被复用一次。单机实测(16 连接 keep-alive):
    ///   直连源站 41602 QPS / 纯 TCP 转发 10187 QPS / HTTP vhost 4262 QPS。
    /// vhost 只有直连的 10%,而纯 TCP 转发有 24% —— 主要损失在 vhost 这层。
    pub http_upstream: crate::vhost::UpstreamPool,
}

impl ProxyRuntime {
    pub fn new(id: u64, client_id: String, spec: ProxySpec) -> Self {
        // 从 spec 初始化无锁快路径标记:无论从 API / recovery / 测试哪条
        // 路径创建,标记都与 spec 一致(不依赖调用方记得设置)。
        let has_group = ProxyManager::group_key(&spec).is_some();
        Self {
            id,
            client_id,
            spec: RwLock::new(spec),
            state: RwLock::new(ProxyState::Offline),
            state_reason: RwLock::new(String::new()),
            confirmed_revision: AtomicU64::new(0),
            latest_revision: AtomicU64::new(0),
            healthy: AtomicBool::new(true),
            bound: AtomicBool::new(false),
            rule_cancel: CancellationToken::new(),
            bytes_in: AtomicU64::new(0),
            bytes_out: AtomicU64::new(0),
            conns_current: AtomicI64::new(0),
            http_cache: std::sync::Mutex::new(HttpCache::default()),
            has_group: AtomicBool::new(has_group),
            http_upstream: crate::vhost::UpstreamPool::default(),
            tcp_listener: Mutex::new(None),
            serve_handle: Mutex::new(None),
            udp_socket: Mutex::new(None),
            pool: Mutex::new(None),
            udp_new_channel_tx: Mutex::new(None),
        }
    }

    pub async fn set_state(&self, s: ProxyState, reason: &str) {
        let mut st = self.state.write().await;
        let mut r = self.state_reason.write().await;
        tracing::debug!(proxy = self.id, state = s.as_str(), reason, "proxy state");
        *st = s;
        *r = reason.to_string();
    }

    pub async fn snapshot_state(&self) -> (ProxyState, String) {
        (
            *self.state.read().await,
            self.state_reason.read().await.clone(),
        )
    }
}

/// C1:一次 USER_CONNECT 的解析结果(visitor + 目标规则)。
pub struct VisitorResolution {
    /// 生成该结果时的客户端配置版本。
    pub gen: u64,
    pub visitor_id: u64,
    pub target_client: String,
    pub vspec: rh_protocol::VisitorSpec,
    pub pspec: ProxySpec,
}

/// 组负载均衡:同 (kind, bind_ip, bind_port, group) 的成员共享监听。
/// TCP 组:首个绑定成功的成员启动共享 serve loop(取消权归组,不随单个成员退出);
/// vhost 组(http/https/tcpmux):仅登记成员供 group_pick 轮询。
pub struct GroupRuntime {
    pub members: RwLock<Vec<u64>>,
    pub rr: AtomicU64,
    /// 共享监听是否已在位(TCP 组)。
    pub listener_bound: AtomicBool,
    /// 共享 serve loop 的取消令牌(组清空时取消;重新绑定时更换新令牌)。
    pub serve_cancel: RwLock<CancellationToken>,
}

impl GroupRuntime {
    fn new() -> Self {
        Self {
            members: RwLock::new(Vec::new()),
            rr: AtomicU64::new(0),
            listener_bound: AtomicBool::new(false),
            serve_cancel: RwLock::new(CancellationToken::new()),
        }
    }
}

// ---------------- ProxyManager ----------------

pub struct ProxyManager {
    pub cfg: Arc<ServerConfig>,
    pub runtimes: dashmap::DashMap<u64, Arc<ProxyRuntime>>,
    /// 组运行时
    pub groups: dashmap::DashMap<String, Arc<GroupRuntime>>,
    /// vhost HTTP 路由表:domain/host → proxy_id
    // C3:路由表用 Arc 快照 —— 请求路径只 clone 一次 Arc(O(1)),
    // 修前每请求 `read().await.clone()` 深拷贝整张表(含全部 String/Vec)。
    pub vhost_http: RwLock<Arc<Vec<VhostRoute>>>,
    pub vhost_https: RwLock<Arc<Vec<VhostRoute>>>,
    pub vhost_tcpmux: RwLock<Arc<Vec<VhostRoute>>>,
    /// 热生效端口策略(网页可改,与 AppState.hot 同源)。
    pub hot: Arc<RwLock<HotConfig>>,
}

#[derive(Clone)]
pub struct VhostRoute {
    pub host: String,
    pub locations: Vec<String>,
    pub http_user: String,
    pub http_password: String,
    pub host_rewrite: String,
    pub request_headers: rh_protocol::HeaderOp,
    pub response_headers: rh_protocol::HeaderOp,
    pub proxy_id: u64,
    /// M2:域名命中但路径未命中 locations 时的自定义 404。
    pub not_found_html: String,
    /// M2:静态缓存配置(仅 http 路由)。
    pub cache: Option<rh_protocol::CacheConfig>,
}

/// M2 HTTP 静态缓存(每规则一份;std::Mutex 短临界区,不跨 await)。
#[derive(Clone)]
pub struct HttpCacheEntry {
    pub expires: std::time::Instant,
    pub status: u16,
    pub content_type: String,
    pub body: std::sync::Arc<[u8]>,
}

#[derive(Default)]
pub struct HttpCache {
    pub map: std::collections::HashMap<String, HttpCacheEntry>,
    pub bytes: usize,
}

impl HttpCache {
    pub fn get(&mut self, key: &str) -> Option<HttpCacheEntry> {
        let expired = self
            .map
            .get(key)
            .map(|e| e.expires < std::time::Instant::now())
            .unwrap_or(true);
        if expired {
            if let Some(e) = self.map.remove(key) {
                self.bytes = self.bytes.saturating_sub(e.body.len());
            }
            return None;
        }
        self.map.get(key).cloned()
    }

    /// 插入(容量不足时先清过期再逐出任意项);超过单项上限或总容量则放弃。
    pub fn put(&mut self, key: String, entry: HttpCacheEntry, max_bytes: usize) {
        if self.map.contains_key(&key) {
            return;
        }
        let len = entry.body.len();
        if len > max_bytes {
            return;
        }
        let now = std::time::Instant::now();
        self.map.retain(|_, e| {
            if e.expires < now {
                self.bytes = self.bytes.saturating_sub(e.body.len());
                return false;
            }
            true
        });
        while self.bytes + len > max_bytes {
            let k = self.map.keys().next().cloned();
            match k {
                Some(k) => {
                    if let Some(e) = self.map.remove(&k) {
                        self.bytes = self.bytes.saturating_sub(e.body.len());
                    }
                }
                None => break,
            }
        }
        self.bytes += len;
        self.map.insert(key, entry);
    }
}

impl ProxyManager {
    pub fn new(cfg: Arc<ServerConfig>, hot: Arc<RwLock<HotConfig>>) -> Self {
        Self {
            cfg,
            hot,
            runtimes: dashmap::DashMap::new(),
            groups: dashmap::DashMap::new(),
            vhost_http: RwLock::new(Arc::new(Vec::new())),
            vhost_https: RwLock::new(Arc::new(Vec::new())),
            vhost_tcpmux: RwLock::new(Arc::new(Vec::new())),
        }
    }

    pub fn get(&self, id: u64) -> Option<Arc<ProxyRuntime>> {
        self.runtimes.get(&id).map(|e| e.value().clone())
    }

    /// 取(或创建)组运行时。
    pub fn group_entry(&self, key: &str) -> Arc<GroupRuntime> {
        self.groups
            .entry(key.to_string())
            .or_insert_with(|| Arc::new(GroupRuntime::new()))
            .clone()
    }

    /// 组共享监听是否已在位(TCP 组,判定创建/更新时是否可跳过试绑)。
    pub fn group_listener_bound(&self, key: &Option<String>) -> bool {
        key.as_ref().is_some_and(|k| {
            self.groups
                .get(k)
                .is_some_and(|g| g.listener_bound.load(std::sync::atomic::Ordering::Relaxed))
        })
    }

    /// 端口占用冲突判定(运行中的独享监听 + 组监听 + DB 其他启用行)。
    /// new_group_key:新建/更新规则的组键;同组成员共享监听,不构成冲突。
    pub async fn port_in_use(
        &self,
        proto: &str,
        ip: IpAddr,
        port: u16,
        except_proxy: Option<u64>,
        new_group_key: Option<String>,
    ) -> bool {
        for e in self.runtimes.iter() {
            let rt = e.value();
            if Some(rt.id) == except_proxy {
                continue;
            }
            let spec = rt.spec.read().await;
            if Some(rt.id) != except_proxy
                && new_group_key.is_some()
                && Self::group_key(&spec) == new_group_key
            {
                continue; // 同组:共享监听
            }
            if spec.kind.uses_bind_port()
                && spec.enabled
                && bind_conflicts(
                    proto,
                    ip,
                    port,
                    proto,
                    rh_core::net::parse_bind_ip(&spec.bind_ip)
                        .unwrap_or(IpAddr::from([0, 0, 0, 0])),
                    spec.bind_port,
                )
            {
                return true;
            }
        }
        false
    }

    /// 白名单校验(读热配置,网页改动即时生效)。
    pub async fn port_allowed(&self, port: u16) -> bool {
        self.hot.read().await.port_allow_list().allows(port)
    }

    /// 从自动分配范围挑选空闲端口(白名单 + 运行时冲突 + OS 探测)。
    pub async fn allocate_port(&self, proto: &str, bind_ip: IpAddr) -> Option<u16> {
        let ranges: Vec<PortsRange> = self.cfg.auto_port_ranges();
        let allow = self.hot.read().await.port_allow_list();
        for r in ranges {
            for p in r.start..=r.end {
                if !allow.allows(p) {
                    continue;
                }
                if self.port_in_use(proto, bind_ip, p, None, None).await {
                    continue;
                }
                let addr = SocketAddr::new(bind_ip, p);
                let probe = if proto == "udp" {
                    std::net::UdpSocket::bind(addr).is_ok()
                } else {
                    std::net::TcpListener::bind(addr).is_ok()
                };
                if probe {
                    return Some(p);
                }
            }
        }
        None
    }

    /// 组键:group 负载均衡支持 tcp 与 vhost(http/https/tcpmux);
    /// 键含 kind/bind_ip/bind_port,同组成员共享同一监听/路由选路池。
    pub fn group_key(spec: &ProxySpec) -> Option<String> {
        let group = spec.group.as_deref().unwrap_or("");
        if group.is_empty() {
            return None;
        }
        if !matches!(
            spec.kind,
            rh_protocol::ProxyKind::Tcp
                | rh_protocol::ProxyKind::Http
                | rh_protocol::ProxyKind::Https
                | rh_protocol::ProxyKind::TcpMux
        ) {
            return None;
        }
        Some(format!(
            "{}|{}|{}|{}",
            spec.kind.as_str(),
            spec.bind_ip,
            spec.bind_port,
            group
        ))
    }

    /// 组退出:移除成员;组清空则取消共享 serve loop 并复位绑定标记。
    pub async fn leave_group(&self, key: &str, proxy_id: u64) {
        let Some(g) = self.groups.get(key).map(|e| e.value().clone()) else {
            return;
        };
        let empty = {
            let mut m = g.members.write().await;
            m.retain(|id| *id != proxy_id);
            m.is_empty()
        };
        if empty {
            g.serve_cancel.read().await.cancel();
            g.listener_bound
                .store(false, std::sync::atomic::Ordering::Relaxed);
        }
    }

    /// vhost 路由重建(简单起见:全量刷新)。
    pub async fn rebuild_vhost_routes(&self) {
        let mut http = Vec::new();
        let mut https = Vec::new();
        let mut tcpmux = Vec::new();
        let subdomain_host = self.cfg.vhost.subdomain_host.clone();
        // ★先快照再遍历:dashmap 的 `iter()` 会持有**分片写锁**,而下面
        // `rt.spec.read().await` 是 await 点 —— 守卫跨 await 会长期占住该分片,
        // 其它 runtime 的增删/查找全被挡住。快照后立即释放。
        let runtimes: Vec<Arc<ProxyRuntime>> = self
            .runtimes
            .iter()
            .map(|e| e.value().clone())
            .collect();
        for rt in runtimes.iter() {
            let spec_guard = rt.spec.read().await;
            let spec = &*spec_guard;
            if !spec.enabled {
                continue;
            }
            match spec.kind {
                rh_protocol::ProxyKind::Http => {
                    let Some(c) = &spec.http else { continue };
                    for d in &c.custom_domains {
                        http.push(route_from(spec, rt, d.clone(), c));
                    }
                    if !c.subdomain.is_empty() && !subdomain_host.is_empty() {
                        http.push(route_from(
                            spec,
                            rt,
                            format!("{}.{}", c.subdomain, subdomain_host),
                            c,
                        ));
                    }
                }
                rh_protocol::ProxyKind::Https => {
                    let Some(c) = &spec.https else { continue };
                    for d in &c.custom_domains {
                        https.push(VhostRoute {
                            host: d.trim().to_lowercase(),
                            locations: vec![],
                            http_user: String::new(),
                            http_password: String::new(),
                            host_rewrite: String::new(),
                            request_headers: Default::default(),
                            response_headers: Default::default(),
                            proxy_id: rt.id,
                            not_found_html: String::new(),
                            cache: None,
                        });
                    }
                    if !c.subdomain.is_empty() && !subdomain_host.is_empty() {
                        https.push(VhostRoute {
                            host: format!("{}.{}", c.subdomain, subdomain_host).to_lowercase(),
                            locations: vec![],
                            http_user: String::new(),
                            http_password: String::new(),
                            host_rewrite: String::new(),
                            request_headers: Default::default(),
                            response_headers: Default::default(),
                            proxy_id: rt.id,
                            not_found_html: String::new(),
                            cache: None,
                        });
                    }
                }
                rh_protocol::ProxyKind::TcpMux => {
                    let Some(c) = &spec.tcpmux else { continue };
                    for d in &c.custom_domains {
                        tcpmux.push(VhostRoute {
                            host: d.trim().to_lowercase(),
                            locations: vec![],
                            http_user: c.http_user.clone(),
                            http_password: c.http_password.clone(),
                            host_rewrite: String::new(),
                            request_headers: Default::default(),
                            response_headers: Default::default(),
                            proxy_id: rt.id,
                            not_found_html: String::new(),
                            cache: None,
                        });
                    }
                }
                _ => {}
            }
        }
        *self.vhost_http.write().await = Arc::new(http);
        *self.vhost_https.write().await = Arc::new(https);
        *self.vhost_tcpmux.write().await = Arc::new(tcpmux);
    }
}

fn route_from(
    _spec: &ProxySpec,
    rt: &ProxyRuntime,
    host: String,
    c: &rh_protocol::HttpProxyConfig,
) -> VhostRoute {
    // B3:注册域名统一小写。请求侧 Host 一律小写(`vhost.rs`),
    // 修前注册 `Example.Foo.com` 永远不命中。
    let host = host.trim().to_lowercase();
    VhostRoute {
        host,
        locations: c.locations.clone(),
        http_user: c.http_user.clone(),
        http_password: c.http_password.clone(),
        host_rewrite: c.host_rewrite.clone(),
        request_headers: c.request_headers.clone(),
        response_headers: c.response_headers.clone(),
        proxy_id: rt.id,
        not_found_html: c.not_found_html.clone(),
        cache: c.cache.clone(),
    }
}

// ---------------- 数据通道请求签发 ----------------
impl AppState {
    /// B8:完整端口占用判定 = 内存运行态 + DB 已启用行。
    ///
    /// `ProxyManager::port_in_use` 只看已注册的 runtime。两个客户端并发
    /// 创建同端口规则时,串行锁是**每客户端**粒度,两者的 runtime 都尚未
    /// 注册,双双通过检查 → 后绑定者失败但记录已落库(enabled=1),留下
    /// 永久 BindError 行,且该端口被它占住。
    ///
    /// 这里补一次 DB 复核(只统计 `enabled=1` 且 `uses_bind_port` 的规则,
    /// 排除自身)。DB 写与检查仍在调用方的 serial_lock 内,跨客户端竞态
    /// 窗口收窄到"读到写"的毫秒级;真正的绑定失败仍由 trial_bind 兜底。
    pub async fn port_in_use_full(
        &self,
        proto: &str,
        ip: IpAddr,
        port: u16,
        except_proxy: Option<u64>,
        new_group_key: Option<String>,
    ) -> bool {
        if self
            .proxies
            .port_in_use(proto, ip, port, except_proxy, new_group_key.clone())
            .await
        {
            return true;
        }
        let Ok(rows) = self.db.list_proxies(None).await else {
            return false;
        };
        for (id, _cid, spec) in rows {
            if Some(id as u64) == except_proxy || !spec.enabled || !spec.kind.uses_bind_port() {
                continue;
            }
            if new_group_key.is_some() && Self::group_key_of(&spec) == new_group_key {
                continue; // 同组共享监听
            }
            let other_ip =
                rh_core::net::parse_bind_ip(&spec.bind_ip).unwrap_or(IpAddr::from([0, 0, 0, 0]));
            if bind_conflicts(proto, ip, port, proto, other_ip, spec.bind_port) {
                return true;
            }
        }
        false
    }

    fn group_key_of(spec: &ProxySpec) -> Option<String> {
        ProxyManager::group_key(spec)
    }

    /// 向客户端签发一个数据通道请求(服务端指令发起)。
    pub async fn issue_create_channel(
        &self,
        sess: &Arc<Session>,
        entity: ChannelEntity,
        purpose: crate::session::RequestPurpose,
    ) -> Option<u64> {
        self.issue_create_channel_with(sess, entity, purpose, None).await
    }

    /// 同 [`Self::issue_create_channel`],但可指定**过期回收钩子**。
    ///
    /// TCP 池路径必须传:`tcp_refill` 在签发前已 `inflight.fetch_add(1)`,
    /// 而只有交付路径或 send 失败才减。客户端在线但 10s 不回报通道时,
    /// 过期条目被静默删除,inflight 永久 +1 → 池慢性饿死。
    pub async fn issue_create_channel_with(
        &self,
        sess: &Arc<Session>,
        entity: ChannelEntity,
        purpose: crate::session::RequestPurpose,
        on_expire: Option<Box<dyn FnOnce() + Send>>,
    ) -> Option<u64> {
        let rid = sess.alloc_request_id();
        sess.add_pending_request(
            rid,
            crate::session::PendingRequest {
                expires: Instant::now() + std::time::Duration::from_secs(10),
                purpose,
                on_expire,
            },
        )
        .await;
        let ok = sess
            .send(ServerMessage::CreateDataChannel(CreateDataChannel {
                request_id: rid,
                entity,
            }))
            .await;
        if ok {
            Some(rid)
        } else {
            // 签发失败:摘掉条目但**不触发** on_expire 钩子。
            //
            // 原因:调用方(tcp_refill)在拿到 None 后**会自己归还** inflight
            // (见 data.rs 的 None 分支)。这里再调钩子就是双减,会把计数打到
            // 0 以下。两处注释必须成对维护 —— 改一边就要改另一边。
            sess.take_pending_request_quiet(rid).await;
            None
        }
    }

    /// 中继双侧通道签发:配对表登记必须先于 CREATE 帧可达。
    /// localhost 下客户端可能在签发任务恢复执行前就完成开流+HELLO 回环,
    /// 若配对表后登记,deliver_pair 会找不到槽位而静默丢流。
    #[allow(clippy::type_complexity)]
    pub async fn issue_relay_pair(
        &self,
        visitor_sess: &Arc<Session>,
        proxy_sess: &Arc<Session>,
        visitor_entity: ChannelEntity,
        visitor_purpose: crate::session::RequestPurpose,
        proxy_entity: ChannelEntity,
        proxy_purpose: crate::session::RequestPurpose,
    ) -> Option<(
        u64,
        u64,
        tokio::sync::oneshot::Receiver<BoxedIo>,
        tokio::sync::oneshot::Receiver<BoxedIo>,
    )> {
        let v_rid = visitor_sess.alloc_request_id();
        let p_rid = proxy_sess.alloc_request_id();
        let (vt, vr) = tokio::sync::oneshot::channel::<BoxedIo>();
        let (pt, pr) = tokio::sync::oneshot::channel::<BoxedIo>();
        {
            let mut pairs = self.relay_pairs.lock().await;
            pairs.insert((visitor_sess.session_id.clone(), v_rid), vt);
            pairs.insert((proxy_sess.session_id.clone(), p_rid), pt);
        }
        let ttl = Instant::now() + std::time::Duration::from_secs(10);
        visitor_sess
            .add_pending_request(
                v_rid,
                crate::session::PendingRequest {
                    expires: ttl,
                    purpose: visitor_purpose,
                    on_expire: None,
                },
            )
            .await;
        proxy_sess
            .add_pending_request(
                p_rid,
                crate::session::PendingRequest {
                    expires: ttl,
                    purpose: proxy_purpose,
                    on_expire: None,
                },
            )
            .await;
        // ★有界发送,不能用 `send().await`。
        //
        // 本函数经 `handle_client_message` → `on_user_connect` → `relay_tcp`
        // 在**控制读循环里内联**执行(control.rs),而那个循环同时是
        // `queue_tx` 的唯一消费者。队列(256)满时 `send().await` 会 park 在
        // "等自己消费"上 —— 与 A1 注释描述的自锁完全同形,心跳与 cancel
        // 全部失联,任务与 fd 永久泄漏。
        // 触发条件很现实:持凭据的 visitor 客户端本地建 256+ 用户连接、
        // 同时停读控制流,再发一条 UserConnect 即可。
        let ok_v = visitor_sess
            .send_bounded(
                ServerMessage::CreateDataChannel(CreateDataChannel {
                    request_id: v_rid,
                    entity: visitor_entity,
                }),
                CONTROL_SEND_TIMEOUT,
            )
            .await;
        let ok_p = proxy_sess
            .send_bounded(
                ServerMessage::CreateDataChannel(CreateDataChannel {
                    request_id: p_rid,
                    entity: proxy_entity,
                }),
                CONTROL_SEND_TIMEOUT,
            )
            .await;
        if ok_v && ok_p {
            return Some((v_rid, p_rid, vr, pr));
        }
        tracing::warn!(
            v_rid,
            p_rid,
            sent_v = ok_v,
            sent_p = ok_p,
            "relay pair create send failed"
        );
        visitor_sess.take_pending_request(v_rid).await;
        proxy_sess.take_pending_request(p_rid).await;
        let mut pairs = self.relay_pairs.lock().await;
        pairs.remove(&(visitor_sess.session_id.clone(), v_rid));
        pairs.remove(&(proxy_sess.session_id.clone(), p_rid));
        None
    }

    /// 组装某客户端的全量 SYNC 实体集。
    pub async fn build_sync_entities(&self, client_id: &str) -> Vec<SyncEntity> {
        let mut out = Vec::new();
        if let Ok(proxies) = self.db.list_proxies(Some(client_id)).await {
            for (_id, _cid, spec) in proxies {
                if spec.enabled {
                    out.push(SyncEntity::Proxy(Box::new(spec)));
                }
            }
        }
        if let Ok(visitors) = self.db.list_visitors(Some(client_id)).await {
            for (_id, _cid, _target, spec) in visitors {
                if spec.enabled {
                    out.push(SyncEntity::Visitor(spec));
                }
            }
        }
        out
    }
}

/// A3:未认证名额守卫(见 [`AppState::enter_unauth_guard`])。
///
/// `Drop` 时若仍 `armed`,归还名额;认证成功点 `disarm()` 后名额已立即归还,
/// Drop 不再动作。这样"名额生命周期"与"认证窗口"严格一致,与会话寿命无关。
pub struct UnauthGuard {
    state: Arc<AppState>,
    ip: IpAddr,
    armed: bool,
}

impl UnauthGuard {
    /// 归还名额并解除武装(幂等)。认证成功的瞬间必须调用。
    pub fn disarm(mut self) {
        self.release();
    }

    fn release(&mut self) {
        if self.armed {
            self.armed = false;
            self.state.exit_unauth(&self.ip);
        }
    }
}

impl Drop for UnauthGuard {
    fn drop(&mut self) {
        self.release();
    }
}

#[cfg(test)]
mod quota_tests {
    use super::*;

    /// `flow_cut` 必须随"加量"解除,否则客户端永久断流。
    ///
    /// 修前只有 `flow_limit == 0`(改成不限)才清标志:管理员把 1GB 调到
    /// 2GB 后,已置位的 flow_cut 会永久保持,所有连接继续被杀,只能重启
    /// 进程或先改 0 再改回来。这条断言按"新上限是否高于已用量"判定。
    #[test]
    fn raising_flow_limit_clears_flow_cut() {
        let u = ClientUsage::default();
        // 用量 1.5GB,上限 1GB → 击穿
        u.bytes
            .store(1_500_000_000, std::sync::atomic::Ordering::Relaxed);
        u.set_quota(ClientQuota {
            rate_limit: String::new(),
            max_conns: 0,
            flow_limit: 1_000_000_000,
        });
        u.add_bytes(0); // 触发判定(总量已达上限)
        assert!(
            u.flow_cut.load(std::sync::atomic::Ordering::Relaxed),
            "用量超上限必须置位 flow_cut"
        );

        // 加量到 2GB:新上限高于已用量 → 必须解除
        u.set_quota(ClientQuota {
            rate_limit: String::new(),
            max_conns: 0,
            flow_limit: 2_000_000_000,
        });
        assert!(
            !u.flow_cut.load(std::sync::atomic::Ordering::Relaxed),
            "加量后 flow_cut 必须解除(修前只有 flow_limit==0 才清,这里会留在 true)"
        );
    }

    /// 调整配额后仍未超过新上限的,不得解除(防止白送额度)。
    #[test]
    fn lowering_or_equal_flow_limit_keeps_flow_cut() {
        let u = ClientUsage::default();
        u.bytes
            .store(3_000_000_000, std::sync::atomic::Ordering::Relaxed);
        u.set_quota(ClientQuota {
            rate_limit: String::new(),
            max_conns: 0,
            flow_limit: 1_000_000_000,
        });
        u.add_bytes(0);
        assert!(u.flow_cut.load(std::sync::atomic::Ordering::Relaxed));

        // 上限仍低于用量:不得解除
        u.set_quota(ClientQuota {
            rate_limit: String::new(),
            max_conns: 0,
            flow_limit: 2_000_000_000,
        });
        assert!(
            u.flow_cut.load(std::sync::atomic::Ordering::Relaxed),
            "新上限仍低于已用量时不得解除 flow_cut"
        );
    }

    /// 改成不限(0)必须无条件解除。
    #[test]
    fn zero_flow_limit_always_clears() {
        let u = ClientUsage::default();
        u.bytes
            .store(9_000_000_000, std::sync::atomic::Ordering::Relaxed);
        u.set_quota(ClientQuota {
            rate_limit: String::new(),
            max_conns: 0,
            flow_limit: 1,
        });
        u.add_bytes(0);
        assert!(u.flow_cut.load(std::sync::atomic::Ordering::Relaxed));

        u.set_quota(ClientQuota::default()); // flow_limit = 0
        assert!(!u.flow_cut.load(std::sync::atomic::Ordering::Relaxed));
    }
}

#[cfg(test)]
mod metrics_listening_tests {
    use super::*;

    /// 任务已结束但句柄仍在时,`metrics_listening()` 必须报 false。
    ///
    /// 修前只看 `is_some()`:任务异常退出(panic / socket 错误)后句柄仍在,
    /// 状态永远报 true —— 控制台显示 metrics 在跑,实际端点已死。
    #[tokio::test]
    async fn reports_false_after_task_finished() {
        let db = crate::db::Db::open("sqlite::memory:").await.unwrap();
        let state = Arc::new(AppState::new(crate::config::ServerConfig::default(), db));
        *state.metrics_task.lock().await = Some(tokio::spawn(async {}));
        // 让任务跑完
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(
            !state.metrics_listening().await,
            "任务已结束,不得再报 listening"
        );
    }

    /// 任务仍在运行时必须报 true(不能因加了 is_finished 就误判)。
    #[tokio::test]
    async fn reports_true_while_task_running() {
        let db = crate::db::Db::open("sqlite::memory:").await.unwrap();
        let state = Arc::new(AppState::new(crate::config::ServerConfig::default(), db));
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        *state.metrics_task.lock().await = Some(tokio::spawn(async move {
            let _ = rx.await;
        }));
        tokio::time::sleep(std::time::Duration::from_millis(30)).await;
        assert!(state.metrics_listening().await, "运行中应报 true");
        let _ = tx.send(());
    }
}
