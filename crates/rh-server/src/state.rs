//! AppState:全局运行态(会话注册表、代理运行时、端口分配、组、vhost 路由)
//! 与每客户端串行域。

use crate::config::{ServerConfig, CONTROL_SEND_TIMEOUT};
use crate::db::Db;
use crate::events::EventBus;
use crate::session::{Session, SessionRegistry};
use rh_core::net::{bind_conflicts, PortAllowList, PortsRange};
use rh_protocol::{
    message::{ChannelEntity, CreateDataChannel, ServerMessage, SyncEntity},
    ProxySpec,
};
use rh_transport::BoxedIo;

use std::collections::{HashMap, VecDeque};
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64};
use std::sync::Arc;
use std::time::Instant;
use tokio::net::TcpListener;
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
    /// B7:白名单解析结果缓存(随 allow_ports 在构造时一次解析)。
    /// 修前 port_allowed/allocate_port 每次调用都逐条字符串解析 + Vec
    /// 分配,批量建/改/启 N 条规则 = N 次重解析。
    port_allow: std::sync::Arc<PortAllowList>,
}

impl HotConfig {
    pub fn from_cfg(cfg: &ServerConfig) -> Self {
        let port_allow = Self::parse_allow_list(&cfg.ports.allow_ports);
        Self {
            allow_ports: cfg.ports.allow_ports.clone(),
            max_services_per_client: cfg.ports.max_services_per_client,
            log_level: cfg.log.level.clone(),
            ssh_gateway: cfg.ssh_gateway.clone(),
            metrics: cfg.metrics.clone(),
            port_allow,
        }
    }

    /// 白名单热更新后重建解析缓存(api.rs 保存端口配置时调用)。
    pub fn reparse_allow_list(&mut self) {
        self.port_allow = Self::parse_allow_list(&self.allow_ports);
    }

    fn parse_allow_list(allow_ports: &[String]) -> std::sync::Arc<PortAllowList> {
        std::sync::Arc::new(PortAllowList {
            ranges: allow_ports
                .iter()
                .filter_map(|s| PortsRange::parse(s))
                .collect(),
        })
    }

    pub fn port_allow_list(&self) -> std::sync::Arc<PortAllowList> {
        self.port_allow.clone()
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
        let limit_unreached = q.flow_limit == 0
            || self.bytes.load(std::sync::atomic::Ordering::Relaxed) < q.flow_limit as u64;
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
        let limit = self
            .limits
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .unauth_conns_per_ip;
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
    /// 本规则的独享 serve loop 句柄(A6 顺带项)。
    ///
    /// teardown→spawn 重建同一端口时,旧 serve loop 可能还没把监听 fd 释放,
    /// 新绑定就撞 `EADDRINUSE`(api.rs 拆完立刻重建的路径实测偶发)。
    /// 重建前带超时 await 它收敛,消除这个窗口。
    pub serve_handle: Mutex<Option<tokio::task::JoinHandle<()>>>,
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
    /// C6c:metrics label 的字符串形式(规则生命周期内 id 不变)。
    /// 修前每条 TCP 连接建/关各一次 `id.to_string()` 分配;
    /// 全仓无 `remove_label_values`,IntGaugeVec 子项按 proxy_id 无界累积,
    /// 长跑 + 频繁建删规则时内存与 /metrics 输出持续增长(teardown 已回收)。
    pub metrics_label: String,
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
            serve_handle: Mutex::new(None),
            pool: Mutex::new(None),
            udp_new_channel_tx: Mutex::new(None),
            metrics_label: id.to_string(),
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
    /// B6:host → 路由下标索引(与上方三张表同批重建)。修前每请求
    /// O(全部域名) 的 String== 线性扫描,规则数上千时每请求上万次比较。
    pub vhost_http_idx: RwLock<Arc<std::collections::HashMap<String, Vec<usize>>>>,
    pub vhost_https_idx: RwLock<Arc<std::collections::HashMap<String, Vec<usize>>>>,
    pub vhost_tcpmux_idx: RwLock<Arc<std::collections::HashMap<String, Vec<usize>>>>,
    /// 热生效端口策略(网页可改,与 AppState.hot 同源)。
    pub hot: Arc<RwLock<HotConfig>>,
    /// vhost 路由「待重建」标记。
    ///
    /// ★为什么需要它:`rebuild_vhost_routes()` 遍历**全部** runtime 并逐条
    /// `spec.read().await`。`spawn_runtime` / `teardown_runtime` 原先每次都
    /// 直接 await 一次重建,而 api.rs 有 17 处生命周期调用点 —— 凡是"对一批
    /// 规则做操作"的路径(delete_client / update_client 禁用分支 /
    /// notify_client_recovery / recovery / batch_services)都是
    /// N 次 × 每次 O(N),整体 O(N²)。N=2000 时是 400 万次读锁 + 400 万次
    /// 路由表重建。
    ///
    /// 改为:生命周期函数只 `mark_vhost_dirty()`,由一个常驻合并任务在
    /// [`VHOST_COALESCE_WINDOW`] 后做一次重建。窗口内多次标脏只重建一次。
    ///
    /// ★语义代价(有意接受):路由表最多滞后一个窗口。窗口取 10ms ——
    /// 远小于任何人工操作的感知阈值,也远小于 e2e 测试里的 `sleep 2`。
    /// 需要"立即生效"的场合(单条规则的 API 路径、测试)可显式调
    /// [`Self::flush_vhost_routes`]。
    vhost_dirty: AtomicBool,
    /// 已排队的合并任务(避免窗口内重复 spawn)。
    vhost_flush_scheduled: AtomicBool,
}

/// vhost 路由重建的合并窗口。
///
/// 取值依据:管理面单次操作的端到端耗时在毫秒~数十毫秒量级,10ms 的
/// 路由滞后不可感知;而批量路径(N 条)在窗口内只会触发一次 O(N) 重建,
/// 把 O(N²) 压回 O(N)。再大就会让"建完规则立刻访问"的脚本偶发 404。
const VHOST_COALESCE_WINDOW: std::time::Duration = std::time::Duration::from_millis(10);

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
    /// B6:按 Arc 共享 —— 修前每请求 `route.cache.clone()` 深拷整份
    /// CacheConfig(含 Vec<String> 扩展名白名单)。
    pub cache: Option<std::sync::Arc<rh_protocol::CacheConfig>>,
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
            vhost_http_idx: RwLock::new(Arc::new(Default::default())),
            vhost_https_idx: RwLock::new(Arc::new(Default::default())),
            vhost_tcpmux_idx: RwLock::new(Arc::new(Default::default())),
            vhost_dirty: AtomicBool::new(false),
            vhost_flush_scheduled: AtomicBool::new(false),
        }
    }

    /// 标脏:本次生命周期变更需要重建 vhost 路由。
    ///
    /// 只置位,不重建。合并任务会在窗口后统一重建一次。
    pub fn mark_vhost_dirty(&self) {
        self.vhost_dirty
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }

    /// 立即重建(如果脏)。
    ///
    /// 供需要"重建后立刻可见"的路径使用(单条规则 API、测试、以及
    /// 进程收尾)。多次调用是幂等的:脏标记被原子地清掉,只有清到的那次
    /// 会真正重建。
    pub async fn flush_vhost_routes(&self) {
        if !self
            .vhost_dirty
            .swap(false, std::sync::atomic::Ordering::Relaxed)
        {
            return;
        }
        self.rebuild_vhost_routes().await;
    }

    /// 标脏并安排一次合并重建。
    ///
    /// 生命周期函数调用这个而不是 `rebuild_vhost_routes().await`。
    /// 用 `tokio::spawn` + 窗口:窗口内无论标脏多少次,只会 spawn 一个
    /// 任务(由 `vhost_flush_scheduled` 去重)。
    pub fn schedule_vhost_rebuild(self: &Arc<Self>) {
        self.mark_vhost_dirty();
        // 已经有排队中的任务 → 它会看到最新状态,不必再排一个
        if self
            .vhost_flush_scheduled
            .swap(true, std::sync::atomic::Ordering::Relaxed)
        {
            return;
        }
        let me = self.clone();
        tokio::spawn(async move {
            tokio::time::sleep(VHOST_COALESCE_WINDOW).await;
            // 先清"已排队"再重建:重建期间若有新的标脏,会重新排一个任务,
            // 不会漏掉(顺序不能反 —— 先重建再清标记会丢掉这期间的新变更)。
            me.vhost_flush_scheduled
                .store(false, std::sync::atomic::Ordering::Relaxed);
            me.flush_vhost_routes().await;
        });
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

    /// 收集与查询地址 `(proto, ip)` 冲突的**已占用端口**集合。
    ///
    /// 语义与「对每个候选端口调一次 [`Self::port_in_use`]」完全等价:
    /// `bind_conflicts` 只在同 proto、同端口、且地址层面冲突(任一方通配,
    /// 或两者相等)时成立 —— 端口维度交给 `HashSet::contains`,集合里只放
    /// 「地址层面就与查询地址冲突」的 runtime 的 `bind_port`。
    ///
    /// 这里传端口 0/0 给 `bind_conflicts`,是让它只走地址判定分支;不另写一份
    /// IP 冲突规则,避免两处判定漂移。
    ///
    /// ★先快照再遍历:dashmap 的 `iter()` 会持有**分片写锁**,而下面
    /// `rt.spec.read().await` 是 await 点 —— 守卫跨 await 会长期占住该分片,
    /// 其它 runtime 的增删/查找全被挡住。快照后立即释放。
    async fn collect_occupied_ports(
        &self,
        proto: &str,
        ip: IpAddr,
        except_proxy: Option<u64>,
        new_group_key: Option<&str>,
    ) -> std::collections::HashSet<u16> {
        let runtimes: Vec<Arc<ProxyRuntime>> =
            self.runtimes.iter().map(|e| e.value().clone()).collect();
        let mut out = std::collections::HashSet::new();
        for rt in runtimes.iter() {
            if Some(rt.id) == except_proxy {
                continue;
            }
            let spec = rt.spec.read().await;
            if new_group_key.is_some() && Self::group_key(&spec).as_deref() == new_group_key {
                continue; // 同组:共享监听
            }
            if !spec.kind.uses_bind_port() || !spec.enabled {
                continue;
            }
            let other_ip =
                rh_core::net::parse_bind_ip(&spec.bind_ip).unwrap_or(IpAddr::from([0, 0, 0, 0]));
            if bind_conflicts(proto, ip, 0, proto, other_ip, 0) {
                out.insert(spec.bind_port);
            }
        }
        out
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
        self.collect_occupied_ports(proto, ip, except_proxy, new_group_key.as_deref())
            .await
            .contains(&port)
    }

    /// 白名单校验(读热配置,网页改动即时生效)。
    pub async fn port_allowed(&self, port: u16) -> bool {
        self.hot.read().await.port_allow_list().allows(port)
    }

    /// 从自动分配范围挑选空闲端口(白名单 + 运行时冲突 + OS 探测)。
    ///
    /// ★占用集合只构建一次,循环内退化为 O(1) 查找。
    ///
    /// 修前循环体内每跳过一个候选端口就调一次 `port_in_use` —— 那是一次
    /// 全 runtime 快照 + 逐条 `spec.read().await`。默认 `auto_range` 是
    /// `10000-25000`,端口被占得越多、前面跳过的越多,最坏
    /// 15001 × N_runtime 次读锁。改为先建一次「已占用端口集合」后,
    /// 端口维度是哈希查找,只剩 OS 探测(每个候选端口一次 bind)。
    pub async fn allocate_port(&self, proto: &str, bind_ip: IpAddr) -> Option<u16> {
        let ranges: Vec<PortsRange> = self.cfg.auto_port_ranges();
        let allow = self.hot.read().await.port_allow_list();
        let occupied = self
            .collect_occupied_ports(proto, bind_ip, None, None)
            .await;
        for r in ranges {
            for p in r.start..=r.end {
                if !allow.allows(p) {
                    continue;
                }
                if occupied.contains(&p) {
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
        let runtimes: Vec<Arc<ProxyRuntime>> =
            self.runtimes.iter().map(|e| e.value().clone()).collect();
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
        // B6:同批构建 host 索引(路由顺序与表一致;同 host 多条规则
        // 保留全部下标,http 的最长 locations 匹配仍需遍历候选)
        let idx_of = |v: &[VhostRoute]| {
            let mut m: std::collections::HashMap<String, Vec<usize>> =
                std::collections::HashMap::with_capacity(v.len());
            for (i, r) in v.iter().enumerate() {
                m.entry(r.host.clone()).or_default().push(i);
            }
            Arc::new(m)
        };
        *self.vhost_http_idx.write().await = idx_of(&http);
        *self.vhost_https_idx.write().await = idx_of(&https);
        *self.vhost_tcpmux_idx.write().await = idx_of(&tcpmux);
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
        cache: c.cache.clone().map(std::sync::Arc::new),
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
        // B8:改走按端口精确查询(可走 idx_proxies_bind 部分索引),
        // 修前 list_proxies(None) 全表读 + 每行 spec JSON 反序列化,
        // N 条规则下每次管理操作 O(N),批量操作 O(N²)。
        let Ok(rows) = self.db.list_proxies_on_port(port).await else {
            return false;
        };
        for (id, kind, bind_ip, enabled, group) in rows {
            if Some(id as u64) == except_proxy || !enabled {
                continue;
            }
            // 组键派生(与 ProxyManager::group_key 同式:空组/非组类
            // kind 不参与组共享)
            let grouped =
                !group.is_empty() && matches!(kind.as_str(), "tcp" | "http" | "https" | "tcpmux");
            if grouped
                && new_group_key
                    .as_ref()
                    .is_some_and(|k| *k == format!("{kind}|{bind_ip}|{port}|{group}"))
            {
                continue; // 同组共享监听
            }
            let other_ip =
                rh_core::net::parse_bind_ip(&bind_ip).unwrap_or(IpAddr::from([0, 0, 0, 0]));
            if bind_conflicts(proto, ip, port, proto, other_ip, port) {
                return true;
            }
        }
        false
    }

    /// 向客户端签发一个数据通道请求(服务端指令发起)。
    pub async fn issue_create_channel(
        &self,
        sess: &Arc<Session>,
        entity: ChannelEntity,
        purpose: crate::session::RequestPurpose,
    ) -> Option<u64> {
        self.issue_create_channel_with(sess, entity, purpose, None)
            .await
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
        // A1 自锁修复:本函数的入口(control.rs 的 handle_client_message 与
        // runtime.rs 的 on_apply_result → warm_proxy → tcp_refill)都内联在
        // 控制读循环里,而读循环同时是 queue_tx 的唯一消费者。无界
        // `send().await` 在队列满时 park 在"等自己消费"上 —— 心跳、cancel
        // 全部失联。同文件 issue_relay_pair / on_apply_result 重发分支均已改
        // 有界,这里对齐。失败路径语义保持:走 take_pending_request_quiet
        //(不触发钩子),调用方自己归还 inflight。
        let ok = sess
            .send_bounded(
                ServerMessage::CreateDataChannel(CreateDataChannel {
                    request_id: rid,
                    entity,
                }),
                crate::config::CONTROL_SEND_TIMEOUT,
            )
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
    ///
    /// 两次查询彼此独立、都只读 —— 并行发出,总耗时取两者较慢的一个而不是
    /// 相加。调用点全在会话建立 / APPLY_RESULT 失败重发 / 客户端恢复路径上,
    /// 每次省一个 SQLite 往返。SQLite 池在非内存库下是 8 连接(`db.rs`),
    /// 并行安全;`:memory:` 库是 1 连接,并行会退化为串行 —— 结果仍正确,
    /// 只是没有收益。
    pub async fn build_sync_entities(&self, client_id: &str) -> Vec<SyncEntity> {
        let (proxies, visitors) = tokio::join!(
            self.db.list_proxies(Some(client_id)),
            self.db.list_visitors(Some(client_id)),
        );
        let mut out = Vec::new();
        if let Ok(proxies) = proxies {
            for (_id, _cid, spec) in proxies {
                if spec.enabled {
                    out.push(SyncEntity::Proxy(Box::new(spec)));
                }
            }
        }
        if let Ok(visitors) = visitors {
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

#[cfg(test)]
mod port_scan_tests {
    //! 端口占用判定的**语义等价性**回归(#4 / #5)。
    //!
    //! 修前 `port_in_use` 直接 `dashmap::iter()`(持分片写锁)跨 `spec.read().await`;
    //! `allocate_port` 每个候选端口都重扫一遍全表。改为「一次快照 + 端口集合」后,
    //! 这里守住两件事:
    //!   1. 快照改造没有改变占用判定的结果(逐端口比对旧实现的等价口径);
    //!   2. 判定期间表仍可被其它任务写 —— 分片锁不得跨 await 持有。
    use super::*;

    const CID: &str = "port-scan-client";

    fn spec(kind: &str, ip: &str, port: u16, enabled: bool, group: Option<&str>) -> ProxySpec {
        serde_json::from_value(serde_json::json!({
            "name": format!("p{port}"),
            "kind": kind,
            "client_id": CID,
            "bind_ip": ip,
            "bind_port": port,
            "target_host": "127.0.0.1",
            "target_port": 9,
            "enabled": enabled,
            "group": group,
        }))
        .unwrap()
    }

    async fn state_with(specs: &[ProxySpec]) -> Arc<AppState> {
        let db = crate::db::Db::open("sqlite::memory:").await.unwrap();
        let state = Arc::new(AppState::new(crate::config::ServerConfig::default(), db));
        for (i, s) in specs.iter().enumerate() {
            let id = (i + 1) as u64;
            state
                .proxies
                .runtimes
                .insert(id, Arc::new(ProxyRuntime::new(id, CID.into(), s.clone())));
        }
        state
    }

    /// 占用判定的**旧实现口径**(逐条 runtime 直判),用于对照。
    ///
    /// 保留为独立函数而不是调 `port_in_use`,否则就成了自己和自己比。
    async fn reference_in_use(
        state: &Arc<AppState>,
        proto: &str,
        ip: IpAddr,
        port: u16,
        except: Option<u64>,
        new_group_key: Option<&str>,
    ) -> bool {
        for e in state.proxies.runtimes.iter() {
            let rt = e.value();
            if Some(rt.id) == except {
                continue;
            }
            let spec = rt.spec.read().await;
            if new_group_key.is_some() && ProxyManager::group_key(&spec).as_deref() == new_group_key
            {
                continue;
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

    /// 逐端口比对:新实现与旧口径在所有端口上必须一致。
    #[tokio::test]
    async fn occupied_set_matches_per_port_scan() {
        let state = state_with(&[
            spec("tcp", "127.0.0.1", 20001, true, None),
            spec("tcp", "0.0.0.0", 20002, true, None),
            spec("udp", "127.0.0.1", 20001, true, None),
            spec("tcp", "127.0.0.1", 20003, false, None), // 停用:不占端口
            spec("http", "127.0.0.1", 20004, true, None), // 无独享端口
            spec("tcp", "127.0.0.1", 20005, true, Some("g1")),
        ])
        .await;
        let probe_ips: [&str; 3] = ["127.0.0.1", "0.0.0.0", "192.168.1.9"];
        for ip_s in probe_ips {
            let ip: IpAddr = ip_s.parse().unwrap();
            for proto in ["tcp", "udp"] {
                for port in 20000u16..=20010 {
                    let want = reference_in_use(&state, proto, ip, port, None, None).await;
                    let got = state.proxies.port_in_use(proto, ip, port, None, None).await;
                    assert_eq!(want, got, "proto={proto} ip={ip} port={port}");
                }
            }
        }
    }

    /// 同组共享监听、以及 except_proxy 排除自身,两个分支都要保持。
    #[tokio::test]
    async fn group_and_except_semantics_preserved() {
        let g1 = spec("tcp", "127.0.0.1", 20005, true, Some("g1"));
        let gkey = ProxyManager::group_key(&g1);
        let state = state_with(&[g1]).await;
        let ip: IpAddr = "127.0.0.1".parse().unwrap();

        // 无组键:同端口算冲突
        assert!(
            state
                .proxies
                .port_in_use("tcp", ip, 20005, None, None)
                .await
        );
        // 同组:共享监听,不算冲突
        assert!(
            !state
                .proxies
                .port_in_use("tcp", ip, 20005, None, gkey.clone())
                .await,
            "同组必须视为共享监听"
        );
        // 排除自身后不算冲突
        assert!(
            !state
                .proxies
                .port_in_use("tcp", ip, 20005, Some(1), None)
                .await,
            "except_proxy 必须排除自身"
        );
    }

    /// 分片守卫不得跨 await:端口判定挂起时,runtime 表仍必须可写。
    ///
    /// ★关于「写锁」的口径:任务书与 `rebuild_vhost_routes` 的注释都说
    /// `dashmap::iter()` 持**分片写锁**。核对 dashmap 6.2.1 源码
    /// (`iter.rs` 的 `GuardIter = Arc<RwLockReadGuard<..>>`,`next()` 里调
    /// `_yield_read_shard`)可知实际是**读锁**。危害因此收窄但依然真实:
    /// 同分片的 `insert`/`remove`(runtime 注册/注销)需要写锁,会被跨
    /// await 持有的读锁挡住,而 dashmap 分片锁是**同步锁** —— 挡住的是
    /// 整条 OS 线程,不只是某个 future。
    ///
    /// 复现:让一条 runtime 的 `spec` 写锁被长期持有,`port_in_use` 就会
    /// 挂起在 `spec.read().await` 上并继续持有该分片读锁;此时另起 OS 线程
    /// 往表里灌大量 key,只要有一个落在同分片就会阻塞。断言用**总耗时**,
    /// 不依赖 key→shard 的具体映射。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn shard_guard_not_held_across_await() {
        let state = state_with(&[spec("tcp", "127.0.0.1", 20001, true, None)]).await;
        let ip: IpAddr = "127.0.0.1".parse().unwrap();
        let rt = state.proxies.get(1).unwrap();

        // 长期持有 spec 写锁:让 port_in_use 卡在 spec.read().await
        let holder = {
            let rt = rt.clone();
            tokio::spawn(async move {
                let _g = rt.spec.write().await;
                tokio::time::sleep(std::time::Duration::from_millis(400)).await;
            })
        };
        // 等写锁真的被拿到
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let probe = {
            let state = state.clone();
            tokio::spawn(async move {
                let _ = state
                    .proxies
                    .port_in_use("tcp", ip, 20001, None, None)
                    .await;
            })
        };
        // 让 probe 进入循环并挂起在 spec.read() 上
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let t0 = std::time::Instant::now();
        let writer = {
            let state = state.clone();
            std::thread::spawn(move || {
                for id in 10..5000u64 {
                    state.proxies.runtimes.insert(
                        id,
                        Arc::new(ProxyRuntime::new(
                            id,
                            CID.into(),
                            spec("tcp", "127.0.0.1", 30000 + (id % 1000) as u16, true, None),
                        )),
                    );
                }
            })
        };
        writer.join().unwrap();
        let elapsed = t0.elapsed();
        probe.await.unwrap();
        holder.await.unwrap();

        // 修复后:快照在进入 await 前就释放了分片守卫,写入不受 probe 影响。
        // 阈值(300ms)明显小于写锁窗口(400ms),留足机器抖动余量。
        assert!(
            elapsed < std::time::Duration::from_millis(300),
            "runtime 表写入被端口判定的分片守卫挡住 {elapsed:?} —— \
             dashmap 迭代器不得跨 await 持有"
        );
    }

    /// `allocate_port` 必须跳过运行时已占用的端口,并给出真实可绑定的端口。
    ///
    /// 断言基于外部可观测行为:返回的端口既不在占用集合里,也确实能 bind。
    /// 不去断言「跳过了几个」—— 那是实现细节。
    #[tokio::test]
    async fn allocate_port_skips_occupied_and_returns_bindable() {
        // auto_range 收窄到一段小窗口,避免测试扫 15001 个端口
        let mut cfg = crate::config::ServerConfig::default();
        cfg.ports.auto_range = Some("24100-24120".into());
        let db = crate::db::Db::open("sqlite::memory:").await.unwrap();
        let state = Arc::new(AppState::new(cfg, db));
        // 占住前 5 个
        for (i, p) in (24100u16..24105).enumerate() {
            let id = (i + 1) as u64;
            state.proxies.runtimes.insert(
                id,
                Arc::new(ProxyRuntime::new(
                    id,
                    CID.into(),
                    spec("tcp", "127.0.0.1", p, true, None),
                )),
            );
        }
        let ip: IpAddr = "127.0.0.1".parse().unwrap();
        let got = state.proxies.allocate_port("tcp", ip).await;
        let Some(p) = got else {
            panic!("窗口内应仍有可分配端口");
        };
        assert!(
            !(24100..24105).contains(&p),
            "不得分配运行时已占用的端口,得到 {p}"
        );
        assert!((24100..=24120).contains(&p), "必须落在 auto_range 内:{p}");
        assert!(
            std::net::TcpListener::bind(SocketAddr::new(ip, p)).is_ok(),
            "分配结果必须真的可绑定:{p}"
        );
    }

    /// #4 / #5 纯函数基准(不依赖网络,手动运行):
    ///
    /// ```text
    /// cargo test -p rh-server --release port_scan_tests::bench -- --ignored --nocapture
    /// ```
    ///
    /// 测量「给定 N 个 runtime,`port_in_use` 单次耗时」与「`allocate_port`
    /// 在端口被大量占用时的总耗时」。改前的对照数据用 git stash 切回旧实现
    /// 得到,结果记在 `docs/concurrency-serialization-fixes.md`。
    #[tokio::test]
    #[ignore = "纯函数基准,手动运行(--ignored --nocapture)"]
    async fn bench_port_scan_scaling() {
        let ip: IpAddr = "127.0.0.1".parse().unwrap();
        for n in [100usize, 500, 1000, 2000] {
            let mut specs = Vec::with_capacity(n);
            for i in 0..n {
                // 端口铺开在 10000-25000 内,保证都启用且互不冲突
                specs.push(spec(
                    "tcp",
                    "127.0.0.1",
                    10000 + (i as u16 % 15000),
                    true,
                    None,
                ));
            }
            let state = state_with(&specs).await;

            // ① port_in_use 单次耗时:取"未占用"端口,逼迫实现走完全表
            //
            // ★两个口径都测,让报告里的"修前"列可复现,不依赖 git stash。
            //   `reference_in_use` 是等价性测试用的旧口径实现(直接
            //   `runtimes.iter()` + 逐条 `spec.read().await`),它恰好完整
            //   复刻了修前"跨 await 持有 dashmap 分片读锁"的形态,因此可以
            //   直接当作性能对照 —— 同一份 runtime 表、同一个查询端口,
            //   两个函数做的是同一件事,唯一差别就是快照与否。
            let reps = 100;
            let t_old = std::time::Instant::now();
            for _ in 0..reps {
                let _ = reference_in_use(&state, "tcp", ip, 30000, None, None).await;
            }
            let per_call_old = t_old.elapsed() / reps;

            let t0 = std::time::Instant::now();
            for _ in 0..reps {
                let _ = state
                    .proxies
                    .port_in_use("tcp", ip, 30000, None, None)
                    .await;
            }
            let per_call = t0.elapsed() / reps;

            // ② allocate_port:auto_range 默认 10000-25000,前 n 个端口全被占
            let mut cfg = crate::config::ServerConfig::default();
            cfg.ports.auto_range = Some("10000-25000".into());
            let db = crate::db::Db::open("sqlite::memory:").await.unwrap();
            let state2 = Arc::new(AppState::new(cfg, db));
            for (i, s) in specs.iter().enumerate() {
                let id = (i + 1) as u64;
                state2
                    .proxies
                    .runtimes
                    .insert(id, Arc::new(ProxyRuntime::new(id, CID.into(), s.clone())));
            }
            let t1 = std::time::Instant::now();
            let got = state2.proxies.allocate_port("tcp", ip).await;
            let alloc = t1.elapsed();

            // ②' allocate_port 的修前口径:每个候选端口重扫一遍全表。
            // 逐端口调 `reference_in_use` 就是修前 `allocate_port` 的循环体
            // (它内部调的就是当时的 `port_in_use`)。这里只跑到**第一个空闲
            // 端口**为止,与真实实现同语义,所以两者可直接比较。
            let t2 = std::time::Instant::now();
            let mut got_old = None;
            'outer: for r in 10000u16..=25000 {
                if reference_in_use(&state2, "tcp", ip, r, None, None).await {
                    continue;
                }
                let addr = SocketAddr::new(ip, r);
                if std::net::TcpListener::bind(addr).is_ok() {
                    got_old = Some(r);
                    break 'outer;
                }
            }
            let alloc_old = t2.elapsed();
            debug_assert_eq!(got, got_old, "新旧口径必须选出同一个端口");

            println!(
                "N={n:>5}  port_in_use: old={per_call_old:>10?} new={per_call:>10?}  \
                 allocate_port: old={alloc_old:>10?} new={alloc:>10?}  → {got:?}"
            );
        }
    }
}

#[cfg(test)]
mod sync_entities_tests {
    //! #11 回归:两次查询并行后,实体集内容与顺序语义必须不变。
    use super::*;

    const CID: &str = "sync-entities-client";

    fn proxy_spec(name: &str, port: u16, enabled: bool) -> ProxySpec {
        serde_json::from_value(serde_json::json!({
            "name": name,
            "kind": "tcp",
            "client_id": CID,
            "bind_ip": "127.0.0.1",
            "bind_port": port,
            "target_host": "127.0.0.1",
            "target_port": 9,
            "enabled": enabled,
        }))
        .unwrap()
    }

    fn visitor_spec(name: &str, enabled: bool) -> rh_protocol::VisitorSpec {
        serde_json::from_value(serde_json::json!({
            "name": name,
            "kind": "stcp",
            "enabled": enabled,
            "bind_addr": "127.0.0.1",
            "bind_port": 24500,
            "target_proxy_name": "p1",
        }))
        .unwrap()
    }

    /// 并行取数后:启用的 proxy 与 visitor 都在,停用的都不在。
    #[tokio::test]
    async fn includes_enabled_proxies_and_visitors_only() {
        let db = crate::db::Db::open("sqlite::memory:").await.unwrap();
        db.create_client(CID, "c", &"0".repeat(64), "")
            .await
            .unwrap();
        db.insert_proxy(CID, &proxy_spec("p-on", 24401, true), false, "console")
            .await
            .unwrap();
        db.insert_proxy(CID, &proxy_spec("p-off", 24402, false), false, "console")
            .await
            .unwrap();
        db.insert_visitor(CID, CID, &visitor_spec("v-on", true))
            .await
            .unwrap();
        db.insert_visitor(CID, CID, &visitor_spec("v-off", false))
            .await
            .unwrap();
        let state = Arc::new(AppState::new(crate::config::ServerConfig::default(), db));

        let entities = state.build_sync_entities(CID).await;
        let proxy_names: Vec<String> = entities
            .iter()
            .filter_map(|e| match e {
                SyncEntity::Proxy(p) => Some(p.name.clone()),
                _ => None,
            })
            .collect();
        let visitor_names: Vec<String> = entities
            .iter()
            .filter_map(|e| match e {
                SyncEntity::Visitor(v) => Some(v.name.clone()),
                _ => None,
            })
            .collect();
        assert_eq!(proxy_names, vec!["p-on".to_string()], "只发启用的规则");
        assert_eq!(visitor_names, vec!["v-on".to_string()], "只发启用的访问器");
    }

    /// 别的客户端的实体不得混进来(两次查询各自带 client_id 过滤)。
    #[tokio::test]
    async fn scoped_to_client() {
        let db = crate::db::Db::open("sqlite::memory:").await.unwrap();
        db.create_client(CID, "c", &"0".repeat(64), "")
            .await
            .unwrap();
        db.create_client("other", "o", &"1".repeat(64), "")
            .await
            .unwrap();
        db.insert_proxy(CID, &proxy_spec("mine", 24403, true), false, "console")
            .await
            .unwrap();
        db.insert_proxy(
            "other",
            &proxy_spec("theirs", 24404, true),
            false,
            "console",
        )
        .await
        .unwrap();
        let state = Arc::new(AppState::new(crate::config::ServerConfig::default(), db));

        let entities = state.build_sync_entities(CID).await;
        assert_eq!(entities.len(), 1, "只应包含本客户端的实体");
        match &entities[0] {
            SyncEntity::Proxy(p) => assert_eq!(p.name, "mine"),
            _ => panic!("期望 Proxy 实体"),
        }
    }
}

#[cfg(test)]
mod vhost_coalesce_tests {
    //! #1 回归:vhost 路由重建走脏标记合并窗口。
    //!
    //! 断言基于外部可观测行为:重建次数、以及最终路由表内容。不引用
    //! 窗口常量做断言 —— 用"窗口足够长时仍未重建 / flush 后立即重建"
    //! 这种相对关系。
    use super::*;

    const CID: &str = "vhost-coalesce-client";

    fn http_spec(id: u64, domain: &str) -> ProxySpec {
        serde_json::from_value(serde_json::json!({
            "id": id,
            "name": format!("h{id}"),
            "kind": "http",
            "client_id": CID,
            "bind_ip": "127.0.0.1",
            "bind_port": 0,
            "target_host": "127.0.0.1",
            "target_port": 9,
            "enabled": true,
            "http": { "custom_domains": [domain] },
        }))
        .unwrap()
    }

    async fn state() -> Arc<AppState> {
        let db = crate::db::Db::open("sqlite::memory:").await.unwrap();
        Arc::new(AppState::new(crate::config::ServerConfig::default(), db))
    }

    /// 窗口内多次标脏,只重建一次;最终表里能看到全部规则。
    #[tokio::test]
    async fn many_marks_coalesce_into_one_rebuild() {
        let state = state().await;
        let n = 200u64;
        for i in 1..=n {
            let spec = http_spec(i, &format!("h{i}.test"));
            state
                .proxies
                .runtimes
                .insert(i, Arc::new(ProxyRuntime::new(i, CID.into(), spec)));
            state.proxies.schedule_vhost_rebuild();
        }
        // 合并窗口是毫秒级:等它过去
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let routes = state.proxies.vhost_http.read().await.clone();
        assert_eq!(
            routes.len(),
            n as usize,
            "合并后必须一次性反映全部 {n} 条 http 规则(实际 {})",
            routes.len()
        );
        // 路由表必须精确对应
        for i in 1..=n {
            let host = format!("h{i}.test");
            assert!(
                routes.iter().any(|r| r.host == host && r.proxy_id == i),
                "缺少路由 {host}"
            );
        }
    }

    /// `flush_vhost_routes` 必须立即生效(供需要同步语义的调用方)。
    #[tokio::test]
    async fn flush_is_immediate() {
        let state = state().await;
        let spec = http_spec(1, "immediate.test");
        state
            .proxies
            .runtimes
            .insert(1, Arc::new(ProxyRuntime::new(1, CID.into(), spec)));
        state.proxies.mark_vhost_dirty();
        state.proxies.flush_vhost_routes().await;
        let routes = state.proxies.vhost_http.read().await.clone();
        assert_eq!(routes.len(), 1, "flush 后必须立即可见");
        assert_eq!(routes[0].host, "immediate.test");
    }

    /// 不脏时 flush 是空操作(不重复重建)。
    #[tokio::test]
    async fn flush_when_clean_is_noop() {
        let state = state().await;
        // 先建好一条并 flush
        let spec = http_spec(1, "a.test");
        state
            .proxies
            .runtimes
            .insert(1, Arc::new(ProxyRuntime::new(1, CID.into(), spec)));
        state.proxies.mark_vhost_dirty();
        state.proxies.flush_vhost_routes().await;

        // 直接改 runtime 但不标脏:flush 不应重建,表里仍是旧的
        let spec2 = http_spec(2, "b.test");
        state
            .proxies
            .runtimes
            .insert(2, Arc::new(ProxyRuntime::new(2, CID.into(), spec2)));
        state.proxies.flush_vhost_routes().await;
        let routes = state.proxies.vhost_http.read().await.clone();
        assert_eq!(routes.len(), 1, "未标脏时 flush 不得重建");
    }

    /// #1 基准(手动运行):模拟「N 条规则各触发一次生命周期重建」的总耗时。
    ///
    /// ```text
    /// cargo test -p rh-server --release vhost_coalesce_tests::bench -- --ignored --nocapture
    /// ```
    ///
    /// 两条路径对比:
    ///   `per-call`  = 每次变更都 `rebuild_vhost_routes().await`(修前行为)
    ///   `coalesce`  = 每次变更 `schedule_vhost_rebuild()`,窗口后统一重建
    /// 前者是 O(N²),后者是 O(N)。这里直接调用底层函数复现两条路径,
    /// 不经过 spawn_runtime(那会引入真实 bind,与要测的东西无关)。
    #[tokio::test]
    #[ignore = "耗时基准,手动运行(--ignored --nocapture)"]
    async fn bench_vhost_rebuild_paths() {
        for n in [100usize, 500, 1000, 2000] {
            // 准备 N 条 http 规则
            let mk = |state: &Arc<AppState>| {
                for i in 1..=n as u64 {
                    let spec = http_spec(i, &format!("h{i}.test"));
                    state
                        .proxies
                        .runtimes
                        .insert(i, Arc::new(ProxyRuntime::new(i, CID.into(), spec)));
                }
            };

            // ① 修前口径:每次变更后立刻全量重建
            let state_a = state().await;
            mk(&state_a);
            let t0 = std::time::Instant::now();
            for _ in 0..n {
                state_a.proxies.rebuild_vhost_routes().await;
            }
            let per_call = t0.elapsed();

            // ② 修后口径:N 次标脏 + 一次合并重建
            let state_b = state().await;
            mk(&state_b);
            let t1 = std::time::Instant::now();
            for _ in 0..n {
                state_b.proxies.mark_vhost_dirty();
            }
            state_b.proxies.flush_vhost_routes().await;
            let coalesced = t1.elapsed();

            println!(
                "N={n:>5}  per-call rebuild={per_call:>12?}  coalesced={coalesced:>12?}  \
                 speedup={:.1}x",
                per_call.as_secs_f64() / coalesced.as_secs_f64()
            );
        }
    }

    /// 重建期间的新标脏不得丢失(先清 scheduled 再重建的顺序要求)。
    #[tokio::test]
    async fn mark_during_window_is_not_lost() {
        let state = state().await;
        let spec = http_spec(1, "first.test");
        state
            .proxies
            .runtimes
            .insert(1, Arc::new(ProxyRuntime::new(1, CID.into(), spec)));
        state.proxies.schedule_vhost_rebuild();
        // 窗口内再加一条并标脏
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        let spec2 = http_spec(2, "second.test");
        state
            .proxies
            .runtimes
            .insert(2, Arc::new(ProxyRuntime::new(2, CID.into(), spec2)));
        state.proxies.schedule_vhost_rebuild();

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        let routes = state.proxies.vhost_http.read().await.clone();
        assert_eq!(routes.len(), 2, "窗口内追加的规则不得丢失");
    }
}
