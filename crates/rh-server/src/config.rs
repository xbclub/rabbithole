//! rh-server 配置(TOML)。示例见 deploy/rh-server.toml。

use rh_core::net::PortsRange;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct ServerConfig {
    pub bind_addr: String,
    /// 主控制监听协议:tcp | tls | ws | wss | quic | kcp(默认 tcp)。
    pub bind_protocol: String,
    pub bind_port: u16,
    /// 配置文件路径(运行期定位 DB/证书)。
    #[serde(skip)]
    pub config_path: std::path::PathBuf,
    /// 附加传输监听(quic/kcp/ws/tls 等多协议并存)。
    pub extra_listeners: Vec<ListenerConfig>,
    /// TLS ALPN 标识列表(留空 = 用 [`rh_transport::DEFAULT_ALPN`] = "h2")。
    ///
    /// ALPN 在 ClientHello 里是**明文**的,项目专属名字会被 DPI 直接识别。
    /// 默认用 `h2`(最常见的 TLS 流量之一)避免成为指纹;需要恢复专属标识
    /// 或改成其它值时在此配置。**必须与客户端一致**,否则 TLS 握手失败。
    pub alpn: Vec<String>,
    /// TCP 传输是否启用 yamux 多路复用。
    pub multiplex: bool,
    /// 数据面中继模式:splice(Linux 零拷贝,默认)| copy(用户态拷贝)。
    /// splice 仅对裸 TCP 两端(无 TLS/WS/多路复用/压缩包装)生效,其余自动回落 copy。
    pub relay_mode: String,
    pub tls: Option<TlsServerFileConfig>,
    pub web: WebConfig,
    pub vhost: VhostConfig,
    pub ports: PortsConfig,
    pub limits: LimitsConfig,
    pub ssh_gateway: SshGatewayConfig,
    pub nathole: NatholeConfig,
    pub metrics: MetricsConfig,
    pub log: LogConfig,
    /// 多载体池允许的最大载体数(含首载体,round-10)。客户端报到时经
    /// `CARRIER_ATTACH_ACK` 回写该值;客户端按 min(期望, 硬件, 该值) 定容。
    /// 取用点钳制 [1,8],默认 8。0 按 1 处理(语义:不允许附加载体)。
    pub max_carriers: usize,
}

/// 载体数上限(与客户端钳制一致;`0` 视为 1)。
pub const MAX_CARRIERS_CAP: usize = 8;

impl ServerConfig {
    /// 生效的载体上限:钳制 [1, MAX_CARRIERS_CAP],0 → 1。
    pub fn max_carriers(&self) -> usize {
        self.max_carriers.clamp(1, MAX_CARRIERS_CAP)
    }
}

impl Default for ServerConfig {
    /// 逐字段与 `#[derive(Default)]` 等价,唯一例外是 `max_carriers = 8`
    /// (derive 会给 0,而 0 会被 `max_carriers()` 钳成 1,等于默认关闭
    /// 多载体)。`#[serde(default)]` 让旧配置文件缺该字段时也落到这里。
    fn default() -> Self {
        Self {
            bind_addr: String::new(),
            bind_protocol: String::new(),
            bind_port: 0,
            config_path: std::path::PathBuf::new(),
            extra_listeners: Vec::new(),
            alpn: Vec::new(),
            multiplex: false,
            relay_mode: String::new(),
            tls: None,
            web: WebConfig::default(),
            vhost: VhostConfig::default(),
            ports: PortsConfig::default(),
            limits: LimitsConfig::default(),
            ssh_gateway: SshGatewayConfig::default(),
            nathole: NatholeConfig::default(),
            metrics: MetricsConfig::default(),
            log: LogConfig::default(),
            max_carriers: MAX_CARRIERS_CAP,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
#[serde(deny_unknown_fields, default)]
pub struct ListenerConfig {
    /// tcp | tls | ws | wss | quic | kcp
    pub protocol: String,
    pub bind_port: u16,
    pub bind_addr: Option<String>,
    pub multiplex: Option<bool>,
    pub tls: Option<TlsServerFileConfig>,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
#[serde(deny_unknown_fields, default)]
pub struct TlsServerFileConfig {
    pub cert_file: Option<PathBuf>,
    pub key_file: Option<PathBuf>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct WebConfig {
    /// 管理 API 监听;默认仅本机。
    pub bind_addr: String,
    pub port: u16,
    /// 前端静态资源目录(可选)。
    pub static_dir: Option<PathBuf>,
    /// JWT 签名密钥;空则启动时生成(重启后 token 失效)。
    pub jwt_secret: Option<String>,
    pub jwt_ttl_hours: u64,
    pub tls_cert: Option<PathBuf>,
    pub tls_key: Option<PathBuf>,
}

impl Default for WebConfig {
    fn default() -> Self {
        Self {
            bind_addr: "127.0.0.1".into(),
            port: 8080,
            static_dir: None,
            jwt_secret: None,
            jwt_ttl_hours: 12,
            tls_cert: None,
            tls_key: None,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct VhostConfig {
    /// HTTP vhost 共享监听端口;0 = 关闭。
    pub http_port: u16,
    /// HTTPS vhost(SNI)共享监听端口;0 = 关闭。
    pub https_port: u16,
    /// tcpmux HTTP CONNECT 共享监听端口;0 = 关闭。
    pub tcpmux_port: u16,
    /// tcpmux passthrough 模式。
    pub tcpmux_passthrough: bool,
    /// 子域名后缀(subdomain 路由)。
    pub subdomain_host: String,
    /// vhost HTTP 读头部超时(秒);0 = 不启用。
    ///
    /// B5:默认 30s。修前默认 0 表示"永不超时",慢速头(slowloris)
    /// 可以无限期占用连接与并发名额。
    pub http_timeout_s: u64,
    pub custom_404_page: Option<PathBuf>,
}

impl Default for VhostConfig {
    fn default() -> Self {
        Self {
            http_port: 0,
            https_port: 0,
            tcpmux_port: 0,
            tcpmux_passthrough: false,
            subdomain_host: String::new(),
            http_timeout_s: 30,
            custom_404_page: None,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
#[derive(Default)]
pub struct PortsConfig {
    /// 自动分配范围(空 = 10000-25000)。
    pub auto_range: Option<String>,
    /// 白名单(空 = 全部允许)。
    pub allow_ports: Vec<String>,
    /// 每客户端最大规则数;0 = 不限。
    pub max_services_per_client: usize,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct LimitsConfig {
    /// 单条复用连接(yamux/QUIC)并发流硬顶;0 = 不限。
    /// 热改仅对新会话生效;yamux 超限 SYN 会终止整条会话,故配额须低于此值。
    pub max_mux_streams: usize,
    /// 每客户端并发数据连接上限(池+在途+转发中);0 = 不限。
    /// 有效值自动钳制到 max_mux_streams-1024(见 data_conn_limit)。
    pub max_data_conns_per_client: usize,
    /// 每代理 UDP 会话映射上限;0 = 不限。
    pub max_udp_sessions_per_proxy: usize,
    /// 每规则预热池深度上限;0 = 不限(仅钳制新建/更新的规则)。
    pub max_pool_size: u8,
    /// 规则未指定池深时的默认值。
    pub default_pool_size: u8,
    /// 控制连接发送队列上限(满则背压等待,不拒连);新会话生效。
    pub control_send_queue: usize,
    /// 每 IP 未认证并发连接上限;0 = 不限。防匿名洪水,不限制已认证业务并发。
    pub unauth_conns_per_ip: usize,
    /// UDP 报文缓冲上限(实际取 max(1500, 此值))。
    pub udp_packet_size: usize,
    /// 用户连接等待池补拨总额(毫秒)。
    pub pool_wait_timeout_ms: u64,
}

impl Default for LimitsConfig {
    fn default() -> Self {
        Self {
            // 默认零上限:并发瓶颈交由系统层(fd/内存/端口),
            // 运维可在控制台或配置文件显式加帽
            max_mux_streams: 0,
            max_data_conns_per_client: 0,
            // B7:UDP 会话表默认必须有上限。修前 0=不限,伪造源地址洪水
            // 在 60s GC 窗口内可把表撑到任意大小。
            max_udp_sessions_per_proxy: 1024,
            max_pool_size: 0,
            default_pool_size: 2,
            control_send_queue: 256,
            unauth_conns_per_ip: 64,
            udp_packet_size: 64 * 1024,
            pool_wait_timeout_ms: 10_000,
        }
    }
}

impl LimitsConfig {
    /// 复用流硬顶有效值(0 = 不限 → 1<<24;64 位限定:256KiB×1<<24 = 4TiB)。
    pub fn mux_streams(&self) -> usize {
        match self.max_mux_streams {
            0 => rh_transport::MUX_STREAMS_UNLIMITED,
            n => n.min(rh_transport::MUX_STREAMS_UNLIMITED),
        }
    }

    /// 每客户端并发数据通道有效上限:配额(0=∞)钳制到 mux 顶 -1024,
    /// 保证应用层永远先于 yamux 干净拒绝(留 1024 给在途未完成 HELLO 的流)。
    ///
    /// ★结果至少为 1。修前 `saturating_sub(1024)` 在 `mux_streams <= 1024`
    /// 时算出 0,而消费点用 `data_conn_limit()` 判 `1 > limit` 恒真 ——
    /// 所有 DATA_CHANNEL_HELLO 被拒,控制面正常、规则显示 Running,
    /// **转发静默全死**,且 api.rs 的校验只查上界不查这个下界。
    pub fn data_conn_limit(&self) -> usize {
        let ceiling = self.mux_streams().saturating_sub(1024).max(1);
        match self.max_data_conns_per_client {
            0 => ceiling,
            n => n.min(ceiling).max(1),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
#[serde(deny_unknown_fields, default)]
pub struct SshGatewayConfig {
    pub enabled: bool,
    pub bind_port: u16,
    pub bind_addr: String,
    pub authorized_keys_file: Option<PathBuf>,
    pub auto_gen_key_dir: Option<PathBuf>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct NatholeConfig {
    /// 打洞 STUN 服务器(host:port);客户端用。
    pub stun_server: String,
    pub punch_timeout_s: u64,
    pub prepare_timeout_s: u64,
}

impl Default for NatholeConfig {
    fn default() -> Self {
        Self {
            stun_server: String::new(),
            punch_timeout_s: 5,
            prepare_timeout_s: 5,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct MetricsConfig {
    pub enable_prometheus: bool,
    /// 独立监听地址:默认仅本机抓取;改 0.0.0.0 对外暴露(端口无鉴权,靠网络边界保护)。
    pub bind_addr: String,
    /// 独立监听端口(不与管理面共口,变更需重启)。
    pub port: u16,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            enable_prometheus: false,
            bind_addr: "127.0.0.1".into(),
            port: 9100,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct LogConfig {
    pub level: String,
    pub file: Option<PathBuf>,
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            level: "info".into(),
            file: None,
        }
    }
}

impl ServerConfig {
    pub fn load(path: &std::path::Path) -> anyhow::Result<Self> {
        let mut cfg: ServerConfig = toml::from_str(&std::fs::read_to_string(path)?)?;
        cfg.config_path = path.to_path_buf();
        Ok(cfg)
    }

    /// 自动分配端口的候选范围。
    ///
    /// ★支持**逗号分隔的多段**(如 `"10000-20000,30000-40000"`)。
    ///
    /// 修前只调 `PortsRange::parse`(只认单段/单端口),多段输入整体返回
    /// None,再被 `unwrap_or_default()` 吞成**空 vec** —— 表现为"配了
    /// auto_range 却分配不出端口",而日志里零线索。现在逐段解析:
    /// 非法段告警并跳过,全部非法才回落默认区间(附 warn)。
    pub fn auto_port_ranges(&self) -> Vec<PortsRange> {
        let Some(raw) = &self.ports.auto_range else {
            return vec![PortsRange {
                start: 10000,
                end: 25000,
            }];
        };
        let mut out = Vec::new();
        for part in raw.split(',') {
            let part = part.trim();
            if part.is_empty() {
                continue;
            }
            match PortsRange::parse(part) {
                Some(r) => out.push(r),
                None => tracing::warn!(
                    segment = part,
                    raw,
                    "auto_range: 无法解析该段(需形如 \"10000-20000\" 或 \"8080\"),已跳过"
                ),
            }
        }
        if out.is_empty() {
            tracing::warn!(
                raw,
                "auto_range: 没有任何有效区间,回落到默认 10000-25000"
            );
            return vec![PortsRange {
                start: 10000,
                end: 25000,
            }];
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `data_conn_limit` 在 `max_mux_streams <= 1024` 时必须 ≥ 1。
    ///
    /// 修前 `saturating_sub(1024)` 算出 0,而消费点用 `1 > limit` 判定,
    /// 恒真 —— 所有 DATA_CHANNEL_HELLO 被拒,控制面正常、规则显示 Running,
    /// **转发静默全死**。这条下界在任何配置下都不能破。
    #[test]
    fn data_conn_limit_never_zero() {
        for (mux, per) in [
            (1usize, 0usize),
            (512, 0),
            (1024, 0),
            (1023, 0),
            (2048, 0),
            (1024, 1),
            (512, 4),
        ] {
            let lim = LimitsConfig {
                max_mux_streams: mux,
                max_data_conns_per_client: per,
                ..Default::default()
            };
            assert!(
                lim.data_conn_limit() >= 1,
                "max_mux_streams={mux} per={per} 算出了 0 —— 数据面会静默全断"
            );
        }
        // 正常配置仍按 mux-1024 收敛(不被 .max(1) 干扰)
        let lim = LimitsConfig {
            max_mux_streams: 4096,
            max_data_conns_per_client: 0,
            ..Default::default()
        };
        assert_eq!(lim.data_conn_limit(), 3072);
        // 显式配额小于 ceiling 时取配额
        let lim = LimitsConfig {
            max_mux_streams: 4096,
            max_data_conns_per_client: 100,
            ..Default::default()
        };
        assert_eq!(lim.data_conn_limit(), 100);
    }

    // round-10:默认必须给 8(derive 会给 0 → 被钳成 1 = 关闭多载体),
    // 旧配置文件缺字段也落到 8;显式值与越界钳制可判。
    #[test]
    fn max_carriers_defaults_and_clamps() {
        assert_eq!(ServerConfig::default().max_carriers, 8);
        assert_eq!(ServerConfig::default().max_carriers(), 8);

        let cfg: ServerConfig = toml::from_str("bind_addr = \"127.0.0.1\"\n").unwrap();
        assert_eq!(cfg.max_carriers, 8, "旧配置缺字段必须默认 8");

        let cfg: ServerConfig =
            toml::from_str("bind_addr = \"127.0.0.1\"\nmax_carriers = 3\n").unwrap();
        assert_eq!(cfg.max_carriers(), 3);

        let cfg: ServerConfig =
            toml::from_str("bind_addr = \"127.0.0.1\"\nmax_carriers = 0\n").unwrap();
        assert_eq!(cfg.max_carriers(), 1, "0 视为不允许附加载体");

        let cfg: ServerConfig =
            toml::from_str("bind_addr = \"127.0.0.1\"\nmax_carriers = 99\n").unwrap();
        assert_eq!(cfg.max_carriers(), 8, "上限钳制 8(红线 5)");
    }
}
