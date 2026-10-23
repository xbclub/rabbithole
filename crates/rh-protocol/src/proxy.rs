//! 共享代理配置类型:服务端、客户端、控制台与协议同步共用的规格模型。
//!
//! 字段语义对齐 frp 0.71 稳定配置面(见 docs/requirements-matrix.md),
//! 但配置格式为本项目自有,不兼容 frp。

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ProxyKind {
    Tcp,
    Udp,
    Http,
    Https,
    Stcp,
    Sudp,
    Xtcp,
    TcpMux,
}

impl ProxyKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            ProxyKind::Tcp => "tcp",
            ProxyKind::Udp => "udp",
            ProxyKind::Http => "http",
            ProxyKind::Https => "https",
            ProxyKind::Stcp => "stcp",
            ProxyKind::Sudp => "sudp",
            ProxyKind::Xtcp => "xtcp",
            ProxyKind::TcpMux => "tcpmux",
        }
    }
    /// 服务端为该类型绑定独享监听端口(tcp/udp)还是共享路由器(其余)。
    pub fn uses_bind_port(&self) -> bool {
        matches!(self, ProxyKind::Tcp | ProxyKind::Udp)
    }
}

impl std::fmt::Display for ProxyKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// PROXY protocol 版本(客户端写入本地目标的头)。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum ProxyProtoVersion {
    #[default]
    None,
    V1,
    V2,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum BandwidthMode {
    /// 客户端在通道↔目标之间限速。
    Client,
    /// 服务端在用户↔通道之间限速(默认)。
    #[default]
    Server,
}

/// HTTP 头操作:先 set 后 del。
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct HeaderOp {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub set: Vec<HeaderPair>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub del: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HeaderPair {
    pub name: String,
    pub value: String,
}

/// 健康检查(客户端探测本地目标)。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HealthCheckSpec {
    #[serde(rename = "type")]
    pub kind: HealthCheckKind,
    #[serde(default = "default_hc_timeout")]
    pub timeout_seconds: u64,
    pub interval_seconds: u64,
    /// 连续失败次数达到后标记不健康;恢复探测成功即恢复。
    #[serde(default = "default_max_failed")]
    pub max_failed: u32,
    /// HTTP 检查的路径与期望(2xx/3xx 视为成功)。
    #[serde(default)]
    pub path: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub http_headers: Vec<HeaderPair>,
}

fn default_hc_timeout() -> u64 {
    3
}
fn default_max_failed() -> u32 {
    3
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum HealthCheckKind {
    Tcp,
    Http,
}

/// 代理级传输选项。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProxyTransport {
    /// 数据通道端到端 snappy 压缩(START_FORWARD flags 同步,防配置代次错位)。
    #[serde(default)]
    pub use_compression: bool,
    /// 带宽限制,如 "1MB"/"512KB";None 不限。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bandwidth_limit: Option<String>,
    #[serde(default)]
    pub bandwidth_mode: BandwidthMode,
    #[serde(default)]
    pub proxy_proto: ProxyProtoVersion,
}

impl Default for ProxyTransport {
    fn default() -> Self {
        Self {
            use_compression: false,
            bandwidth_limit: None,
            bandwidth_mode: BandwidthMode::Server,
            proxy_proto: ProxyProtoVersion::None,
        }
    }
}

// ---------- 客户端插件 ----------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PluginKind {
    UnixDomainSocket,
    HttpProxy,
    Socks5,
    StaticFile,
    Https2Http,
    Https2Https,
    Http2Https,
    Http2Http,
    Tls2Raw,
}

impl PluginKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            PluginKind::UnixDomainSocket => "unix_domain_socket",
            PluginKind::HttpProxy => "http_proxy",
            PluginKind::Socks5 => "socks5",
            PluginKind::StaticFile => "static_file",
            PluginKind::Https2Http => "https2http",
            PluginKind::Https2Https => "https2https",
            PluginKind::Http2Https => "http2https",
            PluginKind::Http2Http => "http2http",
            PluginKind::Tls2Raw => "tls2raw",
        }
    }
}

impl std::fmt::Display for PluginKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// 插件规格。各插件参数以强类型枚举表达;`params` 中的路径等仅落客户端。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PluginSpec {
    /// 本地 Unix domain socket 转发。
    UnixDomainSocket { unix_path: String },
    /// 本地 HTTP 正向代理(把目标的请求转为本机 HTTP 代理请求)。
    HttpProxy {
        #[serde(default)]
        http_user: String,
        #[serde(default)]
        http_password: String,
    },
    /// 本地 SOCKS5 代理。
    Socks5 {
        #[serde(default)]
        user: String,
        #[serde(default)]
        password: String,
    },
    /// 本地静态文件服务。
    StaticFile {
        local_path: String,
        #[serde(default)]
        strip_prefix: String,
        #[serde(default)]
        http_user: String,
        #[serde(default)]
        http_password: String,
    },
    /// 终结 TLS(本地证书)转明文 HTTP 到 local_addr。
    Https2Http {
        local_addr: String,
        crt_path: String,
        key_path: String,
        #[serde(default)]
        host_rewrite: String,
        #[serde(default)]
        request_headers: HeaderOp,
    },
    /// 终结 TLS 转 HTTPS 上游。
    Https2Https {
        local_addr: String,
        crt_path: String,
        key_path: String,
        #[serde(default)]
        host_rewrite: String,
        #[serde(default)]
        request_headers: HeaderOp,
    },
    /// 明文 HTTP 入 → HTTPS 上游。
    Http2Https {
        local_addr: String,
        #[serde(default)]
        host_rewrite: String,
        #[serde(default)]
        request_headers: HeaderOp,
    },
    /// 明文 HTTP 入 → 明文 HTTP 上游(仅做头改写)。
    Http2Http {
        local_addr: String,
        #[serde(default)]
        host_rewrite: String,
        #[serde(default)]
        request_headers: HeaderOp,
    },
    /// TLS 入(本端证书)→ 裸 TCP 上游(不解读 HTTP)。
    Tls2Raw {
        local_addr: String,
        crt_path: String,
        key_path: String,
    },
}

impl PluginSpec {
    pub fn kind(&self) -> PluginKind {
        match self {
            PluginSpec::UnixDomainSocket { .. } => PluginKind::UnixDomainSocket,
            PluginSpec::HttpProxy { .. } => PluginKind::HttpProxy,
            PluginSpec::Socks5 { .. } => PluginKind::Socks5,
            PluginSpec::StaticFile { .. } => PluginKind::StaticFile,
            PluginSpec::Https2Http { .. } => PluginKind::Https2Http,
            PluginSpec::Https2Https { .. } => PluginKind::Https2Https,
            PluginSpec::Http2Https { .. } => PluginKind::Http2Https,
            PluginSpec::Http2Http { .. } => PluginKind::Http2Http,
            PluginSpec::Tls2Raw { .. } => PluginKind::Tls2Raw,
        }
    }
}

// ---------- 各 kind 特有配置 ----------

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct HttpProxyConfig {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub custom_domains: Vec<String>,
    #[serde(default)]
    pub subdomain: String,
    /// 路由路径前缀;空 = 全部。
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub locations: Vec<String>,
    #[serde(default)]
    pub http_user: String,
    #[serde(default)]
    pub http_password: String,
    #[serde(default)]
    pub host_rewrite: String,
    #[serde(default)]
    pub request_headers: HeaderOp,
    #[serde(default)]
    pub response_headers: HeaderOp,
    /// 按终端用户(经 PROXY protocol 传入)二级路由;空 = 不启用。
    #[serde(default)]
    pub route_by_http_user: String,
    /// M2:请求未命中 locations 时的自定义 404 页(HTML;空 = 全局行为)。
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub not_found_html: String,
    /// M2:HTTP 静态资源缓存。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache: Option<CacheConfig>,
}

/// M2 静态缓存配置(vhost 服务端执行)。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CacheConfig {
    pub ttl_seconds: u64,
    pub max_size_mb: u64,
    #[serde(default)]
    pub extensions: Vec<String>,
    #[serde(default)]
    pub max_item_size_kb: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct HttpsProxyConfig {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub custom_domains: Vec<String>,
    #[serde(default)]
    pub subdomain: String,
}

/// STCP/SUDP/XTCP 通用密钥访问配置。
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SecretProxyConfig {
    /// 访问密钥(密钥访问的唯一事实源,访问器留空即继承此值);
    /// 空 = 不校验密钥,任何访问器都可中继(控制台会警示)。
    /// 访问授权由访问器记录本身保证(仅管理员可建,记录绑定客户端与目标规则,
    /// 客户端无上行注册通道),不设规则侧客户端白名单。
    #[serde(default)]
    pub secret_key: String,
}

pub type StcpProxyConfig = SecretProxyConfig;
pub type SudpProxyConfig = SecretProxyConfig;
pub type XtcpProxyConfig = SecretProxyConfig;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct UdpProxyConfig {
    /// UDP 目标可达性探测端口(0 = target_port)。
    #[serde(default)]
    pub check_port: u16,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TcpMuxProxyConfig {
    /// 目前仅支持 httpconnect。
    #[serde(default = "default_mux")]
    pub multiplexer: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub custom_domains: Vec<String>,
    /// 独享监听端口;0 = 用共享 tcpmux 端口按域名路由。
    #[serde(default)]
    pub http_user: String,
    #[serde(default)]
    pub http_password: String,
    #[serde(default)]
    pub route_by_http_user: String,
}

fn default_mux() -> String {
    "httpconnect".to_string()
}

// ---------- 代理完整规格 ----------

/// 代理(规则)完整规格 —— DB、控制台、SERVICE_SYNC 共用。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProxySpec {
    /// 规则 ID(创建时缺省,由服务端 DB 分配)。
    #[serde(default)]
    pub id: u64,
    pub name: String,
    pub kind: ProxyKind,
    /// 仅供管理 API 透传;协议同步与客户端忽略。
    #[serde(default, skip_serializing)]
    pub client_id: String,
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// tcp/udp:服务端监听地址(字面量 IP);其余 kind 忽略。
    #[serde(default = "default_bind_ip")]
    pub bind_ip: String,
    /// tcp/udp:服务端暴露端口,0=自动分配;其余 kind 忽略。
    #[serde(default)]
    pub bind_port: u16,
    /// 客户端侧目标(无插件时的直连地址)。
    #[serde(default = "default_target_host")]
    pub target_host: String,
    #[serde(default)]
    pub target_port: u16,
    /// TCP 数据通道预热池大小(0-16,0=用服务端默认)。
    #[serde(default)]
    pub pool_size: u8,
    #[serde(default)]
    pub transport: ProxyTransport,
    /// 负载均衡组;组内同 (kind,bind) 规则共享监听。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group: Option<String>,
    #[serde(default)]
    pub group_key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub health_check: Option<HealthCheckSpec>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plugin: Option<PluginSpec>,
    // 各 kind 专属配置(flatten 展开到顶层;归一化后仅 kind 对应项保留)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub udp: Option<UdpProxyConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub http: Option<HttpProxyConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub https: Option<HttpsProxyConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stcp: Option<StcpProxyConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sudp: Option<SudpProxyConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub xtcp: Option<XtcpProxyConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tcpmux: Option<TcpMuxProxyConfig>,
}

impl ProxySpec {
    /// kind 专属配置归位:非本 kind 的清空,本 kind 的确保存在。
    pub fn normalize_kind_configs(&mut self) {
        if self.kind != ProxyKind::Udp {
            self.udp = None;
        }
        if self.kind != ProxyKind::Http {
            self.http = None;
        }
        if self.kind != ProxyKind::Https {
            self.https = None;
        }
        if self.kind != ProxyKind::Stcp {
            self.stcp = None;
        }
        if self.kind != ProxyKind::Sudp {
            self.sudp = None;
        }
        if self.kind != ProxyKind::Xtcp {
            self.xtcp = None;
        }
        if self.kind != ProxyKind::TcpMux {
            self.tcpmux = None;
        }
        match self.kind {
            ProxyKind::Udp => {
                self.udp.get_or_insert_with(Default::default);
            }
            ProxyKind::Http => {
                self.http.get_or_insert_with(Default::default);
            }
            ProxyKind::Https => {
                self.https.get_or_insert_with(Default::default);
            }
            ProxyKind::Stcp => {
                self.stcp.get_or_insert_with(Default::default);
            }
            ProxyKind::Sudp => {
                self.sudp.get_or_insert_with(Default::default);
            }
            ProxyKind::Xtcp => {
                self.xtcp.get_or_insert_with(Default::default);
            }
            ProxyKind::TcpMux => {
                self.tcpmux.get_or_insert_with(Default::default);
            }
            ProxyKind::Tcp => {}
        };
    }

    pub fn http_cfg(&self) -> Option<&HttpProxyConfig> {
        self.http.as_ref()
    }
    pub fn https_cfg(&self) -> Option<&HttpsProxyConfig> {
        self.https.as_ref()
    }
    pub fn tcpmux_cfg(&self) -> Option<&TcpMuxProxyConfig> {
        self.tcpmux.as_ref()
    }
    /// stcp/sudp/xtcp 的密钥配置(kind 不符返回 None)。
    pub fn secret_cfg(&self) -> Option<&SecretProxyConfig> {
        match self.kind {
            ProxyKind::Stcp => self.stcp.as_ref(),
            ProxyKind::Sudp => self.sudp.as_ref(),
            ProxyKind::Xtcp => self.xtcp.as_ref(),
            _ => None,
        }
    }
    /// secret_cfg 的可变版本(kind 不符返回 None;section 须先经 normalize 物化)。
    pub fn secret_cfg_mut(&mut self) -> Option<&mut SecretProxyConfig> {
        match self.kind {
            ProxyKind::Stcp => self.stcp.as_mut(),
            ProxyKind::Sudp => self.sudp.as_mut(),
            ProxyKind::Xtcp => self.xtcp.as_mut(),
            _ => None,
        }
    }
}

fn default_true() -> bool {
    true
}
fn default_bind_ip() -> String {
    "0.0.0.0".to_string()
}
fn default_target_host() -> String {
    "127.0.0.1".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn proxy_spec_json_roundtrip() {
        let spec = ProxySpec {
            id: 7,
            name: "web".into(),
            kind: ProxyKind::Http,
            client_id: String::new(),
            enabled: true,
            bind_ip: "0.0.0.0".into(),
            bind_port: 0,
            target_host: "127.0.0.1".into(),
            target_port: 8080,
            pool_size: 0,
            transport: ProxyTransport::default(),
            group: None,
            group_key: String::new(),
            health_check: Some(HealthCheckSpec {
                kind: HealthCheckKind::Http,
                timeout_seconds: 3,
                interval_seconds: 10,
                max_failed: 3,
                path: "/health".into(),
                http_headers: vec![],
            }),
            plugin: None,
            udp: None,
            http: Some(HttpProxyConfig {
                custom_domains: vec!["a.example.com".into()],
                subdomain: "app".into(),
                locations: vec!["/api".into()],
                http_user: "u".into(),
                http_password: "p".into(),
                host_rewrite: String::new(),
                request_headers: Default::default(),
                response_headers: Default::default(),
                route_by_http_user: String::new(),
                not_found_html: String::new(),
                cache: None,
            }),
            https: None,
            stcp: None,
            sudp: None,
            xtcp: None,
            tcpmux: None,
        };
        let json = serde_json::to_string(&spec).unwrap();
        let back: ProxySpec = serde_json::from_str(&json).unwrap();
        assert_eq!(spec, back);
    }

    #[test]
    fn plugin_spec_tagged() {
        let p = PluginSpec::StaticFile {
            local_path: "/tmp/x".into(),
            strip_prefix: "/s".into(),
            http_user: String::new(),
            http_password: String::new(),
        };
        let json = serde_json::to_string(&p).unwrap();
        assert!(json.contains("\"static_file\""));
        let back: PluginSpec = serde_json::from_str(&json).unwrap();
        assert_eq!(p, back);
    }
}
