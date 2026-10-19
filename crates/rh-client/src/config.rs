//! 客户端配置(TOML)。

use rh_transport::Protocol;
use serde::Deserialize;
use std::net::SocketAddr;
use std::path::Path;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ClientConfig {
    pub server_addr: String,
    /// tcp | tls | ws | wss | quic | kcp。
    pub protocol: String,
    /// tcp/tls/ws/wss 是否启用 yamux 多路复用(与服务端一致)。
    pub multiplex: bool,
    /// 数据面中继模式:splice(Linux 零拷贝,默认)| copy(用户态拷贝)。
    /// 仅对两端均裸 TCP(本地目标 + 裸模式明文载体)生效,其余自动回落。
    pub relay_mode: String,
    /// 多载体池规模(round-10)。三态语义:
    /// - `None`(默认):自动定容 = 协议基线(tls/wss 2,其余 1)→ 硬件
    ///   上限 → 服务端 `max_carriers`,最终钳制 [1,8];
    /// - `Some(1)`:强制单载体(等价旧路径,不建池);
    /// - `Some(n)`:显式期望 n(仍经硬件/服务端/8 钳制)。
    ///
    /// QUIC/KCP 自带多路复用,忽略本参数(恒单载体);`multiplex=false`
    /// 同样恒单载体。`RH_MUX_POOL_SIZE` 覆盖写为 `Some(n)`。
    pub mux_pool_size: Option<usize>,
    pub client_id: String,
    pub secret: String,
    /// TLS。
    pub trusted_ca_file: Option<String>,
    pub server_name: Option<String>,
    pub tls_insecure_skip_verify: bool,
    pub tls_pin_sha256: Option<String>,
    /// HTTP/SOCKS5 代理拨号(http://user:pass@host:port 或 socks5://…)。
    pub proxy_url: Option<String>,
    /// XTCP 打洞 STUN 服务器;空 = 以本机地址作候选。
    pub stun_server: Option<String>,
    /// 自定义 DNS 服务器(host:port);空 = 系统解析。
    pub dns_server: Option<String>,
    pub log_level: String,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            server_addr: "127.0.0.1:7100".into(),
            protocol: "tcp".into(),
            multiplex: true,
            relay_mode: "splice".into(),
            // None = 自动定容(round-10);旧配置文件无该字段同样落在此态。
            mux_pool_size: None,
            client_id: String::new(),
            secret: String::new(),
            trusted_ca_file: None,
            server_name: None,
            tls_insecure_skip_verify: false,
            tls_pin_sha256: None,
            proxy_url: None,
            stun_server: None,
            dns_server: None,
            log_level: "info".into(),
        }
    }
}

impl ClientConfig {
    pub fn load(path: &std::path::Path) -> anyhow::Result<Self> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| anyhow::anyhow!("read config {path:?}: {e}"))?;
        Ok(toml::from_str(&text)?)
    }

    /// 构造传输层配置。
    ///
    /// **载体数 K 不经过本结构体**(round-44 修正):`mux_pool_size` 由
    /// [`session::initial_k_for`](crate::session::initial_k_for) 结合协议基线
    /// 与硬件上限算出 K,再作为显式参数传给 `connect_pool`。
    ///
    /// `ClientTransportConfig` 里曾经的 `carriers` 字段被赋值却从未被生产
    /// 代码读取(改它不会改变载体数),已删除。
    pub fn transport_cfg(&self) -> anyhow::Result<rh_transport::ClientTransportConfig> {
        let protocol = Protocol::parse(&self.protocol)
            .ok_or_else(|| anyhow::anyhow!("bad protocol {}", self.protocol))?;
        let server_addr: SocketAddr = self
            .server_addr
            .parse()
            .map_err(|_| anyhow::anyhow!("bad server_addr {}", self.server_addr))?;
        Ok(rh_transport::ClientTransportConfig {
            protocol,
            server_addr,
            multiplex: self.multiplex,
            tls: rh_transport::TlsClientOptions {
                trusted_ca: self
                    .trusted_ca_file
                    .as_ref()
                    .map(std::fs::read)
                    .transpose()?,
                server_name: self.server_name.clone(),
                insecure_skip_verify: self.tls_insecure_skip_verify,
                pin_sha256: self.tls_pin_sha256.clone(),
            },
            proxy_url: self.proxy_url.clone(),
            alpn: vec!["rhtp".into()],
            connect_timeout: std::time::Duration::from_secs(10),
        })
    }

    /// 环境变量覆盖层(RH_*):必填三样 + 常用开关,未设置的变量跳过。
    /// 与配置文件的关系:文件承载全部字段(含 CA/代理/STUN 等高级项),
    /// env 只做薄覆盖——Docker / systemd / k8s 场景传 3 个字符串即可接入,
    /// 无需为客户端准备配置文件(规则本就由服务端集中下发)。
    ///
    /// 优先级(round-10):配置文件 `Some(n)` > `RH_MUX_POOL_SIZE` > 自动。
    /// 但本函数在 `load()` 之后调用,env 存在时会覆盖文件值——这与
    /// "文件优先"冲突,故 `mux_pool_size` 的 env 只在文件为 `None` 时生效
    /// (见下方分支),其余字段维持既有"env 覆盖文件"语义。
    pub fn apply_env(&mut self) -> anyhow::Result<()> {
        let s = |k: &str| std::env::var(k).ok().filter(|v| !v.is_empty());
        if let Some(v) = s("RH_SERVER") {
            self.server_addr = v;
        }
        if let Some(v) = s("RH_CLIENT_ID") {
            self.client_id = v;
        }
        if let Some(v) = s("RH_SECRET") {
            self.secret = v;
        }
        if let Some(v) = s("RH_PROTOCOL") {
            self.protocol = v;
        }
        if let Some(v) = s("RH_MULTIPLEX") {
            self.multiplex = parse_env_bool("RH_MULTIPLEX", &v)?;
        }
        if let Some(v) = s("RH_TLS_INSECURE") {
            self.tls_insecure_skip_verify = parse_env_bool("RH_TLS_INSECURE", &v)?;
        }
        if let Some(v) = s("RH_TLS_PIN_SHA256") {
            self.tls_pin_sha256 = Some(v);
        }
        if let Some(v) = s("RH_TRUSTED_CA_FILE") {
            self.trusted_ca_file = Some(v);
        }
        if let Some(v) = s("RH_SERVER_NAME") {
            self.server_name = Some(v);
        }
        if let Some(v) = s("RH_PROXY_URL") {
            self.proxy_url = Some(v);
        }
        if let Some(v) = s("RH_STUN_SERVER") {
            self.stun_server = Some(v);
        }
        if let Some(v) = s("RH_DNS_SERVER") {
            self.dns_server = Some(v);
        }
        if let Some(v) = s("RH_LOG_LEVEL") {
            self.log_level = v;
        }
        if let Some(v) = s("RH_MUX_POOL_SIZE") {
            // 优先级(红线 2):配置文件显式 `Some(n)` > env > 自动。
            // apply_env 在 load 之后调用,若无条件赋值会踩掉文件值,
            // 故仅在当前为 None(未显式配置)时接受 env。
            if self.mux_pool_size.is_none() {
                // 只校验"正整数":范围钳制统一留给三段式定容(§2.4),
                // 避免 env/配置值在两处各自钳制而语义不一致。
                let n: usize = v.parse().ok().filter(|n| *n > 0).ok_or_else(|| {
                    anyhow::anyhow!("环境变量 RH_MUX_POOL_SIZE 取值非法(应为正整数 1..=8):{v}")
                })?;
                self.mux_pool_size = Some(n);
            }
        }
        Ok(())
    }
}

/// 依据可用 CPU 与网卡接收队列数估算载体上限(round-10)。每条载体在低
/// 流数下约占 1.5~2 核(Framer + 4 Worker + FlushTask)→ cpus/2。
pub(crate) fn detect_hardware_cap() -> usize {
    detect_hardware_cap_with(
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4),
        rx_queue_count(),
    )
}

/// 纯函数(可单测):cpu_cap = max(cpus-1, 1);能读到队列数时取
/// `min(cpu_cap, queues)`,读不到(`None`)时上限由 CPU 决定。结果恒在
/// [1,8],绝不返回 0(调用方拿它做 `.min()`,0 会误把 K 打到下限)。
///
/// "读不到" ≠ "只有 1 个队列":Linux 单队列网卡的 `Some(1)` 必须钳制到
/// 1;非 Linux/读取失败是无信息,不能凭空假设最坏值(勘误后语义)。
///
/// # round-54:CPU 上限从 `cpus/2` 放宽到 `cpus-1`
///
/// 旧公式的依据是"每条载体约占 1.5~2 核",实测否定了这个前提。8 核 /
/// 8 RX 队列的测试床上,16 并发连接下服务端 rh-server 只用 **0.71 核**、
/// 客户端 1.02 核(合计 1.73 核 = 8 核的 21%)——载体数不受"每载体一核"
/// 约束,而载体数对高并发吞吐有决定作用(Go 压测器,三条链路交替):
///
/// | 并发连接 | K=1 | K=4 | frp |
/// |---:|---:|---:|---:|
/// | 16 | 2.34 G | 2.45~2.60 G | 2.21~2.61 G |
/// | 64 | 2.08 G | **3.11~3.49 G** | 2.08~2.43 G |
///
/// K=1 在高并发下**随连接数下降**(16→2.46、32→2.21、64→2.08):所有流
/// 挤在一条 TCP 上共用一个拥塞窗口。K=4 打开 4 个独立窗口后不再下降。
///
/// 旧公式把 8 核机器钳在 4,与新公式 `min(cpus-1, queues)` 在该机器上
/// 一致;差异出现在其它规格:4 核机器旧值 2 / 新值 3,16 核机器旧值 8 /
/// 新值 8(受 `clamp(1,8)` 封顶)。**8 核是最常见的测试/生产规格,两者
/// 相同,因此该改动不改变本测试床的默认行为**,只修正更小机器上的过度
/// 保守。
///
/// 保留 `queues` 上界:载体数超过网卡 RX 队列数不会带来更多 RSS 分散,
/// 只会增加每字节成本。
fn detect_hardware_cap_with(cpus: usize, queues: Option<usize>) -> usize {
    let cpu_cap = cpus.saturating_sub(1).max(1);
    match queues {
        Some(q) => cpu_cap.min(q.max(1)).clamp(1, 8),
        None => cpu_cap.clamp(1, 8),
    }
}

/// 网卡接收队列数上限(多网卡取最大)。Linux 读
/// `/sys/class/net/*/queues/rx-*`;非 Linux 或读取失败 → `None`(无信息)。
fn rx_queue_count() -> Option<usize> {
    #[cfg(target_os = "linux")]
    {
        rx_queue_count_in(Path::new("/sys/class/net"))
    }
    #[cfg(not(target_os = "linux"))]
    {
        // 非 Linux 无 sysfs 队列信息:返回 None 让 CPU 上限单独决定,
        // 而不是把平台差异伪造成"单队列网卡"。
        None
    }
}

/// 遍历 `<root>/<if>/queues/rx-*`,返回最大队列数;根目录不可读或无任何
/// 可用网卡条目 → `None`。
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn rx_queue_count_in(root: &Path) -> Option<usize> {
    let entries = std::fs::read_dir(root).ok()?;
    let mut max: Option<usize> = None;
    for entry in entries.flatten() {
        // 必须用 std::fs::metadata(跟随符号链接):Linux 的
        // /sys/class/net/<if> 全部是指向 ../../devices/... 的符号链接,
        // 而 DirEntry::file_type() 与 DirEntry::metadata() 都**不**跟随
        // 链接,按它们过滤会把所有网卡跳过(生产恒为 None)。
        let Ok(md) = std::fs::metadata(entry.path()) else {
            continue;
        };
        if !md.is_dir() {
            continue;
        }
        let queues = entry.path().join("queues");
        let Ok(qs) = std::fs::read_dir(queues) else {
            continue;
        };
        let n = qs
            .flatten()
            .filter(|q| q.file_name().to_str().is_some_and(|s| s.starts_with("rx-")))
            .count();
        if n > 0 {
            max = Some(max.unwrap_or(0).max(n));
        }
    }
    max
}

/// env 布尔值:1/true/yes/on 与 0/false/no/off(大小写不敏感),其余报错。
fn parse_env_bool(name: &str, v: &str) -> anyhow::Result<bool> {
    match v.to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        _ => Err(anyhow::anyhow!(
            "环境变量 {name} 取值非法(应为 true/false):{v}"
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ENV_KEYS: &[&str] = &[
        "RH_SERVER",
        "RH_CLIENT_ID",
        "RH_SECRET",
        "RH_PROTOCOL",
        "RH_MULTIPLEX",
        "RH_TLS_INSECURE",
        "RH_MUX_POOL_SIZE",
    ];

    /// 单测试函数串行执行:std::env 是进程级全局,避免并行测试互相踩。
    /// round-10 起覆盖三态语义(默认 None / env Some / 非法报错)。
    #[test]
    fn env_overrides_layering() {
        for k in ENV_KEYS {
            std::env::remove_var(k);
        }
        // 无 env:默认自动定容(None → carriers=0),不再默认单载体
        let mut c = ClientConfig::default();
        c.apply_env().unwrap();
        assert_eq!(c.server_addr, "127.0.0.1:7100");
        assert!(c.multiplex);
        assert_eq!(
            c.mux_pool_size, None,
            "默认必须是自动定容(None),不得回落到旧的 1"
        );
        // round-44:mux_pool_size=None 表示"自动定容",生效的 K 取协议基线
        // (tcp=1)。断言真正参与定容的 initial_k_for_with,而不是已删除的
        // ClientTransportConfig::carriers。
        let (k, _cap) = crate::session::initial_k_for_with(&c, rh_transport::Protocol::Tcp, 8);
        assert_eq!(k, 4, "None(自动定容)在 tcp 协议下应得到协议基线 K=4(round-54)");

        // 有 env:覆盖默认;布尔解析大小写不敏感
        std::env::set_var("RH_SERVER", "127.0.0.1:7100");
        std::env::set_var("RH_CLIENT_ID", "cid-1");
        std::env::set_var("RH_SECRET", "sec-1");
        std::env::set_var("RH_MULTIPLEX", "off");
        std::env::set_var("RH_TLS_INSECURE", "TRUE");
        std::env::set_var("RH_MUX_POOL_SIZE", "4");
        c.apply_env().unwrap();
        assert_eq!(c.server_addr, "127.0.0.1:7100");
        assert_eq!(c.client_id, "cid-1");
        assert_eq!(c.secret, "sec-1");
        assert!(!c.multiplex);
        assert!(c.tls_insecure_skip_verify);
        assert_eq!(c.mux_pool_size, Some(4), "RH_MUX_POOL_SIZE 覆盖为显式值");
        // 显式 4:生效的 K 必须是 4(硬件上限放大到 8 以免被钳)
        let (k, _cap) = crate::session::initial_k_for_with(&c, rh_transport::Protocol::Tcp, 8);
        assert_eq!(k, 4, "RH_MUX_POOL_SIZE=4 必须让生效 K=4");

        // 显式单载体:Some(1) 必须与 None(自动)可区分。先回到 None
        // (模拟"文件未配置"),env 才会按优先级生效。
        c.mux_pool_size = None;
        std::env::set_var("RH_MUX_POOL_SIZE", "1");
        c.apply_env().unwrap();
        assert_eq!(c.mux_pool_size, Some(1));
        // 显式 1:生成为 K=1。注意它与 None(自动)在 tcp 下同为 1,
        // 二者只能靠 mux_pool_size 本身区分(上一行已断言 Some(1))。
        // 这里补一条更强的判据:即使硬件上限放大,显式 1 也不会被抬高。
        let (k, _cap) = crate::session::initial_k_for_with(&c, rh_transport::Protocol::Tcp, 8);
        assert_eq!(k, 1, "显式 K=1 不得被硬件上限抬高");

        // 非正整数:报错且带变量名与合法范围。空值按"未设置"处理
        // (与其余 RH_* 变量的 s() 过滤一致),不在本断言内。
        c.mux_pool_size = None;
        for bad in ["two", "0", "-2", "1.5"] {
            std::env::set_var("RH_MUX_POOL_SIZE", bad);
            let err = c.apply_env().unwrap_err().to_string();
            assert!(err.contains("RH_MUX_POOL_SIZE"), "bad={bad:?} err: {err}");
            assert!(err.contains("1..=8"), "err 应给出合法范围: {err}");
        }
        // 越界显式值仍接受(钳制留给定容,规格 §2.1-2)
        std::env::set_var("RH_MUX_POOL_SIZE", "16");
        c.apply_env().unwrap();
        assert_eq!(c.mux_pool_size, Some(16));

        // 红线 2:配置文件显式 Some(n) 必须压过 env。apply_env 在 load
        // 之后运行,若无条件赋值会踩掉文件值。
        let mut from_file = ClientConfig {
            mux_pool_size: Some(3),
            ..ClientConfig::default()
        };
        std::env::set_var("RH_MUX_POOL_SIZE", "6");
        from_file.apply_env().unwrap();
        assert_eq!(
            from_file.mux_pool_size,
            Some(3),
            "配置文件显式值优先于 RH_MUX_POOL_SIZE"
        );

        // 文件未配置(None)时 env 生效
        let mut auto = ClientConfig::default();
        auto.apply_env().unwrap();
        assert_eq!(auto.mux_pool_size, Some(6));

        // 非法布尔值:报错且带变量名
        std::env::set_var("RH_MULTIPLEX", "maybe");
        let err = c.apply_env().unwrap_err().to_string();
        assert!(err.contains("RH_MULTIPLEX"), "err: {err}");

        for k in ENV_KEYS {
            std::env::remove_var(k);
        }
    }

    // T-C2(round-10):配置文件两态反序列化。旧文件(无该字段)必须
    // 落 None(自动),显式值必须落 Some(n)——否则迁移后旧部署会被
    // 静默钉死在某个历史默认。
    #[test]
    fn mux_pool_size_deserializes_three_states() {
        let base = r#"
server_addr = "127.0.0.1:7100"
client_id = "c"
secret = "s"
"#;
        // round-44:断言生效 K(initial_k_for_with)而不是已删除的
        // ClientTransportConfig::carriers。三态的语义差异看 K:tls 协议基线
        // 为 2,因此"未配置"落 2、"显式 3"落 3、"显式 1"落 1——三者可区分。
        let absent: ClientConfig = toml::from_str(base).unwrap();
        assert_eq!(absent.mux_pool_size, None);
        let (k, _) = crate::session::initial_k_for_with(&absent, rh_transport::Protocol::Tls, 8);
        assert_eq!(k, 4, "未配置时取 tls 协议基线 K=4(round-54)");

        let explicit: ClientConfig = toml::from_str(&format!("{base}mux_pool_size = 3\n")).unwrap();
        assert_eq!(explicit.mux_pool_size, Some(3));
        let (k, _) = crate::session::initial_k_for_with(&explicit, rh_transport::Protocol::Tls, 8);
        assert_eq!(k, 3, "显式 3 覆盖协议基线");

        let one: ClientConfig = toml::from_str(&format!("{base}mux_pool_size = 1\n")).unwrap();
        assert_eq!(one.mux_pool_size, Some(1));
        let (k, _) = crate::session::initial_k_for_with(&one, rh_transport::Protocol::Tls, 8);
        assert_eq!(k, 1, "显式 1 覆盖协议基线(单载体)");
    }

    // T-C3(round-10,勘误版):硬件上限纯函数四态覆盖——能读到队列数时
    // min(cpu_cap, queues);读不到(None)时上限由 CPU 决定。结果恒在
    // [1,8],0/超大输入都不得 panic 或返回 0。
    #[test]
    fn hardware_cap_pure_function() {
        // Some(1):真实单队列网卡仍钳制到 1
        assert_eq!(detect_hardware_cap_with(1, Some(1)), 1);
        assert_eq!(detect_hardware_cap_with(16, Some(1)), 1);
        // Some(4):min(cpus-1, 4)(round-54:CPU 上限由 cpus/2 放宽到 cpus-1)
        assert_eq!(detect_hardware_cap_with(4, Some(4)), 3);
        assert_eq!(detect_hardware_cap_with(8, Some(4)), 4);
        assert_eq!(detect_hardware_cap_with(16, Some(4)), 4);
        // Some(8):cpu_cap 主导
        assert_eq!(detect_hardware_cap_with(8, Some(8)), 7);
        assert_eq!(detect_hardware_cap_with(16, Some(8)), 8);
        assert_eq!(detect_hardware_cap_with(64, Some(32)), 8, "上限恒 8");
        // None:无队列信息 → cpu_cap(clamp [1,8])
        assert_eq!(detect_hardware_cap_with(1, None), 1);
        assert_eq!(detect_hardware_cap_with(4, None), 3);
        assert_eq!(detect_hardware_cap_with(8, None), 7);
        assert_eq!(detect_hardware_cap_with(16, None), 8);
        assert_eq!(detect_hardware_cap_with(64, None), 8, "上限恒 8");
        // 退化输入:不得为 0
        assert_eq!(detect_hardware_cap_with(0, Some(0)), 1);
        assert_eq!(detect_hardware_cap_with(0, None), 1);
        assert_eq!(detect_hardware_cap_with(usize::MAX, Some(1)), 1);
    }

    // T-C3:注入路径探测。构造 sysfs 形态的临时目录树,验证"多网卡取
    // 最大 rx 队列数"与"缺失/无队列目录回落 None";不依赖真实 sysfs。
    // 非 Linux 的生产入口必须返回 None(CPU 单独定上限)。
    #[test]
    fn rx_queue_count_from_injected_root() {
        fn touch_rx(iface: &Path, n: usize) {
            let q = iface.join("queues");
            std::fs::create_dir_all(&q).unwrap();
            for i in 0..n {
                std::fs::create_dir_all(q.join(format!("rx-{i}"))).unwrap();
            }
        }

        let root = std::env::temp_dir().join(format!(
            "rh-rxq-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();

        // 空根:无网卡目录 → None(无信息),不是 1
        assert_eq!(rx_queue_count_in(&root), None);

        let eth0 = root.join("eth0");
        std::fs::create_dir_all(&eth0).unwrap();
        touch_rx(&eth0, 4);
        assert_eq!(rx_queue_count_in(&root), Some(4));

        // 第二块网卡队列更多 → 取最大
        let eth1 = root.join("eth1");
        std::fs::create_dir_all(&eth1).unwrap();
        touch_rx(&eth1, 8);
        assert_eq!(rx_queue_count_in(&root), Some(8));

        // 普通文件/缺失 queues 的接口被忽略
        std::fs::write(root.join("not-a-dir"), b"x").unwrap();
        std::fs::create_dir_all(root.join("lo")).unwrap();
        assert_eq!(rx_queue_count_in(&root), Some(8));

        // Linux sysfs 形态:接口条目是符号链接,必须跟随(否则生产恒 None)
        #[cfg(unix)]
        {
            let target = root.join("real-iface");
            std::fs::create_dir_all(&target).unwrap();
            touch_rx(&target, 16);
            // eth1 换成指向真实目录的符号链接;最大值 16 只能来自链接
            std::fs::remove_dir_all(&eth1).unwrap();
            std::os::unix::fs::symlink(&target, &eth1).unwrap();
            assert_eq!(
                rx_queue_count_in(&root),
                Some(16),
                "符号链接网卡条目必须被跟随"
            );
        }

        // 路径不存在 → None
        assert_eq!(rx_queue_count_in(&root.join("missing")), None);

        std::fs::remove_dir_all(&root).ok();

        // 平台约定:非 Linux 生产入口返回 None
        #[cfg(not(target_os = "linux"))]
        assert_eq!(super::rx_queue_count(), None);
        #[cfg(target_os = "linux")]
        assert!(super::rx_queue_count().is_some(), "Linux 必须能读 sysfs");
    }
}
