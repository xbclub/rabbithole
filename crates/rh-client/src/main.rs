//! rh-client 入口:配置加载 + 会话主循环(断线退避重连)。

mod config;
mod dnsquery;
mod forward;
mod health;
mod nathole_cli;
mod plugins;
mod session;
mod udpchan;
mod visitor_run;

use clap::Parser;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

#[derive(Parser)]
#[command(name = "rh-client", version)]
struct Cli {
    /// 配置文件(TOML);缺省时依次回退:当前目录 rh-client.toml → 纯环境变量模式。
    #[arg(short, long)]
    config: Option<std::path::PathBuf>,
    /// 覆盖服务器地址(host:port)。
    #[arg(long)]
    server: Option<String>,
    /// 覆盖客户端 ID。
    #[arg(long)]
    id: Option<String>,
    /// 覆盖共享密钥。
    #[arg(long)]
    secret: Option<String>,
    /// 测试/诊断专用(隐藏):打印硬件上限探测结果后退出。
    ///
    /// 存在理由:`tests/e2e.sh` 需要「客户端实际会用的 hardware_cap」作为
    /// 期望值。脚本早先用 Python 独立推算(读 sched_getaffinity + cgroup
    /// 配额 + sysfs 队列),与 Rust 侧的 `available_parallelism()` 在嵌套
    /// cgroup 下可能不一致(REPORT-round10.md「## 6. 遗留」第 1 条)。
    ///
    /// 现在脚本直接消费**这个二进制自己算出的值**,口径不可能再漂移。
    /// 隐藏(`hide = true`)以免进入 `--help` 的公开接口面。
    #[arg(long, hide = true)]
    print_hardware_cap: bool,
}

/// 全局内存分配器(round-35):mimalloc(理由见 rh-server/src/main.rs 同处)。
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

/// 轻量随机数(仅用于重连抖动;不需要密码学强度)。
///
/// 用 `RandomState` 的哈希器播种:标准库已带、不引入新依赖,
/// 且每次进程启动的种子不同(足以打散雷群)。
fn rand_u64() -> u64 {
    use std::hash::{BuildHasher, Hasher};
    let mut h = std::collections::hash_map::RandomState::new().build_hasher();
    h.write_u64(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0),
    );
    h.finish()
}


#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    // 测试/诊断专用出口:打印硬件上限后立即退出。
    //
    // 必须在读配置之前——`tests/e2e.sh` 只想要这个数,不该被迫提供
    // 一份完整配置(client_id/secret 等)。
    if cli.print_hardware_cap {
        println!("{}", config::detect_hardware_cap());
        return Ok(());
    }
    // 配置来源优先级:CLI 覆盖 > 环境变量(RH_*)> 配置文件 > 内置默认。
    // 文件承载全部字段;env 是薄覆盖层(Docker 等场景 3 个字符串接入,见 config.rs)。
    let default_path = std::path::PathBuf::from("rh-client.toml");
    let explicit_config = cli
        .config
        .clone()
        .or_else(|| default_path.exists().then_some(default_path));
    let (mut cfg, from_file) = match &explicit_config {
        Some(p) => (config::ClientConfig::load(p)?, true),
        None => (config::ClientConfig::default(), false),
    };
    let cli_server = cli.server.clone();
    cfg.apply_env()?;
    if let Some(s) = cli_server.as_deref() {
        cfg.server_addr = s.to_string();
    }
    if let Some(i) = cli.id {
        cfg.client_id = i;
    }
    if let Some(s) = cli.secret {
        cfg.secret = s;
    }
    // 纯 env/CLI 模式(无文件)必须显式给服务器地址,避免静默连默认地址
    if !from_file
        && cli_server.is_none()
        && std::env::var("RH_SERVER").unwrap_or_default().is_empty()
    {
        anyhow::bail!(
            "未提供连接信息:请用 --config 指定配置文件,或设置环境变量 \
             RH_SERVER / RH_CLIENT_ID / RH_SECRET(Docker 用法见 README)"
        );
    }
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(&cfg.log_level));
    tracing_subscriber::fmt().with_env_filter(filter).init();
    // 三方 crate(yamux/tungstenite/kcp)走 log facade,桥接到 tracing
    let _ = tracing_log::LogTracer::init();
    // 启动即确认实际生效的全局分配器(round-35)。
    tracing::info!(allocator = "mimalloc", "global allocator");
    anyhow::ensure!(
        !cfg.client_id.is_empty(),
        "client_id 为空(配置文件 / RH_CLIENT_ID / --id)"
    );
    anyhow::ensure!(
        !cfg.secret.is_empty(),
        "secret 为空(配置文件 / RH_SECRET / --secret)"
    );

    let cfg = Arc::new(cfg);
    dnsquery::set_server(cfg.dns_server.clone());
    let shutdown = CancellationToken::new();
    {
        let sd = shutdown.clone();
        tokio::spawn(async move {
            let ctrl_c = tokio::signal::ctrl_c();
            #[cfg(unix)]
            {
                let mut term =
                    tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                        .expect("SIGTERM handler");
                tokio::select! {
                    _ = ctrl_c => {}
                    _ = term.recv() => {}
                }
            }
            #[cfg(not(unix))]
            {
                let _ = ctrl_c.await;
            }
            info!("shutting down");
            sd.cancel();
        });
    }

    let mut backoff_secs = 1u64;
    loop {
        if shutdown.is_cancelled() {
            return Ok(());
        }
        let started = std::time::Instant::now();
        let mut fatal = false;
        match session::connect_and_run(cfg.clone(), shutdown.clone()).await {
            Ok(()) => {
                info!("session ended");
            }
            Err(e) => {
                // ★致命错误(服务端拒绝 / 版本不兼容)重连再多次也没用。
                //
                // 修前一律按"网络抖动"处理:1,2,4,...,60s 无限重试,永久
                // 冲击服务端(每次都走完整 TLS + 挑战应答,而服务端还要为
                // 每次失败写一条 events)。
                //
                // 这里**不退出进程**:容器里退出会触发 restart 风暴,
                // 运维反而更难看清原因。改为退避到上限 + 每个周期一条
                // 醒目 error,提示"需人工修正"。
                //
                // ★判定必须走**类型**,不能匹配文案。
                //
                // 修前这里是 `msg.contains("session rejected")`,而那个串
                // 只在 `dispatch_frame` 里产生、且被 `run_loop` 的 `()` 吞掉;
                // 握手阶段的真实措辞是 `server rejected:` / `hello rejected:`,
                // 都不含它。于是凭据错误照旧无限重试,提示永不打印 ——
                // 一段从未生效的死代码。
                match e.fatal_kind() {
                    Some(kind) => {
                        fatal = true;
                        error!(
                            error = %e,
                            kind = %kind,
                            "会话被服务端拒绝({}):客户端不会自行恢复,{}",
                            kind,
                            kind.advice()
                        );
                    }
                    None => warn!(error = %e, "session error"),
                }
            }
        }
        // 会话存活超过 30s 视为健康周期:退避重置,避免长期运行后
        // 任意一次断线都要等满 60s 才重连
        if started.elapsed() >= std::time::Duration::from_secs(30) {
            backoff_secs = 1;
        }
        // 凭据类错误:直接顶到最大退避(不累积到 60s 之前每轮都重锤服务端),
        // 但仍保留周期性重试 —— 运维修好凭据后无需重启客户端。
        if fatal {
            backoff_secs = 60;
        }
        if shutdown.is_cancelled() {
            return Ok(());
        }
        // ★加抖动,避免"雷群":服务端重启时,所有客户端按同一确定性序列
        // (1,2,4,...,60s)重连,会在同一秒形成尖峰把刚起来的服务端再压垮。
        // 抖动取 ±25% 的均匀分布(整数秒,最小 1s)。
        let base = backoff_secs.max(1);
        let lo = base.saturating_sub(base / 4).max(1);
        let hi = (base + base / 4).max(lo);
        let wait_secs = lo + (rand_u64() % (hi - lo + 1));
        let wait = std::time::Duration::from_secs(wait_secs);
        info!(retry_in = wait_secs, backoff = base, "reconnecting");
        tokio::select! {
            _ = tokio::time::sleep(wait) => {}
            _ = shutdown.cancelled() => return Ok(()),
        }
        backoff_secs = (backoff_secs * 2).min(60);
    }
}
