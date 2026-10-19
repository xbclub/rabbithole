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
use tracing::{info, warn};

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
}

/// 全局内存分配器(round-35):mimalloc(理由见 rh-server/src/main.rs 同处)。
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
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
        match session::connect_and_run(cfg.clone(), shutdown.clone()).await {
            Ok(()) => {
                info!("session ended");
            }
            Err(e) => {
                warn!(error = %e, "session error");
            }
        }
        // 会话存活超过 30s 视为健康周期:退避重置,避免长期运行后
        // 任意一次断线都要等满 60s 才重连
        if started.elapsed() >= std::time::Duration::from_secs(30) {
            backoff_secs = 1;
        }
        if shutdown.is_cancelled() {
            return Ok(());
        }
        let wait = std::time::Duration::from_secs(backoff_secs);
        info!(retry_in = backoff_secs, "reconnecting");
        tokio::select! {
            _ = tokio::time::sleep(wait) => {}
            _ = shutdown.cancelled() => return Ok(()),
        }
        backoff_secs = (backoff_secs * 2).min(60);
    }
}
