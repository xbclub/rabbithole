//! rh-server 入口:配置加载、迁移、启动恢复、控制面/vhost/SSH 网关/管理面监听。

mod api;
mod config;
mod control;
mod data;
mod db;
mod embedded_ui;
mod events;
mod nathole;
mod recovery;
mod runtime;
mod session;
mod sshgateway;
mod state;
mod vhost;
mod visitor;

use clap::{Parser, Subcommand};
use rh_transport::{Protocol, ServerTransportConfig, TlsServerOptions};
use state::AppState;
use std::sync::Arc;
use tracing::{info, warn};

#[derive(Parser)]
#[command(name = "rh-server", version)]
struct Cli {
    #[arg(short, long, default_value = "rh-server.toml")]
    config: std::path::PathBuf,
    #[command(subcommand)]
    cmd: Option<Cmd>,
}

#[derive(Subcommand)]
enum Cmd {
    /// 管理员账号操作。
    Admin {
        #[command(subcommand)]
        action: AdminAction,
    },
    /// 在目录下生成自签 TLS 证书(cert.pem / key.pem)。
    Gencert { dir: std::path::PathBuf },
}

#[derive(Subcommand)]
enum AdminAction {
    /// 创建初始管理员。
    Create { username: String, password: String },
}

/// 日志级别热切换出口(网页设置页即时生效);启动时经 init_tracing 注入。
pub type SetLevelFn = Box<dyn Fn(&str) -> bool + Send + Sync>;
pub static LOG_SET_LEVEL: std::sync::OnceLock<SetLevelFn> = std::sync::OnceLock::new();

fn init_tracing(cfg: &config::ServerConfig) {
    use tracing_subscriber::layer::SubscriberExt;
    // yamux / tungstenite / kcp 等三方 crate 走 log facade;
    // 不桥接时其协议级错误(如帧错位断连)在生产完全静默
    let _ = tracing_log::LogTracer::init();
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(&cfg.log.level));
    let (filter, handle) = tracing_subscriber::reload::Layer::new(filter);
    let subscriber = tracing_subscriber::fmt().finish().with(filter);
    let _ = tracing::subscriber::set_global_default(subscriber);
    let set = move |level: &str| -> bool {
        match tracing_subscriber::EnvFilter::try_new(level) {
            Ok(f) => handle.modify(|cur| *cur = f).is_ok(),
            Err(_) => false,
        }
    };
    let _ = LOG_SET_LEVEL.set(Box::new(set));
}

/// fd 软上限自检:高并发转发在 ulimit=1024 下会先于测试目标失败,
/// 且报错为 No file descriptors available,极难归因;启动时提前告警。
fn check_fd_limit() {
    if cfg!(target_os = "linux") {
        if let Ok(txt) = std::fs::read_to_string("/proc/self/limits") {
            for line in txt.lines() {
                if let Some(rest) = line.strip_prefix("Max open files") {
                    if let Some(soft) = rest.split_whitespace().next() {
                        let soft: i64 = soft.parse().unwrap_or(-1);
                        if (0..4096).contains(&soft) {
                            tracing::warn!(
                                soft_limit = soft,
                                "low open-files ulimit; high-concurrency forwarding will fail (raise with LimitNOFILE/ulimit -n)"
                            );
                        }
                    }
                }
            }
        }
    }
}

/// 首次启动引导:users 表为空时创建 admin 账号与随机密码。
/// 密码仅打印到控制台(stdout,非 tracing 日志,不落盘不进事件),
/// 与客户端一次性凭据同语义;已有任何用户则跳过(幂等)。
async fn bootstrap_initial_admin(state: &Arc<crate::state::AppState>) {
    match state.db.count_users().await {
        Ok(0) => {}
        Ok(_) => return,
        Err(e) => {
            tracing::warn!(error = %e, "initial admin bootstrap: count users failed");
            return;
        }
    }
    // 随机密码:两个 UUID v4(OsRng)拼接取 24 位
    let pw = format!(
        "{}{}",
        uuid::Uuid::new_v4().simple(),
        uuid::Uuid::new_v4().simple()
    )[..24]
        .to_string();
    let hash = match api::hash_password(&pw) {
        Ok(h) => h,
        Err(e) => {
            tracing::warn!(error = %e, "initial admin bootstrap: hash failed");
            return;
        }
    };
    if let Err(e) = state.db.create_user("admin", &hash).await {
        tracing::warn!(error = %e, "initial admin bootstrap: create failed");
        return;
    }
    state
        .events
        .emit(
            "info",
            "auth",
            "初始管理员账号已创建(随机密码仅在服务端控制台一次性展示,请登录后立即修改)",
        )
        .await;
    println!(
        "\n============================================================\n  初始管理员账号已创建(密码仅此一次展示,请立即登录并修改):\n    username: admin\n    password: {pw}\n============================================================\n"
    );
}

fn tls_opts_from(files: &Option<config::TlsServerFileConfig>) -> anyhow::Result<TlsServerOptions> {
    match files {
        Some(t) if t.cert_file.is_some() && t.key_file.is_some() => {
            let cert_file = t.cert_file.as_ref().unwrap();
            let key_file = t.key_file.as_ref().unwrap();
            let cert_pem = std::fs::read(cert_file)
                .map_err(|e| anyhow::anyhow!("read cert_file {cert_file:?}: {e}"))?;
            let key_pem = std::fs::read(key_file)
                .map_err(|e| anyhow::anyhow!("read key_file {key_file:?}: {e}"))?;
            Ok(TlsServerOptions {
                cert_pem: Some(cert_pem),
                key_pem: Some(key_pem),
                persist_prefix: None,
            })
        }
        _ => Ok(TlsServerOptions::default()),
    }
}

/// 协议需要 TLS 而未配置证书时,自动生成并以 <prefix> 落盘复用。
fn tls_opts_or_auto(
    files: &Option<config::TlsServerFileConfig>,
    proto: Protocol,
    persist_prefix: std::path::PathBuf,
) -> anyhow::Result<TlsServerOptions> {
    let mut o = tls_opts_from(files)?;
    if matches!(proto, Protocol::Tls | Protocol::Wss | Protocol::Quic) && o.cert_pem.is_none() {
        o.persist_prefix = Some(persist_prefix);
    }
    Ok(o)
}

/// 若该监听点会走 TLS,打印叶子证书指纹供客户端 pin。
///
/// **为什么必须打印**:自签证书的指纹是客户端唯一的身份锚点。
/// 不打印的话,`tls_pin_sha256` 只能靠 `openssl x509` 从服务端文件系统里挖,
/// 容器部署下更是拿不到——用户于是只能退化成 `tls_insecure_skip_verify`
/// 或依赖 TOFU 首连不设防。这一行日志把「安全接入」的成本降到复制粘贴。
///
/// 证书来源可能是显式配置的文件,也可能是刚落盘的自签证书;后者在
/// `auto_cert` 里生成,此处读取落盘结果即可(已存在则直接复用)。
fn log_cert_fingerprint(opts: &TlsServerOptions, proto: Protocol, label: &str) {
    if !matches!(proto, Protocol::Tls | Protocol::Wss | Protocol::Quic) {
        return;
    }
    let pem = match &opts.cert_pem {
        Some(p) => Some(p.clone()),
        None => opts.persist_prefix.as_ref().and_then(|prefix| {
            let crt = prefix.with_extension("crt");
            std::fs::read(crt).ok()
        }),
    };
    let Some(pem) = pem else { return };
    match rh_transport::tls::leaf_fingerprint_pem(&pem) {
        Ok(fp) => info!(
            listener = %label,
            fingerprint = %fp,
            "TLS 证书指纹(客户端可用 tls_pin_sha256 / RH_TLS_PIN_SHA256 钉死此值)"
        ),
        Err(e) => warn!(listener = %label, error = %e, "读取 TLS 证书指纹失败"),
    }
}

/// 生效的 ALPN 列表:配置为空时用默认值。
fn alpn_list(configured: &[String]) -> Vec<String> {
    if configured.is_empty() {
        vec![rh_transport::DEFAULT_ALPN.to_string()]
    } else {
        configured.to_vec()
    }
}

fn load_control_cfgs(cfg: &config::ServerConfig) -> anyhow::Result<Vec<ServerTransportConfig>> {
    let cfg_dir = cfg
        .config_path
        .parent()
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| std::path::PathBuf::from("."));
    let mut out = Vec::new();
    let main_proto = Protocol::parse(&cfg.bind_protocol)
        .ok_or_else(|| anyhow::anyhow!("bad bind_protocol {}", cfg.bind_protocol))?;
    let main_tls = tls_opts_or_auto(&cfg.tls, main_proto, cfg_dir.join("certs/rh-main"))?;
    // 触发落盘(缺失时生成),随后才能读到证书算指纹。
    if main_tls.cert_pem.is_none() {
        let _ = rh_transport::tls::auto_cert(&main_tls);
    }
    log_cert_fingerprint(
        &main_tls,
        main_proto,
        &format!("{}:{}", cfg.bind_addr, cfg.bind_port),
    );
    out.push(ServerTransportConfig {
        protocol: main_proto,
        bind_addr: format!("{}:{}", cfg.bind_addr, cfg.bind_port).parse()?,
        multiplex: cfg.multiplex,
        tls: main_tls,
        alpn: crate::alpn_list(&cfg.alpn),
    });
    for l in &cfg.extra_listeners {
        let proto = Protocol::parse(&l.protocol)
            .ok_or_else(|| anyhow::anyhow!("bad listener protocol {}", l.protocol))?;
        let prefix = cfg_dir.join(format!("certs/rh-{}-{}", l.protocol, l.bind_port));
        let tls = tls_opts_or_auto(&l.tls, proto, prefix)?;
        if tls.cert_pem.is_none() {
            let _ = rh_transport::tls::auto_cert(&tls);
        }
        log_cert_fingerprint(&tls, proto, &format!("{}:{}", l.protocol, l.bind_port));
        out.push(ServerTransportConfig {
            protocol: proto,
            bind_addr: format!(
                "{}:{}",
                l.bind_addr.clone().unwrap_or_else(|| cfg.bind_addr.clone()),
                l.bind_port
            )
            .parse()?,
            multiplex: l.multiplex.unwrap_or(cfg.multiplex),
            tls,
            alpn: crate::alpn_list(&cfg.alpn),
        });
    }
    Ok(out)
}

/// 全局内存分配器(round-35):mimalloc。
///
/// 本项目的数据面已由 8 分片缓冲池(vendor/yamux/src/pool.rs)覆盖,
/// 每帧零分配;但控制面仍有约 2 万次/秒的 malloc(实测 test1 负载下
/// 15 秒 29.8 万次):每流的命令通道、`Arc<Mutex<Shared>>`、waker 槽、
/// 任务 Future,以及跨线程释放(载体 Framer 分配帧体、分片 Worker 释放)。
///
/// glibc 默认 malloc 在"跨线程释放"形态下会把对象还到释放者的 arena,
/// 长期运行后形成难以复用的空洞;mimalloc 的延迟队列把跨线程释放归还
/// 原线程回收,正是针对这一点。实测该路径 CPU 占比仅 0.67%,故此项
/// 定位为低成本的稳定性保险(平抑长跑碎片与延迟毛刺),而非吞吐优化。
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let cfg_path = std::fs::canonicalize(&cli.config)
        .map_err(|e| anyhow::anyhow!("config {:?}: {e}", cli.config))?;
    let cfg = config::ServerConfig::load(&cfg_path)?;
    init_tracing(&cfg);
    check_fd_limit();
    // 启动即确认实际生效的全局分配器(round-35):线上排查时唯一可靠依据。
    info!(allocator = "mimalloc", "global allocator");
    let db_dir = cfg_path
        .parent()
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| std::path::PathBuf::from("."));
    let db_path = format!("sqlite://{}/rabbithole.db", db_dir.display());
    let db = db::Db::open(&db_path).await?;
    match cli.cmd {
        Some(Cmd::Admin {
            action: AdminAction::Create { username, password },
        }) => {
            let hash = api::hash_password(&password)?;
            db.create_user(&username, &hash).await?;
            println!("admin {username} created");
            return Ok(());
        }
        Some(Cmd::Gencert { dir }) => {
            std::fs::create_dir_all(&dir)?;
            let opts = TlsServerOptions {
                persist_prefix: Some(dir.join("rh-server")),
                ..Default::default()
            };
            let (cert, key) = rh_transport::tls::auto_cert(&opts)?;
            std::fs::write(dir.join("cert.pem"), &cert)?;
            std::fs::write(dir.join("key.pem"), &key)?;
            println!("cert written to {}", dir.display());
            // 同一份证书被写了两处:dir/cert.pem 供用户配置 cert_file,
            // dir/rh-server.crt 是 persist_prefix 的落盘名。打印指纹方便
            // 直接配到客户端 tls_pin_sha256(容器场景常拿不到文件系统)。
            match rh_transport::tls::leaf_fingerprint_pem(&cert) {
                Ok(fp) => println!("sha256 fingerprint: {fp}"),
                Err(e) => eprintln!("warning: compute fingerprint failed: {e}"),
            }
            return Ok(());
        }
        None => {}
    }
    let state = Arc::new(AppState::new(cfg.clone(), db));
    // 挑战表周期清扫(回收未被消费的过期 nonce)
    tokio::spawn(state.challenge.clone().sweeper());
    // events 表周期裁剪(保留最近 2 万条);此前完全是只增不减。
    {
        let db = state.db.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(3600));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tick.tick().await;
                match db.prune_events(20_000).await {
                    Ok(0) => {}
                    Ok(n) => tracing::debug!(removed = n, "events pruned"),
                    Err(e) => tracing::warn!(error = %e, "events prune failed"),
                }
            }
        });
    }
    // 登录限速器的 key 周期逐出。
    //
    // `governor` 的 keyed limiter 对每个**唯一 key 永久保留状态**。P0 修复
    // 后 key 已不含端口,但扫描器轮换源 IP 时仍会让表缓慢增长。
    // `retain_recent()` 只保留仍在窗口内的条目,10 分钟一次足够。
    {
        let lim_ip = state.login_limiter.clone();
        let lim_user = state.login_limiter_user.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(600));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tick.tick().await;
                lim_ip.retain_recent();
                lim_user.retain_recent();
            }
        });
    }
    // 首次启动引导:无任何用户时创建 admin + 随机密码,仅在控制台一次性展示。
    bootstrap_initial_admin(&state).await;
    // 启动恢复:双方 enabled 的规则重新绑定。
    recovery::recover_all(&state).await;
    // 控制面监听。每个监听器的叶子证书**随监听器**交给 serve_transport:
    // 多监听器各有独立自签证书,认证后必须下发"客户端连的那个端口"的证书,
    // 否则逐字节比对必然失败并被误报为中间人。
    let control_cfgs = load_control_cfgs(&cfg)?;
    // ★先绑定,再 spawn:绑定失败必须让进程起不来。
    //
    // 修前 serve_transport 在任务内部 bind,失败只记 error! 就 return,
    // 而 spawn 无人 join —— 端口被占时进程"启动成功"却没有控制面,
    // systemd 显示 active、退出码 0,客户端全连不上且极难归因。
    let mut bound = Vec::new();
    for tc in control_cfgs.iter() {
        info!(addr = %tc.bind_addr, proto = ?tc.protocol, "control listener starting");
        let (listener, leaf) = control::bind_transport(&state, tc).await.map_err(|e| {
            anyhow::anyhow!(
                "control listen failed on {} ({:?}): {e}",
                tc.bind_addr,
                tc.protocol
            )
        })?;
        bound.push((listener, leaf));
    }
    for (listener, leaf) in bound {
        tokio::spawn(control::serve_transport_bound(
            state.clone(),
            listener,
            leaf,
        ));
    }
    // vhost 监听
    if cfg.vhost.http_port > 0 {
        let l =
            tokio::net::TcpListener::bind((cfg.bind_addr.as_str(), cfg.vhost.http_port)).await?;
        info!(port = cfg.vhost.http_port, "vhost http up");
        tokio::spawn(vhost::http_vhost_serve(state.clone(), l));
    }
    if cfg.vhost.https_port > 0 {
        let l =
            tokio::net::TcpListener::bind((cfg.bind_addr.as_str(), cfg.vhost.https_port)).await?;
        info!(port = cfg.vhost.https_port, "vhost https up");
        tokio::spawn(vhost::https_vhost_serve(state.clone(), l));
    }
    if cfg.vhost.tcpmux_port > 0 {
        let l =
            tokio::net::TcpListener::bind((cfg.bind_addr.as_str(), cfg.vhost.tcpmux_port)).await?;
        info!(port = cfg.vhost.tcpmux_port, "vhost tcpmux up");
        let pt = cfg.vhost.tcpmux_passthrough;
        tokio::spawn(vhost::tcpmux_serve(state.clone(), l, pt));
    }
    // F2:加载客户端配额缓存;M3:加载证书表
    state.refresh_client_quotas().await;
    state.rebuild_certs().await;
    // Prometheus 指标独立监听(热开关;启动绑定失败仅告警,不阻断主服务)
    if cfg.metrics.enable_prometheus {
        if let Err(e) = state.metrics_listen_start(&cfg.metrics).await {
            warn!(error = %e, "prometheus metrics listener failed to start");
        }
    }
    // SSH Tunnel Gateway(supervisor 常驻,跟随热配置启停)
    {
        let st = state.clone();
        let watch = (*st.ssh_watch.1).clone();
        let shutdown = st.shutdown.clone();
        tokio::spawn(sshgateway::supervisor(st, watch, shutdown));
    }
    // 管理 API
    let mut app = api::router(state.clone());
    if let Some(dir) = &cfg.web.static_dir {
        // SPA:静态文件优先;未知路径回退 index.html 并返回 200(history 路由可用)
        let static_root = dir.clone();
        // L-A b(D-15 孪生):SPA 回退的 index.html 字节缓存 —— 首次读取后
        // 进程内复用(修前每次深链回退都重新 fs::read 整份)。文件变更需
        // 重启生效,与嵌入式控制台(编译期内嵌)同级的口径;static_dir
        // 产物由部署方替换,重启窗口可接受。缓存头对齐 embedded_ui:
        // assets/ 内容哈希名 → 一年 immutable;index(含回退)→ no-cache。
        let index_cache = std::sync::Arc::new(tokio::sync::OnceCell::new());
        app = app.fallback(move |req: axum::http::Request<axum::body::Body>| {
            let serve = tower_http::services::ServeDir::new(static_root.clone());
            let index_path = static_root.join("index.html");
            let index_cache = index_cache.clone();
            let is_api = req.uri().path().starts_with("/api/");
            let path = req.uri().path().to_owned();
            async move {
                use axum::response::IntoResponse;
                use tower::ServiceExt;
                if is_api {
                    return crate::api::err(404, "not found");
                }
                let mut resp = match serve.oneshot(req).await {
                    Ok(r) => r,
                    Err(_) => return axum::http::StatusCode::INTERNAL_SERVER_ERROR.into_response(),
                };
                let headers = resp.headers_mut();
                headers.insert(
                    axum::http::header::CACHE_CONTROL,
                    axum::http::HeaderValue::from_static(if path.starts_with("/assets/") {
                        "public, max-age=31536000, immutable"
                    } else {
                        "no-cache"
                    }),
                );
                if resp.status() != axum::http::StatusCode::NOT_FOUND {
                    return resp.map(axum::body::Body::new);
                }
                let body = index_cache
                    .get_or_init(|| async {
                        tokio::fs::read(&index_path).await.unwrap_or_default()
                    })
                    .await
                    .clone();
                if body.is_empty() {
                    return axum::http::StatusCode::NOT_FOUND.into_response();
                }
                let mut resp = axum::response::Html(body).into_response();
                resp.headers_mut().insert(
                    axum::http::header::CACHE_CONTROL,
                    axum::http::HeaderValue::from_static("no-cache"),
                );
                resp
            }
        });
    } else if embedded_ui::available() {
        // 未配置 static_dir 但构建期内嵌了前端:单二进制直接托管控制台
        info!(
            files = embedded_ui::file_count(),
            "web api up with embedded ui (static_dir not configured)"
        );
        app = app.fallback(
            move |req: axum::http::Request<axum::body::Body>| async move {
                embedded_ui::handle(req.uri().path()).await
            },
        );
    }
    let web_addr: std::net::SocketAddr =
        format!("{}:{}", cfg.web.bind_addr, cfg.web.port).parse()?;
    if !web_addr.ip().is_loopback() && cfg.web.tls_cert.is_none() {
        warn!("web api binds non-loopback without TLS; ensure a HTTPS reverse proxy is in front");
    }
    info!(addr = %web_addr, "web api up");
    let listener = tokio::net::TcpListener::bind(web_addr).await?;
    let shutdown = {
        let state = state.clone();
        async move {
            shutdown_signal().await;
            state.shutdown.cancel();
            // 给运行时任务一点时间收尾
            tokio::time::sleep(std::time::Duration::from_millis(300)).await;
            // ★事件落库收尾。
            //
            // `EventBus` 改为异步批量写之后,进程退出时必须显式 drain,
            // 否则队列里最后几条事件随进程消失 —— 而"最后几条"恰恰是
            // 停机相关的事件(客户端下线、配置变更),最不该丢。
            // 先 flush(等 writer 追上)再 shutdown(关队列 + 等它退出)。
            state.events.flush().await;
            state.events.shutdown().await;
        }
    };
    // 可选 TLS:包装 TcpListener 为 axum Listener
    if let (Some(cert_p), Some(key_p)) = (&cfg.web.tls_cert, &cfg.web.tls_key) {
        let rustls_cfg = rh_transport::tls::server_config(&TlsServerOptions {
            cert_pem: Some(std::fs::read(cert_p)?),
            key_pem: Some(std::fs::read(key_p)?),
            persist_prefix: None,
        })?;
        let tls_listener = TlsListener {
            inner: listener,
            acceptor: tokio_rustls::TlsAcceptor::from(rustls_cfg),
        };
        axum::serve(
            tls_listener,
            app.into_make_service_with_connect_info::<crate::WebAddr>(),
        )
        .with_graceful_shutdown(shutdown)
        .await?;
    } else {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<crate::WebAddr>(),
        )
        .with_graceful_shutdown(shutdown)
        .await?;
    }
    Ok(())
}

/// 包装 TLS 的 axum Listener(管理面 HTTPS)。
struct TlsListener {
    inner: tokio::net::TcpListener,
    acceptor: tokio_rustls::TlsAcceptor,
}

/// 管理 API 对端地址:为 TCP 与 TLS 两种 Listener 提供统一的 connect_info 类型。
#[derive(Clone, Copy)]
pub struct WebAddr(pub std::net::SocketAddr);

impl std::fmt::Display for WebAddr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl
    axum::extract::connect_info::Connected<axum::serve::IncomingStream<'_, tokio::net::TcpListener>>
    for WebAddr
{
    fn connect_info(stream: axum::serve::IncomingStream<'_, tokio::net::TcpListener>) -> Self {
        Self(*stream.remote_addr())
    }
}

impl axum::extract::connect_info::Connected<axum::serve::IncomingStream<'_, TlsListener>>
    for WebAddr
{
    fn connect_info(stream: axum::serve::IncomingStream<'_, TlsListener>) -> Self {
        Self(*stream.remote_addr())
    }
}

impl axum::serve::Listener for TlsListener {
    type Io = tokio_rustls::server::TlsStream<tokio::net::TcpStream>;
    type Addr = std::net::SocketAddr;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        loop {
            match self.inner.accept().await {
                // D-13:握手必须有时限 —— 不发 ClientHello 的连接会把
                // accept(Slowloris 式)永久钉死,管理面整体停摆。10s 覆盖
                // 高延迟/弱客户端的正常握手;超时视同握手失败,断开继续。
                Ok((sock, addr)) => {
                    match tokio::time::timeout(
                        std::time::Duration::from_secs(10),
                        self.acceptor.accept(sock),
                    )
                    .await
                    {
                        Ok(Ok(tls)) => return (tls, addr),
                        Ok(Err(e)) => {
                            tracing::debug!(error = %e, "web tls handshake failed");
                        }
                        Err(_) => {
                            tracing::debug!("web tls handshake timeout (10s)");
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(error = %e, "web accept error");
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                }
            }
        }
    }

    fn local_addr(&self) -> std::io::Result<Self::Addr> {
        self.inner.local_addr()
    }
}

async fn shutdown_signal() {
    let ctrl_c = tokio::signal::ctrl_c();
    #[cfg(unix)]
    {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("install SIGTERM handler");
        tokio::select! {
            _ = ctrl_c => {}
            _ = term.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = ctrl_c.await;
    }
}
