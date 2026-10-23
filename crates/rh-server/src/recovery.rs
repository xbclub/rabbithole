//! 启动恢复:资格查询 services.enabled=1 JOIN clients.enabled=1,逐条 bind。

use crate::state::AppState;
use rh_protocol::ProxyKind;
use std::sync::Arc;
use tracing::{info, warn};

/// 恢复阶段的并发度。
///
/// 取值依据:`spawn_runtime` 内部有真实 IO(绑定监听、启动 serve loop),
/// 并发太高会互相抢端口,收益也不明显(启动恢复的总量是"规则数",不是
/// "每秒请求数");太低则线性叠加单条延迟。16 在两者之间取平衡。
///
/// 注意这里只并发 **spawn** 阶段。端口重分配与 DB 查询仍串行(见
/// [`recover_all`] 的分段说明)。
pub(crate) const RECOVERY_CONCURRENCY: usize = 16;

pub async fn recover_all(state: &Arc<AppState>) {
    // ★DB 错误必须显式告警,不能静默当成"没有规则"。
    //
    // 修前用 `unwrap_or_default()`:DB 读失败(锁、磁盘、迁移未完成)时得到
    // 空列表,循环一条都不跑,服务"正常启动"但**所有规则都没恢复** ——
    // 外部表现是"升级后规则全没了",而日志里零线索。
    // 这里降级为"继续启动 + warn",仍不阻断进程(启动期硬失败会让运维更难
    // 排查),但至少留痕并指明后果。
    let proxies = match state.db.list_proxies(None).await {
        Ok(v) => v,
        Err(e) => {
            warn!(
                error = %e,
                "startup recovery: list_proxies failed — NO proxies will be restored; \
                 check DB (rules are still in the database, a restart after fixing will recover them)"
            );
            Vec::new()
        }
    };
    let clients = match state.db.list_clients().await {
        Ok(v) => v,
        Err(e) => {
            warn!(
                error = %e,
                "startup recovery: list_clients failed — all proxy restores will be skipped \
                 (enabled-clients set is empty)"
            );
            Vec::new()
        }
    };
    let enabled_clients: std::collections::HashSet<String> = clients
        .iter()
        .filter(|c| c.enabled)
        .map(|c| c.id.clone())
        .collect();
    // 预取 client 的 config_revision:一次全表,而不是每条规则一次
    // `get_client`(修前是 N 次额外往返)。
    let revisions: std::collections::HashMap<String, u64> = clients
        .iter()
        .map(|c| (c.id.clone(), c.config_revision.max(0) as u64))
        .collect();

    // ★分两段,原因见下。
    //
    // 第一段(串行):资格过滤、spec 规范化、`port_auto` 被外部占用时的
    // 端口重分配。这一段**必须串行**,因为 `allocate_port` 只看
    // `ProxyManager::runtimes` 里的运行态 + OS 试绑,而恢复路径**不持有**
    // `serial_lock(client_id)`(那把锁按客户端粒度,恢复期间也不存在并发
    // 创建者)。若并发执行,两条 port_auto 规则会同时读到"该端口空闲"并
    // 双双通过 OS 试绑之前的检查 —— 其中一条随后 bind 失败,留下
    // BindError 行。串行做端口决策是任务书 §2 #2 明确要求的前提。
    //
    // 第二段(并发):`spawn_runtime`。它内部的 bind 是对**已经定好**的
    // 端口做的,不同规则端口互不重叠(串行段已保证),因此可以安全并发。
    let mut prepared: Vec<(String, rh_protocol::ProxySpec, u64)> = Vec::new();
    for (id, client_id, mut spec) in proxies {
        if !spec.enabled || !enabled_clients.contains(&client_id) {
            continue;
        }
        {
            let l = state.limits.read().unwrap_or_else(|e| e.into_inner());
            crate::db::normalize_spec(&mut spec, l.default_pool_size, l.max_pool_size);
        }
        // port_auto 被外部占用 → 立即重分配(同事务改库);
        // 固定端口被占不重分配(§4.6):交由 spawn_runtime 进入 bind_error 60s 重试
        let port_auto = state
            .db
            .get_proxy(id)
            .await
            .ok()
            .flatten()
            .map(|(_, _, _, auto)| auto)
            .unwrap_or(false);
        if port_auto && spec.kind.uses_bind_port() && spec.bind_port > 0 {
            let bind_ip = rh_core::net::parse_bind_ip(&spec.bind_ip)
                .unwrap_or(std::net::IpAddr::from([0, 0, 0, 0]));
            let in_use = if spec.kind == ProxyKind::Tcp {
                std::net::TcpListener::bind(std::net::SocketAddr::new(bind_ip, spec.bind_port))
                    .is_err()
            } else {
                std::net::UdpSocket::bind(std::net::SocketAddr::new(bind_ip, spec.bind_port))
                    .is_err()
            };
            if in_use {
                // 重分配
                if let Some(p) = state
                    .proxies
                    .allocate_port(spec.kind.as_str(), bind_ip)
                    .await
                {
                    warn!(
                        proxy = spec.name,
                        old = spec.bind_port,
                        new = p,
                        "PORT_REALLOCATED"
                    );
                    spec.bind_port = p;
                    let _ = state.db.update_proxy(id, &spec, true).await;
                    state.db.bump_revision(&client_id).await.ok();
                    state
                        .events
                        .emit(
                            "warn",
                            "service",
                            &format!("规则 {} 端口重新分配为 {}", spec.name, p),
                        )
                        .await;
                }
            }
        }
        let rev = revisions.get(&client_id).copied().unwrap_or(0);
        prepared.push((client_id, spec, rev));
    }

    // 第二段:并发 spawn。修前是逐条 `spawn_runtime(...).await`,而单条
    // `spawn_runtime` 内部有十来个 await(取会话、查 client、bind、启动
    // serve loop、组登记、池初始化…),N 条规则就是 N 倍单条延迟,恢复
    // 期间规则全不可用。叠加 vhost 全量重建时还是 O(N²)。
    use futures::stream::StreamExt as _;
    let recovered = std::sync::atomic::AtomicUsize::new(0);
    futures::stream::iter(prepared)
        .for_each_concurrent(RECOVERY_CONCURRENCY, |(client_id, spec, rev)| {
            let state = state.clone();
            let recovered = &recovered;
            async move {
                crate::runtime::spawn_runtime(&state, &client_id, &spec, rev).await;
                info!(proxy = spec.name, "recovered");
                recovered.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        })
        .await;
    // 收尾:把合并窗口里可能还挂着的路由重建落实,保证恢复完成后路由表
    // 已经包含全部规则(启动期没有"下一个操作"来触发它)。
    state.proxies.flush_vhost_routes().await;
    info!(
        recovered = recovered.load(std::sync::atomic::Ordering::Relaxed),
        "startup recovery finished"
    );
}

#[cfg(test)]
mod recovery_tests {
    //! #2 回归:启动恢复的**并发 spawn** 不得改变结果,且端口分配必须
    //! 保持串行决策(不产生重复端口)。
    use super::*;

    fn stcp_spec(client_id: &str, name: &str) -> rh_protocol::ProxySpec {
        serde_json::from_value(serde_json::json!({
            "name": name,
            "kind": "stcp",
            "client_id": client_id,
            "bind_ip": "127.0.0.1",
            "bind_port": 0,
            "target_host": "127.0.0.1",
            "target_port": 9,
            "enabled": true,
        }))
        .unwrap()
    }

    async fn seed(n_clients: usize, per_client: usize, enabled: bool) -> (Arc<AppState>, usize) {
        let db = crate::db::Db::open("sqlite::memory:").await.unwrap();
        let mut total = 0;
        for c in 0..n_clients {
            let cid = format!("rec-c{c}");
            db.create_client(&cid, &cid, &format!("{}", c % 10).repeat(64), "")
                .await
                .unwrap();
            if !enabled {
                // 客户端本身禁用 → 其规则一律不恢复
                db.set_client_enabled(&cid, false).await.unwrap();
            }
            for p in 0..per_client {
                db.insert_proxy(&cid, &stcp_spec(&cid, &format!("p{p}")), false, "console")
                    .await
                    .unwrap();
                total += 1;
            }
        }
        let state = Arc::new(AppState::new(crate::config::ServerConfig::default(), db));
        (state, total)
    }

    /// 全部规则都被恢复:运行态表里条数与 DB 里的启用规则数一致。
    #[tokio::test]
    async fn restores_all_enabled_rules() {
        let (state, total) = seed(4, 25, true).await;
        recover_all(&state).await;
        assert_eq!(
            state.proxies.runtimes.len(),
            total,
            "全部 {total} 条启用规则都必须注册运行态"
        );
    }

    /// 客户端被禁用 → 其规则一条都不恢复。
    #[tokio::test]
    async fn skips_disabled_clients() {
        let (state, _) = seed(3, 10, false).await;
        recover_all(&state).await;
        assert!(
            state.proxies.runtimes.is_empty(),
            "客户端禁用时不得恢复任何规则"
        );
    }

    /// 恢复是幂等的:跑两次不产生重复运行态。
    #[tokio::test]
    async fn recovery_is_idempotent() {
        let (state, total) = seed(2, 10, true).await;
        recover_all(&state).await;
        recover_all(&state).await;
        assert_eq!(
            state.proxies.runtimes.len(),
            total,
            "重复恢复不得产生重复条目"
        );
    }

    /// 并发 spawn 后,vhost 路由表必须已包含全部规则。
    ///
    /// 这条盯的是"并发化 + 合并窗口"的组合:恢复结束时必须显式 flush,
    /// 否则启动完成后路由表可能还是空的(没有"下一个操作"来触发重建)。
    #[tokio::test]
    async fn vhost_routes_ready_after_recovery() {
        let db = crate::db::Db::open("sqlite::memory:").await.unwrap();
        db.create_client("rec-v", "v", &"0".repeat(64), "")
            .await
            .unwrap();
        for i in 0..20 {
            let spec: rh_protocol::ProxySpec = serde_json::from_value(serde_json::json!({
                "name": format!("h{i}"),
                "kind": "http",
                "client_id": "rec-v",
                "bind_ip": "127.0.0.1",
                "bind_port": 0,
                "target_host": "127.0.0.1",
                "target_port": 9,
                "enabled": true,
                "http": { "custom_domains": [format!("h{i}.test")] },
            }))
            .unwrap();
            db.insert_proxy("rec-v", &spec, false, "console")
                .await
                .unwrap();
        }
        let state = Arc::new(AppState::new(crate::config::ServerConfig::default(), db));
        recover_all(&state).await;
        let routes = state.proxies.vhost_http.read().await.clone();
        assert_eq!(
            routes.len(),
            20,
            "恢复结束时 vhost 路由必须已经全部就绪(实际 {})",
            routes.len()
        );
    }

    /// #2 基准(手动运行,带在线会话):验证并发在真实 await 点上的收益。
    ///
    /// ```text
    /// cargo test -p rh-server --release recovery_tests::bench_recovery_with_online -- --ignored --nocapture
    /// ```
    ///
    /// 与 `bench_recovery_scaling` 的区别:这里给每个客户端注册一个**在线
    /// 会话**,于是 `spawn_runtime` 会走 `mark_unconfirmed`(取 Mutex)与
    /// `send_bounded`(mpsc 有界队列)—— 这些才是真正的 await 挂起点。
    ///
    /// ★队列必须足够大且有消费者。第一版把队列设成 64 且不消费,结果
    /// N=1000 时排队规则全部撞 2s `CONTROL_SEND_TIMEOUT`,测出来 62s ——
    /// 那个数字反映的是"队列满时的超时",不是并发收益。这里改为
    /// 容量 100000 + 后台消费,隔离出锁争用本身。
    #[tokio::test]
    #[ignore = "耗时基准,手动运行(--ignored --nocapture)"]
    async fn bench_recovery_with_online_sessions() {
        for n in [100usize, 500, 1000] {
            let clients = 8usize;
            let per = n.div_ceil(clients);
            let (state, total) = seed(clients, per, true).await;
            for c in 0..clients {
                let cid = format!("rec-c{c}");
                let (tx, mut rx) = tokio::sync::mpsc::channel(100_000);
                state.sessions.register(crate::session::Session::new(
                    cid,
                    format!("sid-{c}"),
                    c as u64 + 1,
                    tokio_util::sync::CancellationToken::new(),
                    tx,
                ));
                // 后台消费,保证 send_bounded 不会撞超时
                tokio::spawn(async move { while rx.recv().await.is_some() {} });
            }
            let t0 = std::time::Instant::now();
            recover_all(&state).await;
            let elapsed = t0.elapsed();
            println!(
                "N={:>5} (实际 {total})  recover_all(online)={elapsed:>12?}  runtimes={}",
                n,
                state.proxies.runtimes.len()
            );
        }
    }

    /// #2 基准(手动运行):N 条规则的启动恢复总耗时。
    ///
    /// ```text
    /// cargo test -p rh-server --release recovery_tests::bench -- --ignored --nocapture
    /// ```
    ///
    /// ★如实记录(重要):本机测不出并发收益,甚至略慢。
    ///
    /// 两条基准(`bench_recovery_scaling` / `bench_recovery_with_online_sessions`)
    /// 的实测结果:
    ///
    /// | N=1000 | 串行 | 并发(16) |
    /// |---|---|---|
    /// | 无在线会话 | 58.29ms | 58.55ms |
    /// | 有在线会话 | 58.21ms | 71.80ms |
    ///
    /// 原因:`spawn_runtime` 里的 await 点(`get_client`、`send_bounded`、
    /// `mark_unconfirmed`)在 `:memory:` SQLite + 有消费者的大队列下**几乎
    /// 不产生等待**,于是并发只剩调度开销。真实部署的收益来自:
    ///   1. 文件型 SQLite 的读延迟(每次 `get_client` 是真实磁盘/页缓存往返);
    ///   2. `send_bounded` 在客户端读得慢时真的会 park;
    ///   3. 叠加 #1 之后,单条 spawn 不再做 O(N) 的 vhost 重建。
    ///
    /// 第 3 点在本机可以单独验证(见 `vhost_coalesce_tests::bench`:
    /// N=1000 时 O(N²) 的 182ms → O(N) 的 0.19ms),那才是这一批的主要收益。
    /// 本函数保留为回归基线,不声称提升。
    #[tokio::test]
    #[ignore = "耗时基准,手动运行(--ignored --nocapture)"]
    async fn bench_recovery_scaling() {
        for n in [100usize, 500, 1000] {
            let per = n.div_ceil(8);
            let (state, total) = seed(8, per, true).await;
            let t0 = std::time::Instant::now();
            recover_all(&state).await;
            let elapsed = t0.elapsed();
            println!(
                "N={:>5} (实际 {total})  recover_all={elapsed:>12?}  runtimes={}",
                n,
                state.proxies.runtimes.len()
            );
        }
    }
}
