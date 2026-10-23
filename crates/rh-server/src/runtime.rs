//! 运行时状态机:会话上下线、确认推进、健康上报、规则生命周期。
//! 设计 §4.1 状态转换表为实现口径。

use crate::session::Session;
use crate::state::{AppState, ProxyRuntime, ProxyState, TcpPool};
use rh_protocol::message::{
    ApplyResult, HealthReport, ServerMessage, ServiceSync, ServiceUpdate, SyncEntity,
};
use rh_protocol::{ProxyKind, ProxySpec};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

/// 控制面发送上限(与 state.rs 同名常量同义):超时按"对端不读"处理。
///
/// `pub(crate)` 供 api.rs 的 admin handler 复用 —— 那里 11 处裸 `send`
/// 要从"队列满时永久 park"降级为"最多等 2s"。
pub(crate) const CONTROL_SEND_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);

/// 会话建立:重置确认、状态推进、预热。
pub async fn on_session_up(state: &Arc<AppState>, sess: &Arc<Session>) {
    let proxies = match state.db.list_proxies(Some(&sess.client_id)).await {
        Ok(p) => p,
        Err(_) => return,
    };
    for (_id, _cid, spec) in proxies {
        let Some(rt) = state.proxies.get(spec.id) else {
            continue;
        };
        // confirmed 重置(会话替换/重连)
        rt.confirmed_revision.store(0, Ordering::Relaxed);
        let (st, _r) = rt.snapshot_state().await;
        let new_state = match st {
            ProxyState::Offline
            | ProxyState::Pending
            | ProxyState::ApplyError
            | ProxyState::Unhealthy => ProxyState::Pending,
            ProxyState::BindError | ProxyState::Disabled => st,
            ProxyState::Running => ProxyState::Pending, // 等新会话确认
        };
        rt.set_state(new_state, "session up").await;
    }
}

/// 会话断开:关池与通道,状态落 offline。
///
/// B2:`on_session_up`(注册新会话)持 `serial_lock(client_id)`,本函数也
/// 必须持同一把锁:否则"判定未被替换 → 新会话注册 → 本函数写 Offline"
/// 的交错会把刚上线的规则永久钉在 Offline(没有后续事件自愈)。
pub async fn on_session_down(state: &Arc<AppState>, sess: &Arc<Session>) {
    let lock = state.serial_lock(&sess.client_id);
    let _g = lock.lock().await;
    let proxies = match state.db.list_proxies(Some(&sess.client_id)).await {
        Ok(p) => p,
        Err(_) => return,
    };
    // 会话死亡:该客户端全部 SUDP 常驻中继槽位一并清理
    if let Ok(visitors) = state.db.list_visitors(Some(&sess.client_id)).await {
        let mut relays = state.udp_relays.lock().await;
        for (_vid, _, _, vspec) in &visitors {
            relays.remove(&vspec.id);
        }
    }
    // 若新会话已注册接管,本会话的清理不得波及新会话的池/通道
    let replaced = state
        .sessions
        .get(&sess.client_id)
        .map(|cur| cur.session_id != sess.session_id)
        .unwrap_or(false);
    if replaced {
        return;
    }
    for (_id, _cid, spec) in proxies {
        let Some(rt) = state.proxies.get(spec.id) else {
            continue;
        };
        rt.confirmed_revision.store(0, Ordering::Relaxed);
        crate::data::close_proxy_channels(&rt).await;
        let (st, _) = rt.snapshot_state().await;
        let new_state = match st {
            ProxyState::Disabled | ProxyState::BindError => st,
            _ => ProxyState::Offline,
        };
        rt.set_state(new_state, "session down").await;
    }
}

/// APPLY_RESULT:推进确认与状态;预热 TCP 池 / 建立 UDP 通道。
pub async fn on_apply_result(state: &Arc<AppState>, sess: &Arc<Session>, r: &ApplyResult) {
    if !r.ok {
        if let Some(err) = &r.error {
            warn!(reason = %err.reason, "apply error from client");
            // 定位规则(属主校验:客户端只能上报自己名下规则的状态)
            if let Some(pid) = err.service_id {
                if let Some(rt) = state.proxies.get(pid) {
                    if rt.client_id == sess.client_id {
                        rt.set_state(ProxyState::ApplyError, &err.reason).await;
                    } else {
                        warn!(proxy = pid, client = %sess.client_id, owner = %rt.client_id,
                            "apply error reported for foreign proxy; ignored");
                    }
                }
            }
        }
        sess.confirm_applied(r.revision, false).await;
        // 全量重发。
        //
        // ★try_send:本函数由控制读循环**内联** await(control.rs:966),
        // 而那个循环同时是 `queue_tx` 的唯一消费者。队列满了再用
        // `send().await` 就会 park 在"等自己消费"上 —— A1 描述的经典自锁。
        // 队列满说明客户端已停读,此刻重发 SYNC 无意义。
        let entities = state.build_sync_entities(&sess.client_id).await;
        let client = state.db.get_client(&sess.client_id).await.ok().flatten();
        let rev = client.map(|c| c.config_revision.max(0) as u64).unwrap_or(0);
        sess.mark_unconfirmed(rev).await;
        if !sess.try_send_msg(ServerMessage::ServiceSync(ServiceSync {
            revision: rev,
            entities,
        })) {
            debug!(
                client_id = %sess.client_id,
                "service sync (full resend) dropped: control queue full"
            );
        }
        return;
    }
    let still_waiting = sess.confirm_applied(r.revision, true).await;
    let _ = still_waiting;
    // 规则状态推进:confirmed ≥ latest → running
    let proxies = state
        .db
        .list_proxies(Some(&sess.client_id))
        .await
        .unwrap_or_default();
    for (_id, _cid, spec) in proxies {
        let Some(rt) = state.proxies.get(spec.id) else {
            continue;
        };
        rt.confirmed_revision.store(r.revision, Ordering::Relaxed);
        let latest = rt.latest_revision.load(Ordering::Relaxed);
        let (st, _) = rt.snapshot_state().await;
        let confirmed_enough = r.revision >= latest;
        let bound = rt.bound.load(Ordering::Relaxed);
        let new_state = match st {
            ProxyState::Pending if confirmed_enough && bound => ProxyState::Running,
            ProxyState::ApplyError if confirmed_enough && bound => ProxyState::Running,
            other => other,
        };
        if new_state != st {
            rt.set_state(new_state, "applied").await;
            if new_state == ProxyState::Running {
                warm_proxy(state, sess, &rt, &spec).await;
            }
        } else if st == ProxyState::Running {
            // 已 running 的补充预热(如重连后)
            warm_proxy(state, sess, &rt, &spec).await;
        }
    }
}

/// 进入 running 后:TCP 池预热 / UDP 通道建立。
pub async fn warm_proxy(
    state: &Arc<AppState>,
    sess: &Arc<Session>,
    rt: &Arc<ProxyRuntime>,
    spec: &ProxySpec,
) {
    match spec.kind {
        ProxyKind::Tcp
        | ProxyKind::Http
        | ProxyKind::Https
        | ProxyKind::TcpMux
        | ProxyKind::Stcp => {
            crate::data::tcp_refill(state, sess, rt).await;
        }
        ProxyKind::Udp => {
            crate::data::udp_ensure_channel(state, sess, rt).await;
        }
        ProxyKind::Sudp => {} // 常驻通道由 visitor 中继按需建立(RelayProxy)
        ProxyKind::Xtcp => {} // 打洞按需
    }
}

/// 健康上报。
pub async fn on_health_report(state: &Arc<AppState>, sess: &Arc<Session>, h: HealthReport) {
    let Some(rt) = state.proxies.get(h.proxy_id) else {
        return;
    };
    // 属主校验:客户端只能影响自己名下规则的健康状态
    if rt.client_id != sess.client_id {
        warn!(proxy = h.proxy_id, client = %sess.client_id, owner = %rt.client_id,
            "health report for foreign proxy; ignored");
        return;
    }
    rt.healthy.store(h.healthy, Ordering::Relaxed);
    let (st, _) = rt.snapshot_state().await;
    let new_state = if h.healthy {
        if st == ProxyState::Unhealthy {
            ProxyState::Running
        } else {
            st
        }
    } else if st == ProxyState::Running {
        ProxyState::Unhealthy
    } else {
        st
    };
    if new_state != st {
        rt.set_state(
            new_state,
            &if h.healthy {
                "healthy".into()
            } else {
                format!("unhealthy: {}", h.reason)
            },
        )
        .await;
        state
            .events
            .emit(
                if h.healthy { "info" } else { "warn" },
                "service",
                &format!(
                    "规则 {} 健康检查{}",
                    h.proxy_id,
                    if h.healthy { "正常" } else { "失败" }
                ),
            )
            .await;
    }
    // 组成员健康变化影响选路(实时读取 healthy)
}

// ---------------- 规则生命周期(串行域内调用) ----------------

/// 运行态创建/重建规则(禁用状态仅返回 runtime 不 bind)。
pub async fn spawn_runtime(
    state: &Arc<AppState>,
    client_id: &str,
    spec: &ProxySpec,
    revision: u64,
) -> Arc<ProxyRuntime> {
    if let Some(existing) = state.proxies.get(spec.id) {
        existing.rule_cancel.cancel();
    }
    let rt = Arc::new(ProxyRuntime::new(
        spec.id,
        client_id.to_string(),
        spec.clone(),
    ));
    rt.latest_revision.store(revision, Ordering::Relaxed);
    state.proxies.runtimes.insert(spec.id, rt.clone());
    let session = state.sessions.get(client_id);
    let online = session.is_some();
    let enabled_client = state
        .db
        .get_client(client_id)
        .await
        .map(|c| c.map(|c| c.enabled).unwrap_or(false))
        .unwrap_or(false);

    if !spec.enabled || !enabled_client {
        rt.set_state(ProxyState::Disabled, "disabled").await;
        // 走合并窗口:批量禁用 N 条规则时,这里是 O(N²) 的主要来源之一。
        state.proxies.schedule_vhost_rebuild();
        return rt;
    }

    // 绑定监听(tcp/udp 独享端口);无独享端口类型视为已绑定
    let bind_ip: std::net::IpAddr =
        rh_core::net::parse_bind_ip(&spec.bind_ip).unwrap_or(std::net::IpAddr::from([0, 0, 0, 0]));
    let mut bind_error = false;
    // 白名单资格同样适用于恢复/批量重建路径(§4.6 与创建共用资格判定)
    if spec.kind.uses_bind_port()
        && spec.bind_port != 0
        && !state.proxies.port_allowed(spec.bind_port).await
    {
        bind_error = true;
        rt.set_state(ProxyState::BindError, "port not in allow_ports")
            .await;
        let st = state.clone();
        let rt2 = rt.clone();
        tokio::spawn(bind_error_retry(st, rt2));
    }
    match spec.kind {
        ProxyKind::Tcp => match tcp_bind_and_serve(state, &rt, spec, bind_ip).await {
            Ok(()) => {}
            Err(e) => {
                bind_error = true;
                rt.set_state(ProxyState::BindError, &format!("bind: {e}"))
                    .await;
                let st = state.clone();
                let rt2 = rt.clone();
                tokio::spawn(bind_error_retry(st, rt2));
            }
        },
        ProxyKind::Udp => match crate::data::bind_udp_socket(state, spec, bind_ip).await {
            Ok(sock) => {
                rt.bound.store(true, Ordering::Relaxed);
                *rt.udp_socket.lock().await = Some(sock.clone());
                let st2 = state.clone();
                let rt2 = rt.clone();
                tokio::spawn(crate::data::udp_serve_loop(st2, rt2, sock));
            }
            Err(e) => {
                bind_error = true;
                rt.set_state(ProxyState::BindError, &format!("bind: {e}"))
                    .await;
                let st = state.clone();
                let rt2 = rt.clone();
                tokio::spawn(bind_error_retry(st, rt2));
            }
        },
        // http/https/tcpmux/stcp/sudp/xtcp:无独享端口,绑定条件恒真
        _ => rt.bound.store(true, Ordering::Relaxed),
    }
    // 组成员登记(tcp/vhost;同组成员共享监听与轮询选路)
    if let Some(key) = crate::state::ProxyManager::group_key(spec) {
        let g = state.proxies.group_entry(&key);
        let mut m = g.members.write().await;
        m.retain(|id| *id != spec.id);
        m.push(spec.id);
    }
    // 初始池
    if matches!(
        spec.kind,
        ProxyKind::Tcp | ProxyKind::Http | ProxyKind::Https | ProxyKind::TcpMux | ProxyKind::Stcp
    ) {
        *rt.pool.lock().await = Some(Arc::new(TcpPool::new(spec.pool_size.max(1) as usize)));
    }
    // 状态:bind_error 保持(§4.1:期间照常收集客户端确认);否则按在线与否
    if !bind_error {
        if online {
            rt.set_state(ProxyState::Pending, "created").await;
        } else {
            rt.set_state(ProxyState::Offline, "created").await;
        }
    }
    state.proxies.schedule_vhost_rebuild();
    // 入队增量(在线时;bind_error 也入队,恢复绑定后按 confirmed 直达 running)
    if let Some(sess) = session {
        rt.latest_revision.store(revision, Ordering::Relaxed);
        sess.mark_unconfirmed(revision).await;
        // 有界发送:客户端零窗口黑洞时,`send().await` 会把 admin API 请求
        // 挂到 TCP 超时(15~20 分钟)。统一用 2s 上限,超时按"对方不读"处理。
        let _ = sess
            .send_bounded(
                ServerMessage::ServiceUpdate(Box::new(ServiceUpdate {
                    revision,
                    entity: SyncEntity::Proxy(Box::new(spec.clone())),
                })),
                CONTROL_SEND_TIMEOUT,
            )
            .await;
    }
    rt
}

/// TCP 绑定 + serve loop 启动(组感知)。
/// 组成员:共享监听已在本组绑定时直接标记 bound;否则由本成员绑定并启动
/// 组共享 serve loop(取消权归组,单个成员退出不影响监听)。
async fn tcp_bind_and_serve(
    state: &Arc<AppState>,
    rt: &Arc<ProxyRuntime>,
    spec: &ProxySpec,
    bind_ip: std::net::IpAddr,
) -> std::io::Result<()> {
    let key = crate::state::ProxyManager::group_key(spec);
    if let Some(k) = key {
        let g = state.proxies.group_entry(&k);
        if g.listener_bound
            .swap(true, std::sync::atomic::Ordering::Relaxed)
        {
            // 共享监听已在位(同组其他成员持有)
            rt.bound.store(true, Ordering::Relaxed);
            return Ok(());
        }
        // 本成员负责绑定:换新取消令牌(组曾清空时旧令牌已取消)
        let token = {
            let fresh = CancellationToken::new();
            let mut c = g.serve_cancel.write().await;
            *c = fresh.clone();
            fresh
        };
        let l = crate::data::bind_tcp_listener(state, spec, bind_ip).await?;
        rt.bound.store(true, Ordering::Relaxed);
        let st2 = state.clone();
        let rt2 = rt.clone();
        // ★A6:共享 serve loop 的退出条件必须是**组级** serve_cancel。
        //
        // 修前这里传的是 rt2(属主成员),而 tcp_serve_loop 内部 select 的是
        // `rt.rule_cancel` —— 属主被禁用/改端口即无条件 cancel,监听 fd 释放,
        // 但 listener_bound 仍为 true → 其余成员 spawn 时跳过重绑 →
        // 组端口永久拒绝连接。组监听属于组,不属于任何单个成员。
        let group_cancel = token.clone();
        tokio::spawn(async move {
            crate::data::tcp_serve_loop_group(st2, rt2, l, group_cancel).await;
        });
        return Ok(());
    }
    // 独享端口
    let l = crate::data::bind_tcp_listener(state, spec, bind_ip).await?;
    rt.bound.store(true, Ordering::Relaxed);
    *rt.tcp_listener.lock().await = marker_listener();
    let rt2 = rt.clone();
    let st2 = state.clone();
    // A6 顺带:记录句柄,供下次重建前收敛(避免 EADDRINUSE 窗口)。
    let h = tokio::spawn(async move {
        crate::data::tcp_serve_loop(st2, rt2, l).await;
    });
    *rt.serve_handle.lock().await = Some(h);
    Ok(())
}

/// bind_error 周期重试(60s);port_auto 立即重分配由调用方处理。
async fn bind_error_retry(state: Arc<AppState>, rt: Arc<ProxyRuntime>) {
    loop {
        tokio::select! {
            _ = rt.rule_cancel.cancelled() => return,
            _ = tokio::time::sleep(std::time::Duration::from_secs(60)) => {}
        }
        let spec = rt.spec.read().await.clone();
        if !spec.enabled || !spec.kind.uses_bind_port() && spec.kind != ProxyKind::Tcp {
            continue;
        }
        let bind_ip: std::net::IpAddr = rh_core::net::parse_bind_ip(&spec.bind_ip)
            .unwrap_or(std::net::IpAddr::from([0, 0, 0, 0]));
        // 拉起 serve loop(首次绑定失败时从未启动);组感知
        let bound = if spec.kind == ProxyKind::Tcp {
            tcp_bind_and_serve(&state, &rt, &spec, bind_ip)
                .await
                .is_ok()
        } else {
            match crate::data::bind_udp_socket(&state, &spec, bind_ip).await {
                Ok(sock) => {
                    let st2 = state.clone();
                    let rt2 = rt.clone();
                    tokio::spawn(crate::data::udp_serve_loop(st2, rt2, sock));
                    true
                }
                Err(_) => false,
            }
        };
        if !bound {
            continue;
        }
        rt.bound.store(true, Ordering::Relaxed);
        // 按会话与确认推进(§4.1 bind_error 恢复行)
        if let Some(sess) = state.sessions.get(&rt.client_id) {
            let confirmed = rt.confirmed_revision.load(Ordering::Relaxed);
            let latest = rt.latest_revision.load(Ordering::Relaxed);
            if confirmed >= latest && latest > 0 {
                rt.set_state(ProxyState::Running, "bind recovered").await;
                warm_proxy(&state, &sess, &rt, &spec).await;
            } else {
                // 未确认:入队增量等待确认后转 running
                rt.set_state(ProxyState::Pending, "bind recovered").await;
                let rev = latest.max(1);
                sess.mark_unconfirmed(rev).await;
                let _ = sess
                    .send_bounded(
                        ServerMessage::ServiceUpdate(Box::new(ServiceUpdate {
                            revision: rev,
                            entity: SyncEntity::Proxy(Box::new(spec.clone())),
                        })),
                        CONTROL_SEND_TIMEOUT,
                    )
                    .await;
            }
        } else {
            rt.set_state(ProxyState::Offline, "bind recovered").await;
        }
        state
            .events
            .emit(
                "info",
                "service",
                &format!("规则 {} 端口绑定已恢复", spec.name),
            )
            .await;
        return;
    }
}

/// 移除运行态(取消域级联)。
pub async fn teardown_runtime(state: &Arc<AppState>, proxy_id: u64) {
    if let Some((_, rt)) = state.proxies.runtimes.remove(&proxy_id) {
        // 组成员退出(组清空时取消共享监听)
        let key = crate::state::ProxyManager::group_key(&*rt.spec.read().await);
        if let Some(k) = key {
            state.proxies.leave_group(&k, proxy_id).await;
        }
        rt.rule_cancel.cancel();
        // A6 顺带:等 serve loop 真正退出(带超时),再让调用方去重建。
        // 否则旧 loop 尚未释放监听 fd,新绑定偶发 EADDRINUSE。
        if let Some(h) = rt.serve_handle.lock().await.take() {
            if tokio::time::timeout(std::time::Duration::from_secs(2), h)
                .await
                .is_err()
            {
                tracing::warn!(
                    proxy = proxy_id,
                    "serve loop did not converge within 2s during teardown"
                );
            }
        }
        crate::data::close_proxy_channels(&rt).await;
        // 失效该规则在途 CREATE 请求:迟到回显不得进入新运行时(§4.5 代次隔离)
        if let Some(sess) = state.sessions.get(&rt.client_id) {
            sess.invalidate_proxy_requests(proxy_id).await;
        }
    }
    // 走合并窗口:`delete_client` / `update_client` 禁用分支 /
    // `batch_services` 都是「循环内 teardown N 次」,这里每次重建就是 O(N²)。
    state.proxies.schedule_vhost_rebuild();
}

pub async fn notify_client_recovery(state: &Arc<AppState>, client_id: &str) {
    // 客户端重新 enable:全部规则批量恢复
    let proxies = state
        .db
        .list_proxies(Some(client_id))
        .await
        .unwrap_or_default();
    let client = state.db.get_client(client_id).await.ok().flatten();
    let Some(c) = client else { return };
    let revision = c.config_revision.max(0) as u64;
    for (_id, _cid, mut spec) in proxies {
        if !spec.enabled {
            continue;
        }
        {
            let l = state.limits.read().unwrap_or_else(|e| e.into_inner());
            crate::db::normalize_spec(&mut spec, l.default_pool_size, l.max_pool_size);
        }
        let _rt = spawn_runtime(state, client_id, &spec, revision).await;
        info!(proxy = spec.name, "recovered after client enable");
    }
    // visitors 重发(全量 SYNC 更简单且正确)
    if let Some(sess) = state.sessions.get(client_id) {
        let entities = state.build_sync_entities(client_id).await;
        sess.mark_unconfirmed(revision).await;
        let _ = sess
            .send_bounded(
                ServerMessage::ServiceSync(ServiceSync { revision, entities }),
                CONTROL_SEND_TIMEOUT,
            )
            .await;
    }
}

/// 占位 listener(表示"已绑定"状态;真正监听由 serve loop 持有)。
fn marker_listener() -> Option<tokio::net::TcpListener> {
    None
}

/// 非端口字段变更的原位发布(§4.3):listener/池不动,仅更新运行态快照并入队。
/// 返回 false 表示不适用(端口/类型变更或运行时缺失),调用方走重建路径。
pub async fn publish_spec_in_place(
    state: &Arc<AppState>,
    rt: &Arc<ProxyRuntime>,
    spec: &ProxySpec,
    revision: u64,
) -> bool {
    *rt.spec.write().await = spec.clone();
    // 与 spec 同步维护无锁快路径标记(见 ProxyRuntime::has_group)
    rt.has_group.store(
        crate::state::ProxyManager::group_key(spec).is_some(),
        Ordering::Relaxed,
    );
    rt.latest_revision.store(revision, Ordering::Relaxed);
    // 规则变更:上游连接池里的连接可能仍指向旧目标(改 target_host /
    // target_port 后复用旧连接会把请求转发到错误后端)。整池丢弃,
    // 后续请求重建。代价是一次冷启动,正确性优先。
    rt.http_upstream.clear();
    if let Some(sess) = state.sessions.get(&rt.client_id) {
        sess.mark_unconfirmed(revision).await;
        // 有界发送:客户端零窗口黑洞时,`send().await` 会把 admin API 请求
        // 挂到 TCP 超时(15~20 分钟)。统一用 2s 上限,超时按"对方不读"处理。
        let _ = sess
            .send_bounded(
                ServerMessage::ServiceUpdate(Box::new(ServiceUpdate {
                    revision,
                    entity: SyncEntity::Proxy(Box::new(spec.clone())),
                })),
                CONTROL_SEND_TIMEOUT,
            )
            .await;
        let (st, _) = rt.snapshot_state().await;
        if st == ProxyState::Running {
            rt.set_state(ProxyState::Pending, "updated").await;
        }
    }
    // 走合并窗口(单条更新也要重建,但同样可能落在批量路径里)。
    state.proxies.schedule_vhost_rebuild();
    true
}
