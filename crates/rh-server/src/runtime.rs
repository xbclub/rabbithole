//! 运行时状态机:会话上下线、确认推进、健康上报、规则生命周期。
//! 设计 §4.1 状态转换表为实现口径。

use crate::config::CONTROL_SEND_TIMEOUT;
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

/// 会话建立:重置确认、状态推进、预热。
pub async fn on_session_up(state: &Arc<AppState>, sess: &Arc<Session>) {
    // R3(B1):数据源改 by-client 索引(on_apply_result B9 同款:旧代码全量
    // 读 DB + 逐行 spec 反序列化,循环内却只消费 spec.id —— 每次连接建立/
    // 断开都在付 N 行 spec blob 传输与解析,重连风暴下翻倍)。
    let rts: Vec<Arc<crate::state::ProxyRuntime>> =
        match state.proxies.runtimes_by_client.get(&sess.client_id) {
            Some(ids) => ids
                .iter()
                .filter_map(|id| state.proxies.runtimes.get(id).map(|e| e.value().clone()))
                .collect(),
            None => state
                .proxies
                .runtimes
                .iter()
                .filter(|e| e.value().client_id == sess.client_id)
                .map(|e| e.value().clone())
                .collect(),
        };
    for rt in rts {
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
    // R3(B1):同 on_session_up —— 索引取代全量 DB 读(此处还在
    // serial_lock 持有窗口内,DB 读会延长串行域临界区)。
    let rts: Vec<Arc<crate::state::ProxyRuntime>> =
        match state.proxies.runtimes_by_client.get(&sess.client_id) {
            Some(ids) => ids
                .iter()
                .filter_map(|id| state.proxies.runtimes.get(id).map(|e| e.value().clone()))
                .collect(),
            None => state
                .proxies
                .runtimes
                .iter()
                .filter(|e| e.value().client_id == sess.client_id)
                .map(|e| e.value().clone())
                .collect(),
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
    for rt in rts {
        rt.confirmed_revision.store(0, Ordering::Relaxed);
        // C6c:IntGaugeVec 子项按 proxy_id 回收(修前只增不减,长跑 +
        // 频繁建删规则时 /metrics 输出与内存无界增长)。
        let _ = state
            .metrics
            .connections
            .remove_label_values(&[&rt.metrics_label, "tcp"]);
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
        // N7:全量重发限速(1s;与 SyncRequest 分支的 last_sync_request
        // 同款)。修前每个失败 ACK 都全量重建实体 + 全量序列化,N 条
        // 规则全失败 = O(N²) 序列化压力,内联在读循环里。
        {
            let mut last = sess.last_failed_resync.lock().await;
            let now = std::time::Instant::now();
            if let Some(prev) = *last {
                if now.duration_since(prev) < std::time::Duration::from_secs(1) {
                    return;
                }
            }
            *last = Some(now);
        }
        // 全量重发。
        //
        // ★try_send:本函数由控制读循环**内联** await(control.rs:966),
        // 而那个循环同时是 `queue_tx` 的唯一消费者。队列满了再用
        // `send().await` 就会 park 在"等自己消费"上 —— A1 描述的经典自锁。
        // 队列满说明客户端已停读,此刻重发 SYNC 无意义。
        let entities = state.build_sync_entities(&sess.client_id).await;
        let client = state.db.get_client(&sess.client_id).await.ok().flatten();
        let rev = client.map(|c| c.config_revision.max(0) as u64).unwrap_or(0);
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
    sess.confirm_applied(r.revision, true).await;
    // 规则状态推进:confirmed ≥ latest → running。
    //
    // B9:数据源改为内存 runtimes(按 client_id 过滤)。修前每个 ACK 都把
    // 该客户端全部 N 条规则从 DB 读出并逐行反序列化 ProxySpec —— 客户端
    // 对每条 ServiceUpdate 回一个 ApplyResult,批量改 N 条规则就是 O(N²)
    // 的 DB 反序列化,且发生在控制读循环内联路径上。运行时集合与
    // 「DB 行 ∩ 已建运行时」等价(旧代码对无运行时的行 continue 跳过)。
    // R2:优先走 by-client 索引(测试直接 insert 的运行态无索引条目,
    // 回退全表扫描)。索引可能略滞后,取不到的 id 由 get 跳过。
    let rts: Vec<Arc<crate::state::ProxyRuntime>> =
        match state.proxies.runtimes_by_client.get(&sess.client_id) {
            Some(ids) => ids
                .iter()
                .filter_map(|id| state.proxies.runtimes.get(id).map(|e| e.value().clone()))
                .collect(),
            None => state
                .proxies
                .runtimes
                .iter()
                .filter(|e| e.value().client_id == sess.client_id)
                .map(|e| e.value().clone())
                .collect(),
        };
    for rt in rts {
        rt.confirmed_revision.store(r.revision, Ordering::Relaxed);
        let latest = rt.latest_revision.load(Ordering::Relaxed);
        // N12:state_kind() 只取枚举(snapshot_state 连带 clone reason
        // String,本循环每 ACK × 每规则都在付)。
        let st = rt.state_kind().await;
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
                rt.mark_warm_session(sess);
                let spec = rt.spec.read().await.clone();
                warm_proxy(state, sess, &rt, &spec).await;
            }
        } else if st == ProxyState::Running {
            // 已 running 的补充预热(重连后);N4:按会话实例去重 ——
            // 客户端对每条 ServiceUpdate 各回一个 ACK,修前每个 ACK 都
            // 深克隆 spec + warm_proxy,批量 N 条 = O(N²) 次克隆+预热。
            // 池满时 tcp_refill 为无签发 no-op,但 spec 深克隆省不掉。
            if rt.mark_warm_session(sess) {
                let spec = rt.spec.read().await.clone();
                warm_proxy(state, sess, &rt, &spec).await;
            }
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
    // R2:by-client 索引(spawn 生命周期点)
    state
        .proxies
        .runtimes_by_client
        .entry(client_id.to_string())
        .or_default()
        .push(spec.id);
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
        // 有界发送:客户端零窗口黑洞时,`send().await` 会把 admin API 请求
        // 挂到 TCP 超时(15~20 分钟)。统一用 2s 上限,超时按"对方不读"处理。
        let _ = sess
            .send_bounded(
                ServerMessage::ServiceUpdate(Box::new(ServiceUpdate {
                    revision,
                    entity: SyncEntity::Proxy(Box::new((*spec).clone())),
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
                let _ = sess
                    .send_bounded(
                        ServerMessage::ServiceUpdate(Box::new(ServiceUpdate {
                            revision: rev,
                            entity: SyncEntity::Proxy(Box::new((*spec).clone())),
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
        // R2:by-client 索引摘除(teardown 生命周期点;按值清全部出现,
        // 防御重复 push)
        if let Some(mut e) = state.proxies.runtimes_by_client.get_mut(&rt.client_id) {
            e.retain(|&id| id != proxy_id);
        }
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
        // C6c:IntGaugeVec 子项按 proxy_id 回收(修前只增不减,长跑 +
        // 频繁建删规则时 /metrics 输出与内存无界增长)。
        let _ = state
            .metrics
            .connections
            .remove_label_values(&[&rt.metrics_label, "tcp"]);
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
    // ★并发恢复:形态与 `recovery.rs` 的启动恢复完全同构(那里已经用
    // `for_each_concurrent(RECOVERY_CONCURRENCY)`)。修前这里是 N 次串行
    // `spawn_runtime` —— 每条含 1 次 `get_client` + bind + serve loop 启动
    // + mpsc 发送,单客户端 N 条规则就是 N 倍单条延迟。
    //
    // ★为什么比 `recovery.rs` 更安全:那里要处理 `port_auto` 被外部占用时
    // 的 `allocate_port` 重分配,所以必须把端口决策留在串行段;
    // 本函数**完全没有** `allocate_port` 调用 —— 端口直接取 DB 里已有的
    // `spec.bind_port`。DB 里同一客户端的端口本来就不重复,所以不存在
    // 端口竞态,整个循环都可以并发。
    //
    // 自锁核查:调用点 `update_client` 持有 `serial_lock(&id)`,而
    // `spawn_runtime` 内部不取 `serial_lock`(grep 结果为 0),不会自锁。
    //
    // 注:`state.limits.read()` 是 `std::sync::RwLock`,**不能跨 await 持有** ——
    // 这里在块内取用完立即释放,与 `recovery.rs` 同写法。
    use crate::recovery::RECOVERY_CONCURRENCY;
    use futures::stream::StreamExt as _;
    futures::stream::iter(proxies)
        .filter(|(_, _, spec)| futures::future::ready(spec.enabled))
        .for_each_concurrent(RECOVERY_CONCURRENCY, |(_id, _cid, mut spec)| {
            let state = state.clone();
            let client_id = client_id.to_string();
            async move {
                {
                    let l = state.limits.read().unwrap_or_else(|e| e.into_inner());
                    crate::db::normalize_spec(&mut spec, l.default_pool_size, l.max_pool_size);
                }
                let _rt = spawn_runtime(&state, &client_id, &spec, revision).await;
                info!(proxy = spec.name, "recovered after client enable");
            }
        })
        .await;
    // visitors 重发(全量 SYNC 更简单且正确)
    if let Some(sess) = state.sessions.get(client_id) {
        let entities = state.build_sync_entities(client_id).await;
        let _ = sess
            .send_bounded(
                ServerMessage::ServiceSync(ServiceSync { revision, entities }),
                CONTROL_SEND_TIMEOUT,
            )
            .await;
    }
}

/// 非端口字段变更的原位发布(§4.3):listener/池不动,仅更新运行态快照并入队。
/// 返回 false 表示不适用(端口/类型变更或运行时缺失),调用方走重建路径。
pub async fn publish_spec_in_place(
    state: &Arc<AppState>,
    rt: &Arc<ProxyRuntime>,
    spec: &ProxySpec,
    revision: u64,
) -> bool {
    *rt.spec.write().await = std::sync::Arc::new(spec.clone());
    // 与 spec 同步维护无锁快路径标记(见 ProxyRuntime::has_group)
    // 与派生组键缓存(R2:group_pick* 直读)
    let key = crate::state::ProxyManager::group_key(spec);
    rt.has_group.store(key.is_some(), Ordering::Relaxed);
    *rt.group_key_cache.lock().unwrap_or_else(|e| e.into_inner()) = key;
    rt.latest_revision.store(revision, Ordering::Relaxed);
    // 规则变更:上游连接池里的连接可能仍指向旧目标(改 target_host /
    // target_port 后复用旧连接会把请求转发到错误后端)。整池丢弃,
    // 后续请求重建。代价是一次冷启动,正确性优先。
    rt.http_upstream.clear();
    if let Some(sess) = state.sessions.get(&rt.client_id) {
        // 有界发送:客户端零窗口黑洞时,`send().await` 会把 admin API 请求
        // 挂到 TCP 超时(15~20 分钟)。统一用 2s 上限,超时按"对方不读"处理。
        let _ = sess
            .send_bounded(
                ServerMessage::ServiceUpdate(Box::new(ServiceUpdate {
                    revision,
                    entity: SyncEntity::Proxy(Box::new((*spec).clone())),
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

#[cfg(test)]
mod notify_client_recovery_tests {
    //! P4 回归:`notify_client_recovery` 的批量恢复并发化后,
    //! 结果必须与串行版本完全一致。
    //!
    //! ★能力边界(如实记录):这条**测不出并发收益**。理由与报告 §2 的
    //! recovery 基准相同 —— `spawn_runtime` 的 await 点在内存 SQLite 且
    //! 无在线背压时几乎不等待,并发只剩调度开销。真实收益依赖文件型
    //! SQLite 与在线会话。所以这里只做**功能性回归**:全部启用规则被恢复、
    //! 禁用规则被跳过、visitor SYNC 正常下发。
    use super::*;

    const CID: &str = "recovery-conc-client";

    fn spec(name: &str, port: u16, enabled: bool) -> ProxySpec {
        serde_json::from_value(serde_json::json!({
            "name": name,
            "kind": "stcp",
            "client_id": CID,
            "bind_ip": "127.0.0.1",
            "bind_port": port,
            "target_host": "127.0.0.1",
            "target_port": 9,
            "enabled": enabled,
        }))
        .unwrap()
    }

    /// 全部启用规则必须被恢复;禁用规则一条都不恢复。
    #[tokio::test]
    async fn recovers_all_enabled_and_skips_disabled() {
        let db = crate::db::Db::open("sqlite::memory:").await.unwrap();
        db.create_client(CID, "c", &"0".repeat(64), "")
            .await
            .unwrap();
        const ON: usize = 20;
        const OFF: usize = 5;
        for i in 0..ON {
            db.insert_proxy(
                CID,
                &spec(&format!("on{i}"), 25000 + i as u16, true),
                false,
                "console",
            )
            .await
            .unwrap();
        }
        for i in 0..OFF {
            db.insert_proxy(
                CID,
                &spec(&format!("off{i}"), 25100 + i as u16, false),
                false,
                "console",
            )
            .await
            .unwrap();
        }
        let state = Arc::new(AppState::new(crate::config::ServerConfig::default(), db));

        notify_client_recovery(&state, CID).await;

        assert_eq!(
            state.proxies.runtimes.len(),
            ON,
            "必须恰好恢复 {ON} 条启用规则(禁用的一条都不该进来)"
        );
        // 每条都必须是启用状态
        let rts: Vec<Arc<ProxyRuntime>> = state
            .proxies
            .runtimes
            .iter()
            .map(|e| e.value().clone())
            .collect();
        for rt in rts {
            let enabled = rt.spec.read().await.enabled;
            assert!(enabled, "恢复出来的必须都是启用的");
        }
    }

    /// `revision` 必须写入每条恢复出来的 runtime。
    #[tokio::test]
    async fn revision_propagated() {
        let db = crate::db::Db::open("sqlite::memory:").await.unwrap();
        db.create_client(CID, "c", &"0".repeat(64), "")
            .await
            .unwrap();
        for i in 0..8 {
            db.insert_proxy(
                CID,
                &spec(&format!("p{i}"), 25200 + i as u16, true),
                false,
                "console",
            )
            .await
            .unwrap();
        }
        // 推进 revision
        let rev = db.bump_revision(CID).await.unwrap();
        db.bump_revision(CID).await.unwrap();
        let state = Arc::new(AppState::new(crate::config::ServerConfig::default(), db));

        notify_client_recovery(&state, CID).await;

        let want = state
            .db
            .get_client(CID)
            .await
            .unwrap()
            .unwrap()
            .config_revision as u64;
        assert!(want >= rev);
        for e in state.proxies.runtimes.iter() {
            assert_eq!(
                e.value().latest_revision.load(Ordering::Relaxed),
                want,
                "并发恢复后每条规则的 revision 必须一致"
            );
        }
    }

    /// 客户端不存在:直接返回,不 panic、不残留。
    #[tokio::test]
    async fn missing_client_is_noop() {
        let db = crate::db::Db::open("sqlite::memory:").await.unwrap();
        let state = Arc::new(AppState::new(crate::config::ServerConfig::default(), db));
        notify_client_recovery(&state, "no-such-client").await;
        assert!(state.proxies.runtimes.is_empty());
    }
}

#[cfg(test)]
mod notify_client_recovery_bench {
    //! P4 基准(手动运行):批量恢复的耗时。
    //!
    //! ```text
    //! cargo test -p rh-server --release notify_client_recovery_bench -- --ignored --nocapture
    //! ```
    //!
    //! ★实测结论(如实记录):**本机测不出并发收益,串行甚至略快**。
    //!
    //! | N=1000 | 串行 | 并发(16) |
    //! |---|---|---|
    //! | 本机 `:memory:` | 30.14ms | 30.54ms |
    //!
    //! 原因与报告 §2 的 recovery 基准完全相同:`spawn_runtime` 的 await 点
    //! (`get_client`、`send_bounded`、`mark_unconfirmed`)在内存 SQLite +
    //! 无在线背压下几乎不等待,并发只剩调度开销。真实收益依赖文件型
    //! SQLite 与在线会话 —— **未在部署环境验证,不为这条声称提升**。
    //!
    //! 保留这个基准是为了给出可复现的口径(后人可以自己复测),不是为了
    //! 证明收益。
    use super::*;

    const CID: &str = "recovery-bench-client";

    fn spec(name: &str, port: u16) -> ProxySpec {
        serde_json::from_value(serde_json::json!({
            "name": name,
            "kind": "stcp",
            "client_id": CID,
            "bind_ip": "127.0.0.1",
            "bind_port": port,
            "target_host": "127.0.0.1",
            "target_port": 9,
            "enabled": true,
        }))
        .unwrap()
    }

    #[tokio::test]
    #[ignore = "耗时基准,手动运行(--ignored --nocapture)"]
    async fn bench_notify_recovery() {
        for n in [100usize, 500, 1000] {
            let db = crate::db::Db::open("sqlite::memory:").await.unwrap();
            db.create_client(CID, "c", &"0".repeat(64), "")
                .await
                .unwrap();
            for i in 0..n {
                db.insert_proxy(CID, &spec(&format!("p{i}"), 0), false, "console")
                    .await
                    .unwrap();
            }
            let state = Arc::new(AppState::new(crate::config::ServerConfig::default(), db));
            let t0 = std::time::Instant::now();
            notify_client_recovery(&state, CID).await;
            println!(
                "N={n:>5}  notify_client_recovery={:>12?}  runtimes={}",
                t0.elapsed(),
                state.proxies.runtimes.len()
            );
        }
    }
}
