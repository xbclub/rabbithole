//! visitor 本地运行:stcp/xtcp TCP 监听、sudp UDP 监听、配对交付。

use crate::session::ClientSession;
use rh_protocol::message::{
    forward_flags, ClientMessage, StartForward, StartVisitorRelay, UserConnect,
};
use rh_protocol::{VisitorKind, VisitorSpec};
use rh_transport::BoxedIo;
use std::sync::Arc;
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;
use tracing::warn;

/// 单次启动的结局。
///
/// 修前 `start` 只返回 bool(绑定失败),调用方对**所有** false 一律重试;
/// 但有两类 false 重试是错的(会把不该活的规则拉起来):
/// * `Stale` —— 本次携带的 spec 已被表内更新的配置取代;
/// * `Aborted` —— bind 窗口内 `stop` 已把本次 token cancel 掉。
///
/// 把它们与真正的端口占用分开,重试只剩 [`StartOutcome::BindFailed`]。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartOutcome {
    /// 监听任务已 spawn。
    Started,
    /// 端口绑定失败(可能被占用),值得重试。
    BindFailed,
    /// 陈旧 start:表内当前 spec 已不是本次的 spec(或已被删除)。
    /// **不 bind、不留登记项** —— 新一代启停已经接管这条规则。
    Stale,
    /// bind 窗口内被 `stop` cancel:尊重停止意图,不重试。
    Aborted,
}

/// 启动一个 visitor 的本地监听。
/// 带重试的启动:吃掉"旧监听器尚未释放端口"的竞态窗口。
///
/// 配置变更时 `stop_visitor` 会 cancel 并**等监听任务退出**(见
/// [`crate::session::VisitorListener::await_exit`]),同代重启时端口已经
/// 释放;重试仍然保留,因为它挡住的是**外部**进程占端口的情形,以及
/// `stop_visitor` 等待超时(任务卡住)的兜底路径。
///
/// 修前的 `stop_visitor` 只 `cancel()`,listener 要等任务跑到下一次
/// accept 才 drop;紧接着 bind 同一端口会 `EADDRINUSE`,而 bind 失败
/// **只 warn 不重试** —— 规则从此永久失联(客户端不重启就不会恢复)。
pub async fn start_with_retry(sess: Arc<ClientSession>, visitor_id: u64, spec: VisitorSpec) {
    // 总预算 ~1s:足够覆盖内核释放端口的延迟,又不会在端口真被别人
    // 占用时无限重试。
    for attempt in 0..10 {
        // R1:会话正在关闭时不再重试 —— start 可能在 bind 窗口内被
        // 关闭路径 cancel,继续重试会把已停止的规则重新拉起。
        if sess.cancel.is_cancelled() {
            return;
        }
        if attempt > 0 {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        match start(sess.clone(), visitor_id, spec.clone()).await {
            // 起监听成功,或本次 start 已无意义(被取代 / 被停止):
            // 三种都不该重试。`Stale` 的 warn 已在 start 内发出。
            StartOutcome::Started | StartOutcome::Stale | StartOutcome::Aborted => return,
            StartOutcome::BindFailed => {}
        }
    }
    warn!(
        visitor = %spec.name,
        addr = %format!("{}:{}", spec.bind_addr, spec.bind_port),
        "visitor bind failed after retries (port busy?); rule stays down until next config change"
    );
}

/// 启动一个 visitor 的本地监听。
///
/// 落地顺序是**先校验、再 bind、再校验**:表(`sess.visitors`)是唯一由
/// run_loop 在 dispatch 时就同步更新的真相源,启停只是排队执行,所以
/// 「晚到的旧 start」只要拿表比一次就能认出来(见 [`StartOutcome::Stale`])。
pub async fn start(sess: Arc<ClientSession>, visitor_id: u64, spec: VisitorSpec) -> StartOutcome {
    // 会话已在关闭:不再起任何监听。
    //
    // 修前这条判断在 `start_with_retry` 的重试循环里;把它下移到 `start`
    // 本身,是因为 `start` 现在也可是直接被调用的落地函数(测试直调、
    // 以及链上最后一个单元)—— 只在重试侧判断会漏掉"关闭过程中恰好
    // 执行的那一次 start",那一次会 bind 成功并 spawn 一个监听任务,
    // 而会话收尾的 stop 名单已经取过,没人再来取消它。
    if sess.cancel.is_cancelled() {
        return StartOutcome::Aborted;
    }

    // R2(前置):表内当前 spec 不是本次 spec ⟹ 陈旧 start。
    //
    // 为什么不比登记表:登记项只说明"有谁在跑",不说明"跑的是哪一代
    // 配置"。而这里正是失败轮的真凶 —— 覆盖前若不认这一眼,晚到的旧
    // start 会把自己的 token 盖到新 start 的头上,旧端口的监听器从此
    // 无人可取消(孤儿),表里却指向新端口。
    //
    // 先判再 bind 还有个副作用是好的:陈旧 start 不再白占端口,也就不会
    // 因为 EADDRINUSE 被误当成"端口被占"去重试。
    if !spec_is_current(&sess, visitor_id, &spec).await {
        warn!(
            visitor = %spec.name,
            visitor_id,
            port = spec.bind_port,
            "stale visitor start (configuration superseded); not binding"
        );
        return StartOutcome::Stale;
    }

    let cancel = CancellationToken::new();
    // R4:退出闩锁。发送端进任务,任务返回(含 panic)时 drop —— 等它的
    // 那一刻即"listener 已 drop",不再用 connect 探针去猜。
    let (exit_tx, exit) = watch::channel(());
    // R2:这里是**唯一**写登记表的地方,所以覆盖防护落在这一行之前。
    //
    // 两种残留必须分开处理,否则会重演孤儿化:
    // * 表内有一条**未被 cancel** 的登记项 → 上一代监听还在跑。生产路径
    //   不会走到这里(链路先 stop 到底再 start),但"不会走到"是调度
    //   假设,不是不变式;直接 insert 就等于制造第二个监听器。宁可放弃
    //   本次 start,也不制造孤儿。
    // * 表内那条**已 cancel 但还没退出** → stop 正在收尾(或等待超时)。
    //   这里先等它退出再替换:否则它的 listener 还占着旧端口,而登记项
    //   已经指向新代 —— 正是"表与实际监听不一致"的形态。
    {
        // ① 判定 + 摘条目必须是**同一次**持锁,否则并发的另一个 start
        //    可以在两步之间插入自己的登记项,被我顺手摘掉(那是孤儿化的
        //    另一种写法)。摘到的旧条目若是"活着的",放回原样再判陈旧。
        let retired = {
            let mut g = sess.visitor_listeners.lock().await;
            match g.get(&visitor_id) {
                // 活着的上一代:拒绝叠第二个监听器
                Some(live) if !live.token.is_cancelled() => Err(live.port),
                // 已 cancel(或没有):取走,腾出位置
                _ => Ok(g.remove(&visitor_id)),
            }
        };
        let retired = match retired {
            Ok(r) => r,
            Err(live_port) => {
                warn!(
                    visitor = %spec.name,
                    visitor_id,
                    live_port,
                    "previous listener still registered and not cancelled; \
                     refusing to stack a second one"
                );
                return StartOutcome::Stale;
            }
        };
        // ② 锁外等旧任务退出(有界)。它已 cancel,这里只是等收尾;
        //    listener 不 drop 之前不能 bind 同端口,也不能让"表里的新端口"
        //    与实际(旧端口)不一致。
        if let Some(mut prev) = retired {
            if !prev.await_exit(crate::session::VISITOR_STOP_WAIT).await {
                warn!(
                    visitor = %spec.name,
                    visitor_id,
                    port = prev.port,
                    "previous (cancelled) listener did not exit before restart; \
                     its port may still be held"
                );
            }
        }
        // ③ 落地自己的登记项。此前的口径是"先登记再 bind":bind 窗口内
        //    到达的 stop 必须能 cancel 到**这一个** token。
        sess.visitor_listeners.lock().await.insert(
            visitor_id,
            crate::session::VisitorListener::new(cancel.clone(), spec.bind_port, exit),
        );
    }

    match spec.kind {
        VisitorKind::Stcp | VisitorKind::Xtcp => {
            let addr = format!("{}:{}", spec.bind_addr, spec.bind_port);
            let l = match tokio::net::TcpListener::bind(&addr).await {
                Ok(l) => l,
                Err(e) => {
                    // 回收自己插入的登记项(`bind_port` 允许为 0,所以这里
                    // 记的端口只是名义值,不能留给后来者当真)。
                    warn!(visitor = %spec.name, error = %e, "visitor bind failed");
                    discard_registration(&sess, visitor_id, &cancel).await;
                    return StartOutcome::BindFailed;
                }
            };
            let actual_port = l.local_addr().map(|a| a.port()).unwrap_or(spec.bind_port);
            if let Err(outcome) =
                settle_registration(&sess, visitor_id, &cancel, &spec, actual_port).await
            {
                // 显式 drop:端口必须在返回前释放(调用方可能立刻换端口重试)。
                drop(l);
                return outcome;
            }
            tokio::spawn(async move {
                // R4:退出信号持进任务 —— 它随任务结束 drop,等
                // `VisitorListener::await_exit()` 的那一刻就是 listener
                // 已 drop、端口已释放(不再用 connect 探针去猜)。
                let _exit = exit_tx;
                run_tcp(sess, visitor_id, spec, l, cancel).await;
            });
            StartOutcome::Started
        }
        VisitorKind::Sudp => {
            let addr = format!("{}:{}", spec.bind_addr, spec.bind_port);
            let s = match tokio::net::UdpSocket::bind(&addr).await {
                Ok(s) => s,
                Err(e) => {
                    warn!(visitor = %spec.name, error = %e, "visitor bind failed");
                    discard_registration(&sess, visitor_id, &cancel).await;
                    return StartOutcome::BindFailed;
                }
            };
            let actual_port = s.local_addr().map(|a| a.port()).unwrap_or(spec.bind_port);
            if let Err(outcome) =
                settle_registration(&sess, visitor_id, &cancel, &spec, actual_port).await
            {
                drop(s);
                return outcome;
            }
            tokio::spawn(async move {
                let _exit = exit_tx;
                run_udp(sess, visitor_id, Arc::new(s), cancel).await;
            });
            StartOutcome::Started
        }
    }
}

/// 表内当前 spec 是否就是本次要起的这一份(表的更新在 dispatch 时同步
/// 完成,见 `session::upsert` / `collect_sync_changes`)。
async fn spec_is_current(sess: &Arc<ClientSession>, visitor_id: u64, spec: &VisitorSpec) -> bool {
    sess.visitors.lock().await.get(&visitor_id) == Some(spec)
}

/// 摘掉自己插入的登记项。
///
/// 按 token 身份比对后只摘自己那一条:万一有并发的同 id 启动已经把
/// 登记项换掉,这里绝不能顺手把**别人的**活登记摘掉(那也是孤儿化的
/// 另一种写法)。
async fn discard_registration(
    sess: &Arc<ClientSession>,
    visitor_id: u64,
    cancel: &CancellationToken,
) {
    let mut g = sess.visitor_listeners.lock().await;
    let mine = g
        .get(&visitor_id)
        .map(|e| e.token == *cancel)
        .unwrap_or(false);
    if mine {
        g.remove(&visitor_id);
    }
}

/// bind 成功后、spawn 之前的收尾:
/// 1. stop 在 bind 窗口内到达(token 已 cancel)→ `Aborted`;
/// 2. R2(后置):bind 期间表可能已被更新 → `Stale`(丢弃刚 bind 的
///    listener,等价于"取消自己刚起的任务",且绝不留登记项);
/// 3. 都没问题 → 把登记端口校正为实际 bind 端口(`bind_port = 0` 时两者
///    不同,测试的一致性断言依赖它)。
async fn settle_registration(
    sess: &Arc<ClientSession>,
    visitor_id: u64,
    cancel: &CancellationToken,
    spec: &VisitorSpec,
    actual_port: u16,
) -> Result<(), StartOutcome> {
    if cancel.is_cancelled() {
        discard_registration(sess, visitor_id, cancel).await;
        return Err(StartOutcome::Aborted);
    }
    // 会话在 bind 窗口内开始关闭:同样按"停止意图"处理(见 `start` 开头)。
    if sess.cancel.is_cancelled() {
        discard_registration(sess, visitor_id, cancel).await;
        return Err(StartOutcome::Aborted);
    }
    if !spec_is_current(sess, visitor_id, spec).await {
        warn!(
            visitor = %spec.name,
            visitor_id,
            "visitor spec superseded during bind; dropping the listener just bound"
        );
        discard_registration(sess, visitor_id, cancel).await;
        return Err(StartOutcome::Stale);
    }
    let mut g = sess.visitor_listeners.lock().await;
    if let Some(e) = g.get_mut(&visitor_id) {
        if e.token == *cancel {
            e.port = actual_port;
        }
    }
    Ok(())
}

/// stcp/xtcp:本地 TCP 接入。xtcp 优先复用已打洞直连;否则 USER_CONNECT
/// 由服务端中继(stcp)或发起打洞(xtcp)。
async fn run_tcp(
    sess: Arc<ClientSession>,
    visitor_id: u64,
    spec: VisitorSpec,
    l: tokio::net::TcpListener,
    cancel: CancellationToken,
) {
    loop {
        tokio::select! {
            acc = l.accept() => {
                let Ok((conn, peer)) = acc else { return };
                // C10:用户侧连接必须关闭 Nagle 并开启 keepalive。
                // 修前 visitor 用户侧是全仓唯一不设这两个选项的 accept 路径,
                // stcp/xtcp 的小包交互会被 Nagle 合并出额外 RTT。
                // N16:keepalive 半边当时只写进了注释、代码漏了 —— 用户
                // 对端静默消失(拔线/NAT 回收)时 relay 双泵永挂,keepalive
                // 让 OS 探测死链并出错,泵才收得了场。60s 空闲起探。
                let _ = conn.set_nodelay(true);
                // tokio TcpStream 无 keepalive API,走 socket2(与
                // rh-transport 的 SockRef 用法一致);60s 空闲起探
                let sref = socket2::SockRef::from(&conn);
                let _ = sref.set_tcp_keepalive(&socket2::TcpKeepalive::new()
                    .with_time(std::time::Duration::from_secs(60)));
                let s2 = sess.clone();
                let sp = spec.clone();
                let vid = visitor_id;
                tokio::spawn(async move {
                    handle_user_conn(s2, vid, sp, conn, peer.to_string()).await;
                });
            }
            _ = cancel.cancelled() => return,
            _ = sess.cancel.cancelled() => return,
        }
    }
}

async fn handle_user_conn(
    sess: Arc<ClientSession>,
    visitor_id: u64,
    spec: VisitorSpec,
    mut conn: tokio::net::TcpStream,
    src: String,
) {
    let user_conn_id = sess.next_conn_id().await;
    // xtcp:已有直连 mux 则直接开流
    if spec.kind == VisitorKind::Xtcp {
        let mux = sess.punched.lock().await.get(&visitor_id).cloned();
        let auth = sess
            .punched_auth
            .lock()
            .await
            .get(&visitor_id)
            .cloned()
            .unwrap_or_default();
        if let Some(mux) = mux {
            match xtcp_direct_stream(&sess, &mux, &spec, auth, user_conn_id, conn).await {
                Some(conn_back) => {
                    // 直连失败:移除失效 mux(与其凭据同生命周期),落入常规路径
                    sess.punched.lock().await.remove(&visitor_id);
                    sess.punched_auth.lock().await.remove(&visitor_id);
                    conn = conn_back;
                }
                None => return,
            }
        }
    }
    // 常规:登记等待 + USER_CONNECT;TTL 10s。
    //
    // TTL 淘汰不再**每连接 spawn 一个 sleep 任务**(修前交付成功后无法
    // abort,任务会活到 10s 才醒来对已移除的 key 做一次无意义 remove,
    // 任务数随连接数增长)。改为登记时刻随条目保存,由会话级单个
    // `waiting_sweeper` 周期淘汰。
    sess.insert_waiting(user_conn_id, conn, visitor_id).await;
    tracing::debug!(visitor_id, user_conn_id, "user connect registered, sending");
    let uc = UserConnect {
        visitor_id,
        user_conn_id,
        src_addr: src,
        udp: false,
    };
    if !sess.send_msg(ClientMessage::UserConnect(uc)).await {
        sess.take_waiting(user_conn_id).await;
    }
}

/// 经打洞直连 mux 开一路流(首帧 START_FORWARD 供对端识别),拼本地连接。
/// Some(conn) = 失败返还连接;None = 已消费(成功)。
async fn xtcp_direct_stream(
    sess: &Arc<ClientSession>,
    mux: &Arc<rh_transport::Multiplexer>,
    spec: &VisitorSpec,
    xtcp_auth: String,
    _user_conn_id: u64,
    conn: tokio::net::TcpStream,
) -> Option<tokio::net::TcpStream> {
    let mut stream = match mux.open_stream().await {
        Ok(s) => s,
        // 开流失败(mux 已死):返还连接,由调用方移除失效 mux 并走常规路径
        Err(_) => return Some(conn),
    };
    // A7:直连首帧凭据由调用方按 visitor_id 从 punched_auth 取。
    let fwd = StartForward {
        proxy_id: 0,
        user_src: String::new(),
        target_host: String::new(),
        target_port: 0,
        flags: 0,
        proxy_proto: rh_protocol::message::ProxyProto::None,
        proxy_name: spec.target_proxy_name.clone(),
        xtcp_auth,
    };
    let Ok(payload) = serde_json::to_vec(&fwd) else {
        return Some(conn);
    };
    if rh_protocol::write_frame(&mut stream, rh_protocol::FrameType::START_FORWARD, &payload)
        .await
        .is_err()
    {
        return Some(conn);
    }
    // Y1 零拷贝 mux 中继(visitor 直连):`mux.open_stream()` 产出的就是
    // 未包装 `RhMuxStream`,满足 chunk 快路径前提。走 `mux_relay` 后数据
    // 以 `Bytes` 引用直达帧体,省掉 copy 回落路径的两次中间拷贝。
    //
    // 与 forward.rs `handle_start_forward` 同式:未压缩 + relay_mode≠copy。
    // xtcp 直连首帧 `flags: 0`(不带 COMPRESSED),这里的「未压缩」由
    // 协议语义保证,但判定仍按实际 `BoxedIo` 类型走——`is_mux_stream`
    // 对任何包装(PrependedIo/压缩流)恒 false,不会误入快路径。
    crate::forward::relay_stream(stream, Box::new(conn), &sess.cfg.relay_mode).await;
    let _ = sess;
    None
}

/// nathole_cli 复用:经打洞直连 mux 开流并拼接。
pub async fn xtcp_direct_stream_pub(
    sess: &Arc<ClientSession>,
    mux: &Arc<rh_transport::Multiplexer>,
    spec: &VisitorSpec,
    auth: String,
    user_conn_id: u64,
    conn: tokio::net::TcpStream,
) -> Option<tokio::net::TcpStream> {
    xtcp_direct_stream(sess, mux, spec, auth, user_conn_id, conn).await
}

/// 中继通道配对交付(session.rs 调用)。
pub async fn deliver_relay(
    sess: Arc<ClientSession>,
    relay: StartVisitorRelay,
    channel: BoxedIo,
    prefix: Vec<u8>,
) {
    let Some((mut conn, _, _)) = sess.take_waiting(relay.user_conn_id).await else {
        tracing::warn!(
            conn = relay.user_conn_id,
            "relay delivered but user conn gone"
        );
        return; // 本地连接已超时:丢弃通道
    };
    // 压缩时残留字节是压缩流首块,须回灌给解压器;未压缩时直接写给本地连接。
    //
    // 只有**未压缩**分支才可能走零拷贝 mux chunk 快路径:`channel` 源自
    // `sess.mux.open_stream()`(未包装 `RhMuxStream`),压缩分支产出
    // `DuplexStream`,`is_mux_stream` 恒 false 自然回落 copy。因此这里不做
    // 额外分支——压缩路径逐行保持原样,判定交给 `relay_stream` 的实际类型。
    let channel = if relay.flags & forward_flags::COMPRESSED != 0 {
        let inner = if prefix.is_empty() {
            channel
        } else {
            rh_transport::PrependedIo::wrap(channel, prefix)
        };
        rh_transport::compress_stream(inner).await
    } else {
        if !prefix.is_empty() {
            use tokio::io::AsyncWriteExt as _;
            let _ = conn.write_all(&prefix).await;
        }
        channel
    };
    crate::forward::relay_stream(channel, Box::new(conn), &sess.cfg.relay_mode).await;
}

/// sudp:本地 UDP 监听;首包触发 USER_CONNECT(udp=true);通道到位后
/// 由通道任务接管 socket(带首包缓冲),死亡后重新武装。
async fn run_udp(
    sess: Arc<ClientSession>,
    visitor_id: u64,
    sock: Arc<tokio::net::UdpSocket>,
    cancel: CancellationToken,
) {
    let (tx, mut rx) = mpsc::channel::<BoxedIo>(1);
    sess.sudp_deliver.lock().await.insert(visitor_id, tx);
    let armed = Arc::new(std::sync::atomic::AtomicBool::new(false));
    // ★VecDeque,不用 Vec:超限时要丢**最旧**的包,`Vec::remove(0)` 是 O(n)
    // 整体搬移。容量上限 512 包 × 最大 65507B ≈ 32MiB,每来一个新包搬 32MiB
    // 在 UDP 高 PPS 下是纯浪费。`pop_front` 是 O(1)。
    let mut pending: std::collections::VecDeque<(std::net::SocketAddr, Vec<u8>)> =
        std::collections::VecDeque::new();
    let mut pkt = vec![0u8; 65536];
    loop {
        tokio::select! {
            r = sock.recv_from(&mut pkt) => {
                let Ok((n, peer)) = r else { return };
                if !armed.load(std::sync::atomic::Ordering::Relaxed) {
                    armed.store(true, std::sync::atomic::Ordering::Relaxed);
                    let uc = UserConnect {
                        visitor_id,
                        user_conn_id: 0,
                        src_addr: peer.to_string(),
                        udp: true,
                    };
                    if !sess.send_msg(ClientMessage::UserConnect(uc)).await {
                        armed.store(false, std::sync::atomic::Ordering::Relaxed);
                    }
                }
                pending.push_back((peer, pkt[..n].to_vec()));
                // 超限丢最旧的(与修前语义一致,只是不再 O(n) 搬移)
                if pending.len() > 512 {
                    pending.pop_front();
                }
            }
            c = rx.recv() => {
                let Some(channel) = c else { return };
                let s2 = sess.clone();
                let aw = armed.clone();
                let sk = sock.clone();
                let c2 = cancel.clone();
                let p = std::mem::take(&mut pending);
                let vid = visitor_id;
                let mut handle = tokio::spawn(async move {
                    crate::udpchan::run_visitor_side(channel, sk, c2, p).await;
                    // 通道死亡:重新武装(下一本地包触发重建)
                    aw.store(false, std::sync::atomic::Ordering::Relaxed);
                    let uc = UserConnect {
                        visitor_id: vid,
                        user_conn_id: 0,
                        src_addr: String::new(),
                        udp: true,
                    };
                    let _ = s2.send_msg(ClientMessage::UserConnect(uc)).await;
                });
                // 通道在位:等待其结束(不与本循环竞争 socket)
                tokio::select! {
                    _ = &mut handle => {}
                    _ = cancel.cancelled() => { handle.abort(); return; }
                    _ = sess.cancel.cancelled() => { handle.abort(); return; }
                }
            }
            _ = cancel.cancelled() => return,
            _ = sess.cancel.cancelled() => return,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    /// 整个用例的 deadline(防挂死;mux 泵在两端的收敛缺陷会让测试永久挂起)。
    const DEADLINE: Duration = Duration::from_secs(20);
    /// 每方向搬运量。够大以覆盖多帧(64KiB 级别 yamux 帧),又不拖慢 CI。
    const BYTES: usize = 512 * 1024;

    fn pseudo_random(n: usize, seed: u64) -> Vec<u8> {
        let mut out = Vec::with_capacity(n);
        let mut x = seed | 1;
        for _ in 0..n {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            out.push((x >> 24) as u8);
        }
        out
    }

    /// 真实 yamux 对:`open_stream` 产出的就是 visitor 手里的 channel 形态。
    fn mux_pair() -> (
        Arc<rh_transport::Multiplexer>,
        Arc<rh_transport::Multiplexer>,
    ) {
        let (a, b) = tokio::io::duplex(64 * 1024);
        let client = rh_transport::mux::wrap_stream_as_client(Box::new(a) as BoxedIo, true, 100);
        let server = rh_transport::mux::wrap_stream_as_server(Box::new(b) as BoxedIo, true, 100);
        (client, server)
    }

    async fn read_exact_n<R: tokio::io::AsyncRead + Unpin>(
        r: &mut R,
        n: usize,
        what: &str,
    ) -> Vec<u8> {
        let mut buf = vec![0u8; n];
        tokio::io::AsyncReadExt::read_exact(r, &mut buf)
            .await
            .unwrap_or_else(|e| panic!("{what}: read_exact({n}) failed: {e}"));
        buf
    }

    /// 核心验收:一对 wrap_stream_as_client/server 建 mux,开一路流,
    /// 经 `relay_stream`(mux_relay)接 duplex 目标,双向各 512KiB 校验一致,
    /// 并验证半关闭沿两个方向传递。
    async fn bidirectional_mux_relay(relay_mode: &'static str, expect_chunk: bool) {
        // 路径探针:整个用例独占计数窗口(async 锁,跨 await 安全)
        let _serial = crate::forward::relay_path_probe::serial().await;
        let base = crate::forward::relay_path_probe::snapshot();
        let (client, server) = mux_pair();
        let stream = client.open_stream().await.expect("open_stream");

        // 前提(快路径可达性):visitor 拿到的必须是**未包装** RhMuxStream,
        // 否则 is_mux_stream 恒 false,这条测试就退化成 copy 路径验证。
        if relay_mode != "copy" {
            assert!(
                rh_transport::mux::is_mux_stream(&stream),
                "open_stream 产物必须是未包装 RhMuxStream"
            );
        }

        let (local_relay, local_app) = tokio::io::duplex(64 * 1024);
        let relay = tokio::spawn(crate::forward::relay_stream(
            stream,
            Box::new(local_relay) as BoxedIo,
            relay_mode,
        ));

        let peer = server.accept().await.expect("accept");
        // 路径自证:accept 侧拿到的也必须是未包装 RhMuxStream —— relay 的
        // 另一头是 duplex,若这里退化成非 mux 流,说明整条链根本没走 chunk。
        assert!(
            rh_transport::mux::is_mux_stream(&peer),
            "对端 accept 产物必须是未包装 RhMuxStream"
        );
        let (mut peer_r, mut peer_w) = tokio::io::split(peer);
        let (mut app_r, mut app_w) = tokio::io::split(local_app);

        // ---- 方向 1:peer(远端)→ local_app(本地目标)----
        let down = pseudo_random(BYTES, 0x1111_2222_3333_4444);
        let expect_down = down.clone();
        let writer = tokio::spawn(async move {
            tokio::io::AsyncWriteExt::write_all(&mut peer_w, &down)
                .await
                .expect("peer write");
            peer_w
        });
        let got = read_exact_n(&mut app_r, BYTES, "peer→local").await;
        let mut peer_w = writer.await.expect("writer join");
        assert!(got == expect_down, "peer→local 数据不一致");

        // ---- 方向 2:local_app(本地目标)→ peer(远端)----
        let up = pseudo_random(BYTES, 0x5555_6666_7777_8888);
        let expect_up = up.clone();
        let writer = tokio::spawn(async move {
            tokio::io::AsyncWriteExt::write_all(&mut app_w, &up)
                .await
                .expect("local write");
            app_w
        });
        let got = read_exact_n(&mut peer_r, BYTES, "local→peer").await;
        let mut app_w = writer.await.expect("writer join");
        assert!(got == expect_up, "local→peer 数据不一致");

        // ---- 半关闭 1:peer 关写 → local 读端 EOF(远端 FIN 透传)----
        tokio::io::AsyncWriteExt::shutdown(&mut peer_w)
            .await
            .expect("peer shutdown");
        let mut probe = [0u8; 1];
        assert_eq!(
            tokio::io::AsyncReadExt::read(&mut app_r, &mut probe)
                .await
                .expect("local read after peer FIN"),
            0,
            "peer 半关闭后 local 必须读到 EOF"
        );

        // ---- 半关闭 2:local 关写 → peer 读端 EOF(本地 FIN 透传)----
        tokio::io::AsyncWriteExt::shutdown(&mut app_w)
            .await
            .expect("local shutdown");
        assert_eq!(
            tokio::io::AsyncReadExt::read(&mut peer_r, &mut probe)
                .await
                .expect("peer read after local FIN"),
            0,
            "local 半关闭后 peer 必须读到 EOF"
        );

        relay.await.expect("relay join");

        // 路径断言:这是本次改造的**核心验收**。功能测试区分不了两条路径
        // (mux_relay 与 splice 语义等价,把快路径关掉照样全绿),必须直接锁。
        let (chunk, copy) = crate::forward::relay_path_probe::since(base);
        if expect_chunk {
            assert_eq!(
                chunk, 1,
                "必须走零拷贝 mux chunk 快路径(chunk={chunk}, copy={copy})"
            );
            assert_eq!(copy, 0, "不得回落 copy(chunk={chunk}, copy={copy})");
        } else {
            assert_eq!(
                chunk, 0,
                "relay_mode=copy 不得走快路径(chunk={chunk}, copy={copy})"
            );
            assert_eq!(
                copy, 1,
                "relay_mode=copy 必须回落 copy(chunk={chunk}, copy={copy})"
            );
        }
    }

    /// 零拷贝 mux chunk 中继(visitor 直连路径的目标形态)。
    #[tokio::test]
    async fn visitor_mux_relay_bidirectional_and_half_close() {
        tokio::time::timeout(DEADLINE, bidirectional_mux_relay("mux", true))
            .await
            .expect("整个用例 deadline");
    }

    /// relay_mode=copy 是 A/B 对照开关:必须回落 copy 且功能完全一致。
    #[tokio::test]
    async fn visitor_relay_mode_copy_still_works() {
        tokio::time::timeout(DEADLINE, bidirectional_mux_relay("copy", false))
            .await
            .expect("整个用例 deadline");
    }

    /// 红线:压缩流不是 RhMuxStream,判定自然 false → 原样回落 copy。
    /// 这里直接锁「压缩包装后 is_mux_stream 为假」这一事实,防止以后有人
    /// 为了"顺手统一"把它改快。
    #[tokio::test]
    async fn compressed_stream_is_not_mux_stream() {
        tokio::time::timeout(DEADLINE, async {
            let (client, _server) = mux_pair();
            let stream = client.open_stream().await.expect("open_stream");
            assert!(rh_transport::mux::is_mux_stream(&stream));

            let compressed = rh_transport::compress_stream(stream).await;
            assert!(
                !rh_transport::mux::is_mux_stream(&compressed),
                "压缩流必须不是 RhMuxStream,否则会被误判进快路径"
            );

            // PrependedIo 同理(残留字节回灌包装)
            let (_client2, _server2) = mux_pair();
            let wrapped = rh_transport::PrependedIo::wrap(compressed, b"prefix".to_vec());
            assert!(
                !rh_transport::mux::is_mux_stream(&wrapped),
                "PrependedIo 包装必须不是 RhMuxStream"
            );
        })
        .await
        .expect("整个用例 deadline");
    }

    /// 压缩回落路径端到端仍正确:compress_stream 包装后经 relay_stream,
    /// 对端解压,padding 数据双向一致。
    #[tokio::test]
    async fn compressed_path_through_relay_stream_roundtrips() {
        tokio::time::timeout(DEADLINE, async {
            let (client, server) = mux_pair();
            let stream = client.open_stream().await.expect("open_stream");
            let compressed = rh_transport::compress_stream(stream).await;

            let (local_relay, local_app) = tokio::io::duplex(64 * 1024);
            let _serial = crate::forward::relay_path_probe::serial().await;
            let base = crate::forward::relay_path_probe::snapshot();
            let relay = tokio::spawn(crate::forward::relay_stream(
                compressed,
                Box::new(local_relay) as BoxedIo,
                "mux",
            ));

            // 对端也要解压才看得懂
            let peer = server.accept().await.expect("accept");
            let mut peer = rh_transport::compress_stream(peer).await;

            let n = 256 * 1024;
            let payload = pseudo_random(n, 0xDEAD_BEEF_0000_0001);
            let expect = payload.clone();
            let (mut local_r, mut local_w) = tokio::io::split(local_app);

            let writer = tokio::spawn(async move {
                tokio::io::AsyncWriteExt::write_all(&mut local_w, &payload)
                    .await
                    .expect("local write");
                local_w
            });
            let got = read_exact_n(&mut peer, n, "local→peer(compressed)").await;
            let _local_w = writer.await.expect("writer join");
            assert!(got == expect, "压缩回落路径数据不一致");
            let _ = &mut local_r;

            // 红线:即使 relay_mode="mux",压缩流也必须回落 copy
            let (chunk, copy) = crate::forward::relay_path_probe::since(base);
            assert_eq!(chunk, 0, "压缩流不得进快路径(chunk={chunk}, copy={copy})");
            assert_eq!(copy, 1, "压缩流必须回落 copy(chunk={chunk}, copy={copy})");

            drop(peer);
            relay.abort();
        })
        .await
        .expect("整个用例 deadline");
    }
}

#[cfg(test)]
mod pending_queue_tests {
    use std::collections::VecDeque;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    /// 超限时丢**最旧**的包(与修前语义一致),且为 O(1)。
    ///
    /// 修前用 `Vec::remove(0)`:容量上限 512 包 × 最大 65507B ≈ 32MiB,
    /// 每来一个新包就要搬移整个缓冲。UDP 高 PPS 下这是纯浪费。
    /// 本测试锁住"丢最旧"这个语义,避免将来改成丢最新时悄悄改变行为。
    #[test]
    fn over_capacity_drops_oldest() {
        let cap = 4usize; // 用极小容量表达语义
        let mut pending: VecDeque<(SocketAddr, Vec<u8>)> = VecDeque::new();
        let peer: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 1234);
        for i in 0u8..6 {
            pending.push_back((peer, vec![i]));
            if pending.len() > cap {
                pending.pop_front();
            }
        }
        assert_eq!(pending.len(), cap, "长度必须被钳住");
        // 留下的是 2,3,4,5(最旧的 0,1 被丢弃)
        let got: Vec<u8> = pending.iter().map(|(_, d)| d[0]).collect();
        assert_eq!(got, vec![2, 3, 4, 5], "必须丢最旧的,不能丢最新");
    }

    /// 未超限时不得丢弃任何包(顺序保持)。
    #[test]
    fn under_capacity_keeps_order() {
        let mut pending: VecDeque<(SocketAddr, Vec<u8>)> = VecDeque::new();
        let peer: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 1234);
        for i in 0u8..4 {
            pending.push_back((peer, vec![i]));
            if pending.len() > 4 {
                pending.pop_front();
            }
        }
        let got: Vec<u8> = pending.iter().map(|(_, d)| d[0]).collect();
        assert_eq!(got, vec![0, 1, 2, 3]);
    }
}
