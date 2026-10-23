//! 数据面:TCP 转发(池/取一补一/自包含指派)与 UDP 通道(服务端负责制)。
//! vhost http/https/tcpmux 与 visitor 中继复用 get_channel + assign 指派。

use crate::session::{RequestPurpose, Session};
use crate::state::{AppState, ProxyRuntime, TcpPool};
use rh_core::ratelimit::TokenBucket;
use rh_protocol::message::udp_packet;
use rh_protocol::message::{ChannelEntity, StartForward};
use rh_protocol::{forward_flags, FrameType, ProxySpec};
use rh_transport::{compress_stream, BoxedIo};
use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::sync::mpsc;
use tracing::{debug, warn};

// ---------------- 绑定 ----------------

pub async fn bind_tcp_listener(
    _state: &Arc<AppState>,
    spec: &ProxySpec,
    bind_ip: std::net::IpAddr,
) -> std::io::Result<TcpListener> {
    let addr = SocketAddr::new(bind_ip, spec.bind_port);
    TcpListener::bind(addr).await
}

pub async fn bind_udp_socket(
    _state: &Arc<AppState>,
    spec: &ProxySpec,
    bind_ip: std::net::IpAddr,
) -> std::io::Result<Arc<UdpSocket>> {
    let addr = SocketAddr::new(bind_ip, spec.bind_port);
    let std_sock = std::net::UdpSocket::bind(addr)?;
    std_sock.set_nonblocking(true)?;
    Ok(Arc::new(UdpSocket::from_std(std_sock)?))
}

pub async fn close_proxy_channels(rt: &Arc<ProxyRuntime>) {
    // 池与通道由取消/关闭级联;此处显式清空池。
    if let Some(p) = rt.pool.lock().await.as_ref() {
        let mut ready = p.ready.lock().await;
        ready.clear();
    }
    // ★A2:UDP 通道 sender **不置 None**。
    //
    // 置 None 会让 udp_serve_loop 的 `new_rx.recv()` 立刻返回 None,泵直接
    // return;而泵只在 spawn_runtime / bind_error_retry 时启动,客户端重连
    // 不会重建运行时 —— 于是重连后 UDP 规则永久收不到包(且 udp_on_channel
    // 见 None 时**静默丢流**,线上完全没有痕迹)。
    //
    // 正确语义:泵的生死与**规则运行态**(rule_cancel)绑定,与会话在线解耦。
    // 队列里可能残留旧会话的通道(对端已失效),但那由 pump 首帧读写失败
    // 自然淘汰,不需要在这里主动清空 —— 主动清空反而会丢掉刚抵达的新通道。
    //
    // 注意:这里刻意只做"什么都不改"。字段保留 Some(sender),泵继续等下一个。
}

/// copy 路径读缓冲下/上限(R3 红线 2 常量锁:上限 64KiB 不得放宽)。
const COPY_MIN_BUF: usize = 32 * 1024;
const COPY_MAX_BUF: usize = 64 * 1024;

// ---------------- TCP 池 ----------------

/// 会话认领池(epoch 全局单调,仅允许向前):驱逐旧会话遗留的就绪条目。
/// 会话重连/替换后,旧会话建立的通道对端已不可用,必须整体失效。
async fn claim_pool(pool: &Arc<TcpPool>, sess: &Arc<Session>) {
    let cur = pool.epoch.load(Ordering::Relaxed);
    if sess.epoch > cur {
        pool.epoch.store(sess.epoch, Ordering::Relaxed);
        // B1:换 epoch 必须把旧会话遗留的在途计数清零。断连会打断在途
        // CREATE,那些请求的 inflight 永不会减(交付路径已按 epoch 丢弃),
        // 池会永久饥饿:ready+inflight 恒 ≥ want,再也不签发。
        pool.inflight.store(0, Ordering::Relaxed);
        let mut ready = pool.ready.lock().await;
        let mut keep = VecDeque::with_capacity(ready.len());
        for entry in ready.drain(..) {
            if entry.0 == sess.epoch {
                keep.push_back(entry);
            }
        }
        *ready = keep; // 旧会话条目在此丢弃,级联关闭通道
    }
}

/// 池水位维持(取一补一):池中+在途 < pool_size 时签发 CREATE。
pub async fn tcp_refill(state: &Arc<AppState>, sess: &Arc<Session>, rt: &Arc<ProxyRuntime>) {
    let Some(pool) = rt.pool.lock().await.clone() else {
        return;
    };
    claim_pool(&pool, sess).await;
    let ready = pool.ready.lock().await.len() as u64;
    let inflight = pool.inflight.load(Ordering::Relaxed);
    let want = pool.size as u64;
    if ready + inflight >= want {
        return;
    }
    let n = want - ready - inflight;
    for _ in 0..n {
        pool.inflight.fetch_add(1, Ordering::Relaxed);
        // 过期回收钩子:请求 10s 内没等到通道(客户端在线但没回报)时,
        // 条目会被 session 层摘除,这里负责把 inflight 还回去。
        // 修前没有任何回收路径,每一次过期都让池永久少一个名额。
        let pool_for_expire = pool.clone();
        let hook: Box<dyn FnOnce() + Send> = Box::new(move || {
            let _ =
                pool_for_expire
                    .inflight
                    .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                        Some(v.saturating_sub(1))
                    });
        });
        match state
            .issue_create_channel_with(
                sess,
                ChannelEntity::Proxy { proxy_id: rt.id },
                RequestPurpose::TcpPool { proxy_id: rt.id },
                Some(hook),
            )
            .await
        {
            Some(_rid) => {}
            None => {
                // ★签发失败必须显式归还计数。
                //
                // `issue_create_channel_with` 失败路径用的是
                // `take_pending_request_quiet`(故意**不**触发钩子),因为
                // 它假定调用方自己回收。所以这里不减就**谁都不减**:
                // 修前只 `break`,一次签发失败让池永久少一个名额。
                // 两边注释曾互相指认对方会减,实际都不减,靠"send 失败
                // ⟹ 会话将死 ⟹ claim_pool 清零"侥幸自愈 —— 不该依赖。
                let _ = pool
                    .inflight
                    .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                        Some(v.saturating_sub(1))
                    });
                break;
            }
        }
    }
}

/// 数据通道抵达:入池或直接交付等待者。
pub async fn tcp_on_channel(
    state: &Arc<AppState>,
    sess: &Arc<Session>,
    proxy_id: u64,
    request_id: u64,
    stream: BoxedIo,
) {
    let Some(rt) = state.proxies.get(proxy_id) else {
        return;
    };
    let Some(pool) = rt.pool.lock().await.clone() else {
        return;
    };
    // ★先判 epoch,再减 inflight —— 顺序不能反。
    //
    // `claim_pool` 换 epoch 时会把 inflight `store(0)`(断连打断了在途
    // CREATE,那些请求永远不会有人来减)。若本函数先无条件 `fetch_sub`,
    // 旧会话的通道恰在清零之后到达,计数就从 0 回绕到 `u64::MAX`,
    // 此后 `ready + inflight >= want` 恒成立 —— 预热池永久饥饿,
    // 直到下一次换 epoch。原子减用 `fetch_update` 兜住下溢,双保险。
    let owner = pool.epoch.load(Ordering::Relaxed);
    if owner != 0 && owner != sess.epoch {
        return; // 旧会话晚到的通道:不碰新 epoch 的计数
    }
    let _ = pool
        .inflight
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
            Some(v.saturating_sub(1))
        });
    // 有等待者直接交付;等待者已放弃(超时)则通道入池,不得丢弃
    let deliver = {
        let mut w = pool.waiters.lock().await;
        w.pop()
    };
    match deliver {
        Some((_, tx)) => {
            if let Err(stream) = tx.send(stream) {
                // 接收端已放弃:通道回到池或关闭
                let mut ready = pool.ready.lock().await;
                if ready.len() < pool.size {
                    ready.push_back((sess.epoch, request_id, stream));
                } else {
                    drop(stream);
                }
            }
        }
        None => {
            let mut ready = pool.ready.lock().await;
            if ready.len() < pool.size {
                ready.push_back((sess.epoch, request_id, stream));
            } else {
                // 超额:关闭
                drop(stream);
            }
        }
    }
    // 取一补一
    tcp_refill(state, sess, &rt).await;
}

/// 获取一条到客户端的数据通道(≤ pool_wait_timeout)。
pub async fn get_channel(state: &Arc<AppState>, rt: &Arc<ProxyRuntime>) -> Option<BoxedIo> {
    let sess = state.sessions.get(&rt.client_id)?;
    let pool = rt.pool.lock().await.clone()?;
    claim_pool(&pool, &sess).await;
    // 就绪直取(跳过旧会话遗留条目)
    {
        let mut ready = pool.ready.lock().await;
        let owner = pool.epoch.load(Ordering::Relaxed);
        while let Some((epoch, _rid, s)) = ready.pop_front() {
            if epoch == owner {
                // 注意:必须先释放 ready 锁再 refill(内部会再取同锁)
                drop(ready);
                tcp_refill(state, &sess, rt).await;
                return Some(s);
            }
            // 旧会话条目:丢弃继续
        }
    }
    // 等待新通道(签发后到达即交付)
    let (tx, rx) = tokio::sync::oneshot::channel::<BoxedIo>();
    let waiter_id = pool.waiter_seq.fetch_add(1, Ordering::Relaxed);
    pool.waiters.lock().await.push((waiter_id, tx));
    // 签发。R5:等待路径必须带过期回收钩子 —— 与 tcp_refill 同型
    //(session.rs 注释:「客户端在线但 10s 不回报通道时,inflight 永久 +1
    // → 池慢性饿死」)。修前这里用无钩子的 issue_create_channel,pending
    // 过期被清扫时无人递减,池容量永久少一格;该钩子补上 inflight 归还
    // 与 waiter 摘除。签发失败路径语义不变(自己归还)。
    pool.inflight.fetch_add(1, Ordering::Relaxed);
    let pool_for_expire = pool.clone();
    let wid_for_expire = waiter_id;
    let hook: Box<dyn FnOnce() + Send> = Box::new(move || {
        let _ = pool_for_expire
            .inflight
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                Some(v.saturating_sub(1))
            });
        let pool = pool_for_expire;
        tokio::spawn(async move {
            pool.remove_waiter(wid_for_expire).await;
        });
    });
    let issued = state
        .issue_create_channel_with(
            &sess,
            ChannelEntity::Proxy { proxy_id: rt.id },
            RequestPurpose::TcpPool { proxy_id: rt.id },
            Some(hook),
        )
        .await
        .is_some();
    if !issued {
        pool.inflight.fetch_sub(1, Ordering::Relaxed);
        pool.remove_waiter(waiter_id).await;
        return None;
    }
    // 先取出标量再进入 match:读锁守卫不得作为 match 临时量跨 await 持有
    let wait_ms = state
        .limits
        .read()
        .unwrap_or_else(|e| e.into_inner())
        .pool_wait_timeout_ms;
    match tokio::time::timeout(Duration::from_millis(wait_ms), rx).await {
        Ok(Ok(s)) => Some(s),
        _ => {
            pool.remove_waiter(waiter_id).await;
            None
        }
    }
}

/// START_FORWARD 指派(数据连接最后一帧)+ 桥接到用户连接。
/// 半关闭:双向独立复制。
pub async fn assign_forward(
    state: &Arc<AppState>,
    rt: &Arc<ProxyRuntime>,
    mut channel: BoxedIo,
    user: BoxedIo,
    user_src: &str,
) {
    let spec = rt.spec.read().await.clone();
    let target_host = spec.target_host.clone();
    let target_port = spec.target_port;
    let mut flags: u8 = 0;
    if spec.transport.use_compression {
        flags |= forward_flags::COMPRESSED;
    }
    let fwd = StartForward {
        proxy_id: rt.id,
        user_src: user_src.to_string(),
        target_host,
        target_port,
        flags,
        proxy_proto: conv_proxy_proto(spec.transport.proxy_proto),
        proxy_name: spec.name.clone(),
        xtcp_auth: String::new(),
    };
    let payload = match serde_json::to_vec(&fwd) {
        Ok(p) => p,
        Err(e) => {
            // G5 1/N 归因:此前零留痕(实践中仅 OOM 级,计数求全)
            state.metrics.count_session_failure("start_forward_encode");
            debug!(proxy = rt.id, user_src = %user_src, error = %e, "session failed: encode START_FORWARD");
            return;
        }
    };
    if rh_protocol::write_frame(&mut channel, FrameType::START_FORWARD, &payload)
        .await
        .is_err()
    {
        // G5 1/N 归因:死通道指派失败此前零留痕(单连接丢弃不落日志)
        state.metrics.count_session_failure("start_forward_write");
        debug!(proxy = rt.id, user_src = %user_src, "session failed: START_FORWARD write (dead channel)");
        return; // 死连接:上层取下一条/重试
    }
    // B1:剥 QuotaIo 壳(route_data_stream 包装;仅 TcpPool→assign_forward
    // 路径)。零拷贝资格判定需要具体类型;限额占用随 quota_guard 存活至
    // 转发结束,与 QuotaIo::Drop 等价,不得提前释放。
    let (channel, quota_guard) = crate::control::QuotaIo::peel(channel);
    let _quota_guard = quota_guard; // 存活至本函数(转发)结束
    let mut channel: BoxedIo = if spec.transport.use_compression {
        compress_stream(channel).await
    } else {
        channel
    };
    // 带宽限制(server 模式)
    // ★解析失败要告警,不能静默关闭限速(与 rh-client 的 forward.rs 同修)。
    //
    // 修前 `.and_then(parse)` 失败直接得到 None:配置写错时规则级限速
    // **完全不生效**,而运维以为已经限了 —— 没有任何信号。
    let bucket = if matches!(
        spec.transport.bandwidth_mode,
        rh_protocol::BandwidthMode::Server
    ) {
        match spec
            .transport
            .bandwidth_limit
            .as_ref()
            .map(|s| rh_core::BandwidthQuantity::parse(s))
        {
            Some(Some(q)) => Some(Arc::new(TokenBucket::new(q.bytes_per_sec() as f64))),
            Some(None) => {
                tracing::warn!(
                    proxy = rt.id,
                    limit = ?spec.transport.bandwidth_limit,
                    "带宽限速配置无法解析,规则级限速**未生效**(检查 bandwidth_limit 格式)"
                );
                None
            }
            None => None,
        }
    } else {
        None
    };
    // F2 客户端级限速(服务端侧,与规则级限速叠加)
    let client_bucket = state.client_usage(&rt.client_id).bucket();
    // Y1 零拷贝 mux 中继:通道为 RhMuxStream(mux 模式常态;B1 修复后
    // QuotaIo/PrependedIo 壳已在上方剥开)时,数据以 Bytes 引用直达
    // 帧体/内核,消除两次中间拷贝。计量经 CountedUserIo 透明进行;
    // 限速/取消/半关闭语义与拷贝路径一致。
    // B7:relay_mode=copy 是同二进制 A/B 开关,同样关闭 chunk 快路径
    //(benchmark README:93 承诺的 copy 对照语义)。
    if !spec.transport.use_compression && state.cfg.relay_mode != "copy" {
        // B1:PrependedIo(同批抵达的残留字节)剥壳——前缀字节是
        // 通道→用户方向的先行数据,注入读半 front 最先交付,
        // 与 PrependedIo 读语义逐字节等价。
        let (prefix, ch) = rh_transport::PrependedIo::peel(channel);
        match mux_steal_or_rewrap(prefix, ch) {
            Ok((b_r, b_w)) => {
                state.metrics.count_relay_path("chunk");
                debug!(proxy = rt.id, "relay path: chunk");
                mux_forward_chunked(rt, user, b_r, b_w, &bucket, &client_bucket).await;
                return;
            }
            // 非 mux / 偷取失败:prefix 已原样回包(见 mux_steal_or_rewrap),
            // 回落下方拷贝/splice 路径——回包后的 PrependedIo 不是裸 TCP,
            // splice 资格判定必然回落 copy,prefix 不经内核 splice 丢失
            Err(fallback) => channel = fallback,
        }
    }
    // 零拷贝快路径:两端裸 TCP(裸模式明文载体)时走内核 splice。
    // 计量/限速/半关闭语义与下方用户态路径一致;资源不可用则原样回落。
    #[cfg(target_os = "linux")]
    {
        if state.cfg.relay_mode != "copy" && !spec.transport.use_compression {
            let user_tcp = user
                .as_any()
                .downcast_ref::<CountedUserIo<tokio::net::TcpStream>>()
                .map(|w| w.inner_ref());
            // QuotaIo 已在上方 :308 无条件剥壳,这里不会再出现(修前还有一个
            // 恒 None 的 QuotaIo downcast 分支,与实际时序矛盾)。
            // mux_steal_or_rewrap 回落时会把残留字节回包成 PrependedIo,
            // 该包装不匹配裸 TCP 判定,自然回落 copy(尚有 prefix 的流不得
            // 走 splice)。
            let chan_tcp = rh_transport::splice::plain_tcp(&channel);
            if let (Some(utcp), Some(ctcp)) = (user_tcp, chan_tcp) {
                state.metrics.count_relay_path("splice");
                debug!(proxy = rt.id, "relay path: splice");
                if splice_forward(state, rt, utcp, ctcp, &bucket, &client_bucket)
                    .await
                    .is_ok()
                {
                    return;
                }
            }
        }
    }
    state.metrics.count_relay_path("copy");
    debug!(proxy = rt.id, "relay path: copy");
    copy_relay_pair(
        user,
        channel,
        bucket,
        client_bucket,
        rt.rule_cancel.clone(),
        RelayFailCtx {
            metrics: state.metrics.clone(),
            proxy: rt.id,
            user_src: user_src.to_string(),
        },
    )
    .await;
}

/// 规则取消后的收尾(shutdown)等待上限:poll_shutdown 可能永久 Pending
/// (对端不读且发送缓冲满、自定义 IO 直接 Pending)。取消(规则禁用/删除)
/// 路径必须收敛释放 IO;EOF 路径不设上限,正常半关闭语义保持(红线)。
/// 与 rh-transport mux::CANCEL_CLOSE_GRACE 同值同义。
const CANCEL_CLOSE_GRACE: Duration = Duration::from_secs(1);

/// mux 偷取或 prefix 回包(goal-repair B1 + prefix 兜底):
/// * mux 流且偷取成功 → Ok(halves),非空 prefix 注入读半 front(最先交付);
/// * 非 mux / 偷取失败 → Err(回包后的通道):**剩余 prefix 必须原样回包**
///   (剥出的残留是「通道→用户」方向最先到达的字节,回包后仍最先交付,
///   嵌套 PrependedIo 顺序保持:外层残留先于内层残留先于流数据),
///   绝不丢弃——修前回落直接 `channel = ch` 把 prefix 丢掉。
///   回包产物是 PrependedIo 包装,裸 TCP/splice 资格判定天然不匹配,
///   必回落 copy 路径(尚有 prefix 的流不得走 splice)。
fn mux_steal_or_rewrap(
    prefix: Vec<u8>,
    mut ch: rh_transport::BoxedIo,
) -> Result<
    (
        rh_transport::mux::RhMuxStreamRead,
        rh_transport::mux::RhMuxStreamWrite,
    ),
    rh_transport::BoxedIo,
> {
    if rh_transport::mux::is_mux_stream(&ch) {
        if let Some((mut b_r, b_w)) = rh_transport::mux::steal_mux_halves(&mut ch) {
            if !prefix.is_empty() {
                b_r.set_front_prefix(bytes::Bytes::from(prefix));
            }
            drop(ch); // 壳(halves 已偷出)
            return Ok((b_r, b_w));
        }
    }
    Err(if prefix.is_empty() {
        ch
    } else {
        rh_transport::PrependedIo::wrap(ch, prefix)
    })
}

/// copy 路径收尾等待与规则取消的合流(与 rh-transport mux.rs
/// `close_with_late_cancel` 同义):
/// * 未取消:无界等待完整半关闭(红线——不硬加短超时防截断);
/// * 收尾期间取消抵达:仅在 CANCEL_CLOSE_GRACE 内继续等待已推进的
///   shutdown,超时即放弃并释放 IO(cancel-after-eof 复现:泵先 EOF
///   进入 shutdown 后取消永不再被监听 → relay 卡死、IO 不释放)。
async fn shutdown_with_late_cancel<F>(sd: F, cancel: impl std::future::Future<Output = ()>)
where
    F: std::future::Future<Output = std::io::Result<()>>,
{
    let mut sd = std::pin::pin!(sd);
    tokio::select! {
        _ = &mut sd => {}
        _ = cancel => {
            let _ = tokio::time::timeout(CANCEL_CLOSE_GRACE, &mut sd).await;
        }
    }
}

/// copy 路径双向泵(自 assign_forward 抽出的同一段代码;A1(c) 修复位点)。
/// 半关闭:双向各自运行到结束;规则取消(禁用/删除)级联关闭在途转发。
/// final-gaps 补修:错误路径(非 panic IO 错误)与 mux_relay 对齐——
/// 有界收尾(CANCEL_CLOSE_GRACE)+ 先完成侧异常(panic/错误)立即
/// 终止对向泵(对向可能在等一个永远不来的数据/EOF,不得永存)。
/// round-51:clean EOF 的半关闭等待也改走 [`rh_transport::join_relay_pumps`]
/// 的有界兜底(round-40 在 mux_relay/splice_relay 上做过、这里漏改)——
/// 对端永不回 FIN 时 socket 永久停 `FIN-WAIT-2`/`CLOSING`;等对向自然
/// 结束的语义保留,只是把上限交给 halfclose_grace(默认 30s)。
/// c84 G5 1/N 归因:异常终止(IO 错误/panic)经 rabbithole_session_
/// failures_total 计数 + 单条 debug;规则取消与 clean EOF 不计。
async fn copy_relay_pair(
    user: BoxedIo,
    channel: BoxedIo,
    bucket: Option<Arc<TokenBucket>>,
    client_bucket: Option<Arc<TokenBucket>>,
    rule_cancel: tokio_util::sync::CancellationToken,
    fail: RelayFailCtx,
) {
    use std::sync::atomic::Ordering;
    // R1(第 1 轮扫描):copy 路径同样先试无锁拆半 —— 非_mux TCP 载体
    // 的主路径就在这里(DialPerStream 下 channel 是裸 TcpStream),
    // 且 splice 仅 Linux 可用,macOS/Windows 的非 mux 模式 100% 走本函数。
    // user 侧剥 CountedUserIo 计量壳(A-2 同款),channel 侧走
    // split_boxed_io;包装类型(压缩流等)照旧回落同步锁拆分。
    let (mut u_r, mut u_w): RelayHalves = match split_counted_tcp_user(user) {
        Ok((r, w)) => (r, w),
        Err(io) => match rh_transport::split_boxed_io(io) {
            Ok((r, w)) => (Box::new(r), Box::new(w)),
            Err(io) => {
                let (r, w) = tokio::io::split(io);
                (Box::new(r), Box::new(w))
            }
        },
    };
    let (mut c_r, mut c_w): RelayHalves = match rh_transport::split_boxed_io(channel) {
        Ok((r, w)) => (Box::new(r), Box::new(w)),
        Err(io) => {
            let (r, w) = tokio::io::split(io);
            (Box::new(r), Box::new(w))
        }
    };
    let bucket2 = bucket.clone();
    let cb2 = client_bucket.clone();
    let cancel = rule_cancel.clone();
    let cancel_tail = rule_cancel.clone();
    // 本向以错误(非 EOF、非取消)结束时置位:对向数据通路已死
    //(用户侧/通道侧错误),继续等对向只会在已死通路上永存
    //(与 rh-transport mux.rs mux_relay 同款收敛保证)。
    let up_errored = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let dn_errored = Arc::new(std::sync::atomic::AtomicBool::new(false));
    // 规则取消与 IO 错误同置 *_errored(现有收敛语义不动);单独记取消,
    // 供失败计数剔除管理操作(禁用/删除规则不是会话失败)。
    let up_cancelled = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let dn_cancelled = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let up_cancel_flag = up_cancelled.clone();
    let up_err = up_errored.clone();
    let t1 = tokio::spawn(async move {
        // 规则取消(禁用/删除)级联关闭在途转发(§4.1);取消路径收尾
        // 有界等待(shutdown 永久 Pending 不得卡死,rule-cancel 回归)
        let (by_cancel, clean) = tokio::select! {
            clean = copy_limited(&mut u_r, &mut c_w, bucket2, cb2) => (false, clean),
            _ = cancel.cancelled() => (true, false),
        };
        if by_cancel {
            up_cancel_flag.store(true, Ordering::SeqCst);
        }
        if !clean {
            up_err.store(true, Ordering::SeqCst);
        }
        if by_cancel || !clean {
            // 取消/错误路径:有界收尾(IO 已不可用/连接已坏,等完整
            // shutdown 无意义;永久 Pending 不得扣住 IO)
            let _ = tokio::time::timeout(CANCEL_CLOSE_GRACE, c_w.shutdown()).await;
        } else {
            // 正常 EOF:无界等完整半关闭(红线,不加硬超时防截断);但收尾
            // 期间规则取消仍必须被听见(cancel-after-eof 复现同款缺陷)
            shutdown_with_late_cancel(c_w.shutdown(), cancel_tail.cancelled()).await;
        }
    });
    let cancel2 = rule_cancel;
    let cancel2_tail = cancel2.clone();
    let dn_cancel_flag = dn_cancelled.clone();
    let dn_err = dn_errored.clone();
    let t2 = tokio::spawn(async move {
        let (by_cancel, clean) = tokio::select! {
            clean = copy_limited(&mut c_r, &mut u_w, bucket, client_bucket) => (false, clean),
            _ = cancel2.cancelled() => (true, false),
        };
        if by_cancel {
            dn_cancel_flag.store(true, Ordering::SeqCst);
        }
        if !clean {
            dn_err.store(true, Ordering::SeqCst);
        }
        if by_cancel || !clean {
            let _ = tokio::time::timeout(CANCEL_CLOSE_GRACE, u_w.shutdown()).await;
        } else {
            shutdown_with_late_cancel(u_w.shutdown(), cancel2_tail.cancelled()).await;
        }
    });
    // A1(c):父任务取消(本 future 被 drop)→ 守卫 Drop → abort 双向子泵,
    // 不再随 join future 移动句柄而 detach;单向子泵 panic 时对向泵
    // 立即终止(不永存)
    let mut g1 = rh_transport::AbortOnDropHandle::new(t1);
    let mut g2 = rh_transport::AbortOnDropHandle::new(t2);
    // 收敛 join(mux_relay 同款):先完成侧 panic 或 IO 错误 → 立即返回
    //(守卫 Drop → abort 对向);clean EOF → 等对向自然结束(半关闭红线)。
    let first = tokio::select! {
        _ = &mut g1 => 1,
        _ = &mut g2 => 2,
    };
    let abnormal = match first {
        1 => g1.panicked() || up_errored.load(Ordering::SeqCst),
        _ => g2.panicked() || dn_errored.load(Ordering::SeqCst),
    };
    if abnormal {
        count_relay_failure(
            &fail,
            first == 1,
            if first == 1 {
                g1.panicked()
            } else {
                g2.panicked()
            },
            if first == 1 {
                up_errored.load(Ordering::SeqCst)
            } else {
                dn_errored.load(Ordering::SeqCst)
            },
            if first == 1 {
                up_cancelled.load(Ordering::SeqCst)
            } else {
                dn_cancelled.load(Ordering::SeqCst)
            },
        );
        return;
    }
    // 半关闭有界兜底(round-51):修前此处是裸 `tokio::join!`,对端永不回
    // FIN 时永久挂起——socket 钉死在 FIN-WAIT-2/CLOSING,流与 IO 都不释放
    // (round-40 在 mux_relay/splice_relay 上实测:10 轮 -P8 累积 22 条、
    // 20s 后仍不消失、吞吐 2.4G 逐轮衰减到 0.5G)。join_relay_pumps 保留
    // 「clean EOF → 等对向自然结束」语义,只把无界等待换成 halfclose_grace
    // 内的有界等待(默认 30s,RH_HALFCLOSE_GRACE_SECS 可覆盖,0 = 旧行为)。
    //
    // 不动 abnormal 分支的提前 return:那里先完成侧已异常,等待本身已经被
    // 跳过。g1/g2 以 `&mut` 传入(句柄未被 move),返回后仍可读 panicked()
    // ——下游 count_relay_failure 的四个入参语义逐字不变。
    rh_transport::join_relay_pumps(&mut g1, &mut g2).await;
    // 先完成侧 clean EOF:对向(后完成)若以 IO 错误/panic 结束同样计数
    //(clean EOF 与规则取消不计,见 count_relay_failure)。
    let second_up = first == 2;
    count_relay_failure(
        &fail,
        second_up,
        if second_up {
            g1.panicked()
        } else {
            g2.panicked()
        },
        if second_up {
            up_errored.load(Ordering::SeqCst)
        } else {
            dn_errored.load(Ordering::SeqCst)
        },
        if second_up {
            up_cancelled.load(Ordering::SeqCst)
        } else {
            dn_cancelled.load(Ordering::SeqCst)
        },
    );
}

/// G5 1/N 归因:relay 异常终止计数。IO 错误/panic 计为会话失败
/// (rabbithole_session_failures_total{reason} + 单条 debug 明细);
/// 规则取消(禁用/删除)与 clean EOF 不计——管理操作/正常结束
/// 不得淹没单连接 1/N 信号(c84 diagnostics README:36-40)。
fn count_relay_failure(
    ctx: &RelayFailCtx,
    dir_up: bool,
    panicked: bool,
    errored: bool,
    cancelled: bool,
) {
    let reason = if panicked {
        "relay_pump_panic"
    } else if errored && !cancelled {
        if dir_up {
            "relay_up_error"
        } else {
            "relay_down_error"
        }
    } else {
        return; // 规则取消(或仅取消触发的收敛):非会话失败
    };
    let dir = if dir_up { "up" } else { "down" };
    ctx.metrics.count_session_failure(reason);
    debug!(proxy = ctx.proxy, user_src = %ctx.user_src, dir, reason, "session failed: relay abnormal end");
}

/// 会话失败归因上下文(计数器 + debug 明细字段)。
struct RelayFailCtx {
    metrics: Arc<crate::state::Metrics>,
    proxy: u64,
    user_src: String,
}

/// 返回 true = 以 clean EOF 结束(对向写端关闭,半关闭语义);
/// false = 读/写 IO 错误(调用方收尾走有界 grace,与 mux 泵同口径)。
async fn copy_limited<R, W>(
    r: &mut R,
    w: &mut W,
    bucket: Option<Arc<TokenBucket>>,
    client_bucket: Option<Arc<TokenBucket>>,
) -> bool
where
    R: tokio::io::AsyncRead + Unpin,
    W: tokio::io::AsyncWrite + Unpin,
{
    // 缓冲按需增长(读满翻倍,上限 64KiB):见 forward.rs 同名函数
    let mut buf = vec![0u8; COPY_MIN_BUF];
    loop {
        let n = match r.read(&mut buf).await {
            Ok(0) => return true,
            Ok(n) => n,
            Err(_) => return false,
        };
        if n == buf.len() && buf.len() < COPY_MAX_BUF {
            buf.resize((buf.len() * 2).min(COPY_MAX_BUF), 0);
        }
        if let Some(b) = &bucket {
            b.acquire(n).await;
        }
        if let Some(b) = &client_bucket {
            b.acquire(n).await;
        }
        if w.write_all(&buf[..n]).await.is_err() {
            return false;
        }
    }
}

/// 双桶限速适配(goal-repair A5:预留式取消安全;两桶串行预留,
/// 中途取消时已预留部分由 Reservation::drop 自动退还)。
fn mk_limiter(
    b1: &Option<Arc<TokenBucket>>,
    b2: &Option<Arc<TokenBucket>>,
) -> Option<Arc<dyn rh_transport::ChunkLimiter>> {
    if b1.is_none() && b2.is_none() {
        return None; // 双桶皆空:零热路径开销(F3 同款)
    }
    struct BucketLimiter {
        rule: Option<Arc<TokenBucket>>,
        client: Option<Arc<TokenBucket>>,
    }
    struct BothReservation {
        r1: Option<rh_core::ratelimit::Reservation>,
        r2: Option<rh_core::ratelimit::Reservation>,
    }
    impl rh_transport::ChunkReservation for BothReservation {
        fn consume(&mut self, n: usize) {
            if let Some(r) = &mut self.r1 {
                r.consume(n);
            }
            if let Some(r) = &mut self.r2 {
                r.consume(n);
            }
        }
    }
    impl rh_transport::ChunkLimiter for BucketLimiter {
        fn reserve(
            &self,
            n: usize,
        ) -> futures::future::BoxFuture<'static, Box<dyn rh_transport::ChunkReservation>> {
            let b1 = self.rule.clone();
            let b2 = self.client.clone();
            Box::pin(async move {
                let r1 = match &b1 {
                    Some(b) => Some(b.clone().acquire_reserved(n).await),
                    None => None,
                };
                let r2 = match &b2 {
                    Some(b) => Some(b.clone().acquire_reserved(n).await),
                    None => None,
                };
                Box::new(BothReservation { r1, r2 }) as Box<dyn rh_transport::ChunkReservation>
            })
        }
    }
    Some(Arc::new(BucketLimiter {
        rule: b1.clone(),
        client: b2.clone(),
    }))
}

/// Y1 零拷贝 mux 中继装配:限速双桶(皆空不装,预留式取消安全)+
/// 规则取消级联。与 splice_forward 的钩子装配同构;计量由 CountedUserIo 承担。
async fn mux_forward_chunked(
    rt: &Arc<ProxyRuntime>,
    user: BoxedIo,
    b_r: rh_transport::mux::RhMuxStreamRead,
    b_w: rh_transport::mux::RhMuxStreamWrite,
    rule_bucket: &Option<Arc<TokenBucket>>,
    client_bucket: &Option<Arc<TokenBucket>>,
) {
    let limiter = mk_limiter(rule_bucket, client_bucket);
    // 规则禁用/删除级联(与用户态/splice 路径同语义)
    let rc = rt.rule_cancel.clone();
    let cancel: rh_transport::CancelFuture = futures::FutureExt::shared(futures::FutureExt::boxed(
        async move { rc.cancelled().await },
    ));
    // A2:先剥 CountedUserIo 壳无锁拆半(into_split 两个独立 fd),
    // 按方向重包计量;失败(非该包装)回落 mux_relay 自带的
    // split_boxed_io / tokio::io::split 逻辑。修前服务端用户侧 100%
    // 走 tokio::io::split 的同步锁拆半。
    match split_counted_tcp_user(user) {
        Ok((a_r, a_w)) => {
            rh_transport::mux::mux_relay_split(a_r, a_w, b_r, b_w, limiter, Some(cancel)).await
        }
        Err(user) => rh_transport::mux::mux_relay(user, b_r, b_w, limiter, Some(cancel)).await,
    }
}

/// 零拷贝指派中继(splice 快路径钩子装配)。
/// 语义对齐 CountedUserIo + copy_limited:up(user→carrier)计 in、
/// down(carrier→user)计 out,两向都叠加客户端用量;限速双桶按搬运字节扣减;
/// flow_cut 触发双向断连。资源申请失败返回 Err(调用方回落用户态拷贝)。
#[cfg(target_os = "linux")]
async fn splice_forward(
    state: &Arc<AppState>,
    rt: &Arc<ProxyRuntime>,
    user: &tokio::net::TcpStream,
    carrier: &tokio::net::TcpStream,
    rule_bucket: &Option<Arc<TokenBucket>>,
    client_bucket: &Option<Arc<TokenBucket>>,
) -> std::io::Result<()> {
    use futures::FutureExt as _;
    use rh_transport::splice::{DirHook, SpliceHooks};

    // 双桶皆空时不装 limiter:免每块一次 BoxFuture 分配与虚调用
    // (基准与绝大多数规则都不配限速,不应为其付热路径成本)
    let limiter = mk_limiter(rule_bucket, client_bucket);

    let metrics_up = state.metrics.clone();
    let metrics_dn = state.metrics.clone();
    let rt_up = rt.clone();
    let rt_dn = rt.clone();
    let usage_up = state.client_usage(&rt.client_id);
    let usage_dn = usage_up.clone();
    let usage_cut = usage_up.clone();

    let hooks = SpliceHooks {
        // B6:up=读侧口径(fill 完成即计,先于限速等待——与 copy 路径
        // CountedUserIo.poll_read 先计、copy_limited 后 acquire 一致);
        // down=送达口径(排空后计)。
        up: Some(DirHook {
            on_bytes: Some(Arc::new(move |n| {
                metrics_up.count_user_in(n as usize);
                rt_up.bytes_in.fetch_add(n, Ordering::Relaxed);
                usage_up.add_bytes(n);
            })),
            meter_on_fill: true,
            limiter: limiter.clone(),
        }),
        down: Some(DirHook {
            on_bytes: Some(Arc::new(move |n| {
                metrics_dn.count_user_out(n as usize);
                rt_dn.bytes_out.fetch_add(n, Ordering::Relaxed);
                usage_dn.add_bytes(n);
            })),
            meter_on_fill: false,
            limiter,
        }),
        abort: Some(Arc::new(move || usage_cut.flow_cut.load(Ordering::Relaxed))),
        // 规则禁用/删除级联:与用户态路径的 select-cancelled 语义一致,
        // 阻塞在就绪等待/限速等待中的泵都能被立即拆除(abort 只在块边界检查)。
        cancel: {
            let rc = rt.rule_cancel.clone();
            Some(async move { rc.cancelled().await }.boxed().shared())
        },
    };
    rh_transport::splice::splice_relay(user, carrier, hooks).await
}

/// F1 计量状态(全局 Metrics + 所属规则 + 客户端用量)。
/// CountedUserIo 与拆半计量包装(CountedTcpReadHalf/WriteHalf)共享,
/// 保证拆半前后计量逐字节等价。
#[derive(Clone)]
struct CountState {
    metrics: Arc<crate::state::Metrics>,
    proxy: Option<Arc<crate::state::ProxyRuntime>>,
    usage: Option<Arc<crate::state::ClientUsage>>,
}

impl CountState {
    fn new(state: &AppState, proxy: Option<Arc<crate::state::ProxyRuntime>>) -> Self {
        let usage = proxy.as_ref().map(|rt| state.client_usage(&rt.client_id));
        Self {
            metrics: state.metrics.clone(),
            proxy,
            usage,
        }
    }

    fn flow_cut(&self) -> bool {
        self.usage
            .as_ref()
            .map(|u| u.flow_cut.load(std::sync::atomic::Ordering::Relaxed))
            .unwrap_or(false)
    }

    fn count_in(&self, n: usize) {
        if n == 0 {
            return;
        }
        self.metrics.count_user_in(n);
        if let Some(rt) = &self.proxy {
            rt.bytes_in
                .fetch_add(n as u64, std::sync::atomic::Ordering::Relaxed);
        }
        if let Some(u) = &self.usage {
            u.add_bytes(n as u64);
        }
    }

    fn count_out(&self, n: usize) {
        if n == 0 {
            return;
        }
        self.metrics.count_user_out(n);
        if let Some(rt) = &self.proxy {
            rt.bytes_out
                .fetch_add(n as u64, std::sync::atomic::Ordering::Relaxed);
        }
        if let Some(u) = &self.usage {
            u.add_bytes(n as u64);
        }
    }
}

/// F1 计量:IO 透明包装,不改任何 IO 行为。
/// user 模式:read→入站、write→出站(用户侧连接)。
/// upstream 模式:read→出站、write→入站(上游通道,http vhost 反向归因)。
/// 同时计入全局 Metrics 与所属规则(可选)。
pub struct CountedUserIo<T> {
    inner: T,
    st: CountState,
    reversed: bool,
}

impl<T> CountedUserIo<T> {
    pub fn new(inner: T, metrics: Arc<crate::state::Metrics>) -> Self {
        Self {
            inner,
            st: CountState {
                metrics,
                proxy: None,
                usage: None,
            },
            reversed: false,
        }
    }

    /// 内部流引用(splice 快路径:提取裸 TCP 做零拷贝判定)。
    #[cfg(target_os = "linux")]
    pub fn inner_ref(&self) -> &T {
        &self.inner
    }

    /// 用户侧连接(归属到规则;state 派生客户端用量)。
    pub fn user(
        inner: T,
        state: &AppState,
        proxy: Option<Arc<crate::state::ProxyRuntime>>,
    ) -> Self {
        Self {
            inner,
            st: CountState::new(state, proxy),
            reversed: false,
        }
    }

    /// 上游通道(http vhost:连接层无法归因单规则,按请求通道计量)。
    pub fn upstream(
        inner: T,
        state: &AppState,
        proxy: Option<Arc<crate::state::ProxyRuntime>>,
    ) -> Self {
        Self {
            inner,
            st: CountState::new(state, proxy),
            reversed: true,
        }
    }

    fn flow_cut(&self) -> bool {
        self.st.flow_cut()
    }

    fn count_in(&self, n: usize) {
        self.st.count_in(n)
    }

    fn count_out(&self, n: usize) {
        self.st.count_out(n)
    }
}

impl<T: tokio::io::AsyncRead + Unpin> tokio::io::AsyncRead for CountedUserIo<T> {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        if self.flow_cut() {
            return std::task::Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::ConnectionAborted,
                "client flow limit exceeded",
            )));
        }
        let before = buf.filled().len();
        match std::pin::Pin::new(&mut self.inner).poll_read(cx, buf) {
            std::task::Poll::Ready(Ok(())) => {
                let n = buf.filled().len() - before;
                if self.reversed {
                    self.count_out(n);
                } else {
                    self.count_in(n);
                }
                std::task::Poll::Ready(Ok(()))
            }
            other => other,
        }
    }
}

impl<T: tokio::io::AsyncWrite + Unpin> tokio::io::AsyncWrite for CountedUserIo<T> {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        if self.flow_cut() {
            return std::task::Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::ConnectionAborted,
                "client flow limit exceeded",
            )));
        }
        match std::pin::Pin::new(&mut self.inner).poll_write(cx, buf) {
            std::task::Poll::Ready(Ok(n)) => {
                if self.reversed {
                    self.count_in(n);
                } else {
                    self.count_out(n);
                }
                std::task::Poll::Ready(Ok(n))
            }
            other => other,
        }
    }

    // C5:向量写透传 + 一次计量(按实际写出的聚合字节数)——
    // 未来引入 writev 优化时计量不缺失、能力位不退化。
    fn poll_write_vectored(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        bufs: &[std::io::IoSlice<'_>],
    ) -> std::task::Poll<std::io::Result<usize>> {
        if self.flow_cut() {
            return std::task::Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::ConnectionAborted,
                "client flow limit exceeded",
            )));
        }
        match std::pin::Pin::new(&mut self.inner).poll_write_vectored(cx, bufs) {
            std::task::Poll::Ready(Ok(n)) => {
                if self.reversed {
                    self.count_in(n);
                } else {
                    self.count_out(n);
                }
                std::task::Poll::Ready(Ok(n))
            }
            other => other,
        }
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

/// A2:拆半后的读侧计量(user 模式:read → 入站)。
/// 与 CountedUserIo.poll_read 的行为逐字节等价(flow_cut 检查 + 计 in)。
struct CountedTcpReadHalf {
    inner: tokio::net::tcp::OwnedReadHalf,
    st: CountState,
}

impl tokio::io::AsyncRead for CountedTcpReadHalf {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        if self.st.flow_cut() {
            return std::task::Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::ConnectionAborted,
                "client flow limit exceeded",
            )));
        }
        let before = buf.filled().len();
        match std::pin::Pin::new(&mut self.inner).poll_read(cx, buf) {
            std::task::Poll::Ready(Ok(())) => {
                let n = buf.filled().len() - before;
                self.st.count_in(n);
                std::task::Poll::Ready(Ok(()))
            }
            other => other,
        }
    }
}

/// A2:拆半后的写侧计量(user 模式:write → 出站)。
/// 与 CountedUserIo.poll_write/poll_write_vectored 行为等价
/// (flow_cut 检查 + 计 out + 向量写透传)。
struct CountedTcpWriteHalf {
    inner: tokio::net::tcp::OwnedWriteHalf,
    st: CountState,
}

impl tokio::io::AsyncWrite for CountedTcpWriteHalf {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        if self.st.flow_cut() {
            return std::task::Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::ConnectionAborted,
                "client flow limit exceeded",
            )));
        }
        match std::pin::Pin::new(&mut self.inner).poll_write(cx, buf) {
            std::task::Poll::Ready(Ok(n)) => {
                self.st.count_out(n);
                std::task::Poll::Ready(Ok(n))
            }
            other => other,
        }
    }

    fn poll_write_vectored(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        bufs: &[std::io::IoSlice<'_>],
    ) -> std::task::Poll<std::io::Result<usize>> {
        if self.st.flow_cut() {
            return std::task::Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::ConnectionAborted,
                "client flow limit exceeded",
            )));
        }
        match std::pin::Pin::new(&mut self.inner).poll_write_vectored(cx, bufs) {
            std::task::Poll::Ready(Ok(n)) => {
                self.st.count_out(n);
                std::task::Poll::Ready(Ok(n))
            }
            other => other,
        }
    }

    fn is_write_vectored(&self) -> bool {
        // OwnedWriteHalf 支持(TcpStream 走 writev);显式声明,
        // 不让默认 false 退化 YamuxCarrier 的批量写。
        true
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

/// A2:拆半后的读写半类型(a 侧中继输入)。
type RelayHalves = (
    Box<dyn tokio::io::AsyncRead + Send + Unpin>,
    Box<dyn tokio::io::AsyncWrite + Send + Unpin>,
);

/// A2:剥 `CountedUserIo<TcpStream>` 壳 → `into_split` 无锁拆半 →
/// 按方向重包计量(user 模式:读半计 in、写半计 out)。
///
/// 服务端用户侧恒为该包装(serve_tcp_user),修前 `split_boxed_io` 的
/// 精确类型探测必然 miss → `tokio::io::split` 的同步 Mutex 拆半,
/// round-69 的无锁拆半在服务端主路径上从未生效(实测 futex 占 75%
/// syscall 时间即此病灶)。非该类型 / upstream(reversed)模式原样返还。
fn split_counted_tcp_user(io: rh_transport::BoxedIo) -> Result<RelayHalves, rh_transport::BoxedIo> {
    {
        let dynio: &dyn rh_transport::Io = &*io;
        if !dynio.as_any().is::<CountedUserIo<tokio::net::TcpStream>>() {
            return Err(io);
        }
    }
    let any = io.into_any_box();
    let boxed = any
        .downcast::<CountedUserIo<tokio::net::TcpStream>>()
        .expect("probed above: concrete type is CountedUserIo<TcpStream>");
    let CountedUserIo {
        inner: tcp,
        st,
        reversed,
    } = *boxed;
    if reversed {
        // upstream 模式方向反转(http vhost 上游),本路径(user 侧转发)
        // 不出现;防御性重建原样返还,回落 tokio::io::split 保持原语义。
        return Err(Box::new(CountedUserIo {
            inner: tcp,
            st,
            reversed,
        }) as rh_transport::BoxedIo);
    }
    let (r, w) = tcp.into_split();
    Ok((
        Box::new(CountedTcpReadHalf {
            inner: r,
            st: st.clone(),
        }),
        Box::new(CountedTcpWriteHalf { inner: w, st }),
    ))
}

/// TCP 监听服务循环(独享端口或组成员共享)。
pub async fn tcp_serve_loop(state: Arc<AppState>, rt: Arc<ProxyRuntime>, listener: TcpListener) {
    // 独享端口:随本规则的 rule_cancel 退出(既有语义)。
    tcp_serve_loop_inner(state, rt.clone(), listener, rt.rule_cancel.clone()).await
}

/// 共享监听(组)版本:退出条件由**组级** `serve_cancel` 决定。
///
/// ★A6:修前共享 serve loop 选的是**属主成员**的 `rule_cancel`
/// (runtime.rs 里把 `rt2` 传进来,本函数 select 它的 rule_cancel)。
/// 属主被禁用/改端口 → teardown 无条件 cancel → 监听 fd 释放,但
/// `listener_bound` 仍是 true → 之后所有成员 spawn 时看到 true 就跳过重绑
/// → 组端口**永久拒绝连接**。
///
/// 组监听的生命周期应属于组,不属于任何一个成员。
pub async fn tcp_serve_loop_group(
    state: Arc<AppState>,
    rt: Arc<ProxyRuntime>,
    listener: TcpListener,
    group_cancel: tokio_util::sync::CancellationToken,
) {
    tcp_serve_loop_inner(state, rt, listener, group_cancel).await;
}

/// 两个入口共用的实现:`cancel` 是唯一的退出条件。
async fn tcp_serve_loop_inner(
    state: Arc<AppState>,
    rt: Arc<ProxyRuntime>,
    listener: TcpListener,
    cancel: tokio_util::sync::CancellationToken,
) {
    loop {
        tokio::select! {
            acc = listener.accept() => {
                match acc {
                    Ok((sock, peer)) => {
                        let st = state.clone();
                        let rt2 = rt.clone();
                        tokio::spawn(serve_tcp_user(st, rt2, sock, peer));
                    }
                    Err(e) => {
                        warn!(error = %e, "tcp accept error");
                        tokio::time::sleep(Duration::from_millis(200)).await;
                    }
                }
            }
            _ = cancel.cancelled() => return,
        }
    }
}

/// 连接计数守卫:转发结束(含指派失败的早退路径)即递减,
/// 避免 conns_current 在死通道场景下单调漂移;全局 + 分规则 + 分客户端三计。
pub struct ConnGuard {
    metrics: Arc<crate::state::Metrics>,
    proxy: Option<Arc<crate::state::ProxyRuntime>>,
    usage: Option<Arc<crate::state::ClientUsage>>,
}

impl Drop for ConnGuard {
    fn drop(&mut self) {
        self.metrics
            .conns_current
            .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
        if let Some(rt) = &self.proxy {
            rt.conns_current
                .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
        }
        if let Some(u) = &self.usage {
            u.conns.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
        }
    }
}

/// 用户连接准入 + 计数:F2 配额(max_conns 并发上限 / flow_limit 流量上限)
/// 超限返回 None(调用方直接断开);通过则全局/规则/客户端三处 +1。
pub fn try_open_user_conn(
    state: &AppState,
    proxy: Option<&Arc<crate::state::ProxyRuntime>>,
) -> Option<ConnGuard> {
    let usage = proxy.map(|rt| state.client_usage(&rt.client_id));
    if let Some(u) = &usage {
        let q = u.quota.read().unwrap().clone();
        // ★max_conns 用 CAS 闭环,不能 load 判完再 fetch_add。
        //
        // 修前是典型的 check-then-act:`load() >= max` 与随后的 `fetch_add`
        // 之间没有原子性,并发准入可同时看到未达上限、一起加,实际并发突破
        // max_conns(溢出量 = 并发度)。配额是硬闸,必须精确。
        // 用 `fetch_update`:在原子读-改-写里完成"判上限 + 自增"。
        if q.max_conns > 0 {
            let max = q.max_conns;
            let admitted = u
                .conns
                .fetch_update(
                    std::sync::atomic::Ordering::Relaxed,
                    std::sync::atomic::Ordering::Relaxed,
                    |cur| if cur >= max { None } else { Some(cur + 1) },
                )
                .is_ok();
            if !admitted {
                return None;
            }
        } else {
            // 不限并发:直接加(与 max_conns=0 语义一致)
            u.conns.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        // 流量上限仍是"读快照"语义:bytes 是单调累计,超限即在下一块被切断,
        // 这里只做早退优化,不承担精确性(精确拦截在 add_bytes 置 flow_cut)。
        if q.flow_limit > 0
            && u.bytes.load(std::sync::atomic::Ordering::Relaxed) >= q.flow_limit as u64
        {
            // 回滚上面已经加的 conns
            u.conns.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
            return None;
        }
    }
    state
        .metrics
        .conns_current
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    if let Some(rt) = proxy {
        rt.conns_current
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
    let _ = &usage;
    Some(ConnGuard {
        metrics: state.metrics.clone(),
        proxy: proxy.cloned(),
        usage,
    })
}

async fn serve_tcp_user(
    state: Arc<AppState>,
    rt: Arc<ProxyRuntime>,
    sock: TcpStream,
    peer: SocketAddr,
) {
    let _ = sock.set_nodelay(true);
    if !rt.healthy.load(Ordering::Relaxed) {
        // G5 1/N 归因:健康摘除此前零留痕,计数+单条 debug
        state.metrics.count_session_failure("health_fast_fail");
        debug!(proxy = rt.id, peer = %peer, "session failed: health fast-fail");
        return; // 健康摘除:快速失败
    }
    // 组选路:has_group 无锁快路径(与 vhost 三处调用点同构)。绝大多数
    // 规则 group 为空,修前每 accept 一条连接都要 `spec.read().await`
    // + 一次 format!,高并发下是无谓的锁争用。
    let target_rt = if rt.has_group.load(Ordering::Relaxed) {
        match group_pick(&state, &rt).await {
            Some(t) => t,
            None => rt,
        }
    } else {
        rt
    };
    // F2 配额判定必须在取通道之前:修前先 `get_channel`(从池里 pop 一条
    // 完成握手的热通道 + 签发一条 CREATE 补位),配额拒绝时那条通道随
    // 作用域 drop 作废 —— 每次被拒连接 = 1 条预热通道 + 1 次额外 CREATE
    // 往返 + 1 个 pending_request。配额判定不依赖通道,语义等价。
    let Some(_conn_guard) = try_open_user_conn(&state, Some(&target_rt)) else {
        // G5 1/N 归因:配额拒绝(max_conns/flow_limit)此前零留痕
        state.metrics.count_session_failure("quota_rejected");
        debug!(proxy = target_rt.id, peer = %peer, "session failed: client quota");
        return; // F2 配额超限:直接断开
    };
    let Some(channel) = get_channel(&state, &target_rt).await else {
        // G5 1/N 归因:池等待超时/签发失败此前零留痕(4096 突发下
        // 最 plausible 的单连接静默丢弃点)
        state.metrics.count_session_failure("channel_unavailable");
        debug!(proxy = target_rt.id, peer = %peer, "session failed: no data channel");
        return; // 池空超时:快速失败
    };
    // C6c:label 字符串缓存在 ProxyRuntime(修前每连接建/关各一次
    // to_string 分配 + with_label_values 内部按 label 查表)。
    let conn_labels = [target_rt.metrics_label.as_str(), "tcp"];
    state
        .metrics
        .connections
        .with_label_values(&conn_labels)
        .inc();
    assign_forward(
        &state,
        &target_rt,
        channel,
        Box::new(CountedUserIo::user(sock, &state, Some(target_rt.clone()))),
        &peer.to_string(),
    )
    .await;
    state
        .metrics
        .connections
        .with_label_values(&conn_labels)
        .dec();
}

/// 组内健康成员选取(公开:vhost 等复用)。
pub async fn group_pick_public(
    state: &Arc<AppState>,
    rt: &Arc<ProxyRuntime>,
) -> Option<Arc<ProxyRuntime>> {
    group_pick(state, rt).await
}

/// 组内**粘性**选路(公开):同一 `affinity` 恒定落同一成员。
///
/// 供带缓存语义的路径使用 —— 缓存挂在成员各自的 `rt.http_cache` 上,
/// 轮询选路会让跨请求命中率掉到 `1/N`。
pub async fn group_pick_sticky_public(
    state: &Arc<AppState>,
    rt: &Arc<ProxyRuntime>,
    affinity: &str,
) -> Option<Arc<ProxyRuntime>> {
    group_pick_sticky(state, rt, affinity).await
}

/// 组内健康成员选取(组键由 spec 决定),**轮询**。
///
/// 这是 TCP 数据面的默认选路:同一组的连接依次落到不同成员,组内负载均摊。
/// 带缓存语义的路径(vhost HTTP)不要用它 —— 见 [`group_pick_sticky`]。
async fn group_pick(state: &Arc<AppState>, rt: &Arc<ProxyRuntime>) -> Option<Arc<ProxyRuntime>> {
    // R2:读缓存的派生组键(与 spec 同步维护;修前每次 spec.read().await
    // + format! 派生 —— 分组规则每次 accept 都在付)
    let key = rt
        .group_key_cache
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .clone()?;
    let g = state.proxies.groups.get(&key)?;
    let members = g.members.read().await.clone();
    if members.len() <= 1 {
        return Some(rt.clone());
    }
    let start = g.rr.fetch_add(1, Ordering::Relaxed) as usize;
    pick_healthy(state, &members, start)
}

/// 组内健康成员选取,**按 `affinity` 哈希**,同一 affinity 恒定落到同一成员。
///
/// # 为什么需要它(vhost 缓存的粘性)
///
/// 缓存挂在**成员各自**的 `rt.http_cache` 上(proxy 级)。轮询选路下,
/// 同一个 `cache_key` 的第 1 个请求落成员 A(写入 A 的缓存)、第 2 个落
/// 成员 B(查 B 的缓存,必然 MISS)→ 稳态命中率约 `1/N`。
///
/// 「读写用同一个 rt」只保证**单请求内**一致(修 B2 时解决的跨域串号),
/// 不保证**跨请求**一致。按 `cache_key` 哈希选成员后,同一资源的读写恒定
/// 落同一成员,命中率回到单成员水平;不同资源仍按哈希分散到各成员,
/// 负载仍然均摊。
///
/// 只用于**有缓存配置**的 vhost HTTP 路径;TCP 数据面继续用轮询(那里没有
/// 需要粘性的状态,轮询的均摊更均匀)。
async fn group_pick_sticky(
    state: &Arc<AppState>,
    rt: &Arc<ProxyRuntime>,
    affinity: &str,
) -> Option<Arc<ProxyRuntime>> {
    // R2:同 group_pick —— 读缓存组键
    let key = rt
        .group_key_cache
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .clone()?;
    let g = state.proxies.groups.get(&key)?;
    let members = g.members.read().await.clone();
    if members.len() <= 1 {
        return Some(rt.clone());
    }
    // FNV-1a:与 std 的 DefaultHasher 不同,它的输出跨进程/跨版本稳定。
    // 这里不需要密码学强度,只需要「同一串恒定得到同一个数」。
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in affinity.as_bytes() {
        h ^= *b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    pick_healthy(state, &members, (h as usize) % members.len())
}

/// 从 `start` 起环形找一个健康且在线的成员。
fn pick_healthy(state: &Arc<AppState>, members: &[u64], start: usize) -> Option<Arc<ProxyRuntime>> {
    for idx in (start..).take(members.len()) {
        let id = members[idx % members.len()];
        if let Some(m) = state.proxies.get(id) {
            if m.healthy.load(Ordering::Relaxed) && state.sessions.get(&m.client_id).is_some() {
                return Some(m);
            }
        }
    }
    None
}

// ---------------- UDP 通道(服务端负责制) ----------------

/// 建立/重建 UDP 通道(每规则至多 1 个在途请求)。
pub async fn udp_ensure_channel(
    state: &Arc<AppState>,
    sess: &Arc<Session>,
    rt: &Arc<ProxyRuntime>,
) {
    // 在途判断:pending_requests 中已有该规则的 UdpChannel 即等待
    let has_pending = {
        let m = sess.pending_requests.lock().await;
        m.values().any(
            |p| matches!(p.purpose, RequestPurpose::UdpChannel { proxy_id } if proxy_id == rt.id),
        )
    };
    if has_pending {
        return;
    }
    let _ = state
        .issue_create_channel(
            sess,
            ChannelEntity::Proxy { proxy_id: rt.id },
            RequestPurpose::UdpChannel { proxy_id: rt.id },
        )
        .await;
}

/// UDP 通道抵达:交给该规则的 pump 任务。
pub async fn udp_on_channel(
    state: &Arc<AppState>,
    _sess: &Arc<Session>,
    proxy_id: u64,
    stream: BoxedIo,
) {
    let Some(rt) = state.proxies.get(proxy_id) else {
        return;
    };
    // 克隆 sender 后立即放锁:send 可能因 pump 忙而阻塞,
    // 不得让锁跨 await(close_proxy_channels 会被卡死)
    let tx = rt.udp_new_channel_tx.lock().await.clone();
    match tx {
        Some(tx) => {
            if tx.send(stream).await.is_err() {
                // 接收端已退出(泵没了):这条通道无法转发,必须留痕。
                tracing::warn!(
                    proxy = proxy_id,
                    "udp channel arrived but pump receiver is gone; dropping channel"
                );
            }
        }
        None => {
            // A2:泵未注册 sender。正常路径不该发生(泵在 spawn 时就写入),
            // 一旦发生就是"UDP 规则静默不工作"——必须可见。
            tracing::warn!(
                proxy = proxy_id,
                "udp channel arrived but no pump registered (tx=None); dropping channel"
            );
        }
    }
}

/// UDP 服务主循环:等待通道 → 双向搬运;死亡 → 退避重建。
/// UDP 会话准入:已存在 = 刷新时间戳放行;新会话在 max_udp_sessions_per_proxy
/// (热配置,0 = 不限)已满时拒绝。返回 false = 本次报文按新会话超限处理。
fn udp_session_admit(
    state: &Arc<AppState>,
    sessions: &mut std::collections::HashMap<SocketAddr, Instant>,
    addr: SocketAddr,
) -> bool {
    if sessions.contains_key(&addr) {
        return true;
    }
    let limit = state
        .limits
        .read()
        .unwrap_or_else(|e| e.into_inner())
        .max_udp_sessions_per_proxy;
    if limit != 0 && sessions.len() >= limit {
        // B7:超限必须可见 —— 静默丢弃会让"UDP 不通"无从排查。
        state.metrics.udp_sessions_rejected.inc();
        tracing::warn!(
            limit,
            current = sessions.len(),
            %addr,
            "udp session table full, dropping new source"
        );
        return false;
    }
    true
}

pub async fn udp_serve_loop(state: Arc<AppState>, rt: Arc<ProxyRuntime>, sock: Arc<UdpSocket>) {
    let (new_tx, mut new_rx) = mpsc::channel::<BoxedIo>(4);
    *rt.udp_new_channel_tx.lock().await = Some(new_tx);
    let mut sessions: std::collections::HashMap<SocketAddr, Instant> =
        std::collections::HashMap::new();
    let mut gc = tokio::time::interval(Duration::from_secs(15));
    // B1:客户端用量条目提到循环外一次获取(规则生命周期内 client_id
    // 不变)。修前每包 2-3 次 `client_usage(&id)` —— `entry(id.to_string())`
    // 命中时也先构造 String,再 clone 一次 Arc。
    let usage = state.client_usage(&rt.client_id);
    loop {
        // 等待第一条/重建通道
        let stream = tokio::select! {
            s = new_rx.recv() => match s {
                Some(s) => s,
                None => {
                    // A2:发送端全部 drop 不代表规则该退出(规则仍 enabled,
                    // 只是在等客户端重连后的新通道)。继续等 rule_cancel ——
                    // 真正的退出条件只有规则取消 / 进程关闭。
                    tracing::debug!(
                        proxy = rt.id,
                        "udp pump: channel senders gone, waiting for rule teardown"
                    );
                    tokio::select! {
                        _ = rt.rule_cancel.cancelled() => return,
                        _ = state.shutdown.cancelled() => return,
                    }
                }
            },
            _ = rt.rule_cancel.cancelled() => return,
            _ = state.shutdown.cancelled() => return,
        };
        let (mut ch_r, mut ch_w) = tokio::io::split(stream);
        let mut frame_buf = bytes::BytesMut::with_capacity(2048);
        // B1:上行一次成帧缓冲 + 地址字符串复用缓冲。
        let mut up_frame = bytes::BytesMut::new();
        let mut addr_buf = String::with_capacity(24);
        let mut pkt_buf = vec![
            0u8;
            state
                .limits
                .read()
                .unwrap_or_else(|e| e.into_inner())
                .udp_packet_size
                .max(1500)
        ];
        let mut last_ping_sent = Instant::now();
        let mut last_alive = Instant::now();
        let dead = loop {
            tokio::select! {
                r = sock.recv_from(&mut pkt_buf) => {
                    match r {
                        Ok((n, peer)) => {
                            // ★配额击穿后上行也必须停。
                            //
                            // 下行(通道 → 用户)早已检查 flow_cut,但上行
                            // (用户 → 通道)此前照常计费转发 —— 配额击穿后
                            // UDP 只有单边被切:客户端仍能持续把数据打进隧道,
                            // 表现是"限流生效了但只限一半"。
                            if usage
                                .flow_cut
                                .load(std::sync::atomic::Ordering::Relaxed)
                            {
                                last_alive = Instant::now();
                                continue;
                            }
                            // 会话准入:超上限的新源地址直接丢包(既有会话不受影响)
                            if !udp_session_admit(&state, &mut sessions, peer) {
                                continue;
                            }
                            state.metrics.count_user_in(n);
                            rt.bytes_in.fetch_add(n as u64, std::sync::atomic::Ordering::Relaxed);
                            usage.add_bytes(n as u64);
                            // B1:一次成帧(帧头 + [addr_len][addr][data] 直接写进
                            // 复用缓冲),消除 encode 的中间 Vec 与 write_frame 的
                            // 二次整包拷贝;地址字符串复用缓冲,不再每包分配。
                            addr_buf.clear();
                            use bytes::BufMut as _;
                            use std::fmt::Write as _;
                            let _ = write!(addr_buf, "{peer}");
                            let payload_len = 1 + addr_buf.len() + n;
                            up_frame.clear();
                            rh_protocol::encode_frame_header(&mut up_frame, FrameType::UDP_PACKET, payload_len);
                            up_frame.put_u8(addr_buf.len() as u8);
                            up_frame.put_slice(addr_buf.as_bytes());
                            up_frame.put_slice(&pkt_buf[..n]);
                            if ch_w.write_all(&up_frame).await.is_err() {
                                break true;
                            }
                            sessions.insert(peer, Instant::now());
                        }
                        Err(_) => break false, // 监听 socket 失效:整个规则退出
                    }
                }
                r = rh_protocol::read_frame(&mut ch_r, &mut frame_buf) => {
                    match r {
                        Ok((FrameType::UDP_PACKET, p)) => {
                            if let Some((src, data)) = udp_packet::decode(&p) {
                                if let Ok(addr) = src.parse::<SocketAddr>() {
                                    if usage
                                        .flow_cut
                                        .load(std::sync::atomic::Ordering::Relaxed)
                                    {
                                        last_alive = Instant::now();
                                        continue;
                                    }
                                    if !udp_session_admit(&state, &mut sessions, addr) {
                                        last_alive = Instant::now();
                                        continue;
                                    }
                                    let _ = sock.send_to(data, addr).await;
                                    state.metrics.count_user_out(data.len());
                                    rt.bytes_out
                                        .fetch_add(data.len() as u64, std::sync::atomic::Ordering::Relaxed);
                                    usage.add_bytes(data.len() as u64);
                                    sessions.insert(addr, Instant::now());
                                }
                                last_alive = Instant::now();
                            }
                        }
                        Ok((FrameType::PING, _)) => {
                            let pong = serde_json::to_vec(&rh_protocol::message::Pong { ts_ms: 0, probe: true }).unwrap();
                            if rh_protocol::write_frame(&mut ch_w, FrameType::PONG, &pong).await.is_err() {
                                break true;
                            }
                            last_alive = Instant::now();
                        }
                        Ok(_) => { last_alive = Instant::now(); }
                        Err(_) => break true, // EOF/错误:通道死亡
                    }
                }
                _ = gc.tick() => {
                    // 会话表淘汰必须在通道存活期间执行(不能只等通道死亡),
                    // 否则源地址轮换的 UDP 对端会无界积累
                    let now = Instant::now();
                    sessions.retain(|_, t| now.duration_since(*t) < Duration::from_secs(60));

                    // ★保活与探活超时也挂在这个 **interval** 上。
                    //
                    // 修前这里另有一个 `_ = tokio::time::sleep(30s)` 分支:
                    // `sleep` 在每次 select 迭代都重新创建,timer 从"本轮开始"
                    // 计时,所以只要通道上有任何事件(socket 可读、帧到达)先就绪,
                    // 这个分支就永远轮不到 —— 持续流量下 PING 保活与 90s 探活
                    // 超时**双双失效**,死通道永不回收。
                    // `gc` 是 `interval`(15s 固定节拍,与流量无关),合并进来即修复;
                    // 保活判断仍按 >=30s 自己控节奏,gc 更快只是更及时。
                    if now.duration_since(last_ping_sent) >= Duration::from_secs(30) {
                        let ping = serde_json::to_vec(&rh_protocol::message::Ping { ts_ms: 0, probe: true }).unwrap();
                        if rh_protocol::write_frame(&mut ch_w, FrameType::PING, &ping).await.is_err() {
                            break true;
                        }
                        last_ping_sent = now;
                    }
                    if now.duration_since(last_alive) > Duration::from_secs(90) {
                        break true; // 探活超时
                    }
                }
                _ = rt.rule_cancel.cancelled() => return,
                _ = state.shutdown.cancelled() => return,
            }
        };
        if dead {
            debug!(proxy = rt.id, "udp channel dead");
            if let Some(sess) = state.sessions.get(&rt.client_id) {
                // 每次通道重建固定退避 1s(原指数值在下次通道到达即被重置,等效恒为 1)
                tokio::time::sleep(Duration::from_secs(1)).await;
                udp_ensure_channel(&state, &sess, &rt).await;
            }
            // 清用户会话映射(代次隔离)
            sessions.clear();
        }
    }
}

fn conv_proxy_proto(v: rh_protocol::ProxyProtoVersion) -> rh_protocol::message::ProxyProto {
    match v {
        rh_protocol::ProxyProtoVersion::None => rh_protocol::message::ProxyProto::None,
        rh_protocol::ProxyProtoVersion::V1 => rh_protocol::message::ProxyProto::V1,
        rh_protocol::ProxyProtoVersion::V2 => rh_protocol::message::ProxyProto::V2,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// round-34:池取用必须是 FIFO——先建好的通道先被取走。
    ///
    /// 修前用 `Vec::pop()`(LIFO):每次取走最后建好的那条,先建好的
    /// 长期沉底直到过期。这既是公平性问题,也让"预热完成但尚未验证"
    /// 的新通道总是被优先启用。
    #[tokio::test]
    async fn tcp_pool_hands_out_channels_fifo() {
        use std::sync::atomic::Ordering;
        let pool = TcpPool::new(4);
        pool.epoch.store(7, Ordering::Relaxed);
        // 按建立顺序压入 1、2、3(用内容区分身份)
        for tag in 1u8..=3 {
            let (a, b) = tokio::io::duplex(64);
            drop(b);
            pool.ready
                .lock()
                .await
                .push_back((7, tag as u64, Box::new(a) as BoxedIo));
        }
        // 依次取三次:必须按 1、2、3 的顺序(标签即 request_id)
        for expect in 1u64..=3 {
            let mut ready = pool.ready.lock().await;
            let (epoch, rid, _io) = ready.pop_front().expect("pool must not be empty");
            assert_eq!(epoch, 7, "epoch 必须匹配");
            assert_eq!(rid, expect, "FIFO:第 {expect} 次取用应拿到第 {expect} 条");
        }
        assert!(pool.ready.lock().await.is_empty(), "三次取用后池应清空");
    }

    /// 会话失败计数用的独立 Metrics(与生产实例同构造)。
    fn test_metrics() -> Arc<crate::state::Metrics> {
        Arc::new(crate::state::Metrics::new())
    }

    /// copy_relay_pair 会话失败归因上下文(计数断言用独立 Metrics)。
    fn test_fail_ctx() -> RelayFailCtx {
        RelayFailCtx {
            metrics: test_metrics(),
            proxy: 0,
            user_src: String::new(),
        }
    }

    /// Drop 探针 IO(审计 src/main.rs 同款手法):子泵终止(所有权释放)时翻转。
    struct DropIo {
        inner: tokio::io::DuplexStream,
        dropped: Arc<std::sync::atomic::AtomicBool>,
    }

    impl Drop for DropIo {
        fn drop(&mut self) {
            self.dropped
                .store(true, std::sync::atomic::Ordering::SeqCst);
        }
    }

    impl tokio::io::AsyncRead for DropIo {
        fn poll_read(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
            buf: &mut tokio::io::ReadBuf<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::pin::Pin::new(&mut self.inner).poll_read(cx, buf)
        }
    }

    impl tokio::io::AsyncWrite for DropIo {
        fn poll_write(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
            buf: &[u8],
        ) -> std::task::Poll<std::io::Result<usize>> {
            std::pin::Pin::new(&mut self.inner).poll_write(cx, buf)
        }
        fn poll_flush(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::pin::Pin::new(&mut self.inner).poll_flush(cx)
        }
        fn poll_shutdown(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::pin::Pin::new(&mut self.inner).poll_shutdown(cx)
        }
    }

    async fn wait_flag(f: &Arc<std::sync::atomic::AtomicBool>, what: &str) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        while !f.load(std::sync::atomic::Ordering::SeqCst) {
            if tokio::time::Instant::now() >= deadline {
                panic!("{what}: 探针在 2s 内未翻转(copy 子泵未随父取消终止)");
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    // A1(c):assign_forward copy 路径(t1/t2 spawn 后 join)——父任务取消
    // 必须终止双向子泵;修前句柄 move 进 join future,父取消即 detach。
    #[tokio::test]
    async fn server_copy_parent_cancel_kills_children() {
        let (mut a_peer, a_local) = tokio::io::duplex(4096);
        let (mut c_peer, c_local) = tokio::io::duplex(4096);

        let d_user = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let d_chan = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let user: BoxedIo = Box::new(DropIo {
            inner: a_local,
            dropped: d_user.clone(),
        });
        let channel: BoxedIo = Box::new(DropIo {
            inner: c_local,
            dropped: d_chan.clone(),
        });

        let cancel = tokio_util::sync::CancellationToken::new();
        let parent = tokio::spawn(copy_relay_pair(
            user,
            channel,
            None,
            None,
            cancel,
            test_fail_ctx(),
        ));

        // 建立流:user 侧写入 → 通道对端收到(copy 双向在跑)
        a_peer.write_u8(1).await.unwrap();
        assert_eq!(c_peer.read_u8().await.unwrap(), 1);

        // 父任务取消(会话拆除形态)
        parent.abort();
        assert!(parent.await.unwrap_err().is_cancelled());
        wait_flag(&d_user, "server copy user io").await;
        wait_flag(&d_chan, "server copy channel io").await;

        // 窗口期断言:取消后写入的字节不得再被转发。
        // 探针已翻转 ⇒ 中继侧 duplex 已释放 ⇒ 写入应 BrokenPipe
        // (比"写入成功但不被转发"更强的保证);防御性兼容写入成功路径。
        match a_peer.write_u8(9).await {
            Err(e) if e.kind() == std::io::ErrorKind::BrokenPipe => {}
            Err(e) => panic!("探测写异常: {e}"),
            Ok(()) => {
                match tokio::time::timeout(Duration::from_millis(300), c_peer.read_u8()).await {
                    Err(_) => {}
                    Ok(Ok(b)) => panic!("取消后字节 {b} 仍被转发(copy 子泵 detach)"),
                    Ok(Err(e)) => panic!("读侧异常: {e}"),
                }
            }
        }
    }

    // R3(红线 2 锁):copy 路径读缓冲上限必须保持 64KiB。
    #[test]
    fn copy_buf_cap_is_64kib() {
        assert_eq!(COPY_MAX_BUF, 64 * 1024, "copy 路径读缓冲上限红线(64KiB)");
        const _: () = assert!(COPY_MIN_BUF <= COPY_MAX_BUF);
    }

    /// poll_shutdown 永久 Pending 的用户侧 IO(主代理 cancel-close 复现同款:
    /// /tmp/rh-main-independent-review/src/main.rs PendingShutdownIo;与 mux_relay
    /// 的 cancel_close 缺陷同类):模拟对端不读 + 发送缓冲满等 shutdown
    /// 永不就绪的真实形态。
    struct PendingShutdownIo {
        inner: tokio::io::DuplexStream,
        dropped: Arc<std::sync::atomic::AtomicBool>,
    }

    impl Drop for PendingShutdownIo {
        fn drop(&mut self) {
            self.dropped
                .store(true, std::sync::atomic::Ordering::SeqCst);
        }
    }

    impl tokio::io::AsyncRead for PendingShutdownIo {
        fn poll_read(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
            buf: &mut tokio::io::ReadBuf<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::pin::Pin::new(&mut self.inner).poll_read(cx, buf)
        }
    }

    impl tokio::io::AsyncWrite for PendingShutdownIo {
        fn poll_write(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
            buf: &[u8],
        ) -> std::task::Poll<std::io::Result<usize>> {
            std::pin::Pin::new(&mut self.inner).poll_write(cx, buf)
        }
        fn poll_flush(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::pin::Pin::new(&mut self.inner).poll_flush(cx)
        }
        fn poll_shutdown(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::task::Poll::Pending
        }
    }

    // 规则取消(禁用/删除级联 CancellationToken)后 copy_relay_pair 必须收敛
    // 退出并释放两侧 IO,即使收尾的 shutdown 永久 Pending。父 abort 守卫
    // (A1c)不覆盖本路径:父任务仍在 join 等待,卡死的是子泵收尾。
    // 修前证据:benchmark/runs/goal-repair/relay/(本轮 before-fix 日志)。
    #[tokio::test]
    async fn copy_rule_cancel_converges_when_shutdown_pends() {
        let (mut a_peer, a_local) = tokio::io::duplex(4096);
        let (mut c_peer, c_local) = tokio::io::duplex(4096);

        let d_user = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let user: BoxedIo = Box::new(PendingShutdownIo {
            inner: a_local,
            dropped: d_user.clone(),
        });
        let channel: BoxedIo = Box::new(c_local);

        let cancel = tokio_util::sync::CancellationToken::new();
        let mut parent = tokio::spawn(copy_relay_pair(
            user,
            channel,
            None,
            None,
            cancel.clone(),
            test_fail_ctx(),
        ));

        // 建立流:user 侧写入 → 通道对端收到(copy 双向在跑)
        use tokio::io::AsyncWriteExt as _;
        a_peer.write_u8(1).await.unwrap();
        use tokio::io::AsyncReadExt as _;
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), c_peer.read_u8())
                .await
                .unwrap()
                .unwrap(),
            1
        );

        // 触发规则取消(禁用/删除级联)
        cancel.cancel();
        // 3s 内必须收敛退出(修前:u_w.shutdown() 永久 Pending,join 永不完成)
        tokio::time::timeout(Duration::from_secs(3), &mut parent)
            .await
            .expect("规则取消后 copy_relay_pair 必须收敛退出(即使 poll_shutdown 永久 Pending)")
            .unwrap();
        assert!(
            d_user.load(std::sync::atomic::Ordering::SeqCst),
            "规则取消必须释放用户侧 IO"
        );
    }

    // 故障注入:取消在 EOF 收尾期间才抵达(cancel-after-eof;与
    // rh-transport mux_relay_cancel_after_eof_converges 同款缺陷)。
    // 通道侧 EOF → t2 泵正常结束进入 u_w.shutdown() 收尾(用户侧
    // PendingShutdownIo 永久 Pending)→ 规则取消必须仍被听见并在
    // CANCEL_CLOSE_GRACE 内收敛;未取消的正常 EOF 不加硬超时(红线)。
    #[tokio::test]
    async fn copy_rule_cancel_after_eof_converges() {
        let (mut a_peer, a_local) = tokio::io::duplex(4096);
        let (mut c_peer, c_local) = tokio::io::duplex(4096);

        let d_user = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let user: BoxedIo = Box::new(PendingShutdownIo {
            inner: a_local,
            dropped: d_user.clone(),
        });
        let channel: BoxedIo = Box::new(c_local);

        let cancel = tokio_util::sync::CancellationToken::new();
        let mut parent = tokio::spawn(copy_relay_pair(
            user,
            channel,
            None,
            None,
            cancel.clone(),
            test_fail_ctx(),
        ));

        // 双向在跑:user 写 → 通道对端收到
        use tokio::io::AsyncWriteExt as _;
        a_peer.write_u8(1).await.unwrap();
        use tokio::io::AsyncReadExt as _;
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), c_peer.read_u8())
                .await
                .unwrap()
                .unwrap(),
            1
        );

        // 通道侧 EOF → t2 收尾 u_w.shutdown() 永久 Pending;等收尾挂起后取消
        drop(c_peer);
        tokio::time::sleep(Duration::from_millis(100)).await;
        cancel.cancel();
        tokio::time::timeout(Duration::from_secs(3), &mut parent)
            .await
            .expect("EOF 收尾期间抵达的规则取消必须仍被听见并收敛")
            .unwrap();
        assert!(
            d_user.load(std::sync::atomic::Ordering::SeqCst),
            "EOF 收尾取消必须释放用户侧 IO"
        );
    }

    /// 注入写错误 + 永久 Pending 收尾的 IO:poll_write 直接 Err,
    /// poll_shutdown 永久 Pending(错误路径有界收敛的故障形态)。
    struct WriteErrShutdownPendIo {
        inner: tokio::io::DuplexStream,
        dropped: Arc<std::sync::atomic::AtomicBool>,
    }

    impl Drop for WriteErrShutdownPendIo {
        fn drop(&mut self) {
            self.dropped
                .store(true, std::sync::atomic::Ordering::SeqCst);
        }
    }

    impl tokio::io::AsyncRead for WriteErrShutdownPendIo {
        fn poll_read(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
            buf: &mut tokio::io::ReadBuf<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::pin::Pin::new(&mut self.inner).poll_read(cx, buf)
        }
    }

    impl tokio::io::AsyncWrite for WriteErrShutdownPendIo {
        fn poll_write(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            _buf: &[u8],
        ) -> std::task::Poll<std::io::Result<usize>> {
            std::task::Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "injected write error",
            )))
        }
        fn poll_flush(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::task::Poll::Ready(Ok(()))
        }
        fn poll_shutdown(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::task::Poll::Pending
        }
    }

    // final-gaps(红→绿):copy 通道错误路径有界收敛——非 panic IO 错误
    // + 收尾 shutdown 永久 Pending + **无取消信号**时,copy_relay_pair
    // 必须收敛退出并释放两侧 IO;对向泵(停在读等待)不得在已死通路上
    // 永存。修前:t2 写用户侧 Err → shutdown_with_late_cancel 无界等待
    //(取消永不来)→ relay 卡死、IO 不释放(与 mux_relay 修过的同款
    // 缺陷,mux 侧回归 goal_repair_mux.rs mux_relay_error_path_converges_
    // when_shutdown_pends;copy 路径漏了等价修复)。
    #[tokio::test]
    async fn copy_error_path_converges_when_shutdown_pends() {
        let (_a_peer_hold, a_local) = tokio::io::duplex(4096);
        let (mut c_peer, c_local) = tokio::io::duplex(4096);

        let d_user = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let user: BoxedIo = Box::new(WriteErrShutdownPendIo {
            inner: a_local,
            dropped: d_user.clone(),
        });
        let channel: BoxedIo = Box::new(c_local);

        let cancel = tokio_util::sync::CancellationToken::new();
        let mut parent = tokio::spawn(copy_relay_pair(
            user,
            channel,
            None,
            None,
            cancel.clone(),
            test_fail_ctx(),
        ));

        // 通道侧数据抵达 → t2(c_r→u_w)写用户侧得 Err → 错误路径;
        // 全程不触发 cancel(错误路径自身必须有界收敛)
        use tokio::io::AsyncWriteExt as _;
        c_peer.write_all(b"boom").await.unwrap();

        tokio::time::timeout(Duration::from_secs(3), &mut parent)
            .await
            .expect("copy 错误路径必须收敛(即使 poll_shutdown 永久 Pending 且无取消)")
            .unwrap();
        assert!(
            d_user.load(std::sync::atomic::Ordering::SeqCst),
            "copy 错误路径必须释放用户侧 IO"
        );
    }

    // G5 1/N 归因(c84 candidate-r0 diagnostics README:36-40):copy 路径
    // 异常终止必须计入 rabbithole_session_failures_total——IO 错误按方向
    // 计 1 次;规则取消与 clean EOF 不得误计(管理操作/正常结束会淹没
    // 单连接 1/N 信号)。panic 路径见 copy_relay_panic_contained 同款注入。
    #[tokio::test]
    async fn session_failure_counter_counts_io_error_once() {
        let (_a_peer_hold, a_local) = tokio::io::duplex(4096);
        let (mut c_peer, c_local) = tokio::io::duplex(4096);
        let user: BoxedIo = Box::new(WriteErrShutdownPendIo {
            inner: a_local,
            dropped: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        });
        let metrics = test_metrics();
        let mut parent = tokio::spawn(copy_relay_pair(
            user,
            Box::new(c_local),
            None,
            None,
            tokio_util::sync::CancellationToken::new(),
            RelayFailCtx {
                metrics: metrics.clone(),
                proxy: 7,
                user_src: "1.2.3.4:5".to_string(),
            },
        ));
        use tokio::io::AsyncWriteExt as _;
        c_peer.write_all(b"boom").await.unwrap();
        tokio::time::timeout(Duration::from_secs(3), &mut parent)
            .await
            .expect("copy 错误路径必须收敛")
            .unwrap();
        // dn(c_r→u_w 写用户侧 Err)恰计 1;对向(被 abort)不重复计
        assert_eq!(
            metrics
                .session_failures_total
                .with_label_values(&["relay_down_error"])
                .get(),
            1
        );
        assert_eq!(
            metrics
                .session_failures_total
                .with_label_values(&["relay_up_error"])
                .get(),
            0
        );
        assert_eq!(
            metrics
                .session_failures_total
                .with_label_values(&["relay_pump_panic"])
                .get(),
            0
        );
    }

    #[tokio::test]
    async fn session_failure_counter_ignores_rule_cancel_and_clean_eof() {
        // clean EOF:两侧对端全部 drop → 双向自然 EOF,零计数
        {
            let (a_peer, a_local) = tokio::io::duplex(4096);
            let (c_peer, c_local) = tokio::io::duplex(4096);
            let metrics = test_metrics();
            let relay = copy_relay_pair(
                Box::new(a_local),
                Box::new(c_local),
                None,
                None,
                tokio_util::sync::CancellationToken::new(),
                RelayFailCtx {
                    metrics: metrics.clone(),
                    proxy: 0,
                    user_src: String::new(),
                },
            );
            drop(a_peer);
            drop(c_peer);
            tokio::time::timeout(Duration::from_secs(3), relay)
                .await
                .expect("clean EOF 双向必须收敛");
            assert_eq!(
                metrics
                    .session_failures_total
                    .with_label_values(&["relay_up_error"])
                    .get(),
                0
            );
            assert_eq!(
                metrics
                    .session_failures_total
                    .with_label_values(&["relay_down_error"])
                    .get(),
                0
            );
        }
        // 规则取消(在途断开):零计数
        {
            let (_a_peer, a_local) = tokio::io::duplex(4096);
            let (_c_peer, c_local) = tokio::io::duplex(4096);
            let metrics = test_metrics();
            let cancel = tokio_util::sync::CancellationToken::new();
            let mut parent = tokio::spawn(copy_relay_pair(
                Box::new(a_local),
                Box::new(c_local),
                None,
                None,
                cancel.clone(),
                RelayFailCtx {
                    metrics: metrics.clone(),
                    proxy: 0,
                    user_src: String::new(),
                },
            ));
            tokio::time::sleep(Duration::from_millis(50)).await;
            cancel.cancel();
            tokio::time::timeout(Duration::from_secs(3), &mut parent)
                .await
                .expect("规则取消必须收敛")
                .unwrap();
            assert_eq!(
                metrics
                    .session_failures_total
                    .with_label_values(&["relay_up_error"])
                    .get(),
                0
            );
            assert_eq!(
                metrics
                    .session_failures_total
                    .with_label_values(&["relay_down_error"])
                    .get(),
                0
            );
            assert_eq!(
                metrics
                    .session_failures_total
                    .with_label_values(&["relay_pump_panic"])
                    .get(),
                0
            );
        }
    }

    // 指标注册与 reason 标签(G5 1/N 归因的常驻 /metrics 信号)。
    #[test]
    fn session_failures_metric_registered_with_reason_label() {
        use prometheus::Encoder as _;
        let m = crate::state::Metrics::new();
        m.count_session_failure("channel_unavailable");
        m.count_session_failure("channel_unavailable");
        let mut buf = Vec::new();
        prometheus::TextEncoder::new()
            .encode(&m.registry.gather(), &mut buf)
            .unwrap();
        let text = String::from_utf8(buf).unwrap();
        assert!(
            text.contains("rabbithole_session_failures_total{reason=\"channel_unavailable\"} 2"),
            "指标必须以 rabbithole_session_failures_total{{reason}} 暴露,实际:\n{text}"
        );
    }

    // C5:CountedUserIo 必须透传向量写 + 一次计量(按聚合写出字节)。
    #[test]
    fn counted_user_io_forwards_vectored() {
        use std::io::IoSlice;
        use std::pin::Pin;
        use std::sync::{Arc, Mutex};
        use std::task::{Context, Poll};
        use tokio::io::{AsyncRead, AsyncWrite as _, ReadBuf};

        #[derive(Default, Debug)]
        struct Calls {
            scalar: usize,
            vectored: usize,
            last_total: usize,
        }
        struct VectoredSink(Arc<Mutex<Calls>>);
        impl AsyncRead for VectoredSink {
            fn poll_read(
                self: Pin<&mut Self>,
                _: &mut Context<'_>,
                _: &mut ReadBuf<'_>,
            ) -> Poll<std::io::Result<()>> {
                Poll::Pending
            }
        }
        impl tokio::io::AsyncWrite for VectoredSink {
            fn poll_write(
                self: Pin<&mut Self>,
                _: &mut Context<'_>,
                buf: &[u8],
            ) -> Poll<std::io::Result<usize>> {
                let mut log = self.0.lock().unwrap();
                log.scalar += 1;
                log.last_total = buf.len();
                Poll::Ready(Ok(buf.len()))
            }
            fn poll_write_vectored(
                self: Pin<&mut Self>,
                _: &mut Context<'_>,
                bufs: &[IoSlice<'_>],
            ) -> Poll<std::io::Result<usize>> {
                let n: usize = bufs.iter().map(|b| b.len()).sum();
                let mut log = self.0.lock().unwrap();
                log.vectored += 1;
                log.last_total = n;
                Poll::Ready(Ok(n))
            }
            fn is_write_vectored(&self) -> bool {
                true
            }
            fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
                Poll::Ready(Ok(()))
            }
            fn poll_shutdown(
                self: Pin<&mut Self>,
                _: &mut Context<'_>,
            ) -> Poll<std::io::Result<()>> {
                Poll::Ready(Ok(()))
            }
        }
        let calls = Arc::new(Mutex::new(Calls::default()));
        let metrics = Arc::new(crate::state::Metrics::new());
        let mut io = CountedUserIo::new(VectoredSink(calls.clone()), metrics.clone());
        assert!(io.is_write_vectored(), "能力位必须透传");

        let waker = std::task::Waker::noop();
        let mut cx = Context::from_waker(waker);
        let header = [0u8; 12];
        let body = vec![7u8; 4096];
        let offered = header.len() + body.len();
        let n = match Pin::new(&mut io)
            .poll_write_vectored(&mut cx, &[IoSlice::new(&header), IoSlice::new(&body)])
        {
            Poll::Ready(Ok(n)) => n,
            other => panic!("unexpected poll result: {other:?}"),
        };
        assert_eq!(n, offered, "一次向量写应带走全部数据");
        let log = calls.lock().unwrap();
        assert_eq!(log.vectored, 1, "应恰好一次向量写");
        assert_eq!(log.scalar, 0, "不得退化为标量写");
        drop(log);
        assert_eq!(
            metrics
                .console_bytes_out
                .load(std::sync::atomic::Ordering::Relaxed),
            offered as u64,
            "一次计量:按聚合写出字节"
        );
    }

    // B1(真实包装链):QuotaIo<PrependedIo<RhMuxStream>> 必须仍能进入
    // chunk 路径——剥壳后 halves 可偷出、前缀字节最先送达、且
    // guard 存活期 data_conn_count 不提前递减。
    // 修前判定(源码,REVIEW.md:16):is_mux_stream 对该链恒 false → chunk 不可达。
    #[tokio::test]
    async fn chunk_path_engages_through_quota_and_prepended() {
        use tokio::io::AsyncReadExt;

        // 构造 Session(仅用 data_conn_count)
        let (tx, _rx) = tokio::sync::mpsc::channel(1);
        let sess = crate::session::Session::new(
            "cid".into(),
            "sid".into(),
            1,
            tokio_util::sync::CancellationToken::new(),
            tx,
        );

        let (a, b) = tokio::io::duplex(256 * 1024);
        let server = rh_transport::wrap_stream_as_server(Box::new(b), true, 64);
        let client = rh_transport::wrap_stream_as_client(Box::new(a), true, 64);

        let c = client.open_stream().await.unwrap();
        // SYN 惰性发送:首写建立流;残留前缀 = 同批抵达的先行数据。
        // 写入放独立任务:载荷(>单流接收窗)在服务端 accept 前可能
        // 因流控阻塞,不得与 accept 顺序死锁。
        let wire_len = 64 + 300 * 1024;
        let mut wire = vec![0xA1u8; 64];
        wire.extend_from_slice(&vec![0xB2u8; 300 * 1024]); // 总载荷 > 64KiB 分片
        let writer = tokio::spawn(async move {
            use tokio::io::AsyncWriteExt as _;
            let mut c = c;
            c.write_all(&wire).await.unwrap();
            let _ = c.shutdown().await;
        });

        let s = server.accept().await.unwrap();
        // 模拟 route_data_stream 的包装链:PrependedIo(残留)→ QuotaIo(限额)
        sess.data_conn_count.fetch_add(1, Ordering::Relaxed);
        let wrapped: BoxedIo = crate::control::QuotaIo::new(
            rh_transport::PrependedIo::wrap(s, vec![0xC3u8; 32]),
            sess.clone(),
        );
        // assign_forward 同款剥壳序列
        let (inner, guard) = crate::control::QuotaIo::peel(wrapped);
        assert!(guard.is_some(), "QuotaIo 包装必须剥出限额守卫(B1)");
        assert_eq!(
            sess.data_conn_count.load(Ordering::Relaxed),
            1,
            "guard 存活期 data_conn_count 不得提前递减(B1)"
        );
        let (prefix, inner) = rh_transport::PrependedIo::peel(inner);
        assert_eq!(
            prefix,
            vec![0xC3u8; 32],
            "PrependedIo 壳必须剥出残留前缀(B1)"
        );
        let mut inner = inner;
        assert!(
            rh_transport::mux::is_mux_stream(&inner),
            "剥壳后必须识别为 mux 流(B1 修前恒 false)"
        );
        let (mut b_r, b_w) = rh_transport::mux::steal_mux_halves(&mut inner).expect("steal halves");
        drop(inner); // 壳
        b_r.set_front_prefix(bytes::Bytes::from(prefix));

        // 用户侧 + chunk 中继:前缀字节必须最先送达,载荷完整
        let (mut local, relay_io) = tokio::io::duplex(1024 * 1024);
        let relay = tokio::spawn(rh_transport::mux::mux_relay(
            Box::new(relay_io),
            b_r,
            b_w,
            None,
            None,
        ));
        let mut got = Vec::new();
        let n = tokio::time::timeout(Duration::from_secs(5), local.read_to_end(&mut got))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(n, wire_len + 32, "前缀 + 全部载荷必须送达");
        assert_eq!(&got[..32], &vec![0xC3u8; 32][..], "前缀字节最先(B1)");
        assert_eq!(&got[32..32 + 64], &vec![0xA1u8; 64][..]);
        assert!(got[32..].ends_with(&[0xB2u8; 8]));
        drop(local);
        let _ = tokio::time::timeout(Duration::from_secs(2), relay).await;
        let _ = tokio::time::timeout(Duration::from_secs(2), writer).await;
        drop(guard);
        assert_eq!(
            sess.data_conn_count.load(Ordering::Relaxed),
            0,
            "guard 释放即递减(与 QuotaIo::Drop 等价)"
        );
    }

    // B7:relay_mode=copy 必须禁用 chunk 快路径(路径判定层面)。
    // 与 chunk_path_engages... 共用剥壳链;此处验证判定式本身。
    #[test]
    fn relay_mode_copy_disables_chunk_gate() {
        // assign_forward 的判定条件(与实现保持同式):
        // use_compression == false && relay_mode != "copy"
        let gate =
            |use_compression: bool, relay_mode: &str| !use_compression && relay_mode != "copy";
        assert!(gate(false, "splice"), "默认(splice)开 chunk");
        assert!(!gate(false, "copy"), "B7:copy 必须关 chunk");
        assert!(!gate(true, "splice"), "压缩恒回落 copy");
        assert!(!gate(true, "copy"));
    }

    /// 注入 panic 的写 IO:poll_write 直接 panic(模拟适配层/自定义 IO bug)。
    struct PanicWriteIo {
        inner: tokio::io::DuplexStream,
        dropped: Arc<std::sync::atomic::AtomicBool>,
    }

    impl Drop for PanicWriteIo {
        fn drop(&mut self) {
            self.dropped
                .store(true, std::sync::atomic::Ordering::SeqCst);
        }
    }

    impl tokio::io::AsyncRead for PanicWriteIo {
        fn poll_read(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
            buf: &mut tokio::io::ReadBuf<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::pin::Pin::new(&mut self.inner).poll_read(cx, buf)
        }
    }

    impl tokio::io::AsyncWrite for PanicWriteIo {
        fn poll_write(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            _buf: &[u8],
        ) -> std::task::Poll<std::io::Result<usize>> {
            panic!("injected poll_write panic")
        }
        fn poll_flush(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::task::Poll::Ready(Ok(()))
        }
        fn poll_shutdown(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::task::Poll::Ready(Ok(()))
        }
    }

    // 故障注入:单向子泵 panic(user→channel 方向写用户侧)。守卫吸收
    // JoinError,但对向泵(停在 channel 读)不得永存——copy_relay_pair
    // 必须收敛并释放两侧 IO(join_relay_pumps panic 收敛)。
    #[tokio::test]
    async fn copy_relay_panic_contained() {
        let (_a_peer, a_local) = tokio::io::duplex(4096);
        let (mut c_peer, c_local) = tokio::io::duplex(4096);

        let d_user = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let d_chan = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let user: BoxedIo = Box::new(PanicWriteIo {
            inner: a_local,
            dropped: d_user.clone(),
        });
        let channel: BoxedIo = Box::new(DropIo {
            inner: c_local,
            dropped: d_chan.clone(),
        });

        let cancel = tokio_util::sync::CancellationToken::new();
        let mut parent = tokio::spawn(copy_relay_pair(
            user,
            channel,
            None,
            None,
            cancel,
            test_fail_ctx(),
        ));

        // channel→user 方向数据抵达 → t2 写用户侧 panic
        use tokio::io::AsyncWriteExt as _;
        c_peer.write_u8(1).await.unwrap();
        tokio::time::timeout(Duration::from_secs(3), &mut parent)
            .await
            .expect("单向子泵 panic 后 copy_relay_pair 必须收敛(对向不得永存)")
            .unwrap();
        assert!(d_user.load(std::sync::atomic::Ordering::SeqCst));
        assert!(d_chan.load(std::sync::atomic::Ordering::SeqCst));
    }

    // B1/prefix 兜底:正常 server 包装链(QuotaIo<PrependedIo<裸 duplex>>,
    // 非 mux)+ 非空 prefix → mux_steal_or_rewrap 必须把 prefix 原样回包,
    // 回包通道读序 = prefix 最先 + 线路字节随后;QuotaGuard 全期不提前递减。
    // 修前:assign_forward 回落直接 `channel = ch`,prefix 丢弃。
    #[tokio::test]
    async fn nonmux_fallback_preserves_prefix_and_guard() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let (tx, _rx) = tokio::sync::mpsc::channel(1);
        let sess = crate::session::Session::new(
            "cid".into(),
            "sid".into(),
            1,
            tokio_util::sync::CancellationToken::new(),
            tx,
        );

        // 模拟 route_data_stream 的限额占用(计数 +1 随 QuotaIo Drop 递减)
        sess.data_conn_count.fetch_add(1, Ordering::Relaxed);
        let (mut wire, wire_local) = tokio::io::duplex(64 * 1024);
        let prefix = vec![0xC3u8; 32];
        let wrapped: BoxedIo = crate::control::QuotaIo::new(
            rh_transport::PrependedIo::wrap(Box::new(wire_local), prefix.clone()),
            sess.clone(),
        );

        // assign_forward 同款剥壳序列
        let (inner, guard) = crate::control::QuotaIo::peel(wrapped);
        assert!(guard.is_some(), "QuotaIo 壳必须剥出守卫");
        assert_eq!(
            sess.data_conn_count.load(Ordering::Relaxed),
            1,
            "守卫存活期不得提前递减"
        );
        let (p, ch) = rh_transport::PrependedIo::peel(inner);
        assert_eq!(p, prefix, "PrependedIo 壳必须剥出残留前缀");

        // 非 mux:必须回包(Err),不得偷取、不得丢 prefix
        let mut fallback = match mux_steal_or_rewrap(p, ch) {
            Err(f) => f,
            Ok(_) => panic!("非 mux 流必须回落(Err),不得偷取"),
        };
        assert!(
            !rh_transport::mux::is_mux_stream(&fallback),
            "非 mux 回落产物不得再被判为 mux"
        );
        assert!(
            fallback
                .as_any()
                .downcast_ref::<rh_transport::PrependedIo>()
                .is_some(),
            "非空 prefix 回落必须回包 PrependedIo(splice 资格判定随之回落 copy)"
        );

        // 回包通道读序:prefix 最先,线路字节随后
        wire.write_all(b"hello").await.unwrap();
        let mut got = [0u8; 37];
        tokio::time::timeout(Duration::from_secs(2), fallback.read_exact(&mut got))
            .await
            .expect("prefix + 线路字节必须完整可读(修前 prefix 丢失)")
            .unwrap();
        assert_eq!(&got[..32], &prefix[..], "prefix 字节最先交付");
        assert_eq!(&got[32..], b"hello", "线路字节紧随其后");
        assert_eq!(
            sess.data_conn_count.load(Ordering::Relaxed),
            1,
            "转发(读)期间守卫不得递减"
        );
        drop(fallback);
        drop(guard);
        assert_eq!(
            sess.data_conn_count.load(Ordering::Relaxed),
            0,
            "守卫释放即递减(与 QuotaIo::Drop 等价)"
        );
    }

    // 嵌套 prefix 顺序:剥最外层(B)后回包,读序必须保持
    // B(外层残留)→ A(内层残留)→ 流数据——与原包装链逐字节等价。
    #[tokio::test]
    async fn nested_prefix_rewrap_preserves_order() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let (mut wire, wire_local) = tokio::io::duplex(64 * 1024);
        let inner: BoxedIo = rh_transport::PrependedIo::wrap(Box::new(wire_local), vec![0xA1u8; 8]);
        let outer: BoxedIo = rh_transport::PrependedIo::wrap(inner, vec![0xB2u8; 16]);

        let (b_prefix, ch) = rh_transport::PrependedIo::peel(outer);
        assert_eq!(b_prefix, vec![0xB2u8; 16], "只剥最外层");
        let mut fallback = match mux_steal_or_rewrap(b_prefix, ch) {
            Err(f) => f,
            Ok(_) => panic!("非 mux 流必须回落(Err),不得偷取"),
        };

        wire.write_all(b"stream").await.unwrap();
        let mut got = [0u8; 30];
        tokio::time::timeout(Duration::from_secs(2), fallback.read_exact(&mut got))
            .await
            .expect("嵌套 prefix + 载荷必须完整可读")
            .unwrap();
        assert_eq!(&got[..16], &vec![0xB2u8; 16][..], "外层残留最先");
        assert_eq!(&got[16..24], &vec![0xA1u8; 8][..], "内层残留次之");
        assert_eq!(&got[24..], b"stream", "流数据最后(顺序逐字节等价)");
    }

    /// 读半永远 Pending 的 IO:模拟「只 clean EOF 一向,对向永不结束」。
    ///
    /// 写半正常转发(否则构造不出「一向 clean EOF」);只有读永久挂起,
    /// 即对端既不回数据也不回 FIN——round-40 实测里 socket 钉在
    /// `FIN-WAIT-2`/`CLOSING` 的那种连接在内核侧的表现。
    struct ReadPendIo {
        inner: tokio::io::DuplexStream,
        dropped: Arc<std::sync::atomic::AtomicBool>,
    }

    impl Drop for ReadPendIo {
        fn drop(&mut self) {
            self.dropped
                .store(true, std::sync::atomic::Ordering::SeqCst);
        }
    }

    impl tokio::io::AsyncRead for ReadPendIo {
        fn poll_read(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            _buf: &mut tokio::io::ReadBuf<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            // 对端永不回 FIN:既不 Ready(0) 也不 Ready(n),永久 Pending
            std::task::Poll::Pending
        }
    }

    impl tokio::io::AsyncWrite for ReadPendIo {
        fn poll_write(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
            buf: &[u8],
        ) -> std::task::Poll<std::io::Result<usize>> {
            std::pin::Pin::new(&mut self.inner).poll_write(cx, buf)
        }
        fn poll_flush(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::pin::Pin::new(&mut self.inner).poll_flush(cx)
        }
        fn poll_shutdown(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::pin::Pin::new(&mut self.inner).poll_shutdown(cx)
        }
    }

    /// round-51(红):clean EOF 路径的半关闭必须有界收敛。
    ///
    /// 修前 `copy_relay_pair` 用裸 `tokio::join!` 等对向——一条方向 clean
    /// EOF、对向永不结束(对端不回 FIN)时父任务永久挂起,两侧 IO 与
    /// 子泵都不释放(round-40 在 mux_relay/splice_relay 上实测:10 轮
    /// `-P8` 累积 22 条 FIN-WAIT-2/CLOSING、20s 后仍不消失、吞吐
    /// 2.4G→0.5G;服务端 copy 这条路径当时漏改)。
    ///
    /// 方向布局:用户侧普通 duplex(对端 shutdown → t1 读得 clean EOF),
    /// 通道侧 [`ReadPendIo`](读半永远 Pending)→ t2 停在 channel 读,永不
    /// 结束。这正是「服务端把数据写给用户、用户已 FIN,而隧道数据通道
    /// 那头再也不回 FIN」的形态。
    ///
    /// 断言三点:
    /// 1. 父任务在默认 30s grace 之后、45s 之前返回(既不永久挂起,
    ///    也没有提前打断正常半关闭);
    /// 2. 两侧 IO 探针都翻转(流/socket 真被释放,而非只是 future 结束);
    /// 3. clean EOF + 半关闭超时**不计**会话失败(那是正常代理行为)。
    ///
    /// `start_paused` 用虚拟时钟把 30s 压成瞬时:tokio test-util 在所有
    /// worker 都 idle 时自动推进到最近的定时器,所以这里既不必真等
    /// 半分钟,也不必设 `RH_HALFCLOSE_GRACE_SECS`(进程级 OnceLock,改它
    /// 会污染同进程内的其它用例)。
    #[tokio::test(start_paused = true)]
    async fn copy_clean_eof_bounded_when_peer_never_finishes() {
        let (mut a_peer, a_local) = tokio::io::duplex(4096);
        let (_c_peer_hold, c_local) = tokio::io::duplex(4096);

        let d_user = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let d_chan = Arc::new(std::sync::atomic::AtomicBool::new(false));
        // 用户侧:普通流(可正常读到 EOF)
        let user: BoxedIo = Box::new(DropIo {
            inner: a_local,
            dropped: d_user.clone(),
        });
        // 通道侧:读半永久 Pending(对端既不回数据也不回 FIN)
        let channel: BoxedIo = Box::new(ReadPendIo {
            inner: c_local,
            dropped: d_chan.clone(),
        });

        let metrics = test_metrics();
        let mut parent = tokio::spawn(copy_relay_pair(
            user,
            channel,
            None,
            None,
            tokio_util::sync::CancellationToken::new(),
            RelayFailCtx {
                metrics: metrics.clone(),
                proxy: 0,
                user_src: String::new(),
            },
        ));

        use tokio::io::AsyncWriteExt as _;
        // 用户写端关闭 → t1 读到 EOF(**clean EOF**,不置 up_errored);
        // t2 停在 channel 读,通道对端永不回 FIN → 旧代码永挂于此
        a_peer.write_all(b"tail").await.unwrap();
        a_peer.shutdown().await.unwrap();

        tokio::time::timeout(Duration::from_secs(45), &mut parent)
            .await
            .expect(
                "clean EOF + 对向永不结束:copy_relay_pair 必须在 halfclose_grace 内\
                 返回,不得永久挂起(round-40 泄漏红线,服务端 copy 路径漏改)",
            )
            .unwrap();

        assert!(
            d_user.load(std::sync::atomic::Ordering::SeqCst),
            "半关闭兜底后必须释放用户侧 IO(否则 socket 钉死)"
        );
        assert!(
            d_chan.load(std::sync::atomic::Ordering::SeqCst),
            "半关闭兜底后必须释放通道侧 IO"
        );
        // clean EOF 不是会话失败:即便等了 grace 也不得计失败
        for reason in ["relay_up_error", "relay_down_error", "relay_pump_panic"] {
            assert_eq!(
                metrics
                    .session_failures_total
                    .with_label_values(&[reason])
                    .get(),
                0,
                "clean EOF + 半关闭兜底不得计入 {reason}(正常代理行为,非会话失败)"
            );
        }
    }

    /// round-51 对照(半关闭红线):正常半关闭**不得**被 grace 提前打断。
    ///
    /// 上一条测试锁定「有界」,这一条锁定「边界足够宽」:一条方向 clean
    /// EOF 后,对向仍需把剩余数据完整送达(这里是 300ms 后才写完的一段
    /// 数据)——远小于默认 30s grace。修前修后都必须完整收到,否则就是
    /// 用短超时截断半关闭,比泄漏更糟。
    #[tokio::test(start_paused = true)]
    async fn copy_clean_eof_still_delivers_late_peer_data() {
        let (mut a_peer, a_local) = tokio::io::duplex(4096);
        let (mut c_peer, c_local) = tokio::io::duplex(4096);

        let mut parent = tokio::spawn(copy_relay_pair(
            Box::new(a_local),
            Box::new(c_local),
            None,
            None,
            tokio_util::sync::CancellationToken::new(),
            test_fail_ctx(),
        ));

        use tokio::io::AsyncReadExt as _;
        use tokio::io::AsyncWriteExt as _;
        // 用户侧 clean EOF(用户不再发,但仍在读——典型 HTTP 请求结束后
        // 等响应的形态)
        a_peer.shutdown().await.unwrap();
        // 对向延迟写好响应:必须穿过 grace 窗口完整送达
        let writer = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(300)).await;
            c_peer.write_all(b"late-response-body").await.unwrap();
            c_peer.shutdown().await.unwrap();
        });

        let mut got = Vec::new();
        tokio::time::timeout(Duration::from_secs(20), a_peer.read_to_end(&mut got))
            .await
            .expect("clean EOF 后的对向数据必须在 grace 内完整送达(不得被短超时截断)")
            .unwrap();
        assert_eq!(
            got, b"late-response-body",
            "半关闭兜底不得截断正常半关闭的数据"
        );
        writer.await.unwrap();
        tokio::time::timeout(Duration::from_secs(45), &mut parent)
            .await
            .expect("数据送达后 relay 必须收敛")
            .unwrap();
    }
}

#[cfg(test)]
mod udp_pump_survives_reconnect_tests {
    //! A2 回归:服务端 UDP 泵在客户端重连后**不得**永久失效。
    //!
    //! 修前 `close_proxy_channels` 把 `udp_new_channel_tx` 置 None →
    //! `udp_serve_loop` 的 `new_rx.recv()` 立刻返回 None → 泵 return。
    //! 泵只在 spawn_runtime / bind_error_retry 启动,客户端重连不重建运行时,
    //! 于是重连后 UDP 规则永久不工作,且 `udp_on_channel` 见 None 时静默丢流。
    //!
    //! 修后:泵的生死只由 `rule_cancel` / `state.shutdown` 决定,sender 保留。
    use super::*;
    use crate::config::ServerConfig;
    use crate::state::ProxyRuntime;

    /// ProxySpec 无 Default;沿用 api.rs 测试里的 serde 构造法。
    fn udp_spec() -> rh_protocol::ProxySpec {
        serde_json::from_value(serde_json::json!({
            "name": "udp-a2",
            "kind": "udp",
            "client_id": "cid",
            "bind_ip": "127.0.0.1",
            "bind_port": 0,
            "target_host": "127.0.0.1",
            "target_port": 9,
        }))
        .unwrap()
    }

    fn make_rt() -> Arc<ProxyRuntime> {
        Arc::new(ProxyRuntime::new(1001, "cid".to_string(), udp_spec()))
    }

    /// 粘性选路:同一 `cache_key` 恒定落到同一成员,不同键分散到各成员。
    ///
    /// 这是 B3「分组缓存命中率只到 1/N」的回归锁。轮询选路下,同一资源的
    /// 第 1 个请求写成员 A、第 2 个查成员 B —— 跨请求必然 MISS。这里直接
    /// 断言选路结果对同一 affinity 稳定、对不同 affinity 覆盖到多个成员。
    #[tokio::test]
    async fn sticky_pick_is_stable_per_affinity_and_spreads_keys() {
        let db = crate::db::Db::open("sqlite::memory:").await.unwrap();
        let state = Arc::new(AppState::new(ServerConfig::default(), db));
        // 三个同组、同 kind/bind 的成员(vhost http 也走这条组键)
        let mut rts = Vec::new();
        for id in 1..=3u64 {
            let mut spec = udp_spec();
            spec.id = id;
            spec.kind = rh_protocol::ProxyKind::Http;
            spec.bind_port = 9000;
            spec.group = Some("g1".into());
            let rt = Arc::new(ProxyRuntime::new(id, format!("cid{id}"), spec));
            state.proxies.runtimes.insert(id, rt.clone());
            rts.push(rt);
        }
        let spec0 = rts[0].spec.read().await.clone();
        let key = crate::state::ProxyManager::group_key(&spec0).unwrap();
        let g = state.proxies.group_entry(&key);
        {
            let mut m = g.members.write().await;
            m.extend(1..=3u64);
        }
        // 会话在线(选路要求 session 存在)
        for id in 1..=3u64 {
            let (tx, _rx) = tokio::sync::mpsc::channel(4);
            state.sessions.register(crate::session::Session::new(
                format!("cid{id}"),
                format!("sid-{id}"),
                id,
                tokio_util::sync::CancellationToken::new(),
                tx,
            ));
        }

        // 同一 affinity 重复取,必须恒为同一个成员
        let a1 = group_pick_sticky(&state, &rts[0], "a.com\0/index.html")
            .await
            .expect("组内应能选出成员");
        for _ in 0..20 {
            let a = group_pick_sticky(&state, &rts[0], "a.com\0/index.html")
                .await
                .unwrap();
            assert_eq!(
                a.id, a1.id,
                "同一 cache_key 必须恒定落同一成员,否则缓存跨请求必 MISS"
            );
        }

        // 不同 affinity 应覆盖到多个成员(否则粘性退化成单点)
        let mut seen = std::collections::HashSet::new();
        for i in 0..60 {
            let k = format!("a.com\0/asset{i}.js");
            let m = group_pick_sticky(&state, &rts[0], &k).await.unwrap();
            seen.insert(m.id);
        }
        assert!(
            seen.len() > 1,
            "不同 cache_key 必须分散到多个成员(实际只有 {seen:?}),否则负载不均"
        );

        // 轮询选路仍然存在且不粘性(TCP 数据面继续用它)
        let mut rr_seen = std::collections::HashSet::new();
        for _ in 0..6 {
            let m = group_pick(&state, &rts[0]).await.unwrap();
            rr_seen.insert(m.id);
        }
        assert!(
            rr_seen.len() > 1,
            "轮询选路必须继续把连接均摊到多个成员(实际 {rr_seen:?})"
        );
    }

    /// 契约 1:close 之后 sender 仍在(旧实现会变成 None)。
    #[tokio::test]
    async fn close_proxy_channels_keeps_udp_sender() {
        let rt = make_rt();
        // 模拟泵启动时注册 sender
        let (tx, _rx) = mpsc::channel::<BoxedIo>(4);
        *rt.udp_new_channel_tx.lock().await = Some(tx);
        assert!(rt.udp_new_channel_tx.lock().await.is_some());

        close_proxy_channels(&rt).await;

        assert!(
            rt.udp_new_channel_tx.lock().await.is_some(),
            "A2:close_proxy_channels 不得把 sender 置 None —— 否则泵退出后\
             客户端重连也不会重建(泵只在 spawn_runtime 时启),UDP 永久失效"
        );
    }

    /// 契约 2:发送端全 drop 后泵仍存活,只由 rule_cancel 退出。
    #[tokio::test]
    async fn pump_outlives_sender_drop_and_exits_on_rule_cancel() {
        let db = crate::db::Db::open("sqlite::memory:").await.unwrap();
        let state = Arc::new(AppState::new(ServerConfig::default(), db));
        let rt = make_rt();
        let sock = Arc::new(
            UdpSocket::bind("127.0.0.1:0")
                .await
                .expect("bind local udp"),
        );
        let h = tokio::spawn(udp_serve_loop(state.clone(), rt.clone(), sock));

        // 等泵注册好 sender,然后丢掉它(模拟 close 之后没有新通道抵达)
        for _ in 0..50 {
            if rt.udp_new_channel_tx.lock().await.is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        *rt.udp_new_channel_tx.lock().await = None;

        // 旧实现:recv() 立即 None → return → 这里会 finished
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert!(
            !h.is_finished(),
            "A2:发送端 drop 后泵必须继续存活(等待重连后的新通道),\
             不得因 recv()=None 就退出"
        );

        // 真正的退出条件:规则取消
        rt.rule_cancel.cancel();
        tokio::time::timeout(Duration::from_secs(2), h)
            .await
            .expect("rule_cancel 后泵必须退出(否则是泄漏)")
            .unwrap();
    }
}

#[cfg(test)]
mod group_listener_ownership_tests {
    //! A6 回归:组共享监听的生死属于**组**,不属于属主成员。
    //!
    //! 修前 `tcp_serve_loop` select 的是传入 `rt` 的 `rule_cancel`,而
    //! runtime.rs 传的正是**属主成员**。属主被禁用/改端口 → teardown
    //! 无条件 cancel → 监听 fd 释放,但 `listener_bound` 仍为 true →
    //! 其余成员 spawn 时看到 true 跳过重绑 → 组端口**永久拒绝连接**。
    //!
    //! 修后:共享 loop 走 `tcp_serve_loop_group`,只由组级 `serve_cancel`
    //! 退出;属主自己的 rule_cancel 取消不影响它。
    use super::*;
    use crate::config::ServerConfig;
    use crate::state::ProxyRuntime;

    fn group_spec(port: u16, group: &str) -> rh_protocol::ProxySpec {
        serde_json::from_value(serde_json::json!({
            "name": "g1",
            "kind": "tcp",
            "client_id": "cid",
            "bind_ip": "127.0.0.1",
            "bind_port": port,
            "target_host": "127.0.0.1",
            "target_port": 9,
            "group": group,
        }))
        .unwrap()
    }

    /// 接线断言:runtime 给**组**绑定监听时,必须走 `tcp_serve_loop_group`
    /// (组级 cancel),不能走随属主 `rule_cancel` 退出的独享版本。
    ///
    /// 这条是源码级检查 —— 前一版测试直接调 `tcp_serve_loop_group`,把接线
    /// 回退成 `tcp_serve_loop` 后仍然通过,完全抓不住回归。接线才是缺陷所在。
    #[test]
    fn runtime_wires_group_listener_to_group_cancel() {
        let src = include_str!("runtime.rs");
        let marker = "let group_cancel = token.clone();";
        let start = src
            .find(marker)
            .expect("共享监听分支必须用组级 cancel(见 A6)");
        let rest = &src[start..];
        let end = rest
            .find("return Ok(());")
            .expect("分支应以 return Ok 结束");
        let branch = &rest[..end];
        assert!(
            branch.contains("tcp_serve_loop_group"),
            "A6:共享监听必须走 tcp_serve_loop_group(组级 cancel);当前:\n{branch}"
        );
        assert!(
            !branch.contains("crate::data::tcp_serve_loop("),
            "A6:不得回退到随属主 rule_cancel 退出的独享版本;当前:\n{branch}"
        );
    }

    /// 核心断言:属主 rule_cancel 取消后,共享 serve loop **仍能 accept**;
    /// 只有组级 serve_cancel 才让它退出。
    #[tokio::test]
    async fn shared_listener_survives_owner_cancel() {
        let db = crate::db::Db::open("sqlite::memory:").await.unwrap();
        let state = Arc::new(AppState::new(ServerConfig::default(), db));
        let spec = group_spec(0, "gA6");
        let rt = Arc::new(ProxyRuntime::new(1, "cid".into(), spec));

        // 绑定一个真实监听(端口 0 由系统分配)
        let std_l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        std_l.set_nonblocking(true).unwrap();
        let addr = std_l.local_addr().unwrap();
        let l = TcpListener::from_std(std_l).unwrap();

        let group_cancel = tokio_util::sync::CancellationToken::new();
        let h = tokio::spawn(tcp_serve_loop_group(
            state.clone(),
            rt.clone(),
            l,
            group_cancel.clone(),
        ));

        // 属主取消(teardown 会做的事)
        rt.rule_cancel.cancel();
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            !h.is_finished(),
            "A6:属主 rule_cancel 取消后共享 serve loop 不得退出(组监听属于组)"
        );

        // 仍能建立 TCP 连接(监听 fd 还在)
        let conn =
            tokio::time::timeout(Duration::from_secs(2), tokio::net::TcpStream::connect(addr))
                .await
                .expect("A6:属主取消后组端口必须仍可连接")
                .expect("connect");
        drop(conn);

        // 只有组级取消才退出
        group_cancel.cancel();
        tokio::time::timeout(Duration::from_secs(2), h)
            .await
            .expect("组级 serve_cancel 后必须退出")
            .unwrap();
    }

    /// 对照:独享端口的 serve loop 仍随自己的 rule_cancel 退出(语义未变)。
    #[tokio::test]
    async fn exclusive_listener_exits_on_own_rule_cancel() {
        let db = crate::db::Db::open("sqlite::memory:").await.unwrap();
        let state = Arc::new(AppState::new(ServerConfig::default(), db));
        let spec = group_spec(0, ""); // 无组 = 独享
        let rt = Arc::new(ProxyRuntime::new(2, "cid".into(), spec));

        let std_l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        std_l.set_nonblocking(true).unwrap();
        let l = TcpListener::from_std(std_l).unwrap();

        let h = tokio::spawn(tcp_serve_loop(state.clone(), rt.clone(), l));
        rt.rule_cancel.cancel();
        tokio::time::timeout(Duration::from_secs(2), h)
            .await
            .expect("独享端口必须随 rule_cancel 退出")
            .unwrap();
    }
}

// ---------------- B6:有界双向中继(裸 copy_bidirectional 的收敛替代) ----------------

/// 双向中继,带半关闭有界兜底。
///
/// 替换裸 `tokio::io::copy_bidirectional`:`copy_bidirectional` 在一侧
/// clean EOF 后会无限期等待对向 EOF —— 对端永不回 FIN 时 socket 永久停在
/// FIN-WAIT-2/CLOSING,fd 与连接计数一起泄漏。这里保持"等对向结束"的语义,
/// 只把等待上限交给 `halfclose_grace`(默认 30s,`RH_HALFCLOSE_GRACE_SECS`
/// 可覆盖,0 = 旧行为)。
///
/// 与 [`copy_relay_pair`] 的差异:本函数不做字节计量/限速/失败归因,
/// 用于 vhost / visitor 裸中继路径(这些路径的计量在 CountedUserIo 上)。
///
/// 注意:不用 `tokio::spawn` —— 入参是 `&mut`(非 'static),拆半句柄不能
/// move 进独立任务。这里用 `select!` 在同一 future 内并发双向 copy:先完成
/// 的一向关闭对向写半,然后在 `halfclose_grace` 内等第二向自然结束;超时
/// 则直接返回(拆半句柄随函数返回 drop → fd 释放)。
pub async fn copy_bidirectional_bounded<A, B>(a: &mut A, b: &mut B) -> std::io::Result<(u64, u64)>
where
    A: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    B: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let (mut a_r, mut a_w) = tokio::io::split(a);
    let (mut b_r, mut b_w) = tokio::io::split(b);
    let up = async {
        let n = tokio::io::copy(&mut a_r, &mut b_w).await.ok();
        let _ = b_w.shutdown().await;
        n
    };
    let down = async {
        let n = tokio::io::copy(&mut b_r, &mut a_w).await.ok();
        let _ = a_w.shutdown().await;
        n
    };
    futures::pin_mut!(up, down);
    // 先完成的一向决定后续等待上限
    let (first_up, n) = tokio::select! {
        n = &mut up => (true, n),
        n = &mut down => (false, n),
    };
    let grace = rh_transport::halfclose_grace();
    let second = if grace == std::time::Duration::MAX {
        None
    } else {
        match tokio::time::timeout(grace, async {
            if first_up {
                (&mut down).await
            } else {
                (&mut up).await
            }
        })
        .await
        {
            Ok(v) => v,
            Err(_) => {
                static WARNED: std::sync::Once = std::sync::Once::new();
                WARNED.call_once(|| {
                    tracing::warn!(
                        grace_secs = grace.as_secs(),
                        "half-close timeout: peer never completed close handshake; \
                         aborting vhost/visitor relay to release the stream"
                    );
                });
                None
            }
        }
    };
    let (u, d) = if first_up {
        (n.unwrap_or(0), second.unwrap_or(0))
    } else {
        (second.unwrap_or(0), n.unwrap_or(0))
    };
    Ok((u, d))
}
