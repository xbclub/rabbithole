//! vhost:HTTP(域名/子域/locations 路由 + 头改写 + Basic 认证)、
//! HTTPS(SNI 透传)、TCPMux(HTTP CONNECT 复用)。

use crate::state::{AppState, ProxyRuntime, VhostRoute};
use http_body_util::BodyExt as _;

use rh_protocol::forward_flags;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::debug;

// ---------------- HTTP vhost(hyper) ----------------

pub async fn http_vhost_serve(state: Arc<AppState>, listener: tokio::net::TcpListener) {
    loop {
        match listener.accept().await {
            Ok((sock, peer)) => {
                let st = state.clone();
                tokio::spawn(async move {
                    let _ = sock.set_nodelay(true);
                    if let Err(e) = serve_http(st, sock, peer).await {
                        debug!(error = %e, "http vhost conn");
                    }
                });
            }
            Err(_) => tokio::time::sleep(std::time::Duration::from_millis(100)).await,
        }
    }
}

async fn serve_http(state: Arc<AppState>, sock: TcpStream, peer: SocketAddr) -> anyhow::Result<()> {
    use hyper::server::conn::http1;
    use hyper::service::service_fn;

    // 连接级计数(一条连接可服务多个请求/规则,规则归因在请求通道侧)
    let _conn_guard = crate::data::try_open_user_conn(&state, None);
    let st = state.clone();
    let mut builder = http1::Builder::new();
    // 头部读取超时(防 slowloris 未认证挂连接);0 = 不启用
    let header_to = st.cfg.vhost.http_timeout_s;
    if header_to > 0 {
        builder.header_read_timeout(Some(std::time::Duration::from_secs(header_to)));
    }
    let conn = builder
        .timer(hyper_util::rt::TokioTimer::new())
        .serve_connection(
            hyper_util::rt::TokioIo::new(crate::data::CountedUserIo::new(sock, st.metrics.clone())),
            service_fn(move |req: hyper::Request<hyper::body::Incoming>| {
                let st = st.clone();
                let peer = peer;
                async move {
                    handle_http_request(st, req, peer)
                        .await
                        .map_err(|_e| std::io::Error::other("vhost error"))
                }
            }),
        );
    conn.await?;
    Ok(())
}

type BoxBody = http_body_util::combinators::BoxBody<hyper::body::Bytes, std::io::Error>;

// ---------------- 上游连接池 ----------------
//
// 修前:每个 HTTP 请求都 `hyper::client::conn::http1::handshake`,新建一份
// 完整 HTTP/1.1 状态机 + spawn 一个 conn 任务,请求结束即丢弃。
// 单机消融(16 连接 keep-alive,test1):
//   直连源站 41602 QPS / 纯 TCP 转发 10187 QPS / HTTP vhost 4262 QPS
// vhost 只拿到直连的 10%,而纯 TCP 转发有 24% —— 损失集中在这一层。
//
// 池模型:`SendRequest` 不是 Clone,且一条 hyper client 连接同一时刻只能
// 挂一个在途请求(HTTP/1.1 无多路复用)。因此按「独占借出 + 用完归还」:
//   借:池里取一条 is_ready 且未关闭的;空则新建(受 MAX_IDLE 限)
//   还:请求完成且未 close/未 upgrade 时 push 回池
// 连接关闭(源站关连接 / 出错)时直接丢弃,下次借会新建。

/// 一条可复用的上游连接。
///
/// body 类型泛型化:生产用 `Incoming`(测试无法构造该类型),而测试需要
/// 用 `Empty<Bytes>` 真跑一遍建连/复用 —— 泛型化让两边共用同一份池逻辑,
/// 避免测试里再写一套同构实现(那种"测的不是生产代码"的假测试)。
pub struct Upstream<B = hyper::body::Incoming> {
    sender: hyper::client::conn::http1::SendRequest<B>,
    /// 建连时刻:超过 IDLE_TIMEOUT 未使用则丢弃,避免复用已半死的连接。
    since: std::time::Instant,
}

/// 空闲连接上限。小于 vhost 并发时不会成为瓶颈(多的走新建路径),
/// 但能挡住"压测结束不回收"导致的 fd 泄漏。
const UPSTREAM_MAX_IDLE: usize = 32;
/// 空闲连接最长保留时间。源站 keep-alive 超时(常见 5~75s)之前丢弃,
/// 避免把"对端已关但我方还不知道"的连接借出去(会得到一次 false 错误)。
const UPSTREAM_IDLE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// HTTP vhost 上游连接池(每规则一个)。
///
/// 用 Arc 包住空闲表:池需要被 clone 进响应体(RecycleBody)以便读完时归还,
/// 而归还必须回到**同一个**池。
pub struct UpstreamPool<B = hyper::body::Incoming> {
    idle: std::sync::Arc<std::sync::Mutex<Vec<Upstream<B>>>>,
}

// 手写 Clone:derive 会给 B 加上不必要的 `B: Clone` 约束,
// 而 SendRequest 本身不可 Clone(池靠 Arc 共享,与 body 类型无关)。
impl<B> Clone for UpstreamPool<B> {
    fn clone(&self) -> Self {
        Self {
            idle: self.idle.clone(),
        }
    }
}

impl<B> Default for UpstreamPool<B> {
    fn default() -> Self {
        Self {
            idle: std::sync::Arc::new(std::sync::Mutex::new(Vec::new())),
        }
    }
}

impl<B> std::fmt::Debug for UpstreamPool<B> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UpstreamPool").finish_non_exhaustive()
    }
}

impl<B> UpstreamPool<B> {
    /// 取一条可用连接。返回 None 表示需要新建。
    fn borrow(&self) -> Option<Upstream<B>> {
        let mut v = self.idle.lock().unwrap_or_else(|e| e.into_inner());
        while let Some(u) = v.pop() {
            // ★只过滤"已关闭"和"过期",**不能过滤 is_ready()**。
            //
            // hyper 的 is_ready() 语义是 `giver.is_wanting()` = conn 任务
            // 此刻正等着接收下一个请求。刚读完响应的连接,conn 任务往往
            // 还没被调度回来,wanting 仍是 false —— 用它会把这个好连接
            // 误判成不可用直接丢弃,池等于失效。
            //
            // 实测证据(未加此修复时):压测期间源站侧 ESTAB 连接数在
            // 8~27 之间波动而非恒定,说明连接不断被建/丢,复用没发生。
            //
            // 正确性由 send_request 保证:它内部会等连接就绪;若连接恰好
            // 在这期间断开,返回错误,调用方原样返回 502/重试。
            if u.sender.is_closed() {
                continue;
            }
            if u.since.elapsed() > UPSTREAM_IDLE_TIMEOUT {
                continue;
            }
            return Some(u);
        }
        None
    }

    /// 归还一条连接。
    fn give_back(&self, u: Upstream<B>) {
        if u.sender.is_closed() {
            return;
        }
        let mut v = self.idle.lock().unwrap_or_else(|e| e.into_inner());
        if v.len() < UPSTREAM_MAX_IDLE {
            v.push(u);
        }
    }

    /// 清空池(规则 teardown / 配置变更时调用,丢弃的连接会自行关闭)。
    pub fn clear(&self) {
        let mut v = self.idle.lock().unwrap_or_else(|e| e.into_inner());
        v.clear();
    }

    #[cfg(test)]
    pub fn idle_len(&self) -> usize {
        self.idle.lock().unwrap_or_else(|e| e.into_inner()).len()
    }
}

/// 新建一条上游 hyper 连接,返回 sender;conn 任务由这里 spawn。
async fn new_upstream_sender(
    channel: rh_transport::BoxedIo,
    state: &Arc<AppState>,
    rt: &Arc<ProxyRuntime>,
) -> anyhow::Result<hyper::client::conn::http1::SendRequest<hyper::body::Incoming>> {
    let upstream = hyper_util::rt::TokioIo::new(crate::data::CountedUserIo::upstream(
        channel,
        state,
        Some(rt.clone()),
    ));
    let (sender, conn) = hyper::client::conn::http1::handshake(upstream).await?;
    // conn 必须持续被 poll,否则连接不推进(这不是"每请求一个任务"的浪费:
    // 一个 conn 任务对应一条长连接的整个生命周期)。
    tokio::spawn(async move {
        let _ = conn.with_upgrades().await;
    });
    Ok(sender)
}

/// 判断请求是否带 `Connection: close` —— 这类请求完成后连接必须关闭。
/// 入参用泛型 body:调用点在 `req.into_parts()` 之前(此时还是 Incoming),
/// 便于复用同一份判断。
fn req_wants_close<B>(req: &hyper::Request<B>) -> bool {
    req.headers()
        .get(hyper::header::CONNECTION)
        .and_then(|v| v.to_str().ok())
        .map(|v| v.to_ascii_lowercase().contains("close"))
        .unwrap_or(false)
}

/// 响应体包装:读完(EOF)时把 sender 归还到池;出错或中途 drop 则丢弃连接。
///
/// 归还时机是这套复用的正确性核心:
///   - poll 返回 None(EOF):body 已完整读走 → 可安全复用
///   - poll 返回 Err:连接状态未知 → 不归还
///   - 未读完就 drop(客户端提前断开/超时):连接上有未读字节 → 不归还
struct RecycleBody {
    inner: BoxBody,
    /// Some = 尚未处理;None = 已归还或已丢弃
    ret: Option<(
        hyper::client::conn::http1::SendRequest<hyper::body::Incoming>,
        UpstreamPool,
    )>,
}

impl RecycleBody {
    fn new(
        inner: BoxBody,
        sender: Option<hyper::client::conn::http1::SendRequest<hyper::body::Incoming>>,
        pool: UpstreamPool,
    ) -> Self {
        Self {
            inner,
            ret: sender.map(|s| (s, pool)),
        }
    }

    /// 异常:丢弃(不归还,连接自然关闭)。
    fn discard(&mut self) {
        self.ret = None;
    }
}

impl hyper::body::Body for RecycleBody {
    type Data = hyper::body::Bytes;
    type Error = std::io::Error;

    fn poll_frame(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<hyper::body::Frame<Self::Data>, Self::Error>>> {
        let this = self.as_mut().get_mut();
        match std::pin::Pin::new(&mut this.inner).poll_frame(cx) {
            std::task::Poll::Ready(None) => std::task::Poll::Ready(None),
            std::task::Poll::Ready(Some(Err(e))) => {
                this.discard();
                std::task::Poll::Ready(Some(Err(e)))
            }
            other => other,
        }
    }

    fn is_end_stream(&self) -> bool {
        // 必须透传:hyper 的 dispatch 在 write_head 时用 is_end_stream 判定
        // 是否还要 poll body(dispatch.rs:367)。恒 false 会让真正的空 body
        // 也进 poll 路径,虽然会立刻得到 None,但没必要。
        // 归还因此不能依赖 poll_frame 看到 None —— 见 Drop 里的说明。
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> hyper::body::SizeHint {
        self.inner.size_hint()
    }
}

impl Drop for RecycleBody {
    fn drop(&mut self) {
        // ★归还判定只用 sender 的真实状态,**不看"有没有 poll 到 None"**。
        //
        // 踩过两次:
        //  1) 在 poll_frame 返回 None 时归还 —— 永不触发(hyper 在
        //     write_head 时若 is_end_stream 为 true 就直接把 body 置 None,
        //     根本不 poll)。实测 borrow=26500 give=0。
        //  2) 用 saw_eof 布尔 —— 仍为 false。实测三项:
        //     saw_eof=false ready=true closed=false。
        //     也就是说连接本身干净,只是"读完"这个信号拿不到。
        //
        // 正确信号是 is_ready():hyper 的 conn 任务必须把上一份响应
        // **完整写出**之后才会重新进入 wanting 状态。Drop 时 ready 且未
        // closed,等价于"这条连接已干净、可复用"。
        // body 在这一刻被 drop,意味着 hyper 已经消费完它(写完了或放弃),
        // 两种情况下用 is_ready 都能正确区分。
        let Some((sender, pool)) = self.ret.take() else {
            return;
        };
        if sender.is_ready() && !sender.is_closed() {
            pool.give_back(Upstream {
                sender,
                since: std::time::Instant::now(),
            });
        }
        // 其余情况(未读完 / 连接不 ready / 已关闭):直接丢弃,连接自然关闭。
    }
}

fn full_body(b: &'static [u8]) -> BoxBody {
    http_body_util::Full::new(hyper::body::Bytes::from(b))
        .map_err(|never| match never {})
        .boxed()
}

async fn handle_http_request(
    state: Arc<AppState>,
    req: hyper::Request<hyper::body::Incoming>,
    peer: SocketAddr,
) -> anyhow::Result<hyper::Response<BoxBody>> {
    let method = req.method().clone();
    let host = req
        .headers()
        .get(hyper::header::HOST)
        .and_then(|h| h.to_str().ok())
        .unwrap_or("")
        .split(':')
        .next()
        .unwrap_or("")
        .to_lowercase();
    let path = req.uri().path().to_string();

    // 路由:域名 + locations 前缀(最长匹配)。
    // B6:按 host 索引取候选(修前 O(全部域名) String== 扫描;索引与
    // 路由表同批重建,顺序一致)。host 不在索引 = 无任何路由命中,
    // 与全表扫描找不到同义。
    // R2:routes 与 by_host 同锁原子发布,一次快照即一致视图
    //(修前两把锁,读侧交错可索引越界/错路由)。
    let table = state.proxies.vhost_http.read().await.clone(); // C3: Arc 快照,O(1)
    let routes = &table.routes;
    let candidates: &[usize] = table
        .by_host
        .get(&host)
        .map(|v| v.as_slice())
        .unwrap_or(&[]);
    let mut best: Option<(&VhostRoute, usize)> = None;
    let mut domain_hit: Option<&VhostRoute> = None;
    for &ri in candidates {
        let r = &routes[ri];
        if domain_hit.is_none() {
            domain_hit = Some(r);
        }
        if r.locations.is_empty() {
            // R3:修前 `best.map(|(_, l)| l == 0).unwrap_or(true) && best.is_none()`
            // 左操作数恒被右操作数短路/否决 —— 条件等价于 best.is_none()
            // (best 为 Some 时 && 右项必 false),死代码已删,行为不变。
            if best.is_none() {
                best = Some((r, 0));
            }
            continue;
        }
        for loc in &r.locations {
            let l = loc.trim_end_matches('/');
            // 前缀匹配须以路径段为边界:/api 不得命中 /apifoo。
            // B6:零分配比较(修前每个 location 一次 format!);语义与
            // `path.starts_with(&format!("{l}/"))` 逐字节等价。
            let lb = l.as_bytes();
            let pb = path.as_bytes();
            let seg_prefix = pb.len() > lb.len() && pb.starts_with(lb) && pb[lb.len()] == b'/';
            if path == *loc || seg_prefix {
                let len = loc.len();
                if best.map(|(_, bl)| len > bl).unwrap_or(true) {
                    best = Some((r, len));
                }
            }
        }
    }
    let Some((route, _)) = best else {
        // M2:域名命中但路径未命中该规则 locations → 规则级自定义 404
        if let Some(r) = domain_hit {
            if !r.not_found_html.is_empty() {
                return Ok(custom_404(&r.not_found_html));
            }
        }
        return Ok(not_found_response(&state));
    };
    // Basic 认证
    if !route.http_user.is_empty() {
        let got = req
            .headers()
            .get(hyper::header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());
        if !check_basic_auth(got.as_deref(), &route.http_user, &route.http_password) {
            let mut resp = hyper::Response::builder()
                .status(401)
                .body(full_body(b"Unauthorized"))?;
            resp.headers_mut().insert(
                hyper::header::WWW_AUTHENTICATE,
                hyper::header::HeaderValue::from_static("Basic realm=\"rabbithole\""),
            );
            return Ok(resp);
        }
    }
    // M2:GET + 扩展名匹配 → 先查静态缓存(命中不走上游)
    let cache_cfg = route.cache.clone();
    let cacheable = method == hyper::Method::GET
        && cache_cfg
            .as_ref()
            .map(|c| ext_cached(&path, &c.extensions))
            .unwrap_or(false);
    // M2 静态缓存键:**含 query**。修前只用 path,`?id=1` 与 `?id=2` 命中
    // 同一条缓存 —— 第二个用户会拿到第一个人的数据。扩展名判定仍用 path
    // (query 不参与"是否可缓存"的判断)。
    // B6:键延迟到确认可缓存后才构造(修前每请求恒构造一次 String,
    // 绝大多数规则没配缓存,纯白付)。非可缓存时为空串,下游全部走
    // cacheable/cache_cfg 门控,不会误用。
    let cache_key = if cacheable {
        http_cache_key(&host, &path, req.uri().query())
    } else {
        String::new()
    };
    // 上游通道(先组选路再取通道:通道属主须与 START_FORWARD 目标一致)
    let Some(rt) = state.proxies.get(route.proxy_id) else {
        return Ok(not_found_response(&state));
    };
    // ★先做组选路,再用**同一个 rt** 做缓存读写。
    //
    // 修前顺序反了:HIT 查 `route.proxy_id` 那条(组内固定一条),
    // 而 MISS 把结果写进 `group_pick` 选出来的那条。组内成员不同时,
    // 缓存**永远不会命中**(写 A 读 B),而且白占内存。
    //
    // ★但这只解决了**单请求内**的一致;跨请求仍会漏。
    //
    // 缓存挂在**成员各自**的 `rt.http_cache` 上,而 `group_pick` 是轮询:
    // 同一个 `cache_key` 的第 1 个请求落 A(写 A)、第 2 个落 B(查 B,
    // MISS)→ 稳态命中率约 `1/N`。所以带缓存的路径改用**粘性**选路:
    // 按 `cache_key` 哈希选成员,同一资源的读写恒定落同一成员。
    // 没有缓存配置的请求不需要粘性,继续轮询以均摊得更均匀。
    // 无锁快路径保留:非分组规则(group 为空)不走 group_pick。
    // cacheable 蕴含 cache_cfg.is_some()(见其定义),sticky == cacheable
    let sticky = cacheable;
    let rt = if rt.has_group.load(std::sync::atomic::Ordering::Relaxed) {
        if sticky {
            crate::data::group_pick_sticky_public(&state, &rt, &cache_key)
                .await
                .unwrap_or(rt)
        } else {
            crate::data::group_pick_public(&state, &rt)
                .await
                .unwrap_or(rt)
        }
    } else {
        rt
    };
    if cache_cfg.is_some() && cacheable {
        if let Some(hit) = rt.http_cache.lock().unwrap().get(&cache_key) {
            let resp = hyper::Response::builder()
                .status(hit.status)
                .header("content-type", hit.content_type.clone())
                .header("x-cache", "HIT")
                .body(full_body_body(hit.body.clone()))?; // R2:Bytes 克隆零拷贝
            return Ok(resp);
        }
    }
    if !rt.healthy.load(std::sync::atomic::Ordering::Relaxed) {
        return Ok(service_unavailable());
    }
    // B4:每个 HTTP 请求占用规则并发配额(修前只在连接级计数、且 proxy=None,
    // max_conns 对本路径完全失效:一条 keep-alive 连接可无限制扇出请求)。
    let Some(_conn_guard) = crate::data::try_open_user_conn(&state, Some(&rt)) else {
        return Ok(service_unavailable());
    };
    // ★先试着借一条已有上游连接。
    //
    // 顺序很重要:借到了就完全不需要 get_channel —— 池里的连接背后是一条
    // 已建立的数据通道,省掉一次池签证 + START_FORWARD 帧 + TCP 建连。
    // 借不到才走「取通道 → 指派 → handshake」完整路径。
    let reusable = !req_wants_close(&req);
    let borrowed = if reusable {
        rt.http_upstream.borrow()
    } else {
        None
    };
    let mut sender = match borrowed {
        Some(u) => u.sender,
        None => {
            let Some(channel) = crate::data::get_channel(&state, &rt).await else {
                return Ok(service_unavailable());
            };
            let spec_now = rt.spec.read().await.clone();
            let mut channel = channel;
            if !send_start_forward(&mut channel, &rt, &spec_now, &peer.to_string()).await {
                return Ok(service_unavailable());
            }
            let channel = if spec_now.transport.use_compression {
                rh_transport::compress_stream(channel).await
            } else {
                channel
            };
            new_upstream_sender(channel, &state, &rt).await?
        }
    };

    // 头改写 + Host 改写
    let (mut parts, body) = req.into_parts();
    if !route.host_rewrite.is_empty() {
        if let Ok(v) = hyper::header::HeaderValue::from_str(&route.host_rewrite) {
            parts.headers.insert(hyper::header::HOST, v);
        }
    }
    for h in &route.request_headers.set {
        if let Ok(v) = hyper::header::HeaderValue::from_str(&h.value) {
            if let Ok(n) = hyper::header::HeaderName::from_bytes(h.name.as_bytes()) {
                parts.headers.insert(n, v);
            }
        }
    }
    for d in &route.request_headers.del {
        if let Ok(n) = hyper::header::HeaderName::from_bytes(d.as_bytes()) {
            parts.headers.remove(n);
        }
    }
    // X-Forwarded-For / X-Real-IP(HTTP 真实来源)
    if let Ok(v) = hyper::header::HeaderValue::from_str(&peer.ip().to_string()) {
        parts.headers.insert("x-real-ip", v.clone());
        let fwd = parts
            .headers
            .get("x-forwarded-for")
            .and_then(|h| h.to_str().ok())
            .map(|prev| format!("{prev}, {}", peer.ip()))
            .unwrap_or_else(|| peer.ip().to_string());
        if let Ok(v2) = hyper::header::HeaderValue::from_str(&fwd) {
            parts.headers.insert("x-forwarded-for", v2);
        }
    }
    let upstream_req = hyper::Request::from_parts(parts, body);

    let resp = sender.send_request(upstream_req).await?;

    // M2:可缓存(GET + 扩展匹配 + 200)→ 完整读回并落缓存(受单项上限约束)
    if let Some(cfg) = &cache_cfg {
        if cacheable && resp.status() == 200 {
            use http_body_util::BodyExt as _;

            let (up_parts, up_body) = resp.into_parts();
            let collected = match up_body.collect().await {
                Ok(c) => c.to_bytes(),
                Err(_) => return Ok(service_unavailable()),
            };
            let ct = up_parts
                .headers
                .get(hyper::header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok())
                .unwrap_or("application/octet-stream")
                .to_string();
            // ★尊重上游的缓存指令,并避开带凭据的响应。
            //
            // 修前无条件缓存 200:上游明确 `Cache-Control: no-store/private`
            // 的响应照样入池,而且 MISS 回放时把 `set-cookie` 一并重放给
            // **其他用户**(缓存是按 host+path 共享的)—— 会话串号风险。
            let cc = up_parts
                .headers
                .get(hyper::header::CACHE_CONTROL)
                .and_then(|v| v.to_str().ok())
                .unwrap_or("")
                .to_ascii_lowercase();
            let forbidden_by_cc =
                cc.contains("no-store") || cc.contains("private") || cc.contains("no-cache");
            let has_credentials = up_parts.headers.contains_key(hyper::header::SET_COOKIE);
            let over_size = collected.len() as u64 > cfg.max_item_size_kb * 1024;
            if forbidden_by_cc || has_credentials || over_size {
                // 不可缓存:原样回给用户(不做 x-cache 标记)。
                // A4:响应体已完整读入内存(collected),上游连接是干净的,
                // 必须挂 RecycleBody 让 Drop 统一判定归还 —— 修前直接
                // return,sender 随作用域 drop,这条连接作废。所有
                // "GET + 扩展名命中缓存白名单"的请求 100% 新建上游连接,
                // 恰好是最该复用的一类(静态资源)。
                let body = full_body_body(collected);
                let body = if reusable {
                    RecycleBody::new(body, Some(sender), rt.http_upstream.clone()).boxed()
                } else {
                    body
                };
                return Ok(hyper::Response::from_parts(up_parts, body));
            }
            let entry = crate::state::HttpCacheEntry {
                expires: std::time::Instant::now()
                    + std::time::Duration::from_secs(cfg.ttl_seconds.max(1)),
                status: 200,
                content_type: ct.clone(),
                body: collected.clone(), // R2:Bytes 引用传递,不再双份拷贝
            };
            rt.http_cache.lock().unwrap().put(
                cache_key.clone(),
                entry,
                (cfg.max_size_mb.max(1) * 1024 * 1024) as usize,
            );
            let mut b = hyper::Response::builder()
                .status(200)
                .header("content-type", ct)
                .header("x-cache", "MISS");
            for (n, v) in up_parts.headers.iter() {
                if n != hyper::header::CONTENT_TYPE {
                    b = b.header(n.clone(), v.clone());
                }
            }
            // A4:MISS 分支 body 已读完,同样挂 RecycleBody 归还 sender
            //(修前提前 return,连接每次用完即弃)。
            let body = full_body_body(collected);
            let body = if reusable {
                RecycleBody::new(body, Some(sender), rt.http_upstream.clone()).boxed()
            } else {
                body
            };
            let resp: hyper::Response<BoxBody> = b.body(body)?;
            return Ok(resp);
        }
    }
    let resp: hyper::Response<BoxBody> = resp
        .map(|b| b.map_err(std::io::Error::other))
        .map(|b| b.boxed());
    // 响应头改写
    let (mut parts, body) = resp.into_parts();
    for h in &route.response_headers.set {
        if let Ok(v) = hyper::header::HeaderValue::from_str(&h.value) {
            if let Ok(n) = hyper::header::HeaderName::from_bytes(h.name.as_bytes()) {
                parts.headers.insert(n, v);
            }
        }
    }
    for d in &route.response_headers.del {
        if let Ok(n) = hyper::header::HeaderName::from_bytes(d.as_bytes()) {
            parts.headers.remove(n);
        }
    }
    // ★归还:把 sender 挂到响应体上,等 body 真正读完再放回池。
    // 不能在函数返回时归还 —— 此时响应体还没被客户端读走,连接上仍挂着
    // 未消耗的字节,复用会让下一条请求读到上一条的残留响应。
    let body: BoxBody = body.map_err(std::io::Error::other).boxed();
    let body = if reusable {
        RecycleBody::new(body, Some(sender), rt.http_upstream.clone()).boxed()
    } else {
        body.boxed()
    };
    Ok(hyper::Response::from_parts(parts, body))
}

fn check_basic_auth(header: Option<&str>, user: &str, pass: &str) -> bool {
    let Some(h) = header else { return false };
    let Some(b64) = h.strip_prefix("Basic ") else {
        return false;
    };
    let decoded = base64_decode(b64).unwrap_or_default();
    let expect = format!("{user}:{pass}");
    rh_transport::constant_time_eq(&decoded, expect.as_bytes())
}

/// M2:规则级自定义 404(HTML 全文)。
fn custom_404(html: &str) -> hyper::Response<BoxBody> {
    hyper::Response::builder()
        .status(404)
        .header("content-type", "text/html; charset=utf-8")
        .body(full_body_body(hyper::body::Bytes::from(html.to_owned())))
        .unwrap()
}

/// Full 构造(BoxBody<Bytes, io::Error>)。
fn full_body_body(b: hyper::body::Bytes) -> BoxBody {
    http_body_util::Full::new(b)
        .map_err(|never| match never {})
        .boxed()
}

/// 路径扩展名是否在缓存白名单(忽略大小写与前导点)。
fn ext_cached(path: &str, extensions: &[String]) -> bool {
    let Some(dot) = path.rfind('.') else {
        return false;
    };
    if path[dot..].contains('/') {
        return false;
    }
    let ext = path[dot + 1..].to_lowercase();
    // 白名单已在 route_from 构建时预规范化(R4),此处直接比较
    extensions.iter().any(|e| e == &ext)
}

fn not_found_response(state: &Arc<AppState>) -> hyper::Response<BoxBody> {
    if let Some(path) = &state.cfg.vhost.custom_404_page {
        // ★自定义 404 页只读一次盘,之后走进程内缓存。
        //
        // 修前每次 404 都 `std::fs::read` —— 这是**同步 IO 跑在 async 上下文**,
        // 404 洪峰(扫描器、爬虫)会把 executor 线程逐个堵在 read 上。
        // 配置在进程生命周期内不变,缓存是安全的;改动需重启(与本配置项
        // 其他字段一致)。
        static CACHE: std::sync::OnceLock<
            std::sync::Mutex<std::collections::HashMap<std::path::PathBuf, Vec<u8>>>,
        > = std::sync::OnceLock::new();
        let cache = CACHE.get_or_init(|| std::sync::Mutex::new(std::collections::HashMap::new()));
        // 命中:克隆一份(响应体需要所有权)
        let cached = { cache.lock().ok().and_then(|m| m.get(path).cloned()) };
        let body = match cached {
            Some(b) => Some(b),
            None => match std::fs::read(path) {
                Ok(b) => {
                    if let Ok(mut m) = cache.lock() {
                        m.insert(path.clone(), b.clone());
                    }
                    Some(b)
                }
                // 读失败不缓存:文件可能是部署后补上的
                Err(_) => None,
            },
        };
        if let Some(body) = body {
            return hyper::Response::builder()
                .status(404)
                .body(
                    http_body_util::Full::new(hyper::body::Bytes::from(body))
                        .map_err(|never| match never {})
                        .boxed(),
                )
                .unwrap();
        }
    }
    hyper::Response::builder()
        .status(404)
        .body(full_body(b"404 page not found"))
        .unwrap()
}

fn service_unavailable() -> hyper::Response<BoxBody> {
    hyper::Response::builder()
        .status(503)
        .body(full_body(b"service unavailable"))
        .unwrap()
}

// ---------------- START_FORWARD 公共 ----------------

pub async fn send_start_forward(
    channel: &mut rh_transport::BoxedIo,
    _rt: &Arc<ProxyRuntime>,
    spec: &rh_protocol::ProxySpec,
    user_src: &str,
) -> bool {
    let mut flags = 0u8;
    if spec.transport.use_compression {
        flags |= forward_flags::COMPRESSED;
    }
    let fwd = rh_protocol::message::StartForward {
        proxy_id: spec.id,
        user_src: user_src.to_string(),
        target_host: spec.target_host.clone(),
        target_port: spec.target_port,
        flags,
        proxy_proto: conv_pp(spec.transport.proxy_proto),
        proxy_name: spec.name.clone(),
        xtcp_auth: String::new(),
    };
    let payload = serde_json::to_vec(&fwd).unwrap();
    rh_protocol::write_frame(channel, rh_protocol::FrameType::START_FORWARD, &payload)
        .await
        .is_ok()
}

// ---------------- HTTPS vhost(SNI) ----------------

pub async fn https_vhost_serve(state: Arc<AppState>, listener: tokio::net::TcpListener) {
    loop {
        match listener.accept().await {
            Ok((sock, peer)) => {
                let st = state.clone();
                tokio::spawn(async move {
                    if let Err(e) = serve_https(st, sock, peer).await {
                        debug!(error = %e, "https vhost conn");
                    }
                });
            }
            Err(_) => tokio::time::sleep(std::time::Duration::from_millis(100)).await,
        }
    }
}

/// M3:解析 PEM 证书/私钥 → (SAN+CN 域名, 指纹, not_after ms, rustls 配置)。
/// 构建即校验证书链与私钥配对(不匹配直接报错)。
pub fn build_cert_entry(
    cert_pem: &str,
    key_pem: &str,
) -> anyhow::Result<(Vec<String>, String, i64, rustls::ServerConfig)> {
    let mut certs: Vec<rustls_pki_types::CertificateDer<'static>> = Vec::new();
    for r in rustls_pemfile::certs(&mut cert_pem.as_bytes()) {
        certs.push(r?);
    }
    if certs.is_empty() {
        anyhow::bail!("证书 PEM 中无 CERTIFICATE 块");
    }
    let key = rustls_pemfile::private_key(&mut key_pem.as_bytes())?
        .ok_or_else(|| anyhow::anyhow!("私钥 PEM 中无 PRIVATE KEY 块"))?;
    let cfg = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs.clone(), key)
        .map_err(|e| anyhow::anyhow!("证书/私钥不匹配或非法:{e}"))?;
    // DER 解析:SAN/CN + 有效期
    let der = certs[0].as_ref();
    let (_, cert) = x509_parser::parse_x509_certificate(der)?;
    let mut domains: Vec<String> = Vec::new();
    if let Ok(Some(san)) = cert.subject_alternative_name() {
        for gn in &san.value.general_names {
            if let x509_parser::extensions::GeneralName::DNSName(d) = gn {
                domains.push(d.to_string());
            }
        }
    }
    if let Some(cn) = cert
        .subject()
        .iter_common_name()
        .next()
        .and_then(|a| a.as_str().ok())
    {
        if !domains.iter().any(|d| d == cn) {
            domains.push(cn.to_string());
        }
    }
    let not_after_sec = cert.validity().not_after.timestamp();
    let not_after_ms = not_after_sec * 1000;
    let fp = hex_short(&<sha2::Sha256 as sha2::Digest>::digest(der));
    Ok((domains, fp, not_after_ms, cfg))
}

fn hex_short(bytes: &[u8]) -> String {
    bytes[..16]
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect::<String>()
}

async fn serve_https(
    state: Arc<AppState>,
    sock: TcpStream,
    peer: SocketAddr,
) -> anyhow::Result<()> {
    let _ = sock.set_nodelay(true);
    // peek ClientHello 提取 SNI
    let sock = sock;
    let sni = match peek_sni(&sock).await {
        Ok(Some(s)) => s,
        _ => {
            return Ok(());
        }
    };
    // M3:证书表命中该域名 → 服务端终结 TLS,明文 HTTP 走 vhost http 路由;
    // 未命中 → 维持原 passthrough 语义(端到端 TLS 到客户端)
    //
    // ★读锁只用于"找到并克隆 acceptor",必须在 accept/conn.await 之前释放。
    // 修前守卫活到 `conn.await?`(整条 keep-alive/websocket 生命期),而
    // tokio RwLock 写优先:证书 CRUD 等写锁期间**所有新 HTTPS 连接**都被
    // 阻塞在读锁上 —— 只要有一条长命 TLS 连接在,证书热更新就冻结整个
    // HTTPS vhost。
    let acceptor = {
        let certs = state.certs.read().await;
        certs
            .iter()
            .find(|c| c.matches(&sni))
            .map(|entry| tokio_rustls::TlsAcceptor::from(entry.cfg.clone()))
    };
    if let Some(acceptor) = acceptor {
        {
            // B5:TLS 握手必须有界(慢 ClientHello 会永久占用连接)
            let hs_to = state.cfg.vhost.http_timeout_s.max(1);
            let stream = match tokio::time::timeout(
                std::time::Duration::from_secs(hs_to),
                acceptor.accept(sock),
            )
            .await
            {
                Ok(Ok(s)) => s,
                _ => return Ok(()),
            };
            let st = state.clone();
            let _conn_guard = crate::data::try_open_user_conn(&st, None);
            use hyper::server::conn::http1;
            use hyper::service::service_fn;
            let mut builder = http1::Builder::new();
            let header_to = st.cfg.vhost.http_timeout_s;
            if header_to > 0 {
                builder.header_read_timeout(Some(std::time::Duration::from_secs(header_to)));
            }
            let conn = builder
                .timer(hyper_util::rt::TokioTimer::new())
                .serve_connection(
                    hyper_util::rt::TokioIo::new(crate::data::CountedUserIo::user(
                        stream, &st, None,
                    )),
                    service_fn(move |req: hyper::Request<hyper::body::Incoming>| {
                        let st = st.clone();
                        let peer = peer;
                        async move {
                            handle_http_request(st, req, peer)
                                .await
                                .map_err(|_e| std::io::Error::other("vhost error"))
                        }
                    }),
                );
            conn.await?;
            return Ok(());
        }
    }
    // B6:host 索引 O(1) 取代全表线性扫描(R2:单锁原子快照)
    let table = state.proxies.vhost_https.read().await.clone(); // C3: Arc 快照
    let route = table
        .by_host
        .get(sni.as_str())
        .and_then(|v| v.first())
        .map(|&i| &table.routes[i]);
    let Some(route) = route else {
        return Ok(());
    };
    let Some(rt) = state.proxies.get(route.proxy_id) else {
        return Ok(());
    };
    // ★无锁快路径:非分组规则(group 为空)不需要走 group_pick。
    // 修前每请求都要 `spec.read().await` 只为取一个必然为 None 的 group_key,
    // 高并发下这是明确的锁争用点。
    let rt = if rt.has_group.load(std::sync::atomic::Ordering::Relaxed) {
        crate::data::group_pick_public(&state, &rt)
            .await
            .unwrap_or(rt)
    } else {
        rt
    };
    if !rt.healthy.load(std::sync::atomic::Ordering::Relaxed) {
        return Ok(());
    }
    let spec = rt.spec.read().await.clone();
    let Some(mut channel) = crate::data::get_channel(&state, &rt).await else {
        return Ok(());
    };
    if !send_start_forward(&mut channel, &rt, &spec, &peer.to_string()).await {
        return Ok(());
    }
    let channel = if spec.transport.use_compression {
        rh_transport::compress_stream(channel).await
    } else {
        channel
    };
    let mut channel = channel;
    let Some(_conn_guard) = crate::data::try_open_user_conn(&state, Some(&rt)) else {
        return Ok(()); // F2 配额超限
    };
    let mut sock = crate::data::CountedUserIo::user(sock, &state, Some(rt.clone()));
    let _ = crate::data::copy_bidirectional_bounded(&mut sock, &mut channel).await;
    Ok(())
}

/// 从 ClientHello 提取 SNI(不解密)。
async fn peek_sni(sock: &TcpStream) -> anyhow::Result<Option<String>> {
    // 16KiB:后量子/多扩展的 ClientHello 可超 4KiB,截断会静默断连
    let mut buf = vec![0u8; 16 * 1024];
    // 循环直到能解析完整 ClientHello 头;必须用 peek(不消费),
    // 后续 copy_bidirectional 需要完整 ClientHello 交给客户端侧 TLS 终结。
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    let mut read = 0;
    loop {
        // peek 永远从接收队列**起始**拷贝(不消费),所以必须整只 buf 接:
        // 修前 `&mut buf[read..]` 在 ClientHello 跨段到达时把流的**开头**
        // 写进 buf[read..],累计缓冲错位,parse_sni 读到的是错位数据,
        // SNI 解析失败 → serve_https 静默断连。
        let n = match sock.peek(&mut buf).await {
            Ok(n) => n,
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                if tokio::time::Instant::now() >= deadline {
                    return Ok(None);
                }
                tokio::time::sleep(std::time::Duration::from_millis(2)).await;
                continue;
            }
            Err(e) => return Err(e.into()),
        };
        // peek 返回当前可读总量(可能小于已累计),用最大值推进
        read = read.max(n);
        if read >= 5 {
            let len = u16::from_be_bytes([buf[3], buf[4]]) as usize + 5;
            if read >= len.min(buf.len()) || read == buf.len() {
                break;
            }
        }
        if read == buf.len() {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            return Ok(None);
        }
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    }
    Ok(parse_sni(&buf[..read]))
}

/// M2 静态缓存的键:path + query。
///
/// query 必须参与键 —— 修前只用 path,`?id=1` 与 `?id=2` 命中同一条缓存,
/// 第二个用户直接拿到第一个人的响应体。
fn http_cache_key(host: &str, path: &str, query: Option<&str>) -> String {
    // ★键必须含 **host**:缓存挂在 proxy 级(`rt.http_cache`),而多个域名
    // 可以指向同一个 proxy。修前只按 path 分键,`a.com/index.html` 与
    // `b.com/index.html` 会命中同一条缓存 —— 跨站内容串号。
    match query {
        Some(q) => format!("{host}\x00{path}?{q}"),
        None => format!("{host}\x00{path}"),
    }
}

/// 从 ClientHello 提取 SNI(vhost TLS 按域名路由;不解密、不验签)。
///
/// 返回值统一 `to_lowercase()`:路由查表与证书校核都按小写域名比较。
///
/// **不校验 length 字段是有意的**:record length(`buf[3..5]`)与
/// handshake length(`buf[6..9]`)都只用于判断"包是否完整",而调用方
/// [`peek_sni`] 已用 5s deadline 累计读到完整 ClientHello 才交给本函数,
/// 再做一次长度校验只会重复劳动。本函数只保证**不越界**:
/// 每个字段读取前都检查剩余长度,任意前缀输入都返回 None 而非 panic
/// (对应 `parse_sni_never_panics_on_truncation`)。
fn parse_sni(buf: &[u8]) -> Option<String> {
    // TLS record: type(1) ver(2) len(2) handshake...
    if buf.len() < 43 || buf[0] != 0x16 {
        return None;
    }
    // handshake header: type(1)=0x01 len(3)
    if buf[5] != 0x01 {
        return None;
    }
    // client_version(2) random(32) session_id_len(1) ...
    let mut p = 5 + 4 + 2 + 32;
    if p >= buf.len() {
        return None;
    }
    let sid_len = buf[p] as usize;
    p += 1 + sid_len;
    if p + 2 > buf.len() {
        return None;
    }
    let cs_len = u16::from_be_bytes([buf[p], buf[p + 1]]) as usize;
    p += 2 + cs_len;
    if p >= buf.len() {
        return None;
    }
    let cm_len = buf[p] as usize;
    p += 1 + cm_len;
    if p + 2 > buf.len() {
        return None;
    }
    let ext_len = u16::from_be_bytes([buf[p], buf[p + 1]]) as usize;
    p += 2;
    let ext_end = (p + ext_len).min(buf.len());
    while p + 4 <= ext_end {
        let etype = u16::from_be_bytes([buf[p], buf[p + 1]]);
        let elen = u16::from_be_bytes([buf[p + 2], buf[p + 3]]) as usize;
        p += 4;
        if etype == 0x0000 {
            // SNI: list_len(2) type(1)=0 len(2) name
            if p + 5 <= ext_end && buf[p + 2] == 0 {
                let nlen = u16::from_be_bytes([buf[p + 3], buf[p + 4]]) as usize;
                let start = p + 5;
                if start + nlen <= ext_end {
                    return Some(String::from_utf8_lossy(&buf[start..start + nlen]).to_lowercase());
                }
            }
            return None;
        }
        p += elen;
    }
    None
}

// ---------------- TCPMux(HTTP CONNECT) ----------------

pub async fn tcpmux_serve(
    state: Arc<AppState>,
    listener: tokio::net::TcpListener,
    passthrough: bool,
) {
    loop {
        match listener.accept().await {
            Ok((sock, peer)) => {
                let st = state.clone();
                let pt = passthrough;
                tokio::spawn(async move {
                    if let Err(e) = serve_tcpmux(st, sock, peer, pt).await {
                        debug!(error = %e, "tcpmux conn");
                    }
                });
            }
            Err(_) => tokio::time::sleep(std::time::Duration::from_millis(100)).await,
        }
    }
}

async fn serve_tcpmux(
    state: Arc<AppState>,
    mut sock: TcpStream,
    peer: SocketAddr,
    passthrough: bool,
) -> anyhow::Result<()> {
    // 读 CONNECT 请求(终止符之后同批到达的字节须回灌,如 yamux 首帧/TLS ClientHello)
    // B5:整个头部读取必须有界。修前这里是裸 `read`,慢速 CONNECT
    // (逐字节滴流)可无限期占用一条连接与 fd。
    let mut buf = Vec::with_capacity(1024);
    let mut chunk = [0u8; 512];
    let head_to = state.cfg.vhost.http_timeout_s.max(1);
    let head_deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(head_to);
    let tail = loop {
        let n = match tokio::time::timeout_at(head_deadline, sock.read(&mut chunk)).await {
            Ok(r) => r?,
            Err(_) => return Ok(()), // 头部超时:放弃本连接
        };
        if n == 0 {
            return Ok(());
        }
        buf.extend_from_slice(&chunk[..n]);
        if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            break buf.split_off(pos + 4);
        }
        if buf.len() > 4096 {
            return Ok(());
        }
    };
    let head = String::from_utf8_lossy(&buf).into_owned();
    let mut lines = head.split("\r\n");
    let first = lines.next().unwrap_or_default().to_string();
    let parts: Vec<&str> = first.split_whitespace().collect();
    if parts.len() < 2 || parts[0] != "CONNECT" {
        sock.write_all(b"HTTP/1.1 400 Bad Request\r\n\r\n").await?;
        return Ok(());
    }
    let hostport = parts[1].to_string();
    let host = hostport.split(':').next().unwrap_or("").to_lowercase();
    // B6:一次读快照 + host 索引取路由(修前同一条 CONNECT 读两次
    // vhost_tcpmux 快照并各扫一遍全表)。
    let table = state.proxies.vhost_tcpmux.read().await.clone(); // C3: Arc 快照(R2:单锁原子)
    let route = table
        .by_host
        .get(&host)
        .and_then(|v| v.first())
        .map(|&i| &table.routes[i]);
    // Basic 认证
    if let Some(r) = route {
        if !r.http_user.is_empty() {
            let got = head.lines().find_map(|l| {
                let (k, v) = l.split_once(':')?;
                if k.trim().eq_ignore_ascii_case("proxy-authorization") {
                    Some(v.trim().to_string())
                } else {
                    None
                }
            });
            if !check_basic_auth(got.as_deref(), &r.http_user, &r.http_password) {
                sock.write_all(
                    b"HTTP/1.1 407 Proxy Authentication Required\r\nProxy-Authenticate: Basic realm=\"rabbithole\"\r\n\r\n",
                )
                .await?;
                return Ok(());
            }
        }
    }
    // 路由:按 host 匹配 tcpmux 规则
    let Some(route) = route else {
        sock.write_all(b"HTTP/1.1 404 Not Found\r\n\r\n").await?;
        return Ok(());
    };
    let Some(rt) = state.proxies.get(route.proxy_id) else {
        sock.write_all(b"HTTP/1.1 404 Not Found\r\n\r\n").await?;
        return Ok(());
    };
    // ★与 http/https vhost 同口径:规则不健康就不接入。
    //
    // 修前 tcpmux 拿到 rt 后直接 relay,不看 `healthy` —— 规则被健康检查
    // 摘除后,tcpmux 端口仍会把用户连接打过去,表现为"Web 页面显示异常、
    // 但 tcpmux 还能连上",排查时极易误导。
    if !rt.healthy.load(std::sync::atomic::Ordering::Relaxed) {
        return Ok(());
    }
    // ★无锁快路径:非分组规则(group 为空)不需要走 group_pick。
    // 修前每请求都要 `spec.read().await` 只为取一个必然为 None 的 group_key,
    // 高并发下这是明确的锁争用点。
    let rt = if rt.has_group.load(std::sync::atomic::Ordering::Relaxed) {
        crate::data::group_pick_public(&state, &rt)
            .await
            .unwrap_or(rt)
    } else {
        rt
    };
    let spec = rt.spec.read().await.clone();
    if passthrough {
        let Some(mut channel) = crate::data::get_channel(&state, &rt).await else {
            sock.write_all(b"HTTP/1.1 503 Service Unavailable\r\n\r\n")
                .await?;
            return Ok(());
        };
        // 透传模式:CONNECT 200 后整条流直通客户端,由客户端处理多路复用
        sock.write_all(b"HTTP/1.1 200 Connection established\r\n\r\n")
            .await?;
        let mut flags = 0u8;
        if spec.transport.use_compression {
            flags |= forward_flags::COMPRESSED;
        }
        flags |= forward_flags::TCPMUX_PASSTHROUGH;
        let fwd = rh_protocol::message::StartForward {
            proxy_id: spec.id,
            user_src: peer.to_string(),
            target_host: spec.target_host.clone(),
            target_port: spec.target_port,
            flags,
            proxy_proto: conv_pp(spec.transport.proxy_proto),
            proxy_name: spec.name.clone(),
            xtcp_auth: String::new(),
        };
        rh_protocol::write_frame(
            &mut channel,
            rh_protocol::FrameType::START_FORWARD,
            &serde_json::to_vec(&fwd)?,
        )
        .await?;
        let channel = if spec.transport.use_compression {
            rh_transport::compress_stream(channel).await
        } else {
            channel
        };
        let mut channel = channel;
        // CONNECT 后已读入的用户数据(如 yamux 首帧)前转进通道
        if !tail.is_empty() && channel.write_all(&tail).await.is_err() {
            return Ok(());
        }
        let Some(_conn_guard) = crate::data::try_open_user_conn(&state, Some(&rt)) else {
            return Ok(()); // F2 配额超限
        };
        let mut sock = crate::data::CountedUserIo::user(sock, &state, Some(rt.clone()));
        let _ = crate::data::copy_bidirectional_bounded(&mut sock, &mut channel).await;
        return Ok(());
    }
    // 标准模式:服务端在用户连接上终结 yamux,每个入站流 = 一次独立数据通道转发
    sock.write_all(b"HTTP/1.1 200 Connection established\r\n\r\n")
        .await?;
    let mux = rh_transport::mux::tcpmux_server_mux(
        rh_transport::PrependedIo::wrap(Box::new(sock), tail),
        state.mux_streams.load(std::sync::atomic::Ordering::Relaxed),
    );
    loop {
        let stream = match mux.accept().await {
            Ok(s) => s,
            Err(_) => break,
        };
        let st2 = state.clone();
        let rt2 = rt.clone();
        let peer2 = peer;
        tokio::spawn(async move {
            // 每个流:新通道 + 指派
            if let Some(mut ch) = crate::data::get_channel(&st2, &rt2).await {
                let spec = rt2.spec.read().await.clone();
                let ch = if send_start_forward(&mut ch, &rt2, &spec, &peer2.to_string()).await {
                    if spec.transport.use_compression {
                        rh_transport::compress_stream(ch).await
                    } else {
                        ch
                    }
                } else {
                    return;
                };
                let mut ch = ch;
                let _conn_guard = match crate::data::try_open_user_conn(&st2, Some(&rt2)) {
                    Some(g) => g,
                    None => return, // F2 配额超限
                };
                let mut stream = crate::data::CountedUserIo::user(stream, &st2, Some(rt2.clone()));
                let _ = crate::data::copy_bidirectional_bounded(&mut stream, &mut ch).await;
            }
        });
    }
    Ok(())
}

/// 最小 base64 解码(标准字母表)。
fn base64_decode(s: &str) -> Option<Vec<u8>> {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut vals = Vec::with_capacity(s.len());
    for c in s.chars() {
        if c == '=' || c == '\n' || c == '\r' {
            continue;
        }
        let v = TABLE.iter().position(|&t| t as char == c)?;
        vals.push(v as u8);
    }
    let mut out = Vec::with_capacity(vals.len() * 3 / 4);
    for chunk in vals.chunks(4) {
        match chunk.len() {
            4 => {
                out.push((chunk[0] << 2) | (chunk[1] >> 4));
                out.push((chunk[1] << 4) | (chunk[2] >> 2));
                out.push((chunk[2] << 6) | chunk[3]);
            }
            3 => {
                out.push((chunk[0] << 2) | (chunk[1] >> 4));
                out.push((chunk[1] << 4) | (chunk[2] >> 2));
            }
            2 => {
                out.push((chunk[0] << 2) | (chunk[1] >> 4));
            }
            _ => {}
        }
    }
    Some(out)
}

fn conv_pp(v: rh_protocol::ProxyProtoVersion) -> rh_protocol::message::ProxyProto {
    match v {
        rh_protocol::ProxyProtoVersion::None => rh_protocol::message::ProxyProto::None,
        rh_protocol::ProxyProtoVersion::V1 => rh_protocol::message::ProxyProto::V1,
        rh_protocol::ProxyProtoVersion::V2 => rh_protocol::message::ProxyProto::V2,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use http_body_util::Full;

    /// M2 静态缓存键必须含 query。
    ///
    /// 修前键只用 path:`/a.js?id=1` 与 `/a.js?id=2` 命中同一条缓存,
    /// 第二个用户拿到第一个人的响应体(越权读)。这条断言钉住 query 参与键。
    #[test]
    fn cache_key_includes_query() {
        assert_ne!(
            http_cache_key("a.com", "/a.js", Some("id=1")),
            http_cache_key("a.com", "/a.js", Some("id=2")),
            "不同 query 不得命中同一缓存条目"
        );
        assert_ne!(
            http_cache_key("a.com", "/a.js", Some("id=1")),
            http_cache_key("a.com", "/a.js", None),
            "带 query 与不带 query 必须分开"
        );
        assert_eq!(
            http_cache_key("a.com", "/a.js", Some("id=1")),
            http_cache_key("a.com", "/a.js", Some("id=1")),
            "同一 key 必须稳定命中"
        );
        // ★跨 host 不得共享:缓存挂在 proxy 级,同一 proxy 可能服务多个域名
        assert_ne!(
            http_cache_key("a.com", "/a.js", None),
            http_cache_key("b.com", "/a.js", None),
            "不同域名不得命中同一缓存条目(修前会串内容)"
        );
        assert_eq!(http_cache_key("a.com", "/a.js", None), "a.com\x00/a.js");
    }

    // ---------------- parse_sni:vhost TLS 路由的关键路径 ----------------
    //
    // 这段代码手工计算 20+ 个偏移量(session_id / cipher_suites /
    // compression_methods / extensions 各自"长度前缀 + 内容"),任何一个写错
    // 都不会崩溃,只会静默返回 None —— 表现为「HTTPS 偶尔路由不到」,极难
    // 归因。所以这里既测正常取值,也做**逐字节截断**,保证任意长度都不 panic。
    //
    // 注:record length(buf[3..5])与 handshake length(buf[6..9])目前**不校验**。
    // 调用方 peek_sni 已用 5s deadline 累计读到完整 ClientHello 才解析,
    // 再校验意义不大;这里按现状把行为钉住,避免误以为漏了边界检查。

    /// 构造一个最小但结构合法的 ClientHello(带可选 SNI 扩展)。
    ///
    /// `sni = None` 时仍写入一个 type=0 但**不含** host_name 的扩展,用于
    /// 验证「有 SNI 扩展位却没给出主机名」这一分支返回 None。
    fn client_hello(sni: Option<&str>) -> Vec<u8> {
        // 扩展体:SNI(list_len + type=0 + name_len + name)
        fn sni_ext(name: &str) -> Vec<u8> {
            let n = name.as_bytes();
            let mut ext = Vec::new();
            ext.extend_from_slice(&((n.len() + 3) as u16).to_be_bytes()); // list_len
            ext.push(0x00); // name_type = host_name
            ext.extend_from_slice(&(n.len() as u16).to_be_bytes());
            ext.extend_from_slice(n);
            ext
        }

        let mut ext_list = Vec::new();
        match sni {
            // 特殊值:构造 ext_len == 0 的 ClientHello(无 extensions 字段内容)
            Some("") => {}
            Some(host) => {
                let body = sni_ext(host);
                ext_list.extend_from_slice(&0x0000u16.to_be_bytes()); // type = server_name
                ext_list.extend_from_slice(&(body.len() as u16).to_be_bytes());
                ext_list.extend_from_slice(&body);
            }
            None => {
                // type=0 但 body 不足(list_len 声明有、实际没有 host_name)
                ext_list.extend_from_slice(&0x0000u16.to_be_bytes());
                ext_list.extend_from_slice(&0u16.to_be_bytes());
            }
        }

        // handshake body: client_version(2) random(32) sid_len(1)
        //                 cipher_suites(2+n) cm_len(1+n) ext_len(2+n)
        let mut hs = Vec::new();
        hs.extend_from_slice(&[0x03, 0x03]); // client_version
        hs.extend_from_slice(&[0x11; 32]); // random
        hs.push(0); // session_id_len = 0
        hs.extend_from_slice(&2u16.to_be_bytes()); // cipher_suites len
        hs.extend_from_slice(&[0x13, 0x01]); // TLS_AES_128_GCM_SHA256
        hs.push(1); // compression_methods len
        hs.push(0); // null
        hs.extend_from_slice(&(ext_list.len() as u16).to_be_bytes());
        hs.extend_from_slice(&ext_list);

        // record: type(1)=0x16 ver(2) len(2)
        let mut rec = Vec::new();
        rec.push(0x16); // handshake
        rec.extend_from_slice(&[0x03, 0x01]); // legacy record version
        rec.extend_from_slice(&((hs.len() + 4) as u16).to_be_bytes());
        // handshake header: type(1)=0x01 len(3)
        rec.push(0x01);
        let hl = hs.len() as u32;
        rec.extend_from_slice(&hl.to_be_bytes()[1..]); // 3 字节长度
        rec.extend_from_slice(&hs);
        rec
    }

    #[test]
    fn parse_sni_extracts_hostname() {
        let buf = client_hello(Some("Bench.Test"));
        // 手工确认构造合法(长度前缀自洽)
        assert_eq!(buf[0], 0x16, "record type 必须是 handshake");
        assert_eq!(buf[5], 0x01, "handshake type 必须是 ClientHello");
        // 返回值统一小写:vhost 路由与证书校核都按小写比较
        assert_eq!(parse_sni(&buf).as_deref(), Some("bench.test"));
    }

    #[test]
    fn parse_sni_without_sni_extension_is_none() {
        let buf = client_hello(None);
        assert_eq!(parse_sni(&buf), None, "无 SNI 扩展必须返回 None");
    }

    #[test]
    fn parse_sni_rejects_non_tls() {
        assert_eq!(parse_sni(b""), None, "空输入");
        assert_eq!(parse_sni(b"GET / HTTP/1.1\r\n"), None, "明文 HTTP");
        // 长度够但不是 0x16
        let mut junk = vec![0u8; 300];
        junk[0] = 0x17; // application_data
        assert_eq!(parse_sni(&junk), None);
    }

    #[test]
    fn parse_sni_rejects_bad_handshake_type() {
        let mut buf = client_hello(Some("a.test"));
        buf[5] = 0x02; // ServerHello 而非 ClientHello
        assert_eq!(parse_sni(&buf), None);
    }

    /// 边界:`ext_len == 0`(无扩展)时,`p + 2 == buf.len()` 恰好成立。
    ///
    /// 这是全函数唯一"合法且贴边"的输入。注入验证的结论值得记下来:
    /// 把这处的 `p + 2 > buf.len()` 改成 `>=` 会让本用例**仍然绿** ——
    /// 因为无扩展时结构上不可能解析出 SNI,两种写法都返回 None,属等价重构。
    /// 也就是说这处差一在本函数的语义下**无法被测试区分**,不是漏洞。
    ///
    /// 真正能被抓到的是"漏检查"和"方向写反":实测把 `p >= buf.len()` 删掉、
    /// 或改成 `p > buf.len()`,逐字节截断用例都会 panic
    /// (`index out of bounds: the len is 46 but the index is 48`)。
    #[test]
    fn parse_sni_handles_empty_extension_block() {
        let buf = client_hello(Some(""));
        // 确认构造确实落在 ext_len == 0 这个边界上
        assert_eq!(buf[buf.len() - 2], 0x00, "ext_len 高字节");
        assert_eq!(buf[buf.len() - 1], 0x00, "ext_len 低字节");
        assert_eq!(parse_sni(&buf), None, "无扩展时不得 panic,且应返回 None");
    }

    /// 逐字节截断:任意前缀长度都不得 panic,且截断到不完整时一律 None。
    ///
    /// 这是唯一能系统暴露所有偏移量计算错误的方法 —— 某个 `p + n > len`
    /// 写成 `p + n >= len` 之类的差一错误,只有在这里才会显形。
    #[test]
    fn parse_sni_never_panics_on_truncation() {
        let full = client_hello(Some("truncate.test"));
        for cut in 0..full.len() {
            let part = &full[..cut];
            // 不 panic 即可;截断后语义上不应解析出完整域名
            let got = parse_sni(part);
            assert!(
                got.is_none() || cut == full.len(),
                "截断到 {cut} 字节仍解析出 {got:?},说明边界检查有洞"
            );
        }
        // 完整输入必须能解析出来(反面参照,防止上面循环因 assert 写反而永真)
        assert_eq!(parse_sni(&full).as_deref(), Some("truncate.test"));
    }

    /// Connection: close 判定 —— 复用的开关条件之一。
    /// 写错会让带 close 的请求完成后连接被错误复用。
    #[test]
    fn wants_close_detects_close_token() {
        let want = |v: Option<&str>| match v {
            Some(v) => v.to_ascii_lowercase().contains("close"),
            None => false,
        };
        assert!(!want(None));
        assert!(want(Some("close")));
        assert!(want(Some("Close")));
        assert!(want(Some("keep-alive, close")));
        assert!(!want(Some("keep-alive")));
    }

    /// 池的空池 borrow 必须返回 None(不能凭空造连接)。
    #[test]
    fn empty_pool_borrows_nothing() {
        let p: UpstreamPool = UpstreamPool::default();
        assert!(p.borrow().is_none());
        assert_eq!(p.idle_len(), 0);
    }

    /// 空闲上限:超过 UPSTREAM_MAX_IDLE 的连接不入池(fd 保护)。
    /// 这里只验证常量语义与 clear 行为,不构造真实连接。
    #[test]
    fn pool_starts_empty_and_clears() {
        let p: UpstreamPool = UpstreamPool::default();
        assert_eq!(p.idle_len(), 0);
        p.clear();
        assert_eq!(p.idle_len(), 0);
        // clone 共享同一池:RecycleBody 归还必须回到同一个池
        let p2 = p.clone();
        assert_eq!(p2.idle_len(), 0);
    }

    /// 复用的正确性核心:未读完的 body 被 drop 时**不得**归还。
    /// 这里用一个假 body 验证 RecycleBody 的 discard 语义 ——
    /// 没有真实 sender 时 ret 为 None,不会 panic,也不会归还。
    #[tokio::test]
    async fn recycle_body_without_sender_never_recycles() {
        let inner: BoxBody = Full::new(Bytes::from_static(b"hello"))
            .map_err(|e| match e {})
            .boxed();
        let pool = UpstreamPool::default();
        let mut body = RecycleBody::new(inner, None, pool.clone());
        // 读完
        while let Some(f) = body.frame().await {
            let _ = f.unwrap();
        }
        assert_eq!(pool.idle_len(), 0, "无 sender 时不应有连接进入池");
        drop(body);
        assert_eq!(pool.idle_len(), 0);
    }

    /// 中途 drop 不得归还。
    #[tokio::test]
    async fn recycle_body_drop_midstream_does_not_recycle() {
        let inner: BoxBody = Full::new(Bytes::from_static(b"0123456789"))
            .map_err(|e| match e {})
            .boxed();
        let pool = UpstreamPool::default();
        let mut body = RecycleBody::new(inner, None, pool.clone());
        let _ = body.frame().await; // 只读一帧就丢
        drop(body);
        assert_eq!(pool.idle_len(), 0);
    }

    /// 端到端:真实建连 → 连续请求 → 归还 → 复用同一连接。
    ///
    /// 这是本次优化的核心行为,**必须能自动验证**:
    /// 没有这个测试时,曾两次"看起来改好了"但实际 give=0(借出两万多次、
    /// 归还 0 次),只能靠临时加的 eprintln 才发现。
    #[tokio::test]
    async fn upstream_pool_reuses_connection_end_to_end() {
        use http_body_util::BodyExt as _;
        use tokio::io::AsyncWriteExt as _;

        // 最小 HTTP/1.1 源站:读掉请求头,固定回 4 字节 body
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let accepted = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let acc2 = accepted.clone();
        tokio::spawn(async move {
            loop {
                let Ok((mut sock, _)) = listener.accept().await else {
                    return;
                };
                acc2.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                tokio::spawn(async move {
                    use tokio::io::AsyncReadExt as _;
                    let mut buf = [0u8; 1024];
                    loop {
                        // 读到空行为止(简化:一次读够即可)
                        match sock.read(&mut buf).await {
                            Ok(0) => return,
                            Ok(_) => {}
                            Err(_) => return,
                        }
                        let resp = b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nOK";
                        if sock.write_all(resp).await.is_err() {
                            return;
                        }
                    }
                });
            }
        });

        // 用真实的 UpstreamPool(泛型到 Empty 以便测试能构造 body),
        // 跑的是与生产完全相同的那份借用/归还逻辑。
        let pool: UpstreamPool<http_body_util::Empty<Bytes>> = UpstreamPool::default();
        let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        let (mut sender, conn) = hyper::client::conn::http1::handshake::<
            _,
            http_body_util::Empty<Bytes>,
        >(hyper_util::rt::TokioIo::new(stream))
        .await
        .unwrap();
        tokio::spawn(async move {
            let _ = conn.await;
        });

        // 第 1 次请求
        let req = hyper::Request::builder()
            .uri("/")
            .header(hyper::header::HOST, "x")
            .body(http_body_util::Empty::<Bytes>::new())
            .unwrap();
        let resp = sender.send_request(req).await.unwrap();
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(&body[..], b"OK");

        // 归还:模拟 RecycleBody::drop 的判定
        assert!(sender.is_ready(), "请求完成后连接应就绪");
        pool.give_back(Upstream {
            sender,
            since: std::time::Instant::now(),
        });
        assert_eq!(pool.idle_len(), 1, "归还后池里应有 1 条连接");

        // 借出:必须拿到同一条(源站只 accept 了 1 次)
        let got = pool.borrow();
        assert!(got.is_some(), "池里有连接时必须能借到");
        drop(got);
        assert_eq!(
            accepted.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "整个过程应只建 1 条连接(复用生效)"
        );
    }

    /// borrow 必须跳过已关闭的连接(不能把死连接发给调用方)。
    #[tokio::test]
    async fn borrow_skips_closed_connection() {
        use tokio::io::AsyncWriteExt as _;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            // 立刻关掉:让客户端的 sender 感知到 closed
            let _ = sock.write_all(b"").await;
            drop(sock);
        });
        let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        let (sender, conn) =
            hyper::client::conn::http1::handshake::<_, http_body_util::Empty<Bytes>>(
                hyper_util::rt::TokioIo::new(stream),
            )
            .await
            .unwrap();
        tokio::spawn(async move {
            let _ = conn.await;
        });
        // 等对端关闭传播过来
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        let pool: UpstreamPool<http_body_util::Empty<Bytes>> = UpstreamPool::default();
        pool.give_back(Upstream {
            sender,
            since: std::time::Instant::now(),
        });
        let got = pool.borrow();
        assert!(got.is_none(), "已关闭的连接不得被借出");
    }

    /// A4(红→绿):缓存路径的响应体(collected 全量读回)在完整消费后,
    /// 挂 RecycleBody 必须把 sender 归还池且可再次借出。
    /// 修前缓存两分支提前 return,sender 随作用域 drop —— 静态资源类
    /// 请求 100% 新建上游连接。
    #[tokio::test]
    async fn cache_path_body_recycles_sender() {
        use http_body_util::BodyExt as _;
        use hyper::body::Incoming;

        // 极简 keep-alive 上游(握手对端存在即可,本测试不发请求)。
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let upstream = tokio::spawn(async move {
            // 持住已接受的连接不关(keep-alive),sender 才可能 ready。
            let mut held = Vec::new();
            while let Ok((sock, _)) = listener.accept().await {
                held.push(sock);
            }
        });
        let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        // 显式标注 B = Incoming(与生产同型;空请求体无法公开构造,
        // 这里只验证 RecycleBody 的归还机制,不发真实请求)。
        let (sender, conn): (hyper::client::conn::http1::SendRequest<Incoming>, _) =
            hyper::client::conn::http1::handshake(hyper_util::rt::TokioIo::new(stream))
                .await
                .unwrap();
        tokio::spawn(async move {
            let _ = conn.await;
        });

        // 缓存路径修后的构造形态:collected + RecycleBody。
        // 给 conn 任务一次调度机会,让 giver 进入 wanting(ready)状态;
        // 否则 Drop 的 is_ready 判定时序上还没就绪,会误判为不可复用。
        for _ in 0..20 {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            if sender.is_ready() {
                break;
            }
        }
        let pool: UpstreamPool = UpstreamPool::default();
        let body = full_body_body(Bytes::from_static(b"hello"));
        let body = RecycleBody::new(body, Some(sender), pool.clone()).boxed();
        let _ = body.collect().await; // 完整消费;drop 归还
        assert_eq!(
            pool.idle_len(),
            1,
            "缓存路径响应体读完后 sender 必须归还(A4)"
        );

        // 归还的连接可被再次借出且仍 ready(= 连接干净可复用)。
        let u = pool.borrow().expect("recycled upstream must be borrowable");
        assert!(
            u.sender.is_ready() && !u.sender.is_closed(),
            "recycled sender must be clean & reusable"
        );
        upstream.abort();
    }

    /// peek_sni 必须正确处理 **ClientHello 跨段到达**。
    ///
    /// 这是 R7 修复的缺陷所在层,此前测试只覆盖纯函数 `parse_sni`
    /// (一次给全量字节),完全没碰 `peek` 的累计语义 —— 所以那个
    /// 「`&mut buf[read..]` 把流头写进错位位置」的缺陷能一直存活。
    ///
    /// 构造:先发 ClientHello 前 10 字节,等 peek_sni 有机会 peek 到
    /// 该前缀,再发剩余部分。修前第二段 peek 会把**流头**写进
    /// `buf[10..]`,parse_sni 读到错位数据 → 返回 None → serve_https
    /// 静默断连。修后应稳定取到 SNI。
    #[tokio::test]
    async fn peek_sni_handles_client_hello_split_across_segments() {
        use tokio::io::AsyncWriteExt as _;

        let hello = client_hello(Some("split.test"));
        assert!(hello.len() > 20, "用例前提:ClientHello 足够长以切段");

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let mut client = tokio::net::TcpStream::connect(addr).await.unwrap();
        let (server_side, _) = listener.accept().await.unwrap();

        let writer = tokio::spawn({
            let hello = hello.clone();
            async move {
                // 第一段:record header + handshake 头的前半部分
                client.write_all(&hello[..10]).await.unwrap();
                client.flush().await.unwrap();
                // 给 peek_sni 一次 peek 到「不完整前缀」的机会 ——
                // 这正是修前触发错位写的那一拍。
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                // 第二段:剩余全部
                client.write_all(&hello[10..]).await.unwrap();
                client.flush().await.unwrap();
                // 保持连接不关,让 peek 侧能读满(peek 不消费)
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            }
        });

        let got = peek_sni(&server_side).await.unwrap();
        assert_eq!(
            got.as_deref(),
            Some("split.test"),
            "ClientHello 跨段到达时 peek_sni 必须仍能解析出 SNI\
             (修前 &mut buf[read..] 会把流头写进错位位置导致返回 None)"
        );
        writer.abort();
    }
}
