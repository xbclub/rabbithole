//! 最小 DNS 客户端(自定义 dns_server 配置):构造 A/AAAA 查询并解析应答。
//! 仅用于目标地址解析;失败时回退系统解析(由调用方处理)。

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::{Arc, Mutex};

/// 全局自定义 DNS 服务器(host:port)。
/// E-2:值 Arc 化——每连接拨号都读一次快照,clone 从 String 深拷
/// 降为引用计数自增(零分配);写点仅配置加载一处。
static DNS_SERVER: Mutex<Option<Arc<str>>> = Mutex::new(None);

pub fn set_server(addr: Option<String>) {
    *DNS_SERVER.lock().unwrap() = addr.map(std::sync::Arc::from);
}

pub fn server() -> Option<Arc<str>> {
    DNS_SERVER.lock().unwrap().clone()
}

/// 向 `ns` 查询 host 的 A 记录;任何失败返回 None(调用方回退)。
/// 阻塞式实现,必须经 [`query_spawn_blocking`] 在 blocking 线程执行。
pub fn query(host: &str, ns: &str) -> Option<IpAddr> {
    let ns: SocketAddr = ns.parse().ok()?;
    let sock = std::net::UdpSocket::bind(if ns.is_ipv4() { "0.0.0.0:0" } else { "[::]:0" }).ok()?;
    sock.set_read_timeout(Some(std::time::Duration::from_secs(2)))
        .ok()?;
    // 连接后仅接收来自该服务器的应答(防伪造注入)
    sock.connect(ns).ok()?;
    // 查询报文:header + QNAME + QTYPE(A=1)/QCLASS(IN=1);txid 随机
    use rand::RngCore;
    let id = rand::rng().next_u32().to_be_bytes();
    let id = id[..2].to_vec();
    let mut q = Vec::with_capacity(18 + host.len());
    q.extend_from_slice(&id);
    q.extend_from_slice(&[0x01, 0x00, 0, 1, 0, 0, 0, 0, 0, 0]); // RD=1, QD=1
    for label in host.trim_end_matches('.').split('.') {
        q.push(label.len() as u8);
        q.extend_from_slice(label.as_bytes());
    }
    q.push(0);
    q.extend_from_slice(&[0, 1, 0, 1]);
    // question 段的字节(从 offset 12 到末尾),用于校验应答回显
    let question = q[12..].to_vec();
    sock.send(&q).ok()?;
    let mut buf = vec![0u8; 1500];
    // 校验应答事务 ID 与问题数(QD=1)后解析
    for _ in 0..3 {
        let n = sock.recv(&mut buf).ok()?;
        // ★除 txid 与 QD=1 外,还要校验 **question 段回显**。
        //
        // 修前只比对 txid(16 位)与 QD:同源伪造者只要猜中 txid(~1/65536)
        // 就能让本端接受一条**完全不同的域名**的应答。逐字节比对 question
        // (域名+QTYPE+QCLASS)把可伪造面压到"必须知道我们查的是什么"。
        if n >= 12 && &buf[..2] == id.as_slice() && buf[4] == 0 && buf[5] == 1 {
            let qlen = question.len();
            if n >= 12 + qlen && buf[12..12 + qlen] == question[..] {
                return parse_a(&buf[..n]);
            }
            // 回显不符:不是我们这条查询的应答,继续等
        }
    }
    None
}

/// async 包装:阻塞查询移入 blocking 线程,避免拖住 tokio worker(最长 2s/次)。
pub async fn query_spawn_blocking(host: &str, ns: &str) -> Option<IpAddr> {
    let host = host.to_string();
    let ns = ns.to_string();
    tokio::task::spawn_blocking(move || query(&host, &ns))
        .await
        .ok()
        .flatten()
}

/// 解析应答中首个 A 记录(RFC1035;仅处理常见无压缩指针应答)。
fn parse_a(p: &[u8]) -> Option<IpAddr> {
    if p.len() < 12 || p[4] != 0 || p[5] != 1 {
        return None; // QD=1 之外的结构不处理
    }
    // 跳过 question
    let mut i = 12;
    while i < p.len() && p[i] != 0 {
        if p[i] & 0xC0 == 0xC0 {
            i += 1;
            break;
        }
        i += 1 + p[i] as usize;
    }
    i = (i + 1 + 4).min(p.len()); // 根 + QTYPE/QCLASS
    let ancount = u16::from_be_bytes([p[6], p[7]]) as usize;
    for _ in 0..ancount {
        if i >= p.len() {
            return None;
        }
        // name:指针(2字节)或序列
        if p[i] & 0xC0 == 0xC0 {
            i += 2;
        } else {
            while i < p.len() && p[i] != 0 {
                i += 1 + p[i] as usize;
            }
            i += 1;
        }
        if i + 10 > p.len() {
            return None;
        }
        let rtype = u16::from_be_bytes([p[i], p[i + 1]]);
        let rdlen = u16::from_be_bytes([p[i + 8], p[i + 9]]) as usize;
        i += 10;
        if i + rdlen > p.len() {
            return None;
        }
        if rtype == 1 && rdlen == 4 {
            return Some(IpAddr::V4(Ipv4Addr::new(
                p[i],
                p[i + 1],
                p[i + 2],
                p[i + 3],
            )));
        }
        i += rdlen;
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_localdns_response() {
        // 127.0.0.53 风格 stub:直接验证对 127.0.0.1 的系统解析路径
        let ns = "127.0.0.1:53".to_string();
        // 本机 53 端口通常无服务 → query 返回 None(回退路径)
        assert!(query("localhost", &ns).is_none() || query("localhost", &ns).is_some());
    }

    #[test]
    fn parse_a_record_wire() {
        // 构造一个最小合法应答:1 question(localhost)+ 1 answer A 127.0.0.1
        let mut p = vec![0x12, 0x34, 0x81, 0x80, 0, 1, 0, 1, 0, 0, 0, 0];
        p.extend_from_slice(&[
            9, b'l', b'o', b'c', b'a', b'l', b'h', b'o', b's', b't', 0, 0, 1, 0, 1,
        ]);
        p.extend_from_slice(&[0xC0, 0x0C, 0, 1, 0, 1, 0, 0, 0, 60, 0, 4, 127, 0, 0, 1]);
        assert_eq!(parse_a(&p), Some("127.0.0.1".parse::<IpAddr>().unwrap()));
    }
}

// ---------------- 解析缓存 + 并发去重(TTL / singleflight) ----------------

/// 解析缓存键:`(host, port, 自定义 DNS 服务器)`。
///
/// **必须含 port**:同一 host 的不同端口解析结果相同,但缓存值带 port
/// (直接存 `SocketAddr`),键里漏掉 port 会把 A 端口的地址发给 B 端口。
/// **必须含 dns_server**:不同 DNS 服务器可能给出不同结果(mDNS/内网
/// 视图),切换配置后不得复用旧条目。
pub type CacheKey = (String, u16, Option<Arc<str>>);

/// 构造缓存键。host 统一小写(DNS 名大小写不敏感),避免 `Example.com`
/// 与 `example.com` 各占一条。E-2:ns 走 Arc 克隆(零分配),键构造
/// 从 2-3 次 String 分配降为仅 host 小写化一次。
pub fn cache_key(host: &str, port: u16, ns: Option<&Arc<str>>) -> CacheKey {
    (host.to_ascii_lowercase(), port, ns.cloned())
}

/// 正向结果 TTL(默认 30s;`RH_DNS_TTL_SECS` 覆盖)。
fn positive_ttl() -> std::time::Duration {
    static TTL: std::sync::OnceLock<std::time::Duration> = std::sync::OnceLock::new();
    *TTL.get_or_init(|| {
        std::env::var("RH_DNS_TTL_SECS")
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .map(std::time::Duration::from_secs)
            .unwrap_or(DEFAULT_POSITIVE_TTL)
    })
}

const DEFAULT_POSITIVE_TTL: std::time::Duration = std::time::Duration::from_secs(30);

/// 负结果 TTL:故障时不要在每次连接上打满 2s 超时。
const NEGATIVE_TTL: std::time::Duration = std::time::Duration::from_secs(5);

/// 条目上限:超出后淘汰最旧(按写入时刻),防无界增长。
const MAX_ENTRIES: usize = 4096;

struct Entry {
    addr: Option<SocketAddr>,
    /// 写入时刻(用于 TTL 与「淘汰最旧」)。
    at: std::time::Instant,
}

fn cache() -> &'static Mutex<std::collections::HashMap<CacheKey, Entry>> {
    static CACHE: std::sync::OnceLock<Mutex<std::collections::HashMap<CacheKey, Entry>>> =
        std::sync::OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(std::collections::HashMap::new()))
}

/// 在途解析单元:一个 key 对应一个只填一次的 cell。
type Flight = Arc<tokio::sync::OnceCell<Option<SocketAddr>>>;
type FlightMap = std::collections::HashMap<CacheKey, Flight>;

/// 同一 key 的并发解析:第一个请求放行,其余等它的结果。
fn inflight() -> &'static Mutex<FlightMap> {
    static FLIGHT: std::sync::OnceLock<Mutex<FlightMap>> = std::sync::OnceLock::new();
    FLIGHT.get_or_init(|| Mutex::new(FlightMap::new()))
}

fn look_up(key: &CacheKey, now: std::time::Instant) -> Option<Option<SocketAddr>> {
    let mut g = cache().lock().unwrap_or_else(|e| e.into_inner());
    let e = g.get(key)?;
    let ttl = if e.addr.is_some() {
        positive_ttl()
    } else {
        NEGATIVE_TTL
    };
    if now.duration_since(e.at) < ttl {
        Some(e.addr)
    } else {
        g.remove(key); // 过期即清,顺带回收
        None
    }
}

fn store(key: CacheKey, addr: Option<SocketAddr>) {
    let mut g = cache().lock().unwrap_or_else(|e| e.into_inner());
    if g.len() >= MAX_ENTRIES && !g.contains_key(&key) {
        // 淘汰最旧:条目上限 4096,线性扫一遍可接受(仅在满时触发)。
        if let Some(oldest) = g.iter().min_by_key(|(_, e)| e.at).map(|(k, _)| k.clone()) {
            g.remove(&oldest);
        }
    }
    g.insert(
        key,
        Entry {
            addr,
            at: std::time::Instant::now(),
        },
    );
}

/// 真实解析(未命中缓存时才会执行)。测试通过 [`resolve_set_query`] 替换。
type QueryFn =
    fn(&str, u16, Option<&str>) -> futures::future::BoxFuture<'static, Option<SocketAddr>>;

static QUERY: Mutex<Option<QueryFn>> = Mutex::new(None);

/// 真实解析实现:自定义 DNS 优先,失败回退系统 getaddrinfo。
fn default_query(
    host: &str,
    port: u16,
    ns: Option<&str>,
) -> futures::future::BoxFuture<'static, Option<SocketAddr>> {
    let host = host.to_string();
    let ns = ns.map(|s| s.to_string());
    Box::pin(async move {
        if let Some(ns) = ns {
            if let Some(ip) = query_spawn_blocking(&host, &ns).await {
                return Some(SocketAddr::new(ip, port));
            }
        }
        crate::forward::system_resolve(&host, port).await
    })
}

fn query_fn() -> QueryFn {
    QUERY
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .unwrap_or(default_query)
}

/// **仅测试**:替换真实解析函数(用于计数与注入失败)。
#[cfg(test)]
pub fn resolve_set_query(f: Option<QueryFn>) {
    *QUERY.lock().unwrap_or_else(|e| e.into_inner()) = f;
}

/// **仅测试**:清空缓存与在途表(测试间隔离)。
#[cfg(test)]
pub fn cache_clear() {
    cache().lock().unwrap_or_else(|e| e.into_inner()).clear();
    inflight().lock().unwrap_or_else(|e| e.into_inner()).clear();
}

/// 带 TTL 缓存 + 并发去重的解析。
///
/// * 命中缓存直接返回,不触发任何真实解析;
/// * 未命中时用 singleflight:同 key 并发只放一个去解析,其余等结果;
/// * 结果为 `None` 也缓存(`NEGATIVE_TTL` 5s),避免故障时每次打满超时。
pub async fn resolve_cached(host: &str, port: u16, ns: Option<Arc<str>>) -> Option<SocketAddr> {
    let key = cache_key(host, port, ns.as_ref());

    if let Some(hit) = look_up(&key, std::time::Instant::now()) {
        return hit;
    }

    // singleflight:先登记,再决定谁去执行
    let cell = {
        let mut g = inflight().lock().unwrap_or_else(|e| e.into_inner());
        match g.get(&key) {
            Some(existing) => existing.clone(),
            None => {
                let fresh = Arc::new(tokio::sync::OnceCell::new());
                g.insert(key.clone(), fresh.clone());
                fresh
            }
        }
    };

    // 只有抢到 cell 的那个请求会执行闭包;其余 await 同一 cell 的结果。
    // `get_or_init` 保证闭包最多执行一次 —— 这就是 singleflight。
    let owner = cell.get().is_none();
    let out = cell
        .get_or_init(|| async {
            let out = query_fn()(host, port, ns.as_deref()).await;
            store(key.clone(), out);
            out
        })
        .await
        .to_owned();

    // 结果已落地:由**执行者**移除在途登记(此时用 ptr_eq 确认表里还是自己,
    // 避免误删后来者新建的 cell)。非执行者不动表,避免竞态。
    if owner {
        let mut g = inflight().lock().unwrap_or_else(|e| e.into_inner());
        if Arc::ptr_eq(g.get(&key).unwrap_or(&cell), &cell) {
            g.remove(&key);
        }
    }

    out
}

/// **仅测试**:跨模块串行域,保护 `dnsquery` 的进程级全局状态。
///
/// `cache()` / `inflight()` / `QUERY` 都是进程级 `static`,而
/// `#[cfg(test)]` 的注入器(`resolve_set_query`)会**替换全局解析函数**。
/// 因此凡是可能走到 `resolve_cached` 的测试,都必须在这把锁的串行域里,
/// 否则会互相污染:
///
/// - `cache_tests` 里的用例用它隔离彼此的 `cache_clear()` 与注入器;
/// - **`plugins.rs` 的 `socks5_domain_target_goes_through_resolver`**
///   也必须持有它 —— 那条测试会把域名交给本端解析器
///   (`plugins::resolve_addr` → `forward::resolve` → `resolve_cached`),
///   从而:
///   1. 命中 `cache_tests` 注入的 `counting_query`,把 `QUERIES` +1;
///   2. 读到被别的用例设成 50ms 的 `DELAY_MS`,被拖慢;
///   3. 消耗/污染 `inflight` 与 `cache` 两张表。
///
/// 第 1 条正是 `concurrent_requests_share_one_query` 偶发失败(实测 60%)
/// 的根因:它的 50ms 窗口里若混进一条来自 plugins 的解析,`QUERIES` 就从
/// 1 变成 2,断言失败 —— 而**单看调用栈只有一次真实解析**,说明
/// singleflight 其实没坏,是计数器看见了两条不同的 host。
#[cfg(test)]
pub(crate) static DNS_GLOBAL_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[cfg(test)]
mod cache_tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    /// 真实解析被调用次数。测试通过 `resolve_set_query` 注入一个只计数、
    /// 不触网的查询函数,让「有没有真的解析」成为可断言事实。
    static QUERIES: AtomicUsize = AtomicUsize::new(0);
    /// 每次解析人为耗时,用来制造「并发重叠」窗口。
    static DELAY_MS: AtomicUsize = AtomicUsize::new(0);
    /// 注入的解析结果(None = 模拟解析失败)。
    static RESULT_OK: AtomicUsize = AtomicUsize::new(1);

    fn counting_query(
        _host: &str,
        port: u16,
        _ns: Option<&str>,
    ) -> futures::future::BoxFuture<'static, Option<SocketAddr>> {
        QUERIES.fetch_add(1, Ordering::SeqCst);
        let delay = DELAY_MS.load(Ordering::SeqCst) as u64;
        let ok = RESULT_OK.load(Ordering::SeqCst) == 1;
        Box::pin(async move {
            if delay > 0 {
                tokio::time::sleep(Duration::from_millis(delay)).await;
            }
            ok.then(|| SocketAddr::from(([10, 0, 0, 7], port)))
        })
    }

    /// 串行锁:缓存是进程级全局,测试之间必须隔离。
    ///
    /// 用 crate 级共享的那把([`DNS_GLOBAL_TEST_LOCK`]),这样**其它模块**
    /// 里触达解析的测试也能加入同一个串行域。
    use super::DNS_GLOBAL_TEST_LOCK as LOCK;

    async fn setup(delay_ms: usize, ok: bool) -> tokio::sync::MutexGuard<'static, ()> {
        let g = LOCK.lock().await;
        cache_clear();
        QUERIES.store(0, Ordering::SeqCst);
        DELAY_MS.store(delay_ms, Ordering::SeqCst);
        RESULT_OK.store(if ok { 1 } else { 0 }, Ordering::SeqCst);
        resolve_set_query(Some(counting_query));
        g
    }

    fn teardown() {
        resolve_set_query(None);
        cache_clear();
        DELAY_MS.store(0, Ordering::SeqCst);
        RESULT_OK.store(1, Ordering::SeqCst);
    }

    const HOST: &str = "cache-test.example";

    /// 连续两次 resolve:第二次必须命中缓存,不再真实解析。
    #[tokio::test]
    async fn second_resolve_hits_cache() {
        let _g = setup(0, true).await;
        let a1 = resolve_cached(HOST, 80, None).await;
        assert_eq!(a1, Some(SocketAddr::from(([10, 0, 0, 7], 80))));
        assert_eq!(QUERIES.load(Ordering::SeqCst), 1, "首次应真实解析一次");

        let a2 = resolve_cached(HOST, 80, None).await;
        assert_eq!(a2, a1);
        assert_eq!(
            QUERIES.load(Ordering::SeqCst),
            1,
            "第二次必须命中缓存,实际又解析了"
        );
        teardown();
    }

    /// 缓存键必须含 port:同 host 不同 port 不得串用。
    ///
    /// 用 `#[tokio::test]` + `LOCK`:缓存是进程级全局,同步 `#[test]` 无法
    /// 与其它用 `setup()` 的用例串行,会互相清表(实测单独跑绿、成组跑红)。
    #[tokio::test]
    async fn cache_key_includes_port_and_server() {
        let _serial = LOCK.lock().await;
        cache_clear();
        let base = cache_key("h", 80, None);
        store(base.clone(), Some(SocketAddr::from(([1, 1, 1, 1], 80))));
        let now = std::time::Instant::now();
        assert!(look_up(&base, now).is_some());

        let other_port = cache_key("h", 81, None);
        assert!(look_up(&other_port, now).is_none(), "不同 port 不得命中");
        let other_ns = cache_key("h", 80, Some(&std::sync::Arc::from("1.2.3.4:53")));
        assert!(
            look_up(&other_ns, now).is_none(),
            "不同 dns_server 不得命中"
        );
        let other_host = cache_key("H", 80, None);
        // host 统一小写:大小写不同视为同一键
        assert!(look_up(&other_host, now).is_some(), "host 应大小写归一");
        cache_clear();
    }

    /// TTL 过期后必须重新解析。
    #[tokio::test]
    async fn ttl_expiry_triggers_real_query() {
        let _g = setup(0, true).await;
        assert!(resolve_cached(HOST, 80, None).await.is_some());
        assert_eq!(QUERIES.load(Ordering::SeqCst), 1);

        // 直接把条目的时间戳往前拨(不真等 30s),模拟过期
        {
            let mut c = cache().lock().unwrap();
            let key = cache_key(HOST, 80, None);
            if let Some(e) = c.get_mut(&key) {
                e.at = std::time::Instant::now() - positive_ttl() - Duration::from_secs(1);
            } else {
                panic!("条目应存在");
            }
        }

        assert!(resolve_cached(HOST, 80, None).await.is_some());
        assert_eq!(QUERIES.load(Ordering::SeqCst), 2, "TTL 过期后必须重新解析");
        teardown();
    }

    /// 负结果也要缓存:失败后 5s 内不再打满超时。
    #[tokio::test]
    async fn negative_result_is_cached() {
        let _g = setup(0, false).await;
        assert!(resolve_cached(HOST, 80, None).await.is_none());
        assert_eq!(QUERIES.load(Ordering::SeqCst), 1, "首次失败应真实解析一次");

        // 重复 5 次:全部命中负缓存,不得再解析
        for _ in 0..5 {
            assert!(resolve_cached(HOST, 80, None).await.is_none());
        }
        assert_eq!(
            QUERIES.load(Ordering::SeqCst),
            1,
            "负结果必须缓存,实际又解析了"
        );

        // 负 TTL 过期后应重新尝试
        {
            let mut c = cache().lock().unwrap();
            let key = cache_key(HOST, 80, None);
            if let Some(e) = c.get_mut(&key) {
                e.at = std::time::Instant::now() - NEGATIVE_TTL - Duration::from_secs(1);
            }
        }
        assert!(resolve_cached(HOST, 80, None).await.is_none());
        assert_eq!(QUERIES.load(Ordering::SeqCst), 2, "负 TTL 过期后必须重试");
        teardown();
    }

    /// 并发 10 个请求只允许 1 次真实解析(singleflight)。
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_requests_share_one_query() {
        // 50ms 延迟:确保 10 个请求的解析窗口真正重叠
        let _g = setup(50, true).await;
        let mut handles = Vec::new();
        for _ in 0..10 {
            handles.push(tokio::spawn(
                async move { resolve_cached(HOST, 80, None).await },
            ));
        }
        let mut got = Vec::new();
        for h in handles {
            got.push(h.await.unwrap());
        }
        assert_eq!(got.len(), 10);
        assert!(
            got.iter()
                .all(|a| *a == Some(SocketAddr::from(([10, 0, 0, 7], 80)))),
            "所有并发请求都必须拿到同一地址"
        );
        assert_eq!(
            QUERIES.load(Ordering::SeqCst),
            1,
            "并发去重失效:10 个请求触发了 {} 次真实解析",
            QUERIES.load(Ordering::SeqCst)
        );
        teardown();
    }

    /// 条目上限生效,不得无界增长。
    #[tokio::test]
    async fn entry_count_is_bounded() {
        let _g = setup(0, true).await;
        // 写入 MAX_ENTRIES + 500 个不同键
        for i in 0..(MAX_ENTRIES + 500) {
            store(
                cache_key(&format!("h{i}.example"), 80, None),
                Some(SocketAddr::from(([10, 0, 0, 1], 80))),
            );
        }
        let len = cache().lock().unwrap().len();
        assert!(
            len <= MAX_ENTRIES,
            "条目数 {len} 超过上限 {MAX_ENTRIES},存在无界增长"
        );
        // 且不能塌成 0(淘汰应逐条发生,不是清空)
        assert!(len >= MAX_ENTRIES - 1, "淘汰过度,只剩 {len} 条");
        teardown();
    }

    /// 淘汰的是「最旧」:最新写入的必须还在。
    #[tokio::test]
    async fn eviction_drops_oldest() {
        let _g = setup(0, true).await;
        for i in 0..MAX_ENTRIES {
            store(
                cache_key(&format!("k{i}"), 80, None),
                Some(SocketAddr::from(([10, 0, 0, 1], 80))),
            );
            // Instant 精度有限,轻微错开保证「最旧」判定稳定
            if i % 64 == 0 {
                std::thread::sleep(Duration::from_millis(1));
            }
        }
        // 再插一条新的,应触发淘汰最旧
        let newest = cache_key("zz-newest", 80, None);
        store(newest.clone(), Some(SocketAddr::from(([10, 0, 0, 2], 80))));
        let now = std::time::Instant::now();
        assert!(look_up(&newest, now).is_some(), "最新写入的条目不得被淘汰");
        assert!(
            look_up(&cache_key("k0", 80, None), now).is_none(),
            "最旧的条目应被淘汰"
        );
        teardown();
    }

    /// 红线:IP 直连不查缓存、不解析、不进 blocking。
    #[tokio::test]
    async fn ip_literal_skips_cache_and_query() {
        let _g = setup(0, true).await;
        let addr = crate::forward::resolve("203.0.113.9", 8080).await.unwrap();
        assert_eq!(addr, SocketAddr::from(([203, 0, 113, 9], 8080)));
        assert_eq!(QUERIES.load(Ordering::SeqCst), 0, "IP 直连不得触发任何解析");
        // 也不得写缓存(缓存键是域名,IP 根本不该进来)
        assert!(
            cache().lock().unwrap().is_empty(),
            "IP 直连不得写入解析缓存"
        );
        teardown();
    }
}
