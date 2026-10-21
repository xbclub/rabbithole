# rh-client 目标解析:TTL 缓存 + 并发去重(C9)

一句话结论:域名目标的 http vhost 短连接 QPS **3466~3522 → 5212~5295(+51%)**。收益来自把「每请求一次 `spawn_blocking` getaddrinfo」换成 TTL 内直接命中缓存,并发请求再经 singleflight 合并。IP 直连快路径、缓存键(host/port/dns_server)、TTL 与条目上限都有测试锁住。

## 0. 身份

- 仓库 `rabbithole`,起点 HEAD `90f6f6b`(工作树另含前几轮未提交改动)。
- 改动文件:`crates/rh-client/src/dnsquery.rs`(缓存 + singleflight + 测试)、`crates/rh-client/src/forward.rs`(`resolve` 接入缓存,抽出 `system_resolve`)。
- 本机 macOS arm64 / rustc 1.98.1 / release。**非三机基准**。

## 1. 改了什么

### 缓存

```rust
// dnsquery.rs
type CacheKey = (String, u16, Option<String>);  // (host 小写, port, dns_server)
static CACHE: OnceLock<Mutex<HashMap<CacheKey, Entry>>>;
struct Entry { addr: Option<SocketAddr>, at: Instant }
```

| 参数 | 值 |
|---|---|
| 正结果 TTL | 30s(`RH_DNS_TTL_SECS` 覆盖) |
| 负结果 TTL | 5s(故障时不再每次打满 2s 超时) |
| 条目上限 | 4096,超出淘汰「最旧」(按写入时刻) |

**缓存键必须含 port**:缓存值直接是 `SocketAddr`(含 port),键里漏掉 port 会把 A 端口的地址发给 B 端口。
**缓存键必须含 dns_server**:不同 DNS 服务器可能给出不同结果(内网视图),切换配置后不得复用旧条目。

### singleflight

`HashMap<CacheKey, Arc<OnceCell<Option<SocketAddr>>>>`:同 key 并发时,`OnceCell::get_or_init` 保证闭包最多执行一次,其余请求 await 同一 cell 的结果。

清理时机:由**执行者**在结果落地后移除在途登记,并用 `Arc::ptr_eq` 确认表里还是自己,避免误删后来者新建的 cell。

### `resolve()` 接线

```rust
pub async fn resolve(host: &str, port: u16) -> anyhow::Result<SocketAddr> {
    if let Ok(ip) = host.parse::<IpAddr>() {
        return Ok(SocketAddr::new(ip, port));   // 红线:IP 直连不查缓存不解析
    }
    dnsquery::resolve_cached(host, port, session::dns_server()).await.ok_or(...)
}
```

同时把系统解析抽成 `forward::system_resolve(host, port)`,供 `dnsquery::default_query` 复用(避免两处各写一份)。

## 2. 验收测试(10 个)

缓存是进程级全局,所有用例用 `tokio::sync::Mutex` 串行 + `cache_clear()`,并用 `resolve_set_query` 注入一个**只计数、不触网**的查询函数——让「有没有真的解析」成为可断言事实。

| 测试 | 断言 |
|---|---|
| `second_resolve_hits_cache` | 连续两次 resolve,真实解析计数恒为 1 |
| `ttl_expiry_triggers_real_query` | 把条目时间戳前拨越过 TTL,计数变 2 |
| `negative_result_is_cached` | 失败结果重复 5 次只解析 1 次;越过负 TTL 后重试 |
| `concurrent_requests_share_one_query` | **10 个并发只触发 1 次真实解析**(50ms 注入延迟保证窗口重叠) |
| `entry_count_is_bounded` | 写 MAX+500 条后 `len <= 4096`,且不塌成 0 |
| `eviction_drops_oldest` | 满后插入新条目,最新的在、最旧的被淘汰 |
| `cache_key_includes_port_and_server` | 不同 port / 不同 dns_server 不命中;**host 大小写归一** |
| `ip_literal_skips_cache_and_query` | IP 直连:解析计数 0,**缓存表为空** |

### 负向验证(逐个做过)

| 注入的回归 | 结果 |
|---|---|
| 去掉缓存读取分支 | `second_resolve_hits_cache`、`negative_result_is_cached` 报红 |
| 去掉 singleflight(每个请求都自己解析) | `并发去重失效:10 个请求触发了 10 次真实解析` |
| 去掉条目上限 | `条目数 4596 超过上限 4096,存在无界增长` |
| 缓存命中分支改恒 false(TLS 缓存测试) | — |

### 一个被测试抓到的真 bug

`cache_key_includes_port_and_server` 最初报「host 应大小写归一」:小写化原本只写在 `resolve_cached` 里,`store`/`look_up` 直调时不一致。修正为统一的 `cache_key(host, port, ns)` 构造函数,内部做 `to_ascii_lowercase()`。

同一测试还暴露了**并行污染**:它原本是同步 `#[test]`,无法与用 `setup()` 的其他用例串行,单独跑绿、成组跑红。改为 `#[tokio::test]` + 同一把 `LOCK`。

## 3. 吞吐:域名目标 http vhost 短连接 QPS

拓扑:`压测端 → vhost :31911 → rh 服务端 → rh 客户端 → 域名目标(localhost:31902)`。规则 `target_host` 用**域名**,使每请求都要 `resolve`。并发 8,每轮 6s,零错误。

| 版本 | 各轮 QPS | med |
|---|---|---|
| **改前** | 3516.2, 3521.5, 3466.4, 3483.0, 3469.7 | **3483.0** |
| **改后** | 5282.7, 5220.3, 5253.6, 5212.0, 5294.5 | **5253.6** |
| 改后(交叉复测) | 5337.6, 5207.1, 5224.6 | 5224.6 |

**med +50.8%,best +51.8%**。改后两组独立运行一致(5254 / 5225),改前组内极差仅 55 QPS(1.6%),组间无重叠。

> 对照说明:前几轮做过「IP 目标」的 http2http 基准(5500 左右且改前改后无差别),而本轮域名目标改前是 3483 —— 差值(约 2000 QPS)正是每请求那次 getaddrinfo 的代价。这与「缓存命中后回到 IP 直连量级」的预期吻合。

## 4. 基准本身踩过的坑(留给下次)

1. **`localhost` 在 macOS 上优先解析到 `::1`**。源站只绑 `127.0.0.1` 时,域名目标连不上,自检 `http=000`,QPS 读成 0——差点被误判成「缓存无效」。最终源站用 `AF_INET6` socket + `IPV6_V6ONLY=0` 绑 `::`,v4/v6 都通。
2. **`socket()` 类型必须与绑定地址族一致**。用 `AF_INET` 的 socket 去 `bind(("::", port))` 会抛 `OSError: [Errno 22] Invalid argument`,而且这个错误在后台进程里只出现在 stderr,主脚本看不到。
3. **固定 `sleep 3` 等不到服务端就绪**。改为轮询控制端口 `LISTEN` + 进程存活双重判定,并在压测前插一条 `curl` 自检,失败就打印服务端/客户端日志尾部——否则 QPS=0 时无从判断是代码问题还是环境问题。
4. macOS 临时端口仅 16384 个(`ip_local_port_range` 49152–65535)、`msl=15s`:短连接压测**超过 6s 就会 `EADDRNOTAVAIL`**,与代码无关。所有对照都卡在 6s + 轮间 2s。

## 5. 红线核对

| 红线 | 状态 |
|---|---|
| IP 直连不查缓存不解析 | `resolve` 第一行就 `parse::<IpAddr>()` 直返;`ip_literal_skips_cache_and_query` 断言解析计数 0 且缓存表为空 |
| 缓存键含 port | `cache_key(host,port,ns)`;测试断言不同 port 不命中 |
| 缓存键含 dns_server | 同上,不同 ns 不命中 |
| 正 TTL 30s / 负 TTL 5s | 常量定义 + 两个过期测试 |
| 条目上限 4096 | `MAX_ENTRIES`;越界淘汰最旧,有两条测试 |

全量:`cargo test --workspace` 全绿(rh-client 57 → 65);`cargo clippy --workspace --all-targets -- -D warnings` 通过;`tests/e2e.sh` **PASS=82 FAIL=0**。

## 6. 未做的部分(如实)

- **没有改用应答里的 DNS TTL**。当前固定 30s,忽略了 A 记录自带的 TTL 字段。若目标域名做短 TTL 轮转(蓝绿/故障转移),30s 内会拿到旧地址。要做需要改 `parse_a` 同时返回 TTL,并取「应答 TTL 与配置上限的较小值」。
- **没有做后台刷新**。TTL 到期瞬间会有一个请求承担真实解析(其余被 singleflight 合并),会有一次额外延迟。可加「过期前主动刷新」缓解,但会引入后台任务与生命周期管理。
- **负缓存只针对整个解析失败,不区分 NXDOMAIN 与超时**。两者都按 5s 缓存。若目标域名确实不存在,5s 内会快速失败而不是每次等 2s——这是想要的;但如果是瞬时网络抖动,也会被锁 5s。
- **没在三机环境验证**。本机 loopback,结论只用于「改前 vs 改后」的方向判定。
- **`RH_DNS_TTL_SECS` 用 `OnceLock` 只读一次**。进程内改环境变量不会生效,这对生产是正确的(避免运行期抖动),但测试里无法通过改环境变量来测 TTL——所以测试是直接改条目时间戳。
