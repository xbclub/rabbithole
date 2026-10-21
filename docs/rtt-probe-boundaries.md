# RTT 自适应探针的三个边界问题(round-78)

一句话结论:探针从 `TcpStream::connect` 改为**与真实拨号同路径**(含 `proxy_url`)、结果按 `(addr, proxy_url)` 进程内缓存、协议 gate 提到探测之前、注释与实现对齐。四个问题全部修复,每一条都有断言级证据(而非"K 等于几"这种两条路径都成立的弱断言)。

## 0. 身份

- 仓库 `rabbithole`,起点 HEAD `90f6f6b`(工作树另含前几轮未提交改动)。
- 改动文件:`crates/rh-transport/src/lib.rs`(探针/缓存/计数)、`crates/rh-client/src/session.rs`(gate 提前 + 传递 proxy_url + 测试)。
- 本机 macOS arm64 / rustc 1.98.1 / release。

## 1. 四个问题与修法

### ① 探针绕过 `proxy_url`

修前:`probe_rtt` 无条件 `tokio::net::TcpStream::connect(addr)`,而真实拨号在 `tcp::dial_tcp` 里按 `cfg.proxy_url` 走 `proxy_dial::dial_via_proxy`。走代理的部署里探针必然失败 → `auto_carriers` 返回 1 → 自适应静默失效,还白等一次超时。

修后:抽出 `DialFn` 类型 + 可替换的 `DIAL` 静态,默认实现 `default_dial` 与 `dial_tcp` 同语义:

```rust
match proxy_url {
    Some(url) => proxy_dial::dial_via_proxy(&ProxyUrl::parse(&url)?, addr).await,
    None => tokio::net::TcpStream::connect(addr).await,
}
```

`probe_rtt(addr, timeout, proxy_url)` 与 `auto_carriers(addr, timeout, proxy_url)` 都新增了 `proxy_url` 形参,由 `session.rs` 传入 `cfg.proxy_url.clone()`。

### ② 每次重连都探

修前:重连循环(`main.rs:136`)反复调 `connect_and_run` → `initial_k_for_async` → `auto_carriers`,每次都多打一条 TCP(accept 完立刻断),断线抖动时变成周期性小冲击。

修后:进程内缓存 `HashMap<(SocketAddr, Option<String>), usize>`,命中直接返回。**失败结果也缓存**(否则断线时每次重连都白等 2s 超时)。

### ③ 对 quic/kcp 也照探

修前:探测在 `session.rs:111-114` 执行,而 `initial_k` 的 gate(`is_tcp_based() && multiplex`)在**之后**才判;这两类协议恒 1,探测结果直接丢弃。

修后:gate 提到探测之前:

```rust
match cfg.mux_pool_size {
    Some(_) => proto_baseline,                                   // 显式配置:不探
    None if !protocol.is_tcp_based() || !cfg.multiplex => 1,     // gate:不探
    None => { /* 探测 */ rtt_k.max(proto_baseline) }
}
```

> 这里返回 `1` 而不是 `proto_baseline`:调用方的
> `if is_tcp_based() && multiplex { sized_k } else { 1 }` 会把值压成 1,
> 返回基线会让本函数输出与最终结果不一致。这个不一致是测试抓出来的
> (`left: 2, right: 1`)。

### ④ 注释与实现不符

修前 `lib.rs:596` 写"探针失败时退回协议基线",实际 `:605` 是 `None => 1`(单载体;协议基线由调用方的 `.max(proto_baseline)` 另外参与)。

修后:文档改为"退回单载体(不是协议基线 —— 协议基线由调用方的 `default_carriers` 另外参与 `max`)",与实现一致。

## 2. 验收测试

### `rh-transport`(探针本体,7 个)

用 `probe_set_dial` 注入一个**只计数、必返回真实 loopback 连接**的拨号函数,让「有没有真的探测」成为可断言事实。

| 测试 | 断言 |
|---|---|
| `repeated_reconnects_probe_only_once` | 连续 10 次 `auto_carriers` 只探测 **1** 次,且返回值一致 |
| `cache_key_includes_proxy_url` | 同地址不同 `proxy_url` 各自探测一次 |
| `probe_passes_proxy_url_to_dialer` | 拨号函数收到的 `proxy_url` 与传入完全相同 |
| `probe_failure_degrades_to_single_carrier` | 注入失败 → 返回 1;且**失败结果也缓存**(再调不再探测) |
| `probe_result_matches_thresholds` | 1ms→K=1、20ms→K=4,与 `carriers_for_rtt` 一致 |
| `carriers_for_rtt_thresholds`(既有) | 阈值边界逐点(0/1/4.999/5/10/14.999/15/20/200ms)+ 单调性,**未改动** |
| `default_carriers_covers_all_protocols`(既有) | 六协议基线,**未改动** |

### `rh-client`(gate,5 个)

用 `rh_transport::probe_call_count()` 断言「一次都没有探测」。纯功能断言(K==1)在修前也成立,区分不了两条路径。

| 测试 | 断言 |
|---|---|
| `non_tcp_protocol_skips_probe` | quic/kcp 探测计数不增 |
| `non_multiplex_skips_probe` | `multiplex=false` 探测计数不增 |
| `explicit_pool_size_skips_probe` | `mux_pool_size=Some(3)` → K=3 且探测计数不增(红线) |
| `tcp_multiplex_does_probe` | 反面证据:tcp+multiplex 确实探测 1 次 |
| `probe_failure_does_not_block` | 不可达地址下正常返回合法 K,不 panic、不上抛(红线) |

### 负向验证(逐个做过)

| 注入的回归 | 结果 |
|---|---|
| 删掉 gate 分支(gate 退回探测之后) | `non_tcp_protocol_skips_probe`、`non_multiplex_skips_probe` 报红:`quic/kcp 不得触发 RTT 探测` |
| 删掉 RTT 缓存读取 | `repeated_reconnects_probe_only_once`、`cache_key_includes_proxy_url`、`probe_failure_degrades_to_single_carrier` 三个报红 |
| 探针忽略 proxy_url(键与拨号都传 None) | `cache_key_includes_proxy_url`、`probe_passes_proxy_url_to_dialer` 报红 |

## 3. 端到端证据(走代理)

本机起一个**真实 HTTP CONNECT 代理**(记录每次 CONNECT),客户端配 `proxy_url = "http://127.0.0.1:32999"`:

**首次上线:**

```
CONNECT 记录:2 次(1 探针 + 1 真实拨号,K=2 时池复用)
客户端日志:
  pool carriers converged initial_k=2 final_k=2 server_cap=8 failed=0
  pool sizing protocol=tcp protocol_baseline=2 hardware_cap=7
              server_cap=8 initial_k=2 final_k=2 ...
  timeout / refused 计数:0
```

**这是修复 ① 的决定性证据**:`initial_k=2` 说明探针拿到了真实 RTT 并算出 K=2。修前走代理时探针必然超时 → `auto_carriers` 返回 1 → 与基线 2 取 max 后仍是 2,**但日志里会多一条超时**且白等 2s。现在零超时记录。

**同进程重连 10 次**(反复杀服务端、客户端常驻):

```
BASE_CONNECTS=4  AFTER_CONNECTS=14  DELTA=10
```

10 次重连 = 10 次真实拨号,**探针多出 0 次**。若缓存失效,每次重连会多 1 次(共 20 次)。

> 对照:直接杀客户端进程再重启 10 次,`delta=20`(= 10 次 × 每次 1 探针 + 1 拨号)。这符合预期——缓存是进程内的 `OnceLock`,进程重启自然清空。需求说的"连续重连"指**同进程重连循环**,已在上面验证。

## 4. 红线核对

| 红线 | 状态 |
|---|---|
| 显式 `mux_pool_size` / `RH_MUX_POOL_SIZE` 最高优先且不探测 | `Some(_) => proto_baseline` 在第一支;`explicit_pool_size_skips_probe` 断言 K=3 且探测计数不增 |
| 探针失败不得阻断连接 | `auto_carriers` 失败返回 1,`initial_k_for_async` 不上抛;`probe_failure_does_not_block` 覆盖 |

全量:`cargo test --workspace` 全绿(rh-transport 163→170,rh-client 65→70);`cargo clippy --workspace --all-targets -- -D warnings` 通过;`tests/e2e.sh` **PASS=82 FAIL=0**。

## 5. 实现上的几个取舍(如实)

- **缓存无过期**。RTT 在同一目标上基本稳定,进程内永不过期是合理的;但若网络条件大幅变化(切 WiFi/换路由),需重启客户端才会重测。若要支持,可加长 TTL(如 10 分钟)而非短 TTL——短 TTL 会重新引入"周期性探测"。
- **缓存无上限**。键是 `(SocketAddr, proxy_url)`,客户端配置里只有一个 `server_addr`,条目数天然是 1~2。未加淘汰(与 DNS 缓存不同,那里键是任意域名)。
- **`probe_call_count` / `probe_cache_clear` 不是 `#[cfg(test)]`**。最初写成 `cfg(test)`,结果 rh-client(rh-transport 的下游 crate)在测试里编译失败——`cfg(test)` 只在编译该 crate 自身测试时生效。改为始终编译的 `pub fn`;计数器用 `Relaxed` 自增,生产开销可忽略。
- **`DialFn` 用了 `futures::future::BoxFuture`**。`probe_set_dial` 需要注入任意实现,`fn` 指针 + boxed future 是最简单方案;每次探测一次 box(仅未命中缓存时),不影响生产路径。
- **没有把 `dial_tcp` 直接曝露给探针复用**。它在 `tcp.rs` 里是私有 fn,且带 `set_nodelay`/keepalive/`sndbuf` 等载体专属调优——探针只需要"拨通并计时",带上这些反而让测量偏离纯建连耗时。所以是**同语义的独立实现**(代理分支一致),而非字面复用。
