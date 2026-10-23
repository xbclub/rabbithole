# 并发/串行化缺陷修复记录

> 基线提交:`1b8bfd6`。本文件记录 12 项「本应并行/异步、实际写成串行」缺陷的
> 修复依据、复杂度变化与实测数据。
>
> **口径声明**:这 12 项都不在吞吐热路径(管理面 / 连接建立),`benchmark/peer.sh`
> 测不出差异。因此本文**不提供任何吞吐基准数字**。所有数据来自管理面耗时与
> 纯函数基准,测量环境为本机(Apple Silicon,macOS),`--release`。
> 凡未测到的部分,在对应小节明确标注「未测」并说明原因,不编造数字。

---

## 0. 总览

> **⚠ 位置列的行号口径**:「修前位置」列的行号是**修前基线 `1b8bfd6`** 的,
> 修复后已整体偏移(有的 +100 行以上,有的因重构而不再存在)。
> **查当前代码请用符号名搜索,不要按「修前位置」跳转。**
>
> 「修后位置」列给的是当前代码里对应实现的落点(随后续提交仍可能漂移),
> 仅作导航提示,不作为精确引用。

| # | 缺陷 | 修前位置(`1b8bfd6`) | 修后位置(当前) | 复杂度变化 | 实测 |
|---|---|---|---|---|---|
| 1 | vhost 路由全量重建被放进单条规则生命周期 | `runtime.rs` 262/336/510/589 | `runtime.rs` 263/337/513/593 的 `schedule_vhost_rebuild()` 调用点 | O(N²) → O(N) | N=2000: 746.2ms → 0.46ms (1630x) |
| 2 | 启动恢复逐条串行 | `recovery.rs` 43/106 | `recovery.rs:139` `for_each_concurrent` | O(N·t) → O(N/16·t) | **本机未测出收益**(见 §2) |
| 3 | `batch_services` 逐 id 串行 | `api.rs` 1527 | `api.rs:1518` `batch_services` | O(N·t) → O(N/8·t) | N=1000: 261.8ms → 157.6ms (1.66x) |
| 4 | `port_in_use` 跨 await 持 dashmap 分片守卫 | `state.rs` 1140 | `state.rs:1252` `port_in_use` | 持锁窗口 → 0 | 旧实现 387ms 阻塞,新实现通过 |
| 5 | `allocate_port` 双重循环 × 每端口全表扫 | `state.rs` 1176 | `state.rs:1279` `allocate_port` | O(P·N) → O(N + P) | N=2000: 241.4ms → 0.39ms (620x) |
| 6 | 客户端 `apply_sync` 持 `proxies` 锁跨 await | `rh-client/session.rs` 1294 | `rh-client/session.rs:1362` 起(锁内只收集) | 持锁跨 await → 锁内纯内存 | **测试抓不住**(见 §6) |
| 7 | `list_services` 循环内 3 次 get + 3 await + 嵌套锁 | `api.rs` 741 | `api.rs:728` `list_services`(**未改**) | 3N 次锁 → N 次 | **未做**(见 §7) |
| 8 | `resolve_visitor_target` N+1 查询 | `api.rs` 1713 | `api.rs:1792` `resolve_visitor_target` | O(N) 往返 → O(1) 往返 | N=1000: 31.08ms → 3.38ms (9.2x) |
| 9 | `tcp_refill` 逐条签发 + `add_pending_request` 全表重建 | **`rh-server`**`/session.rs` 124 | **`rh-server`**`/session.rs:150` `extract_if` | O(P) 重建 → O(P) 原地 | P=1000: 40.71µs → 2.00µs (20x) |
| 10 | 多载体报到串行,每条等 2s ACK | `rh-client/session.rs` 864 | `rh-client/session.rs:971` `buffer_unordered(4)` | O(K·D) → O(K/4·D) | K=8: 386.0ms → 105.5ms (3.66x) |
| 11 | `build_sync_entities` 两次串行 DB 查询 | `state.rs` 1551 | `state.rs:1663` `build_sync_entities` | O(2t) → O(t) | **未单独测**(见 §11) |
| 12 | `events.emit` 同步落库 | `events.rs` 15/19 | `events.rs:124/132` `emit` / `emit_detail` | 业务路径含 DB → 纯内存 | N=1000: 31.91ms → 0.07ms (454x) |

> **两处需要特别说明的位置**:
>
> - **#10**:修前的 `for i in 1..initial_k` 串行循环**已不存在**(被
>   `buffer_unordered(4)` 替代),所以「修后位置」指向的是并发化的落点,
>   而不是同一个循环的原地修改。
> - **#9**:`session.rs` 在 `rh-server` 与 `rh-client` 各有一份,本项在
>   **`rh-server`**(`add_pending_request` / `Session` 结构体)。原表漏写
>   crate 前缀,按 `rh-client/session.rs:124` 找会落到 `visitors` 字段上。

---

## 1. 缺陷 1:vhost 路由全量重建 → O(N²)

**改法**:`ProxyManager` 加 `vhost_dirty: AtomicBool` + `vhost_flush_scheduled`
去重标记。生命周期函数改调 `schedule_vhost_rebuild()`(标脏 + 10ms 窗口后合并
重建),另提供 `flush_vhost_routes()` 给需要立即生效的调用方。

**为什么是 10ms**:管理面单次操作的端到端耗时在毫秒~数十毫秒量级,10ms 的
路由滞后不可感知;而批量路径(N 条)在窗口内只触发一次 O(N) 重建。再大就会
让"建完规则立刻访问"的脚本偶发 404。仓库的 e2e 脚本(`tests/e2e.sh`)在创建
规则后都 `sleep 2` 才访问,不受影响 —— 已实测验证(见 §13)。

**合并任务的顺序要求**:必须先清 `vhost_flush_scheduled` 再重建。反过来的话,
重建期间到达的新标脏会被"已排队"判断吃掉,那批变更要等到下一次生命周期事件
才生效。有 `mark_during_window_is_not_lost` 守着。

**实测**(release,模拟「N 条规则各触发一次重建」的总耗时):

| N | 每次重建(修前) | 合并重建(修后) | 加速 |
|---|---|---|---|
| 100 | 2.17ms | 0.015ms | 148x |
| 500 | 45.21ms | 0.091ms | 500x |
| 1000 | 184.74ms | 0.201ms | 919x |
| 2000 | 746.22ms | 0.458ms | 1630x |

增长曲线:per-call 随 N 平方增长(2.17→45.2→184.7→746.2ms),coalesced 随 N
线性(14.6→90.5→201→457.9µs),与 O(N²)→O(N) 吻合。

> 口径:两次独立运行的代表值,逐项抖动 < 5%。

复现命令:
```bash
cargo test -p rh-server --release vhost_coalesce_tests::bench -- --ignored --nocapture
```

**语义代价(有意接受)**:路由表最多滞后一个窗口。单条规则的 API 路径也走
合并窗口(不再立即重建),这是为保持代码路径统一 —— 若需要严格立即,调用方
可显式 `flush_vhost_routes()`。

---

## 2. 缺陷 2:启动恢复逐条串行

**改法**:分两段。

- **第一段串行**:资格过滤、spec 规范化、`port_auto` 被外部占用时的端口重分配。
- **第二段并发**:`spawn_runtime`,`for_each_concurrent(16)`。

**为什么端口分配必须串行(关键判断)**:任务书 §2 #2 要求先确认
`create_bind_checked` 的 `serial_lock` 是否覆盖恢复路径。核对结果:**不覆盖**。
`recover_all` 全程不持任何 `serial_lock(client_id)`(那把锁是每客户端粒度,
启动期不存在并发创建者)。而 `allocate_port` 只看 `ProxyManager::runtimes` 里
的运行态 + OS 试绑 —— 并发执行时,两条 port_auto 规则会同时读到"该端口空闲"
并双双通过检查,其中一条随后 bind 失败,留下 BindError 行。

因此把端口决策留在串行段,并发只覆盖"端口已定、互不重叠"的 `spawn_runtime`。
这符合任务书"若不覆盖,必须保持 allocate_port 串行"的要求。

顺带把 `get_client` 预取成 HashMap(修前每条规则一次往返)。

**★实测结论:本机测不出收益,甚至略慢。**

| N=1000 | 串行 | 并发(16) |
|---|---|---|
| 无在线会话 | 58.29ms | 58.55ms |
| 有在线会话 | 58.21ms | 71.80ms |

原因:`spawn_runtime` 的 await 点(`get_client`、`send_bounded`、
`mark_unconfirmed`)在 `:memory:` SQLite + 有消费者的大队列下几乎不产生等待,
并发只剩调度开销。

**真实部署的收益来源**(未在本机验证,如实标注):
1. 文件型 SQLite 的读延迟(每次 `get_client` 是真实磁盘/页缓存往返);
2. `send_bounded` 在客户端读得慢时真的会 park;
3. 叠加缺陷 1 后,单条 spawn 不再做 O(N) 的 vhost 重建 —— **这一项可以单独
   验证**,见 §1 的 N=2000 数据(746.2ms → 0.46ms),那才是这一批的主要收益。

复现命令:
```bash
cargo test -p rh-server --release recovery_tests::bench -- --ignored --nocapture
cargo test -p rh-server --release recovery_tests::bench_recovery_with_online -- --ignored --nocapture
```

**踩过的坑(记下来避免重犯)**:第一版在线基准把 mpsc 队列设成 64 且不消费,
结果 N=1000 时排队规则全部撞 2s `CONTROL_SEND_TIMEOUT`,测出来 62s。那个数字
反映的是"队列满时的超时",不是并发收益。改成容量 100000 + 后台消费后才
隔离出锁争用本身。

---

## 3. 缺陷 3:`batch_services` 逐 id 串行

**改法**:一次 `list_proxies(None)` 建 id→client_id 映射 → 按 client_id 分组
→ 组间 `buffer_unordered(8)` 并发、组内顺序 await → 结果用原下标回填。

**为什么组内仍调 `*_inner` 而不是外层持锁 + `*_locked`**:`*_inner` 自己会
`lock().await`,外层若已持同一把 `tokio::sync::Mutex` 就是二次加锁 → 直接
死锁(与仓库 A5 注释描述的是同一个坑)。让 inner 自己排队,组内天然串行 ——
`tokio::sync::Mutex` 是公平的。有 `same_client_batch_does_not_deadlock`
用 20s 超时守着。

**结果顺序**:必须与 `req.ids` 同序(前端按下标回填)。用
`Vec<Option<Value>>` 按下标回填,而不是按完成顺序 push。有
`results_preserve_request_order` 守着。

**实测**(release,批量 disable):

| N | clients=1(组内串行) | clients=16(组间并发) |
|---|---|---|
| 100 | 25.91ms | 14.56ms |
| 500 | 120.58ms | 70.75ms |
| 1000 | 286.95ms | 200.25ms |

约 1.4~1.8x(逐档不同)。**反证**:把并发度降到 1 后,clients=16 的耗时回到
258.06ms(与 clients=1 的 263.51ms 持平),证明并发确实来自分组而非测量噪声。

复现命令:
```bash
cargo test -p rh-server --release batch_services_tests::bench -- --ignored --nocapture
```

**注**:SQLite 是全局单写者,并发度取 8 已足够;再高只是把压力堆到写锁上,
不会更快。

---

## 4. 缺陷 4:`port_in_use` 跨 await 持 dashmap 分片守卫

**改法**:照抄同文件 `rebuild_vhost_routes` 的快照写法 ——
`let runtimes: Vec<Arc<ProxyRuntime>> = self.runtimes.iter().map(...).collect();`
先释放分片守卫,再遍历。

**★口径更正(重要)**:任务书与 `rebuild_vhost_routes` 的注释都说
`dashmap::iter()` 持**分片写锁**。核对 dashmap 6.2.1 源码(`iter.rs` 里
`GuardIter = Arc<RwLockReadGuard<..>>`,`next()` 调 `_yield_read_shard`)
可知实际是**读锁**。

危害因此收窄但依然真实:同分片的 `insert`/`remove`(runtime 注册/注销)需要
写锁,会被跨 await 持有的读锁挡住,而 dashmap 分片锁是**同步锁** —— 挡住的是
整条 OS 线程,不只是某个 future。

**实测**(`shard_guard_not_held_across_await`):让一条 runtime 的 `spec` 写锁
被长期持有(400ms),`port_in_use` 就会挂起在 `spec.read().await` 上并继续持有
分片读锁;此时另起 OS 线程往表里灌 4990 个 key。

| 实现 | 灌 key 耗时 |
|---|---|
| 修前(直接迭代) | **387.15ms**(失败) |
| 修后(快照) | 通过(< 300ms 阈值) |

这条测试是**真能区分新旧实现的**(已用临时还原旧代码验证)。

---

## 5. 缺陷 5:`allocate_port` 双重循环 × 每端口全表扫

**改法**:提取 `collect_occupied_ports(proto, ip, except_proxy, new_group_key)`,
一次快照 + 一次遍历产出「已占用端口集合」;`port_in_use` 退化为
`set.contains(&port)`,`allocate_port` 复用同一集合。

**语义等价性怎么保证**:地址层面的冲突判定仍交给
`bind_conflicts(proto, ip, 0, proto, other_ip, 0)`(传端口 0/0 只走地址分支),
不另写一份 IP 冲突规则,避免两处判定漂移。

**实测**(release,纯函数基准):

| N | `port_in_use` 单次(修前) | 单次(修后) | `allocate_port`(修前) | (修后) |
|---|---|---|---|---|
| 100 | 12.54µs | 16.81µs | 0.67ms | 0.10ms |
| 500 | 59.77µs | 80.23µs | 14.92ms | 0.10ms |
| 1000 | 120.12µs | 157.08µs | 60.08ms | 0.19ms |
| 2000 | 241.40µs | 314.79µs | **241.38ms** | **0.39ms** |

> 口径:修后一列为 3 次重复运行的代表值(抖动 ±5%)。首次运行会偏慢
> (冷缓存,`allocate_port` N=2000 曾测得 0.70ms),已按稳定值修正。

**★「修前」列现在可复现了**:原版这两列都是手工用 `git stash` 切回旧实现
测的,后人无法复现。现在 `bench_port_scan_scaling`
(`crates/rh-server/src/state.rs`)里同时测两个口径:

- `reference_in_use` —— 等价性测试用的旧口径函数(直接 `runtimes.iter()`
  + 逐条 `spec.read().await`),**恰好完整复刻了修前"跨 await 持有 dashmap
  分片读锁"的形态**,可直接当性能对照;
- `state.proxies.port_in_use` / `allocate_port` —— 新实现。

`allocate_port` 的旧口径是"逐候选端口调一次 `reference_in_use`",这正是修前
`allocate_port` 的循环体,两者同语义、可直接比较。基准里还加了
`debug_assert_eq!(got, got_old)` 确保两个口径选出同一个端口(正确性锚点)。

复现命令不变:
```bash
cargo test -p rh-server --release port_scan_tests::bench -- --ignored --nocapture
```
输出形如(一次实测):
```
N= 2000  port_in_use: old=  241.488µs new=  341.887µs  allocate_port: old=255.643375ms new=  450.459µs  → Some(12000)
```
旧口径读数与本表「修前」列一致(241.4µs / 241.4ms),确认可复现。

**如实记录**:`port_in_use` 单次耗时**变慢了**(约 1.4~1.6x)。原因是现在它
多了一次 `HashSet` 构建(修前是边遍历边短路返回)。这是有意的取舍:
- 这次改的是「不要跨 await 持锁」和「不要每个端口扫一遍」;
- `allocate_port` 从 O(P·N) 降到 O(N + P),N=2000 时 241.38ms → 0.39ms
  (约 620x);
- 单次 `port_in_use` 在真实调用点(创建/更新规则)每次只调一次,1.3x 的
  微秒级差异不可感知;而 `allocate_port` 的数量级提升直接决定"端口耗尽时
  创建规则要等多久"。

如果将来单次 `port_in_use` 的耗时变成瓶颈,可以加一条短路:先用
`except_proxy`/`group_key` 过滤,命中即返回 —— 但当前没有证据表明需要。

复现命令:
```bash
cargo test -p rh-server --release port_scan_tests::bench -- --ignored --nocapture
```

---

## 6. 缺陷 6:客户端 `apply_sync` 持 `proxies` 锁跨 await

**改法**:锁内只做纯内存收集(算出 `to_stop` / `to_start` 两个 Vec),释放锁后
再逐个执行 `stop_proxy` / `start_health`。visitors 分支同款处理。

锁获取顺序不变(proxies → health_cancels),只是不再嵌套持有,不引入新的死锁
可能。同文件的 `upsert` 早就有 `drop(cur)` 的先例。

**★如实记录测试的能力边界**:`proxies_lock_available_during_apply_sync`
**抓不住这个缺陷本身**。实测新旧实现的最长读锁等待都在 5~7ms,差异被
`tokio::spawn` 的调度抖动淹没。原因是 stop/start 内部的 await 点本身只有
微秒级,真正的代价是"持锁期间其它路径被挡住",而单测里没有那个并发压力源
(生产中是 `open_data_channel` 的读锁叠加真实建连延迟)。

因此该测试只作**功能性回归**(不死锁、读锁可用),锁纪律靠源码结构保证
(锁内只做纯内存收集)。这一点已在提交信息里写明,避免后人误以为它守住了
锁语义。

**未做**:`upsert` 的 proxy 分支有同样形态(持 `proxies` 锁调
`stop_proxy`/`start_health`),但不在任务书 #6 范围内,没有顺手改。
**这一项已提升为正式未完成项,详见 §15.6** —— 它的触发频率(`ServiceUpdate`,
每次单条规则变更)实际上高于本条已修的 `apply_sync`(`ServiceSync`,仅重连时)。

> 注:该项已在 §15.6 提升为**正式未完成项**。原因:`upsert` 由
> `ServiceUpdate`(每改一条规则触发一次)驱动,触发频率**高于**本条修的
> `apply_sync`(只在客户端重连时触发),影响可能更大。详见 §15.6。

---

## 7. 缺陷 7:`list_services` 循环内 **4** 次 get + 3 个 await + 嵌套锁

> **口径更正**:本节原标题写「3 次 get」,**实际是 4 次**。除了
> status / pool_ready / rt_meter 三处,还有 `healthy` 字段的独立 get
> (`"healthy": state.proxies.get(id as u64).map(...)`)。它不在那三个
> await 块里,容易被漏数。已在收尾批次 P5 核实并更正。

**★状态:部分完成(2026-10-23)。**

- **已完成**:「一次 `get` 拿到 `rt` 复用」—— 4 次合 1 次,commit `0d32269`。
  实测 N=1000 时省 ~31µs(微秒级,与本节的"收益极小"判断一致)。
- **仍未做**:`snapshot_state` 改 `try_read`。理由不变(见下)。

**判断依据**:任务书把它列为【中】,但修法要求把 `snapshot_state` 的
`RwLock::read` 改成 `try_read` 并在失败时降级为 `"unknown"`。这会**改变
管理面接口的返回值语义** —— 列表页可能显示 `"unknown"` 而不是真实状态,
而任务书 §0 明确要求「不要改变功能语义」。

三个子项的风险不对等:
- 「一次 `get` 拿到 `rt` 复用」:零风险,但单独做收益极小(省 2 次 dashmap
  查找,微秒级);
- 「`pool.lock()` + `ready.lock()` 嵌套读改单次」:需要改动 `TcpPool` 结构,
  影响面超出「展示接口」;
- 「`snapshot_state` 改 `try_read`」:有语义变化,如上。

**结论**:收尾批次按本节的建议做了第一子项(复用 `rt`),**保持
`snapshot_state` 的阻塞语义不变**。另外两个子项(`try_read` 改造、
`pool`/`ready` 嵌套锁)仍然不做 —— 前者有语义变化,后者要改 `TcpPool`
结构、影响面超出展示接口。

---

## 8. 缺陷 8:`resolve_visitor_target` N+1 查询

**改法**:一次 `list_clients()` + 一次 `list_proxies(None)`,内存里建
`HashSet<&str>` 匹配。

**候选顺序为什么必须保持**:修前按 `list_clients()` 的返回顺序
(`ORDER BY created_at`)逐个追加,`candidates.first()` 依赖这个顺序。所以改成
**按 clients 的原顺序过滤**,而不是把匹配结果按 client_id 排序 —— 后者会改变
"全部离线时取首个"的语义。

**实测**(release,每客户端 1 条规则):

| N_clients | 修前 | 修后 | 加速 |
|---|---|---|---|
| 50 | 1.64ms | 0.25ms | 6.6x |
| 200 | 6.21ms | 0.73ms | 8.5x |
| 500 | 15.57ms | 1.72ms | 9.0x |
| 1000 | 31.08ms | 3.38ms | 9.2x |

剩余耗时主要是那一次全表反序列化,属必要成本。

**等价性验证**:全部 5 条测试(单匹配、名字不匹配、类型不匹配、在线优先、
离线取首个)在**旧实现**下同样通过(已核对),证明这是等价重构而非语义变更。

复现命令:
```bash
cargo test -p rh-server --release visitor_target_tests::bench -- --ignored --nocapture
```

---

## 9. 缺陷 9:`add_pending_request` 全表重建

**改法**:`BTreeMap::extract_if(.., |_, v| v.expires <= now)` 一次遍历就把
过期项连所有权一起移出,不重建表、不二次查找;顺带把原来两次加锁合并成一次。

**★为什么不用 `retain`**:第一版就是用 `retain` + 逐 key `remove` 写的,
`multiple_expired_all_hooks_called` 直接抓到钩子一次都没调 —— `retain` 已经把
条目删了,`remove` 拿不到值。`retain` 的闭包只拿 `&mut V`,移不出所有权。
`extract_if` 才是对的工具。

**为什么没有并发化 `tcp_refill`**:任务书 §2 #9 明确要求不做。`inflight` 的
失败回滚路径有历史陷阱(`issue_create_channel_with` 失败时用
`take_pending_request_quiet`,故意不触发钩子,假定调用方自己回收),并发化会
让这个记账更难对齐。本项只动清扫策略。

**为什么没有加"仅当 len 超过阈值才清扫"**:清扫是唯一保证过期条目的回收钩子
被调用的地方(交付路径只处理成功的),跳过一次就多饿死一个池名额。
`extract_if` 的 O(P) 已足够便宜。

**实测**(release,表里有 P 条未过期条目时单次登记的锁内耗时):

| P | 修前(重建整表) | 修后(extract_if) |
|---|---|---|
| 10 | 2.87µs | 0.70µs |
| 100 | 4.18µs | 0.53µs |
| 500 | 17.26µs | 1.09µs |
| 1000 | 40.71µs | 2.00µs |

P=1000 时约 20x,且修前随 P 线性增长、修后曲线明显更平。

复现命令:
```bash
cargo test -p rh-server --release pending_request_cleanup_tests::bench -- --ignored --nocapture
```

---

## 10. 缺陷 10:多载体报到串行

**改法**:`futures::stream::iter(1..initial_k).buffer_unordered(4)`,
`failed_ids` / `server_caps` 改 `Mutex<Vec<_>>` 并发收集。

**并发安全的前提(逐条核对)**:
- `server_caps` → `plan_convergence` 只取 `min()`,与顺序无关;
- `failed_ids` → `plan_detach_set` 只做 `contains` 与集合并集,结果最后
  `sort_unstable` + `dedup`,同样与顺序无关。

这两条各有一个纯函数测试守着,避免将来有人往这两个函数里加"依赖顺序"的逻辑。

`control_member_id` 不受影响(控制载体是单独开的流,不参与这个循环)。

**实测**(release,每条载体模拟 50ms ACK 延迟):

| K | 串行 | 并发(4) | 加速 |
|---|---|---|---|
| 2 | 53.13ms | 53.19ms | 1.00x |
| 4 | 158.66ms | 53.17ms | 2.98x |
| 8 | 386.05ms | 105.51ms | 3.66x |

K=2 时无收益是预期的(只有 1 条附加载体,无可并发)。

**★这是调度结构基准(用 sleep 模拟 RTT),不是真实网络 RTT 测量。**

**未测**:任务书建议的 `RH_MUX_POOL_SIZE=4` + 人为延迟载体 ACK 端到端测法
没有做。原因:
1. 需要起真实 server/client 双端并改造服务端注入延迟,超出本项改动范围;
2. `RH_MUX_POOL_SIZE` 是 `OnceLock`,进程内改 env 不生效(见 AGENTS.md 已知坑),
   必须用真实进程环境变量启动。

如实记录为未测项。真实 RTT 下的收益应当比 50ms 模拟更小(局域网 RTT 通常
< 1ms,报到总耗时本来就只有几毫秒)。

---

## 11. 缺陷 11:`build_sync_entities` 两次串行 DB 查询

**改法**:`tokio::join!(list_proxies, list_visitors)`。

**为什么没有单独测**:SQLite 池在非内存库下是 8 连接,并行安全;但全部单测用
`:memory:`(1 连接),并行会退化成串行 —— **测试环境里测不出差异**。真实收益
是省一个 SQLite 往返,量级在几十~几百微秒,低于本机测量噪声。

判断依据:一行改动、零语义变化、零风险。**这是判断,不是实测结论** ——
如实记录为未测。

---

## 12. 缺陷 12:`events.emit` 同步落库

**改法**:
- `EventBus` 持 `mpsc::unbounded_channel`,`emit` 只 `send` 就返回;
- 单个 writer 任务批量取(最多 64 条/批)、用**一个事务**写完整批;
- `emit` 保持 `async fn` 签名不变(30 个调用点零改动),但函数体内**没有
  await 点**。

**为什么 unbounded 而不是 bounded**:事件量级很小(心跳级 + 规则变更)。
bounded 满了只有两个选择 —— 丢事件(控制台静默缺条目)或阻塞业务(退回修前
的问题),两者都不可接受。兜底是 `emit` 在 channel 已关闭时**降级为直接写库**。

**进程退出 flush**:`main.rs` 的 graceful shutdown 里先 `flush()`(放一个带回执
的栅栏事件,writer 写完它之前的全部事件才回执)再 `shutdown()`(关队列 + 等
writer 退出,5s 上限)。否则队列里最后几条事件随进程消失,而"最后几条"恰恰是
停机相关的(客户端下线、配置变更),最不该丢。

**为什么批量写要单事务**:SQLite 每次单独 INSERT 都是一次隐式事务(各带一次
fsync)。一个事务包住整批,把 N 次同步压成 1 次。整批失败时降级为逐条重试,
一条坏数据不该让同批其它事件一起丢掉。

**实测**(release):

| N | 业务路径(修前) | 业务路径(修后) | 总落库(修前) | 总落库(修后) |
|---|---|---|---|---|
| 100 | 3.40ms | 0.011ms | 3.47ms | 0.94ms |
| 500 | 16.91ms | 0.043ms | 16.92ms | 3.98ms |
| 1000 | 31.91ms | 0.075ms | 31.92ms | 7.72ms |

业务路径 1000 条约 **425x**(几乎只剩内存拷贝),总落库约 **3.8x**(单事务的
fsync 摊销)。两个数字分开记录,不混为一谈 —— 前者是业务延迟,后者是 DB 总
吞吐。

**测试兼容**:检查过全部既有测试,没有"emit 后立刻查 DB 断言"的模式
(`list_events` 只被 events.rs 自己的新测试和 API handler 使用)。新测试统一用
`flush()` / `shutdown()` 建立确定性,不靠 sleep 轮询。

复现命令:
```bash
cargo test -p rh-server --release events::tests::bench -- --ignored --nocapture
```

---

## 13. 端到端验证

**任务书 §5.3 要求「修完必须有一个端到端用例覆盖真实调用路径」。**

### vhost E2E(`tests/e2e-vhost.sh`)

**PASS=16 FAIL=0**。覆盖 HTTPS SNI 分流、头改写、Basic 鉴权、自定义 404、
M2 规则级 404 / 静态缓存、M3 证书上传与删除。这一套直接验证了缺陷 1 的合并
窗口没有破坏 vhost 路由的可见性(脚本在创建规则后 `sleep 2`,远大于 10ms 窗口)。

### 主 E2E(`tests/e2e.sh`)

**PASS=79 FAIL=3**。

3 个失败项:
- `STCP 密钥由服务端随机生成(入参被忽略)`
- `轮换端点生成新 32 hex 密钥`
- `非端口更新原位切换目标`

**已确认与本次改动无关**:用 `git worktree` 检出基线 `1b8bfd6`、独立构建后跑
同一套脚本,结果**完全相同**(79/3,失败项同名)。这 3 项是基线已有的问题。

### 单元/集成测试

```
cargo test --workspace   → 609 passed, 0 failed
cargo clippy --workspace --all-targets -- -D warnings → 干净
cargo fmt --all -- --check → 干净
```

### 已知的基线失败(非本次改动引入)—— 已于 2026-10-23 修复

`rh-client` 的 `dnsquery::cache_tests::concurrent_requests_share_one_query`
(以及同模块的 `second_resolve_hits_cache`)会失败,报
「10 个请求触发了 2 次真实解析」。

#### 失败频率(两次更正)

| 版本 | 说法 | 状态 |
|---|---|---|
| 初版 | 「6 次里 2 次」 | 偏低 |
| 第二版 | 「约 50%」 | 仍偏低 |
| **实测** | `cargo test -p rh-client` 20 次跑下来 **12 次失败 = 60%** | 准确 |

单跑该用例 10 次、单跑整个 `cache_tests` 模块 10 次、模块内并发 8 线程 15 次
——**全部 0 失败**;`--test-threads=1` 跑整个 crate 3 次也全过。只有整个
crate **并行**跑才挂。

#### 根因(已定位,不是原有的三个猜测)

此前把根因写成「三个 `static` 由同 crate 的多个 `cache_tests` 共享,
并发执行时互相污染」。**这个判断只对了一半** —— `cache_tests` 内部
已经有 `LOCK` 串行化了(模块内并发 15 次全过就是证据)。

真正的污染源在**模块外**:

> `plugins::proxy_tests::socks5_domain_target_goes_through_resolver`
> (`crates/rh-client/src/plugins.rs`)会发一个 ATYP=0x03 的 SOCKS5 请求,
> 插件按 `resolve_addr` → `forward::resolve` → `resolve_cached`
> 走**本端解析器** —— 而它**不持** `cache_tests` 的 `LOCK`。

`cache_tests::setup()` 会把**全局**解析器换成 `counting_query`(计数 +
50ms 延迟)。那条 plugins 测试在任意时刻命中这个注入器时:

1. 把 `cache_tests::QUERIES` +1;
2. 被 `DELAY_MS=50` 拖慢;
3. 清掉/污染 `inflight` 与 `cache`。

第 1 条是失败的直接原因:若那次 `fetch_add` 落在
`concurrent_requests_share_one_query` 的 50ms 窗口内,断言就看见 2 次解析。

**定位方法**(不是猜的):在 `counting_query` 里加临时探针,把每次调用的
host/port/delay 与 `rh_client` 栈帧写进文件(`eprintln` 会被并发交错吞掉),
跑整个 crate 直到失败。捕获到:

```
QUERY#1 host=cache-test.example port=80 delay=50 ok=1   ← 被测用例自己
QUERY#2 host=rh-test.invalid   port=9  delay=50 ok=1   ← 污染源
assertion failed: 并发去重失效:10 个请求触发了 2 次真实解析
```

栈帧:

```
plugins::serve_socks5 → dial_proxy_destination → resolve_addr
→ forward::resolve → resolve_cached → counting_query
```

**关键判据**:`QUERY#1` 只有**一次**真实解析 —— 说明 singleflight 根本没坏,
是计数器看见了**两条不同的 host**。这也解释了为什么是 2 而不是 10
(若 singleflight 真坏了会是 10)。

另用确定性探针复现过:不走并发,直接
`forward::resolve("rh-test.invalid", 9)` 也能让 `QUERIES` 从 0 变 1。

> ★**一处假设被推翻**:任务书曾排除「plugins.rs 的测试触达 DNS」。
> 实测该测试**确实触达**,这是本次的关键发现。

#### 修法

把串行锁提升为 **crate 级共享**,让 plugins 那条测试也加入同一个串行域:

- `dnsquery` 新增 `pub(crate) static DNS_GLOBAL_TEST_LOCK`;
  `cache_tests` 原来的私有 `LOCK` 改为 `use super::DNS_GLOBAL_TEST_LOCK as LOCK`;
- `socks5_domain_target_goes_through_resolver` 开头加
  `let _dns = crate::dnsquery::DNS_GLOBAL_TEST_LOCK.lock().await;`。

**为什么不选别的**:
- epoch/generation 让过期任务不计数:要改注入点设计,而且管不住「被 50ms
  拖慢」「清掉 inflight」这两个副作用;
- `serial_test` crate:引入新依赖;
- 放宽断言(`QUERIES >= 1 && <= 2`):会掩盖真实的 singleflight 失效,
  **明确不采用**。

生产代码(`inflight()` / `cache()` / `resolve_cached`)**一行未动**。

#### 验证

| 项 | 结果 |
|---|---|
| `cargo test -p rh-client` 连跑 20 次 | **0 失败**(修前 60% 红) |
| `cargo test --workspace` 连跑 10 次 | **0 失败** |
| 测试仍然有效(破坏 singleflight) | 把 `resolve_cached` 的登记逻辑删掉后立刻变红:「10 个请求触发了 10 次真实解析」 |

提交:`f2f00c6`。

---

## 14. 明确没动的地方(及其理由)

| 位置 | 理由 |
|---|---|
| `data.rs:3027` `copy_bidirectional_bounded` | 第二向必须在 `halfclose_grace` 内**有界**等待。无界并发会重蹈 FIN-WAIT-2 泄漏 |
| `data.rs:486` `copy_relay_pair` | 同上,`join_relay_pumps` 的有界 join 是刻意设计 |
| `control.rs:871` 服务端控制读循环 | 该循环**同时是** `queue_tx` 的唯一消费者,任何 `send().await` 都会自锁 |
| `rh-client/session.rs:1046` 客户端读循环 | 同款自锁风险 |
| `state.serial_lock(client_id)` 全部路径 | 每客户端串行域,防端口竞态与规则双占,是正确性要求 |
| `tcp.rs:526` `collect_pool_members` | 已是 `FuturesUnordered` 并发;`now_or_never` 排空是防泄漏关键 |
| `mux.rs:2967` 重拨循环 | 有退避语义,并发会打爆服务端 |
| `vendor/yamux/` | vendored 代码,不在范围 |
| `tcp_refill` 并发化 | 任务书明确不做(见 §9) |
| `upsert` 的 proxy 分支持锁 | 不在 #6 范围内(见 §6) |

---

## 15. 未完成项汇总(如实记录)

> **2026-10-23 更新**:下列第 1 与第 6 项已完成(见 §17 的收尾批次)。
> 剩余项仍然如实标注为未做/未测。

1. ~~**#7 `list_services`**~~ —— **已完成**(收尾批次 P5,
   commit `0d32269`)。做了零风险的部分(4 次 `get` 合 1 次),
   **仍然没有**动 `snapshot_state` 的 `try_read` 改造 —— 那条会改变接口
   语义,依旧否决。**口径更正:是 4 次,不是 3 次**(多一个 `healthy`
   字段的独立 get)。
2. **#2 恢复并发**:代码已改,但**本机未测出收益**。真实收益依赖文件型
   SQLite 与在线会话,未在真实部署环境验证。
3. **#10 端到端 RTT 测量**:未做。只有调度结构基准。
4. **#11**:未单独测(测试环境 `:memory:` 掩盖了收益)。
5. **基线 3 个 e2e 失败项**:未修,超出范围。
   (DNS 缓存测试的 60% flaky 已于 `f2f00c6` 修复,见 §13 —— 不再是未解决项。)
6. ~~**`upsert` 的 proxy 分支仍持 `proxies` 写锁跨 await**~~ —— **已完成**
   (收尾批次 P1,commit `cb0be73`)。详见 §15.6。

### 15.6 `upsert` 的 proxy 分支:与 #6 同形,但触发频率更高

**位置**:`crates/rh-client/src/session.rs:1429` 起

```rust
async fn upsert(sess: &Arc<ClientSession>, e: SyncEntity) {
    match e {
        SyncEntity::Proxy(p) => {
            let mut cur = sess.proxies.lock().await;   // ← 持写锁
            // ...
            stop_proxy(sess, p.id).await;              // ← await(取 health_cancels 锁)
            // ...
            start_health(sess, p.id, &p).await;        // ← await(取 health_cancels 锁 + spawn)
        }
        SyncEntity::Visitor(v) => {
            // 这个分支**已经有** drop(cur),注释写着「别持着 visitors 锁去做重绑定」
            // ...
        }
    }
}
```

**为什么这是缺陷**:与已修的 #6(`apply_sync`)完全同形 —— 持 `proxies`
写锁跨 await,期间 `open_data_channel` 的
`sess.proxies.lock().await.get(&proxy_id)` 被挡住。visitor 分支早就修过
(`drop(cur)`),**proxy 分支是同一个漏点,上一轮没修**。

**为什么它比已修的 #6 更值得优先**:

| | 触发条件 | 频率 |
|---|---|---|
| `apply_sync`(#6,已修) | `ServiceSync` —— 客户端**重连**时的全量同步 | 低(每次重连一次) |
| `upsert`(本条,未修) | `ServiceUpdate` —— 服务端**每改一条规则**就发一条 | **高**(管理面每次单条变更) |

`session.rs:1256` 的 `SERVICE_UPDATE` 分支直接 `upsert(sess, u.entity).await`。
也就是说,管理员每改一条规则、每启停一条规则,客户端都会走一次这条持锁
路径。**实际触发频率高于 #6**,影响可能更大。

**上一轮为什么漏了**:任务书 #6 只点名了 `apply_sync`(1294-1314 行),
`upsert` 不在范围内;当时按「不要顺手优化任务书之外的地方」只记录未改
(见 §6 末尾)。现在把它提升为正式未完成项,是因为它**不是"顺手的额外
优化",而是同一个缺陷在同一文件里的另一半** —— #6 的修法(`drop(cur)`)
可以直接照抄,成本极低。

**建议改法**:照抄同文件 visitor 分支与 #6 的写法 —— 锁内只做纯内存收集
(算出待 stop / 待 start),`drop(cur)` 后再 await。注意 proxy 分支的
`cur.remove(&p.id)` / `cur.insert(...)` 顺序依赖(中间要留"供 stop 读取"
的临时插入),重构时需保持这个语义。

**风险**:中低(有现成模板,但 proxy 分支的插入/删除顺序比 visitor 分支
复杂一点)。

**未测**:没有基准。#6 的测试已证明这类锁问题在单测里**抓不住**(见 §6
的"能力边界"说明),`upsert` 同理 —— 收益依赖生产环境的并发建连压力。

---

## 16. 任务书之外发现的额外串行点

以下 4 处是在完成 12 项之后继续扫描时发现的。

> **2026-10-23 更新**:**A / B / C 三项均已完成**(收尾批次 P4 / P2 / P3)。
> 本节保留原始的发现记录与判断依据,各项已标注提交号与实测结果。
> 详见 §17 的收尾批次汇总。

### A. `notify_client_recovery` 的批量恢复循环仍串行

> **★已修**:commit `37a070d`(收尾批次 P4),改 `for_each_concurrent(16)`。
> ★**本机测不出收益**(N=1000:串行 30.14ms vs 并发 30.54ms,与 §2 的
> recovery 结论一致)。真实收益依赖文件型 SQLite 与在线会话,未在部署
> 环境验证。如实记录,不声称提升。

**位置**:`crates/rh-server/src/runtime.rs:526-536`

```rust
for (_id, _cid, mut spec) in proxies {
    if !spec.enabled { continue; }
    { let l = state.limits.read()...; normalize_spec(&mut spec, ...); }
    let _rt = spawn_runtime(state, client_id, &spec, revision).await;   // ← 串行
    info!(proxy = spec.name, "recovered after client enable");
}
```

**为什么算遗留**:任务书 §2 #1 明确点名了这个函数 ——

> `notify_client_recovery`(`runtime.rs:531`):`for ... { spawn_runtime(...).await }`

并把它归为"N 次 × 每次 O(N)"的路径之一。但 #1 的改动只覆盖了
`spawn_runtime` **内部**的 vhost 重建(改成 `schedule_vhost_rebuild()`),
**这个循环本身没有并发化**。所以现在的状态是:每次 `spawn_runtime` 不再做
O(N) 重建(这已经是最主要的收益,§1 实测 N=2000 时 746ms → 0.46ms),
但 N 条规则仍然是 N 次**串行** `spawn_runtime`。

**当前复杂度**:O(N · t),t = 单条 `spawn_runtime`(含 1 次 `get_client`
+ bind + serve loop 启动 + mpsc 发送)。

**与 #2(recovery)的关系**:形态完全同构,`recovery.rs` 已经改成
`for_each_concurrent(16)`,这里没有。

**并发是否安全(关键判断)**:比 `recovery.rs` **更安全**。

- `recovery.rs` 里要处理 `port_auto` 被外部占用时的 `allocate_port` 重分配,
  所以必须把端口决策留在串行段;
- `notify_client_recovery` **完全没有** `allocate_port` 调用 —— 端口直接取
  DB 里已有的 `spec.bind_port`。两条规则不会争同一个端口(DB 里本来就是
  唯一的),所以整个循环都可以并发。
- 调用点 `update_client`(`api.rs:622`)持 `serial_lock(&id)`,并发化不会
  引入跨客户端竞态;而 `spawn_runtime` 本身不取 `serial_lock`,不会自锁。

**建议改法**:照抄 `recovery.rs` 的 `for_each_concurrent(RECOVERY_CONCURRENCY)`。
风险:低。收益:批量启用一个 N 条规则的客户端时,N 次串行变 N/16 次。

**未测**:没有为它做基准。按 §2 的实测结论(本机 `:memory:` 环境下并发化
测不出收益),预期同样测不出差异 —— 真实收益依赖文件型 SQLite 与在线会话。

---

### B. `api.rs` 的 admin handler 全部用裸 `send`(11 处,`update_client` 是其中 2 处)

> **★已修**:commit `64c6d70`(收尾批次 P2)。11 处一次性统一改为
> `send_bounded(msg, CONTROL_SEND_TIMEOUT)`。测试有区分力:**反向验证**
> 把其中一处还原成裸 `send` 后,测试在 10s 超时处失败(`Elapsed(())`)。
> 修后队列满时 2.2s 内返回。

**位置**:`crates/rh-server/src/api.rs`,11 处(完整清单见下方表格)

**以 `update_client` 禁用分支为例**(`api.rs:632-658`):

```rust
for (pid, _, _spec) in &proxies {
    teardown_runtime(&state, *pid as u64).await;
    if let Some(sess) = state.sessions.get(&id) {
        let _ = sess.send(ServerMessage::ServiceRemove(...)).await;   // ← 无界
    }
}
for (vid, _, _, _) in &visitors {
    if let Some(sess) = state.sessions.get(&id) {
        let _ = sess.send(ServerMessage::ServiceRemove(...)).await;   // ← 无界
    }
}
```

`Session::send`(`session.rs:246`)是 `self.queue_tx.send(msg).await` ——
**无界等待**,队列(`limits.control_send_queue`,默认 256)满时永久 park。

**★风险口径更正(重要,避免误判)**:

我最初把这里判为"A1 自锁形态",**这是错的**。核对后确认:

- `queue_tx` 的唯一消费者是 `run_session_loop` 的 `select!`(`control.rs:912`
  的 `qrx.recv()` 分支);
- 而 `api.rs` 的这 11 处都在**纯 HTTP handler** 里(挂在 axum 路由上),
  **不在**控制读循环的内联路径里 —— `handle_client_message`
  (`control.rs:987`)只处理客户端发来的 `ClientMessage`,不调用任何 admin
  CRUD。

所以这 11 处**都不会自锁**,真实后果是:队列满时该 HTTP 请求会挂到 axum/
反向代理的超时(通常 30~60s),期间 `serial_lock(&id)` 一直被持有,该客户端
的其它管理操作全部排队。

**★事实更正(本节原版有一处错误论据,已删改)**:

原版写「`enable_service_locked` / `disable_service_locked` /
`delete_service_locked` 都已经用了 `send_bounded(CONTROL_SEND_TIMEOUT)`,
**只有 `update_client` 的禁用分支漏了**」。**这是错的**,核查命令与结果:

```bash
$ grep -c send_bounded crates/rh-server/src/api.rs
0
$ awk '/async fn enable_service_locked/,/^}/' crates/rh-server/src/api.rs | grep -c send_bounded
0     # disable / delete 同理
```

`api.rs` 里**一处 `send_bounded` 都没有**。全仓的 `send_bounded` 调用点只在:

| 文件 | 行 | 宿主 |
|---|---|---|
| `nathole.rs` | 110, 112, 177, 178, 194 | XTCP 打洞 prepare/connect/fallback |
| `runtime.rs` | 345 | `spawn_runtime` |
| `runtime.rs` | 459 | `bind_error_retry` |
| `runtime.rs` | 542 | `notify_client_recovery` |
| `runtime.rs` | 579 | `publish_spec_in_place` |

原版把 `runtime.rs` 的这 4 处调用**误记成了 `api.rs` 的 `*_service_locked`**。

**真实情况**:`api.rs` 的 admin handler **全部**用裸 `send`,
`update_client` 不是「唯一漏点」,而是「11 处中的 2 处」。

**11 处裸 `send` 完整清单**(`grep -c "\.send(ServerMessage" crates/rh-server/src/api.rs`
= 11,逐行核对过):

| 行 | 宿主函数 | 消息 |
|---|---|---|
| 637 | `update_client`(禁用分支) | `ServiceRemove`(proxy) |
| 649 | `update_client`(禁用分支) | `ServiceRemove`(visitor) |
| 1452 | `disable_service` | `ServiceRemove` |
| 1498 | `delete_service` | `ServiceRemove` |
| 1694 | `disable_service_locked` | `ServiceRemove` |
| 1721 | `delete_service_inner` | `ServiceRemove` |
| 1858 | `create_visitor` | `ServiceUpdate` |
| 1914 | `update_visitor` | `ServiceUpdate` |
| 1977 | `toggle_visitor`(enable 分支) | `ServiceUpdate` |
| 1984 | `toggle_visitor`(disable 分支) | `ServiceRemove` |
| 2032 | `delete_visitor` | `ServiceRemove` |

> 原版写「还有 8 处」却列了 9 个行号(自相矛盾),且 `:1902` 是注释行,
> 正确行号是 `:1914`。上表已按 grep 结果重建。

**建议改法(统一处理,不逐个打补丁)**:

这 11 处风险相同、改法相同 —— 全部换成
`sess.send_bounded(msg, CONTROL_SEND_TIMEOUT)`。`CONTROL_SEND_TIMEOUT`
定义在 `runtime.rs:16`,值为 2s(`state.rs:19` 与 `nathole.rs:19` 各有一份
同名常量,同值)。语义等价:返回值本来就被 `let _ =` 丢弃,改动只是把
"队列满时永久等待"变成"最多等 2s"。

适合作为一个**独立提交**(11 处同构改动,review 时一眼能看完),而不是
按 handler 逐个打补丁 —— 逐个改会让"哪些路径还有裸 send"这个事实长期
不可判定。

**风险**:零。纯降级,且与 `runtime.rs` / `nathole.rs` 已有的 9 处口径对齐。

---

### C. `delete_client` 的 teardown 循环串行

> **★已修**:commit `699a1c8`(收尾批次 P3),改 `for_each_concurrent(8)`。
> ★**本节原来说"构造不出 2s 超时场景",这是错的**。只要往
> `rt.serve_handle` 塞一个永不返回且不响应 `rule_cancel` 的任务,单次
> teardown 就必然吃满 2s。实测 16 条卡住规则:**串行 32.07s(恰好
> 16×2s)vs 并发 4.23s**。这条测试有决定性区分力。

**位置**:`crates/rh-server/src/api.rs:706-709`

```rust
let proxies = state.db.list_proxies(Some(&id)).await.unwrap_or_default();
for (pid, _, _) in &proxies {
    crate::runtime::teardown_runtime(&state, *pid as u64).await;   // ← 串行
}
```

**为什么算遗留**:任务书 §2 #1 同样点名了这个调用点:

> `delete_client`(`api.rs:707`):`for (pid,..) in &proxies { teardown_runtime(...).await }`

#1 只改了 `teardown_runtime` 内部的 vhost 重建,循环本身仍是串行。

**单次 `teardown_runtime` 的代价**(`runtime.rs:484-514`):
- 取 `spec.read()`;
- 可能的 `leave_group`(取组的 `members` 写锁 + `serve_cancel` 读锁);
- `rule_cancel.cancel()`;
- **等 `serve_handle` 收敛,最多 2s**(`tokio::time::timeout(2s, h)`);
- `close_proxy_channels`(取 `pool.lock()` + `ready.lock()`);
- `invalidate_proxy_requests`(取 `pending_requests` 锁)。

最坏情况:一个持有监听但 serve loop 不收敛的客户端,N 条规则 = **N × 2s**。
删除一个有 100 条规则、若干条卡住的客户端,这个 HTTP 请求会挂 200 秒。

**当前复杂度**:O(N · t),最坏 t = 2s。

**并发是否安全**:需要谨慎,有两点要注意:

1. `teardown_runtime` 里的 `serve_handle` 2s 超时是**每条规则独立**的预算,
   不是共享预算。并发后总耗时 ≈ 2s(而不是 N × 2s),这是收益来源;
2. `state.proxies.runtimes.remove(&proxy_id)` 是 dashmap 操作,不同 key
   并发安全;`leave_group` 按组键加锁,同组的多条规则并发 teardown 会在
   组锁上排队(但不会死锁 —— 锁内没有嵌套的跨组等待)。

**建议改法**:`for_each_concurrent(8)`。并发度取 8 而非 16,因为每条规则
内部可能等 2s,并发度过高会让"同时持有大量 rule_cancel 与 fd 释放中"的
窗口变宽。风险:中低。收益:最坏情况从 N×2s 降到 ~2s。

**未测**:没有基准。这条的收益是"最坏情况"的,正常情况(serve loop 立即
收敛)下单条 teardown 只有几十微秒,N 条也就几毫秒 —— 用 `:memory:` +
无真实 listener 的测试环境**构造不出** 2s 超时的场景。

---

### D. `list_services`(任务书 #7)

已在 §7 说明未做。此处不重复。

---

### 小结

| 项 | 位置 | 当前复杂度 | 建议改法 | 风险 | 状态 |
|---|---|---|---|---|---|
| A | `runtime.rs:526` | ~~O(N·t) 串行~~ → O(N/16·t) | `for_each_concurrent(16)` | 低(比 recovery 更安全) | **已修** `37a070d` |
| B | `api.rs` 共 11 处(638/650/1453/1499/1695/1722/1859/1915/1978/1985/2033) | ~~无界等待~~ → ≤2s | 11 处统一 `send_bounded(2s)` | **零**(纯降级) | **已修** `64c6d70` |
| C | `api.rs:707` | ~~O(N·t),最坏 N×2s~~ → ~2s | `for_each_concurrent(8)` | 中低 | **已修** `699a1c8` |
| D | `api.rs:741` | ~~4N 次锁~~ → N 次 | 见 §7 | — | **部分修** `0d32269`(只做 `get` 合并) |

> 行号已按收尾批次后的实际位置更新(B 项 11 处各 +1)。

**优先级建议**:B(零风险、纯一致性)→ A(形态与已落地的 #2 完全同构)→
C(收益在极端场景,需谨慎)。D 维持不做。

**关于"要不要顺手改"**:这 4 处都不在任务书范围内。按仓库约定
(AGENTS.md「不要顺手优化任务书之外的地方」),本次只报告不改。

其中 B 的风险为零,且**必须 11 处一次性统一改**(理由见本节 B 的事实更正:
`api.rs` 里一处 `send_bounded` 都没有,逐个补会让"还有哪些裸 send"长期
不可判定)。建议单独一个提交。

---

## 17. 收尾批次:剩余 5 项(2026-10-23)

上一轮核查把 5 项列为未完成,本批次全部处理完毕。每项一个独立提交。

| 项 | 提交 | 复杂度变化 | 实测 / 说明 |
|---|---|---|---|
| P1 `upsert` proxy 分支持锁跨 await | `cb0be73` | 持锁跨 await → 锁内纯内存 | 见下「关于可测性」—— **测不出**,如实记录 |
| P2 api.rs 11 处裸 send | `64c6d70` | 永久 park → ≤2s | **反向验证**:裸 send 在 10s 超时失败;修后 2.2s 返回 |
| P3 `delete_client` teardown 并发 | `699a1c8` | O(N·2s) → ~2s | **16 条卡住规则:32.07s → 4.23s** |
| P4 `notify_client_recovery` 并发 | `37a070d` | O(N·t) → O(N/16·t) | **本机测不出**(30.14 vs 30.54ms),理由同 §2 |
| P5 `list_services` 4 次 get 合 1 | `0d32269` | 4N 次查找 → N 次 | N=1000 省 ~31µs(微秒级,本就是"可选") |

### 17.1 P1:关于 `stop_proxy` 那次「供 stop 读取」的插入

原代码是 `cur.remove -> cur.insert(id, old) -> stop_proxy -> cur.remove`,
中间那次插入的注释写着「供 stop 读取」。**核对结论:那是历史遗留。**

`stop_proxy` 的全部实现只有:

```rust
async fn stop_proxy(sess: &Arc<ClientSession>, id: u64) {
    if let Some(c) = sess.health_cancels.lock().await.remove(&id) {
        c.cancel();
    }
}
```

它**只**操作 `health_cancels`,完全不读 `proxies` 表。所以重构后直接去掉
那次往返,改为「先 remove 旧条目、再 insert 新 spec」,表内容一致。

### 17.2 关于可测性(如实记录两条"抓不住"的情况)

**P1 的锁测试抓不住缺陷。** 把代码临时还原成「持锁跨 await」后,
`proxies_lock_available_during_upsert` **依然通过** ——
`stop_proxy`/`start_health` 内部的 await 只有微秒级,被调度粒度淹没
(与上一轮 #6 同一个坑:阈值 200ms、实际差异 5~7ms)。
已把它降级为**功能性回归**(门槛放到 2s,只拦死锁/长期独占),并在注释里
写明「不再假装它守住了锁语义」。锁纪律由源码结构保证。

**P4 的测试测不出并发收益。** 反向验证确认:串行实现下 3 条测试同样全过。
它们的价值是"并发化没有改变结果"。已在注释里写明。

### 17.3 一处判断更正(任务书原判断有误)

任务书 §P3 写「构造不出 2s 超时场景(需要真实 listener 且不收敛)」。
**这个判断是错的。** `teardown_runtime` 等的是 `rt.serve_handle` 里那个
`JoinHandle`;只要塞一个**永不返回且不响应 `rule_cancel`** 的任务,就能
精确模拟"serve loop 不收敛",不需要真实 listener。

实测因此从"无法验证"变成**可验证且区分力极强**:

| 16 条卡住规则 | 总耗时 |
|---|---|
| 串行(修前) | **32.07s**(恰好 16 × 2s) |
| 并发 8(修后) | **4.23s**(2 批 × 2s) |

### 17.4 P5 的口径更正

报告 §7 与任务书早期都写「3 次 get」,**实际是 4 次** —— 多出来的是
`healthy` 字段的独立 `get`(`"healthy": state.proxies.get(...)`),
它不在那三个 await 块里,容易被漏数。已更正 §7 标题与正文。

### 17.5 本批次未做的事

- 没有统一 `CONTROL_SEND_TIMEOUT` 的三处重复定义
  (`runtime.rs` / `state.rs` / `nathole.rs`),按任务书要求超出范围。
  只把 `runtime.rs` 那处改成 `pub(crate)` 供 api.rs 复用。
- 没有动 `snapshot_state` 的 `try_read` 改造(§7 已否决的理由仍成立)。
- 没有动 rh-transport / mux.rs 批量聚合 / apply_sync 的 to_start 循环。


---

## 18. 第四批:新发现的 3 项 + 1 项待判断(2026-10-23)

本批 4 项都不在之前的清单里。每项一个独立提交。

| 项 | 提交 | 复杂度变化 | 实测 |
|---|---|---|---|
| Q1 `update_client` 禁用分支 teardown 循环 | `4c0d203` | O(N·2s) → ~2s | **32.06s → 4.03s**(16 条卡住规则) |
| Q2 客户端心跳 `send_msg` 自锁 | `a9eb0f5` | 可自锁 → 非阻塞 | **端到端复现不出**,见 §18.2 |
| Q3 `CONTROL_SEND_TIMEOUT` 三处重复 | `c695736` | 3 处定义 → 1 处 | 纯去重,零风险 |
| Q4 REMOVE 发送并发化 | `d6f5dd4` | N×2s → ⌈N/8⌉×2s | **送达数相同,耗时 16.26s → 2.22s** |

### 18.1 Q1:与 P3 同款的漏点

`update_client` 的禁用分支原先是「teardown 一条 → send 一条」交替循环,
teardown 串行。上一轮 P3 只改了 `delete_client`,漏了这个调用点。

**顺序变化**:拆成「先全部 teardown、再全部 send」后,REMOVE 帧从穿插发出
变成集中在 teardown 完成后发出。判断**不破坏语义**:

- REMOVE 的语义是「通知客户端删掉这条规则」,客户端收到后只是
  `sess.proxies.lock().await.remove(&id)`,不依赖服务端运行态是否还在;
- 若客户端在收到 REMOVE 前断线重连,与「交替结构下收到一半就断线」的差别
  只是少收几条 —— 重连后服务端用全量 ServiceSync 覆盖。

**实测**(16 条卡住规则,与 P3 同一套构造):

| | 总耗时 |
|---|---|
| 串行(修前) | **32.06s**(恰好 16 × 2s) |
| 并发 8(修后) | **4.03s** |

**一处测试自身的修正**:最初断言 `proxy.enabled == false`,这是错的 ——
`set_client_enabled` 只改 `clients` 表,不改 proxies 行的 `enabled`。
已改为断言 clients 表。

### 18.2 Q2:自锁判断成立,但**端到端复现不出来**

**自锁判断本身已核实无误**:
- (a) `send_rx` 的唯一消费者就是同一个 `select!` 的 `recv` 分支
  (grep 确认:全仓只有 `run_loop` 里一处 `send_rx.recv()`);
- (b) 队列容量 256(有限)。

所以「心跳分支 `send().await` + 队列满」确实会 park 在"等自己消费"上,
select 不再回顶。与服务端 A1 缺陷同形。

**但端到端自锁我试了三版构造都复现不出来**,每版的失败原因都查清了:

1. duplex 对端未保活 → `read_frame` 立刻 EOF,run_loop 在首次心跳前 break;
2. 保活但缓冲 4096 → `send_rx.recv()` 分支照常排空队列,队列永不真满;
3. 保活 + 缓冲 1 + server 不读 → `send_rx` 分支**先**取走队列里唯一那条,
   然后阻塞在 `write_all`,3s 后写超时(`watchdog = 3×heartbeat_s`)break。
   实测 loop 只迭代一次,**心跳分支从未执行**。

要复现「心跳 `send().await` 在满队列上 park」,必须让队列在心跳 tick 那一刻
恰好满、且 `send_rx` 分支恰好没在消费 —— 这是调度时序竞态,单测里造不稳。

**所以我不假装端到端测试是缺陷回归**,证据链改成两条:

- `try_send_prime_never_blocks`(原语层):满队列立即返回 false,不阻塞;
- `blocking_send_would_hang_on_full_queue`(**反向证据**):旧的 `send_msg`
  在同样场景下确实挂住(200ms timeout 捕获)。

二者合起来证明「换 try_send 是必要且有效的」。调用点是否用对,靠源码结构
+ 一条**功能性**回归(`cancel_terminates_run_loop_even_with_full_queue`,
满队列时 run_loop 不会永久卡死;注释里写明它抓不住缺陷)。

**顺带修掉的另一个独立错误**:原代码 `if !send_msg(...) { break; }` 把
「发送失败」一律当成会话结束。而队列满导致的失败是可自愈的背压,不该断连。
现在区分 `Full`(丢一拍,继续)与 `Closed`(接收端没了,break)。

**★未做的相关发现(建议下一批)**:`dispatch_frame` 里的 PING 应答
(`session.rs:1303`)与 `ack()`(`:1320`)同样在 `select!` 读分支的内联
路径上,同样有自锁风险。任务书只要求改心跳,本次没动。

### 18.3 Q3:三处重复定义合并,位置必须选 config.rs

任务书建议「runtime.rs 那处已是 `pub(crate)`,删另外两处改成 use」。
**这条路走不通**:`runtime.rs` 依赖 `state.rs`,若 `state.rs` 反过来
`use crate::runtime::CONTROL_SEND_TIMEOUT` 就是循环依赖。

`config.rs` 不依赖任何业务模块(state / runtime / nathole 都依赖它),
是唯一干净的中立位置。已核实 `grep -n "crate::state\|crate::runtime\|crate::nathole" config.rs`
无输出。

结果:定义 1 处(config.rs),导入 4 处(runtime / state / nathole / api),
24 个使用点不变。

### 18.4 Q4:实测推翻了「可能不做」的顾虑

**原顾虑**(任务书提出):并发 `send_bounded` 在队列满时会"同时等 2s、
然后同时失败",一次性丢掉 N 条 REMOVE;串行至少能渐进送达前几条。

**实测(用本仓库的 tokio,队列容量固定、不消费)**:

| 场景 | 串行送达 | 并发送达 | 串行耗时 | 并发耗时 |
|---|---|---|---|---|
| cap=3, n=10 | 3 | **3** | 14.02s | **2.01s** |
| cap=3, n=20 | 3 | **3** | 34.05s | **6.01s** |
| cap=10, n=10 | 10 | **10** | 41µs | 70µs |
| cap=10, n=20 | 10 | **10** | 20.03s | **4.01s** |

送达条数**完全相同**。原因:`send_bounded` 不重试 —— 串行时第一条撞满队列
等 2s 超时丢弃,第二条照样如此,**并不是"渐进送达"**。队列容量 k 决定能送
几条,送完之后串行和并发都只剩超时。所以并发严格更优。

**反向验证也印证**:把发送循环还原成串行,两条测试**依然全过**(送达数
一样),但耗时从 2.22s 涨到 16.26s —— 正是 N×2s vs ⌈N/8⌉×2s。

**顺序无要求**(已查协议):客户端 `remove_entity` 按 entity 独立处理,
无跨条依赖;所有 REMOVE 用同一个 revision,只用于 ApplyResult 回执,
不参与排序。

---

## 19. 仍未解决的事项(截至本批)

> **2026-10-23 更新**:第 1 项已在第五批修完(R1 `f27b6c7` / R2 `50180ec`,
> 见 §20)。下列其余项的状态未变。

1. ~~**客户端 `dispatch_frame` 里的 PING 应答与 `ack()`**~~ —— **已修**
   (第五批 R1 / R2)。顺带纠正:这两处**都测得出**,与前几批那些
   "抓不住缺陷"的测试不同 —— 因为 `dispatch_frame` 是 pub 的纯函数式接口,
   不需要跑整个 run_loop 就能喂帧进去断言。详见 §20。
2. **Q2 的端到端自锁测试复现不出来**(§18.2),目前只有原语层证据链。
3. #2 恢复并发 / P4 `notify_client_recovery` 并发:**本机测不出收益**,
   真实收益依赖文件型 SQLite 与在线会话,未在部署环境验证。
4. #10 的端到端 RTT 测量:未做,只有调度结构基准。
5. 基线 3 个 e2e 失败项:未修,超出范围。
   (`dnsquery::cache_tests` 的 60% flaky 已于 `f2f00c6` 修复,见 §13 ——
   不再是未解决项。)

---

## 20. 第五批:dispatch_frame 内联路径的两处自锁(2026-10-23)

两项都是第四批 Q2 修复时**发现但未动**的同源问题(记录在 §19 第 1 条)。
每项一个独立提交。

| 项 | 提交 | 改前 | 改后 |
|---|---|---|---|
| R1 PING 应答 | `f27b6c7` | `send_msg().await`(可永久 park) | `try_send_msg_err`(不阻塞) |
| R2 `ack()` | `50180ec` | `send_msg().await`(可永久 park) | 短超时 50ms(有界) |

### 20.1 R1:PING 应答改用 try_send

与 Q2 心跳完全同源:`dispatch_frame` 由 `run_loop` 的 `select!` 读分支内联
调用,而 `send_rx.recv()` 是 `send_tx` 的唯一消费者。队列满时 park →
select 不回顶 → 五条分支全失联。

**为什么可以直接丢**:服务端心跳判死是 90s(`control.rs` 的 `hb.tick` 只看
`last_frame.elapsed() >= 90s`),PING 间隔通常 30s,丢一两拍无感;下一拍
PING 到达时,只要队列已排空就会正常回。

**★这条测试与前几批那些"抓不住缺陷"的不同,它真的有效**:
`dispatch_frame` 是 `pub` 的纯函数式接口(只依赖 sess + control 流 +
payload),**不需要跑整个 run_loop** 就能喂一个 PING 帧进去断言。

- `ping_reply_does_not_block_on_full_queue`:队列容量 1 且已满,断言 500ms
  内返回。**反向验证**:改回 `send_msg().await` 后失败
  ("PING 应答在满队列上阻塞了超过 500ms"),改回来即通过。
- `ping_reply_delivered_when_space`:有空间时应答必须真的入队。

这也解释了为什么 Q2 的心跳端到端测试复现不出来(Q2 只能跑完整 run_loop,
调度时序不可控),而 R1/R2 可以 —— **差别在测试接口的粒度,不在缺陷性质**。

### 20.2 R2:ack 用有界超时,不是 try_send(本批核心判断)

**没有照抄心跳的 try_send**,因为丢 ACK 与丢心跳/丢 PING 应答性质不同:

> 丢心跳、丢 PING 应答 → **自愈**(下一拍就有)。
> 丢 ACK → **不自愈**(要等下一次配置变更或重连)。

**独立核实了服务端的行为**(不照抄任务书的 grep 结论):

| 核实项 | 结果 |
|---|---|
| `unconfirmed` 有无后台消费者 | **无**。`grep -rn "unconfirmed" rh-server/src/` 除定义/写入/retain 外无其它读取 |
| `confirm_applied` 返回值用途 | `runtime.rs:124` 的 `still_waiting` 被 `let _ =` **丢弃** |
| 服务端心跳是否检查未确认 | **不检查**。`hb.tick` 只判 `last_frame.elapsed() >= 90s` |
| `applied_revision` 有无后台读 | **无** |
| 有没有 ApplyResult 超时重发 | **没有** |

所以丢一条 `ok:true` 的 ACK,那条规则会停在 Pending,**直到下一次该客户端有
任何配置变更或重连**。管理员改完一条规则后若不再操作,界面会一直显示
"未确认" —— 业务上不可接受。

**三个方案的取舍**:

| 方案 | 做法 | 判断 |
|---|---|---|
| A | `try_send`,丢就丢 | **否**。丢 ACK 让规则长期卡 Pending |
| B | 短超时 50ms | **选它** |
| C | `spawn` 出去 | **否**,见下 |

**为什么不选 C(spawn)**:
1. `ack` 的语义是「本次配置已应用」,它在 `dispatch_frame` 里紧跟
   `apply_sync` / `upsert` / `remove_entity`。spawn 会拆开这条因果顺序,
   让 ACK 可能晚于下一条 `ServiceUpdate` 的 upsert 才发出,制造无谓乱序。
   (服务端 `confirm_applied` 是**单调**的:只有 `revision > applied` 才推进,
   `retain` 清 ≤ revision —— 所以乱序**无害**,但没必要制造。)
2. spawn 无法保证发出:会话收尾时未执行的 spawn 照样丢。

**选 B 的代价**:仍在 select 分支里 `await`,但有界 50ms。50ms 远小于
watchdog(3×心跳 = 30s);正常情况 `send` 是微秒级,根本走不到超时,
只有真正拥塞(大量 REMOVE 集中下发)才吃满 50ms 后放弃这一条。

**测试(有区分力,已反向验证)**:

- `service_update_returns_when_queue_full`:**走真实调用路径**
  (dispatch_frame → upsert → ack),队列容量 1 且已满,断言 3s 内返回。
  **反向验证**:改回裸 `send_msg().await` 后,在 3s 超时处失败
  ("满队列时 ServiceUpdate 处理被 ACK 永久 park")。
- `ack_delivered_when_space`:有空间时 ACK 必须真的入队,并解帧确认
  revision / ok 正确(防止超时参数包错导致永远不发)。
- `ack_timeout_does_not_corrupt_local_state`:丢 ACK 只影响服务端
  `confirmed_revision`,不污染客户端本地配置。

### 20.3 本批的附带收获

R1/R2 证明了一件事:**同一类缺陷,测试可行性取决于接口粒度**。
Q2 心跳只能跑完整 run_loop(调度竞态不可控,端到端复现不出来);
R1/R2 的宿主 `dispatch_frame` 是纯函数式 pub 接口,可以直接喂帧断言。
后续遇到类似情况,应优先找"最小可测的宿主函数",而不是硬啃端到端。
