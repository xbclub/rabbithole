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

| # | 缺陷 | 位置 | 复杂度变化 | 实测 |
|---|---|---|---|---|
| 1 | vhost 路由全量重建被放进单条规则生命周期 | `runtime.rs` 262/336/510/589 | O(N²) → O(N) | N=2000: 746.2ms → 0.46ms (1630x) |
| 2 | 启动恢复逐条串行 | `recovery.rs` 43/106 | O(N·t) → O(N/16·t) | **本机未测出收益**(见 §2) |
| 3 | `batch_services` 逐 id 串行 | `api.rs` 1527 | O(N·t) → O(N/8·t) | N=1000: 261.8ms → 157.6ms (1.66x) |
| 4 | `port_in_use` 跨 await 持 dashmap 分片守卫 | `state.rs` 1140 | 持锁窗口 → 0 | 旧实现 387ms 阻塞,新实现通过 |
| 5 | `allocate_port` 双重循环 × 每端口全表扫 | `state.rs` 1176 | O(P·N) → O(N + P) | N=2000: 241.4ms → 0.39ms (620x) |
| 6 | 客户端 `apply_sync` 持 `proxies` 锁跨 await | `rh-client/session.rs` 1294 | 持锁跨 await → 锁内纯内存 | **测试抓不住**(见 §6) |
| 7 | `list_services` 循环内 3 次 get + 3 await + 嵌套锁 | `api.rs` 741 | 3N 次锁 → N 次 | **未做**(见 §7) |
| 8 | `resolve_visitor_target` N+1 查询 | `api.rs` 1713 | O(N) 往返 → O(1) 往返 | N=1000: 31.08ms → 3.38ms (9.2x) |
| 9 | `tcp_refill` 逐条签发 + `add_pending_request` 全表重建 | `session.rs` 124 | O(P) 重建 → O(P) 原地 | P=1000: 40.71µs → 2.00µs (20x) |
| 10 | 多载体报到串行,每条等 2s ACK | `rh-client/session.rs` 864 | O(K·D) → O(K/4·D) | K=8: 386.0ms → 105.5ms (3.66x) |
| 11 | `build_sync_entities` 两次串行 DB 查询 | `state.rs` 1551 | O(2t) → O(t) | **未单独测**(见 §11) |
| 12 | `events.emit` 同步落库 | `events.rs` 15/19 | 业务路径含 DB → 纯内存 | N=1000: 31.91ms → 0.07ms (454x) |

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
> 修前一列为单次运行(其数值远大于抖动,不影响结论)。

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

---

## 7. 缺陷 7:`list_services` 循环内 3 次 get + 3 个 await + 嵌套锁

**★状态:未做。**

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

**结论**:在没有实测证据表明 `list_services` 是瓶颈的前提下,不做比做更安全。
如实记录为未完成项,不计入"已修复"。若后续确认列表页轮询有压力,建议只做
第一子项(复用 `rt`),并保持 `snapshot_state` 的阻塞语义不变。

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

### 已知的基线偶发失败(非本次改动引入)

`rh-client` 的 `dnsquery::cache_tests::concurrent_requests_share_one_query`
与 `second_resolve_hits_cache` 会偶发失败(约 6 次里 2 次),报
「10 个请求触发了 2 次真实解析」。

**已用 `git stash` 剥离本次改动后复现**,确认是基线问题。根因是测试隔离缺陷:
`QUERIES` / `DELAY_MS` / `RESULT_OK` 是进程级 `static`,由多个 `cache_tests`
共享;测试 panic 时 `teardown()` 不执行,污染后续测试。**未修**(超出任务范围)。

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

1. **#7 `list_services`**:未做。理由见 §7(改法涉及接口语义变化)。
2. **#2 恢复并发**:代码已改,但**本机未测出收益**。真实收益依赖文件型
   SQLite 与在线会话,未在真实部署环境验证。
3. **#10 端到端 RTT 测量**:未做。只有调度结构基准。
4. **#11**:未单独测(测试环境 `:memory:` 掩盖了收益)。
5. **基线 3 个 e2e 失败项 + DNS 缓存测试偶发失败**:未修,超出范围。

---

## 16. 任务书之外发现的额外串行点(仅报告,未改代码)

以下 4 处是在完成 12 项之后继续扫描时发现的。**本次只做报告,没有改代码**
(按用户指示)。逐处给出位置、现状、影响判断,以及我建议的改法与风险 ——
其中 B 项附了一个重要的**风险口径更正**。

### A. `notify_client_recovery` 的批量恢复循环仍串行

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

### B. `update_client` 禁用分支的两处无界 `send().await`

**位置**:`crates/rh-server/src/api.rs:632-658`

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
- 而 `update_client` 是**纯 HTTP handler**(`api.rs:548`,挂在 axum 路由上),
  **不在**控制读循环的内联路径里 —— `handle_client_message`
  (`control.rs:987`)只处理客户端发来的 `ClientMessage`,不调用任何 admin
  CRUD。

所以这里**不会自锁**,真实后果是:队列满时这个 HTTP 请求会挂到 axum/反向
代理的超时(通常 30~60s),期间 `serial_lock(&id)` 一直被持有,该客户端的
其它管理操作全部排队。

**同一个 handler 里的一致性缺口**:这条路径的兄弟函数
`enable_service_locked` / `disable_service_locked` / `delete_service_locked`
都已经用了 `send_bounded(CONTROL_SEND_TIMEOUT)`(2s 上限),**只有
`update_client` 的禁用分支漏了**。这是纯粹的不一致,不是设计取舍。

**建议改法**:两处 `sess.send(...)` 换成
`sess.send_bounded(..., CONTROL_SEND_TIMEOUT)`。语义等价(都是尽力而为,
返回值本来就被 `let _ =` 丢弃),只是把"永久等待"变成"最多 2s"。

**风险**:零。这是纯降级,与同文件另外三处对齐。

**顺带发现的同类裸 send(需要单独评估,不建议顺手改)**:
`api.rs` 还有 8 处裸 `send`(`:1452` `:1498` `:1694` `:1721` `:1858`
`:1902` `:1977` `:1984` `:2032`),分别在 `disable_service` /
`delete_service` / `create_visitor` / `update_visitor` / `delete_visitor` /
`enable_service` 等 handler 里。它们同样不在控制循环内联路径上,同样不会
自锁,但同样会在队列满时挂住 HTTP 请求。**属于同一类问题,但不在本次报告
的 4 处范围内** —— 如果要修,建议一次性把所有 admin handler 的裸 `send`
统一换成 `send_bounded`,作为独立的一批,而不是逐个打补丁。

---

### C. `delete_client` 的 teardown 循环串行

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

| 项 | 位置 | 当前复杂度 | 建议改法 | 风险 | 是否已改 |
|---|---|---|---|---|---|
| A | `runtime.rs:526` | O(N·t) 串行 | `for_each_concurrent(16)` | 低(比 recovery 更安全) | 否 |
| B | `api.rs:632/649` | 无界等待 | `send_bounded(2s)` | **零**(纯降级) | 否 |
| C | `api.rs:707` | O(N·t),最坏 N×2s | `for_each_concurrent(8)` | 中低 | 否 |
| D | `api.rs:741` | 3N 次锁 | 见 §7 | — | 否(§7 已说明理由) |

**优先级建议**:B(零风险、纯一致性)→ A(形态与已落地的 #2 完全同构)→
C(收益在极端场景,需谨慎)。D 维持不做。

**关于"要不要顺手改"**:这 4 处都不在任务书范围内。按仓库约定
(AGENTS.md「不要顺手优化任务书之外的地方」),本次只报告不改。其中 B 的
风险为零且是明显的一致性缺口,如果决定做,建议单独一个提交。
