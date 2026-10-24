# 任务书:修复「应并行/异步却串行」的性能缺陷

> **状态:已完成**(2026-10-24)。12 项全部落地,执行报告见
> [concurrency-serialization-fixes.md](concurrency-serialization-fixes.md);
> 该报告中的 2 处事实错误 + 1 处逻辑矛盾 + 1 处遗漏,另由
> [task-fix-report-errors.md](task-fix-report-errors.md) 更正。
>
> 本文是**当时**下发的任务书,保留用于追溯"为什么这么改"。
> **不要当作现行指令** —— 基线 `1b8bfd6` 早已不是 HEAD,文中行号全部漂移。

> 本文是一份**自包含**的任务书,可直接交给另一个 AI 执行。
> 基线提交:`1b8bfd6`(仓库 `rabbithole`,Rust workspace)。

---

## 0. 你的角色与目标

你是 Rust + tokio 异步并发方向的资深工程师。任务:修复本仓库中 **12 处「本应并行/异步、实际写成串行」** 的缺陷。

这些缺陷不改变功能语义,但会在**规则数 N 增大**或**并发操作**时把 O(N) 劣化成 O(N²),或让锁跨越 await 点阻塞无关任务。

**不要做的事**:不要重构架构、不要重命名公开接口、不要顺手"优化"任务书之外的地方。

---

## 1. 环境与前置

```bash
cd <repo-root>
git log --oneline -1          # 必须是 1b8bfd6 或其后代
cargo --version               # 需支持 edition 2021
```

**开工前必读**(仓库约定,`AGENTS.md`):

- 提交前**三道门必须全绿**:
  ```bash
  cargo fmt --all -- --check
  cargo clippy --workspace --all-targets -- -D warnings
  cargo test --workspace
  ```
- **不要 `git add -A`**。只 add 自己动过的文件,先看 `git status`。
- **不要把纯格式化与业务改动混在一个提交里**。
- 提交信息写**为什么**,不写**做了什么**(diff 自己会说)。

---

## 2. 缺陷清单

按严重度排序。每条给出:位置 / 现状 / 为什么错 / 修法 / 风险。

---

### 缺陷 1【高】vhost 路由全量重建被放进单条规则生命周期 → O(N²)

**位置**:`crates/rh-server/src/runtime.rs` 第 **262 / 336 / 510 / 589** 行

```rust
state.proxies.rebuild_vhost_routes().await;
```

四处分别位于 `spawn_runtime` 的 Disabled 分支、`spawn_runtime` 正常结束、`teardown_runtime`、`publish_spec_in_place`。

**现状**:`rebuild_vhost_routes()`(`crates/rh-server/src/state.rs:1244`)的实现是遍历**全部** runtime,逐条 `rt.spec.read().await`:

```rust
let runtimes: Vec<Arc<ProxyRuntime>> =
    self.runtimes.iter().map(|e| e.value().clone()).collect();
for rt in runtimes.iter() {
    let spec_guard = rt.spec.read().await;   // 每条一次 async 读锁
    // ... 构建 http/https/tcpmux 三张路由表
}
```

**为什么错**:`crates/rh-server/src/api.rs` 里有 17 处 `spawn_runtime`/`teardown_runtime` 调用。凡是「对一批规则做操作」的路径都是 `N 次 × 每次 O(N)`:

- `delete_client`(`api.rs:707`):`for (pid,..) in &proxies { teardown_runtime(...).await }`
- `update_client` 禁用分支(`api.rs:632`):同上
- `notify_client_recovery`(`runtime.rs:531`):`for ... { spawn_runtime(...).await }`

N=2000 时是 400 万次读锁 + 400 万次 route 重建。

**修法(二选一,优先 A)**:

- **A. 脏标记 + 合并重建**:在 `ProxyManager` 上加 `vhost_dirty: AtomicBool`。`spawn_runtime`/`teardown_runtime`/`publish_spec_in_place` 只 `store(true)`,不再直接 await 重建。由一个常驻任务(或 10ms 合并窗口的 `tokio::spawn`)检查脏标记并执行一次 `rebuild_vhost_routes`。
  - 注意:测试若依赖「spawn 后路由立即可用」会失败,需要在测试里显式 flush 或保留一个 `rebuild_now()`。
- **B. 批量路径显式合并**:把 `delete_client`/`update_client` 禁用分支/`recovery`/`notify_client_recovery` 改成「循环内只 teardown、循环外调一次 `rebuild_vhost_routes`」。单条路径保留原语义。
  - 改动面小,但需要给 `teardown_runtime` 加一个 `skip_rebuild: bool` 参数或拆出 `teardown_runtime_inner`。

**风险**:中。路由表短暂过期(窗口内新规则不可访问)。窗口必须很小(≤10ms)且**只在批量路径**放宽。

---

### 缺陷 2【高】启动恢复完全串行

**位置**:`crates/rh-server/src/recovery.rs:43`(循环),`recovery.rs:106`(`spawn_runtime`)

```rust
for (id, client_id, mut spec) in proxies {
    // 每条串行:get_proxy / allocate_port / get_client / spawn_runtime
    crate::runtime::spawn_runtime(state, &client_id, &spec, rev).await;
}
```

**为什么错**:每条规则串行执行至少 **3 次 DB 查询 + 1 次 `spawn_runtime`**(后者内部有 17 个 `.await`,含 `get_client`、`port_allowed`、`bind`、`rebuild_vhost_routes`)。N 条规则 = N × 单条耗时,叠加缺陷 1 后是 O(N²)。恢复期间规则不可用。

**修法**:

```rust
use futures::stream::StreamExt as _;
futures::stream::iter(proxies)
    .for_each_concurrent(16, |(id, client_id, mut spec)| {
        let state = state.clone();
        async move {
            // 原循环体
        }
    })
    .await;
```

**并发度取 16**:太高会抢端口(bind 是真实 IO),太低收益不明显。可做成常量并注释理由。

**额外优化**:循环内的 `state.db.get_proxy(id)` 和 `state.db.get_client(&client_id)` 可以**一次性批量预取**成 HashMap,避免 2N 次查询。

**风险**:中。`allocate_port` 在并发下可能给两条规则分配到同一端口 —— 检查 `create_bind_checked` 的 `serial_lock` 是否覆盖恢复路径。**若不覆盖,必须保持 allocate_port 串行**(可以先串行跑完端口分配阶段,再并发 spawn)。

---

### 缺陷 3【中高】`batch_services` 逐条串行

**位置**:`crates/rh-server/src/api.rs:1527`

```rust
for id in req.ids {
    let r = match req.action.as_str() {
        "enable"  => enable_service_inner(&state, id).await,
        "disable" => disable_service_inner(&state, id).await,
        "delete"  => delete_service_inner(&state, id).await,
        _ => (9000, ...),
    };
    results.push(...);
}
```

**为什么错**:前端「全选 → 批量启用/禁用/删除」打的就是这个接口。每个 `*_inner` 内部是 `get_proxy` + `serial_lock` + `bump_revision` + `teardown_runtime` + `spawn_runtime` + `events.emit`(同步落库)。选中 100 条 = 100 次串行全链路。

**⚠ 关键约束**:三个 `*_inner` 都会取 `state.serial_lock(&client_id)`。**同一客户端的规则必须串行**(防端口竞态、防规则双占),这是正确性要求,不能并发。

**修法**:按 `client_id` 分组,组间并发、组内串行:

```rust
// 1. 先一次性建 id -> client_id 映射(一次 list_proxies(None))
// 2. 按 client_id 分组
// 3. stream::iter(groups).for_each_concurrent(8, |(cid, ids)| async move {
//        for id in ids { /* 组内串行 */ }
//    })
```

**风险**:中。注意结果顺序 —— 前端可能依赖 `results` 与 `req.ids` 同序。**必须保持输出顺序**(用索引回填)。

---

### 缺陷 4【中高】`port_in_use` 跨 `.await` 持有 dashmap 分片锁

**位置**:`crates/rh-server/src/state.rs:1140`

```rust
for e in self.runtimes.iter() {          // ← dashmap::iter() 持分片写锁
    let rt = e.value();
    if Some(rt.id) == except_proxy { continue; }
    let spec = rt.spec.read().await;     // ← await 点,分片锁跨 await 持有
    ...
}
```

**为什么错**:`dashmap::iter()` 会持有**分片写锁**,而循环体内有 await 点。守卫跨 await 会长期占住该分片,其它 runtime 的增删/查找全被挡住。

**关键证据**:同文件的 `rebuild_vhost_routes`(`state.rs:1249-1253`)已经写明了这个警告并做了快照:

> ★先快照再遍历:dashmap 的 `iter()` 会持有**分片写锁**,而下面 `rt.spec.read().await` 是 await 点 —— 守卫跨 await 会长期占住该分片,其它 runtime 的增删/查找全被挡住。快照后立即释放。

`port_in_use` **就是**那段注释描述的形态,但没做快照。

**修法**:照抄同文件的快照写法:

```rust
let runtimes: Vec<Arc<ProxyRuntime>> =
    self.runtimes.iter().map(|e| e.value().clone()).collect();
for rt in &runtimes {
    let spec = rt.spec.read().await;
    ...
}
```

**风险**:低。纯收益,有现成模板。

---

### 缺陷 5【中】`allocate_port` 双重循环 × 每端口全表扫

**位置**:`crates/rh-server/src/state.rs:1176`

```rust
for r in ranges {
    for p in r.start..=r.end {                 // 默认 10000..=25000 = 15001 个
        if !allow.allows(p) { continue; }
        if self.port_in_use(proto, bind_ip, p, None, None).await { continue; }  // O(N_runtime)
        ...
    }
}
```

**为什么错**:默认 `auto_range` 是 `10000-25000`。端口被占得越多,前面跳过的越多,而**每个被跳过的端口都要付一次 O(N_runtime) 全表扫 + 读锁**。最坏 15001 × N_runtime。

**修法**:一次性构建「已占用端口集合」`HashSet<u16>`(快照一次 runtime 表,复用缺陷 4 的快照),循环内退化为 O(1) 查找。

**注意**:`port_in_use` 还有 `new_group_key` 与 `except_proxy` 参数,集合化时要保留同组共享监听的语义 —— 建议提取一个 `collect_occupied_ports(proto, ip, except_proxy, new_group_key) -> HashSet<u16>`,`port_in_use` 改为 `set.contains(&port)`,`allocate_port` 复用同一个 set。

**风险**:中。语义等价性需要测试覆盖(尤其 group 共享监听)。

---

### 缺陷 6【中】客户端 `apply_sync` 持有 `proxies` 锁跨 `await`

**位置**:`crates/rh-client/src/session.rs:1294`(取锁)~ `1314`(`start_health`)

```rust
{
    let mut cur = sess.proxies.lock().await;      // ← 持写锁
    ...
    for (id, spec) in &new_proxies {
        if cur.contains_key(id) { continue; }
        let mut spec = spec.clone();
        spec.normalize_kind_configs();
        cur.insert(*id, spec.clone());
        start_health(sess, *id, &spec).await;     // ← await(内部再取 health_cancels 锁 + spawn)
    }
}
```

**为什么错**:全量 `ServiceSync`(客户端每次重连必走)期间持有 `proxies` 写锁。任何需要读 `proxies` 的路径都被挡住 —— 例如 `open_data_channel` 里的 `sess.proxies.lock().await.get(&proxy_id)`(`session.rs:1447`)。表现为「重连后首批连接建立慢」。

**关键证据**:同文件的 `upsert`(`session.rs:1345` 附近)已经修过同类问题,注释写着「`drop(cur)`; // 别持着 visitors 锁去做重绑定」。`apply_sync` 漏了。

**修法**:锁内只收集,锁外启动:

```rust
let to_start: Vec<(u64, ProxySpec)> = {
    let mut cur = sess.proxies.lock().await;
    // ... 原有 drain/重填逻辑 ...
    let mut out = Vec::new();
    for (id, spec) in &new_proxies {
        if cur.contains_key(id) { continue; }
        let mut spec = spec.clone();
        spec.normalize_kind_configs();
        cur.insert(*id, spec.clone());
        out.push((*id, spec));
    }
    out
};  // ← 锁在这里释放
for (id, spec) in to_start {
    start_health(sess, id, &spec).await;
}
```

**注意**:`stop_proxy(sess, id).await` 在同一个循环的前半段,也要检查是否持锁调用 —— 它内部取 `health_cancels` 锁。若两把锁的获取顺序在别处相反,会死锁。

**风险**:中低。有现成模板。

---

### 缺陷 7【中】`list_services` 循环内 3 次 `get` + 3 个 await + 嵌套锁

**位置**:`crates/rh-server/src/api.rs:741`

```rust
for (id, client_id, mut spec) in rows {
    ...
    let (status, reason) = match state.proxies.get(id as u64) {
        Some(rt) => { let (st, r) = rt.snapshot_state().await; ... }   // await ①
    };
    let pool_ready = match state.proxies.get(id as u64) {               // 重复 get
        Some(rt) => match rt.pool.lock().await.as_ref() {               // await ②
            Some(p) => p.ready.lock().await.len(),                      // await ③ 嵌套锁
        }
    };
    let rt_meter = state.proxies.get(id as u64).map(...);               // 重复 get 第三次
```

**为什么错**:每条规则 3 次 `dashmap.get` + 3 个 await(含 `pool.lock()` 内嵌 `ready.lock()`)。前端服务列表页轮询时,N 条 = 3N 次锁操作串行。

**修法**:
- 一次 `get` 拿到 `rt` 复用;
- `snapshot_state` 内部是 `RwLock::read`,列表页允许读稍旧状态 → 改 `try_read`,失败时降级为上次状态或 `"unknown"`;
- `pool.lock()` + `ready.lock()` 的嵌套读改为单次获取后一次取 len。

**风险**:低。只影响展示接口。

---

### 缺陷 8【中】`resolve_visitor_target` N+1 查询

**位置**:`crates/rh-server/src/api.rs:1713`

```rust
let clients = state.db.list_clients().await.unwrap_or_default();
for c in &clients {
    if let Ok(list) = state.db.list_proxies(Some(&c.id)).await {   // ← 每客户端一次查询
        ...
    }
}
```

**为什么错**:每次创建/更新 visitor 执行 `1 + N_clients` 次 SQLite 查询。`create_visitor` 与 `update_visitor` 各调一次。

**修法**:一次全表查询后内存分组:

```rust
let all = state.db.list_proxies(None).await.unwrap_or_default();  // 一次
// 按 client_id 建 HashMap<&str, Vec<&ProxySpec>>,再在内存里匹配 name/kind
```

**风险**:低。语义不变。注意 `list_proxies(None)` 返回 `(i64, String, ProxySpec)`,client_id 在元组第二项。

---

### 缺陷 9【中】`tcp_refill` 池预热逐条串行签发

**位置**:`crates/rh-server/src/data.rs:103`

```rust
for _ in 0..n {
    pool.inflight.fetch_add(1, Ordering::Relaxed);
    ...
    match state.issue_create_channel_with(sess, ..., Some(hook)).await {
        Some(_rid) => {}
        None => { /* 归还 inflight */ break; }
    }
}
```

**为什么错**:每次签发要 `sess.add_pending_request().await`(内部两次取 `pending_requests` 锁 + 一次全表 `retain`)+ `sess.send().await`(mpsc 有界队列)。

**⚠ 不要直接并发化**:`add_pending_request`(`crates/rh-server/src/session.rs:124`)内部是 `std::mem::take` + 重建 BTreeMap 的全表扫描。并发化后锁竞争更重。而且 `inflight` 的失败回滚路径有历史陷阱 —— 代码注释明确写着:

> `issue_create_channel_with` 失败路径用的是 `take_pending_request_quiet`(故意**不**触发钩子),因为它假定调用方自己回收。所以这里不减就**谁都不减**:修前只 `break`,一次签发失败让池永久少一个名额。两边注释曾互相指认对方会减,实际都不减。

**修法(低风险优先)**:只优化 `add_pending_request` 的过期清扫 —— 把「`mem::take` + 全表重建」改成 `BTreeMap::retain`(Rust 1.53+ 提供),或改成「仅当 `m.len() > 阈值` 时才清扫」。这能把每次签发的锁内代价从 O(P) 降到 O(1)。

**并发化留待后续**,本任务不做。

**风险**:低(只改清扫策略)。必须补测试:过期项仍被正确摘除且钩子被调用。

---

### 缺陷 10【中】客户端多载体报到串行,每条等 2s ACK

**位置**:`crates/rh-client/src/session.rs:864`

```rust
for i in 1..initial_k {
    let (mut s, member_id) = mux.open_stream_tagged().await?;   // 串行开流
    frame::write_frame(&mut s, FrameType::HELLO, &payload).await?;
    let ack = tokio::time::timeout(Duration::from_secs(2),
                                   frame::read_frame(&mut s, &mut abuf)).await;  // 串行等 ACK
    ...
}
```

**为什么错**:`initial_k` 默认 2(TCP 基线),RTT ≥ 15ms 时可达 4。K 条载体**逐条**报到,每条最坏等 2s。一条卡住就拖住后续所有载体,进而拖住整个会话建立。这是**连接建立路径**,直接决定重连速度。

**修法**:`futures::stream::iter(1..initial_k).buffer_unordered(4)` 并发报到。

**⚠ 必须处理**:
- `failed_ids: Vec<u64>` 与 `server_caps: Vec<usize>` 的收集要改成并发安全(`Mutex<Vec<_>>` 或收集 `Vec<Result>` 后统一处理);
- `control_member_id` 不受影响(控制载体是单独开的流);
- **`server_caps` 的顺序不参与语义**(`plan_convergence` 只看 max),但要确认 `plan_detach_set` 不依赖报到顺序;
- 并发度上限 4(与 `initial_k` 的 clamp 上限一致)。

**风险**:中。需要补测试:部分载体失败时 `failed_ids` 完整、`plan_detach_set` 仍摘除正确身份。

---

### 缺陷 11【低中】`build_sync_entities` 两次串行 DB 全扫

**位置**:`crates/rh-server/src/state.rs:1551`

```rust
if let Ok(proxies) = self.db.list_proxies(Some(client_id)).await { ... }
if let Ok(visitors) = self.db.list_visitors(Some(client_id)).await { ... }
```

**为什么错**:两次独立查询串行。调用点包括 `SyncRequest` 处理(控制循环内联)、`on_apply_result` 失败重发、`notify_client_recovery`。

**修法**:

```rust
let (proxies, visitors) = tokio::join!(
    self.db.list_proxies(Some(client_id)),
    self.db.list_visitors(Some(client_id)),
);
```

SQLite 池是 8 连接(`db.rs:36` 的 `max_connections(if in_memory { 1 } else { 8 })`),并行查询安全。

**风险**:**零**。纯收益,一行改动。

---

### 缺陷 12【低中】`events.emit` 同步落库,所有调用方 `.await`

**位置**:`crates/rh-server/src/events.rs:15`(`emit`)、`:19`(`emit_detail`)

```rust
pub async fn emit_detail(&self, level: &str, source: &str, message: &str, detail: &str) {
    tracing::info!(level, source, message, "event");
    if let Err(e) = self.db.insert_event(level, source, message, detail).await { ... }
}
```

**为什么错**:每次业务操作都同步等一次 SQLite INSERT。调用点遍布 `api.rs`(创建/更新/删除/启停/登录)、`runtime.rs`、`control.rs`(上下线)、`nathole.rs`。在 `batch_services` 里让每条规则多付一次 INSERT;在 `recovery` 里同样。

而注释自己说「事件是旁路,不该让业务流程失败」—— 既然是旁路,就不该在业务路径同步等待。

**修法**:改成 `mpsc::unbounded_channel` + 单个 writer 任务批量写(`insert_many` 或事务批提交)。

**⚠ 必须处理**:
- **进程退出时 flush** —— 否则丢最后几条。在 `state.shutdown` 的收尾处 drain 一次;
- **测试兼容** —— 现有测试可能 `emit(...).await` 后立刻查 DB 断言。改成异步后需要提供 `flush_for_test()` 或把测试改为轮询;
- **背压** —— `unbounded` 在高事件率下会吃内存,但事件量级很小(心跳级 + 规则变更),可接受;若担心,用 `bounded(1024)` + 满了丢弃并计数。

**风险**:中。涉及进程生命周期与测试兼容性,**建议最后做**。

---

## 3. 明确**不要**动的地方

以下是刻意设计的串行/有界,改动会引入回归:

| 位置 | 原因 |
|---|---|
| `crates/rh-server/src/data.rs:3027` `copy_bidirectional_bounded` | 第二向必须在 `halfclose_grace` 内**有界**等待。无界并发会重蹈 round-40 的 FIN-WAIT-2 泄漏(实测 10 轮 `-P8` 累积 22 条、20s 后仍不消失、吞吐 2.4G→0.5G) |
| `crates/rh-server/src/data.rs:486` `copy_relay_pair` | 同上,`join_relay_pumps` 的有界 join 是刻意设计 |
| `crates/rh-server/src/control.rs:871` 服务端控制读循环 | 该循环**同时是** `queue_tx` 的唯一消费者。任何 `send().await` 都会自锁(A1 缺陷:等自己消费)。现有代码全用 `try_send`/`send_bounded` 是**正确**的 |
| `crates/rh-client/src/session.rs:1046` 客户端读循环 | 同款自锁风险,已用独立 send 任务规避 |
| `state.serial_lock(client_id)` 保护的全部路径 | 每客户端粒度的串行域,防端口竞态与规则双占,是**正确性要求** |
| `crates/rh-transport/src/tcp.rs:526` `collect_pool_members` | 已是 `FuturesUnordered` 并发。错误分支的 `now_or_never` 排空是防泄漏关键,不要简化成 `join_all`(会退化成慢失败) |
| `crates/rh-transport/src/mux.rs:2967` 重拨循环 | 重拨有退避语义,并发重拨会打爆服务端 |
| `vendor/yamux/` 全部 | 上游 vendored 代码,不在本任务范围 |

---

## 4. 执行顺序

按「收益/风险比」排序。**严格按序做,每个缺陷一个独立提交**:

| 批次 | 缺陷 | 理由 |
|---|---|---|
| 1 | **#4 + #5** | 改动小、有同文件模板、一次快照两处受益 |
| 2 | **#11** | 一行 `tokio::join!`,零风险 |
| 3 | **#8** | N+1 拍平,语义不变 |
| 4 | **#6** | 有 `upsert` 现成写法可抄 |
| 5 | **#1** | 收益最大但改动面宽,单独提交 |
| 6 | **#2 + #3** | **必须等 #1 做完**。否则并发化会把 O(N²) 重建变成并发争抢,可能更糟 |
| 7 | **#10** | 需重构 `failed_ids` 收集,要补测试 |
| 8 | **#9 + #12** | 收益小且有正确性陷阱,放最后 |

每完成一批,**立刻**跑三道门并提交,再进下一批。不要攒到最后一起验。

---

## 5. 验证要求

### 5.1 每批必跑

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

### 5.2 针对性验证

多数改动在**管理面/连接建立路径**,`benchmark/peer.sh`(吞吐基准)测不出来。按缺陷类型选:

- **#1 / #2 / #3**:`N=500` 与 `N=1000` 条规则,测**启动耗时**与**批量操作耗时**对比。
  参考 `docs/scale-inefficiency-fixes.md` 里 `apply_sync` 的测法。
- **#4 / #5**:纯函数基准 —— 给定 N 个 runtime,测 `port_in_use` 单次耗时。不依赖网络。
- **#10**:`RH_MUX_POOL_SIZE=4` + 人为延迟载体 ACK,测会话建立总耗时。

### 5.3 测试设计红线

按 `docs/pending-unified-review.md` §8.1:

- **不要引用被测常量**做断言(自证循环)。例如测 `pool_size=2` 时不要断言「签发了 2 次」,要断言「最终 ready 队列有 2 条」。
- **断言基于外部可观测行为**:耗时、连接数、最终状态、DB 行数。
- **纯函数测试抓不到调用点错误** —— 修完必须有一个端到端用例覆盖真实调用路径。

### 5.4 性能结论口径

若涉及跨机性能对比,必须走 `benchmark/peer.sh` / `benchmark/peer-frp.sh`(仓库约定),且 `THP_TOOL=loadgen` 必须显式给出。**不要自己另写压测脚本**。

但本任务的 12 项**都不在吞吐热路径**(是管理面/连接建立),预计 `peer.sh` 测不出差异。**不要为了交差去跑吞吐基准然后声称有提升** —— 拿管理面耗时说话。

---

## 6. 交付物

1. **代码改动**,按批次切分提交,每提交对应一个缺陷(或一对相关缺陷)。
2. **`docs/concurrency-serialization-fixes.md`**,记录:
   - 每项的改前/改后复杂度(如 `O(N²) → O(N)`);
   - 实测数据(管理面耗时对比);
   - 判断依据(为什么这样改、为什么不并发化某处);
   - **如实记录没测到的部分** —— 不要编数字。
3. 提交信息模板:
   ```
   perf(server): vhost 路由重建改为脏标记合并,避免批量操作 O(N²)

   修前 spawn_runtime/teardown_runtime 每次都全量重建 vhost 路由
   (遍历全部 runtime + 逐条 spec.read().await),而 api.rs 有 17 处
   调用点。批量操作 N 条规则 = N × O(N)。

   改为脏标记 + 10ms 合并窗口。单条路径行为不变。
   ```

---

## 7. 常见坑(来自本仓库历史)

- **`pkill -f` 会杀掉自己的 shell**。命令行含 `rh-client` 字样时用精确匹配:
  ```bash
  ps -eo pid,comm | awk '$2 ~ /^(rh-server|rh-client|loadgen)$/{print $1}' | xargs -r kill
  ```
- **`cargo build` 不重建 test 二进制**。改完源码必须 `cargo test --no-run` 再跑测试二进制。
- **`std::os::fd` / `std::os::unix` 必须放在 `#[cfg(unix)]` 内**。`#[cfg(test)]` 里的代码同样参与 Windows 编译。
- **`RH_MUX_POOL_SIZE` 与 `RH_DNS_TTL_SECS` 是 `OnceLock`**,进程内改 env 不生效,测试要另想办法。
- **`dashmap::iter()` 持分片写锁** —— 循环体内不得有 await 点(本任务 #4 就是这个问题)。

---

## 8. 一句话总结

按第 4 节的批次顺序,逐批修、逐批验、逐批提交。**不要跳批次**(#2/#3 依赖 #1 先落地)。遇到判断困难的地方,**在提交信息里写清依据**,不要悄悄改掉。
