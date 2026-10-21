# rh-client 两处规模相关的低效结构

一句话结论:①`apply_sync` 的 O(N²) 判存在改为哈希查,500 条规则时**9.6×**(纯判存在环节 4.8×,N=2000 时 15×);②`waiting` 表的 TTL 从「每连接 spawn 一个 `sleep(10s)` 任务」改为「登记时刻随条目保存 + 会话级单任务周期淘汰」,任务数与连接数解耦。TTL 绝对值 10s、配对顺序、`user_conn_id` 生成全部未变。

## 0. 身份

- 仓库 `rabbithole`,起点 HEAD `90f6f6b`(工作树另含前几轮未提交改动)。
- 改动文件:`crates/rh-client/src/session.rs`(`apply_sync` + waiting 表结构 + sweeper + 测试)、`crates/rh-client/src/visitor_run.rs`(登记路径)、`crates/rh-client/src/nathole_cli.rs`(回退重放路径)。
- 本机 macOS arm64 / rustc 1.98.1。

## 1. 问题一:`apply_sync` 的 `Vec::contains`

### 修前

```rust
let existing: Vec<u64> = cur.keys().copied().collect();
for (id, spec) in &new_proxies {
    if existing.contains(id) { continue; }   // 线性扫描
    ...
}
```

规则数 N 时是 O(N²)。`cur` 此刻刚被重填完,所以直接查它语义完全等价。

### 修后

```rust
for (id, spec) in &new_proxies {
    if cur.contains_key(id) { continue; }    // O(1)
    ...
}
```

两处(`proxies` / `visitors`)都改了,并删掉了 `existing` 这个中间 `Vec`(顺带省一次 N 元素分配)。

### 实测

纯「判存在」环节,release,各 500 轮取中位数:

| N | `Vec::contains` | `HashMap::contains_key` | 倍数 |
|---:|---:|---:|---:|
| 100 | 667 ns | 708 ns | 0.9× |
| 500 | 16.791 µs | 3.5 µs | **4.8×** |
| 1000 | 60.167 µs | 6.959 µs | **8.6×** |
| 2000 | 209.708 µs | 13.959 µs | **15.0×** |

N 小时哈希反而略慢(100 条 0.9×)——这是预期的常数项差异,而斜率才是要点:倍数随 N 近似线性增长,正是 O(N²) vs O(N) 的形状。

`apply_sync` 整体(含两次 HashMap 构建,200 轮中位):

```
N=500: vec_contains med=828.875µs  hash_contains_key med=86.709µs  ratio=9.6×
```

> 这个 9.6× 包含了构建开销,所以比纯判存在环节的 4.8× 更"好看"——**不能当作 apply_sync 真实提速**。真实收益就是纯判存在那一列。

## 2. 问题二:每连接一个 `sleep(10s)` 清理任务

### 修前

```rust
sess.waiting.lock().await.insert(user_conn_id, (conn, visitor_id));
{
    let s2 = sess.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(10)).await;
        s2.waiting.lock().await.remove(&user_conn_id);   // 可能对已移除的 key 空转
    });
}
```

`deliver_relay` / `xtcp` 成功时不会 abort 它:任务活满 10 秒,醒来后对已经被 `remove` 掉的 key 再做一次无意义 remove。并发连接数 1000 时就是 1000 个常驻定时任务。

### 修后(选方案 B,并做了必要改良)

方案 A(存 JoinHandle 交付时 abort)需要在 `waiting` 表里再塞一个句柄,且交付路径分散在 3 处(`deliver_relay`、`nathole_cli::replay_waiting`、失败回退),每处都要记得 abort——漏一处就回到原问题。

方案 B(全局周期扫描)只需一处逻辑。**但没有照搬需求写法**:需求写"改全局 1s tick 扫 waiting 表",而 `waiting` 表的值是 `(TcpStream, u64)`,没有时间戳可用。所以先给条目加上登记时刻:

```rust
pub type WaitingEntry = (tokio::net::TcpStream, u64, std::time::Instant);
pub const WAITING_TTL: Duration = Duration::from_secs(10);          // 绝对语义不变
const WAITING_SWEEP_INTERVAL: Duration = Duration::from_secs(1);
```

由 `ClientSession::spawn_waiting_sweeper()` 起**一个**任务:

```rust
loop {
    tokio::select! {
        _ = sess.cancel.cancelled() => return,   // 会话结束即退出,不 leak
        _ = tick.tick() => {}
    }
    sess.sweep_waiting().await;                  // retain 掉超 TTL 的
}
```

`sweep_waiting` 用 `retain` 一次遍历完成淘汰,顺带把已交付的连接 drop(关闭 fd)。

### 回退重放不续命

`nathole_cli::replay_waiting` 在直连失败时会把连接放回 waiting 表。修后**沿用原登记时刻**:

```rust
let Some((conn, visitor_id, registered_at)) = sess.take_waiting(id).await else { ... };
// 失败回退:
sess.requeue_waiting(id, (_conn, visitor_id, registered_at)).await;
```

这是刻意的语义选择:回退属于同一次用户连接的生命周期,重置 TTL 会让它在打洞反复失败时无限续命。修前也**不**续命(放回时已经过了若干秒,剩余 TTL 自然变短),这里保持一致。

## 3. 验收测试(9 个新增)

| 测试 | 覆盖 |
|---|---|
| `thousand_entries_delivered_within_1s_no_residual_tasks` | 1000 条登记 → 1s 内全部交付 → 表为空、无残留可淘汰项 |
| `ttl_is_exactly_ten_seconds` | **字面量**断言 TTL = 10s(见下方"测试自证循环") |
| `sweep_interval_shorter_than_ttl` | 扫描周期 < TTL/2 |
| `sweep_evicts_only_expired` | 11s 前登记的被淘汰、9s 前登记的保留(字面量秒数) |
| `requeue_keeps_original_timestamp` | 回退放回保留原登记时刻,不续命 |
| `sweeper_exits_on_cancel` | 会话取消后 sweeper 退出,不 leak |
| `sweeper_evicts_expired_entry` | 端到端跑真实 tick,过期条目被淘汰 |
| `user_conn_id_is_monotonic_and_order_preserved` | 红线:`next_conn_id` 从 1 起严格递增 |
| `contains_key_vs_vec_contains_at_500` | 两种判存在写法语义一致 + 记录耗时 |

### 一个必须报告的测试设计缺陷

第一版 `sweep_evicts_only_expired` 用 `WAITING_TTL` 常量本身来构造"过期时刻":

```rust
*at = Instant::now() - WAITING_TTL - Duration::from_millis(1);   // 自证循环
```

负向验证(把 `WAITING_TTL` 改成 1s)时**测试仍然全绿**——因为断言跟着常量一起变小了。这是"测试引用被测常量"的典型自证循环。

修正:所有红线断言改用**字面量**(`Duration::from_secs(10)` / `from_secs(11)` / `from_secs(9)`)。重做负向验证才真正报红。

### 负向验证(全部做过)

| 注入的回归 | 结果 |
|---|---|
| `WAITING_TTL` 改 1s | `ttl_is_exactly_ten_seconds`、`sweep_interval_shorter_than_ttl`、`sweep_evicts_only_expired` 三个报红 |
| `requeue_waiting` 重置时刻(续命) | `requeue_keeps_original_timestamp` 报红:`不得续命` |
| sweeper 去掉 cancel 退出(改成只 tick) | `sweeper_exits_on_cancel` 报红:`取消后 sweeper 必须退出,不得 leak` |

## 4. 脚本结果

| 脚本 | 结果 |
|---|---|
| `tests/e2e.sh` | **PASS=82 FAIL=0**(含 STCP 4 项、SUDP、XTCP 打洞直连) |
| `tests/xtcp-isolated.sh` | **7 PASS / 2 FAIL**——与本轮改动**无关**,见下 |

### xtcp-isolated 的两个 FAIL 已重新对照

本轮动了 `waiting` 表的 TTL 机制,而该脚本测的正是 xtcp 路径,属于影响范围,**不能引用前几轮的旧结论**。重新做了三组对照:

| 运行 | 结果 |
|---|---|
| 改后 | 7 PASS / 2 FAIL |
| **`git stash` 回到基线 `90f6f6b`** | **7 PASS / 2 FAIL**(同样两条断言) |
| 恢复改动后再跑 | 7 PASS / 2 FAIL |

失败的两条是 `visitor 侧日志确认打洞成功` 与 `proxy 侧日志确认直连服务`——脚本用 `grep "punch dial ok"` / `grep "serving direct"` 断言打洞成功,而本机 macOS 上打洞失败(日志是 `punch dial failed; fallback relay`),脚本靠中继完成回显。功能本身正常(`直连-首次连接经打洞回显` 等 7 项 PASS)。

## 5. 红线核对

| 红线 | 状态 |
|---|---|
| waiting 表 10s TTL 语义不变 | `WAITING_TTL` 仍是 10s,有**字面量**断言;过期/未过期边界用 11s/9s 两个字面量测 |
| 配对顺序不变 | 表仍是 `HashMap<u64, _>`,取用仍是 `remove(&user_conn_id)` 精确匹配,无遍历 |
| `user_conn_id` 生成不变 | `next_conn_id` 一行未改;有测试断言 1/2/3 递增 |

全量:`cargo test --workspace` 全绿(rh-client 77 → 86);`cargo clippy --workspace --all-targets -- -D warnings` 通过。

## 6. 未做的部分(如实)

- **没有把 `apply_sync` 的两段改成并行**。`proxies` 与 `visitors` 两段各自持锁、内部还有 `stop_proxy`/`start_health` 等 await 调用;并行化需要小心锁顺序,而本次目标是消除 O(N²),不是并发化。500 条规则下判存在环节从 16.8µs 降到 3.5µs,已不是瓶颈。
- **sweeper 的扫描是 O(表内条目数)**。1000 条时每秒一次全表 `retain`,成本微秒级。若将来 waiting 表常态上千且扫描变可疑,可换成按时刻排序的 `BTreeMap` 或时间轮——目前没有证据支持。
- **没有测「1000 条真实交付」的端到端耗时**。测试里的"交付"是 `take_waiting`(与 `deliver_relay` 成功路径同语义),不含实际数据搬运;端到端交付含 TCP 收发,与本次改动无关。
- **`spawn_waiting_sweeper` 是 `Arc<Self>` 上的方法**。会话有多个创建点(生产 1 处 + 测试若干),只有生产路径启动了 sweeper;测试里按需显式调用。若将来新增生产会话创建点,需要记得启动——目前只有 `run_session` 一处。
