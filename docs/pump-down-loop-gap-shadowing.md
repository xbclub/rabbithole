# pump_down 的 __last_read_end 变量遮蔽

一句话结论:`pump_down` 循环体末尾用 `let __last_read_end = ...` **重新绑定**了同名局部变量,外层那个永远停在 `None`,导致 `LOOP_GAP_NS`/`LOOP_GAP_N` 恒 0、`PUMP_GAP` 永不打印。已改为对外层赋值,并有实测对照证明 `PUMP_GAP` 从"没有输出"变成"`n=5 avg_us=951173`"。数据路径未改。

## 0. 身份

- 仓库 `rabbithole`,起点 HEAD `90f6f6b`(工作树另含前几轮未提交改动)。
- 改动文件:`crates/rh-transport/src/mux.rs`(纯函数 `loop_gap_step` + 调用点修正 + 测试)。
- 本机 macOS arm64 / rustc 1.98.1。

## 1. 核实(动手前先确认任务描述属实)

任务描述的三处行号与实际完全一致:

| 描述 | 实际 |
|---|---|
| `:1096` 外层声明 | `let mut __last_read_end: Option<Instant> = None;` ✓ |
| `:1123` 读旧值累加 | `if let Some(prev) = __last_read_end { ... }` ✓ |
| `:1211` 循环体末尾重新绑定 | `let __last_read_end = if pump_diag::enabled() { ... }` ✓ |

**最小复现**(独立 rustc 程序)确认遮蔽行为:三轮全部输出 `saw None`;去掉 `let` 后变成 `None / prev / prev`。

### 为什么这个 bug 能长期存在

恢复遮蔽后 `cargo build` **没有任何告警**:

- 外层 `__last_read_end` 仍被读取(`if let Some(prev) = ...`)→ 不构成 unused;
- 内层 `__last_read_end` 仍被赋值 → 不构成 unused;
- 两者互不干扰,编译器无从发现。

只有 `#[allow(unused_assignments)]` 之类的 lint 才会碰到,但这里的内层变量确实被"读"过一次吗——不,它是 `let x = f(...)`,赋值本身即定义,连 unused_assignments 都不触发。

## 2. 改法

### 抽出纯函数

```rust
#[inline]
fn loop_gap_step(
    prev: Option<std::time::Instant>,
    enabled: bool,
) -> Option<std::time::Instant> {
    if let Some(prev) = prev {
        pump_diag::LOOP_GAP_NS.fetch_add(prev.elapsed().as_nanos() as u64, Relaxed);
        pump_diag::LOOP_GAP_N.fetch_add(1, Relaxed);
    }
    if enabled { Some(Instant::now()) } else { None }
}
```

把"读旧值 → 累计 → 写新值"收进一个函数,既可单测,也让调用点只可能写成赋值或 `let`——两种写法在代码审查时一眼可辨。

### 调用点

```rust
// 修前
let __last_read_end = loop_gap_step(...);   // ← 遮蔽,外层永远 None

// 修后
__last_read_end = loop_gap_step(__last_read_end, pump_diag::enabled());
```

`Instant` 是 `Copy`,直接赋值无借用冲突。已确认赋值点之后循环体内再无该变量的引用(逐行核查)。

## 3. 验收

### 纯函数测试(4 个)

| 测试 | 覆盖 |
|---|---|
| `loop_gap_observes_previous_round` | 第一轮无 prev(计数 0),**第二轮必须观测到**(计数 1),第三轮 2 |
| `shadowed_rebind_would_keep_counter_zero` | 反证:每轮从 `None` 起(遮蔽等价)计数恒 0 |
| `disabled_produces_no_timestamp` | 红线:关闭时不产生时间点、不累加(零开销) |
| `toggle_off_stops_recording_new_stamps` | 开启→关闭→重开 的迁移语义 |

### 一个重要的测试教训

**纯函数测试抓不到调用点的遮蔽。** 我把 `let` 恢复回去跑了一遍:

```
test loop_gap_observes_previous_round ... ok
test shadowed_rebind_would_keep_counter_zero ... ok
```

全绿。因为纯函数本身没写错,错在**调用点用了 `let`**。

所以补了一条端到端测试 `pump_down_records_loop_gap_end_to_end`:真实起 mux、真实跑 `pump_down`、直接断言 `LOOP_GAP_N > 0`。这条才是唯一能抓到遮蔽的:

```
# 遮蔽版
test pump_down_records_loop_gap_end_to_end ... FAILED
LOOP_GAP_N 必须 > 0(修前是遮蔽导致恒 0);实际 n=0

# 修复版
test pump_down_records_loop_gap_end_to_end ... ok
```

> 该测试依赖 `RH_PUMP_STATS=1`(`enabled()` 是进程级 `OnceLock`,测试内无法切换)。
> 未开启时**显式 skip 并打印原因**,不假绿。

### 实跑证明 PUMP_GAP 真的打印

临时 example(已删除)用真实 mux + `mux_relay` 驱动 `pump_down`,消费端慢速读制造多轮:

| 版本 | reads | PUMP_GAP 行 |
|---|---|---|
| **遮蔽版**(修前) | 6 | **无输出** |
| **修复版** | 6 | `PUMP_GAP down n=5 avg_us=951173` |

两版 `reads=6`、数据量相同(`consumed=2097152 bytes`),证明**数据路径未受影响**,只有诊断输出不同。

## 4. 红线核对

| 红线 | 状态 |
|---|---|
| 不改 pump 数据路径 | 只改了循环末尾一行赋值与位置挪动的累计逻辑;实测 `reads`/`bytes` 两版相同 |
| 诊断关闭时零开销 | `loop_gap_step` 在 `enabled=false` 时**不调用** `Instant::now()`;有 `disabled_produces_no_timestamp` 断言 |

全量:`cargo test --workspace` 16 套件全绿(连跑 3 次,`exit=0`,零失败);`cargo clippy --workspace --all-targets -- -D warnings` 通过;`tests/e2e.sh` **PASS=82 FAIL=0**。

## 5. 顺带修掉一个反复出现的测试缺陷

`probe_result_matches_thresholds` 又在全量跑时失败过一次。上一轮我以为改成 `thread_local` 就解决了,**判断错了**:

- `probe_set_dial` 注入的函数指针、`DIALS` 计数都还是**进程级全局**;
- 而 `#[tokio::test]` 不保证整个测试跑在同一线程(multi_thread 下会跨 worker 迁移),`thread_local` 在多线程场景下 set/read 可能不是同一份状态。

正确组合是**全局原子 + 一把 async 锁把「设置 → 运行 → 断言」整个窗口串行化**。已改回全局原子(`DIALS`/`DIAL_MS`/`DIAL_OK`/`LAST_PROXY`),由 `PROBE_LOCK` 保护。改后连跑 5 次 `cargo test -p rh-transport --lib` 全绿。

> 教训:上一轮我把"偶发失败"归因为"线程局部即可隔离",没有验证 `#[tokio::test]` 的线程模型。这次用"连跑多次 + 落盘看 exit code"的方式验证,而不是靠单次 grep 计数。

## 6. 未做的部分(如实)

- **`pump_up` 没有同款 gap 统计**。`pump_up` 里完全没有 `LOOP_GAP` 相关代码,所以不存在同款遮蔽;也没有被要求加。
- **只修了 `pump_down` 这一处**。全仓搜索 `__last_read_end` 仅此三处(声明/读取/赋值),无其它同名遮蔽。
- **`PUMP_GAP` 的 avg 值仅供参考**。实测 `avg_us=951173`(≈951ms)是"慢速消费 + 写阻塞"的场景,不代表生产;这里只用它证明"打印了、且有非零值"。
