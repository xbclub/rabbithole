# 任务书:更正「并发/串行化修复报告」的 4 处问题

> **状态:已完成**(2026-10-24)。4 项全部落地,对应提交 `3a0d205`(§16B 归属更正)
> 与 `9a68def`(补记任务书之外的 4 处串行点)。
>
> 本文是**当时**下发的任务书,保留用于追溯"报告为什么被改过"。
> **不要当作现行指令** —— 基线 `9a68def` 早已不是 HEAD。

你是 Rust + tokio 异步并发方向的资深工程师。仓库 `rabbithole`（Rust workspace，
当前 HEAD `9a68def`）。

上一轮有人完成了 12 项「本应并行/异步却串行」的修复，并写了报告
`docs/concurrency-serialization-fixes.md`。**报告已经过独立核查**：8 项修后数字
可复现、测试真实存在、未完成项披露诚实。但核查发现 **2 处事实错误 + 1 处逻辑
矛盾 + 1 处遗漏**。

你的任务是**只修这些问题**，不要重跑基准、不要改已通过的代码、不要重新审视
12 项修复本身。

---

## 任务 1：修正报告 §16B 的事实错误（`send_bounded` 归属）

**位置**：`docs/concurrency-serialization-fixes.md` 第 593-596 行

**报告现在写的**：

> 这条路径的兄弟函数 `enable_service_locked` / `disable_service_locked` /
> `delete_service_locked` 都已经用了 `send_bounded(CONTROL_SEND_TIMEOUT)`
> （2s 上限），**只有 `update_client` 的禁用分支漏了**。

**核查结果：这是错的。**

```
$ awk '/async fn enable_service_locked/,/^}/' crates/rh-server/src/api.rs | grep -c send_bounded
0
$ awk '/async fn disable_service_locked/,/^}/' crates/rh-server/src/api.rs | grep -c send_bounded
0
$ awk '/async fn delete_service_locked/,/^}/' crates/rh-server/src/api.rs | grep -c send_bounded
0
```

`api.rs` 里**一处 `send_bounded` 都没有**。全仓的 `send_bounded` 调用在：

- `nathole.rs:110,112,177,178,194`（5 处）
- `runtime.rs:345,459,542,579`（4 处，属 `spawn_runtime` / `bind_error_retry` /
  `notify_client_recovery` / `publish_spec_in_place`）

报告把 `runtime.rs` 的调用误记成了 `api.rs` 的 `*_service_locked`。

**要改成的正确表述**：整个 `api.rs` 的 admin handler 全部用裸 `send`，
`update_client` 不是"唯一漏点"，而是"11 处中的 2 处"。因此结论不是"补一处
对齐"，而是"需要统一处理"。

---

## 任务 2：修正报告 §16B 的裸 `send` 数量与行号

**位置**：`docs/concurrency-serialization-fixes.md` 第 604-607 行

**报告现在写的**：

> `api.rs` 还有 8 处裸 `send`（`:1452` `:1498` `:1694` `:1721` `:1858`
> `:1902` `:1977` `:1984` `:2032`）

**核查结果**：

- 说"8 处"，但列出了 **9 个行号**（自相矛盾）；
- `grep -c "\.send(ServerMessage" crates/rh-server/src/api.rs` = **11 处**；
- `:1902` 实际是注释行，正确行号是 **`:1914`**。

**真实清单（11 处，按函数归属）**：

| 行号 | 所属函数 |
|---|---|
| 637 | `update_client`（禁用分支，proxy） |
| 649 | `update_client`（禁用分支，visitor） |
| 1452 | `disable_service` |
| 1498 | `delete_service` |
| 1694 | `disable_service_locked` |
| 1721 | `delete_service_inner` |
| 1858 | `create_visitor` |
| 1914 | `update_visitor` |
| 1977 | `toggle_visitor`（enable 分支） |
| 1984 | `toggle_visitor`（disable 分支） |
| 2032 | `delete_visitor` |

**注意**：这 11 处**都不在控制读循环的内联路径上**（`handle_client_message`
只处理 `ClientMessage`，不调用 admin CRUD），所以**不会自锁**。真实后果是
队列满时 HTTP 请求挂到反向代理超时，期间 `serial_lock(&id)` 一直被持有。
报告 §16B 对这个后果的判断是**正确的**，只有数量和归属写错了。

---

## 任务 3：消除报告 §16B 的自相矛盾

**位置**：`docs/concurrency-serialization-fixes.md` 第 598-611 行

报告同时说了两件事：

1. 第 598-600 行：「建议改法：两处 `sess.send(...)` 换成 `send_bounded(...)`」
   —— 暗示逐个补；
2. 第 610-611 行：「如果要修，建议一次性把所有 admin handler 的裸 `send`
   统一换成 `send_bounded`，作为独立的一批，而不是逐个打补丁」
   —— 暗示一次性改。

既然任务 1 已证明"只有它漏了"不成立，**必须删掉第 1 条**，只保留
"一次性统一处理 11 处"的建议。并补一句说明：这 11 处风险相同、改法相同
（`send_bounded(CONTROL_SEND_TIMEOUT)`，`CONTROL_SEND_TIMEOUT` 定义在
`runtime.rs:16`，2s），适合作为一个独立提交。

---

## 任务 4：把 `upsert` 的持锁问题提升为正式未完成项

**位置**：报告 §6 第 245-246 行 + §15 未完成项汇总

报告现在只在 §6 末尾一句话带过：

> **未做**：`upsert` 的 proxy 分支有同样形态（持 `proxies` 锁调
> `stop_proxy`/`start_health`），但不在任务书 #6 范围内，没有顺手改。

**核查确认这确实存在**（`crates/rh-client/src/session.rs:1429` 起）：

```rust
async fn upsert(sess: &Arc<ClientSession>, e: SyncEntity) {
    match e {
        SyncEntity::Proxy(p) => {
            let mut cur = sess.proxies.lock().await;      // ← 持写锁
            ...
            stop_proxy(sess, p.id).await;                  // ← await
            ...
            start_health(sess, p.id, &p).await;            // ← await
        }
        SyncEntity::Visitor(v) => {
            // 这个分支已经有 drop(cur)，注释写着「别持着 visitors 锁去做重绑定」
            ...
        }
    }
}
```

**为什么值得提升**：`upsert` 是**单条规则更新**路径，`ServiceUpdate` 每改一条
规则就触发一次；而 `apply_sync` 只在全量同步（客户端重连）时触发。
**`upsert` 的实际触发频率高于已修的 #6**，影响可能更大。

**要做的事**：
1. 在 §15「未完成项汇总」里加一条，说明 `upsert` 的 proxy 分支仍是持锁跨
   await，且频率高于 `apply_sync`；
2. 保留 §6 的说明，但补一句交叉引用（"见 §15 第 N 条"）；
3. **不要现在改代码** —— 本任务只修报告。

---

## 任务 5（可选，仅当你能确认）：`port_in_use` 性能基准缺旧实现对照

**位置**：报告 §5 第 195-206 行的表格

报告 §5 的"修前"列标注了"单次运行"，但基准测试
（`crates/rh-server/src/state.rs:2097` `bench_port_scan_scaling`）**只测新实现**。
旧数据靠 `git stash` 手工测得，后人无法复现。

对比：同文件的 `port_scan_tests::reference_in_use`（第 1867 行附近）为**等价性
测试**提供了完整的旧口径对照函数 —— 那里做对了，但**性能基准没有**。

**如果你判断值得**（本项可选，不强求）：在 `bench_port_scan_scaling` 里加一个
旧实现口径的计时分支（复用 `reference_in_use` 的形态），让"修前"列可复现。

**如果做**：注意 `reference_in_use` 是逐条 `spec.read().await`，正好复刻了
修前"跨 await 持分片读锁"的形态，可以直接用它计时。

---

## 不要做的事

- **不要重跑基准**去"验证"报告数字 —— 已核查过，8 项修后数字全部可复现。
- **不要改 12 项已落地的代码** —— 除了任务 4 明确要求"只写报告不改代码"。
- **不要动 §16 的 A / C 两项** —— 核查确认 A（`notify_client_recovery` 串行）
  与 C（`delete_client` teardown 串行，最坏 N×2s）的描述**完全属实**，
  建议也合理。
- **不要动 §7（缺陷 7 未做）** —— 核查确认 `list_services` 确实仍是 3 次
  `get` + 3 个 await，报告"不做"的理由（改 `try_read` 会改变接口语义）成立。
- **不要顺手修 §16 的 A/B/C** —— 那是下一批任务，本次只修报告。

---

## 验收

1. `docs/concurrency-serialization-fixes.md` 的 §16B 不再有"只有它漏了"的
   错误论据，数量与行号与 `grep` 结果一致；
2. §15 未完成项汇总包含 `upsert` 持锁问题；
3. `cargo fmt --all -- --check` 干净（若改了代码）；
4. 提交信息写清"这是核查后的事实更正"，并附上 `grep` 命令作为依据。

## 提交信息模板

```
docs: 更正 §16B 的 send_bounded 归属与裸 send 数量

上一版报告称「enable/disable/delete_service_locked 已用 send_bounded，
只有 update_client 漏了」。核查发现 api.rs 里一处 send_bounded 都没有：
全仓 9 处都在 nathole.rs 与 runtime.rs，报告把 runtime.rs 的调用误记为
api.rs。

实际 api.rs 有 11 处裸 send（报告写 8 处却列了 9 个行号，且 :1902 是
注释行，正确是 :1914）。因此结论从「补一处对齐」改为「统一处理 11 处」，
并删掉与之矛盾的逐个补丁建议。

另：把 upsert 的 proxy 分支持锁问题从 §6 的一句话提升为 §15 正式未完成项
—— 它走单条规则更新路径，触发频率高于已修的 apply_sync。
```
