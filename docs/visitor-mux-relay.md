# visitor 数据面接入零拷贝 mux 中继

一句话结论:**rh-client 的两条 visitor 中继路径从 copy 回落改为可走 `mux_relay` chunk 快路径**,STCP 单流本机吞吐 best 625.7 → 716.9 MiB/s(**+14.6%**),双向数据与半关闭语义不变,压缩路径与 `relay_mode=copy` 逐字未动。

## 0. 身份

- 仓库 `rabbithole`,起点 HEAD `90f6f6b`。
- 改动文件:`crates/rh-client/src/visitor_run.rs`(两处调用点 + 测试)、`crates/rh-client/src/forward.rs`(新增 `relay_stream` 与测试用路径探针)。
- 本机 macOS / rustc 1.98.1 / release。**非三机基准**,不与 `benchmark/peer.sh` 的跨机保留率混用。

## 1. 改了什么

### 两处调用点

| 位置 | 改造前 | 改造后 |
|---|---|---|
| `xtcp_direct_stream`(`visitor_run.rs:175`) | `forward::splice(stream, Box::new(conn), None)` | `forward::relay_stream(stream, Box::new(conn), &sess.cfg.relay_mode)` |
| `deliver_relay`(`visitor_run.rs:227`) | `forward::splice(channel, Box::new(conn), None)` | `forward::relay_stream(channel, Box::new(conn), &sess.cfg.relay_mode)` |

两处的 IO 来源都确认是未包装 `RhMuxStream`:
- `xtcp_direct_stream`:紧跟 `mux.open_stream()`,而 `open_stream` 返回 `Box::new(RhMuxStream::new(stream))`(`mux.rs:1807`);
- `deliver_relay`:`channel` 来自 `session.rs:1051` 的 `sess.mux.open_stream()`。

### 判定收进一个函数

新增 `forward::relay_stream(peer, local, relay_mode)`,把 `handle_start_forward` 里那段快路径判定的**外形**抽出来复用:

```rust
if relay_mode != "copy" && is_mux_stream(&peer) {
    if let Some((b_r, b_w)) = steal_mux_halves(&mut peer) {
        mux_relay(local, b_r, b_w, None, None).await;
        return;
    }
}
splice(peer, local, None).await;
```

`handle_start_forward` 的原逻辑**一行未动**(它还要装配规则级联取消与限速器,与 visitor 的 `cancel=None`/`limiter=None` 不同,不宜强行合并)。

### 压缩路径逐字未动

`deliver_relay` 的压缩分支保持原样:

```rust
let channel = if relay.flags & forward_flags::COMPRESSED != 0 {
    // PrependedIo::wrap + compress_stream —— 一行未改
} else { /* prefix 直写 conn */ channel };
```

压缩产出 `DuplexStream`,`is_mux_stream` 恒 false,**自然**回落 `splice`,不需要额外判压缩位。这一点有专门测试锁住(见 §3)。

## 2. 为什么必须有「路径探针」才叫验收

`mux_relay` 与 `splice` **语义等价**(双向搬运 + 半关闭 + 错误收敛),所以纯功能测试**区分不了两条路径**。实测:把快路径判定改成 `if false && ...` 强制回落 copy,原有的数据/半关闭测试**照样全绿**。

也就是说,只写「双向 512KiB 一致 + 半关闭」的测试,改造没生效也会静默通过。因此加了 `#[cfg(test)] relay_path_probe`:

- `relay_stream` 在选路后打点(`Chunk` / `Copy`);
- 测试用 `tokio::sync::Mutex` 串行化「基线 → 运行 → 断言」窗口。

> 这里踩了两个坑,记下来免得重犯:
> 1. 用 `std::sync::Mutex` 跨 `await` 持锁 → clippy `await_holding_lock` 报警,且会阻塞 OS 线程;
> 2. 改成「取基线即放锁」的短临界区 → 并行测试**互相计数**,实测读到 `chunk=1, copy=2` 这种交叉污染值,断言假红。
>
> 最终用 `tokio::sync::Mutex`(`const_new`)整段持有:async 锁跨 await 安全,窗口独占。

## 3. 验收

### 新增测试(`crates/rh-client/src/visitor_run.rs`)

| 测试 | 内容 |
|---|---|
| `visitor_mux_relay_bidirectional_and_half_close` | `wrap_stream_as_client/server` 建 mux → `open_stream` → `relay_stream` 接 duplex → 双向各 **512KiB** 逐字节一致 + **双向半关闭**(peer FIN → local EOF;local FIN → peer EOF)+ **断言走 Chunk 路径**(`chunk==1, copy==0`) |
| `visitor_relay_mode_copy_still_works` | 同上数据/半关闭,`relay_mode="copy"`;**断言走 Copy 路径**(`chunk==0, copy==1`) |
| `compressed_stream_is_not_mux_stream` | 锁红线:`open_stream` 产物是 mux 流;`compress_stream` 后**不是**;再套 `PrependedIo` 也**不是** |
| `compressed_path_through_relay_stream_roundtrips` | 压缩流经 `relay_stream`(即使 `relay_mode="mux"`)仍端到端正确,且**断言走 Copy**(`chunk==0, copy==1`) |

**负向验证**:把 `relay_stream` 快路径判定改成 `if false && ...`,`visitor_mux_relay_bidirectional_and_half_close` 立刻报红:

```
assertion `left == right` failed: 必须走零拷贝 mux chunk 快路径(chunk=0, copy=1)
```

其余 3 个测试仍绿(它们本就该走 copy)。恢复后 4/4 全绿。

### 脚本

| 脚本 | 结果 |
|---|---|
| `tests/e2e.sh` | **PASS=82 FAIL=0**(含 STCP / SUDP / XTCP 各项) |
| `tests/xtcp-isolated.sh` | **PASS=7 FAIL=2** |

`xtcp-isolated.sh` 的 2 个 FAIL 是**既有问题,与本次改动无关**,已用对照证明:

- 在**未改动的原始代码**(`git stash` 后重建)上跑同一脚本,得到**完全相同的 7 PASS / 2 FAIL**,失败的也是同样两条断言:
  - `FAIL: visitor 侧日志确认打洞成功`
  - `FAIL: proxy 侧日志确认直连服务`
- 根因:本机 macOS 上打洞失败,日志是 `punch dial failed; fallback relay ... error=punch timeout`,走的是服务端中继;而脚本用 `grep "punch dial ok"` / `grep "serving direct"` 断言打洞成功。功能本身正常(「直连-首次连接经打洞回显」等 7 项 PASS,回显数据正确),只是日志断言与这台机器的网络条件不符。
- 该脚本无执行权限(`-rw-r--r--`),需 `bash tests/xtcp-isolated.sh` 运行;未改权限(与本次改动无关)。

### 吞吐

STCP 单流、本机 loopback、双向 64MiB echo、每档 5 轮:

| 版本 | best | med |
|---|---|---|
| **改前**(原始代码) | 625.7 MiB/s | 592.0 MiB/s |
| **改后** | **716.9 MiB/s** | 681.5 MiB/s |
| 改后(交叉复测) | 713.0 MiB/s | 672.9 MiB/s |
| 对照:改后但强制 copy | 687.1 MiB/s | 648.7 MiB/s |

- 改前 → 改后 **best +14.6%**、med +15.1%;
- 改后两次独立运行一致(717 / 713),排除单轮噪声;
- 「强制 copy」落在改前与改后之间,说明提升确实来自 mux chunk 路径,但不是全部(这份基准里 echo 服务端与 loopback 本身占了不少占比,chunk 收益被稀释)。

> 说明:这份基准是**为本次改动临时写的本机 STCP 压测**(`/tmp/stcp-bench.sh`,未入库)。仓库既有 `benchmark/peer.sh` 明确不覆盖 STCP(`benchmark/README.md:339`),而本次要求正是 STCP 单流,故另建。结论只用于「改前 vs 改后」的方向判定,不作绝对性能声明。

## 4. 红线核对

| 红线 | 状态 |
|---|---|
| 压缩路径必须原样 | 压缩分支代码**一行未改**;`is_mux_stream` 对压缩流恒 false(测试 `compressed_stream_is_not_mux_stream` 锁死);端到端仍正确(`compressed_path_through_relay_stream_roundtrips`) |
| `relay_mode != "copy"` 前提 | 已带;`visitor_relay_mode_copy_still_works` 断言 copy 分支可达 |
| waiting 表 10s TTL 语义不变 | 未触碰 `handle_user_conn` 里 `waiting.insert` + 10s `sleep` 后 `remove` 的任何一行 |
| 不"顺手统一"压缩 | 未合并 `handle_start_forward` 的快路径(它需装配 limiter/cancel,visitor 不需要) |
| 线格式/协议不变 | 只改中继实现,未动 `rh-protocol` |

全量:`cargo test --workspace` 全绿(rh-client 39 → 43,rh-transport 164 → 165);`cargo clippy -p rh-client --all-targets -- -D warnings` 通过。

## 5. 未做的部分(如实)

- **只覆盖 STCP/XTCP 的 TCP 中继路径**。SUDP 走 `udpchan::run_visitor_side`,UDP 包语义与 mux chunk 不同,不在本次范围。
- **XTCP 打洞直连路径无法在本机验证快路径生效**:本机打洞失败(见 §3),`xtcp_direct_stream` 实际不执行。该路径与 `deliver_relay` 共用同一个 `relay_stream`,`is_mux_stream` 前提由 `open_stream` 保证,逻辑上等价;但**没有在真打洞成功的环境下跑过**,如实记录。
- **chunk 收益被 loopback 稀释**。这份基准里 echo 服务端用 Python 线程(纯用户态收发),加上 loopback 本身,都吃掉了相当比例的绝对时间;真实网络下 chunk 省掉的是两次中间拷贝,占比应更高,但这需要三机验证,本机给不出。
