# 控制面消息的重复序列化与整包拷贝消除

一句话结论:控制消息从「序列化一次 → 队列传 payload → `write_frame` 再分配 `BytesMut` 并整包拷一遍」改为「序列化 + 成帧各一次,队列直接传完整帧 `Bytes`」。同一消息的帧字节与修前**逐字节相同**(有测试锁住),队列容量 256 的背压语义不变。

## 0. 身份

- 仓库 `rabbithole`,起点 HEAD `90f6f6b`(工作树另含前几轮未提交改动)。
- 改动文件:`crates/rh-client/src/session.rs`(编码路径 + 队列类型 + 测试)。
- 本机 macOS arm64 / rustc 1.98.1。

## 1. 改了什么

### 修前

```rust
// send_msg:序列化一次,队列里是 payload
let payload = serde_json::to_vec(&m).unwrap_or_default();
self.send_tx.send((frame_type_of_client(&m), payload)).await.is_ok()

// run_loop:再成帧一次(write_frame → encode_frame_bytes)
//   BytesMut::with_capacity(HEADER + payload)
//   put_slice(payload)          ← 整包再拷一遍
//   w.write_all(&frame)
Some((ty, payload)) => { frame::write_frame(&mut control, ty, &payload).await }
```

一条消息 = 1 次 `serde_json::to_vec` 分配 + 1 次 `BytesMut` 分配 + 1 次整包 `memcpy`。

### 修后

```rust
fn encode_control_frame(m: &ClientMessage) -> Option<bytes::Bytes> {
    let ty = frame_type_of_client(m);
    let payload = serde_json::to_vec(m).ok()?;
    if payload.len() > frame::MAX_FRAME_LEN {
        return None;                    // 编码前判长度
    }
    Some(frame::encode_frame_bytes(ty, &payload))
}

// 队列:mpsc::Sender<Bytes>(容量仍是 256)
Some(bytes) => { control.write_all(&bytes).await }   // 只有一次 write_all
```

`Bytes` 跨任务移动是引用计数,不再有 payload 的二次拷贝。

### `MAX_FRAME_LEN` 校验保留且提前

校验从 `write_frame`(编码之后)移到 `encode_control_frame`(编码之前)。语义不变但更早拒绝:超限载荷一旦编码出去就是**腐化帧**(长度字段被 `as u32` 截断),对端会以 `TooLarge` 关掉整条连接,不如本端直接拒绝。

### 序列化失败的处理

修前 `unwrap_or_default()` 会把失败变成空 payload 发出去(一个语义为空的帧);修后返回 `None` → `send_msg` 返回 `false`,**不往队列里塞任何东西**。这比发一个空帧更安全。

## 2. 验收测试(7 个)

| 测试 | 断言 |
|---|---|
| `control_frame_bytes_identical_to_legacy_path` | 对 Ping / UserConnect / HealthReport / NatHoleReport 四种消息,新路径的帧字节与「`write_frame` 旧路径」**逐字节相等**;且能被 `read_frame` 解出,FrameType 与 payload 不变 |
| `frame_type_mapping_unchanged` | 五种 ClientMessage 变体的 FrameType 映射逐一核对 |
| `oversized_payload_is_rejected` | payload > `MAX_FRAME_LEN` → `encode_control_frame` 返回 `None` |
| `payload_at_limit_is_accepted` | payload 恰为 `MAX_FRAME_LEN` → 放行(边界不误伤) |
| `each_message_is_framed_exactly_once` | **队列元素长度 = `HEADER_LEN` + payload.len()**,即已是完整帧;再用 `write_all` 写出,字节与原帧完全一致 |
| `queue_capacity_preserves_backpressure` | 塞满 256 条后第 257 条 `send` 必须 pending;消费一条后恢复可写 |
| `send_msg_returns_false_on_oversized` | 超限消息 `send_msg` 返回 `false`,且**队列为空**(不得发出腐化帧) |

### 负向验证(逐个做过)

| 注入的回归 | 结果 |
|---|---|
| 队列改回只存 payload(`encode_control_frame` 不封头) | `control_frame_bytes_identical_to_legacy_path` 报红:`FrameType(3) 的帧字节与修前不一致`;`each_message_is_framed_exactly_once` 同时报红 |
| 删掉 `MAX_FRAME_LEN` 校验 | `oversized_payload_is_rejected`、`send_msg_returns_false_on_oversized` 报红 |

## 3. 顺带修掉一个上一轮的间歇性失败

跑 `cargo test --workspace` 时,上一轮新增的 `probe_result_matches_thresholds` **偶发**失败(`left: 2, right: 1`),单独跑该 crate 却稳定绿。

根因:那组探针用例的注入状态(`DIALS`/`DIAL_MS`/`DIAL_OK`/`LAST_PROXY`)用 `static AtomicUsize`,而持锁窗口只覆盖 `probe_setup`;用例如在窗口内改 `DIAL_MS` 测两档 RTT。rh-client 的测试也在同一进程里调用 `probe_call_count()` 并改这些全局值,两边互相污染。

修复:注入状态改为 `thread_local`(tokio current_thread 测试在单线程内跑,天然隔离);缓存本身仍是进程级全局(生产语义如此),故仍用 `tokio::sync::Mutex` 串行。

修后 `cargo test --workspace` 连跑 5 次,每次 16 个测试套件全绿。

## 4. 红线核对

| 红线 | 状态 |
|---|---|
| 控制帧线格式不变 | `control_frame_bytes_identical_to_legacy_path` 用**旧实现**产出字节做基准,逐字节比对;`read_frame` 可解 |
| FrameType 映射不变 | `frame_type_of_client` 一行未改;`frame_type_mapping_unchanged` 逐个核对 |
| 256 队列容量与背压语义 | `mpsc::channel::<Bytes>(256)` 容量不变;`queue_capacity_preserves_backpressure` 断言队满时 `send` pending,不丢弃、不扩容 |

全量:`cargo test --workspace` 全绿(rh-client 70 → 77);`cargo clippy --workspace --all-targets -- -D warnings` 通过;`tests/e2e.sh` **PASS=82 FAIL=0**(控制面 HELLO/认证/同步/心跳/USER_CONNECT/健康上报全链路)。

## 5. 未做的部分(如实)

- **没有做零拷贝的进一步优化**:`encode_frame_bytes` 仍然先写 `BytesMut` 再 `freeze()`。理论上可以用 `BytesMut::with_capacity` + 预留头空间让 payload 直接写进 spare 区(类似上一轮压缩泵的做法),但序列化产物本来就是独立 `Vec`,要避免这次拷贝得改 `serde_json` 的写入目标(`to_writer` 到一个预分配缓冲)。收益是一条消息省一次小 `memcpy`(控制帧通常几百字节),而控制面不是热路径——**没有性能证据支持,故不做**。
- **没有基准数据**。控制面消息频率是心跳级(默认 30s 一次)+ 规则变更时若干条,不在吞吐热路径上。这次改动的价值是「少一次分配与拷贝」这个结构性质,而不是可测的 QPS 提升。刻意没有编造一个基准数字。
- **`send_msg` 的 `false` 语义有一处细微变化**:序列化失败时,修前会发出一个空 payload 帧(对端解出来是无效消息),修后直接不发并返回 `false`。这是**行为改进**,但确实与旧行为不同,记在此处。目前 `ClientMessage` 全是可序列化的普通结构体,该分支实际不可达。
