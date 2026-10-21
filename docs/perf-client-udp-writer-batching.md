# rh-client UDP 写路径重构:唯一写任务 + 批聚合

日期:2026-10-20
提交:`ffd49d3`

## 问题

`crates/rh-client/src/udpchan.rs::run_proxy_side` 此前是"每个用户源地址一个
回写任务,全部共享 `Arc<Mutex<Option<W>>>`":

```rust
// 改前
let (mut r, w) = tokio::io::split(channel);
let w: Arc<Mutex<Option<_>>> = Arc::new(Mutex::new(Some(w)));
// 每个回写任务里有:
let mut guard = w.lock().await;          // ← 锁跨 await 持有
match guard.as_mut() {
    Some(ww) => { frame::write_frame(ww, FrameType::UDP_PACKET, &payload).await; }
    None => break,
}
```

四个问题:

1. **锁跨 await 持有**:`guard` 活到 `write_frame().await` 结束。任一慢写
   让所有会话队头阻塞。
2. **PING 分支双重加锁**:为了拿到 `&mut`,先 `lock()` 看一眼、`drop`、
   再 `lock()`,中间留了一个可被回写任务插队的窗口。
3. **每包两次格式化**:`src.parse::<SocketAddr>()`(收方向)与
   `user.to_string()`(发方向),高 PPS 下是实打实的分配 + 解析开销。
4. **每包一帧**:11 字节帧头 + 一次 `write_all` syscall,无任何批聚合
   (对比 TCP 侧 1 MiB 批)。

## 改法

```text
  回写任务(每会话) ─┐
  主循环 PING/PONG  ─┼─► mpsc(有界 1024) ─► 写任务 ─► 写半
  主循环 PING(超时)─┘        └─ 凑够 BATCH_MAX(32) 帧 或队列空时,
                                多帧 encode 进同一个 BytesMut,一次 write_all
```

三条红线逐条守住:

| 红线 | 落实 |
|---|---|
| 不改 UDP_PACKET 线格式 | 仍是若干个**独立**帧 `[len][addr][data]`,只是拼进同一次 `write_all` |
| 不合并成"一帧多包" | 批里每个 `Pending` 各自 `encode_frame`;有专门单测按帧头切分验证 |
| 不引入无界队列 | `mpsc::channel(1024)`,满了 `send().await` 阻塞生产者 = 背压 |

低延迟:批聚合只在"队列里已经有货"时 `try_recv` 贪心多取,**绝不 sleep、
不额外等待凑数**。

其它三项:
- 地址解析走每线程 8 槽缓存(同一源地址反复出现,命中率近 100%);
- `user.to_string()` 在建会话时算一次,由回写任务捕获复用;
- recv 缓冲改为首次收包时按需分配(不再建会话就占 64 KiB);
- visitor 侧同样接上写任务,让 PING/PONG 与数据帧共用一条队列 → 帧序。

## 单元测试

| 测试 | 锁住的语义 |
|---|---|
| `concurrent_sources_never_cross_packets` | 16 源 × 64 包并发,逐帧解回的 src 与载荷标识必须一致(不串源) |
| `ping_pong_interleaved_keeps_order` | `d0, ping, d1, pong, d2` 投递 → 写出顺序逐字相同 |
| `batch_still_emits_independent_frames` | 手工按帧头切分,64 包必须是 64 个独立帧 |
| `bounded_queue_applies_backpressure` | 队列满时 `try_send` 必须 Full、`send().await` 必须阻塞 |
| `addr_cache_matches_parse` | 缓存命中/未命中结果与直接 `parse` 一致 |

红→绿(两次):

- 把批聚合改成"多包合并进一帧" → `batch_still_emits_independent_frames`、
  `ping_pong_interleaved_keeps_order`、`concurrent_sources_never_cross_packets`
  三个 FAILED;
- 改成"每批复用第一帧 payload(串源)" → 多源用例精确报出重复序号
  `[0,0,...,32,32,...]`。

## E2E

`tests/e2e.sh` **82 PASS / 0 FAIL**,含 UDP 转发 / SUDP visitor / 重启后
UDP 恢复转发。

## 性能测量

### 方法

- 三机 peer 基准(`AGENTS.md` 流程),`benchmark/peer.sh`
- `PROTO=tcp RELAY=splice MUX=1 THP_TOOL=loadgen PEER_REPEAT=3
  PEER_DURATION=15 PEER_UDP_SOURCES=8`
- 客户端二进制由 `identity.jsonl` 的 `exe_sha256` 核对:
  - A(改前 `bb9901c`)`7ebb8a40d97c…`
  - B(改后 `ffd49d3`)`fc0f178c5161…`
- CPU 按 **UDP 段实际时间窗口**切取(`results-*.jsonl` 的
  `started_us`/`ended_us`),不是整轮平均

### 需要先说明的测量工具改动

`benchmark/py/loadgen.py` 的 `udp` 子命令原先只有一个 socket,
即**单源**。而本改动的目标场景正是"多会话写路径"—— 单源压测永远测不到
多会话争用。故新增 `--sources N`(每源一个独立 UDP socket、绑定不同本地
端口),`peer.sh` 接 `PEER_UDP_SOURCES`。`--sources 1` 与旧行为等价。

### 结果

| 指标 | A(改前) | B(改后) | 差 |
|---|---:|---:|---:|
| 隧道吞吐(中位) | 83.5 Mbps | 79.2 Mbps | −5.2% |
| 直连吞吐(中位) | 128.95 Mbps | 131.8 Mbps | +2.2% |
| 隧道 p50 RTT(中位) | 2.31 ms | 2.46 ms | +6% |
| 隧道 p99 RTT(中位) | 19.8 ms | 18.5 ms | −6% |
| **UDP 段 CPU(单核占比)** | **41.7%** | **40.2%** | **−1.5pp(−3.6%)** |
| 丢包 | 0% | 0% | — |

原始隧道样本:

```
A: 83.51  79.28  95.47 Mbps
B: 81.40  74.41  79.20 Mbps
```

### 判读

**没测出预期收益。**

1. **吞吐 ±5% 在噪声内**。A 组三次自身就在 79.28~95.47 之间摆了 20%,
   中位数之间 5% 的差没有意义。
2. **CPU 只降 3.6%**(41.7% → 40.2%),而这个改动消掉的是一把真实的
   跨 await 锁 + 每包格式化 + 每包 syscall。若这些真在高 PPS 下占主导,
   应该看到更明显的下降。
3. **根本原因是压测强度不够**。8 源、每源 pps ≈ 1000、总 pps ≈ 8000。
   在这个量级下:
   - 锁竞争窗口极短(单次 `write_frame` 是本地 socket 写,微秒级);
   - CPU 大头是 UDP 收发本身与 snappy/协议栈,不是等锁;
   - 队头阻塞无从体现。

   要放大锁争用,需要**更高 PPS**:更多源(64/256)、更小包、更高发送
   速率。8 源 × 1200B 远未压到写路径的瓶颈。

4. **RTT p50 +6% 需留意**。批聚合理论上应降低 p50(单次 syscall 送更多
   包),但实测略升。可能与批聚合引入的"多等一次 try_recv"有关,也可能
   是噪声(三次样本 2.31/2.46 的差低于同一组内摆动)。

### 诚实结论

**改动在代码层面是正确且必要的**(消除跨 await 锁、消除每包格式化、
批聚合减少 syscall、帧序由单队列保证),但**当前测试强度下测不出可测
收益**。吞吐 ±5%(噪声内)、CPU −3.6%、p50 略升,都不足以支撑"提升"。

要给出可信的性能结论,需要:64+ 并发源、包长降到 200~
bytes、并确认压测端自身不是瓶颈。这轮没做。

## 附带发现

- **`/tmp/peer-udp` 这类固定 `PEER_DIR` 会让前一组证据被后一组覆盖**,
  导致无法回看对照。本轮 A 组 CPU 数据第一次就被 B 组覆盖,只能重跑。
  对照实验必须用两个独立目录。
- **`ps -eo pid,comm | awk '$2=="rh-server"'` 在 termark exec 里会被
  外层 shell 吃掉引号**,`$2` 提前展开成空 → kill 不到目标,旧 server
  仍占 17100。改用 `ss -ltnp | sed -n 's/.*pid=\([0-9]*\).*/\1/p'`
  按端口解析 PID 更稳。
