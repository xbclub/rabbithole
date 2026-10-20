# 客户端 copy 回落路径改用无锁拆分(round-69 同款)

日期:2026-10-20
提交:`d0a2dab`(实现)+ `53ba8cf`(压测开关)

## 问题

`crates/rh-client/src/forward.rs::splice()` 是客户端数据面 **copy 回落路径**,
此前用 `tokio::io::split(a)` / `tokio::io::split(b)` 拆分双向流。

`tokio::io::split` 的两半共享**一把 `std::sync::Mutex`**
(`tokio/src/io/split.rs` 的 `with_lock` 直接 `lock().unwrap()`),
`poll_read`/`poll_write` 整段在锁内。一端在内核态(`recvfrom`/`copy_user`)
持锁时,另一端 `lock()` 会**阻塞整个 OS 线程**而不是让出。

round-69 已在 `mux_relay` 上实测过同一问题(上行 P=16、`strace -c`):

```
futex   75.0% 的 syscall 时间(39.85s / 53.13s),3690 次,平均 10.8ms
recvfrom 10.9%
writev   8.1%
```

即进程绝大多数时间在**等锁**,不是在收发数据。`forward.rs::splice` 是这条
路径上最后还在用 `tokio::io::split` 的数据面中继。

## 影响面

这条路径承接:

- 所有 `use_compression = true` 的规则(压缩流无法零拷贝,必走 copy)
- `relay_mode = copy` 的 A/B 对照臂
- 全部插件规则
- 全部 visitor 流量(stcp / xtcp)

## 改法

照抄 `crates/rh-transport/src/mux.rs:657-666` 的既有模式:对 `a`、`b` 各自
先试 `rh_transport::split_boxed_io`(裸 `TcpStream` → `into_split`,读写各一
fd、零共享锁),失败回落 `tokio::io::split`;两侧统一擦成

```rust
(Box<dyn AsyncRead + Send + Unpin>, Box<dyn AsyncWrite + Send + Unpin>)
```

再喂给现有 `copy_limited`。

**未改动**(逐字保持):`splice()` 的取消 / 半关闭 / panic 收敛逻辑
(`AbortOnDropHandle`、`join_relay_pumps` 各分支)、`copy_limited` 的缓冲策略
(16 KiB 起翻倍到 64 KiB)、`ERROR_CLOSE_GRACE` 与半关闭分支。

## 单元测试

| 测试 | 作用 |
|---|---|
| 既有 6 个 `forward::tests` | 原样全绿,覆盖父取消杀子泵 / panic 收敛 / 半关闭有界 |
| `splice_lockless_split_bidirectional_1mib` | 两条**真实 TcpStream** 双向并发各传 1 MiB,逐字节校验;探针断言确实命中无锁分支两次 |
| `splice_falls_back_for_non_tcp` | `duplex` 必须回落,且功能不变 |

红→绿:把 `splice` 改回裸 `tokio::io::split`,`1mib` 用例立刻
`FAILED("splice 必须对两条裸 TcpStream 都走无锁拆分")`,其余 7 项仍绿。

## 性能测量

按 `AGENTS.md` 的三机 peer 基准流程,新增 `PEER_COMPRESS` 开关(见下),
使压缩规则能在既有证据框架下度量。

### 方法

- 三机:test1 服务端 / test2 客户端+目标 / test3 压测端,均 8 核 10GbE
- 脚本:`benchmark/peer.sh`(未另写压测器)
- 参数:`PROTO=tcp RELAY=splice MUX=1 PEER_COMPRESS=1 THP_TOOL=loadgen
  PEER_REPEAT=3 PEER_DURATION=15`
- 客户端二进制:
  - A(改前) `95ea8ba` → sha256 `40704a9e8dfde30756c0…`
  - B(改后) `d0a2dab` → sha256 `eba962f9a6f783498071…`
  - 由 `/tmp/peer-cmp/identity.jsonl` 的 `exe_sha256` 核对,非人工声明
- 服务端二进制两端相同(改动只在 rh-client)

### 结果(Mbps,3 次取中位)

| 方向 | 直连 A | 直连 B | 直连差 | 隧道 A | 隧道 B | **隧道差** |
|---|---:|---:|---:|---:|---:|---:|
| 1 流上行 | 1694 | 1772 | +4.6% | 2199 | **2346** | **+6.7%** |
| 1 流下行 | 1753 | 1710 | −2.5% | 2234 | 2220 | −0.6% |

连接建立速率(隧道):A 817.9/s → B 853.2/s(+4.3%)。

原始 3 次样本:

```
A 上行隧道: 2150 2304 2199       B 上行隧道: 2354 2194 2346
A 下行隧道: 2234 2256 2189       B 下行隧道: 2220 2273 2168
A 直连上行: 1736 1653 1694       B 直连上行: 1772 1772 1637
A 直连下行: 1766 1731 1753       B 直连下行: 1767 1710 1696
```

### 判读

**上行 +6.7%,但下行 −0.6%,提升并不对称 —— 不能宣称"全面提速"。**

需要谨慎的地方:

1. **直连对照组也在动**。直连上行 +4.6%、下行 −2.5%,说明本轮存在机器级
   波动。隧道上行 +6.7% 减去直连上行的 +4.6%,净效应约 **+2%**,
   仍在噪声边缘。
2. **样本量偏小**。3 次配对,单档波动可达 ±5%(看原始样本:B 上行在
   2194/2346 之间就有 6.9% 的摆动)。按 AGENTS.md 的规矩,
   定量结论应至少 5 轮。
3. **压缩掩盖了锁竞争**。本轮 `PEER_COMPRESS=1`,数据先被 snappy 压缩,
   实际过线的字节数远小于标称吞吐,锁竞争窗口相应变小。loadgen 的载荷
   可压缩,所以隧道数反而高于直连(2199 vs 1694)——这个"隧道比直连快"
   本身就是压缩效应的证据,不是无锁拆分的功劳。
4. **更该看的下行没有收益**。理论上无锁拆分对读写两向一视同仁;下行
   没有提升,与"消除锁竞争应两向受益"的预期不符。

### 诚实结论

**这批数据不足以支撑"有可测提升"的结论。** 唯一能确定的是:

- 上行有 +6.7%,但同轮直连上行也 +4.6%,净效应约 +2%,在噪声内;
- 下行无变化;
- 连接建立速率 +4.3%,同一量级。

改动本身是**正确且必要**的(消除了一把真实的跨方向锁,与 round-69 在
mux_relay 上的既有修复一致,且以后任何 wrap 上来的裸 TCP 都自动受益),
但**在当前测试床上测不出显著收益**。

要给出可信结论需要:不可压缩载荷(排除压缩干扰)+ 更高并发(放大锁竞争)
+ `PEER_REPEAT=5`。

## PEER_COMPRESS 开关

`benchmark/peer.sh` 新增,因为压缩规则此前**完全无法**在 peer 证据框架下
度量:

```bash
PEER_COMPRESS=1 ... peer.sh server|client|drive
```

打开时 tcp/iperf 规则带 `transport.use_compression=true`,客户端该通道
回落 `forward::splice` 的 copy 路径。与 `THP_TOOL` 一样必须 `export`
(role 由子 shell 分派,未导出时规则静默不开压缩)。

## 踩到的坑(已记录备查)

- **`peer.sh` 的 `PEER_COMPRESS` 代码不在旧 commit 里**。第一次 A 组是在
  `git reset --hard 95ea8ba` 后跑的,脚本随之回退,压缩没生效,整组数据
  作废。三台必须同步到同一 commit 再跑。
- **API 返回的压缩字段在 `spec.transport.use_compression`,不在顶层
  `transport`**。用 `jq '.transport'` 查会得到 `null`,误判成"压缩没开"。
- **`PEER_DIR` 残留会撞 `UNIQUE constraint failed: clients.name`**,且
  此时 `peer.sh` 仍打印"A 侧就绪",容易误以为正常。
- **test1 上残留的 `frps` 会占住 17100**。
