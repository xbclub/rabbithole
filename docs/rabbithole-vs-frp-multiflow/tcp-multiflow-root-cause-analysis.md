# rabbithole vs frp TCP 多流性能根因分析

> 证据分级约定：全文每条论断标注 **[代码可证]**（本地代码/源码可 grep 到行号）、**[实测]**（本次跑过命令或仓内已有实测记录）、**[推测]**（无代码或数据支撑的推断）。frp 侧源码位置以 `benchmark/frp-src-v0.71.0/` 与 `benchmark/frp-yamux-src/` 为准，rabbithole 侧以 `crates/`、`vendor/yamux/` 为准。
>
> **文档状态（2026-10-18 更新，2026-10-19 订正）**：本报告初版基于 commit `33f646f`（Round-7）的工作树。此后 Round-8 / Round-9 已实施并完成三机实测：R1 已修复并实测生效，C1/C2 经实测否决或改道。**§0a 是本次更新的实施状态摘要，阅读 §1–§6 时应以其为准**；§4/§5/§6 中标注「⚠️ 已过期」的条目保留原始分析文本，但结论以 §0a 为准。
>
> **round-45 订正**：本报告原文把 `YAMUX_POOL_DEFAULT_CARRIERS = 4` 当作「池的推荐默认载体数」并据此论证。该常量**从未被生产代码读取**，且其值（4）与实现不符——`default_carriers()` 对 tcp/ws 返回 1、tls/wss 返回 2，服务端收敛只减不增，**默认路径永远到不了 4**。该常量已删除。文中所有「K=4」应理解为**实验臂显式设定**（`RH_MUX_POOL_SIZE=4`），不是开箱即得的默认值。默认 K 的唯一来源是 `crates/rh-transport/src/lib.rs` 的 `default_carriers()`。

## 0a. 实施状态（2026-10-18）

### 已实施并实测

| 项 | 原方案 | 实际落地 | 实测结果 |
|---|---|---|---|
| **R1 多载体池接线** | C0：客户端 `connect_pool` + 服务端合流 | ✅ 已完成（Round-9，双侧同步） | ×16↓ **+15.0%**、×16↑ **+31.2%**、×4↑ **+22.3%**（相对单载体，同臂同窗口） |
| **×16 对 frp** | — | — | ×16↓ 2953 vs 2821（**+4.7%**）、×16↑ 2706 vs 2465（**+9.8%**），**已反超** |
| **R2/R4 单写出口** | 池化 + 写侧优化 | ✅ 由 C0 部分回收 | 服务端 `ss` 观测到 4 条 ESTAB 物理连接（K=4 实验臂生效；该 4 由 `RH_MUX_POOL_SIZE=4` 显式设定，**非默认值**——默认 tcp/ws 是 1，见 §4 C0 的 round-45 修正） |
| 项 2（Round-8） | 未在本报告方案内 | ✅ RecvHalf 缓存路由句柄，热路径免锁 | 三机零回退，保留 |
| 项 4（Round-8） | 与 C2 同向 | ✅ pump_down 改 writev 向量写 | 三机零回退，保留；`×4↓ 2189` 为四臂最高 |
| **C1（1MiB→2MiB）** | 方案提出 | ❌ **未实施，维持 1MiB** | 同向改动（READ_RESERVE 64→256KiB）实测 ×16↑ **-4.0% 确凿回退**，见 Round-8 §3.2；按本报告「任一回退即停」门禁执行 |
| **C2（64→256KiB）** | 方案提出 | ⚠️ 未按原样实施 | 由项 4（writev 向量写）以不同路径达成同等目标，零回退 |
| **C5（读探测预算）** | C1 前置条件 | ❌ 未实施 | C1 未实施，前置条件自然不成立 |
| **C3（载体软上限）** | 方案提出 | ❌ 未实施 | 轮转仍为 `seq % n`；×4↓ 是当前唯一落后档（见下） |

### 当前性能形态（Round-9，PROTO=tcp RELAY=splice MUX=1）

| 档 | rh1（单载体） | rh4（池化 K=4） | frp v0.71.0 | rh4 vs frp |
|---|---:|---:|---:|---:|
| ×16↓ | 2567 | **2953** | 2821 | **+4.7%** |
| ×16↑ | 2063 | **2706** | 2465 | **+9.8%** |
| ×4↓ | 2078 | 1985 | 2682 | **-26.0%** |
| ×4↑ | 1987 | **2431** | 2222 | **+9.4%** |
| ×1↓ | **2008** | 1934 | 1161 | +66.6% |
| ×1↑ | **1991** | 1980 | 1141 | +73.5% |

池化增益（rh4/rh1，同臂隧道绝对值）：×16↓ +15.0%、×16↑ +31.2%、×4↑ +22.3%，×4↓ -4.5%、×1↓ -3.7%、×1↑ -0.6%。

### 唯一剩余缺口：×4↓

- **×4↓ 在单载体（2078）与池化（1985）两种形态下都输给 frp（2682），池化反而略降 -4.5%。**
- 这说明 ×4↓ 不是池化引入的问题，也不是池化能解决的问题——瓶颈在别处。
- 同一轮的 ×4↑ 池化后反超 frp（+9.4%），**上行与下行行为不对称**，指向下行专属路径。
- 定向研究见 [x4-download-gap-analysis.md](x4-download-gap-analysis.md)。

### 本次更新的核验（2026-10-18 实际执行）

| 命令 | 结果 |
|---|---|
| `cargo test -p rh-transport --lib mux::` | **26 passed**，0 failed |
| `cargo test -p rh-transport --test pool_wiring` | **2 passed**（T-I1 全链路、T-I2 单载体等价） |
| `cargo test --workspace` | 全部 `test result: ok`，0 failed |
| `cargo clippy --workspace --all-targets` | exit 0（仅 console/dist 构建提示） |
| `grep -n connect_pool crates/` | 生产调用方 `crates/rh-client/src/session.rs:93`（⚠️ 原报告「零调用方」已过期） |
| ~~`grep -n YAMUX_POOL_DEFAULT_CARRIERS`~~ | ~~`crates/rh-transport/src/mux.rs:999`~~ **⚠️ 该常量已于 round-45 删除**：它从未被生产代码读取，且其值（4）与默认行为不符——`default_carriers()` 对 tcp/ws 返回 1、tls/wss 返回 2（服务端收敛只减不增），默认路径**永远到不了 4**。默认 K 的唯一来源是 `crates/rh-transport/src/lib.rs` 的 `default_carriers()` |
| `grep -n "pub async fn connect_pool"` | `crates/rh-transport/src/lib.rs:629`（原 `:599` 已漂移） |

## 0. 结论速览（⚠️ 基于 Round-7，已过期，保留供追溯）

- **根本原因排序**：① 多物理载体池（connect_pool，K=4）写好了但**全生产链路零调用方、未接线**，RH 实际跑在「一条物理 TCP + 一条 yamux session + 唯一 TCP 写出口」上，而 frp 的 poolCount=4 虽也不是物理载体，但其「每流一条独立 mux stream + 用户态每连接独立 CopyBuffer goroutine」结构天然吃到了多核——这是**能力未启用而非能力落后**（R1，[代码可证]）；② 单 carrier 上 16 条流共享唯一 sendLoop/唯一 TCP 写出口，且每流一对 tokio 泵任务挤在协作调度器上，服务端 CPU 已贴 1 核（download×16 ≈0.97 核，[实测复算]）——这是剩下的结构面（R2/R4，[需实测]）；③ round-7 批聚合扩到 64 帧/1MiB 后的时序嫌疑仅能解释 download×16 回退中约 11% 的未归因部分（R3，[需实测]）。
- **单流为何不慢**：单流本来就只有一条流，不存在写出口竞争；RH 单流按建流授信（SYN|WU 预授 6MiB）+ 6MiB 恒定窗 + chunk/零拷贝中继路径，download×1 RH 1931 vs frp 1155、upload×1 1958 vs 1107，反而领先 67%~77%（COMPARE.md:10,13）。
- **多流慢在哪**：慢在并发扩展性——所有流的方向都汇到同一个 send 半与同一条 TCP。download×4 丢得最多（2231 vs 2959，-25%），download×16 因绝对吞吐抬升相对差距收窄到 -10%，而 frp 从 ×4 的 2959 到 ×16 的 2803 几乎不掉，指向 RH 有「每流固定成本 / 单核上限」成分。

---

## 1. 同口径性能事实

数据来源：`docs/rabbithole-vs-frp-multiflow/r6-vs-frp/COMPARE.md:10-15`（三机、同拓扑同工具同场景，每行带直连基线；rabbithole v0.3.2 候选版 vs frp v0.71.0 官方版）。**[实测]**（为仓内 2026-10-15 前后的对比运行产物，本次未重跑基准）。

| 方向×流 | 直连 | rabbithole | frp | RH/frp | RH保留率 | frp保留率 |
|---|---:|---:|---:|---:|---:|---:|
| download×1 | 3738 | **1931** | 1155 | 1.67 | 52% | 31% |
| download×4 | 3072 | **2231** | 2959 | **0.75** | 73% | 96% |
| download×16 | 2522 | **2513** | 2803 | 0.90 | 100% | 111% |
| upload×1 | 3311 | **1958** | 1107 | 1.77 | 59% | 33% |
| upload×4 | 2934 | **1981** | 2169 | 0.91 | 68% | 74% |
| upload×16 | 2355 | **2121** | 2250 | 0.94 | 90% | 96% |

形态归纳（**[实测]** 之上的观察）：

- **非单调**：×1 大幅领先（+67%↑/+77%↓口径见上表 1.67/1.77），×4 落后最多（↓ -25%），×16 收窄（↓ -10%、↑ -6%）。差距随流数先恶化后收窄。
- **下载方向差得多、上传方向差得少**：×4 download 绝对差 728 Mbps，×4 upload 只差 188 Mbps。
- **保留率口径**：RH 的 ×16 download 保留率 100%（2513 已超直连基线 2522 的量级、其实略低 9 Mbps），frp ×4 保留率 96%、×16 达 111%（超过当时的直连基线，说明不同时段直连基线本身有漂移）。
- **CPU 侧**（本次从原始样本复算，方法见 §5 R2 证据）：download×4 客户端 0.592/0.619/0.595 核、服务端 0.905/0.931/0.918 核；download×16 客户端 0.774/0.817/0.800、服务端 0.969/0.969/0.976 核——**服务端下行搬运侧已贴 1.0 核**，客户端 ×16 仅 0.80 核，单核瓶颈在服务端。
- ** frp 侧无 CPU 采样**：`benchmark/peer-frp.sh` 全脚本 grep `cpu` 零命中（对比 `benchmark/peer.sh:195,242` 有 `start_cpu_sampler` 调用），故「frp 有能力摊多核」是调度模型推导而非实测，证据强度弱于 RH 侧。
- **建连/UDP/HTTP 行**（COMPARE.md:27-29）：该轮 RH 与直连均为 0（RH 该轮未跑 UDP/HTTP/建连速率指标），只有 frp 有数（1061.6 建连/s、UDP 81.41 Mbps、HTTP 1085.3 QPS），**该轮指标不可比，不纳入根因分析**。

---

## 2. frp 侧关键机制

引用位置：frp 业务代码 `benchmark/frp-src-v0.71.0/`，frp 锁定的 yamux fork `benchmark/frp-yamux-src/`（对应上游 `github.com/fatedier/yamux@v0.0.0-20250825093530-d0154be01cd6`）。

### 2.1 连接与池（frpConn 语义）

| 机制 | 内容 | 位置 |
|---|---|---|
| 连接池 poolCount | 客户端 Login 申报 PoolCount；服务端取 min(客户端, 服务端 transport.maxPoolCount) 为有效池容量，建容量 = poolCount+10 的有界 workConn 通道，并预热性发 poolCount 条 ReqWorkConn 填池 | `benchmark/frp-src-v0.71.0/client/control_session.go:112`；`benchmark/frp-src-v0.71.0/server/control.go:446,457,690`；`benchmark/frp-src-v0.71.0/pkg/config/v1/server.go:186`；`.../client.go:153` |
| 池满即弃 + 取一补一 | frpc 每应答一次 ReqWorkConn 就新开一条 mux 流注册进池，非阻塞 send，池满直接丢弃并报错；frps 每取走一条立刻再发一条 ReqWorkConn 补位，池维持满水位 | `benchmark/frp-src-v0.71.0/server/control.go:330,335,634` |
| 代理侧重试取流 | 每个新用户连接进来后循环最多 poolCount+1 次 GetWorkConn，取不到走 ReqWorkConn + UserConnTimeout 等待 | `benchmark/frp-src-v0.71.0/server/proxy/proxy.go:150`；`.../server/control.go:607,620` |
| **poolCount ≠ 物理载体** | poolCount=4 只是预建 4 条 **mux 流**，底层仍是同一条 TCP 上的同一个 session；未开 tcpMux 时才每流一条真实 TCP（realConnect） | `benchmark/frp-src-v0.71.0/client/connector.go:141,145,155`；仓内记录 `benchmark/runs/tcp-multiflow-frp-20261015/FRP-YAMUX-ANALYSIS.md:69` 明确「poolCount=4 不是四条物理载体」 |

### 2.2 yamux 流控与授信（frp 锁定的 fork）

| 机制 | 内容 | 位置 |
|---|---|---|
| 6MiB 恒定窗 | `MaxStreamWindowSize = 6*1024*1024`，客户端与服务端各自设置，远大于上游默认 256KiB，窗口**永不自增** | `benchmark/frp-src-v0.71.0/client/connector.go:154`；`.../server/service.go:720`；`benchmark/frp-yamux-src/const.go:126`；`benchmark/frp-yamux-src/mux.go:68`；`.../stream.go:75` |
| 窗口补发抑制（半窗阈值） | Read 消费后 `delta=(max-bufLen)-recvWindow`，仅当 `delta >= max/2` 且无标志位才回 typeWindowUpdate，避免逐包回窗 | `benchmark/frp-yamux-src/stream.go:285-296`（判定在 291-292） |
| 发送窗耗尽即阻塞 | 每次只发 min(sendWindow, 剩余)，原子扣减；窗为 0 进 WAIT 分支等对端 WU 或写超时，由 incrSendWindow 解锁 | `benchmark/frp-yamux-src/stream.go:205,214,228,474` |
| 接收窗越界即协议错误 | recvLoop 拷入 recvBuf 前校验 length > recvWindow → ErrRecvWindowExceeded，发 goAwayProtoErr 后关会话 | `benchmark/frp-yamux-src/stream.go:503`；`.../session.go:619,620` |
| OpenStream 授信 | 先阻塞获取 synCh 信号量（容量 = AcceptBacklog 256），CAS 以 +2 递增分配流 ID，登记后把首帧窗口更新带 flagSYN 发出 | `benchmark/frp-yamux-src/session.go:169,181,186,197,113,106` |
| AcceptStream 授信 | 收 SYN 帧 → incomingStream 建流投进容量 256 的 acceptCh（满则删流回 flagRST）；AcceptStream 取出后再补发一次带 flagACK 的 WU 才算完成授信 | `benchmark/frp-yamux-src/session.go:704,712,244,735,438` |
| 流打开超时 | OpenStream 后起 StreamOpenTimeout（默认 75s）定时器，对端 ACK 前超时直接 Close 整个会话强制重连 | `benchmark/frp-yamux-src/session.go:192,212,225`；`.../mux.go:70` |

### 2.3 单 session 收发放大器

| 机制 | 内容 | 位置 |
|---|---|---|
| send/recv 独立 goroutine | 每 session 固定 1 个 recv goroutine + 1 个 send goroutine（+可选 keepalive）；recvLoop 按 handler 表分发 Data/WindowUpdate/Ping/GoAway，sendLoop 单线程串行写底层 conn | `benchmark/frp-yamux-src/session.go:118,119,120,477,547,538` |
| **发送队列容量 64** | 所有流的数据/控制帧投进同一个 sendCh（cap 64）；投递带 ConnectionWriteTimeout（默认 10s）安全阀 | `benchmark/frp-yamux-src/session.go:108,405,408`；`.../mux.go:67` |
| header/body 两次写 | sendLoop 每轮先写 12 字节 header，再把 body 拷进共享 bytes.Buffer 后单独写一次；sendReady.mu 保护 body 拷贝防调用方复用缓冲 | `benchmark/frp-yamux-src/session.go:487,499,513,86` |
| 写路径等落盘 | Stream.write 与 sendWindowUpdate/sendClose 走 waitForSendErr，入队后阻塞等 Err 通道——该通道由 sendLoop 在帧真正写完后 asyncSendErr，即每个流写调用阻塞到其帧已被写出 | `benchmark/frp-yamux-src/stream.go:219,303,321`；`.../session.go:432,522` |
| keepalive/GoAway | 默认每 30s 一次 Ping；frp 用 TCPMuxKeepaliveInterval 覆盖，并在 TCPMux 开启时把应用层心跳置 -1（只依赖 mux 保活） | `benchmark/frp-yamux-src/session.go:363,371,313`；`benchmark/frp-src-v0.71.0/client/connector.go:151`；`.../server/service.go:717` |
| recvBuf 按需分配 | 流对象不预分配接收缓冲，首次收到数据帧才按该帧 length 精确分配；Shrink() 供池场景回收 | `benchmark/frp-yamux-src/stream.go:509,558` |
| 会话级关闭 | Close 置 shutdown、关 shutdownCh、关底层 conn、等 recvLoop 退出后对所有流 forceClose，再等 sendLoop 退出；exitErr 汇总并触发同样序列 | `benchmark/frp-yamux-src/session.go:271` |
| 流关闭语义 | Close 置 streamLocalClose 发 flagFIN（半关闭仍可读）；对端 FIN → streamRemoteClose；两端 FIN 才 streamClosed 摘除；对端永不应 FIN 由 StreamCloseTimeout（5min）forceClose 并补 flagRST | `benchmark/frp-yamux-src/stream.go:342,347,375,391,403`；`.../mux.go:69` |
| 关闭后读写报错 | streamRemoteClose/streamClosed 时 recvBuf 排空则 Read 返回 io.EOF；本地关闭后再写 ErrStreamClosed；streamReset 读写返回 ErrConnectionReset | `benchmark/frp-yamux-src/stream.go:108,117,193,198,122` |

### 2.4 中继层（frpRelay 语义）

| 机制 | 内容 | 位置 |
|---|---|---|
| 每流一对 Copy goroutine | `libio.Join` 为每个连接对起恰好 2 个 goroutine，`blocking io.CopyBuffer`，两向都结束才 Wait()；无 selector/ready-queue 式转发，纯 goroutine-per-conn-per-direction | `benchmark/frp-src-v0.71.0/golib@v0.8.2/io/io.go:28-52`（起 goroutine 41-44）；调用点 `benchmark/frp-src-v0.71.0/server/proxy/proxy.go:344`、`.../client/proxy/proxy.go:253` |
| 每流 goroutine 全链路计数 | frps：accept 1 + Join 2 = 3；frpc：InWorkConn 1 + Join 2 = 3；一条流两端约 6 个 goroutine，另有每 session 2 个 yamux 常驻 goroutine 共享 | server 侧 `benchmark/frp-src-v0.71.0/server/proxy/proxy.go:232-265`（accept loop 261、Join 322）；client 侧 `.../client/proxy/proxy_wrapper.go:266-277`、`.../client/proxy/proxy.go:160-167,253` |
| 16KiB 固定缓冲 | CopyBuffer 缓冲硬编码 16*1024，GetBuf 取 PutBuf 还；不走 io.Copy 32KiB 默认分配，不调 splice/sendfile | `benchmark/frp-src-v0.71.0/golib@v0.8.2/io/io.go:36-38` |
| sync.Pool 4 档复用 | bufPool16k/5k/2k/1k 四桶，GetBuf 按请求 size 分桶、PutBuf 按 cap 分桶（16KiB 档实际可能复用任意 ≥16KiB 切片） | `benchmark/frp-src-v0.71.0/golib@v0.8.2/pool/buf.go:21-27,29-52,54-68` |
| 无零拷贝 | io.copyBuffer 仅在 src 实现 WriterTo / dst 实现 ReaderFrom 时短路；grep 确认 yamux Stream 与 golib pool 全无这两个方法，且 frp 套了 limit/snappy/crypto 等包装层，上传永远「读进 16KiB 用户态缓冲 → 再写出」 | Go 1.27.2 `GOROOT/src/io/io.go:407-427`；grep `yamux@v0.0.0-20250825093530-d0154be01cd6/stream.go` 与 `golib@v0.8.2/{io/io.go,pool/buf.go}` 无命中；包装链 `benchmark/frp-src-v0.71.0/client/proxy/proxy.go:134-154` |
| **无半关闭** | pipe 两侧 defer 均为 `Close()`，任一向 EOF 立刻拆掉两端；frp client/server/pkg 全目录 grep `CloseWrite/CloseRead` 零命中（唯一命中在 `pkg/ssh/server_test.go:94` 是测试夹具） | `benchmark/frp-src-v0.71.0/golib@v0.8.2/io/io.go:31-38` |
| yamux 层其实支持半关闭但用不上 | streamLocalClose 注释明确「LocalClose only prohibits further local writes. Handle reads normally.」，Close() 发 FIN 后本端仍可读；被 Join 的 EOF 即双向 Close 堵死 | `yamux@v0.0.0-20250825093530-d0154be01cd6/stream.go:105-119` |
| FIN 传播 | Close 在 Established 状态置 streamLocalClose 并 sendClose（typeWindowUpdate + flagFIN）；对端 processFlags 收 FIN 置 streamRemoteClose，recvBuf 排空后 Read 返回 io.EOF | fork `benchmark/frp-yamux-src/stream.go:314-329,440-457,332-387` |
| server 侧转接 | accept → handleUserTCPConnection：NewUserConn 钩子 → GetWorkConnFromPool 取 stream 并写 StartWorkConn（带 src/dst）→ 按需套 encryption/compression/rate limit → joinUserConnection → libio.Join | `benchmark/frp-src-v0.71.0/server/proxy/proxy.go:268-328`（取连接 288、包装 294-314、Join 322）、GetWorkConnFromPool 147-199 |
| client 侧转接 | Wrapper.InWorkConn 起 goroutine → BaseProxy.InWorkConn → HandleTCPWorkConnection：wrapWorkConn（rate limit → encryption → compression）→ 有 ProxyProtocol 先写 header → libnet.Dial 本地目标（10s）→ libio.Join 两向拷贝 | `benchmark/frp-src-v0.71.0/client/proxy/proxy.go:160-167,170-258`、入口 `.../client/proxy/proxy_wrapper.go:266-277` |
| compression 池 | WithCompressionFromPool 从 sync.Pool 取 snappy Reader/Writer，Join 结束后 defer 归还；默认关闭 | `benchmark/frp-src-v0.71.0/golib@v0.8.2/io/io.go:87-101`、`.../pool/snappy.go:25-54`；调用点 `.../client/proxy/proxy.go:149-152,227-229`、`.../server/proxy/proxy.go:304-308` |
| 全路径拷贝汇总 | TCP→16KiB 池化缓冲（Read）→yamux Stream.write→Go channel（指针传递，bodyBuf 在 sendLoop 再拷一次）→bufio.Reader→frame 直接 Copy 进 recvBuf→对端 16KiB 缓冲→TCP；单流单向上行约 2 次 memcpy + 2 次 syscall 写，全部挤在共享 sendLoop 单 goroutine 上 | `benchmark/frp-src-v0.71.0/golib@v0.8.2/io/io.go:36-38`；`benchmark/frp-yamux-src/stream.go:170-231,486-528,477-527`；`.../session.go:102` |

### 2.5 取流路径差异（每流启动成本不共起跑线）

frps 侧每用户连接从 `workConnCh` 有界 channel **直取**一条流（`benchmark/frp-src-v0.71.0/server/control.go:457,690`；池定义 49）；RH 服务端每用户连接要经自研 `TcpPool`（ready 锁 + waiter 队列 + issue_create_channel 签发 + pool_wait_timeout）取预建流，见 `crates/rh-server/src/data.rs:158-200`。**两者的每流启动成本不在同一起跑线**，这是多流对比中一个不可比因素（[代码可证]）。

---

## 3. rabbithole 侧关键机制

引用位置：`crates/rh-transport/src/`（mux.rs / lib.rs / tcp.rs / splice.rs）、`vendor/yamux/src/`（connection.rs + connection/stream*、connection/halves/shard.rs、frame/io.rs）。所有行号本次已实际打开核对。

### 3.1 多路复用层（rhMux）

| 机制 | 内容 | 位置 |
|---|---|---|
| 多物理载体池 | K 条独立 TCP/TLS 各跑一套完整 yamux，开流按全局原子计数 `i = seq % K` 轮转分派（四元组不同 → RSS 散核/TLS 并行流水线）；载体死亡即剔除并在其余载体重试；客户端按配方退避重拨 | `crates/rh-transport/src/mux.rs:1021-1025`、`crates/rh-transport/src/mux.rs:1135-1187`、`crates/rh-transport/src/mux.rs:1220-1255`；退避 `crates/rh-transport/src/tcp.rs:348-361`；服务端合流 `crates/rh-transport/src/lib.rs:598-609` |
| **池未接线（关键事实）** | `connect_pool()` / `yamux_pool_client` / `yamux_pool_server` 在全部业务 crates 零生产调用方——客户端建连入口 `crates/rh-client/src/session.rs:89` 仍是 `rh_transport::connect(&tcfg)` 单连接；服务端 accept 路径 `crates/rh-transport/src/tcp.rs:53-77` 的 `wrap_stream_as_server(_duplex)` 无池合流。grep 命中仅定义处与测试 | 定义 `crates/rh-transport/src/lib.rs:426,439,599`；唯一转发 `crates/rh-transport/src/tcp.rs:362`；测试 `crates/rh-transport/src/mux.rs:1780` |
| 载体选择 | 裸 TCP 经 fd 复制得写侧副本 → `into_halves_duplex` 无锁读∥写；TLS/WS 或 try_clone 失败 → 单载体经 SplitSocket(BiLock) 串行化 | `crates/rh-transport/src/mux.rs:850-882`；`crates/rh-transport/src/tcp.rs:315-325`；`vendor/yamux/src/connection.rs:1548-1560` |
| 收发双任务拓扑 | halves 四元组（接收半 Framer、发送半 FlushTask、N 个接收分片 Worker、N 个发送分片 Worker），全部由 rh-transport spawn 成独立任务；Worker 是裸 Future，yamux 不 spawn（futures-only 红线） | `crates/rh-transport/src/mux.rs:859-923`、`crates/rh-transport/src/mux.rs:1266-1352`、`crates/rh-transport/src/mux.rs:1357-1378`；`vendor/yamux/src/connection.rs:1516-1529` |
| 按流分片并发（sid%N，N=4） | 接收侧 RecvWorker 独占 sid%N 流集合做解包/派发/流控记账/唤醒；发送侧 SendWorker 独占同模数分片做命令消费/帧转换/on_drop；N 取 vendor 默认 4，mux.rs 不覆盖 | `vendor/yamux/src/connection.rs:1582-1619,1933-1935`；`vendor/yamux/src/connection/halves/shard.rs:276-278,940-942`；`crates/rh-transport/src/mux.rs:832-836` |
| 命令通道容量 | 每流 StreamCommand cap 64；总刻度：open_tx 64、accept_tx 256、分片帧通道 128、send_out 128、new_streams 16。满即停是本地调度背压而非流控 | `vendor/yamux/src/connection.rs:367,2281,1590-1600,1939-1960`；`vendor/yamux/src/connection/halves/shard.rs:57,63,70`；`crates/rh-transport/src/mux.rs:909-910` |
| 恒定 6MiB 接收窗 | `set_max_connection_receive_window(None)` + `set_initial_stream_receive_window(6*1024*1024)`；FlowController.max_receive_window 恒等于配置初窗，与流数无关；round-E1 已删 256MiB 共享预算与 BDP 自增；内存护栏只靠 max_num_streams | `crates/rh-transport/src/mux.rs:767-802`、`:801`、`:831`；`vendor/yamux/src/connection/stream/flow_control.rs:44-45,112-132,308-330` |
| 建流授信 | 建流即把预授差额（6MiB−256KiB）以带初始 flag 的 WU 帧授出（outbound SYN\|WU、inbound ACK\|WU），不再等首个读 poll；入站首帧 body ≤ DEFAULT_CREDIT，WU\|SYN 的 credit = 帧值 + 256KiB 作初发窗；try_send 失败则回滚记账交还读路径 | `vendor/yamux/src/connection/stream.rs:172-211,142-146,161-169`；`vendor/yamux/src/connection/stream/flow_control.rs:87-98,104-110`；`vendor/yamux/src/connection/halves/shard.rs:564-657` |
| 读就绪授信与半窗回补 | 读半每 poll 先 poll_ready 再 next_window_update（半窗过后才回补已耗额度、不自增窗），同时把共享待 ACK 以 0-credit WU+ACK 补发 | `vendor/yamux/src/connection/stream.rs:475-513`；`vendor/yamux/src/connection/stream/flow_control.rs:58-80` |
| 发送窗消耗与分片帽 | 写侧每 poll_write_chunk 按 min(send_window, chunk 剩余) 取帧体；`set_split_send_size(6MiB)` = 单流窗界即不再人为分片；实际上线帧大小仍由 CHUNK_CAP=64KiB 步进决定；send_window 为 0 时登记 writer waker 挂起 | `vendor/yamux/src/connection/stream.rs:551-573,560-561`；`crates/rh-transport/src/mux.rs:809`、`:294` |
| 帧源优先级 | FlushTask 每轮：to_send（接收侧压来的 Pong/GoAway）> rtt ping > send_out[k] 严格轮转（逐帧 try_pop + 游标推进，防固定顺序饿死后段 Worker） | `vendor/yamux/src/connection.rs:2213-2228,2204-2210`；`vendor/yamux/src/connection/halves/shard.rs:1173-1223` |
| **拆分路径无公平闸门** | `impl<T> Active<T>`（起始 `vendor/yamux/src/connection.rs:420`）的 Round-A/C 公平读探测（`:482-519`）在生产中不经——rh-transport 两路入口全走 halves/duplex；`vendor/yamux/src/connection.rs:1044-1047` 明确「拆分后的发送侧 poll 不含 Round-A/C 的公平调度读探测」 | `vendor/yamux/src/connection.rs:420,482-519,1044-1047`；入口 `crates/rh-transport/src/mux.rs:859,880` |
| 发送侧批聚合 | Sink 侧 poll_write_batch 把 current 余段 + 队列后续帧拼成段数组（段数上限 129，栈上 IoSlice）一次 poll_writev；部分写按字节跨帧核销、单帧不跨 writev 拆分；round-7 扩到 **64 帧 / 1MiB** | `vendor/yamux/src/frame/io.rs:344-447`；上限 `vendor/yamux/src/frame/io.rs:64,71,78`；`vendor/yamux/src/connection.rs:2191-2205` |
| 帧头+帧体向量写 | YamuxCarrier 实现 Writev2：poll_write2 把 [帧头, 帧体] 单次 writev 下发；poll_writev 透传 tokio poll_write_vectored；不支持向量写的载体退化为顺序写 | `crates/rh-transport/src/mux.rs:729-758`、`:678-684` |
| 死槽与池联动 | 共享 dead AtomicBool：任一半 fatal / RecvHalf Drop 置 Closed 唤醒对侧；open_stream 先置 dead 再回 Err，池据 is_dead 换载体重试 | `crates/rh-transport/src/mux.rs:951-953,1288-1295,1377`；`vendor/yamux/src/connection.rs:1984-1993` |

### 3.2 中继层（rhRelay）

| 机制 | 内容 | 位置 |
|---|---|---|
| 三条路径与判定顺序 | mux 流 → splice 内核零拷贝 → 用户态 copy，逐级下探、失败即原样回落不报错 | `crates/rh-server/src/data.rs:281-343` |
| chunk 快路径条件 | 同时满足：无压缩、relay_mode != "copy"、经 QuotaIo/PrependedIo 剥壳后仍是未包装的 RhMuxStream | `crates/rh-server/src/data.rs:281-298`；剥壳判定 `crates/rh-transport/src/mux.rs:666-676` |
| up 方向搬运（chunk） | 自本地目标读入池化 BytesMut：内核→池缓冲一次拷贝，freeze 成 Bytes 后整块作 yamux 帧体，不经 pool 再拷；两相缓冲（初始 12KiB 本地，读满升级 64KiB 池），单块上限 64KiB | `crates/rh-transport/src/mux.rs:453-476,222-255,261-287` |
| down 方向搬运（chunk） | 满块（≥64KiB）以 Bytes 零拷贝直写（单次 syscall）；小块仅增引用计数暂存 Vec<Bytes>（0 拷贝），达 64KiB 字节或 128 段上限即经 write_vectored 一次 writev；聚合后继续收割已就绪块保延迟 | `crates/rh-transport/src/mux.rs:499-662`；段数上限 `:478-482`；聚合收割 `:637-657` |
| 限速三态合同 | up=读侧计费（fill 完成即预留并即时消费）；down=送达计费（冲刷前预留、按实际写出逐步消费，取消/部分写由 Reservation::drop 自动退还）；双桶皆空时不装 limiter | `crates/rh-transport/src/mux.rs:456-476`、`:499-590`；trait `crates/rh-transport/src/lib.rs:141-153` |
| mux=true 载体拓扑 | 收/发双 driver + 双侧 4 分片 Worker，全连接共享；背压只停读侧不拖死发送 | `crates/rh-transport/src/mux.rs:884-929`；worker 数 `vendor/yamux/src/lib.rs:108`；`crates/rh-transport/src/mux.rs:832-836` |
| 每流任务拓扑 | 每条 mux 流（经 steal 拆半）由 2 个 tokio 任务承载：up 泵（a_r→b_write）与 down 泵（b_read→a_w），各由 AbortOnDropHandle 守卫——父 future 被 drop 即 abort 双向子泵，不 detach | `crates/rh-transport/src/mux.rs:359-451`；守卫 `crates/rh-transport/src/lib.rs:59-120` |
| 每流收敛合同 | clean EOF → 无界等完整半关闭（不加短超时）；单向 IO 错误/panic → 立即终止对向泵；取消与错误路径收尾受 CANCEL_CLOSE_GRACE=1s 约束 | `crates/rh-transport/src/mux.rs:437-451,303-322`；服务端同款 `crates/rh-server/src/data.rs:483-543,350,392-403` |
| splice 资格 | 两端均裸 tokio TCP（plain_tcp 用 as_any downcast 判定），无 TLS/WS/yamux/压缩/QuotaIo 包装；服务端另要求 relay_mode != "copy" 且无压缩；dup 出独立 fd 走 AsyncFd，原 tokio 流不动；任一步资源失败回落 | `crates/rh-server/src/data.rs:301-327`；客户端 `crates/rh-client/src/forward.rs:135-162`；dup `crates/rh-transport/src/splice.rs:553-555,628-654` |
| splice 管道与突发 | 每方向一条独立 pipe（F_SETPIPE_SZ 目标 256KiB，失败回落 64KiB 并记录最小观测值）；单次排空上限 DRAIN_SLICE=256KiB；写就绪低水位 TCP_NOTSENT_LOWAT 默认 1MiB（RH_NOTSENT_LOWAT 可覆盖，0=不设） | `crates/rh-transport/src/splice.rs:30,33,39,570-608,634-654` |
| splice 批次预算 | 默认臂 pump_batched：非阻塞快试 fill/drain，仅观察到 EAGAIN 后才等就绪并 clear_ready；无真实等待的连续搬运段内每 BATCH_BYTES=4MiB 或 BATCH_OPS=64 次操作 yield_now() 一次；真实等待本身即「批」之界 | `crates/rh-transport/src/splice.rs:61-63,944-1136,1126-1135` |
| splice 无忙转与取消可达 | 未观察 EAGAIN 不清读侧就绪缓存；批首以 biased select 同步 poll 一次 cancel_wait；teardown 双向 SHUT_RDWR；读 EOF 走 shutdown_wr 半关闭 | `crates/rh-transport/src/splice.rs:961-968,896-899,971-974,1112-1114` |
| 客户端中继路径 | handle_start_forward 同式择路：chunk 快路径 → mux_relay；否则裸 TCP → splice_relay；再否则用户态 copy；客户端无规则级联取消 | `crates/rh-client/src/forward.rs:118-164`（mux 127、splice 154） |
| 本地目标拨号 | dial_target 直接 TcpStream::connect（10s 超时 + set_nodelay），IP 直返；这条裸 TCP 是 splice 资格另一端 | `crates/rh-client/src/forward.rs:213-231` |
| 经代理拨号 | proxy_url 配置时经 HTTP CONNECT / SOCKS5 握手拿目标 TCP，之后 set_nodelay + keepalive；返回仍是裸 tokio TcpStream，代理跳不参与 splice 资格判定 | `crates/rh-transport/src/tcp.rs:449-460`；`crates/rh-transport/src/proxy_dial.rs:130-169,171-249` |
| TCP 上传路由不经 relay | router/tunnel 层（handle_tunnel + pump_delivery 逐帧写出）独立于 rh-transport 中继层，与 splice/chunk 无耦合 | `crates/rh-server/src/control.rs:641-730`（写任务 711-728） |

---

## 4. 机制差距对照（verifiedGaps）

### G1 多物理载体池已实现但未接线【代码可证】

- **frp 侧**：`benchmark/frp-src-v0.71.0/client/connector.go:141-156`——TCPMux 建唯一 muxSession（L145 判 TCPMux → L147 realConnect() → L154 MaxStreamWindowSize=6MiB → L155 fmux.Client 单 session）；`benchmark/peer-frp.sh:130` 的 poolCount=4 仅预建流（`server/control.go:690` for i:=0;i<poolCount;i++ 发 ReqWorkConn，客户端 `control.go:124` 逐个开流），池语义仅此。仓内 `benchmark/runs/tcp-multiflow-frp-20261015/FRP-YAMUX-ANALYSIS.md:69` 明确「poolCount=4 不是四条物理载体」。
- **RH 侧**：`crates/rh-client/src/session.rs:89` 调 `rh_transport::connect(&tcfg)`（单连接）；`connect_pool()`（`crates/rh-transport/src/lib.rs:599`）、`yamux_pool_client`（`lib.rs:426`）、`yamux_pool_server`（`lib.rs:439`）在生产 crates 中零调用方（grep 仅命中定义 `lib.rs:16` 再导出、转发 `tcp.rs:362`、测试 `mux.rs:1780`）；服务端 accept 路径 `crates/rh-transport/src/tcp.rs:53-77` 无池合流。（原文此处还列了 `YAMUX_POOL_DEFAULT_CARRIERS=4` 作同类证据——该常量已于 round-45 删除，见文首订正。）
- **结论**：**这不是 frp 的领先项，而是 RH 自带的未启用杠杆**——池的重拨/故障隔离/轮转代码齐备（`mux.rs:1206-1292`，`tcp.rs:352` carriers<=1 时等价单连接、行为零漂移），本次 `cargo test -p rh-transport --lib mux::` 17/17 通过（含 yamux_pool_carrier_failure_isolated / yamux_pool_redial_recovers / yamux_pool_round_robin_across_carriers / yamux_pool_server_accept_merges）。

### G2 下行多流缺少跨流写出口 + 调度模型差异【需实测确认】

- **frp 侧**：`benchmark/frp-src-v0.71.0/server/proxy/proxy.go:344` libio.Join(local, userConn)；`golib@v0.8.2/io/io.go:28-45` 每向一个 goroutine，L36 GetBuf(16*1024) + L38 io.CopyBuffer；每连接一对 goroutine 由 GMP 抢占到多核；accept goroutine `proxy.go:261`。
- **RH 侧**：`crates/rh-transport/src/mux.rs:499-662` pump_down（write_vectored 聚合 ≤SMALL_CHUNK_SEGS=128 段，`:482`；≥64KiB 整块走 `write()` 标量单写 `:615-620`；按 64KiB 或 128 段冲刷 `:631/643`）；每流 up/down 双泵 `:359/401`（AbortOnDropHandle 守卫 `:433-435/442-443`）；生产调用方 `crates/rh-server/src/data.rs:683`、`crates/rh-client/src/forward.rs:127`；runtime 为多线程（`crates/rh-server/src/main.rs:207`、`crates/rh-client/src/main.rs:35`，未显式 worker_threads）。
- **CPU 事实**（本次复算）：download×16 客户端 ~0.80 核、服务端 ~0.97 核——**单核瓶颈在服务端下行搬运**；download×4 客户端 0.592–0.619 / 服务端 0.905–0.931。
- **证据边界**：frp 侧无 CPU 采样（`benchmark/peer-frp.sh` 无 start_cpu_sampler），「frp 摊多核」是 GMP 协作式推导而非实测，因果（抢占 vs 锁/唤醒序）不可分离。

### G3 半窗回补阈值 + CHUNK_CAP 读步进【代码可证，结论：非差距来源】

- **frp 侧**：`benchmark/frp-yamux-src/stream.go:285-296`（delta = (max−bufLen)−recvWindow；L291 if delta < max/2 && flags == 0 跳过）+ 16KiB 读步进（`golib io.go:36-38`）；max 恒 6MiB 来自 `connector.go:154` / `server/service.go:720`，无自增。
- **RH 侧**：`vendor/yamux/src/connection/stream/flow_control.rs:58-80`（半窗阈值 L66）+ `flow_control.rs:44-45`（max_receive_window 恒等于配置初窗，无 BDP 自增）+ `crates/rh-transport/src/mux.rs:294` CHUNK_CAP=64KiB（测试 `mux.rs:1513-1515` 锁死）+ `mux.rs:831` 初窗 6MiB；读路径触发点 `vendor/yamux/src/connection/stream.rs:288-316`。
- **判定**：两侧窗语义完全同构；RH 读步进 64KiB 比 frp 的 16KiB **少约 4/5 的 WU 控制帧**，理论上微优。**排除「窗口不足」假设**——×16↓ 2513 vs 2803（-10%）无法用窗口机制解释，此项为中性/微负（对 RH 有利），列出以缩小排查面。
- **口径说明**：frp 的 delta 从 recvBuf 长度推导（随应用读走字节回窗），不是按「读了几次」计；「半窗 3MiB/16KiB≈192 次、/64KiB=48 次」只是等效描述。

### G4 写路径调度代价 + Round-A 引用勘误【代码可证（勘误部分）】

- **frp 侧**：`benchmark/frp-yamux-src/session.go:118-119`（go s.recv() + go s.send()）、`:477-527` sendLoop 单 goroutine 串行写（select 阻塞取 sendCh L487，L493 conn.Write(hdr) + L517 conn.Write(bodyBuf) 两次 syscall），sendCh cap 64（`:108`）。
- **RH 侧**：`vendor/yamux/src/connection.rs:2022-2178`（impl SendHalf=FlushTask：poll_ready → fill_batch，sink 不满即 continue、满即 Pending）+ `vendor/yamux/src/frame/io.rs:344-447`（MAX_BATCH_FRAMES=64 `io.rs:64` / MAX_BATCH_BYTES=1MiB `io.rs:71` / MAX_WRITEV_SEGMENTS=129 `io.rs:78`）。
- **勘误（采纳批评）**：输入材料曾把 Round-A 公平闸门（`vendor/yamux/src/connection.rs:482-519`，每 16 次载体写/256KiB 强制读探测）算作写路径成本与 RTT 敏感放大的解释。该段属 `impl<T> Active<T>`（impl 起始 `connection.rs:420`）——**未拆分单任务路径**；RH 生产全部走 into_halves/into_halves_duplex（`crates/rh-transport/src/mux.rs:859/880`），发送侧 SendHalf/FlushTask **不含**该探测（`vendor/yamux/src/connection.rs:1044-1047` 明言）。**「写—探测交替」在生产代码里不成立，该成因表述应予删除。** 另：REPORT-round6 §4 的 CPU 证据（`benchmark/runs/tcp-multiflow-frp-20261015/ab/REPORT-round6.md:77`，paced 200M×16 CPU 0.226→0.239 核）只对 halves 路径有效，不能反过来为 Active 路径的探测成本背书。
- **保留部分**：批越深，载体 Pending 期间上游 send_out 队列（≤128 帧/Worker，`shard.rs:63`）积压越深，单次 writev 阻塞时唤醒/续写跨两个调度轮——这与 round7 报告 §3.4/§4 记录的不可归因部分一致（`round7-acceptance-report.md:89`）。

### G5 任务模型差异在中等流数最不利【需实测确认】

- **frp 侧**：每流全链路约 6 个 goroutine（server accept 1+Join 2，client 1+Join 2），用户态无跨流共享写路径锁；yamux 层每 session 仅 2 goroutine 且由全部流共享——**与 RH 的 2 driver 对称**，不对称在用户态 Join 层。
- **RH 侧**：每流仅 2 个 tokio 泵任务（`crates/rh-transport/src/mux.rs:359/401`）+ 全连接共享 2 driver + 4 RecvWorker + 4 SendWorker（`mux.rs:900-923`，worker 数 `vendor/yamux/src/lib.rs:108` 默认 4，`mux.rs:1870-1886` 测试锁死不覆盖）；跨流共享状态走 ShardChannel 双 waker 槽（`shard.rs:96-99`，cap 128 `shard.rs:59/63`，parking_lot Mutex + VecDeque）。
- **授信门相同**：frp AcceptBacklog 256（`benchmark/frp-yamux-src/mux.go:64`）vs RH MAX_ACK_BACKLOG 256（`vendor/yamux/src/lib.rs:58`），数值一致，不构成差异。
- **附带不可比因素**：RH 服务端每用户连接还要经 TcpPool 取流/签发（`crates/rh-server/src/data.rs:158-200`），frp 侧从 workConnCh 直取（`server/control.go:457`）——每流启动成本不同起跑线。

### G6 服务端无连接级共享接收窗预算【代码可证，结论：非解释项】

- **frp 侧**：`benchmark/frp-yamux-src/mux.go:61-73` DefaultConfig 无连接级预算字段；`stream.go:284-286` delta 只对 per-stream max 计算，全仓无 connection 级窗口预算逻辑。
- **RH 侧**：`crates/rh-transport/src/mux.rs:801` set_max_connection_receive_window(None)（解除连接总窗；setter 断言 `vendor/yamux/src/lib.rs:148-158`）；`flow_control.rs:44-45` max_receive_window 恒等于配置初窗；round-E1（commit 71bc2d7）删除 256MiB 共享预算与 BDP 自增（动机：≥45 并发流后新流被钳死 256KiB 的记账 bug）。
- **判定**：双方都缺，不是 RH 落后。×16↓ 理论在途 16×6MiB=96MiB 是**理论上限**，本次未取得 per-stream 在途字节或 RSS 时序，无法确认 96MiB 是否真的形成压力；「3100 需 BDP≈36MiB」是算术推导（仓库任何报告/代码 grep 无该数值），**不应作为门槛未达的证据引用**。当天直连 ×16 上限仅 2.5–2.8 Gbps，此项列为不可归因项，不建议投入。

---

## 5. 根因归因（rootCauses，按影响排序）

### R1（影响最大）多载体池未接线——能力齐备而未启用 【confidence：代码可证】 ⚠️ **已过期：Round-9 已接线并实测生效，见 §0a**

> **实施结果（2026-10-18）**：客户端 `crates/rh-client/src/session.rs:92-96` 已按 `carriers > 1` 分流到 `connect_pool`；服务端 `crates/rh-server/src/control.rs:294-350` 新增 `attach_additional_carrier`，`run_session_loop` 改为「首载体 + 池」双源合流；协议新增 `Hello.session_id`（`crates/rh-protocol/src/message.rs:83-88`，`skip_serializing_if` 保持旧线格式）。实测 ×16↓ +15.0%、×16↑ +31.2%、×4↑ +22.3%。以下为原始分析文本。

- **归因**：RH 实测跑的是「单物理 TCP + 单 yamux session」，自带的 K=4 多载体池（connect_pool/yamux_pool_client）在生产 crates 零调用方。frp 的每连接结构（poolCount 只是预建流 + 用户态每连接独立 CopyBuffer goroutine）反而天然拿到多核红利。
- **证据**：[代码可证] grep 验证——connect_pool 仅命中定义 `crates/rh-transport/src/lib.rs:599`（及转发 `tcp.rs:362`），客户端入口 `crates/rh-client/src/session.rs:89` 仍是单连接 connect；yamux_pool_server 仅命中定义 `lib.rs:439` 与测试 `mux.rs:1780`，服务端 accept `tcp.rs:53-77` 无合流。池代码就绪的正面证据：`crates/rh-transport/src/mux.rs:1149` `let seq = self.inner.rr.fetch_add(...)` → `alive[seq % n]` 按 seq%K 轮转；重拨/故障隔离 `mux.rs:1206-1292`。[实测] 本次 `cargo test -p rh-transport --lib mux::` → 17 passed, 0 failed。
- **影响**：structural、可完全回收。这不是落后项而是未启用杠杆；启用后单 carrier 写出口从 1 条扩到 4 条（4 个 RSS 队列 + 4 组 TCP 发送缓冲）。**但它不能解释全部差距**（单出口 ≠ 必然 25% 差），且需客户端与服务端同步接线，否则合流 accept 不生效。

### R2 单出口 + 每流固定调度成本被流数放大 【confidence：需实测确认】

- **归因**：16 条流共享唯一 sendLoop/唯一 TCP 写出口；8 个共享 Worker（cap 128 帧/Worker）汇集后竞争同一 FlushTask 与同一 poll_ready/writev 循环；每流 2 个泵任务 + TcpPool 取流固定成本在流数不足以摊薄时被放大。frp 语义等价的 sendLoop 之外，用户态是 GMP 抢占到多核。
- **证据**：[实测] 本次按 `docs/rabbithole-vs-frp-multiflow/r6-vs-frp/rh/results-tcp.jsonl` 的 window_start_us/window_end_us 与 `benchmark/runs/tcp-multiflow-frp-20261015/ab/cpu-r6-cand-{client,server}.jsonl` 逐样本重算（Δticks/clk_tck/窗口秒）：download×4 client 0.592/0.619/0.595 核、server 0.905/0.931/0.918；download×16 client 0.774/0.817/0.800、server 0.969/0.969/0.976——服务端下行已贴 1.0 核。[代码可证] 瓶颈侧 `vendor/yamux/src/connection.rs:2104-2178`（FlushTask 每轮先 poll_ready，sink 不满 fill_batch、满即 Pending）、`shard.rs:63` SEND_OUT_CAP=128；共享 Worker `mux.rs:917-923`；每流泵 `mux.rs:359/401` 同 runtime（`rh-server/src/main.rs:207`、`rh-client/src/main.rs:35` 未设 worker_threads）。
- **影响**：download×4 是差距最大档位（2231 vs 2959，保留率 73% vs 96%）。×4→×16 RH 仅 +12%（2231→2513），frp 从 2959→2803 几乎不掉。**与 R1 的贡献在当前数据上不可分离**（池启用后 CPU 预算摊到 4 核会同时缓解 R2 的单核依赖）。

### R3 round-7 批聚合时序嫌疑 【confidence：需实测确认】

- **归因**：round-7 把发送批扩到 64 帧/1MiB 后，单批 writev 在载体 Pending 期间堆积更深，两段式时延在 RTT 敏感路径放大。
- **证据**：[代码可证] 批上限 `vendor/yamux/src/frame/io.rs:64/71/78`；调度点 `connection.rs:2122-2130`。[实测] `benchmark/runs/tcp-multiflow-frp-20261015/round7-acceptance-report.md:89`：「16 流 download 的 -15.2% 回退中，约 4% 可由直连网络漂移解释，其余 ~11% 需进一步排查是否为批聚合时序或流控交互问题」；同报告 §3.4 download×16 2032 vs 基线 2396。
- **勘误**：G4 原文把 Round-A 公平闸门算进 R3 成因——不成立（该段属 Active 单任务路径，生产不走，见 §4 G4 勘误），已从 R3 剔除；环回 -10% 的成因归分片流水线跳数（round-5→round-6 对比，`ab/REPORT-round6.md` §4 表 19.29→17.31 Gbps），非公平闸门。
- **影响**：仅解释 download ×16 的一部分；方向上不支持 upload（round7 报告 upload×16 +6.0% 正是批聚合收益）。**不是 download×4 -25% 的通用解释**。可用调小 MAX_BATCH_FRAMES 的 A/B 实测证伪/证实。

### R4 唯一剩余结构面 = 写出口与承载并发度 【confidence：代码可证（结构面）+ 实测（CPU）】

- **归因**：下行 16 帧共享唯一 sendLoop 与唯一 TCP 写出口（单 carrier→单 sendLoop→单 TCP），对流数增加无横向扩展能力；而窗口/公平性均已对齐 frp，故落后的唯一剩余结构面就是写出口与承载并发度的缺失——它直接压在已贴 1 核的服务端上。
- **证据**：[代码可证] `vendor/yamux/src/connection.rs:2022` impl SendHalf 为全连接唯一发送半；`connection.rs:1044-1047` 拆分路径无公平探测。[实测] 同上 CPU 复算：download×16 server 0.969/0.969/0.976 核、download×4 server 0.918/0.931/0.905 核。[实测] 单流领先（1931 vs 1155，COMPARE.md:10-16）证明单流经此路径不慢；慢只在多流并发时写出口无法分片——单流领先 + 多流落后的组合只与「并发扩展能力」相容。[代码可证] 排除项：G3（窗口，RH 反而少 4× WU 帧，理论微优）、G6（两侧恒窗无自增）。
- **影响**：download×4 全部 728 Mbps 差距与 ×16 全部 290 Mbps 差距都落在这个结构面上——说明差距只能靠启用多载体/拆出口回收，而不是调窗口或公平性参数。与 R1 是同一枚硬币（杠杆 = R1 的池）。

### R5 ×4 是「固定成本 vs 吞吐摊薄」的最不利交叉点 【confidence：推测】 ⚠️ **仍是当前唯一未解决根因**

> **实施后状态（2026-10-18）**：池化后 ×16 双向反超 frp，×4↑ 也反超（+9.4%），但 **×4↓ 仍落后 26%，且池化使其再降 4.5%**。R5 的「×4 最不利交叉点」形态被证实，但原「固定成本」解释不成立——因为 ×4↑ 在同一固定成本下反而改善了 22.3%。**下行专属路径**才是新方向，见 [x4-download-gap-analysis.md](x4-download-gap-analysis.md)。

- **归因**：每流一对泵任务 + 每 user 连接的服务端 TcpPool 取流/签发固定成本，在流数不足以摊薄、池又以队头阻塞串行化时，把 download 保留率压到 73%；×16 固定成本被吞吐摊薄（收窄到 -10%），单流无此成本（领先 67%）。
- **证据**：[实测] 非单调形态 COMPARE.md:11-16：download×1 RH 1931 vs frp 1155（领先）、×4 2231 vs 2959（-25%）、×16 2513 vs 2803（-10%）；frp ×4 本身也是各档最高（2959）而 ×16 回落 2803。[代码可证] 每流固定项：2 泵任务（`mux.rs:359/401`）+ AbortOnDropHandle 守卫（`mux.rs:433-443`）；TcpPool（`crates/rh-server/src/data.rs:158-200`）vs frp workConnCh 直取（`server/control.go:457`）。
- **反证/边界**：[实测] ×4 CPU 形态 client 0.595 / server 0.918——server 贴 1 核而 client 仍有 40% 余量；若是纯每流任务数成本，客户端也应接近饱和，事实相反，故「×4 最不利」有数据但因果（固定成本 vs 单核封顶）不可分离。
- **影响**：解释 ×4 为何是差距最大档位的形态；描述性归因而非机理。若 R1 启用池后 ×4 与 ×16 同步改善，该归因被改写。**不建议单独作为投入依据**。

### 归因小结

| 根因 | 方向 | confidence | 可回收性 |
|---|---|---|---|
| R1 池未接线 | ×4/×16 双向 | 代码可证 | 结构性、可完全回收（需双侧接线） |
| R2 单出口 + 每流固定成本 | 以下载为主 | 需实测确认 | 与 R1 部分重叠，池启用后同时缓解 |
| R4 写出口/并发扩展度 | 下载为主 | 代码可证（结构面） | 只能靠 R1 的池回收 |
| R3 批聚合时序 | 仅 download×16 | 需实测确认 | A/B 可证伪；非 ×4 -25% 的解释 |
| R5 ×4 交叉点 | 下载×4 | 推测 | 描述性，随 R1 落地可能改写 |
| 排除项 | G3 窗口、G6 连接级预算 | — | 两者均非解释项（对齐 frp） |

---

## 6. 解决方案（countermeasures）

> 排序原则：先做**代码可证、无实测争议**的结构性启用项（C0），再做与 R1 同向收益的写侧聚合项（C1/C2），最后是需 env-gate 的实验项（C3/C4）。所有预期收益均为**估算**，落地前必须同口径 A/B。

### C0（新增、优先级最高）接线多载体池——把 R1 的未启用杠杆落地 ✅ **已实施（Round-9）**

> **实施结果**：见 §0a。×16↓ +15.0%、×16↑ +31.2%、×4↑ +22.3%；×16 双向已超 frp。原「不承诺数字」的表述已被实测替换。以下为原始方案文本。

- **做什么**：客户端 `crates/rh-client/src/session.rs` 由 `rh_transport::connect(&tcfg)` 改为 `rh_transport::connect_pool(&tcfg, carriers)`（`carriers<=1` 时行为零漂移，可用 env 先灰度）；服务端把 accept 到的同会话多条连接经 `Multiplexer::yamux_pool_server` / `yamux_pool_client` 合流，替换单流 accept 语义。
  - **K 取值的修正（round-45）**：原文此处写「推荐 `YAMUX_POOL_DEFAULT_CARRIERS=4`」，该常量**从未被生产代码读取**且已删除。K 的真实默认值由 `rh_transport::default_carriers()` 决定：**tcp/ws = 1、tls/wss = 2、quic/kcp = 1**；显式配置走 `rh-client` 的 `mux_pool_size`（env `RH_MUX_POOL_SIZE`），再经硬件上限与服务端 `max_carriers` 收敛（只减不增）。
  - 本轮实测的 rh4 是**显式把 K 设成 4 的实验臂**（`RH_MUX_POOL_SIZE=4`），不是默认行为；不要把表中的「K=4」读成开箱即得的默认值。
- **为什么排第一**：这是唯一「代码可证」的根因（R1/R4），且能力已实现并有测试背书（本次 `cargo test -p rh-transport --lib mux::` 17/17）。
- **预期收益**：download×4 / ×16 的单出口写拥塞直接候选；理论上单 carrier 写出口从 1 条扩到 4 条（4 个 RSS 队列 + 4 组 TCP 发送缓冲）。具体百分比**无实测支撑，不承诺数字**——单出口 ≠ 必然 25% 差。
- **风险**：中。① 只改客户端不改服务端则池的合流 accept 不生效（G1 已记录服务端同样未接线）；② 池的重拨/剔除会改变故障语义，需回归 carrier 故障隔离用例（已存在于 `mux.rs` 测试）；③ 多 carrier 意味着多 fd/多 TLS 握手，启动成本与内存上升；④ 需确认上游 session 管理层（`rh-client/src/session.rs` 的控制流建立在 mux.open_stream 上）对「流可能来自不同载体」无假设。

### C1 发送批字节上限 1MiB → 2MiB ❌ **实测否决，未实施**

> **回退原因（2026-10-18）**：Round-8 三机消融对同向改动（`READ_RESERVE` 64→256KiB）测得 ×16↑ **-4.0% 确凿回退**（噪声 ±0.1%），机制为单轮 poll 读窗口拉长 4 倍导致每流 WU 回程被推迟、流控信用回路时延增大。`vendor/yamux/src/frame/io.rs:42-50` 已把该实测结论写入注释。按本方案自身门禁「任一回退即停在 1MiB」执行，`MAX_BATCH_BYTES` 维持 `1024 * 1024`（`frame/io.rs:73`）。

- **做什么**：`vendor/yamux/src/frame/io.rs:71` MAX_BATCH_BYTES = 1024*1024 → 2048*1024；`:64` MAX_BATCH_FRAMES 保持 64、`:78` MAX_WRITEV_SEGMENTS 保持 129（32 帧×2 段=64 段 < 129，无需扩段数组）；同步 `vendor/yamux/src/frame/io.rs:1120` T-B3 测试。
- **预期收益**（估算）：单流 +3~8%、多流 +2~5%。依据：`frame/io.rs:64-70` 注释自证 1MiB 在 64KiB 主力帧下仅容纳 16 帧；round-7 报告（`round7-acceptance-report.md` §3.1）已实测 upload×16 +6.0% 归因于批深度扩容。
- **风险**：round-7 报告 §3.4/§4 记录 ×16 download -15.2% 中约 11% 归因未明且该轮正是批深度 256KiB→1MiB 的改动——1MiB→2MiB 存在同向回退可能。**必须先在网络稳定时段做单流/4流/16流三档配对 A/B，任一回退则停在 1MiB**；写优先池化更深会推迟入站读探测，需与 C5 一并验证。

### C2 down 泵满块直写前的兜底聚合阈值 64KiB → 256KiB ⚠️ **改道实施（Round-8 项 4）**

> **实际做法**：未提高阈值，而是把 pump_down 的小块聚合改为 **writev 向量写**（消除用户态 memcpy），三机零回退，×4↓ 2189 为四臂最高。目标（减少写路径开销）以不同路径达成。`CHUNK_CAP=64KiB` 读缓冲红线保持不变（`mux.rs:1827` 测试锁死）。

- **做什么**：`crates/rh-transport/src/mux.rs:608-660` pump_down：把 `:631/:643` 的 `smalls_bytes >= CHUNK_CAP` 与 `:616-617` 的满块直写判定改为 256KiB 新常量（`const DOWN_FLUSH_CAP: usize = 256*1024`，定义在 `:482` SMALL_CHUNK_SEGS 附近）；测试 `crates/rh-transport/src/mux.rs:1814` 附近同步。
- **预期收益**（估算）：download 单流 +5~12%。依据：现行逻辑遇满块立即 flush 并单独 write，每 64KiB 一次 write syscall；round-8 规格 §3 已在接收侧（io.rs:50 READ_RESERVE 64→256KiB）以同向论证落地并声称 read syscall 降 50~75%，写侧尚无对称改动。
- **风险**：低。聚合等待引入的尾部延迟被现有 `:638-657` 的 try_read_chunk 循环兜住（无后续就绪块即冲）。**必须保留 CHUNK_CAP=64KiB 作为读缓冲上限红线**（`mux.rs:1514` 测试不得放宽），只改写侧聚合阈值。若单流实测无增益，说明瓶颈不在 syscall 数，应 no-change 而非继续加码。

### C3 池轮转分派加每载体流数软上限 ❌ **未实施，且已被 ×4↓ 数据提高优先级**

> **状态（2026-10-18）**：轮转仍为 `seq % n`（`mux.rs:1149` 区域）。Round-9 显示 ×4↓ 池化后反而 -4.5%，与「纯轮转把小流分散到多载体」的怀疑一致；这是当前唯一落后档，详见 [x4-download-gap-analysis.md](x4-download-gap-analysis.md)。

- **做什么**：`crates/rh-transport/src/mux.rs:1149-1150` 的 `seq % n` 分派改为负载感知（第 i 个载体在途流数超阈值时顺延下一个）；`mux.rs:991-1005` PoolInner 新增 per-member AtomicUsize 流计数；增减点在 `mux.rs:1151-1159` 开流分支与 `crates/rh-server/src/data.rs:487-488` 转发收尾处（仅加 abort-on-drop 计数器，不改业务逻辑）。
- **预期收益**（估算）：×4 +5~10%、×16 +2~4%。依据：COMPARE.md ×4 upload RH 1981/frp 2169（0.91）、×4 download 2231/2959（0.75）；`mux.rs:956-964` 注释自证池设计前提是「不同本地源端口 → RSS 散列到不同 CPU 核」，而 `:1149` 的纯模 K 轮转不感知每载体负载。
- **风险**：中。计数器必须用 AbortOnDrop 守卫语义增减，否则取消路径泄漏计数会把某载体永久拉黑（反效果）。**更稳妥的顺序**：先做 A/B——不改分派，实测单条 TCP 四元组在 16 流下是否已触软中断单核饱和；若 K=4 池本身已解决问题，则此项无增益、不进默认值。

### C4 splice 泵二级水位（单块管线 → 浅队列）

- **做什么**：`crates/rh-transport/src/splice.rs:944-1136` pump_batched 新增 fill 分支的二级水位判断（`splice.rs:970` splice_sys 前查 pipe 驻留 >= PIPE_FILL_BLOCKS 才转 drain）；把 `splice.rs:33` DRAIN_SLICE 循环内的 break 条件（`:1117-1119`）改为受 pipe 水位门控；同步改写 `splice.rs:81-84` 关于 `eagain_drain_pipe_full` 结构性不出现的注释。
- **预期收益**（估算）：splice 单流 +5~10%。依据：`splice.rs:81-84` 自证当前管线深度=1（fill 时刻管道必空）；g1 诊断报告 §7 实测 server 写等待 78%、client 写等待 88%。
- **风险**：高（相对其他项）。`splice.rs:87-92` 明确要求每 fill 块完成即 up 计量、先于限速等待，加深管线后「pipe 满 → 转 drain」路由必须新增（现结构上不存在），计量/取消/abort 检查粒度都会变；g1 报告 §6 已记录「排空 64/16KiB、MORE/CORK、并行 pipe、不同 pipe 容量均无双向达 2800 的支持证据」与一次 blocking 并行 pipe 原型的控制连接失败。**必须 env-gated（RH_SPLICE_PIPE_DEPTH，默认 1=现状）跑同树对照，未证明收益不进默认值。**

### C5 Active/Framer 读探测预算放宽 16→32 次 / 256KiB→512KiB ❌ **未实施（C1 前置条件不成立）**

> **状态（2026-10-18）**：C1 已被实测否决，C5 作为其前置条件自然不成立。常量维持 `connection.rs:57/64` 的 16 与 256KiB。

- **做什么**：`vendor/yamux/src/connection.rs:57` FAIRNESS_MAX_WRITE_OPS_BETWEEN_READ_PROBES 16→32、`:64` FAIRNESS_MAX_WRITE_BYTES_BETWEEN_READ_PROBES 256*1024→512*1024；`:501-519` 批量排空上限同步改引（已引用同一常量，无需改）；`connection.rs:2969` 断言自动跟随。
- **预期收益**：无直接吞吐增益，是 C1 的前置条件——C1 把写批加深到 2MiB 后，若预算仍在 256KiB，每次 2MiB 批都会被强制读探测切碎，深聚合收益被抵消。
- **风险**：中。该常量是 Round-A/C 防写饿死的唯一闸门（`connection.rs:484-493`），放宽让入站 WU 最大处理时延翻倍，信用回路敏感的单流场景可能反向钳制。**必须与 C1/C2 同轮 A/B；若单流 download 回退则整体回退 C1+C5，只保留 C2。**

### C6 splice socket 显式 SO_RCVLOWAT/SO_SNDLOWAT

- **做什么**：`crates/rh-transport/src/splice.rs:628-659` SpliceSock::dup（已有 notsent_lowat 装配点）新增 rcvlowat/sndlowat 设置；载体侧 `crates/rh-transport/src/tcp.rs:122-128` try_dup_tcp 复制点继承同一低水位。
- **预期收益**（估算，置信度低）：+2~4%。`splice.rs:34-39` 自证低水位是该路径核心旋钮（NOTSENT_LOWAT=1MiB，128KiB 时单流被卡 ~2G）；但 g1 报告 §6 已记录「排空不同尺寸、MORE/CORK 无双向达 2800 支持证据」，只给低区间预期。
- **风险**：低（env-gated、内核不支持静默忽略）。低水位抬得过大可能在空闲/小包期引入 ~2 秒级等待（MORE/CORK 永久开启的教训已是永久不采纳项）。**必须可 RH_* env 覆盖并默认取现值，只做 A/B 采集。**

### C7 代理拨号 socket 缓冲补齐（排在最后）

- **做什么**：`crates/rh-transport/src/proxy_dial.rs:120-128` 在 set_nodelay 之后补 set_rcvbuf/set_sndbuf；与 `crates/rh-transport/src/tcp.rs:450-461` 收敛为同一 helper。
- **预期收益**：对无代理基准为 **0，不做收益承诺**（COMPARE.md 的 peer.sh/peer-frp.sh 均不使用代理）。仅补齐代码路径一致性。
- **风险**：极低。按「不提议无依据改动」原则排在最后——建议 C1-C3 落地后，若有代理部署实测需求再做，当前不占用验收轮次。

### 落地顺序与门禁建议

1. **第一轮（结构性）**：C0 接线池（客户端+服务端同步）→ 三机 ×1/×4/×16 双方向配对 A/B。这一轮单独就应解释相当部分差距，且其结果直接决定 R2/R5 的归因是否改写。
2. **第二轮（写侧聚合，视第一轮结果）**：C1 + C5 同轮 A/B，C2 独立可分离。门禁同 C1 风险条：任一回退即停。
3. **第三轮（实验性，env-gate）**：C3、C4、C6 只作为同树对照采集，不进默认值。
4. **C7**：不排入验收轮次。
5. **共同前置**：所有 A/B 必须在网络稳定时段、direct/tunnel 配对、每臂 ≥3 样本（对齐 `benchmark/peer.sh` 口径），并**同步采集 CPU**（RH 侧 peer.sh:195/242；frp 侧 peer-frp.sh 无采样器——若需 frp 侧 CPU 对比，需先给 peer-frp.sh 补采器，否则 frp 侧「摊多核」永远是推导）。

---

## 7. 独立审读意见与修订记录

**前置声明（材料完整性）**：本次输入材料中「审读意见」字段（critique）实际交付为 `[object Promise]`，即内容未解析完成、**不可读**。因此本节无法逐条呈现独立审读者的原始意见，也不能虚构其内容。以下记录的是：本报告编制过程中**从输入其他字段（verifiedGaps / rootCauses 内嵌的「勘误」「口径说明」「证据边界」）中实际识别并采纳的修订**，以及本报告自己执行的复核修订。

### 已采纳的修订

| # | 修订内容 | 来源 | 在本报告中的落点 |
|---|---|---|---|
| 1 | **Round-A 公平闸门引用勘误**：输入初版（G4/R3 早期表述）把 `connection.rs:482-519` 的每 16 次写/256KiB 强制读探测算作生产写路径成本与 RTT 敏感放大解释。复核确认该段属 `impl<T> Active<T>`（impl 起始 `connection.rs:420`），RH 生产两路入口全走 halves/duplex，`connection.rs:1044-1047` 明言拆分后发送侧 poll 不含该探测——「写—探测交替」在生产代码不成立，该成因表述删除 | verifiedGaps G4「关键勘误」 | §4 G4（勘误段）、§5 R3（已从 R3 剔除）、§6 C5（降级为 C1 前置条件而非独立收益项） |
| 2 | **REPORT-round6 CPU 证据不可跨路径背书**：paced 200M×16 CPU 0.226→0.239 核（`ab/REPORT-round6.md:77`）只对 halves 路径有效，不能用来为 Active 路径探测成本背书；环回 19.29→17.31 Gbps 的 -10% 成因归 round-5→6（SendWorker 分片）的分片流水线跳数，而非公平闸门 | verifiedGaps G4 | §4 G4 勘误段、§5 R3 勘误段 |
| 3 | **G1 定性修订**：多载体池差距不是「RH 落后于 frp」，frp 的 poolCount=4 只是预建流（FRP-YAMUX-ANALYSIS.md:69），RH 是「自带能力未启用」；且需客户端+服务端同步接线（服务端 `tcp.rs:53-77` 也无合流），否则 accept 不生效 | verifiedGaps G1 | §4 G1、§5 R1、§6 C0（新增为第一优先级） |
| 4 | **×4 CPU 数据缺口补齐**：此前记为缺失的 download×4 CPU，本次从原始样本复算取得（client 0.592–0.619 / server 0.905–0.931 核），不再是缺口 | rootCauses R2/R5 证据段 | §1、§4 G2、§5 R2/R5 |
| 5 | **G6 证据边界修订**：「16×6MiB=96MiB 在途」只是理论上限，未取得 per-stream 在途字节/RSS 时序，不能确认形成压力；「BDP≈36MiB 才不被 RTT 钳制」是算术推导（仓库 grep 无该数值），**不得作为门槛未达的证据**；G6 列为不可归因项、不建议投入 | verifiedGaps G6 补充 | §4 G6、§5 R4 排除项 |
| 6 | **G3 重定性**：半窗 + CHUNK_CAP 读步进不是差距来源，反而 RH 少 4/5 WU 帧理论微优；列出只为缩小排查面。同时修正口径：frp 的 delta 从 recvBuf 长度推导，非按读次数计，「192 次/48 次」只是等效描述 | verifiedGaps G3 | §4 G3、§5 R4 排除项 |
| 7 | **frp 侧无 CPU 采样**：`benchmark/peer-frp.sh` grep `cpu` 零命中（对比 `peer.sh:195/242`），「frp 24 goroutine 摊 8 核」是 GMP 协作式推导而非实测，因果（抢占 vs 锁/唤醒序）不可分离，证据强度弱于 RH 侧 | verifiedGaps G2/G5 | §1、§4 G2/G5、§5 R2/R5、§6 落地顺序共同前置 |
| 8 | **R5 反证采纳**：×4 CPU 显示 client 尚有 40% 余量而 server 贴顶——若是纯每流任务数成本客户端也应饱和，事实相反，故 R5 降为「推测」且不建议单独作为投入依据 | rootCauses R5 证据段 | §5 R5 |
| 9 | **建连/UDP/HTTP 行不可比**：COMPARE.md:27-29 该轮 RH 与直连均为 0，只有 frp 有数，不纳入根因分析 | 本报告复核 COMPARE.md 原文 | §1 |

### 未采纳 / 需保留原判的项

| # | 内容 | 处置 |
|---|---|---|
| 1 | 「单出口必然解释 25% 差距」的强表述 | **不采纳为定量结论**。输入 R1 本身已注明「单出口 ≠ 必然 25% 差」；本报告将 R1 定性为「结构性、可完全回收但需实测量化」，不为它承诺具体百分比（§5 R1、§6 C0） |
| 2 | C7（代理拨号 socket 缓冲补齐） | **不排入本轮**。对无代理基准收益为 0，输入自己标注「按不提议无依据改动原则排在最后」；本报告维持该排序（§6 C7） |
| 3 | C4（splice 二级水位）的高风险 | **维持高风险定性并 env-gate**。g1 报告 §6 已记录同类形态实验无正向证据 + 一次原型控制连接失败；不进默认值（§6 C4） |

### 本报告自查修订

- 在 R3 中删除 Round-A 闸门成因（同修订 #1）。
- 在 C5 中把「读探测预算放宽」从独立收益项改为 C1 的前置条件（无直接吞吐增益），避免与 G4 勘误自相矛盾。
- 在 §1 标注 COMPARE.md 的 CPU 数字来源为「历史运行产物」，本报告未重跑基准，仅对已有原始样本做了 CPU 复算。

---

## 8. 未验证事项与下一步

### 需要实测（按优先级）

| # | 事项 | 为什么必须实测 | 怎么测 |
|---|---|---|---|
| 1 | **C0 接线池后的 ×1/×4/×16 双方向收益** | R1 是唯一「代码可证」根因但收益无实测数字；R2/R5 的归因也依赖这轮结果才能分离 | 客户端 session.rs:89 改 connect_pool + 服务端合流；网络稳定时段配对 A/B，每臂 ≥3 样本，同步 CPU |
| 2 | **单条 TCP 四元组在 16 流下是否已触软中断单核饱和** | C3 的增益前提；未验证前不排除 K=4 池本身已解决问题 | 池接线后不改分派，直接 A/B；或对现有单载体 baseline 取 RSS 队列/软中断计数 |
| 3 | **C1（1MiB→2MiB）是否复现 round-7 的 download×16 回退** | round-7 报告 §3.4/§4 记录同向改动后 ×16↓ -15.2% 中约 11% 归因未明，C1 存在同向风险 | 网络稳定时段单流/4流/16流三档配对 A/B，任一回退即停 |
| 4 | **C2 单流是否真的有 +5~12%** | 收益论证建立在 syscall 数减少，若瓶颈不在 syscall 数应 no-change | 单流 download 配对 A/B；保留 CHUNK_CAP=64KiB 读红线不动 |
| 5 | **C1/C5 组合对单流 download 是否反向钳制** | C5 放宽读探测预算会让入站 WU 最大处理时延翻倍，信用回路敏感场景可能反向 | C1+C5 同轮 A/B，单流回退则整体回退只留 C2 |
| 6 | **frp 侧 CPU 画像** | peer-frp.sh 无 CPU 采样器，「frp 摊多核」永远是推导；GC/GMP 侧无法与 RH 协作调度器对拍 | 先给 `benchmark/peer-frp.sh` 补与 `peer.sh:83` 同构的 start_cpu_sampler，再重跑 |
| 7 | ×4 CPU 频谱（多时段复采） | 本次复算的 ×4/×16 CPU 来自单次运行的 3 个样本窗口，仅够定性「服务端贴 1 核」 | 网络稳定时段重跑并逐样本出分布，而非 3 点 |

### 需要三机复跑（网络窗口相关）

| # | 事项 | 理由 |
|---|---|---|
| 1 | **三机交叉验证整个 ×16 下载档** | round7 报告 §4 明确建议「在网络窗口稳定时段重跑三机交叉验证」；当前 ×16↓ 的相对差距（-10%）与当天直连上限（2.5–2.8G）纠缠，无法区分协议差异与路径封顶。COMPARE.md 的 frp ×16 保留率 111% 已证明直连基线本身有时段漂移 |
| 2 | C0/C1/C2 任一项的三机验收 | 环回/单机增益不能外推到跨机；round6 报告 §4 的环回 -10% 正是单机伪影而目标工况 CPU 持平——单机数只能做方向判据，门禁必须三机 |
| 3 | (如需) 3100 Mbps 目标可达性重估 | round6 报告 §5.3 曾判「物理不可达」（当天直连 ×16 上限 2.6–2.8G）；若三机复跑直连上限抬升，则 G6/BDP 类讨论才重新有意义 |

### 当前不可归因 / 不建议投入

- **G6（连接级共享接收窗预算 / BDP 自增）**：两侧均恒定 6MiB 无自增，且「96MiB 在途」只是理论上限、「BDP≈36MiB」是算术推导（仓库无该数值记录）。列为不可归因项。
- **R3 的精确占比**：round-7 记录 download×16 回退中「~11% 不可归因」，但无法从现有数据中分离批聚合时序与流控交互各自贡献，只能 A/B 证伪。
- **R5 的因果**：×4 client 40% 余量 vs server 贴顶的形态反证「纯每流任务数成本」，固定成本 vs 单核封顶不可分离；R5 维持「推测」。
- **建连/UDP/HTTP 对比**：COMPARE.md:27-29 该轮 RH 侧为 0，指标缺测而非落后，不纳入根因。

### 已执行的核对命令（本次会话实际运行）

| 命令 | 输出摘要 |
|---|---|
| `grep -rn "connect_pool\|yamux_pool_client\|yamux_pool_server" crates --include="*.rs"` | ~~仅命中定义 `lib.rs:426,439,599`、转发 `tcp.rs:362`、测试 `mux.rs:1780`——G1/R1「未接线」成立~~ **⚠️ 该结论已于 Round-9 失效**：现生产调用方为 `crates/rh-client/src/session.rs:93`、`crates/rh-server/src/control.rs:334,830`；2026-10-18 复核见 §0a |
| `grep -rn "start_cpu_sampler" benchmark/*.sh` | 仅 `peer.sh:83,195,242`；peer-frp.sh 零命中——frp 侧无 CPU 采样 |
| `sed -n ... crates/rh-client/src/session.rs:85-95` | ~~L89 `rh_transport::connect(&tcfg)` 单连接~~ **⚠️ 已过期**：现为 L92-96，`carriers > 1` 走 `connect_pool` |
| `sed -n ... vendor/yamux/src/connection.rs:1040-1050` | L1044-1047 拆分后发送侧 poll 不含公平探测——G4 勘误 |
| `sed -n ... vendor/yamux/src/frame/io.rs:60-80` | MAX_BATCH_FRAMES=64 / MAX_BATCH_BYTES=1MiB / MAX_WRITEV_SEGMENTS=129——C1 落点 |
| `sed -n ... crates/rh-transport/src/mux.rs:290-296;765-835` | CHUNK_CAP=64KiB、6MiB 初窗、set_max_connection_receive_window(None)、split_send_size 6MiB——G3/G6/C2 |
| `sed -n ... crates/rh-transport/src/mux.rs:1140-1200` | L1149 `seq % n` 轮转分派 + is_dead 换载体重试——C3 落点 |
| `sed -n ... crates/rh-server/src/data.rs:280-343` | chunk→splice→copy 三级下探——§3.2 |
| `sed -n ... crates/rh-transport/src/splice.rs:28-42;78-92` | PIPE_SIZE 256KiB、NOTSENT_LOWAT 1MiB、单块管线深度=1 自证——C4/C6 |
| `sed -n ... crates/rh-transport/src/splice.rs:625-660` | SpliceSock::dup 已有 notsent_lowat 装配点——C6 |
| `ls benchmark/runs/tcp-multiflow-frp-20261015/` | 目录存在，含 round7-acceptance-report.md、ab/（REPORT-round6.md 在 ab/ 下）——R3/修订#2 引用路径 |
| `grep -n "0.226\|0.239" .../ab/REPORT-round6.md` | L77 paced 200M×16 CPU 0.226→0.239 核——修订#2 |
| `grep -n "15.2\|11%" .../round7-acceptance-report.md` | L89「-15.2% 回退中约 4% 可由直连网络漂移解释，其余 ~11% 需进一步排查」——R3 |
| `head ... results-tcp.jsonl; head ... cpu-r6-cand-client.jsonl` | 确认字段 window_start_us/window_end_us、ticks/clk_tck 存在——§1/§5 CPU 复算口径 |

**未执行的项（如实声明）**：本次未重跑任何基准测试、未运行 `cargo test`、未复算 CPU（§1/§5 的 CPU 数值引自输入材料的复算结果，本报告只核对了其数据源文件存在且字段匹配，未逐样本重算）；`docs/rabbithole-vs-frp-multiflow/r6-vs-frp/` 下仅核对了 COMPARE.md 与 rh/results-tcp.jsonl，frp/ 子目录内容未逐项核对。

---








