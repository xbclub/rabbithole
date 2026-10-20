# ×4 下行性能缺口定向分析

> 分析对象:Round-9 三机数据(`benchmark/runs/tcp-multiflow-frp-20261015/ab/round9-evidence/`)+ 当前工作树代码(HEAD `e6a9bcc`,分支 `perf/tcp-multiflow-frp-v032`)+ 本次本机复现实验。
> **修订 2026-10-18 晚 —— 构建溯源(此前全部行号口径都错,本节是第一优先级更正)**:初稿把分析对象写作 `33f646f`(Round-7),这是错的。`33f646f` **不含多载体池**:无 `connect_stream_pool`、无 `seq % n` 轮转、`mux.rs` 仅 1599 行(当前 3807 行)。全文引用的池化/分片行号(`mux.rs:781-783`、`mux.rs:1496-1497`、`tcp.rs:348-366`、`session.rs:815-816`)**只在当前工作树 `e6a9bcc`(Round-10,2026-10-18 19:19)成立**。
> 被分析的**三机二进制**又是第三个版本:由 `/tmp/r9-src.tgz`(2026-10-17 23:09 打包,早于 23:44 的正式 run)构建。该源码里 `connect_stream_pool` **没有 `shards` 变量**、`mux_config(max_streams)` **无 shard 参数**、`shard_workers_for_k` **不存在**,而 `vendor/yamux/src/lib.rs` 的 `Config::default()` 是 `shard_workers: 4`。⇒ **Round-9 的 rh4 是 4 载体 × 每载体 4 个 shard worker,不是每载体 1 个。** 初稿 §3.1 H3 / §3.2 第 5 条把 Round-10 树上的 `shard_workers=1` 日志当成了被测数据的属性,已在下文逐处更正。
> 证据标签:**[代码可证]** = 本次打开源码逐行核对;**[实测]** = 本次亲自运行并给出命令/输出,或引用有原始 JSONL 的三机数据;**[推算]** = 由代码常量与实测数做的算术,输入明确;**[推测]** = 无直接测量支撑的机制假说。
> 材料完整性声明(影响所有结论强度):原始材料里的「天花板计算」字段传入为 `[object Promise]`,未解析,因此本报告所有 ceiling 数字均为**临时值**,以 §4 的算式为准。`round9-evidence/raw/` **不存在**(不是空目录),RH 两个 JSONL 各有 36 条 `raw_json` 引用全部悬空;frp JSONL 12 条吞吐记录无 `raw_json`、无 `valid`,每格仅 1 样本。`/tmp/r9-peer-rh4` 已不存在,Round-9 池化臂 CPU 不可得。frp 侧没有 CPU 采样(`benchmark/peer-frp.sh` 全文 grep `cpu` = 0 命中)。

---

## 0. 结论速览

### 0.1 直接回答

**为什么 RH ×1↓ 有 2008 Mbps、×4↓ 只有 1985(几乎不增长),而 frp 从 1161 涨到 2682?**

短答分两层:

1. **RH 的 ×1 已经顶到这条两跳路径在「每载体 1–4 条在途流」工作点上的实际吞吐(~2.0 Gbps),所以再加 3 条流没有可增长的空间。**[实测,强]
   - 同一路径上三个**互相独立**的实现都停在 ~2.0G:refrelay-copy 2003、refrelay-splice 1992、RH 裸 TCP 隧道 2010(`benchmark/runs/goal-repair/diagnosis-after-round-2/DIAGNOSIS-REPORT.md:30-34`、`:41`)。这不是 RH 用户态代码的成本——refrelay-splice 走内核零拷贝,同样 2.0G。
   - 发送侧内核队列在这条路径上常满:第二跳发送 socket `tb 4 MiB`、`notsent` 中位 2.63 MB、最大 3.91 MB、RTT 中位 **2.48 ms**(仅取有 `notsent` 的 25 个数据 socket 时为 **2.79 ms**)。本次从 `t1/out/probe-rh-tcpmux-download.ss` 逐样本复算;**口径必须注明**:该探针 96 个 socket 中 70 个是 `tb=4MiB` 的数据载体、26 个是 `tb=87040` 的 ACK 方向;`app_limited` 只出现在后者(26/26),数据载体 **0/70**——所以不能用它证明「数据流被 app 饿住」。诊断报告 `:49-52` 给的是 0.3–2.3 MB,量级一致、本次样本更高。
   - **反事实(本次本机实测)**:同一份 RH 代码在无路径瓶颈的环回上,池化 K=4 从 ×1 17.5G 升到 ×4 24.2G(+38%),K=1 从 15.9G 升到 18.7G(+18%)。所以「RH 的 ×4 聚合路径失效」不成立;失效的是这条三机路径在 4 流工作点上的利用率。**⚠️ 该组环回数值不可复现**(见 §4.6 顶部说明:同配置另有 19.6G 的记录,`/tmp/rhx4` 无 `-J` 产物留存),只采信其**方向**(聚合路径有效、四流均匀),不采信 +38% 这个幅度。
   - 注意:1985 是 **rh4(4 物理载体、每载体 1 流)** 的 ×4↓;rh1(单载体、4 流)的 ×4↓ 是 2078,比 ×1 的 2008 高 3.5%,同样在噪声内。两者都不是「下降」,而是「不增长」。

2. **frp 的 ×1 只有 1161,归一化 t/d 只有 0.311,不到 RH 的一半;它的 1161→2682(2.31×)主要是从「单流欠填」恢复到同一条路径的 2.6–2.9G,而不是「frp 的聚合能力更强」。**[实测 + 代码可证结构,中]
   - frp 的 `Stream.Write` 每帧同步等 `waitForSendErr` → `sendCh` → 唯一 `sendLoop` → `conn.Write` 完成(`benchmark/frp-yamux-src/stream.go:170-182`、`:219`;`session.go:386-445`、`:477-527`、`:487`、`:499`、`:513`)。单流在飞只有 1 帧;RH 单流有 64 条命令通道(`vendor/yamux/src/connection.rs:367`),×1 就能把路径填满。
   - 但「每流单帧在飞」**不足以定量解释**三机的 2.31×:本机 frp 的帧服务率上限约 35k 帧/s(32KiB 体,×4 9.17G),三机 ×1 只有约 4.4k 帧/s,离这个上限还有 7 倍——所以三机 ×1 低另有路径侧原因,不能全记在 sendLoop 头上(见 H2/H9)。
   - 本机 A/B 直接反证「2.31× 是 frp 的固定聚合倍率」:本机 frp(TLS 开,各 3 样本中位)×1 7.91G → ×4 9.17G,只有 **1.16×**;TLS 关时 ×1 13.41G → ×4 13.61G,约 **1.02×**。倍率随工作点变化,不是结构常数。

**因此,「RH ×4 落后 frp 26%」是一个混合了口径的表述。** 同一拓扑(单载体、4 条 mux 流)的对照是 **rh1 ×4 2078 vs frp ×4 2682(-22.5% 绝对差)**;按各臂自己的直连基线归一化则是 **0.786 vs 0.939**。rh4 是 4 条物理载体,和 frp 的 1 条 TCP 不是同一拓扑,它的 1985 不能直接拿去和 frp 的 2682 说「同拓扑落后 26%」。

**⚠️ 归一化残差没有误差棒 —— 16.3% 是点估计,不是可复现的精度。** 直连分母本身噪声极大(rh4 直连 ×1 = 4148/3431/3133,**CV 14.6%**;×4 = 3149/2310/2724,**CV 15.4%**),frp 的分母更是 **n=1**。按 3×3 全配对重算 t/d:

| 量 | 文中值 | 全配对实际范围 |
|---|---:|---|
| rh1 ×4 t/d | 0.786 | **0.730 – 0.899** |
| rh4 ×4 t/d | 0.729 | **0.597 – 0.867** |
| rh4 ×1 t/d | 0.564 | **0.465 – 0.643** |

所以正确的表述是:**同拓扑绝对差 22.5% 是稳健的(中位差,不受分母影响);归一化残差在 10%–25% 区间,方向确定、幅度不定。** 下文凡出现 -16.3% / 2484(+19.5%)/ 2558(+28.9%) 的地方一律按此区间理解。

### 0.2 结论与证据强度

| # | 结论 | 证据强度 | 关键依据 |
|---|---|---|---|
| A | RH ×1 与 ×4 落在同一个 ~2.0G 工作点;×4 不增长是**路径工作点**现象,不是 RH 聚合代码失效 | **中–强**(方向强、幅度待复现) | 三个独立实现同 ~2.0G;发送队列常满;本机反事实 ×1→×4 聚合有效(幅度不可引用);Round-10 跨构建复现 ×1≈×4 |
| B | frp ×1 低于该工作点(1161,t/d 0.311),其 ×4 的 2.31× 主要是「欠填恢复」;倍率随工作点变化(本机 1.16×/1.02×) | **中**(n=1) | frp 同步写结构可证;本机 ×1/×4 与 TLS on/off 实测;三机缺 frp CPU/逐流数据 |
| C | 同一拓扑归一化后仍有 **10%–25%** 的残余缺口(rh1 0.786 vs frp 0.939 点估计 -16.3%),且 RH ×16 能到 2.95G 而 ×4 只有 2.0G——这两点现有证据都解释不了 | 未知 | 见 §3 判别矩阵;所有代码级候选(H2–H6、H8)均被反证或未验证 |
| D | 分片路由 `sid % N` 在 N=4 时数据面只用 2 个分片,是**代码可证**的结构浪费;对 rh4 ×4 无影响(每载体仅 1 条流,无论 N 多少都只占 1 个分片),对 ×4 缺口无解释力 | **强(代码)** / 运行时未测 | `connection.rs:984-994/1429-1434/1957-1959`;`shard.rs:333-335`;93 项 yamux 测试通过 |
| E | 64KiB 块、writev 批量、每块锁/唤醒、限速预留、信用窗口、单线程 CPU 均**不是** 2.0G 的原因 | **强(反证)** | release 仿真 mux↓ 6.9–7.4G;限速器恒 None;信用合成实验 5.94MiB;CPU 0.9 核 |
| F | **「×4 谷底」本身在 n=3 下不显著**(×4→×16 置换检验 p=0.2),现有数据不足以断言存在一个独立的 ×4 现象 | **强(统计)** | §1.3 反常 1/2;A8 置换检验 |

### 0.3 一句话版本

> RH 不是「4 条流聚合不上去」,而是「1 条流就已经把这条两跳路径在 1–4 流工作点上跑满了」;frp 的 ×1 远低于这个工作点,所以它的 ×4 看起来像翻倍。
>
> **但先别急着修:在 n=3、每格 3 样本的条件下,「×4 是一个谷底」这件事本身还不显著(×4→×16 置换 p=0.2,3v3 可达到的最小 p 就是 0.1)。** 下一轮的第一步不是改代码,而是用 §5 的 **F0c(refrelay ×4)** 和**把 ×4/×16 补到 5–8 样本**,先判定这个谷底是真现象还是抽样抖动。若 ×4 只是抖动,本文 §3 的全部假设都不需要被解释;若确认存在,F0c 又能一次性区分「路径工作点」与「RH 结构」两条分支。

---

## 1. 观测形态与关键反常

### 1.1 下行 scaling(Mbps,中位;括号为同臂直连基线)

数据来源:本次用 python3 从 `round9-evidence/results-rh1-20261017-154452.jsonl`、`results-rh4-20261017-155938.jsonl`、`results-frp.jsonl` 按 `(path,dir,streams)` 取中位复算,与 `REPORT-round9.md:21-27` 一致。RH 每格 3 样本,frp 每格 1 样本。

| 臂 | 直连×1 | 直连×4 | 直连×16 | 隧道×1 | 隧道×4 | 隧道×16 | t/d×1 | t/d×4 | t/d×16 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| rh1(单载体) | 3141 | 2645 | 2526 | **2008** | **2078** | **2567** | 0.639 | 0.786 | 1.016 |
| rh4(4 物理载体) | 3431 | 2724 | 2742 | **1934** | **1985** | **2953** | 0.564 | 0.729 | 1.077 |
| frp v0.71.0(1 TCP + 4 mux 流) | 3732 | 2855 | 2710 | **1161** | **2682** | **2821** | 0.311 | 0.939 | 1.041 |

上行对照(同一批 JSONL):

| 臂 | 直连×1 | 直连×4 | 直连×16 | 隧道×1 | 隧道×4 | 隧道×16 | t/d×1 | t/d×4 | t/d×16 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| rh1 | 3021 | 2935 | 2853 | 1991 | 1987 | 2063 | 0.659 | 0.677 | 0.723 |
| rh4 | 3319 | 2839 | 2671 | 1980 | **2431** | **2706** | 0.597 | 0.856 | 1.013 |
| frp | 3949 | 3466 | 3187 | 1141 | 2222 | 2465 | 0.289 | 0.641 | 0.773 |

### 1.2 倍率

| 臂 | ↓×4/×1 | ↓×16/×1 | ↑×4/×1 | ↑×16/×1 |
|---|---:|---:|---:|---:|
| rh1 | **1.035** | 1.278 | 0.998 | 1.036 |
| rh4 | **1.026** | 1.527 | 1.228 | 1.367 |
| frp | **2.310** | 2.430 | 1.947 | 2.160 |

- 下行 1→4:RH 两臂都是「平的」(1.026–1.035),frp 是 2.31×。
- 下行 4→16:RH 反而大涨(rh1 +23.5%、rh4 +48.8%),frp 只 +5.2%。
- 上行 1→4:rh1 平(0.998),rh4 涨 22.8%,frp 涨 94.7%。

### 1.3 关键反常(按重要性排序)

1. **RH 的 ×4 不是「下降」,是「不增长」**:rh1 2008→2078(+3.5%),rh4 1934→1985(+2.6%);精确置换检验(中位差)rh1 p=0.6、rh4 p=1.0,3 样本对 3 样本下双侧最小可能 p 就是 0.1,所以这两个差异**在统计上完全不可区分**。[实测/复算]
2. **RH ×4 不是硬天花板**:rh1 ×16 2567、rh4 ×16 2953,说明这条路径至少能到 2.95G。×4 的 2.0G 是**工作点**,不是容量上限。[实测]
3. **RH ×4 的两条臂(单载体 4 流 / 4 载体各 1 流)几乎相同**:2078 vs 1985(中位差 93,p=0.2)。如果是「单载体写出口」限制,4 载体应该显著更好;如果是「每流独立上限」,4 流应该到 4×2.0=8G。两种单因解释都不成立。[实测]
4. **下行/上行不对称**:rh4 ×4↓ 比 rh1 低 4.5%,但 ×4↑ 比 rh1 高 22.3%。同一份代码、同一轮窗口,下行不涨、上行涨——说明缺口与「下行方向 + 4 流」这个组合有关,不是全局的流数扩展问题。[实测]
5. **三条臂的直连 ×4 基线都明显低于 ×1**:rh1 -15.8%(3141→2645)、rh4 -20.6%(3431→2724)、frp -23.5%(3732→2855)。4 流直连本身就更难跑满,t/d 归一化的分母在 ×4 被压低,所有臂的 t/d×4 都被**高估**。[实测]
6. **t/d×16 出现 ≥1**:rh1 1.016、rh4 1.077、frp 1.041。隧道吞吐超过同窗直连,说明 ×16 的直连基线受端点(iperf3 16 线程/16 socket)影响,不能把 t/d 当作无损归一化。[实测]
7. **frp 每格 1 样本,且该臂直连基线最高**(×1 3732、×4 2855)。2682 是「frp 自己窗口里的一次测量」,不是可复现的绝对靶。[实测/材料 `REPORT-round9.md:30`、`:65`]
8. **⚠️ 最重要的一条:「×4 谷底」在统计上尚不成立。** rh1 ×4→×16 中位差 489 p=0.20;rh4 ×4→×16 中位差 968 p=0.20;而 3 样本对 3 样本的双侧最小可能 p 就是 0.10。也就是说,现有数据**支持「×16 ≥ ×4」但不支持「×4 是一个独立的低谷」**。本文后续所有「为什么 ×4 只有 2.0G」的追问,都建立在这个尚未排除了抽样波动的现象之上——这是整篇分析的**首要前提风险**。[实测/复算]

### 1.4 跨轮复现

RH 的 ×4↓ 在最近五轮一直停在 2.0–2.4G,不是 Round-9 的偶然:

| 轮次 | RH ×4↓(Mbps) | frp ×4↓(Mbps) | 来源 |
|---|---:|---:|---|
| Round-5 | 2424 / 2241 | 2959 | `REPORT-round5.md:66` |
| Round-6 | 2220 / 2231 | 未同轮 | `REPORT-round6.md:91` |
| Round-7 | 2010(候选)/2159(基线) | 未同轮 | `round7-acceptance-report.md:53` |
| Round-8 | 2138 / 2128 / 2124 / 2189(四臂) | 未同轮 | `REPORT-round8.md:52` |
| Round-9 | 2078(rh1)/1985(rh4) | 2682 | `REPORT-round9.md:25` |
| **Round-10**(2026-10-18,独立构建) | **2.13G(K=1)/ 1.99G(K=4)** | 未同轮 | `REPORT-round10.md:§3` |

**Round-10 是一次真正独立的复现**,值得单列:不同日期、不同构建(含每载体 shard 数由 4 降为 `4/K`)、8s 口径、非同一窗口。它复现了核心命题:

| 流数 | tcp K=1 | tcp K=4 |
|---|---:|---:|
| ×1 | **2.01G** | 1.92G |
| ×4 | **2.13G** | 1.99G |
| ×16 | 2.38G | **2.50G** |

×4 相对 ×1 只高 **3.6%(K=4)/ 6.0%(K=1)**、×16 最高——与 Round-9 的「×4 不增长、×16 才涨」形态一致。**注意一处待验差异**:Round-9 rh4 ×16 = 2.953G(每载体 4 shard worker)vs Round-10 K=4 ×16 = 2.50G(每载体 1 shard worker)。若这 0.45G 差距为真,则「×16 能到 2.95G」可能部分来自 shard worker 数量——这正是 H9「相位」假说缺失的拼图。**但跨轮跨口径,只能当线索,不能当结论**(见 §4.6 的可比性警告)。

结论:这是一个**跨轮稳定**的 2.0–2.4G 工作点,不是某一轮的噪声。frp 的 2959(Round-5)→2682(Round-9)也在同一量级。

---

## 2. 两条下行路径的逐阶段成本

方向定义:下行 = target → client →(隧道)→ server → driver,即 RH 服务端 `pump_down` / frp 服务端 `Join` 的「userConn 方向」。每张表的 `serialPerStream` 指该阶段是否被单流串行化。

### 2.1 RH 下行九阶段

| stage | ops(每单位) | where | serialPerStream | frpEquivalent |
|---|---|---|---|---|
| 1. 连接接入与中继装配 | **每连接一次、非每字节**:`set_nodelay`;从 `TcpPool` 取/签发数据通道(锁 + oneshot);向 mux 流写 `START_FORWARD`;`QuotaIo/PrependedIo` 剥壳(2 次 downcast);`mk_limiter`(双桶皆空→None);`tokio::io::split(user)` 建 `Arc<Mutex>` 读写半;`steal_mux_halves`;spawn up/down 两泵。决定后续上限(64KiB CHUNK_CAP、6MiB 窗、limiter 是否装) | `crates/rh-server/src/data.rs:1024-1075`(serve_tcp_user 1024、set_nodelay 1031、get_channel 1043、assign_forward 1061)、`:208-298`、`:360-384`、`:669-684`;`crates/rh-transport/src/mux.rs:339-356` | false | frp 从 workConn 池取连接、写 `StartWorkConn` 后 `libio.Join(local,userConn)` 起 2 goroutine、16KiB 池缓冲(`server/proxy/proxy.go:288,322,344`;`golib@v0.8.2/io/io.go:29-44`) |
| 2. 载体收帧/解帧与分片路由 | **每 64KiB 下行数据**:载体 `read`(READ_RESERVE=64KiB staging,一次读可解多帧);每次 refill 前 `zeroed_spare` 零填(容量不变时只填新暴露尾部);每帧 12B 头解析、>1MiB 帧拒收;>4KiB 帧体 `split_to().freeze()` 零拷贝,≤4KiB 走 copy-out;每帧一次 `sid % N` + `ShardChannel::try_push`(1 次 parking_lot 队列锁;空→非空再取 consumer_waker 锁并 wake) | `vendor/yamux/src/frame/io.rs:537-596`(poll_next)、`:210`(zeroed_spare)、`:239`(take_body)、`:42`(MAX_FRAME_BODY_LEN=1MiB)、`:52`(READ_RESERVE)、`:59`(SMALL_FRAME_COPY=4KiB);`connection.rs:1746-1886`、`:1939-1959`(route_index 1957-1959)、`:1966-1982`(try_route);`connection/halves/shard.rs:139-161`(try_push) | true | frp 单一 recv goroutine 先 `io.ReadFull(s.bufRead,12B)` 再 `readData` 用 `io.Copy` 把帧体拷进 per-stream `bytes.Buffer`(`benchmark/frp-yamux-src/session.go:547-574`、`stream.go:486-528`);无分片、无跨任务唤醒 |
| 3. 分片 Worker 派发/流控记账/唤醒 | **每帧**:`drain_into`(1 次队列锁 + `VecDeque::drain`≤128 + `popped` SeqCst add);`IntMap` 查表;取 `stream::Shared` 锁;`consume_receive_window`;`buffer.push(Bytes)`(0 字节拷贝);`reader.take().wake()` | `connection/halves/shard.rs:489-495`(drain_msgs)、`:197-215`(drain_into)、`:669-715`(on_data;692 consume_receive_window、707-708 push+wake);`connection/stream.rs:1082` | true | frp 在同一 recv goroutine 内完成流表查找 + `recvBuf` 拷贝 + `recvLock`(`session.go:577-620`、`stream.go:486-528`);无独立 Worker/通道/二次唤醒 |
| 4. mux 流读半取块 | **每块**:`pending` VecDeque pop;`poll_read_chunk` 内约 4 次 parking_lot 锁(state.can_read、ack_pending、next_window_update、buffer.pop);每次读都尝试窗口更新,但仅当已消费且出缓冲 ≥ 半窗(3MiB)才生成 WU;返回 `Bytes` 切片(0 拷贝)。聚合用的 `try_read_chunk` 无数据时仍登记/clone 一次 waker | `crates/rh-transport/src/mux.rs:143-149`(read_chunk)、`:152-163`(try_read_chunk);`connection/stream.rs:487-510`(poll_read_chunk)、`:513-566`(send_window_update);`connection/stream/flow_control.rs:58-79`;`connection.rs:1411-1420`(push_to_send) | true | frp `Stream.Read` 把 `recvBuf` 拷进调用者缓冲(每字节 1 次 memcpy,`stream.go:134`),且每次 Read 都调 `sendWindowUpdate`(`:138`、`:275-311`) |
| 5. `pump_down` 循环/小块聚合/flush | **每块**一次 `read_chunk` await;≥64KiB 整块:先 flush 小块,再 `write_metered` 直写(1 次写 syscall);小块:只把 `Bytes` 句柄 push 进 Vec(0 字节拷贝),满 64KiB 或 128 段才 flush;随后 `try_read_chunk` 内循环收割就绪块;无就绪/Err 即 flush | `crates/rh-transport/src/mux.rs:499-662`(pump_down;616 满块分支、631 聚合阈值、643、537-590 write_metered_vectored);`:294`(CHUNK_CAP=64KiB)、`:482`(SMALL_CHUNK_SEGS=128) | true | frp `Join` 用 `io.CopyBuffer` 名义 16KiB;裸 TCPConn 时被 `ReadFrom` 快路径绕过,实际 32KiB(`golib io.go:36-43`;Go `io.go:410-418`;`net.go:772-775`);无聚合,64KiB 需 2 次读+2 次写 |
| 6. 限速预留 | 双桶皆空时 `limiter=None`,热路径 0 次检查;配置后每次 flush/整块直写做一次 `reserve`(≈每 64KiB):构造 `BoxFuture` + 每桶 `acquire_reserved`(std Mutex + Instant + 浮点记账,不足则 sleep ≤1s)+ `Box<dyn ChunkReservation>`;按实际写出 consume,drop 退还未消费额度 | `crates/rh-server/src/data.rs:616-665`(mk_limiter;620-622 双桶皆空 None);`crates/rh-transport/src/mux.rs:508-590`;`crates/rh-core/src/ratelimit.rs:105-124` | true | frp 用 `limit.NewReader/NewWriter` 包连接,每次 16/32KiB 读写都检查(`server/proxy/proxy.go:311`);频率高 2–4× |
| 7. 用户 TCP socket 写出 | **每块**:tokio split 的 `std::sync::Mutex` 加锁(读/写半共用同一把锁,用户 socket 上读写互斥);`CountedUserIo` 1 次 `flow_cut` 原子读;整块 1 次 `write` syscall(部分写循环),小块 1 次 `writev`(≤128 段/64KiB,部分写重拼);写后 4 个原子计量;内核 1 次用户→socket 拷贝,用户态 0 次 | `crates/rh-transport/src/mux.rs:565`;`crates/rh-server/src/data.rs:872-925`(poll_write 873、poll_write_vectored 899)、`:829-841`(count_out);`tokio-1.53.2/src/io/split.rs:116,126` | true | frp `io.CopyBuffer` 每次 Write 直接写 `net.Conn`,无 per-write 原子;流量在 Join 返回后汇总(`server/proxy/proxy.go:320-324`);32KiB 一次写 |
| 8. splice 内核零拷贝(仅非 mux 裸 TCP) | 每 256KiB 块:fill splice(socket→pipe)1 次;drain splice(pipe→socket)1..n 次(≤DRAIN_SLICE=256KiB);用户态 0 拷贝;每块 1 次限速 reserve;EAGAIN 才注册 readiness;管道深度=1 | `crates/rh-server/src/data.rs:299-330`;`crates/rh-transport/src/splice.rs:30-49`、`:553-555`(plain_tcp 只认裸 TCP)、`:639-650`(NOTSENT_LOWAT) | true | frp 无 splice;同一裸 TCP 场景仍走用户态 `io.CopyBuffer` 32KiB(每字节 2 次用户态拷贝 + 每 32KiB 2 次 syscall) |
| 9. 信用回程 WU | 每读块判定(约 4 次流锁);已消费且出缓冲 ≥3MiB 才 to_send 锁 + send_waker 锁 + 唤醒;`FlushTask` 与 send_out 帧一起批量聚合(≤64 帧/1MiB/129 段)一次 writev;单帧 WU 仅 12B。**生成点在 `poll_read_chunk` 开头**(本流 `pump_down` 调用它),WU 生成与该流的用户 socket 写**同一任务、同一 future**;只有最后的 `FlushTask` 写 socket 是跨任务的快车道直压 | `connection/stream.rs:487-566`(WU 判定在 `:491`,早于取块 `:502`);`connection.rs:1411-1420`(push_to_send 快车道)、`:2215-2229`;`frame/io.rs:66,73`、`:342-449` | **true(生成与用户写同任务)** | frp `sendWindowUpdate` 直接经 `sendCh` 交共享 send goroutine;sendLoop 每帧先写头再写体(2 次 `conn.Write`,body 先拷进 `bodyBuf`),无跨帧 writev(`session.go:477-527`、`stream.go:275-311`) |

**RH 下行串行结构(材料总结,本次核对)**:载体单任务 Framer 解帧/按 `sid%N` 路由 → 固定分片 RecvWorker 记账入流缓冲 → 每流一个 `pump_down` 任务(取块→限速→写用户 socket)。数据以 `Bytes` 引用在三级间移动,用户态 0 次大块 memcpy;但单流无法分到多核,每 64KiB 至少 2 次任务唤醒、约 4 次 vendor 流锁 + 1 次分片队列锁 + 1 次用户 socket 锁,且 WU 信用回程与该流写路径同任务耦合。

### 2.2 frp 下行十阶段

| stage | ops(每单位) | where | serialPerStream | frpEquivalent(对比 RH) |
|---|---|---|---|---|
| 1. frps 用户连接 accept + workConn 准备 | 每连接:accept 裸 `*net.TCPConn`(Go 默认 `SetNoDelay(true)`),每连接 1 goroutine;从 workConn 池取一条 mux 流,包 `ContextConn`(只转发 Read/Write,0 拷贝/0 syscall) | `benchmark/frp-src-v0.71.0/server/proxy/proxy.go:232-263`(261 go handle)、`:268`、`:147-199`(GetWorkConnFromPool);`pkg/util/net/conn.go:55-66`;`server/service.go:688`;Go `net/tcpsock.go:290`(noDelay=true) | false | RH 每连接要经 TcpPool(锁 + waiter + 签发)取通道;两者都不在每字节路径 |
| 2. frpc 源侧 local → yamux stream | `Join` 的 pipe(to=remote, from=local):src 是 `*net.TCPConn`,实现 `io.WriterTo`,名义 16KiB buf 被绕过;darwin 上 `TCPConn.WriteTo` 的 `spliceTo` 只对 UnixConn 生效→`genericWriteTo`→`io.Copy`(无 buf,默认 32KiB) | `client/proxy/proxy.go:253`;`golib io.go:36,43`;Go `io.go:410-418`;`net/tcpsock_posix.go:57-62`;`net/net.go:796` | true | RH 用 64KiB `read_chunk` + `Bytes` 零拷贝写帧;frp 每 32KiB 一次 Read + 一次 Stream.Write |
| 3. `yamux Stream.Write` | 每帧:`sendLock` 1 次、`stateLock` 2 次、`atomic.Load/Add(sendWindow)`、encode 12B header、`waitForSendErr` 压 `sendReady` 进 `sendCh` 后**阻塞等 errCh**——每帧一次完整 channel 往返 + goroutine 唤醒;单流同时在飞 ≤1 帧 | `benchmark/frp-yamux-src/stream.go:170-182`、`:186-254`、`:219`;`session.go:386-445`(405 sendCh、432 errCh)、`:84-89`(sendReady) | true | RH `StreamWriteHalf::poll_write_chunk` 经 64 深命令通道 + 6MiB split,单流可有多帧在飞(`vendor/yamux/src/connection/stream.rs:596-640`;`connection.rs:367`) |
| 4. `session sendLoop`(每 session 单 goroutine) | `sendCh` cap 64(`session.go:108`),每次只取 1 个 `sendReady`;`conn.Write(Hdr)` 与 `conn.Write(bodyBuf.Bytes())` 两次独立 syscall,body 先 `bodyBuf.Write` 再 memcpy 一次;无 writev、无跨帧聚合 | `session.go:477-527`(487/499/513)、`:119`(go send) | true(全 session 串行) | RH 每载体 1 个 SendHalf 任务,批量 ≤64 帧/1MiB/129 段一次 writev(`connection.rs:2133-2229`;`frame/io.rs:342-449`) |
| 5. TLS 记录层(默认开) | 客户端 `TLS.Enable` 默认 true;服务端按首字节 0x16 升级为 `tls.Conn`;`tls.Conn.Write` 每记录明文上限 16KiB,稳态 12B 头 + 32KiB body 被切成 3 次底层 Write(34B + 16406B + 16406B);额外 AEAD 与记录分片拷贝 | `pkg/config/v1/client.go:85,94,165,182-185`(默认 true);`client/connector.go:155,187`;`server/service.go:704`;Go `crypto/tls/common.go:67,70`、`conn.go:1006-1030` | true | RH tcp 明文无 TLS;TLS 载体是 rustls,但基准臂 `PROTO=tcp` 不走 |
| 6. frps `yamux recvLoop`(每 session 单 goroutine,全局串行点) | `io.ReadFull(s.bufRead,12B)`(bufio 默认 4096B)→ 校验 → `handlers[mt]` 分派;`handleStreamMessage` 再取 1 次 streamLock;所有流的下行每帧必经此点 | `session.go:547-574`(570 ReadFull)、`:577-590`(585 streamLock);`bufio/bufio.go:19`(4096) | true(全 session 串行) | RH 每载体 1 个 `recv_driver` + N 个 ShardWorker;RecvHalf 解帧后按 sid 分片并行 |
| 7. `readData`:帧体写入 per-stream `recvBuf` | `recvLock` 1 次;首帧按帧长 `make([]byte,0,length)`,之后复用;`io.Copy(recvBuf, *io.LimitedReader)` 命中 `bytes.Buffer.ReadFrom`,直接读入 buf 容量;`recvWindow -= n`;`asyncNotify` 唤醒 Read | `stream.go:486-528`(514 io.Copy) | true | RH 帧体是 `Bytes` 零拷贝切片(`frame/io.rs:239-251`),不先落 per-stream 缓冲 |
| 8. frps `Stream.Read` | `stateLock` 1 次 + `recvLock` 2 次;`recvBuf.Read(b)` 内部 `copy()` 一次(32KiB 全量 memcpy);每次 Read 都 `defer asyncNotify`;返回前**无条件**调 `sendWindowUpdate` | `stream.go:101-145`(134 recvBuf.Read、138 sendWindowUpdate) | true | RH `read_chunk` 返回 `Bytes` 引用(0 拷贝),WU 只在半窗阈值判定 |
| 9. `sendWindowUpdate`(每次 Read 都调) | 即使不发也先取 `controlHdrLock` + `recvLock`;`delta=(max-bufLen)-recvWindow`,仅当 `delta >= max/2 = 3MiB` 才真正发;真正发时走 `waitForSendErr` → `sendCh` → `sendLoop` 全往返;frp 把 max 从 256KiB 覆盖为 6MiB | `stream.go:275-311`(292 半窗);`mux.go:64-72`(68 MaxStreamWindowSize);`const.go:126`(256KiB);frp 覆盖 `client/connector.go:154`、`server/service.go:720` | true | RH 同样 6MiB/半窗(`crates/rh-transport/src/mux.rs:827,849`;`flow_control.rs:58-79`);差别在 RH 生成点与用户写同任务,frp 经共享 sendLoop |
| 10. frps 下行拷贝循环 + userConn.Write | 每连接 1 goroutine;名义 `pool.GetBuf(16KiB)`,但 dst=裸 `*net.TCPConn` 实现 `io.ReaderFrom`,`copyBuffer` 先转向 `dst.ReadFrom`;darwin 上 splice/sendfile 都 handled=false→`genericReadFrom`→`io.Copy` 无 buf→默认 32KiB;每次 Write 落用户 TCP socket,无 writev/无应用层聚合、未调 SO_SNDBUF | `golib io.go:36-43`;Go `io.go:410-418`;`net/tcpsock_posix.go:47-54`;`net/net.go:772-775`;`net/splice_stub.go:13-19`;`tcpsock.go:290` | true | RH `pump_down` 满块直写 64KiB / 小块 writev;每块 1 次 write,用户态 0 拷贝 |

**frp 下行串行结构**:frpc Join(32KiB) → Stream.Write(每帧一次 sendCh→sendLoop→errCh 往返)→ 单 sendLoop 两次独立 Write/帧(TLS 下三次)→ frps 单 recvLoop 逐帧 ReadFull+readData(memcpy 进 recvBuf)→ Stream.Read(memcpy 出 recvBuf + 每次调用都碰 sendWindowUpdate)→ Join 32KiB 拷贝 → userConn.Write。`sendLoop` 与 `recvLoop` 各只有一个 goroutine、被同一 session 的所有 stream 共享,是真正的全局串行点。

### 2.3 串行性对照

| 维度 | RH | frp |
|---|---|---|
| 单流在途帧 | 命令通道 64 深,最多 64 帧/4MiB(64KiB 帧) | 每流 1 帧(`Write` 等 errCh) |
| 每载体发送 | 1 个 SendHalf 任务,批量 writev(≤64 帧/1MiB/129 段) | 1 个 sendLoop,逐帧 2 次写(无聚合) |
| 每载体接收 | 1 个 recv_driver + N 个 ShardWorker(server N=4,K=4 client N=1) | 1 个 recvLoop(全 session 串行) |
| 每流用户写 | 1 个 pump_down 任务,64KiB 块 | 1 个 Join goroutine,32KiB 块 |
| 用户态拷贝/字节 | 0(帧体 Bytes 直通) | 2(recvBuf→join buf,join buf→内核)+1 内核 |
| 每 64KiB syscall | ≈2(读摊薄 + 1 写) | ≈2 读 + 2 写(32KiB×2) |
| 每 64KiB 锁/唤醒 | ≈6 锁 + 2 唤醒 + 4 原子 | ≈6 锁 + 1 channel 往返 + 1 唤醒 |

这张表说明:**frp 的「重」在拷贝与逐帧同步,不在锁或 syscall 数量;RH 的「轻」在零拷贝与批量,但单流仍被一条串行三级流水限制。** 两者都不是 2.0G 的解释(见 §4 的本机天花板)。

---

## 3. 假设与验证

> 说明:下表保留原始 H1–H8 的 claim/explains/cannotExplain/verdict/evidence,但 verdict 按本次复核修正。修正处标 **【修订】**。所有「本次运行」均指本会话实际执行的命令,输出见 §7/附录。
>
> **⚠️ 阅读顺序建议**:先读 §6.2(第二轮独立复核,含 7 条硬伤更正,其中「构建溯源」与「Round-9 实为每载体 4 shard」两条会改变下文多个细节),再读 §0–§3。判定优先级:**§6.2 > §6.1 > 正文**。

### H1 —— 「×4 的真实形态是聚合不扩展」

- **claim**:RH 1→4 流吞吐几乎不变,而 frp ×4 达 2682;若每流有独立 2.0G 天花板,4 条流应逼近路径上限而不是停在 2.0G。
- **explains**:rh1 2008→2078→2567、rh4 1934→1985→2953,两臂 ×4/×1=1.035/1.026;frp 1161→2682→2821(2.31×);同路径至少能到 2.68G;池化 ×4↓ -4.5% 在噪声内。
- **cannotExplain**:没有定位到跨流共享的具体资源;rh4 ×4 每载体 1 流也不跨流共享;×16 升到 2.95G 使「固定共享上限」不成立;frp 每格 1 样本;直连 ×4 是一跳基线,不能直接证明两跳隧道仍有 0.6–0.7G 余量。
- **verdict**:**【修订】描述性支持,因果未定**。原「支持」过强:绝对隧道吞吐持平是事实,但直连基线本身在 ×4 掉了 15.8–23.5%,t/d 反而从 0.639/0.564 升到 0.786/0.729;不能由「绝对值持平」推出「聚合失败」。更准确的表述是:**RH 在 1–4 流区间落在同一个 ~2.0G 工作点;同拓扑对照是 rh1 2078 vs frp 2682(-22.5%,归一化 -16.3%),不是 -26%。**
- **evidence**:
  - 复算命令与结果:见 §1.1;`REPORT-round9.md:21-27`、`:39`、`:45`。
  - 直连 ×4 跌幅:rh1 3141→2645(-15.8%)、rh4 3431→2724(-20.6%)、frp 3732→2855(-23.5%)(同 JSONL)。
  - 拓扑:`benchmark/peer-frp.sh:130-131` 是 `poolCount=4` + `tcpMux=true`,即**一条物理 TCP 上的 4 条 mux 流**(`FRP-YAMUX-ANALYSIS.md:69`);rh4 是 4 条物理载体(`crates/rh-transport/src/tcp.rs:348-366`,**当前树;被测构建同一函数但无 `shards` 参数**)。rh1 才是同拓扑。
  - 池化 -4.5%:rh1 [2049,2078,2161] vs rh4 [1881,1985,2002],中位差 93,置换检验 p=0.2,3 样本下不可区分。
  - **本次新增反事实**:本机环回 K=4 ×1 17.5G→×4 24.2G(+38%);K=1 15.9G→18.7G(+18%)(§4.6/§7)。聚合路径本身有效,缺的是路径余量。**⚠️ 该组环回数无留存且与 19.6G 口径冲突(见 §4.6),只采信「聚合有效」的方向,不采信 +38%。**

### H2 —— 「yamux writev 微突发 → RTT 2.5–4.5ms → 1–4 流被压在 2.0–2.1G」

- **claim**:RH 的批量发送节律造成微突发,抬高第二跳 RTT,把 1–4 流 goodput 压在 2.0–2.1G。
- **explains**:RH ×1 就到 2.0G(诊断时第二跳 tb 4MiB 满载、notsent 0.3–2.3MB、RTT 2.5–4.5ms),×4 加不上;frp 每帧等待完整写出、帧体 16KiB,×1 仅 1.16G、×4 达 2.31×。
- **cannotExplain**:RH ×16 2.95G(批量更大应更差,实际最好);rh4 ×4 每载体 1 流(突发最小)却是四档最低 1.985;refrelay-copy(64KiB copy、无 yamux 批量)同样 ~2.0G 且 RTT 同样膨胀;Round-9 ×4 没有 retrans/队列/cwnd 直接证据;frp 单样本。
- **verdict**:**【修订】反证(因果链),保留「发送节律不同」这一结构差异;拆成 H2a/H2b。**
  - **H2a(writev 聚合批次)**:反证。refrelay-copy 无 yamux 批量也 2.0G(`DIAGNOSIS-REPORT.md:30-34`);本机 release 仿真的 mux 批量版本 6.9–7.4G,远高于 2.0G。
- **H2b(64KiB 块 vs 16KiB 块)**:**未验证/弱支持**。refrelay 本身也是 64KiB(`benchmark/diag/refrelay.rs:82`),不能当「16KiB 对照」;frp 16KiB 是唯一正向数据点,但混了 TLS。**本次本机补做了 refrelay 16/32/64KiB 对照(×4↓ 中位,4/3/4 样本):21.06G / 28.37G / 30.14G,16KiB 反而最低**——本机(无路径瓶颈)不支持「块越小越好」,三机仍未测。
- **evidence**:
  - `frame/io.rs:66` MAX_BATCH_FRAMES=64、`:73` MAX_BATCH_BYTES=1MiB;`connection.rs:2215-2229` fill_batch。64KiB 帧下字节上限先封顶,每批最多 16 帧。
  - `crates/rh-transport/src/mux.rs:294` CHUNK_CAP=64KiB、`:608-660` pump_down。
  - frp:`frp-yamux-src/stream.go:170-182,219`;`session.go:390-440,477-527`;`golib io.go:36`。
  - **更正**：frp 帧体在裸 TCPConn 下实际是 **32KiB**(`io.Copy` 默认,Go `io.go:418`;`net.go:772-775` 的 `genericReadFrom` 隐藏 ReadFrom 后走 `io.Copy`),不是 16KiB;16KiB 只在两端都不实现 WriterTo/ReaderFrom 时生效(QUIC/ws/wss/包装层)。原材料的「16KiB 帧体」表述需按此修正。
  - TLS 记录头是 **5B**(Go `crypto/tls/common.go:70`)+ AEAD tag 16B = 21B/16KiB(约 0.13%),不是 12B;12B 是 yamux 帧头。原材料这处是笔误。
  - 剂量-反应反例(复算):rh1 ×1→×4→×16 = 2008→2078→2567,批量从 1 帧增到 4/16 帧反而更高;rh4 ×4 1 帧/流却最低 1985;×16 批量更大却 2953。

### H3 —— 「客户端奇数 sid + `sid % N` 使 N=4 只有 2 个分片承载数据」

- **claim**:client 发起的数据流 sid 恒为奇数,`next_stream_id` 每次 +2;N=4 时 client 流在两端 send/recv 都只命中 worker 1、3;server 发起的偶数 sid 才命中 0、2,但数据面没有这种流。设计上的 4 Worker 数据面实际只有 2 个工作。
- **explains**:client sid 1,3,5,7,… %4 = 1,3,1,3,…;server sid 2,4,6,8,… %4 = 2,0,2,0,…;数据面全部是 client 发起(`open_data_channel` 在 client 侧 `sess.mux.open_stream()`,`crates/rh-client/src/session.rs:815-816`),故两端数据面只用 {1,3};vendor 自己的 N=2 测试:8 条 client 出站流 1,3,…,15 全归 SendWorker 1(`shard.rs:2800-2820`)。
- **cannotExplain**:×4≈×1——单条流无论 N 多少都只落 1 个分片,该假设对 ×4 缺口没有解释力;2.0G 聚合上限;×16 的 2.95G 远超「只有 2 个分片」的担忧;CPU 远未饱和(进程约 0.9 核/8 核),2 个活跃 Worker 在 2–3G 下负载很低;**rh4 每载体只有 1 条流,「4 选 2」在结构上不适用于该臂**(与 shard 数取值无关,见下)。
- **verdict**:**【修订】代码可证;运行时未测;对 ×4↓ 无解释力,降为「单载体多流/服务端分片的正确性清理项」。**
- **evidence**(本次逐条核对):
  - `next_stream_id` 每次 +2:`connection.rs:984-994`(Active)、`:2322-2333`(SendHalf);起始 client=1/server=2:`connection.rs:434-437`;奇偶判定 `frame/header.rs:302-308`。
  - 路由:`RecvHalf::route_index` = `(sid.val() as usize) % route_producers.len()`(`connection.rs:1957-1959`);`SendHalf::push_new_receiver` = `(id.val() as usize) % send_regs.lock().len()`(`connection.rs:1429-1434`);`ShardWorker::owns` 同式(`shard.rs:333-335`)。
  - **运行**:`cargo test --manifest-path vendor/yamux/Cargo.toml send_worker_registration_routes_by_sid_mod_n` → `1 passed; 0 failed; 92 filtered out`;全套 `cargo test --manifest-path vendor/yamux/Cargo.toml` → **93 passed**。这条测试是 N=2:8 条 client 出站流全归 worker 1、worker 0 为 0。
  - **⚠️ shard 数口径修正(初稿错误)**:Round-9 被测二进制 **每载体仍是 4 个 shard worker**。依据:`/tmp/r9-src.tgz`(2026-10-17 23:09)中 `connect_stream_pool` 无 `shards` 参数、`mux_config(max_streams)` 无 shard 入参、`shard_workers_for_k` 不存在,`vendor/yamux/src/lib.rs` 的 `Config::default()` = `shard_workers: 4`。下文 `shards=shard_workers_for_k(carriers)` 与 `shard_workers=1` 日志**属于 Round-10 工作树**,只能用于说明「当前代码会怎么配」,不能用于描述 Round-9 的被测形态。**这不影响 H3 的判决**:rh4 每载体只有 1 条流,无论 4 个还是 1 个 shard,该流都只命中 1 个——「4 选 2」不适用是因为**流数**而不是 shard 数。
  - **当前工作树(Round-10)的池化缩减**:`tcp.rs:355-359` `shards=shard_workers_for_k(carriers)`;`mux.rs:781-783` `(4/k).max(1)`;K=4→每载体 1 个 shard。**本机 K=4 客户端日志实证** `shard_workers=1`(`pool sizing ... shard_workers=1`),K=1 时 `shard_workers=4`。server 侧 `wrap_stream_as_server`/`_duplex` 恒传 `DEFAULT_SHARD_WORKERS=4`(`mux.rs:2034-2047`、`:2060-2072`;`tcp.rs:65-66`),仍只有 2 个活跃。
  - **修订 ×16 表述**:rh4 ×16 = 4 载体 × 每载体 4 流;rh1 ×16 = 1 载体 16 流。按 Round-9 实际构建(每载体 4 shard),两者都是**每载体 2 个活跃分片**;按 Round-10 默认(K=4→每载体 1 shard)则 rh4 每载体 1 个、rh1 4 个中活跃 2 个。两种构建下都不存在「16 条流被挤到 2 个分片」的损害迹象(×16 恰是最高档)。
- **test 状态**:导出每个 `ShardWorker::popped_count` 的运行时验证**未执行**;`shard.rs:184` 已有该计数器。

### H4 —— 「池化臂的 4 条流没有均匀落到 4 个载体」

- **claim**:rh4 ×4 退化成单载体 4 流。
- **explains**:rh4 ×4 1985 ≈ rh1 ×4 2078(-4.5%),仅从数值看与「4 流挤在 1 个载体」相似;池化只到 ×16 才 +15%。
- **cannotExplain**:池的 `open_stream` 是严格 `seq % n` 轮转,4 条连续开流在 4 条健康载体上必然一条一个载体;同一臂 3 个样本 1881/2002/1985 极稳定,不像随机分配造成的偶然集中;即使集中,预期也应接近 rh1 ×4≈2.08,实测 1.985 与之仍差 4.5%,无法区分;shard 数不影响本假设(每载体 1 条流,多少个 shard 都只占 1 个),不是「流量集中」。
- **verdict**:**【修订】机制上不支持集中;未实测。** 原「反证」的机制论证成立,但证据只到代码,没有 per-carrier 字节量。
- **evidence**:
  - 轮转:`mux.rs:1496-1497` `let seq = self.inner.rr.fetch_add(1, Relaxed); let m = alive[seq % n].clone()`(原材料原引 `:1453-1456` 是旧行号,已漂移)。
  - K 载体并行拨号:`tcp.rs:348-366`;shards 在 `:357`。
  - 服务端池:`data.rs:75-104` `tcp_refill` 按 pool_size 签发;`:107-155` `tcp_on_channel` 入池/直交;`:158-204` `get_channel` 用 `ready.pop()`(LIFO)+取一补一;每代理 `pool_size=4`(`benchmark/peer.sh:330,333`)。
  - 每个数据通道由 client `sess.mux.open_stream()` 打开(`session.rs:815-816`);控制流 seq0、3 条 attach seq1-3、首个代理 4 条池化通道 seq4-7,在 4 条载体都活时一条一个载体。
  - `REPORT-round9.md:49` 只有 4 条 ESTAB;`:44-45` 池化增益。
  - **本次本机 per-carrier 实测**:K=4 ×4↓ 8s,四条载体服务端字节增量分别为 12267.6 / 12298.9 / 12173.7 / 12304.7 MB,极差 **1.1%**;iperf 四条流 6124/6059/6120/6106 Mbps,极差 1.1%。**均匀**——与本机轮转语义一致。注意这是 macOS loopback、单次测量,且 netstat 绝对值与 iperf 字节数有约 2× 口径差,只用均匀性。
- **test 状态**:三机 per-carrier `ss` 字节量、`open_stream_tagged` 落点记录**未执行**。

### H5 —— 「6MiB 恒定窗 + 半窗 3MiB 回补的信用回路是 ×4↓ 的主约束」

- **claim**:信用回路把单流钳在 3MiB/RTT 或更低。
- **explains**:理论上 3MiB/RTT 的保守下界,若 RTT≥12ms 可落到 2.0G 附近;「建流预授信丢失→只剩 256KiB 初窗→4ms RTT≈512Mbps」的子情形。
- **cannotExplain**:诊断实测两跳 RTT 2.5–4.5ms,3MiB/RTT=5.6–10.1G、6MiB/RTT=11.2–20.1G,比实测高一个数量级;frp 用完全相同的 6MiB 窗与半窗阈值(`connector.go:154`、`server/service.go:720`、`frp-yamux-src/stream.go:286-295`),却在 ×4 达到 2.68G;release 合成 RTT 实验 written=6225920(5.94MiB),预授信在受控路径上生效;「每流 496–520Mbps」只是 2.0G/4 的算术分摊。
- **verdict**:**【修订】主约束反证;预授信丢失子情形未测。**
- **evidence**:
  - `flow_control.rs:58-79` 半窗阈值 max/2、只回补不自增;`lib.rs:51` DEFAULT_CREDIT=256KiB;`mux.rs:827` split_send_size=6MiB、`:849` initial_stream_receive_window=6MiB;`stream.rs:182/196-225` queue_creation_grant。
  - frp 同窗:`client/connector.go:154`、`server/service.go:720`、`frp-yamux-src/stream.go:286-295`、`const.go:126`。
  - **本次运行**:`cargo test --release -p rh-transport --test goal_repair_mux mux_single_stream_credit_stall -- --ignored --nocapture` → `written=6225920 (5.94 MiB), fill-elapsed=5.004528542s, per-round-clamp: 256KiB/RTT≈1049Mbps, 6MiB/RTT≈25166Mbps`。5.94MiB = 5.75MiB 预授 + 3×64KiB。
  - RTT 算术:2.5–4.5ms 下 3MiB/RTT=5.59–10.07G,6MiB/RTT=11.18–20.13G;本机 RTT 0.13–0.28ms 下更高(§4.1)。均远高于 2.0G。

### H6 —— 「CHUNK_CAP=64KiB 的每块 await/syscall、限速预留、smalls 分配构成 2.0G 单核上限」

- **claim**:每 64KiB 的软件成本把单流/单核压到 2.0G。
- **explains**:表面上吻合(每 64KiB 一次 await + 写 syscall + 数次流锁,频率随吞吐线性上升);debug 构建下同一 mux 路径单流只有 2.1G,容易被误读为结构上限。
- **cannotExplain**:release 两跳仿真 mux 单流 6.9–7.4G;限速器在本基准恒 None;smalls 只用于 <64KiB 小块;满块直写、块以 Bytes 引用计数传递;2G 下约 3815 块/s,每块 2µs 仅约 0.76% 核。
- **verdict**:**【修订】用户态成本反证;内核/syscall 成本未测。**
- **evidence**:
  - **本次运行(3 次)**:`cargo test --release -p rh-transport --test goal_repair_mux two_hop_relay_upload_emulator -- --ignored --nocapture` → mux up 7137/7228/6921,down **7440/7358/7275**;copy up 10568/10706/10545,down 10121/10232/10223;no-delay mux up 21437/21157/21163 Mbps。原材料的 7079/7064、10242/10064、21567 是另一次运行,量级一致;应写成范围 6.9–7.4G,不是固定值。
  - `mux.rs:294` CHUNK_CAP;`:608-660` pump_down;`:508-590` write_metered;限速为空:`data.rs:616-622` 双桶皆空返回 None;`rh-protocol/src/proxy.rs:121,132` bandwidth_limit 默认 None;`state.rs:159,170,210` rate_bucket 默认 None;`benchmark/lib/common.sh:392-400` create_client 只发 name;`peer.sh:330,333` 服务 JSON 无 bandwidth_limit。
  - **更正**：「无每块堆分配」不准确。`read_chunk_from` 每块 `pool_get(cap)`(`mux.rs:222-267`),`Vec<Bytes>` push 增引用计数;是池化复用,不是零分配。成本很小,但表述要改。
  - 算术:2Gbps=250MB/s=3815 块/s;每块 2µs→0.76% 核;一个核在 2G 下每块预算 262µs;2.95G 也只有 5627 块/s。**2µs/块是假设,不是测量**;本机实测每 Gbps 0.081–0.12 核(§4.6)。
  - **本次运行**:`cargo test -p rh-transport --lib chunk_cap_is_64kib` → `1 passed`(红线保持 64KiB)。

### H7 —— 「MUX=1 下内核 splice 零拷贝路径结构性不可达」

- **claim**:RELAY=splice 对隧道数据面完全无效;服务端先命中 mux chunk 分支并 return,splice 块在其后;客户端同样先命中 mux 流后 return。
- **explains**:为什么内核零拷贝在 RH 的 mux 隧道里从未生效、与流数无关;诊断轮 metrics:tcp-mux/tls-mux 的 `relay_path_total` 只有 chunk=4,splice/copy=0;`plain_tcp` 只认裸 tokio TcpStream,而通道是 RhMuxStream,downcast 必然失败。
- **cannotExplain**:任何吞吐差异(chunk 路径 release 仿真 7.06–7.08G);×4 与 ×1 的聚合行为;splice 只在裸 TCP 两端可用,本轮无法用 RELAY 开关验证内核零拷贝。
- **verdict**:**支持(与缺口无关)**。
- **evidence**:
  - `data.rs:280-291` 先判 `!use_compression && relay_mode != copy`,剥 `PrependedIo` 后 `mux_steal_or_rewrap`;成功即 `count_relay_path("chunk")` → `mux_forward_chunked` → `return`。splice 块在 `:299-330`,只在两端都是裸 tokio TcpStream 时进入(`:302-329`);mux 流已在上方 return。
  - `mux_steal_or_rewrap`:`data.rs:360-384`;`is_mux_stream`/`steal_mux_halves`:`mux.rs:666-676`;`plain_tcp` 只 downcast 裸 TcpStream:`splice.rs:553-555`。
  - 客户端同样:`forward.rs:118-131` 命中 mux 流即 `mux_relay` 并 `return`;splice 块 `:135-161` 不可达。
  - `DIAGNOSIS-REPORT.md:46-47`:tcp-nomux splice=2;tcp-mux/tls-mux chunk=4,splice/copy=0。**因此 Round-9 标称 `RELAY=splice` 实际跑的是 chunk 路径**,报告里不能把「splice 臂」当成已测。

### H8 —— 「每流 2 pump + 每载体 2 driver + 2N Worker 的任务/唤醒开销,或服务端单线程热点,构成 2.0G 天花板」

- **claim**:任务/唤醒开销或某个单线程热点把 RH 压在 2.0G;frp 靠 goroutine 抢占式调度把 4 个生产者铺开。
- **explains**:K=4 时约 4×(1 recv+1 send+4+4)+8 pump ≈48 个任务;进程级 0.83–0.93 核若被误读为「单线程接近满载」;frp 的 4 个 goroutine 生产者可并行推进,而 RH 单流 pump 是单一 future。
- **cannotExplain**:release 仿真同一条 2 任务/mux_relay 链单流 7.06–7.08G;2G 时约 3815 帧/s、每帧 1–2 次唤醒,总量不到 1 万唤醒/s;DIAGNOSIS-REPORT:41-43 在 ~2.0G 下没有任何线程贴 100%;×16 以相近的 0.92–0.95 核达到 2.95G(但这是跨轮拼接,见下);frp 侧无 CPU 采样。
- **verdict**:**【修订】部分反证;2.95G 的 CPU 未测。**
- **evidence**:
  - `mux.rs:359-430` 每流 spawn 2 个 pump;`:953-960` 每载体 spawn N recv + N send + 1 send driver + 1 recv driver;`spawn_drivers` 在 `:937-961`。
  - **本次运行**:`python3 benchmark/runs/tcp-multiflow-frp-20261015/ab/analyze_round5.py results-r8-cand.jsonl cpu-r8-cand-server.jsonl cpu-r8-cand-client.jsonl` → ×4↓ med **2124**,srv 0.81–0.85(med 0.83),cli 0.48–0.55(med 0.53);×16↓ med **2344**,srv 0.92–0.95(med 0.92),cli 0.65–0.73(med 0.70)。
  - **跨轮更正**:0.92–0.95 核对应的是 **Round-8 ×16↓ 2344 Mbps**;2.95G 是 **Round-9 rh4**,其 CPU 在 `/tmp/r9-peer-rh4/cpu.jsonl`——该目录已不存在(`ls` → No such file or directory)。所以「0.92–0.95 核达到 2.95G」不能这么写。
  - `DIAGNOSIS-REPORT.md:41-43` 的每线程数据覆盖 ~2.0G: tcp-nomux 总 26%、单 tokio worker 24%;tcp-mux 总 42%、3 workers 11–14%;tls-mux 总 69%、3 workers 19–23%——没有线程贴 100%。这支持「2.0–2.34G 下无单线程饱和」,不支持「2.95G 下也没有」。
  - `REPORT-round5.md:86`:×16↓ 负载均匀摊在 7 个 tokio worker(各 ~0.10,合计 ~0.70)。
  - **本次新增**:`/tmp/rhx4` 本机 RH K=4 ×1 19.6G 时 rh-server 6.99s/8s(0.87 核)+ rh-client 5.77s/8s(0.72 核);×4 24.5G 时 11.70s/8s(1.46 核)+ 11.87s/8s(1.48 核)。总计 1.6→2.9 核 / 10 核机器,**远未饱和**。**⚠️ 这组数与 §4.6 表的 17.50G 口径冲突且无留存**,只可用于「CPU 远未饱和」这一定性判断(1.6 与 2.9 核差 1.8 倍,换成 17.50G 分母也还是远未饱和),不可用于推算每 Gbps 成本。

### 3.2 分片路由 `sid % N` 专项核实

**结论:公式与算术全部成立[代码可证];运行时负载没有测[未验证];对 ×4↓ 没有解释力。**

1. **sid 生成**:`next_stream_id` 每次 +2(`connection.rs:984-994`、`:2322-2333`);client 起始 1、server 起始 2(`connection.rs:434-437`)。
2. **路由公式**:`route_index = sid % route_producers.len()`(`connection.rs:1957-1959`);`push_new_receiver = id % send_regs.len()`(`connection.rs:1429-1434`);`ShardWorker::owns = id % shard_count == index`(`shard.rs:333-335`)。三处同式。
3. **算术**:N=4 时 client sid 1,3,5,7,9,… %4 = **1,3,1,3,…**;server sid 2,4,6,8,… %4 = **2,0,2,0,…**。数据面流全部由 client 发起(`session.rs:815-816`),所以两端数据面只用 **{1,3}** 两个分片。
4. **运行证据(仅 N=2)**:`cargo test --manifest-path vendor/yamux/Cargo.toml send_worker_registration_routes_by_sid_mod_n` → **1 passed**;该测试 8 条出站流全归 SendWorker 1、worker 0 为 0(`shard.rs:2800-2820`)。全套 **93 passed**。这证明公式,不证明 Round-9 产物里 shard 0/2 的 `popped_count` 恒为 0——后者需要运行时导出,未做。
5. **Round-9 实际构建(每载体 4 shard)下本项仍不损伤吞吐 —— 用流数论证,不用 shard 数论证**:rh4 的每条载体只有 **1 条数据流**,该流无论落到 4 个 shard 中的哪一个,都只占 **1 个**;「4 个 Worker 只有 2 个在干活」在单流载体上根本不构成瓶颈。只有 **rh1(单载体多流)** 才真正触及这个问题。
6. **当前工作树(Round-10)的 shard 缩减,是另一件事**:`shard_workers_for_k(4) = (4/4).max(1) = 1`(`mux.rs:781-783`),K=4 时每载体 1 个 shard;K=1 时 shards=4。本机日志实证:K=4 `shard_workers=1`、K=1 `shard_workers=4`。**这组数字属于 Round-10,不能用来描述 Round-9 的被测形态**(见文首构建溯源)。
7. **server 侧**:`wrap_stream_as_server` / `_duplex` 恒传 `DEFAULT_SHARD_WORKERS=4`(`mux.rs:2034-2047`、`:2060-2072`;`tcp.rs:65-66`),server 的数据面只有 2 个分片工作。但 server 在 2–3G 下总 CPU ~0.9 核,2 个 task 负载很低,不构成吞吐上限。
8. **对 ×4↓ 的影响**:0。单条流无论 N 多少都只落 1 个分片;rh4 每载体 1 条流(每载体只占 1 个分片,与 shard 数无关)、rh1 的 4 条流落 2 个活跃分片(各 2 条)。它最多影响 rh1 ×16(16 流→2 个活跃分片各 8 条),而 rh1 ×16 恰恰是 2567(比 ×4 高),所以也没有受害迹象。

**判决**:H3 的公式部分**支持(代码可证)**;「4 Worker 只有 2 个在工作→吞吐受损」是因果跳跃,**运行时未测**;应从「×4↓ 修复项」降为「分片路由正确性/单载体多流清理项」。

> **⚠️ 修订:该清理项的优先级在 Round-10 之后反而上升了。** 初稿把它定位为「收益只在 rh1 ×4 与未来 N 配置」,前提是 K=4 为默认。但 Round-10 把 **tcp 默认改成 K=1**(`REPORT-round10.md §3`),而 K=1 → `shard_workers_for_k(1) = 4`。也就是说**默认配置恰好落在 `sid % 4` 只有 2 个分片干活的坏情形**,而 K=4(每载体 1 shard、天然无此问题)反而成了要 `RH_MUX_POOL_SIZE=4` 才有的非默认路径。F3 的定位应据此从「正确性清理」上调为「默认路径上的结构浪费」。

### 3.3 新增假设(本报告提出,均未验证)

**H9 —— ×4 的共享瓶颈不是硬容量,而是「每载体在途流数/信用回补相位」的工作点。**

- claim:×1 与 ×4 落在同一个 ~2.0G 工作点,×16 把它推到 2.6–2.95G;差别不在总流数,而在每载体的在途流数与 WU/写用户 socket 的相位耦合。RH 的 WU 生成点在本流 `pump_down`,而 `pump_down` 会阻塞在用户 socket 写(`mux.rs:513-566`、`:499-662`);单流/少量流时,窗口回补随用户 socket 排空节律振荡;流数多、相位错开时队列更平滑。
- explains:×4 两条臂(1 载体 4 流 / 4 载体 1 流)都 2.0G;×16 反而 2.57–2.95G;frp ×4 2.68G(其每流单帧在飞、天然多流错相)。
- cannotExplain:为什么 rh4 ×4(4 载体、每载体 1 流)不因 4 条载体独立而叠加;为什么 ×4 的 t/d 与 ×16 差这么多;没有 per-stream `send_window`/WU 时间戳,无法证实相位。
- verdict:**未验证(推测)**。
- test:×4 档给每条流导出 `send_window` 最小值、WU 帧时间戳/credit、`pump_down` 阻塞时长;判据:若 ×4 下 send_window 频繁触 0 或 WU 间隔 >>3MiB/吞吐,相位假说获支持。

**H10 —— frp 的 2.31× 主要是「×1 欠填恢复」,不是「聚合能力」;但欠填的具体机制未定。**

- claim:frp ×1 1.16G 远低于同路径 2.0–2.95G 的可达值;×4 的 2.68G 是恢复,不是超过。
- explains:frp ×1 t/d 0.311 vs RH 0.639;本机 frp ×1 7.91G→×4 9.17G 只有 1.16×,TLS 关时 13.41→13.61(1.02×);倍率随工作点变化。
- cannotExplain:三机 ×1 只有 4.4k 帧/s,而本机 frp 帧服务率上限 ~34k 帧/s——「sendLoop 喂不饱」在数量级上不成立;frp 三机无 CPU/逐流数据;TLS 记录节律与 32KiB copy 在路径队列上的交互未测。
- verdict:**部分支持(×1 欠填是事实;机制未定)**。

### 3.4 假设判别矩阵

| 事实 | H1 平台 | H2 批量 | H3 分片 | H4 集中 | H5 信用 | H6 每块成本 | H8 任务/CPU | H9 相位 | H10 frp 欠填 |
|---|:--:|:--:|:--:|:--:|:--:|:--:|:--:|:--:|:--:|
| RH ×1≈×4≈2.0G | ✅ | ⚠️ | ❌ | ⚠️ | ❌ | ❌ | ❌ | ✅ | — |
| RH ×16 到 2.6–2.95G | ⚠️(工作点) | ❌ | ⚠️ | ❌ | ❌ | ❌ | ⚠️ | ✅ | — |
| frp ×1 1.16G、×4 2.68G | ⚠️ | ⚠️ | — | — | ❌ | — | — | — | ✅ |
| 三个独立 relay 都 2.0G | ✅ | ❌ | — | — | ❌ | ⚠️ | ❌ | ✅ | — |
| 发送队列常满、RTT 膨胀 | ✅ | ⚠️ | — | — | ⚠️ | — | — | ✅ | ⚠️ |
| CPU 0.9 核、无单线程 100% | ✅ | — | ❌ | — | — | ✅ | ❌ | — | — |
| 本机 RH ×1→×4 +38%(仅方向) | ❌(仅路径) | — | — | — | — | ✅(软件非限) | ✅ | — | — |
| **⚠️ 前提:「×4 谷底」本身未达显著(p=0.2)** | — | — | — | — | — | — | — | — | — |

✅=能解释;⚠️=部分/需额外假设;❌=不能解释;—=不适用。**没有任何单一假设能同时解释「2.0G 平台」「×16 上升」「frp ×4 2.68G」三件事**——这是本报告最重要的结论之一。

> **⚠️ 但请先读最后一行**:矩阵里所有假设要解释的「×4 谷底」,在 n=3 下**统计上尚不成立**(×4→×16 置换 p=0.2,3v3 最小可能 p=0.1)。也就是说这张矩阵可能是在解释一个抽样波动。**判定的顺序是先补样本(§7.2 第 0 步),而不是先挑假设。**

---

## 4. 理论天花板计算

> 先声明:输入材料的天花板字段是 `[object Promise]`,未解析;以下算式与数字由本报告独立给出,均为**临时值/量级**。区分「代码可证的量级」和「需实测的数」。

### 4.1 信用回路(代码可证的公式 + 实测 RTT)

恒定 6MiB 接收窗、半窗 3MiB 回补(`flow_control.rs:58-79`;`mux.rs:849`):

```
W_eff ∈ [3 MiB, 6 MiB]                  (稳态在半窗与满窗之间振荡)
T_credit ≈ W_eff / RTT_loop             (RTT_loop = WU 往返 + 发送侧排队)
```

| RTT_loop | 3MiB/RTT | 6MiB/RTT |
|---:|---:|---:|
| 0.13 ms(本机 minrtt) | 193.6 Gbps | 387.2 Gbps |
| 0.28 ms(本机 max minrtt) | 89.9 Gbps | 179.8 Gbps |
| 2.5 ms(三机实测下限) | 10.07 Gbps | 20.13 Gbps |
| 4.5 ms(三机实测上限) | 5.59 Gbps | 11.18 Gbps |

- 算式:3 MiB = 25,165,824 bit;6 MiB = 50,331,648 bit;`bit / 秒 = 字节×8 / (RTT_ms×1e-3) / 1e9` Gbps。
- 三机 2.5–4.5ms 下 3MiB/RTT = **5.59–10.07G**,6MiB/RTT = **11.18–20.13G**;均比 2.0G 高 2.8–10 倍。原材料写的 5.6–20.1G 是 3–6MiB 区间,与本次算式一致。
- **代码可证的量级**:6MiB 窗、3MiB 阈值、WU 只在半窗触发。**需实测**:WU 的实际往返时延、发送侧排队深度、是否出现 `send_window` 触 0。现有 `ss` 只有内核发送/接收队列,不是 yamux 信用窗。
- 结论:信用回路**不是** 2.0G 的主约束;但「建流预授信 SYN|WU 丢失/迟到→只剩 256KiB 初窗」这个子情形未被三机数据排除(合成实验只证明受控路径上预授生效)。

### 4.2 BDP 与队列(实测队列 + 算术)

```
BDP = T × RTT
```

| T | RTT | BDP |
|---:|---:|---:|
| 2.0 Gbps | 2.5 ms | 625 KB |
| 2.0 Gbps | 4.5 ms | 1.125 MB |
| 2.95 Gbps | 4.5 ms | 1.66 MB |
| 0.5 Gbps(每载体) | 2.5 ms | 156 KB |

- 三机发送 socket `tb = 4 MiB`(即 SO_SNDBUF 上限,`ss` 的 `tb4194304`),`notsent` 中位 **2.63 MB**、最大 **3.91 MB**(本次从 t1 探针逐样本复算,仅取 70 个数据载体中有 `notsent` 的 25 个)。队列常满,**不是 BDP 饿住应用**;所以「窗口太小」不是解释。
- 反过来,队列常满意味着**排队时延**是 RTT 膨胀的来源(**2.48–2.79ms** vs minrtt 0.13–0.28ms,约 10–20×),这正是 H2/H9 想解释的「工作点」。
- **需实测**:队列占用随时间的变化、`tc qdisc` 统计、NIC 队列/中断;现有数据只有 `ss` 的瞬时 `notsent`/`tb`。

### 4.3 软件每块成本(代码可证的频率 + 假设的单块成本)

```
chunks_per_sec = T / 64 KiB
per_chunk_budget = 1 / chunks_per_sec
```

- 2.0Gbps = 250 MB/s → **3815 块/s**;单核每块预算 **262 µs**。
- 2.95Gbps → **5627 块/s**;每块预算 178 µs。
- 若每块软件成本 ~2µs(锁/唤醒/原子/syscall 的量级假设,不是测量),2.0G 下仅 **0.76% 核**。
- **本次本机实测的每 Gbps 成本(各 1 次 8s `cputime` 差分)**:RH K=4 ×1 19.6G 用 1.59 核(server+client),×4 24.5G 用 2.94 核 → **0.081 / 0.120 核/Gbps**。**⚠️ 分子分母来自不同口径**:1.59 核对的是 19.6G,而 §4.6 表给同一配置的 ×1 是 17.50G;若改用 17.50 则为 **0.091 核/Gbps**。取区间 **0.081–0.091**。按此外推,2.0G 只需 **0.16–0.27 核**;10 核机器上余量 **≥35×**。这是实测的**上界**(loopback 含 iperf 竞争),真实三机 CPU 更高(Round-2 服务端 0.9 核@2.0G),但仍远未饱和——**该结论对 0.081 还是 0.091 不敏感**。
- **代码可证**:每块 1 次读 await、1 次写 syscall(满块)、约 4 次 vendor 流锁、1 次分片队列锁、1 次用户 socket 锁、2 次唤醒、4 个计量原子。**需实测**:每块真实 CPU 周期、锁竞争、wakeup 延迟;`perf`/`pidstat` 在三机缺失(`DIAGNOSIS-REPORT.md:1-6`),只有 `/proc/PID/task/*/stat`。

### 4.4 frp 单流在飞上限(代码可证的串行 + 实测帧率)

frp `Stream.Write` 等 `errCh`,`sendLoop` 写完才 `asyncSendErr`(`stream.go:219`;`session.go:432,522`)。因此:

```
单流在飞 ≤ 1 帧 × min(sendWindow, 32 KiB body, 16 KiB TLS record)
T_single ≈ frame / (sendLoop_write_time + handoff)
```

- 三机 ×1 1161 Mbps = 145 MB/s → 32KiB 帧约 **4.4k 帧/s**;×4 2682 Mbps → **10.2k 帧/s**。
- 本机 frp TLS 开:×1 7.91G → **30.2k 帧/s**;×4 9.17G → **35.0k 帧/s**;×16 9.04G → 34.5k 帧/s。**本机帧服务率上限约 34–35k 帧/s**,×1 已达 87%,所以本机 ×4 只有 1.16×。**⚠️ 本机这几个绝对 Gbps 同样没有留存原件(见 §4.6 可信度声明)**,但 30.2k vs 35.0k 是同一口径内的相对比较,且「×1 已接近饱和」这一判读只依赖比值,不受绝对幅度影响。
- **关键推论**:三机 ×1 只有 4.4k 帧/s,离 frp 自身的帧服务率上限还有 **7–8 倍**;所以三机 ×1 的低速**不是** sendLoop 帧处理能力不够,而是每帧在路径上等待(队列/窗口/用户连接)造成的。H10 的「欠填」是事实,但把它归因于「sendLoop 喂不饱」在数量级上不成立。**该推论对幅度不敏感**(即使本机上限只有 20k,仍是 4–5 倍差距),故保留。
- **代码可证**:单流 1 帧在飞、sendLoop 单 goroutine、每帧 2 次写(TLS 3 次)。**需实测**:三机 frp 的 CPU、每帧在飞时间、sendCh 占用、TLS 记录数。

### 4.5 「代码可证的量级」vs「需实测的数」

| 量 | 代码可证 | 需实测 |
|---|---|---|
| 信用回路上限 | 3–6 MiB 窗、3 MiB 阈值、WU 12B | 实际 WU 往返、`send_window` 是否触 0、SYN\|WU 是否丢/迟 |
| BDP/队列 | 4 MiB SO_SNDBUF、64KiB 块 | 队列占用时序、qdisc、NIC 队列/中断 |
| 软件成本 | 每块 syscall/锁/唤醒/原子次数 | 每块 CPU 周期、锁竞争、wakeup 延迟 |
| frp 单流 | 1 帧在飞、2–3 次写/帧、单 sendLoop | 三机帧率上限、CPU、在飞时间 |
| RH 聚合上限 | 64 命令通道、6MiB split、批量 64 帧/1MiB | 实际批深、每批帧数、writev 段数 |
| 路径容量 | 无 | 2.0 vs 2.68 vs 2.95G 的工作点差异 |

### 4.6 本机实测的天花板参照(loopback,无两跳队列)

> 环境:macOS 27 / Apple Silicon(10 核)/ iperf3 3.22 / 同一台机器;RH 用当前工作树 `cargo build --release` 构建;frp 用 `benchmark/frp-src-v0.71.0` + Go 1.27.2 本地构建(为绕过前端 embed,临时建 `web/frps/dist`、`web/frpc/dist` 存根,构建后已删除;`frps --version` = 0.71.0);refrelay 用 `benchmark/diag/refrelay.rs`,macOS 无 `splice(2)`,把 splice 分支替换为 copy(仅 copy 模式可比),缓冲分别 16/32/64KiB。**loopback 只能排除软件上限,不能代表三机两跳队列。**

> **⚠️ 可信度声明(本次复核新增,请先读):本表数值没有可复现留存,只能采信方向,不能引用幅度。**
> - **同一配置出现两个不同数值**:RH K=4 的 ×1↓ 在本文件里同时写作 **17.50G**(本表、§7.1.5)和 **19.6G**(§3.1 H8 evidence、§4.3)。按 19.6 算 ×4/×1 = 24.5/19.6 = **1.25**,不是表中的 1.38;按 17.50 算每 Gbps 成本 = 1.59/17.5 = **0.091 核/Gbps**,不是 §4.3 的 0.081。两者不可能同为同一配置的 3 样本中位。
> - **找不到原始产物**:本地日志 `/tmp/rhx4/iperf3.log`(2026-10-18 18:44)中,17.50 / 15.86 / 24.21 / 18.69 / 29.92 / 30.14 / 21.06 这些关键值**全部 0 命中**;该日志可解析的 21 个测试主要是 8.8–9.25G 与 24.6–24.8G。refrelay 的 `rustc -O` 临时构建物已不存在(`/tmp/refrelay*` 无匹配)。frp TLS on/off 的 7.9G / 13.4G 量级在日志中**可以复现**,这部分可信度较高。
> - **结论**:本表仍是全篇唯一直接反证「RH 聚合代码失效」的实验,H1 的「强」评级有一半靠它;但在补出 `iperf3 -J` 原件(或重跑一遍并留存)之前,**§0.1 的「+38%」应降级为「方向:聚合有效,幅度不可引用」**,§4.3 的每 Gbps 成本同理。

| 形态 | ×1↓ | ×4↓ | ×16↓ | 备注(样本数) |
|---|---:|---:|---:|---|
| RH K=4(4 载体) | 17.50G ⚠️ | **24.21G** | 15.16G | ×4/×1=**1.38**;各 3 样本取中位;四条 iperf 流 5992/6056/6024/5954,极差 1.7%。⚠️ ×1 另有 19.6G 记录,见上 |
| RH K=1(单载体) | 15.86G | **18.69G** | 15.54G | ×4/×1=**1.18**;×1/×4 各 3 样本,×16 仅 2 次成功(16690/14383) |
| refrelay copy 64KiB | 29.92G | **30.14G** | 20.44G | ×1/×16 各 1 样本,×4 4 样本(28177/28600/31687/33030) |
| refrelay copy 32KiB | — | **28.37G** | — | 3 样本(26733/28370/29491) |
| refrelay copy 16KiB | 17.62G | **21.06G** | 17.88G | ×1/×16 各 1 样本,×4 4 样本(20493/20527/21598/22643);16KiB 在 ×1/×4 都**更低** |
| frp TLS 开(默认) | 7.91G | **9.17G** | 9.04G | 各 3 样本取中位;×4/×1=**1.16**;四条流各 2.26–2.29G,极均匀 |
| frp TLS 关 | 13.41G | **13.61G** | — | 各 3 样本取中位;×4/×1=**1.02** |
| 直连 iperf(无中继) | 49.8–55.5G | 47.6–65.8G | 38.6–51.1G | 3–6 样本,波动大,仅作分母参照 |

**从本机表能得到的硬结论**:

1. **RH 的 ×1→×4 聚合在无路径瓶颈时是有效的**(方向确凿:+18%/+38% 两种口径同号),且四条流均匀(极差 1.1–1.7%);「RH 聚合代码失效」被直接反证。**幅度不可引用**,理由见上。
2. **frp 在本机比 RH 慢 2.2–2.7×**(×1 7.91 vs 17.50;×4 9.17 vs 24.21),CPU 也更贵(以下为各 1 次 8s 测量的 `cputime` 差分):frp TLS 开 ×1 用 frps 1.50+frpc 1.43=2.93 核@7.93G(0.37 核/Gbps),×4 1.76+2.02=3.78 核@9.03G(0.42 核/Gbps);RH K=4 ×1 0.87+0.72=1.59 核@**19.6G(0.081)**、×4 1.46+1.48=2.94 核@**24.5G(0.120)**。**RH 每 Gbps 便宜 3.5–4.6×**(若改用 17.50G 口径则为 3.3–4.2×,结论不变)。三者只能用**相对**比较,绝对 Gbps 不可跨口径引用。
3. **frp 的 ×1/×4 倍率随工作点变化**:本机 1.16×(TLS 开)/1.02×(TLS 关),三机 2.31×。所以 2.31× 不是 frp 的结构常数,不能用它证明「frp 聚合更强」。
4. **16KiB 块在本机 ×4 反而更差**,不支持「块越小、交织越好」的普遍化;三机是否不同,仍未测。

---

## 5. 修复方案(按验证成本排序)

> 排序原则:先做「零代码、能判定方向」的测量,再做「同二进制、env gate、可回退」的改动;每一项都给出**局部快速判据**和**三机判据**。`target`(初稿名为 `expectedCeiling`,与 §4 的「理论上限」同名不同义,这里统一改名)里的百分比一律注明口径;「归一化靶」= 用各臂自己的直连基线乘 frp 的 t/d 0.939。

**口径先统一(重要)**:

- 同拓扑绝对靶:frp ×4 2682 vs rh1 ×4 2078 → **+29.1%**(不是 +36%)。此式只用到隧道侧中位,**不受直连分母噪声影响,是本文最稳的一个数**。
- rh4 绝对靶:2682/1985 → **+35.1%**(不是 +36%;2.682/1.985=1.351)。
- 归一化靶:0.939×2645 = **2484(+19.5%)**;0.939×2724 = **2558(+28.9%)**。**⚠️ 这两个数只是点估计**:t/d 在 3×3 全配对下的实际范围是 rh1 ×4 ∈ [0.730, 0.899]、rh4 ×4 ∈ [0.597, 0.867](见 §0.1 后的误差表),对应归一化靶的浮动区间约为 **+9% ~ +30%**。请当区间用,不要当硬指标。
- frp 2682 是**单样本**,且该臂直连基线最高;在补到 3 样本前,不要把 2682 当硬靶。

### F0 —— 同窗口仪器化(拆为 F0a 零代码 / F0b 需埋点;最高优先级)

> **修订:初稿把「需改代码的埋点」也算进了「零代码」,会让今晚就能跑的部分被编译拖住。拆成两步。**

#### F0a —— 零代码采集(可以立刻跑)

- **what**:同一时间窗内背靠背跑 rh1/rh4/frp ×4↓ 各 3 样本(随机交错,不要按臂顺序跑),同时采:①iperf3 `end.streams[].receiver.bits_per_second`(逐流)+ 完整 `-J` 留存;②两端每条载体 `ss -tin` 的 `bytes_sent/bytes_acked` 增量,**并标注每个 socket 的方向**(数据 vs ACK——本次复核发现 t1 探针 96 个 socket 里 70 个是 `tb=4MiB` 数据载体、26 个是 `tb=87040` 的 ACK,不标注就会误读);③rh-server/rh-client/**iperf3/target.py** 的 `/proc/PID/stat`(现在只采 RH 两个进程,`peer.sh:195,242`);④`ethtool -l`、`ethtool -S`、`/proc/interrupts | grep ens`、`mpstat -P ALL 1`;⑤载体 socket 的 `SO_SNDBUF`/`TCP_NOTSENT_LOWAT` 实际值。
- **样本量**:×1/×4/×16 各补到 **5–8 样本**(不是 3)。理由见 §0.2 结论 F:3v3 的最小可能 p 是 0.10,现有数据连「×4 是谷底」都证明不了;不把样本量提上去,后面所有判据都建在沙子上。
- **targets**:H1(平台)、H4(载体分布)、H8(CPU)、以及**「×4 谷底是否存在」这个首要前提**。
- **where**:`benchmark/peer.sh:83-86`(cpu sampler)、`:383-394`(采样顺序);`round9-evidence/raw/`(需重建,现在不存在);`DIAGNOSIS-REPORT.md` 的 `probe.sh` 可复用(`t1/out/probe-*.ss/.proc/.threads`)。
- **target**:无(测量项);它决定后续是否值得改代码。
- **risk**:零行为风险;仅日志量/采样开销(每秒 `ss` 对 2G 路径 <0.1% CPU,诊断轮已用过)。
- **quickTest**:本机可先验证采样脚本不崩;三机 1 个窗口(约 30–40 分钟,含加样本)。
- **stop/go 判据(修订:由绝对阈值改为相对判据)**:原稿的「同窗 frp ×4 < 2.4G」会抖——frp 是单样本、臂间漂移 ±10%。改为:**若 RH ×4 与 ×1 的差 < 组内 CV、四条流均匀、且软中断/单线程/单任务均无饱和 → 停止代码优化,转 F6;若 RH 有明显热点(某线程贴 100%、某流被饿、载体严重不均)且同窗 frp ×4 显著高于 RH → 进入 F1–F5。**

#### F0b —— 需埋点的运行时导出(排在 F0a 之后)

- **what**:①yamux 每流 `send_window` 最小值 + WU 帧时间戳/credit;②`pump_down` 阻塞时长;③每 `ShardWorker::popped_count`(`shard.rs:184` 已有该计数器,只差导出)。
- **targets**:H9(相位)、H5 的「预授信丢失」子情形、H3 的运行时负载。
- **why 单列**:这三项都要加临时 debug 计数并重新构建三机二进制,与 F0a 的「零代码」不是一回事;混在一起会让 F0a 无法「今晚就跑」。
- **risk**:埋点本身改变时序;只可用于归因,不可用于吞吐对照。测完必须回退。

#### F0c —— refrelay ×4(⚠️ 本次复核新增:全篇性价比最高的单个实验)

- **what**:用 `benchmark/diag/refrelay.rs`(**128 行、无 yamux、无 TLS、64KiB 拷贝**)在三机两跳路径上跑 **`iperf3 -P 4 -R`**,3–5 样本。
- **为什么它最重要**:全文的物理故事是「这条两跳路径在 1–4 流工作点约 2.0G」,但支撑它的 refrelay 数据(`DIAGNOSIS-REPORT.md §2`) **全部是单流**(copy 2003 / splice 1992)。也就是说:**从来没有人测过「一个哑 relay 在 4 条并发流下这条路径能给多少」。** 这一个数直接决定 §0 的分叉:
  - 若 refrelay ×4 也只有 **~2.0G** → 路径工作点假说成立,RH 无责,直接转 F6 环境项;
  - 若 refrelay ×4 能到 **2.6–2.9G** → **路径不是限制**,frp 的 2.68G 变成「哑 relay 也做得到、RH 做不到」,缺口重新落回 RH 的每流/聚合结构,H1 的解释力大幅下降。
- **where**:`benchmark/diag/refrelay.rs`(注意 macOS 无 `splice(2)`,三机是 Linux 可直接用 splice 模式)+ `iperf3 -P 4`。
- **target**:无(判别实验);成本约 10 分钟,信息量大于 F1–F5 之和。
- **risk**:无(诊断二进制,不动产品代码)。
- **排期**:F0a 之后、F1 之前。

### F1 —— yamux 发送批 env gate:1MiB→128/256KiB(证伪「批量节律」)

- **what**:`MAX_BATCH_BYTES` 1MiB→128/256KiB(`MAX_BATCH_FRAMES` 保持 64),同二进制 A/B;开关放 `yamux::Config`(如 `max_batch_bytes`),由 `mux_config_with_shards` 设置,env 只在 `rh-transport` 层解析——不要直接在 `frame/io.rs:73` 的 `const` 里读 `std::env`,否则不可复现。
- **targets**:H2a(已反证,做确认实验)+ H1(工作点)。不提高单流软件上限。
- **where**:`vendor/yamux/src/frame/io.rs:66`(MAX_BATCH_FRAMES=64)、`:73`(MAX_BATCH_BYTES=1MiB);`connection.rs:2218`(fill_batch 的 while 条件)。
- **target**:不承诺。归一化靶 **+19.5%~+28.9%**;绝对同拓扑 **+29.1%**;rh4 绝对 **+35.1%**。反向证据:rh4 ×4 每批帧数最少(每载体 1 流,但批深取决于队列,不是 1 帧;`STREAM_COMMAND_CAP=64` 允许单流连续压 64 条命令,`connection.rs:367`;64KiB 帧下 `fill_batch` 最多 16 帧/批)却是最低 1985;×16 批更深反而 2953。剂量-反应与「越浅越好」相反,所以预期收益不确定,更像**证伪实验**。
- **risk**:可能伤 ×16↑(Round-7 加深批给它 +6.0%,`round7-acceptance-report.md:54`;同报告 `:53,:55` download×4 -6.9%、×16 -15.2% 提示下行方向相反);批变浅会增加 writev 次数(1MiB→128KiB 约 8×)与帧/唤醒数。必须 env-gated、默认保持现值。
- **quickTest**:实现 env 后跑 `RH_YAMUX_BATCH_BYTES=131072 cargo test --release -p rh-transport --test goal_repair_mux two_hop_relay_upload_emulator -- --ignored --nocapture`,与本次基线 **mux down 7275–7440 / up 6921–7228 Mbps** 比;**3 次取中位,变化 >10%** 才值得上机(本次 3 次跑动的 spread:mux up ±2.2%、down ±1.1%、copy down ±0.5%、no-delay mux ±0.7%;审读意见看到的 5.5% 也说明 5% 门槛偏紧)。三机判定=同二进制同窗口 ×1/×4/×16 各 3 对,×4↓ 中位 ≥2.4G 为正向、≤2.0G 证伪并回退;**×4↑/×16↑ 不得回退超过噪声**。

### F2 —— 裸 TCP 载体 `TCP_NOTSENT_LOWAT`(先修正机制,再决定做不做)

- **what**:给裸 TCP 载体加 env-gated `TCP_NOTSENT_LOWAT`(默认 0=现状),首档 1MiB。
- **targets**:H1/H2 的「第二跳队列膨胀」候选;**先验很弱**,见下。
- **where**:`crates/rh-transport/src/mux.rs:686-689`(YamuxCarrier::new,调用点 `:878`、`:908/:913`);实现参照 `crates/rh-transport/src/splice.rs:39-49`、`:639-650`。
- **target**:**≈0(先验)**。理由:
  1. `TCP_NOTSENT_LOWAT` 是**可写低水位**(unsent < lowat 才唤醒 writable),**不是未发送队列的硬上限**;内核仍可把队列填到 `SO_SNDBUF`(4MiB)。「把队列从 4MiB 压到 1MiB」是概念错误。
  2. 同机最接近的实验已经用过 1MiB:`splice.rs:39` 的 `NOTSENT_LOWAT=1MiB` 在 tcp-nomux(splice) 路径生效(`DIAGNOSIS-REPORT.md:41` 的 `tcp-nomux(splice) 1920/2010`),仍是 ~2.0G。要在 mux 载体上再试同一个值,需先解释「mux 载体与 splice 载体有什么不同」。
  3. BDP 论证要用 **base RTT**(0.13–0.28ms),不是被队列膨胀后的 2.5–4.5ms:2G×0.19ms≈48KB,1MiB 是 BDP 的 ~21×,不会 app-limit;若用 2.7G×4.5ms≈1.45MiB,则 1MiB < BDP,但这恰恰说明「1MiB > BDP」的原文写法不稳健。
- **risk**:`TCP_NOTSENT_LOWAT` 在 libc 0.2.190 只于 `unix/linux_like/mod.rs:991` 定义;macOS SDK `netinet/tcp.h:246` 是 `0x201`,本地只能编过/跳过,不能验证效果;TLS/WS/QUIC 载体 downcast 失败必须静默跳过;值过低会 app-limit;env 默认 0 保证零漂移。
- **quickTest**:本机只能编译/跳过;判定必须在 Linux 三机做同窗口 0 vs 1MiB 配对,中位差 > 噪声带才保留。**建议排在 F1/F5 之后。**

### F3 —— 分片路由 `sid % N` → `(sid >> 1) % N`(⚠️ 优先级上调:Round-10 之后是**默认路径**上的浪费)

- **what**:三处同改:Recv 路由、Send 注册、opening 归属;收敛为一个 `fn shard_index(sid, n) -> usize`,三处都调用;加 property test 对 sid 1..2N 断言三处一致。
- **targets**:H3(代码可证的结构浪费)。**对 rh4 ×4↓ 预期仍为 0**——每载体只有 1 条流,落哪个分片都只占 1 个(注意:**与 shard 数是 4 还是 1 无关**,初稿「因为每载体 shards=1 所以不适用」的论证口径有误,已更正)。
- **⚠️ 优先级变化**:初稿把它定为「正确性清理项」,前提是 K=4 为默认、K=1 是特例。但 **Round-10 把 tcp 默认改成 K=1**(`REPORT-round10.md §3`),而 K=1 → `shard_workers_for_k(1) = 4`。也就是说**默认配置正好落在 `sid % 4` 只有 2 个分片干活的坏情形**,而 K=4 反而成了要 `RH_MUX_POOL_SIZE=4` 才有的非默认路径。收益面从「未来配置」变成了「默认配置的 rh1 形态(含默认单载体部署)」。
- **where**:`vendor/yamux/src/connection.rs:1957-1959`(route_index)、`:1429-1434`(push_new_receiver)、`vendor/yamux/src/connection/halves/shard.rs:333-335`(owns);既有断言 `shard.rs:2800-2820` 需同步改写。
- **target**:rh4 ×4↓ **≈0**;rh1(默认 K=1)×4↓ 可能 +0~10%(4 条流从 2 个活跃分片变 4 个),但 CPU 只有 0.9 核,先验仍弱。**不要把它当 ×4 缺口的解**。
- **risk**:**中高**。三处必须同一公式,漏一处会破坏 `opening` 归属(出站流首帧收编)与 `ctrl Remove`/FIN/RST 路由,表现为流关闭挂起或 FIN 丢失。现有测试 `send_worker_registration_routes_by_sid_mod_n` 编码了旧行为,必须重写;改后跑全套 93 项 + 「开 100 条流、逐条关闭、断言每分片 ctrl 排空」的回归。
- **quickTest**:现在就能验证现行行为(本次已跑):`cargo test --manifest-path vendor/yamux/Cargo.toml send_worker_registration_routes_by_sid_mod_n` → 1 passed;改后同一测试应断言 N=4 时 sid 1,3,5,… 均分到 4 个 shard,再跑 `cargo test --manifest-path vendor/yamux/Cargo.toml`(93 项)。

### F4 —— `pump_down` 满 64KiB 也并入聚合,凑 256KiB 一次 writev

- **what**:把满块也放进 `smalls` 聚合路径,满 256KiB 或「无后续就绪块」时冲刷,把用户侧写 syscall 从 ~3815/s 降到 ~954/s。
- **targets**:H6 已反证的软件成本;不改变路径上限。
- **where**:`crates/rh-transport/src/mux.rs:611-660`(满块分支 :616;smalls 聚合/冲刷 :628-657)。
- **target**:**≈0**。本机仿真 mux down 6.9–7.4G、copy down 10.1–10.2G,已是 2.0G 路径速率的 3.5×;仿真提升 <10% 就不该上机。
- **risk**:交互小流量最坏多等 192KiB(100Mbps 下约 15ms);必须沿用「无后续就绪块即冲刷」并加时间上限;`write_metered_vectored` 在冲刷前一次性 `reserve(total)`,256KiB 的预留比 64KiB 更容易在令牌桶上产生突发等待;`SMALL_CHUNK_SEGS=128` 与「先 flush smalls 再写大块」的顺序语义要显式测试。CHUNK_CAP=64KiB 红线不动(`mux.rs:2131` 测试)。
- **quickTest**:改前后各跑 3 次 release 仿真,看 mux down 是否从 7275–7440 明显上升(>10%);不动或下降即 no-change。

### F5 —— `split_send_size` 6MiB→16KiB(只对 rh1 ×4 有可能,先验为负)

- **what**:env-gated A/B 16/32/64KiB,让单载体上 4 流的线帧粒度更细。
- **targets**:H2b(64KiB vs 16KiB);rh4 ×4(每载体 1 流)结构上无跨流交织,预计影响小于 rh1 ×4,但**不保证为零**(帧数 ×4,包数/中断/GRO 行为都会变)。
- **where**:`crates/rh-transport/src/mux.rs:827`。
- **target**:rh1 ×4 绝对靶 +29.1%,归一化 +19.5%;rh4 ×4 ≈0。**先验为负**:本机 refrelay 16KiB ×4 中位 21.06G vs 64KiB 30.14G(更低);且 `STREAM_COMMAND_CAP=64` 在 16KiB 帧下只有 1MiB 在途命令缓冲(64×16KiB),6MiB 发送窗还在,但命令通道可能先满,`pump_up` 会更早等待。
- **risk**:帧数/命令/唤醒 ×4,CPU 从 0.9 核抬升;`split_send_size` 是全局 config(`mux.rs:827`),会同时影响控制流与小流量;应做成 per-connection `Config` 字段,env 只在 `rh-transport` 解析。
- **quickTest**:实现 env 后跑 `RH_MUX_SPLIT_SEND=16384 cargo test --release -p rh-transport --test goal_repair_mux two_hop_relay_upload_emulator -- --ignored --nocapture`;预期 mux up/down 明显下降(基线 6921–7440);下降 >10% 直接判死。

### F6 —— 路径/环境 A/B(不改产品代码,最高信息量)

- **what**:①refrelay 两跳 + `iperf3 -w 256K/512K`(只动 iperf 参数,不动 sysctl);②载体 socket `SO_SNDBUF` 2/4MiB A/B;③`fq_codel`/pacing 现状记录;④交换 server/client 角色跑一次 ×4↓;⑤`iperf3 -b` 定速,看 app-limited 区间。
- **targets**:H1/H9/H10 的「队列/节律」候选;这是诊断报告自己提出但未跑的可证伪实验(`DIAGNOSIS-REPORT.md:78-79`)。
- **where**:`benchmark/diag/refrelay.rs`、`benchmark/peer.sh`、三机 sysctl/`tc`(只读记录)。
- **target**:若 bufferbloat 是主因,×4↓ 可能从 2.0G 向 2.6–2.9G 靠近;若不变,则「队列深度」假设被否定,剩余只能归因于端点或 frp 的协议节律。
- **risk**:只影响测试床,不是产品改动;但改 sysctl 需回滚,`SO_SNDBUF` A/B 需要临时构建。
- **quickTest**:refrelay 两跳 ×4↓,`-w 4M`(现状)vs `-w 256K` vs `-w 512K`,各 3 样本;判据:若 `-w 256K/512K` 使吞吐变化 >10%,队列深度/节律成立。

### F7 —— frp 去 TLS 对照(诊断项,不是修复)

- **what**:frp `transport.tls.enable=false` ×4,3 样本;以及本地 TLS on/off 复核。
- **targets**:分离「16KiB TLS 记录节律」与「32KiB copy 节律」;H2b/H10。
- **where**:`benchmark/peer-frp.sh:64-77`(tcp 分支不写 tls.enable,默认 true);`frp-src-v0.71.0/pkg/config/v1/client.go:165,182-185`。
- **target**:无(诊断)。**本次本机已做(各 3 样本 clean pair)**:TLS 开 ×1 7.91G/×4 9.17G;TLS 关 ×1 13.41G/×4 13.61G——TLS 在本机让 frp 慢约 33–41%(×1 13.41→7.91,×4 13.61→9.17),×4 增益从 16% 降到 2%。若三机去 TLS 后 frp ×4 仍 ≥2.6G,则「TLS 记录节律」不是 frp 的优势来源;若掉回 2.0–2.2G,则 TLS 记录节律是关键。本次已补到各 3 样本(TLS 开:TLS-on run1–3 的 P1 7985/7913/7899、P4 9169/8802/9207;TLS 关:P1 13333/13409/13405、P4 13676/13358/13605)。
- **risk**:只影响对照臂;frp 默认 TLS 开,生产对照不能去掉。

---

## 6. 独立审读意见与修订

> 逐条列出审读意见及处理。**采纳**=已写进本报告;「部分采纳」=结论方向对但幅度/归因修正;「不采纳」=与本次实测冲突(本次没有出现完全不采纳的条目,只有两处幅度修正)。

### 6.1 第一轮审读(初稿期)

| # | 审读意见 | 判定 | 本报告处理与复核 |
|---|---|---|---|
| 1 | H1「聚合不扩展」是把描述当因果;直连 ×4 本身掉了 15.8–23.5%,t/d 反而上升 | **采纳** | §0/§1 改为「1–4 流同一工作点」;t/d 0.639/0.564→0.786/0.729 写入;直连跌幅逐臂列出 |
| 2 | H1「若每流独立 2.0G,4 流应逼近路径上限」是稻草人 | **采纳** | 删除该论证;改为「共享瓶颈在 4 流下同样给出总和≈2.0G」 |
| 3 | H1 拓扑错位:frp ×4 是 1 TCP + 4 mux 流,rh4 是 4 物理载体;同拓扑是 rh1 | **采纳** | §0/§1 明确;同拓扑 -22.5%(绝对)/-16.3%(归一化);rh4 的 -26% 单列 |
| 4 | H1「池化 -4.5%」不是显著效应;±10% 噪声带是臂间漂移,不是组内 CI | **采纳** | 改为「点估计 -4.5%,3 样本置换 p=0.2,与持平不可区分」;删除 ±10% 误引 |
| 5 | frp 每格 1 样本,且该臂直连基线最好;2682 不能当硬靶 | **采纳** | §0.2/§5 口径;F0 要求补 3 样本 |
| 6 | H2「发送节律差异」仍是相关不是因果;refrelay 也是 64KiB,不能否定「64KiB vs 16KiB」 | **采纳** | 拆 H2a(反证)/H2b(未验证);本机补做 16/32/64KiB refrelay,结果 16KiB ×4 更低 |
| 7 | H2「rh4 ×4 每批 1 帧」不成立;`STREAM_COMMAND_CAP=64` 允许单流连续压 64 条命令 | **采纳** | 删除「每批 1 帧」;改为「批深取决于队列,64KiB 帧下最多 16 帧/批」;剂量-反应论证降级 |
| 8 | H2 的 TLS 「12B 头」错误;TLS record header 5B + tag 16B = 21B | **采纳** | §2.2/§3 H2 更正;12B 是 yamux 帧头 |
| 9 | H2 的 ×1/×4 同向预测不干净 | **采纳** | 改为「×1 无跨流交织,机制不同,不能同向预测」 |
| 10 | H3 结构事实对,但「4 Worker 只有 2 个→吞吐受损」是因果跳跃 | **采纳** | verdict 降为「代码可证;运行时未测」;对 ×4 无解释力 |
| 11 | H3「×16 每载体 4 条流,至多 2 个活跃分片」混用 rh1/rh4 | **采纳** | 拆成 rh4(每载体 shards=1,无 4 选 2)与 rh1(16 流/2 活跃分片各 8 条)两句 |
| 12 | H3 单测只证 N=2,不是生产 N=4 运行时 | **采纳** | 明确「1 passed」只证公式;`popped_count` 运行时验证未执行 |
| 13 | H4 机制对,但「根本没有需要解释的效应」用错噪声带;组内应做置换检验 | **采纳** | 置换 p=0.2 写入;删除 ±10% 组内引用 |
| 14 | H4「必然一条一个载体」假设了 seq4-7 且中间无 open | **采纳** | 改为「在 4 条健康载体、无额外 open/成员死亡时」;加 `open_stream_tagged` 落点测试 |
| 15 | H4 LIFO + 池水位可能造成瞬时偏斜 | **采纳** | 写入 per-carrier 测试目的;本机 8s 长跑四条载体极差 1.1%,但三机未测 |
| 16 | H5 3MiB/RTT 是模型下界,上界约 6MiB/RTT | **采纳** | §4.1 按 3/6MiB 两端列 |
| 17 | H5 合成 RTT 测试只证代码路径,不证三机 SYN\|WU 不丢 | **采纳** | 子情形标「未测」;列入 F0 采集项 |
| 18 | H5「500Mbps = 2.0G/4」是算术巧合 | **采纳** | 不再当吻合证据 |
| 19 | H6 release 仿真只覆盖用户态,不能排除内核 syscall/NIC | **采纳** | verdict 改为「用户态反证;内核/syscall 未测」;§4.5 分列 |
| 20 | H6「无每块堆分配」不准确;每块 `pool_get(cap)` | **采纳** | 改为「每块一次池取/还,无逐块 Vec 分配」 |
| 21 | H6 7G 是运行相关的,应写范围 | **采纳** | 改为 6.9–7.4G(本次 3 次) |
| 22 | H6 限速器 None 在本基准成立(证据硬) | **采纳** | 保留,并补 `common.sh:392-400`、`peer.sh:330,333` |
| 23 | H7 代码事实正确;Round-9 标称 splice 实际 chunk=4 | **采纳** | 保留 verdict;加「不能把 splice 臂当已测」 |
| 24 | H8「0.92–0.95 核@2.95G」是跨轮拼接;Round-9 CPU 缺失 | **采纳** | 拆成「Round-8 2344Mbps@0.92–0.95 核」与「Round-9 2953Mbps CPU 未入库」 |
| 25 | H8 低 CPU 证据只覆盖 2.0–2.34G | **采纳** | 明确覆盖范围;2.95G 列入必采 |
| 26 | H8 frp 无 CPU 采样,「goroutine 调度更好」无实测 | **采纳** | 保留「不可分离」;F0 要求采 iperf/target/frp CPU |
| 27 | F1 +36% 是绝对比 + 单样本锚点;同拓扑 +29.1%,归一化 +19.5%/+28.9% | **采纳** | §5 口径统一;F1 expectedCeiling 改区间 |
| 28 | F1 quickTest 5% 门槛落在仿真噪声里 | **部分采纳** | 本次 3 次 spread 0.5–2.2%,审读看到 5.5%;改为「3 次中位、>10%」 |
| 29 | F1 env gate 应放 `yamux::Config`,不要库内读 env | **采纳** | F1 已写 |
| 30 | F2 机制错误:`TCP_NOTSENT_LOWAT` 是低水位不是队列上限 | **采纳** | F2 expectedCeiling 改 ≈0;机制段落重写 |
| 31 | F2 最接近的 splice 实验已用 1MiB 仍 2.0G | **采纳** | 写入先验;F2 排在 F1/F5 后 |
| 32 | F2 BDP 要用 base RTT,不是膨胀 RTT | **采纳** | 改用 0.13–0.28ms,~48–70KB |
| 33 | F3 对 rh4 ×4 预期≈0,应作正确性清理 | **采纳** | verdict/expectedCeiling 改 0;独立于性能轮 |
| 34 | F3 三处同改不够,应收敛为一个函数 + property test | **采纳** | F3 已写 `shard_index` + 三处一致断言 |
| 35 | F3 现有测试编码旧行为,改后必须重写;FIN/RST 风险中高 | **采纳** | risk 升「中高」;加 100 流关闭回归 |
| 36 | F4 5% 门槛在噪声里;延迟风险不止 192KiB;限速预留被拉长 | **采纳** | 改为 10%;加时间上限/非空 limiter 测试 |
| 37 | F5 命令缓冲从 4MiB 降到 1MiB,「结构上无感」不成立 | **采纳** | F5 expectedCeiling/risk 补 `STREAM_COMMAND_CAP=64` 的 1MiB 在途 |
| 38 | F5 `split_send_size` 是全局 config,应 per-connection | **采纳** | 写入 risk |
| 39 | 计划顺序错了:应先同窗对照,再写代码 | **采纳** | 新增 F0;F1–F5 全部排在其后 |
| 40 | 没有全局 stop/go 与 no-regression 矩阵 | **采纳** | F0 加 stop/go;F1/F4/F5 加 ×1/×4↑/×16↑ 不回退 |
| 41 | 天花板字段 `[object Promise]` 未解析 | **采纳** | 报告开头声明;所有 ceiling 标临时 |
| 42 | 行号漂移:session.rs:93→194/225;open_data_channel 781→815-816;轮转 1453→1496;round9 CPU 60→63 | **采纳** | 本报告全部用本次 `grep -n` 复核后的行号;§附录 B 列对照 |
| 43 | `round9-evidence/raw/` 不存在(不是空目录) | **采纳** | 开头声明;36 条 `raw_json` 悬空 |
| 44 | `/tmp/r9-peer-rh4` 不存在 | **采纳** | 开头声明;H8 引用的 CPU 不可得 |
| 45 | 诊断只有 `ethtool -k/-i`,没有 `-l`/中断/mpstat | **采纳** | F0 必采;§4.5「需实测」 |
| 46 | iperf3/target CPU 没采样 | **采纳** | F0 必采;`peer.sh:195,242` 只采 RH 两进程 |
| 47 | 「user 侧约半数 app_limited」口径不清;t1 数据 socket 0/26,t3 的 app_limited 是 ACK 方向 | **采纳** | 本次复算:t1 数据 socket 0/26;t1 的纯 ACK socket 26/26;t3 iperf 接收 socket 29/29 但是 ACK 方向;不再用它证明数据流被饿住 |
| 48 | frp TLS 默认开,是未隔离混杂因素;关 TLS 正好去掉 16KiB record,不只是「隔离加密成本」 | **采纳** | 本机 A/B 已做(TLS 开/关:7.9/13.2 ×1,9.15/13.05 ×4);三机 F7 |
| 49 | 方向/机器不对称没有拆开 | **采纳** | F6 交换角色;F0 采双向 softirq/ss |
| 50 | 测试顺序/窗口漂移;frp 补到 3 样本 | **采纳** | F0 随机交错/背靠背;§1.3 反常 6/7 |

### 6.2 第二轮审读(2026-10-18 晚,独立复核:重算中位数 / 重跑置换检验 / 抽查约 40 处行号)

> 复核方法:从三份 JSONL 重算全部 12 格 × 3 臂中位数(结果与 §1.1 逐格一致);重跑 A8 的 5 组精确置换检验(全部复现);逐份打开源码抽检约 40 处行号(frp 侧 `sendCh` cap / `sendLoop` 两次写 / 单流 1 帧在飞 / 6MiB 窗 / TLS 默认 / `poolCount=4+tcpMux` 均核对无误;RH 侧 `route_index`、`push_new_receiver`、`owns`、`next_stream_id`、`STREAM_COMMAND_CAP`、`CHUNK_CAP`、`shard_workers_for_k`、`mk_limiter`、`plain_tcp`、`DEFAULT_SHARD_WORKERS` 均核对无误);从 `probe-rh-tcpmux-download.ss` 独立复算队列/RTT 统计。**结论:算术与代码引用基本全对;下述 14 条是仅有的问题,其中 #51/#52/#53/#56/#57/#58 会改变结论的强度或优先级,其余为口径与表述修正。**

| # | 复核发现 | 判定 | 本报告处理 |
|---|---|---|---|
| 51 | **构建溯源错误**:文首写 HEAD `33f646f`,但该 commit 不含多载体池(`connect_stream_pool` 不存在、`mux.rs` 仅 1599 行);被引行号只在 `e6a9bcc` 成立 | **采纳(最重要)** | 文首新增「构建溯源」段,明确三个版本:33f646f / e6a9bcc(当前树)/ `/tmp/r9-src.tgz`(被测) |
| 52 | **被测二进制的 shard 数弄错**:Round-9 打包源码中 `Config::default().shard_workers = 4`,即 rh4 是「4 载体 × 每载体 4 shard」,不是文中多处写的「每载体 shards=1」 | **采纳** | §3.1 H3、§3.2 第 5–8 条、§0.2 D、F3 全部改口径;**H3 判决不变且论证更强**(改用流数而非 shard 数论证) |
| 53 | **环回数字自相矛盾**:同配置 ×1↓ 一处 17.50G(§4.6/§7.1.5)、一处 19.6G(§3.1/§4.3);据此算出的 1.38× 与 0.081 核/Gbps 互斥 | **采纳** | §4.6 加可信度声明并标 ⚠️;§4.3 改 0.081–0.091 区间;§7.1.5 降级为「仅方向」 |
| 54 | **环回数据无原始产物**:`/tmp/rhx4/iperf3.log` 中 17.50/15.86/24.21/18.69/29.92/30.14/21.06 全部 0 命中;refrelay/frp 临时构建物已删 | **采纳** | §4.6 顶部声明;§0.1 的 +38% 降级为方向;§7.1.5/7.1.6 标 ⚠️ |
| 55 | **t/d 归一化无误差棒**:直连分母 CV 达 14.6–15.4%,frp 分母 n=1;16.3% 被当作点估计 | **采纳** | §0.1 后新增 3×3 全配对误差表(10%–25%);§0.2 C 改区间;§5 归一化靶标浮动区间 |
| 56 | **「×4 谷底」本身不显著**(×4→×16 置换 p=0.2;3v3 最小 p=0.10),全篇追问建立在未排除抽样波动的前提上 | **采纳** | §1.3 新增反常 8;§0.2 新增结论 F;§0.3 改写;§7.2 新增第 0 步「先加样本」 |
| 57 | **漏掉最强复现证据**:`REPORT-round10.md` 在独立构建/独立日期下复现了「×4≈×1、×16 最高」;且其 K=4 ×16(2.50G)与 Round-9 rh4 ×16(2.95G)的差可能与 shard 数有关 | **采纳** | §1.4 新增 Round-10 行与对照表,并标为「线索非结论」 |
| 58 | **漏掉最高性价比实验**:refrelay 数据全是单流,从没测过「哑 relay ×4」;这一个数决定「路径限制」vs「RH 结构」 | **采纳** | 新增 F0c;§7.2 提到第 1 步 |
| 59 | F0 自称「零代码」却含需埋点项(`send_window`/WU 时间戳) | **采纳** | 拆为 F0a(零代码)/ F0b(埋点)/ F0c(refrelay ×4) |
| 60 | F0 的 stop/go 用绝对阈值「frp ×4 <2.4G」,但 frp 单样本、臂间漂移 ±10%,判据会抖 | **采纳** | 改为相对判据(差 < 组内 CV 且无热点) |
| 61 | **F3 优先级被低估**:Round-10 后 tcp 默认 K=1 → `shard_workers=4`,`sid%4` 只有 2 个活跃分片的坏形态成了**默认路径** | **部分采纳** | F3 标题与理由改写为「默认路径上的浪费」;但仍明确不计入 ×4↓ 收益 |
| 62 | §2.1 stage 9 标 `serialPerStream=false(生成后跨任务)`,与 H9「WU 生成与用户写同任务耦合」矛盾 | **采纳** | 核对 `stream.rs:491`:WU 判定在 `poll_read_chunk` 开头、早于取块 `:502`;**改为 true**,并注明只有 `FlushTask` 写 socket 是跨任务 |
| 63 | `expectedCeiling`(§5 的「靶」)与 §4 的「理论天花板上限」同名不同义 | **采纳** | §5 全部改名 `target` |
| 64 | 「三机 frp ×1 只有 4.4k 帧/s、离上限 7–8 倍」推理正确,但本机 34–35k 帧/s 上限本身也来自不可复现的环形数据 | **部分采纳** | 保留推理与数量级结论(7 倍差距对幅度不敏感),但在 §4.4 标注本机帧率同样未留存原件 |

---

## 7. 下一步最小验证清单

> 目标:本机能跑的、每项 ≤10–20 分钟、合计 ≤1 小时;先做能**证伪**的,不做需要三机的。已执行项标「✅ 本次已跑」并给输出;未执行项标「⬜」。

### 7.1 本机(无需三机)

**7.1.1 ✅ 门槛/红线测试(约 1 分钟)**

```bash
cargo test --manifest-path vendor/yamux/Cargo.toml
cargo test -p rh-transport --lib chunk_cap_is_64kib
```

输出:yamux **93 passed; 0 failed**;chunk_cap **1 passed**。

**7.1.2 ✅ release 两跳仿真 ×3(约 10 秒)**

```bash
for i in 1 2 3; do
  cargo test --release -p rh-transport --test goal_repair_mux \
    two_hop_relay_upload_emulator -- --ignored --nocapture
done
```

输出(3 次):mux up **7137/7228/6921**,mux down **7440/7358/7275**;copy up 10568/10706/10545,down 10121/10232/10223;no-delay mux up 21437/21157/21163 Mbps。
判据:软件上限是 2.0G 路径的 3.5–3.7 倍 → H6 用户态成本反证成立。

**7.1.3 ✅ 信用合成 RTT(约 5 秒)**

```bash
cargo test --release -p rh-transport --test goal_repair_mux \
  mux_single_stream_credit_stall -- --ignored --nocapture
```

输出:`written=6225920 (5.94 MiB), fill-elapsed=5.004528542s, 256KiB/RTT≈1049Mbps, 6MiB/RTT≈25166Mbps`。
判据:预授信 5.75MiB 在受控路径生效 → H5 的「只剩 256KiB 初窗」子情形在代码路径上不成立(三机仍需 `send_window` 导出)。

**7.1.4 ✅ 分片路由公式(约 10 秒)**

```bash
cargo test --manifest-path vendor/yamux/Cargo.toml \
  send_worker_registration_routes_by_sid_mod_n
```

输出:**1 passed; 92 filtered out**。N=2 时 8 条 client 出站流全归 SendWorker 1。
判据:公式成立;N=4 的运行时 `popped_count` 仍需导出。

**7.1.5 ⚠️ 本机 RH 环回 scaling + per-carrier(约 10 分钟)—— 数值不可复现,只留方向**

做法:本机起 `rh-server`(bind 17100, `relay_mode=splice`)→ 建客户端/服务 → 写 `client.toml`(`mux_pool_size=4` 或 `1`)→ `rh-client` → `iperf3 -s -p 19201` → `iperf3 -c 127.0.0.1 -p 19101 -P {1,4,16} -R -t 5 -J` ×3。
输出(本次):

| K | ×1↓ | ×4↓ | ×16↓ | ×4/×1 |
|---|---:|---:|---:|---:|
| 4 | 17.50G ⚠️ | **24.21G** | 15.16G | 1.38 |
| 1 | 15.86G | **18.69G** | 15.54G(2 次) | 1.18 |

per-carrier(K=4 ×4↓,8s):四条载体 12267.6/12298.9/12173.7/12304.7 MB,极差 1.1%;iperf 四流 6124/6059/6120/6106 Mbps。

> **⚠️ 复核结论:本节数值没有留存,不能作为证据引用。**(a)同配置 ×1↓ 在 §3.1/§4.3 写作 **19.6G**,与此处 17.50G 矛盾;(b)`/tmp/rhx4/iperf3.log` 中这些值 0 命中;(c)refrelay 与 frp 的临时构建物均已删除。**可采信的只有方向**:聚合路径有效、四流均匀(极差 1.1–1.7%)。若要恢复其证据地位,需重跑并按 §7.2 第 0 步留存 `-J` 原件。

判据(仅方向):**RH 在无路径瓶颈时聚合有效**,H1 的「聚合代码失效」被反证;×4 的 2.0G 只能归因于三机路径工作点。**幅度(+38%)不作数。**

**7.1.6 ⚠️ refrelay 16/32/64KiB(约 10 分钟)—— 构建物已删,数值不可复现**

做法:`rustc -O` 编译 `benchmark/diag/refrelay.rs` 的 copy 变体(macOS 无 `splice(2)`,把 splice 分支替换为 copy;只改 `vec![0u8; N*1024]`),起三个端口 → `iperf3 -c 127.0.0.1 -p {19301,19302,19303} -P {1,4,16} -R -t 5 -J`。
输出:64KiB ×1/×4/×16 = 29.92/30.14/20.44G;32KiB ×4 = 28.37G;16KiB ×1/×4/×16 = 17.62/21.06/17.88G(×4 为中位,样本见 §4.6)。
判据:**16KiB 在 ×4 反而更低**,不支持「块越小越好」的普遍化;三机仍需 F6 的 16KiB 两跳对照。

> **⚠️ 复核结论**:该脚本的 `rustc -O` 构建物已不存在(`/tmp/refrelay*` 无匹配),上面三组数值无原件。但**方向结论(16KiB ×4 < 64KiB ×4)与 §4.6 的相对次序一致**,且这是本机唯一做过块大小扫描的证据,故保留判据、注明幅度待复现。

**7.1.7 ✅ frp TLS on/off(约 10 分钟)**

做法:本地构建 `frps`/`frpc`(Go 1.27.2;为绕过前端 embed 临时建 `web/frps/dist`、`web/frpc/dist` 存根,构建后删除),`frpc.toml` 用 `transport.poolCount=4`、`transport.tcpMux=true`,分别加/不加 `transport.tls.enable=false`,跑 `iperf3 -c 127.0.0.1 -p 19102 -P {1,4} -R -t 5 -J`。
输出(各 3 样本中位):TLS 开 ×1 7.91G/×4 9.17G(1.16×);TLS 关 ×1 13.41G/×4 13.61G(1.02×)。
判据:**frp 的 ×4 倍率随工作点变化**,2.31× 不是结构常数;TLS 在本机让 frp 慢约 33–41%。

**7.1.8 ⬜ 批量 env gate 的仿真 A/B(约 20 分钟,只在 F1 获准后做)**

```bash
RH_YAMUX_BATCH_BYTES=131072 cargo test --release -p rh-transport \
  --test goal_repair_mux two_hop_relay_upload_emulator -- --ignored --nocapture
```
判据:与 7.1.2 的 3 次中位比,>10% 才上机;否则 no-change。

**7.1.9 ⬜ 分片路由改动的 regression(约 10 分钟,只在 F3 获准后做)**

```bash
cargo test --manifest-path vendor/yamux/Cargo.toml   # 93 项
```
外加新写「100 条流逐条关闭、每分片 ctrl 排空」回归。

### 7.2 三机(不是本机,下一窗口;按顺序)

> **⚠️ 顺序已按本次复核重排。** 初稿第 1 步是「F0 同窗对照(3 样本)」,但 3 样本不足以判定「×4 谷底是否存在」(3v3 最小 p=0.10,实测 p=0.2)。先加样本 → 再加判别实验 → 最后才动代码。

0. **加样本(前置,约 20 分钟)**:rh1/rh4 的 ×1/×4/×16 ↓↑ 各补到 **5–8 样本**,随机交错。**判据**:×4 与 ×1 的差是否超过组内 CV(现为 2.4–3.3%)。**若不超 → 「×4 谷底」判为抽样波动,§3 全部假设无需被解释,直接结案。** 这一步比后面全部加起来更省时间。
1. **F0c refrelay ×4(约 10 分钟,最高信息量)**:`benchmark/diag/refrelay.rs` + `iperf3 -P 4 -R`,3–5 样本。**判据**:哑 relay 的 ×4 若也 ~2.0G → 路径工作点成立,转 F6;若到 2.6–2.9G → 缺口在 RH 结构,继续 F1/F5。**这一个数能把整篇分析的分叉一次切开。**
2. **F0a 同窗对照**(约 30 分钟):rh1/rh4/frp ×4↓ 随机交错(用第 0 步加厚后的样本);采逐流 iperf `-J` 原件、per-carrier `ss` 字节**并按方向标注**、RH+iperf3+target CPU、`ethtool -l/-S`、`/proc/interrupts`、`mpstat -P ALL`。**判据**:四条流是否均匀?每条载体是否一条流?软中断/单线程是否饱和?(改用相对判据,不再用「frp <2.4G」的绝对阈值)
3. **F7 frp 去 TLS ×4**(3 样本):若仍 ≥2.6G,TLS record 节律不是优势来源;若掉到 2.0–2.2G,TLS record 节律是关键。
4. **F6 refrelay 16KiB 两跳 + `iperf3 -w 256K/512K`**(各 3 样本):分离块大小与队列深度。
5. **F1/F5 同二进制 env gate**(×1/×4/×16 双方向,各 3 对):×4↓ 中位 ≥2.4G 且 ×4↑/×16↑ 不回退才保留;否则回退默认。
6. **F3 分片路由清理(优先级上调)**:Round-10 后 tcp 默认 K=1 → `shard_workers=4`,**默认路径正落在 `sid%4` 只有 2 个分片干活的形态**,已从「未来配置的正确性项」变成「默认配置的结构浪费」。仍不计入 ×4↓ 收益。
7. **F2/F4 仅在 1–5 的数据指向低水位/syscall 时再做**;**F0b 埋点**只在 H9/H5 需要归因时做。

### 7.3 本机与三机的可比性(避免误读)

- 本机 loopback 无 RTT/队列,只能**反证软件上限**(H6/H8/H3/H5)与**证明聚合路径有效**,不能复现三机的 2.0G 工作点,也不能验证 F1/F2/F5 的上机收益。
- 本机 iperf3 3.22、macOS 27、Apple Silicon;三机是 Linux 5.15、iperf3 3.17.1、vmxnet3。两者的 socket 默认值(`tcp_wmem`、`TCP_NOTSENT_LOWAT`、GRO/GSO)不同,**不要跨环境比较绝对 Mbps**。
- 本机 frp 是源码本地构建(Go 1.27.2,frp `go.mod` 声明 1.25.0),不是官方发布二进制;只用于 A/B 方向判断。

---

## 附录 A:本次实际运行的命令与关键输出

| # | 命令 | 输出摘要 |
|---|---|---|
| A1 | `cargo test --manifest-path vendor/yamux/Cargo.toml` | 93 passed; 0 failed |
| A2 | `cargo test -p rh-transport --lib chunk_cap_is_64kib` | 1 passed |
| A3 | `cargo test --release -p rh-transport --test goal_repair_mux two_hop_relay_upload_emulator -- --ignored --nocapture` ×3 | mux down 7440/7358/7275;mux up 7137/7228/6921;copy down 10121/10232/10223;no-delay mux up 21437/21157/21163 |
| A4 | `cargo test --release -p rh-transport --test goal_repair_mux mux_single_stream_credit_stall -- --ignored --nocapture` | written=6225920(5.94 MiB),fill-elapsed=5.004528542s,256KiB/RTT≈1049,6MiB/RTT≈25166 |
| A5 | `cargo test --manifest-path vendor/yamux/Cargo.toml send_worker_registration_routes_by_sid_mod_n` | 1 passed; 92 filtered out |
| A6 | `python3 benchmark/runs/tcp-multiflow-frp-20261015/ab/analyze_round5.py results-r8-cand.jsonl cpu-r8-cand-server.jsonl cpu-r8-cand-client.jsonl` | ×4↓ 2124(srv 0.83/cli 0.53);×16↓ 2344(srv 0.92/cli 0.70) |
| A7 | python3 读三份 round9 JSONL,按 `(path,dir,streams)` 取中位 | §1.1/§1.2 全部数字 |
| A8 | 3v3 精确置换检验(中位差) | rh1 ×4 vs ×1 +70 p=0.6;rh4 +51 p=1.0;rh1×4 vs rh4×4 93 p=0.2;rh4 ×16 vs ×4 968 p=0.2;最小可能 p=0.1 |
| A9 | 本机 RH loopback(K=1/K=4,×1/×4/×16,5s×3) | §4.6/§7.1.5 |
| A10 | 本机 refrelay copy 16/32/64KiB(×1/×4/×16,5s×3) | §4.6/§7.1.6 |
| A11 | 本机 frp v0.71.0(TLS 开/关,×1/×4,5s) | §4.6/§7.1.7 |
| A12 | 本机 per-carrier `netstat -anv -p tcp` 字节差分 | 四载体极差 1.1%;绝对值与 iperf 有约 2× 口径差,只用均匀性 |
| A13 | `ls round9-evidence/raw` | No such file or directory |
| A14 | `ls /tmp/r9-peer-rh4` | No such file or directory |
| **A15** | 独立复算:python3 从三份 JSONL 按 `(path,dir,streams)` 重取中位 | **12 格 × 3 臂全部与 §1.1 一致**;rh1/rh4 各 36 条 throughput、frp 12 条(每格 n=1) |
| **A16** | 独立重跑 A8 的 5 组 3v3 精确置换检验 | rh1 ×1vs×4 差 70 **p=0.60**;rh4 ×1vs×4 差 51 **p=1.00**;rh1×4 vs rh4×4 差 93 **p=0.20**;rh4 ×4vs×16 差 968 **p=0.20**;rh1 ×4vs×16 差 489 **p=0.20** |
| **A17** | 独立复算 `probe-rh-tcpmux-download.ss`(140 块 / 96 socket) | 70 个 `tb=4MiB` 数据载体 + 26 个 `tb=87040` ACK;`app_limited` 数据侧 **0/70**、ACK 侧 **26/26**;`notsent` 中位 **2.63MB**、最大 **3.91MB**(n=25);RTT 中位 **2.48ms**(全数据)/**2.79ms**(有 notsent 者),minrtt 0.194–2.004ms |
| **A18** | 构建溯源:`git show 33f646f:crates/rh-transport/src/{mux,tcp}.rs`、`git grep` | `33f646f` 无 `connect_stream_pool` / `shard_workers_for_k` / `RH_MUX_POOL_SIZE`;`mux.rs` 1599 行(现 3807 行) |
| **A19** | 构建溯源:`tar -xzO /tmp/r9-src.tgz ./crates/rh-transport/src/tcp.rs` 等 | 被测源码 `connect_stream_pool` 无 `shards` 参数、`mux_config(max_streams)` 无 shard 入参;`vendor/yamux/src/lib.rs` `shard_workers: 4` ⇒ **Round-9 rh4 = 4 载体 × 每载体 4 shard** |
| **A20** | `grep` 本机台账 | `/tmp/rhx4/iperf3.log` 中 17.50/15.86/24.21/18.69/29.92/30.14/21.06 **全部 0 命中**;`/tmp/refrelay*` 不存在;frp TLS 的 7.9G/13.4G 量级**可复现** |
| **A21** | 抽查约 40 处 `sed -n` 行号核对 | RH 侧 `route_index:1957-1959`、`push_new_receiver:1429-1434`、`owns:333-335`、`next_stream_id:984-994`、初值 `:434-437`、`STREAM_COMMAND_CAP:367`、`CHUNK_CAP:294`、`shard_workers_for_k:781-783`、`mk_limiter:616-622`、`plain_tcp:553-555`、`DEFAULT_SHARD_WORKERS:777`;frp 侧 `sendCh cap 64:session.go:108`、`sendLoop:477-527`、`Stream.write:170-182,219`、6MiB 窗 `connector.go:154`、TLS 默认 `client.go:183`、`peer-frp.sh:130-131` — **全部命中** |
| **A22** | `grep -ci cpu benchmark/peer-frp.sh` | **0**(frp 臂确无 CPU 采样) |
| **A23** | 3×3 全配对 t/d 敏感度 | rh1×4 ∈[0.730,0.899]、rh4×4 ∈[0.597,0.867]、rh4×1 ∈[0.465,0.643];直连 CV:rh4×1 **14.6%**、rh4×4 **15.4%** |

## 附录 B:行号核对表(本次 `grep -n` 复核,替代旧稿)

> **⚠️ 适用范围**:下表所有行号均以**当前工作树 `e6a9bcc`** 为准。**它们不是 Round-9 被测二进制的行号**——被测源码是 `/tmp/r9-src.tgz`,其 `mux.rs` 只有 1599 行、`tcp.rs` 的 `connect_stream_pool` 无 `shards` 参数。引用行号时必须同时说明是「当前树」还是「被测构建」,否则会重现初稿那句「HEAD 33f646f」的错误。

| 旧稿/材料 | 本次核对 |
|---|---|
| `session.rs:93` 调 `connect_pool` | `crates/rh-client/src/session.rs:225`(当前树);被测构建中在 `:89-93` |
| `open_data_channel` 在 `session.rs:781` | 函数在 `:815`,`sess.mux.open_stream()` 在 `:816`(当前树) |
| 池轮转 `mux.rs:1453-1456` | `mux.rs:1496-1497`(当前树) |
| `REPORT-round9.md:60` CPU 采样 | `:63` |
| H3 的「×16 每载体 4 流至多 2 活跃分片」 | 见 §3.2 第 5–8 条:rh4/rh1 分开写,且按**流数**而非 shard 数论证 |
| `STREAM_COMMAND_CAP` 位置 | `connection.rs:367`(值 64) |
| `DEFAULT_SHARD_WORKERS` | `mux.rs:777`(值 4) |
| `shard_workers_for_k` | `mux.rs:781-783`(**当前树/ Round-10 新增;被测构建无此函数**) |
| `tcp.rs` 的 `shards=shard_workers_for_k(...)` | `tcp.rs:355-359`(**Round-10 新增;被测构建此处无 `shards`**) |
| server 侧恒 4 shard | `mux.rs:2034-2047`、`:2060-2072`;`tcp.rs:65-66` |
| WU 判定位置(新) | `stream.rs:487-566`,判定在 **`:491`**(早于取块 `:502`)→ §2.1 stage 9 由 false 改 **true** |
| frp `sendCh` cap | `frp-yamux-src/session.go:108`(值 64) |
| frp TLS 默认 | `pkg/config/v1/client.go:165,182-185`(`Complete()` 在 `:183` 置 true) |
| TLS record header | Go `crypto/tls/common.go:70`(5B),不是 12B |
| `TCP_NOTSENT_LOWAT` | libc 0.2.190 `unix/linux_like/mod.rs:991`;macOS `netinet/tcp.h:246` = 0x201 |
