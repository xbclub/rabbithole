# 多载体池自适应定容:前因后果与技术方案

> 本文档面向接手实施的工程师/AI。所有"已核实事实"均标注了源码位置;所有性能数字均来自
> 真实三机实测(2026-10-17 / 10-18)。请先读 §1-§4 建立背景,再读 §5 方案。

---

## 0. 一页纸摘要

- **已实现**:多物理载体池(`YamuxMultiplexerPool`)+ 三段式定容(协议基线 → 硬件上限 → 服务端上限)。
- **当前默认**:tcp/ws = 1 条载体,tls/wss = 2 条,quic/kcp = 1 条。**自动路径到不了 4**。
- **实测结论**:K 的最优值取决于并发流数——×1/×4 时 K=1 更优(快 5~7%),×16 时 K=4 更优(快 5~31%)。
- **问题**:自动逻辑看不到并发流数(连接建立时流还不存在),所以只能手工配 `RH_MUX_POOL_SIZE=4`。
- **目标**:多客户端场景下,服务端把有限的核预算自动分配给最需要的客户端,零人工调优。
- **关键前提**:`COST_PER_CARRIER`(每载体实际消耗几核)**尚未标定**,必须先做一次实验。

---

## 1. 前因:为什么会有多载体池

### 1.1 最初的问题

单条 TCP 连接的收包软中断会集中在一个 CPU 核上(网卡 RSS 只按四元组散列)。
在 16 流高速灌水下,单个核先被打满,而链路本身还有余量。

### 1.2 多载体的机制

开 K 条独立物理连接(不同本地源端口 → 不同四元组 → RSS 散到不同核):

- 收包软中断分摊到 K 个核
- TLS 下 = K 组独立的 Record 加解密流水线
- 单条连接的拥塞丢包不再造成全局队头阻塞

源码:`crates/rh-transport/src/mux.rs` 的 `YamuxMultiplexerPool`。
业务流按全局原子计数轮转分派到第 `seq % K` 条载体。

### 1.3 但多载体有代价

K 条载体各分走一部分流 → 每条载体上的流变少 → 每帧更小 → 批聚合从 16 帧退化到 4 帧
(单次 writev 覆盖的流数下降)→ **每字节 syscall 成本上升**。

同时多付 K 份 driver + Worker 的固定开销(每条载体是一整套 Framer + 4 ShardWorker
+ 4 SendWorker + FlushTask)。

---

## 2. 已实现的定容逻辑(现状)

### 2.1 三段式公式(源码实测)

`crates/rh-client/src/session.rs:93-101`:

```rust
fn initial_k_for_with(cfg, protocol, hardware_cap) -> (usize, usize) {
    let protocol_baseline = rh_transport::default_carriers(protocol);
    let desired = cfg.mux_pool_size.unwrap_or(protocol_baseline);
    (desired.min(hardware_cap).clamp(1, 8), hardware_cap)
}
```

三段含义:

| 段 | 来源 | 作用 |
|---|---|---|
| `desired` | 显式配置 > 环境变量 > 协议基线 | **想要几条** |
| `hardware_cap` | CPU 核数 / 网卡 RX 队列 | **最多能开几条**(只压不抬) |
| `server_cap` | 服务端 `max_carriers`(报到后取得) | **服务端允许几条** |

最终:`final_k = initial_k.min(server_cap).clamp(1, 8)`。

### 2.2 协议基线

`crates/rh-transport/src/lib.rs:314-319`:

```rust
pub fn default_carriers(protocol: Protocol) -> usize {
    match protocol {
        Protocol::Tls | Protocol::Wss => 2,
        _ => 1,
    }
}
```

**这就是"自动路径到不了 4"的原因**:tcp 的基线是 1,而硬件上限只能往下压。

### 2.3 硬件上限

`crates/rh-client/src/config.rs:188-194`:

```rust
fn detect_hardware_cap_with(cpus: usize, queues: Option<usize>) -> usize {
    let cpu_cap = (cpus / 2).max(1);
    match queues {
        Some(q) => cpu_cap.min(q.max(1)).clamp(1, 8),
        None => cpu_cap.clamp(1, 8),
    }
}
```

- `cpus/2`:假设每载体约 1.5~2 核。**这个系数从未实测标定过。**
- `queues`:Linux 读 `/sys/class/net/*/queues/rx-*`;非 Linux 或读不到为 `None`(不钳制)。

### 2.4 配置优先级

1. 配置文件 `mux_pool_size = N`(`Option<usize>`,缺省为 `None` = 自动)
2. 环境变量 `RH_MUX_POOL_SIZE=N`
3. 协议基线

### 2.5 服务端上限

`crates/rh-server/src/config.rs`:`max_carriers` 默认 8,钳制 1..8。
服务端在附加载体报到时:若 `池内附加载体数 + 2 > cap` 则回 `CarrierAttachAck{ok:false, error_code:"POOL_FULL"}` 且不聚合。

**注意**:这是**每客户端**上限,没有全局上限。100 个客户端各开 4 → 服务端 400 条载体。

---

## 3. 实测数据(真实三机,可直接引用)

### 3.1 拓扑

```
test3(driver) --10GbE--> test1(rh-server / frps) --隧道--> test2(rh-client + iperf3)
```

三机均 8 核,网卡 ens160 多 RX 队列(实测 `hardware_cap = min(8/2, queues) = 4`)。
Linux release 构建,PROTO=tcp,RELAY=splice,MUX=1,每档 10 秒。

### 3.2 Round-10 验收(2026-10-18)

| 验收项 | 结果 |
|---|---|
| 不设环境变量,tcp | K=1,物理连接 1 条,`shard_workers=4`,`pooled=false` |
| 不设环境变量,tls | K=2,物理连接 2 条,`failed=0` |
| 服务端 `max_carriers=1` + tls | 报到被 `POOL_FULL` 拒绝 → 收敛到 K=1,物理连接 1 条 |
| `RH_MUX_POOL_SIZE=4` + tcp | K=4,物理连接 4 条,`shard_workers=1` |

定容日志实证:

```
protocol=tcp protocol_baseline=1 hardware_cap=4 server_cap=1 initial_k=1 final_k=1 shard_workers=4 pooled=false
protocol=tls protocol_baseline=2 hardware_cap=4 server_cap=8 initial_k=2 final_k=2 shard_workers=2 pooled=true
protocol=tcp protocol_baseline=1 hardware_cap=4 server_cap=8 initial_k=4 final_k=4 shard_workers=1 pooled=true
```

### 3.3 吞吐对照:直连 / 单载体 / 池化 / frp

同一窗口内 direct/tunnel 紧邻配对,rh 各 3 样本取中位,frp 1 样本。

| 方向×流 | rh K=1 隧道 | rh K=4 隧道 | frp 隧道 | rh K=1 直连 | rh K=4 直连 | frp 直连 |
|---|---:|---:|---:|---:|---:|---:|
| ×1↓ | **2008** | 1934 | 1161 | 3141 | 3431 | 3732 |
| ×1↑ | **1991** | 1980 | 1141 | 3021 | 3319 | 3949 |
| ×4↓ | **2078** | 1985 | 2682 | 2645 | 2724 | 2855 |
| ×4↑ | 1987 | **2431** | 2222 | 2935 | 2839 | 3466 |
| ×16↓ | 2567 | **2953** | 2821 | 2526 | 2742 | 2710 |
| ×16↑ | 2063 | **2706** | 2465 | 2853 | 2671 | 3187 |

**池化增益(K=4 vs K=1,隧道绝对值)**:
×16↓ +15.0%,×16↑ +31.2%,×4↑ +22.3%,×4↓ -4.5%,×1↓ -3.7%,×1↑ -0.6%。

**相对 frp(K=4 隧道 / frp 隧道)**:
×16↓ 1.05×,×16↑ 1.10×,×4↑ 1.09×,×4↓ 0.74×,×1↓ 1.67×,×1↑ 1.74×。

### 3.4 Round-10 复测(同日更晚窗口)

| 流数 | K=1 | K=4 | 差值 |
|---|---:|---:|---:|
| ×1 | **2.01** Gbps | 1.92 Gbps | K=1 +4.7% |
| ×4 | **2.13** Gbps | 1.99 Gbps | K=1 +7.0% |
| ×16 | 2.38 Gbps | **2.50** Gbps | K=4 +5.0% |

该轮绝对吞吐整体低于 §3.3(2.0~2.5 vs 2.6~3.0 Gbps),因窗口路径已封顶。
**跨轮不可直接比绝对值,只能比同轮内的相对关系。** 方向一致:K=1 低流数占优,K=4 高流数占优。

### 3.5 数据怎么读

- **×1/×4**:K=1 更优。8 核里只有 1~2 核在忙,K>1 纯属多付开销。
- **×16**:K=4 更优。单核软中断打满,多载体把负载摊开。
- **×4↓ 对 frp 输 26%**:已知短板,尚未定位(怀疑与池化把小流分散到多载体有关)。

---

## 4. 当前的核心矛盾

### 4.1 矛盾

```
最优 K = f(并发流数)          ← 实测结论
但连接建立时,流还不存在         ← 时序事实
所以自动逻辑只能用"协议"这个代理信号  ← 当前实现
```

### 4.2 为什么"运行中扩容"解决不了问题

限制:**存量流不会迁移**。

```
t=0    16 条流建立在载体 0(K=1)
t=5s   检测到 16 条并发 → 补拨 3 条 → K=4
t=5s+  只有新开的流会分到新载体
       已建立的 16 条仍在载体 0
```

对"16 条流同时建立、持续跑 10 秒"的压测模型,补拨发生在流建立之后 → **收益几乎为零**。

只对"持续新建流"的场景有效(HTTP 短连接、API 网关)。

### 4.3 为什么"学习并发历史"不好

1. 冷启动无历史 → 首次连接必须猜,而首次最需要准
2. 滞后一个窗口 → 形态变化后要等统计周期
3. 正反馈风险 → K 小导致吞吐低,可能被统计成"并发不高",越学越小

### 4.4 所以真正要解决的是

**在会话建立之前(Hello/HelloAck 往返中)就把 K 算准。**
需要的输入只有三个:规则需求、客户端硬件能力、服务端预算。

---

## 5. 目标方案:服务端全局预算 + 客户端能力上报

### 5.1 设计原则

1. **客户端上报能力,服务端做决策**。服务端不知道客户端有几核,客户端不知道全局负载。
2. **零人工调优**:不要求运维给每台机器配 `RH_MUX_POOL_SIZE`。
3. **多客户端复用同一批核**:预算全局共享,按需分配,空闲让出。
4. **不做运行中增减载体**(§4.2 已论证无效)。
5. **不做统计学习**(§4.3 已论证不可靠)。

### 5.2 协议改动

**客户端 → 服务端**(`Hello` 新增字段):

```rust
/// 客户端可承受的物理载体上限(由 CPU 核数与网卡 RX 队列折算)。
/// 服务端决策时取 min(规则需求, 该值, 服务端全局预算, max_carriers)。
#[serde(default)]
pub carriers_cap: usize,
```

**服务端 → 客户端**(`HelloAck` 新增字段):

```rust
/// 服务端建议的载体数(已按规则需求、客户端能力、全局预算钳制)。
/// 0 / 缺省 = 客户端按本地协议基线自行决定(兼容旧服务端)。
#[serde(default, skip_serializing_if = "is_zero")]
pub recommended_carriers: usize,
```

两个字段都用 `#[serde(default)]`,旧版本互通时退化为现状行为。

### 5.3 服务端决策算法

```rust
// 每客户端需求(已有字段,无需新增配置)
fn client_demand(c: &Client) -> usize {
    c.enabled_tcp_proxies().map(|p| p.pool_size as usize).max().unwrap_or(1)
}

// 全局预算(唯一需要标定的参数)
let total_budget = max(1, cpus / COST_PER_CARRIER);

// 分配(会话建立时执行)
let mut budget_left = total_budget.saturating_sub(已分配);
for c in clients_sorted_by_activity_desc {
    let demand = client_demand(c);
    let cap = min(c.carriers_cap, HARD_MAX_PER_CLIENT /* 8 */);
    let k = clamp(min(demand, cap, budget_left.max(1)), 1, cap);
    budget_left = budget_left.saturating_sub(k);
    ack.recommended_carriers = k;
}
```

三条约束:

- 单客户端:`k <= min(carriers_cap, max_carriers)`(后者默认 8)
- 全局:`Σ(k - 1) <= total_budget`(首载体不计入预算,保证每个客户端至少 1)
- 下限:`k >= 1`

### 5.4 多客户端的"复用核心"怎么实现

关键:**不是按客户端数均分,而是按当前谁在用动态分配**。

```
活跃集合 = 最近 N 秒内 bytes 增长 > 0 的客户端
空闲客户端 → k = 1,不占预算
活跃客户端按 demand 降序 → 依次分配剩余预算
```

服务端已有 `ClientUsage { conns, bytes, ... }`(`crates/rh-server/src/state.rs:149-160`),
可直接作为活跃度依据,**无需新增采集**。

效果:

| 场景 | 预算(total=4) | 结果 |
|---|---|---|
| 1 个客户端在跑 16 流 | 4 | 该客户端 K=4 |
| 4 个客户端各跑 16 流 | 4 | 每人 K=1(物理上就该这样) |
| 10 个客户端,2 个活跃 | 4 | 活跃的各 K=2~4,其余 K=1 |
| 100 个客户端,5 个活跃 | 4 | 5 个活跃的分 4 条预算 |

### 5.5 时序

```
1. 客户端启动 → 探测 hardware_cap → Hello.carriers_cap
2. 服务端认证通过 → 查该客户端规则 → 算 demand
3. 服务端按全局预算 + 活跃度排序 → 得 k
4. HelloAck.recommended_carriers = k
5. 客户端 k = clamp(recommended, 1, hardware_cap) → 一次拨到位
6. 会话结束 → 归还预算
```

**一次到位,不需要 Round-10 那套"先拨再收敛"的链路**
(`set_active_prefix` + `detach_carrier` + 终态校验)。那些代码可以保留作为兜底
(服务端 cap 在运行中变化时仍可能用上),但主路径不再依赖。

### 5.6 需要新增的服务端状态

```rust
struct CarrierBudget {
    total: usize,
    /// client_id → 已分配载体数(不含首载体或含?需明确,建议含)
    allocated: DashMap<String, usize>,
}
```

生命周期:会话建立时申请,会话断开时归还。进程重启后全空(无害)。

---

## 6. 必须先做的事:标定 `COST_PER_CARRIER`

### 6.1 为什么

`cpus/2` 是估计值(假设每载体 1.5~2 核),从未实测。整个预算公式建立在这个数上:

- 如果实际每载体 0.5 核,`cpus/2` 过于保守,4 核机器本该开 4 却只开 2 → 白丢性能
- 如果实际每载体 3 核,8 核开 4 会过载

### 6.2 实验设计

在 8 核机器上(SAT 环境与真实部署一致):

```
for K in 1 2 4 8:
    客户端设 RH_MUX_POOL_SIZE=K
    跑 ×16 下行 iperf3 30 秒
    同时采样 rh-client 进程 CPU(ps cputime 差分或 /proc/PID/stat)
    记录:吞吐(Mbps)、CPU 核数、进程线程数
    重复 3 次取中位
```

同时观察:

- 吞吐拐点在哪(K 增大到什么程度不再涨)
- CPU 是否线性增长(线性 = 每载体成本固定;次线性 = 存在共享瓶颈)
- 软中断分布(`/proc/interrupts` 或 `mpstat -P ALL`)

### 6.3 产出

`COST_PER_CARRIER` = 实测 CPU 核数 / K(在吞吐拐点处取值)。
例如:8 核跑 K=4 时进程占 6 核 → `COST_PER_CARRIER = 1.5` → `total_budget = cpus / 1.5`。

### 6.4 没有标定前不要实施 §5

预算公式里的 `cpus / COST_PER_CARRIER` 是唯一决定"总共能开几条"的数。
用估计值上线,会导致**要么资源闲置、要么服务端过载**,两种都是生产事故。

---

## 7. 实施路线

| 阶段 | 内容 | 依赖 | 产出 |
|---|---|---|---|
| P0 | 标定 `COST_PER_CARRIER`(§6) | 8 核机器 | 实测系数 + 拐点报告 |
| P1 | 协议加两个字段 + 服务端决策 + 客户端按建议拨池 | P0 | 单客户端场景:服务端直接给对 K |
| P2 | 全局预算 + 活跃度排序 | P1 | 多客户端自动分配 |
| P3 | (可选)按规则粒度分配 | P2 | 混合规则场景优化 |

P1 完成即可消除 `RH_MUX_POOL_SIZE` 手工配置;P2 才解决 100 客户端问题。

---

## 8. 验收标准

### 8.1 P1 验收

- 单客户端 tcp,规则 `pool_size=16` → 服务端建议 K=4,客户端实际开 4 条
- 单客户端 tcp,规则 `pool_size=1` → 建议 K=1,实际 1 条
- 4 核客户端 + 服务端建议 4 → 实际 K=2(硬件钳制生效),日志记录钳制原因
- 不设任何环境变量,上述行为自动发生

### 8.2 P2 验收

- 4 个客户端同时跑 ×16,全局预算 4 → 每个 K=1,总连接数 4
- 4 个客户端中只有 1 个跑流量 → 该客户端 K=4,其余 K=1
- 100 个客户端(可用脚本模拟),5 个活跃 → 服务端总载体数不超预算,无过载

### 8.3 性能验收

- 单客户端 ×16:达到 §3.3 中 K=4 的水平(2953 Mbps 量级)
- 单客户端 ×1:达到 §3.3 中 K=1 的水平(2008 Mbps 量级),不得因误判退化
- 全局:服务端 CPU 不超 `total_budget * COST_PER_CARRIER`

---

## 9. 明确的非目标

1. **不做运行中动态增减载体**(§4.2:存量流不迁移,无效)
2. **不做并发历史学习**(§4.3:冷启动/滞后/正反馈)
3. **不做按流粒度分配载体**(架构改动过大,单主规则场景收益为零)
4. **不改变现有池的轮转/attach/shutdown 语义**

---

## 10. 关键源码索引

| 主题 | 位置 |
|---|---|
| 协议基线 | `crates/rh-transport/src/lib.rs:314-319` |
| 硬件探测 | `crates/rh-client/src/config.rs:188-194` |
| 定容公式 | `crates/rh-client/src/session.rs:93-101` |
| 报到应答 | `crates/rh-protocol/src/message.rs` `CarrierAttachAck` |
| 服务端 cap 检查 | `crates/rh-server/src/control.rs` `attach_additional_carrier` |
| 池本体 | `crates/rh-transport/src/mux.rs` `YamuxMultiplexerPool` |
| 轮转分派 | 同上 `open_stream` / `open_stream_tagged` |
| 前缀收缩 | 同上 `set_active_prefix` / `detach_carrier_by_id` |
| 客户端使用量 | `crates/rh-server/src/state.rs:149-160` `ClientUsage` |
| 规则池深字段 | `crates/rh-protocol/src/proxy.rs:386-388` `pool_size` |

---

## 11. 已知遗留问题(与本方案相关)

1. **`×4↓` 对 frp 输 26%**(§3.3),原因未定位。可能是池化把小流分散到多载体导致批聚合退化。
   若 P1 上线后该档仍输,需要单独排查。
2. **`YamuxMultiplexer` 无 `Drop`**:`try_join_all` 部分失败时 driver 滞留(既有问题)。
3. **服务端 cap 检查与聚合非原子**:并发报到可能短暂多 1 条(已披露)。
4. **e2e 的 CPU 期望在嵌套 cgroup 下可能与 `available_parallelism` 不一致**(只影响测试期望,不误 PASS)。

---

## 12. 一句话交给实施方

**先花 20 分钟标定 `COST_PER_CARRIER`(8 核跑 K=1/2/4/8 记吞吐与 CPU),再用
"客户端上报 `carriers_cap` + 服务端按规则需求与全局预算决策 + `HelloAck` 下发
`recommended_carriers`"替代当前的协议基线猜测。不要做运行中扩容,不要做学习。**
