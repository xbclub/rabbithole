# 深审修复 + TCP/TLS 对 frp 吞吐评估(2026-10-20)

基线:`ffd49d3` → 本轮终点 `bc26e68`(分支 `bench/tls-alpn`)。
测量工具:**Go loadgen**(`benchmark/loadgen`,仓库自带),三机 peer 拓扑,
`peer.sh` / `peer-frp.sh` 同脚本同场景。5 次配对取中位,每档 15s。

## 0. 结论

| 协议 | 单流 | 4 流 | 16 流 |
|---|---|---|---|
| TCP 上行 | **1.93×** | 1.00× | **1.06×** |
| TCP 下行 | **1.97×** | 0.92× | **1.02×** |
| TLS 上行 | **1.60×** | **1.07×** | **1.02×** |
| TLS 下行 | **1.66×** | **1.05×** | **1.01×** |

- **TLS 8/8 档全部超过 frp**(1.01×~1.66×)。
- **TCP 6 档中 5 档 ≥1.00×**,唯一低于 frp 的是「4 流下行 0.92×」,
  绝对差 203 Mbps;该项 frp 样本跨度 2080~2449(±18%),rh 为
  2200~2323(±5%),属测量噪声量级,但**本报告不把它记为达标**。
- 与 v0.3.2 基线相比,单流隧道从 890~971 Mbps 提到 2099~2141(TCP)、
  1746~1783(TLS);16 流从 1398/881 提到 2317/2408。

## 1. 三机环境

| 角色 | 主机 | 地址 | 配置 |
|---|---|---|---|
| 服务端 | test1 | 172.16.1.6 | 8 核 / 7 GiB / 10GbE |
| 客户端+目标 | test2 | 172.16.1.17 | 同上 |
| 压测端 | test3 | 172.16.1.18 | 同上 |

RTT ≈ 0.5 ms(同机房)。直连基线 1700~2790 Mbps(随档位与波动)。

证据目录:`benchmark/runs/deep-review-k2/`(rh/frp × tcp/tls 的 results jsonl)。

## 2. 完整数据(Mbps,中位)

### TCP

| 方向 | 流数 | 直连 | rh | frp | rh/frp |
|---|---:|---:|---:|---:|---:|
| 上行 | 1 | 1707 | 2099 | 1086 | 1.933 |
| 上行 | 4 | 2535 | 2200 | 2192 | 1.004 |
| 上行 | 16 | 2782 | 2326 | 2200 | 1.057 |
| 下行 | 1 | 1728 | 2141 | 1089 | 1.966 |
| 下行 | 4 | 2442 | 2232 | 2435 | 0.917 |
| 下行 | 16 | 2780 | 2317 | 2274 | 1.019 |

### TLS

| 方向 | 流数 | 直连 | rh | frp | rh/frp |
|---|---:|---:|---:|---:|---:|
| 上行 | 1 | 1707 | 1746 | 1094 | 1.596 |
| 上行 | 4 | 2418 | 2231 | 2079 | 1.073 |
| 上行 | 16 | 2715 | 2464 | 2426 | 1.016 |
| 下行 | 1 | 1716 | 1783 | 1075 | 1.659 |
| 下行 | 4 | 2407 | 2274 | 2164 | 1.051 |
| 下行 | 16 | 2759 | 2408 | 2380 | 1.012 |

## 3. 本轮改了什么(只列影响吞吐的)

### 3.1 默认载体数 1 → 2(`default_carriers`)

**定位过程**:单载体隧道封顶 2.1-2.2 G,且**与流数无关**(1/4/16 流同值),
而直连可达 2.6-2.9 G。服务端 CPU 均值仅 0.26 核(峰值 1.78),排除 CPU。
机理是单条 TCP 流的拥塞窗 + yamux **单 FlushTask 串行写**。

**A/B(旧二进制,仅验证方向)**:K=1 4 流 2072 / 16 流 2178;
K=2 2213 / 2306;K=4 2184 / 2323(与 K=2 持平,只增成本)→ 取 K=2。

**关键缺陷修正**:上一版只改了 `default_carriers` 却没生效——
`initial_k_for_async` 在 `mux_pool_size=None` 时直接取 RTT 探测结果,
低 RTT 分支返回 1,把协议基线完全绕过。改为
`max(RTT 建议, 协议基线)`,RTT 仍可在高 BDP 下继续上调。

QUIC/KCP 保持 K=1:自带多路复用;KCP 的端到端背压语义依赖单通道
(`b08_public_mux_stalled_reader_backpressure_and_recovery` 锁定)。

### 3.2 yamux 缓冲池加大缓冲类(C4)

上行读块默认 256KiB,超出原池上限(64KiB+8KiB),每块都新分配。
新增 `LARGE_MAX = 1MiB+8KiB`、总预算 32MiB 的大类,主力上行命中池。

### 3.3 下行预取改增量计数(C5)

修前每次判断都在 `pending` 队列上 `sum()` 一遍;小帧 + 写阻塞时队列可达
百万级段数,每轮 O(n) 变成整体 O(n²)。改为增量维护字节数 + 段数上限。

### 3.4 已验证无效的方向(如实记录)

- **抬高发送窗**(`RH_MUX_SEND_WINDOW=6MiB`):4/16 流无改善(2020/2196
  vs 默认 2072/2178)。且 512KiB 初窗即让 KCP 背压回归 `b08` 失败
  (8MB 停读场景写端不再停顿),故保持 vendor 默认 256KiB,仅保留显式开关。
- **K=4 载体**:与 K=2 持平,只增加每条连接的爬窗成本。
- **抬高下行读块**(`CHUNK_CAP` 64KiB 红线):会推迟 yamux 窗口更新回程,
  历史上已有 -4% 的确凿回退证据,未动。

## 4. 复现方式

```bash
# 三机各自
cd /tmp/rh-bench && git fetch -q origin bench/tls-alpn && git reset -q --hard origin/bench/tls-alpn
# test1 构建
sed -i 's/^strip = true/strip = false/' Cargo.toml
~/.cargo/bin/cargo build --release -p rh-server -p rh-client
rm -f /tmp/rh-bin/rh-server /tmp/rh-bin/rh-client
cp target/release/rh-server target/release/rh-client /tmp/rh-bin/

# 本地编排(TCP 与 TLS 各一轮 rh + frp)
DURATION=15 REPEAT=5 CONC_LEVELS="64 256 1024" CONC_HOLD=5 \
  bash benchmark/run-pair.sh final tcp tls
```

## 5. 局限

1. **单环境单轮**。直连基线本身波动 ±15%,4 流下行那 0.92× 落在噪声带内,
   但本轮没有更多重复来把置信区间收窄。
2. **低 RTT(0.5ms)**。高 BDP(跨地域)场景下载体数、窗口的结论可能不同,
   `carriers_for_rtt` 的 RTT 自适应仍保留并在高 RTT 下上调 K。
3. **未覆盖 ws/wss/quic/kcp** 的本轮对照(沿用历史数据)。
4. 4 流下行若要在结构上确认,应在干净环境用 `RH_PUMP_STATS=1` +
   `RH_SPLICE_STATS=1` 采集读/写耗时分解,本轮未做。

## 6. 正确性与回归

- `cargo test --workspace` 全绿;
- `cargo clippy --workspace --all-targets -- -D warnings` 全绿;
- `cargo fmt --all` 已执行;
- `tests/e2e.sh` **82 PASS / 0 FAIL**,含本轮新增/更新的:
  - `T-E2: tcp 默认定容 initial_k=final_k=2`
  - `T-E2: tcp 默认 K=2,客户端控制口物理连接数 = 2`
  - `XTCP 打洞直连`(A7 加认证后仍通)

## 7. A7 的行为变化(发布必列)

XTCP 直连首帧现在必须携带 `HMAC(目标规则 secret_key, nonce)`:

- 协议新增可选字段 `NatHolePrepare.nonce`、`NatHoleConnect.{nonce,auth}`、
  `StartForward.xtcp_auth`;
- **旧端缺字段 = 拒绝直连**(不静默降级),新旧混连时 XTCP 直连会失败并
  回退中继(session 层已有 fallback);
- 打洞 listen/dial 只接受服务端下发的候选来源;
- 规则未配置 secret 时 `auth = nonce`(能力令牌语义,与"空 secret 不校验"
  的既有语义一致)。
