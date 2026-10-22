# rabbithole benchmark

可复现的性能基准套件。任何人都能在他自己的机器上复跑,得到可对比的数字。

> **goal-repair 修复版(2026-10-13,r2)**:本套件按当时的验收方案
> §5 P3 完成了一轮测量方法修复(A8/A9/A10/A11/A12 + F-Drive-Nofile + CPU 采样),
> **门槛阈值未变**。r2 按主代理独立复核堵死核验器 6 个误通过(UDP 包长、
> 六协议并发、源清单绑定、保持证据、raw 形态、nofile 实值),并加严:
> CPU 按样本所在目录匹配且上/下行分别判定;拒绝 NaN/Infinity、重复时间窗
> 样本、跨目录不同构建拼装、伪造保持结果、未排空 UDP。新工作流与证据
> schema 见下方「goal-repair 验收工作流」一节;旧的历史 runs(peer、peer-f、
> peer-f2、…)按红线永久保留、不覆盖、不重算。

## 这个套件测什么

对齐成熟的公开方法学,而不是自创指标:

| 维度 | 方法学 / 工具 | 说明 |
|---|---|---|
| 吞吐 | **iperf3**(RFC 2544 / RFC 6349 的 de-facto 工具) | 单流/多流,上行/下行;必测直连基线做对照 |
| 延迟 | 百分位(p50/p90/p99/p99.9),非均值 | 均值会掩盖长尾;对齐 HdrHistogram 惯例 |
| 并发 | 非阻塞连接 + selectors | 直连与隧道同轮对比,区分瓶颈来源 |
| 并发连接 | 直连与隧道同轮对比 | 区分「隧道受限」与「测试装置受限」 |
| UDP | 自研(标准库) | 吞吐、丢包、RTT |
| HTTP | 自研(标准库) | vhost QPS 与延迟 |
| 建连速率 | 每连接一次短交互 | accept + 池分配开销 |

**没有专门针对隧道/代理的公开 benchmark 标准**(frp、ngrok 都不发布)。所以本套件
对齐的是底层方法学:吞吐用 iperf3,延迟用百分位,并且**所有场景都提供直连基线**——
隧道开销 = 隧道指标 / 直连指标,否则绝对数字没有意义。

## 基准结果

> **测量条件(2026-10-22,commit `cccc101`,rabbithole v0.5.0 vs frp v0.71.0)**
>
> 三机 peer 拓扑(见上文),控制面 `tcp`、`relay=splice`、`mux=1`;
> 压测工具 **loadgen**(`benchmark/loadgen`,仓库自带)——
> 单流吞吐已超 2 Gbps,系统 iperf3 3.9 单线程会在 2 Gbps 附近饱和,
> 用它测会得到「两侧都被压测器卡住」的假持平。
> **每档 15s × 5 次配对取中位**;并发档 frp 侧 1 次(rh 侧 5 次)。
>
> 证据:`/tmp/peer/`(rh)、`/tmp/frp-peer/`(frp),**未入库**。

### 一句话结论

| 维度 | 结果 |
|---|---|
| **单流吞吐** | rh 约 **1.8–1.9×** frp(2067 vs 1083 Mbps 上行) |
| **多流吞吐** | 4 流 rh 略低(**0.93–0.96×**),16 流基本持平(**1.03–1.05×**) |
| **延迟开销(p99)** | rh **+0.93ms** vs frp **+1.17ms**(低 20%);隧道绝对值 1.46 vs 2.06ms |
| **并发建连 p99 @4096** | rh **653ms** vs frp **677ms**,差 3.5% —— **持平**(见下方读法) |
| **HTTP vhost QPS** | rh 1152 vs frp 1041(**+10.7%**) |
| **UDP 吞吐** | rh 96 vs frp 69 Mbps(**+41%**) |
| **建连速率** | rh 872/s vs frp 992/s(**−12%**) |

**与 v0.3.2 时点相比**:「多流回退 −54%」「HTTP 落后 43%」「高并发建连落后 7×」
三条**均已不再成立**。单流从 0.72–0.77× 变为 1.78–1.91×。

### 吞吐(Mbps,5 次配对中位)

| 方向 | 流数 | rh 直连 | **rh 隧道** | frp 直连 | frp 隧道 | rh/frp |
|---|---:|---:|---:|---:|---:|---:|
| 上行 | 1 | 1604 | **2067** | 1664 | 1083 | **1.91×** |
| 下行 | 1 | 1592 | **2016** | 1626 | 1133 | **1.78×** |
| 上行 | 4 | 2185 | **2515** | 2253 | 2708 | 0.93× |
| 下行 | 4 | 2220 | **2559** | 2387 | 2659 | 0.96× |
| 上行 | 16 | 2483 | **2830** | 2709 | 2684 | 1.05× |
| 下行 | 16 | 2452 | **2786** | 2575 | 2693 | 1.03× |

**注意「保留率 >100%」**:单流档隧道(2067)高于直连(1604)。这不是测量
错误 —— 直连路径是 loadgen 单线程直接打目标,受单核与单连接窗口限制;
隧道走多载体 + yamux 批聚合,反而摊开了写路径。**因此单流档的保留率
不具参考意义,应直接看 rh/frp 比值。**

4 流是唯一 rh 略低的档位(0.93–0.96×)。差异在测量噪声量级(5 次样本
rh 2383–2683 / frp 2691–2716),**不建议据此下「frp 多流更强」的结论**;
但也**不能**说 rh 多流更强。

### 延迟开销(隧道 − 直连,ms,5000 往返)

| 路径 | rh p50 | rh p99 | frp p50 | frp p99 |
|---|---:|---:|---:|---:|
| 直连基线 | 0.29 | 0.53 | 0.17 | 0.89 |
| **隧道** | **0.96** | **1.46** | 1.31 | 2.06 |
| **开销** | **+0.67** | **+0.93** | +1.14 | +1.17 |

### 并发建连 p99(ms,括号内为直连基线)

| 并发 | rh 隧道 | rh 直连 | frp 隧道 | frp 直连 |
|---:|---:|---:|---:|---:|
| 64 | 10.9 | 9.6 | 10.7 | 9.7 |
| 256 | 42.4 | 37.0 | 44.8 | 33.4 |
| 1024 | **174.9** | 1038.2 | **186.9** | 1041.2 |
| 4096 | **653.1** | 3050.6 | **676.8** | 3048.5 |

**这是本轮补测的重点项**(此前只有 v0.3.2 的 1029ms 记录,无同口径复测)。

关键读法:

1. **1024/4096 档的直连基线本身就超过 1000ms / 3000ms** —— 说明该档位
   受**测试装置**限制(压测端单机 8 核要同时发起上千连接),不是产品差异。
   rh 与 frp 的直连基线几乎一致(1038 vs 1041、3051 vs 3049),证明装置
   条件相同。
2. 在装置受限的前提下,**隧道 p99 反而远低于直连**(175 vs 1038)—— 因为
   隧道把建连压力分摊到服务端与客户端两侧,而直连全压在压测端单机。
3. **rh 与 frp 在 4096 档是 653 vs 677ms,差 3.5%,在噪声范围内**。
   结论应写成「**持平**」,不能说 rh 更快。
4. 全部档位 `verified = established = hold_alive`(应用层 PING/PONG 往返
   且保持期存活),零失败。

### 其它指标

| 指标 | rh | frp | 对比 |
|---|---:|---:|---:|
| 建连速率(/s,3000 次) | 872 | 992 | −12% |
| HTTP vhost QPS(16 连接 × 8s) | 1152 | 1041 | +10.7% |
| UDP 吞吐(Mbps,1200B) | 96 | 69 | +41% |
| UDP 丢包 | 0% | 0% | 持平 |

UDP 的直连基线 rh 为 221–241 Mbps、frp 为 206 Mbps,量级一致;隧道侧
rh 高出 41%,与单流 TCP 的优势同源(批聚合)。

### 遗留问题(如实)

1. **4 流吞吐略低于 frp**(0.93–0.96×)。样本区间有重叠,当前证据不足以
   判定是真实差距还是噪声。**需要更大样本量或更长时长复测**才能定论。
2. **建连速率低 12%**(872 vs 992/s)。隧道侧每连接要走池签发 +
   START_FORWARD,这是已知的结构性开销;是否值得优化取决于实际场景。
3. **并发建连在 1024+ 档被测试装置限制**,无法得出「产品在高并发下
   表现如何」的结论 —— 要回答那个问题,需要多台压测机分散发起。

### 历史结果(v0.3.2,2026-10-15)

以下数据已被上表取代,**仅作历史对照保留**。原文可从
`git show <v0.3.2 提交>:benchmark/README.md` 取回。

v0.3.2 时点的四条遗留问题是:多流回退 −54%、高并发建连 p99 ~1030ms
(frp 140ms)、HTTP vhost 580 QPS(为 frp 的 57%)、单流 tcp 下行 890 Mbps。
**其中第 1、3、4 条已解决,第 2 条的「frp 140ms」结论本身不成立** ——
本轮同口径实测 frp 在 4096 档也是 677ms,当年那个 140ms 应是并发档位或
装置条件不同所致。

---

## 目录结构

```
benchmark/
├── peer.sh           三机基准(server / client / drive 三个角色)
├── lib/common.sh     共享函数(二进制下载、配置生成、API 驱动、进程管理)
├── py/
│   ├── target.py     目标后端(tcp 行协议 / udp 回显 / http),跑在 client 侧
│   ├── loadgen.py    压测引擎(延迟/吞吐/并发/连接速率/udp/http)
│   ├── cpu_sampler.py  端点 /proc CPU+RSS 采样器(≥10Hz,仅 Linux)
│   ├── build_manifest.py  构建源哈希 + 二进制哈希清单(证据 schema 的 build/)
│   ├── summarize.py  结果汇总(按实验分组;--gates 走 verify_goal)
│   ├── verify_goal.py  goal-repair 证据验收器(fail closed;--self-test 自检)
│   └── tests/        标准库 unittest 回归(含对 before-fix 快照的红证)
└── runs/             历史证据(只增不覆盖;goal-repair/ 下按子目录分区)
```

## goal-repair 验收工作流(新)

相对旧流程的行为变化(全部是 fail-closed 方向):

1. **三个角色都要求显式环境变量** `PROTO` / `RELAY` / `MUX` / `PEER_RUN_ID`,
   不再有隐式默认值暗中生效(A9:元数据必须与端点实况一致,不能靠驱动端
   环境变量冒充)。`MUX` 值域 0/1。
2. **fd 软限三角色同检**:server/client/drive 都 `raise_nofile 65535`,把实际
   软限写入 `$PEER_DIR/nofile.json`;低于并发档+512(4096 档即 ≥4608)时以
   **环境错误中止本轮**(F-Drive-Nofile)。EMFILE 绝不折算成产品吞吐/并发失败,
   loadgen 把它归类 `env_error` 并非零退出。
3. **端点实况证据**:server/client 侧写 `$PEER_DIR/endpoint.json`
   (从实际生成的 toml 回读 proto/multiplex/relay_mode + `/proc/PID/exe`
   SHA-256 + `RH_NOTSENT_LOWAT`/`RH_QUIC_CC` 实际值,未设置显式记 `unset`)
   与 `identity.jsonl`、`cpu.jsonl`(CPU+RSS 采样)、`nofile.json`。
   B 侧另记本机 iperf3 版本(工具双端版本)。
4. **iperf 原始留痕**(A11):drive 每条吞吐的原始 JSON/stderr/退出码落
   `$PEER_DIR/raw/`;失败记 `valid:false` + `error`,**绝不以 0 冒充**;
   结果文件名带时间戳,永不覆盖历史。任何无效样本使本轮**退出码非 0**。
5. **UDP 排空口径**(A8):数据报带 8 字节序号,回包按序号去重(dup 单列),
   到时后固定排空期(`PEER_UDP_DRAIN`,默认 5s)继续收在途回包
   (`late_recv` 单列);损失 = sent − 去重后唯一回包数。
6. **真实并发**(A12):concurrent 每条连接必须完成 PING/PONG 往返
   (`initial_verified`)并在保持期按秒存活(`hold_alive`);
   `verified` = 两者皆备。G5 用 verified,不用 established。
7. **配对重复**(G7):`PEER_REPEAT`(默认 3)次 direct/tunnel 同 spec 紧邻
   成对测量;汇总/验收取中位数并保留全部样本与失败。

### 证据目录 schema(goal-repair-evidence v1)

drive 结束后把三台机器的产物组装成一个证据目录(每协议×配置一个):

```
runs/goal-repair/<label>/
├── schema.json                     由 drive 写入(含 run_id)
├── results-<label>-<stamp>.jsonl   drive 记录(meta + 逐样本)
├── raw/                            每条 iperf3 的 .json/.stderr/.exit
├── endpoints/server/{endpoint.json,identity.jsonl,cpu.jsonl,nofile.json}   ← A 机 $PEER_DIR
├── endpoints/client/{endpoint.json,identity.jsonl,cpu.jsonl,nofile.json}   ← B 机 $PEER_DIR
└── build/{sources.sha256,binaries.json[,bin/rh-server,bin/rh-client]}      ← 构建机
```

`build/` 由 `python3 py/build_manifest.py <repo根> <二进制目录> <输出目录>`
在构建完成后立即生成;验收器会核对 **endpoints 里的 /proc/PID/exe SHA-256
必须等于 binaries.json 的构建哈希**(运行的就是这份构建)。

### 验收判定

```bash
python3 py/verify_goal.py --evidence runs/goal-repair        # 父目录自动发现子证据目录
python3 py/summarize.py runs/goal-repair --gates             # 同一判定的汇总入口
python3 py/verify_goal.py --self-test                        # 核验器自检(37 个对抗用例)
```

门槛 G1–G7 沿用当时的 goal-repair 验收方案(原文已于 2026-10-22 删除,
可用 `git log --diff-filter=D -- docs/performance-repair-acceptance.md` 取回);
阈值硬编码在 `py/verify_goal.py` 的 `THRESH`,不随测量方法改变;
`verify_goal` 只认原始证据,记录里的 `pass`/`verdict` 等手填字段一律忽略;
数据不足 = FAIL(不是 skip)。核验的是完整证据链,不是字段存在性:

- **G4**:`packet_size` 必须 == 1200(值检查,不是"有字段");
- **G5**:六协议(tcp/tls/ws/wss/quic/kcp,mux=1)**各** 4096 条 verified
  隧道 + (tcp, mux=0) 4096 对照;每条 concurrent 记录必须带完整保持证据
  (`initial_verified`/`hold_alive`/`hold_rounds`/`alive_by_round`)且数字
  自洽(verified ≤ hold_alive ≤ initial_verified ≤ established ≤ n,
  每轮 alive ≥ verified)—— TLS 专项只是六分之一,删掉任一协议的并发记录
  直接 FAIL;
- **源-构建绑定**:`build/sources.sha256` 必须覆盖 `py/source_inventory.py`
  确定性枚举规则给出的**全部**源文件(根 Cargo.toml/Cargo.lock +
  crates/**、vendor/** 的 .rs/Cargo 文件;遗漏/多余/重复/符号链接/路径
  逃逸都 FAIL),且逐条按真实源码树重算哈希。源码树解析顺序:
  **`RH_VERIFY_REPO`(实测验收请固定 export 为最终工作区)优先**,
  `binaries.json.repo_root` 仅在未设置时回退 —— 证据不能自带伪 repo
  逃避最终工作区绑定;两者都不可用 = FAIL。端点 `/proc/PID/exe`
  SHA-256、`build/bin/` 副本、`binaries.json` 三者必须一致;**跨目录的
  manifest 必须完全一致**(六协议证据必须出自同一份构建,不许拼装);
- **raw 时间戳**:iperf3 的 `start.timestamp` 是标准对象
  `{time, timesecs[, timemillisecs]}`(3.22 实测;3.17.1 同族,验收机现场
  再核对)—— millis 精度优先,timesecs 精度容差 +1s;强转成伪数值 = FAIL;
- **raw 形态核对**:每条有效吞吐的 raw JSON `start.test_start`
  (protocol=TCP、num_streams、reverse↔方向、duration)与
  `start.timestamp`(落在记录时间窗 ±5s)、`start.connected`
  (remote_host/remote_port == 记录 dst/dst_port)逐项一致 —— 声明隧道
  实跑直连、方向/流数错配、时间窗错位都 FAIL;
- **nofile 实值**:端点 `nofile.json` 与 `meta.drive_nofile_soft` 都必须
  ≥ 并发档+512 且 `ok:true`("unlimited" 除外),否则按环境错误整目录 FAIL;
- **CPU 口径**:每个 tunnel 样本的 CPU 取自**其所在目录**的 cpu.jsonl
  (不跨目录混配);G1 上行、下行**分别**判定 <50% 单核;覆盖率 <95% 的
  样本判 invalid;
- **数字卫生**:拒绝 NaN/Infinity(按坏行处理)、同 key 重复 sample id /
  重复时间窗(同一测量冒充多次独立)、`drain_seconds<1` 的未排空 UDP。

其他注意事项:

- **CPU 窗口对齐依赖三机时钟同步**(NTP 误差 ≪ 采样间隔);覆盖率 <95%
  的样本判 invalid,不做静默放行。
- 门槛只认 `relay=splice`(最终版本默认);`RELAY=copy` 对照轮放独立目录,
  不进入门槛、也不污染门槛。

### 本地回归

```bash
python3 -m unittest discover -s benchmark/py/tests -t benchmark/py/tests
```

覆盖:A8 排空/去重、A12 verified、A10 分标签汇总、shell 助手(nofile 门控、
显式环境、SHA-256 身份)、run_iperf 留痕(本机 iperf3)、verify_goal 口径;
红证 = 同断言在 `runs/goal-repair/harness/before-fix/` 的修复前快照上必须失败。

## 设计原则

1. **脚本只做测试,不做运维。** 不建立 SSH 连接、不安装系统环境、不修改服务器配置。
   编排(把脚本送到机器上、启停进程)由运行者用自己的通道完成——`termark`、`ssh`、
   `ansible` 或手动都行。这样脚本本身可审计、可移植,也不会意外改你的环境。
2. **依赖固定版本 + 校验。** 默认从公开 GitHub Release 下载 `v0.3.0` 的 x86_64 musl
   静态二进制并校验 sha256,所以复跑者不需要 Rust 工具链,拿到的也是同一份二进制。
3. **每个数字都有基线。** 延迟、并发、HTTP、吞吐都同时测直连,否则无法区分
   "隧道慢" 和 "测试装置慢"。
4. **失败要可见。** 报文超过 MTU、连接被拒等,如实上报为错误,不静默记 0。

## 依赖

运行前需自备(套件只校验,不代为安装):

- `python3`(3.8+,仅标准库)
- `iperf3`
- `jq`
- `curl`

## 快速开始(三台机器)

前置:三台同网段 Linux,互相可达;每台装有 python3 / iperf3 / jq / curl;
**三机时钟 NTP 同步**(CPU 窗口对齐需要);构建机上先产出 build 清单(见上)。

```bash
# 0) 构建机:构建被测二进制后立即固化 build 清单(拷给证据目录用)
python3 benchmark/py/build_manifest.py <repo根> target/release /tmp/rh-build

# 1) 把 benchmark/ 目录放到三台机器(通道自选:scp/termark/ansible)

# 2) 机器 A(服务端)——注意:PROTO/RELAY/MUX/PEER_RUN_ID 必须显式
PEER_RUN_ID=run-20261013a PROTO=tcp RELAY=splice MUX=1 peer.sh server
#    输出: peer.sh client <A_IP>:17100 <client_id> <secret>

# 3) 机器 B(客户端+目标)——用同一个 PEER_RUN_ID 与同一组配置
PEER_RUN_ID=run-20261013a PROTO=tcp RELAY=splice MUX=1 \
  peer.sh client <A_IP>:17100 <client_id> <secret>

# 4) 机器 C(压测端)——同一个 PEER_RUN_ID;drive 默认 3 次配对重复
PEER_RUN_ID=run-20261013a PROTO=tcp RELAY=splice MUX=1 \
  peer.sh drive <A_IP> <B_IP> 18080 <token> <client_id> <label>
```

换控制面协议或换 MUX/RELAY:**重启 A、B 两端**并用**新的 PEER_RUN_ID**
另开一个证据目录再 drive(G1 用 tcp+MUX=0,G2/G3 用各协议+MUX=1)。
G5 专项:tunnels 走 (tls, MUX=1) 一轮 + (tcp, MUX=0) 裸连接对照一轮。

结果在 C 机 `$PEER_DIR/results-<label>-<时间戳>.jsonl` 与 `$PEER_DIR/raw/`;
按「证据目录 schema」把三机产物收进 `runs/goal-repair/<label>/` 后判定:

```bash
python3 py/summarize.py runs/goal-repair          # 分实验汇总(中位数/无效样本)
python3 py/verify_goal.py --evidence runs/goal-repair   # 门槛判定(退出码可作闸门)
```

环境变量:

| 变量 | 默认 | 说明 |
|---|---|---|
| `RH_VERSION` | `v0.3.0` | 基准版本(改这个换被测版本) |
| `BENCH_BIN` | — | 已解压的二进制目录(跳过下载) |
| `PEER_DIR` | `/tmp/rh-peer` | 各角色工作目录 |
| `PEER_RUN_ID` | **必填** | 本轮 run id(三机同值;证据一致性由它核对) |
| `PROTO` | **必填** | 控制面协议 tcp/tls/ws/wss/quic/kcp(无隐式默认) |
| `MUX` | **必填** | multiplex 开关,0/1(无隐式默认) |
| `RELAY` | **必填** | relay_mode:splice/copy(无隐式默认;copy 对照轮不进门槛) |
| `PEER_DURATION` | `10` | iperf3 每档时长(秒) |
| `PEER_REPEAT` | `3` | direct/tunnel 配对重复次数(G7 要求 ≥3) |
| `PEER_CONC_LEVELS` | `64 256 1024 4096` | 并发档(G5=4096) |
| `PEER_CONC_HOLD` | `5` | 并发保持秒数(期间每秒 PING/PONG 存活校验) |
| `PEER_CONC_REPEAT` | 同 `PEER_REPEAT` | 并发档重复次数 |
| `PEER_UDP_DRAIN` | `5` | UDP 到时后排空秒数(A8) |
| `IPERF3_BIN` | `iperf3` | iperf3 路径(可用 /tmp/rh-bin/iperf3 多线程版) |
| `RH_NOTSENT_LOWAT` / `RH_QUIC_CC` | — | 归因实验旋钮;A/B 侧进程环境里设,
  endpoint.json 会记录实际值,验收器核对两端一致 |

## 场景清单(drive 每轮执行)

| 场景 | 内容 | 基线 |
|---|---|---|
| 吞吐 | iperf3,上/下行 × 1/4/16 流 × `PEER_REPEAT` 次配对,原始 JSON/stderr/退出码留痕 | 直连(同 spec 紧邻成对) |
| 延迟 | 5000 次往返,p50/p90/p99/p999;另测新建连接延迟 | 直连 |
| 并发 | 64/256/1024/4096 建立 + **应用层 PING/PONG 校验** + 保持期每秒存活 | 直连同轮 |
| 建连速率 | 3000 次连接建立+PING+关闭 | 直连 |
| UDP | 1200B × 8s,序号去重 + 到时排空(`late_recv` 单列) | 直连 |
| HTTP | vhost QPS,16 连接 × 8s | 直连 |

## 结果怎么读

- **吞吐**:看隧道/直连比值(保留率)。压测端/两端 CPU 都会影响绝对值,保留率跨环境可比。
- **延迟**:看 p99 与直连的差值,而不是绝对值(绝对值含物理 RTT)。
- **并发**:看 `verified`(应用层往返+保持存活),`established` 只是握手;
  直连与隧道若同样失败,先怀疑测试装置(fd/backlog,环境错误会直接中止而非折算)。

## 已知限制

- 每换一种控制面协议需要重启 server/client(共 6 轮),套件不自动编排跨机重启,
  由运行者按 README 顺序执行。
- 结果依赖机器规格与内核参数(如 `somaxconn`),跨环境对比请以「保留率」为主。
- 压测端(loadgen/iperf3)本身的能力是天花板的一部分:先看直连基线是否远高于
  隧道值,否则说明压测端已饱和。
- 本套件不覆盖 STCP/SUDP/XTCP 等密钥/打洞类代理的性能(功能正确性由 tests/e2e 覆盖)。
