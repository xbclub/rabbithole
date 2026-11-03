# P2 / P3 / 形态 C:次级数据面快路径的后续方案(e476165 之后)

> **状态:方案(未实施)**。分析基线 HEAD=`e476165`(P1:次级数据面接入 mux chunk
> 快路径,已合入 main)。本文是三个后续 SCOPE 的实施设计与依据:
>
> - **P2**:mux↔mux 零拷贝泵 + S3 每向一次池化拷贝清零(§1)
> - **P3**:通用 copy 泵升级(§2)
> - **非 Linux 裸模式(形态 C)**(§3)
>
> 三案均为只读分析产出(未改任何代码);引用的 file:line 均按 e476165 现场行号。
> §0 附 P1 落地当天(2026-11-03)的三机回归基准摘要。

---

## 0. P1 回归基准摘要(2026-11-03,四轮)

条件:三机 peer 拓扑(test1 服务端 / test2 客户端+目标 / test3 压测),
`PROTO=tcp RELAY=splice MUX=1 THP_TOOL=loadgen PEER_DURATION=15 PEER_REPEAT=5`,
A/B/A + 工具链对照共四轮;证据在 test3 `/tmp/peer-{rel-a,new-b,rel-c,src-v071}/`
(本机留档 `/tmp/secfp-bench/`,未入库)。

| 轮 | 二进制 | 来源 |
|---|---|---|
| rel-a / rel-c | v0.7.1 **musl** 静态 | GitHub Release 产物(sha256 校验) |
| src-v071 | v0.7.1 **gnu** 源码构建 | test1 worktree 检出 tag(非 Release 产物) |
| new-b | e476165 **gnu** 源码构建 | test1(非 Release 产物) |

**隧道吞吐中位数(Mbps)**:

| 档位 | rel-a(musl) | rel-c(musl) | src(gnu v0.7.1) | new(gnu e476165) |
|---|---:|---:|---:|---:|
| 1流 上 | 2473 | 2475 | 2639 | 2711 |
| 1流 下 | 2418 | 2353 | 2447 | 2599 |
| 4流 上 | 3270 | 3053 | 3073 | 3245 |
| 4流 下 | 3206 | 3166 | 3192 | 3076 |
| 16流 上 | 3450 | 3450 | 3365 | 3381 |
| 16流 下 | 3414 | 3362 | 3390 | 3448 |

直连基线四轮稳定(1流 ~1470–1520、4流 ~1990–2075、16流 ~2160–2370,±2% 内),
机器状态可比。

**结论**:

1. **主路径无回归**。new-b 的全部指标落在同二进制日方差带内(两个 musl 轮自身
   在 4流上档就差 7%:3270 vs 3053);延迟(p50 1.047–1.066ms 隧道)、UDP
   (隧道中位 ~102–108 Mbps、丢包全 0)、并发(64–4096 档四轮全部
   `verified=established=hold_alive`)、建连速率(单次采样、噪声内)均无趋势。
2. **单流 +7~9% 是工具链效应,不是 P1 代码**:gnu 的两轮(src 2639 / new 2711)
   都高于 musl 两轮(2473/2475),而 src 与 new 之间(同工具链、只差 P1 代码)
   ≤3% 且样本区间重叠 → 判"无代码效应"。**附带观察:linux x86_64 的 musl
   Release 产物在该工作负载下单流比 gnu 构建慢 ~7–9%**(是否改发 gnu 产物
   涉及静态可移植性取舍,另行决策;发版产物仍是 musl,本条只记实测)。
3. HTTP vhost 隧道 QPS:1188/1154(musl)vs 1145/1130(gnu),单次采样,
   同为工具链方向、噪声量级,无代码回归信号。
4. **次级路径未被套件覆盖**:peer.sh 无 vhost HTTPS / CONNECT / tcpmux / stcp
   场景(README 明示 STCP/SUDP 不覆盖)。P1 五组接线点的「必须走 chunk」
   与计量等价由 9 组探针/等价测试背书(cargo test),**套件级吞吐收益未测**;
   补测路径见 §1.7(PEER_SCENARIOS=stcp 扩展)。
5. 伪影警示照旧:直连单流 ~1.5G 是 `loadgen sink` 的 `SetReadBuffer` cwnd
   塌陷伪影,保留率 >100% 一律判伪(benchmark/README.md「测量伪影」节)。

---

## 1. P2:mux↔mux 零拷贝泵 + S3 每向一次池化拷贝清零

### 1.1 现状(P1 妥协的解剖)

mux↔mux 恰好是三个接线点,全在服务端:

| 接线点 | 位置 | a 侧(user 位) | mux↔mux |
|---|---|---|---|
| S3a tcpmux 每流 | vhost.rs:1271 | `CountedUserIo<BoxedIo(RhMuxStream)>`(本地 `tcpmux_server_mux` accept 产物) | 是 |
| S3b stcp | visitor.rs:433 | `CountedUserIo<BoxedIo(QuotaIo<BoxedIo(RhMuxStream>)>)>` | 是 |
| S3c sudp | visitor.rs:299 | 同 S3b | 是 |

(S2a/S2b vhost HTTPS/CONNECT 与主路径 user 恒为 `CountedUserIo<TcpStream>`,
走 `mux_relay_split` 无锁路径,与 P2 无关;S5 客户端 tcpmux 的 local 非 mux 流。)

a 侧进 `mux_forward_chunked`(data.rs:922)后:`split_counted_tcp_user`
必然 miss → `mux_relay`(mux.rs:599)→ `split_boxed_io` miss →
`tokio::io::split` **共享一把 std::sync::Mutex**(round-69 病灶:futex 占 75.0%
syscall 时间,39.85s/53.13s、3690 次、均 10.8ms;A2 注释 data.rs:1344-1347
明说该病灶"在服务端整流路径从未修")。两向各有一次池化拷贝:

- **up**:RhMuxStream::poll_read 从 yamux 收缓冲零拷贝取 `Bytes` 后
  `put_slice` 拷入 UpReader 池化缓冲(mux.rs:77,12KiB→256KiB 两相);
- **down**:`Box<dyn Io>` 未实现 `poll_write_vectored` → 128 段聚合退化为
  逐帧单写,最终落 vendor 兼容写 `pool::get + put_slice`
  (vendor/yamux/src/connection/stream.rs:1071-1072,每 ≤16KiB 帧一次)。

零拷贝物理基础(已复核):vendor `StreamReadHalf::poll_read_chunk` 从 `Shared.buffer`
pop 出 `Bytes`(帧体本就是一个 Bytes,引用计数 1);`StreamWriteHalf::poll_write_chunk`
按所有权把 Bytes 直接作帧体 —— 同进程 `a.read_chunk() → Bytes → b.write_chunk()`
用户态零拷贝,窗口锁步(64KiB 红线)天然满足(帧体 ≤16KiB)。

### 1.2 候选与推荐

| 维度 | A:双侧偷半 + chunk↔chunk 直泵(钩子计量) | B:halves 实现 tokio trait 复用 mux_relay_split | C:vendor Chunks 整链移交 |
|---|---|---|---|
| 每向拷贝 | **0** | 1 次 | 0 |
| 同步锁 | 无 | 无 | 无 |
| down 批量写 | 恢复(vendor SendWorker ≤33 段 writev) | 仍退化 | 恢复 |
| 风险 | 中 | 小 | 大(vendor API 扩张) |
| 达成立项目标 | **是** | 否(只清锁不清拷贝) | 是但代价不成比例 |

**推荐 A**;B 可作骨架抽取评审不过时的降级备选;C 否决。

### 1.3 设计要点

rh-transport(`mux.rs`)新增:

```rust
pub struct MuxDirHook {            // 形态对齐 splice::DirHook(splice.rs:547)
    pub on_bytes: Option<Arc<dyn Fn(u64) + Send + Sync>>,
    pub meter_on_fill: bool,       // up=true(读侧 fill)/down=false(送达)
    pub limiter: Option<Arc<dyn crate::ChunkLimiter>>,
}
pub struct MuxRelayHooks { pub up: Option<MuxDirHook>, pub down: Option<MuxDirHook>,
    pub abort: Option<Arc<dyn Fn() -> bool + Send + Sync>>,   // flow_cut
    pub cancel: Option<crate::CancelFuture> }                  // rule_cancel

pub async fn mux_relay_mux(a_read: RhMuxStreamRead, a_write: RhMuxStreamWrite,
    b_read: RhMuxStreamRead, b_write: RhMuxStreamWrite, hooks: MuxRelayHooks)
```

- `pump_up_mux`/`pump_down_mux` 逐点对应 pump_up/pump_down(mux.rs:777/1063)
  删去 socket 特有部分:批收集(UP_BATCH 64 段/1MiB;DOWN 1MiB/128 段)→
  计费(fill 即计即耗 / 写完计)→ 逐块 `write_chunk`(Bytes 所有权直通)→
  写期间持续预取(round-68/76 形态)。
- **收敛骨架物理共用**(红线 3):从 mux_relay_split 抽 `pump_task`
  (select(泵,cancel)→errored→1s 有界 / clean EOF 无界 close_with_late_cancel)
  与 `relay_join_tail`(AbortOnDrop + panic/错误断对向 + join_relay_pumps
  30s 兜底),mux_relay_split 与 mux_relay_mux 用同一段代码,不是复制。
  半关闭形态等价:a 侧 clean EOF 的 `close()` 与 P1 的 `poll_shutdown` 是
  同一个 vendor FIN(mux.rs:225 / 118-129)。
- rh-server(data.rs):`peel_mux_user`(剥 `CountedUserIo<BoxedIo>` →
  [QuotaIo] → RhMuxStream,壳不匹配 Err 原样返还回落 P1 路径)+
  `mk_mux_hooks`;接线在 `mux_forward_chunked` 的 split_counted_tcp_user
  miss 分支;命中时**追加**计 `count_relay_path("chunk_mux")`(新 label,
  既有 "chunk" 计数与断言不动)。
- 回落链:kill-switch / 压缩 / copy → copy 泵(不变);channel steal 命中 →
  a=TcpStream 形态走 mux_relay_split(不变)/ **a=mux 流走 mux_relay_mux(新)**/
  壳不匹配走 mux_relay(P1 现状)。P2 位于 `RH_SECONDARY_FASTPATH` 下游,
  **不新增开关**。

### 1.4 红线核对

1. B7:P2 gate 在 data.rs:542 既有 gate 下游,copy/压缩根本进不来 ✓
2. 计量:`meter_on_fill` 语义与 splice::DirHook/mux_relay 注释逐字对齐;
   七元组等价测试锁定 ✓
3. 收敛:骨架共用(见上)✓
4. 无任何 fd 操作,纯用户态 ✓
5. kill-switch 沿袭 RH_SECONDARY_FASTPATH ✓

### 1.5 测试计划(镜像 P1)

- transport:`mux_relay_mux_transfers_and_halfcloses_both_directions`、
  `..._read_chunks_within_64kib`、`..._rule_cancel_converges_bounded`、
  `..._carrier_death_not_clean`、`..._meter_hooks_fill_vs_delivery`、
  `..._zero_copy_pool_quiescent`(处理 N 字节期间 vendor pool 计数增量为 0
  ——「拷贝清零」的可断言红测试,需 vendor pool.rs 加 `#[cfg(test)]`
  计数读取口);mux_relay_split 既有测试群全绿 = 骨架重构锚点。
- server:`secondary_mux_user_chunk_mux_engages_through_quota_shell`、
  `secondary_mux_user_metering_equivalence_vs_p1`(七元组)、
  `secondary_mux_user_write_failure_converges`、
  `secondary_mux_user_kill_switch_disables`、
  `peel_mux_user_rejects_reversed_or_wrapped`;vhost/visitor 接线点探针
  (`..._hits_chunk_mux`)。

### 1.6 预期收益(如实)

消除:S3 三点每向 1 次用户态拷贝 + 共享 Mutex(75% futex 病灶首次覆盖 S3)
+ down 向 writev 退化恢复。**单流改善有限,如实声明**:内存带宽算术下拷贝
约占单核 10~15%,S3 单流上限受 yamux 16KiB 帧与窗口锁步支配(P2 不改);
主要收益在**多流并发**(stcp 多 visitor / sudp 常驻 / tcpmux 每流)的
futex 清零与每流 CPU 份额。KPI 定为 P=8/16 的 stcp 保留率、CPU/GB、
futex 占比,不做数字承诺。

### 1.7 基准测法

peer.sh 无 tcpmux/stcp 场景。两步:
(a) 单机 micro-bench(#[ignore]+--release,双 yamux duplex 基建,
  mux_relay_mux vs mux_relay 整流 A/B + RH_PUMP_STATS + pool 计数)——
  只做归因不做结论;
(b) **peer.sh 加 `PEER_SCENARIOS` 开关(默认=现状,零漂移)**,`stcp` 档:
  server 建 stcp 规则 + test3 跑 visitor 实例 + loadgen 打 visitor 本地口,
  记录复用 `metric:"throughput"` 同 schema —— stcp 与 S3a/S3c 共用同一泵,
  作为代表场景;tcpmux 需 loadgen 加 CONNECT 前导模式,不做则以 stcp+
  micro-bench 代偿并如实标注。A/B:P1 vs P2 跨提交同机,每轮带
  kill-switch=0 底线,REPEAT≥5×15s,先看直连基线、>100% 判伪影。

### 1.8 实施顺序

vendor pool 测试读数口(小提交)→ 骨架抽取(mux_relay_split 改造,纯重构)
→ MuxDirHook/pump_*_mux/mux_relay_mux + transport 测试 → data.rs
peel_mux_user/mk_mux_hooks 接线 + chunk_mux 计数 + server 测试 →
micro-bench → peer.sh stcp 场景 → 三机结论。

---

## 2. P3:通用 copy 泵升级

### 2.1 现状盘点(e476165 之后谁还在 copy 泵上)

- 服务端 `copy_bidirectional_bounded`(data.rs:4932,泵核 copy_64k:4914):
  ①kill-switch 臂(data.rs:538)②chunk 未命中回落(554-556,channel 侧
  kcp/ws/tls/PrependedIo → tokio::io::split 共享锁)③**B7 压缩/copy 臂**
  (561-563,channel=JoinDuplex → 共享锁)④visitor sudp/stcp rt=None 回落
  (visitor.rs:301/434)⑤测试旧链。
- 服务端 `copy_relay_pair`(data.rs:636,泵核 copy_limited:834):
  `assign_forward` 末端(382-396)——压缩/copy/包装流/splice 失败回落。
- 客户端 `forward::splice`(forward.rs:513,泵核 copy_limited:607):
  ①S5 kill-switch 臂(46)②压缩/copy/非 mux 回落(231;含 PEER_COMPRESS=1)
  ③relay_stream 回落(294/299)④插件规则(plugins.rs:303,chan=DuplexStream
  → 共享锁)。
- 泵核:64KiB(红线测试 `copy_buf_cap_is_64kib` data.rs:2636 锁死)串行
  read→write_all,无 ReadBuf 直读/预取/writev;user 侧无锁拆半已落地
  (split_counted_tcp_user / split_boxed_io),channel 侧包装流仍共享锁。
- 不在本任务:UDP/sudp 帧泵(read_frame/write_frame,帧语义不适用字节泵)、
  rewrite_and_splice 内联泵(plugins.rs:1276,可选跟进)。

### 2.2 候选与推荐

**推荐:批量化+预取(= mux 泵既有技术平移)+ JoinDuplex 无锁拆半**。

- 红线冲突最小化:不改任何 gate/接线,B7 的 copy 臂"本身变快"而非"改道";
- `JoinDuplex`(compress.rs:68-81)是仓内自有类型,r/w 本就是两条独立
  duplex,加入 `split_boxed_io` downcast 分支零机理风险,压缩臂 100% 命中;
- TLS/kcp/ws 外来包装不强拆(锁内 poll 是用户态短操作,持锁时长与内核态
  不同量级),只做"自有类型可拆则拆"。

### 2.3 设计要点

rh-transport 新模块 `src/copy.rs`:

```rust
/// 通用 copy 泵核:ReadBuf 直读(单块 ≤64KiB 红线)+ 就绪抢收成批 +
/// writev 聚合 + 写期间持续预取。Ok(n)=clean EOF。
pub async fn copy_batched<R, W>(r: &mut R, w: &mut W,
    limiter: Option<Arc<dyn crate::ChunkLimiter>>) -> std::io::Result<u64>
```

- 部件从 mux.rs 平移提 pub(crate):`TryChunk`/`UpReader`/`read_chunk_into`
  (mux.rs:254-413)/`write_metered_vectored`(mux.rs:1072-1125);
- 常量:`COPY_CHUNK_CAP=64KiB`(单次读上限,与红线同值同义);
  `COPY_BATCH_BYTES=256KiB` 起步(`RH_COPY_BATCH_BYTES` 可覆盖,1024..=1MiB,
  仿 RH_UP_CHUNK_CAP;保守于 mux 的 1MiB,copy 泵承接慢连接/插件会话);
  `COPY_BATCH_SEGS=64`;**限速规则批降为单块**(保限速时延语义,
  收费点=批组装后 reserve+consume,总量仍=已读字节);
- `split_boxed_io` 加 JoinDuplex 分支(JoinDuplex 提 pub(crate),
  拆 r/w 包进既有 HalfRead/HalfWrite)—— 四个调用点零接线自动受益;
- 服务端接线:copy_bidirectional_bounded 的 copy_64k → copy_batched
  (select/halfclose_grace/WARNED 骨架逐行不动);copy_relay_pair 的
  copy_limited → copy_batched + 既有 mk_limiter 合成 ChunkLimiter
  (P3 后 copy/splice/chunk 三路径限速形态统一);
- 客户端接线:copy_limited → mk_client_limiter + copy_batched 薄包装
  (clean/ERROR_CLOSE_GRACE shutdown 逻辑原样);plugins.rs 经 splice 自动升级;
- **不加运行时 kill-switch**:语义保持的内核替换,等价性由测试锁定,
  回滚=revert(开关会双倍维护两条泵核)。

### 2.4 红线核对

1. B7:所有 gate 原样,压缩流仍 100% 进 copy 臂(只是臂内变快)✓
2. 计量挂点在 CountedUserIo 包装层,泵核替换不触碰;writev 按 Ok(k) 计数 ✓
3. 收敛骨架(select/spawn/AbortOnDrop/grace)逐行不动;泵内"写失败立即返回
   不 join 读"与 mux 泵既有红线同款 ✓
4. 只用可移植 API(ReadBuf/IoSlice/write_vectored),unsafe 仅
   advance_mut 带同款 SAFETY 注释 ✓

### 2.5 测试计划

A 路径/拆分:`split_compress_stream_lockless`、
`split_boxed_io_unknown_wrapper_still_errs`、`copy_batched_coalesces_writev`、
`copy_batched_flushes_without_waiting`;B 计量:
`copy_batched_metering_fill_and_delivery`(含跨批/部分写注入);既有
`secondary_chunk_vs_copy_metering_equivalence` 与全部收敛测试**原样全绿**
= "骨架未动"证明;D 红线锁:`copy_batched_chunk_cap_is_64kib`、
`copy_batch_bytes_env_bounds`;E:P1 全部探针不动必须全绿(dispatch 未漂移)。

### 2.6 基准测法(peer.sh 既有开关)

| 轮 | RELAY | PEER_COMPRESS | 对象 |
|---|---|---|---|
| base / cand | splice | **1** | 压缩臂(peer.sh:78-86 该开关即为此设计) |
| base / cand | **copy** | 0 | copy 对照臂 |
| 护栏 | splice | 0 | 快路径臂,必须无变化 |

看:保留率(>100% 判伪)、**单流档**(验证 ~1.25G 串行上限形态解除;
mux 泵同型修复先例 1.08G→向 3.06G 收敛,mux.rs:893-896)、CPU/GB、
strace futex 占比(round-69 同款取样,仅观测)。

### 2.7 预期收益与风险

copy 对照臂消除"每批等 RTT/2"串行节奏(机理同 mux 泵 round-56→68 修复,
幅度以基准为准不预写);压缩臂清共享锁 + 批量化;全臂写 syscall 摊薄。
风险:TLS/kcp/ws 载体仍共享锁(仅获批量化收益);WsIo 未实现
poll_write_vectored(退化逐段写,正确性无恙);限速规则不获聚合收益(刻意)。

---

## 3. 非 Linux 裸模式(形态 C)

### 3.1 现状

- 裸模式 = 客户端级 `multiplex=false`(**默认 true**,config.rs:97)+
  `protocol=tcp`:每数据通道一条物理 TCP(dial_per_stream),通道是裸
  TcpStream(tcp.rs:505-519/647-659)。
- 分流闸门顺序(forward.rs):tcpmux passthrough(:63)→ mux chunk gate
  (:185,裸 TcpStream 恒 miss)→ Linux 内核 splice 块(:202-229,
  `cfg(target_os="linux")`)→ **:231 `splice(peer, local, bucket)`**
  (e476165 后行号,任务书的 :190 已漂移)。
- :231 泵:拆半已无锁(split_boxed_io,round-69 修正已落地),但泵核是
  64KiB 串行 copy_limited —— **round-56 的 ~1.25G 单流形态是形态 C 剩余的
  全部结构性缺口**;Linux 同配置吃内核 splice 零拷贝,无此问题。
- 承诺落差:config.rs:28-29 明文 relay_mode=splice 是"Linux 零拷贝,默认",
  macOS/Windows 用户实际拿到 64KiB 串行泵 —— 静默缩水。
- 计量:客户端只有带宽桶(mk_client_limiter 适配 ChunkLimiter),
  **无 CountedUserIo/CountState**(那是服务端概念,推广到服务端时才按
  data.rs:922-947 的 A2 模式挂)。

### 3.2 候选与推荐

| 方案 | 判定 |
|---|---|
| 1. 串行泵微调(大缓冲/tokio copy_bidirectional_with_sizes) | 否:不解决串行结构,只平移上限 |
| **2. P3 通用 chunk 泵接线(推荐)** | UpReader/write_metered_vectored 本就与 mux 无关,抽通用件是**移动而非新写**;三平台同一路径零平台分支 |
| 3. macOS/Windows 零拷贝 syscall | **不存在**(无 socket→socket 零拷贝),否决 |
| 4. Linux splice 块一起统一? | 否:内核零拷贝用户态追不上;新泵不加 `cfg(not(linux))`,落位为 Linux splice 资源失败的兜底 |

### 3.3 设计要点

- rh-transport 新模块 `chunk_relay.rs`:
  `pub async fn chunk_relay(a: BoxedIo, b: BoxedIo, limiter, cancel)`
  (内部 split_boxed_io 两侧 + 每向批收集/预取/writev,收敛骨架镜像
  mux_relay_split);`pub fn plain_tcp(io) -> Option<&TcpStream>`
  (把 splice.rs:560-562 的裸 TCP 判定从 linux-only 模块提升到 lib.rs,
  消除平台不对称)。
- 客户端接线:Linux 块与 :231 之间插入
  `bare_fastpath_enabled() && client_chunk_gate(!压缩, relay_mode)
  && plain_tcp(peer) && plain_tcp(local)` → chunk_relay;
  kill-switch **独立** `RH_BARE_FASTPATH`(形态 C 是主数据面路径,
  与次级数据面的 RH_SECONDARY_FASTPATH 回滚域不同,不复用)。
- prefix/PROXY 头无需处理(无压缩路径下已写完);UDP 规则不经本函数。

### 3.4 红线 / 测试 / 基准

红线:B7 gate 同式;计量=带宽桶总量等价(读侧单点计费);收敛骨架镜像;
**零平台分支**是 Windows 红线的最小化形态(唯一验证=CI windows job)。
测试:chunk_relay 模块 8 项(双向穿透/半关闭/父取消/错误收敛/clean EOF
有界+迟到数据/计费等价(新旧泵 consumed_total 相等)/writev 聚合/
plain_tcp 真值表)+ 客户端探针 3 项(bare 探针 copy 反断言、env 真值表、
limiter 接线)。
基准:三机都是 Linux,形态 C 端到端不自然触发 → **移除 Linux splice 块的
对照树**(base=移除块走旧泵 / cand=移除块走新泵 / ref=原样内核 splice 天花板),
`MUX=0` 跑 G1 姿态,标注"对照实验构建,非 Release 产物";macOS 本机
RH_BARE_FASTPATH=0/1 定性验证命中;Windows 无实机,如实记未验证。

### 3.5 优先级建议:中低,排在 P2/P3 之后

前置是 P3 通用泵(单独做会造出第二个不共享内核的泵);主流用户在 mux=1
路径上(本方案零收益);但边际成本低(P3 后 ~30-50 行接线),顺带修复
Linux splice 资源失败兜底与"splice 承诺在非 Linux 静默缩水"。
建议顺序:**P2 → P3(通用泵抽取+压缩臂)→ 形态 C 接线 → 视基准决定 P4**
(压缩回落/服务端非 Linux 裸模式平移)。

---

## 4. 三案的实施顺序与共享重构

P2 与 P3 都要从 mux.rs 抽部件(P2 抽收敛骨架 pump_task/relay_join_tail,
P3 抽 UpReader/write_metered_vectored 到 copy.rs)。为避免两次重叠重构:

1. **P2 先行**(S3 专属收益,骨架抽取是它的前置);
2. **P3 复用 P2 抽好的骨架**,再把读写部件平移进 copy.rs(纯移动,
   mux.rs 既有 ~40 条泵测试全绿是抽取不改行为的锚点);
3. **形态 C 挂在 P3 的通用泵上**;
4. peer.sh 的 `PEER_SCENARIOS` 场景扩展(P2 §1.7)独立于代码顺序,
   可先行合入以补齐次级路径的度量缺口。
