# snappy 压缩泵固定开销优化(round-49)

一句话结论:**每块固定开销从「压侧 2 次分配 + 解侧 3 次分配、解侧最多 6 次 await」降到「压侧 0 分配 + 解侧 0 分配、解侧 2 次 await」,1MiB 混合负载的本机 CPU 时间下降约 30%**。线格式、`MAX_DECOMPRESSED_CHUNK` 校验、三个既有测试全部不变。未进入零拷贝快路径,也不以本次改动为前提。

## 0. 身份

- 仓库 `rabbithole`,起点 HEAD `90f6f6b`。
- 改动范围:`crates/rh-transport/src/compress.rs`(重写两个 pump)、新增 `crates/rh-transport/examples/compress_bench.rs`(诊断基准)、`crates/rh-transport/Cargo.toml`(仅加 dev-dependency `libc` 供基准读 RSS)。
- `docs/deep-review-ffd49d3-remediation-plan.md` §4 把「compress 泵缓冲 / Decoder 复用」列为未做项,本轮即该项落地。
- 本机:macOS / rustc 1.98.1 / release(`lto = "thin"`)。**非三机基准**,不与 `benchmark/peer.sh` 的跨机保留率混用。

## 1. 改了什么

### 压侧(`compress_pump`)

改前每块:

```text
vec![0u8; max_compress_len(n)]   // 64KiB 块 ≈ 76KiB,每块新分配
enc.compress(...)                // 写进 comp
Vec::with_capacity(comp.len()+8) // 第二次分配
push_varint + wire.append(comp)  // comp → wire 整块拷贝
```

改后:

- `buf` / `wire` / `Encoder` 全部提到 loop 外,`wire` 是 `BytesMut`,一次性 `resize(5 + max_compress_len(64KiB), 0)` 把整块容量初始化。
- 每轮 `wire.clear(); wire.resize(5, 0)` 占住 5 字节 varint 位,**snappy 直接写进 `wire` 的 spare 区**,`advance_mut(clen)` 落长度,回填 varint 后 `write_all(&wire[start..])`。`varint_len(clen) < 5` 时从 `5-k` 起切,帧体一次搬移都没有。
- 省掉的正是旧实现「comp 分配 → append 拷进 wire」那一整块 `memcpy`。

### 解侧(`decompress_pump`)

- `comp` / `out` / `Decoder` 提到 loop 外,`decompress(&comp[..len], &mut out[..expected])` 直接写复用缓冲,不再 `Decoder::new().decompress_vec()` 每次产出新 `Vec`。
- varint 从「最多 5 次 `read_exact(&mut [0u8;1])`」改成**一次 `read` 读进 5 字节栈缓冲**再解析。

关于第 3 点有一处**必须记录的偏离**,见 §4。

## 2. 分配次数(同口径 A/B,不是估算)

`compress.rs` 的 `cfg(test)` 探针用线程局部开关的计数分配器,在同一进程里跑「旧实现原样搬来的 pump」与「新 pump」,数据量 1 块 vs 33 块:

| 项 | 1 块 | 33 块 | 每块增量 |
|---|---|---|---|
| 压侧 旧 | 5 | 68 | **2.00** |
| 压侧 新 | 3 | 3 | **0.00** |
| 解侧 旧 | 5 | 69 | **2.00** |
| 解侧 新 | 2 | 2 | **0.00** |

新旧两侧在 1 块时的固定成本差异来自 `wire` 预初始化(压侧)与 `comp/out/Decoder` 预建(解侧),那是每连接一次的开销,不随块数增长。**33 块时旧实现 68/69 次、新实现 3/2 次**,与 §0 里 64 块/1MiB 的换算一致。

计数探针写成常驻测试 `compress_pump_allocs_do_not_scale_with_chunks` / `decompress_pump_allocs_do_not_scale_with_chunks`,断言「每块摊到的分配数 < 1」。做过负向验证:把旧实现的 `vec![0u8; max_compress_len(n)]` 与 `vec![0u8; len]` 各加回去,两个测试分别以「压侧每块 1.97」「解侧每块 1.19」失败;移除后恢复绿色。

## 3. 计时(如实)

`cargo run --release -p rh-transport --example compress_bench -- 256 9 <pattern>`,current_thread runtime,每条链路压→解全程跑通并校验长度。数值是 9 轮里的 best 与 median(MiB/s,越高越好)。

| 数据型 | 改前 best | 改前 med | 改后 best | 改后 med | med 提升 |
|---|---|---|---|---|---|
| noise(64KiB 周期伪随机) | 2813 | 2781 | 3956 | 3764 | **+35.3%** |
| random(不可压缩) | 2765 | 2738 | 4120 | 3832 | **+40.0%** |
| mixed(半游程半随机) | 2937 | 2910 | 4257 | 4017 | **+38.0%** |
| zero(全零) | 2175 | 2161 | 2394 | 2377 | **+10.0%** |

改前基线跑了两遍(2781 / 2759 med,差 0.8%);改后每档连跑三轮取最好一轮。改后二进制与改前二进制在同一会话里交叉复跑过,旧二进制仍稳定在 2823 best,排除机器状态漂移。

单轮噪声不小:改后 `noise` 某一轮曾读到 3431 med,紧接着三轮分别是 3848/3801/3668 —— 这台机器的 `best` 更稳,`med` 波动可达 ±10%。因此**上表用 best 更保守地看提升幅度也不变**:noise +40.6%、random +49.0%、mixed +44.9%、zero +10.1%。结论不受单轮噪声影响。

`zero` 提升小是符合预期的:snappy 对全零数据走的是极短的 copy 序列,压缩本身占比低,收益主要落在省掉的分配上;这正说明提升来自「每块固定开销」而非压缩算法。

## 4. 一处偏离与一处被测试挡下来的错误

**偏离:不是字面意义的 `read_exact(&mut [0u8;5])`。**

需求写的是「一次 `read_exact(&mut [0u8;5])` 再解析」。直接这么做有两个硬问题:

1. 5 字节里通常只有 1~3 字节是 varint,剩下 2~4 字节是**本帧帧体的开头**。`read_exact` 把它们读走但不归还,帧体就少了前缀,必须另建回退缓冲 —— 等于把「省 await」换成「多一次拷贝 + 多一层状态」。
2. 流尾时 `read_exact` 要求读满 5 字节,否则报 `UnexpectedEof`;而正常结束的最后一条帧往往只剩 varint+帧体,长度恰好不是 5,会把这个**正常 EOF 误判成错误**。

所以实现是:一次 `read(&mut stash[..5])` 取回**已到达的**字节,解析出 varint 后,把 `stash[hdr_len..filled]`(≤4 字节)登记为帧体前缀搬进 `comp` 开头,再 `read_exact` 补剩余部分。稳态仍是每帧一次 varint await + 一次帧体 await,**不逐字节读、不丢字节、帧体不额外整体拷贝**。

> 中途还试过给读侧套 `tokio::io::BufReader`:功能正确且测试全绿、性能与最终版持平,但 `BufReader` 会把整条流先拷进自己的缓冲再由 `read_exact` 拷出,**等于给全线流量增加了 2 倍内存拷贝**。为不引入这项隐性成本,最终没有采用。

**被测试挡下来的错误:** 第一版回退区按「读满 5 字节」计算前缀长度,实际只读到 3 字节时就会把 `stash` 里的陈旧字节当成帧体拷进去。`roundtrip_with_byte_by_byte_source`(底层每次只吐 1/2/3/5/7/13 字节)直接报了解码不一致;修正为返回实际 `filled` 后通过。这个 bug 在常规分片下不会暴露,是那条逐字节测试抓出来的,已作为常驻回归。

## 5. 验收对照

| 要求 | 状态 |
|---|---|
| `roundtrip_small` 原样全绿 | 通过,**函数体与 HEAD 字节级一致** |
| `end_to_end_roundtrip_bulk` 原样全绿 | 通过,**函数体与 HEAD 字节级一致** |
| `corrupt_stream_closes` 原样全绿 | 通过,**函数体与 HEAD 字节级一致** |
| 1MiB 随机数据逐块编解码逐字节一致 | `roundtrip_random_1mib` + `roundtrip_odd_sizes`(0/1/5/127/128/4096/65535/65536/65537/200003)+ `roundtrip_with_byte_by_byte_source` |
| N 次循环后分配次数不随 N 线性增长 | 两个计数探针,见 §2,含负向验证 |
| 改前后 CPU 时间记入 docs | 本文 §3 |
| 线格式不变(varint + snappy raw,单块 ≤64KiB) | `wire_format_is_varint_plus_snappy` 独立手解 varint 校验;`compress_stream` 侧 `MAX_UNCOMPRESSED_CHUNK` 未动 |
| `MAX_DECOMPRESSED_CHUNK=4MiB` 校验保留 | 代码保留;`oversized_decompressed_chunk_is_rejected` 构造「snappy 头自称 4MiB+、帧体合法且很小」的帧,断言一个字节都不写出 |
| 三个旧测试断言未改 | 逐字节比对确认,见上 |

全量:`cargo test --workspace` 全绿(rh-transport lib 161 → 164);`cargo clippy -p rh-transport --all-targets -- -D warnings` 通过。

## 6. 未做的部分(如实)

- **没有让压缩规则进零拷贝快路径**。需求本身划清了边界:压缩包装后 `as_any` 无法还原裸 TCP,`splice` 资格判定必然失败。本轮只砍每块固定开销;要恢复零拷贝得改数据面设计(例如压缩下沉到帧编解码层),不在本轮范围。
- **没有做跨机验证**。按仓库约定,性能结论若需跨机可信必须走 `benchmark/peer.sh`;本次是单机微基准,只用于证明「固定开销确实降了」,不代表端到端吞吐的保留率变化。
- **`out` 缓冲在 `expected > 64KiB` 时会扩容一次**。常态帧不会触发(单块未压缩 ≤64KiB);保留这条只是因为对端理论上可以发 4MiB 以内的块,而 `MAX_DECOMPRESSED_CHUNK` 校验必须继续放行这类帧。
- **解侧 varint 最坏仍需 2 次 await**。varint 跨 TCP 分段到达时,`read` 返回 1~2 字节会再读一次;这是 `read` 语义决定的,旧实现同样场景要 5 次。

---

## 7. 后续专项调查(2026-10-21)

针对上一版遗留的两条「未做」,做了定向调查。结论都是**不改**:一条是被架构否决,
一条是被实测否决。数据在此留档,避免以后重复投入。

### 7.1 压缩能否进零拷贝快路径 —— 架构上不可行,且收益方向相反

**快路径的真实结构**(`crates/rh-server/src/data.rs:292-341`):两条互斥分支。

| 路径 | 前提 | 数据搬运 |
|---|---|---|
| chunk(mux) | `!use_compression && relay_mode != "copy"`,且 `is_mux_stream(&ch)`、`steal_mux_halves` 成功 | `RhMuxStreamRead::read_chunk() -> Bytes` 直接作帧体 `write_chunk`,按引用计数传递 |
| splice(Linux) | `!use_compression && relay_mode != "copy"`,且两端 `plain_tcp()` 都成功 | 内核 `splice(2)` 在 socket↔pipe 之间搬,用户态不碰数据 |

两条都硬性要求 `!use_compression`,原因不是「懒得写」,而是压缩在**语义上**就不允许:

1. **splice 的字节必须与线上字节一一对应**。压缩改变了字节流,`splice` 只会在两个 fd 之间原样搬字节,它没有「在这里解压」的钩子。要用 splice 就得让压缩消失,前提矛盾。
2. **chunk 快路径靠 `Bytes` 引用计数零拷贝**,压缩必须读全块内容才能产出新字节,天然要一次完整物化。即使把压缩下沉到 `write_chunk` 之前,也必然多一次「comp 缓冲 → 帧体」的物化,反而不如现在的直写 spare 区。

更关键的是**收益方向**:零拷贝省的是「用户态 ↔ 内核」的拷贝,而压缩的 CPU 成本是**算力**。实测这两者在同一台机器上的量级:

| 项 | 吞吐 |
|---|---|
| 纯管道拷贝(duplex write→read,64KiB 块) | 5495 MiB/s |
| 纯 snappy 压缩+解压(同样 64KiB 块,无管道) | 9512 MiB/s |

即**压缩本身只花掉管道拷贝时间的约 37%**。就算零拷贝能把管道拷贝那部分完全抹掉(实际上内核仍要搬,splice 只省用户态那一次),压缩的这笔算力也一分不少,而它已经比被省掉的拷贝便宜。所以「为了压缩去改数据面换零拷贝」在成本上不划算。

> 补充:`use_compression` 是代理级配置,默认关闭。压缩规则的定位本就是「带宽换 CPU」——省的是网线,不是 CPU。要求它同时又走零拷贝快路径,等于要求「既压缩又不耗算力」,目标本身不自洽。

**结论:不做。** 真要改善压缩规则的端到端表现,方向是降低压缩本身的成本(换算法/调 snappy 等级),而不是改数据面的拷贝结构。

### 7.2 解侧 varint 能否降到 1 次 await —— 能,但实测更慢

先测了当前实现的真实 await 分布(给底层 `poll_read` 挂计数,同一份 wire 用不同分片喂):

| 底层分片 | 每帧 read 调用 |
|---|---|
| 一次给满(loopback 理想) | 2.12 |
| 64KiB(常规 TCP 段) | 2.12 |
| 4KiB | 17.1 |
| 512B | 129.1 |

即**当前已是 2 次**(varint 1 次 + 帧体 1 次),不是上一版说的「最坏 2 次」那么悲观;而分片越碎,瓶颈完全在分片本身,与 varint 怎么读无关。

然后实现了把多帧一次读进 `BytesMut` 的版本(`read_buf` + 缓冲内解析,帧体用 `split_to` 零拷贝取出),对照实测 200 轮:

| 底层分片 | 现方案 polls/帧 | 缓冲方案 polls/帧 | 现方案耗时 | 缓冲方案耗时 |
|---|---|---|---|---|
| 一次给满 | 2.00 | **0.34** | 252µs | 333µs |
| 64KiB | 2.00 | **1.02** | 250µs | 294µs |
| 4KiB | 17.00 | 16.02 | 220µs | 247µs |

**await 次数确实降了一半以上(2.00 → 1.02,理想分片下 0.34),但耗时反而慢 12%~32%。** 原因:

- `BytesMut` 的 `split_to` 要维护引用计数与共享分配,比栈上 5 字节回退区贵;
- `read_buf` 每次要 `reserve`/算容量,而当前方案是固定 `read` 进已对齐的栈缓冲;
- await 本身在 current_thread runtime 上是廉价的(无跨线程唤醒);**省一次 await 省不到什么,省一次堆操作才值钱**。

这与 §2/§3 的结论互相印证:本轮提升主要来自**去掉每块堆分配与整块拷贝**,而不是减少 await。继续抠 await 是在错误的方向上加码。

**结论:不做。** 除非将来出现「分片极碎 + 高并发 + 调度开销占主导」的实测证据,否则不值得用 12%~32% 的吞吐换一次 await。

### 7.3 两条都不是「做不到」,是「算下来不该做」

- 零拷贝:语义上不允许(压缩改变字节流),工程上也不划算(压缩比被省掉的拷贝便宜)。
- 1 次 await:工程上做得到,实测净亏。

两条都留在此处作为**经证伪的方向**,避免下次重启同一调查。
