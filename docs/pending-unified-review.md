# 统一收尾清单(rh-client / rh-transport 六轮 + 后续任务)

> **⚠ 已销项(2026-10-22 补注)**。本文约定"全部完成后统一收尾"的那批改动,
> 已于 `9bd748b`(`perf: 六轮固定开销优化 + 三处规模/诊断修正`)切分落库,
> 文中的"未提交工作树""待切分提交""下一步动作清单"**全部失效**。
> 保留本文是因为 §8.1「测试设计教训」与 §1「证据强度分级」仍是有效的
> 方法论约束(测试引用被测常量 = 自证循环、纯函数测试抓不到调用点错误等)。

> **用途**(历史):后续还有若干任务未做。约定为**全部完成后统一做分析与修正**,不逐项收尾。
> 本清单用于最后销项,避免跨轮遗漏。
>
> **基线**(历史):`90f6f6b` 之上全部改动**均未提交**,持续叠加在同一棵工作树上。
> 最后统一 review 后再决定如何切分提交。

## 0. 当前状态快照

- 代码改动(11):`crates/rh-transport/{compress.rs, lib.rs, tcp.rs, Cargo.toml}`、
  `crates/rh-client/{dnsquery.rs, forward.rs, plugins.rs, session.rs, visitor_run.rs,
  nathole_cli.rs}`
- 新增:`crates/rh-transport/examples/compress_bench.rs`、
  `crates/rh-client/src/testdata/{cache-test.crt,cache-test.key}`
- 文档:`docs/{compress-pump-fixed-cost, visitor-mux-relay, plugin-fixed-cost,
  dns-cache-c9, rtt-probe-boundaries, control-frame-encode-once,
  scale-inefficiency-fixes, mux-stream-limit-investigation,
  pump-down-loop-gap-shadowing}.md` +
  更新 `docs/deep-review-ffd49d3-remediation-plan.md`(C9 改为 ✅)
- 验证现状:`cargo test --workspace` 16 套件全绿(clippy 干净)、`tests/e2e.sh` 82/82、
  `tests/e2e-plugins.sh` 15/15、`tests/xtcp-isolated.sh` 7/2(既有环境问题,已三组对照)

## 1. 证据强度分级(最后分析时按此判断可信度)

| 级别 | 含义 | 涉及项 |
|---|---|---|
| **A. 单机实测、提升明确** | 有改前/改后对照,多次运行一致;但仅 loopback,非生产数据 | compress 泵、visitor mux、DNS 缓存 |
| **B. 有基准但结论为"无提升"** | 基准本身受限,测不到改动收益(非改动无效) | 插件路径 |
| **C. 调查后判定"不该做"** | 有量化依据,方向被否决 | 压缩进零拷贝、varint 降到 1 次 await |
| **D. 无基准、刻意不造** | 结构上更省,但不值当造数据 | 控制帧编码合并、RTT 探针取舍 |
| **E. 行为变化** | 语义确实变了,已判断可接受 | 见 §4 |
| **F. 功能缺口** | 明确没做的范围 | 见 §5 |

## 2. A 级:单机实测有提升(均**未**跑三机 `peer.sh`)

| 项 | 实测 | 文档 |
|---|---|---|
| compress 泵固定开销 | 1MiB 链路 CPU 时间降 10%~43%(随可压缩性);33 块时压侧分配 68→3、解侧 69→2 | `compress-pump-fixed-cost.md` §2/§3 |
| visitor mux 中继 | STCP 单流 best 625.7→716.9 MiB/s(+14.6%),med +15.1% | `visitor-mux-relay.md` §3 |
| DNS TTL 缓存 | 域名目标 vhost 短连接 QPS med 3483→5254(+50.8%) | `dns-cache-c9.md` §3 |
| apply_sync 判存在 | 500 条规则:16.791µs→3.5µs(**4.8×**);N=2000 时 **15×** | `scale-inefficiency-fixes.md` §1 |

**销项动作(待定)**:是否补三机 `benchmark/peer.sh` 验证。仓库约定跨机可信性能必须走该脚本;
本轮三项都只有 loopback 数据。

## 3. B 级:有基准,结论"没有可测提升"

**插件路径固定开销**(`plugin-fixed-cost.md`):改前 med 5539.5 → 改后 5444.7(**-1.7%**),
在改前自身波动范围内。原因是基准测不到,不是改动无效:

1. 单进程 Python 上游仅 2742 QPS(比被测链路还慢),换 6 进程 + `SO_REUSEPORT` 后到 8806;
2. macOS 临时端口仅 16384 个 + `msl=15s`,压测 >6s 必然 `EADDRNOTAVAIL`;
3. rh 链路每请求的服务端转发 + mux 流 + 插件拨号成本,远大于"读 300 字节头"。

**销项动作(待定)**:需 Linux(临时端口充足)且上游能吃满压测的机器重测。当前无此条件。

## 4. C 级:调查后判定"不该做"(有数据)

**① 压缩进零拷贝快路径** —— 架构不可行且收益方向相反
- splice 要求字节与线上字节一一对应,压缩改变了字节流;
- 实测:纯管道拷贝 5495 MiB/s vs 纯 snappy 压解 9512 MiB/s → 压缩只占拷贝时间的 37%,
  零拷贝抹掉用户态拷贝也省不掉这笔算力,而它比被省掉的拷贝便宜。
- 详见 `compress-pump-fixed-cost.md` §7.1

**② 解侧 varint 降到 1 次 await** —— 做得到,实测净亏
- 多帧缓冲方案:await 2.00→1.02 polls/帧(理想分片 0.34),但耗时 **+12%~32%**;
- 原因:`BytesMut::split_to` 引用计数 + `read_buf` 容量管理,比栈上 5 字节回退区贵;
  省一次 await 收益不足,省一次堆操作才值钱。
- 详见 `compress-pump-fixed-cost.md` §7.2

**销项动作**:除非出现"分片极碎 + 高并发 + 调度开销主导"的实测证据,否则不重开。

## 5. D 级:无基准、刻意不造

**① 控制帧编码合并**(`control-frame-encode-once.md`):一条消息少一次分配 + 一次整包 `memcpy`。
控制面是心跳级(默认 30s)+ 规则变更若干条,**不在吞吐热路径**;刻意没编 QPS 数字。
- 未做的下一步:让 `serde_json` 直接写进预分配缓冲(`to_writer`),再省一次小 `memcpy`。
  控制帧几百字节、非热路径,**没有性能证据支持,故不做**。

**② RTT 探针取舍**(`rtt-probe-boundaries.md`):缓存永不过期、无上限。
键只有 1~2 个、RTT 在同目标上稳定,加 TTL 或淘汰都没有基准支撑。

## 6. E 级:行为变化(已知悉,非遗漏)

| 项 | 变化 | 判断依据 |
|---|---|---|
| TLS acceptor 缓存 | 证书在运行期被替换时**不自动刷新**(需重启) | 插件证书来自静态配置,轮换本就要重启 |
| `serve_static_file` root canonicalize | 缓存到函数入口,运行期删建 root 不反映 | 静态 root 不预期运行期变更 |
| `send_msg` 序列化失败 | 由"发空 payload 帧"改为"不发 + 返回 false" | 行为改进;目前 `ClientMessage` 全可序列化,该分支不可达 |
| DNS 负缓存 | 不区分 NXDOMAIN 与超时,均按 5s 缓存 | 想要的:故障时不再每次等 2s;代价是瞬时抖动会被锁 5s |
| DNS `RH_DNS_TTL_SECS` | `OnceLock` 只读一次,进程内改 env 不生效 | 生产正确(避免运行期抖动);代价是测试只能改条目时间戳 |

## 7. F 级:功能缺口(明确没做)

| 项 | 说明 | 文档 |
|---|---|---|
| SUDP 未接 mux 快路径 | 走 `udpchan::run_visitor_side`,UDP 包语义与 mux chunk 不同 | `visitor-mux-relay.md` §5 |
| XTCP 打洞直连路径未在真打洞环境验证 | 本机打洞失败,该路径实际不执行;与 `deliver_relay` 共用 `relay_stream`,逻辑等价但没跑过 | 同上 |
| DNS 未用应答里的 TTL | 固定 30s,短 TTL 轮转场景会拿旧地址 | `dns-cache-c9.md` §6 |
| DNS 无后台刷新 | TTL 到期瞬间有一个请求承担真实解析 | 同上 |
| `load_certs`/`load_key` 未改 `spawn_blocking` | 仅首次 dial 读盘且有缓存兜底,改 async 签名收益不抵成本 | `plugin-fixed-cost.md` §6 |
| compress `out` 缓冲 | `expected > 64KiB` 时扩容一次;常态帧不触发,保留是为放行 4MiB 以内的块 | `compress-pump-fixed-cost.md` §6 |
| 插件 `load_*` 之外未改异步 | 同上 | `plugin-fixed-cost.md` §6 |
| `apply_sync` 两段未并行化 | 消除 O(N²) 后判存在已非瓶颈(3.5µs@500) | `scale-inefficiency-fixes.md` §6 |
| waiting sweeper 全表扫描 | 1000 条时每秒一次 retain,微秒级;无证据需换 BTreeMap/时间轮 | 同上 |

## 8. 中途修掉的"非本轮引入"问题(记账用)

- **RTT 探针用例的并行污染**(`control-frame-encode-once.md` §3):上一轮用 `static AtomicUsize`
  存注入状态,持锁窗口只覆盖 `probe_setup`,rh-client 测试同进程改同一批全局值 → 偶发
  `probe_result_matches_thresholds` 得到 K=2(应为 1)。已改为 `thread_local`,全量连跑 5 次稳定。
- **visitor mux 功能测试区分不了路径**(`visitor-mux-relay.md` §2):`mux_relay` 与 `splice`
  语义等价,把快路径关掉功能测试照样全绿。已加 `#[cfg(test)] relay_path_probe` 锁路径。

## 8.1 测试设计教训(记账用)

- **测试引用被测常量 = 自证循环**。本轮 first version 的 TTL 测试用 `WAITING_TTL`
  构造"过期时刻",负向验证(把常量改 1s)时**测试仍然全绿**。凡红线断言必须用
  **字面量**(`from_secs(11)` / `from_secs(9)`),不能用常量本身。
- **纯函数测试抓不到调用点错误**。pump_down 的遮蔽 bug:纯函数 `loop_gap_step`
  本身没错,把 `let` 恢复回去纯函数测试**全绿**,只有真实驱动 `pump_down` 的
  端到端测试才报红。凡是"抽函数便于单测"的重构,必须同时保留一条真实路径测试。
- **`thread_local` 在 `#[tokio::test]` 下不可靠**。multi_thread 运行时会让测试跨
  worker 迁移,set/read 可能不是同一份状态。探针测试要用「全局原子 + async 锁
  串行化整个窗口」。上一轮我误判成已修,本轮才定位到。
- **横向对照必须用"改动前代码"**。`xtcp-isolated.sh` 的两个 FAIL 本轮重新跑了
  `git stash` 基线对照(7/2 vs 7/2 vs 7/2),而不是引用前几轮结论——因为本轮恰好
  动了 `waiting` 表,而该脚本测的正是 xtcp 路径。

## 9. 最后统一分析时的建议动作清单

按优先级:

1. **决定是否切分提交**。当前是一棵累积了 8 个文件改动的工作树;建议至少按
   「compress 泵 / visitor mux / 插件 / DNS / RTT 探针 / 控制帧」切成 6 个独立提交,
   便于出问题时二分定位。
2. **A 级三项是否需要三机验证**。若需要,走 `benchmark/peer.sh`,并把结果补进各自文档。
3. **B 级(插件路径)是否换机器重测**。需 Linux + 上游能吃满压测。
4. **F 级逐条确认**哪些留作后续、哪些本轮补齐(尤其 SUDP 与 DNS 应答 TTL)。
5. **E 级行为变化**是否有需要回滚或加显式开关的(尤其 TLS 缓存失效)。
6. 全量复跑:`cargo test --workspace`、`clippy --workspace --all-targets -D warnings`、
   `tests/e2e.sh`、`tests/e2e-plugins.sh`、`tests/xtcp-isolated.sh`(后者本机有 2 个既有失败,
   已在 `visitor-mux-relay.md` §3 用对照证明与改动无关——最后复跑时注意别重新归因)。
