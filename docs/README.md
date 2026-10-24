# docs/ 索引

本目录按**用途**分区,而不是按时间堆放。每篇文档都标了状态,读之前先看状态:

| 状态 | 含义 |
|---|---|
| **现役** | 描述当前实现,可作为依据 |
| **参考** | 结论仍有效,但细节可能滞后于代码 |
| **历史** | 当时的记录,结论可能已被推翻,**不可作为现役事实** |

> **一句话原则**:判断"现在是什么样",看代码 + 标为**现役**的文档;
> 判断"为什么这样做",再查**历史**文档。

---

## 1. 现役(当前实现的依据)

| 文档 | 内容 |
|---|---|
| [design.md](design.md) | P0 设计主文档:架构、RHCP 协议、数据面状态机、数据库、安全量化。**协议与状态机的唯一口径** |
| [requirements-matrix.md](requirements-matrix.md) | frp v0.71.0 能力对照矩阵(A–E 五域 58 项 + X 实验项 3 项),逐项状态与证据指针 |
| [acceptance.md](acceptance.md) | 验收记录。§1–§15 为 10-08~10-10 累积,§16 为现役控制台质量门 |
| [review-log.md](review-log.md) | 评审日志 R1–R33(10-08~10-12)。**10-13 之后的评审见下方专题文档** |
| [vhost-benchmark-rh-vs-frp.md](vhost-benchmark-rh-vs-frp.md) | vhost 专项基准(TCP/TLS × HTTP/HTTPS),当前对 frp 结论 |
| [deep-review-performance-report.md](deep-review-performance-report.md) | **当前性能口径**:TCP/TLS 12 档全部超过 frp(2026-10-21) |

## 2. 交接与状态(接手先读)

| 文档 | 内容 |
|---|---|
| [review-2026-10-handoff.md](review-2026-10-handoff.md) | **最新交接**:2026-10-22 静态审查(1 P0 + 14 P1 + P2/P3)修复进度、未完成项、待决策 |
| [HANDOFF-virtual-net-plugin.md](HANDOFF-virtual-net-plugin.md) | ⏸ **已搁置**(2026-10-22,用户判定不重要)。`virtual_net` 任务书 + 核实结论(frp 是双插件配对,原文清单缺 visitor 侧) |
| [not-supported.md](not-supported.md) | **明确不做**的能力及 frp 源码证据(SOCKS5 BIND 等),防止被反复当成遗漏 |
| [fixed-plugin-proxy-semantics.md](fixed-plugin-proxy-semantics.md) | `http_proxy` / `socks5` 插件语义重写(已完成并验收) |
| [deep-review-ffd49d3-remediation-plan.md](deep-review-ffd49d3-remediation-plan.md) | 七路深审 A/B/C 三批修复方案与逐项状态 |
| [static-audit-2026-10-perf.md](static-audit-2026-10-perf.md) | **2026-10 全量静态审计**(5.5 万行):性能 / 死代码 / 并行串行 / 零拷贝,30 条发现。**已完成**,提交 `b84e7e1`;另有 12 轮后续扫描记在各自提交信息里 |
| [task-concurrency-serialization.md](task-concurrency-serialization.md) | 任务书:「应并行/异步却串行」12 项。**已完成**,报告见 [concurrency-serialization-fixes.md](concurrency-serialization-fixes.md)。一次性任务书,基线 `1b8bfd6`,勿当现行指令 |
| [task-fix-report-errors.md](task-fix-report-errors.md) | 任务书:更正上述报告的 2 处事实错误 + 1 处逻辑矛盾 + 1 处遗漏。**已完成**(`3a0d205` / `9a68def`) |

## 3. 专题:设计与决策

| 文档 | 内容 | 状态 |
|---|---|---|
| [design-parity.md](design-parity.md) | 对标 nps 的能力补齐设计(F1–F9)。**能力口径有效**,前端实现锚点已过时 | 参考 |
| [console-ui-redesign.md](console-ui-redesign.md) | 控制台 UI 重设计(全面版)。形状/层次/响应式规范仍有效,色值与行号已过时 | 参考 |
| [settings-page-review.md](settings-page-review.md) | 设置页评审。**引用的是已删除的 antd 实现**(`components/ui.tsx`、`global.less`),问题已随重写解决 | 历史 |
| [0rtt-remediation.md](0rtt-remediation.md) | TLS 1.3 / QUIC 0-RTT 整改。代码已按此实现(`max_early_data_size=0` / `enable_early_data=false`),由 `tls.rs` 三条回归测试锁定 | 现役 |
| [carriers-decision-analysis.md](carriers-decision-analysis.md) | `default_carriers=2` 与 RTT 自适应的冲突判断 | 参考 |
| [multicarrier-autosizing-handoff.md](multicarrier-autosizing-handoff.md) | 多载体池自适应定容的前因后果与方案。**定容规则已两次演进**(协议基线改为恒 2、硬件上限 `cpus-1`、新增 RTT 自适应),现役口径见 README | 参考 |
| [not-supported.md](not-supported.md) | **明确不做的能力**(SOCKS5 BIND/UDP ASSOCIATE、插件级代理链)。经 frp 源码核实,含证据链 —— 这些不是待办 | 现役 |

## 4. 专题:性能优化(单点结论)

| 文档 | 优化点 | 结论 |
|---|---|---|
| [compress-pump-fixed-cost.md](compress-pump-fixed-cost.md) | snappy 压缩泵每块固定开销 | 已落地 |
| [control-frame-encode-once.md](control-frame-encode-once.md) | 控制消息重复序列化与整包拷贝 | 已落地 |
| [dns-cache-c9.md](dns-cache-c9.md) | 目标解析 TTL 缓存 + 并发去重 | 已落地,QPS +51% |
| [plugin-fixed-cost.md](plugin-fixed-cost.md) | 插件路径每请求固定开销 | 已落地(但 http2http QPS 无可测提升) |
| [perf-client-copy-lockless-split.md](perf-client-copy-lockless-split.md) | 客户端 copy 回落路径无锁拆分 | 已落地 |
| [perf-client-udp-writer-batching.md](perf-client-udp-writer-batching.md) | UDP 写路径单写任务 + 批聚合 | 已落地 |
| [rtt-probe-boundaries.md](rtt-probe-boundaries.md) | RTT 自适应探针三个边界问题 | 已落地 |
| [scale-inefficiency-fixes.md](scale-inefficiency-fixes.md) | `apply_sync` O(N²) 与 waiting 表 | 已落地 |
| [visitor-mux-relay.md](visitor-mux-relay.md) | visitor 数据面零拷贝 mux 中继 | 已落地 |
| [pump-down-loop-gap-shadowing.md](pump-down-loop-gap-shadowing.md) | `pump_down` 变量遮蔽缺陷 | 已修 |
| [mux-stream-limit-investigation.md](mux-stream-limit-investigation.md) | mux 并发流上限**不该**加到客户端 | 结论:不改 |
| [single-carrier-bottleneck-summary.md](single-carrier-bottleneck-summary.md) | 单载体/多载体瓶颈实测结论:K 的最优值取决于 BDP,低延迟下 K=1 更快 | 参考 |

## 5. 历史:性能攻坚(2026-10-13 ~ 10-15,已被取代)

| 文档 | 内容 |
|---|---|
| [performance-2026-10-history.md](performance-2026-10-history.md) | **合并稿**:当时目标与判定、被证伪的 9 个方向、后来怎么解决的、方法论教训 |

> 该时期原有 **15 篇分阶段文档**,已于 2026-10-22 合并为上面这一篇后删除
> (它们结论一致均为"G1/G2/G6 未达标",且 11 篇引用不入库的 `benchmark/runs/`,
> 新克隆必断链)。原文可从 git 取回:
>
> ```bash
> git log --diff-filter=D --name-only -- 'docs/performance-*'
> git show <commit>:docs/performance-g1-tcp-report.md
> ```
>
> 同批删除的还有 `docs/single-carrier-bottleneck-measurement.md`
> (13,119 行 / 472 KB 的 85 轮实验日志),结论已抽入
> [single-carrier-bottleneck-summary.md](single-carrier-bottleneck-summary.md)。

## 6. 子目录

| 路径 | 内容 |
|---|---|
| [rabbithole-vs-frp-multiflow/](rabbithole-vs-frp-multiflow/) | TCP 多流对 frp 的根因分析与原始对照数据 |
| [screenshots/console-shadcn/](screenshots/console-shadcn/) | 现役控制台走查截图(40 张,生产构建 + 真实后端) |

## 7. 已废止

| 文档 | 说明 |
|---|---|
| [../PROGRESS.md](../PROGRESS.md) | 已停止更新的会话备忘(截至会话 11)。根因与踩坑教训仍有效,状态描述已过期 |

> `CONSOLE-REWRITE-PROMPT.md` 已于 2026-10-22 **删除**(而非归档):它是写给
> agent 的一次性任务书,要求的 antd v6 技术栈与 `secret_key` 表单 /
> `allow_users` / `/auth/login` 三处规格均已被推翻,留在根目录会被误当作
> 现行指令。需要时 `git show <commit>:CONSOLE-REWRITE-PROMPT.md` 可取回。

---

## 关于证据指针

本目录多篇文档引用 `benchmark/runs/` 下的原始证据。**该目录不入版本控制**
(见 `.gitignore`:体积大、含内网地址),因此**克隆仓库后这些路径不存在**,
文档里的「详见 `benchmark/runs/xxx/verdict.json`」会是死链。

复核证据时的做法:

1. **结论性数字已内联在文档正文**,可直接引用,不必追溯原始文件;
2. 需要原始 JSONL / verdict 时,向维护者索取,或按根目录 `AGENTS.md`
   「性能基准(三机 peer 基准)」一节在三机上复跑重建;
3. 新增文档引用这类路径时,**必须同时写出关键数字**,不能只给指针。

---

## 维护约定

- 新文档落盘时,**在本文对应分区加一行**,并标注状态。
- 结论被推翻时,优先**合并进综述并删除分阶段文档**(避免读者从中间一节读起
  得到已被自身后续推翻的结论);确实需要保留原貌时,在开头加
  "⚠ 已被 X 取代"横幅。
- 分阶段的过程日志(如逐轮实验记录)**不要长期堆积在 `docs/` 根目录**,
  它的价值在结论,不在过程。
- 判定"不做"的能力,**写进 [not-supported.md](not-supported.md) 并给出依据**
  (通常是 frp 源码位置),而不是只在讨论里说一句。否则它会被反复当成遗漏
  重新提起。
- `benchmark/runs/` 与 `*.jsonl` 不入库(见 `.gitignore`),文档引用这些路径时
  应说明"原始证据在磁盘上,不入库"。
