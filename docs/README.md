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
| [fixed-plugin-proxy-semantics.md](fixed-plugin-proxy-semantics.md) | `http_proxy` / `socks5` 插件语义重写(已完成并验收) |
| [deep-review-ffd49d3-remediation-plan.md](deep-review-ffd49d3-remediation-plan.md) | 七路深审 A/B/C 三批修复方案与逐项状态 |

## 3. 专题:设计与决策

| 文档 | 内容 | 状态 |
|---|---|---|
| [design-parity.md](design-parity.md) | 对标 nps 的能力补齐设计(F1–F9)。**能力口径有效**,前端实现锚点已过时 | 参考 |
| [console-ui-redesign.md](console-ui-redesign.md) | 控制台 UI 重设计(全面版)。形状/层次/响应式规范仍有效,色值与行号已过时 | 参考 |
| [settings-page-review.md](settings-page-review.md) | 设置页评审。**引用的是已删除的 antd 实现**(`components/ui.tsx`、`global.less`),问题已随重写解决 | 历史 |
| [0rtt-remediation.md](0rtt-remediation.md) | TLS 1.3 / QUIC 0-RTT 整改。代码已按此实现(`max_early_data_size=0` / `enable_early_data=false`),由 `tls.rs` 三条回归测试锁定 | 现役 |
| [carriers-decision-analysis.md](carriers-decision-analysis.md) | `default_carriers=2` 与 RTT 自适应的冲突判断 | 参考 |
| [multicarrier-autosizing-handoff.md](multicarrier-autosizing-handoff.md) | 多载体池自适应定容的前因后果与方案。README 已记录最终实现 | 参考 |

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
| [single-carrier-bottleneck-measurement.md](single-carrier-bottleneck-measurement.md) | 单载体多流瓶颈实测(含 13k 行原始数据) | 参考 |

## 5. 历史:性能攻坚(2026-10-13 ~ 10-15,已被取代)

> **这 14 篇是同一场攻坚的分阶段产物,结论均为"G1/G2/G6 未达标"。**
> 那是**当时构建**的判定;现役版本在 2026-10-21 已达成全部 12 档超过 frp。
> 保留它们是作为证据链与归因过程,**不要引用其性能结论**。

按时间顺序:

1. [performance-repair-acceptance.md](performance-repair-acceptance.md) — 验收方案(规划稿 r5)
2. [performance-repair-history.md](performance-repair-history.md) — 1059 行全过程(含历史附录)
3. [performance-repair-history-2.md](performance-repair-history-2.md) — 44 行简报
4. [performance-repair-history-3.md](performance-repair-history-3.md) — 主代理复核撤回"全部通过"
5. [performance-repair-history-4.md](performance-repair-history-4.md) — kcp-boundary 七轮收官
6. [performance-repair-status.md](performance-repair-status.md) — c84f631 收口轮 full-accept
7. [performance-closure-c84-plan.md](performance-closure-c84-plan.md) — c84 一页执行设计
8. [performance-closure-c84-report.md](performance-closure-c84-report.md) — c84 收口轮报告
9. [performance-closure-r1-plan.md](performance-closure-r1-plan.md) — r1 计划
10. [performance-g1-tcp-plan.md](performance-g1-tcp-plan.md) — G1 3000 Mbps 专项方案
11. [performance-g1-tcp-r2-plan.md](performance-g1-tcp-r2-plan.md) — G1 第二轮方案
12. [performance-g1-tcp-report.md](performance-g1-tcp-report.md) — G1 FAIL(1952/2036 Mbps)
13. [performance-g1-tcp-2800-plan.md](performance-g1-tcp-2800-plan.md) — G1-2800 方案
14. [performance-g1-tcp-2800-progress.md](performance-g1-tcp-2800-progress.md) — G1-2800 进度
15. [performance-g1-tcp-2800-report.md](performance-g1-tcp-2800-report.md) — G1-2800 FAIL

> 另有 [post-task-evaluation.md](post-task-evaluation.md)(后续任务评估 + 三机回归,
> 结论:F 级 9 项无一项构成真实优化)与
> [pending-unified-review.md](pending-unified-review.md)(六轮改动的统一收尾清单,
> 其"未提交工作树"状态已随 `9bd748b` 落库而失效)。

## 6. 子目录

| 路径 | 内容 |
|---|---|
| [rabbithole-vs-frp-multiflow/](rabbithole-vs-frp-multiflow/) | TCP 多流对 frp 的根因分析与原始对照数据 |
| [screenshots/console-shadcn/](screenshots/console-shadcn/) | 现役控制台走查截图(40 张,生产构建 + 真实后端) |

## 7. 已废止

| 文档 | 说明 |
|---|---|
| [../CONSOLE-REWRITE-PROMPT.md](../CONSOLE-REWRITE-PROMPT.md) | 已废止的一次性任务书(2026-10-12 标注)。多项要求被推翻,仅作历史追溯 |
| [../PROGRESS.md](../PROGRESS.md) | 已停止更新的会话备忘(截至会话 11)。根因与踩坑教训仍有效,状态描述已过期 |

---

## 维护约定

- 新文档落盘时,**在本文对应分区加一行**,并标注状态。
- 结论被推翻时,**不要删除旧文档**,在开头加"⚠ 已被 X 取代"横幅,并更新本索引。
- `benchmark/runs/` 与 `*.jsonl` 不入库(见 `.gitignore`),文档引用这些路径时
  应说明"原始证据在磁盘上,不入库"。
