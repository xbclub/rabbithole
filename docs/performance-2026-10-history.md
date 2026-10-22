# 性能攻坚史(2026-10-13 ~ 10-15)— 已被后续测量取代

> **状态:历史**。本文合并自 15 篇分阶段文档(2026-10-22 整理),结论均为
> 「G1/G2/G6 未达标」——那是**当时构建**的判定,现役版本已于 2026-10-21
> 达成全部 12 档超过 frp,见 [deep-review-performance-report.md](deep-review-performance-report.md)。
>
> **保留本文的理由只有一个**:它记录了「为什么当时没达标」以及
> **哪些方向被证伪**——这部分对将来做性能工作仍有价值,可避免重走弯路。
>
> 原始 15 篇可从 git 历史取回:
> ```bash
> git log --diff-filter=D --name-only -- 'docs/performance-*'
> git show <commit>:docs/performance-g1-tcp-report.md
> ```

---

## 1. 当时的目标与判定

目标口径(全程未变):三机 peer 拓扑,`multiplex=false` / `relay_mode=splice`,
单流,每方向 ≥3 次 direct/tunnel 配对取中位数,双端 CPU 每方向 <50% 单核,
采样覆盖 ≥95%。

| 阶段 | 目标 | 实测中位(上行/下行) | 判定 |
|---|---|---:|---|
| G1 | ≥3000 Mbps | 1952.5 / 2036.0 | **FAIL**(65.1% / 67.9%) |
| G1-2800 | ≥2800 Mbps | 1963.6 / 1990.6 | **FAIL** |
| G2 / G6 | — | — | **FAIL** |

**关键观察**:双向 CPU 仅 ≤34.6% 单核,采样覆盖 ≥97.5%,四套 E2E 与
Linux 全量测试全绿——**瓶颈不在 CPU 饱和**,也**不是正确性问题**。

> 注意:当时的 verdict JSON 里 `failures` 有 6 条,但其中 4 条是 CPU 条目
> (达标时也写入的 note,文案产自 note 分支)。真正导致 `g1_pass=false` 的
> 只有 2 条吞吐条目。这个坑当时专门记录过,复核旧证据时别被 6 条吓到。

---

## 2. 被证伪的方向(最有价值的部分)

以下方向都做过实验并被否定,**不要再试**:

| 方向 | 结论 | 证据 |
|---|---|---|
| 分片通道容量不足 | +3.5%,噪声内 | 单载体实测 |
| 上下行锁竞争 | 上行等待仅 2758B | 埋点统计 |
| 目标侧(目标服务)瓶颈 | 目标仅 0.22 核 | CPU 采样 |
| 块大小不当 | 块全程恒定 64KiB | `RH_PUMP_STATS` |
| cwnd 窗口受限 | 推翻,非主因 | Round-8 |
| 应用层写入节奏 | **确认相关**(后续在此方向解决) | Round-8 |
| 1MiB→2MiB 读缓冲 | ×16↑ **-4.0% 确凿回退** | Round-8 §3.2 |
| yamux 并发流上限 | 超限是**连接级**错误,会杀掉该载体所有流 | `mux-stream-limit-investigation.md` |
| 单核软中断打满 | **证伪**:NET_RX 分散在 7/8 核,最高 29.7% | 单载体实测 |

**当时的核心未解问题**(Round-3 收口时):
> 4 条流时每块 0.86ms、吞吐 2.40;16 条流时每块 2.3~7.3ms、吞吐 2.12。
> CPU 只有 0.68 核、目标只有 0.22 核、通道容量无关、块大小不变。
> ⇒ 剩余唯一可能:16 条 `pump_down` 与 1 个 `RecvHalf` + 4 个 `SendWorker`
> 之间的 **tokio 任务调度延迟**。

这个推断后来被证明是对的(见 §3)。

---

## 3. 后来怎么解决的

2026-10-20 的 `9bd748b`(六轮固定开销优化)与后续工作终结了这场攻坚:

| 优化 | 内容 | 收益 |
|---|---|---|
| 压缩泵 | 泵缓冲/Decoder 提出循环复用,直写 BytesMut spare 区 | 33 块时压侧分配 68→3、解侧 69→2 |
| `pump_down` 变量遮蔽 | `__last_read_end` 重新绑定导致 `LOOP_GAP` 恒 0 | 修复后循环间隙可见 |
| visitor mux 中继 | 两条路径接入 chunk 零拷贝快路径 | STCP +14.6% |
| DNS 缓存 | TTL 缓存 + singleflight | 域名 vhost QPS +50.8% |
| `apply_sync` | O(N²) 判存在 → 哈希查 | 500 规则时 9.6× |
| 控制帧 | 序列化 + 成帧各一次 | 消除整包拷贝 |
| 插件路径 | `read_http_head` 替换逐字节读、TLS acceptor 缓存 | — |

最终结果(2026-10-21,每档 8 次配对):**TCP 单流约 1.9× frp、TLS 约 1.6×,
全部 12 档 ≥1.01×**。

---

## 4. 方法论教训(仍有效)

1. **不要用单轮测量下结论**。当时 5 次配对曾误判「TCP 4 流下行 0.92× 未达标」,
   实为 frp 命中分布上尾;改 8 次配对后 rh 反超到 1.025×。
2. **CPU 条目混进 failures 会误导**。判定器把 note 与 fail 混列,6 条 failures
   里只有 2 条是真判定条件。
3. **"未达标"不等于"有缺陷"**。当时四套 E2E、全量测试、回归全部通过,
   纯粹是吞吐目标未达。
4. **横向对照必须用"改动前代码"**,不能用前几轮的历史结论代替。
5. **测量装置自身会饱和**。iperf3 3.9 单线程与 Python loadgen 的天花板都在
   2 Gbps 附近,接近该量级时必须切 Go loadgen(见 `AGENTS.md`)。

---

## 5. 指向

- **现役性能结论**:[deep-review-performance-report.md](deep-review-performance-report.md)
- **vhost 专项**:[vhost-benchmark-rh-vs-frp.md](vhost-benchmark-rh-vs-frp.md)
- **单载体 85 轮实验原始日志**:已随本次整理删除,`git log -- docs/single-carrier*` 可取回
- **基准套件与复跑方法**:`benchmark/README.md` 与根目录 `AGENTS.md`
