# 性能修复状态(2026-10-14,c84f631 收口轮 full-accept 后)

> **⚠ 已被后续测量取代(2026-10-22 补注)**。本文的 G1/G2/G6 判定基于
> 2026-10-14 的构建,其"未达标"结论**不代表现役版本**。此后:
>
> - 2026-10-15 ~ 10-18:多载体/单载体瓶颈定位(`single-carrier-bottleneck-measurement.md`);
> - 2026-10-20:`ffd49d3` 七路深审修复与六轮固定开销优化(`9bd748b`);
> - 2026-10-21:**TCP/TLS 全部 12 档吞吐超过 frp**(`deep-review-performance-report.md`)。
>
> 现役性能口径以 `docs/deep-review-performance-report.md` 与
> `docs/vhost-benchmark-rh-vs-frp.md` 为准。本文保留为当时证据链与判定过程记录。

**结论:固定性能目标仍未完成。**基线 `c84f63174cd2ebafd994c4fafacd5beba4a0240b`(分支 `fix/performance-closure-c84f631`,HEAD 即该检查点,本轮**无新提交**;旧版"无 git 提交、HEAD 仍 `c3ca4bd`"的说法已随检查点落库而过期)。本轮在既有授权内完成:风险项 A–E 全部修复并红→绿、验收装置落地、三机 full-accept(candidate-r0)真实执行——四套 E2E 全绿、Linux 全 workspace 测试/构建 exit=0、六协议全矩阵逐条测量;但 **G1/G2/G6 及 G5 的 kcp 单缺口未达标**(§3),candidatePassed=false,不以单项改善替代结论。上一版 79 行已原样存档 [performance-repair-history-4.md](performance-repair-history-4.md)(SHA256 `d81a6729…`,cp 后 shasum 核验);更早为 history-3/history-2/history.md,后缀唯一、均不再重写。本轮明细见 [performance-closure-c84-report.md](performance-closure-c84-report.md)。

## 0. 授权边界(更正旧版 §7)

- 原授权已覆盖红线内修复、三机测试与最终完整验收(E2E+六协议全矩阵)——candidate-r0 即按此执行完毕。旧版"完整验收重跑……现有授权未覆盖"为虚假阻断,已删除。
- 仍需另行授权的仅剩:sysctl(rmem_max/wmem_max 等)、网卡/拓扑变更、内核升级、权限提升。

## 1. 当前源码与身份(本会话实读/实测)

- `git log/rev-parse`:HEAD=`c84f63174cd2…`,无新 commit;本轮改动全部为工作树未提交状态(kcp.rs、api.rs、state.rs、data.rs、loadgen.py、kcp_loopback_bench.rs、tests/tcp_listener_lifecycle.rs,新增 benchmark/closure_c84.py、test_closure_c84.py、两份 docs)。
- kcp.rs:基线 `16b72155…`(=baseline-local 快照)→ 当前工作树 `610dd1e9…`(shasum 实测,含 A/B/C/D+关闭假成功修正);candidate-r0 构建的 kcp.rs 与当前逐字节一致(独立复核抽查,报告 §3)。
- candidate-r0(最终 Linux 构建):tree_hash `6e8f720b…`(build-inputs 80 项)、sources.sha256 `24f655e9…`(76 文件)、rh-server `2912ab8c…`、rh-client `dd1ecb18…`;rustc/cargo 1.98.1、内核 5.15.0-47、nofile 65535(run-index.json)。
- **时序注意**:candidate-r0 快照=16:25–18:04;其后 20:04–20:41 的 test-only 修(listener 探针)与会话失败计数器(state.rs/data.rs)不在该构建内,只做了定向回归与本机全量(§4),未重跑强验收。

## 2. 本轮五项修复(A–E)与追加项(红绿明细见报告 §1–§2)

| 项 | 内容 | 红 → 绿(路径在报告 §2) |
|---|---|---|
| A | KCP poll_shutdown 只关本地写侧(半关闭保读,修 F5) | 基线 0/1 exit101 → 1/1 EXIT=0 |
| B | unread read_buf 不刷新 idle(停滞有界回收,修 F6);idle 终止不得伪报成功关闭 | 基线 0/2 → 1/1;候选期新红 0/1 exit101 → 1/1 |
| C | PunchOutput WouldBlock 有界队列,复用 connect 的 UdpBacklog/drain(修 F9) | 基线 repro 5 failed → 67/0;**c2 守卫首两轮 66/1 偶发红、第三轮绿,未闭环** |
| D | ACK 合批(recv_many,按 batching-plan 修订版) | 单因果对照 t1 306.9→664.9、t4 317.3→644.6 Mbps,CPU/2、flush/10、低载 p50 持平;保留 |
| E | 2007/3005 区分自身/外部端口占用(api.rs/state.rs) | 新守护 8 例+全 bin 20/20,api:: 连续 5 次全绿(修前无测试,属允许的"修前即绿") |

追加:**F** 验收装置 lab(closure_c84.py 自检 14/14、stdlib 单测、linux-baseline attempt1 EMFILE 原样保留、r2 targeted-baseline test/build exit=0);**G** TCP listener 测试混杂修复(Linux 3/10 挂起→test-only 因果化修,linux-fix-r2 10×ROUND_EXIT=0;机理未定论,旧失败日志保留);**H** 会话失败计数器 rabbithole_session_failures_total(rh-server 23/0,为 G5 1/12288 归因)。

## 3. 固定门槛 G1–G7(candidate-r0 实测;verify_goal exit=1,明细报告 §5)

- **G1 TCP nomux 单流 ≥3000:FAIL**——2077/2156 Mbps;CPU 分量达标(server 28.1–30.2%、client 14.6–34.6% 单核,覆盖 98–99%≥95%)→瓶颈不在 CPU。直连同窗口中位 upload 3182/download 2957(download 直连亦 <3000)。
- **G2 TLS mux 单流 ≥2500:FAIL**——1229/1801 Mbps。
- **G3 六协议单流正吞吐:PASS**(mux=1,relay=splice)。
- **G4 1200B UDP 损失<1%:PASS**——六协议中位 0%(各 n=3,排空去重,sent==recv_unique)。
- **G5 4096 verified+5 轮保持:FAIL 仅 kcp**——verified=[4095,4096,4096](s3 建连 4096、应用层首验 4095,保持期未恢复,1/12288);tcp/tls/ws/wss/quic-mux 与 tcp-nomux 对照均 4096×3。
- **G6 配对保留率 ≥80%:FAIL**——verify-goal-report 原件逐条 22/36 条件 <80%(独立复核概述称 21,以原件为准):tcp/tls/ws 单流 34–67%、wss 另含 4 流 52.6–72.3%、quic 全档 19.9–29.9%、kcp 全档 13.6–22.9%。
- **G7 ≥3 次有效配对:PASS**——每条件 3/3、invalid=0;独立抽查 252 个 raw exit 全 0、窗口重叠 0。
- 机械结果:accept runner exit=0(5955s);closure verify 18/19(exit=1);verify_goal exit=1。

## 4. 未解决/未验证(如实,不以计划替代)

1. G1/G2/G6 软件侧缺口未修:本轮对吞吐路径**零改动**(诊断对照 refsplice/copy 与 rh 同噪声带,无热点证据,不伪造归因);候选方向见报告 §6/§7。
2. G5 kcp 1/12288 原因未定(不能写"超窗瞬态");下次运行带 H 计数器归因。
3. listener Linux 挂起机理未定论(内核拆除竞态孤儿未排除);10/10 绿非数学证明;旧 3/10 失败日志并存保留。
4. C 项 c2_punch 守卫偶发(3 轮中 2 红)未根治。
5. termark exec 60s/-1 根因在工具侧不可查;已按"不判死+pid 文件恢复"处置(accept 内 11 次恢复均成功);该新增恢复路径未被深审(独立复核观察)。
6. candidate-r0 之后的增量改动未重新强验收(本机 `cargo test --workspace` 15 套 206/0/2、clippy exit=0 为 mac 定向;Linux 全量以 candidate-r0 为准)。
7. tcp-nomux 16s upload 时 client CPU 66–69% 接近饱和——解读 G6 多流时不得全归因转发路径。

## 5. 需用户决定(未授权不执行)

1. sysctl 解锁实验;2. 网卡/拓扑变更;3. 内核升级;4. 权限提升。软件侧可证伪实验(单流转发路径归因、quic/kcp 用户态旋钮 targeted A/B)红线内可继续,无需另授权。

## 证据索引(路径即结论,全文见报告)

- 最终验收:`benchmark/runs/performance-closure-c84/candidate-r0/`(run-index.json、verify-goal-report.txt、evidence/×7、logs/、events.jsonl)。
- 机械检查:`…/checks/{linux-full-accept-atoctbgz,linux-artifact-verify-m3l3wbp3,fixed-performance-goal-xxdxm43w,optimized-workspace-1d0gosuv,optimized-clippy-soaiwyhy}`。
- A–C 红绿:`…/main-independent/`、`…/kcp/{red,candidate-tests,shutdown-guard}/`;D:`…/kcp/counter-ab-isolated/analysis.json`;E:`…/ports/`。
- 装置:`…/lab/README.md`;基线:`…/baseline-local/`、`…/linux-baseline(-r2)/`;listener:`…/{listener-probe,tcp-cancel-race}/`;计数器:`…/session-failure-counter/`。
- 旧证据(goal-repair/kcp-boundary 等)仅背景,索引见 history-4 §证据索引;本文件不重复其内容。
