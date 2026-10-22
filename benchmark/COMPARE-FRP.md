# rabbithole vs frp 性能对比方案

> **时效说明(2026-10-22 补注)**:本文是 2026-10-12 定下的**对比方法学**,
> 其中的公平性原则(只换被测软件、对等配置、同一直连基线)至今有效,
> `benchmark/peer-frp.sh` 即按此实现。文中"被测版本 v0.3.0"是当时的取值,
> 现役对 frp 的结论见 `docs/deep-review-performance-report.md`(2026-10-21)。

## 1. 目标

在与 rabbithole 基准完全相同的条件下测量 frp v0.71.0(rabbithole README 声明对标的
基线版本),得到逐指标的「rabbithole / frp / 直连基线」三方对比。

## 2. 公平性原则(方案的核心)

| 原则 | 落实方式 |
|---|---|
| **只换被测软件** | 同三台机器、同角色分工、同目标服务(target.py/iperf3)、同压测端、同端口规划、同场景与顺序、同就绪门控 |
| **对等配置** | 传输多路复用:rabbithole `multiplex=true` ↔ frp `tcpMux=true`;连接池:`pool_size=4` ↔ `poolCount=4`;压缩:双方默认关;日志:双方 warn;fd:双方启动前提升到 65535 |
| **同一直连基线** | 直连(压测端→B 目标,不经任何隧道)每轮重测,两侧共用同一目标端口与工具 |
| **版本固定可复现** | rabbithole v0.3.0(release,sha256 校验);frp v0.71.0(官方 release;frp 不提供 sidecar 校验文件,记录实测 sha256 于结果目录) |
| **逐轮对齐** | 每种控制面协议各跑一轮,frp 与 rabbithole 轮次相邻执行,避免环境漂移 |

## 3. 拓扑与端口(与 rabbithole 基准完全一致)

```
[test3: 压测端]           [test1: 服务端]              [test2: 客户端+目标]
iperf3 -c  ──10GbE──►  frps :17100  ──隧道──►  frpc ─┬─ iperf3 -s :19101
loadgen.py             :20001 tcp 转发口              ├─ target.py :19102/19103
                       :20003 udp                      └─ http     :19104
                       :20004 vhost http
直连基线: test3 ──直接──► test2 :1910x(不经任何隧道)
```

## 4. 控制面协议映射

| rabbithole PROTO | frp transport.protocol | 备注 |
|---|---|---|
| tcp | tcp | 双方默认 |
| tls | tcp + `transport.tls.enable=true` | frp 的 tls 是在 tcp 通道上做 TLS |
| ws | websocket | |
| wss | wss | |
| quic | quic(server 需 `quicBindPort`) | |
| kcp | kcp(server 需 `kcpBindPort`) | |

注:frp 的 TLS 是「tcp+TLS」,与 rabbithole 的 tls 语义对齐;两侧 ws/wss 同理。

## 5. 场景矩阵(与 rabbithole peer 基准逐项相同)

| # | 场景 | 参数 | 基线 |
|---|---|---|---|
| 0 | 就绪门控 | PING 通过隧道才开始,≤30s | — |
| 1 | 吞吐 | iperf3,上/下行 × 1/4/16 流 × 10s | 直连同目标 |
| 2 | 延迟 | 5000 次往返 + 500 次新建连接 | 直连 |
| 3 | 建连速率 | 3000 次 connect+PING+close,64 并发 | 直连 |
| 4 | UDP | 1200B × 8s,窗口 32 | 直连 |
| 5 | HTTP vhost | 16 连接 × 8s,Host=bench.test | 直连 |
| 6 | 并发连接 | 64/256/1024/4096(仅 tcp 轮) | 直连同轮 |

## 6. 执行流程

```bash
# 0) 分发:三台机器各放一份 benchmark/(含 frp 与 rabbithole 二进制下载逻辑)
#    依赖:python3 / iperf3 / jq / curl(不安装系统环境)

# 1) test1  $ PROTO=tcp bash peer-frp.sh server     # 起 frps + 记录版本/sha256
# 2) test2  $ PROTO=tcp bash peer-frp.sh client <A_IP>
# 3) test3  $ PROTO=tcp bash peer-frp.sh drive <A_IP> <B_IP> tcp
# 4) 对每种 PROTO ∈ {tls,ws,wss,quic,kcp} 重复 1-3(PEER_CONCURRENT=0)
# 5) 汇总: py/compare.py <rabbithole结果目录> <frp结果目录>
```

无凭据传递(frps/frpc 用固定 bench token),编排比 rabbithole 轮更简单。

## 7. 输出与呈现

- 原始:frp 侧 `results-<proto>.jsonl`(记录格式与 rabbithole 完全一致)
- 对比:`COMPARE.md`,每个指标一行:

```
| 指标 | 协议 | 直连 | rabbithole | frp | RH/frp |
```

- 判读:保留率(隧道/直连)看各自实现效率;RH/frp 看相对差距;双方都对照同一基线。

## 8. 边界与已知差异(判读时必须考虑)

1. **frp 的规则在客户端配置**(frpc.toml 静态声明),rabbithole 是服务端集中下发;
   本对比只测数据面,不对比控制面 API/规则下发能力。
2. frp 心跳/保活参数与 rabbithole 不同(双方均用默认值),空载控制面开销略有差异。
3. frp kcp/quic 与 rabbithole 实现不同但协议相同,对比的是「同一传输协议下的实现效率」。
4. frp 不提供 checksums sidecar,以「固定 tag + 实测 sha256 记录」保证可复现。
5. 直连基线受压测端 CPU 影响(此前实测 ~2.3-3.4 Gbps),对比以保留率为主。
