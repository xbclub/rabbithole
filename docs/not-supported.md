# 明确不支持的能力(与 frp 对齐)

> **状态:结论已定,不再作为待办。**
>
> 本文记录**经过源码核实**后判定「不做」的能力。目的是防止这些项被反复
> 当成"遗漏"或"待补"重新提起 —— 每一项都写清了为什么不做、依据是什么。
>
> 判定原则:**frp 做不到的,我们也不做**(功能基线对齐 frp v0.71.0)。
> 少数项 frp 能做但我们不做,会单独注明理由。
>
> 2026-10 起同时收录**内部性能取舍**类「不做」判定(§6~§11,标注
> 「内部判定」):与 frp 无关,依据是 `docs/perf-static-analysis-tasklist-2026-10.md`
> 对应条目的反证与三机基准数据,防止被当成遗漏重新提起。

---

## 1. SOCKS5 `BIND`(命令 0x02)

**不做。**

### 依据:frp 同样不支持,且是上游库的未实现项

frp 的 socks5 插件直接使用 `github.com/armon/go-socks5`
(`pkg/plugin/client/socks5.go:24`),版本 `v0.0.0-20160902184237-e75332964ef5`
(`benchmark/frp-src-v0.71.0/go.mod:6`)。

该库 `request.go` 的命令分发:

```go
switch req.Command {
case ConnectCommand:   return s.handleConnect(ctx, conn, req)
case BindCommand:      return s.handleBind(ctx, conn, req)
case AssociateCommand: return s.handleAssociate(ctx, conn, req)
```

而 `handleBind` 的**完整实现**是:

```go
func (s *Server) handleBind(ctx context.Context, conn conn, req *Request) error {
	if ctx_, ok := s.config.Rules.Allow(ctx, req); !ok {
		... 回 ruleFailure ...
	}
	// TODO: Support bind
	if err := sendReply(conn, commandNotSupported, nil); err != nil {
		return fmt.Errorf("Failed to send reply: %v", err)
	}
	return nil
}
```

**除了回 `commandNotSupported`(0x07)之外什么都没做**,且带着从 2016 年
至今未实现的 `TODO`。frp 侧也没有任何补救:`socks5.go` 只调
`sp.Server.ServeConn(wrapConn)`,没有自定义 `Rules`、没有 `Dial`、没有
绕开库自己处理 BIND。

> 核实方式:在联网机器上 `git clone` 该库,HEAD 为
> `e75332964ef517daa070d7c38a9466a0d687e0a5`,与 frp `go.mod` 记录的
> `e75332964ef5` **是同一个 commit**。

### 技术上为什么不成立

BIND 要求代理**监听一个端口**等待第三方连入,并在 `REP` 里返回
`BND.ADDR`/`BND.PORT` 供客户端告知对端。而本项目的代理插件运行在
**客户端进程内**,用户经隧道连入:

- 插件**没有可回连的公网入口** —— 我们的 `socks5_reply()` 只能填
  `0.0.0.0:0`,正是因为"隧道出口没有可回连的绑定地址";
- 要真正支持,需要在**服务端**开一个端口并与插件关联 —— 那是另一套架构
  (接近 frp 的 stcp visitor 模型),不是"补一个命令分支"。

### 我们的现状

`crates/rh-client/src/plugins.rs` 的 `socks5_handshake` 对非 CONNECT
命令回 `0x07`。**这是 RFC 1928 规定的正确行为**,不是缺陷。

已由 `tests/e2e-plugins.sh` 的「socks5:非 CONNECT 命令回 REP=0x07」
与单测 `socks5_rejects_non_connect_command` 锁定。

---

## 2. SOCKS5 `UDP ASSOCIATE`(命令 0x03)

**不做。**

### 依据:同上,frp 不支持

`handleAssociate` 与 `handleBind` 结构完全相同 —— 一句
`// TODO: Support associate` 加无条件 `commandNotSupported`。

### 技术上为什么不成立

UDP ASSOCIATE 要求代理开一个 **UDP 中继端口**,之后客户端用
`RSV FRAG ATYP DST.ADDR DST.PORT DATA` 格式的 UDP 报文通信。这需要:

1. 一条**独立的 UDP 通道**(本项目的这条路径是 TCP 流;UDP 走
   `udpchan` / SUDP 那套完全不同的机制);
2. 解析并转发 SOCKS5 的 UDP 封装帧;
3. 维护 UDP 关联表(客户端源地址 ↔ 隧道通道 ↔ 目标)。

估算 300~500 行 + 一条新通道类型。而 frp 连 CONNECT 之外的命令都没做。

### 我们的现状

同 §1,回 `0x07`。

---

## 3. 插件级代理链(插件再连一个上游代理)

**不做。**

### 依据:frp 没有这个能力

frp 的**代理链能力在传输层,不在插件层**:

| 场景 | frp | 本项目 |
|---|---|---|
| 客户端 → 服务端 的载体走代理 | ✅ `transport.proxyURL`(frpc 配置) | ✅ `rh_transport::proxy_dial`(已实现,支持 `http://` / `socks5://`) |
| 插件(作为代理服务端)再连上游代理 | ❌ 无此配置项 | ❌ 无此字段 |

frp 的 `Socks5PluginOptions` 只有 `Username` / `Password`
(`pkg/config/v1/proxy_plugin.go:183-187`),`HTTPProxyPluginOptions` 同理,
**没有任何 upstream/proxy 字段**。插件解析出用户请求的目的地后,直接
`net.Dial` / `http.DefaultTransport.RoundTrip`,不经第二个代理。

### 如果将来要做

改动面不大(已评估):

| 位置 | 改动 |
|---|---|
| `rh-protocol/src/proxy.rs` | `PluginSpec::HttpProxy` / `Socks5` 各加 `#[serde(default)] upstream_proxy: String` |
| `rh-client/src/plugins.rs` | `dial_proxy_destination` 改走代理,复用已有的 `rh_transport::proxy_dial`(约 10 行) |
| `console/src/features/services/matrix.ts` | 两个插件各加一个 `pluginText` |

但**语义要先定清楚**,否则会做成半成品:

- **A. 全局单跳**:该插件所有流量都经同一个上游代理 —— 加一个字段即可;
- **B. 按目的地分流**:某些域名走代理、某些直连 —— 需要分流规则,不是加字段能解决;
- **C. 多级链**:A → B → C —— 需要链表结构。

**在 frp 补齐之前,本项目按「与 frp 对齐」原则不做。**

---

## 4. `virtual_net` 插件(第 10 个客户端插件)

**不做(已搁置)。**

### 背景

`README.md` 写「10 种客户端插件」,但 `PluginKind` 只有 9 个变体。这个数字是
按 **frp v0.71.0 基线**数的(frp 在 `pkg/plugin/client/` 下注册了 10 个),
不是按本项目实现数的。

### 为什么不做的理由与别的项不同

前三条都是「frp 也做不到」。这一条是 **frp 能做,但我们判定不值得做** ——
用户于 2026-10-22 明确决定搁置。

技术上它是**唯一一个需要服务端参与数据面路由的插件**:不是「拿到连接 →
转发 → 结束」,而是要开 TUN 设备、读 IP 包、维护 IP 路由表。

### 核实过的三个关键事实(将来重启必读)

1. **frp 的 vnet 是「两个插件配对」,不是一个插件**:
   proxy 侧(`pkg/plugin/client/virtual_net.go`,92 行)+ **visitor 侧**
   (`pkg/plugin/visitor/virtual_net.go`,约 210 行)。后者建 `/32` host
   route,是真正的入口。只做 proxy 侧,`clientRouter` 恒为空表,所有包落到
   `no route found` —— 功能等于不存在。
2. **本项目没有 visitor 插件维度**:`VisitorKind` 只有 `Stcp/Sudp/Xtcp`
   (`crates/rh-protocol/src/visitor.rs:8`)。要支持 vnet visitor 得给
   `VisitorSpec` 加插件字段,属 RHTP 协议变更。
3. **需要 root + 新依赖类别**:frp 的 `virtualNet.address` 是客户端配置项
   (`config/v1/client.go:66`),要求 root 运行;仓库当前零 TUN 依赖。
   Windows 上 frp 自己也返回错误(`tun_unsupported.go`),对齐即可。

### 现状

- `PluginKind` 仍为 9 个变体,**未加** `VirtualNet`;
- `requirements-matrix.md` X1 保持 `experimental`;
- 任务书与完整核实结论见 [HANDOFF-virtual-net-plugin.md](HANDOFF-virtual-net-plugin.md)。

---

## 5. 相关但**不属于**本文范围的项

以下曾被误认为"不支持",实际是设计使然而非缺陷,一并说明以免混淆:

| 项 | 说明 |
|---|---|
| HTTP 代理的 keep-alive | 一条通道一次请求,强制 `Connection: close`。与「隧道里一条 TCP 流 = 一条代理会话」的模型一致,不是缺陷 |
| HTTP 代理的 `https://` absolute-form | 极少见;缺端口时按 80 补(见 `serve_http_proxy` 注释)。CONNECT 才是 HTTPS 的正常路径 |
| `http_proxy`/`socks5` 忽略配置里的 `target_host`/`target_port` | **这是正确语义** —— 目的地来自用户请求。见 [fixed-plugin-proxy-semantics.md](fixed-plugin-proxy-semantics.md) |

---

## 6. xtcp 直连按 name 线性扫规则表(by_name 副索引)

**不做。**(内部判定,非 frp 对齐)

### 依据:风险与收益不成比例(任务书 E-3,判据入库 @ 5de8773)

xtcp 是打洞直连低频路径:`nathole_cli.rs` 的 `find_by_name`(审查时点
:310-315)每接受一条流做一次 O(N) 字符串比较,且**临界区内无 await**,
不拖停其他分支。改造 by_name 副索引需动 8 处 `.proxies` 表访问,并在
3 个写点(upsert / apply_sync / remove)永久维护双表一致性 —— 用一个
一致性维护面去优化一个 almost-never-hit 的低频查询,不值。

### 我们的现状

维持 id 为键的单表 + 顺序扫描;xtcp 直连功能本身完整,无行为缺陷。

---

## 7. 限速路径每块两次装箱分配(limiter 去装箱)

**不做。**(内部判定,非 frp 对齐)

### 依据:默认形态已最优,限速场景收益可忽略(任务书 B-5,判据入库 @ d1ccf6e)

默认装配不装 limiter 时本就零开销(最优形态);收益仅在限速激活场景,
~10k allocs/s @10G 的量级在限速瓶颈(令牌桶本身)里可忽略。而消掉
BoxFuture + Box<ChunkReservation> 需要改 `ChunkLimiter` trait 签名
(reserve 返回具体 future),同步修改 `data.rs` BucketLimiter、
`forward.rs` ClientBucketLimiter 全部实现方 —— 改动面远大于收益
(任务书自标"可选")。

### 我们的现状

限速开启/关闭的转发测试通过;限速路径行为不变
(`splice.rs:1028-1037` 审查时点)。

---

## 8. yamux `to_send` 全连接队列有界化

**不做。**(内部判定,非 frp 对齐)

### 依据:间接反压已钉住上限,有界化等于撤销既有热路径优化(任务书 C-7,判据入库 @ 2623c4e)

复核确认间接反压链成立:WU 堆积 → 对端窗口耗尽 → 写侧停发 → 读侧
断粮,稳态上限 ≈ 3 帧 × 活跃流数(~100KB),**不是实际无界**。而
有界化要求 WU 快车道回落到有界通路,该路径必须"先 poll_ready 再
记账" —— `stream.rs:558` 注释明言该模式与单锁热路径不相容(正是
当年为消掉它才直压 to_send)。重新引入等于撤销既有优化,换一个已
被间接钉住的内存上限。

### 我们的现状

`connection.rs:1303` 审查时点维持无界 to_send + 间接反压;
停滞场景内存上限由流数 × 窗口自然约束。

---

## 9. yamux Framer 每帧固定开销(take_error 锁 + workers_gone 遍历)

**不做(已量化归档)。**(内部判定,非 frp 对齐)

### 依据:三机基准证明该量级在端到端不可见(任务书 C-9,归档 @ 0f572ba)

2026-10-26 三机基准(peer.sh,REPEAT=5×15s):yamux 路径(tcp+MUX=1)
的吞吐/延迟/CPU 与基线**全部持平** —— C-3~C-8 的帧路径优化尚且测不出
端到端差异,C-9(单帧 ~50-100ns,低于原发现主张)量级更小,不可能
单独可见。证据目录与数据见任务书「三机基准结论」节。

### 我们的现状

C-3 已把 error 热检换成 `error_hot()` 原子门(残余开销仅 workers_gone
遍历);`connection.rs:1916` 审查时点形态保持不变。若未来基准显示
yamux 帧路径出现可测热点,再按任务书 C-9 的量化建议②单独评估
workers_gone 快照化。

---

## 10. KCP 服务端 demux recvmmsg 批收

**不做(经基准回退)。**(内部判定,非 frp 对齐)

### 依据:实测加每包延迟、零吞吐收益(任务书 A-5,回退 @ 0801505)

2026-10-26 三机基准二分:recvmmsg 批收结构对回显窗口型 UDP 载荷
(1200B×window32)每程加 ~110µs 固定延迟(隧道 UDP 96.9→77.4 Mbps,
RTT p50 3.16→3.82ms);而 agg 关闭对照轮的 **TCP 吞吐与基线持平**
(16流 729 vs 728-730;其 UDP 78.4 仍与候选 76.2 同陷回归)——
批收的 syscall 摊薄在本基准全部档位买不回该代价。
回退后 TCP 增益(+5~8%,来自 sendmmsg 聚合)完整保留。

### 我们的现状

demux 统一为 tokio 原生 `recv_from` 逐包收(唤醒点即收包点,无就绪
缓存簿记),`demux_route_one` 抽取与报文池化保留。若未来单连接 pps
量级再上一个数量级且火焰图显示收包 syscall 是热点,需先设计不清
就绪缓存的批收形态(如 async_fd 风格)并按任务书「三机基准结论」
节的二分矩阵流程验证延迟无回归后再启用。

---

## 11. mux_relay 回落侧的 tokio::io::split 拆半(B-2 后半)

**不做。**(内部判定,非 frp 对齐)

### 依据:跨 crate 剥壳不可行,残余影响弱(任务书 B-2,判据入库 @ d1ccf6e)

建议的修法是"对包装类型剥壳/downcast 拆半走 mux_relay_split",但
包装类型(CountedUserIo/QuotaIo)定义在 rh-server / rh-client,
rh-transport 无从命名,downcast 目标不存在。而真正热的路径(服务端)
已先剥壳走 mux_relay_split(mux.rs:634 注释,审查时点);残余的
Err 回落调用仅剩客户端插件目标回落,锁内是本机 TLS/socket IO,
任务书复核自证影响弱 —— 改动面与收益不成比例。

### 我们的现状

B-2 的 compress 侧已完成(两条独立单向 duplex,`JoinDuplex` 拼合,
@ 2416b8a);mux_relay 回落侧维持共享锁双泵形态,行为不变。

---

## 变更记录

| 日期 | 内容 |
|---|---|
| 2026-10-22 | 建立本文。SOCKS5 BIND / UDP ASSOCIATE / 插件级代理链三项,经 frp 源码核实后判定不做 |
| 2026-10-22 | 增补 §4:`virtual_net` 插件。用户判定不重要、暂时搁置;同时把 `README.md` 的「10 种客户端插件」改为「9 种」(实测 `PluginKind` 就是 9 个变体) |
| 2026-10-26 | 增补 §6~§9:2026-10 性能任务书的四条「不做」内部判定(E-3 by_name 副索引 / B-5 limiter 去装箱 / C-7 to_send 有界化 / C-9 Framer 每帧开销,后者经三机基准量化归档) |
| 2026-10-26 | 增补 §10:A-5 服务端 demux recvmmsg 批收,经三机基准回退(UDP 每包延迟 -19% 吞吐、零收益;回退提交 0801505) |
| 2026-10-26 | 复核修正:D-15 hash 归属(9b6c3ca→62470a5)、三处「持平」措辞按指标显式化;补登 §11 B-2 mux_relay 回落侧;C-9 任务书状态翻牌为不做 |
