# 明确不支持的能力(与 frp 对齐)

> **状态:结论已定,不再作为待办。**
>
> 本文记录**经过源码核实**后判定「不做」的能力。目的是防止这些项被反复
> 当成"遗漏"或"待补"重新提起 —— 每一项都写清了为什么不做、依据是什么。
>
> 判定原则:**frp 做不到的,我们也不做**(功能基线对齐 frp v0.71.0)。
> 少数项 frp 能做但我们不做,会单独注明理由。

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

## 4. 相关但**不属于**本文范围的项

以下曾被误认为"不支持",实际是设计使然而非缺陷,一并说明以免混淆:

| 项 | 说明 |
|---|---|
| HTTP 代理的 keep-alive | 一条通道一次请求,强制 `Connection: close`。与「隧道里一条 TCP 流 = 一条代理会话」的模型一致,不是缺陷 |
| HTTP 代理的 `https://` absolute-form | 极少见;缺端口时按 80 补(见 `serve_http_proxy` 注释)。CONNECT 才是 HTTPS 的正常路径 |
| `http_proxy`/`socks5` 忽略配置里的 `target_host`/`target_port` | **这是正确语义** —— 目的地来自用户请求。见 [fixed-plugin-proxy-semantics.md](fixed-plugin-proxy-semantics.md) |

---

## 变更记录

| 日期 | 内容 |
|---|---|
| 2026-10-22 | 建立本文。SOCKS5 BIND / UDP ASSOCIATE / 插件级代理链三项,经 frp 源码核实后判定不做 |
