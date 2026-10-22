# 已修复:`http_proxy` / `socks5` 插件语义(2026-10-22 收尾)

> 状态:**已按 frp 语义重写并验收**。原文(问题描述与三个方向)保留在下方
> 「原记录」一节,便于对照;结论与实现见前面几节。

## 结论

`rh-client` 的 `http_proxy` 与 `socks5` 不再是「拨号器」,而是**在客户端
进程内运行的代理服务端**,与 frp 的 `pkg/plugin/client/http_proxy.go` /
`socks5.go` 同构:

```
用户 → 隧道 → rh-client → 【进程内代理服务端】→ 解析用户的 CONNECT / DST.ADDR
                                             → 自己拨目的地
```

**目的地来自用户的代理请求,配置里的 `target_host`/`target_port` 对这两个
插件不再有任何意义。** 修前那版把两者当成同一个地址,于是「连代理 →
CONNECT 代理自己」,对任何真实代理(squid / tinyproxy)都用不了。

## 实现(`crates/rh-client/src/plugins.rs`)

| 环节 | 做法 |
| --- | --- |
| 入口 | `serve_proxy(proto, user, pass)` 建一对 duplex,会话任务持有另一侧;`dial_plugin` 不再接收 `StartForward` |
| HTTP 代理 | 解析请求行与头;`CONNECT` 取 authority,普通请求取 absolute-form 的 authority 并把请求行改写成 origin-form |
| SOCKS5 | 方法协商(需要认证时只接受 `0x02`)→ RFC1929 → 只支持 CONNECT,其它命令回 `0x07` |
| 鉴权 | 恒定时间比较(`rh_transport::constant_time_eq`);用户名为空即免鉴权,与 `static_file` 的 Basic Auth 同口径 |
| 拨号 | 复用 `forward::resolve`(异步 DNS + 缓存)与 10s 超时;失败回 502 / REP `0x04` |
| 转发 | 复用 `forward::splice`(半关闭语义 + 无锁拆半 / 内核 splice 快路径) |
| 多读字节 | 读头一次读 4KiB,`CONNECT` 后紧跟的 TLS ClientHello 必须原样送达目的地 |
| 上限 | 握手 60s(`PROXY_HANDSHAKE_TIMEOUT`,与 frp 的 `ReadHeaderTimeout` 同值) |

新增 15 条单测(`plugins::proxy_tests`),覆盖:CONNECT 到用户目的地、
pipelined 字节、407/凭据正确、502、普通请求改写、SOCKS5 三种 ATYP、
RFC1929 失败、非 CONNECT 命令、地址拆分与 origin-form 归一化。

## 验收

单测之外,**e2e 用例刻意把规则的 `target_host:target_port` 填成不可达的
`127.0.0.1:1`**:只要实现回头去读 target,用例立刻变红。这是防止回归到
自指语义最直接的一把锁。

`tests/e2e-plugins.sh` 的 http_proxy/socks5 两段已重写为 6 条断言:

- http_proxy:CONNECT 到用户指定的 echo 并回显;错误凭据 → 407 +
  `Proxy-Authenticate`;目的地不可达 → 502
- socks5:CONNECT 到用户指定的 echo 并回显;错误凭据 → `0x01 0x01`;
  非 CONNECT 命令 → REP `0x07`

原先的两个「本地代理辅助服务」(python 写的假 squid / 假 socks5)已删除 ——
被测插件自己就是代理服务端,再拿一个假代理去配合它,只会把自指语义
固化进测试。

## 兼容性与行为变化

- **配置**:已有规则里 `http_proxy`/`socks5` 的 `target_host`/`target_port`
  从此被忽略。修前这些规则本来也不工作,所以没有「能用的配置变成不能用」。
- **控制台**:`target` 字段在启用插件时本就隐藏,无需改动;插件说明文案
  补了一句「目的地由用户请求决定」。
- **协议**:`ProxySpec` / `PluginSpec` 零变更,不涉及迁移。

## 与原记录的差异(两处判断修正)

1. 原记录说「需要实现 HTTP `CONNECT host:port` 与 SOCKS5 握手的服务端侧
   解析」,评估 200~300 行。实际还多出两件原记录没提到、但**不做就是新
   bug** 的事:
   - `CONNECT` 之后紧跟的 TLS ClientHello 会被 `read_http_head` 一起读进
     来,必须原样送达目的地(否则表现为「隧道通了但 https 永远超时」);
   - SOCKS5 的 `BIND`/`UDP ASSOCIATE` 与「隧道里的一条 TCP 流」不兼容,
     必须显式回 `0x07`,不能假装成功。
2. 原记录的方向 A(API 层防呆拒绝创建)不再需要:功能已可用,拒绝创建
   反而会挡掉正常用法。

---

## 原记录(2026-10-22,保留作对照)

### 现象:自指

`crates/rh-client/src/plugins.rs` 的 `dial_http_proxy`:

```rust
async fn dial_http_proxy(fwd: &StartForward, user: &str, pass: &str) -> Result<BoxedIo> {
    // 目标字段承载本地代理地址
    let addr = resolve_addr(&fwd.target_host, fwd.target_port).await?;
    let mut s = connect_with_timeout(addr).await?;      // ① 连到代理
    let mut req = format!(
        "CONNECT {host}:{port} HTTP/1.1\r\n...",
        host = fwd.target_host,                          // ② CONNECT 到同一个地址
        port = fwd.target_port
    );
```

`①` 与 `②` 是**同一个** `fwd.target_host`:

```
客户端 → 连上代理 → 发 CONNECT 代理自己 → squid 默认拒绝自连
```

而**真正的目的地(用户想访问的站点)根本没出现在代码里**。
`dial_socks5` 同构:连接与 SOCKS5 握手的目标也是同一个地址。

### 对照 frp 的正确语义

frp 的插件不是"拨号器",而是一个**在内存里跑的代理服务端**
(`benchmark/frp-src-v0.71.0/pkg/plugin/client/`):

```go
// http_proxy.go
listener := NewProxyListener()        // 内存 Listener,不占端口
hp.s = &http.Server{Handler: hp}      // 真正的 HTTP 代理服务端
go hp.s.Serve(listener)

// 隧道把连接塞进这个 Listener
func (hp *HTTPProxy) Handle(conn io.ReadWriteCloser, ...) { hp.l.PutConn(sc) }

// 代理服务端自己解析用户请求、自己拨目的地
remote, err := net.Dial("tcp", req.URL.Host)   // 目的地来自**用户请求**
```

链路:

```
用户 → 隧道 → frp 客户端 → 【内存 Listener】→ http.Server 解析 CONNECT
                                            → net.Dial(用户要的地址)
```

**目的地是用户在 CONNECT 请求里写的**,不是配置里的 `target`。

### 三个可选方向(当时)

| 方向 | 代价 | 说明 |
|---|---|---|
| A. 防呆 | 小 | 在 API 层拒绝创建这类插件规则,避免用户踩坑 |
| B. 按 frp 语义重写 | 大 | 推荐方向 |
| C. 保持现状 | 0 | 不可接受:已知不可用的功能,等于埋雷 |

最终选 B(见上)。
