# 待办:`http_proxy` / `socks5` 插件语义需要重写

> 状态:**未修复,已知不可用**。记录于 2026-10-22 的静态审查收尾。
> 本文只记录问题与方案,不含实现。

## 结论先行

`rh-client` 的 `http_proxy` 与 `socks5` 两个插件**功能上从未正确工作过**。
不是回归,是初版就没实现对。

对任何**真实的** HTTP/SOCKS 代理(squid / tinyproxy 等)都用不了;
只有配一个"允许自连"的代理时才会"跑通",而那是一条没有意义的回环,
并不会真的转发用户流量。

## 现象:自指

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
`dial_socks5` 同构:连接与 SOCKS 握手的目标也是同一个地址。

## 对照 frp 的正确语义

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

## 为什么不能"改几行"

当前实现缺的正是最关键的一环:**没有人在解析用户的代理请求**。
要修得对,至少需要:

1. 实现 HTTP `CONNECT host:port` 与 SOCKS5 握手的**服务端**侧解析;
2. 用解析出的地址 `dial`;
3. 双向转发(可复用 `forward::splice`)。

等于重新实现一个代理,约 200~300 行。

## 验收方式(必须)

**不能只靠单测**:自指问题的本质是"与真实代理不兼容",必须拿
squid 或 tinyproxy 实测:

1. 起一个 squid,禁止自连(默认配置即可);
2. 配置 `http_proxy` 插件指向它;
3. 经隧道访问一个外部站点;
4. 断言:能拿到真实响应,且 squid 日志里能看到**用户请求的域名**
   (而不是代理自己的地址)。

## 三个可选方向

| 方向 | 代价 | 说明 |
|---|---|---|
| **A. 防呆** | 小 | 在 API 层拒绝创建这类插件规则,或配置里明确标注"暂不支持",避免用户踩坑 |
| **B. 按 frp 语义重写** | 大(200~300 行 + 真实代理验收) | 推荐方向 |
| **C. 保持现状** | 0 | **不可接受**:这是已知不可用的功能,等于埋雷 |

建议先做 A 止血(几行),再排期做 B。

## 相关背景

这条是 2026-10-22 那轮静态审查(A 组 A5)留下的唯一"需要重做语义"的项。
同轮其他项的处理情况见各提交信息;另外两项未修的是:

- **B9** 打洞/sudp 回包方向由服务端指定 —— 判定为**设计使然**(UDP 打洞
  的协议本质就是协调方指定候选),不是缺陷;
- **B11** vhost 缓存挂 proxy 级 —— B2 修好键(含 host)后已无正确性问题,
  剩余差异只是容量共享。
