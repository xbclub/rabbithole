# 交接:2026-10-22 静态审查与修复进度

> 本文是压缩上下文前的交接记录。按"已完成 / 未完成 / 待决策"三段组织。

## 一、已完成(7 个提交,均在 main,未推送)

```
5e6de9a fix(client): P9/P10 仲裁 —— 我的"误报"判定错了,两项均成立并已修
d8b7d07 fix: 把最后 9 项核实完 —— 修 7 项,B9/B11 判定为设计使然而非缺陷
ce707a5 fix: 补齐 B 组与 P3 组核实 —— 修 8 项,澄清 2 项误报
58d7c3c fix: 修 B2 跨域缓存串号、B3 分组缓存永不命中、B4 缓存指令、B5 源地址伪造
fe47dd0 fix(server): 修 A1/A2 自锁、A3 记账缺口、A4 UDP 单边限流、A6-A9
7b1366b fix(transport): 握手背压真正前移到 accept 之前
81cc9eb docs(client): 如实改写 dial_target 的注释(它不是 SSRF 修复)
```

更早的 `07b1aa2`(多载体证书绑定等)与 `ae04257` 已推送。

### 状态

- 测试:**543 通过 / 0 失败 / 2 忽略**
- clippy:`--workspace --all-targets -D warnings` 零错误
- 工作区干净
- 三台测试机已清理

### 这批修复覆盖的清单

1 P0(登录限速键含端口)+ 14 P1 + 约 19 项 P2/P3,全部核实完毕。

## 二、未完成(3 项)

### 1. `http_proxy`/`socks5` 插件语义(唯一需要重做功能的项)

**详见 [`docs/pending-plugin-proxy-semantics.md`](pending-plugin-proxy-semantics.md)**。

一句话:两个插件把代理地址同时当"连接目标"与"CONNECT 目的地",自指;
真实目的地在用户请求里,而当前代码没有任何东西解析它。
frp 的插件是"内存里的代理服务端",我们是"拨号器",语义错位。

**建议**:先做防呆(API 层拒绝创建),再排期按 frp 语义重写 + 拿 squid 实测。

### 2. B9 打洞方向由服务端指定 —— 判定为设计使然

`nathole_cli.rs` 按服务端下发的 `role` 决定打洞方向;`udpchan.rs` 从帧里
的 `src` 解析地址并回发 UDP。这是 **UDP 打洞的协议本质**(协调方指定候选、
双端互发探测包)。

清单所说"可被用作 UDP 探测器"在"服务端不可信"模型下成立,但那个模型下
整个隧道都不可信(服务端可任意指定目标,用户流量本就全经它)。
信任边界是**服务端本身可信**(客户端用 secret / TLS pin / CA 认证)。

**若要收紧**:协议层加候选白名单(只允许打洞会话中交换过的地址),属新特性。

### 3. B11 vhost 缓存挂 proxy 级 —— 非正确性问题

`rt.http_cache` 是 `ProxyRuntime` 字段,一个 proxy 下多个 route 共享。

**B2 修好之后已无正确性问题**:缓存键从 `path?query` 改为
`host\0path?query`,不会再跨站串内容。剩余差异只是**容量共享**
(同 proxy 下多 route 分摊 `max_size_mb`),不会返回错误数据。

**若要改**:需给每个 `VhostRoute` 分配独立缓存实例,收益仅容量隔离。

## 三、待决策

### 是否推送

7 个提交未推送(`git log --oneline origin/main..HEAD`)。

推送前建议:

```bash
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
```

两者当前都是绿的。

### 是否发版

改动均为 fix/chore/refactor,按语义化版本应走 patch(`0.5.0` → `0.5.1`)。
发版流程见 `AGENTS.md`(两处版本号 + 打标签)。

**注意**:`tcp.rs` 新增的 `HANDSHAKE_MAX_INFLIGHT = 512` 是**行为变更**
(建连洪峰下新连接在内核 backlog 排队)。发版说明应写明。

## 四、本轮值得记住的教训

1. **说比做多**:`forward.rs` 的"SSRF 修复"、`tcp.rs` 的"不耗用户态 fd",
   注释都描述了**意图**而非**验证过的行为**。两次都是外部指出后才修正。
2. **搜索范围自己划定,却把"没搜到"当成"不存在"**:P10 只 grep 了两个文件
   就判"全仓库没有";P9 第一遍只搜了 `session.rs`。正确做法是搜
   `crates/` 全树。
3. **引用了没打开过的行号**:说 `visitor_run.rs:99` 有 remove,实际那行是
   `cancel.cancelled() => return`。
4. **分批核实要明说**:清单有 30+ 项时,应事先声明"我分几批、每批哪些",
   而不是修到一半在提交信息里写"留待后续"——那是把欠账包装成计划。
