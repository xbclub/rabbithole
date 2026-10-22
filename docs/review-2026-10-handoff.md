# 交接:2026-10-22 静态审查与修复进度

> 本文是压缩上下文前的交接记录。按"已完成 / 未完成 / 待决策"三段组织。

## 一、已完成(8 个提交,均在 main,未推送)

```
c01a3e2 fix(client): 按 frp 语义重写 http_proxy / socks5 插件
        (含 splice 探针改 task-local 作用域的 flaky 修复)
4f34114 docs: 记录待办(插件代理语义)与本轮审查交接
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

- 测试:**558 通过 / 0 失败 / 2 忽略**(交接时 543;新增 15 条代理插件测试)
  —— 计数含 2 条 `#[ignore]` 之外的通过项,`--ignored` 单列 2 条
- clippy:`--workspace --all-targets -D warnings` 零错误
- `tests/e2e-plugins.sh`:**16/16 PASS**
- 真实代理客户端验收(curl 经隧道访问外网):见 §五

### 这批修复覆盖的清单

1 P0(登录限速键含端口)+ 14 P1 + 约 19 项 P2/P3,全部核实完毕;
外加 1 项"需要重做功能"的插件语义(原 A5)。

## 二、未完成(2 项,均为"可选优化"而非缺陷)

### 1. B9 打洞方向由服务端指定 —— 判定为设计使然

`nathole_cli.rs` 按服务端下发的 `role` 决定打洞方向;`udpchan.rs` 从帧里
的 `src` 解析地址并回发 UDP。这是 **UDP 打洞的协议本质**(协调方指定候选、
双端互发探测包)。

清单所说"可被用作 UDP 探测器"在"服务端不可信"模型下成立,但那个模型下
整个隧道都不可信(服务端可任意指定目标,用户流量本就全经它)。
信任边界是**服务端本身可信**(客户端用 secret / TLS pin / CA 认证)。

**若要收紧**:协议层加候选白名单(只允许打洞会话中交换过的地址),属新特性。

### 2. B11 vhost 缓存挂 proxy 级 —— 非正确性问题

`rt.http_cache` 是 `ProxyRuntime` 字段,一个 proxy 下多个 route 共享。

**B2 修好之后已无正确性问题**:缓存键从 `path?query` 改为
`host\0path?query`,不会再跨站串内容。剩余差异只是**容量共享**
(同 proxy 下多 route 分摊 `max_size_mb`),不会返回错误数据。

**若要改**:需给每个 `VhostRoute` 分配独立缓存实例,收益仅容量隔离。

## 三、待决策

### 是否推送

8 个提交未推送(`git log --oneline origin/main..HEAD`)。

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
(建连洪峰下新连接在内核 backlog 排队);`http_proxy`/`socks5` 的语义重写
也是**行为变更**(配置里的 `target` 从此被忽略,目的地来自用户请求)。
发版说明两处都要写明。

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
5. **测试替身会把 bug 固化成"规格"**:`e2e-plugins.sh` 里那两个 python 写的
   "假代理"配合自指实现一起绿了很久。测试替身越像被测对象本身,越容易
   把错误语义焊死。这次改成"target 填死地址 + 断言用户指定的目的地可达",
   才真正锁住了语义。
6. **全局计数探针在并行测试里必然 flaky**:`LOCKLESS_SPLITS` 只靠一把串行锁
   隔离,挡不住"别的测试也 splice 真实 TCP"(代理插件的端到端用例就是)。
   改成 `tokio::task_local` 作用域,按 future 隔离才是对的。

## 五、真实代理客户端验收(2026-10-22,本地)

e2e 用手写 socket 精确断言协议字节;这一轮额外用**用户实际会用的代理客户端**
(curl)做了端到端验收。规则里的 `target` 一律填不可达的 `127.0.0.1:1`:

| 用例 | 结果 |
| --- | --- |
| `curl -x http://pu:pp@127.0.0.1:<port> https://www.baidu.com` | 200,取到真实页面内容 |
| `curl -x http://pu:pp@127.0.0.1:<port> http://www.baidu.com` | 200 |
| `curl --socks5-hostname su:sp@127.0.0.1:<port> https://www.baidu.com` | 200,取到真实页面内容 |
| `curl --socks5-hostname su:sp@127.0.0.1:<port> http://www.baidu.com` | 200 |
| 错误凭据(http_proxy / socks5) | 均被拒 |

**未做**:拿 squid / tinyproxy 做"代理链"(我们的插件再连一个真实代理)。
新语义下插件本身就是代理服务端,`target` 不再承载代理地址,`ProxySpec`
里也没有"上游代理"字段 —— 要做代理链是**新特性**(需要在 `PluginSpec`
里加 `upstream_proxy`),不在本次范围内。

> 注:`example.com` 在本机某段时间不可达(直连也 000),故改用可达站点验收;
> 这是网络问题,与代码无关。
