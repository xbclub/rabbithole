# 交接:2026-10-22 静态审查与修复进度

> 本文是压缩上下文前的交接记录。按"已完成 / 未完成 / 待决策"三段组织。
>
> **状态更新(2026-10-22 补注)**:文中"8 个提交未推送"已不准确 ——
> 当前 `origin/main..HEAD` 为 **11 个提交**(交接后又落了
> `a119766`/`e1e3c1e` 两个文档提交与 `c01a3e2` 插件重写)。
> 推送与发版决策**仍未执行**,见 §三。

## 一、已完成(11 个提交,均在 main,未推送)

```
3fdfc19 fix(client,server): 凭据错误退避是死代码 + 分组缓存只到 1/N 命中
e1e3c1e docs: 交接文档补测试计数口径
a119766 docs: 交接文档填入提交哈希
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
  - **2026-10-22 复跑更正**:**564 通过 / 0 失败 / 2 忽略**(交接时 558)。
    注意:复跑时工作区存在**多个文件的未提交改动**且仍在变动,该计数
    含其影响,不能等同于"已提交状态"的计数;要拿到稳定数字,需在干净
    检出上重跑 `cargo test --workspace`。
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

11 个提交未推送(`git log --oneline origin/main..HEAD`,2026-10-22 核实;
交接时记为 8 个,此后又落了 3 个)。

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

## 四点五、2026-10-22 二轮核实:四条"剩余问题"

外部提出四条,逐条核实结果如下(修与不修都给了理由)。

| 编号 | 结论 | 处置 |
| --- | --- | --- |
| B1 凭据错误退避是死代码 | **属实,且比报告更严重** | 已修(见下) |
| B3 分组缓存只到 1/N 命中 | **属实** | 已修(粘性选路) |
| Linux-only 背压测试从未执行 | **属实** | 已补 CI |
| KEY_TYPES 的 `rsa-`/`dsa-` 冗余 | **不成立** | 不改 |

### B1:报告漏了两层,实际有三处断裂

1. `run_loop` 返回 `()`,`connect_and_run` 恒 `Ok(())` —— `dispatch_frame`
   产生的串被 `break` 吞掉;
2. 握手阶段措辞是 `server rejected:` / `hello rejected:`,不含该串;
3. **报告没提到的第三处**:服务端 ERROR 帧发的是 `ServerMessage::Error`
   **信封**(`{"Error":{...}}`),客户端在 HELLO_ACK 阶段按**裸
   `ErrorMessage`** 反序列化 → 必然失败 → 每例拒绝都变成
   `server error (unparsable)`,错误码整个丢失。所以即使 1、2 都修好,
   判定仍然拿不到 `AUTH_FAILED` / `CLIENT_DISABLED`。

修法:新增 `SessionError { Fatal(FatalKind), Transient }` 与
`FatalKind::{Rejected(code), VersionMismatch}`,拒绝点用 `reject(code)`
打类型标记,`main` 只认类型不认文案;新增 `parse_error_code()`
(先信封、再裸结构、都失败才 `UNKNOWN`,**绝不因解析失败丢掉错误码**)。

实测(错误 secret / 客户端被禁用,release 二进制):
退避从 1,2,4,8,16,32 逐级爬 → 直接顶到 60s,并按错误码给出不同提示
(`AUTH_FAILED` → 检查 secret;`CLIENT_DISABLED` → 检查控制台启用状态)。

### B3:粘性选路

新增 `group_pick_sticky`(FNV-1a,跨进程稳定)。vhost HTTP 在**有缓存配置**
时按 `cache_key` 哈希选成员 —— 同一资源读写恒定落同一成员,不同资源仍分散。
TCP 数据面继续轮询(那里没有需要粘性的状态)。

### Linux-only 测试:补 CI

新增 `.github/workflows/ci.yml`(push main / PR 跑 clippy + cargo test,
并显式确认 `ss` 可用)。**刻意不含 `cargo fmt --check`**:仓库当前有 93 处
fmt 差异(24 文件),是既有状态;加进去门禁一上线就红,红的门禁很快会被
无视,那比没有更糟。要加,先把 fmt 单独整理成一次纯格式化提交。

### KEY_TYPES:`rsa-`/`dsa-` 不是冗余

`rsa-` 覆盖的是 `rsa-sha2-256` / `rsa-sha2-512` 这类**不含 `ssh-` 前缀**的
算法名;`dsa-` 同理。二者与 `ssh-` 是并集关系而非子集,删掉会让这类行被
当作"没有 options"整行交给 `from_openssh`,解析失败后整行丢弃。

### 顺带修掉的自己引入的 flaky

代理插件的域名用例原先注入**全局**解析函数,会与 `dnsquery` 自己的注入
用例互相污染(单独跑绿、成组跑红)。改为只断言协议契约 + 纯函数级验证。

### 一个**未修**的独立问题(如实记录)

`rh-transport` 的 `probe_result_matches_thresholds` 偶发失败(5 轮全量里
出现 1 次;单独重复 8 次全过)。它依赖注入的拨号延迟做 RTT 分档断言,
时序敏感。本次未触及该 crate,不混入本次提交,但**它确实是个真 flaky**,
下一轮应单独处理(把"注入延迟"换成可控时钟,而不是真的 sleep)。

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
