# 任务:为 rabbithole 补齐 `virtual_net` 插件(第 10 个客户端插件)

## 0. 先读这一段:任务可能比你以为的大

**`virtual_net` 不是一个"连接处理器",它背后是一个协议子系统。**

另外 9 个插件(见 §2)都是同一个模式:拿到一条连接 → 转发到某个上游 → 结束。
`virtual_net` 不同,它需要:

1. 在客户端**打开一个 TUN 设备**(虚拟网卡),读 IP 包;
2. 把 IP 包经隧道发给服务端,由服务端侧的**控制器**按目标 IP 路由到正确的客户端;
3. 服务端维护 **IP 路由表**(哪个客户端负责哪些网段)。

frp 的实现在 `benchmark/frp-src-v0.71.0/pkg/vnet/` 下,共 **822 行**:

| 文件 | 行数 | 职责 |
|---|---:|---|
| `controller.go` | 387 | 控制器:读 TUN、包分发、客户端路由表、读写循环 |
| `tun.go` | 109 | TUN 设备抽象 |
| `tun_linux.go` | 131 | Linux 实现,依赖 `golang.zx2c4.com/wireguard/tun` + `vishvananda/netlink` |
| `tun_darwin.go` | 85 | macOS 实现 |
| `tun_unsupported.go` | 29 | 其他平台(返回错误) |
| `message.go` | 81 | vnet 自有协议消息 |

**这引入了仓库此前没有的依赖类别**:操作系统 TUN 设备、需要 root/管理员权限、
Rust 侧需要 `tun` 类 crate(如 `tun-rs` / `tokio-tun`)或直接 `libc` 开 `/dev/net/tun`。

### 因此,开工前必须先确认走哪条路

**路径 A —— 完整实现 vnet 子系统**
- 新增服务端控制器、客户端 TUN 接入、vnet 协议消息、IP 路由表
- 预计工作量:远大于一个插件,接近一个新功能模块
- 需要处理:平台差异(Linux/macOS/Windows)、权限失败降级、路由冲突、
  与现有 RHTP 帧协议的关系(是复用数据通道还是新增消息类型)

**路径 B —— 只补枚举与前端,运行时明确"不支持"**
- 让插件数量对齐 10,但选择它时返回清晰错误而非静默失败
- 工作量:小(半天内)
- 代价:对用户没有实际功能,属于"占位"

**如果用户没有明确指定,先问,不要自己选。** 两条路的工作量差一个数量级。

---

## 1. 背景:为什么会有这个任务

`README.md` 第 19 行写「10 种客户端插件」,但 `PluginKind` 枚举只有 9 个变体。
这个数字是按 **frp v0.71.0 基线**数的(frp 在 `pkg/plugin/client/` 下注册了 10 个),
不是按我们的实现数的。

`docs/requirements-matrix.md` 第 77 行当前记录:

```
| X1 | VirtualNet(vnet) | pkg/virtual, Alpha | 不实现,不宣称 | — | — | experimental |
```

这是当初**有意排除**(frp 自己也标 Alpha),不是遗漏。本任务就是把它从
`experimental` 变成 `done`。

> **注意**:用户已明确要求"改回 10 种"并"把这个功能补上",所以不要再回头
> 争论"要不要做"或"改成 9 种更诚实"。**任务已定,只需执行。**

---

## 2. 现有 9 个插件的模式(照这个结构扩展)

协议定义在 `crates/rh-protocol/src/proxy.rs`:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PluginKind {
    UnixDomainSocket,   // unix_domain_socket
    HttpProxy,          // http_proxy
    Socks5,             // socks5
    StaticFile,         // static_file
    Https2Http,         // https2http
    Https2Https,        // https2https
    Http2Https,         // http2https
    Http2Http,          // http2http
    Tls2Raw,            // tls2raw
}
```

对应 `PluginSpec` 是 `#[serde(tag = "kind", rename_all = "snake_case")]` 的
强类型枚举,每个变体携带自己的参数。

客户端实现在 `crates/rh-client/src/plugins.rs`,一个 `match spec { ... }`
按变体分派;大部分分支返回 `Ok(Box::new(a))` 之类的 `Box<dyn AsyncRead+Write>`。

---

## 3. 需要改动的落点(已核实,行号以当前 HEAD 为准)

| # | 文件 | 位置 | 改动 |
|---|---|---|---|
| 1 | `crates/rh-protocol/src/proxy.rs` | `:143` `pub enum PluginKind` | 加 `VirtualNet` 变体 |
| 2 | 同上 | `:156` `as_str()` | 加 `PluginKind::VirtualNet => "virtual_net"` |
| 3 | 同上 | `:180` `pub enum PluginSpec` | 加 `VirtualNet { ... }` 变体 |
| 4 | 同上 | `:262` `PluginSpec -> PluginKind` 映射 | 加分支 |
| 5 | `crates/rh-client/src/plugins.rs` | `:100` 之后(Tls2Raw 分支后) | 加 `PluginSpec::VirtualNet` 分支 |
| 6 | `console/src/features/services/matrix.ts` | `:54` `PLUGIN_NAMES` | 数组加 `'virtual_net'` |
| 7 | 同上 | `:784` `PLUGIN_FIELDS` | 加 `virtual_net: [...]` 子表单声明 |
| 8 | `docs/requirements-matrix.md` | `:77` | X1 状态 `experimental` → `done`(或按实际填) |
| 9 | `README.md` | `:19` | 「10 种客户端插件」**保持不变**(改完后就是事实) |

### 关于 `health.rs`(容易误判,特别说明)

`crates/rh-client/src/health.rs:127` 的 `plugin_local()` 用的是:

```rust
match p {
    ...各变体... => addr_of(local_addr),
    _ => (String::new(), 0),      // ← 通配符兜底
}
```

**有 `_` 兜底,新增枚举变体不会导致编译失败。** 但你要判断新插件是否需要
健康检查语义——如果需要,就在这个 match 里加分支;如果不需要,保持兜底即可。

### 关于服务端

若走**路径 A**,服务端(`crates/rh-server`)需要新增控制器。当前服务端对插件
是**透明**的(只负责把 `PluginSpec` 下发到客户端),vnet 是第一个需要服务端
参与数据面路由的插件——这是架构上的新东西,需要单独设计,不要照抄现有插件路径。

---

## 4. 验收要求

### 必须通过的门(与仓库现有标准一致)

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

`console/` 侧:

```bash
cd console && pnpm typecheck && pnpm lint && pnpm test && pnpm build
```

### 必须提供测试

- **协议层**:`PluginSpec` 序列化/反序列化往返(`virtual_net` 的 serde 名字要正确)
- **客户端层**:至少一条端到端用例(若走路径 A:两个客户端经 vnet 互通 ping;
  若走路径 B:选择该插件时返回明确错误而非 panic/静默)
- 参照 `tests/e2e-plugins.sh` 的组织方式,**不要另建测试框架**

### 必须更新的文档

- `docs/requirements-matrix.md` X1 行
- `docs/README.md` 索引(如果新增了设计文档)
- 若引入新依赖,在 `Cargo.toml` 的对应 crate 里加并说明用途

---

## 5. 已知的坑(照仓库既有约定)

1. **Windows 编译**:`std::os::fd` / `std::os::unix` 必须放在 `#[cfg(unix)]` 内。
   TUN 相关代码**几乎肯定是 Unix 专属**,必须做好平台门控:

   ```bash
   grep -rn "std::os::fd\|std::os::unix" --include=*.rs crates/ vendor/
   ```

   本地 macOS 上 `cargo test` 绿 **不代表** Windows 能编,只能靠 CI 的
   `windows-check` job 验证。

2. **提交规范**(见根目录 `AGENTS.md`):
   - 只 `git add` 自己动过的文件,不要 `git add -A`
   - 提交信息写**为什么**,不写**做了什么**
   - 纯格式化不要和业务改动混在一个提交

3. **性能基准**不要自己写压测脚本,用 `benchmark/peer.sh`。

4. **视觉判读**(截图复核等)禁用 `analyze image` 工具,交 `glm-5.3-flash`。

---

## 6. 参考实现

frp 源码在 `benchmark/frp-src-v0.71.0/`(只读研究材料,不参与构建):

- `pkg/plugin/client/virtual_net.go` —— 插件本体(很短,主要是把连接交给控制器)
- `pkg/vnet/controller.go` —— 控制器,核心逻辑
- `pkg/vnet/tun_linux.go` —— TUN 设备打开方式
- `pkg/config/v1/proxy_plugin.go:249` —— 配置结构(`VirtualNetPluginOptions`,
  注意它**没有任何参数**)

---

## 7. 开工前请回答

1. 走**路径 A**(完整子系统)还是**路径 B**(占位)?如果用户没说,问。
2. 若走 A:服务端控制器放在哪个模块?是否新增 RHTP 消息类型,还是复用现有
   数据通道承载 IP 包?
3. 若走 A:Windows 上如何处理(明确不支持并返回错误,还是用 wintun)?

**先给出方案再动手,不要直接开始写代码。**
