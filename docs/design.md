# rabbithole 设计文档（P0）— v1.4

> 目标：Rust 实现的端口转发 / 内网穿透服务。服务端提供 Web 控制台，集中管理多个客户端与端口映射规则（对标 nps 的管理模式），数据面能力对标 frp。
> 本文档覆盖 P0 范围；P1/P2 扩展方向在文末路线图中列出。
> 决策记录：**全新自有协议，不兼容 frp/frpc**；前端为独立 `console/` 工程（Vite + React 19 + Tailwind v4 + shadcn/ui，见 §8）。
>
> **v1.4 修订**（相对 v1.3，来源为四次评审）：
> 1. 会话注册锁内**同时复验最新 secret**（重算 HMAC 比对），修复"挑战校验通过后才轮换密钥"的交错窗口；
> 2. 状态表补齐：**客户端重新 enable 的批量恢复行**；`bind_error` 的会话取消/替换行（`confirmed` 重置）；
> 3. disabled 跳步判定以 `services.enabled=0` **或** `clients.enabled=0` 任一成立为准（编辑/规则 enable/创建均仅落库、不推进 revision、不 bind），客户端重新启用按状态表批量恢复——消除 §4.1 与 §4.4 口径冲突；
> 4. UDP 恢复预算公式删去重复计数的"拨号"项（CREATE→HELLO 10s 已含拨号与握手）；
> 5. 增量不一致时**挂起并发 SYNC_REQUEST**（消除与 `ok=false` 的双路径歧义，`ok=false` 仅表示客户端本地应用失败）；死池连接丢弃即补位签发；澄清 ACK 字段名、步骤号、规则级确认版本、`CHALLENGE_EXPIRED` 覆盖范围、§3.6 串行域括注、§3.5 视角标注。
>
> **v1.3 修订**（相对 v1.2，来源为三次评审）：
> 1. **数据连接一律服务端指令发起**：TCP 预热与 UDP 建立统一为 `CREATE_DATA_CHANNEL{service_id, request_id}`，`request_id` 一次性、单调、必须回显——消除 v1.2 中"TCP 预热双责任方"冲突与 UDP 迟到旧拨号接管问题；客户端拨号纯被动。
> 2. **确认闭环**：重复消息不执行副作用但**必须回执确认**（ACK 重放语义：`APPLY_RESULT{applied_revision}`）；增量只发一次，超时**放弃增量改发全量**；全量确认清空队列；**下发队列归属会话**，会话替换即废弃重建，迟到确认随旧会话自然失效。
> 3. **串行域扩展**：事务提交 → 运行态快照发布（含 `START_FORWARD` 目标快照）→ 入队，三步同属每客户端串行域；等待客户端确认在域外不阻塞提交；非端口更新同样在域内发布服务端快照。
> 4. **`bind_error` 独立状态**：服务端绑定失败与客户端应用失败（`apply_error`）分离；进入 `running` 的双条件 = listener 绑定 ∧ 当前会话已确认当前 revision；固定端口 60s 周期重试，自动分配端口立即重分配。
> 5. **UDP 期限与探活**：CREATE→HELLO 10s 期限；通道层心跳（PING/PONG 允许于 UDP 数据连接，30s×2 判死）；首建失败/请求超时/断开/探活失败统一进入重建状态机；恢复承诺改为**预算公式**（检测 + 重建 + 退避），不再宣称"≤30s"。
> 6. **认证终检与互斥**：挑战绑定签发连接；AUTH_RESPONSE 以 **DB 最新** enabled/secret 校验；会话注册与禁用/轮换在同一每客户端锁内完成，挑战期间禁用/轮换必然导致注册失败。
> 7. **disabled 编辑语义**：停用状态下修改仅落库、不 bind、不入队（v1.3 推进 revision，v1.4 起不推进）；enable 时才资格校验+绑定+下发。
> 8. 状态表补齐常规迁移：running→pending（更新待确认）、pending→offline（离线）、会话替换重置确认等；`SERVICE_SYNC` 只含 enabled 规则（全量替换即隐式移除停用规则）；池在途额度、死池连接写失败重取等资源边界量化。
>
> **v1.2 修订**（摘要）：指派自包含（`START_FORWARD` 携带目标）；nonce 挑战认证（+1 RTT，零时钟假设）；bind 后 DB 失败补偿与同事务提交；会话/规则两级取消域；UDP 通道服务端负责制；重叠地址热更新 P0 拒绝；TCP/UDP 端口命名空间分离；P0 仅 IPv4；vben logout 显式附 Bearer。
> **v1.1 修订**（摘要）：数据通道状态机与指派信号、每客户端 revision、APPLY_RESULT/SYNC_REQUEST、状态转换表、量化超时与限额、vben 真实契约对接、撤回 rathole 池"不补充"错误归因（实际按用户连接补位：server.rs:596-609、633-647；`StartForwardTcp` 显式指派：server.rs:633-640、client.rs:214-247）。

---

## 1. 总体架构

### 1.1 组件

| 组件 | 说明 |
|---|---|
| `rh-server` | 单进程服务端。三合一：控制面（客户端接入/认证/规则下发）、数据面（映射端口监听与转发）、管理面（REST API + Web 静态资源托管） |
| `rh-client` | 客户端。运行在 NAT 后的目标机器，主动连接服务端，按服务端下发的规则把本地服务暴露出去 |
| `console` | Web 控制台前端（Vite + React 19 + TypeScript + Tailwind v4 + shadcn/ui；2026-10-12 起替换 antd 版），编译产物由 rh-server 托管 |

### 1.2 端口规划

| 端口 | 用途 | 默认 |
|---|---|---|
| 控制端口 | 客户端接入，**控制连接与数据连接共用**（首帧分流，见 §3.2） | `:7000` |
| 管理 API | REST API + 前端静态资源，JWT 保护；默认只绑 `127.0.0.1`，公网部署见 §9.6 | `127.0.0.1:8080` |
| 映射端口 | 每条规则监听，由控制台配置或端口分配器自动分配（见 §4.4） | 可配范围 |

### 1.3 核心数据流

```
                 ┌─────────────────────────── rh-server ───────────────────────────┐
                 │                                                                  │
  用户 ──▶ :52000 (规则A监听) ──┐                                                    │
  用户 ──▶ :52001 (规则B监听) ──┼──▶ 数据通道路由(会话/归属/请求号四重校验) ────┐      │
                              │                                             │      │
                 控制连接 ◀──首帧分流── 共用 :7000 ◀──数据连接(全部服务端指令发起)─┘      │
                     │                                                               │
                     ├──▶ 会话管理(client_id → Session{session_id, epoch, cancel})     │
                     ├──▶ 规则引擎(DB=期望状态, 内存=运行状态, revision 对账)           │
                     └──▶ axum /api ◀── Web 控制台(console/)                             │
                 └──────────────────────────────────────────────────────────┘
                     ▲
                     │ noise/tls 加密传输, 规则由服务端下发(指派自包含)
                 rh-client ──▶ 本地目标 127.0.0.1:xxxx
```

---

## 2. 代码结构

```
rabbithole/
├── Cargo.toml            # workspace
├── crates/
│   ├── rh-protocol/      # RHCP 帧格式 + 消息定义 + ServiceRule 共享类型（零 IO）
│   ├── rh-transport/     # 传输层抽象, 源自 rathole transport/（Apache-2.0）, 首轮升级依赖
│   │                     # 接口边界按 §10 定义（连接/流分离, datagram 可选能力）
│   ├── rh-server/        # bin: 控制面 + 数据面 + axum API + sqlx
│   └── rh-client/        # bin: 会话状态机 + 被动拨号器 + 本地转发
├── console/              # Vite + React 19 + shadcn/ui 控制台(见 §8)
├── migrations/           # sqlx 迁移
└── docs/
```

依赖选型：`tokio`；`tokio-util`（**`sync::CancellationToken`** 用于会话/规则两级级联取消）、`axum`、`sqlx` + SQLite（SQL 兼容 PG 便于迁移）、`serde`/`serde_json`、`snowstorm`（Noise）、`tokio-rustls`（TLS）、`jsonwebtoken` + `argon2`（管理面）、`tracing`、`uuid`、`governor`（登录限速）、`socket2`（TCP keepalive）。

---

## 3. 协议设计：RHCP v1（Rabbithole Control Protocol）

### 3.1 帧格式

```
 0        4        5          7         11         11+len
+--------+--------+----------+---------+----------+-----------+
| "RHTP" | ver u8 | type u16 | len u32 |  payload             |
+--------+--------+----------+---------+----------+-----------+
 magic     =1      消息类型    payload   len ≤ 1 MiB
                              长度(大端)
```

- 控制消息 payload 为 **JSON（UTF-8）**：低频小消息，换取可调试性与演进能力（serde 忽略未知字段）。
- `UDP_PACKET` payload 为二进制：`[src_addr_len u8][src_addr ASCII "ip:port"][data...]`。
- TCP 数据连接在收到 `START_FORWARD` 后**退化为裸字节流**，此后该连接上**不再出现任何帧**（错误即断开，见 §3.5）。
- UDP 数据连接始终为帧序列（`UDP_PACKET` 与通道层 `PING/PONG`，见 §4.5）。

### 3.2 首帧分流

服务端 accept 后先完成传输层握手（Noise/TLS，超时 5s），随后读第一帧（等待 5s）：

| 首帧类型 | 判定 | 后续行为 |
|---|---|---|
| `HELLO` | 控制连接 | 进入认证握手（§3.4） |
| `DATA_CHANNEL_HELLO` | 数据连接 | 四重校验（§3.3）→ 入池/成通道 |
| 其他 / 非法 magic | 攻击或探测 | 立即断开，计数告警 |

### 3.3 消息定义（逐字段）

方向：C=client→server，S=server→client。

#### `0x0001 HELLO`（C→S，控制连接首帧）

| 字段 | 类型 | 说明 |
|---|---|---|
| `client_id` | string(uuid) | 声明身份（此时尚未认证） |
| `version` | string | 语义化版本 |
| `hostname` / `os` / `arch` | string | 上报展示 |
| `capabilities` | [string] | P0 固定 `["tcp","udp"]`，预留 `http`/`quic` |
| `ts_ms` | i64 | 客户端时间，**仅作诊断参考，不参与认证判定** |

#### `0x0009 AUTH_CHALLENGE`（S→C，收到 HELLO 后）

| 字段 | 类型 | 说明 |
|---|---|---|
| `nonce` | string(hex) | 服务端 32 字节随机数，单次有效（消费即删；TTL 30s 为兜底清理上界，实际响应窗 10s，见 §9.3） |

- 服务端校验 `client_id` 存在且 `enabled=1` 后才发挑战；否则直接 `HELLO_ACK{ok:false}` 并断开。
- 挑战表 `DashMap<nonce_hex, (client_id, conn_id, deadline)>`——**nonce 绑定签发它的连接**（`conn_id`）；`AUTH_RESPONSE` 只在签发连接上被接受，其他连接送达的响应不查表即拒。
- 取出即删（原子），天然防并发与重放。

#### `0x000A AUTH_RESPONSE`（C→S）

| 字段 | 类型 | 说明 |
|---|---|---|
| `client_id` | string(uuid) | 须与挑战归属一致（以挑战表记录为准） |
| `auth` | string(hex) | `HMAC-SHA256(secret_bytes, "rh2:" + client_id + ":" + nonce_hex)` 的 hex |

校验顺序（全部通过才进入会话注册 §3.4）：

1. 本连接存在未过期挑战（`conn_id` 匹配，取出即删）；
2. **重新读取 DB 最新行**：`enabled=1`；
3. 以 **DB 当前 secret** 重算 HMAC 并常数时间比较。

第 2/3 步使用最新值意味着：挑战发出后才发生的**禁用** → `CLIENT_DISABLED`；**密钥轮换** → 旧 secret 的 HMAC 失败 → `AUTH_FAILED`。响应须在挑战发出后 10s 内到达（超时、nonce 不存在或已消费统一回 `CHALLENGE_EXPIRED`）。

#### `0x0101 HELLO_ACK`（S→C）

| 字段 | 类型 | 说明 |
|---|---|---|
| `ok` | bool | 认证结果 |
| `error_code` | string? | `AUTH_FAILED` / `CLIENT_DISABLED` / `VERSION_MISMATCH` / `CHALLENGE_EXPIRED` |
| `session_id` | string(uuid) | 本次会话令牌 |
| `heartbeat_interval_s` | u32 | 默认 30 |
| `server_version` | string | — |

#### `0x0102 SERVICE_SYNC`（S→C，会话建立后必发）

| 字段 | 类型 | 说明 |
|---|---|---|
| `revision` | u64 | **该客户端**的期望配置版本（每客户端单调递增） |
| `rules` | [ServiceRule] | 该客户端**全部 enabled 规则**，全量幂等替换；停用/删除的规则不在列表中，客户端全量替换即隐式移除 |

#### `0x0103 SERVICE_UPDATE` / `0x0104 SERVICE_REMOVE`（S→C，在线时增量下发）

`SERVICE_UPDATE { revision, rule }`；`SERVICE_REMOVE { revision, service_id }`。

**下发管线（归属会话）**：

- 每个会话一个下发队列（单任务顺序发送）。会话建立 → 先发全量 `SERVICE_SYNC`；会话替换/断开 → **队列废弃**（在途等待作废），新会话重新全量——迟到的旧会话 `APPLY_RESULT` 随旧队列一起失效，不会污染新会话。
- 增量按"每客户端串行域"（§4.4 步骤 5）的提交顺序入队，**每条只发送一次**。
- 某条增量发出后 10s 未确认 → **丢弃队列中全部未发送项，改发全量 SYNC（携带当时最新 revision）**；全量重试 10s×3 次，其后 30s 间隔。**全量确认后仅移除 revision ≤ 已确认值的队列项**；快照时点之后新入队的增量照常顺序发送。
- revision 语义：客户端以"收到的最大 revision"为准；全量覆盖其快照时点前的全部状态，快照后提交的增量照常后续下发（客户端按 revision 判重，乱序到达不会错误应用）。

#### ServiceRule（协议与数据库共用结构）

| 字段 | 类型 | 说明 |
|---|---|---|
| `service_id` | u64 | 规则 ID |
| `name` | string | 客户端内唯一，`[a-zA-Z0-9_-]{1,64}` |
| `proto` | enum | `tcp` / `udp` |
| `bind_ip` | string | 服务端监听地址，默认 `0.0.0.0`；P0 仅 IPv4（见 §4.4） |
| `bind_port` | u16 | 服务端暴露端口；**0 = 自动分配**（分配结果落库并回填，见 §4.4） |
| `target_host` | string | 客户端侧本地目标（可达局域网其他机器） |
| `target_port` | u16 | — |
| `pool_size` | u8 | TCP 数据连接预热池大小，默认 2，上限 16 |
| `extra` | object | P0 恒为 `{}`；P1 承载 vhost 域名/限速/PROXY protocol 等 |

客户端规则集的用途限定为：**UDP 通道目标**与展示/日志。TCP 转发目标不来自客户端规则集（`START_FORWARD` 自包含）。

#### `0x0105 CREATE_DATA_CHANNEL`（S→C，控制连接）

| 字段 | 类型 | 说明 |
|---|---|---|
| `service_id` | u64 | 规则 |
| `request_id` | u64 | **会话内单调递增、一次性**；标识本次拨号请求 |

**数据连接一律由服务端指令发起**（TCP 预热、TCP 补位、UDP 建立与重建全部经由本消息）；客户端拨号纯被动，不存在客户端自发拨号。触发场景：

- TCP：规则进入 `running` 时预热 `pool_size` 条（§4.2）；用户连接取池后补位（取一补一）。
- UDP：通道建立/重建（§4.5）。

**期限与失效**：请求发出后 10s 内未收到回显该 `request_id` 的 `DATA_CHANNEL_HELLO` → 请求失效并计数；会话取消、规则停用/删除、UDP 配置屏障同样使其全部在途请求失效。失效后迟到的回显 → 立即关闭。

#### `0x0107 START_FORWARD`（S→C，**在数据连接上发送**，指派信号）

| 字段 | 类型 | 说明 |
|---|---|---|
| `service_id` | u64 | 指派给哪条规则 |
| `user_src` | string | 用户连接来源 `ip:port`（日志/透传用） |
| `target_host` | string | **完整指派：转发目标由服务端运行态快照给定** |
| `target_port` | u16 | — |

自包含指派：客户端收到后直接连接 `target_host:target_port`（≤10s），成功进入裸流转发，失败关闭本连接——**全程不查、不比对本端配置版本**。服务端运行态快照在"每客户端串行域"内随事务发布（§4.4 步骤 5），因此指派目标与 DB 提交顺序一致，不存在旧发布覆盖新快照。此帧是数据连接上**最后一个帧**；数据连接上不存在 ERROR 帧，错误语义 = 断开。

#### `0x0003 PING` / `0x0106 PONG`

- 控制连接：C 每 `heartbeat_interval_s` 发 `PING{ts_ms}`，S 回 `PONG`。S 3 个周期未收到 PING 断开会话；C 2 个周期未收到 PONG 主动重连。
- **UDP 数据连接（通道层探活）**：S 每 30s 发 `PING`，C 必须回 `PONG`；S 连续 2 个周期未收到 `PONG` 判定通道死亡（§4.5）。TCP 数据连接不发应用层心跳，依赖传输层 TCP keepalive（§4.2）。

#### `0x0005 DATA_CHANNEL_HELLO`（C→S，数据连接首帧）

| 字段 | 类型 | 说明 |
|---|---|---|
| `session_id` | string(uuid) | 会话归属 |
| `service_id` | u64 | 规则归属 |
| `request_id` | u64 | **回显触发本次拨号的请求号** |

四重校验（任一不满足立即断开）：

1. 会话存在且未取消；
2. `service_id` 对应规则的 `client_id` 等于该会话的 client_id；
3. 规则处于 enabled；
4. `request_id` 属于该规则**当前在途且未消费**的请求（单次消费）。

通过后：TCP → 入该规则池（或立即配对等待中的用户连接）；UDP → 仅当该 `request_id` 是该规则**当前待建请求**时成为新通道（否则关闭）。

#### `0x0007 UDP_PACKET`（双向，二进制 payload）

UDP 规则的全部流量走该规则**当前唯一**数据连接，每包一帧（P0；P1 演进双模式）。

#### `0x0004 SYNC_REQUEST`（C→S，控制连接）

`{}` — 客户端检测到增量不一致时发起。处理优先级：**先判重**（revision ≤ `applied_revision` → 不执行副作用，回 ACK 重放，见 `APPLY_RESULT`）；判重通过但引用未知 `service_id` → **不回 `ok=false`**，发送本请求并挂起等待全量（`applied_revision` 不动）；`ok=false` 仅表示客户端本地应用过程自身失败（如解析错误）。服务端收到本请求后向**该会话队列**压入全量 SYNC。

#### `0x0008 APPLY_RESULT`（C→S，控制连接）

| 字段 | 类型 | 说明 |
|---|---|---|
| `revision` | u64 | 确认语义：**客户端当前已应用到的 revision** |
| `ok` | bool | 应用结果 |
| `error` | {service_id?, reason}? | 失败定位（可选） |

**应用语义（原子）**：客户端把一批 `SERVICE_SYNC`（全量快照替换）或单条 `UPDATE`/`REMOVE`（原子 diff）**整体应用**——应用即内存规则集原子替换/增删，要么整体成功（`applied_revision` = 消息 revision），要么整体失败（保留旧集合，`applied_revision` 不动）。部分成功不存在。

**重复消息（ACK 重放）**：收到 revision ≤ `applied_revision` 的重复消息，不执行副作用，但**必须回复** `APPLY_RESULT{ revision: applied_revision, ok: true }`——确认"当前已处于该版本"，服务端等待因此总能收敛。

**服务端完成条件**：所属会话队列收到 `APPLY_RESULT` 且 `revision ≥ 等待中的 revision` 且 `ok=true`。`ok=false` → 规则状态 `apply_error`（§4.1），以全量 SYNC 重试。确认超时**不产生** `apply_error`（只是未确认）。

#### `0x0199 ERROR`（S→C，**仅控制连接**）

`{ "code": string, "message": string }`，发送后关闭控制连接。数据连接上永不出现此帧。

### 3.4 认证与会话

**凭据**：控制台创建客户端生成 `client_id`(uuid v4) + `secret`(32 字节随机, hex 展示 64 字符，仅创建/轮换响应返回一次)。

**认证流程**（nonce 挑战，三次消息）：

```
C ── HELLO{client_id, version, ...} ──▶ S   client 存在且 enabled → 生成 nonce(绑定本连接, TTL 30s)
C ◀─ AUTH_CHALLENGE{nonce} ─────────── S
C ── AUTH_RESPONSE{client_id, hmac} ──▶ S   §3.3 三步校验(含 DB 最新 enabled/secret)
C ◀─ HELLO_ACK{ok, session_id, hb} ──── S   §下方注册完成后
```

**会话注册（与禁用/轮换互斥）**：AUTH_RESPONSE 初校验（§3.3）通过后，在**每客户端锁**内完成最终校验与注册：重读 DB 最新行，要求 `enabled=1` **且** 以最新 secret 重算 HMAC 常数时间比对一致（等价于确认 secret 自初校验后未变）；任一不满足 → `CLIENT_DISABLED` / `AUTH_FAILED`，不注册。通过才创建 `Session` 并替换注册表。由此：注册前必然看到已提交的禁用/轮换（注册失败）；注册后才发生的禁用/轮换会立即触发 `rule_cancel`/踢线（取消域见下），两者不存在交错窗口。

- 全程位于 Noise/TLS 加密信道内，nonce 不暴露给第三方；重放任何一步均失败；无时钟假设。
- 每 IP 未认证并发连接限额（§9.4）约束挑战握手成本。

**会话与两级取消域**：

- `Session { session_id, epoch: u64, session_cancel: CancellationToken, dispatch_queue }`，注册表 `DashMap<client_id, Arc<Session>>`。
- **会话取消**（只影响连接，不动 listener）：同一 `client_id` 注册新会话 = 替换（`epoch+1` 原子换入 → 旧 `session_cancel` 触发 → 旧控制连接与旧会话名下全部数据连接关闭、旧下发队列废弃）；密钥轮换 = 仅替换会话（踢线重认证），**listener 保持**；心跳超时/断开 = 会话取消。
- **规则取消**（`RuleRuntime.rule_cancel`，独立于会话）：规则 disable/删除、客户端禁用/删除时触发——关闭该规则 listener、池与**该规则存量转发连接**（不影响同客户端其他规则）。数据连接同时持有 `session_cancel` 与所属规则 `rule_cancel` 克隆，任一触发即关闭。
- 规则 listener 存活条件 = 规则 enabled ∧ 客户端 enabled（与在线无关，行为对齐 frp：端口始终监听，客户端不在线时用户连接快速失败）。
- 离线清理时先比对注册表当前会话 `Arc` 指针与自身一致才移除，防止旧清理任务误删新会话。

**secret 存储**：校验需原文，P0 明文列 + 数据库文件权限 0600，信任边界为单机文件系统；P1 可选信封加密，不改变协议。

### 3.5 数据通道状态机

以数据连接为主体（拨号全部由 `CREATE_DATA_CHANNEL` 触发，四重校验由服务端执行）：

```
(收到 CREATE{service, request}) ──拨号+传输握手──▶ Authing ──DATA_CHANNEL_HELLO 四重校验──▶ Pooled/Channel
                                                      │(校验失败: 关闭)                        
                                                      ▼
TCP:  Pooled ──START_FORWARD(本连接最后一帧, 帧内 target)──▶ Assigned ──连目标(≤10s)──▶ Forwarding(裸流)
UDP:  Channel（当前唯一通道; 收发 UDP_PACKET + 通道层 PING/PONG; 失效→服务端重建, §4.5）
      任一状态错误/取消 → Closed
```

关键约定：

1. **池中等待时不连接本地目标**——预热连接只完成传输层握手 + `DATA_CHANNEL_HELLO`。
2. **指派信号在数据连接本体上发送**，不经过控制连接，避免二次配对竞态。
3. **指派自包含**：目标地址随 `START_FORWARD` 给定且取自服务端串行域发布的快照，客户端配置视图新旧不影响转发正确性。
4. **目标先发协议（SSH/SMTP）安全**：指派后立即连目标并搬运，无互等。
5. **TCP 进入裸流后无任何帧**：错误一律以断开表达；`ERROR` 帧只在控制连接。UDP 连接始终帧化。

### 3.6 关键时序

**上线与同步**

```
rh-client                        rh-server
   │── transport connect :7000 ──────▶│  (Noise 握手 ≤5s)
   │── HELLO(id,version,...) ─────────▶│  client 存在且 enabled → 发 nonce(绑定本连接)
   │◀─ AUTH_CHALLENGE{nonce} ─────────│
   │── AUTH_RESPONSE{id,hmac} ───────▶│  DB 最新 enabled/secret 校验; 锁内注册会话
   │◀─ HELLO_ACK(ok,session_id, hb=30)│
   │◀─ SERVICE_SYNC(enabled rules, n) │  会话队列首条=全量
   │── APPLY_RESULT{n, ok} ─────────▶│  规则 pending→running
   │◀─ CREATE×pool_size(每条tcp规则)  │  预热(服务端发起); udp 规则每条 1 个 CREATE
   │··· PING/PONG 每 30s ·············│
```

**TCP 用户连接数据路径（规则 A :52000）**

```
user ──▶ rh-server :52000 accept
           │ 池取一条; 池空 → 发 CREATE{A, rid} 并等待 ≤10s(总额)
           │ 取出的连接上写 START_FORWARD{A, user_src, target} → 本连接转裸流
           │ (写指派失败=死连接 → 丢弃该连接, 取下一条/继续等补拨, 总额仍 ≤10s)
rh-client: 收 START_FORWARD → 连接帧内 target(≤10s)
   ├─ 成功: copy_bidirectional(user ◀──裸流──▶ target)
   └─ 失败: 关闭数据连接 → 服务端关闭该用户连接
服务端取池后立即再发 CREATE 补位(取一补一; 池+在途 ≤ pool_size)
```

**规则热更新（TCP 与 UDP 差异）**

```
浏览器 ──▶ PUT /api/services/{id}
rh-server: 每客户端串行域内完成: 校验→占位→bind→同事务落库(revision+1)→快照发布→入队
           (域外仅等待客户端确认)
  TCP 规则: SERVICE_UPDATE{revision, rule} → APPLY_RESULT
  │    在途旧指派不受影响(自包含); 新指派取新快照
  │    bind_port 变化: bind 新口 → 同事务提交 → 域内换 listener → 关旧口
  │    (旧口存量用户连接随既有数据通道自然结束)
  UDP 规则(目标变化): 关旧通道+使其在途请求失效 → SERVICE_UPDATE → APPLY_RESULT(新revision)
  │    → CREATE(新 request_id) 建新通道   ←「指派屏障」
  └─ client 离线: 仅落库, 规则状态 offline, 上线后全量 SYNC 对齐
```

**会话替换（重复上线/密钥轮换）**

```
新认证+锁内注册 → registry 原子换入 Session{epoch+1} → 旧.session_cancel 触发
  → 旧控制连接+旧会话数据连接关闭; 旧下发队列废弃; listener 不动
  → 新会话: 全量 SERVICE_SYNC → APPLY_RESULT → running → 服务端重新预热各规则
```

---

## 4. rh-server 设计

```
crates/rh-server/src/
├── main.rs            # 配置加载, tokio 启动, 优雅关停, 启动恢复(§4.6)
├── config.rs          # rh-server.toml
├── control/
│   ├── mod.rs         # 控制端口 accept + 首帧分流 + nonce 挑战认证 + 每客户端锁
│   ├── session.rs     # Session{session_id, epoch, session_cancel, dispatch_queue}
│   ├── challenge.rs   # nonce 表(绑定连接, 取出即删)
│   └── registry.rs    # DashMap<client_id, Arc<Session>>; 指针比对安全清理
├── data/
│   ├── router.rs      # DATA_CHANNEL_HELLO 四重校验 → 池/通道句柄
│   ├── tcp.rs         # 每规则 listener + 池(取一补一, 在途额度) + START_FORWARD
│   └── udp.rs         # 每规则 UdpSocket + 通道生命周期(建立/探活/重建, 服务端负责制)
├── rules/
│   ├── runtime.rs     # RuleRuntime{state, listener, target快照, pool, rule_cancel}
│   ├── store.rs       # sqlx 读写; 规则+端口+revision 同事务
│   ├── dispatch.rs    # 会话下发队列 + 每客户端串行域
│   └── allocator.rs   # 端口预留/冲突检测(TCP/UDP 分命名空间, 含通配)
├── api/               # axum: auth/user/clients/services/stats/events + 静态托管
└── events.rs          # broadcast 事件总线 → events 表 + SSE(P1)
```

### 4.1 规则生命周期（状态转换表为唯一口径）

状态集合：`disabled / bind_error / offline / pending / running / apply_error`。

- **listener 绑定**：`offline/pending/running/apply_error` 有；`disabled/bind_error` 无。
- **`running` 双条件**：listener 绑定成功 ∧ 当前会话已确认**包含该规则最新变更的 revision**（`confirmed` 标志随会话替换/离线重置）。
- `bind_error` = 服务端资源失败（端口被占等）；`apply_error` = 客户端回报应用失败。二者互不掩盖：`bind_error` 期间照常收集客户端确认（记 `confirmed`），恢复绑定后直接进入 `running`。

| 当前状态 | 事件 | 动作 | 次态 |
|---|---|---|---|
| （不存在） | 创建完成（§4.4 全序；`clients.enabled=0` 时仅落库） | bind listener | 在线→`pending`；离线→`offline`；客户端禁用→`disabled` |
| （不存在/恢复期） | bind 失败（含启动恢复） | 记事件；固定端口→60s 周期重试；`port_auto`→立即重分配（同事务改库） | `bind_error` |
| `bind_error` | 重试/重分配后 bind 成功 | 依 `confirmed`（已确认 ≥ 该规则最新变更 revision）：是→`running`；否→`pending`；无会话→`offline` | 如左 |
| `bind_error` | `APPLY_RESULT ok` | `confirmed=true`（确认 ≥ 该规则最新变更 revision；保持 `bind_error` 等待绑定恢复） | `bind_error` |
| `offline` | 会话建立（全量 SYNC 入队） | — | `pending` |
| `pending` | `APPLY_RESULT(revision ≥ 该规则最新入队变更的 revision, ok)` | — | `running`；随后服务端发预热 CREATE |
| `pending` | `APPLY_RESULT(ok=false)` | — | `apply_error` |
| `pending` | 确认超时 10s | 放弃增量改发全量（10s×3 → 30s 间隔） | `pending`（不变） |
| `running` | 本规则变更已提交入队 | 快照/listener 已在串行域发布；等新确认 | `pending`（转发不中断） |
| `running` | 会话取消/客户端离线 | 关池与通道；`confirmed` 重置 | `offline` |
| `pending`/`apply_error` | 会话取消/客户端离线 | 同上 | `offline` |
| `bind_error` | 会话取消/替换/客户端离线 | `confirmed` 重置（无池/通道可关） | `bind_error` |
| `running`/`pending`/`apply_error` | 会话替换（新会话注册） | 关旧池与通道；`confirmed` 重置；新会话队列先发全量 | `pending` |
| `disabled`（因客户端禁用，规则 DB 仍 `enabled=1`） | 客户端重新 enable | 对该客户端全部此类规则批量执行资格校验+bind+入队 | `pending`/`offline`/`bind_error` |
| `apply_error` | 全量重发后 `ok` | — | `running` |
| 上述任一（含 `bind_error`） | 规则 disable/删除；客户端禁用/删除 | **`rule_cancel`**（有 listener 则关 listener+池+该规则存量转发）；失效其在途请求；在线则 REMOVE 入队 | `disabled`（删除则移除运行态） |
| `disabled` | 编辑（PUT） | **仅落库；不推进 revision、不 bind、不发布运行态、不入队**（判定 = `services.enabled=0` 或 `clients.enabled=0`，与 §4.4 步骤 1 一致；enable 时才走全序） | `disabled` |
| `disabled` | 规则 enable | 若 `clients.enabled=0`：仅置 `enabled=1` 落库（不 bind，等客户端恢复）；否则资格校验 → bind → 入队（同创建） | 前者保持 `disabled`；后者 `pending`/`offline`/`bind_error` |

- 状态存内存，控制台 join 展示；`bind_error`/`apply_error` 附原因。
- 「确认超时」永远不是错误态；错误态只来自明确的失败信号（bind 失败 / 客户端回报失败）。

### 4.2 连接池（TCP）

- 每规则×每会话一个池。**在途额度**：`池中 + 在途请求 ≤ pool_size`（服务端签发侧保证）；迟到/超额抵达的数据连接（四重校验第 4 步失败）直接关闭。
- 取一补一：accept 用户连接 → 池取一条 + 立即签发 CREATE 补位；池空同步等待 ≤10s（总额，含死连接重取：写 `START_FORWARD` 失败即丢弃换下一条），超时关闭用户连接（快速失败不排队）。**丢弃死池连接时同样立即签发 CREATE 补位**，使 `池中 + 在途` 趋近 `pool_size`（达 §9.4 总限额时暂停签发，水位自然低于该值）。
- 池连接开启传输层 **TCP keepalive**（30s 周期，缩小死连接窗口）；池连接生命周期 = 会话生命周期（有界，无需空闲回收）。
- 每客户端数据连接总数（池+在途+转发中）受 §9.4 限额；达上限暂停签发 CREATE，池空用户连接照 10s 快速失败。

### 4.3 规则更新与端口切换

- 非 `bind_ip/bind_port` 字段变更：串行域内发布**服务端运行态快照**（`START_FORWARD` 即时取新 target）→ 入队下发。TCP 在途指派不受影响；UDP 目标变更走指派屏障（§3.6）。
- `bind_ip/bind_port` 变更：**先 bind 新口 → 同事务落库 → 域内换 listener → 关旧口**；旧口存量连接自然结束。
- **重叠地址更新 P0 拒绝**：新旧 `(proto, bind_ip, bind_port)` 因地址重叠无法共存（如 `0.0.0.0:52000 → 127.0.0.1:52000`）→ `OVERLAP_ADDR_UPDATE`，改用两步（disable→改→enable，§4.1 已保证 disabled 编辑不 bind）。
- 冲突判定与端口命名空间：**TCP 与 UDP 分别独立判定**；P0 仅支持 IPv4 字面地址，IPv6（含 `::` 双栈与 `IPV6_V6ONLY` 语义）留待 P1。

### 4.4 端口分配与提交顺序（双向失败补偿 + 串行域）

**每客户端串行域**：以下全部步骤（含发布）在同一把每客户端锁内按序执行；**等待客户端确认在域外**（不阻塞后续变更，后续变更排队）。DB 提交序 = 运行态发布序 = 入队序，三者一致，不存在旧发布覆盖新状态。

1. **资格校验**：`allow_ports` 白名单、`max_services_per_client`、与内存中全部活跃 bind 的冲突判定（TCP/UDP 分命名空间；含通配：`0.0.0.0:P` 与任何地址的 `:P` 冲突）。**跳步判定：`services.enabled=0` 或 `clients.enabled=0` 任一成立 → 仅执行步骤 4 落库（不推进 revision，跳过 2/3/5）**——适用于编辑、规则 enable 与创建（在禁用客户端上创建的规则直接落库为 `disabled` 态）；被禁用方的规则一律不 bind、不抢端口（与 §3.4 listener 存活条件、§4.6 资格查询一致）。恢复统一走状态表：客户端重新 enable 批量恢复；规则 enable 且客户端在线可用时重走全序。
2. **占位**：分配器预留端口（`bind_port=0` 时从白名单范围选空闲口）。
3. **bind 新 listener**（尚不对外服务）：失败 → 释放占位、返回错误、**DB 零变更**。
4. **单事务落库**：规则行（含实际端口、`port_auto`）+ `clients.config_revision+1` 同一 SQLite 事务。失败 → 关闭新 socket、释放占位、旧运行态保持、返回错误。
5. **发布**（仍在串行域内）：更新运行态快照（target 等）→ 原子换 listener（新口开始服务）→ 关旧口 → 写事件 → 若会话在线，变更入该会话下发队列。
   - 通知失败/客户端离线**不回滚** DB：期望状态已确立，待上线全量对齐。
   - （步骤 4 与 5 之间进程崩溃：重启恢复按 DB 绑定，状态自洽。）

并发创建同端口：步骤 1-5 串行于串行域+分配器锁；两管理员同时提交只有一个成功，另一个收 `PORT_CONFLICT`。

**自动分配端口的服务端重启**：优先复用落库端口 bind；被外部占用则立即重分配（同事务改库+revision+1），事件表记 `PORT_REALLOCATED` 告警。固定端口被占 → `bind_error` 60s 周期重试（§4.1）。

### 4.5 UDP 数据面（服务端负责制）

- **建立**：规则进入 `running` 后（或通道失效后）由服务端签发 `CREATE_DATA_CHANNEL{service_id, request_id}`；客户端拨号，`DATA_CHANNEL_HELLO` 回显 `request_id` 且为该规则**当前待建请求**时，成为该规则当前唯一通道。每规则同时至多 1 个待建请求。
- **失效检测（统一入口）**：① 首建/重建请求超时（CREATE 后 10s 未见回显）→ 退避重建；② 通道读循环 EOF/错误 → 退避重建；③ 通道层探活失败（S 每 30s 发 PING，连续 2 周期无 PONG，§3.3）→ 退避重建；④ 会话取消 → 不走退避，由会话重建路径处理（新会话 SYNC 确认后重建通道）。
- **重建**：检测到 ①②③ → 使旧请求/旧句柄失效 → 退避重签 CREATE（1s/2s/4s…封顶 30s）。重建期间用户 UDP 包**丢弃并计数**，首次触发落事件。
- **代次隔离**：新通道入表即替换旧句柄；迟到回显旧 `request_id` 的连接关闭（四重校验第 4 步）。服务端 `user_src` 映射表与客户端 per-source UDP socket **归属通道代次**：通道关闭/替换时，服务端清其映射、客户端取消其 socket 任务并回收（60s 空闲 GC 仅用于通道内存活条目）——不跨代复用，避免旧回复进入新通道。
- **配置变更屏障**：UDP 规则目标变更 = 关旧通道 + 失效在途请求 → 下发 → `APPLY_RESULT` → 签发新 CREATE（§3.6），杜绝新包走旧目标。
- **恢复时间预算**（替代任何固定秒数承诺）：`失效检测（探活最坏 60s，或读循环即时）+ 重建退避累计 + CREATE→HELLO（≤10s，已含客户端拨号与传输握手）`；验收按此公式在链路可达前提下测量（§11）。
- 服务端维护 `user_src ↔ 通道` 映射（60s 空闲 GC）；报文上限 64KiB。

### 4.6 启动恢复

进程启动：**资格查询 = `services.enabled=1 JOIN clients WHERE clients.enabled=1`**（与创建/enable 共用同一资格函数）→ 逐条 bind（**失败记 `bind_error`** 并落事件，不阻塞其他规则；被禁用客户端的规则不参与恢复、不抢端口；`port_auto` 规则被占则立即重分配）→ 等客户端上线全量 SYNC 对账。DB 是期望状态唯一事实源，内存运行态可由 DB + `APPLY_RESULT` 重建。

---

## 5. rh-client 设计

```
crates/rh-client/src/
├── main.rs            # rh-client.toml: server_addr, transport, client_id, secret
├── session.rs         # Connecting → Challenged → Authed → Syncing → Running
│                      #   →(Backoff→Connecting); PONG 超时/断开→Backoff
├── services/
│   ├── mod.rs         # 规则集原子替换/增删 + APPLY_RESULT 回报(含 ACK 重放); 
│   │                  # 不一致→SYNC_REQUEST
│   ├── tcp.rs         # 被动拨号器: 收 CREATE{service, request} → 拨号+回显 HELLO;
│   │                  # START_FORWARD(帧内 target)→连目标(≤10s)→裸流搬运
│   └── udp.rs         # 被动拨号器(同上) + UDP_PACKET 解包→per-source socket
│                      # (归属通道代次, 通道关闭即取消) + 通道层 PING→PONG
└── backoff.rs         # 指数退避 1s→60s, 成功归零
```

- 客户端**不自发拨号**：一切数据连接由服务端 `CREATE_DATA_CHANNEL` 触发（预热亦然），因此全量 SYNC 重发/更高版本 SYNC 不会引发重复拨号（是否补拨由服务端按池在途额度决定）。
- 规则集应用即内存原子替换（§3.3），无本地端口绑定，因此无"部分应用失败"。

---

## 6. 数据库（sqlx + SQLite）

```sql
CREATE TABLE users (
  id            INTEGER PRIMARY KEY,
  username      TEXT NOT NULL UNIQUE,
  password_hash TEXT NOT NULL,          -- argon2
  role          TEXT NOT NULL DEFAULT 'admin',
  created_at    INTEGER NOT NULL        -- unix ms
);

CREATE TABLE clients (
  id              TEXT PRIMARY KEY,     -- uuid v4
  name            TEXT NOT NULL UNIQUE,
  secret          TEXT NOT NULL,        -- 64 hex; HMAC 校验需原文(§3.4)
  enabled         INTEGER NOT NULL DEFAULT 1,
  remark          TEXT NOT NULL DEFAULT '',
  config_revision INTEGER NOT NULL DEFAULT 0,   -- 每客户端期望配置版本(与规则变更同事务推进)
  created_at      INTEGER NOT NULL,
  last_online_at  INTEGER,
  client_version  TEXT, hostname TEXT, os TEXT
);

CREATE TABLE services (
  id           INTEGER PRIMARY KEY AUTOINCREMENT,
  client_id    TEXT NOT NULL REFERENCES clients(id) ON DELETE CASCADE,
  name         TEXT NOT NULL,
  proto        TEXT NOT NULL CHECK (proto IN ('tcp','udp')),
  bind_ip      TEXT NOT NULL DEFAULT '0.0.0.0',   -- P0 仅 IPv4 字面量
  bind_port    INTEGER NOT NULL CHECK (bind_port BETWEEN 0 AND 65535), -- 落库后为实际端口
  port_auto    INTEGER NOT NULL DEFAULT 0,  -- 1=自动分配(重启重分配语义见 §4.4)
  target_host  TEXT NOT NULL,
  target_port  INTEGER NOT NULL,
  pool_size    INTEGER NOT NULL DEFAULT 2,
  enabled      INTEGER NOT NULL DEFAULT 1,
  extra        TEXT NOT NULL DEFAULT '{}',
  created_at   INTEGER NOT NULL,
  updated_at   INTEGER NOT NULL,
  UNIQUE (client_id, name)
);

CREATE TABLE events (
  id      INTEGER PRIMARY KEY AUTOINCREMENT,
  ts      INTEGER NOT NULL,
  level   TEXT NOT NULL,               -- info/warn/error
  source  TEXT NOT NULL,               -- auth/client/service/system
  message TEXT NOT NULL
);
```

运行态（§4.1 六状态、在线状态、流量计数）只存内存，重启由 §4.6 重建，不落库。

---

## 7. Web API（axum，前缀 `/api`，JWT Bearer）

**响应与错误语义**（与 vben `requestClient` 行为对齐，见 §8.2）：

- 业务成功：HTTP 200 + `{ "code": 0, "data": ..., "message": "ok" }`，前端取 `data`。
- 业务失败：HTTP 200 + `{ "code": <非0>, "message": "..." }`，前端弹错误提示。
  **`message` 为面向控制台的中文说明**(事件表「消息」列与 toast 均为中文,2026-10-10 起);
  机器可读契约以数字 `code` 为准,个别错误保留符号前缀(`PORT_CONFLICT:` 等)便于日志检索。
- **认证失效：HTTP 401 状态码**（vben 拦截器依据 HTTP status，不是业务 code）。

### 7.1 认证与用户（按 vben 默认调用路径设计，待 M4 联调验收）

| 方法 | 路径 | 请求/响应（`data` 字段内容） |
|---|---|---|
| POST | `/api/auth/login` | `{username,password}` → `{ accessToken }`（字段名 **accessToken**；不启用 refresh） |
| GET | `/api/user/info` | → `{ userId, username, realName, avatar, desc, homePath:"/rabbithole/overview", roles:["admin"] }` |
| GET | `/api/auth/codes` | → `[]`（登录链路会调用，必须存在） |
| POST | `/api/auth/logout` | **须携带 Bearer**；校验通过记审计，回 204；无效凭据回 401。**P0 登出不撤销已签发 JWT**（自然过期 12h），已知取舍见 §13 |
| POST | `/api/auth/password` | 须携带 Bearer；`{old_password,new_password}`。验旧密码（argon2，错则 code=1001 + warn 审计）、新密码 ≥8（code=1002）、更新落库后**轮换会话签名密钥**（所有已签发 JWT 立即失效，前端随即可强制重登），回 `{relogin:true}` |

登录限速 governor;连续失败写 events。JWT HS256 12h;401 触发前端跳登录。

**首启引导(2026-10-09 增)**:users 表为空时服务端启动自动创建 `admin`,
密码为 24 字符随机串(两个 UUID v4 simple 拼接收窄),**仅在 stdout 横幅一次性展示**
(tracing 事件只记"已创建"不含密码);已有任何用户则跳过(重启幂等,不再展示)。
`rh-server admin create` 保留为追加管理员的可选方式。

> 对接注意：vben 的 `logoutApi` 默认使用不带 Bearer 注入的 `baseRequestClient`——**前端需一处一行改动**（`logoutApi` 显式附 `Authorization`），见 §8.2。

**双凭据(2026-10 增,API Key)**:除浏览器 JWT 外,REST API 同时接受
API Key —— `Authorization: Bearer rhk_<8hex>_<64hex>`。靠 `rhk_` 前缀
分流(不做"JWT 失败再试 Key"的顺序试探);`key_hash = sha256(完整 bearer 串)`
只存哈希,明文仅创建/轮换时返回一次。判权为 scope 位掩码(每资源 2 位,
偶=读奇=写;`READONLY=5461`/`OPS=5503`/`FULL=16383`),GET/HEAD 判读、
其余判写,`allowed==0` 一律 fail-closed 403。session 类接口
(logout/password/user/info/auth/codes/menu/all 与 api-keys 管理本身)
整组拒绝 Key。curl 示例:

```bash
# 建一把只读 key(需 JWT;明文仅此一次)
curl -X POST http://host:8080/api/api-keys -H "Authorization: Bearer $JWT" \
  -H 'Content-Type: application/json' -d '{"name":"ci","preset":"readonly"}'
# 用 key 调业务接口
curl http://host:8080/api/clients -H "Authorization: Bearer rhk_xxxxxxxx_..."
```

### 7.2 业务接口

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/api/clients` | 列表 join 内存会话：`online`、`online_since`、`reported_revision` |
| POST | `/api/clients` | `{name,remark}` → 生成 id+secret，**secret 仅本次返回** |
| PUT | `/api/clients/{id}` | name/enabled/remark；`enabled=0` = 触发其全部规则 `rule_cancel` 入 `disabled`；`enabled=1` = 按状态表批量恢复（资格校验+bind+入队） |
| POST | `/api/clients/{id}/secret:rotate` | 轮换 = 仅踢会话（listener 不动），新 secret 仅本次返回 |
| DELETE | `/api/clients/{id}` | 级联删 services + 全部规则取消 + 踢线 |
| GET | `/api/services?client_id=&page=` | 列表含运行态 `status`: `offline/pending/running/apply_error/bind_error/disabled` + `status_reason` |
| POST | `/api/services` | 按 §4.4 全序；错误码：`PORT_CONFLICT`/`PORT_DENIED`/`LIMIT_EXCEEDED`/`VALIDATION`/`OVERLAP_ADDR_UPDATE`；成功返回含实际 `bind_port` |
| PUT | `/api/services/{id}` | 更新并热下发（§4.3）；`disabled` 状态下仅落库（§4.1）；同上错误码 |
| POST | `/api/services/{id}/enable` `/disable` | disable = `rule_cancel` + REMOVE 入队；enable = 资格校验+bind+入队（`clients.enabled=0` 时仅置位落库，不 bind，§4.1） |
| DELETE | `/api/services/{id}` | 删除 + `rule_cancel` + REMOVE 入队（离线则靠全量 SYNC 隐式移除） |
| GET | `/api/stats/overview` | `{clients_total, clients_online, services_total, services_running, conns_current, bytes_in, bytes_out}` |
| GET | `/api/events?limit=100&level=` | 倒序 |
| GET | `/api/api-keys` | API Key 列表(含已吊销);**不回显 key_hash/明文**;session-only(整组拒绝 Key) |
| POST | `/api/api-keys` | `{name,preset?,scopes?,expires_in_days?}` → `{id,name,prefix,scopes,key}`;**明文 key 仅此一次**;preset=readonly/ops/full,scopes 是 u32 位掩码且优先于 preset |
| DELETE | `/api/api-keys/{id}` | 软删(`enabled=0`,保留审计痕迹,name 不可复用) → `{deleted:true}` |
| POST | `/api/api-keys/{id}/rotate` | 换 prefix+hash,旧 key 立即失效 → `{id,prefix,key}`(新明文仅此一次) |

P1 增加 `/api/stats/stream`（SSE）。

---

## 8. 前端设计

> **现状（2026-10-12 第二次重写）**:前端为顶层 `console/` 独立工程，技术栈
> **Vite 7 + React 19 + TypeScript(strict) + Tailwind v4 + shadcn/ui**，数据层
> axios + TanStack Query + zustand，表格 TanStack Table，表单 react-hook-form + zod。
> `web/`(vben)已于 2026-10-10 删除；2026-10-10 至 10-12 期间的 Umi Max + antd v6
> 版本也已整体替换。§8.1 的 vben 基座条款**废止**(历史存档)；§8.2 对接契约语义不变，
> 由 console/ 承接(axios envelope 解包、HTTP 401、改密强制重登均已在 console/ 实现；
> logout 由 axios 拦截器统一附 Bearer，不再需要模板期的一行改动)。

### 8.1 基座锁定与裁剪边界（已废止,历史存档）

- **锁定 commit `d27a5b8a`**（main 分支 2026-10-07）。后续换 commit 为主动行为，需重跑 §8.2 对照。工具链：Node `^22.18.0 || ^24.12.0`，pnpm 锁定 `11.16.0`（corepack）。
- `web-antd` 依赖 `workspace:*` 与 `catalog:`，**不能单独剪出应用目录**。裁剪规则：
  - **保留**：根 `package.json`、`pnpm-workspace.yaml`（含 catalog）、`pnpm-lock.yaml`、完整 `packages/**`、完整 `internal/**`、**`scripts/turbo-run/**` 与 `scripts/vsh/**` 包实体**、根 `postinstall`（stub 构建 `internal/vite-config` 等）。
  - **可裁**：`apps/web-ele`、`apps/web-naive`、`playground`、`docs`。
  - **必改**：生产 `VITE_GLOB_API_URL=/api`；关闭内置 mock。

### 8.2 后端对接契约（按模板实际调用链设计，待联调）

| 项 | 处理 |
|---|---|
| 登录 | `POST /api/auth/login`，auth store 解构 **`accessToken`**（后端按 §7.1 设计） |
| 用户信息 | 登录后并行请求 `/api/user/info` + `/api/auth/codes`（静态路由模式也调用 codes） |
| 响应解包 | `requestClient` 按 `code===0` 取 `data`，错误提示读 `error ?? message`——与后端 envelope 一致 |
| 401 | 拦截器依据 **HTTP 401 状态码**；后端认证失效一律回 HTTP 401 |
| logout | **v1.2 修正**：`logoutApi` 默认用 `baseRequestClient`（无 Bearer 注入）——前端一处一行改动：`logoutApi` 显式附 `Authorization`（模板内 `apps/web-antd/src/api/core/auth.ts`）。后端记审计；**本地清 token ≠ 服务端撤销**，P0 无 JWT 撤销（§13） |
| refresh | 模板默认 `enableRefreshToken=false`——P0 保持，不实现 `/api/auth/refresh` |
| token 持久化 | 模板默认持久化（dev=localStorage，prod=SecureLS），与后端无关 |
| 路由权限 | P0 单角色 admin，frontend 静态路由（`/menu/all` 不会被调用） |

### 8.3 页面规划

| 路由 | 页面 | 要点 |
|---|---|---|
| `/auth/login` | 登录 | vben 内置页（后端按 §7.1 契约实现；演示登录变体已裁剪） |
| `/rabbithole/overview` | 总览 | 统计卡片 + 最近事件流 |
| `/rabbithole/clients` | 客户端管理 | 在线徽标/版本/hostname；新建抽屉展示一次性 secret；轮换（仅踢线）/禁用 |
| `/rabbithole/services` | 转发规则 | `status` 徽标（offline/pending/running/apply_error/bind_error/disabled）；新建/编辑弹窗（客户端、协议、服务端口留空=自动、目标地址）；启停即时生效 |
| `/rabbithole/visitors` | 访问器 | STCP/SUDP/XTCP visitor 管理 |
| `/rabbithole/settings` | 设置 | 修改密码（成功后全量会话失效并强制重登，§7.1） |

---

## 9. 安全设计（量化）

### 9.1 传输

默认 Noise（snowstorm，`Noise_NK_25519_ChaChaPoly_BLAKE2s`，`rh-server --genkey` 生成；**客户端配置固定服务端公钥**，防中间人）；可选 TLS(rustls)；明文 tcp 仅本机调试且启动告警。

### 9.2 协议认证

nonce 挑战（§3.4）：`HMAC-SHA256(secret, "rh2:"+client_id+":"+nonce_hex)`；nonce 单次有效（取出即删、TTL 30s、**绑定签发连接**）；AUTH_RESPONSE 以 DB 最新 enabled/secret 校验；注册与禁用/轮换在每客户端锁内互斥。无时钟假设；重放在任何时点（含服务端重启后）均失败。数据连接四重校验（会话/归属/启用/请求号）。

### 9.3 超时（全部量化）

| 项 | 值 |
|---|---|
| 传输层握手 | 5s |
| 首帧等待 | 5s |
| AUTH_CHALLENGE → AUTH_RESPONSE | 10s（nonce TTL 30s） |
| CREATE → DATA_CHANNEL_HELLO（请求期限） | 10s |
| 池空等待补拨（用户连接总额，含死连接重取） | 10s，超时快速关闭 |
| START_FORWARD 后连接本地目标 | 10s |
| APPLY_RESULT 等待 | 10s → 放弃增量改全量（10s×3，其后 30s 间隔） |
| 心跳丢失踢线 | 3 × interval（90s） |
| 客户端判定失联重连 | 2 × interval（60s） |
| UDP 通道探活 | PING 每 30s，2 周期无 PONG 判死 |
| UDP 通道重建退避 | 1s 起 ×2，封顶 30s |
| 固定端口 bind_error 重试 | 60s 周期 |

### 9.4 资源限额（server 配置项）

| 项 | 默认 |
|---|---|
| 帧长上限 | 1 MiB |
| 每客户端并发数据连接（池+在途+转发中） | 256 |
| 每池在途+池中连接 | ≤ pool_size |
| 每规则 UDP 会话 | 1024（60s GC） |
| 控制连接发送队列 | 256（满则断开，慢客户端保护） |
| 每 IP 未认证并发连接 | 64（挑战握手成本约束） |
| pool_size 上限 | 16 |
| UDP 报文上限 | 64 KiB |

### 9.5 管理面

argon2 密码、JWT 12h、登录 IP 限速;管理员账号首启自动引导(随机密码 stdout
横幅一次性展示,§7.1),控制台「设置」页支持改密(验旧密 + 全量会话失效);
`rh-server admin create` 仍可追加管理员;secret 明文列信任边界声明见 §3.4。

### 9.6 管理 API 暴露

默认 `127.0.0.1`。公网访问要求（二选一，检测 `0.0.0.0` 且无 TLS 时启动告警）：前置反代 TLS，或启用内置 `api.tls`（rustls）。

### 9.7 审计

登录/失败、登出、密钥轮换、客户端增删/禁用、规则增删改、上下线、端口重分配、UDP 通道重建、bind 失败，全部落 events。

---

## 10. 传输层接口边界（为 QUIC 预留）

P0 仅实现字节流传输（tcp/noise/tls/ws，源自 rathole），trait 按以下边界定义，保证 P2 加 QUIC 时不重构协议层：

```rust
// 连接与流分离: P0 实现"一连接一流", QUIC 实现可"一连接多流"
trait Transport { type Listener; type Conn; /* accept/connect */ }
trait Conn: Send {
    fn open_stream(&self) -> BoxFuture<Stream>;   // 发起方
    fn accept_stream(&self) -> BoxFuture<Stream>; // 接受方(P0: 即连接本身)
}
// 可选能力探测, P0 无实现者
trait DatagramCapable { fn max_datagram_size(&self) -> usize;
                        fn send_datagram(&self, buf: Bytes); fn recv_datagram(&self) -> ...; }
```

同时约束：会话（控制）与数据通道统一建模为"控制连接上的一个流 + 数据流集合"；代理类型（proxy kind）、共享监听器（vhost/tcpmux 的多路由一监听）、目标连接器（plugin 化：`host:port` 是 P0 唯一实现，Unix socket/内置 socks5 是并列实现）在 rh-server 模块边界预留位置，详见 P1/P2。

---

## 11. 里程碑与验收（含失败场景）

| 里程碑 | 内容 | 验收 |
|---|---|---|
| M1 协议骨架 | rh-protocol + rh-transport（迁移+依赖升级），nonce 认证+心跳+会话替换 | 集成测试：正常认证；坏 secret；过期 nonce；**重放 AUTH_RESPONSE 拒绝（nonce 已消费）**；**挑战期间禁用客户端 → 注册失败**；**挑战期间轮换密钥 → 认证失败**；**其他连接送达的响应被拒（nonce 绑定连接）**；心跳超时踢线；**重复上线旧会话连接全部关闭、旧队列确认作废**；并发认证互不干扰 |
| M2 数据面 | tcp/udp 转发、池取一补一（在途额度）、自包含指派、端口分配器 | `curl`/`nc`/`dig` 打通；**SSH（目标先发协议）banner 正常**；**目标拒连→用户连接立即关闭**；**规则目标热更新：新连接走新目标、在途旧连接不受扰**；**迟到旧 request_id 回显被拒**；**重复/更高版本 SYNC 不引发重复拨号**；**池写指派失败自动换连接（≤10s 总额）**；**UDP 通道杀死后按 §4.5 预算公式恢复**；**UDP 首建失败自动退避重试**；**通道探活 60s 判死**；**端口冲突创建失败且 DB 无残留**；**bind 成功后 DB 故障不产生半提交**（故障注入）；断线重连恢复（TCP 重连成功起 ≤30s）；记录单通道吞吐基线（本机回环 iperf3，参考 ≥500Mbps） |
| M3 管理面 | §7 全部 API + 迁移 + JWT | API 集成测试全绿：权限、限速、参数校验、`PORT_CONFLICT`/`LIMIT_EXCEEDED`/`OVERLAP_ADDR_UPDATE`、HTTP 401 语义、**并发创建同端口仅一个成功**、**禁用客户端后重启不恢复其 listener**、**disabled 状态编辑不占用端口、enable 后才绑定**、**固定端口被占 → bind_error 且 60s 重试、自动端口被占 → 重分配** |
| M4 前端 | vben 裁剪接入 + 四页面 + 静态托管 | 按 §8.1 边界裁剪后 `pnpm build` 通过；登录链路（login→user/info→codes→logout 带 Bearer）真实联调；浏览器完成「建客户端→拿密钥→建规则→验证转发」≤2 分钟；`status` 徽标（六状态）与真实状态一致 |
| M5 加固 | 优雅关停、事件、重启恢复 | kill -TERM 无连接/fd 泄漏；**服务端重启后仅恢复双方 enabled 的规则**，`port_auto` 被占重分配且事件告警、固定端口被占进入 bind_error；控制台操作全量留痕 |

---

## 12. P1 / P2 路线（frp 对齐清单，不承诺排期）

**P1**：http/https vhost（hyper 反代 + domain→location 路由，共享监听器）、UDP 双模式（asport SPEC 思路重写）、IPv6 支持（含 `IPV6_V6ONLY` 语义）、重叠地址热更新（停旧-绑新-失败恢复策略）、压缩/限速、PROXY protocol（ppp）、Prometheus、SSE 面板、客户端本地静态规则、TCP/HTTP **服务健康检查**（客户端探测 + 摘除，frp 行为）。

**P2**：stcp/sudp visitor、xtcp P2P 打洞、**tcpmux**（HTTP CONNECT 按 host 分流）、客户端插件系统（unix socket/socks5/静态文件）、group 负载均衡、OIDC、**KCP 传输**、QUIC 传输（quinn + asport-quinn-hyphae 双许可 crate，走 §10 接口）、**SSH Tunnel Gateway**（`ssh -R` 直接注册）、**VirtualNet**（frp 侧为 Alpha，仅评估）。

> 对齐状态：frp 8 种代理类型中 P0 覆盖 tcp/udp，P1 覆盖 http/https，P2 覆盖 stcp/sudp/xtcp/tcpmux；**9 种客户端插件已实现**（frp 基线为 10 种，缺 `virtual_net` —— 已判定不做，见 `docs/not-supported.md` §4）；KCP/SSH Gateway 在 P2/评估。
> **排除范围仅限协议兼容层**：v1/v2 双协议、INI legacy、与存量 frpc/frps 互操作，明确不做。其余 frp 业务能力未排期者状态为"待评估"，不视为排除。

## 13. 已知取舍

- 认证 +1 RTT（nonce 挑战三消息）：连接建立为低频事件，换取零时钟假设与彻底的重放免疫。
- 数据连接全服务端指令发起（含 TCP 预热）：统一 `request_id` 校验、消除双责任方，代价是预热多一跳控制消息（可忽略）。
- UDP over TCP 帧（P0）高丢包链路表现一般 → P1 双模式；UDP 重建期间用户包丢弃（计数+事件；恢复时间按 §4.5 预算公式，链路可达前提下典型 <90s）。
- 控制面 JSON：低频小消息，性能无影响，换调试与演进能力。
- 客户端重连瞬间存量用户连接断开（会话取消主动关闭）：与 frp 行为一致。
- 登出不撤销已签发 JWT（12h 自然过期）：P0 无撤销机制；若需即时失效，P1 引入短 TTL + refresh 或黑名单。**改密除外**：`POST /api/auth/password` 成功后轮换会话签名密钥，全部已签发 JWT 立即失效（2026-10-09 增，e2e.sh boot 用例验证旧令牌 401）。
- 重叠地址热更新 P0 拒绝（`OVERLAP_ADDR_UPDATE`）：用两步操作替代（disabled 编辑语义已支持），P1 实现停旧-绑新。
- 目标连接失败不做应答帧（直接断开）：省一帧往返，用户侧行为等价连接重置。
- secret 明文存库：校验需原文，单机文件系统为信任边界，P1 可选信封加密。
- 固定端口被外部占用时规则停在 `bind_error` 60s 重试，不自动改口（用户显式指定的端口不应被悄悄更换）。

---

## 附录:实现状态(2026-10-08 交付时点)

本设计文档为输入契约;实现以仓库代码与 docs/requirements-matrix.md 状态为准。关键实现口径与差异:

- **帧与消息**:RHTP 11 字节头,MAX 1MiB;控制消息 JSON;会话队列消息为枚举信封,
  握手(AUTH_RESPONSE/HELLO_ACK)与数据通道指派(START_FORWARD/START_VISITOR_RELAY)
  为裸帧。`SyncEntity::Proxy` 与 `ServerMessage::ServiceUpdate` 载荷已 Box 化
  (内存布局优化,序列化不变)。
- **read_frame 残留字节契约(实现强制)**:`read_frame` 会把同批到达的后续帧留在
  调用方缓冲;任何"读一帧后把流转交下一处理者"的代码必须以 `PrependedIo`
  (rh-transport)前传残留。客户端 open_data_channel、服务端 mux 数据接受、
  裸模式首帧路由均已执行;新增路径必须沿用。
- **数据通道配对**:配对表(relay_pairs)登记必须先于 CREATE 帧发送
  (`AppState::issue_relay_pair`),消除 localhost 回环竞速;request_id 一次性、10s TTL。
- **心跳**:服务端 30s 检查、90s 无任何客户端帧判死;客户端 30s Ping。
  会话内数据通道限额按流存活期占用(QuotaIo),关闭即归还。
- **SSH Tunnel Gateway**:russh 0.64;认证=密码(客户端密钥,常量时间)或
  authorized_keys(按 key_data 比较;行尾注释恰为客户端 ID(UUID 形态)时
  该密钥绑定该客户端,ssh-keygen 默认的 user@host 注释视为全局密钥);
  会话通道接受并对 exec/shell 空跑(仅保持);
  forwarded-tcpip 的 connected_address 与注册地址一致;断开级联释放全部监听。
  安全策略(R10 收紧):绑定地址仅环回(含 localhost);客户端指定端口须过
  allow_ports 白名单与占用检查(与常规规则同策略);每连接转发数量上限 16;
  自动分配仍走 auto_port_ranges。
- **健康检查**:客户端探测本地目标,TCP/HTTP 两型;失败 max_failed 次转
  unhealthy 并被 group 摘除;恢复即报。group 为同组健康成员轮询。
- **DNS**:客户端 `dns_server` 配置 + 最小 A 查询(dnsquery.rs),失败回退系统解析。
- **已知限制**(详见 docs/acceptance.md):前端生产构建空白视图;
  ~~TLS/WS/QUIC/HTTPS-vhost/头改写/插件的自动化 E2E 证据缺口~~ 已全部补齐;
  OIDC 曾实现并验证后按用户决策移除(2026-10-09)。
- **§4 十条契约独立审计(R8)**:C-1/C-2/C-6/C-8/C-9/C-10 的实现缺口已在交付前修复
  (running=已绑定∧已确认、端口变更先试绑失败不动 DB、规则行+revision 单事务、
  禁用方跳步、在途请求失效与级联关闭、UDP 目标变更原位发布)。macOS 平台
  通配 bind 可与具体地址监听共存,试绑补环回探针对齐 Linux 语义。
  全量核验与修复记录见 docs/review-log.md R8。
- **控制台生产部署(2026-10-09 补记,历史存档)**:`pnpm build` 产物经 `[web].static_dir`
  由 rh-server 单进程托管(SPA 回退 200、未知 /api/* 返回 JSON 404)。曾因
  pnpm 图 vue-router 双实例导致登录后空白,已以 `resolve.dedupe` 修复并按
  生产构建复跑浏览器验收。以上均为 vben 时代记录;现役控制台为 React 工程,
  无 vue-router 依赖。
- **单二进制内嵌控制台(2026-10-10 补记)**:`build.rs` 检测
  `console/dist` 存在时置 `embed_ui` cfg,`rust-embed` 将 dist
  编译进 rh-server(约 +3.2MB);未配置 `static_dir` 时由内嵌资产托管,
  语义与磁盘分支一致(SPA 回退/`/api` JSON 404),`static_dir` 显式配置
  优先于内嵌。无 dist 的纯 Rust 检出仍可构建(二进制不含 UI,API 不受影响)。
  验证:tests/e2e-embed.sh(9/9)。旧路径 `web/apps/web-antd/dist` 已随
  2026-10-10 CUT 移除。
- **前端基座重写(2026-10-10 决策,2026-10-12 再次替换)**:管理面前端先由
  vue-vben-admin 裁剪版切换为 React 19 + Ant Design v6 独立工程(顶层 `console/`,
  Vite 产物托管方式不变:`[web].static_dir` / rust-embed,登录链路与 envelope
  契约逐项保留);2026-10-12 又整体重写为 **Vite + React 19 + Tailwind v4 +
  shadcn/ui**,彻底移除 Umi Max / antd / ProComponents。
  §8.1 基座锁定与裁剪边界条款**废止**,§8.2 对接契约语义由新工程承接。
  详见 docs/design-parity.md §10(F9)、docs/console-ui-redesign.md 与
  docs/review-log.md R20–R33。
