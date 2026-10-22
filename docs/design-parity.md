# rabbithole 能力补齐设计(对标 nps,排除多用户)— v0.2

> **时效说明(2026-10-22 补注)**:本文是 2026-10-10 的能力补齐设计,**能力口径仍有效**,
> 但前端实现细节已两次演进:先由 vben 换为 Umi Max + antd v6(本文决策 5),
> 2026-10-12 又整体重写为 **Vite + React 19 + Tailwind v4 + shadcn/ui**。
> 凡本文提到"antd v6 / Umi Max / ProComponents / web-antd 路径 / `/rabbithole/*` 路由"
> 的具体实现锚点,**以 `console/` 现役代码与 `docs/review-log.md` R20 之后的条目为准**;
> 后端契约(F1–F8 的数据模型与 API 语义)不受影响。

> 范围:补齐与 nps 网页端对标中识别出的能力差距,**不含多用户/注册/角色体系**(明确非目标,见 §15)。
> 基线:docs/design.md v1.4 及其附录的实现状态。本文档不改变 RHCP 协议帧格式、认证握手与规则状态机;
> 唯一的协议载荷变更是 `HttpProxyConfig` 增一个可选字段(§5)。
> 差距来源(2026-10-10 分析):三类——后端真正缺失、后端已有但网页端未暴露、体验细节。
>
> 决策记录:
> 1. **流量统计为第一优先级**:现状 `Totals.bytes_in/bytes_out` 从未被递增(data.rs `copy_limited` 只限速不计数),总览页两张流量卡片恒为 0,属"空壳展示";且 F2 配额、F8 图表均依赖计数。
> 2. `ProxySpec` 在 DB 中以 JSON blob 存储(db.rs),spec 内新增字段**零迁移**;只有 clients 配额列、traffic、certs 三处需要 `0002` 迁移。
> 3. 服务端入口的用户连接(tcp/udp/vhost/tcpmux)统一计量、统一受限额约束;visitor 的入口在访问端客户端,不占服务端连接配额,其中继流量按目标代理归属计量(§2.2)。
> 4. **侧边栏按转发类型拆分(2026-10-10,采纳 nps 信息架构)**:每类型独立菜单 + 参数化页面组件(nps 即此拆法:7 个类型菜单底层共用参数化模板);访问器并入密钥访问页;旧 `/rabbithole/services`、`/rabbithole/visitors` 路由重定向兼容。详见 §8.0。
> 5. **前端基座重写(2026-10-10 决策)**:以 React 19 + Ant Design v6 独立工程重写管理面(新建顶层 `console/`,迁移期与 `web/` 并存,切换后删除);修订 design.md §8——基座锁定与裁剪边界条款废止,§8.2 对接契约语义保留并由新工程承接。详见 §10(F9)。

---

## 0. 背景与差距清单

| 编号 | 能力 | 差距类型 | nps 对应 |
|---|---|---|---|
| F1 | 流量统计(全局/分客户端/分规则,持久化) | **后端缺失**(字段存在但从未计数) | 流量展示、实时带宽 |
| F2 | 客户端级配额(限速/最大连接/流量上限/最大隧道数) | **后端缺失**(仅全局配置) | 客户端列表配额字段 |
| F3 | 泛解析(通配域名) | **后端缺失**(三处 vhost 匹配均为严格相等) | 泛解析 |
| F4 | 按域名 404 页面 | 后端弱化(仅全局配置文件项) | 域名解析 404 配置 |
| F5 | HTTP 静态资源缓存 | **后端缺失** | 缓存 |
| F6 | HTTPS 多证书(SNI 选证书 + 网页管理) | **后端缺失**(单证书、SNI 仅用于路由) | 多证书 |
| F7 | 控制台重组:侧边栏按类型拆页 + 表单补齐(locations/auth/头改写/group/健康检查/插件/限速/PROXY proto/pool_size) | 后端已有、**前端未拆页未暴露** | 每类型独立菜单(nps 拆法)、域名解析高级项、隧道表单 |
| F8 | 接入引导(客户端配置片段)、总览图表、分规则连接数 | 体验缺失 | 复制启动命令、仪表盘图表 |
| F9 | 前端基座重写(React + Ant Design v6,新顶层 console/ 工程) | 基座替换(自驱需求) | —(超出 nps 对标;承接 F6/F7/F8 的前端交付) |

**不在本档范围**(后端已实现且与本档正交):`route_by_http_user`、批量 API、Prometheus 端点——它们的缺口同属 F7 表单暴露,随 F7 一并落地。(原列于此的 `allow_users` 已整体移除:访问授权以管理员配置的访问器记录为准——记录绑定客户端与目标规则且客户端无上行注册通道——规则侧客户端白名单冗余且易自堵。)

---

## 1. 模块总览与依赖

| 模块 | 层 | 依赖 | 里程碑 |
|---|---|---|---|
| F1 流量统计 | 数据面 + 管理面 + 前端 | — | M1 |
| F2 客户端配额 | 数据面 + 管理面 + 前端 | F1(flow_limit 判定用累计值) | M1 |
| F8 接入引导/图表 | 前端为主 | F1(图表数据源) | M1(引导)/R3(图表) |
| F3 泛解析 | 数据面(vhost) | — | M2 |
| F4 按域名 404 | 数据面(vhost) + 前端 | — | M2 |
| F5 HTTP 缓存 | 数据面(vhost) | — | M2 |
| F6 多证书 | 数据面(tls)+ 管理面 + 前端 | F3(通配 matcher 复用) | M3 |
| F7 控制台重组与表单补齐 | 前端 | —(字段后端均已存在) | R2 |
| F9 前端基座重写 | 前端工程 + 构建接线 | 承接 F6/F7/F8 前端交付 | R0–R3 + CUT |

---

## 2. F1 流量统计

### 2.1 现状

- `Metrics { totals: Mutex<Totals>, ... }`(state.rs:118),`Totals { conns_current, bytes_in, bytes_out }`。
- 全仓库仅 `conns_current` 有增减(data.rs:269/335、sshgateway.rs);**`bytes_in/bytes_out` 无任何写点**。
- Prometheus 仅有 `rabbithole_connections` GaugeVec 与 `rabbithole_clients_online`(state.rs:130-139),无字节指标。

### 2.2 计数模型

**方向定义**(与 API/前端现有口径一致,不改名):

- `bytes_in`:服务端**用户入口侧**读入的字节(用户 → 服务端监听口)。
- `bytes_out`:服务端写往用户侧的字节(服务端 → 用户)。

**计数点**(全部在服务端,客户端不参与,避免上报失真):

| 路径 | in 计数点 | out 计数点 |
|---|---|---|
| TCP 独享端口/组共享 | `copy_limited(user→channel)` 每次 `write_all(n)` 成功后 +n | `copy_limited(channel→user)` 同上 |
| HTTP/HTTPS vhost | hyper body 向上游写 n 后 | 上游 body 写回用户 n 后 |
| TCPMux | 同 vhost(CONNECT 建立后的裸流双向) | 同左 |
| UDP 规则 | 用户 UdpSocket 读到数据报时 +payload 长度 | 通道 `UDP_PACKET` 写回用户时 +payload 长度 |
| SSH Gateway | 注册监听口 accept 后双向 copy | 同左 |
| visitor 中继 | 中继数据通道双向 copy,**归属目标代理的客户端**计数(不占访问端客户端) | 同左 |

**内存结构**:

```rust
// ProxyRuntime 增:
pub bytes_in: AtomicU64,  // Ordering::Relaxed
pub bytes_out: AtomicU64,
pub conns_current: AtomicI64,          // 供 F2 与列表展示(§9)
// 客户端桶:DashMap<client_id, ClientMeter { bytes_in, bytes_out: AtomicU64 }>
```

全局 `Totals.bytes_in/out` 改为**每次计数同步聚合**(与规则/客户端计数在同一 fetch_add 序列中,三次 Relaxed add;每 32KiB chunk 一次,开销可忽略)。规则计数与客户端桶计数保持一致口径:规则计数含该规则全部流量(含 visitor 中继),客户端桶 = 该客户端全部规则之和(服务端单点累加,天然守恒)。

### 2.3 持久化

新表(迁移 `0002`):

```sql
CREATE TABLE traffic (
  owner      TEXT PRIMARY KEY,   -- 'svc:{id}' | 'cli:{id}'
  bytes_in   INTEGER NOT NULL DEFAULT 0,
  bytes_out  INTEGER NOT NULL DEFAULT 0,
  updated_at INTEGER NOT NULL    -- unix ms
);
```

- **刷盘策略**:后台任务每 **30s** + 优雅关停时全量 flush;内存另存 `last_flushed[owner]`,upsert 增量 `mem - last_flushed`。崩溃丢失上界 = 30s 流量。
- **启动装载**:恢复期(§4.6 同一入口)把 traffic 表读入内存计数作为初值,重启后累计值连续;`updated_at` 仅诊断用。
- **不做**逐日分表与历史曲线(前端滚动窗口替代,见 §9;历史报表列 P2,§15)。

### 2.4 API 变更

| 接口 | 变更 |
|---|---|
| GET `/api/services` | 行增 `bytes_in`/`bytes_out`(累计)与 `conns_current` |
| GET `/api/clients` | 行增 `bytes_in`/`bytes_out`(累计) |
| GET `/api/stats/overview` | `bytes_in/out` 取真实 totals(修复恒 0) |

### 2.5 Prometheus

注册 `IntCounterVec rabbithole_bytes_total{direction="in|out", client, service}` 与 `IntGaugeVec rabbithole_service_conns{client, service}`;flush 任务同一周期内 `inc_by(delta)`,标签基数 = 规则数 ×2(有界;规则删除时指标保留至进程重启,可接受)。现有两个 gauge 不变。

### 2.6 验收

- iperf3 经 tcp 规则打 1GiB:列表/总览/Prometheus 三处计数与传输量偏差 ≤ 1 个 chunk(32KiB);
- `kill -9` 后重启,累计值 ≥ kill 前落盘值,丢失 ≤ 30s 窗口内流量;
- UDP 规则 `dig`/打流后双向计数均非零;vhost(http/https/tcpmux)各打一轮同验;
- visitor 中继流量计入**目标代理归属客户端**,访问端客户端计数不变。

---

## 3. F2 客户端级配额

### 3.1 字段(clients 表,迁移 `0002`)

| 列 | 类型 | 语义 | 0/空 = |
|---|---|---|---|
| `rate_limit` | TEXT | 出向(服务端→用户)限速,格式同规则级 `"1MB"/"512KB"` | 不限 |
| `max_conns` | INTEGER | 该客户端**服务端入口**并发用户连接上限 | 不限 |
| `flow_limit` | INTEGER | 累计流量上限(字节,基于 F1) | 不限 |
| `max_services` | INTEGER | 该客户端规则数上限;**覆盖**全局 `max_services_per_client` | 用全局值 |

API:`PUT /api/clients/{id}`、`POST /api/clients` 接受上述字段;新建客户端表单同步。校验:数值非法回 `VALIDATION`。

### 3.2 执行点

- **max_services**:并入创建/启用资格校验(§4.4 步骤 1),阈值取 `per_client > 0 ? per_client : 全局`;超限回 `LIMIT_EXCEEDED`(现有错误码)。
- **max_conns**:客户端桶增 `conns_current: AtomicI64`;在 tcp accept、vhost/http/https/tcpmux 连接进入、UDP per-source 会话建立、SSH Gateway 转发连接建立处 `fetch_add(1)`,关闭处 `-1`。达到上限 → 新连接**立即关闭**(不排队),事件 `CLIENT_CONN_LIMIT`(同客户端去抖 60s 一条)。visitor 不计入(入口在访问端)。
- **rate_limit**:每客户端一个 `TokenBucket`(复用 data.rs 现有实现,容量按配置解析);挂载在**用户写方向**(server→user)的 `copy_limited`,与规则级 bucket 串联(两者都需 acquire)。修改配置热生效 = 重建桶,仅对**新连接**生效(在途连接沿用旧桶,避免中途变速语义歧义)。
- **flow_limit**:新用户连接进入时检查客户端累计字节(F1 内存值)≥ `flow_limit` → 立即关闭 + 事件 `CLIENT_FLOW_LIMIT`(边沿触发一次;重新 enable 客户端或调大上限后自动恢复)。存量连接不中断,自然结束。

### 3.3 可见性

客户端列表行展示:`流量 used/limit`(无上限只显 used)、`连接 n/limit`;服务列表行展示规则级(连接数、限速徽标可选)。

### 3.4 验收

- `max_conns=2` 下第 3 条并发连接立即被关,事件落库;
- `rate_limit=1MB` 下 iperf3 出向稳定在 ~1MB/s ±10%,入向不受限;
- `flow_limit=10MB` 打满后新连接拒绝、事件一条;调大上限后新连接恢复;
- `max_services=1` 创建第 2 条规则回 `LIMIT_EXCEEDED`,DB 无残留。

---

## 4. F3 泛解析(通配域名)

### 4.1 语法与校验

- 仅支持**最左标签单个通配**:`*.example.com` 合法;`*`、`a.*.com`、`*example.com`、`www.*` 一律 `VALIDATION` 拒绝。
- 大小写归一(现有 `to_lowercase()` 沿用)。
- 冲突判定:同一 vhost 域名空间内(与现有 exact 冲突同域:按 kind 分别判定),**相同通配串只允许一个规则声明**;exact 与通配可共存(`a.example.com` 与 `*.example.com` 同时合法,匹配时 exact 优先)。

### 4.2 匹配语义

三处入口(http host、https SNI、tcpmux host)统一走一个 matcher:

1. 精确匹配命中 → 返回;
2. 否则遍历通配项,`host == 通配串去 "*." 后的后缀` 且 host 标签数 = 通配标签数(即恰好一层子域:`x.example.com` 命中 `*.example.com`,`a.b.example.com` 不命中)→ 候选;
3. 多候选(理论上仅当声明了不同层级的重叠通配,如 `*.example.com` 与 `*.sub.example.com`)取**标签数最多**者;
4. 无命中 → 现有行为(http 404/全局页、https/tcpmux 直接断开)。

### 4.3 实现

`VhostRoute`(state.rs:281)路由容器由 `Vec<VhostRoute>` 改为:

```rust
struct VhostTable { exact: HashMap<String, Arc<VhostRoute>>,
                    wildcards: Vec<(String /*后缀,不含 *.*/, Arc<VhostRoute>)> }
```

- 三组路由表(vhost_http/vhost_https/vhost_tcpmux)同构替换;发布路径(串行域内原子换表)不变。
- `subdomain` 特性不受影响(仍走 exact,由发布时拼接基础域名生成)。

### 4.4 验收

- `*.example.com` 下 `a.example.com` 命中、`a.b.example.com` 不命中(404);
- exact `a.example.com` 与通配并存时请求走 exact;
- https 经 `openssl s_client -servername` 验证 SNI 通配路由;tcpmux 以 CONNECT host 验证;
- 创建重复通配回 `PORT_CONFLICT` 域错误(复用冲突判定通道,消息区分域名冲突)。

---

## 5. F4 按域名 404 页面

- `HttpProxyConfig` 增 `not_found_html: String`(`#[serde(default, skip_serializing_if = "String::is_empty")]`,上限 **8 KiB**,超出回 `VALIDATION`)。
- **触发条件**:host 命中该规则、但请求 path 未命中其任何 `location`(或规则无 locations 且 path 校验失败的场景不存在——无 locations 即全收,则此字段不生效)→ 返回 `404` + 该 HTML 作为 body(`Content-Type: text/html; charset=utf-8`)。
- host 未命中任何规则 → 维持现状(全局 `vhost.custom_404_page` 或内置 404)。
- **已知取舍**:该字段随 `ProxySpec` JSON 进入 SERVICE_SYNC 载荷(客户端忽略非本 kind 配置),8 KiB 上限下单客户端数十条 http 规则仍在 1 MiB 帧限内;备选方案(独立 `http_extras` 表、不进 SYNC)复杂度不值,记录于此备查。
- 前端:http 规则表单"高级"区增多行文本框。
- 验收:配 `not_found_html` 的规则,请求未命中 location 返回自定义内容与 404 状态;未配置的规则行为不变。

---

## 6. F5 HTTP 静态资源缓存

### 6.1 配置(HttpProxyConfig 增,随规则存储)

```rust
pub cache: Option<HostCacheConfig>,   // None = 不缓存(默认)
pub struct HostCacheConfig {
    pub ttl_seconds: u64,             // 10..86400, 默认 300
    pub max_size_mb: u32,             // 1..256, 默认 16
    pub extensions: Vec<String>,      // 默认 ["js","css","png","jpg","jpeg","gif","svg","webp","ico","woff","woff2","ttf","html"]
    pub max_item_size_kb: u32,        // 默认 1024;响应体超限不缓存,直接透传
}
```

### 6.2 缓存行为

- **键**:`(host, path)`——不含 query(带 query 的请求可命中同 path 条目);仅 `GET`、仅扩展名在列表内、仅上游状态 = 200、仅 `Content-Length` 已知且 ≤ `max_item_size` 的响应可缓存。
- **跳过条件**:响应头含 `Cache-Control: no-store / private / no-cache` 或 `Set-Cookie` → 透传不缓存。
- **命中**:直接返回缓存副本(原状态/头/体),命中计数 +1;**不做** ETag/If-None-Match 协商(§15 取舍)。
- **实现**:每 route 一块自研简易 LRU(`Mutex<HashMap<Key, Entry> + 双向链表>`,Entry 含创建时刻);TTL 惰性过期 + 容量 LRU 逐出。不引入新依赖。
- **失效**:规则配置变更在串行域内发布新路由表时**整块丢弃**重建;listener 存活期间无其他主动失效。
- **全局上限**:启动时 Σ `max_size_mb` > 512 → warn 事件;单条目缓冲区在响应体流式转发路径外单独分配,超限响应体不落缓存、不缓冲(`Content-Length` 已知才考虑缓存,天然规避无限流)。

### 6.3 验收

- 同 path 二次请求上游不再收到请求(上游计数/日志验证);TTL 过期后回源;
- `no-store` 响应永不缓存;`?query` 请求命中同 path 条目;
- 命中/未命中计数进 events(每 route 首次命中落一条 info)与 Prometheus(`rabbithole_vhost_cache_hits_total{service}`);
- 修改规则缓存配置后旧缓存立即失效。

---

## 7. F6 HTTPS 多证书

### 7.1 存储(迁移 `0002`)

```sql
CREATE TABLE certs (
  id         INTEGER PRIMARY KEY AUTOINCREMENT,
  name       TEXT NOT NULL UNIQUE,
  domains    TEXT NOT NULL,      -- JSON 数组,可含通配(§4 语法)
  cert_pem   TEXT NOT NULL,
  key_pem    TEXT NOT NULL,      -- 私钥;信任边界与 clients.secret 相同(§3.4),文件 0600
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL
);
```

### 7.2 SNI 证书解析器

- 以 rustls `ResolvesServerCert` 实现证书选择:精确表 + 通配表,**复用 §4.2 matcher 语义**(SNI ↔ domains 匹配,通配仅一层子域,多候选取标签数最多)。
- 解析顺序:SNI 命中 DB 证书 → 未命中 → 配置文件默认证书(`vhost.tls`/listener 现有 `TlsServerFileConfig`)→ 仍无则按现状关闭连接。
- **热更新**:证书 CRUD 在写锁内重建 resolver 并原子换入;在途 TLS 连接不受影响,仅影响后续握手。启动时 DB + 配置文件合并构建。

### 7.3 API 与前端

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/api/certs` | 列表(`key_pem` 不返回;含 `domains`、到期时间、指纹摘要) |
| POST | `/api/certs` | `{name, cert_pem, key_pem, domains?}`;PEM 解析失败回 `VALIDATION`;`domains` 缺省从 SAN/CN 自动提取 |
| PUT | `/api/certs/{id}` | 同上(全量替换) |
| DELETE | `/api/certs/{id}` | 删除;引用它的 SNI 回落默认证书,事件告警 `CERT_REMOVED` |

前端"证书"页(设置同级菜单):列表 + 上传表单(粘贴 PEM 或文件选择);显示域名、到期时间(≤30 天橙色徽标)。**不与规则强绑定**:证书按 SNI 自动匹配,规则侧无需选证书(https 规则仍只配域名)。

### 7.4 验收

- 上传两张证书(a.example.com、`*.example.com`),`openssl s_client -servername` 验证各自返回正确链;
- 删除证书后同 SNI 回落默认证书并落事件;
- 私钥不出现在任何 GET 响应;到期提醒徽标正确;
- 上传非法 PEM 回 `VALIDATION` 且库无残留。

---

## 8. F7 控制台重组:侧边栏按类型拆页 + 表单补齐(后端字段均已存在)

### 8.0 侧边栏信息架构(采纳 nps 拆法)

现状 8 种 kind 混在单一「转发规则」页(kind 下拉 + 类型列)。改为**每类型独立菜单**,页面组件参数化复用——与 nps 同构:nps 的 7 个类型菜单(TCP/UDP/HTTP 代理/SOCKS/私密/P2P/文件)底层共用按类型参数化的 list/add/edit 模板,仅域名解析因字段差异大单独一套。

| 菜单 | 路由 | 覆盖 kind | 页内 kind 选择 |
|---|---|---|---|
| 总览 | `/rabbithole/overview` | — | —(现有) |
| 客户端 | `/rabbithole/clients` | — | —(现有) |
| 域名服务 | `/rabbithole/domains` | http / https / tcpmux | 有(默认 http) |
| TCP 隧道 | `/rabbithole/tcp` | tcp | 无 |
| UDP 隧道 | `/rabbithole/udp` | udp | 无 |
| 密钥访问 | `/rabbithole/secret` | stcp / sudp / xtcp | 有(默认 stcp) |
| 证书 | `/rabbithole/certs` | — | —(F6,M3 新增) |
| 设置 | `/rabbithole/settings` | — | —(现有) |

- **组件复用**:单一 `ProxyListPage` 组件(props:`kinds`、默认 kind、表单 schema 驱动),四个业务页均为其实例;现有 services/index.vue 的表格/弹窗逻辑迁入其中,visitors/index.vue 整体作为密钥访问页的第二个 tab(「访问器」,`?tab=visitors` 持久化)。
- **旧路由兼容**:`/rabbithole/services` → 重定向 `/rabbithole/tcp`;`/rabbithole/visitors` → 重定向 `/rabbithole/secret?tab=visitors`。
- **列表隔离**:每页仅展示本 kind 集合(前端过滤 `listServices` 结果;后端列表接口**可选**增 `kind` 查询参数以减少载荷,M4 顺手项,非依赖)。
- **插件不单列菜单**(与 nps 的有意差异):nps 把 HTTP 代理/SOCKS/文件作为一等模式各占一菜单,源于其模式即类型的数据模型;rabbithole 中插件是**规则的属性**、横跨多个 kind,单列菜单会造成同一规则双入口的归属歧义。插件选择器在各类型页表单的"目标"区提供(§8.2);隧道页的 plugin 过滤列为可选项,不在验收内。
- 实现锚点:`web/apps/web-antd/src/router/routes/modules/rabbithole.ts`(菜单 5 → 8 项)与 `views/rabbithole/` 页面重组。

### 8.1 按 kind 字段矩阵(新建/编辑弹窗;"高级"区默认折叠;页内多 kind 时含 kind 选择器)

| 字段 | tcp | udp | http | https | stcp/sudp/xtcp | tcpmux | 备注 |
|---|---|---|---|---|---|---|---|
| bind_ip / bind_port(0=自动) | ✅(现有) | ✅ | — | — | — | — | |
| target_host/port | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | 插件启用时隐藏(见下) |
| pool_size | ✅(0-16,0=默认) | — | — | — | — | — | |
| bandwidth_limit + mode | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | 空串=不限;mode 仅 client/server |
| proxy_proto(none/v1/v2) | ✅ | — | ✅(vhost) | — | — | — | |
| group / group_key | ✅ | ✅ | ✅ | ✅ | — | — | 组内同 (kind,bind) 共享监听 |
| health_check | ✅ | ✅ | ✅ | ✅ | — | — | 子表单:type tcp/http、interval、timeout、max_failed、path(http) |
| custom_domains / subdomain | — | — | ✅(现有+通配输入) | ✅ | — | ✅(仅 domains) | 通配语法前端同校验 |
| locations | — | — | ✅ | — | — | — | 多值输入 |
| host_rewrite / http_user / http_password | — | — | ✅ | — | — | — | |
| request/response_headers(增/改/删列表) | — | — | ✅ | — | — | — | |
| route_by_http_user | — | — | ✅ | — | — | — | |
| not_found_html | — | — | ✅(§5) | — | — | — | |
| cache(§6) | — | — | ✅ | — | — | — | 子表单四项 |
| secret_key | — | — | — | — | ✅(key 现有) | — | allow_users 已移除(授权由访问器记录承担) |
| plugin(9 种) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | 见 §8.2;启用后 target 区按插件替换 |

### 8.2 插件子表单

按 `PluginKind` 动态渲染字段(全部为后端 `PluginSpec` 既有参数):

| 插件 | 字段 |
|---|---|
| unix_domain_socket | unix_path |
| http_proxy | http_user, http_password |
| socks5 | user, password |
| static_file | local_path, strip_prefix(如有)及既有参数 |
| https2http / https2https | crt_path, key_path, local_addr, host_rewrite |
| http2https / http2http | local_addr, host_rewrite, request_headers |
| tls2raw | local_addr, crt_path, key_path |

启用插件时隐藏 target_host/port(后端语义:插件接管目标),列表"目标"列显示 plugin 标签(现有行为)。

### 8.3 列表与入口

- 各类型页列表通用列:名称、状态徽标、目标/插件、流量(入/出,hover 显字节)、当前连接、group、启用、操作;TCP/UDP 隧道页含监听列,域名服务页以「域名」列替换监听列(首个域名 + `+n` 计数);
- 设置页增:Prometheus `/metrics` 地址展示(只读,来自当前部署约定);
- 各类型页工具区"批量导入"入口(调 `/api/services/batch`,粘贴 JSON 数组,失败逐条回报);
- 总览页统计口径不变(全局聚合,不受拆页影响)。

### 8.4 验收

- 侧边栏 8 项与 §8.0 表一致;旧 `/rabbithole/services`、`/rabbithole/visitors` 重定向可达且功能不丢;
- 每类型页列表仅含本 kind 集合;单 kind 页(tcp/udp)表单无 kind 选择器,多 kind 页(域名服务/密钥访问)有且默认值正确;
- 密钥访问页「规则/访问器」tab 切换后刷新保持(query 参数);
- 矩阵中每一字段:表单可建 → 列表/API 回读一致 → 实际生效(抽样:locations 分流、basic auth 401、host 改写抓包、group 两成员轮询、健康检查摘除/恢复、限速生效);
- 插件 9 种各一条 e2e(socks5/unix/静态文件按 acceptance 既有用例口径);
- 高级区折叠默认态不改变现有创建最短路径(≤5 项必填即可提交)。

---

## 9. F8 接入引导、图表、分规则连接

- **配置片段弹窗**:新建客户端与轮换密钥后的 secret 弹窗内,增"rh-client.toml 模板"折叠区——`server_addr`(可编辑,默认 `location.hostname + 控制端口默认 7000`,并提示以实际部署为准)、`transport`(下拉 noise/tls,默认 noise)、`client_id`、`secret`(已填);一键复制。纯前端,无新 API。
- **总览图表**:复用现有 5s 轮询,前端维护 60 点滚动窗口(5 分钟)对 `bytes_in/out` 差分得速率,双线折线(recharts,F9 §10.2);断网期间缺口断线显示,不外推。
- **分规则连接数**:`conns_current` 来自 F1(§2.2),服务列表展示;visitor 中继连接计入目标代理。

验收:弹窗模板粘贴到 rh-client 可直接启动认证;图表速率与 `iperf3 -b` 设定档位偏差 ±10%;规则连接数随 `nc` 并发开合实时增减。

---

## 10. F9 前端基座重写(React + Ant Design v6)

> **已实现并被后续重写取代**:F9 的成果(独立 `console/` 工程、后端零变更、
> 契约平移)已全部落地;其中的 antd 技术选型于 2026-10-12 由
> Vite + Tailwind v4 + shadcn/ui 取代。本节保留为决策与迁移路径记录,
> 技术栈细节以 `console/README.md` 为准。

### 10.1 决策与范围

- 以独立 React 工程**整体重写**管理面前端,替换 vue-vben-admin 裁剪版(现 `web/` monorepo)。修订 design.md §8:§8.1 基座锁定与裁剪边界条款**废止**;§8.2 对接契约与 §8.3 页面规划的**语义保留**,由新工程承接。
- 动因:vben 为通用模板(commit 锁定、turbo/pnpm workspace、大量未使用的特性),单管理员控制台的维护成本与体量不匹配;React 生态与后续维护方向对齐。
- 范围:仅管理面前端。**后端 API、RHCP 协议、数据面零变更**;§11 迁移与 §12 API 变更不受影响。

### 10.2 技术栈

| 层 | 选型 | 说明 |
|---|---|---|
| 框架 | React 19 + TypeScript | — |
| UI | Ant Design v6(React 版) | 与现 ant-design-vue 组件 1:1 对应(Table/Form/Modal/Popconfirm/Switch/Tag/Statistic/InputPassword) |
| 构建 | Vite + @vitejs/plugin-react | 产物 `dist/`,base `/` |
| 路由 | react-router v7(library mode) | 路由表 = §8.0 八菜单;AuthGuard 包裹业务路由 |
| 数据 | TanStack Query v5 | `refetchInterval` 承接 5s 轮询;CRUD 成功后 invalidate 精确刷新 |
| 本地状态 | zustand(localStorage 持久化) | 仅 token + userInfo;不引 redux |
| HTTP | axios 封装 | envelope 解包(`code===0` 取 `data`)、Bearer 注入、HTTP 401 → 清 token 跳登录(保留 return path)、业务错误统一 toast |
| 图表 | recharts | F8 总览双线滚动窗口 |
| 不引入 | i18n 框架、主题/多标签系统、微前端 | 中文直出;vben 特性不追求对等(§15) |

### 10.3 工程结构

```
console/                      # 顶层独立 npm 工程(Node ^22 || ^24, pnpm)
├── vite.config.ts / tsconfig.json / package.json
└── src/
    ├── api/                  # request.ts(axios 封装)+ rabbithole.ts
    ├── stores/auth.ts        # zustand:token/userInfo + login/logout
    ├── router/               # 路由表 + AuthGuard + 旧路由重定向
    │                         #   (services→tcp、visitors→secret?tab=visitors)
    ├── layouts/Console.tsx   # 侧边栏(§8.0 八项)+ 页头(logout)
    ├── components/           # ProxyListPage(参数化)、SecretModal、QuotaBar、
    │                         #   TrafficText、ConfigSnippetModal、HeaderPairsInput
    └── pages/                # overview/ clients/ domains/ tcp/ udp/ secret/
                              #   certs/ settings/ auth/(登录)+ 403/404
```

- 现 `web/apps/web-antd/src/api/rabbithole.ts` 的接口定义与 TypeScript 类型**原样平移**(契约不变,缩小联调面)。
- 迁移期 `web/`(vben)与 `console/` 并存;旧 UI **冻结**(仅修阻断性 bug);CUT 后整目录删除 `web/`(含 pnpm workspace 与 turbo 配置)。

### 10.4 部署与托管契约(不变,仅换产物路径)

- 构建产物 `console/dist`;`[web].static_dir` 显式配置优先的语义不变。
- `build.rs` 内嵌检测改为**先 `console/dist`、后旧 `web/apps/web-antd/dist`**(双路径择一,CUT 后移除旧路径);`rust-embed`、SPA 回退 200、未知 `/api/*` JSON 404 语义全部沿用。
- API base 构建时注入:`VITE_API_BASE=/api`(对应现 `VITE_GLOB_API_URL`)。
- 登录链路契约逐项保留(design.md §8.2):login → `accessToken` → 并行 `/user/info` + `/auth/codes` → 应用;logout **显式附 Bearer**;HTTP 401 → 登录页;改密成功后旧 token 全失效 → 强制重登。

### 10.5 与 F6/F7/F8 的关系

- §8.0 八菜单侧边栏**直接在 React 实现**——不再经历"vben 单页改造再拆页",F7 的前端交付整体并入 R2;
- F8 图表/接入引导弹窗、F6 证书页前端并入 R3;
- 现 services/index.vue 等仅作字段与行为参照,不迁移代码。

### 10.6 R 系列里程碑与切换

| 阶段 | 内容 | 验收 |
|---|---|---|
| R0 脚手架与认证 | 工程初始化、axios 封装、login/info/codes/logout 真实联调、侧边栏骨架(8 菜单占位页)、build/embed 接线 | `pnpm build` 产物由 rh-server 单进程托管;登录→登出全链路;深链/刷新 SPA 回退正常 |
| R1 对等迁移 | 客户端页(含 F2 四配额字段与用量展示)、设置页(改密+强制重登)、总览(卡片+事件+5s 轮询) | 与 vben 版功能对等;F2 配额用例在后端就绪后补跑 |
| R2 类型页(吸收 F7) | ProxyListPage 参数化四页 + §8.1 字段矩阵 + 插件子表单 + 密钥访问页双 tab(规则/访问器)+ 批量导入 + 旧路由重定向 | §8.4 验收全量 |
| R3 增值页 | 证书页(F6 前端)、总览 recharts 图表(F8)、客户端配置片段弹窗(F8)、Prometheus 入口展示 | §7.4 / §9 对应前端验收项 |
| CUT 切换 | build.rs 默认新路径、删除 `web/`、README/deploy 文档更新 | tests/e2e*.sh 浏览器用例在新 UI 全量重跑通过;二进制体积对比记录 |

### 10.7 取舍

- 双栈并存维护成本:R0–R3 期间旧 UI 冻结,成本有界;
- 放弃 vben 特性(多标签页、主题偏好、i18n 切换):单管理员控制台不需要,列 §15 可选项;
- bundle 目标 ≤ 500KB gz(vite tree-shake + antd v6 按需引入,预期 ~300KB);
- token 明文 localStorage:与"JWT 12h 无撤销"的既有取舍同信任边界;XSS 面由 React 默认转义与 antd DOM 渲染覆盖。

## 11. 数据库迁移(0002)

```sql
ALTER TABLE clients ADD COLUMN rate_limit   TEXT    NOT NULL DEFAULT '';
ALTER TABLE clients ADD COLUMN max_conns    INTEGER NOT NULL DEFAULT 0;
ALTER TABLE clients ADD COLUMN flow_limit   INTEGER NOT NULL DEFAULT 0;
ALTER TABLE clients ADD COLUMN max_services INTEGER NOT NULL DEFAULT 0;

CREATE TABLE traffic ( ... );   -- §2.3
CREATE TABLE certs   ( ... );   -- §7.1
```

`ProxySpec`/`HttpProxyConfig`/`HostCacheConfig` 增字段随 JSON blob 演进,**零迁移**;旧数据反序列化走 `#[serde(default)]`。

## 12. API 变更汇总

| 接口 | 变更类型 |
|---|---|
| GET `/api/services` / `/api/clients` | 响应增计量字段 |
| GET `/api/stats/overview` | 修复 bytes 恒 0 |
| POST/PUT `/api/clients` | 请求增 4 个配额字段 |
| POST/PUT `/api/services` | 无 schema 变更(F7 仅前端暴露既有字段;§5/§6 新 spec 字段按 default 兼容) |
| GET/POST/PUT/DELETE `/api/certs` | 新增 |
| `/metrics` | 增 bytes/conns/cache 指标 |

## 13. 安全与资源边界

| 项 | 约束 |
|---|---|
| 证书私钥落库 | 明文,信任边界 = 单机文件系统(DB 0600),与 clients.secret 同口径;P1 可选信封加密 |
| 计数开销 | 每 32KiB chunk ≤ 3 次 Relaxed atomic add + flush 任务 30s 一次 upsert(脏 owner 数有界) |
| 缓存内存 | 全局 Σ ≤ 512MiB(超限启动告警);单条目 ≤ max_item_size 且需已知 Content-Length |
| Prometheus 标签基数 | 规则数 × 方向(有界);删除规则的指标残留至重启 |
| 配额绕过面 | max_conns 覆盖全部服务端用户入口(tcp/vhost×3/udp 会话/ssh);管理 API 不受限额 |
| not_found_html | ≤8 KiB,防 SYNC 载荷膨胀;XSS 不设服务端过滤(控制台管理员自配内容,信任边界内) |

## 14. 里程碑与验收汇总

| 里程碑 | 内容 | 关键验收(全量见各节) |
|---|---|---|
| M1 计量与配额 | F1 + F2 + F8 引导 | 三处计数一致(±32KiB);kill -9 丢失 ≤30s;四类配额各有量化用例;引导模板可启动 |
| M2 vhost 增强 | F3 + F4 + F5 | 通配匹配/优先级用例;404 自定义;缓存命中/失效/TTL 用例 |
| M3 证书管理 | F6 | SNI 多证书正确、热更新、私钥不出 GET |
| M4 → R2/R3 | 原 M4 内容(F7 拆页+字段矩阵、F8 图表)由 F9 React 新基座交付,详表见 §10.6 | §8.4 于 R2 验收;图表 ±10% 于 R3;e2e 全量回归于 CUT |
| CUT 前端切换 | build.rs 默认 console/dist、删除 web/、文档更新 | tests/e2e*.sh 浏览器用例在新 UI 全量重跑通过 |

每里程碑完成后在 docs/requirements-matrix.md 增行、docs/acceptance.md 落验收记录(沿用现有流程)。

## 15. 已知取舍与非目标

**取舍**:
- 流量崩溃窗口 ≤30s(定时 flush),不做逐连接落库;
- 不做逐日历史分表与报表(前端 5 分钟滚动窗口已覆盖"实时带宽"对标点;历史曲线 P2);
- flow_limit 为累计口径、不做按月重置(nps 亦为累计);
- per-client 限速仅约束服务端→用户方向,与规则级 BandwidthMode::Server 语义对齐;在途连接不受配置变更影响;
- 缓存不做 ETag/If-None-Match 协商与 query 区分(nps 亦无协商);
- not_found_html 随 SYNC 下发的载荷代价(≤8KiB/规则);
- 证书按 SNI 自动匹配而非规则显式绑定,减少一处人工映射。

**非目标(本档不做)**:多用户/角色/注册;OIDC;按月配额重置与流量清零操作;SSE 实时推送(design.md §12 P1 另行评估);HTTP 缓存的 range/conditional request 语义;certs 的 ACPE/Let's Encrypt 自动签发;控制台多语言(中文直出)与 vben 特性对等(多标签页/主题偏好系统)。
