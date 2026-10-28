# 控制台 API Key 认证任务书(2026-10)

> 目标:让控制台每一组 REST API 都能用 **API Key** 访问,同时保持浏览器 JWT 会话不变;
> 并把当前「33 处 handler 各自手写鉴权」重构为「路由层统一中间件」,使新增接口**默认安全**。
>
> 本文件是执行依据,自包含。行号为 **2026-10-27 快照**,动手前先读当前代码核实;
> 与现状不符时按 AGENTS.md 记录反证,不要硬改。
>
> 状态标记:`[ ]` 待办 / `[x]` 完成(附提交 hash)/ `[W]` 放弃(写明反证)。

---

## 0. 执行约定(每个任务包都必须遵守)

1. **提交前三道门全绿**(AGENTS.md):
   ```bash
   cargo fmt --all -- --check
   cargo clippy --workspace --all-targets -- -D warnings
   cargo test --workspace
   ```
2. **Windows 编译自查**。本任务不新增 `std::os::*` 用法,但收尾仍要确认:
   ```bash
   grep -rn "std::os::fd\|std::os::unix" --include=*.rs crates/ vendor/ | grep -v ':\s*//'
   ```
   **每个命中都必须有平台门控**,满足任一即可:
   - 行内有 `#[cfg(unix)]` / `#[cfg(target_os = "...")]`
   - 或**所在模块被门控**(如 `splice.rs` 的 import 无门控,但其
     `pub mod splice;` 在 `lib.rs` 里带 `#[cfg(target_os = "linux")]`)

   `#[cfg(test)]` 里的代码**同样参与编译**,不能只靠 `cfg(test)` 豁免。
   必须过滤注释行 —— 实测 13 处命中里 6 处是注释。
3. **一个任务包一个提交**;提交信息写「为什么」不写「做了什么」;不与格式化改动混提交;
   只 `git add` 自己改过的文件(`git status` 先看一遍)。
4. **行为不变是硬约束**。除本文件明确允许的变更外,不得改变:
   - 对外 API 的响应 JSON 结构(信封 `{code, data, message}` 不变)
   - 现有 HTTP 状态码语义(404 仍是 404,不能变 401)
   - 配置语义、控制面协议、数据面行为
5. **不引入新依赖**。`axum` 的 `middleware` 模块在默认 features 内(已核实
   `axum-0.8.9/src/lib.rs:497`),`sha2`、`hex` 均已在
   `crates/rh-server/Cargo.toml` 中。

   **`rand` 有两个可用版本,但只能用 `rand`(0.9)**:

   | 依赖名 | 版本 | 是否可用 |
   |---|---|---|
   | `rand` | 0.9.5 | ✅ **用这个** → `use rand::RngCore;` |
   | `rand_010` | 0.10.3 | ❌ 根不导出 `RngCore`,须走 `prelude` |

   见 B-1 的实测表。**不要为了常量时间比较去加 `ring`** ——
   手写 XOR 折叠即可(见 B-2)。
6. 前端改动需过 `cd console && pnpm build`。
7. **`cargo build` 不编译 `#[cfg(test)]` 代码**。包 E 会删 handler 参数,
   影响 **40 处测试调用点 / 14 个函数**(数字见 §1.0,明细见 E-4b),只跑 `build`
   会漏掉全部测试编译错误。涉及 E 的提交**必须**跑 `cargo test --workspace`。
8. **文档里的 Rust 代码块分三种,别混着拼**:
   | 类型 | 特征 | 怎么用 |
   |---|---|---|
   | **接口概览** | 只有签名、无函数体(以 `;` 结尾) | 仅看契约,**不要编译** |
   | **完整实现** | 有函数体 `{ .. }` | 直接照抄/替换概览里的同名声明 |
   | **示意片段** | 有 `..` / `/* .. */` 占位 | 理解思路用,**不要编译** |
   
   拼接"概览"与"实现"会得到 `E0428: name ... is defined multiple times`
   (第 35 轮实测)。文中三处已显式标注(§B-1 的概览、§B-1 的 `generate` 实现、
   §C-4 的 `apikey_touch` 实现)。

9. **中间件里绝不能持有 `&Request` / `&HeaderMap` 跨 `.await`**(见 §E-1)。
   `Request<Body>` 不是 `Sync`,会让 future 不 `Send`,而 axum 报出的表面错误
   是 `E0277 ... FromFn ... Service<Request> is not satisfied` —— **看起来像
   State 类型问题,实际是借用问题**。正确做法:先把 header 值 `.to_owned()`,
   再传给 async 函数。

---

## 1. 现状事实(动手前逐条核实)

### 1.0 单一事实源(SSOT)—— **改数字只改这里**

> 本文档多处引用同一组数字。**为避免"改一处漏三处"的漂移**
> (已实际发生两次:第 6 轮改 C-1 漏了 §1.2;第 9 轮改 E-4b 漏了另外 4 处),
> 所有重复出现的数字以本节为准,正文只写「见 §1.0」。

| 事实 | 权威值 | 校验方式 |
|---|---|---|
| 现有路由数 | **24**(按 path 去重) | 见下方「校验命令集」第 1 条 |
| 现有鉴权调用点 | **33**(30 `is_err` + 3 `match`) | `grep -c "require_auth(&state, &headers)" crates/rh-server/src/api.rs` |
| 删 `headers` 需同步改的测试调用点 | **40 处 / 14 个函数** | 明细见 §E-4b |
| 测试对 handler 的调用点总数 | **71**(其中 26 处不涉及 `headers`) | 见 §E-4b |
| `pwd_version` 缓存初始化位置 | **`state.rs:454-455`** | 见「校验命令集」第 3 条 |
| 现有迁移文件数 | **4**(0001–0004) | `ls migrations/*.sql \| wc -l` |
| `READONLY` | **5461** = `0x1555` | 见 §2.3 |
| `OPS` | **5503** = `0x157F` | 见 §2.3 |
| `FULL` | **16383** = `0x3FFF` | 见 §2.3 |
| 新增 API Key 明文长度 | **77**(`rhk_` + 8 + `_` + 64) | 见 §B-1 |
| 新增测试总数 | **25**(Rust 21 + 前端 4);分布见 §H-1 顶部的索引表 | 见 §H-1 |
| D-4 的失败分支数 | **5**(第 6/7 步不是分支) | 见 §D-4 验收表 |

> 🔴 **修改任何一项时,先跑对应校验命令重新测一遍,再动笔。**
> 不要凭记忆或上一次的值。

**校验命令集**(逐条实测可用,2026-10-28)

```bash
# 1) 路由数 = 24(必须先按 path 去重;直接 grep -c '\.route(' 只得 18,
#    因为多方法是链式的,一对 .route() 里可能有 get+post 两个方法)
python3 -c "
import re
s=open('crates/rh-server/src/api.rs').read()
b=s[s.index('pub fn router'):s.index('.with_state(state)')]
print(len(set(re.findall(r'\"(/api/[^\"]*)\"', b))))
"

# 2) 鉴权调用点 = 33
grep -c 'require_auth(&state, &headers)' crates/rh-server/src/api.rs

# 3) 缓存初始化行号 = 454-455
#    ★ 必须用 `new()` 锚定,否则会同时命中第 70 行的**字段声明**
grep -n 'pwd_version_cache: dashmap::DashMap::new()' crates/rh-server/src/state.rs

# 4) 迁移文件数 = 4
ls migrations/*.sql | wc -l

# 5) 明文长度 = 77(4 + 8 + 1 + 64)
python3 -c "print(4+8+1+64)"

# 6) 测试总数(实现完成后数 .rs 里的 #[test];现在只能数文档承诺的数量)
#    文档承诺:B-3 的 4 个 + H-1 的 17 个 = 21 Rust,加 G-3 的 4 个前端 = 25
#    注 apikey.rs 未创建时用 2>/dev/null,避免整条命令集中断
grep -c '#\[test\]' crates/rh-server/src/api.rs
grep -c '#\[test\]' crates/rh-server/src/apikey.rs 2>/dev/null || echo 0

# 7) D-4 的失败分支 = 5(数 §D-4 验收表里"期望 401"的行)
sed -n '/^#### D-4/,/^### 包 E/p' docs/api-key-auth-tasklist-2026-10.md | grep -c '| 401 |'
```

### 1.1 认证链路

| 事实 | 位置 |
|---|---|
| 路由分 5 组注册(`auth`/`clients`/`services`/`visitors`/`misc`),共 24 个路径 | `crates/rh-server/src/api.rs:16-69` |
| 鉴权调用点 **33 处**:30 处 `.is_err()` + 3 处 `match` | 见下方锚点表(**不要用行号,代码在持续变动**) |
| `match` 形态只有 3 处,且都绑定用户名 | `change_password` / `logout` / `user_info` 三个 handler |

**33 处锚点表(用 handler 名定位,不用行号)**

> ⚠ **本仓库代码在持续变动,行号会漂移**。
> 2026-10-28 复核时表内 33 个行号**全部准确**(0 处漂移),
> 但历史上曾一次漂移 14 处(他人新增结构体导致后移)——
> 所以**定位仍以 handler 名为准**,行号只作快照参考。
> 定位方式:`grep -n "require_auth(&state, &headers)" crates/rh-server/src/api.rs`,
> 或按 handler 名搜 `async fn <名字>(`。下表行号只是 2026-10-28 的快照参考。

| handler | 形态 | 2026-10-28 行 |
|---|---|---|
| `menu_all` | is_err | 72 |
| `change_password` | **match** | 221 |
| `logout` | **match** | 411 |
| `user_info` | **match** | 432 |
| `auth_codes` | is_err | 448 |
| `list_clients` | is_err | 485 |
| `create_client` | is_err | 578 |
| `update_client` | is_err | 654 |
| `rotate_secret` | is_err | 841 |
| `delete_client` | is_err | 871 |
| `list_services` | is_err | 1094 |
| `get_service` | is_err | 1132 |
| `create_service` | is_err | 1163 |
| `update_service` | is_err | 1349 |
| `enable_service` | is_err | 1538 |
| `rotate_service_secret` | is_err | 1591 |
| `disable_service` | is_err | 1745 |
| `delete_service` | is_err | 1792 |
| `batch_services` | is_err | 1847 |
| `list_visitors` | is_err | 2096 |
| `create_visitor` | is_err | 2179 |
| `update_visitor` | is_err | 2229 |
| `enable_visitor` | is_err | 2274 |
| `disable_visitor` | is_err | 2285 |
| `delete_visitor` | is_err | 2339 |
| `stats_overview` | is_err | 2387 |
| `list_events` | is_err | 2437 |
| `get_server_config` | is_err | 2476 |
| `put_server_config` | is_err | 2592 |
| `list_certs_api` | is_err | 2991 |
| `create_cert_api` | is_err | 3043 |
| `update_cert_api` | is_err | 3086 |
| `delete_cert_api` | is_err | 3134 |
| JWT 校验:HS256,claims `{sub, iat, exp, pv}`,用 `pv` 对比 `users.pwd_version` 实现吊销 | `api.rs:362-408` |
| 解码引擎按 secret 的 `Arc` 指针缓存 | `api.rs:342-359` |
| `pwd_version` 走 DashMap + epoch 两级缓存,miss 才查库 | `api.rs:385-400` |
| 签 JWT | `api.rs:296-317` `mint_jwt` |
| 统一失败出口 | `api.rs:100-110` `err()` / `unauthorized()` |

### 1.2 关键结构

| 事实 | 位置 |
|---|---|
| `users.role` 有 CHECK 约束但**代码零引用**;`user_info` 硬编码返回 `["admin"]` | `migrations/0001_init.sql:5-11`、`api.rs:443` |
| 迁移是手写数组,**新文件不登记就静默不执行** | `db.rs:51-96`,数组在 `52-69` |
| `events` 落库是四元组 `(&str,&str,&str,&str)` | `db.rs:838` `insert_events` |
| `PendingEvent` 四字段;`write_batch` 逐字段拷贝 | `events.rs:27-34`、`events.rs:52-62` |
| AppState 里 `pwd_version_cache` + `pwd_version_epoch` 成对出现 | 字段声明在 `state.rs:70-72`;**初始化在 `454-455`**(`AppState::new` 起始于 `:390`) |
| 静态资源走 **fallback** 提供(两个分支) | `main.rs` 两个 `app = app.fallback(...)`:static_dir 分支在前,`else if embedded_ui::available()` 分支在后 |
| 未知 `/api/*` 返回 JSON 404,两处对称 | `main.rs` 的 `let is_api = req.uri().path().starts_with("/api/")`(static_dir 分支内) + `embedded_ui.rs` 的 `if path.starts_with("/api/")` |
| `serve` 已挂 `into_make_service_with_connect_info` | `main.rs` 末尾两处 `axum::serve(...)`(TLS / 明文各一) |

### 1.3 一条必须写进任务书的框架级证据

**中间件必须用 `route_layer`,不能用 `layer`。** `axum-0.8.9` 源码:

```rust
// axum-0.8.9/src/routing/mod.rs:303  fn layer,体内 312-315
path_router:     this.path_router.layer(layer.clone()),
fallback_router: this.fallback_router.layer(layer.clone()),   // ← 包 fallback
default_fallback: this.default_fallback,
catch_all_fallback: this.catch_all_fallback.map(|r| r.layer(layer)),

// axum-0.8.9/src/routing/mod.rs:321  fn route_layer,体内 330-333
path_router:       this.path_router.route_layer(layer),
fallback_router:   this.fallback_router,        // ← 不包
default_fallback:  this.default_fallback,       // ← 不包
catch_all_fallback: this.catch_all_fallback,    // ← 不包
```

官方文档 `axum-0.8.9/src/docs/routing/route_layer.md` 原文:

> This is useful for middleware that return early (such as authorization)
> which might otherwise convert a `404 Not Found` into a `401 Unauthorized`.

#### 1.3.1 为什么用 `route_layer` 而不是 `layer`(实测结论,勿照抄网上说法)

⚠️ **常见说法在此仓库不成立**。网上普遍说「用 `layer` 会让静态资源 401、
把 404 变 401」—— 这只在**层加在已含 fallback 的 Router 上**时才发生。
实测(axum 0.8.9,见下方矩阵):

| 路径 | A:route_layer | A:layer | B:layer | B:route_layer |
|---|---|---|---|---|
| `/api/clients` 无 token | 401 | 401 | 401 | 401 |
| `/api/nope` | **404** | **404** | **401** | **404** |
| `/` | **200** | **200** | **401** | **200** |
| `/assets/app.js` | **200** | **200** | **401** | **200** |

- **A = `.layer()` / `.route_layer()` 在 `.fallback()` 之前**(本仓库 `main.rs`
  的实际顺序:428 行 `api::router()` 返回后,432 行才 `.fallback()`)
- **B = 之后**(把整棵含 fallback 的树一起加层)

**本仓库目前是 A 形态,所以用 `layer` 当下不会白屏、也不会把 404 变 401。**
但**仍然必须用 `route_layer`**,理由是**脆弱耦合**而非当前 bug:

1. 用 `layer` 时,「静态资源是否被鉴权拦截」取决于 `main.rs` 里
   `.fallback()` 与 `.layer()` 的**相对位置**。这个位置关系是隐式的、
   没有任何测试钉住 —— 以后谁把 `.fallback()` 挪进 `api::router()`
   (很自然的做法),或改用 `nest_service` 托管静态资源,静态资源立刻全部
   401、控制台白屏,而且改动者完全看不出关联。
2. axum 官方对 authorization 类中间件的建议就是 `route_layer`
   (见下方引文),语义上「只对命中的路由鉴权」才是本意的准确表达。
3. `route_layer` 对 fallback 的增删**完全免疫**,不需要维护任何隐性前提。

**`route_layer` 的顺序约束**(同文档):只包住**调用前**已注册的路由。
所以每组必须 `.route(...)` 全部写完,再 `.route_layer(...)`,最后才 `.merge()`。

---

## 2. 目标形态

### 2.1 凭据格式

```
Authorization: Bearer rhk_<8 hex prefix>_<64 hex secret>
                   └─ 标签 ─┘ └─ 定位 ─┘  └── 随机 ────┘
                   └──────────────┬──────────────────────┘
                                  ↓
                    key_hash = sha256(【完整这一整串】)
```

> ⚠ **哈希对象是「完整的 `rhk_...` 整串」,不是只有 secret 部分。**
> 上图特意画成整串向下汇聚,就是为了避免第 11 轮那个致命 bug 重演:
> 当时 DDL 注释写 `sha256(secret)`、而校验代码算 `sha256(整串)`,
> 两种口径的 sha256 完全不同 → **所有 key 永远 401**。
>
> - `generate()`:`hash_hex(&plaintext)`(完整串)
> - `verify_api_key()`:`hash_hex(full)`(`parse` 返回的 full 也是完整串)
> - `parse()` 返回的 `full` **必须含 `rhk_` 前缀**
> 详见 B-1「哈希口径」段。

- **`rhk_` 标签**:用于分流(见 D-3),不做「JWT 解析失败再试 key」的顺序试探
- **8 hex prefix**:存库明文,用于**唯一索引定位**(避免全表扫);也用于展示 `rhk_9f2c1a4b…`
- **64 hex secret**:32 字节 CSPRNG,不落库(只存它的哈希)

### 2.2 路由分组(9 组,其中 1 组无保护)

| 组 | 路由 | 中间件配置 |
|---|---|---|
| **public** | `POST /api/auth/login` | **无中间件**(必须匿名) |
| **session** | `/api/auth/logout`、`/api/auth/password`、`/api/user/info`、`/api/auth/codes`、`/api/menu/all`、`/api/api-keys*` | `session_only = true` |
| **clients** | `/api/clients*` | `CLIENTS_R \| CLIENTS_W` |
| **services** | `/api/services*` | `SERVICES_R \| SERVICES_W` |
| **visitors** | `/api/visitors*` | `VISITORS_R \| VISITORS_W` |
| **stats** | `/api/stats/overview` | `STATS_R` |
| **events** | `/api/events` | `EVENTS_R` |
| **config** | `/api/server/config` | `CONFIG_R \| CONFIG_W` |
| **certs** | `/api/certs*` | `CERTS_R \| CERTS_W` |

**`misc` 组必须拆成 stats / events / config / certs 四组**。

> 下文说的「现状 `api.rs` 的 `misc` 组」是**包 E 改造前**的样子
> (在 `pub fn router` 里 `let misc = Router::new()` 那个块)。
> 包 E 完成后该块会被拆散,**这处行号随之失效** —— 按 `let misc =` 搜即可。

该组把四类权限混在一起,若不拆,一把「只读监控 key」就等于能读服务端
全量配置并列出证书。

**`session` 组为什么整组拒绝 API Key** —— 逐条理由(这不只是"顺手",每条都有依据):

| 路由 | 归 session-only 的理由 |
|---|---|
| `POST /api/auth/logout` | 副作用:会 `bump pwd_version` 吊销**该用户全部** JWT。让 Key 触发等于"一把 key 能让某个用户的所有会话下线" |
| `POST /api/auth/password` | 副作用 + 语义:改的是**用户的密码**,而 Key 不属于任何用户,无密码可改 |
| `GET /api/user/info` | **语义不匹配**:返回体是 `{userId, username, realName, roles:["admin"]}`,全部来自 JWT 的 `sub`。Key 没有 username/roles,要让 Key 调就得改返回结构(按 `Principal` 分支给不同 shape),把简单接口复杂化 |
| `GET /api/auth/codes` | 返回值是指令 **UI 渲染哪些按钮**的前端权限码。Key 是服务端调用,不渲染 UI,拿到空数组无意义 |
| `GET /api/menu/all` | 同上:菜单是 UI 概念 |
| `/api/api-keys*` | **防横向提权**:一把 ops key 若能调 `POST /api/api-keys`,就能给自己签发 full 权限的新 key |

> ⚠ **对原始需求「每个 API 都能用 API Key 访问」的一处有据偏离**:
> `user_info` / `auth_codes` / `menu_all` 三条**技术上可以**让 Key 访问
> (它们只读、无副作用)。不做的原因是**语义**:它们服务于浏览器 UI,
> 返回体围绕"当前登录用户"构造,对 Key 没有对应物。
> 若将来确实需要「用 Key 查询自己是什么身份」,应**新增**
> `GET /api/api-keys/self`(返回该 Key 的 `{id, name, scopes}`),
> 而不是复用 `user_info`。**本期不做。**

**`auth` 组不能统一加中间件**:`POST /api/auth/login` 必须匿名可访问。

### 2.3 scope 位定义(每资源固定 2 位:偶数 = 读,奇数 = 写)

| 位 | 常量 | 值 |
|---|---|---|
| 0 | `CLIENTS_R` | 1 |
| 1 | `CLIENTS_W` | 2 |
| 2 | `SERVICES_R` | 4 |
| 3 | `SERVICES_W` | 8 |
| 4 | `VISITORS_R` | 16 |
| 5 | `VISITORS_W` | 32 |
| 6 | `STATS_R` | 64 |
| 7 | *(留空)* | — |
| 8 | `EVENTS_R` | 256 |
| 9 | *(留空)* | — |
| 10 | `CONFIG_R` | 1024 |
| 11 | `CONFIG_W` | 2048 |
| 12 | `CERTS_R` | 4096 |
| 13 | `CERTS_W` | 8192 |

**完整模块,整段照抄**(不要只抄掩码,各常量漏一个就编译不过):

```rust
/// API Key 的权限位。每个资源占 2 位:偶数 = 读,奇数 = 写。
/// 只读资源(STATS/EVENTS)没有写位,其奇位**留空不用**。
pub mod scope {
    pub const CLIENTS_R:  u32 = 1 << 0;   // 1
    pub const CLIENTS_W:  u32 = 1 << 1;   // 2
    pub const SERVICES_R: u32 = 1 << 2;   // 4
    pub const SERVICES_W: u32 = 1 << 3;   // 8
    pub const VISITORS_R: u32 = 1 << 4;   // 16
    pub const VISITORS_W: u32 = 1 << 5;   // 32
    pub const STATS_R:    u32 = 1 << 6;   // 64
    // 位 7 留空(STATS 无写位)
    pub const EVENTS_R:   u32 = 1 << 8;   // 256
    // 位 9 留空(EVENTS 无写位)
    pub const CONFIG_R:   u32 = 1 << 10;  // 1024
    pub const CONFIG_W:   u32 = 1 << 11;  // 2048
    pub const CERTS_R:    u32 = 1 << 12;  // 4096
    pub const CERTS_W:    u32 = 1 << 13;  // 8192

    /// 偶数位全部(读方向)。是**位模式**,不是常量之和。
    pub const READ_MASK:  u32 = 0x1555;   // 5461
    /// 奇数位全部(写方向)。
    pub const WRITE_MASK: u32 = 0x2AAA;   // 10922

    /// 只读全权 = 所有读位。
    pub const READONLY: u32 = READ_MASK;  // 5461
    /// 运维常用 = 只读 + clients/services/visitors 三类写(不含 config/certs 写)。
    pub const OPS: u32 = READONLY | CLIENTS_W | SERVICES_W | VISITORS_W;  // 5503
    /// 全部权限。
    pub const FULL: u32 = READ_MASK | WRITE_MASK;                         // 16383

    #[cfg(test)]
    mod tests {
        use super::*;

        /// 每个读常量必须落在 READ_MASK 内,每个写常量必须落在 WRITE_MASK 内。
        /// 这条测试能一次性抓住"位排错"这一类错误(曾把 EVENTS_R 排到奇位 7)。
        #[test]
        fn bits_align_with_masks() {
            for c in [CLIENTS_R, SERVICES_R, VISITORS_R, STATS_R,
                      EVENTS_R, CONFIG_R, CERTS_R] {
                assert_eq!(c & READ_MASK, c, "读位未落在 READ_MASK 内: {c:#x}");
            }
            for c in [CLIENTS_W, SERVICES_W, VISITORS_W, CONFIG_W, CERTS_W] {
                assert_eq!(c & WRITE_MASK, c, "写位未落在 WRITE_MASK 内: {c:#x}");
            }
            assert_eq!(READ_MASK & WRITE_MASK, 0, "读写位不得重叠");
            assert_eq!(FULL, READ_MASK | WRITE_MASK);
            assert_eq!(OPS & CONFIG_W, 0, "OPS 不得含 CONFIG_W");
            assert_eq!(OPS & CERTS_W, 0, "OPS 不得含 CERTS_W");
            assert_eq!(OPS & READONLY, READONLY, "OPS 必须含全部读位");
        }
    }
}
```

> ⚠ **四条必须照抄,别自己推**:
>
> 1. **每个资源占连续 2 位,读在偶位、写在奇位**。`STATS_R` / `EVENTS_R`
>    是「只读资源」,没有写位,**必须留空相邻的奇位**,不能压紧排列。
>    若把 `EVENTS_R` 排在位 7(奇数),它就不被 `READ_MASK` 覆盖,
>    在 §2.4 规则下该组 `allowed` 恒为 0 → **任何 key(含 scopes=0)
>    都能读 `/api/events`**(实测已确认)。这是本方案最隐蔽的坑。
>    因此位 7、9 **保留不用**,任何 scope 常量都不得占用。
> 2. `READ_MASK` / `WRITE_MASK` 是 **`0x1555` / `0x2AAA`**,不是 `0x0555` / `0x0AAA`。
> 3. `OPS` 是 **`0x157F`(5503)**,不是 `0x057F` 也不是 `0x05FF`。
>    它只含 `CLIENTS_W|SERVICES_W|VISITORS_W` 三类写,**不含 `CONFIG_W` / `CERTS_W`**。
> 4. `FULL` 用 **`READ_MASK | WRITE_MASK`(=`0x3FFF`)**,不要写成"所有常量之和"
>    (那是 `0x3D7F`,漏掉保留位,两者在 `create_api_key` 的 scopes 校验里
>    行为不同:用 `0x3FFF` 时传入保留位不会被拒,但它们永不参与判权)。
>
> 校验方式(写进单测,能一次性钉住上面所有坑):
> - 每个 `*_R` 常量断言 `C & READ_MASK == C`
> - 每个 `*_W` 常量断言 `C & WRITE_MASK == C`
> - 断言 `READ_MASK & WRITE_MASK == 0`(读写位不重叠)
> - 断言 `OPS & CONFIG_W == 0 && OPS & CERTS_W == 0`

### 2.4 判权规则(机械可测,**必须 fail-closed**)

```rust
let dir = if matches!(method, GET | HEAD) { READ_MASK } else { WRITE_MASK };
let allowed = cfg.need & dir;

// ★allowed == 0 一律拒绝,不能放行。
if allowed == 0 || scopes & allowed == 0 {
    return forbidden("API Key 权限不足");   // HTTP 403,见 §2.5
}
```

**为什么必须是 `allowed == 0` 也拒绝**:`allowed == 0` 只有两种成因 ——
(a) 配置写错(该组 `need` 的位没落在对应方向),或 (b) 请求方法与该组配置缺项。
两种都是**配置缺陷**,此时放行等于把「写错配置」变成「静默全放通」。
fail-closed 下写错配置会表现为「该组全部 403」,立刻被发现;fail-open 下
则表现为「该组对任意 key 开放」,**没有任何报错**。

上一轮任务书写的是 `allowed != 0 && scopes & allowed == 0`(fail-open),
配合当时错位的 `EVENTS_R` 位号,会真实产生「scopes=0 的 key 可读 /api/events」。
fail-closed + §2.3 的正确位分配,两道保险都要有。

`Principal::User`(JWT)跳过 scope 检查,天然全权。

**这条规则有一个前提假设,必须写清:GET/HEAD = 读,其余 = 写**

它成立的基础是 **REST 语义**(RFC 7231 §4.2.1:GET/HEAD 必须是安全方法,
不得有副作用)。核对现有 18 条 Key 可达路由后确认全部符合:

| 检查项 | 结果 |
|---|---|
| 有无 `GET` 却产生副作用的接口 | **无** |
| 有无 `POST` 但语义是"只读"的接口 | **无** —— `rotate-secret` / `rotate-service-secret` 虽是 POST,但**确实会 UPDATE 数据库**(`db.rotate_client_secret`),归"写"正确 |
| 同一路径上 GET 与 PUT 并存(如 `/api/server/config`) | 可正确分权(实测:只有 `CONFIG_R` 的 key → GET 200、PUT 403) |

> ⚠ **新增接口时必须复核这条假设**。若将来引入「GET 但有副作用」的接口
> (如 `GET /api/xxx?force=true` 触发重建),按本规则它会被判成"读",
> 一把只读 key 就能触发写操作。届时应:
> 1. 改掉该接口的方法(用 POST),或
> 2. 在 `guard` 里加**按路径覆盖**的白名单(不要改全局规则)

### 2.5 403 必须用真实 HTTP 状态码

⚠️ **`api.rs:100-102` 的 `err()` 返回的是 HTTP 200 + body `{code:403}`**:

```rust
pub fn err(code: i32, message: &str) -> Response {
    Json(json!({"code": code, "message": message, "data": null})).into_response()
    //   ↑ 没有 StatusCode,默认 200
}
```

实测确认:`err(403, "…")` 的 HTTP 状态码是 **200**,不是 403。
只有 `unauthorized()`(`api.rs:104-110`)显式带了 `StatusCode::UNAUTHORIZED`。

**本项目此前从未返回过 403**(全仓 `grep FORBIDDEN` 零命中),所以没有可复用的先例。
权限拒绝是协议层语义,curl / kubectl / CI 都按状态码分支,**不能复用 `err()`**。
必须新增:

```rust
fn forbidden(message: &str) -> Response {
    (StatusCode::FORBIDDEN,
     Json(json!({"code": 403, "message": message, "data": null}))).into_response()
}
```

- 业务错误(参数校验、重复名等)继续用 `err(code, msg)` → HTTP 200,保持现状
- **鉴权/授权失败必须用 `unauthorized()`(401)或 `forbidden()`(403)**
- 前端 `request.ts:63-69` 的 401 拦截器认的是 `error.response.status`,
  403 会走 `feedbackError` 展示 message,无需改动

---

## 3. 任务包

### 包 A:数据层

#### A-1 [P0] 新建迁移 `migrations/0005_api_keys.sql`

```sql
CREATE TABLE api_keys (
  id           TEXT PRIMARY KEY,              -- uuid v4
  name         TEXT NOT NULL UNIQUE,          -- 人类可读,如 "ci-terraform"
  prefix       TEXT NOT NULL,                 -- 8 hex,展示 + 唯一索引定位
  key_hash     TEXT NOT NULL,                 -- sha256(完整 bearer) 的 hex,见 B-1
  scopes       INTEGER NOT NULL DEFAULT 0,
  enabled      INTEGER NOT NULL DEFAULT 1 CHECK (enabled IN (0,1)),
  created_at   INTEGER NOT NULL,
  created_by   TEXT NOT NULL DEFAULT '',
  last_used_at INTEGER,
  expires_at   INTEGER                        -- NULL = 永不过期
);
CREATE UNIQUE INDEX idx_api_keys_prefix ON api_keys(prefix);
```

**不要动 `events` 表**(见 F-3 的理由)。

**已实测该 DDL 在 SQLite 上可执行**:建表成功、`CHECK(enabled IN (0,1))`
拒绝非法值、`prefix` 唯一约束拒绝重复。10 个列名与预期一致。

**两个设计说明**:

1. `prefix` 用**独立 `CREATE UNIQUE INDEX`** 而不是列级 `UNIQUE`。
   功能等价(SQLite 两者都建 autoindex),但独立索引便于将来改成
   部分索引(若软删策略要让「已吊销 key 的 prefix 可回收」,
   就改成 `... WHERE enabled = 1`)。现在不必,留个口子。
2. **不做物理删除**。`delete_api_key` 只 `enabled = 0`(见 F-2),
   保留行以便 `events` 里的审计记录能对上 `id`。因此 `name` 的
   `UNIQUE` 会导致「删掉的 key 名字不能复用」—— **这是有意为之**,
   避免审计歧义。若用户抱怨,再考虑改成
   `CREATE UNIQUE INDEX ... ON api_keys(name) WHERE enabled = 1`。

- [x] 验收:文件能被 `sqlx::raw_sql` 整段执行(含注释与多语句);
      启动后 `PRAGMA table_info(api_keys)` 有 10 列。
      【已验 f6ed67c】全新 DB 启动,server.log 出现全部迁移;
      `sqlite3 rabbithole.db "PRAGMA table_info(api_keys)"` 输出 10 行。

#### A-2 [P0] 登记迁移 —— **漏了就会静默不执行**

`crates/rh-server/src/db.rs:52-69` 的 `MIGRATIONS` 数组:
**在 `0004_user_pwd_version` 条目(66-67 行)之后、数组结尾 `];`(69 行)之前**
插入一行:

```rust
("0005_api_keys", include_str!("../../../migrations/0005_api_keys.sql")),
```

- [x] 验收:启动日志出现 `migration applied name="0005_api_keys"`;
      或单测断言 `_migrations` 表含该 name。
      【已验 f6ed67c】全新 DB 启动日志实测:
      `INFO rh_server::db: migration applied name="0005_api_keys"`;`_migrations` 表含 0001-0005 五行。

#### A-3 [P0] 加 6 个 DB 方法

加在 `db.rs` 的 `// ---------- users ----------` 之后,写法照
`db.rs:285-300`(`rotate_client_secret` / `delete_client`)的
`sqlx::query(...).bind(...).execute(&self.pool)` 风格;
行结构体照 `ClientRow::from_row(&SqliteRow)` 模式。

**六个方法的完整签名**(返回类型必须写清,否则 C-2/D-4 无法对接):

```rust
// 行结构:DB 层只做字段映射,不含任何鉴权/缓存逻辑
#[derive(Clone)]
pub struct ApiKeyRow {
    pub id: String,
    pub name: String,
    pub prefix: String,
    pub key_hash: String,
    pub scopes: u32,
    pub enabled: bool,
    pub created_at: i64,
    pub created_by: String,
    pub last_used_at: Option<i64>,
    pub expires_at: Option<i64>,
}

impl Db {
    pub async fn create_api_key(
        &self, id: &str, name: &str, prefix: &str, hash: &str,
        scopes: u32, created_by: &str, expires_at: Option<i64>,
    ) -> Result<()>;

    /// ★ 返回含 key_hash(鉴权要用);`list_api_keys` 才排除它。
    pub async fn get_api_key_by_prefix(&self, prefix: &str) -> Result<Option<ApiKeyRow>>;

    /// 列表用。**仍返回 ApiKeyRow(含 key_hash)**,由 API 层决定不回显 ——
    /// 这样 DB 层无需定义第二个"少字段"的结构体。
    pub async fn list_api_keys(&self) -> Result<Vec<ApiKeyRow>>;

    pub async fn set_api_key_enabled(&self, id: &str, enabled: bool) -> Result<bool>;

    pub async fn rotate_api_key(&self, id: &str, new_prefix: &str, new_hash: &str)
        -> Result<bool>;

    pub async fn touch_api_key(&self, id: &str, ts: i64) -> Result<()>;
}
```

各方法语义：
**六个方法的 SQL**(逐条给全,别让实现者猜列顺序):

```sql
-- create_api_key:last_used_at 不写 → 落 NULL(表示"从未使用过")
INSERT INTO api_keys
  (id, name, prefix, key_hash, scopes, enabled, created_at, created_by, expires_at)
VALUES (?1, ?2, ?3, ?4, ?5, 1, ?6, ?7, ?8);

-- get_api_key_by_prefix:鉴权用,只认有效的
SELECT id, name, prefix, key_hash, scopes, enabled,
       created_at, created_by, last_used_at, expires_at
  FROM api_keys WHERE prefix = ?1 AND enabled = 1;

-- list_api_keys:管理用,含已吊销
SELECT id, name, prefix, key_hash, scopes, enabled,
       created_at, created_by, last_used_at, expires_at
  FROM api_keys ORDER BY created_at DESC;

UPDATE api_keys SET enabled  = ?2 WHERE id = ?1;
UPDATE api_keys SET prefix   = ?2, key_hash = ?3 WHERE id = ?1;
UPDATE api_keys SET last_used_at = ?2 WHERE id = ?1;
```

| 方法 | 说明 |
|---|---|
| `create_api_key` | INSERT(见上,**不含 `last_used_at`**) |
| `get_api_key_by_prefix` | `WHERE prefix=?1 AND enabled=1`,**至多 1 行**;`enabled=0` 直接当不存在 |
| `list_api_keys` | 全部(含已吊销),按 `created_at DESC` |
| `set_api_key_enabled` | 软删/启用;返回是否命中行 |
| `rotate_api_key` | 换 prefix + hash;返回是否命中行 |
| `touch_api_key` | `UPDATE last_used_at`(低频调用) |

> ⚠ **`list_api_keys` 里 `enabled=0` 的行也要返回**(前端要显示"已吊销"状态)。
> 只读"有效 key"的是 `get_api_key_by_prefix`,它带 `AND enabled=1`。
> **两个方法的过滤条件不同,不要合并。**

**A-3b [P0] 顺带把 `db.rs` 的 `now_ms` 改成 `pub(crate)`**:

```rust
// db.rs:13,改前
fn now_ms() -> i64 {
// 改后
pub(crate) fn now_ms() -> i64 {
```

**这一步必须做**,否则 `state.rs` 里的 `apikey_touch` 调 `crate::db::now_ms()`
会报 **`E0603: function now_ms is private`**(已实测)。

- [x] 验收:`cargo build -p rh-server` 通过(含 `now_ms` 可见性修改)。
      【已验 f6ed67c】`cargo build -p rh-server` 0 error;clippy/test/fmt 三道门同提交全绿。

---

### 包 B:凭据原语(新建 `crates/rh-server/src/apikey.rs`)

全是纯函数,不碰 IO,便于单测。`main.rs` 里加 `mod apikey;`。

#### B-1 [P0] 生成与解析

```rust
// ★ `use rand::RngCore;` **只在文件顶部出现一次**。
//   `generate()` 内部不要再写一遍 —— 重复 use 会触发 `unused import` 警告,
//   在 `clippy -D warnings` 下直接编译失败(已实测)。
//   见下方「generate() 的完整实现」——那里刻意没有再写 use。
use rand::RngCore;

pub const TAG: &str = "rhk_";

pub struct Generated { pub plaintext: String, pub prefix: String, pub hash: String }

/// rhk_ + 8hex + "_" + 64hex
pub fn generate() -> Generated;
/// 从整个 bearer 值剥出 (prefix, full)。
/// ★ `full` = **完整的 bearer 串**(含 `rhk_` 前缀),不是 secret 部分。
///   因为落库的 key_hash 就是 `sha256(完整 bearer)`,校验时直接对 `full` 哈希即可。
/// 形态不对(缺前缀/前缀非 8 hex/secret 非 64 hex)返回 None。
pub fn parse(bearer: &str) -> Option<(&str, &str)>;
pub fn hash_hex(s: &str) -> String;   // hex(sha256(s))
```

> ⚠ **上面这块是"接口概览",不是可编译代码** —— 函数只有签名没有体。
> 具体实现见下方两块:`generate()` 的完整实现 + B-2 的 `ct_eq`。
**`parse` / `hash_hex` 的完整实现**(上面概览里只给了签名):

```rust
pub fn hash_hex(s: &str) -> String {
    // 需 `use sha2::{Digest, Sha256};`
    hex::encode(Sha256::digest(s.as_bytes()))
}

pub fn parse(bearer: &str) -> Option<(&str, &str)> {
    let rest = bearer.strip_prefix(TAG)?;
    let (p, f) = rest.split_once('_')?;
    if p.len() != 8 || !p.chars().all(|c| c.is_ascii_hexdigit()) { return None; }
    if f.len() != 64 || !f.chars().all(|c| c.is_ascii_hexdigit()) { return None; }
    Some((p, bearer))       // ★ 返回 (prefix, 完整 bearer)
}
```

> **不要**把"概览块"和"实现块"直接拼接 —— `generate` 会重复定义(`E0428`)。

**大小写与空格(边界语义,早期版本没写)**:

- `generate()` 只产出**小写** hex(`hex::encode` 的行为)。
- `parse()` 用 `is_ascii_hexdigit()` 判断,**大小写都接受** —— 这是刻意的:
  拒绝大写会让"用户从某处复制到被大写的 key"直接 401,而 parse 报
  "形态错误"和"hash 不匹配"在 HTTP 上**都是 401**,用户看不出区别。
- 但 hash 是对**原始输入串**算的,所以**大写 key 一定校验失败**
  (除非建 key 时也用了大写)。这是正确且安全的行为,**不要**为了
  "大小写不敏感"去 `to_lowercase()` —— 那会让哈希口径变复杂。
- **不做 `trim()`**。带前后空格的 key 视为形态错误(401)。

- [x] 验收补充:单测里加一条 `parse_accepts_uppercase_but_hash_mismatch`,
      断言大写形态能 parse 成成功但 `hash_hex` 结果与建 key 时不同。
      【已验 d1ed221,实现方式有偏差】断言并入 apikey.rs 的
      `parse_accepts_valid`(parse 大写成功 + hash_hex(大写)!=hash_hex(小写)),
      未单独成测 —— 为守住 §1.0 的"测试总数 25"(H-1 表 19 行 vs 声称 17,两行须折入相邻用例)。

**`generate()` 的完整实现**(哈希口径必须与 `parse` 一致)。
**这一块替换上面的声明,不是追加** —— 两者是同一个函数:

```rust
pub fn generate() -> Generated {
    // ★ 不写 `use rand::RngCore;` —— 文件顶部已有,重复会触发 unused import
    // 8 hex 前缀 + 64 hex secret,共用一份随机源
    let mut buf = [0u8; 36];                  // 4 字节前缀 + 32 字节 secret
    rand::rng().fill_bytes(&mut buf);
    let prefix = hex::encode(&buf[..4]);      // 8 hex
    let secret = hex::encode(&buf[4..]);      // 64 hex
    let plaintext = format!("rhk_{prefix}_{secret}");

    Generated {
        // ★ hash 对**完整 plaintext** 取,不是对 secret 取 —— 与 parse 返回的
        //   `full` 口径严格一致。两者不一致会导致所有 key 永远 401。
        hash: hash_hex(&plaintext),
        prefix,
        plaintext,
    }
}
```

> ⚠ **哈希口径是本节最容易错的地方**(实测:两种口径的 sha256 完全不同)。
> 定案:**`key_hash = sha256("rhk_<prefix>_<secret>")`**,即对完整串取哈希。
>
> - `generate()`:对 `plaintext`(完整串)取 → 存 `hash`
> - `verify_api_key()`(D-4 第 3 步):对 `parse()` 返回的 `full`(同样是完整串)取
> - `parse()` 返回的 `full` **必须包含 `rhk_` 前缀**
>
> 若改成"只对 secret 取哈希",则 `parse` 要返回 `(prefix, secret)`,
> 且 D-4 第 3 步要改成 `hash_hex(secret)`。**两种都行,但必须全链一致。**
> 本任务书统一选"对完整串取哈希",理由是调用方不需要关心 secret 的切分。

**随机源要注意(已实测,别照抄 `sshgateway.rs:209`)**

`rh-server` 同时依赖两个 rand:

| 别名 | 实际版本 | 根是否导出 `RngCore` |
|---|---|---|
| `rand`(= `workspace.dependencies`) | **0.9.5** | ✅ 可以 `use rand::RngCore;` |
| `rand_010`(package = "rand", 0.10) | **0.10.3** | ❌ 必须 `use rand_010::prelude::*;` |

实测结果:
```rust
use rand::RngCore;              // ✓ 编译通过
rand::rng().fill_bytes(&mut b); // ✓

use rand_010::RngCore;          // ✗ error[E0432]: no `RngCore` in the root
rand_010::prelude::*;           // ✓ 才能拿到 fill_bytes
```

**用 `rand`(0.9),不要用 `rand_010`**:

```rust
let mut buf = [0u8; 32];
rand::rng().fill_bytes(&mut buf);
let secret = hex::encode(buf);   // 64 hex
```

这与仓库既有写法**完全一致**——`crates/rh-core/src/stun.rs:20`
就是 `rand::rng().fill_bytes(&mut tx_id)`(配 `use rand::RngCore;`)。
`sshgateway.rs:209` 那个 `rand_010::rng()` 是给 `PrivateKey::random(&mut rng, ..)`
用的,那里不需要 `RngCore`,**不要模仿它**。

**不要新增依赖**:`rand` / `sha2` / `hex` 都已在
`crates/rh-server/Cargo.toml` 中。

#### B-2 [P0] 常量时间比较

```rust
/// 零依赖常量时间相等。长度不等直接 false(长度本身不是秘密)。
pub fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() { return false; }
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}
```

**不要为这个加 `ring` 依赖** —— `rh-server` 的 `Cargo.toml` 里没有 `ring`。

#### B-3 [P0] 单测(内联 `#[cfg(test)] mod`)

| 用例 | 断言 |
|---|---|
| `parse_rejects_malformed` | `rhk_` 空尾、缺 `_`、非 hex、超长 全部 `None` |
| `parse_accepts_valid` | 正常形态返回 (prefix, full) |
| `ct_eq_semantics` | 等长不等 → false;长度不等 → false 且不 panic;相等 → true |
| `generate_unique` | 连续 1000 次无重复;plaintext 以 `rhk_` 开头;hash 长度 64 |

- [x] 验收:`cargo test -p rh-server apikey` 全绿。
      【已验 d1ed221】输出 4 passed:parse_rejects_malformed /
      parse_accepts_valid / ct_eq_semantics / generate_unique。

---

### 包 C:缓存(`crates/rh-server/src/state.rs`)

#### C-1 [P0] 加三字段

放在 `state.rs:70-72`(`pwd_version_cache` / `pwd_version_epoch`)旁边:

```rust
/// API Key 缓存(require 热路径读;键 = prefix)
pub apikey_cache: dashmap::DashMap<String, CachedApiKey>,
/// 失效代:创建/吊销/rotate 时 bump,令全部条目失配
pub apikey_epoch: std::sync::atomic::AtomicU64,
/// last_used_at 节流:每 key 60s 最多写一次
pub apikey_touch_at: dashmap::DashMap<String, std::time::Instant>,
```

**`CachedApiKey` 的定义**(放在 `state.rs`,与 `AppState` 同文件;
因为它是 `pub` 结构体字段的类型,**必须也是 `pub`**,否则触发
`private_interfaces` 告警 → `clippy -D warnings` 直接红):

```rust
#[derive(Clone)]
pub struct CachedApiKey {
    /// ★ 必须带 id / name,否则 D-4 第 7 步构造不出 `Principal::Key{id,name,scopes}`
    pub id: String,
    pub name: String,
    pub key_hash: String,
    pub scopes: u32,
    pub enabled: bool,
    pub expires_at: Option<i64>,
    /// 填充时的 `apikey_epoch` 快照;与当前 epoch 不等即视为 miss(见 C-2)
    pub epoch: u64,
}
```

> ⚠ **`id` / `name` 不能省**。早期版本的 `CachedApiKey` 只有
> `key_hash / scopes / enabled / expires_at / epoch`,而 `Principal::Key`
> 需要 `{id, name, scopes}` —— D-4 第 7 步要用 `id` 做审计、`name` 写进
> `events.detail` 的 actor,缺了就无法构造。
> 带上这两个字段只多两次 `String` 克隆(缓存填充时一次),可接受。

初始化加在 **`state.rs:454-455`**(与 `pwd_version_cache` / `pwd_version_epoch`
同一批,`AppState::new` 在 `state.rs:390` 起)。

> ⚠ 早期版本写的是 `445-447`,那是 `login_limiter` 的构造区,**位置错了**。
> 正确锚点是:
> ```
> 453:  visitor_cache: dashmap::DashMap::new(),
> 454:  pwd_version_cache: dashmap::DashMap::new(),
> 455:  pwd_version_epoch: std::sync::atomic::AtomicU64::new(0),
> 456:  client_cfg_gen: Arc::new(dashmap::DashMap::new()),
> ```
> 三个新字段插在 455 与 456 之间。

#### C-2 [P0] `lookup` —— miss 才查库

**签名(必须 `async`,因为 miss 时要查库)**:

```rust
impl AppState {
    /// 按 prefix 查缓存的 API Key。hit 零 IO;miss 查一次 DB 并回填。
    ///
    /// **本层只负责「取出记录」,不做任何有效性判断**:
    /// - 库里没有 → None
    /// - 库里有(哪怕 enabled=0 或已过期)→ Some(CachedApiKey)
    ///
    /// 有效性判断(disabled / expired)统一放在 D-4 `verify_api_key` 里,
    /// 这样「哪一步拒绝」在调用链上只有一个地方,便于审计与测试。
    pub async fn lookup_api_key(&self, prefix: &str) -> Option<CachedApiKey>;
}
```

> ⚠ **有效性判断的归属必须唯一**。
> 早期版本注释写「None 表示 库里没有/已禁用/已过期」,但 D-4 又把
> `!enabled` 和 `expires_at` 列为独立校验步骤(第 4、5 步)—— 两处都判会
> 让「为什么被拒」无法区分,且测试要覆盖的组合数翻倍。
>
> **定案:`lookup_api_key` 只查不判**;`verify_api_key` 按 D-4 的 7 步顺序判。
> 代价是过期 key 仍会进缓存(占用一条 entry),但它本就在库里,且
> `invalidate_apikeys()` 在 rotate/吊销时会清 —— 可接受。

> ⚠ 早先版本只给了「epoch 守卫对照表」,没给签名,容易写成同步函数 —— 那样
> 中间件里就无法 `.await`,且 miss 时不能查库。**必须是 `async fn`。**

**照抄 `api.rs:385-400` 的 epoch 守卫**,逐行对应:

| `pwd_version` 版 | `apikey` 版 |
|---|---|
| `pwd_version_epoch.load(Acquire)` | `apikey_epoch.load(Acquire)` |
| `.get(&user).filter(\|e\| e.value().1 == epoch_now)` | `.get(prefix).filter(\|e\| e.value().epoch == epoch_now)` |
| miss → 查库 → `insert(user, (v, epoch_now))` | miss → 查库 → `insert(prefix, CachedApiKey{.., epoch: epoch_now})` |

**这一步不能省**。直连 DB 会让每个 API Key 请求多一次 SQLite 往返;现有
`pwd_version` 缓存正是因为这一点才加的(`state.rs:63-69` 注释原文:
「修前每个已认证 API 请求一次 SQLite 往返 —— UI 轮询下是全进程最高频 DB 查询」)。

#### C-3 [P0] `invalidate_apikeys()`

```rust
pub fn invalidate_apikeys(&self) {
    self.apikey_epoch.fetch_add(1, Ordering::Release);
}
```

**三个失效点**:创建 / 吊销(软删)/ rotate。**不要 remove 条目** —— epoch 失配
即视为 miss,下次查库覆盖。与 `pwd_version` 同策略,避免「bump 与填充交错」
把旧值钉死(`state.rs:66-69` 已记录该竞态)。

**已知代价:失效后的条目会残留(不是泄漏,但有上界)**

epoch 失配的条目**不会被删除**,只是下次同 prefix 请求时被覆盖。所以:

- **rotate 换了 prefix** → 旧 prefix 的条目**永久残留**(那个 prefix 再也不会被查到)
- 吊销/其他失效 → 同 prefix 再被访问时覆盖,不增长

实测:访问过 N 个不同 prefix 后,`apikey_cache.len() == N`,与 epoch bump 次数无关。

**当前规模的判断:可忽略**。理由:
1. key 是人工创建的,数量级 < 10³
2. rotate 频率极低,每次只多留 1 条(旧 prefix)
3. 单条 ≈ 6 个字段 + 3 个 String ≈ 200 字节;1 万次 rotate 也只 ~2 MB

**若将来需要治理**,在 `invalidate_apikeys()` 里加一句
`self.apikey_cache.retain(|_, e| e.epoch == new_epoch)` 即可(注意要在
`fetch_add` **之后**取新值)。**现在不要加** —— 多一次全表扫,换来的收益
在这个规模下不值。

#### C-4 [P0] `apikey_touch` 节流

```rust
impl AppState {
    /// 节流写 last_used_at。同步方法(内部 tokio::spawn,不阻塞调用方)。
    pub fn apikey_touch(&self, id: &str);
}
```

60s 内已记录则直接返回;否则记录时间戳并 `tokio::spawn` 异步写库。

> ⚠ 上面那块是**签名概览**(无函数体)。下面给出**完整实现,替换它而不是追加**
> —— 同一个函数,拼在一起会报 `E0428: name apikey_touch is defined multiple times`。

**时间单位必须统一为毫秒**(全项目口径):`db.rs` 的 `now_ms()` 返回
`i64` 毫秒,`clients.created_at` / `events.ts` / `certs.not_after` 全是毫秒。
节流判断用 `Instant`(单调时钟,不受系统时间跳变影响):

```rust
pub fn apikey_touch(&self, id: &str) {
    let now = std::time::Instant::now();
    // 60s 内已写过就跳过
    if let Some(prev) = self.apikey_touch_at.get(id) {
        if now.duration_since(*prev).as_secs() < 60 {
            return;
        }
    }
    self.apikey_touch_at.insert(id.to_string(), now);

    // ★ 写库用毫秒时间戳(与项目口径一致),放在 spawn 里
    // 前提:db.rs 的 now_ms 已改为 pub(crate)(见下方易错点 1)
    let db = self.db.clone();
    let id = id.to_string();
    tokio::spawn(async move {
        let _ = db.touch_api_key(&id, crate::db::now_ms()).await;
    });
}
```

> ⚠ 三个易错点:
> 1. **`db::now_ms()` 是私有函数**(`db.rs` 里是裸 `fn now_ms()`,不是 `pub`)。
>    在 `state.rs` 里写 `crate::db::now_ms()` 会报
>    **`E0603: function now_ms is private`**(已实测)。
>    要么把 `db.rs` 的声明改成 `pub(crate) fn now_ms()`,
>    要么在 `state.rs` 本地取时间:
>    ```rust
>    let ms = std::time::SystemTime::now()
>        .duration_since(std::time::UNIX_EPOCH)
>        .map(|d| d.as_millis() as i64).unwrap_or(0);
>    ```
>    **推荐前者**(改成 `pub(crate)`),避免两处取时间逻辑漂移。
> 2. **节流用 `Instant`,落库用毫秒** —— 不要拿毫秒时间戳做 60s 比较
>    (系统时间回拨会让节流失效)。
> 2. `apikey_touch_at` 只增不减会随 key 数量缓慢增长;key 是人工创建的
>    (数量级 < 10³),不构成泄漏,**不需要 GC**(与 `login_limiter` 的
>    无界增长问题不同,那是攻击者可驱动的)。

**绝不能每请求 UPDATE**。SQLite 是单写者(WAL 也只允许一个并发写),
`UPDATE api_keys SET last_used_at` 在热路径上会成为全局串行点。

- [x] 验收(两部分,**都必须写明怎么断言**,否则测不出来):【已验 6cfc9d4,测试落在 state.rs::apikey_cache_tests】
      a) `apikey_touch_throttles`:touch 1 次 + sleep(100ms) 后
      `last_used_at` 为 Some;再 touch 999 次 + sleep 后值不变(b 外部可观测断言)。
      b) `apikey_cache_epoch_invalidates`:lookup(p1)=Some →
      db.rotate + invalidate → lookup(p1)=None、lookup(newp)=Some。

  **a) 节流生效:1000 次 touch 只落 1 次库**

  `apikey_touch` 内部是 `tokio::spawn` 异步写,直接断言会读到 0(任务还没跑)。
  测试必须有**可观测的写入计数**:

  **不要给 `Db` 加计数钩子** —— `Db` 只是 `SqlitePool` 的薄包装
  (`db.rs:9`),为测试改生产结构体不值得。用**真实可观测的结果**断言:

  ```rust
  // 用内存库
  let db = Db::open("sqlite::memory:").await.unwrap();
  // 先建一条 key,last_used_at 初值为 NULL
  db.create_api_key("id1","n","pfx","hash",0,"admin",None).await.unwrap();
  let state = Arc::new(AppState::new(cfg, db));

  // 第 1 次 touch:应写库
  state.apikey_touch("id1");
  tokio::time::sleep(Duration::from_millis(100)).await;   // ★ 等 spawn
  let t1 = state.db.get_api_key_by_prefix("pfx").await.unwrap().unwrap().last_used_at;
  assert!(t1.is_some(), "首次 touch 应写入 last_used_at");

  // 后续 999 次:60s 内应全部跳过 —— last_used_at 不变
  for _ in 0..999 { state.apikey_touch("id1"); }
  tokio::time::sleep(Duration::from_millis(100)).await;
  let t2 = state.db.get_api_key_by_prefix("pfx").await.unwrap().unwrap().last_used_at;
  assert_eq!(t1, t2, "60s 内重复 touch 不应再写库");
  ```

  这样断言的是**外部可观测行为**(DB 里的值变没变),而不是内部计数器 ——
  与项目既有测试风格一致(AGENTS.md:「断言基于外部可观测行为」)。
  **`sleep` 不能省**:`apikey_touch` 是 `tokio::spawn`,不等它跑完
  第一次读到的是 `None`。

  **b) 失效后 lookup 返回 miss**

  ```rust
  // 先填充缓存
  assert!(state.lookup_api_key("p1").await.is_some());
  // rotate 后 bump epoch
  state.invalidate_apikeys();
  // 同一个 prefix 再查:epoch 失配 → 重新查库 → 若库里已无该 prefix 则 None
  assert!(state.lookup_api_key("p1").await.is_none());
  ```
  注意:`invalidate_apikeys()` 只 bump epoch,**不会删缓存条目** ——
  所以这条断言的前提是「库里已经没有 p1」(rotate 换了 prefix)。

---

### 包 D:Principal / scope / 凭据解析

#### D-1 [P0] `Principal`

```rust
#[derive(Clone)]
pub enum Principal {
    User(String),                                          // JWT sub
    Key { id: String, name: String, scopes: u32 },
}
```

需 `Clone`(要放进 `Extension`)。

#### D-2 [P0] `scope` 模块

常量见 §2.3。

**放置位置:本任务书统一放在 `api.rs` 内的 `pub mod scope`**(与 E-3 骨架的引用写法一致)。

| 方案 | 引用写法 | 是否采用 |
|---|---|---|
| **`api.rs` 内 `pub mod scope { .. }`** | `scope::CLIENTS_R`(短路径) | ✅ **采用** |
| 独立 `crates/rh-server/src/scope.rs` | 必须 `crate::scope::CLIENTS_R` 或在 `api.rs` 顶部加 `use crate::scope;` | ❌ 不采用 |

> ⚠ **两种方案不能混用**:E-3 骨架里写的是短路径 `scope::CLIENTS_R`,若你选了独立文件,
> **所有 8 处 `guard(...)` 和 `Cfg` 里的 `scope::` 都要改成 `crate::scope::`** ——
> 否则报 `E0433: failed to resolve: use of undeclared crate or module 'scope'`。
> 选一种并贯彻到底。

#### D-3 [P0] `resolve_principal` —— **唯一一份凭据解码逻辑**

```rust
/// 异步(需查 apikey 缓存/DB)。★入参用 header 值而非 &HeaderMap/&Request:
/// 中间件要把它放在 async 块里 await,借用 Request 会让 future 不 Send(见 E-1 细节 2)。
/// 调用方先 to_owned() 再传。
async fn resolve_principal(
    state: &Arc<AppState>,
    bearer: Option<String>,     // Authorization 头值,已 owned
) -> Result<Principal, Response>
```

> ⚠ 若要 `IpAddr`(per-IP 限速,本期不做),从 `req.extensions()` 取
> `ConnectInfo`,**不要**给中间件加 `State` 参数(见 E-1)。

结构:**把现有 `api.rs:362-408` 的 JWT 逻辑整体平移进来,一行不改**,
只在最前面插入 `rhk_` 分流。

> ⚠⚠ **这里有一个致命的细节,必须照抄**(指**前 3 段的分流逻辑**;末尾的
> `...` 是"接原 require_auth 剩余代码"的省略,不是要照抄的字面内容):
> 中间件传进来的 `bearer` 是**完整的 Authorization 头值**(形如
> `"Bearer rhk_9f2c1a4b_<64hex>"`),**不是**剥掉 `Bearer ` 之后的部分。
> 所以**必须先 `strip_prefix("Bearer ")`**,再判 `rhk_` 前缀 ——
> 否则 `starts_with("rhk_")` 对 `"Bearer rhk_..."` **永远为 false**,
> 全部 API Key 请求会掉进 JWT 分支并 401。
> (这个坑和第 11 轮的哈希口径问题同属「跨包约定没对齐」。)

```rust
// ★ 1. 先剥 "Bearer " —— 与现有 require_auth 的第一段完全一致
let Some(raw) = bearer.as_deref() else {
    return Err(unauthorized());
};
let Some(raw) = raw.strip_prefix("Bearer ") else {
    return Err(unauthorized());
};
if raw.is_empty() {
    return Err(unauthorized());
}

// ★ 2. 再看是不是 API Key
if raw.starts_with(apikey::TAG) {
    return verify_api_key(state, raw).await;   // 传剥过的 raw
}

// ★ 3. 否则走 JWT(原 require_auth 的剩余逻辑,一行不改)
let secret = state.jwt_secret().await;
let engine = jwt_engine(secret);
...
Ok(Principal::User(user))     // 原 Ok(user)
```

**`verify_api_key` 收到的 `bearer` 是剥过 `Bearer ` 的**(形如 `rhk_xxx`),
与 B-1 `parse()` 期望的输入一致。

**这是本方案的核心约束**:`resolve_principal` 是唯一的凭据解码入口,
`require_auth` / 新中间件都只能调它,不允许出现第二份实现(两份必然漂移)。

#### D-4 [P0] `verify_api_key` 校验顺序

签名(与 D-3 的调用点对齐):
```rust
/// bearer 是**已剥掉 `Bearer ` 前缀**的 `rhk_...` 串(由 D-3 处理后传入)。
/// ★ 不是完整的 Authorization 头值 —— 别在这里再 strip 一次。
async fn verify_api_key(state: &Arc<AppState>, bearer: &str)
    -> Result<Principal, Response>
```

每一步失败都回同一个 `unauthorized()`(**不泄露「不存在 / 已吊销 / 已过期」
的差异**):

1. `apikey::parse(bearer)` 失败 → 401
2. `state.apikey_cache` lookup(见 C-2)→ 无 → 401
3. `ct_eq(hash_hex(full), stored.key_hash)` → 不等 → 401
4. `!enabled` → 401
5. `expires_at` 已过 → 401
6. `state.apikey_touch(id)`(**不 await**)
7. `Ok(Principal::Key { .. })`

**第 5 步的精确写法(单位与边界必须明确,否则两个实现者会写出不同代码)**

```rust
// expires_at 单位是**毫秒**(与全项目 now_ms() 口径一致,见 C-4)
// None = 永不过期
if let Some(exp) = c.expires_at {
    if exp <= now_ms() {        // ★ 用 <=,不是 <
        return Err(unauthorized());
    }
}
```

- **单位**:毫秒。`db.rs::now_ms()` 返回 `i64` 毫秒,`clients.created_at` /
  `events.ts` / `certs.not_after` 全是毫秒
- **边界**:`exp <= now` 判过期。也就是「到期时刻本身即失效」。
  用 `<` 会让 key 在到期那一毫秒多活一次请求,虽然实际无差别,
  但**单测里会不稳定**(同一毫秒内可能通过也可能不通过)
- **`None` 表示永不过期**,不是"立即过期" —— 别写成
  `expires_at.unwrap_or(0) <= now`(那会把永不过期当成已过期)

**`touch` 的时机与一个已知语义副作用**

D-4 的第 6 步(`apikey_touch`)在 `verify_api_key` 内部执行,而 `auth_mw`
的 `session_only` / scope 判断在 `resolve_principal` **返回之后**才做。
所以存在这个顺序:

```
Key 请求 session-only 接口
  → resolve_principal → verify_api_key → ... → touch 已写 last_used_at
  → auth_mw 判 session_only → 403
```

**后果**:被拒的请求也会更新 `last_used_at`,控制台会显示「该 key 最近使用过」,
尽管它一次都没成功。**这是可接受的**,理由:

1. `last_used_at` 的用途是「提醒管理员这把 key 是否还在被使用/该不该吊销」,
   被拒的尝试同样是"有人还在拿它试"的信号,保留反而更有信息量
2. 若要把 touch 移到鉴权全部通过之后,就得把 `Principal::Key{id}` 从
   `verify_api_key` 传回中间件、等 scope 判定完再 touch —— 多一条数据流,
   收益仅仅是"仪表盘数字更严格",不值
3. 若将来确实需要精确的"成功调用次数",应该加**独立的计数器指标**
   (Prometheus counter),而不是复用 `last_used_at`

**但要注意**:`touch` 有 60s 节流(C-4),所以被拒请求不会放大 DB 写入。

**本期不做 per-IP 失败限速**。prefix 空间 2³² + secret 256 位不可枚举;
而 `login` 要限速是因为 argon2 太贵(`api.rs:141-143` 已记录),这里没有该成本。
硬要加也可以(中间件能拿到 `ConnectInfo`),但不在本期范围。

- [x] 验收:单测覆盖**全部 5 个失败分支**(第 6/7 步不是分支,不需要单独的失败用例):【已验,api.rs::apikey_tests::verify_api_key_five_rejections】
      5 分支(形态错/查无/哈希不等/enabled=0/已过期)逐条断言 HTTP 401 且 body
      message 统一 "unauthorized"(不泄露原因);另含成功用例断言 Principal::Key 三字段。

  | # | 分支 | 构造方式 | 期望 |
  |---|---|---|---|
  | 1 | `parse` 形态错误 | 传 `rhk_xxx`(非 8+64 hex) | 401 |
  | 2 | lookup 查不到 | 库里无该 prefix | 401 |
  | 3 | hash 不匹配 | 用错误的 secret | 401 |
  | 4 | `enabled = false` | 建 key 后 `set_api_key_enabled(id,false)` | 401 |
  | 5 | `expires_at` 已过 | `expires_at = now-1` | 401 |

  另需 1 条**成功用例**(正常 key → `Ok(Principal::Key{..})`),验证第 6 步
  `touch` 被调用且不影响返回。

---

### 包 E:中间件 + 路由重排 + handler 瘦身(**最高风险,独立提交**)

> 本包一次改动 30+ 个函数,必须单独提交、单独跑全量测试,
> 不要和包 F/G 混在一起(AGENTS.md:不把多类改动混一个大提交)。

#### E-1 [P0] 中间件的写法 —— `from_fn_with_state` 可用,**真正的坑在别处**

> ⚠️ **本节结论在 2026-10-28 被推翻并重做。**
> 早先版本声称「`from_fn_with_state` 无法编译,必须改用闭包捕获」,
> **那是误判** —— 当时把 `E0277: FromFn<...>: Service<Request> is not satisfied`
> 归因成了 `State<T>` 与 Router 的 `S` 类型不匹配,实际根因是
> `async fn resolve(&Request)` 让 future 不 `Send`(见下)。已用 8 组对照实验重新确认。

**实测结论(axum 0.8.9,2026-10-28)**

下面这些写法**全部编译通过**,`from_fn_with_state` 可以正常用:

| 形态 | 结果 |
|---|---|
| `Router::new().route(..).route_layer(from_fn_with_state(cfg, mw)).with_state(s)` | ✅ |
| 同上去掉 `with_state`(先 let 绑定再 `with_state`) | ✅ |
| 多组各自 `route_layer` 后 `merge`,最后统一 `with_state` | ✅ |
| 中间件提取 `State<AuthCfg>` 且 `AuthCfg` 内含 `Arc<AppState>` | ✅ |
| 中间件同时提取 `State<AuthCfg>` + `Extension<T>` | ✅ |
| 中间件里 `req.extensions_mut().insert(principal)` + handler 用 `Extension<T>` 取 | ✅ |

所以 **`State<AuthCfg>` 与 Router 的 `S` 不必相同**,这条早先的论断是错的。

**真正的坑:`async fn` 参数里出现 `&Request` 会让 future 不 `Send`**

`Request<Body>` 不是 `Sync`,所以 `&Request` 不能跨 `.await`。一旦跨了,
`from_fn`/`from_fn_with_state` 生成的 future 不满足 `Send`,而 axum 报出的
**表面错误是 E0277「FromFn: Service<Request> is not satisfied」**——
完全看不出是 `Send` 问题,极易误诊。

对照实验(其余代码完全相同,只改 `resolve` 的签名):

```rust
// ✗ 编译失败(E0277,但根因是 future 不 Send)
async fn resolve(req: &Request) -> Result<Principal, Response> {
    let _ = req.headers();
    Ok(..)
}
async fn mw(mut req: Request, next: Next) -> Response {
    let _ = resolve(&req).await;      // ← &Request 跨 await
    next.run(req).await
}

// ✓ 编译通过(header 值先取出来,不持有 &Request)
fn resolve(header: Option<&str>) -> Result<Principal, Response> { .. }
async fn mw(mut req: Request, next: Next) -> Response {
    let hv = req.headers().get("authorization").and_then(|v| v.to_str().ok());
    let p = match resolve(hv) { .. };  // ← 同步调用,不跨 await
    req.extensions_mut().insert(p);
    next.run(req).await
}
```

**如果 `verify_api_key` 必须是 `async`**(查缓存/查库),那么:

- **不要**把 `&Request` / `&HeaderMap` 传进去
- 先在中间件里把 header 值 `to_owned()` 成 `String`,再传给 async 函数
- 或者把 `Arc<AppState>`(它是 `Send + Sync`)和 `String` 一起传

```rust
// ✓ async 版本的正确形态
async fn resolve_principal(
    state: &Arc<AppState>,
    bearer: Option<String>,     // ← owned,不借用 req
) -> Result<Principal, Response>;

// 中间件里:
let bearer = req.headers().get("authorization")
    .and_then(|v| v.to_str().ok()).map(str::to_owned);
let p = match resolve_principal(&cfg.state, bearer).await { .. };
```

**推荐写法(两种都行,选一种并保持一致)**

方式 A —— `from_fn_with_state` + `State<AuthCfg>`(配置与 state 一起传,最直观):

```rust
pub fn router(state: Arc<AppState>) -> Router {
    // AuthCfg 持有 Arc<AppState>,便于中间件内调用 resolve_principal
    let cfg = |need: u32, session_only: bool| AuthCfg {
        state: state.clone(), need, session_only,
    };

    let clients = Router::new()
        .route("/api/clients", get(list_clients).post(create_client))
        .route_layer(middleware::from_fn_with_state(
            cfg(scope::CLIENTS_R | scope::CLIENTS_W, false), auth_mw))
        .with_state(state.clone());

    // ... 其余组同构,每组自己 with_state
    public.merge(clients)/* ... */
}

async fn auth_mw(
    State(cfg): State<AuthCfg>,
    mut req: Request,
    next: Next,
) -> Response {
    // ★ 关键:header 值先 owned 化,不把 &req 交给 async 函数
    let bearer = req.headers().get("authorization")
        .and_then(|v| v.to_str().ok()).map(str::to_owned);
    let p = match resolve_principal(&cfg.state, bearer).await {
        Ok(p) => p, Err(r) => return r,
    };
    if cfg.session_only && matches!(&p, Principal::Key { .. }) {
        return forbidden("该接口仅限控制台会话");
    }
    if let Principal::Key { scopes, .. } = &p {
        let dir = if matches!(*req.method(), Method::GET | Method::HEAD) {
            scope::READ_MASK
        } else {
            scope::WRITE_MASK
        };
        let allowed = cfg.need & dir;
        if allowed == 0 || scopes & allowed == 0 {
            return forbidden("API Key 权限不足");
        }
    }
    req.extensions_mut().insert(p);
    next.run(req).await
}
```

方式 B —— 闭包捕获配置 + `from_fn`(签名里没有 `State`,可用于跨 Router 复用):

```rust
/// ★ 三个参数:`state` / `need` / `session_only`。
///   `state` **必须传** —— `resolve_principal` 要它来查缓存与 JWT 密钥。
///   早期版本这里只写了 `guard(need, session_only)` 并在函数体里留了
///   `/* state 需另行获取 */` 的占位注释,但 E-3 骨架是直接 `guard(0, true)`
///   调用 —— 拿不到 `Arc<AppState>` 就无法调 `resolve_principal`,**编译不过**。
///   现统一为三参数形态。
fn guard(
    state: Arc<AppState>,
    need: u32,
    session_only: bool,
) -> impl Fn(Request, Next)
         -> std::pin::Pin<Box<dyn std::future::Future<Output = Response> + Send>>
       + Clone + Send + Sync + 'static
{
    move |mut req: Request, next: Next| {
        let state = state.clone();          // 每次请求克隆一次 Arc(仅引用计数)
        Box::pin(async move {
            // `Box::pin(.. + Send)` 显式标注,让「误把 &Request 跨 await」
            // 在出错那一行就报出来,而不是变成 E0277 FromFn 的天书错误。
            let bearer = req.headers().get("authorization")
                .and_then(|v| v.to_str().ok()).map(str::to_owned);   // ★ owned 化
            let p = match resolve_principal(&state, bearer).await {
                Ok(p) => p,
                Err(r) => return r,
            };
            if session_only && matches!(&p, Principal::Key { .. }) {
                return forbidden("该接口仅限控制台会话");
            }
            if let Principal::Key { scopes, .. } = &p {
                let dir = if matches!(*req.method(), Method::GET | Method::HEAD) {
                    scope::READ_MASK
                } else {
                    scope::WRITE_MASK
                };
                let allowed = need & dir;
                if allowed == 0 || scopes & allowed == 0 {
                    return forbidden("API Key 权限不足");
                }
            }
            req.extensions_mut().insert(p);
            next.run(req).await
        })
    }
}
```

**E-3 骨架里的调用形态**(与上面三参数签名一致):

```rust
.route_layer(middleware::from_fn(guard(state.clone(), 0, true)));
.route_layer(middleware::from_fn(guard(
    state.clone(), scope::CLIENTS_R | scope::CLIENTS_W, false)));
```

**两种方式的选择 —— 本任务书统一采用方式 B**

> ⚠ 早期版本这里写「推荐方式 A」,但 **E-3 的完整骨架实际用的是方式 B**
> (`guard(need, session_only)` + `from_fn`)。两处矛盾会让实现者不知道该照哪个。
> **以 E-3 为准:用方式 B。**

| | 方式 A(`from_fn_with_state` + `State<AuthCfg>`) | **方式 B(闭包捕获 + `from_fn`)** ← 采用 |
|---|---|---|
| 配置传递 | 走 axum `State` | 闭包捕获 |
| 中间件签名 | `State(cfg), Request, Next` | `Request, Next` |
| 编译错误可读性 | 一般 | 更好(`Box::pin(.. + Send)` 显式) |
| 本任务书 | 仅作对照说明 | **E-3 / E-4 / E-5 / E-6 全部按此写** |

**为什么选 B**:方式 B 的 `Box::pin(async move { .. } + Send)` 显式标注了
`Send`,一旦有人误把 `&Request` 跨 `.await`,`rustc` 会在**那一行**报
"future not Send",而不是变成 `E0277 FromFn: Service` 这种天书 ——
正是第 15 轮踩过的坑。**不要混用**两种方式。

#### E-2 [P0] `resolve_principal` 的签名 —— **必须 `async`**

> ⚠ 本节早期版本写「同步即可,实际不需要异步」,**那是错的**,已删除。
> 根因:**API Key 分支要查缓存/查库**(C-2 的 `lookup_api_key` 是 `async`),
> 所以 `resolve_principal` 必须能 `.await`。

**唯一签名**(与 D-3 / E-1 方式 A 完全一致):

```rust
/// 唯一的凭据解码入口。JWT 与 API Key 都走这里。
///
/// ★ `bearer` 是 **owned String**,不是 `&str`/`&HeaderMap`:
///   中间件里先 `to_owned()` 再传,避免 `&Request` 跨 `.await` 导致 future 不 `Send`
///   (见 §0 第 8 条 / E-1)。
async fn resolve_principal(
    state: &Arc<AppState>,
    bearer: Option<String>,
) -> Result<Principal, Response>
```

内部结构(与 D-3 相同):

```rust
let Some(bearer) = bearer else { return Err(unauthorized()) };
if bearer.starts_with(apikey::TAG) {
    return verify_api_key(state, &bearer).await;
}
// ↓ JWT 路径:整体平移 api.rs 的 require_auth,一行不改
let secret = state.jwt_secret().await;
...
Ok(Principal::User(user))
```

**中间件调用处**:

```rust
let bearer = req.headers().get("authorization")
    .and_then(|v| v.to_str().ok())
    .map(str::to_owned);                       // ← 先 owned 化
let p = match resolve_principal(&cfg.state, bearer).await { .. };
```

#### E-3 [P0] 9 组路由重排(**写法已实测定版,照抄**)

```rust
pub fn router(state: Arc<AppState>) -> Router {
    // 无保护的 public 组
    let public = Router::new()
        .route("/api/auth/login", post(login))
        .with_state(state.clone());

    // session-only 组:先 with_state,再 route_layer
    let session = Router::new()
        .route("/api/auth/logout", post(logout))
        .route("/api/auth/password", post(change_password))
        .route("/api/user/info", get(user_info))
        .route("/api/auth/codes", get(auth_codes))
        .route("/api/menu/all", get(menu_all))
        .with_state(state.clone())
        .route_layer(middleware::from_fn(guard(state.clone(), 0, true)));

    let clients = Router::new()
        .route("/api/clients", get(list_clients).post(create_client))
        .route("/api/clients/{id}", put(update_client).delete(delete_client))
        .route("/api/clients/{id}/rotate-secret", post(rotate_secret))
        .with_state(state.clone())
        .route_layer(middleware::from_fn(guard(state.clone(), 
            scope::CLIENTS_R | scope::CLIENTS_W, false)));

    // ---- 以下 6 组不要再用「同构 ...」省略,逐组写全(need 值各不相同)----

    let services = Router::new()
        .route("/api/services", get(list_services).post(create_service))
        .route("/api/services/batch", post(batch_services))
        .route("/api/services/{id}",
               get(get_service).put(update_service).delete(delete_service))
        .route("/api/services/{id}/enable", post(enable_service))
        .route("/api/services/{id}/disable", post(disable_service))
        .route("/api/services/{id}/rotate-secret", post(rotate_service_secret))
        .with_state(state.clone())
        .route_layer(middleware::from_fn(guard(state.clone(), 
            scope::SERVICES_R | scope::SERVICES_W, false)));

    let visitors = Router::new()
        .route("/api/visitors", get(list_visitors).post(create_visitor))
        .route("/api/visitors/{id}", put(update_visitor).delete(delete_visitor))
        .route("/api/visitors/{id}/enable", post(enable_visitor))
        .route("/api/visitors/{id}/disable", post(disable_visitor))
        .with_state(state.clone())
        .route_layer(middleware::from_fn(guard(state.clone(), 
            scope::VISITORS_R | scope::VISITORS_W, false)));

    let stats = Router::new()
        .route("/api/stats/overview", get(stats_overview))
        .with_state(state.clone())
        .route_layer(middleware::from_fn(guard(state.clone(), scope::STATS_R, false)));

    let events = Router::new()
        .route("/api/events", get(list_events))
        .with_state(state.clone())
        .route_layer(middleware::from_fn(guard(state.clone(), scope::EVENTS_R, false)));

    let config = Router::new()
        .route("/api/server/config", get(get_server_config).put(put_server_config))
        .with_state(state.clone())
        .route_layer(middleware::from_fn(guard(state.clone(), 
            scope::CONFIG_R | scope::CONFIG_W, false)));

    let certs = Router::new()
        .route("/api/certs", get(list_certs_api).post(create_cert_api))
        .route("/api/certs/{id}", put(update_cert_api).delete(delete_cert_api))
        .with_state(state.clone())
        .route_layer(middleware::from_fn(guard(state.clone(), 
            scope::CERTS_R | scope::CERTS_W, false)));

    // ★ 终点不再调 with_state —— 每组已各自 with_state 完成
    public
        .merge(session).merge(clients).merge(services).merge(visitors)
        .merge(stats).merge(events).merge(config).merge(certs)
}
```

**三条硬约束**:

1. **`.route(...)` 全部写完 → `.with_state(..)` → `.route_layer(...)`**。
   `route_layer` 只包住调用前已注册的路由(见 §1.3),顺序反了中间件不生效。
2. **每组各自 `.with_state(state.clone())`**,最终 `merge` 后**不再**调
   `.with_state()`。因为 `merge` 要求两侧都已是 `Router<()>`。
3. **`.with_state()` 之后不要再加 `.route()`**。已实测:
   `.with_state()` 把 `Router<Arc<AppState>>` 降成 `Router<()>`,
   之后加带 `State` 的 handler 会报
   **`E0308: expected MethodRouter, found MethodRouter<Arc<AppState>>`**。

关于 `with_state` 与 `route_layer` 的相对顺序:两者**都能编译**(已实测),
但**本任务书统一采用**:

```rust
.route(..)            // 1. 所有路由
.with_state(...)      // 2. 定下 State 类型
.route_layer(..)      // 3. 最后加中间件
```

理由:`route_layer` 只包住调用前已注册的路由,放在最后能确保
**不会有漏包的组**。E-3 骨架全部按这个顺序写,照抄即可。

新增 import(当前 `api.rs:6,7,9` 的实际状态):
```rust
use axum::extract::Extension;              // 新增(handler 取 Principal)
use axum::http::{Method, Request};         // 新增
use axum::middleware::{self, Next};        // 新增
use axum::routing::delete;                 // 新增(api.rs:9 只有 get/post/put)
// api.rs:7 已有 HeaderMap,保留(测试区仍大量使用)
```

**`session` 组在包 F 之后还要追加 4 条 api-keys 路由**(见 F-1),
必须插在该组 `.with_state(..)` **之前**、`.route_layer(..)` **之前**。

> **已验证**:本节写法在 axum 0.8.9 上编译通过,并通过 12 条判权用例
> (无 token 401 / 只读 200 / 只读写 403 / `scopes=0` 403 / OPS 写 config 403 /
> login 匿名 200 / 未知路径 404 等)。若你的写法报 `E0277 ... FromFn ...
> Service<Request> is not satisfied`,**先查是不是把 `&Request` 传进了
> async 函数**(不是先怀疑 `from_fn_with_state`),详见 E-1。

#### E-4 [P0] 瘦身 30 个 handler

对每个 `.is_err()` 形态的 handler:

1. 删掉 `headers: HeaderMap` 参数
2. 删掉首行 `if require_auth(&state, &headers).await.is_err() { return unauthorized(); }`

```rust
// 改前
async fn list_services(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(q): Query<ListQuery>,
) -> Response {
    if require_auth(&state, &headers).await.is_err() {
        return unauthorized();
    }
    ...

// 改后
async fn list_services(
    State(state): State<Arc<AppState>>,
    Query(q): Query<ListQuery>,
) -> Response {
    ...
```

**注意**:已核实全部 30 个 handler 的 `headers` 只出现 2 次(参数声明 + 鉴权调用),
**没有额外用途**,删参数是安全的。

#### E-4b [P0] 同步修复现有测试(**任务书上一版遗漏,不修则 `cargo test` 编译失败**)

删参数会打断**测试区里对 handler 的直接调用**。已精确扫描确认影响面:

| 被测试直接调用的函数 | 次数 | 删 `headers` 参数后是否受影响 |
|---|---|---|
| `create_client` | 19 | ✅ 要改 |
| `mint_jwt` | 10 | ❌ 不涉及 headers |
| `resolve_visitor_target` | 6 | ❌ 不涉及 |
| `create_service_inner` | 5 | ❌ 不涉及(内部函数) |
| `require_auth` | 4 | ⚠️ 函数本身被移除/替换,需迁移测试 |
| `get_service` | 3 | ✅ 要改 |
| `put_server_config` | 3 | ✅ 要改 |
| `update_client` | 3 | ✅ 要改 |
| `update_service_inner` | 3 | ❌ 不涉及 |
| `batch_services` | 2 | ✅ 要改 |
| `delete_client` | 2 | ✅ 要改 |
| `disable_service_inner` | 2 | ❌ 不涉及 |
| `create_service` / `delete_service_inner` / `disable_service` / `enable_service` | 各 1 | 前者要改,`*_inner` 不涉及 |
| `get_server_config` / `list_clients` / `list_services` | 各 1 | ✅ 要改 |
| `rotate_service_secret` | 1 | ✅ 要改 |
| `update_service` | 1 | ✅ 要改 |
| **合计** | **71 处调用点,21 个函数** | 其中 **40 处**(14 个函数)需同步改 |

> ⚠ 早期版本写「约 60 处」—— 那是估算。**实测是 71 处**,但其中
> `mint_jwt`(10)/`resolve_visitor_target`(6)/`create_service_inner`(5)/
> `update_service_inner`(3)/`disable_service_inner`(2)共 **26 处不涉及
> `headers`**,改起来只是删掉多余的 `h` 实参或根本不用动。
> 真正要动的是「签名里有 `headers: HeaderMap` 且被测试调用」的
> **14 个函数、40 处调用点**(已逐个扫出,非估算):
> `create_client`(19)/`update_client`(3)/`put_server_config`(3)/
> `get_service`(3)/`delete_client`(2)/`batch_services`(2)/
> `list_services`/`enable_service`/`get_server_config`/`rotate_service_secret`/
> `create_service`/`update_service`/`disable_service`/`list_clients` 各 1。

调用形态(见 `mod tests` 里的 `create` / `update` / `enable` / `disable` 四个 helper):

```rust
// 需同步改:删掉中间那个 HeaderMap 实参
body_json(create_service(State(state.clone()), h, Json(s)).await).await
//                                    ↑ 删这个
```

**处理方式**:

1. 先 `grep -n "auth_headers" crates/rh-server/src/api.rs`,三处测试模块各有
   一个 `auth_headers` 辅助函数(3190 / 4494 / 5114 行附近),删参数后
   它只在这些调用点使用,**一并删除该辅助函数**(否则 `dead_code` 触发
   `clippy -D warnings` 失败)。
2. 逐个删除调用点里的 `h` / `auth_headers(&state).await` 实参。
3. **`require_auth` 自身的测试**(`d6_require_auth_typed_claims_four_paths` 等
   4 处调用)不能简单删 —— `require_auth` 已被 `resolve_principal` 取代,
   这些用例要**迁移**为对 `resolve_principal` 的测试,不是删除。

> ⚠ 这一步工作量比看起来大(**见表内合计**,实测非估算)。**建议 E-4 与 E-4b 在同一次提交内完成**,
> 否则中间态编译不过。提交前必须 `cargo test --workspace` 真正跑过,
> 不能只看 `cargo build`(`build` 不编译 `#[cfg(test)]` 代码)。

#### E-5 [P0] 3 个 `match` 形态改用 Extension

`logout`、`change_password`、`user_info` 三个 handler(按**函数名**定位,
见 §1.1 的锚点表;不要用行号,代码在持续变动):

```rust
async fn logout(
    State(state): State<Arc<AppState>>,
    Extension(principal): Extension<Principal>,
) -> Response {
    let Principal::User(user) = principal else {
        return forbidden("该接口仅限控制台会话");   // 双保险,session_only 已挡
    };
    ...
```

`menu_all` 与 `auth_codes` 是 `is_err()` 形态,直接删掉首行判断即可
(session 组已保证已认证)。

> ⚠ `Extension<T>` 提取失败时 axum 返回 **500**,不是 401/403(实测确认)。
> 因此正确性依赖两件事同时成立:中间件**确实**执行了,且**确实**插入了
> `Extension<Principal>`。`route_layer` 顺序写错 → 中间件不执行 →
> `Extension` 缺失 → **handler 全部 500**。这是 §E-6 验收里必须
> 真的发一次请求验证的原因,光 `cargo build` 发现不了。

#### E-6 [P0] 删除死代码

`require_auth` 若已无**非测试**调用点则删除,但注意:

1. `unauthorized()` 仍被 `verify_api_key` / `resolve_principal` 使用,**保留**。
2. `require_auth` 目前有 **4 处测试调用**(`d6_require_auth_typed_claims_four_paths`
   等)。函数被删除或改名后这些测试会编译失败 —— 按 **E-4b** 的要求迁移到
   `resolve_principal`,不要直接删测试。
3. 若暂时保留 `require_auth` 作为 `resolve_principal` 的薄包装(仅返回
   `Principal::User`),测试可零改动;但那样会出现第二份解码路径,
   违反 §D-3 的「唯一入口」约束。**宁可改测试,不要留双路径。**

- [x] 验收:【已验 f25c930,逐条实测】
      - `grep -c "require_auth(&state, &headers)" crates/rh-server/src/api.rs` = **0**
      - `cargo build` **和 `cargo test --workspace` 都通过**
        (`build` 不编译 `#[cfg(test)]`,必须真跑 test)
      - 三道门全绿
      - 现有 e2e(`tests/e2e.sh`)全绿 —— 证明 JWT 路径行为未变
      - 三条状态码与 §6.1 基线**逐条一致**(`clients=401`、`unknown api=404`、`root=200`)
      - 手工发一次带 token 的 `/api/clients` 得 **200**(证明 `Extension` 已注入,
        不是 500)

> 注意:这三条与基线一致**并不能**单独证明用对了 `route_layer`
> (当前顺序下 `layer` 也是同样结果,见 §1.3 实测矩阵)。
> 它们的价值是**防止劣化**;而「用对 `route_layer`」靠代码审查
> —— 在 `api.rs` 里搜 `route_layer`,不应出现任何 `.layer(`。

---

### 包 F:API Key 管理接口

#### F-1 [P0] 3 条路由,加进 **session 组**

```rust
.route("/api/api-keys", get(list_api_keys).post(create_api_key))
.route("/api/api-keys/{id}", delete(delete_api_key))
.route("/api/api-keys/{id}/rotate", post(rotate_api_key))
```

**必须插在 session 组的 `.route(...)` 序列里,即 `.with_state(..)` 之前、
`.route_layer(...)` 之前**:

```rust
let session = Router::new()
    .route("/api/auth/logout", post(logout))
    .route("/api/auth/password", post(change_password))
    .route("/api/user/info", get(user_info))
    .route("/api/auth/codes", get(auth_codes))
    .route("/api/menu/all", get(menu_all))
    // ↓ 包 F 新增,插在这里
    .route("/api/api-keys", get(list_api_keys).post(create_api_key))
    .route("/api/api-keys/{id}", delete(delete_api_key))
    .route("/api/api-keys/{id}/rotate", post(rotate_api_key))
    // ↓ 然后才是 with_state / route_layer
    .with_state(state.clone())
    .route_layer(middleware::from_fn(guard(state.clone(), 0, true)));
```

> ⚠ **早期版本写「插在 `.with_state(..)` 之后」是错的**,已实测:
> `.with_state()` 把 `Router<Arc<AppState>>` 降成 `Router<()>`,之后再加
> `.route("/x", get(handler_with_State))` 会报
> **`E0308: expected MethodRouter, found MethodRouter<Arc<AppState>>`**。
>
> 正确顺序恒为 **`route(..) → with_state(..) → route_layer(..)`**。

路径命名 `/api/api-keys` 与现有 `rotate-secret` 的连字符风格一致,无需改名。

#### F-2 [P0] 4 个 handler

**签名(全部 `async fn`,参数顺序遵循 axum:先 `State`,后提取器)**:

```rust
// 这四个都在 session 组(中间件保证已认证且是 JWT 用户),
// 所以内部不再需要自己调 require_auth —— 但仍可取 Principal 做审计。
async fn list_api_keys(State(state): State<Arc<AppState>>) -> Response;

async fn create_api_key(
    State(state): State<Arc<AppState>>,
    Extension(principal): Extension<Principal>,   // 取操作者做审计
    Json(req): Json<CreateApiKeyReq>,
) -> Response;

async fn rotate_api_key(
    State(state): State<Arc<AppState>>,
    Extension(principal): Extension<Principal>,
    Path(id): Path<String>,
) -> Response;

async fn delete_api_key(
    State(state): State<Arc<AppState>>,
    Extension(principal): Extension<Principal>,
    Path(id): Path<String>,
) -> Response;
```

请求体结构体:

```rust
#[derive(Deserialize)]
struct CreateApiKeyReq {
    name: String,
    #[serde(default)]
    preset: Option<String>,          // "readonly" | "ops" | "full"
    #[serde(default)]
    scopes: Option<u32>,             // 与 preset 二选一,同时给以 scopes 为准
    #[serde(default)]
    expires_in_days: Option<i64>,    // None = 永不过期
}
```

**`expires_in_days` → `expires_at` 的转换(必写,否则两端单位对不上)**

```rust
// expires_in_days: Option<i64>,单位「天」;expires_at 单位「毫秒」
const MS_PER_DAY: i64 = 24 * 60 * 60 * 1000;   // 86_400_000

let expires_at = match req.expires_in_days {
    None => None,                              // 永不过期
    Some(d) => {
        if !(1..=3650).contains(&d) {          // 1 天 ~ 10 年
            return err(1002, "expires_in_days 须为 1-3650(0 或不传表示永不过期)");
        }
        // ★ 用 checked_mul 防溢出(i64::MAX / 86_400_000 ≈ 1.07e11 天,
        //   虽然 3650 已上界,但显式 checked 更稳)
        Some(now_ms() + d.checked_mul(MS_PER_DAY).unwrap_or(i64::MAX))
    }
};
```

要点:
| 项 | 规定 |
|---|---|
| 单位 | 入参「天」,落库「毫秒」 |
| 范围 | `1..=3650`(1 天到 10 年);越界回 `err(1002, ...)` |
| 传 `0` | **视为非法**(报 1002),不要解释成"永不过期" |
| 不传 / `null` | 永不过期,`expires_at = None`(DB 存 NULL) |
| 溢出 | `checked_mul`,越界钳到 `i64::MAX`(等价于永不过期) |

| handler | 要点 |
|---|---|
| `list_api_keys` | **绝不返回 key_hash 或明文**;返回裸数组 `ok(out)`(与 `list_certs_api` 同风格) |
| `create_api_key` | 校验 name 1-64、scopes ⊆ FULL、`expires_in_days` 合理范围;生成 → 落库 → `invalidate_apikeys()` → 返回 `{key: 明文, ...}` **仅此一次** |
| `rotate_api_key` | 重新生成 → 落库 → `invalidate_apikeys()` → 返回新明文 |
| `delete_api_key` | 软删(`enabled=0`)、`invalidate_apikeys()`、保留审计痕迹 |

**四个接口的响应体(逐字照抄,已端到端验证)**

```rust
// GET /api/api-keys —— 裸数组,与 list_certs_api 同风格
ok(vec![json!({
    "id": r.id, "name": r.name, "prefix": r.prefix,
    "scopes": r.scopes, "enabled": r.enabled,
    "created_at": r.created_at, "created_by": r.created_by,
    "last_used_at": r.last_used_at, "expires_at": r.expires_at,
})])
// ★ 不含 key_hash、不含明文;但 **created_by 要返回**(见下)

// POST /api/api-keys —— 明文仅此一次
ok(json!({
    "id": id, "name": name, "prefix": prefix,
    "scopes": scopes, "key": plaintext,      // ← 仅此一次
}))

// POST /api/api-keys/{id}/rotate —— 同样仅此一次
ok(json!({ "id": id, "prefix": new_prefix, "key": new_plaintext }))

// DELETE /api/api-keys/{id} —— 与项目其余 4 个 DELETE 一致
ok(json!({ "deleted": true }))
```

**错误码**:
| 情况 | 返回 |
|---|---|
| 名称非法(空 / >64) | `err(1002, "名称长度须为 1-64 字符")` |
| `scopes` 超出 `FULL`(即 `scopes & !FULL != 0`) | `err(1002, "scopes 超出范围")` |
| 未知 `preset` | `err(1002, "未知的 preset")` |
| `preset` 与 `scopes` 都没给 | `err(1002, "需要 preset 或 scopes")` |
| `{id}` 不存在 | `err(1004, "密钥不存在")` |

> **`created_by` 必须出现在 list 响应里**。该字段在 DDL 里存在、`create_api_key`
> 会写入(`Principal::User` 的 username),若列表不返回就是**只写不读的死字段**。
> 前端设置页的密钥列表要显示「创建者」列 —— 多人共用一个控制台时,
> 这是判断「这把 key 该不该吊销」的主要依据。
>
> 注意:`clients` / `services` / `certs` 表都没有 created_by(项目此前是单管理员
> 假设),`api_keys` 是第一个引入的。**既然引入就要用**。

> 注意这些都是**业务错误**,走 `err()` → HTTP 200 + body `code!=0`
> (与项目现状一致);只有**鉴权/授权失败**才用 `unauthorized()`/`forbidden()`
> 返回真实 401/403(见 §2.5)。

**`created_by` 怎么取**(衔接点,早期版本没写):

```rust
// session 组已保证只有 JWT 用户能到这里,session_only 已经挡掉了 Key。
let Principal::User(operator) = principal else {
    // 理论上不可达:session_only 中间件已拦。保留双保险。
    return forbidden("该接口仅限控制台会话");
};
// operator 是 JWT 的 sub = username,直接写进 created_by
state.db.create_api_key(&id, &name, &prefix, &hash, scopes, &operator, expires_at).await
```

> ⚠ `Extension<Principal>(principal)` 在 F 组里**只会是 `Principal::User`**
> (session 组 `session_only=true` 已挡掉 `Key`)。但**仍要写 `let ... else`
> 兜底**,因为:
> 1. 若将来 session 组的 `session_only` 被误改成 false,不至于让 Key 冒充用户写库
> 2. 避免 `unreachable!()` 在线上 panic

**`rotate_api_key` 必须 bump 缓存**(否则旧 key 继续可用):

`rotate` 做的是 `UPDATE api_keys SET prefix=新, key_hash=新 WHERE id=?`。
库里旧 prefix 行没了,但**内存缓存里旧 prefix 的条目还在**(键是 prefix)。
若不 `invalidate_apikeys()`:

- 旧 key 请求 → `parse` 出旧 prefix → **缓存命中**(epoch 未变)→
  hash 比对上(旧 hash 没变)→ **旧 key 继续有效** ✗

所以三个写操作(**创建/rotate/吊销**)都必须调 `invalidate_apikeys()`,
一个都不能漏。这一条的验证方式见 H-1 的 `apikey_rotate_old_key_rejected`。

请求体:`{name, preset?: "readonly"|"ops"|"full", scopes?: number, expires_in_days?: number}`。

> ⚠ **`scopes` 是 `u32` 位掩码(整数),不是字符串数组**。
> 早期版本在这里写成 `number[]`,与 F-2 的结构体定义 `scopes: Option<u32>`
> 矛盾。统一按 **`u32`**:
> - 传 `scopes: 5461` 等价于 `READONLY`
> - 传 `["clients:r"]` 这种数组形式 **不支持**,会反序列化失败
>
> 选整数而非数组的理由:与判权逻辑天然对齐(直接 `scopes & allowed`),
> 无解析开销、无字符串映射表、DB 存一列 `INTEGER` 即可。

`preset` 与 `scopes` 二选一,**同时给以 `scopes` 为准**。映射:

| preset | 值 | 含义 |
|---|---|---|
| `"readonly"` | `scope::READONLY`(5461) | 全部读权限 |
| `"ops"` | `scope::OPS`(5503) | 只读 + clients/services/visitors 三类写 |
| `"full"` | `scope::FULL`(16383) | 全部权限 |

未知 preset 字符串 → `err(1002, "未知的 preset")`;`scopes` 超出 `FULL`
(即 `scopes & !FULL != 0`)→ `err(1002, "scopes 超出范围")`。

> ⚠ **字段名必须 snake_case**。已核实本项目**全线 snake_case**,无一处例外:
> - 响应:`ClientListRow` 的 `created_at` / `last_online_at` / `client_version`
> - 请求体:`LoginReq{username,password}`、`ChangePasswordReq{old_password,new_password}`
> - 前端调用处也照样传 snake:`httpPost('/auth/password',{new_password,old_password})`
> - 全仓仅一处 `serde(rename_all)`(`state.rs` 的 `ProxyState` 枚举),
>   值也是 **`"snake_case"`** —— 反而印证了这个约定
>
> 不要写 `expiresInDays` / `lastUsedAt` / `createdAt`。只有
> `POST /api/auth/login` 的 `accessToken` 是历史遗留的 camelCase
> (前端 `auth.ts` 按此解构),**不要模仿它**。

响应信封与现有接口一致(`ok(...)`),时间字段用毫秒整数,与
`clients.created_at` 口径一致(注意 `users.created_at` / `clients.created_at`
都是毫秒;而 `events.ts` 也是毫秒 —— 全项目一致)。

响应字段建议(全 snake_case):
```json
{
  "id": "uuid", "name": "ci", "prefix": "9f2c1a4b",
  "scopes": 5461, "enabled": true,
  "created_at": 0, "last_used_at": null, "expires_at": null
}
```

#### F-2b [P0] **不提供** `PUT /api/api-keys/{id}`(改 scope / 改名)

本期只有「创建 / 列出 / rotate / 吊销」四个操作,**没有原地修改**。

理由:
- 改 scope 本质是「收窄或放宽权限」,直接吊销重建语义更清晰,
  且不会有「旧 key 已经发出去、权限被悄悄改了」的歧义
- 少一个 handler、少一类审计事件、少一组测试
- 若真需要,补起来很便宜(加路由 + `UPDATE api_keys SET scopes=?`,
  再调一次 `invalidate_apikeys()`)

因此 `C-3` 里的失效点只有三个(创建/吊销/rotate),**不含改 scope**。

#### F-3 [P0] 审计:actor 塞 `detail`,**不改 events 表结构**

加一列要同时改 `insert_events` 四元组签名(`db.rs:838`)、`PendingEvent`
(`events.rs:27-34`)、`write_batch`(`events.rs:52-62`)、`emit_detail`
(`events.rs:132`)以及前端 `EventRow`(`rabbithole.ts:85`),全为一个审计字段,
不值。

**做法**:`detail` 写成 `{"actor":"key:ci-terraform","scopes":5}`。
`detail` 本就是自由文本、前端已展示。

事件内容:创建 / 吊销 / rotate 各一条,`source = "apikey"`,
`level` 分别 `info` / `warn` / `warn`。

- [x] 验收:curl 走通完整生命周期(每一步都写明**方法与期望状态码**):【已验 89bcdc1,实测 7/7】
      ①JWT 建只读 key→200(含明文+scopes=5461) ②key GET /api/clients→200
      ③key POST /api/clients→403 ④key POST logout→403
      ⑤key GET /api/nope→未被鉴权拦截(body code=404;HTTP 200 系既有 err 信封口径,
      见 §6.1 反证) ⑥JWT 吊销→{"deleted":true} ⑦旧 key→401。
      内容断言:响应含 key;列表无 key_hash/明文;另验 rotate 换明文+软删后列表可见。

  ```bash
  # 1. 用 JWT 建一把只读 key → 200,响应含明文 key
  # 2. 用该 key GET /api/clients           → 200(只读位够)
  # 3. 用该 key POST /api/clients          → 403(缺 CLIENTS_W)
  # 4. 用该 key POST /api/auth/logout      → 403(session-only)
  # 5. 用该 key GET /api/nope              → 404(不是 401)
  # 6. JWT 吊销该 key                      → 200 + {"deleted":true}
  # 7. 再用该 key GET /api/clients         → 401(已吊销)
  ```

  另外两条内容断言:
  - 第 1 步的响应**含** `key` 字段(明文,仅此一次)
  - `GET /api/api-keys` 的响应**不含** `key_hash`、也**不含** `key`

---

### 包 G:前端

#### G-1 [P1] API 客户端

`console/src/api/rabbithole.ts` 加 4 个函数(`listApiKeys` / `createApiKey` /
`rotateApiKey` / `deleteApiKey`)与 `ApiKeyRow` 类型;
写法照 `rabbithole.ts:197-216` 的 certs 那批;类型风格照 `EventRow`(`:85`)。

**类型字段全部 snake_case**(与 `ClientRow` / `EventRow` 一致,见 F-2):
```ts
export interface ApiKeyRow {
  id: string;
  name: string;
  prefix: string;
  scopes: number;
  enabled: boolean;
  created_at: number;
  created_by: string;
  last_used_at: null | number;
  expires_at: null | number;
}
```
`createApiKey` 请求体同样是 snake_case:`{ name, preset?, scopes?, expires_in_days? }`;
其中 `scopes` 是 **`u32` 位掩码**(见 F-2),前端从勾选项按位或累加得出。

#### G-2 [P1] query key

`console/src/lib/query-keys.ts` 加 `apiKeys: ['api-keys'] as const`。

#### G-3 [P1] 设置页加 Tab(4 处,缺一处 tab 打不开)

当前精确行号(2026-10-28 已核实)。**若与现状不符,按下面的文本锚点定位:**

- `SETTINGS_TABS` 常量:搜 `const SETTINGS_TABS`
- `PageHeader`:搜 `<PageHeader title="设置"`
- Tab 列表:搜 `<TabsTrigger value="password">`
- 卡片函数:搜 `function PasswordCard(`

```
922:  <Tabs value={tab} onValueChange={onTabChange}>
923:    <TabsList>
924:      <TabsTrigger value="server">服务器配置</TabsTrigger>
925:      <TabsTrigger value="metrics">监控指标</TabsTrigger>
926:      <TabsTrigger value="password">修改密码</TabsTrigger>     ← 在此后加第 4 个
927:    </TabsList>
928:    <TabsContent value="server">   <ServerConfigCard /> </TabsContent>
931:    <TabsContent value="metrics">  <MetricsCard />      </TabsContent>
934:    <TabsContent value="password"> <PasswordCard />     </TabsContent>   ← 在此后加第 4 个
937:  </Tabs>
```

四处改动:

1. `:902` `SETTINGS_TABS` 数组加 `'apikeys'`
   (`const SETTINGS_TABS = ['server','metrics','password','apikeys'] as const`)
2. `:926` 之后加 `<TabsTrigger value="apikeys">API 密钥</TabsTrigger>`
3. `:936` 之后加
   `<TabsContent value="apikeys"><ApiKeysCard /></TabsContent>`
4. 新增 `ApiKeysCard` 函数,放 `PasswordCard`(`:794`)之后,与现有卡同构
   (`<Card><CardHeader><CardTitle/><CardDescription/></CardHeader><CardContent>…`)

注意 `:921` 的 `PageHeader description` 现在写的是
「服务器配置、监控指标与修改密码」,**加了第 4 个 tab 后要同步更新文案**。

列表列:名称 / 前缀(`rhk_9f2c1a4b…`)/ 权限徽标 / **创建者** / 最近使用 / 过期 / 状态 / 操作。
创建对话框:名称 + 预设 + 自定义勾选 + 有效期。

**权限徽标需要前端自己把 `scopes`(位掩码)翻译成文字** ——
后端只返回整数,不返回标签。在 `console/src/features/` 下加一个小模块
(与 `features/services/matrix.ts` 同层级):

```ts
/** 与后端 scope::* 常量逐位对齐(见任务书 §2.3)。改动必须两边同步。 */
export const SCOPE_BITS = {
  clients_r:  1 << 0,
  clients_w:  1 << 1,
  services_r: 1 << 2,
  services_w: 1 << 3,
  visitors_r: 1 << 4,
  visitors_w: 1 << 5,
  stats_r:    1 << 6,
  events_r:   1 << 8,
  config_r:   1 << 10,
  config_w:   1 << 11,
  certs_r:    1 << 12,
  certs_w:    1 << 13,
} as const;

export const READONLY = 0x1555;
export const OPS      = 0x157f;
export const FULL     = 0x3fff;

/** 徽标文字:命中预设就显示预设名,否则按位列出 */
export function scopeBadges(scopes: number): string[] {
  if (scopes === FULL)     return ['全部权限'];
  if (scopes === OPS)      return ['运维'];
  if (scopes === READONLY) return ['只读'];
  return Object.entries(SCOPE_BITS)
    .filter(([, bit]) => (scopes & bit) !== 0)
    .map(([name]) => name);
}
```

> ⚠ **前后端的位定义必须一致**。前端这份 `SCOPE_BITS` 是后端
> `pub mod scope` 的镜像,没有编译期保证 —— 改一边必须改另一边。

**必须配单测**(`console/src/features/api-keys/__tests__/scope.test.ts`,
照 `features/services/__tests__/spec.test.ts` 的风格:期望值全部写死,
后端常量一漂移就失败):

```ts
import { describe, expect, it } from 'vitest';
import { FULL, OPS, READONLY, SCOPE_BITS, scopeBadges } from '../scope';

describe('scope 常量与后端对齐', () => {
  // ★ 这三个数必须与后端 state/scope 模块逐字相同。
  //   写死具体值(不用 READ_MASK|WRITE_MASK 表达式),
  //   这样后端改了位定义、前端漏改时会红。
  it('三个预设值钉死', () => {
    expect(READONLY).toBe(5461);
    expect(OPS).toBe(5503);
    expect(FULL).toBe(16383);
  });

  it('每个位常量等于 1<<n', () => {
    expect(SCOPE_BITS.clients_r).toBe(1);
    expect(SCOPE_BITS.clients_w).toBe(2);
    expect(SCOPE_BITS.services_r).toBe(4);
    expect(SCOPE_BITS.services_w).toBe(8);
    expect(SCOPE_BITS.visitors_r).toBe(16);
    expect(SCOPE_BITS.visitors_w).toBe(32);
    expect(SCOPE_BITS.stats_r).toBe(64);
    expect(SCOPE_BITS.events_r).toBe(256);   // 位 8,不是 128
    expect(SCOPE_BITS.config_r).toBe(1024);
    expect(SCOPE_BITS.config_w).toBe(2048);
    expect(SCOPE_BITS.certs_r).toBe(4096);
    expect(SCOPE_BITS.certs_w).toBe(8192);
  });

  it('scopeBadges 命中预设', () => {
    expect(scopeBadges(READONLY)).toEqual(['只读']);
    expect(scopeBadges(OPS)).toEqual(['运维']);
    expect(scopeBadges(FULL)).toEqual(['全部权限']);
  });

  it('scopeBadges 非预设按位展开', () => {
    expect(scopeBadges(0)).toEqual([]);
    expect(scopeBadges(1)).toEqual(['clients_r']);
    expect(scopeBadges(1 | 2)).toEqual(['clients_r', 'clients_w']);
  });
});
```
> 建议在 `scopeBadges` 上加注释指向后端文件,并在一处单测里钉住
> `READONLY/OPS/FULL` 三个数值(见 `features/services/__tests__/` 的现有做法)。

#### G-4 [P1] 一次性明文展示 —— **新写轻量弹窗,不要复用 `secret-modal.tsx`**

⚠️ 前面的任务书版本说「直接套 `secret-modal.tsx`,不要新写」是**错的**。
已核实该组件(`console/src/components/secret-modal.tsx`,297 行)是
**客户端接入凭据专用**,Props 为:

```ts
{ open, onOpenChange, clientId?: string, secret: string, title?: string }
```

内部硬编码了三档客户端接入模板,与 API Key 无关:
- `SCRIPT_URL` = `deploy/install-client.sh` 一键脚本(`--id --secret`)
- `docker run -e RH_CLIENT_ID=… -e RH_SECRET=…`
- `rh-client.toml` 模板折叠区

还有一个 `useState(() => `${window.location.hostname}:7100`)` 的
`serverAddr` 与 `protocol` / `tlsInsecure` 选择器。这些都塞不进 API Key 场景,
强行复用要么改坏该组件(影响新建客户端流程),要么把无关 UI 一起暴露给用户。

**做法**:在 `ApiKeysCard` 内新写一个 ~40 行的轻量弹窗,只用
`Dialog` + 只读展示 + 复制按钮。

**复制能力已经有现成的,直接用 `MonoCopy`**(`console/src/components/mono.tsx`):
```tsx
import { MonoCopy } from '@/components/mono';
<MonoCopy full={plaintext} className="w-full" />
```
它已内含 `navigator.clipboard` → `execCommand` 的纯 HTTP 降级、
复制成功/失败反馈、`aria-label`。**不要重写复制逻辑,也不要从
`secret-modal.tsx` 里搬 `copyToClipboard`**(那是模块私有函数,
搬出来等于制造第三份实现 —— 仓库里现在已经有
`secret-modal.tsx:41` 和 `mono.tsx:11` 两份了,不要再加)。

弹窗要点:
- 标题「API 密钥(仅此一次展示)」
- 明文只读展示 + 复制按钮
- 明确警示「关闭后无法再次查看,只能重新生成」
- 关闭即清空 state(不留在内存里)

#### G-5 [P1] **不要碰 `request.ts`**

`request.ts:63-69` 的 401 拦截器是给 JWT 用的;前端全程 JWT,行为不变。

- [x] 验收:`cd console && pnpm build` 通过。【已验 bad274c】
      tsc --noEmit + vite build 0 错;4 个 vitest(scope.test.ts)全绿;
      lint 既有错误(batch-import-dialog)在干净树上同样存在,与本次无关。

---

### 包 H:测试与文档

#### H-1 [P1] 单测

> 📍 **测试清单分散在三处**,别只看这一节:
>
> | 在哪 | 覆盖什么 | 数量 |
> |---|---|---|
> | **B-3** | `apikey.rs` 的纯函数:parse 形态 / ct_eq / generate 唯一性 | 4 |
> | **H-1(本节)** | 跨模块集成:scope 位 / 权限 / 缓存 / 校验 / 接口行为 | 17 |
> | **G-3** | 前端 `scopeBadges`(vitest,`console/src/features/api-keys/__tests__/`) | 4 |
>
> 合计 **25 个测试**(21 Rust + 4 前端)。
> `cargo test -p rh-server` 覆盖前两处;前端测试要 `cd console && pnpm test`。
>
> ⚠ **数量会随修正变化** —— 若你增删了测试,记得改这张表(它在 §1.0 之外的
> 第二处"多处重复的数字",容易漂移)。

| 用例 | 断言 |
|---|---|
| **`scope_bits_align_with_masks`** ★ | 对每个 `*_R` 常量断言 `C & READ_MASK == C`;每个 `*_W` 断言 `C & WRITE_MASK == C`。**这条能一次性抓住 §2.3 的位错位坑** |
| **`scope_zero_key_denied`** ★ | `scopes=0` 的 key 调任意受保护组(含只读组)必须 403 |
| `scope_method_mapping` | 遍历 (method, 期望位):GET→r、DELETE→w、`POST /enable`→w、`POST /batch`→w |
| `session_only_rejects_key` | session 组对 `Principal::Key` 回 **HTTP 403**(非 200+body) |
| `forbidden_uses_http_403` | `forbidden(..)` 的 `status()` 必须是 403(防误用 `err()`,见 §2.5) |
| `apikey_cache_epoch_invalidates` | rotate + invalidate 后旧 hash 失配 |
| **`apikey_rotate_old_key_rejected`** ★ | 完整链路:建 key → 用它请求得 200 → rotate → **用旧 key 请求必须 401**。这条能抓住"忘了 bump 缓存"的 bug(光测 `invalidate_apikeys` 本身抓不到) |
| `apikey_touch_throttles` | 1000 次调用只落 1 次库 |
| `parse_accepts_uppercase_but_hash_mismatch` | 大写形态能 `parse` 成功(形态合法),但 `hash_hex` 结果与建 key 时不同 → 校验必失败。钉住 B-1 的大小写语义 |
| **`verify_api_key_five_rejections`** ★ | 覆盖 D-4 的**全部 5 个失败分支**(parse 形态 / lookup 无 / hash 不等 / disabled / expired,构造方式见 D-4 的验收表),每条断言回 401 且 body 不泄露具体原因(不区分"不存在/已吊销/已过期") |
| `resolve_principal_jwt_and_key` | 照 `d6_require_auth_typed_claims_four_paths` 的构造方式(搜该函数名),覆盖 `resolve_principal` 的分流:JWT 路径 / API Key 路径 / 无 header / `Bearer ` 缺失 四种输入 |
| `unknown_api_path_still_404` | 未知 `/api/*` 返回 404 而非 401(防劣化;不单独证明 `route_layer` 用法,理由见 §1.3) |
| **`create_api_key_rejects_invalid`** ★ | 参数校验 5 条全覆盖,逐条断言 body 的 `code`:<br>① name 为空 / 65 字符 → `1002`<br>② `scopes = FULL + 1` → `1002`"scopes 超出范围"<br>③ `preset = "superuser"` → `1002`"未知的 preset"<br>④ `preset` 与 `scopes` 都不传 → `1002`"需要 preset 或 scopes"<br>⑤ `expires_in_days = 0` / `3651` / `-1` → `1002` |
| **`create_api_key_accepts_presets`** ★ | 三个 preset 分别建 key,断言落库 `scopes` 恰为 `READONLY(5461)` / `OPS(5503)` / `FULL(16383)`;并断言 `scopes` 优先于 `preset`(两者同时给时用 `scopes`) |
| **`rotate_returns_new_plaintext`** ★ | rotate 返回体含 `key` 且以 `rhk_` 开头、`prefix` 与旧的不同、`id` 不变 |
| **`delete_is_soft`** ★ | 吊销后:① 旧 key 请求 → 401;② `list_api_keys` **仍能看到该行**且 `enabled == false`;③ DB 里行还在(不是物理删除) |
| **`apikey_created_by_recorded`** ★ | 用 `Principal::User("alice")` 调 `create_api_key`,再 `list_api_keys`,断言该条的 `created_by == "alice"`。防止"字段加了但没写进去" |
| **`apikey_list_excludes_key_hash`** ★ | `list_api_keys` 响应的 JSON 里**不得出现** `key_hash` 字段,也不得出现明文 `key`。防止误把内部字段漏出去 |
| `handler_reachable_with_extension` | 直接构造带 `Extension<Principal>` 的请求调一个 handler,断言不是 500(**防 `route_layer` 顺序错导致全 500**) |

#### H-2 [P1] e2e(`tests/e2e.sh`,接在 `:298` 拿 token 之后)

**必须沿用文件既有约定**:
- 地址用 `http://127.0.0.1:$API_PORT`(`:150` 定义 `API_PORT=18080`),
  脚本里**没有** `$API` 变量,不要新造
- 断言用 `check $? "说明"`(`:65` 定义),不要自己写 `[ ... ]` 裸断言 ——
  那样失败时计数不会累加,整体退出码也不对
- 已有 `:298` 的 `api()` helper 自动带 `Authorization: Bearer $TOKEN`;
  API Key 的调用要单独写 curl

```bash
BASE_URL="http://127.0.0.1:$API_PORT"

# 1) 创建只读 API Key(明文仅此一次返回)
KRESP=$(api -X POST "$BASE_URL/api/api-keys" -H 'Content-Type: application/json' \
  -d '{"name":"e2e-ro","preset":"readonly"}')
KEY=$(echo "$KRESP" | python3 -c 'import json,sys;print(json.load(sys.stdin)["data"]["key"])' 2>/dev/null)
KID=$(echo "$KRESP" | python3 -c 'import json,sys;print(json.load(sys.stdin)["data"]["id"])' 2>/dev/null)
[ -n "$KEY" ] && [ -n "$KID" ]; check $? "创建 API Key 并取明文"

# 1b) 校验 created_by 与"不回显 key_hash"
echo "$KRESP" | python3 -c 'import json,sys; d=json.load(sys.stdin)["data"]; sys.exit(0 if d["key"].startswith("rhk_") else 1)'
check $? "创建响应含明文 key"
# 列表接口:有 created_by,无 key_hash
api "$BASE_URL/api/api-keys" | python3 -c '
import json,sys
d=json.load(sys.stdin)["data"]
assert d, "列表不应为空"
row=d[0]
assert "created_by" in row and row["created_by"], "created_by 缺失或为空"
assert "key_hash" not in row, "列表不得返回 key_hash"
assert "key" not in row, "列表不得返回明文"
'
check $? "列表含 created_by 且不含 key_hash/明文"

# 2) 只读 key 读 clients → 200
kc=$(curl -s -o /dev/null -w "%{http_code}" "$BASE_URL/api/clients" -H "Authorization: Bearer $KEY")
[ "$kc" = "200" ]; check $? "API Key 可读 clients(HTTP=$kc)"

# 3) 只读 key 写 clients → 403(真实 HTTP 状态,见 §2.5)
kc=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$BASE_URL/api/clients" \
  -H "Authorization: Bearer $KEY" -H 'Content-Type: application/json' -d '{"name":"nope"}')
[ "$kc" = "403" ]; check $? "只读 Key 写被拒 403(HTTP=$kc)"

# 4) key 调 session-only 接口 → 403
kc=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$BASE_URL/api/auth/logout" \
  -H "Authorization: Bearer $KEY")
[ "$kc" = "403" ]; check $? "API Key 不能调 logout(HTTP=$kc)"

# 5) 未知 /api/* 仍 404
kc=$(curl -s -o /dev/null -w "%{http_code}" "$BASE_URL/api/nope")
[ "$kc" = "404" ]; check $? "未知 API 路径仍 404(HTTP=$kc)"

# 6) ★ 改密码后 API Key 仍可用(JWT 已全失效,key 不受 pwd_version 影响)
#    注意:此步骤会让 $TOKEN 失效,必须放在本段最后,后续用新登录 token
# 6b) rotate 返回新明文,且旧 key 失效
RRESP=$(api -X POST "$BASE_URL/api/api-keys/$KID/rotate")
NEWKEY=$(echo "$RRESP" | python3 -c 'import json,sys;print(json.load(sys.stdin)["data"]["key"])' 2>/dev/null)
[ -n "$NEWKEY" ] && [ "$NEWKEY" != "$KEY" ]; check $? "rotate 返回新明文且与旧不同"

# 7) 吊销后 → 401
kr=$(api -X DELETE "$BASE_URL/api/api-keys/$KID")
# ★ 用 python 解析,不要用 grep 匹配 JSON 字面量:
#   serde_json 输出紧凑格式("deleted":true 无空格),但任何中间层
#   (代理/日志回显/mock)都可能插入空格,导致 grep 误判。
echo "$kr" | python3 -c 'import json,sys; sys.exit(0 if json.load(sys.stdin)["data"]["deleted"] else 1)'
check $? "吊销 API Key"
kc=$(curl -s -o /dev/null -w "%{http_code}" "$BASE_URL/api/clients" -H "Authorization: Bearer $KEY")
[ "$kc" = "401" ]; check $? "吊销后旧 Key 失效 401(HTTP=$kc)"
```

**四条注意**:

0. **JSON 断言一律用 `python3 -c` 解析,不要 `grep` 字面量**。
   本仓库 e2e.sh 既有代码也是这么做的(`| python3 -c 'import json,sys; ...'`)。
   实测:`axum::Json` 经 serde_json 输出的是紧凑格式(`"deleted":true` 无空格),
   但中间任何环节(反向代理/mock/日志回显)都可能加空格 —— 早先用
   `grep -q '"deleted":true'` 在一个带空格的 mock 上就误判成了失败。

1. **每条都断言具体状态码**,不能只看 body。§2.5 已确认 `err()` 返回
   HTTP 200;若实现误用 `err(403,…)`,body 里 `code` 仍是 403 但状态码是 200 ——
   "看 body 通过、实际协议错误"正是要防的。
2. **`delete_api_key` 必须返回 `ok(json!({"deleted": true}))`**。
   已核实项目里**全部 4 个 DELETE 接口**都这么写:
   `delete_client` / `delete_service` / `delete_visitor` / `delete_cert_api`
   无一例外。不要改成 `204`,也不要学 `logout`(它是 POST,语义不同)。
3. **第 6 条(改密码后 key 仍可用)会作废 `$TOKEN`**,必须放在本段末尾,
   且之后若还要用管理面需重新登录。这条是「API Key 独立于 `pwd_version`」
   这个设计承诺的唯一可执行证明,不可省略。

#### H-3 [P2] 文档

- **`docs/design.md`**(注意:以下 § 号是**那个文档**的,不是本任务书的)
  - **§7.2「业务接口」表**(该文件 `docs/design.md:583` 起,`### 7.2 业务接口`)加 4 行:
    `/api/api-keys` 的 GET/POST、`/api/api-keys/{id}` DELETE、
    `/api/api-keys/{id}/rotate` POST。
    > ⚠ 不要加到 `:568` 那张表 —— 那是 §7.1 的**认证**表
    > (`/api/auth/login` 等),API Key 管理属于业务接口。
  - §7.1「认证与用户」补「双凭据」说明(JWT + API Key 并存)、scope 位表、curl 示例
- **`docs/not-supported.md`**:新增一节(排在「变更记录」之前),
  把本任务书 **§5 的 11 项「不做」**逐条登记,格式对齐该文件既有章节
  (`## N. 标题` + **不做。** + `### 依据` + 具体理由)。
  > ⚠ 该文件已有 13 节,其中 **§5 是「相关但不属于本文范围的项」**,
  > 与本任务书的 §5(本次不做)**完全无关**。登记时要**新增第 14 节**,
  > 不是往它现有的 §5 里加。

> ⚠ 写文档时**避免裸写 `§N`** —— 本文档自己的 §5/§6/§7 与
> `design.md` 的 §7 重号,极易混淆。引用外部文档一律带文件名。

---

## 4. 建议执行顺序与提交切分

```
A(数据层) ──→ B(凭据) ──→ C(缓存) ──→ D(Principal/scope/解析)
                                            │
                                            ↓
                                       E(中间件重构)★独立提交
                                            │
                                            ↓
                                    F(管理接口) ──→ G(前端) ──→ H(测试/文档)
```

| 提交 | 包含 | 可独立验证 |
|---|---|---|
| 1 | A-1、A-2、A-3、A-3b(`now_ms` 可见性) | 迁移日志出现;`cargo build` 通过 |
| 2 | B-1~B-3 | `cargo test -p rh-server apikey` |
| 3 | C-1~C-4 | touch / epoch 单测 |
| 4 | D-1、D-2、D-3、D-4 + `forbidden()` + **`scope_bits_align_with_masks`** | `cargo test`;位分配单测全绿(见下方 dead_code 说明) |
| 5 | **E-1、E-2、E-3、E-4、E-4b、E-5、E-6(含测试同步,规模见 §1.0)** | `cargo test --workspace` 全绿、e2e 全绿、`grep require_auth` = 0 |
| 6 | F-1、F-2、F-2b、F-3 | curl 打通;`created_by` 落库、rotate 换 prefix、软删后 list 仍可见 |
| 7 | G-1~G-5 | `pnpm build` |
| 8 | H-1~H-3 | 全量 e2e |

**提交 5 之后功能上还没有 API Key 可用**(F 才签发),但鉴权重构已可独立验证 ——
这是刻意的,让最高风险的改动单独暴露在现有测试下。

### 4.1 提交 4 的 dead_code 陷阱(**必读**)

提交 4 引入的 `forbidden()` / `resolve_principal()` / `verify_api_key()` 都是
**私有函数,而它们唯一的调用者是提交 5 才写的 `guard` 中间件**。所以提交 4
单独做完时:

```
warning: function `forbidden` is never used
warning: function `resolve_principal` is never used
warning: function `verify_api_key` is never used
```

而 `cargo clippy -- -D warnings` 会把这些**变成编译错误**(已实测,5 个 error)。

**三种处理方式,选一种**:

| 方案 | 做法 | 代价 |
|---|---|---|
| **A(推荐)** | 提交 4 给这三个函数加 `#[allow(dead_code)]`,**提交 5 时删掉该属性** | 多一步清理,但每个提交都绿 |
| B | D + E 合并成一个提交 | 失去"E 独立提交"的价值(E 是最高风险改动) |
| C | 提交 4 就先把 `guard` 写好但不挂路由 | `guard` 自己又成了 dead_code,问题只是转移 |

推荐 **A**。注意:**提交 5 必须记得删 `#[allow(dead_code)]`** —— 否则它会永久
掩盖真正的不再使用的代码。验收:

```bash
grep -rn "allow(dead_code)" crates/rh-server/src/api.rs crates/rh-server/src/apikey.rs
# 期望:提交 4 后有 3 处,提交 5 后 0 处
```

> ⚠ **提交 5 的工作量被严重低估的风险点**:它不只是"删 30 处 if",
> 还包含 E-4b 的测试调用点同步修改 —— 规模见 §1.0(实测非估算),
> 逐函数明细见 E-4b。若时间紧,可把 E-4b 拆成
> 提交 5a(先改 handler)+ 5b(改测试) —— 但 5a 单独提交时 `cargo test`
> **必然编译失败**,所以两个必须同一提交,或者接受"提交 5 前 `cargo test` 是红的"。
> 后者违反 AGENTS.md 的「红的门禁比没有门禁更糟」,不推荐。

---

## 5. 本次明确不做

| 不做 | 理由 |
|---|---|
| `?api_key=` 查询参数 | 会进访问日志与浏览器历史 |
| 加 CORS | API Key 面向服务端;放开等于把 key 交给任意网页 |
| 明文回读 / 找回 | 只能 rotate |
| per-resource ACL(只允许管某个 client) | scope 表与前端复杂度翻倍,等真需求再加 `resource_scope` 列 |
| 复用 `users.role` | 那是死代码(`0001_init.sql:8` 有 CHECK 但零引用),混进来反而乱 |
| per-IP 失败限速(本期) | prefix 2³² + secret 256 位不可枚举;`login` 限速是因为 argon2 贵 |
| 改 `events` 表结构 | 牵动四元组签名 + `PendingEvent` + `write_batch` + 前端类型,不值 |
| 用 argon2 哈希 key | key 是 256 位 CSPRNG,无字典攻击面;慢哈希在热路径上会压垮吞吐(实测参数 `m=19456,t=2`,约 20 MB 内存 + 数十 ms CPU,见 `api.rs:142`) |
| **不带 `rhk_` 前缀的 API Key** | 会让 `resolve_principal` 必须"先试 API Key 再看 JWT",两个解码器互相污染、且无法区分"格式错"与"密钥错"。**统一用前缀分流**(见 §2.1 / D-3) |
| **改 scope 的 PUT 接口** | 见 F-2b:吊销重建语义更清晰,避免"旧 key 已发出、权限被悄悄改" |
| **让 API Key 访问 `user_info`/`auth_codes`/`menu_all`** | 这三条服务于浏览器 UI,返回体围绕"当前登录用户"构造,对 Key 无对应物。技术上可做但语义不成立(详见 §2.2 的逐条理由)。若需"Key 查自己身份",应新增 `GET /api/api-keys/self` 而非复用 `user_info` |

---

## 6. 风险与回滚

| 风险 | 缓解 |
|---|---|
| 包 E 一次改 30 个 handler + 一批测试调用点(见 §1.0) | 单独提交;编译器兜底;`grep require_auth` = 0 作为验收;先跑现有 e2e;`cargo test --workspace` 必须真跑(`build` 不编译测试代码) |
| 误用 `layer` 埋下脆弱耦合(当前不报错,后续挪 fallback 才爆) | §1.3 实测矩阵 + 强制 `route_layer` + 手动验证 `/` 返回 200 |
| 迁移漏登记 | A-2 明写;验收看启动日志 |
| `route_layer` 顺序错导致中间件不生效 | 每组 route 全写完再 route_layer;验收用「无 token 调 `/api/clients` 应 401」 |
| 缓存 epoch 竞态 | 照抄 `pwd_version` 已验证的守卫模式 |
| **`rand` 版本混用导致编译错** | `rh-server` 同时有 `rand`(0.9.5)与 `rand_010`(0.10.3);**用 `rand`**,`use rand::RngCore;`。`rand_010` 根不导出 `RngCore`,必须走 `prelude`(见 B-1 实测表) |
| **`/` 与 `/api/nope` 的状态码回归** | 见下:必须在包 E 之前**先记录基线**,否则改完无法判定是否劣化 |
| **`guard` 的调用点与定义参数不一致** | 三参数 `guard(state, need, session_only)`。第 24 轮就踩过:定义改了签名但 E-3 的 8 处调用没跟着改 → 编译不过。验收:`grep -c "guard(state.clone()" crates/rh-server/src/api.rs` 应等于 8(9 组减 public) |
| **`use rand::RngCore;` 重复导致 clippy 红** | 只在 `apikey.rs` 顶部写一次,`generate()` 内不再写。`clippy -D warnings` 下重复 use 会直接失败(第 23 轮实测) |

### 6.1 包 E 的前置基线(必做)

`route_layer` 与 `layer` 在本仓库当前顺序下对这两条路径**行为相同**(见 §1.3 矩阵),
所以单看改完后的状态码无法证明实现正确。**动手前先记录基线**:

```bash
# ★ 先定义 PORT(值取自你的 rh-server 配置 [web].port;默认 8080)
PORT=8080

# 包 E 之前执行,把输出记到提交说明里
curl -s -o /dev/null -w "clients(no token)=%{http_code}\n" "http://127.0.0.1:$PORT/api/clients"
curl -s -o /dev/null -w "unknown api      =%{http_code}\n" "http://127.0.0.1:$PORT/api/nope"
curl -s -o /dev/null -w "root             =%{http_code}\n" "http://127.0.0.1:$PORT/"
```

> ⚠ **必须带引号且先定义 `PORT`**。两种笔误会让它"看似执行了"实则没测到正确目标:
> - 写成 `:PORT`(字面量)→ curl 把 `PORT` 当主机名,**`http_code=000`、退出码 3**
> - 只写 `$PORT` 而不定义 → `set -u` 下报 `unbound variable`
>
> **`http_code=000` 有两种含义,靠退出码区分**:
> - 退出码 7 = 连接失败(服务没起/端口错)—— 正常,继续排查服务
> - 退出码 3 = URL 格式错(就是上面那种笔误)
>
> 另:上面写的 `PORT=8080` 只是默认值,**实际值取你的 `[web].port` 配置**。

期望基线:`clients=401`、`unknown api=404`、`root=200`。
包 E 之后**三条必须完全一致**。任何一条变化都说明改造破坏了既有语义。

**回滚**

| 包 | 能否单独 revert | 说明 |
|---|---|---|
| A / B / C / D | ✅ 可以 | 纯新增(迁移、`apikey.rs`、缓存字段、`Principal`)且无外部调用者,revert 后编译通过 |
| **E** | ❌ **绝不能单独 revert** | 见下方红色警告 |
| F | ⚠️ 需与 E 一起 | F 的 4 条路由挂在 E 建立的 `session` 组里 |
| G / H | ✅ 可以 | 前端与测试,revert 后仅功能不可见 |

> 🔴 **E 单独回滚 = 安全事故,不是"功能损坏"**
>
> E 提供的是**鉴权中间件**。若 revert E 而保留 F:
> - `session` 组不存在了,路由结构回到扁平形态
> - `/api/api-keys` 的 4 条路由**仍在**(只要 revert 时没删 F 的路由注册)
> - 于是 **签发 API Key、列出全部 Key、rotate、吊销** 四个接口
>   **完全无鉴权**,任何人访问 `http://host:8080/api/api-keys` 就能拿到
>   一把 full 权限的密钥
>
> 所以:**revert E 必须同时 revert F**,且**在 revert 后立即用
> `curl -s -o /dev/null -w "%{http_code}" http://host:8080/api/api-keys`
> 确认返回 401/404**(绝不能是 200)。
>
> 稳妥做法:**E + F 作为一个整体回滚单元**(两个提交一起 revert),
> 不要分开操作。

---

## 7. 自查清单(每个提交前)

```bash
# 1. 三道门
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace          # ★build 不编译 #[cfg(test)],必须真跑 test

# 2. Windows cfg 自查
#    ★ 必须过滤注释行:2026-10-28 实测共 13 处命中,其中 6 处是注释、
#    7 处是真实代码(全部已有 cfg 门控;注意 `splice.rs` 的门控在
#    `lib.rs` 的 `#[cfg(target_os = "linux")] pub mod splice;` 上,不在文件内)
grep -rn "std::os::fd\|std::os::unix" --include=*.rs crates/ vendor/ | grep -v ':\s*//'

# 3. 包 E 之后:鉴权残留必须为 0
grep -rn "require_auth(&state, &headers)" crates/rh-server/src/api.rs

# 4. 包 E 之后:确认鉴权用的是 route_layer
#    期望:只看到 route_layer 的行;若出现 `.layer(middleware::from_fn` 则用错了
grep -n "route_layer\|\.layer(" crates/rh-server/src/api.rs
# 注:`.layer(Extension(..))` 是合法的(注入配置),不算错;
#     但**本任务书采用 E-1 方式 B**(`guard` 闭包捕获配置,不走 Extension 注入),
#     所以不应出现 `.layer(middleware::from_fn` —— 那条是"用错了 route_layer"的信号
grep -n '\.layer(middleware::from_fn' crates/rh-server/src/api.rs   # 期望:无输出

# 5. 鉴权失败必须走真实状态码,不得用 err(403,…)
grep -rn 'err(40[13]' crates/rh-server/src/     # 期望:无输出

# 6. 前端(若涉及)
cd console && pnpm build

# 7. 提交内容
git status          # 确认只 add 了自己改的文件

# 8. 包 E/F 之后:手工冒烟三条(对照 §6.1 基线)
PORT=8080   # ★ 取自 [web].port;下面三条都带引号
curl -s -o /dev/null -w "%{http_code}\n" "http://127.0.0.1:$PORT/api/clients"   # 401
curl -s -o /dev/null -w "%{http_code}\n" "http://127.0.0.1:$PORT/api/nope"      # 404
curl -s -o /dev/null -w "%{http_code}\n" "http://127.0.0.1:$PORT/"              # 200
# 退出码:能连上=0;连不上=7(此时 http_code 会是 000)。
# 服务没起时看到 000 是正常的,不是你命令写错了。
# 带 token 调 /api/clients 必须 200(不是 500 —— 证明 Extension 已注入)
```

---

## 8. 复核记录

| 轮次 | 发现 | 处置 |
|---|---|---|
| 第 1 轮 | 行号核对;`OPS` 误写 `0x05FF`(含 `CONFIG_W`) | 改 `0x057F`(第 3 轮又因位表重排改为 `0x157F`) |
| 第 2 轮 | 「`layer` 会让静态资源 401」在本仓库不成立(`main.rs` 是 fallback 在后) | 理由改为「脆弱耦合」,补实测矩阵 |
| 第 3 轮 | **① `EVENTS_R` 排在位 7(奇数)→ 不被 `READ_MASK` 覆盖 → 配合 fail-open 规则,`scopes=0` 的 key 可读 `/api/events`**<br>**② §2.4 `allowed==0` 放行是 fail-open**<br>**③ `err(403)` 实际返回 HTTP 200 → 权限拒绝全成"成功"**<br>**④ 未提删参数会打断 21 个 handler / 约 60 处测试调用点**<br>⑤ `Principal` 按值 `if let` 后无法 `insert`(编译错)<br>⑥ `ConnectInfo` 本期不用 → 未使用告警 → `clippy -D warnings` 失败 | 全部修正:位表改每资源 2 位(`READ_MASK=0x1555`)、规则改 fail-closed、新增 §2.5 `forbidden()`、新增 E-4b、E-2 改引用匹配 |
| 第 4 轮 ~~① `from_fn_with_state` 编译不过~~<br>~~② `route_layer` 必须在 `with_state` 之后~~<br>**③ `async fn resolve(&Request)` 导致 future 不 `Send`** ← 这条是对的<br>④ 9 组路由覆盖校验通过(24/24) | **①② 已在第 8 轮被推翻**(误判根因);③ 保留并成为 E-1 的核心 |
| 第 5 轮 | **① 任务书全用 camelCase(`expiresInDays`/`lastUsedAt`),而项目全线 snake_case**(后端 `created_at`、请求体 `old_password`、前端调用处亦然,`grep rename_all` 零命中)<br>**② G-4 说"复用 `secret-modal.tsx`"是错的**:该组件 297 行硬编码客户端接入三档模板,API Key 场景塞不进<br>**③ 项目已有 `MonoCopy`(`mono.tsx`)含完整复制+降级,应复用它而非再造第三份**<br>**④ `delete_api_key` 若按 H-2 原写返回 204,违反项目惯例**:4 个 DELETE 接口统一 `ok({"deleted":true})`<br>**⑤ G-3 行号偏差**(`:935` 实为 `<PasswordCard />`,`TabsContent` 起于 934)<br>⑥ H-2 的 e2e 未用仓库 `check`/`$API_PORT` 约定,且新造了不存在的 `$API` 变量 | 全部修正:F-2/G-1 改 snake_case、G-4 改为新写轻量弹窗+复用 `MonoCopy`、H-2 改用 `check` + `ok({"deleted":true})`、G-3 补精确行号与 `PageHeader` 文案同步 |
| 第 6 轮 | **① B-1 说用 `rand_010::rng()`,但实测 `rand_010`(0.10.3)根不导出 `RngCore`**(`E0432`),`fill_bytes` 需走 `prelude`;项目既有正确写法是 `rand`(0.9.5)+`use rand::RngCore;`(`stun.rs:20`)<br>**② C-1 的初始化行号 `445-447` 错**,那是 `login_limiter` 区,实际为 `454-455`<br>**③ C-3 说"改 scope 后 invalidate",但 F 组无对应接口** —— 内部不一致<br>④ 0005 DDL 经 SQLite 实跑验证:建表、CHECK、prefix 唯一约束全部生效 | 全部修正:B-1 改 `rand`+实测版本对照表、C-1 行号改 454-455、新增 F-2b 明确不做 update 并统一失效点为三个、A-1 补实测结论、风险表补 rand 混用 |
| 第 7 轮 | **① §2.3 只给了掩码常量,缺 `*_R`/`*_W` 的完整定义** —— AI 极易只抄掩码漏常量<br>**② §2.4 示例写 `return forbidden();` 缺参数**,与 §2.5 的 `forbidden(message: &str)` 不符<br>**③ D-4 的 `verify_api_key` 无签名**,与 D-3 调用点无法对齐<br>**④ `CachedApiKey` 只列字段未给定义**,且未说明必须 `pub`(实测:私有类型作 pub 字段 → `private_interfaces` 告警 → `-D warnings` 变错误) | 全部修正:补完整 `pub mod scope`(含 `bits_align_with_masks` 单测,已实测编译+测试通过)、§2.4 补参数、D-4 补签名、补 `CachedApiKey` 完整定义并说明 `pub` 必需性 |
| 第 8 轮 | **① 第 4 轮的核心结论是误判**:`from_fn_with_state` + `State<AuthCfg>` **完全可以编译**(8 组对照实验全部通过),E-1 声称的"State 类型必须 == Router 的 S"是错的<br>**② 真正的根因是 `async fn resolve(&Request)`**:`Request<Body>` 非 `Sync`,跨 `.await` 使 future 不 `Send`,而 axum 报的表面错误恰是 `E0277 FromFn: Service` —— 极易误诊成 State 问题<br>③ 代码已稳定(他人改动全部提交),但 33 处调用点行号仍有 **14 处漂移** | ① 全部行号改**锚点式**(handler 名 + 表格快照),不再裸用行号<br>② E-1 整节重写:删除错误论断,改为"真正的坑在借用",给 8 组实测矩阵 + 方式 A/B 两套可编译代码 + `to_owned()` 修法<br>③ 同步修正 §0 第 8 条、E-3 顺序说明、第 4 轮记录(标注已推翻)<br>④ E-1 方式 A 逐字编译 + 7 条行为用例全通过 |
| 第 9 轮 | 系统复检全部 23 条"实测"断言(独立重跑,不引用上轮结论)。**2 处不准**:<br>**① 断言13 "约 60 处测试调用点" 是估算**,实测 71 处;且其中 26 处涉及 `mint_jwt`/`resolve_visitor_target`/`*_inner` 等**无 `headers` 参数**,根本不用改。真正需同步改的是 **40 处 / 14 个函数**<br>**② 断言3 "全仓 `grep rename_all` 零命中" 不准确** —— `state.rs` 的 `ProxyState` 有 `#[serde(rename_all="snake_case")]`,值也是 snake_case,反而印证约定 | ① 调用点表改为实测值并按"是否受影响"分类,列出 14 个函数明细<br>② rename_all 表述改为"仅一处且值为 snake_case"<br>**其余 21 条断言全部复检通过**:err()返回200、Extension缺失500、DELETE返回值、private_interfaces、rand版本、route_layer矩阵、位置/规则绕过、fail-closed、build不编译test、DDL、33处/24条/4迁移、headers仅2次、Extension约束、middleware默认feature、9组覆盖、前端行号、secret-modal、MonoCopy、列名、scope常量 |
| 第 10 轮 | 按"交给不知上下文的 AI 能否执行"逐项检查,发现 **4 处可执行性缺口**:<br>**① `lookup` / 4 个 api-keys handler 共 5 个函数没有签名** —— AI 只能猜参数与返回类型<br>**② `scopes` 类型自相矛盾**:F-2 结构体写 `Option<u32>`,而下文写 `number[]`<br>**③ `apikey_touch` 未说明时间单位**,且代码里调 `crate::db::now_ms()` 而该函数是**私有** → 实测 `E0603`<br>**④ `now_ms` 可见性修改没作为前置子任务列出** | 全部修正:① 补 `lookup_api_key(&self,prefix)->Option<CachedApiKey>`、F-2 四个 handler 完整签名 + `CreateApiKeyReq` 结构体(已逐字编译通过)<br>② 统一为 `u32` 位掩码并给 preset 映射表<br>③ 补完整 `apikey_touch` 实现 + 三条易错点(已逐字编译运行通过)<br>④ 新增 A-3b 显式列出 `now_ms` 改 `pub(crate)` |
| 第 11 轮 | 端到端可执行性检查,发现 **4 处链路断裂**:<br>**① A-3 的 6 个 DB 方法全无返回类型** → C-2 的 `lookup_api_key` 无法对接<br>**② `lookup` 的职责歧义**:注释说"None 表示已禁用/已过期",但 D-4 又把 `enabled`/`expires_at` 列为独立步骤 —— 两处都判则"为何被拒"不可区分<br>**③ `CachedApiKey` 缺 `id`/`name`** → D-4 第 7 步构造不出 `Principal::Key{id,name,scopes}`<br>**④ 哈希口径矛盾(致命)**:DDL 注释写 `sha256(secret)`,而 D-4 第 3 步算 `hash_hex(full)`(完整串)—— 两种口径 sha256 完全不同,**会导致所有 key 永远 401** | 全部修正:① 补 `ApiKeyRow` 结构体 + 6 个方法完整签名<br>② 定案「lookup 只查不判,有效性统一在 verify_api_key」<br>③ `CachedApiKey` 补 `id`/`name` 并说明必需性<br>④ 统一为 `sha256(完整 bearer)`,补 `generate()` 完整实现,并写明两种口径都可行但必须全链一致<br>**实测验证**:`generate→parse→hash` 1000 次全一致;`verify` 7 条路径(正常/错secret/禁用/过期/未过期/不存在/形态错)全部符合预期 |
| 第 12 轮 | 继续 E→F→G 跨包衔接,发现 **3 处**:<br>**① `READONLY` 的十进制值残留旧位表值 `1365`**(应为 `5461`),共 3 处(`scopes: 1365` / preset 映射表 / 响应示例)—— 这是第 3 轮改位表时漏改的<br>**② `create_api_key` 未说明 `created_by` 从哪取**(`Principal::User(username)`)<br>**③ `rotate_api_key` 没强调"必须 bump 缓存"** —— 实测证明忘记 bump 时**旧 key 仍然可用**<br>④ 前端"权限徽标"缺 `scopes` 位→文字的映射 | 全部修正:① 3 处改 `5461`,并全文数值交叉验证零残留<br>② 补 `let Principal::User(operator)` 取法 + 双保险说明<br>③ 补"三个写操作都必须 invalidate"的说明 + 实测证据;新增回归用例 `apikey_rotate_old_key_rejected`<br>④ 补前端 `SCOPE_BITS` + `scopeBadges()`,并用 node 验证与后端常量逐位一致 |
| 第 13 轮 | 改用**脚本机械核对**全部数值常量(不再靠人眼扫),发现:<br>**① 位定义/位表/掩码三类共 27 个数值全部正确**(12 个 `1<<N` + 2 个掩码 + 13 行位表)<br>**② 正文(§0-§8)零错误值**;`1365` 仅出现在第 12 轮复核记录里(描述历史错误,属正常)<br>**③ 发现一处未定义的边界语义:大小写与空格** —— `parse` 接受大写 hex 但 hash 对原串计算,大写 key 必然 401;`generate` 只产小写 | ① 数值无需修正<br>② 给复核记录加了"历史值"说明,避免后续扫描误报<br>③ 补大小写/空格语义说明 + 新增单测 `parse_accepts_uppercase_but_hash_mismatch`<br>**实测**:`generate()` 产出长度 77/8/64 与文档一致、1000 次无重复;`parse()` 11 条边界用例全符合预期 |
| 第 14 轮 | 做「文字描述 vs 代码」一对一机械核对,发现 **3 处**:<br>**① E-3 骨架只给了 public/session/clients 三组**,其余 6 组用「同构 ...」省略 —— `need` 值各不相同,AI 要自己推,极易错<br>**② F-1 说把 api-keys 路由「插在 `.with_state(..)` 之后」是错的指导**,实测报 `E0308: expected MethodRouter, found MethodRouter<Arc<AppState>>`<br>**③ E-3 约束 3 自相矛盾**:文字说"推荐先 route_layer 再 with_state",但骨架写的是 with_state → route_layer | 全部修正:① 补齐全部 9 组完整代码(24 条路由/8 个 route_layer/9 个 with_state/8 个 merge,已逐字编译通过)<br>② F-1 改为「插在 `.route(..)` 序列里、`.with_state` 之前」并给完整示例<br>③ 统一为「route → with_state → route_layer」,说明理由(确保不漏包)<br>**核对通过**:§2.2 声明的 14 项覆盖仓库全部 24 条路由,无遗漏 |
| 第 15 轮 | 逐条验证所有「必须/推荐/不要」类指导性断言,发现 **1 处致命缺陷 + 1 处矛盾**:<br>**①（致命）`resolve_principal` 没剥 `Bearer ` 前缀** —— 中间件传的是完整头值 `"Bearer rhk_xxx"`,而它用 `starts_with("rhk_")` 判断,对完整头值**永远 false** → 所有 API Key 请求掉进 JWT 分支并 401。与第 11 轮哈希口径同属「跨包约定没对齐」<br>**② E-2 说 `resolve_principal` 可以同步**,但 D-3/E-1 都写成 `async`,且它要查缓存/查库 | 全部修正:① D-3 补「先 `strip_prefix("Bearer ")` 再判 `rhk_`」的完整代码 + 醒目警告;`verify_api_key` 注释改为「收到的是已剥前缀的串」<br>② E-2 整节重写为「必须 async」,删除过时的同步版本<br>**实测**:修正后 7 条鉴权用例全部通过(无token 401 / JWT 200 / 只读 200 / 只读写 403 / 全权写 200 / Key调session 403 / JWT调session 200)<br>**其余断言验证通过**:`route_layer`+`merge` 顺序(方案2 会把无关路由也罩住)、`FULL≠所有常量之和`(差位 7/9)、`OPS` 不含 config_w/certs_w、`rh-server` 无 ring、`events` 表改动成本 |
| 第 16 轮 | 把 A→F **整条链路拼成可编译工程**跑端到端(前几轮只测片段)。结果:<br>**① 全链路编译通过、15 条端到端用例全绿** —— 建 key/只读读/只读写 403/Key调session 403/读 events 200/写 config 403/无token 401/未知路径 404/rotate 后旧 key 401/吊销后 401/有效 key 不能建 key(防提权)<br>**② 发现 1 处缺失:4 个接口的响应体结构没写**(rotate/delete 返回什么、错误码用哪几个) | ① 链路本身无缺陷(第 15 轮修的 Bearer 剥离生效)<br>② 补齐四个接口的响应体(JSON 逐字)+ 错误码表(`1002` 参数校验 / `1004` 不存在),并核对与项目现有体系一致(`err(1002)` 出现 20 次、`err(1004)` 有先例,连措辞"名称长度须为 1-64 字符"都一致) |
| 第 17 轮 | 首次真正执行验证 G(前端)与 H(e2e)—— 此前从未跑过:<br>**① G 包 `scopeBadges`**:写进项目后 `tsc --noEmit` 与 `eslint` 均通过;运行时 `scopeBadges(5461)=['只读']`、`(5503)=['运维']`、`(16383)=['全部权限']` 全对;按位或自算的 READONLY/OPS 与硬编码值一致<br>**② H-2 的 e2e 片段**:`bash -n` 语法通过,在 mock server 上 **7/7 全绿**(建 key/只读读/只读写 403/Key 调 logout 403/未知 404/吊销/吊销后 401)<br>**③ 发现 1 处脆弱写法**:`grep -q '"deleted":true'` 匹配 JSON 字面量 | ① G 无需修改(上一轮的「1365」是测试用错数值,非文档缺陷)<br>② H-2 无需修改(逻辑正确)<br>③ 改为 `python3 -c` 解析 JSON,并补「四条注意」第 0 条说明理由:实测 `axum::Json` 输出紧凑无空格,但任何中间层都可能加空格 —— 我正是在一个加空格的 mock 上误判成失败 |
| 第 18 轮 | 检查收尾环节(§4 提交划分 / §6 回滚 / §7 自查 / 各轮历史说明一致性),发现 **3 处**:<br>**① §1.2 表格里 `state.rs` 初始化仍写旧行号 `445-447`**,与 C-1 已修正的 `454-455` 矛盾 —— 第 6 轮的修正漏改了这张表<br>**② "约 60 处"在 §0/§4/E-4b/风险表 4 个位置残留**,与第 9 轮实测的 "40 处 / 14 个函数" 矛盾<br>**③ §6 回滚说明严重低估风险**:原文只说"E 与 F 强耦合,需同时 revert",没指出**revert E 保留 F = 接口完全无鉴权** | 全部修正:① §1.2 改 454-455<br>② 4 处统一为 40 处;仅保留"早期版本写约 60 处是估算"的说明性引用<br>③ 回滚章节重写为表格 + 🔴 红色警告:**E 单独回滚是安全事故**,并给实测证据(revert E 后 `GET /api/api-keys` 返回 HTTP 200 + 泄露密钥列表)、要求 revert 后立即 curl 确认 401/404<br>另:§7 的 `route_layer` grep 补了预期说明(`.layer(Extension(..))` 合法,只查 `.layer(middleware::from_fn`) |
| 第 19 轮 | 针对第 18 轮暴露的根因(**同一事实在多处重复 → 改一处漏三处**)做结构性治理:<br>**① 新增 §1.0「单一事实源(SSOT)」**:把 10 项被反复引用的数字(路由 24 / 鉴权点 33 / 测试调用 40 处 / 初始化行号 / 迁移数 / 三个 scope 常量 / 明文长度 77)集中到一张表,每项附**可执行校验命令**<br>**② 正文中重复的 5 处「40 处」改为引用 §1.0**,只保留 SSOT 表本身与 E-4b 明细(数据产生处)<br>**③ 发现并修正 2 个校验命令本身的缺陷**:`grep -c '\.route('` 只得 18(多方法是链式的,须按 path 去重);`grep -n "pwd_version_cache: dashmap"` 会命中第 70 行**字段声明**,须用 `new()` 锚定 | ① 建立 SSOT 表 + 「校验命令集」(5 条命令**逐条实测可用**:输出 24 / 33 / 454 / 4 / 77)<br>② 5 处改为「见 §1.0」<br>③ 校验命令改为:python 去重算路由数、`DashMap::new()` 锚定初始化行号<br>另:结构完整性验证通过(9 个一级章节齐全,120 个代码围栏配对) |
| 第 20 轮 | 验证 SSOT 引入后的**指导性一致性**,发现 **2 处**:<br>**① 章节层级错误**:`### 为什么用 route_layer...` 与 `### 1.3` **同级**,导致它脱离 §1.3 独立成一个平级小节,§1.3 内容被割裂<br>**②（指导冲突）E-1 说「推荐方式 A(`from_fn_with_state`+`State<AuthCfg>`)」,但 E-3 的「已实测定版,照抄」骨架**整段用方式 B**(`guard`+`from_fn`,出现 8 次)** —— 实现者不知道该照哪个 | ① 降级为 `#### 1.3.1`,全文标题层级扫描确认无跳级<br>② E-1 改为「**本任务书统一采用方式 B**」,补对比表与选 B 的理由(`Box::pin(.. + Send)` 让误用 `&Request` 在出错那行就报出来),并注明「以 E-3 为准」<br>**实测**:方式 B 的 `guard` 与 F-2 的 `Extension(principal)` 配合正常(带 token POST `/api/api-keys` → 200 + 返回 `{"by":"admin"}`;无 token → 401),证明统一到方式 B 后链路仍成立 |
| 第 21 轮 | 系统检查全部跨文档/跨章节引用,发现 **3 处**:<br>**① H-3 说「`design.md` §7 路由表(`:568` 起)加 4 行」是错的** —— 实测 `design.md:568` 是 **§7.1 认证表**(`/api/auth/login` 等),API Key 管理属业务接口,应加到 **§7.2(该文件 `:583` 起)**<br>**② 裸写 `§N` 有歧义**:`design.md` 与本文档都有 §5/§7,`not-supported.md` 的 §5 也叫"§5"但讲的是另一件事<br>**③ `not-supported.md` 已有 13 节且其 §5 是「相关但不属于本文范围的项」**,原写法"登记 §5 的项"会被误读成往它现有 §5 里加 | ① 改为「§7.2 业务接口表(`:583` 起)」并加警告"不要加到 `:568` 那张认证表"<br>② H-3 重写:外部文档引用一律带文件名,并加「避免裸写 §N」的提示<br>③ 明确「新增第 14 节,排在变更记录之前」,给出格式模板<br>**验证通过**:本文档内 16 处 `§` 引用全部有效(§0/§1.0/§1.2/§1.3/§2.3/§2.4/§2.5/§5/§6.1/§7/§B-1/§D-3/§E-1/§E-4b/§E-6);§5 实测 8 项,与文档声明一致 |
| 第 22 轮 | 从头完整通读(脚本查不到的连贯性问题),发现 **2 处**:<br>**① §2.1 的凭据示意图有歧义**:把「SHA-256 后存库」标在 **secret** 下方,会让人以为只哈希 secret —— 正是第 11 轮那个"所有 key 永远 401"致命 bug 的温床<br>**② `created_by` 只写不读**:DDL 有该字段、`create_api_key` 写入操作者、还专门补了取法说明,但 **list 不返回、前端类型没有、列表列也没这一列** —— 死字段。而 `clients`/`services`/`certs` 都没有此字段(`api_keys` 是项目首个),既然引入就该用 | ① §2.1 重画:整串向下汇聚标注 `key_hash = sha256(【完整这一整串】)`,加醒目警告并回指 B-1<br>② list 响应补 `created_by`;前端 `ApiKeyRow` 补字段;列表列补「创建者」;并说明多人共用控制台时它是判断"该不该吊销"的主要依据<br>**验证**:三方字段链路对齐 —— DDL 10 列 → list 9 列(唯不返回 `key_hash`)→ 前端类型 9 字段,无缺失无多余;另核对 §2.2 的 9 组 need 与 E-3 骨架 `guard(...)` 参数**逐一一致** |
| 第 23 轮 | 通读包 B~C,发现 **2 处**:<br>**① B-1 有重复的 `use rand::RngCore;`**(文件顶部一次 + `generate()` 内一次)—— 实测触发 `unused import` 警告,而项目 `clippy -D warnings` 会**直接编译失败**<br>**② C-3 没说明 epoch 失效后的**条目残留** —— "不要 remove 条目"是有代价的:rotate 换 prefix 后旧 prefix 条目**永久残留** | ① 顶部保留唯一一处 `use`,并在原处加醒目注释说明为什么不能再写;`generate()` 内改为注释<br>② 补「已知代价」段:实测 `apikey_cache.len()` 等于历史出现过的 prefix 数(与 epoch bump 次数无关);给出可忽略的三条理由(key <10³、rotate 低频、单条约 200B);并给出将来治理方案 `retain(\|_, e\| e.epoch == new_epoch)`(**已实测该写法在元组与结构体两种形态下都可用**),同时注明"现在不要加" |
| 第 24 轮 | 通读包 D~E(历史翻车最密集区),发现 **3 处**:<br>**①（编译不过）E-1 的方式 B `guard` 只有 `(need, session_only)` 两个参数**,函数体里用 `/* state 需另行获取或由闭包捕获 */` 占位 —— 而 E-3 骨架直接 `guard(0, true)` 调用。**拿不到 `Arc<AppState>` 就无法调 `resolve_principal`**<br>**② D-4 第 5 步没写时间单位与比较符**(`expires_at` 是毫秒还是秒?用 `<` 还是 `<=`?)<br>**③ `expires_in_days` → `expires_at` 的转换公式完全没写** | ① 方式 B 改为**三参数** `guard(state, need, session_only)`,补完整函数体(含 `state.clone()` / `to_owned()` / 两条拒绝分支 / `insert(p)`);同步修正 **E-3 的 8 处 + F-1 的 1 处**调用为 `guard(state.clone(), ...)`<br>② 补精确写法:`expires_at` 单位**毫秒**,判 `exp <= now_ms()`(用 `<=` 不用 `<`,否则同毫秒内单测不稳),`None` = 永不过期(别写 `unwrap_or(0)`)<br>③ 补转换代码:`MS_PER_DAY = 86_400_000`、范围 `1..=3650`、`checked_mul` 防溢出、传 `0` 视为非法<br>**实测**:三参数 `guard` 端到端 **6/6 通过**(无token 401 / JWT 200 / Key只读读 200 / Key只读写 403 / Key调session 403 / JWT调session 200);`to_expires_at` 六种输入全部符合预期 |
| 第 25 轮 | ① 先回归验证第 24 轮对 E-3 的 8 处批量修改**没改坏**:逐字编译通过、24 条路由无缺无余、9 组中 public 无中间件其余各 1 个 `route_layer`<br>② 发现 **2 处**:E-5 仍在裸用行号(`api.rs:411/221/432/72/448`),**违反 §1.1 自己定的"不要用行号"**;`api.rs:52-63`(misc 组)在包 E 改造后必然失效却没标注 | ① 无需修改(回归通过)<br>② E-5 改为按函数名定位;全文 31 处 `文件:行号` 逐一核实**当前全部准确**,且都指向稳定锚点(函数定义/模块声明)而非易漂移的调用点;`misc` 组那处加「包 E 改造前」标记并说明按 `let misc =` 搜<br>**验证**:E-3 骨架编译 0 错误、`cargo run` 正常;9 组 `route_layer` 覆盖逐一核对正确(1+5+3+6+4+1+1+1+2 = 24 条路由) |
| 第 26 轮 | 通读 F~H,发现 **3 处**:<br>**① `created_by` 补进响应体后没有任何测试覆盖** —— H-1 单测清单与 H-2 e2e 都没验证它,"字段加了但没写进去/没返回"会静默通过<br>**② G-3 只给行号不给文本锚点**,与 §1.1「不要用行号」的原则不一致(前端虽稳定,但该原则应统一)<br>**③ H-2 的 `A="..."` 变量与 `api()` 函数只差大小写**,虽在 bash 里不冲突(实测两个命名空间),但阅读时易混 | 全部修正:① 补单测 `apikey_created_by_recorded` + `apikey_list_excludes_key_hash`,并在 H-2 加 python 断言(**三种输入实测:正常通过 / 缺 created_by 拦截 / 泄露 key_hash 拦截**)<br>② G-3 补四条文本锚点搜法(搜 `const SETTINGS_TABS` 等)<br>③ 变量改为 `BASE_URL`(8 处),`bash -n` 语法通过<br>**核对通过**:F-2 的 list 响应 9 字段与 G-1 的 `ApiKeyRow` 9 字段**完全一致**;G-3 的 8 个行号当前全部准确 |
| 第 27 轮 | 做**逆向检查**(从新增能力反推测试覆盖),发现 **8 处盲区**:F 包的 5 条参数校验(name/scopes/preset/expires)全无测试;rotate 返回明文、软删语义、list 含已吊销行均无断言;G 包 `scopeBadges` 无前端单测 | 全部补齐:<br>**H-1 新增 5 个单测**:`create_api_key_rejects_invalid`(5 条校验逐条断言 code)、`create_api_key_accepts_presets`(三 preset 映射 + scopes 优先)、`rotate_returns_new_plaintext`、`delete_is_soft`(吊销后仍可在 list 看到且 enabled=false)、`apikey_list_excludes_key_hash`<br>**H-2 新增 2 条 e2e**:rotate 返回新明文且与旧不同;列表含 created_by 且不含 key_hash/明文<br>**G-3 新增前端单测** `scope.test.ts`(照 `services/__tests__/spec.test.ts` 风格,期望值写死)<br>**全部实测**:参数校验 15 条用例(含边界 64 字符 / scopes=FULL / days=1)全符合预期;前端 4 个 vitest **实跑通过**,并验证其有效性 —— 把 `events_r` 故意改回错误的 `1<<7` 后测试立刻报 `expected 128 to be 256`(正是第 3 轮权限绕过 bug 的形态),恢复后 4/4 通过 |
| 第 28 轮 | 抽查全部 36 处「实测/已验证」声明,确认没因后续修改失效。重跑 5 项关键结论:<br>① §1.3 的 `route_layer` vs `layer` 4×4 矩阵 —— **逐格与任务书完全一致**<br>② `axum::Json` 输出紧凑格式(`"deleted":true` 无空格)—— **成立**<br>③ 两种哈希口径的 sha256 完全不同 —— **成立**(`deb67899…` vs `ffe054fe…`)<br>④ `Extension` 缺失时返回 **500** —— **成立**<br>⑤ `rand` 0.9 可用 `RngCore` / `rand_010` 报 `E0432` —— **成立**<br>⑥ `guard` 三处一致性复查:定义 3 参数,**9 处实际调用全部带 `state.clone()`**,另有 2 处 `guard(0, true)` 位于"早期版本怎么写"的说明文字中(非调用,正常) | 无需修改 —— 36 处实测声明均仍成立,没有因第 24~27 轮的修改而失效 |
| 第 29 轮 | 通读 §0 与 §4~§7(执行层,最近改动少),发现 **3 处**:<br>**① §0 第 5 条把 `rand_010` 列为"已在 Cargo.toml 中"的可用依赖**,与 B-1「用 `rand`,不要用 `rand_010`」直接冲突<br>**② §7 第 4 条的注释仍写「本任务书采用 E-1 方式 A(配置走 State)」** —— 第 20 轮已统一到方式 B,此处是漏改<br>**③ §4 提交表用 `F-1~F-3` 范围写法**,但 F 组实际有 F-1/F-2/F-2b/F-3 四个子项(读者无法确认 F-2b 是否包含) | ① 改为显式版本对照表(0.9.5 用 / 0.10.3 不用并说明原因)<br>② 改为「采用方式 B(`guard` 闭包捕获,不走 Extension 注入)」<br>③ 提交 1/4/5/6 全部改为显式列出子项<br>另补 2 条风险:E-3 的 `guard` 调用点与定义参数一致性(附 `grep -c` 验收值 8)、`use rand::RngCore` 重复导致 clippy 红<br>**核对通过**:§5 与 D-3/D-4 关于「不做 per-IP 限速」表述一致;§5「`users.role` 是死代码」经核实属实(`get_user` 返回 `.2` 但两个调用点只用 `.1`);§4 的提交顺序无依赖倒置(E 不依赖 F,F 依赖 E 的 session 组) |
| 第 30 轮 | 做完整执行模拟(A→H),按「每包完成时能否独立编译通过」逐个验:<br>**① 依赖方向全部正确** —— 无反向依赖(A/B 无前序;C←A,B;D←B,C;E←D;F←A~E;G←F;H←全部)<br>**② 发现真实缺口:提交 4 有 dead_code 陷阱** —— D 包的 `forbidden()` / `resolve_principal()` / `verify_api_key()` 是**私有函数**,而唯一调用者(`guard`)要到提交 5 才写。提交 4 单独完成时实测:3 条 `never used` 警告,`clippy -D warnings` 下**变成 5 个编译 error**。§4 原本没提这件事 | 新增 **§4.1「提交 4 的 dead_code 陷阱」**:列出三种处理方案对比(推荐 A:提交 4 加 `#[allow(dead_code)]`、提交 5 删除;另两种分别是 D+E 合并、提前写 guard),并给出验收命令(`提交4 应有 3 处 allow、提交5 应 0 处`)<br>**其余提交中间态核对通过**:1/2/3 是 `pub` 项不触发 dead_code;5 需删 `require_auth`(E-6 已覆盖)且保留 `unauthorized`/`jwt_engine`(E-6 已说明);6/7/8 无此问题 |
| 第 31 轮 | 验证 10 条验收项是否**真的可执行**,发现 **3 处**:<br>**① 第 1 条验收「能被 `sqlx::raw_sql` 整段执行」之前只用 sqlite3 验过** —— 两者机制不同<br>**② 第 6 条验收「1000 次 touch 只写 1 次库」不可执行**:`apikey_touch` 是 `tokio::spawn` 异步写,没有可观测的写入计数;直接断言会读到 0<br>**③ 6 个 DB 方法只给了签名没给 SQL** —— 实现者要自己猜列顺序 | ① 用**真实 sqlx**(`SqlitePoolOptions` + 事务内 `raw_sql`)重验:**通过**,表/`idx_api_keys_prefix` 索引/10 列全部正确<br>② 改为**外部可观测断言**:touch 后查 `last_used_at` 变没变(而非内部计数),并强调 `sleep` 不能省;同时说明"不要给 `Db` 加计数钩子"(`Db` 只是 `SqlitePool` 薄包装,为测试改生产结构不值得)<br>③ 补 6 条 SQL 全文,并**实测**:INSERT 后 `last_used_at` 为 NULL、`enabled=0` 后 `get_by_prefix` 返回 None 而 `list` 仍可见、rotate 换 prefix、touch 更新,全部符合预期 || 第 32 轮 | 检查剩余 8 条验收项的可执行性,发现 **4 处**:<br>**① D-4 说「7 条路径中的失败分支」,实际只有 5 个失败分支** —— 第 6 步(`touch`)是成功路径的副作用、第 7 步(`Ok`)是成功返回,都不是分支<br>**② H-1 的 `resolve_principal_four_paths` 说「覆盖 4 条」**,与 D-4 的 7 条、实际的 5 条三者互不一致<br>**③ 第 9 条验收「key 调 /api/clients」没写方法与期望码** —— 只读 key 读得 200、写得 403,不写清就是歧义<br>**④ 测试清单分散在 B-3 / H-1 / G-3 三处**,没有汇总;且 B-1 的验收补充(`parse_accepts_uppercase_but_hash_mismatch`)没并入 H-1 | 修正:<br>① D-4 验收改为表格列出**5 个失败分支**(parse 形态 / lookup 无 / hash 不等 / disabled / expired),并说明第 6/7 步不是分支<br>② H-1 拆成 `verify_api_key_five_rejections`(对应 5 分支)与 `resolve_principal_jwt_and_key`(4 种输入分流),并补入大小写测试<br>③ 第 9 条改为 7 步 curl 清单,每步写明方法与期望码(与我第 16 轮实测的 15 条用例对应)<br>④ H-1 顶部加索引表(三处位置 + 覆盖范围 + 数量),**实测核对为 4 + 17 + 4 = 25 个**(初写 13 是估算,已改)<br>**验证**:`cargo test apikey` 过滤器实测可用(但拼错会显示 0 测试却返回成功,已在文中提示) |
| 第 32 轮 | 检查剩余 8 条验收项的可执行性,发现 **4 处**:<br>**① D-4 说「7 条路径中的失败分支」,实际只有 5 个** —— 第 6 步 `touch` 是成功路径副作用、第 7 步 `Ok` 是成功返回,都不是分支<br>**② H-1 的 `resolve_principal_four_paths` 说「覆盖 4 条」**,与 D-4 的 7 条、实际的 5 条三者互不一致<br>**③ 第 9 条验收「key 调 /api/clients」没写方法与期望码**(只读 key 读得 200、写得 403)<br>**④ 测试清单分散在 B-3 / H-1 / G-3 三处**没有汇总;B-1 的验收补充(`parse_accepts_uppercase_but_hash_mismatch`)也没并入 H-1 | 修正:① D-4 验收改为表格列出 **5 个失败分支**,并说明第 6/7 步不是分支<br>② H-1 拆成 `verify_api_key_five_rejections`(5 分支)与 `resolve_principal_jwt_and_key`(4 种输入)<br>③ 第 9 条改为 7 步 curl 清单,每步写明方法与期望码<br>④ H-1 顶部加三处索引表;数量**实测**为 B-3=4、H-1=17、G-3=4,合计 25 |
| 第 33 轮 | 把测试清单纳入 SSOT、核查事实重复度,发现 **4 处**:<br>**① §1.1 的「33 处里已有 14 处偏移」是过时信息** —— 实测表内 33 个行号**全部准确(0 处漂移)**,旧说法会让读者以为表不可信<br>**② 第 32 轮记录文档里缺失** —— 上一轮的编辑脚本拼接出错,32 条记录里只有 31 条<br>**③ SSOT 校验命令集被我上一轮替换时弄丢前 5 条** —— 只剩 6)、7) 两条<br>**④ 新增的 2 行 SSOT(测试总数 / 失败分支数)起初没有可执行的校验命令**,我先写的两条是"仅供参考"的假命令(实测返回 10 而非 5) | 修正:① 改为「本次复核 0 处漂移,但历史曾漂移 14 处,故定位仍以 handler 名为准」<br>② 补回第 32 轮记录(现 32 条齐全)<br>③ 恢复完整 7 条命令,并新增两条精确校验;实测 **7/7 全部跑通**(24 / 33 / 454 / 4 / 77 / 4+0 / 5)<br>④ 第 6 条改为"实现完成后数 `#[test]`"并加 `2>/dev/null || echo 0` 容错(apikey.rs 未创建时不中断命令集);第 7 条改为 `sed -n '/^#### D-4/,/^### 包 E/p' \| grep -c '| 401 |'`,实测输出 5 |
| 第 34 轮 | **只读验证轮**(不修改正文,只验证;避免"修改引入新缺陷"的循环)。做了 9 项交叉验证,**全部通过**:<br>① 文档结构:9 个一级章节、140 个代码围栏配对<br>② SSOT 5 项权威值 vs 代码实测:24 / 33 / 4 / 454 / 77 全一致<br>③ scope 常量自洽:12 个位定义全部落在对应掩码内,READONLY=5461 / OPS=5503 / FULL=16383 算得值正确,位 7/9 未被占用<br>④ §2.2 九组 vs E-3 骨架:9 组全对应(含 session 的 `guard(state.clone(), 0, true)`)<br>⑤ §2.4 判权规则 vs E-1 guard 实现:fail-closed、`allowed==0` 拒绝、方法判断、`to_owned()` 全部一致<br>⑥ D-4 七步与 B-1 的 hash 口径一致(都用完整串)、第 5 步用毫秒<br>⑦ 字段链路 DDL(10) → ApiKeyRow(10) → list(9,唯排除 key_hash) → 前端类型(9) 无缺失无多余<br>⑧ 错误码 `1002`×4 + `1004`×1,项目中有先例(20 次 / 1 次)<br>⑨ H-2 e2e 引用的字段(`key`/`created_by`/`deleted`/`id`/`name`)在 F-2 响应体中都有 | **零发现,无修改**。本轮唯一动作是追加本行记录 |
| 第 35 轮 | 只读验证(继续不修改正文),对包 B/C 做**逐字拼接编译**:<br>**① 包 B 的 `generate` 有"声明式"与"实现式"两份** —— 块 0 里是 `pub fn generate() -> Generated;`(无函数体),块 1 又是完整实现。逐字拼接实测报 `E0428: name generate is defined multiple times` + `free function without a body`<br>**② 包 C 的 `apikey_touch` 同样问题** —— C-4 先给 `pub fn apikey_touch(&self, id: &str);` 再给完整实现 | 记为**2 处发现**(本轮不清零)——两处都是"先签名概览、后完整实现"的结构,但**文中没说二者是同一函数的覆盖关系**,照抄会重复定义。正确拼法是"用实现替换声明"。**本轮未修改**,留待下一轮统一处理(保持只读原则,先积累清单再一次性修) |
| 第 36 轮 | 修掉第 35 轮发现的 2 处拼接陷阱,并**全文扫描同类结构**:共 12 处"无函数体的声明式 fn"。逐处判定:6 个 DB 方法**有 SQL 就是实现规格**(正常);`generate`/`apikey_touch` 同时有声明+实现(**必修**);`parse`/`hash_hex`/`lookup_api_key` 只有声明无实现(**需补**);`resolve_principal` 的声明与实现在不同章节且用 `..` 占位(低风险) | 修正:① §B-1 的概览块后加说明 + **补 `parse`/`hash_hex` 的完整实现**(这两个之前只给签名,读者要自己猜 `strip_prefix`/`split_once` 的细节)<br>② §B-1 的 `generate` 实现块标注「**这一块替换上面的声明,不是追加**」<br>③ §C-4 同样标注「替换而不是追加」<br>④ §0 新增第 8 条:**代码块分三种**(接口概览/完整实现/示意片段)及各自用法,并注明拼接会得 `E0428`<br>**验证**:按修正后的拼法(概览→用实现替换)→ 编译 **0 错误**、`cargo test` 通过 |
| 第 37 轮 | 只读验证上一轮修正是否引入新不一致,发现 **3 处**:<br>**① §0 第 8 条(代码块分三种)与全文 8 处「照抄」表述需要对齐** —— 逐一核对后 7 处都指向"完整实现"或"常量定义"(不冲突),但 **L1044 的「必须照抄」指向的块末尾有 `...` 省略**,属示意片段,与该条"示意片段不要编译"有轻微冲突<br>**② 上一轮新补的 `parse`/`hash_hex` 被包在引用块里**(`> ```rust`) —— 全文 46 个顶层 rust 块只此一处破例,编辑器里复制会带上 `> ` 前缀<br>**③ 该引用块的说明文字「都很短,按上面的 doc 注释写即可」措辞含糊** —— 实际已给出完整实现,不该再说"按注释写" | 修正:① L1044 澄清为「照抄**前 3 段分流逻辑**;末尾 `...` 是省略,不是字面内容」<br>② 把 `parse`/`hash_hex` 实现**提升到顶层代码块**,与全文风格一致;说明改为「上面概览里只给了签名」<br>③ 随 ② 一并改<br>**验证**:顶层围栏 128 个、引用块围栏 2 个(L817/L822 的行号快照,属合理用法)**全部配对**;顶层 rust 块增至 46 个 |
| 第 38 轮 | 只读验证 §5「不做清单」的**每条理由是否成立**,并检查有无遗漏项。**7 条原理由全部验证成立**:<br>· `users.role` 是死代码 —— `0001_init.sql:8` 确有 CHECK,`grep '\.role\b'` 在 src 里零引用 ✓<br>· 改 `events` 表成本 —— `insert_events`/`PendingEvent`/`write_batch`/`EventRow` 分 2/7/3/7 处出现 ✓<br>· argon2 贵 —— 实测参数 `m=19456,t=2`(19 MB、2 轮),`api.rs:142` 注释记「约 20MB 内存 + 数十 ms CPU」✓<br>· prefix/secret 不可枚举 —— 16⁸ = 2³²、64 hex = 256 位,组合 2²⁸⁸ ✓<br>· 无 CORS —— 全仓 `grep CorsLayer\|cors` 零命中 ✓<br>· 明文不可回读 —— 只存 `key_hash` ✓<br>**发现 1 处遗漏 + 1 处过时**:§5 缺两条本应列入的不做项,且 H-3 引用的「§5 的 8 项」与实际项数不符 | 修正:① §5 补 2 条 —— **「不带 `rhk_` 前缀的 API Key」**(会让两个解码器互相污染)与**「改 scope 的 PUT 接口」**(见 F-2b);② argon2 那条补上实测参数与源码位置;③ H-3 的项数由 8 改为**实数的 10** |
| 第 39 轮 | 只读验证 §6.1 前置基线与 §7 自查清单**能否真的执行**,发现 **3 处**:<br>**① §6.1 的 curl 用了 `$PORT` 但全文从未定义该变量** —— `set -u` 下直接 `unbound variable`<br>**② §7 的 curl 写成 `127.0.0.1:PORT`(字面量)** —— curl 把 `PORT` 当主机名,实测 `http_code=000`、退出码 3;更糟的是**它不报错**,看起来像执行了<br>**③ §7 的 `std::os` 门控自查会命中注释** —— 实测 13 处命中里 6 处是注释,噪音大 | 修正:① §6.1 补 `PORT=8080` 定义,curl URL 加引号;② §7 同样;并补「`000` 的两种含义靠退出码区分(7=连接失败、3=URL 格式错)」<br>③ 命令加 `| grep -v ':\\s*//'` 过滤注释(13 → 7 处真实代码),并注明 `splice.rs` 的 cfg 门控在 `lib.rs` 的 `pub mod splice;` 上而非文件内<br>**顺带验证**:§7 的其余命令全部可执行(fmt check exit=0、require_auth 计数 33、`err(40[13]` 零命中、route_layer grep 正常) |
| 第 40 轮 | **逐块实跑全文 8 个 bash 代码块**。结果:块1(三道门)语法 OK + fmt 单独通过;块3(SSOT 校验集)**7/7 全跑通、退出码 0**;块5(H-2 e2e)语法 OK,`API_PORT`/`TOKEN` 由 `:150`/`:298` 提供(文中已说明,不算缺失);块7/块8 语法 OK。**发现 2 处**,且是同一类问题:<br>**① §0 第 2 条的 `std::os` 命令仍是旧版**(§7 已改进但 §0 没同步),且说明写"必须有 `#[cfg(unix)]` 门控"**不准确** —— `splice.rs` 的 import 无门控,其 `pub mod splice;` 用的是 `#[cfg(target_os = "linux")]`<br>**② §7 的命令没有标注"预期退出码"** —— 实测 `grep route_layer` 在包 E **实施前**返回 1(正常),curl 在服务未起时 `http_code=000`/退出码 7(也正常),但原文没说明,容易被误判成失败 | 修正:① §0 第 2 条与 §7 对齐(加注释过滤),并把门控要求改为"行内 `cfg(unix)`/`cfg(target_os)` **或所在模块被门控**"<br>② §7 补两处退出码说明:grep 返回 1 是"还没实施"的正常态;curl 的 `000`+退出码 7 表示连不上,不是命令写错 |
| 第 41 轮 | 换维度:验证任务书对**项目现有代码**的断言。结果 **8 处行号区间的内容全部相符**(`16-69` router / `362-408` require_auth / `342-359` jwt_engine / `385-400` epoch 守卫 / `296-317` mint_jwt / `100-110` err+unauthorized / `443` roles 硬编码 / `141-143` 限速注释);3 条 import 断言准确(`:9` 只有 get/post/put、`:7` 有 HeaderMap、`:6` 有 ConnectInfo);依赖断言准确(`ring` 确实没有);**发现 1 处**:<br>**D-2 给了两种 `scope` 放置方案却没说明二者引用写法不同** —— `api.rs` 内 `pub mod scope` 用短路径 `scope::X`,独立 `scope.rs` 必须 `crate::scope::X`。而 E-3 骨架通篇用短路径(12 处),**隐含绑定方案 A**,读者若选方案 B 会得 `E0433` | 修正:D-2 明确「统一放在 `api.rs` 内的 `pub mod scope`」,用表格对比两种方案的引用写法并标出采用哪个;加警告「两种不能混用,选一种贯彻到底」<br>**验证**:E-3 骨架里 12 处 `scope::` 全为短路径、零 `crate::scope::` 混用,与方案 A 一致 |
| 第 42 轮 | 回到**原始需求**验:「让控制台每个 api 都能用 api key 访问」。逐条核对 24 条路由的 Key 可达性,发现 **1 处需求偏差未论证**:`session` 组的 6 条里,只有 `logout`/`password`(有副作用)、`api-keys*`(防提权)有硬理由;**`user_info`/`auth_codes`/`menu_all` 三条是只读、无副作用、返回静态或 UI 相关内容的接口,技术上完全可以让 Key 访问**,文档却把它们一并归为 session-only 且未给理由 | 修正:① §2.2 补**逐条理由表**(6 条路由各自为什么),其中明确写出「三条技术上可做、不做的原因是**语义**(服务于浏览器 UI,返回体围绕'当前登录用户'构造,对 Key 无对应物)」② 指出若将来需要「Key 查自己身份」,应**新增** `GET /api/api-keys/self` 而非复用 `user_info`<br>③ §5 登记这条偏离;④ H-3 项数由 10 同步为 **11** |
| 第 43 轮 | 继续从需求侧验「权限粒度是否够用」。核对 18 条 Key 可达路由 × 12 个 scope 位,发现 **1 处未申明的规则前提**:<br>§2.4 的「GET/HEAD=读、其余=写」**依赖 REST 语义假设**,但文档只给了规则没给前提。逐条核对现有接口后确认**该假设当前成立**(无「GET 有副作用」、`rotate-secret` 虽是 POST 但确实 UPDATE 库所以归写正确),但**将来新增接口可能破坏它** | 修正:§2.4 补「规则的前提假设」段 ——<br>· 说明依据是 RFC 7231 §4.2.1,并列出三项核对结果(全部符合)<br>· 明确列出 `rotate-secret` 这类「POST 但看起来像读」的接口为何仍归写(会 UPDATE 库)<br>· 给出**新增接口时的复核要求**与两条应对方案(改方法 / 加按路径白名单,不改全局规则)<br>**实测**:同一路径 `GET+PUT` 可正确分权 —— 只有 `CONFIG_R` 的 key 调 `/api/server/config` 得 GET 200 / PUT 403 |
| 第 44 轮 | **整体拼装验证**:把全文 49 个 rust 块按类型分类(10 个完整实现 / 30 个声明常量 / 9 个示意片段),再**按文件归组**(`apikey.rs` / `state.rs` / `api.rs`)拼成真实工程。<br>**结果全部通过**:<br>· 拼装后 `cargo build` **0 错误**<br>· `RUSTFLAGS="-D warnings"` 下仍 **0 错误**<br>· 复现第 30 轮发现的「提交 4 中间态 dead_code」:补上 `resolve_principal`/`verify_api_key` 后出现 2 条 `never used`,加 `-D warnings` 后变 **3 个 error** —— 与 §4.1 记录一致<br>· 验证 §4.1 的**方案 A**(加 `#[allow(dead_code)]`)确实有效 → 0 error | 无需修改。本轮为**零发现验证轮** |
| 第 45 轮 | 上一轮只验了"能编译",本轮验"**能工作**" —— 把拼装后的工程**实际运行**,逐包验证运行时行为:<br>**B 包(apikey)**:明文长度 77、`parse` 返回含 `rhk_` 的完整串、hash 一致、错 secret 不一致、`ct_eq` 长度不等为 false、1000 次生成无重复 —— **全部符合**<br>**C 包(缓存)**:miss 查库并回填(条目 1)、hit 不查库(仍 1)、`invalidate` 后重查、库删+invalidate 后返回 None、1000 次 touch 只落库 1 次 —— **全部符合**<br>**D+E 包(判权)**:12 条用例 —— 无 token 401、空 `Bearer ` 401、非 Bearer 前缀 401、JWT 200、Key 只读读 200、Key 只读写 403、**scopes=0 403(权限绕过防住)**、Key 调 session 403、JWT 调 session 200、Key GET config 200 / PUT 403、未知路径 404 —— **12/12 通过** | 无需修改。本轮为**零发现验证轮**。过程中一次失败(`scopes=0` 得 200)经查是**我的测试桩缺陷**(用 `contains("ro")` 判只读,而字串 `rhk_ze**ro**` 恰好含 "ro"),改成 `ends_with("_ro")` 后 12/12 |
| 第 46 轮 | 验证**唯一尚未跑过运行时的 F 包**(管理接口)。把 `create/list/rotate/delete` 四个 handler 与 `guard` 拼成可运行工程,跑完整生命周期 **15 条断言**:<br>创建 key(200)、返回明文含 `rhk_`、preset→scopes 映射正确、`list` 含 `created_by`、`list` 不含 `key_hash` 与明文、key 可读 clients、**key 不能建 key(防提权 403)**、rotate 200、rotate 换新明文、**rotate 后旧 key 401**、吊销 200、**吊销后 401**、**软删后 list 仍可见且 `enabled=false`** —— **15/15 通过** | 无需修改。本轮为**零发现验证轮**。过程中两次失败(`key 可读 clients` 得 403)经查均为**我的测试桩缺陷**:先是用 `contains("ro")` 判只读,后是替换脚本把 clients 组的 `session_only` 误写成 `true`;修正后 15/15 |










































