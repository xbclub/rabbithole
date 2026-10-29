# `rhctl` CLI 控制端任务书(2026-10)

> 目标:为 API Key 认证落地后的管理 REST API 制作命令行控制端 **`rhctl`**
> (Go + cobra),让脚本 / CI 用一把 API Key 完成 clients / services /
> visitors / stats / events / config / certs 七组管理操作。
>
> 硬约束(用户指定):
> 1. **语言 Go,框架 cobra**;
> 2. **禁止手改 `go.mod`/`go.sum`** —— 一切模块变更只能由
>    `go mod init` / `go get` / `go mod tidy` 产生;
> 3. 完成判据 = **连续三轮审计零 finding**(协议见 §5),接口覆盖以
>    §3 覆盖契约为准。
>
> 本文件自包含,是执行依据。行号为 **2026-10-29 快照**,动手前先读当前
> 代码核实;与现状不符时按 AGENTS.md 记录反证,不要硬改。
>
> 状态标记:`[ ]` 待办 / `[x]` 完成(附提交 hash)/ `[W]` 放弃(写明反证)。

---

## 0. 执行约定(每个任务包都必须遵守)

1. **Go 三道门**(每个提交前,类比 Rust 三道门):

   ```bash
   cd cli/rhctl
   test -z "$(gofmt -l .)"     # 格式:无输出即通过
   go vet ./...                # 静态检查
   go test ./...               # 全量测试
   ```

2. **`go.mod` 纪律(用户硬约束,审计 C 项直接查)**:

   | 允许 | 禁止 |
   |---|---|
   | `go mod init <module>` 创建 | 用编辑器改 `go.mod` / `go.sum` 的任何一行 |
   | `go get github.com/spf13/cobra@…` 加依赖 | 手写 `require` 块、`replace` 指令 |
   | `go mod tidy` 清理 | `go mod vendor`(不 vendor) |

   - **直接依赖白名单只有 `github.com/spf13/cobra` 一个**;它的传递依赖
     (`spf13/pflag`、`inconshreveable/mousetrap` 等)由 `go get` 自动带入,
     不算违规。
   - **不引入 viper**:配置面只有 6 个 flag + 单个 profile JSON(§2.2),
     分层语义(优先级/`--server` 别名例外)是本任务自定义规则,viper 的
     AutomaticEnv/BindPFlag 有自己的优先级坑反而要绕;依赖树会从 cobra
     的 2 个传递依赖膨胀到 ~8-10 个(其中 TOML/YAML 等解析器一个不用);
     且默认静默忽略未知键、WriteConfig 语义固定,与「严格解析 + 原子写」
     要求冲突。通用配置文件需求出现时再评估(§7 第 4 条)。
   - **不引入 testify**(断言用标准库 `testing`)。
   - 触碰 `go.mod`/`go.sum` 的提交,提交信息里**写明是哪条 go 命令**改的。

3. **服务端零改动**。本任务不改 `crates/`、`migrations/`、`console/`、
   `tests/`、`.github/`(CI 除外,见包 G)。CLI 与服务端之间只存在 HTTP
   契约;执行中发现服务端缺陷 → 停下登记到本文件,不顺手改。
   这条是审计 D 项的判定基础:**Rust 侧 diff 为零**。

4. **交叉编译自查**(类比仓库的 Windows 编译检查惯例,Go 零成本):

   ```bash
   cd cli/rhctl
   GOOS=windows GOARCH=amd64 go build ./...
   GOOS=linux   GOARCH=amd64 go build ./...
   GOOS=linux   GOARCH=arm64 go build ./...
   ```

   代码里不得出现平台专属 API(`syscall` 直接调用、`os/exec` 等);
   HTTP 全走标准库 `net/http`,天然可移植。

5. **一个任务包一个提交**;提交信息写「为什么」不写「做了什么」;
   只 `git add` 自己动过的文件。

6. **凭据纪律(凭证侧唯一强制行为)**:Authorization 头与 key 值不得出现
   在任何日志、错误消息、debug 输出与常规命令输出中(一次性 secret 的
   **响应展示**除外,见 §2.6)—— 由包 B / 包 I 的行为测试钉住,
   不做源码 grep。

7. **时间口径**:服务端全部时间戳是毫秒(`now_ms()`)。`--output json`
   原样输出毫秒整数;table 输出转本地时区可读格式。

8. **CLI 不做业务校验**。名称长度、端口范围、配额格式等服务端是权威
   校验者(`api.rs` 各 handler 已有 1002 系列校验),CLI 只做:
   - 必选参数缺失 → 本地 exit 2;
   - 其余原样发出,把服务端的 `{code, message}` 透给用户。
   这样 CLI 永远不会与服务端校验规则漂移。

9. **命令契约测试统一命名 `TestContract_<组>_<命令>`**(如
   `TestContract_clients_create`,包 C–F 每命令一个)。审计 A 项靠
   `go test ./... -run TestContract -v` 过滤出全部契约测试与 §3.1
   对账 —— 该前缀**只**给命令契约测试用,包 B 的信封/错误分类测试用
   `TestEnvelope_*` 前缀,不进对账。

---

## 1. 现状事实(动手前逐条核实)

### 1.0 单一事实源(SSOT)—— 改数字只改这里

| 事实 | 权威值 | 校验方式 |
|---|---|---|
| 管理路由总数 | **27 路径 / 38 操作**(方法×路径) | §1.0 校验命令第 1 条 |
| Key 可达(在 CLI 范围内) | **18 路径 / 28 操作** | 同上,排除 `/api/(auth\|user\|menu\|api-keys)` 前缀 |
| session-only(Key 整组被拒) | 8 路径 / 9 操作 | `api.rs` router 的 `session` 组 |
| 公开(无鉴权) | 1 路径 / 1 操作(`POST /api/auth/login`) | `public` 组 |
| 信封字段 | `{code, data, message}`;成功 `code=0, message="ok"` | `api.rs` `OkEnvelope` |
| 业务错误 HTTP 状态 | **200**(err() 不带状态码) | §1.2 表第 2 行 |
| 未知 `/api/*` 路径 | **HTTP 200 + body code=404** | e2e 实测,提交 `9252f14` |
| 管理 API 默认端口 | **8080**,绑定 127.0.0.1 | `config.rs` `WebConfig::default()` |
| scope 位 | 见 §1.4(READONLY=5461 / OPS=5503 / FULL=16383) | `api.rs` `pub mod scope` |
| go 工具链(开发机) | go1.27.3 darwin/arm64 | `go version` |
| cobra 版本 | **v1.10.2**(传递:pflag v1.0.9、mousetrap v1.1.0) | A-2 `go get …@latest` 解析值,2026-10-29 |

**校验命令集**(第 1 条 2026-10-29 实测可用;注意正则要容忍链式调用的
**尾逗号**,否则会漏掉多行 `.route(` 块、少算 6 条路径):

```bash
# 1) 路由全景:路径数 / 操作数 / Key 可达数
python3 - <<'EOF'
import re
s=open('crates/rh-server/src/api.rs').read()
b=s[s.index('pub fn router'):s.index('public\n        .merge')]
routes=[]
for m in re.finditer(r'\.route\(\s*"(/api/[^"]+)",\s*(.+?),?\s*\)\s*\n', b, re.S):
    for meth, h in re.findall(r'(get|post|put|delete)\(([a-z_]+)\)', m.group(2)):
        routes.append((meth.upper(), m.group(1), h))
paths=sorted(set(r[1] for r in routes))
keyr=[r for r in routes if not re.match(r'/api/(auth|user|menu|api-keys)', r[1])]
print(f'paths={len(paths)} ops={len(routes)}')
print(f'key-reachable paths={len(set(r[1] for r in keyr))} ops={len(keyr)}')
for p in paths: print(' ', p, [r[0] for r in routes if r[1]==p])
EOF

# 2) 业务错误不带状态码(err 默认 200)
grep -n 'pub fn err(' -A 3 crates/rh-server/src/api.rs   # 无 StatusCode

# 3) 默认端口
grep -n 'impl Default for WebConfig' -A 10 crates/rh-server/src/config.rs
```

### 1.1 路由全景(27 路径;2026-10-29 从 router 块机械提取)

| 组 | scope 要求 | 路径(方法) |
|---|---|---|
| public | 无 | `/api/auth/login`(POST) |
| session | **整组拒绝 Key** | `/api/auth/logout`(POST)、`/api/auth/password`(POST)、`/api/user/info`(GET)、`/api/auth/codes`(GET)、`/api/menu/all`(GET)、`/api/api-keys`(GET,POST)、`/api/api-keys/{id}`(DELETE)、`/api/api-keys/{id}/rotate`(POST) |
| clients | CLIENTS_R \| CLIENTS_W | `/api/clients`(GET,POST)、`/api/clients/{id}`(PUT,DELETE)、`/api/clients/{id}/rotate-secret`(POST) |
| services | SERVICES_R \| SERVICES_W | `/api/services`(GET,POST)、`/api/services/batch`(POST)、`/api/services/{id}`(GET,PUT,DELETE)、`/api/services/{id}/enable`(POST)、`/api/services/{id}/disable`(POST)、`/api/services/{id}/rotate-secret`(POST) |
| visitors | VISITORS_R \| VISITORS_W | `/api/visitors`(GET,POST)、`/api/visitors/{id}`(PUT,DELETE)、`/api/visitors/{id}/enable`(POST)、`/api/visitors/{id}/disable`(POST) |
| stats | STATS_R | `/api/stats/overview`(GET) |
| events | EVENTS_R | `/api/events`(GET) |
| config | CONFIG_R \| CONFIG_W | `/api/server/config`(GET,PUT) |
| certs | CERTS_R \| CERTS_W | `/api/certs`(GET,POST)、`/api/certs/{id}`(PUT,DELETE) |

**判权方向**:GET/HEAD = 读位,其余 = 写位;fail-closed(`allowed==0`
也拒绝)。CLI 不解释判权,403 的 message 原样透出。

### 1.2 信封与错误语义(客户端层必须逐行覆盖,包 B 的测试清单)

| 情形 | HTTP 状态 | body | CLI 行为 |
|---|---|---|---|
| 成功 | 200 | `{"code":0,"data":…,"message":"ok"}` | 解析 data,exit 0 |
| 业务错误 | **200** | `{"code":≠0,"message":…,"data":null}` | stderr `api 错误 <code>: <message>`,exit 1 |
| 未认证(无/坏 Key) | 401 | `{"code":401,…}` | stderr 认证失败提示,exit 1 |
| 权限不足 / session-only | 403 | `{"code":403,"message":"API Key 权限不足"…}` | 同上,exit 1 |
| 未知 `/api/*` 路径 | **200** | `{"code":404,…}` | 理论上 CLI 触发不到(路径都是常量),解码层仍按 code 分支 |
| 网络层失败 | — | — | stderr 连接错误,exit 1 |
| 本地错误(参数/文件/凭据缺失) | — | — | exit 2 |

⚠ **最大的坑**:业务错误是 HTTP 200 + `code≠0`(`err()` 不带
`StatusCode`,`api.rs` 的 `err` 实现)。**不能**只看 HTTP 状态判断成败,
curl 直觉在这里是反的。另注意 axum 的 Json 拒收(如 config put 传未知
字段触发 `deny_unknown_fields` 反序列化失败)返回的是 **422 + 非 JSON
body**,客户端解码必须容忍「body 不是信封」的情形,回退显示状态码文本。

### 1.3 各资源负载形状(锚点 = `api.rs` handler / 结构体名,不用行号)

**clients**
- `CreateClientReq`:`{name, remark?, rate_limit?, max_conns?, max_services?, flow_limit?}`;
  响应 `create_client` → `{id, name, remark, secret}`(**secret 仅本次返回**,64 hex)。
- `UpdateClientReq`(全部可选):`{enabled?, name?, remark?, rate_limit?, max_conns?, max_services?, flow_limit?}`。
  ⚠ **clients 没有独立的 enable/disable 路由**,启停就是 `PUT {enabled: true|false}`。
- `rotate_secret` → `{secret}`(旧会话被踢线重认证)。
- `delete_client` → 删除级联其全部规则;`ClientListRow` 20 字段(id/name/enabled/online/…/bytes_in/bytes_out/conns_current/四项配额)。

**services**
- create / update 的 body 是**整份 `ProxySpec` JSON**(定义在
  `crates/rh-protocol/src/proxy.rs`,`#[derive(Deserialize)]` 直接做
  handler 参数)。CLI 透传,不复制字段表(见 §2.5)。
- `list_services` 查询参数 `?client_id=`(可选);行结构 `ServiceListRow`
  22 字段;**列表不带整份 spec**(`custom_domains`/`subdomain` 是投影列)。
- `get_service` 单条**带 spec**(编辑回读用),但 `secret_key` 已抹掉
  (读接口不回显密钥;编辑回写时留空 = 不修改)。
- `batch_services` body `{ids: [i64], action: "enable"|"disable"|"delete"}`。
- `rotate_service_secret` → `{rotated, secret, revision}`;仅 STCP/SUDP/XTCP
  规则支持(其余 4004)。
- `create_service` → `{id, bind_port, port_auto, revision}`(客户端离线时
  的 deferred 分支为 `{id, bind_port, port_auto, deferred:true}`)。
  ⚠ **响应不含 secret**:STCP 族的密钥在创建时服务端生成并存库,但
  **不回显** —— 想拿到明文只能 rotate 一次(§2.6 因此只列三个命令)。
- `delete_service` / `enable` / `disable` 响应含各自状态字段,以实测为准。

**visitors**
- create body:`CreateVisitorReq = {client_id, …VisitorSpec 扁平化}`
  (`#[serde(flatten)]`)—— **body 是一层的**,`client_id` 与 spec 字段平铺。
- update body:**纯 `VisitorSpec`**(无 client_id 包装;`secret_key` 服务端
  强制清空、入参忽略)。
- `VisitorListRow` 每行内嵌整份 spec。
- `target_proxy_name` 是目标规则名,服务端跨客户端解析归属(优先在线)。

**stats / events**
- `stats_overview` → `{clients_total, clients_online, services_total, services_running, conns_current, bytes_in, bytes_out}`。
- `list_events` 查询 `?limit=&level=`;limit 缺省 100、服务端 clamp 1..500;
  行 `{id, ts, level, source, message, detail}`(ts 毫秒)。

**config**
- `get_server_config` → `{config, hot, authorized_keys, alpn}`(config 中
  `web.jwt_secret` 已脱敏为 null)。
- `put_server_config` body `ServerConfigUpdate`:六段可选
  `{ports?, log?, ssh_gateway?, metrics?, limits?, authorized_keys?, tls?}`,
  **`deny_unknown_fields`**(未知字段整体拒收 422)。

**certs**
- `CertReq`:`{name, cert_pem, key_pem, domains?}`(PEM 全文内嵌;domains
  缺省取证书 SAN)。create → `{id, fingerprint}`;update → `{updated:true}`;
  delete → `{deleted:true}`;`CertListRow` 6 字段(不含 PEM 本体)。

### 1.4 鉴权与 scope(引用,不复制)

- 凭据:`Authorization: Bearer rhk_<8hex>_<64hex>`;完整 bearer 串的
  sha256 落库比对。
- scope 位定义与 READONLY/OPS/FULL 见 `api.rs` `pub mod scope` 与
  `docs/api-key-auth-tasklist-2026-10.md` §2.3(**那份是权威,本文件不复制**)。
- CLI 对 scope 的唯一展示需求:错误 message 透出,不本地位运算。

---

## 2. 目标形态

### 2.1 命名与位置

| 项 | 定案 | 理由 |
|---|---|---|
| 二进制名 | **`rhctl`** | 对齐 `rh-server`/`rh-client` 的 `rh-` 前缀;kubectl 式后缀 |
| 目录 | **`cli/rhctl/`** | 与 `benchmark/loadgen` 平级,不进 cargo workspace |
| module 路径 | `github.com/xbclub/rabbithole/cli/rhctl` | 与 git remote 一致(loadgen 无依赖所以路径随意,这里不行) |
| go 指令版本 | `go mod init` 产生的默认值(开发机 1.27) | 不手改 go.mod,也就不手写 go 行 |
| 依赖 | cobra(唯一直接依赖) | 用户指定;其余全标准库 |

```
cli/rhctl/
  go.mod / go.sum          ← 只许 go 命令写
  README.md                ← 安装/凭据/示例(包 G)
  cmd/rhctl/main.go        ← 仅 main():os.Exit(cli.Execute())
  internal/cli/root.go     ← cobra 根命令 + persistent flags + 版本
  internal/cli/clients.go  services.go  visitors.go
  internal/cli/stats.go    events.go    config.go    certs.go
  internal/cli/output.go   ← table(tabwriter)/json 双输出
  internal/cli/profile.go  ← 多管理端 profile(包 I,读写 profiles.json)
  internal/cli/*_test.go
  internal/api/client.go   ← HTTP 客户端 + 信封解码 + 错误分类
  internal/api/types.go    ← 请求体结构(§3 表中「结构化」的那些)
  internal/api/client_test.go
```

构建产物不入库:`.gitignore` 追加 `/cli/rhctl/rhctl`。

### 2.2 凭据与连接(persistent flags,全部子命令可用)

| 来源 | flag | 环境变量 | 默认 |
|---|---|---|---|
| 服务地址 | `--server` / `-s`(URL 或 profile 别名) | `RH_SERVER` | `http://127.0.0.1:8080`(服务端默认口) |
| API Key | `--key` / `-k` | `RH_API_KEY` | 无 |
| profile 名 | `--profile` | `RH_PROFILE` | 无(配置文件有 `current` 时用它) |
| 输出格式 | `--output` / `-o` | — | `table`(`json` 为机器可读输出) |
| 请求超时 | `--timeout` | — | `30s`(`http.Client.Timeout`) |
| 跳过 TLS 校验 | `--insecure` | — | false(自签场景) |
| 版本 | `--version` | — | `dev`(`-ldflags -X` 注入,发版集成不做,见 §7 不做清单) |

取值优先级:flag > 环境变量 > profile(包 I)> 默认;server 与 key
**各自独立**取最高优先级(例:用 prod profile 的 server、`--key` 临时换凭据;
唯一例外:`--server <别名>` 生效整个 profile,见下方「别名访问」)。
**调远端命令时 key 缺失** → stderr 明确提示(列出 flag/env/profile 三种来源
写法),exit 2 —— 不发起 HTTP。server 地址允许 `https://`(配 `--insecure`)。

**凭证存储(包 I,对齐 kubectl/aws 的通行分层)**:

| 优先级 | 来源 | 场景 | 泄漏面 |
|---|---|---|---|
| 1 | `--key` / `--server` | 一次性覆盖 | ⚠ shell history 与 `ps` 可见,README 必须警示 |
| 2 | `RH_API_KEY` / `RH_SERVER` | CI、secret manager 注入 | 进程环境(`/proc/*/environ`、误 dump) |
| 3 | **profile 文件(交互式默认)** | 日常使用,单机与多机同一机制 | 仅文件系统权限 |
| 4 | 默认 `http://127.0.0.1:8080` | 本机即用 | 无凭据 |

- 存储:`~/.config/rhctl/profiles.json`(尊重 `XDG_CONFIG_HOME`,
  `RHCTL_CONFIG` 强制覆盖),加载用 `json.Decoder` + `DisallowUnknownFields`
  (**拼错的键 exit 2,不静默忽略**)。结构:

  ```json
  {
    "current": "prod",
    "profiles": {
      "prod":   { "server": "http://172.16.1.6:8080",   "key": "rhk_…",
                  "insecure": false, "aliases": ["p"] },
      "ci-lab": { "server": "https://172.16.1.17:8443", "key": "rhk_…",
                  "insecure": true,  "aliases": ["lab"] }
    }
  }
  ```

  字段:`server`/`key` 必填;`insecure` 可选(默认 false,自签 TLS);
  `aliases` 可选(访问别名);`current` 必须是规范名(不接受别名)。
- **不做权限强制(定案,用户裁定)**:加载不检查权限、不警告、不拒载
  (先前两版 —— 拒载 / 警告 —— 均废弃)。凭证侧**唯一保留的强制行为**
  就是输出脱敏(见下条)。文件创建用 0600 只是默认创建模式,不是检查。
  **写入原子化**(临时文件 + rename,防中途崩溃留下半截文件)。
- 由 `rhctl profile add/use/remove` 命令化管理;`add` 支持 `--key-stdin`
  (防 key 进 shell history)。`list` 的 key 列**掩码**(仅 prefix 前
  8 hex + `…`):防的**不是**读文件(文件主人 `cat` 一下全都有),是
  **终端输出被粘贴/截图进工单和群聊** —— kubectl `config view` 默认
  脱敏 token/cert 是同一理由。想看完整 key 直接 cat 文件,CLI 不提供
  reveal。
- 选 JSON 而非 TOML / OS keyring:标准库即可解析、零平台差异,守住
  「唯一直接依赖 cobra」(§0 第 2 条);keyring 登记在 §7,真需求再评估。
- 每台 server 独立签发自己的 key(服务端只存 sha256 哈希、无导入接口,
  **同一把 key 不能跨 server 复用**),多管理端 = N 对 (server, key)
  的 profile,`--profile`/`RH_PROFILE`/`current` 三级选择。
- **别名访问**(ssh Host 模式):profile 名与全部别名共享**一个全局唯一
  命名空间**(冲突加载即 exit 2),解析一律**精确匹配**(不做前缀/模糊,
  可预测性优先):
  - `--profile` / `RH_PROFILE` 接受规范名**或**别名;
  - `--server` 的值带 `http(s)://` 前缀 → 纯 URL 覆盖;**不带前缀 →
    按名/别名解析为整个 profile**(server+key+insecure 一并生效,等价
    临时切机;`--key`/`--insecure` 仍可再覆盖)—— 对齐 `ssh <别名>`
    的直觉。解析不到 → exit 2 并列出可用名。

### 2.3 命令树(覆盖 §3 全部 28 个操作)

```
rhctl
├── clients
│   ├── list
│   ├── create  --name --remark --rate-limit --max-conns --max-services --flow-limit
│   ├── update  <id> [--enabled|--disabled] [--name …(同 create 的可选项)]
│   ├── rotate-secret <id>
│   └── delete  <id>
├── services
│   ├── list    [--client-id]
│   ├── get     <id>
│   ├── create  --file <spec.json>   (ProxySpec,§2.5)
│   ├── update  <id> --file <spec.json>
│   ├── enable  <id>
│   ├── disable <id>
│   ├── rotate-secret <id>
│   ├── batch   --ids 1,2,3 --action enable|disable|delete
│   └── delete  <id>
├── visitors
│   ├── list    [--client-id]
│   ├── create  --client-id <cid> --file <visitor.json>
│   ├── update  <id> --file <visitor.json>
│   ├── enable  <id>
│   ├── disable <id>
│   └── delete  <id>
├── stats
│   └── overview
├── events
│   └── list    [--limit] [--level info|warn|error]
├── config
│   ├── get
│   └── put     --file <update.json>   (ServerConfigUpdate)
├── certs
│   ├── list
│   ├── create  --name --cert-file <pem> --key-file <pem> [--domains a,b]
│   ├── update  <id> --name --cert-file <pem> --key-file <pem> [--domains a,b]
│   └── delete  <id>
└── profile                ← 本地命令,不访问 REST API(不进 §3 覆盖审计)
    ├── list               # key 掩码:仅 prefix(前 8 hex)+ …,附别名列
    ├── add    <name> --server --key [--alias a]… [--key-stdin] [--force]
    ├── use    <name>      # 写 current(name 可为别名,写入规范名)
    └── remove <name>      # name 可为别名;删的是 current 则同步清空
```

命名规则:资源名用 API 路径的复数形式(clients 不是 client);动词小写。
`--file` 统一支持 `-`(stdin),便于 `| rhctl services update 3 --file -`。
内置命令别名(cobra 原生 `Aliases`,零成本,单测断言注册):
`ls`≡`list`、`rm`≡`delete`、`rotate`≡`rotate-secret`、`ov`≡`overview`。
仅此四组;不做用户自定义别名机制(§7 第 6 条)。

### 2.4 输出与退出码

- **table**(默认):`text/tabwriter`,表头小写;bytes 人类可读
  (MiB/GiB);毫秒时间戳转本地时区 `YYYY-MM-DD HH:MM:SS`。每命令选
  关键列(见 §3 映射表「table 列」),完整字段看 `--output json`。
- **json**:信封的 `data` 部分**原样**输出(不重组、不丢字段),
  便于 `| jq`。一次性 secret 字段照样输出(§2.6)。
- 退出码:`0` 成功;`1` 远端错误(业务 code≠0 / 401 / 403 / 网络);
  `2` 本地错误(用法、文件读不到、key 缺失)。
- 数据走 stdout,错误与提示走 stderr;**成功的诊断输出不得污染 stdout**
  (否则 `--output json` 管道会被破坏)。

### 2.5 spec 透传约定(services / visitors 的 create / update)

`ProxySpec`/`VisitorSpec` 是 20+ 字段、随协议演进的版本化结构,权威定义
在 `crates/rh-protocol/`。**CLI 不复制其字段表、不做字段校验**,一律
`--file`(或 stdin)读 JSON、原样放进请求体;本地只查「JSON 可解析」。

配套工作流(README 必须示例化):
```bash
rhctl services get 3 -o json | jq .spec > spec.json   # 回读(密钥已抹掉)
vim spec.json                                          # 编辑,密钥留空 = 不改
rhctl services update 3 --file spec.json
```

`visitors create` 的 body 是 `{client_id, …spec 扁平}`(§1.3):CLI 把
`--client-id` 与文件里的 spec 合并为单层 JSON 再发;`visitors update`
直接发文件内容(纯 VisitorSpec)。

### 2.6 一次性 secret 的展示规则

`clients create` / `clients rotate-secret` / `services rotate-secret` 的
响应里 secret **仅本次返回**,服务端只存哈希、无法找回。这些命令:
- table 输出**必须**完整显示 secret(这是用户拿到它的唯一机会);
- README 用显眼的框提示「保存 now,关闭即失」。
- 逆向红线:**传入的** key(--key/RH_API_KEY)永不回显(§0 第 6 条)。

---

## 3. 接口覆盖契约(审计 A 项的基线,SSOT)

> 审计「无接口遗漏」= 本表两半都成立:28 个在范围操作全有命令 + 契约测试;
> 10 个范围外操作全部在下方的登记清单里且理由成立。
> `profile` 组(包 I)是**本地命令**,不访问 REST API,不参与覆盖审计。

### 3.1 Key 可达 28 操作 → 命令映射

| # | 操作 | rhctl 命令 | 请求形状 | table 列(建议) |
|---|---|---|---|---|
| 1 | GET /api/clients | `clients list` | — | id name online enabled remark |
| 2 | POST /api/clients | `clients create` | 结构化(CreateClientReq) | id name secret |
| 3 | PUT /api/clients/{id} | `clients update` | 结构化(UpdateClientReq,全可选) | updated(响应即 `{updated:true}`) |
| 4 | DELETE /api/clients/{id} | `clients delete` | — | 结果行 |
| 5 | POST /api/clients/{id}/rotate-secret | `clients rotate-secret` | — | id secret |
| 6 | GET /api/services | `services list` | query client_id | id client name kind bind_port target status |
| 7 | POST /api/services | `services create` | spec 透传 | 返回体(含 bind_port 等) |
| 8 | POST /api/services/batch | `services batch` | {ids,action} | 逐 id 结果 |
| 9 | GET /api/services/{id} | `services get` | — | 列表列 + spec(json 输出才有) |
| 10 | PUT /api/services/{id} | `services update` | spec 透传 | 同 create |
| 11 | DELETE /api/services/{id} | `services delete` | — | 结果行 |
| 12 | POST /api/services/{id}/enable | `services enable` | — | 结果行 |
| 13 | POST /api/services/{id}/disable | `services disable` | — | 结果行 |
| 14 | POST /api/services/{id}/rotate-secret | `services rotate-secret` | — | id secret revision |
| 15 | GET /api/visitors | `visitors list` | query client_id | id client name kind bind_addr:port target |
| 16 | POST /api/visitors | `visitors create` | {client_id + spec 扁平} | id name bind |
| 17 | PUT /api/visitors/{id} | `visitors update` | spec 透传 | 结果行 |
| 18 | DELETE /api/visitors/{id} | `visitors delete` | — | 结果行 |
| 19 | POST /api/visitors/{id}/enable | `visitors enable` | — | 结果行 |
| 20 | POST /api/visitors/{id}/disable | `visitors disable` | — | 结果行 |
| 21 | GET /api/stats/overview | `stats overview` | — | 七字段一屏 |
| 22 | GET /api/events | `events list` | query limit/level | id time level source message |
| 23 | GET /api/server/config | `config get` | — | 关键段(config put 的镜像) |
| 24 | PUT /api/server/config | `config put` | 文件透传(ServerConfigUpdate) | applied 摘要 |
| 25 | GET /api/certs | `certs list` | — | id name domains not_after |
| 26 | POST /api/certs | `certs create` | 结构化(CertReq,PEM 从文件读入) | id fingerprint |
| 27 | PUT /api/certs/{id} | `certs update` | 同上 | 结果行 |
| 28 | DELETE /api/certs/{id} | `certs delete` | — | 结果行 |

### 3.2 范围外 10 操作(不做,登记理由)

| 操作 | 理由 |
|---|---|
| POST /api/auth/login | CLI 的凭据模型单一(API Key)。login 换的是 JWT,而 JWT 会牵动 `pwd_version` 吊销语义;Key 由控制台签发,CLI 不做密码登录(见 §7「不做清单」第 1 条) |
| POST /api/auth/logout | 服务端 session-only;且语义是吊销**该用户全部** JWT,CLI 触发只会误伤浏览器会话 |
| POST /api/auth/password | session-only;改密码属交互式控制台操作 |
| GET /api/user/info | session-only;返回体围绕浏览器会话用户构造,对 Key 无对应物(服务端设计如此) |
| GET /api/auth/codes | session-only;前端按钮权限码,UI 概念 |
| GET /api/menu/all | session-only;恒返回 `[]`,无信息量 |
| GET/POST /api/api-keys(2 op)、DELETE /api/api-keys/{id}、POST /api/api-keys/{id}/rotate | **整组拒绝 API Key(防横向提权)**:一把 ops key 若能建 key 就能给自己签 full 权限。Key 管理只能用控制台会话,这是服务端有意设计(`api.rs` session 组注释;`docs/api-key-auth-tasklist-2026-10.md` §2.2) |

范围 = Key 可达面,与 `docs/design.md` §7.1「双凭据」口径一致;
server 侧的完整不做清单在 `docs/not-supported.md` §14。

---

## 4. 任务包

> **执行顺序:A → B → C → D → E → F → I → G → H**(字母只是标签,
> 不代表次序)。I(凭证)先于 G(README 要写 profile 用法);H(审计)
> 必须最后 —— 在 A–I 全部验收、复选框回填之后才跑 §5 三轮。

### 包 A:工程骨架与依赖(P0)

- A-1 建目录 `cli/rhctl/`,执行 `go mod init github.com/xbclub/rabbithole/cli/rhctl`;
  `.gitignore` 追加 `/cli/rhctl/rhctl`。
- A-2 `go get github.com/spf13/cobra@latest`,把**解析出的版本号**回填到
  §1.0 SSOT 表(新增一行「cobra 版本」)。
- A-3 `cmd/rhctl/main.go` + `internal/cli/root.go`:根命令、§2.2 全部
  persistent flags、`--version`、flag>env>默认 的取值逻辑
  (包 I 落地后并入 profile 一级)。
- [x] 验收:`go build ./...` 过;`./rhctl --help` 列出全部 persistent
      flags 与用法、`./rhctl --version` 输出 `dev`(命令组在包 C–F 才注册,
      本阶段 `--help` 不含子命令属预期);§0 交叉编译三条全过。
      (「key 缺失 → exit 2 且不发 HTTP」的行为测试在包 B-2,此处不重复。)
      ✅ `9553a09`。补记:包 A 阶段根命令尚无子命令,cobra 对「不可运行且
      无子命令」的命令不输出 usage 段,--help 看不到 flags —— 根命令加了
      「无参打印帮助」的 RunE 解决,不算偏离验收(验收要的就是列出 flags)。

### 包 B:API 客户端层(P0)

- B-1 `internal/api/client.go`:
  `Do(ctx, method, path, body any, out any) error`,职责:
  注入 `Authorization: Bearer <key>` 与 `Content-Type: application/json`
  (有 body 时);按 §1.2 表解码分类(`APIError{Code,Message}` /
  `AuthError` / `NetworkError`);容忍非 JSON 错误 body(422 等);
  `http.Client{Timeout}` 从 `--timeout` 来。
- B-2 行为测试(httptest)覆盖 §1.2 表全部 7 行 + 「Authorization 头
  恰为 `Bearer rhk_…` 入参原样」「POST/PUT 带 Content-Type」
  「key 不出现在任何 error 字符串里」「key 缺失 → 本地错误、
  零 HTTP 请求(断言服务端零请求)」。命名 `TestEnvelope_*`
  (§0 第 9 条:不占用 TestContract 前缀)。
- B-3 `internal/cli/output.go`:table(tabwriter)/json(data 原样)双输出、
  bytes 人类可读、毫秒转本地时区 —— **放在本包**是因为 C–F 每个命令
  都要用,必须先于命令组落地(单测:json 输出与输入 data 逐字节一致)。
- [x] 验收:`go test ./internal/api/... ./internal/cli/...` 绿;§1.2 每行
      至少一个用例名出现在测试输出里。
      ✅ `fba5d6d`。§1.2 七行 ↔ 用例:成功 `TestEnvelope_OK`;业务错误
      `TestEnvelope_BusinessErrorIsHTTP200`;401 `TestEnvelope_Unauthorized401`;
      403 `TestEnvelope_Forbidden403`;未知路径
      `TestEnvelope_UnknownPathIs200WithCode404`;网络
      `TestEnvelope_NetworkErrorConnectionRefused`/`…Timeout`;本地
      (key 缺失) `TestEnvelope_MissingKeyNoHTTP`(cli 包,零请求断言)。
      另有 422 非 JSON(`TestEnvelope_NonJSONBody422`)、头精确/Content-Type/
      key 不泄漏(`TestEnvelope_AuthorizationHeaderExact`/
      `TestEnvelope_ContentTypeOnBodyMethods`/`TestEnvelope_KeyNeverInErrorStrings`)。

### 包 C:clients 组(P0,一提交)

§3.1 #1–#5 五命令,结构化请求体(`internal/api/types.go` 定义
CreateClientReq/UpdateClientReq,字段以 §1.3 为准);`update` 提供
`--enabled/--disabled` 布尔对(映射 `{enabled:bool}`);table/json 双输出;
每命令一个 httptest 契约测试(断言方法/路径/query/body JSON)。
- [x] 验收:5 测试绿;`clients create --name x` 的出站 body 与 §1.3
      CreateClientReq 逐字段一致(测试断言,不是人工目检)。
      ✅ `ec050a8`。TestContract_clients_{list,create,update,rotate_secret,delete}
      五契约测试;create 全字段/仅必填两种 body 断言;互斥对与缺
      --name 断言零请求。

### 包 D:services 组(P0,一提交)

§3.1 #6–#14 九命令;spec 透传按 §2.5(`--file`/`-`);`batch` 的
`--ids` 逗号分隔、`--action` 三选一(本地校验,非法 exit 2);
`get -o json` 输出含 spec。
- [x] 验收:9 契约测试绿;`services create --file` 出站 body 与文件
      内容逐字节一致(除末尾换行);`batch` 非法 action exit 2。
      ✅ `71df63f`。TestContract_services_{list,get,create,update,enable,
      disable,rotate_secret,batch,delete};文件与 stdin 两种透传都断言
      逐字节一致;非法 action / 非数字 id / 非 JSON 文件均为本地错误且
      断言零请求。

### 包 E:visitors + certs 组(P0,一提交)

§3.1 #15–#20 的 visitors 六命令 + #25–#28 的 certs 四命令。visitors create 的
「client_id 与 spec 扁平合并」逻辑单独抽函数并单测(嵌套 vs 扁平两种
输入都测);certs 的 `--cert-file/--key-file` 读文件填 `cert_pem/key_pem`,
`--domains` 逗号分隔可缺省。
- [x] 验收:10 契约测试绿;visitors create 出站 body 是**单层扁平**
      JSON(测试断言无嵌套 `spec` 键)。
      ✅ `b20b035`。TestContract_visitors_{list,create,update,enable,disable,
      delete} + TestContract_certs_{list,create,update,delete};合并函数
      `mergeVisitorCreate` 单测覆盖嵌套({spec:…} 包装,--client-id 优先)
      与扁平两种输入;缺 PEM 文件本地错误零请求。

### 包 F:stats / events / config 组(P0,一提交)

§3.1 #21–#24(output 层已在包 B-3 落地,本组只写命令);`config put`
文件透传,README 提示 `deny_unknown_fields` 语义(未知字段会被整单
拒收,先 `config get` 对照)。
- [x] 验收:4 契约测试绿;`events list --limit 0` 不本地拦截
      (服务端 clamp 是权威,CLI 只透传——测试断言 query 原样发出);
      json 输出与 httptest 返回的 data 逐字节一致;
      内置别名单测:`ls`/`rm`/`rotate`/`ov` 解析到对应主命令。
      ✅ `3c1db64`。TestContract_{stats_overview,events_list,config_get,
      config_put};--limit 0 断言 query 恰为 `level=warn&limit=0`;
      config get 的 json 子测试用带不规则空白的服务端原文断言逐字节一致;
      TestAlias_Builtin(12 组)+ TestAlias_EndToEnd。至此 TestContract
      共 28 个,与 §3.1 的 28 个操作一一对应。

### 包 I:凭证存储与多管理端 profile(P1,标准 CLI 凭证设计)

> 对齐 kubectl(kubeconfig)/ aws(profiles)的通行做法:凭证**受控落盘**
> 而非不落盘 —— `--key` 走 flag 的泄漏面(shell history、`ps`)其实比
> 0600 文件更大。单管理端与多管理端走**同一机制**(一个 profile 也是
> profile),不再有「单机=flag/env、多机=profile」的分裂。

- I-1 `internal/cli/profile.go`:读写 `~/.config/rhctl/profiles.json`
  (尊重 `XDG_CONFIG_HOME`,`RHCTL_CONFIG` 强制覆盖)。结构见 §2.2;
  加载用 `DisallowUnknownFields`(拼错的键 exit 2)。**不做权限检查**
  (§2.2 定案:不警告、不拒载;创建默认 0600 只是创建模式);
  **写入原子化**(临时文件 + rename)。
- I-2 别名与命名空间(§2.2 的「别名访问」条):**profile 名与全部别名
  共享一个命名空间,全局唯一**,冲突加载即 exit 2;`current` 必须是规范名;
  解析一律精确匹配。`--server` 与 `RH_SERVER` 同一解析:值带
  `http(s)://` 前缀 → 纯 URL;否则按名/别名解析为**整个 profile**
  (server+key+insecure 一并生效,`--key`/`--insecure` 仍可再覆盖);
  解析不到 → exit 2 列出可用名。
- I-3 `profile` 子命令(§2.3,本地命令):
  `add <name> --server <url> --key <key> [--alias a]… [--key-stdin] [--force]`
  (`--alias` 可重复);`use`/`remove` 接受别名(use 写入规范名);
  `remove` 删 current 则同步清空;`list` 见 §2.2 掩码规则,附别名列。
- I-4 取值链并入 §2.2:flag > env > profile > 默认,server 与 key 各自
  独立取最高优先级(prod 的 server + `--key` 临时换凭据是合法组合;
  `--server <别名>`=整 profile 生效是唯一例外,见 I-2)。
- I-5 测试:优先级矩阵(flag/env/profile/默认 交叉)、未知字段拒载、
  命名空间冲突拒载、别名解析(`--profile` 别名 /
  `--server` 别名=整 profile 生效 / 未知名 exit 2 列可用名)、
  掩码断言(list 输出不含完整 key,防粘贴外泄)、add→load JSON 往返、
  原子写(写中断不留半截)、`--key-stdin`、remove 清 current。
- [x] 验收:两个 profile 指向两台本地服务(httptest 双地址即可)分别
      `stats overview` 成功,`--profile <别名>` 与 `--server <别名>`
      两条路径都实测;`profile list` 输出不含任何完整 key(脱敏)。
      ✅ `20395c1`。TestProfile_TwoServersOverview(双 httptest,断言各
      恰 1 次命中);优先级矩阵 11 例、拒载三类、别名解析、掩码、
      add→load 往返、原子写(无 .tmp 残留 + 0600)、--key-stdin、
      remove 清 current 全部落地;真实二进制对 v0.6.0 服务端冒烟通过
      (见包 G 轮次记录)。

### 包 G:文档与 CI(P1)

- G-1 `cli/rhctl/README.md`:安装(`go build ./cmd/rhctl`)、凭据三级
  来源(flag/env/profile,§2.2 分层表)、profile 与多管理端用法
  (含别名)、七组命令示例、§2.5 的 get→编辑→update 工作流、§2.6
  secret 保存警示、`--key` 进 shell history 的警示、退出码表。
- G-2 `docs/README.md` 索引登记本任务书(§2 交接与状态表加一行)。
  (规划阶段已先行登记一行;执行时核对状态措辞即可。)
- G-3(可选,先决条件:本地三道门 + 交叉编译全绿)ci.yml 增加
  `cli (go)` job:gofmt check / vet / build / test。遵守 AGENTS.md
  「红的门禁比没有门禁更糟」:加之前先在本地按 CI 同款命令跑一遍。
- [x] 验收:README 示例逐条可复制执行(对着本地起的服务实测一遍,
      过程记录进轮次表);ci.yml 若加,`gh run` 确认该 job 绿。
      ✅ `9cfe93e`。实测:服务端取 **v0.6.0 Release 产物**(darwin/arm64,
      sha256 校验通过)本地起服,控制台会话签发 full 权限 key 后逐条跑通:
      profile add(--key-stdin)/use 别名/list 掩码、stats overview、
      clients 全生命周期(create 的 secret 完整展示)、services 全九命令
      (get→编辑→update 工作流、stcp rotate-secret 拿明文)、visitors 全六
      命令、events/config(get/put/未知字段 422)、certs(openssl 自签
      PEM 上传→list→update→delete);错误路径:重复名(HTTP 200+code≠0
      → exit 1)、坏 key(401 → exit 1)、key 缺失(exit 2 零请求)、
      用法错(exit 2 附 usage)。G-2 状态措辞在收尾提交定稿(需引用三轮
      审计结果)。G-3 已加 `cli (go)` job(本地同款命令全绿后加),
      推送后 `gh run` 确认见 §6 后记。

### 包 H:审计执行与收尾(P0,必须最后执行)

按 §5 协议执行**连续三轮**审计(在包 A–I 全部完成、复选框回填之后),
回填 §6 轮次记录;`git status` clean。
- [x] 验收:§6 有连续三轮零 finding 记录;每轮的命令输出留痕
      (贴进轮次表或附文件路径)。
      ✅ `4b44299`(被审计代码状态;本节内容在收尾 docs 提交,码不变)。
      三轮输出全文见 §6 表上方代码块;`git status` clean(构建产物
      rhctl 已 gitignore)。

---

## 5. 审计协议(完成判据)

> 「连续三次审计、无接口遗漏、无新引入问题」的机械化定义。
> 一轮审计 = 下表四项全部执行并留痕;任一项有 finding → 修复 →
> **计数归零**重计(连续 = consecutive,不是累计)。

### 5.1 一轮审计的四项检查

| 项 | 查什么 | 命令/方法 | 零 finding 标准 |
|---|---|---|---|
| **A 接口覆盖** | §3 契约与代码双向一致 | ① 跑 §1.0 校验命令第 1 条重新生成路由清单;② 与 §3.1 逐行比对(28 op);③ `go test ./... -run TestContract -v`(命名约定 §0 第 9 条)列出全部契约测试,与 §3.1「命令」列一一对上;④ §3.2 的 10 个排除项理由仍成立(服务端 session 组没变) | 路由清单与 §3 无差异;每个在范围 op 有命令 + 契约测试;每个范围外 op 有登记理由 |
| **B Go 质量门** | 代码质量 | `gofmt -l .`(空)/ `go vet ./...` / `go test ./...` / `go build ./...` / §0 第 4 条的三条交叉编译 | 全绿零输出 |
| **C 模块纪律** | go.mod 只被 go 命令动过 | `go mod tidy -diff`(退出码 0)/ `go mod verify` / `go list -m all` 依赖集 ⊆ {本模块, cobra 传递闭包} / `git log -p -- go.mod go.sum` 每个变更提交的信息含对应 go 命令 | 四条全过 |
| **D 无新引入问题** | 改动面 | `git diff <基线提交> --name-only`:只含 `cli/rhctl/**`、`docs/rhctl-cli-tasklist-2026-10.md`、`docs/README.md`、(若做 G-3)`.github/workflows/ci.yml`、(A-1)`.gitignore`;**`crates/`、`migrations/`、`console/`、`tests/` 零文件** | diff 面干净 |

说明:
- **凭据安全不做 grep 类检查**(源码字面量 grep 抓不到真实风险 —— 真风险
  是**变量**被打印,那由包 B 的行为测试钉住:key 不得出现在任何错误消息/
  输出的断言里)。审计只查行为测试存在且绿,不扫源码字面量。
- **A 项必须重新跑路由枚举**,不许拿上一次的输出 —— 服务端在演进,
  清单变了(比如新增了一个资源组)而 §3 没跟上,就是 finding。
- **D 项基线** = 包 A 开工前的 main HEAD,开工时记录在 §6 表头。
- finding 修复后从 A 项重新开始整轮,不许只补查发现问题的那一项
  (修复可能引入新问题,整轮重跑才是「审计」而非「抽查」)。

### 5.2 三轮执行方式

三轮在**同一最终代码状态**下执行(轮与轮之间代码不变;变了就得重计)。
每轮在 §6 追加一行。三轮全零 finding + 包 A–I 复选框全回填 = 任务完成。

---

## 6. 审计轮次记录

> 基线提交:`9518f2e`(包 A 开工前 main HEAD,2026-10-29)。

三轮均在同一最终代码状态(`4b44299`,与实现末提交 `9cfe93e` 同码,
其后仅文档差异)下连续执行;轮次脚本把 §5.1 四项各做成断言,
输出逐行留痕如下(三轮输出完全一致):

```
A①②④ paths=27 ops=38 key-reachable=28;§3.1 28 行一致;session 9+login=10 排除项理由成立
A③   TestContract 28/28 PASS
B    gofmt/vet/test/build + cross×3 全绿
C    tidy-diff=0 / verify 过 / 根出边仅 cobra 系(闭包 ⊆ cobra)
D    diff 面干净(白名单外文件 0;crates/migrations/console/tests 0)
```

| 轮次 | 日期 | commit | A 覆盖 | B 质量门 | C 模块 | D 无新引入 | finding(编号+一句话) | 状态 |
|---|---|---|---|---|---|---|---|---|
| 1 | 2026-10-29 | 4b44299 | ✓(§3.1 28 行零差异;28 契约测试;10 排除项) | ✓(三道门+交叉×3) | ✓(tidy-diff 0/verify/闭包) | ✓(白名单外 0) | 无 | 零 finding |
| 2 | 2026-10-29 | 4b44299 | ✓(同上,重跑枚举) | ✓ | ✓ | ✓ | 无 | 零 finding |
| 3 | 2026-10-29 | 4b44299 | ✓(同上,重跑枚举) | ✓ | ✓ | ✓ | 无 | 零 finding |

细目备注:
- **A② 对账方法**:§1.0 校验命令第 1 条原文重跑 → key-reachable 28 op
  与 §3.1 的机械编码(方法+路径)双向 diff 为空;A③ 的 28 个
  `TestContract_*` 名单与 §3.1「命令」列按命名规则(空格→下划线)
  双向比对,无多余无缺失;A④ 的 session 组按 router 原文
  (`let session = Router::new()…route_layer(guard(…, true))`)重提取,
  9 op + public login 与 §3.2 登记的 10 操作集合相等。
- **C 项闭包证据**:`go mod graph` 根模块出边仅
  {cobra, mousetrap, pflag, go@1.27.3}(后两者为 `go get` 自动带入的
  cobra 传递依赖);其余模块(md2man/blackfriday/yaml/check)全部位于
  cobra 子树。`go list -m all` 共 8 行,无白名单外直接依赖。
- **流程备注**:三轮曾先跑过一遍(代码状态相同),因 G 复选框尚未
  回填、不合 §4「A–I 全部回填后才跑」的次序,在 `4b44299` 回填 G 后
  **整三轮重跑**,以重跑结果为准(本表)。

---

## 7. 不做清单(本期明确不做,防止被当成遗漏)

1. **密码登录 / JWT / api-keys 管理** —— CLI 只认 API Key。Key 的签发
   与吊销在控制台完成(服务端整组 session-only 防横向提权)。若未来
   真需要 `rhctl apikeys`,应作为独立任务评估(引入密码交互 + JWT
   生命周期 + `pwd_version` 语义,凭据模型复杂度翻倍)。
2. **发版产物集成(release.yml)** —— 不改发版流水线。它是发版关键路径,
   失败代价是删标签重打(AGENTS.md);`rhctl` v1 以 `go build ./cmd/rhctl`
   自行构建。单独立任务再做。
   > 2026-10-29 翻牌:用户裁定「加进 release」,已作为独立任务落地 ——
   > `release.yml` 新增 `cli` job(Go 六平台单 runner 交叉编译,
   > `-ldflags` 从标签注入版本,产物 `rhctl-<版本>-<os>-<arch>` +
   > `.sha256` 挂 Release)。其余五条不做项仍有效(not-supported §15)。
3. **spec 的 flag 化编辑** —— services/visitors 的 create/update 只吃
   JSON 文件(§2.5)。20+ 字段做成 flag 必然与服务端漂移。
4. **交互式 shell / 系统 keyring / viper / 权限强制** —— 凭据持久化经
   包 I 的 profiles.json 解决(明文文件,业界基线同 kubectl kubeconfig /
   aws credentials;**不做权限检查**,§2.2 定案);OS keyring
   (zalando/go-keyring)会新增直接依赖并引入平台差异(darwin 走 cgo、
   linux 走 dbus),真需求出现再评估;不引入 viper。
5. **对业务错误码做本地翻译** —— code 与 message 原样透出(§0 第 8 条),
   CLI 不内置错误码表(那是服务端事实,复制必漂移)。
6. **用户自定义命令别名(gh 式 `alias set`)** —— 不做。内置四组别名
   (§2.3:ls/rm/rotate/ov)覆盖高频缩写;更长定制用 shell alias / 函数
   已足够,CLI 内再做一层别名存储徒增帮助文档与配置面。

完成后把本节登记进 `docs/not-supported.md`(新增一节,格式照 §14)。

✅ 已登记为 `docs/not-supported.md` §15(2026-10-29,含变更记录行)。

---

## 8. 预埋坑备忘(实现者先读,均来自 §1 的代码事实)

| 坑 | 后果 | 对策 |
|---|---|---|
| 业务错误是 HTTP 200 + code≠0 | 按状态码分支的客户端把失败当成功 | 客户端层一律先解信封再判 code(§1.2) |
| 未知 `/api/*` 是 200 + code 404 | 同上 | 同上,按 code 分支 |
| axum Json 拒收(422,非 JSON body) | 解码 panic 或空指针 | 容忍非 JSON 错误 body(包 B-1) |
| clients 无 enable/disable 路由 | 硬造 `clients enable` 会 404 | 经 `update {enabled}`(§1.3) |
| VisitorSpec `serde(flatten)` | create 发嵌套 `{client_id, spec:{…}}` 服务端 422 | 扁平合并(包 E) |
| `get_service` 的 spec 无 secret_key | 编辑回写时误清密钥 | README 示例明示「留空 = 不改」(§2.5) |
| `create_service` 不回显 STCP 族 secret | 用户以为创建时能拿到密钥 | README 明示:拿明文只能 rotate(§1.3) |
| events limit 服务端 clamp 1..500 | CLI 本地「纠正」反而与服务端漂移 | 原样透传(包 F 验收) |
| config put `deny_unknown_fields` | 多写一个字段整单 422 | 文件透传 + README 提示先 `config get` |
| 凭据文件被同步/备份工具带走 | key 明文随文件外流 | 接受的风险(定案:不做权限强制,业界 token 文件亦明文);唯一强制行为是输出脱敏 |
| 时间戳全毫秒 | table 显示成一串数字不可读 | §0 第 7 条双口径 |
| `go get` 在 workspace 根误执行 | 根目录长出无关 go.mod | 所有 go 命令必须 `cd cli/rhctl` 后执行(§0 第 2 条) |
