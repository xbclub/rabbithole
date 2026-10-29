# rhctl — rabbithole 管理端 CLI

一把 API Key 完成 clients / services / visitors / stats / events / config /
certs 七组管理操作。Go + cobra,除 cobra 外零第三方依赖,`go build` 即得
单二进制。

## 安装

```bash
cd cli/rhctl
go build -o rhctl ./cmd/rhctl
./rhctl --version   # dev(-ldflags 注入正式版本号,发版集成见任务书 §7)
```

交叉编译同理:`GOOS=linux GOARCH=amd64 go build -o rhctl ./cmd/rhctl`。

## 凭据:三级来源

| 优先级 | 来源 | 场景 | 泄漏面 |
|---|---|---|---|
| 1 | `--key` / `--server` | 一次性覆盖 | ⚠ **shell history 与 `ps` 可见**,慎用 |
| 2 | `RH_API_KEY` / `RH_SERVER` | CI、secret manager 注入 | 进程环境(`/proc/*/environ`、误 dump) |
| 3 | **profile 文件(交互式默认)** | 日常使用 | 仅文件系统权限(0600) |
| 4 | 默认 `http://127.0.0.1:8080` | 本机即用 | 无凭据 |

- server 与 key **各自独立**取最高优先级:`--key` 临时换凭据 + profile 的
  server 是合法组合。
- 唯一例外:`--server <名/别名>`(不带 `http(s)://` 前缀)= 整个 profile
  临时生效(server+key+insecure 一并切过去),对齐 `ssh <别名>` 的直觉。

```bash
export RH_API_KEY=rhk_…          # CI 常用
./rhctl stats overview --key rhk_…   # 一次性
```

> ⚠ `--key rhk_…` 会进 shell history 与 `ps` 输出。日常请用 profile
> (下节)或 `--key-stdin`。

## profile:多管理端

每台 server 独立签发自己的 key(服务端只存 sha256 哈希,同一把 key 不能
跨 server 复用)。多管理端 = N 对 `(server, key)` 的 profile:

```bash
# 添加(--key-stdin 防 key 进 history;--alias 可重复)
./rhctl profile add prod --server http://172.16.1.6:8080 --key-stdin --alias p
./rhctl profile add lab  --server https://172.16.1.17:8443 --key rhk_… --alias lb

./rhctl profile use p            # 切当前(别名可,写入规范名)
./rhctl profile list             # key 掩码显示(rhk_ 前 8 hex + …)
./rhctl profile remove lab       # 删 current 则同步清空
```

存储在 `~/.config/rhctl/profiles.json`(尊重 `XDG_CONFIG_HOME`,
`RHCTL_CONFIG` 强制覆盖),原子写、拼错的键直接报错不静默。
**不做权限检查**;文件明文与 kubectl kubeconfig / aws credentials 同基线,
想看完整 key 直接 `cat` 文件,CLI 不提供 reveal。

选机三种写法:

```bash
./rhctl stats overview --profile p    # 按名/别名
./rhctl stats overview --server lb    # 整 profile 生效(临时切机)
./rhctl stats overview                # 用 current
```

## 一次性 secret:当场保存,关闭即失

`clients create` / `clients rotate-secret` / `services rotate-secret` 的
响应里 secret **仅本次返回**(服务端只存哈希,无法找回):

```
⚠ secret 仅本次返回(服务端只存哈希,无法找回),请立即保存 —— 关闭即失。
id                          name    secret
0192…                       gw-1    a3f0…(64 hex)
```

**现在就存进密码管理器。** 另外:`services create` 创建 STCP/SUDP/XTCP
规则时**响应不含 secret**(服务端生成存库但不回显),要拿明文只能再执行
一次 `services rotate-secret`。

## 七组命令

```bash
# clients(启停即 update --enabled/--disabled,没有独立 enable/disable)
./rhctl clients list
./rhctl clients create --name gw-1 --remark 边缘网关 --rate-limit 1MB --max-conns 100
./rhctl clients update <id> --disabled
./rhctl clients rotate-secret <id>
./rhctl clients delete <id>

# services(spec JSON 文件透传,见下节工作流)
./rhctl services list [--client-id <cid>]
./rhctl services get <id> -o json
./rhctl services create --file spec.json
./rhctl services update <id> --file spec.json
./rhctl services enable <id>        # 客户端离线时显示 deferred=true
./rhctl services disable <id>
./rhctl services rotate-secret <id> # 仅 STCP/SUDP/XTCP
./rhctl services batch --ids 1,2,3 --action enable|disable|delete
./rhctl services delete <id>

# visitors(create 的 body 是 client_id 与 spec 扁平合并,文件写纯 spec 即可)
./rhctl visitors list [--client-id <cid>]
./rhctl visitors create --client-id <cid> --file visitor.json
./rhctl visitors update <id> --file visitor.json
./rhctl visitors enable <id>
./rhctl visitors disable <id>
./rhctl visitors delete <id>

# stats / events
./rhctl stats overview
./rhctl events list --limit 50 --level warn

# config(deny_unknown_fields:先 get 对照再 put,未知字段整单 422)
./rhctl config get
./rhctl config put --file update.json

# certs(PEM 从文件读入;--domains 缺省取证书 SAN)
./rhctl certs list
./rhctl certs create --name lab --cert-file cert.pem --key-file key.pem [--domains a.com,b.com]
./rhctl certs update <id> --name lab --cert-file cert.pem --key-file key.pem
./rhctl certs delete <id>
```

内置别名:`ls`≡`list`、`rm`≡`delete`、`rotate`≡`rotate-secret`、
`ov`≡`overview`。`--file` 支持 `-`(stdin)。

## spec 工作流:回读 → 编辑 → 回写

`ProxySpec` / `VisitorSpec` 是 20+ 字段的版本化结构,CLI 不复制字段表、
不做字段校验 —— 服务端是权威校验者,CLI 透传 JSON 文件。

```bash
./rhctl services get 3 -o json | jq .spec > spec.json   # 回读(密钥已抹掉)
vim spec.json                                          # 编辑;secret_key 留空 = 不改
./rhctl services update 3 --file spec.json
```

`visitors create` 会把 `--client-id` 与文件里的 spec **扁平合并**成单层
body(服务端是 `serde(flatten)`,发嵌套 `{client_id, spec:{…}}` 会被
422 拒收);`visitors update` 直接发文件内容(纯 VisitorSpec)。

## 输出与退出码

- 默认 `table`;`--output json`(`-o json`)把信封 `data` 部分**原样**
  输出(不重组、不丢字段),可接 `jq`。时间戳在 table 里转本地时区,
  json 里保持毫秒整数。
- 数据走 stdout,提示与错误走 stderr —— `-o json` 的管道不会被诊断输出污染。

| 退出码 | 含义 |
|---|---|
| 0 | 成功 |
| 1 | 远端错误:业务 `code≠0`(注意:**HTTP 仍是 200**,信封里 code 才是判据)、401/403、网络失败 |
| 2 | 本地错误:用法/参数、文件读不到、key 缺失(不发请求) |

## 开发

```bash
cd cli/rhctl
gofmt -l . && go vet ./... && go test ./...   # 三道门
GOOS=windows GOARCH=amd64 go build ./...      # 三条交叉编译自查
```

模块纪律:`go.mod`/`go.sum` 只由 go 命令变更;直接依赖仅
`github.com/spf13/cobra`。契约测试 `TestContract_*` 与管理 API 的 28 个
Key 可达操作一一对应(任务书 §3.1)。
