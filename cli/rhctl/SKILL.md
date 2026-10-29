---
name: rhctl
description: rabbithole 管理端 CLI。当用户要管理 rabbithole 服务端(创建/启停客户端、转发规则 services、访问器 visitors、查统计与事件、改服务器配置、上传 TLS 证书)时,优先用 rhctl 而不是手写 curl。凭据是控制台签发的 API Key(rhk_…)。
---

# rhctl —— rabbithole 管理端 CLI

一把 API Key 完成 clients / services / visitors / stats / events /
config / certs 七组管理操作。

## 安装(若未安装)

```bash
# 从 Release 下载本平台归档(linux/darwin/windows × amd64/arm64),校验后解压到 PATH
gh release download -R xbclub/rabbithole latest -p "rhctl-*<平台>*" -D /tmp/rhctl-dl
cd /tmp/rhctl-dl && shasum -a 256 -c *.sha256 && tar xzf *.tar.gz
sudo cp rhctl-*/rhctl /usr/local/bin/ && rhctl --version
# 或源码构建:git clone 后 cd cli/rhctl && go build -o /usr/local/bin/rhctl ./cmd/rhctl
```

## 凭据(先于一切命令)

API Key **只能由人在控制台签发**(设置 → API 密钥;rhctl 自身不能建 key,
服务端有意如此)。拿到 key 后配置一次:

```bash
echo "rhk_…" | rhctl profile add prod --server http://<管理端>:8080 --key-stdin
rhctl profile use prod
```

- 取值链:`--key/--server` > `RH_API_KEY/RH_SERVER` > profile > 默认
  `http://127.0.0.1:8080`。
- **不要把 key 写进命令行参数**(会进 shell history 与 ps);用
  `--key-stdin`、环境变量或 profile。
- `--server <profile 别名>` 可临时切到另一台管理端。

## 命令速查

```bash
rhctl clients list|create --name <n> …|update <id> --enabled|--disabled|rotate-secret <id>|delete <id>
rhctl services list [--client-id <cid>] | get <id> [-o json] | create --file spec.json
rhctl services update <id> --file spec.json | enable|disable <id> | rotate-secret <id>
rhctl services batch --ids 1,2,3 --action enable|disable|delete | delete <id>
rhctl visitors list | create --client-id <cid> --file visitor.json | update <id> --file visitor.json
rhctl visitors enable|disable|delete <id>
rhctl stats overview          # 别名 ov;events list --limit N --level warn
rhctl config get | put --file update.json
rhctl certs list | create --name <n> --cert-file c.pem --key-file k.pem [--domains a,b] | update|delete <id>
rhctl profile list|add|use|remove
```

别名:`ls`≡`list`、`rm`≡`delete`、`rotate`≡`rotate-secret`、`ov`≡`overview`。

## Agent 必须知道的语义

- **退出码**:0 成功;1 远端错误(业务错误 / 401 / 403 / 网络);2 本地
  错误(用法、文件读不到、key 缺失)。**业务错误是 HTTP 200 + 信封
  `code≠0`** —— 脚本判成败看退出码,不要看 HTTP。
- 错误消息形如 `api 错误 1002: <服务端消息>`,code/message 原样透传,
  语义查服务端 `api.rs`;401 = key 无效/过期,提醒用户在控制台重签。
- **spec 类命令(services/visitors create/update、config put)吃 JSON
  文件原样透传**,本地不校验字段:编辑规则的标准流是
  `rhctl services get <id> -o json | jq .spec > s.json` → 改 →
  `rhctl services update <id> --file s.json`(spec 里 `secret_key` 留空
  = 不修改)。`--file` 支持 `-`(stdin)。
- **一次性 secret**:`clients create / *rotate-secret` 的 secret 仅本次
  返回且无法找回 —— 拿到就完整转告用户,提醒立即保存。
- `-o json` 输出信封 data 原文(毫秒时间戳、字段不重组),喂 jq 或程序
  消费;默认 table 是给人看的。数据走 stdout、提示走 stderr,管道安全。
- 时间戳:json 里是毫秒整数;table 里已转本地时区可读格式。
