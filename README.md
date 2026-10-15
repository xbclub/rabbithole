# rabbithole

自托管的端口转发与远程服务接入平台:Rust 服务端 + 客户端 + Web 管理控制台(Umi Max + antd v6)。
功能基线对齐 frp v0.71.0 的稳定能力(对照矩阵见 `docs/requirements-matrix.md`),
协议为全新自有设计(RHTP),不兼容 frp。

## 功能总览

- **8 种代理类型**:TCP / UDP / HTTP vhost(域名+子域名路由)/ HTTPS vhost(SNI)/
  STCP / SUDP(密钥访问)/ XTCP(P2P 打洞,含 KCP 直连)/ TCPMux(HTTP CONNECT 复用)
- **传输**:tcp / tls(rustls,支持自动自签证书)/ ws / wss / quic(quinn)/ kcp;
  yamux 多路复用;客户端可经 HTTP CONNECT / SOCKS5 代理拨号
- **认证**:客户端密钥 HMAC 挑战应答;
  管理面 argon2 + JWT(首次启动自动创建 `admin`,随机密码仅在服务端 stdout 横幅展示一次;
  控制台「设置」页可改密,改密后所有旧会话令牌立即失效);凭据轮换/撤销/会话替换
- **运行能力**:连接预热池、TCP/HTTP 健康检查、group 负载均衡、带宽限制、
  snappy 压缩、PROXY protocol v1/v2、HTTP 头/Host 改写、端口白名单与自动分配、
  IPv4/IPv6、10 种客户端插件、自定义 DNS、SSH Tunnel Gateway(`ssh -R`)
- **集中管理**:SQLite 存储;规则热更新(期望状态 + 幂等全量同步);
  在线状态/失败原因;全局/分客户端/分规则流量与连接统计;客户端级配额
  (并发/规则数/限速/流量上限);HTTPS 多证书(SNI 服务端终结);事件审计;
  Prometheus 指标;重启恢复
- **Web 控制台**:登录/登出、客户端管理(一次性凭据展示 + 配额)、规则与访问器管理
  (批量启停删、插件、HTTP 增强:自定义 404/静态缓存/头改写)、证书管理、
  总览统计/流量速率图/事件流(级别筛选)、服务器配置热生效
  (SSH 网关/端口策略/日志级别/公钥白名单)、修改密码

## 快速开始

### 服务端(公网机器,Linux)

一键脚本:下载 Release 静态二进制(含 sha256 校验)→ 写入默认配置 → 注册 systemd 服务:

```bash
curl -fsSL https://raw.githubusercontent.com/xbclub/rabbithole/main/deploy/install-server.sh | sudo bash
journalctl -u rabbithole-server | grep -A3 'password:'   # 初始 admin 密码,仅首次启动展示一次
```

控制台默认 `http://<本机地址>:8080`(账号 admin,首登后立即改密);配置、数据库与
自动生成的证书都在 `/etc/rabbithole/`,重复执行脚本 = 升级(配置与数据保留)。
Docker 方式见下文 [Docker](#docker) 一节;macOS / 手动部署可直接运行脚本
(自动选对应平台包)或从 [Releases](https://github.com/xbclub/rabbithole/releases) 下载。

### 客户端(内网机器)

先在控制台「客户端」页创建——凭据弹窗直接给出三条可复制命令
(一键脚本 / Docker / 配置模板,凭据已填好)。手动执行:

```bash
curl -fsSL https://raw.githubusercontent.com/xbclub/rabbithole/main/deploy/install-client.sh \
  | sudo bash -s -- --server <服务端地址:7100> --protocol tls \
  --id <客户端ID> --secret <密钥>
# systemd 服务经 /etc/rabbithole/client.env(0600)接入,零配置文件;
# 轮换密钥后重跑同一命令即可。需 rabbithole v0.2.2+(默认安装最新版)
```

### 源码构建

```bash
# 服务端 + 客户端(需 Rust stable)
cargo build --release
./target/release/rh-server --config deploy/server.example.toml
# 首次启动自动创建 admin,随机密码只在 stdout 横幅展示一次;
# 首登后请立即在控制台「设置」页修改密码
```

完整配置说明见 `deploy/` 与 `docs/design.md`。控制台两种运行方式:

```bash
# A. 生产模式(推荐,单二进制):构建前端产物后重新编译服务端,
#    dist 会被 rust-embed 自动嵌进 rh-server(build.rs 自动检测),无需任何配置
cd console && pnpm install
pnpm build                            # 产物 console/dist/(Umi Max + antd v6)
cd .. && cargo build --release        # 二进制内嵌控制台
./target/release/rh-server --config deploy/server.example.toml
# 也可改用 [web] static_dir 指向磁盘目录(优先级高于内嵌,便于只更新前端不重编);
# SPA 回退已内置:深层路由 200,未知 /api/* 返回 JSON 404
# 未构建前端时直接 cargo build 也可编译(二进制不含控制台,API 正常,补 UI 需重建)

# B. 开发模式:Umi dev server(/api 代理,端口与后端见 console/config/proxy.ts)
cd console && pnpm dev
```

SSH Tunnel Gateway(可选,`[ssh_gateway]` 开启):

```bash
ssh -R 0:内网目标:端口 <client-id>@<server> -p <ssh_gateway.bind_port>   # 密码=客户端密钥
# 或公钥:authorized_keys_file 中列入公钥,用户名=客户端 ID
```

## Docker

官方镜像分两个,均为多架构(linux/amd64 + linux/arm64),随版本标签发布:

**服务端**(公网机器):

```bash
docker run -d --name rabbithole \
  -p 7100:7100 -p 8080:8080 -p 8081:8081 -p 8082:8082 -p 8443:8443 \
  -v rabbithole-data:/var/lib/rabbithole \
  ghcr.io/xbclub/rabbithole-server:latest
# 首次启动:admin 随机密码与自签 TLS 证书一次性打印在容器日志,
# 配置落在数据卷 /var/lib/rabbithole/server.toml,可挂载修改
docker logs rabbithole        # 查看初始密码
# 旧地址 ghcr.io/xbclub/rabbithole 与 server 内容相同,继续同步发布
```

**客户端**(内网机器;先在控制台「客户端」页创建——创建成功会直接给出可复制的 `docker run` 命令):

```bash
docker run -d --name rh-client --restart unless-stopped \
  -e RH_SERVER=example.com:7100 \
  -e RH_CLIENT_ID=<客户端ID> \
  -e RH_SECRET=<密钥> \
  ghcr.io/xbclub/rabbithole-client:latest
# 规则由服务端集中下发,客户端零配置文件;可选 env:RH_PROTOCOL / RH_MULTIPLEX /
# RH_TLS_INSECURE / RH_TLS_PIN_SHA256 / RH_TRUSTED_CA_FILE / RH_LOG_LEVEL 等
```

需要高级项(自定义 CA / 代理 / STUN / DNS)时挂载 TOML 配置(模板 `deploy/client.example.toml`),
挂载后文件模式优先:`-v $PWD/client.toml:/etc/rabbithole/client.toml:ro`。
裸机同理:`RH_SERVER=host:7100 RH_CLIENT_ID=... RH_SECRET=... rh-client` 直接接入
(优先级:CLI `--server/--id/--secret` > 环境变量 > 配置文件)。

转发端口段(默认 20000-21000)按需追加 `-p` 映射;生产请置于反向代理 + HTTPS 之后。
也可自行构建:`deploy/Dockerfile`(server)与 `deploy/Dockerfile.client`(client)
(打包 musl 静态二进制,无 RUN 步骤,本地构建方式见文件头注释)。

## 发布(Release)

推送 `v*` 标签即触发 [release 工作流](.github/workflows/release.yml):

```bash
git tag v0.1.0 && git push origin v0.1.0
```

一次发布产出:

- **二进制归档**:Windows(zip)/ Linux / macOS(tar.gz)× x86_64 / arm64 共 6 目标,
  每个含 rh-server、rh-client、示例配置与 sha256,发布到 GitHub Release
  (Linux 为 musl 静态链接,任意发行版可直接运行)
- **Docker 镜像**:`ghcr.io/xbclub/rabbithole:<tag>` 与 `:latest`(amd64 + arm64)


## 仓库结构

```
crates/rh-protocol   RHTP 帧协议、控制消息、代理/访问器规格
crates/rh-transport  tcp/tls/ws/quic/kcp、yamux 复用、压缩、代理拨号
crates/rh-core       限速、PROXY protocol、STUN、网络工具
crates/rh-server     控制面/数据面/管理 API/vhost/SSH 网关/SQLite
crates/rh-client     会话与重连、转发、插件、健康检查、visitor、打洞
console/            管理控制台(React 19 + antd v6,Umi Max + ProComponents)
tests/               E2E 与负载测试脚本
docs/                设计文档、需求矩阵、验收记录、评审日志
deploy/              部署示例配置与 systemd 单元
```

## 许可

Apache-2.0(与基线 frp 一致)。

## 鸣谢

- **[frp](https://github.com/fatedier/frp)**(Apache-2.0)—— 功能基线与转发语义的对标项目,
  多处模块设计(插件、visitor、打洞、vhost)参考了其成熟实现
- **[ant-design-admin-v6](https://github.com/arvinaij/ant-design-admin-v6)**(MIT,
  LICENSE 见 `console/`)—— 管理控制台前端基座(Umi Max + React 19 + antd v6 + ProComponents)
- **[vue-vben-admin](https://github.com/vbenjs/vue-vben-admin)**(MIT)—— 早期控制台基座,
  已于 2026-10 移除,其交互惯例影响了首版控制台设计

