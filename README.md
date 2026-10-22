# rabbithole

自托管的端口转发与远程服务接入平台:Rust 服务端 + 客户端 + Web 管理控制台
(React 19 + Vite + Tailwind v4 + shadcn/ui)。
功能基线对齐 frp v0.71.0 的稳定能力(对照矩阵见 `docs/requirements-matrix.md`),
协议为全新自有设计(RHTP),不兼容 frp。

## 性能(对 frp v0.71.0)

三机 peer 基准,控制面 tcp / `relay=splice` / `mux=1`,压测用仓库自带
`benchmark/loadgen`,每档 15s × 5 次配对取中位。**完整数据与方法见
[`benchmark/README.md`](benchmark/README.md)**。

| 维度 | rabbithole | frp v0.71.0 | 对比 |
|---|---:|---:|---:|
| 单流吞吐(上行 / 下行) | 2067 / 2016 Mbps | 1083 / 1133 Mbps | **1.78–1.91×** |
| 4 流吞吐 | 2515–2559 Mbps | 2659–2708 Mbps | 0.93–0.96× |
| 16 流吞吐 | 2786–2830 Mbps | 2684–2693 Mbps | 1.03–1.05× |
| 延迟开销 p99 | +0.93 ms | +1.17 ms | 低 21% |
| 并发建连 p99 @4096 | 653 ms | 677 ms | 持平 |
| HTTP vhost QPS | 1152 | 1041 | +10.7% |
| UDP 吞吐 | 96 Mbps | 69 Mbps | +41% |
| 建连速率 | 936 /s | 992 /s | 差距在噪声内 |

> **怎么读这张表**:单流优势来自多载体 + yamux 批聚合。4 流略低但**不是性能
> 问题** —— frp 那轮的直连基线系统性偏高(4 流 2399 vs rh 2308),而 rh 自己
> 的流数扫描 1→2→4→8→16 流为 2026/2436/2453/2776/2980 Mbps,单调递增,
> 4 流只是处在平台区起步处。
> 并发建连 4096 档 rh 与 frp 差 3.5%,在噪声内,结论是「**持平**」而非「更快」。
>
> **已知性能短板**:`quic` / `kcp` 两种控制面协议的吞吐只有 TCP 系的 1/4
> (单流约 500–590 vs 1700–2200 Mbps,保留率 29–32%),且不随流数增长。
> 详见 [`benchmark/README.md`](benchmark/README.md)「六种控制面协议对比」。

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
  IPv4/IPv6、9 种客户端插件、自定义 DNS、SSH Tunnel Gateway(`ssh -R`)
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
pnpm build                            # 产物 console/dist/(Vite + React 19 + shadcn/ui)
cd .. && cargo build --release        # 二进制内嵌控制台
./target/release/rh-server --config deploy/server.example.toml
# 也可改用 [web] static_dir 指向磁盘目录(优先级高于内嵌,便于只更新前端不重编);
# SPA 回退已内置:深层路由 200,未知 /api/* 返回 JSON 404
# 未构建前端时直接 cargo build 也可编译(二进制不含控制台,API 正常,补 UI 需重建)

# B. 开发模式:Vite dev server(/api 代理到 127.0.0.1:8080,见 console/vite.config.ts)
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

### 多载体池自动定容

启用 `multiplex = true` 的 tcp/tls/ws/wss 客户端会决定拨几条物理载体(K),
控制流占第 0 条,其余用于数据面轮转分派。**K 不是固定值,而是按链路 RTT
自适应**——这是 2026-10-18 实测后的结论(85 轮实验,见
`docs/single-carrier-bottleneck-summary.md`)。

期望值的来源优先级(见 `initial_k_for_async`):

1. **显式配置**:配置文件 `mux_pool_size = N` > 环境变量 `RH_MUX_POOL_SIZE`
   > 自动。显式配置时**不探测 RTT**。
2. **RTT 自适应**(自动路径):`carriers_for_rtt()`
   - `RTT < 5ms` → 1;`5~15ms` → 2;`>= 15ms` → **4**
   - 依据:0.4ms 时 K=1 比 K=4 快 8%;20ms 时 K=4 快 16%;
     300ms 时 **K=8 比 K=4 慢 33%**,故上限取 4 而非 8。
   - 注意:该结果还要与下一项取 `max`,**所以低 RTT 下最终也不是 1**。
3. **协议基线兜底**:`default_carriers()` 对所有 TCP 系协议返回 **2**
   (QUIC/KCP 恒 1)。自动路径取 `max(rtt_k, 协议基线)`——协议基线抬高**下限**,
   RTT 仍可在高 BDP 下继续上调。依据:单载体在 2.1–2.2 G 封顶且与流数无关,
   瓶颈是单条 TCP 拥塞窗 + yamux 单 `FlushTask` 串行,K=2 可测提升 6–10%,
   K=4 与 K=2 持平(只增成本)。
4. **硬件上限**:`max(CPU 核数 - 1, 1)`,能读到网卡 RX 队列数时再取
   `min(该值, 队列数)`;结果钳制 [1,8]。非 Linux 或读不到队列数时仅由 CPU
   决定("读不到"不等于"单队列")。
5. **服务端上限**:报到应答回写 `max_carriers`(默认 8,钳制 [1,8])。

最终 `K = min(期望值, 硬件上限).clamp(1, 8)`,服务端上限在报到后收敛。

> **探测失败不阻断连接**:探针只打一条 TCP(走与真实拨号相同的路径,含
> `proxy_url`),超时 2s;失败时退回协议基线。QUIC/KCP 与非 multiplex 场景
> **在探测之前**就 gate 成 1,不会白打这条 TCP。

`mux_pool_size = 1` 表示强制单载体(严格等价旧路径);K=1 时每条载体的
yamux 分片 Worker 仍为 4,不会被缩减。K>1 时每载体分片 Worker 按
`max(1, 4/K)` 缩减,避免线程总量随 K 线性膨胀。

报表中的 shard 数是**拨号时固化**的:收敛保留控制载体 + (`final_k` - 1) 个
成功载体、摘除失败身份与其余成员,不重建,因此 `initial_k == 1` 时 shard = 4;
从 `initial_k > 1` 收敛到 1 时沿用初始值 `max(1, 4/initial_k)`(例如
initial_k=2 → 2、initial_k≥4 → 1)。
不回收重建的理由:载体在拨号时已 spawn 分片 Worker,收敛后重建会拆掉
控制流所在的载体 0,代价远超收益;"K=1 零回退"只针对 `initial_k == 1`
路径(不建池、shard=4),该路径已满足。

报到结束后 K 即固定:运行中不因服务端上限变大而补拨。收敛按**成员身份**
(池内单调递增 id,不随成员表压缩漂移)摘除:保留控制载体 + 成员表中
`final_k - 1` 条报到成功的载体,失败身份与其余成员逆序摘除(先清重拨
配方再关闭物理连接)。报到被拒/超时/坏报文/写失败的载体同样按身份摘除,
不再参与后续轮转。保留额按"当前表内成功数"计——不能按表尾位置取,
否则前缀内失败(如 initial_k=3、final_k=2、第 1 条失败第 2 条成功)会把
成功的第 2 条一并摘掉。收敛前 `reap` 可能已 spawn 重拨,而拨号受
`connect_timeout`(默认 10s)约束;`set_active_prefix` 同时登记补位目标数,
`redial_loop` 在入表前丢弃使成员数超过目标的多余补位并关闭其物理连接,
因此迟到的重拨不会让物理载体数永久多出 1 条。之后的正常故障重拨不受
影响(成员数低于目标时照常补位)。收敛后另有最多 3 轮"重读成员表 + 按
同一保留规则再摘"的终态校验。
报到窗口内死亡的载体由池健康监视剔除。
启动日志单行给出定容依据:`pool sizing protocol=… protocol_baseline=…
hardware_cap=… server_cap=… initial_k=… final_k=… shard_workers=…`;
收敛日志追加 `failed=… detached=…`(实际成功摘除数)。

服务端侧在 `deploy/rh-server.toml`/`server.example.toml` 用顶层
`max_carriers = N` 控制上限(0 视为 1)。**该上限是强制的**:cap 含首
载体,附加载体报到时若 `池内现有载体 + 首载体 + 本次 > cap`,服务端回
`CarrierAttachAck{ok:false, error_code:"POOL_FULL"}` 并**不聚合**;
重拨补位路径(首流为数据通道)同样检查,超限的连接不入池但仍可服务
其首条数据流。检查与聚合之间存在并发窗口:多个附加连接同时报到时可能
各自通过检查而短暂超出 1 条(拒绝路径无害,严格串行化会引入额外耦合)。

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
console/            管理控制台(React 19 + Vite + Tailwind v4 + shadcn/ui)
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
  LICENSE 见 `console/`)—— 第二代控制台的前端基座(Umi Max + React 19 + antd v6 + ProComponents),
  已于 2026-10-12 由 Vite + Tailwind v4 + shadcn/ui 重写取代,`console/LICENSE` 沿用其 MIT 许可
- **[vue-vben-admin](https://github.com/vbenjs/vue-vben-admin)**(MIT)—— 早期控制台基座,
  已于 2026-10 移除,其交互惯例影响了首版控制台设计

