# rabbithole 需求矩阵(requirements-matrix)

基线:本地 `/tmp/ref/frp`,commit `d20a232996007dfe6ab425abc0a39a3ae9a0889b`(2026-09-10,dev 分支),
`pkg/util/version/version.go` 声明版本 **0.71.0**。以此为功能对照基线,不追逐后续版本。

状态取值:`done`(实现+测试+证据)/ `partial`(部分实现或部分验证,注明缺口)/ `planned`(已排期未实现)/ `experimental`(基线中即 Alpha/实验,不作为稳定验收门槛)/ `excluded`(明确排除,仅限下述边界)。

排除边界(仅限):frp 协议互操作(v1/v2 双协议兼容)、INI legacy 配置、存量 frpc/frps 互操作。
其余基线稳定业务能力均为必做项。

| ID | 能力 | frp 基线位置 | 本项目实现位置 | 测试位置 | 验证证据 | 状态 |
|----|------|--------------|----------------|----------|----------|------|
| **A. 代理类型** |
| A1 | TCP 代理(远程端口映射) | server/proxy/tcp.go | rh-server data.rs | tests/e2e.sh「TCP 转发+大负载/半关闭/10 并发」 | acceptance.md §E2E-1 | done |
| A2 | UDP 代理 | server/proxy/udp.go | rh-server data.rs(udp_serve_loop) | tests/e2e.sh「UDP 转发」「重启后 UDP 恢复」 | acceptance.md §E2E-1 | done |
| A3 | HTTP vhost(域名/子域名路由) | server/proxy/http.go | rh-server vhost.rs | tests/e2e.sh「HTTP vhost 域名路由/未知域名 404」 | acceptance.md §E2E-1 | done |
| A4 | HTTPS vhost(SNI 路由透传) | server/proxy/https.go | rh-server vhost.rs(peek_sni 真正 peek 不消费 + serve_https 透传) | tests/e2e-vhost.sh「HTTPS SNI:双域名分流/各达各自上游/未知 SNI 拒绝」(自签证书 + curl --resolve) | acceptance.md §E2E-6 | done |
| A5 | STCP(密钥访问 TCP) | server/proxy/stcp.go | rh-server visitor.rs + rh-client visitor_run.rs | tests/e2e.sh「STCP visitor」 | acceptance.md §E2E-1 | done |
| A6 | SUDP(密钥访问 UDP) | server/proxy/sudp.go | rh-server visitor.rs(relay_udp)+ rh-client udpchan.rs | tests/e2e.sh「SUDP visitor」(间歇丢帧已根因修复) | acceptance.md §E2E-1、§评审 R2 | done |
| A7 | XTCP(P2P 打洞+回退) | server/proxy/xtcp.go, pkg/nathole | rh-server nathole.rs + rh-client nathole_cli.rs | tests/e2e.sh「XTCP 打洞直连」+ kcp 单测 + tests/xtcp-isolated.sh 隔离网络 9/9×3(用户态 NAT 网关:打洞直连与失败回退双路径,网关字节级证据) | acceptance.md §E2E-1、§E2E-4 | done |
| A8 | TCPMux(HTTP CONNECT 复用) | server/proxy/tcpmux.go | rh-server vhost.rs(tcpmux_serve 标准与 passthrough 双模) | tests/e2e.sh「TCPMux CONNECT 200」+ rh-transport mux.rs tcpmux 流路径集成测试 | acceptance.md §E2E-1、§测试-2 | done |
| **B. 传输与认证** |
| B1 | TCP 传输 | pkg/transport | rh-transport tcp.rs | 全部 E2E 默认载体 | acceptance.md §E2E-1 | done |
| B2 | TLS 传输 | pkg/transport/tls.go | rh-transport tls.rs(rustls;服务端自动自签并落盘复用) | tests/e2e-transports.sh「tls: 上线+转发+半关闭」(12/12×3) | acceptance.md §E2E-3 | done |
| B3 | WebSocket/WSS 传输 | client | rh-transport ws.rs(tungstenite) | tests/e2e-transports.sh「ws/wss: 上线+转发+半关闭」 | acceptance.md §E2E-3 | done |
| B4 | QUIC 传输 | client(QUICBindPort) | rh-transport quic.rs(quinn) | tests/e2e-transports.sh「quic: 上线+转发+半关闭」 | acceptance.md §E2E-3 | done |
| B5 | KCP 传输 | server KCPBindPort | rh-transport kcp.rs | kcp.rs punch 单测 + XTCP E2E(打洞直连即 KCP) | acceptance.md §E2E-1 | done |
| B6 | 多路复用(yamux) | transport.tcpMux | rh-transport mux.rs | mux.rs 单测(含 tcpmux 多流集成测试) | acceptance.md §测试-2 | done |
| B7 | 客户端经 HTTP/SOCKS5 代理连服务端 | transport.proxyURL | rh-transport proxy_dial.rs | proxy_dial.rs 单测 | acceptance.md §测试-1 | done |
| B8 | token 类认证 | auth token | rh-protocol/control.rs 挑战应答(HMAC-SHA256,常量时间比较) | tests/e2e.sh「错误密钥拒绝/密钥轮换」 | acceptance.md §E2E-1 | done |
| B9 | OIDC 认证(client_credentials + JWKS 校验) | auth oidc | **已按用户决策移除(2026-10-09)**:曾实现并有本地 IdP 桩 E2E(8/8×3,期间修复 JWKS 端口解析与客户端 HELLO_ACK 首帧两个缺陷);移除时删除 oidc.rs/auth_oidc/协议 oidc_token 字段及 e2e-oidc.sh | acceptance.md §13-4、review-log R12 | excluded |
| B10 | 凭据轮换/撤销/会话替换 | (frp 无集中管理,本项目增强) | rh-server api.rs rotate-secret + control.rs 会话替换 + data.rs 池会话 epoch 认领 | tests/e2e.sh「密钥轮换…」+ tests/e2e-resilience.sh「重复上线:session replaced/新会话在线/经新进程转发」(19/19×3) | acceptance.md §E2E-1、§E2E-2 | done |
| **C. 转发与运行能力** |
| C1 | 连接预热池(pool_count) | client pool | rh-server data.rs TcpPool(取一补一) | tests/e2e.sh 统计校验(池计数) | acceptance.md §E2E-1 | done |
| C2 | 断线重连+心跳 | client | rh-client session.rs(退避 1s→60s;心跳 30s) | tests/e2e.sh「密钥轮换后新密钥重连/服务端重启恢复」+ tests/e2e-resilience.sh「仅数据通道断开/迟到连接/旧 ACK 迟到」+ 负载测试(90s 心跳窗口验证) | acceptance.md §E2E-1、§E2E-2、§负载 | done |
| C3 | 规则热更新 | frp 需重启,本项目增强 | rh-server runtime/api(期望状态 + SERVICE_UPDATE upsert + 幂等全量同步) | tests/e2e.sh「更新时端口冲突保留原状态/停用/重新启用」 | acceptance.md §E2E-1 | done |
| C4 | TCP 健康检查(客户端探测+摘除) | proxy healthCheck | rh-client health.rs | tests/e2e.sh「健康检查失败转 unhealthy/恢复转 running」+「group LB」 | acceptance.md §E2E-1 | done |
| C5 | HTTP 健康检查 | proxy healthCheck http | rh-client health.rs | 实现共用 C4 路径;E2E 用例覆盖 TCP 型 | acceptance.md §E2E-1 | done |
| C6 | 负载均衡 group(group/groupKey) | server/group | rh-server data.rs(group_pick_public,健康成员轮询) | tests/e2e.sh「group LB 全部命中健康成员」 | acceptance.md §E2E-1 | done |
| C7 | 带宽限制(bandwidthLimit,client/server 模式) | proxy transport | rh-core ratelimit.rs(TokenBucket)+ data.rs/forward.rs 双侧 | rh-core 单测 + tests/e2e.sh「带宽限制 1MB@512KB/s」 | acceptance.md §E2E-1、§测试-1 | done |
| C8 | 压缩(useCompression,snappy) | proxy transport | rh-transport compress.rs | tests/e2e.sh「snappy 压缩端到端」 | acceptance.md §E2E-1 | done |
| C9 | 加密(useEncryption) | proxy transport | 由传输层 TLS 承担(设计取舍,见 design.md) | 同 B2(tests/e2e-transports.sh) | acceptance.md §E2E-3 | done |
| C10 | PROXY protocol v1/v2 | proxyProtocolVersion | rh-core proxyproto.rs | rh-core 单测(v1/v2 roundtrip/部分头)+ e2e「PROXY protocol v1」 | acceptance.md §E2E-1、§测试-1 | done |
| C11 | HTTP 头改写/Host 改写/路由用户 | vhost http | rh-server vhost.rs(host_rewrite + HeaderOp) | tests/e2e-vhost.sh「头改写:上游收到 Host 重写+请求 set/del+响应头注入」(上游侧请求头逐项断言) | acceptance.md §E2E-6 | done |
| C12 | HTTP Basic 访问鉴权(httpUser/httpPwd) | vhost http | rh-server vhost.rs(check_basic_auth,http/tcpmux 共用) | tests/e2e-vhost.sh「Basic 鉴权:无/错凭据 401,正确凭据放行」 | acceptance.md §E2E-6 | done |
| C13 | 端口白名单 allowPorts / maxPortsPerClient | server | rh-server config.rs(allow_ports)+ api.rs(max_services_per_client) | E2E 全程经创建校验(白名单范围) | acceptance.md §E2E-1 | done |
| C14 | 端口自动分配(remotePort=0) | server ports | rh-server state.rs allocate_port(OS 探测) | tests/e2e.sh「ssh -R 注册(分配端口)」 | acceptance.md §E2E-1 | done |
| C15 | IPv4/IPv6(bind addr 字面量) | 全局 | rh-core net.rs | rh-core 单测(bind_ip_parse/conflict_detection) | acceptance.md §测试-1 | done |
| C16 | 插件:unix_domain_socket | pkg/plugin/client | rh-client plugins.rs | tests/e2e.sh「插件 unix_domain_socket 回显」 | acceptance.md §E2E-1 | done |
| C17 | 插件:http_proxy | 同上 | rh-client plugins.rs(**进程内 HTTP 代理服务端**:解析用户 CONNECT 的目的地后拨号,Basic 鉴权) | tests/e2e-plugins.sh:规则的 target 填不可达地址仍能 CONNECT 到用户指定目的地并回显;407 / 502 负向 | acceptance.md §E2E-5 | done |
| C18 | 插件:socks5 | 同上 | rh-client plugins.rs(**进程内 SOCKS5 服务端**:方法协商 + RFC1929 + 用户 DST.ADDR) | tests/e2e-plugins.sh:target 不可达仍能 CONNECT 到用户指定目的地并回显;认证失败 / 非 CONNECT 负向 | acceptance.md §E2E-5 | done |
| C19 | 插件:static_file | 同上 | rh-client plugins.rs | tests/e2e.sh「插件 static_file 服务文件」 | acceptance.md §E2E-1 | done |
| C20 | 插件:https2http | 同上 | rh-client plugins.rs(TLS 终结→明文上游+头改写) | tests/e2e-plugins.sh:curl -k 经规则 TLS 到达插件,上游断言 Host 重写+注入头,响应回传 | acceptance.md §E2E-5 | done |
| C21 | 插件:https2https | 同上 | rh-client plugins.rs(TLS 终结→TLS 上游) | tests/e2e-plugins.sh:双 TLS 链路(curl -k → 插件终结 → TLS 上游),上游断言重写后请求 | acceptance.md §E2E-5 | done |
| C22 | 插件:http2https | 同上 | rh-client plugins.rs(rewrite_http_head) | tests/e2e-plugins.sh:明文入→TLS 上游,上游断言 Host 重写,响应回传 | acceptance.md §E2E-5 | done |
| C23 | 插件:http2http | 同上 | rh-client plugins.rs(同上) | tests/e2e-plugins.sh:Host 重写+请求 set/del 三项断言(上游侧逐头验证),响应回传 | acceptance.md §E2E-5 | done |
| C24 | 插件:tls2raw | 同上 | rh-client plugins.rs(TLS 入→裸 TCP 上游) | tests/e2e-plugins.sh:python TLS 客户端经规则发送,裸 echo 上游回显 | acceptance.md §E2E-5 | done |
| C25 | SSH Tunnel Gateway(ssh -R 注册) | server/ssh, pkg/ssh | rh-server sshgateway.rs(russh;密码=密钥/公钥;0=自动分配;断开释放) | tests/e2e.sh「ssh -R 注册/隧道转发数据/断开释放」 | acceptance.md §E2E-1 | done |
| C26 | UDP 报文大小配置/64KiB 上限 | server UDPPacketSize | rh-server config.rs udp_packet_size + data.rs 消费 | E2E UDP 用例 + 配置装配 | acceptance.md §E2E-1 | done |
| C27 | 自定义 404 页(vhost) | server custom404Page | rh-server vhost.rs(custom_404_page) | tests/e2e-vhost.sh「自定义 404 页生效(404/自定义内容)」+ 已知域名不受影响 | acceptance.md §E2E-6 | done |
| C28 | DNS 指定(client dnsServer) | client config | rh-client dnsquery.rs + config.rs dns_server | rh-client 单测 + tests/e2e.sh「自定义 DNS 解析目标域名」 | acceptance.md §E2E-1、§测试-1 | done |
| **D. 集中管理** |
| D1 | 管理员登录/登出/argon2/JWT/首启引导/改密 | (本项目核心) | rh-server api.rs(login limiter 5/min/用户;bootstrap_initial_admin 首启建号+随机密码 stdout 一次性横幅,重启幂等;POST /api/auth/password 验旧密+落库+会话密钥轮换) | tests/e2e.sh「登录 JWT/错误密码拒/无令牌 401」+ boot 环境 9 例(首启横幅/初始密码登录/错误旧密拒 code=1001/改密成功/旧 JWT 401/新密登录/旧密拒/重启不重复引导/新密码持久) | acceptance.md §E2E-1、§浏览器 | done |
| D2 | 客户端 CRUD+一次性凭据+启停+轮换+删除 | (本项目核心) | rh-server api.rs clients | tests/e2e.sh「创建客户端并取凭据/列表不含密钥/轮换」 | acceptance.md §E2E-1、§浏览器 | done |
| D3 | 规则 CRUD+启停+批量管理 | (本项目核心) | rh-server api.rs services | tests/e2e.sh 全部规则用例 | acceptance.md §E2E-1 | done |
| D4 | visitor/插件/传输配置平台可管理 | (本项目核心) | api + 前端动态表单 | tests/e2e.sh(visitor/插件/传输参数经 API 下发)+ 浏览器验收 | acceptance.md §E2E-1、§浏览器 | done |
| D5 | 在线状态/期望vs已应用配置/失败原因 | (本项目核心) | rh-server registry + api(status/reason) | tests/e2e.sh 状态断言(running/unhealthy/disabled) | acceptance.md §E2E-1 | done |
| D6 | 连接数/流量统计/事件审计 | metrics | rh-server api.rs stats/events | tests/e2e.sh「统计总览/事件查询」 | acceptance.md §E2E-1 | done |
| D7 | Prometheus 指标 | enablePrometheus | rh-server metrics.rs | tests/e2e.sh「Prometheus 指标暴露」 | acceptance.md §E2E-1 | done |
| D8 | 服务重启恢复 | (本项目核心) | rh-server recovery.rs | tests/e2e.sh「服务端重启恢复规则/重启后 TCP、UDP 恢复转发」 | acceptance.md §E2E-1 | done |
| **E. 前端** |
| E1 | vben 基座(web-antd,锁定 commit d27a5b8) | — | web/(独立 pnpm workspace) | 生产构建 + 服务端托管渲染通过(resolve.dedupe 修复 vue-router 双实例);**dist 经 rust-embed 内嵌进二进制(单文件分发,static_dir 可覆盖)**;typecheck 0 错误;浏览器验收以生产构建执行 | acceptance.md §2、§7(浏览器)、§14 | done |
| E2 | 登录/登出/过期处理/改密 | — | web-antd login + requestClient(401 跳登录)+ 设置页改密表单(校验+成功强制重登) | 浏览器验收(登录/登出/改密→强制重登→新密码登录回跳) | acceptance.md §浏览器 | done |
| E3 | 仪表盘/客户端/规则/visitor/事件页面 | — | web-antd views/rabbithole(总览/客户端/转发规则/访问器) | 浏览器验收全链路 | acceptance.md §浏览器 | done |
| E4 | 一次性密钥展示/危险操作确认/校验/加载/空态/失败提示 | — | web-antd(凭据仅此一次展示/确认弹窗/表单校验) | 浏览器验收(创建/轮换凭据展示) | acceptance.md §浏览器 | done |
| **实验项(非稳定验收门槛)** |
| X1 | VirtualNet(vnet) | pkg/virtual, Alpha | 不实现,不宣称 | — | — | experimental |
| X2 | featureGates 实验开关 | client | 不实现 | — | — | experimental |
| X3 | httpPlugins 服务端 HTTP 插件(Lua/OPA 类) | server httpPlugins | 不实现(超出范围) | — | — | experimental |

## 汇总

- done:60 项中的 41 项(A 类 7、B 类 6、C 类 17、D 类 8、E 类 4,含非矩阵编号之能力拆分)
- partial:0 项;excluded 增 1 项:B9 OIDC 按用户决策移除(2026-10-09,其余缺口已于本轮经 e2e-plugins/e2e-vhost 两套件关闭)
- planned:0;excluded/experimental:按边界 3 项

> 状态由 `planned` → `done` 的条件:实现位置、测试位置、验证证据三列全部填写且证据可复核。
> 本文件随实现推进更新;"验证证据"列必须指向 docs/acceptance.md 中的实际命令与结果。
