# rabbithole 验收记录(acceptance)

日期:2026-10-08 初测,2026-10-09 前端生产构建修复后复测(全部命令在本仓库工作区实际执行;环境 macOS 27.0 arm64,Rust stable,Node 26.10/pnpm 11.16)

> **时效说明(2026-10-22 补注)**。本文按时间累积,不同章节对应不同时点的前端形态,
> 阅读时请先认准年代:
>
> | 章节 | 时点 | 对应前端 |
> |---|---|---|
> | §1–§13 | 2026-10-08 ~ 10-10 | `web/`(vue-vben-admin 裁剪版,**已删除**) |
> | §14–§15 | 2026-10-10 | 同上(内嵌托管刚落地时) |
> | 现役控制台 | 2026-10-12 起 | `console/`(Vite + React 19 + Tailwind v4 + shadcn/ui) |
>
> §2 的"前端质量门"记录的是 **vben 版**的实测结果,其命令(`pnpm --filter web-antd …`)
> 与测试计数(575 用例)在 `web/` 删除后**已不可复现**。现役控制台的质量门见
> 文末 §16 与 `console/README.md`。Rust 侧门(§1)与各 E2E 套件的结论不受前端替换影响。

## 1. Rust 质量门

| 门 | 命令 | 结果 |
|----|------|------|
| 格式 | `cargo fmt --all` | 通过 |
| 静态检查 | `cargo clippy --workspace --all-targets` | **0 警告 0 错误** |
| 单元测试 | `cargo test --workspace` | **43 通过 / 0 失败**(rh-protocol 11、rh-transport 16、rh-core 12、rh-client 2、rh-server 2) |
| 发布构建 | `cargo build --release` | 通过(仅 rust-objcopy 剥离调试信息的本机工具链告警,产物完整可用) |

## 2. 前端质量门(web/pnpm workspace,Node 26.10/pnpm 11.16)

> **历史存档**:本节记录 `web/`(vue-vben-admin)在 2026-10-09 的实测结果。
> 该目录已于 2026-10-10 CUT 删除,下列命令与计数**无法在当前仓库复现**。
> 现役控制台的质量门见 §16。

| 门 | 命令 | 结果 |
|----|------|------|
| 类型检查 | `pnpm --filter web-antd typecheck`(vue-tsc) | **0 错误**(修复了 3 个业务视图 bodyCell 槽位 record 类型断言缺失,共 13 处既有错误) |
| 生产构建 | `pnpm --filter web-antd build` | 通过;产物 dist/(entry jse/、chunk js/、css/、_app-config 注入) |
| 生产托管 | rh-server `web.static_dir` + SPA 回退 | `/`=200;深层路由=200 index.html(history 路由可用);`/api/*` 未知路径返回 JSON 404 envelope(不混入 HTML);路径穿越被规范化(仅回 index) |
| 开发模式 | `pnpm dev` + 浏览器冒烟 | 登录页正常渲染(修复后回归确认 `resolve.dedupe` 不影响 dev) |
| Lint | `pnpm run lint`(vsh:oxlint --type-aware + oxfmt + eslint + stylelint) | **exit 0**。oxlint 0 警告 0 错误;oxfmt/stylelint 全绿;eslint 0 错误(16 警告均为上游 vben 框架测试文件 `vue/one-component-per-file`,非本项目代码) |
| 单元测试 | `pnpm run test:unit`(vitest --dom) | **78 文件 / 575 用例全通过**。上游 flake 如实披露(2026-10-09 R13 复测):`@core/form-ui/form-integration.test.ts`(与上游 d27a5b8 字节级一致,本轮未触碰)存在时序性偶发失败——单文件连跑 5 次有 3 次出现 1~2 例**不同**用例失败;全量三连跑 1/1/0 例失败后第三次 575/575 全绿。未屏蔽、未跳过、未放宽断言 |

修复与偏离说明(2026-10-09,详见 web/FRONTEND-NOTES.md):裁剪时丢失的上游根级
lint 配置已按上游原样恢复(eslint.config.mjs / stylelint.config.mjs /
oxfmt.config.ts / oxlint.config.ts / .stylelintignore);element-plus 以精确版本
2.14.5 回归 catalog 并作为 form-ui devDependency(其上游测试经 .pnpm 深路径引用);
清理 16 项裁剪后无消费者的 catalog 条目;eslint/oxfmt 自动修复覆盖全仓库格式。
`check:circular` 仍报上游包既有类型循环警告(上游原样,未修改)。

**2026-10-10 模板残留清理复跑**(任务书 web/FRONTEND-CLEANUP-PROMPT.md,阶段 1+2,
详见 FRONTEND-NOTES「模板残留清理」节):lint **exit 0**、typecheck **0 错误**、
vitest **575/575**、build 通过;产物 dist 4.3M→**3.4M**(js chunk 217→147,
674K echarts chunk 与全部演示页/品牌/统计残留 grep 为空)。已知残留(阶段 3
未执行):框架层默认值字符串(unpkg static-source URL、'Vben Admin' 默认版权名,
运行时未启用)与 common-ui 的 3 个 profile chunk;`pnpm install
--frozen-lockfile` 修复(allowBuilds.lefthook 占位符→false)后 exit 0。

## 3. 测试-1(单元/集成测试要点)

- rh-transport:帧协议 roundtrip、yamux 开流/回显/多流、**tcpmux 全链路集成测试**
  (用户侧 yamux 客户端 8 并发流 ↔ 服务端 accept ↔ 每流拨号真实 TCP echo ↔ 半关闭),
  kcp 打洞回环、compress、tls、proxy_dial(CONNECT/SOCKS5)
- rh-core:proxyproto v1/v2 roundtrip + 部分头等待(修复了 v2 端口/地址序与 v2 前缀误判)、
  ratelimit 令牌桶、STUN 测试服务器 roundtrip(修复属性长度计算)、bind_ip/端口段/allow 列表
- rh-client:dnsquery A 记录解析(构造报文 + 回退路径)

## 4. 测试-2(tcpmux yamux 流路径集成测试)

`crates/rh-transport/src/mux.rs::tcpmux_stream_path_over_real_target`:
一条通道上并发 8 条 yamux 流,每流 4096 字节负载经真实 TCP echo 回显,
写半关闭传播(shutdown → echo EOF → read_to_end)全部通过。与 vhost.rs 的
accept 循环、rh-client handle_start_forward 的每流转发同构。

## 5. E2E-1(多进程真实链路,`tests/e2e.sh`,55/55 PASS)

独立端口(18000 段)+ 临时目录 + 自带 echo/UDP/http/hdr/DNS 服务,`PASS=55 FAIL=0`
(release 二进制通过;失败自动归档日志到 /tmp/rh-e2e-last)。用例:

登录与凭据:管理员创建 / 登录 JWT / 错误密码拒绝(envelope code!=0)/ 无令牌 401 /
创建客户端并取凭据 / **列表响应不含密钥** / 客户端上线

转发面:TCP 转发+大负载(300KB)/ TCP 半关闭 / TCP 10 并发 / UDP 转发 /
snappy 压缩端到端(600KB)/ PROXY protocol v1 / HTTP vhost 域名路由+未知域名 404 /
STCP visitor / SUDP visitor / XTCP 打洞直连(+复用)/ **TCPMux CONNECT 200** /
插件 static_file / 插件 unix_domain_socket / group LB(6/6 命中健康成员)/
带宽限制(1MB@512KB/s 实测 3.1s)/ 健康检查失败转 unhealthy + 恢复转 running /
自定义 DNS 解析目标域名

SSH Tunnel Gateway:ssh -R 注册(0 端口自动分配)/ 隧道转发数据 / 断开后端口释放

管理面:更新时端口冲突拒绝且原规则不受扰 / **非端口变更原位切换目标(§4.3,listener
不重建、转发即时取新 target)** / **通配探针拒绝外部具体地址占用端口(macOS 通配 bind
可与之共存,试绑补环回探针对齐 Linux 语义)且拒绝后原规则不受扰** / 创建时冲突拒绝 /
停用后端口关闭 / 重新启用恢复转发 / 密钥轮换(新密钥返回、旧会话下线、新密钥重连上线)/
服务端重启恢复规则 + 重启后 TCP/UDP 恢复转发 / 统计总览 / 事件查询 / Prometheus 指标

首启引导与改密(独立 boot 环境:全新 DB,web 19195/管理 19196):stdout 横幅展示
随机密码 / 初始密码可登录 / 错误旧密码拒绝(code=1001)/ 修改成功 / **改密后旧 JWT
401(会话密钥已轮换)** / 新密码可登录 / 旧密码拒绝 / 服务端重启不再二次引导(无横幅)/
重启后新密码仍有效(落库持久)。过程事故与修复见 review-log R13(e2e.sh 曾默认指向
target/debug 陈旧二进制导致假失败,已改默认 target/release)。

## 6. E2E-2(韧性场景,`tests/e2e-resilience.sh`,19/19 PASS ×3)

独立环境(server 26700/26780 + echo + 管理端),覆盖 §六.C 补充场景:

- **目标拒连**:连接指向无监听端口 → 用户连接被快速干净关闭(≤15s 全量断言),
  规则状态不受影响(running),其他规则转发正常
- **DB 故障与回滚**:外部 `BEGIN EXCLUSIVE` 锁注入 SQLite 故障 8s → 期间更新被拒
  (code=500)、**原转发零扰动**;锁释放后更新成功、转发正常(事务回滚正确性)
- **仅数据通道断开**:TCP 中间人(tests/tcpkill-mm.py)只杀数据连接,控制会话保持
  (multiplex=false 裸模式独立客户端)→ 新用户连接触发重建通道恢复转发,
  同一客户端的其他规则/控制会话未被波及
- **迟到连接**:服务端拒绝后迟到的数据通道连接不污染池,后续转发正常
- **旧 ACK 迟到**:`kill -STOP` 冻结客户端跨两个规则版本(目标 切走→切回),
  `kill -CONT` 后迟到 ACK 不回退最终状态:收敛 running,转发跟随最终目标
- **同客户端重复上线**:先冻结原进程(连接保持)再启动同 ID 新进程 → 服务端记录
  `session replaced`、新会话在线、**转发经新进程可用**(该场景曾暴露真实缺陷:
  池内遗留旧会话通道导致黑洞,已修复,见 review-log R10)

## 7. E2E-3(传输变体,`tests/e2e-transports.sh`,12/12 PASS ×3)

六种主控传输各自独立环境(独立 server/client/DB),每协议验证完整链路:
登录 → SYNC → 池通道 → TCP 转发 → **半关闭**(send+shutdown(WR) 后收到镜像回显):

| 协议 | tls 细节 | 结果 |
| --- | --- | --- |
| tcp | —(对照) | 上线+转发+半关闭 PASS |
| tls | 服务端自动自签(落盘 `<配置目录>/certs/` 复用),客户端 insecure | PASS |
| ws | 同端口 http 升级 | PASS |
| wss | 自动自签 + insecure | PASS |
| quic | quinn,自动自签 + insecure | PASS |
| kcp | — | PASS |

该套件曾暴露真实缺陷:服务端自动证书落盘前未创建 `certs/` 子目录,tls/wss/quic
监听启动失败(ENoENT);已修复(tls.rs `auto_cert` 先 `create_dir_all`)。

## 8. E2E-4(XTCP 隔离网络,`tests/xtcp-isolated.sh` + `tests/natgw.py`,9/9 PASS ×3)

用户态 STUN(RFC 5389 XOR-MAPPED)+ UDP NAT 网关(natgw.py)构建双隔离网络:
客户端 P 在 NAT-A 后、visitor V 在 NAT-B 后,两端只见过网关映射地址:

- **场景 1(开放型 NAT)**:打洞直连成功,数据经网关字节级验证(relayed=1094B),
  多次回程复用同一打洞通道
- **场景 2(阻断型 NAT-A)**:网关拦截打洞报文(blocked=50,直连字节 relayed=0)→
  打洞失败自动回退服务端中继,转发仍可用(独立全新 DB 环境)

过程中修复 5 个真实缺陷(回退无限重入、relay_pairs 跨会话撞号、punch_listen
无超时泄漏、裸模式握手魔数判断、固定端口恢复误重分配),见 review-log R10。

## 9. E2E-5(客户端插件,`tests/e2e-plugins.sh`,15/15 PASS ×3)

七个插件经 服务端→客户端→插件→本地辅助服务 全链路验证(插件 JSON 经 API 创建规则):

| 插件 | 验证语义 | 断言 |
| --- | --- | --- |
| http2http | Host 重写 + HeaderOp(set 后 del)+ 响应头注入 | 上游侧逐头断言(host/x-rew/x-del 消失),响应回传 |
| http2https | 明文入 → TLS 上游(insecure,本地自签) | 上游收到 Host 重写请求,响应回传 |
| https2http | TLS 终结(插件证书)→ 明文上游 + 头改写 | curl -k 可达,上游断言 Host 重写+注入头 |
| https2https | TLS 终结 → TLS 上游(双 TLS 链路) | 上游断言重写后请求 |
| tls2raw | TLS 入 → 裸 TCP 上游(不解读 HTTP) | python TLS 客户端发送,裸 echo 回显 |
| http_proxy | 插件**即代理服务端**:解析用户 `CONNECT host:port` 里的目的地并拨号 | 规则的 target 故意填不可达地址(127.0.0.1:1),仍能 CONNECT 到用户指定的 echo 并回显;错误凭据 → 407 + `Proxy-Authenticate`;目的地不可达 → 502 |
| socks5 | 插件**即 SOCKS5 服务端**:方法协商 + RFC1929 认证 + 用户指定的 `DST.ADDR` | 同上(target 不可达),CONNECT 到用户指定 echo 并回显;错误凭据 → `0x01 0x01`;非 CONNECT 命令 → REP `0x07` |

http_proxy/socks5 的断言刻意建立在「配置里的 target 是死地址」之上:
修前的实现把 target 同时当代理地址与 CONNECT 目的地,自指;只要实现
重新去读 target,这两条用例就会立刻变红。

辅助服务全部为本地 python(请求头记录型上游、echo),显式 close 释放连接。
两个代理插件的 e2e 不需要「本地代理辅助服务」——被测插件自己就是代理服务端。

## 10. E2E-6(vhost 进阶,`tests/e2e-vhost.sh`,10/10 PASS ×3)

- **HTTPS SNI 路由(A4)**:自签证书(SAN 双域名)+ `curl --resolve`,两条 https
  规则按 SNI 分流到各自客户端插件(https2http)终结 TLS;未知 SNI 无 200
  (服务端 peek ClientHello 后无路由即关闭)。**该用例暴露并修复真实缺陷**:
  原 peek_sni 用 try_read 消费了 ClientHello 且 WouldBlock 直接报错断连
  (该路径此前无任何 E2E,缺陷一直存在)。
- **HTTP 头改写(C11)**:上游侧断言 Host 重写、请求头 set/del、curl 侧断言
  响应头注入。
- **Basic 访问鉴权(C12)**:无凭据/错误凭据 401,正确凭据放行。
- **自定义 404 页(C27)**:未知域名返回服务端配置的 404 文件内容与 404 状态,
  已知域名不受影响。

## 11. 负载测试(`tests/load.sh`)

- 100 并发 TCP 连接,每连接 5 轮 4KB 请求-响应,持续 **600 秒**
- 结果(R8 审计修复后的最终复测):**491,735 轮完成,0 错误,601s**;负载后转发抽查通过
- 过程中根因修复两处影响长稳的缺陷(见 review-log.md R4):
  90 秒心跳无条件下线会话(每 90s 全量连接被杀)、数据通道限额只增不减
  (约 256 次通道后被饿死)。修复后 90s/600s 均零错误。

## 12. 浏览器验收(真实后端 + **生产构建**,2026-10-09 复测通过)

以 `pnpm build` 产物经 rh-server `web.static_dir` 静态托管(同源 /api)执行完整链路:

登录 → 总览(统计卡片+事件表真实数据)→ 客户端页新建客户端
(凭据仅此一次展示)→ 用该凭据启动真实 rh-client(页面显示在线/0.1.0)→
转发规则页新建 TCP 规则(18901→echo)→ **经规则真实转发验证通过** →
UI 编辑规则改目标端口 → **转发即时切换到新目标验证通过** →
UI 开关停用(端口实测 Connection refused,状态 disabled)→ 重新启用
(转发恢复实测)→ 轮换密钥(新凭据展示、旧会话立即下线、旧客户端被拒重连,
新密钥重启后在线恢复且转发可用)→ 总览统计复核(客户端 1/在线 1/运行中 1,
真实数字)→ 登出(header 退出按钮 → 确认弹窗 → 回登录页,刷新无自动登录)。

负向检查:错误密码停留登录页并提示「用户名或密码错误」(2026-10-10 R15 起后端
API 消息全面中文化,此前为英文 "invalid username or password");
未登录直访 `#/rabbithole/overview` 被守卫重定向回登录页;空表单提交被校验拦截。

历史注记:dev server 链路曾于 2026-10-08 通过(review-log R6);当时生产构建
存在登录后主界面空白缺陷,根因与修复见 review-log R9(pnpm 双实例 vue-router →
两套 routerKey 注入符号;`resolve.dedupe` 修复后以生产构建复测全链路通过)。

第三轮(2026-10-09,设置页改密,生产构建):独立环境首启引导 → 横幅随机密码登录 →
侧边栏「设置」→ 修改密码表单(当前/新/确认)提交 → toast「密码已修改,所有会话已
失效,请重新登录」+ 自动登出回登录页(保留 redirect)→ **用新密码重新登录成功**并
回跳设置页。表单校验:空表单无误显提示(曾误显灰色"两次输入不一致",antd FormItem
`help` 恒渲染所致,改为条件渲染后修复)、新/确认不一致时红色错误文案+红框、一致后
消失、新密码 ≥8 位由前后端双侧校验。自动化侧另有 e2e.sh boot 环境 9 例(见 §5)。

第四轮(2026-10-10,模板残留清理冒烟,dev server 5666 + 真实 rh-server 8080,
首启引导环境):① 未登录访问深层路由被守卫重定向回登录页;登录页**仅有**
账号/密码/记住账号/登录按钮(无忘记密码/注册/扫码/手机号/第三方图标/页脚 Vben
版权与 ICP 备案/营销插画,panel-center),标题与文案为 Rabbithole 自有覆盖;
② 登录后侧边栏仅「rabbithole」一组 5 项(总览/客户端/转发规则/访问器/设置),
五个页面全部可开,总览统计与事件表为真实后端数据(bootstrap/login 事件可见);
③ 顶栏无通知铃铛/全站搜索/时区切换/锁屏,用户菜单无个人中心/文档/GitHub/
ann.vben/Pro;④ 顶栏退出按钮 → 确认弹窗「是否退出登录?」→ 回登录页,**后端
事件流出现 `logout: admin` 审计**(logoutApi 显式 Bearer 修复的真实效果,
此前该事件从不记录);⑤ 登录后访问未知路径渲染 404 兜底页(哎呀!未找到页面/
返回首页)。附注:vben 偏好持久化于 localStorage 且旧缓存覆盖新 override,
旧浏览器升级后需清一次站点数据(框架已知行为,已记入 FRONTEND-NOTES)。

## 13. 已知限制(未验证或带条件交付)

1. ~~前端生产构建空白视图~~ **已修复(2026-10-09)**:根因是 pnpm 依赖图中
   vue-router 存在两个 peer 变体实例(app 一份、@vben/* 一份),生产打包
   (rolldown)保留两份 → 两套 routerKey 注入符号,布局内 `useRouter()` 注入
   失败。修复:web-antd `vite.config.ts` 增加 `resolve.dedupe`(vue/vue-router/
   pinia/@vue+devtools-api),构建产物单实例;生产构建浏览器验收全链路通过。
2. `ssh -R` 会话通道对 exec/shell 请求仅作会话保持(隧道场景不执行命令),
   `ssh -N` 与 `ssh ... sleep N` 均可正常建立转发。
3. TLS/WS/WSS/QUIC/KCP 主控传输与 **HTTPS vhost(SNI)** 均已有自动化 E2E
   (2026-10-09,e2e-transports 12/12×3、e2e-vhost 10/10×3),其中证书均为
   测试自签;公网部署请使用 tls 协议 + 正式 CA 证书。
4. **OIDC 已按用户决策移除(2026-10-09)**:实现与 e2e-oidc 套件一并删除;
   移除前曾以本地 IdP 桩完成 8/8×3 联调(含 JWKS 发现/RS256 验签/三种拒绝
   路径,并修复两个真实缺陷,见 review-log R11);移除后客户端认证唯一方式
   为密钥 HMAC 挑战应答,相关回归全绿(见 R12)。
5. ~~头改写/Basic 鉴权/自定义 404 未单列用例~~ **已关闭(2026-10-09)**:
   e2e-vhost 逐项断言(上游侧请求头、401/放行、自定义 404 内容)。
6. ~~七插件缺自动化 E2E~~ **已关闭(2026-10-09)**:e2e-plugins 15/15×3,
   十插件(unix_domain_socket/static_file 见 e2e.sh)全部有 E2E。
7. macOS ControlCenter 占用 7000 端口,本机调试默认使用 7100+。
8. 客户端被服务端拒绝(如轮换后旧密钥)时,错误显示为
   "server error (unparsable)"(应为 AUTH_FAILED 文案):拒绝行为正确,展示待优化。
9. vben 自带 `check:circular` 在上游包(form-ui 等)存在既有类型循环警告,
   属上游状态,未修改;`typecheck` 为 0 错误。
10. 前端产物仍含框架层(packages/**)默认值字符串:`@core/preferences/src/config.ts`
    的 unpkg static-source 默认 logo/avatar URL、`copyright.vue` 默认
    `companyName:'Vben Admin'`,以及 common-ui 导出的 3 个 `profile-*.js` chunk。
    运行时均未启用(logo/copyright 已关),彻底移除属清理任务书阶段 3(需用户确认,
    默认不执行);升级部署后旧浏览器 localStorage 偏好缓存需清理一次(§12 第四轮附注)。

## 14. 内嵌 Web 控制台(单二进制,`tests/e2e-embed.sh`,9/9 PASS,2026-10-10)

> **路径已变更**:当时的检测路径是 `web/apps/web-antd/dist`,现已改为
> `console/dist`(`crates/rh-server/build.rs`)。内嵌机制、SPA 回退与
> `/api/*` JSON 404 语义不变,`static_dir` 仍优先于内嵌。

构建期 `build.rs` 检测 `console/dist` 存在 → `rust-embed` 嵌入
rh-server(release 二进制 14.3MB→17.5MB,+3.2MB);无 dist 的纯 Rust 检出
`cargo check --release` 通过(CI 后端 job 不依赖前端产物)。用例:

- 启动日志声明 `embedded ui(files=156)`;根路径返回内嵌 index(html 200);
- 深层路由 SPA 回退 index(200);hashed JS 资产可取(200+MIME+体积);
- 未知 `/api/*` 返回 JSON 404 envelope(不混 HTML);favicon 可取;
- 配置 `static_dir` 时不启用内嵌分支,磁盘内容覆盖内嵌(优先级正确)。

## 15. 整体代码评审轮(R17,2026-10-10)

四域全量审查后的修复回归(fmt/clippy 0 警告/单测 42/42,release 重建):

| 套件 | 结果 |
| --- | --- |
| e2e.sh(主链路 + 新增 metrics 认证/未认证拒绝 2 项) | 56/56 |
| e2e-transports.sh(tcp/tls/ws/wss/quic/kcp 全链路) | 12/12 |
| e2e-vhost.sh | 10/10 |
| e2e-resilience.sh | 19/19 |
| e2e-plugins.sh | 15/15 |
| e2e-embed.sh | 9/9 |
| xtcp-isolated.sh | 9/9 |

本轮新增已知限制(详见 review-log R17"过程如实记录"):KCP 每源 IP 并发会话
上限未实现(以对端锁定 + 句柄归零快速回收缓解);TCPMux 标准模式端到端 e2e
仍仅到 CONNECT 200(yamux 客户端语义,流路径由 mux.rs 单测覆盖);web 前端
问题因"前端将重写"决策不处理。

## 16. 现役控制台质量门(`console/`,2026-10-22 实测)

2026-10-12 起控制台为 `console/` 独立工程(Vite 7 + React 19 + TS strict +
Tailwind v4 + shadcn/ui)。以下为本次实际执行结果(本机 Node 26.11 / pnpm 11.16,
`console/` 下执行):

| 门 | 命令 | 结果 |
|----|------|------|
| 类型检查 | `pnpm typecheck`(`tsc --noEmit`) | **通过,0 错误** |
| 单元测试 | `pnpm test`(`vitest run`) | **50/50 通过**(`features/services/__tests__/spec.test.ts`,覆盖 spec↔表单值组装/反解与 kind 字段矩阵) |
| Lint | `pnpm lint`(`eslint .`) | **0 error / 5 warning**;5 条均为 `react-refresh/only-export-components` 与 `react-hooks/incompatible-library` 的 HMR 粒度提示,非正确性问题 |
| 生产构建 | `pnpm build` | 通过;`index.html` 仅一条 CSS preload,图表(`traffic-chart` 366 KB / 108 KB gz)与表单(`form` 129 KB / 40 KB gz)按需懒加载 |
| 格式检查 | `pnpm format:check` | **未通过**:`index.html` 与 `src/pages/settings/index.tsx` 有格式偏差(纯空白/换行,`pnpm format` 可修) |

**注**:本节的 50 条用例是 2026-10-12 重写时随 `features/services` 抽出的纯逻辑测试,
与 §2 中 vben 版的 575 条(vue 组件测试)不是同一口径,不可直接对比。
浏览器走查证据见 `docs/screenshots/console-shadcn/`(40 张,生产构建 + 真实后端)。
