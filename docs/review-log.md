# rabbithole 评审日志(review-log)

> **覆盖范围**:R1(2026-10-08)~ R33(2026-10-12),共 33 轮。
>
> **2026-10-13 之后的评审不在本文**,分散在下列专题文档中,查找时请一并检索:
>
> | 时段 | 内容 | 文档 |
> |---|---|---|
> | 10-13 ~ 10-15 | 性能修复与门槛判定(G1/G2/G6) | `performance-repair-*.md`、`performance-g1-tcp-*.md` |
> | 10-15 ~ 10-18 | 多载体/单载体瓶颈实测 | `single-carrier-bottleneck-measurement.md`、`multicarrier-autosizing-handoff.md` |
> | 10-20 | 七路静态深审(A/B/C 三批) | `deep-review-ffd49d3-remediation-plan.md` |
> | 10-21 | 深审后对 frp 吞吐复测 | `deep-review-performance-report.md` |
> | 10-22 | 静态审查(1 P0 + 14 P1 + P2/P3)与插件代理语义重写 | `review-2026-10-handoff.md`、`fixed-plugin-proxy-semantics.md` |
>
> 本文中 R20–R33 涉及的前端实现(Umi Max + antd)已于 2026-10-12 被
> Vite + Tailwind v4 + shadcn/ui 重写取代;**行为契约(登录路径、术语、
> 密钥零输入、错误提示可见性等)仍然有效**,组件与文件路径不再对应。

## R1 功能对照审查(frp 0.71.0 基线)

- 以 /tmp/ref/frp(d20a232)为基线逐文件对照,产出 docs/requirements-matrix.md
  (A/B/C/D/E 五域 60 项 + 3 实验项),排除边界:frp 协议互操作、INI legacy、
  存量 frpc/frps 互操作。
- 结论:代理类型/传输/认证/转发能力/集中管理五域全部有实现落点;
  部分项在实现完成后按"实现+测试+证据"标准回填状态。

## R2 SUDP 间歇丢帧根因评审(本会话)

**症状**:SUDP visitor 在 E2E 脚本中约 1/3 概率收不到回显,独立环境通过;
加入打印日志后概率显著变化(时序敏感)。

**排查过程**(全部有日志证据):
1. 最初怀疑上一会话遗留:客户端二进制陈旧(上一会话最后修改 udpchan.rs 时引入
   编译错误,`cargo build` 输出被截断掩盖,rh-client 未重编)——修复编译后复测仍失败。
2. 最小化复现环境(仅 udbs+visitor)以 ~30% 概率失败,排除多服务互扰。
3. 字节级插桩后取得决定性证据:客户端已把 UDP 帧写入通道(`vs flushed n=5`),
   服务端 splice 两端就绪,但代理侧从未收到该帧(`ps pkt` 缺失)。

**根因(服务端 read_frame 残留字节丢失)**:控制连接数据接受任务读取
DATA_CHANNEL_HELLO 时,`read_frame` 可能将同批到达的后续帧留在本地缓冲;
路由把裸流交给下游后缓冲被丢弃。客户端"写 HELLO 后立即冲首包"与本服务端
"读 HELLO 后路由"竞速:同 TCP 段到达时首包被吃。eprintln 恰好隔开两次写,
故加日志即"修复"。客户端侧同类问题在前一会话已以 leftover 前传修复,
服务端两处(mux 数据流接受、裸模式首帧)此前遗漏。

**修复**:
- `PrependedIo` 下沉到 rh-transport 共用;服务端 mux 数据接受与裸模式
  首帧路由均以残留字节前转(control.rs)。
- 顺带修复同类时序:`relay_tcp/relay_udp` 的配对表登记移至 CREATE 帧发送之前
  (state.rs `issue_relay_pair`),消除 localhost 下客户端先回环、配对表未及登记
  而静默丢流的窗口。
- `warm_proxy` 不再对 sudp 预热常驻 UdpChannel(sudp 无服务端监听,预热通道
  被即刻丢弃,徒增噪声)。
**验证**:最小复现 24/24 通过;全量 E2E 多遍 43/43(debug+release)。

## R3 长稳缺陷两处(负载测试发现)

1. **90 秒心跳无条件下线**:服务端心跳 arm 在 interval 到期时无条件
   `break Err("heartbeat timeout")`,未跟踪最近收帧时间 → 每个会话每 90s 被杀。
   E2E(~70s)与 60s 负载均在窗口内,未暴露;90s+ 负载每 90s 全量连接重置。
   修复:30s 检查一次,仅当 90s 无任何客户端帧才判死(与注释/设计一致)。
2. **数据通道限额只增不减**:`data_conn_count` 仅在路由时 fetch_add,流关闭后
   不归还 → 单会话累计 256 条通道后全部新通道被拒。修复:`QuotaIo` 守卫流,
   流丢弃即释放限额(池内空闲通道按存活期占用,语义正确)。
   修复前 30s 负载 300 次重置/1280 轮;修复后 10 分钟 491,560 轮 0 错误。

## R4 单元测试修复(不得以放宽断言换取通过)

- proxyproto:encode_v2 端口/地址顺序与规范(及解码端)相反 → 修编码;
  parse_header 对 6–11 字节的 v2 签名前缀误报 NotProxyProtocol → 修前缀判定。
- stun 测试服务器 XOR-MAPPED-ADDRESS 长度字段计算错误(8 字节体记作 4)→ 修正。
- ratelimit 断言"两次 take 之间零补充"与微秒流逝现实矛盾 → 改为确定性上界断言。
- tcpmux 增加 yamux 流路径集成测试(见 acceptance §3)。

## R5 SSH Tunnel Gateway 实现评审(russh 0.64)

- 认证:密码=客户端密钥(常量时间比较)、公钥=authorized_keys(按 key_data 比较,
  **修复**:ssh-key PublicKey 派生相等含注释字段,authorized_keys 行注释与线上
  无关,初版比较恒假)。
- 会话通道:接受 channel_open_session 并对 exec/shell 回 success 仅作会话保持
  (**修复**:默认拒绝会使 `ssh -R ... <cmd>` 在注册后立即退出、监听被级联释放)。
- forwarded-tcpip 的 connected_address 必须与注册地址串一致(**修复**:初版传空串,
  OpenSSH 按该字段匹配转发注册,一律拒绝导致 RST)。
- 断开级联释放:会话结束取消全部监听 token;`-R 0:` 从端口分配器自动分配。
- E2E:注册/数据/释放三用例全过。

## R6 浏览器验收复验(后端多轮修复后)

以真实后端(本机 release server)+ vben dev server 复验全链路:
登录 → 总览真实数据 → 建客户端(凭据仅展示一次)→ 启动真实 rh-client →
在线状态/版本显示 → 建 TCP 规则 → 真实转发验证 → UI 停用(端口实测关闭)→
启用(转发实测恢复)→ 轮换(旧会话实测下线)→ 总览统计复核 → 登出(204+清令牌)。
期间发现"总览短暂显示全零"为硬导航后 2 秒内抢读(数据未返回),刷新后正确,
非缺陷;登出验证沿用等效链路(下拉菜单在自动化下不渲染,调用同一 API+清令牌+回登录页)。

## R7 文档与交付审查

- requirements-matrix 全部行按"实现+测试+证据"回填;无证据项标 partial 并在
  acceptance.md"已知限制"逐条注明(不虚报 done)。
- 新增 README、deploy/(server/client 示例配置 + systemd 单元)、CI 工作流
  (fmt/clippy -D warnings/test/release/前端构建/E2E)。
- design.md 附实现状态附录;PROGRESS.md 记录会话 4(全部根因与修复)。

## 遗留事项(不阻塞交付,如实列出)

- 前端生产构建空白视图(acceptance §已知限制-1)。
- OIDC/TLS/WS/QUIC/HTTPS-vhost/头改写/6 个插件的自动化 E2E 证据缺口(§限制-3~6)。
- 被拒客户端错误文案 "unparsable" 展示优化(§限制-8)。

## R8 独立审计(设计 §4 十条契约逐条代码核验)与修复

独立审计(不依赖既有测试结论,直接读代码核对 §4 契约)结果:
C-3(SERVICE_UPDATE upsert)、C-4(重复消息可重确认)、C-5(DB→发布→入队顺序)、
C-7(服务端发起数据通道一次性 request_id)PASS;
C-1(bind 失败保留旧状态)、C-2(期望态 enable 判定)、C-6(单事务落库+revision)、
C-8(UDP 屏障在途失效)、C-9(停用/轮换/换会话资源级联关闭)、
C-10(running = 已绑定 ∧ 已确认)存在缺口。修复批次:

- **C-10**:`ProxyRuntime.bound` 原子标志;`spawn_runtime` 绑定成功才置位,
  `bind_error` 保持错误态但照常入队(§4.1"期间收集确认");
  `on_apply_result` 仅 `bound ∧ confirmed≥latest` 才转 running;
  `bind_error_retry` 重试成功即拉起 serve loop 并按 confirmed 直达 running/pendioffline。
- **C-1**:update_service 端口变更路径改为「重叠拒绝 → 白名单 → 内存冲突 →
  **tcp/udp 双试绑(失败不动 DB、不动旧运行态)** → 单事务落库 → 重建」;
  create 同理(tcp/udp 均试绑)。macOS 通配 bind 可与既有具体地址监听共存,
  试绑补**环回探针**(0.0.0.0:P 再试 127.0.0.1:P)对齐 Linux 语义。
- **C-6**:新增 `db.insert_proxy_bump`/`update_proxy_bump`(规则行 + revision+1 同一
  SQLite 事务),api.rs 全部调用点切换;不再存在两步提交窗口。
- **C-2**:禁用客户端上创建 → 仅落库 disabled(不占端口、不推进 revision);
  禁用客户端上规则 enable → 仅置 enabled=1(等客户端恢复批量走全序);
  恢复路径 `spawn_runtime` 补白名单资格判定。
- **C-9**:`teardown_runtime` 调 `invalidate_proxy_requests`(失效该规则在途
  UdpChannel/RelayProxy/TcpPool 请求);`on_session_down` 增会话替换守卫(新会话已
  注册则跳过旧会话清理);`assign_forward` 级联 `rule_cancel`(停用/换端口时
  在途转发即断)。
- **C-8**:UDP 目标变更走 §4.3 原位发布(快照换新 + 入队),通道重建按 §4.5
  代次隔离;在途请求由 invalidate 失效。
- 新增 `publish_spec_in_place`:非端口变更不动 listener/池,原位换快照 + 入队,
  running→pending 等新确认(§4.3"转发不中断")。

修复后回归:cargo fmt --check 通过、clippy --workspace --all-targets 0 警告、
workspace 测试 43/43、release 构建 0 错误、E2E **46/46 连续三遍**
(新增 3 用例:非端口更新原位切换目标 / 通配探针拒绝外部占用 / 拒绝后原规则仍可用)。
定向验证脚本另证:原位更新后 revision 前进、状态回 running、外部占用端口 PUT
收 3005 且 DB/旧运行态零扰动。

## R9 前端生产构建空白视图:根因分析与修复(独立复验)

现象复现:生产构建(dist/)经 rh-server `web.static_dir` 托管,登录页正常渲染、
登录成功并跳转 `#/rabbithole/overview`,随后**主界面空白**(app 容器仅剩
`<div class="ant-app"><!----></div>`),此后任意路由均不渲染;无 window 报错、
无未处理 Promise 拒绝、资源与 /api/user/info 均 200。

根因定位(逐层取证):
1. 控制台捕获到真实异常:`TypeError: Cannot read properties of undefined
   (reading 'beforeEach')`,位于 basic chunk `useLayoutScroll` 的 setup——
   `useLayoutScroll(router = useRouter())` 默认参数求值时 `useRouter()` 返回 undefined。
2. 产物出现**两个同名 useApi chunk**(各含独立的 `Symbol("")` vue-router 注入键)
   → 依赖图中存在两份 vue-router 模块实例。
3. `pnpm why vue-router`:**1 version, 2 instances**——app(@vben/web-antd)解析到
   一个 peer 变体,@vben/*(layouts/stores/hooks 等)解析到另一个 peer 变体。
4. 机理:开发模式 Vite 依赖优化把 vue-router 合并为单份,故 dev 正常;生产
   (rolldown)按各包的真实解析路径打包,保留两份 → 两套 routerKey 注入符号 →
   app 安装的 router 用 A 符号 provide,布局内 useRouter() 用 B 符号 inject →
   undefined → 渲染中止。登录页不含该布局,故登录链路正常,掩盖了问题。

修复:
- `apps/web-antd/vite.config.ts` 增加 `resolve.dedupe: ['vue','vue-router','pinia',
  '@vue/devtools-api']`,构建期强制单实例(修后产物仅一个 useApi chunk)。
- 顺带修复托管行为:SPA 回退改回 **200**(原 tower-http `not_found_service` 语义
  返回 404 状态 + index.html 体);未知 `/api/*` 路径返回 JSON 404 envelope
  (不再混入 SPA HTML);路径穿越经 ServeDir 规范化(仅回退 index)。
- 前端既有类型错误清理:clients/services/visitors 三个视图 `bodyCell` 槽位
  `record` 为 `Record<string, any>`,调用处理器需 `as XxxRow` 断言,共 13 处,
  `typecheck` 修后 0 错误(此前该门实际未通过,系此前记录失实,一并纠正)。

复验(全部真实现场执行):
- 干净重建 dist ×2 均单 useApi chunk;`typecheck` 0 错误。
- **生产构建 + 真实后端浏览器验收全链路通过**(acceptance §7):登录/总览真实
  统计/建客户端取一次性凭据/启动真实 rh-client 显示在线/建 TCP 规则 running/
  真实转发 ECHO:/改目标即时切换 SECOND:/停用端口实测关闭/启用恢复/轮换后旧会话
  被拒、新密钥恢复在线且转发可用/统计复核/登出后刷新无自动登录;负向:错误密码
  提示、未登录路由守卫重定向、空表单校验。
- Rust 回归:fmt --check 通过、clippy 0 警告、test 43/43、release 构建 0 错误、
  E2E 46/46 两遍(main.rs 托管改动后全量回归)。

## R10(2026-10-09)XTCP 隔离网络验证、韧性 E2E、传输变体 E2E、前端 lint/vitest

### XTCP 隔离网络(tests/xtcp-isolated.sh + tests/natgw.py,9/9 ×3)
用户态 STUN+双 NAT 网关验证打洞直连(网关字节级 relayed=1094)与拦截回退
(blocked=50/relayed=0 → 服务端中继)。暴露并修复 5 个真实缺陷:
1. 打洞失败 reissue 无限重入(等待连接 10s TTL 过期):NatCoordinator 增加
   `mark_attempted`((session_id, user_conn_id) 60s 去重),仅首次进入打洞,
   失败即回退 issue_relay_pair;
2. relay_pairs 按裸 request_id 键控,两会话各自 rid=1 互相覆盖 → 中继一侧静默
   丢流:改键为 (session_id, request_id)(STCP e2e 单会话故未暴露);
3. punch_listen 无超时,每次失败打洞泄漏任务+socket:2×punch_secs 超时;
4. multiplex=false 裸模式全坏("bad magic"):服务端 peek 前 4 字节自动识别
   raw(RHTP)/yamux,peek 字节经 PrependedIo 转发;
5. 恢复期对固定端口也重分配(违反 §4.6):重分配收敛到 port_auto 规则。

### 韧性 E2E(tests/e2e-resilience.sh,19/19 ×3)
新增 §六.C 五场景独立套件(目标拒连/DB EXCLUSIVE 锁故障回滚/仅数据通道断开
(tcpkill-mm.py)/迟到连接/旧 ACK 迟到(冻结跨版本)/重复上线)。两个关键发现:
- **测试侧教训**:echo 服务器未显式 `finally: c.close()` 时,CPython 线程对象
  `_args` 延迟释放 socket,FIN 不达 → 转发"超时"。曾误判为产品缺陷并以
  yamux 帧级追踪排查数小时;e2e.sh 通过恰因它的 echo 显式关闭。测试基建已修正
  并在脚本内注明。
- **真实缺陷(重复上线)**:会话替换后 on_session_down 对被替换会话早退保护新
  会话(正确),但池内旧会话就绪通道无人驱逐,tcp_refill 见 ready≥want 不补建
  → 用户连接弹出到冻结旧通道形成黑洞。修复:池条目携带建立会话 epoch,
  get_channel/tcp_refill 以单调 epoch **前向认领**并驱逐旧条目,迟到的旧会话
  通道在 tcp_on_channel 丢弃(state.rs TcpPool + data.rs claim_pool)。

### 传输变体 E2E(tests/e2e-transports.sh,12/12 ×3)
六协议(含 tcp 对照)独立环境全链路(上线+池+转发+半关闭)。暴露真实缺陷:
TLS/WSS/QUIC 自动自签证书落盘前未创建 `<配置目录>/certs/` → 监听 ENOENT 失败;
tls.rs auto_cert 先 create_dir_all。矩阵 B2/B3/B4/C9 由 partial 转 done。

### 前端 lint + vitest(§六.B)
- 恢复裁剪时丢失的上游根级 lint 配置(eslint/stylelint/oxfmt/oxlint/.stylelintignore
  均按上游 d27a5b8 原样);修复 pnpm 11 设置位置(neverBuiltDependencies→
  workspace yaml;allowBuilds 恢复但剔除 lefthook,防向父仓库写 git hooks);
  element-plus 精确 2.14.5 回归 catalog + form-ui devDependency(上游测试深路径
  依赖);清理 16 项无消费者 catalog 条目;全仓 eslint/oxfmt/oxlint --fix 收敛
  (发现 oxfmt format-mode 与 eslint sort-keys 在 package.json 上互改,以
  oxfmt.config.ts sortPackageJson:false 为准的上游方案恢复后不再冲突)。
- 结果:`pnpm run lint` exit 0(oxlint 0/0、oxfmt/stylelint 全绿、eslint 0 错误,
  余 16 警告均为上游框架测试文件);`pnpm run test:unit` **575/575**;
  typecheck 0 错误;build:antd 通过 + dist 经 rh-server 冒烟(index/深路由 200,
  /api 未授权 401)。

### 回归(全部真实现场执行)
cargo fmt --check 通过 / clippy --release --all-targets 0 警告 / test 43/43 /
release 构建 0 错误;tests/e2e.sh **46/46**;e2e-resilience **19/19**×3;
xtcp-isolated **9/9**;e2e-transports **12/12**×3。副产物:relay_pairs 复杂类型
抽别名消 clippy type_complexity;nathole_cli 超时代码 fmt 对齐。

## R11(2026-10-09)插件/vhost 进阶/OIDC 三套件 E2E:关闭全部 partial 行 + 三个真实缺陷

### tests/e2e-plugins.sh(15/15 ×3)
七插件(http_proxy/socks5/https2http/https2https/http2https/http2http/tls2raw)
经 API 创建插件规则全链路验证:改写类在上游侧逐头断言(Host 重写、set、del、
响应头注入);TLS 类用自签证书(curl -k / python TLS);代理类本地辅助服务
断言握手细节(CONNECT 目标与 Basic 凭据、SOCKS5 方法协商/用户密码/域名目标)
及隧道回显,含错误凭据 407 负向。教训:插件 JSON kind 为 serde 变体名
(https2_http 等),与 PluginKind::as_str 展示名不同。

### tests/e2e-vhost.sh(10/10 ×3)与 peek_sni 真实缺陷
自签证书 + curl --resolve 验证 HTTPS SNI 双域名分流/未知 SNI 拒绝、HTTP 头改写、
Basic 鉴权、自定义 404。**暴露真实缺陷(该路径此前零 E2E)**:peek_sni 用
try_read(消费 ClientHello,后续 copy_bidirectional 丢失握手字节)且把
WouldBlock 当错误直接断连。修复:改用 async peek(不消费)+ WouldBlock 容忍
等待 + 5s 截止。修后双域名分流与未知 SNI 拒绝均通过。

### tests/e2e-oidc.sh(8/8 ×3)本地 IdP 桩与两个真实缺陷
纯 python + openssl CLI 实现 IdP 桩(发现文档/JWKS RS256//token 可定制
sub/aud/ttl)。**暴露两个真实缺陷**:
1. 服务端 JWKS 获取丢弃 URL 显式端口(`http://127.0.0.1:30901` 连成
   `…:30901:80`,getaddrinfo 失败)——oidc.rs http_get_json 端口解析修复
   (仅纯数字尾巴拆端口,避免误拆 IPv6 字面量);
2. 客户端 hello 首帧仅接受 ERROR/AUTH_CHALLENGE,oidc 路径服务端验签通过
   直接回 HELLO_ACK,客户端报 unexpected frame 退出——OIDC 客户端链路此前
   从未端到端可用。修复:首帧接受 HELLO_ACK(挑战路径不变)。
另补服务端 oidc 拒绝 warn 日志(原因字符串,不含令牌;已验证输出无 JWT 内容)。

### 回归(全部真实现场执行,最终二进制)
cargo fmt --check ✓ / clippy --release --all-targets 0 警告 / test 43/43 /
release 构建 ✓;e2e.sh **46/46**、e2e-resilience **19/19**、xtcp-isolated
**9/9**、e2e-transports **12/12**、e2e-plugins **15/15×3**、e2e-vhost
**10/10×3**、e2e-oidc **8/8×3**。requirements-matrix **partial 清零**
(A4/B9/C11/C12/C17~C24/C27 全部 done),acceptance 新增 §9/§10/§11 并将
已知限制 3~6 改为已关闭。

## R12(2026-10-09)按用户决策移除 OIDC 功能

移除范围(代码/协议/配置/测试/文档全链):
- 删除 crates/rh-server/src/oidc.rs、control.rs auth_oidc 与 hello.auth_method 分支、
  config.rs OidcConfig/[auth] AuthConfig.allow_oidc(该字段无任何读者)、db.rs
  get_client_by_name(仅 oidc 使用)、control.rs NoVerifyTls(仅 JWKS https 用)。
- 协议:Hello 删除 oidc_token 与 auth_method 字段(自有协议,直接改版);
  客户端 hello 流还原为仅挑战路径,config 删除 oidc_token。
- 测试与配置:删除 tests/e2e-oidc.sh;CI 移除对应步骤;deploy 两个示例移除
  oidc 相关键。jsonwebtoken 保留(管理端 JWT 会话使用)。
- 文档:矩阵 B9 → excluded(注明用户决策与移除前验证史);acceptance 删除
  E2E-7 章节并重编号、限制-4 改写为移除记录;design.md P2 清单与遗留句同步。

移除过程事故与恢复(如实记录):脚本化删除 auth_oidc 时括号配平算法误配
(函数签名跨行,首个 `{` 前深度已归零),第一刀切掉了 auth_and_run 的挑战路径
主体。恢复方式:上下文中存有 hello 分发/挑战签发/会话注册尾段的原文本,
缺失的 AUTH_RESPONSE 验证段按协议契约(session.rs 客户端公式
HMAC-SHA256(secret,"rh2:{client_id}:{nonce}") hex + ChallengeTable.consume
一次性消费与归属校验 + Hmac::verify_slice 常量时间比较)重建,并以全量
E2E 验证(e2e.sh 46/46 含错误密钥拒绝/轮换中踢线等认证用例)。
教训:大段删除优先用 Edit 的唯一锚点匹配,不用行号/深度扫描脚本。

回归(移除后,全部真实现场执行):cargo fmt --check 通过、clippy 0 警告、
test 42/42(少的 1 例为 oidc 单测随文件删除)、release 构建 0 错误(仅本机
rust-objcopy/libLLVM 既有剥离告警);e2e.sh 46/46、e2e-resilience 19/19、
xtcp-isolated 9/9、e2e-transports 12/12、e2e-plugins 15/15、e2e-vhost 10/10。

## R13(2026-10-09)首启引导 admin + 控制台改密(用户指令)

用户指令:原流程需手动 `rh-server admin create` 建号,被判定不合理;要求首次
启动自动创建 admin(随机密码),并确认改密能力(当时不存在)。

实现:
- 服务端 `bootstrap_initial_admin`(main.rs):users 表为空时创建 admin,
  密码 = 两个 UUID v4 simple 拼接收窄 24 字符;**仅 stdout 横幅一次性展示**,
  tracing 事件不含密码;已有用户即跳过(重启幂等)。db 新增
  count_users / update_user_password。
- `POST /api/auth/password`(api.rs):require_auth → 验旧密码(argon2,错则
  code=1001 + "password change rejected" warn 审计)→ 新密码 ≥8(code=1002)→
  落库 → **轮换 web_jwt_secret**(所有已签发 JWT 立即失效)→ 审计
  "password changed: {user}"。
- 前端:api changePassword;新增「设置」页(rabbithole/settings)改密表单
  (当前/新/确认,前端校验 ≥8 与一致性),成功后提示并 authStore.logout()
  强制重登;README 快速开始去掉 admin create 步骤。

缺陷与事故(如实记录):
- **e2e.sh 假失败(工具链问题,非产品缺陷)**:新增 boot 用例首跑全挂,经
  $BOOT/binary.sha 指纹定位为脚本默认 BIN_DIR=target/debug——本机 debug 目录
  存在 R12 之前的陈旧二进制(无 bootstrap)。默认改为 target/release(与 README
  一致),另两处 curl JSON 单引号内 `\"` 转义传成字面反斜杠致假登录失败,改
  双引号。
- **设置页空表单误显灰色"两次输入的新密码不一致"**:antd FormItem `help`
  恒渲染(validate-status 只控色),浏览器验收发现;改条件渲染
  (:help/:validate-status 仅 mismatch 时给值),复测空=无提示、不一致=红字
  红框(h=22,color=error)、一致=消失。

浏览器验收(生产构建 + static_dir 托管,同源 /api):横幅随机密码登录 →
侧边栏「设置」→ 提交改密 → toast「密码已修改,所有会话已失效,请重新登录」+
自动登出(保留 redirect)→ **新密码重新登录成功**并回跳设置页。
工具备注:IAB 下 Playwright locator click 的 actionability 等待在本应用挂起
(登录/菜单/提交按钮),fill 与快照正常;改 DOM 级 evaluate(el.click())或
坐标点击(按钮实际位于分栏右半 x≈1013)后全流程通过——此前多轮"点击无效"
实为坐标点在左侧营销面板 + locator 等待挂起的叠加。

回归(改动后全部真实现场执行,fmt 修复触发的全新 release 构建):
- cargo fmt --check 通过(本轮曾发现 db.rs update_user_password 格式偏差,已 fmt 修复);
  clippy --workspace --all-targets 0 警告(rh-server 重编译复核);test 42/42。
- e2e 六套件(全新 release 二进制):e2e.sh **55/55**(含 boot 9 例)、resilience
  19/19、transports 12/12、xtcp-isolated 9/9、plugins 15/15、vhost 10/10,合计 120。
- 前端门禁(设置页修复后):typecheck 0 错、lint exit 0、build 通过、浏览器验收
  生产构建全链路(见上);vitest 全量三连跑 1/1/0 例失败,第三次 **575/575 全绿**
  ——失败均为上游 form-integration.test.ts 时序偶发(文件与上游字节级一致,单文件
  5 连跑 3 次失败且每次用例不同),未屏蔽未跳过,证据见 acceptance §2。

## R14(2026-10-10)前端模板残留清理(任务书 FRONTEND-CLEANUP-PROMPT.md,阶段 1+2)

按用户提供的任务书执行(阶段 3 按其自身规定默认不执行,未征求确认不动 packages/**):

- **P0**:`allowBuilds.lefthook` 占位符 → 显式 `false`,`pnpm install
  --frozen-lockfile` exit 0(CI frontend 首步恢复可信)。
- **阶段 1**:删演示页(dashboard/demos/about/profile/4 登录变体/3 兜底)、演示
  路由与 locales、vxe-table adapter、上游 CHANGELOG;登录页显式关闭全部演示入口;
  basic.vue 删假通知与品牌菜单;preferences 关闭 copyright/widget 四项/
  checkUpdates、panel-center、logo 关闭并以空串覆盖 unpkg 默认;.env 换标题与
  随机 STORE_SECURE_KEY;index.html 删百度统计/__bootErrs/品牌 meta;自有
  favicon;vite license:false;authentication.json 覆盖登录页文案。
- **死代码与真实缺陷**:删菜单下发链与 refreshToken 链路;**修复 logoutApi 不带
  Bearer 导致 `logout: admin` 审计事件从不记录的真实缺陷**(任务书 1.7-5,浏览器
  冒烟验证事件已落库)。
- **阶段 2(保守 A + 二阶)**:adapter/form.ts 缩至 initSetupVbenForm、access.ts
  删 pageMap/layoutMap、bootstrap 删 v-loading/v-access/tippy 注册、resetRoutes/
  IFrameView 删除、fallback 组件名去 Demo。
- **门禁与产物**:lint 0 / typecheck 0 / vitest 575/575(上游 flake 如实披露)/
  build ✓;dist 4.3M→3.4M,chunk 217→147,echarts 674K 消失,演示/品牌/统计
  残留 grep 全空;残留仅框架层默认值字符串(限制-10 如实记录)。
- **浏览器冒烟 5/5**(dev server + 真实后端):守卫、净化后登录页、仅 rabbithole
  菜单 + 五页真实数据、顶栏净化 + 退出弹窗 + logout 审计、404 兜底。
- 过程如实记录:① 排查"copyright 关闭不生效"确认为 vben localStorage 偏好缓存
  覆盖 override(框架已知行为,清缓存即恢复,已写入运维注意);② 任务书 md 本身
  未过 oxfmt,由仓库 format 一并格式化(内容不变);③ 任务书所述根 .gitignore
  死规则(apps/web-antd/dist)经查不存在,跳过并说明。

## R15(2026-10-10)用户可见消息全面中文化(用户指出事件表「消息」列为英文)

- **范围界定(如实)**:用户截图中的英文来自总览「最近事件」表——服务端审计事件
  消息原本全部为英文;同图中的网络提示为框架已本地化的中文 toast(后端停机时
  轮询失败提示)。首轮误判为"英文报错",经用户澄清后按正确范围实施。
- **后端(rh-server)**:19 处 events.emit 消息与 39 处 err()/Err 元组的 message
  全部中文化(登录/改密/登出/客户端与规则增删启停/轮换/离线/健康检查/端口重分配/
  首启引导等);数字 code 契约不变,PORT_CONFLICT/PORT_DENIED/LIMIT_EXCEEDED/
  OVERLAP_ADDR_UPDATE 保留符号前缀便于日志检索(如 "PORT_CONFLICT:端口已被占用")。
  内部 anyhow/诊断日志(stdout/认证协议层)保持英文,不属控制台可见面。
- **前端兜底**:request.ts 错误拦截器对 Network Error/timeout 的英文原文映射中文
  (框架 $t 已覆盖主路径,此为兜底);**新增相同文案 10s 节流 + 任一请求成功即重置**
  ——修复后端失联时轮询页每 5s 连刷 toast 的问题(实测 13s 内 9 次失败仅 2 条提示)。
- **验证**:cargo fmt/clippy 0 警告/test 42/42、release 重建、e2e.sh **55/55**(code
  断言不受消息文案影响);curl 实测错误密码→「用户名或密码错误」、错误旧密→「旧
  密码错误」、事件表全中文;浏览器实测错误登录 toast 中文、事件表消息列中文。
  附:一次失败登录仅一条 toast(曾疑重复,实为测量选择器同时匹配 notice 与其
  content 子元素)。

## R16(2026-10-10)前端内嵌进二进制(用户指令"改")

- 实现:`crates/rh-server/build.rs` 检测 `web/apps/web-antd/dist/index.html`,
  存在则置 `embed_ui` cfg(`rustc-check-cfg` 声明防 unexpected_cfgs 告警)+
  `rerun-if-changed=dist`;`src/embedded_ui.rs` 用 rust-embed 嵌入 dist,
  fallback 处理器语义与磁盘分支对齐(`/api/*` JSON 404、SPA 回退 index 200、
  扩展名 MIME 表);`main.rs` 仅在未配置 `static_dir` 且内嵌可用时挂内嵌分支,
  显式 `static_dir` 优先。依赖:rust-embed 8。
- 关键取舍(如实):**自动检测而非强制 feature**——干净检出(未构建前端)纯
  Rust 构建仍通过(已模拟验证:隐去 dist 后 `cargo check --release` 0 错误),
  CI 后端 job 不受影响;代价是 dist 缺失时构建的二进制不含 UI(启动日志与
  README 均已说明,补 UI 需构建前端后重编)。dist 内容变化经目录 mtime 触发重编。
- 验证:tests/e2e-embed.sh **9/9**(内嵌声明/index/SPA 回退/资产 MIME/JSON 404/
  favicon/static_dir 覆盖优先级/覆盖时不走内嵌);回归 fmt/clippy 0 警告/
  test 42/42/e2e.sh 55/55;二进制 14.3MB→17.5MB。文档:README 快速开始改写
  (单二进制为推荐方式)、deploy 两示例注释、design.md 补记、acceptance §14。

## R17(2026-10-10)整体代码评审:四域全量审查与修复(用户指令"整体代码 review 直到没有任何问题")

四路并行深度审查(rh-server 管理面/转发面、rh-protocol+transport+core、rh-client,
后补 web 前端)共报 71 项候选;逐项人工核实后按"真实缺陷才修"口径处理,Rust 侧
61 项全部落地,web 前端 10 项中 4 个 P1 已修后用户告知前端将重写、其余不处理。

### 安全修复

- **P0 static_file 路径遍历**(plugins.rs):`..` 深度以 root 为参考点(原 `pop()`
  实现从文件系统根判定,`GET /../../etc/passwd` 可读任意文件);并加 canonicalize
  防符号链接逃逸、剥离查询串(缓存戳 URL 不再 404)、大文件流式发送防内存放大。
- **SSH Tunnel Gateway 端口策略绕过**:客户端指定端口现须过 allow_ports 白名单与
  port_in_use 占用检查;绑定地址仅环回(含 localhost);每连接转发上限 16;
  authorized_keys 行尾注释恰为客户端 ID(UUID 形态)时绑定该用户,user@host 类
  默认注释视为全局密钥;主机私钥/自签 TLS 私钥以 0600 直接创建(消除先写后
  chmod 的世界可读窗口);SSH 通道 EOF 向用户侧传播半关闭。
- **/metrics 未认证暴露 + 死开关**:`enable_prometheus` 现真正生效(默认关闭),
  开启后仍需与管理 API 相同的 Bearer 认证;e2e 相应改造(认证拉取 + 未认证拒绝)。
- **登录面**:限速 key 改为 IP+用户名(原仅用户名,可被未认证者锁定真实管理员),
  用户名截断 64 字符防 key 空间撑内存;用户不存在时执行等价 argon2 dummy 校验
  消除时序侧信道。Web/API 监听挂 ConnectInfo(TLS 分支自定义 Connected 实现)。
- **跨租户状态篡改**:APPLY_RESULT/HealthReport 增加属主校验,客户端只能影响
  自己名下规则的状态。STUN 应答校验来源地址(共享 socket 防候选投毒);
  DNS 查询随机 txid + connect + 应答校验(防伪造应答)。
- **挑战表与未认证并发**:ChallengeTable 周期清扫 + 65536 容量上限(连接→收挑战
  →断开不再累积);实现 `unauth_conns_per_ip`(原仅为死配置)。

### 功能性修复(整功能不可用/损坏)

- **TCPMux 标准模式挂错方向**:服务端把 yamux 终结建在到客户端的通道上而用户
  连接被弃置,任何 CONNECT 200 后无数据流;改为在用户连接(带 CONNECT 后同批
  字节回灌)上终结 yamux,每流独立数据通道转发(与 mux.rs 单测拓扑一致);
  passthrough 模式补齐 COMPRESSED 与残留字节回灌。e2e 原只断言 CONNECT 200,
  未覆盖数据流(如实记录)。
- **group 负载均衡从未接线**:groups 表无任何写入方,组选路/共享监听全为死代码,
  同组第二条规则必然 PORT_CONFLICT 或 BindError 死循环;且 `blocking_read()`
  在 async 上下文调用是潜在 panic。现完成接线:spawn/teardown 维护成员表、
  首个绑定成员启动组共享 serve loop(取消权归组,单成员退出不影响)、
  port_in_use 同组豁免、试绑跳过、`blocking_read` 改 `.read().await`;
  group_key 扩展支持 http/https/tcpmux(vhost 组选路)。e2e 原有
  "group LB 全部命中健康成员"用例继续通过。
- **COMPRESSED+残留字节三处遗漏**(forward.rs 常规路径、tcpmux passthrough、
  visitor deliver_relay):指派帧后同批到达的压缩流首块被当明文写出且解压器缺
  首块失步;统一为 `PrependedIo` 回灌进 compress_stream。
- **WS 握手违反 RFC 6455**:Sec-WebSocket-Accept 误用 SHA-256(须 SHA-1),
  标准浏览器/反代全部握手失败;并补 Upgrade/Connection/Version 头校验。
- **xtcp 失效 mux 黑洞**:`open_stream` 失败被映射为"已消费",用户连接静默丢弃
  且坏 mux 永久缓存;改为返还连接走中继回退,打洞 socket 缺失时同样回退重发。

### 可靠性/资源修复

- **QUIC 空闲误杀**:quinn 默认 idle_timeout(30s)==心跳间隔,单向抖动即断;
  idle 提至 90s + 客户端 15s keepalive。
- **punch_dial 任务泄漏**:超时后轮询/接收任务永久发包;abort 通知收尾。
- **KCP**:会话锁定首见对端(防同 conv 注入);KcpIo 全句柄释放即令会话退出
  (原空转至 90s);会话表清理以 same_channel 原子判定(防误删重建会话)——
  该修复首版在持锁路径内二次加锁引入死锁,被 `kcp_session_streams` 测试当场
  挂起捕获,改为调用方直传 own_tx 后全绿;Incoming.remote 修正为真实对端。
- **TCP/QUIC 握手串行**:接受与握手解耦(后台泵 + 就绪队列),慢握手不再阻塞
  后续接入。
- **池等待者误删**:超时 LIFO pop 会删错等待者、新抵达通道被丢弃;等待者带
  唯一 id 精确移除,死信通道回池。
- **relay_tcp 无超时**:与 relay_udp 对齐 15s 超时 + 配对表清理(原任务与表项
  永久泄漏);SUDP 常驻中继登记改先占位后签发(消竞态)。
- **中继压缩一致性**:双端压缩开关统一由签发时决策(purpose 携带),配置不一致
  不再产生静默流损坏;顺带消除每通道全表扫 list_visitors 的热路径查库。
- **UDP 会话映射无界增长**:服务端 gc 移入通道存活期 select;客户端侧按源地址
  会话 60s 空闲回收 + bind 失败不再 panic + IPv6 目标按族绑定。
- **看门狗与退避**:客户端控制链 3×心跳无服务端帧即重连(原死链最长挂 15-30
  分钟);重连退避在健康周期(≥30s 存活)后重置(原只增不减,长期运行断线
  必等 60s)。
- **阻塞 DNS 移出 worker**:自定义 DNS 与系统解析均走 spawn_blocking(原 DNS
  不可达时每连接阻塞 tokio worker 2s+)。
- **API 一致性**:批量 enable 绑定失败回滚 enabled(原产生 DB=启用/无 runtime
  死态且同端口可被抢占);实体读取移入 serial_lock 内(消 TOCTOU);update 拒绝
  bind_port=0(随机端口仅创建时支持);update_client 支持 name/remark;
  tls_opts_from 证书读取失败显式报错(原吞成空 PEM);SQLite 内存库限单连接;
  proxies 坏行跳过不炸全表;登录/metrics 之外补 vhost HTTP 头读超时
  (slowloris)、locations 前缀按路径段边界匹配(/api 不再命中 /apifoo)、
  SNI peek 扩至 16KiB、assign_forward 连接计数 RAII 守卫、udp_on_channel
  锁不跨 await、nathole decide 读取即移除(防双发)、remove_if_current 改
  dashmap remove_if 原子判定、encode_frame 超限拒绝编码。

### 过程如实记录

1. 修复过程自身引入并被测试捕获的回归:kcp 会话表清理的死锁(上见)、SSH
   公钥注释绑定与 localhost 地址误伤 e2e(ssh-keygen 默认注释即 user@host),
   均当场修复;全部修改均经四道门禁复核。
2. deploy/server.example.toml 的 `[metrics] enabled` 为错误键名
   (deny_unknown_fields 下启动即失败),改为 `enable_prometheus`。
3. 未修复而记录的已知限制:KCP 每源 IP 并发会话上限未实现(已以对端锁定 +
   句柄归零快速回收缓解);kcp 载体本身无加密(叠加 tls/wss 使用,文档已明示);
   proxyproto v1 的 `PROXY UNKNOWN` 简式拒绝(仅影响第三方来源头);
   TCPMux 标准模式端到端 e2e 仍仅到 CONNECT 200(yamux 客户端语义,单测覆盖
   流路径);web 前端问题因"前端将重写"不处理。

### 验证

- `cargo fmt --check` / `cargo clippy --workspace --all-targets`(0 警告)/
  `cargo test --workspace` 42/42;
- release 重建后:e2e.sh **56/56**(含新增 metrics 认证/拒绝 2 项)、
  e2e-transports 12/12、e2e-vhost 10/10、e2e-resilience 19/19、
  e2e-plugins 15/15、e2e-embed 9/9、xtcp-isolated 9/9(数值见 acceptance §15)。

## R18(2026-10-10)CUT:删除旧 web/,前端切换 console/(用户指令"移除旧web")

- **删除**:`web/` 整目录(vben monorepo,1.6GB 含 node_modules;git+磁盘),
  连同 pnpm workspace/turbo 配置;R14-R16 的 vben 内改动随目录消失(其交付
  已由 console/ 全量承接)。
- **内嵌接线收敛**:`build.rs` 移除旧路径与 `embed_ui_legacy`/`embed_ui_console`
  cfg,仅检测 `console/dist`;`embedded_ui.rs` 移除 legacy 分支。注:R0 时
  "只改 build.rs"不可行(folder 硬编码在 embedded_ui.rs),当时即两文件同改。
- **产物瘦身**:console 生产构建剔除 sourcemap(新增 `clean-maps.js` 步骤),
  dist 12MB→2.8MB;release rh-server 17.5MB(vben 内嵌)→**16.6MB**(console 内嵌)。
- **文档/配置同步**:根 README(构建命令/console/结构/许可)、
  deploy 两示例 static_dir 注释指向 console/dist、docs/design.md §8 加现状
  说明(§8.1 vben 条款废止,§8.2 契约由 console/ 承接)、e2e-embed.sh 适配
  console 产物特征(id="root"、根路径 /umi*.js 资产)。
- **验证(全绿)**:cargo fmt/clippy 0 警告/test 42/42;release 重编内嵌指向
  console/dist;e2e 全套:e2e-embed 9/9、e2e.sh 56/56、vhost 10/10、
  transports 12/12、resilience 19/19、plugins 15/15、xtcp 9/9(计 130 项)。
- console 侧 typecheck/build exit 0。

## R19(2026-10-10)console 前端 + 后端管理接口专项 review(用户指令"review前端 和 后端新增接口")

范围:console/ 全部页面/组件/api 层/stores/wrappers/config,与 rh-server api.rs 全部
管理端点;前后端契约逐字段比对(ProxySpec/PluginSpec/HttpProxyConfig/HeaderOp/
HealthCheck/BandwidthMode/ProxyProto 的 serde 名与前端拼装、envelope、401 语义)。
契约结论:**完全对齐**(spec 各 section 字段名、plugin 内联 tag=kind、HeaderOp.set、
skip_serializing_if 的 optional 降级、client_id skip_serializing 均一致)。
修复 9 项(真实缺陷才修口径):

后端(api.rs):
- **update_service 缺名称校验**:PUT 可写入空名/超长/非法字符名(创建路径有校验),
  name 是 visitor 目标解析的稳定键 → 补与创建一致的 1-64 [A-Za-z0-9_-] 校验(2001)。
- **update_visitor 目标解析与创建语义不一致**:仅按名称匹配(创建要求名称+类型),
  可静默改绑到同名异类型规则;且找不到目标时静默保留旧绑定 → 改为名称+类型匹配、
  找不到即 4002 拒绝(与创建同语义)。
- **visitor 三端点(update/toggle/delete)实体读在串行域外**:与并发删除竞态时可对
  已删实体 bump revision 并下发迟到 ServiceUpdate(客户端侧可短暂复活)→ 首读仅定位
  属主,锁内重读实体,不存在即 4003(对齐 R17 services 修复模式)。
- **batch_services**:清理死码(拼了没用的假 Bearer HeaderMap);批量 enable/disable/
  delete 现与单个操作一致落审计事件(原先批量操作无事件可查)。
- **menu_all 未鉴权**:其余全部 /api/* 均需 Bearer,补齐(虽只返回常量 [])。
- **list_events limit 未钉区间**:limit<=0 时 SQLite LIMIT 视为无限制 → clamp 1..500。

前端(console/):
- 密钥访问页规则表"目标代理"列实际渲染 allow_users(语义错位)→ 改名"允许访问方"。
- VisitorsPanel 隐藏死刷新按钮(hidden 属性)删除。
- 批量导入无在途防抖,双击重复提交 → running 状态 + 按钮 loading。
- **dev 代理指向 18085 与 deploy 示例 web 端口 8080 不一致**(开箱即联调失败)→
  proxy.ts 三环境统一 8080,console/README 同步。

核实后不改的候选(记录):ProxySpec.client_id 虽可由 body 携带但 update_proxy 不写
client_id 列(无跨客户端改属主风险);协议按 proxy_id/revision 关联而非名字,同名
规则不破坏控制面(不加唯一性约束);onPageChange 在启动期网络抖动时会踢回登录页
(token 仍在,重新登录即恢复,接受的边缘行为);Login 页/clients 页 setFieldsValue
仅触已注册字段且均经浏览器实测无 antd v6 validateFields 挂起。

验证:cargo fmt/clippy 0 警告/test 42/42;console typecheck/build exit 0;
release 重编;e2e 全套复跑:e2e.sh 56/56、embed 9/9、resilience 19/19、
vhost 10/10、plugins 15/15、transports 12/12、xtcp 9/9(计 130 项)。

## R20(2026-10-11)F1 全局流量计量落地(用户实测:总览流量恒 0)

用户报告总览页入/出站流量在有真实转发时恒为 0 —— 即 design-parity F1 记录的
"字段存在但从未计数"空壳展示。本次落地**全局口径**(总览卡片 + 速率图即实数据):

- `Metrics` 新增 `count_user_in/out(n)`(totals 供控制台 + `rabbithole_bytes_in/out_total`
  IntCounter 供 /metrics)。
- `data.rs` 新增 `CountedUserIo<T>`:用户侧 IO 透明包装(read→入站,write→出站,
  不改任何 IO 行为),七个服务端入口统一套用:TCP(serve_tcp_user)、HTTP vhost
  (hyper 连接层包装)、HTTPS vhost、TCPMux passthrough、TCPMux 标准模式(每流)、
  visitor TCP/UDP 中继(splice_relay / relay_udp,取 visitor 侧=用户口径)、
  UDP 规则(udp_serve_loop 内联 recv_from/send_to 计数)。
- XTCP 打洞直连流量不经过服务端,天然不计;SSH 网关流量暂未计量(已知限制)。
- 计数为进程内存量,重启归零;分客户端/分规则(F1 完整口径,需 0002 迁移)仍属 M1 未落地。

验证:e2e.sh 新增断言"流量统计(F1:入/出站字节数递增)"(套件内多类真实转发后
/api/stats/overview bytes_in/out 必须 > 0),57/57;全套 131 项复跑全绿
(vhost 10 / plugins 15 / transports 12 / xtcp 9 / resilience 19 / embed 9);
fmt/clippy 0 警告、test 42/42。

## R21(2026-10-11)管理面功能补全(F1 完整口径/F2 配额/M2/M3/服务器配置,用户指令"补全所有应该有但没有的管理页面的功能")

一次补齐控制台全部"表单在、后端空"的降级项与缺失管理面:

- **A 服务器配置(网页管理,热生效)**:GET/PUT /api/server/config(GET 脱敏不回
  jwt_secret)。热生效子集:ports.allow_ports / max_services_per_client、log.level
  (tracing reload 热切)、ssh_gateway 全项(supervisor 常驻,配置变更即热启停监听)、
  authorized_keys(读写文件,未配置路径时默认 <配置目录>/authorized_keys)。
  保存后合并写回 toml(原文件备份 .bak 一次,注释会丢失;写回失败仅告警不回滚运行态)。
  port_allowed/allocate_port 改读 HotConfig;其余字段(vhost 端口/主监听/web)仍需重启。
- **B F1 分实体计量**:ProxyRuntime 增 bytes_in/out + conns_current 原子计数,
  CountedUserIo 全部七个入口同步累计(tcp/http/https/tcpmux×2/visitor×2/udp),
  http vhost 因连接多路复用改在请求通道侧反向归因;list_services 回规则级字段、
  list_clients 回该客户端全部规则聚合 —— 控制台对应列自动点亮(此前空壳)。
  进程内存量,重启归零(持久化仍未做,DB 迁移路径已在 design-parity 记录)。
- **C F2 客户端配额**:迁移 0002(clients 增 rate_limit/max_conns/max_services/
  flow_limit 列);创建/更新接口收配额并校验(rate 用 BandwidthQuantity 解析);
  执行点:max_services 覆盖全局默认(create 串行域内计数)、max_conns 与 flow_limit
  在用户连接准入(try_open_user_conn,超限即断)、flow 超限置 flow_cut 位后
  CountedUserIo 下一轮 IO 即断连、rate_limit 在 copy_limited 与规则级限速叠加
  (仅 TCP 族;vhost/UDP 限速不执行,已知限制)。ClientUsage 配额缓存随 CRUD 刷新。
- **D M2 域名增强**:HttpProxyConfig 增 not_found_html + cache(CacheConfig);
  vhost 路由域名命中而路径未命中 locations → 规则级自定义 404;静态缓存按
  (GET+扩展名白名单+200+单项 KB 上限)整读落缓存,容量上限内逐出,X-Cache: MISS/HIT。
- **E M3 证书管理**:迁移 0003(certs 表);/api/certs CRUD(PEM 解析校验证书链与
  私钥配对、SAN/CN 自动提取域名、sha256 指纹、not_after;key_pem 任何响应不回显);
  state.certs 预构建 rustls 配置,SNI 命中(精确/单层通配)→ 服务端终结 TLS 并走
  vhost http 路由,未命中维持原 passthrough(端到端 TLS 到客户端)。
- **F 控制台**:设置页新增「服务器配置」区(SSH 网关表单+公钥白名单编辑+端口策略+
  日志级别);规则列表行选择 + 批量启用/停用/删除(接既有 /services/batch);
  总览事件级别筛选;访问器目标改为同类型规则下拉(带空态提示);客户端页配额
  提示改为"保存即生效"。

验证:cargo fmt/clippy 0 警告、test 42/42;console typecheck/build exit 0;
e2e 主套 65/65(新增 8 项:F1 分规则、配置 401/收紧/拒白名单外端口/SSH 热开热关×2、
日志热切、F2 max_services)与 vhost 套 16/16(新增 6 项:M2 自定义 404、缓存
MISS→HIT、M3 上传不回显私钥、SNI 服务端终结(--cacert 校验)、非法 PEM 拒绝、
删除后恢复 passthrough);其余套件回归:plugins 15 / transports 12 / xtcp 9 /
resilience 19 / embed 9 —— 共 145 项全绿。

已知限制(如实):配额与计量均为进程内存量(重启归零,持久化未做);客户端级
rate_limit 仅在 TCP 族转发执行;SSH 网关流量未计量;HTTPS 证书命中即服务端终结
(与同名 https 规则的 passthrough 语义按"证书优先"裁决)。

## R22(2026-10-11)console UI 重设计执行(用户指令"docs/console-ui-redesign.md 按照这个的要求修改前端")

按设计文档 P0–P4 全量落地(配色不动,§4.2 明示另行立项):

- **P0 去模板脸**:isDev 收缩为纯 NODE_ENV、config.ts 移除 `process.env.CI` 内联
  (V3 生产齿轮根因);删 waterMarkProps;Logo 默认标题 'rabbithole'。
- **P1 Token 与资产**:AlibabaSans 五字重自托管进 public/fonts(+preload,离线零外域);
  antd token {borderRadius:6, borderRadiusLG:8};`.rh-mono` 工具类
  (ui-monospace 栈 + tabular-nums)用于端口/IP/UUID/字节/速率/时间;TopHeader 折叠
  div→button(aria-label)、HeaderPairsInput 删除钮 aria-label;登录页 autocomplete;
  IA 分组(总览/客户端 + 转发组 + 设置,路径全不变)、TagsView 关、Footer 删、侧栏底
  menuFooterRender 显示 `rabbithole v{APP_VERSION}`。
- **P2 页面重排**:总览单卡分格指标条 + 渐变面积图(当前值标注)+ 隧道健康面板 +
  事件表(默认 5 条/展开);ProxyListPage 参数化(信号点徽章 StatusBadge、mono 列、
  空态三件套、分页);ProxyFormModal「自动分配」开关替代魔法值 0、连接/目标/行为
  分组、遮罩加深 rgba(22,35,43,.45)、按钮动词化「创建规则」;客户端页 UUID 后 8 位
  +复制、相对时间、行展开(连接/流量/规则上限/限速);证书页到期倒计时
  (<7 红/<30 橙)+指纹前 8 位复制;设置页服务器配置标题去括号。
- **P3 文案与 i18n 决策 A**:删 SelectLang 与 7×2 locale 文件、oneapi.json,明确
  中文单语;删除确认全部具体对象+后果;登录按钮「进入控制台」。
- **P4 验收(浏览器逐页走查 + 产物审计)**,走查中修复三处:①密钥页规则 Tab 内层
  PageContainer 回退路由名致「密钥访问」同屏两次 → ProxyListPage 增 bare 模式;
  ②证书页空态裸「暂无数据」→ 三件套(上传 PEM 后 SNI 自动匹配说明+主按钮)+分页;
  ③访问器删除确认具体化。

验证:console typecheck/build exit 0;dist 审计无 `process.env.CI`、无外域 woff2、
无水印/SettingDrawer 运行时渲染(ProLayout 库内 "Ant Design Pro" 为被覆盖的死代码,
运行时侧栏实证为 rabbithole);浏览器走查登录/总览/TCP(含弹窗遮罩与自动分配开关)/
客户端(展开行)/密钥(双 Tab)/域名/证书(自签证书实测 89 天倒计时)/设置
(autocomplete current-password/new-password)全过。

已知限制(如实):<768px 卡片降级与触控目标未专项设计(§4.6 部分保留现状);
暗色模式未做(文档明确不在本次范围)。

## R23(2026-10-11)密钥访问凭据单一事实源(用户指出:访问器选了规则还要再输一次密钥,不合理)

问题链:规则密钥非必填且留空无提示(留空 = 服务端跳过校验,静默裸奔);
访问器表单要求手工重输同一密钥——两侧同库同管理员,比对防不了任何外部方
(on_user_connect 中 visitor 查找限定属主会话,访问器仅管理 API 可建),
唯一实际效果是打错字即中继静默断连。

- **后端(visitor.rs 单点)**:访问器密钥留空 = 继承目标规则当前密钥
  (连接时判定,规则轮换天然不失效,无副本无同步);非空仍比对,兼容旧
  显式双写部署。协议注释同步更新。
- **规则弹窗**:密钥默认预填 crypto 随机 32 hex(用户动作从"想密钥"变成
  "复制/换一个"),旁置「复制」「换一个」;清空显红色警示
  "留空 = 不校验密钥,任何客户端的访问器都能中继此规则(不推荐)"。
  复制带非安全上下文(纯 HTTP)execCommand 降级。
- **访问器表单**:删除「访问密钥」字段与列表密钥列;目标字段注明
  "中继密钥自动继承目标规则"。规则列表新增「创建访问器」行内动作,
  跳到访问器 Tab 并预填目标+类型(seed 经 ref 一次性消费,不触发表单重置)。

验证:cargo fmt/clippy 0 警告、test 42/42;console typecheck/build exit 0;
e2e 主套新增 1 项"STCP 访问器密钥留空继承规则密钥"(真实中继)后 66/66,
其余套件 16/15/12/19/9/9 回归全绿,共 146 项;浏览器走查:预填/换一个/
清空警示/创建访问器预填(类型 STCP+目标 db)/访问器表单无密钥字段全过。

语义变化(如实):规则密钥非空 + 访问器密钥留空,此前拒绝、现允许中继
——访问器记录仅管理员可建,选了规则即视为授权,无越权面。

## R24(2026-10-11)密钥完全服务端托管(用户定稿要求:不允许为空、必须服务端随机生成、轮换两边自动、表单零输入、只留轮换按钮)

R23 的"继承式"仍保留输入框,用户裁定进一步收敛为纯服务端托管:

- **后端**:create_service 对 stcp/sudp/xtcp 一律生成 32 hex 随机密钥(OsRng
  UUID v4),**入参提供的密钥一律忽略**;update_service 永不改写既有密钥
  (历史空值顺带补齐)——任何写路径后库中密钥不为空。新增
  POST /api/services/{id}/rotate-secret:生成新随机值落库并推进 revision,
  密钥不参与 listener/池,原位发布快照即可。create/update_visitor 强制
  secret_key=""(访问器永不存副本,连接时继承规则当前密钥,R23 判定保留)。
  ProxySpec 增 secret_cfg_mut。
- **前端**:规则弹窗删除密钥输入(含 R23 的复制/换一个/警示整块),
  仅"允许访问方"保留并注明凭据托管;列表删除"密钥"列,操作列新增
  「轮换密钥」(Popconfirm 说明"服务端生成新随机密钥,访问器自动继承,
  现有中继不受影响",成功 toast 明示无需其他操作)。
- **e2e 重构口径**:stcp/sudp/xtcp 全部不再显式传密钥(与产品口径一致:
  服务端生成 + 访问器继承)。主套新增:入参密钥被忽略(库中为随机 32 hex)、
  轮换端点生成新值、**轮换后中继不断**(真实中继验证继承生效),65→69 项;
  xtcp-isolated 同步去密钥。全量 149 项。

语义变化(如实):API 调用方提供的规则密钥/访问器密钥不再生效(忽略),
密钥只能由服务端生成与轮换;旧库中已存在的显式密钥继续按原值比对工作。

## R25(2026-10-11)Docker 拆分 server/client 双镜像 + 客户端零配置文件接入(用户:"两个 docker 镜像 一个 server 一个 client" + "客户端真的需要配置文件吗")

- **镜像拆分**:ghcr.io/xbclub/rabbithole-server(仅 rh-server + 默认配置
  + entrypoint)与 rabbithole-client(仅 rh-client + entrypoint);旧名
  rabbithole 作为 server 别名继续同步推送,老拉取路径不断。Dockerfile 均
  无 RUN 步骤、COPY --chmod=755,CI 逐架构单平台构建 + manifest 合并免 QEMU。
- **客户端零配置文件**(灵魂拷问的答案:不需要——必填仅 server_addr/
  client_id/secret 三字符串,规则本就服务端集中下发):
  - rh-client 原生 env 覆盖层(RH_SERVER/RH_CLIENT_ID/RH_SECRET/RH_PROTOCOL/
    RH_MULTIPLEX/RH_TLS_INSECURE/RH_TLS_PIN_SHA256/RH_TRUSTED_CA_FILE/
    RH_SERVER_NAME/RH_PROXY_URL/RH_STUN_SERVER/RH_DNS_SERVER/RH_LOG_LEVEL),
    优先级 CLI --server/--id/--secret > env > 配置文件;--config 变为可选
    (回退当前目录 rh-client.toml,再回退纯 env 模式);无任何来源时报错
    带环境变量指引,不静默连默认地址。单测覆盖层叠与布尔解析。
  - client 镜像 entrypoint:挂载文件优先(高级项逃生舱),否则纯 env;
    两者皆无打印最简 docker run 用法后退出。
  - 控制台 SecretModal 重写:创建/轮换后首推「Docker 一键命令」(真实
    client_id/secret 已填、server_addr 可编辑、协议可选),折叠提供 toml
    模板。顺带修复存量 bug:旧模板生成 transport="noise" 字段——
    ClientConfig 为 deny_unknown_fields,该字段不存在,照抄必解析失败;
    且端口硬编码 7000(实际默认 7100)。
  - e2e 新增:纯环境变量客户端上线(经无文件路径)、裸启动报错含指引;
    本地全链路联调(env 接入→上线→隧道回环)实证。
- **CI**:release.yml docker job 重构为双镜像(server 双仓推送)+ 旧名单播;
  workflow_dispatch 触发时仅打 ci-<sha> 临时标签(不覆盖 latest/版本),
  release job 门控 refs/tags(手动触发不再误发 Release)。

验证:cargo fmt/clippy 0 警告、test 43/43(env 覆盖层单测);console
typecheck/build exit 0;e2e 主套 69→71,全量 151/151;浏览器走查创建客户端
→凭据弹窗 Docker 命令/toml 模板切换/复制全过。

## R26(2026-10-11)一键部署脚本 + README 部署步骤修正 + 网页接入指令更正(用户指令)

- **deploy/install-server.sh**:Linux systemd 一键部署——解析 latest 版本 → 下载
  musl 静态包并 sha256 校验 → 安装 rh-server → 首次写入 /etc/rabbithole/server.toml
  (升级保留配置;DB/证书同目录)→ 注册并启动服务;macOS 降级为装二进制+配置并
  打印手动指引(darwin 包)。重复执行 = 升级。
- **deploy/install-client.sh**:同框架;凭据经 --server/--id/--secret(缺省交互式
  询问,secret 不回显)写入 /etc/rabbithole/client.env(0600,重跑备份 .bak =
  轮换路径),systemd 以 EnvironmentFile 接入(零配置文件)。需 v0.2.2+(env 支持)。
  实现:哈希直接比对(校验文件记录原始包名,-c 按名找不到改名后的下载文件——
  实测踩到后修正);--prefix 自定义前缀自动 mkdir。
- **README**:快速开始改为三条部署路径(服务端一键脚本 + journalctl 取首启密码 /
  客户端一键脚本 / 源码构建),Docker 段已有,控制台构建说明移入源码构建小节。
- **网页接入指令(SecretModal)升为三档**:一键安装脚本(推荐,Linux systemd,
  凭据已填)/ Docker 一键命令 / rh-client.toml 模板;协议默认 tls 与服务端脚本
  一致(此前默认 tcp 与脚本模板不一致);脚本 tab 附说明(0600、轮换重跑、版本要求)。

验证:双脚本真机(macOS 降级路径)全链路实测——latest 解析、darwin/musl 包
选择、sha256、安装、配置/凭据落盘、升级备份;脚本产物 end-to-end:server.toml
直接起服(TLS 自签 + admin 引导)+ client.env 以新二进制接入成功上线;浏览器
走查凭据弹窗三档命令(真实凭据/协议 tls)全过;console typecheck/build exit 0。

## R27(2026-10-11)名词 ℹ 说明标志:悬停/点按弹出配置项功能说明(用户指令)

- **InfoLabel 组件**(components/ui.tsx):名词 + InfoCircle 图标,Tooltip
  trigger=['hover','click'](桌面悬停、触屏点按),图标带 aria-label
  「<名词> 说明」(屏幕阅读器可及);说明内容按后端真实行为撰写,不照抄旧文案。
- **全站替换 80 处、括号补充文案(extra=)清零**:规则弹窗 41 项(连接/目标/
  行为/传输限速/分组/健康检查/HTTP 高级/域名增强)、访问器 7、客户端表单 8、
  设置(SSH 网关/端口策略/日志)9、证书 5、规则列表列头 4(状态/池/连接/
  允许访问方)、总览指标条 6(metricCell 增 info 参数)。
- 顺带修正:设置页「服务器配置」卡标题括号说明 → ℹ;客户端弹窗配额灰字
  说明文案 → 「高级配置」ℹ;「域名增强(需服务端 M2)」过时括号 → 去括号
  (M2 已上线)。

验证:console typecheck/build exit 0;浏览器实测——总览「在线客户端」悬停弹出
"已连接服务端的客户端数 / 注册总数"、规则弹窗「允许访问方」悬停弹出凭据托管
说明、点按触发同效;弹窗内 ℹ 图标 9 处渲染正常。(过程中一度误判"弹窗无图标":
实为空环境无客户端时新建按钮 disabled,属正确空态引导。)

## R28(2026-10-12)Docker 基础镜像 debian:bookworm-slim → alpine:3.24(用户指令)

- 两个镜像(server/client)统一换 `alpine:3.24`(当前稳定版,支持至 2028-06)。
- 可行性依据:二进制为 musl 全静态,不依赖 glibc;入口脚本是纯 POSIX sh
  (mkdir/cp/echo/exec 均为 busybox 内建),无 bashism;Dockerfile 本就无 RUN
  步骤,基础镜像不含 ca-certificates 的现状与换基前后一致(TLS 信任走
  rustls 内建根/自签 pin,不读系统证书库)。
- 收益:镜像体积约 75MB → 10MB 级(base 层),拉取更快。
- 验证:workflow_dispatch 触发 docker job 构建成功,ci-<sha> 镜像层内出现
  /etc/alpine-release,二进制与 entrypoint 权限 -rwxr-xr-x 不变。

## R29(2026-10-12)Prometheus 开关进设置页 + /metrics 开发代理(用户反馈)

- 问题一「没有开关」:enable_prometheus 原本只在启动时决定是否注册 /metrics
  路由,控制台只能提示用户改文件重启。现改为路由常驻、处理器按热配置判定
  (未开启 404,不暴露端点存在性),enable_prometheus 进 HotConfig 与
  GET/PUT /api/server/config 热子集,即时生效并写回 toml;设置页「监控指标」
  卡新增 Switch(复用 ['server-config'] query 与 ServerConfigCard 去重,
  失败态沿用 T2 的 Alert+重试模式)。
- 问题二「地址访问走前端页面」:开发模式下 umi(8000)只代理 /api,
  /metrics 打到 dev server 返回 SPA。proxy.ts 三个环境补 /metrics →
  8080;生产本就同源(rh-server 托管控制台)不受影响。
- 验证:cargo fmt/clippy/test、pnpm typecheck 全绿;实测——关:404;开:
  无令牌 401/带令牌 200 指标文本;UI 点开关双向翻转后端热配置并写回
  server.toml;8000 端口经代理访问 /metrics 行为与 8080 一致。

## R30(2026-10-12)Prometheus 端点与管理面解耦:独立端口、无鉴权(用户拍板)

- 结论路径:共享管理台 JWT 做抓取鉴权对监控不可用(默认 12h 过期、改密即废);
  指标内容本身仅为规则级连接数/在线客户端数/累计字节(label 是数字 ID),
  无名称无凭据——不需要令牌,需要的是把暴露面从公网管理口挪开。
- 实现:[metrics] 新增 bind_addr(默认 127.0.0.1)/port(默认 9100),
  /metrics 改为独立 TcpListener 上的无鉴权输出,路由从管理口撤下;
  enable_prometheus 热启停监听器(AppState::metrics_listen_start/stop,
  绑定失败报错且不落配置),写回保持 bind/port 原值;控制台开关与抓取地址
  (按 bind_addr/port 拼接,0.0.0.0 时展示当前主机名)同步更新,
  上轮为共口加的 umi dev /metrics 代理随之撤销。
- deploy 三个示例 toml 补 [metrics] 段(docker 版 bind 0.0.0.0 + 端口映射说明)。
- 验证:cargo fmt/clippy/test、pnpm typecheck 全绿(mime_of 死代码警告系
  本地 dist 缺失导致 embed_ui 关闭,CI 先建 dist 不受影响);实测——开关热启停
  9100(200/拒连)、8080/metrics 404、端口占用时 PUT 报 500 且热配置不落、
  UI 双向翻转 + 写回 toml。

## R31(2026-10-12)Prometheus 监听地址/端口进设置页,热重启生效(用户反馈"配置监听地址的地方呢")

- R30 遗留:bind_addr/port 只能改配置文件重启,界面无入口。现整个
  MetricsConfig 进 HotConfig(含 PartialEq),PUT /api/server/config 的
  metrics 子集扩展为 enable_prometheus/bind_addr/port;地址/端口变更时
  热重启监听器,绑定失败回滚旧值并按旧地址恢复监听(不再静默吞错)。
- 修复实测发现的竞态:metrics_listen_stop 原 abort 后立即返回,旧任务 fd
  未释放就重绑同端口必失败(Address already in use)——补 await 等任务
  真正退出;metrics_listen_start 改为地址由调用方传入,避免与持有 hot
  写锁的调用方互锁。
- 控制台「监控指标」卡新增监听地址/端口表单 + 保存按钮(无改动时禁用),
  保存后抓取地址文本同步;校验:bind_addr 须为合法 IP、port 1-65535。
- 验证:fmt/clippy/test、typecheck 全绿;实测——占用端口保存报 500 且
  旧监听恢复(200)、非法 IP 报 1002、热改端口即时切换(旧关新开)、
  UI 改端口保存后端联动 + toml 写回 + 页面地址更新。

## R32(2026-10-12)移除密钥规则 allow_users 白名单(用户拍板"访问器都配置了,可以自动 allow")

- 依据:访问器仅管理员可建(DB 表 + 管理 API 单一入口),协议 ClientMessage
  无客户端上行注册通道,同步单向(服务端 DB → 客户端);访问器记录本身绑定
  「哪个客户端跑、中继到哪个客户端的哪条规则」——建访问器即授权,规则侧
  allow_users 是第二份事实源,零安全增益,只会自堵(建了访问器忘加白名单即拒)。
- 删除范围:SecretProxyConfig.allow_users 字段(serde 不开 deny_unknown_fields,
  存量 DB JSON 中的旧键反序列化时安全忽略)、visitor.rs 白名单校验块
  (secret_and_allow_ok → secret_ok,改同步函数)、控制台表单项/初始值/回填/
  payload/列表列/死变量 usesSecret;design-parity.md 两处提及改为"已移除"并注明理由。
- 验证:fmt/clippy/test(12 通过)、typecheck 全绿;浏览器实测——新建规则弹窗
  无该字段、规则列表无该列、无 allow_users 的 payload 创建 STCP 规则成功、
  「创建访问器」seed 带出目标并成功落库。secret_key 比对(显式双写兼容)保留。

## R33(2026-10-12)登录页密码错误无提示修复(用户反馈)

- 根因:错误提示由 axios 响应拦截器统一弹出(feedback.ts 模块级出口),
  但绑定者 FeedbackBinder 挂在布局 childrenRender 里;登录路由 layout:false
  不渲染布局 → 直接落地登录页(登出/token 失效整页跳转)时 api 恒为 null,
  报错只进 console。经布局页后再 SPA 跳到登录页的路径碰巧可用(绑定残留),
  掩盖了问题。
- 修复:登录页挂载时补绑同一 FeedbackApi(useEffect),登录页上的所有
  拦截器报错(密码错误/网络错误/5xx)恢复可见;布局挂载后重绑同一实例,无冲突。
- 验证:pnpm build 生产包 + rh-server static_dir 直连,清空 storage 直接
  落地 /user/login,错误密码提交 → 「用户名或密码错误」toast 出现。
  (过程教训:antd v6 message DOM 结构已变,内容节点类名为
  .ant-message-notice-title,按 v5 的 -notice-content 断言"无提示"会误判,
  dev 与 prod 的 toast 实际一直正常。)
