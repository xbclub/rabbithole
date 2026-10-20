# 深审修复方案(2026-10-20,基线 ffd49d3)

七路并行只读审查(server 控制面 / server 数据面 / server API+DB / rh-client /
mux+yamux / 传输载体 / kcp+core+protocol)的结论与修复方案。行号 = ffd49d3
实读,已抽查核对。审查确认**无数据损坏级(P0)**;数据搬送主路径(splice/copy
中继、mux 背压、yamux 零拷贝)质量高,大量历史坑有红线测试锁定,不在本方案
重复列入(见 §7 排除清单)。

风险集中在两处:**生命周期管理**(断连/重连/更新时的清理与重建)与
**配置面默认值**。修复按四批推进,批次内条目互相独立,可并行。

红线(全程有效,沿 C84 口径):
- 不新增 KCP/yamux wire 帧,不伪造 FIN,不动既有红线测试;
- 数字错误码与 JSON 响应形状不变(仅消息细化除外);
- `PROTO`/`RELAY`/`MUX` 语义与 benchmark 证据链不受影响;
- 每项修复必须先有红证(能失败的测试)再改,禁止只改码不补测。

---

## 批次一:P1 止血(10 项,全部有确定性触发路径)

### A1 控制回包队列死锁(PING 洪水可致会话永久卡死)

- 现状:`run_session_loop` 的 select 分支 handler 内联 `sess.send().await`
  (control.rs:784 PING 快路径、797 `handle_client_message`),队列满时
  park;而队列唯一消费者 `qrx` 就是本循环(control.rs:803)→ 队列永满,
  select 不再回顶。心跳超时、断连检测(`sess.cancel`)全部失联,task/fd
  永久泄漏。session.rs:162 `queue_tx.send().await` 是阻塞点,容量 256。
  同族问题:UDP 泵探活 sleep 每包重置(data.rs:1269)。
- 改:PING 回执改 `try_send`,满则计数丢弃(metrics 加 dropped_pong);
  `handle_client_message` 内所有 `sess.send` 包 `timeout(2s)`;UDP 泵与
  客户端 udpchan 探活改 `interval` + `MissedTickBehavior::Delay`(不随
  事件重置,同文件 gc 分支已是正确写法)。长期项(可单列):回包写出拆
  独立 writer 任务,select 只投队列。
- 回归:单测——填满 queue_tx 后发 PING,断言循环仍能处理 rule_cancel 与
  hb.tick;集成——客户端 STOP 读回包 + 狂发 PING 10s,断言会话仍被心跳
  超时拆除、无任务残留(`tokio::runtime::Handle` 任务计数或 endpoint 检查)。

### A2 服务端 UDP 泵在客户端重连后永久失效

- 现状:`close_proxy_channels`(data.rs:43-50)把 `udp_new_channel_tx`
  置 None → `udp_serve_loop` 的 `new_rx.recv()` 得 None 即 return
  (data.rs:1194)→ 泵退出。重连后 `warm_proxy` 签发 CREATE,客户端开通道,
  `udp_on_channel` 里 `tx=None` 静默丢流(data.rs:1165-1168)。泵只在
  spawn_runtime/bind_error_retry 启动,重连不重建运行时。
- 改:泵不因发送端清空退出——`close_proxy_channels` 不置 None 而是换新
  epoch sender(池同款做法);或 `udp_on_channel` 发现 None 时重建 sender
  并 re-注册。最小语义:泵的存活与规则运行态绑定(rule_cancel),与会话
  在线解耦。丢流路径必须 `warn!`(现状静默)。
- 回归:e2e-resilience 加拓扑——UDP 规则跑通 → 断开客户端 → 重连 →
  再发 UDP 包必须通(现有 e2e.sh 的 UDP 用例只单发一包,不覆盖重连;
  这正是漏网原因)。

### A3 unauth 每 IP 名额被已认证会话终身占用

- 现状:`exit_unauth` 在 `auth_and_run` 返回后才执行(control.rs:188-189),
  而它以 `run_session_loop(...).await` 结尾(control.rs:341)——名额被整个
  会话生命周期占用。注释写"认证完成或失败即释放"(control.rs:181),
  config.rs:177 声明"不限制已认证业务并发",实现与之相反。同一 NAT 出口
  第 65 个客户端永远 `too many pending auth`。
- 改:`auth_and_run` 内注册成功点(`on_session_up` 附近、`drop(_g)` 处)
  即 `exit_unauth`;失败路径保持现状。用守卫类型(EnterUnauth RAII)保证
  各早退路径配对,防手滑漏放。
- 回归:单测——同 IP 起 64 个完成认证的会话后,第 65 个 HELLO 应通过
  (现断言失败即红);e2e 用两个客户端同源 IP(localhost)并发接入。

### A4 客户端 UDP 回写任务泄漏(fd 耗尽 + 通道任务链永挂)

- 现状:60s GC 只 `sessions.retain` 删 map 条目(udpchan.rs:159-162),
  该会话 spawn 的回写任务(udpchan.rs:216-241)持 `sock2` + `tx2`,唯一
  退出条件是 socket 报错(自持,永不会)或 tx2 失败(writer 死了才有)。
  连锁:`drop(tx)`(udpchan.rs:176)后泄漏任务仍持 `tx2` → `writer.await`
  永挂 → `run_proxy_side` 永挂 → 每次重连整套泄漏旧 ClientSession。
  源端口轮换客户端(QUIC/DNS)十几分钟耗尽 fd。
- 改:Session 结构加 `CancellationToken`;GC 逐会话 cancel 并 `sock2`
  shutdown(使 recv_from 报错);通道退出路径同样 cancel 全体会话。
  或者最少:回写任务持 `Weak<mpsc::Sender>` + 周期存活检查,socket 在
  GC 时显式 drop。
- 回归:单测——起 N 个源地址会话后触发 GC,断言任务退出(socket 计数
  或 drop hook);集成——QUIC 源端口轮换压 60s+,断言 fd 数稳定不涨。

### A5 update_service 置 enabled=false 不拆运行态

- 现状:`!port_changed` 分支直接 `update_proxy_bump`(落库 enabled=0)
  + `publish_spec_in_place`(只改 spec 下发 Update),不 teardown、不发
  ServiceRemove(api.rs:980-991);对照 disable 端点(api.rs:1282)有完整
  teardown。后果:端口继续监听;且 `port_in_use` 只统计 enabled 运行态
  (state.rs:1001-1012)→ 同端口可被新规则再次占用,双占冲突。
- 改:`!port_changed` 且 `spec.enabled == false` 时走 disable 同款路径
  (抽出 `disable_service_inner` 供两处共用,顺带解决 batch 与单个端点
  语义分叉,见 D-batch);`enabled == true` 反向同理对齐 enable。
- 回归:api 单测——update 置 false 后 `port_in_use` 必须报告占用消失、
  runtime 不在表;e2e——update 禁用后端口拒绝连接、再 enable 恢复。

### A6 组共享监听随属主规则死亡且不自愈

- 现状:共享 serve loop select 的是**属主成员**的 `rule_cancel`
  (runtime.rs:367-370 持 rt2,`tcp_serve_loop` 内 data.rs:974 select
  rule_cancel);`leave_group` 仅组空才取消/复位(state.rs:1076-1090),
  `teardown_runtime` 无条件 cancel(runtime.rs:452-466)。禁用/改端口属主
  → 监听 fd 释放、`listener_bound` 仍 true → 之后所有 spawn 看到 true
  跳过重绑(runtime.rs:346-351),组端口永久拒绝连接。
- 改:共享 serve loop 的退出条件改为组级 `serve_cancel`(不随单成员
  rule_cancel);属主 teardown 时若组非空,转移监听所有权(组结构记
  owner,重指到任一存活成员)并复位 `listener_bound` 由存活成员触发
  重绑。两案取一,推荐前者(serve_cancel 语义更简单)。
- 回归:e2e-vhost/e2e.sh 加拓扑——同 group 同 (kind,bind) 两条规则(现
  有用例 bind_port 不同实为两个单成员组,没覆盖真共享),禁用属主,
  断言另一成员端口仍可连;再启用属主恢复。
- 顺带:teardown→spawn 重建不 join 旧 serve loop,重绑同端口偶发
  BindError(runtime.rs:224 不 join;api.rs:1055 拆完立刻重建)。ProxyRuntime
  记 serve loop JoinHandle,重建前带超时 await 收敛。

### A7 XTCP 直连路径无 secret 校验 + punch"谁先来认谁"

- 现状:中继路径有 `secret_ok`(visitor.rs:44),直连没有——
  `serve_direct`(nathole_cli.rs:209-255)首帧 `START_FORWARD` 只要
  `proxy_name` 命中本地任意规则即拨内部目标,不读不比对任何凭据;
  KCP 层 `punch_listen` 接受任意来源首个合法 kcp 包并 pin(kcp.rs:1495),
  `punch_dial` 的 `learn(from)` 同样无候选校验(kcp.rs:1760-1772)。
  打洞窗口内能投递 UDP 包者可占用会话/劫持 dial 侧。
- 改:直连首帧加基于 secret_key 的质询应答——服务端在 NAT_HOLE_PREPARE
  下发随机 nonce,visitor 侧 `xtcp_direct_stream` 首帧携
  HMAC(secret_key, nonce),proxy 侧 `serve_direct` 比对(恒定时间)失败
  即断;`learn` 要求来源 ∈ 服务端下发的候选地址集合。wire 上是新增首帧
  载荷,放在既有 START_FORWARD 的字段里(serde 加 optional 字段,旧端
  缺字段=拒绝,不兼容即断,避免静默降级)。
- 回归:单测——错误 HMAC 首帧必须被拒且不拨本地目标(现有测试恰好只测
  正向);集成——打洞窗口内用错误 secret 的第三方 socket 发包,断言无法
  建流。
- 注意:这是**行为变化**(旧版直连无认证),发布说明必须写明;建议同
  minor 内完成,不兼容窗口只存在于新旧混连的直连场景。

### A8 KCP 监听预认证资源无界(单包建会话 + retired 表洪水)

- 现状:demux 收包即注册会话(2 任务 + 256 槽通道,kcp.rs:1280-1318),
  UDP 源地址可伪造,`enter_unauth` 按 remote IP 计数挡不住随机源
  IP,且只在 HELLO 后生效;`retired` 表只在 `len>1024` 时 retain 已过期
  条目(kcp.rs:1122-1135),唯一 conv 洪水下 90s 窗口内单调增长,每插
  入一次 O(n) 扫描。
- 改:demux 前置两级闸——全局在建会话数上限 + 每 IP 建会话速率限制
  (令牌桶,复用 rh-core ratelimit);retired 改固定容量环形(超限驱逐
  最旧,接受极小概率 conv 复用重退让一次)。上限值进 config,默认给
  保守值。
- 回归:单测——伪造洪流(随机 conv)下任务数/内存有界(断言会话表
  长度上限生效);retired 表长度断言不超容量。

### A9 mux `open_stream` prefix_pending 分支缺防丢唤醒

- 现状:mux.rs:2347-2349 直接 `self.inner.changed.notified().await`,
  未按同函数 2353-2368 的自家模式"先创建 notified 再复查"——快照与
  首次 poll 之间若 `notify_waiters` 恰好发生,通知永久错过;无后续成员
  表变更时 open_stream 永久挂起(收敛 detach 不再有 redial notify)。
- 改:`prefix_pending` 分支并入下方模式:先 `let notified = ...notified()`,
  复查 dead 与表内存活,再 await。几行改动,零行为风险。
- 回归:单测——并行触发 detach(无 redial)与 open_stream 的交错
  (用小 sleeps 放大窗口),断言 open_stream 在成员表空后不悬挂、按
  dead 分支返回;现有 mux 红线测试(`mux_relay_echo_and_half_close` 等)
  全绿。

### A10 发送分片帽 6MiB > 接收帧体上限 1MiB,大缓冲写撕裂整载体

- 现状:mux.rs:1574 `set_split_send_size(6MiB)`,而 yamux 接收侧
  `MAX_FRAME_BODY_LEN = 1MiB`(vendor/yamux/src/frame/io.rs:42)。
  中继路径 `up_chunk_cap` 钳在 1MiB 恰好压线,但控制面 `write_all`
  无上限——>1MiB 的帧(如超大 ApplyResult/配置回显)必被对端
  FrameTooLarge 拒收 → `begin_fatal` 拆掉整条载体(其上全部流)。
- 改:发送侧帧体帽钳到 `min(split_send_size, MAX_FRAME_BODY_LEN)`
  (从 yamux 导出常量,两处联动加 `const_assert`);`rh-protocol` 的
  `write_frame` 已有 1MiB 上限,补一处 e2e 级断言:控制帧编码超限必须
  在发送侧报错而不是上线路后被撕。
- 回归:单测——构造 2MiB 缓冲 `poll_write`,断言产出的每个帧 ≤1MiB
  且对端(本地回环 yamux)能完整收;控制面注入 >1MiB 的 ApplyResult,
  断言发送侧报错、载体不拆。

---

## 批次二:P2 生命周期/逻辑/安全

| # | 位置 | 现状(一句) | 改(方向) | 回归要点 |
|---|---|---|---|---|
| B1 | data.rs:76-105 | `TcpPool.inflight` 会话丢失后不清零,预热池永久饥饿 | `claim_pool` 换 epoch 时 `inflight.store(0)` | 断连打断在途 CREATE 后重连,池能再补满 |
| B2 | runtime.rs:53-74 | teardown 不持串行锁,与新注册竞态可把规则钉死 Offline(无自愈) | `on_session_down` 判定+写入段持 `serial_lock`;或写入前二次复核 replaced | 并发重连压测后无规则停 Offline |
| B3 | state.rs:1093 + vhost.rs:77/519/700 | 注册域名不小写归一,请求侧一律小写,混合大小写永不匹配 | `rebuild_vhost_routes`(或入库时)统一 `to_lowercase` | `Example.Foo.com` 注册后小写 Host 命中 |
| B4 | vhost.rs:490 | HTTP vhost 请求不占并发配额(proxy=None) | `handle_http_request` 取得 rt 后按请求调 `try_open_user_conn` | max_conns=N 时第 N+1 个并发请求被拒 |
| B5 | vhost.rs:41/676/484 | http_timeout_s 默认 0、tcpmux CONNECT 无超时、TLS 握手无超时 | 默认给 http_timeout_s 安全值(如 30s);CONNECT 读循环加 deadline;accept 包 timeout | 慢速头/慢 CONNECT/慢 ClientHello 有界拆除 |
| B6 | vhost.rs:549/783/820, visitor.rs:258/375 | 五处裸 `copy_bidirectional`,无半关闭有界兜底 | 套用 data.rs 的 `join_relay_pumps`/grace 收敛 | 复用 round-40 系列断言形态 |
| B7 | data.rs:1174 + config.rs:192 | UDP 会话表默认无上限,伪造源洪水 60s 窗口无界膨胀 | `max_udp_sessions_per_proxy` 默认非零(如 1024),超限计数告警 | 洪水下内存有界 |
| B8 | state.rs:979 | 跨客户端同端口 TOCTOU(串行锁是每客户端粒度) | `port_in_use` 补查 DB enabled 行;或端口判定用全局端口锁 | 并发创建同端口,恰一条成功 |
| B9 | control.rs:345-378 | 附加载体仅凭明文 session_id 聚合,窃取 id 可混入会话池 | 报到流复用挑战-应答(或至少 HMAC(session_secret, nonce)) | 错误 id/无 HMAC 的报到被拒 |
| B10 | lib.rs:631 + session.rs:398 | 多载体 secret_verified 只校验一条载体,其余 TLS NoVerify(默认公网即 K=4) | 每载体独立 sealed-cert 比对(含重拨载体);短期缓解:该档下多载体降级单载体并 warn | 每条载体握手证书不一致即断,告警可见 |
| B11 | api.rs:284 | 登出不失效 JWT,无吊销 | claims 加 `pwd_version`,与用户行版本比对;登出即 bump | 登出后旧 token 401 |
| B12 | tls.rs:32-56 | 0-RTT 整改文档的显式禁用项与回归测试全部未落地 | 按 docs/0rtt-remediation.md §3.1/3.2/3.4 落地 | 三个回归测试进 tree |
| B13 | nathole_cli / visitor_run(xtcp) | 见 A7(同一条修复线) | — | — |
| B14 | kcp.rs:82-87 | `now_ms()` 用 SystemTime 墙钟驱动 KCP RTO/重传 | 改基于 Instant 的单调毫秒(进程启动差值) | 时钟回拨注入测试不再停摆 flush |
| B15 | api.rs:1560 vs 1650 | visitor 目标解析 create(优先在线)与 update(取首个)不一致 | 抽公共 `resolve_target_proxy()` 两处共用 | 重名+一在线一离线时 update 不改归属 |
| B16 | ws.rs:98/231 | tungstenite 默认 64MiB 帧/消息上限 + rd_buf 不 shrink | 显式 WebSocketConfig 压到 ~1MiB;rd_buf 消费后 shrink | 巨帧连接被拒且内存有界 |

## 批次三:P2 性能

| # | 位置 | 现状 | 改 | 度量 |
|---|---|---|---|---|
| C1 | visitor.rs:20-43 + control.rs:797 | 每条 stcp/sudp 用户连接两次全表 DB 读 + 全量 JSON 反序列化,且内联在控制读循环 | `(client_id, revision)` 缓存解析结果;配对逻辑 spawn 出控制循环 | 高建连速率下控制循环 p99 延迟 |
| C2 | control.rs:1130-1142 | 裸模式每数据连接 O(在线客户端) 扫描,且无每 IP 限制 | `session_id → Session` 索引 DashMap;该分支套 enter_unauth | 1000 客户端在线时建连延迟 |
| C3 | vhost.rs:89/704 | 每 HTTP 请求深拷贝整张路由表 | `Arc<Vec<VhostRoute>>` 原子换发 | vhost RPS / 每请求分配数 |
| C4 | mux.rs:417-426 + pool.rs:84 | 上行块默认 256KiB 超出池容量类[16KiB,72KiB]被短路,池化在主力上行零生效 | 池加 256KiB 类目,或默认 `up_chunk_cap` 收回 64KiB 与池对齐(先 benchmark 三机对比再定) | peer.sh 吞吐 + alloc 剖面 |
| C5 | mux.rs:1226-1230 | 下行预取只按字节封顶、每块 O(n) 重算,小帧+写阻塞下 O(n²)/百万级队列 | 与上行同构:加段数上限(`SMALL_CHUNK_SEGS`)+ 增量字节计数 | 滴流+慢消费者场景 CPU/内存 |
| C6 | api.rs:127/1869/2183 | argon2 校验与配置文件 IO 在 tokio worker 上同步执行 | `spawn_blocking` 包裹;密码/用户名长度上限 | 并发登录时转发任务毛刺 |
| C7 | api.rs:110 + db.rs:630 | 登录限速键 `{ip}|{user}` 可轮换绕过、键无淘汰;每失败登录一条 events INSERT | 限速改 IP 维度(或两级)+ limiter 键容量;事件聚合 | 伪造登录洪峰下内存/DB 增速 |
| C8 | plugins.rs:119-134 | 插件拨号 async 内同步 getaddrinfo + connect 无超时 | 复用 `forward::resolve`;connect 包 10s timeout | DNS 故障注入下 worker 不饿 |
| C9 | dnsquery.rs:20-50 | DNS 无缓存无 singleflight;自定义 DNS 只查 A | TTL 缓存 + 并发去重;补 AAAA 或修注释 | 短连接压测解析次数 |
| C10 | visitor_run.rs:59-70 | visitor 用户侧连接全仓唯一无 TCP_NODELAY/keepalive | accept 后补设 | stcp 交互延迟(RTT 内小包) |

## 批次四:P3 择要(小改动,顺手清)

**服务端**:主监听 bind 失败 fail-fast(main.rs:332);优雅退出等会话收敛
+ `db.close()`(main.rs:420);events 定期裁剪 + `(level,id)` 索引;
`ChallengeTable::issue` 去锁内 retain(control.rs:65);vhost Host 解析修
IPv6 字面量与剥端口(vhost.rs:77,700);缓存分支补响应头改写、缓存键加
host;通道池等待者改 FIFO 交付(data.rs:128);配额检查前移到取通道前
(data.rs:1057);404 页缓存(vhost.rs:341);UDP `send_to` 按结果计费
(data.rs:1242);Hello 元字段截断入库(control.rs:326);SSH 网关复用
enter_unauth;指标标签预生成、quota 标量原子缓存(data.rs:1008,1070);
裸模式端口默认值/`auto_range` 校验 fail-fast(config.rs:295)。

**API/DB**:batch 与单个端点共用 inner(吞错修复);`rotate_client_secret`
查 rows_affected(db.rs:254);log_level 先验后写;500 通用文案+细节进
tracing;create_client 配额校验前置;hash 资产 `Cache-Control:
immutable` + index.html no-cache;`.synchronous(Normal)`(评估断电权衡);
列表端点 COUNT/裁剪列;visitor update 补创建侧校验;insert_proxy 非事务
变体并入事务。

**客户端**:重连退避加抖动(main.rs:135);AUTH_FAILED/CLIENT_DISABLED
类型化错误停止/降频重试(session.rs:730 字符串前缀判定);UDP target
支持域名(udpchan.rs:115,失败升 warn);`parse_addr_cached` 改全长哈希
(udpchan.rs:249);NAT_HOLE_PREPARE spawn(session.rs:838);server_addr
支持域名(config.rs:93);HTTP 插件改缓冲读头(plugins.rs:231/583);
sudp pending 改 VecDeque;PROXY protocol dst 透传真实目的
(forward.rs:60);socks5 长度校验拒截断(plugins.rs:191);探测连接
`probe_rtt` 复用(lib.rs:553)。

**传输**:QuicListener 补 Drop 关停(quic.rs:60);QUIC endpoint 按地址族
绑 [::]:0(quic.rs:133);urldecode 去 `+` 分支(proxy_dial.rs:97);
http_connect 回灌头部后早到字节(proxy_dial.rs:143);auto_cert 残缺状态
自愈(tls.rs:59);WsIo 少一次拷贝(ws.rs:231/254);compress 泵缓冲/
Decoder 复用(compress.rs:52/112);WS 状态行判等 101;yamux 读半快车道
去 Arc clone(stream.rs:559);`UpReader::try_read_chunk` Empty 归还池;
`try_read_chunk` EOF 三态;`attach_carrier_with_cap` 拒绝路径关停;带宽
`"0"` 语义(报错或=不限,bandwidth.rs:38);`udp_packet::encode` 长度
断言(message.rs:148);STUN 强制 XOR 优先(stun.rs:74)。

---

## 5 e2e 覆盖缺口(本次审查暴露,随批次补)

1. UDP 规则 × 客户端断线重连(A2 的漏网原因);
2. 同 group 同 (kind,bind) 真共享监听拓扑,含属主禁用/恢复(A6);
3. update_service 置 enabled=false 的端口行为(A5);
4. xtcp 直连错误 secret 必须被拒(A7);
5. 多载体 secret_verified 下逐载体证书一致性(B10;现 e2e 只覆盖单载体)。

## 6 验收

- 每项:定向 `cargo test -p <crate> --lib <模块>` 先红后绿;红线测试
  (`mux_relay_echo_and_half_close`、e2e-*.sh 全套)全程绿。
- 批次收尾:`cargo fmt --all && cargo clippy --workspace --all-targets
  -- -D warnings && cargo test --workspace` + 全部 e2e。
- 性能项(C 批)必须走 `benchmark/peer.sh` 三机基线,结论只认保留率;
  C4 涉及默认参数变化,先跑 A/B 再定取舍。
- 发版注意:A7(xtcp 直连加认证)是行为变化,Release 说明必须列入
  兼容性小节。

## 7 排除清单(审查确认已修好/非问题,不再立项)

clients_online 上报(b760169);`QuotaIo::peel` unsafe 拆壳;nonce 挑战
一次性+恒定时间 HMAC;SQL 全参数化;embedded_ui 穿越免疫;JWT 算法
混淆/空密钥防护;argon2id 参数与 dummy-hash;写路径锁外首读+锁内重读;
FK 级联;splice EAGAIN/EOF/半关闭/管道死锁/fd 泄漏;d0a2dab 无锁拆分与
ffd49d3 批聚合正确性;83ec2ab/1f5dc55/95ea5ba 证书下发线(单载体/QUIC
fail-closed);yamux 池不变式、writev 批回退、staging 边界、侧链背压
(PENDING_ROUTE_CAP)、80decd6 回窗时机;frame.rs 1MiB 双侧强制;
certprov HKDF 绑定;ratelimit 单调时钟与取消安全;kcp `get_conv` 前置
检查、demux 满队列自锁修复、punch 任务泄漏修复(PunchCancel)。
