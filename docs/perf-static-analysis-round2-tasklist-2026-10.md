# 性能静态分析修复任务书 · 第二轮(2026-10-27)

> 来源:第二轮并行静态审计(10 个单元:KCP/mux/compress/ws/yamux×2/服务端×2/客户端/传输层/协议核心/console),聚焦十月修复新增代码 + 首轮漏网。约 45 条新发现:11 条 medium、34 条 low,无 high。11 条 medium 的关键机械事实已由主会话逐一亲验。
> 首轮任务书见 `docs/perf-static-analysis-tasklist-2026-10.md`(47 条全部闭环,勿重做)。另确认:首轮报告 47 条发现中 coreProtocol-1/2 两条在生成任务书时漏收,本文件补入(见 L-19)。
> 状态:`[ ]` 待办 / `[x]` 完成(附 hash)/ `[W]` 放弃(写反证;"不做"按 AGENTS.md 记入 docs/not-supported.md)。

## 执行约定(与第一轮相同,逐条遵守)

1. **先核查后实施,禁止无脑照改**:每条动手前亲自 Read 当前代码,确认问题仍在、行号未漂(漂移则修正)、追调用点确认在真实路径、修法与现状匹配;核查结论一行回写本文件(`> 核查:✅/❌ …`)。核查不过就标 `[W]` + 反证,跳下一条。
2. 三道门全绿:`cargo fmt --all -- --check` / `cargo clippy --workspace --all-targets -- -D warnings` / `cargo test --workspace`;新增 `std::os::fd`/`std::os::unix` 必须 cfg 门控(含 cfg(test));console 改动跑 `npm run build`。
3. 行为不变硬约束:不改对外协议/API JSON 结构/配置语义(条目明确允许除外)。一个内聚任务一个提交,提交信息写"为什么",只 add 自己改的文件。推 main 后确认 CI 绿。
4. **不许回退第一轮的收益**:KCP TCP +5~8%、UDP 已恢复 93-95 Mbps、AGG_BATCH=64、A-5 保持回退(不重新引入 recvmmsg 批收)。收发包路径的任何改动,基准对照必须包含 **UDP RTT**(A-5 教训:只有吞吐看不出每程 ~110µs 的延迟回归)。
5. 三机基准由 AI 视可达性执行(AGENTS.md 流程);不可达就把"怎么测、看什么指标"写进条目留人。

## P0:十月修复引入的回归(最先做)

### R1 [P0] 客户端 visitor 启停 spawn 化的串行化缺口 —— 新代码正确性
- [x] 状态:完成 @ 72cfec1
> 核查:✅ 三处现场全对上(spawn/裸调 start/token 先插后 bind),交错时序自证可达。实施三件全做:entity_ops 链串行化全部启停、start 失败回收 token + bind 后复查 cancel、start_with_retry 会话级熔断;sync 侧改 start_with_retry。新增 3 条竞态回归测试;两条旧测试改 2s 轮询等静默。
- 位置:`crates/rh-client/src/session.rs:1342-1348`(ServiceSync spawn)、:1616-1623(run_sync_changes 忽略 start() 的 bool)、:1731-1733(upsert spawn start_with_retry);`crates/rh-client/src/visitor_run.rs:39-53`(token 先插后 bind)
- 问题:① ServiceSync 的 spawn T1 与紧随的 ServiceUpdate upsert(T2)可并发对同一 visitor bind;`visitor_run::start` 在 bind 前插入 cancel token,bind 失败留死 token,后续 stop/remove cancel 死 token → 旧 spec 活监听器孤儿化到会话结束。② run_sync_changes 裸调 start 不看返回值、无重试,而 ACK 已前置,bind 失败后无机制重触发(upsert 路径的 start_with_retry 正是为吃这个窗口)。
- 修法(三件全做):① run_sync_changes 的 visitor 启动改走 `start_with_retry`(或消费 bool + warn + 可重入);② `visitor_run::start` 把 token 插入移到 bind 成功之后(bind 失败不留死 token;注意与 stop 的时序——插 token 前持 visitor_cancels 锁检查是否已被 stop);③ 串行化:ClientSession 挂 `sync_apply_chain: Mutex<Option<JoinHandle>>`(或等效),upsert/remove 的启停操作排到上一轮 sync 启停之后,恢复修前全序且保留 ACK 前置。
- 验收:三道门绿;**新增竞态回归测试**(SYNC spawn 后立刻 UPDATE 同一 visitor:最终只有一个监听器、token 可 stop、spec 为新);现有 apply_sync/upsert 测试全过。

### R2 [P0] yamux 接收 staging 自管扩容在零拷贝主路径每帧重建 —— C-1 修复的主张不成立
- [x] 状态:完成 @ 3582377
> 核查:✅ 主张成立(split_to 携走容量 → 64KiB 帧稳态每帧重建 + 整段预零填)。深层核查:yamux 无 tokio 依赖、底层为 futures AsyncRead(签名硬性要求已初始化切片),ReadBuf::uninit 路线不可行,水位机制有存在必要 —— 按任务书原方案懒填:reserve 后水位停在 len,新空闲区由 zeroed_spare 按需填;自管扩容保留(C-1 reclaim 可见性理由仍立),不变量放宽为 len ≤ zeroed ≤ capacity。每字节一次初始化是 futures API 地板,消除的是「预留未读」整段。测试:水位测试改判放宽不变量;新增实例级零填计数缝(全局静态会被并行测试互踩)+ 断言「预留 1MiB 只读 100B → 零填恰 100」「16×64KiB 帧零填 ≤ 读入总量」。基准对照待收尾统一跑(mux/tcp 档)。
- 位置:`vendor/yamux/src/frame/io.rs:249-262`(reserve)、:295-311(take_body/split_to)
- 问题:>4KiB 帧体走 split_to 零拷贝交付,bytes 1.12.1 的 split_to 把容量移交帧体 → staging 容量随每帧缩回 → 下一帧必触发自管重建(新分配 + extend 拷残留 + **整段零填**)。64KiB 转发主力帧每帧 malloc+memcpy+memset,与 e2c0b41 注释"零填 ≈2× 峰值容量有界"矛盾(该主张仅 ≤4KiB 小帧成立)。
- 修法:**零填改懒填**——自管扩容下 bytes 内部搬移已不可见,reserve 里 `extend_from_slice` 后 `zeroed = len` 即可(新空闲区按需由 zeroed_spare 填),消掉整段 eager memset;顺带修正注释,把"有界"主张收窄为容量保留型流量。可选加:扩容从 crate::pool 取 backing。**不要**改回 bytes reserve(会复活 C-1 原始 bug)。
- 验收:三道门绿;新增单测:64KiB 帧连续流,断言零填总量不随帧数线性增长(可用计数器缝或行为断言);yamux 全量测试过。
- 备注:基准 mux 档(tcp+MUX=1)看 CPU 与保留率,重点 server 端。

### R3 [P0] 三条一行级正确性收尾(合并一个提交)
- [x] 状态:完成 @ c3a831c
> 核查:✅ 三条现场全对上。a) agg 冲刷错误路径 note_fatal 无差别 fetch_sub 未计入的在途(usize 回绕)—— 加 counted_inflight 参数,drain(pop 已计)传 true、聚合传 false,契约单测; b) advance_confirmed 无条件 store,陈旧定点 ACK 回退确认号 → fetch_max 单调推进,陈旧 ACK 回归测试; c) poll_flush Ready 不清 send_queued,「写 Pending 取消+外部 flush+新写」序列静默丢载荷 —— flush Ready 清位,红→绿测试(noop-waker 取消序列,修前确证失败)。
- a) `crates/rh-transport/src/kcp.rs:941-944`:SendAgg 错误路径调 note_fatal 对未计入的在途计数 fetch_sub → usize 回绕,stats 恒巨值。修:note_fatal 加 `inflight: bool` 参数(agg 路径 false)。
- b) `crates/rh-server/src/runtime.rs:216`:advance_confirmed 无条件 store,重复/陈旧定点 ACK 回退确认号 → bind_error_retry 多余重发。修:`fetch_max(revision, Relaxed)`。
- c) `crates/rh-transport/src/ws.rs:354-360`:poll_flush 完成(Ready)时不清 send_queued,外部 flush 完成后新载荷会被静默丢(潜伏,仓内暂无活体路径)。修:flush Ready 时 `send_queued = false`,补"Pending 写被取消+外部 flush+新写"红→绿测试。
- 验收:三道门绿;三条各带测试。

## P1:中影响(7 条)

### M1 [P1] 服务端 vhost/visitor 中继 8KiB 默认缓冲 + split 共享锁 —— 首轮漏网的全量吞吐路径
- [ ] 状态:
- 位置:`crates/rh-server/src/data.rs:4101-4117`(copy_bidirectional_bounded);调用点 vhost.rs:944(HTTPS passthrough)/1229/1266(tcpmux)、visitor.rs:296/417(stcp/sudp)
- 问题:tokio::io::split + io::copy 默认 8KiB + 共享锁双泵,是同文件 R3 口径(32-64KiB)的 1/4-1/8;E-1 在客户端的孪生,首轮单元划分漏掉服务端。
- 修法:对齐口径——缓冲 64KiB(`copy_bidirectional_with_sizes` 或手写);user 侧复用 split_counted_tcp_user、channel 侧 split_boxed_io 的无锁拆半(签名从 &mut 改所有权,调用点均持所有权可行);TCP 主路径已有 copy_relay_pair/splice,本函数只服务 passthrough/visitor,别动主路径。
- 验收:三道门绿;vhost/tcpmux/visitor 转发测试过。
- 备注:基准 tcp 档(HTTPS passthrough 与 tcpmux 场景)看吞吐保留率。

### M2 [P1] QUIC 服务端握手无在途上限 —— TCP 已修的同类缺口
- [ ] 状态:
- 位置:`crates/rh-transport/src/quic.rs:147-179`;对照 tcp.rs:28 HANDSHAKE_MAX_INFLIGHT=512
- 问题:每个 Incoming 无条件 spawn + 15s 超时,零背压;建连洪水下任务/quinn 状态无界堆积。
- 修法:镜像 tcp.rs 方案——信号量(512)acquire 与 endpoint.accept() 绑同一 select,拿到许可才 spawn;或 JoinSet+上限+abort_all。
- 验收:三道门绿;单测:并发大量假 Incoming 时在途数封顶。

### M3 [P1] on_session_down 全量取回 visitor 行只用 id —— D-5 孪生漏点
- [ ] 状态:
- 位置:`crates/rh-server/src/runtime.rs:76-81`
- 问题:list_visitors 逐行构造完整 VisitorSpec 只为拿 id,且在 serial_lock 窗口内、先于 replaced 早退。
- 修法:换 `list_visitor_ids`(db.rs 已有,一行);或至少移到 replaced 判定之后。
- 验收:三道门绿;断连清理测试过。

### M4 [P1] kcp 入站包池是进程级单 Mutex —— A-4 引入的跨核争用
- [ ] 状态:
- 位置:`crates/rh-transport/src/kcp.rs:1272-1287`(PKT_POOL)
- 问题:demux 每包 get、N 会话任务每包 put,同一锁字跨核乒乓(~17 万临界区/秒 @85k pps),替代的 malloc 本有 per-thread arena;注释自评"争用弱"低估了跨任务形态。
- 修法:分片池(8-16 片按会话 id 哈希取片)或 demux 本地 free-list + 溢出触全局;put 按来源分片。
- 验收:三道门绿;KCP 回环测试过;槽归还全分支不回归。
- 备注:基准 kcp 档吞吐+CPU+**UDP RTT**(A-5 教训)。

### M5 [P1] UDP 会话表满时锁内逐包 warn —— 攻击场景日志放大
- [ ] 状态:
- 位置:`crates/rh-server/src/data.rs:1624-1633`(udp_session_admit 拒绝分支,持 sessions 锁)
- 问题:表满后伪造源洪泛每包在临界区内格式化 warn + tracing 全局 writer 锁,up/down 两泵被串行化,日志无上界。
- 修法:限频(借用 down 泵 15s gc tick 汇总"本周期拒绝 N 包"一条);`udp_sessions_rejected` 计数保留每包。
- 验收:三道门绿;表满压测下日志行数有界。

### M6 [P1] SendAgg 聚合缓冲每会话驻留最高 ~300KB 不收缩
- [ ] 状态:
- 位置:`crates/rh-transport/src/kcp.rs:870-954`(AggQueue,drain 只减 len)
- 修法:flush_now 排空后 `capacity > 2×AGG_BATCH×1300` 则 shrink/换新 Vec;偶发再增长由几何扩容摊销。
- 验收:三道门绿;新增测试:满窗后排空,容量回落。

### M7/M8 [P1] console 两条(合并一包做)
- [ ] 状态:
- a) `console/src/components/proxy-list-page.tsx:107/606-613`:批量导入弹窗每键入一字符整页表格全量重渲染。修:Dialog(含 batchText/batchResults/batchRunning)抽独立子组件,状态内聚。
- b) `/services` 列表响应携带每条完整 spec(前端 rabbithole.ts:47/124,四个消费方只有编辑弹窗要 spec)。修:后端加轻量投影(列表去 spec,编辑按 id 单条取)+ 前端 api 与消费方改造;**响应结构变化需同步改 console 并验证**(管理 API 非隧道协议,允许变,但要在提交信息写明)。
- 验收:a) `npm run build` 过 + 弹窗打字不再触发表格重渲染(React DevTools 或日志验证);b) 列表/编辑/批量导入/⌘K 全功能冒烟。

## P2:轻微(按包合并提交,核查后逐条处置)

### L-A 服务端孪生债(5 条,D/E 包修复的未跟修面)
- [ ] a) plugins.rs:1045-1076/276/877 客户端 base64+format!(D-9 孪生):换 base64 crate Indifferent 引擎;期望凭据建会话时算一次。
- [ ] b) main.rs:429-454 static_dir 分支无缓存头(D-15 孪生):assets/ immutable 头 + index no-cache;SPA 回退 index.html 字节缓存。
- [ ] c) control.rs:1128-1144 route_data_stream 每通道读 limits RwLock(D-12 孪生):data_conn_limit 原子快照,热改点同步 store。
- [ ] d) runtime.rs:38/100/282 snapshot_state 取 reason 即弃(N12 孪生):换 state_kind().await。
- [ ] e) session.rs:1844/visitor_run.rs:99/225 VisitorSpec 未 Arc 化(E-6 孪生):visitors 表改 Arc<VisitorSpec>,:1844 锁内深克隆消除,deliver_relay 的 _vspec 未用参数删除,run_tcp 持 Arc。

### L-B 服务端序列化/状态(4 条,D-1 遗留与状态层)
- [ ] a) api.rs:1984-1997 list_visitors 仍 json! 建树内嵌 spec:仿 ClientListRow 强类型化。
- [ ] b) api.rs:2336-2359 get_server_config 每请求整份 ServerConfig 深克隆 + 4 段 to_value:具名 Serialize 结构。
- [ ] c) api.rs:2843-2852 list_certs_api 逐行建树 + 每行 from_str 解析 domains:强类型化。
- [ ] d) state.rs:1215-1228 HttpCache::get 双查找 + String 克隆:get_key_value 单查 + content_type Arc 化;另 state.rs:290-298 bump_cfg_gen 全表扫被 batch 放大 N 倍:batch 去重后统一 bump。

### L-C yamux/pool 周边(6 条)
- [ ] a) pool.rs:122-143 大类 get 弹出无容量过滤(潜伏合同违约,split_send_size 中间尺寸可播种):比照小类加过滤。
- [ ] b) pool.rs:197-206 大类 put CAS 败者直接丢缓冲:改 fetch_add 后判预算。
- [ ] c) pool.rs:119/141/147/185 allocations 诊断计数未门控(违反自家红线,C-4/A-1 同族):收进 cfg(test) 或 perf-counters feature。
- [ ] d) connection.rs:1953-1961 RecvHalf 每帧对 N 条 new_streams 通道 poll(每分片一锁):排空提升到 per-poll。
- [ ] e) connection.rs:2546-2549 FlushTask 稀疏分片每帧 N 次空锁:ChannelShared 加非空原子标志先查后锁。
- [ ] f) shard.rs:960 drain_ctrl 不受预算约束(C-2 不对称):ctrl 消费计入 poll 级预算;另 stream.rs:629/686 on_park 双写时间戳删一处。

### L-D kcp 收尾(4 条)
- [ ] a) kcp.rs:952-954 flush_now 每批 Vec 头部 drain(整轮 ~1.5× memmove):改读游标 off,与 A-2 同型。
- [ ] b) kcp.rs:965-977 EAGAIN 余段 to_vec 二次拷贝:agg buf 改 Bytes,freeze 后 slice 移交 backlog。
- [ ] c) kcp.rs:180-182/701/746 backlog 每入队线性扫字节预算:queued_bytes 增量记账 O(1)。
- [ ] d) kcp.rs:1467-1491 交付循环每消息两次 guarded 锁:批内合并(一次读 room,本地 scratch 收,一次 extend+wake)。

### L-E 传输层(3 条)
- [ ] a) tcp.rs:721-727/proxy_dial.rs:120-128 代理拨号每连接重 parse URL + 域名无 DNS 缓存:装配时 parse 一次存 ProxyUrl;代理 (host,port) 解析结果缓存。
- [ ] b) tcp.rs:179-183 verify 在 peek 判定前就 dup 写侧 fd:明文分支先 peek 判 yamux 再 dup。
- [ ] c) quic.rs:375-391 connect 证书链最多 3 次深拷:提取一次复用。

### L-F 存量正确性(3 条,低频触发)
- [ ] a) proxy_dial.rs:143-168 http_connect 丢弃 CONNECT 终止符后残余字节(server-first 协议丢首段):记录命中下标,残余经 PrependedIo 前传。
- [ ] b) rh-core/proxyproto.rs:33-51 混族地址(v6 源+硬编码 v4 dst)静默 UNSPEC 丢源地址:dst 按源同族合成或透传真实地址;混族 warn。
- [ ] c) api.rs:124-149 login_limiter_user 键含用户名单调增长:去用户名维度或定期收缩。

### L-G 杂项(4 条)
- [ ] a) api.rs:2779-2812 put_server_config 持 hot 读锁跨 spawn_blocking await:块内提取完快照 drop(hot) 再阻塞写。
- [ ] b) vhost.rs:318-327/493/520-531 每请求 3-4 次 host/IP 格式化分配:连接级预计算(x-real-ip 头值、peer 串、START_FORWARD user_src)。
- [ ] c) main.rs:552-584 D-13 完整收尾:握手移出 accept 循环进每连接任务(lazy-TlsStream),消除 10s 队头阻塞残余。
- [ ] d) visitor.rs:29-85 visitor_cache 未命中路径两次按客户端全量查询内联控制读循环:命中路径 C1 已优化,未命中侧至少把 list_proxies 投影化。

### L-H 核心库(4 条)
- [ ] a) certprov.rs:64-97 双拷贝 + 每连接重复 HKDF:SealedCert 按值收、truncate 复用缓冲、derive_key 按 secret 缓存。
- [ ] b) bandwidth.rs:26 每连接重解析(eq_ignore_ascii_case 零分配)+ 调用方物化一次;**先确认规则桶 per-connection 语义是否本意**(TCP 每连接各一份桶 vs UDP 按规则共享,不一致)——非本意则改为规则级共享桶,是本意则记 not-supported.md。
- [ ] c) **coreProtocol-1(首轮漏收)**:rh-protocol/frame.rs:151-164 write_frame 每帧双分配双拷;最高频调用点 vhost 每请求 START_FORWARD。修:write_all_vectored([头;payload]) 或复用缓冲预编码变体;**不要**改两次 write_all(裸 TCP 拆 2 syscall)。
- [ ] d) **coreProtocol-2(首轮漏收)**:rh-core/proxyproto.rs:17-63 encode_v1 format! / encode_v2 中间 Vec:栈缓冲直写;顺手与 L-F b) 同点处置。
- 验收:c/d 完成后在本文件标注"首轮漏收已补"。

### L-I console 收尾(2 条)
- [ ] a) pages/overview/index.tsx:120-123 「转发规则健康」卡不随 5s 轮询刷新(池/连接数冻结):挂 refetchInterval 或注释写明取舍。
- [ ] b) components/data-table.tsx:213-215 挂载即 setSelected([]) 新数组多一轮全表渲染:空选集跳过回调。

### L-J 可选/观察项(默认不做,写明即可)
- ws.rs JoinDuplex 缺 poll_write_vectored(当前无向量写消费者,零影响):记 not-supported.md 或上报 is_write_vectored=false。
- mux.rs pump_down 三处 enabled() 未提升(≈0 开销):下次触碰该文件顺手。
- kcp PunchOutput/客户端 punch 收包循环错误分支槽直接还给分配器(仅会话终结,无泄漏):不动。
- plugins.rs static_file into_owned 整头拷贝 + 4 次 tokio::fs:n17 借用化 + fs 操作合并(open→metadata 两次)。
- plugins.rs:393-399 read_proxy_request 双 to_vec:split_off 单分配。
- sudp 建道前 pending 逐包 to_vec:低频窗口,不动。

## 完成标准

P0 三条 + P1 七条全部处置(实施或 [W]);P2 按包推进,每包一个提交;收尾自查 cfg 门控 grep + console build;涉及 kcp/yamux 收发包路径的(R2/M4/M6/L-D)必须附基准对照结论(或不可达时的测法清单)。
