# 性能静态分析修复任务书(2026-10)

> 来源:workflow「性能静态分析」——15 个单元并行静态审查(crates 五个 crate + vendor/yamux + console,约 8.6 万行),每条发现由独立复核员仅凭代码证据核实。55 条发现合并去重后 **47 条:46 条已确认,1 条存疑**;无 high,11 条 medium,35 条 low。
> 本文件是执行依据;每条的完整证据与复核记录见 workflow 产物「性能静态分析报告」。行号为审查时点快照,**动手前先读当前代码核实**。
> 状态:`[ ]` 待办 / `[x]` 完成(附提交 hash)/ `[W]` 放弃(写明反证;"判定不做"的按 AGENTS.md 记入 `docs/not-supported.md`)。

## 执行约定(每个任务包都要遵守)

1. 提交前三道门全绿:`cargo fmt --all -- --check`、`cargo clippy --workspace --all-targets -- -D warnings`、`cargo test --workspace`(AGENTS.md)。
2. 任何新增 `std::os::fd` / `std::os::unix` 用法必须放 `#[cfg(unix)]` 内(含 `#[cfg(test)]`),收尾自查:
   `grep -rn "std::os::fd\|std::os::unix" --include=*.rs crates/ vendor/`。
3. 任务书是静态分析候选:先核实问题仍在、机理成立再动手;与现状不符就标 `[W]` 并记录反证,不要硬改。
4. 一个任务(或一个内聚任务包)一个提交;提交信息写"为什么";不与格式化改动混提交;只 `git add` 自己改过的文件。
5. 数据面任务的**绝对收益**以 `benchmark/peer.sh` 实测为准(保留率口径,`PEER_REPEAT>=5` + 每档 15s);三机基准由人组织,AI 只保证改动正确、行为不变,并在条目备注里写清"怎么测、看什么指标"。
6. 行为不变是硬约束:除任务书明确允许外,不得改变对外协议、API 响应 JSON 结构、配置语义。
7. `vendor/yamux` 是仓库内 vendor 代码,直接修改;遵守其文件内注释声明的既有设计约定(如诊断零开销红线、stream.rs:558 的 poll_ready 记账纪律)。

## 建议执行顺序

P1 共 11 条:先做独立的单点修(clientMisc-1、serverData-1、mux-1、kcp-1),再做改动面大的(serverApi-1、serverMisc-1、yamuxRest-1、yamuxShard-1),KCP 其余两条(kcp-2、kcp-3)与包 A 的 P2 条目顺手合并做。P2 按包推进,同包内相邻位置的任务合并为一个提交。P3 不直接修,只记录量化建议。

---

## 包 E:客户端(rh-client)

### E-1 [P1] 静态文件/tls2raw 搬运缓冲 8KiB → 64KiB —— 来源 clientMisc-1
- [x] 状态:完成 @ 8ee8672
> 核查:✅ 问题在,行号无漂移(plugins.rs:124 / :965);tokio 锁定 1.53.2,`copy_bidirectional_with_sizes`(1.35+)可用;修法可行。
- 位置:`crates/rh-client/src/plugins.rs:965`(serve_static_file 成功分支)、`plugins.rs:124`(tls2raw 的 copy_bidirectional)
- 问题:落在 tokio `io::copy` / `copy_bidirectional` 默认 8KiB 缓冲,是同文件其余搬运路径刻意用的 64KiB 口径(plugins.rs:1270 注释已论证)的 1/8,大响应/大流量 syscall 与 await 轮次放大 8 倍。
- 修法:两处换成与 cp/copy_limited 同款的 64KiB 搬运循环;双向场景可直接用 `copy_bidirectional_with_sizes`(tokio 1.53 已有)。注意 static_file 写侧是内存 DuplexStream,收益主要在读侧与 await 轮次。
- 验收:三道门绿;行为不变;现有插件测试通过。

### E-2 [P2] DNS 解析入口每次命中也做键分配 + 配置克隆 —— 来源 clientMisc-2
- [ ] 状态:
- 位置:`crates/rh-client/src/dnsquery.rs:160-162`(cache_key)、`:14-16`(dns_server 每次 clone)
- 问题:缓存命中路径仍付 1-3 次 String 分配 + 2 把进程级 std Mutex;每连接拨号/每插件请求一次。量级噪声级,顺手修。
- 修法:dns_server 快照改 OnceLock/Arc 读取;cache_key 改 Arc<str> 组合使命中路径零分配。
- 验收:三道门绿。

### E-3 [P2] xtcp 直连每流按 name 线性扫规则表 —— 来源 clientMisc-3
- [ ] 状态:
- 位置:`crates/rh-client/src/nathole_cli.rs:310-315`(find_by_name),调用点 :270-273
- 问题:serve_direct 每接受一条流在 proxies 锁内 O(N) String 比较;表以 id 为键本可 O(1)。
- 修法:随 apply_sync/upsert 维护 name → Arc<ProxySpec> 辅助索引。量级小(临界区内无 await),顺手修。
- 验收:三道门绿;upsert/apply_sync 后索引一致(现有 session 测试覆盖)。

### E-4 [P2] upsert visitor 分支持锁跨 await —— 来源 clientSession-1
- [ ] 状态:
- 位置:`crates/rh-client/src/session.rs:1593-1598`
- 问题:持有 `sess.visitors` 写锁期间 await stop_visitor(内部再取 4 把锁),违反同文件 proxy 分支与 apply_sync 已确立的"锁内只收集、await 全在锁外"纪律(session.rs:1550 注释)。有 `control_path_stays_responsive_during_upsert` 测试佐证该路径关注此问题。
- 修法:比照同函数 proxy 分支——锁内只做 changed 判定与 insert,stop_visitor 移到 drop(cur) 之后。
- 验收:三道门绿;上述测试保持通过。

### E-5 [P2] apply_sync 全量 stop/start 内联在控制读循环 —— 来源 clientSession-2
- [ ] 状态:
- 位置:`crates/rh-client/src/session.rs:1319-1321`(ServiceSync 分支)、:1507-1512、:1538-1543
- 问题:重连全量 ServiceSync 期间,逐实体的 stop/start(含每个 visitor 一次 listener bind)串行占住 run_loop 读分支,写/心跳/cancel 分支整体停摆,时长正比规则数(千级可达百毫秒)。
- 修法:实体表更新(锁内收集)完成后即发 ACK,把逐实体 stop/start spawn 出去执行——与 upsert visitor 重试的处理(session.rs:1607-1631)一致。
- 验收:三道门绿;apply_sync_lock_tests(n=3000)通过;ACK 先于实体启动的行为变化需在提交信息说明(服务端 confirmed_revision 提前,语义安全:客户端仍会逐条执行)。

### E-6 [P2] ProxySpec 每条变更深克隆 3 次 —— 来源 clientSession-3
- [ ] 状态:
- 位置:`crates/rh-client/src/session.rs:1580-1582`(upsert)、:1499-1502(apply_sync 新增)、:1700(start_health)
- 问题:一份深克隆 ≈15+ 次分配(N5 注释自证),表已存 Arc 却在链路上再深克隆 2 次。
- 修法:构建一次 `Arc<ProxySpec>`(含 normalize),表存 Arc、start_health 改收 `Arc<ProxySpec>`(health.rs:11 签名同步改),深克隆降为 1 次。
- 验收:三道门绿;health 测试通过。

---

## 包 A:KCP 载体(rh-transport/kcp.rs)

### A-1 [P1] 进程级 COUNTERS 每包 5-7 次原子 RMW —— 来源 kcp-1
- [ ] 状态:
- 位置:`crates/rh-transport/src/kcp.rs:962/971/974/1049/1097/830/766/1511/1747/1834/2057`
- 问题:生产每包路径对 static COUNTERS(7 个 AtomicU64 同 struct)做 fetch_add,跨核多会话时缓存行乒乓;生产二进制无任何读者(唯一读者是 examples/kcp_loopback_bench.rs)。B3 注释声称成本已移走,实际经 COUNTERS 原样保留。
- 修法:收进 feature 开关(如 `perf-counters`,bench example 启用)或 cfg(test);或改为每会话本地累积、按需聚合。
- 验收:三道门绿 + `cargo build --examples` 通过(bench example 仍能编译运行);生产路径零计数。

### A-2 [P1] write_queue 搬运用 split_off 反复切尾 —— 来源 kcp-2
- [ ] 状态:
- 位置:`crates/rh-transport/src/kcp.rs:1017-1019`
- 问题:每片为"尾部剩余"重新分配 + 整段 memcpy;预算受限(慢 ACK/丢包链路)时对同一 64KiB 项按每轮 1 段消费,尾拷贝总量 ≈1.8MB(放大 ~28 倍),恰发生在拥塞恢复最不想烧 CPU 的时刻。
- 修法:前端项加消费游标(Vec, offset):piece 以 `&front[off..off+len]` 视角记账,锁外只对 piece 本身 to_vec(≤64KiB,通常 ≤mss),消费到尾部才弹出项。
- 验收:三道门绿;现有 KCP 测试通过;补一个"大项 + 受限预算"单测断言数据完整。

### A-3 [P1] 每输出数据报一次 sendto,满窗单轮 ~256 次 syscall —— 来源 kcp-3
- [ ] 状态:
- 位置:`crates/rh-transport/src/kcp.rs:862-877`(UdpOutput::write)
- 问题:kcp 0.6.0 每满一个 MTU 就 write_all 一次 → 每 datagram 一次 syscall;未用 sendmmsg/UDP GSO。TX 路径最大固定开销项。
- 修法:UdpOutput 在单轮 kcp.flush 期间聚合 datagram,会话层 flush 后显式冲刷并 `sendmmsg` 批量提交(Linux 可再加 UDP_SEGMENT/GSO)。注意 kcp 不调 output.flush(已 grep 证实),冲刷点需自建(如 Arc 共享的 flush 句柄)。**sendmmsg 必须 cfg(unix),非 Unix 回落现有逐包路径。**
- 验收:三道门绿;Windows 侧仅靠 cfg 隔离(本地无法交叉验证,靠 CI windows job);KCP 回环基准 example 不回退。
- 备注:收益用 benchmark/peer.sh 的 kcp 档实测(PROTO=kcp),看保留率与 CPU(cpu.jsonl)。

### A-4 [P1] 入站每报文 pkt.to_vec() 一次堆分配 —— 来源 kcp-5(合并 transports-2)
- [ ] 状态:
- 位置:`crates/rh-transport/src/kcp.rs:1598`(demux,服务端全部会话唯一入站通道)、同型 :1749/:1839/:1853/:2070/:620/:331
- 项目自述 1 Gbps ≈ 85k pps,即每秒 ~8.5 万次 ~1200B 小分配;1598 参数先求值,通道 Full 时分配照做再丢弃。
- 修法:SessionCmd::Packet 改携池化载体(参照 mux.rs:230 既有 BytesMut 池风格,或用已有 bytes 依赖的引用计数),recv_from 直接读进池缓冲移交,消费端 kcp.input 后归还;发送积压 push(620)同理。
- 验收:三道门绿;KCP 回环测试数据完整。
- 备注:同 A-3 用 peer.sh kcp 档验证。

### A-5 [P2] demux 单任务逐包 recv_from,无 recvmmsg —— 来源 kcp-4
- [ ] 状态:
- 位置:`crates/rh-transport/src/kcp.rs:1506-1532`(服务端)、:1741-1759(客户端)
- 问题:高聚合 pps 下 syscall 频率与单任务调度是收包扩展上限;4MiB SO_RCVBUF 只是兜底。
- 修法:Linux 下 `recvmmsg` 一次取一批(≤64)再按 conv 分发;或按 conv 哈希分片到多收包任务(需 SO_REUSEPORT 多 socket)。cfg(unix)/cfg(target_os) 门控。
- 验收:三道门绿;与 A-3/A-4 合并为一次基准对照。

### A-6 [P2] 每输出数据报先锁 fatal_error 再锁 state —— 来源 kcp-6
- [ ] 状态:
- 位置:`crates/rh-transport/src/kcp.rs:826`(检查)、:579-581(实现)
- 问题:fatal_error 是一次性置位永不清除的状态,却用 Mutex 每包读;is_idle 的 state 锁有 FIFO 正确性依据应保留。
- 修法:fatal_error 改 AtomicBool 快路径;错误串仅在首个致命错误时写入一次供诊断/桥接读取。
- 验收:三道门绿;致命错误传播测试(kcp.rs 内 drain_once fatal 分支)通过。

### A-7 [P2] 每次 write 克隆 Arc 再立即 drop —— 来源 kcp-7
- [ ] 状态:
- 位置:`crates/rh-transport/src/kcp.rs:863`(UdpOutput)、:1958-1959(PunchOutput)
- 问题:每数据报 2-4 次纯多余原子 RMW;direct 闭包是 FnOnce 局部闭包,借用即可。
- 修法:闭包改借用形式(`|b| self.sock.try_send(b)`),或把 output_write_via_backlog 的 direct 参数改为显式引用。
- 验收:三道门绿。

### A-8 [P2] poll_write 持锁 notify_one —— 来源 kcp-8
- [ ] 状态:
- 位置:`crates/rh-transport/src/kcp.rs:336`、`:381`
- 问题:guard 存活期间 notify_one,被唤醒的会话任务立刻在同一把锁上空等;poll_flush(:408→412)已按"先 drop 再 notify"实现,写路径未对齐。
- 修法:两处各改成 drop(s) 后再 notify_one。一致性清理,零风险。
- 验收:三道门绿。

---

## 包 B:mux / splice / ws(rh-transport)

### B-1 [P1] UpReader Empty 路径丢弃 256KiB 池缓冲 —— 来源 mux-1
- [ ] 状态:
- 位置:`crates/rh-transport/src/mux.rs:342-367`
- 问题:try_read_chunk 在数据未就绪(Empty)时,已取出的 256KiB 池缓冲未消费即 drop(不归池);仓库自证典型中继"几乎总是返回 Empty"(mux.rs:750-753),即上行每批一次 256KiB 分配/释放或池条目永久流失。
- 修法:UpReader 加 `spare: Option<BytesMut>` 字段,Empty 时暂存供下次复用(容量不符再丢弃);初始相 12KiB 本地缓冲同理。
- 验收:三道门绿;mux 现有测试通过。
- 备注:peer.sh mux 档看 CPU 与吞吐保留率。

### B-2 [P2] tokio::io::split 共享锁残留回落两处 —— 来源 mux-2(合并 transports-1)
- [ ] 状态:
- 位置:`crates/rh-transport/src/mux.rs:609-612`(mux_relay Err 回落)、`crates/rh-transport/src/compress.rs:44-65`
- 问题:B4/split_boxed_io 修完主路径后,仍有两处共享 std Mutex:插件目标回落的 mux_relay_split 双泵,与 compress 泵侧"一条 duplex + split"。复核确认影响弱(锁内是本机 IO/memcpy),低优先。
- 修法:mux_relay 对包装类型增加剥壳/downcast 拆半分支走 mux_relay_split;compress 改两条独立单向 duplex(四半端天然无锁)。
- 验收:三道门绿;压缩开关开/关的转发测试通过。

### B-3 [P2] pump_up 诊断关闭时仍调 Instant::now —— 来源 mux-3
- [ ] 状态:
- 位置:`crates/rh-transport/src/mux.rs:783`、`:854`
- 问题:违反本文件自声明的"诊断关闭时零开销"红线(mux.rs:1018-1020);pump_down 同位置已正确门控(:1145/:1241)。
- 修法:两处 Instant::now 移入 `if diag` 分支(存 Option<Instant>),与 pump_down 的 __t0/__w0 模式同型。
- 验收:三道门绿;disabled_produces_no_timestamp 测试(mux.rs:3330)保持通过。

### B-4 [P2] 每次 splice relay 重读两个环境变量 —— 来源 splice-1
- [ ] 状态:
- 位置:`crates/rh-transport/src/splice.rs:1155-1156`(调用)、:500/:227(实现)
- 问题:每次建 relay 各取一次进程环境锁 + String 分配;同文件 notsent_lowat() 已用 OnceLock(:41-49)做同类缓存,这两个开关漏了。
- 修法:两个开关各用 OnceLock(或合并为一个 OnceLock 结构)缓存。复核确认:全仓无测试 set_var("RH_SPLICE_*"),缓存不破坏测试策略。
- 验收:三道门绿;splice 测试(显式注入走 inner/with_stats)通过。

### B-5 [P2] 限速路径每块两次装箱分配 —— 来源 splice-2
- [ ] 状态:
- 位置:`crates/rh-transport/src/splice.rs:1028-1037`、trait 签名 `crates/rh-transport/src/lib.rs:259`
- 问题:限速激活时每 256KiB 块一次 BoxFuture + 一次 Box<ChunkReservation> + Arc clone(~10k allocs/s @10G);默认装配不装 limiter 时零开销。
- 修法:reserve 返回具体 future 类型(impl Trait/枚举)消掉两层 Box,或按多块聚合粗粒度预留;改 trait 需同步全部实现方(data.rs BucketLimiter、forward.rs ClientBucketLimiter)。改动面大,收益仅在限速场景,**可选**。
- 验收:三道门绿;限速开启/关闭的转发测试通过。

### B-6 [P2] WsIo 写路径每帧/每批整块分配 —— 来源 transports-3
- [ ] 状态:
- 位置:`crates/rh-transport/src/ws.rs:283`(标量写 to_vec)、:316-320(向量写 with_capacity)
- 问题:ws/wss 载体每 ≤64KiB 块/每 ≤1MiB 批一次堆分配+拷贝,无复用。ws/wss 非默认载体,且 tungstenite 发送侧本有一次整载荷 memcpy(收益有天花板),低优先。
- 修法:WsIo 内持有跨写复用的 scratch BytesMut,两条写路径统一从 scratch 构造 Message::Binary(payload.freeze())。
- 验收:三道门绿;ws/wss 连接测试通过。

---

## 包 C:yamux(vendor/yamux)

### C-1 [P1] 接收 staging 的 zeroed 水位被 reclaim 误重置 —— 来源 yamuxRest-1(合并 yamuxCore-1)
- [ ] 状态:
- 位置:`vendor/yamux/src/frame/io.rs:232-239`(重置条件)、:264-288(消费)、:593(零填)
- 问题:水位在任何 capacity 变化时重置为 buf.len(),而 bytes 1.12.1 的 reclaim(无新分配、指针复位)也改变 capacity → 稳态每收 1 字节约做 1 字节 memset,写脏的 cacheline 随即被内核拷贝覆盖。
- 修法:重置条件收窄为"backing 真实搬迁":用分配基址跟踪初始化区间,或 (alloc 代际, zeroed_cap) / 累计消费字节计数。**正确性红线:不要用朴素 as_ptr 比对(reclaim 会把指针移回起点)或"最大容量回落即视为 reclaim"判据(fresh-alloc 可小于历史最大且确实搬移)——误判会把未初始化内存当已零填。** reclaim 仅在上一帧体已 drop 时触发,同 wake 的 fresh-alloc 轮 memset 属契约必需,收益有上界。
- 验收:三道门绿;新增单测覆盖"reclaim 轮不重零填 + 数据正确";yamux 全量测试通过。
- 备注:peer.sh mux 档看 CPU。

### C-2 [P1] ShardWorker poll 循环无消费预算 —— 来源 yamuxShard-1
- [ ] 状态:
- 位置:`vendor/yamux/src/connection/halves/shard.rs:939-957`
- 问题:持续满载下单次 poll 可无限延续、不让出 tokio worker(SendWorker 有预算 shard.rs:112 `SEND_CMDS_PER_POLL=64` 且注释明言"防单 Worker 独占线程",接收侧恰好缺失)。影响公平性/尾延迟,吞吐不受损。
- 修法:仿 SendWorker 加 poll 级预算(如每 poll ≤256 帧),耗尽后 `cx.waker().wake_by_ref()` + Pending 让出;帧仍在分片通道/缓冲中,不丢不重。
- 验收:三道门绿;yamux 测试通过;建议加"满载下可让出"的压力单测。

### C-3 [P2] error 互斥锁被当热标志高频检查 —— 来源 yamuxCore-3
- [ ] 状态:
- 位置:`vendor/yamux/src/connection/halves/shard.rs:876-877/974/1194-1196/1312-1316`、`vendor/yamux/src/connection.rs:1966/1990/2019/2455`
- 问题:2+2N 个任务每轮 poll 对 error 槽 lock-is_some;set_error 本就同步置位粘性 workers_shutdown(:1378),语义等价"曾置位"。
- 修法:所有 `error.lock().is_some()` 前加 `workers_shutdown.load(Relaxed)` 快路径门(为假跳过取锁);仅取错误值时才真正锁。
- 验收:三道门绿;yamux 测试通过(错误传播路径不变)。

### C-4 [P2] 每帧路由路径未门控的 sched_diag 自增 —— 来源 yamuxRest-3
- [ ] 状态:
- 位置:`vendor/yamux/src/connection.rs:1977-1979`(FRAMES_IN_POLL);同型:flow_control.rs:88(on_wu_credit)、connection.rs:1201(on_read_lock_miss)、frame/io.rs:597(on_spare_empty)、stream.rs:735(on_wu_path)
- 问题:所有连接的接收任务每帧对同一 static cacheline 做原子 RMW,违反 sched_diag"关闭时零开销"的模块约定;读取方全部已有 enabled() 门控,门控递增后语义不变。
- 修法:五处统一包进 `if crate::sched_diag::enabled()`。
- 验收:三道门绿;诊断开启时统计仍工作(sched_diag 相关测试)。

### C-5 [P2] 发送热路径每帧过一次 rtt.next_ping(锁 + vDSO) —— 来源 yamuxRest-4(合并 yamuxCore-4)
- [ ] 状态:
- 位置:`vendor/yamux/src/connection.rs:2516`(调用)、`vendor/yamux/src/connection/rtt.rs:47-56`(实现)
- 问题:ping 10s 一次,数据面每帧却付一次 parking_lot Mutex + Instant::now;pop_to_send 在前、next_ping 第二(to_send 常态为空故每帧必经)。
- 修法:判定抬到"每 poll 一次"(fill_batch 前查一次即可,rtt 状态不可能在取帧中途变到点);或把到期时刻缓存为原子量先无锁比较。保持既有 deliberate 语义:空闲连接无 poll 就不发 ping。
- 验收:三道门绿;yamux ping/pong 与空闲保活测试通过。

### C-6 [P2] StreamWriteHalf 每帧对同一把流锁取放 2-3 次 —— 来源 yamuxShard-2
- [ ] 状态:
- 位置:`vendor/yamux/src/connection/stream.rs:796-833`
- 问题:poll_write_chunk 三段临界区(写窗口/ack_pending/update_state)可合并;转发热路径每帧执行,常见路径多付 1 次无争用取锁。复核确认合并无顺序障碍(全部状态在同一把 Shared 锁内,唯一锁外状态 self.flag 由 &mut self 独占)。
- 修法:第一个临界区内一并读清 ack_pending、判定最终 ACK 形态并完成 update_state;header 的 flag 修改(frame.header_mut().ack())留锁外。单帧收敛为 1 次取锁。
- 验收:三道门绿;yamux 流控/ACK 测试通过。

### C-7 [P2] to_send 是全连接唯一无界队列 —— 来源 yamuxRest-5
- [ ] 状态:
- 位置:`vendor/yamux/src/connection.rs:1303`
- 问题:WU 快车道/RST/FIN/Pong 只进不出,载体写侧长停滞时累积。复核补充:存在间接反压(WU 堆积→对端窗口耗尽→写侧停发→读侧断粮),稳态上限 ≈ 3 帧 × 活流数,实际量级 ~100KB。
- 修法:给 to_send 设条数上限(如 STREAM_COMMAND_CAP 量级),越限时 WU 快车道回落流级 mpsc 有界通路。**注意成本:回落路径必须"先 poll_ready 再记账"(stream.rs:558 注释明言该模式与单锁热路径不相容);鉴于间接反压已钉住上限,本条收益有限,可选——若不做,在提交/文档记录该判定依据。**
- 验收:三道门绿;停滞场景模拟测试(可参照既有测试基建)。

### C-8 [P2] 小缓冲 miss 一律按 64KiB 类分配 —— 来源 yamuxCore-5
- [ ] 状态:
- 位置:`vendor/yamux/src/pool.rs:177`、收进条件 :213
- 问题:控制面小帧每帧背 64KiB 缓冲,内存放大 10-20 倍;池 256 条上限被 64KiB 条目占满(≤18MiB 常驻)。复核确认主要作用在低速率控制帧,绝对驻留 ~128KiB,无 CPU 开销,顺手修。
- 修法:按容量分档(16/32/64KiB 三档,各档独立计数上界),get 分配最小满足档;或对小 k 直接精确分配不复用(参照 take_body ≤4KiB 路径)。
- 验收:三道门绿;池相关测试(上界断言)通过。

### C-9 [P3·存疑] Framer 每帧固定开销(take_error 锁 + workers_gone 遍历) —— 来源 yamuxCore-2
- [ ] 状态:
- 位置:`vendor/yamux/src/connection.rs:1916`
- 复核结论:机制存在,但空 poll 为无锁原子操作,实测推算单帧 ~50-100ns,低于原发现主张;不值得盲改。
- 处置:**不直接修**。若要处理,先量化(mux 档基准 + `perf`/火焰图看该函数占比);确认占比显著再做"错误快路径原子门 + workers_gone 快照化";确认不显著则按 AGENTS.md 写入 `docs/not-supported.md` 并附复核数据。

---

## 包 D:服务端(rh-server)

### D-1 [P1] API 响应双序列化(Value 树 → 再序列化) —— 来源 serverApi-1
- [ ] 状态:
- 位置:`crates/rh-server/src/api.rs:80-82`(ok() 信封);最重的消费方 :925-947(list_services 每行 20 字段 + 内嵌整份 spec)、:446-468(list_clients)
- 问题:所有响应先经 `json!` 构造完整 serde_json::Value 树,axum Json 再把树二次序列化——同一数据两遍,分配量数倍于响应体积;api.rs:884 注释"json! 按引用序列化"低估了 to_value 成本。
- 修法:列表端点改 `#[derive(Serialize)]` 强类型行结构体直接交 axum::Json;envelope 用包一层 Serialize 的包装 struct 实现。先改最重的 list_services/list_clients/stats_overview/list_events,再逐步铺开。
- 验收:三道门绿;**新增对比测试:改造前后同一数据的 JSON 结构逐字段一致**(这是行为红线);前端 console 各页正常(list 依赖的字段名不能变)。

### D-2 [P1] UDP 上/下行泵每包对同一会话表加锁两次 —— 来源 serverData-1
- [x] 状态:完成 @ 665ba1a
> 核查:✅ 问题在,行号无漂移(1957/1990、2233/2243);up 泵 insert 提前到写之前,写失败残留 entry 由通道死亡路径 data.rs:1858 的 clear() 与 retain GC 兜底;down 泵 send_to 错误本就被忽略、insert 照做,行为等价。修法可行。
- 位置:`crates/rh-server/src/data.rs:1957/1990`(up 泵)、:2233/2243(down 泵)
- 问题:admit 一次 + 发送后刷新时间戳一次,双向高 pps 时两任务在同一 std Mutex 上互抢。
- 修法:"admit + 时间戳刷新"合并进一次 lock()(insert 提前到 send 之前),每包每方向 2 次降 1 次,不改数据结构。复核确认语义差异仅为 µs 级 idle 窗口偏移,通道死亡路径外层有 clear() 兜底。
- 验收:三道门绿;UDP 转发测试通过。
- 备注:peer.sh UDP 档验证。

### D-3 [P1] 每 ACK 全量遍历客户端规则,O(N²) 内联在控制读循环 —— 来源 serverMisc-1
- [ ] 状态:
- 位置:`crates/rh-server/src/runtime.rs:170-214`;调用链 control.rs:951→1056;协议 `crates/rh-protocol/src/message.rs:112-127`(ApplyResult ok 无 service_id)
- 问题:批量改 N 条规则 = N 个 ACK × O(N) 收集遍历 + N 次 Vec 分配,全在该客户端控制读循环内,挤占 PONG/取消处理。
- 修法(两档):① 协议方案——ApplyResult ok 路径携带 service_id,降为 O(1) 定点推进(涉及 rh-protocol 协议字段,确认服务端/客户端同仓同发、无跨版本兼容负担后再做);② 保守方案——对 `revision <= 会话已应用 revision` 的重复/落后 ACK 短路跳过整轮遍历(复核提醒:只对同 revision 重复 ACK 生效,逐条编辑场景解不了,可作第一步)。
- 验收:三道门绿;可参照 session.rs 的 apply_sync_lock_tests(n=3000)形态加服务端侧批量 ACK 测试。

### D-4 [P2] server config 端点同步 std::fs 混入 async handler —— 来源 serverApi-2(合并 serverApi-3)
- [ ] 状态:
- 位置:`crates/rh-server/src/api.rs:2667-2698`(put:持 hot 读锁窗口内 toml 序列化 + std::fs::copy/write)、:2241-2243(get:同步 read_to_string)
- 问题:R1 注释自证此模式危害并已修掉 authorized_keys 一处,persist 段同款问题保留;get 侧同步读盘无上界。
- 修法:put 的序列化与文件写移到锁外并用 spawn_blocking(或 tokio::fs);get 改 tokio::fs::read_to_string。与 R1 对 authorized_keys 的修法完全同构,两处一并覆盖。
- 验收:三道门绿;server config 读写 API 测试通过。

### D-5 [P2] update_client 禁用分支 visitor 全量取回只用 vid —— 来源 serverApi-4
- [ ] 状态:
- 位置:`crates/rh-server/src/api.rs:659`;`crates/rh-server/src/db.rs:777-780`
- 问题:R4 投影优化漏了 visitor 侧(同函数 proxies 行已有"一列投影"注释)。低频管理路径,顺手修。
- 修法:db.rs 增加 `list_visitor_ids(client_id)` 一列投影(SELECT id),与既有 list_proxy_ids(db.rs:496)对称。
- 验收:三道门绿;client 禁用流程测试通过。

### D-6 [P2] require_auth 每请求 Value 树 + 重建 DecodingKey/Validation —— 来源 serverApi-5
- [ ] 状态:
- 位置:`crates/rh-server/src/api.rs:320-342`
- 问题:32 处调用点每请求付:claims → Value 树动态取值、from_secret 拷贝 secret、Validation::new 的 Vec/HashSet 分配;jsonwebtoken 内部 claims 实际还反序列化两次。
- 修法:定义 `#[derive(Deserialize)] struct Claims { sub: String, pv: i64 }` 强类型解码(顺带消掉内部双解析);DecodingKey/Validation 以 web_jwt_secret 的 Arc 指针为键缓存(写入点仅 api.rs:250 change_password 一处)。
- 验收:三道门绿;认证成功/过期/密码版本不符的 API 测试通过。

### D-7 [P2] enable_service 同一请求两次 get_client —— 来源 serverApi-6
- [ ] 状态:
- 位置:`crates/rh-server/src/api.rs:1351-1370`
- 问题:第一次只留 enabled 整行丢弃,禁用分支再查一次取 config_revision;主键点查微秒级,顺手修。
- 修法:第一次保留整行,禁用分支复用(handler 已持 serial_lock,无一致性障碍,对照 enable_service_locked 的 R1 约定)。
- 验收:三道门绿;enable 流程测试通过。

### D-8 [P2] 自定义 404 页读失败不缓存,每 404 同步读盘 —— 来源 serverCtl-1
- [ ] 状态:
- 位置:`crates/rh-server/src/vhost.rs:698`;调用点 :384/:428
- 问题:配置了 404 页但文件缺失时,每个 404(扫描器洪峰主力)都在 tokio worker 上同步 std::fs::read,永不收敛(注释 705-706 表明"补文件后生效"是有意取舍)。
- 修法:保守改法 `tokio::fs::read`(保留补文件语义);**不要用永久负缓存**(会废掉补文件生效行为);顺带 :695 `m.get(path).cloned()` 改 Bytes 免整页 memcpy。
- 验收:三道门绿;vhost 404 路径测试通过。

### D-9 [P2] Basic 认证每请求 3 次可免分配 + 手写 base64 线性查表 —— 来源 serverCtl-2
- [ ] 状态:
- 位置:`crates/rh-server/src/vhost.rs:392`(to_string)、:641-648/:1276-1290(check_basic_auth/base64_decode);第二个调用点 :1142-1150
- 问题:每字符对 64 字节表线性扫描 + 2 个 Vec 分配;base64 crate 已是依赖(control.rs 在用),手写实现纯属重复。
- 修法::392 改传 Option<&str>;base64_decode 换 base64 crate engine,**必须配 `DecodePaddingMode::Indifferent`** 保持现有容忍缺 padding/内嵌换行的行为(复核确认直接替换属行为收紧)。
- 验收:三道门绿;认证测试补"缺 padding/带换行"用例后再替换。

### D-10 [P2] SQLite 只设 WAL 未设 synchronous(默认 FULL,每提交一次 fsync) —— 来源 serverData-2
- [ ] 状态:
- 位置:`crates/rh-server/src/db.rs:29-43`
- 问题:sqlx 0.9 不显式下发 synchronous pragma,SQLite 默认 FULL;客户端每次连接的 update_client_meta 单条 UPDATE = 一次隐式提交 = 一次 fsync。
- 修法:`.synchronous(SqliteSynchronous::Normal)`(SQLite 官方 WAL 推荐组合:掉电至多回滚最近几笔提交、无损坏风险);若刻意保留 FULL,补注释说明并记录。
- 验收:三道门绿;db 测试通过;提交信息写清持久性取舍依据。

### D-11 [P2] allocate_port 在 async fn 内阻塞探测最多 1.5 万端口 —— 来源 serverData-3
- [ ] 状态:
- 位置:`crates/rh-server/src/state.rs:1425-1443`(探测在 :1434-1438)
- 问题:循环体内无 await 点,std bind 逐个探测,端口占用率高时阻塞 worker 数十 ms(实测参照:15001 次 ≈ 97ms)。三个调用点均为低频管理路径。
- 修法:探测改 tokio::net 异步 bind,或整个循环移入 spawn_blocking。
- 验收:三道门绿;port_auto 分配测试通过。

### D-12 [P2] 每新 UDP 源读全局 limits RwLock —— 来源 serverData-4
- [ ] 状态:
- 位置:`crates/rh-server/src/data.rs:1619-1623`
- 问题:仅新源首包触发(contains_key 快路径在先),读多写少竞争弱;复核反证:同路径每包必付的是 sessions Mutex insert,此项收益进一步缩小,顺手修。
- 修法:仿 mux_streams 原子缓存(state.rs:81 先例):max_udp_sessions_per_proxy 存 AtomicI64,热改保存点(api.rs mux_streams.store 同位置)同步 store;0=不限语义在 data.rs:1624 判断,保持不变。
- 验收:三道门绿;UDP 限额测试通过。

### D-13 [P2] 管理 API 的 TLS 握手串行在 accept 循环且无超时 —— 来源 serverMisc-2
- [ ] 状态:
- 位置:`crates/rh-server/src/main.rs:556-571`
- 问题:axum::serve 等 accept() 返回才 spawn 连接任务,握手既不并发也无时限;一个不发 ClientHello 的连接可让管理面 accept 无限期停摆(Slowloris 式,亦是可用性/韧性修复)。仅 tls_cert/tls_key 配置时生效。
- 修法:accept 只返回裸 TCP 流,把 `TlsAcceptor::accept` 移进每连接任务并包 `tokio::time::timeout`(如 10s);最低限度先给现有握手包 timeout 防永久停摆。
- 验收:三道门绿;TLS 启动的管理面手工/集成验证握手正常、超时连接不拖垮 accept。

### D-14 [P2] SSH 网关 accept 出错裸 continue(忙转 + 零日志) —— 来源 serverMisc-3
- [ ] 状态:
- 位置:`crates/rh-server/src/sshgateway.rs:98-99`
- 问题:EMFILE/ENFILE 等持续错误下循环紧空转打满一核且无任何日志;仓内 vhost.rs:22/:771、control.rs:220-223、main.rs:566-567 均有退避,唯独此处裸 continue。
- 修法:错误分支加 50-100ms 退避 + 一条 warn,对齐既有做法。
- 验收:三道门绿;sshgateway 测试通过。

### D-15 [P2] 内嵌控制台静态资源无任何缓存头 —— 来源 serverMisc-4
- [ ] 状态:
- 位置:`crates/rh-server/src/embedded_ui.rs:66-73`
- 问题:每响应仅 content-type,无 ETag/Cache-Control;浏览器每次打开控制台全量重下(~1.5MB,经隧道访问尤其浪费)。Vite 产物已自带内容 hash 文件名,缺的只是服务端缓存头;static_dir 分支(ServeDir)有 Last-Modified,两路径行为不一致。
- 修法:hash 过的 `/assets/*` 下发 `Cache-Control: public, max-age=31536000, immutable`;index.html 下发 `no-cache`(保证发版即生效);保守做法统一 ETag/no-cache 走 304。
- 验收:三道门绿;手工验证:首次 200、二次 304(或 assets 命中磁盘缓存)、发版后 index 不陈旧。

---

## 包 F:console 前端

### F-1 [P2] columns 的 useMemo 永不命中(死代码) —— 来源 console-1
- [ ] 状态:
- 位置:`console/src/components/proxy-list-page.tsx:395-403`
- 问题:依赖含 useMutation 返回对象 ×3(每次渲染 spread 新对象,react-query 5.104.2 实现证实)+ 内联 onCreateVisitor,memo 必不命中;且下游 data-table.tsx:195 传给 useReactTable 的是展开数组,这层 memo 修好也无下游收益——纯死代码。姊妹页(clients/visitors-panel/certs)均为普通内联定义。
- 修法:直接删掉这层无效 useMemo,与同组列表页保持一致(最省事);或解构 isPending 等基本值修依赖。
- 验收:`console` 能构建(npm run build);页面功能手工冒烟(列表/启停/批量导入)。

---

## 已确认优化到位、不要重复优化的热点

以下位置经本轮审查确认已是刻意优化(详见报告"已检查、无需再改的热点"节),再动它们前先想清楚:
mux 主路径 64KiB 缓冲 / 零拷贝 poll_write_chunk / split_boxed_io 无锁拆半;forward copy_limited 16→64KiB 自适应;udpchan 与 data.rs 的 PING/PONG OnceLock 缓存与一次成帧;session.rs encode_control_frame 已编码帧队列;events 批量事务(N 次 fsync 压 1 次);mux_streams 原子缓存;yamux take_body ≤4KiB 精确分配与帧体回收池。

## 不覆盖范围(下轮如需再开)

- tests/、benchmark/、migrations/、deploy/ 目录;docs 与脚本。
- 纯静态分析:所有定量结论(哪条真的值多少吞吐/CPU)以 benchmark/peer.sh 三机实测为准;P1 数据面任务修完建议组织一轮对照基准。
