# 全量静态审计:性能 / 死代码 / 并行串行 / 零拷贝

> **状态:已全部落实**(2026-10-24)。30 条发现逐条核实属实并修完,提交 `b84e7e1`。
> 其后另有 12 轮全量扫描(第 1–12 轮,提交 `565569c`…`a4bcbae`)追加
> 修复约 150 项,其中多项属本文条目的**同型漏点**(如 R5 的 visitor 缓存
> 半边、R8 的 nathole retain)与**修后回归**(R2 的 vhost 双锁、R9 的
> order 空洞),均已记在各提交信息里。
> **读本文的口径**:条目本身是当时的事实与判断依据;若与当前代码冲突,
> 以代码为准 —— 本文列出的位置行号已随修复漂移。

审计基线:`e3f1324`(v0.5.1),工作树另有 2 个未跟踪文档。
范围:`crates/` 全部 5 个 crate 的 `src/`(约 5.0 万行)+ `vendor/yamux`(约 6.6 千行)。
方法:逐行静态阅读(热路径按大段全文读,不靠 grep 抽样)+ 机械信号交叉验证
(`cargo clippy --all-targets`、全仓 grep 调用点、registry 源码核对上游契约)。
本次**未跑基准**;所有"影响分级"是基于调用频率与代码形态的推断,不含实测数字。

---

## 0. 结论摘要

共 30 条发现。按"是否值得现在动手"分三档:

| 档 | 条目 | 说明 |
|---|---|---|
| **A. 应当修** | 8 条 | 有明确机理、触发条件在常见配置下成立、修复面局部 |
| **B. 值得修但需实测** | 10 条 | 机理成立,但收益取决于部署形态(载体类型/UDP 占比/规则规模) |
| **C. 死代码与一致性** | 12 条 | 无运行时收益,但会误导后续维护与排障 |

**最值得先动的三条**:

1. **`vendor/yamux/src/pool.rs:112` 大类缓冲预算只增不减** —— 3 行修复,
   直接恢复 C4 优化(见 A-1)。
2. **服务端 mux 中继的用户侧永远回落 `tokio::io::split`** —— round-69 的无锁
   拆分在服务端主路径上完全没生效(见 A-2)。
3. **`issue_create_channel_with` 用无界 `send().await`,且它在控制读循环内联
   路径上** —— A1 自锁形态的最后一处漏点(见 A-3)。

前两项都是"文档以为已经修好、实际被包装类型绕过"的形态,因此在性能报告里
看不到症状,却一直在付代价。

---

# A 档:应当修

## A-1【高】yamux 大缓冲池的字节预算只增不减,约 128 块后大类池永久失效

**位置**:`vendor/yamux/src/pool.rs:112-129`(`get`)、`:178-207`(`put`)

```rust
// get():大类分支,弹出缓冲后直接返回
if let Some(b) = g.pop() {
    drop(g);
    return b;            // ← 缺 self.large_bytes.fetch_sub(b.capacity())
}
```

```rust
// put():归还时累加预算
let cur = self.large_bytes.load(Ordering::Relaxed);
if cur + cap > LARGE_BUDGET
    || self.large_bytes.compare_exchange(cur, cur + cap, ...).is_err()
{
    return;              // 预算"满"即拒收
}
```

**分析**:`large_bytes` 全仓只有 5 个写点(`:94` 初始化、`:183`/`:186` 累加、
`:205` 在"所有分片都满"的失败路径里减)。`get()` 弹出缓冲时**不减**。

上行读块默认 256KiB(`mux.rs:432` 的 `up_chunk_cap`),落在
`(CLASS_MAX, LARGE_MAX]` 区间,恒走大类。每次 `get`→`put` 净增 256KiB,
`LARGE_BUDGET = 32MiB` → **约 128 个块后 `cur + cap > LARGE_BUDGET` 恒成立**,
此后所有 `put` 直接 return,池内条目消耗完后大类即空,`get(256KiB)` 每块
`BytesMut::with_capacity(262144)` 新分配。

也就是说 `docs/deep-review-performance-report.md` §3.3 记录的 C4 优化
(新增 `LARGE_MAX`/`LARGE_BUDGET` 大类),在稳态下**被自己的计数器绕过**。
默认配置即命中(任何 bulk 上行流),无需特殊开关。

现有测试抓不到:`pool_hit_rate_microbench` 用的是 `CLASS`(64KiB,走小类),
`put_oversized_rejected` 用 256KiB 但只验证"get 返回的容量有界",不验证复用。

**修复**:
```rust
if let Some(b) = g.pop() {
    drop(g);
    self.large_bytes.fetch_sub(b.capacity(), Ordering::Relaxed);   // 用实际容量
    return b;
}
```
注意用弹出缓冲的 `capacity()` 而非 `min`(put 侧也是按 `cap` 计的)。

**附带建议**:`put` 的"预算判定 + 加计"跨了两把锁(无锁 CAS 后在分片锁里再决定),
CAS 失败会平白丢掉一个本可复用的缓冲。把 `cur + cap` 判定挪进分片锁内、
用普通字段计数即可(`large[idx]` 已是 `Mutex<Vec<BytesMut>>`)。这不是必需项。

**验证方式**:补一条测试——连续 `get(256KiB)`→`put` 交叉 1000 次后断言
`allocations()` 增量 < 操作数的 5%(与 `pool_hit_rate_microbench` 同型,
但用大类容量)。

**置信度**:高(所有写点已全量核对;`large_bytes` 在 `get` 路径确实无减法)。

---

## A-2【高】服务端 mux 中继的用户侧恒为包装类型,无锁拆半从未生效

**位置**:`crates/rh-transport/src/mux.rs:672-681`、`crates/rh-transport/src/lib.rs:84-104`

```rust
// mux_relay:只有裸 TcpStream 才能无锁拆半
match crate::split_boxed_io(a) {
    Ok((r, w)) => (Box::new(r), Box::new(w)),
    Err(io) => {
        let (r, w) = tokio::io::split(io);   // ← std::sync::Mutex
        (Box::new(r), Box::new(w))
    }
}
```

```rust
// split_boxed_io:只认具体类型 tokio::net::TcpStream
let is_tcp = { let dynio: &dyn Io = &*io; dynio.as_any().is::<tokio::net::TcpStream>() };
if !is_tcp { return Err(io); }
```

**分析**:服务端 TCP 数据面在 `data.rs:1193-1198` 把用户连接包成
`CountedUserIo<TcpStream>` 后才交给 `assign_forward` → `mux_forward_chunked`
→ `mux_relay`。`CountedUserIo` 不是 `TcpStream`,`as_any().is::<TcpStream>()`
必然 false,于是回落到 `tokio::io::split`(内部是一把 `std::sync::Mutex`)。
上/下行两个泵份属不同 task,读写同一把同步锁。

`mux.rs:648-674` 的注释正是记录这个病灶:实测 `futex` 占 75% syscall 时间、
3690 次调用、平均 10.8ms。round-69 的修复只覆盖了"两侧都是裸 `TcpStream`"的
场景;**服务端用户侧 100% 是 `CountedUserIo`**(`data.rs:1197`,
vhost/visitor 也同),所以这条修复在服务端主路径上是空转的。

客户端侧 `dial_target` 返回裸 `TcpStream`,因此受益——这解释了为什么
客户端路径的 futex 问题看起来"已修好",服务端没有。

**修复**(两条路,推荐前者):
- **解包探针**:给 `CountedUserIo` 加 `peel(self) -> (TcpStream, CountedUserIoState)`,
  `mux_relay` 在调用 `split_boxed_io` 之前把已知的几层壳(`CountedUserIo`、
  `QuotaIo`、`PrependedIo`)剥掉,拆半后再把计量包装分别套到读写半上。
  `CountedUserIo::inner_ref()` 已存在(`data.rs:855`,linuex-only),
  可参考。工作量中等:需要为读半/写半各造一个只计单方向的计量包装。
- **最小改动**:`split_boxed_io` 增加 `CountedUserIo<TcpStream>` 的
  downcast 分支,直接 `into_split` 内部 `TcpStream`;计量语义上,
  读半只累计 in、写半只累计 out(当前 `CountedUserIo` 是双向都计,
  需要按方向拆分)。

**风险**:中。计量结果必须逐字节保持等价(有 `bytes_in`/`bytes_out`
断言测试)。建议先只做 `CountedUserIo`,不要一次动 `QuotaIo`/`PrependedIo`。

**置信度**:高(代码链完整:`data.rs:1197` 包装 → `assign_forward` →
`mux_forward_chunked` → `mux_relay`;`split_boxed_io` 的类型探测是精确类型匹配)。

---

## A-3【高】`issue_create_channel_with` 用无界 `send().await`,且在控制读循环内联路径上

**位置**:`crates/rh-server/src/state.rs:1539-1545`

```rust
let ok = sess
    .send(ServerMessage::CreateDataChannel(CreateDataChannel { request_id: rid, entity }))
    .await;
```

**分析**:`Session::send`(`session.rs:246`)是**无超时**的
`queue_tx.send().await`。而这条链路的两个入口都在"循环唯一消费者"的自锁形态里:

1. `control.rs:907` — `handle_client_message(...).await` 内联在
   `run_session_loop` 的读分支里;
2. `runtime.rs:126-153` — `on_apply_result` 内联在同一读分支,
   其 `warm_proxy` → `tcp_refill`(`data.rs:117`)→ `issue_create_channel_with`。

同一读循环同时是 `qrx.recv()`(队列唯一消费者)。队列(默认 256)满时,
`send()` 会 park 在"等自己消费"上,心跳、`sess.cancel`、`state.shutdown`
全部失联,任务与 fd 永久泄漏。

这正是 `session.rs:250-266` 那段注释描述的 A1 形态。同文件 `issue_relay_pair`
(`state.rs:1616`/`:1625`)与 `on_apply_result` 的重发分支(`runtime.rs:110`)
都已改成 `send_bounded` / `try_send_msg`,**唯独这条没有**。

**修复**:改用 `send_bounded(msg, CONTROL_SEND_TIMEOUT)`。
失败路径按现有语义处理——注意 `issue_create_channel_with` 失败时走
`take_pending_request_quiet`(不触发钩子),调用方(`tcp_refill`)会自己归还
inflight,这个约定必须保持。

**置信度**:高(调用链与自锁前提都已核对;`on_apply_result` 确实内联在读循环,
`warm_proxy` 确实调 `tcp_refill`)。

---

## A-4【中高】vhost 缓存路径提前 return,sender 永不归还上游连接池

**位置**:`crates/rh-server/src/vhost.rs:510-575`

```rust
let resp = sender.send_request(upstream_req).await?;
if let Some(cfg) = &cache_cfg {
    if cacheable && resp.status() == 200 {
        // ... collect 整个响应体 ...
        return Ok(resp);      // ← 548:不可缓存分支
        // ... 落缓存 ...
        return Ok(resp);      // ← 571:MISS 分支,body 已读完
    }
}
// 只有走到这里才挂 RecycleBody 归还
let body = if reusable {
    RecycleBody::new(body, Some(sender), rt.http_upstream.clone()).boxed()
```

**分析**:`sender` 从 `rt.http_upstream.borrow()` 借出(`:452`),池的归还
**只发生在 `RecycleBody::drop`**(`:277-300`)。缓存路径在 548 与 571 两处
提前 return,`sender` 随作用域 drop,hyper 连接任务收到关闭信号。

即使 `collected` 已把整个响应体读完(连接完全干净)、`reusable == true`,
也不会归还。结果是**所有"GET + 扩展名命中缓存白名单"的请求 100% 新建上游
连接**——恰好是最该复用的一类(静态资源)。`:284` 的注释还记录过
"实测 borrow=26500 give=0"这种形态。

**修复**:两处 return 前把 `collected` 包成 `RecycleBody` 返回(与 597 行同型),
让 Drop 统一判定:
```rust
let body = full_body_body(collected);
let resp = /* 构造响应 */;
let body = if reusable {
    RecycleBody::new(body, Some(sender), rt.http_upstream.clone()).boxed()
} else { body };
return Ok(resp_with_body);
```
注意 548 分支(`forbidden_by_cc || has_credentials || over_size`)同样适用——
响应体已完整读入内存,连接是干净的。

**先决条件**:`RecycleBody` 的 Drop 里已有"未 poll 完就丢弃"的判定
(`:277-300`),`Full` body 被完整消费后 `is_end_stream()` 为 true,
判定应当通过。需要补一条测试:缓存 MISS 后第二次请求命中缓存,
且上游连接池的 `borrow`/`give_back` 计数平衡。

**置信度**:高(两处 return 的位置与 `sender` 的生命周期都已核对)。

---

## A-5【中】TCP 数据面每连接无条件 `group_pick`,绕过已有的 `has_group` 快路径

**位置**:`crates/rh-server/src/data.rs:1170-1175`

```rust
let target_rt = match group_pick(&state, &rt).await {
    Some(t) => t,
    None => rt,
};
```

**分析**:`group_pick`(`data.rs:1232-1244`)第一件事是
`rt.spec.read().await`,命中分组后 `group_key()` 还要一次 `format!`
(`state.rs:1318-1324`)。vhost 的三处调用点(`vhost.rs:415/876/1130`)
都先查 `rt.has_group.load(Relaxed)` 跳过,注释明确写着"修前每请求都要
`spec.read().await`…高并发下这是明确的锁争用点"。

TCP 数据面是**每 accept 一条连接**就走这条路径,却没有这个快路径。
绝大多数规则 `group` 为空,等于给每条新连接白加一次 async 读锁。

**修复**:照抄 vhost 写法:
```rust
let target_rt = if rt.has_group.load(Ordering::Relaxed) {
    group_pick(&state, &rt).await.unwrap_or(rt)
} else {
    rt
};
```
`has_group` 在 `runtime.rs:585` 随 spec 同步维护,初值见 `state.rs:919`,
是现成的无锁标记。

**置信度**:高(与 vhost 三处已修复的形态完全同构)。

---

## A-6【中】配额判定在取通道之后,被拒连接白烧一条预热通道

**位置**:`crates/rh-server/src/data.rs:1175-1187`

```rust
let Some(channel) = get_channel(&state, &target_rt).await else { ... return };
let Some(_conn_guard) = try_open_user_conn(&state, Some(&target_rt)) else {
    // 配额超限:直接断开 —— 但 channel 已 pop 出池,随作用域 drop
    return;
};
```

**分析**:`get_channel` 已从池里 `pop_front` 取走一条完成握手的热通道,
并在返回前 `tcp_refill` 又签发了一条 CREATE 补位(`data.rs:209-222`)。
随后配额拒绝,`channel`(底层是一条 yamux 流)随作用域 drop 关闭。

每次被配额拒绝的用户连接 = 1 条预热通道作废 + 1 次额外 CREATE 往返 +
1 个 pending_request。`max_conns`/`flow_limit` 生效时,持续超限连接会
持续消耗客户端的通道建立能力。

**修复**:把 `try_open_user_conn` 提到 `get_channel` 之前
(`target_rt` 在组选路后已确定):
```rust
let target_rt = /* 组选路 */;
let Some(_conn_guard) = try_open_user_conn(&state, Some(&target_rt)) else {
    state.metrics.count_session_failure("quota_rejected");
    return;
};
let Some(channel) = get_channel(&state, &target_rt).await else { ... };
```
语义等价:配额判定不依赖通道。

**置信度**:高(`get_channel` 的 pop + refill 顺序已核对)。

---

## A-7【中】`invalidate_proxy_requests` 仍是 `mem::take` + 全表重建,与已修的 `add_pending_request` 不一致

**位置**:`crates/rh-server/src/session.rs:168-188`

```rust
let removed: Vec<PendingRequest> = {
    let mut m = self.pending_requests.lock().await;
    let mut keep = std::collections::BTreeMap::new();
    for (k, v) in std::mem::take(&mut *m) {
        if hit { drop_list.push(v); } else { keep.insert(k, v); }
    }
    *m = keep;
    drop_list
};
```

**分析**:`add_pending_request`(`:146-155`)已改成 `extract_if` 原地摘除,
注释写明"修前每登记一个请求都要把整张在途表搬进新 BTreeMap…O(P)"。
`invalidate_proxy_requests` 是同一张表、同一个模式,**持锁期间**新建
BTreeMap 并逐条 `keep.insert` 重插(每个节点重新分配)。

调用点是 `teardown_runtime`(`runtime.rs:504-507`),批量删除/禁用
N 条规则时是 O(N · P log P) + 2N 次整表分配,锁窗口与表大小成正比。

**修复**:改成 `m.extract_if(.., |_, v| hit(v))` 收集并就地删除;
钩子仍在锁外执行。

**置信度**:高(同文件已有现成模板)。

---

## A-8【中高】客户端 `NatHolePrepare` 在控制读分支内联执行:最长 5s 停摆 + 无界 `send_msg` 自锁

**位置**:`crates/rh-client/src/session.rs:1333-1335`、
`crates/rh-client/src/nathole_cli.rs:20-75`

```rust
// session.rs —— dispatch_frame 内联在 run_loop 的 select 读分支(:1176)
ServerMessage::NatHolePrepare(p) => {
    crate::nathole_cli::on_prepare(sess.clone(), p).await;
}
ServerMessage::NatHoleConnect(c) => {          // ← 这个已经是 spawn
    let s2 = sess.clone();
    tokio::spawn(async move { crate::nathole_cli::on_connect(s2, c, sd).await; });
}
```

```rust
// nathole_cli.rs:on_prepare
let Ok(sock) = tokio::net::UdpSocket::bind(bind_local).await else { return };
...
tokio::time::timeout(m.timeout_seconds.clamp(2, 5), rh_core::stun::stun_binding(&sock, server)).await
...
if sess.send_msg(ClientMessage::NatHoleReport(report)).await {   // ← 无界
```

**分析**:两个独立问题叠在同一个分支上。

1. **停摆**:`on_prepare` 做 UDP bind + STUN,最长 5s
   (`timeout_seconds.clamp(2, 5)`)。它内联在 `run_loop` 的 `select!` 读分支,
   而该 select 的 `send_rx.recv()` 分支是发送队列(256)的**唯一消费者**
   (`session.rs:1123` 起,已核对)。STUN 不可达时,读帧/写帧/心跳/cancel
   四条分支全部轮不到,持续 5s。
2. **自锁**:末尾 `sess.send_msg(...)`(`session.rs:280`)是
   `send_tx.send(bytes).await.is_err()`,**无超时**。队列满时 park 在
   "等自己消费"——与服务端 A1、以及同文件 PING 分支(`:1176` 附近)
   完全同形。PING 已经改 `try_send_msg`,`on_prepare` 漏了。

`NatHoleConnect` 已经是 `tokio::spawn` 形态,`Prepare` 没有对齐。

**修复**:
- 与 `NatHoleConnect` 一致改为 `tokio::spawn`;
- `on_prepare` 内的 `send_msg` 改有界发送(`ack` 已用
  `timeout(ACK_SEND_TIMEOUT, ...)` 的写法,可直接套用)。

**触发条件**:XTCP 打洞 + STUN 服务器不可达(5s 级),或控制队列因连接
churn(大量 UserConnect/HealthReport)接近满。

**置信度**:高(内联位置与队列唯一消费者关系都已核对;`send_msg` 无界确认)。

---

# B 档:值得修,但收益取决于部署形态

## B-1【高·零拷贝】UDP 热循环每包 3 次分配 + 2 次拷贝

**位置**:`crates/rh-server/src/data.rs:1453-1457`(上行)、`:1469-1486`(下行)

```rust
state.client_usage(&rt.client_id).add_bytes(n as u64);
let payload = udp_packet::encode(&peer.to_string(), &pkt_buf[..n]);
rh_protocol::write_frame(&mut ch_w, FrameType::UDP_PACKET, &payload).await
```

**分析**:每个上行报文付:
1. `client_usage(&rt.client_id)` —— `state.rs:290-295` 用
   `entry(id.to_string())`,**命中时也先构造 String**;同包在 flow_cut 检查处
   已经调过一次,这是第二次;
2. `peer.to_string()` —— 一次 `SocketAddr` 格式化 + 分配;
3. `udp_packet::encode` —— 新建 `Vec` 并把载荷整包拷进去;
4. `write_frame` → `encode_frame_bytes` —— 再建 `BytesMut` 并整包拷第二遍。

下行同样有 `src.parse::<SocketAddr>()` + 两次 `client_usage`。
按 10 万 pps 估算约 50 万次堆分配/秒,且全部落在单条 pump 任务上。

**修复**(按收益排序):
- `let usage = state.client_usage(&rt.client_id);` 提到外层循环一次
  (规则生命周期内 `client_id` 不变),或直接缓存到 `ProxyRuntime`;
- 上行改**一次成帧**:按 `HEADER + 1 + addr_len + n` 预留一个 `BytesMut`,
  先 `put` 帧头、再填地址前缀、再 `extend_from_slice(&pkt_buf[..n])`,
  一次 `write_all`,消除中间 `Vec` 与第二次 memcpy;
- 对端地址字符串复用缓冲(`write!(&mut buf, "{peer}")`)。

**注意**:服务端 UDP 泵目前**没有**客户端那样的写任务/批聚合
(`docs/perf-client-udp-writer-batching.md` 只改了客户端)。单任务双向 inline
`read_frame`/`write_frame`,任一方向写阻塞会让另一方向停摆——这是并行度问题,
改动面比上面三条大。建议先做"一次成帧 + usage 外提",写任务拆分单独评估。

**置信度**:高(代码路径确凿;幅度取决于 PPS)。

---

## B-2【中高·零拷贝】WsIo 未实现 `poll_write_vectored`,yamux 批量写在 ws/wss 上退化

**位置**:`crates/rh-transport/src/ws.rs:276-310`

**分析**:`YamuxCarrier::poll_write2/poll_writev`(`mux.rs:1424-1443`)固定走
`poll_write_vectored`。TCP/TLS 载体支持向量写,但 `WsIo` 两个方法都没覆盖,
落到 tokio 默认实现(`tokio-1.53.2/src/io/async_write.rs:152-162`:
**只取第一个非空 slice**)。

后果:
1. 每个 yamux 帧变成 ≥2 条 WS 消息(12B 头一条、帧体一条),各带 WS 帧头、
   一次 `to_vec()`、一次 flush;
2. `poll_writev` 的整批(≤33 段)只写出首段,批处理完全失效。

同仓库已在 `PrependedIo` 上修过同一缺陷并留了回归测试
(`lib.rs:1603-1631`,注释"实测复现:只写 12B 帧头"),`WsIo` 是漏网的第二个
包装层。`kcp.rs`/`quic.rs` 的包装同理(见 B-3)。

**修复**:给 `WsIo` 实现 `poll_write_vectored`(把 `bufs` 拼进一条
`Message::Binary` 后发出)与 `is_write_vectored() -> true`。
注:`tungstenite 0.30` 的 `Message::Binary` 载荷是 `Bytes`
(`message.rs:161`),可以直接从 `Bytes` 构造,无需 `Vec`。
补一条与 `prepended_io_forwards_vectored_write` 同型的回归测试。

**附带**(零拷贝):`ws.rs:262` 的 `*this.rd_buf = data.to_vec();` 多拷一次——
`Message::Binary` 本身就是 `Bytes`,可把字段改成 `Bytes`,用 `slice`/`advance`
交付,并顺带删掉 `:244-248` 的"超过 64KiB 就丢弃缓冲"特判。

**置信度**:高(两侧代码与上游默认实现都已核对)。

---

## B-3【中】KCP 会话空闲时 2ms 定时器空转;每包 5–6 次仅测试用原子 RMW

**位置**:`crates/rh-transport/src/kcp.rs:857-859`、`:892-911`

```rust
let mut ticker = tokio::time::interval(TICK);   // TICK = 2ms
ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
...
tokio::select! {
    _ = ticker.tick() => {}
    n = inbound.recv_many(&mut batch, BATCH_CAP) => { ... }
    _ = shared.notify.notified() => {}
}
shared.stats.wakeups.fetch_add(1, Ordering::Relaxed);
COUNTERS.wakeups.fetch_add(1, Ordering::Relaxed);
```

**分析**:`ticker.tick()` 无条件参与 select,**完全空闲**的会话也每 2ms
醒一次,醒来要做 2 次原子自增、`kcp.update()`、`is_dead_link()`、
`peeksize()`、约 4 次锁。`max_sessions = 4096` 时最坏 205 万次唤醒/秒。

`Shared.stats`(5 组计数)与 `batch_cap_hwm` 的**唯一读者是测试**
(`:4803` 的 `snapshot()`、`:5649` 的 `load`);生产读的是进程级 `COUNTERS`。
也就是每包/每轮有 5–6 次纯为测试服务的 Relaxed 原子 RMW,并与 `Shared`
的其他字段共享 cacheline。

**前置判断**:KCP **不是默认载体**(`config.rs:84` 默认 `protocol: "tcp"`),
只在显式配置 `kcp` 时启用。所以这条的优先级取决于是否有人在用 KCP。

**修复**:
- 定时器自适应:会话空闲(`wait_snd()==0 && 无待 ACK && read_buf 空`)时
  只等 `inbound`/`notify`/idle 超时,有活跃输入后再重建 2ms ticker;
- `Shared.stats`/`batch_cap_hwm` 收进 `#[cfg(test)]`(与 `splice.rs` 的
  `PumpObs` 同款门控)。

**置信度**:中高(机理确定;是否值得做取决于 KCP 部署量)。

---

## B-4【中】`compress_stream` 对底层 IO 无条件 `tokio::io::split`

**位置**:`crates/rh-transport/src/compress.rs:44-51`

```rust
let (inner_r, inner_w) = tokio::io::split(inner);
```
**分析**:这是数据面第三个入口(服务端 `data.rs:311`、客户端
`forward.rs:31/97`、`visitor_run.rs:239`)。当底层是裸 `TcpStream`
(multiplex=false 的 tcp 载体)时,压缩写泵与解压读泵共享一把 `std::sync::Mutex`,
与 A-2 是同一机理。

`pump_side` 是 `DuplexStream`(无 `into_split`,只能保持现状),
但 `inner` 是 `BoxedIo`,可以直接复用 `split_boxed_io`:
```rust
let (inner_r, inner_w) = match crate::split_boxed_io(inner) {
    Ok((r, w)) => (r, w),
    Err(io) => { let (r, w) = tokio::io::split(io); (Box::new(r), Box::new(w)) }
};
```

**置信度**:高(同仓库已有两处现成模板)。

---

## B-5【中】TLS 每条载体每次拨号都重建 `ClientConfig`,会话恢复缓存永远为空

**位置**:`crates/rh-transport/src/tcp.rs:640-652`(dial_io)、`tls.rs:249-257`

```rust
let (rust, verify) = crate::tls::client_config_with_alpn(&cfg.tls, &cfg.alpn)?;
...
cfg.resumption = rustls::client::Resumption::in_memory_sessions(256);
```

**分析**:`dial_io` 每次拨号(含 `PoolRedial` 的每次重拨、K 条载体各一次)
都重建配置:
1. `trusted_ca` 分支把 CA PEM 重新解析并 `RootCertStore::add` 一遍;
2. `with_alpn` 每次新建 `in_memory_sessions(256)`,即**全新的空票据缓存**——
   rustls 的恢复缓存挂在该 `ClientConfig` 实例上,实例被丢弃 → 缓存失效。

所以掉线重连/池补位永远走完整握手,1-RTT PSK 恢复形同虚设
(与 `tls.rs:251-255` 注释声称的意图相悖)。

**修复**:在 `connect_stream_pool`/`connect_stream` 层构造一次
`Arc<rustls::ClientConfig>` 并下传,重拨闭包捕获同一份。

**置信度**:高(构造点与缓存归属都已核对)。

---

## B-6【中】vhost HTTP 热路径:每请求 4–6 次分配 + 全路由线性扫描

**位置**:`crates/rh-server/src/vhost.rs:323-361`、`:390`、`:865`

```rust
let host = req.headers().get(HOST)...split(':').next()...to_lowercase();  // 分配
let path = req.uri().path().to_string();                                  // 分配
let cache_key = http_cache_key(&host, &path, req.uri().query());           // 分配(恒执行)
...
let routes = state.proxies.vhost_http.read().await.clone();
for r in routes.iter() {
    if r.host != host { continue; }
    for loc in &r.locations {
        if path == *loc || path.starts_with(&format!("{l}/")) {            // 每 location 一次 format!
```

**分析**:
- `cache_key` 在确认 `route.cache.is_some()` **之前**就构造;
- `route.cache.clone()`(`:390`)深拷贝 `CacheConfig`(含 `Vec<String>`);
- 路由匹配是 O(全部域名) 的 `String ==` 比较,locations 内层再逐个 `format!`;
- HTTPS(`:865`)与 TCPMux(`:1085` 与 `:1110`,同一条 CONNECT 读两次
  `vhost_tcpmux` 快照并各扫一遍)同款。规则数上千时每请求上万次比较。

**修复**:
- `cache_key` 延迟到确认有缓存配置后构造;
- `VhostRoute.cache` 改 `Arc<CacheConfig>`;
- 路径前缀比较改 `path.as_bytes()` + 下一字节为 `/` 或末尾判断(零分配);
- `rebuild_vhost_routes` 时构建 `HashMap<String, SmallVec<[usize; 2]>>` 索引
  (host → 路由下标),请求路径 O(1) 取候选。

**置信度**:高(代码形态确定;绝对收益取决于规则规模)。

---

## B-7【中】`port_allowed` / `allocate_port` 每次调用重新解析白名单字符串

**位置**:`crates/rh-server/src/state.rs:120-126`、`:1261-1263`、`:1281`

```rust
pub fn port_allow_list(&self) -> PortAllowList {
    PortAllowList { ranges: self.allow_ports.iter().filter_map(|s| PortsRange::parse(s)).collect() }
}
pub async fn port_allowed(&self, port: u16) -> bool {
    self.hot.read().await.port_allow_list().allows(port)
}
```

**分析**:每次创建/更新/启停/恢复一条规则调用 1–2 次,每次 `hot.read()`
+ 逐条字符串解析 + `Vec` 分配。批量 1000 条 = 1000 次重解析。
`allocate_port`(`:1281`)也每次重建一份。

**修复**:解析结果缓存在 `HotConfig`(`port_allow_list()` 返回
`Arc<PortAllowList>`,写 hot 时重建一次)。

**置信度**:高。

---

## B-8【中】`port_in_use_full` 每次管理操作全表读 + 逐行反序列化

**位置**:`crates/rh-server/src/state.rs:1483-1495`

```rust
let Ok(rows) = self.db.list_proxies(None).await else { return false; };
for (id, _cid, spec) in rows { /* 每行 spec 已是反序列化后的 ProxySpec */ }
```

**分析**:`list_proxies` 会把**每一行**的 spec JSON 反序列化成 `ProxySpec`。
调用点 `api.rs:1018`(创建)、`:1465`(更新)、`:1499`(启用),N 条规则下
每次建/改/启一条都是 O(N),批量 N 条 = O(N²)。
`migrations/0001_init.sql:47` 已有 `idx_proxies_bind(kind, bind_port)` 索引。

**修复**:改按索引精确查(`WHERE bind_port=? AND kind=? AND enabled=1`),
或只取 `bind_ip/bind_port/group_key` 三列。

**置信度**:高。

---

## B-9【中】`on_apply_result` 每个 ACK 全量重读规则并重复预热(O(N²))

**位置**:`crates/rh-server/src/runtime.rs:125-155`

```rust
let proxies = state.db.list_proxies(Some(&sess.client_id)).await.unwrap_or_default();
for (_id, _cid, spec) in proxies {
    let (st, _) = rt.snapshot_state().await;
    ...
    } else if st == ProxyState::Running {
        warm_proxy(state, sess, &rt, &spec).await;   // 无条件重预热
    }
}
```

**分析**:客户端对**每条** `ServiceUpdate` 回一个 `ApplyResult`
(`rh-client/src/session.rs:1316-1318`)。服务端每收到一个 ACK 就把该客户端
**全部 N 条规则**从 DB 读出并反序列化,再对每条取锁;已 Running 的还无条件
`warm_proxy`(进 `tcp_refill`,取池锁 + 签发)。批量改 N 条 → O(N²) 的
DB 反序列化 + 锁操作,且发生在控制读循环内联路径上。

**修复**:
- 只处理 `r.revision` 相关规则(或 `rt.latest_revision <= r.revision` 过滤);
- 数据源改为遍历 `state.proxies.runtimes`(内存,按 client_id 过滤),不重读 DB;
- `warm_proxy` 只在状态**由非 Running 变为 Running** 时调(删掉
  `else if st == Running` 分支,或每会话只做一次)。

**风险**:中。最后一条会改变"重连后补预热"的行为,需要确认该分支的用途
(注释写"如重连后"),可能应改为"每会话首次 Running 时预热"。

**置信度**:中高(复杂度确凿;改动需判断语义)。

---

## B-10【低中】mux 泵每轮重建批缓冲 Vec;WS 读侧与客户端 udpchan 的重复分配

**位置**:`crates/rh-transport/src/mux.rs:829`、`:919`、`:1206`;`ws.rs:262`;
`rh-client/src/udpchan.rs:382`、`:322`

```rust
// mux.rs pump_up:pending 在循环外,但 batch / prefetched 在循环内
loop {
    let mut batch: Vec<Bytes> = Vec::with_capacity(UP_BATCH_SEGS);
    ...
    let mut prefetched: Vec<Bytes> = Vec::new();
// pump_down::1206 同款
```

**分析**(三条独立但同型):
1. `flush()` 里的 `smalls` 明确保留容量跨轮复用,同循环的 `batch`/`prefetched`
   却每轮重建。满批(1MiB)时每 1MiB 一次分配可忽略;小流/滴流时每批可能只有
   1 块,于是每 chunk 都要一次 2KB 分配,`prefetched` 还额外 realloc。
2. `ws.rs:262` 的 `*this.rd_buf = data.to_vec();` —— `Message::Binary` 载荷
   已是 `Bytes`(`tungstenite-0.30/message.rs:161`),多拷一次;
   `:244-248` 还有"capacity > 64KiB 就丢弃缓冲"的特判,改 `Bytes` 后可删。
3. `udpchan.rs:382` 的 visitor 侧每包 `peer.to_string()`,而 proxy 侧
   (`:254`)已把地址提到建会话时算一次(`Arc<str>`)。visitor 侧漏改。
   `:322` 的地址缓存用**首字节**取槽(`SLOTS = 8`),IPv4 源地址首字符几乎
   总是 `'1'`/`'2'`,实际只用 2–3 槽;多用户时每包都 miss,
   文档 `perf-client-udp-writer-batching.md` 的"命中率接近 100%"只在单用户下成立。

**修复**:
- `batch`/`prefetched` 提到 `loop` 外,循环开头 `clear()`;
- `WsIo.rd_buf` 改 `Bytes` + 偏移,用 `slice`/`advance` 交付;
- visitor 侧地址字符串按会话缓存;
- 缓存槽位改整串哈希或 `(len, first, last)`。

**置信度**:高(形态确凿;单项收益都是常数级)。

---

# C 档:死代码与一致性

## C-1 `pump_diag` 的 9 个统计量从未写入,`dump()` 无调用者

`crates/rh-transport/src/mux.rs:463-482` 声明 `READ_NS`/`WRITE_NS`/`READS`/
`WRITES`/`SLOW_READS`/`VERY_SLOW_READS`/`SLOW_WRITES`/`RESERVE_NS`/
`SOCKWRITE_NS`/`PARTIAL_WRITES`,全仓 grep 只有 `dump()`(`:571`)里的 `load`;
`dump()` 本身零调用(诊断已改走 `dump_local`)。

**影响**:无运行时成本,但一旦有人按旧文档调 `dump()` 会打印全 0 的
`PUMP_STATS`,误导排障。**修复**:删除 `dump()` 与这些静态量。

## C-2 KCP `Shared.stats` / `batch_cap_hwm` 仅测试读取

见 B-3。与 C-1 不同,这些在**热路径上有写**。

## C-3 `data.rs:379-386` 剥壳后再探测 `QuotaIo`,分支不可达

`:308` 已 `QuotaIo::peel(channel)` 无条件剥壳,`:384` 的
`downcast_ref::<QuotaIo>()` 恒 `None`。注释("更内层还有 QuotaIo")与实际时序
矛盾,会误导后续维护者。**修复**:删掉 `or_else` 分支。

## C-4 `control.rs:993-1004` `handle_client_message` 的 `Ping` 分支不可达

`run_session_loop:880-897` 在反序列化前就拦截 `FrameType::PING`,
唯一调用点(`:907`)永远拿不到 `ClientMessage::Ping`。且该分支用的是
`send().await` + 2s 超时,与函数"读循环唯一消费者"约束相悖——将来若有人
把 PING 挪进来就是 2s 自锁。**修复**:删除该分支。

## C-5 `server/session.rs` 的 `unconfirmed` 表:写了也读了,但结果无人消费

`mark_unconfirmed`(`:220`)写入;`confirm_applied`(`:228`)的返回值是
"仍在等待的最小 revision",但它只被读出来就被丢弃——两处调用点
(`runtime.rs:101` 直接表达式、`:123-124` 显式 `let _ =`)都不使用,
也没有后台任务消费它(无超时重发)。也就是说每次 ACK 的
`retain`(O(P))+ `insert` 对任何行为都没有影响。

**修复**:删除 `unconfirmed` 字段与两条方法的维护逻辑(保留
`applied_revision`),或补齐"未确认超时重发"的消费方。

## C-6 `state.rs:880/887` 两个只写不读的运行时字段 + `api.rs:254` 恒等函数

- `rt.tcp_listener`:唯一写入是 `runtime.rs:400` 的 `marker_listener()`(恒 `None`),
  无读取;
- `rt.udp_socket`:只在 `runtime.rs:296` 写入,UDP 泵用的是 move 进任务的 `sock`,
  无读取;
- `api.rs:254 fn chrono_hours(h: u64) -> u64 { h }` 恒等包装。

## C-6b `db.rs:331` `insert_proxy` 的 id 回填是独立语句,不在事务里

```rust
let id = r.last_insert_rowid();
spec.id = id as u64;
// 回填 id 保证 spec JSON 与行一致(读取方依赖 spec.id)
let _ = sqlx::query("UPDATE proxies SET spec=?2 WHERE id=?1") ... .execute(&self.pool).await;
```

INSERT 与 UPDATE 是两次独立往返,且 `let _ =` 吞掉错误——UPDATE 失败会留下
`spec.id = 0` 的行。同文件 `insert_proxy_bump` 把同类 UPDATE 放在事务内,
说明事务版本可用。**修复**:改用 `INSERT ... RETURNING id`(SQLite 3.35+)
或把两条语句并入事务。

## C-6c 每连接 `with_label_values(&[&rt.id.to_string(), "tcp"])`,且子项从不回收

`crates/rh-server/src/data.rs:1188-1204` 每条 TCP 连接建/关各一次
`rt.id.to_string()` 分配,`with_label_values` 内部还要按 label 查表加锁。
更关键的是全仓无 `remove_label_values` 调用,`IntGaugeVec` 的子项按 proxy_id
**无界累积**——长跑 + 频繁建删规则时内存与 `/metrics` 输出持续增长。

**修复**:label 字符串缓存在 `ProxyRuntime`(如 `id_str: String`);
`teardown_runtime` 里 `remove_label_values`。

## C-7 `rh-core/proxyproto.rs:79-182` 入站解析侧无生产调用

`parse_header` 全仓只出现在自身测试与 `lib.rs:11` 的 re-export;
服务端不解析入站 PROXY 头,客户端只用 `encode_v1`/`encode_v2`。
`parse_v1` 内部还用 `collect::<Vec<_>>()` + 两次 `format!`,
若将来接入则是每连接开销。**修复**:标 `#[cfg(test)]` 或删除,
保留 `encode_*`。

## C-8 其他零调用导出 API

- `Multiplexer::detach_carrier`(`lib.rs:1039-1048`)——零调用
  (测试直接用 `YamuxMultiplexerPool::detach_carrier`);
- `Multiplexer::is_dead`(`:1134`)、`pool_live_carriers`(`:1023`)——库外零调用;
- `ProxySpec::udp_cfg`(`proxy.rs:467`)、`PortAllowList::allow_all`(`net.rs:42`)、
  `PortAllowList::pick`(`net.rs:55`,内部是**阻塞** `TcpListener::bind`,
  在 async 上下文误用会阻塞线程)、`BandwidthQuantity::from_bytes_per_sec`、
  `SyncEntity::{entity_id, entity_name, enabled}`;
- `stun::spawn_test_stun_server`(`stun.rs:141`)未加 `#[cfg(test)]`,
  测试服务器代码被编进生产二进制。

## C-9 `plan_convergence` 第二返回值在生产路径算完即弃

`rh-client/src/session.rs:415-420` 返回 `(final_k, Vec<usize>)`,
调用点 `:1023` 直接丢弃并分配临时 `Vec`。函数注释自己也写明"仅诊断/兼容
既有断言"。**修复**:拆成生产用单值 + `#[cfg(test)]` 辅助。

## C-10 `dispatch_frame` 的 `control`/`buf` 参数无用

`rh-client/src/session.rs:1290-1293` 只在 `:1384-1385` 的"忽略帧"分支
`let _ = (control, buf);` 消费掉,任何逻辑都不读。**修复**:删除这两个参数。

---

# 已核实为刻意设计、不再报为缺陷

以下都复核过注释/测试/文档,确认是正确性要求或已论证的取舍:

| 位置 | 理由 |
|---|---|
| `copy_bidirectional_bounded` 的有界第二向、`copy_relay_pair` 的有界 join | round-40 的 FIN-WAIT-2 泄漏实测(10 轮 `-P8` 累积 22 条),红线 |
| 控制读循环全部 `try_send`/`send_bounded` | A1 自锁,必须 |
| `serial_lock(client_id)` 覆盖的路径 | 防端口竞态与规则双占,正确性要求 |
| `tcp_refill` 逐条签发 | 任务书明确要求;`add_pending_request` 已改 `extract_if` |
| `collect_pool_members` 的 `FuturesUnordered` + `now_or_never` | 快速失败 + 不泄漏已建成员 |
| 下行 64KiB 锁步(`CHUNK_CAP`) | round-8 实测放大读块 -4.0% |
| `Staging::take_body` 对 ≤4KiB 帧 copy-out | C1:防小切片钉住 staging backing |
| `pump_up` 的预取门控 `batch_bytes < LIMIT` | 满批时数据在内核继续堆积,下一轮立即返回 |
| `drain_pending_routes` 的全量重试 | round-34 的侧链设计;B-3 之外的 O(n²) 风险只在单分片长时间阻塞时出现 |
| `MissingTickBehavior::Skip` 下的 KCP tick | 见 B-3,已在 B 档 |
| `split_boxed_io` 只认裸 `TcpStream` | 类型探测是保守正确的;A-2 是"调用方没剥壳",不是这个函数错 |
| `events` 无界队列 | 注释论证:事件量级小,bounded 只能在丢事件与阻塞业务间二选一 |
| `list_services` 未改 `try_read` | 会改变返回语义,报告 §7 已否决 |
| yamux `Active`/`Closing`/`Cleanup` 状态机生产不可达 | 上游 parity 保留,`stream.rs:114-120` 有明确注释 |

---

# 附:本次审计的 5 条方法论备注

1. **"已修的优化"要验证它是否真的在热路径上生效**。A-1(池预算泄漏)与
   A-2(包装类型绕过无锁拆半)都属于"文档记录已修、代码看起来也对、
   但实际被另一种形态绕过"。仅读 diff 与注释会漏掉这两条。
2. **计数器类不变量用"所有写点"核对**,不要只看定义处。A-1 的正确性来自
   `grep large_bytes` 只有 5 个写点这个事实。
3. **`tokio::io::split` 的代价被多处低估**。本次在 4 个地方发现它
   (A-2、B-1、B-4、B-6 的 `copy_bidirectional_bounded`),而仓库已有
   无锁拆分的现成工具与量化数据,只是没推广到所有入口。
4. **上游默认实现的语义要回 registry 核**。B-2 的结论
   ("tokio 默认 `poll_write_vectored` 只写第一段")不是推测,是
   `tokio-1.53.2/src/io/async_write.rs:152-162` 的代码。
5. **测试盲区**:`pool_hit_rate_microbench` 只测小类,所以 A-1 一直绿;
   vhost 缓存路径缺"borrow/give 平衡"断言,所以 A-4 一直绿。
   凡是"优化用计数器/池"的地方,应有对应的**稳态复用**断言。
