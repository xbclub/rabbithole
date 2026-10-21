# 调查:mux 并发流上限该不该加到客户端(结论:不改)

一句话结论:**不建议按原方案实施**。实测证明 yamux 的 `max_num_streams` 超限会**连带杀死该载体上所有已有流**,而不是"只让单条流报错"。这个上限加在客户端是**危险的**,且与仓库现有设计方向相反。性能上倒不用担心(`open_stream` 一次原子递增),但它防不住任何东西、只会把"新流失败"变成"整条会话挂掉"。

## 0. 调查方式

- 仓库 `rabbithole`,HEAD `90f6f6b` + 前几轮未提交改动。
- 用两个一次性 example 实测(`_mux_limit_probe` / `_mux_limit_probe2`,**已删除**,未进工作树)。

## 1. 决定性证据:超限会杀死整条载体

对照实验(`wrap_stream_as_client(io, true, limit)`,客户端开流,服务端 echo):

| 场景 | `open_stream` 结果 | 已开流的可用性 |
|---|---|---|
| 限 10,开 4(不超限) | 4 次全 OK | **4/4 正常收发** |
| 限 4,开 4(**恰好**到限) | 4 次全 OK | **4/4 正常收发** |
| 限 4,开 5(**超限**) | 第 5 次 `ERR yamux: maximum number of streams reached` | **0/4 全部失效** |

超限后的第二次开流得到 `ERR mux driver gone` —— 连接已进入终结。

### 代码层面的原因

`vendor/yamux/src/connection.rs:2307-2313`:

```rust
if self.shared.stream_count.fetch_add(1, SeqCst) + 1 > self.shared.config.max_num_streams {
    self.shared.stream_count.fetch_sub(1, SeqCst);
    log::error!("{}: maximum number of streams reached", self.shared.id);
    return Poll::Ready(Err(ConnectionError::TooManyStreams));   // ← 连接级错误
}
```

`poll_new_outbound` 返回的是 **connection 级的 `Err`**,不是 stream 级的拒绝。同文件 `:762-776` 的对端路径更直接:

```rust
if self.streams.len() == self.config.max_num_streams {
    log::error!("{}: maximum number of streams reached", self.id);
    return Action::Terminate(Frame::internal_error());          // ← 直接 Terminate
}
```

即**对端收到超限 SYN 会直接终止连接**。这也解释了 `rh-server/src/config.rs:180` 的注释:

> yamux 超限 SYN 会终止整条会话,故配额须低于此值。

## 2. 原方案的三处问题

### ① "超限的 open_stream 返回错误(不静默排队),让上层当该通道失败处理" —— 做不到

上层拿到错误时,**该载体上的其它流已经一起死了**。客户端在 mux 模式下,一条载体承载该会话的所有数据通道;`initial_k=2` 时 4 条流死一半。原方案以为的"单通道失败"实际是"半条会话失败"。

要真做到"单流失败",需要改 yamux 在超限时返回流级错误而非连接级 —— 那是 vendor 的行为改动,超出本任务范围,且会偏离上游语义。

### ② 服务端已有完整的对称机制,客户端这层是重复的

服务端在应用层做了配额(`control.rs:1013`):

```rust
let count = sess.data_conn_count.fetch_add(1, Relaxed) + 1;
if count as usize > state.limits.read().unwrap().data_conn_limit() {
    sess.data_conn_count.fetch_sub(1, Relaxed);
    return Err(anyhow::anyhow!("data connection limit"));      // ← 流级拒绝,不杀连接
}
```

而 `data_conn_limit()`(`config.rs:229`)刻意钳制到 `mux_streams() - 1024`:

> 每客户端并发数据通道有效上限:配额(0=∞)钳制到 mux 顶 -1024,保证应用层永远先于 yamux 干净拒绝(留 1024 给在途未完成 HELLO 的流)。

**这是正确的设计**:应用层用"拒绝新流 + 保留老流"的方式拒绝,而不是让 yamux 炸掉连接。客户端再加一层 yamux 硬顶,等于把"干净拒绝"换成"粗暴断连"。

### ③ 客户端是开流方,天然不是需要设防的那一侧

`tcp.rs:397` 的注释说得很清楚:

> 客户端为开流方:超限是单流报错(不杀会话),取"不限制"即可,真正的策略上限由服务端在 `DATA_CHANNEL_HELLO` 处执行。

注意这条注释里的"超限是单流报错(不杀会话)"**在 yamux 层面是错的**(我的实测证明了),但它描述的**意图**是对的:客户端不需要自我限流,因为限流是服务端对客户端的约束,客户端自己设一个只会自伤。

## 3. "会不会降性能" —— 不会,但这不是重点

性能上这个改动是**无害**的:

- `stream_count` 是一次 `fetch_add`/`fetch_sub`,且只在 `open_stream` 时发生一次 —— 不是每帧、不是每字节。
- 405 行注释里的 `open_stream` 是低频操作(每条数据通道一次),不是热路径。
- `set_max_num_streams` 里的 assert 约束由 `set_max_connection_receive_window(None)` 解除(`unwrap_or(usize::MAX)` 恒成立),4096 与 `1<<24` 都不会触发。

所以"降性能"的担忧不成立。**问题不在性能,在于语义**:这个上限一旦触发就是断连,而它的防护价值近乎为零(见 §2③)。

## 4. 那客户端到底有没有需要防的场景?

有,但不是这个改法。真正的风险是 **fd/内存敞口**:每流 6MiB 接收窗,`MUX_STREAMS_UNLIMITED = 1<<24` 时理论敞口 96 TiB。不过:

- 实际并发受**上层**限制:每条数据通道对应一条用户连接,用户连接数受 `ulimit -n` 约束(默认 256~65535);
- 服务端 `data_conn_limit` 已经限制每客户端并发数据通道数;
- 客户端崩溃的场景是"本地用户连接爆炸",那种情况下要防的也不是 mux 流数。

如果将来确实需要客户端侧护栏,**正确做法**是:

1. 在 `open_stream` **之前**用应用层计数器判断(像服务端那样),超限返回流级错误;
2. 或者把 yamux 硬顶设得**远高于**应用层配额(像服务端 `mux_streams - 1024` 的思路),只作为最后一道保险,让应用层先拒。

原方案(4096 直连 yamux 硬顶)恰好是反的:把保险丝设成了最紧的那道。

## 5. 建议

**不做**。若坚持要加客户端护栏,建议改成:

- `max_mux_streams` 只作为**应用层**配额,在 `ClientSession::open_data_channel` 前检查;
- yamux 硬顶保持 `MUX_STREAMS_UNLIMITED`,或设为 `max_mux_streams + 1024` 之类的**兜底**,永不先触发。

## 6. 顺带修正的一处文档错误(已改)

`tcp.rs` 原注释(修前):

> 客户端为开流方:超限是单流报错(不杀会话),取"不限制"即可

**前半是错的** —— 实测超限会杀死整条载体(`TooManyStreams` 是 connection 级错误)。

**已修正**,并把"为什么取不限制"改成了正确理由:

- `connect_stream` 内改为一段完整说明:指出原说法错误、给出实测结论、
  贴上代码路径(`poll_new_outbound` 返 `Err` → `send_driver` 先
  `dead.store(true)` 再终结驱动;对端 `Action::Terminate`)、并说明正确
  的分层(服务端应用层计数器 + 配额钳到 `mux_streams - 1024`)。
- `dial_client_mux` 内补一句指针,说明同样取"不限制"的理由,指向上述长注释。

修正的三条事实依据(逐条核对过):

| 断言 | 证据 |
|---|---|
| 超限是连接级错误 | `vendor/yamux/src/connection.rs:2307-2313` `poll_new_outbound` 返回 `Err(TooManyStreams)` |
| 对端收到超限 SYN 直接终止 | 同文件 `:770-775` `Action::Terminate(Frame::internal_error())` |
| rh-transport 把错误升格为载体死亡 | `mux.rs` `send_driver`:`dead.store(true, Relaxed)` 后 `return Poll::Ready(true)` |
| 服务端配额钳到 `mux_streams - 1024` | `rh-server/src/config.rs:234` `saturating_sub(1024)` |

实测复核(限 4 开 5):第 5 条 `ERR yamux: maximum number of streams reached`,
已开的 4 条 **0/4 可用**,再开流 `ERR mux driver gone`。对照不超限场景 4/4 正常。

验证:`cargo test -p rh-transport --lib` 170 passed;`clippy --all-targets -D warnings` 通过。
