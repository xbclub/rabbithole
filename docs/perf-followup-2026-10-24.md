# 2026-10-24 性能复核后续(基线 6cd514a / v0.5.2)

上一轮「30 条审计 + 12 轮扫描」收敛后,新一轮静态复核(三个子代理)又发现 19 条。
本文档记录该轮的处置:**8 条已修**(见 git log 同日提交),其余**留档防重复排查**。
所有结论均为静态机理,收益量化需 `benchmark/peer.sh` 三机基准。

## 已修(同日提交)

| 项 | 内容 |
|---|---|
| N1 | KcpIo 补 `poll_write_vectored` + `is_write_vectored`(B-2 的 kcp 半边,照 WsIo 模板) |
| N2 | `HttpCache::put` 插入路径 retain 限频 ≥1s(修前每个可缓存 MISS 全表扫,冷启动 O(K²)) |
| N4 | `on_apply_result` 对 Running 规则的补充预热按会话实例去重(修前每 ACK 深克隆 spec + warm,N 条批量 = O(N²)) |
| N5 | 客户端实体表 `HashMap<u64, ProxySpec>` → `HashMap<u64, Arc<ProxySpec>>`(修前每条数据通道在锁内深克隆整份 spec) |
| N7 | ApplyResult(!ok) 全量 SYNC 重发限速 1s(与 SyncRequest 分支同款) |
| N8 | 控制面写臂 `write_frame` 包 2s 超时(修前对端零窗口时读循环整臂失联) |
| N12 | 新增 `ProxyRuntime::state_kind()`;on_apply_result 不再为拿枚举 clone reason String |
| N16 | visitor 用户侧 accept 补 TCP keepalive(socket2,60s 空闲起探;C10 整改当时只做了 nodelay 半边,注释却声称两者都开) |

## 二批落地(同日,「能安全处理的」复评后)

| 项 | 内容 |
|---|---|
| N6 | KCP `run_session` 搬运改锁内切分、锁外 `kcp.send`(唯一喂入方,站立上界预算逐片扣减,算术与修前等价) |
| N9a | VhostRoute 头改写规则构建时预解析(`HeaderName/HeaderValue` 免每请求解析,四处应用点改预解析表) |
| N9b | `ext_cached` 改 `eq_ignore_ascii_case`(免每请求 `to_lowercase` 分配) |
| N14 | `RH_QUIC_CC` env 读 OnceLock 化(Endpoint 复用半边仍留档) |
| N19 | web_jwt_secret 改 `Arc<String>`(require_auth 每请求整串 clone → 引用计数) |

## 留档缓修(需要基准或重设计,勿当遗漏反复重提)

- **N3 客户端 UDP 上行每包 1 分配 + 2 拷贝**(`udpchan.rs`,`udp_packet::encode` 新 Vec → writer 再拷进 batch)。
  B-1 只修了服务端半边。根治需 Pending 携带已成帧 `Bytes` + writer 走 writev/chunk 路径
  (mux 载体可复用 `steal_mux_halves` 零拷贝),属 udpchan 写侧重设计,**必须配三机 UDP 基准**。
- **N9 残余**:path.to_string(缓存键固有)、basic-auth 每请求 format!(仅配了认证的路由,µs 级)。
  预解析与 ext 免分配已在二批落地。
- ~~N6~~ 二批已修。
- **N10 udpchan mux 载体下 `tokio::io::split` 同步锁**:锁内纯内存操作,量级远小于服务端 splice
  路径当年实测的 futex;与 N3 一起修最划算。
- **N11 KCP 读路径逐字节多次 memcpy**(`read_buf` VecDeque):改 BytesMut 让 `kcp.recv` 直写,
  收益需 KCP 基准证明。
- **N13 KCP drain 任务空闲 500ms 唤醒**:4096 会话 = 8192 唤醒/s;改法是生产者存活时不 arm sleep,
  属唤醒合并优化,基准观测到 CPU 占比再动。
- **N14 残余**:quic Endpoint 按 bind 族复用 —— 改变 socket 生命周期,quic 非默认协议,留档。
- **N15 mux 泵预取 future 被 drop 时,已弹出缓冲绕过池释放**:稀释池命中率,非泄漏;
  修法(取消安全归还)复杂度高于收益。
- **N17/N18**(CONNECT head 拷贝 / 静态文件元数据 syscall):低频或纳秒级,维持不动。
  (N19 已在二批落地。)
- **服务端 UDP 泵单任务双向**(任一方向写阻塞拖停另一方向):B-1 当时已注明「写任务拆分单独评估」,
  仍开放,拆分时需基准验证半队列语义。

## 教训(写给下一轮审计的自己和代理)

1. 「连续 N 轮零发现」只证明**扫描口径内**收敛,不证明没有问题。本轮 19 条里有 4 条
   (N1/N4/N16/N12)是此前 12 轮的**真漏报**——代理多次"读过"同一文件却没查具体维度
   (vectored 能力位、每事件放大形态、注释与代码一致性)。
2. 「有界/架构固有」类排除理由要留具体反证;N2/N3 的排除在更强的论证下不成立。
3. 排除清单会随修复演进失效,每轮应抽查而非整表继承。
