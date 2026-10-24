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

## 三批落地(「能修吗」复评)

| 项 | 内容 |
|---|---|
| N9 残余① | VhostRoute 只存预拼接的 `http_credentials: Option<String>`(user/password 两字段删除,启用判定看 None;修前每请求 format!) |
| N9 残余② | `path` 改 `&str` 借用(前缀匹配/扩展名判定/缓存键拼接都不需要所有权) |
| N14 残余 | quic 客户端 Endpoint 按 (bind 族, runtime Id) 缓存复用,配置经 `connect_with` 每次携带;键含 runtime Id 是因为 driver 任务绑定创建它的 runtime(按进程全局缓存会让多 runtime 环境握手死锁,测试已抓到) |
| N17 | CONNECT 解析的 `from_utf8_lossy` 改借用(合法 UTF-8 零分配;修前 into_owned 再整块拷一次) |
| N13 | 核实为**有意设计**不改:空闲复查是 10ms 起指数退避封顶 500ms 的兜底(生产者活跃时 notify 即时唤醒),每次唤醒 µs 级,4096 空闲会话合计 <1% 单核 |
| N11 | 核实后**维持不动**:VecDeque→BytesMut 直写只省一次 memcpy(仍需 memset 预填),类型改写波及全部读端逻辑,收益/风险不成比 |

## 留档缓修(需要基准或重设计,勿当遗漏反复重提)

- **N3 客户端 UDP 上行每包 1 分配 + 2 拷贝**(`udpchan.rs`,`udp_packet::encode` 新 Vec → writer 再拷进 batch)。
  B-1 只修了服务端半边。根治需 Pending 携带已成帧 `Bytes` + writer 走 writev/chunk 路径
  (mux 载体可复用 `steal_mux_halves` 零拷贝),属 udpchan 写侧重设计,**必须配三机 UDP 基准**。
- ~~N6/N9/N17~~ 已修(二/三批);~~N13/N11~~ 核实后判定不动(见上)。
- **N10 udpchan mux 载体下 `tokio::io::split` 同步锁**:锁内纯内存操作,量级远小于服务端 splice
  路径当年实测的 futex;与 N3 一起修最划算。
- **N11 KCP 读路径逐字节多次 memcpy**(`read_buf` VecDeque):改 BytesMut 让 `kcp.recv` 直写,
  收益需 KCP 基准证明。
- **N13 KCP drain 任务空闲 500ms 唤醒**:4096 会话 = 8192 唤醒/s;改法是生产者存活时不 arm sleep,
  属唤醒合并优化,基准观测到 CPU 占比再动。

- **N15 mux 泵预取 future 被 drop 时,已弹出缓冲绕过池释放**:稀释池命中率,非泄漏;
  修法(取消安全归还)复杂度高于收益。
- **N18**(静态文件元数据 syscall):冷门插件路径;正确性要求每请求 stat 校验 mtime,可省的只有重复调用,维持不动。
- **服务端 UDP 泵单任务双向**(任一方向写阻塞拖停另一方向):B-1 当时已注明「写任务拆分单独评估」,
  仍开放,拆分时需基准验证半队列语义。

## 教训(写给下一轮审计的自己和代理)

1. 「连续 N 轮零发现」只证明**扫描口径内**收敛,不证明没有问题。本轮 19 条里有 4 条
   (N1/N4/N16/N12)是此前 12 轮的**真漏报**——代理多次"读过"同一文件却没查具体维度
   (vectored 能力位、每事件放大形态、注释与代码一致性)。
2. 「有界/架构固有」类排除理由要留具体反证;N2/N3 的排除在更强的论证下不成立。
3. 排除清单会随修复演进失效,每轮应抽查而非整表继承。
