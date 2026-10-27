//! START_FORWARD 处理:拨号本地目标或插件、压缩、PROXY protocol、带宽限制、
//! tcpmux passthrough、半关闭。

use crate::session::ClientSession;
use rh_core::proxyproto::{self, ProxyProtoInfo};
use rh_protocol::message::{forward_flags, StartForward};
use rh_protocol::ProxySpec;
use rh_transport::BoxedIo;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// TCP 语义数据通道:按 START_FORWARD 自包含指派桥接本地。
pub async fn handle_start_forward(
    sess: Arc<ClientSession>,
    channel: BoxedIo,
    spec: &ProxySpec,
    fwd: StartForward,
    prefix: Vec<u8>,
) -> anyhow::Result<()> {
    tracing::debug!(proxy = %fwd.proxy_name, target = %fwd.target_host, port = fwd.target_port, "forward assigned");
    // 指派帧后已抵达的用户数据:前转到本地目标(先于其他写入)
    let prefix = (!prefix.is_empty()).then_some(prefix);
    // tcpmux passthrough:通道本体是 yamux 服务端,接受多路流,每流独立转发。
    // 残留字节属于 yamux 线路字节(或压缩流首块),必须回灌进通道,而非写进任何一条流
    if fwd.flags & forward_flags::TCPMUX_PASSTHROUGH != 0 {
        let channel = match prefix {
            Some(p) => rh_transport::PrependedIo::wrap(channel, p),
            None => channel,
        };
        let channel = if fwd.flags & forward_flags::COMPRESSED != 0 {
            rh_transport::compress_stream(channel).await
        } else {
            channel
        };
        // 客户端侧终结打洞多路流:取不限制(策略上限在服务端)
        let mux =
            rh_transport::wrap_stream_as_server(channel, true, rh_transport::MUX_STREAMS_UNLIMITED);
        let spec = spec.clone();
        let fwd = fwd.clone();
        let sess = sess.clone();
        tokio::spawn(async move {
            while let Ok(stream) = mux.accept().await {
                let spec2 = spec.clone();
                let fwd2 = fwd.clone();
                let s2 = sess.clone();
                tokio::spawn(async move {
                    if let Ok(local) = dial_target(&s2, &spec2, &fwd2).await {
                        splice(stream, local, None).await;
                    }
                });
            }
        });
        return Ok(());
    }

    let mut local = dial_target(&sess, spec, &fwd).await?;

    // PROXY protocol 头(版本以 START_FORWARD 显式指派为准)
    //
    // ★源地址解析失败时必须**放弃发 PROXY 头并记录**,不能回落
    // `127.0.0.1:0`。修前那样做会让上游看到"来自本机"—— 若有"仅信任
    // loopback"的本地 ACL,等于把任意远端用户伪造成本机。
    // 不发 PROXY 头时上游看到的是真实的对端地址(隧道出口),比伪造安全。
    if fwd.proxy_proto != rh_protocol::message::ProxyProto::None {
        // 解析失败 → 不写 PROXY 头,继续正常转发(见下方注释)。
        let src = fwd.user_src.parse::<std::net::SocketAddr>().ok();
        if src.is_none() {
            tracing::warn!(
                proxy = %fwd.proxy_name,
                user_src = %fwd.user_src,
                "PROXY protocol: user_src 非法,跳过 PROXY 头(不伪造本机地址)"
            );
        }
        let info = src.map(|src| ProxyProtoInfo {
            src,
            dst: "127.0.0.1:0".parse().unwrap(),
        });
        let hdr = match (&info, fwd.proxy_proto) {
            (Some(info), rh_protocol::message::ProxyProto::V1) => proxyproto::encode_v1(info),
            (Some(info), rh_protocol::message::ProxyProto::V2) => proxyproto::encode_v2(info),
            // 源地址不可用:不发 PROXY 头(见上)。上游看到的是真实隧道出口地址,
            // 不会把远端用户伪造成本机。
            _ => Vec::new(),
        };
        if !hdr.is_empty() && local.write_all(&hdr).await.is_err() {
            return Ok(());
        }
    }

    // 压缩:端到端 snappy,两端各自 compress_stream。
    // 残留字节:未压缩时直接前转到本地;压缩时是压缩流首块,须喂给解压器
    let peer = if fwd.flags & forward_flags::COMPRESSED != 0 {
        let channel = match prefix {
            Some(p) => rh_transport::PrependedIo::wrap(channel, p),
            None => channel,
        };
        rh_transport::compress_stream(channel).await
    } else {
        if let Some(p) = &prefix {
            use tokio::io::AsyncWriteExt as _;
            if local.write_all(p).await.is_err() {
                return Ok(());
            }
        }
        channel
    };

    // 带宽(客户端模式)
    #[allow(unused_mut)]
    let mut peer = peer;
    let bucket = match (
        &spec.transport.bandwidth_limit,
        spec.transport.bandwidth_mode,
    ) {
        (Some(limit), rh_protocol::BandwidthMode::Client) => {
            // ★解析失败必须告警,不能静默关闭限速。
            //
            // 修前 `.map(...)` 直接得到 None:配置写错(如 "100Mbps" 用了
            // 不支持的写法)时**限速完全不生效**,而运维以为已经限了。
            // 这是"配了没生效"里最难发现的一类 —— 没有任何信号。
            match rh_core::bandwidth::BandwidthQuantity::parse(limit) {
                Some(q) => Some(Arc::new(rh_core::ratelimit::TokenBucket::new(
                    q.bytes_per_sec() as f64,
                ))),
                None => {
                    tracing::warn!(
                        proxy = %spec.name,
                        limit = %limit,
                        "带宽限速配置无法解析,本规则限速**未生效**(检查 bandwidth_limit 格式)"
                    );
                    None
                }
            }
        }
        _ => None,
    };

    // Y1 零拷贝 mux 中继:peer 为未包装 RhMuxStream(mux 模式常态)时,
    // 数据以 Bytes 引用直达帧体/内核,消除两次中间拷贝。
    // 压缩/残留包装时不匹配,回落下方路径;客户端无规则级联取消。
    // B7(与 data.rs assign_forward 同式):relay_mode=copy 是同二进制 A/B
    // 开关——copy 对照轮同样关闭 chunk 快路径,避免污染 copy 对照臂
    //(benchmark/README.md:223 承诺;修前客户端只关了 splice 分支)。
    if client_chunk_gate(
        fwd.flags & forward_flags::COMPRESSED == 0,
        &sess.cfg.relay_mode,
    ) && rh_transport::mux::is_mux_stream(&peer)
    {
        let mut p = peer;
        if let Some((b_r, b_w)) = rh_transport::mux::steal_mux_halves(&mut p) {
            drop(p); // 壳(halves 已偷出)
            let limiter = mk_client_limiter(&bucket);
            rh_transport::mux::mux_relay(local, b_r, b_w, limiter, None).await;
            return Ok(());
        }
        peer = p; // 理论不可达;保底回落
    }

    // 零拷贝快路径:通道与本地目标均为裸 TCP(裸模式明文载体 + 直连 TCP 目标)
    // 时走内核 splice;计量/限速/半关闭语义与下方用户态路径一致,失败回落。
    #[cfg(target_os = "linux")]
    {
        if sess.cfg.relay_mode != "copy" && fwd.flags & forward_flags::COMPRESSED == 0 {
            let peer_tcp = rh_transport::splice::plain_tcp(&peer);
            let local_tcp = rh_transport::splice::plain_tcp(&local);
            if let (Some(p), Some(l)) = (peer_tcp, local_tcp) {
                let dir = rh_transport::splice::DirHook {
                    on_bytes: None,
                    // 无计量回调(客户端不经 CountedUserIo),口径位无效值
                    meter_on_fill: false,
                    limiter: mk_client_limiter(&bucket),
                };
                let hooks = rh_transport::splice::SpliceHooks {
                    up: Some(dir.clone()),
                    down: Some(dir),
                    abort: None,
                    // 客户端无规则级联取消:连接随 carrier 关闭而结束
                    cancel: None,
                };
                if rh_transport::splice::splice_relay(p, l, hooks)
                    .await
                    .is_ok()
                {
                    return Ok(());
                }
            }
        }
    }

    splice(peer, local, bucket).await;
    Ok(())
}

/// 客户端限速适配(goal-repair A5:预留式取消安全)。
/// 与服务端 data.rs `mk_limiter` 同构,但客户端只有一个桶
/// (bandwidth_mode=Client):reserve → [`rh_core::ratelimit::Reservation`],
/// 消费不可退、drop 未消费即退(三态合同)。双方向共用同一实例。
struct ClientBucketLimiter(Arc<rh_core::ratelimit::TokenBucket>);

struct ClientReservation(rh_core::ratelimit::Reservation);

impl rh_transport::ChunkReservation for ClientReservation {
    fn consume(&mut self, n: usize) {
        self.0.consume(n);
    }
}

impl rh_transport::ChunkLimiter for ClientBucketLimiter {
    fn reserve(
        &self,
        n: usize,
    ) -> futures::future::BoxFuture<'static, Box<dyn rh_transport::ChunkReservation>> {
        let b = self.0.clone();
        Box::pin(async move {
            let r = b.acquire_reserved(n).await;
            Box::new(ClientReservation(r)) as Box<dyn rh_transport::ChunkReservation>
        })
    }
}

/// B7:客户端 mux chunk 快路径资格(与 data.rs assign_forward 判定同式):
/// relay_mode=copy 是同二进制 A/B 开关,copy 对照轮同样关闭 chunk 快路径,
/// 避免污染 copy 对照臂(benchmark/README.md:223;修前客户端只关了
/// Linux splice 分支)。测试直接锁本函数,防判定与实现漂移。
fn client_chunk_gate(use_compression_flag_clear: bool, relay_mode: &str) -> bool {
    use_compression_flag_clear && relay_mode != "copy"
}

/// 中继一条「通道 ↔ 本地 IO」:能走零拷贝 mux chunk 快路径就走,否则回落
/// copy(与 [`splice`] 等价)。
///
/// 这是 [`handle_start_forward`] 里那段快路径判定的**可复用外形**:
/// * `peer` 是未包装 `RhMuxStream`、且 `relay_mode != "copy"` → 偷出双半,
///   交 [`rh_transport::mux::mux_relay`] 按 `Bytes` 引用搬运;
/// * 任何包装(压缩流 / `PrependedIo`)或 `steal` 失败 → 原样回落
///   [`splice`]。压缩流不是 `RhMuxStream`,`is_mux_stream` 恒 false,
///   所以这里**不需要**再单独判压缩位——判定与实现不脱钩。
///
/// 用于没有规则级联取消的 visitor 路径,故 `cancel` 恒 `None`。
pub async fn relay_stream(peer: BoxedIo, local: BoxedIo, relay_mode: &str) {
    if relay_mode != "copy" && rh_transport::mux::is_mux_stream(&peer) {
        let mut p = peer;
        if let Some((b_r, b_w)) = rh_transport::mux::steal_mux_halves(&mut p) {
            drop(p); // 壳(halves 已偷出)
            #[cfg(test)]
            relay_path_probe::note(relay_path_probe::Path::Chunk);
            rh_transport::mux::mux_relay(local, b_r, b_w, None, None).await;
            return;
        }
        // steal 失败(理论不可达):壳仍完整,回落 copy
        #[cfg(test)]
        relay_path_probe::note(relay_path_probe::Path::Copy);
        splice(p, local, None).await;
        return;
    }
    #[cfg(test)]
    relay_path_probe::note(relay_path_probe::Path::Copy);
    splice(peer, local, None).await;
}

/// 测试用路径探针:让「走了 chunk 还是 copy」成为可断言的事实。
///
/// 必要性:`mux_relay` 与 `splice` 语义等价(双向搬运 + 半关闭),纯功能
/// 测试**无法**区分两条路径——把快路径关掉测试照样全绿(实测过)。所以
/// 验收必须直接锁路径,否则「改了但没生效」会静默通过。
#[cfg(test)]
pub(crate) mod relay_path_probe {
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Clone, Copy, PartialEq, Eq, Debug)]
    pub enum Path {
        Chunk,
        Copy,
    }

    static CHUNK: AtomicUsize = AtomicUsize::new(0);
    static COPY: AtomicUsize = AtomicUsize::new(0);
    /// 串行化「基线 → 运行 → 断言」整个用例。
    ///
    /// 必须是 **async 锁**:同步 `MutexGuard` 跨 await 持有会阻塞整个 OS
    /// 线程(clippy `await_holding_lock` 报错);而用「取基线即放锁」的短
    /// 临界区,并行测试会在窗口内互相计数(实测 chunk=1/copy=2 交叉污染)。
    pub static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    pub fn note(p: Path) {
        match p {
            Path::Chunk => CHUNK.fetch_add(1, Ordering::SeqCst),
            Path::Copy => COPY.fetch_add(1, Ordering::SeqCst),
        };
    }

    /// (chunk 次数, copy 次数)
    pub fn snapshot() -> (usize, usize) {
        (CHUNK.load(Ordering::SeqCst), COPY.load(Ordering::SeqCst))
    }

    /// 进入串行窗口:整个用例期间持有,结束随 guard drop 释放。
    ///
    /// 这是 async 锁,跨 await 持有是安全的(只是让其他用例如期等待),
    /// 也正是它保证了「基线 → 运行 → 断言」期间没有别的用例计数。
    pub async fn serial() -> tokio::sync::MutexGuard<'static, ()> {
        SERIAL.lock().await
    }

    /// 相对基线的增量((chunk, copy))。调用方须已持有 `serial()` 窗口。
    pub fn since(base: (usize, usize)) -> (usize, usize) {
        let now = snapshot();
        (now.0 - base.0, now.1 - base.1)
    }
}

/// 装配客户端限速器:无桶(未配限速/非 Client 模式)返回 None,
/// chunk/splice 快路径零热路径开销。
fn mk_client_limiter(
    bucket: &Option<Arc<rh_core::ratelimit::TokenBucket>>,
) -> Option<Arc<dyn rh_transport::ChunkLimiter>> {
    bucket
        .clone()
        .map(|b| Arc::new(ClientBucketLimiter(b)) as Arc<dyn rh_transport::ChunkLimiter>)
}

/// 拨号本地目标(插件优先)。
///
/// # 目标来源:`fwd`(线路下发)权威,`spec` 只做兜底
///
/// 常规中继的目标以**线路下发的 `fwd.target_host/port`** 为准。
///
/// ## 为什么不能用 `spec`(`25b7505` 引入的回归)
///
/// `spec` 是客户端 `proxies` 表里的副本,而它被读取的时机在
/// `session.rs::open_data_channel` —— **数据通道建立的那一刻**,远早于该通道
/// 真正承载用户连接的时刻(TCP 池预热通道尤其如此:池里的通道在规则创建时
/// 就建好了,可以静置到几分钟后被取用)。
///
/// 于是"同端口只改 target"这类原位发布会出现:
/// 1. 服务端 `publish_spec_in_place` 更新 `rt.spec` 并下发 `ServiceUpdate`;
/// 2. 池里那 2 条**预热通道**早已带着旧 `spec` 快照在 `dial_target` 的调用栈里
///    等待 `START_FORWARD`;
/// 3. 用户连接落到这些通道上 → 拨的是**旧 target**;要等池被消耗一遍、新签发的
///    通道才带上新快照。
///
/// 实测(HEAD,池大小 2):改 target 后前 **2** 次连接仍打旧目标,第 3 次起才切。
/// 对照 `90f6f6b`(用 `fwd`)第 1 次即切换;把本函数临时改回 `fwd` 亦第 1 次即切换
/// —— 见 `docs/concurrency-serialization-fixes.md` §13.3。
///
/// `fwd` 是服务端 `data.rs::assign_forward` 在**指派这一条通道的那一刻**从
/// `rt.spec` 读出的,与规则当前状态同步;`StartForward` 的定义即
/// 「指派信号:目标自包含」(`rh-protocol/src/message.rs`)。
///
/// ## 这为什么不构成 SSRF 面的回退
///
/// 引入 `spec` 的那次提交的理由是"被攻破的 server 可让 client 拨内网任意地址"。
/// 但 `ProxySpec` 本身**完全由服务端 `ServiceSync` 下发**(客户端 `ClientConfig`
/// 里没有本地规则字段),`spec.target_host` 与 `fwd.target_host` **同为服务端可控**。
/// 在"服务端被攻破"的威胁模型下,两者改与不改都拦不住 —— 信任边界是**服务端本身
/// 可信**(客户端用 secret / TLS pin / CA 认证服务端身份)。这一点 `25b7505` 之后
/// 的 `81cc9eb` 已经如实写明("这不是 SSRF 修复")。
///
/// 用的是同一个服务端、同一份规则,只是取「指派时刻的权威值」而不是「建通道时刻的
/// 陈旧副本」—— 不引入任何新的信任假设。
///
/// ## 保底与插件分支
///
/// `fwd.target_host` 为空时回落到 `spec`(旧端服务端可能不下发该字段,
/// `serde(default)` 会得到空串;此时行为与修前一致,不会把连接打到空地址)。
///
/// 插件分支**不读** `fwd`:所有插件的目标都来自 `spec.plugin` 自身
/// (`local_addr`/`unix_path`),`http_proxy`/`socks5` 的目的地更是来自
/// 用户在代理请求里写的地址。从前这里把 `fwd.target_host` 当成「本地代理
/// 地址」传给插件,才导致那两个插件自指(连代理 → CONNECT 代理自己)。
pub async fn dial_target(
    _sess: &Arc<ClientSession>,
    spec: &ProxySpec,
    fwd: &StartForward,
) -> anyhow::Result<BoxedIo> {
    if let Some(plugin) = &spec.plugin {
        return crate::plugins::dial_plugin(plugin).await;
    }
    let (host, port) = if fwd.target_host.is_empty() {
        (spec.target_host.as_str(), spec.target_port)
    } else {
        (fwd.target_host.as_str(), fwd.target_port)
    };
    let addr = resolve(host, port).await?;
    let stream = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        tokio::net::TcpStream::connect(addr),
    )
    .await
    .map_err(|_| anyhow::anyhow!("dial {} timeout", addr))??;
    stream.set_nodelay(true).ok();
    Ok(Box::new(stream))
}

/// 目标解析:IP 直返;自定义 DNS 走 blocking 线程;系统解析(getaddrinfo)
/// 同样移出 async 线程池(DNS 不可达时每连接可阻塞 2s+,拖垮 runtime 调度)。
pub async fn resolve(host: &str, port: u16) -> anyhow::Result<std::net::SocketAddr> {
    // 红线:IP 直连快路径 —— 不查缓存、不解析、不进 spawn_blocking。
    if let Ok(ip) = host.parse::<std::net::IpAddr>() {
        return Ok(std::net::SocketAddr::new(ip, port));
    }
    // 域名目标:TTL 缓存 + 并发去重(C9)。
    //
    // 修前每次调用都要走一趟 `spawn_blocking`(自定义 DNS 还要等最多 2s
    // 超时),短连接场景下等于每请求一次。现在同 (host,port,dns_server)
    // 在 TTL 内直接命中;并发请求合并为一次真实解析。
    crate::dnsquery::resolve_cached(host, port, crate::session::dns_server())
        .await
        .ok_or_else(|| anyhow::anyhow!("resolve {host}:{port} failed"))
}

/// 系统解析(getaddrinfo),在 blocking 线程执行。
///
/// 抽出来供 `dnsquery::default_query` 复用,避免两处各写一份解析逻辑。
pub async fn system_resolve(host: &str, port: u16) -> Option<std::net::SocketAddr> {
    let host_owned = host.to_string();
    tokio::task::spawn_blocking(move || {
        use std::net::ToSocketAddrs;
        (host_owned.as_str(), port)
            .to_socket_addrs()
            .ok()
            .and_then(|mut it| it.next())
    })
    .await
    .ok()
    .flatten()
}

/// 错误路径收尾等待上限(final-gaps,与服务端 data.rs CANCEL_CLOSE_GRACE
/// 同值):IO 错误(非 EOF)后对向通路已坏,等完整 shutdown 无意义;
/// poll_shutdown 永久 Pending 时不得扣住 IO。clean EOF 不受此限(红线)。
const ERROR_CLOSE_GRACE: std::time::Duration = std::time::Duration::from_secs(1);

/// 无锁拆分命中计数(仅测试用):断言 splice 在裸 TCP 上确实走了
/// `split_boxed_io` 而非共享一把 Mutex 的 `tokio::io::split`。
#[cfg(test)]
static LOCKLESS_SPLITS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

tokio::task_local! {
    /// 探针作用域标记。**只有被它包裹的那次 splice 才计数。**
    ///
    /// 为什么不是全局开关:`splice` 在别的测试里也会跑(代理插件的端到端
    /// 用例就 splice 真实 TCP),全局开关会把它们的命中一起算进来 ——
    /// 实测就是这样 flaky 的(`left: 6, right: 0`)。task-local 天然按
    /// 「这次 future」隔离,并发用例互不可见。
    static SPLIT_PROBE_SCOPE: ();
}

/// 采集点:不在作用域内时 `try_with` 返回 `Err`,一次分支即可,代价可忽略。
#[cfg(test)]
fn note_lockless_split() {
    let _ = SPLIT_PROBE_SCOPE.try_with(|_| {
        LOCKLESS_SPLITS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    });
}

/// 探针测试串行锁(见 [`LOCKLESS_SPLITS`])。用 tokio Mutex:守卫要跨
/// `await` 持有(std MutexGuard 跨 await 会被 clippy 判为错误)。
#[cfg(test)]
static SPLIT_PROBE_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// 双向搬运(保持半关闭:一边 EOF 只关闭另一边的写方向)。
/// A1(d):父任务取消(本 future 被 drop)→ 守卫 Drop → abort 双向子泵,
/// 不再随 await 移动句柄而 detach(与服务端 copy_relay_pair 同形态)。
/// final-gaps 补修:单向 IO 错误(非 panic/非 EOF)→ 有界收尾 + 立即
/// 终止对向泵(与 mux_relay/服务端 copy_relay_pair 对齐,修前对向可能
/// 永存、收尾 shutdown 永久 Pending 时 join 永不完成)。
/// round-52:clean EOF 的半关闭等待改走 [`rh_transport::join_relay_pumps`]
/// 的有界兜底(round-40 在 mux_relay/splice_relay 上做过、这里漏改)——
/// 对端永不回 FIN 时 socket 永久停 `FIN-WAIT-2`/`CLOSING`;等对向自然
/// 结束的语义保留,只是把上限交给 halfclose_grace(默认 30s)。
pub async fn splice(a: BoxedIo, b: BoxedIo, bucket: Option<Arc<rh_core::ratelimit::TokenBucket>>) {
    // round-69 同款:先试无锁拆分(裸 TcpStream → into_split,读写各一 fd、
    // 零共享锁),不支持时回落 `tokio::io::split`。
    //
    // 为什么必须换:`tokio::io::split` 的两半共享**一把 `std::sync::Mutex`**
    // (`tokio/src/io/split.rs` 的 `with_lock` 直接 `lock().unwrap()`),
    // poll_read/poll_write 整段在锁内。一端在内核态(`recvfrom`/`copy_user`)
    // 持锁时,另一端 `lock()` 会**阻塞整个 OS 线程**而不是让出。
    // 实测(mux_relay,上行 P=16、strace -c):futex 占 75.0% 的 syscall 时间
    // (39.85s/53.13s),同期 recvfrom 仅 10.9%、writev 8.1% —— 绝大部分时间
    // 在等锁而非收发。
    //
    // 这条路径承接:所有 use_compression=true 的规则、relay_mode=copy 的
    // A/B 对照、全部插件规则、全部 visitor 流量(stcp/xtcp)。
    let (mut ar, mut aw): (
        Box<dyn tokio::io::AsyncRead + Send + Unpin>,
        Box<dyn tokio::io::AsyncWrite + Send + Unpin>,
    ) = match rh_transport::split_boxed_io(a) {
        Ok((r, w)) => {
            #[cfg(test)]
            note_lockless_split();
            (Box::new(r), Box::new(w))
        }
        Err(io) => {
            let (r, w) = tokio::io::split(io);
            (Box::new(r), Box::new(w))
        }
    };
    let (mut br, mut bw): (
        Box<dyn tokio::io::AsyncRead + Send + Unpin>,
        Box<dyn tokio::io::AsyncWrite + Send + Unpin>,
    ) = match rh_transport::split_boxed_io(b) {
        Ok((r, w)) => {
            #[cfg(test)]
            note_lockless_split();
            (Box::new(r), Box::new(w))
        }
        Err(io) => {
            let (r, w) = tokio::io::split(io);
            (Box::new(r), Box::new(w))
        }
    };
    let bucket2 = bucket.clone();
    // 本向以错误(非 EOF)结束时置位:对向数据通路已死,不得永存
    let up_errored = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let dn_errored = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let up_err = up_errored.clone();
    let t1 = tokio::spawn(async move {
        let clean = copy_limited(&mut ar, &mut bw, bucket).await;
        if !clean {
            up_err.store(true, std::sync::atomic::Ordering::SeqCst);
        }
    });
    let dn_err = dn_errored.clone();
    let t2 = tokio::spawn(async move {
        let clean = copy_limited(&mut br, &mut aw, bucket2).await;
        if !clean {
            dn_err.store(true, std::sync::atomic::Ordering::SeqCst);
        }
    });
    let mut g1 = rh_transport::AbortOnDropHandle::new(t1);
    let mut g2 = rh_transport::AbortOnDropHandle::new(t2);
    // A1(d) + panic/错误收敛:父取消 → 守卫 abort 双向;先完成侧异常
    //(panic 或 IO 错误)→ 立即返回 abort 对向;双向 clean → 等各自结束。
    let first = tokio::select! {
        _ = &mut g1 => 1,
        _ = &mut g2 => 2,
    };
    let abnormal = match first {
        1 => g1.panicked() || up_errored.load(std::sync::atomic::Ordering::SeqCst),
        _ => g2.panicked() || dn_errored.load(std::sync::atomic::Ordering::SeqCst),
    };
    if abnormal {
        return;
    }
    // 半关闭有界兜底(round-52):修前此处是裸 `tokio::join!(g1, g2)`,
    // 对端永不回 FIN 时永久挂起——socket 钉死在 FIN-WAIT-2/CLOSING,
    // 流与 fd 都不释放(round-40 在客户端侧实测:10 轮 -P8 累积
    // 22 CLOSING + 22 FIN-WAIT-2、20s 后仍不消失、吞吐 2.4G→0.5G)。
    // join_relay_pumps 保留「clean EOF → 等对向自然结束」语义,只把无界
    // 等待换成 halfclose_grace 内的有界等待(默认 30s,
    // RH_HALFCLOSE_GRACE_SECS 可覆盖,0 = 旧行为)。
    //
    // 句柄以 `&mut` 传入(不再 move):返回后守卫仍在本地,Drop 即 abort
    // 未完成的子泵。本函数 join 之后不再读 g1/g2 的任何状态,故改造只
    // 涉及借用形式;abnormal 分支与 *_errored 置位逻辑逐字未动。
    rh_transport::join_relay_pumps(&mut g1, &mut g2).await;
}

/// 带限速的拷贝;EOF 时对写端做 shutdown(半关闭传递)。
/// 缓冲按需增长(读满即翻倍,上限 64KiB):批量流减少 syscall/唤醒次数,
/// 大量慢连接停留在小缓冲,内存不随并发数线性膨胀。
/// 返回 true = clean EOF(半关闭,shutdown 无界等待,红线);
/// false = 读/写 IO 错误(shutdown 受 ERROR_CLOSE_GRACE 约束)。
pub async fn copy_limited<R, W>(
    r: &mut R,
    w: &mut W,
    bucket: Option<Arc<rh_core::ratelimit::TokenBucket>>,
) -> bool
where
    R: tokio::io::AsyncRead + Unpin,
    W: tokio::io::AsyncWrite + Unpin,
{
    const MIN_BUF: usize = 16 * 1024;
    const MAX_BUF: usize = 64 * 1024;
    let mut buf = vec![0u8; MIN_BUF];
    let mut clean = true;
    loop {
        match r.read(&mut buf).await {
            Ok(0) => break,
            Err(_) => {
                clean = false;
                break;
            }
            Ok(n) => {
                if n == buf.len() && buf.len() < MAX_BUF {
                    buf.resize((buf.len() * 2).min(MAX_BUF), 0);
                }
                if let Some(b) = &bucket {
                    b.acquire(n).await;
                }
                if w.write_all(&buf[..n]).await.is_err() {
                    clean = false;
                    break;
                }
            }
        }
    }
    if clean {
        // clean EOF:无界等完整半关闭传递(红线,不加硬超时防截断)
        let _ = w.shutdown().await;
    } else {
        // 错误路径:有界收尾(IO 已坏,永久 Pending 的 shutdown 不得卡死)
        let _ = tokio::time::timeout(ERROR_CLOSE_GRACE, w.shutdown()).await;
    }
    clean
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};
    use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

    /// Drop 探针 IO(审计 src/main.rs 同款手法):子泵终止(所有权释放)时翻转。
    struct DropIo {
        inner: tokio::io::DuplexStream,
        dropped: Arc<AtomicBool>,
    }

    impl Drop for DropIo {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::SeqCst);
        }
    }

    impl AsyncRead for DropIo {
        fn poll_read(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::pin::Pin::new(&mut self.inner).poll_read(cx, buf)
        }
    }

    impl AsyncWrite for DropIo {
        fn poll_write(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
            buf: &[u8],
        ) -> std::task::Poll<std::io::Result<usize>> {
            std::pin::Pin::new(&mut self.inner).poll_write(cx, buf)
        }
        fn poll_flush(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::pin::Pin::new(&mut self.inner).poll_flush(cx)
        }
        fn poll_shutdown(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::pin::Pin::new(&mut self.inner).poll_shutdown(cx)
        }
    }

    async fn wait_flag(f: &Arc<AtomicBool>, what: &str) {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
        while !f.load(Ordering::SeqCst) {
            if tokio::time::Instant::now() >= deadline {
                panic!("{what}: 探针在 2s 内未翻转(copy 子泵未随父取消终止)");
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    }

    // A1(d):客户端 copy 路径(splice() 的 t1.await/t2.await)——父任务取消
    // 必须终止双向子泵;修前句柄 move 进 await 状态,父取消即 detach。
    #[tokio::test]
    async fn client_copy_parent_cancel_kills_children() {
        let (mut a_peer, a_local) = tokio::io::duplex(4096);
        let (mut b_peer, b_local) = tokio::io::duplex(4096);

        let d_a = Arc::new(AtomicBool::new(false));
        let d_b = Arc::new(AtomicBool::new(false));
        let a: BoxedIo = Box::new(DropIo {
            inner: a_local,
            dropped: d_a.clone(),
        });
        let b: BoxedIo = Box::new(DropIo {
            inner: b_local,
            dropped: d_b.clone(),
        });

        let parent = tokio::spawn(splice(a, b, None));

        // 建立流:a 侧写入 → b 侧收到(双向 copy 在跑)
        a_peer.write_u8(1).await.unwrap();
        assert_eq!(b_peer.read_u8().await.unwrap(), 1);

        // 父任务取消
        parent.abort();
        assert!(parent.await.unwrap_err().is_cancelled());
        wait_flag(&d_a, "client copy a io").await;
        wait_flag(&d_b, "client copy b io").await;

        // 窗口期断言:取消后写入的字节不得再被转发。
        // 探针已翻转 ⇒ 中继侧 duplex 已释放 ⇒ 写入应 BrokenPipe
        // (比"写入成功但不被转发"更强的保证);防御性兼容写入成功路径。
        match a_peer.write_u8(9).await {
            Err(e) if e.kind() == std::io::ErrorKind::BrokenPipe => {}
            Err(e) => panic!("探测写异常: {e}"),
            Ok(()) => {
                match tokio::time::timeout(std::time::Duration::from_millis(300), b_peer.read_u8())
                    .await
                {
                    Err(_) => {}
                    Ok(Ok(v)) => panic!("取消后字节 {v} 仍被转发(copy 子泵 detach)"),
                    Ok(Err(e)) => panic!("读侧异常: {e}"),
                }
            }
        }
    }

    /// 注入 panic 的写 IO:poll_write 直接 panic(模拟适配层/自定义 IO bug)。
    struct PanicWriteIo {
        inner: tokio::io::DuplexStream,
        dropped: Arc<AtomicBool>,
    }

    impl Drop for PanicWriteIo {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::SeqCst);
        }
    }

    impl AsyncRead for PanicWriteIo {
        fn poll_read(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::pin::Pin::new(&mut self.inner).poll_read(cx, buf)
        }
    }

    impl AsyncWrite for PanicWriteIo {
        fn poll_write(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            _buf: &[u8],
        ) -> std::task::Poll<std::io::Result<usize>> {
            panic!("injected poll_write panic")
        }
        fn poll_flush(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::task::Poll::Ready(Ok(()))
        }
        fn poll_shutdown(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::task::Poll::Ready(Ok(()))
        }
    }

    // 故障注入:单向子泵 panic(b→a 方向写 a 侧)。守卫吸收 JoinError,
    // 但对向泵(t1,停在 a 读)不得永存——splice() 必须收敛并释放两侧 IO
    // (join_relay_pumps panic 收敛,与服务端 copy_relay_pair 同形态)。
    #[tokio::test]
    async fn client_copy_panic_contained() {
        let (_a_peer, a_local) = tokio::io::duplex(4096);
        let (mut b_peer, b_local) = tokio::io::duplex(4096);

        let d_a = Arc::new(AtomicBool::new(false));
        let d_b = Arc::new(AtomicBool::new(false));
        let a: BoxedIo = Box::new(PanicWriteIo {
            inner: a_local,
            dropped: d_a.clone(),
        });
        let b: BoxedIo = Box::new(DropIo {
            inner: b_local,
            dropped: d_b.clone(),
        });

        let mut parent = tokio::spawn(splice(a, b, None));
        // b 侧数据抵达 → t2(br→aw)写 a 侧 → 注入 panic;t1 停在 a 读
        b_peer.write_u8(2).await.unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(3), &mut parent)
            .await
            .expect("单向子泵 panic 后 splice 必须收敛(对向不得永存)")
            .unwrap();
        assert!(d_a.load(Ordering::SeqCst));
        assert!(d_b.load(Ordering::SeqCst));
    }

    /// 注入写错误 + 永久 Pending 收尾的 IO:poll_write 直接 Err,
    /// poll_shutdown 永久 Pending(错误路径有界收敛的故障形态)。
    struct WriteErrShutdownPendIo {
        inner: tokio::io::DuplexStream,
        dropped: Arc<AtomicBool>,
    }

    impl Drop for WriteErrShutdownPendIo {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::SeqCst);
        }
    }

    impl AsyncRead for WriteErrShutdownPendIo {
        fn poll_read(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::pin::Pin::new(&mut self.inner).poll_read(cx, buf)
        }
    }

    impl AsyncWrite for WriteErrShutdownPendIo {
        fn poll_write(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            _buf: &[u8],
        ) -> std::task::Poll<std::io::Result<usize>> {
            std::task::Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "injected write error",
            )))
        }
        fn poll_flush(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::task::Poll::Ready(Ok(()))
        }
        fn poll_shutdown(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::task::Poll::Pending
        }
    }

    // final-gaps(红→绿):客户端 copy 通道错误路径有界收敛——非 panic
    // IO 错误 + 收尾 shutdown 永久 Pending(客户端无取消信号概念)时,
    // splice()/copy_limited 必须收敛并释放两侧 IO;对向泵(停在 a 读)
    // 不得永存。修前:copy_limited 出错后 `w.shutdown().await` 无界等待
    // → t2 卡死 → join 永不完成(服务端 copy/mux 早已修过同款缺陷,
    // 客户端漏了)。clean EOF 半关闭不受影响(见既有双向流测试)。
    #[tokio::test]
    async fn client_copy_error_converges_when_shutdown_pends() {
        let (_a_peer_hold, a_local) = tokio::io::duplex(4096);
        let (mut b_peer, b_local) = tokio::io::duplex(4096);

        let d_a = Arc::new(AtomicBool::new(false));
        let d_b = Arc::new(AtomicBool::new(false));
        let a: BoxedIo = Box::new(WriteErrShutdownPendIo {
            inner: a_local,
            dropped: d_a.clone(),
        });
        let b: BoxedIo = Box::new(DropIo {
            inner: b_local,
            dropped: d_b.clone(),
        });

        let mut parent = tokio::spawn(splice(a, b, None));
        // b 侧数据抵达 → t2(br→aw)写 a 侧得 Err → 错误路径;t1 停在 a 读
        b_peer.write_u8(2).await.unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(3), &mut parent)
            .await
            .expect("客户端 copy 错误路径必须收敛(即使 poll_shutdown 永久 Pending)")
            .unwrap();
        assert!(d_a.load(Ordering::SeqCst), "错误路径必须释放 a 侧 IO");
        assert!(
            d_b.load(Ordering::SeqCst),
            "对向泵不得永存,b 侧 IO 必须释放"
        );
    }

    // B7(客户端侧,与 data.rs relay_mode_copy_disables_chunk_gate 同式):
    // handle_start_forward 的 mux chunk 快路径判定 = 未压缩 && relay_mode≠copy。
    // copy 对照轮必须同时关闭客户端 chunk 路径(修前只关了 Linux splice 分支,
    // 双端 copy 时客户端仍走 chunk,污染 A/B 对照臂)。
    #[test]
    fn relay_mode_copy_disables_client_chunk_gate() {
        // 直接锁真实判定函数(非镜像):实现漂移/门控回退立红
        assert!(client_chunk_gate(true, "splice"), "默认(splice)开 chunk");
        assert!(
            !client_chunk_gate(true, "copy"),
            "B7:copy 必须关客户端 chunk"
        );
        assert!(!client_chunk_gate(false, "splice"), "压缩恒回落 copy");
        assert!(!client_chunk_gate(false, "copy"));
    }

    /// 读半永远 Pending 的 IO:模拟「一向 clean EOF、对向永不结束」。
    ///
    /// 写半正常转发(否则构造不出「一向 clean EOF」);只有读永久挂起,
    /// 即对端既不回数据也不回 FIN——round-40 实测里客户端侧 socket 钉在
    /// `FIN-WAIT-2`/`CLOSING` 的那种连接在应用层可见的表现。
    struct ReadPendIo {
        inner: tokio::io::DuplexStream,
        dropped: Arc<AtomicBool>,
    }

    impl Drop for ReadPendIo {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::SeqCst);
        }
    }

    impl AsyncRead for ReadPendIo {
        fn poll_read(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            _buf: &mut ReadBuf<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            // 对端永不回 FIN:既不 Ready(0) 也不 Ready(n),永久 Pending
            std::task::Poll::Pending
        }
    }

    impl AsyncWrite for ReadPendIo {
        fn poll_write(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
            buf: &[u8],
        ) -> std::task::Poll<std::io::Result<usize>> {
            std::pin::Pin::new(&mut self.inner).poll_write(cx, buf)
        }
        fn poll_flush(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::pin::Pin::new(&mut self.inner).poll_flush(cx)
        }
        fn poll_shutdown(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::pin::Pin::new(&mut self.inner).poll_shutdown(cx)
        }
    }

    /// round-52(红):客户端 copy 中继的 clean EOF 半关闭必须有界收敛。
    ///
    /// 修前 `splice()` 用裸 `tokio::join!(g1, g2)` 等对向——一条方向 clean
    /// EOF、对向永不结束(对端不回 FIN)时父任务永久挂起,两侧 IO 与子泵
    /// 都不释放。round-40 实测抓到的正是**客户端侧到本地目标**的
    /// `FIN-WAIT-2`/`CLOSING`(10 轮 -P8 累积 22+22、20s 后仍不消失、
    /// 吞吐 2.4G→0.5G),而当时只改了 `mux_relay`/`splice_relay`,漏了这里。
    ///
    /// 方向布局:a 侧(对端通道)普通流、对端 shutdown → t1 读得 clean EOF;
    /// b 侧用 [`ReadPendIo`](读半永久 Pending)→ t2 停在 b 读,永不结束。
    ///
    /// 断言:
    /// 1. 父任务在默认 30s grace 之后、45s 之前返回(既不永久挂起,
    ///    也没有提前打断正常半关闭);
    /// 2. 两侧 IO 探针都翻转(流/fd 真被释放,而非只是 future 结束)。
    ///
    /// `start_paused` 用虚拟时钟把 30s 压成瞬时(tokio test-util 在 worker
    /// 全部 idle 时自动推进到最近定时器),既不必真等半分钟,也不必设
    /// `RH_HALFCLOSE_GRACE_SECS`——那是进程级 OnceLock,改它会污染同进程
    /// 内的其它用例。
    #[tokio::test(start_paused = true)]
    async fn client_clean_eof_bounded_when_peer_never_finishes() {
        let (mut a_peer, a_local) = tokio::io::duplex(4096);
        let (_b_peer_hold, b_local) = tokio::io::duplex(4096);

        let d_a = Arc::new(AtomicBool::new(false));
        let d_b = Arc::new(AtomicBool::new(false));
        // a 侧:普通流(可正常读到 EOF)
        let a: BoxedIo = Box::new(DropIo {
            inner: a_local,
            dropped: d_a.clone(),
        });
        // b 侧:读半永久 Pending(对端既不回数据也不回 FIN)
        let b: BoxedIo = Box::new(ReadPendIo {
            inner: b_local,
            dropped: d_b.clone(),
        });

        let mut parent = tokio::spawn(splice(a, b, None));

        // a 写端关闭 → t1 读到 EOF(**clean EOF**,不置 up_errored);
        // t2 停在 b 读,b 对端永不回 FIN → 旧代码永挂于此
        a_peer.write_all(b"tail").await.unwrap();
        a_peer.shutdown().await.unwrap();

        tokio::time::timeout(std::time::Duration::from_secs(45), &mut parent)
            .await
            .expect(
                "clean EOF + 对向永不结束:客户端 splice() 必须在 halfclose_grace 内\
                 返回,不得永久挂起(round-40 泄漏红线,客户端 copy 路径漏改)",
            )
            .unwrap();

        assert!(
            d_a.load(Ordering::SeqCst),
            "半关闭兜底后必须释放 a 侧 IO(否则 socket 钉死 FIN-WAIT-2)"
        );
        assert!(d_b.load(Ordering::SeqCst), "半关闭兜底后必须释放 b 侧 IO");
    }

    /// round-52 对照(半关闭红线):正常半关闭**不得**被 grace 提前打断。
    ///
    /// 上一条锁定「有界」,这一条锁定「边界足够宽」:a 侧 clean EOF 后,
    /// 对向仍需把剩余数据完整送达(这里是 300ms 后才写完的一段数据)——
    /// 远小于默认 30s grace。若有人把 grace 改成短超时,这条会立刻变红。
    #[tokio::test(start_paused = true)]
    async fn client_clean_eof_still_delivers_late_peer_data() {
        let (mut a_peer, a_local) = tokio::io::duplex(4096);
        let (mut b_peer, b_local) = tokio::io::duplex(4096);

        let mut parent = tokio::spawn(splice(Box::new(a_local), Box::new(b_local), None));

        // a 侧 clean EOF(a 不再发,但仍在读——典型请求结束后等响应的形态)
        a_peer.shutdown().await.unwrap();
        // 对向延迟写好响应:必须穿过 grace 窗口完整送达
        let writer = tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(300)).await;
            b_peer.write_all(b"late-response-body").await.unwrap();
            b_peer.shutdown().await.unwrap();
        });

        let mut got = Vec::new();
        tokio::time::timeout(
            std::time::Duration::from_secs(20),
            a_peer.read_to_end(&mut got),
        )
        .await
        .expect("clean EOF 后的对向数据必须在 grace 内完整送达(不得被短超时截断)")
        .unwrap();
        assert_eq!(
            got, b"late-response-body",
            "半关闭兜底不得截断正常半关闭的数据"
        );
        writer.await.unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(45), &mut parent)
            .await
            .expect("数据送达后 relay 必须收敛")
            .unwrap();
    }

    // ------------------------------------------------------------------
    // 无锁拆分(round-69 同款):`tokio::io::split` 的两半共享一把
    // `std::sync::Mutex`,一端在内核态持锁时另一端会阻塞整个 OS 线程。
    // splice() 改为优先 `rh_transport::split_boxed_io`(裸 TcpStream →
    // into_split,读写各一 fd、零共享锁),包装类型回落旧路径。
    //
    // 以下两条测试用**真实 TcpStream**(而非 duplex —— duplex 不是
    // TcpStream,永远走回落分支,测不到目标路径)。
    // ------------------------------------------------------------------

    /// 起一对回环 TcpStream(连接已建立)。
    async fn tcp_pair() -> (tokio::net::TcpStream, tokio::net::TcpStream) {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap();
        let accept = tokio::spawn(async move { l.accept().await.unwrap().0 });
        let cli = tokio::net::TcpStream::connect(addr).await.unwrap();
        let srv = accept.await.unwrap();
        (cli, srv)
    }

    /// 核心验收:splice 两条真实 TcpStream,双向各传 1 MiB,内容必须完整。
    /// 同时断言**确实走了无锁分支**(探针计数增加)。
    #[tokio::test]
    async fn splice_lockless_split_bidirectional_1mib() {
        // 探针是进程级全局:与 splice_falls_back_for_non_tcp 串行,避免并行
        // 测试互相污染计数差值。
        let _serial = SPLIT_PROBE_LOCK.lock().await;
        let before = LOCKLESS_SPLITS.load(Ordering::Relaxed);
        const N: usize = 1024 * 1024;

        // 外侧两端(测试自己持有),中间两条交给 splice 对接。
        // 外侧各自 into_split:读写要在不同任务里并发进行。
        let (out_a, a) = tcp_pair().await; // out_a ↔ a
        let (mut out_a_r, mut out_a_w) = out_a.into_split();
        let (out_b, b) = tcp_pair().await; // out_b ↔ b
        let (mut out_b_r, mut out_b_w) = out_b.into_split();

        let relay =
            tokio::spawn(SPLIT_PROBE_SCOPE.scope((), splice(Box::new(a), Box::new(b), None)));
        // 无锁分支必须在 splice 起步后立刻命中两次(a、b 各一)
        for _ in 0..200 {
            if LOCKLESS_SPLITS.load(Ordering::Relaxed) >= before + 2 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        let after = LOCKLESS_SPLITS.load(Ordering::Relaxed);
        assert!(
            after >= before + 2,
            "splice 必须对两条裸 TcpStream 都走无锁拆分(实际命中 {} 次)",
            after - before
        );

        // 内容用可校验的模式填(避免全 0 掩盖截断/错位)
        let up: Vec<u8> = (0..N).map(|i| (i % 251) as u8).collect();
        let dn: Vec<u8> = (0..N).map(|i| ((i * 7 + 3) % 241) as u8).collect();

        // 双向并发:每一侧同时写自己那份、读对方那份,制造两个方向同时在途
        // (正是旧实现里两个 poll 争同一把 Mutex 的现场)。
        // a 侧写 up、读 dn;b 侧写 dn、读 up。
        let up_a = up.clone();
        let dn_b = dn.clone();
        let side_a = tokio::spawn(async move {
            let ((), r) = tokio::join!(
                async {
                    out_a_w.write_all(&up_a).await.unwrap();
                    out_a_w.shutdown().await.unwrap();
                },
                async {
                    let mut v = Vec::with_capacity(N);
                    out_a_r.read_to_end(&mut v).await.unwrap();
                    v
                }
            );
            r
        });
        let side_b = tokio::spawn(async move {
            let ((), r) = tokio::join!(
                async {
                    out_b_w.write_all(&dn_b).await.unwrap();
                    out_b_w.shutdown().await.unwrap();
                },
                async {
                    let mut v = Vec::with_capacity(N);
                    out_b_r.read_to_end(&mut v).await.unwrap();
                    v
                }
            );
            r
        });

        let got_dn = side_a.await.unwrap();
        let got_up = side_b.await.unwrap();

        assert_eq!(got_up.len(), N, "a→b 方向字节数必须完整");
        assert_eq!(got_dn.len(), N, "b→a 方向字节数必须完整");
        assert_eq!(got_up, up, "a→b 方向内容必须逐字节一致");
        assert_eq!(got_dn, dn, "b→a 方向内容必须逐字节一致");

        tokio::time::timeout(std::time::Duration::from_secs(10), relay)
            .await
            .expect("双向 clean EOF 后 splice 必须收敛")
            .unwrap();
    }

    /// 非 TcpStream(包装/内存流)必须安全回落到 `tokio::io::split`,
    /// 且功能不受影响 —— 无锁拆分是优化,不是功能前提。
    #[tokio::test]
    async fn splice_falls_back_for_non_tcp() {
        let _serial = SPLIT_PROBE_LOCK.lock().await;
        let (mut a_peer, a_local) = tokio::io::duplex(64 * 1024);
        let (mut b_peer, b_local) = tokio::io::duplex(64 * 1024);

        let before = LOCKLESS_SPLITS.load(Ordering::Relaxed);
        let relay = tokio::spawn(
            SPLIT_PROBE_SCOPE.scope((), splice(Box::new(a_local), Box::new(b_local), None)),
        );
        // duplex 不是 TcpStream:不得命中无锁分支
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert_eq!(
            LOCKLESS_SPLITS.load(Ordering::Relaxed),
            before,
            "duplex 必须回落到 tokio::io::split,不得误判为裸 TCP"
        );

        a_peer.write_all(b"ping").await.unwrap();
        let mut buf = [0u8; 4];
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            b_peer.read_exact(&mut buf),
        )
        .await
        .expect("回落路径必须仍能转发")
        .unwrap();
        assert_eq!(&buf, b"ping");

        b_peer.write_all(b"pong").await.unwrap();
        let mut buf2 = [0u8; 4];
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            a_peer.read_exact(&mut buf2),
        )
        .await
        .expect("回落路径反向也必须能转发")
        .unwrap();
        assert_eq!(&buf2, b"pong");

        // 收尾:两侧 shutdown 让 splice 自然结束
        drop(a_peer);
        drop(b_peer);
        tokio::time::timeout(std::time::Duration::from_secs(10), relay)
            .await
            .expect("回落路径也必须收敛")
            .unwrap();
    }

    // ------------------------------------------------------------------
    // 拨号目标来源(§13.3 回归)
    //
    // `25b7505` 把 `dial_target` 的目标从线路下发的 `fwd` 改为本地 `spec`
    // 副本。`spec` 是**数据通道建立时**的快照(池预热通道尤其陈旧),于是
    // "同端口只改 target"的原位发布后,池里的旧通道仍打旧目标。
    //
    // 下面这条用**两个真实监听器**判定拨号最终落在哪一边 —— 不是断言常量、
    // 不是断言"用了 fwd"这个实现细节,而是断言**流量到哪去了**,
    // 所以对实现方式中立(将来换任何取值方案,只要语义对就仍然通过)。
    // ------------------------------------------------------------------

    /// 最小 `ClientSession`:`dial_target` 不读它的任何字段(参数是 `_sess`),
    /// 故只构到能通过编译即可,避免依赖 session.rs 的测试辅助(跨模块)。
    fn bare_session() -> Arc<ClientSession> {
        let (tx, _rx) = tokio::sync::mpsc::channel::<bytes::Bytes>(1);
        Arc::new(ClientSession {
            cfg: Arc::new(crate::config::ClientConfig::default()),
            session_id: "s".into(),
            heartbeat_s: 30,
            send_tx: tx,
            cancel: tokio_util::sync::CancellationToken::new(),
            proxies: tokio::sync::Mutex::new(Default::default()),
            visitors: tokio::sync::Mutex::new(Default::default()),
            visitor_cancels: tokio::sync::Mutex::new(Default::default()),
            health_cancels: tokio::sync::Mutex::new(Default::default()),
            waiting: tokio::sync::Mutex::new(Default::default()),
            punched: tokio::sync::Mutex::new(Default::default()),
            nat_socks: tokio::sync::Mutex::new(Default::default()),
            punched_auth: tokio::sync::Mutex::new(Default::default()),
            sudp_deliver: tokio::sync::Mutex::new(Default::default()),
            entity_ops: tokio::sync::Mutex::new(()),
            mux: rh_transport::mux::wrap_stream_as_client(
                Box::new(tokio::io::duplex(4096).0) as rh_transport::BoxedIo,
                true,
                100,
            ),
            conn_counter: std::sync::atomic::AtomicU64::new(0),
        })
    }

    /// 起一个只回 `tag` 的监听器(收到任意字节即写 tag 并关闭)。
    async fn tag_listener(tag: &'static [u8]) -> (u16, tokio::task::JoinHandle<()>) {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = l.local_addr().unwrap().port();
        let h = tokio::spawn(async move {
            // 只接受一条连接足够:测试每次只拨一次
            if let Ok((mut s, _)) = l.accept().await {
                use tokio::io::AsyncWriteExt as _;
                let mut buf = [0u8; 16];
                let _ = tokio::io::AsyncReadExt::read(&mut s, &mut buf).await;
                let _ = s.write_all(tag).await;
                let _ = s.shutdown().await;
            }
        });
        (port, h)
    }

    fn spec_with_target(id: u64, host: &str, port: u16) -> ProxySpec {
        serde_json::from_value(serde_json::json!({
            "id": id,
            "name": "t",
            "kind": "tcp",
            "client_id": "c",
            "bind_ip": "127.0.0.1",
            "bind_port": 24900,
            "target_host": host,
            "target_port": port,
            "enabled": true,
        }))
        .unwrap()
    }

    fn fwd_to(id: u64, host: &str, port: u16) -> StartForward {
        StartForward {
            proxy_id: id,
            user_src: "127.0.0.1:1".into(),
            target_host: host.into(),
            target_port: port,
            flags: 0,
            proxy_proto: rh_protocol::message::ProxyProto::None,
            proxy_name: "t".into(),
            xtcp_auth: String::new(),
        }
    }

    /// `fwd` 与 `spec` 指向**不同**目标时,必须拨 `fwd`(服务端指派时刻的权威值)。
    ///
    /// `spec` 指向的端口是一个**已经关掉的**监听器(必然拒连),所以只要实现
    /// 仍在用 `spec`,这条测试就会失败(拨号错误),而不是"碰巧连上也拿到 tag"。
    #[tokio::test]
    async fn dial_target_prefers_fwd_over_stale_spec() {
        let spec_port = {
            let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            l.local_addr().unwrap().port()
        }; // 监听器在此 drop → 该端口无监听,拨它必失败
        let (fwd_port, fwd_srv) = tag_listener(b"AUTHORITATIVE").await;

        let spec = spec_with_target(1, "127.0.0.1", spec_port);
        let fwd = fwd_to(1, "127.0.0.1", fwd_port);

        let sess = bare_session();
        let mut io = dial_target(&sess, &spec, &fwd)
            .await
            .expect("必须拨 fwd 指定的目标;拨到 spec 的陈旧端口会连不上");
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
        io.write_all(b"ping").await.unwrap();
        let mut buf = Vec::new();
        let _ =
            tokio::time::timeout(std::time::Duration::from_secs(5), io.read_to_end(&mut buf)).await;
        assert_eq!(
            buf, b"AUTHORITATIVE",
            "拨号必须落在 START_FORWARD 指定的目标上,而不是通道建立时的 spec 副本"
        );
        fwd_srv.await.unwrap();
    }

    /// 反向:旧服务端不下发 target(`serde(default)` → 空串)时回落 `spec`,
    /// 不能把连接打到空地址。保证修法不会破坏兼容性。
    #[tokio::test]
    async fn dial_target_falls_back_to_spec_when_fwd_target_empty() {
        let (spec_port, spec_srv) = tag_listener(b"FROM-SPEC").await;
        let spec = spec_with_target(1, "127.0.0.1", spec_port);
        // 空 host = 旧端行为
        let fwd = fwd_to(1, "", 0);

        let sess = bare_session();
        let mut io = dial_target(&sess, &spec, &fwd)
            .await
            .expect("fwd 未下发 target 时必须回落 spec,而不是连空地址");
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
        io.write_all(b"ping").await.unwrap();
        let mut buf = Vec::new();
        let _ =
            tokio::time::timeout(std::time::Duration::from_secs(5), io.read_to_end(&mut buf)).await;
        assert_eq!(buf, b"FROM-SPEC");
        spec_srv.await.unwrap();
    }
}
