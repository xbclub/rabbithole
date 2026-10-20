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
    if fwd.proxy_proto != rh_protocol::message::ProxyProto::None {
        let info = ProxyProtoInfo {
            src: fwd
                .user_src
                .parse()
                .unwrap_or_else(|_| "127.0.0.1:0".parse().unwrap()),
            dst: "127.0.0.1:0".parse().unwrap(),
        };
        let hdr = match fwd.proxy_proto {
            rh_protocol::message::ProxyProto::V1 => proxyproto::encode_v1(&info),
            rh_protocol::message::ProxyProto::V2 => proxyproto::encode_v2(&info),
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
            rh_core::bandwidth::BandwidthQuantity::parse(limit).map(|q| {
                Arc::new(rh_core::ratelimit::TokenBucket::new(
                    q.bytes_per_sec() as f64
                ))
            })
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
pub async fn dial_target(
    _sess: &Arc<ClientSession>,
    spec: &ProxySpec,
    fwd: &StartForward,
) -> anyhow::Result<BoxedIo> {
    if let Some(plugin) = &spec.plugin {
        return crate::plugins::dial_plugin(plugin, fwd).await;
    }
    let addr = resolve(&fwd.target_host, fwd.target_port).await?;
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
    if let Ok(ip) = host.parse::<std::net::IpAddr>() {
        return Ok(std::net::SocketAddr::new(ip, port));
    }
    // 自定义 DNS(配置了 dns_server 时):直接向该服务器发 A/AAAA 查询
    if let Some(ns) = crate::session::dns_server() {
        if let Some(ip) = crate::dnsquery::query_spawn_blocking(host, &ns).await {
            return Ok(std::net::SocketAddr::new(ip, port));
        }
    }
    let host_owned = host.to_string();
    let resolved = tokio::task::spawn_blocking(move || {
        use std::net::ToSocketAddrs;
        (host_owned.as_str(), port)
            .to_socket_addrs()
            .ok()
            .and_then(|mut it| it.next())
    })
    .await
    .map_err(|e| anyhow::anyhow!("resolve task: {e}"))?;
    resolved.ok_or_else(|| anyhow::anyhow!("resolve {host}:{port} failed"))
}

/// 错误路径收尾等待上限(final-gaps,与服务端 data.rs CANCEL_CLOSE_GRACE
/// 同值):IO 错误(非 EOF)后对向通路已坏,等完整 shutdown 无意义;
/// poll_shutdown 永久 Pending 时不得扣住 IO。clean EOF 不受此限(红线)。
const ERROR_CLOSE_GRACE: std::time::Duration = std::time::Duration::from_secs(1);

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
    let (mut ar, mut aw) = tokio::io::split(a);
    let (mut br, mut bw) = tokio::io::split(b);
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
}
