//! UDP 帧通道(代理侧 udp/sudp 与 visitor 侧 sudp 共用的 UDP_PACKET 帧模式)。
//!
//! # 写路径:唯一写任务 + 批聚合
//!
//! 代理侧曾经是"每个用户源地址一个回写任务,共享 `Arc<Mutex<Option<W>>>`":
//! 每收到一个包就 `w.lock().await` 再 `write_frame().await`,**锁跨 await
//! 持有**。后果是全部 UDP 用户会话共享一个写半 + 一把锁,任一慢写让所有
//! 会话队头阻塞,高 PPS 下锁争用直接吃 CPU。PING 分支更糟:为了拿 `&mut`
//! 先加锁、看一眼、释放、再加锁,中间留了一个窗口。
//!
//! 现在改为**唯一写任务**独占写半,其余一律 `tx.send(..)`:
//!
//! ```text
//!   回写任务(每会话) ─┐
//!   主循环 PING/PONG  ─┼─► mpsc(有界) ─► 写任务 ─► 写半
//!   主循环 PING(超时)─┘        └─ 凑够 BATCH_MAX 帧 或 channel 空时,
//!                                  多帧 encode 进同一个 BytesMut,
//!                                  一次 write_all 发出
//! ```
//!
//! 三个必须守住的点:
//! - **线格式不变**:仍是若干个**独立**的 UDP_PACKET 帧(`[len][addr][data]`),
//!   只是被拼进同一次 `write_all`。**不得**合并成"一帧多包"。
//! - **不许无界队列**:channel 有界,满了 `send().await` 阻塞生产者 = 背压。
//! - **低延迟**:批聚合只在"已经有帧在队列里"时贪心多取,绝不额外等待凑数。

use rh_protocol::message::udp_packet;
use rh_protocol::{frame, FrameType, ProxySpec};
use rh_transport::BoxedIo;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::AsyncWriteExt;
use tokio::net::UdpSocket;
use tokio::sync::mpsc;

/// N3:PING/PONG 载荷恒定,成帧一次进程内缓存(修前每事件 serde 序列化
/// + 每帧进批缓冲再拷)。
fn framed_ping() -> bytes::Bytes {
    static B: std::sync::OnceLock<bytes::Bytes> = std::sync::OnceLock::new();
    B.get_or_init(|| {
        frame::encode_frame_bytes(
            FrameType::PING,
            &serde_json::to_vec(&rh_protocol::message::Ping {
                ts_ms: 0,
                probe: true,
            })
            .unwrap(),
        )
    })
    .clone()
}
fn framed_pong() -> bytes::Bytes {
    static B: std::sync::OnceLock<bytes::Bytes> = std::sync::OnceLock::new();
    B.get_or_init(|| {
        frame::encode_frame_bytes(
            FrameType::PONG,
            &serde_json::to_vec(&rh_protocol::message::Pong {
                ts_ms: 0,
                probe: true,
            })
            .unwrap(),
        )
    })
    .clone()
}

/// N3:UDP 数据帧追加成帧(帧头 + [addr_len][addr][data] 直写)——
/// 供单包成帧与批收整批成帧共用,保证每包仍为独立标准 `UDP_PACKET` 帧。
fn append_udp_packet(dst: &mut bytes::BytesMut, addr: &str, data: &[u8]) {
    use bytes::BufMut as _;
    frame::encode_frame_header(dst, FrameType::UDP_PACKET, 1 + addr.len() + data.len());
    dst.put_u8(addr.len() as u8);
    dst.put_slice(addr.as_bytes());
    dst.put_slice(data);
}

/// N3:UDP 数据帧一次成帧(帧头 + [addr_len][addr][data] 直写)——
/// 1 分配 1 拷贝。修前 `udp_packet::encode` 新 Vec(拷贝 1)+ 写任务
/// 批聚合再整包拷(拷贝 2)。
fn frame_udp_packet(addr: &str, data: &[u8]) -> bytes::Bytes {
    let mut b = bytes::BytesMut::with_capacity(frame::HEADER_LEN + 1 + addr.len() + data.len());
    append_udp_packet(&mut b, addr, data);
    b.freeze()
}

/// 一次 `write_all` 最多聚合的帧数。
///
/// 32 帧 × (11B 头 + ~64B addr/data) ≈ 2.4 KiB,远小于 socket 缓冲区,
/// 不会因单次写过大而增加 flush 延迟;同时把 syscall 次数压到 1/32。
const BATCH_MAX: usize = 32;

/// 写队列容量(有界 = 背压)。
///
/// 1024 帧约 64 KiB 量级:足够吸收突发,又不至于让内存随会话数线性膨胀。
const WRITE_QUEUE: usize = 1024;

/// UDP 单报文的物理上限(IPv4/IPv6 载荷最大 65507,取 64 KiB 缓冲足够)。
const UDP_RECV_BUF: usize = 65536;

/// `UdpRecvBatch` 池上限:空闲会话不长期占用 2 MiB 批收缓冲,仅活跃发送时借用。
const RECV_BATCH_POOL_MAX: usize = 8;

/// 待写帧:**已成帧**的完整帧或同源多帧连续字节切片(每个元素均为完整独立帧,N3)。
struct Pending {
    framed: bytes::Bytes,
}

#[derive(Clone)]
struct UdpRecvBatchPool(Arc<std::sync::Mutex<Vec<rh_transport::udp_batch::UdpRecvBatch>>>);

struct PooledRecvBatch {
    pool: Arc<std::sync::Mutex<Vec<rh_transport::udp_batch::UdpRecvBatch>>>,
    batch: Option<rh_transport::udp_batch::UdpRecvBatch>,
}

impl UdpRecvBatchPool {
    fn new() -> Self {
        Self(Arc::new(std::sync::Mutex::new(Vec::new())))
    }

    fn acquire(&self) -> PooledRecvBatch {
        let batch = {
            let mut g = self.0.lock().unwrap_or_else(|e| e.into_inner());
            g.pop()
        }
        .unwrap_or_else(|| rh_transport::udp_batch::UdpRecvBatch::new(UDP_RECV_BUF));
        PooledRecvBatch {
            pool: self.0.clone(),
            batch: Some(batch),
        }
    }
}

impl std::ops::Deref for PooledRecvBatch {
    type Target = rh_transport::udp_batch::UdpRecvBatch;
    fn deref(&self) -> &Self::Target {
        self.batch.as_ref().expect("batch taken only on drop")
    }
}

impl std::ops::DerefMut for PooledRecvBatch {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.batch.as_mut().expect("batch taken only on drop")
    }
}

impl Drop for PooledRecvBatch {
    fn drop(&mut self) {
        if let Some(batch) = self.batch.take() {
            let mut g = self.pool.lock().unwrap_or_else(|e| e.into_inner());
            if g.len() < RECV_BATCH_POOL_MAX {
                g.push(batch);
            }
        }
    }
}

/// 构造读缓冲:hello 之后已抵达的字节(`prefix`)必须预置其中。
///
/// **两条载体路径共用这一个入口**,是修"prefix 静默丢失"的关键:
/// 修前 Chunks 路径在构造时并入 prefix、Stream 路径却另起一个空缓冲,
/// 而注释还写着"Stream 路径用 fbuf" —— 注释与代码不符,且一旦有调用方
/// 传非空 prefix,非 mux 载体就会**静默丢掉**隧道建立后最先到达的字节
/// (常是帧头),表现为偶发解析失败。
///
/// 抽成函数既统一了两条路径,也让"是否预置"这件事可被单测直接锁住
/// (直接测 `run_proxy_side` 需要真 mux/真 socket,构造成本高)。
fn read_buf_with_prefix(prefix: &[u8]) -> bytes::BytesMut {
    let mut b = bytes::BytesMut::with_capacity(prefix.len().max(64 * 1024));
    b.extend_from_slice(prefix);
    b
}

/// N10:通道读源。mux 偷出的读半无 AsyncRead 实现,走 chunk 组帧
/// (chunk 边界 ≠ 帧边界,对端可能一批多帧/一帧跨界);其余载体保持
/// 字节流 + `read_frame`。
enum ChanReader {
    Stream(Box<dyn tokio::io::AsyncRead + Send + Unpin>),
    Chunks {
        r: rh_transport::mux::RhMuxStreamRead,
        buf: bytes::BytesMut,
    },
}

impl ChanReader {
    async fn read_frame(
        &mut self,
        fbuf: &mut bytes::BytesMut,
    ) -> std::io::Result<(FrameType, bytes::Bytes)> {
        match self {
            Self::Stream(r) => frame::read_frame(r, fbuf).await.map_err(|e| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("udp channel frame: {e}"),
                )
            }),
            Self::Chunks { r, buf } => loop {
                match frame::decode_frame(buf) {
                    Ok(Some(f)) => return Ok(f),
                    Ok(None) => {}
                    Err(e) => {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!("udp channel frame: {e}"),
                        ))
                    }
                }
                match r.read_chunk().await? {
                    Some(c) => buf.extend_from_slice(&c),
                    None => {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::UnexpectedEof,
                            "udp channel eof",
                        ))
                    }
                }
            },
        }
    }

    /// 非阻塞排空下一帧(无就绪完整帧返回 `Ok(None)`,不挂起)。
    /// `Chunks` 优先解 `buf` 内余帧并尝试 `try_read_chunk`;`Stream` 先解
    /// `fbuf` 内已读字节,不足时以非阻塞 `poll_read` 尝试一次拉取。
    async fn try_read_frame(
        &mut self,
        fbuf: &mut bytes::BytesMut,
    ) -> std::io::Result<Option<(FrameType, bytes::Bytes)>> {
        match self {
            Self::Stream(r) => loop {
                use tokio::io::AsyncRead as _;
                match frame::decode_frame(fbuf) {
                    Ok(Some(f)) => return Ok(Some(f)),
                    Ok(None) => {}
                    Err(e) => {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!("udp channel frame: {e}"),
                        ))
                    }
                }
                if fbuf.len() > frame::HEADER_LEN + frame::MAX_FRAME_LEN {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "udp channel frame exceeds limit",
                    ));
                }
                let mut tmp = [0u8; 8192];
                let n = std::future::poll_fn(|cx| {
                    let mut rb = tokio::io::ReadBuf::new(&mut tmp);
                    match std::pin::Pin::new(&mut *r).poll_read(cx, &mut rb) {
                        std::task::Poll::Ready(Ok(())) => {
                            std::task::Poll::Ready(Ok(Some(rb.filled().len())))
                        }
                        std::task::Poll::Ready(Err(e)) => std::task::Poll::Ready(Err(e)),
                        std::task::Poll::Pending => std::task::Poll::Ready(Ok(None)),
                    }
                })
                .await?;
                match n {
                    Some(0) | None => return Ok(None),
                    Some(k) => fbuf.extend_from_slice(&tmp[..k]),
                }
            },
            Self::Chunks { r, buf } => loop {
                match frame::decode_frame(buf) {
                    Ok(Some(f)) => return Ok(Some(f)),
                    Ok(None) => {}
                    Err(e) => {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!("udp channel frame: {e}"),
                        ))
                    }
                }
                match r.try_read_chunk().await? {
                    Some(c) => buf.extend_from_slice(&c),
                    None => return Ok(None),
                }
            },
        }
    }
}

/// N10:通道写侧。mux 载体偷半后 `write_chunk(Bytes)` 引用直达 yamux
/// 帧(零拷贝);其余载体批量 writev。
enum ChanWriter {
    Stream(Box<dyn tokio::io::AsyncWrite + Send + Unpin>),
    Chunks(rh_transport::mux::RhMuxStreamWrite),
}

/// N3:全量向量写(本 tokio 版本无 `write_all_vectored`,自实现):
/// 逐次 `poll_write_vectored` 消费切片。载体不支持向量写时 tokio 默认
/// 实现只写首个非空段 —— 这里按返回值推进已写字节,剩余段后续轮次
/// 继续,线格式与逐帧写完全一致。
async fn writev_all<W: tokio::io::AsyncWrite + Unpin>(
    w: &mut W,
    mut slices: &mut [std::io::IoSlice<'_>],
) -> std::io::Result<()> {
    use std::future::poll_fn;
    while !slices.is_empty() && slices[0].is_empty() {
        slices = &mut slices[1..];
    }
    while !slices.is_empty() {
        let n = poll_fn(|cx| std::pin::Pin::new(&mut *w).poll_write_vectored(cx, slices)).await?;
        if n == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                "writev_all: zero-length write",
            ));
        }
        let mut rem = n;
        while let Some(first) = slices.first_mut() {
            if first.len() <= rem {
                rem -= first.len();
                slices = &mut slices[1..];
            } else {
                first.advance(rem);
                break;
            }
        }
    }
    Ok(())
}

/// 唯一写任务:独占写半,从有界队列收帧写出。
///
/// - mux 载体(N10 偷半):每帧 `write_chunk`,Bytes 引用直达 yamux 帧
///   —— 零拷贝,且消掉 mux 流 `tokio::io::split` 的同步锁;
/// - 其余载体:贪心多取 ≤BATCH_MAX 帧,`write_all_vectored` 单次下发
///   (N3:修前把每帧 payload 再拷进批缓冲,每包一次整包拷贝;载体
///   不支持向量写时由 write_all_vectored 内部逐段推进,线格式不变)。
///
/// 返回即表示写半已关闭(对端断开或写错误),调用方据此终止整个通道。
async fn writer_task(mut w: ChanWriter, mut rx: mpsc::Receiver<Pending>) {
    match &mut w {
        ChanWriter::Chunks(hw) => {
            let mut carry: Option<Pending> = None;
            let mut frames: Vec<bytes::Bytes> = Vec::with_capacity(BATCH_MAX);
            'outer: loop {
                // 阻塞等第一帧(空队列时不空转;有界队列让 send 侧承担背压)。
                let first = match carry.take() {
                    Some(p) => p,
                    None => match rx.recv().await {
                        Some(p) => p,
                        None => break, // 所有发送端已 drop = 通道结束
                    },
                };
                match rx.try_recv() {
                    Err(_) => {
                        // 单会话常见零拷贝快路径:直接移交 Bytes 给 yamux。
                        if hw.write_chunk(first.framed).await.is_err() {
                            while rx.try_recv().is_ok() {}
                            break 'outer;
                        }
                    }
                    Ok(second) => {
                        if first.framed.len() + second.framed.len() > 64 * 1024 {
                            carry = Some(second);
                            if hw.write_chunk(first.framed).await.is_err() {
                                while rx.try_recv().is_ok() {}
                                break 'outer;
                            }
                        } else {
                            frames.clear();
                            let mut total = first.framed.len() + second.framed.len();
                            frames.push(first.framed);
                            frames.push(second.framed);
                            while frames.len() < BATCH_MAX && total < 64 * 1024 {
                                match rx.try_recv() {
                                    Ok(next) => {
                                        if total + next.framed.len() > 64 * 1024 {
                                            carry = Some(next);
                                            break;
                                        }
                                        total += next.framed.len();
                                        frames.push(next.framed);
                                    }
                                    Err(_) => break,
                                }
                            }
                            let mut merged = bytes::BytesMut::with_capacity(total);
                            for f in &frames {
                                merged.extend_from_slice(f);
                            }
                            if hw.write_chunk(merged.freeze()).await.is_err() {
                                while rx.try_recv().is_ok() {}
                                break 'outer;
                            }
                        }
                    }
                }
            }
        }
        ChanWriter::Stream(sw) => {
            // 帧持有缓冲复用;IoSlice 用固定栈数组按自然借用构造。
            let mut frames: Vec<bytes::Bytes> = Vec::with_capacity(BATCH_MAX);
            loop {
                let Some(first) = rx.recv().await else {
                    break;
                };
                frames.clear();
                frames.push(first.framed);
                let mut n = 1;
                while n < BATCH_MAX {
                    match rx.try_recv() {
                        Ok(next) => {
                            frames.push(next.framed);
                            n += 1;
                        }
                        Err(_) => break,
                    }
                }
                // 一次 writev 写出 n 个**独立**帧(线格式与逐帧写完全一致)。
                let mut slices = [std::io::IoSlice::new(&[]); BATCH_MAX];
                for (i, f) in frames.iter().enumerate() {
                    slices[i] = std::io::IoSlice::new(f.as_ref());
                }
                if writev_all(sw, &mut slices[..frames.len()]).await.is_err() {
                    break;
                }
            }
        }
    }
    // 关闭写半:Chunks 有序关闭(yamux FIN);Stream 常规 shutdown。
    match &mut w {
        ChanWriter::Chunks(hw) => {
            let _ = hw.close().await;
        }
        ChanWriter::Stream(sw) => {
            let _ = sw.shutdown().await;
        }
    }
}

/// 代理侧:通道 ↔ 本地目标 UDP。按用户源地址各开一个本地 socket,
/// 回写任务经唯一写任务写帧;主循环读帧 + 探活。
/// 每 60s 清理 60s 无活动的用户会话(源端口轮换的客户端会持续产生新条目)。
pub async fn run_proxy_side(channel: BoxedIo, spec: std::sync::Arc<ProxySpec>, prefix: Vec<u8>) {
    // ★目标解析支持**域名**(经 TTL 缓存,与 TCP 路径同一入口)。
    //
    // 修前只 `parse::<SocketAddr>()` —— 域名直接失败,而且只打 `debug`
    // 级日志(默认 info 下不可见)。表现为"UDP 规则配了域名就是不工作",
    // 零线索。`forward::resolve` 已实现"IP 直连快路径 + 域名缓存 +
    // 并发去重",直接复用。
    let target = match crate::forward::resolve(&spec.target_host, spec.target_port).await {
        Ok(a) => a,
        Err(e) => {
            tracing::warn!(
                proxy = spec.name,
                host = %spec.target_host,
                port = spec.target_port,
                error = %e,
                "udp target unresolvable (rule will not work)"
            );
            return;
        }
    };
    // N10:优先偷 mux 半 —— 写侧 chunk 零拷贝(Bytes 引用直达 yamux 帧),
    // 读侧 chunk 组帧,并消掉 mux 载体下 tokio::io::split 的同步锁;
    // 非 mux 载体先试无锁 into_split(裸 TcpStream 两个独立 fd),
    // 不行再回落同步锁拆分。
    let mut channel = channel;
    let (mut r, w): (ChanReader, ChanWriter) =
        match rh_transport::mux::steal_mux_halves(&mut channel) {
            Some((mr, mw)) => (
                ChanReader::Chunks {
                    r: mr,
                    buf: read_buf_with_prefix(&prefix),
                },
                ChanWriter::Chunks(mw),
            ),
            None => match rh_transport::split_boxed_io(channel) {
                Ok((r, w)) => (ChanReader::Stream(r), ChanWriter::Stream(w)),
                Err(io) => {
                    let (r, w) = tokio::io::split(io);
                    (
                        ChanReader::Stream(Box::new(r)),
                        ChanWriter::Stream(Box::new(w)),
                    )
                }
            },
        };
    let (tx, rx) = mpsc::channel::<Pending>(WRITE_QUEUE);
    // 写任务独占写半:此后主循环与各回写任务都不再持有它,
    // 也就不存在"跨 await 持锁"和"全部会话等同一把锁"。
    let writer = tokio::spawn(writer_task(w, rx));

    let recv_pool = UdpRecvBatchPool::new();
    let mut sessions: HashMap<std::net::SocketAddr, Session> = HashMap::new();
    let mut batch_items: Vec<(bytes::Bytes, std::net::SocketAddr)> =
        Vec::with_capacity(rh_transport::udp_batch::UDP_BATCH_MAX);
    let dummy_addr = std::net::SocketAddr::from(([0, 0, 0, 0], 0));
    // hello 之后已抵达的字节必须预置进读缓冲,**两条载体路径都要**。
    //
    // 修前只有 Chunks 路径做了(见上方 `ChanReader::Chunks { buf: prefix }`),
    // Stream 路径的 fbuf 是空 `with_capacity(2048)` —— 而紧邻的注释却写着
    // "Stream 路径用 fbuf",与代码不符。当时没有行为差异,因为唯一调用方
    // (`session.rs::open_data_channel`)传入的 `buf.to_vec()` 恒为空
    // (buf 只分配、未读)。但这是个陷阱:一旦将来有调用方传非空 prefix,
    // mux 载体正常、非 mux 载体**静默丢数据**,丢的还是隧道建立后最先
    // 到达的那几字节(往往是用户数据帧的帧头),表现为偶发解析失败,
    // 极难归因。
    //
    // 修法取最小等价:Stream 路径的 fbuf 直接以 prefix 起头,与 Chunks
    // 路径、以及拆分双任务之前的旧实现(两条路径都预置)一致。
    let mut fbuf = read_buf_with_prefix(&prefix);
    let mut last_alive = Instant::now();
    let mut last_ping = Instant::now();
    let mut gc = tokio::time::interval(Duration::from_secs(60));
    gc.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let _ = gc.tick().await;
    // A1 同族:sleep 分支会在**每次 select 迭代**重新创建并重新计时
    // (事件频率高时探活永不触发,事件稀疏时又可能连发)。改用固定周期
    // interval + Delay,时间基准稳定、不随事件重置。
    let mut probe = tokio::time::interval(Duration::from_secs(15));
    probe.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let _ = probe.tick().await;
    loop {
        tokio::select! {
            rr = r.read_frame(&mut fbuf) => {
                match rr {
                    Ok((FrameType::UDP_PACKET, p)) => {
                        batch_items.clear();
                        let mut frames_drained = 1usize;
                        let mut valid_seen = false;
                        push_udp_proxy_packet(&mut batch_items, &mut valid_seen, p);
                        let mut trailing_frame: Option<FrameType> = None;
                        let mut trailing_err = false;
                        while frames_drained < rh_transport::udp_batch::UDP_BATCH_MAX {
                            match r.try_read_frame(&mut fbuf).await {
                                Ok(Some((FrameType::UDP_PACKET, p2))) => {
                                    frames_drained += 1;
                                    push_udp_proxy_packet(&mut batch_items, &mut valid_seen, p2);
                                }
                                Ok(Some((ty, _))) => {
                                    trailing_frame = Some(ty);
                                    break;
                                }
                                Ok(None) => break,
                                Err(_) => {
                                    trailing_err = true;
                                    break;
                                }
                            }
                        }
                        let mut idx = 0usize;
                        while idx < batch_items.len() {
                            let user = batch_items[idx].1;
                            let mut end = idx + 1;
                            while end < batch_items.len() && batch_items[end].1 == user {
                                end += 1;
                            }
                            if let Some(s) = get_session(&mut sessions, user, target, &tx, &recv_pool).await {
                                let mut out_arr: [(&[u8], std::net::SocketAddr); rh_transport::udp_batch::UDP_BATCH_MAX] =
                                    [(&[], dummy_addr); rh_transport::udp_batch::UDP_BATCH_MAX];
                                let mut out_len = 0usize;
                                for (data, _) in &batch_items[idx..end] {
                                    out_arr[out_len] = (data.as_ref(), target);
                                    out_len += 1;
                                }
                                let _ = rh_transport::udp_batch::send_to_batch(&s, &out_arr[..out_len]).await;
                            }
                            idx = end;
                        }
                        if valid_seen {
                            last_alive = Instant::now();
                        }
                        if let Some(ty) = trailing_frame {
                            if ty == FrameType::PING
                                && tx.send(Pending { framed: framed_ping_or_pong(false) }).await.is_err()
                            {
                                break;
                            }
                            last_alive = Instant::now();
                        }
                        if trailing_err {
                            break;
                        }
                    }
                    Ok((FrameType::PING, _)) => {
                        // 只投递,不等待:写任务会按序写出(帧序由单队列保证)。
                        if tx.send(Pending { framed: framed_pong() }).await.is_err() {
                            break;
                        }
                        last_alive = Instant::now();
                    }
                    Ok(_) => { last_alive = Instant::now(); }
                    Err(_) => break,
                }
            }
            _ = gc.tick() => {
                // A4:淘汰必须**真正取消**回写任务,不能只删 map 条目。
                // 只删条目的话任务仍持 sock2/tx2 活着,泄漏链见 Session::cancel。
                let now = Instant::now();
                let stale: Vec<std::net::SocketAddr> = sessions
                    .iter()
                    .filter(|(_, s)| now.duration_since(s.last) >= Duration::from_secs(60))
                    .map(|(k, _)| *k)
                    .collect();
                for k in stale {
                    if let Some(s) = sessions.remove(&k) {
                        s.cancel.cancel();
                        drop(s); // 释放 Arc<UdpSocket>(任务侧仍有 clone,由 cancel 退出后释放)
                    }
                }
            }
            _ = probe.tick() => {
                if last_ping.elapsed() >= Duration::from_secs(30) {
                    if tx.send(Pending { framed: framed_ping() }).await.is_err() {
                        break;
                    }
                    last_ping = Instant::now();
                }
                if last_alive.elapsed() > Duration::from_secs(90) {
                    break;
                }
            }
        }
    }
    // A4:退出前取消**全部**回写任务。否则它们仍持 tx2 的 clone,
    // `drop(tx)` 不足以让 rx 关闭 → writer 永不结束 → 本函数永挂 →
    // 上层每次重连都泄漏一整套 ClientSession。
    for s in sessions.values() {
        s.cancel.cancel();
    }
    drop(sessions);
    drop(tx); // 关闭队列 → 写任务排空后 shutdown 写半
    let _ = writer.await;
}

fn push_udp_proxy_packet(
    batch_items: &mut Vec<(bytes::Bytes, std::net::SocketAddr)>,
    valid_seen: &mut bool,
    payload: bytes::Bytes,
) {
    if let Some((src, _data)) = udp_packet::decode(&payload) {
        let offset = 1 + src.len();
        if let Some(user) = parse_addr_cached(src) {
            batch_items.push((payload.slice(offset..), user));
        }
        *valid_seen = true;
    }
}

fn framed_ping_or_pong(is_ping: bool) -> bytes::Bytes {
    if is_ping {
        framed_ping()
    } else {
        framed_pong()
    }
}

/// 一个用户源地址对应的本地 socket 会话。
struct Session {
    sock: Arc<UdpSocket>,
    last: Instant,
    /// A4:回写任务的取消令牌。
    ///
    /// 修前 GC 只 `retain` 删 map 条目,而该会话 spawn 的回写任务持着
    /// `sock2` + `tx2`,唯一退出条件是 socket 报错(自持,永不会)或
    /// tx2 失败(writer 死了才有)。泄漏任务仍持 `tx2` → `drop(tx)` 后
    /// `writer.await` 永挂 → `run_proxy_side` 永挂 → 每次重连整套泄漏旧
    /// ClientSession。源端口轮换的客户端(QUIC/DNS)十几分钟就能耗尽 fd。
    cancel: tokio_util::sync::CancellationToken,
}

async fn get_session(
    sessions: &mut HashMap<std::net::SocketAddr, Session>,
    user: std::net::SocketAddr,
    target: std::net::SocketAddr,
    tx: &mpsc::Sender<Pending>,
    recv_pool: &UdpRecvBatchPool,
) -> Option<Arc<UdpSocket>> {
    if let Some(s) = sessions.get_mut(&user) {
        s.last = Instant::now();
        return Some(s.sock.clone());
    }
    // 按目标地址族选择本地绑定(目标为 IPv6 时回退 IPv4 socket 必然发不出去)
    let bind_local = if target.is_ipv4() {
        "0.0.0.0:0"
    } else {
        "[::]:0"
    };
    let sock = UdpSocket::bind(bind_local).await.ok()?;
    let sock = Arc::new(sock);
    // 用户源地址的字符串形式**只在这里算一次**,由回写任务捕获复用。
    // 曾经是每收一个包就 `user.to_string()` 一次 —— 高 PPS 下是实打实的
    // 分配 + 格式化开销。
    let addr_str: Arc<str> = Arc::from(user.to_string());
    let cancel = tokio_util::sync::CancellationToken::new();
    sessions.insert(
        user,
        Session {
            sock: sock.clone(),
            last: Instant::now(),
            cancel: cancel.clone(),
        },
    );
    // 回写任务:本地目标响应 → 池化 recvmmsg 批收整批成帧 → 通道(src = 该用户源地址)
    let sock2 = sock.clone();
    let tx2 = tx.clone();
    let pool2 = recv_pool.clone();
    tokio::spawn(async move {
        loop {
            // 空闲挂起不占 UdpRecvBatch(2 MiB),就绪后临时借用并立即归还。
            // A4:会话被 GC 取消(或整个通道退出)时必须能退出这个 await。
            tokio::select! {
                r = sock2.readable() => {
                    if r.is_err() {
                        break;
                    }
                }
                _ = cancel.cancelled() => break,
            }
            let framed = {
                let mut batch = pool2.acquire();
                match rh_transport::udp_batch::try_recv_from_batch(&sock2, &mut batch) {
                    Ok(0) => continue,
                    Ok(count) => {
                        let total_data: usize = batch.iter().map(|(pkt, _)| pkt.len()).sum();
                        let needed = count * (frame::HEADER_LEN + 1 + addr_str.len()) + total_data;
                        let mut out = bytes::BytesMut::with_capacity(needed);
                        for (pkt, _) in batch.iter() {
                            append_udp_packet(&mut out, &addr_str, pkt);
                        }
                        out.freeze()
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => continue,
                    Err(_) => break,
                }
            };
            // ★send 必须与 cancel 同一个 select:A4 修好了 recv 侧的退出,
            // 但 send 一旦因队列满而阻塞,就再也观察不到 cancel ——
            // 任务永久持有 sock2/tx2,`writer.await` 永挂,每次重连泄漏
            // 整套通道。
            let sent = tokio::select! {
                r = tx2.send(Pending { framed }) => r.is_ok(),
                _ = cancel.cancelled() => false,
            };
            if !sent {
                break;
            }
        }
    });
    Some(sock)
}

/// 解析 `SocketAddr`,走一个**每线程**的小缓存。
///
/// 服务端下发的 src 字符串在同一会话内高度重复(同一个用户源地址反复发包),
/// 但这里拿不到会话表的 key(那要先解析)。用一个极小的直接映射缓存把这
/// 一步也省掉:命中率在高 PPS 单用户场景接近 100%。
fn parse_addr_cached(src: &str) -> Option<std::net::SocketAddr> {
    use std::cell::RefCell;
    use std::net::SocketAddr;
    use std::str::FromStr;

    /// 缓存槽数(小 = 恒定开销;冲突直接覆盖,不做淘汰)。
    const SLOTS: usize = 8;

    thread_local! {
        static CACHE: RefCell<[Option<(String, SocketAddr)>; SLOTS]> =
            const { RefCell::new([const { None }; SLOTS]) };
    }

    // B10:槽位用整串字节折叠哈希。修前用**首字节**取模 —— IPv4 源地址
    // 首字符几乎恒为 '1'/'2',实际只用 2-3 槽,多用户时每包都 miss,
    // "命中率接近 100%"只在单用户下成立。
    let slot = src
        .bytes()
        .fold(0usize, |a, b| a.wrapping_mul(31).wrapping_add(b as usize))
        % SLOTS;
    CACHE.with(|c| {
        let mut c = c.borrow_mut();
        if let Some((k, v)) = &c[slot] {
            if k == src {
                return Some(*v);
            }
        }
        let v = SocketAddr::from_str(src).ok()?;
        c[slot] = Some((src.to_string(), v));
        Some(v)
    })
}

/// visitor 侧 sudp:本地 UDP 监听 ↔ 通道(src 字段承载本地用户源)。
/// 单循环:sock.recv → 帧;帧 → sock.send_to;PING/PONG 探活;
/// 结束即返回(上层重新武装)。
///
/// 与代理侧同样走"唯一写任务 + 批聚合":visitor 侧的写半此前由主循环独占,
/// 虽无锁争用,但每包一帧(11B 头)在大包量下 syscall 开销同样可观,
/// 且 PING/PONG 与 UDP_PACKET 混在同一写任务里可保证**帧序**。
fn visitor_addr_slot(peer: std::net::SocketAddr, len: usize) -> usize {
    let mut h = peer.port() as usize;
    match peer.ip() {
        std::net::IpAddr::V4(ip) => {
            h = h.wrapping_mul(31).wrapping_add(u32::from(ip) as usize);
        }
        std::net::IpAddr::V6(ip) => {
            for seg in ip.segments() {
                h = h.wrapping_mul(31).wrapping_add(seg as usize);
            }
        }
    }
    h % len
}

/// P0-2 / P0-5:visitor 侧 UDP 通道拆半(支持 PrependedIo(RhMuxStream) 直达零拷贝 Chunks)。
fn split_visitor_channel(mut channel: BoxedIo) -> (ChanReader, ChanWriter) {
    match rh_transport::mux::steal_mux_halves(&mut channel) {
        Some((mr, mw)) => (
            ChanReader::Chunks {
                r: mr,
                buf: bytes::BytesMut::with_capacity(64 * 1024),
            },
            ChanWriter::Chunks(mw),
        ),
        None => match rh_transport::split_boxed_io(channel) {
            Ok((r, w)) => (ChanReader::Stream(r), ChanWriter::Stream(w)),
            Err(io) => {
                let (r, w) = tokio::io::split(io);
                (
                    ChanReader::Stream(Box::new(r)),
                    ChanWriter::Stream(Box::new(w)),
                )
            }
        },
    }
}

pub async fn run_visitor_side(
    channel: BoxedIo,
    sock: Arc<UdpSocket>,
    cancel: tokio_util::sync::CancellationToken,
    pending: std::collections::VecDeque<(std::net::SocketAddr, Vec<u8>)>,
) {
    // N10:与 run_proxy_side 同款 —— 优先偷 mux 半(写侧 chunk 零拷贝,
    // 读侧 chunk 组帧,消掉同步锁);非 mux 先试无锁 into_split。
    let (mut r, w) = split_visitor_channel(channel);
    let (tx, rx) = mpsc::channel::<Pending>(WRITE_QUEUE);
    let writer = tokio::spawn(writer_task(w, rx));
    // 先冲通道到位前缓冲的本地包(仍在写任务里按序写出)
    for (peer, data) in pending {
        let framed = frame_udp_packet(&peer.to_string(), &data);
        if tx.send(Pending { framed }).await.is_err() {
            drop(tx);
            let _ = writer.await;
            return;
        }
    }
    let mut fbuf = bytes::BytesMut::with_capacity(64 * 1024);
    let mut recv_batch = rh_transport::udp_batch::UdpRecvBatch::new(UDP_RECV_BUF);
    let mut batch_items: Vec<(bytes::Bytes, std::net::SocketAddr)> =
        Vec::with_capacity(rh_transport::udp_batch::UDP_BATCH_MAX);
    let dummy_addr = std::net::SocketAddr::from(([0, 0, 0, 0], 0));
    // B10:源地址字符串直接映射缓存(SocketAddr → String),按完整 SocketAddr 哈希进 16 槽。
    let mut addr_cache: [Option<(std::net::SocketAddr, String)>; 16] = [const { None }; 16];
    let mut last_alive = Instant::now();
    let mut last_ping = Instant::now();
    // 同 run_proxy_side:探活用固定周期 interval,不随事件重置。
    let mut probe = tokio::time::interval(Duration::from_secs(15));
    probe.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let _ = probe.tick().await;
    loop {
        tokio::select! {
            rr = rh_transport::udp_batch::recv_from_batch(&sock, &mut recv_batch) => {
                let Ok(count) = rr else { break };
                if count == 0 {
                    continue;
                }
                let total_data: usize = recv_batch.iter().map(|(pkt, _)| pkt.len()).sum();
                let mut out = bytes::BytesMut::with_capacity(
                    count * (frame::HEADER_LEN + 1 + 48) + total_data,
                );
                for (pkt, peer) in recv_batch.iter() {
                    let slot = visitor_addr_slot(peer, addr_cache.len());
                    if !matches!(&addr_cache[slot], Some((p, _)) if *p == peer) {
                        addr_cache[slot] = Some((peer, peer.to_string()));
                    }
                    let addr_str = addr_cache[slot].as_ref().map(|(_, s)| s.as_str()).unwrap_or("");
                    // N3:整批多帧一次写入同一个 BytesMut(每包仍为独立标准 UDP_PACKET 帧)
                    append_udp_packet(&mut out, addr_str, pkt);
                }
                if tx.send(Pending { framed: out.freeze() }).await.is_err() {
                    break;
                }
            }
            rr = r.read_frame(&mut fbuf) => {
                match rr {
                    Ok((FrameType::UDP_PACKET, p)) => {
                        batch_items.clear();
                        let mut frames_drained = 1usize;
                        let mut valid_seen = false;
                        push_udp_proxy_packet(&mut batch_items, &mut valid_seen, p);
                        let mut trailing_frame: Option<FrameType> = None;
                        let mut trailing_err = false;
                        while frames_drained < rh_transport::udp_batch::UDP_BATCH_MAX {
                            match r.try_read_frame(&mut fbuf).await {
                                Ok(Some((FrameType::UDP_PACKET, p2))) => {
                                    frames_drained += 1;
                                    push_udp_proxy_packet(&mut batch_items, &mut valid_seen, p2);
                                }
                                Ok(Some((ty, _))) => {
                                    trailing_frame = Some(ty);
                                    break;
                                }
                                Ok(None) => break,
                                Err(_) => {
                                    trailing_err = true;
                                    break;
                                }
                            }
                        }
                        if !batch_items.is_empty() {
                            let mut out_arr: [(&[u8], std::net::SocketAddr); rh_transport::udp_batch::UDP_BATCH_MAX] =
                                [(&[], dummy_addr); rh_transport::udp_batch::UDP_BATCH_MAX];
                            let mut out_len = 0usize;
                            for (data, addr) in &batch_items {
                                out_arr[out_len] = (data.as_ref(), *addr);
                                out_len += 1;
                            }
                            let _ = rh_transport::udp_batch::send_to_batch(&sock, &out_arr[..out_len]).await;
                        }
                        if valid_seen {
                            last_alive = Instant::now();
                        }
                        if let Some(ty) = trailing_frame {
                            if ty == FrameType::PING
                                && tx.send(Pending { framed: framed_pong() }).await.is_err()
                            {
                                break;
                            }
                            last_alive = Instant::now();
                        }
                        if trailing_err {
                            break;
                        }
                    }
                    Ok((FrameType::PING, _)) => {
                        if tx.send(Pending { framed: framed_pong() }).await.is_err() {
                            break;
                        }
                        last_alive = Instant::now();
                    }
                    Ok(_) => { last_alive = Instant::now(); }
                    Err(_) => break,
                }
            }
            _ = probe.tick() => {
                if last_ping.elapsed() >= Duration::from_secs(30) {
                    if tx.send(Pending { framed: framed_ping() }).await.is_err() {
                        break;
                    }
                    last_ping = Instant::now();
                }
                if last_alive.elapsed() > Duration::from_secs(90) {
                    break;
                }
            }
            _ = cancel.cancelled() => break,
        }
    }
    drop(tx);
    let _ = writer.await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncReadExt;

    /// 起一对 duplex 通道,跑 writer_task,返回(写端 tx,读端 r,任务句柄)。
    fn spawn_writer(
        cap: usize,
    ) -> (
        mpsc::Sender<Pending>,
        tokio::io::DuplexStream,
        tokio::task::JoinHandle<()>,
    ) {
        let (a, b) = tokio::io::duplex(1 << 20);
        let (tx, rx) = mpsc::channel::<Pending>(cap);
        let h = tokio::spawn(writer_task(ChanWriter::Stream(Box::new(b)), rx));
        (tx, a, h)
    }

    async fn read_all_frames(r: &mut tokio::io::DuplexStream) -> Vec<(FrameType, Vec<u8>)> {
        let mut out = Vec::new();
        let mut buf = bytes::BytesMut::with_capacity(4096);
        while let Ok((ty, p)) = frame::read_frame(r, &mut buf).await {
            out.push((ty, p.to_vec()));
        }
        out
    }

    fn pkt(addr: &str, data: &[u8]) -> Pending {
        Pending {
            framed: frame_udp_packet(addr, data),
        }
    }

    /// **回归(prefix 静默丢失)**:读缓冲构造必须预置 hello 后已抵达的字节。
    ///
    /// 修前 Chunks 路径在构造时并入 prefix、Stream 路径却另起一个空
    /// `with_capacity(2048)`,而紧邻注释写着"Stream 路径用 fbuf" ——
    /// 注释与代码不符。当时无行为差异(唯一调用方传的 prefix 恒空),
    /// 但一旦有调用方传非空 prefix,mux 载体正常、非 mux 载体**静默丢数据**,
    /// 丢的是隧道建立后最先到达的字节(常是帧头),表现为偶发解析失败。
    ///
    /// 这条直接测 `read_buf_with_prefix`(两条路径共用的唯一入口),
    /// 所以改坏实现**必然**变红 —— 不像"自己造 fbuf"那种写法会绕过被测代码。
    #[test]
    fn read_buf_prepends_prefix() {
        // 用一帧完整的 UDP_PACKET 当 prefix:若实现丢弃它,首帧就解不出来
        let first = frame_udp_packet("1.1.1.1:1000", b"first");
        let mut fbuf = read_buf_with_prefix(&first);
        let decoded = frame::decode_frame(&mut fbuf)
            .expect("prefix 必须是合法帧字节")
            .expect("prefix 中的完整帧必须能立刻被解出");
        assert_eq!(decoded.0, FrameType::UDP_PACKET);
        let (src, data) = udp_packet::decode(&decoded.1).expect("首帧必须可解码");
        assert_eq!(src, "1.1.1.1:1000", "解出的必须是 prefix 里那一帧");
        assert_eq!(data, b"first");
    }

    /// 反向对照:空 prefix 时缓冲为空、后续字节照常解析(现有调用方的真实情形)。
    #[test]
    fn read_buf_with_empty_prefix_is_empty() {
        let mut fbuf = read_buf_with_prefix(&[]);
        assert!(fbuf.is_empty(), "空 prefix 不得凭空产生字节");
        let f = frame_udp_packet("3.3.3.3:3000", b"only");
        fbuf.extend_from_slice(&f);
        let decoded = frame::decode_frame(&mut fbuf).unwrap().unwrap();
        assert_eq!(decoded.0, FrameType::UDP_PACKET);
        let (src, data) = udp_packet::decode(&decoded.1).unwrap();
        assert_eq!(src, "3.3.3.3:3000");
        assert_eq!(data, b"only");
    }

    /// prefix 之后紧跟真实通道字节时,两段必须被拼成连续帧流
    /// (这正是"hello 后的残留字节 + 后续帧"的真实形态)。
    #[tokio::test]
    async fn stream_path_parses_prefix_then_channel_bytes() {
        let first = frame_udp_packet("1.1.1.1:1000", b"first");
        let second = frame_udp_packet("2.2.2.2:2000", b"second");

        // 通道里只放第二帧:第一帧完全来自 prefix
        let (mut w, r) = tokio::io::duplex(4096);
        w.write_all(&second).await.unwrap();
        drop(w);

        let mut r = ChanReader::Stream(Box::new(r));
        let mut fbuf = read_buf_with_prefix(&first);

        let (ty, payload) = r
            .read_frame(&mut fbuf)
            .await
            .expect("prefix 中的完整帧必须能被解出(修前 Stream 路径会丢掉它)");
        assert_eq!(ty, FrameType::UDP_PACKET);
        let (src, data) = udp_packet::decode(&payload).expect("首帧必须可解码");
        assert_eq!(src, "1.1.1.1:1000");
        assert_eq!(data, b"first");

        let (ty2, payload2) = r.read_frame(&mut fbuf).await.expect("后续帧必须可读");
        assert_eq!(ty2, FrameType::UDP_PACKET);
        let (src2, data2) = udp_packet::decode(&payload2).unwrap();
        assert_eq!(src2, "2.2.2.2:2000");
        assert_eq!(data2, b"second");
    }

    /// **核心回归(串源)**:多个用户源地址并发写帧,每个源的包不得串到别的源。
    ///
    /// 修前是各回写任务共享 `Arc<Mutex<Option<W>>>` 逐帧写;现在改为单队列
    /// 批聚合。批聚合最容易出的错就是"批次里帧边界算错",让某个 src 的
    /// 数据挂到另一个 src 上 —— 这条测试逐帧解回来比对。
    #[tokio::test]
    async fn concurrent_sources_never_cross_packets() {
        const SOURCES: usize = 16;
        const PER_SOURCE: usize = 64;

        let (tx, mut r, h) = spawn_writer(256);
        // 并发投递:每源各一个任务,交替插入,制造交错
        let mut tasks = Vec::new();
        for s in 0..SOURCES {
            let tx = tx.clone();
            tasks.push(tokio::spawn(async move {
                let addr = format!("10.0.0.{s}:{}", 1000 + s);
                for i in 0..PER_SOURCE {
                    // 载荷里带上源标识与序号,便于校验归属
                    let body = format!("s{s}-i{i}").into_bytes();
                    tx.send(pkt(&addr, &body)).await.unwrap();
                }
            }));
        }
        drop(tx);
        for t in tasks {
            t.await.unwrap();
        }

        let frames = read_all_frames(&mut r).await;
        h.await.unwrap();

        assert_eq!(
            frames.len(),
            SOURCES * PER_SOURCE,
            "帧总数必须等于发出的包数(不得丢帧或合并成一帧)"
        );
        // 每源收到的序号必须完整且有序(单队列保序)
        let mut seen: Vec<Vec<usize>> = vec![Vec::new(); SOURCES];
        for (ty, p) in &frames {
            assert_eq!(*ty, FrameType::UDP_PACKET);
            let (src, data) = udp_packet::decode(p).expect("每帧都必须能解回");
            let data = std::str::from_utf8(data).unwrap();
            // 串源检测:载荷里的标识必须与帧头 src 一致
            let (s_part, i_part) = data.split_once("-i").expect("载荷格式");
            let s: usize = s_part.trim_start_matches('s').parse().unwrap();
            let i: usize = i_part.parse().unwrap();
            assert_eq!(
                src,
                format!("10.0.0.{s}:{}", 1000 + s),
                "帧头 src 与载荷来源不符 = 串源"
            );
            seen[s].push(i);
        }
        for (s, got) in seen.iter().enumerate() {
            assert_eq!(
                got,
                &(0..PER_SOURCE).collect::<Vec<_>>(),
                "源 {s} 的包必须完整且按序,实际: {got:?}"
            );
        }
    }

    /// **核心回归(帧序)**:PING/PONG 与 UDP_PACKET 交错时,写出顺序必须
    /// 与投递顺序一致。
    ///
    /// 修前 PING 分支为了拿 `&mut` 先加锁、释放、再加锁,中间存在窗口,
    /// 可能被回写任务插队导致 PONG 早于/晚于预期位置。现在单队列天然保序。
    #[tokio::test]
    async fn ping_pong_interleaved_keeps_order() {
        let (tx, mut r, h) = spawn_writer(256);
        // 投递:data0, ping, data1, pong, data2
        let seq = ["d0", "d1", "d2"];
        tx.send(pkt("1.2.3.4:5", seq[0].as_bytes())).await.unwrap();
        tx.send(Pending {
            framed: framed_ping(),
        })
        .await
        .unwrap();
        tx.send(pkt("1.2.3.4:5", seq[1].as_bytes())).await.unwrap();
        tx.send(Pending {
            framed: framed_pong(),
        })
        .await
        .unwrap();
        tx.send(pkt("1.2.3.4:5", seq[2].as_bytes())).await.unwrap();
        drop(tx);

        let frames = read_all_frames(&mut r).await;
        h.await.unwrap();

        let kinds: Vec<String> = frames
            .iter()
            .map(|(ty, p)| {
                if *ty == FrameType::UDP_PACKET {
                    let (_, d) = udp_packet::decode(p).unwrap();
                    format!("pkt:{}", std::str::from_utf8(d).unwrap())
                } else if *ty == FrameType::PING {
                    "ping".to_string()
                } else if *ty == FrameType::PONG {
                    "pong".to_string()
                } else {
                    format!("{ty:?}")
                }
            })
            .collect();
        assert_eq!(
            kinds,
            vec!["pkt:d0", "ping", "pkt:d1", "pong", "pkt:d2"],
            "PING/PONG 与数据帧的相对顺序必须与投递顺序一致"
        );
    }

    /// 线格式红线:批聚合写出的仍是**多个独立帧**,不是"一帧多包"。
    ///
    /// 用一个严格按帧头长度切分的读取器验证:每个 UDP_PACKET 的 payload
    /// 必须恰好是 `[len][addr][data]` 一个包,解出的 addr 必须是单个地址。
    #[tokio::test]
    async fn batch_still_emits_independent_frames() {
        let (tx, mut r, h) = spawn_writer(256);
        for i in 0..64u16 {
            let addr = format!("10.1.0.1:{}", 2000 + i);
            tx.send(pkt(&addr, b"payload")).await.unwrap();
        }
        drop(tx);

        // 手工逐帧解析(不经 decode_frame 的缓冲合并),确认帧边界清晰
        let mut raw = Vec::new();
        r.read_to_end(&mut raw).await.unwrap();
        h.await.unwrap();

        let mut off = 0usize;
        let mut count = 0usize;
        while off < raw.len() {
            assert_eq!(&raw[off..off + 4], b"RHTP", "帧必须逐个以 MAGIC 开头");
            let ty = u16::from_be_bytes([raw[off + 5], raw[off + 6]]);
            let len = u32::from_be_bytes([raw[off + 7], raw[off + 8], raw[off + 9], raw[off + 10]])
                as usize;
            assert_eq!(ty, FrameType::UDP_PACKET.0);
            let payload = &raw[off + 11..off + 11 + len];
            let decoded = udp_packet::decode(payload).expect("每帧一个包");
            assert_eq!(
                decoded.0,
                format!("10.1.0.1:{}", 2000 + count),
                "帧内容必须与投递顺序一致,且一帧只承载一个包"
            );
            assert_eq!(decoded.1, b"payload");
            off += 11 + len;
            count += 1;
        }
        assert_eq!(count, 64, "64 个包必须是 64 个独立帧");
    }

    /// 背压红线:队列有界,生产者必须被阻塞而不是无限堆积。
    ///
    /// 构造要点:写任务取走一帧后会卡在 `write_all`(读端缓冲极小且永不读),
    /// 此后队列只进不出 —— 这时才能稳定观察 "队列满" 与 "send 阻塞"。
    /// 若只塞一轮就断言,写任务可能刚好取走一格让 send 成功(实测 flaky)。
    #[tokio::test]
    async fn bounded_queue_applies_backpressure() {
        const CAP: usize = 4;
        let (_a, b) = tokio::io::duplex(64); // 读端缓冲极小且永不读
        let (tx, rx) = mpsc::channel::<Pending>(CAP);
        let h = tokio::spawn(writer_task(ChanWriter::Stream(Box::new(b)), rx));

        // 第一轮:把写任务喂到"卡在 write_all"的状态(它取走 1 帧后不动了)
        let padded = format!("{:0>2000}", "x"); // 超过 duplex 缓冲,保证写阻塞
        let _ = tx.try_send(pkt("1.1.1.1:1", padded.as_bytes()));
        // 让写任务真正跑起来并卡住
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // 此时队列只进不出:填满它
        let mut sent = 0usize;
        for i in 0..(CAP * 4) {
            match tx.try_send(pkt("1.1.1.1:1", format!("y{i}").as_bytes())) {
                Ok(()) => sent += 1,
                Err(_) => break, // Full 或 Closed
            }
        }
        assert!(
            sent < CAP * 4,
            "队列必须有界:try_send 必须因 Full 失败,而不是无限接收(实际收下 {sent} 个)"
        );

        // async send 此时也必须阻塞
        let blocked = tokio::time::timeout(
            std::time::Duration::from_millis(150),
            tx.send(pkt("1.1.1.1:1", b"blocked")),
        )
        .await;
        assert!(
            blocked.is_err(),
            "队列满时 send() 必须阻塞(背压),不得丢弃或无界增长"
        );
        drop(tx);
        h.abort();
    }

    /// 地址缓存:同一字符串重复解析结果一致,且与直接 parse 相同。
    #[test]
    fn addr_cache_matches_parse() {
        let cases = [
            "127.0.0.1:53",
            "10.0.0.7:1234",
            "[::1]:8080",
            "[2001:db8::1]:443",
        ];
        for c in cases {
            let direct: std::net::SocketAddr = c.parse().unwrap();
            // 第一次(未命中,写入缓存)
            assert_eq!(parse_addr_cached(c), Some(direct));
            // 第二次(命中)
            assert_eq!(parse_addr_cached(c), Some(direct));
        }
        assert_eq!(parse_addr_cached("not-an-addr"), None);
    }
}

#[cfg(test)]
mod session_leak_tests {
    //! A4 回归:UDP 会话回写任务不得泄漏。
    //!
    //! 修前 GC 只 `retain` 删 map 条目,回写任务仍持 `sock2` + `tx2`
    //! (唯一退出条件是 socket 报错或 tx 失败,两者都不会自发发生)。
    //! 泄漏链:`drop(tx)` 不足让 rx 关闭(还有 tx2 clone)→ writer 永不结束
    //! → `run_proxy_side` 永挂 → 每次重连泄漏整套 ClientSession。
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// 可观测的"任务退出"探针:包一层 socket,drop 时计数。
    ///
    /// 回写任务持有 `Arc<UdpSocket>`;它退出即释放,Arc 计数归 0 时我们
    /// 通过外部持有的 Weak 观察到 upgraded() == None。
    #[tokio::test]
    async fn cancelled_session_task_exits_and_releases_socket() {
        let sock = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let weak = Arc::downgrade(&sock);
        let cancel = tokio_util::sync::CancellationToken::new();
        let (tx, _rx) = mpsc::channel::<Pending>(8);

        let sock2 = sock.clone();
        let tx2 = tx.clone();
        let c = cancel.clone();
        let done = Arc::new(AtomicUsize::new(0));
        let done2 = done.clone();
        let addr_str: Arc<str> = Arc::from("127.0.0.1:1");
        tokio::spawn(async move {
            let mut pkt: Option<Vec<u8>> = None;
            loop {
                let buf = pkt.get_or_insert_with(|| vec![0u8; UDP_RECV_BUF]);
                let n = tokio::select! {
                    r = sock2.recv_from(buf) => match r {
                        Ok((n, _)) => n,
                        Err(_) => break,
                    },
                    _ = c.cancelled() => break,
                };
                let framed = frame_udp_packet(&addr_str, &buf[..n]);
                if tx2.send(Pending { framed }).await.is_err() {
                    break;
                }
            }
            done2.fetch_add(1, Ordering::SeqCst);
            drop(sock2);
        });

        // 让任务跑起来(进入 recv await)
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(done.load(Ordering::SeqCst), 0, "任务不应自行退出");

        // 模拟 GC:取消
        cancel.cancel();
        for _ in 0..50 {
            if done.load(Ordering::SeqCst) == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(
            done.load(Ordering::SeqCst),
            1,
            "A4:取消后回写任务必须退出(否则 GC 只删 map 条目 = fd 泄漏)"
        );

        // 任务退出后 socket 引用应可回收(我们这份 drop 后 Weak 失效)
        drop(sock);
        for _ in 0..50 {
            if weak.upgrade().is_none() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(
            weak.upgrade().is_none(),
            "A4:任务退出后 socket 必须被释放,不得有残留强引用"
        );
    }

    /// 核心泄漏链验证:**只要会话任务被取消,writer 就能结束**。
    ///
    /// 修前的现象正是"drop(tx) 后 writer.await 永挂"。这里用一个受控
    /// 模型复现:回写任务持 tx clone,若不取消,即使 drop(tx) 队列也不关;
    /// 取消后它能退出并释放 clone,队列随之关闭、writer 收敛。
    #[tokio::test]
    async fn writer_converges_only_after_session_tasks_cancelled() {
        let (tx, rx) = mpsc::channel::<Pending>(8);
        let (_a, b) = tokio::io::duplex(1 << 16);
        let writer = tokio::spawn(writer_task(ChanWriter::Stream(Box::new(b)), rx));

        let cancel = tokio_util::sync::CancellationToken::new();
        let sock = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        // 模拟一个会话回写任务:持 tx clone
        let task_tx = tx.clone();
        let task_sock = sock.clone();
        let c = cancel.clone();
        let task = tokio::spawn(async move {
            let mut buf = vec![0u8; 2048];
            loop {
                tokio::select! {
                    r = task_sock.recv_from(&mut buf) => {
                        if r.is_err() { break; }
                    }
                    _ = c.cancelled() => break,
                }
            }
            drop(task_tx); // 退出即释放 clone
        });

        // 此时 drop 我们手里的 tx:队列**不会**关闭(任务还持一个 clone)
        drop(tx);
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            !writer.is_finished(),
            "会话任务仍持 tx clone 时 writer 不应结束 —— 这正是修前永挂的形态"
        );

        // 取消会话任务 → clone 释放 → 队列关闭 → writer 收敛
        cancel.cancel();
        task.await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), writer)
            .await
            .expect("A4:取消会话任务后 writer 必须收敛(修前会永挂)")
            .unwrap();
    }

    /// P0-2 / P0-5:sudp 访问器经 session.rs 交付 PrependedIo(RhMuxStream) 时,
    /// split_visitor_channel 必须命中零拷贝 Chunks 路径而非回落 tokio::io::split,
    /// 且非空前缀按序优先交付。
    #[tokio::test]
    async fn visitor_channel_prepended_mux_hits_chunks_lockfree() {
        use tokio::io::AsyncWriteExt;

        let (a, b) = tokio::io::duplex(64 * 1024);
        let server = rh_transport::wrap_stream_as_server(
            Box::new(b),
            true,
            rh_transport::MUX_STREAMS_UNLIMITED,
        );
        let client = rh_transport::wrap_stream_as_client(
            Box::new(a),
            true,
            rh_transport::MUX_STREAMS_UNLIMITED,
        );
        let srv = tokio::spawn(async move {
            let mut s = server.accept().await.unwrap();
            s.write_all(b"REST").await.unwrap();
        });

        let raw = client.open_stream().await.unwrap();
        let wrapped = rh_transport::PrependedIo::wrap(raw, b"PRE_".to_vec());
        let (r, w) = split_visitor_channel(wrapped);
        let mut mr = match (r, w) {
            (ChanReader::Chunks { r, .. }, ChanWriter::Chunks(_)) => r,
            _ => {
                panic!("PrependedIo(RhMuxStream) 必须命中 ChanReader::Chunks / ChanWriter::Chunks")
            }
        };
        let mut got = Vec::new();
        while got.len() < 8 {
            let c = mr.read_chunk().await.unwrap().expect("eof");
            got.extend_from_slice(&c);
        }
        assert_eq!(&got, b"PRE_REST");
        srv.await.unwrap();
    }

    #[tokio::test]
    async fn chan_reader_try_read_frame_drains_and_preserves_trailing_ping() {
        use tokio::io::AsyncWriteExt;

        // 1. Stream 路径
        let (mut w, r) = tokio::io::duplex(64 * 1024);
        let mut raw_bytes = bytes::BytesMut::new();
        for i in 0..3 {
            append_udp_packet(&mut raw_bytes, "10.0.0.1:1000", format!("p{i}").as_bytes());
        }
        raw_bytes.extend_from_slice(&framed_ping());
        w.write_all(&raw_bytes).await.unwrap();

        let mut cr = ChanReader::Stream(Box::new(r));
        let mut fbuf = read_buf_with_prefix(&[]);
        let (ty0, p0) = cr.read_frame(&mut fbuf).await.unwrap();
        assert_eq!(ty0, FrameType::UDP_PACKET);
        assert_eq!(udp_packet::decode(&p0).unwrap().1, b"p0");

        let mut drained = Vec::new();
        let mut trailing = None;
        while let Some((ty, p)) = cr.try_read_frame(&mut fbuf).await.unwrap() {
            if ty == FrameType::UDP_PACKET {
                drained.push(udp_packet::decode(&p).unwrap().1.to_vec());
            } else {
                trailing = Some(ty);
                break;
            }
        }
        assert_eq!(drained, vec![b"p1".to_vec(), b"p2".to_vec()]);
        assert_eq!(trailing, Some(FrameType::PING));
        assert!(cr.try_read_frame(&mut fbuf).await.unwrap().is_none());
        drop(w);

        // 2. Chunks 路径
        let (a, b) = tokio::io::duplex(64 * 1024);
        let server = rh_transport::wrap_stream_as_server(
            Box::new(b),
            true,
            rh_transport::MUX_STREAMS_UNLIMITED,
        );
        let client = rh_transport::wrap_stream_as_client(
            Box::new(a),
            true,
            rh_transport::MUX_STREAMS_UNLIMITED,
        );
        let srv = tokio::spawn(async move {
            let mut s = server.accept().await.unwrap();
            s.write_all(&raw_bytes).await.unwrap();
            s
        });
        let mut mr_box = client.open_stream().await.unwrap();
        let (mr, _mw) = rh_transport::mux::steal_mux_halves(&mut mr_box).unwrap();
        let mut cr = ChanReader::Chunks {
            r: mr,
            buf: read_buf_with_prefix(&[]),
        };
        let mut unused = bytes::BytesMut::new();
        let (ty0, p0) = cr.read_frame(&mut unused).await.unwrap();
        assert_eq!(ty0, FrameType::UDP_PACKET);
        assert_eq!(udp_packet::decode(&p0).unwrap().1, b"p0");
        let mut drained = Vec::new();
        let mut trailing = None;
        while let Some((ty, p)) = cr.try_read_frame(&mut unused).await.unwrap() {
            if ty == FrameType::UDP_PACKET {
                drained.push(udp_packet::decode(&p).unwrap().1.to_vec());
            } else {
                trailing = Some(ty);
                break;
            }
        }
        assert_eq!(drained, vec![b"p1".to_vec(), b"p2".to_vec()]);
        assert_eq!(trailing, Some(FrameType::PING));
        assert!(cr.try_read_frame(&mut unused).await.unwrap().is_none());
        let _keep_srv = srv.await.unwrap();
    }

    #[tokio::test]
    async fn writer_chunks_coalesces_queued_frames_preserving_wire_format() {
        use tokio::io::AsyncReadExt;

        let (a, b) = tokio::io::duplex(256 * 1024);
        let server = rh_transport::wrap_stream_as_server(
            Box::new(b),
            true,
            rh_transport::MUX_STREAMS_UNLIMITED,
        );
        let client = rh_transport::wrap_stream_as_client(
            Box::new(a),
            true,
            rh_transport::MUX_STREAMS_UNLIMITED,
        );
        let srv = tokio::spawn(async move {
            let mut s = server.accept().await.unwrap();
            let mut raw = Vec::new();
            s.read_to_end(&mut raw).await.unwrap();
            raw
        });

        let mut ch = client.open_stream().await.unwrap();
        let (_mr, mw) = rh_transport::mux::steal_mux_halves(&mut ch).unwrap();
        let (tx, rx) = mpsc::channel::<Pending>(128);
        let wh = tokio::spawn(writer_task(ChanWriter::Chunks(mw), rx));

        for i in 0..20usize {
            let addr = format!("10.2.0.1:{}", 3000 + i);
            tx.send(Pending {
                framed: frame_udp_packet(&addr, format!("c{i}").as_bytes()),
            })
            .await
            .unwrap();
            if i == 9 {
                tx.send(Pending {
                    framed: framed_ping(),
                })
                .await
                .unwrap();
            }
        }
        tx.send(Pending {
            framed: framed_pong(),
        })
        .await
        .unwrap();
        drop(tx);
        wh.await.unwrap();

        let raw = srv.await.unwrap();
        let mut off = 0usize;
        let mut pkts = Vec::new();
        let mut ping_pos = None;
        let mut pong_pos = None;
        let mut idx = 0usize;
        while off < raw.len() {
            assert_eq!(&raw[off..off + 4], b"RHTP");
            let ty = u16::from_be_bytes([raw[off + 5], raw[off + 6]]);
            let len = u32::from_be_bytes([raw[off + 7], raw[off + 8], raw[off + 9], raw[off + 10]])
                as usize;
            let payload = &raw[off + 11..off + 11 + len];
            if ty == FrameType::UDP_PACKET.0 {
                let (src, data) = udp_packet::decode(payload).unwrap();
                pkts.push((src.to_string(), data.to_vec()));
            } else if ty == FrameType::PING.0 {
                ping_pos = Some(idx);
            } else if ty == FrameType::PONG.0 {
                pong_pos = Some(idx);
            } else {
                panic!("unexpected frame type {ty}");
            }
            off += 11 + len;
            idx += 1;
        }
        assert_eq!(pkts.len(), 20);
        for (i, (src, data)) in pkts.iter().enumerate() {
            assert_eq!(src, &format!("10.2.0.1:{}", 3000 + i));
            assert_eq!(data, format!("c{i}").as_bytes());
        }
        assert_eq!(ping_pos, Some(10));
        assert_eq!(pong_pos, Some(21));
    }

    #[test]
    fn recv_batch_pool_reuses_and_returns_on_drop() {
        let pool = UdpRecvBatchPool::new();
        let b1 = pool.acquire();
        drop(b1);
        assert_eq!(pool.0.lock().unwrap().len(), 1);

        let mut held = Vec::new();
        for _ in 0..(RECV_BATCH_POOL_MAX + 3) {
            held.push(pool.acquire());
        }
        assert_eq!(pool.0.lock().unwrap().len(), 0);
        drop(held);
        assert_eq!(pool.0.lock().unwrap().len(), RECV_BATCH_POOL_MAX);
    }
}
