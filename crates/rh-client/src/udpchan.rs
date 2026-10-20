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

fn pong_bytes() -> Vec<u8> {
    serde_json::to_vec(&rh_protocol::message::Pong {
        ts_ms: 0,
        probe: true,
    })
    .unwrap()
}
fn ping_bytes() -> Vec<u8> {
    serde_json::to_vec(&rh_protocol::message::Ping {
        ts_ms: 0,
        probe: true,
    })
    .unwrap()
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

/// 待写帧:帧类型 + 已编码的 payload。
///
/// 注意这是**一帧的 payload**,不是"多包合并";批聚合发生在写任务里,
/// 由多个 [`Pending`] 拼成一次 `write_all`。
struct Pending {
    ty: FrameType,
    payload: Vec<u8>,
}

/// 唯一写任务:独占写半,从有界队列收帧并批聚合写出。
///
/// 返回即表示写半已关闭(对端断开或写错误),调用方据此终止整个通道。
async fn writer_task<W>(mut w: W, mut rx: mpsc::Receiver<Pending>)
where
    W: tokio::io::AsyncWrite + Unpin,
{
    // 复用同一个缓冲:每批清空重用,避免每包分配。
    let mut batch = bytes::BytesMut::with_capacity(BATCH_MAX * 128);
    loop {
        // 阻塞等第一帧(空队列时不空转;有界队列让 send 侧承担背压)。
        let Some(first) = rx.recv().await else {
            break; // 所有发送端已 drop = 通道结束
        };
        batch.clear();
        frame::encode_frame(&mut batch, first.ty, &first.payload);
        // 贪心多取:只在"队列里已经有货"时继续,绝不 sleep / 不额外等待,
        // 保持"有就发"的低延迟语义。try_recv 为空即刻收手。
        let mut n = 1;
        while n < BATCH_MAX {
            match rx.try_recv() {
                Ok(next) => {
                    frame::encode_frame(&mut batch, next.ty, &next.payload);
                    n += 1;
                }
                Err(_) => break,
            }
        }
        // 一次 write_all 写出 n 个**独立**帧(线格式与逐帧写完全一致)。
        if w.write_all(&batch).await.is_err() {
            break;
        }
    }
    let _ = w.shutdown().await;
}

/// 代理侧:通道 ↔ 本地目标 UDP。按用户源地址各开一个本地 socket,
/// 回写任务经唯一写任务写帧;主循环读帧 + 探活。
/// 每 60s 清理 60s 无活动的用户会话(源端口轮换的客户端会持续产生新条目)。
pub async fn run_proxy_side(channel: BoxedIo, spec: ProxySpec, prefix: Vec<u8>) {
    let target = format!("{}:{}", spec.target_host, spec.target_port);
    let Ok(target) = target.parse::<std::net::SocketAddr>() else {
        tracing::debug!(proxy = spec.name, "udp target unresolvable");
        return;
    };
    let (mut r, w) = tokio::io::split(channel);
    let (tx, rx) = mpsc::channel::<Pending>(WRITE_QUEUE);
    // 写任务独占写半:此后主循环与各回写任务都不再持有它,
    // 也就不存在"跨 await 持锁"和"全部会话等同一把锁"。
    let writer = tokio::spawn(writer_task(w, rx));

    let mut sessions: HashMap<std::net::SocketAddr, Session> = HashMap::new();
    // hello 后已抵达的帧数据预置进缓冲
    let mut fbuf = bytes::BytesMut::from(&prefix[..]);
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
            rr = frame::read_frame(&mut r, &mut fbuf) => {
                match rr {
                    Ok((FrameType::UDP_PACKET, p)) => {
                        if let Some((src, data)) = udp_packet::decode(&p) {
                            if let Some(user) = parse_addr_cached(src) {
                                if let Some(s) = get_session(&mut sessions, user, target, &tx).await {
                                    let _ = s.send_to(data, target).await;
                                }
                            }
                            last_alive = Instant::now();
                        }
                    }
                    Ok((FrameType::PING, _)) => {
                        // 只投递,不等待:写任务会按序写出(帧序由单队列保证)。
                        if tx.send(Pending { ty: FrameType::PONG, payload: pong_bytes() }).await.is_err() {
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
                    if tx.send(Pending { ty: FrameType::PING, payload: ping_bytes() }).await.is_err() {
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
    // 回写任务:本地目标响应 → 通道帧(src = 该用户源地址)
    let sock2 = sock.clone();
    let tx2 = tx.clone();
    tokio::spawn(async move {
        // 缓冲按会话首次收包时分配(不是建会话时就占 64 KiB):
        // 大量空闲会话不再各自钉住一页。
        let mut pkt: Option<Vec<u8>> = None;
        loop {
            let buf = pkt.get_or_insert_with(|| vec![0u8; UDP_RECV_BUF]);
            // A4:会话被 GC 取消(或整个通道退出)时必须能退出这个 await,
            // 否则任务永久持有 sock2/tx2 → writer.await 永挂。
            let n = tokio::select! {
                r = sock2.recv_from(buf) => match r {
                    Ok((n, _)) => n,
                    Err(_) => break,
                },
                _ = cancel.cancelled() => break,
            };
            // 地址字符串复用会话里那份,不再每包格式化。
            let payload = udp_packet::encode(&addr_str, &buf[..n]);
            if tx2
                .send(Pending {
                    ty: FrameType::UDP_PACKET,
                    payload,
                })
                .await
                .is_err()
            {
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

    // 用字符串首字节做槽位:同一源地址恒定落同槽。
    let slot = (src.as_bytes().first().copied().unwrap_or(0) as usize) % SLOTS;
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
pub async fn run_visitor_side(
    channel: BoxedIo,
    sock: Arc<UdpSocket>,
    cancel: tokio_util::sync::CancellationToken,
    pending: Vec<(std::net::SocketAddr, Vec<u8>)>,
) {
    let (mut r, w) = tokio::io::split(channel);
    let (tx, rx) = mpsc::channel::<Pending>(WRITE_QUEUE);
    let writer = tokio::spawn(writer_task(w, rx));
    // 先冲通道到位前缓冲的本地包(仍在写任务里按序写出)
    for (peer, data) in pending {
        let payload = udp_packet::encode(&peer.to_string(), &data);
        if tx
            .send(Pending {
                ty: FrameType::UDP_PACKET,
                payload,
            })
            .await
            .is_err()
        {
            drop(tx);
            let _ = writer.await;
            return;
        }
    }
    let mut fbuf = bytes::BytesMut::with_capacity(2048);
    // 按需分配:首次收包才占 64 KiB。
    let mut pkt: Option<Vec<u8>> = None;
    let mut last_alive = Instant::now();
    let mut last_ping = Instant::now();
    // 同 run_proxy_side:探活用固定周期 interval,不随事件重置。
    let mut probe = tokio::time::interval(Duration::from_secs(15));
    probe.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let _ = probe.tick().await;
    loop {
        let buf = pkt.get_or_insert_with(|| vec![0u8; UDP_RECV_BUF]);
        tokio::select! {
            rr = sock.recv_from(buf) => {
                let Ok((n, peer)) = rr else { break };
                let payload = udp_packet::encode(&peer.to_string(), &buf[..n]);
                if tx.send(Pending { ty: FrameType::UDP_PACKET, payload }).await.is_err() {
                    break;
                }
            }
            rr = frame::read_frame(&mut r, &mut fbuf) => {
                match rr {
                    Ok((FrameType::UDP_PACKET, p)) => {
                        if let Some((src, data)) = udp_packet::decode(&p) {
                            if let Some(addr) = parse_addr_cached(src) {
                                let _ = sock.send_to(data, addr).await;
                            }
                            last_alive = Instant::now();
                        }
                    }
                    Ok((FrameType::PING, _)) => {
                        if tx.send(Pending { ty: FrameType::PONG, payload: pong_bytes() }).await.is_err() {
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
                    if tx.send(Pending { ty: FrameType::PING, payload: ping_bytes() }).await.is_err() {
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
        let h = tokio::spawn(writer_task(b, rx));
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
            ty: FrameType::UDP_PACKET,
            payload: udp_packet::encode(addr, data),
        }
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
            ty: FrameType::PING,
            payload: ping_bytes(),
        })
        .await
        .unwrap();
        tx.send(pkt("1.2.3.4:5", seq[1].as_bytes())).await.unwrap();
        tx.send(Pending {
            ty: FrameType::PONG,
            payload: pong_bytes(),
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
        let h = tokio::spawn(writer_task(b, rx));

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
                let payload = udp_packet::encode(&addr_str, &buf[..n]);
                if tx2
                    .send(Pending {
                        ty: FrameType::UDP_PACKET,
                        payload,
                    })
                    .await
                    .is_err()
                {
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
        let writer = tokio::spawn(writer_task(b, rx));

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
}
