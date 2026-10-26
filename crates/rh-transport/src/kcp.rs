//! KCP 载体:UDP 之上的可靠有序字节流(会话),会话之上再走 yamux 多路复用。
//!
//! - 服务端:单 UDP 端口,kcp 头部的 conv 字段做会话分离;未知 conv 的
//!   合法 kcp 报文触发建会话并交给 accept 队列。
//! - 客户端:随机 conv,直接开始发送(首包即隐式握手)。
//! - 会话驱动:事件(用户写 notify / 入站批 recv_many)+ 2ms tick 调用
//!   kcp.update;有新写入/新入包/待发数据时立即 flush(kcp 0.6.0
//!   interval 下限 10ms,等 interval 会把写延迟放大到毫秒级)。同一
//!   唤醒批量消费已到达入包(≤BATCH_CAP,有 1 条即返回不凑满),
//!   整批仅 1×update+1×flush。SESSION_IDLE_TIMEOUT 内无「进展」则
//!   回收:无待交付时进展=任何入包;有待交付时进展=应用实际消费
//!   (活跃慢消费者受保护,无人消费的滞留数据不再无限续命)。
//! - 有界性(两个方向,均为站立不变量而非每轮配额):
//!   发送侧 = 前端 write_queue(条数/字节上限)+ KCP 待发
//!   (MAX_KCP_PENDING_SEGS,按 kcp.wait_snd() 即 snd_buf+snd_queue
//!   段数),只有对端 ACK 使 wait_snd 下降后才会继续搬运;
//!   接收侧 = read_buf(MAX_READ_BUF),超出时数据留在 kcp rcv_queue,
//!   rcv 窗口(wnd_unused)自动收敛为 0 通知对端停发,不丢应用数据。
//! - UDP 输出:全部经同一积压队列由唯一 drain 任务串行发送(FIFO 由
//!   构造保证,无直发插队);kernel 缓冲满(WouldBlock)有界排队、
//!   超限按容量策略丢包计数(kcp RTO 重传);真实错误/短写致命化,
//!   记录后经 kcp flush 传播终止会话并浮出到 IO,不静默吞掉。connect
//!   与 NAT 打洞(punch)输出复用同一 backlog/drain 状态机:打洞
//!   Rotating 阶段每报发往弹出时刻的候选地址,learn 后锁定;
//!   WouldBlock 入队补送(至多一次),假成功丢报不存在。
//! - 半关闭:poll_shutdown 只关本地写侧 —— 交付完成(队列排空 + 全部
//!   段被确认)即返回 Ok,会话继续服务读侧直到正常 EOF/idle/错误;
//!   成功关写不再连带终止读端。
//! - 生命周期:每个 spawn 的任务都有 owner 与退出路径 —— 会话任务随
//!   用户句柄归零/通道关闭/错误退出;drain 任务随生产者(UdpOutput)
//!   释放或致命错误退出;各接收循环 select 会话 shutdown 通知退出,
//!   不在会话结束后挂起 recv 持有 socket。

use crate::mux::{wrap_stream_as_client, wrap_stream_as_server, MUX_STREAMS_UNLIMITED};
use crate::{
    other_err, ClientTransportConfig, Incoming, Multiplexer, Protocol, ServerTransportConfig,
};
use kcp::{Kcp, KCP_OVERHEAD};
use std::collections::{HashMap, VecDeque};
use std::io::Write;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::UdpSocket;
use tokio::sync::{mpsc, Notify};

// 2ms tick:interval=10ms 时吞吐被「窗口/间隔」封顶(256包×1200B/10ms ≈ 240Mbps),
// 低 RTT 内网需要更细粒度 flush;CPU 代价可接受(每会话一个轻量定时器)
const TICK: Duration = Duration::from_millis(2);
const MTU: usize = 1200;
const SESSION_IDLE_TIMEOUT: Duration = Duration::from_secs(90);
const MAX_WRITE_QUEUE: usize = 256;
/// 写队列字节上限:仅按条数限制时,大帧(16KiB+)可让单会话排队内存失控
/// (256 条 × 16KiB = 4MiB/会话)。与条数上限共同生效。
const MAX_WRITE_QUEUE_BYTES: usize = 256 * 1024;
/// 单次喂给 kcp.send 的最大片:kcp 0.6.0 stream 模式下 count >= KCP_WND_RCV(128)
/// 会返回 UserBufTooBig,128×mss(1176)≈ 148KiB;64KiB(count=56)留足余量。
const SEND_PIECE: usize = 64 * 1024;
/// KCP 待发段数上限(snd_buf + snd_queue,站立不变量):达到后不再从
/// 前端 write_queue 搬运,只有对端 ACK 使 wait_snd 下降才继续。取
/// 2×snd_wnd(256):snd_buf 可保持满窗不饿,同时发送路径总内存有界
/// (前端 256KiB + 512×1176B ≈ 858KiB/会话)。未 ACK 也持续 accept
/// 的旧缺陷(前端限额被无条件搬到无界 snd_queue)由此闭合。
const MAX_KCP_PENDING_SEGS: usize = 512;
/// 读缓冲字节上限:超出时数据留在 kcp rcv_queue,rcv 窗口收敛为 0,
/// 对端 kcp 停发(协议流控),不静默丢应用数据。读端消费后由
/// poll_read 低水位 notify 唤醒会话继续 recv + flush 窗口重开 ACK。
const MAX_READ_BUF: usize = 256 * 1024;
const READ_LOW_WATER: usize = MAX_READ_BUF / 2;
/// UDP 发送积压上限(条数/字节)。kernel 发送缓冲满时的有界 FIFO,
/// 超出即丢弃并计数(kcp RTO 会重传),不允许无界堆积。
const MAX_BACKLOG_PACKETS: usize = 128;
const MAX_BACKLOG_BYTES: usize = 64 * 1024;
/// drain 任务空闲/受阻时的存活复查退避:生产者 drop 有 Drop 通知
/// (立即退出),该定时仅兜底无生产者路径(测试/异常构造),上限 500ms。
const BACKLOG_RECHECK_MAX: Duration = Duration::from_millis(500);

/// KCP 内部时钟(毫秒)。**必须单调**:KCP 用它对 RTO/重传/flush 做差分,
/// 墙钟回拨(NTP 校时、手工改时间)会让 `now - last` 下溢,重传计时停滞。
/// 以进程启动时刻为锚取 `Instant` 差值(u32 毫秒约 49.7 天回绕,KCP 本身
/// 按 `wrapping_sub` 处理差分,周期回绕安全)。
fn now_ms() -> u32 {
    static START: std::sync::OnceLock<std::time::Instant> = std::sync::OnceLock::new();
    let start = START.get_or_init(std::time::Instant::now);
    start.elapsed().as_millis() as u32
}

// ---------------- 诊断计数(零行为变更,仅观测) ----------------

/// KCP 诊断计数快照(测试/基准可读,Relaxed 语义):
/// 会话任务唤醒轮数、入包消费数、kcp input/update/flush 调用数、
/// UDP send 尝试(fast-path 直发 + drain 队列发送)与 recvfrom 次数。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct KcpCounters {
    pub wakeups: u64,
    pub packets_consumed: u64,
    pub input_calls: u64,
    pub update_calls: u64,
    pub flush_calls: u64,
    pub send_attempts: u64,
    pub recvfrom_calls: u64,
}

struct KcpCountersStatic {
    wakeups: AtomicU64,
    packets_consumed: AtomicU64,
    input_calls: AtomicU64,
    update_calls: AtomicU64,
    flush_calls: AtomicU64,
    send_attempts: AtomicU64,
    recvfrom_calls: AtomicU64,
}

impl KcpCountersStatic {
    const fn new() -> Self {
        Self {
            wakeups: AtomicU64::new(0),
            packets_consumed: AtomicU64::new(0),
            input_calls: AtomicU64::new(0),
            update_calls: AtomicU64::new(0),
            flush_calls: AtomicU64::new(0),
            send_attempts: AtomicU64::new(0),
            recvfrom_calls: AtomicU64::new(0),
        }
    }
    fn snapshot(&self) -> KcpCounters {
        KcpCounters {
            wakeups: self.wakeups.load(Ordering::Relaxed),
            packets_consumed: self.packets_consumed.load(Ordering::Relaxed),
            input_calls: self.input_calls.load(Ordering::Relaxed),
            update_calls: self.update_calls.load(Ordering::Relaxed),
            flush_calls: self.flush_calls.load(Ordering::Relaxed),
            send_attempts: self.send_attempts.load(Ordering::Relaxed),
            recvfrom_calls: self.recvfrom_calls.load(Ordering::Relaxed),
        }
    }
    fn reset(&self) {
        self.wakeups.store(0, Ordering::Relaxed);
        self.packets_consumed.store(0, Ordering::Relaxed);
        self.input_calls.store(0, Ordering::Relaxed);
        self.update_calls.store(0, Ordering::Relaxed);
        self.flush_calls.store(0, Ordering::Relaxed);
        self.send_attempts.store(0, Ordering::Relaxed);
        self.recvfrom_calls.store(0, Ordering::Relaxed);
    }
}

static COUNTERS: KcpCountersStatic = KcpCountersStatic::new();

/// 进程级 KCP 诊断计数快照(基准/bench 用;进程内全部会话聚合)。
pub fn counters() -> KcpCounters {
    COUNTERS.snapshot()
}

/// 清零进程级诊断计数(基准测量窗起点)。
pub fn counters_reset() {
    COUNTERS.reset()
}

/// 递增进程级诊断计数:仅在 `perf-counters` 特性下编译,生产构建
/// 完全移除 —— 这些递增全部落在每包热路径,对同一 static 缓存行的
/// fetch_add 在跨核多会话下会乒乓,而生产二进制没有任何读者。
macro_rules! bump_counter {
    ($field:ident) => {
        #[cfg(feature = "perf-counters")]
        COUNTERS.$field.fetch_add(1, Ordering::Relaxed);
    };
}

/// 同一唤醒批量消费入站包的上限(≤通道容量 256):单轮工作量 =
/// 64×input(O(包长)) + 1 update + ≤256KiB recv + 1 flush,天然有界;
/// recv_many 有 ≥1 条即返回,不凑满、不加等待。
const BATCH_CAP: usize = 64;

/// 队列内字节总数(条数上限之外的第二个容量维度)。
fn backlog_bytes(q: &VecDeque<Vec<u8>>) -> usize {
    q.iter().map(|p| p.len()).sum()
}

// ---------------- 会话 IO(用户侧字节流) ----------------

/// write_queue 条目:缓冲 + 消费游标(A-2)。
///
/// 修前每片 `split_off(piece_len)` 切尾:一次新分配 + 尾段整块 memcpy。
/// 预算受限(慢 ACK/丢包链路使 wait_snd 居高)时对同一 64KiB 项按每轮
/// 1 段消费,尾拷贝总量按片数平方放大(实测口径 ≈28×),恰发生在拥塞
/// 恢复最不想烧 CPU 的时刻。游标只前移,零额外分配/拷贝。
#[derive(Default)]
struct WriteChunk {
    buf: Vec<u8>,
    off: usize,
}

impl WriteChunk {
    fn new(buf: Vec<u8>) -> Self {
        Self { buf, off: 0 }
    }

    fn remaining(&self) -> usize {
        self.buf.len() - self.off
    }
}

struct Guarded {
    read_buf: VecDeque<u8>,
    read_waker: Option<Waker>,
    write_queue: VecDeque<WriteChunk>,
    /// write_queue 中排队字节总数(与条数上限共同做回压)
    write_bytes: usize,
    write_waker: Option<Waker>,
    closed: bool,
    /// 显式半关闭已请求:会话在「队列排空 + kcp 全部确认」后置 closed。
    /// 裸 drop(未 shutdown)不置位,走 handles==0 即时取消 ——
    /// 两种语义刻意分离:显式成功关闭保证交付,取消不保证。
    closing: bool,
    /// 本地写侧已成功关闭(A):显式 shutdown 完成(队列排空 + wait_snd==0)
    /// 时置位并唤醒等待者,但会话**继续**服务读侧(入包交付/窗口 ACK/
    /// dead_link/idle/句柄归零照常)—— 成功关写不得杀读侧。`closed`
    /// 仅会话真终止(错误/idle/句柄归零/通道关闭)时置位,之后读端
    /// 排空存量才见 EOF。
    write_closed: bool,
    /// 应用消费进展(B):poll_read 实际消费 n>0 字节时自增。会话侧以
    /// 「consume_seq 是否变化」作为有待交付数据时的唯一活跃证据,
    /// 无人消费的 read_buf 不再无限刷新空闲时钟。
    consume_seq: u64,
    /// 已接受写序号(poll_write/punch 哨兵每入队一段递增)
    write_seq: u64,
    /// 会话侧完成 flush(数据已交给 kcp 输出)的序号,poll_flush 依据
    flushed_seq: u64,
    /// 会话终止原因(真实错误):浮出到 poll_read/poll_write/poll_shutdown,
    /// 不吞真实错误。None = 正常关闭(读端先排空再 EOF)。
    error: Option<String>,
}

impl Guarded {
    fn wake_read(&mut self) {
        if let Some(w) = self.read_waker.take() {
            w.wake();
        }
    }
    fn wake_write(&mut self) {
        if let Some(w) = self.write_waker.take() {
            w.wake();
        }
    }

    /// A-2:按段预算从 write_queue 切出下一批 piece(锁内调用;
    /// kcp.send 由调用方在锁外执行,N6)。游标前移消费、消费尽才弹
    /// 条目 —— 取代修前的 split_off 切尾(每片一次新分配 + 尾段
    /// memcpy,预算受限时对同一大项按每轮 1 段消费,尾拷贝平方放大)。
    fn take_send_pieces(
        &mut self,
        budget_segs: usize,
        mss: usize,
        piece_cap: usize,
    ) -> std::collections::VecDeque<Vec<u8>> {
        let mut pieces = std::collections::VecDeque::new();
        let mut budget = budget_segs;
        while budget > 0 {
            let Some(front) = self.write_queue.front_mut() else {
                break;
            };
            // 单片 ≤ 余量×mss(≥mss,不产生微片),使段预算算术成立
            let piece_len = front
                .remaining()
                .min(piece_cap)
                .min(budget.saturating_mul(mss));
            let piece = front.buf[front.off..front.off + piece_len].to_vec();
            front.off += piece_len;
            self.write_bytes = self.write_bytes.saturating_sub(piece_len);
            if front.remaining() == 0 {
                self.write_queue.pop_front();
            }
            // 该片将占用的段数(整 mss 切段、末段取余),从预算扣减
            budget -= piece_len.div_ceil(mss);
            pieces.push_back(piece);
        }
        pieces
    }
}

/// 会话共享状态:notify 独立于锁,避免借用临时 guard。
/// handles 记录在世的 KcpIo 用户句柄数,归零即令会话任务退出
/// (避免服务端丢弃连接后会话空转至 90s 空闲超时)。
/// shutdown 在会话任务收尾 notify_one:唤醒挂起的接收循环随会话退出,
/// 不再持有 socket(permit 语义:接收循环未就绪也能补投)。
struct Shared {
    guarded: Mutex<Guarded>,
    notify: Notify,
    shutdown: Notify,
    handles: AtomicUsize,
    /// 本会话诊断计数(测试隔离读,避免进程级计数被并行测试污染)。
    /// B3:唯一读者是测试 → 收进 cfg(test),生产不再为它付每包
    /// 5-6 次原子 RMW(并与 Shared 其它字段共享 cacheline)。
    #[cfg(test)]
    stats: KcpCountersStatic,
    /// 入站批缓冲容量高水位(D 守护:跨轮复用不增长;仅测试读)
    #[cfg(test)]
    batch_cap_hwm: AtomicU64,
    /// A-3 输出聚合批(Linux;None = 未启用)。会话任务每轮迭代尾
    /// flush_now 批量提交本轮 kcp 输出。
    send_agg: Option<Arc<SendAgg>>,
}

/// KCP 会话的字节流句柄。
pub struct KcpIo {
    shared: Arc<Shared>,
}

impl Drop for KcpIo {
    fn drop(&mut self) {
        if self.shared.handles.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.shared.notify.notify_one();
        }
    }
}

impl AsyncRead for KcpIo {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        // 零容量读必须立即就绪:与空写(Ok(0))对称,注册 waker 等待
        // 永远不会到来的数据会把调用方(如探测读)挂死。
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        let mut s = self.shared.guarded.lock().unwrap();
        if !s.read_buf.is_empty() {
            let n = s.read_buf.len().min(buf.remaining());
            if n > 0 {
                // as_slices 双段直拷进 ReadBuf:不再 drain→collect 出临时
                // Vec(旧实现每次 read 一次分配+双倍拷贝)
                let (a, b) = s.read_buf.as_slices();
                let an = a.len().min(n);
                buf.put_slice(&a[..an]);
                if an < n {
                    buf.put_slice(&b[..n - an]);
                }
                s.read_buf.drain(..n);
                // B:应用消费进展记账(有待交付时的唯一活跃证据)
                s.consume_seq += 1;
            }
            let below_low_water = s.read_buf.len() < READ_LOW_WATER;
            drop(s);
            if below_low_water {
                // 读端腾出空间:唤醒会话继续 kcp.recv + flush
                // 窗口重开 ACK(对端才可能恢复发送)。
                self.shared.notify.notify_one();
            }
            Poll::Ready(Ok(()))
        } else if let Some(e) = s.error.clone() {
            Poll::Ready(Err(other_err(e)))
        } else if s.closed {
            Poll::Ready(Ok(()))
        } else {
            s.read_waker = Some(cx.waker().clone());
            Poll::Pending
        }
    }
}

impl AsyncWrite for KcpIo {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let mut s = self.shared.guarded.lock().unwrap();
        if let Some(e) = s.error.clone() {
            return Poll::Ready(Err(other_err(e)));
        }
        if s.closed {
            return Poll::Ready(Err(other_err("kcp session closed")));
        }
        // closing 或写侧已成功关闭(A):继续写必须报错(半关闭写端语义)
        if s.closing || s.write_closed {
            return Poll::Ready(Err(other_err("kcp session shutting down")));
        }
        // 空写与空读一致:立即 Ok(0),且不占用队列槽位/字节额度。
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }
        // 槽位或字节额度耗尽才回压;否则按剩余额度接受前缀
        // (partial write,AsyncWrite 契约允许 n < buf.len())。
        // 修复前:空队列写 buf.len() > 上限也返回 Pending 且一个字节
        // 都不入队,同 buffer 永远无法推进。
        if s.write_queue.len() >= MAX_WRITE_QUEUE || s.write_bytes >= MAX_WRITE_QUEUE_BYTES {
            s.write_waker = Some(cx.waker().clone());
            return Poll::Pending;
        }
        let accept = buf.len().min(MAX_WRITE_QUEUE_BYTES - s.write_bytes);
        s.write_bytes += accept;
        s.write_seq += 1;
        s.write_queue
            .push_back(WriteChunk::new(buf[..accept].to_vec()));
        // 入队即唤醒会话任务:不等 2ms tick。kcp 0.6.0 的 flush 受
        // ts_flush(interval 下限 10ms)门控,写若只靠 tick 拾取 +
        // interval flush,单次写延迟可被放大到 10ms 级(RTT/吞吐同受
        // 其害)。Notify 许可合并,多次写只触发一轮拾取,不会忙等。
        // A-8:先 drop 锁再唤醒(与 poll_flush 同款纪律)——被唤醒的
        // 会话任务立刻取同一把锁,持锁 notify 只让它空等一次。
        drop(s);
        self.shared.notify.notify_one();
        Poll::Ready(Ok(accept))
    }
    /// N1(B-2 的 kcp 半边):向量写透传。修前落到 tokio 默认实现——
    /// 只写第一个非空 slice,YamuxCarrier 的批量写(poll_writev,
    /// ≤33 段)在 kcp 载体上整批退化:每段一次同步锁 + to_vec 分配
    /// (12B 帧头也分配)+ notify,批聚合完全失效。与 WsIo 的 B-2
    /// 修复对齐:多段拼成单条队列项,一次锁、一次分配、一次拷贝。
    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[std::io::IoSlice<'_>],
    ) -> Poll<std::io::Result<usize>> {
        let total: usize = bufs.iter().map(|b| b.len()).sum();
        if total == 0 {
            return Poll::Ready(Ok(0));
        }
        let mut s = self.shared.guarded.lock().unwrap();
        if let Some(e) = s.error.clone() {
            return Poll::Ready(Err(other_err(e)));
        }
        if s.closed {
            return Poll::Ready(Err(other_err("kcp session closed")));
        }
        if s.closing || s.write_closed {
            return Poll::Ready(Err(other_err("kcp session shutting down")));
        }
        if s.write_queue.len() >= MAX_WRITE_QUEUE || s.write_bytes >= MAX_WRITE_QUEUE_BYTES {
            s.write_waker = Some(cx.waker().clone());
            return Poll::Pending;
        }
        let accept = total.min(MAX_WRITE_QUEUE_BYTES - s.write_bytes);
        let mut combined = Vec::with_capacity(accept);
        for b in bufs {
            let remain = accept - combined.len();
            if remain == 0 {
                break;
            }
            let take = b.len().min(remain);
            combined.extend_from_slice(&b[..take]);
        }
        s.write_bytes += accept;
        s.write_seq += 1;
        s.write_queue.push_back(WriteChunk::new(combined));
        // 与 poll_write 同因:入队即唤醒会话任务,不等 2ms tick;
        // 先 drop 锁再唤醒(A-8,同 poll_write)
        drop(s);
        self.shared.notify.notify_one();
        Poll::Ready(Ok(accept))
    }

    fn is_write_vectored(&self) -> bool {
        // 多段拼成单条队列项,能力位如实上报
        true
    }
    /// flush 契约:成功返回 = 此前全部已接受写都已交给 kcp 输出
    /// (write_queue 排空且会话完成了一轮 flush)。修复前无条件
    /// Ready 是契约缺口(tokio AsyncWrite flush 成功隐含写出完成)。
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        let mut s = self.shared.guarded.lock().unwrap();
        if let Some(e) = s.error.clone() {
            return Poll::Ready(Err(other_err(e)));
        }
        if s.write_queue.is_empty() && s.write_seq <= s.flushed_seq {
            return Poll::Ready(Ok(()));
        }
        // 真终止兜底:会话已亡(closed),flushed_seq 不会再被推进,
        // 未完成的 flush 契约不得永远 Pending(错误路径已被上方 error
        // 分支截获;此处覆盖剩余 corner,如通道关闭时写已入队未搬运)。
        if s.closed {
            return Poll::Ready(Err(other_err("kcp session closed before flush completion")));
        }
        s.write_waker = Some(cx.waker().clone());
        let need_kick = !s.write_queue.is_empty();
        drop(s);
        // 仅在有排队数据需要会话搬运时通知(否则可能已在节流等待 ACK,
        // 重复 notify 只会造成空转唤醒)。
        if need_kick {
            self.shared.notify.notify_one();
        }
        Poll::Pending
    }
    /// 显式半关闭(只关本地写侧,A):置 closing 后由会话在
    /// 「write_queue 排空 + kcp wait_snd()==0(全部段被对端确认,
    /// 即真实对端已完整收到)」时置 write_closed 并唤醒本 poll →
    /// Ready(Ok)。此后**读侧继续可用**:对端仍可发数据,会话照常
    /// 交付/ACK,直到正常 EOF(读端排空后 closed)、idle 超时或错误。
    /// 正常路径不加强制超时;对端死亡经重传 dead_link 以 Err 浮出,
    /// 不吞错误。裸 drop 不走此路径(handles==0 即时取消,不保证交付)。
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        let mut s = self.shared.guarded.lock().unwrap();
        if let Some(e) = s.error.clone() {
            return Poll::Ready(Err(other_err(e)));
        }
        if s.closed {
            return Poll::Ready(Ok(()));
        }
        if s.write_closed {
            return Poll::Ready(Ok(()));
        }
        let newly_closing = !s.closing;
        s.closing = true;
        s.write_waker = Some(cx.waker().clone());
        drop(s);
        // 仅状态迁移时通知:重复 poll 重复 notify 会与每轮 wake_write
        // 形成忙环(见 run_session)。
        if newly_closing {
            self.shared.notify.notify_one();
        }
        Poll::Pending
    }
}

// ---------------- kcp 输出 → UDP ----------------

/// UDP 发送积压诊断快照(测试与日志可读,不是无人观察的 private atomic)。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BacklogStats {
    /// 成功入队次数
    pub enqueued: u64,
    /// 从队列实际发出次数
    pub sent: u64,
    /// 容量满丢弃次数(容量策略,kcp RTO 重传兜底)
    pub dropped_full: u64,
    /// 真实错误/短写致命次数(传播终止会话)
    pub fatal: u64,
    /// 当前排队条数/字节与在途(已弹出未发出)字节
    pub queued_packets: usize,
    pub queued_bytes: usize,
    pub inflight_bytes: usize,
}

/// UDP 发送积压:kernel 发送缓冲满(WouldBlock)时的有界 FIFO。
///
/// - 唯一串行发送所有权:所有数据报(含队首在途的)一律入队,由
///   drain 任务按序发出,FIFO 由构造保证,不存在直发插队路径。
/// - 容量(条数+字节)对「排队中」计算;在途包(已弹出正在发送)
///   腾出的额度允许生产者使用;WouldBlock 回插时在途包重新计入,
///   超限则从队尾(最新)逐包丢弃至放得下,保住队头 FIFO。
/// - 真实错误(非 WouldBlock)与短写:致命化 —— 记录错误、计数、
///   drain 收敛退出;后续生产者写出错经 kcp flush 传播终止会话并
///   浮出到 IO。不再「计数后 continue 吞掉」。
struct UdpBacklog {
    state: Mutex<VecDeque<Vec<u8>>>,
    notify: Notify,
    /// 诊断计数(测试与日志用):入队 / 从队列发出 / 满丢 / 致命
    enqueued: AtomicU64,
    sent: AtomicU64,
    dropped_full: AtomicU64,
    fatal: AtomicU64,
    /// 在途包记账(已 pop 未终结):pop 记增,发出/丢弃/回插记减。
    inflight_bytes: AtomicUsize,
    inflight_packets: AtomicUsize,
    fatal_error: Mutex<Option<String>>,
    /// A-6:fatal 一次性置位永不清除,热路径(每输出一次的输出回调)先读
    /// 原子再决定是否取锁拿错误串。
    fatal_flag: std::sync::atomic::AtomicBool,
    /// 会话错误桥:drain 遇真实输出错误时,直接把错误写进会话 IO 并
    /// 唤醒会话/接收循环 —— idle 会话(无后续写/待发数据)不必等下一
    /// 次 UdpOutput::write 才感知。Weak:不延长会话生命周期。
    session: Mutex<Option<std::sync::Weak<Shared>>>,
    /// A-3 输出聚合批注册槽(生产 Linux 会话经 SendAgg::new 注册;
    /// SendAgg 持 Weak 反向引用,无 Arc 环)。未注册 = 逐包直发。
    agg_slot: std::sync::OnceLock<Arc<SendAgg>>,
}

impl UdpBacklog {
    fn new() -> Self {
        Self {
            state: Mutex::new(VecDeque::new()),
            notify: Notify::new(),
            enqueued: AtomicU64::new(0),
            sent: AtomicU64::new(0),
            dropped_full: AtomicU64::new(0),
            fatal: AtomicU64::new(0),
            inflight_bytes: AtomicUsize::new(0),
            inflight_packets: AtomicUsize::new(0),
            fatal_error: Mutex::new(None),
            fatal_flag: std::sync::atomic::AtomicBool::new(false),
            session: Mutex::new(None),
            agg_slot: std::sync::OnceLock::new(),
        }
    }

    /// A-3:注册的输出聚合批(生产 Linux 会话;未注册 = 逐包路径)。
    fn agg(&self) -> Option<&Arc<SendAgg>> {
        self.agg_slot.get()
    }

    /// 绑定会话(生产路径每个 backlog 恰好一次,spawn drain 前调用)。
    fn attach_session(&self, shared: &Arc<Shared>) {
        *self.session.lock().unwrap() = Some(Arc::downgrade(shared));
    }

    /// 把致命输出错误送进会话:置 Guarded.error + closed、唤醒全部
    /// IO 等待者、唤醒会话任务收尾、通知接收循环退出。不发送任何
    /// dummy 唤醒包。会话已消亡时(upgrade 失败)静默跳过。
    fn notify_session_fatal(&self, msg: &str) {
        let shared = self
            .session
            .lock()
            .unwrap()
            .as_ref()
            .and_then(|w| w.upgrade());
        if let Some(shared) = shared {
            {
                let mut s = shared.guarded.lock().unwrap();
                if s.error.is_none() {
                    s.error = Some(msg.to_string());
                }
                s.closed = true;
                s.wake_read();
                s.wake_write();
            }
            shared.notify.notify_one();
            shared.shutdown.notify_one();
        }
    }

    /// 诊断快照:测试断言与退出日志用。
    fn stats(&self) -> BacklogStats {
        let q = self.state.lock().unwrap();
        BacklogStats {
            enqueued: self.enqueued.load(Ordering::Relaxed),
            sent: self.sent.load(Ordering::Relaxed),
            dropped_full: self.dropped_full.load(Ordering::Relaxed),
            fatal: self.fatal.load(Ordering::Relaxed),
            queued_packets: q.len(),
            queued_bytes: backlog_bytes(&q),
            inflight_bytes: self.inflight_bytes.load(Ordering::Relaxed),
        }
    }

    #[cfg(test)]
    fn bytes(&self) -> usize {
        backlog_bytes(&self.state.lock().unwrap())
    }

    fn is_empty(&self) -> bool {
        self.state.lock().unwrap().is_empty()
    }

    fn queued_packets(&self) -> usize {
        self.state.lock().unwrap().len()
    }

    /// 快路径判定:无排队且无在途(此刻不存在可被插队的包)。
    /// 顺序必须是先(锁内)确认队列为空、再读在途:若 drain 刚弹出
    /// 队首,其锁内在途记账与本判定的锁读取构成同步——队列为空可见
    /// 时在途 >0 必然已可见,不会漏判插队。
    fn is_idle(&self) -> bool {
        self.is_empty() && self.inflight_bytes.load(Ordering::Relaxed) == 0
    }

    /// A-6:快路径读一次性置位的原子标志(置位点见 note_fatal;
    /// 永不清除,Acquire 与 store Release 配对)。
    fn is_fatal(&self) -> bool {
        self.fatal_flag.load(Ordering::Acquire)
    }

    fn fatal_error(&self) -> Option<String> {
        self.fatal_error.lock().unwrap().clone()
    }

    /// 记录致命错误(首个记 warn 日志,后续幂等);唤醒 drain 收敛;
    /// 经错误桥立即送达会话(idle 会话不等待下一次输出)。
    fn note_fatal(&self, msg: String, pkt_len: usize) {
        self.fatal_flag.store(true, Ordering::Release);
        {
            let mut g = self.fatal_error.lock().unwrap();
            if g.is_none() {
                tracing::warn!(error = %msg, "kcp udp backlog fatal send error");
                self.notify_session_fatal(&msg);
            }
            *g = Some(msg);
        }
        self.fatal.fetch_add(1, Ordering::Relaxed);
        self.inflight_bytes.fetch_sub(pkt_len, Ordering::Relaxed);
        self.inflight_packets.fetch_sub(1, Ordering::Relaxed);
        self.notify.notify_one();
    }

    /// 生产者入队。容量按「排队中 + 在途」总预算(条数与字节):
    /// 在途包(drain 已弹出、正在发送)不腾出额度 —— 否则合法交错中
    /// queued+inflight 可超上限。超出丢弃并计数(kcp RTO 重传);
    /// 成功入队后唤醒 drain。在途增量在持锁内发生(pop),本处在持锁后
    /// 读在途原子;发出/致命路径的在途减量只会让预算变小(方向安全)。
    fn push(&self, pkt: &[u8]) -> bool {
        let mut q = self.state.lock().unwrap();
        let inflight_b = self.inflight_bytes.load(Ordering::Relaxed);
        let inflight_p = self.inflight_packets.load(Ordering::Relaxed);
        if q.len() + inflight_p + 1 > MAX_BACKLOG_PACKETS
            || backlog_bytes(&q) + inflight_b + pkt.len() > MAX_BACKLOG_BYTES
        {
            drop(q);
            self.dropped_full.fetch_add(1, Ordering::Relaxed);
            tracing::debug!(
                len = pkt.len(),
                "kcp udp backlog full: drop (kcp retransmits)"
            );
            return false;
        }
        q.push_back(pkt.to_vec());
        drop(q);
        self.enqueued.fetch_add(1, Ordering::Relaxed);
        self.notify.notify_one();
        true
    }

    /// drain 弹出队首并发送(在途记账 +1);队列为空返回 None。
    /// 在途记账必须在持锁内完成:释放锁后「队列为空」才可能被生产者
    /// 看到,此时在途 >0 已随之可见(互斥锁的同步序),快路径直发
    /// 不会插队刚弹出的在途包。
    fn pop_front_inflight(&self) -> Option<Vec<u8>> {
        let mut q = self.state.lock().unwrap();
        let pkt = q.pop_front()?;
        self.inflight_bytes.fetch_add(pkt.len(), Ordering::Relaxed);
        self.inflight_packets.fetch_add(1, Ordering::Relaxed);
        drop(q);
        Some(pkt)
    }

    /// 发送成功:在途记账清零、计数。
    fn note_sent(&self, pkt_len: usize) {
        self.sent.fetch_add(1, Ordering::Relaxed);
        self.inflight_bytes.fetch_sub(pkt_len, Ordering::Relaxed);
        self.inflight_packets.fetch_sub(1, Ordering::Relaxed);
    }

    /// WouldBlock 回插队头:在途→排队,总预算(queued+inflight)不变。
    /// 生产者 push 已被总预算约束,回插必然放得下;若不满足说明记账
    /// 出错 —— 丢弃回插包本身并计数暴露问题,绝不丢队尾(不能用
    /// 驱逐最新包掩盖超预算)。
    fn requeue_front(&self, pkt: Vec<u8>) {
        let mut q = self.state.lock().unwrap();
        self.inflight_bytes.fetch_sub(pkt.len(), Ordering::Relaxed);
        self.inflight_packets.fetch_sub(1, Ordering::Relaxed);
        if backlog_bytes(&q) + pkt.len() > MAX_BACKLOG_BYTES || q.len() + 1 > MAX_BACKLOG_PACKETS {
            self.dropped_full.fetch_add(1, Ordering::Relaxed);
            tracing::warn!(
                len = pkt.len(),
                "kcp udp backlog requeue over budget: drop self"
            );
            return;
        }
        q.push_front(pkt);
    }
}

/// 同步清空一轮积压。预算 = 调用时已排队列条数:本轮只发进入时已
/// 存在的包,期间新入队的交给下一轮(入队已 notify,worker 立即
/// 续轮)——避免发送回调内的再入队造成同轮无限循环,同时不破坏
/// FIFO(轮与轮之间仍按队列顺序)。
/// - 发送成功计 sent;WouldBlock 放回队头(容量安全)返回 Blocked;
/// - 短写/真实错误:致命 —— 记录错误并返回 Fatal,由 worker 收敛
///   退出、会话侧经 flush 传播终止。不吞、不盲目重试。
#[derive(Debug)]
enum DrainSt {
    Empty,
    Blocked,
    /// 真实错误/短写:错误已记录(可经 fatal_error()/stats() 观察)。
    Fatal,
}

fn drain_once(
    bl: &UdpBacklog,
    mut try_send: impl FnMut(&[u8]) -> std::io::Result<usize>,
) -> DrainSt {
    let budget = bl.queued_packets();
    let mut processed = 0usize;
    loop {
        if processed >= budget {
            return DrainSt::Empty;
        }
        let Some(pkt) = bl.pop_front_inflight() else {
            return DrainSt::Empty;
        };
        processed += 1;
        match try_send(&pkt) {
            Ok(n) if n == pkt.len() => {
                bl.note_sent(pkt.len());
            }
            // UDP 报文原子发送;短写属于异常,致命化(修复前仅计数后
            // continue,违反不吞真实错误约束)
            Ok(n) => {
                bl.note_fatal(format!("kcp udp short write {n}/{}", pkt.len()), pkt.len());
                return DrainSt::Fatal;
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                bl.requeue_front(pkt);
                return DrainSt::Blocked;
            }
            Err(e) => {
                bl.note_fatal(e.to_string(), pkt.len());
                return DrainSt::Fatal;
            }
        }
    }
}

/// 积压 drain 任务:socket 可写时按序发出(唯一串行发送所有权)。
///
/// 目的址泛化(C):connect 已连 / 固定对端 / 打洞轮询状态机 ——
/// connect 与 punch 复用同一 backlog/drain 状态机,不另起一套。
/// Rotating 阶段每个数据报发往「弹出时刻」peer.current() 的候选,
/// Learned 后锁定;pop→send 一次、WouldBlock 回插队头,同一 pending
/// 报至多送达一次。
///
/// 生命周期:owner = 生产者(UdpOutput/PunchOutput,随 Kcp 存亡)。
/// - 生产者 drop 时 Drop notify 唤醒本任务,复查引用计数
///   后退出,不泄漏 backlog/socket/task;
/// - 致命错误后退出(剩余队列随 Arc 释放回收,kcp 语义上可重传的
///   数据本就保留在对端协议状态之外——未 ACK 段在对端 snd_buf);
/// - 无生产者路径(测试构造等)由退避定时兜底复查退出。
///
/// 空闲等待持有 Arc 期间不监听 writable(常可写的 socket 会忙循环),
/// 只等入队通知 + 存活复查定时。
#[derive(Clone)]
enum Dest {
    /// 客户端 socket 已 connect:必须用 try_send(否则 EISCONN)
    Connected,
    Peer(SocketAddr),
    Punch(Arc<PunchPeer>),
}

impl Dest {
    fn try_send(&self, sock: &UdpSocket, pkt: &[u8]) -> std::io::Result<usize> {
        match self {
            Dest::Connected => sock.try_send(pkt),
            Dest::Peer(p) => sock.try_send_to(pkt, *p),
            Dest::Punch(p) => sock.try_send_to(pkt, p.current()),
        }
    }
}

/// A-3:单轮 kcp flush 的输出聚合批。
///
/// 修前空闲直发路径每数据报一次 try_send/try_send_to,满窗单轮
/// (≤ MAX_KCP_PENDING_SEGS=256 段)即 ~256 次 syscall。聚合批把同轮
/// 数据报收进**单一大缓冲 + 长度表**(稳态零分配:缓冲复用容量),
/// 会话任务在每轮迭代尾 [`SendAgg::flush_now`] 批量提交。
///
/// 满载回落:socket 缓冲满(EAGAIN)或部分提交时,余段转入
/// [`UdpBacklog`] 串行化(drain 任务等 writable 补送)—— FIFO 顺序
/// 保持(聚合缓冲内余段一并转移,不与后续轮次竞争),余段仍留在
/// kcp snd_buf(计入 wait_snd),会话收尾守卫的"未确认交付"合同不破。
///
/// 平台门控:sendmmsg 仅 Linux(macOS 无此系统调用,libc 依赖也只
/// 声明在 linux target)。非 Linux 的 [`SendAgg::new`] 恒 None,写路径
/// 原样保留修前的逐包直发;flush_now 的逐包回退实现仅供跨平台测试。
struct SendAgg {
    sock: Arc<UdpSocket>,
    dest: Dest,
    /// Weak 反向引用(聚合批注册在 backlog 的槽上;Weak 防止
    /// backlog ↔ agg 的 Arc 环)。会话存活期内 upgrade 恒成功
    /// (kcp 内的 UdpOutput 持有 backlog Arc,直到会话收尾)。
    backlog: std::sync::Weak<UdpBacklog>,
    /// 聚合缓冲与各数据报长度(单锁;仅会话任务触达,零争用)。
    queue: std::sync::Mutex<AggQueue>,
}

#[derive(Default)]
struct AggQueue {
    buf: Vec<u8>,
    lens: Vec<usize>,
}

/// sendmmsg 单次提交的数据报数上限(UIO_FASTIOV 口径;非 Linux 逐包
/// 回退,值为 1 保语义)。
#[cfg(target_os = "linux")]
const AGG_BATCH: usize = 64;
#[cfg(not(target_os = "linux"))]
const AGG_BATCH: usize = 1;
/// 聚合挂起上限(超出即在写回调内先冲刷;防缓冲无界,正常满窗 256 内不触)。
const AGG_MAX_QUEUED: usize = 256;

impl SendAgg {
    /// 生产构造:仅 Linux 启用聚合(见类型注释);其余平台返回 None,
    /// UdpOutput 走修前逐包路径。构造即把自身注册进 backlog 的聚合槽
    /// (UdpOutput::write 经 backlog.agg() 触达,无需改构造字面量)。
    fn new(backlog: &Arc<UdpBacklog>, sock: Arc<UdpSocket>, dest: Dest) -> Option<Arc<Self>> {
        #[cfg(target_os = "linux")]
        {
            let agg = Arc::new(Self {
                sock,
                dest,
                backlog: Arc::downgrade(backlog),
                queue: std::sync::Mutex::new(AggQueue::default()),
            });
            let _ = backlog.agg_slot.set(agg.clone());
            Some(agg)
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = (backlog, sock, dest);
            None
        }
    }

    /// 写回调入口(kcp 输出契约:同步、不可阻塞,除致命错误外报成功)。
    fn push(&self, buf: &[u8]) {
        {
            let mut q = self.queue.lock().unwrap();
            if q.lens.len() < AGG_MAX_QUEUED {
                q.buf.extend_from_slice(buf);
                q.lens.push(buf.len());
                return;
            }
        }
        // 挂起超限:先内联冲刷再入队。硬错误经 note_fatal 桥传播
        // (与 backlog drain 同口径),本包仍按契约报成功。
        let _ = self.flush_now();
        let mut q = self.queue.lock().unwrap();
        q.buf.extend_from_slice(buf);
        q.lens.push(buf.len());
    }

    /// 冲刷聚合批。锁内同步执行(纯 try 语义,无阻塞调用)。
    fn flush_now(&self) -> std::io::Result<()> {
        let mut q = self.queue.lock().unwrap();
        loop {
            if q.lens.is_empty() {
                return Ok(());
            }
            let Some(backlog) = self.backlog.upgrade() else {
                // backlog 已亡(会话收尾):聚合内容随会话废弃
                q.buf.clear();
                q.lens.clear();
                return Ok(());
            };
            let sent = match self.send_batch(&q) {
                Ok(k) => k,
                Err(e) => {
                    backlog.note_fatal(format!("kcp agg send: {e}"), q.lens[0]);
                    return Err(e);
                }
            };
            let attempted = q.lens.len().min(AGG_BATCH);
            if sent == 0 {
                // EAGAIN:余段全部转 backlog(writable 唤醒后 drain 补送)
                self.transfer_to_backlog(&mut q);
                return Ok(());
            }
            let consumed: usize = q.lens[..sent].iter().sum();
            q.buf.drain(..consumed);
            q.lens.drain(..sent);
            if sent < attempted {
                // 部分提交:余段转 backlog 串行化,保 FIFO
                self.transfer_to_backlog(&mut q);
                return Ok(());
            }
            // 本批全发;仍有余量则继续
        }
    }

    /// 余段(含 buf/lens 全部)按序转入 backlog。
    fn transfer_to_backlog(&self, q: &mut AggQueue) {
        let Some(backlog) = self.backlog.upgrade() else {
            q.buf.clear();
            q.lens.clear();
            return;
        };
        let mut off = 0usize;
        for len in q.lens.drain(..) {
            let _ = backlog.push(&q.buf[off..off + len]);
            off += len;
        }
        q.buf.clear();
    }

    /// 批量提交当前队列头部一批;返回成功发出的数据报数(0 = EAGAIN)。
    fn send_batch(&self, q: &AggQueue) -> std::io::Result<usize> {
        #[cfg(target_os = "linux")]
        {
            self.send_batch_mmsg(q)
        }
        #[cfg(not(target_os = "linux"))]
        {
            self.send_batch_per_packet(q)
        }
    }

    /// 逐包回退(非 Linux 平台的 send_batch 实现;Linux 只走 mmsg,
    /// 此函数在 Linux 目标下不编译 —— 否则无调用点触发 dead_code)。
    #[cfg(not(target_os = "linux"))]
    fn send_batch_per_packet(&self, q: &AggQueue) -> std::io::Result<usize> {
        let mut off = 0usize;
        let mut sent = 0usize;
        for len in q.lens.iter() {
            match self.dest.try_send(&self.sock, &q.buf[off..off + len]) {
                Ok(n) if n == *len => {
                    sent += 1;
                    off += len;
                }
                Ok(n) => {
                    return Err(std::io::Error::other(format!(
                        "kcp udp short write {n}/{len}"
                    )))
                }
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                Err(e) => return Err(e),
            }
        }
        Ok(sent)
    }
}

/// sendmmsg 批量提交(std::os::fd 仅存在于此 cfg 块内)。
#[cfg(target_os = "linux")]
impl SendAgg {
    fn send_batch_mmsg(&self, q: &AggQueue) -> std::io::Result<usize> {
        use std::os::fd::AsRawFd as _;
        let n = q.lens.len().min(AGG_BATCH);
        let mut iovs: [libc::iovec; AGG_BATCH] = unsafe { std::mem::zeroed() };
        let mut hdrs: [libc::mmsghdr; AGG_BATCH] = unsafe { std::mem::zeroed() };
        let mut addrs: [libc::sockaddr_storage; AGG_BATCH] = unsafe { std::mem::zeroed() };
        let mut off = 0usize;
        for i in 0..n {
            iovs[i].iov_base = q.buf[off..].as_ptr() as *mut libc::c_void;
            iovs[i].iov_len = q.lens[i];
            // 目的地址先取值再填 sockaddr(或-pattern 绑定不同类型变量
            // 编不过;Punch 按 per-packet 语义在此取 current(),与逐包
            // try_send 的轮换一致)
            let dest_addr: Option<SocketAddr> = match &self.dest {
                Dest::Connected => None,
                Dest::Peer(p) => Some(*p),
                Dest::Punch(pc) => Some(pc.current()),
            };
            if let Some(addr) = dest_addr {
                let a: &mut libc::sockaddr_storage = &mut addrs[i];
                let len = fill_sockaddr(a, addr);
                hdrs[i].msg_hdr.msg_namelen = len;
                hdrs[i].msg_hdr.msg_name = a as *mut libc::sockaddr_storage as *mut libc::c_void;
            }
            hdrs[i].msg_hdr.msg_iov = &mut iovs[i];
            hdrs[i].msg_hdr.msg_iovlen = 1;
            off += q.lens[i];
        }
        let rc = unsafe {
            libc::sendmmsg(
                self.sock.as_raw_fd(),
                hdrs.as_mut_ptr(),
                n as libc::c_uint,
                0,
            )
        };
        if rc < 0 {
            let err = std::io::Error::last_os_error();
            if err.kind() == std::io::ErrorKind::WouldBlock {
                return Ok(0);
            }
            return Err(err);
        }
        Ok(rc as usize)
    }
}

/// SocketAddr → libc sockaddr(Linux 聚合提交用;零值头 + 显式逐字段)。
#[cfg(target_os = "linux")]
fn fill_sockaddr(ss: &mut libc::sockaddr_storage, p: SocketAddr) -> libc::socklen_t {
    match p {
        SocketAddr::V4(v4) => {
            let a: &mut libc::sockaddr_in =
                unsafe { &mut *(ss as *mut _ as *mut libc::sockaddr_in) };
            a.sin_family = libc::AF_INET as libc::sa_family_t;
            a.sin_port = v4.port().to_be();
            a.sin_addr = libc::in_addr {
                s_addr: u32::from_ne_bytes(v4.ip().octets()),
            };
            std::mem::size_of::<libc::sockaddr_in>() as libc::socklen_t
        }
        SocketAddr::V6(v6) => {
            let a: &mut libc::sockaddr_in6 =
                unsafe { &mut *(ss as *mut _ as *mut libc::sockaddr_in6) };
            a.sin6_family = libc::AF_INET6 as libc::sa_family_t;
            a.sin6_port = v6.port().to_be();
            a.sin6_addr = libc::in6_addr {
                s6_addr: v6.ip().octets(),
            };
            a.sin6_flowinfo = v6.flowinfo();
            a.sin6_scope_id = v6.scope_id();
            std::mem::size_of::<libc::sockaddr_in6>() as libc::socklen_t
        }
    }
}

fn spawn_udp_backlog_drain(sock: Arc<UdpSocket>, dest: Dest, backlog: Arc<UdpBacklog>) {
    let weak = Arc::downgrade(&backlog);
    tokio::spawn(async move {
        let mut recheck = Duration::from_millis(10);
        loop {
            let Some(bl) = weak.upgrade() else {
                break;
            };
            if bl.is_fatal() {
                tracing::debug!(stats = ?bl.stats(), "kcp udp backlog drain exit: fatal");
                break;
            }
            let st = drain_once(&bl, |pkt| {
                bump_counter!(send_attempts);
                dest.try_send(&sock, pkt)
            });
            // bl 自身持一个 strong 引用:仅剩自己(计数 1)说明生产者
            // 已全部释放 —— 队列属主已亡,退出回收。
            let producers_alive = weak.strong_count() > 1;
            drop(bl);
            if matches!(st, DrainSt::Fatal) {
                tracing::debug!("kcp udp backlog drain exit: send error propagated");
                break;
            }
            if !producers_alive {
                tracing::debug!("kcp udp backlog drain exit: producers released");
                break;
            }
            let Some(bl) = weak.upgrade() else {
                break;
            };
            match st {
                DrainSt::Blocked => {
                    tokio::select! {
                        _ = sock.writable() => {}
                        _ = bl.notify.notified() => {}
                        _ = tokio::time::sleep(recheck) => {}
                    }
                }
                _ => {
                    tokio::select! {
                        _ = bl.notify.notified() => {}
                        _ = tokio::time::sleep(recheck) => {}
                    }
                }
            }
            recheck = (recheck * 2).min(BACKLOG_RECHECK_MAX);
        }
    });
}

struct UdpOutput {
    sock: Arc<UdpSocket>,
    peer: SocketAddr,
    /// 客户端 socket 已 connect:必须用 try_send 而非 try_send_to(否则 EISCONN)。
    connected: bool,
    backlog: Arc<UdpBacklog>,
}

/// 生产者写出策略(connect 对端与 punch 轮询共用,C):致命错误检查 →
/// 队列/在途全空时直发快路径(此刻不存在可被插队的包,FIFO 平凡成立;
/// 每数据报一次任务跳换会把 loopback 吞吐打掉近一个数量级)→ 否则有界
/// 入队。UDP 报文原子发送,短写属于异常:显式报错,返回 Ok(n) 会让
/// kcp 的 write_all 把剩余字节当第二个报文发出,损坏对端流。入队返回
/// Ok 是 kcp 输出契约(同步回调不可阻塞,报错即终止会话,只有容量满
/// 可丢,kcp RTO 重传兜底)。WouldBlock 假成功就此消除:入队计数可
/// 观察、drain 补送至多一次。
fn output_write_via_backlog(
    backlog: &UdpBacklog,
    direct: impl FnOnce(&[u8]) -> std::io::Result<usize>,
    buf: &[u8],
) -> std::io::Result<usize> {
    // 致命错误:向 kcp 报错,经 flush 传播终止会话(不吞)。
    // A-6:一次性置位标志先做原子快路径,置位才取锁拿错误串。
    if backlog.is_fatal() {
        if let Some(e) = backlog.fatal_error() {
            return Err(other_err(e));
        }
    }
    if backlog.is_idle() {
        bump_counter!(send_attempts);
        match direct(buf) {
            Ok(n) if n == buf.len() => return Ok(n),
            Ok(n) => {
                return Err(std::io::Error::other(format!(
                    "kcp udp short write {n}/{}",
                    buf.len()
                )))
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(e) => return Err(e),
        }
    }
    let _ = backlog.push(buf);
    Ok(buf.len())
}

impl UdpOutput {
    /// 启动本输出的唯一串行发送任务(drain):sock/peer/connected 的
    /// 读取权移交 drain(它是唯一发送者),生产者只经 backlog 入队。
    /// 必须在 move 进 Kcp 之前调用一次,且每个 backlog 恰好一个 drain。
    fn spawn_drain(&self) {
        let dest = if self.connected {
            Dest::Connected
        } else {
            Dest::Peer(self.peer)
        };
        spawn_udp_backlog_drain(self.sock.clone(), dest, self.backlog.clone());
    }
}

impl Write for UdpOutput {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        // A-3 聚合路径:空闲直发改为入聚合批(单一大缓冲,零稳态分配),
        // 会话任务迭代尾 sendmmsg 批量提交;硬错误/容量满语义与直发
        // 路径一致(fatal 桥 + backlog 串行化)。聚合批注册在 backlog 上
        // (生产 wiring;测试与 PunchOutput 未注册 → 修前逐包路径)。
        // 顺带免去每数据报的 Arc clone 闭包(A-7 同款收益)。
        if let Some(agg) = self.backlog.agg() {
            if self.backlog.is_fatal() {
                if let Some(e) = self.backlog.fatal_error() {
                    return Err(other_err(e));
                }
            }
            if self.backlog.is_idle() {
                bump_counter!(send_attempts);
                agg.push(buf);
                return Ok(buf.len());
            }
            let _ = self.backlog.push(buf);
            return Ok(buf.len());
        }
        // 非 Linux/未启用聚合:修前逐包直发路径。direct 是 FnOnce 局部
        // 闭包,借用即可(A-7:修前每数据报 clone Arc + 搬移标量)。
        let this = &*self;
        output_write_via_backlog(
            &self.backlog,
            move |b| {
                if this.connected {
                    this.sock.try_send(b)
                } else {
                    this.sock.try_send_to(b, this.peer)
                }
            },
            buf,
        )
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl Drop for UdpOutput {
    fn drop(&mut self) {
        // 生产者消亡通知:唤醒 drain 复查存活并退出(permit 语义,
        // drain 未在等待也能补投),消除「idle worker 持 Arc 永挂」。
        self.backlog.notify.notify_one();
    }
}

// ---------------- 会话任务 ----------------

enum SessionCmd {
    Packet(PooledPkt),
}

// ---------------- A-4:入站包池 ----------------

/// 入站包缓冲大小(单包上限;与修前收包缓冲一致)。
const PKT_BUF: usize = 65536;
/// 池槽上限:在途槽 ≈ 收包任务数 + 通道容量;256 槽 = 16MiB 硬上界。
const PKT_POOL_CAP: usize = 256;

/// 进程级入站包池(64KiB 槽复用)。1 Gbps ≈ 85k pps,修前每报文
/// `pkt.to_vec()` 一次 ~1200B 堆分配,且通道 Full 时参数先求值、分配
/// 照做再丢弃;池化后收包直接读进槽内、整槽移交、消费端 kcp.input
/// 后归还,稳态零分配。std Mutex 临界区仅 Vec push/pop,争用弱。
static PKT_POOL: std::sync::Mutex<Vec<Vec<u8>>> = std::sync::Mutex::new(Vec::new());

fn pkt_pool_get() -> Vec<u8> {
    PKT_POOL
        .lock()
        .unwrap()
        .pop()
        .unwrap_or_else(|| vec![0u8; PKT_BUF])
}

fn pkt_pool_put(buf: Vec<u8>) {
    let mut p = PKT_POOL.lock().unwrap();
    if p.len() < PKT_POOL_CAP && buf.len() == PKT_BUF {
        p.push(buf);
    }
}

/// 池化入站包:槽 + 有效长度(kcp.input 只看 [..len])。
struct PooledPkt {
    buf: Vec<u8>,
    len: usize,
}

impl std::ops::Deref for PooledPkt {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        &self.buf[..self.len]
    }
}

impl PooledPkt {
    /// 从切片拷入新槽(首包投喂与测试;热路径直接读进槽再移交)。
    fn copy_of(src: &[u8]) -> Self {
        let mut buf = pkt_pool_get();
        buf[..src.len()].copy_from_slice(src);
        Self {
            buf,
            len: src.len(),
        }
    }
}

async fn run_session<O: std::io::Write>(
    kcp: Kcp<O>,
    inbound: mpsc::Receiver<SessionCmd>,
    shared: Arc<Shared>,
) {
    run_session_with_idle_timeout(kcp, inbound, shared, SESSION_IDLE_TIMEOUT).await
}

/// 可注入空闲超时的会话主循环(测试传 100-300ms;生产默认 90s 不变)。
async fn run_session_with_idle_timeout<O: std::io::Write>(
    mut kcp: Kcp<O>,
    mut inbound: mpsc::Receiver<SessionCmd>,
    shared: Arc<Shared>,
    idle_timeout: Duration,
) {
    let mut ticker = tokio::time::interval(TICK);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut last_activity = Instant::now();
    // B:应用消费进展观察(consume_seq 上次见值;有待交付数据时唯一
    // 的活跃证据)。初始读一次,后续按「变化」判定。
    let mut last_consume_seq = shared.guarded.lock().unwrap().consume_seq;
    // kcp recv 缓冲复用:peeksize→recv 每消息一次分配改为复用
    let mut recv_scratch: Vec<u8> = Vec::new();
    // 会话终止原因(真实错误浮出到 IO;None = 正常收尾)
    let mut error: Option<String> = None;
    // 非错误退出的具体原因(通道提前结束/idle):收尾假成功守卫用。
    // 正常收尾(write_closed 已确认或写侧本就干净)= None。
    let mut exit_reason: Option<&'static str> = None;
    // 段预算精确化:单次喂入不超过剩余段余量 × mss,使站立不变量
    // wait_snd ≤ MAX_KCP_PENDING_SEGS 精确成立(此前「先判后整 64KiB」
    // 可越过上限约 56 段)。
    let mss = kcp.mss().max(1);
    // D:入站批缓冲(容量 BATCH_CAP,Vec 跨轮复用;recv_many 有 ≥1 条
    // 即返回,不凑满、不加等待 —— 低负载单包即时推进不变)。
    let mut batch: Vec<SessionCmd> = Vec::with_capacity(BATCH_CAP);

    loop {
        batch.clear();
        // B3:自适应节拍。完全空闲(无在途段、无待写、无待交付)时
        // kcp.update() 无事可做 —— 重传/探测由在途段驱动,无在途段
        // 则没有 2ms 精度需求。停掉 ticker 分支,空闲会话只等
        // inbound/notify/空闲死线(修前完全空闲也每 2ms 醒一次,
        // max_sessions=4096 时最坏 205 万次唤醒/秒)。任一事件到达后
        // 下一轮恢复 ticker;空闲死线到点由循环尾的 idle 检查收敛。
        let idle = kcp.wait_snd() == 0 && {
            let s = shared.guarded.lock().unwrap();
            s.write_queue.is_empty() && s.read_buf.is_empty()
        };
        let idle_deadline = last_activity + idle_timeout;
        tokio::select! {
            _ = ticker.tick(), if !idle => {}
            n = inbound.recv_many(&mut batch, BATCH_CAP) => {
                // 通道已关闭且空:立即退出,不在 select 热转到 idle 超时。
                if n == 0 {
                    exit_reason = Some("inbound channel closed");
                    break;
                }
            }
            _ = shared.notify.notified() => {}
            // 空闲轮的唯一定时唤醒:到点后走循环尾的 idle 超时判定。
            // 非空闲轮不创建该 future(ticker 分支接管)。
            _ = tokio::time::sleep_until(tokio::time::Instant::from_std(idle_deadline)), if idle => {}
        }
        #[cfg(test)]
        shared.stats.wakeups.fetch_add(1, Ordering::Relaxed);
        bump_counter!(wakeups);
        let got_packet = !batch.is_empty();
        for cmd in batch.drain(..) {
            let SessionCmd::Packet(pkt) = cmd;
            #[cfg(test)]
            shared
                .stats
                .packets_consumed
                .fetch_add(1, Ordering::Relaxed);
            bump_counter!(packets_consumed);
            #[cfg(test)]
            shared.stats.input_calls.fetch_add(1, Ordering::Relaxed);
            bump_counter!(input_calls);
            // input 错误(段格式/conv 不一致)由 demux 前置过滤,
            // 此处按可丢弃网络杂音处理,不终止会话。
            let _ = kcp.input(&pkt);
            // A-4:槽归还池(稳态零分配的关键:消费端闭环)
            let PooledPkt { buf, .. } = pkt;
            pkt_pool_put(buf);
        }
        // D 守护观测:批缓冲容量高水位(复用不增长;仅测试读)
        #[cfg(test)]
        shared
            .batch_cap_hwm
            .fetch_max(batch.capacity() as u64, Ordering::Relaxed);
        // 用户句柄已全部释放:裸 drop 取消语义 —— 不保证交付排队数据,
        // 与显式 shutdown(下方 closing 完成判定,保证交付)刻意分离。
        if shared.handles.load(Ordering::Acquire) == 0 {
            break;
        }
        // 输出致命错误已经错误桥写入(并置 closed/唤醒):立即收尾,
        // 不再等任何后续输出触发。保留桥写入的原始错误。
        if shared.guarded.lock().unwrap().error.is_some() {
            break;
        }
        // 用户写队列 → kcp:有界搬运(站立限额,不每轮清零)。
        // wait_snd() = snd_buf + snd_queue 段数,只有对端 ACK 使其
        // 下降后才会继续搬运;单次喂入 ≤ SEND_PIECE(kcp.send 的
        // UserBufTooBig 上限)。修复前:每轮无条件全部搬入无界
        // snd_queue,前端 256KiB 限额形同虚设,未 ACK 也可持续 accept。
        let user_wrote = {
            // N6:锁内只做队列切分与记账,kcp.send(段化 + memcpy,单轮
            // 循环可到数十段)移到锁外 —— 修前整个搬运持 OS 级互斥锁,
            // 期间 KcpIo::poll_read/poll_write(yamux 收发任务)在同一把
            // 锁上自旋。本任务是 kcp 的唯一喂入方(全仓唯一 send 调用点),
            // 站立上界预算在锁内按进入时 wait_snd() 一次算定、逐片扣减,
            // 与修前「每片 send 后重读 wait_snd」的算术等价:
            // piece_len ≤ 余量×mss ⇒ 该片段数 ≤ 余量。
            let mut s = shared.guarded.lock().unwrap();
            let budget = MAX_KCP_PENDING_SEGS.saturating_sub(kcp.wait_snd());
            let pieces = s.take_send_pieces(budget, mss, SEND_PIECE);
            let wrote = !pieces.is_empty();
            // 仅在真正腾出前端容量时唤醒等待写者:无条件唤醒会与
            // flush/shutdown 等待者的重注册形成忙环(两者互相唤醒,
            // 运行时永不空闲,暂停时钟不再推进、CPU 空转)。
            if wrote {
                s.wake_write();
            }
            drop(s);
            for piece in pieces {
                if let Err(e) = kcp.send(&piece) {
                    error = Some(format!("kcp send: {e}"));
                    break;
                }
            }
            wrote
        };
        if error.is_some() {
            break;
        }
        // 驱动 kcp(重传/探测/输出)
        #[cfg(test)]
        shared.stats.update_calls.fetch_add(1, Ordering::Relaxed);
        bump_counter!(update_calls);
        if let Err(e) = kcp.update(now_ms()) {
            error = Some(format!("kcp update: {e}"));
            break;
        }
        // 重传超限(dead link):会话不可恢复,可观察地终止而非空转
        if kcp.is_dead_link() {
            error = Some("kcp dead link: max resends exceeded".to_string());
            break;
        }
        // kcp 接收 → 读缓冲(有界):read_buf 达到 MAX_READ_BUF 后数据
        // 留在 kcp rcv_queue,rcv 窗口收敛为 0 令对端停发(KCP 协议
        // 流控),不丢已接受字节;读端消费后经 poll_read 低水位
        // notify 唤醒本循环继续 recv。
        let mut recv_moved = false;
        loop {
            // 读端站立上界精确为 MAX_READ_BUF:流模式下 peeksize ≤ mss
            // (每段 frg=0),按「剩余空间不足一个满段」提前停,不做
            // 「先判后整段 extend」的一段越界。
            let room = {
                let s = shared.guarded.lock().unwrap();
                MAX_READ_BUF - s.read_buf.len().min(MAX_READ_BUF)
            };
            if room < mss {
                break;
            }
            let Ok(size) = kcp.peeksize() else {
                break;
            };
            recv_scratch.resize(size, 0);
            match kcp.recv(&mut recv_scratch) {
                Ok(n) => {
                    let mut s = shared.guarded.lock().unwrap();
                    s.read_buf.extend(&recv_scratch[..n]);
                    s.wake_read();
                    recv_moved = true;
                }
                Err(_) => break,
            }
        }
        // 事件驱动 flush:有新写入 / 刚收到包(及时 ACK)/ 本轮从 kcp
        // 取出了数据(rcv 窗口变化,需向对端通告窗口重开)/ 仍有待发
        // 数据时立刻输出,不等 ts_flush —— kcp 0.6.0 interval 下限
        // 10ms,等 interval 会把每次写延迟放大到毫秒级。
        // idle 会话各条件皆假,不产生额外工作,无忙等/ACK 风暴。
        if user_wrote || got_packet || recv_moved || kcp.wait_snd() > 0 {
            #[cfg(test)]
            shared.stats.flush_calls.fetch_add(1, Ordering::Relaxed);
            bump_counter!(flush_calls);
            if let Err(e) = kcp.flush() {
                error = Some(format!("kcp flush: {e}"));
                break;
            }
        }
        // A-3:冲刷本轮聚合批(sendmmsg 批量提交;空批仅一次无争用锁)。
        // EAGAIN/部分提交的余段已在 flush_now 内转 backlog 串行化。
        if let Some(agg) = &shared.send_agg {
            if let Err(e) = agg.flush_now() {
                error = Some(format!("kcp agg flush: {e}"));
                break;
            }
        }
        // flush 契约记账:本轮排空了前端队列,则此前全部已接受写
        // 已交给 kcp 输出 → 推进 flushed_seq 并唤醒 flush/shutdown 等待者。
        {
            let mut s = shared.guarded.lock().unwrap();
            if s.write_queue.is_empty() && s.write_seq > s.flushed_seq {
                s.flushed_seq = s.write_seq;
                s.wake_write();
            }
        }
        // 显式半关闭完成判定(A):请求关闭 + 队列排空 + kcp 全部段已
        // 确认(wait_snd==0,真实对端 kcp 已完整收到)→ **只关本地写
        // 侧**:置 write_closed 并唤醒 flush/shutdown 等待者,会话循环
        // 继续 —— 入包交付、窗口 ACK、dead_link、idle、句柄归零全部
        // 照常(成功关写不得杀读侧)。正常路径不设超时;无 ACK 的对端
        // 由重传 dead_link 以 Err 浮出,poll_shutdown 返回 Err。
        {
            let mut s = shared.guarded.lock().unwrap();
            if s.closing && !s.write_closed && s.write_queue.is_empty() && kcp.wait_snd() == 0 {
                s.write_closed = true;
                s.wake_write();
            }
        }
        // 空闲进展判定(B):读端尚有未交付数据(或 kcp rcv 队列有存货)
        // 时,活跃证据 = 应用消费(consume_seq 变化,≤timeout 内有读的
        // 慢消费者受保护);待交付存在时的入包(纯窗口探测/重传)不再
        // 刷新时钟,无人消费且无消费进展按 idle 超时回收(读端存量
        // 排空后 EOF,退出走现有 shutdown/日志可观察终态)。无待交付时
        // 维持原语义:任何入包刷新。
        let has_pending_delivery = {
            let s = shared.guarded.lock().unwrap();
            !s.read_buf.is_empty()
        } || kcp.peeksize().is_ok();
        if has_pending_delivery {
            let s = shared.guarded.lock().unwrap();
            if s.consume_seq != last_consume_seq {
                last_consume_seq = s.consume_seq;
                last_activity = Instant::now();
            }
        } else if got_packet {
            last_activity = Instant::now();
        }
        if last_activity.elapsed() > idle_timeout {
            tracing::debug!("kcp session idle timeout");
            exit_reason = Some("session idle timeout");
            break;
        }
    }
    // 统一收尾:关闭 + 错误浮出 + 唤醒 IO + 通知接收循环退出。
    {
        let mut s = shared.guarded.lock().unwrap();
        // 关闭假成功守卫:idle/通道提前结束时,写侧仍有未确认交付
        // (write_queue 未排空 / kcp 段未被 ACK / 显式 shutdown 请求了
        // 但 write_closed 未完成),且仍有存活句柄可观察 —— 不得以
        // error=None 伪 EOF/伪成功 shutdown。已接受字节的交付合同
        // 要求失败可观察。正常路径不受影响:write_closed 已确认或
        // 写侧干净 → pending=false;裸 drop 取消(handles==0,无观察者)
        // 也不注入。真实错误路径 error 已先行置位,此处跳过。
        if error.is_none() && shared.handles.load(Ordering::Acquire) > 0 {
            let pending =
                !s.write_queue.is_empty() || kcp.wait_snd() > 0 || (s.closing && !s.write_closed);
            if pending {
                let reason = exit_reason.unwrap_or("session terminated");
                s.error = Some(format!(
                    "kcp {reason} with unconfirmed write delivery (queued_writes={} kcp_unacked_segs={} closing_unconfirmed={})",
                    s.write_queue.len(),
                    kcp.wait_snd(),
                    s.closing && !s.write_closed,
                ));
            }
        }
        s.closed = true;
        if error.is_some() {
            s.error.clone_from(&error);
        }
        s.wake_read();
        s.wake_write();
    }
    shared.shutdown.notify_one();
    if let Some(e) = &error {
        tracing::debug!(error = %e, "kcp session terminated with error");
    }
}

// ---------------- 服务端监听 ----------------

/// demux 生命周期控制:listener drop → closing(停止接入新会话);
/// 会话清理时若已 closing 且会话表空 → wake 通知 demux 退出,
/// 释放 UDP socket(端口可重绑)。存量会话继续共享 socket 服务,
/// 不为立即解绑截断。
struct DemuxCtl {
    closing: std::sync::atomic::AtomicBool,
    wake: Notify,
    /// 已退出/被拒 conv 的退休表:到期时间戳。会话退出后同 conv 的
    /// 报文不再重建会话(kcp 无会话恢复,同 conv 复活只会产出 sn 断层
    /// 的僵尸会话污染 accept 队列);ready 满拒绝短退避,防止单 conv
    /// 洪泛逐包「注册→spawn→拒绝→移除」的任务创建风暴,同时保留
    /// 「队列腾出后 kcp 重传触发重建」的恢复路径。
    ///
    /// A8:固定容量(条目数上限)+ FIFO 驱逐。修前是"超过 1024 才 retain
    /// 已过期条目",唯一 conv 洪水(90s 内每次退出插一个新 conv)会让表
    /// 单调增长,且每次插入 O(n) 扫描。
    retired: Mutex<RetiredTable>,
    /// A8:预认证资源闸。会话在**首包**即注册(HELLO 之前),随机源地址
    /// 洪泛会直接放大成"2 任务 + 256 槽通道"×N。这里限制全局在建会话数
    /// 与每 IP 建会话速率。
    gate: SessionGate,
}

/// 退休表容量与驱逐粒度。超过容量即按插入顺序驱逐最旧条目;被驱逐的
/// conv 若很快重传会重新建会话(极小概率,退让一次,语义可接受)。
const RETIRED_CAPACITY: usize = 4096;

#[derive(Default)]
struct RetiredTable {
    map: HashMap<u32, std::time::Instant>,
    /// 插入顺序(只增不删的环形序号 → conv),按序驱逐最旧。
    order: std::collections::VecDeque<u32>,
}

impl RetiredTable {
    fn insert(&mut self, conv: u32, until: std::time::Instant) {
        // R4:重复 conv 取**较早**的截止 —— 满拒路径先落 200ms 短退避,
        // 随后清理任务的 90s EXIT retire 会到达;修前无条件覆盖,短退避
        // 恒失效,被满拒的 conv 要等 90s 才能靠重传重建(注释承诺的
        // 「队列腾空后 ≥100ms RTO 重传可重建」永不成立)。min 语义下
        // 洪泛防护不弱化(同 conv 仍在表内),恢复语义生效。
        if let Some(prev) = self.map.get_mut(&conv) {
            if until < *prev {
                *prev = until;
            }
            // 沿用原有序位置(避免同 conv 反复插入撑大 order)
            return;
        }
        self.map.insert(conv, until);
        self.order.push_back(conv);
        while self.order.len() > RETIRED_CAPACITY {
            if let Some(old) = self.order.pop_front() {
                self.map.remove(&old);
            }
        }
    }

    fn is_retired(&mut self, conv: u32) -> bool {
        let now = std::time::Instant::now();
        match self.map.get(&conv) {
            Some(&until) if until > now => true,
            Some(_) => {
                // 惰性清理:过期即删,顺带把 order 里的位置留下(entry 已不在
                // map,pop 时是 no-op),避免每次查询都全表扫描。
                self.map.remove(&conv);
                false
            }
            None => false,
        }
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.map.len()
    }
}

/// A8 预认证闸参数:全局在建会话上限 + 每 IP 每秒新建数。
#[derive(Debug, Clone, Copy)]
pub struct SessionGateConfig {
    pub max_sessions: usize,
    pub new_sessions_per_ip_per_sec: f64,
}

impl Default for SessionGateConfig {
    fn default() -> Self {
        Self {
            // 与 ready 队列(512)+ 合理突发对齐;超出即拒绝新 conv,存量
            // 会话不受影响(kcp 重传会在腾出后成功)。
            max_sessions: 4096,
            new_sessions_per_ip_per_sec: 32.0,
        }
    }
}

struct SessionGate {
    cfg: SessionGateConfig,
    /// R4:上次全表淘汰扫描时刻 —— retain 限频(≥1s 一次)。修前只要
    /// 表 >4096,每个成功 admit 都在 per_ip 锁 + sessions 锁双重持锁下
    /// O(n) 全表扫描;伪造源洪泛(R≥1000/s)下 demux 单任务被扫描
    /// 打满、存量会话收包停摆。限频后表上界 = 60s 窗口 + 1s 新到。
    last_sweep: Mutex<std::time::Instant>,
    /// 全局在建会话计数(注册 +1,会话退出 -1)。
    live: std::sync::atomic::AtomicUsize,
    /// 每 IP 令牌桶(惰性创建,随 IP 数量增长;按最近使用淘汰到上限)。
    per_ip: Mutex<HashMap<std::net::IpAddr, (f64, std::time::Instant)>>,
}

impl SessionGate {
    fn new(cfg: SessionGateConfig) -> Self {
        Self {
            cfg,
            live: std::sync::atomic::AtomicUsize::new(0),
            per_ip: Mutex::new(HashMap::new()),
            last_sweep: Mutex::new(std::time::Instant::now()),
        }
    }

    /// 准入:全局上限 + 每 IP 速率。返回 true 才允许注册新会话。
    fn admit(&self, ip: std::net::IpAddr) -> bool {
        use std::sync::atomic::Ordering;
        // 上限判定与计数递增必须在**同一个临界区**内:修前 `load` 判上限、
        // 末尾 `fetch_add` 两步分离,并行 admit 可同时通过判定、令 live 突破
        // max_sessions(超出量 = 并发度)。per_ip 锁本就在本函数内持有,
        // 把判定移进锁内即闭合,无需 CAS 循环。
        let mut g = self.per_ip.lock().unwrap();
        if self.live.load(Ordering::Relaxed) >= self.cfg.max_sessions {
            return false;
        }
        let now = std::time::Instant::now();
        let cap = self.cfg.new_sessions_per_ip_per_sec.max(1.0);
        let e = g.entry(ip).or_insert((cap, now));
        let elapsed = now.duration_since(e.1).as_secs_f64();
        e.0 = (e.0 + elapsed * cap).min(cap);
        e.1 = now;
        if e.0 < 1.0 {
            return false;
        }
        e.0 -= 1.0;
        // 惰性淘汰(R3):纯时间口径 —— 超过 4096 个 IP 时清掉闲置 >60s
        // 的条目。修前谓词 `tokens < cap || last < 1s` 恰好**保留**所有
        // 一次性源(新条目立即扣 1 令牌,恒 tokens=cap−1<cap,永不淘汰),
        // 与"防随机源地址洪泛撑爆表"的目标正好相反:1M 伪造源 ≈70MB
        // 常驻,且表一旦超过 4096 此后每次 admit 都在锁内 O(n) 全表扫描。
        // 时间口径下洪泛表上限 = 60s 窗口内的唯一源数,自然有界。
        // R4:retain 限频(≥1s 一次)—— 修前表超 4096 后每个成功 admit
        // 都在 per_ip 锁 + sessions 锁双重持锁下 O(n) 全表扫描,伪造源
        // 洪泛下 demux 单任务被扫描打满、存量会话收包停摆。
        let mut sweep = self.last_sweep.lock().unwrap();
        if g.len() > 4096 && sweep.elapsed() >= Duration::from_secs(1) {
            *sweep = now;
            g.retain(|_, (_, last)| now.duration_since(*last) < Duration::from_secs(60));
        }
        self.live.fetch_add(1, Ordering::Relaxed);
        true
    }

    /// 会话退出:全局计数 -1(由清理路径调用一次)。
    fn release(&self) {
        use std::sync::atomic::Ordering;
        let _ = self
            .live
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                Some(v.saturating_sub(1))
            });
    }
}

impl DemuxCtl {
    fn retire(&self, conv: u32, ttl: Duration) {
        self.retired
            .lock()
            .unwrap()
            .insert(conv, std::time::Instant::now() + ttl);
    }

    fn is_retired(&self, conv: u32) -> bool {
        self.retired.lock().unwrap().is_retired(conv)
    }
}

/// 会话正常退出后的同 conv 退休期(与空闲超时同量级:同 conv 复活
/// 在 kcp 语义下永非合法 —— 客户端重连必用新 conv)。
const RETIRE_AFTER_EXIT: Duration = Duration::from_secs(90);
/// ready 满拒绝后的短退避:限流同 conv 洪泛的重复建会话,不阻断
/// 队列腾空后 kcp 重传(RTO ≥100ms)触发的合法重建。
const RETIRE_AFTER_REJECT: Duration = Duration::from_millis(200);

/// ready 队列条目:未交付会话的 KcpIo + 对端地址。accept 时才 wrap
/// 成 mux —— 未交付会话的唯一句柄在本队列(listener 所有):listener
/// drop / 队列满拒绝即 drop io → handles==0 → run_session/接收循环/
/// drain 全部取消并从 map 移除,不产生 detached yamux driver 拖住
/// socket。已 accept 会话的 io 由消费方(mux)持有,正常交换不截断。
type ReadyItem = (KcpIo, SocketAddr);

pub struct KcpListener {
    local: SocketAddr,
    accept_rx: mpsc::Receiver<ReadyItem>,
    /// accept 时 wrap mux 用的流数上限快照(热配置,取当前值)
    mux_streams: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    ctl: Arc<DemuxCtl>,
    /// 会话表(与 demux 共享;测试用作真实 ready/首包证据的前置探针,
    /// 生产代码不经此字段访问)
    #[cfg_attr(not(test), allow(dead_code))]
    sessions: SessionMap,
}

impl Drop for KcpListener {
    fn drop(&mut self) {
        self.ctl.closing.store(true, Ordering::Release);
        self.ctl.wake.notify_one();
    }
}

impl KcpListener {
    pub fn local_addr(&self) -> SocketAddr {
        self.local
    }

    pub async fn accept(&mut self) -> std::io::Result<Incoming> {
        let (io, remote) = self
            .accept_rx
            .recv()
            .await
            .ok_or_else(|| other_err("kcp listener closed"))?;
        // 此刻才交付:wrap 出的 mux/io 所有权移交消费方
        let mux = wrap_stream_as_server(
            Box::new(io),
            true,
            self.mux_streams.load(std::sync::atomic::Ordering::Relaxed),
        );
        Ok(Incoming {
            mux,
            remote,
            protocol: Protocol::Kcp,
        })
    }
}

/// UDP socket 收发缓冲目标值(字节)。
///
/// 为什么要显式设:内核默认 `net.core.rmem_default` 在主流发行版是
/// **212992(208 KiB)**。KCP 单会话在 1 Gbps 下是 ~85k pps × 1200B,
/// 208 KiB 只能缓冲约 170 个报文(≈2ms)。接收循环一旦被调度延迟
/// (会话任务在跑 update/flush、或多会话争用同一 worker),内核直接
/// 丢包 —— 实测 `ss -uanmp` 上 `rb212992` 配 `d52901`(已丢 5.3 万包),
/// 而 KCP 只能靠 RTO 重传补,吞吐塌到直连的 24~33%。
///
/// 4 MiB 的依据:1200B 报文约 3500 个(≈40ms 缓冲),足以吸收一次
/// 调度抖动而不丢包;再大只会增加内存占用与首包排队延迟,对低 RTT
/// 内网没有收益。
///
/// **设不上不算失败**:`net.core.rmem_max` 可能低于目标,内核会静默
/// 截断到上限(与 `sndbuf_explicit_set_takes_effect` 踩过的坑同型)。
/// 这里只尽力而为并 debug 记录实际值,绝不因调优失败而让连接建不起来。
const UDP_BUFFER_BYTES: usize = 4 * 1024 * 1024;

/// 绑定 UDP socket 并尽力放大收发缓冲。
///
/// `tokio::net::UdpSocket::bind` 之后、任何读写之前调用;失败只记日志。
async fn bind_udp(addr: impl tokio::net::ToSocketAddrs) -> std::io::Result<UdpSocket> {
    let sock = UdpSocket::bind(addr).await?;
    tune_udp_buffer(&sock);
    Ok(sock)
}

/// 尽力设置 `SO_RCVBUF` / `SO_SNDBUF`,不返回错误(见 `UDP_BUFFER_BYTES`)。
fn tune_udp_buffer(sock: &UdpSocket) {
    let sref = socket2::SockRef::from(sock);
    for (name, res) in [
        ("SO_RCVBUF", sref.set_recv_buffer_size(UDP_BUFFER_BYTES)),
        ("SO_SNDBUF", sref.set_send_buffer_size(UDP_BUFFER_BYTES)),
    ] {
        if let Err(e) = res {
            tracing::debug!(option = name, error = %e, "kcp udp buffer 设置失败(按尽力而为忽略)");
        }
    }
}

pub async fn listen(
    cfg: &ServerTransportConfig,
    mux_streams: std::sync::Arc<std::sync::atomic::AtomicUsize>,
) -> std::io::Result<KcpListener> {
    let udp = Arc::new(bind_udp(cfg.bind_addr).await?);
    let local = udp.local_addr()?;
    let (accept_tx, accept_rx) = mpsc::channel::<ReadyItem>(512);
    let ctl = Arc::new(DemuxCtl {
        closing: std::sync::atomic::AtomicBool::new(false),
        wake: Notify::new(),
        retired: Mutex::new(RetiredTable::default()),
        gate: SessionGate::new(SessionGateConfig::default()),
    });
    let sessions: SessionMap = Arc::new(Mutex::new(HashMap::new()));
    tokio::spawn(demux_loop(
        udp,
        accept_tx,
        mux_streams.clone(),
        ctl.clone(),
        sessions.clone(),
    ));
    Ok(KcpListener {
        local,
        accept_rx,
        mux_streams,
        ctl,
        sessions,
    })
}

type SessionMap = Arc<Mutex<HashMap<u32, (mpsc::Sender<SessionCmd>, SocketAddr)>>>;

async fn demux_loop(
    udp: Arc<UdpSocket>,
    accept_tx: mpsc::Sender<ReadyItem>,
    mux_streams: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    ctl: Arc<DemuxCtl>,
    sessions: SessionMap,
) {
    loop {
        // A-4:每迭代取池槽收包,随包整槽移交消费端(drop 路径由
        // demux_route_one 统一归还)。
        //
        // A-5 回退(2026-10-26 基准):Linux 曾用「readable() + raw
        // recvmmsg 批收 + try 探测清缓存」替代本逐包 recv_from,实测
        // 对回显窗口型 UDP 载荷(1200B×window32)每程加 ~110µs 固定
        // 延迟(RTT p50 3.16→3.82ms,吞吐 -19%),而对 TCP 吞吐零贡献
        // (agg 关闭对照轮与基线持平)。批收的 syscall 摊薄在该负载
        // 下买不回这个延迟代价,统一回到 tokio 原生 recv_from 的等待
        // 语义 —— 唤醒点就是收包点,无就绪缓存簿记。
        let mut buf = pkt_pool_get();
        let (n, peer) = tokio::select! {
            r = udp.recv_from(&mut buf) => match r {
                Ok(x) => {
                    bump_counter!(recvfrom_calls);
                    x
                }
                Err(e) => {
                    // 持续性 socket 错误(如 fd 失效)不退避会忙转烧 CPU
                    tracing::warn!(error = %e, "kcp recv error");
                    pkt_pool_put(buf);
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    continue;
                }
            },
            // listener drop / 最后会话退出通知:已 closing 且无会话
            // → 退出释放 socket;仍有会话则继续服务存量。
            _ = ctl.wake.notified() => {
                if ctl.closing.load(Ordering::Acquire)
                    && sessions.lock().unwrap().is_empty()
                {
                    tracing::debug!("kcp demux exit: listener dropped, no sessions");
                    break;
                }
                pkt_pool_put(buf);
                continue;
            }
        };
        demux_route_one(
            &udp,
            &accept_tx,
            &mux_streams,
            &ctl,
            &sessions,
            peer,
            PooledPkt { buf, len: n },
        );
    }
}

/// demux 单包路由(逐包与 recvmmsg 批收共用)。Skip/满/关闭路径在
/// 内部归还池槽。
#[allow(clippy::too_many_arguments)]
fn demux_route_one(
    udp: &Arc<UdpSocket>,
    accept_tx: &mpsc::Sender<ReadyItem>,
    mux_streams: &std::sync::Arc<std::sync::atomic::AtomicUsize>,
    ctl: &Arc<DemuxCtl>,
    sessions: &SessionMap,
    peer: SocketAddr,
    pkt: PooledPkt,
) {
    let PooledPkt { buf, len } = pkt;
    if len < KCP_OVERHEAD {
        pkt_pool_put(buf);
        return;
    }
    let conv = kcp::get_conv(&buf[..len]);
    // cmd 合法性:81(PUSH) 82(ACK) 83(WASK) 84(WINS)
    if !(81..=84).contains(&buf[4]) {
        pkt_pool_put(buf);
        return;
    }
    // 会话表锁只做查表/注册:锁内绝不 spawn/投递/清理 ——
    // spawn_server_session 在 ready 满队列/关闭分支会再次锁同一
    // 非重入 Mutex(旧实现在此自锁,demux 与全部会话清理一起卡死,
    // 见 DEMUX-FULL-DEADLOCK)。注册竞态由清理路径的 channel 身份
    // 比较(same_channel)防旧会话误删重建同 conv 的新注册。
    enum Route {
        Existing(mpsc::Sender<SessionCmd>),
        New((mpsc::Sender<SessionCmd>, mpsc::Receiver<SessionCmd>)),
        Skip,
    }
    let route = {
        let mut g = sessions.lock().unwrap();
        match g.get(&conv) {
            // 会话锁定首见对端:第三方伪造同 conv 报文不得注入既有会话
            Some((tx, pinned)) if !tx.is_closed() && *pinned == peer => Route::Existing(tx.clone()),
            Some(_) => Route::Skip,
            // listener 已关闭:不再接入新会话(存量继续服务)
            None if ctl.closing.load(Ordering::Acquire) => Route::Skip,
            // 已退休 conv(刚退出/刚被满队列拒绝):不重建
            None if ctl.is_retired(conv) => Route::Skip,
            // A8:预认证闸 —— 只对**新**会话生效(存量会话已在上面的
            // Existing 分支放行)。全局上限与每 IP 速率都在此判定,避免
            // 随机源地址把会话表/任务数放大成无界。
            None if !ctl.gate.admit(peer.ip()) => {
                tracing::debug!(%peer, conv, "kcp session gate: rejected new session");
                Route::Skip
            }
            None => {
                let (tx, rx) = mpsc::channel::<SessionCmd>(256);
                g.insert(conv, (tx.clone(), peer));
                Route::New((tx, rx))
            }
        }
    };
    let route = match route {
        Route::Skip => {
            pkt_pool_put(buf);
            return;
        }
        // 锁已释放后再 spawn:helper 内部(含满队列/关闭失败分支、
        // 清理回调)的任何锁获取都不会与调用方嵌套
        Route::New((tx, rx)) => {
            spawn_server_session(
                conv,
                tx.clone(),
                rx,
                sessions.clone(),
                udp.clone(),
                peer,
                accept_tx,
                mux_streams.load(std::sync::atomic::Ordering::Relaxed),
                ctl.clone(),
            );
            tx
        }
        Route::Existing(tx) => tx,
    };
    // 满丢包:kcp 重传兜底;两种失败都回收槽(Full = 丢包,Closed = 会话已亡)
    match route.try_send(SessionCmd::Packet(PooledPkt { buf, len })) {
        Ok(()) => {}
        Err(mpsc::error::TrySendError::Full(p)) | Err(mpsc::error::TrySendError::Closed(p)) => {
            let SessionCmd::Packet(PooledPkt { buf, .. }) = p;
            pkt_pool_put(buf);
        }
    }
}

#[allow(clippy::too_many_arguments)] // 会话参数各自独立语义,打包反而模糊;max_streams 为热配置快照
fn spawn_server_session(
    conv: u32,
    own_tx: mpsc::Sender<SessionCmd>,
    rx: mpsc::Receiver<SessionCmd>,
    sessions: SessionMap,
    udp: Arc<UdpSocket>,
    peer: SocketAddr,
    accept_tx: &mpsc::Sender<ReadyItem>,
    _max_streams: usize,
    ctl: Arc<DemuxCtl>,
) {
    let backlog = Arc::new(UdpBacklog::new());
    // A-3 输出聚合批(仅 Linux;其余平台 None 走逐包路径)
    let agg = SendAgg::new(&backlog, udp.clone(), Dest::Peer(peer));
    let (io, shared) = new_io_with(agg.clone());
    // 错误桥:idle 会话也要能及时收到真实输出错误(不等下一次写)
    backlog.attach_session(&shared);
    let output = UdpOutput {
        sock: udp.clone(),
        peer,
        connected: false,
        backlog: backlog.clone(),
    };
    output.spawn_drain();
    let mut kcp = Kcp::new_stream(conv, output);
    kcp.set_mtu(MTU).ok();
    kcp.set_wndsize(256, 256);
    kcp.set_nodelay(true, 2, 2, true);
    kcp.set_maximum_resend_times(30);
    let cleanup_sessions = sessions.clone();
    let own_tx2 = own_tx.clone();
    let cleanup_ctl = ctl.clone();
    tokio::spawn(async move {
        run_session(kcp, rx, shared).await;
        // 先退休再移除:退休先于 map 删除生效,窗口期内的同 conv 报文
        // 命中「Existing→closed 通道丢弃」,移除后命中退休表,均不重建
        cleanup_ctl.retire(conv, RETIRE_AFTER_EXIT);
        cleanup_ctl.gate.release();
        // 仅当表内仍是本会话时移除,避免误删同 conv 重建的新会话
        let mut g = cleanup_sessions.lock().unwrap();
        if let Some((cur, _)) = g.get(&conv) {
            if cur.same_channel(&own_tx2) {
                g.remove(&conv);
            }
        }
        // listener 已关闭且本会话是最后一个:通知 demux 退出释放端口
        if cleanup_ctl.closing.load(Ordering::Acquire) && g.is_empty() {
            drop(g);
            cleanup_ctl.wake.notify_one();
        }
    });
    // ready 排队(KcpIo + 对端;accept 时才 wrap mux)。try_send 失败
    // (队列满/接收端已关闭)时,Err 中的 io 随之 drop → handles==0 →
    // run_session/接收循环/drain 自行取消;此处撤销 map 注册,会话
    // 任务退出后的清理幂等。
    if accept_tx.try_send((io, peer)).is_err() {
        tracing::warn!("kcp accept queue full, dropping new session");
        // 短退休:限流同 conv 洪泛的重复注册/spawn;队列腾空后 kcp
        // 重传(≥100ms RTO)仍可重建
        ctl.retire(conv, RETIRE_AFTER_REJECT);
        let mut g = sessions.lock().unwrap();
        if let Some((cur, _)) = g.get(&conv) {
            if cur.same_channel(&own_tx) {
                g.remove(&conv);
            }
        }
    }
}

fn new_io() -> (KcpIo, Arc<Shared>) {
    new_io_with(None)
}

/// A-3:生产路径带输出聚合批构造;测试/非 Linux 传 None。
fn new_io_with(send_agg: Option<Arc<SendAgg>>) -> (KcpIo, Arc<Shared>) {
    let shared = Arc::new(Shared {
        guarded: Mutex::new(Guarded {
            read_buf: VecDeque::new(),
            read_waker: None,
            write_queue: VecDeque::new(),
            write_bytes: 0,
            write_waker: None,
            closed: false,
            closing: false,
            write_closed: false,
            consume_seq: 0,
            write_seq: 0,
            flushed_seq: 0,
            error: None,
        }),
        notify: Notify::new(),
        shutdown: Notify::new(),
        handles: AtomicUsize::new(1),
        #[cfg(test)]
        stats: KcpCountersStatic::new(),
        #[cfg(test)]
        batch_cap_hwm: AtomicU64::new(0),
        send_agg,
    });
    (
        KcpIo {
            shared: shared.clone(),
        },
        shared,
    )
}

// ---------------- 客户端 ----------------

static CONV_COUNTER: AtomicU64 = AtomicU64::new(1);

pub async fn connect(cfg: &ClientTransportConfig) -> std::io::Result<Arc<Multiplexer>> {
    let bind_local: &str = if cfg.server_addr.is_ipv4() {
        "0.0.0.0:0"
    } else {
        "[::]:0"
    };
    let udp = Arc::new(bind_udp(bind_local).await?);
    udp.connect(cfg.server_addr).await?;
    let conv = (CONV_COUNTER.fetch_add(1, Ordering::Relaxed) as u32) ^ rand_u32();
    let backlog = Arc::new(UdpBacklog::new());
    // A-3 输出聚合批(仅 Linux;connected socket,sendmmsg 走隐式对端)
    let agg = SendAgg::new(&backlog, udp.clone(), Dest::Connected);
    let (io, shared) = new_io_with(agg.clone());
    // 错误桥:idle 会话也要能及时收到真实输出错误(不等下一次写)
    backlog.attach_session(&shared);
    let output = UdpOutput {
        sock: udp.clone(),
        peer: cfg.server_addr,
        connected: true,
        backlog: backlog.clone(),
    };
    output.spawn_drain();
    let mut kcp = Kcp::new_stream(conv, output);
    kcp.set_mtu(MTU).ok();
    kcp.set_wndsize(256, 256);
    kcp.set_nodelay(true, 2, 2, true);
    kcp.set_maximum_resend_times(30);
    // 客户端侧接收循环:UDP → 会话任务。owner = 会话任务:
    // select 会话 shutdown(会话结束即退出,不再 recv 挂起持 socket)
    // + 通道关闭。send().await 背压:try_send 满即 break 会终结会话
    // (负载下会话消费慢于收包时通道必然打满);等待期间突发由
    // kernel socket 缓冲吸收,丢包交给 kcp 重传。
    let (pkt_tx, pkt_rx) = mpsc::channel::<SessionCmd>(256);
    let recv_sock = udp;
    {
        let pkt_tx = pkt_tx;
        let shared = shared.clone();
        tokio::spawn(async move {
            loop {
                // A-4:池槽收包,随包整槽移交(修前每包 to_vec 分配)
                let mut buf = pkt_pool_get();
                tokio::select! {
                    r = recv_sock.recv_from(&mut buf) => {
                        let Ok((n, _)) = r else { break };
                        bump_counter!(recvfrom_calls);
                        if pkt_tx
                            .send(SessionCmd::Packet(PooledPkt { buf, len: n }))
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                    _ = shared.shutdown.notified() => break,
                }
            }
        });
    }
    tokio::spawn(run_session(kcp, pkt_rx, shared));
    Ok(wrap_stream_as_client(
        Box::new(io),
        true,
        MUX_STREAMS_UNLIMITED,
    ))
}

// ---------------- NAT 打洞 ----------------

/// 打洞 listen 角色:在已绑定 socket 上被动接受首个 kcp 会话。
/// 首个 kcp 消息约定为打洞哨兵(dial 侧重传它作为打洞包),此处剥离。
///
/// A7:`allowed` 是服务端下发的对端候选地址集合。打洞窗口内只有来自该集合
/// 的首包能锁定会话;候选集合为空(旧服务端/无候选)时退化为任意来源并
/// 记 warn —— 该退化只出现在新旧混连,不与 A7 的认证路径叠加。
pub async fn punch_listen(
    udp: Arc<UdpSocket>,
    allowed: &[SocketAddr],
) -> std::io::Result<Arc<Multiplexer>> {
    const SENTINEL: &[u8] = b"RHTP-PUNCH!";
    if allowed.is_empty() {
        tracing::warn!("punch listen: empty candidate set, source pinning disabled");
    }
    // 1. 等待首个合法 kcp 包(来源必须属于候选集合)
    let mut buf = vec![0u8; 65536];
    let (n, peer) = loop {
        let (n, from) = udp.recv_from(&mut buf).await?;
        if n >= KCP_OVERHEAD
            && (81..=84).contains(&buf[4])
            && (allowed.is_empty() || allowed.contains(&from))
        {
            break (n, from);
        }
        if !allowed.is_empty() && n >= KCP_OVERHEAD && (81..=84).contains(&buf[4]) {
            tracing::debug!(%from, "punch listen: drop non-candidate source");
        }
    };
    let conv = kcp::get_conv(&buf[..n]);
    // 2. 建会话并投喂首包
    let backlog = Arc::new(UdpBacklog::new());
    // A-3 输出聚合批(仅 Linux;打洞数据会话对端已锁定)
    let agg = SendAgg::new(&backlog, udp.clone(), Dest::Peer(peer));
    let (io, shared) = new_io_with(agg.clone());
    // 错误桥:idle 会话也要能及时收到真实输出错误(不等下一次写)
    backlog.attach_session(&shared);
    let output = UdpOutput {
        sock: udp.clone(),
        peer,
        connected: false,
        backlog: backlog.clone(),
    };
    output.spawn_drain();
    let mut kcp = Kcp::new_stream(conv, output);
    kcp.set_mtu(MTU).ok();
    kcp.set_wndsize(256, 256);
    kcp.set_nodelay(true, 2, 2, true);
    kcp.set_maximum_resend_times(60);
    let (pkt_tx, pkt_rx) = mpsc::channel::<SessionCmd>(256);
    // 后续入站路由到会话(owner = 会话:shutdown 即退出,不挂起持 socket)。
    // 来源锁定:仅接受首包对端(与 demux 的 pinned peer 防护对齐,
    // 第三方伪造同 conv 段不得注入打洞会话);满队列丢包不断流
    // (kcp RTO 重传兜底),仅接收端关闭才退出 —— 与 connect/demux
    // 的语义对齐(修复前 Full 与 Closed 同样断流,socket 无人收包)。
    {
        let udp = udp.clone();
        let pkt_tx = pkt_tx.clone();
        let shared = shared.clone();
        let pinned = peer;
        tokio::spawn(async move {
            loop {
                // A-4:池槽收包,随包整槽移交
                let mut b = pkt_pool_get();
                tokio::select! {
                    r = udp.recv_from(&mut b) => {
                        let Ok((n, from)) = r else { break };
                        bump_counter!(recvfrom_calls);
                        if from != pinned {
                            pkt_pool_put(b);
                            continue;
                        }
                        if n >= KCP_OVERHEAD && (81..=84).contains(&b[4]) {
                            match pkt_tx.try_send(SessionCmd::Packet(PooledPkt { buf: b, len: n })) {
                                Ok(()) => {}
                                Err(mpsc::error::TrySendError::Full(p)) => {
                                    // 满丢包:kcp 重传兜底,不断流;槽回收
                                    let SessionCmd::Packet(PooledPkt { buf, .. }) = p;
                                    pkt_pool_put(buf);
                                }
                                Err(mpsc::error::TrySendError::Closed(_)) => break,
                            }
                        } else {
                            pkt_pool_put(b);
                        }
                    }
                    _ = shared.shutdown.notified() => break,
                }
            }
        });
    }
    let _ = pkt_tx.try_send(SessionCmd::Packet(PooledPkt::copy_of(&buf[..n])));
    tokio::spawn(run_session(kcp, pkt_rx, shared.clone()));
    // 3. 等首条消息落地:哨兵则剥离
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    loop {
        {
            let mut s = shared.guarded.lock().unwrap();
            if s.read_buf.len() >= SENTINEL.len() {
                let is_sentinel = s
                    .read_buf
                    .iter()
                    .take(SENTINEL.len())
                    .copied()
                    .collect::<Vec<u8>>()
                    == SENTINEL;
                if is_sentinel {
                    let mut rest: VecDeque<u8> = s.read_buf.split_off(SENTINEL.len());
                    std::mem::swap(&mut s.read_buf, &mut rest);
                }
                break;
            }
        }
        if tokio::time::Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    Ok(wrap_stream_as_server(
        Box::new(io),
        true,
        MUX_STREAMS_UNLIMITED,
    ))
}

/// 打洞对端状态机(单锁原语):Rotating 阶段候选轮询可改写目标;
/// 首个合法入包 learn 后进入 Learned 并永久锁定 —— 轮询任务的
/// check-then-act 竞态(过 learned 检查后仍覆写 peer)由此闭合:
/// 覆写与 learn 在同一互斥锁内串行,Learned 后轮询为 no-op。
#[derive(Debug)]
enum PunchPeerState {
    Rotating(SocketAddr),
    Learned(SocketAddr),
}

struct PunchPeer {
    st: Mutex<PunchPeerState>,
}

impl PunchPeer {
    fn new(first: SocketAddr) -> Self {
        Self {
            st: Mutex::new(PunchPeerState::Rotating(first)),
        }
    }

    /// 候选轮换:仅 Rotating 阶段生效。
    fn rotate(&self, c: SocketAddr) {
        if let PunchPeerState::Rotating(p) = &mut *self.st.lock().unwrap() {
            *p = c;
        }
    }

    /// 锁定真实对端;仅在 Rotating→Learned 迁移时返回 true。
    fn learn(&self, a: SocketAddr) -> bool {
        let mut g = self.st.lock().unwrap();
        if matches!(&*g, PunchPeerState::Rotating(_)) {
            *g = PunchPeerState::Learned(a);
            true
        } else {
            false
        }
    }

    fn current(&self) -> SocketAddr {
        match &*self.st.lock().unwrap() {
            PunchPeerState::Rotating(p) | PunchPeerState::Learned(p) => *p,
        }
    }
}

/// 打洞输出(C):与 connect 复用同一 UdpBacklog/drain 状态机 ——
/// peer 经 PunchPeer 状态机(learn 后锁定),WouldBlock 有界入队由
/// drain 在 socket 可写时按序补送(每个 pending 报至多一次),真实
/// 错误经 attach_session 错误桥直达会话 IO(idle 会话不等下一次写),
/// 不再有「try_send_to WouldBlock → Ok(buf.len()) 假成功丢报」。
struct PunchOutput {
    sock: Arc<UdpSocket>,
    peer: Arc<PunchPeer>,
    backlog: Arc<UdpBacklog>,
}

impl PunchOutput {
    /// 与 UdpOutput::spawn_drain 同款:唯一串行发送任务,目的址为
    /// Punch(Rotating 每报取弹出时刻候选,Learned 锁定)。
    fn spawn_drain(&self) {
        spawn_udp_backlog_drain(
            self.sock.clone(),
            Dest::Punch(self.peer.clone()),
            self.backlog.clone(),
        );
    }
}

impl Write for PunchOutput {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        // A-7:direct 闭包借用 self(FnOnce,无需每数据报 clone 两个 Arc)
        let this = &*self;
        output_write_via_backlog(
            &self.backlog,
            move |b| this.sock.try_send_to(b, this.peer.current()),
            buf,
        )
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl Drop for PunchOutput {
    fn drop(&mut self) {
        // 生产者消亡通知(与 UdpOutput::drop 同款):唤醒 drain 复查
        // 存活并退出,不持 socket 永挂。
        self.backlog.notify.notify_one();
    }
}

/// 打洞辅助任务的取消所有权(RAII):future 因任何非成功路径离开 ——
/// 超时返回 Err、或调用方 drop/abort(parent cancel)—— 都在 Drop 中
/// notify_one(permit 语义)关停入站/轮询任务;成功路径显式 disarm
/// (learned 后轮询任务自退,入站任务转为会话生命周期,由会话
/// shutdown 收敛)。修复前仅超时分支通知,parent cancel 后轮询任务
/// 永生:持有 socket 每 800ms 向全部候选发 RHPUNCH 直到进程重启
/// (主代理 PUNCH-PARENT-CANCEL 实测 socket_refs=2 released=false)。
struct PunchCancel {
    abort_inbound: Arc<tokio::sync::Notify>,
    abort_poll: Arc<tokio::sync::Notify>,
    armed: bool,
}

impl Drop for PunchCancel {
    fn drop(&mut self) {
        if self.armed {
            self.abort_inbound.notify_one();
            self.abort_poll.notify_one();
        }
    }
}

/// 打洞 dial 角色:向全部候选轮询发送 kcp 输出与原始打洞包;
/// 首个合法 kcp 入站包锁定对端。seconds 超时返回错误。
pub async fn punch_dial(
    udp: Arc<UdpSocket>,
    candidates: &[SocketAddr],
    seconds: u64,
) -> std::io::Result<Arc<Multiplexer>> {
    if candidates.is_empty() {
        return Err(other_err("no punch candidates"));
    }
    let conv = (CONV_COUNTER.fetch_add(1, Ordering::Relaxed) as u32) ^ rand_u32();
    let peer = Arc::new(PunchPeer::new(candidates[0]));
    let (io, shared) = new_io();
    // C:打洞输出接入同一积压队列 + 错误桥(idle 会话也能及时收到
    // 真实输出错误,与 connect/listen 接线一致)
    let backlog = Arc::new(UdpBacklog::new());
    backlog.attach_session(&shared);
    let output = PunchOutput {
        sock: udp.clone(),
        peer: peer.clone(),
        backlog: backlog.clone(),
    };
    output.spawn_drain();
    drop(backlog);
    let mut kcp = Kcp::new_stream(conv, output);
    kcp.set_mtu(MTU).ok();
    kcp.set_wndsize(256, 256);
    kcp.set_nodelay(true, 2, 2, true);
    kcp.set_maximum_resend_times(30);
    let (pkt_tx, pkt_rx) = mpsc::channel::<SessionCmd>(256);
    // 入站:合法 kcp 包 learn 对端并喂会话;abort = 打洞失败,收尾退出;
    // 会话 shutdown = 会话结束,同样退出(不挂起持 socket)。
    // 满队列丢包不断流(kcp RTO 重传兜底),仅接收端关闭才退出 ——
    // 与 connect(send().await 背压)/demux(丢包不断流)语义对齐。
    let inbound = Arc::new(tokio::sync::Notify::new());
    let learned = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let cands_for_inbound = candidates.to_vec();
    // 两个独立 abort 通道 + notify_one(permit 语义):notify_waiters 无
    // permit,任务处于「检查后、select 注册前」执行段时通知永久丢失,
    // 轮询任务将永生并每 800ms 向全部候选发包(结构性泄漏)。
    let abort_inbound = Arc::new(tokio::sync::Notify::new());
    let abort_poll = Arc::new(tokio::sync::Notify::new());
    {
        let pkt_tx = pkt_tx.clone();
        let udp = udp.clone();
        let peer = peer.clone();
        let inbound = inbound.clone();
        let learned = learned.clone();
        let abort = abort_inbound.clone();
        let shared = shared.clone();
        tokio::spawn(async move {
            loop {
                // A-4:池槽收包,随包整槽移交
                let mut buf = pkt_pool_get();
                tokio::select! {
                    r = udp.recv_from(&mut buf) => {
                        let Ok((n, from)) = r else { break };
                        bump_counter!(recvfrom_calls);
                        if n >= KCP_OVERHEAD && (81..=84).contains(&buf[4]) {
                            // A7:只接受服务端下发的候选来源,拒绝打洞窗口内
                            // 第三方注入(否则首个合法包即可劫持 dial 侧)。
                            if !cands_for_inbound.iter().any(|c| c == &from) {
                                tracing::debug!(%from, "punch dial: drop non-candidate source");
                                pkt_pool_put(buf);
                                continue;
                            }
                            if peer.learn(from) {
                                learned.store(true, Ordering::Relaxed);
                                inbound.notify_one();
                            }
                            match pkt_tx.try_send(SessionCmd::Packet(PooledPkt { buf, len: n })) {
                                Ok(()) => {}
                                Err(mpsc::error::TrySendError::Full(p)) => {
                                    // 满丢包:kcp 重传兜底,不断流;槽回收
                                    let SessionCmd::Packet(PooledPkt { buf, .. }) = p;
                                    pkt_pool_put(buf);
                                }
                                Err(mpsc::error::TrySendError::Closed(_)) => break,
                            }
                        } else {
                            pkt_pool_put(buf);
                        }
                    }
                    _ = abort.notified() => break,
                    _ = shared.shutdown.notified() => break,
                }
            }
        });
    }
    // 原始打洞包 + 候选轮询(learn 前;失败时经 abort 停止)。peer 轮换
    // 经 PunchPeer 状态机:learn 后为 no-op,不会覆写已锁定地址。
    {
        let udp = udp.clone();
        let peer = peer.clone();
        let learned = learned.clone();
        let abort = abort_poll.clone();
        let candidates = candidates.to_vec();
        tokio::spawn(async move {
            let mut i = 0usize;
            loop {
                if learned.load(Ordering::Relaxed) {
                    break;
                }
                for c in &candidates {
                    let _ = udp.try_send_to(b"RHPUNCH", *c);
                }
                peer.rotate(candidates[i % candidates.len()]);
                i += 1;
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_millis(800)) => {}
                    _ = abort.notified() => break,
                }
            }
        });
    }
    // 取消所有权守卫:自此刻起,任何非成功离开(含 parent drop/abort)
    // 都关停两个辅助任务;成功路径在返回前 disarm。
    let mut _cancel = PunchCancel {
        abort_inbound: abort_inbound.clone(),
        abort_poll: abort_poll.clone(),
        armed: true,
    };
    // 打洞包 = 哨兵消息:kcp 未确认前持续重传,listen 端据其建会话并剥离。
    {
        let mut s = shared.guarded.lock().unwrap();
        s.write_bytes += b"RHTP-PUNCH!".len();
        s.write_seq += 1;
        s.write_queue
            .push_back(WriteChunk::new(b"RHTP-PUNCH!".to_vec()));
        drop(s);
        shared.notify.notify_one();
    }
    tokio::spawn(run_session(kcp, pkt_rx, shared));
    // 等待 learn(kcp 会话可用以开流;失败回退中继)
    let deadline = tokio::time::Instant::now() + Duration::from_secs(seconds.max(3));
    loop {
        if learned.load(Ordering::Relaxed) {
            // 成功:解除武装 —— learned 后轮询任务自退,入站任务随会话
            // 生命周期(shutdown 收敛),不得误杀
            _cancel.armed = false;
            return Ok(wrap_stream_as_client(
                Box::new(io),
                true,
                MUX_STREAMS_UNLIMITED,
            ));
        }
        if tokio::time::Instant::now() >= deadline {
            // 失败收尾:返回 Err 时 RAII 守卫 Drop 以 notify_one(permit
            // 语义)关停两个辅助任务 —— 任务处于任意执行段(含「检查
            // 后、select 注册前」窗口)也能在下一次 await 消费到通知,
            // 不会永久泄漏任务+socket 并持续发包。(返回后 io 被 drop,
            // 会话任务随之退出)
            return Err(other_err("punch timeout"));
        }
        tokio::select! {
            _ = inbound.notified() => continue,
            _ = tokio::time::sleep(Duration::from_millis(200)) => continue,
        }
    }
}

fn rand_u32() -> u32 {
    use rand::RngCore;
    rand::rng().next_u32()
}

#[cfg(test)]
mod tests {
    use crate::{connect, listen, ClientTransportConfig, Protocol, ServerTransportConfig};
    use std::time::Duration;

    #[tokio::test]
    async fn kcp_session_streams() {
        let server_cfg = ServerTransportConfig {
            protocol: Protocol::Kcp,
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            ..Default::default()
        };
        let mut listener = listen(
            &server_cfg,
            std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        )
        .await
        .unwrap();
        let addr = listener.local_addr;

        let srv = tokio::spawn(async move {
            let inc = listener.accept().await.unwrap();
            let mut s = inc.mux.accept().await.unwrap();
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let mut buf = [0u8; 12];
            s.read_exact(&mut buf).await.unwrap();
            s.write_all(&buf).await.unwrap();
            // 多流验证
            let mut s2 = inc.mux.accept().await.unwrap();
            let mut b2 = [0u8; 4];
            s2.read_exact(&mut b2).await.unwrap();
            s2.write_all(b"done").await.unwrap();
        });

        let cfg = ClientTransportConfig {
            protocol: Protocol::Kcp,
            server_addr: addr,
            ..Default::default()
        };
        let mux = connect(&cfg).await.unwrap();
        let mut s = mux.open_stream().await.unwrap();
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        s.write_all(b"kcp-hello-ok").await.unwrap();
        let mut back = [0u8; 12];
        s.read_exact(&mut back).await.unwrap();
        assert_eq!(&back, b"kcp-hello-ok");

        let mut s2 = mux.open_stream().await.unwrap();
        s2.write_all(b"ping").await.unwrap();
        let mut b2 = [0u8; 4];
        tokio::time::timeout(Duration::from_secs(20), s2.read_exact(&mut b2))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&b2, b"done");
        srv.abort();
    }
}

#[cfg(test)]
mod punch_tests {
    use super::*;

    use std::time::Duration;

    #[tokio::test]
    async fn punch_dial_listen_loopback() {
        let listen_sock = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let cand = listen_sock.local_addr().unwrap();
        let dial_sock = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let dial_addr = dial_sock.local_addr().unwrap();
        // A7:listen 侧只接受候选来源(此处即 dial 的本地地址)
        let listener = tokio::spawn(async move { punch_listen(listen_sock, &[dial_addr]).await });
        let dialer = tokio::spawn(async move { punch_dial(dial_sock, &[cand], 8).await });
        // 服务端先就位,接受首个流并回显
        let srv = tokio::spawn(async move {
            let mux = match listener.await {
                Ok(Ok(m)) => m,
                _ => return,
            };
            if let Ok(mut s) = mux.accept().await {
                use tokio::io::{AsyncReadExt, AsyncWriteExt};
                let mut b = [0u8; 8];
                if s.read_exact(&mut b).await.is_ok() {
                    let _ = s.write_all(&b).await;
                }
            }
        });
        let mux = tokio::time::timeout(Duration::from_secs(10), dialer)
            .await
            .expect("dial join")
            .expect("dial task")
            .expect("punch dial");
        let mut s = mux.open_stream().await.expect("open");
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        s.write_all(b"punch-ok").await.unwrap();
        let mut back = [0u8; 8];
        tokio::time::timeout(Duration::from_secs(10), s.read_exact(&mut back))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&back, b"punch-ok");
        let _ = srv.await;
    }
}

#[cfg(test)]
mod kcp_regress {
    //! kcp.rs 有界发送/批量读取修复的回归测试。
    //! t01–t04 为「修复前必须失败」的行为回归(验证方式见
    //! benchmark/runs/goal-repair/kcp-improvement/);其余为单元
    //! (积压队列)与既有行为守护。全部默认运行且带 deadline。

    use super::*;
    use futures::task::noop_waker;
    use std::sync::atomic::AtomicBool;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn poll_w(io: &mut KcpIo, buf: &[u8]) -> Poll<std::io::Result<usize>> {
        let w = noop_waker();
        let mut cx = Context::from_waker(&w);
        Pin::new(io).poll_write(&mut cx, buf)
    }

    fn poll_r(io: &mut KcpIo, buf: &mut [u8]) -> (Poll<std::io::Result<()>>, usize) {
        let w = noop_waker();
        let mut cx = Context::from_waker(&w);
        let mut rb = ReadBuf::new(buf);
        let p = Pin::new(io).poll_read(&mut cx, &mut rb);
        (p, rb.filled().len())
    }

    /// 记录 kcp 输出的裸 Write(供 run_session 驱动真实 KCP)。
    struct RecOut(Arc<Mutex<Vec<u8>>>);
    impl Write for RecOut {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    fn rec_contains(rec: &Arc<Mutex<Vec<u8>>>, needle: &[u8]) -> bool {
        rec.lock()
            .unwrap()
            .windows(needle.len())
            .any(|w| w == needle)
    }

    fn rec_pos(rec: &Arc<Mutex<Vec<u8>>>, needle: &[u8]) -> Option<usize> {
        let g = rec.lock().unwrap();
        g.windows(needle.len()).position(|w| w == needle)
    }

    // ---------- A:写 → notify → 立即 flush(修复前失败) ----------

    /// poll_write 入队后必须立刻通知会话任务,不能只等 2ms tick。
    #[tokio::test(start_paused = true)]
    async fn t01_poll_write_notifies_session_immediately() {
        let (mut io, shared) = new_io();
        assert!(matches!(poll_w(&mut io, b"hello"), Poll::Ready(Ok(5))));
        // 暂停时钟:若只能靠 tick 唤醒,1ms 内 notified 不会完成
        let got = tokio::time::timeout(Duration::from_millis(1), shared.notify.notified()).await;
        assert!(got.is_ok(), "poll_write 入队后必须 notify 会话任务");
    }

    /// 真实 KCP:写后不经 tick/interval 推进即输出(flush 不被 ts_flush 挡)。
    #[tokio::test(start_paused = true)]
    async fn t02_write_flushes_without_tick_or_interval() {
        let (mut io, shared) = new_io();
        let rec: Arc<Mutex<Vec<u8>>> = Arc::new(Mutex::new(Vec::new()));
        let mut kcp = Kcp::new_stream(7, RecOut(rec.clone()));
        kcp.set_mtu(MTU).ok();
        kcp.set_wndsize(256, 256);
        kcp.set_nodelay(true, 2, 2, true);
        let (_tx, rx) = mpsc::channel::<SessionCmd>(4);
        let task = tokio::spawn(run_session(kcp, rx, shared));
        // 先让会话完成首轮(interval 首个 tick 立即就绪)并停在 select:
        // 否则 before 代码会被首个立即 tick 顺路拾取,测试失效
        yield_ensure_scheduled().await;
        let t0 = tokio::time::Instant::now();
        const P: &[u8] = b"kcp-fast-flush";
        assert!(matches!(poll_w(&mut io, P), Poll::Ready(Ok(_))));
        // 只让任务调度,不推进时间(yield 不触发 auto-advance)
        let mut found = false;
        for _ in 0..10_000 {
            if rec_contains(&rec, P) {
                found = true;
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(found, "写后必须在不等 tick/interval 的情况下输出");
        assert_eq!(
            tokio::time::Instant::now(),
            t0,
            "不得依赖时间推进(若依赖说明仍在等 tick/interval)"
        );
        task.abort();
    }

    /// 多次写合并为一轮拾取,通知不丢、顺序保持(修复前失败)。
    #[tokio::test(start_paused = true)]
    async fn t03_multiple_writes_one_drain_all_delivered_in_order() {
        let (mut io, shared) = new_io();
        let rec: Arc<Mutex<Vec<u8>>> = Arc::new(Mutex::new(Vec::new()));
        let mut kcp = Kcp::new_stream(8, RecOut(rec.clone()));
        kcp.set_mtu(MTU).ok();
        kcp.set_wndsize(256, 256);
        kcp.set_nodelay(true, 2, 2, true);
        let (_tx, rx) = mpsc::channel::<SessionCmd>(4);
        let task = tokio::spawn(run_session(kcp, rx, shared));
        yield_ensure_scheduled().await;
        const P1: &[u8] = b"first-chunk-a";
        const P2: &[u8] = b"second-chunk-b";
        const P3: &[u8] = b"third-chunk-c";
        assert!(matches!(poll_w(&mut io, P1), Poll::Ready(Ok(_))));
        assert!(matches!(poll_w(&mut io, P2), Poll::Ready(Ok(_))));
        assert!(matches!(poll_w(&mut io, P3), Poll::Ready(Ok(_))));
        let mut ok = false;
        for _ in 0..10_000 {
            if rec_contains(&rec, P1) && rec_contains(&rec, P2) && rec_contains(&rec, P3) {
                ok = true;
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(ok, "一次唤醒必须拾取全部积压写");
        let (a, b, c) = (rec_pos(&rec, P1), rec_pos(&rec, P2), rec_pos(&rec, P3));
        assert!(
            a < b && b < c,
            "流式输出顺序必须保持: {a:?} < {b:?} < {c:?}"
        );
        task.abort();
    }

    // ---------- C:写队列按字节上限回压(修复前失败) ----------

    /// A-3(任务书):聚合批 FIFO —— 空闲时逐包入批、flush_now 依序
    /// 发出、发尽即清。跨平台:Linux 走 sendmmsg,其余走逐包回退
    /// (语义断言一致)。
    #[tokio::test]
    async fn a03_send_agg_fifo_delivery() {
        let rx_sock = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let addr = rx_sock.local_addr().unwrap();
        let tx_sock = Arc::new(tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let backlog = Arc::new(UdpBacklog::new());
        // 直接构造(生产经 SendAgg::new,仅 Linux 返回 Some;测试要跨平台)
        let agg = Arc::new(SendAgg {
            sock: tx_sock.clone(),
            dest: Dest::Peer(addr),
            backlog: Arc::downgrade(&backlog),
            queue: std::sync::Mutex::new(AggQueue::default()),
        });
        // prime tokio 就绪缓存(生产形态:会话任务先经 select await;
        // 未 prime 时首次 try_* 恒 WouldBlock,首批本就回落 backlog)
        tx_sock.writable().await.unwrap();
        // 3 个可区分数据报:入批 → 冲刷 → 接收序一致
        let mut expect = Vec::new();
        for i in 0..3u8 {
            let pkt = vec![i; 64];
            agg.push(&pkt);
            expect.push(pkt);
        }
        agg.flush_now().unwrap();
        for want in &expect {
            let mut b = [0u8; 128];
            let (n, from) =
                tokio::time::timeout(std::time::Duration::from_secs(2), rx_sock.recv_from(&mut b))
                    .await
                    .unwrap()
                    .unwrap();
            assert_eq!(from, tx_sock.local_addr().unwrap());
            assert_eq!(&b[..n], want.as_slice(), "聚合批发送顺序必须与入批一致");
        }
        {
            let q = agg.queue.lock().unwrap();
            assert!(q.lens.is_empty(), "全部发出后聚合批必须清空");
        }
        assert!(backlog.is_empty(), "空闲冲刷不得残留 backlog");

        // 满载回落(EAGAIN 余段转 backlog)不做真实 socket 洪泛断言:
        // loopback UDP 在 macOS 与 Linux(test1/CI ubuntu 实测 4096×1400B
        // 收紧 sndbuf 后全部发送成功)都是接收满即静默丢包、不回压发送
        // 端,断言依赖内核调参必假绿或必红。回落路径的行为与顺序性由
        // a03_send_agg_transfer_keeps_order 直接调用内部路径确定性钉住。
    }

    /// A-3:余段转移的 FIFO —— transfer_to_backlog 必须把聚合缓冲内
    /// 全部数据报按序转入 backlog(部分提交/EAGAIN 回落的顺序性)。
    #[tokio::test]
    async fn a03_send_agg_transfer_keeps_order() {
        let tx_sock = Arc::new(tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let backlog = Arc::new(UdpBacklog::new());
        let agg = Arc::new(SendAgg {
            sock: tx_sock,
            dest: Dest::Connected,
            backlog: Arc::downgrade(&backlog),
            queue: std::sync::Mutex::new(AggQueue::default()),
        });
        let mut q = AggQueue::default();
        for i in 0..5u8 {
            q.buf.extend_from_slice(&[i; 32]);
            q.lens.push(32);
        }
        agg.transfer_to_backlog(&mut q);
        assert!(q.lens.is_empty() && q.buf.is_empty(), "转移后聚合缓冲清空");
        let state = backlog.state.lock().unwrap();
        assert_eq!(state.len(), 5, "全部余段转入 backlog");
        for (i, pkt) in state.iter().enumerate() {
            assert_eq!(pkt, &vec![i as u8; 32], "backlog 内顺序与入批一致");
        }
    }

    /// A-2(任务书):大项 + 受限预算(每轮 1 段,模拟慢 ACK 链路)分多轮
    /// 消费,数据逐字节完整、记账同步清零、消费尽才弹条目 —— 钉住
    /// 游标化的正确性(修前 split_off 语义等价但每片切尾重分配)。
    #[test]
    fn a02_write_queue_piecemeal_budget_integrity() {
        let (_io, shared) = new_io();
        let payload: Vec<u8> = (0..48 * 1024u32).map(|i| (i % 251) as u8).collect();
        {
            let mut s = shared.guarded.lock().unwrap();
            s.write_bytes = payload.len();
            s.write_seq = 1;
            s.write_queue.push_back(WriteChunk::new(payload.clone()));
        }
        const MSS: usize = 1200;
        let mut rebuilt = Vec::with_capacity(payload.len());
        let mut rounds = 0usize;
        loop {
            let (pieces, drained) = {
                let mut s = shared.guarded.lock().unwrap();
                let p = s.take_send_pieces(1, MSS, SEND_PIECE);
                let d = s.write_queue.is_empty() && s.write_bytes == 0;
                (p, d)
            };
            if pieces.is_empty() {
                assert!(drained, "无片可取时队列与记账必须已清空");
                break;
            }
            for p in pieces {
                rebuilt.extend_from_slice(&p);
            }
            rounds += 1;
        }
        assert_eq!(rebuilt, payload, "分片消费必须逐字节还原");
        assert!(
            rounds >= payload.len() / MSS,
            "确需多轮受限消费才消费完(实际 {rounds} 轮)"
        );
    }

    /// 条数远未到 256 时,字节上限(256KiB)必须先触发 Pending。
    #[test]
    fn t04_write_queue_byte_cap_applies_backpressure() {
        let (mut io, shared) = new_io();
        let big = vec![0u8; 64 * 1024];
        for _ in 0..4 {
            assert!(
                matches!(poll_w(&mut io, &big), Poll::Ready(Ok(n)) if n == 64 * 1024),
                "4×64KiB 未达上限,写应成功"
            );
        }
        assert!(
            matches!(poll_w(&mut io, &big), Poll::Pending),
            "256KiB 字节上限必须生效(条数仅 5 << 256)"
        );
        // 会话侧清空队列后写必须恢复
        {
            let mut s = shared.guarded.lock().unwrap();
            while let Some(chunk) = s.write_queue.pop_front() {
                s.write_bytes = s.write_bytes.saturating_sub(chunk.remaining());
            }
            s.wake_write();
        }
        assert!(
            matches!(poll_w(&mut io, &big), Poll::Ready(Ok(n)) if n == 64 * 1024),
            "清空队列后写应恢复"
        );
    }

    /// poll_read 两段拷贝语义:环绕 deque、空 ReadBuf、EOF 全部保持。
    #[test]
    fn t05_poll_read_segment_copy_semantics() {
        let (mut io, shared) = new_io();
        {
            let mut s = shared.guarded.lock().unwrap();
            for i in 0..100u8 {
                s.read_buf.push_back(i);
            }
            for _ in 0..90 {
                s.read_buf.pop_front();
            }
            for i in 200..250u8 {
                s.read_buf.push_back(i);
            }
        }
        {
            let s = shared.guarded.lock().unwrap();
            let (a, b) = s.read_buf.as_slices();
            assert!(!a.is_empty() && !b.is_empty(), "前置:deque 必须环绕成两段");
        }
        let mut expect = Vec::new();
        expect.extend(90u8..100);
        expect.extend(200..250);
        let mut out = [0u8; 60];
        let (p, n) = poll_r(&mut io, &mut out);
        assert!(matches!(p, Poll::Ready(Ok(()))));
        assert_eq!(n, 60);
        assert_eq!(&out[..n], &expect[..]);
        // 空 buffer + 有数据 → Ready(Ok(())) 且不消费数据
        {
            let mut s = shared.guarded.lock().unwrap();
            s.read_buf.push_back(0xEE);
        }
        let (p, n) = poll_r(&mut io, &mut []);
        assert!(matches!(p, Poll::Ready(Ok(()))));
        assert_eq!(n, 0);
        {
            let s = shared.guarded.lock().unwrap();
            assert_eq!(s.read_buf.len(), 1, "空 buffer 不得消费数据");
        }
        // 关闭后读完剩余 → 再读 → EOF(Ready Ok + 0 字节)
        let mut rest = [0u8; 8];
        let (p, n) = poll_r(&mut io, &mut rest);
        assert!(matches!(p, Poll::Ready(Ok(()))));
        assert_eq!(n, 1);
        assert_eq!(rest[0], 0xEE);
        {
            let mut s = shared.guarded.lock().unwrap();
            s.closed = true;
        }
        let (p, n) = poll_r(&mut io, &mut rest);
        assert!(
            matches!(p, Poll::Ready(Ok(()))),
            "关闭后必须 EOF 而非 Pending"
        );
        assert_eq!(n, 0);
    }

    /// 慢消费者:队列满 Pending 注册 waker,会话排空后必须唤醒。
    #[test]
    fn t06_write_waker_fires_after_drain() {
        struct Fw(Arc<AtomicBool>);
        impl std::task::Wake for Fw {
            fn wake(self: Arc<Self>) {
                self.0.store(true, Ordering::Release);
            }
            fn wake_by_ref(self: &Arc<Self>) {
                self.0.store(true, Ordering::Release);
            }
        }
        let (mut io, shared) = new_io();
        let flag = Arc::new(AtomicBool::new(false));
        let waker = Waker::from(Arc::new(Fw(flag.clone())));
        let mut cx = Context::from_waker(&waker);
        let big = vec![0u8; 64 * 1024];
        {
            let mut s = shared.guarded.lock().unwrap();
            for _ in 0..4 {
                s.write_bytes += big.len();
                s.write_queue.push_back(WriteChunk::new(big.clone()));
            }
        }
        assert!(matches!(
            Pin::new(&mut io).poll_write(&mut cx, &big),
            Poll::Pending
        ));
        assert!(!flag.load(Ordering::Acquire));
        {
            let mut s = shared.guarded.lock().unwrap();
            while let Some(chunk) = s.write_queue.pop_front() {
                s.write_bytes = s.write_bytes.saturating_sub(chunk.remaining());
            }
            s.wake_write();
        }
        assert!(flag.load(Ordering::Acquire), "排空队列后必须唤醒等待的写者");
        assert!(matches!(
            Pin::new(&mut io).poll_write(&mut cx, &big),
            Poll::Ready(Ok(_))
        ));
    }

    /// 句柄全部释放后会话任务必须及时退出(不空转到空闲超时)。
    #[tokio::test(start_paused = true)]
    async fn t07_session_exits_promptly_when_io_dropped() {
        let (io, shared) = new_io();
        let (_tx, rx) = mpsc::channel::<SessionCmd>(4);
        let rec: Arc<Mutex<Vec<u8>>> = Arc::new(Mutex::new(Vec::new()));
        let kcp = Kcp::new_stream(9, RecOut(rec));
        let task = tokio::spawn(run_session(kcp, rx, shared));
        yield_ensure_scheduled().await;
        drop(io);
        tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .expect("句柄归零后会话任务必须退出")
            .expect("join");
    }

    async fn yield_ensure_scheduled() {
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
    }

    // ---------- B:UDP 输出积压 ----------

    /// WouldBlock:包头放回队头,后续可在可写后重试;已发的计入 sent。
    #[test]
    fn t08_drain_once_wouldblock_requeues_for_retry() {
        let bl = UdpBacklog::new();
        bl.push(b"one");
        bl.push(b"two");
        let mut first = true;
        let st = drain_once(&bl, |pkt| {
            if first {
                first = false;
                Ok(pkt.len())
            } else {
                Err(std::io::Error::from(std::io::ErrorKind::WouldBlock))
            }
        });
        assert!(matches!(st, DrainSt::Blocked));
        assert_eq!(bl.sent.load(Ordering::Relaxed), 1);
        {
            let q = bl.state.lock().unwrap();
            assert_eq!(q.len(), 1);
            assert_eq!(q.front().unwrap(), b"two", "WouldBlock 的包必须回队头");
        }
        let st = drain_once(&bl, |pkt| Ok(pkt.len()));
        assert!(matches!(st, DrainSt::Empty));
        assert!(bl.is_empty());
        assert_eq!(bl.sent.load(Ordering::Relaxed), 2);
    }

    /// 真实发送错误:致命化 —— 记录错误、停止本轮,错误可经
    /// fatal_error()/stats() 观察,由会话终止传播(修复前:仅计数后
    /// continue 吞掉,违反不吞真实错误约束)。
    #[test]
    fn t09_drain_once_real_error_is_fatal_and_observable() {
        let bl = UdpBacklog::new();
        bl.push(b"a");
        bl.push(b"b");
        let st = drain_once(&bl, |_| std::io::Result::Err(std::io::Error::other("boom")));
        assert!(matches!(st, DrainSt::Fatal), "真实错误必须致命而非继续");
        assert!(
            bl.fatal_error().as_deref().unwrap_or("").contains("boom"),
            "错误必须可观察: {:?}",
            bl.fatal_error()
        );
        {
            let q = bl.state.lock().unwrap();
            assert_eq!(q.len(), 1, "致命后剩余队列交还所有者回收,不再继续发");
            assert_eq!(q.front().unwrap(), b"b");
        }
        assert_eq!(bl.sent.load(Ordering::Relaxed), 0);
        assert_eq!(bl.stats().fatal, 1);
    }

    /// 短写(UDP 不应出现):致命错误,不当作发送成功。
    #[test]
    fn t10_drain_once_short_write_is_fatal() {
        let bl = UdpBacklog::new();
        bl.push(b"abcdef");
        let st = drain_once(&bl, |_| Ok(3));
        assert!(matches!(st, DrainSt::Fatal), "短写必须致命");
        assert!(
            bl.fatal_error()
                .as_deref()
                .unwrap_or("")
                .contains("short write"),
            "短写原因必须可观察: {:?}",
            bl.fatal_error()
        );
    }

    /// 真实 socket:kernel 缓冲满 → 入积压而非丢弃;对端消费后经
    /// writable 通知按序发出。不 setsockopt,用默认缓冲灌满。
    #[tokio::test]
    async fn t11_udp_output_wouldblock_backlogs_then_sends() {
        let peer = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let peer_addr = peer.local_addr().unwrap();
        let sender = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let pkt = vec![0x7Bu8; MTU];
        // 对端不读,灌满发送通路直到 WouldBlock(默认缓冲必然有限)
        let mut blocked = false;
        for _ in 0..200_000 {
            match sender.try_send_to(&pkt, peer_addr) {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    blocked = true;
                    break;
                }
                Err(_) => break,
            }
        }
        assert!(blocked, "loopback 默认缓冲下灌满应出现 WouldBlock");
        let backlog = Arc::new(UdpBacklog::new());
        let mut out = UdpOutput {
            sock: sender.clone(),
            peer: peer_addr,
            connected: false,
            backlog: backlog.clone(),
        };
        let n = out.write(&pkt).expect("WouldBlock 必须假装成功(kcp 合同)");
        assert_eq!(n, pkt.len());
        assert_eq!(backlog.enqueued.load(Ordering::Relaxed), 1);
        assert!(!backlog.is_empty(), "包必须入积压而不是丢弃");
        assert!(backlog.fatal_error().is_none(), "容量排队不是错误");
        // 对端开始消费 → drain 任务经 writable 通知清空积压
        spawn_udp_backlog_drain(sender, Dest::Peer(peer_addr), backlog.clone());
        let reader = tokio::spawn(async move {
            let mut buf = vec![0u8; 65536];
            loop {
                if peer.recv_from(&mut buf).await.is_err() {
                    break;
                }
            }
        });
        let deadline = Instant::now() + Duration::from_secs(10);
        while backlog.sent.load(Ordering::Relaxed) < 1 || !backlog.is_empty() {
            assert!(Instant::now() < deadline, "对端消费后积压必须在 10s 内发出");
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        reader.abort();
    }

    /// 容量按字节+条数有界;超限丢弃必须计数且可经 stats() 观察。
    #[test]
    fn t12_backlog_capacity_bounded_and_counted() {
        let bl = UdpBacklog::new();
        let pkt = vec![0u8; 1200];
        let attempts = MAX_BACKLOG_PACKETS + 10;
        let mut ok = 0;
        for _ in 0..attempts {
            if bl.push(&pkt) {
                ok += 1;
            }
        }
        assert_eq!(ok, MAX_BACKLOG_BYTES / 1200, "字节上限先生效");
        assert!(bl.bytes() <= MAX_BACKLOG_BYTES);
        assert_eq!(
            bl.dropped_full.load(Ordering::Relaxed) as usize,
            attempts - ok
        );
        assert_eq!(bl.enqueued.load(Ordering::Relaxed) as usize, ok);
        // 诊断快照:测试/日志可读,不是无人观察的 private atomic
        let s = bl.stats();
        assert_eq!(s.dropped_full, (attempts - ok) as u64);
        assert_eq!(s.enqueued, ok as u64);
        assert_eq!(s.queued_packets, ok);
        assert_eq!(s.queued_bytes, ok * 1200);
        assert_eq!(s.inflight_bytes, 0);
    }

    // ---------- 集成:echo 完整性 / 并发 / 压缩兼容 ----------

    /// 4 条流并发 × 512KiB 伪随机数据回显,逐字节校验(含压缩层兼容)。
    #[tokio::test]
    async fn t13_kcp_concurrent_echo_integrity() {
        run_echo_integrity(false).await;
    }

    #[tokio::test]
    async fn t14_kcp_compress_compat_roundtrip() {
        run_echo_integrity(true).await;
    }

    async fn run_echo_integrity(compress: bool) {
        let server_cfg = ServerTransportConfig {
            protocol: Protocol::Kcp,
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            ..Default::default()
        };
        let mut listener = listen(
            &server_cfg,
            std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        )
        .await
        .unwrap();
        let addr = listener.local_addr();
        let srv = tokio::spawn(async move {
            let inc = listener.accept().await.unwrap();
            for _ in 0..4 {
                let mut s = inc.mux.accept().await.unwrap();
                if compress {
                    s = crate::compress::compress_stream(s).await;
                }
                tokio::spawn(async move {
                    let mut buf = vec![0u8; 16 * 1024];
                    loop {
                        match s.read(&mut buf).await {
                            Ok(0) | Err(_) => break,
                            Ok(n) => {
                                if s.write_all(&buf[..n]).await.is_err() {
                                    break;
                                }
                            }
                        }
                    }
                });
            }
        });
        let cfg = ClientTransportConfig {
            protocol: Protocol::Kcp,
            server_addr: addr,
            ..Default::default()
        };
        let mux = connect(&cfg).await.unwrap();
        let mut tasks = Vec::new();
        const TOTAL: u64 = 512 * 1024;
        for stream_id in 0..4u64 {
            let mut s = mux.open_stream().await.unwrap();
            if compress {
                s = crate::compress::compress_stream(s).await;
            }
            tasks.push(tokio::spawn(async move {
                let (mut r, mut w) = tokio::io::split(s);
                let verifier = tokio::spawn(async move {
                    let mut x: u64 = 0x2545F4914F6CDD1D ^ stream_id;
                    let mut got = 0u64;
                    let mut buf = vec![0u8; 16 * 1024];
                    while got < TOTAL {
                        let want = buf.len().min((TOTAL - got) as usize);
                        r.read_exact(&mut buf[..want]).await.expect("echo read");
                        for b in &buf[..want] {
                            x ^= x << 13;
                            x ^= x >> 7;
                            x ^= x << 17;
                            assert_eq!(
                                *b,
                                (x >> 24) as u8,
                                "stream {stream_id} 偏移 {got} 数据损坏"
                            );
                        }
                        got += want as u64;
                    }
                });
                let mut x: u64 = 0x2545F4914F6CDD1D ^ stream_id;
                let mut chunk = vec![0u8; 16 * 1024];
                let mut written = 0u64;
                while written < TOTAL {
                    let n = chunk.len().min((TOTAL - written) as usize);
                    for b in &mut chunk[..n] {
                        x ^= x << 13;
                        x ^= x >> 7;
                        x ^= x << 17;
                        *b = (x >> 24) as u8;
                    }
                    w.write_all(&chunk[..n]).await.expect("write");
                    written += n as u64;
                }
                verifier.await.expect("verifier join");
            }));
        }
        let all = tokio::time::timeout(Duration::from_secs(60), async {
            for t in tasks {
                t.await.expect("stream task");
            }
        });
        all.await.expect("echo 必须在 60s 内完成");
        srv.abort();
    }

    /// 诊断微基准(仅打印,不设时间断言):新两段拷贝 vs 旧
    /// drain→collect 临时 Vec 的 poll_read 消耗。--nocapture 查看。
    #[test]
    fn t15_micro_poll_read_drain_print() {
        let (mut io, shared) = new_io();
        const TOTAL: usize = 4 * 1024 * 1024;
        {
            let mut s = shared.guarded.lock().unwrap();
            let mut x: u64 = 0x9E3779B97F4A7C15;
            for _ in 0..TOTAL {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                s.read_buf.push_back((x >> 24) as u8);
            }
        }
        let mut sink = 0u64;
        let mut buf = vec![0u8; 64 * 1024];
        let t_new = std::time::Instant::now();
        let mut drained = 0usize;
        while drained < TOTAL {
            let (_, n) = poll_r(&mut io, &mut buf);
            assert!(n > 0);
            for b in &buf[..n] {
                sink = sink.wrapping_add(*b as u64);
            }
            drained += n;
        }
        let new_elapsed = t_new.elapsed();
        // 旧算法形状(同数据同机对照):drain(..n).collect() 出临时 Vec
        {
            let mut s = shared.guarded.lock().unwrap();
            let mut x: u64 = 0x9E3779B97F4A7C15;
            for _ in 0..TOTAL {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                s.read_buf.push_back((x >> 24) as u8);
            }
        }
        let t_old = std::time::Instant::now();
        let mut drained = 0usize;
        while drained < TOTAL {
            let n = {
                let mut s = shared.guarded.lock().unwrap();
                let n = s.read_buf.len().min(buf.len());
                let chunk: Vec<u8> = s.read_buf.drain(..n).collect();
                for b in &chunk {
                    sink = sink.wrapping_add(*b as u64);
                }
                n
            };
            drained += n;
        }
        let old_elapsed = t_old.elapsed();
        assert_eq!(drained, TOTAL);
        println!(
            "MICRO poll_read 4MiB: new_two_segment={:?} old_drain_collect={:?} (sink={sink})",
            new_elapsed, old_elapsed
        );
    }
}

/// 主代理复核六项(kcp-main-review 原样断言;独立复现见
/// benchmark/runs/goal-repair/kcp-main-review/,红日志 red.log)。
/// 修复前 0 passed / 6 failed,现全部纳入工作区默认回归。
#[cfg(test)]
mod main_review_tests {
    use super::*;
    use tokio::io::AsyncWriteExt;

    #[test]
    fn oversized_write_must_make_partial_progress() {
        let (mut io, _) = new_io();
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        let bytes = vec![7; MAX_WRITE_QUEUE_BYTES + 1];
        let result = Pin::new(&mut io).poll_write(&mut cx, &bytes);
        assert!(
            matches!(result, Poll::Ready(Ok(n)) if n > 0 && n <= MAX_WRITE_QUEUE_BYTES),
            "empty queue oversized write cannot ever progress: {result:?}"
        );
    }

    #[test]
    fn empty_readbuf_on_idle_stream_must_complete() {
        let (mut io, _) = new_io();
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        let mut empty = [];
        let mut rb = ReadBuf::new(&mut empty);
        let result = Pin::new(&mut io).poll_read(&mut cx, &mut rb);
        assert!(
            matches!(result, Poll::Ready(Ok(()))),
            "zero-capacity read blocks on an idle stream: {result:?}"
        );
    }

    #[test]
    fn failed_send_reinsertion_must_respect_byte_cap() {
        let bl = UdpBacklog::new();
        let packet = vec![4u8; 1024];
        for _ in 0..MAX_BACKLOG_BYTES / packet.len() {
            assert!(bl.push(&packet));
        }
        let result = drain_once(&bl, |_| {
            // 主代理裁定(BOUNDARY-REVIEW-2):此处 assert! 只是构造旧实现
            // 可复现交错的前置,并非「总预算满时必须接受」的生产合同;
            // 总预算(排队+在途)记账下该 push 被拒绝是正确行为。
            let _ = bl.push(&packet);
            Err(std::io::Error::from(std::io::ErrorKind::WouldBlock))
        });
        assert!(matches!(result, DrainSt::Blocked));
        let bytes: usize = bl.state.lock().unwrap().iter().map(Vec::len).sum();
        assert!(
            bytes <= MAX_BACKLOG_BYTES,
            "backlog exceeded byte cap after requeue: {bytes} > {MAX_BACKLOG_BYTES}"
        );
        // 更强:完整预算(排队+在途)在回插后同样不得超上限
        let total = bytes + bl.inflight_bytes.load(Ordering::Relaxed);
        assert!(
            total <= MAX_BACKLOG_BYTES,
            "queued+inflight exceeded byte cap: {total} > {MAX_BACKLOG_BYTES}"
        );
    }

    #[tokio::test]
    async fn draining_datagram_must_not_be_overtaken() {
        let peer = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let peer_addr = peer.local_addr().unwrap();
        let sock = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        sock.writable().await.unwrap();
        let backlog = Arc::new(UdpBacklog::new());
        assert!(backlog.push(b"old"));
        let mut output = UdpOutput {
            sock: sock.clone(),
            peer: peer_addr,
            connected: false,
            backlog: backlog.clone(),
        };
        let state = drain_once(&backlog, |packet| {
            assert_eq!(output.write(b"new").unwrap(), 3);
            sock.try_send_to(packet, peer_addr)
        });
        assert!(matches!(state, DrainSt::Empty));
        let mut buf = [0u8; 16];
        let (n, _) = tokio::time::timeout(Duration::from_secs(1), peer.recv_from(&mut buf))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            &buf[..n],
            b"old",
            "new datagram bypassed in-flight FIFO head"
        );
    }

    #[tokio::test]
    async fn idle_backlog_worker_must_release_after_producer_drop() {
        let peer = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let sock = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let backlog = Arc::new(UdpBacklog::new());
        let weak_sock = Arc::downgrade(&sock);
        let weak_backlog = Arc::downgrade(&backlog);
        spawn_udp_backlog_drain(
            sock.clone(),
            Dest::Peer(peer.local_addr().unwrap()),
            backlog.clone(),
        );
        for _ in 0..32 {
            tokio::task::yield_now().await;
        }
        drop(sock);
        drop(backlog);
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(weak_backlog.upgrade().is_none() && weak_sock.upgrade().is_none(), "idle worker retained both backlog and UDP socket after producer disappeared: backlog={} socket={}", weak_backlog.strong_count(), weak_sock.strong_count());
    }

    struct DiscardOutput;
    impl Write for DiscardOutput {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    struct RecordOutput(Arc<Mutex<Vec<u8>>>);
    impl Write for RecordOutput {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    /// 主代理 SHUTDOWN-ADDENDUM 断言(原样):显式 shutdown 成功返回
    /// 后丢弃已接受字节 = 契约缺口。修复前:poll_shutdown 无条件 Ready,
    /// run_session 先见 handles==0 退出,payload 从未到输出器。
    /// 修复后:shutdown 在交付完成前 Pending(本测试无 ACK 对端 →
    /// 走 else 分支提前返回,安全性由 b09 真实对端闭合)。
    #[tokio::test(start_paused = true)]
    async fn successful_shutdown_must_not_discard_accepted_bytes() {
        let (mut io, shared) = new_io();
        let output = Arc::new(Mutex::new(Vec::new()));
        let mut kcp = Kcp::new_stream(988, RecordOutput(output.clone()));
        kcp.set_mtu(MTU).unwrap();
        kcp.set_nodelay(true, 2, 2, true);
        let (_tx, rx) = mpsc::channel(8);
        let task = tokio::spawn(run_session(kcp, rx, shared));
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
        let payload = b"must-deliver-before-successful-shutdown";
        io.write_all(payload).await.unwrap();
        let shutdown = tokio::time::timeout(Duration::from_millis(20), io.shutdown()).await;
        let Ok(Ok(())) = shutdown else {
            task.abort();
            return;
        };
        drop(io);
        for _ in 0..32 {
            tokio::task::yield_now().await;
        }
        let output = output.lock().unwrap();
        let delivered = output.windows(payload.len()).any(|w| w == payload);
        task.abort();
        assert!(
            delivered,
            "shutdown returned Ok, then dropping io discarded all previously accepted bytes"
        );
    }

    #[tokio::test]
    async fn no_ack_peer_must_backpressure_total_pending_writes() {
        let (mut io, shared) = new_io();
        let (tx, rx) = mpsc::channel::<SessionCmd>(4);
        let mut kcp = Kcp::new_stream(987, DiscardOutput);
        kcp.set_mtu(MTU).unwrap();
        kcp.set_wndsize(256, 256);
        kcp.set_nodelay(true, 2, 2, true);
        let task = tokio::spawn(run_session(kcp, rx, shared));
        let chunk = vec![3; 64 * 1024];
        let mut accepted = 0usize;
        for _ in 0..64 {
            if tokio::time::timeout(Duration::from_millis(50), io.write_all(&chunk))
                .await
                .is_err()
            {
                break;
            }
            accepted += chunk.len();
            tokio::task::yield_now().await;
        }
        task.abort();
        let _ = task.await;
        drop(tx);
        assert!(accepted < 4 * 1024 * 1024, "no ACK ever received, but all {accepted} bytes were accepted: cap only covers front queue, not KCP snd_queue");
    }
}

/// 本轮(kcp-boundary)新增回归:边界语义、错误传播、生命周期、
/// 真实 UDP 背压与恢复。b 系列在修复前快照上均失败(红验证见
/// benchmark/runs/goal-repair/kcp-boundary/)。
#[cfg(test)]
mod boundary_tests {
    use super::*;
    use futures::task::noop_waker;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn poll_w(io: &mut KcpIo, buf: &[u8]) -> Poll<std::io::Result<usize>> {
        let w = noop_waker();
        let mut cx = Context::from_waker(&w);
        Pin::new(io).poll_write(&mut cx, buf)
    }

    // ---------- 1/2:超大写部分接受 + 空写/空读一致性 ----------

    /// 空写必须立即 Ok(0) 且不占用条数槽位/字节额度
    /// (修复前:空 Vec 入队,占满 256 条槽位后空写也 Pending)。
    #[test]
    fn b01_empty_write_consumes_no_slot_and_is_ready() {
        let (mut io, _shared) = new_io();
        assert!(
            matches!(poll_w(&mut io, b""), Poll::Ready(Ok(0))),
            "空写必须立即 Ok(0)"
        );
        let one = [7u8; 1];
        for _ in 0..(MAX_WRITE_QUEUE - 1) {
            assert!(matches!(poll_w(&mut io, &one), Poll::Ready(Ok(1))));
        }
        assert!(
            matches!(poll_w(&mut io, b""), Poll::Ready(Ok(0))),
            "空写不得占用条数槽位"
        );
        assert!(
            matches!(poll_w(&mut io, &one), Poll::Ready(Ok(1))),
            "条数槽位不被空写消耗"
        );
    }

    /// 队列非空时超额写接受剩余额度前缀(partial write),
    /// 额度归零才 Pending(与主代理断言 1 互补)。
    #[test]
    fn b02_partial_write_takes_remaining_capacity() {
        let (mut io, shared) = new_io();
        {
            let mut s = shared.guarded.lock().unwrap();
            s.write_bytes = MAX_WRITE_QUEUE_BYTES - 1024;
            s.write_queue.push_back(WriteChunk::new(vec![0u8; 1024]));
        }
        let big = vec![1u8; 64 * 1024];
        let r = poll_w(&mut io, &big);
        assert!(
            matches!(r, Poll::Ready(Ok(1024))),
            "应接受剩余 1KiB 前缀: {r:?}"
        );
        assert!(
            matches!(poll_w(&mut io, b"x"), Poll::Pending),
            "额度归零必须 Pending"
        );
    }

    // ---------- 真实错误/短写:传播而非吞掉 ----------

    /// 积压 drain 遇真实错误后,生产者写必须报错(修复前返回 Ok 吞掉)。
    #[tokio::test]
    async fn b03_real_send_error_poisons_output_write() {
        let bl = Arc::new(UdpBacklog::new());
        bl.push(b"a");
        let _ = drain_once(&bl, |_| std::io::Result::Err(std::io::Error::other("boom")));
        let sock = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let peer = sock.local_addr().unwrap();
        let mut out = UdpOutput {
            sock,
            peer,
            connected: false,
            backlog: bl.clone(),
        };
        let r = out.write(b"x");
        assert!(r.is_err(), "积压真实错误后输出写必须报错(不吞): {r:?}");
        assert!(
            bl.fatal_error().as_deref().unwrap_or("").contains("boom"),
            "错误内容必须可观察: {:?}",
            bl.fatal_error()
        );
    }

    /// 短写(UDP 原子性破坏)同样致命化并传播。
    #[tokio::test]
    async fn b04_short_write_poisons_output_write() {
        let bl = Arc::new(UdpBacklog::new());
        bl.push(b"abcdef");
        let _ = drain_once(&bl, |_| Ok(3));
        let sock = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let peer = sock.local_addr().unwrap();
        let mut out = UdpOutput {
            sock,
            peer,
            connected: false,
            backlog: bl.clone(),
        };
        assert!(out.write(b"x").is_err(), "短写后输出写必须报错");
        assert!(
            bl.fatal_error()
                .as_deref()
                .unwrap_or("")
                .contains("short write"),
            "短写原因必须可观察: {:?}",
            bl.fatal_error()
        );
    }

    /// 会话真实错误必须浮出到 IO 写(修复前统一吞成 "kcp session closed")。
    #[tokio::test(start_paused = true)]
    async fn b05_session_error_surfaces_at_io() {
        struct BoomOutput;
        impl Write for BoomOutput {
            fn write(&mut self, _buf: &[u8]) -> std::io::Result<usize> {
                Err(std::io::Error::other("boom"))
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let (mut io, shared) = new_io();
        let (_tx, rx) = mpsc::channel::<SessionCmd>(4);
        let mut kcp = Kcp::new_stream(11, BoomOutput);
        kcp.set_mtu(MTU).ok();
        kcp.set_wndsize(256, 256);
        kcp.set_nodelay(true, 2, 2, true);
        let task = tokio::spawn(run_session(kcp, rx, shared));
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
        assert!(matches!(poll_w(&mut io, b"hello"), Poll::Ready(Ok(5))));
        // 会话 flush 报错 → 终止并浮出
        let mut surfaced = None;
        for _ in 0..10_000 {
            if let Poll::Ready(Err(e)) = poll_w(&mut io, b"x") {
                surfaced = Some(e);
                break;
            }
            tokio::task::yield_now().await;
        }
        let e = surfaced.expect("会话错误必须传播到 IO 写");
        assert!(
            e.to_string().contains("boom"),
            "错误内容必须保留原始原因: {e}"
        );
        task.abort();
        let _ = task.await;
    }

    // ---------- 生命周期:会话结束释放接收任务与 socket ----------

    /// run_session 结束(用户句柄归零)后,connect 式接收循环必须经
    /// shutdown 退出并释放 socket,不得在无新入包时 recv 永久挂起
    /// (修复前:两任务独立 spawn,接收循环无 owner/退出路径)。
    /// 注:punch/mux 句柄丢弃后由 yamux driver 持续服务存量流直至
    /// 对端关闭或空闲超时,是 mux 层既有语义(见 mux.rs driver),
    /// 不属于会话任务泄漏;本测直接验证会话→接收任务的生命周期。
    #[tokio::test]
    async fn b06_session_end_releases_recv_task_and_socket() {
        let sock_a = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let peer_addr = sock_a.local_addr().unwrap();
        let sock_b = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        sock_b.connect(peer_addr).await.unwrap();
        let (io, shared) = new_io();
        let backlog = Arc::new(UdpBacklog::new());
        let output = UdpOutput {
            sock: sock_b.clone(),
            peer: peer_addr,
            connected: true,
            backlog: backlog.clone(),
        };
        output.spawn_drain();
        drop(backlog);
        let mut kcp = Kcp::new_stream(0x600D_0002, output);
        kcp_conf(&mut kcp);
        let (pkt_tx, pkt_rx) = mpsc::channel::<SessionCmd>(64);
        {
            let shared = shared.clone();
            let pkt_tx = pkt_tx;
            let recv_sock = sock_b.clone();
            tokio::spawn(async move {
                let mut buf = vec![0u8; 65536];
                loop {
                    tokio::select! {
                        r = recv_sock.recv_from(&mut buf) => {
                            let Ok((n, _)) = r else { break };
                            if pkt_tx.send(SessionCmd::Packet(PooledPkt::copy_of(&buf[..n]))).await.is_err() {
                                break;
                            }
                        }
                        _ = shared.shutdown.notified() => break,
                    }
                }
            });
        }
        let session = tokio::spawn(run_session(kcp, pkt_rx, shared));
        // 确认会话与接收任务均已运行(等若干 tick)
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            Arc::strong_count(&sock_b) >= 3,
            "前置:接收任务/输出/drain 持有 socket"
        );
        // 丢弃唯一用户句柄 → 会话结束 → shutdown → 接收任务/drain 退出
        drop(io);
        tokio::time::timeout(Duration::from_secs(2), session)
            .await
            .expect("句柄归零后会话必须退出")
            .expect("join");
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(
            Arc::strong_count(&sock_b),
            1,
            "会话结束后接收任务/drain/输出必须全部释放 socket"
        );
    }

    // ---------- 真实 UDP 背压与恢复(公共 API 之外的裸会话对) ----------

    fn byte_at(p: usize) -> u8 {
        ((p as u64).wrapping_mul(0x9E3779B97F4A7C15) >> 33) as u8
    }

    fn gen_from(pos: usize, len: usize) -> Vec<u8> {
        (pos..pos + len).map(byte_at).collect()
    }

    fn digest_of_positions(total: usize) -> u64 {
        let mut x: u64 = 0x2545F4914F6CDD1D;
        for p in 0..total {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x = x.wrapping_add(byte_at(p) as u64);
        }
        x
    }

    fn kcp_conf(kcp: &mut Kcp<UdpOutput>) {
        kcp.set_mtu(MTU).ok();
        kcp.set_wndsize(256, 256);
        kcp.set_nodelay(true, 2, 2, true);
    }

    /// 真实 UDP 会话对:阶段1 对端只丢包(永不 ACK)→ 写必须停顿且
    /// 接受量有界(跨 front+KCP pending 背压);阶段2 接入真实对端
    /// (同 conv)→ ACK 回流、窗口重开、写恢复,4MiB 逐字节校验。
    /// 修复前:前端限额被无条件搬入无界 snd_queue,永不停顿(红)。
    #[tokio::test]
    async fn b07_real_udp_stalled_peer_backpressure_then_recovery_integrity() {
        const TOTAL: usize = 4 * 1024 * 1024;
        const CHUNK: usize = 64 * 1024;
        let conv = 0x5EC5_0001u32;

        let server_sock = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let server_addr = server_sock.local_addr().unwrap();
        let client_sock = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let client_addr = client_sock.local_addr().unwrap();
        client_sock.connect(server_addr).await.unwrap();

        // ---- 客户端会话(真实 UdpOutput/backlog/drain)----
        let (mut io, shared) = new_io();
        let backlog = Arc::new(UdpBacklog::new());
        let output = UdpOutput {
            sock: client_sock.clone(),
            peer: server_addr,
            connected: true,
            backlog: backlog.clone(),
        };
        output.spawn_drain();
        drop(backlog);
        let mut kcp = Kcp::new_stream(conv, output);
        kcp_conf(&mut kcp);
        // 阶段1 无 ACK 重传不得触发 dead_link(阶段短,200 次足够)
        kcp.set_maximum_resend_times(200);
        let (pkt_tx, pkt_rx) = mpsc::channel::<SessionCmd>(256);
        {
            let shared = shared.clone();
            let pkt_tx = pkt_tx;
            let recv_sock = client_sock.clone();
            tokio::spawn(async move {
                let mut buf = vec![0u8; 65536];
                loop {
                    tokio::select! {
                        r = recv_sock.recv_from(&mut buf) => {
                            let Ok((n, _)) = r else { break };
                            if pkt_tx.send(SessionCmd::Packet(PooledPkt::copy_of(&buf[..n]))).await.is_err() {
                                break;
                            }
                        }
                        _ = shared.shutdown.notified() => break,
                    }
                }
            });
        }
        tokio::spawn(run_session(kcp, pkt_rx, shared));

        // ---- 服务端会话(先建好,但阶段1不喂包 = 永不 ACK)----
        let (io2, shared2) = new_io();
        let backlog2 = Arc::new(UdpBacklog::new());
        let output2 = UdpOutput {
            sock: server_sock.clone(),
            peer: client_addr,
            connected: false,
            backlog: backlog2.clone(),
        };
        output2.spawn_drain();
        drop(backlog2);
        let mut kcp2 = Kcp::new_stream(conv, output2);
        kcp_conf(&mut kcp2);
        kcp2.set_maximum_resend_times(200);
        let (pkt_tx2, pkt_rx2) = mpsc::channel::<SessionCmd>(256);
        let feed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        {
            let feed = feed.clone();
            let shared2 = shared2.clone();
            let pkt_tx2 = pkt_tx2;
            let recv_sock = server_sock.clone();
            tokio::spawn(async move {
                let mut buf = vec![0u8; 65536];
                loop {
                    tokio::select! {
                        r = recv_sock.recv_from(&mut buf) => {
                            let Ok((n, from)) = r else { break };
                            if !feed.load(Ordering::Relaxed) {
                                continue; // 阶段1:丢弃(对端视角 = 无 ACK)
                            }
                            if from == client_addr
                                && n >= KCP_OVERHEAD
                                && kcp::get_conv(&buf[..n]) == conv
                                && pkt_tx2
                                    .send(SessionCmd::Packet(PooledPkt::copy_of(&buf[..n])))
                                    .await
                                    .is_err()
                            {
                                break;
                            }
                        }
                        _ = shared2.shutdown.notified() => break,
                    }
                }
            });
        }
        tokio::spawn(run_session(kcp2, pkt_rx2, shared2));

        // ---- 阶段1:写必须停顿,且停顿前接受量有界 ----
        let mut pos = 0usize;
        let mut stalled = false;
        'phase1: while pos < TOTAL {
            let chunk = gen_from(pos, CHUNK.min(TOTAL - pos));
            let mut off = 0usize;
            while off < chunk.len() {
                match tokio::time::timeout(Duration::from_millis(150), io.write(&chunk[off..]))
                    .await
                {
                    Ok(Ok(n)) => {
                        off += n;
                        pos += n;
                    }
                    Ok(Err(e)) => panic!("phase1 write error: {e}"),
                    Err(_) => {
                        stalled = true;
                        break 'phase1;
                    }
                }
            }
        }
        assert!(stalled, "无 ACK 对端必须让写停顿(修复前会全量接受)");
        assert!(
            pos > 0 && pos < 2 * 1024 * 1024,
            "停顿前接受量必须有界且非零: {pos}"
        );
        let accepted_at_stall = pos;

        // ---- 阶段2:喂包(ACK 回流)→ 恢复 + 完整性 ----
        feed.store(true, Ordering::Relaxed);
        let reader = tokio::spawn(async move {
            let mut io2 = io2;
            let mut got = 0usize;
            let mut x: u64 = 0x2545F4914F6CDD1D;
            let mut buf = vec![0u8; 64 * 1024];
            while got < TOTAL {
                let want = buf.len().min(TOTAL - got);
                io2.read_exact(&mut buf[..want])
                    .await
                    .expect("server read_exact");
                for b in &buf[..want] {
                    x ^= x << 13;
                    x ^= x >> 7;
                    x ^= x << 17;
                    x = x.wrapping_add(*b as u64);
                }
                got += want;
            }
            x
        });
        let deadline = Instant::now() + Duration::from_secs(30);
        while pos < TOTAL {
            assert!(
                Instant::now() < deadline,
                "恢复后 30s 内未写完: pos={pos} stalled_at={accepted_at_stall}"
            );
            let chunk = gen_from(pos, CHUNK.min(TOTAL - pos));
            let mut off = 0usize;
            while off < chunk.len() {
                match tokio::time::timeout(Duration::from_secs(5), io.write(&chunk[off..])).await {
                    Ok(Ok(n)) => {
                        off += n;
                        pos += n;
                    }
                    Ok(Err(e)) => panic!("phase2 write error at {pos}: {e}"),
                    Err(_) => continue, // 仍受限:等窗口重开
                }
            }
        }
        let server_digest = tokio::time::timeout(Duration::from_secs(20), reader)
            .await
            .expect("server read join")
            .expect("server digest");
        assert_eq!(
            server_digest,
            digest_of_positions(TOTAL),
            "4MiB 逐字节完整性(经停顿-恢复)"
        );
    }

    /// 真实 KCP 公共 API 多流背压+恢复:服务端读 64KiB 后停 1s,
    /// 客户端 mux 单流写必须被端到端背压拦停;服务端恢复读后全部
    /// 送达且校验一致(读端有界 + 窗口重开唤醒的端到端验证)。
    #[tokio::test]
    async fn b08_public_mux_stalled_reader_backpressure_and_recovery() {
        // TOTAL 必须超过 256KiB 规约初窗 + 6MiB 预授信用(≈6.25MiB),否则停读窗口内写端不会停顿
        const TOTAL: usize = 8 * 1024 * 1024;
        const CHUNK: usize = 64 * 1024;

        let server_cfg = ServerTransportConfig {
            protocol: Protocol::Kcp,
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            ..Default::default()
        };
        let mut listener = listen(
            &server_cfg,
            std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        )
        .await
        .unwrap();
        let addr = listener.local_addr();

        // 服务端:先读 64KiB,停 1s,再读完 TOTAL 并回摘要
        let (digest_tx, digest_rx) = tokio::sync::oneshot::channel::<u64>();
        let srv = tokio::spawn(async move {
            let inc = listener.accept().await.unwrap();
            let mut s = inc.mux.accept().await.unwrap();
            let mut x: u64 = 0x2545F4914F6CDD1D;
            let mut got = 0usize;
            let mut buf = vec![0u8; 64 * 1024];
            while got < TOTAL {
                let want = buf.len().min(TOTAL - got);
                s.read_exact(&mut buf[..want]).await.expect("srv read");
                for b in &buf[..want] {
                    x ^= x << 13;
                    x ^= x >> 7;
                    x ^= x << 17;
                    x = x.wrapping_add(*b as u64);
                }
                got += want;
                if got == CHUNK {
                    tokio::time::sleep(Duration::from_secs(1)).await; // 停读
                }
            }
            let _ = digest_tx.send(x);
        });

        let cfg = ClientTransportConfig {
            protocol: Protocol::Kcp,
            server_addr: addr,
            ..Default::default()
        };
        let mux = connect(&cfg).await.unwrap();
        let mut s = mux.open_stream().await.unwrap();
        let mut pos = 0usize;
        let mut stalled_at: Option<usize> = None;
        let deadline = Instant::now() + Duration::from_secs(60);
        while pos < TOTAL {
            assert!(Instant::now() < deadline, "写未在 60s 内完成: pos={pos}");
            let chunk = gen_from(pos, CHUNK.min(TOTAL - pos));
            let mut off = 0usize;
            while off < chunk.len() {
                // 停读窗口内单笔 300ms 超时记录停顿(数据仍可部分推进)
                let per = if stalled_at.is_none() {
                    Duration::from_millis(300)
                } else {
                    Duration::from_secs(5)
                };
                match tokio::time::timeout(per, s.write(&chunk[off..])).await {
                    Ok(Ok(n)) => {
                        off += n;
                        pos += n;
                    }
                    Ok(Err(e)) => panic!("write error at {pos}: {e}"),
                    Err(_) => {
                        if stalled_at.is_none() {
                            stalled_at = Some(pos);
                        }
                    }
                }
            }
        }
        assert!(
            stalled_at.is_some_and(|p| p > 0 && p < TOTAL),
            "停读窗口必须造成写停顿(端到端背压): {stalled_at:?}"
        );
        let server_digest = tokio::time::timeout(Duration::from_secs(20), digest_rx)
            .await
            .expect("digest timeout")
            .expect("digest channel");
        assert_eq!(server_digest, digest_of_positions(TOTAL), "完整性校验");
        srv.abort();
        let _ = srv.await;
    }

    // ---------- 追加项:shutdown 交付语义与 listener 生命周期 ----------

    /// 真实对端完整收到后 shutdown 才成功:裸会话对(真实 UDP + 真实
    /// 双端 kcp),客户端写 256KiB → shutdown 必须最终返回 Ok(有真实
    /// ACK 回流,不允许永远 Pending 骗过安全性断言),随后 drop 句柄,
    /// 服务端必须完整读全部 256KiB 且校验一致。
    /// 修复前红:poll_shutdown 无条件 Ready → drop → handles==0 先于
    /// 搬运退出 → 服务端收不到数据,read_exact 超时。
    #[tokio::test]
    async fn b09_shutdown_completes_only_after_real_peer_receives_all() {
        const TOTAL: usize = 256 * 1024;
        let conv = 0x5EC5_0003u32;
        let server_sock = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let server_addr = server_sock.local_addr().unwrap();
        let client_sock = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let client_addr = client_sock.local_addr().unwrap();
        client_sock.connect(server_addr).await.unwrap();

        // 客户端会话
        let (mut io, shared) = new_io();
        let backlog = Arc::new(UdpBacklog::new());
        let output = UdpOutput {
            sock: client_sock.clone(),
            peer: server_addr,
            connected: true,
            backlog: backlog.clone(),
        };
        output.spawn_drain();
        drop(backlog);
        let mut kcp = Kcp::new_stream(conv, output);
        kcp_conf(&mut kcp);
        let (pkt_tx, pkt_rx) = mpsc::channel::<SessionCmd>(256);
        {
            let pkt_tx = pkt_tx;
            let recv_sock = client_sock.clone();
            let shared = shared.clone();
            tokio::spawn(async move {
                let mut buf = vec![0u8; 65536];
                loop {
                    tokio::select! {
                        r = recv_sock.recv_from(&mut buf) => {
                            let Ok((n, _)) = r else { break };
                            if pkt_tx.send(SessionCmd::Packet(PooledPkt::copy_of(&buf[..n]))).await.is_err() {
                                break;
                            }
                        }
                        _ = shared.shutdown.notified() => break,
                    }
                }
            });
        }
        tokio::spawn(run_session(kcp, pkt_rx, shared));

        // 服务端会话(同 conv,喂包)
        let (io2, shared2) = new_io();
        let backlog2 = Arc::new(UdpBacklog::new());
        let output2 = UdpOutput {
            sock: server_sock.clone(),
            peer: client_addr,
            connected: false,
            backlog: backlog2.clone(),
        };
        output2.spawn_drain();
        drop(backlog2);
        let mut kcp2 = Kcp::new_stream(conv, output2);
        kcp_conf(&mut kcp2);
        let (pkt_tx2, pkt_rx2) = mpsc::channel::<SessionCmd>(256);
        {
            let pkt_tx2 = pkt_tx2;
            let recv_sock = server_sock.clone();
            let shared2 = shared2.clone();
            tokio::spawn(async move {
                let mut buf = vec![0u8; 65536];
                loop {
                    tokio::select! {
                        r = recv_sock.recv_from(&mut buf) => {
                            let Ok((n, from)) = r else { break };
                            if from == client_addr
                                && n >= KCP_OVERHEAD
                                && kcp::get_conv(&buf[..n]) == conv
                                && pkt_tx2
                                    .send(SessionCmd::Packet(PooledPkt::copy_of(&buf[..n])))
                                    .await
                                    .is_err()
                            {
                                break;
                            }
                        }
                        _ = shared2.shutdown.notified() => break,
                    }
                }
            });
        }
        tokio::spawn(run_session(kcp2, pkt_rx2, shared2));

        // 客户端:全量写 → 显式 shutdown(交付完成才 Ok;无强制短超时,
        // 测试侧 10s 只为防挂)→ 成功后 drop 句柄。
        io.write_all(&gen_from(0, TOTAL)).await.expect("write_all");
        let shutdown = tokio::time::timeout(Duration::from_secs(10), io.shutdown()).await;
        match shutdown {
            Ok(Ok(())) => {}
            Ok(Err(e)) => panic!("shutdown 不应报错: {e}"),
            Err(_) => panic!("真实对端 ACK 回流下 shutdown 必须完成,不得永远 Pending"),
        }
        drop(io);

        // 服务端:必须完整收到全部字节(逐字节校验);整体限时防回归挂死
        let mut io2 = io2;
        let read_all = async {
            let mut got = 0usize;
            let mut x: u64 = 0x2545F4914F6CDD1D;
            let mut buf = vec![0u8; 64 * 1024];
            while got < TOTAL {
                let want = buf.len().min(TOTAL - got);
                io2.read_exact(&mut buf[..want])
                    .await
                    .expect("server read_exact");
                for b in &buf[..want] {
                    x ^= x << 13;
                    x ^= x >> 7;
                    x ^= x << 17;
                    x = x.wrapping_add(*b as u64);
                }
                got += want;
            }
            x
        };
        let x = tokio::time::timeout(Duration::from_secs(10), read_all)
            .await
            .expect("服务端读取未在 10s 内完成(shutdown 后数据未完整交付)");
        assert_eq!(
            x,
            digest_of_positions(TOTAL),
            "shutdown 成功后服务端必须已完整收到"
        );
    }

    /// shutdown 已请求后继续写必须报错(半关闭写端语义)。
    /// 纯 poll 状态机断言,无需会话任务。
    #[tokio::test(start_paused = true)]
    async fn b09b_write_after_shutdown_request_is_error() {
        let (mut io, _shared) = new_io();
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        use tokio::io::AsyncWriteExt;
        let _ = io.write_all(b"payload").await;
        assert!(
            Pin::new(&mut io).poll_shutdown(&mut cx).is_pending(),
            "未交付完成前 shutdown 必须 Pending"
        );
        let r = Pin::new(&mut io).poll_write(&mut cx, b"more");
        assert!(
            matches!(r, Poll::Ready(Err(_))),
            "shutdown 请求后继续写必须报错: {r:?}"
        );
    }

    /// listener drop 生命周期(第四轮语义):零会话 drop 立即释放;
    /// 已建但未 accept 的会话属未交付 —— 随 listener drop 取消并释放
    /// (不得被 detached yamux driver 拖住,对端持续入包也不得延长)。
    /// 前置用真实 ready 证据(listener.sessions 非空 = 真实首包已建会话)。
    /// 修复前红:demux 会话的 KcpIo 被 yamux driver 持有,未 accept 也
    /// 拖住 socket(主代理 kcp-unaccepted-red.log exit1)。
    #[tokio::test]
    async fn b10_listener_drop_cancels_unaccepted_and_releases_port() {
        // 1) 零会话:drop 后 100ms 内端口可重绑
        let cfg = ServerTransportConfig {
            protocol: Protocol::Kcp,
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            ..Default::default()
        };
        let listener = listen(
            &cfg,
            std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        )
        .await
        .unwrap();
        let addr = listener.local_addr();
        drop(listener);
        tokio::time::sleep(Duration::from_millis(100)).await;
        let rebound = UdpSocket::bind(addr).await;
        assert!(
            rebound.is_ok(),
            "零会话 drop 后端口必须可重绑: {:?}",
            rebound.err()
        );
        drop(rebound.unwrap());

        // 2) 已建未 accept(公共探针场景,同 kcp_unaccepted_probe):
        //    真实首包建会话 → ready 证据 → drop listener → 2s 内释放
        let cfg2 = ServerTransportConfig {
            protocol: Protocol::Kcp,
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            ..Default::default()
        };
        let listener2 = listen(
            &cfg2,
            std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(1024)),
        )
        .await
        .unwrap();
        let addr2 = listener2.local_addr();
        let client_cfg = ClientTransportConfig {
            protocol: Protocol::Kcp,
            server_addr: addr2,
            ..Default::default()
        };
        let mux = connect(&client_cfg).await.unwrap();
        let mut stream = mux.open_stream().await.unwrap();
        stream.write_all(b"unaccepted").await.unwrap();
        // 前置:真实首包已在服务端建立会话(ready),不是猜测
        let deadline = Instant::now() + Duration::from_secs(2);
        while listener2.sessions.lock().unwrap().is_empty() {
            assert!(
                Instant::now() < deadline,
                "前置失败:真实首包未建立服务端会话"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(
            !listener2.sessions.lock().unwrap().is_empty(),
            "前置:未 accept 会话确已存在(ready)"
        );
        drop(listener2);
        // 未交付会话不得拖住端口(修复前:被 driver 持有 ≥ idle 超时)
        let mut rebound2 = false;
        let start = Instant::now();
        while start.elapsed() < Duration::from_secs(2) {
            if UdpSocket::bind(addr2).await.is_ok() {
                rebound2 = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(
            rebound2,
            "drop listener 后未 accept 会话必须在 2s 内释放端口(未交付≠存量)"
        );
        drop(stream);
        drop(mux);
    }
}

/// 主代理 BOUNDARY-REVIEW-2 更强两例 + 两项源码边界余量核实的强回归。
/// S1/S2 为原样断言(S2 按生产接线补一行 attach_session,断言不变);
/// E1/E2 为站立总量上界(发送路径 256KiB+512×mss;读端精确 256KiB)。
#[cfg(test)]
mod stronger_tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    const MSS: usize = MTU - kcp::KCP_OVERHEAD;

    // ---------- S1:完整发送预算必须包含在途包(主代理原样) ----------

    #[test]
    fn complete_backlog_budget_counts_inflight_packet() {
        let bl = UdpBacklog::new();
        let packet = vec![1; 1024];
        for _ in 0..MAX_BACKLOG_BYTES / packet.len() {
            assert!(bl.push(&packet));
        }
        let mut observed = 0;
        drain_once(&bl, |_| {
            let _ = bl.push(&packet);
            let q = bl.state.lock().unwrap();
            observed =
                q.iter().map(Vec::len).sum::<usize>() + bl.inflight_bytes.load(Ordering::Relaxed);
            Err(std::io::Error::from(std::io::ErrorKind::WouldBlock))
        });
        assert!(
            observed <= MAX_BACKLOG_BYTES,
            "queue+inflight reached {observed}, exceeding byte budget {MAX_BACKLOG_BYTES}"
        );
    }

    // ---------- S2:idle 会话必须及时收到 backlog 真实错误 ----------
    // (主代理原样断言;按生产接线补 attach_session 一行 —— 生产三处
    // 均如此接线,断言与超时不变,不发 dummy 包、不等下一次写。)

    #[tokio::test]
    async fn fatal_backlog_error_reaches_idle_io_without_next_write() {
        let peer = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let sock = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let backlog = Arc::new(UdpBacklog::new());
        let output = UdpOutput {
            sock,
            peer: peer.local_addr().unwrap(),
            connected: false,
            backlog: backlog.clone(),
        };
        let kcp = Kcp::new_stream(765, output);
        let (mut io, shared) = new_io();
        backlog.attach_session(&shared); // 生产接线(connect/listen/punch 三处同款)
        let (_tx, rx) = mpsc::channel(4);
        let session = tokio::spawn(run_session(kcp, rx, shared));
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
        assert!(backlog.push(b"an-ack-datagram"));
        assert!(matches!(
            drain_once(&backlog, |_| Err(std::io::Error::other(
                "injected-udp-fatal"
            ))),
            DrainSt::Fatal
        ));
        let mut one = [0u8; 1];
        let result = tokio::time::timeout(Duration::from_millis(100), io.read(&mut one)).await;
        session.abort();
        let _ = session.await;
        assert!(
            matches!(&result, Ok(Err(e)) if e.to_string().contains("injected-udp-fatal")),
            "fatal output error requires another output packet before IO learns of it: {result:?}"
        );
    }

    /// S2 补充:错误桥触发后会话任务与全部 owned 任务必须收敛退出
    /// (会话退出 → kcp/UdpOutput 释放 → drain 退出),socket 不被悬挂持有。
    #[tokio::test]
    async fn fatal_bridge_stops_session_and_owned_tasks() {
        let peer = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let sock = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let weak_sock = Arc::downgrade(&sock);
        let backlog = Arc::new(UdpBacklog::new());
        let output = UdpOutput {
            sock: sock.clone(),
            peer: peer.local_addr().unwrap(),
            connected: false,
            backlog: backlog.clone(),
        };
        let kcp = Kcp::new_stream(766, output);
        let (io, shared) = new_io();
        backlog.attach_session(&shared);
        let weak_backlog = Arc::downgrade(&backlog);
        let (_tx, rx) = mpsc::channel(4);
        let session = tokio::spawn(run_session(kcp, rx, shared));
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
        drop(io);
        drop(backlog);
        assert!(backlog_push_poison(&weak_backlog));
        drop(sock); // 测试自身引用也释放
        tokio::time::timeout(Duration::from_secs(2), session)
            .await
            .expect("错误桥后空闲会话必须自行退出")
            .expect("join");
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(
            weak_sock.upgrade().is_none(),
            "会话/kcp/输出/drain 全部退出后 socket 必须释放"
        );
    }

    fn backlog_push_poison(weak: &std::sync::Weak<UdpBacklog>) -> bool {
        let bl = weak.upgrade().expect("backlog alive");
        bl.push(b"x");
        matches!(
            drain_once(&bl, |_| Err(std::io::Error::other("fatal-stop"))),
            DrainSt::Fatal
        )
    }

    // ---------- E1:发送路径站立总量上界(精确) ----------

    /// 跨 front+KCP pending 的总量恒 ≤ MAX_WRITE_QUEUE_BYTES +
    /// MAX_KCP_PENDING_SEGS×MSS(=864,256B)。先以 500 个满段铺底,再
    /// 用 64KiB 大块越过 512 边界:修复前「先判后整块」一次可越界约
    /// 56 段(实测接受 915,680B);修复后按剩余段余量×mss 截断喂入,
    /// 总量恰为上界。
    struct DiscardOutput;
    impl Write for DiscardOutput {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn e1_send_path_total_bound_is_exact() {
        let (mut io, shared) = new_io();
        let (_tx, rx) = mpsc::channel::<SessionCmd>(4);
        let mut kcp = Kcp::new_stream(771, DiscardOutput);
        kcp.set_mtu(MTU).ok();
        kcp.set_wndsize(256, 256);
        kcp.set_nodelay(true, 2, 2, true);
        let task = tokio::spawn(run_session(kcp, rx, shared));
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
        let seg = vec![2u8; MSS];
        // 500 个满段(=588,000B)铺底,等待会话全部喂入
        for _ in 0..500 {
            io.write_all(&seg).await.expect("write seg");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        // 64KiB 大块直到停顿(50ms/块)
        let big = vec![3u8; 64 * 1024];
        let mut accepted = 500 * MSS;
        for _ in 0..64 {
            match tokio::time::timeout(Duration::from_millis(50), io.write_all(&big)).await {
                Ok(Ok(())) => accepted += big.len(),
                Ok(Err(e)) => panic!("write err: {e}"),
                Err(_) => break,
            }
        }
        task.abort();
        let _ = task.await;
        let bound = MAX_WRITE_QUEUE_BYTES + MAX_KCP_PENDING_SEGS * MSS;
        assert!(
            accepted <= bound,
            "发送路径总量 {accepted} 超过站立上界 {bound}(front 256KiB + 512×mss)"
        );
    }

    // ---------- E2:读端站立上界精确为 MAX_READ_BUF ----------

    /// 真实 UDP 对:客户端灌数据,服务端应用不读 → 服务端 read_buf
    /// 不得超过 MAX_READ_BUF(修复前「先判后整段」可越界最多一段)。
    #[tokio::test]
    async fn e2_read_buf_bound_is_exact_under_flood() {
        let conv = 0x5EC5_0004u32;
        let server_sock = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let server_addr = server_sock.local_addr().unwrap();
        let client_sock = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let client_addr = client_sock.local_addr().unwrap();
        client_sock.connect(server_addr).await.unwrap();

        let (mut io, shared) = new_io();
        let backlog = Arc::new(UdpBacklog::new());
        backlog.attach_session(&shared);
        let output = UdpOutput {
            sock: client_sock.clone(),
            peer: server_addr,
            connected: true,
            backlog: backlog.clone(),
        };
        output.spawn_drain();
        drop(backlog);
        let mut kcp = Kcp::new_stream(conv, output);
        kcp.set_mtu(MTU).ok();
        kcp.set_wndsize(256, 256);
        kcp.set_nodelay(true, 2, 2, true);
        kcp.set_maximum_resend_times(200);
        let (pkt_tx, pkt_rx) = mpsc::channel::<SessionCmd>(256);
        {
            let pkt_tx = pkt_tx;
            let recv_sock = client_sock.clone();
            let shared = shared.clone();
            tokio::spawn(async move {
                let mut buf = vec![0u8; 65536];
                loop {
                    tokio::select! {
                        r = recv_sock.recv_from(&mut buf) => {
                            let Ok((n, _)) = r else { break };
                            if pkt_tx.send(SessionCmd::Packet(PooledPkt::copy_of(&buf[..n]))).await.is_err() {
                                break;
                            }
                        }
                        _ = shared.shutdown.notified() => break,
                    }
                }
            });
        }
        tokio::spawn(run_session(kcp, pkt_rx, shared));

        let (io2, shared2) = new_io();
        let backlog2 = Arc::new(UdpBacklog::new());
        backlog2.attach_session(&shared2);
        let output2 = UdpOutput {
            sock: server_sock.clone(),
            peer: client_addr,
            connected: false,
            backlog: backlog2.clone(),
        };
        output2.spawn_drain();
        drop(backlog2);
        let mut kcp2 = Kcp::new_stream(conv, output2);
        kcp2.set_mtu(MTU).ok();
        kcp2.set_wndsize(256, 256);
        kcp2.set_nodelay(true, 2, 2, true);
        kcp2.set_maximum_resend_times(200);
        let (pkt_tx2, pkt_rx2) = mpsc::channel::<SessionCmd>(256);
        {
            let pkt_tx2 = pkt_tx2;
            let recv_sock = server_sock.clone();
            let shared2 = shared2.clone();
            tokio::spawn(async move {
                let mut buf = vec![0u8; 65536];
                loop {
                    tokio::select! {
                        r = recv_sock.recv_from(&mut buf) => {
                            let Ok((n, from)) = r else { break };
                            if from == client_addr
                                && n >= KCP_OVERHEAD
                                && kcp::get_conv(&buf[..n]) == conv
                                && pkt_tx2
                                    .send(SessionCmd::Packet(PooledPkt::copy_of(&buf[..n])))
                                    .await
                                    .is_err()
                            {
                                break;
                            }
                        }
                        _ = shared2.shutdown.notified() => break,
                    }
                }
            });
        }
        tokio::spawn(run_session(kcp2, pkt_rx2, shared2.clone()));

        // 客户端灌到自身背压停(足够填满服务端 read_buf + rcv 窗)
        let big = vec![4u8; 64 * 1024];
        let mut pushed = 0usize;
        for _ in 0..64 {
            match tokio::time::timeout(Duration::from_millis(50), io.write_all(&big)).await {
                Ok(Ok(())) => pushed += big.len(),
                Ok(Err(e)) => panic!("write err: {e}"),
                Err(_) => break,
            }
        }
        assert!(pushed > MAX_READ_BUF, "前置:必须灌满读端(推送 {pushed}B)");
        // 应用从不读:等服务端会话把能收的都收进 read_buf
        tokio::time::sleep(Duration::from_millis(300)).await;
        let len = shared2.guarded.lock().unwrap().read_buf.len();
        assert!(
            len <= MAX_READ_BUF,
            "读端缓冲 {len} 超过精确上界 {MAX_READ_BUF}"
        );
        drop(io);
        drop(io2);
    }
}

/// 第四轮:未交付会话生命周期(BOUNDARY 判定)。全部走真实路径
/// (真实首包经 demux 建会话 / 真实 spawn_server_session / 真实
/// run_session+清理回调),不以人工 map.remove 冒充公共路径释放。
#[cfg(test)]
mod unaccepted_tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    /// ready 队列满拒绝:被拒会话必须整体取消(io drop → handles==0 →
    /// run_session/接收/drain 退出 → 真实清理回调移除 map),不能只删
    /// map 留下会话。
    #[tokio::test]
    async fn u01_ready_queue_full_rejection_cancels_session() {
        let udp = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let peer = "127.0.0.1:9".parse().unwrap();
        let sessions: SessionMap = Arc::new(Mutex::new(HashMap::new()));
        let ctl = Arc::new(DemuxCtl {
            closing: std::sync::atomic::AtomicBool::new(false),
            wake: Notify::new(),
            retired: Mutex::new(RetiredTable::default()),
            gate: SessionGate::new(SessionGateConfig::default()),
        });
        // 容量 1 且预填满:try_send 必然 Full(无等待中的接收者)
        let (accept_tx, mut accept_rx) = mpsc::channel::<ReadyItem>(1);
        let (fill_io, _fill_shared) = new_io();
        accept_tx.try_send((fill_io, peer)).expect("prefill ready");
        let conv = 0x0F01u32;
        let (tx, rx) = mpsc::channel::<SessionCmd>(16);
        sessions.lock().unwrap().insert(conv, (tx.clone(), peer));
        spawn_server_session(
            conv,
            tx,
            rx,
            sessions.clone(),
            udp,
            peer,
            &accept_tx,
            0,
            ctl,
        );
        // 被拒会话的 ready 不得入队:取出预填项后队列应为空
        assert!(accept_rx.try_recv().is_ok(), "预填项应可取出");
        assert!(
            accept_rx.try_recv().is_err(),
            "满队列拒绝后不得交付被拒会话的 ready 条目"
        );
        // 真实清理:会话因 io drop(handles==0)退出并移除自身
        let deadline = Instant::now() + Duration::from_secs(1);
        while !sessions.lock().unwrap().is_empty() {
            assert!(
                Instant::now() < deadline,
                "ready 满拒绝后会话必须取消并从 map 移除"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(sessions.lock().unwrap().is_empty(), "被拒会话必须整体取消");
    }

    /// 已 accept 会话在 listener drop 后继续正常交换(已交付存量不截断),
    /// 且存活期间端口仍被共享占用。
    #[tokio::test]
    async fn u02_accepted_session_survives_listener_drop() {
        let cfg = ServerTransportConfig {
            protocol: Protocol::Kcp,
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            ..Default::default()
        };
        let mut listener = listen(
            &cfg,
            std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        )
        .await
        .unwrap();
        let addr = listener.local_addr();
        let client_cfg = ClientTransportConfig {
            protocol: Protocol::Kcp,
            server_addr: addr,
            ..Default::default()
        };
        let mux = connect(&client_cfg).await.unwrap();
        let mut cs = mux.open_stream().await.unwrap();
        cs.write_all(b"u02").await.unwrap();
        let inc = listener.accept().await.expect("accept");
        let mut ss = inc.mux.accept().await.expect("stream");
        let mut b = [0u8; 3];
        ss.read_exact(&mut b).await.expect("read");
        ss.write_all(&b).await.expect("echo");
        // drop listener:已交付会话继续
        drop(listener);
        let mut back = [0u8; 3];
        cs.read_exact(&mut back).await.expect("echo back");
        assert_eq!(&back, b"u02");
        // 继续交换
        cs.write_all(b"more").await.unwrap();
        let mut b2 = [0u8; 4];
        ss.read_exact(&mut b2).await.expect("read2");
        ss.write_all(&b2).await.unwrap();
        let mut back2 = [0u8; 4];
        cs.read_exact(&mut back2).await.expect("echo2");
        assert_eq!(&back2, b"more");
        // 已交付会话存活期间端口仍被共享
        assert!(
            UdpSocket::bind(addr).await.is_err(),
            "已交付会话存活期间端口不得被抢占"
        );
        drop(ss);
        drop(cs);
        drop(mux);
    }

    /// 最后一个已交付会话结束后,socket 真正释放:真实首包经 demux 建
    /// 会话(固定 conv 客户端真实发包)→ ready 交付(io 即交付句柄)→
    /// 关闭 listener(closing)→ 交付中继续交换 → 消费方释放 io →
    /// 真实清理回调移除 map → demux 退出 → 端口可重绑。全程无人工
    /// map.remove。
    #[tokio::test]
    async fn u03_socket_released_after_last_delivered_session_ends() {
        let conv = 0x5EC5_0006u32;
        let server_sock = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let server_addr = server_sock.local_addr().unwrap();
        let client_sock = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        client_sock.connect(server_addr).await.unwrap();

        // 真实 demux 栈
        let sessions: SessionMap = Arc::new(Mutex::new(HashMap::new()));
        let ctl = Arc::new(DemuxCtl {
            closing: std::sync::atomic::AtomicBool::new(false),
            wake: Notify::new(),
            retired: Mutex::new(RetiredTable::default()),
            gate: SessionGate::new(SessionGateConfig::default()),
        });
        let (accept_tx, mut accept_rx) = mpsc::channel::<ReadyItem>(8);
        let demux = tokio::spawn(demux_loop(
            server_sock.clone(),
            accept_tx,
            std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            ctl.clone(),
            sessions.clone(),
        ));

        // 真实客户端会话(固定 conv),首包触发服务端建会话
        let (mut io_c, shared_c) = new_io();
        let backlog = Arc::new(UdpBacklog::new());
        backlog.attach_session(&shared_c);
        let output = UdpOutput {
            sock: client_sock.clone(),
            peer: server_addr,
            connected: true,
            backlog: backlog.clone(),
        };
        output.spawn_drain();
        drop(backlog);
        let mut kcp = Kcp::new_stream(conv, output);
        kcp.set_mtu(MTU).ok();
        kcp.set_wndsize(256, 256);
        kcp.set_nodelay(true, 2, 2, true);
        let (pkt_tx, pkt_rx) = mpsc::channel::<SessionCmd>(256);
        {
            let pkt_tx = pkt_tx;
            let recv_sock = client_sock.clone();
            let shared = shared_c.clone();
            tokio::spawn(async move {
                let mut buf = vec![0u8; 65536];
                loop {
                    tokio::select! {
                        r = recv_sock.recv_from(&mut buf) => {
                            let Ok((n, _)) = r else { break };
                            if pkt_tx.send(SessionCmd::Packet(PooledPkt::copy_of(&buf[..n]))).await.is_err() {
                                break;
                            }
                        }
                        _ = shared.shutdown.notified() => break,
                    }
                }
            });
        }
        tokio::spawn(run_session(kcp, pkt_rx, shared_c));
        io_c.write_all(b"first-packet").await.expect("client write");

        // 真实 ready:demux 收到真实首包建会话并入队
        let deadline = Instant::now() + Duration::from_secs(2);
        while sessions.lock().unwrap().is_empty() {
            assert!(Instant::now() < deadline, "前置:真实首包未建会话");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        // 交付(消费方持有 io)
        let (mut io_s, _peer) = tokio::time::timeout(Duration::from_secs(2), accept_rx.recv())
            .await
            .expect("ready 超时")
            .expect("ready channel");
        let mut hello = [0u8; 12];
        io_s.read_exact(&mut hello).await.expect("read first");
        assert_eq!(&hello, b"first-packet");

        // 关闭 listener(closing):交付中会话继续交换
        ctl.closing.store(true, Ordering::Release);
        ctl.wake.notify_one();
        io_c.write_all(b"delivered-continue").await.unwrap();
        let mut cont = [0u8; 18];
        io_s.read_exact(&mut cont).await.expect("read after close");
        assert_eq!(&cont, b"delivered-continue");
        assert!(
            UdpSocket::bind(server_addr).await.is_err(),
            "交付会话存活期间端口不得被抢占"
        );

        // 消费方结束:释放 io → 真实清理 → demux 退出 → 端口释放
        drop(io_s);
        drop(io_c);
        drop(server_sock); // 测试自身引用也释放(demux 持有克隆)
        let mut released = false;
        let start = Instant::now();
        while start.elapsed() < Duration::from_secs(2) {
            if sessions.lock().unwrap().is_empty() && UdpSocket::bind(server_addr).await.is_ok() {
                released = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(
            released,
            "最后交付会话结束后 socket 必须在 2s 内释放(真实清理路径)"
        );
        let _ = tokio::time::timeout(Duration::from_secs(2), demux)
            .await
            .expect("demux 必须退出");
    }
}

/// 主代理 DEMUX-FULL-DEADLOCK:真实 demux ready 满队列/关闭路径不得
/// 自锁 SessionMap(旧实现 demux_loop 持锁调 spawn_server_session,
/// 失败分支重锁同一非重入 Mutex,demux 与全部会话清理一起卡死)。
/// 真实 UDP 入站、真实 demux_loop、容量 1 满队列/已关闭队列;
/// watchdog 用独立 OS 线程 + process exit,runtime 被 mutex 堵死时
/// async timeout 不可依赖(主代理 r3 取证方法)。
#[cfg(test)]
mod independent_full_tests {
    use super::*;

    fn spawn_watchdog(seconds: u64) -> Arc<std::sync::atomic::AtomicBool> {
        let finished = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let w = finished.clone();
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_secs(seconds));
            if !w.load(Ordering::Acquire) {
                eprintln!(
                    "WATCHDOG: actual demux ready-full rejection blocked its own SessionMap mutex and starved the runtime"
                );
                std::process::exit(71);
            }
        });
        finished
    }

    fn kcp_ack_packet(conv: u32) -> [u8; 24] {
        let mut packet = [0u8; 24];
        packet[..4].copy_from_slice(&conv.to_le_bytes());
        packet[4] = 82; // ACK
        packet[6..8].copy_from_slice(&256u16.to_le_bytes());
        packet
    }

    /// 真实 UDP → demux → 容量 1 满 ready:第二个 conv 触发实际失败
    /// 分支;随后 closing + drop 接收端,socket 必须及时释放。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn demux_ready_full_must_not_lock_itself() {
        let finished = spawn_watchdog(2);
        let udp = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let addr = udp.local_addr().unwrap();
        let sender = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let (tx, rx) = mpsc::channel::<ReadyItem>(1);
        let ctl = Arc::new(DemuxCtl {
            closing: std::sync::atomic::AtomicBool::new(false),
            wake: Notify::new(),
            retired: Mutex::new(RetiredTable::default()),
            gate: SessionGate::new(SessionGateConfig::default()),
        });
        let sessions: SessionMap = Arc::new(Mutex::new(HashMap::new()));
        tokio::spawn(demux_loop(
            udp,
            tx,
            Arc::new(std::sync::atomic::AtomicUsize::new(1024)),
            ctl.clone(),
            sessions,
        ));
        for conv in [1001u32, 1002] {
            sender.send_to(&kcp_ack_packet(conv), addr).await.unwrap();
            tokio::time::sleep(Duration::from_millis(50)).await;
            if conv == 1001 {
                assert_eq!(
                    rx.len(),
                    1,
                    "first session must fill the actual ready queue"
                );
            }
        }
        // 第二个 conv 走真实满队列失败分支;若自锁,下方 closing 清理
        // 与 watchdog 均无法推进。
        ctl.closing.store(true, Ordering::Release);
        ctl.wake.notify_one();
        drop(rx);
        let released = tokio::time::timeout(Duration::from_millis(400), async {
            loop {
                if UdpSocket::bind(addr).await.is_ok() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .is_ok();
        println!("demux ready-full then drop releases socket={released}");
        finished.store(true, Ordering::Release);
        assert!(
            released,
            "demux re-locks SessionMap while caller still holds it on full-ready rejection"
        );
    }

    /// 已关闭队列(closing 前 rx 已 drop):真实 Closed 失败分支同样
    /// 不得自锁,被拒会话取消、closing 后 socket 释放。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn demux_closed_ready_must_not_lock_itself() {
        let finished = spawn_watchdog(2);
        let udp = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let addr = udp.local_addr().unwrap();
        let sender = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let (tx, mut rx) = mpsc::channel::<ReadyItem>(1);
        let ctl = Arc::new(DemuxCtl {
            closing: std::sync::atomic::AtomicBool::new(false),
            wake: Notify::new(),
            retired: Mutex::new(RetiredTable::default()),
            gate: SessionGate::new(SessionGateConfig::default()),
        });
        let sessions: SessionMap = Arc::new(Mutex::new(HashMap::new()));
        tokio::spawn(demux_loop(
            udp,
            tx,
            Arc::new(std::sync::atomic::AtomicUsize::new(1024)),
            ctl.clone(),
            sessions,
        ));
        // 第一个会话进 ready;消费后关闭队列
        sender.send_to(&kcp_ack_packet(2001), addr).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(rx.try_recv().is_ok(), "first session enters ready");
        drop(rx);
        // 第二个 conv:注册 + spawn 后 try_send 命中 Closed 失败分支
        sender.send_to(&kcp_ack_packet(2002), addr).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        ctl.closing.store(true, Ordering::Release);
        ctl.wake.notify_one();
        let released = tokio::time::timeout(Duration::from_millis(400), async {
            loop {
                if UdpSocket::bind(addr).await.is_ok() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .is_ok();
        println!("demux closed-ready then closing releases socket={released}");
        finished.store(true, Ordering::Release);
        assert!(
            released,
            "demux Closed 失败分支自锁或被拒会话未取消导致 socket 不释放"
        );
    }
}

/// 第六轮(punch 生命周期与 conv 退休,审计 F1/F2/F3/F4/F7):
/// pa1 为确定性红绿(同 conv 僵尸复活);pa2/pa4 为守护(微秒级竞态
/// 窗口无法确定性注入,如实标注);pa3/pa5 为新原语的单元/内部探针。
#[cfg(test)]
mod punch_audit_tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn ctl_new() -> Arc<DemuxCtl> {
        Arc::new(DemuxCtl {
            closing: std::sync::atomic::AtomicBool::new(false),
            wake: Notify::new(),
            retired: Mutex::new(RetiredTable::default()),
            gate: SessionGate::new(SessionGateConfig::default()),
        })
    }

    fn ack_packet(conv: u32) -> [u8; 24] {
        let mut p = [0u8; 24];
        p[..4].copy_from_slice(&conv.to_le_bytes());
        p[4] = 82;
        p[6..8].copy_from_slice(&256u16.to_le_bytes());
        p
    }

    /// F4a 确定性红绿:会话退出后,同 conv 的后续报文不得重建会话
    /// (修复前:Route::New 重建 → ready 再入队,产出 sn 断层僵尸会话
    /// 污染 accept 队列)。
    #[tokio::test]
    async fn pa1_exited_conv_does_not_revive_into_ready_queue() {
        let udp = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let addr = udp.local_addr().unwrap();
        let sender = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let (tx, mut rx) = mpsc::channel::<ReadyItem>(8);
        let ctl = ctl_new();
        let sessions: SessionMap = Arc::new(Mutex::new(HashMap::new()));
        tokio::spawn(demux_loop(
            udp.clone(),
            tx,
            Arc::new(std::sync::atomic::AtomicUsize::new(1024)),
            ctl.clone(),
            sessions.clone(),
        ));
        let conv = 0x0A01u32;
        // 首包建会话 → ready
        sender.send_to(&ack_packet(conv), addr).await.unwrap();
        let (io1, _peer1) = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("ready timeout")
            .expect("first ready");
        // 等注册落地
        let deadline = Instant::now() + Duration::from_secs(2);
        while sessions.lock().unwrap().is_empty() {
            assert!(Instant::now() < deadline, "前置:会话已注册");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        // 消费方释放(会话退出 → 清理 → 退休)
        drop(io1);
        let deadline = Instant::now() + Duration::from_secs(2);
        while !sessions.lock().unwrap().is_empty() {
            assert!(Instant::now() < deadline, "会话退出后必须清空 map");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(ctl.is_retired(conv), "会话退出后 conv 必须进入退休表");
        // 同 conv 复活报文:不得重建(修复前会再入 ready)
        sender.send_to(&ack_packet(conv), addr).await.unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(
            rx.try_recv().is_err(),
            "已退出 conv 的报文不得重建会话进 ready 队列(僵尸复活)"
        );
        assert!(
            sessions.lock().unwrap().is_empty(),
            "已退出 conv 不得重新注册"
        );
    }

    /// F4b:ready 满拒绝进入短退休(内部探针),窗口内同 conv 不再
    /// 重复注册/spawn(限流任务创建风暴)。
    #[tokio::test]
    async fn pa5_ready_full_rejection_retires_conv_shortly() {
        let udp = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let peer = "127.0.0.1:9".parse().unwrap();
        let sessions: SessionMap = Arc::new(Mutex::new(HashMap::new()));
        let ctl = ctl_new();
        let (accept_tx, mut accept_rx) = mpsc::channel::<ReadyItem>(1);
        let (fill_io, _fs) = new_io();
        accept_tx.try_send((fill_io, peer)).expect("prefill");
        let conv = 0x0A05u32;
        let (tx, rx) = mpsc::channel::<SessionCmd>(16);
        sessions.lock().unwrap().insert(conv, (tx.clone(), peer));
        spawn_server_session(
            conv,
            tx,
            rx,
            sessions.clone(),
            udp,
            peer,
            &accept_tx,
            0,
            ctl.clone(),
        );
        assert!(accept_rx.try_recv().is_ok(), "预填项");
        assert!(accept_rx.try_recv().is_err(), "被拒会话不得入队");
        assert!(ctl.is_retired(conv), "满拒绝后 conv 必须短退休");
        let deadline = Instant::now() + Duration::from_secs(2);
        while !sessions.lock().unwrap().is_empty() {
            assert!(Instant::now() < deadline, "被拒会话必须取消并清出 map");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// F2 单元(新原语):learn 后 rotate 不得覆写目标地址。
    /// 修复前不存在该原语(peer 为裸 Mutex 覆写,check-then-act 竞态)。
    #[test]
    fn pa3_punch_peer_state_locks_after_learn() {
        let c1: SocketAddr = "127.0.0.1:1001".parse().unwrap();
        let c2: SocketAddr = "127.0.0.1:1002".parse().unwrap();
        let learned_addr: SocketAddr = "127.0.0.1:9999".parse().unwrap();
        let p = PunchPeer::new(c1);
        p.rotate(c2);
        assert_eq!(p.current(), c2, "Rotating 阶段候选轮换生效");
        assert!(p.learn(learned_addr), "首次 learn 迁移返回 true");
        p.rotate(c1);
        p.rotate(c2);
        assert_eq!(
            p.current(),
            learned_addr,
            "learn 后任何轮换不得覆写已锁定地址"
        );
        assert!(!p.learn(c1), "重复 learn 幂等返回 false");
        assert_eq!(p.current(), learned_addr);
    }

    /// F1 守护(非确定性红,微秒窗口):punch_dial 超时后,轮询/入站
    /// 辅助任务必须经 permit 语义 abort 收敛,socket 引用全部释放。
    #[tokio::test]
    async fn pa2_punch_timeout_tasks_converge() {
        let udp = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let dead: SocketAddr = "127.0.0.1:1".parse().unwrap();
        let r = punch_dial(udp.clone(), &[dead], 3).await;
        assert!(r.is_err(), "无响应候选必须超时失败");
        // 会话任务随 io drop 退出;轮询/入站任务经 abort permit 退出
        let deadline = Instant::now() + Duration::from_secs(4);
        while Arc::strong_count(&udp) > 1 {
            assert!(
                Instant::now() < deadline,
                "超时后辅助任务未收敛,socket 引用残留 {}",
                Arc::strong_count(&udp)
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert_eq!(Arc::strong_count(&udp), 1, "只剩测试自身引用");
    }

    /// F3 守护(非确定性红,微秒窗口无法注入):punch 承载路径洪泛 +
    /// 服务端停读 2s 后恢复,接收循环不得因通道满断流(修复前 Full 即
    /// break,socket 无人收包,会话静默死)。客户端并发读写(真实转发
    /// 形态;读侧盲写会使服务端 kcp 接收窗耗尽 → 重传风暴 → dead_link,
    /// 那是负载设计问题而非本缺陷)。
    #[tokio::test]
    async fn pa4_punch_flood_pause_resume_integrity() {
        const TOTAL: usize = 2 * 1024 * 1024;
        let listen_sock = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let cand = listen_sock.local_addr().unwrap();
        let dial_sock = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let ls = listen_sock.clone();
        let dial_addr = dial_sock.local_addr().unwrap();
        let listener = tokio::spawn(async move { punch_listen(ls, &[dial_addr]).await });
        let dialer = tokio::spawn(async move { punch_dial(dial_sock, &[cand], 8).await });
        let mux_l = tokio::time::timeout(Duration::from_secs(10), listener)
            .await
            .expect("listen join")
            .expect("listen task")
            .expect("punch listen");
        let mux_d = tokio::time::timeout(Duration::from_secs(10), dialer)
            .await
            .expect("dial join")
            .expect("dial task")
            .expect("punch dial");
        let cs = mux_d.open_stream().await.expect("open");
        // 服务端:停读 2s(积压)后读全部并回显
        let echo = {
            let mux_l = mux_l.clone();
            tokio::spawn(async move {
                let mut ss = mux_l.accept().await.expect("accept");
                tokio::time::sleep(Duration::from_secs(2)).await;
                let mut buf = vec![0u8; 64 * 1024];
                let mut got = 0usize;
                while got < TOTAL {
                    let want = buf.len().min(TOTAL - got);
                    ss.read_exact(&mut buf[..want]).await.expect("srv read");
                    ss.write_all(&buf[..want]).await.expect("echo");
                    got += want;
                }
            })
        };
        // 客户端:并发写全部 + 读回显(t13 形态)
        let (mut cr, mut cw) = tokio::io::split(cs);
        let verifier = tokio::spawn(async move {
            let mut x: u64 = 0x2545F4914F6CDD1D;
            let mut got = 0usize;
            let mut buf = vec![0u8; 64 * 1024];
            while got < TOTAL {
                let want = buf.len().min(TOTAL - got);
                cr.read_exact(&mut buf[..want]).await.expect("echo read");
                for b in &buf[..want] {
                    x ^= x << 13;
                    x ^= x >> 7;
                    x ^= x << 17;
                    x = x.wrapping_add(*b as u64);
                }
                got += want;
            }
            x
        });
        // 写序列(独立 PRNG)+ 同式累积摘要(与服务端/校验器折叠一致)
        let mut wx: u64 = 0x2545F4914F6CDD1D;
        let mut expect_digest: u64 = 0x2545F4914F6CDD1D;
        let mut chunk = vec![0u8; 64 * 1024];
        let mut pos = 0usize;
        let write_all = async {
            while pos < TOTAL {
                for b in chunk.iter_mut() {
                    wx ^= wx << 13;
                    wx ^= wx >> 7;
                    wx ^= wx << 17;
                    *b = (wx >> 24) as u8;
                    expect_digest ^= expect_digest << 13;
                    expect_digest ^= expect_digest >> 7;
                    expect_digest ^= expect_digest << 17;
                    expect_digest = expect_digest.wrapping_add(*b as u64);
                }
                cw.write_all(&chunk).await.expect("write");
                pos += chunk.len();
            }
        };
        tokio::time::timeout(Duration::from_secs(30), write_all)
            .await
            .expect("写未在 30s 内完成");
        let echo_digest = tokio::time::timeout(Duration::from_secs(30), verifier)
            .await
            .expect("洪泛+停读后回显必须完成(接收循环不得断流)")
            .expect("verifier join");
        let _ = tokio::time::timeout(Duration::from_secs(10), echo)
            .await
            .expect("echo join");
        assert_eq!(echo_digest, expect_digest, "2MiB 逐字节完整性(经停读-恢复)");
    }

    /// PUNCH-PARENT-CANCEL(主代理实测复现,确定性红绿):调用方
    /// drop/abort 打洞 future(parent cancel,不走超时分支)后,全部
    /// 辅助任务必须收敛、socket 引用释放 —— 修复前轮询任务永生,
    /// 持 socket 每 800ms 向全部候选发 RHPUNCH(socket_refs=2
    /// released=false,主代理 punch-parent-cancel-check-r2.log)。
    #[tokio::test]
    async fn pa6_parent_cancel_releases_punch_helpers() {
        let udp = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let dead: SocketAddr = "127.0.0.1:1".parse().unwrap();
        // 30s:窗口内不会走超时分支,parent cancel 是唯一离开路径
        let cands = vec![dead];
        let handle = tokio::spawn(punch_dial(udp.clone(), cands.leak(), 30));
        // 等轮询任务进入工作状态
        tokio::time::sleep(Duration::from_millis(300)).await;
        handle.abort();
        let _ = handle.await;
        // 全部辅助任务(轮询/入站/会话)必须收敛:socket 只剩测试引用
        let deadline = Instant::now() + Duration::from_secs(3);
        while Arc::strong_count(&udp) > 1 {
            assert!(
                Instant::now() < deadline,
                "parent cancel 后辅助任务未收敛: socket_refs={}",
                Arc::strong_count(&udp)
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert_eq!(Arc::strong_count(&udp), 1, "只剩测试自身引用");
    }

    /// 成功路径不受取消守卫影响:learned 后回显可用,且轮询任务按期
    /// 退出(审计第二探针语义:success echo + poll released)。
    #[tokio::test]
    async fn pa7_success_path_unaffected_by_cancel_guard() {
        let listen_sock = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let cand = listen_sock.local_addr().unwrap();
        let dial_sock = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let ls = listen_sock.clone();
        let dial_addr = dial_sock.local_addr().unwrap();
        let listener = tokio::spawn(async move { punch_listen(ls, &[dial_addr]).await });
        let sock_for_dial = dial_sock.clone();
        let dialer = tokio::spawn(async move { punch_dial(sock_for_dial, &[cand], 8).await });
        let mux_l = tokio::time::timeout(Duration::from_secs(10), listener)
            .await
            .expect("listen join")
            .expect("listen task")
            .expect("punch listen");
        let mux = tokio::time::timeout(Duration::from_secs(10), dialer)
            .await
            .expect("dial join")
            .expect("dial task")
            .expect("punch dial");
        let mut cs = mux.open_stream().await.expect("open");
        cs.write_all(b"svcpunch").await.unwrap();
        let echo = {
            let mux_l = mux_l.clone();
            tokio::spawn(async move {
                let mut ss = mux_l.accept().await.expect("accept");
                let mut b = [0u8; 8];
                ss.read_exact(&mut b).await.unwrap();
                ss.write_all(&b).await.unwrap();
            })
        };
        let mut back = [0u8; 8];
        tokio::time::timeout(Duration::from_secs(10), cs.read_exact(&mut back))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&back, b"svcpunch");
        echo.await.unwrap();
        // learned 后轮询任务按期退出(≤1 个轮询周期 + 余量)
        tokio::time::sleep(Duration::from_millis(1200)).await;
        // 会话/入站任务仍在服务(socket 引用保持:测试+会话链+入站)
        assert!(
            Arc::strong_count(&dial_sock) >= 2,
            "成功路径会话必须存活: refs={}",
            Arc::strong_count(&dial_sock)
        );
    }
}

/// performance-closure-c84 本轮回归(A 半关闭 / B 空闲进展 / C 打洞
/// 积压 / D 入站合批)。红证据:benchmark/runs/performance-closure-c84/
/// kcp/red/repro-baseline-red.log(c84f631 副本确定性复现:5 红 + 5 守护)。
/// b12/b_channel/b_idle/b_bound/b_fairness/d_reuse 为冻结守护(允许修前绿),
/// 其余在基线上失败、修复后转绿。
#[cfg(test)]
mod c84_closure_tests {
    use super::*;
    use futures::task::noop_waker;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    const MSS: usize = MTU - KCP_OVERHEAD;

    async fn yield16() {
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
    }

    struct DiscardOutput;
    impl Write for DiscardOutput {
        fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
            Ok(b.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    /// 双端经 mpsc 交叉连接的确定性线对(无真实 UDP/内核抖动)。
    struct WireOut(mpsc::Sender<SessionCmd>);
    impl Write for WireOut {
        fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
            self.0
                .try_send(SessionCmd::Packet(PooledPkt::copy_of(b)))
                .map_err(|e| std::io::Error::other(e.to_string()))?;
            Ok(b.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    struct RecVecOut(Arc<Mutex<Vec<Vec<u8>>>>);
    impl Write for RecVecOut {
        fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().push(b.to_vec());
            Ok(b.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    /// 对已发段 sn 的纯 ACK 报文(cmd=82,无 payload);ts 回显该段 ts
    /// (真实 ACK 语义,避免 update_ack 以 epoch 差算 rtt 溢出)。
    fn ack_for(conv: u32, sn: u32, ts: u32) -> Vec<u8> {
        let mut p = vec![0u8; 24];
        p[..4].copy_from_slice(&conv.to_le_bytes());
        p[4] = 82;
        p[6..8].copy_from_slice(&256u16.to_le_bytes());
        p[8..12].copy_from_slice(&ts.to_le_bytes());
        p[12..16].copy_from_slice(&sn.to_le_bytes());
        p
    }

    fn sess(shared: &Arc<Shared>) -> KcpCounters {
        shared.stats.snapshot()
    }

    // ================= A:半关闭只关写侧 =================

    /// 真实双端长度协议(c84_review_tests 原样移植;基线红见
    /// main-independent/halfclose-exact-baseline-red.log):写 7B →
    /// shutdown Ready → 对端收全 → 25ms 后回 8B → 本端仍必须读到。
    #[tokio::test]
    async fn a1_shutdown_write_must_preserve_delayed_reply_read() {
        let (mut client_io, client_shared) = new_io();
        let (mut server_io, server_shared) = new_io();
        let (client_tx, client_rx) = mpsc::channel(256);
        let (server_tx, server_rx) = mpsc::channel(256);
        let mut client = Kcp::new_stream(0xCA84, WireOut(server_tx));
        let mut server = Kcp::new_stream(0xCA84, WireOut(client_tx));
        for k in [&mut client, &mut server] {
            k.set_mtu(MTU).unwrap();
            k.set_wndsize(256, 256);
            k.set_nodelay(true, 2, 2, true);
        }
        let client_session = tokio::spawn(run_session(client, client_rx, client_shared));
        let server_session = tokio::spawn(run_session(server, server_rx, server_shared));
        let service = tokio::spawn(async move {
            let mut request = [0u8; 7];
            server_io
                .read_exact(&mut request)
                .await
                .expect("server request");
            assert_eq!(&request, b"REQUEST");
            tokio::time::sleep(Duration::from_millis(25)).await;
            server_io
                .write_all(b"RESPONSE")
                .await
                .expect("server reply");
            server_io.flush().await.expect("server reply flush");
            tokio::time::sleep(Duration::from_millis(200)).await;
        });
        let result = tokio::time::timeout(Duration::from_secs(2), async {
            client_io.write_all(b"REQUEST").await?;
            client_io.shutdown().await?;
            let mut response = [0u8; 8];
            client_io.read_exact(&mut response).await?;
            assert_eq!(&response, b"RESPONSE");
            std::io::Result::Ok(())
        })
        .await;
        service.abort();
        client_session.abort();
        server_session.abort();
        let _ = tokio::join!(service, client_session, server_session);
        assert!(
            matches!(result, Ok(Ok(()))),
            "successful write shutdown must still accept delayed reply: {result:?}"
        );
    }

    /// write_closed 后:poll_flush/poll_shutdown Ready(Ok)、poll_write Err;
    /// 会话错误(error)可见性优先于 write_closed(flush/read/shutdown 全 Err)。
    #[test]
    fn a2_flush_ready_and_error_visible_after_write_closed() {
        let (mut io, shared) = new_io();
        {
            let mut s = shared.guarded.lock().unwrap();
            s.closing = true;
            s.write_closed = true;
            s.write_seq = 3;
            s.flushed_seq = 3;
        }
        let w = noop_waker();
        let mut cx = Context::from_waker(&w);
        assert!(matches!(
            Pin::new(&mut io).poll_flush(&mut cx),
            Poll::Ready(Ok(()))
        ));
        assert!(matches!(
            Pin::new(&mut io).poll_shutdown(&mut cx),
            Poll::Ready(Ok(()))
        ));
        assert!(
            matches!(
                Pin::new(&mut io).poll_write(&mut cx, b"x"),
                Poll::Ready(Err(_))
            ),
            "写侧已关,继续写必须报错"
        );
        // 错误可见性优先:真实错误浮出而非被「已关写」掩盖
        shared.guarded.lock().unwrap().error = Some("boom-visible".into());
        let e = Pin::new(&mut io).poll_flush(&mut cx);
        assert!(
            matches!(&e, Poll::Ready(Err(er)) if er.to_string().contains("boom-visible")),
            "flush 必须先报会话错误: {e:?}"
        );
        assert!(matches!(
            Pin::new(&mut io).poll_shutdown(&mut cx),
            Poll::Ready(Err(_))
        ));
        let mut rbuf = [0u8; 4];
        let mut rb = ReadBuf::new(&mut rbuf);
        assert!(matches!(
            Pin::new(&mut io).poll_read(&mut cx, &mut rb),
            Poll::Ready(Err(_))
        ));
    }

    /// 真实 UDP 对:shutdown 成功后丢弃句柄 → 句柄归零 → session/inbound/
    /// drain 全部退出、socket 释放(b06 语义在「显式关闭完成」路径上)。
    #[tokio::test]
    async fn a3_drop_after_shutdown_releases_tasks_and_socket() {
        const TOTAL: usize = 64 * 1024;
        let conv = 0x5EC5_00A3u32;
        let server_sock = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let server_addr = server_sock.local_addr().unwrap();
        let client_sock = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let client_addr = client_sock.local_addr().unwrap();
        client_sock.connect(server_addr).await.unwrap();

        let (mut io, shared) = new_io();
        let backlog = Arc::new(UdpBacklog::new());
        backlog.attach_session(&shared);
        let output = UdpOutput {
            sock: client_sock.clone(),
            peer: server_addr,
            connected: true,
            backlog: backlog.clone(),
        };
        output.spawn_drain();
        drop(backlog);
        let mut kcp = Kcp::new_stream(conv, output);
        kcp.set_mtu(MTU).ok();
        kcp.set_wndsize(256, 256);
        kcp.set_nodelay(true, 2, 2, true);
        let (pkt_tx, pkt_rx) = mpsc::channel::<SessionCmd>(256);
        {
            let pkt_tx = pkt_tx;
            let recv_sock = client_sock.clone();
            let shared = shared.clone();
            tokio::spawn(async move {
                let mut buf = vec![0u8; 65536];
                loop {
                    tokio::select! {
                        r = recv_sock.recv_from(&mut buf) => {
                            let Ok((n, _)) = r else { break };
                            if pkt_tx
                                .send(SessionCmd::Packet(PooledPkt::copy_of(&buf[..n])))
                                .await
                                .is_err()
                            {
                                break;
                            }
                        }
                        _ = shared.shutdown.notified() => break,
                    }
                }
            });
        }
        let session = tokio::spawn(run_session(kcp, pkt_rx, shared));

        // 对端会话(喂包,ACK 回流)
        let (io2, shared2) = new_io();
        let backlog2 = Arc::new(UdpBacklog::new());
        backlog2.attach_session(&shared2);
        let output2 = UdpOutput {
            sock: server_sock.clone(),
            peer: client_addr,
            connected: false,
            backlog: backlog2.clone(),
        };
        output2.spawn_drain();
        drop(backlog2);
        let mut kcp2 = Kcp::new_stream(conv, output2);
        kcp2.set_mtu(MTU).ok();
        kcp2.set_wndsize(256, 256);
        kcp2.set_nodelay(true, 2, 2, true);
        let (pkt_tx2, pkt_rx2) = mpsc::channel::<SessionCmd>(256);
        {
            let pkt_tx2 = pkt_tx2;
            let recv_sock = server_sock.clone();
            let shared2 = shared2.clone();
            tokio::spawn(async move {
                let mut buf = vec![0u8; 65536];
                loop {
                    tokio::select! {
                        r = recv_sock.recv_from(&mut buf) => {
                            let Ok((n, from)) = r else { break };
                            if from == client_addr
                                && n >= KCP_OVERHEAD
                                && kcp::get_conv(&buf[..n]) == conv
                                && pkt_tx2
                                    .send(SessionCmd::Packet(PooledPkt::copy_of(&buf[..n])))
                                    .await
                                    .is_err()
                            {
                                break;
                            }
                        }
                        _ = shared2.shutdown.notified() => break,
                    }
                }
            });
        }
        let session2 = tokio::spawn(run_session(kcp2, pkt_rx2, shared2));

        let data: Vec<u8> = (0..TOTAL).map(|i| (i % 251) as u8).collect();
        io.write_all(&data).await.expect("write_all");
        tokio::time::timeout(Duration::from_secs(10), io.shutdown())
            .await
            .expect("shutdown 必须在 10s 内完成(真实 ACK 回流)")
            .expect("shutdown 不应报错");
        // 写侧已关、会话仍存活:丢弃句柄 → 句柄归零 → 全部任务收敛
        drop(io);
        tokio::time::timeout(Duration::from_secs(2), session)
            .await
            .expect("shutdown 完成后句柄归零,会话必须退出")
            .expect("join");
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(
            Arc::strong_count(&client_sock),
            1,
            "会话/接收循环/drain 退出后 socket 必须释放"
        );
        // 服务端仍能完整读全部字节(交付不受关写影响)
        let mut io2 = io2;
        let mut got = Vec::with_capacity(TOTAL);
        let read_all = async {
            let mut buf = vec![0u8; 64 * 1024];
            while got.len() < TOTAL {
                let want = buf.len().min(TOTAL - got.len());
                io2.read_exact(&mut buf[..want]).await.expect("srv read");
                got.extend_from_slice(&buf[..want]);
            }
        };
        tokio::time::timeout(Duration::from_secs(10), read_all)
            .await
            .expect("服务端读取未在 10s 内完成");
        assert_eq!(got, data, "shutdown 后交付完整性");
        drop(io2);
        session2.abort();
        let _ = session2.await;
    }

    // ================= B:无人消费不再无限续命 =================

    /// read_buf 预置数据 + 零消费 + 无入包 → 有界时间内回收,
    /// closed 可观察,存量排空后 EOF。
    #[tokio::test]
    async fn b_unconsumed_stall_reclaims() {
        let (mut io, shared) = new_io();
        shared.guarded.lock().unwrap().read_buf.push_back(7u8);
        let (keep_tx, rx) = mpsc::channel::<SessionCmd>(4);
        let kcp = Kcp::new_stream(0xDB01, DiscardOutput);
        let mut session = tokio::spawn(run_session_with_idle_timeout(
            kcp,
            rx,
            shared.clone(),
            Duration::from_millis(150),
        ));
        let exited = tokio::time::timeout(Duration::from_millis(600), &mut session)
            .await
            .is_ok();
        if !exited {
            session.abort();
            let _ = session.await;
        }
        assert!(exited, "read_buf 无人消费且无入包:会话必须在有界时间内回收");
        assert!(
            shared.guarded.lock().unwrap().closed,
            "回收后 closed 必须可观察"
        );
        // 存量仍可读,排空后 EOF(正常 EOF 语义保持)
        let mut one = [0u8; 1];
        tokio::time::timeout(Duration::from_millis(200), io.read_exact(&mut one))
            .await
            .expect("存量读超时")
            .expect("存量读");
        assert_eq!(one[0], 7);
        let e = tokio::time::timeout(Duration::from_millis(200), io.read(&mut one)).await;
        assert!(
            matches!(&e, Ok(Err(err)) if err.kind() == std::io::ErrorKind::UnexpectedEof)
                || matches!(&e, Ok(Ok(0))),
            "排空后应 EOF: {e:?}"
        );
        drop(io);
        drop(keep_tx);
    }

    /// 活跃慢消费者(每个 idle 窗口内有读)跨多个窗口存活;
    /// 停止消费后有界回收。
    #[tokio::test]
    async fn b_slow_consumer_survives_then_reclaims_after_stop() {
        let (mut io, shared) = new_io();
        {
            let mut s = shared.guarded.lock().unwrap();
            for i in 0..64u8 {
                s.read_buf.push_back(i);
            }
        }
        let (keep_tx, rx) = mpsc::channel::<SessionCmd>(4);
        let kcp = Kcp::new_stream(0xDB02, DiscardOutput);
        let mut session = tokio::spawn(run_session_with_idle_timeout(
            kcp,
            rx,
            shared.clone(),
            Duration::from_millis(150),
        ));
        // 慢消费:每 50ms 读 1 字节,跨 4× idle 窗口存活
        for _ in 0..12 {
            let mut one = [0u8; 1];
            tokio::time::timeout(Duration::from_millis(200), io.read_exact(&mut one))
                .await
                .expect("slow read timeout")
                .expect("slow read");
            tokio::time::sleep(Duration::from_millis(50)).await;
            assert!(!session.is_finished(), "活跃慢消费者(窗口内有读)不得被回收");
        }
        // 停止消费:有界回收
        let exited = tokio::time::timeout(Duration::from_millis(600), &mut session)
            .await
            .is_ok();
        if !exited {
            session.abort();
            let _ = session.await;
        }
        drop(io);
        drop(keep_tx);
        assert!(exited, "停止消费后必须在有界时间内回收(读端存量排空后 EOF)");
    }

    /// 无流量无待交付:维持原语义按 idle 超时回收(冻结守护)。
    #[tokio::test]
    async fn b_idle_no_traffic_recycles_unchanged() {
        let (io, shared) = new_io();
        let (keep_tx, rx) = mpsc::channel::<SessionCmd>(4);
        let kcp = Kcp::new_stream(0xDB03, DiscardOutput);
        let mut session = tokio::spawn(run_session_with_idle_timeout(
            kcp,
            rx,
            shared.clone(),
            Duration::from_millis(150),
        ));
        let t0 = Instant::now();
        let exited = tokio::time::timeout(Duration::from_millis(600), &mut session)
            .await
            .is_ok();
        if !exited {
            session.abort();
            let _ = session.await;
        }
        drop(io);
        drop(keep_tx);
        assert!(exited, "无流量无待交付:必须按 idle 超时回收");
        assert!(
            t0.elapsed() >= Duration::from_millis(140),
            "不得先于 idle 窗口退出"
        );
    }

    /// 无人消费 + 持续入站灌数据:读端缓冲贴精确上界且不越界
    /// (MAX_READ_BUF 界不动;确定性线对,守护)。
    #[tokio::test]
    async fn b_read_buf_bound_exact_under_stall() {
        const TOTAL: usize = 512 * 1024;
        let (mut cio, _cs) = new_io();
        let (_sio, sshared) = new_io();
        let (ctx, crx) = mpsc::channel::<SessionCmd>(4096);
        let (stx, srx) = mpsc::channel::<SessionCmd>(4096);
        let mut ckcp = Kcp::new_stream(0xDB04, WireOut(stx));
        let mut skcp = Kcp::new_stream(0xDB04, WireOut(ctx));
        for k in [&mut ckcp, &mut skcp] {
            k.set_mtu(MTU).unwrap();
            k.set_wndsize(256, 256);
            k.set_nodelay(true, 2, 2, true);
        }
        tokio::spawn(run_session_with_idle_timeout(
            ckcp,
            crx,
            _cs,
            Duration::from_secs(30),
        ));
        tokio::spawn(run_session_with_idle_timeout(
            skcp,
            srx,
            sshared.clone(),
            Duration::from_secs(30),
        ));
        // 服务端应用从不读(确定性线对无损,ACK 持续回流,写不必须停顿;
        // 本测守护的不变量是:无人消费下读端缓冲贴上界且不越界)
        let chunk = vec![5u8; 16 * 1024];
        let mut written = 0usize;
        while written < TOTAL {
            let n = tokio::time::timeout(Duration::from_secs(10), cio.write(&chunk))
                .await
                .expect("write timeout(窗口应回流)")
                .expect("write err");
            written += n;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
        let len = sshared.guarded.lock().unwrap().read_buf.len();
        assert!(
            len <= MAX_READ_BUF,
            "stall 下读端缓冲 {len} 超上界 {MAX_READ_BUF}"
        );
        assert!(
            len > MAX_READ_BUF - MTU,
            "stall 时读端必须贴近精确上界: {len}"
        );
    }

    // ================= C:打洞输出有界积压 =================

    /// WouldBlock 必须入队(计数可观察)并由 drain 补送恰好一次,
    /// 不再假成功丢报(基线红:repro-baseline-red.log c_punch_*)。
    /// macOS loopback 无法稳定造真实 kernel EAGAIN(实测 prime 后
    /// 200k×MTU 发送无一 WouldBlock,UDP 满缓冲静默丢弃;探针 0/3),
    /// WouldBlock 分支按合同注入断言 —— 不冒充内核复现(设计文档同款)。
    #[tokio::test]
    async fn c1_punch_wouldblock_enqueues_not_fake_ok() {
        let peer = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let peer_addr = peer.local_addr().unwrap();
        let sender = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        sender.writable().await.expect("writable prime");
        let probe = vec![0xC1u8; 64];
        let backlog = Arc::new(UdpBacklog::new());
        let out = PunchOutput {
            sock: sender.clone(),
            peer: Arc::new(PunchPeer::new(peer_addr)),
            backlog: backlog.clone(),
        };
        // 注入 WouldBlock(生产闭包为 sock.try_send_to;此分支语义与
        // socket 无关):不得假成功丢弃,必须入队且计数可观察。
        let n = output_write_via_backlog(
            &backlog,
            |_| Err(std::io::Error::from(std::io::ErrorKind::WouldBlock)),
            &probe,
        )
        .expect("WouldBlock 写合同:返回 Ok(入队)");
        assert_eq!(n, probe.len());
        let st = backlog.stats();
        assert_eq!(st.enqueued, 1, "WouldBlock 数据报必须入队(不再假成功)");
        assert_eq!(st.queued_packets, 1);
        assert!(st.fatal == 0, "容量排队不是错误");
        // 对端开始消费 → drain 经 writable 补送,恰好一次(真实 socket)
        out.spawn_drain();
        let mut buf = vec![0u8; 65536];
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut got_probe = 0usize;
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                break;
            }
            match tokio::time::timeout(remaining, peer.recv_from(&mut buf)).await {
                Ok(Ok((n, _))) => {
                    if n == probe.len() && buf[..n] == probe[..] {
                        got_probe += 1;
                    }
                }
                Ok(Err(_)) | Err(_) => break,
            }
        }
        assert_eq!(got_probe, 1, "补送必须恰好一次: {got_probe}");
        let st = backlog.stats();
        assert_eq!(st.sent, 1);
        assert_eq!(st.queued_packets, 0);
    }

    /// 阶段内确定性排空:Blocked(tokio 注册后就绪缓存空/瞬态内核
    /// 压力,实测负载下可出现)按生产 drain 同语义处理 —— 等 writable
    /// 后重试;WouldBlock 回插队头,顺序/恰好一次不变。Fatal 立即失败,
    /// 总时长有界(5s)。测试不依赖「内核永不 WouldBlock」假设。
    async fn drain_until_empty(sock: &UdpSocket, bl: &Arc<UdpBacklog>, dest: &Dest) {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            match drain_once(bl, |pkt| dest.try_send(sock, pkt)) {
                DrainSt::Empty => return,
                DrainSt::Blocked => {
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    assert!(
                        !remaining.is_zero(),
                        "drain Blocked 超时: stats={:?}",
                        bl.stats()
                    );
                    tokio::time::timeout(remaining, sock.writable())
                        .await
                        .expect("writable timeout")
                        .expect("writable err");
                }
                DrainSt::Fatal => panic!("drain Fatal: stats={:?}", bl.stats()),
            }
        }
    }

    /// Rotating 每报发往「弹出时刻」候选;learn 后锁定;每报恰好一次
    /// 落点正确(生产 drain 发送路径 Dest::Punch,确定性同步 drain_once)。
    #[tokio::test]
    async fn c2_punch_rotating_learned_delivery_exactly_once() {
        let a = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let a_addr = a.local_addr().unwrap();
        let b = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let b_addr = b.local_addr().unwrap();
        let sender = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        // prime:见 c1 注释(实测并行负载下未 prime 的首送可 Blocked)。
        sender.writable().await.expect("writable prime");
        let peer = Arc::new(PunchPeer::new(a_addr));
        let backlog = Arc::new(UdpBacklog::new());
        let dest = Dest::Punch(peer.clone());

        // 阶段1:候选=A,两报 → A
        backlog.push(b"P1");
        backlog.push(b"P2");
        drain_until_empty(&sender, &backlog, &dest).await;
        // 阶段2:rotate → B,两报 → B
        peer.rotate(b_addr);
        backlog.push(b"P3");
        backlog.push(b"P4");
        drain_until_empty(&sender, &backlog, &dest).await;
        // 阶段3:learn(A) 锁定,后续 rotate 为 no-op,两报 → A
        assert!(peer.learn(a_addr));
        peer.rotate(b_addr);
        backlog.push(b"P5");
        backlog.push(b"P6");
        drain_until_empty(&sender, &backlog, &dest).await;

        async fn read_all(sock: &UdpSocket, expect: &[&[u8]]) {
            let mut buf = [0u8; 64];
            for e in expect {
                let (n, _) = tokio::time::timeout(Duration::from_secs(2), sock.recv_from(&mut buf))
                    .await
                    .expect("recv timeout")
                    .expect("recv");
                assert_eq!(&buf[..n], *e, "顺序与落点必须精确");
            }
        }
        read_all(&a, &[b"P1", b"P2", b"P5", b"P6"]).await;
        read_all(&b, &[b"P3", b"P4"]).await;
        // 恰好一次:A/B 各收完预期后不得再有报文(短暂静默窗口)
        tokio::time::sleep(Duration::from_millis(100)).await;
        let mut probe_buf = [0u8; 64];
        assert!(
            a.try_recv_from(&mut probe_buf).is_err(),
            "A 不得收到重复/错位报文"
        );
        assert!(
            b.try_recv_from(&mut probe_buf).is_err(),
            "B 不得收到重复/错位报文"
        );
        let st = backlog.stats();
        assert_eq!(st.sent, 6, "6 报恰好各发一次");
        assert_eq!(st.queued_packets, 0);
    }

    /// drain 真实错误经错误桥直达 punch 会话 IO(idle 会话不等下一次
    /// 写),后续 PunchOutput::write 报 Err(毒化)。
    #[tokio::test]
    async fn c3_punch_fatal_bridges_to_io() {
        let peer = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let peer_addr = peer.local_addr().unwrap();
        let (mut io, shared) = new_io();
        let backlog = Arc::new(UdpBacklog::new());
        backlog.attach_session(&shared);
        let mut out = PunchOutput {
            sock: Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap()),
            peer: Arc::new(PunchPeer::new(peer_addr)),
            backlog: backlog.clone(),
        };
        backlog.push(b"x");
        assert!(matches!(
            drain_once(&backlog, |_| Err(std::io::Error::other(
                "punch-fatal-inject"
            ))),
            DrainSt::Fatal
        ));
        // idle 会话无任何后续写:IO 读侧必须立即感知错误
        let mut one = [0u8; 1];
        let r = tokio::time::timeout(Duration::from_millis(200), io.read(&mut one)).await;
        assert!(
            matches!(&r, Ok(Err(e)) if e.to_string().contains("punch-fatal-inject")),
            "fatal 必须经错误桥直达 IO: {r:?}"
        );
        assert!(out.write(b"y").is_err(), "毒化后的 punch 输出写必须报错");
    }

    // ================= 关闭假成功守卫(idle/通道提前结束) =================

    /// 已接受写从未被 ACK、显式 shutdown 未完成时,idle 终止必须
    /// 可观察 Err,不得 error=None 伪成功关闭(主代理独立红:
    /// main-independent/shutdown-idle-result-red.log exit101)。
    #[tokio::test]
    async fn idle_termination_must_not_report_successful_shutdown_of_unacked_bytes() {
        let (mut io, shared) = new_io();
        let mut kcp = Kcp::new_stream(0xFA84, DiscardOutput);
        kcp.set_mtu(MTU).unwrap();
        kcp.set_nodelay(true, 2, 2, true);
        let (_keep_tx, rx) = mpsc::channel(8);
        let session = tokio::spawn(run_session_with_idle_timeout(
            kcp,
            rx,
            shared,
            Duration::from_millis(30),
        ));
        io.write_all(b"accepted-but-never-acked").await.unwrap();
        let shutdown = tokio::time::timeout(Duration::from_millis(250), io.shutdown()).await;
        session.abort();
        let _ = session.await;
        assert!(
            !matches!(shutdown, Ok(Ok(()))),
            "idle timeout dropped pending ACK state but shutdown incorrectly returned Ok: {shutdown:?}"
        );
    }

    /// 已接受写未确认时 inbound 通道提前关闭(demux 丢弃会话而用户
    /// 句柄仍在):poll_shutdown 与 poll_flush 必须可观察 Err ——
    /// 不得伪成功,也不得永远 Pending。
    #[tokio::test]
    async fn channel_closed_with_pending_writes_must_be_observable_err() {
        let (mut io, shared) = new_io();
        let mut kcp = Kcp::new_stream(0xFA85, DiscardOutput);
        kcp.set_mtu(MTU).unwrap();
        let (tx, rx) = mpsc::channel(8);
        let session = tokio::spawn(run_session_with_idle_timeout(
            kcp,
            rx,
            shared,
            // 不靠 idle:由通道关闭触发退出
            Duration::from_secs(30),
        ));
        io.write_all(b"pending-when-channel-closes").await.unwrap();
        drop(tx);
        let shutdown = tokio::time::timeout(Duration::from_millis(250), io.shutdown()).await;
        session.abort();
        let _ = session.await;
        assert!(
            matches!(shutdown, Ok(Err(_))),
            "通道提前结束且写未确认:shutdown 必须 Err: {shutdown:?}"
        );
        let flushed = tokio::time::timeout(Duration::from_millis(250), io.flush()).await;
        assert!(
            matches!(flushed, Ok(Err(_))),
            "真终止后 flush 不得 Pending/伪 Ok: {flushed:?}"
        );
    }

    // ================= D:入站合批 =================

    /// 等会话静默:计数连续 stable_n 个 yield 无变化(暂停时钟下仅
    /// permit 驱动唤醒,permit 排空后不再有轮次)。
    async fn wait_stable(shared: &Arc<Shared>, stable_n: usize) {
        let mut last = sess(shared).flush_calls;
        let mut stable = 0usize;
        for _ in 0..4000 {
            tokio::task::yield_now().await;
            let c = sess(shared).flush_calls;
            if c == last {
                stable += 1;
                if stable >= stable_n {
                    return;
                }
            } else {
                stable = 0;
                last = c;
            }
        }
    }

    async fn spin_until(shared: &Arc<Shared>, pred: impl Fn(KcpCounters) -> bool) {
        for _ in 0..10_000 {
            if pred(sess(shared)) {
                return;
            }
            tokio::task::yield_now().await;
        }
    }

    async fn setup_ack_session(
        conv: u32,
    ) -> (
        KcpIo,
        Arc<Shared>,
        Arc<Mutex<Vec<Vec<u8>>>>,
        mpsc::Sender<SessionCmd>,
        tokio::task::JoinHandle<()>,
    ) {
        let (io, shared) = new_io();
        let rec = Arc::new(Mutex::new(Vec::<Vec<u8>>::new()));
        let mut kcp = Kcp::new_stream(conv, RecVecOut(rec.clone()));
        kcp.set_mtu(MTU).unwrap();
        kcp.set_wndsize(256, 256);
        kcp.set_nodelay(true, 2, 2, true);
        let (tx, rx) = mpsc::channel::<SessionCmd>(512);
        let task = tokio::spawn(run_session(kcp, rx, shared.clone()));
        yield16().await;
        (io, shared, rec, tx, task)
    }

    /// K=8 个纯 ACK(对已发段,无 user_wrote/recv_moved)同一唤醒到达:
    /// 整批合并为 ≤1 次 flush(基线红:每包一 flush = 8)。
    #[tokio::test(start_paused = true)]
    async fn b11_batch_ack_single_flush() {
        const K: usize = 8;
        let conv = 0xB11Cu32;
        let (mut io, shared, rec, tx, task) = setup_ack_session(conv).await;
        let seg = vec![0xABu8; MSS];
        for _ in 0..K {
            io.write_all(&seg).await.unwrap();
        }
        spin_until(&shared, |_| rec.lock().unwrap().len() >= K).await;
        assert!(rec.lock().unwrap().len() >= K, "前置:K 段必须已输出");
        wait_stable(&shared, 300).await;
        let sns: Vec<(u32, u32)> = rec
            .lock()
            .unwrap()
            .iter()
            .take(K)
            .map(|d| {
                (
                    u32::from_le_bytes([d[12], d[13], d[14], d[15]]),
                    u32::from_le_bytes([d[8], d[9], d[10], d[11]]),
                )
            })
            .collect();
        let base = sess(&shared);
        for (sn, ts) in sns {
            tx.try_send(SessionCmd::Packet(PooledPkt::copy_of(&ack_for(
                conv, sn, ts,
            ))))
            .unwrap();
        }
        spin_until(&shared, |c| {
            c.packets_consumed - base.packets_consumed >= K as u64
        })
        .await;
        wait_stable(&shared, 100).await;
        let after = sess(&shared);
        assert_eq!(
            after.packets_consumed - base.packets_consumed,
            K as u64,
            "K 个 ACK 必须全部消费"
        );
        let flushes = after.flush_calls - base.flush_calls;
        assert!(
            flushes <= 1,
            "K=8 个纯 ACK 输入必须合并为 ≤1 次 flush: {flushes}"
        );
        task.abort();
        let _ = task.await;
    }

    /// 单个入包:恰 1 轮 + 1 次 flush,不等待凑批(冻结守护,t02 语义
    /// 在入站侧的镜像)。
    #[tokio::test(start_paused = true)]
    async fn b12_single_packet_one_round_one_flush() {
        let conv = 0xB12Cu32;
        let (mut io, shared, rec, tx, task) = setup_ack_session(conv).await;
        let seg = vec![0xCDu8; MSS];
        io.write_all(&seg).await.unwrap();
        spin_until(&shared, |_| !rec.lock().unwrap().is_empty()).await;
        wait_stable(&shared, 300).await;
        let (sn, ts) = {
            let g = rec.lock().unwrap();
            (
                u32::from_le_bytes([g[0][12], g[0][13], g[0][14], g[0][15]]),
                u32::from_le_bytes([g[0][8], g[0][9], g[0][10], g[0][11]]),
            )
        };
        let base = sess(&shared);
        tx.try_send(SessionCmd::Packet(PooledPkt::copy_of(&ack_for(
            conv, sn, ts,
        ))))
        .unwrap();
        spin_until(&shared, |c| c.packets_consumed - base.packets_consumed >= 1).await;
        wait_stable(&shared, 100).await;
        let after = sess(&shared);
        assert_eq!(after.packets_consumed - base.packets_consumed, 1);
        assert_eq!(
            after.flush_calls - base.flush_calls,
            1,
            "单包不得等待凑批:恰 1 次 flush"
        );
        assert_eq!(after.wakeups - base.wakeups, 1, "单包恰 1 轮");
        task.abort();
        let _ = task.await;
    }

    /// 通道关闭(且空):立即退出,不得热转到 idle 超时(冻结守护)。
    #[tokio::test(start_paused = true)]
    async fn b_channel_closed_zero_break() {
        let (io, shared) = new_io();
        let (tx, rx) = mpsc::channel::<SessionCmd>(4);
        let kcp = Kcp::new_stream(0xB13C, DiscardOutput);
        let task = tokio::spawn(run_session(kcp, rx, shared.clone()));
        yield16().await;
        drop(tx);
        let exited = tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .is_ok();
        assert!(exited, "通道关闭(且空)必须立即退出");
        assert!(
            shared.guarded.lock().unwrap().closed,
            "退出后 closed 可观察"
        );
        drop(io);
    }

    /// 满批入站(>BATCH_CAP)下用户写必须在有界轮内被服务(公平性守护)。
    #[tokio::test(start_paused = true)]
    async fn b_full_batch_write_fairness() {
        let (mut io, shared, rec, tx, task) = setup_ack_session(0xB14C).await;
        // 通道预置 200 个入站(conv 全 0,kcp.input 容错丢弃,只驱动轮次)
        let filler = vec![0u8; 24];
        for _ in 0..200 {
            tx.try_send(SessionCmd::Packet(PooledPkt::copy_of(&filler)))
                .unwrap();
        }
        // 满批未清空时用户写到达:有界轮内必须被服务(输出可观察)
        io.write_all(b"FAIRNESS-USER-WRITE").await.unwrap();
        let user = b"FAIRNESS-USER-WRITE";
        let mut found = false;
        for _ in 0..10_000 {
            {
                let g = rec.lock().unwrap();
                if g.iter().any(|d| d.windows(user.len()).any(|w| w == user)) {
                    found = true;
                    break;
                }
            }
            tokio::task::yield_now().await;
        }
        assert!(found, "满批入站下用户写必须有界轮内被服务");
        spin_until(&shared, |c| c.packets_consumed >= 200).await;
        assert!(sess(&shared).packets_consumed >= 200, "全部入包最终被消费");
        task.abort();
        let _ = task.await;
    }

    /// 批缓冲跨轮复用不增长:300+ 入包跨多轮消费,容量高水位 ≤ BATCH_CAP。
    #[tokio::test(start_paused = true)]
    async fn d_batch_reuse_no_growth() {
        let (_io, shared, _rec, tx, task) = setup_ack_session(0xB15C).await;
        let filler = vec![0u8; 24];
        for _ in 0..200 {
            tx.try_send(SessionCmd::Packet(PooledPkt::copy_of(&filler)))
                .unwrap();
        }
        spin_until(&shared, |c| c.packets_consumed >= 200).await;
        for _ in 0..100 {
            tx.try_send(SessionCmd::Packet(PooledPkt::copy_of(&filler)))
                .unwrap();
        }
        spin_until(&shared, |c| c.packets_consumed >= 300).await;
        assert!(
            sess(&shared).packets_consumed >= 300,
            "300 入包必须全部消费"
        );
        let hwm = shared.batch_cap_hwm.load(Ordering::Relaxed);
        assert!(
            hwm <= BATCH_CAP as u64,
            "批缓冲容量高水位 {hwm} 不得超过 BATCH_CAP={BATCH_CAP}(跨轮复用不增长)"
        );
        task.abort();
        let _ = task.await;
    }
    // ---------------- A8 预认证资源闸回归 ----------------

    /// 退休表必须固定容量:FIFO 驱逐最旧条目,表长永远 ≤ RETIRED_CAPACITY。
    /// 修前是"超过 1024 才 retain 未过期条目",90s 窗口内单 conv 洪水可单调
    /// 增长到任意长度,且每次插入 O(n) 扫描。
    #[test]
    fn a8_retired_table_is_bounded() {
        let mut tbl = RetiredTable::default();
        for i in 0..(RETIRED_CAPACITY as u32 * 2) {
            tbl.insert(i, std::time::Instant::now() + Duration::from_secs(90));
        }
        assert!(
            tbl.len() <= RETIRED_CAPACITY,
            "退休表超过容量: {}",
            tbl.len()
        );
        // 最旧的 conv 已被驱逐(允许极小概率重退让)
        assert!(!tbl.is_retired(0), "最旧条目必须被驱逐");
        // 最新的仍在
        assert!(
            tbl.is_retired(RETIRED_CAPACITY as u32 * 2 - 1),
            "最新条目必须保留"
        );
    }

    /// 过期条目惰性删除:同一 conv 在 TTL 之后不再命中。
    #[test]
    fn a8_retired_entry_expires() {
        let mut tbl = RetiredTable::default();
        tbl.insert(7, std::time::Instant::now() + Duration::from_millis(1));
        std::thread::sleep(Duration::from_millis(5));
        assert!(!tbl.is_retired(7), "过期条目必须不再命中");
        assert_eq!(tbl.len(), 0, "过期条目必须在查询时删除");
    }

    /// 预认证闸:同一 IP 短时间内建会话数受速率限制,且全局计数可用。
    #[test]
    fn a8_session_gate_limits_per_ip_rate() {
        let g = SessionGate::new(SessionGateConfig {
            max_sessions: 1024,
            new_sessions_per_ip_per_sec: 4.0,
        });
        let ip: std::net::IpAddr = "10.1.2.3".parse().unwrap();
        let mut admitted = 0;
        for _ in 0..16 {
            if g.admit(ip) {
                admitted += 1;
            }
        }
        assert_eq!(admitted, 4, "同 IP 首秒只应放行桶容量 4 个");
        // 另一个 IP 不受影响
        let ip2: std::net::IpAddr = "10.1.2.4".parse().unwrap();
        assert!(g.admit(ip2), "不同源 IP 必须独立计量");
    }

    /// 全局在建会话上限:达到 max_sessions 后新会话一律拒绝。
    #[test]
    fn a8_session_gate_global_cap() {
        let g = SessionGate::new(SessionGateConfig {
            max_sessions: 3,
            new_sessions_per_ip_per_sec: 1000.0,
        });
        let ip: std::net::IpAddr = "10.9.9.9".parse().unwrap();
        assert!(g.admit(ip));
        assert!(g.admit(ip));
        assert!(g.admit(ip));
        assert!(!g.admit(ip), "超过全局上限必须拒绝");
        g.release();
        assert!(g.admit(ip), "释放名额后应重新放行");
    }

    /// A7:punch_listen 只接受候选来源。第三方(非候选)首发 kcp 包不得锁定
    /// 会话;候选来源随后仍能正常建流。
    #[tokio::test]
    async fn a7_punch_listen_rejects_non_candidate_source() {
        let listen_sock = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let listen_addr = listen_sock.local_addr().unwrap();
        // 候选来源 = dial 侧真实 socket 地址(服务端从 STUN/本地候选得知)
        let dial_sock = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let dial_addr = dial_sock.local_addr().unwrap();
        let attacker = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());

        let ls = listen_sock.clone();
        let listener = tokio::spawn(async move { punch_listen(ls, &[dial_addr]).await });

        // 第三方先用伪造的 kcp 包头(conv + PUSH)敲门:不得锁定会话
        let mut bogus = vec![0u8; KCP_OVERHEAD + 4];
        bogus[0..4].copy_from_slice(&0xdead_beefu32.to_le_bytes());
        bogus[4] = 81;
        let _ = attacker.send_to(&bogus, listen_addr).await;
        tokio::time::sleep(Duration::from_millis(120)).await;
        assert!(
            !listener.is_finished(),
            "非候选来源不得让 punch_listen 完成会话锁定"
        );

        // 候选来源接手:正常打洞应成功
        let dialer = tokio::spawn(async move { punch_dial(dial_sock, &[listen_addr], 8).await });
        let mux = tokio::time::timeout(Duration::from_secs(10), dialer)
            .await
            .expect("dial join")
            .expect("dial task")
            .expect("punch dial");
        let _mux_l = tokio::time::timeout(Duration::from_secs(10), listener)
            .await
            .expect("listen join")
            .expect("listen task")
            .expect("punch listen");
        drop(mux);
    }

    /// A7:punch_dial 只从服务端下发的候选集合 learn 对端。非候选来源
    /// 的首个合法 kcp 包不得锁定对端;候选来源仍能完成打洞。
    #[tokio::test]
    async fn a7_punch_dial_rejects_non_candidate_source() {
        let listen_sock = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let cand = listen_sock.local_addr().unwrap();
        let dial_sock = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let dial_addr = dial_sock.local_addr().unwrap();
        let attacker = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());

        let ls = listen_sock.clone();
        let listener = tokio::spawn(async move { punch_listen(ls, &[dial_addr]).await });
        // 攻击者先向 dial 侧发合法 kcp 包头
        let ds = dial_sock.clone();
        let dialer = tokio::spawn(async move { punch_dial(ds, &[cand], 8).await });
        tokio::time::sleep(Duration::from_millis(100)).await;
        let mut bogus = vec![0u8; KCP_OVERHEAD + 4];
        bogus[0..4].copy_from_slice(&0x0bad_f00du32.to_le_bytes());
        bogus[4] = 81;
        let _ = attacker.send_to(&bogus, dial_addr).await;
        tokio::time::sleep(Duration::from_millis(120)).await;

        let mux = tokio::time::timeout(Duration::from_secs(10), dialer)
            .await
            .expect("dial join")
            .expect("dial task")
            .expect("候选来源仍必须能完成打洞");
        let _mux_l = tokio::time::timeout(Duration::from_secs(10), listener)
            .await
            .expect("listen join")
            .expect("listen task")
            .expect("punch listen");
        drop(mux);
    }
}
