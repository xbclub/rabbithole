//! snappy 流压缩(代理级 use_compression)。
//!
//! 实现:pump 任务 + duplex 管道 —— 用户侧拿到一条普通字节流,
//! 两个后台任务分别负责「压缩写」与「解压读」,压缩逻辑全部是
//! async fn 循环(read/read_buf/write_all),不手写任何 poll 状态机。
//!
//! 帧格式:`varint(压缩块长) + snappy raw 块`,单块未压缩 ≤64KiB。
//! 流损坏/截断时 pump 退出,上层表现为连接断开。
//!
//! # 每块固定开销(round-49)
//!
//! 数据面按 64KiB 切块,块级堆分配与 await 次数直接决定小包/高 PPS 场景的
//! 开销。泵内所有缓冲都提到 loop 外,稳态下每块:
//!
//! * 压侧:`buf`(未压缩输入)与 `wire`(输出帧)复用。`wire` 预留 5 字节
//!   varint 占位,snappy 直接写进 `wire` 的 spare 区,回填长度后一次
//!   `write_all` —— 省掉旧实现「comp 分配 → append 拷进 wire」的整块拷贝。
//! * 解侧:`rbuf`(持久大读缓冲)、`out`(解压结果)、`Decoder` 全部复用。
//!   一次 `read` 尽量填满 128KiB 缓冲,帧在缓冲内连续解析(varint + 帧体
//!   直接切片解压),缺字节才回到读端续读 —— 取代旧的「Stash 逐字节补
//!   varint + read_exact 帧体」(每帧至少 2 次读 await,小帧密集场景下
//!   读调用数是帧数的两倍,syscall 同比;旧实现已随之删除)。
//!
//! 稳态:压侧每块 0 次堆分配 + 1 次写 await;解侧每块 0 次堆分配,读 await
//! 摊薄到约每 128KiB 缓冲一次(小帧串一次读可覆盖数十帧)。

use crate::BoxedIo;
use bytes::{Buf, BufMut, BytesMut};
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const MAX_UNCOMPRESSED_CHUNK: usize = 64 * 1024;
const MAX_DECOMPRESSED_CHUNK: usize = 4 * 1024 * 1024;
/// varint 长度前缀的字节上限(协议约定,见模块文档)。
const MAX_VARINT_LEN: usize = 5;
/// 解侧持久读缓冲容量:一次 `read` 尽量填满,缓冲内连续解析多帧。
/// 协议单帧上限 = `max_frame_len()` ≈ 76.5KiB,128KiB 既保证半截的最大帧
/// 也装得下(容量即硬上界,解析残余恒小于单帧、永不触发扩容),又让
/// 小帧密集场景一次读覆盖数十帧。
const READ_CHUNK: usize = 128 * 1024;

/// 单块压缩帧的缓冲上限:5 字节 varint + snappy 最坏膨胀。
fn max_frame_len() -> usize {
    MAX_VARINT_LEN + snap::raw::max_compress_len(MAX_UNCOMPRESSED_CHUNK)
}

/// 把 `inner` 包一层端到端 snappy 压缩,返回面向调用方的普通字节流。
/// 丢弃返回值时 pump 任务随之结束并释放 inner。
pub async fn compress_stream(inner: BoxedIo) -> BoxedIo {
    // B-2:两条独立单向 duplex(各 64KiB/方向,总量与单 duplex 相同)。
    // 修前一条 duplex + `tokio::io::split` 让两个 pump 任务共享一把
    // std::sync::Mutex(DuplexStream 无 into_split);四半端各自独立,
    // 天然无锁。用户侧两个半端由 [`JoinDuplex`] 拼成单一字节流对象。
    let (user_w, pump_r) = tokio::io::duplex(64 * 1024); // 上行:用户 → 压泵
    let (pump_w, user_r) = tokio::io::duplex(64 * 1024); // 下行:解泵 → 用户
                                                         // B4:底层可无锁拆半时(裸 TcpStream / mux 流 / QUIC BiStream,
                                                         // 见 split_boxed_io)优先走独立半;包装类型回落 `tokio::io::split`
                                                         //(与 A2 同机理,round-69 实测 futex 占 75% syscall 时间)。
                                                         // mux 流命中场景:use_compression=true 且 multiplex=true,
                                                         // 修前 compress_pump 与 decompress_pump 抢同一把同步锁。
    let (inner_r, inner_w): (
        Box<dyn tokio::io::AsyncRead + Send + Unpin>,
        Box<dyn tokio::io::AsyncWrite + Send + Unpin>,
    ) = match crate::split_boxed_io(inner) {
        Ok((r, w)) => (Box::new(r), Box::new(w)),
        Err(io) => {
            note_split_fallback();
            let (r, w) = tokio::io::split(io);
            (Box::new(r), Box::new(w))
        }
    };
    tokio::spawn(compress_pump(pump_r, inner_w));
    tokio::spawn(decompress_pump(inner_r, pump_w));
    Box::new(JoinDuplex {
        r: user_r,
        w: user_w,
    })
}

/// B-2:两条单向 duplex 的用户侧半端拼合(读走 `r`,写走 `w`)。
/// DuplexStream 自身就是"读写两半 + 内部双通道"的拼合体,这里同型:
/// poll_read 委托 r,poll_write/flush/shutdown 委托 w;drop 时两半
/// 各自关闭对应方向,pump 侧按 EOF/写失败收敛(与修前语义一致)。
struct JoinDuplex {
    r: tokio::io::DuplexStream,
    w: tokio::io::DuplexStream,
}

impl tokio::io::AsyncRead for JoinDuplex {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.r).poll_read(cx, buf)
    }
}

impl tokio::io::AsyncWrite for JoinDuplex {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.w).poll_write(cx, buf)
    }
    // 向量写透传:tokio DuplexStream 原生支持(io/util/mem.rs 覆写了
    // poll_write_vectored,能力位恒 true)。漏掉会让 compress 流用户侧的
    // 批量 writev 落到 tokio 默认实现(每次只写第一个非空段),退化成
    // 逐段标量写——与 HalfWrite / rh-server CountedTcpWriteHalf 同因。
    // 部分写/WriteZero 语义靠委托保持原生行为,不自己实现。
    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.w).poll_write_vectored(cx, bufs)
    }
    fn is_write_vectored(&self) -> bool {
        // DuplexStream 恒 true;透传而不是写死字面量,与 PrependedIo/
        // QuotaIo 的「能力位跟随内层」语义一致。
        self.w.is_write_vectored()
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.w).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.w).poll_shutdown(cx)
    }
}

// 测试探针:compress_stream 对 inner 回落 `tokio::io::split` 的次数。
// 仅测试读取(断言 mux 内层命中无锁拆分);生产路径零开销。
//
// thread_local 而非全局原子:split 发生在 compress_stream 调用者线程
// (pump 才是 spawn 的),`#[tokio::test]` 的 current_thread 语义下
// 读写都落在测试自己的线程 —— 并行跑的其它 compress 测试(它们也走
// 回落分支)不会污染本计数,无需串行化。与下方 COUNTING 探针同款。
//(`thread_local!` 是宏,`///` 文档注释无法附加到展开项,用普通注释。)
#[cfg(test)]
thread_local! {
    static SPLIT_FALLBACKS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// 回落计数(测试观测点)。生产编译为空函数——不改任何行为,只保证
/// 调用点在两个 cfg 下都存在,避免代码路径分叉。
#[cfg(test)]
fn note_split_fallback() {
    SPLIT_FALLBACKS.with(|c| c.set(c.get() + 1));
}
#[cfg(not(test))]
fn note_split_fallback() {}

fn varint_len(mut v: usize) -> usize {
    let mut n = 1;
    while v >= 0x80 {
        v >>= 7;
        n += 1;
    }
    n
}

/// 把 `v` 的 varint 编码写进 `dst` 开头(`dst.len()` 必须 ≥ `varint_len(v)`)。
fn write_varint(dst: &mut [u8], mut v: usize) {
    let mut i = 0;
    while v >= 0x80 {
        dst[i] = ((v & 0x7f) as u8) | 0x80;
        v >>= 7;
        i += 1;
    }
    dst[i] = v as u8;
}

/// 把 `wire` 的未填充尾部当作 `&mut [u8]` 交给 snappy 直接写。
///
/// # Safety
///
/// 调用方必须保证:
/// 1. `wire` 底层分配在 `wire.len()..wire.capacity()` 区间已全部初始化
///    (本文件在循环外 `resize(max_frame_len(), 0)` 一次性完成,之后
///    `clear` / `resize(5)` / `advance_mut` 都不会扩容);
/// 2. `max <= wire.capacity() - wire.len()`。
#[inline]
unsafe fn spare_mut(wire: &mut BytesMut, max: usize) -> &mut [u8] {
    let spare = wire.spare_capacity_mut();
    debug_assert!(spare.len() >= max, "spare {} < {}", spare.len(), max);
    // SAFETY:见函数文档,长度不超过 spare 容量且该区间已初始化。
    unsafe { std::slice::from_raw_parts_mut(spare.as_mut_ptr().cast::<u8>(), max) }
}

async fn compress_pump<R, W>(mut src: R, mut dst: W)
where
    R: AsyncReadExt + Unpin,
    W: AsyncWriteExt + Unpin,
{
    // 任何退出路径都显式关闭写方向(WriteHalf drop 不会关闭 duplex)
    let mut enc = snap::raw::Encoder::new();
    let mut buf = vec![0u8; MAX_UNCOMPRESSED_CHUNK];
    // 一次性把整块帧缓冲初始化:此后每轮只改长度,spare 区恒为已初始化内存,
    // `spare_mut` 的 `&mut [u8]` 视图才成立。
    let mut wire = BytesMut::with_capacity(max_frame_len());
    wire.resize(max_frame_len(), 0);
    loop {
        match src.read(&mut buf).await {
            Ok(0) => break,
            Ok(n) => {
                let max = snap::raw::max_compress_len(n);
                // 清空后先占住 5 字节 varint 位;若实际只用 k 字节,
                // 输出切片从 5-k 开始,帧体本身不做任何搬移。
                wire.clear();
                wire.resize(MAX_VARINT_LEN, 0);
                // SAFETY:循环前已整体初始化,resize 只缩 len;snap 只写不读该区域。
                let spare = unsafe { spare_mut(&mut wire, max) };
                let clen = match enc.compress(&buf[..n], spare) {
                    Ok(c) => c,
                    Err(_) => break,
                };
                // SAFETY:snap 已写满 spare 的前 clen 字节。
                unsafe { wire.advance_mut(clen) };
                let k = varint_len(clen);
                let start = MAX_VARINT_LEN - k;
                write_varint(&mut wire[start..MAX_VARINT_LEN], clen);
                if dst.write_all(&wire[start..]).await.is_err() {
                    break;
                }
            }
            Err(_) => break,
        }
    }
    let _ = dst.shutdown().await;
}

/// 从 `bytes` 开头解析一个 varint。
///
/// 返回 `Some((值, 用掉的字节数))`;返回 `None` 表示「前缀还没读完」
/// (调用方应继续读)或者「超过 5 字节仍无终止位」(非法,由调用方判定)。
fn parse_varint(bytes: &[u8]) -> Option<(usize, usize)> {
    let mut acc: u64 = 0;
    let mut shift: u32 = 0;
    for (i, &b) in bytes.iter().take(MAX_VARINT_LEN).enumerate() {
        acc |= u64::from(b & 0x7f) << shift;
        if b & 0x80 == 0 {
            return usize::try_from(acc).ok().map(|v| (v, i + 1));
        }
        shift += 7;
    }
    None
}

// (旧实现的跨帧回退区 `Stash` 与 `read_frame_len` —— 逐帧「varint 补读 +
// read_exact 帧体」—— 已被下方持久大读缓冲方案整体取代:每帧至少 2 次读
// await/syscall,小帧密集场景读调用数是帧数的两倍。新实现不再引用它们,
// 单测也不依赖,直接删除;边界行为由缓冲内解析统一覆盖。)

async fn decompress_pump<R, W>(mut src: R, mut dst: W)
where
    R: AsyncReadExt + Unpin,
    W: AsyncWriteExt + Unpin,
{
    // 持久大读缓冲:一次 read 尽量填满(容量即上界,见 READ_CHUNK),帧在
    // 缓冲内连续解析,稳态读 await 从「每帧 2 次」摊薄到「每 128KiB 一次」。
    // 缓冲有界且零搬运:解析消费用 advance 归还头部,未解析残余恒为单帧
    // 前缀(< max_frame_len()),补读需求 ≤ 单帧上限 < 初始容量,reserve
    // 永远走 no-op 分支 —— 不扩容、不随帧大小或流量增长。
    // 逐帧复用的解压输出缓冲与 Decoder 与旧实现相同。
    let mut rbuf = BytesMut::with_capacity(READ_CHUNK);
    let mut out = vec![0u8; MAX_UNCOMPRESSED_CHUNK];
    let mut dec = snap::raw::Decoder::new();
    let max_body = snap::raw::max_compress_len(MAX_UNCOMPRESSED_CHUNK);
    'pump: loop {
        // ---- 解析:连续消化缓冲内所有完整帧,直到缺字节或坏帧 ----
        // `want` 是「至少还要补多少字节才能继续解析」:半截帧补完一帧,
        // 半截 varint 补满 5 字节。实际单次读入量不受它限制 —— read_buf
        // 总是尽量填满剩余容量(≈READ_CHUNK),一次读可带回多帧。
        let mut want = MAX_VARINT_LEN;
        loop {
            let (len, used) = match parse_varint(&rbuf) {
                Some(v) => v,
                // 第 5 字节仍带 continuation:非法长度前缀,拒绝整条流
                None if rbuf.len() >= MAX_VARINT_LEN => break 'pump,
                None => break, // varint 未读满,补读后再解析
            };
            if len == 0 || len > max_body {
                break 'pump; // 零长 / 超单块压缩上限:非法帧长,拒绝
            }
            let total = used + len;
            if rbuf.len() < total {
                want = total - rbuf.len(); // 半截帧体:补完这一帧
                break;
            }
            // 帧体直接在缓冲内切片,不再拷进独立的 comp 缓冲。
            let body = &rbuf[used..total];
            let expected = match snap::raw::decompress_len(body) {
                Ok(e) => e,
                Err(_) => break 'pump,
            };
            if expected > MAX_DECOMPRESSED_CHUNK {
                break 'pump;
            }
            // 常态 ≤64KiB,永远走不到扩容;只有对端发来 >64KiB 的单块才会长一次。
            if out.len() < expected {
                out.resize(expected, 0);
            }
            match dec.decompress(body, &mut out[..expected]) {
                Ok(n) => {
                    if dst.write_all(&out[..n]).await.is_err() {
                        break 'pump;
                    }
                }
                Err(_) => break 'pump,
            }
            rbuf.advance(total);
        }
        // ---- 补读 ----
        rbuf.reserve(want);
        match src.read_buf(&mut rbuf).await {
            // EOF:缓冲已空 = 对端正常收尾;还挂着字节 = 半截帧中止。
            // 两者对上层同貌(pump 退出、读端见 EOF),与旧实现语义一致。
            Ok(0) => break 'pump,
            Ok(_) => {}
            Err(_) => break 'pump,
        }
    }
    // 显式关闭:上层 read 将收到 EOF 而非永久 Pending
    let _ = dst.shutdown().await;
}

/// 解压方向的单帧解码(测试与诊断用)。
pub fn decode_frame(comp: &[u8]) -> io::Result<Vec<u8>> {
    let expected =
        snap::raw::decompress_len(comp).map_err(|e| io::Error::other(format!("compress: {e}")))?;
    if expected > MAX_DECOMPRESSED_CHUNK {
        return Err(io::Error::other("compress: chunk too large"));
    }
    snap::raw::Decoder::new()
        .decompress_vec(comp)
        .map_err(|e| io::Error::other(format!("compress: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::alloc::{GlobalAlloc, Layout, System};
    use std::cell::Cell;
    use std::pin::Pin;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Context, Poll};
    use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

    // ---- 分配计数探针 ---------------------------------------------------
    //
    // 只统计「当前线程」在窗口内的分配:pump 跑在 current_thread runtime 上,
    // 整条压缩/解压路径都在测试线程内执行。用 thread-local 开关而不是全局
    // 开关,cargo test 的并行测试之间不会互相污染。
    thread_local! {
        static COUNTING: Cell<bool> = const { Cell::new(false) };
    }
    static ALLOCS: AtomicUsize = AtomicUsize::new(0);

    fn counting_on() -> bool {
        COUNTING.try_with(Cell::get).unwrap_or(false)
    }

    struct CountingAlloc;

    // SAFETY:全部转发给 System,仅在前后加一次原子自增。
    unsafe impl GlobalAlloc for CountingAlloc {
        unsafe fn alloc(&self, l: Layout) -> *mut u8 {
            if counting_on() {
                ALLOCS.fetch_add(1, Ordering::Relaxed);
            }
            unsafe { System.alloc(l) }
        }
        unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
            unsafe { System.dealloc(p, l) }
        }
        unsafe fn alloc_zeroed(&self, l: Layout) -> *mut u8 {
            if counting_on() {
                ALLOCS.fetch_add(1, Ordering::Relaxed);
            }
            unsafe { System.alloc_zeroed(l) }
        }
        unsafe fn realloc(&self, p: *mut u8, l: Layout, new: usize) -> *mut u8 {
            if counting_on() {
                ALLOCS.fetch_add(1, Ordering::Relaxed);
            }
            unsafe { System.realloc(p, l, new) }
        }
    }

    #[global_allocator]
    static ALLOC: CountingAlloc = CountingAlloc;

    /// 在计数窗口内运行 `f`,返回结果与窗口内的分配次数。
    fn count_allocs<T>(f: impl FnOnce() -> T) -> (T, usize) {
        let before = ALLOCS.load(Ordering::Relaxed);
        COUNTING.with(|c| c.set(true));
        let out = f();
        COUNTING.with(|c| c.set(false));
        (out, ALLOCS.load(Ordering::Relaxed) - before)
    }

    // ---- 测试用最小 IO --------------------------------------------------

    /// 稳定按 `chunk` 长度吐数据的读端(自身零分配)。
    struct SliceSrc {
        data: Vec<u8>,
        pos: usize,
        chunk: usize,
    }

    impl SliceSrc {
        fn new(data: Vec<u8>, chunk: usize) -> Self {
            Self {
                data,
                pos: 0,
                chunk,
            }
        }
    }

    impl AsyncRead for SliceSrc {
        fn poll_read(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            let remain = self.data.len() - self.pos;
            let n = remain.min(self.chunk).min(buf.remaining());
            let pos = self.pos;
            buf.put_slice(&self.data[pos..pos + n]);
            self.pos += n;
            Poll::Ready(Ok(()))
        }
    }

    /// 丢弃写入的写端。
    struct NullSink;

    impl AsyncWrite for NullSink {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            Poll::Ready(Ok(buf.len()))
        }
        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    /// 收集写入字节的写端(只在计数窗口外用,用来造 wire 数据)。
    #[derive(Default)]
    struct VecSink(Vec<u8>);

    impl AsyncWrite for VecSink {
        fn poll_write(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            self.0.extend_from_slice(buf);
            Poll::Ready(Ok(buf.len()))
        }
        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    /// 计数读端:统计 poll_read 调用次数。本文件的测试源都立即就绪,
    /// 每次 poll_read 即一次真实读,可用来断言「读次数与帧数解耦」。
    /// 计数用 Rc 句柄共享:pump 拿走读端所有权后测试侧仍能读数。
    struct CountingSrc<S> {
        inner: S,
        reads: std::rc::Rc<Cell<usize>>,
    }

    impl<S: AsyncRead + Unpin> AsyncRead for CountingSrc<S> {
        fn poll_read(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            self.reads.set(self.reads.get() + 1);
            Pin::new(&mut self.inner).poll_read(cx, buf)
        }
    }

    fn pseudo_random(n: usize, seed: u64) -> Vec<u8> {
        let mut out = Vec::with_capacity(n);
        let mut x = seed | 1;
        for _ in 0..n {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            out.push((x >> 24) as u8);
        }
        out
    }

    // ---- 三个既有测试:原样保留,断言一个字都没动 -----------------------

    #[tokio::test]
    async fn roundtrip_small() {
        let (a, b) = tokio::io::duplex(4096);
        let compressed = compress_stream(Box::new(b)).await;
        let mut w = compressed;
        let mut r_side = a;
        w.write_all(b"hello compress!").await.unwrap();
        w.flush().await.unwrap();
        // 直接从对端读的是压缩字节;先简单验证非空且与原文不同
        let mut wire = vec![0u8; 64];
        let n = tokio::time::timeout(std::time::Duration::from_secs(2), r_side.read(&mut wire))
            .await
            .unwrap()
            .unwrap();
        assert!(n > 0);
        assert_ne!(&wire[..n], b"hello compress!");
    }

    /// 端到端:两条 compress_stream 互连,验证数据完整性。
    #[tokio::test]
    async fn end_to_end_roundtrip_bulk() {
        // 模拟网络管道
        let (net_a, net_b) = tokio::io::duplex(64 * 1024);
        let client_stream = compress_stream(Box::new(net_a)).await;
        let server_stream = compress_stream(Box::new(net_b)).await;

        let mut data = vec![0xABu8; 3_000_000];
        let mut pseudo = Vec::with_capacity(200_000);
        let mut x: u64 = 0x2545F4914F6CDD1D;
        for _ in 0..200_000 {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            pseudo.push((x >> 24) as u8);
        }
        data.extend_from_slice(&pseudo);

        let writer = tokio::spawn(async move {
            let mut w = client_stream;
            w.write_all(&data).await.unwrap();
            w.shutdown().await.unwrap();
        });
        let reader = tokio::spawn(async move {
            let mut r = server_stream;
            let mut got = Vec::with_capacity(3_200_000);
            let mut buf = [0u8; 32 * 1024];
            loop {
                let n = tokio::time::timeout(std::time::Duration::from_secs(30), r.read(&mut buf))
                    .await
                    .expect("read timeout")
                    .unwrap();
                if n == 0 {
                    break;
                }
                got.extend_from_slice(&buf[..n]);
            }
            got
        });
        writer.await.unwrap();
        let got = reader.await.unwrap();
        assert_eq!(got.len(), 3_200_000);
        assert!(got[..3_000_000].iter().all(|&v| v == 0xAB));
        let mut x2: u64 = 0x2545F4914F6CDD1D;
        for (i, g) in got[3_000_000..].iter().enumerate() {
            x2 ^= x2 << 13;
            x2 ^= x2 >> 7;
            x2 ^= x2 << 17;
            assert_eq!(*g, (x2 >> 24) as u8, "pseudo mismatch at {}", i + 3_000_000);
        }
    }

    #[tokio::test]
    async fn corrupt_stream_closes() {
        let (mut a, b) = tokio::io::duplex(1024);
        // 对端发送非法长度后关闭
        tokio::spawn(async move {
            use tokio::io::AsyncWriteExt as _;
            let _ = a.write_all(&[0xff, 0xff, 0xff, 0xff, 0x7f]).await;
        });
        let mut r = compress_stream(Box::new(b)).await;
        let mut out = vec![0u8; 16];
        // 解压泵退出 → EOF(可能读到 0 字节或错误)
        let _ = r.read(&mut out).await;
        drop(tokio::io::duplex(1));
    }

    // ---- 新增:1MiB 随机往返 ---------------------------------------------

    async fn roundtrip_via_public_api(data: Vec<u8>) -> Vec<u8> {
        let (net_a, net_b) = tokio::io::duplex(64 * 1024);
        let client = compress_stream(Box::new(net_a)).await;
        let server = compress_stream(Box::new(net_b)).await;
        let writer = tokio::spawn(async move {
            let mut w = client;
            w.write_all(&data).await.unwrap();
            w.shutdown().await.unwrap();
        });
        let reader = tokio::spawn(async move {
            let mut r = server;
            let mut got = Vec::new();
            let mut buf = [0u8; 16 * 1024];
            loop {
                let n = tokio::time::timeout(std::time::Duration::from_secs(30), r.read(&mut buf))
                    .await
                    .expect("read timeout")
                    .unwrap();
                if n == 0 {
                    break;
                }
                got.extend_from_slice(&buf[..n]);
            }
            got
        });
        writer.await.unwrap();
        reader.await.unwrap()
    }

    /// 1MiB 随机数据走公开 API 编解码,逐字节一致。
    /// 随机数据压缩率≈1,可覆盖多块帧(16 块)与最坏膨胀路径。
    #[tokio::test]
    async fn roundtrip_random_1mib() {
        let data = pseudo_random(1024 * 1024, 0x2545_F491_4F6C_DD1D);
        let got = roundtrip_via_public_api(data.clone()).await;
        assert_eq!(got.len(), data.len(), "decoded length mismatch");
        assert!(got == data, "decoded bytes mismatch");
    }

    /// 非整块边界(小于 64KiB、跨块、空写)也要一致。
    #[tokio::test]
    async fn roundtrip_odd_sizes() {
        for size in [
            0usize, 1, 5, 127, 128, 4096, 65_535, 65_536, 65_537, 200_003,
        ] {
            let data = pseudo_random(size, 0x9E37_79B9_7F4A_7C15 ^ size as u64);
            let got = roundtrip_via_public_api(data.clone()).await;
            assert_eq!(got, data, "size {size} mismatch");
        }
    }

    /// varint 跨缓冲区边界:底层每次只给 1 字节,varint 会被拆成多次
    /// `fill_buf`。旧实现逐字节读天然正确,新实现必须同样能拼回来。
    #[tokio::test]
    async fn varint_spanning_buffer_boundary() {
        for chunks in [1usize, 2, 33] {
            let data = pseudo_random(MAX_UNCOMPRESSED_CHUNK * chunks, 0x0BAD_F00D_1234_5678);
            let mut sink = VecSink::default();
            let mut src = SliceSrc::new(data.clone(), MAX_UNCOMPRESSED_CHUNK);
            compress_pump(&mut src, &mut sink).await;
            let wire = sink.0;

            // 每次只吐 1 字节:任何 varint 都必须跨多次 fill_buf 才能拼完
            let mut r = tokio::io::BufReader::with_capacity(1, SliceSrc::new(wire, 1));
            let mut got = VecSink::default();
            decompress_pump(&mut r, &mut got).await;
            assert_eq!(got.0, data, "{chunks} 块在 1 字节边界下解码不一致");
        }
    }

    /// varint 前缀超过 5 字节(第 5 字节仍带 continuation)必须被拒。
    #[tokio::test]
    async fn overlong_varint_is_rejected() {
        use tokio::io::AsyncWriteExt as _;
        let (mut a, b) = tokio::io::duplex(1024);
        tokio::spawn(async move {
            // 5 个 continuation 字节,无终止字节
            let _ = a.write_all(&[0x80, 0x80, 0x80, 0x80, 0x80, 0x01]).await;
            let _ = a.shutdown().await;
        });
        let mut r = compress_stream(Box::new(b)).await;
        let mut out = vec![0u8; 16];
        let n = r.read(&mut out).await.unwrap_or(0);
        assert_eq!(n, 0, "非法 varint 不得产出任何解压字节");
    }

    /// 读路径边界:底层每次只给 1 字节(最坏分片)时,各种块数的往返必须一致。
    /// 这条专门盯 varint 与帧体被切在任意位置的情况。
    #[tokio::test]
    async fn roundtrip_with_byte_by_byte_source() {
        for chunks in [1usize, 2, 3, 17] {
            let data = pseudo_random(
                MAX_UNCOMPRESSED_CHUNK * chunks - 7,
                0x5EED_0000 + chunks as u64,
            );
            let mut sink = VecSink::default();
            let mut src = SliceSrc::new(data.clone(), MAX_UNCOMPRESSED_CHUNK);
            compress_pump(&mut src, &mut sink).await;

            for src_chunk in [1usize, 2, 3, 5, 7, 13] {
                let mut r = SliceSrc::new(sink.0.clone(), src_chunk);
                let mut got = VecSink::default();
                decompress_pump(&mut r, &mut got).await;
                assert_eq!(
                    got.0, data,
                    "chunks={chunks} src_chunk={src_chunk}: 逐字节分片下解码不一致"
                );
            }
        }
    }

    /// 跨帧回退:小帧(帧体比一次 read 能带回的还短)连续排布时,
    /// 多读的字节既有本帧帧体、也有下一帧的 varint,不能丢也不能错位。
    #[tokio::test]
    async fn tiny_frames_sharing_one_read() {
        // 3 字节输入几乎压不动,帧体很小;直接构造一长串小帧。
        let mut wire = Vec::new();
        let mut expect = Vec::new();
        for i in 0..200u8 {
            let payload = vec![i, i.wrapping_mul(7), 0xFF - i];
            let mut enc = snap::raw::Encoder::new();
            let body = enc.compress_vec(&payload).unwrap();
            let mut hdr = [0u8; MAX_VARINT_LEN];
            let k = varint_len(body.len());
            let start = MAX_VARINT_LEN - k;
            write_varint(&mut hdr[start..], body.len());
            wire.extend_from_slice(&hdr[start..]);
            wire.extend_from_slice(&body);
            expect.extend_from_slice(&payload);
        }
        // 一次读能拿回 64 字节 → 必然会同时覆盖「本帧帧体 + 下一帧 varint」
        for src_chunk in [7usize, 16, 64] {
            let mut r = SliceSrc::new(wire.clone(), src_chunk);
            let mut got = VecSink::default();
            decompress_pump(&mut r, &mut got).await;
            assert_eq!(got.0, expect, "src_chunk={src_chunk}: 小帧串解码错位");
        }
    }

    // ---- 新增:持久大读缓冲 —— 多帧连解与拒绝路径 ------------------------

    /// 一次读入多帧:整条 wire(< READ_CHUNK)单次 read 给足,解泵必须在
    /// 缓冲内连解完全部帧,读端只被碰 2 次(1 次数据读 + 1 次 EOF 探测)。
    /// 旧实现每帧「varint 补读 + read_exact 帧体」至少 2 次读,8 帧至少
    /// 16 次 —— 读次数与帧数解耦正是本次改造的目标行为。
    #[tokio::test]
    async fn single_read_parses_multiple_frames() {
        let frames = 8usize;
        let per = 9 * 1024;
        let mut data = Vec::with_capacity(frames * per);
        for i in 0..frames {
            data.extend_from_slice(&pseudo_random(per, 0x7000_0000 + i as u64));
        }
        // 9KiB 一读一块 → 每块一帧,共 8 帧;随机数据几乎不压缩,总 wire
        // 仍 < 128KiB,保证单次 read 能整条带回(下面有断言守住这个前提)。
        let mut sink = VecSink::default();
        let mut src = SliceSrc::new(data.clone(), per);
        futures::executor::block_on(compress_pump(&mut src, &mut sink));
        let wire = sink.0;
        assert!(
            wire.len() < READ_CHUNK,
            "测试前提:整条 wire({}) 应小于读缓冲 {}",
            wire.len(),
            READ_CHUNK
        );

        let reads = std::rc::Rc::new(Cell::new(0usize));
        let mut r = CountingSrc {
            inner: SliceSrc::new(wire, usize::MAX),
            reads: reads.clone(),
        };
        let mut got = VecSink::default();
        decompress_pump(&mut r, &mut got).await;
        assert_eq!(got.0, data, "多帧一次读入解码不一致");
        assert_eq!(
            reads.get(),
            2,
            "应为「1 次数据读 + 1 次 EOF 探测」,实际 {} 次 —— 多帧未在缓冲内连解",
            reads.get()
        );
    }

    /// 半截帧中止:EOF 落在帧体中间或 varint 中间,前序完整帧必须照常写出,
    /// 半截帧不得产出任何字节、不得错位、不得永久 Pending。
    #[tokio::test]
    async fn truncated_frame_at_eof_keeps_prior_frames_only() {
        // 3 帧:每块 200B 随机数据一次给足(压缩后 ≈200+B,varint 2 字节)
        let payload = pseudo_random(200, 0xABCD_1234);
        let mut data = payload.clone();
        data.extend_from_slice(&payload);
        data.extend_from_slice(&payload);
        let mut sink = VecSink::default();
        let mut src = SliceSrc::new(data.clone(), 200);
        futures::executor::block_on(compress_pump(&mut src, &mut sink));
        let wire = sink.0;

        // 逐帧走 varint 定位第 3 帧起点
        let mut pos = 0usize;
        for _ in 0..2 {
            let (len, used) = parse_varint(&wire[pos..]).unwrap();
            pos += used + len;
        }
        let third = pos;
        let (len3, used3) = parse_varint(&wire[third..]).unwrap();
        assert!(
            used3 >= 2 && wire[third] & 0x80 != 0,
            "测试前提:第 3 帧 varint ≥ 2 字节且首字节带 continuation"
        );
        assert!(len3 > 2, "测试前提:帧体截在 2 字节处仍是半截帧");

        // (a) EOF 落在第 3 帧帧体中间:varint 完整 + 帧体只到 2 字节
        // (b) EOF 落在第 3 帧第 1 个 varint 字节之后(半截 varint)
        for (name, cut) in [("帧体中间", third + used3 + 2), ("varint 中间", third + 1)] {
            let mut r = SliceSrc::new(wire[..cut].to_vec(), usize::MAX);
            let mut got = VecSink::default();
            decompress_pump(&mut r, &mut got).await;
            assert_eq!(got.0, &data[..400], "EOF 截在 {name}:只应写出前两帧");
        }
    }

    /// 非法帧长拒绝:len = 0 与 len = max_compress_len(64KiB)+1 都是坏帧,
    /// pump 在该位置整流退出;此前已完整到达的合法帧必须照常写出。
    /// (5 字节 continuation 的非法 varint 由上方 overlong_varint_is_rejected
    /// 覆盖;解压超限由 oversized_decompressed_chunk_is_rejected 覆盖。)
    #[tokio::test]
    async fn zero_and_oversized_frame_len_are_rejected() {
        let payload = pseudo_random(64, 0x0F0F_0F0F);
        let mut enc = snap::raw::Encoder::new();
        let body = enc.compress_vec(&payload).unwrap();
        let mut wire = Vec::with_capacity(MAX_VARINT_LEN + body.len());
        let k = varint_len(body.len());
        let mut hdr = [0u8; MAX_VARINT_LEN];
        let start = MAX_VARINT_LEN - k;
        write_varint(&mut hdr[start..], body.len());
        wire.extend_from_slice(&hdr[start..]);
        wire.extend_from_slice(&body);

        let max_body = snap::raw::max_compress_len(MAX_UNCOMPRESSED_CHUNK);
        for bad_len in [0usize, max_body + 1] {
            let mut bad = wire.clone();
            let k = varint_len(bad_len);
            let start = MAX_VARINT_LEN - k;
            let mut hdr = [0u8; MAX_VARINT_LEN];
            write_varint(&mut hdr[start..], bad_len);
            bad.extend_from_slice(&hdr[start..]);

            let mut r = SliceSrc::new(bad, usize::MAX);
            let mut got = VecSink::default();
            decompress_pump(&mut r, &mut got).await;
            assert_eq!(
                got.0, payload,
                "bad_len={bad_len}:前序合法帧必须完整写出,坏帧不得产出字节"
            );
        }
    }

    // ---- 新增:分配次数不随块数线性增长 ----------------------------------

    /// 压侧:1 块 vs 33 块,多出的块摊到的分配数必须 <1。
    /// 旧实现每块 2 次(comp 缓冲 + wire 缓冲),这里会直接失败。
    #[tokio::test]
    async fn compress_pump_allocs_do_not_scale_with_chunks() {
        // 计数只包住 pump 本身:buf/wire/Encoder 的分配是每连接一次的
        // 固定成本,随块数增长的部分才是要盯的可变成本。
        let same_alloc = |data: Vec<u8>| {
            let mut src = SliceSrc::new(data, MAX_UNCOMPRESSED_CHUNK);
            let mut dst = NullSink;
            let mut fut = Box::pin(compress_pump(&mut src, &mut dst));
            let ((), n) = count_allocs(|| futures::executor::block_on(&mut fut));
            n
        };
        let one = same_alloc(pseudo_random(MAX_UNCOMPRESSED_CHUNK, 0x1111));
        let many = same_alloc(pseudo_random(MAX_UNCOMPRESSED_CHUNK * 33, 0x2222));
        let per_extra = (many.saturating_sub(one)) as f64 / 32.0;
        assert!(
            per_extra < 1.0,
            "压侧每块新增 {per_extra:.2} 次分配(1 块={one},33 块={many}),缓冲复用退化"
        );
    }

    /// 解侧:同上。旧实现每块 3 次(comp、decompress_vec 输出、内部 vec)。
    #[tokio::test]
    async fn decompress_pump_allocs_do_not_scale_with_chunks() {
        let wire_for = |chunks: usize, seed: u64| {
            let data = pseudo_random(MAX_UNCOMPRESSED_CHUNK * chunks, seed);
            let mut sink = VecSink::default();
            let mut src = SliceSrc::new(data, MAX_UNCOMPRESSED_CHUNK);
            futures::executor::block_on(compress_pump(&mut src, &mut sink));
            sink.0
        };
        let run = |wire: Vec<u8>| {
            let mut r = tokio::io::BufReader::new(SliceSrc::new(wire, 8192));
            let mut dst = NullSink;
            let mut fut = Box::pin(decompress_pump(&mut r, &mut dst));
            let ((), n) = count_allocs(|| futures::executor::block_on(&mut fut));
            n
        };
        let one = run(wire_for(1, 0x3333));
        let many = run(wire_for(33, 0x4444));
        let per_extra = (many.saturating_sub(one)) as f64 / 32.0;
        assert!(
            per_extra < 1.0,
            "解侧每块新增 {per_extra:.2} 次分配(1 块={one},33 块={many}),缓冲复用退化"
        );
    }

    // ---- 新增:线格式回归 -----------------------------------------------

    /// 首字节必须是 varint 长度,snappy 体长度与之相等,且解回来一致。
    /// 另测「大块 >64KiB 的解压上限校验」仍然生效。
    #[tokio::test]
    async fn wire_format_is_varint_plus_snappy() {
        let chunk_data = pseudo_random(MAX_UNCOMPRESSED_CHUNK, 0xDEAD_BEEF_CAFE_0001);
        let mut sink = VecSink::default();
        let mut src = SliceSrc::new(chunk_data.clone(), MAX_UNCOMPRESSED_CHUNK);
        futures::executor::block_on(compress_pump(&mut src, &mut sink));
        let wire = sink.0;

        let mut buf: &[u8] = &wire;
        let mut frames = 0usize;
        while !buf.is_empty() {
            let (len, used) = {
                let mut len = 0usize;
                let mut shift = 0u32;
                let mut used = 0usize;
                loop {
                    let b = buf[used];
                    used += 1;
                    len |= ((b & 0x7f) as usize) << shift;
                    if b & 0x80 == 0 {
                        break;
                    }
                    shift += 7;
                    assert!(used < MAX_VARINT_LEN, "varint 超过 5 字节");
                }
                (len, used)
            };
            buf = &buf[used..];
            assert!(len > 0);
            assert!(len <= snap::raw::max_compress_len(MAX_UNCOMPRESSED_CHUNK));
            let body = &buf[..len];
            buf = &buf[len..];
            assert_eq!(decode_frame(body).unwrap(), chunk_data);
            frames += 1;
        }
        assert_eq!(frames, 1, "64KiB 输入应当只有一帧");
    }

    /// MAX_DECOMPRESSED_CHUNK 校验仍在:snappy 头里自称解压后 >4MiB,
    /// 帧体本身很小(能过「单块压缩长 ≤ max_compress_len(64KiB)」这一关),
    /// pump 必须在这里拒绝退出,绝不能把超限数据写出去。
    #[tokio::test]
    async fn oversized_decompressed_chunk_is_rejected() {
        // snappy raw 头 = 未压缩长度 varint;伪造一个 4MiB+ 的声明。
        let mut body = Vec::new();
        let mut hdr = [0u8; 10];
        let mut v = MAX_DECOMPRESSED_CHUNK as u64 + 1;
        let mut i = 0;
        while v >= 0x80 {
            hdr[i] = ((v & 0x7f) as u8) | 0x80;
            v >>= 7;
            i += 1;
        }
        hdr[i] = v as u8;
        body.extend_from_slice(&hdr[..=i]);
        // 体里塞一个合法的 1 字节 literal tag + 1 字节,保证帧体非空且长度合法。
        body.extend_from_slice(&[0x00, 0x41]);
        assert!(body.len() <= snap::raw::max_compress_len(MAX_UNCOMPRESSED_CHUNK));

        let mut wire = vec![0u8; MAX_VARINT_LEN];
        let k = varint_len(body.len());
        let start = MAX_VARINT_LEN - k;
        write_varint(&mut wire[start..], body.len());
        wire.truncate(MAX_VARINT_LEN);
        wire.drain(..start);
        wire.extend_from_slice(&body);

        let mut r = tokio::io::BufReader::new(SliceSrc::new(wire, 4096));
        let mut sink = VecSink::default();
        decompress_pump(&mut r, &mut sink).await;
        assert_eq!(sink.0.len(), 0, "超过 4MiB 上限的帧不得写出任何字节");
    }

    // ---- 新增:mux 内层无锁拆分(round-69 扩展)--------------------------

    /// compress_stream 对 RhMuxStream 内层必须命中 split_boxed_io
    /// (use_compression=true 且 multiplex=true 的组合),不得回落
    /// `tokio::io::split` 的同步锁;且拆半后压缩数据面仍双向可用。
    /// 修前该组合下 compress_pump 与 decompress_pump 抢同一把锁
    /// (round-69 futex 病灶在压缩路径上的具体形态)。
    #[tokio::test]
    async fn mux_inner_splits_lockfree_and_roundtrips() {
        use crate::{wrap_stream_as_client, wrap_stream_as_server, MUX_STREAMS_UNLIMITED};

        let (a, b) = tokio::io::duplex(256 * 1024);
        let server = wrap_stream_as_server(Box::new(b), true, MUX_STREAMS_UNLIMITED);
        let client = wrap_stream_as_client(Box::new(a), true, MUX_STREAMS_UNLIMITED);

        // 对端:accept 出的 mux 流原样回显(压缩字节层面的 echo)
        let srv = tokio::spawn(async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let mut s = server.accept().await.unwrap();
            let mut buf = [0u8; 4096];
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

        let inner: crate::BoxedIo = client.open_stream().await.unwrap();
        let before = SPLIT_FALLBACKS.with(|c| c.get());
        let mut s = compress_stream(inner).await;
        assert_eq!(
            SPLIT_FALLBACKS.with(|c| c.get()),
            before,
            "RhMuxStream 内层必须命中 split_boxed_io,不得回落 tokio::io::split"
        );

        // 拆半后的数据面仍双向可用:用户写 → 压缩 → mux 帧 → echo →
        // 解压 → 用户读
        s.write_all(b"mux+compress").await.unwrap();
        let mut back = [0u8; 12];
        tokio::time::timeout(std::time::Duration::from_secs(5), s.read_exact(&mut back))
            .await
            .expect("echo deadline")
            .unwrap();
        assert_eq!(&back, b"mux+compress");
        drop(s);
        let _ = srv.await;
    }

    /// P0-2 / P0-5:PrependedIo 包装的 RhMuxStream 经 compress_stream 必须命中
    /// split_boxed_io(SPLIT_FALLBACKS 不增加),且前缀与后续压缩帧按序完整交付。
    #[tokio::test]
    async fn prepended_mux_inner_splits_lockfree_and_roundtrips() {
        use crate::{
            wrap_stream_as_client, wrap_stream_as_server, PrependedIo, MUX_STREAMS_UNLIMITED,
        };

        let (a, b) = tokio::io::duplex(256 * 1024);
        let server = wrap_stream_as_server(Box::new(b), true, MUX_STREAMS_UNLIMITED);
        let client = wrap_stream_as_client(Box::new(a), true, MUX_STREAMS_UNLIMITED);

        // 先构造一段合法 snappy 压缩帧作为 PrependedIo 前缀(模拟同批读入残留)
        let prefix_plain = b"hello-prefix-";
        let mut enc = snap::raw::Encoder::new();
        let compressed = enc.compress_vec(prefix_plain).unwrap();
        let mut framed_prefix = Vec::new();
        let mut v = compressed.len();
        while v >= 0x80 {
            framed_prefix.push(((v & 0x7f) as u8) | 0x80);
            v >>= 7;
        }
        framed_prefix.push(v as u8);
        framed_prefix.extend_from_slice(&compressed);

        let srv = tokio::spawn(async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let mut s = server.accept().await.unwrap();
            let mut buf = [0u8; 4096];
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

        let inner: crate::BoxedIo =
            PrependedIo::wrap(client.open_stream().await.unwrap(), framed_prefix);
        let before = SPLIT_FALLBACKS.with(|c| c.get());
        let mut s = compress_stream(inner).await;
        assert_eq!(
            SPLIT_FALLBACKS.with(|c| c.get()),
            before,
            "PrependedIo(RhMuxStream) 内层必须命中 split_boxed_io,不得回落 tokio::io::split"
        );

        s.write_all(b"tail").await.unwrap();
        let mut back = [0u8; 17];
        tokio::time::timeout(std::time::Duration::from_secs(5), s.read_exact(&mut back))
            .await
            .expect("echo deadline")
            .unwrap();
        assert_eq!(&back, b"hello-prefix-tail");
        drop(s);
        let _ = srv.await;
    }

    // ---- 新增:JoinDuplex 向量写透传 --------------------------------------

    // 回归:JoinDuplex 修前只覆写标量 poll_write,向量写落到 tokio 默认
    // 实现(每次只写第一个非空段),compress 流用户侧的批量 writev 退化
    // 成逐段标量写。两层断言:公开 API 能力位为 true(透过 BoxedIo 的
    // 转发层);绕开 pump 直接构造 JoinDuplex,单次向量写带走全部段、
    // 字节按段序落在 pump 侧半端。
    #[tokio::test]
    async fn join_duplex_forwards_vectored_write() {
        use std::io::IoSlice;
        use tokio::io::AsyncReadExt;

        // 公开 API:compress_stream 产物必须如实上报向量写能力
        let (net_a, net_b) = tokio::io::duplex(1024);
        let s = compress_stream(Box::new(net_a)).await;
        assert!(
            AsyncWrite::is_write_vectored(&s),
            "compress 流用户侧必须透传 DuplexStream 的向量写能力位"
        );
        drop(s);
        drop(net_b);

        // 单元级:与 compress_stream 同构的两条单向 duplex,不经 pump
        let (pump_r, user_w) = tokio::io::duplex(64 * 1024);
        let (user_r, _pump_w) = tokio::io::duplex(64 * 1024);
        let mut j = JoinDuplex {
            r: user_r,
            w: user_w,
        };

        let a = [0x33u8; 8];
        let b = [0x44u8; 4096];
        let noop = std::task::Waker::noop();
        let mut cx = Context::from_waker(noop);
        // 4104B 远小于 duplex 缓冲(64KiB),单次向量写必然全量成功;
        // 若退化为「只写第一个非空段」,这里只会拿到 8。
        let n = match Pin::new(&mut j)
            .poll_write_vectored(&mut cx, &[IoSlice::new(&a), IoSlice::new(&b)])
        {
            Poll::Ready(Ok(n)) => n,
            other => panic!("unexpected poll result: {other:?}"),
        };
        assert_eq!(n, a.len() + b.len(), "一次向量写应带走全部段");

        let mut pump_r = pump_r;
        let mut got = vec![0u8; a.len() + b.len()];
        pump_r.read_exact(&mut got).await.unwrap();
        assert_eq!(&got[..a.len()], &a, "首段字节必须先行");
        assert_eq!(&got[a.len()..], &b, "次段字节必须按段序跟上");
    }
}
