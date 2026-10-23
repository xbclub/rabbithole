//! snappy 流压缩(代理级 use_compression)。
//!
//! 实现:pump 任务 + duplex 管道 —— 用户侧拿到一条普通字节流,
//! 两个后台任务分别负责「压缩写」与「解压读」,压缩逻辑全部是
//! async fn 循环(read_exact/write_all),不手写任何 poll 状态机。
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
//! * 解侧:`comp`(压缩帧)、`out`(解压结果)、`Decoder` 全部复用;varint
//!   一次 `read` 取回已到达的字节在本地解析,不再逐字节 `read_exact`
//!   (旧实现最坏 5 次独立 await,每次都可能在 reactor 上让出)。
//!
//! 稳态每块:压侧 0 次堆分配 + 1 次写 await;解侧 0 次堆分配 + 2 次读 await
//! (varint 1 次、帧体 1 次)。改前是 2~3 次分配 + 1 次写、3 次分配 + 6 次读。

use crate::BoxedIo;
use bytes::{BufMut, BytesMut};
use std::io;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const MAX_UNCOMPRESSED_CHUNK: usize = 64 * 1024;
const MAX_DECOMPRESSED_CHUNK: usize = 4 * 1024 * 1024;
/// varint 长度前缀的字节上限(协议约定,见模块文档)。
const MAX_VARINT_LEN: usize = 5;
/// 跨帧回退区容量:至多 `MAX_VARINT_LEN - 1` 个帧体前缀 + 一次补读。
const STASH_CAP: usize = MAX_VARINT_LEN * 2;

/// 单块压缩帧的缓冲上限:5 字节 varint + snappy 最坏膨胀。
fn max_frame_len() -> usize {
    MAX_VARINT_LEN + snap::raw::max_compress_len(MAX_UNCOMPRESSED_CHUNK)
}

/// 把 `inner` 包一层端到端 snappy 压缩,返回面向调用方的普通字节流。
/// 丢弃返回值时 pump 任务随之结束并释放 inner。
pub async fn compress_stream(inner: BoxedIo) -> BoxedIo {
    let (user_side, pump_side) = tokio::io::duplex(64 * 1024);
    // DuplexStream 无 into_split,保持现状。
    let (pump_r, pump_w) = tokio::io::split(pump_side);
    // B4:底层是裸 TcpStream(multiplex=false 的 tcp 载体)时优先无锁
    // 拆半(into_split 两个独立 fd);修前无条件 `tokio::io::split`,
    // 压缩写泵与解压读泵共享一把 std::sync::Mutex,与 A2 同机理
    //(round-69 实测 futex 占 75% syscall 时间)。包装类型照旧回落。
    let (inner_r, inner_w): (
        Box<dyn tokio::io::AsyncRead + Send + Unpin>,
        Box<dyn tokio::io::AsyncWrite + Send + Unpin>,
    ) = match crate::split_boxed_io(inner) {
        Ok((r, w)) => (Box::new(r), Box::new(w)),
        Err(io) => {
            let (r, w) = tokio::io::split(io);
            (Box::new(r), Box::new(w))
        }
    };
    tokio::spawn(compress_pump(pump_r, inner_w));
    tokio::spawn(decompress_pump(inner_r, pump_w));
    Box::new(user_side)
}

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

/// 跨帧保留的小回退区:装「读过 varint 之后、本帧还没用掉」的字节。
///
/// 一次 `read` 能取回的字节数由内核决定,可能比 varint 多。多出来的属于
/// 帧体甚至下一帧的 varint,必须存住,否则流会错位。
struct Stash {
    buf: [u8; STASH_CAP],
    len: usize,
}

impl Stash {
    fn new() -> Self {
        Self {
            buf: [0u8; STASH_CAP],
            len: 0,
        }
    }
}

/// 读一帧的长度前缀。`stash` 里已有的字节优先参与解析,读进来但用不完的
/// 也留在 `stash` 里供下一帧或本帧帧体取用。
///
/// 返回 `Some(帧长)`;`None` 表示 EOF 或坏帧。
///
/// 之所以不是字面意义的 `read_exact(&mut [0u8; 5])`:varint 绝大多数只有
/// 1~3 字节,一次读满 5 字节必然多读进本帧帧体的开头几位(`read_exact`
/// 在流尾不足 5 字节时还会直接报 UnexpectedEof)。这里用一次 `read`
/// 取回「已到达的」字节,解析出 varint 后把多读部分留在 `stash`,
/// 由调用方先取帧体前缀再补读剩余部分 —— 既只花一次 await,
/// 又不丢字节、不额外复制帧体。
async fn read_frame_len<R>(src: &mut R, stash: &mut Stash) -> Option<usize>
where
    R: AsyncReadExt + Unpin,
{
    loop {
        if let Some((v, used)) = parse_varint(&stash.buf[..stash.len]) {
            // 前移剩余字节,保持 `buf[..len]` 始终是「还没消费的数据」。
            stash.buf.copy_within(used..stash.len, 0);
            stash.len -= used;
            return Some(v);
        }
        if stash.len >= MAX_VARINT_LEN {
            return None; // 第 5 字节仍带 continuation:非法长度前缀
        }
        match src.read(&mut stash.buf[stash.len..]).await {
            Ok(0) => return None, // EOF:正常结束;半截 = 上层连接错误
            Ok(n) => stash.len += n,
            Err(_) => return None,
        }
    }
}

async fn decompress_pump<R, W>(mut src: R, mut dst: W)
where
    R: AsyncReadExt + Unpin,
    W: AsyncWriteExt + Unpin,
{
    // 逐帧复用:压缩帧缓冲、解压输出缓冲、Decoder 都只建一次。
    // comp 需容纳「帧体 + 与之同时到达的 varint」,故比单块上限多留几个字节。
    let mut comp = vec![0u8; MAX_VARINT_LEN + snap::raw::max_compress_len(MAX_UNCOMPRESSED_CHUNK)];
    let mut out = vec![0u8; MAX_UNCOMPRESSED_CHUNK];
    let mut dec = snap::raw::Decoder::new();
    let mut stash = Stash::new();
    loop {
        let len = match read_frame_len(&mut src, &mut stash).await {
            Some(v) => v,
            None => break,
        };
        if len == 0 || len > snap::raw::max_compress_len(MAX_UNCOMPRESSED_CHUNK) {
            break;
        }
        // 回退区里可能已有本帧帧体的开头几个字节,先搬进 comp 再补读剩余。
        let carried = stash.len.min(len);
        if carried > 0 {
            comp[..carried].copy_from_slice(&stash.buf[..carried]);
            stash.buf.copy_within(carried..stash.len, 0);
            stash.len -= carried;
        }
        if src.read_exact(&mut comp[carried..len]).await.is_err() {
            break;
        }
        let body = &comp[..len];
        let expected = match snap::raw::decompress_len(body) {
            Ok(e) => e,
            Err(_) => break,
        };
        if expected > MAX_DECOMPRESSED_CHUNK {
            break;
        }
        // 常态 ≤64KiB,永远走不到扩容;只有对端发来 >64KiB 的单块才会长一次。
        if out.len() < expected {
            out.resize(expected, 0);
        }
        match dec.decompress(body, &mut out[..expected]) {
            Ok(n) => {
                if dst.write_all(&out[..n]).await.is_err() {
                    break;
                }
            }
            Err(_) => break,
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
}
