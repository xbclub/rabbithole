//! 通用批量字节拷贝泵(P3):ReadBuf 直读(单块 ≤64KiB 红线)+ 有界分片共享池按需借还 +
//! writev 聚合 + 写期间持续预取 + 取消安全限速预留。

use bytes::{BufMut as _, BytesMut};
use std::collections::VecDeque;
use std::io::{self, IoSlice};
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::Poll;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt as _, ReadBuf};

/// 单次读入字节上限(64KiB 红线,不得放宽)。
pub const COPY_CHUNK_CAP: usize = 64 * 1024;

/// 通用 copy 泵单批聚合成段上限。
pub const COPY_BATCH_SEGS: usize = 64;

const DEFAULT_COPY_BATCH_BYTES: usize = 256 * 1024;
const MIN_COPY_BATCH_BYTES: usize = 1024;
const MAX_COPY_BATCH_BYTES: usize = 1024 * 1024;

const COPY_POOL_SHARDS: usize = 8;
const COPY_POOL_SHARD_CAP: usize = 32;
const COPY_POOL_MAX_IDLE: usize = COPY_POOL_SHARDS * COPY_POOL_SHARD_CAP;
const COPY_POOL_CLASS_MAX: usize = COPY_CHUNK_CAP + 8 * 1024;

// 全局空闲池硬上界:256 × 64KiB = 16 MiB,杜绝高并发空闲连接长驻放大 RSS。
const _: () = assert!(COPY_POOL_MAX_IDLE * COPY_CHUNK_CAP <= 16 * 1024 * 1024);

static NEXT_SHARD: AtomicUsize = AtomicUsize::new(0);

thread_local! {
    static HOME_SHARD: usize = NEXT_SHARD.fetch_add(1, Ordering::Relaxed) & (COPY_POOL_SHARDS - 1);
}

fn home_shard() -> usize {
    HOME_SHARD.with(|&idx| idx)
}

struct CopyBufPool {
    shards: [Mutex<Vec<BytesMut>>; COPY_POOL_SHARDS],
    #[cfg(test)]
    allocations: AtomicUsize,
    #[cfg(test)]
    live: AtomicUsize,
}

impl CopyBufPool {
    const fn new() -> Self {
        Self {
            shards: [
                Mutex::new(Vec::new()),
                Mutex::new(Vec::new()),
                Mutex::new(Vec::new()),
                Mutex::new(Vec::new()),
                Mutex::new(Vec::new()),
                Mutex::new(Vec::new()),
                Mutex::new(Vec::new()),
                Mutex::new(Vec::new()),
            ],
            #[cfg(test)]
            allocations: AtomicUsize::new(0),
            #[cfg(test)]
            live: AtomicUsize::new(0),
        }
    }

    fn get(&self) -> PooledBuf<'_> {
        let start = home_shard();
        for i in 0..COPY_POOL_SHARDS {
            let idx = (start + i) & (COPY_POOL_SHARDS - 1);
            let popped = {
                let mut g = self.shards[idx].lock().unwrap_or_else(|e| e.into_inner());
                g.pop()
            };
            if let Some(mut bm) = popped {
                bm.clear();
                #[cfg(test)]
                self.live.fetch_add(1, Ordering::Relaxed);
                return PooledBuf {
                    pool: self,
                    buf: Some(bm),
                };
            }
        }
        #[cfg(test)]
        {
            self.allocations.fetch_add(1, Ordering::Relaxed);
            self.live.fetch_add(1, Ordering::Relaxed);
        }
        PooledBuf {
            pool: self,
            buf: Some(BytesMut::with_capacity(COPY_CHUNK_CAP)),
        }
    }

    fn recycle(&self, bm: BytesMut) {
        #[cfg(test)]
        self.live.fetch_sub(1, Ordering::Relaxed);
        self.put_raw(bm);
    }

    fn put_raw(&self, mut bm: BytesMut) {
        bm.clear();
        if !(COPY_CHUNK_CAP..=COPY_POOL_CLASS_MAX).contains(&bm.capacity()) {
            return;
        }
        let start = home_shard();
        for i in 0..COPY_POOL_SHARDS {
            let idx = (start + i) & (COPY_POOL_SHARDS - 1);
            let mut g = self.shards[idx].lock().unwrap_or_else(|e| e.into_inner());
            if g.len() < COPY_POOL_SHARD_CAP {
                g.push(bm);
                return;
            }
        }
    }

    #[cfg(test)]
    fn idle_count(&self) -> usize {
        self.shards
            .iter()
            .map(|s| s.lock().unwrap_or_else(|e| e.into_inner()).len())
            .sum()
    }

    #[cfg(test)]
    fn live_count(&self) -> usize {
        self.live.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    fn allocations(&self) -> usize {
        self.allocations.load(Ordering::Relaxed)
    }
}

static COPY_POOL: CopyBufPool = CopyBufPool::new();

struct PooledBuf<'a> {
    pool: &'a CopyBufPool,
    buf: Option<BytesMut>,
}

impl PooledBuf<'_> {
    fn as_slice(&self) -> &[u8] {
        self.buf.as_deref().unwrap_or(&[])
    }

    fn len(&self) -> usize {
        self.buf.as_ref().map_or(0, BytesMut::len)
    }

    fn buf_mut(&mut self) -> &mut BytesMut {
        self.buf.as_mut().expect("PooledBuf accessed after Drop")
    }
}

impl Drop for PooledBuf<'_> {
    fn drop(&mut self) {
        if let Some(bm) = self.buf.take() {
            self.pool.recycle(bm);
        }
    }
}

fn parse_copy_batch_bytes(val: Option<&str>) -> usize {
    val.and_then(|s| s.trim().parse::<usize>().ok())
        .map(|n| n.clamp(MIN_COPY_BATCH_BYTES, MAX_COPY_BATCH_BYTES))
        .unwrap_or(DEFAULT_COPY_BATCH_BYTES)
}

/// 单批收集字节上限(`RH_COPY_BATCH_BYTES` 可覆盖,进程内缓存一次;
/// 默认 256KiB,钳制在 `1024..=1MiB`)。
pub fn copy_batch_bytes() -> usize {
    static V: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *V.get_or_init(|| parse_copy_batch_bytes(std::env::var("RH_COPY_BATCH_BYTES").ok().as_deref()))
}

enum TryChunk<'a> {
    Data(PooledBuf<'a>),
    Eof,
    Empty,
}

/// 真实 waker 非阻塞读:按需借块并在 `Poll::Pending`/EOF/错误 时立刻还回全局池。
async fn try_read_chunk<'a, R: AsyncRead + Unpin>(
    pool: &'a CopyBufPool,
    r: &mut R,
) -> io::Result<TryChunk<'a>> {
    std::future::poll_fn(|cx| {
        let mut bm = pool.get();
        let spare = bm.buf_mut().spare_capacity_mut();
        let limit = spare.len().min(COPY_CHUNK_CAP);
        let mut rb = ReadBuf::uninit(&mut spare[..limit]);
        match Pin::new(&mut *r).poll_read(cx, &mut rb) {
            Poll::Ready(Ok(())) => {
                let n = rb.filled().len();
                if n == 0 {
                    return Poll::Ready(Ok(TryChunk::Eof));
                }
                // SAFETY: poll_read 只把已写入的前 n 字节标为 filled,且 n <= limit <= spare.len()。
                unsafe { bm.buf_mut().advance_mut(n) };
                Poll::Ready(Ok(TryChunk::Data(bm)))
            }
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Pending => Poll::Ready(Ok(TryChunk::Empty)),
        }
    })
    .await
}

/// 阻塞读一块:`Ok(None)` = EOF。在 `poll_fn` 内按需从共享池借用并在 `Poll::Pending`
/// 挂起前立刻归还,确保空闲或被取消的连接长期驻留缓冲为 0。
async fn read_chunk<'a, R: AsyncRead + Unpin>(
    pool: &'a CopyBufPool,
    r: &mut R,
) -> io::Result<Option<PooledBuf<'a>>> {
    std::future::poll_fn(|cx| {
        let mut bm = pool.get();
        let spare = bm.buf_mut().spare_capacity_mut();
        let limit = spare.len().min(COPY_CHUNK_CAP);
        let mut rb = ReadBuf::uninit(&mut spare[..limit]);
        match Pin::new(&mut *r).poll_read(cx, &mut rb) {
            Poll::Ready(Ok(())) => {
                let n = rb.filled().len();
                if n == 0 {
                    return Poll::Ready(Ok(None));
                }
                // SAFETY: poll_read 只把已写入的前 n 字节标为 filled,且 n <= limit <= spare.len()。
                unsafe { bm.buf_mut().advance_mut(n) };
                Poll::Ready(Ok(Some(bm)))
            }
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Pending => Poll::Pending,
        }
    })
    .await
}

async fn write_vectored_batch<W: AsyncWrite + Unpin>(
    w: &mut W,
    chunks: &[PooledBuf<'_>],
    limiter: &Option<Arc<dyn crate::ChunkLimiter>>,
) -> io::Result<usize> {
    let total: usize = chunks.iter().map(PooledBuf::len).sum();
    debug_assert!(total > 0, "caller flushes only non-empty batches");
    let mut res = match limiter {
        Some(l) => Some(l.reserve(total).await),
        None => None,
    };
    let mut seg = 0usize;
    let mut off = 0usize;
    let mut written = 0usize;
    while written < total {
        let mut slices = [IoSlice::new(&[]); COPY_BATCH_SEGS];
        let mut nsegs = 0usize;
        for (i, c) in chunks[seg..].iter().enumerate() {
            if nsegs == COPY_BATCH_SEGS {
                break;
            }
            let raw = c.as_slice();
            let s = if i == 0 { &raw[off..] } else { raw };
            if !s.is_empty() {
                slices[nsegs] = IoSlice::new(s);
                nsegs += 1;
            }
        }
        let k =
            std::future::poll_fn(|cx| Pin::new(&mut *w).poll_write_vectored(cx, &slices[..nsegs]))
                .await?;
        if k == 0 {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "write_vectored returned 0",
            ));
        }
        if k > total - written {
            return Err(io::Error::other(format!(
                "write_vectored count {k} exceeds remaining {}",
                total - written
            )));
        }
        written += k;
        if let Some(r) = res.as_mut() {
            r.consume(k);
        }
        let mut rem = k;
        while rem > 0 {
            let left = chunks[seg].len() - off;
            if rem >= left {
                seg += 1;
                off = 0;
                rem -= left;
            } else {
                off += rem;
                rem = 0;
            }
        }
    }
    Ok(written)
}

/// 通用批量 copy 泵:ReadBuf 直读(单块 ≤ 64KiB 红线)+ 有界共享池按需借还 +
/// `poll_write_vectored` 批量聚合写出 + 写期间持续预取 + 取消安全限速预留。
///
/// clean EOF 时刷新写端并返回 `Ok(total_bytes)`(不自动 `shutdown`,由上层
/// 骨架按 clean EOF / 取消语义收尾);读或写失败立即返回 `Err`(写失败时
/// 立即退出而不 join 读)。
pub async fn copy_batched<R: AsyncRead + Unpin, W: AsyncWrite + Unpin>(
    r: &mut R,
    w: &mut W,
    limiter: Option<Arc<dyn crate::ChunkLimiter>>,
) -> io::Result<u64> {
    copy_batched_with_pool(&COPY_POOL, r, w, limiter).await
}

async fn copy_batched_with_pool<R: AsyncRead + Unpin, W: AsyncWrite + Unpin>(
    pool: &CopyBufPool,
    r: &mut R,
    w: &mut W,
    limiter: Option<Arc<dyn crate::ChunkLimiter>>,
) -> io::Result<u64> {
    let batch_limit = copy_batch_bytes();
    let has_limiter = limiter.is_some();
    let max_segs = if has_limiter { 1 } else { COPY_BATCH_SEGS };

    let mut pending: VecDeque<PooledBuf<'_>> = VecDeque::new();
    let mut pending_bytes: usize = 0;
    let mut batch: Vec<PooledBuf<'_>> = Vec::new();
    let mut prefetched: VecDeque<PooledBuf<'_>> = VecDeque::new();
    let mut eof_seen = false;
    let mut read_err: Option<io::Error> = None;
    let mut total_copied: u64 = 0;

    loop {
        batch.clear();
        let mut batch_bytes = 0usize;

        while let Some(c) = pending.pop_front() {
            if batch_bytes >= batch_limit || batch.len() >= max_segs {
                pending.push_front(c);
                break;
            }
            pending_bytes = pending_bytes.saturating_sub(c.len());
            batch_bytes += c.len();
            batch.push(c);
        }

        if batch.is_empty() {
            if let Some(e) = read_err.take() {
                return Err(e);
            }
            if eof_seen {
                w.flush().await?;
                return Ok(total_copied);
            }
            match read_chunk(pool, r).await? {
                Some(c) => {
                    batch_bytes += c.len();
                    batch.push(c);
                }
                None => {
                    w.flush().await?;
                    return Ok(total_copied);
                }
            }
        }

        while !eof_seen && batch_bytes < batch_limit && batch.len() < max_segs {
            match try_read_chunk(pool, r).await {
                Ok(TryChunk::Data(c)) => {
                    batch_bytes += c.len();
                    batch.push(c);
                }
                Ok(TryChunk::Eof) => {
                    eof_seen = true;
                    break;
                }
                Ok(TryChunk::Empty) => break,
                Err(e) => {
                    read_err = Some(e);
                    break;
                }
            }
        }

        prefetched.clear();
        let mut prefetched_bytes = 0usize;
        let write_res =
            if !has_limiter && batch_bytes < batch_limit && !eof_seen && read_err.is_none() {
                let write_fut = write_vectored_batch(w, &batch, &limiter);
                futures::pin_mut!(write_fut);
                loop {
                    if pending_bytes + prefetched_bytes >= batch_limit
                        || pending.len() + prefetched.len() >= COPY_BATCH_SEGS
                    {
                        break write_fut.await;
                    }
                    let read_fut = read_chunk(pool, r);
                    futures::pin_mut!(read_fut);
                    // 优先推进 write_fut:快速写出在首轮即返回,无需触碰读半与缓冲池。
                    match futures::future::select(write_fut.as_mut(), read_fut).await {
                        futures::future::Either::Left((res, _)) => break res,
                        futures::future::Either::Right((nr, _)) => match nr {
                            Ok(Some(c)) => {
                                prefetched_bytes += c.len();
                                prefetched.push_back(c);
                            }
                            Ok(None) => {
                                eof_seen = true;
                                break write_fut.await;
                            }
                            Err(e) => {
                                read_err = Some(e);
                                break write_fut.await;
                            }
                        },
                    }
                }
            } else {
                write_vectored_batch(w, &batch, &limiter).await
            };

        let written = write_res?;
        total_copied += written as u64;

        batch.clear();
        pending_bytes += prefetched_bytes;
        pending.append(&mut prefetched);

        if let Some(e) = read_err.take() {
            return Err(e);
        }
        if eof_seen && pending.is_empty() {
            w.flush().await?;
            return Ok(total_copied);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{compress_stream, split_boxed_io, ChunkLimiter, ChunkReservation, PrependedIo};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[tokio::test]
    async fn split_compress_stream_lockless() {
        let (net_a, net_b) = tokio::io::duplex(256 * 1024);
        let comp_a = compress_stream(Box::new(net_a)).await;
        let comp_b = compress_stream(Box::new(net_b)).await;

        let (mut ra, mut wa) = match split_boxed_io(comp_a) {
            Ok(pair) => pair,
            Err(_) => panic!("JoinDuplex 必须命中 split_boxed_io 无锁拆半"),
        };
        let (mut rb, mut wb) = match split_boxed_io(comp_b) {
            Ok(pair) => pair,
            Err(_) => panic!("JoinDuplex 必须命中 split_boxed_io 无锁拆半"),
        };
        assert!(
            wa.is_write_vectored(),
            "DuplexHalfWrite 必须透传 is_write_vectored"
        );
        assert!(
            wb.is_write_vectored(),
            "DuplexHalfWrite 必须透传 is_write_vectored"
        );

        // 读半写、写半读保持 Unsupported 单向契约
        assert_eq!(
            ra.write(b"x").await.unwrap_err().kind(),
            io::ErrorKind::Unsupported
        );
        let mut sink = [0u8; 1];
        assert_eq!(
            wa.read(&mut sink).await.unwrap_err().kind(),
            io::ErrorKind::Unsupported
        );

        // 向量写 + 双向全双工传输 + 半关闭
        let seg1 = vec![0x11u8; 32 * 1024];
        let seg2 = vec![0x22u8; 48 * 1024];
        let seg1_len = seg1.len();
        let seg2_len = seg2.len();
        let expected_ab_len = seg1_len + seg2_len;

        let reply_payload = vec![0x5Au8; 64 * 1024 + 17];
        let reply_expected = reply_payload.clone();

        let t_a = tokio::spawn(async move {
            let mut off = 0usize;
            while off < expected_ab_len {
                let s = if off < seg1.len() {
                    [IoSlice::new(&seg1[off..]), IoSlice::new(&seg2)]
                } else {
                    [IoSlice::new(&seg2[off - seg1.len()..]), IoSlice::new(&[])]
                };
                let n = std::future::poll_fn(|cx| Pin::new(&mut wa).poll_write_vectored(cx, &s))
                    .await
                    .unwrap();
                assert!(n > 0);
                off += n;
            }
            wa.shutdown().await.unwrap();
            let mut got = Vec::new();
            ra.read_to_end(&mut got).await.unwrap();
            got
        });

        let mut got_ab = Vec::new();
        rb.read_to_end(&mut got_ab).await.unwrap();
        assert_eq!(got_ab.len(), seg1_len + seg2_len);
        assert!(got_ab[..seg1_len].iter().all(|&b| b == 0x11));
        assert!(got_ab[seg1_len..].iter().all(|&b| b == 0x22));

        wb.write_all(&reply_payload).await.unwrap();
        wb.shutdown().await.unwrap();

        let got_ba = tokio::time::timeout(Duration::from_secs(5), t_a)
            .await
            .expect("split compress roundtrip timeout")
            .unwrap();
        assert_eq!(got_ba, reply_expected);

        // PrependedIo(JoinDuplex) 同样无锁拆半且前缀保序
        let (p_a, p_b) = tokio::io::duplex(64 * 1024);
        let c1 = compress_stream(Box::new(p_a)).await;
        let mut c2 = compress_stream(Box::new(p_b)).await;
        let wrapped = PrependedIo::wrap(c1, b"PRE_".to_vec());
        let (mut pr, mut pw) = split_boxed_io(wrapped)
            .ok()
            .expect("PrependedIo(JoinDuplex) 必须命中 split_boxed_io");
        c2.write_all(b"BODY").await.unwrap();
        c2.shutdown().await.unwrap();
        let mut out = Vec::new();
        pr.read_to_end(&mut out).await.unwrap();
        assert_eq!(&out, b"PRE_BODY");
        pw.write_all(b"ACK").await.unwrap();
        pw.shutdown().await.unwrap();
        let mut ack = Vec::new();
        c2.read_to_end(&mut ack).await.unwrap();
        assert_eq!(&ack, b"ACK");
    }

    struct MultiReadySrc {
        chunks: VecDeque<Vec<u8>>,
        max_buf_seen: Arc<AtomicUsize>,
    }

    impl AsyncRead for MultiReadySrc {
        fn poll_read(
            mut self: Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            self.max_buf_seen
                .fetch_max(buf.remaining(), Ordering::SeqCst);
            match self.chunks.pop_front() {
                Some(c) => {
                    let n = c.len().min(buf.remaining());
                    buf.put_slice(&c[..n]);
                    if n < c.len() {
                        self.chunks.push_front(c[n..].to_vec());
                    }
                    Poll::Ready(Ok(()))
                }
                None => Poll::Ready(Ok(())),
            }
        }
    }

    #[derive(Default)]
    struct VectoredRecorder {
        vectored_calls: usize,
        scalar_calls: usize,
        max_segs: usize,
        partial_cap: usize,
        fail_after_bytes: Option<usize>,
        written: Vec<u8>,
    }

    struct MockVectoredWriter(Arc<Mutex<VectoredRecorder>>);

    impl AsyncWrite for MockVectoredWriter {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            let mut st = self.0.lock().unwrap();
            st.scalar_calls += 1;
            st.written.extend_from_slice(buf);
            Poll::Ready(Ok(buf.len()))
        }

        fn poll_write_vectored(
            self: Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            bufs: &[IoSlice<'_>],
        ) -> Poll<io::Result<usize>> {
            let mut st = self.0.lock().unwrap();
            st.vectored_calls += 1;
            st.max_segs = st.max_segs.max(bufs.len());
            let offered: usize = bufs.iter().map(|s| s.len()).sum();
            let mut take = if st.partial_cap > 0 {
                offered.min(st.partial_cap)
            } else {
                offered
            };
            if let Some(limit) = st.fail_after_bytes {
                if st.written.len() >= limit {
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::BrokenPipe,
                        "injected write fault",
                    )));
                }
                take = take.min(limit - st.written.len());
            }
            let mut rem = take;
            for s in bufs {
                if rem == 0 {
                    break;
                }
                let k = s.len().min(rem);
                st.written.extend_from_slice(&s[..k]);
                rem -= k;
            }
            Poll::Ready(Ok(take))
        }

        fn is_write_vectored(&self) -> bool {
            true
        }

        fn poll_flush(
            self: Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(
            self: Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn copy_batched_coalesces_writev() {
        let chunks: Vec<Vec<u8>> = (0..8u8)
            .map(|i| vec![0x10 + i; 1024 + i as usize])
            .collect();
        let expected: Vec<u8> = chunks.iter().flatten().copied().collect();
        let max_seen = Arc::new(AtomicUsize::new(0));
        let mut src = MultiReadySrc {
            chunks: VecDeque::from(chunks),
            max_buf_seen: max_seen,
        };
        let rec = Arc::new(Mutex::new(VectoredRecorder {
            partial_cap: 1500, // 迫使部分写跨段推进
            ..Default::default()
        }));
        let mut dst = MockVectoredWriter(rec.clone());

        let copied = copy_batched(&mut src, &mut dst, None).await.unwrap();
        assert_eq!(copied, expected.len() as u64);

        let st = rec.lock().unwrap();
        assert_eq!(st.written, expected, "跨部分写推进后字节必须逐字节一致");
        assert_eq!(st.scalar_calls, 0, "不得退化为标量 poll_write");
        assert!(
            st.max_segs > 1,
            "多段就绪块必须聚合进单次 poll_write_vectored(max_segs={})",
            st.max_segs
        );
        assert!(st.vectored_calls > 1, "partial_cap 应触发多次续写推进");
    }

    #[tokio::test]
    async fn copy_batched_flushes_without_waiting() {
        let (mut src_tx, mut src_rx) = tokio::io::duplex(64 * 1024);
        let (mut dst_tx, mut dst_rx) = tokio::io::duplex(64 * 1024);

        let pump = tokio::spawn(async move { copy_batched(&mut src_rx, &mut dst_tx, None).await });

        // 只发 64B(远小于 256KiB 批上限)且不关闭写端:必须立即冲刷到对端
        src_tx
            .write_all(b"immediate-flush-64-bytes-payload")
            .await
            .unwrap();
        let mut got = [0u8; 32];
        tokio::time::timeout(Duration::from_secs(1), dst_rx.read_exact(&mut got))
            .await
            .expect("小包不得等满批或 EOF 才冲刷")
            .unwrap();
        assert_eq!(&got, b"immediate-flush-64-bytes-payload");

        drop(src_tx);
        let copied = tokio::time::timeout(Duration::from_secs(1), pump)
            .await
            .expect("EOF 收敛")
            .unwrap()
            .unwrap();
        assert_eq!(copied, 32);
    }

    struct CountingLimiter {
        charged: AtomicUsize,
        refunded: Arc<AtomicUsize>,
    }

    impl CountingLimiter {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                charged: AtomicUsize::new(0),
                refunded: Arc::new(AtomicUsize::new(0)),
            })
        }
        fn net_consumed(&self) -> usize {
            self.charged.load(Ordering::SeqCst) - self.refunded.load(Ordering::SeqCst)
        }
    }

    struct CountingReservation {
        remaining: usize,
        refunded: Arc<AtomicUsize>,
    }

    impl ChunkReservation for CountingReservation {
        fn consume(&mut self, n: usize) {
            self.remaining = self.remaining.saturating_sub(n);
        }
    }

    impl Drop for CountingReservation {
        fn drop(&mut self) {
            if self.remaining > 0 {
                self.refunded.fetch_add(self.remaining, Ordering::SeqCst);
            }
        }
    }

    impl ChunkLimiter for CountingLimiter {
        fn reserve(
            &self,
            n: usize,
        ) -> futures::future::BoxFuture<'static, Box<dyn ChunkReservation>> {
            self.charged.fetch_add(n, Ordering::SeqCst);
            let refunded = self.refunded.clone();
            Box::pin(async move {
                Box::new(CountingReservation {
                    remaining: n,
                    refunded,
                }) as Box<dyn ChunkReservation>
            })
        }
    }

    #[tokio::test]
    async fn copy_batched_metering_with_limiter() {
        // 1) 正常全量(含部分写):净扣费 == 实际写入 == 返回值
        let chunks = vec![vec![0xAAu8; 4096], vec![0xBBu8; 5000]];
        let expected_len: usize = chunks.iter().map(|c| c.len()).sum();
        let mut src = MultiReadySrc {
            chunks: VecDeque::from(chunks),
            max_buf_seen: Arc::new(AtomicUsize::new(0)),
        };
        let rec = Arc::new(Mutex::new(VectoredRecorder {
            partial_cap: 1300,
            ..Default::default()
        }));
        let mut dst = MockVectoredWriter(rec.clone());
        let lim = CountingLimiter::new();
        let copied = copy_batched(&mut src, &mut dst, Some(lim.clone()))
            .await
            .unwrap();
        assert_eq!(copied, expected_len as u64);
        assert_eq!(lim.net_consumed(), expected_len);
        assert_eq!(lim.refunded.load(Ordering::SeqCst), 0);

        // 2) 部分写后写端故障:未送达预留必须全额退还
        let mut src2 = MultiReadySrc {
            chunks: VecDeque::from(vec![vec![0xCCu8; 4096]]),
            max_buf_seen: Arc::new(AtomicUsize::new(0)),
        };
        let rec2 = Arc::new(Mutex::new(VectoredRecorder {
            partial_cap: 1000,
            fail_after_bytes: Some(1500),
            ..Default::default()
        }));
        let mut dst2 = MockVectoredWriter(rec2.clone());
        let lim2 = CountingLimiter::new();
        let err = copy_batched(&mut src2, &mut dst2, Some(lim2.clone()))
            .await
            .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::BrokenPipe);
        assert_eq!(rec2.lock().unwrap().written.len(), 1500);
        assert_eq!(
            lim2.net_consumed(),
            1500,
            "部分送达后故障必须按实际写入扣费、其余退还"
        );
        assert_eq!(lim2.refunded.load(Ordering::SeqCst), 4096 - 1500);
    }

    #[tokio::test]
    async fn copy_batched_chunk_cap_is_64kib() {
        assert_eq!(COPY_CHUNK_CAP, 64 * 1024, "单次读上限红线必须为 64KiB");

        let max_seen = Arc::new(AtomicUsize::new(0));
        // 单块 200KiB,分多次读且每轮不得向 poll_read 暴露 > 64KiB 的缓冲区
        let mut src = MultiReadySrc {
            chunks: VecDeque::from(vec![vec![0x55u8; 200 * 1024]]),
            max_buf_seen: max_seen.clone(),
        };
        let rec = Arc::new(Mutex::new(VectoredRecorder::default()));
        let mut dst = MockVectoredWriter(rec.clone());
        let copied = copy_batched(&mut src, &mut dst, None).await.unwrap();
        assert_eq!(copied, 200 * 1024);
        assert!(
            max_seen.load(Ordering::SeqCst) <= COPY_CHUNK_CAP,
            "单次读缓冲 {} 不得超过 COPY_CHUNK_CAP ({COPY_CHUNK_CAP})",
            max_seen.load(Ordering::SeqCst)
        );
        assert_eq!(
            max_seen.load(Ordering::SeqCst),
            COPY_CHUNK_CAP,
            "应充分利用 64KiB 读缓冲上限"
        );
    }

    #[test]
    fn copy_batch_bytes_env_bounds() {
        assert_eq!(parse_copy_batch_bytes(None), 256 * 1024);
        assert_eq!(parse_copy_batch_bytes(Some("")), 256 * 1024);
        assert_eq!(parse_copy_batch_bytes(Some("invalid")), 256 * 1024);
        assert_eq!(parse_copy_batch_bytes(Some("0")), 1024);
        assert_eq!(parse_copy_batch_bytes(Some("512")), 1024);
        assert_eq!(parse_copy_batch_bytes(Some("1024")), 1024);
        assert_eq!(parse_copy_batch_bytes(Some(" 524288 ")), 524_288);
        assert_eq!(parse_copy_batch_bytes(Some("1048576")), 1024 * 1024);
        assert_eq!(parse_copy_batch_bytes(Some("99999999")), 1024 * 1024);
        assert!((1024..=1024 * 1024).contains(&copy_batch_bytes()));
    }

    #[tokio::test]
    async fn copy_batched_idle_connections_retain_zero_buffers() {
        static POOL: CopyBufPool = CopyBufPool::new();
        const CONNS: usize = 512;

        let mut in_txs = Vec::with_capacity(CONNS);
        let mut out_rxs = Vec::with_capacity(CONNS);
        let mut tasks = Vec::with_capacity(CONNS);

        for _ in 0..CONNS {
            let (in_tx, mut in_rx) = tokio::io::duplex(4096);
            let (mut out_tx, out_rx) = tokio::io::duplex(4096);
            in_txs.push(in_tx);
            out_rxs.push(out_rx);
            tasks.push(tokio::spawn(async move {
                copy_batched_with_pool(&POOL, &mut in_rx, &mut out_tx, None).await
            }));
        }

        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
        assert_eq!(
            POOL.live_count(),
            0,
            "512 条空闲挂起连接不得长期持有任何 64KiB 读缓冲"
        );

        for tx in &mut in_txs {
            tx.write_all(b"PING\n").await.unwrap();
        }
        for rx in &mut out_rxs {
            let mut got = [0u8; 5];
            tokio::time::timeout(Duration::from_secs(2), rx.read_exact(&mut got))
                .await
                .expect("ping roundtrip timeout")
                .unwrap();
            assert_eq!(&got, b"PING\n");
        }

        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
        assert_eq!(
            POOL.live_count(),
            0,
            "收发结束后回泊 Poll::Pending 必须全数归还缓冲"
        );
        assert!(POOL.idle_count() <= COPY_POOL_MAX_IDLE);
        assert!(
            POOL.allocations() < 128,
            "共享池复用下 512 连接总分配 ({}) 应远低于 128",
            POOL.allocations()
        );

        drop(in_txs);
        for t in tasks {
            assert_eq!(t.await.unwrap().unwrap(), 5);
        }
        assert_eq!(POOL.live_count(), 0);
    }

    struct PendingWriteIo;

    impl AsyncWrite for PendingWriteIo {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            _buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            Poll::Pending
        }
        fn poll_write_vectored(
            self: Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            _bufs: &[IoSlice<'_>],
        ) -> Poll<io::Result<usize>> {
            Poll::Pending
        }
        fn is_write_vectored(&self) -> bool {
            true
        }
        fn poll_flush(
            self: Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
        fn poll_shutdown(
            self: Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn copy_batched_drop_mid_write_returns_all_buffers() {
        static POOL: CopyBufPool = CopyBufPool::new();
        let (mut tx, mut rx) = tokio::io::duplex(256 * 1024);
        tx.write_all(&vec![0xABu8; 128 * 1024]).await.unwrap();

        let mut dst = PendingWriteIo;
        let fut = Box::pin(copy_batched_with_pool(&POOL, &mut rx, &mut dst, None));
        let mut fut = fut;
        tokio::select! {
            _ = &mut fut => panic!("PendingWriteIo 不可能自行完成"),
            _ = async {
                for _ in 0..8 {
                    tokio::task::yield_now().await;
                }
            } => {}
        }
        assert!(POOL.live_count() > 0, "写背压阻塞期间批内应存在借出缓冲");
        drop(fut);
        assert_eq!(
            POOL.live_count(),
            0,
            "写中途取消/丢弃 future 后必须全额归还借出缓冲"
        );
    }

    #[test]
    fn copy_pool_bounds_idle_and_reuses_cross_thread() {
        let pool = CopyBufPool::new();
        // 拒绝过小或过大缓冲,防止异常尺寸污染池体
        pool.put_raw(BytesMut::with_capacity(128));
        pool.put_raw(BytesMut::with_capacity(COPY_POOL_CLASS_MAX + 4096));
        assert_eq!(pool.idle_count(), 0);

        // 上限严格封顶在 COPY_POOL_MAX_IDLE (256)
        for _ in 0..(COPY_POOL_MAX_IDLE + 64) {
            pool.put_raw(BytesMut::with_capacity(COPY_CHUNK_CAP));
        }
        assert_eq!(pool.idle_count(), COPY_POOL_MAX_IDLE);

        // 跨线程归还后主线程取用应 0 新分配复用
        let fresh_pool = std::sync::Arc::new(CopyBufPool::new());
        let worker_pool = fresh_pool.clone();
        std::thread::spawn(move || {
            for _ in 0..8 {
                worker_pool.put_raw(BytesMut::with_capacity(COPY_CHUNK_CAP));
            }
        })
        .join()
        .unwrap();
        assert_eq!(fresh_pool.idle_count(), 8);
        let held: Vec<_> = (0..8).map(|_| fresh_pool.get()).collect();
        assert_eq!(
            fresh_pool.allocations(),
            0,
            "跨线程放入的缓冲必须被命中复用"
        );
        assert_eq!(fresh_pool.live_count(), 8);
        drop(held);
        assert_eq!(fresh_pool.live_count(), 0);
        assert_eq!(fresh_pool.idle_count(), 8);
    }
}
