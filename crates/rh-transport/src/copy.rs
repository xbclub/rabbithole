//! 通用批量字节拷贝泵(P3):ReadBuf 直读(单块 ≤64KiB 红线)+ 就绪抢收成批 +
//! writev 聚合 + 写期间持续预取 + 取消安全限速预留。

use bytes::{BufMut as _, BytesMut};
use std::collections::VecDeque;
use std::io::{self, IoSlice};
use std::pin::Pin;
use std::sync::Arc;
use std::task::Poll;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt as _, ReadBuf};

/// 单次读入字节上限(64KiB 红线,不得放宽)。
pub const COPY_CHUNK_CAP: usize = 64 * 1024;

/// 通用 copy 泵单批聚合成段上限。
pub const COPY_BATCH_SEGS: usize = 64;

const DEFAULT_COPY_BATCH_BYTES: usize = 256 * 1024;
const MIN_COPY_BATCH_BYTES: usize = 1024;
const MAX_COPY_BATCH_BYTES: usize = 1024 * 1024;

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

enum TryChunk {
    Data(BytesMut),
    Eof,
    Empty,
}

struct BatchReader {
    spare: Option<BytesMut>,
    free: Vec<BytesMut>,
}

impl BatchReader {
    fn new() -> Self {
        Self {
            spare: None,
            free: Vec::with_capacity(8),
        }
    }

    fn alloc_buf(&mut self) -> BytesMut {
        self.free
            .pop()
            .unwrap_or_else(|| BytesMut::with_capacity(COPY_CHUNK_CAP))
    }

    fn recycle(&mut self, mut bm: BytesMut) {
        bm.clear();
        if self.free.len() < COPY_BATCH_SEGS {
            self.free.push(bm);
        }
    }

    /// 真实 waker 非阻塞读:无就绪数据返回 `TryChunk::Empty`(不挂起)。
    async fn try_read_chunk<R: AsyncRead + Unpin>(&mut self, r: &mut R) -> io::Result<TryChunk> {
        let mut spare_slot = self.spare.take().unwrap_or_else(|| self.alloc_buf());
        let n = std::future::poll_fn(|cx| {
            let spare = spare_slot.spare_capacity_mut();
            let limit = spare.len().min(COPY_CHUNK_CAP);
            let mut rb = ReadBuf::uninit(&mut spare[..limit]);
            match Pin::new(&mut *r).poll_read(cx, &mut rb) {
                Poll::Ready(Ok(())) => Poll::Ready(Ok::<usize, io::Error>(rb.filled().len())),
                Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
                Poll::Pending => Poll::Ready(Ok::<usize, io::Error>(usize::MAX)),
            }
        })
        .await?;
        if n == usize::MAX {
            self.spare = Some(spare_slot);
            return Ok(TryChunk::Empty);
        }
        if n == 0 {
            self.spare = Some(spare_slot);
            return Ok(TryChunk::Eof);
        }
        // SAFETY: poll_read 只把已写入的前 n 字节标为 filled,且 n <= limit <= spare.len()。
        unsafe { spare_slot.advance_mut(n) };
        Ok(TryChunk::Data(spare_slot))
    }

    /// 阻塞读一块:`Ok(None)` = EOF。把未消费缓冲留在 `self.spare` 中,
    /// 使 `select!` 取消读 future 时不丢失已分配的 64KiB 缓冲。
    async fn read_chunk<R: AsyncRead + Unpin>(
        &mut self,
        r: &mut R,
    ) -> io::Result<Option<BytesMut>> {
        let n = std::future::poll_fn(|cx| {
            if self.spare.is_none() {
                self.spare = Some(self.alloc_buf());
            }
            let bm = self.spare.as_mut().expect("spare populated above");
            let spare = bm.spare_capacity_mut();
            let limit = spare.len().min(COPY_CHUNK_CAP);
            let mut rb = ReadBuf::uninit(&mut spare[..limit]);
            match Pin::new(&mut *r).poll_read(cx, &mut rb) {
                Poll::Ready(Ok(())) => Poll::Ready(Ok(rb.filled().len())),
                Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
                Poll::Pending => Poll::Pending,
            }
        })
        .await?;
        if n == 0 {
            return Ok(None);
        }
        let mut out = self.spare.take().expect("spare populated before read");
        // SAFETY: poll_read 只把已写入的前 n 字节标为 filled,且 n <= limit <= spare.len()。
        unsafe { out.advance_mut(n) };
        Ok(Some(out))
    }
}

async fn write_vectored_batch<W: AsyncWrite + Unpin>(
    w: &mut W,
    chunks: &[BytesMut],
    limiter: &Option<Arc<dyn crate::ChunkLimiter>>,
) -> io::Result<usize> {
    let total: usize = chunks.iter().map(|c| c.len()).sum();
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
            let s = if i == 0 { &c[off..] } else { &c[..] };
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

/// 通用批量 copy 泵:ReadBuf 直读(单块 ≤ 64KiB 红线)+ 就绪抢收成批 +
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
    let batch_limit = copy_batch_bytes();
    let has_limiter = limiter.is_some();
    let max_segs = if has_limiter { 1 } else { COPY_BATCH_SEGS };

    let mut reader = BatchReader::new();
    let mut pending: VecDeque<BytesMut> = VecDeque::new();
    let mut pending_bytes: usize = 0;
    let mut batch: Vec<BytesMut> = Vec::with_capacity(max_segs);
    let mut prefetched: VecDeque<BytesMut> = VecDeque::new();
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

        while !eof_seen && batch_bytes < batch_limit && batch.len() < max_segs {
            match reader.try_read_chunk(r).await {
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

        if batch.is_empty() {
            if let Some(e) = read_err.take() {
                return Err(e);
            }
            if eof_seen {
                w.flush().await?;
                return Ok(total_copied);
            }
            match reader.read_chunk(r).await? {
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
                    let read_fut = reader.read_chunk(r);
                    futures::pin_mut!(read_fut);
                    match futures::future::select(read_fut, write_fut.as_mut()).await {
                        futures::future::Either::Left((nr, _)) => match nr {
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
                        futures::future::Either::Right((res, _)) => break res,
                    }
                }
            } else {
                write_vectored_batch(w, &batch, &limiter).await
            };

        let written = write_res?;
        total_copied += written as u64;

        for bm in batch.drain(..) {
            reader.recycle(bm);
        }
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
}
