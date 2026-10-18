// Copyright (c) 2019 Parity Technologies (UK) Ltd.
//
// Licensed under the Apache License, Version 2.0 or MIT license, at your option.
//
// A copy of the Apache License, Version 2.0 is included in the software as
// LICENSE-APACHE and a copy of the MIT license is included as
// LICENSE-MIT. You may also obtain a copy of the Apache License, Version 2.0, at
// https://www.apache.org/licenses/LICENSE-2.0, or the MIT license, at
// https://opensource.org/licenses/MIT, at your option.

//! 零拷贝改造说明(rabbithole vendor):
//!
//! * 写侧:帧头(12B)+帧体通过 [`Writev2::poll_write2`] 合并下发
//!   (载体实现可用 `writev` 单 syscall);帧体来自 `crate::pool` 池化缓冲,
//!   写穿后归还。round-5 §3 起 Sink 持批队列([`WriteState::Batch`]):
//!   多帧聚合为一次 [`Writev2::poll_writev`] 多段写出,把"每帧一次
//!   writev"降为"每批一次 syscall"。
//! * 读侧:staging `BytesMut` 批量接收,一次 read 可含多帧;帧体以
//!   `BytesMut::split_to(...).freeze()` 零拷贝切片交付,消除逐帧
//!   `vec![0; body_len]` 的分配+清零。

use super::{
    header::{self, HeaderDecodeError},
    Frame,
};
use crate::connection::Id;
use bytes::{Buf, BufMut, BytesMut};
use futures::{prelude::*, ready};
use std::{
    collections::VecDeque,
    fmt,
    io::{self, IoSlice},
    pin::Pin,
    task::{Context, Poll},
};

/// Maximum Yamux frame body length
///
/// Limits the amount of bytes a remote can cause the local node to allocate at once when reading.
///
/// Chosen based on intuition in past iterations.
const MAX_FRAME_BODY_LEN: usize = crate::MIB;

/// staging 一次读入的最小预留(批量接收窗口;一次 read 可解出多帧)。
///
/// round-8 三机消融实测(2026-10-17,benchmark/runs/tcp-multiflow-frp-20261015/
/// ab/REPORT-round8.md):提升至 256KiB 在 ×16 上行(臂间噪声 ±0.1% 的
/// 稳定方向)造成 -4.0% 确凿回退——单轮 poll 的读窗口拉长 4 倍,每流
/// WU 窗口更新回程被推迟,流控信用回路时延增大(Round-7 批时序回退同类
/// 机制)。READ_RESERVE 保持 64KiB;接收侧 syscall 削减的探索须另寻
/// 不阻塞 WU 回程节奏的形态。
const READ_RESERVE: usize = 64 * 1024;

/// 小帧 copy-out 阈值(≤ READ_RESERVE)。goal-repair C1:帧体不完整时
/// staging 按 `missing.max(READ_RESERVE)` 预留,交错大帧会驱动 backing
/// 扩容迁移;若小帧仍以零拷贝切片交付,未消费的小切片会把整段
/// (不断增长的)staging backing 钉在驻留里。阈值内帧体改为精确小分配,
/// 大帧保留零拷贝切片(转发主力路径不受影响)。
const SMALL_FRAME_COPY: usize = 4 * 1024;

/// 跨帧批量写的聚合上限——帧数(round-5 §3.2;round-7 扩至 64)。一次
/// `poll_writev` 最多聚合队列中的 [`MAX_BATCH_FRAMES`] 帧;发送半按同一
/// 上限连续取帧入队(§3.3)。64 = 1MiB/64KiB 的 16 满帧留足余量:64KiB
/// 主力帧由字节上限先封顶,小帧(4KiB 控制交错)场景才由本上限封顶,
/// 得以更深聚合。
pub(crate) const MAX_BATCH_FRAMES: usize = 64;

/// 跨帧批量写的聚合上限——字节(round-5 §3.2;round-7 扩至 1MiB)。只约束
/// 聚合:队列为空时单帧无条件入队,单帧可超此值。1MiB 在 64KiB 帧
/// (应用层 CHUNK_CAP 步进,转发主力形态)下完整容纳 16 满帧——单批覆盖
/// 16 流;256KiB 上限下同形态 4 帧即止(帧数利用率 25%),16 流需 4 次
/// writev。
pub(crate) const MAX_BATCH_BYTES: usize = 1024 * 1024;

/// 一次 `poll_writev` 的段数上限(§3.2;round-7 随帧数上限扩至 129)。
/// 半写中的 current 至多贡献 2 段(帧头余段 + 帧体),队列后续每帧 ≤2 段
/// (帧头 + 非空帧体):current 1 段 + 64 帧 × 2 = 129;current 2 段时只
/// 容得下 63 帧。栈上 IoSlice 数组 129 × 16B ≈ 2KB,远低于 Linux
/// IOV_MAX=1024。
const MAX_WRITEV_SEGMENTS: usize = 129;

/// 两段缓冲合并写出。
///
/// 由载体侧实现:支持向量写出的 IO 用 `writev` 一次下发(a 为帧头、b 为帧体),
/// 不支持时退化为顺序写。返回值语义与 `poll_write` 一致:针对 a+b 拼接视图
/// 本次写出的字节数;a 为空时只写 b,b 为空时只写 a。
pub trait Writev2: AsyncWrite {
    fn poll_write2(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        a: &[u8],
        b: &[u8],
    ) -> Poll<io::Result<usize>>;

    /// 多段批量写出(round-5 §3.1):返回针对 `bufs` 拼接视图本次写出的
    /// 字节数,部分写出由调用方按返回值推进偏移后重拼剩余段。
    ///
    /// 默认实现:顺序退化——每次只写出前两个非空段,经 [`Self::poll_write2`]
    /// 下发(a/b 契约是纯字节拼接视图,不假定帧边界,跨帧两段同样合法)。
    /// 只实现了 `poll_write2` 的旧载体由此零改动即可编译,且保持原
    /// "每帧一次两段写"的 syscall 形态不回退;真正的多段向量写由载体
    /// 覆盖(`YamuxCarrier` → `poll_write_vectored`、`SplitSocket` 取锁后
    /// 委托内层)。全空段 → `Ok(0)`,本模块 Sink 保证不以全空段调用。
    fn poll_writev(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let mut it = bufs.iter().filter(|b| !b.is_empty());
        match (it.next(), it.next()) {
            (Some(a), Some(b)) => self.poll_write2(cx, &a[..], &b[..]),
            (Some(a), None) => self.poll_write2(cx, &a[..], &[]),
            (None, _) => Poll::Ready(Ok(0)),
        }
    }
}

/// 顺序写出适配器:把 [`Writev2`] 实现为「先 a 后 b」的两段顺序写。
/// yamux 内部测试与不支持向量写的载体使用。
pub struct Sequential<T>(pub T);

impl<T: AsyncRead + Unpin> AsyncRead for Sequential<T> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().0).poll_read(cx, buf)
    }
}

impl<T: AsyncWrite + Unpin> AsyncWrite for Sequential<T> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().0).poll_write(cx, buf)
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().0).poll_flush(cx)
    }
    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().0).poll_close(cx)
    }
}

impl<T: AsyncWrite + Unpin> Writev2 for Sequential<T> {
    fn poll_write2(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        a: &[u8],
        b: &[u8],
    ) -> Poll<io::Result<usize>> {
        let inner = &mut self.get_mut().0;
        if !a.is_empty() {
            Pin::new(inner).poll_write(cx, a)
        } else {
            Pin::new(inner).poll_write(cx, b)
        }
    }
}

/// A [`Stream`] and writer of [`Frame`] values.
pub(crate) struct Io<T> {
    id: Id,
    io: T,
    read: Staging,
    write_state: WriteState,
}

/// 接收 staging:批量读入 + 零拷贝帧体切片。
///
/// 初始化不变量:`[0, zeroed)` 相对当前 buf 起点已初始化,且恒有
/// `zeroed >= buf.len()`。向底层 IO 暴露空闲区前,先把将写区间零填,
/// 使「未初始化内存 → `&mut [u8]`」的转换满足 futures `AsyncRead`
/// 的缓冲契约(调用方传入的切片必须已初始化)。
struct Staging {
    buf: BytesMut,
    zeroed: usize,
}

impl Staging {
    fn new() -> Self {
        Self {
            buf: BytesMut::new(),
            zeroed: 0,
        }
    }

    fn len(&self) -> usize {
        self.buf.len()
    }

    fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    fn reserve(&mut self, additional: usize) {
        let cap_before = self.buf.capacity();
        self.buf.reserve(additional);
        if self.buf.capacity() != cap_before {
            // 扩容/搬移只保留 [0, len):其后重新视为未初始化
            self.zeroed = self.zeroed.min(self.buf.len());
        }
    }

    /// 确保空闲区前 `want` 字节已初始化,返回该可写切片
    /// (实际长度受当前空闲容量截断,≥0)。
    fn zeroed_spare(&mut self, want: usize) -> &mut [u8] {
        let len = self.buf.len();
        let fill_from = self.zeroed;
        let spare = self.buf.spare_capacity_mut();
        let take = want.min(spare.len());
        let ptr = spare.as_mut_ptr();
        if fill_from < len + take {
            for b in &mut spare[fill_from - len..take] {
                *b = std::mem::MaybeUninit::new(0);
            }
            self.zeroed = len + take;
        }
        // SAFETY: [len, len+take) ⊆ [0, zeroed) 已初始化(上面刚保证),
        // 构造 &mut [u8] 不违反初始化契约。
        #[allow(unsafe_code)]
        unsafe {
            std::slice::from_raw_parts_mut(ptr.cast::<u8>(), take)
        }
    }

    /// 消费前缀 k 字节(帧头/帧体边界推进)。
    fn advance(&mut self, k: usize) {
        self.buf.advance(k);
        self.zeroed -= k;
    }

    /// 取出前缀 k 字节为帧体。
    /// 小帧(≤ SMALL_FRAME_COPY)copy-out 为精确独立分配,避免未消费的
    /// 小切片钉住 staging backing(C1);大帧保持零拷贝切片。
    fn take_body(&mut self, k: usize) -> bytes::Bytes {
        if k <= SMALL_FRAME_COPY {
            let mut b = BytesMut::with_capacity(k);
            b.extend_from_slice(&self.buf[..k]);
            self.advance(k);
            b.freeze()
        } else {
            self.split_to(k)
        }
    }

    /// 取出前缀 k 字节为零拷贝帧体。
    fn split_to(&mut self, k: usize) -> bytes::Bytes {
        let body = self.buf.split_to(k).freeze();
        self.zeroed -= k;
        body
    }

    /// 读入 n 字节后推进长度(bytes 官方 IO 扩展模式;n ≤ 已初始化区间)。
    fn appended(&mut self, n: usize) {
        #[allow(unsafe_code)]
        unsafe {
            self.buf.advance_mut(n);
        }
    }
}

impl<T: AsyncRead + AsyncWrite + Unpin> Io<T> {
    pub(crate) fn new(id: Id, io: T) -> Self {
        Io {
            id,
            io,
            read: Staging::new(),
            write_state: WriteState::Init,
        }
    }
}

/// 已编码待写帧:12B 帧头 + 帧体(所有权自 [`Frame`] 转入,写穿后归池)。
struct PendingFrame {
    header: [u8; header::HEADER_SIZE],
    body: bytes::Bytes,
}

impl PendingFrame {
    /// 线上总字节(帧头 + 帧体)。
    fn wire_len(&self) -> usize {
        header::HEADER_SIZE + self.body.len()
    }
}

/// The stages of writing `Frame`s.
///
/// round-5 §3.2 批队列:`start_send` 只入队;`poll_ready` 把半写中的
/// `current` 余段与队列后续帧拼成 IoSlice 数组一次 [`Writev2::poll_writev`]
/// 写出,按返回字节跨帧推进 `offset`,写穿的帧体归池。`Ready(Ok)` 仅当
/// 队列 + current 全部写穿(→ `Init`);载体 Pending 时半写状态原地保留;
/// 写错误 → `Poisoned`(B4:此后拒绝新帧,原始 IO 错误向上传播)。
enum WriteState {
    /// 队列空且无半写。
    Init,
    Batch {
        queue: VecDeque<PendingFrame>,
        /// 半写中的帧(`offset < wire_len`);`None` 时下一轮自队首取。
        current: Option<PendingFrame>,
        /// `current` 的已写偏移(跨帧头/帧体边界计)。
        offset: usize,
    },
    Poisoned,
}

impl fmt::Debug for WriteState {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            WriteState::Init => f.write_str("(WriteState::Init)"),
            WriteState::Batch {
                queue,
                current,
                offset,
            } => write!(
                f,
                "(WriteState::Batch (queued {}) (current offset {offset} body-len {:?}))",
                queue.len(),
                current.as_ref().map(|c| c.body.len())
            ),
            WriteState::Poisoned => f.write_str("(WriteState::Poisoned)"),
        }
    }
}

/// 帧写穿:池化帧体按值取回归还(引用计数不为 1 时自然释放)。
fn recycle_body(body: bytes::Bytes) {
    if let Ok(mut bm) = body.try_into_mut() {
        bm.clear();
        crate::pool::put(bm);
    }
}

impl WriteState {
    /// 批量写出队列 + current,直至全部写穿(`Ready(Ok)`)/ 载体 Pending
    /// (半写状态原地保留)/ 写错误(调用方置 Poisoned)。
    ///
    /// 每轮聚合:current 自 `offset` 起的余段 + 队列中后续 ≤
    /// [`MAX_BATCH_FRAMES`] 帧,总段数 ≤ [`MAX_WRITEV_SEGMENTS`],一次
    /// `poll_writev`;写出 n 字节后先烧穿 current 余段、再逐帧核销队列,
    /// 落在帧中间的位置成为新的 current/offset。帧不会跨 writev 拆分
    /// 核销——单帧(含超 [`MAX_BATCH_BYTES`] 的)整帧归属一个 current。
    fn poll_write_batch<T: Writev2 + Unpin>(
        &mut self,
        io: &mut T,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<()>> {
        let WriteState::Batch {
            queue,
            current,
            offset,
        } = self
        else {
            // Init / Poisoned 由调用方处理,不该到达这里
            return Poll::Ready(Ok(()));
        };
        loop {
            if current.is_none() {
                match queue.pop_front() {
                    Some(f) => {
                        *current = Some(f);
                        *offset = 0;
                    }
                    None => return Poll::Ready(Ok(())),
                }
            }
            let cur = current.as_ref().expect("current set above");
            let cur_total = cur.wire_len();
            debug_assert!(*offset < cur_total, "current must have unwritten bytes");

            // 拼段:current 余段(帧头余段 + 帧体 / 仅帧体余段)+ 队列后续帧
            let mut slices = [IoSlice::new(&[]); MAX_WRITEV_SEGMENTS];
            let mut nsegs = 0usize;
            let mut total = 0usize;
            {
                let (a, b) = if *offset < header::HEADER_SIZE {
                    (&cur.header[*offset..], &cur.body[..])
                } else {
                    (&cur.header[..0], &cur.body[*offset - header::HEADER_SIZE..])
                };
                for s in [a, b] {
                    if !s.is_empty() {
                        slices[nsegs] = IoSlice::new(s);
                        nsegs += 1;
                        total += s.len();
                    }
                }
            }
            for (appended, f) in queue.iter().enumerate() {
                if appended >= MAX_BATCH_FRAMES {
                    break;
                }
                let need = 1 + usize::from(!f.body.is_empty());
                if nsegs + need > MAX_WRITEV_SEGMENTS {
                    break;
                }
                slices[nsegs] = IoSlice::new(&f.header);
                nsegs += 1;
                if !f.body.is_empty() {
                    slices[nsegs] = IoSlice::new(&f.body);
                    nsegs += 1;
                }
                total += f.wire_len();
            }

            let n = match Pin::new(&mut *io).poll_writev(cx, &slices[..nsegs]) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Ready(Ok(0)) => {
                    return Poll::Ready(Err(io::ErrorKind::WriteZero.into()));
                }
                Poll::Ready(Ok(n)) => n,
            };
            if n > total {
                let err = io::Error::other(format!(
                    "Writer returned invalid write count n={n}: exceeds offered {total}"
                ));
                return Poll::Ready(Err(err));
            }

            // 跨帧推进:先 current 余段,再逐帧核销队列
            let cur_left = cur_total - *offset;
            if n < cur_left {
                *offset += n;
                continue;
            }
            let mut rem = n - cur_left;
            recycle_body(current.take().expect("current set above").body);
            *offset = 0;
            while rem > 0 {
                let ft = queue
                    .front()
                    .expect("writev count is bounded by offered bytes")
                    .wire_len();
                if rem >= ft {
                    recycle_body(queue.pop_front().expect("front checked").body);
                    rem -= ft;
                } else {
                    *current = queue.pop_front();
                    *offset = rem;
                    rem = 0;
                }
            }
            // 就绪状态下继续写剩余批(队列空且无半写时下一轮返回 Ok)
        }
    }
}

fn poisoned_error() -> io::Error {
    io::Error::other("Sink is in poisoned state due to previous write error")
}

impl<T> Sink<Frame<()>> for Io<T>
where
    T: AsyncRead + AsyncWrite + Unpin + Writev2,
{
    type Error = io::Error;

    /// `Ready(Ok)` 仅当队列 + current 全部写穿(§3.2 Sink 契约):批量
    /// 入队的调用方由此得到背压——载体阻塞时半写状态保留并返回
    /// Pending(载体写 waker 已武装),不会再接受新帧。
    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = Pin::into_inner(self);
        log::trace!("{}: write: {:?}", this.id, this.write_state);
        match this.write_state {
            WriteState::Init => Poll::Ready(Ok(())),
            WriteState::Batch { .. } => match this.write_state.poll_write_batch(&mut this.io, cx) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(Err(e)) => {
                    this.write_state = WriteState::Poisoned;
                    Poll::Ready(Err(e))
                }
                Poll::Ready(Ok(())) => {
                    this.write_state = WriteState::Init;
                    Poll::Ready(Ok(()))
                }
            },
            WriteState::Poisoned => Poll::Ready(Err(poisoned_error())),
        }
    }

    /// 入队(编码 12B 帧头,帧体所有权转入);聚合上限由调用方按
    /// [`MAX_BATCH_FRAMES`] / [`MAX_BATCH_BYTES`] 控制入队节奏,本方法
    /// 不拒绝超限入队(Sink 契约下调用方每 `poll_ready` Ready 后的入队
    /// 批自然有界)。
    fn start_send(self: Pin<&mut Self>, f: Frame<()>) -> Result<(), Self::Error> {
        let this = Pin::into_inner(self);
        // B4:写错误已置 Poisoned 后必须拒绝新帧——无条件接受会覆盖
        // 半写状态,使帧边界错乱(前帧残段 + 新帧拼接破坏线协议)。
        if matches!(this.write_state, WriteState::Poisoned) {
            return Err(poisoned_error());
        }
        let frame = PendingFrame {
            header: header::encode(&f.header),
            body: f.body,
        };
        match &mut this.write_state {
            WriteState::Batch { queue, .. } => queue.push_back(frame),
            state => {
                let mut queue = VecDeque::with_capacity(MAX_BATCH_FRAMES);
                queue.push_back(frame);
                *state = WriteState::Batch {
                    queue,
                    current: None,
                    offset: 0,
                };
            }
        }
        Ok(())
    }

    /// 整队写穿后才 flush 载体(未排空前 Pending)。
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = Pin::into_inner(self);
        ready!(this.poll_ready_unpin(cx))?;
        Pin::new(&mut this.io).poll_flush(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = Pin::into_inner(self);
        ready!(this.poll_ready_unpin(cx))?;
        Pin::new(&mut this.io).poll_close(cx)
    }
}

impl<T: AsyncRead + AsyncWrite + Unpin> Stream for Io<T> {
    type Item = Result<Frame<()>, FrameDecodeError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        let this = &mut *self;
        loop {
            // 1) staging 中已有完整帧则直接交付(零拷贝切片)
            if this.read.len() >= header::HEADER_SIZE {
                let hdr = {
                    let bytes: &[u8; header::HEADER_SIZE] = this.read.buf[..header::HEADER_SIZE]
                        .try_into()
                        .expect("length checked above");
                    match header::decode(bytes) {
                        Ok(hd) => hd,
                        Err(e) => return Poll::Ready(Some(Err(e.into()))),
                    }
                };
                log::trace!("{}: read: {}", this.id, hdr);

                if hdr.tag() != header::Tag::Data {
                    this.read.advance(header::HEADER_SIZE);
                    return Poll::Ready(Some(Ok(Frame::new(hdr))));
                }

                let body_len = hdr.len().val() as usize;
                if body_len > MAX_FRAME_BODY_LEN {
                    return Poll::Ready(Some(Err(FrameDecodeError::FrameTooLarge(body_len))));
                }

                if this.read.len() - header::HEADER_SIZE >= body_len {
                    this.read.advance(header::HEADER_SIZE);
                    let body = this.read.take_body(body_len);
                    return Poll::Ready(Some(Ok(Frame { header: hdr, body })));
                }
                // 帧体不完整:按缺口预留
                let missing = body_len - (this.read.len() - header::HEADER_SIZE);
                this.read.reserve(missing.max(READ_RESERVE));
            } else {
                // 帧头不完整(或 staging 空):批量预留
                this.read.reserve(READ_RESERVE);
            }

            // 2) 从底层读入 staging 空闲区(已按需零填,满足初始化契约)
            let spare = this.read.zeroed_spare(usize::MAX);
            if spare.is_empty() {
                // reserve 后仍无空闲(理论不可达):自唤醒让出,避免
                // 无 waker 的 Pending 挂死,也不自旋
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            let n = match ready!(Pin::new(&mut this.io).poll_read(cx, spare)) {
                Ok(0) => 0,
                Ok(n) => {
                    this.read.appended(n);
                    n
                }
                Err(e) => return Poll::Ready(Some(Err(FrameDecodeError::Io(e)))),
            };
            if n == 0 {
                // EOF:staging 干净(帧边界)= 正常关闭;残留半帧 = 协议错
                if this.read.is_empty() {
                    return Poll::Ready(None);
                }
                let e = FrameDecodeError::Io(io::ErrorKind::UnexpectedEof.into());
                return Poll::Ready(Some(Err(e)));
            }
        }
    }
}

/// Possible errors while decoding a message frame.
#[non_exhaustive]
#[derive(Debug)]
pub enum FrameDecodeError {
    /// An I/O error.
    Io(io::Error),
    /// Decoding the frame header failed.
    Header(HeaderDecodeError),
    /// A data frame body length is larger than the configured maximum.
    FrameTooLarge(usize),
}

impl std::fmt::Display for FrameDecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            FrameDecodeError::Io(e) => write!(f, "i/o error: {e}"),
            FrameDecodeError::Header(e) => write!(f, "decode error: {e}"),
            FrameDecodeError::FrameTooLarge(n) => write!(f, "frame body is too large ({n})"),
        }
    }
}

impl std::error::Error for FrameDecodeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            FrameDecodeError::Io(e) => Some(e),
            FrameDecodeError::Header(e) => Some(e),
            FrameDecodeError::FrameTooLarge(_) => None,
        }
    }
}

impl From<std::io::Error> for FrameDecodeError {
    fn from(e: std::io::Error) -> Self {
        FrameDecodeError::Io(e)
    }
}

impl From<HeaderDecodeError> for FrameDecodeError {
    fn from(e: HeaderDecodeError) -> Self {
        FrameDecodeError::Header(e)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::frame::header::StreamId;
    use quickcheck::{Arbitrary, Gen, QuickCheck};
    use rand::RngCore;

    impl Arbitrary for Frame<()> {
        fn arbitrary(g: &mut Gen) -> Self {
            let mut header: header::Header<()> = Arbitrary::arbitrary(g);
            let body = if header.tag() == header::Tag::Data {
                header.set_len(header.len().val() % 4096);
                let mut b = vec![0; header.len().val() as usize];
                rand::thread_rng().fill_bytes(&mut b);
                b.into()
            } else {
                bytes::Bytes::new()
            };
            Frame { header, body }
        }
    }

    #[test]
    fn encode_decode_identity() {
        fn property(f: Frame<()>) -> bool {
            futures::executor::block_on(async move {
                let id = crate::connection::Id::next();
                let cursor = futures::io::Cursor::new(Vec::new());
                let mut io = Io::new(id, Sequential(cursor));
                if io.send(f.clone()).await.is_err() {
                    return false;
                }
                if io.flush().await.is_err() {
                    return false;
                }
                io.io.0.set_position(0);
                if let Ok(Some(x)) = io.try_next().await {
                    x == f
                } else {
                    false
                }
            })
        }

        QuickCheck::new()
            .tests(10_000)
            .quickcheck(property as fn(Frame<()>) -> bool)
    }

    /// 写侧恒定失败的载体。
    struct FailingWriter;

    impl AsyncRead for FailingWriter {
        fn poll_read(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            _buf: &mut [u8],
        ) -> Poll<io::Result<usize>> {
            Poll::Pending
        }
    }

    impl AsyncWrite for FailingWriter {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            _buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            Poll::Ready(Err(io::Error::other("mock-io-write-failure")))
        }
        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
        fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    /// B4(红→绿):写错误之后 sink 必须 Poisoned——poll_ready 持续 Err,
    /// start_send 拒绝新帧,不得覆盖半写状态后继续接受帧。
    #[test]
    fn write_error_poisons_sink() {
        let mut io = Io::new(crate::connection::Id::next(), Sequential(FailingWriter));

        let frame: Frame<()> = Frame::data(StreamId::new(1), bytes::Bytes::from_static(b"x"))
            .unwrap()
            .into();
        // 先排入一帧(start_send 在无前一帧时合法)
        Sink::start_send(Pin::new(&mut io), frame).expect("start_send with idle sink");

        let w = futures::task::noop_waker();
        let mut cx = Context::from_waker(&w);
        // 写穿尝试 → 载体写失败 → poll_ready 返回 Err
        match Sink::poll_ready(Pin::new(&mut io), &mut cx) {
            Poll::Ready(Err(e)) => {
                assert!(
                    e.to_string().contains("mock-io-write-failure"),
                    "original io error must propagate, got: {e}"
                );
            }
            other => panic!("poll_ready after write error must be Ready(Err), got {other:?}"),
        }
        // Poisoned 持续:再次 poll_ready 仍 Err
        match Sink::poll_ready(Pin::new(&mut io), &mut cx) {
            Poll::Ready(Err(_)) => {}
            other => panic!("poisoned sink must stay Err, got {other:?}"),
        }
        // start_send 必须拒绝(B4 修复点:修复前无条件接受并覆盖 Poisoned)
        let frame2: Frame<()> = Frame::data(StreamId::new(3), bytes::Bytes::from_static(b"y"))
            .unwrap()
            .into();
        assert!(
            Sink::start_send(Pin::new(&mut io), frame2).is_err(),
            "start_send must reject frames after a write error (B4)"
        );
    }

    /// 读侧按脚本逐块返回(支持部分读)、写侧收集的载体。
    struct ScriptedIo {
        blocks: std::collections::VecDeque<Vec<u8>>,
        cur: Vec<u8>,
        off: usize,
        written: Vec<u8>,
    }

    impl AsyncRead for ScriptedIo {
        fn poll_read(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &mut [u8],
        ) -> Poll<io::Result<usize>> {
            let this = self.get_mut();
            if this.off >= this.cur.len() {
                this.cur = this.blocks.pop_front().unwrap_or_default();
                this.off = 0;
            }
            if this.off < this.cur.len() {
                let n = (this.cur.len() - this.off).min(buf.len());
                buf[..n].copy_from_slice(&this.cur[this.off..this.off + n]);
                this.off += n;
                return Poll::Ready(Ok(n));
            }
            Poll::Ready(Ok(0))
        }
    }

    impl AsyncWrite for ScriptedIo {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            self.get_mut().written.extend_from_slice(buf);
            Poll::Ready(Ok(buf.len()))
        }
        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
        fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    impl Writev2 for ScriptedIo {
        fn poll_write2(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            a: &[u8],
            b: &[u8],
        ) -> Poll<io::Result<usize>> {
            let this = self.get_mut();
            this.written.extend_from_slice(a);
            this.written.extend_from_slice(b);
            let _ = cx;
            Poll::Ready(Ok(a.len() + b.len()))
        }
    }

    /// C1(红→绿):小帧帧体必须 copy-out 交付——未消费的小帧切片
    /// 不得钉住整个 staging backing(交错大帧扩容下驻留无界放大)。
    /// 判据:小帧 body 独占分配(try_into_mut 成功);
    /// 大帧 body 保持零拷贝切片(try_into_mut 失败,与 staging 共享)。
    #[test]
    fn staging_small_frame_bounded() {
        const M: usize = 32;
        let big = vec![7u8; 256 * 1024];
        let mut blocks = std::collections::VecDeque::new();
        let mut small_payloads = Vec::new();
        for i in 0..M {
            // 小帧:1B 数据
            let mut h = header::Header::data(StreamId::new((i + 1) as u32), 1);
            h.syn();
            small_payloads.push(i as u8 + 0xA0);
            let mut blk = header::encode(&h).to_vec();
            blk.push(i as u8 + 0xA0);
            blocks.push_back(blk);
            // 大帧:触发 staging reserve 扩容,使 backing 迁移
            let mut h = header::Header::data(StreamId::new((i + 1) as u32), big.len() as u32);
            h.syn();
            let mut blk = header::encode(&h).to_vec();
            blk.extend_from_slice(&big);
            blocks.push_back(blk);
        }

        let mut io = Io::new(
            crate::connection::Id::next(),
            ScriptedIo {
                blocks,
                cur: Vec::new(),
                off: 0,
                written: Vec::new(),
            },
        );

        let mut small_bodies = Vec::new();
        let mut saw_big = false;
        while let Some(mut frame) = futures::executor::block_on(io.try_next()).unwrap() {
            if frame.header().tag() == header::Tag::Data && frame.body.len() == 1 {
                small_bodies.push(std::mem::take(&mut frame.body));
            } else if frame.body.len() == big.len() {
                let body = std::mem::take(&mut frame.body);
                // 大帧保持零拷贝:与 staging backing 共享,不可 try_into_mut
                assert!(
                    body.try_into_mut().is_err(),
                    "large frames must stay zero-copy slices of the staging buffer"
                );
                saw_big = true;
            }
        }
        assert_eq!(small_bodies.len(), M, "all small frames must be delivered");
        assert!(saw_big, "large frames must be delivered");
        // C1 判据(容量口径):小帧交付后的可变缓冲容量必须处于
        // 精确小分配量级——不得是 staging backing(64KiB 量级)。
        // 交错大帧驱动 reserve 扩容后,零拷贝小切片要么与 staging
        // 共享 backing(try_into_mut Err),要么独占旧 backing
        // (try_into_mut Ok 但 capacity ~READ_RESERVE 量级)——两者都把
        // 1B 载荷钉在 64KiB+ 驻留上;M 个未消费小帧的总驻留无界放大。
        let mut max_small_cap = 0usize;
        for (i, b) in small_bodies.iter_mut().enumerate() {
            assert_eq!(&b[..], &[small_payloads[i]], "payload must round-trip");
            let bm = match std::mem::take(b).try_into_mut() {
                Ok(bm) => bm,
                Err(_) => panic!(
                    "small frame body still shares the staging backing (C1): copy-out expected"
                ),
            };
            assert!(
                bm.capacity() <= SMALL_FRAME_COPY * 2,
                "small frame body pins a {}-byte backing (C1 unbounded residency)",
                bm.capacity()
            );
            max_small_cap = max_small_cap.max(bm.capacity());
        }
        assert!(
            max_small_cap > 0,
            "small bodies must be independently owned"
        );
    }

    // ===== round-5 §5.4 跨帧批量写(Sink 级:WriteState::Batch 契约)=====

    /// 批量写测试载体:`blocked` → 恒 Pending(载体阻塞);`chunk` → 每次
    /// writev 至多写出 chunk 字节(0 = 不限);`pend_after_write` → 每次
    /// 成功写后下一次调用 Pending 一次(半写状态跨 Pending 保留的驱动);
    /// `fail_after = Some(k)` → 第 k 次成功写之后恒 Err。计 writev 成功
    /// 次数与载体 flush 次数。读侧恒 Pending。
    #[derive(Default)]
    struct BatchState {
        written: Vec<u8>,
        blocked: bool,
        chunk: usize,
        pend_after_write: bool,
        pend_next: bool,
        fail_after: Option<usize>,
        writev_calls: usize,
        flush_calls: usize,
    }

    struct BatchIo(std::rc::Rc<std::cell::RefCell<BatchState>>);

    impl BatchIo {
        fn new(st: BatchState) -> (Self, std::rc::Rc<std::cell::RefCell<BatchState>>) {
            let st = std::rc::Rc::new(std::cell::RefCell::new(st));
            (BatchIo(st.clone()), st)
        }
    }

    impl AsyncRead for BatchIo {
        fn poll_read(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            _buf: &mut [u8],
        ) -> Poll<io::Result<usize>> {
            Poll::Pending
        }
    }

    impl AsyncWrite for BatchIo {
        fn poll_write(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            Writev2::poll_writev(self, cx, &[IoSlice::new(buf)])
        }
        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            self.0.borrow_mut().flush_calls += 1;
            Poll::Ready(Ok(()))
        }
        fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    impl Writev2 for BatchIo {
        fn poll_write2(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            a: &[u8],
            b: &[u8],
        ) -> Poll<io::Result<usize>> {
            Writev2::poll_writev(self, cx, &[IoSlice::new(a), IoSlice::new(b)])
        }

        fn poll_writev(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            bufs: &[IoSlice<'_>],
        ) -> Poll<io::Result<usize>> {
            let mut st = self.0.borrow_mut();
            if st.blocked {
                return Poll::Pending; // noop waker 驱动:测试手动复 poll
            }
            if let Some(k) = st.fail_after {
                if st.writev_calls >= k {
                    return Poll::Ready(Err(io::Error::other("mock-batch-write-failure")));
                }
            }
            if st.pend_next {
                st.pend_next = false;
                return Poll::Pending;
            }
            st.writev_calls += 1;
            let mut budget = if st.chunk == 0 { usize::MAX } else { st.chunk };
            let mut n = 0;
            for b in bufs {
                let take = b.len().min(budget);
                st.written.extend_from_slice(&b[..take]);
                n += take;
                budget -= take;
                if budget == 0 {
                    break;
                }
            }
            if st.pend_after_write {
                st.pend_next = true;
            }
            Poll::Ready(Ok(n))
        }
    }

    fn data_frame(id: u32, body: Vec<u8>) -> Frame<()> {
        Frame::data(StreamId::new(id), body.into()).unwrap().into()
    }

    /// 帧的线上编码(12B 帧头 + 帧体)。
    fn encode_frame(f: &Frame<()>) -> Vec<u8> {
        let mut v = header::encode(&f.header).to_vec();
        v.extend_from_slice(&f.body);
        v
    }

    fn batch_cx() -> Context<'static> {
        Context::from_waker(Box::leak(Box::new(futures::task::noop_waker())))
    }

    /// T-B2:部分写推进——载体每次只收 10B 且每次成功后 Pending 一次,
    /// 跨帧头/帧体边界与帧边界多轮推进后全帧写穿、字节零错(含空帧体
    /// Data、非 Data 控制帧、4KiB 帧的混合批)。Pending 轮数 = 成功写
    /// 次数 − 1:每轮 Pending 前恰有进展,半写状态跨 Pending 原地保留。
    #[test]
    fn batch_partial_write_advances_across_frames() {
        let (carrier, st) = BatchIo::new(BatchState {
            chunk: 10,
            pend_after_write: true,
            ..Default::default()
        });
        let mut io = Io::new(crate::connection::Id::next(), carrier);
        let mut cx = batch_cx();

        let frames: Vec<Frame<()>> = vec![
            data_frame(1, Vec::new()),
            data_frame(3, vec![0xA1; 100]),
            Frame::window_update(StreamId::new(5), 7).into(),
            data_frame(7, vec![0xB2; 4096]),
        ];
        let expected: Vec<u8> = frames.iter().flat_map(encode_frame).collect();

        assert!(
            Sink::poll_ready(Pin::new(&mut io), &mut cx).is_ready(),
            "idle sink must be ready"
        );
        for f in frames {
            Sink::start_send(Pin::new(&mut io), f).expect("enqueue");
        }
        let mut pending_rounds = 0usize;
        loop {
            match Sink::poll_flush(Pin::new(&mut io), &mut cx) {
                Poll::Ready(Ok(())) => break,
                Poll::Pending => {
                    pending_rounds += 1;
                    assert!(pending_rounds < 10_000, "flush must converge");
                }
                Poll::Ready(Err(e)) => panic!("unexpected write error: {e}"),
            }
        }
        let st = st.borrow();
        assert_eq!(
            st.written, expected,
            "bytes on the wire must equal frame order"
        );
        let calls = expected.len().div_ceil(10);
        assert_eq!(
            st.writev_calls, calls,
            "every successful writev must take exactly the 10B chunk"
        );
        assert_eq!(
            pending_rounds,
            calls - 1,
            "each Pending must be preceded by progress (state kept across Pending)"
        );
    }

    /// T-B3:背压——16 帧入队后载体阻塞 → poll_ready Pending(不接受
    /// 新帧);写穿后 Ready。写穿时 16 帧聚合为**一次** writev(帧数、段数
    /// 均在 [`MAX_BATCH_FRAMES`] / `MAX_WRITEV_SEGMENTS` 内);第二阶段
    /// `MAX_BATCH_FRAMES + 1` 帧 → 恰 2 次 writev:段数上限 = 1 + 2 ×
    /// MAX_BATCH_FRAMES,current 占 2 段时一次只容 MAX_BATCH_FRAMES 帧,
    /// 末帧落到下一次。
    #[test]
    fn batch_backpressure_pending_until_drained() {
        let (carrier, st) = BatchIo::new(BatchState {
            blocked: true,
            ..Default::default()
        });
        let mut io = Io::new(crate::connection::Id::next(), carrier);
        let mut cx = batch_cx();

        let frames: Vec<Frame<()>> = (0..16u8).map(|i| data_frame(1, vec![i; 4096])).collect();
        let expected: Vec<u8> = frames.iter().flat_map(encode_frame).collect();
        assert!(Sink::poll_ready(Pin::new(&mut io), &mut cx).is_ready());
        for f in frames {
            Sink::start_send(Pin::new(&mut io), f).expect("enqueue");
        }
        match Sink::poll_ready(Pin::new(&mut io), &mut cx) {
            Poll::Pending => {}
            other => panic!("full queue on a blocked carrier must be Pending, got {other:?}"),
        }
        assert!(
            st.borrow().written.is_empty(),
            "nothing leaves a blocked carrier"
        );

        st.borrow_mut().blocked = false;
        match Sink::poll_ready(Pin::new(&mut io), &mut cx) {
            Poll::Ready(Ok(())) => {}
            other => panic!("drained queue must be Ready, got {other:?}"),
        }
        assert_eq!(
            st.borrow().written,
            expected,
            "all 16 frames written in order"
        );
        assert_eq!(
            st.borrow().writev_calls,
            1,
            "16 queued frames must coalesce into a single writev"
        );

        // 第二阶段:MAX_BATCH_FRAMES + 1 帧 → 2 次 writev(帧数/段数上限
        // 把前 MAX_BATCH_FRAMES 帧封进第一次,末帧独立成批)
        st.borrow_mut().written.clear();
        st.borrow_mut().writev_calls = 0;
        let frames: Vec<Frame<()>> = (0..MAX_BATCH_FRAMES + 1)
            .map(|i| data_frame(3, vec![(i & 0xFF) as u8; 1024]))
            .collect();
        let expected: Vec<u8> = frames.iter().flat_map(encode_frame).collect();
        for f in frames {
            Sink::start_send(Pin::new(&mut io), f).expect("enqueue");
        }
        match Sink::poll_ready(Pin::new(&mut io), &mut cx) {
            Poll::Ready(Ok(())) => {}
            other => panic!("drained queue must be Ready, got {other:?}"),
        }
        assert_eq!(st.borrow().written, expected);
        assert_eq!(
            st.borrow().writev_calls,
            2,
            "{} frames exceed the per-writev frame bound of {MAX_BATCH_FRAMES}: exactly two writevs",
            MAX_BATCH_FRAMES + 1
        );
    }

    /// T-B5:半批写失败——2 帧入队、载体写穿第一帧帧头(12B)后 Err →
    /// poll_ready 以**原始 IO 错误**返回、sink Poisoned 持续 Err、
    /// start_send 拒绝新帧(现行 B4 等价,半写不得被新帧覆盖)。
    #[test]
    fn batch_half_write_error_poisons_sink() {
        let (carrier, st) = BatchIo::new(BatchState {
            chunk: 12,
            fail_after: Some(1),
            ..Default::default()
        });
        let mut io = Io::new(crate::connection::Id::next(), carrier);
        let mut cx = batch_cx();

        assert!(Sink::poll_ready(Pin::new(&mut io), &mut cx).is_ready());
        Sink::start_send(Pin::new(&mut io), data_frame(1, vec![1; 100])).expect("enqueue");
        Sink::start_send(Pin::new(&mut io), data_frame(3, vec![2; 100])).expect("enqueue");
        match Sink::poll_ready(Pin::new(&mut io), &mut cx) {
            Poll::Ready(Err(e)) => assert!(
                e.to_string().contains("mock-batch-write-failure"),
                "original io error must propagate, got: {e}"
            ),
            other => panic!("write failure must surface as Ready(Err), got {other:?}"),
        }
        assert_eq!(
            st.borrow().written.len(),
            12,
            "exactly the first header was written before the failure"
        );
        match Sink::poll_ready(Pin::new(&mut io), &mut cx) {
            Poll::Ready(Err(_)) => {}
            other => panic!("poisoned sink must stay Err, got {other:?}"),
        }
        assert!(
            Sink::start_send(Pin::new(&mut io), data_frame(5, vec![3; 8])).is_err(),
            "start_send must reject frames after a write error (B4)"
        );
        match Sink::poll_flush(Pin::new(&mut io), &mut cx) {
            Poll::Ready(Err(_)) => {}
            other => panic!("poisoned flush must be Err, got {other:?}"),
        }
    }

    /// T-B6:flush 语义——未排空前 poll_flush Pending 且**不得**触碰载体
    /// flush;排空(一次 writev)+ 载体 flush 后才 Ready。
    #[test]
    fn batch_flush_pending_until_drained_then_carrier_flush() {
        let (carrier, st) = BatchIo::new(BatchState {
            blocked: true,
            ..Default::default()
        });
        let mut io = Io::new(crate::connection::Id::next(), carrier);
        let mut cx = batch_cx();

        let frames: Vec<Frame<()>> = (0..3u8).map(|i| data_frame(1, vec![i; 256])).collect();
        let expected: Vec<u8> = frames.iter().flat_map(encode_frame).collect();
        assert!(Sink::poll_ready(Pin::new(&mut io), &mut cx).is_ready());
        for f in frames {
            Sink::start_send(Pin::new(&mut io), f).expect("enqueue");
        }
        match Sink::poll_flush(Pin::new(&mut io), &mut cx) {
            Poll::Pending => {}
            other => panic!("flush before drain must be Pending, got {other:?}"),
        }
        assert_eq!(
            st.borrow().flush_calls,
            0,
            "carrier flush must not run before the queue is drained"
        );

        st.borrow_mut().blocked = false;
        match Sink::poll_flush(Pin::new(&mut io), &mut cx) {
            Poll::Ready(Ok(())) => {}
            other => panic!("flush after drain must be Ready, got {other:?}"),
        }
        let st = st.borrow();
        assert_eq!(st.written, expected, "all frames written in order");
        assert_eq!(st.writev_calls, 1, "3 frames coalesce into one writev");
        assert_eq!(
            st.flush_calls, 1,
            "carrier flush runs exactly once after drain"
        );
    }
}
