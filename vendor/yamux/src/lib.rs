// Copyright (c) 2018-2019 Parity Technologies (UK) Ltd.
//
// Licensed under the Apache License, Version 2.0 or MIT license, at your option.
//
// A copy of the Apache License, Version 2.0 is included in the software as
// LICENSE-APACHE and a copy of the MIT license is included in the software
// as LICENSE-MIT. You may also obtain a copy of the Apache License, Version 2.0
// at https://www.apache.org/licenses/LICENSE-2.0 and a copy of the MIT license
// at https://opensource.org/licenses/MIT.

//! This crate implements the [Yamux specification][1].
//!
//! It multiplexes independent I/O streams over reliable, ordered connections,
//! such as TCP/IP.
//!
//! The two primary objects, clients of this crate interact with, are:
//!
//! - [`Connection`], which wraps the underlying I/O resource, e.g. a socket, and
//!   provides methods for opening outbound or accepting inbound streams.
//! - [`Stream`], which implements [`futures::io::AsyncRead`] and
//!   [`futures::io::AsyncWrite`].
//!
//! [1]: https://github.com/hashicorp/yamux/blob/master/spec.md

// vendor 改造:frame::io 的 staging 扩展(bytes 标准模式)需要
// advance_mut;除该处外全 crate 禁 unsafe。
#![deny(unsafe_code)]

mod chunks;
mod error;
mod frame;
pub mod pool;
pub mod sched_diag;

pub(crate) mod connection;
mod tagged_stream;

pub use crate::connection::{
    Connection, Mode, Packet, RecvHalf, SendHalf, SendWorker, ShardWorker, SplitSocket, Stream,
    StreamReadHalf, StreamWriteHalf,
};
pub use crate::error::ConnectionError;
pub use crate::frame::{
    header::{HeaderDecodeError, StreamId},
    FrameDecodeError, Sequential, Writev2,
};

const KIB: usize = 1024;
const MIB: usize = KIB * 1024;
const GIB: usize = MIB * 1024;

pub const DEFAULT_CREDIT: u32 = 256 * KIB as u32; // as per yamux specification

pub type Result<T> = std::result::Result<T, ConnectionError>;

/// The maximum number of streams we will open without an acknowledgement from the other peer.
///
/// This enables a very basic form of backpressure on the creation of streams.
const MAX_ACK_BACKLOG: usize = 256;

/// Default maximum number of bytes a Yamux data frame might carry as its
/// payload when being send. Larger Payloads will be split.
///
/// The data frame payload size is not restricted by the yamux specification.
/// Still, this implementation restricts the size to:
///
/// 1. Reduce delays sending time-sensitive frames, e.g. window updates.
/// 2. Minimize head-of-line blocking across streams.
/// 3. Enable better interleaving of send and receive operations, as each is
///    carried out atomically instead of concurrently with its respective
///    counterpart.
///
/// For details on why this concrete value was chosen, see
/// https://github.com/paritytech/yamux/issues/100.
const DEFAULT_SPLIT_SEND_SIZE: usize = 16 * KIB;

/// Yamux configuration.
///
/// The default configuration values are as follows:
///
/// - max. for the total receive window size across all streams of a connection = 1 GiB
/// - max. number of streams = 512
/// - read after close = true
/// - split send size = 16 KiB
/// - shard workers = 4
#[derive(Debug, Clone)]
pub struct Config {
    max_connection_receive_window: Option<usize>,
    max_num_streams: usize,
    read_after_close: bool,
    split_send_size: usize,
    /// 每流初始接收窗(round-E1 起为**恒定值**:窗口不自增、无预算钳制;
    /// 见 [`Config::set_initial_stream_receive_window`])。默认
    /// [`DEFAULT_CREDIT`],即 upstream 原行为。
    initial_stream_receive_window: usize,
    /// 每流**发送窗**初值(rabbithole round-59)。
    ///
    /// 修前发送窗初值在两个构造点都写死 `DEFAULT_CREDIT`(256KiB),
    /// 与本字段(接收窗)无关:`new_outbound` 传 `DEFAULT_CREDIT`,
    /// `make_new_inbound_stream` 也传 `DEFAULT_CREDIT`。于是**发送方**
    /// 每 256KiB 就要等一次对端窗口更新的往返。
    ///
    /// 实测(上行 n=16):rh 1.29 G vs frp 2.05 G,差 36%;`PUMP_UP`
    /// 显示每批只有 ~100KB(受 256KiB 窗与往返节奏钳制),而同条件
    /// **下行**每批 ~574KB、吞吐 2.41 G——差别正是下行时我们是写入
    /// 用户 socket(无 mux 窗口),上行时我们是写入 mux 流(受窗口)。
    ///
    /// 设为与 `initial_stream_receive_window` 同值(6MiB)后,发送方
    /// 首窗即 6MiB,不再每 256KiB 等一次往返。
    initial_stream_send_window: usize,
    /// 分片 Worker 数(halves 路径,round-5 §2.7)。≥1;默认 4;
    /// 未拆分路径(Active/Connection::poll_*)忽略该配置。
    shard_workers: usize,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            max_connection_receive_window: Some(GIB),
            max_num_streams: 512,
            read_after_close: true,
            split_send_size: DEFAULT_SPLIT_SEND_SIZE,
            initial_stream_receive_window: DEFAULT_CREDIT as usize,
            initial_stream_send_window: DEFAULT_CREDIT as usize,
            shard_workers: 4,
        }
    }
}

impl Config {
    /// Set the upper limit for the total receive window size across all streams of a connection.
    ///
    /// Must be `>= 256 KiB * max_num_streams` to allow each stream at least the Yamux default
    /// window size.
    ///
    /// The window of a stream starts at 256 KiB and is increased (auto-tuned) based on the
    /// connection's round-trip time and the stream's bandwidth (striving for the
    /// bandwidth-delay-product).
    ///
    /// Set to `None` to disable limit, i.e. allow each stream to grow receive window based on
    /// connection's round-trip time and stream's bandwidth without limit.
    ///
    /// ## DOS attack mitigation
    ///
    /// A remote node (attacker) might trick the local node (target) into allocating large stream
    /// receive windows, trying to make the local node run out of memory.
    ///
    /// This attack is difficult, as the local node only increases the stream receive window up to
    /// 2x the bandwidth-delay-product, where bandwidth is the amount of bytes read, not just
    /// received. In other words, the attacker has to send (and have the local node read)
    /// significant amount of bytes on a stream over a long period of time to increase the stream
    /// receive window. E.g. on a 60ms 10Gbit/s connection the bandwidth-delay-product is ~75 MiB
    /// and thus the local node will at most allocate ~150 MiB (2x bandwidth-delay-product) per
    /// stream.
    ///
    /// Despite the difficulty of the attack one should choose a reasonable
    /// `max_connection_receive_window` to protect against this attack, especially since an attacker
    /// might use more than one stream per connection.
    ///
    /// round-E1(rabbithole,2026-10-16):流控对齐 frp v0.2.0 后本字段无任何
    /// 消费者——BDP 自增窗与连接级共享预算记账已删(恒定单流窗,见
    /// `connection::stream::flow_control`);rabbithole 接线(mux.rs)设为
    /// `None`(不限,对齐 frp"无连接级共享预算")。字段为上游 API 兼容
    /// 保留;`None` 时下方 setter 相互断言对 `usize::MAX` 恒真。
    pub fn set_max_connection_receive_window(&mut self, n: Option<usize>) -> &mut Self {
        self.max_connection_receive_window = n;

        assert!(
            self.max_connection_receive_window.unwrap_or(usize::MAX)
                >= self.max_num_streams.saturating_mul(DEFAULT_CREDIT as usize),
            "`max_connection_receive_window` must be `>= 256 KiB * max_num_streams` to allow each
            stream at least the Yamux default window size"
        );

        self
    }

    /// Set the max. number of streams per connection.
    pub fn set_max_num_streams(&mut self, n: usize) -> &mut Self {
        self.max_num_streams = n;

        assert!(
            self.max_connection_receive_window.unwrap_or(usize::MAX)
                >= self.max_num_streams.saturating_mul(DEFAULT_CREDIT as usize),
            "`max_connection_receive_window` must be `>= 256 KiB * max_num_streams` to allow each
            stream at least the Yamux default window size"
        );

        self
    }

    /// Allow or disallow streams to read from buffered data after
    /// the connection has been closed.
    pub fn set_read_after_close(&mut self, b: bool) -> &mut Self {
        self.read_after_close = b;
        self
    }

    /// Set the max. payload size used when sending data frames. Payloads larger
    /// than the configured max. will be split.
    pub fn set_split_send_size(&mut self, n: usize) -> &mut Self {
        self.split_send_size = n;
        self
    }

    /// Set the initial per-stream receive window the local side grants.
    ///
    /// Wire semantics stay per yamux specification: the remote sender starts
    /// with [`DEFAULT_CREDIT`] (256 KiB) of credit. What this configures is the
    /// receiver side: a freshly created stream's `max_receive_window` starts at
    /// `n` instead of [`DEFAULT_CREDIT`], and the difference `n - 256 KiB` is
    /// granted at stream creation via a window update frame carrying the
    /// initial flag (outbound SYN|WU / inbound ACK|WU, both upstream-legal
    /// combinations; round-4/B1, aligned with frp OpenStream/AcceptStream)
    /// instead of waiting for the receiver's first read poll. round-E1
    /// (2026-10-16, frp v0.2.0 alignment) makes the window **constant**: it is
    /// never auto-tuned (no BDP growth) and not charged against any connection
    /// budget (the shared-budget accounting has been removed); read-driven
    /// window updates only replenish consumed credit.
    ///
    /// Motivation (rabbithole round-1, measured on the 3-machine testbed,
    /// `benchmark/runs/goal-repair/round-1/single-stream-medians.txt`): every
    /// mux protocol stalled single-stream at ~1.05-1.23 Gbit/s up /
    /// ~1.47-1.96 Gbit/s down regardless of TLS, while 16 concurrent streams
    /// on the same path reached 2.2-3.0 Gbit/s — consistent with a per-stream
    /// credit-loop clamp (`256 KiB / credit-loop-latency`), not a per-byte
    /// cost. Hypothesis (not yet verified on the testbed): the credit loop
    /// crossing two relay processes exceeds twice the ping RTT, so the
    /// RTT-gated auto-tuning never fires; the synthetic-RTT experiment
    /// (`goal_repair_mux.rs::mux_single_stream_credit_stall_under_synthetic_rtt`)
    /// reproduces the clamp mechanism locally.
    ///
    /// Must be `>= 256 KiB` (per-stream spec floor). Memory note: this is a
    /// credit *limit*, not an allocation; buffering only grows with data the
    /// remote actually sends and the local side has not yet consumed.
    pub fn set_initial_stream_receive_window(&mut self, n: usize) -> &mut Self {
        assert!(
            n >= DEFAULT_CREDIT as usize,
            "`initial_stream_receive_window` must be `>= 256 KiB` (yamux spec floor)"
        );
        assert!(
            n <= u32::MAX as usize,
            "`initial_stream_receive_window` must fit u32"
        );
        self.initial_stream_receive_window = n;
        self
    }

    /// 读取当前配置的每流发送窗初值。
    pub(crate) fn initial_stream_send_window(&self) -> usize {
        self.initial_stream_send_window
    }

    /// 设置每流**发送窗**初值(rabbithole round-59)。
    ///
    /// 语义:我方作为发送方时,对每条新流默认拥有的发送额度。yamux 规范
    /// 把初始额度定为 256KiB(接收方必须按需补发窗口更新);把初值抬高
    /// 等于**预授**额度,对端无需先发窗口更新即可写满该量。
    ///
    /// 约束与 `set_initial_stream_receive_window` 一致(>= 256KiB、
    /// 必须 fit u32)。两侧必须相等或对端不小于本值,否则实际生效值
    /// 由对端授信决定——抬高只是让我方在**收到对端窗口更新之前**就能
    /// 继续写,不改变协议语义(对端仍可以只授 256KiB)。
    ///
    /// 注意:这**不是**绕过流控。真正的额度仍由对端授予;本项只是把
    /// "首窗"从我方视角抬高到与接收窗对称,消除每 256KiB 一次往返。
    pub fn set_initial_stream_send_window(&mut self, n: usize) -> &mut Self {
        assert!(
            n >= DEFAULT_CREDIT as usize,
            "`initial_stream_send_window` must be `>= 256 KiB` (yamux spec floor)"
        );
        assert!(
            n <= u32::MAX as usize,
            "`initial_stream_send_window` must fit u32"
        );
        self.initial_stream_send_window = n;
        self
    }

    /// Set the number of stream-shard workers for the split-halves path
    /// (round-5 §2.7). Must be within `1..=64`; defaults to 4. The
    /// unsplit path (`Active` / `Connection::poll_*`) ignores this.
    pub fn set_shard_workers(&mut self, n: usize) -> &mut Self {
        assert!(
            (1..=64).contains(&n),
            "`shard_workers` must be within 1..=64"
        );
        self.shard_workers = n;
        self
    }
}

// Check that we can safely cast a `usize` to a `u64`.
static_assertions::const_assert! {
    std::mem::size_of::<usize>() <= std::mem::size_of::<u64>()
}

// Check that we can safely cast a `u32` to a `usize`.
static_assertions::const_assert! {
    std::mem::size_of::<u32>() <= std::mem::size_of::<usize>()
}

#[cfg(test)]
impl quickcheck::Arbitrary for Config {
    fn arbitrary(g: &mut quickcheck::Gen) -> Self {
        // quickcheck 1 的 Gen 不暴露底层 rng:用 Arbitrary 取模近似 gen_range
        // (impl 块体内被实现的 trait 已在作用域,无需再 use)
        fn gr_usize(g: &mut quickcheck::Gen, lo: usize, hi: usize) -> usize {
            if hi <= lo {
                return lo;
            }
            (u64::arbitrary(g) % ((hi - lo) as u64)) as usize + lo
        }

        let max_num_streams = gr_usize(g, 0, u16::MAX as usize);

        Config {
            max_connection_receive_window: if bool::arbitrary(g) {
                Some(gr_usize(
                    g,
                    DEFAULT_CREDIT as usize * max_num_streams,
                    usize::MAX,
                ))
            } else {
                None
            },
            max_num_streams,
            read_after_close: bool::arbitrary(g),
            split_send_size: gr_usize(g, DEFAULT_SPLIT_SEND_SIZE, usize::MAX),
            // 随机配置保持 upstream 默认初窗(预授信行为由专项测试显式构造)
            initial_stream_receive_window: DEFAULT_CREDIT as usize,
            // round-59:发送窗初值同样保持 upstream 默认
            initial_stream_send_window: DEFAULT_CREDIT as usize,
            // 分片数保持默认(分片行为由专项测试显式构造)
            shard_workers: 4,
        }
    }
}
