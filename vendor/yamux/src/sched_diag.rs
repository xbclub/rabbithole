//! round-11 诊断:测"数据就绪→任务被再次 poll"的调度延迟。
//!
//! 背景(§17.15):16 条并发流时,`pump_down` 每块读等待 2.2~5.0ms,
//! 但进程 CPU 仅 0.68 核、目标端 0.22 核。怀疑其中大部分是 tokio
//! 任务调度延迟(线程空闲但任务在队列里等)。
//!
//! 本模块记录:每次 `poll_read_chunk` 返回 Pending(park)的时刻,
//! 与下次进入 `poll_read_chunk` 的时刻之差。**注意这是"被再次 poll 的
//! 间隔",包含"数据真正到达"的时间**,因此要结合"是否真的读到数据"
//! 解读:若 park 后很快被 poll 但没数据 → 是空唤醒;若 park 后隔很久
//! 才被 poll 且有数据 → 其中含数据到达时间。
//!
//! 仅 `RH_SCHED_STATS=1` 时生效,否则 `enabled()` 返回 false 且无任何
//! 额外开销(一次 OnceLock 读)。
use std::sync::atomic::{AtomicU64, Ordering};

static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();

pub fn enabled() -> bool {
    *ENABLED.get_or_init(|| std::env::var("RH_SCHED_STATS").map(|v| v == "1").unwrap_or(false))
}

// 每线程独立的 park 时戳(避免跨流污染):thread_local 足够,因为
// 同一条流的 pump 通常在同一个 worker 上被 poll,但 tokio 会迁移任务。
// 因此用 stream 级槽位不可行;这里退化为"全局统计分布",只回答
// "park→poll 间隔的分布",不区分具体流。
thread_local! {
    static LAST_PARK: std::cell::Cell<Option<std::time::Instant>> = const { std::cell::Cell::new(None) };
}

pub static POLLS: AtomicU64 = AtomicU64::new(0);
pub static HIT_LE_10US: AtomicU64 = AtomicU64::new(0);
pub static HIT_LE_100US: AtomicU64 = AtomicU64::new(0);
pub static HIT_LE_1MS: AtomicU64 = AtomicU64::new(0);
pub static HIT_GT_1MS: AtomicU64 = AtomicU64::new(0);
pub static HIT_GT_10MS: AtomicU64 = AtomicU64::new(0);
pub static SUM_US: AtomicU64 = AtomicU64::new(0);

pub fn on_park() {
    LAST_PARK.with(|c| c.set(Some(std::time::Instant::now())));
}

pub static IN_POLL_NS: AtomicU64 = AtomicU64::new(0);

thread_local! {
    static POLL_START: std::cell::Cell<Option<std::time::Instant>> = const { std::cell::Cell::new(None) };
}

/// 记录一次"进入 poll → 返回 Ready/Pending"的耗时(即 poll 内部工作耗时)。
pub fn begin_poll() {
    POLL_START.with(|c| c.set(Some(std::time::Instant::now())));
}

pub fn end_poll() {
    POLL_START.with(|c| {
        if let Some(t) = c.take() {
            IN_POLL_NS.fetch_add(t.elapsed().as_nanos() as u64, Ordering::Relaxed);
        }
    });
}

pub fn on_poll() {
    LAST_PARK.with(|c| {
        if let Some(t) = c.take() {
            let us = t.elapsed().as_micros() as u64;
            POLLS.fetch_add(1, Ordering::Relaxed);
            SUM_US.fetch_add(us, Ordering::Relaxed);
            if us <= 10 {
                HIT_LE_10US.fetch_add(1, Ordering::Relaxed);
            } else if us <= 100 {
                HIT_LE_100US.fetch_add(1, Ordering::Relaxed);
            } else if us <= 1000 {
                HIT_LE_1MS.fetch_add(1, Ordering::Relaxed);
            } else if us <= 10000 {
                HIT_GT_1MS.fetch_add(1, Ordering::Relaxed);
            } else {
                HIT_GT_10MS.fetch_add(1, Ordering::Relaxed);
            }
        }
    });
}

/// 定时打印(由调用方在固定间隔调用)。
pub fn dump(tag: &str) {
    if !enabled() {
        return;
    }
    let p = POLLS.load(Ordering::Relaxed);
    if p == 0 {
        return;
    }
    let avg = SUM_US.load(Ordering::Relaxed) as f64 / p as f64;
    eprintln!(
        "SCHED {tag} polls={p} avg_us={avg:.0} in_poll_avg_us={:.1} <=10us={} <=100us={} <=1ms={} <=10ms={} >10ms={}",
        IN_POLL_NS.load(Ordering::Relaxed) as f64 / p as f64 / 1000.0,
        HIT_LE_10US.load(Ordering::Relaxed),
        HIT_LE_100US.load(Ordering::Relaxed),
        HIT_LE_1MS.load(Ordering::Relaxed),
        HIT_GT_1MS.load(Ordering::Relaxed),
        HIT_GT_10MS.load(Ordering::Relaxed),
    );
}

// ---- park 时窗口状态(二分判据)----
pub static PARK_WIN0: AtomicU64 = AtomicU64::new(0);   // send_window == 0
pub static PARK_WIN_LOW: AtomicU64 = AtomicU64::new(0); // 0 < w < 64KiB
pub static PARK_WIN_OK: AtomicU64 = AtomicU64::new(0);  // >= 64KiB(说明不是窗口)

pub fn on_park_with_window(w: u32) {
    if w == 0 {
        PARK_WIN0.fetch_add(1, Ordering::Relaxed);
    } else if w < 64 * 1024 {
        PARK_WIN_LOW.fetch_add(1, Ordering::Relaxed);
    } else {
        PARK_WIN_OK.fetch_add(1, Ordering::Relaxed);
    }
    on_park();
}

pub fn dump_window(tag: &str) {
    if !enabled() {
        return;
    }
    let a = PARK_WIN0.load(Ordering::Relaxed);
    let b = PARK_WIN_LOW.load(Ordering::Relaxed);
    let c = PARK_WIN_OK.load(Ordering::Relaxed);
    let t = a + b + c;
    if t == 0 {
        return;
    }
    eprintln!(
        "PARK_WIN {tag} total={t} win0={a} win_low={b} win_ok={c}"
    );
}

// ---- RecvHalf poll 统计(验证"poll 间隔 34ms"假设)----
pub static FRAMES_IN_POLL: AtomicU64 = AtomicU64::new(0);
static RECV_POLLS: AtomicU64 = AtomicU64::new(0);
static RECV_FRAMES: AtomicU64 = AtomicU64::new(0);
static RECV_PENDING: AtomicU64 = AtomicU64::new(0);
static RECV_POLL_NS: AtomicU64 = AtomicU64::new(0);
static LAST_RECV_POLL: AtomicU64 = AtomicU64::new(0); // 单调纳秒时间戳
static GAP_SUM_NS: AtomicU64 = AtomicU64::new(0);
static GAP_N: AtomicU64 = AtomicU64::new(0);
static GAP_GT_1MS: AtomicU64 = AtomicU64::new(0);

fn now_ns() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

pub fn on_recv_poll(dur: std::time::Duration, frames: u64, pending: bool) {
    RECV_POLLS.fetch_add(1, Ordering::Relaxed);
    RECV_FRAMES.fetch_add(frames, Ordering::Relaxed);
    if pending {
        RECV_PENDING.fetch_add(1, Ordering::Relaxed);
    }
    RECV_POLL_NS.fetch_add(dur.as_nanos() as u64, Ordering::Relaxed);

    let t = now_ns();
    let prev = LAST_RECV_POLL.swap(t, Ordering::Relaxed);
    if prev != 0 && t > prev {
        let gap = t - prev;
        GAP_SUM_NS.fetch_add(gap, Ordering::Relaxed);
        GAP_N.fetch_add(1, Ordering::Relaxed);
        if gap > 1_000_000 {
            GAP_GT_1MS.fetch_add(1, Ordering::Relaxed);
        }
    }
}

pub fn dump_recv(tag: &str) {
    if !enabled() {
        return;
    }
    let p = RECV_POLLS.load(Ordering::Relaxed);
    if p == 0 {
        return;
    }
    let f = RECV_FRAMES.load(Ordering::Relaxed);
    let g = GAP_N.load(Ordering::Relaxed).max(1);
    eprintln!(
        "RECV_POLL {tag} polls={p} frames={f} frames_per_poll={:.1} pending={} in_poll_us={:.1} gap_avg_us={} gap_gt_1ms={}",
        f as f64 / p as f64,
        RECV_PENDING.load(Ordering::Relaxed),
        RECV_POLL_NS.load(Ordering::Relaxed) as f64 / p as f64 / 1000.0,
        GAP_SUM_NS.load(Ordering::Relaxed) / g / 1000,
        GAP_GT_1MS.load(Ordering::Relaxed)
    );
}

// ---- FlushTask 批处理统计 ----
pub static FILL_CALLS: AtomicU64 = AtomicU64::new(0);
pub static FILL_FRAMES: AtomicU64 = AtomicU64::new(0);
pub static FILL_BYTES: AtomicU64 = AtomicU64::new(0);
pub static FILL_NS: AtomicU64 = AtomicU64::new(0);
pub static FILL_ZERO: AtomicU64 = AtomicU64::new(0);

pub fn on_fill_batch(frames: usize, bytes: usize, dur: std::time::Duration) {
    FILL_CALLS.fetch_add(1, Ordering::Relaxed);
    if frames == 0 {
        FILL_ZERO.fetch_add(1, Ordering::Relaxed);
    }
    FILL_FRAMES.fetch_add(frames as u64, Ordering::Relaxed);
    FILL_BYTES.fetch_add(bytes as u64, Ordering::Relaxed);
    FILL_NS.fetch_add(dur.as_nanos() as u64, Ordering::Relaxed);
}

pub fn dump_fill(tag: &str) {
    if !enabled() {
        return;
    }
    let c = FILL_CALLS.load(Ordering::Relaxed);
    if c == 0 {
        return;
    }
    eprintln!(
        "FILL {tag} calls={c} zero={} frames_per_call={:.2} bytes_per_call={:.0} ns_per_call={:.0}",
        FILL_ZERO.load(Ordering::Relaxed),
        FILL_FRAMES.load(Ordering::Relaxed) as f64 / c as f64,
        FILL_BYTES.load(Ordering::Relaxed) as f64 / c as f64,
        FILL_NS.load(Ordering::Relaxed) as f64 / c as f64
    );
}

// ---- 帧大小分布 ----
pub static FRAME_TOTAL: AtomicU64 = AtomicU64::new(0);
pub static FRAME_LT1K: AtomicU64 = AtomicU64::new(0);
pub static FRAME_LT8K: AtomicU64 = AtomicU64::new(0);
pub static FRAME_LT32K: AtomicU64 = AtomicU64::new(0);
pub static FRAME_LT64K: AtomicU64 = AtomicU64::new(0);
pub static FRAME_GE64K: AtomicU64 = AtomicU64::new(0);
pub static FRAME_SUM_BYTES: AtomicU64 = AtomicU64::new(0);

pub fn on_frame_size(n: u64) {
    FRAME_TOTAL.fetch_add(1, Ordering::Relaxed);
    FRAME_SUM_BYTES.fetch_add(n, Ordering::Relaxed);
    if n < 1024 {
        FRAME_LT1K.fetch_add(1, Ordering::Relaxed);
    } else if n < 8192 {
        FRAME_LT8K.fetch_add(1, Ordering::Relaxed);
    } else if n < 32768 {
        FRAME_LT32K.fetch_add(1, Ordering::Relaxed);
    } else if n < 65536 {
        FRAME_LT64K.fetch_add(1, Ordering::Relaxed);
    } else {
        FRAME_GE64K.fetch_add(1, Ordering::Relaxed);
    }
}

pub fn dump_frame_size(tag: &str) {
    if !enabled() {
        return;
    }
    let t = FRAME_TOTAL.load(Ordering::Relaxed);
    if t == 0 {
        return;
    }
    eprintln!(
        "FRAMESIZE {tag} total={t} avg={:.0} <1K={} <8K={} <32K={} <64K={} >=64K={}",
        FRAME_SUM_BYTES.load(Ordering::Relaxed) as f64 / t as f64,
        FRAME_LT1K.load(Ordering::Relaxed),
        FRAME_LT8K.load(Ordering::Relaxed),
        FRAME_LT32K.load(Ordering::Relaxed),
        FRAME_LT64K.load(Ordering::Relaxed),
        FRAME_GE64K.load(Ordering::Relaxed)
    );
}

// ---- 帧类型分布 ----
pub static TAG_DATA: AtomicU64 = AtomicU64::new(0);
pub static TAG_WU: AtomicU64 = AtomicU64::new(0);
pub static TAG_PING: AtomicU64 = AtomicU64::new(0);
pub static TAG_GOAWAY: AtomicU64 = AtomicU64::new(0);

pub fn on_frame_tag(tag: u8) {
    match tag {
        0 => { TAG_DATA.fetch_add(1, Ordering::Relaxed); }
        1 => { TAG_WU.fetch_add(1, Ordering::Relaxed); }
        2 => { TAG_PING.fetch_add(1, Ordering::Relaxed); }
        _ => { TAG_GOAWAY.fetch_add(1, Ordering::Relaxed); }
    }
}

pub fn dump_tags(tag: &str) {
    if !enabled() {
        return;
    }
    eprintln!(
        "FRAMETAG {tag} data={} wu={} ping={} goaway={}",
        TAG_DATA.load(Ordering::Relaxed),
        TAG_WU.load(Ordering::Relaxed),
        TAG_PING.load(Ordering::Relaxed),
        TAG_GOAWAY.load(Ordering::Relaxed)
    );
}

// ---- WU credit 分布 ----
pub static WU_ZERO: AtomicU64 = AtomicU64::new(0);
pub static WU_LT64K: AtomicU64 = AtomicU64::new(0);
pub static WU_LT1M: AtomicU64 = AtomicU64::new(0);
pub static WU_GE1M: AtomicU64 = AtomicU64::new(0);
pub static WU_SUM: AtomicU64 = AtomicU64::new(0);

pub fn on_wu_credit(c: u64) {
    WU_SUM.fetch_add(c, Ordering::Relaxed);
    if c == 0 {
        WU_ZERO.fetch_add(1, Ordering::Relaxed);
    } else if c < 64 * 1024 {
        WU_LT64K.fetch_add(1, Ordering::Relaxed);
    } else if c < 1024 * 1024 {
        WU_LT1M.fetch_add(1, Ordering::Relaxed);
    } else {
        WU_GE1M.fetch_add(1, Ordering::Relaxed);
    }
}

pub fn dump_wu(tag: &str) {
    if !enabled() {
        return;
    }
    eprintln!(
        "WUCREDIT {tag} zero={} <64K={} <1M={} >=1M={} sum={}MB",
        WU_ZERO.load(Ordering::Relaxed),
        WU_LT64K.load(Ordering::Relaxed),
        WU_LT1M.load(Ordering::Relaxed),
        WU_GE1M.load(Ordering::Relaxed),
        WU_SUM.load(Ordering::Relaxed) / 1024 / 1024
    );
}

// ---- WU 路径分布 ----
pub static WUP_CREDIT: AtomicU64 = AtomicU64::new(0);   // 有 credit
pub static WUP_ACK0: AtomicU64 = AtomicU64::new(0);     // 0-credit + ACK
pub static WUP_ACK0_NOACK: AtomicU64 = AtomicU64::new(0); // 0-credit 无 ACK(异常)

pub fn on_wu_path(has_credit: bool, ack: bool) {
    if has_credit {
        WUP_CREDIT.fetch_add(1, Ordering::Relaxed);
    } else if ack {
        WUP_ACK0.fetch_add(1, Ordering::Relaxed);
    } else {
        WUP_ACK0_NOACK.fetch_add(1, Ordering::Relaxed);
    }
}

pub fn dump_wu_path(tag: &str) {
    if !enabled() {
        return;
    }
    eprintln!(
        "WUPATH {tag} credit={} ack0={} zero_noack={}",
        WUP_CREDIT.load(Ordering::Relaxed),
        WUP_ACK0.load(Ordering::Relaxed),
        WUP_ACK0_NOACK.load(Ordering::Relaxed)
    );
}

// ---- to_send 压入的帧类型 ----
pub static PUSH_DATA: AtomicU64 = AtomicU64::new(0);
pub static PUSH_WU: AtomicU64 = AtomicU64::new(0);
pub static PUSH_PING: AtomicU64 = AtomicU64::new(0);
pub static PUSH_OTHER: AtomicU64 = AtomicU64::new(0);

pub fn on_push_to_send(tag: u8) {
    match tag {
        0 => { PUSH_DATA.fetch_add(1, Ordering::Relaxed); }
        1 => { PUSH_WU.fetch_add(1, Ordering::Relaxed); }
        2 => { PUSH_PING.fetch_add(1, Ordering::Relaxed); }
        _ => { PUSH_OTHER.fetch_add(1, Ordering::Relaxed); }
    }
}

pub fn dump_push(tag: &str) {
    if !enabled() {
        return;
    }
    eprintln!(
        "PUSH {tag} data={} wu={} ping={} other={}",
        PUSH_DATA.load(Ordering::Relaxed),
        PUSH_WU.load(Ordering::Relaxed),
        PUSH_PING.load(Ordering::Relaxed),
        PUSH_OTHER.load(Ordering::Relaxed)
    );
}

// ---- WU 调用栈追踪(仅前 3 次) ----
static WU_TRACED: AtomicU64 = AtomicU64::new(0);

pub fn trace_wu() -> bool {
    WU_TRACED.fetch_add(1, Ordering::Relaxed) < 3
}

pub fn no_wu() -> bool {
    static V: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *V.get_or_init(|| std::env::var("RH_NO_WU").map(|v| v == "1").unwrap_or(false))
}
