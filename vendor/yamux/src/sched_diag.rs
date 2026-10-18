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
