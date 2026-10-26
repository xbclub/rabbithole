//! Linux splice(2) 零拷贝中继:socket → pipe → socket,数据不经用户态。
//!
//! 适用条件(调用方判定):两端均为裸 tokio TCP 流——无 TLS/WS/yamux/压缩包装
//! (multiplex=false 且明文 tcp 载体时的典型形态)。实现方式是对两端 socket
//! 各 `dup` 一个独立 fd,就绪事件经 [`AsyncFd`] 注册,原 tokio 流全程不动,
//! 因此任一步资源失败(dup/pipe)都可原样回落用户态拷贝路径。
//!
//! 语义与用户态拷贝路径对齐:
//! * splice 返回 0(读 EOF)→ shutdown 对端写侧(半关闭传递);
//! * 限速按返回字节扣减令牌,令牌不足时管道(有界)充当节流缓冲;
//! * abort(流量封禁)→ 双向 SHUT_RDWR 模拟断连,唤醒对向泵。

use crate::BoxedIo;
use pin_project_lite::pin_project;
use std::future::Future;
use std::io;
use std::os::fd::{AsRawFd, FromRawFd, IntoRawFd, OwnedFd};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::io::unix::AsyncFd;

const SPLICE_F_MOVE: u32 = 1;
const SPLICE_F_NONBLOCK: u32 = 2;
/// 管道目标容量:一次 readable 事件尽量搬空 socket 接收缓冲。
/// 页按需分配(空闲连接不占内存);F_SETPIPE_SZ 失败则回落默认 64KiB。
/// 实验记录:64KiB 与 256KiB 在三机串联链路上吞吐无差(均衡点由拓扑决定),
/// 取 256KiB 降低唤醒频次。
const PIPE_SIZE: i32 = 256 * 1024;
/// 单次排空上限:控制注入对端 socket 的突发尺寸,同时足够大以维持
/// 发送队列不空转(过小会 app-limit 饿住内核发送速率)。
const DRAIN_SLICE: usize = 256 * 1024;
/// 未发送数据低水位:writable 仅在 unsent < 低水位时唤醒。取值需显著
/// 大于 BDP(过小会把发送队列饿成 app-limited,实测 128KiB 时单流被
/// 卡在 ~2G);1MiB 在 0.4ms RTT 链路上足以维持满窗口。
/// 内核不支持时忽略。可用 RH_NOTSENT_LOWAT 覆盖(字节数,0 = 不设置;
/// C2 低水位矩阵实验用)。
const NOTSENT_LOWAT: u32 = 1024 * 1024;

fn notsent_lowat() -> u32 {
    static V: std::sync::OnceLock<u32> = std::sync::OnceLock::new();
    *V.get_or_init(|| {
        std::env::var("RH_NOTSENT_LOWAT")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(NOTSENT_LOWAT)
    })
}

/// 每批字节预算(G1 修正 2 新增不变量):tokio `AsyncFd::readable/writable`
/// 的 async 路径不消耗协作预算(实测 1.53.2 `async_fd.rs` 全文无
/// `poll_proceed`),两端持续就绪时泵可在单 worker 上无限期霸占执行。
/// 批次化泵(`pump_batched`)在**无真实等待的连续搬运段**内每搬运该
/// 字节数后 `tokio::task::yield_now()` 显式让步一次——不用
/// `consume_budget()`:它只在任务预算(默认 128)耗尽时才让步,而本泵
/// 每批仅调用一次,预算永不耗尽,让步不会发生(设计期自查发现)。
/// yield_now 是确定性让步:每批至多一次调度切换,4MiB = 16 块 @
/// 256KiB,开销相对搬运量可忽略;「批」以真实等待(EAGAIN)为界,
/// 低负载小包场景每批 2 ops 远不触预算,不引入额外让步延迟。
const BATCH_BYTES: usize = 4 * 1024 * 1024;
/// 每批 splice 操作数预算(小包/小块场景按操作数兜底,字节预算不触发)。
const BATCH_OPS: u32 = 64;

/// 单泵本地计数(G1-A 最小观测;计划 §A「每泵本地累计、结束时输出,
/// 默认不影响热路径」):每泵私有实例,泵任务独占写,AtomicU64 仅为
/// 结束后的外部读取(测试断言/诊断输出)服务——不存在跨泵争用点,
/// 频率为每搬运块常数次(≈5 次/256KiB,远低于一次 splice syscall),
/// 不构成「每包全局原子」热点。
///
/// 生产默认(`RH_SPLICE_STATS` 未设/空/"0")泵**不持有**计数:不分配
/// Arc、热循环不执行任何统计原子/Instant/输出(见 [`splice_relay`]);
/// 以下字段仅在统计开启(计数句柄注入)时被写。
///
/// 语义对齐 example-contract(fixed-ab-v1)的 counters 字段:
/// * `pending_waits_*`:**EAGAIN 观察后**经 `guard.clear_ready()` 的
///   等待尝试次数——**旧口径(尝试),保留历史可比性**;真实挂起由
///   下方 v2 字段(pending_polls_/wait_cycles_/wait_ns_)区分,不写入
///   本字段。legacy 臂的首次初始等待(无 EAGAIN 前置)不在该口径内。
/// * `eagain_fill_input_empty` / `eagain_drain_pipe_full` /
///   `eagain_drain_write_full`:按 EAGAIN 的**实际来源方向**分开计
///   (G1 复核修正 3)。单块管线深度(fill 时刻管道必空)下
///   `eagain_drain_pipe_full` 结构性不出现(恒 0);引入多块管线时
///   必须先补「管道满 → 转 drain」的路由再使用该计数。
///
/// v2 真实挂起观测(口径):
/// * `pending_polls_*`:poll 实际返回 `Poll::Pending` 的次数(同周期
///   多次 Pending 各计 1;poll 数 ≠ 周期数 ≠ 内核 wakeup);
/// * `wait_cycles_*`:「至少一次 Pending」的周期数(首次 Pending 计 1,
///   Ready 立返不计);
/// * `wait_ns_*`:各周期「首次 Pending → Ready/取消/Drop」累计纳秒
///   (时钟可注入;Drop 结算不遗失);
/// * `*_min/max_bytes`:成功 fill/drain 单次字节数(块分布;无成功
///   调用输出 null,不以 0 冒充)。
pub(crate) struct PumpCounters {
    pub fill_calls: AtomicU64,
    pub fill_bytes: AtomicU64,
    pub drain_calls: AtomicU64,
    pub drain_bytes: AtomicU64,
    pub eagain_fill_input_empty: AtomicU64,
    pub eagain_drain_pipe_full: AtomicU64,
    pub eagain_drain_write_full: AtomicU64,
    pub pending_waits_read: AtomicU64,
    pub pending_waits_write: AtomicU64,
    pub coop_yields: AtomicU64,
    pub pending_polls_read: AtomicU64,
    pub pending_polls_write: AtomicU64,
    pub wait_cycles_read: AtomicU64,
    pub wait_cycles_write: AtomicU64,
    pub wait_ns_read: AtomicU64,
    pub wait_ns_write: AtomicU64,
    pub fill_min_bytes: AtomicU64,
    pub fill_max_bytes: AtomicU64,
    pub drain_min_bytes: AtomicU64,
    pub drain_max_bytes: AtomicU64,
}

impl Default for PumpCounters {
    fn default() -> Self {
        Self {
            fill_calls: AtomicU64::new(0),
            fill_bytes: AtomicU64::new(0),
            drain_calls: AtomicU64::new(0),
            drain_bytes: AtomicU64::new(0),
            eagain_fill_input_empty: AtomicU64::new(0),
            eagain_drain_pipe_full: AtomicU64::new(0),
            eagain_drain_write_full: AtomicU64::new(0),
            pending_waits_read: AtomicU64::new(0),
            pending_waits_write: AtomicU64::new(0),
            coop_yields: AtomicU64::new(0),
            pending_polls_read: AtomicU64::new(0),
            pending_polls_write: AtomicU64::new(0),
            wait_cycles_read: AtomicU64::new(0),
            wait_cycles_write: AtomicU64::new(0),
            wait_ns_read: AtomicU64::new(0),
            wait_ns_write: AtomicU64::new(0),
            // min 哨兵 = u64::MAX(0 会让 fetch_min 永远停在 0);
            // 无成功调用时快照输出 null,哨兵值不外泄
            fill_min_bytes: AtomicU64::new(u64::MAX),
            fill_max_bytes: AtomicU64::new(0),
            drain_min_bytes: AtomicU64::new(u64::MAX),
            drain_max_bytes: AtomicU64::new(0),
        }
    }
}

/// v2 结束记录的非计数字段(relay/pump 建立时采样一次)。
struct RelayMeta {
    mode: PumpMode,
    dir: &'static str,
    /// 泵起始时刻(ns,相对 relay 共享时钟锚点;up/down 可关联)。
    start_ns: u64,
    /// relay 建立时刻墙钟(UNIX 微秒);与 duration_ns 配对,把
    /// start_ns/duration 对齐到 iperf epoch 窗口。未采样 → null。
    started_unix_us: Option<u64>,
    /// 源 socket(泵输入)本地/对端地址;采样失败 → None(JSON null)。
    src_local: Option<String>,
    src_peer: Option<String>,
    dst_local: Option<String>,
    dst_peer: Option<String>,
}

impl PumpCounters {
    /// 单行严格 JSON(schema v2):计数快照 + 可关联元数据(模式/方向/
    /// 单调时间区间/四元组地址)。沿既有 stderr 单行输出形态。
    fn snapshot_json(&self, m: &RelayMeta, end_ns: u64) -> String {
        let fc = self.fill_calls.load(Ordering::Relaxed);
        let dc = self.drain_calls.load(Ordering::Relaxed);
        // 无成功调用 → null(未观测,不以 0 冒充)
        let block = |calls: u64, v: u64| -> serde_json::Value {
            if calls == 0 {
                serde_json::Value::Null
            } else {
                v.into()
            }
        };
        serde_json::json!({
            "splice_stats": "v2",
            "dir": m.dir,
            "pump_mode": match m.mode {
                PumpMode::Legacy => "legacy",
                PumpMode::Batched => "batched",
            },
            "start_ns": m.start_ns,
            "started_unix_us": m.started_unix_us,
            "duration_ns": end_ns.saturating_sub(m.start_ns),
            "src_local": m.src_local.as_deref(),
            "src_peer": m.src_peer.as_deref(),
            "dst_local": m.dst_local.as_deref(),
            "dst_peer": m.dst_peer.as_deref(),
            "fill_calls": fc,
            "fill_bytes": self.fill_bytes.load(Ordering::Relaxed),
            "fill_min_bytes": block(fc, self.fill_min_bytes.load(Ordering::Relaxed)),
            "fill_max_bytes": block(fc, self.fill_max_bytes.load(Ordering::Relaxed)),
            "drain_calls": dc,
            "drain_bytes": self.drain_bytes.load(Ordering::Relaxed),
            "drain_min_bytes": block(dc, self.drain_min_bytes.load(Ordering::Relaxed)),
            "drain_max_bytes": block(dc, self.drain_max_bytes.load(Ordering::Relaxed)),
            "eagain_fill_input_empty": self.eagain_fill_input_empty.load(Ordering::Relaxed),
            "eagain_drain_pipe_full": self.eagain_drain_pipe_full.load(Ordering::Relaxed),
            "eagain_drain_write_full": self.eagain_drain_write_full.load(Ordering::Relaxed),
            "pending_waits_read": self.pending_waits_read.load(Ordering::Relaxed),
            "pending_waits_write": self.pending_waits_write.load(Ordering::Relaxed),
            "pending_polls_read": self.pending_polls_read.load(Ordering::Relaxed),
            "pending_polls_write": self.pending_polls_write.load(Ordering::Relaxed),
            "wait_cycles_read": self.wait_cycles_read.load(Ordering::Relaxed),
            "wait_cycles_write": self.wait_cycles_write.load(Ordering::Relaxed),
            "wait_ns_read": self.wait_ns_read.load(Ordering::Relaxed),
            "wait_ns_write": self.wait_ns_write.load(Ordering::Relaxed),
            "coop_yields": self.coop_yields.load(Ordering::Relaxed),
        })
        .to_string()
    }
}

/// 开关解析(纯函数,可测):未设置/空/"0" 视为关闭,其余任意值开启。
fn stats_enabled_value(v: Option<&str>) -> bool {
    match v {
        Some(s) => !s.is_empty() && s != "0",
        None => false,
    }
}

/// `RH_SPLICE_STATS`:生产入口初始化读一次决定是否装配统计,热循环
/// 不再读 env。开启时泵退出(含 panic unwind)输出 v2 单行 JSON。
/// B-4:开关本身也经 OnceLock 缓存(修前每次建 relay 读一次进程
/// 环境锁 + String 分配)。
fn stats_enabled() -> bool {
    static V: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *V.get_or_init(|| stats_enabled_value(std::env::var("RH_SPLICE_STATS").ok().as_deref()))
}

/// 等待时长时钟(单调纳秒差值;可注入,测试用受控时钟)。
trait WaitClock: Send + Sync {
    fn now_ns(&self) -> u64;
}

/// 生产单调时钟:锚点 = relay 初始化,两泵共享。
struct MonoClock {
    anchor: std::time::Instant,
}

impl MonoClock {
    fn new() -> Self {
        Self {
            anchor: std::time::Instant::now(),
        }
    }
}

impl WaitClock for MonoClock {
    fn now_ns(&self) -> u64 {
        self.anchor.elapsed().as_nanos() as u64
    }
}

/// readiness 等待方向(选择 v2 计数字段)。
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum WaitDir {
    Read,
    Write,
}

/// 泵观测装配:counters/clock/started 同 Some(开启)或同 None(默认)。
#[derive(Clone, Default)]
struct PumpObs {
    counters: Option<Arc<PumpCounters>>,
    clock: Option<Arc<dyn WaitClock>>,
    /// relay 建立墙钟(UNIX 微秒):开启时采一次,关不读 SystemTime。
    started_unix_us: Option<u64>,
}

/// 建立时刻墙钟(UNIX 微秒);时钟不可用时 None(JSON null,不冒充)。
fn unix_now_us() -> Option<u64> {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .map(|d| d.as_micros() as u64)
}

/// 统计开启才执行的原子自增(关闭 = 零原子操作,仅 Option 判空分支)。
macro_rules! stat_bump {
    ($obs:expr, $field:ident, $v:expr) => {
        if let Some(s) = &$obs.counters {
            s.$field.fetch_add($v as u64, Ordering::Relaxed);
        }
    };
}

// readiness 等待观测包装(v2 口径见 [`PumpCounters`];仅统计开启时装配)。
pin_project! {
    struct ReadinessWatch<F> {
        #[pin]
        fut: F,
        c: Arc<PumpCounters>,
        dir: WaitDir,
        clock: Arc<dyn WaitClock>,
        first_pending_ns: Option<u64>,
    }
    impl<F> PinnedDrop for ReadinessWatch<F> {
        fn drop(this: Pin<&mut Self>) {
            let p = this.project();
            settle_wait(p.c, *p.dir, p.clock.as_ref(), p.first_pending_ns.take());
        }
    }
}

/// 幂等结算一个等待周期(已被 take → no-op):结束 − 首次 Pending,
/// 累计入 wait_ns_*。
fn settle_wait(
    c: &PumpCounters,
    dir: WaitDir,
    clock: &dyn WaitClock,
    first_pending_ns: Option<u64>,
) {
    if let Some(first) = first_pending_ns {
        let ns = clock.now_ns().saturating_sub(first);
        match dir {
            WaitDir::Read => &c.wait_ns_read,
            WaitDir::Write => &c.wait_ns_write,
        }
        .fetch_add(ns, Ordering::Relaxed);
    }
}

impl<F> ReadinessWatch<F> {
    fn new(fut: F, c: Arc<PumpCounters>, dir: WaitDir, clock: Arc<dyn WaitClock>) -> Self {
        Self {
            fut,
            c,
            dir,
            clock,
            first_pending_ns: None,
        }
    }
}

impl<F: Future> Future for ReadinessWatch<F> {
    type Output = F::Output;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<F::Output> {
        let mut this = self.project();
        match this.fut.as_mut().poll(cx) {
            Poll::Pending => {
                let now_ns = this.clock.now_ns();
                match this.dir {
                    WaitDir::Read => {
                        this.c.pending_polls_read.fetch_add(1, Ordering::Relaxed);
                    }
                    WaitDir::Write => {
                        this.c.pending_polls_write.fetch_add(1, Ordering::Relaxed);
                    }
                }
                if this.first_pending_ns.is_none() {
                    *this.first_pending_ns = Some(now_ns);
                    match this.dir {
                        WaitDir::Read => {
                            this.c.wait_cycles_read.fetch_add(1, Ordering::Relaxed);
                        }
                        WaitDir::Write => {
                            this.c.wait_cycles_write.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
                Poll::Pending
            }
            r @ Poll::Ready(_) => {
                settle_wait(
                    this.c,
                    *this.dir,
                    this.clock.as_ref(),
                    this.first_pending_ns.take(),
                );
                r
            }
        }
    }
}

pin_project! {
    /// Off = 统计关闭原样转发(零计数、无额外 await,取消行为不变);
    /// On = 观测。两臂同口径。
    #[project = WatchedProj]
    enum Watched<F> {
        Off {
            #[pin]
            fut: F,
        },
        On {
            #[pin]
            watch: ReadinessWatch<F>,
        },
    }
}

impl<F: Future> Future for Watched<F> {
    type Output = F::Output;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<F::Output> {
        match self.project() {
            WatchedProj::Off { fut } => fut.poll(cx),
            WatchedProj::On { watch } => watch.poll(cx),
        }
    }
}

/// 就绪等待包装点:开启 → On 观测;否则 Off 原样透传。
fn watch_ready<F: Future>(obs: &PumpObs, fut: F, dir: WaitDir) -> Watched<F> {
    match (&obs.counters, &obs.clock) {
        (Some(c), Some(k)) => Watched::On {
            watch: ReadinessWatch::new(fut, c.clone(), dir, k.clone()),
        },
        _ => Watched::Off { fut },
    }
}

/// 泵退出的计数输出守卫:统计开启时任何 return/panic 路径经 Drop 输出
/// 一行 v2 JSON(stderr);地址/时间建立采一次、结束采一次。
struct StatsEmitter {
    c: Arc<PumpCounters>,
    meta: RelayMeta,
    clock: Arc<dyn WaitClock>,
}

/// socket 四元组采样(本地/对端各一次;失败 → None,JSON null)。
fn sock_addrs(s: &SpliceSock) -> (Option<String>, Option<String>) {
    let r = socket2::SockRef::from(s.fd.get_ref());
    let addr = |f: fn(&socket2::SockRef) -> io::Result<socket2::SockAddr>| {
        f(&r)
            .ok()
            .and_then(|a| a.as_socket())
            .map(|a| a.to_string())
    };
    (addr(|r| r.local_addr()), addr(|r| r.peer_addr()))
}

impl StatsEmitter {
    fn new(
        c: Arc<PumpCounters>,
        mode: PumpMode,
        dir: &'static str,
        started_unix_us: Option<u64>,
        in_sock: &SpliceSock,
        out_sock: &SpliceSock,
        clock: Arc<dyn WaitClock>,
    ) -> Self {
        let (src_local, src_peer) = sock_addrs(in_sock);
        let (dst_local, dst_peer) = sock_addrs(out_sock);
        Self {
            c,
            meta: RelayMeta {
                mode,
                dir,
                start_ns: clock.now_ns(),
                started_unix_us,
                src_local,
                src_peer,
                dst_local,
                dst_peer,
            },
            clock,
        }
    }
}

impl Drop for StatsEmitter {
    fn drop(&mut self) {
        eprintln!("{}", self.c.snapshot_json(&self.meta, self.clock.now_ns()));
    }
}

/// 统计开启时装配 v2 输出守卫(泵顶部调用一次)。
fn emitter_for(
    obs: &PumpObs,
    mode: PumpMode,
    dir: &'static str,
    in_sock: &SpliceSock,
    out_sock: &SpliceSock,
) -> Option<StatsEmitter> {
    match (&obs.counters, &obs.clock) {
        (Some(c), Some(k)) => Some(StatsEmitter::new(
            c.clone(),
            mode,
            dir,
            obs.started_unix_us,
            in_sock,
            out_sock,
            k.clone(),
        )),
        _ => None,
    }
}

/// 泵实现选择:默认 `Batched`(非阻塞快试 + 仅 EAGAIN 后等待 + 有界
/// 批次预算/让步);`RH_SPLICE_PUMP=legacy` 临时切回旧控制流作同树
/// 同计数对照(本地归因用;最终获益机制以默认路径交付,不以开关偷换
/// 目标——fixed-ab/三机验收均在无 env 下走默认臂)。
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum PumpMode {
    Legacy,
    Batched,
}

fn pump_mode_from_env() -> PumpMode {
    // B-4:进程级缓存(OnceLock,同 notsent_lowat 先例)——修前每次建
    // relay 各取一次进程环境锁 + String 分配。全仓无测试改写该变量,
    // 缓存不破坏测试策略(g1_splice_bench 在 main 起点设置,先于首建)。
    static V: std::sync::OnceLock<PumpMode> = std::sync::OnceLock::new();
    *V.get_or_init(|| match std::env::var("RH_SPLICE_PUMP").as_deref() {
        Ok("legacy") => PumpMode::Legacy,
        _ => PumpMode::Batched,
    })
}

/// 限速:预留式取消安全接口(goal-repair A5;适配 TokenBucket 等)。
/// reserve(n) 等待凑足 n 额度;等待期间 future 被 drop 不丢已扣令牌。
pub use crate::ChunkLimiter;

/// 取消信号(共享,lib 定义):完成即双向断连。用于规则禁用/删除等
/// 需要立即拆除在途转发的场景;两个方向的泵各自持有一份同时等待。
pub use crate::CancelFuture;

/// 全部 relay 中观测到的最小管道容量(字节;0 = 尚未建立过 relay)。
/// 高并发触及 fs.pipe-user-pages-soft 时内核会静默缩小新管道,
/// 该值用于诊断容量退化(如回落到 8KiB)。
pub static PIPE_CAP_MIN: AtomicUsize = AtomicUsize::new(0);

static PIPE_CAP_WARNED: AtomicBool = AtomicBool::new(false);

/// 观测到管道实际容量 actual 时更新最小值(goal-repair A7:compare_exchange
/// 循环实现真原子最小——修前 load+条件 store 在并发观测 8KiB/256KiB 时
/// 可把 256KiB 留作"最小值")。
fn update_pipe_cap_min(actual: usize) {
    let mut cur = PIPE_CAP_MIN.load(Ordering::Relaxed);
    loop {
        if cur != 0 && actual >= cur {
            return;
        }
        match PIPE_CAP_MIN.compare_exchange_weak(cur, actual, Ordering::Relaxed, Ordering::Relaxed)
        {
            Ok(_) => return,
            Err(c) => cur = c,
        }
    }
}

/// 单方向钩子:计量 + 限速。
#[derive(Clone, Default)]
pub struct DirHook {
    /// 每实际搬运 n 字节回调一次(计量)。
    /// meter_on_fill=true:fill(搬入管道)完成即计——up 读侧口径,与
    /// copy 路径 CountedUserIo 读侧计数点位一致(先计量、后限速等待);
    /// false:排空送达后计——down 送达口径(与 copy 写侧成功计数一致)。
    pub on_bytes: Option<Arc<dyn Fn(u64) + Send + Sync>>,
    pub meter_on_fill: bool,
    /// 限速(预留式,取消安全):up=fill 后预留即消费;down=预留后按
    /// 实际送达消费,取消/部分送达退还未消费预留。
    pub limiter: Option<Arc<dyn ChunkLimiter>>,
}

/// BoxedIo 是否裸 tokio TCP(可 splice);TLS/WS/yamux/压缩包装均返回 None。
pub fn plain_tcp(io: &BoxedIo) -> Option<&tokio::net::TcpStream> {
    io.as_any().downcast_ref::<tokio::net::TcpStream>()
}

#[derive(Clone, Default)]
pub struct SpliceHooks {
    /// a→b 方向(服务端语义:user→carrier)。
    pub up: Option<DirHook>,
    /// b→a 方向(carrier→user)。
    pub down: Option<DirHook>,
    /// 返回 true → 双向断连(流量封禁)。
    pub abort: Option<Arc<dyn Fn() -> bool + Send + Sync>>,
    /// 取消(规则禁用/删除):完成即双向断连并结束两个泵,
    /// 包括阻塞在就绪等待中的泵(abort 闭包只在块边界被检查)。
    pub cancel: Option<CancelFuture>,
}

struct Pipe {
    r: OwnedFd,
    w: OwnedFd,
}

impl Pipe {
    fn new() -> io::Result<Self> {
        let mut fds = [0i32; 2];
        if unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_NONBLOCK) } != 0 {
            return Err(io::Error::last_os_error());
        }
        let p = Pipe {
            r: unsafe { OwnedFd::from_raw_fd(fds[0]) },
            w: unsafe { OwnedFd::from_raw_fd(fds[1]) },
        };
        // 扩容失败不影响正确性,但必须知道实际容量:触及
        // fs.pipe-user-pages-soft 后内核会静默缩小管道(批量并发场景),
        // 记录最小观测值供诊断(A7:原子最小);首次退化告警一次。
        unsafe {
            if libc::fcntl(p.r.as_raw_fd(), libc::F_SETPIPE_SZ, PIPE_SIZE) < 0 {
                let e = io::Error::last_os_error();
                tracing::debug!(error = %e, "F_SETPIPE_SZ failed");
            }
            let actual = libc::fcntl(p.r.as_raw_fd(), libc::F_GETPIPE_SZ);
            if actual >= 0 {
                update_pipe_cap_min(actual as usize);
                if (actual as i32) < PIPE_SIZE && !PIPE_CAP_WARNED.swap(true, Ordering::Relaxed) {
                    tracing::warn!(
                        requested = PIPE_SIZE,
                        actual,
                        min_observed = PIPE_CAP_MIN.load(Ordering::Relaxed),
                        "pipe capacity below target (fs.pipe-user-pages-soft?)"
                    );
                }
            }
        }
        Ok(p)
    }
}

fn splice_sys(fd_in: i32, fd_out: i32, len: usize) -> io::Result<usize> {
    let n = unsafe {
        libc::splice(
            fd_in,
            std::ptr::null_mut(),
            fd_out,
            std::ptr::null_mut(),
            len,
            SPLICE_F_MOVE | SPLICE_F_NONBLOCK,
        )
    };
    if n < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(n as usize)
    }
}

struct SpliceSock {
    /// 与原 tokio 流共享同一 socket 的独立 fd:就绪事件走 AsyncFd,
    /// 原 fd 的注册/生命周期不受影响。
    fd: Arc<AsyncFd<OwnedFd>>,
}

impl SpliceSock {
    fn dup(stream: &tokio::net::TcpStream) -> io::Result<Self> {
        let dup = socket2::SockRef::from(stream).try_clone()?;
        let fd = unsafe { OwnedFd::from_raw_fd(dup.into_raw_fd()) };
        // 作用于共享 socket 本体;失败(旧内核)静默忽略;0 = 不设置
        let v = notsent_lowat();
        if v > 0 {
            unsafe {
                libc::setsockopt(
                    fd.as_raw_fd(),
                    libc::IPPROTO_TCP,
                    libc::TCP_NOTSENT_LOWAT,
                    &v as *const u32 as *const libc::c_void,
                    std::mem::size_of::<u32>() as libc::socklen_t,
                );
            }
        }
        Ok(Self {
            fd: Arc::new(AsyncFd::new(fd)?),
        })
    }

    fn raw_fd(&self) -> i32 {
        self.fd.get_ref().as_raw_fd()
    }
}

fn shutdown_wr(s: &SpliceSock) {
    unsafe {
        libc::shutdown(s.raw_fd(), libc::SHUT_WR);
    }
}

fn shutdown_rdwr(s: &SpliceSock) {
    unsafe {
        libc::shutdown(s.raw_fd(), libc::SHUT_RDWR);
    }
}

/// 单向泵(旧控制流,`RH_SPLICE_PUMP=legacy` 臂):每次 fill 前建立
/// readable/select future,然后排空管道;每次 drain 前建立 writable/
/// select future。相对批次化臂**无协作让步**(coop_yields 恒 0)与
/// 非阻塞快试——保留用于同树同计数对照(G1-A),行为与 g1 输入基线
/// 的旧泵一致,仅插入计数点。
///
/// 退出路径(EOF/读错误/写错误 → shutdown out 写侧;中止/取消 → 断连),
/// 与用户态 copy 循环「循环结束即 shutdown 写端」语义对齐(B5:写错误
/// 分支同样 shutdown 写侧,不再与 copy 口径分叉)。
/// 计量(B6):up(fill 口径,fill 完成即计、先于限速等待)与
/// down(送达口径,排空后按实际送达字节计)由 DirHook 装配决定。
/// 限速(A5):预留式等待与取消信号竞争;down 按实际送达消费,
/// 取消/部分送达退还未消费预留(A6:取消时已送达字节先计量再退出)。
/// 观测(A):`obs` 全 None 时热循环零统计;开启时就绪等待经
/// [`watch_ready`] 包装记录真实挂起(两臂同口径)。
#[allow(clippy::too_many_arguments)]
async fn pump_legacy(
    in_sock: Arc<SpliceSock>,
    out_sock: Arc<SpliceSock>,
    pipe: Pipe,
    hook: Option<DirHook>,
    abort: Option<Arc<dyn Fn() -> bool + Send + Sync>>,
    cancel: Option<CancelFuture>,
    dir: &'static str,
    obs: PumpObs,
) {
    let _stats_out = emitter_for(&obs, PumpMode::Legacy, dir, &in_sock, &out_sock);
    let pr = pipe.r.as_raw_fd();
    let pw = pipe.w.as_raw_fd();

    // 取消等待(None = 永不触发);多个 select 复用同一实例,
    // 触发后 Shared 保持完成态,后续 select 立即取胜。
    let cancel_wait = async {
        match cancel {
            Some(c) => c.await,
            None => std::future::pending().await,
        }
    };
    tokio::pin!(cancel_wait);

    fn teardown(s1: &SpliceSock, s2: &SpliceSock) {
        shutdown_rdwr(s1);
        shutdown_rdwr(s2);
    }

    loop {
        if let Some(f) = &abort {
            if f() {
                teardown(&in_sock, &out_sock);
                return;
            }
        }
        // 一次事件搬运一块(≤ 管道容量);EAGAIN 重新等就绪。
        // 就绪等待与取消信号竞争:规则禁用/删除要求拆除阻塞中的泵。
        let n = 'fill: loop {
            let mut guard = match tokio::select! {
                g = watch_ready(&obs, in_sock.fd.readable(), WaitDir::Read) => g,
                _ = &mut cancel_wait => {
                    teardown(&in_sock, &out_sock);
                    return;
                }
            } {
                Ok(g) => g,
                Err(_) => {
                    shutdown_wr(&out_sock);
                    return;
                }
            };
            match splice_sys(in_sock.raw_fd(), pw, PIPE_SIZE as usize) {
                Ok(0) => {
                    shutdown_wr(&out_sock);
                    return;
                }
                Ok(n) => {
                    stat_bump!(obs, fill_calls, 1);
                    stat_bump!(obs, fill_bytes, n);
                    if let Some(s) = &obs.counters {
                        s.fill_min_bytes.fetch_min(n as u64, Ordering::Relaxed);
                        s.fill_max_bytes.fetch_max(n as u64, Ordering::Relaxed);
                    }
                    break 'fill n;
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    // 块语义:fill 时管道必空,EAGAIN 来源=输入空
                    stat_bump!(obs, eagain_fill_input_empty, 1);
                    guard.clear_ready();
                    // 旧口径:等待尝试次数(真实挂起由包装 future 记录)
                    stat_bump!(obs, pending_waits_read, 1);
                    continue;
                }
                Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(_) => {
                    shutdown_wr(&out_sock);
                    return;
                }
            }
        };
        // B6:up 读侧口径——fill 完成即计量,先于限速等待
        //(copy 路径:CountedUserIo.poll_read 先计,copy_limited 后 acquire)
        if let Some(h) = &hook {
            if h.meter_on_fill {
                if let Some(c) = &h.on_bytes {
                    c(n as u64);
                }
            }
        }
        // A5:令牌等待与取消信号竞争(双泵同阻塞在限速等待时取消仍可达);
        // up(fill 口径)预留即消费;down(送达口径)持有至排空按送达消费。
        let mut reservation = match hook.as_ref().and_then(|h| h.limiter.clone()) {
            Some(l) => Some(tokio::select! {
                r = l.reserve(n) => r,
                _ = &mut cancel_wait => {
                    teardown(&in_sock, &out_sock);
                    return;
                }
            }),
            None => None,
        };
        if hook.as_ref().is_some_and(|h| h.meter_on_fill) {
            if let Some(r) = reservation.as_mut() {
                r.consume(n);
            }
        }
        if let Some(f) = &abort {
            if f() {
                drop(reservation); // 未消费预留退还
                teardown(&in_sock, &out_sock);
                return;
            }
        }
        // 排空管道到对端(对端拥塞时管道有界积压 → 背压到来源)。
        // 单次 ≤ DRAIN_SLICE:突发尺寸与拷贝路径一致。
        let mut delivered: usize = 0;
        loop {
            let mut guard = match tokio::select! {
                g = watch_ready(&obs, out_sock.fd.writable(), WaitDir::Write) => g,
                _ = &mut cancel_wait => {
                    // A6:排空中取消——已送达字节先计量再退出
                    if delivered > 0 {
                        if let Some(h) = &hook {
                            if !h.meter_on_fill {
                                if let Some(c) = &h.on_bytes {
                                    c(delivered as u64);
                                }
                            }
                        }
                        if let Some(r) = reservation.as_mut() {
                            r.consume(delivered);
                        }
                    }
                    drop(reservation); // 未消费预留退还
                    teardown(&in_sock, &out_sock);
                    return;
                }
            } {
                Ok(g) => g,
                Err(_) => {
                    // 写侧死亡:送达部分仍计量;B5:同样收尾 shutdown 写侧
                    shutdown_wr(&out_sock);
                    break;
                }
            };
            match splice_sys(pr, out_sock.raw_fd(), (n - delivered).min(DRAIN_SLICE)) {
                Ok(m) => {
                    delivered += m;
                    stat_bump!(obs, drain_calls, 1);
                    stat_bump!(obs, drain_bytes, m);
                    if let Some(s) = &obs.counters {
                        s.drain_min_bytes.fetch_min(m as u64, Ordering::Relaxed);
                        s.drain_max_bytes.fetch_max(m as u64, Ordering::Relaxed);
                    }
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    stat_bump!(obs, eagain_drain_write_full, 1);
                    guard.clear_ready();
                    // 旧口径:等待尝试次数(真实挂起由包装 future 记录)
                    stat_bump!(obs, pending_waits_write, 1);
                    continue;
                }
                Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(_) => {
                    // B5:写错误分支与 copy 路径一致地收尾 shutdown 写侧
                    shutdown_wr(&out_sock);
                    break;
                }
            }
            if delivered >= n {
                break;
            }
        }
        // 送达计量(down 口径):按实际送达字节,与 copy 路径口径一致
        if delivered > 0 {
            if let Some(h) = &hook {
                if !h.meter_on_fill {
                    if let Some(c) = &h.on_bytes {
                        c(delivered as u64);
                    }
                }
            }
            if let Some(r) = reservation.as_mut() {
                r.consume(delivered);
            }
        }
        drop(reservation); // down 部分送达:未消费预留退还
        if delivered < n {
            // 未排空即退出:写侧已不可用
            return;
        }
    }
}

/// 单向泵(默认臂,G1 候选):in socket → pipe → out socket。
///
/// 相对 legacy 的**单变量**控制流差异(G1 计划 §B):
/// 1. **非阻塞快试**:fill/drain 直接以 `SPLICE_F_NONBLOCK` splice 尝试,
///    不先建立 readiness future——legacy 每次 fill/drain 各建一次
///    `readable()/writable()` future(ready 态是异步快速路径,无 syscall,
///    但仍有 future 构造/poll/drop 成本;两端持续就绪时每块两次)。
/// 2. **仅 EAGAIN 后等待,按来源归因**(round-2 注释修正:顺序以实现
///    为准——AsyncFd 的 `clear_ready` 须先持有 guard,故为「等待→清→
///    重试」):fill EAGAIN(块语义下管道必空,来源=输入空)→ 等
///    `readable()` 取得 guard → `clear_ready` 清就绪缓存 → 重试快试;
///    drain EAGAIN(写侧满)→ 等 `writable()` → `clear_ready` → 重试。
///    陈旧缓存使等待至多一次立即返回,清掉后无新沿(EPOL_ET)时下一次
///    等待即真实 Pending——只在**观察到对应方向 EAGAIN** 后 clear 是
///    无丢唤醒/无忙转的关键(G1 复核修正 3:数据已在缓冲而缓存被清后
///    没有新沿,不得在未观察 EAGAIN 时清读侧就绪)。
/// 3. **有界批次 + 显式让步**:无等待连续搬运段内每搬运 `BATCH_BYTES`/
///    `BATCH_OPS` 后 `yield_now()`(确定性让步;`consume_budget()` 在
///    每批仅一次调用的形态下预算永不耗尽、让步不会发生,见 BATCH_BYTES
///    注释),两端持续就绪时不再霸占 worker(G1 复核修正 2,
///    legacy 无此不变量)。零进展(EAGAIN)必然落入真实 readiness 等待,
///    绝不忙转。
///
/// 严格保持(与 legacy 逐点一致):每 fill 块完成即 up 计量、先于限速
/// 等待(B6);限速预留/消费/退还三态(A5);abort 检查粒度=每块两次
/// (批首 + fill/限速后);读 EOF 半关闭;写错误/读错误收尾 shutdown 写侧
/// (B5);排空中取消的已送达计量(A6——本臂逐 drain 进展即计量,取消
/// 窗口更小);管道有界;任何 drain 返回零即停止而非自旋。
/// 观测(A):`obs` 全 None 时热循环零统计;开启时 EAGAIN 后的就绪等待
/// 经 [`watch_ready`] 包装记录真实挂起(口径与 legacy 臂一致)。
#[allow(clippy::too_many_arguments)]
async fn pump_batched(
    in_sock: Arc<SpliceSock>,
    out_sock: Arc<SpliceSock>,
    pipe: Pipe,
    hook: Option<DirHook>,
    abort: Option<Arc<dyn Fn() -> bool + Send + Sync>>,
    cancel: Option<CancelFuture>,
    dir: &'static str,
    obs: PumpObs,
) {
    let _stats_out = emitter_for(&obs, PumpMode::Batched, dir, &in_sock, &out_sock);
    let pr = pipe.r.as_raw_fd();
    let pw = pipe.w.as_raw_fd();

    let cancel_wait = async {
        match cancel {
            Some(c) => c.await,
            None => std::future::pending().await,
        }
    };
    tokio::pin!(cancel_wait);

    fn teardown(s1: &SpliceSock, s2: &SpliceSock) {
        shutdown_rdwr(s1);
        shutdown_rdwr(s2);
    }

    let mut batch_bytes = 0usize;
    let mut batch_ops = 0u32;
    loop {
        // 批首 abort(块边界口径与 legacy 一致:每块两次检查之一)
        if let Some(f) = &abort {
            if f() {
                teardown(&in_sock, &out_sock);
                return;
            }
        }
        // B1(round-2 复核修复):恢复「每块至少一次 cancel poll」。legacy
        // 每 fill/drain 都经 select{readiness|cancel}(select! 随机顺序下
        // 即使 readiness 恒 Ready,pending 的 cancel 分支也有 ~50% 概率被
        // poll),而本臂快试路径无 readiness 等待点——满载两端持续 ready
        // (无 EAGAIN、无限速:生产装配双桶皆空时不装 limiter)时 cancel
        // 无人 poll,「完成即双向断连」的响应上界退化为偶发 EAGAIN/流
        // 自然断。biased select 在块边界同步 poll 一次 cancel_wait:
        // 已完成立即 teardown,未完成 poll 一次(注册 waker,几十 ns/块,
        // 相对 256KiB 块的 splice syscall 微秒级成本可忽略)即继续。
        tokio::select! {
            biased;
            _ = &mut cancel_wait => {
                teardown(&in_sock, &out_sock);
                return;
            }
            _ = futures::future::ready(()) => {}
        }
        // fill 快试(非阻塞,不建 readiness future)
        let n = match splice_sys(in_sock.raw_fd(), pw, PIPE_SIZE as usize) {
            Ok(0) => {
                // 读 EOF:块语义下 fill 时管道必空,直接半关闭
                shutdown_wr(&out_sock);
                return;
            }
            Ok(n) => {
                stat_bump!(obs, fill_calls, 1);
                stat_bump!(obs, fill_bytes, n);
                if let Some(s) = &obs.counters {
                    s.fill_min_bytes.fetch_min(n as u64, Ordering::Relaxed);
                    s.fill_max_bytes.fetch_max(n as u64, Ordering::Relaxed);
                }
                batch_ops += 1;
                n
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                // 来源归因:单块管线深度下 fill 时管道必空 → 输入空;
                // 管道满来源(须转 drain)在本臂结构性不出现,计数恒 0
                stat_bump!(obs, eagain_fill_input_empty, 1);
                let mut guard = match tokio::select! {
                    g = watch_ready(&obs, in_sock.fd.readable(), WaitDir::Read) => g,
                    _ = &mut cancel_wait => {
                        teardown(&in_sock, &out_sock);
                        return;
                    }
                } {
                    Ok(g) => g,
                    Err(_) => {
                        shutdown_wr(&out_sock);
                        return;
                    }
                };
                // 已观察到 EAGAIN:清就绪缓存,使下一次 readable() 仅在
                // 新数据沿后返回——陈旧缓存立即唤醒即零进展忙转
                guard.clear_ready();
                // 旧口径:等待尝试次数(真实挂起由包装 future 记录)
                stat_bump!(obs, pending_waits_read, 1);
                // 真实等待为「批」之界:连续搬运段结束,预算重新计满
                batch_bytes = 0;
                batch_ops = 0;
                continue;
            }
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(_) => {
                shutdown_wr(&out_sock);
                return;
            }
        };
        // B6:up 读侧口径——fill 完成即计量,先于限速等待(点位/粒度不变)
        if let Some(h) = &hook {
            if h.meter_on_fill {
                if let Some(c) = &h.on_bytes {
                    c(n as u64);
                }
            }
        }
        // A5:预留式限速等待,与取消信号竞争
        let mut reservation = match hook.as_ref().and_then(|h| h.limiter.clone()) {
            Some(l) => Some(tokio::select! {
                r = l.reserve(n) => r,
                _ = &mut cancel_wait => {
                    teardown(&in_sock, &out_sock);
                    return;
                }
            }),
            None => None,
        };
        if hook.as_ref().is_some_and(|h| h.meter_on_fill) {
            if let Some(r) = reservation.as_mut() {
                r.consume(n);
            }
        }
        // fill/限速后的 abort 检查(每块第二次,粒度不放宽)
        if let Some(f) = &abort {
            if f() {
                drop(reservation); // 未消费预留退还
                teardown(&in_sock, &out_sock);
                return;
            }
        }
        // drain:非阻塞快试至块尽/EAGAIN。down 口径**逐 drain 进展**
        // 计量+消费(总量口径与 legacy 一致;A6 取消窗口更小:已送达
        // 即已计),reservation 差额随 drop 退还。
        let mut delivered: usize = 0;
        loop {
            match splice_sys(pr, out_sock.raw_fd(), (n - delivered).min(DRAIN_SLICE)) {
                Ok(0) => {
                    // 管道空但块未排空:状态错乱(本泵独占管道,不应发生),
                    // 保守收尾防零进展自旋
                    shutdown_wr(&out_sock);
                    break;
                }
                Ok(m) => {
                    delivered += m;
                    stat_bump!(obs, drain_calls, 1);
                    stat_bump!(obs, drain_bytes, m);
                    if let Some(s) = &obs.counters {
                        s.drain_min_bytes.fetch_min(m as u64, Ordering::Relaxed);
                        s.drain_max_bytes.fetch_max(m as u64, Ordering::Relaxed);
                    }
                    batch_ops += 1;
                    if let Some(h) = &hook {
                        if !h.meter_on_fill {
                            if let Some(c) = &h.on_bytes {
                                c(m as u64);
                            }
                            if let Some(r) = reservation.as_mut() {
                                r.consume(m);
                            }
                        }
                    }
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    // 写侧满:清 out 写就绪缓存后真实等待 writable
                    stat_bump!(obs, eagain_drain_write_full, 1);
                    let mut guard = match tokio::select! {
                        g = watch_ready(&obs, out_sock.fd.writable(), WaitDir::Write) => g,
                        _ = &mut cancel_wait => {
                            // A6:取消——已送达已逐进展计量,仅退差额
                            drop(reservation);
                            teardown(&in_sock, &out_sock);
                            return;
                        }
                    } {
                        Ok(g) => g,
                        Err(_) => {
                            // 写侧死亡:B5 收尾 shutdown 写侧
                            shutdown_wr(&out_sock);
                            break;
                        }
                    };
                    guard.clear_ready();
                    // 旧口径:等待尝试次数(真实挂起由包装 future 记录)
                    stat_bump!(obs, pending_waits_write, 1);
                    // 真实等待为「批」之界(同读侧)
                    batch_bytes = 0;
                    batch_ops = 0;
                    continue;
                }
                Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(_) => {
                    // B5:写错误分支与 copy 路径一致地收尾 shutdown 写侧
                    shutdown_wr(&out_sock);
                    break;
                }
            }
            if delivered >= n {
                break;
            }
        }
        drop(reservation); // down 未消费部分(块中断)退还
        if delivered < n {
            // 未排空即退出:写侧已不可用
            return;
        }
        batch_bytes += n;
        // 有界批次:无等待连续搬运段的字节/操作预算触达 → 显式让步一次,
        // 同 worker 任务不再被持续就绪的泵饿死(确定性 yield,理由见
        // BATCH_BYTES 注释)
        if batch_bytes >= BATCH_BYTES || batch_ops >= BATCH_OPS {
            stat_bump!(obs, coop_yields, 1);
            tokio::task::yield_now().await;
            batch_bytes = 0;
            batch_ops = 0;
        }
    }
}

/// 零拷贝双向中继。a/b 为两条裸 TCP(借用,不消费——回落时调用方仍持有原值)。
/// 仅在 dup/pipe 等资源申请失败时返回 Err(回落信号);中继本身运行至双向结束。
/// 父任务被取消(A2)时双向泵随 [`crate::AbortOnDropHandle`] 守卫终止并断连
/// ——守卫以 `&mut` join,句柄不移出,父 future 被 drop 即 abort 双泵。
///
/// 泵实现经 `RH_SPLICE_PUMP` 选择:默认批次化(快试/仅 EAGAIN 等待/有界
/// 预算让步);`legacy` 临时切回旧控制流(本地同树同计数对照)。
///
/// 统计(A):`RH_SPLICE_STATS` 初始化读一次——未设/空/"0"(生产默认)
/// 不分配计数,热循环零统计原子/Instant/输出;开启时装配每泵计数 +
/// 共享单调时钟,泵退出输出 v2 单行 JSON(stderr)。
pub async fn splice_relay(
    a: &tokio::net::TcpStream,
    b: &tokio::net::TcpStream,
    hooks: SpliceHooks,
) -> io::Result<()> {
    let mode = pump_mode_from_env();
    let (up, down) = if stats_enabled() {
        (
            Some(Arc::new(PumpCounters::default())),
            Some(Arc::new(PumpCounters::default())),
        )
    } else {
        (None, None)
    };
    splice_relay_with_stats(a, b, hooks, mode, up, down).await
}

/// 双向中继(泵实现显式注入;无统计 = 生产默认等价路径;测试不经 env
/// 直测两臂,避免并行测试的进程级 env 污染)。
#[cfg(test)]
pub(crate) async fn splice_relay_inner(
    a: &tokio::net::TcpStream,
    b: &tokio::net::TcpStream,
    hooks: SpliceHooks,
    mode: PumpMode,
) -> io::Result<()> {
    splice_relay_with_stats(a, b, hooks, mode, None, None).await
}

/// 双向中继(泵实现与计数句柄显式注入):任一方向 Some 即视为统计
/// 开启(共享时钟 + v2 输出 + 真实挂起观测);全 None 与生产默认一致。
pub(crate) async fn splice_relay_with_stats(
    a: &tokio::net::TcpStream,
    b: &tokio::net::TcpStream,
    hooks: SpliceHooks,
    mode: PumpMode,
    up_stats: Option<Arc<PumpCounters>>,
    down_stats: Option<Arc<PumpCounters>>,
) -> io::Result<()> {
    let clock: Option<Arc<dyn WaitClock>> = if up_stats.is_some() || down_stats.is_some() {
        Some(Arc::new(MonoClock::new()))
    } else {
        None
    };
    // 建立时刻墙钟仅统计开启时采样一次(默认关闭不读 SystemTime)。
    let started_unix_us = if clock.is_some() { unix_now_us() } else { None };
    let obs = |counters: Option<Arc<PumpCounters>>| PumpObs {
        counters,
        clock: clock.clone(),
        started_unix_us,
    };
    let sa = Arc::new(SpliceSock::dup(a)?);
    let sb = Arc::new(SpliceSock::dup(b)?);
    let p_ab = Pipe::new()?;
    let p_ba = Pipe::new()?;
    let spawn = |in_sock: Arc<SpliceSock>,
                 out_sock: Arc<SpliceSock>,
                 pipe: Pipe,
                 hook: Option<DirHook>,
                 abort: Option<Arc<dyn Fn() -> bool + Send + Sync>>,
                 cancel: Option<CancelFuture>,
                 dir: &'static str,
                 obs: PumpObs| {
        match mode {
            PumpMode::Legacy => crate::AbortOnDropHandle::new(tokio::spawn(pump_legacy(
                in_sock, out_sock, pipe, hook, abort, cancel, dir, obs,
            ))),
            PumpMode::Batched => crate::AbortOnDropHandle::new(tokio::spawn(pump_batched(
                in_sock, out_sock, pipe, hook, abort, cancel, dir, obs,
            ))),
        }
    };
    let mut g1 = spawn(
        sa.clone(),
        sb.clone(),
        p_ab,
        hooks.up,
        hooks.abort.clone(),
        hooks.cancel.clone(),
        "up",
        obs(up_stats),
    );
    let mut g2 = spawn(
        sb,
        sa,
        p_ba,
        hooks.down,
        hooks.abort,
        hooks.cancel,
        "down",
        obs(down_stats),
    );
    // final-gaps 补修:单泵 panic 时对向泵立即终止(不永存)。修前
    // `tokio::join!` 等双泵——panic 泵不做 teardown,对向阻塞在
    // readable() 永不结束,relay 卡死扣住 socket(与 mux/copy 路径的
    // panic 收敛缺口同款;写错误路径无此问题:B5 shutdown_wr 经 EOF
    // 链唤醒对向,见 splice_write_error_shutdown_parity)。
    // join_relay_pumps:先完成侧 panic → 立即返回(守卫 Drop → abort
    // 对向,fd 随 Arc 释放关闭);正常 EOF → 等对向自然结束(半关闭)。
    crate::join_relay_pumps(&mut g1, &mut g2).await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::FutureExt as _;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;

    async fn tcp_pair() -> (tokio::net::TcpStream, tokio::net::TcpStream) {
        let ln = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = ln.local_addr().unwrap();
        let c = tokio::net::TcpStream::connect(addr).await.unwrap();
        let (s, _) = ln.accept().await.unwrap();
        (c, s)
    }

    /// 轮询等待原子计数到达期望值(计量在泵任务内异步发生,读者完成≠已回调)。
    async fn wait_count(v: &AtomicU64, want: u64) {
        for _ in 0..200 {
            if v.load(Ordering::Relaxed) == want {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(v.load(Ordering::Relaxed), want);
    }

    // 基本搬运 + 半关闭:echo 目标侧 EOF 透传回用户侧
    #[tokio::test]
    async fn splice_relay_echo_and_half_close() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let (user_cli, user_srv) = tcp_pair().await;
        let (tgt_conn, mut tgt_echo) = tcp_pair().await;
        // echo:读尽即关
        tokio::spawn(async move {
            let mut buf = vec![0u8; 4096];
            loop {
                match tgt_echo.read(&mut buf).await {
                    Ok(0) | Err(_) => break,
                    Ok(n) => {
                        if tgt_echo.write_all(&buf[..n]).await.is_err() {
                            break;
                        }
                    }
                }
            }
        });
        let relay = tokio::spawn(async move {
            splice_relay(&user_srv, &tgt_conn, SpliceHooks::default())
                .await
                .unwrap();
        });
        // 并发读者:避免回程数据写满缓冲导致写端死锁
        let payload = vec![0xabu8; 256 * 1024];
        let (mut r_half, mut w_half) = user_cli.into_split();
        let recv = tokio::spawn(async move {
            let mut got = Vec::new();
            let _ = r_half.read_to_end(&mut got).await;
            got
        });
        w_half.write_all(&payload).await.unwrap();
        // 半关闭:写侧 EOF → echo 关闭 → 中继传递 → 读侧 EOF
        w_half.shutdown().await.unwrap();
        let got = recv.await.unwrap();
        assert_eq!(got.len(), payload.len());
        assert!(got.iter().all(|&b| b == 0xab));
        relay.await.unwrap();
    }

    // 计量钩子:双向字节计数与实际载荷一致
    #[tokio::test]
    async fn splice_relay_counts_bytes() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let up_count = Arc::new(AtomicU64::new(0));
        let down_count = Arc::new(AtomicU64::new(0));
        let (a_cli, a_srv) = tcp_pair().await;
        let (b_cli, b_srv) = tcp_pair().await;
        let uc = up_count.clone();
        let dc = down_count.clone();
        let hooks = SpliceHooks {
            up: Some(DirHook {
                on_bytes: Some(Arc::new(move |n| {
                    uc.fetch_add(n, Ordering::Relaxed);
                })),
                meter_on_fill: true,
                limiter: None,
            }),
            down: Some(DirHook {
                on_bytes: Some(Arc::new(move |n| {
                    dc.fetch_add(n, Ordering::Relaxed);
                })),
                meter_on_fill: false,
                limiter: None,
            }),
            abort: None,
            cancel: None,
        };
        let relay = tokio::spawn(async move {
            splice_relay(&a_srv, &b_srv, hooks).await.unwrap();
        });
        // 并发读者(双向各一个):up(a→b)的数据到 b 侧,down(b→a)到 a 侧
        let payload: Vec<u8> = (0..128 * 1024).map(|i| (i % 251) as u8).collect();
        let back = vec![7u8; 300 * 1024];
        let (mut a_r, mut a_w) = a_cli.into_split();
        let (mut b_r, mut b_w) = b_cli.into_split();
        let p2 = back.clone();
        let recv_dn = tokio::spawn(async move {
            let mut buf = vec![0u8; p2.len()];
            let _ = a_r.read_exact(&mut buf).await;
            buf
        });
        let p1 = payload.clone();
        let recv_up = tokio::spawn(async move {
            let mut buf = vec![0u8; p1.len()];
            let _ = b_r.read_exact(&mut buf).await;
            buf
        });
        a_w.write_all(&payload).await.unwrap();
        b_w.write_all(&back).await.unwrap();
        let got_up = recv_up.await.unwrap();
        let got_dn = recv_dn.await.unwrap();
        assert_eq!(got_up, payload);
        assert_eq!(got_dn, back);
        // 计量在排空完成后回调:读者完成后再等一小段窗口
        wait_count(&up_count, payload.len() as u64).await;
        wait_count(&down_count, back.len() as u64).await;
        relay.abort();
    }

    // 限速钩子:acquire 被调用且不破坏数据(固定延迟 shim 验证调用时序)
    #[tokio::test]
    async fn splice_relay_rate_limit_hook() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let (a_cli, a_srv) = tcp_pair().await;
        let (b_cli, b_srv) = tcp_pair().await;
        struct SlowLimiter(Arc<std::sync::atomic::AtomicUsize>);
        impl crate::ChunkLimiter for SlowLimiter {
            fn reserve(
                &self,
                n: usize,
            ) -> futures::future::BoxFuture<'static, Box<dyn crate::ChunkReservation>> {
                let c = self.0.clone();
                Box::pin(async move {
                    c.fetch_add(1, Ordering::Relaxed);
                    tokio::time::sleep(Duration::from_millis((n as u64 / 65536).max(1))).await;
                    struct NoRes;
                    impl crate::ChunkReservation for NoRes {
                        fn consume(&mut self, _n: usize) {}
                    }
                    Box::new(NoRes) as Box<dyn crate::ChunkReservation>
                })
            }
        }
        let limiter: Arc<SlowLimiter> = Arc::new(SlowLimiter(Arc::new(
            std::sync::atomic::AtomicUsize::new(0),
        )));
        let hooks = SpliceHooks {
            up: Some(DirHook {
                on_bytes: None,
                meter_on_fill: true,
                limiter: Some(limiter.clone() as Arc<dyn ChunkLimiter>),
            }),
            down: None,
            abort: None,
            cancel: None,
        };
        let relay = tokio::spawn(async move {
            splice_relay(&a_srv, &b_srv, hooks).await.unwrap();
        });
        let payload = vec![1u8; 64 * 1024];
        let (mut b_r, mut a_w) = {
            let (_ar, aw) = a_cli.into_split();
            let (br, bw) = b_cli.into_split();
            drop(bw);
            (br, aw)
        };
        let p1 = payload.clone();
        let recv = tokio::spawn(async move {
            let mut buf = vec![0u8; p1.len()];
            let _ = b_r.read_exact(&mut buf).await;
            buf
        });
        a_w.write_all(&payload).await.unwrap();
        let got = recv.await.unwrap();
        assert_eq!(got, payload);
        assert!(
            limiter.0.load(Ordering::Relaxed) >= 1,
            "限速 reserve 必须被调用"
        );
        relay.abort();
    }

    // 取消:规则禁用/删除 → 阻塞在就绪等待中的泵被唤醒,双向断连,relay 收敛
    #[tokio::test]
    async fn splice_relay_cancel_disconnects() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let (a_cli, a_srv) = tcp_pair().await;
        let (b_cli, b_srv) = tcp_pair().await;
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let cancel: CancelFuture = rx.map(|_| ()).boxed().shared();
        let hooks = SpliceHooks {
            up: None,
            down: None,
            abort: None,
            cancel: Some(cancel),
        };
        let relay = tokio::spawn(async move {
            splice_relay(&a_srv, &b_srv, hooks).await.unwrap();
        });
        // 先送一小笔让泵进入搬运状态(也覆盖「块间触发」路径),再等其回到等待
        let mut a = a_cli;
        let mut b = b_cli;
        a.write_u8(1).await.unwrap();
        let mut probe = [0u8; 1];
        b.read_exact(&mut probe).await.unwrap();
        assert_eq!(probe[0], 1);
        // 泵此刻阻塞在 readable 等待:触发取消
        tx.send(()).unwrap();
        // 双向断连:两侧读均应观察到 EOF,且 relay 任务随之结束
        let mut buf = [0u8; 16];
        let ra = tokio::time::timeout(Duration::from_secs(2), a.read(&mut buf))
            .await
            .unwrap()
            .unwrap();
        let rb = tokio::time::timeout(Duration::from_secs(2), b.read(&mut buf))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(ra, 0);
        assert_eq!(rb, 0);
        tokio::time::timeout(Duration::from_secs(2), relay)
            .await
            .unwrap()
            .unwrap();
    }
    // A2:父任务取消必须终止双向泵(修前 take_handle 把句柄移出守卫,
    // 父取消时 Drop 不再 abort,子泵 detach——/tmp/rh-splice-audit-ZrHoaE
    // harness parent_drop_detaches:取消后存活子任务 = 2)。
    // 探测协议:取消后写入的字节在窗口期内不得被转发。
    #[tokio::test]
    async fn splice_parent_cancel_kills_children() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let (mut a_cli, a_srv) = tcp_pair().await;
        let (mut b_cli, b_srv) = tcp_pair().await;
        let relay = tokio::spawn(async move {
            splice_relay(&a_srv, &b_srv, SpliceHooks::default())
                .await
                .unwrap();
        });
        // 建立流:双向各一字节
        a_cli.write_u8(1).await.unwrap();
        assert_eq!(b_cli.read_u8().await.unwrap(), 1);
        b_cli.write_u8(2).await.unwrap();
        assert_eq!(a_cli.read_u8().await.unwrap(), 2);
        // 父任务取消(必须以 cancelled 完成 —— 守卫生效,子泵未 detach)
        relay.abort();
        assert!(relay.await.unwrap_err().is_cancelled());
        // 泵终止需要一次调度窗口,随后写探测字节。
        // ★测试修正(2026-10-13,主代理授权):teardown 已对中继双端 SHUT_RDWR,
        // 因此本地探测写与对端读都可能观察到断连(BrokenPipe/ConnectionReset/
        // UnexpectedEof)—— 断连 == 没有字节被转发,与超时一样合法;
        // 只有「收到数据字节」才证明子泵 detach(A2)。其余错误仍失败。
        tokio::time::sleep(Duration::from_millis(50)).await;
        match a_cli.write_u8(9).await {
            Ok(()) => {}
            Err(e)
                if matches!(
                    e.kind(),
                    io::ErrorKind::BrokenPipe | io::ErrorKind::ConnectionReset
                ) => {}
            Err(e) => panic!("探测写入失败(非断连类错误): {e}"),
        }
        match tokio::time::timeout(Duration::from_millis(300), b_cli.read_u8()).await {
            Err(_) => {} // 超时 = 未转发 = 通过(泵已终止)
            // EOF(正常断连表现)= 未转发 = 通过;其他读错误仍失败
            Ok(Err(e)) if e.kind() == io::ErrorKind::UnexpectedEof => {}
            Ok(Ok(v)) => panic!("取消后字节 {v} 仍被转发(子泵 detach,A2)"),
            Ok(Err(e)) => panic!("读侧异常(非断连类错误): {e}"),
        }
        // 资源清理:两侧客户端连接正常丢弃,无残余任务(本测试结束即 runtime
        // 单测实例收尾,若有泄漏任务会在后续测试的 runtime 上留下干扰)。
        drop(a_cli);
        drop(b_cli);
    }

    // A5:双向泵同时阻塞在限速预留等待时,取消信号必须可达并拆除泵
    //(修前裸 acquire await 不在 select 中,harness:双泵仍 pending)。
    // ★测试修正(2026-10-13,主代理授权):修前 NeverLimiter 立即返回预留,
    // 泵从未真正进入等待 —— 现在改为真正 pending 的预留 future,并以
    // 「两泵都已进入 reserve 等待」的计数屏障确保取消发生在等待中;
    // 未进入等待即取消成功,不构成本回归的通过条件。
    #[tokio::test]
    async fn splice_token_cancel_reaches_pumps_in_rate_wait() {
        use tokio::io::AsyncWriteExt;
        struct WaitLimiter {
            /// reserve future 首次被 poll(pump 真正开始等待预留)时 +1。
            entered: Arc<std::sync::atomic::AtomicUsize>,
        }
        impl crate::ChunkLimiter for WaitLimiter {
            fn reserve(
                &self,
                _n: usize,
            ) -> futures::future::BoxFuture<'static, Box<dyn crate::ChunkReservation>> {
                let entered = self.entered.clone();
                Box::pin(async move {
                    entered.fetch_add(1, Ordering::SeqCst);
                    // 永不完成:泵将停留在预留等待中,直到取消信号到达
                    std::future::pending::<Box<dyn crate::ChunkReservation>>().await
                })
            }
        }
        let (mut a_cli, a_srv) = tcp_pair().await;
        let (mut b_cli, b_srv) = tcp_pair().await;
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let cancel: CancelFuture = rx.map(|_| ()).boxed().shared();
        let entered = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let dir = |l: Arc<WaitLimiter>| DirHook {
            on_bytes: None,
            meter_on_fill: false,
            limiter: Some(l as Arc<dyn ChunkLimiter>),
        };
        let hooks = SpliceHooks {
            up: Some(dir(Arc::new(WaitLimiter {
                entered: entered.clone(),
            }))),
            down: Some(dir(Arc::new(WaitLimiter {
                entered: entered.clone(),
            }))),
            abort: None,
            cancel: Some(cancel),
        };
        let relay = tokio::spawn(async move {
            splice_relay(&a_srv, &b_srv, hooks).await.unwrap();
        });
        // 双向各写一笔:两泵都 fill 后进入永不完成的预留等待
        a_cli.write_u8(1).await.unwrap();
        b_cli.write_u8(2).await.unwrap();
        // 屏障:两个泵的 reserve future 都已被 poll(真正进入等待)才允许取消
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        while entered.load(Ordering::SeqCst) < 2 {
            assert!(
                tokio::time::Instant::now() < deadline,
                "两泵未都进入限速预留等待(entered={})",
                entered.load(Ordering::SeqCst)
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        // 再让出一拍,确保 select! 稳定挂起在 reserve 分支后触发取消
        tokio::time::sleep(Duration::from_millis(50)).await;
        tx.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(2), relay)
            .await
            .expect("取消必须到达阻塞在限速等待中的泵(A5)")
            .unwrap();
    }

    // A7:PIPE_CAP_MIN 并发更新必须保持真原子最小
    //(修前 load+条件 store 可把 256KiB 留作最小值;
    // harness pipe_min_lost_update:观测 [8192,262144] 记录 min=262144)。
    #[test]
    fn pipe_cap_min_atomic() {
        PIPE_CAP_MIN.store(0, Ordering::Relaxed);
        let threads: Vec<_> = (0..8)
            .map(|i| {
                std::thread::spawn(move || {
                    for _ in 0..2000 {
                        if i % 2 == 0 {
                            update_pipe_cap_min(8192);
                        } else {
                            update_pipe_cap_min(262144);
                        }
                    }
                })
            })
            .collect();
        for t in threads {
            t.join().unwrap();
        }
        assert_eq!(
            PIPE_CAP_MIN.load(Ordering::Relaxed),
            8192,
            "并发观测 8KiB/256KiB 后最小值必须保持 8KiB(A7)"
        );
    }

    // A6:排空中途取消——已送达字节必须先计量再退出
    //(修前 return 先于回调,harness:accepted=3, callback=0)。
    // 构造:down 方向灌满对端接收缓冲(对端不读)使排空部分送达,
    // 取消后对端排干 socket,收到的字节数必须 == 计量值。
    // ★测试修正(2026-10-13,主代理授权):read_to_end 原漏 .await(future 被
    // 直接丢弃,got 恒 0)——现补 .await 并以 timeout 包裹,relay/writer/read
    // 三者结果显式检查,不再 let _ 吞错。
    // ★覆盖口径如实标注:本用例验证「实际收到字节 == 计量值」的不变量;
    // 512KiB 在 loopback 缓冲下可能全部送达(m == payload_len),此时**不
    // 构成**「部分块边界」的覆盖 —— 部分写故障分支由下方
    // splice_partial_meter_write_fault 注入回归单独覆盖与报告。
    #[tokio::test]
    async fn splice_partial_meter_on_cancel() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let (mut a_cli, a_srv) = tcp_pair().await;
        let (b_cli, b_srv) = tcp_pair().await;
        let metered = Arc::new(AtomicU64::new(0));
        let m2 = metered.clone();
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let cancel: CancelFuture = rx.map(|_| ()).boxed().shared();
        let dir = DirHook {
            on_bytes: Some(Arc::new(move |n| {
                m2.fetch_add(n, Ordering::Relaxed);
            })),
            meter_on_fill: false,
            limiter: None,
        };
        // b→a 为 down(carrier→user):送达计量挂 down 钩
        let hooks = SpliceHooks {
            up: None,
            down: Some(dir),
            abort: None,
            cancel: Some(cancel),
        };
        let relay = tokio::spawn(async move {
            splice_relay(&a_srv, &b_srv, hooks).await.unwrap();
        });
        // b 侧(客户端对端)灌 512KiB:a 侧不读 → 排空部分送达后阻塞
        let mut b_w = b_cli;
        let payload_len = 512 * 1024;
        let payload = vec![3u8; payload_len];
        let writer = tokio::spawn(async move {
            let _ = b_w.write_all(&payload).await;
        });
        tokio::time::sleep(Duration::from_millis(200)).await;
        tx.send(()).unwrap();
        // 取消后 relay 必须收敛;panic/abort 都是失败,不再吞 JoinError
        match tokio::time::timeout(Duration::from_secs(3), relay).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) if e.is_panic() => panic!("relay 任务 panic: {e}"),
            Ok(Err(e)) => panic!("relay 被意外 abort: {e}"),
            Err(_) => panic!("取消后 relay 未在 3s 内收敛"),
        }
        writer.await.expect("writer 任务必须正常结束");
        // 对端排干 a 侧收到的字节:必须与计量值一致(部分送达如实计量)
        let mut got = Vec::new();
        tokio::time::timeout(Duration::from_secs(3), a_cli.read_to_end(&mut got))
            .await
            .expect("a_cli read_to_end 必须在 3s 内完成(teardown FIN 已发出)")
            .expect("read_to_end IO 失败");
        let m = metered.load(Ordering::Relaxed);
        assert!(
            m > 0 && m <= payload_len as u64,
            "取消时已送达字节必须计量(m={m})"
        );
        assert_eq!(
            got.len() as u64,
            m,
            "计量值必须等于实际送达字节数(A6:got={} metered={m})",
            got.len()
        );
    }

    // A6 部分写分支(注入回归,2026-10-13 主代理授权新增):写侧中途死亡时,
    // 已送达字节必须如实计量(0 < m < payload 即命中),relay 必须收敛。
    // 注入方式:a_cli 读侧 SHUT_RD → 内核对在途数据回 RST → a_srv 写侧
    // EPIPE → 排空循环中途退出(B5 分支)按已送达量计量。
    // ★覆盖口径如实:RST 到达时的排空进度由调度决定,单次尝试可能全部
    // 送达(未命中部分分支)。本回归多次尝试,不变量断言(收敛、
    // 0 ≤ m ≤ payload)恒真,「部分块命中」次数以测试输出如实报告,
    // 不假称覆盖;不为制造背压触碰任何 SO_RCVBUF/SO_SNDBUF(红线)。
    #[tokio::test]
    async fn splice_partial_meter_write_fault() {
        use tokio::io::AsyncWriteExt;
        let attempts = 6usize;
        let payload_len = 16 * 1024 * 1024;
        let mut hit_partial = 0usize;
        for attempt in 0..attempts {
            let (a_cli, a_srv) = tcp_pair().await;
            let (b_cli, b_srv) = tcp_pair().await;
            let metered = Arc::new(AtomicU64::new(0));
            let m2 = metered.clone();
            let (tx, rx) = tokio::sync::oneshot::channel::<()>();
            let cancel: CancelFuture = rx.map(|_| ()).boxed().shared();
            let dir = DirHook {
                on_bytes: Some(Arc::new(move |n| {
                    m2.fetch_add(n, Ordering::Relaxed);
                })),
                meter_on_fill: false,
                limiter: None,
            };
            let hooks = SpliceHooks {
                up: None,
                down: Some(dir),
                abort: None,
                cancel: Some(cancel),
            };
            let relay = tokio::spawn(async move {
                splice_relay(&a_srv, &b_srv, hooks).await.unwrap();
            });
            let mut b_w = b_cli;
            let payload = vec![3u8; payload_len];
            let writer = tokio::spawn(async move {
                let _ = b_w.write_all(&payload).await;
            });
            // 不同窗口后注入 RST:窗口越小,越可能落在排空中途
            tokio::time::sleep(Duration::from_millis(1 + attempt as u64)).await;
            unsafe { libc::shutdown(a_cli.as_raw_fd(), libc::SHUT_RD) };
            tokio::time::sleep(Duration::from_millis(100)).await;
            let _ = tx.send(());
            // 不变量:relay 必须收敛(B5 写错误不挂死);panic 必须暴露
            match tokio::time::timeout(Duration::from_secs(3), relay).await {
                Ok(Ok(())) => {}
                Ok(Err(e)) if e.is_panic() => panic!("relay 任务 panic: {e}"),
                Ok(Err(e)) => panic!("relay 意外被 abort: {e}"),
                Err(_) => panic!("写故障注入后 relay 3s 未收敛(attempt {attempt})"),
            }
            let _ = writer.await;
            let m = metered.load(Ordering::Relaxed);
            assert!(
                m <= payload_len as u64,
                "计量不得超过载荷(m={m},attempt {attempt})"
            );
            if m > 0 && m < payload_len as u64 {
                hit_partial += 1;
            }
            eprintln!(
                "write_fault attempt {attempt}: metered={m}/{payload_len} partial_hit={}",
                m > 0 && m < payload_len as u64
            );
            drop(a_cli);
        }
        eprintln!("write_fault: {hit_partial}/{attempts} 次命中部分送达分支(0 < m < payload)");
    }

    // B6:up 计量点位 = fill 完成后、限速等待之前
    //(极小令牌桶阻塞期间 fill 字节立即可见,与 copy 路径
    // CountedUserIo 读侧先计的时序窗口一致;harness 同款断言)。
    #[tokio::test]
    async fn splice_up_meter_visible_during_rate_block() {
        use tokio::io::AsyncWriteExt;
        struct NeverLimiter;
        impl crate::ChunkLimiter for NeverLimiter {
            fn reserve(
                &self,
                _n: usize,
            ) -> futures::future::BoxFuture<'static, Box<dyn crate::ChunkReservation>> {
                Box::pin(async {
                    struct NoRes;
                    impl crate::ChunkReservation for NoRes {
                        fn consume(&mut self, _n: usize) {}
                    }
                    Box::new(NoRes) as Box<dyn crate::ChunkReservation>
                })
            }
        }
        let (mut a_cli, a_srv) = tcp_pair().await;
        let (_b_cli, b_srv) = tcp_pair().await;
        let metered = Arc::new(AtomicU64::new(0));
        let m2 = metered.clone();
        let hooks = SpliceHooks {
            up: Some(DirHook {
                on_bytes: Some(Arc::new(move |n| {
                    m2.fetch_add(n, Ordering::Relaxed);
                })),
                meter_on_fill: true,
                limiter: Some(Arc::new(NeverLimiter) as Arc<dyn ChunkLimiter>),
            }),
            down: None,
            abort: None,
            cancel: None,
        };
        let relay = tokio::spawn(async move {
            splice_relay(&a_srv, &b_srv, hooks).await.unwrap();
        });
        // 写 64KiB:fill 完成即计量,随后阻塞在预留等待(零送达)
        let payload = vec![5u8; 64 * 1024];
        a_cli.write_all(&payload).await.unwrap();
        // 限速阻塞期间计量必须已可见(轮询带截止)
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        while metered.load(Ordering::Relaxed) < payload.len() as u64 {
            assert!(
                tokio::time::Instant::now() < deadline,
                "限速阻塞期间 fill 字节必须已计量(B6)"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        relay.abort();
    }

    // B5:写错误分支必须 shutdown 写侧(与 copy 路径口径一致;修前
    // 直接 return,harness:WRITE_ERROR 计 3B 且无 shutdown)。
    // 构造:对端以 SO_LINGER=0 关闭产生 RST → 写侧 EPIPE → 断言
    // 泵收敛(不挂死)且对向读侧观察到断连。
    #[tokio::test]
    async fn splice_write_error_shutdown_parity() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let (mut a_cli, a_srv) = tcp_pair().await;
        let (b_cli, b_srv) = tcp_pair().await;
        let relay = tokio::spawn(async move {
            splice_relay(&a_srv, &b_srv, SpliceHooks::default())
                .await
                .unwrap();
        });
        a_cli.write_u8(1).await.unwrap();
        // RST 断连 b 侧对端
        let std_b = b_cli.into_std().unwrap();
        let linger = libc::linger {
            l_onoff: 1,
            l_linger: 0,
        };
        unsafe {
            libc::setsockopt(
                std_b.as_raw_fd(),
                libc::SOL_SOCKET,
                libc::SO_LINGER,
                &linger as *const _ as *const libc::c_void,
                std::mem::size_of::<libc::linger>() as libc::socklen_t,
            );
        }
        drop(std_b); // RST
                     // a→b 方向写错误;泵必须收敛(relay 任务结束,不挂死)
        a_cli.write_all(&vec![7u8; 64 * 1024]).await.unwrap_or(());
        tokio::time::timeout(Duration::from_secs(3), relay)
            .await
            .expect("写错误后泵必须收敛(B5:shutdown 写侧,不挂死)")
            .unwrap();
        // 对向读侧观察到断连(EOF 或错误)
        let mut buf = [0u8; 16];
        let _ = tokio::time::timeout(Duration::from_secs(2), a_cli.read(&mut buf)).await;
    }

    // final-gaps(红→绿,Linux gate):单泵 panic(经 on_bytes 计量回调
    // 注入,回调在泵内执行)时对向泵必须立即终止——修前 splice_relay 用
    // `tokio::join!` 等双泵,panic 泵不做 teardown,对向阻塞在
    // readable() 永不结束,relay 卡死扣住 socket(mux/copy 路径已有
    // 同款回归,splice 漏了)。对向静默(无数据)是永存形态的严格判据。
    #[tokio::test]
    async fn splice_sibling_aborted_on_pump_panic() {
        use tokio::io::AsyncWriteExt;
        let (mut a_cli, a_srv) = tcp_pair().await;
        let (_b_cli, b_srv) = tcp_pair().await;
        let hooks = SpliceHooks {
            // up 泵(a→b)首次计量回调即 panic
            up: Some(DirHook {
                on_bytes: Some(Arc::new(|_n| panic!("injected meter panic"))),
                meter_on_fill: true,
                limiter: None,
            }),
            down: None,
            abort: None,
            cancel: None,
        };
        let relay = tokio::spawn(async move {
            splice_relay(&a_srv, &b_srv, hooks).await.unwrap();
        });
        // 触发 up 泵 fill → 计量回调 panic;down 泵停在 b 读(对端静默)
        a_cli.write_u8(1).await.unwrap();
        tokio::time::timeout(Duration::from_secs(3), relay)
            .await
            .expect("单泵 panic 后 splice_relay 必须收敛(对向泵不得永存)")
            .unwrap();
    }

    // ======================= G1 批次化泵定向回归 =======================
    // 以上既有回归全部经默认 `splice_relay`(=Batched 臂)运行,即新泵
    // 全量语义回归;以下为 G1 候选新增不变量/观测的直接覆盖。

    /// 泵模式默认值:未设 RH_SPLICE_PUMP 时必须落 Batched(最终获益机制
    /// 为默认路径,不以开关偷换目标)。只读断言——不在测试内 set_var,
    /// 避免与并行测试的进程级 env 互相污染。
    #[test]
    fn splice_pump_mode_defaults_to_batched() {
        assert_eq!(pump_mode_from_env(), PumpMode::Batched);
    }

    /// legacy 臂(RH_SPLICE_PUMP=legacy 的同树对照)基本搬运+半关闭:
    /// 旧控制流保留且不回归(经 inner 直测,不动进程 env)。
    #[tokio::test]
    async fn splice_legacy_arm_transfers_and_half_close() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let (user_cli, user_srv) = tcp_pair().await;
        let (tgt_conn, mut tgt_echo) = tcp_pair().await;
        tokio::spawn(async move {
            let mut buf = vec![0u8; 4096];
            loop {
                match tgt_echo.read(&mut buf).await {
                    Ok(0) | Err(_) => break,
                    Ok(n) => {
                        if tgt_echo.write_all(&buf[..n]).await.is_err() {
                            break;
                        }
                    }
                }
            }
        });
        let relay = tokio::spawn(async move {
            splice_relay_inner(
                &user_srv,
                &tgt_conn,
                SpliceHooks::default(),
                PumpMode::Legacy,
            )
            .await
            .unwrap();
        });
        let payload = vec![0x5au8; 192 * 1024];
        let (mut r_half, mut w_half) = user_cli.into_split();
        let recv = tokio::spawn(async move {
            let mut got = Vec::new();
            let _ = r_half.read_to_end(&mut got).await;
            got
        });
        w_half.write_all(&payload).await.unwrap();
        w_half.shutdown().await.unwrap();
        let got = recv.await.unwrap();
        assert_eq!(got.len(), payload.len());
        assert!(got.iter().all(|&b| b == 0x5a));
        relay.await.unwrap();
    }

    /// 每泵计数(G1-A 最小观测):搬运量与载荷一致;down 口径逐 drain
    /// 进展计量的总量与 fill 相等;legacy/batched 两臂同款计数语义。
    #[tokio::test]
    async fn splice_stats_count_payload_bytes() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        for mode in [PumpMode::Legacy, PumpMode::Batched] {
            let (a_cli, a_srv) = tcp_pair().await;
            let (b_cli, b_srv) = tcp_pair().await;
            let up = Arc::new(PumpCounters::default());
            let down = Arc::new(PumpCounters::default());
            let (u, d) = (up.clone(), down.clone());
            let relay = tokio::spawn(async move {
                splice_relay_with_stats(
                    &a_srv,
                    &b_srv,
                    SpliceHooks::default(),
                    mode,
                    Some(u),
                    Some(d),
                )
                .await
                .unwrap();
            });
            let payload_len = 512 * 1024;
            let payload = vec![0x11u8; payload_len];
            let (mut b_r, mut a_w) = {
                let (_ar, aw) = a_cli.into_split();
                let (br, _bw) = b_cli.into_split();
                (br, aw)
            };
            let p = payload.clone();
            let recv = tokio::spawn(async move {
                let mut buf = vec![0u8; p.len()];
                let _ = b_r.read_exact(&mut buf).await;
                buf
            });
            a_w.write_all(&payload).await.unwrap();
            a_w.shutdown().await.unwrap();
            let got = tokio::time::timeout(Duration::from_secs(5), recv)
                .await
                .expect("载荷必须在 5s 内送达")
                .unwrap();
            assert_eq!(got.len(), payload_len);
            // up 泵(a→b):fill 与 drain 总量都等于载荷;计数在泵任务内
            // 异步发生,轮询等待收敛
            let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
            while up.fill_bytes.load(Ordering::Relaxed) < payload_len as u64
                || up.drain_bytes.load(Ordering::Relaxed) < payload_len as u64
            {
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "计数未收敛到载荷量(fill={} drain={},mode={:?})",
                    up.fill_bytes.load(Ordering::Relaxed),
                    up.drain_bytes.load(Ordering::Relaxed),
                    mode
                );
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            assert!(up.fill_calls.load(Ordering::Relaxed) >= 1);
            assert!(up.drain_calls.load(Ordering::Relaxed) >= 1);
            // 静默方向(down)不得有搬运
            assert_eq!(down.fill_bytes.load(Ordering::Relaxed), 0);
            assert_eq!(down.drain_bytes.load(Ordering::Relaxed), 0);
            // D6:v2 结束记录离线渲染(不经 env):字段齐全、计数与载荷
            // 一致、块分布语义成立(有成功调用 → min/max 非 null 且有序)
            let line = up.snapshot_json(
                &RelayMeta {
                    mode,
                    dir: "up",
                    start_ns: 5,
                    started_unix_us: Some(1_700_000_000_000_000),
                    src_local: Some("127.0.0.1:1".into()),
                    src_peer: Some("127.0.0.1:2".into()),
                    dst_local: Some("127.0.0.1:3".into()),
                    dst_peer: None,
                },
                105,
            );
            let v: serde_json::Value = serde_json::from_str(&line).expect("v2 行必须是合法 JSON");
            assert_eq!(v["splice_stats"], "v2");
            assert_eq!(v["started_unix_us"], 1_700_000_000_000_000u64);
            assert_eq!(
                v["pump_mode"],
                if mode == PumpMode::Legacy {
                    "legacy"
                } else {
                    "batched"
                }
            );
            assert_eq!(v["duration_ns"], 100);
            assert_eq!(v["src_peer"], "127.0.0.1:2");
            assert!(v["dst_peer"].is_null(), "未采样地址必须为 null 而非 0");
            assert_eq!(v["fill_bytes"], payload_len as u64);
            assert_eq!(v["drain_bytes"], payload_len as u64);
            let fmin = v["fill_min_bytes"].as_u64().expect("有成功 fill → 非 null");
            let fmax = v["fill_max_bytes"].as_u64().expect("有成功 fill → 非 null");
            assert!(
                fmin >= 1 && fmin <= fmax,
                "块分布 min/max 语义(fmin={fmin} fmax={fmax})"
            );
            let dmin = v["drain_min_bytes"]
                .as_u64()
                .expect("有成功 drain → 非 null");
            let dmax = v["drain_max_bytes"]
                .as_u64()
                .expect("有成功 drain → 非 null");
            assert!(
                dmin >= 1 && dmin <= dmax,
                "块分布 min/max 语义(dmin={dmin} dmax={dmax})"
            );
            for k in [
                "pending_waits_read",
                "pending_waits_write",
                "pending_polls_read",
                "pending_polls_write",
                "wait_cycles_read",
                "wait_cycles_write",
                "wait_ns_read",
                "wait_ns_write",
                "coop_yields",
            ] {
                assert!(v.get(k).is_some_and(|x| x.is_u64()), "缺 v2 字段 {k}");
            }
            relay.abort();
        }
    }

    /// 公平性/抗饿死(G1 复核修正 2 新增不变量):tokio AsyncFd 的
    /// async 路径不消耗协作预算,两端持续就绪时无让步的泵可在单
    /// worker 上饿死同驻任务。Batched 臂每批字节/操作预算触达后
    /// `yield_now()` 显式让步——本回归在单 worker(current_thread)
    /// 上让满载泵与 yield_now 伴生任务共驻,断言伴生任务在泵搬运
    /// 期间获得调度(≥4 次)。legacy 臂无该不变量,不参与断言。
    /// ★口径如实:loopback 的偶发 EAGAIN 也会让出调度,本回归保护
    /// 「批次化泵存在显式让步路径且伴生任务不被饿死」,不构成对
    /// legacy 臂的判定。
    #[tokio::test]
    async fn splice_batch_yields_to_sibling_task_on_single_worker() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let (a_cli, a_srv) = tcp_pair().await;
        let (b_cli, b_srv) = tcp_pair().await;
        let up = Arc::new(PumpCounters::default());
        let down = Arc::new(PumpCounters::default());
        let (u, d) = (up.clone(), down.clone());
        let relay = tokio::spawn(async move {
            splice_relay_with_stats(
                &a_srv,
                &b_srv,
                SpliceHooks::default(),
                PumpMode::Batched,
                Some(u),
                Some(d),
            )
            .await
            .unwrap();
        });
        let payload_len = 32 * 1024 * 1024;
        let payload = vec![0x33u8; payload_len];
        let (mut b_r, mut a_w) = {
            let (_ar, aw) = a_cli.into_split();
            let (br, _bw) = b_cli.into_split();
            (br, aw)
        };
        let p = payload.clone();
        let recv = tokio::spawn(async move {
            let mut buf = vec![0u8; p.len()];
            let _ = b_r.read_exact(&mut buf).await;
            buf
        });
        let ticks = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let t2 = ticks.clone();
        let ticker = tokio::spawn(async move {
            loop {
                // yield_now 重排回队列;只有泵 Pending/让步时才获得 poll
                tokio::task::yield_now().await;
                t2.fetch_add(1, Ordering::SeqCst);
            }
        });
        a_w.write_all(&payload).await.unwrap();
        a_w.shutdown().await.unwrap();
        let got = tokio::time::timeout(Duration::from_secs(30), recv)
            .await
            .expect("32MiB 必须在 30s 内搬完")
            .unwrap();
        assert_eq!(got.len(), payload_len);
        let n = ticks.load(Ordering::SeqCst);
        assert!(
            n >= 4,
            "单 worker 上满载泵必须让步(伴生任务 ticks={n} < 4,批次预算/yield_now 失效)"
        );
        // round-2 增强(复核建议):报告显式让步计数供人工核验。不做硬断言:
        // 「批」以真实等待为界,loopback 偶发 EAGAIN 会重置批计数,让步由
        // 等待路径承担时 coop_yields 可为 0——ticks(总让步效果)才是不变量。
        eprintln!(
            "fairness: ticks={n} coop_yields={} (up)",
            up.coop_yields.load(Ordering::Relaxed)
        );
        ticker.abort();
        relay.abort();
    }

    /// B1(round-2 复核 blocker 修复回归):满载两端持续 ready(无 EAGAIN、
    /// 无限速——生产装配双桶皆空时不装 limiter)时,cancel 信号也必须在
    /// **块边界**被 poll——`SpliceHooks::cancel` 文档与 data.rs 装配均承诺
    /// 「完成即双向断连」。修复前 batched 快试路径无 readiness 等待点,
    /// cancel 无任何 poll,响应上界退化为偶发 EAGAIN/流自然断;修复=批首
    /// biased select 同步 poll 一次 cancel_wait(每块至少一次,粒度不低于
    /// legacy 的 select{readiness|cancel} ~50% poll 机会)。
    /// 两臂同测:legacy 每 fill/drain 经 select 语义本就覆盖。
    #[tokio::test]
    async fn splice_cancel_during_saturated_transfer() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        for mode in [PumpMode::Legacy, PumpMode::Batched] {
            let (a_cli, a_srv) = tcp_pair().await;
            let (b_cli, b_srv) = tcp_pair().await;
            let (tx, rx) = tokio::sync::oneshot::channel::<()>();
            let cancel: CancelFuture = rx.map(|_| ()).boxed().shared();
            let hooks = SpliceHooks {
                up: None,
                down: None,
                abort: None,
                cancel: Some(cancel),
            };
            let relay = tokio::spawn(async move {
                splice_relay_inner(&a_srv, &b_srv, hooks, mode)
                    .await
                    .unwrap();
            });
            // 满载稳态:source 无限写 256KiB 块,sink 并发持续读——两端长期
            // ready,泵处于快试连续搬运形态(无 EAGAIN/无限速等待点)
            let mut a_w = a_cli;
            let writer = tokio::spawn(async move {
                let buf = vec![0x2du8; 256 * 1024];
                loop {
                    if a_w.write_all(&buf).await.is_err() {
                        break; // teardown 断连后退出
                    }
                }
            });
            let mut b_r = b_cli;
            let reader = tokio::spawn(async move {
                let mut buf = vec![0u8; 256 * 1024];
                loop {
                    match b_r.read(&mut buf).await {
                        Ok(0) | Err(_) => break,
                        Ok(_) => {}
                    }
                }
            });
            // 稳态满载窗口后触发取消(规则禁用/删除形态)
            tokio::time::sleep(Duration::from_millis(300)).await;
            let _ = tx.send(());
            // 块边界 poll 保证:1s 内 relay 必须收敛(teardown 双向断连)
            tokio::time::timeout(Duration::from_secs(1), relay)
                .await
                .unwrap_or_else(|_| {
                    panic!("满载中取消必须在 1s 内断连(B1:块边界 cancel poll 缺失,mode={mode:?})")
                })
                .unwrap();
            // 断连后 writer/reader 应随 socket 死亡退出
            tokio::time::timeout(Duration::from_secs(3), writer)
                .await
                .expect("writer 必须随断连退出")
                .unwrap();
            tokio::time::timeout(Duration::from_secs(3), reader)
                .await
                .expect("reader 必须随断连退出")
                .unwrap();
        }
    }

    /// 零进展不得忙转(G1 计划 §B 红线):静默期(无新数据)泵必须停留在
    /// 真实 readiness 等待,不得以陈旧缓存就绪反复唤醒自旋。判据:静默
    /// 期两次采样之间 pending_waits_read 增长近似为零(忙转形态会以
    /// 微秒级循环推高该计数)。up 泵把 a→b 的载荷搬完后进入静默。
    #[tokio::test]
    async fn splice_silent_period_no_busy_spin() {
        use tokio::io::AsyncWriteExt;
        let (mut a_cli, a_srv) = tcp_pair().await;
        let (_b_cli, b_srv) = tcp_pair().await;
        let up = Arc::new(PumpCounters::default());
        let down = Arc::new(PumpCounters::default());
        let (u, d) = (up.clone(), down.clone());
        let relay = tokio::spawn(async move {
            splice_relay_with_stats(
                &a_srv,
                &b_srv,
                SpliceHooks::default(),
                PumpMode::Batched,
                Some(u),
                Some(d),
            )
            .await
            .unwrap();
        });
        let payload = vec![0x77u8; 256 * 1024];
        a_cli.write_all(&payload).await.unwrap();
        // 等待搬运完成(计数收敛)
        let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        while up.fill_bytes.load(Ordering::Relaxed) < payload.len() as u64 {
            assert!(tokio::time::Instant::now() < deadline, "载荷搬运未完成");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        tokio::time::sleep(Duration::from_millis(100)).await; // 进入静默等待
        let s1 = up.pending_waits_read.load(Ordering::Relaxed);
        tokio::time::sleep(Duration::from_millis(300)).await; // 静默窗口
        let s2 = up.pending_waits_read.load(Ordering::Relaxed);
        assert!(
            s2.saturating_sub(s1) < 10,
            "静默期读侧等待计数不得增长(忙转:s1={s1} s2={s2})"
        );
        relay.abort();
    }

    /// RH_SPLICE_STATS 输出格式(v2):单行严格 JSON,新旧字段齐全;
    /// 无数据字段为 null(不以 0 冒充可观测);pending_waits_* 保留旧
    /// 「尝试」口径(直接驱动输出函数,不经 env,避免并行测试的进程级
    /// env 污染)。
    #[test]
    fn splice_stats_snapshot_json_fields() {
        let c = PumpCounters::default();
        c.fill_calls.store(3, Ordering::Relaxed);
        c.fill_bytes.store(7 * 256 * 1024, Ordering::Relaxed);
        c.fill_min_bytes.fetch_min(64 * 1024, Ordering::Relaxed);
        c.fill_max_bytes.fetch_max(256 * 1024, Ordering::Relaxed);
        c.pending_waits_write.store(5, Ordering::Relaxed);
        c.pending_polls_read.store(11, Ordering::Relaxed);
        c.wait_cycles_read.store(4, Ordering::Relaxed);
        c.wait_ns_read.store(987654321, Ordering::Relaxed);
        c.coop_yields.store(2, Ordering::Relaxed);
        let s = c.snapshot_json(
            &RelayMeta {
                mode: PumpMode::Batched,
                dir: "up",
                start_ns: 42,
                started_unix_us: Some(1_728_972_000_000_000),
                src_local: None,
                src_peer: None,
                dst_local: None,
                dst_peer: None,
            },
            142,
        );
        let v: serde_json::Value = serde_json::from_str(&s).expect("必须是合法 JSON");
        assert_eq!(v["splice_stats"], "v2");
        assert_eq!(v["dir"], "up");
        assert_eq!(v["pump_mode"], "batched");
        assert_eq!(v["start_ns"], 42);
        assert_eq!(v["started_unix_us"], 1_728_972_000_000_000u64);
        assert_eq!(v["duration_ns"], 100);
        // 时间字段自检:相对锚 + 建立墙钟 + 时长三件套固定存在且为整数
        for k in ["start_ns", "started_unix_us", "duration_ns"] {
            assert!(v.get(k).is_some_and(|x| x.is_u64()), "缺时间字段 {k}");
        }
        for k in [
            "fill_calls",
            "fill_bytes",
            "fill_min_bytes",
            "fill_max_bytes",
            "drain_calls",
            "drain_bytes",
            "drain_min_bytes",
            "drain_max_bytes",
            "eagain_fill_input_empty",
            "eagain_drain_pipe_full",
            "eagain_drain_write_full",
            "pending_waits_read",
            "pending_waits_write",
            "pending_polls_read",
            "pending_polls_write",
            "wait_cycles_read",
            "wait_cycles_write",
            "wait_ns_read",
            "wait_ns_write",
            "coop_yields",
        ] {
            assert!(
                v.get(k).is_some_and(|x| x.is_u64() || x.is_null()),
                "缺字段 {k}"
            );
        }
        // 旧字段保留(尝试口径)
        assert_eq!(v["pending_waits_write"], 5);
        // v2 真实挂起字段
        assert_eq!(v["pending_polls_read"], 11);
        assert_eq!(v["wait_cycles_read"], 4);
        assert_eq!(v["wait_ns_read"], 987654321);
        // 块分布:有成功 fill → 非 null;drain 无成功调用 → null(不冒充 0)
        assert_eq!(v["fill_min_bytes"], 64 * 1024);
        assert_eq!(v["fill_max_bytes"], 256 * 1024);
        assert!(v["drain_min_bytes"].is_null());
        assert!(v["drain_max_bytes"].is_null());
        assert_eq!(v["coop_yields"], 2);
        // 地址未采样 → null
        assert!(v["src_local"].is_null());
    }

    // ================= v2 观测回归(spec D:确定性,无进程 env 并行污染) =================

    /// D1:开关解析(纯函数,不触 env):None/空/"0" 关,其余任意值开。
    #[test]
    fn splice_stats_switch_parsing() {
        assert!(!stats_enabled_value(None));
        assert!(!stats_enabled_value(Some("")));
        assert!(!stats_enabled_value(Some("0")));
        assert!(stats_enabled_value(Some("1")));
        assert!(stats_enabled_value(Some("verbose")));
    }

    /// 受控时钟(纳秒计数由测试推进;确定性,不依赖真实时间精度)。
    struct FakeClock(Arc<AtomicU64>);
    impl WaitClock for FakeClock {
        fn now_ns(&self) -> u64 {
            self.0.load(Ordering::SeqCst)
        }
    }

    /// 伪 future:按脚本逐次返回(空脚本 = 恒 Pending)。
    struct ScriptFut {
        script: std::collections::VecDeque<Poll<()>>,
    }

    impl ScriptFut {
        fn new(script: Vec<Poll<()>>) -> Self {
            Self {
                script: script.into(),
            }
        }
    }

    impl Future for ScriptFut {
        type Output = ();
        fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<()> {
            self.script.pop_front().unwrap_or(Poll::Pending)
        }
    }

    fn noop_cx() -> Context<'static> {
        Context::from_waker(futures::task::noop_waker_ref())
    }

    /// D2:伪 future 立即 Ready:pending poll / cycle / 时长全为 0
    /// (时钟非零,排除「未走时钟」的假阴性)。
    #[test]
    fn readiness_watch_immediate_ready_counts_nothing() {
        let c = Arc::new(PumpCounters::default());
        let mut w = ReadinessWatch::new(
            ScriptFut::new(vec![Poll::Ready(())]),
            c.clone(),
            WaitDir::Read,
            Arc::new(FakeClock(Arc::new(AtomicU64::new(1_000_000)))),
        );
        let mut cx = noop_cx();
        assert!(matches!(Pin::new(&mut w).poll(&mut cx), Poll::Ready(())));
        drop(w);
        assert_eq!(c.pending_polls_read.load(Ordering::Relaxed), 0);
        assert_eq!(c.wait_cycles_read.load(Ordering::Relaxed), 0);
        assert_eq!(c.wait_ns_read.load(Ordering::Relaxed), 0);
        // 对向方向计数不受影响
        assert_eq!(c.pending_polls_write.load(Ordering::Relaxed), 0);
        assert_eq!(c.wait_cycles_write.load(Ordering::Relaxed), 0);
    }

    /// D3:一 Pending 后 Ready:poll=1、cycle=1、时长按受控时钟结算,
    /// Ready 后 drop 不重复累计。
    #[test]
    fn readiness_watch_one_pending_then_ready_settles() {
        let c = Arc::new(PumpCounters::default());
        let ticks = Arc::new(AtomicU64::new(100));
        let mut w = ReadinessWatch::new(
            ScriptFut::new(vec![Poll::Pending, Poll::Ready(())]),
            c.clone(),
            WaitDir::Write,
            Arc::new(FakeClock(ticks.clone())),
        );
        let mut cx = noop_cx();
        assert!(matches!(Pin::new(&mut w).poll(&mut cx), Poll::Pending));
        ticks.store(100 + 1_000_000, Ordering::SeqCst); // 受控挂起 1ms
        assert!(matches!(Pin::new(&mut w).poll(&mut cx), Poll::Ready(())));
        drop(w);
        assert_eq!(c.pending_polls_write.load(Ordering::Relaxed), 1);
        assert_eq!(c.wait_cycles_write.load(Ordering::Relaxed), 1);
        assert_eq!(c.wait_ns_write.load(Ordering::Relaxed), 1_000_000);
    }

    /// D4:多次 Pending:poll 数(3)与周期数(1)不同且精确;时长 =
    /// 首次 Pending → 结束(受控时钟精确断言)。
    #[test]
    fn readiness_watch_multi_pending_distinguishes_polls_and_cycles() {
        let c = Arc::new(PumpCounters::default());
        let ticks = Arc::new(AtomicU64::new(1_000));
        let mut w = ReadinessWatch::new(
            ScriptFut::new(vec![
                Poll::Pending,
                Poll::Pending,
                Poll::Pending,
                Poll::Ready(()),
            ]),
            c.clone(),
            WaitDir::Read,
            Arc::new(FakeClock(ticks.clone())),
        );
        let mut cx = noop_cx();
        // 三次 Pending,期间受控推进时钟(周期起点固定在首次 = 1000)
        for t in [1_010u64, 1_020, 1_030] {
            assert!(matches!(Pin::new(&mut w).poll(&mut cx), Poll::Pending));
            ticks.store(t, Ordering::SeqCst);
        }
        ticks.store(1_060, Ordering::SeqCst);
        assert!(matches!(Pin::new(&mut w).poll(&mut cx), Poll::Ready(())));
        drop(w);
        assert_eq!(c.pending_polls_read.load(Ordering::Relaxed), 3);
        assert_eq!(c.wait_cycles_read.load(Ordering::Relaxed), 1);
        assert_eq!(c.wait_ns_read.load(Ordering::Relaxed), 60);
    }

    /// D5:等待被 drop(未就绪即丢弃):已发生挂起必须结算,不因丢弃遗失。
    #[test]
    fn readiness_watch_drop_after_pending_settles() {
        let c = Arc::new(PumpCounters::default());
        let ticks = Arc::new(AtomicU64::new(7));
        let mut w = ReadinessWatch::new(
            ScriptFut::new(vec![Poll::Pending, Poll::Pending]),
            c.clone(),
            WaitDir::Read,
            Arc::new(FakeClock(ticks.clone())),
        );
        let mut cx = noop_cx();
        assert!(matches!(Pin::new(&mut w).poll(&mut cx), Poll::Pending));
        ticks.store(7 + 42, Ordering::SeqCst);
        drop(w); // 未 Ready 即 drop(取消形态)
        assert_eq!(c.pending_polls_read.load(Ordering::Relaxed), 1);
        assert_eq!(c.wait_cycles_read.load(Ordering::Relaxed), 1);
        assert_eq!(c.wait_ns_read.load(Ordering::Relaxed), 42);
    }

    /// 屏障伪 future:首次 poll 置位并恒 Pending(select 取消集成形态用)。
    struct BarrierPendingFut(Arc<AtomicBool>);
    impl Future for BarrierPendingFut {
        type Output = ();
        fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<()> {
            self.0.store(true, Ordering::SeqCst);
            Poll::Pending
        }
    }

    /// D5:select 取消路径。取消仅在包装 future ≥1 次 Pending 后触发
    /// (屏障 + yield,无 sleep 抢跑);被 pin 的 wrapper 由显式词法作用域
    /// 持有,**析构完成后**才断言:poll ≥ 1、cycle=1、FakeClock 精确结算、
    /// Drop 不重复累计。
    #[tokio::test]
    async fn readiness_watch_select_cancel_settles_no_leak() {
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let polled = Arc::new(AtomicBool::new(false));
        let armed = polled.clone();
        tokio::spawn(async move {
            while !armed.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
            let _ = tx.send(());
        });
        let c = Arc::new(PumpCounters::default());
        let ticks = Arc::new(AtomicU64::new(1_000));
        {
            let w = ReadinessWatch::new(
                BarrierPendingFut(polled),
                c.clone(),
                WaitDir::Read,
                Arc::new(FakeClock(ticks.clone())),
            );
            tokio::pin!(w);
            tokio::select! {
                _ = &mut w => panic!("屏障 future 不应就绪"),
                _ = rx => {}
            }
            ticks.store(1_500, Ordering::SeqCst); // 受控挂起窗口 500ns
        } // 作用域结束:wrapper 析构(PinnedDrop 结算)先于下方断言
        ticks.store(9_999, Ordering::SeqCst); // 若 Drop 重复结算,此值会被计入
        assert!(c.pending_polls_read.load(Ordering::Relaxed) >= 1);
        assert_eq!(c.wait_cycles_read.load(Ordering::Relaxed), 1);
        assert_eq!(
            c.wait_ns_read.load(Ordering::Relaxed),
            500,
            "取消前已发生的等待必须精确结算且不重复"
        );
    }

    /// D6:默认无统计路径(计数全 None,与生产 RH_SPLICE_STATS 未设等价,
    /// 不经 env):默认臂(Batched)传输 + 半关闭不退化。
    #[tokio::test]
    async fn splice_relay_stats_off_transfers_and_half_close() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let (user_cli, user_srv) = tcp_pair().await;
        let (tgt_conn, mut tgt_echo) = tcp_pair().await;
        tokio::spawn(async move {
            let mut buf = vec![0u8; 4096];
            loop {
                match tgt_echo.read(&mut buf).await {
                    Ok(0) | Err(_) => break,
                    Ok(n) => {
                        if tgt_echo.write_all(&buf[..n]).await.is_err() {
                            break;
                        }
                    }
                }
            }
        });
        let relay = tokio::spawn(async move {
            splice_relay_inner(
                &user_srv,
                &tgt_conn,
                SpliceHooks::default(),
                PumpMode::Batched,
            )
            .await
            .unwrap();
        });
        let payload = vec![0xc1u8; 128 * 1024];
        let (mut r_half, mut w_half) = user_cli.into_split();
        let recv = tokio::spawn(async move {
            let mut got = Vec::new();
            let _ = r_half.read_to_end(&mut got).await;
            got
        });
        w_half.write_all(&payload).await.unwrap();
        w_half.shutdown().await.unwrap();
        let got = recv.await.unwrap();
        assert_eq!(got.len(), payload.len());
        assert!(got.iter().all(|&b| b == 0xc1));
        relay.await.unwrap();
    }
}
