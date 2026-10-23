//! 发送/接收聚合缓冲池。
//!
//! 高吞吐场景每秒可产生数万个数据帧,逐帧 `Vec` 分配/释放(含 64KiB 段的
//! memset 清零)带来显著 CPU 与内存抖动。池以 `BytesMut` 为单位复用:
//! 发送侧(`Stream::poll_write` / chunk 路径)取出作帧体,
//! `frame::io` 写穿后 `try_into_mut` 归还;接收聚合侧(mux down 合并写)
//! 同池取还。引用计数不为 1(帧被共享)时自然走正常释放,不影响正确性。
//!
//! Y2 分片:全转发任务共用本池,单锁在高并发(多流多任务)下是争用点;
//! 按轮转指针分 8 片独立锁,片空闲条目上限 = MAX_IDLE/SHARDS。
//!
//! goal-repair B2/B3 修复:
//! * B2(取还失配):`get` 与 `put` 各自推进轮转指针,串行 get→use→put
//!   落在不同片,旧实现 `get` 只查起始片 → 池有存货也持续新分配。
//!   现以「起始片 miss 时扫描其余片」兜底,put 落哪片不再影响命中;
//!   容量匹配仍由逐条 `capacity() >= min` 判定完成(非键哈希)。
//! * B3(字节口径上界):归还容量纳入 [MIN_RECYCLE, CLASS_MAX] 才收纳,
//!   超限(含外部播种的超大缓冲)一律拒收;池内总驻留字节因此有静态
//!   上界 MAX_IDLE × CLASS_MAX ≈ 18MiB(单条 ≤ 64KiB+8KiB 对齐余量、
//!   条目数 ≤ 256),杜绝超限条目经 `get` 外溢(A4 池侧合同)。
//!
//! 实例化(补修):池逻辑收敛为 [`Pool`] 实例,进程内全局共享一个
//! `static POOL`(`get`/`put` 薄委托,公共签名不变);测试以私有实例
//! 断言语义,与全局池及并行测试完全隔离,默认并行 `cargo test` 可靠通过。

use bytes::BytesMut;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

/// 池内小类目标容量(对齐 split_send_size 默认档)。
const CLASS: usize = 64 * 1024;
/// 小类接收纳管的最大容量(标准类上界 = CLASS + 分配器对齐余量)。
const CLASS_MAX: usize = CLASS + 8 * 1024;
/// 空闲缓冲条目上限(小类,全部分片合计)。
const MAX_IDLE: usize = 256;
/// 归还的最小容量(过小缓冲不值得复用)。
const MIN_RECYCLE: usize = 16 * 1024;

// ---- C4:大缓冲类 ----
//
// 上行中继读块默认 256KiB(`mux.rs::up_chunk_cap`),超出小类 CLASS_MAX,
// 于是 `get(256KiB)` 必然 miss、每块新分配,池在主力上行路径上零命中。
// 这里加一个大类:容量落在 (CLASS_MAX, LARGE_MAX] 的缓冲单独归集。
//
// 字节上界:大类的条目数按总字节预算约束(不超过 LARGE_BUDGET),而非
// 条目数 —— 1MiB 级缓冲按 64 条封顶会到 64MiB,这里显式压到 32MiB。
const LARGE_MAX: usize = 1024 * 1024 + 8 * 1024;
const LARGE_BUDGET: usize = 32 * 1024 * 1024;
const MAX_IDLE_LARGE: usize = 64;
/// 分片数(2 的幂,轮转指针位与即可)。
const SHARDS: usize = 8;

// B3 字节上界静态锁:小类条目 capacity ≤ CLASS_MAX、条目数 ≤ MAX_IDLE,
// 总驻留 ≤ 256 × 72KiB = 18MiB(与模块文档口径一致)。
const _: () = assert!(MAX_IDLE * CLASS_MAX <= 20 * 1024 * 1024);
// C4 大类的静态上界同样锁死。
const _: () = assert!(MAX_IDLE_LARGE * LARGE_MAX <= 72 * 1024 * 1024);

/// 分片聚合缓冲池实例(小类 + 大类)。
pub(crate) struct Pool {
    shards: [Mutex<Vec<BytesMut>>; SHARDS],
    large: [Mutex<Vec<BytesMut>>; SHARDS],
    /// 大类驻留字节(粗粒度,仅用于预算判定;Relaxed 足够)。
    large_bytes: AtomicUsize,
    rotor: AtomicUsize,
    /// 新分配计数(诊断缝:命中率回归断言用;仅 miss 新分配路径计数,
    /// 开销相对 64KiB 分配可忽略)。
    allocations: AtomicUsize,
}

impl Pool {
    pub(crate) const fn new() -> Self {
        Pool {
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
            large: [
                Mutex::new(Vec::new()),
                Mutex::new(Vec::new()),
                Mutex::new(Vec::new()),
                Mutex::new(Vec::new()),
                Mutex::new(Vec::new()),
                Mutex::new(Vec::new()),
                Mutex::new(Vec::new()),
                Mutex::new(Vec::new()),
            ],
            large_bytes: AtomicUsize::new(0),
            rotor: AtomicUsize::new(0),
            allocations: AtomicUsize::new(0),
        }
    }

    fn lock_shard(&self, idx: usize) -> std::sync::MutexGuard<'_, Vec<BytesMut>> {
        match self.shards[idx].lock() {
            Ok(g) => g,
            // 中毒池:继续用,无副作用
            Err(e) => e.into_inner(),
        }
    }

    /// 取一个容量 ≥ min 的缓冲(空内容)。
    ///
    /// B2:先试轮转起始片,miss 时扫描其余片(容量 ≥ min 逐条判定),
    /// 全部 miss 才新分配;put 落哪片不再影响后续 get 命中。
    pub(crate) fn get(&self, min: usize) -> BytesMut {
        if min > CLASS_MAX && min <= LARGE_MAX {
            // C4:大缓冲类 —— 上行 256KiB 读块走这里命中,不再每块新分配。
            let start = self.rotor.fetch_add(1, Ordering::Relaxed) & (SHARDS - 1);
            for i in 0..SHARDS {
                let idx = (start + i) & (SHARDS - 1);
                let mut g = match self.large[idx].lock() {
                    Ok(g) => g,
                    Err(e) => e.into_inner(),
                };
                if let Some(b) = g.pop() {
                    // 弹出即减预算(put 侧按 cap 累加,这里按实际 capacity 扣回)。
                    // 修前 get 不减:每次 get→put 净增一块容量,约 128 块后
                    // `cur + cap > LARGE_BUDGET` 恒成立,put 永远拒收,
                    // 大类池在稳态下被自己的计数器关停(每块重新分配)。
                    self.large_bytes.fetch_sub(b.capacity(), Ordering::Relaxed);
                    drop(g);
                    return b;
                }
            }
            self.allocations.fetch_add(1, Ordering::Relaxed);
            return BytesMut::with_capacity(min);
        }
        if min > CLASS_MAX {
            // 池内条目容量恒 ≤ CLASS_MAX,必然 miss:跳过扫描,避免
            // 途中误淘汰较小条目。
            self.allocations.fetch_add(1, Ordering::Relaxed);
            return BytesMut::with_capacity(min);
        }
        let start = self.rotor.fetch_add(1, Ordering::Relaxed) & (SHARDS - 1);
        for i in 0..SHARDS {
            let idx = (start + i) & (SHARDS - 1);
            let mut g = self.lock_shard(idx);
            // round-30:用栈上固定数组替代 Vec,避免每次 get 一次堆分配。
            // 片上限 MAX_IDLE/SHARDS = 32,故 32 槽足够容纳全部跳过项。
            const SLOTS: usize = MAX_IDLE / SHARDS;
            let mut skipped: [Option<BytesMut>; SLOTS] = [const { None }; SLOTS];
            let mut n_skipped = 0usize;
            let mut found = None;
            while let Some(mut b) = g.pop() {
                b.clear();
                if b.capacity() >= min {
                    found = Some(b);
                    break;
                }
                if n_skipped < SLOTS {
                    skipped[n_skipped] = Some(b);
                    n_skipped += 1;
                } else {
                    // 理论不可达(片内条目 ≤ SLOTS);防御性放回,不丢缓冲
                    g.push(b);
                }
            }
            // 容量不足的条目放回原片(数量不变,不超片上限)
            for slot in skipped.iter_mut().take(n_skipped) {
                if let Some(b) = slot.take() {
                    g.push(b);
                }
            }
            drop(g);
            if let Some(b) = found {
                return b;
            }
        }
        self.allocations.fetch_add(1, Ordering::Relaxed);
        BytesMut::with_capacity(min.max(CLASS))
    }

    /// 归还缓冲;容量过小或超限(B3:字节口径上界)、或池满则丢弃(正常释放)。
    pub(crate) fn put(&self, b: BytesMut) {
        let cap = b.capacity();
        if cap > CLASS_MAX {
            if cap > LARGE_MAX {
                return;
            }
            // C4:大类归还,按总字节预算封顶。
            let cur = self.large_bytes.load(Ordering::Relaxed);
            if cur + cap > LARGE_BUDGET
                || self
                    .large_bytes
                    .compare_exchange(cur, cur + cap, Ordering::Relaxed, Ordering::Relaxed)
                    .is_err()
            {
                // 预算已满或并发更新失败:放弃复用(直接释放)。
                return;
            }
            let start = self.rotor.fetch_add(1, Ordering::Relaxed) & (SHARDS - 1);
            for i in 0..SHARDS {
                let idx = (start + i) & (SHARDS - 1);
                let mut g = match self.large[idx].lock() {
                    Ok(g) => g,
                    Err(e) => e.into_inner(),
                };
                if g.len() < MAX_IDLE_LARGE / SHARDS {
                    g.push(b);
                    return;
                }
            }
            self.large_bytes.fetch_sub(cap, Ordering::Relaxed);
            return;
        }
        if !(MIN_RECYCLE..=CLASS_MAX).contains(&cap) {
            return;
        }
        let start = self.rotor.fetch_add(1, Ordering::Relaxed) & (SHARDS - 1);
        for i in 0..SHARDS {
            let idx = (start + i) & (SHARDS - 1);
            let mut g = self.lock_shard(idx);
            if g.len() < MAX_IDLE / SHARDS {
                g.push(b);
                return;
            }
        }
    }

    /// 新分配计数(测试诊断缝)。
    #[cfg(test)]
    pub(crate) fn allocations(&self) -> usize {
        self.allocations.load(Ordering::Relaxed)
    }
}

static POOL: Pool = Pool::new();

/// 取一个容量 ≥ min 的缓冲(空内容)。见 [`Pool::get`]。
pub fn get(min: usize) -> BytesMut {
    POOL.get(min)
}

/// 归还缓冲;容量过小或超限、或池满则丢弃(正常释放)。见 [`Pool::put`]。
pub fn put(b: BytesMut) {
    POOL.put(b)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// B2(红→绿):串行 get→use→put 之后,下一次 get 必须命中刚归还的缓冲。
    /// 修复前 get/put 各自推进 ROTOR,连续 get→put 落在不同片,
    /// get 只查自己那片 → 池有存货也持续新分配(指针必不相等)。
    /// 私有实例:与全局池及并行测试完全隔离(默认 cargo test 可靠通过)。
    #[test]
    fn pool_roundtrip_reuse() {
        let pool = Pool::new();
        for round in 0..16u8 {
            let mut b = pool.get(CLASS);
            let marker = b.as_ptr();
            b.extend_from_slice(&[round; 1024]);
            assert_eq!(b.len(), 1024);
            b.clear();
            pool.put(b);
            let b2 = pool.get(CLASS);
            assert_eq!(
                b2.as_ptr(),
                marker,
                "get must reuse the buffer just put back (B2 rotor parity mismatch)"
            );
            assert!(b2.is_empty());
        }
    }

    /// B3(红→绿):put 拒收超限缓冲。播种 SHARDS 条 256KiB(修复前
    /// 覆盖全部分片)后,任何 get 都不得返回超限容量——池内不得
    /// 存在字节口径超限的条目(A4 池侧合同)。
    #[test]
    fn put_oversized_rejected() {
        let pool = Pool::new();
        for _ in 0..SHARDS {
            pool.put(BytesMut::with_capacity(256 * 1024));
        }
        for _ in 0..MAX_IDLE {
            let b = pool.get(CLASS);
            assert!(
                b.capacity() <= CLASS_MAX,
                "pool returned oversized buffer capacity {} (B3 byte-cap violated)",
                b.capacity()
            );
        }
    }

    /// B3 字节上界:混入一批超限与正常缓冲后,get 返回的容量恒有界,
    /// 且池可回收条目的容量不超过标准类上界(总驻留 ≤ MAX_IDLE×CLASS_MAX)。
    #[test]
    fn pool_byte_cap() {
        let pool = Pool::new();
        // 混合播种:超限条目(应被拒收)+ 标准条目
        for i in 0..(MAX_IDLE + SHARDS) {
            let cap = if i % 2 == 0 { 256 * 1024 } else { CLASS };
            pool.put(BytesMut::with_capacity(cap));
        }
        let mut max_seen = 0usize;
        for _ in 0..MAX_IDLE {
            let b = pool.get(CLASS);
            max_seen = max_seen.max(b.capacity());
            assert!(
                b.capacity() <= CLASS_MAX,
                "capacity {} exceeds standard class bound",
                b.capacity()
            );
            pool.put(b); // 正常归还,稳态复用
        }
        assert!(
            max_seen >= MIN_RECYCLE,
            "standard entries must be recyclable"
        );
    }

    /// C4(红→绿):大类稳态复用。修前 `get` 弹出大缓冲不减 `large_bytes`,
    /// 每次 get→put 净增一块容量,约 128 块后预算恒满、put 永远拒收,
    /// 之后每次 get 都是新分配。串行 get(256KiB)→put 交叉多轮,
    /// 新分配数必须远小于操作数(与 pool_hit_rate_microbench 同型,
    /// 但用大类容量 —— 修前该测试因计数器泄漏恒红)。
    #[test]
    fn pool_large_class_steady_reuse() {
        let pool = Pool::new();
        const LARGE: usize = 256 * 1024;
        const M: usize = 1000;
        let before = pool.allocations();
        for _ in 0..M {
            let mut b = pool.get(LARGE);
            debug_assert!(b.capacity() >= LARGE);
            b.extend_from_slice(&[1u8; 64]);
            b.clear();
            pool.put(b);
        }
        let delta = pool.allocations() - before;
        assert!(
            delta * 20 < M,
            "large-class steady-state allocations {delta} for {M} ops — budget leak killed reuse (C4)"
        );
    }

    /// B2 性能中性(命中率锁,诊断缝类):**并发** get/put 循环下,
    /// 稳态必须以复用为主,新分配次数远小于操作数。多线程压力保留
    /// (4 线程 × 2000 次,共享同一池实例);实例级 allocations 计数
    /// 不受全局池及其它测试干扰。
    /// 红取证由同根缺陷的 pool_roundtrip_reuse 承担
    /// (计数缝随修复引入,修前无法编译)。
    #[test]
    fn pool_hit_rate_microbench() {
        let pool = std::sync::Arc::new(Pool::new());
        const THREADS: usize = 4;
        const M: usize = 2000;
        let before = pool.allocations();
        std::thread::scope(|scope| {
            for _ in 0..THREADS {
                let pool = pool.clone();
                scope.spawn(move || {
                    for _ in 0..M {
                        let mut b = pool.get(CLASS);
                        b.extend_from_slice(&[1u8; 64]);
                        b.clear();
                        pool.put(b);
                    }
                });
            }
        });
        let delta = pool.allocations() - before;
        let total = THREADS * M;
        assert!(
            delta * 20 < total,
            "steady-state allocations {delta} for {total} ops — pool reuse broken (B2)"
        );
    }
}
