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

/// 池内单缓冲目标容量(对齐 split_send_size 默认档)。
const CLASS: usize = 64 * 1024;
/// 接收纳管的最大容量(标准类上界 = CLASS + 分配器对齐余量)。
const CLASS_MAX: usize = CLASS + 8 * 1024;
/// 空闲缓冲总条目上限(全部分片合计)。
const MAX_IDLE: usize = 256;
/// 归还的最小容量(过小缓冲不值得复用)。
const MIN_RECYCLE: usize = 16 * 1024;
/// 分片数(2 的幂,轮转指针位与即可)。
const SHARDS: usize = 8;

// B3 字节上界静态锁:池内条目 capacity ≤ CLASS_MAX、条目数 ≤ MAX_IDLE,
// 总驻留 ≤ 256 × 72KiB = 18MiB(与模块文档口径一致)。
const _: () = assert!(MAX_IDLE * CLASS_MAX <= 20 * 1024 * 1024);

/// 分片聚合缓冲池实例。
pub(crate) struct Pool {
    shards: [Mutex<Vec<BytesMut>>; SHARDS],
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
            let mut skipped: Vec<BytesMut> = Vec::new();
            let mut found = None;
            while let Some(mut b) = g.pop() {
                b.clear();
                if b.capacity() >= min {
                    found = Some(b);
                    break;
                }
                skipped.push(b);
            }
            // 容量不足的条目放回原片(数量不变,不超片上限)
            for b in skipped {
                g.push(b);
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
