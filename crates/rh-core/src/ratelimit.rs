//! 异步令牌桶限速(带宽限制 bandwidthLimit 的实现)。
//!
//! 按字节补充令牌;`acquire(n)` 等待凑足 n 个字节额度。
//! 读写循环每次搬运前 acquire(至多 chunk),从而平滑限制吞吐。
//!
//! goal-repair A5:既有 `acquire` 在 future 被 drop(取消)后已扣令牌不退还,
//! 直接放进 `select!` 会在取消路径丢令牌。新增预留式取消安全 API
//! [`TokenBucket::acquire_reserved`]:等待期间取消自动退还已扣部分,
//! 完成后经 [`Reservation`] 显式消费/退款(送达计费合同)。
//! 既有 `acquire`/`take` 的签名与语义不变(copy 路径照旧)。

#[cfg(test)]
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::Duration;
use tokio::time::Instant;

#[derive(Debug)]
pub struct TokenBucket {
    /// 同步锁:临界区内无 await(refund 需在 Drop 上下文同步执行),
    /// 持有时间为一小段算术,无争用风险。
    state: Mutex<State>,
    capacity: f64,
    rate_per_sec: f64,
    /// 已消费且未退还的令牌累计(诊断/测试口径:退款递减)。
    /// 仅测试读(消费记账断言);R1:cfg(test) 门控,生产不付原子 RMW。
    #[cfg(test)]
    consumed_total: AtomicU64,
}

#[derive(Debug)]
struct State {
    tokens: f64,
    last: Instant,
}

impl TokenBucket {
    /// rate:字节/秒。容量 = 1 秒的量(允许小突发)。
    pub fn new(bytes_per_sec: f64) -> Self {
        Self {
            state: Mutex::new(State {
                tokens: bytes_per_sec,
                last: Instant::now(),
            }),
            capacity: bytes_per_sec.max(1.0),
            rate_per_sec: bytes_per_sec.max(0.001),
            #[cfg(test)]
            consumed_total: AtomicU64::new(0),
        }
    }

    /// 非阻塞地取 up to n 字节额度,返回实际可用量(0..=n)。
    pub async fn take(&self, n: usize) -> usize {
        let mut s = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let now = Instant::now();
        let elapsed = now.duration_since(s.last).as_secs_f64();
        s.tokens = (s.tokens + elapsed * self.rate_per_sec).min(self.capacity);
        s.last = now;
        let avail = s.tokens.floor() as usize;
        let granted = avail.min(n);
        s.tokens -= granted as f64;
        drop(s);
        #[cfg(test)]
        if granted > 0 {
            self.consumed_total
                .fetch_add(granted as u64, Ordering::Relaxed);
        }
        granted
    }

    /// 等待直到凑足 n 字节额度(n 超过容量时按容量分批)。
    pub async fn acquire(&self, n: usize) {
        let mut remain = n;
        while remain > 0 {
            let got = self.take(remain).await;
            remain -= got;
            if remain > 0 {
                let wait = (remain as f64 / self.rate_per_sec).min(1.0);
                tokio::time::sleep(Duration::from_secs_f64(wait.max(0.001))).await;
            }
        }
    }

    /// 已消费且未退还的令牌累计(退款递减;诊断/回归断言用)。
    #[cfg(test)]
    pub fn consumed_total(&self) -> u64 {
        self.consumed_total.load(Ordering::Relaxed)
    }

    /// 退还 n 个令牌(预留未消费部分;同步——Drop 上下文调用)。
    fn refund(&self, n: usize) {
        if n == 0 {
            return;
        }
        let mut s = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let now = Instant::now();
        let elapsed = now.duration_since(s.last).as_secs_f64();
        s.tokens = (s.tokens + elapsed * self.rate_per_sec + n as f64).min(self.capacity);
        s.last = now;
        drop(s);
        #[cfg(test)]
        self.consumed_total.fetch_sub(n as u64, Ordering::Relaxed);
    }

    fn rate(&self) -> f64 {
        self.rate_per_sec
    }

    /// 预留式取消安全获取:等待凑足 n 额度,返回 [`Reservation`]。
    /// * 等待期间 future 被 drop:已扣部分自动退还(不丢令牌);
    /// * 完成后 `consume(n)` 消费(不可退);drop 未消费部分即退还。
    pub async fn acquire_reserved(self: std::sync::Arc<Self>, n: usize) -> Reservation {
        let mut held = Held {
            bucket: Some(self.clone()),
            held: 0,
        };
        let mut remain = n;
        while remain > 0 {
            let got = self.take(remain).await;
            held.held += got;
            remain -= got;
            if remain > 0 {
                let wait = (remain as f64 / self.rate()).min(1.0);
                tokio::time::sleep(Duration::from_secs_f64(wait.max(0.001))).await;
            }
        }
        Reservation(held)
    }
}

/// 已扣未消费的令牌(释放 = 退还)。
struct Held {
    bucket: Option<std::sync::Arc<TokenBucket>>,
    held: usize,
}

impl Drop for Held {
    fn drop(&mut self) {
        if self.held > 0 {
            if let Some(b) = self.bucket.take() {
                b.refund(self.held);
            }
        }
    }
}

/// 预留句柄:三态合同(预留可见 / 消费不可退 / drop 未消费即退)。
pub struct Reservation(Held);

impl Reservation {
    /// 消费 n 个已预留令牌(不可退;超出持有量按持有量截断)。
    pub fn consume(&mut self, n: usize) {
        let n = n.min(self.0.held);
        self.0.held -= n;
        if self.0.held == 0 {
            // 置空桶引用:Drop 不再退款(已全部消费)
            self.0.bucket = None;
        }
    }

    /// 当前未消费的预留量(预留可见性)。
    pub fn held(&self) -> usize {
        self.0.held
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[tokio::test]
    async fn rate_limit_basic() {
        // 1MB/s,容量 1MB
        let bucket = TokenBucket::new(1024.0 * 1024.0);
        // 首秒容量内一次取满
        assert_eq!(bucket.take(1024 * 1024).await, 1024 * 1024);
        // 已耗尽:两次调用间的微秒级流逝只可能补回极少量
        assert!(bucket.take(1024).await < 512);
        tokio::time::sleep(Duration::from_millis(30)).await;
        let got = bucket.take(1024 * 1024).await;
        // ~30ms 补充 ~30KB
        assert!(got > 10_000 && got < 60_000, "got={got}");
    }

    // A5 前置:Reservation 三态合同(预留可见、消费不可退、drop 未消费即退)。
    #[tokio::test]
    async fn token_reservation_cancel_safe() {
        // 容量充足:预留即时完成
        let bucket = Arc::new(TokenBucket::new(1024.0 * 1024.0));
        let mut r = bucket.clone().acquire_reserved(4096).await;
        assert_eq!(r.held(), 4096, "预留可见");
        assert_eq!(bucket.consumed_total(), 4096);
        r.consume(1000);
        assert_eq!(r.held(), 3096, "消费后余量可见");
        assert_eq!(
            bucket.consumed_total(),
            4096,
            "消费不改变累计(已在 take 计入)"
        );
        drop(r); // 未消费的 3096 退还
        assert_eq!(bucket.consumed_total(), 1000, "drop 未消费即退");

        // 消费后 drop:不再退款
        let mut r2 = bucket.clone().acquire_reserved(512).await;
        r2.consume(512);
        drop(r2);
        assert_eq!(bucket.consumed_total(), 1512, "消费不可退");
    }

    // A5:等待期间取消(future drop)不丢已扣令牌。
    #[tokio::test]
    async fn token_reservation_wait_cancel_refunds() {
        // 100 B/s 小桶:预留 100000 必然进入等待
        let bucket = Arc::new(TokenBucket::new(100.0));
        let b2 = bucket.clone();
        let wait = tokio::spawn(async move { b2.acquire_reserved(100_000).await });
        tokio::time::sleep(Duration::from_millis(50)).await; // 已扣 ~100(容量),正在等待
        wait.abort(); // 取消等待中的预留 future
        let _ = wait.await;
        // 已扣部分全额退还:消耗清零(调度窗口内新补充的令牌未被任何 acquire 消费)
        assert_eq!(
            bucket.consumed_total(),
            0,
            "等待期间取消必须退还已扣令牌(不丢令牌)"
        );
    }

    // 预留式获取与既有 acquire 的等效性(无取消全量运行)。
    #[tokio::test]
    async fn reserved_matches_acquire_on_full_run() {
        let a = Arc::new(TokenBucket::new(64.0 * 1024.0));
        let b = TokenBucket::new(64.0 * 1024.0);
        let mut ra = a.clone().acquire_reserved(100_000).await;
        ra.consume(100_000);
        b.acquire(100_000).await;
        assert_eq!(a.consumed_total(), 100_000, "预留式与既有 acquire 全量等效");
    }
}
