use std::{cmp, sync::Arc};

use parking_lot::Mutex;
use web_time::Instant;

use crate::{connection::rtt::Rtt, Config, ConnectionError, DEFAULT_CREDIT};

#[derive(Debug)]
pub(crate) struct FlowController {
    config: Arc<Config>,
    last_window_update: Instant,
    /// See [`Connection::rtt`].
    rtt: Rtt,
    /// See [`Connection::accumulated_max_stream_windows`].
    accumulated_max_stream_windows: Arc<Mutex<usize>>,
    receive_window: u32,
    max_receive_window: u32,
    send_window: u32,
    /// round-2 预授信:建流抬高的 (max − DEFAULT_CREDIT) 差额尚未经
    /// 窗口更新帧授出前为 `true`。round-4/B1 起该差额在流创建时即由
    /// [`FlowController::take_creation_grant`] 授出(排队失败的兜底会
    /// 回滚本标记,差额回落由读路径授出)。此差额不是实测接收吞吐,
    /// 在它授出之前不进入 BDP 自增窗——否则首个 WU 会以
    /// "建流瞬间消耗了初窗" 的假速率立刻翻倍。
    pregrant_pending: bool,
}

/// 连接级"超出规约保底"的窗口总预算:
/// `max_connection_receive_window − 256KiB × max_num_streams`(None =
/// 不限)。所有减法/乘法饱和运算:`max_num_streams` 上限 usize::MAX 时
/// 乘积封顶,预算归 0(预授信/自增窗退回规约保底,不 panic、不回绕)。
fn connection_budget(config: &Config) -> usize {
    config
        .max_connection_receive_window
        .unwrap_or(usize::MAX)
        .saturating_sub(
            config
                .max_num_streams
                .saturating_mul(DEFAULT_CREDIT as usize),
        )
}

impl FlowController {
    pub(crate) fn new(
        receive_window: u32,
        send_window: u32,
        accumulated_max_stream_windows: Arc<Mutex<usize>>,
        rtt: Rtt,
        config: Arc<Config>,
    ) -> Self {
        // 预授信初窗(round-2):max_receive_window 从配置初窗起步(默认
        // DEFAULT_CREDIT = upstream 原行为)。receive_window 仍按规约从
        // DEFAULT_CREDIT 开始——(初窗 − 256KiB) 的差额自 round-4/B1 起
        // 由 take_creation_grant 在建流时以带初始 flag 的 WU 帧授给对端
        //(排队失败的兜底回落读路径 next_window_update:max − receive_window
        // ≥ max/2 恒成立,半窗阈值同样放行),对端任何版本都只见到标准 WU。
        //
        // 预授信必须先吃连接预算的剩余额度:预算
        // (max_connection_receive_window − 256KiB×max_num_streams)是
        // 所有流共享的"超出保底"总额,N 条流各自预授信不得穿底——
        // 否则 next_window_update 的连接限额减法下溢(修前:无条件入账,
        // 累计值可超预算),且 4096 流会预留不受控的额外信用。额度不足
        // 时退回规约保底 DEFAULT_CREDIT(每流保底仍由 yamux 断言保证)。
        let wanted = (config.initial_stream_receive_window as u32).max(DEFAULT_CREDIT) as usize;
        let mut accumulated = accumulated_max_stream_windows.lock();
        let room = connection_budget(&config).saturating_sub(*accumulated);
        let extra = room.min(wanted - DEFAULT_CREDIT as usize);
        let max_receive_window = u32::try_from(DEFAULT_CREDIT as usize + extra)
            .expect("DEFAULT_CREDIT + budget extra fits u32 (both <= u32::MAX checked)");
        // 记账:assert_invariants 与 Drop 都按 (max − DEFAULT_CREDIT) 计
        // 入/扣回累计计数器,起步差额必须在此入账(与 Drop 守恒)。
        *accumulated += (max_receive_window - DEFAULT_CREDIT) as usize;
        drop(accumulated);
        Self {
            receive_window,
            send_window,
            config,
            rtt,
            accumulated_max_stream_windows,
            max_receive_window,
            last_window_update: Instant::now(),
            pregrant_pending: max_receive_window > DEFAULT_CREDIT,
        }
    }

    /// Calculate the number of additional window bytes the receiving side (local) should grant the
    /// sending side (remote) via a window update message.
    ///
    /// Returns `None` if too small to justify a window update message.
    pub(crate) fn next_window_update(&mut self, buffer_len: usize) -> Option<u32> {
        self.assert_invariants(buffer_len);

        let bytes_received = self.max_receive_window - self.receive_window;
        let mut next_window_update =
            bytes_received.saturating_sub(buffer_len.try_into().unwrap_or(u32::MAX));

        // Don't send an update in case half or more of the window is still available to the sender.
        if next_window_update < self.max_receive_window / 2 {
            return None;
        }

        log::trace!(
            "received {} mb in {} seconds ({} mbit/s)",
            next_window_update as f64 / crate::MIB as f64,
            self.last_window_update.elapsed().as_secs_f64(),
            next_window_update as f64 / crate::MIB as f64 * 8.0
                / self.last_window_update.elapsed().as_secs_f64()
        );

        // Auto-tuning `max_receive_window`
        //
        // The ideal `max_receive_window` is equal to the bandwidth-delay-product (BDP), thus
        // allowing the remote sender to exhaust the entire available bandwidth on a single stream.
        // Choosing `max_receive_window` too small prevents the remote sender from exhausting the
        // available bandwidth. Choosing `max_receive_window` to large is wasteful and delays
        // backpressure from the receiver to the sender on the stream.
        //
        // In case the remote sender has exhausted half or more of its credit in less than 2
        // round-trips, try to double `max_receive_window`.
        //
        // For simplicity `max_receive_window` is never decreased.
        //
        // This implementation is heavily influenced by QUIC. See document below for rational on the
        // above strategy.
        //
        // https://docs.google.com/document/d/1F2YfdDXKpy20WVKJueEf4abn_LVZHhMUMS5gX6Pgjl4/edit?usp=sharing
        if !self.pregrant_pending
            && self
                .rtt
                .get()
                .map(|rtt| self.last_window_update.elapsed() < rtt * 2)
                .unwrap_or(false)
        {
            let mut accumulated_max_stream_windows = self.accumulated_max_stream_windows.lock();

            // Ideally one can just double it:
            let new_max = self.max_receive_window.saturating_mul(2);

            // But one has to consider the configured connection limit:
            let new_max = {
                // 饱和运算:预算减法在 accumulated 已吃满时归 0(修前为
                // usize 下溢,debug panic / release 回绕成超大窗口),
                // 再叠回当前 max 即得本流可达上限。
                let room =
                    connection_budget(&self.config).saturating_sub(*accumulated_max_stream_windows);
                let connection_limit = (self.max_receive_window as usize).saturating_add(room);

                cmp::min(new_max, connection_limit.try_into().unwrap_or(u32::MAX))
            };

            // Account for the additional credit on the accumulated connection counter.
            *accumulated_max_stream_windows += (new_max - self.max_receive_window) as usize;
            drop(accumulated_max_stream_windows);

            log::debug!(
                "old window_max: {} mb, new window_max: {} mb",
                self.max_receive_window as f64 / crate::MIB as f64,
                new_max as f64 / crate::MIB as f64
            );

            self.max_receive_window = new_max;

            // Recalculate `next_window_update` with the new `max_receive_window`.
            let bytes_received = self.max_receive_window - self.receive_window;
            next_window_update =
                bytes_received.saturating_sub(buffer_len.try_into().unwrap_or(u32::MAX));
        }

        self.last_window_update = Instant::now();
        // 预授信差额在建流时已授出(round-4/B1;排队失败的兜底路径则随
        // 本窗口更新授出):自此的窗口消耗才是实测接收吞吐,下一个满足
        // 时延条件的更新可进入 BDP 自增窗。
        self.pregrant_pending = false;
        self.receive_window += next_window_update;

        self.assert_invariants(buffer_len);

        Some(next_window_update)
    }

    /// 建流即授信(round-4/B1,对齐 frp OpenStream/AcceptStream 时机):
    /// 把预授差额(max_receive_window − DEFAULT_CREDIT)在流创建时一次性授出,
    /// 不再等读路径首个窗口更新 poll。返回授出额度(0 = 无预授或已授出)。
    /// 记账与读路径 next_window_update 同构:consume pregrant_pending、
    /// receive_window += delta、刷新 last_window_update;不动
    /// accumulated_max_stream_windows(max 未变,Drop 守恒不受影响)。
    pub(crate) fn take_creation_grant(&mut self) -> u32 {
        let delta = self.max_receive_window - DEFAULT_CREDIT;
        if !self.pregrant_pending || delta == 0 {
            // 无预授(预算钳制的流在此自然回退)或已授出:返回 0;
            // 差额为 0 时也把 pregrant_pending 置 false(读路径无需再守)。
            self.pregrant_pending = false;
            return 0;
        }
        self.pregrant_pending = false;
        self.receive_window += delta;
        self.last_window_update = Instant::now();
        delta
    }

    /// [`FlowController::take_creation_grant`] 的回滚入口(建流授信帧排队
    /// 失败的兜底专用):复原 take 时刻的记账,使差额重新交由读路径
    /// [`FlowController::next_window_update`] 授出。仅在流构造路径调用
    /// (`Shared` 尚未共享,receive_window 减法无并发、不会下溢)。
    pub(crate) fn restore_creation_grant(&mut self, delta: u32) {
        if delta == 0 {
            return;
        }
        self.receive_window -= delta;
        self.pregrant_pending = true;
    }

    fn assert_invariants(&self, buffer_len: usize) {
        if !cfg!(debug_assertions) {
            return;
        }

        let config = &self.config;
        let rtt = self.rtt.get();
        let accumulated_max_stream_windows = *self.accumulated_max_stream_windows.lock();

        assert!(
            buffer_len <= self.max_receive_window as usize,
            "The current buffer size never exceeds the maximum stream receive window."
        );
        assert!(
            self.receive_window <= self.max_receive_window,
            "The current window never exceeds the maximum."
        );
        assert!(
            (self.max_receive_window - DEFAULT_CREDIT) as usize <= connection_budget(config),
            "The maximum never exceeds its maximum portion of the configured connection limit."
        );
        assert!(
            (self.max_receive_window - DEFAULT_CREDIT) as usize
                <= accumulated_max_stream_windows,
            "The amount by which the stream maximum exceeds DEFAULT_CREDIT is tracked in accumulated_max_stream_windows."
        );
        if rtt.is_none() {
            // 自动增窗仍要求 RTT 测量;配置初窗本身是合法起步值,但预授信
            // 受连接预算钳制:无 RTT 时 max ≤ min(配置初窗, 保底+剩余预算)
            //(round-2 预授信:见 Config::set_initial_stream_receive_window)。
            assert!(
                self.max_receive_window
                    <= (self.config.initial_stream_receive_window as u32).max(DEFAULT_CREDIT),
                "The maximum is only increased beyond the configured initial window iff an rtt \
                 measurement is available."
            );
        }
    }

    pub(crate) fn send_window(&self) -> u32 {
        self.send_window
    }

    pub(crate) fn consume_send_window(&mut self, i: u32) -> Result<(), ConnectionError> {
        self.send_window = self
            .send_window
            .checked_sub(i)
            .ok_or(ConnectionError::InvalidWindowUpdate)?;
        Ok(())
    }

    pub(crate) fn increase_send_window_by(&mut self, i: u32) -> Result<(), ConnectionError> {
        self.send_window = self
            .send_window
            .checked_add(i)
            .ok_or(ConnectionError::InvalidWindowUpdate)?;
        Ok(())
    }

    pub(crate) fn consume_receive_window(&mut self, i: u32) -> Result<(), ConnectionError> {
        self.receive_window = self
            .receive_window
            .checked_sub(i)
            .ok_or(ConnectionError::InvalidWindowUpdate)?;
        Ok(())
    }
}

impl Drop for FlowController {
    fn drop(&mut self) {
        let mut accumulated_max_stream_windows = self.accumulated_max_stream_windows.lock();

        debug_assert!(
            *accumulated_max_stream_windows >= (self.max_receive_window - DEFAULT_CREDIT) as usize,
            "{accumulated_max_stream_windows} {}",
            self.max_receive_window
        );

        *accumulated_max_stream_windows -= (self.max_receive_window - DEFAULT_CREDIT) as usize;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use quickcheck::QuickCheck;
    use web_time::Duration;

    // quickcheck 1 的 Gen 不暴露底层 rng(上游用 quickcheck-ext 提供此扩展),
    // 这里以 u64::arbitrary 取模近似,仅用于测试模糊生成。
    fn gr_usize(g: &mut quickcheck::Gen, lo: usize, hi: usize) -> usize {
        if hi <= lo {
            return lo;
        }
        use quickcheck::Arbitrary;
        (u64::arbitrary(g) % ((hi - lo) as u64)) as usize + lo
    }

    fn gr_u32(g: &mut quickcheck::Gen, lo: u32, hi: u32) -> u32 {
        if hi <= lo {
            return lo;
        }
        use quickcheck::Arbitrary;
        (u32::arbitrary(g) % (hi - lo)) + lo
    }

    fn gr_u64(g: &mut quickcheck::Gen, lo: u64, hi: u64) -> u64 {
        if hi <= lo {
            return lo;
        }
        use quickcheck::Arbitrary;
        u64::arbitrary(g) % (hi - lo) + lo
    }

    #[derive(Debug)]
    struct Input {
        controller: FlowController,
        buffer_len: usize,
    }

    #[cfg(test)]
    impl Clone for Input {
        fn clone(&self) -> Self {
            Self {
                controller: FlowController {
                    config: self.controller.config.clone(),
                    accumulated_max_stream_windows: Arc::new(Mutex::new(
                        *self.controller.accumulated_max_stream_windows.lock(),
                    )),
                    rtt: self.controller.rtt.clone(),
                    last_window_update: self.controller.last_window_update,
                    receive_window: self.controller.receive_window,
                    max_receive_window: self.controller.max_receive_window,
                    send_window: self.controller.send_window,
                    pregrant_pending: self.controller.pregrant_pending,
                },
                buffer_len: self.buffer_len,
            }
        }
    }

    impl quickcheck::Arbitrary for Input {
        fn arbitrary(g: &mut quickcheck::Gen) -> Self {
            let config = Arc::new(Config::arbitrary(g));
            let rtt = Rtt::arbitrary(g);

            let max_connection_minus_default =
                config.max_connection_receive_window.unwrap_or(usize::MAX)
                    - (config.max_num_streams * (DEFAULT_CREDIT as usize));

            let max_receive_window = if rtt.get().is_none() {
                DEFAULT_CREDIT
            } else {
                gr_u32(
                    g,
                    DEFAULT_CREDIT,
                    (DEFAULT_CREDIT as usize)
                        .saturating_add(max_connection_minus_default)
                        .try_into()
                        .unwrap_or(u32::MAX)
                        .saturating_add(1),
                )
            };
            let receive_window = gr_u32(g, 0, max_receive_window);
            let buffer_len = gr_usize(g, 0, max_receive_window as usize);
            let accumulated_max_stream_windows = Arc::new(Mutex::new(gr_usize(
                g,
                (max_receive_window - DEFAULT_CREDIT) as usize,
                max_connection_minus_default.saturating_add(1),
            )));
            let last_window_update =
                Instant::now() - Duration::from_secs(gr_u64(g, 0, 60 * 60 * 24));
            let send_window = gr_u32(g, 0, u32::MAX);

            Self {
                controller: FlowController {
                    accumulated_max_stream_windows,
                    rtt,
                    last_window_update,
                    config,
                    receive_window,
                    max_receive_window,
                    send_window,
                    // 直接构造(非 new)的控制器视作预授信已授出:
                    // quickcheck 只测 next_window_update 的纯函数性质。
                    pregrant_pending: false,
                },
                buffer_len,
            }
        }
    }

    #[test]
    fn next_window_update() {
        fn property(
            Input {
                mut controller,
                buffer_len,
            }: Input,
        ) {
            controller.next_window_update(buffer_len);
        }

        QuickCheck::new().quickcheck(property as fn(_))
    }

    // ---- round-2 预授信回归(预算钳制 / 溢出 / 守恒 / 首 WU 不倍增)----

    use crate::connection::rtt::Rtt;
    use web_time::Duration as WDuration;

    fn make_controller(
        config: Arc<Config>,
        accumulated: Arc<Mutex<usize>>,
        rtt: Rtt,
    ) -> FlowController {
        FlowController::new(DEFAULT_CREDIT, DEFAULT_CREDIT, accumulated, rtt, config)
    }

    // 预算钳制:N 条流瓜分"超出保底"预算,后到的流退回保底;
    // 预算耗尽不 panic(next_window_update 的连接限额减法不下溢)。
    #[test]
    fn pregrant_respects_connection_budget() {
        let mut config = Config::default();
        config.set_max_num_streams(8);
        // 预算 = 窗口 − 8×256KiB = 512KiB
        config.set_max_connection_receive_window(Some(8 * DEFAULT_CREDIT as usize + 512 * 1024));
        config.set_initial_stream_receive_window(2 * 1024 * 1024);
        let config = Arc::new(config);
        let accumulated = Arc::new(Mutex::new(0usize));
        let rtt = Rtt::new(); // 无 RTT 测量

        // 流 1:剩余预算 512KiB,预授信被钳到 256KiB+512KiB
        let s1 = make_controller(config.clone(), accumulated.clone(), rtt.clone());
        assert_eq!(s1.max_receive_window, DEFAULT_CREDIT + 512 * 1024);
        assert_eq!(*accumulated.lock(), 512 * 1024);

        // 流 2/3:预算耗尽,退回规约保底,零额外信用
        let s2 = make_controller(config.clone(), accumulated.clone(), rtt.clone());
        let s3 = make_controller(config.clone(), accumulated.clone(), rtt.clone());
        assert_eq!(s2.max_receive_window, DEFAULT_CREDIT);
        assert_eq!(s3.max_receive_window, DEFAULT_CREDIT);
        assert_eq!(*accumulated.lock(), 512 * 1024);

        // 预算耗尽状态下驱动窗口更新(模拟数据到达):不得 panic
        // (修前:连接限额减法 usize 下溢,debug 直接 panic)。
        let mut s2 = s2;
        s2.consume_receive_window(64 * 1024).unwrap();
        let _ = s2.next_window_update(0);

        drop(s1); // 守恒:释放的额度回到池子
        assert_eq!(*accumulated.lock(), 0);
        let s4 = make_controller(config, accumulated.clone(), rtt);
        assert_eq!(s4.max_receive_window, DEFAULT_CREDIT + 512 * 1024);
        drop(s2);
        drop(s3);
        drop(s4);
        assert_eq!(*accumulated.lock(), 0, "全部流释放后预算必须归零(守恒)");
    }

    // u32/usize 溢出组合:max_num_streams 巨大(保底乘积封顶 usize::MAX)
    // + 不限总额(None):预算饱和归 0,预授信退保底,全程不 panic。
    #[test]
    fn pregrant_overflow_combinations_saturate() {
        let mut config = Config::default();
        // 先解除总额(None 对默认 512 流断言成立),再抬流数到极限;
        // 反序:有限 1GiB 默认窗 × usize::MAX 流保底,断言正确拒绝。
        config.set_max_connection_receive_window(None);
        config.set_max_num_streams(usize::MAX);
        config.set_initial_stream_receive_window(u32::MAX as usize); // 最大合法初窗
        let config = Arc::new(config);
        let accumulated = Arc::new(Mutex::new(0usize));
        let rtt = Rtt::new();

        let s = make_controller(config.clone(), accumulated.clone(), rtt.clone());
        // 256KiB × usize::MAX 饱和 = usize::MAX → 预算 = MAX − MAX = 0
        assert_eq!(s.max_receive_window, DEFAULT_CREDIT);
        assert_eq!(*accumulated.lock(), 0);

        // 自增窗路径同样不得下溢/回绕(rtt 未测得 → 不进增窗;再以
        // 已测得 RTT 的控制器验证增窗在零预算下收敛到当前 max)。
        let mut s2 = make_controller(config, accumulated, rtt_with_measurement());
        s2.consume_receive_window(DEFAULT_CREDIT).unwrap();
        let update = s2.next_window_update(0).expect("整窗耗尽必有更新");
        assert_eq!(update, DEFAULT_CREDIT, "零预算下更新只回授已耗额度");
        assert_eq!(s2.max_receive_window, DEFAULT_CREDIT, "零预算下不得增窗");
    }

    // 首 WU 不触发 BDP 倍增:预授信差额不是实测接收吞吐。
    // 修前:last_window_update 建流置 now,首个更新在 rtt 已测得时
    // elapsed≈0 < 2×rtt 恒真 → 首个 WU 即把窗口翻倍(假速率)。
    fn rtt_with_measurement() -> Rtt {
        let mut rtt = Rtt::new();
        let ping = rtt.next_ping().expect("初始 ping 立即可发");
        std::thread::sleep(WDuration::from_millis(30));
        let action = rtt.handle_pong(ping.id());
        assert!(matches!(action, crate::connection::Action::None));
        assert!(rtt.get().is_some());
        rtt
    }

    #[test]
    fn first_pregrant_window_update_does_not_double() {
        let mut config = Config::default();
        config.set_initial_stream_receive_window(2 * 1024 * 1024);
        // 预算充裕(默认 1GiB − 512×256KiB = 896MiB)
        let config = Arc::new(config);
        let accumulated = Arc::new(Mutex::new(0usize));

        let mut s = make_controller(config, accumulated, rtt_with_measurement());
        assert_eq!(s.max_receive_window, 2 * 1024 * 1024);

        // 对端写满 64KiB(消耗授信)→ 首个窗口更新授出预授信差额
        s.consume_receive_window(64 * 1024).unwrap();
        let update = s.next_window_update(0).expect("半窗阈值已过,必有更新");
        assert_eq!(
            update,
            2 * 1024 * 1024 - DEFAULT_CREDIT + 64 * 1024,
            "首 WU 授出全额预授信差额+已耗额度"
        );
        assert_eq!(
            s.max_receive_window,
            2 * 1024 * 1024,
            "首 WU 不得把 max 翻倍(预授信≠实测吞吐;修前此处变 4MiB)"
        );
        assert!(!s.pregrant_pending, "预授信差额已授出");

        // 之后真实消耗驱动的更新仍可正常自增窗(机制未被关死):
        // consume 到剩余窗口 < max/2 再触发一次,若时延条件满足即翻倍。
        s.consume_receive_window(2 * 1024 * 1024 - DEFAULT_CREDIT - 64 * 1024)
            .unwrap(); // receive 已授额度全部耗尽
        let _ = s.next_window_update(0);
        assert!(s.max_receive_window >= 2 * 1024 * 1024);
    }
}
