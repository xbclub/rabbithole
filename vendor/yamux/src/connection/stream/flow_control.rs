use std::sync::Arc;

use crate::{Config, ConnectionError, DEFAULT_CREDIT};

/// 恒定单流接收窗流控(round-E1,对齐 frp v0.2.0):
/// - MaxStreamWindowSize 恒定(fatedier/yamux stream.go L284
///   `max := config.MaxStreamWindowSize`),窗口永不自增;
/// - 无连接级共享预算、无 BDP/auto-tune(全源码无此逻辑)。
///
/// `max_receive_window` 恒等于配置初窗
/// ([`Config::initial_stream_receive_window`],下限 [`DEFAULT_CREDIT`]),
/// 不随流数收缩、不随吞吐自增;(初窗 − DEFAULT_CREDIT)差额由建流授信
/// ([`FlowController::take_creation_grant`],round-4/B1,对齐 frp
/// OpenStream/AcceptStream 的 sendWindowUpdate 时机)一次性授出;读路径
/// [`FlowController::next_window_update`] 只在半窗阈值过后回补已耗额度,
/// 不增窗。
///
/// 内存护栏:聚合在途敞口 = 并发流数 × max_receive_window,唯一护栏是
/// [`Config::max_num_streams`](rh-server 的 limits.mux_streams;0=不限时
/// 敞口无界,与 frp 行为一致,生产建议显式配置流上限)。
#[derive(Debug)]
pub(crate) struct FlowController {
    config: Arc<Config>,
    receive_window: u32,
    max_receive_window: u32,
    send_window: u32,
    /// 建流授信(round-2 预授信 / round-4/B1):建流抬高的
    /// (max − DEFAULT_CREDIT) 差额尚未经窗口更新帧授出前为 `true`。
    /// round-4/B1 起该差额在流创建时即由 [`FlowController::take_creation_grant`]
    /// 授出(排队失败的兜底会回滚本标记,差额回落由读路径授出)。
    /// round-E1 起无 BDP 自增,本标记只服务 take/restore 的幂等与回滚。
    pregrant_pending: bool,
}

impl FlowController {
    pub(crate) fn new(receive_window: u32, send_window: u32, config: Arc<Config>) -> Self {
        // 恒定窗(round-E1):max_receive_window = max(配置初窗, DEFAULT_CREDIT),
        // 不再随连接预算收缩——连接级共享预算已删除(对齐 frp,无此概念)。
        // receive_window 仍按规约从 DEFAULT_CREDIT 开始——(初窗 − 256KiB)
        // 的差额自 round-4/B1 起由 take_creation_grant 在建流时以带初始
        // flag 的 WU 帧授给对端(排队失败的兜底回落读路径
        // next_window_update:max − receive_window ≥ max/2 恒成立,半窗
        // 阈值同样放行),对端任何版本都只见到标准 WU。
        let max_receive_window = (config.initial_stream_receive_window as u32).max(DEFAULT_CREDIT);
        Self {
            receive_window,
            send_window,
            config,
            max_receive_window,
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
        let next_window_update =
            bytes_received.saturating_sub(buffer_len.try_into().unwrap_or(u32::MAX));

        // Don't send an update in case half or more of the window is still available to the sender.
        if next_window_update < self.max_receive_window / 2 {
            return None;
        }

        // round-E1:恒定窗——对齐 frp v0.2.0(窗口永不自增),此处只回补
        // 已耗额度;BDP 自增与连接预算钳制均已删除。
        // 预授信差额在建流时已授出(round-4/B1;排队失败的兜底路径则随
        // 本窗口更新授出)——标记消费与授出配套。
        self.pregrant_pending = false;
        self.receive_window += next_window_update;

        self.assert_invariants(buffer_len);

        Some(next_window_update)
    }

    /// 建流即授信(round-4/B1,对齐 frp OpenStream/AcceptStream 时机):
    /// 把预授差额(max_receive_window − DEFAULT_CREDIT)在流创建时一次性授出,
    /// 不再等读路径首个窗口更新 poll。返回授出额度(0 = 无预授或已授出)。
    /// 记账与读路径 next_window_update 同构:consume pregrant_pending、
    /// receive_window += delta;不动 max(恒定窗,无 Drop 守恒记账)。
    pub(crate) fn take_creation_grant(&mut self) -> u32 {
        let delta = self.max_receive_window - DEFAULT_CREDIT;
        if !self.pregrant_pending || delta == 0 {
            // 无预授或已授出:返回 0;差额为 0 时也把 pregrant_pending
            // 置 false(读路径无需再守)。
            self.pregrant_pending = false;
            return 0;
        }
        self.pregrant_pending = false;
        self.receive_window += delta;
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

        assert!(
            buffer_len <= self.max_receive_window as usize,
            "The current buffer size never exceeds the maximum stream receive window."
        );
        assert!(
            self.receive_window <= self.max_receive_window,
            "The current window never exceeds the maximum."
        );
        // round-E1:恒定窗——max 恒等于配置初窗(不再随预算收缩/自增变动;
        // 修前此处为"无 RTT 时 ≤ 初窗"的弱形式 + 两条预算断言)。
        assert_eq!(
            self.max_receive_window,
            (self.config.initial_stream_receive_window as u32).max(DEFAULT_CREDIT),
            "The maximum receive window is constant at the configured initial window."
        );
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

#[cfg(test)]
mod tests {
    use super::*;
    use quickcheck::QuickCheck;

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

            // round-E1:生成域与新不变量一致——max == 配置初窗
            //(Config::arbitrary 恒取 upstream 默认初窗,即 DEFAULT_CREDIT),
            // 窗口不自增;max_connection_receive_window 派生约束已随
            // 连接预算删除。
            let max_receive_window =
                (config.initial_stream_receive_window as u32).max(DEFAULT_CREDIT);
            let receive_window = gr_u32(g, 0, max_receive_window);
            let buffer_len = gr_usize(g, 0, max_receive_window as usize);
            let send_window = gr_u32(g, 0, u32::MAX);

            Self {
                controller: FlowController {
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

    // ---- round-E1 恒定窗回归 ----

    fn make_controller(config: Arc<Config>) -> FlowController {
        FlowController::new(DEFAULT_CREDIT, DEFAULT_CREDIT, config)
    }

    // 恒定窗:连续多轮"整窗耗尽 → 回补",max 恒等于配置初窗。
    //(重写自 first_pregrant_window_update_does_not_double:BDP 自增已删,
    // 窗口在任意轮次后都不得越过配置初窗;修前时延条件满足时每轮翻倍。)
    #[test]
    fn window_never_grows_beyond_config_initial() {
        let mut config = Config::default();
        config.set_initial_stream_receive_window(2 * 1024 * 1024);
        let config = Arc::new(config);

        let mut s = make_controller(config);
        assert_eq!(s.max_receive_window, 2 * 1024 * 1024);

        // 首个窗口更新(兜底路径形态):授出建流预授差额,max 不动
        let update = s.next_window_update(0).expect("半窗阈值已过,必有更新");
        assert_eq!(
            update,
            2 * 1024 * 1024 - DEFAULT_CREDIT,
            "首 WU 授出全额预授信差额"
        );
        assert_eq!(
            s.max_receive_window,
            2 * 1024 * 1024,
            "窗口不得自增(修前:时延条件满足时首 WU 即翻倍)"
        );
        assert!(!s.pregrant_pending, "预授信差额已授出");

        // 连续多轮:整窗耗尽 → 回补,max 恒等于配置初窗
        for round in 0..8 {
            s.consume_receive_window(s.max_receive_window).unwrap();
            let update = s.next_window_update(0).expect("整窗耗尽必有更新");
            assert_eq!(
                update,
                2 * 1024 * 1024,
                "回补额度 == 已耗额度(round {round})"
            );
            assert_eq!(
                s.max_receive_window,
                2 * 1024 * 1024,
                "窗口不得自增(修前:时延条件满足时每轮翻倍;round {round})"
            );
        }
    }

    // round-E1(红→绿):同一连接的每条流都拿到完整配置初窗,与流数无关。
    // 旧语义(256MiB 共享预算):预算 = 总窗 − 256KiB×流数 = 256MiB,
    // 每流预授差额 6MiB−256KiB = 5.75MiB,44 条流吃满后第 45 条被钳到
    // 256KiB+3MiB、第 46 条起冻结在 256KiB 规约保底(红证:修前产品代码
    // 上运行本测试,第 45 条流断言 FAILED,left=3407872 right=6291456)。
    // 控制器须全部同时存活(旧 Drop 会归还预算,逐个构造-析构测不出耗尽)。
    // 下方 set_max_connection_receive_window 镜像 mux.rs 旧接线,锁
    // "配置了有限连接总窗也不得钳流" 的回归(新语义下该字段无消费者)。
    #[test]
    fn every_stream_gets_full_window_regardless_of_count() {
        let mut config = Config::default();
        config.set_max_connection_receive_window(Some(
            4096 * DEFAULT_CREDIT as usize + 256 * 1024 * 1024,
        ));
        config.set_max_num_streams(4096);
        config.set_initial_stream_receive_window(6 * 1024 * 1024);
        let config = Arc::new(config);

        let mut streams = Vec::with_capacity(4096);
        for _ in 0..4096 {
            streams.push(make_controller(config.clone()));
        }
        for (i, s) in streams.iter().enumerate() {
            assert_eq!(
                s.max_receive_window,
                6 * 1024 * 1024,
                "第 {} 条流必须拿到完整 6MiB 初窗(旧语义:第 46 条起冻结 256KiB)",
                i + 1
            );
        }
    }
}
