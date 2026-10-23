//! 事件审计:落库(供 API 查询)。
//!
//! ★事件是**旁路**:它不该让业务流程等待,也不该让业务流程失败。
//!
//! 修前 `emit`/`emit_detail` 直接 `await db.insert_event(...)` —— 每次业务
//! 操作都同步等一次 SQLite INSERT。调用点遍布 api.rs(创建/更新/删除/
//! 启停/登录)、runtime.rs、control.rs(上下线)、nathole.rs、recovery.rs,
//! 共 30 处。在 `batch_services` 里让每条规则多付一次 INSERT;在
//! `recovery` 里同样。而本文件自己的注释就写着「事件是旁路,不该让业务
//! 流程失败」—— 既然是旁路,就不该在业务路径同步等待。
//!
//! 改为:调用方 `try_send` 进 channel 后立即返回,单个 writer 任务批量
//! 落库。`emit` 保持 `async fn` 签名不变(30 个调用点无需改动),但内部
//! **没有 await 点** —— 它不会挂起调用方。
//!
//! 为什么用 `unbounded` 而不是 `bounded`:事件量级很小(心跳级 + 规则
//! 变更),而 `bounded` 满了只有两个选择 —— 丢事件(控制台静默缺条目)
//! 或阻塞业务(退回到修前的问题)。两者都不可接受。作为兜底,`emit`
//! 在 channel 关闭(进程收尾)时降级为**直接写库**,保证退出路径上的
//! 事件不丢。

use crate::db::Db;
use tokio::sync::mpsc;

/// 单条待落库事件。
#[derive(Debug)]
struct PendingEvent {
    level: String,
    source: String,
    message: String,
    detail: String,
    /// 仅 flush 栅栏事件使用:writer 处理到它时回执。
    ack: Option<tokio::sync::oneshot::Sender<()>>,
}

/// 单批最多取多少条。批量写要平衡"吞吐"与"事件可见延迟":
/// 上限太小则批量无意义,太大则控制台要等更久才看到最新事件。
const EVENT_BATCH_MAX: usize = 64;

/// 一次批量落库。
///
/// 用一个事务包住整批:SQLite 的单次 INSERT 各自是一次隐式事务(每次都要
/// fsync),批量提交把 N 次 fsync 压成 1 次。这是"批量写"真正的收益来源,
/// 而不只是少几次 SQL 解析。
///
/// 整批失败时降级为逐条重试:一条坏数据(超长、编码问题)不该让同批其它
/// 事件一起丢掉。
async fn write_batch(db: &Db, batch: &[PendingEvent]) {
    if batch.is_empty() {
        return;
    }
    let rows: Vec<(&str, &str, &str, &str)> = batch
        .iter()
        .map(|e| {
            (
                e.level.as_str(),
                e.source.as_str(),
                e.message.as_str(),
                e.detail.as_str(),
            )
        })
        .collect();
    if let Err(err) = db.insert_events(&rows).await {
        tracing::warn!(
            error = %err,
            count = batch.len(),
            "event batch insert failed; falling back to per-row insert"
        );
        for e in batch {
            // 失败仍逐条留痕(不阻断):控制台「事件」页依赖这张表,
            // 写失败时页面会静默缺条目,至少要能在日志里看到。
            if let Err(err) = db
                .insert_event(&e.level, &e.source, &e.message, &e.detail)
                .await
            {
                tracing::warn!(
                    error = %err,
                    level = %e.level,
                    source = %e.source,
                    "event persisted to DB failed (console event list will miss this entry)"
                );
            }
        }
    }
}

#[derive(Clone)]
pub struct EventBus {
    db: Db,
    /// 待落库队列。`None` = 已进入收尾阶段(或未启动 writer),
    /// 此时 `emit` 退化为直接写库。
    tx: std::sync::Arc<std::sync::Mutex<Option<mpsc::UnboundedSender<PendingEvent>>>>,
    /// writer 任务的句柄,供 [`Self::shutdown`] 等待收尾。
    writer: std::sync::Arc<std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>>,
}

impl EventBus {
    /// 创建事件总线并启动 writer 任务。
    ///
    /// 若当前**不在** tokio 运行时内(理论上不该发生:`AppState::new` 在生产
    /// 与全部测试里都在运行时内),退化为"无 writer"模式 —— `emit` 走直接
    /// 写库的降级路径,功能仍然正确,只是退回修前的同步语义。这样
    /// `AppState::new` 不会因为调用位置意外而 panic。
    pub fn new(db: Db) -> Self {
        let (tx, rx) = mpsc::unbounded_channel::<PendingEvent>();
        let (tx_slot, writer) = match tokio::runtime::Handle::try_current() {
            Ok(handle) => {
                let h = handle.spawn(writer_loop(db.clone(), rx));
                (Some(tx), Some(h))
            }
            Err(_) => {
                // 没有运行时:丢弃 receiver,emit 全部走降级直写
                drop(rx);
                (None, None)
            }
        };
        Self {
            db,
            tx: std::sync::Arc::new(std::sync::Mutex::new(tx_slot)),
            writer: std::sync::Arc::new(std::sync::Mutex::new(writer)),
        }
    }

    pub async fn emit(&self, level: &str, source: &str, message: &str) {
        self.emit_detail(level, source, message, "").await;
    }

    /// 投递一条事件。
    ///
    /// 保持 `async fn` 是为了不惊动 30 个调用点,但**函数体里没有 await**:
    /// 它只是把事件塞进无界队列就返回。真正的落库在 writer 任务里。
    pub async fn emit_detail(&self, level: &str, source: &str, message: &str, detail: &str) {
        tracing::info!(level, source, message, "event");
        let ev = PendingEvent {
            level: level.to_string(),
            source: source.to_string(),
            message: message.to_string(),
            detail: detail.to_string(),
            ack: None,
        };
        // 先试着走队列
        let sent = {
            let g = self.tx.lock().unwrap_or_else(|e| e.into_inner());
            match g.as_ref() {
                Some(tx) => tx.send(ev).is_ok(),
                None => false,
            }
        };
        if sent {
            return;
        }
        // 队列不可用(收尾中/已关闭):降级为直接写库,保证退出路径不丢事件。
        if let Err(e) = self.db.insert_event(level, source, message, detail).await {
            tracing::warn!(
                error = %e,
                level,
                source,
                "event persisted to DB failed (console event list will miss this entry)"
            );
        }
    }

    /// 把队列里剩余事件全部落库后返回。
    ///
    /// 供进程收尾与测试使用。实现是往队列里放一个**带回执的栅栏事件**:
    /// 无界 channel 保证 FIFO,writer 处理到栅栏时,它之前的每一条都已被
    /// `write_batch` 写完,于是回执即"全部落库"。
    ///
    /// writer 已退出(见 [`Self::shutdown`])时直接返回 —— 此时队列里的
    /// 东西已在 shutdown 里处理完。
    pub async fn flush(&self) {
        let (ack_tx, ack_rx) = tokio::sync::oneshot::channel::<()>();
        let sent = {
            let g = self.tx.lock().unwrap_or_else(|e| e.into_inner());
            match g.as_ref() {
                Some(tx) => tx
                    .send(PendingEvent {
                        level: String::new(),
                        source: FLUSH_SENTINEL.to_string(),
                        message: String::new(),
                        detail: String::new(),
                        ack: Some(ack_tx),
                    })
                    .is_ok(),
                None => false,
            }
        };
        if sent {
            // 有上限:writer 卡在 DB 上时不要让调用方无限等
            let _ = tokio::time::timeout(std::time::Duration::from_secs(5), ack_rx).await;
        }
    }

    /// 收尾:关闭队列、等 writer 把剩余事件写完。
    ///
    /// 进程退出时调用,避免丢最后几条。可重复调用(幂等)。
    pub async fn shutdown(&self) {
        // 先丢弃 sender,writer 的 `recv()` 会返回 None 并处理完积压后退出
        {
            let mut g = self.tx.lock().unwrap_or_else(|e| e.into_inner());
            *g = None;
        }
        let handle = {
            let mut g = self.writer.lock().unwrap_or_else(|e| e.into_inner());
            g.take()
        };
        if let Some(h) = handle {
            // 给 writer 一个上限:它最多再写 EVENT_BATCH_MAX 条
            let _ = tokio::time::timeout(std::time::Duration::from_secs(5), h).await;
        }
    }
}

/// flush 哨兵事件的 `source` 标记。writer 见到它不落库,只当作
/// "此前消息都已处理完"的栅栏。
const FLUSH_SENTINEL: &str = "\u{0}flush";

/// writer 主循环:批量取、批量写。
async fn writer_loop(db: Db, mut rx: mpsc::UnboundedReceiver<PendingEvent>) {
    loop {
        // 阻塞等第一条;channel 关闭则收尾退出
        let Some(first) = rx.recv().await else {
            break;
        };
        let mut batch = Vec::with_capacity(EVENT_BATCH_MAX);
        let mut acks: Vec<tokio::sync::oneshot::Sender<()>> = Vec::new();
        if first.source == FLUSH_SENTINEL {
            if let Some(a) = first.ack {
                acks.push(a);
            }
        } else {
            batch.push(first);
        }
        // 把队列里**当前已就绪**的全部取走,凑一批
        while batch.len() < EVENT_BATCH_MAX {
            match rx.try_recv() {
                Ok(ev) => {
                    if ev.source == FLUSH_SENTINEL {
                        if let Some(a) = ev.ack {
                            acks.push(a);
                        }
                    } else {
                        batch.push(ev);
                    }
                }
                Err(_) => break,
            }
        }
        if !batch.is_empty() {
            write_batch(&db, &batch).await;
        }
        // 栅栏回执必须在**写完这一批之后**发:调用方收到回执时,
        // 它之前投递的事件才真的落库了。
        for a in acks {
            let _ = a.send(());
        }
    }
    // channel 关闭:残留已被 recv 循环取空,无需额外处理
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn bus() -> (EventBus, Db) {
        let db = Db::open("sqlite::memory:").await.unwrap();
        (EventBus::new(db.clone()), db)
    }

    /// emit 之后 flush,事件必须已落库。
    #[tokio::test]
    async fn emit_then_flush_persists() {
        let (bus, db) = bus().await;
        bus.emit("info", "test", "hello").await;
        bus.flush().await;
        // flush 之后给 writer 一点点时间把批次写完
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let rows = db.list_events(10, None).await.unwrap();
        assert_eq!(rows.len(), 1, "事件必须已落库");
        assert_eq!(rows[0].message, "hello");
    }

    /// 多条事件全部落库,不丢。
    #[tokio::test]
    async fn many_events_all_persisted() {
        let (bus, db) = bus().await;
        for i in 0..200 {
            bus.emit("info", "test", &format!("m{i}")).await;
        }
        bus.shutdown().await;
        let rows = db.list_events(1000, None).await.unwrap();
        assert_eq!(
            rows.len(),
            200,
            "200 条事件必须全部落库,实际 {}",
            rows.len()
        );
    }

    /// shutdown 之后 emit 仍能落库(降级为直接写)。
    #[tokio::test]
    async fn emit_after_shutdown_still_persists() {
        let (bus, db) = bus().await;
        bus.shutdown().await;
        bus.emit("warn", "test", "late").await;
        let rows = db.list_events(10, None).await.unwrap();
        assert!(
            rows.iter().any(|r| r.message == "late"),
            "收尾后的事件必须仍能落库(降级路径)"
        );
    }

    /// shutdown 是幂等的。
    #[tokio::test]
    async fn shutdown_is_idempotent() {
        let (bus, _db) = bus().await;
        bus.shutdown().await;
        bus.shutdown().await;
    }

    /// emit 不得阻塞调用方:大量投递应在很短时间内完成(不逐条等 DB)。
    ///
    /// 断言的是**外部可观测的耗时**。修前每条 emit 都要一次 SQLite
    /// INSERT(每次隐式事务),这里会慢一个数量级。
    #[tokio::test]
    async fn emit_does_not_block_on_db() {
        let (bus, db) = bus().await;
        let n = 500;
        let t0 = std::time::Instant::now();
        for i in 0..n {
            bus.emit("info", "test", &format!("m{i}")).await;
        }
        let emit_elapsed = t0.elapsed();
        // 投递 500 条应当远快于 500 次 SQLite 往返;给足余量避免抖动误报
        assert!(
            emit_elapsed < std::time::Duration::from_millis(200),
            "500 条 emit 耗时 {emit_elapsed:?} —— 不得在业务路径同步等 DB"
        );
        bus.shutdown().await;
        let rows = db.list_events(1000, None).await.unwrap();
        assert_eq!(rows.len(), n, "异步落库仍不得丢事件");
    }

    /// #12 基准(手动运行):N 条事件的投递耗时(业务路径)与总落库耗时。
    ///
    /// ```text
    /// cargo test -p rh-server --release events::tests::bench -- --ignored --nocapture
    /// ```
    ///
    /// 两个数字:
    ///   `emit_*`  = 调用方在业务路径上花的时间(修前含 DB 往返)
    ///   `total`   = 从投递到全部落库完成(含 writer 批处理)
    #[tokio::test]
    #[ignore = "耗时基准,手动运行(--ignored --nocapture)"]
    async fn bench_event_emit() {
        for n in [100usize, 500, 1000] {
            let (bus, db) = bus().await;
            let t0 = std::time::Instant::now();
            for i in 0..n {
                bus.emit("info", "bench", &format!("m{i}")).await;
            }
            let emit_elapsed = t0.elapsed();
            bus.flush().await;
            bus.shutdown().await;
            let total = t0.elapsed();
            let rows = db.list_events(10_000, None).await.unwrap();
            println!(
                "N={n:>5}  emit_path={emit_elapsed:>12?}  total={total:>12?}  rows={}",
                rows.len()
            );
            assert_eq!(rows.len(), n, "不得丢事件");
        }
    }

    /// detail 也要完整落库。
    #[tokio::test]
    async fn detail_persisted() {
        let (bus, db) = bus().await;
        bus.emit_detail("warn", "test", "msg", "the-detail").await;
        bus.shutdown().await;
        let rows = db.list_events(10, None).await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].detail, "the-detail");
    }
}
