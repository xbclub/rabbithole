//! 事件审计:落库(供 API 查询)。

use crate::db::Db;

#[derive(Clone)]
pub struct EventBus {
    db: Db,
}

impl EventBus {
    pub fn new(db: Db) -> Self {
        Self { db }
    }

    pub async fn emit(&self, level: &str, source: &str, message: &str) {
        self.emit_detail(level, source, message, "").await;
    }

    pub async fn emit_detail(&self, level: &str, source: &str, message: &str, detail: &str) {
        tracing::info!(level, source, message, "event");
        // 落库失败要留痕:控制台「事件」页依赖这张表,写失败时页面会
        // 静默缺条目。修前 `let _ = ` 把错误全吞了,连 warn 都没有。
        // 仍不阻断调用方(事件是旁路,不该让业务流程失败)。
        if let Err(e) = self.db.insert_event(level, source, message, detail).await {
            tracing::warn!(
                error = %e,
                level,
                source,
                "event persisted to DB failed (console event list will miss this entry)"
            );
        }
    }
}
