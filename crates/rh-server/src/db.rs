//! 数据库访问层:sqlx + SQLite。运行时查询(非宏),迁移嵌入。

use anyhow::{Context, Result};
use rh_protocol::{ProxySpec, VisitorKind, VisitorSpec};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions, SqliteRow};
use sqlx::{Row, SqlitePool};

#[derive(Clone)]
pub struct Db {
    pool: SqlitePool,
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

impl Db {
    pub async fn open(path: &str) -> Result<Self> {
        let path = path.strip_prefix("sqlite://").unwrap_or(path);
        let in_memory = path.ends_with(":memory:");
        let opts = if in_memory {
            // 内存库:解析 URL 语义(测试用)。
            // 每个池连接是独立空库,必须限制单连接,否则迁移与查询落在不同库上
            <SqliteConnectOptions as std::str::FromStr>::from_str(&format!("sqlite:{path}"))?
        } else {
            SqliteConnectOptions::new()
                .filename(path)
                .create_if_missing(true)
        }
        .foreign_keys(true)
        .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal);
        let pool = SqlitePoolOptions::new()
            .max_connections(if in_memory { 1 } else { 8 })
            .connect_with(opts)
            .await
            .with_context(|| format!("open sqlite {path}"))?;
        let db = Self { pool };
        db.migrate().await?;
        Ok(db)
    }

    /// 轻量迁移器:按序应用未执行的迁移文件(嵌入二进制)。
    async fn migrate(&self) -> Result<()> {
        const MIGRATIONS: &[(&str, &str)] = &[
            (
                "0001_init",
                include_str!("../../../migrations/0001_init.sql"),
            ),
            (
                "0002_client_quotas",
                include_str!("../../../migrations/0002_client_quotas.sql"),
            ),
            (
                "0003_certs",
                include_str!("../../../migrations/0003_certs.sql"),
            ),
        ];
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS _migrations (name TEXT PRIMARY KEY, applied_at INTEGER NOT NULL)",
        )
        .execute(&self.pool)
        .await?;
        for (name, sql_text) in MIGRATIONS {
            let done: Option<i64> = sqlx::query_scalar("SELECT 1 FROM _migrations WHERE name=?1")
                .bind(name)
                .fetch_optional(&self.pool)
                .await?
                .flatten();
            if done.is_some() {
                continue;
            }
            let mut tx = self.pool.begin().await?;
            // raw_sql 原生执行整段脚本(含注释与多语句)
            sqlx::raw_sql(*sql_text).execute(&mut *tx).await?;
            sqlx::query("INSERT INTO _migrations (name, applied_at) VALUES (?1, ?2)")
                .bind(name)
                .bind(now_ms())
                .execute(&mut *tx)
                .await?;
            tx.commit().await?;
            tracing::info!(name, "migration applied");
        }
        Ok(())
    }

    // ---------- users ----------

    pub async fn create_user(&self, username: &str, password_hash: &str) -> Result<i64> {
        let r = sqlx::query(
            "INSERT INTO users (username, password_hash, role, created_at) VALUES (?1, ?2, 'admin', ?3)",
        )
        .bind(username)
        .bind(password_hash)
        .bind(now_ms())
        .execute(&self.pool)
        .await?;
        Ok(r.last_insert_rowid())
    }

    pub async fn count_users(&self) -> Result<i64> {
        let row: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM users")
            .fetch_one(&self.pool)
            .await?;
        Ok(row.0)
    }

    pub async fn update_user_password(&self, username: &str, password_hash: &str) -> Result<bool> {
        let r = sqlx::query("UPDATE users SET password_hash = ?2 WHERE username = ?1")
            .bind(username)
            .bind(password_hash)
            .execute(&self.pool)
            .await?;
        Ok(r.rows_affected() > 0)
    }
    pub async fn get_user(&self, username: &str) -> Result<Option<(i64, String, String)>> {
        let row = sqlx::query("SELECT id, password_hash, role FROM users WHERE username = ?1")
            .bind(username)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map(|r| {
            (
                r.get::<i64, _>(0),
                r.get::<String, _>(1),
                r.get::<String, _>(2),
            )
        }))
    }

    /// 仅测试使用(初始化断言:首用户存在)。
    #[cfg(test)]
    pub async fn user_count(&self) -> Result<i64> {
        let r: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM users")
            .fetch_one(&self.pool)
            .await?;
        Ok(r)
    }

    // ---------- clients ----------

    pub async fn create_client(
        &self,
        id: &str,
        name: &str,
        secret: &str,
        remark: &str,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO clients (id, name, secret, remark, created_at) VALUES (?1, ?2, ?3, ?4, ?5)",
        )
        .bind(id)
        .bind(name)
        .bind(secret)
        .bind(remark)
        .bind(now_ms())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn get_client(&self, id: &str) -> Result<Option<ClientRow>> {
        let row = sqlx::query("SELECT id, name, secret, enabled, remark, config_revision, created_at, last_online_at, client_version, hostname, os, rate_limit, max_conns, max_services, flow_limit FROM clients WHERE id = ?1")
            .bind(id)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map(|r| ClientRow::from_row(&r)))
    }

    pub async fn list_clients(&self) -> Result<Vec<ClientRow>> {
        let rows = sqlx::query("SELECT id, name, secret, enabled, remark, config_revision, created_at, last_online_at, client_version, hostname, os, rate_limit, max_conns, max_services, flow_limit FROM clients ORDER BY created_at")
            .fetch_all(&self.pool)
            .await?;
        Ok(rows.iter().map(ClientRow::from_row).collect())
    }

    /// 测试缝:关闭连接池,使后续查询返回错误(round-10.2 §2.4 覆盖
    /// `attach_additional_carrier` 的 DB 错误 → INTERNAL 回包路径)。
    #[cfg(test)]
    pub async fn close_for_test(&self) {
        self.pool.close().await;
    }

    pub async fn update_client_meta(
        &self,
        id: &str,
        version: &str,
        hostname: &str,
        os: &str,
    ) -> Result<()> {
        sqlx::query(
            "UPDATE clients SET client_version=?2, hostname=?3, os=?4, last_online_at=?5 WHERE id=?1",
        )
        .bind(id)
        .bind(version)
        .bind(hostname)
        .bind(os)
        .bind(now_ms())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn set_client_enabled(&self, id: &str, enabled: bool) -> Result<()> {
        sqlx::query("UPDATE clients SET enabled=?2 WHERE id=?1")
            .bind(id)
            .bind(enabled as i64)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// 更新客户端名称/备注(控制台编辑)。
    pub async fn update_client_profile(
        &self,
        id: &str,
        name: Option<&str>,
        remark: Option<&str>,
    ) -> Result<()> {
        sqlx::query(
            "UPDATE clients SET name=COALESCE(?2, name), remark=COALESCE(?3, remark) WHERE id=?1",
        )
        .bind(id)
        .bind(name)
        .bind(remark)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// F2 配额更新(控制台);逐字段可空 = 不改。
    pub async fn update_client_quota(
        &self,
        id: &str,
        rate_limit: Option<&str>,
        max_conns: Option<i64>,
        max_services: Option<i64>,
        flow_limit: Option<i64>,
    ) -> Result<()> {
        sqlx::query(
            "UPDATE clients SET rate_limit=COALESCE(?2, rate_limit), max_conns=COALESCE(?3, max_conns), max_services=COALESCE(?4, max_services), flow_limit=COALESCE(?5, flow_limit) WHERE id=?1",
        )
        .bind(id)
        .bind(rate_limit)
        .bind(max_conns)
        .bind(max_services)
        .bind(flow_limit)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn rotate_client_secret(&self, id: &str, new_secret: &str) -> Result<()> {
        sqlx::query("UPDATE clients SET secret=?2 WHERE id=?1")
            .bind(id)
            .bind(new_secret)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn delete_client(&self, id: &str) -> Result<()> {
        sqlx::query("DELETE FROM clients WHERE id=?1")
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    // ---------- proxies ----------

    pub async fn insert_proxy(
        &self,
        client_id: &str,
        spec: &ProxySpec,
        port_auto: bool,
        source: &str,
    ) -> Result<i64> {
        let mut spec = spec.clone();
        let spec_json = serde_json::to_string(&spec)?;
        let r = sqlx::query(
            "INSERT INTO proxies (client_id, name, kind, enabled, bind_ip, bind_port, port_auto, target_host, target_port, pool_size, group_name, group_key, source, spec, created_at, updated_at) \
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?15)",
        )
        .bind(client_id)
        .bind(&spec.name)
        .bind(spec.kind.as_str())
        .bind(spec.enabled as i64)
        .bind(&spec.bind_ip)
        .bind(spec.bind_port as i64)
        .bind(port_auto as i64)
        .bind(&spec.target_host)
        .bind(spec.target_port as i64)
        .bind(spec.pool_size as i64)
        .bind(spec.group.clone().unwrap_or_default())
        .bind(&spec.group_key)
        .bind(source)
        .bind(&spec_json)
        .bind(now_ms())
        .execute(&self.pool)
        .await?;
        let id = r.last_insert_rowid();
        spec.id = id as u64;
        // 回填 id 保证 spec JSON 与行一致(读取方依赖 spec.id)
        let _ = sqlx::query("UPDATE proxies SET spec=?2 WHERE id=?1")
            .bind(id)
            .bind(serde_json::to_string(&spec)?)
            .execute(&self.pool)
            .await;
        Ok(id)
    }

    /// 创建规则 + revision+1(单事务;§4.4 步骤 4)。
    pub async fn insert_proxy_bump(
        &self,
        client_id: &str,
        spec: &ProxySpec,
        port_auto: bool,
        source: &str,
    ) -> Result<(i64, u64)> {
        let mut tx = self.pool.begin().await?;
        let mut spec = spec.clone();
        let spec_json = serde_json::to_string(&spec)?;
        let r = sqlx::query(
            "INSERT INTO proxies (client_id, name, kind, enabled, bind_ip, bind_port, port_auto, target_host, target_port, pool_size, group_name, group_key, source, spec, created_at, updated_at) \
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?15)",
        )
        .bind(client_id)
        .bind(&spec.name)
        .bind(spec.kind.as_str())
        .bind(spec.enabled as i64)
        .bind(&spec.bind_ip)
        .bind(spec.bind_port as i64)
        .bind(port_auto as i64)
        .bind(&spec.target_host)
        .bind(spec.target_port as i64)
        .bind(spec.pool_size as i64)
        .bind(spec.group.clone().unwrap_or_default())
        .bind(&spec.group_key)
        .bind(source)
        .bind(&spec_json)
        .bind(now_ms())
        .execute(&mut *tx)
        .await?;
        let id = r.last_insert_rowid();
        spec.id = id as u64;
        let _ = sqlx::query("UPDATE proxies SET spec=?2 WHERE id=?1")
            .bind(id)
            .bind(serde_json::to_string(&spec)?)
            .execute(&mut *tx)
            .await;
        sqlx::query("UPDATE clients SET config_revision = config_revision + 1 WHERE id=?1")
            .bind(client_id)
            .execute(&mut *tx)
            .await?;
        let rev: i64 = sqlx::query_scalar("SELECT config_revision FROM clients WHERE id=?1")
            .bind(client_id)
            .fetch_one(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok((id, rev as u64))
    }

    /// 更新规则 + revision+1(单事务)。
    pub async fn update_proxy_bump(
        &self,
        id: i64,
        client_id: &str,
        spec: &ProxySpec,
        port_auto: bool,
    ) -> Result<u64> {
        let spec_json = serde_json::to_string(&spec)?;
        let mut tx = self.pool.begin().await?;
        sqlx::query(
            "UPDATE proxies SET name=?2, kind=?3, enabled=?4, bind_ip=?5, bind_port=?6, port_auto=?7, target_host=?8, target_port=?9, pool_size=?10, group_name=?11, group_key=?12, spec=?13, updated_at=?14 WHERE id=?1",
        )
        .bind(id)
        .bind(&spec.name)
        .bind(spec.kind.as_str())
        .bind(spec.enabled as i64)
        .bind(&spec.bind_ip)
        .bind(spec.bind_port as i64)
        .bind(port_auto as i64)
        .bind(&spec.target_host)
        .bind(spec.target_port as i64)
        .bind(spec.pool_size as i64)
        .bind(spec.group.clone().unwrap_or_default())
        .bind(&spec.group_key)
        .bind(&spec_json)
        .bind(now_ms())
        .execute(&mut *tx)
        .await?;
        sqlx::query("UPDATE clients SET config_revision = config_revision + 1 WHERE id=?1")
            .bind(client_id)
            .execute(&mut *tx)
            .await?;
        let rev: i64 = sqlx::query_scalar("SELECT config_revision FROM clients WHERE id=?1")
            .bind(client_id)
            .fetch_one(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(rev as u64)
    }

    pub async fn update_proxy(&self, id: i64, spec: &ProxySpec, port_auto: bool) -> Result<()> {
        let spec_json = serde_json::to_string(&spec)?;
        sqlx::query(
            "UPDATE proxies SET name=?2, kind=?3, enabled=?4, bind_ip=?5, bind_port=?6, port_auto=?7, target_host=?8, target_port=?9, pool_size=?10, group_name=?11, group_key=?12, spec=?13, updated_at=?14 WHERE id=?1",
        )
        .bind(id)
        .bind(&spec.name)
        .bind(spec.kind.as_str())
        .bind(spec.enabled as i64)
        .bind(&spec.bind_ip)
        .bind(spec.bind_port as i64)
        .bind(port_auto as i64)
        .bind(&spec.target_host)
        .bind(spec.target_port as i64)
        .bind(spec.pool_size as i64)
        .bind(spec.group.clone().unwrap_or_default())
        .bind(&spec.group_key)
        .bind(&spec_json)
        .bind(now_ms())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn set_proxy_enabled(&self, id: i64, enabled: bool) -> Result<()> {
        sqlx::query("UPDATE proxies SET enabled=?2, updated_at=?3 WHERE id=?1")
            .bind(id)
            .bind(enabled as i64)
            .bind(now_ms())
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn delete_proxy(&self, id: i64) -> Result<()> {
        sqlx::query("DELETE FROM proxies WHERE id=?1")
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn get_proxy(&self, id: i64) -> Result<Option<(i64, String, ProxySpec, bool)>> {
        let row =
            sqlx::query("SELECT id, client_id, port_auto, enabled, spec FROM proxies WHERE id=?1")
                .bind(id)
                .fetch_optional(&self.pool)
                .await?;
        match row {
            Some(r) => {
                let pid: i64 = r.get(0);
                let client_id: String = r.get(1);
                let port_auto: bool = r.get::<i64, _>(2) != 0;
                let enabled = r.get::<i64, _>(3) != 0;
                let mut spec: ProxySpec = serde_json::from_str(&r.get::<String, _>(4))?;
                spec.id = pid as u64;
                spec.enabled = enabled;
                Ok(Some((pid, client_id, spec, port_auto)))
            }
            None => Ok(None),
        }
    }

    pub async fn list_proxies(
        &self,
        client_id: Option<&str>,
    ) -> Result<Vec<(i64, String, ProxySpec)>> {
        let rows = match client_id {
            Some(cid) => sqlx::query(
                "SELECT id, client_id, enabled, spec FROM proxies WHERE client_id=?1 ORDER BY id",
            )
            .bind(cid)
            .fetch_all(&self.pool)
            .await?,
            None => {
                sqlx::query("SELECT id, client_id, enabled, spec FROM proxies ORDER BY id")
                    .fetch_all(&self.pool)
                    .await?
            }
        };
        let mut out = Vec::new();
        for r in rows {
            let id: i64 = r.get(0);
            let client_id: String = r.get(1);
            let enabled = r.get::<i64, _>(2) != 0;
            // 单行损坏(如手工改库/版本残留)只跳过该行,不得令整个列表失败
            let raw: String = r.get(3);
            let Ok(mut spec) = serde_json::from_str::<ProxySpec>(&raw) else {
                tracing::warn!(proxy = id, "proxies row has corrupted spec json; skipping");
                continue;
            };
            spec.id = id as u64;
            spec.enabled = enabled;
            out.push((id, client_id, spec));
        }
        Ok(out)
    }

    /// 单事务:推进 config_revision 并返回新值(与规则变更同事务调用)。
    /// SQLite 无 FOR UPDATE;单写者 + 事务内原子自增保证单调。
    pub async fn bump_revision(&self, client_id: &str) -> Result<u64> {
        let mut tx = self.pool.begin().await?;
        sqlx::query("UPDATE clients SET config_revision = config_revision + 1 WHERE id=?1")
            .bind(client_id)
            .execute(&mut *tx)
            .await?;
        let new_rev: i64 = sqlx::query_scalar("SELECT config_revision FROM clients WHERE id=?1")
            .bind(client_id)
            .fetch_one(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(new_rev as u64)
    }

    // ---------- visitors ----------

    pub async fn insert_visitor(
        &self,
        client_id: &str,
        target_client_id: &str,
        spec: &VisitorSpec,
    ) -> Result<i64> {
        let r = sqlx::query(
            "INSERT INTO visitors (client_id, name, kind, enabled, bind_addr, bind_port, target_proxy_name, target_client_id, secret_key, use_compression, created_at, updated_at) \
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?11)",
        )
        .bind(client_id)
        .bind(&spec.name)
        .bind(spec.kind.as_str())
        .bind(spec.enabled as i64)
        .bind(&spec.bind_addr)
        .bind(spec.bind_port as i64)
        .bind(&spec.target_proxy_name)
        .bind(target_client_id)
        .bind(&spec.secret_key)
        .bind(spec.use_compression as i64)
        .bind(now_ms())
        .execute(&self.pool)
        .await?;
        Ok(r.last_insert_rowid())
    }

    pub async fn update_visitor(
        &self,
        id: i64,
        target_client_id: &str,
        spec: &VisitorSpec,
    ) -> Result<()> {
        sqlx::query(
            "UPDATE visitors SET name=?2, kind=?3, enabled=?4, bind_addr=?5, bind_port=?6, target_proxy_name=?7, target_client_id=?8, secret_key=?9, use_compression=?10, updated_at=?11 WHERE id=?1",
        )
        .bind(id)
        .bind(&spec.name)
        .bind(spec.kind.as_str())
        .bind(spec.enabled as i64)
        .bind(&spec.bind_addr)
        .bind(spec.bind_port as i64)
        .bind(&spec.target_proxy_name)
        .bind(target_client_id)
        .bind(&spec.secret_key)
        .bind(spec.use_compression as i64)
        .bind(now_ms())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn delete_visitor(&self, id: i64) -> Result<()> {
        sqlx::query("DELETE FROM visitors WHERE id=?1")
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn set_visitor_enabled(&self, id: i64, enabled: bool) -> Result<()> {
        sqlx::query("UPDATE visitors SET enabled=?2, updated_at=?3 WHERE id=?1")
            .bind(id)
            .bind(enabled as i64)
            .bind(now_ms())
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn list_visitors(
        &self,
        client_id: Option<&str>,
    ) -> Result<Vec<(i64, String, String, VisitorSpec)>> {
        let rows = match client_id {
            Some(cid) => sqlx::query("SELECT id, client_id, target_client_id, name, kind, enabled, bind_addr, bind_port, target_proxy_name, secret_key, use_compression FROM visitors WHERE client_id=?1 ORDER BY id")
                .bind(cid).fetch_all(&self.pool).await?,
            None => sqlx::query("SELECT id, client_id, target_client_id, name, kind, enabled, bind_addr, bind_port, target_proxy_name, secret_key, use_compression FROM visitors ORDER BY id")
                .fetch_all(&self.pool).await?,
        };
        let mut out = Vec::new();
        for r in rows {
            let spec = VisitorSpec {
                id: r.get::<i64, _>(0) as u64,
                name: r.get::<String, _>(3),
                kind: match r.get::<String, _>(4).as_str() {
                    "sudp" => VisitorKind::Sudp,
                    "xtcp" => VisitorKind::Xtcp,
                    _ => VisitorKind::Stcp,
                },
                enabled: r.get::<i64, _>(5) != 0,
                bind_addr: r.get::<String, _>(6),
                bind_port: r.get::<i64, _>(7) as u16,
                target_proxy_name: r.get::<String, _>(8),
                secret_key: r.get::<String, _>(9),
                use_compression: r.get::<i64, _>(10) != 0,
            };
            out.push((
                r.get::<i64, _>(0),
                r.get::<String, _>(1),
                r.get::<String, _>(2),
                spec,
            ));
        }
        Ok(out)
    }

    // ---------- events ----------

    pub async fn insert_event(
        &self,
        level: &str,
        source: &str,
        message: &str,
        detail: &str,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO events (ts, level, source, message, detail) VALUES (?1,?2,?3,?4,?5)",
        )
        .bind(now_ms())
        .bind(level)
        .bind(source)
        .bind(message)
        .bind(detail)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn list_events(&self, limit: i64, level: Option<&str>) -> Result<Vec<EventRow>> {
        let rows = match level {
            Some(lv) => sqlx::query("SELECT id, ts, level, source, message, detail FROM events WHERE level=?1 ORDER BY id DESC LIMIT ?2")
                .bind(lv).bind(limit).fetch_all(&self.pool).await?,
            None => sqlx::query("SELECT id, ts, level, source, message, detail FROM events ORDER BY id DESC LIMIT ?1")
                .bind(limit).fetch_all(&self.pool).await?,
        };
        Ok(rows.iter().map(EventRow::from_row).collect())
    }
}

#[derive(Debug, Clone)]
pub struct ClientRow {
    pub id: String,
    pub name: String,
    pub secret: String,
    pub enabled: bool,
    pub remark: String,
    pub config_revision: i64,
    pub created_at: i64,
    pub last_online_at: Option<i64>,
    pub client_version: String,
    pub hostname: String,
    pub os: String,
    /// F2 配额(0/空 = 不限)。rate_limit 如 "1MB";flow_limit 单位字节。
    pub rate_limit: String,
    pub max_conns: i64,
    pub max_services: i64,
    pub flow_limit: i64,
}

impl ClientRow {
    fn from_row(r: &SqliteRow) -> Self {
        Self {
            id: r.get(0),
            name: r.get(1),
            secret: r.get(2),
            enabled: r.get::<i64, _>(3) != 0,
            remark: r.get(4),
            config_revision: r.get(5),
            created_at: r.get(6),
            last_online_at: r.get(7),
            client_version: r.get(8),
            hostname: r.get(9),
            os: r.get(10),
            rate_limit: r.get(11),
            max_conns: r.get(12),
            max_services: r.get(13),
            flow_limit: r.get(14),
        }
    }
}

#[derive(Debug, Clone)]
pub struct CertRow {
    pub id: i64,
    pub name: String,
    /// JSON 数组字符串。
    pub domains: String,
    pub cert_pem: String,
    pub key_pem: String,
    pub fingerprint: String,
    /// unix ms。
    pub not_after: i64,
    pub created_at: i64,
}

/// 证书写入参数(domains 为 JSON 数组字符串)。
pub struct CertUpdate {
    pub name: String,
    pub domains: String,
    pub cert_pem: String,
    pub key_pem: String,
    pub fingerprint: String,
    pub not_after: i64,
}

impl Db {
    pub async fn insert_cert(&self, c: &CertUpdate) -> Result<i64> {
        let r = sqlx::query(
            "INSERT INTO certs (name, domains, cert_pem, key_pem, fingerprint, not_after, created_at) VALUES (?1,?2,?3,?4,?5,?6,?7)",
        )
        .bind(&c.name)
        .bind(&c.domains)
        .bind(&c.cert_pem)
        .bind(&c.key_pem)
        .bind(&c.fingerprint)
        .bind(c.not_after)
        .bind(now_ms())
        .execute(&self.pool)
        .await?;
        Ok(r.last_insert_rowid())
    }

    pub async fn list_certs(&self) -> Result<Vec<CertRow>> {
        let rows = sqlx::query(
            "SELECT id, name, domains, cert_pem, key_pem, fingerprint, not_after, created_at FROM certs ORDER BY created_at",
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .iter()
            .map(|r| CertRow {
                id: r.get(0),
                name: r.get(1),
                domains: r.get(2),
                cert_pem: r.get(3),
                key_pem: r.get(4),
                fingerprint: r.get(5),
                not_after: r.get(6),
                created_at: r.get(7),
            })
            .collect())
    }

    pub async fn update_cert(&self, id: i64, c: &CertUpdate) -> Result<()> {
        sqlx::query(
            "UPDATE certs SET name=?2, domains=?3, cert_pem=?4, key_pem=?5, fingerprint=?6, not_after=?7 WHERE id=?1",
        )
        .bind(id)
        .bind(&c.name)
        .bind(&c.domains)
        .bind(&c.cert_pem)
        .bind(&c.key_pem)
        .bind(&c.fingerprint)
        .bind(c.not_after)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn delete_cert(&self, id: i64) -> Result<()> {
        sqlx::query("DELETE FROM certs WHERE id=?1")
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct EventRow {
    pub id: i64,
    pub ts: i64,
    pub level: String,
    pub source: String,
    pub message: String,
    pub detail: String,
}

impl EventRow {
    fn from_row(r: &SqliteRow) -> Self {
        Self {
            id: r.get(0),
            ts: r.get(1),
            level: r.get(2),
            source: r.get(3),
            message: r.get(4),
            detail: r.get(5),
        }
    }
}

/// ProxySpec 默认值归一化。max_pool:池深上限(0 = 不限),超出即钳制。
pub fn normalize_spec(spec: &mut ProxySpec, default_pool: u8, max_pool: u8) {
    if spec.pool_size == 0 {
        spec.pool_size = default_pool;
    }
    if max_pool > 0 && spec.pool_size > max_pool {
        spec.pool_size = max_pool;
    }
    if spec.bind_ip.is_empty() {
        spec.bind_ip = "0.0.0.0".into();
    }
    if spec.target_host.is_empty() {
        spec.target_host = "127.0.0.1".into();
    }
    spec.normalize_kind_configs();
}

#[cfg(test)]
mod tests {
    use super::*;
    use rh_protocol::{ProxyKind, ProxyTransport};

    #[tokio::test]
    async fn migrations_and_crud_roundtrip() {
        let db = Db::open("sqlite::memory:").await.unwrap();
        db.create_user("admin", "hash").await.unwrap();
        assert_eq!(db.user_count().await.unwrap(), 1);
        assert!(db.get_user("admin").await.unwrap().is_some());
        assert!(db.get_user("nobody").await.unwrap().is_none());

        db.create_client("cid-1", "c1", &"s".repeat(32), "")
            .await
            .unwrap();
        let mut spec = ProxySpec {
            id: 0,
            client_id: String::new(),
            name: "web".into(),
            kind: ProxyKind::Tcp,
            enabled: true,
            bind_ip: "0.0.0.0".into(),
            bind_port: 5200,
            target_host: "127.0.0.1".into(),
            target_port: 80,
            pool_size: 2,
            transport: ProxyTransport::default(),
            group: None,
            group_key: String::new(),
            health_check: None,
            plugin: None,
            udp: None,
            http: None,
            https: None,
            stcp: None,
            sudp: None,
            xtcp: None,
            tcpmux: None,
        };
        normalize_spec(&mut spec, 2, 0);
        let id = db
            .insert_proxy("cid-1", &spec, false, "console")
            .await
            .unwrap();
        let got = db.get_proxy(id).await.unwrap().unwrap();
        assert_eq!(got.2.name, "web");
        assert_eq!(got.2.kind, ProxyKind::Tcp);

        // UNIQUE(client_id, name) 约束生效
        assert!(db
            .insert_proxy("cid-1", &spec, false, "console")
            .await
            .is_err());

        // 外键级联
        db.delete_client("cid-1").await.unwrap();
        assert!(db.get_proxy(id).await.unwrap().is_none());
    }
}
