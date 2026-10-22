//! 管理 REST API(axum):vben 契约 + 客户端/规则/visitor/统计/事件/指标。

use crate::db::normalize_spec;
use crate::state::{AppState, ProxyState};
use axum::extract::{ConnectInfo, Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post, put};
use axum::{Json, Router};
use rh_protocol::{ProxyKind, ProxySpec, VisitorSpec};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::Arc;

pub fn router(state: Arc<AppState>) -> Router {
    let auth = Router::new()
        .route("/api/auth/login", post(login))
        .route("/api/auth/logout", post(logout))
        .route("/api/auth/password", post(change_password))
        .route("/api/user/info", get(user_info))
        .route("/api/auth/codes", get(auth_codes))
        .route("/api/menu/all", get(menu_all));
    let clients = Router::new()
        .route("/api/clients", get(list_clients).post(create_client))
        .route(
            "/api/clients/{id}",
            put(update_client).delete(delete_client),
        )
        .route("/api/clients/{id}/rotate-secret", post(rotate_secret));
    let services = Router::new()
        .route("/api/services", get(list_services).post(create_service))
        .route(
            "/api/services/{id}",
            put(update_service).delete(delete_service),
        )
        .route("/api/services/{id}/enable", post(enable_service))
        .route("/api/services/{id}/disable", post(disable_service))
        .route(
            "/api/services/{id}/rotate-secret",
            post(rotate_service_secret),
        )
        .route("/api/services/batch", post(batch_services));
    let visitors = Router::new()
        .route("/api/visitors", get(list_visitors).post(create_visitor))
        .route(
            "/api/visitors/{id}",
            put(update_visitor).delete(delete_visitor),
        )
        .route("/api/visitors/{id}/enable", post(enable_visitor))
        .route("/api/visitors/{id}/disable", post(disable_visitor));
    let misc = Router::new()
        .route("/api/stats/overview", get(stats_overview))
        .route("/api/events", get(list_events))
        .route(
            "/api/server/config",
            get(get_server_config).put(put_server_config),
        )
        .route("/api/certs", get(list_certs_api).post(create_cert_api))
        .route(
            "/api/certs/{id}",
            put(update_cert_api).delete(delete_cert_api),
        );
    auth.merge(clients)
        .merge(services)
        .merge(visitors)
        .merge(misc)
        .with_state(state)
}

async fn menu_all(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    if require_auth(&state, &headers).await.is_err() {
        return unauthorized();
    }
    ok(serde_json::json!([]))
}

// ---------------- envelope ----------------

pub fn ok<T: Serialize>(data: T) -> Response {
    Json(json!({"code": 0, "data": data, "message": "ok"})).into_response()
}

pub fn err(code: i32, message: &str) -> Response {
    Json(json!({"code": code, "message": message, "data": null})).into_response()
}

fn unauthorized() -> Response {
    (
        StatusCode::UNAUTHORIZED,
        Json(json!({"code": 401, "message": "unauthorized", "data": null})),
    )
        .into_response()
}

// ---------------- auth ----------------

#[derive(Deserialize)]
struct LoginReq {
    username: String,
    password: String,
}

/// 登录限速的两级键:(每 IP, 每 IP+用户名)。
///
/// **只含 IP,绝不含端口** —— 见 `login` 里的详细说明。抽成纯函数是为了
/// 让"键里不许有端口"这条约束可被单测钉住(见 `login_rate_keys_ignores_port`)。
fn login_rate_keys(ip: std::net::IpAddr, uname: &str) -> (String, String) {
    (format!("ip|{ip}"), format!("{ip}|{uname}"))
}

async fn login(
    State(state): State<Arc<AppState>>,
    ConnectInfo(peer): ConnectInfo<crate::WebAddr>,
    Json(req): Json<LoginReq>,
) -> Response {
    // C7:登录限速改为两级 —— 每 IP 一个宽桶 + 每 IP+用户名一个小桶。
    //
    // 修前只有 `{ip}|{user}` 单键:攻击者轮换用户名即可绕过 IP 限制,
    // 且键随"用户名"无限增长(无淘汰)。现在 IP 维度的桶先兜住洪峰,
    // 用户名截断仍然保留。
    //
    // ★限速键必须只含 **IP**,不能含端口:`WebAddr` 的 Display 直通
    // `SocketAddr`(main.rs:467),输出带临时端口。每次请求新建 TCP 连接
    // (curl 默认行为)就是新键,两级限速全部落空 —— 而登录路径每次失败
    // 都会跑一次 argon2id(约 20MB 内存 + 数十 ms CPU),governor 还会为
    // 每个唯一键**永久保留**状态,构成未认证 DoS + 无界内存增长。
    let ip = peer.0.ip();
    let uname: String = req.username.chars().take(64).collect();
    let (k_ip, k_user) = login_rate_keys(ip, &uname);
    if state.login_limiter.check_key(&k_ip).is_err()
        || state.login_limiter_user.check_key(&k_user).is_err()
    {
        state
            .events
            .emit("warn", "auth", &format!("登录尝试受限:{uname}"))
            .await;
        return err(429, "尝试过于频繁,请稍后再试");
    }
    let user = match state.db.get_user(&req.username).await {
        Ok(Some(u)) => u,
        _ => {
            // 对不存在的用户也执行一次等价 argon2 校验,消除时序侧信道枚举。
            // C6:argon2id 是 CPU 密集的同步计算,必须放到 blocking 池 ——
            // 直接在 tokio worker 上跑会让并发登录把转发任务一起卡住。
            const DUMMY_HASH: &str = "$argon2id$v=19$m=19456,t=2,p=1$OXRoZXJlYXJlZnJwaHJvbGl0Z$5ENkQGI9XCo8uUU6eW/y6vLzRnF8YD2NO6wanjaApfE";
            let pw = req.password.clone();
            let _ = tokio::task::spawn_blocking(move || verify_password(&pw, DUMMY_HASH)).await;
            state
                .events
                .emit("warn", "auth", &format!("登录失败(用户不存在):{uname}"))
                .await;
            return err(1001, "用户名或密码错误");
        }
    };
    // C6:同上 —— 真实校验也必须在 blocking 池。
    let pw = req.password.clone();
    let hash = user.1.clone();
    let verified = tokio::task::spawn_blocking(move || verify_password(&pw, &hash))
        .await
        .unwrap_or(false);
    if !verified {
        state
            .events
            .emit(
                "warn",
                "auth",
                &format!("登录失败(密码错误):{}", req.username),
            )
            .await;
        return err(1001, "用户名或密码错误");
    }
    let ttl = chrono_hours(state.cfg.web.jwt_ttl_hours);
    // B11:令牌携带签发时的 pwd_version,登出/改密递增后旧令牌立即 401。
    let pv = state.db.user_pwd_version(&req.username).await.unwrap_or(0);
    let token = match mint_jwt(&state, &req.username, ttl, pv).await {
        Ok(t) => t,
        Err(e) => return err(500, &format!("内部错误(jwt):{e}")),
    };
    state
        .events
        .emit("info", "auth", &format!("登录成功:{}", req.username))
        .await;
    ok(json!({ "accessToken": token }))
}

#[derive(Deserialize)]
struct ChangePasswordReq {
    old_password: String,
    new_password: String,
}

/// 修改自身密码:验旧密 → Argon2 落库 → 轮换 JWT 密钥(所有已发令牌立即失效,
/// 含当前会话,前端需回到登录页)。新密码最短 8 位。
async fn change_password(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(req): Json<ChangePasswordReq>,
) -> Response {
    let user = match require_auth(&state, &headers).await {
        Ok(u) => u,
        Err(resp) => return *resp,
    };
    if req.new_password.len() < 8 {
        return err(1002, "新密码至少需要 8 位");
    }
    let row = match state.db.get_user(&user).await {
        Ok(Some(r)) => r,
        _ => return err(1001, "用户不存在"),
    };
    // C6 同款:argon2 是 CPU/内存密集操作(m=19456,t=2),必须在 blocking 池
    // 里跑。修前在 worker 线程同步执行 —— 持 token 并发调本接口即可把
    // executor 线程逐个卡住(login 早已修,这里此前漏了)。
    let old_pw = req.old_password.clone();
    let old_hash = row.1.clone();
    let old_ok = tokio::task::spawn_blocking(move || verify_password(&old_pw, &old_hash))
        .await
        .unwrap_or(false);
    if !old_ok {
        state
            .events
            .emit(
                "warn",
                "auth",
                &format!("修改密码被拒(旧密码错误):{}", user),
            )
            .await;
        return err(1001, "旧密码错误");
    }
    let new_pw = req.new_password.clone();
    let hash = match tokio::task::spawn_blocking(move || hash_password(&new_pw)).await {
        Ok(Ok(h)) => h,
        Ok(Err(e)) => return err(500, &format!("内部错误(hash):{e}")),
        Err(_) => return err(500, "内部错误(hash worker)"),
    };
    match state.db.update_user_password(&user, &hash).await {
        Ok(true) => {}
        _ => return err(500, "更新失败"),
    }
    // 轮换会话密钥:旧令牌(含本请求所用)全部失效
    *state.web_jwt_secret.lock().await = Some(uuid::Uuid::new_v4().simple().to_string());
    state
        .events
        .emit("info", "auth", &format!("密码已修改:{}", user))
        .await;
    ok(json!({ "relogin": true }))
}

fn chrono_hours(h: u64) -> u64 {
    h
}

fn verify_password(plain: &str, hash: &str) -> bool {
    use argon2::PasswordVerifier;
    argon2::password_hash::PasswordHash::new(hash)
        .map(|parsed| {
            argon2::Argon2::default()
                .verify_password(plain.as_bytes(), &parsed)
                .is_ok()
        })
        .unwrap_or(false)
}

pub fn hash_password(plain: &str) -> anyhow::Result<String> {
    use argon2::password_hash::{PasswordHasher, SaltString};
    let salt = SaltString::generate(&mut argon2::password_hash::rand_core::OsRng);
    Ok(argon2::Argon2::default()
        .hash_password(plain.as_bytes(), &salt)
        .map_err(|e| anyhow::anyhow!("hash: {e:?}"))?
        .to_string())
}

async fn mint_jwt(
    state: &Arc<AppState>,
    username: &str,
    ttl_hours: u64,
    pwd_version: i64,
) -> anyhow::Result<String> {
    let secret = state.jwt_secret().await;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs() as i64;
    let claims = json!({
        "sub": username,
        "iat": now,
        "exp": now + (ttl_hours as i64) * 3600,
        "pv": pwd_version,
    });
    Ok(jsonwebtoken::encode(
        &jsonwebtoken::Header::default(),
        &claims,
        &jsonwebtoken::EncodingKey::from_secret(secret.as_bytes()),
    )?)
}

impl AppState {
    pub async fn jwt_secret(&self) -> String {
        self.web_jwt_secret.lock().await.clone().unwrap_or_default()
    }
}

/// JWT 校验中间件:返回 username 或 401 response(装箱避免超大 Err 变体)。
async fn require_auth(state: &Arc<AppState>, headers: &HeaderMap) -> Result<String, Box<Response>> {
    let bearer = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .unwrap_or("");
    if bearer.is_empty() {
        return Err(Box::new(unauthorized()));
    }
    let secret = state.jwt_secret().await;
    let data = jsonwebtoken::decode::<serde_json::Value>(
        bearer,
        &jsonwebtoken::DecodingKey::from_secret(secret.as_bytes()),
        &jsonwebtoken::Validation::default(),
    );
    match data {
        Ok(d) => {
            let user = d
                .claims
                .get("sub")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            if user.is_empty() {
                return Err(Box::new(unauthorized()));
            }
            // B11:令牌版本必须与用户当前 pwd_version 一致。登出/改密递增
            // 后,旧令牌在下一次请求即失效(无状态 JWT 的吊销点)。
            let want = d.claims.get("pv").and_then(|v| v.as_i64()).unwrap_or(0);
            let cur = state.db.user_pwd_version(&user).await.unwrap_or(0);
            if want != cur {
                return Err(Box::new(unauthorized()));
            }
            Ok(user)
        }
        Err(_) => Err(Box::new(unauthorized())),
    }
}

async fn logout(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    match require_auth(&state, &headers).await {
        Ok(user) => {
            // B11:登出即吊销 —— 递增 pwd_version,该用户全部既有令牌失效。
            if let Err(e) = state.db.bump_user_pwd_version(&user).await {
                tracing::warn!(error = %e, user = %user, "logout: bump pwd_version failed");
            }
            state
                .events
                .emit("info", "auth", &format!("已登出:{user}"))
                .await;
            StatusCode::NO_CONTENT.into_response()
        }
        Err(r) => *r,
    }
}

async fn user_info(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    let user = match require_auth(&state, &headers).await {
        Ok(u) => u,
        Err(r) => return *r,
    };
    ok(json!({
        "userId": user,
        "username": user,
        "realName": user,
        "avatar": "",
        "desc": "rabbithole administrator",
        "homePath": "/rabbithole/overview",
        "roles": ["admin"],
    }))
}

async fn auth_codes(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    if require_auth(&state, &headers).await.is_err() {
        return unauthorized();
    }
    ok(json!([]))
}

// ---------------- clients ----------------

async fn list_clients(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    if require_auth(&state, &headers).await.is_err() {
        return unauthorized();
    }
    let rows = match state.db.list_clients().await {
        Ok(r) => r,
        Err(e) => return err(500, &format!("{e}")),
    };
    // F1 分客户端聚合:其名下全部规则运行时之和(重名规则口径与 visitor 归因一致)
    let per_client: std::collections::HashMap<String, (u64, u64, i64)> = state
        .proxies
        .runtimes
        .iter()
        .fold(std::collections::HashMap::new(), |mut m, e| {
            let rt = e.value();
            let ent = m.entry(rt.client_id.clone()).or_insert((0, 0, 0));
            ent.0 += rt.bytes_in.load(std::sync::atomic::Ordering::Relaxed);
            ent.1 += rt.bytes_out.load(std::sync::atomic::Ordering::Relaxed);
            ent.2 += rt
                .conns_current
                .load(std::sync::atomic::Ordering::Relaxed)
                .max(0);
            m
        });
    let out: Vec<_> = rows
        .into_iter()
        .map(|c| {
            let sess = state.sessions.get(&c.id);
            let meter = per_client.get(&c.id).copied().unwrap_or((0, 0, 0));
            json!({
                "id": c.id,
                "name": c.name,
                "enabled": c.enabled,
                "remark": c.remark,
                "online": sess.is_some(),
                "online_since": sess.as_ref().map(|s| s.started_at.elapsed().as_secs()),
                "reported_revision": sess.as_ref().map(|s| s.applied()),
                "expected_revision": c.config_revision,
                "client_version": c.client_version,
                "hostname": c.hostname,
                "os": c.os,
                "last_online_at": c.last_online_at,
                "created_at": c.created_at,
                "bytes_in": meter.0,
                "bytes_out": meter.1,
                "conns_current": meter.2,
                // F2 配额(0/空 = 不限)
                "rate_limit": c.rate_limit,
                "max_conns": c.max_conns,
                "max_services": c.max_services,
                "flow_limit": c.flow_limit,
            })
        })
        .collect();
    ok(out)
}

#[derive(Deserialize)]
struct CreateClientReq {
    name: String,
    #[serde(default)]
    remark: String,
    /// F2 配额(可选;rate_limit 如 "1MB",flow_limit 单位字节,0/空 = 不限)。
    #[serde(default)]
    rate_limit: Option<String>,
    #[serde(default)]
    max_conns: Option<i64>,
    #[serde(default)]
    max_services: Option<i64>,
    #[serde(default)]
    flow_limit: Option<i64>,
}

fn validate_quota(rate: Option<&str>, max_conns: i64, max_services: i64) -> Option<String> {
    if let Some(r) = rate {
        if !r.is_empty() && rh_core::BandwidthQuantity::parse(r).is_none() {
            return Some(format!("限速格式非法:{r}(示例 1MB / 512KB)"));
        }
    }
    if max_conns < 0 || max_services < 0 {
        return Some("配额不能为负".into());
    }
    None
}

async fn create_client(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(req): Json<CreateClientReq>,
) -> Response {
    if require_auth(&state, &headers).await.is_err() {
        return unauthorized();
    }
    let name = req.name.trim();
    if name.is_empty() || name.len() > 64 {
        return err(1002, "名称长度须为 1-64 字符");
    }
    let id = uuid::Uuid::new_v4().to_string();
    let secret = crate::control::hmac_hex(
        uuid::Uuid::new_v4().as_bytes(),
        &uuid::Uuid::new_v4().to_string(),
    );
    // 64 hex
    let secret = format!("{secret}{secret}");
    let secret = secret[..64].to_string();
    if let Err(e) = state
        .db
        .create_client(&id, name, &secret, &req.remark)
        .await
    {
        return err(1003, &format!("创建失败:{e}"));
    }
    if let Some(msg) = validate_quota(
        req.rate_limit.as_deref(),
        req.max_conns.unwrap_or(0),
        req.max_services.unwrap_or(0),
    ) {
        let _ = state.db.delete_client(&id).await;
        return err(1002, &msg);
    }
    let _ = state
        .db
        .update_client_quota(
            &id,
            req.rate_limit.as_deref(),
            req.max_conns,
            req.max_services,
            req.flow_limit,
        )
        .await;
    state.refresh_client_quotas().await;
    state
        .events
        .emit("info", "client", &format!("客户端已创建:{name}"))
        .await;
    ok(json!({
        "id": id,
        "name": name,
        "remark": req.remark,
        "secret": secret,   // 仅本次返回
    }))
}

#[derive(Deserialize)]
struct UpdateClientReq {
    enabled: Option<bool>,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    remark: Option<String>,
    #[serde(default)]
    rate_limit: Option<String>,
    #[serde(default)]
    max_conns: Option<i64>,
    #[serde(default)]
    max_services: Option<i64>,
    #[serde(default)]
    flow_limit: Option<i64>,
}

async fn update_client(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(req): Json<UpdateClientReq>,
) -> Response {
    if require_auth(&state, &headers).await.is_err() {
        return unauthorized();
    }
    if let Some(name) = &req.name {
        let name = name.trim();
        if name.is_empty() || name.len() > 64 {
            return err(1002, "名称长度须为 1-64 字符");
        }
    }
    let lock = state.serial_lock(&id);
    let _g = lock.lock().await;
    let Some(_c) = state.db.get_client(&id).await.ok().flatten() else {
        return err(1004, "客户端不存在");
    };
    if req.name.is_some() || req.remark.is_some() {
        let name = req.name.as_deref().map(str::trim);
        if let Err(e) = state
            .db
            .update_client_profile(&id, name, req.remark.as_deref().map(|r| r.trim()))
            .await
        {
            return err(500, &format!("{e}"));
        }
        state
            .events
            .emit("info", "client", &format!("客户端资料已更新:{}", _c.name))
            .await;
    }
    if req.rate_limit.is_some()
        || req.max_conns.is_some()
        || req.max_services.is_some()
        || req.flow_limit.is_some()
    {
        let cur_rate = req
            .rate_limit
            .clone()
            .unwrap_or_else(|| _c.rate_limit.clone());
        if let Some(msg) = validate_quota(
            Some(&cur_rate),
            req.max_conns.unwrap_or(_c.max_conns),
            req.max_services.unwrap_or(_c.max_services),
        ) {
            return err(1002, &msg);
        }
        if let Err(e) = state
            .db
            .update_client_quota(
                &id,
                req.rate_limit.as_deref(),
                req.max_conns,
                req.max_services,
                req.flow_limit,
            )
            .await
        {
            return err(500, &format!("{e}"));
        }
        state.refresh_client_quotas().await;
        state
            .events
            .emit("info", "client", &format!("客户端配额已更新:{}", _c.name))
            .await;
    }
    if let Some(enabled) = req.enabled {
        if let Err(e) = state.db.set_client_enabled(&id, enabled).await {
            return err(500, &format!("{e}"));
        }
        if enabled {
            crate::runtime::notify_client_recovery(&state, &id).await;
        } else {
            // 禁用:踢会话 + 全部规则 rule_cancel + REMOVE 入队
            if let Some(sess) = state.sessions.get(&id) {
                sess.cancel.cancel();
            }
            let proxies = state.db.list_proxies(Some(&id)).await.unwrap_or_default();
            let visitors = state.db.list_visitors(Some(&id)).await.unwrap_or_default();
            let rev = state.db.bump_revision(&id).await.unwrap_or(0);
            state.bump_cfg_gen(&id);
            for (pid, _, _spec) in &proxies {
                crate::runtime::teardown_runtime(&state, *pid as u64).await;
                if let Some(sess) = state.sessions.get(&id) {
                    use rh_protocol::message::{ServerMessage, ServiceRemove};
                    let _ = sess
                        .send(ServerMessage::ServiceRemove(ServiceRemove {
                            revision: rev,
                            entity_type: "proxy".into(),
                            entity_id: *pid as u64,
                        }))
                        .await;
                }
            }
            for (vid, _, _, _) in &visitors {
                if let Some(sess) = state.sessions.get(&id) {
                    use rh_protocol::message::{ServerMessage, ServiceRemove};
                    let _ = sess
                        .send(ServerMessage::ServiceRemove(ServiceRemove {
                            revision: rev,
                            entity_type: "visitor".into(),
                            entity_id: *vid as u64,
                        }))
                        .await;
                }
            }
            state
                .events
                .emit("warn", "client", &format!("客户端已禁用:{}", _c.name))
                .await;
        }
    }
    ok(json!({"updated": true}))
}

async fn rotate_secret(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    if require_auth(&state, &headers).await.is_err() {
        return unauthorized();
    }
    let lock = state.serial_lock(&id);
    let _g = lock.lock().await;
    let new_secret = crate::control::hmac_hex(
        uuid::Uuid::new_v4().as_bytes(),
        &uuid::Uuid::new_v4().to_string(),
    );
    let new_secret = format!("{new_secret}{new_secret}");
    let new_secret = new_secret[..64].to_string();
    if let Err(e) = state.db.rotate_client_secret(&id, &new_secret).await {
        return err(500, &format!("{e}"));
    }
    // 踢线重认证(listener 不动)
    if let Some(sess) = state.sessions.get(&id) {
        sess.cancel.cancel();
    }
    state
        .events
        .emit("warn", "client", "客户端密钥已轮换")
        .await;
    ok(json!({ "secret": new_secret }))
}

async fn delete_client(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    if require_auth(&state, &headers).await.is_err() {
        return unauthorized();
    }
    let lock = state.serial_lock(&id);
    let _g = lock.lock().await;
    let proxies = state.db.list_proxies(Some(&id)).await.unwrap_or_default();
    for (pid, _, _) in &proxies {
        crate::runtime::teardown_runtime(&state, *pid as u64).await;
    }
    if let Some(sess) = state.sessions.get(&id) {
        sess.cancel.cancel();
    }
    if let Err(e) = state.db.delete_client(&id).await {
        return err(500, &format!("{e}"));
    }
    state.refresh_client_quotas().await;
    state.events.emit("warn", "client", "客户端已删除").await;
    ok(json!({"deleted": true}))
}

// ---------------- services(proxies) ----------------

#[derive(Deserialize)]
struct ListQuery {
    client_id: Option<String>,
}

async fn list_services(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(q): Query<ListQuery>,
) -> Response {
    if require_auth(&state, &headers).await.is_err() {
        return unauthorized();
    }
    let rows = match state.db.list_proxies(q.client_id.as_deref()).await {
        Ok(r) => r,
        Err(e) => return err(500, &format!("{e}")),
    };
    let mut out = Vec::new();
    for (id, client_id, mut spec) in rows {
        {
            let l = state.limits.read().unwrap();
            normalize_spec(&mut spec, l.default_pool_size, l.max_pool_size);
        }
        // ★列表回显前抹掉 secret_key。
        //
        // 下方 `"spec": spec` 会把整个 ProxySpec 序列化给前端,而 stcp/sudp
        // 的 `secret_key` 是访问密钥(与客户端 secret 同级敏感)。它"仅创建时
        // 返回一次"是既有策略(见 rotate-secret),列表接口不该把它翻出来。
        // 前端不需要该值:编辑时留空即表示"不修改"(update 路径按空值保留原密钥,
        // 见 api.rs 里 `old...filter(|c| !c.secret_key.is_empty())`)。
        let mut spec_view = spec.clone();
        if let Some(secret_cfg) = spec_view.stcp.as_mut() {
            secret_cfg.secret_key.clear();
        }
        if let Some(secret_cfg) = spec_view.sudp.as_mut() {
            secret_cfg.secret_key.clear();
        }
        let (status, reason) = match state.proxies.get(id as u64) {
            Some(rt) => {
                let (st, r) = rt.snapshot_state().await;
                (st.as_str().to_string(), r)
            }
            None => ("disabled".into(), String::new()),
        };
        let pool_ready = match state.proxies.get(id as u64) {
            Some(rt) => match rt.pool.lock().await.as_ref() {
                Some(p) => p.ready.lock().await.len(),
                None => 0,
            },
            None => 0,
        };
        let rt_meter = state.proxies.get(id as u64).map(|rt| {
            (
                rt.bytes_in.load(std::sync::atomic::Ordering::Relaxed),
                rt.bytes_out.load(std::sync::atomic::Ordering::Relaxed),
                rt.conns_current
                    .load(std::sync::atomic::Ordering::Relaxed)
                    .max(0),
            )
        });
        out.push(json!({
            "id": id,
            "client_id": client_id,
            "name": spec.name,
            "kind": spec.kind.as_str(),
            "enabled": spec.enabled,
            "bind_ip": spec.bind_ip,
            "bind_port": spec.bind_port,
            "target": format!("{}:{}", spec.target_host, spec.target_port),
            "target_host": spec.target_host,
            "target_port": spec.target_port,
            "plugin": spec.plugin.as_ref().map(|p| p.kind().as_str().to_string()),
            "group": spec.group,
            "status": status,
            "status_reason": reason,
            "healthy": state.proxies.get(id as u64).map(|rt| rt.healthy.load(std::sync::atomic::Ordering::Relaxed)).unwrap_or(true),
            "pool_ready": pool_ready,
            "spec": spec_view,
            // F1 分规则计量(用户侧口径;运行时不在(已禁用)则为 0)
            "bytes_in": rt_meter.map(|(i, _, _)| i).unwrap_or(0),
            "bytes_out": rt_meter.map(|(_, o, _)| o).unwrap_or(0),
            "conns_current": rt_meter.map(|(_, _, c)| c).unwrap_or(0),
        }));
    }
    ok(out)
}

async fn create_service(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(mut spec): Json<ProxySpec>,
) -> Response {
    if require_auth(&state, &headers).await.is_err() {
        return unauthorized();
    }
    match create_service_inner(&state, &mut spec).await {
        Ok(v) => ok(v),
        Err((code, msg)) => err(code, &msg),
    }
}

async fn create_service_inner(
    state: &Arc<AppState>,
    spec: &mut ProxySpec,
) -> Result<serde_json::Value, (i32, String)> {
    // 基础校验
    if spec.name.is_empty()
        || !spec
            .name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
        || spec.name.len() > 64
    {
        return Err((2001, "名称须为 1-64 位字母/数字/下划线/连字符".into()));
    }
    {
        let l = state.limits.read().unwrap();
        normalize_spec(spec, l.default_pool_size, l.max_pool_size);
    }
    // 密钥完全服务端管理:创建即生成随机凭据(入参提供的一律覆盖),
    // 库中密钥访问规则永不为空,也不存在人编口令
    if let Some(cfg) = spec.secret_cfg_mut() {
        cfg.secret_key = generate_secret();
    }
    if spec.client_id.is_empty() {
        return Err((2002, "缺少 client_id".into()));
    }
    let client = state
        .db
        .get_client(&spec.client_id)
        .await
        .map_err(|e| (500, format!("{e}")))?
        .ok_or((2002, "客户端不存在".to_string()))?;
    let client_enabled = client.enabled;
    spec.enabled = spec.enabled && client_enabled;
    let lock = state.serial_lock(&client.id);
    let _g = lock.lock().await;
    // 每客户端数量上限(串行域内计数,防并发创建突破配额);
    // F2:客户端级 max_services 覆盖全局默认
    let per_client_max = if client.max_services > 0 {
        client.max_services as usize
    } else {
        state.hot.read().await.max_services_per_client
    };
    if per_client_max > 0 {
        let count = state
            .db
            .list_proxies(Some(&client.id))
            .await
            .map_err(|e| (500, format!("{e}")))?
            .len();
        if count >= per_client_max {
            return Err((2003, "LIMIT_EXCEEDED:超出配额限制".into()));
        }
    }
    // 资格跳步(§4.4):客户端禁用 → 仅落库为 disabled(不占端口、不推进 revision)
    if !client_enabled {
        let id = state
            .db
            .insert_proxy(&client.id, spec, false, "console")
            .await
            .map_err(|e| (2010, format!("{e}")))?;
        spec.id = id as u64;
        let cur_rev = client.config_revision.max(0) as u64;
        crate::runtime::spawn_runtime(state, &client.id, spec, cur_rev).await;
        return Ok(json!({
            "id": id,
            "bind_port": spec.bind_port,
            "port_auto": false,
            "deferred": true,
        }));
    }
    // 端口处理(tcp/udp)
    let mut port_auto = false;
    if spec.kind.uses_bind_port() {
        let bind_ip = rh_core::net::parse_bind_ip(&spec.bind_ip)
            .ok_or((2004, "invalid bind_ip".to_string()))?;
        if spec.bind_port == 0 {
            port_auto = true;
        } else {
            if !state.proxies.port_allowed(spec.bind_port).await {
                return Err((2005, "PORT_DENIED:端口不在允许范围".into()));
            }
            let proto = spec.kind.as_str();
            let gkey = crate::state::ProxyManager::group_key(spec);
            if state
                .port_in_use_full(proto, bind_ip, spec.bind_port, None, gkey.clone())
                .await
            {
                return Err((2006, "PORT_CONFLICT:端口已被占用".into()));
            }
            // 同组共享监听已在位时无需(也无法)再次试绑
            let group_bound = state.proxies.group_listener_bound(&gkey);
            let trial_ok = group_bound
                || if spec.kind == ProxyKind::Tcp {
                    trial_bind_tcp(bind_ip, spec.bind_port)
                } else {
                    trial_bind_udp(bind_ip, spec.bind_port)
                };
            if !trial_ok {
                let msg = bind_failure_message(
                    state,
                    "端口绑定失败:端口已被占用",
                    proto,
                    bind_ip,
                    spec.bind_port,
                )
                .await;
                return Err((2007, msg));
            }
        }
    } else if matches!(
        spec.kind,
        ProxyKind::Http | ProxyKind::Https | ProxyKind::TcpMux
    ) {
        // vhost 必须有域名或子域
        let has_domain = match spec.kind {
            ProxyKind::Http => spec
                .http_cfg()
                .map(|c| !c.custom_domains.is_empty() || !c.subdomain.is_empty())
                .unwrap_or(false),
            ProxyKind::Https => spec
                .https_cfg()
                .map(|c| !c.custom_domains.is_empty() || !c.subdomain.is_empty())
                .unwrap_or(false),
            ProxyKind::TcpMux => spec
                .tcpmux_cfg()
                .map(|c| !c.custom_domains.is_empty())
                .unwrap_or(false),
            _ => false,
        };
        if !has_domain {
            return Err((
                2008,
                "vhost proxy requires custom_domains or subdomain".into(),
            ));
        }
    }
    // 分配端口
    if port_auto {
        let bind_ip = rh_core::net::parse_bind_ip(&spec.bind_ip)
            .unwrap_or(std::net::IpAddr::from([0, 0, 0, 0]));

        let port = state
            .proxies
            .allocate_port(spec.kind.as_str(), bind_ip)
            .await
            .ok_or((2009, "no free port in auto range".to_string()))?;
        spec.bind_port = port;
    }
    // 落库 + revision(单事务,§4.4 步骤 4)
    let (id, rev) = state
        .db
        .insert_proxy_bump(&client.id, spec, port_auto, "console")
        .await
        .map_err(|e| (2010, format!("{e}")))?;
    spec.id = id as u64;
    // 发布运行态
    crate::runtime::spawn_runtime(state, &client.id, spec, rev).await;
    state
        .events
        .emit("info", "service", &format!("转发规则已创建:{}", spec.name))
        .await;
    Ok(json!({
        "id": id,
        "bind_port": spec.bind_port,
        "port_auto": port_auto,
        "revision": rev,
    }))
}

async fn update_service(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<i64>,
    Json(spec): Json<ProxySpec>,
) -> Response {
    if require_auth(&state, &headers).await.is_err() {
        return unauthorized();
    }
    update_service_inner(&state, id, spec).await
}

/// update 的实现本体(抽出以便单测直接驱动,A5)。
async fn update_service_inner(state: &Arc<AppState>, id: i64, mut spec: ProxySpec) -> Response {
    spec.id = id as u64;
    // 名称校验与创建一致(名字是 visitor 目标解析的稳定键)
    if spec.name.is_empty()
        || !spec
            .name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
        || spec.name.len() > 64
    {
        return err(2001, "名称须为 1-64 位字母/数字/下划线/连字符");
    }
    {
        let l = state.limits.read().unwrap();
        normalize_spec(&mut spec, l.default_pool_size, l.max_pool_size);
    }
    // 实体读取在串行域内进行,避免与并发管理操作竞态
    let Some(client_id) = state
        .db
        .get_proxy(id)
        .await
        .ok()
        .flatten()
        .map(|(_, cid, _, _)| cid)
    else {
        return err(3001, "转发规则不存在");
    };
    let lock = state.serial_lock(&client_id);
    let _g = lock.lock().await;
    let Some((_, client_id, old, port_auto)) = state.db.get_proxy(id).await.ok().flatten() else {
        return err(3001, "转发规则不存在");
    };
    // 密钥只由服务端管理:编辑永不改写(入参忽略),保证"任何写路径后库中不为空";
    // 历史遗留空值在此顺带补齐为随机值
    if let Some(cfg) = spec.secret_cfg_mut() {
        cfg.secret_key = old
            .secret_cfg()
            .filter(|c| !c.secret_key.is_empty())
            .map(|c| c.secret_key.clone())
            .unwrap_or_else(generate_secret);
    }
    let client = state.db.get_client(&client_id).await.ok().flatten();
    let client_enabled = client.as_ref().map(|c| c.enabled).unwrap_or(false);
    // 资格跳步:规则或客户端禁用 → 仅落库。
    //
    // ★但 bind_port=0 的校验必须先做:修前这个早退分支在校验**之前**,
    // 于是"停用状态下 PUT 一个 bind_port=0"能直接落库;之后 enable 时
    // 绑到内核随机分配的临时端口,规则对外表现完全错乱(端口每次可能不同)。
    if spec.kind.uses_bind_port() && spec.bind_port == 0 {
        return err(3006, "bind_port 不能为 0(仅创建时支持自动分配)");
    }
    if !old.enabled || !client_enabled {
        if let Err(e) = state.db.update_proxy(id, &spec, port_auto).await {
            return err(500, &format!("{e}"));
        }
        return ok(json!({"updated": true, "deferred": true}));
    }
    // 更新路径不允许 bind_port=0(随机端口语义仅创建时支持)
    if spec.kind.uses_bind_port() && spec.bind_port == 0 {
        return err(3006, "bind_port 不能为 0(仅创建时支持自动分配)");
    }
    // ★A5:enabled 由 true → false 必须**真的拆运行态**。
    //
    // 修前走下面 `!port_changed` 分支:只落库 enabled=0 + publish_spec_in_place
    // (仅下发 Update 快照),既不 teardown 也不发 ServiceRemove。后果:
    //   - 端口继续监听(用户以为已停用);
    //   - `port_in_use` 只统计 enabled 运行态 → 该端口可被**新规则再次占用**,
    //     形成双占冲突。
    // 对照 disable 端点(api.rs 的 disable_service_inner)有完整 teardown。
    //
    // 这里直接复用 disable_service_inner 的语义(落库 + bump + teardown +
    // ServiceRemove + 事件),避免两条路径再次分叉。
    if old.enabled && !spec.enabled {
        // 先把非 enabled 字段落库(用户同时改了 target 等),再走停用。
        if let Err(e) = state.db.update_proxy(id, &spec, port_auto).await {
            return err(500, &format!("{e}"));
        }
        let (code, msg) = disable_service_locked(state, id).await;
        if code != 0 {
            return err(code, &msg);
        }
        let rev = state
            .db
            .get_client(&client_id)
            .await
            .ok()
            .flatten()
            .map(|c| c.config_revision.max(0) as u64)
            .unwrap_or(0);
        return ok(json!({"updated": true, "disabled": true, "revision": rev}));
    }
    // 同理:false → true 走 enable 的完整路径(重建运行态 + 下发)
    if !old.enabled && spec.enabled {
        if let Err(e) = state.db.update_proxy(id, &spec, port_auto).await {
            return err(500, &format!("{e}"));
        }
        let (code, msg) = enable_service_locked(state, id).await;
        if code != 0 {
            return err(code, &msg);
        }
        let rev = state
            .db
            .get_client(&client_id)
            .await
            .ok()
            .flatten()
            .map(|c| c.config_revision.max(0) as u64)
            .unwrap_or(0);
        return ok(json!({"updated": true, "enabled": true, "revision": rev}));
    }
    // 非端口字段变更(§4.3):listener/池不动,原位发布快照 + 入队
    let port_changed =
        spec.kind != old.kind || spec.bind_port != old.bind_port || spec.bind_ip != old.bind_ip;
    if !port_changed {
        if let Some(rt) = state.proxies.get(id as u64) {
            let rev = match state
                .db
                .update_proxy_bump(id, &client_id, &spec, port_auto)
                .await
            {
                Ok(r) => r,
                Err(e) => return err(500, &format!("{e}")),
            };
            crate::runtime::publish_spec_in_place(state, &rt, &spec, rev).await;
            return ok(json!({"updated": true, "revision": rev}));
        }
    }
    // bind 变更:重叠地址拒绝 + 试绑定(tcp 与 udp 均试;失败不动 DB 与旧运行态)
    if spec.kind.uses_bind_port() {
        let new_ip = rh_core::net::parse_bind_ip(&spec.bind_ip)
            .unwrap_or(std::net::IpAddr::from([0, 0, 0, 0]));
        let old_ip = rh_core::net::parse_bind_ip(&old.bind_ip)
            .unwrap_or(std::net::IpAddr::from([0, 0, 0, 0]));
        if rh_core::net::bind_conflicts(
            spec.kind.as_str(),
            new_ip,
            spec.bind_port,
            old.kind.as_str(),
            old_ip,
            old.bind_port,
        ) && (new_ip != old_ip || spec.bind_port == old.bind_port)
        {
            return err(3002, "OVERLAP_ADDR_UPDATE:绑定地址重叠,不支持直接修改");
        }
        if !state.proxies.port_allowed(spec.bind_port).await {
            return err(3003, "PORT_DENIED:端口不在允许范围");
        }
        let gkey = crate::state::ProxyManager::group_key(&spec);
        if state
            .port_in_use_full(
                spec.kind.as_str(),
                new_ip,
                spec.bind_port,
                Some(id as u64),
                gkey.clone(),
            )
            .await
        {
            return err(3004, "PORT_CONFLICT:端口已被占用");
        }
        let group_bound = state.proxies.group_listener_bound(&gkey);
        let trial_ok = group_bound
            || if spec.kind == ProxyKind::Tcp {
                trial_bind_tcp(new_ip, spec.bind_port)
            } else {
                trial_bind_udp(new_ip, spec.bind_port)
            };
        if !trial_ok {
            let msg = bind_failure_message(
                state,
                "端口绑定失败",
                spec.kind.as_str(),
                new_ip,
                spec.bind_port,
            )
            .await;
            return err(3005, &msg);
        }
    }
    let rev = match state
        .db
        .update_proxy_bump(id, &client_id, &spec, port_auto)
        .await
    {
        Ok(r) => r,
        Err(e) => return err(500, &format!("{e}")),
    };
    // 端口/类型变更:重建运行态(在途连接由 rule_cancel 级联关闭)
    crate::runtime::teardown_runtime(state, id as u64).await;
    crate::runtime::spawn_runtime(state, &client_id, &spec, rev).await;
    ok(json!({"updated": true, "revision": rev}))
}

async fn enable_service(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<i64>,
) -> Response {
    if require_auth(&state, &headers).await.is_err() {
        return unauthorized();
    }
    let Some(client_id) = state
        .db
        .get_proxy(id)
        .await
        .ok()
        .flatten()
        .map(|(_, cid, _, _)| cid)
    else {
        return err(3001, "转发规则不存在");
    };
    let lock = state.serial_lock(&client_id);
    let _g = lock.lock().await;
    let Some((_, client_id, mut spec, port_auto)) = state.db.get_proxy(id).await.ok().flatten()
    else {
        return err(3001, "转发规则不存在");
    };
    let client_enabled = state
        .db
        .get_client(&client_id)
        .await
        .ok()
        .flatten()
        .map(|c| c.enabled)
        .unwrap_or(false);
    // 客户端禁用:仅置 enabled=1 落库(不推进 revision、不 bind,等客户端恢复)
    if !client_enabled {
        state.db.set_proxy_enabled(id, true).await.ok();
        spec.enabled = true;
        let cur_rev = state
            .db
            .get_client(&client_id)
            .await
            .ok()
            .flatten()
            .map(|c| c.config_revision.max(0) as u64)
            .unwrap_or(0);
        crate::runtime::teardown_runtime(&state, id as u64).await;
        crate::runtime::spawn_runtime(&state, &client_id, &spec, cur_rev).await;
        return ok(json!({"enabled": true, "deferred": true}));
    }
    state.db.set_proxy_enabled(id, true).await.ok();
    spec.enabled = true;
    // 资格校验 + bind + 入队(同创建);失败回滚 enabled
    match create_bind_checked(&state, &client_id, &mut spec, port_auto, id).await {
        Ok(()) => {}
        Err((code, msg)) => {
            state.db.set_proxy_enabled(id, false).await.ok();
            return err(code, &msg);
        }
    }
    let rev = state.db.bump_revision(&client_id).await.unwrap_or(0);
    state.bump_cfg_gen(&client_id);
    crate::runtime::teardown_runtime(&state, id as u64).await;
    crate::runtime::spawn_runtime(&state, &client_id, &spec, rev).await;
    ok(json!({"enabled": true, "revision": rev}))
}

/// 轮换密钥访问规则的凭据:生成新随机值落库并推进 revision。
/// 访问器侧不存密钥副本(连接时继承规则当前密钥),因此"两边一起轮换"天然成立,
/// 在线客户端的原有中继不中断。
async fn rotate_service_secret(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<i64>,
) -> Response {
    if require_auth(&state, &headers).await.is_err() {
        return unauthorized();
    }
    // 首读仅定位属主与资格(串行域);实体在锁内重读,与 update 同口径消解并发竞态
    let Some((_, client_id, first, _)) = state.db.get_proxy(id).await.ok().flatten() else {
        return err(3001, "转发规则不存在");
    };
    if first.secret_cfg().is_none() {
        return err(4004, "仅密钥访问规则(STCP/SUDP/XTCP)支持轮换");
    }
    let lock = state.serial_lock(&client_id);
    let _g = lock.lock().await;
    let Some((_, client_id, mut spec, port_auto)) = state.db.get_proxy(id).await.ok().flatten()
    else {
        return err(3001, "转发规则不存在");
    };
    let Some(cfg) = spec.secret_cfg_mut() else {
        return err(4004, "仅密钥访问规则(STCP/SUDP/XTCP)支持轮换");
    };
    cfg.secret_key = generate_secret();
    let secret = cfg.secret_key.clone();
    let rev = match state
        .db
        .update_proxy_bump(id, &client_id, &spec, port_auto)
        .await
    {
        Ok(r) => r,
        Err(e) => return err(500, &format!("{e}")),
    };
    // 密钥不参与 listener/池:原位发布快照保持运行态与库一致即可
    if let Some(rt) = state.proxies.get(id as u64) {
        crate::runtime::publish_spec_in_place(&state, &rt, &spec, rev).await;
    }
    ok(json!({"rotated": true, "secret": secret, "revision": rev}))
}

/// 密钥访问规则的服务端凭据:32 hex(OsRng UUID v4)。
/// 创建与轮换时生成;密钥完全服务端管理——不为空、不由人编、调用方入参一律忽略。
fn generate_secret() -> String {
    uuid::Uuid::new_v4().simple().to_string()
}

/// enable 时的绑定资格检查(端口冲突/试绑)。
/// 试绑 TCP:macOS 上通配 bind 可与既有具体地址监听共存(Linux 拒绝);
/// 通配时补一次环回探针,使两平台判定一致。
fn trial_bind_tcp(ip: std::net::IpAddr, port: u16) -> bool {
    if !std::net::TcpListener::bind(std::net::SocketAddr::new(ip, port)).is_ok() {
        return false;
    }
    if ip.is_unspecified() {
        return std::net::TcpListener::bind(std::net::SocketAddr::new(
            std::net::IpAddr::from([127, 0, 0, 1]),
            port,
        ))
        .is_ok();
    }
    true
}

/// 试绑 UDP(同上)。
fn trial_bind_udp(ip: std::net::IpAddr, port: u16) -> bool {
    if !std::net::UdpSocket::bind(std::net::SocketAddr::new(ip, port)).is_ok() {
        return false;
    }
    if ip.is_unspecified() {
        return std::net::UdpSocket::bind(std::net::SocketAddr::new(
            std::net::IpAddr::from([127, 0, 0, 1]),
            port,
        ))
        .is_ok();
    }
    true
}

/// 2007/3005 消息细化:命中本服务自身配置/在用的监听 → 附加来源(名称/地址)提示;
/// 未命中 → 原样返回通用文案(外部进程占用口径),数字码与 JSON 形状不变。
/// 静态配置来源只声明"配置保留",不冒充实时进程检测;OS 试绑仍是最终裁定。
async fn bind_failure_message(
    state: &Arc<AppState>,
    base: &str,
    proto: &str,
    ip: std::net::IpAddr,
    port: u16,
) -> String {
    match state.classify_port_occupation(proto, ip, port).await {
        Some(hit) => {
            let how = if hit.live {
                "正在监听"
            } else {
                "配置监听"
            };
            format!(
                "{base};占用方:本服务自身 {}({} {})",
                hit.label, how, hit.addr
            )
        }
        None => base.to_string(),
    }
}

async fn create_bind_checked(
    state: &Arc<AppState>,
    _client_id: &str,
    spec: &mut ProxySpec,
    _port_auto: bool,
    id: i64,
) -> Result<(), (i32, String)> {
    if !spec.kind.uses_bind_port() {
        return Ok(());
    }
    let bind_ip =
        rh_core::net::parse_bind_ip(&spec.bind_ip).ok_or((2004, "invalid bind_ip".to_string()))?;
    if !state.proxies.port_allowed(spec.bind_port).await {
        return Err((2005, "PORT_DENIED:端口不在允许范围".into()));
    }
    let gkey = crate::state::ProxyManager::group_key(spec);
    if state
        .port_in_use_full(
            spec.kind.as_str(),
            bind_ip,
            spec.bind_port,
            Some(id as u64),
            gkey.clone(),
        )
        .await
    {
        return Err((2006, "PORT_CONFLICT:端口已被占用".into()));
    }
    let group_bound = state.proxies.group_listener_bound(&gkey);
    let trial_ok = group_bound
        || if spec.kind == ProxyKind::Tcp {
            trial_bind_tcp(bind_ip, spec.bind_port)
        } else {
            trial_bind_udp(bind_ip, spec.bind_port)
        };
    if !trial_ok {
        let msg = bind_failure_message(
            state,
            "端口绑定失败",
            spec.kind.as_str(),
            bind_ip,
            spec.bind_port,
        )
        .await;
        return Err((2007, msg));
    }
    Ok(())
}

async fn disable_service(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<i64>,
) -> Response {
    if require_auth(&state, &headers).await.is_err() {
        return unauthorized();
    }
    let Some(client_id) = state
        .db
        .get_proxy(id)
        .await
        .ok()
        .flatten()
        .map(|(_, cid, _, _)| cid)
    else {
        return err(3001, "转发规则不存在");
    };
    let lock = state.serial_lock(&client_id);
    let _g = lock.lock().await;
    let Some((_, client_id, spec, _)) = state.db.get_proxy(id).await.ok().flatten() else {
        return err(3001, "转发规则不存在");
    };
    state.db.set_proxy_enabled(id, false).await.ok();
    let rev = state.db.bump_revision(&client_id).await.unwrap_or(0);
    state.bump_cfg_gen(&client_id);
    crate::runtime::teardown_runtime(&state, id as u64).await;
    if let Some(sess) = state.sessions.get(&client_id) {
        use rh_protocol::message::{ServerMessage, ServiceRemove};
        let _ = sess
            .send(ServerMessage::ServiceRemove(ServiceRemove {
                revision: rev,
                entity_type: "proxy".into(),
                entity_id: id as u64,
            }))
            .await;
    }
    state
        .events
        .emit("info", "service", &format!("转发规则已停用:{}", spec.name))
        .await;
    ok(json!({"disabled": true, "revision": rev}))
}

async fn delete_service(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<i64>,
) -> Response {
    if require_auth(&state, &headers).await.is_err() {
        return unauthorized();
    }
    let Some(client_id) = state
        .db
        .get_proxy(id)
        .await
        .ok()
        .flatten()
        .map(|(_, cid, _, _)| cid)
    else {
        return err(3001, "转发规则不存在");
    };
    let lock = state.serial_lock(&client_id);
    let _g = lock.lock().await;
    let Some((_, client_id, spec, _)) = state.db.get_proxy(id).await.ok().flatten() else {
        return err(3001, "转发规则不存在");
    };
    let rev = state.db.bump_revision(&client_id).await.unwrap_or(0);
    state.bump_cfg_gen(&client_id);
    crate::runtime::teardown_runtime(&state, id as u64).await;
    if let Err(e) = state.db.delete_proxy(id).await {
        return err(500, &format!("{e}"));
    }
    if let Some(sess) = state.sessions.get(&client_id) {
        use rh_protocol::message::{ServerMessage, ServiceRemove};
        let _ = sess
            .send(ServerMessage::ServiceRemove(ServiceRemove {
                revision: rev,
                entity_type: "proxy".into(),
                entity_id: id as u64,
            }))
            .await;
    }
    state
        .events
        .emit("warn", "service", &format!("转发规则已删除:{}", spec.name))
        .await;
    ok(json!({"deleted": true}))
}

#[derive(Deserialize)]
struct BatchReq {
    ids: Vec<i64>,
    action: String, // enable | disable | delete
}

async fn batch_services(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(req): Json<BatchReq>,
) -> Response {
    if require_auth(&state, &headers).await.is_err() {
        return unauthorized();
    }
    let mut results = Vec::new();
    for id in req.ids {
        let r = match req.action.as_str() {
            // 内部直调 inner(本端点已通过 Bearer 校验)
            "enable" => enable_service_inner(&state, id).await,
            "disable" => disable_service_inner(&state, id).await,
            "delete" => delete_service_inner(&state, id).await,
            _ => (9000, format!("未知操作:{}", req.action)),
        };
        results.push(json!({"id": id, "code": r.0, "message": r.1}));
    }
    ok(results)
}

async fn enable_service_inner(state: &Arc<AppState>, id: i64) -> (i32, String) {
    let Some(client_id) = state
        .db
        .get_proxy(id)
        .await
        .ok()
        .flatten()
        .map(|(_, cid, _, _)| cid)
    else {
        return (3001, "not found".into());
    };
    let lock = state.serial_lock(&client_id);
    let _g = lock.lock().await;
    enable_service_locked(state, id).await
}

/// 启用的实现本体,**要求调用方已持有 serial_lock**(理由同
/// [`disable_service_locked`])。
async fn enable_service_locked(state: &Arc<AppState>, id: i64) -> (i32, String) {
    let Some((_, client_id, mut spec, port_auto)) = state.db.get_proxy(id).await.ok().flatten()
    else {
        return (3001, "not found".into());
    };
    state.db.set_proxy_enabled(id, true).await.ok();
    spec.enabled = true;
    let rev = state.db.bump_revision(&client_id).await.unwrap_or(0);
    state.bump_cfg_gen(&client_id);
    let client_enabled = state
        .db
        .get_client(&client_id)
        .await
        .ok()
        .flatten()
        .map(|c| c.enabled)
        .unwrap_or(false);
    if !client_enabled {
        state
            .events
            .emit(
                "info",
                "service",
                &format!("转发规则已启用(等待客户端恢复):{}", spec.name),
            )
            .await;
        return (0, "ok".into());
    }
    // 绑定资格失败:回滚 enabled,保持 DB 与运行态一致(与单个 enable 同语义)
    if let Err((code, msg)) = create_bind_checked(state, &client_id, &mut spec, port_auto, id).await
    {
        state.db.set_proxy_enabled(id, false).await.ok();
        return (code, msg);
    }
    crate::runtime::teardown_runtime(state, id as u64).await;
    crate::runtime::spawn_runtime(state, &client_id, &spec, rev).await;
    state
        .events
        .emit("info", "service", &format!("转发规则已启用:{}", spec.name))
        .await;
    (0, "ok".into())
}

async fn disable_service_inner(state: &Arc<AppState>, id: i64) -> (i32, String) {
    let Some((_, client_id, _, _)) = state.db.get_proxy(id).await.ok().flatten() else {
        return (3001, "not found".into());
    };
    let lock = state.serial_lock(&client_id);
    let _g = lock.lock().await;
    disable_service_locked(state, id).await
}

/// 停用的实现本体,**要求调用方已持有该客户端的 serial_lock**。
///
/// ★为什么必须拆出这一层(A5):`update_service_inner` 自己已经持锁,
/// 若它直接调 [`disable_service_inner`] 会在同一把 `tokio::sync::Mutex`
/// 上二次加锁 —— 直接死锁(不是 RwLock 的重入读,是写锁自锁)。
async fn disable_service_locked(state: &Arc<AppState>, id: i64) -> (i32, String) {
    let Some((_, client_id, spec, _)) = state.db.get_proxy(id).await.ok().flatten() else {
        return (3001, "not found".into());
    };
    state.db.set_proxy_enabled(id, false).await.ok();
    let rev = state.db.bump_revision(&client_id).await.unwrap_or(0);
    state.bump_cfg_gen(&client_id);
    crate::runtime::teardown_runtime(state, id as u64).await;
    if let Some(sess) = state.sessions.get(&client_id) {
        use rh_protocol::message::{ServerMessage, ServiceRemove};
        let _ = sess
            .send(ServerMessage::ServiceRemove(ServiceRemove {
                revision: rev,
                entity_type: "proxy".into(),
                entity_id: id as u64,
            }))
            .await;
    }
    state
        .events
        .emit("info", "service", &format!("转发规则已停用:{}", spec.name))
        .await;
    (0, "ok".into())
}

async fn delete_service_inner(state: &Arc<AppState>, id: i64) -> (i32, String) {
    let Some((_, client_id, spec, _)) = state.db.get_proxy(id).await.ok().flatten() else {
        return (3001, "not found".into());
    };
    let lock = state.serial_lock(&client_id);
    let _g = lock.lock().await;
    let rev = state.db.bump_revision(&client_id).await.unwrap_or(0);
    state.bump_cfg_gen(&client_id);
    crate::runtime::teardown_runtime(state, id as u64).await;
    let _ = state.db.delete_proxy(id).await;
    if let Some(sess) = state.sessions.get(&client_id) {
        use rh_protocol::message::{ServerMessage, ServiceRemove};
        let _ = sess
            .send(ServerMessage::ServiceRemove(ServiceRemove {
                revision: rev,
                entity_type: "proxy".into(),
                entity_id: id as u64,
            }))
            .await;
    }
    state
        .events
        .emit("warn", "service", &format!("转发规则已删除:{}", spec.name))
        .await;
    (0, "ok".into())
}

// ---------------- visitors ----------------

async fn list_visitors(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(q): Query<ListQuery>,
) -> Response {
    if require_auth(&state, &headers).await.is_err() {
        return unauthorized();
    }
    let rows = match state.db.list_visitors(q.client_id.as_deref()).await {
        Ok(r) => r,
        Err(e) => return err(500, &format!("{e}")),
    };
    let out: Vec<_> = rows
        .into_iter()
        .map(|(id, client_id, target_client, spec)| {
            json!({
                "id": id,
                "client_id": client_id,
                "target_client_id": target_client,
                "name": spec.name,
                "kind": spec.kind.as_str(),
                "enabled": spec.enabled,
                "bind_addr": spec.bind_addr,
                "bind_port": spec.bind_port,
                "target_proxy_name": spec.target_proxy_name,
                "spec": spec,
            })
        })
        .collect();
    ok(out)
}

#[derive(Deserialize)]
struct CreateVisitorReq {
    client_id: String,
    #[serde(flatten)]
    spec: VisitorSpec,
}

/// B15:解析 visitor 的目标代理归属(创建与更新共用)。
///
/// 同名规则可能存在于多个客户端:优先当前在线的客户端;全部离线时取
/// 创建序首个,歧义时告警。修前 update 取"首个匹配"(不看在不在线),
/// create 优先在线 —— 同名一在线一离线时,update 会把归属改到离线端。
async fn resolve_visitor_target(state: &Arc<AppState>, spec: &VisitorSpec) -> Option<String> {
    let clients = state.db.list_clients().await.unwrap_or_default();
    let mut candidates: Vec<String> = Vec::new();
    for c in &clients {
        if let Ok(list) = state.db.list_proxies(Some(&c.id)).await {
            if list.iter().any(|(_, _, p)| {
                p.name == spec.target_proxy_name && p.kind.as_str() == spec.kind.as_str()
            }) {
                candidates.push(c.id.clone());
            }
        }
    }
    if candidates.len() > 1 {
        tracing::warn!(
            proxy = %spec.target_proxy_name,
            clients = ?candidates,
            "visitor target proxy name ambiguous; preferring online client"
        );
    }
    candidates
        .iter()
        .find(|c| state.sessions.get(c).is_some())
        .or_else(|| candidates.first())
        .cloned()
}

async fn create_visitor(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(req): Json<CreateVisitorReq>,
) -> Response {
    if require_auth(&state, &headers).await.is_err() {
        return unauthorized();
    }
    let mut spec = req.spec;
    if spec.name.is_empty() || spec.bind_port == 0 {
        return err(4001, "缺少 name 或 bind_port");
    }
    spec.id = 0;
    // 密钥由目标规则单一管理:访问器一律不存储(连接时继承规则当前密钥),入参忽略
    spec.secret_key = String::new();
    let target_client = resolve_visitor_target(&state, &spec).await;
    let target_proxy_found = target_client.is_some();
    if !target_proxy_found {
        return err(4002, "目标规则不存在(类型须匹配)");
    }
    let lock = state.serial_lock(&req.client_id);
    let _g = lock.lock().await;
    let id = match state
        .db
        .insert_visitor(&req.client_id, &target_client.unwrap(), &spec)
        .await
    {
        Ok(id) => id,
        Err(e) => return err(500, &format!("{e}")),
    };
    let rev = state.db.bump_revision(&req.client_id).await.unwrap_or(0);
    state.bump_cfg_gen(&req.client_id);
    spec.id = id as u64;
    // 下发
    if let Some(sess) = state.sessions.get(&req.client_id) {
        use rh_protocol::message::{ServerMessage, ServiceUpdate, SyncEntity};
        let _ = sess
            .send(ServerMessage::ServiceUpdate(Box::new(ServiceUpdate {
                revision: rev,
                entity: SyncEntity::Visitor(spec.clone()),
            })))
            .await;
    }
    ok(json!({"id": id, "revision": rev}))
}

async fn update_visitor(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<i64>,
    Json(mut spec): Json<VisitorSpec>,
) -> Response {
    if require_auth(&state, &headers).await.is_err() {
        return unauthorized();
    }
    // 首读仅为定位属主(串行域);实体在锁内重读,消解与并发管理操作的竞态
    let Some((_, client_id, _, _)) = state
        .db
        .list_visitors(None)
        .await
        .unwrap_or_default()
        .into_iter()
        .find(|(vid, ..)| *vid == id)
    else {
        return err(4003, "访问器不存在");
    };
    let lock = state.serial_lock(&client_id);
    let _g = lock.lock().await;
    let Some((_, _, _, _old)) = state
        .db
        .list_visitors(Some(&client_id))
        .await
        .unwrap_or_default()
        .into_iter()
        .find(|(vid, ..)| *vid == id)
    else {
        return err(4003, "访问器不存在");
    };
    spec.id = id as u64;
    // 密钥由目标规则单一管理:访问器一律不存储(连接时继承规则当前密钥),入参忽略
    spec.secret_key = String::new();
    // B15:与创建同一解析器(名称 + 类型都须匹配,优先在线客户端)
    let Some(target_client) = resolve_visitor_target(&state, &spec).await else {
        return err(4002, "目标规则不存在(类型须匹配)");
    };
    if let Err(e) = state.db.update_visitor(id, &target_client, &spec).await {
        return err(500, &format!("{e}"));
    }
    let rev = state.db.bump_revision(&client_id).await.unwrap_or(0);
    state.bump_cfg_gen(&client_id);
    if let Some(sess) = state.sessions.get(&client_id) {
        use rh_protocol::message::{ServerMessage, ServiceUpdate, SyncEntity};
        let _ = sess
            .send(ServerMessage::ServiceUpdate(Box::new(ServiceUpdate {
                revision: rev,
                entity: SyncEntity::Visitor(spec.clone()),
            })))
            .await;
    }
    ok(json!({"updated": true}))
}

async fn enable_visitor(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<i64>,
) -> Response {
    if require_auth(&state, &headers).await.is_err() {
        return unauthorized();
    }
    toggle_visitor(state, id, true).await
}

async fn disable_visitor(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<i64>,
) -> Response {
    if require_auth(&state, &headers).await.is_err() {
        return unauthorized();
    }
    toggle_visitor(state, id, false).await
}

async fn toggle_visitor(state: Arc<AppState>, id: i64, enabled: bool) -> Response {
    // 首读定位属主;实体在串行域内重读,防与并发删除竞态
    let Some((_, client_id, _, _)) = state
        .db
        .list_visitors(None)
        .await
        .unwrap_or_default()
        .into_iter()
        .find(|(vid, ..)| *vid == id)
    else {
        return err(4003, "访问器不存在");
    };
    let lock = state.serial_lock(&client_id);
    let _g = lock.lock().await;
    let Some((_, _, _, mut spec)) = state
        .db
        .list_visitors(Some(&client_id))
        .await
        .unwrap_or_default()
        .into_iter()
        .find(|(vid, ..)| *vid == id)
    else {
        return err(4003, "访问器不存在");
    };
    state.db.set_visitor_enabled(id, enabled).await.ok();
    spec.enabled = enabled;
    let rev = state.db.bump_revision(&client_id).await.unwrap_or(0);
    state.bump_cfg_gen(&client_id);
    if let Some(sess) = state.sessions.get(&client_id) {
        use rh_protocol::message::{ServerMessage, ServiceRemove, ServiceUpdate, SyncEntity};
        if enabled {
            let _ = sess
                .send(ServerMessage::ServiceUpdate(Box::new(ServiceUpdate {
                    revision: rev,
                    entity: SyncEntity::Visitor(spec),
                })))
                .await;
        } else {
            let _ = sess
                .send(ServerMessage::ServiceRemove(ServiceRemove {
                    revision: rev,
                    entity_type: "visitor".into(),
                    entity_id: id as u64,
                }))
                .await;
        }
    }
    ok(json!({"enabled": enabled}))
}

async fn delete_visitor(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<i64>,
) -> Response {
    if require_auth(&state, &headers).await.is_err() {
        return unauthorized();
    }
    // 首读定位属主;实体在串行域内重读,防重复删除/迟到操作复活实体
    let Some((_, client_id, _, _)) = state
        .db
        .list_visitors(None)
        .await
        .unwrap_or_default()
        .into_iter()
        .find(|(vid, ..)| *vid == id)
    else {
        return err(4003, "访问器不存在");
    };
    let lock = state.serial_lock(&client_id);
    let _g = lock.lock().await;
    if !state
        .db
        .list_visitors(Some(&client_id))
        .await
        .unwrap_or_default()
        .iter()
        .any(|(vid, ..)| *vid == id)
    {
        return err(4003, "访问器不存在");
    }
    let rev = state.db.bump_revision(&client_id).await.unwrap_or(0);
    state.bump_cfg_gen(&client_id);
    let _ = state.db.delete_visitor(id).await;
    if let Some(sess) = state.sessions.get(&client_id) {
        use rh_protocol::message::{ServerMessage, ServiceRemove};
        let _ = sess
            .send(ServerMessage::ServiceRemove(ServiceRemove {
                revision: rev,
                entity_type: "visitor".into(),
                entity_id: id as u64,
            }))
            .await;
    }
    ok(json!({"deleted": true}))
}

// ---------------- stats / events / metrics ----------------

async fn stats_overview(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    if require_auth(&state, &headers).await.is_err() {
        return unauthorized();
    }
    let clients = state.db.list_clients().await.unwrap_or_default();
    let proxies = state.db.list_proxies(None).await.unwrap_or_default();
    let running = state
        .proxies
        .runtimes
        .iter()
        .filter(|e| {
            e.value()
                .state
                .try_read()
                .map(|g| matches!(&*g, ProxyState::Running))
                .unwrap_or(false)
        })
        .count();
    let totals = state.metrics.totals_snapshot();
    ok(json!({
        "clients_total": clients.len(),
        "clients_online": state.sessions.count_online(),
        "services_total": proxies.len(),
        "services_running": running,
        "conns_current": totals.conns_current,
        "bytes_in": totals.bytes_in,
        "bytes_out": totals.bytes_out,
    }))
}

#[derive(Deserialize)]
struct EventsQuery {
    #[serde(default = "default_limit")]
    limit: i64,
    level: Option<String>,
}

fn default_limit() -> i64 {
    100
}

async fn list_events(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(q): Query<EventsQuery>,
) -> Response {
    if require_auth(&state, &headers).await.is_err() {
        return unauthorized();
    }
    // 钉住合法区间:防 limit<=0(SQLite 视为无限制)或超大值拖库
    let limit = q.limit.clamp(1, 500);
    let rows = match state.db.list_events(limit, q.level.as_deref()).await {
        Ok(r) => r,
        Err(e) => return err(500, &format!("{e}")),
    };
    ok(rows)
}

// ---------------- 服务器配置(网页可管子集:端口策略/日志级别/SSH 网关) ----------------

/// GET:完整配置(脱敏)+ 热配置当前值 + authorized_keys 内容。
async fn get_server_config(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    if require_auth(&state, &headers).await.is_err() {
        return unauthorized();
    }
    let mut view = state.cfg.as_ref().clone();
    view.web.jwt_secret = None; // 密钥不回显
    let hot = state.hot.read().await.clone();
    let keys_path = hot.ssh_gateway.authorized_keys_file.clone();
    let authorized_keys = keys_path
        .as_ref()
        .and_then(|p| std::fs::read_to_string(p).ok());
    ok(json!({
        "config": serde_json::to_value(&view).unwrap_or(serde_json::Value::Null),
        "hot": {
            "allow_ports": hot.allow_ports,
            "max_services_per_client": hot.max_services_per_client,
            "log_level": hot.log_level,
            "ssh_gateway": serde_json::to_value(&hot.ssh_gateway).unwrap_or(serde_json::Value::Null),
            "metrics": serde_json::to_value(&hot.metrics).unwrap_or(serde_json::Value::Null),
            "limits": serde_json::to_value(&*state.limits.read().unwrap())
                .unwrap_or(serde_json::Value::Null),
        },
        "authorized_keys": authorized_keys,
        // 重启生效项:ALPN 在监听器构建时固化(TLS acceptor),热改无效。
        "alpn": state.cfg.alpn,
    }))
}

#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct ServerConfigUpdate {
    #[serde(default)]
    ports: Option<PortsUpdate>,
    #[serde(default)]
    log: Option<LogUpdate>,
    #[serde(default)]
    ssh_gateway: Option<SshGatewayUpdate>,
    #[serde(default)]
    metrics: Option<MetricsUpdate>,
    #[serde(default)]
    limits: Option<LimitsUpdate>,
    /// authorized_keys 文件全文(空串 = 清空;null = 不动)。
    #[serde(default)]
    authorized_keys: Option<String>,
    /// TLS 相关(重启生效)。
    #[serde(default)]
    tls: Option<TlsUpdate>,
}

/// [tls] 段:目前只有 ALPN。改动需重启(监听器构建时固化)。
#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct TlsUpdate {
    /// ALPN 标识列表;空数组 = 用默认值(`h2`)。
    alpn: Option<Vec<String>>,
}

/// [limits] 热改子集:全部字段可选,0 = 不限(max_pool_size 除外,见字段)。
#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct LimitsUpdate {
    max_mux_streams: Option<usize>,
    max_data_conns_per_client: Option<usize>,
    max_udp_sessions_per_proxy: Option<usize>,
    max_pool_size: Option<u8>,
    default_pool_size: Option<u8>,
    control_send_queue: Option<usize>,
    unauth_conns_per_ip: Option<usize>,
    udp_packet_size: Option<usize>,
    pool_wait_timeout_ms: Option<u64>,
}

#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct PortsUpdate {
    allow_ports: Option<Vec<String>>,
    max_services_per_client: Option<usize>,
}

#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct LogUpdate {
    level: Option<String>,
}

#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct SshGatewayUpdate {
    enabled: Option<bool>,
    bind_port: Option<u16>,
    bind_addr: Option<String>,
}

#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct MetricsUpdate {
    enable_prometheus: Option<bool>,
    bind_addr: Option<String>,
    port: Option<u16>,
}

/// PUT:仅接受热生效子集;应用后回写 toml(原文件备份 .bak 一次,注释会丢失)。
async fn put_server_config(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(req): Json<ServerConfigUpdate>,
) -> Response {
    if require_auth(&state, &headers).await.is_err() {
        return unauthorized();
    }
    // 校验
    if let Some(p) = &req.ports {
        if let Some(list) = &p.allow_ports {
            for item in list {
                if rh_core::net::PortsRange::parse(item).is_none() {
                    return err(1002, &format!("端口范围非法:{item}(示例 10000-40000)"));
                }
            }
        }
        if let Some(n) = p.max_services_per_client {
            if n > 10_000 {
                return err(1002, "max_services_per_client 须为 0-10000");
            }
        }
    }
    if let Some(l) = &req.log {
        if let Some(lv) = &l.level {
            if !matches!(lv.as_str(), "trace" | "debug" | "info" | "warn" | "error") {
                return err(1002, "日志级别须为 trace/debug/info/warn/error");
            }
        }
    }
    if let Some(g) = &req.ssh_gateway {
        if let Some(a) = &g.bind_addr {
            if !a.is_empty() && a.parse::<std::net::IpAddr>().is_err() {
                return err(1002, "bind_addr 须为合法 IP 字面量");
            }
        }
        if let Some(p) = g.bind_port {
            if p == 0 {
                return err(1002, "bind_port 不能为 0");
            }
        }
    }
    if let Some(m) = &req.metrics {
        if let Some(a) = &m.bind_addr {
            if a.trim().parse::<std::net::IpAddr>().is_err() {
                return err(
                    1002,
                    "metrics.bind_addr 须为合法 IP(如 127.0.0.1 / 0.0.0.0)",
                );
            }
        }
        if m.port == Some(0) {
            return err(1002, "metrics.port 须为 1-65535");
        }
    }
    if let Some(keys) = &req.authorized_keys {
        for (i, line) in keys.lines().enumerate() {
            let t = line.trim();
            if t.is_empty() || t.starts_with('#') {
                continue;
            }
            // 形态粗校验(key-type base64 comment);完整解析在网关重启载入时做
            let mut parts = t.split_whitespace();
            let ktype = parts.next().unwrap_or("");
            if !ktype.starts_with("ssh-")
                && !ktype.starts_with("ecdsa-")
                && !ktype.starts_with("sk-")
            {
                return err(1002, &format!("authorized_keys 第 {} 行不是公钥", i + 1));
            }
        }
    }

    if let Some(t) = &req.tls {
        if let Some(list) = &t.alpn {
            if list.len() > 8 {
                return err(1002, "alpn 最多 8 项");
            }
            for item in list {
                // RFC 7301:ALPN 标识是 1-255 字节的 opaque 字节串;
                // 实际使用中都为可打印 ASCII,这里按可见 ASCII 收紧校验,
                // 避免控制字符进入配置文件后无法编辑。
                if item.is_empty() || item.len() > 255 {
                    return err(1002, "alpn 每项须为 1-255 字节");
                }
                if !item.bytes().all(|b| (0x21..=0x7e).contains(&b)) {
                    return err(1002, &format!("alpn 含非法字符(须为可见 ASCII):{item}"));
                }
            }
        }
    }
    if let Some(l) = &req.limits {
        if let Some(n) = l.max_mux_streams {
            if n > rh_transport::MUX_STREAMS_UNLIMITED {
                return err(
                    1002,
                    &format!(
                        "max_mux_streams 须为 0 或 ≤ {}",
                        rh_transport::MUX_STREAMS_UNLIMITED
                    ),
                );
            }
        }
        if let Some(n) = l.control_send_queue {
            if n < 16 {
                return err(1002, "control_send_queue 须 ≥ 16(过小会让协议帧排队抖动)");
            }
        }
        if let Some(n) = l.udp_packet_size {
            if n != 0 && !(1500..=65536).contains(&n) {
                return err(1002, "udp_packet_size 须为 0(=用默认)或 1500-65536");
            }
        }
        if let Some(n) = l.pool_wait_timeout_ms {
            if !(100..=600_000).contains(&n) {
                return err(1002, "pool_wait_timeout_ms 须为 100-600000");
            }
        }
    }

    let mut applied: Vec<String> = Vec::new();
    let mut ssh_changed = false;
    {
        let mut hot = state.hot.write().await;
        if let Some(p) = req.ports {
            if let Some(list) = p.allow_ports {
                hot.allow_ports = list;
                applied.push("ports.allow_ports".into());
            }
            if let Some(n) = p.max_services_per_client {
                hot.max_services_per_client = n;
                applied.push("ports.max_services_per_client".into());
            }
        }
        if let Some(l) = req.log {
            if let Some(lv) = l.level {
                hot.log_level = lv.clone();
                match crate::LOG_SET_LEVEL.get() {
                    Some(f) if f(&lv) => applied.push("log.level".into()),
                    _ => return err(500, "日志级别切换失败(不支持热更新)"),
                }
            }
        }
        if let Some(g) = req.ssh_gateway {
            let target = &mut hot.ssh_gateway;
            if let Some(v) = g.enabled {
                if target.enabled != v {
                    ssh_changed = true;
                }
                target.enabled = v;
                applied.push("ssh_gateway.enabled".into());
            }
            if let Some(v) = g.bind_port {
                if target.bind_port != v {
                    ssh_changed = true;
                }
                target.bind_port = v;
                applied.push("ssh_gateway.bind_port".into());
            }
            if let Some(v) = g.bind_addr {
                if target.bind_addr != v {
                    ssh_changed = true;
                }
                target.bind_addr = v;
                applied.push("ssh_gateway.bind_addr".into());
            }
        }
        // metrics:独立监听热启停;地址/端口变更热重启监听器,绑定失败回滚不落配置
        if let Some(m) = req.metrics {
            let old = hot.metrics.clone();
            let mut new = old.clone();
            if let Some(v) = m.enable_prometheus {
                new.enable_prometheus = v;
            }
            if let Some(v) = m.bind_addr.as_deref() {
                new.bind_addr = v.trim().to_string();
            }
            if let Some(v) = m.port {
                new.port = v;
            }
            if new != old {
                let was_running = state.metrics_listening().await;
                if was_running {
                    state.metrics_listen_stop().await;
                }
                hot.metrics = new.clone();
                if new.enable_prometheus {
                    if let Err(e) = state.metrics_listen_start(&new).await {
                        // 回滚:恢复旧值;旧监听原本在跑则尽力按旧地址拉回
                        hot.metrics = old.clone();
                        if was_running {
                            if let Err(e2) = state.metrics_listen_start(&old).await {
                                tracing::error!(error = %e2, "prometheus 回滚重启失败,当前无监听");
                            }
                        }
                        return err(500, &format!("Prometheus 监听启动失败:{e}"));
                    }
                }
                if new.enable_prometheus != old.enable_prometheus {
                    applied.push("metrics.enable_prometheus".into());
                }
                if new.bind_addr != old.bind_addr {
                    applied.push("metrics.bind_addr".into());
                }
                if new.port != old.port {
                    applied.push("metrics.port".into());
                }
            }
        }
        // authorized_keys:写入(或初始化默认)文件路径
        if let Some(keys) = &req.authorized_keys {
            if hot.ssh_gateway.authorized_keys_file.is_none() {
                let dir = state
                    .cfg
                    .config_path
                    .parent()
                    .map(|p| p.to_path_buf())
                    .unwrap_or_default();
                hot.ssh_gateway.authorized_keys_file = Some(dir.join("authorized_keys"));
            }
            let path = hot.ssh_gateway.authorized_keys_file.clone().unwrap();
            if let Err(e) = std::fs::write(&path, keys) {
                return err(
                    500,
                    &format!("authorized_keys 写入失败({}):{e}", path.display()),
                );
            }
            ssh_changed = true;
            applied.push("authorized_keys".into());
        }
    }
    // limits:独立同步锁(enter_unauth 等非 async 路径共用);
    // 生效时机:data_conns/unauth/udp_sessions/池超时/包大小 = 立即,
    // mux 顶与发送队列 = 新会话,池上限 = 新建/更新的规则
    if let Some(l) = req.limits {
        let mut lim = state.limits.write().unwrap();
        let set = |cond: bool, name: &str, applied: &mut Vec<String>| {
            if cond {
                applied.push(format!("limits.{name}"));
            }
        };
        if let Some(v) = l.max_mux_streams {
            set(lim.max_mux_streams != v, "max_mux_streams", &mut applied);
            lim.max_mux_streams = v;
        }
        if let Some(v) = l.max_data_conns_per_client {
            set(
                lim.max_data_conns_per_client != v,
                "max_data_conns_per_client",
                &mut applied,
            );
            lim.max_data_conns_per_client = v;
        }
        if let Some(v) = l.max_udp_sessions_per_proxy {
            set(
                lim.max_udp_sessions_per_proxy != v,
                "max_udp_sessions_per_proxy",
                &mut applied,
            );
            lim.max_udp_sessions_per_proxy = v;
        }
        if let Some(v) = l.max_pool_size {
            set(lim.max_pool_size != v, "max_pool_size", &mut applied);
            lim.max_pool_size = v;
        }
        if let Some(v) = l.default_pool_size {
            set(
                lim.default_pool_size != v,
                "default_pool_size",
                &mut applied,
            );
            lim.default_pool_size = v;
        }
        if let Some(v) = l.control_send_queue {
            set(
                lim.control_send_queue != v,
                "control_send_queue",
                &mut applied,
            );
            lim.control_send_queue = v;
        }
        if let Some(v) = l.unauth_conns_per_ip {
            set(
                lim.unauth_conns_per_ip != v,
                "unauth_conns_per_ip",
                &mut applied,
            );
            lim.unauth_conns_per_ip = v;
        }
        if let Some(v) = l.udp_packet_size {
            set(lim.udp_packet_size != v, "udp_packet_size", &mut applied);
            lim.udp_packet_size = v;
        }
        if let Some(v) = l.pool_wait_timeout_ms {
            set(
                lim.pool_wait_timeout_ms != v,
                "pool_wait_timeout_ms",
                &mut applied,
            );
            lim.pool_wait_timeout_ms = v;
        }
        // mux 顶热值同步给 transport accept 泵(新会话生效)
        state
            .mux_streams
            .store(lim.mux_streams(), std::sync::atomic::Ordering::Relaxed);
    }
    if ssh_changed {
        let _ = state.ssh_watch.0.send(u64::MAX);
    }

    // ALPN:监听器构建时固化,无法热改 —— 写回文件并标记 restart_required。
    let mut restart_required: Vec<String> = Vec::new();
    if let Some(t) = &req.tls {
        if let Some(list) = &t.alpn {
            let norm: Vec<String> = list.iter().map(|s| s.trim().to_string()).collect();
            if norm != state.cfg.alpn {
                restart_required.push("alpn".into());
            }
        }
    }

    // 回写配置文件:热子集合并进启动配置快照;原文件备份一次(.bak),注释会丢失
    let persist_err;
    {
        let hot = state.hot.read().await;
        let mut disk = state.cfg.as_ref().clone();
        if let Some(t) = &req.tls {
            if let Some(list) = &t.alpn {
                disk.alpn = list.iter().map(|s| s.trim().to_string()).collect();
            }
        }
        disk.ports.allow_ports = hot.allow_ports.clone();
        disk.ports.max_services_per_client = hot.max_services_per_client;
        disk.log.level = hot.log_level.clone();
        disk.ssh_gateway = hot.ssh_gateway.clone();
        disk.metrics = hot.metrics.clone();
        disk.limits = state.limits.read().unwrap().clone();
        disk.web.jwt_secret = state.cfg.web.jwt_secret.clone(); // 保留原值(未脱敏源)
        persist_err = match toml::to_string_pretty(&disk) {
            Ok(text) => {
                let path = &state.cfg.config_path;
                let bak = path.with_extension("toml.bak");
                if !bak.exists() {
                    let _ = std::fs::copy(path, &bak);
                }
                std::fs::write(path, text).map_err(|e| e.to_string())
            }
            Err(e) => Err(e.to_string()),
        };
    }
    if let Err(e) = persist_err {
        state
            .events
            .emit("warn", "server", &format!("配置已生效但写回文件失败:{e}"))
            .await;
        return ok(
            json!({ "applied": applied, "restart_required": restart_required, "persisted": false, "error": e }),
        );
    }
    state
        .events
        .emit("info", "server", "服务器配置已更新(热生效并写回文件)")
        .await;
    ok(json!({ "applied": applied, "restart_required": restart_required, "persisted": true }))
}

// ---------------- 证书管理(M3:SNI 服务端终结) ----------------

async fn list_certs_api(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    if require_auth(&state, &headers).await.is_err() {
        return unauthorized();
    }
    let rows = match state.db.list_certs().await {
        Ok(r) => r,
        Err(e) => return err(500, &format!("{e}")),
    };
    // key_pem 任何响应不回显
    let out: Vec<_> = rows
        .into_iter()
        .map(|c| {
            json!({
                "id": c.id,
                "name": c.name,
                "domains": serde_json::from_str::<Vec<String>>(&c.domains).unwrap_or_default(),
                "not_after": if c.not_after > 0 { Some(c.not_after) } else { None },
                "fingerprint": c.fingerprint,
                "created_at": c.created_at,
            })
        })
        .collect();
    ok(out)
}

#[derive(Deserialize)]
struct CertReq {
    name: String,
    cert_pem: String,
    key_pem: String,
    #[serde(default)]
    domains: Option<Vec<String>>,
}

async fn create_cert_api(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(req): Json<CertReq>,
) -> Response {
    if require_auth(&state, &headers).await.is_err() {
        return unauthorized();
    }
    let name = req.name.trim();
    if name.is_empty() || name.len() > 64 {
        return err(1002, "名称须为 1-64 字符");
    }
    let (san_domains, fp, not_after, _cfg) =
        match crate::vhost::build_cert_entry(&req.cert_pem, &req.key_pem) {
            Ok(v) => v,
            Err(e) => return err(5002, &format!("证书解析失败:{e}")),
        };
    let domains = req.domains.filter(|d| !d.is_empty()).unwrap_or(san_domains);
    let doms_json = serde_json::to_string(&domains).unwrap_or_else(|_| "[]".into());
    let id = match state
        .db
        .insert_cert(&crate::db::CertUpdate {
            name: name.to_string(),
            domains: doms_json,
            cert_pem: req.cert_pem.clone(),
            key_pem: req.key_pem.clone(),
            fingerprint: fp.clone(),
            not_after,
        })
        .await
    {
        Ok(id) => id,
        Err(e) => return err(500, &format!("{e}")),
    };
    state.rebuild_certs().await;
    state
        .events
        .emit("info", "cert", &format!("证书已上传:{name}"))
        .await;
    ok(json!({ "id": id, "fingerprint": fp }))
}

async fn update_cert_api(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<i64>,
    Json(req): Json<CertReq>,
) -> Response {
    if require_auth(&state, &headers).await.is_err() {
        return unauthorized();
    }
    let name = req.name.trim();
    if name.is_empty() || name.len() > 64 {
        return err(1002, "名称须为 1-64 字符");
    }
    let (san_domains, fp, not_after, _cfg) =
        match crate::vhost::build_cert_entry(&req.cert_pem, &req.key_pem) {
            Ok(v) => v,
            Err(e) => return err(5002, &format!("证书解析失败:{e}")),
        };
    let domains = req.domains.filter(|d| !d.is_empty()).unwrap_or(san_domains);
    let doms_json = serde_json::to_string(&domains).unwrap_or_else(|_| "[]".into());
    let rows = state.db.list_certs().await.unwrap_or_default();
    if !rows.iter().any(|c| c.id == id) {
        return err(5001, "证书不存在");
    }
    if let Err(e) = state
        .db
        .update_cert(
            id,
            &crate::db::CertUpdate {
                name: name.to_string(),
                domains: doms_json,
                cert_pem: req.cert_pem.clone(),
                key_pem: req.key_pem.clone(),
                fingerprint: fp,
                not_after,
            },
        )
        .await
    {
        return err(500, &format!("{e}"));
    }
    state.rebuild_certs().await;
    state
        .events
        .emit("info", "cert", &format!("证书已更新:{name}"))
        .await;
    ok(json!({"updated": true}))
}

async fn delete_cert_api(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<i64>,
) -> Response {
    if require_auth(&state, &headers).await.is_err() {
        return unauthorized();
    }
    if let Err(e) = state.db.delete_cert(id).await {
        return err(500, &format!("{e}"));
    }
    state.rebuild_certs().await;
    state.events.emit("warn", "cert", "证书已删除").await;
    ok(json!({"deleted": true}))
}

/// Prometheus 抓取端点:独立端口(metrics.bind_addr:port)上的无鉴权输出。
/// 暴露面由监听地址控制(默认仅 127.0.0.1,靠网络边界保护),
/// 不与管理面共口;启停由 AppState::metrics_listen_start/stop 热开关。
pub(crate) async fn prometheus_metrics(State(state): State<Arc<AppState>>) -> Response {
    use prometheus::TextEncoder;
    let encoder = TextEncoder::new();
    let mut buffer = Vec::new();
    if let Ok(mfs) = encoder.encode_to_string(&state.metrics.registry.gather()) {
        buffer.extend_from_slice(mfs.as_bytes());
    }
    (
        StatusCode::OK,
        [("content-type", "text/plain; version=0.0.4")],
        buffer,
    )
        .into_response()
}

#[cfg(test)]
mod login_rate_key_tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

    /// 限速键**只含 IP,不含端口**。
    ///
    /// 这条修的是 P0:修前键用 `peer`(`WebAddr` 直通 `SocketAddr` 的 Display,
    /// 带临时端口)。攻击者每次新建 TCP 连接就换一个键,两级限速全部落空,
    /// 而每次尝试都要跑 argon2id,构成未认证 DoS + 限速器内存无界增长。
    ///
    /// 断言方式:同一 IP 不同端口必须产生**完全相同**的键。
    #[test]
    fn login_rate_keys_ignores_port() {
        let ip = IpAddr::V4(Ipv4Addr::new(203, 0, 113, 7));
        // 同一 IP:无论源端口怎么变,键都必须一致
        let (a_ip, a_user) = login_rate_keys(ip, "admin");
        let (b_ip, b_user) = login_rate_keys(ip, "admin");
        assert_eq!(a_ip, b_ip);
        assert_eq!(a_user, b_user);

        // 键里不得出现冒号(IPv4 带端口的形式);IPv6 本身含冒号,单独测
        assert!(
            !a_ip.contains(':'),
            "IPv4 限速键不得含端口: {a_ip}"
        );
        assert_eq!(a_ip, "ip|203.0.113.7");
        assert_eq!(a_user, "203.0.113.7|admin");
    }

    /// 不同 IP 必须得到不同键(不能把所有人合成一个桶)。
    #[test]
    fn login_rate_keys_separates_ips() {
        let a = login_rate_keys(IpAddr::V4(Ipv4Addr::new(198, 51, 100, 1)), "u");
        let b = login_rate_keys(IpAddr::V4(Ipv4Addr::new(198, 51, 100, 2)), "u");
        assert_ne!(a.0, b.0);
        assert_ne!(a.1, b.1);
    }

    /// 同一 IP 不同用户名:IP 级键相同(共享宽桶),用户级键不同(独立小桶)。
    #[test]
    fn login_rate_keys_splits_by_username_at_second_level() {
        let ip = IpAddr::V4(Ipv4Addr::new(192, 0, 2, 9));
        let (ip_a, user_a) = login_rate_keys(ip, "alice");
        let (ip_b, user_b) = login_rate_keys(ip, "bob");
        assert_eq!(ip_a, ip_b, "IP 级桶必须共享");
        assert_ne!(user_a, user_b, "用户级桶必须分开");
    }

    /// IPv6 也要稳定(不含端口):同一地址的键恒定。
    #[test]
    fn login_rate_keys_stable_for_ipv6() {
        let ip = IpAddr::V6(Ipv6Addr::LOCALHOST);
        let a = login_rate_keys(ip, "root");
        let b = login_rate_keys(ip, "root");
        assert_eq!(a, b);
        assert_eq!(a.0, "ip|::1");
    }
}

#[cfg(test)]
mod tests {
    //! E(2007/3005 自身/外部占用区分)定向回归:独立临时端口 + 内存 DB,
    //! 直接调用 handler 公开路径(create/update/enable/disable)与最小 helper,
    //! 由真实 bind 驱动试绑失败;数字码与 JSON 形状断言保持不变。
    use super::*;
    use crate::config::{MetricsConfig, ServerConfig, SshGatewayConfig};
    use std::net::{IpAddr, TcpListener};

    const CID: &str = "diag-e-client";
    const LO: IpAddr = IpAddr::V4(std::net::Ipv4Addr::LOCALHOST);

    /// 挑一个避开常见固定端口(8080=web 默认/9100=metrics 默认)的空闲端口(探测后释放)。
    fn free_port() -> u16 {
        loop {
            let l = TcpListener::bind(("127.0.0.1", 0)).unwrap();
            let p = l.local_addr().unwrap().port();
            drop(l);
            if p != 8080 && p != 9100 {
                return p;
            }
        }
    }

    /// 真实占住一个 TCP 端口(模拟外部进程/其他持有者)。返回 (端口, 持有的 listener)。
    fn hold_tcp() -> (u16, TcpListener) {
        let l = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        (l.local_addr().unwrap().port(), l)
    }

    async fn diag_state(cfg: ServerConfig) -> Arc<AppState> {
        let db = crate::db::Db::open("sqlite::memory:").await.unwrap();
        db.create_client(CID, "diag-e", &"0".repeat(64), "")
            .await
            .unwrap();
        Arc::new(AppState::new(cfg, db))
    }

    async fn auth_headers(state: &Arc<AppState>) -> HeaderMap {
        let token = mint_jwt(state, "admin", 1, 0).await.unwrap();
        let mut h = HeaderMap::new();
        h.insert(
            "authorization",
            axum::http::HeaderValue::from_str(&format!("Bearer {token}")).unwrap(),
        );
        h
    }

    fn spec(kind: &str, name: &str, port: u16) -> ProxySpec {
        serde_json::from_value(json!({
            "name": name,
            "kind": kind,
            "client_id": CID,
            "bind_ip": "127.0.0.1",
            "bind_port": port,
            "target_host": "127.0.0.1",
            "target_port": 9,
        }))
        .unwrap()
    }

    async fn body_json(resp: Response) -> serde_json::Value {
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    async fn create(state: &Arc<AppState>, s: ProxySpec) -> serde_json::Value {
        let h = auth_headers(state).await;
        body_json(create_service(State(state.clone()), h, Json(s)).await).await
    }

    async fn update(state: &Arc<AppState>, id: i64, s: ProxySpec) -> serde_json::Value {
        let h = auth_headers(state).await;
        body_json(update_service(State(state.clone()), h, Path(id), Json(s)).await).await
    }

    async fn enable(state: &Arc<AppState>, id: i64) -> serde_json::Value {
        let h = auth_headers(state).await;
        body_json(enable_service(State(state.clone()), h, Path(id)).await).await
    }

    async fn disable(state: &Arc<AppState>, id: i64) -> serde_json::Value {
        let h = auth_headers(state).await;
        body_json(disable_service(State(state.clone()), h, Path(id)).await).await
    }

    /// 在指定端口拉起真实 metrics 监听(teardown 后监听释放是异步的,短暂重试)。
    async fn start_metrics(state: &Arc<AppState>, port: u16) {
        for _ in 0..40 {
            if state
                .metrics_listen_start(&MetricsConfig {
                    enable_prometheus: true,
                    bind_addr: "127.0.0.1".into(),
                    port,
                })
                .await
                .is_ok()
            {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
        panic!("metrics listen start failed on 127.0.0.1:{port}");
    }

    #[tokio::test]
    async fn e_2007_names_self_listener() {
        // 格1:metrics —— 本进程真实在跑的监听(热启停记录的地址)
        let m = free_port();
        let state = diag_state(ServerConfig::default()).await;
        start_metrics(&state, m).await;
        let v = create(&state, spec("tcp", "diag-metrics", m)).await;
        assert_eq!(v["code"], 2007, "{v}");
        let msg = v["message"].as_str().unwrap();
        assert_eq!(
            msg,
            format!(
                "端口绑定失败:端口已被占用;占用方:本服务自身 Prometheus 指标监听(正在监听 127.0.0.1:{m})"
            )
        );
        // 停止后记录清除,不再归属 metrics(热改即时反映)
        state.metrics_listen_stop().await;
        assert!(state.classify_port_occupation("tcp", LO, m).await.is_none());

        // 格2:Web 管理后台(静态配置保留口径)
        let (w, hold) = hold_tcp();
        let mut cfg = ServerConfig::default();
        cfg.web.bind_addr = "127.0.0.1".into();
        cfg.web.port = w;
        let state = diag_state(cfg).await;
        let v = create(&state, spec("tcp", "diag-web", w)).await;
        assert_eq!(v["code"], 2007, "{v}");
        let msg = v["message"].as_str().unwrap();
        assert_eq!(
            msg,
            format!(
                "端口绑定失败:端口已被占用;占用方:本服务自身 Web 管理后台(配置监听 127.0.0.1:{w})"
            )
        );
        drop((hold, state));

        // 格3:vhost http(与控制面同 bind_addr;0=关闭不占用,>0 才保留)
        let (h, hold) = hold_tcp();
        let mut cfg = ServerConfig {
            bind_addr: "127.0.0.1".into(),
            ..Default::default()
        };
        cfg.vhost.http_port = h;
        let state = diag_state(cfg).await;
        let v = create(&state, spec("tcp", "diag-vhost", h)).await;
        assert_eq!(v["code"], 2007, "{v}");
        let msg = v["message"].as_str().unwrap();
        assert!(msg.contains("vhost HTTP 共享监听"), "{msg}");
        assert!(msg.contains(&format!("配置监听 127.0.0.1:{h}")), "{msg}");
        drop((hold, state));
    }

    #[tokio::test]
    async fn e_2007_external_occupation_message() {
        let (x, hold) = hold_tcp();
        let state = diag_state(ServerConfig::default()).await;
        // 该端口不在任何自身启用配置中:无自身归属(未启用字段不保留占用)
        assert!(state.classify_port_occupation("tcp", LO, x).await.is_none());
        let v = create(&state, spec("tcp", "diag-ext", x)).await;
        assert_eq!(v["code"], 2007, "{v}");
        // 外部占用:数字码/JSON 形状/通用文案均保持不变
        assert_eq!(v["message"], "端口绑定失败:端口已被占用");
        assert!(v["data"].is_null());
        drop((hold, state));
    }

    #[tokio::test]
    async fn e_udp_on_tcp_held_port_no_false_conflict() {
        let (x, hold) = hold_tcp();
        let mut cfg = ServerConfig::default();
        cfg.web.bind_addr = "127.0.0.1".into();
        cfg.web.port = x;
        let state = diag_state(cfg).await;
        // TCP 视角:自身 web 配置命中
        assert!(state.classify_port_occupation("tcp", LO, x).await.is_some());
        // UDP 视角:协议族独立(TCP/UDP 命名空间),不构成冲突
        assert!(state.classify_port_occupation("udp", LO, x).await.is_none());
        // 真实路径:TCP 被占不影响 UDP 规则创建
        let v = create(&state, spec("udp", "diag-udp", x)).await;
        assert_eq!(v["code"], 0, "{v}");
        drop((hold, state));
    }

    #[tokio::test]
    async fn e_update_3005_same_classification() {
        let state = diag_state(ServerConfig::default()).await;
        let p = free_port();
        let v = create(&state, spec("tcp", "diag-upd", p)).await;
        assert_eq!(v["code"], 0, "{v}");
        let id = v["data"]["id"].as_i64().unwrap();

        // 自身:metrics 真实占住 m → 3005 指名自身来源(与创建同口径)
        let m = free_port();
        start_metrics(&state, m).await;
        let v = update(&state, id, spec("tcp", "diag-upd", m)).await;
        assert_eq!(v["code"], 3005, "{v}");
        let msg = v["message"].as_str().unwrap();
        assert_eq!(
            msg,
            format!("端口绑定失败;占用方:本服务自身 Prometheus 指标监听(正在监听 127.0.0.1:{m})")
        );

        // 外部:不在自身配置的端口 → 通用文案不变
        let (x, hold) = hold_tcp();
        let v = update(&state, id, spec("tcp", "diag-upd", x)).await;
        assert_eq!(v["code"], 3005, "{v}");
        assert_eq!(v["message"], "端口绑定失败");
        drop((hold, state));
    }

    #[tokio::test]
    async fn e_group_shared_no_trial_misreport() {
        let state = diag_state(ServerConfig::default()).await;
        let p = free_port();
        let mut a = spec("tcp", "diag-grp-a", p);
        a.group = Some("g1".into());
        let v = create(&state, a).await;
        assert_eq!(v["code"], 0, "{v}");
        // 同组第二成员:组共享监听已在位,短路试绑,不误报 2007
        let mut b = spec("tcp", "diag-grp-b", p);
        b.group = Some("g1".into());
        let v = create(&state, b).await;
        assert_eq!(v["code"], 0, "{v}");
        // 非同组成员同端口:仍走既有 2006(本次改动不波及)
        let v = create(&state, spec("tcp", "diag-grp-c", p)).await;
        assert_eq!(v["code"], 2006, "{v}");
    }

    #[tokio::test]
    async fn e_auto_allocate_untouched() {
        // 自动分配的 OS 探测跳过语义不被破坏:自身 metrics 实占范围端点,
        // 分配结果落在范围内且避开被占端口
        let m = free_port();
        let x = free_port();
        let (lo, hi) = if m < x { (m, x) } else { (x, m) };
        let mut cfg = ServerConfig::default();
        cfg.ports.auto_range = Some(format!("{lo}-{hi}"));
        let state = diag_state(cfg).await;
        start_metrics(&state, m).await;
        let v = create(&state, spec("tcp", "diag-auto", 0)).await;
        assert_eq!(v["code"], 0, "{v}");
        let got = v["data"]["bind_port"].as_u64().unwrap() as u16;
        assert!(
            got >= lo && got <= hi && got != m,
            "allocated {got} not in [{lo},{hi}] avoiding {m}"
        );
        assert_eq!(v["data"]["port_auto"], true);
    }

    #[tokio::test]
    async fn e_enable_2007_self_classification() {
        let state = diag_state(ServerConfig::default()).await;
        let p = free_port();
        let v = create(&state, spec("tcp", "diag-en", p)).await;
        assert_eq!(v["code"], 0, "{v}");
        let id = v["data"]["id"].as_i64().unwrap();
        let v = disable(&state, id).await;
        assert_eq!(v["code"], 0, "{v}");
        // 停用释放端口后由自身 metrics 占住 → enable(复用 create_bind_checked)
        // 同样指名自身来源
        start_metrics(&state, p).await;
        let v = enable(&state, id).await;
        assert_eq!(v["code"], 2007, "{v}");
        let msg = v["message"].as_str().unwrap();
        assert_eq!(
            msg,
            format!("端口绑定失败;占用方:本服务自身 Prometheus 指标监听(正在监听 127.0.0.1:{p})")
        );
        // enable 失败回滚:规则保持停用
        let row = state.db.get_proxy(id).await.unwrap().unwrap();
        assert!(!row.2.enabled);
    }

    #[tokio::test]
    async fn e_classify_sources_and_no_phantom_reservation() {
        // 默认配置(无控制面/extra/vhost/ssh/metrics)不构成任何自身占用
        let state = diag_state(ServerConfig::default()).await;
        let any = free_port();
        assert!(state
            .classify_port_occupation("tcp", LO, any)
            .await
            .is_none());
        assert!(state
            .classify_port_occupation("udp", LO, any)
            .await
            .is_none());

        // 控制面主监听 kcp(UDP 族)+ extra quic(UDP 族)+ vhost https/tcpmux(TCP 族)
        let k = free_port();
        let q = free_port();
        let hs = free_port();
        let tm = free_port();
        let mut cfg = ServerConfig {
            bind_addr: "127.0.0.1".into(),
            bind_protocol: "kcp".into(),
            bind_port: k,
            extra_listeners: vec![crate::config::ListenerConfig {
                protocol: "quic".into(),
                bind_port: q,
                bind_addr: Some("127.0.0.1".into()),
                multiplex: None,
                tls: None,
            }],
            ..Default::default()
        };
        cfg.vhost.https_port = hs;
        cfg.vhost.tcpmux_port = tm;
        let state = diag_state(cfg).await;

        let hit = state.classify_port_occupation("udp", LO, k).await.unwrap();
        assert!(hit.label.contains("控制面主监听(kcp)"), "{}", hit.label);
        assert_eq!(hit.addr, std::net::SocketAddr::from((LO, k)));
        assert!(!hit.live);
        // 族隔离:TCP 查询不命中 UDP 监听
        assert!(state.classify_port_occupation("tcp", LO, k).await.is_none());

        let hit = state.classify_port_occupation("udp", LO, q).await.unwrap();
        assert!(hit.label.contains("附加传输监听(quic)"), "{}", hit.label);

        let hit = state.classify_port_occupation("tcp", LO, hs).await.unwrap();
        assert!(hit.label.contains("vhost HTTPS"), "{}", hit.label);
        let hit = state.classify_port_occupation("tcp", LO, tm).await.unwrap();
        assert!(hit.label.contains("tcpmux"), "{}", hit.label);

        // 异地址不误判:web 绑 127.0.0.1,查 127.0.0.3 同端口 → 无冲突;
        // 通配查询侧与具体监听冲突(bind_conflicts 通配/具体双向口径)
        let w = free_port();
        let mut cfg2 = ServerConfig::default();
        cfg2.web.bind_addr = "127.0.0.1".into();
        cfg2.web.port = w;
        let state2 = diag_state(cfg2).await;
        let other: IpAddr = "127.0.0.3".parse().unwrap();
        assert!(state2
            .classify_port_occupation("tcp", other, w)
            .await
            .is_none());
        let un = IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED);
        assert!(state2
            .classify_port_occupation("tcp", un, w)
            .await
            .is_some());
        drop(state2);

        // SSH 网关:热配置 enabled 才命中,关闭后不保留占用
        let s = free_port();
        state.hot.write().await.ssh_gateway = SshGatewayConfig {
            enabled: true,
            bind_port: s,
            bind_addr: "127.0.0.1".into(),
            ..Default::default()
        };
        let hit = state.classify_port_occupation("tcp", LO, s).await.unwrap();
        assert!(hit.label.contains("SSH 网关"), "{}", hit.label);
        state.hot.write().await.ssh_gateway.enabled = false;
        assert!(state.classify_port_occupation("tcp", LO, s).await.is_none());

        // metrics 陈旧配置不得作为归属依据:hot.metrics 指向 stale 而实际监听在
        // live_p → 只命中 live_p;未监听时 stale 也不命中
        let stale = free_port();
        let live_p = free_port();
        state.hot.write().await.metrics = MetricsConfig {
            enable_prometheus: true,
            bind_addr: "127.0.0.1".into(),
            port: stale,
        };
        assert!(state
            .classify_port_occupation("tcp", LO, stale)
            .await
            .is_none());
        start_metrics(&state, live_p).await;
        let hit = state
            .classify_port_occupation("tcp", LO, live_p)
            .await
            .unwrap();
        assert!(hit.label.contains("Prometheus"), "{}", hit.label);
        assert!(hit.live);
        assert!(state
            .classify_port_occupation("tcp", LO, stale)
            .await
            .is_none());
    }
}

#[cfg(test)]
mod update_disable_teardown_tests {
    //! A5 回归:update_service 置 enabled=false 必须**真的拆运行态**。
    //!
    //! 修前走 `!port_changed` 分支:只落库 enabled=0 + publish_spec_in_place,
    //! 既不 teardown 也不发 ServiceRemove。后果:
    //!   - 端口继续监听(用户以为已停用);
    //!   - `port_in_use` 只统计 enabled 运行态 → 同端口可被新规则再次占用。
    //!
    //! 对照 disable 端点有完整 teardown,两条路径曾经分叉。
    use super::*;

    async fn body_json(resp: Response) -> serde_json::Value {
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    const CID: &str = "a5-client";

    async fn setup() -> (Arc<AppState>, i64) {
        let db = crate::db::Db::open("sqlite::memory:").await.unwrap();
        db.create_client(CID, "a5", &"0".repeat(64), "")
            .await
            .unwrap();
        let state = Arc::new(AppState::new(crate::config::ServerConfig::default(), db));
        (state, 0)
    }

    fn spec_enabled(port: u16, enabled: bool) -> rh_protocol::ProxySpec {
        serde_json::from_value(serde_json::json!({
            "name": "a5",
            "kind": "tcp",
            "client_id": CID,
            "bind_ip": "127.0.0.1",
            "bind_port": port,
            "target_host": "127.0.0.1",
            "target_port": 9,
            "enabled": enabled,
        }))
        .unwrap()
    }

    /// 核心断言:update 置 false 后,运行时必须已从表里移除(teardown 生效)。
    #[tokio::test]
    async fn update_to_disabled_tears_down_runtime() {
        let (state, _) = setup().await;
        // 直接建一条运行态(绕过 HTTP,聚焦 update 的语义)
        let spec = spec_enabled(23456, true);
        let id = state
            .db
            .insert_proxy(CID, &spec, false, "console")
            .await
            .unwrap();
        // 直接注册运行态(不做真实 bind —— spawn_runtime 会占端口且需客户端在线)
        let rt = std::sync::Arc::new(crate::state::ProxyRuntime::new(
            id as u64,
            CID.into(),
            spec.clone(),
        ));
        state.proxies.runtimes.insert(id as u64, rt);
        assert!(
            state.proxies.get(id as u64).is_some(),
            "前置:运行态应已注册"
        );

        // update 里置 enabled=false
        let mut upd = spec_enabled(23456, false);
        upd.name = "a5".into();
        let resp = update_service_inner(&state, id, upd).await;
        let body = body_json(resp).await;
        assert_eq!(body["code"], 0, "update 应成功: {body}");

        assert!(
            state.proxies.get(id as u64).is_none(),
            "A5:update 置 enabled=false 必须 teardown 运行态(修前端口仍在监听)"
        );
        // 落库也必须是 disabled
        let got = state.db.get_proxy(id).await.unwrap().unwrap();
        assert!(!got.2.enabled, "DB 里必须 enabled=false");
    }

    /// 端口占用随之释放:禁用后同端口可被新规则占用(修前会双占)。
    #[tokio::test]
    async fn disabled_after_update_releases_port() {
        let (state, _) = setup().await;
        let spec = spec_enabled(23457, true);
        let id = state
            .db
            .insert_proxy(CID, &spec, false, "console")
            .await
            .unwrap();
        let rt = std::sync::Arc::new(crate::state::ProxyRuntime::new(
            id as u64,
            CID.into(),
            spec.clone(),
        ));
        state.proxies.runtimes.insert(id as u64, rt);

        let mut upd = spec_enabled(23457, false);
        upd.name = "a5".into();
        let resp = update_service_inner(&state, id, upd).await;
        let body = body_json(resp).await;
        assert_eq!(body["code"], 0, "update 应成功: {body}");

        // ① 运行态必须已从表里消失(这是 teardown 的直接证据)
        assert!(
            state.proxies.get(id as u64).is_none(),
            "A5:update 置 false 后运行态必须被移除(修前只改 spec,端口继续监听)"
        );
        // ② 端口占用随之释放(port_in_use 只统计 enabled 运行态)
        assert!(
            !state
                .proxies
                .port_in_use("tcp", "127.0.0.1".parse().unwrap(), 23457, None, None)
                .await,
            "A5:teardown 后端口必须报告未占用(否则新规则可与之双占)"
        );
    }
}
