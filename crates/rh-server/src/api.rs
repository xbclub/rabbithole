//! 管理 REST API(axum):vben 契约 + 客户端/规则/visitor/统计/事件/指标。

use crate::config::CONTROL_SEND_TIMEOUT;
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

/// 统一成功信封。D-1:修前 `json!` 把 data 经 to_value 深拷贝进 Value 树,
/// axum 再把整棵树序列化 —— 同一数据两遍、分配量数倍于响应体积。包装
/// struct 让 serde 直接把 data 写进输出缓冲,不再建中间树(data 本身是
/// Value 时省掉的是第二次建树;是强类型行时连第一次都没有)。
#[derive(Serialize)]
struct OkEnvelope<T> {
    code: i32,
    data: T,
    message: &'static str,
}

pub fn ok<T: Serialize>(data: T) -> Response {
    Json(OkEnvelope {
        code: 0,
        data,
        message: "ok",
    })
    .into_response()
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
                // ★用截断后的 uname,不用 req.username。
                //
                // 上面已把用户名截到 64 字符用于限速键,但这里用了原始值:
                // 请求体可达 ~2MB,攻击者每次失败登录都能往 events 表写一条
                // 2MB 记录,而 events 表**没有清理机制**(见 P3)。
                &format!("登录失败(密码错误):{uname}"),
            )
            .await;
        return err(1001, "用户名或密码错误");
    }
    let ttl = state.cfg.web.jwt_ttl_hours;
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
    // R1/R2:update_user_password 内部递增 pwd_version,递增失效代
    state
        .pwd_version_epoch
        .fetch_add(1, std::sync::atomic::Ordering::Release);
    // 轮换会话密钥:旧令牌(含本请求所用)全部失效
    *state.web_jwt_secret.lock().await = Some(std::sync::Arc::new(
        uuid::Uuid::new_v4().simple().to_string(),
    ));
    state
        .events
        .emit("info", "auth", &format!("密码已修改:{}", user))
        .await;
    ok(json!({ "relogin": true }))
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
    pub async fn jwt_secret(&self) -> std::sync::Arc<String> {
        // N19:Arc 引用计数替代整串 clone
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
            // R1:读内存缓存(失效点在本文件 logout / change_password)。
            // R2:epoch 守卫 —— miss 填充与失效交错时,以「填充前捕获的
            // epoch」入条目;任何后续 bump 都令其失配,旧值不会钉死。
            let want = d.claims.get("pv").and_then(|v| v.as_i64()).unwrap_or(0);
            use std::sync::atomic::Ordering as AtomicOrdering;
            let epoch_now = state.pwd_version_epoch.load(AtomicOrdering::Acquire);
            let cached = state
                .pwd_version_cache
                .get(&user)
                .filter(|e| e.value().1 == epoch_now)
                .map(|e| e.value().0);
            let cur = match cached {
                Some(v) => v,
                None => {
                    let v = state.db.user_pwd_version(&user).await.unwrap_or(0);
                    state.pwd_version_cache.insert(user.clone(), (v, epoch_now));
                    v
                }
            };
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
            // R1/R2:递增失效代,本代条目全部失配(含并发填充中的旧值)
            state
                .pwd_version_epoch
                .fetch_add(1, std::sync::atomic::Ordering::Release);
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

/// D-1:list_clients 行结构(修前逐行 `json!` 建 Value 树)。
/// 字段集与修前 json! 构造完全一致(对比测试 `d1_rows_match_legacy_json`
/// 逐字段钉住;null 语义靠 Option 无 skip_serializing_if 保持)。
#[derive(Serialize)]
struct ClientListRow {
    id: String,
    name: String,
    enabled: bool,
    remark: String,
    online: bool,
    online_since: Option<u64>,
    reported_revision: Option<u64>,
    expected_revision: i64,
    client_version: String,
    hostname: String,
    os: String,
    last_online_at: Option<i64>,
    created_at: i64,
    bytes_in: u64,
    bytes_out: u64,
    conns_current: i64,
    // F2 配额(0/空 = 不限)
    rate_limit: String,
    max_conns: i64,
    max_services: i64,
    flow_limit: i64,
}

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
            // R1:先查再插,命中不再克隆 key(修前每个运行态克隆一次 uuid)
            let ent = match m.get_mut(&rt.client_id) {
                Some(ent) => ent,
                None => m.entry(rt.client_id.clone()).or_insert((0, 0, 0)),
            };
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
            ClientListRow {
                id: c.id,
                name: c.name,
                enabled: c.enabled,
                remark: c.remark,
                online: sess.is_some(),
                online_since: sess.as_ref().map(|s| s.started_at.elapsed().as_secs()),
                reported_revision: sess.as_ref().map(|s| s.applied()),
                expected_revision: c.config_revision,
                client_version: c.client_version,
                hostname: c.hostname,
                os: c.os,
                last_online_at: c.last_online_at,
                created_at: c.created_at,
                bytes_in: meter.0,
                bytes_out: meter.1,
                conns_current: meter.2,
                // F2 配额(0/空 = 不限)
                rate_limit: c.rate_limit,
                max_conns: c.max_conns,
                max_services: c.max_services,
                flow_limit: c.flow_limit,
            }
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
            // R4:一列投影(消费方只用 id;修前全量 spec 反序列化)
            let proxies = state.db.list_proxy_ids(&id).await.unwrap_or_default();
            // D-5:一列投影(消费方只用 id;修前全量 VisitorSpec 构造)
            let visitors = state.db.list_visitor_ids(&id).await.unwrap_or_default();
            let rev = state.db.bump_revision(&id).await.unwrap_or(0);
            state.bump_cfg_gen(&id);
            // ★teardown 并发,发送仍串行。
            //
            // 与 `delete_client`(上一轮 P3 已修)完全同构:单次
            // `teardown_runtime` 最坏等 `serve_handle` 收敛 2s,串行执行时
            // N 条规则 = N × 2s。禁用一个有 100 条 serve loop 不收敛规则的
            // 客户端,这个 HTTP 请求会挂 200 秒。P3 当时只改了 delete_client,
            // 漏了这个同款调用点。
            //
            // ★为什么拆成「先全部 teardown、再全部 send」而不是保持原来的
            // 「teardown 一条 → send 一条」交替:
            // 交替结构无法并发化(每次 send 都要等前一条 teardown 完成)。
            // 拆分后的顺序变化是:REMOVE 帧从「穿插发出」变成「集中在
            // teardown 全部完成后发出」。
            //
            // 这个变化不破坏语义:REMOVE 的语义是「通知客户端删掉这条规则」,
            // 客户端收到它之后只是 `sess.proxies.lock().await.remove(&id)`
            // (session.rs 的 `remove_entity`),不依赖服务端运行态是否还在。
            // 服务端先拆干净再发通知,方向更清晰。
            //
            // 若客户端在收到 REMOVE 前断线重连,与「交替结构下收到一半
            // REMOVE 就断线」的差别只是「少收几条 REMOVE」—— 重连后服务端
            // 会用全量 ServiceSync 覆盖,结果一致。
            //
            // 并发度 8:与 delete_client 一致(理由见那里 —— 每条规则内部
            // 可能等满 2s,过高会让「同时持有大量 rule_cancel + fd 正在
            // 释放」的窗口变宽)。
            //
            // 自锁核查:本函数外层持有 `serial_lock(&id)`,而
            // `teardown_runtime` 内部不取 `serial_lock`(grep 结果 0)。
            {
                use futures::stream::StreamExt as _;
                futures::stream::iter(proxies.iter())
                    .for_each_concurrent(8, |pid| {
                        let state = state.clone();
                        let pid = *pid as u64;
                        async move {
                            crate::runtime::teardown_runtime(&state, pid).await;
                        }
                    })
                    .await;
            }
            // ★REMOVE 发送也并发(原计划 Q4 判断"可能不做",实测后改为做)。
            //
            // 原本担心:并发 send_bounded 在队列满时会"同时等 2s、然后同时
            // 失败",一次性丢掉 N 条 REMOVE,而串行至少能渐进送达前几条。
            //
            // **实测推翻了这个担心**(用本仓库的 tokio 直接测,队列容量固定、
            // 不消费):
            //
            // | 场景 | 串行送达 | 并发送达 | 串行耗时 | 并发耗时 |
            // |---|---|---|---|---|
            // | cap=3,  n=10 | 3 | **3** | 14.02s | **2.01s** |
            // | cap=3,  n=20 | 3 | **3** | 34.05s | **6.01s** |
            // | cap=10, n=10 | 10 | **10** | 41µs | 70µs |
            // | cap=10, n=20 | 10 | **10** | 20.03s | **4.01s** |
            //
            // 送达条数**完全相同** —— 因为 `send_bounded` 不重试:串行时第一条
            // 撞满队列等 2s 超时丢弃,第二条照样如此,并不是"渐进送达"。
            // 队列容量 k 决定能送几条,送完之后串行和并发都只剩超时。
            // 所以并发化严格更优(同样的送达率,耗时从 N×2s 降到 ⌈N/8⌉×2s)。
            //
            // 顺序:客户端 `remove_entity` 按 entity 独立处理
            // (`proxies.remove(&id)` / `visitors.remove(&id)`),无跨条依赖;
            // revision 只用于 ApplyResult 回执,不参与排序。所以乱序无害。
            {
                use futures::stream::StreamExt as _;
                let payloads: Vec<(&'static str, u64)> = proxies
                    .iter()
                    .map(|pid| ("proxy", *pid as u64))
                    .chain(visitors.iter().map(|vid| ("visitor", *vid as u64)))
                    .collect();
                futures::stream::iter(payloads)
                    .for_each_concurrent(8, |(entity_type, entity_id)| {
                        let state = state.clone();
                        let id = id.clone();
                        async move {
                            if let Some(sess) = state.sessions.get(&id) {
                                use rh_protocol::message::{ServerMessage, ServiceRemove};
                                let _ = sess
                                    .send_bounded(
                                        ServerMessage::ServiceRemove(ServiceRemove {
                                            revision: rev,
                                            entity_type: entity_type.into(),
                                            entity_id,
                                        }),
                                        CONTROL_SEND_TIMEOUT,
                                    )
                                    .await;
                            }
                        }
                    })
                    .await;
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
    // R4:一列投影(teardown 只用 pid;修前全量 spec 反序列化)
    let proxies = state.db.list_proxy_ids(&id).await.unwrap_or_default();
    // ★并发 teardown:单次 `teardown_runtime` 最坏等 `serve_handle` 收敛 2s,
    // 串行执行时 N 条规则 = N × 2s。删一个有 100 条规则、若干条 serve loop
    // 不收敛的客户端,这个 HTTP 请求会挂 200 秒。
    //
    // 并发度取 **8 而非 16**:每条规则内部可能等满 2s,并发度过高会让
    // 「同时持有大量 rule_cancel + fd 正在释放」的窗口变宽。8 已足够把
    // 最坏总耗时从 N×2s 压到约 2s(16 换来的边际收益很小)。
    //
    // 并发安全性:
    //   - `serve_handle` 的 2s 超时是**每条规则独立**的预算(不是共享预算),
    //     这是收益来源;
    //   - `runtimes.remove(&proxy_id)` 是 dashmap 操作,不同 key 并发安全;
    //   - `leave_group` 按组键加锁,同组规则会在组锁上排队,但锁内没有
    //     嵌套的跨组等待,不会死锁;
    //   - **已核实 `teardown_runtime` 内部不取 `serial_lock`**(grep 结果为 0),
    //     否则外层已持有 `serial_lock(&id)` 时并发化会自锁。
    {
        use futures::stream::StreamExt as _;
        futures::stream::iter(proxies.iter())
            .for_each_concurrent(8, |pid| {
                let state = state.clone();
                let pid = *pid as u64;
                async move {
                    crate::runtime::teardown_runtime(&state, pid).await;
                }
            })
            .await;
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

/// D-1:list_services 行结构(修前逐行 `json!` 建 Value 树;每行 20 字段
/// 且内嵌整份 spec,是最重的消费方)。字段集与修前 json! 构造完全一致
/// (对比测试 `d1_rows_match_legacy_json` 逐字段钉住)。
#[derive(Serialize)]
struct ServiceListRow {
    id: i64,
    client_id: String,
    name: String,
    kind: &'static str,
    enabled: bool,
    bind_ip: String,
    bind_port: u16,
    target: String,
    target_host: String,
    target_port: u16,
    plugin: Option<String>,
    group: Option<String>,
    status: String,
    status_reason: String,
    healthy: bool,
    pool_ready: usize,
    spec: ProxySpec,
    // F1 分规则计量(用户侧口径;运行时不在(已禁用)则为 0)
    bytes_in: u64,
    bytes_out: u64,
    conns_current: i64,
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
    // R1:limits 读锁提到循环外(修前每行取一次;std RwLock,微秒级)
    let (default_pool_size, max_pool_size) = {
        let l = state.limits.read().unwrap_or_else(|e| e.into_inner());
        (l.default_pool_size, l.max_pool_size)
    };
    for (id, client_id, mut spec) in rows {
        normalize_spec(&mut spec, default_pool_size, max_pool_size);
        // ★列表回显前抹掉 secret_key。
        //
        // 下方 `"spec": spec` 会把整个 ProxySpec 序列化给前端,而 stcp/sudp
        // 的 `secret_key` 是访问密钥(与客户端 secret 同级敏感)。它"仅创建时
        // 返回一次"是既有策略(见 rotate-secret),列表接口不该把它翻出来。
        // 前端不需要该值:编辑时留空即表示"不修改"(update 路径按空值保留原密钥,
        // 见 api.rs 里 `old...filter(|c| !c.secret_key.is_empty())`)。
        // R1:spec 是本行独占所有权,直接原地抹除(修前先整份深克隆)。
        // 强类型行按声明序直接序列化,下方字段读取都发生在构造时。
        if let Some(secret_cfg) = spec.stcp.as_mut() {
            secret_cfg.secret_key.clear();
        }
        if let Some(secret_cfg) = spec.sudp.as_mut() {
            secret_cfg.secret_key.clear();
        }
        // ★一次 `get` 拿 rt,四处复用。
        //
        // 修前这里调了 **4 次** `state.proxies.get(id)`(任务书与报告
        // 早期都写成 3 次,实际还有 `healthy` 那一处,已核实为 4 次):
        // status / pool_ready / rt_meter / healthy。N 条规则 = 4N 次
        // dashmap 查找,每次都要 hash + 分片锁。
        //
        // 只做零风险的合并:**不动 `snapshot_state` 的阻塞语义**。
        // 改成 `try_read` 会让列表页在竞争时显示 "unknown",那是接口
        // 语义变化(报告 §7 已否决),不在本项范围。
        let rt = state.proxies.get(id as u64);
        let (status, reason) = match rt.as_ref() {
            Some(rt) => {
                let (st, r) = rt.snapshot_state().await;
                (st.as_str().to_string(), r)
            }
            None => ("disabled".into(), String::new()),
        };
        let pool_ready = match rt.as_ref() {
            Some(rt) => match rt.pool.lock().await.as_ref() {
                Some(p) => p.ready.lock().await.len(),
                None => 0,
            },
            None => 0,
        };
        let rt_meter = rt.as_ref().map(|rt| {
            (
                rt.bytes_in.load(std::sync::atomic::Ordering::Relaxed),
                rt.bytes_out.load(std::sync::atomic::Ordering::Relaxed),
                rt.conns_current
                    .load(std::sync::atomic::Ordering::Relaxed)
                    .max(0),
            )
        });
        // spec 整份移入行结构(放最后构造);被引用的标量字段先 clone,
        // 相对修前的整树深拷贝仍是净省。
        out.push(ServiceListRow {
            id,
            client_id,
            name: spec.name.clone(),
            kind: spec.kind.as_str(),
            enabled: spec.enabled,
            bind_ip: spec.bind_ip.clone(),
            bind_port: spec.bind_port,
            target: format!("{}:{}", spec.target_host, spec.target_port),
            target_host: spec.target_host.clone(),
            target_port: spec.target_port,
            plugin: spec.plugin.as_ref().map(|p| p.kind().as_str().to_string()),
            group: spec.group.clone(),
            status,
            status_reason: reason,
            healthy: rt
                .as_ref()
                .map(|rt| rt.healthy.load(std::sync::atomic::Ordering::Relaxed))
                .unwrap_or(true),
            pool_ready,
            spec,
            // F1 分规则计量(用户侧口径;运行时不在(已禁用)则为 0)
            bytes_in: rt_meter.map(|(i, _, _)| i).unwrap_or(0),
            bytes_out: rt_meter.map(|(_, o, _)| o).unwrap_or(0),
            conns_current: rt_meter.map(|(_, _, c)| c).unwrap_or(0),
        });
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
        let l = state.limits.read().unwrap_or_else(|e| e.into_inner());
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
        // R4:COUNT 走索引(修前全量取回 + 逐行 spec 反序列化只为 .len(),
        // 且全程在 serial_lock 临界区内)
        let count = state
            .db
            .count_proxies_for_client(&client.id)
            .await
            .map_err(|e| (500, format!("{e}")))?;
        if count as usize >= per_client_max {
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
        let l = state.limits.read().unwrap_or_else(|e| e.into_inner());
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
        let (code, msg, rev) = disable_service_locked(state, id).await;
        if code != 0 {
            return err(code, &msg);
        }
        return ok(json!({"updated": true, "disabled": true, "revision": rev}));
    }
    // 同理:false → true 走 enable 的完整路径(重建运行态 + 下发)
    if !old.enabled && spec.enabled {
        if let Err(e) = state.db.update_proxy(id, &spec, port_auto).await {
            return err(500, &format!("{e}"));
        }
        let (code, msg, rev) = enable_service_locked(state, id).await;
        if code != 0 {
            return err(code, &msg);
        }
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
    // D-7:一次查询保留整行(修前第一次只留 enabled 整行丢弃、锁后
    // 重查一次、禁用分支再查一次取 revision)。同客户端的并发变更
    // 都经 serial_lock 串行,锁前单次读取无一致性障碍(R1 约定同型)。
    let Some((_, client_id, mut spec, port_auto)) = state.db.get_proxy(id).await.ok().flatten()
    else {
        return err(3001, "转发规则不存在");
    };
    let lock = state.serial_lock(&client_id);
    let _g = lock.lock().await;
    // 行缺失(极端竞态)= 按禁用走 deferred、rev=0,与修前 get_client
    // 失败时的语义一致,不 panic。
    let client = state.db.get_client(&client_id).await.ok().flatten();
    let client_enabled = client.as_ref().map(|c| c.enabled).unwrap_or(false);
    // 客户端禁用:仅置 enabled=1 落库(不推进 revision、不 bind,等客户端恢复)
    if !client_enabled {
        state.db.set_proxy_enabled(id, true).await.ok();
        spec.enabled = true;
        let cur_rev = client
            .as_ref()
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
            .send_bounded(
                ServerMessage::ServiceRemove(ServiceRemove {
                    revision: rev,
                    entity_type: "proxy".into(),
                    entity_id: id as u64,
                }),
                CONTROL_SEND_TIMEOUT,
            )
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
            .send_bounded(
                ServerMessage::ServiceRemove(ServiceRemove {
                    revision: rev,
                    entity_type: "proxy".into(),
                    entity_id: id as u64,
                }),
                CONTROL_SEND_TIMEOUT,
            )
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
    // ★按 client_id 分组:组间并发、组内串行。
    //
    // 为什么必须分组:三个 `*_inner` 内部都会取 `serial_lock(client_id)`,
    // 那是**每客户端串行域** —— 防端口竞态、防规则双占,是正确性要求。
    // 不同客户端的串行域互不相交,可以并行;同客户端必须排队。
    //
    // 组内为什么仍调 `*_inner`(而不是外层持锁 + `*_locked`):inner 自己
    // 会 `lock().await`,外层若已持同一把 `tokio::sync::Mutex` 就是二次
    // 加锁 → 直接死锁(与 A5 注释描述的同一个坑)。让 inner 自己排队,
    // 组内天然串行 —— `tokio::sync::Mutex` 是公平的,同客户端的多个
    // future 会按到达顺序逐个进入临界区。
    //
    // ★结果必须与 `req.ids` 同序:前端按请求顺序回填。这里用下标回填,
    // 而不是按完成顺序 push。
    let ids = req.ids;
    let action = req.action;
    let mut results: Vec<Option<serde_json::Value>> = vec![None; ids.len()];
    if !matches!(action.as_str(), "enable" | "disable" | "delete") {
        let msg = format!("未知操作:{action}");
        for (i, id) in ids.iter().enumerate() {
            results[i] = Some(json!({"id": id, "code": 9000, "message": msg}));
        }
        return ok(results.into_iter().flatten().collect::<Vec<_>>());
    }
    // 一次全表查询建立 id -> client_id 映射(而不是逐 id 查一次)。
    // R3:轻量两列投影(修前 list_proxies 把每行 spec JSON 全量反序列化
    // 只为取两列)。
    let owner: std::collections::HashMap<i64, String> = match state.db.list_proxy_owners().await {
        Ok(rows) => rows.into_iter().collect(),
        Err(e) => return err(500, &format!("{e}")),
    };
    // 按 client_id 分组,组内保持 ids 的原顺序;同时记住每项的原下标。
    let mut groups: std::collections::HashMap<String, Vec<(usize, i64)>> =
        std::collections::HashMap::new();
    let mut orphans: Vec<(usize, i64)> = Vec::new();
    for (i, id) in ids.iter().enumerate() {
        match owner.get(id) {
            Some(cid) => groups.entry(cid.clone()).or_default().push((i, *id)),
            // 不存在的 id:不占锁,直接给出 not found
            None => orphans.push((i, *id)),
        }
    }
    for (i, id) in orphans {
        results[i] = Some(json!({"id": id, "code": 3001, "message": "not found"}));
    }

    // 组间并发。并发度取 8:与 `serial_lock` 的粒度一致,再高也只是把
    // 压力堆到 SQLite 写锁上(它是全局单写者),不会更快。
    use futures::stream::StreamExt as _;
    let outcomes: Vec<((usize, i64), (i32, String))> = {
        let state = state.clone();
        let action = action.clone();
        futures::stream::iter(groups)
            .map(|(_, items)| {
                let state = state.clone();
                let action = action.clone();
                async move {
                    // 组内串行:逐个 await,inner 内部自己取 serial_lock
                    let mut out = Vec::with_capacity(items.len());
                    for (idx, id) in items {
                        let r = match action.as_str() {
                            "enable" => enable_service_inner(&state, id).await,
                            "disable" => disable_service_inner(&state, id).await,
                            _ => delete_service_inner(&state, id).await,
                        };
                        out.push(((idx, id), r));
                    }
                    out
                }
            })
            .buffer_unordered(8)
            .flat_map(futures::stream::iter)
            .collect()
            .await
    };
    for ((idx, id), (code, message)) in outcomes {
        results[idx] = Some(json!({"id": id, "code": code, "message": message}));
    }
    // 批量路径结束:把合并窗口里的路由重建落实一次,让调用方在响应返回时
    // 就能看到新路由(批量操作是"一次请求改一大批",不该让用户等下一个事件)。
    state.proxies.flush_vhost_routes().await;
    ok(results.into_iter().flatten().collect::<Vec<_>>())
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
    // 批量端点只要 (code, msg);rev 供单条路径直接使用(R1)
    let (code, msg, _) = enable_service_locked(state, id).await;
    (code, msg)
}

/// 启用的实现本体,**要求调用方已持有 serial_lock**(理由同
/// [`disable_service_locked`])。
/// 返回 (code, msg, revision):R1 —— revision 由本函数计算,调用方
/// 不再为取 revision 重复查一次 clients 表。
async fn enable_service_locked(state: &Arc<AppState>, id: i64) -> (i32, String, u64) {
    let Some((_, client_id, mut spec, port_auto)) = state.db.get_proxy(id).await.ok().flatten()
    else {
        return (3001, "not found".into(), 0);
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
        return (0, "ok".into(), rev);
    }
    // 绑定资格失败:回滚 enabled,保持 DB 与运行态一致(与单个 enable 同语义)
    if let Err((code, msg)) = create_bind_checked(state, &client_id, &mut spec, port_auto, id).await
    {
        state.db.set_proxy_enabled(id, false).await.ok();
        return (code, msg, 0);
    }
    crate::runtime::teardown_runtime(state, id as u64).await;
    crate::runtime::spawn_runtime(state, &client_id, &spec, rev).await;
    state
        .events
        .emit("info", "service", &format!("转发规则已启用:{}", spec.name))
        .await;
    (0, "ok".into(), rev)
}

async fn disable_service_inner(state: &Arc<AppState>, id: i64) -> (i32, String) {
    let Some((_, client_id, _, _)) = state.db.get_proxy(id).await.ok().flatten() else {
        return (3001, "not found".into());
    };
    let lock = state.serial_lock(&client_id);
    let _g = lock.lock().await;
    let (code, msg, _) = disable_service_locked(state, id).await;
    (code, msg)
}

/// 停用的实现本体,**要求调用方已持有该客户端的 serial_lock**。
///
/// ★为什么必须拆出这一层(A5):`update_service_inner` 自己已经持锁,
/// 若它直接调 [`disable_service_inner`] 会在同一把 `tokio::sync::Mutex`
/// 上二次加锁 —— 直接死锁(不是 RwLock 的重入读,是写锁自锁)。
/// 返回 (code, msg, revision):同 enable_service_locked(R1)。
async fn disable_service_locked(state: &Arc<AppState>, id: i64) -> (i32, String, u64) {
    let Some((_, client_id, spec, _)) = state.db.get_proxy(id).await.ok().flatten() else {
        return (3001, "not found".into(), 0);
    };
    state.db.set_proxy_enabled(id, false).await.ok();
    let rev = state.db.bump_revision(&client_id).await.unwrap_or(0);
    state.bump_cfg_gen(&client_id);
    crate::runtime::teardown_runtime(state, id as u64).await;
    if let Some(sess) = state.sessions.get(&client_id) {
        use rh_protocol::message::{ServerMessage, ServiceRemove};
        let _ = sess
            .send_bounded(
                ServerMessage::ServiceRemove(ServiceRemove {
                    revision: rev,
                    entity_type: "proxy".into(),
                    entity_id: id as u64,
                }),
                CONTROL_SEND_TIMEOUT,
            )
            .await;
    }
    state
        .events
        .emit("info", "service", &format!("转发规则已停用:{}", spec.name))
        .await;
    (0, "ok".into(), rev)
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
            .send_bounded(
                ServerMessage::ServiceRemove(ServiceRemove {
                    revision: rev,
                    entity_type: "proxy".into(),
                    entity_id: id as u64,
                }),
                CONTROL_SEND_TIMEOUT,
            )
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
///
/// ★一次全表查询后内存匹配,而不是每客户端查一次。
///
/// 修前是 `1 + N_clients` 次 SQLite 查询(create_visitor 与 update_visitor
/// 各调一次本函数)。逐客户端查的代价不只是多 N 次往返 —— 每次还要把该
/// 客户端的全部规则反序列化一遍。改成一次 `list_proxies(None)` 后,反序列化
/// 总量与"所有客户端规则之和"持平,但往返只剩一次。
///
/// 候选顺序必须与修前一致:修前按 `list_clients()` 的返回顺序
/// (`ORDER BY created_at`)逐个追加。所以这里**按 clients 的原顺序过滤**,
/// 而不是按 client_id 排序 —— 后者会改变"全部离线时取首个"的语义。
async fn resolve_visitor_target(state: &Arc<AppState>, spec: &VisitorSpec) -> Option<String> {
    let clients = state.db.list_clients().await.unwrap_or_default();
    // R1:名称/类型都是独立列,轻量查询即可(修前 list_proxies 全表把每条
    // spec JSON 反序列化成 ProxySpec,只为比对 name+kind)。
    let all = state.db.list_proxy_names().await.unwrap_or_default();
    let matched: std::collections::HashSet<&str> = all
        .iter()
        .filter(|(_cid, name, kind)| *name == spec.target_proxy_name && *kind == spec.kind.as_str())
        .map(|(cid, _, _)| cid.as_str())
        .collect();
    let candidates: Vec<String> = clients
        .iter()
        .filter(|c| matched.contains(c.id.as_str()))
        .map(|c| c.id.clone())
        .collect();
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
            .send_bounded(
                ServerMessage::ServiceUpdate(Box::new(ServiceUpdate {
                    revision: rev,
                    entity: SyncEntity::Visitor(spec.clone()),
                })),
                CONTROL_SEND_TIMEOUT,
            )
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
    // R1:按 id 精确定位(修前 list_visitors 全表取回 + 线性 find,两次)。
    let Some((_, client_id, _, _)) = state.db.get_visitor(id).await.unwrap_or(None) else {
        return err(4003, "访问器不存在");
    };
    let lock = state.serial_lock(&client_id);
    let _g = lock.lock().await;
    let Some((_, _, _, _old)) = state.db.get_visitor(id).await.unwrap_or(None) else {
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
            .send_bounded(
                ServerMessage::ServiceUpdate(Box::new(ServiceUpdate {
                    revision: rev,
                    entity: SyncEntity::Visitor(spec.clone()),
                })),
                CONTROL_SEND_TIMEOUT,
            )
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
    // 首读定位属主;实体在串行域内重读,防与并发删除竞态。
    // R1:精确查(修前全表取回 + 线性 find)。
    let Some((_, client_id, _, _)) = state.db.get_visitor(id).await.unwrap_or(None) else {
        return err(4003, "访问器不存在");
    };
    let lock = state.serial_lock(&client_id);
    let _g = lock.lock().await;
    let Some((_, _, _, mut spec)) = state.db.get_visitor(id).await.unwrap_or(None) else {
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
                .send_bounded(
                    ServerMessage::ServiceUpdate(Box::new(ServiceUpdate {
                        revision: rev,
                        entity: SyncEntity::Visitor(spec),
                    })),
                    CONTROL_SEND_TIMEOUT,
                )
                .await;
        } else {
            let _ = sess
                .send_bounded(
                    ServerMessage::ServiceRemove(ServiceRemove {
                        revision: rev,
                        entity_type: "visitor".into(),
                        entity_id: id as u64,
                    }),
                    CONTROL_SEND_TIMEOUT,
                )
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
    // 首读定位属主;实体在串行域内重读,防重复删除/迟到操作复活实体。
    // R1:精确查(修前全表取回 + 线性 find,锁内再全表 any)。
    let Some((_, client_id, _, _)) = state.db.get_visitor(id).await.unwrap_or(None) else {
        return err(4003, "访问器不存在");
    };
    let lock = state.serial_lock(&client_id);
    let _g = lock.lock().await;
    if state.db.get_visitor(id).await.unwrap_or(None).is_none() {
        return err(4003, "访问器不存在");
    }
    let rev = state.db.bump_revision(&client_id).await.unwrap_or(0);
    state.bump_cfg_gen(&client_id);
    let _ = state.db.delete_visitor(id).await;
    if let Some(sess) = state.sessions.get(&client_id) {
        use rh_protocol::message::{ServerMessage, ServiceRemove};
        let _ = sess
            .send_bounded(
                ServerMessage::ServiceRemove(ServiceRemove {
                    revision: rev,
                    entity_type: "visitor".into(),
                    entity_id: id as u64,
                }),
                CONTROL_SEND_TIMEOUT,
            )
            .await;
    }
    ok(json!({"deleted": true}))
}

// ---------------- stats / events / metrics ----------------

/// D-1:stats_overview 响应体(修前 `json!` 建树;仪表盘轮询端点,
/// 管理面频率最高)。字段集与修前 json! 构造一致(对比测试钉住)。
#[derive(Serialize)]
struct StatsOverview {
    clients_total: i64,
    clients_online: usize,
    services_total: i64,
    services_running: usize,
    conns_current: i64,
    bytes_in: u64,
    bytes_out: u64,
}

async fn stats_overview(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    if require_auth(&state, &headers).await.is_err() {
        return unauthorized();
    }
    // R1:COUNT 查询(修前全表取回 + 逐行 spec JSON 反序列化只为 .len();
    // 这是仪表盘轮询端点,管理面频率最高)。
    // R7:两个 COUNT 无依赖,join! 并行(与 build_sync_entities 同型)。
    let (clients_total, services_total) =
        tokio::join!(state.db.count_clients(), state.db.count_proxies());
    let clients_total = clients_total.unwrap_or(0);
    let services_total = services_total.unwrap_or(0);
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
    ok(StatsOverview {
        clients_total,
        clients_online: state.sessions.count_online(),
        services_total,
        services_running: running,
        conns_current: totals.conns_current,
        bytes_in: totals.bytes_in,
        bytes_out: totals.bytes_out,
    })
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
            "limits": serde_json::to_value(&*state.limits.read().unwrap_or_else(|e| e.into_inner()))
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
    // R1:authorized_keys 的**阻塞文件写**延后到热配置写锁外执行
    //(修前 `std::fs::write` 在 `hot.write()` 持有窗口内,同时阻塞 worker
    //线程,期间所有 hot 读者 —— port_allowed/规则创建路径/ssh supervisor
    //—— 全部停等;磁盘慢时窗口不可控)。
    let mut ak_write: Option<(std::path::PathBuf, String)> = None;
    {
        let mut hot = state.hot.write().await;
        if let Some(p) = req.ports {
            if let Some(list) = p.allow_ports {
                hot.allow_ports = list;
                // B7:解析缓存随白名单同步重建(不得只改 Vec 不改缓存)
                hot.reparse_allow_list();
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
            ak_write = Some((path, keys.clone()));
            ssh_changed = true;
            applied.push("authorized_keys".into());
        }
    }
    if let Some((path, keys)) = ak_write {
        if let Err(e) = std::fs::write(&path, &keys) {
            return err(
                500,
                &format!("authorized_keys 写入失败({}):{e}", path.display()),
            );
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
            // D-12:原子快照同步 store(修前从未 store,热改对新会话
            // 一直不生效——此次一并修正)
            state.mux_streams.store(
                crate::config::LimitsConfig::mux_streams(&lim),
                std::sync::atomic::Ordering::Relaxed,
            );
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
            // D-12:数据面读原子快照,这里同步 store
            state
                .udp_sessions_limit
                .store(v, std::sync::atomic::Ordering::Relaxed);
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
        disk.limits = state
            .limits
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
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
    // R1:存在性用精确查(修前 list_certs 全表取回 KB 级 PEM 只为 any)
    if !state.db.cert_exists(id).await.unwrap_or(false) {
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
    use prometheus::{Encoder as _, TextEncoder};
    let encoder = TextEncoder::new();
    let mut buffer = Vec::new();
    // R1:直接 encode 进目标缓冲(修前 encode_to_string 先成 String 再
    // 整块拷入,每次抓取多一次全量分配+拷贝)。
    let _ = encoder.encode(&state.metrics.registry.gather(), &mut buffer);
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
        assert!(!a_ip.contains(':'), "IPv4 限速键不得含端口: {a_ip}");
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

    /// D-1 红线:信封与强类型行结构必须与修前 json! 构造逐字段一致
    /// (右侧 legacy 就是修前代码原样;JSON 对象键序无语义,serde_json
    /// Value 相等 = 字段/类型/值全等,Option::None 两侧都序列化为 null)。
    #[tokio::test]
    async fn d1_envelope_and_rows_match_legacy_json() {
        // --- 信封 ---
        let v = body_json(ok(json!({"a": 1}))).await;
        assert_eq!(v, json!({"code": 0, "data": {"a": 1}, "message": "ok"}));

        // --- stats_overview ---
        let s = StatsOverview {
            clients_total: 3,
            clients_online: 2,
            services_total: 10,
            services_running: 7,
            conns_current: 5,
            bytes_in: 111,
            bytes_out: 222,
        };
        assert_eq!(
            serde_json::to_value(&s).unwrap(),
            json!({
                "clients_total": 3,
                "clients_online": 2,
                "services_total": 10,
                "services_running": 7,
                "conns_current": 5,
                "bytes_in": 111,
                "bytes_out": 222,
            })
        );

        // --- list_clients 行(含 Option 两侧的 Some/None) ---
        let row = ClientListRow {
            id: "c1".into(),
            name: "n".into(),
            enabled: true,
            remark: "r".into(),
            online: true,
            online_since: Some(12),
            reported_revision: None,
            expected_revision: 5,
            client_version: "0.5.1".into(),
            hostname: "h".into(),
            os: "linux".into(),
            last_online_at: None,
            created_at: 1700000000,
            bytes_in: 1,
            bytes_out: 2,
            conns_current: 3,
            rate_limit: "1MB".into(),
            max_conns: 9,
            max_services: 8,
            flow_limit: 7,
        };
        assert_eq!(
            serde_json::to_value(&row).unwrap(),
            json!({
                "id": "c1",
                "name": "n",
                "enabled": true,
                "remark": "r",
                "online": true,
                "online_since": 12,
                "reported_revision": null,
                "expected_revision": 5,
                "client_version": "0.5.1",
                "hostname": "h",
                "os": "linux",
                "last_online_at": null,
                "created_at": 1700000000,
                "bytes_in": 1,
                "bytes_out": 2,
                "conns_current": 3,
                "rate_limit": "1MB",
                "max_conns": 9,
                "max_services": 8,
                "flow_limit": 7,
            })
        );

        // --- list_services 行(含整份 spec 与两个 Option 字段) ---
        let sp = spec("tcp", "svc", 20001);
        let legacy = json!({
            "id": 42,
            "client_id": "c1",
            "name": sp.name,
            "kind": sp.kind.as_str(),
            "enabled": sp.enabled,
            "bind_ip": sp.bind_ip,
            "bind_port": sp.bind_port,
            "target": format!("{}:{}", sp.target_host, sp.target_port),
            "target_host": sp.target_host,
            "target_port": sp.target_port,
            "plugin": sp.plugin.as_ref().map(|p| p.kind().as_str().to_string()),
            "group": sp.group,
            "status": "running",
            "status_reason": "",
            "healthy": true,
            "pool_ready": 2,
            "spec": &sp,
            "bytes_in": 10,
            "bytes_out": 20,
            "conns_current": 30,
        });
        let row = ServiceListRow {
            id: 42,
            client_id: "c1".into(),
            name: sp.name.clone(),
            kind: sp.kind.as_str(),
            enabled: sp.enabled,
            bind_ip: sp.bind_ip.clone(),
            bind_port: sp.bind_port,
            target: format!("{}:{}", sp.target_host, sp.target_port),
            target_host: sp.target_host.clone(),
            target_port: sp.target_port,
            plugin: sp.plugin.as_ref().map(|p| p.kind().as_str().to_string()),
            group: sp.group.clone(),
            status: "running".into(),
            status_reason: String::new(),
            healthy: true,
            pool_ready: 2,
            spec: sp,
            bytes_in: 10,
            bytes_out: 20,
            conns_current: 30,
        };
        assert_eq!(serde_json::to_value(&row).unwrap(), legacy);
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

#[cfg(test)]
mod visitor_target_tests {
    //! #8 回归:`resolve_visitor_target` 从 N+1 查询改成一次全取后,
    //! 匹配规则与候选顺序语义必须与修前一致。
    //!
    //! 这里断言的是**外部可观测行为** —— 解析出哪个 client_id。不碰
    //! "查了几次 DB" 这种实现细节(那正是这次要改掉的东西,断言它会形成
    //! 自证循环)。
    use super::*;

    fn proxy_spec(client_id: &str, name: &str, kind: &str) -> ProxySpec {
        serde_json::from_value(json!({
            "name": name,
            "kind": kind,
            "client_id": client_id,
            "bind_ip": "127.0.0.1",
            "bind_port": 24500,
            "target_host": "127.0.0.1",
            "target_port": 9,
            "enabled": true,
        }))
        .unwrap()
    }

    fn visitor_spec(target_proxy_name: &str, kind: &str) -> VisitorSpec {
        serde_json::from_value(json!({
            "name": "v",
            "kind": kind,
            "enabled": true,
            "bind_addr": "127.0.0.1",
            "bind_port": 24600,
            "target_proxy_name": target_proxy_name,
        }))
        .unwrap()
    }

    async fn state_with(clients: &[(&str, &str)]) -> Arc<AppState> {
        let db = crate::db::Db::open("sqlite::memory:").await.unwrap();
        for (i, (id, secret)) in clients.iter().enumerate() {
            db.create_client(id, id, &format!("{i}").repeat(64), "")
                .await
                .unwrap();
            let _ = secret;
        }
        Arc::new(AppState::new(crate::config::ServerConfig::default(), db))
    }

    async fn add_proxy(state: &Arc<AppState>, client_id: &str, name: &str, kind: &str) {
        state
            .db
            .insert_proxy(
                client_id,
                &proxy_spec(client_id, name, kind),
                false,
                "console",
            )
            .await
            .unwrap();
    }

    /// 唯一匹配:直接返回该客户端。
    #[tokio::test]
    async fn single_match_resolves() {
        let state = state_with(&[("c1", "a"), ("c2", "b")]).await;
        add_proxy(&state, "c2", "target", "stcp").await;
        let got = resolve_visitor_target(&state, &visitor_spec("target", "stcp")).await;
        assert_eq!(got.as_deref(), Some("c2"));
    }

    /// 类型不匹配不算命中(stcp 访问器不能挂到 http 规则上)。
    #[tokio::test]
    async fn kind_must_match() {
        let state = state_with(&[("c1", "a")]).await;
        add_proxy(&state, "c1", "target", "http").await;
        let got = resolve_visitor_target(&state, &visitor_spec("target", "stcp")).await;
        assert!(got.is_none(), "类型不匹配时不得解析到该规则");
    }

    /// 名字不匹配不算命中。
    #[tokio::test]
    async fn name_must_match() {
        let state = state_with(&[("c1", "a")]).await;
        add_proxy(&state, "c1", "other", "stcp").await;
        let got = resolve_visitor_target(&state, &visitor_spec("target", "stcp")).await;
        assert!(got.is_none());
    }

    /// 多个客户端同名同类型:优先**在线**的那个。
    ///
    /// 这是 B15 的核心语义,也是最容易被"改成一次查询 + 内存分组"时弄丢的:
    /// 一旦候选顺序变了,在线优先之外的回退分支(全部离线取首个)就会选错人。
    #[tokio::test]
    async fn prefers_online_client_over_created_order() {
        let state = state_with(&[("c1", "a"), ("c2", "b")]).await;
        add_proxy(&state, "c1", "target", "stcp").await;
        add_proxy(&state, "c2", "target", "stcp").await;
        // c2 在线(注册一个会话),c1 离线
        let (tx, _rx) = tokio::sync::mpsc::channel(4);
        state.sessions.register(crate::session::Session::new(
            "c2".into(),
            "sid-c2".into(),
            1,
            tokio_util::sync::CancellationToken::new(),
            tx,
        ));
        let got = resolve_visitor_target(&state, &visitor_spec("target", "stcp")).await;
        assert_eq!(
            got.as_deref(),
            Some("c2"),
            "多客户端同名时必须优先在线客户端,而不是创建序首个"
        );
    }

    /// #8 基准(手动运行):N 个客户端各 1 条规则时的解析耗时。
    ///
    /// ```text
    /// cargo test -p rh-server --release visitor_target_tests::bench -- --ignored --nocapture
    /// ```
    #[tokio::test]
    #[ignore = "耗时基准,手动运行(--ignored --nocapture)"]
    async fn bench_resolve_scaling() {
        for n in [50usize, 200, 500, 1000] {
            let db = crate::db::Db::open("sqlite::memory:").await.unwrap();
            for i in 0..n {
                let cid = format!("c{i}");
                db.create_client(&cid, &cid, &"0".repeat(64), "")
                    .await
                    .unwrap();
                db.insert_proxy(
                    &cid,
                    &proxy_spec(&cid, &format!("p{i}"), "stcp"),
                    false,
                    "console",
                )
                .await
                .unwrap();
            }
            let state = Arc::new(AppState::new(crate::config::ServerConfig::default(), db));
            let spec = visitor_spec(&format!("p{}", n - 1), "stcp");
            let reps = 20;
            let t0 = std::time::Instant::now();
            for _ in 0..reps {
                let got = resolve_visitor_target(&state, &spec).await;
                assert!(got.is_some());
            }
            println!("N_clients={n:>5}  resolve={:>12?}/次", t0.elapsed() / reps);
        }
    }

    /// 全部离线:按 `list_clients()` 的创建序取首个。
    #[tokio::test]
    async fn all_offline_falls_back_to_creation_order() {
        let state = state_with(&[("c1", "a"), ("c2", "b")]).await;
        add_proxy(&state, "c1", "target", "stcp").await;
        add_proxy(&state, "c2", "target", "stcp").await;
        let got = resolve_visitor_target(&state, &visitor_spec("target", "stcp")).await;
        assert_eq!(
            got.as_deref(),
            Some("c1"),
            "全部离线时应取创建序首个(顺序语义不得因改法漂移)"
        );
    }
}

#[cfg(test)]
mod batch_services_tests {
    //! #3 回归:`batch_services` 按 client_id 分组、组间并发、组内串行,
    //! 且结果与请求 `ids` 同序。
    //!
    //! 断言基于外部可观测行为:返回码、DB 最终状态、`results` 的顺序。
    use super::*;

    async fn state_with_clients(n_clients: usize, per_client: usize) -> (Arc<AppState>, Vec<i64>) {
        let db = crate::db::Db::open("sqlite::memory:").await.unwrap();
        let mut ids = Vec::new();
        for c in 0..n_clients {
            let cid = format!("batch-c{c}");
            db.create_client(&cid, &cid, &format!("{}", c % 10).repeat(64), "")
                .await
                .unwrap();
            for p in 0..per_client {
                let spec: ProxySpec = serde_json::from_value(json!({
                    "name": format!("p{p}"),
                    "kind": "stcp",
                    "client_id": cid,
                    "bind_ip": "127.0.0.1",
                    "bind_port": 0,
                    "target_host": "127.0.0.1",
                    "target_port": 9,
                    "enabled": true,
                }))
                .unwrap();
                // stcp 无独享端口,不需要真实 bind
                let id = db
                    .insert_proxy(&cid, &spec, false, "console")
                    .await
                    .unwrap();
                ids.push(id);
            }
        }
        let state = Arc::new(AppState::new(crate::config::ServerConfig::default(), db));
        (state, ids)
    }

    async fn call_batch(
        state: &Arc<AppState>,
        ids: Vec<i64>,
        action: &str,
    ) -> Vec<serde_json::Value> {
        let token = mint_jwt(state, "admin", 1, 0).await.unwrap();
        let mut headers = HeaderMap::new();
        headers.insert(
            "authorization",
            axum::http::HeaderValue::from_str(&format!("Bearer {token}")).unwrap(),
        );
        let resp = batch_services(
            State(state.clone()),
            headers,
            Json(BatchReq {
                ids,
                action: action.into(),
            }),
        )
        .await;
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        // `ok()` 把结果包在 {code,data,message} 里,这里取 data
        let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        v["data"].as_array().cloned().unwrap_or_default()
    }

    /// 结果必须与请求 ids 同序(前端按下标回填)。
    #[tokio::test]
    async fn results_preserve_request_order() {
        let (state, ids) = state_with_clients(3, 3).await;
        // 打乱顺序,混入一个不存在的 id
        let mut req_ids = vec![ids[7], ids[1], 999_999, ids[4], ids[0]];
        let want = req_ids.clone();
        let results = call_batch(&state, std::mem::take(&mut req_ids), "disable").await;
        assert_eq!(results.len(), want.len(), "结果条数必须与请求一致");
        for (got, want_id) in results.iter().zip(want.iter()) {
            assert_eq!(
                got["id"].as_i64(),
                Some(*want_id),
                "results 必须与 req.ids 同序"
            );
        }
    }

    /// 不存在的 id 给 3001,存在的给 0。
    #[tokio::test]
    async fn missing_ids_reported_individually() {
        let (state, ids) = state_with_clients(1, 2).await;
        let results = call_batch(&state, vec![ids[0], 888_888], "disable").await;
        assert_eq!(results[0]["code"], 0, "存在的规则应成功");
        assert_eq!(results[1]["code"], 3001, "不存在的 id 应报 not found");
    }

    /// 批量 disable 之后,DB 里全部规则都必须是 enabled=0。
    #[tokio::test]
    async fn batch_disable_applies_to_all() {
        let (state, ids) = state_with_clients(4, 5).await;
        let results = call_batch(&state, ids.clone(), "disable").await;
        assert!(
            results.iter().all(|r| r["code"] == 0),
            "全部应成功: {results:?}"
        );
        for id in &ids {
            let (_, _, spec, _) = state.db.get_proxy(*id).await.unwrap().unwrap();
            assert!(!spec.enabled, "规则 {id} 必须已被停用");
        }
    }

    /// 批量 delete 之后,DB 里一条都不剩。
    #[tokio::test]
    async fn batch_delete_removes_all() {
        let (state, ids) = state_with_clients(2, 3).await;
        let results = call_batch(&state, ids.clone(), "delete").await;
        assert!(results.iter().all(|r| r["code"] == 0), "{results:?}");
        let left = state.db.list_proxies(None).await.unwrap();
        assert!(left.is_empty(), "删除后不得残留规则:{left:?}");
    }

    /// 未知 action:全部 9000,且不碰 DB。
    #[tokio::test]
    async fn unknown_action_reports_9000() {
        let (state, ids) = state_with_clients(1, 2).await;
        let results = call_batch(&state, ids.clone(), "explode").await;
        assert_eq!(results.len(), ids.len());
        assert!(results.iter().all(|r| r["code"] == 9000));
        // 规则仍启用
        for id in &ids {
            let (_, _, spec, _) = state.db.get_proxy(*id).await.unwrap().unwrap();
            assert!(spec.enabled, "未知操作不得改动任何规则");
        }
    }

    /// 同客户端的多条规则:组内串行 → 不会死锁,且全部成功。
    ///
    /// 这条是分组并发最危险的地方:若组内改成并发,同一把
    /// `serial_lock` 上的公平队列仍能保证串行,但若有任何路径二次加锁
    /// (inner + locked 混用),这里会直接挂住。用超时把"挂住"变成可见失败。
    #[tokio::test]
    async fn same_client_batch_does_not_deadlock() {
        let (state, ids) = state_with_clients(1, 12).await;
        let fut = call_batch(&state, ids.clone(), "disable");
        let results = tokio::time::timeout(std::time::Duration::from_secs(20), fut)
            .await
            .expect("同客户端批量操作不得死锁");
        assert!(results.iter().all(|r| r["code"] == 0), "{results:?}");
    }

    /// #3 基准(手动运行):N 条规则的批量操作耗时。
    ///
    /// ```text
    /// cargo test -p rh-server --release batch_services_tests::bench -- --ignored --nocapture
    /// ```
    ///
    /// 对比两种分组策略:全部放一个客户端(组内串行,退化为修前行为)
    /// vs 分散到多个客户端(组间并发,修后路径)。同一份 DB 规模下比较。
    #[tokio::test]
    #[ignore = "耗时基准,手动运行(--ignored --nocapture)"]
    async fn bench_batch_scaling() {
        for n in [100usize, 500, 1000] {
            for clients in [1usize, 16] {
                let per = n.div_ceil(clients);
                let (state, ids) = state_with_clients(clients, per).await;
                let ids = ids.into_iter().take(n).collect::<Vec<_>>();
                let token = mint_jwt(&state, "admin", 1, 0).await.unwrap();
                let mut headers = HeaderMap::new();
                headers.insert(
                    "authorization",
                    axum::http::HeaderValue::from_str(&format!("Bearer {token}")).unwrap(),
                );
                let t0 = std::time::Instant::now();
                let resp = batch_services(
                    State(state.clone()),
                    headers,
                    Json(BatchReq {
                        ids: ids.clone(),
                        action: "disable".into(),
                    }),
                )
                .await;
                let _ = axum::body::to_bytes(resp.into_body(), usize::MAX)
                    .await
                    .unwrap();
                println!(
                    "N={n:>5}  clients={clients:>3}  batch_disable={:>12?}",
                    t0.elapsed()
                );
            }
        }
    }

    /// 多客户端批量:全部成功(组间并发路径真的跑通)。
    #[tokio::test]
    async fn multi_client_batch_all_succeed() {
        let (state, ids) = state_with_clients(8, 4).await;
        let fut = call_batch(&state, ids.clone(), "disable");
        let results = tokio::time::timeout(std::time::Duration::from_secs(30), fut)
            .await
            .expect("多客户端批量操作不得超时");
        assert!(
            results.iter().all(|r| r["code"] == 0),
            "全部应成功: {results:?}"
        );
    }
}

#[cfg(test)]
mod admin_send_bounded_tests {
    //! P2 回归:api.rs 的 admin handler 一律用 `send_bounded`,不得裸 `send`。
    //!
    //! 裸 `send` 是 `queue_tx.send(msg).await` —— 队列满时**永久 park**。
    //! 这 11 处都在纯 HTTP handler 里(不在控制读循环内联路径上,所以
    //! 不会自锁),真实后果是 HTTP 请求挂到 axum/反向代理超时,期间
    //! `serial_lock(&id)` 一直被持有。
    //!
    //! 断言基于外部可观测行为:**队列满时,操作必须在有限时间内返回**。
    use super::*;

    const CID: &str = "bounded-send-client";

    fn proxy_spec(name: &str, port: u16) -> ProxySpec {
        serde_json::from_value(json!({
            "name": name,
            "kind": "stcp",
            "client_id": CID,
            "bind_ip": "127.0.0.1",
            "bind_port": port,
            "target_host": "127.0.0.1",
            "target_port": 9,
            "enabled": true,
        }))
        .unwrap()
    }

    /// 队列被填满且**无人消费**时,禁用一条规则必须仍然返回。
    ///
    /// 修前:`sess.send()` 在满队列上 park。测试用外层 timeout 把它变成
    /// 可见失败(不是卡死整个测试套件)。修后:`send_bounded` 等 2s 后
    /// 放弃,操作照常返回。
    ///
    /// ★能力边界:这条能区分新旧,因为旧实现是**永久** park(不是慢一点)。
    /// 但它要等 2s 超时,所以测试本身耗时 ~2s。
    #[tokio::test]
    async fn disable_returns_even_when_queue_full() {
        let db = crate::db::Db::open("sqlite::memory:").await.unwrap();
        db.create_client(CID, "c", &"0".repeat(64), "")
            .await
            .unwrap();
        let id = db
            .insert_proxy(CID, &proxy_spec("p", 24500), false, "console")
            .await
            .unwrap();
        let state = Arc::new(AppState::new(crate::config::ServerConfig::default(), db));

        // 注册一个"在线但不消费"的会话:capacity=1,填满即满。
        let (tx, _rx) = tokio::sync::mpsc::channel(1);
        let sess = crate::session::Session::new(
            CID.into(),
            "sid".into(),
            1,
            tokio_util::sync::CancellationToken::new(),
            tx.clone(),
        );
        state.sessions.register(sess);
        // 先把队列填满(第 1 条进队,第 2 条会 park —— 用 try_send 避免自己挂住)
        let _ = tx.try_send(rh_protocol::message::ServerMessage::Pong(
            rh_protocol::message::Pong {
                ts_ms: 0,
                probe: false,
            },
        ));

        // 现在队列满。disable 必须仍能返回(最多等 CONTROL_SEND_TIMEOUT)。
        let fut = disable_service_inner(&state, id);
        let r = tokio::time::timeout(std::time::Duration::from_secs(10), fut)
            .await
            .expect("队列满时 disable 不得永久阻塞");
        assert_eq!(r.0, 0, "disable 应成功: {r:?}");
        // DB 状态已落
        let (_, _, spec, _) = state.db.get_proxy(id).await.unwrap().unwrap();
        assert!(!spec.enabled);
    }

    /// 删除同理(走 delete_service_inner)。
    #[tokio::test]
    async fn delete_returns_even_when_queue_full() {
        let db = crate::db::Db::open("sqlite::memory:").await.unwrap();
        db.create_client(CID, "c", &"0".repeat(64), "")
            .await
            .unwrap();
        let id = db
            .insert_proxy(CID, &proxy_spec("p", 24501), false, "console")
            .await
            .unwrap();
        let state = Arc::new(AppState::new(crate::config::ServerConfig::default(), db));

        let (tx, _rx) = tokio::sync::mpsc::channel(1);
        let sess = crate::session::Session::new(
            CID.into(),
            "sid".into(),
            1,
            tokio_util::sync::CancellationToken::new(),
            tx.clone(),
        );
        state.sessions.register(sess);
        let _ = tx.try_send(rh_protocol::message::ServerMessage::Pong(
            rh_protocol::message::Pong {
                ts_ms: 0,
                probe: false,
            },
        ));

        let fut = delete_service_inner(&state, id);
        let r = tokio::time::timeout(std::time::Duration::from_secs(10), fut)
            .await
            .expect("队列满时 delete 不得永久阻塞");
        assert_eq!(r.0, 0, "delete 应成功: {r:?}");
        assert!(state.db.get_proxy(id).await.unwrap().is_none());
    }

    /// 队列有空间时,操作必须成功且消息真的进队(不能因加了超时就误判)。
    #[tokio::test]
    async fn normal_send_still_delivers() {
        let db = crate::db::Db::open("sqlite::memory:").await.unwrap();
        db.create_client(CID, "c", &"0".repeat(64), "")
            .await
            .unwrap();
        let id = db
            .insert_proxy(CID, &proxy_spec("p", 24502), false, "console")
            .await
            .unwrap();
        let state = Arc::new(AppState::new(crate::config::ServerConfig::default(), db));

        let (tx, mut rx) = tokio::sync::mpsc::channel(16);
        let sess = crate::session::Session::new(
            CID.into(),
            "sid".into(),
            1,
            tokio_util::sync::CancellationToken::new(),
            tx,
        );
        state.sessions.register(sess);

        let r = disable_service_inner(&state, id).await;
        assert_eq!(r.0, 0);
        // 队列里应当有那条 ServiceRemove
        let msg = rx.try_recv().expect("消息必须已入队");
        assert!(
            matches!(msg, rh_protocol::message::ServerMessage::ServiceRemove(_)),
            "应收到 ServiceRemove,实际 {msg:?}"
        );
    }
}

#[cfg(test)]
mod delete_client_concurrency_tests {
    //! P3 回归:`delete_client` 的 teardown 循环必须并发,不得 N × 2s。
    //!
    //! 任务书原以为"构造不出 2s 超时场景"。**这是可以构造的**:
    //! `teardown_runtime` 会 `timeout(2s, serve_handle)` 等 serve loop 收敛。
    //! 只要往 `rt.serve_handle` 里塞一个**永不退出**的任务(且不响应
    //! `rule_cancel`,模拟真实的不收敛),单次 teardown 就必然吃满 2s。
    //!
    //! 断言基于**外部可观测行为**:删除 N 条这种规则的总耗时。串行是
    //! N×2s,并发(8)约 2s。不引用并发度常量。
    use super::*;

    const CID: &str = "del-conc-client";

    fn spec(is: i64) -> ProxySpec {
        serde_json::from_value(json!({
            "name": format!("p{is}"),
            "kind": "stcp",
            "client_id": CID,
            "bind_ip": "127.0.0.1",
            "bind_port": 0,
            "target_host": "127.0.0.1",
            "target_port": 9,
            "enabled": true,
        }))
        .unwrap()
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

    /// 造一个 `serve_handle` = 永不退出的任务。
    async fn stuck_rt(id: u64) -> Arc<crate::state::ProxyRuntime> {
        let rt = Arc::new(crate::state::ProxyRuntime::new(
            id,
            CID.into(),
            spec(id as i64),
        ));
        let h = tokio::spawn(async {
            // 不 select cancel,永不返回 —— 精确模拟"serve loop 不收敛"
            std::future::pending::<()>().await;
        });
        *rt.serve_handle.lock().await = Some(h);
        rt
    }

    /// N 条"卡住的 serve loop":删除总耗时必须是 ~2s 量级,不是 N×2s。
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn delete_many_stuck_rules_does_not_take_n_times_2s() {
        let db = crate::db::Db::open("sqlite::memory:").await.unwrap();
        db.create_client(CID, "c", &"0".repeat(64), "")
            .await
            .unwrap();
        const N: usize = 16;
        let mut ids = Vec::new();
        for i in 0..N {
            let id = db
                .insert_proxy(CID, &spec(i as i64), false, "console")
                .await
                .unwrap();
            ids.push(id);
        }
        let state = Arc::new(AppState::new(crate::config::ServerConfig::default(), db));
        for id in &ids {
            let rt = stuck_rt(*id as u64).await;
            state.proxies.runtimes.insert(*id as u64, rt);
        }
        assert_eq!(state.proxies.runtimes.len(), N);

        let t0 = std::time::Instant::now();
        let r = delete_client(
            State(state.clone()),
            auth_headers(&state).await,
            Path(CID.to_string()),
        )
        .await;
        let elapsed = t0.elapsed();
        let body = axum::body::to_bytes(r.into_body(), usize::MAX)
            .await
            .unwrap();
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["code"], 0, "delete 应成功: {v}");

        // 串行会是 N × 2s = 32s;并发(8)是 2 批 × 2s ≈ 4s。
        // 上限取 10s:明显低于串行的 32s,又给调度留余量。
        assert!(
            elapsed < std::time::Duration::from_secs(10),
            "删除 {N} 条卡住规则耗时 {elapsed:?} —— 串行会是 {N}×2s=32s,必须并发"
        );
        // 功能性:全部运行态必须消失
        assert!(
            state.proxies.runtimes.is_empty(),
            "删除后不得残留运行态,实际 {}",
            state.proxies.runtimes.len()
        );
        assert!(state.db.list_proxies(Some(CID)).await.unwrap().is_empty());
    }

    /// 功能性回归:普通(无卡住 serve loop)的客户端删除后运行态清空。
    #[tokio::test]
    async fn delete_plain_client_clears_runtimes() {
        let db = crate::db::Db::open("sqlite::memory:").await.unwrap();
        db.create_client(CID, "c", &"0".repeat(64), "")
            .await
            .unwrap();
        let mut ids = Vec::new();
        for i in 0..10 {
            let id = db
                .insert_proxy(CID, &spec(i), false, "console")
                .await
                .unwrap();
            ids.push(id);
        }
        let state = Arc::new(AppState::new(crate::config::ServerConfig::default(), db));
        for id in &ids {
            state.proxies.runtimes.insert(
                *id as u64,
                Arc::new(crate::state::ProxyRuntime::new(
                    *id as u64,
                    CID.into(),
                    spec(*id),
                )),
            );
        }
        let r = delete_client(
            State(state.clone()),
            auth_headers(&state).await,
            Path(CID.to_string()),
        )
        .await;
        let body = axum::body::to_bytes(r.into_body(), usize::MAX)
            .await
            .unwrap();
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["code"], 0);
        assert!(state.proxies.runtimes.is_empty());
    }
}

#[cfg(test)]
mod list_services_tests {
    //! P5 回归:`list_services` 把 4 次 `proxies.get` 合并为 1 次后,
    //! 返回的字段必须与修前完全一致。
    //!
    //! ★能力边界:这条**测不出性能差异**(4 次 dashmap 查找 vs 1 次是
    //! 微秒级)。它的价值是保证合并没有弄丢/弄错任何字段 —— 特别是
    //! `healthy` 那一处,原来用的是**独立的第 4 次 get**,合并时最容易漏改。
    use super::*;

    const CID: &str = "list-svc-client";

    fn spec(name: &str, port: u16, kind: &str) -> ProxySpec {
        serde_json::from_value(json!({
            "name": name,
            "kind": kind,
            "client_id": CID,
            "bind_ip": "127.0.0.1",
            "bind_port": port,
            "target_host": "10.0.0.1",
            "target_port": 8080,
            "enabled": true,
            "stcp": { "secret_key": "should-be-hidden" },
        }))
        .unwrap()
    }

    async fn auth(state: &Arc<AppState>) -> HeaderMap {
        let token = mint_jwt(state, "admin", 1, 0).await.unwrap();
        let mut h = HeaderMap::new();
        h.insert(
            "authorization",
            axum::http::HeaderValue::from_str(&format!("Bearer {token}")).unwrap(),
        );
        h
    }

    async fn call_list(state: &Arc<AppState>) -> Vec<serde_json::Value> {
        let resp = list_services(
            State(state.clone()),
            auth(state).await,
            Query(ListQuery { client_id: None }),
        )
        .await;
        let b = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
        v["data"].as_array().cloned().unwrap_or_default()
    }

    /// 运行态存在时:status / healthy / 计量字段都必须正确。
    #[tokio::test]
    async fn fields_reflect_runtime() {
        let db = crate::db::Db::open("sqlite::memory:").await.unwrap();
        db.create_client(CID, "c", &"0".repeat(64), "")
            .await
            .unwrap();
        let id = db
            .insert_proxy(CID, &spec("s1", 24600, "stcp"), false, "console")
            .await
            .unwrap();
        let state = Arc::new(AppState::new(crate::config::ServerConfig::default(), db));

        let rt = Arc::new(crate::state::ProxyRuntime::new(
            id as u64,
            CID.into(),
            spec("s1", 24600, "stcp"),
        ));
        rt.set_state(ProxyState::Running, "test").await;
        rt.healthy
            .store(false, std::sync::atomic::Ordering::Relaxed);
        rt.bytes_in.store(111, std::sync::atomic::Ordering::Relaxed);
        rt.bytes_out
            .store(222, std::sync::atomic::Ordering::Relaxed);
        rt.conns_current
            .store(3, std::sync::atomic::Ordering::Relaxed);
        state.proxies.runtimes.insert(id as u64, rt);

        let rows = call_list(&state).await;
        assert_eq!(rows.len(), 1);
        let r = &rows[0];
        assert_eq!(r["id"], id);
        assert_eq!(r["status"], "running");
        assert_eq!(r["status_reason"], "test");
        assert_eq!(r["healthy"], false, "healthy 必须反映运行态(false)");
        assert_eq!(r["bytes_in"], 111);
        assert_eq!(r["bytes_out"], 222);
        assert_eq!(r["conns_current"], 3);
    }

    /// 运行态不存在(已禁用):status=disabled、healthy 默认 true、计量为 0。
    ///
    /// 这条专门盯 `healthy` —— 它原本是**独立的第 4 次 get**,合并时
    /// 最容易被漏改成别的变量。
    #[tokio::test]
    async fn absent_runtime_uses_defaults() {
        let db = crate::db::Db::open("sqlite::memory:").await.unwrap();
        db.create_client(CID, "c", &"0".repeat(64), "")
            .await
            .unwrap();
        let id = db
            .insert_proxy(CID, &spec("s2", 24601, "stcp"), false, "console")
            .await
            .unwrap();
        let state = Arc::new(AppState::new(crate::config::ServerConfig::default(), db));

        let rows = call_list(&state).await;
        assert_eq!(rows.len(), 1);
        let r = &rows[0];
        assert_eq!(r["id"], id);
        assert_eq!(r["status"], "disabled");
        assert_eq!(r["healthy"], true, "运行态不存在时 healthy 默认为 true");
        assert_eq!(r["bytes_in"], 0);
        assert_eq!(r["bytes_out"], 0);
        assert_eq!(r["conns_current"], 0);
        assert_eq!(r["pool_ready"], 0);
    }

    /// secret_key 必须被抹掉(与本次合并无关,但同一条回归路径)。
    #[tokio::test]
    async fn secret_key_redacted() {
        let db = crate::db::Db::open("sqlite::memory:").await.unwrap();
        db.create_client(CID, "c", &"0".repeat(64), "")
            .await
            .unwrap();
        db.insert_proxy(CID, &spec("s3", 24602, "stcp"), false, "console")
            .await
            .unwrap();
        let state = Arc::new(AppState::new(crate::config::ServerConfig::default(), db));
        let rows = call_list(&state).await;
        assert_eq!(
            rows[0]["spec"]["stcp"]["secret_key"], "",
            "列表不得回显 secret_key"
        );
    }

    /// 多条规则:每条都要有正确的计量(不能因复用 rt 变量而串号)。
    #[tokio::test]
    async fn multiple_rows_have_independent_meters() {
        let db = crate::db::Db::open("sqlite::memory:").await.unwrap();
        db.create_client(CID, "c", &"0".repeat(64), "")
            .await
            .unwrap();
        let mut ids = Vec::new();
        for i in 0..5u16 {
            let id = db
                .insert_proxy(
                    CID,
                    &spec(&format!("m{i}"), 24700 + i, "stcp"),
                    false,
                    "console",
                )
                .await
                .unwrap();
            ids.push(id);
        }
        let state = Arc::new(AppState::new(crate::config::ServerConfig::default(), db));
        // 每条给不同的 bytes_in
        for (k, id) in ids.iter().enumerate() {
            let rt = Arc::new(crate::state::ProxyRuntime::new(
                *id as u64,
                CID.into(),
                spec(&format!("m{k}"), 24700 + k as u16, "stcp"),
            ));
            rt.bytes_in
                .store((k as u64 + 1) * 100, std::sync::atomic::Ordering::Relaxed);
            state.proxies.runtimes.insert(*id as u64, rt);
        }
        let rows = call_list(&state).await;
        assert_eq!(rows.len(), 5);
        for (k, r) in rows.iter().enumerate() {
            assert_eq!(
                r["bytes_in"],
                (k as u64 + 1) * 100,
                "第 {k} 条的计量串号了 —— rt 复用必须逐条重新取"
            );
        }
    }
}

#[cfg(test)]
mod list_services_bench {
    //! P5 基准(手动运行):4 次 `proxies.get` vs 1 次。
    //!
    //! ```text
    //! cargo test -p rh-server --release list_services_bench -- --ignored --nocapture
    //! ```
    //!
    //! 直接测 dashmap 查找本身(不经过 HTTP/JSON),隔离出本项改动的那一部分。
    //! 如实预期:这是**微秒级**优化,报告里原本就把它列为"可选"。
    use super::*;

    fn spec(name: &str, port: u16) -> ProxySpec {
        serde_json::from_value(json!({
            "name": name,
            "kind": "stcp",
            "client_id": "bench",
            "bind_ip": "127.0.0.1",
            "bind_port": port,
            "target_host": "127.0.0.1",
            "target_port": 9,
            "enabled": true,
        }))
        .unwrap()
    }

    #[tokio::test]
    #[ignore = "耗时基准,手动运行(--ignored --nocapture)"]
    async fn bench_get_once_vs_four_times() {
        for n in [100usize, 500, 1000] {
            let db = crate::db::Db::open("sqlite::memory:").await.unwrap();
            let state = Arc::new(AppState::new(crate::config::ServerConfig::default(), db));
            for i in 0..n as u64 {
                state.proxies.runtimes.insert(
                    i + 1,
                    Arc::new(crate::state::ProxyRuntime::new(
                        i + 1,
                        "bench".into(),
                        spec(&format!("p{i}"), 0),
                    )),
                );
            }
            let reps = 20;
            // 4 次查找(修前)
            let t0 = std::time::Instant::now();
            for _ in 0..reps {
                for i in 1..=n as u64 {
                    let _ = state.proxies.get(i);
                    let _ = state.proxies.get(i);
                    let _ = state.proxies.get(i);
                    let _ = state.proxies.get(i);
                }
            }
            let four = t0.elapsed() / reps;

            // 1 次查找(修后)
            let t1 = std::time::Instant::now();
            for _ in 0..reps {
                for i in 1..=n as u64 {
                    let _ = state.proxies.get(i);
                }
            }
            let one = t1.elapsed() / reps;
            println!(
                "N={n:>5}  4×get={four:>12?}  1×get={one:>12?}  省={:?}",
                four.saturating_sub(one)
            );
        }
    }
}

#[cfg(test)]
mod secret_key_management_tests {
    //! 密钥访问规则的凭据完全服务端管理 —— 这条语义原先**只由 e2e 覆盖**
    //! (`tests/e2e.sh` 读 `/api/services` 的 `spec.stcp.secret_key` 断言 32 hex)。
    //
    //! `07b1aa2`(安全加固)让列表接口抹掉 `secret_key` 之后,e2e 那条断言
    //! 恒为 0 长度而失败。但**语义本身没有变**,变的是「从哪个口径能观察到它」:
    //!   - 现在没有任何 API 再返回库中的密钥(创建响应不含、列表抹除、轮换响应
    //!     返回的是**新**值而不是旧值);
    //!   - 于是这条语义必须在**服务端内部**接住,而不是把 e2e 断言删掉了事。
    //!
    //! 这里直接驱动 `create_service_inner` / `rotate_service_secret`,断言落在
    //! 「库里存的是什么」—— 这是密钥语义的**唯一事实源**(转发时用的就是它)。
    //! e2e 侧改为验证它仍能验证的、外部可观察的性质(列表不回显),见
    //! `tests/e2e.sh` 的 STCP 段。
    use super::*;

    const CID: &str = "secret-mgmt-client";

    async fn state() -> Arc<AppState> {
        let db = crate::db::Db::open("sqlite::memory:").await.unwrap();
        db.create_client(CID, "c", &"0".repeat(64), "")
            .await
            .unwrap();
        Arc::new(AppState::new(crate::config::ServerConfig::default(), db))
    }

    fn stcp_spec(name: &str, secret: &str) -> ProxySpec {
        serde_json::from_value(json!({
            "name": name,
            "kind": "stcp",
            "client_id": CID,
            "bind_ip": "127.0.0.1",
            "bind_port": 0,
            "target_host": "127.0.0.1",
            "target_port": 9,
            "enabled": true,
            "stcp": { "secret_key": secret },
        }))
        .unwrap()
    }

    /// 32 位小写 hex 的判定(不放宽:长度 + 字符集都要满足)。
    fn is_32_hex(s: &str) -> bool {
        s.len() == 32
            && s.bytes()
                .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
    }

    /// 创建:入参提供的 secret_key 必须被**忽略**,库中是服务端生成的 32 hex。
    #[tokio::test]
    async fn create_ignores_caller_secret_and_generates_32_hex() {
        let st = state().await;
        let mut spec = stcp_spec("sdb", "client-side-should-be-ignored");
        let v = create_service_inner(&st, &mut spec).await.unwrap();
        let id = v["id"].as_i64().unwrap();

        let (_, _, stored, _) = st.db.get_proxy(id).await.unwrap().unwrap();
        let got = stored.secret_cfg().unwrap().secret_key.clone();
        assert_ne!(
            got, "client-side-should-be-ignored",
            "调用方提供的 secret_key 必须被忽略"
        );
        assert!(
            is_32_hex(&got),
            "库中密钥必须是服务端随机生成的 32 位小写 hex,实际是 {got:?}(长度 {})",
            got.len()
        );
    }

    /// 每次创建都不同(随机性):防止退化成固定值。
    #[tokio::test]
    async fn generated_secrets_are_distinct_across_creates() {
        let st = state().await;
        let mut a = stcp_spec("a", "");
        let mut b = stcp_spec("b", "");
        create_service_inner(&st, &mut a).await.unwrap();
        create_service_inner(&st, &mut b).await.unwrap();
        let rows = st.db.list_proxies(Some(CID)).await.unwrap();
        let secrets: Vec<String> = rows
            .iter()
            .map(|(_, _, s)| s.secret_cfg().unwrap().secret_key.clone())
            .collect();
        assert_eq!(secrets.len(), 2);
        assert_ne!(secrets[0], secrets[1], "两条规则的密钥不得相同");
        assert!(secrets.iter().all(|s| is_32_hex(s)));
    }

    /// 轮换:必须是**新**的 32 hex,且落库(转发时读的就是库里的值)。
    #[tokio::test]
    async fn rotate_generates_new_32_hex_and_persists() {
        let st = state().await;
        let mut spec = stcp_spec("sdb2", "");
        let v = create_service_inner(&st, &mut spec).await.unwrap();
        let id = v["id"].as_i64().unwrap();
        let before = st
            .db
            .get_proxy(id)
            .await
            .unwrap()
            .unwrap()
            .2
            .secret_cfg()
            .unwrap()
            .secret_key
            .clone();

        let resp =
            rotate_service_secret(State(st.clone()), auth_headers(&st).await, Path(id)).await;
        let b = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&b).unwrap();
        let returned = body["data"]["secret"].as_str().unwrap().to_string();

        let after = st
            .db
            .get_proxy(id)
            .await
            .unwrap()
            .unwrap()
            .2
            .secret_cfg()
            .unwrap()
            .secret_key
            .clone();

        assert_ne!(before, after, "轮换必须换掉库中的密钥");
        assert!(
            is_32_hex(&after),
            "轮换后库中必须是新的 32 hex,实际 {after:?}"
        );
        assert_eq!(returned, after, "轮换响应返回的必须是新值(且只此一次)");
    }

    /// 编辑:入参空的 secret_key 表示「不修改」,原密钥必须保留。
    /// (这是列表接口敢于抹除密钥的前提 —— 前端拿不到值也不会把它清空。)
    #[tokio::test]
    async fn update_with_empty_secret_keeps_existing() {
        let st = state().await;
        let mut spec = stcp_spec("keep", "");
        let v = create_service_inner(&st, &mut spec).await.unwrap();
        let id = v["id"].as_i64().unwrap() as u64;
        let before = st
            .db
            .get_proxy(id as i64)
            .await
            .unwrap()
            .unwrap()
            .2
            .secret_cfg()
            .unwrap()
            .secret_key
            .clone();

        // 模拟前端:从列表拿到的是空 secret_key,原样回传
        let mut roundtrip = stcp_spec("keep", "");
        roundtrip.id = id;
        update_service_inner(&st, id as i64, roundtrip).await;

        let after = st
            .db
            .get_proxy(id as i64)
            .await
            .unwrap()
            .unwrap()
            .2
            .secret_cfg()
            .unwrap()
            .secret_key
            .clone();
        assert_eq!(after, before, "空 secret_key 的编辑必须保留原密钥");
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
}

#[cfg(test)]
mod update_client_teardown_tests {
    //! Q1 回归:`update_client` 禁用分支的 teardown 循环必须并发,
    //! 不得 N × 2s。
    //!
    //! 与 `delete_client_concurrency_tests`(上一轮 P3)同一套构造:
    //! 往 `rt.serve_handle` 塞一个**永不返回且不响应 `rule_cancel`** 的任务,
    //! 精确模拟"serve loop 不收敛",单次 teardown 必然吃满 2s。
    //!
    //! 断言基于外部可观测行为:总耗时 + 最终状态。不引用并发度常量。
    use super::*;
    use axum::extract::Path as AxumPath;

    const CID: &str = "upd-teardown-client";

    fn proxy_spec(i: i64) -> ProxySpec {
        serde_json::from_value(json!({
            "name": format!("p{i}"),
            "kind": "stcp",
            "client_id": CID,
            "bind_ip": "127.0.0.1",
            "bind_port": 0,
            "target_host": "127.0.0.1",
            "target_port": 9,
            "enabled": true,
        }))
        .unwrap()
    }

    async fn auth(state: &Arc<AppState>) -> HeaderMap {
        let token = mint_jwt(state, "admin", 1, 0).await.unwrap();
        let mut h = HeaderMap::new();
        h.insert(
            "authorization",
            axum::http::HeaderValue::from_str(&format!("Bearer {token}")).unwrap(),
        );
        h
    }

    async fn stuck_rt(id: u64) -> Arc<crate::state::ProxyRuntime> {
        let rt = Arc::new(crate::state::ProxyRuntime::new(
            id,
            CID.into(),
            proxy_spec(id as i64),
        ));
        let h = tokio::spawn(async {
            std::future::pending::<()>().await;
        });
        *rt.serve_handle.lock().await = Some(h);
        rt
    }

    /// 16 条卡住规则 + 禁用客户端:总耗时必须是 ~2s 量级,不是 16×2s。
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn disable_client_does_not_take_n_times_2s() {
        let db = crate::db::Db::open("sqlite::memory:").await.unwrap();
        db.create_client(CID, "c", &"0".repeat(64), "")
            .await
            .unwrap();
        const N: usize = 16;
        let mut ids = Vec::new();
        for i in 0..N {
            let id = db
                .insert_proxy(CID, &proxy_spec(i as i64), false, "console")
                .await
                .unwrap();
            ids.push(id);
        }
        let state = Arc::new(AppState::new(crate::config::ServerConfig::default(), db));
        for id in &ids {
            let rt = stuck_rt(*id as u64).await;
            state.proxies.runtimes.insert(*id as u64, rt);
        }

        let t0 = std::time::Instant::now();
        let r = update_client(
            State(state.clone()),
            auth(&state).await,
            AxumPath(CID.to_string()),
            Json(UpdateClientReq {
                enabled: Some(false),
                name: None,
                remark: None,
                rate_limit: None,
                max_conns: None,
                max_services: None,
                flow_limit: None,
            }),
        )
        .await;
        let elapsed = t0.elapsed();
        let body = axum::body::to_bytes(r.into_body(), usize::MAX)
            .await
            .unwrap();
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["code"], 0, "update 应成功: {v}");

        // 串行是 16 × 2s = 32s;并发(8)是 2 批 × 2s ≈ 4s。
        // 上限取 10s:明显低于 32s,又给调度留余量。
        assert!(
            elapsed < std::time::Duration::from_secs(10),
            "禁用 {N} 条卡住规则耗时 {elapsed:?} —— 串行会是 {N}×2s=32s,必须并发"
        );
        // 功能性:全部运行态必须消失
        assert!(
            state.proxies.runtimes.is_empty(),
            "禁用后不得残留运行态,实际 {}",
            state.proxies.runtimes.len()
        );
        // 客户端本身必须被置为禁用。
        // (注意:`set_client_enabled` 只改 clients 表,不改 proxies 行的
        //  enabled —— 规则行保留自己的 enabled,客户端禁用期间不恢复。
        //  这里断言的是 clients 表,不是 proxy 行。)
        let c = state.db.get_client(CID).await.unwrap().unwrap();
        assert!(!c.enabled, "clients 表必须已置 disabled");
    }

    /// 功能性回归:普通客户端禁用后运行态清空,且 REMOVE 帧真的入队。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn disable_plain_client_clears_and_sends_remove() {
        let db = crate::db::Db::open("sqlite::memory:").await.unwrap();
        db.create_client(CID, "c", &"0".repeat(64), "")
            .await
            .unwrap();
        let mut ids = Vec::new();
        for i in 0..5 {
            let id = db
                .insert_proxy(CID, &proxy_spec(i), false, "console")
                .await
                .unwrap();
            ids.push(id);
        }
        let state = Arc::new(AppState::new(crate::config::ServerConfig::default(), db));
        for id in &ids {
            state.proxies.runtimes.insert(
                *id as u64,
                Arc::new(crate::state::ProxyRuntime::new(
                    *id as u64,
                    CID.into(),
                    proxy_spec(*id),
                )),
            );
        }
        // 注册在线会话,收 REMOVE 帧
        let (tx, mut rx) = tokio::sync::mpsc::channel(64);
        state.sessions.register(crate::session::Session::new(
            CID.into(),
            "sid".into(),
            1,
            tokio_util::sync::CancellationToken::new(),
            tx,
        ));

        let r = update_client(
            State(state.clone()),
            auth(&state).await,
            AxumPath(CID.to_string()),
            Json(UpdateClientReq {
                enabled: Some(false),
                name: None,
                remark: None,
                rate_limit: None,
                max_conns: None,
                max_services: None,
                flow_limit: None,
            }),
        )
        .await;
        let body = axum::body::to_bytes(r.into_body(), usize::MAX)
            .await
            .unwrap();
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["code"], 0);

        assert!(state.proxies.runtimes.is_empty(), "运行态必须清空");
        // 5 条 REMOVE 必须全部入队(拆成两个循环后不能漏发)
        let mut n_remove = 0;
        while let Ok(m) = rx.try_recv() {
            if matches!(m, rh_protocol::message::ServerMessage::ServiceRemove(_)) {
                n_remove += 1;
            }
        }
        assert_eq!(n_remove, 5, "每条规则都必须发 REMOVE,实际 {n_remove}");
    }
}

#[cfg(test)]
mod remove_send_concurrency_tests {
    //! Q4 回归:REMOVE 帧并发发送后,送达条数与串行**完全相同**。
    //!
    //! 关键实测(见 `update_client` 里的注释表格):`send_bounded` 不重试,
    //! 队列容量 k 就决定能送几条,送完后串行和并发都只剩超时。所以并发化
    //! 不降低送达率,只降低耗时。
    //!
    //! ★能测什么:队列充足时"一条不少";队列不足时"送满容量"。
    //! ★测不了什么:耗时(N 条 × 2s vs ⌈N/8⌉ × 2s 需要真的等超时,
    //! 那会让单测跑几十秒)。耗时口径在 `update_client_teardown_tests`
    //! 的卡住场景里已经覆盖了 teardown 侧,发送侧的耗时由上面的实测表格
    //! 说明,不在单测里重复。
    use super::*;
    use axum::extract::Path as AxumPath;

    const CID: &str = "rm-conc-client";

    fn proxy_spec(i: i64) -> ProxySpec {
        serde_json::from_value(json!({
            "name": format!("p{i}"),
            "kind": "stcp",
            "client_id": CID,
            "bind_ip": "127.0.0.1",
            "bind_port": 0,
            "target_host": "127.0.0.1",
            "target_port": 9,
            "enabled": true,
        }))
        .unwrap()
    }

    async fn auth(state: &Arc<AppState>) -> HeaderMap {
        let token = mint_jwt(state, "admin", 1, 0).await.unwrap();
        let mut h = HeaderMap::new();
        h.insert(
            "authorization",
            axum::http::HeaderValue::from_str(&format!("Bearer {token}")).unwrap(),
        );
        h
    }

    async fn setup(n_proxies: usize, n_visitors: usize, cap: usize) -> (Arc<AppState>, MsgRx) {
        let db = crate::db::Db::open("sqlite::memory:").await.unwrap();
        db.create_client(CID, "c", &"0".repeat(64), "")
            .await
            .unwrap();
        for i in 0..n_proxies {
            db.insert_proxy(CID, &proxy_spec(i as i64), false, "console")
                .await
                .unwrap();
        }
        for i in 0..n_visitors {
            // visitor name 有 UNIQUE(client_id, name) 约束,必须逐条给不同名字
            let v: rh_protocol::VisitorSpec = serde_json::from_value(json!({
                "name": format!("v{i}"),
                "kind": "stcp",
                "enabled": true,
                "bind_addr": "127.0.0.1",
                "bind_port": 24800 + i as u16,
                "target_proxy_name": "p0",
            }))
            .unwrap();
            db.insert_visitor(CID, CID, &v).await.unwrap();
        }
        let state = Arc::new(AppState::new(crate::config::ServerConfig::default(), db));
        let (tx, rx) = tokio::sync::mpsc::channel(cap);
        state.sessions.register(crate::session::Session::new(
            CID.into(),
            "sid".into(),
            1,
            tokio_util::sync::CancellationToken::new(),
            tx,
        ));
        (state, rx)
    }

    type MsgRx = tokio::sync::mpsc::Receiver<rh_protocol::message::ServerMessage>;

    async fn disable(state: &Arc<AppState>) {
        let r = update_client(
            State(state.clone()),
            auth(state).await,
            AxumPath(CID.to_string()),
            Json(UpdateClientReq {
                enabled: Some(false),
                name: None,
                remark: None,
                rate_limit: None,
                max_conns: None,
                max_services: None,
                flow_limit: None,
            }),
        )
        .await;
        let b = axum::body::to_bytes(r.into_body(), usize::MAX)
            .await
            .unwrap();
        let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
        assert_eq!(v["code"], 0, "disable 应成功: {v}");
    }

    /// 队列充足:proxies + visitors 的 REMOVE 必须**一条不少**。
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn all_removes_delivered_when_queue_has_room() {
        let (state, mut rx) = setup(10, 5, 256).await;
        disable(&state).await;

        let mut proxies = 0;
        let mut visitors = 0;
        while let Ok(m) = rx.try_recv() {
            if let rh_protocol::message::ServerMessage::ServiceRemove(r) = m {
                assert!(r.entity_type == "proxy" || r.entity_type == "visitor");
                if r.entity_type == "proxy" {
                    proxies += 1;
                } else {
                    visitors += 1;
                }
            }
        }
        assert_eq!(proxies, 10, "10 条 proxy REMOVE 必须全部送达");
        assert_eq!(visitors, 5, "5 条 visitor REMOVE 必须全部送达");
    }

    /// 队列不足:并发送达数 = 队列容量(与串行相同 —— 这是 Q4 判断的核心)。
    ///
    /// 容量 4、共 12 条 REMOVE:只可能送到 4 条,其余超时丢弃。
    /// 断言"恰好等于容量",不是"等于总数" —— 这条盯的正是"并发不会额外丢"。
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn delivery_count_equals_queue_capacity_when_short() {
        let (state, mut rx) = setup(12, 0, 4).await;
        disable(&state).await;
        let mut got = 0;
        while rx.try_recv().is_ok() {
            got += 1;
        }
        assert_eq!(
            got, 4,
            "队列容量 4 时应恰好送达 4 条(串行也是 4 —— send_bounded 不重试)"
        );
    }
}
