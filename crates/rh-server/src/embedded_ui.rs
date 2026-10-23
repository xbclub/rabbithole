//! 内嵌 Web 控制台服务。
//!
//! 构建期(build.rs)检测 `console/dist` 存在时置 `embed_ui`,rust-embed
//! 将整个 dist 编译进二进制;运行期 `[web] static_dir` 配置优先(显式配置
//! 视为有意覆盖),未配置时回退内嵌资产。
//! SPA 回退语义与磁盘分支一致:未知 `/api/*` 返回 JSON 404 envelope,
//! 其余未知路径回退 index.html 并返回 200(history 路由可用)。

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

#[cfg(embed_ui)]
#[derive(rust_embed::RustEmbed)]
#[folder = "../../console/dist"]
struct EmbeddedUi;

/// 本二进制是否内嵌了 Web 控制台。
pub fn available() -> bool {
    cfg!(embed_ui)
}

/// 内嵌文件数(日志用);未内嵌返回 0。
pub fn file_count() -> usize {
    #[cfg(embed_ui)]
    {
        return EmbeddedUi::iter().count();
    }
    #[allow(unreachable_code)]
    0
}

#[cfg(embed_ui)]
fn mime_of(path: &str) -> &'static str {
    match path.rsplit('.').next().unwrap_or("") {
        "css" => "text/css; charset=utf-8",
        "html" => "text/html; charset=utf-8",
        "ico" => "image/x-icon",
        "js" | "mjs" => "text/javascript; charset=utf-8",
        "json" | "map" => "application/json; charset=utf-8",
        "png" => "image/png",
        "svg" => "image/svg+xml",
        "txt" => "text/plain; charset=utf-8",
        "webp" => "image/webp",
        "woff" => "font/woff",
        "woff2" => "font/woff2",
        _ => "application/octet-stream",
    }
}

fn asset_response(path: &str) -> Option<Response> {
    #[cfg(embed_ui)]
    {
        let rel = path.trim_start_matches('/');
        let (name, file) = if rel.is_empty() {
            ("index.html", EmbeddedUi::get("index.html")?)
        } else {
            match EmbeddedUi::get(rel) {
                Some(f) => (rel, f),
                // SPA 深层路由:回退 index.html(200)
                None => ("index.html", EmbeddedUi::get("index.html")?),
            }
        };
        // R2:rust-embed 的 data 是 Cow<'static, _>(release 内嵌 =
        // Borrowed 静态切片,零拷贝出响应;开发态 fs 加载 = Owned,
        // 直接移动 Vec)。修前 Vec::from 每个管理页请求整份拷贝。
        return Some(match file.data {
            std::borrow::Cow::Borrowed(b) => {
                (StatusCode::OK, [("content-type", mime_of(name))], b).into_response()
            }
            std::borrow::Cow::Owned(v) => {
                (StatusCode::OK, [("content-type", mime_of(name))], v).into_response()
            }
        });
    }
    #[allow(unreachable_code)]
    {
        let _ = path;
        None
    }
}

/// 兜底路由处理器:语义对齐 main.rs 的 static_dir 分支。
pub async fn handle(path: &str) -> Response {
    if path.starts_with("/api/") {
        return crate::api::err(404, "not found");
    }
    asset_response(path).unwrap_or_else(|| StatusCode::NOT_FOUND.into_response())
}
