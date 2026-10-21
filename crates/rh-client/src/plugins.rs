//! 客户端插件(10 种):本地目标拨号的替代实现。
//!
//! 结构:插件返回一对 duplex 的通道侧;协议处理任务持有另一侧并连接
//! 上游,完成握手/终结/改写后纯搬运。HTTP 改写类插件对请求头做
//! Host 重写 + HeaderOp + `Connection: close`(每连接单请求,语义正确)。

use base64::Engine as _;
use rh_protocol::message::StartForward;
use rh_protocol::HeaderOp;
use rh_protocol::PluginSpec;
use rh_transport::BoxedIo;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// 按插件规格建立本地连接。
pub async fn dial_plugin(plugin: &PluginSpec, fwd: &StartForward) -> anyhow::Result<BoxedIo> {
    match plugin {
        PluginSpec::UnixDomainSocket { unix_path } => {
            #[cfg(unix)]
            {
                let s = tokio::net::UnixStream::connect(unix_path).await?;
                Ok(Box::new(s))
            }
            #[cfg(not(unix))]
            {
                let _ = unix_path;
                anyhow::bail!("unix_domain_socket unsupported on this platform")
            }
        }
        PluginSpec::HttpProxy {
            http_user,
            http_password,
        } => dial_http_proxy(fwd, http_user, http_password).await,
        PluginSpec::Socks5 { user, password } => dial_socks5(fwd, user, password).await,
        PluginSpec::StaticFile {
            local_path,
            strip_prefix,
            http_user,
            http_password,
        } => {
            let (a, b) = tokio::io::duplex(64 * 1024);
            tokio::spawn(serve_static_file(
                b,
                std::path::PathBuf::from(local_path),
                strip_prefix.clone(),
                basic_auth(http_user, http_password),
            ));
            Ok(Box::new(a))
        }
        PluginSpec::Https2Http {
            local_addr,
            crt_path,
            key_path,
            host_rewrite,
            request_headers,
        }
        | PluginSpec::Https2Https {
            local_addr,
            crt_path,
            key_path,
            host_rewrite,
            request_headers,
        } => {
            let tls = tls_acceptor(crt_path, key_path)?;
            let upstream_tls = matches!(plugin, PluginSpec::Https2Https { .. });
            let (a, b) = tokio::io::duplex(64 * 1024);
            tokio::spawn(serve_tls_term(
                tls,
                b,
                local_addr.clone(),
                upstream_tls,
                host_rewrite.clone(),
                request_headers.clone(),
            ));
            Ok(Box::new(a))
        }
        PluginSpec::Http2Https {
            local_addr,
            host_rewrite,
            request_headers,
        } => dial_rewrite_upstream(local_addr, true, host_rewrite, request_headers).await,
        PluginSpec::Http2Http {
            local_addr,
            host_rewrite,
            request_headers,
        } => dial_rewrite_upstream(local_addr, false, host_rewrite, request_headers).await,
        PluginSpec::Tls2Raw {
            local_addr,
            crt_path,
            key_path,
        } => {
            let tls = tls_acceptor(crt_path, key_path)?;
            let local_addr = local_addr.to_string();
            let (a, b) = tokio::io::duplex(64 * 1024);
            tokio::spawn(async move {
                let Ok(tls_stream) = tls.accept(b).await else {
                    return;
                };
                let Ok(up) = tokio::net::TcpStream::connect(&local_addr).await else {
                    return;
                };
                let mut t = tls_stream;
                let mut u = up;
                let _ = tokio::io::copy_bidirectional(&mut t, &mut u).await;
            });
            Ok(Box::new(a))
        }
    }
}

fn basic_auth(user: &str, pass: &str) -> Option<(String, String)> {
    if user.is_empty() {
        None
    } else {
        Some((user.to_string(), pass.to_string()))
    }
}

/// C8:插件拨号解析必须走异步路径。
///
/// 修前 `to_socket_addrs()` 在 async 上下文里同步执行 getaddrinfo ——
/// DNS 故障时会把 tokio worker 整个阻塞(几十秒),期间该 worker 上
/// 所有转发任务停摆。这里复用 `forward::resolve`(内部 spawn_blocking
/// + 自定义 DNS 支持)。
async fn resolve_addr(host: &str, port: u16) -> anyhow::Result<std::net::SocketAddr> {
    crate::forward::resolve(host, port).await
}

/// C8:插件拨号超时。DNS 慢 + connect 慢的叠加必须有界。
const PLUGIN_DIAL_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

async fn connect_with_timeout(addr: std::net::SocketAddr) -> anyhow::Result<tokio::net::TcpStream> {
    match tokio::time::timeout(PLUGIN_DIAL_TIMEOUT, tokio::net::TcpStream::connect(addr)).await {
        Ok(r) => Ok(r?),
        Err(_) => Err(anyhow::anyhow!("plugin dial timeout: {addr}")),
    }
}

// ---------------- HTTP 头读取(三处共用) ----------------

/// 读一个 HTTP 头(含结尾的 `\r\n\r\n`),最多 `limit` 字节。
///
/// 三处调用点原先都是 `read_exact(&mut [0u8;1])` 单字节循环:一个 300 字节
/// 的请求头要推 300 次 read future,是每请求实打实的固定成本。这里改成
/// 一次读入 4KiB、在缓冲内找 `\r\n\r\n`,没找到再续读。
///
/// 语义与旧实现保持:
/// * 返回值**包含**头部结束标记(调用方按整块做改写/解析);
/// * 超过 `limit` 仍未见到结束标记 → `Err`,**绝不无限读**;
/// * 连接在头部未收全时 EOF → `Err`(旧实现是 `read_exact` 的
///   `UnexpectedEof`,同样按错误上抛)。
///
/// 三处上限不同,由调用方传入:代理 CONNECT 应答 8KiB、static_file 与
/// 改写路径各 64KiB。
async fn read_http_head<R>(io: &mut R, limit: usize) -> std::io::Result<Vec<u8>>
where
    R: tokio::io::AsyncRead + Unpin,
{
    /// 单次读取块大小:一个典型请求头(300~800B)一次就能读完。
    const CHUNK: usize = 4096;
    let mut buf: Vec<u8> = Vec::with_capacity(CHUNK.min(limit));
    let mut chunk = [0u8; CHUNK];
    loop {
        // 读数钳到「limit + 1」以内:一旦越过 limit 且仍无结束标记就判超限,
        // 与旧实现「push 第 limit+1 字节后 bail」逐字节等价(不会多吃数据)。
        let want = (limit + 1).saturating_sub(buf.len()).min(CHUNK);
        let n = io.read(&mut chunk[..want]).await?;
        if n == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "connection closed before HTTP head completed",
            ));
        }
        buf.extend_from_slice(&chunk[..n]);
        // 结束标记可能跨块,故从本次新增字节点回退 3 字节重新扫。
        let scan_from = buf.len().saturating_sub(n + 3);
        if buf[scan_from..].windows(4).any(|w| w == b"\r\n\r\n") {
            return Ok(buf);
        }
        if buf.len() > limit {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("HTTP head exceeds {limit} bytes"),
            ));
        }
    }
}

// ---------------- http_proxy / socks5(经本地代理出站) ----------------

async fn dial_http_proxy(fwd: &StartForward, user: &str, pass: &str) -> anyhow::Result<BoxedIo> {
    // 目标字段承载本地代理地址
    let addr = resolve_addr(&fwd.target_host, fwd.target_port).await?;
    let mut s = connect_with_timeout(addr).await?;
    let mut req = format!(
        "CONNECT {host}:{port} HTTP/1.1\r\nHost: {host}:{port}\r\n",
        host = fwd.target_host,
        port = fwd.target_port
    );
    if !user.is_empty() {
        let cred = base64::engine::general_purpose::STANDARD.encode(format!("{user}:{pass}"));
        req.push_str(&format!("Proxy-Authorization: Basic {cred}\r\n"));
    }
    req.push_str("\r\n");
    s.write_all(req.as_bytes()).await?;
    // 上限 8KiB(与旧实现的 `buf.len() > 8192` 判定一致)
    let buf = read_http_head(&mut s, 8192)
        .await
        .map_err(|e| anyhow::anyhow!("http proxy response: {e}"))?;
    let head = String::from_utf8_lossy(&buf);
    let first = head.lines().next().unwrap_or("");
    if !first.contains(" 2") {
        anyhow::bail!("http proxy CONNECT failed: {first}");
    }
    Ok(Box::new(s))
}

async fn dial_socks5(fwd: &StartForward, user: &str, pass: &str) -> anyhow::Result<BoxedIo> {
    let addr = resolve_addr(&fwd.target_host, fwd.target_port).await?;
    let mut s = connect_with_timeout(addr).await?;
    if user.is_empty() {
        s.write_all(&[0x05, 0x01, 0x00]).await?;
    } else {
        s.write_all(&[0x05, 0x01, 0x02]).await?;
    }
    let mut resp = [0u8; 2];
    s.read_exact(&mut resp).await?;
    anyhow::ensure!(resp[0] == 0x05, "bad socks version");
    if resp[1] == 0x02 {
        let u = user.as_bytes();
        let p = pass.as_bytes();
        let mut auth = vec![0x01, u.len() as u8];
        auth.extend_from_slice(u);
        auth.push(p.len() as u8);
        auth.extend_from_slice(p);
        s.write_all(&auth).await?;
        let mut ar = [0u8; 2];
        s.read_exact(&mut ar).await?;
        anyhow::ensure!(ar[1] == 0x00, "socks auth failed");
    } else if resp[1] != 0x00 {
        anyhow::bail!("socks5 no acceptable auth method");
    }
    let mut req = vec![0x05, 0x01, 0x00, 0x03, fwd.target_host.len() as u8];
    req.extend_from_slice(fwd.target_host.as_bytes());
    req.extend_from_slice(&fwd.target_port.to_be_bytes());
    s.write_all(&req).await?;
    let mut head = [0u8; 4];
    s.read_exact(&mut head).await?;
    anyhow::ensure!(head[1] == 0x00, "socks connect failed: {}", head[1]);
    match head[3] {
        0x01 => {
            let mut a = [0u8; 4];
            s.read_exact(&mut a).await?;
        }
        0x03 => {
            let mut l = [0u8; 1];
            s.read_exact(&mut l).await?;
            let mut d = vec![0u8; l[0] as usize];
            s.read_exact(&mut d).await?;
        }
        0x04 => {
            let mut a = [0u8; 16];
            s.read_exact(&mut a).await?;
        }
        other => anyhow::bail!("bad socks atyp {other}"),
    }
    let mut port = [0u8; 2];
    s.read_exact(&mut port).await?;
    Ok(Box::new(s))
}

// ---------------- static_file(最小 HTTP/1.1 文件服务) ----------------

async fn serve_static_file(
    mut io: tokio::io::DuplexStream,
    root: std::path::PathBuf,
    strip_prefix: String,
    auth: Option<(String, String)>,
) {
    // root 的 canonicalize 与 root 生命周期同长:提到循环外只算一次。
    // 修前每个请求都要 async canonicalize 一次(一次额外 await + 一次
    // 路径解析 syscall),而这与请求内容无关。
    let root_canon = tokio::fs::canonicalize(&root)
        .await
        .unwrap_or_else(|_| root.clone());
    loop {
        // 读请求头(上限 64KiB;超限应答 431,与旧行为一致)
        let buf = match read_http_head(&mut io, 64 * 1024).await {
            Ok(b) => b,
            Err(e) if e.kind() == std::io::ErrorKind::InvalidData => {
                let _ = write_simple(&mut io, 431, "Request Header Too Large", b"").await;
                return;
            }
            Err(_) => return, // EOF / IO 错误:与旧实现同样直接结束
        };
        let head_end = buf.len();
        let head = String::from_utf8_lossy(&buf[..head_end]).into_owned();
        let mut lines = head.split("\r\n");
        let req_line = lines.next().unwrap_or("");
        let mut path = req_line.split(' ').nth(1).unwrap_or("/").to_string();
        // 剥离查询串与 fragment(缓存戳 URL 如 /app.js?v=1 不应 404)
        if let Some(p) = path.split_once(['?', '#']) {
            path = p.0.to_string();
        }
        let mut headers = Vec::new();
        for l in lines {
            if let Some((k, v)) = l.split_once(':') {
                headers.push((k.trim().to_ascii_lowercase(), v.trim().to_string()));
            }
        }
        // basic auth
        if let Some((u, p)) = &auth {
            let got = headers
                .iter()
                .find(|(k, _)| k == "authorization")
                .map(|(_, v)| v.strip_prefix("Basic ").map(|s| s.to_string()));
            let ok = matches!(got, Some(Some(enc)) if base64_decode(&enc).map(|d| d == format!("{u}:{p}")).unwrap_or(false));
            if !ok {
                let _ = io
                    .write_all(
                        b"HTTP/1.1 401 Unauthorized\r\nWWW-Authenticate: Basic realm=\"static\"\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                    )
                    .await;
                return;
            }
        }
        // keep-alive(无 Connection: close 即保持)
        let keep = !headers
            .iter()
            .any(|(k, v)| k == "connection" && v.eq_ignore_ascii_case("close"));
        // 去前缀
        if !strip_prefix.is_empty() {
            let sp = strip_prefix.trim_end_matches('/');
            if let Some(rest) = path.strip_prefix(sp) {
                if rest.is_empty() || rest.starts_with('/') {
                    path = rest.to_string();
                }
            }
        }
        if path.is_empty() {
            path = "/".into();
        }
        // 安全路径解析:以 root 为参考点做深度计数,`..` 不允许越出 root;
        // 再以 canonicalize 校验最终落点(同时防符号链接逃逸)。
        // root_canon 在函数入口已算好,这里直接复用。
        let rel = path.trim_start_matches('/');
        let decoded = percent_decode(rel);
        let mut full = root.clone();
        let mut depth = 0usize;
        for seg in decoded.split('/') {
            match seg {
                "" | "." => {}
                ".." => {
                    if depth == 0 {
                        let _ = write_simple(&mut io, 403, "Forbidden", b"").await;
                        return;
                    }
                    depth -= 1;
                    full.pop();
                }
                s => {
                    full.push(s);
                    depth += 1;
                }
            }
        }
        if tokio::fs::metadata(&full)
            .await
            .map(|m| m.is_dir())
            .unwrap_or(false)
        {
            let idx = full.join("index.html");
            if tokio::fs::metadata(&idx)
                .await
                .map(|m| m.is_file())
                .unwrap_or(false)
            {
                full = idx;
            } else {
                let _ = write_simple(&mut io, 404, "Not Found", b"").await;
                if !keep {
                    return;
                }
                continue;
            }
        }
        match tokio::fs::canonicalize(&full).await {
            Ok(c) if c == root_canon || c.starts_with(&root_canon) => full = c,
            _ => {
                let _ = write_simple(&mut io, 404, "Not Found", b"").await;
                if !keep {
                    return;
                }
                continue;
            }
        }
        match tokio::fs::File::open(&full).await {
            Ok(mut f) => {
                let len = match f.metadata().await {
                    Ok(m) if m.is_file() => m.len() as usize,
                    _ => 0,
                };
                let mime = mime_of(full.extension().and_then(|e| e.to_str()));
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: {mime}\r\nContent-Length: {len}\r\nConnection: {}\r\n\r\n",
                    if keep { "keep-alive" } else { "close" }
                );
                if io.write_all(resp.as_bytes()).await.is_err()
                    || tokio::io::copy(&mut f, &mut io).await.is_err()
                {
                    return;
                }
            }
            Err(_) => {
                let _ = write_simple(&mut io, 404, "Not Found", b"").await;
            }
        }
        if !keep {
            return;
        }
    }
}

async fn write_simple(
    io: &mut tokio::io::DuplexStream,
    code: u16,
    text: &str,
    body: &[u8],
) -> std::io::Result<()> {
    let resp = format!(
        "HTTP/1.1 {code} {text}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    io.write_all(resp.as_bytes()).await?;
    io.write_all(body).await?;
    Ok(())
}

fn percent_decode(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%' && i + 2 < b.len() + 1 && i + 2 < b.len() + 1 {
            if let (Some(h), Some(l)) = (hexval(b.get(i + 1)), hexval(b.get(i + 2))) {
                out.push(h * 16 + l);
                i += 3;
                continue;
            }
        }
        out.push(b[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn hexval(c: Option<&u8>) -> Option<u8> {
    match c {
        Some(c @ b'0'..=b'9') => Some(c - b'0'),
        Some(c @ b'a'..=b'f') => Some(c - b'a' + 10),
        Some(c @ b'A'..=b'F') => Some(c - b'A' + 10),
        _ => None,
    }
}

fn base64_decode(s: &str) -> Option<String> {
    // 手写 base64 解码(无 padding 容错)
    let mut vals = Vec::new();
    for c in s.bytes() {
        let v = match c {
            b'A'..=b'Z' => c - b'A',
            b'a'..=b'z' => c - b'a' + 26,
            b'0'..=b'9' => c - b'0' + 52,
            b'+' => 62,
            b'/' => 63,
            b'=' => break,
            _ => return None,
        };
        vals.push(v);
    }
    let mut out = Vec::new();
    for chunk in vals.chunks(4) {
        let b0 = chunk[0] as u32;
        let b1 = *chunk.get(1).unwrap_or(&0) as u32;
        let b2 = *chunk.get(2).unwrap_or(&0) as u32;
        let b3 = *chunk.get(3).unwrap_or(&0) as u32;
        let n = (b0 << 18) | (b1 << 12) | (b2 << 6) | b3;
        out.push((n >> 16) as u8);
        if chunk.len() > 2 {
            out.push((n >> 8) as u8);
        }
        if chunk.len() > 3 {
            out.push(n as u8);
        }
    }
    String::from_utf8(out).ok()
}

fn mime_of(ext: Option<&str>) -> &'static str {
    match ext {
        Some("html") | Some("htm") => "text/html; charset=utf-8",
        Some("css") => "text/css",
        Some("js") => "application/javascript",
        Some("json") => "application/json",
        Some("png") => "image/png",
        Some("jpg") | Some("jpeg") => "image/jpeg",
        Some("gif") => "image/gif",
        Some("svg") => "image/svg+xml",
        Some("ico") => "image/x-icon",
        Some("txt") | Some("md") => "text/plain; charset=utf-8",
        Some("pdf") => "application/pdf",
        Some("wasm") => "application/wasm",
        _ => "application/octet-stream",
    }
}

// ---------------- TLS 终结 + HTTP 头改写 ----------------

fn load_certs(
    path: &str,
) -> anyhow::Result<Vec<tokio_rustls::rustls::pki_types::CertificateDer<'static>>> {
    let data = std::fs::read(path)?;
    let mut rd = std::io::BufReader::new(&data[..]);
    let certs: Result<Vec<_>, _> = rustls_pemfile::certs(&mut rd).collect();
    Ok(certs?)
}

fn load_key(path: &str) -> anyhow::Result<tokio_rustls::rustls::pki_types::PrivateKeyDer<'static>> {
    let data = std::fs::read(path)?;
    let mut rd = std::io::BufReader::new(&data[..]);
    rustls_pemfile::private_key(&mut rd)?.ok_or_else(|| anyhow::anyhow!("no private key in {path}"))
}

/// 进程级 TLS acceptor 缓存,键为 `(crt_path, key_path)`。
///
/// 修前**每次 dial** 都走 `tls_acceptor()`:两次 `std::fs::read`(pem)+
/// 重新构建 `rustls::ServerConfig`(含私钥解析与证书链处理)。这些都是
/// 与连接无关的固定成本,且发生在 async 上下文里。证书路径在一份配置的
/// 生命周期内不变,所以按路径缓存即可。
///
/// 缓存命中后只做一次 `Arc::clone`。注意:`ServerConfig` 内部有会话
/// 票据等可变状态,但**这正是我们想要的复用**——同一个 acceptor 服务
/// 多条连接是 rustls 的正常用法。
type AcceptorCache = std::collections::HashMap<(String, String), Arc<tokio_rustls::TlsAcceptor>>;

fn acceptor_cache() -> &'static std::sync::Mutex<AcceptorCache> {
    static CACHE: std::sync::OnceLock<std::sync::Mutex<AcceptorCache>> = std::sync::OnceLock::new();
    CACHE.get_or_init(|| std::sync::Mutex::new(AcceptorCache::new()))
}

/// 取(或构建并缓存)acceptor。
///
/// `load_*` 仍是同步 fn,但在锁内调用:命中缓存时**完全不碰文件系统**;
/// 未命中时才读两次 pem(仅首次 dial)。持锁期间不做任何 await,不会
/// 把同步阻塞扩散到别的任务上。
fn tls_acceptor(crt: &str, key: &str) -> anyhow::Result<Arc<tokio_rustls::TlsAcceptor>> {
    let key_id = (crt.to_string(), key.to_string());
    let mut cache = acceptor_cache()
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    if let Some(hit) = cache.get(&key_id) {
        return Ok(hit.clone());
    }
    let certs = load_certs(crt)?;
    let priv_key = load_key(key)?;
    let cfg = tokio_rustls::rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, priv_key)?;
    let acceptor = Arc::new(tokio_rustls::TlsAcceptor::from(Arc::new(cfg)));
    cache.insert(key_id, acceptor.clone());
    Ok(acceptor)
}

/// https2http / https2https:通道侧 TLS 终结 → 明文改写 → 上游(明文或 TLS)。
async fn serve_tls_term(
    tls: Arc<tokio_rustls::TlsAcceptor>,
    chan: tokio::io::DuplexStream,
    local_addr: String,
    upstream_tls: bool,
    host_rewrite: String,
    headers: HeaderOp,
) {
    let Ok(tls_stream) = tls.accept(chan).await else {
        return;
    };
    let mut plain = tls_stream;
    match connect_upstream(&local_addr, upstream_tls).await {
        Ok(mut up) => {
            if rewrite_and_splice(&mut plain, &mut up, &host_rewrite, &headers)
                .await
                .is_err()
            {
                let _ = plain.shutdown().await;
                let _ = up.shutdown().await;
            }
        }
        Err(_) => {
            let _ = plain.shutdown().await;
        }
    }
}

/// http2https / http2http:通道侧明文,改写后到上游。
async fn dial_rewrite_upstream(
    local_addr: &str,
    upstream_tls: bool,
    host_rewrite: &str,
    headers: &HeaderOp,
) -> anyhow::Result<BoxedIo> {
    let up = connect_upstream(local_addr, upstream_tls).await?;
    let (a, b) = tokio::io::duplex(64 * 1024);
    let hr = host_rewrite.to_string();
    let headers = headers.clone();
    tokio::spawn(async move {
        let mut chan = b;
        let mut up = up;
        if rewrite_and_splice(&mut chan, &mut up, &hr, &headers)
            .await
            .is_err()
        {
            let _ = chan.shutdown().await;
            let _ = up.shutdown().await;
        }
    });
    Ok(Box::new(a))
}

async fn connect_upstream(local_addr: &str, tls: bool) -> anyhow::Result<BoxedIo> {
    // local_addr: host:port
    let (host, port) = local_addr
        .rsplit_once(':')
        .ok_or_else(|| anyhow::anyhow!("bad local_addr {local_addr}"))?;
    let port: u16 = port.parse()?;
    let addr = resolve_addr(host, port).await?;
    let tcp = connect_with_timeout(addr).await?;
    if !tls {
        return Ok(Box::new(tcp));
    }
    // 本地上游通常自签:跳过校验(与 frp 语义一致)
    let opts = rh_transport::TlsClientOptions {
        server_name: Some(host.to_string()),
        insecure_skip_verify: true,
        ..Default::default()
    };
    let (cfg, _verify) = rh_transport::tls::client_config(&opts)?;
    let connector = tokio_rustls::TlsConnector::from(cfg);
    let tls_stream = connector
        .connect(
            tokio_rustls::rustls::pki_types::ServerName::try_from(host.to_string())
                .map_err(|_| anyhow::anyhow!("bad server name"))?
                .to_owned(),
            tcp,
        )
        .await?;
    Ok(Box::new(tls_stream))
}

/// 读首个请求头 → 改写(Host/HeaderOp/Connection: close)→ 写上游 →
/// 之后纯双向搬运(半关闭传递)。
async fn rewrite_and_splice<A, B>(
    chan: &mut A,
    up: &mut B,
    host_rewrite: &str,
    headers: &HeaderOp,
) -> anyhow::Result<()>
where
    A: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    B: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    // 上限 64KiB(旧实现的 `buf.len() > 64 * 1024` 语义)
    let buf = read_http_head(chan, 64 * 1024)
        .await
        .map_err(|e| anyhow::anyhow!("request head: {e}"))?;
    let rewritten = rewrite_http_head(&buf, host_rewrite, headers);
    up.write_all(&rewritten).await?;
    // 双向搬运(半关闭):借用式 split + join,无需 'static
    let (mut cr, mut cw) = tokio::io::split(chan);
    let (mut ur, mut uw) = tokio::io::split(up);
    async fn cp<R, W>(r: &mut R, w: &mut W)
    where
        R: tokio::io::AsyncRead + Unpin,
        W: tokio::io::AsyncWrite + Unpin,
    {
        // 与主路径 copy_limited 的缓冲上限(MAX_BUF=64KiB)对齐;
        // 16KiB 只有其四分之一,大响应要切 4 倍的 syscall/await 次数。
        let mut b = vec![0u8; 64 * 1024];
        loop {
            match r.read(&mut b).await {
                Ok(0) | Err(_) => break,
                Ok(n) => {
                    if w.write_all(&b[..n]).await.is_err() {
                        break;
                    }
                }
            }
        }
        let _ = w.shutdown().await;
    }
    futures::join!(cp(&mut cr, &mut uw), cp(&mut ur, &mut cw));
    Ok(())
}

/// HTTP/1.1 请求头改写:Host 重写、HeaderOp(先 set 后 del)、
/// 强制 Connection: close(单请求语义,免于体长解析)。
fn rewrite_http_head(head: &[u8], host_rewrite: &str, op: &HeaderOp) -> Vec<u8> {
    let text = String::from_utf8_lossy(head);
    let mut lines = text.split("\r\n");
    let req_line = lines.next().unwrap_or("").to_string();
    let mut out = Vec::with_capacity(head.len() + 128);
    out.extend_from_slice(req_line.as_bytes());
    out.extend_from_slice(b"\r\n");
    let mut saw_host = false;
    for line in lines {
        if line.is_empty() {
            continue;
        }
        let (k, v) = match line.split_once(':') {
            Some((k, v)) => (k.trim(), v.trim()),
            None => continue,
        };
        let lk = k.to_ascii_lowercase();
        if lk == "host" {
            if saw_host {
                continue;
            }
            saw_host = true;
            let host_val = if host_rewrite.is_empty() {
                v
            } else {
                host_rewrite
            };
            out.extend_from_slice(format!("Host: {host_val}\r\n").as_bytes());
            continue;
        }
        if lk == "connection" || op.del.iter().any(|d| d.eq_ignore_ascii_case(k)) {
            continue;
        }
        out.extend_from_slice(format!("{k}: {v}\r\n").as_bytes());
    }
    if !saw_host && !host_rewrite.is_empty() {
        out.extend_from_slice(format!("Host: {host_rewrite}\r\n").as_bytes());
    }
    for p in &op.set {
        out.extend_from_slice(format!("{}: {}\r\n", p.name, p.value).as_bytes());
    }
    out.extend_from_slice(b"Connection: close\r\n\r\n");
    out
}

#[allow(dead_code)]
fn unused_base64_table() {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io;
    use std::pin::Pin;
    use std::task::{Context, Poll};
    use tokio::io::ReadBuf;

    /// 按固定分片长度吐数据的读端(模拟「一次到齐」/「分片到达」)。
    struct ChunkedSrc {
        data: Vec<u8>,
        pos: usize,
        chunk: usize,
        /// 已发生的 `poll_read` 次数,用于断言没有逐字节读。
        reads: usize,
    }

    impl ChunkedSrc {
        fn new(data: impl Into<Vec<u8>>, chunk: usize) -> Self {
            Self {
                data: data.into(),
                pos: 0,
                chunk,
                reads: 0,
            }
        }
    }

    impl tokio::io::AsyncRead for ChunkedSrc {
        fn poll_read(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            self.reads += 1;
            let remain = self.data.len() - self.pos;
            let n = remain.min(self.chunk).min(buf.remaining());
            let pos = self.pos;
            buf.put_slice(&self.data[pos..pos + n]);
            self.pos += n;
            Poll::Ready(Ok(()))
        }
    }

    /// 「无限数据」源:用来证明超限会**在 limit 附近停止**,而不是一直读下去。
    /// 永远不产生 `\r\n\r\n`,且记录被读走的总字节数。
    struct EndlessSrc {
        served: usize,
        reads: usize,
    }

    impl tokio::io::AsyncRead for EndlessSrc {
        fn poll_read(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            self.reads += 1;
            let n = buf.remaining().min(4096);
            let fill = vec![b'x'; n];
            buf.put_slice(&fill);
            self.served += n;
            Poll::Ready(Ok(()))
        }
    }

    const SIMPLE_HEAD: &[u8] = b"GET /a HTTP/1.1\r\nHost: h\r\nX-Y: z\r\n\r\n";
    const TAIL_BODY: &[u8] = b"BODY-BYTES-AFTER-HEAD";

    /// 三态之一:一次到齐。
    #[tokio::test]
    async fn read_http_head_all_at_once() {
        let mut src = ChunkedSrc::new(SIMPLE_HEAD, usize::MAX);
        let head = read_http_head(&mut src, 64 * 1024).await.unwrap();
        assert_eq!(head, SIMPLE_HEAD);
        // 一次读就该拿到(4KiB 块 > 头长度)
        assert_eq!(src.reads, 1, "一次到齐应只推 1 次 read");
    }

    /// 三态之二:分片到达(逐字节最坏情况)。
    #[tokio::test]
    async fn read_http_head_fragmented() {
        for chunk in [1usize, 2, 3, 7, 16, 100] {
            let mut src = ChunkedSrc::new(SIMPLE_HEAD, chunk);
            let head = read_http_head(&mut src, 64 * 1024).await.unwrap();
            assert_eq!(head, SIMPLE_HEAD, "chunk={chunk} 拼出的头不一致");
        }
    }

    /// 结束标记恰好跨分片边界:必须仍然找得到。
    #[tokio::test]
    async fn read_http_head_crlf_split_across_chunks() {
        // 让 `\r\n\r\n` 的每一部分落在不同分片里
        for chunk in [1usize, 2, 3] {
            let mut src = ChunkedSrc::new(SIMPLE_HEAD, chunk);
            let head = read_http_head(&mut src, 64 * 1024).await.unwrap();
            assert!(head.ends_with(b"\r\n\r\n"));
        }
    }

    /// 多读的字节必须由调用方可见(旧实现用 read_exact 逐字节,不会多读;
    /// 新实现一次 4KiB 可能把头后面的字节也读进来,故返回整块缓冲)。
    #[tokio::test]
    async fn read_http_head_may_include_body_bytes() {
        let mut data = SIMPLE_HEAD.to_vec();
        data.extend_from_slice(TAIL_BODY);
        let mut src = ChunkedSrc::new(data, usize::MAX);
        let buf = read_http_head(&mut src, 64 * 1024).await.unwrap();
        assert!(buf.starts_with(SIMPLE_HEAD), "头必须完整");
        // 尾巴是否一起读进来取决于分片,但**绝不能丢**:要么在这里,要么还在流里
        let carried = buf.len() - SIMPLE_HEAD.len();
        assert_eq!(carried, TAIL_BODY.len(), "一次到齐时尾随字节应一并返回");
        assert_eq!(&buf[SIMPLE_HEAD.len()..], TAIL_BODY);
    }

    /// 三态之三:超限必须报错,且不能无限读。
    #[tokio::test]
    async fn read_http_head_over_limit_errors_without_unbounded_read() {
        let limit = 8 * 1024;
        let mut src = EndlessSrc {
            served: 0,
            reads: 0,
        };
        let err = read_http_head(&mut src, limit).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData, "应为 InvalidData");
        // 关键:必须停在 limit+1 附近,而不是把「无限」读下去
        assert!(
            src.served <= limit + 1,
            "超限后仍读了 {} 字节(limit={limit}),存在无界读取",
            src.served
        );
        // 读次数也应是有界的(4KiB 块 → 约 3 次)
        assert!(
            src.reads <= 4,
            "超限判定用了 {} 次 read,块读未生效",
            src.reads
        );
    }

    /// 刚好在 limit 之内结束:必须成功(边界不能误伤)。
    #[tokio::test]
    async fn read_http_head_exactly_at_limit_ok() {
        let limit = 1024;
        let mut head = vec![b'a'; limit - 4];
        head.extend_from_slice(b"\r\n\r\n");
        assert_eq!(head.len(), limit);
        let mut src = ChunkedSrc::new(head.clone(), usize::MAX);
        let got = read_http_head(&mut src, limit).await.unwrap();
        assert_eq!(got, head, "长度恰为 limit 的合法头不得被拒");
    }

    /// 头未收全就 EOF:报 UnexpectedEof(与旧 read_exact 行为一致)。
    #[tokio::test]
    async fn read_http_head_eof_before_complete() {
        let mut src = ChunkedSrc::new(b"GET / HTTP/1.1\r\nHost: h\r\n", usize::MAX);
        let err = read_http_head(&mut src, 64 * 1024).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
    }

    /// 三处调用点的上限都要真正生效(8192 / 64KiB / 64KiB)。
    #[tokio::test]
    async fn read_http_head_respects_per_call_limits() {
        for limit in [8192usize, 64 * 1024] {
            // limit 内:成功
            let mut head = vec![b'h'; limit - 4];
            head.extend_from_slice(b"\r\n\r\n");
            let mut ok_src = ChunkedSrc::new(head, usize::MAX);
            assert!(read_http_head(&mut ok_src, limit).await.is_ok());

            // 超 limit:失败
            let big = vec![b'h'; limit + 1];
            let mut bad_src = ChunkedSrc::new(big, usize::MAX);
            let err = read_http_head(&mut bad_src, limit).await.unwrap_err();
            assert_eq!(err.kind(), io::ErrorKind::InvalidData, "limit={limit}");
        }
    }

    // ---- 红线段:改写语义逐字保留 ----

    fn hdr(name: &str, value: &str) -> rh_protocol::proxy::HeaderPair {
        rh_protocol::proxy::HeaderPair {
            name: name.to_string(),
            value: value.to_string(),
        }
    }

    /// Host 重写 + `Connection: close` + set 在 del 之后。
    #[test]
    fn rewrite_http_head_semantics_preserved() {
        let head = b"GET /p HTTP/1.1\r\nHost: orig\r\nX-Del: v\r\nX-Keep: k\r\nConnection: keep-alive\r\n\r\n";
        let op = rh_protocol::HeaderOp {
            set: vec![hdr("X-Set", "s")],
            del: vec!["X-Del".to_string()],
        };
        let out = rewrite_http_head(head, "new.example", &op);
        let text = String::from_utf8(out).unwrap();

        assert!(text.contains("Host: new.example\r\n"), "Host 必须被重写:{text}");
        assert!(!text.contains("Host: orig"), "原 Host 必须消失:{text}");
        assert!(!text.contains("X-Del:"), "del 必须生效:{text}");
        assert!(text.contains("X-Keep: k\r\n"), "无关头必须保留:{text}");
        assert!(!text.contains("keep-alive"), "Connection 必须被替换:{text}");
        assert!(text.ends_with("Connection: close\r\n\r\n"), "必须强制 close:{text}");

        // 顺序:set 的头出现在 del 判定之后(即位于头列表末尾)
        let set_pos = text.find("X-Set: s").unwrap();
        let keep_pos = text.find("X-Keep: k").unwrap();
        assert!(set_pos > keep_pos, "set 应在原始头之后追加:{text}");
    }

    /// host_rewrite 为空时保留原始 Host(不得删掉)。
    #[test]
    fn rewrite_http_head_empty_rewrite_keeps_host() {
        let head = b"GET / HTTP/1.1\r\nHost: orig\r\n\r\n";
        let op = rh_protocol::HeaderOp::default();
        let out = rewrite_http_head(head, "", &op);
        let text = String::from_utf8(out).unwrap();
        assert!(text.contains("Host: orig\r\n"), "空改写时原 Host 必须保留:{text}");
    }

    /// 无 Host 且给了 host_rewrite:必须补上。
    #[test]
    fn rewrite_http_head_adds_missing_host() {
        let head = b"GET / HTTP/1.1\r\nX-A: 1\r\n\r\n";
        let op = rh_protocol::HeaderOp::default();
        let out = rewrite_http_head(head, "added.example", &op);
        let text = String::from_utf8(out).unwrap();
        assert!(text.contains("Host: added.example\r\n"), "缺失 Host 必须补:{text}");
    }

    /// 重复 Host:只保留第一个(旧行为)。
    #[test]
    fn rewrite_http_head_dedups_host() {
        let head = b"GET / HTTP/1.1\r\nHost: a\r\nHost: b\r\n\r\n";
        let op = rh_protocol::HeaderOp::default();
        let out = rewrite_http_head(head, "", &op);
        let text = String::from_utf8(out).unwrap();
        assert_eq!(text.matches("Host:").count(), 1, "只应保留一个 Host:{text}");
    }
}

#[cfg(test)]
mod cache_tests {
    use super::*;

    /// TLS acceptor 缓存必须命中同一条目(按 (crt,key) 键)。
    /// 这是 `tls_acceptor` 从「每 dial 重建」改为「进程级缓存」的直接证据。
    #[test]
    fn tls_acceptor_is_cached_per_key() {
        let dir = std::env::temp_dir().join(format!("rh-tls-cache-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let crt = dir.join("t.pem");
        let key = dir.join("k.pem");
        std::fs::write(&crt, TEST_CERT_PEM).unwrap();
        std::fs::write(&key, TEST_KEY_PEM).unwrap();
        // 测试证书:自签 CN=rh-cache-test,随源码存放(仅测试用)
        // 见 src/testdata/cache-test.{crt,key}
        let (crt, key) = (crt.to_str().unwrap(), key.to_str().unwrap());

        let a1 = tls_acceptor(crt, key).expect("first build");
        // 删掉文件:若再读盘就会失败;命中缓存则仍成功
        std::fs::remove_file(crt).unwrap();
        std::fs::remove_file(key).unwrap();
        let a2 = tls_acceptor(crt, key).expect("must hit cache (files deleted)");

        assert!(
            Arc::ptr_eq(&a1, &a2),
            "同 (crt,key) 必须拿到同一个 Arc<TlsAcceptor>,实际是重建了"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// 不同路径不得串用缓存。
    #[test]
    fn tls_acceptor_distinguishes_keys() {
        let dir = std::env::temp_dir().join(format!("rh-tls-cache2-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let mut paths = Vec::new();
        for i in 0..2 {
            let crt = dir.join(format!("t{i}.pem"));
            let key = dir.join(format!("k{i}.pem"));
            std::fs::write(&crt, TEST_CERT_PEM).unwrap();
            std::fs::write(&key, TEST_KEY_PEM).unwrap();
            paths.push((
                crt.to_str().unwrap().to_string(),
                key.to_str().unwrap().to_string(),
            ));
        }
        let a0 = tls_acceptor(&paths[0].0, &paths[0].1).unwrap();
        let a1 = tls_acceptor(&paths[1].0, &paths[1].1).unwrap();
        assert!(!Arc::ptr_eq(&a0, &a1), "不同证书路径必须各自独立缓存");
        let _ = std::fs::remove_dir_all(&dir);
    }


    const TEST_CERT_PEM: &str = include_str!("testdata/cache-test.crt");
    const TEST_KEY_PEM: &str = include_str!("testdata/cache-test.key");
}
