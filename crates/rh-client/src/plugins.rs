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
    let mut buf = Vec::new();
    let mut byte = [0u8; 1];
    loop {
        s.read_exact(&mut byte).await?;
        buf.push(byte[0]);
        if buf.ends_with(b"\r\n\r\n") {
            break;
        }
        if buf.len() > 8192 {
            anyhow::bail!("proxy response too large");
        }
    }
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
    loop {
        // 读请求头
        let mut buf = Vec::with_capacity(1024);
        let mut byte = [0u8; 1];
        let head_end = loop {
            match io.read_exact(&mut byte).await {
                Ok(_) => {
                    buf.push(byte[0]);
                    if buf.ends_with(b"\r\n\r\n") {
                        break buf.len();
                    }
                    if buf.len() > 64 * 1024 {
                        let _ = write_simple(&mut io, 431, "Request Header Too Large", b"").await;
                        return;
                    }
                }
                Err(_) => return,
            }
        };
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
        // 再以 canonicalize 校验最终落点(同时防符号链接逃逸)
        let root_canon = tokio::fs::canonicalize(&root)
            .await
            .unwrap_or_else(|_| root.clone());
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
        if full.is_dir() {
            let idx = full.join("index.html");
            if idx.is_file() {
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

fn tls_acceptor(crt: &str, key: &str) -> anyhow::Result<tokio_rustls::TlsAcceptor> {
    let certs = load_certs(crt)?;
    let key = load_key(key)?;
    let cfg = tokio_rustls::rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)?;
    Ok(tokio_rustls::TlsAcceptor::from(Arc::new(cfg)))
}

/// https2http / https2https:通道侧 TLS 终结 → 明文改写 → 上游(明文或 TLS)。
async fn serve_tls_term(
    tls: tokio_rustls::TlsAcceptor,
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
    let mut buf = Vec::with_capacity(1024);
    let mut byte = [0u8; 1];
    loop {
        chan.read_exact(&mut byte).await?;
        buf.push(byte[0]);
        if buf.ends_with(b"\r\n\r\n") {
            break;
        }
        if buf.len() > 64 * 1024 {
            anyhow::bail!("request head too large");
        }
    }
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
        let mut b = vec![0u8; 16 * 1024];
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
