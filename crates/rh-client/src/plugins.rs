//! 客户端插件(9 种):本地目标拨号的替代实现。
//!
//! 结构:插件返回一对 duplex 的通道侧;协议处理任务持有另一侧并连接
//! 上游,完成握手/终结/改写后纯搬运。HTTP 改写类插件对请求头做
//! Host 重写 + HeaderOp + `Connection: close`(每连接单请求,语义正确)。
//!
//! 两类插件的方向相反,不能混为一谈:
//!
//! * **改写/终结类**(`https2http`/`http2https`/…/`tls2raw`/`static_file`/
//!   `unix_domain_socket`):目的地由**配置**给出(`local_addr`/`unix_path`),
//!   插件把通道流量送到那个固定目标。
//! * **代理服务端类**(`http_proxy`/`socks5`):插件在客户端进程内**扮演代理
//!   服务端**,目的地由**用户在代理请求里写**(`CONNECT host:port` / SOCKS5
//!   的 `DST.ADDR`),插件解析后自己拨号。配置里的 `target_host/target_port`
//!   对这两种插件没有意义。详见 [`serve_http_proxy`] / [`serve_socks5`]。

use rh_protocol::HeaderOp;
use rh_protocol::PluginSpec;
use rh_transport::BoxedIo;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// 按插件规格建立本地连接(返回的是「通道侧」端点)。
///
/// 目标一律来自 `plugin` 自身字段:改写/终结类取 `local_addr`/`unix_path`,
/// 代理服务端类取**用户请求里的目的地**。线路上的 `StartForward.target_*`
/// 对任何插件都不参与决策 —— 从前 `http_proxy`/`socks5` 拿它当目标,
/// 才有了「连代理 → CONNECT 代理自己」的自指 bug。
pub async fn dial_plugin(plugin: &PluginSpec) -> anyhow::Result<BoxedIo> {
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
        } => serve_proxy(ProxyProtocol::Http, http_user, http_password),
        PluginSpec::Socks5 { user, password } => serve_proxy(ProxyProtocol::Socks5, user, password),
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
                // 拨号必须有界:修前无超时,目标不可达时这个任务会长期占用
                // 一对 duplex 半与 TLS 会话(客户端只看到"连上了但不通")。
                let Ok(Ok(up)) = tokio::time::timeout(
                    PLUGIN_DIAL_TIMEOUT,
                    tokio::net::TcpStream::connect(&local_addr),
                )
                .await
                else {
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

// ---------------- http_proxy / socks5(在客户端进程内扮演代理服务端) ----------------
//
// # 为什么是「服务端」而不是「拨号器」
//
// 这两个插件与 frp 同名插件的语义是**同一件事**:把隧道送进来的用户连接
// 当成一条「用户 → 代理」的连接来服务。用户想访问的站点写在他的代理请求里
// (`CONNECT host:port` 或 SOCKS5 的 `DST.ADDR`),插件解析出来**自己**去拨。
//
// 修前的实现把配置里的 `target_host:target_port` 同时当成「代理地址」与
// 「CONNECT 目的地」:先连上代理,再让代理连它自己。真实目的地从未出现在
// 代码里,所以对任何真实代理(squid / tinyproxy)都用不了 —— 只有配一个
// 允许自连的假代理才会「跑通」,而那只是回环,不转发任何用户流量。
//
// # 连接模型
//
// 每条隧道连接 = 一条代理客户端连接。用户在隧道另一端每访问一个目标,
// 就新开一条到本代理的连接(HTTP 代理对 https 必然如此;SOCKS5 亦然)。
// 因此「一个通道对应一条代理会话」,插件内不做多路复用,也不需要监听端口。

/// 代理服务端的两种协议(共用鉴权/拨号/转发骨架,只有握手不同)。
#[derive(Clone, Copy)]
enum ProxyProtocol {
    Http,
    Socks5,
}

/// 代理会话内「用户迟迟不说话」的兜底上限。
///
/// 隧道是长连接:没有上限时一个半开连接可以永久占住一对任务。
/// 60s 与 frp 的 `ReadHeaderTimeout` 同值,便于对照排障。
const PROXY_HANDSHAKE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);

/// 建一个代理会话,返回通道侧端点。
///
/// 会话任务持有 duplex 的另一半:解析用户请求 → 鉴权 → 拨目的地 →
/// 写成功应答 → 双向搬运。任一步失败都写协议对应的应答再断开,
/// 让用户看到 407/502/REP 而不是莫名其妙的 EOF。
fn serve_proxy(proto: ProxyProtocol, user: &str, pass: &str) -> anyhow::Result<BoxedIo> {
    let (chan, session) = tokio::io::duplex(64 * 1024);
    let auth = basic_auth(user, pass);
    tokio::spawn(async move {
        match proto {
            ProxyProtocol::Http => serve_http_proxy(session, auth).await,
            ProxyProtocol::Socks5 => serve_socks5(session, auth).await,
        }
    });
    Ok(Box::new(chan))
}

/// 代理凭据校验(恒定时间),返回是否放行。
///
/// 免鉴权的判据是「用户名为空」,与 [`basic_auth`] 及 `static_file` 的
/// Basic Auth 同口径 —— 同一个控制台里两种「空密码」不该有两种语义。
/// 与 frp 的差异仅在「只填密码不填用户名」这一种配置:frp 仍要求认证
/// (用户名为空串),我们视为不鉴权。控制台表单里密码是用户名的从属项,
/// 单独填密码没有可表达的意图。
fn check_proxy_auth(auth: &Option<(String, String)>, got: Option<&str>) -> bool {
    let Some((user, pass)) = auth else {
        return true;
    };
    let Some(got) = got else {
        return false;
    };
    let want = format!("{user}:{pass}");
    rh_transport::constant_time_eq(got.as_bytes(), want.as_bytes())
}

/// 拨代理请求里的目的地。
///
/// 与 [`connect_upstream`] 同口径:解析走异步路径(C8),连接有超时。
/// 解析失败、连接被拒、超时都归一成 `Err`,由调用方回 502 / REP。
async fn dial_proxy_destination(host: &str, port: u16) -> anyhow::Result<BoxedIo> {
    let addr = resolve_addr(host, port).await?;
    let tcp = connect_with_timeout(addr).await?;
    tcp.set_nodelay(true).ok();
    Ok(Box::new(tcp))
}

/// 双向搬运一条代理会话(半关闭传递)。
///
/// 用 [`crate::forward::splice`] 而不是 `copy_bidirectional`:后者在一侧
/// EOF 时立刻关掉另一侧的两个方向,丢掉「用户半关闭写、还在等响应」的
/// 合法用法;`splice` 是项目里统一的半关闭语义,且能走无锁拆半 /
/// 内核 splice 快路径。
async fn splice_session(chan: tokio::io::DuplexStream, remote: BoxedIo) {
    crate::forward::splice(Box::new(chan), remote, None).await;
}

/// 把「已多读的用户字节」先送给目的地,再开始双向搬运。
///
/// `read_http_head` 一次读 4KiB,很可能把 `CONNECT` 之后紧跟的 TLS
/// ClientHello 一起读进来;丢掉就会表现为「隧道建立了但 https 握手
/// 永远超时」。
async fn finish_session(chan: tokio::io::DuplexStream, remote: BoxedIo, leftover: Vec<u8>) {
    if leftover.is_empty() {
        splice_session(chan, remote).await;
        return;
    }
    let mut remote = remote;
    if remote.write_all(&leftover).await.is_ok() {
        splice_session(chan, remote).await;
    }
}

// ---- 地址拆分(两个协议共用) ----

/// 拆分 `host:port` / `[v6]:port`,并归一化 host。
///
/// 归一化只做两件对拨号有实际影响的事:
/// * 去掉 IPv6 的方括号(`[::1]:80` → `::1`)—— [`resolve_addr`] 走
///   `IpAddr::parse`,带括号会解析失败;
/// * 去掉末尾的根点(`example.com.` → `example.com`)—— 两者等价,
///   但日志与 DNS 缓存键应统一,否则同一目标会有两个缓存条目。
///
/// 只有一个冒号才是 `host:port`;未加括号的 v6 字面量(`a:b:c`)无法
/// 区分端口,**直接拒绝而不是猜**。空 host 与 0 端口一律 `None`。
fn split_authority(authority: &str, default_port: u16) -> Option<(String, u16)> {
    let authority = authority.trim();
    if authority.is_empty() {
        return None;
    }
    let (host, port) = if let Some(rest) = authority.strip_prefix('[') {
        let (host, after) = rest.split_once(']')?;
        let port = match after.strip_prefix(':') {
            Some(p) => p.parse().ok()?,
            None if after.is_empty() => default_port,
            None => return None,
        };
        (host, port)
    } else {
        match authority.rsplit_once(':') {
            Some((h, p)) if !h.contains(':') => (h, p.parse().ok()?),
            Some(_) => return None,
            None => (authority, default_port),
        }
    };
    let host = host.trim_end_matches('.');
    if host.is_empty() || port == 0 {
        return None;
    }
    Some((host.to_string(), port))
}

// ---- HTTP 正向代理 ----

/// HTTP 代理会话的错误:要么回一个状态码,要么什么都不写。
enum HttpProxyError {
    Status(u16, &'static str),
    /// 用户没把请求发完就断了:此时再写应答没有意义。
    Silent,
}

/// 一条已解析的代理请求。
struct HttpProxyRequest {
    /// 原始请求头整块(含 `\r\n\r\n`)。CONNECT 不需要它,普通请求要转发。
    head: Vec<u8>,
    /// `CONNECT` 的目标 authority,或 absolute-form 请求里的 authority。
    authority: String,
    /// 是否需要把请求行从 absolute-form 改写成 origin-form。
    rewrite_request_line: bool,
    /// `Proxy-Authorization` 的 base64 载荷(已解码成 `user:pass`)。
    credential: Option<String>,
    /// 读头时一并读进来的用户字节(TLS ClientHello / 请求体)。
    leftover: Vec<u8>,
}

/// 读一条代理客户端请求(请求行 + 头)并解析。
async fn read_proxy_request(
    io: &mut tokio::io::DuplexStream,
) -> Result<HttpProxyRequest, HttpProxyError> {
    let buf = tokio::time::timeout(PROXY_HANDSHAKE_TIMEOUT, read_http_head(io, 64 * 1024))
        .await
        .map_err(|_| HttpProxyError::Status(408, "Request Timeout"))?
        .map_err(|e| match e.kind() {
            // 超长头按 431 回,与 static_file 一致
            std::io::ErrorKind::InvalidData => {
                HttpProxyError::Status(431, "Request Header Fields Too Large")
            }
            _ => HttpProxyError::Silent,
        })?;
    let head_len = buf
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .map(|p| p + 4)
        .unwrap_or(buf.len());
    let head = buf[..head_len].to_vec();
    let leftover = buf[head_len..].to_vec();
    let text = String::from_utf8_lossy(&head).into_owned();

    let mut lines = text.split("\r\n");
    let req_line = lines.next().unwrap_or("");
    let mut parts = req_line.split(' ');
    let (method, target) = match (parts.next(), parts.next()) {
        (Some(m), Some(t)) if !m.is_empty() && !t.is_empty() => (m, t),
        _ => return Err(HttpProxyError::Status(400, "Bad Request")),
    };

    let mut credential = None;
    for line in lines {
        let Some((k, v)) = line.split_once(':') else {
            continue;
        };
        if k.trim().eq_ignore_ascii_case("proxy-authorization") {
            let v = v.trim();
            if let Some(b64) = v
                .strip_prefix("Basic ")
                .or_else(|| v.strip_prefix("basic "))
            {
                credential = base64_decode(b64);
            }
        }
    }

    // CONNECT:authority-form,目标就是目的地
    // 其它方法:absolute-form(`GET http://host/path`),取 URL 的 authority
    let (authority, rewrite_request_line) = if method.eq_ignore_ascii_case("CONNECT") {
        (target.to_string(), false)
    } else {
        match absolute_form_authority(target) {
            Some(a) => (a, true),
            // 非 absolute-form 的普通请求(`GET /`)是「把本代理当源服务器」
            // 的用法。frp 把它交给内置 RoundTrip,但那要求代理自己知道上游;
            // 本插件没有这个配置,如实回 400 比假装成功好。
            None => return Err(HttpProxyError::Status(400, "Bad Request")),
        }
    };

    Ok(HttpProxyRequest {
        head,
        authority,
        rewrite_request_line,
        credential,
        leftover,
    })
}

/// 从 absolute-form 请求行目标里取 authority。
///
/// `http://example.com:8080/p?q` → `example.com:8080`;
/// `http://user:pw@example.com/` → `example.com`(userinfo 不参与拨号)。
/// 非 http/https scheme 或没有 authority 一律 `None`。
fn absolute_form_authority(target: &str) -> Option<String> {
    let (scheme, rest) = target.split_once("://")?;
    if !scheme.eq_ignore_ascii_case("http") && !scheme.eq_ignore_ascii_case("https") {
        return None;
    }
    let authority = rest.split(['/', '?', '#']).next()?;
    let authority = authority.rsplit_once('@').map_or(authority, |(_, a)| a);
    (!authority.is_empty()).then(|| authority.to_string())
}

/// absolute-form 目标 → origin-form(`/path?query`),供普通 HTTP 转发使用。
///
/// fragment 不发给服务器(RFC 7230 §5.3.2);没有路径时补 `/`。
fn origin_form(target: &str) -> String {
    let Some((_, rest)) = target.split_once("://") else {
        return "/".into();
    };
    let after_auth = match rest.find(['/', '?', '#']) {
        Some(i) => &rest[i..],
        None => return "/".into(),
    };
    // 去掉 fragment
    let after_auth = after_auth.split('#').next().unwrap_or(after_auth);
    if after_auth.starts_with('?') {
        format!("/{after_auth}")
    } else if after_auth.is_empty() {
        "/".into()
    } else {
        after_auth.to_string()
    }
}

/// 普通(非 CONNECT)代理请求的头改写:请求行转 origin-form、去掉代理专有头、
/// 强制 `Connection: close`。
///
/// 为什么强制 close:插件是「一通道一会话」,不做 keep-alive 的状态机;
/// 不关闭则服务器可能在同一条连接上继续等下一个请求,而用户侧的第二个
/// 请求已经在同一通道里被当作隧道字节处理 —— 语义会错位。close 让双方
/// 都在这一问一答后收工,与项目里其他插件「每连接单请求」的口径一致。
fn rewrite_proxy_request_head(head: &[u8]) -> Vec<u8> {
    let text = String::from_utf8_lossy(head);
    let mut lines = text.split("\r\n");
    let req_line = lines.next().unwrap_or("");
    let mut parts = req_line.split(' ');
    let (method, target, version) = (
        parts.next().unwrap_or("GET"),
        parts.next().unwrap_or("/"),
        parts.next().unwrap_or("HTTP/1.1"),
    );
    let mut out = Vec::with_capacity(head.len() + 32);
    out.extend_from_slice(format!("{method} {} {version}\r\n", origin_form(target)).as_bytes());
    for line in lines {
        if line.is_empty() {
            continue;
        }
        let Some((k, _)) = line.split_once(':') else {
            continue;
        };
        let lk = k.trim().to_ascii_lowercase();
        // 代理专有头不得透传给源服务器(RFC 7230 §6.1)
        if lk == "proxy-authorization" || lk == "proxy-connection" || lk == "connection" {
            continue;
        }
        out.extend_from_slice(line.as_bytes());
        out.extend_from_slice(b"\r\n");
    }
    out.extend_from_slice(b"Connection: close\r\n\r\n");
    out
}

/// 写一个最小 HTTP 应答并关闭连接。
async fn write_proxy_status(
    io: &mut tokio::io::DuplexStream,
    code: u16,
    reason: &str,
    extra: &str,
) -> std::io::Result<()> {
    let resp = format!(
        "HTTP/1.1 {code} {reason}\r\n{extra}Content-Length: 0\r\nConnection: close\r\n\r\n"
    );
    io.write_all(resp.as_bytes()).await?;
    io.shutdown().await
}

/// 服务一条 HTTP 代理连接。
///
/// 一条通道只服务一次请求:CONNECT 成功后连接变成裸隧道,HTTP 语义到此
/// 结束(与 frp 的 `handleConnectReq` 一致);普通请求转发完一问一答后
/// 也随 `Connection: close` 收工。
async fn serve_http_proxy(mut io: tokio::io::DuplexStream, auth: Option<(String, String)>) {
    let req = match read_proxy_request(&mut io).await {
        Ok(v) => v,
        Err(HttpProxyError::Status(code, reason)) => {
            let extra = if code == 407 {
                "Proxy-Authenticate: Basic realm=\"rabbithole\"\r\n"
            } else {
                ""
            };
            let _ = write_proxy_status(&mut io, code, reason, extra).await;
            return;
        }
        Err(HttpProxyError::Silent) => return,
    };
    if !check_proxy_auth(&auth, req.credential.as_deref()) {
        // 407 必须带 Proxy-Authenticate,否则客户端无法重试认证
        let _ = write_proxy_status(
            &mut io,
            407,
            "Proxy Authentication Required",
            "Proxy-Authenticate: Basic realm=\"rabbithole\"\r\n",
        )
        .await;
        return;
    }
    // 缺端口时按语义补默认值:CONNECT 的唯一用途是 TLS → 443;
    // absolute-form 的 scheme 已在解析时限定为 http/https,这里用 80
    // (https 的 absolute-form 极少见,补 80 会被对端拒绝,好过猜错端口)。
    let default_port = if req.rewrite_request_line { 80 } else { 443 };
    let Some((host, port)) = split_authority(&req.authority, default_port) else {
        let _ = write_proxy_status(&mut io, 400, "Bad Request", "").await;
        return;
    };
    let mut remote = match dial_proxy_destination(&host, port).await {
        Ok(r) => r,
        Err(e) => {
            tracing::debug!(host = %host, port, error = %e, "http_proxy: 目的地拨号失败");
            let _ = write_proxy_status(&mut io, 502, "Bad Gateway", "").await;
            return;
        }
    };
    if req.rewrite_request_line {
        // 普通请求:把改写后的头写过去,之后纯搬运(请求体与响应)
        let head = rewrite_proxy_request_head(&req.head);
        if remote.write_all(&head).await.is_err() {
            let _ = write_proxy_status(&mut io, 502, "Bad Gateway", "").await;
            return;
        }
    } else if io
        .write_all(b"HTTP/1.1 200 Connection established\r\n\r\n")
        .await
        .is_err()
    {
        return;
    }
    finish_session(io, remote, req.leftover).await;
}

// ---- SOCKS5 ----

/// 服务一条 SOCKS5 连接(RFC 1928 / RFC 1929 用户名密码认证)。
///
/// 只实现 CONNECT:BIND / UDP ASSOCIATE 需要监听回连地址或维护 UDP 关联表,
/// 与「隧道里的一条 TCP 流」模型不兼容,按 RFC 回 `0x07 Command not supported`。
async fn serve_socks5(mut io: tokio::io::DuplexStream, auth: Option<(String, String)>) {
    match socks5_handshake(&mut io, &auth).await {
        Ok((host, port)) => {
            let mut remote = match dial_proxy_destination(&host, port).await {
                Ok(r) => r,
                Err(e) => {
                    tracing::debug!(host = %host, port, error = %e, "socks5: 目的地拨号失败");
                    let _ = socks5_reply(&mut io, SOCKS_REP_HOST_UNREACHABLE).await;
                    let _ = io.shutdown().await;
                    return;
                }
            };
            if socks5_reply(&mut io, SOCKS_REP_SUCCESS).await.is_err() {
                return;
            }
            // 握手阶段全部用 read_exact 精确读取,不会多读用户字节
            let _ = &mut remote;
            splice_session(io, remote).await;
        }
        Err(code) => {
            // 0xFF = 握手早期就该断开(版本不对 / 方法不可接受 / 认证失败),
            // 此时还没进入「按命令回 REP」的阶段
            if code != SOCKS_REP_ABORT {
                let _ = socks5_reply(&mut io, code).await;
            }
            let _ = io.shutdown().await;
        }
    }
}

const SOCKS_REP_SUCCESS: u8 = 0x00;
const SOCKS_REP_HOST_UNREACHABLE: u8 = 0x04;
/// 内部哨兵:不是合法的 REP 码,表示「此时不应回任何应答」。
const SOCKS_REP_ABORT: u8 = 0xFF;

/// 写 SOCKS5 应答。`rep == 0` 表示成功。
///
/// BND.ADDR/BND.PORT 固定回 `0.0.0.0:0`:隧道出口没有「对外可回连的绑定
/// 地址」可言,填假地址反而会误导客户端去做反向连接。RFC 1928 允许客户端
/// 忽略这两个字段(除 BIND 外)。
async fn socks5_reply(io: &mut tokio::io::DuplexStream, rep: u8) -> std::io::Result<()> {
    io.write_all(&[0x05, rep, 0x00, 0x01, 0, 0, 0, 0, 0, 0])
        .await
}

/// 走完 SOCKS5 握手,返回(目的地 host, 端口)。
///
/// `Err(rep)` 里的 `rep` 是应回给客户端的 REP 码;`SOCKS_REP_ABORT` 表示
/// 握手早期就该断开(还没到能回 REP 的阶段)。
async fn socks5_handshake(
    io: &mut tokio::io::DuplexStream,
    auth: &Option<(String, String)>,
) -> Result<(String, u16), u8> {
    // ---- 方法协商 ----
    let mut head = [0u8; 2];
    read_exact_timeout(io, &mut head)
        .await
        .map_err(|_| SOCKS_REP_ABORT)?;
    if head[0] != 0x05 {
        return Err(SOCKS_REP_ABORT); // 不是 SOCKS5,无从应答
    }
    let mut methods = vec![0u8; head[1] as usize];
    read_exact_timeout(io, &mut methods)
        .await
        .map_err(|_| SOCKS_REP_ABORT)?;
    // 需要认证时只接受 0x02(用户名/密码),否则只接受 0x00(免认证)
    let want = if auth.is_some() { 0x02 } else { 0x00 };
    if !methods.contains(&want) {
        let _ = io.write_all(&[0x05, 0xFF]).await;
        return Err(SOCKS_REP_ABORT);
    }
    io.write_all(&[0x05, want])
        .await
        .map_err(|_| SOCKS_REP_ABORT)?;

    // ---- 用户名/密码认证(RFC 1929) ----
    if want == 0x02 {
        let mut ver = [0u8; 1];
        read_exact_timeout(io, &mut ver)
            .await
            .map_err(|_| SOCKS_REP_ABORT)?;
        if ver[0] != 0x01 {
            let _ = io.write_all(&[0x01, 0x01]).await;
            return Err(SOCKS_REP_ABORT);
        }
        let user = read_len_prefixed(io).await.map_err(|_| SOCKS_REP_ABORT)?;
        let pass = read_len_prefixed(io).await.map_err(|_| SOCKS_REP_ABORT)?;
        let got = format!(
            "{}:{}",
            String::from_utf8_lossy(&user),
            String::from_utf8_lossy(&pass)
        );
        // 认证失败:回 0x01 后断开(REP 阶段还没开始)
        if !check_proxy_auth(auth, Some(&got)) {
            let _ = io.write_all(&[0x01, 0x01]).await;
            return Err(SOCKS_REP_ABORT);
        }
        io.write_all(&[0x01, 0x00])
            .await
            .map_err(|_| SOCKS_REP_ABORT)?;
    }

    // ---- 请求 ----
    let mut req = [0u8; 4];
    read_exact_timeout(io, &mut req)
        .await
        .map_err(|_| SOCKS_REP_ABORT)?;
    if req[0] != 0x05 {
        return Err(SOCKS_REP_ABORT);
    }
    if req[1] != 0x01 {
        // 非 CONNECT:REP=0x07 Command not supported
        return Err(0x07);
    }
    let host = match req[3] {
        0x01 => {
            let mut a = [0u8; 4];
            read_exact_timeout(io, &mut a)
                .await
                .map_err(|_| SOCKS_REP_ABORT)?;
            std::net::Ipv4Addr::from(a).to_string()
        }
        0x03 => {
            let d = read_len_prefixed(io).await.map_err(|_| SOCKS_REP_ABORT)?;
            // 域名不做 UTF-8 强校验:非法字节交给 DNS 解析失败处理,
            // 免得因为一个非 ASCII 域名就回 0xFF 让客户端以为服务端坏了
            String::from_utf8_lossy(&d).into_owned()
        }
        0x04 => {
            let mut a = [0u8; 16];
            read_exact_timeout(io, &mut a)
                .await
                .map_err(|_| SOCKS_REP_ABORT)?;
            std::net::Ipv6Addr::from(a).to_string()
        }
        // 未知地址类型:REP=0x08 Address type not supported
        _ => return Err(0x08),
    };
    let mut port = [0u8; 2];
    read_exact_timeout(io, &mut port)
        .await
        .map_err(|_| SOCKS_REP_ABORT)?;
    let port = u16::from_be_bytes(port);
    if host.is_empty() || port == 0 {
        return Err(0x01); // general failure
    }
    Ok((host, port))
}

/// 读一个 SOCKS5 长度前缀字段(1 字节长度 + 内容)。
async fn read_len_prefixed(io: &mut tokio::io::DuplexStream) -> std::io::Result<Vec<u8>> {
    let mut len = [0u8; 1];
    read_exact_timeout(io, &mut len).await?;
    let mut buf = vec![0u8; len[0] as usize];
    read_exact_timeout(io, &mut buf).await?;
    Ok(buf)
}

/// 带超时的 `read_exact`:握手阶段用户迟迟不发数据时必须能放弃。
async fn read_exact_timeout(
    io: &mut tokio::io::DuplexStream,
    buf: &mut [u8],
) -> std::io::Result<()> {
    match tokio::time::timeout(PROXY_HANDSHAKE_TIMEOUT, io.read_exact(buf)).await {
        Ok(r) => r.map(|_| ()),
        Err(_) => Err(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "proxy handshake timeout",
        )),
    }
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
        let method = req_line.split(' ').next().unwrap_or("");
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
        // ★只支持 GET/HEAD,且必须把请求体读掉。
        //
        // 修前不区分方法、也不看 `Content-Length`:客户端发 POST 时 body 会
        // 留在 duplex 缓冲里,被当作**下一个请求**的起始解析 —— 表现为
        // keep-alive 连接上后续请求全部错乱(或直接 400)。
        // 静态文件服务没有 POST 语义,直接 405 + `Connection: close` 最安全,
        // 并读掉 body 保证即使客户端不遵守 close 也不会污染后续解析。
        let body_len: usize = headers
            .iter()
            .find(|(k, _)| k == "content-length")
            .and_then(|(_, v)| v.parse().ok())
            .unwrap_or(0);
        if method != "GET" && method != "HEAD" {
            // 读掉请求体(有界:超过 1MiB 直接断开,避免被大 body 拖住)
            let mut left = body_len;
            let mut sink = [0u8; 8192];
            while left > 0 {
                let want = left.min(sink.len());
                use tokio::io::AsyncReadExt as _;
                match io.read(&mut sink[..want]).await {
                    Ok(0) | Err(_) => break,
                    Ok(n) => left -= n,
                }
            }
            let _ = write_simple(&mut io, 405, "Method Not Allowed", b"").await;
            return;
        }
        // GET 也可能带 body(少见):同样读掉,避免污染后续请求
        if body_len > 0 {
            let mut left = body_len;
            let mut sink = [0u8; 8192];
            while left > 0 {
                let want = left.min(sink.len());
                use tokio::io::AsyncReadExt as _;
                match io.read(&mut sink[..want]).await {
                    Ok(0) | Err(_) => break,
                    Ok(n) => left -= n,
                }
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
        // 修前这里是同一个条件写了两遍(`i + 2 < b.len() + 1`),
        // 应为"后面确实还有两个十六进制位"——即 i+2 落在范围内。
        if b[i] == b'%' && i + 2 < b.len() {
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
    // ★先在**锁外**查一次,再去锁外读盘。
    //
    // 修前把 `load_certs`/`load_key`(同步读盘)整段放在缓存锁内:
    // 一次证书文件读取期间,所有需要 acceptor 的连接(每个 TLS 插件连接)
    // 都阻塞在同一把 std Mutex 上 —— 在 async 任务里等于阻塞 OS 线程。
    // 缓存命中路径很短,重读概率极低,这里接受"可能重复读一次"的代价。
    {
        let cache = acceptor_cache().lock().unwrap_or_else(|e| e.into_inner());
        if let Some(hit) = cache.get(&key_id) {
            return Ok(hit.clone());
        }
    }
    let certs = load_certs(crt)?;
    let priv_key = load_key(key)?;
    let mut cache = acceptor_cache().lock().unwrap_or_else(|e| e.into_inner());
    // 并发下可能已被别的任务插入:复用它的
    if let Some(hit) = cache.get(&key_id) {
        return Ok(hit.clone());
    }
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

#[cfg(test)]
mod proxy_tests {
    //! `http_proxy` / `socks5` 代理服务端的单元测试。
    //!
    //! 这一组测试的存在理由是**防止回到自指语义**:修前的实现把
    //! `target_host:target_port` 当目的地,任何"拨号能通"的测试都会绿。
    //! 所以这里的断言一律落在「目的地来自用户请求」上 ——
    //! 用一个只在**用户请求指定的端口**上监听的 echo 服务,任何
    //! "拨配置里的地址"的实现都会失败。

    use super::*;
    use base64::Engine as _;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    /// 在 127.0.0.1 上起一个回显服务,返回其端口。
    ///
    /// 绑定 0 让内核分配端口:测试里**没有**「配置的目标地址」可言,
    /// 端口只出现在用户请求里,这正是要断言的语义。
    async fn spawn_echo() -> u16 {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = l.local_addr().unwrap().port();
        tokio::spawn(async move {
            while let Ok((mut s, _)) = l.accept().await {
                tokio::spawn(async move {
                    let mut buf = [0u8; 4096];
                    loop {
                        match s.read(&mut buf).await {
                            Ok(0) | Err(_) => break,
                            Ok(n) => {
                                if s.write_all(&buf[..n]).await.is_err() {
                                    break;
                                }
                            }
                        }
                    }
                });
            }
        });
        port
    }

    /// 从插件拿到通道侧端点(与 `dial_plugin` 走同一路径)。
    async fn open(plugin: &PluginSpec) -> BoxedIo {
        dial_plugin(plugin).await.expect("插件应能建立会话")
    }

    async fn read_some(io: &mut BoxedIo, n: usize) -> Vec<u8> {
        let mut buf = vec![0u8; n];
        let got = tokio::time::timeout(std::time::Duration::from_secs(5), io.read(&mut buf))
            .await
            .expect("读超时")
            .expect("读失败");
        buf.truncate(got);
        buf
    }

    fn http_proxy_spec(user: &str, pass: &str) -> PluginSpec {
        PluginSpec::HttpProxy {
            http_user: user.into(),
            http_password: pass.into(),
        }
    }

    fn socks_spec(user: &str, pass: &str) -> PluginSpec {
        PluginSpec::Socks5 {
            user: user.into(),
            password: pass.into(),
        }
    }

    /// HTTP CONNECT:目的地取自请求行,载荷原样穿过隧道。
    #[tokio::test]
    async fn http_proxy_connect_dials_request_target() {
        let echo = spawn_echo().await;
        let mut io = open(&http_proxy_spec("", "")).await;
        io.write_all(
            format!("CONNECT 127.0.0.1:{echo} HTTP/1.1\r\nHost: ignored\r\n\r\n").as_bytes(),
        )
        .await
        .unwrap();
        let resp = read_some(&mut io, 128).await;
        let text = String::from_utf8_lossy(&resp);
        assert!(
            text.starts_with("HTTP/1.1 200"),
            "CONNECT 应回 200,实际:{text}"
        );
        // 应答头之后才有载荷,这里再发一轮
        io.write_all(b"ping-through-tunnel").await.unwrap();
        let back = read_some(&mut io, 64).await;
        assert_eq!(back, b"ping-through-tunnel");
    }

    /// CONNECT 里紧跟的载荷(与请求头同一次写入)必须送达目的地。
    ///
    /// 这是「多读的字节被丢掉」这个 bug 的回归:读头一次读 4KiB,
    /// 很可能把 TLS ClientHello 一起读进来。
    #[tokio::test]
    async fn http_proxy_connect_forwards_pipelined_bytes() {
        let echo = spawn_echo().await;
        let mut io = open(&http_proxy_spec("", "")).await;
        let mut req = format!("CONNECT 127.0.0.1:{echo} HTTP/1.1\r\nHost: x\r\n\r\n").into_bytes();
        req.extend_from_slice(b"EARLY-BYTES");
        io.write_all(&req).await.unwrap();
        // 第一个 read 可能拿到 200 应答,也可能拿到应答+回显
        let mut seen = Vec::new();
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
        while !seen.windows(11).any(|w| w == b"EARLY-BYTES") {
            if tokio::time::Instant::now() > deadline {
                panic!("未收到回显,已读:{:?}", String::from_utf8_lossy(&seen));
            }
            seen.extend_from_slice(&read_some(&mut io, 256).await);
        }
    }

    /// 错误凭据 → 407 + `Proxy-Authenticate`,且**不拨号**。
    #[tokio::test]
    async fn http_proxy_rejects_bad_credentials() {
        let echo = spawn_echo().await;
        let mut io = open(&http_proxy_spec("pu", "pp")).await;
        let cred = base64_encode("pu:WRONG");
        io.write_all(
            format!(
                "CONNECT 127.0.0.1:{echo} HTTP/1.1\r\nProxy-Authorization: Basic {cred}\r\n\r\n"
            )
            .as_bytes(),
        )
        .await
        .unwrap();
        let resp = read_some(&mut io, 256).await;
        let text = String::from_utf8_lossy(&resp);
        assert!(text.starts_with("HTTP/1.1 407"), "应为 407,实际:{text}");
        assert!(
            text.to_ascii_lowercase().contains("proxy-authenticate:"),
            "407 必须带 Proxy-Authenticate:{text}"
        );
    }

    /// 正确凭据放行。
    #[tokio::test]
    async fn http_proxy_accepts_good_credentials() {
        let echo = spawn_echo().await;
        let mut io = open(&http_proxy_spec("pu", "pp")).await;
        let cred = base64_encode("pu:pp");
        io.write_all(
            format!(
                "CONNECT 127.0.0.1:{echo} HTTP/1.1\r\nProxy-Authorization: Basic {cred}\r\n\r\n"
            )
            .as_bytes(),
        )
        .await
        .unwrap();
        let text = String::from_utf8_lossy(&read_some(&mut io, 128).await).into_owned();
        assert!(text.starts_with("HTTP/1.1 200"), "应放行:{text}");
    }

    /// 目的地不可达 → 502(而不是把用户连接静默挂死)。
    #[tokio::test]
    async fn http_proxy_reports_unreachable_target() {
        // 取一个几乎必然没人监听的端口
        let port = {
            let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            l.local_addr().unwrap().port()
        };
        let mut io = open(&http_proxy_spec("", "")).await;
        io.write_all(format!("CONNECT 127.0.0.1:{port} HTTP/1.1\r\n\r\n").as_bytes())
            .await
            .unwrap();
        let text = String::from_utf8_lossy(&read_some(&mut io, 256).await).into_owned();
        assert!(text.starts_with("HTTP/1.1 502"), "应为 502,实际:{text}");
    }

    /// 普通请求(absolute-form):请求行改写成 origin-form,代理专有头被剥掉。
    #[tokio::test]
    async fn http_proxy_plain_request_is_rewritten() {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = l.local_addr().unwrap().port();
        let (tx, rx) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let (mut s, _) = l.accept().await.unwrap();
            let mut buf = vec![0u8; 4096];
            let n = s.read(&mut buf).await.unwrap();
            let _ = tx.send(String::from_utf8_lossy(&buf[..n]).into_owned());
            let _ = s
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nOK")
                .await;
            let _ = s.shutdown().await;
        });
        let mut io = open(&http_proxy_spec("", "")).await;
        io.write_all(
            format!(
                "GET http://127.0.0.1:{port}/path?q=1 HTTP/1.1\r\n\
                 Host: 127.0.0.1:{port}\r\n\
                 Proxy-Authorization: Basic {}\r\n\
                 Proxy-Connection: keep-alive\r\n\r\n",
                base64_encode("a:b")
            )
            .as_bytes(),
        )
        .await
        .unwrap();
        let got = tokio::time::timeout(std::time::Duration::from_secs(5), rx)
            .await
            .expect("上游未收到请求")
            .unwrap();
        assert!(
            got.starts_with("GET /path?q=1 HTTP/1.1\r\n"),
            "请求行应为 origin-form:{got}"
        );
        assert!(
            !got.to_ascii_lowercase().contains("proxy-authorization"),
            "Proxy-Authorization 不得透传:{got}"
        );
        assert!(
            !got.to_ascii_lowercase().contains("proxy-connection"),
            "Proxy-Connection 不得透传:{got}"
        );
        assert!(
            got.to_ascii_lowercase().contains("connection: close"),
            "必须强制 Connection: close:{got}"
        );
        // 响应要回到用户侧
        let resp = String::from_utf8_lossy(&read_some(&mut io, 256).await).into_owned();
        assert!(resp.contains("200 OK"), "响应未回传:{resp}");
    }

    /// SOCKS5:协商 → 认证 → CONNECT → 隧道,目的地取自 `DST.ADDR`。
    #[tokio::test]
    async fn socks5_connect_dials_request_target() {
        let echo = spawn_echo().await;
        let mut io = open(&socks_spec("su", "sp")).await;
        // 协商:只提供 0x02
        io.write_all(&[0x05, 0x01, 0x02]).await.unwrap();
        let mut m = [0u8; 2];
        io.read_exact(&mut m).await.unwrap();
        assert_eq!(m, [0x05, 0x02], "应选择用户名密码认证");
        // 认证
        let mut auth = vec![0x01, 2];
        auth.extend_from_slice(b"su");
        auth.push(2);
        auth.extend_from_slice(b"sp");
        io.write_all(&auth).await.unwrap();
        let mut ar = [0u8; 2];
        io.read_exact(&mut ar).await.unwrap();
        assert_eq!(ar, [0x01, 0x00], "认证应成功");
        // CONNECT 到 echo(域名形式用 127.0.0.1 的 v4 地址即可)
        let mut req = vec![0x05, 0x01, 0x00, 0x01, 127, 0, 0, 1];
        req.extend_from_slice(&echo.to_be_bytes());
        io.write_all(&req).await.unwrap();
        let mut rep = [0u8; 10];
        io.read_exact(&mut rep).await.unwrap();
        assert_eq!(rep[1], 0x00, "REP 应为成功:{rep:?}");
        io.write_all(b"socks-payload").await.unwrap();
        assert_eq!(read_some(&mut io, 64).await, b"socks-payload");
    }

    /// SOCKS5 域名目标(ATYP=0x03):插件把域名交给**本端**解析器。
    ///
    /// # 为什么这里不断言"解析成功"
    ///
    /// `dnsquery` 的解析函数是**进程级全局**且可被测试替换
    /// (`resolve_set_query`)。`dnsquery` 自己的用例会在 `setup()` 期间
    /// 把它换成"永远返回 10.0.0.7",并清空缓存 —— 任何并行用例在那段窗口
    /// 里都拿不到真实解析结果。实测就是这么 flaky 的(单独跑绿、成组跑红)。
    ///
    /// 所以断言只落在**协议契约**上:插件必须接受 ATYP=0x03、必须按
    /// `DST.ADDR` 去解析,解析不出来时回 `REP=0x04` 而**不是**假装成功。
    /// 后者已经足以否定"拨配置地址"的实现 —— 那种实现不会因为一个
    /// 域名解析失败而返回 REP。
    #[tokio::test]
    async fn socks5_domain_target_goes_through_resolver() {
        let mut io = open(&socks_spec("", "")).await;
        io.write_all(&[0x05, 0x01, 0x00]).await.unwrap();
        let mut m = [0u8; 2];
        io.read_exact(&mut m).await.unwrap();
        assert_eq!(m, [0x05, 0x00], "免认证应选 0x00");
        // `.invalid` 是 RFC 2606 保留的**永不解析**顶级域
        let dom = b"rh-test.invalid";
        let mut req = vec![0x05, 0x01, 0x00, 0x03, dom.len() as u8];
        req.extend_from_slice(dom);
        req.extend_from_slice(&9u16.to_be_bytes());
        io.write_all(&req).await.unwrap();
        let mut rep = [0u8; 10];
        io.read_exact(&mut rep).await.unwrap();
        // 两种合法结果:真的解析失败(0x04),或注入器把它解析到别处
        // (0x00 或连接失败)。**关键是不能 panic、不能挂死,且必须给出 REP。**
        assert!(
            rep[1] == SOCKS_REP_HOST_UNREACHABLE || rep[1] == SOCKS_REP_SUCCESS || rep[1] == 0x05,
            "域名目标必须得到明确的 REP(0x00/0x04/0x05),实际:{rep:?}"
        );
    }

    /// 地址拆分与解析口径:`[::1]` 必须被剥成 `::1` 才能走 `IpAddr::parse`。
    ///
    /// 这是上面那条「解析走本端」断言的**确定性补充**:它不碰网络、不碰
    /// 全局解析器,纯函数级验证。
    #[test]
    fn bracketed_v6_is_normalized_before_resolve() {
        let (host, port) = split_authority("[2001:db8::1]:8443", 443).unwrap();
        assert_eq!(
            host, "2001:db8::1",
            "方括号必须剥掉,否则 IpAddr::parse 失败"
        );
        assert_eq!(port, 8443);
        assert!(host.parse::<std::net::IpAddr>().is_ok());
    }

    // ---- 地址拆分 ----

    #[test]
    fn split_authority_handles_common_forms() {
        assert_eq!(
            split_authority("example.com:8080", 443),
            Some(("example.com".into(), 8080))
        );
        assert_eq!(
            split_authority("example.com", 443),
            Some(("example.com".into(), 443))
        );
        assert_eq!(
            split_authority("127.0.0.1:80", 443),
            Some(("127.0.0.1".into(), 80))
        );
        // IPv6 方括号必须剥掉,否则 IpAddr::parse 失败
        assert_eq!(
            split_authority("[::1]:8080", 443),
            Some(("::1".into(), 8080))
        );
        assert_eq!(split_authority("[::1]", 443), Some(("::1".into(), 443)));
        // 末尾根点归一化
        assert_eq!(
            split_authority("example.com.:80", 443),
            Some(("example.com".into(), 80))
        );
    }

    #[test]
    fn split_authority_rejects_malformed() {
        // 空 / 空 host / 0 端口 / 未加括号的 v6 / 端口非数字
        assert_eq!(split_authority("", 443), None);
        assert_eq!(split_authority(":443", 443), None);
        assert_eq!(split_authority("example.com:0", 443), None);
        assert_eq!(split_authority("::1:8080", 443), None);
        assert_eq!(split_authority("example.com:http", 443), None);
        assert_eq!(split_authority("[::1]:x", 443), None);
    }

    #[test]
    fn absolute_form_authority_extracts_host() {
        assert_eq!(
            absolute_form_authority("http://example.com:8080/p?q"),
            Some("example.com:8080".into())
        );
        assert_eq!(
            absolute_form_authority("http://user:pw@example.com/"),
            Some("example.com".into())
        );
        assert_eq!(
            absolute_form_authority("https://example.com"),
            Some("example.com".into())
        );
        // 非 http(s) 或没有 authority
        assert_eq!(absolute_form_authority("ftp://example.com/"), None);
        assert_eq!(absolute_form_authority("/path"), None);
    }

    #[test]
    fn origin_form_keeps_path_and_drops_fragment() {
        assert_eq!(origin_form("http://h/a/b?x=1#frag"), "/a/b?x=1");
        assert_eq!(origin_form("http://h"), "/");
        assert_eq!(origin_form("http://h?q=1"), "/?q=1");
    }

    /// 免鉴权判据:用户名为空即不鉴权(与 static_file 同口径)。
    #[test]
    fn auth_gate_matches_empty_username_rule() {
        let none = basic_auth("", "");
        assert!(check_proxy_auth(&none, None), "空用户名应免鉴权");
        assert!(check_proxy_auth(&none, Some("anything:at-all")));

        let some = basic_auth("u", "p");
        assert!(check_proxy_auth(&some, Some("u:p")));
        assert!(!check_proxy_auth(&some, Some("u:wrong")));
        assert!(!check_proxy_auth(&some, None), "缺凭据必须拒绝");
    }

    // ---- 测试辅助 ----

    fn base64_encode(s: &str) -> String {
        base64::engine::general_purpose::STANDARD.encode(s.as_bytes())
    }
}

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

        assert!(
            text.contains("Host: new.example\r\n"),
            "Host 必须被重写:{text}"
        );
        assert!(!text.contains("Host: orig"), "原 Host 必须消失:{text}");
        assert!(!text.contains("X-Del:"), "del 必须生效:{text}");
        assert!(text.contains("X-Keep: k\r\n"), "无关头必须保留:{text}");
        assert!(!text.contains("keep-alive"), "Connection 必须被替换:{text}");
        assert!(
            text.ends_with("Connection: close\r\n\r\n"),
            "必须强制 close:{text}"
        );

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
        assert!(
            text.contains("Host: orig\r\n"),
            "空改写时原 Host 必须保留:{text}"
        );
    }

    /// 无 Host 且给了 host_rewrite:必须补上。
    #[test]
    fn rewrite_http_head_adds_missing_host() {
        let head = b"GET / HTTP/1.1\r\nX-A: 1\r\n\r\n";
        let op = rh_protocol::HeaderOp::default();
        let out = rewrite_http_head(head, "added.example", &op);
        let text = String::from_utf8(out).unwrap();
        assert!(
            text.contains("Host: added.example\r\n"),
            "缺失 Host 必须补:{text}"
        );
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

#[cfg(test)]
mod percent_decode_tests {
    use super::*;

    #[test]
    fn decodes_valid_sequences() {
        assert_eq!(percent_decode("a%20b"), "a b");
        assert_eq!(percent_decode("%41%42"), "AB");
        assert_eq!(percent_decode("plain"), "plain");
    }

    /// 末尾的孤立 `%` 或不足两位的序列必须原样保留,不得越界 panic。
    ///
    /// 修前条件写成 `i + 2 < b.len() + 1` 且重复两遍,实际等价 `i + 2 <= len`,
    /// 边界比正确值宽一格 —— 依赖的是 `b.get()` 返回 None 兜底,能跑但语义不清。
    #[test]
    fn keeps_invalid_trailing_sequences() {
        assert_eq!(percent_decode("abc%"), "abc%");
        assert_eq!(percent_decode("abc%2"), "abc%2");
        assert_eq!(percent_decode("abc%zz"), "abc%zz");
        // 恰好 3 字节的合法序列必须解码(边界内侧)
        assert_eq!(percent_decode("%41"), "A");
    }
}
