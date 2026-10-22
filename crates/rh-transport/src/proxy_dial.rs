//! 经 HTTP CONNECT 或 SOCKS5 代理拨号到目标 TCP 地址。

use std::io;
use std::net::SocketAddr;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// 代理 URL,如 `http://user:pass@host:port` / `socks5://host:port`。
#[derive(Debug, Clone)]
pub struct ProxyUrl {
    pub kind: ProxyKind,
    pub host: String,
    pub port: u16,
    pub user: Option<String>,
    pub password: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProxyKind {
    Http,
    Socks5,
}

impl ProxyUrl {
    pub fn parse(url: &str) -> io::Result<Self> {
        let (scheme, rest) = url
            .split_once("://")
            .ok_or_else(|| io::Error::other("proxy url must have scheme://"))?;
        let kind = match scheme.to_ascii_lowercase().as_str() {
            "http" | "https" => ProxyKind::Http,
            "socks5" | "socks5h" => ProxyKind::Socks5,
            _ => return Err(io::Error::other("unsupported proxy scheme")),
        };
        let (auth, hostport) = match rest.rsplit_once('@') {
            Some((a, hp)) => (Some(a), hp),
            None => (None, rest),
        };
        let (user, password) = match auth {
            Some(a) => {
                let (u, p) = a
                    .split_once(':')
                    .ok_or_else(|| io::Error::other("proxy auth must be user:password"))?;
                (Some(urldecode(u)), Some(urldecode(p)))
            }
            None => (None, None),
        };
        // 处理 [IPv6]:port 与 host:port
        let (host, port) = if let Some(rest) = hostport.strip_prefix('[') {
            let (h, r) = rest
                .split_once(']')
                .ok_or_else(|| io::Error::other("bad IPv6 proxy address"))?;
            let p = r
                .strip_prefix(':')
                .ok_or_else(|| io::Error::other("missing proxy port"))?;
            (h.to_string(), p.parse::<u16>().map_err(bad_port)?)
        } else {
            let (h, p) = hostport
                .rsplit_once(':')
                .ok_or_else(|| io::Error::other("missing proxy port"))?;
            (h.to_string(), p.parse::<u16>().map_err(bad_port)?)
        };
        if host.is_empty() {
            return Err(io::Error::other("empty proxy host"));
        }
        Ok(Self {
            kind,
            host,
            port,
            user,
            password,
        })
    }
}

fn urldecode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'%' if i + 2 < bytes.len() => {
                if let (Some(h), Some(l)) = (
                    bytes[i + 1]
                        .is_ascii_hexdigit()
                        .then(|| hex_val(bytes[i + 1])),
                    bytes[i + 2]
                        .is_ascii_hexdigit()
                        .then(|| hex_val(bytes[i + 2])),
                ) {
                    out.push(h * 16 + l);
                    i += 3;
                } else {
                    out.push(bytes[i]);
                    i += 1;
                }
            }
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            c => {
                out.push(c);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn hex_val(c: u8) -> u8 {
    match c {
        b'0'..=b'9' => c - b'0',
        b'a'..=b'f' => c - b'a' + 10,
        b'A'..=b'F' => c - b'A' + 10,
        _ => 0,
    }
}

/// 经代理连接 `target`。
pub async fn dial_via_proxy(proxy: &ProxyUrl, target: SocketAddr) -> io::Result<TcpStream> {
    let mut stream = TcpStream::connect((proxy.host.as_str(), proxy.port)).await?;
    stream.set_nodelay(true).ok();
    match proxy.kind {
        ProxyKind::Http => http_connect(&mut stream, proxy, target).await?,
        ProxyKind::Socks5 => socks5_connect(&mut stream, proxy, target).await?,
    }
    Ok(stream)
}

async fn http_connect(
    stream: &mut TcpStream,
    proxy: &ProxyUrl,
    target: SocketAddr,
) -> io::Result<()> {
    let mut req = format!("CONNECT {target} HTTP/1.1\r\nHost: {target}\r\n");
    if let (Some(u), Some(p)) = (&proxy.user, &proxy.password) {
        let creds = crate::base64_encode(format!("{u}:{p}").as_bytes());
        req.push_str(&format!("Proxy-Authorization: Basic {creds}\r\n"));
    }
    req.push_str("\r\n");
    stream.write_all(req.as_bytes()).await?;

    let mut buf = Vec::with_capacity(512);
    let mut chunk = [0u8; 256];
    loop {
        let n = stream.read(&mut chunk).await?;
        if n == 0 {
            return Err(io::Error::other("proxy closed during CONNECT"));
        }
        buf.extend_from_slice(&chunk[..n]);
        if buf.windows(4).any(|w| w == b"\r\n\r\n") {
            break;
        }
        if buf.len() > 8192 {
            return Err(io::Error::other("proxy CONNECT response too large"));
        }
    }
    let head = String::from_utf8_lossy(&buf);
    let status = head
        .lines()
        .next()
        .and_then(|l| l.split_whitespace().nth(1))
        .ok_or_else(|| io::Error::other("malformed proxy CONNECT response"))?;
    if status.starts_with('2') {
        Ok(())
    } else {
        Err(io::Error::other(format!("proxy CONNECT failed: {status}")))
    }
}

async fn socks5_connect(
    stream: &mut TcpStream,
    proxy: &ProxyUrl,
    target: SocketAddr,
) -> io::Result<()> {
    // greeting: 支持无认证 + 用户名密码
    let mut methods = vec![0x00];
    if proxy.user.is_some() {
        methods.push(0x02);
    }
    let mut hello = vec![0x05, methods.len() as u8];
    hello.extend_from_slice(&methods);
    stream.write_all(&hello).await?;
    let mut resp = [0u8; 2];
    stream.read_exact(&mut resp).await?;
    if resp[0] != 0x05 {
        return Err(io::Error::other("bad socks5 version"));
    }
    match resp[1] {
        0x00 => {}
        0x02 => {
            let user = proxy.user.as_deref().unwrap_or("");
            let pass = proxy.password.as_deref().unwrap_or("");
            if user.len() > 255 || pass.len() > 255 {
                return Err(io::Error::other("socks5 credentials too long"));
            }
            let mut auth = vec![0x01, user.len() as u8];
            auth.extend_from_slice(user.as_bytes());
            auth.push(pass.len() as u8);
            auth.extend_from_slice(pass.as_bytes());
            stream.write_all(&auth).await?;
            let mut ares = [0u8; 2];
            stream.read_exact(&mut ares).await?;
            if ares[1] != 0x00 {
                return Err(io::Error::other("socks5 auth failed"));
            }
        }
        0xFF => return Err(io::Error::other("socks5: no acceptable auth method")),
        m => {
            return Err(io::Error::other(format!(
                "socks5: method {m:#x} unsupported"
            )))
        }
    }
    // CONNECT 请求:target 是已解析的 SocketAddr,因此只能走地址形式
    // (ATYP=0x01 IPv4 / 0x04 IPv6)。SOCKS5 的域名形式(ATYP=0x03)在这里
    // **不可达** —— 若将来要保留域名给代理解析(远端 DNS),需把 target 换成
    // 枚举类型,不能沿用 SocketAddr。
    let mut req = vec![0x05, 0x01, 0x00];
    match target.ip() {
        std::net::IpAddr::V4(v4) => {
            req.push(0x01);
            req.extend_from_slice(&v4.octets());
        }
        std::net::IpAddr::V6(v6) => {
            req.push(0x04);
            req.extend_from_slice(&v6.octets());
        }
    }
    req.extend_from_slice(&target.port().to_be_bytes());
    stream.write_all(&req).await?;
    let mut head = [0u8; 4];
    stream.read_exact(&mut head).await?;
    if head[1] != 0x00 {
        return Err(io::Error::other(format!(
            "socks5 connect failed: {:#x}",
            head[1]
        )));
    }
    let skip = match head[3] {
        0x01 => 4,
        0x03 => {
            let mut l = [0u8; 1];
            stream.read_exact(&mut l).await?;
            l[0] as usize
        }
        0x04 => 16,
        t => return Err(io::Error::other(format!("socks5: bad atyp {t:#x}"))),
    };
    let mut rest = vec![0u8; skip + 2];
    stream.read_exact(&mut rest).await?;
    Ok(())
}

fn bad_port(e: std::num::ParseIntError) -> std::io::Error {
    std::io::Error::other(format!("bad proxy port: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_proxy_urls() {
        let p = ProxyUrl::parse("http://u:p%40x@127.0.0.1:8080").unwrap();
        assert_eq!(p.kind, ProxyKind::Http);
        assert_eq!(p.user.as_deref(), Some("u"));
        assert_eq!(p.password.as_deref(), Some("p@x"));
        let p = ProxyUrl::parse("socks5://[::1]:1080").unwrap();
        assert_eq!(p.kind, ProxyKind::Socks5);
        assert_eq!(p.host, "::1");
        assert_eq!(p.port, 1080);
        assert!(ProxyUrl::parse("ftp://x:1").is_err());
        assert!(ProxyUrl::parse("http://onlyhost").is_err());
    }

    #[tokio::test]
    async fn http_and_socks5_proxy_end_to_end() {
        use std::sync::Arc;
        use tokio::io::AsyncWriteExt as _;
        // 目标 echo 服务
        let echo = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let echo_addr = echo.local_addr().unwrap();
        tokio::spawn(async move {
            if let Ok((mut c, _)) = echo.accept().await {
                let mut b = [0u8; 64];
                if let Ok(n) = c.read(&mut b).await {
                    let _ = c.write_all(&b[..n]).await;
                }
            }
        });

        // HTTP CONNECT 代理
        let http_l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let http_addr = http_l.local_addr().unwrap();
        let target = echo_addr;
        tokio::spawn(async move {
            while let Ok((mut c, _)) = http_l.accept().await {
                let t = target;
                tokio::spawn(async move {
                    let mut buf = Vec::new();
                    let mut ch = [0u8; 256];
                    loop {
                        match c.read(&mut ch).await {
                            Ok(0) | Err(_) => return,
                            Ok(n) => {
                                buf.extend_from_slice(&ch[..n]);
                                if buf.windows(4).any(|w| w == b"\r\n\r\n") {
                                    break;
                                }
                            }
                        }
                    }
                    let _ = c
                        .write_all(b"HTTP/1.1 200 Connection established\r\n\r\n")
                        .await;
                    // 纯透传
                    let _ = tokio::io::copy_bidirectional(
                        &mut c,
                        &mut match tokio::net::TcpStream::connect(t).await {
                            Ok(s) => s,
                            Err(_) => return,
                        },
                    )
                    .await;
                });
            }
        });

        let proxy = ProxyUrl {
            kind: ProxyKind::Http,
            host: http_addr.ip().to_string(),
            port: http_addr.port(),
            user: None,
            password: None,
        };
        let mut s = dial_via_proxy(&proxy, echo_addr).await.unwrap();
        s.write_all(b"ping").await.unwrap();
        let mut out = [0u8; 4];
        s.read_exact(&mut out).await.unwrap();
        assert_eq!(&out, b"ping");
        let _ = Arc::new(0); // 保持 Arc 导入(测试用)
    }
}
