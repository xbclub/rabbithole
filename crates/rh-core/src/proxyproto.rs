//! PROXY protocol v1/v2 编解码(HAProxy 规格)。
//!
//! v1:文本 `PROXY TCP4|TCP6|UNKNOWN src dst sport dport\r\n`
//! v2:二进制 12 字节签名 + 长 16 字节头,支持 TCP4/TCP6/UNIX/UNSPEC。

use std::net::SocketAddr;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProxyProtoInfo {
    pub src: SocketAddr,
    pub dst: SocketAddr,
}

const V2_SIG: &[u8; 12] = b"\r\n\r\n\x00\r\nQUIT\n";

/// 编码 v1 头。
pub fn encode_v1(info: &ProxyProtoInfo) -> Vec<u8> {
    let family = if info.src.is_ipv4() { "TCP4" } else { "TCP6" };
    format!(
        "PROXY {family} {} {} {} {}\r\n",
        info.src.ip(),
        info.dst.ip(),
        info.src.port(),
        info.dst.port()
    )
    .into_bytes()
}

/// 编码 v2 头(PROXY 指令,即包含客户端地址)。
pub fn encode_v2(info: &ProxyProtoInfo) -> Vec<u8> {
    let mut out = Vec::with_capacity(16 + 36);
    out.extend_from_slice(V2_SIG);
    let (fam, addrs): (u8, Vec<u8>) = match (&info.src.ip(), &info.dst.ip()) {
        (std::net::IpAddr::V4(s), std::net::IpAddr::V4(d)) => (
            0x11,
            s.octets()
                .iter()
                .chain(d.octets().iter())
                .copied()
                .collect(),
        ),
        (std::net::IpAddr::V6(s), std::net::IpAddr::V6(d)) => (
            0x21,
            s.octets()
                .iter()
                .chain(d.octets().iter())
                .copied()
                .collect(),
        ),
        _ => (0x00, Vec::new()), // UNSPEC
    };
    let len = addrs.len() + if fam == 0x00 { 0 } else { 4 };
    out.push(0x21); // version 2 | command PROXY
    out.push(fam);
    out.extend_from_slice(&(len as u16).to_be_bytes());
    // 规范顺序:src IP, dst IP, src port, dst port
    out.extend_from_slice(&addrs);
    if fam != 0x00 {
        out.extend_from_slice(&info.src.port().to_be_bytes());
        out.extend_from_slice(&info.dst.port().to_be_bytes());
    }
    out
}

/// 尝试从缓冲解析 PROXY protocol 头(v1 或 v2)。
/// `Ok(None)` = 数据不足(需要更多字节);
/// `Ok(Some((info, consumed)))` = 解析成功。
/// 不以 PROXY 协议开头时返回 `Err(NotProxyProtocol)`(调用方按普通流量处理)。
#[derive(Debug, thiserror::Error)]
pub enum ParseError {
    #[error("not a proxy protocol header")]
    NotProxyProtocol,
    #[error("malformed proxy protocol header: {0}")]
    Malformed(String),
    #[error("proxy protocol header too large")]
    TooLarge,
}

/// 仅测试使用:服务端不解析入站 PROXY 头,客户端只用 encode_v1/v2。
/// 保留在 cfg(test) 内供 encode↔parse 往返测试,不编进生产二进制
/// (parse_v1 内部含逐连接 Vec/format 开销,接入生产前需先改写)。
#[cfg(test)]
pub fn parse_header(buf: &[u8]) -> Result<Option<(ProxyProtoInfo, usize)>, ParseError> {
    if buf.is_empty() {
        return Ok(None);
    }
    // 两条线的部分前缀都可能是"尚不完整"
    if buf.len() < 12 && V2_SIG[..buf.len()] == buf[..] {
        return Ok(None);
    }
    if buf.len() < 5 && buf[..] == b"PROXY"[..buf.len()] {
        return Ok(None);
    }
    if buf.len() >= 12 && &buf[..12] == V2_SIG {
        return parse_v2(buf);
    }
    if buf[..buf.len().min(5)] == b"PROXY"[..buf.len().min(5)] {
        return parse_v1(buf);
    }
    Err(ParseError::NotProxyProtocol)
}

#[cfg(test)]
fn parse_v1(buf: &[u8]) -> Result<Option<(ProxyProtoInfo, usize)>, ParseError> {
    let end = match buf.windows(2).position(|w| w == b"\r\n") {
        Some(p) => p + 2,
        None => {
            if buf.len() > 108 {
                return Err(ParseError::TooLarge);
            }
            return Ok(None);
        }
    };
    let line = std::str::from_utf8(&buf[..end - 2])
        .map_err(|_| ParseError::Malformed("invalid utf8".into()))?;
    let parts: Vec<&str> = line.split(' ').collect();
    if parts.len() != 6 {
        return Err(ParseError::Malformed("expect 6 fields".into()));
    }
    let src: SocketAddr = format!("{}:{}", parts[2], parts[4])
        .parse()
        .map_err(|_| ParseError::Malformed("bad src addr".into()))?;
    let dst: SocketAddr = format!("{}:{}", parts[3], parts[5])
        .parse()
        .map_err(|_| ParseError::Malformed("bad dst addr".into()))?;
    if (parts[1] == "TCP4" && !src.is_ipv4()) || (parts[1] == "TCP6" && !src.is_ipv6()) {
        return Err(ParseError::Malformed("family mismatch".into()));
    }
    Ok(Some((ProxyProtoInfo { src, dst }, end)))
}

#[cfg(test)]
fn parse_v2(buf: &[u8]) -> Result<Option<(ProxyProtoInfo, usize)>, ParseError> {
    if buf.len() < 16 {
        return Ok(None);
    }
    let ver_cmd = buf[12];
    if ver_cmd >> 4 != 2 {
        return Err(ParseError::Malformed("bad version".into()));
    }
    let fam = buf[13];
    let len = u16::from_be_bytes([buf[14], buf[15]]) as usize;
    if len > 65535 - 16 {
        return Err(ParseError::TooLarge);
    }
    if buf.len() < 16 + len {
        return Ok(None);
    }
    let body = &buf[16..16 + len];
    let (src, dst) = match fam {
        0x11 => {
            if body.len() < 12 {
                return Err(ParseError::Malformed("short v4 body".into()));
            }
            let mut s4 = [0u8; 4];
            let mut d4 = [0u8; 4];
            s4.copy_from_slice(&body[0..4]);
            d4.copy_from_slice(&body[4..8]);
            let sport = u16::from_be_bytes([body[8], body[9]]);
            let dport = u16::from_be_bytes([body[10], body[11]]);
            (
                SocketAddr::from((std::net::Ipv4Addr::from(s4), sport)),
                SocketAddr::from((std::net::Ipv4Addr::from(d4), dport)),
            )
        }
        0x21 => {
            if body.len() < 36 {
                return Err(ParseError::Malformed("short v6 body".into()));
            }
            let mut s6 = [0u8; 16];
            let mut d6 = [0u8; 16];
            s6.copy_from_slice(&body[0..16]);
            d6.copy_from_slice(&body[16..32]);
            let sport = u16::from_be_bytes([body[32], body[33]]);
            let dport = u16::from_be_bytes([body[34], body[35]]);
            (
                SocketAddr::from((std::net::Ipv6Addr::from(s6), sport)),
                SocketAddr::from((std::net::Ipv6Addr::from(d6), dport)),
            )
        }
        0x00 => {
            // UNSPEC:0.0.0.0:0
            ("0.0.0.0:0".parse().unwrap(), "0.0.0.0:0".parse().unwrap())
        }
        _ => return Err(ParseError::Malformed("unsupported family".into())),
    };
    Ok(Some((ProxyProtoInfo { src, dst }, 16 + len)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn v1_roundtrip() {
        let info = ProxyProtoInfo {
            src: "1.2.3.4:5678".parse().unwrap(),
            dst: "10.0.0.1:80".parse().unwrap(),
        };
        let hdr = encode_v1(&info);
        let (parsed, n) = parse_header(&hdr).unwrap().unwrap();
        assert_eq!(parsed, info);
        assert_eq!(n, hdr.len());
    }

    #[test]
    fn v2_roundtrip_v4_v6() {
        for (s, d) in [
            ("1.2.3.4:5678", "10.0.0.1:80"),
            ("[2001:db8::1]:443", "[::ffff:10.0.0.1]:80"),
        ] {
            let info = ProxyProtoInfo {
                src: s.parse().unwrap(),
                dst: d.parse().unwrap(),
            };
            let hdr = encode_v2(&info);
            let (parsed, n) = parse_header(&hdr).unwrap().unwrap();
            assert_eq!(parsed, info);
            assert_eq!(n, hdr.len());
        }
    }

    #[test]
    fn partial_header_needs_more() {
        let info = ProxyProtoInfo {
            src: "1.2.3.4:5678".parse().unwrap(),
            dst: "10.0.0.1:80".parse().unwrap(),
        };
        let hdr = encode_v2(&info);
        assert!(parse_header(&hdr[..8]).unwrap().is_none());
        assert!(parse_header(&hdr[..20]).unwrap().is_none());
    }

    #[test]
    fn non_proxy_rejected() {
        assert!(matches!(
            parse_header(b"GET / HTTP/1.1\r\n"),
            Err(ParseError::NotProxyProtocol)
        ));
        // v1 前缀但未结束
        assert!(parse_header(b"PROXY TCP4 1.2.3").unwrap().is_none());
    }
}
