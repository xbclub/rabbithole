//! PROXY protocol v1/v2 编解码(HAProxy 规格)。
//!
//! v1:文本 `PROXY TCP4|TCP6|UNKNOWN src dst sport dport\r\n`
//! v2:二进制 12 字节签名 + 长 16 字节头,支持 TCP4/TCP6/UNIX/UNSPEC。

use std::fmt::Write as _;
use std::net::SocketAddr;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProxyProtoInfo {
    pub src: SocketAddr,
    pub dst: SocketAddr,
}

const V2_SIG: &[u8; 12] = b"\r\n\r\n\x00\r\nQUIT\n";

/// L-F b:dst 按源同族合成。生产调用方的 dst 是 v4 占位(127.0.0.1:0),
/// 用户经 v6 接入时 src 为 v6 —— 修前 v1 产出「TCP6 行 + v4 dst」的
/// 非法头(对端解析必败),v2 静默 UNSPEC 连源地址一起丢。
/// v4→v6 用 ipv4-mapped 合成(信息无损);v6 dst 无法映回 v4 的组合
/// 返回 None(调用方 UNSPEC 兜底,已无法两全)。
fn unify_family(info: &ProxyProtoInfo) -> Option<ProxyProtoInfo> {
    use std::net::IpAddr;
    match (info.src.ip(), info.dst.ip()) {
        (IpAddr::V6(_), IpAddr::V4(d4)) => Some(ProxyProtoInfo {
            src: info.src,
            dst: std::net::SocketAddr::new(IpAddr::V6(d4.to_ipv6_mapped()), info.dst.port()),
        }),
        (IpAddr::V4(_), IpAddr::V6(d6)) => d6.to_ipv4_mapped().map(|v4| ProxyProtoInfo {
            src: info.src,
            dst: std::net::SocketAddr::new(std::net::IpAddr::V4(v4), info.dst.port()),
        }),
        _ => Some(info.clone()),
    }
}

/// 栈上格式化缓冲(fmt::Write 落栈,零堆分配)。
struct StackBuf {
    b: [u8; 128],
    len: usize,
}

impl std::fmt::Write for StackBuf {
    fn write_str(&mut self, s: &str) -> std::fmt::Result {
        if self.len + s.len() > self.b.len() {
            return Err(std::fmt::Error);
        }
        self.b[self.len..][..s.len()].copy_from_slice(s.as_bytes());
        self.len += s.len();
        Ok(())
    }
}

/// 编码 v1 头。
///
/// L-H d(coreProtocol-2,首轮漏收):修前 format! 一次 String 分配 +
/// into_bytes 二次分配;现栈上格式化,仅最终一次分配出 Vec(接口不变)。
pub fn encode_v1(info: &ProxyProtoInfo) -> Vec<u8> {
    let info = match unify_family(info) {
        Some(i) => i,
        None => {
            tracing::debug!(?info, "proxy proto v1: 无法统一地址族,降级 TCP4 占位");
            // v1 无 UNSPEC;保住源地址的前提下选 src 的族
            let dst = if info.src.is_ipv4() {
                "127.0.0.1:0".parse().unwrap()
            } else {
                "[::1]:0".parse().unwrap()
            };
            ProxyProtoInfo { src: info.src, dst }
        }
    };
    let family = if info.src.is_ipv4() { "TCP4" } else { "TCP6" };
    let mut buf = StackBuf {
        b: [0; 128],
        len: 0,
    };
    let _ = write!(
        buf,
        "PROXY {family} {} {} {} {}\r\n",
        info.src.ip(),
        info.dst.ip(),
        info.src.port(),
        info.dst.port()
    );
    buf.b[..buf.len].to_vec()
}

/// 编码 v2 头(PROXY 指令,即包含客户端地址)。
///
/// L-H d:修前中间 `addrs` Vec(chain+collect)再整段拷入 out —— 现
/// 直接分段 extend,无中间分配;混族经 unify_family 合成(L-F b)。
pub fn encode_v2(info: &ProxyProtoInfo) -> Vec<u8> {
    let mut out = Vec::with_capacity(16 + 36);
    out.extend_from_slice(V2_SIG);
    // ver/cmd + fam + len 占位,写完地址段后回填(fam 决定长度)
    out.extend_from_slice(&[0x21, 0x00, 0x00, 0x00]);
    let unified = unify_family(info);
    let fam: u8 = match unified.as_ref() {
        Some(u) => match (&u.src.ip(), &u.dst.ip()) {
            (std::net::IpAddr::V4(s), std::net::IpAddr::V4(d)) => {
                out.extend_from_slice(&s.octets());
                out.extend_from_slice(&d.octets());
                out.extend_from_slice(&u.src.port().to_be_bytes());
                out.extend_from_slice(&u.dst.port().to_be_bytes());
                0x11
            }
            (std::net::IpAddr::V6(s), std::net::IpAddr::V6(d)) => {
                out.extend_from_slice(&s.octets());
                out.extend_from_slice(&d.octets());
                out.extend_from_slice(&u.src.port().to_be_bytes());
                out.extend_from_slice(&u.dst.port().to_be_bytes());
                0x21
            }
            // unify 后理论不可达
            _ => 0x00,
        },
        None => {
            // 真混族且无法映射:仍 UNSPEC,但显式留痕(修前静默)
            tracing::warn!(
                ?info,
                "proxy proto v2: 混族地址无法统一,UNSPEC 降级(源地址丢失)"
            );
            0x00
        }
    };
    out[13] = fam;
    let total = (out.len() - 16) as u16;
    out[14..16].copy_from_slice(&total.to_be_bytes());
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
    // v1 的 v6 地址按规范无括号;SocketAddr 解析需要括号,测试解析器
    // 据族补齐(真实对端实现同理)
    let (src, dst) = if parts[1] == "TCP6" {
        (
            format!("[{}]:{}", parts[2], parts[4]).parse(),
            format!("[{}]:{}", parts[3], parts[5]).parse(),
        )
    } else {
        (
            format!("{}:{}", parts[2], parts[4]).parse(),
            format!("{}:{}", parts[3], parts[5]).parse(),
        )
    };
    let src: SocketAddr = src.map_err(|_| ParseError::Malformed("bad src addr".into()))?;
    let dst: SocketAddr = dst.map_err(|_| ParseError::Malformed("bad dst addr".into()))?;
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

    /// L-F b:混族(src v6 + v4 占位 dst)—— v1 必须产出合法 TCP6 行、
    /// v2 保留 v6 源(修前 v1 行非法、v2 UNSPEC 丢源)。
    #[test]
    fn lh_mixed_family_synthesizes_dst() {
        let info = ProxyProtoInfo {
            src: "[2001:db8::9]:443".parse().unwrap(),
            dst: "127.0.0.1:0".parse().unwrap(),
        };
        let h1 = encode_v1(&info);
        let (p1, _) = parse_header(&h1).unwrap().unwrap();
        assert_eq!(p1.src, info.src, "v1 混族不得丢源");
        assert!(p1.dst.is_ipv6(), "v1 dst 须与源同族");
        let h2 = encode_v2(&info);
        let (p2, _) = parse_header(&h2).unwrap().unwrap();
        assert_eq!(p2.src, info.src, "v2 混族不得 UNSPEC 丢源");
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
