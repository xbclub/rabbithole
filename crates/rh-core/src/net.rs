//! 地址与端口工具:端口范围、绑定地址校验、IP 解析(IPv4/IPv6)。

use std::net::IpAddr;

/// 端口范围段(含两端),支持单端口 "8080"、区间 "6000-6100"、"6000-6000"。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PortsRange {
    pub start: u16,
    pub end: u16,
}

impl PortsRange {
    pub fn parse(s: &str) -> Option<Self> {
        let s = s.trim();
        if let Some((a, b)) = s.split_once('-') {
            let start: u16 = a.trim().parse().ok()?;
            let end: u16 = b.trim().parse().ok()?;
            if start <= end {
                Some(Self { start, end })
            } else {
                None
            }
        } else {
            let p: u16 = s.parse().ok()?;
            Some(Self { start: p, end: p })
        }
    }

    pub fn contains(&self, port: u16) -> bool {
        port >= self.start && port <= self.end
    }
}

/// 端口白名单。
#[derive(Debug, Clone, Default)]
pub struct PortAllowList {
    pub ranges: Vec<PortsRange>,
}

impl PortAllowList {
    /// 空 = 允许全部。
    pub fn is_empty(&self) -> bool {
        self.ranges.is_empty()
    }

    pub fn allows(&self, port: u16) -> bool {
        self.is_empty() || self.ranges.iter().any(|r| r.contains(port))
    }
}

/// 校验 bind 地址字符串:必须是合法 IP 字面量(空 = 0.0.0.0)。
pub fn parse_bind_ip(s: &str) -> Option<IpAddr> {
    let s = s.trim();
    if s.is_empty() {
        return Some(IpAddr::from([0, 0, 0, 0]));
    }
    s.parse().ok()
}

/// 地址冲突判定:两个 (proto, ip, port) 是否会争抢同一监听。
/// 通配 0.0.0.0/:: 与任何同协议同端口冲突;IPv4 与 IPv6 双栈视为不同命名空间
/// (bind 语义由 OS IPV6_V6ONLY 决定,这里保守处理同族即可)。
pub fn bind_conflicts(
    a_proto: &str,
    a_ip: IpAddr,
    a_port: u16,
    b_proto: &str,
    b_ip: IpAddr,
    b_port: u16,
) -> bool {
    if a_proto != b_proto {
        return false; // TCP/UDP 独立命名空间
    }
    if a_port != b_port {
        return false;
    }
    let unspecified = |ip: IpAddr| {
        matches!(ip, IpAddr::V4(v) if v.is_unspecified())
            || matches!(ip, IpAddr::V6(v) if v.is_unspecified())
    };
    if unspecified(a_ip) || unspecified(b_ip) {
        return true;
    }
    // IPv6 双栈监听(::)与 IPv4 任意地址也可能冲突,保守判定为冲突
    let v6_unspecified = |ip: IpAddr| matches!(ip, IpAddr::V6(v) if v.is_unspecified());
    if v6_unspecified(a_ip) || v6_unspecified(b_ip) {
        return true;
    }
    a_ip == b_ip
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ports_range_parse() {
        assert_eq!(
            PortsRange::parse("8080").unwrap(),
            PortsRange {
                start: 8080,
                end: 8080
            }
        );
        assert_eq!(
            PortsRange::parse("6000-6100").unwrap(),
            PortsRange {
                start: 6000,
                end: 6100
            }
        );
        assert!(PortsRange::parse("6100-6000").is_none());
        assert!(PortsRange::parse("abc").is_none());
    }

    #[test]
    fn allow_list() {
        let mut l = PortAllowList::default();
        assert!(l.allows(1));
        l.ranges.push(PortsRange::parse("6000-6100").unwrap());
        l.ranges.push(PortsRange::parse("8080").unwrap());
        assert!(l.allows(6000));
        assert!(l.allows(6100));
        assert!(l.allows(8080));
        assert!(!l.allows(8081));
        assert!(!l.allows(5999));
    }

    #[test]
    fn bind_ip_parse() {
        assert_eq!(
            parse_bind_ip("0.0.0.0").unwrap(),
            IpAddr::from([0, 0, 0, 0])
        );
        assert_eq!(parse_bind_ip("").unwrap(), IpAddr::from([0, 0, 0, 0]));
        assert_eq!(parse_bind_ip("::").unwrap(), IpAddr::from([0u16; 8]));
        assert!(parse_bind_ip("localhost").is_none());
    }

    #[test]
    fn conflict_detection() {
        use std::net::Ipv4Addr;
        let any = IpAddr::V4(Ipv4Addr::UNSPECIFIED);
        let lo = IpAddr::V4(Ipv4Addr::LOCALHOST);
        assert!(bind_conflicts("tcp", any, 80, "tcp", lo, 80));
        assert!(bind_conflicts("tcp", lo, 80, "tcp", any, 80));
        assert!(bind_conflicts("tcp", lo, 80, "tcp", lo, 80));
        assert!(!bind_conflicts("tcp", lo, 80, "tcp", lo, 81));
        assert!(!bind_conflicts(
            "tcp",
            lo,
            80,
            "tcp",
            IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)),
            80
        ));
        // TCP 与 UDP 端口命名空间独立
        assert!(!bind_conflicts("tcp", lo, 80, "udp", lo, 80));
    }
}
