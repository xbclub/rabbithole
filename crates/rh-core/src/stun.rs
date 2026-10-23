//! 最小 STUN 客户端:RFC 5389 Binding Request/Response,
//! 用于 XTCP 打洞前的公网映射地址探测。

use rand::RngCore;
use std::net::SocketAddr;
use std::time::Duration;

const MAGIC_COOKIE: u32 = 0x2112A442;
const BINDING_REQUEST_LEN: usize = 20;
const ATTR_XOR_MAPPED_ADDRESS: u16 = 0x0020;
const ATTR_MAPPED_ADDRESS: u16 = 0x0001;

/// 向 `server` 发送 Binding Request,返回本 socket 的映射地址。
/// 单次、无重试(调用方负责重试策略)。
pub async fn stun_binding(
    sock: &tokio::net::UdpSocket,
    server: SocketAddr,
) -> std::io::Result<SocketAddr> {
    let mut tx_id = [0u8; 12];
    rand::rng().fill_bytes(&mut tx_id);
    let mut req = Vec::with_capacity(BINDING_REQUEST_LEN);
    req.extend_from_slice(&0x0001u16.to_be_bytes()); // Binding Request
    req.extend_from_slice(&0u16.to_be_bytes()); // length
    req.extend_from_slice(&MAGIC_COOKIE.to_be_bytes());
    req.extend_from_slice(&tx_id);
    sock.send_to(&req, server).await?;

    let mut buf = vec![0u8; 1024];
    // 只接受来自 STUN 服务器的应答:共享 socket 场景(打洞)下,
    // 任意先到的 UDP 包不得被当作应答解析(候选投毒)
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    let n = loop {
        if tokio::time::Instant::now() >= deadline {
            return Err(std::io::Error::other("stun timeout"));
        }
        match tokio::time::timeout_at(deadline, sock.recv_from(&mut buf)).await {
            Ok(Ok((n, from))) if from == server => break n,
            Ok(Ok(_)) => continue, // 非服务器来源:丢弃继续等
            Ok(Err(e)) => return Err(e),
            Err(_) => return Err(std::io::Error::other("stun timeout")),
        }
    };
    parse_binding_response(&buf[..n], &tx_id)
}

fn parse_binding_response(pkt: &[u8], tx_id: &[u8; 12]) -> std::io::Result<SocketAddr> {
    if pkt.len() < 20 {
        return Err(std::io::Error::other("stun: short response"));
    }
    let msg_type = u16::from_be_bytes([pkt[0], pkt[1]]);
    if msg_type & 0x0111 != 0x0101 {
        return Err(std::io::Error::other(
            "stun: not a binding response success",
        ));
    }
    let cookie = u32::from_be_bytes([pkt[4], pkt[5], pkt[6], pkt[7]]);
    if cookie != MAGIC_COOKIE || &pkt[8..20] != tx_id {
        return Err(std::io::Error::other("stun: tx id mismatch"));
    }
    let len = u16::from_be_bytes([pkt[2], pkt[3]]) as usize;
    if pkt.len() < 20 + len {
        return Err(std::io::Error::other("stun: truncated attributes"));
    }
    let mut off = 20;
    let end = 20 + len;
    while off + 4 <= end {
        let attr = u16::from_be_bytes([pkt[off], pkt[off + 1]]);
        let alen = u16::from_be_bytes([pkt[off + 2], pkt[off + 3]]) as usize;
        let body_start = off + 4;
        if body_start + alen > end {
            break;
        }
        let body = &pkt[body_start..body_start + alen];
        if attr == ATTR_XOR_MAPPED_ADDRESS && body.len() >= 8 {
            let addr = parse_xor_addr(body, tx_id)?;
            return Ok(addr);
        }
        if attr == ATTR_MAPPED_ADDRESS && body.len() >= 8 {
            let fam = body[1];
            let port = u16::from_be_bytes([body[2], body[3]]);
            let addr = match fam {
                0x01 if body.len() >= 8 => {
                    let mut o = [0u8; 4];
                    o.copy_from_slice(&body[4..8]);
                    std::net::IpAddr::from(o)
                }
                0x02 if body.len() >= 20 => {
                    let mut o = [0u8; 16];
                    o.copy_from_slice(&body[4..20]);
                    std::net::IpAddr::from(o)
                }
                _ => return Err(std::io::Error::other("stun: bad family")),
            };
            return Ok(SocketAddr::new(addr, port));
        }
        off = body_start + alen + ((4 - alen % 4) % 4);
    }
    Err(std::io::Error::other("stun: no mapped address attribute"))
}

fn parse_xor_addr(body: &[u8], tx_id: &[u8; 12]) -> std::io::Result<SocketAddr> {
    if body.len() < 8 {
        return Err(std::io::Error::other("stun: short xor addr"));
    }
    let fam = body[1];
    let xport = u16::from_be_bytes([body[2], body[3]]) ^ (MAGIC_COOKIE >> 16) as u16;
    let port = xport;
    match fam {
        0x01 => {
            let mut x = [0u8; 4];
            x.copy_from_slice(&body[4..8]);
            let cookie = MAGIC_COOKIE.to_be_bytes();
            let ip = std::net::Ipv4Addr::new(
                x[0] ^ cookie[0],
                x[1] ^ cookie[1],
                x[2] ^ cookie[2],
                x[3] ^ cookie[3],
            );
            Ok(SocketAddr::new(std::net::IpAddr::V4(ip), port))
        }
        0x02 => {
            if body.len() < 20 {
                return Err(std::io::Error::other("stun: short xor v6"));
            }
            let mut x = [0u8; 16];
            x.copy_from_slice(&body[4..20]);
            let cookie = MAGIC_COOKIE.to_be_bytes();
            for i in 0..4 {
                x[i] ^= cookie[i];
            }
            for i in 0..12 {
                x[4 + i] ^= tx_id[i];
            }
            Ok(SocketAddr::new(std::net::IpAddr::V6(x.into()), port))
        }
        _ => Err(std::io::Error::other("stun: bad xor family")),
    }
}

/// 测试/隔离环境用迷你 STUN 服务器:回 Binding Response(XOR-MAPPED-ADDRESS)。
/// 仅测试使用(不编进生产二进制;C8)。跨 crate 测试如需使用,
/// 经 rh-core 自身测试覆盖。
#[cfg(test)]
pub async fn spawn_test_stun_server() -> std::io::Result<(SocketAddr, tokio::task::JoinHandle<()>)>
{
    let sock = tokio::net::UdpSocket::bind("127.0.0.1:0").await?;
    let addr = sock.local_addr()?;
    let task = tokio::spawn(async move {
        let mut buf = vec![0u8; 1024];
        loop {
            if let Ok((n, from)) = sock.recv_from(&mut buf).await {
                if n < 20 {
                    continue;
                }
                let tx_id: [u8; 12] = buf[8..20].try_into().unwrap();
                let mut attr = Vec::with_capacity(12);
                attr.push(0);
                attr.push(if from.is_ipv4() { 0x01 } else { 0x02 });
                let xport = from.port() ^ ((MAGIC_COOKIE >> 16) as u16);
                attr.extend_from_slice(&xport.to_be_bytes());
                match from.ip() {
                    std::net::IpAddr::V4(v4) => {
                        let c = MAGIC_COOKIE.to_be_bytes();
                        let o = v4.octets();
                        for i in 0..4 {
                            attr.push(o[i] ^ c[i]);
                        }
                    }
                    std::net::IpAddr::V6(v6) => {
                        let c = MAGIC_COOKIE.to_be_bytes();
                        let o = v6.octets();
                        for i in 0..4 {
                            attr.push(o[i] ^ c[i]);
                        }
                        for i in 0..12 {
                            attr.push(o[4 + i] ^ tx_id[i]);
                        }
                    }
                }
                let attr_body_len = attr.len();
                let msg_len = 4 + attr_body_len;
                let mut out = Vec::with_capacity(20 + msg_len);
                out.extend_from_slice(&0x0101u16.to_be_bytes());
                out.extend_from_slice(&(msg_len as u16).to_be_bytes());
                out.extend_from_slice(&MAGIC_COOKIE.to_be_bytes());
                out.extend_from_slice(&tx_id);
                out.extend_from_slice(&ATTR_XOR_MAPPED_ADDRESS.to_be_bytes());
                out.extend_from_slice(&(attr_body_len as u16).to_be_bytes());
                out.extend_from_slice(&attr);
                let _ = sock.send_to(&out, from).await;
            }
        }
    });
    Ok((addr, task))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn binding_against_test_server() {
        let (stun_addr, task) = spawn_test_stun_server().await.unwrap();
        let sock = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let mapped = stun_binding(&sock, stun_addr).await.unwrap();
        assert_eq!(mapped, sock.local_addr().unwrap());
        task.abort();
    }

    #[tokio::test]
    async fn timeout_when_no_server() {
        // 绑定一个无服务的本地端口
        let blackhole = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let dead = blackhole.local_addr().unwrap();
        drop(blackhole);
        let sock = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let r = stun_binding(&sock, dead).await;
        assert!(r.is_err());
    }
}
