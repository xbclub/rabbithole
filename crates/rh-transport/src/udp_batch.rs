//! UDP 批收发原语:Linux 走 `recvmmsg` / `sendmmsg`,其余平台走非阻塞贪心排空 / 逐包发送。
//!
//! 跨平台纪律:所有 `std::os::fd` 与 `libc` 均严格门控在 `#[cfg(target_os = "linux")]`
//! 内(Cargo.toml 仅在 Linux 目标引入 `libc`),确保 macOS / Windows MSVC 编译无误。

use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

/// 单批最大收发数据报数(对齐常见内核 fastiov / UdpChan 批大小)。
pub const UDP_BATCH_MAX: usize = 32;

/// 可复用的 UDP 批收缓冲区。
pub struct UdpRecvBatch {
    bufs: Vec<Vec<u8>>,
    lens: [usize; UDP_BATCH_MAX],
    addrs: [SocketAddr; UDP_BATCH_MAX],
    count: usize,
    pkt_cap: usize,
}

impl UdpRecvBatch {
    /// 分配可容纳 `UDP_BATCH_MAX` 个报文的可复用批缓冲区。
    pub fn new(pkt_cap: usize) -> Self {
        let cap = pkt_cap.max(1);
        let mut bufs = Vec::with_capacity(UDP_BATCH_MAX);
        for _ in 0..UDP_BATCH_MAX {
            bufs.push(vec![0u8; cap]);
        }
        Self {
            bufs,
            lens: [0; UDP_BATCH_MAX],
            addrs: [SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0)); UDP_BATCH_MAX],
            count: 0,
            pkt_cap: cap,
        }
    }

    /// 本批已接收的有效数据报数量。
    pub fn len(&self) -> usize {
        self.count
    }

    /// 本批是否为空。
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// 迭代本批所有已接收报文 `(payload, peer_addr)`。
    pub fn iter(&self) -> impl Iterator<Item = (&[u8], SocketAddr)> {
        (0..self.count).map(|i| (&self.bufs[i][..self.lens[i]], self.addrs[i]))
    }

    /// 记录单个报文(供仅支持单包 `recv_from` 的 `UdpRecv` 实现使用)。
    pub fn set_single(&mut self, len: usize, addr: SocketAddr) {
        self.lens[0] = len.min(self.pkt_cap);
        self.addrs[0] = addr;
        self.count = 1;
    }

    /// 返回首槽可写缓冲切片(供 `UdpRecv` 默认单包回退使用)。
    pub fn first_buf_mut(&mut self) -> &mut [u8] {
        &mut self.bufs[0]
    }
}

/// 从 `sock` 批量接收最多 `UDP_BATCH_MAX` 个 UDP 数据报写入 `batch`,返回收到的包数。
pub async fn recv_from_batch(
    sock: &tokio::net::UdpSocket,
    batch: &mut UdpRecvBatch,
) -> std::io::Result<usize> {
    batch.count = 0;

    #[cfg(target_os = "linux")]
    {
        sock.async_io(tokio::io::Interest::READABLE, || {
            recvmmsg_nonblock(sock, batch)
        })
        .await
    }

    #[cfg(not(target_os = "linux"))]
    {
        let (n0, addr0) = sock.recv_from(&mut batch.bufs[0]).await?;
        batch.lens[0] = n0.min(batch.pkt_cap);
        batch.addrs[0] = addr0;
        let mut count = 1usize;
        while count < UDP_BATCH_MAX {
            match sock.try_recv_from(&mut batch.bufs[count]) {
                Ok((n, addr)) => {
                    batch.lens[count] = n.min(batch.pkt_cap);
                    batch.addrs[count] = addr;
                    count += 1;
                }
                Err(_) => break,
            }
        }
        batch.count = count;
        Ok(count)
    }
}

/// 非阻塞批量接收最多 `UDP_BATCH_MAX` 个 UDP 数据报写入 `batch`,返回收到的包数。
///
/// 无就绪报文时返回 `Err(ErrorKind::WouldBlock)` 且清除 Tokio 可读就绪位,
/// 供调用方归还池化缓冲后再 `sock.readable().await`(空闲会话零驻留)。
pub fn try_recv_from_batch(
    sock: &tokio::net::UdpSocket,
    batch: &mut UdpRecvBatch,
) -> std::io::Result<usize> {
    batch.count = 0;

    #[cfg(target_os = "linux")]
    {
        sock.try_io(tokio::io::Interest::READABLE, || {
            recvmmsg_nonblock(sock, batch)
        })
    }

    #[cfg(not(target_os = "linux"))]
    {
        let (n0, addr0) = sock.try_recv_from(&mut batch.bufs[0])?;
        batch.lens[0] = n0.min(batch.pkt_cap);
        batch.addrs[0] = addr0;
        let mut count = 1usize;
        while count < UDP_BATCH_MAX {
            match sock.try_recv_from(&mut batch.bufs[count]) {
                Ok((n, addr)) => {
                    batch.lens[count] = n.min(batch.pkt_cap);
                    batch.addrs[count] = addr;
                    count += 1;
                }
                Err(_) => break,
            }
        }
        batch.count = count;
        Ok(count)
    }
}

/// 向 `sock` 批量发送 `pkts` 中的 UDP 数据报,返回已发送包数。
pub async fn send_to_batch(
    sock: &tokio::net::UdpSocket,
    pkts: &[(&[u8], SocketAddr)],
) -> std::io::Result<usize> {
    if pkts.is_empty() {
        return Ok(0);
    }

    #[cfg(target_os = "linux")]
    {
        let mut offset = 0usize;
        while offset < pkts.len() {
            let end = (offset + UDP_BATCH_MAX).min(pkts.len());
            let chunk = &pkts[offset..end];
            let sent = sock
                .async_io(tokio::io::Interest::WRITABLE, || {
                    sendmmsg_nonblock(sock, chunk)
                })
                .await?;
            offset += sent;
        }
        Ok(offset)
    }

    #[cfg(not(target_os = "linux"))]
    {
        let mut sent = 0usize;
        for &(data, addr) in pkts {
            sock.send_to(data, addr).await?;
            sent += 1;
        }
        Ok(sent)
    }
}

#[cfg(target_os = "linux")]
fn recvmmsg_nonblock(
    sock: &tokio::net::UdpSocket,
    batch: &mut UdpRecvBatch,
) -> std::io::Result<usize> {
    use std::os::fd::AsRawFd as _;

    let mut iovs: [libc::iovec; UDP_BATCH_MAX] = unsafe { std::mem::zeroed() };
    let mut hdrs: [libc::mmsghdr; UDP_BATCH_MAX] = unsafe { std::mem::zeroed() };
    let mut addrs: [libc::sockaddr_storage; UDP_BATCH_MAX] = unsafe { std::mem::zeroed() };

    for i in 0..UDP_BATCH_MAX {
        iovs[i].iov_base = batch.bufs[i].as_mut_ptr().cast::<libc::c_void>();
        iovs[i].iov_len = batch.pkt_cap;
        hdrs[i].msg_hdr.msg_name =
            (&mut addrs[i] as *mut libc::sockaddr_storage).cast::<libc::c_void>();
        hdrs[i].msg_hdr.msg_namelen =
            std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t;
        hdrs[i].msg_hdr.msg_iov = &mut iovs[i];
        hdrs[i].msg_hdr.msg_iovlen = 1;
    }

    // musl 中 recvmmsg/sendmmsg 的 flags 为 c_uint,gnu 中为 c_int。
    #[cfg(target_env = "musl")]
    let flags = libc::MSG_DONTWAIT as libc::c_uint;
    #[cfg(not(target_env = "musl"))]
    let flags = libc::MSG_DONTWAIT;

    let rc = loop {
        let rc = unsafe {
            libc::recvmmsg(
                sock.as_raw_fd(),
                hdrs.as_mut_ptr(),
                UDP_BATCH_MAX as libc::c_uint,
                flags,
                std::ptr::null_mut(),
            )
        };
        if rc < 0 {
            let err = std::io::Error::last_os_error();
            if err.kind() == std::io::ErrorKind::Interrupted {
                continue;
            }
            return Err(err);
        }
        break rc;
    };

    if rc == 0 {
        return Err(std::io::Error::from(std::io::ErrorKind::WouldBlock));
    }

    let n = (rc as usize).min(UDP_BATCH_MAX);
    for i in 0..n {
        let addr = parse_sockaddr(&addrs[i], hdrs[i].msg_hdr.msg_namelen)?;
        batch.addrs[i] = addr;
        batch.lens[i] = (hdrs[i].msg_len as usize).min(batch.pkt_cap);
    }
    batch.count = n;
    Ok(n)
}

#[cfg(target_os = "linux")]
fn sendmmsg_nonblock(
    sock: &tokio::net::UdpSocket,
    pkts: &[(&[u8], SocketAddr)],
) -> std::io::Result<usize> {
    use std::os::fd::AsRawFd as _;

    let n = pkts.len().min(UDP_BATCH_MAX);
    if n == 0 {
        return Ok(0);
    }

    let mut iovs: [libc::iovec; UDP_BATCH_MAX] = unsafe { std::mem::zeroed() };
    let mut hdrs: [libc::mmsghdr; UDP_BATCH_MAX] = unsafe { std::mem::zeroed() };
    let mut addrs: [libc::sockaddr_storage; UDP_BATCH_MAX] = unsafe { std::mem::zeroed() };

    for (i, &(data, addr)) in pkts.iter().take(n).enumerate() {
        iovs[i].iov_base = data.as_ptr() as *mut libc::c_void;
        iovs[i].iov_len = data.len();
        let len = fill_sockaddr(&mut addrs[i], addr);
        hdrs[i].msg_hdr.msg_name =
            (&mut addrs[i] as *mut libc::sockaddr_storage).cast::<libc::c_void>();
        hdrs[i].msg_hdr.msg_namelen = len;
        hdrs[i].msg_hdr.msg_iov = &mut iovs[i];
        hdrs[i].msg_hdr.msg_iovlen = 1;
    }

    #[cfg(target_env = "musl")]
    let flags = libc::MSG_DONTWAIT as libc::c_uint;
    #[cfg(not(target_env = "musl"))]
    let flags = libc::MSG_DONTWAIT;

    let rc = loop {
        let rc = unsafe {
            libc::sendmmsg(
                sock.as_raw_fd(),
                hdrs.as_mut_ptr(),
                n as libc::c_uint,
                flags,
            )
        };
        if rc < 0 {
            let err = std::io::Error::last_os_error();
            if err.kind() == std::io::ErrorKind::Interrupted {
                continue;
            }
            return Err(err);
        }
        break rc;
    };

    if rc == 0 {
        return Err(std::io::Error::from(std::io::ErrorKind::WouldBlock));
    }
    Ok(rc as usize)
}

#[cfg(target_os = "linux")]
fn fill_sockaddr(ss: &mut libc::sockaddr_storage, addr: SocketAddr) -> libc::socklen_t {
    match addr {
        SocketAddr::V4(v4) => {
            let a = unsafe { &mut *(ss as *mut libc::sockaddr_storage as *mut libc::sockaddr_in) };
            a.sin_family = libc::AF_INET as libc::sa_family_t;
            a.sin_port = v4.port().to_be();
            a.sin_addr = libc::in_addr {
                s_addr: u32::from_ne_bytes(v4.ip().octets()),
            };
            std::mem::size_of::<libc::sockaddr_in>() as libc::socklen_t
        }
        SocketAddr::V6(v6) => {
            let a = unsafe { &mut *(ss as *mut libc::sockaddr_storage as *mut libc::sockaddr_in6) };
            a.sin6_family = libc::AF_INET6 as libc::sa_family_t;
            a.sin6_port = v6.port().to_be();
            a.sin6_addr = libc::in6_addr {
                s6_addr: v6.ip().octets(),
            };
            a.sin6_flowinfo = v6.flowinfo();
            a.sin6_scope_id = v6.scope_id();
            std::mem::size_of::<libc::sockaddr_in6>() as libc::socklen_t
        }
    }
}

#[cfg(target_os = "linux")]
fn parse_sockaddr(
    ss: &libc::sockaddr_storage,
    namelen: libc::socklen_t,
) -> std::io::Result<SocketAddr> {
    let family = ss.ss_family as libc::c_int;
    if family == libc::AF_INET {
        if (namelen as usize) < std::mem::size_of::<libc::sockaddr_in>() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "truncated ipv4 sockaddr from recvmmsg",
            ));
        }
        let sin = unsafe { &*(ss as *const libc::sockaddr_storage as *const libc::sockaddr_in) };
        let ip = Ipv4Addr::from(sin.sin_addr.s_addr.to_ne_bytes());
        let port = u16::from_be(sin.sin_port);
        Ok(SocketAddr::V4(SocketAddrV4::new(ip, port)))
    } else if family == libc::AF_INET6 {
        if (namelen as usize) < std::mem::size_of::<libc::sockaddr_in6>() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "truncated ipv6 sockaddr from recvmmsg",
            ));
        }
        let sin6 = unsafe { &*(ss as *const libc::sockaddr_storage as *const libc::sockaddr_in6) };
        let ip = std::net::Ipv6Addr::from(sin6.sin6_addr.s6_addr);
        let port = u16::from_be(sin6.sin6_port);
        Ok(SocketAddr::V6(std::net::SocketAddrV6::new(
            ip,
            port,
            sin6.sin6_flowinfo,
            sin6.sin6_scope_id,
        )))
    } else {
        Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("unsupported sockaddr family {family} from recvmmsg"),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::net::UdpSocket;

    #[tokio::test]
    async fn ipv4_loopback_multi_packet_send_and_recv_batch_roundtrip() {
        let rx = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let tx = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let rx_addr = rx.local_addr().unwrap();
        let tx_addr = tx.local_addr().unwrap();

        // 超过 UDP_BATCH_MAX(32) 以同时覆盖多块 send_to_batch 与循环 recv_from_batch
        let payloads: Vec<Vec<u8>> = (0..40usize)
            .map(|i| format!("udp-batch-pkt-{i:02}-payload").into_bytes())
            .collect();
        let pkts: Vec<(&[u8], SocketAddr)> =
            payloads.iter().map(|p| (p.as_slice(), rx_addr)).collect();

        let sent = send_to_batch(&tx, &pkts).await.unwrap();
        assert_eq!(sent, payloads.len(), "send_to_batch 必须发送全部报文");
        assert_eq!(send_to_batch(&tx, &[]).await.unwrap(), 0);

        let mut batch = UdpRecvBatch::new(1500);
        assert!(batch.is_empty());
        let mut received = Vec::with_capacity(payloads.len());
        while received.len() < payloads.len() {
            let n = tokio::time::timeout(Duration::from_secs(2), recv_from_batch(&rx, &mut batch))
                .await
                .expect("loopback udp batch recv timeout")
                .unwrap();
            assert!((1..=UDP_BATCH_MAX).contains(&n));
            assert_eq!(batch.len(), n);
            assert!(!batch.is_empty());
            for (data, peer) in batch.iter() {
                assert_eq!(peer, tx_addr, "对端源地址必须精确匹配发送 socket");
                received.push(data.to_vec());
            }
        }
        assert_eq!(received, payloads, "收到的批载荷必须与发出完全一致");
    }

    #[tokio::test]
    async fn try_recv_from_batch_would_block_and_drains_burst() {
        let rx = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let tx = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let rx_addr = rx.local_addr().unwrap();
        let tx_addr = tx.local_addr().unwrap();

        let mut batch = UdpRecvBatch::new(1500);
        let empty = try_recv_from_batch(&rx, &mut batch);
        assert_eq!(
            empty.unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock,
            "空 socket 非阻塞批收必须返回 WouldBlock"
        );

        let payloads: Vec<Vec<u8>> = (0..10usize)
            .map(|i| format!("try-udp-batch-{i:02}").into_bytes())
            .collect();
        let pkts: Vec<(&[u8], SocketAddr)> =
            payloads.iter().map(|p| (p.as_slice(), rx_addr)).collect();
        assert_eq!(send_to_batch(&tx, &pkts).await.unwrap(), payloads.len());

        let mut received = Vec::with_capacity(payloads.len());
        while received.len() < payloads.len() {
            tokio::time::timeout(Duration::from_secs(2), rx.readable())
                .await
                .expect("readable timeout")
                .unwrap();
            match try_recv_from_batch(&rx, &mut batch) {
                Ok(n) => {
                    assert!(n >= 1);
                    for (data, peer) in batch.iter() {
                        assert_eq!(peer, tx_addr);
                        received.push(data.to_vec());
                    }
                }
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => continue,
                Err(e) => panic!("unexpected try_recv_from_batch error: {e}"),
            }
        }
        assert_eq!(received, payloads);
        assert_eq!(
            try_recv_from_batch(&rx, &mut batch).unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock
        );
    }
}
