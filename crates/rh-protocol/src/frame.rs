//! RHTP 帧格式与编解码。
//!
//! ```text
//!  0        4        5          7         11         11+len
//! +--------+--------+----------+---------+----------+-----------+
//! | "RHTP" | ver u8 | type u16 | len u32 |  payload             |
//! +--------+--------+----------+---------+----------+-----------+
//! ```
//!
//! - 多字节整数大端。
//! - `len` 为 payload 字节数,上限 [`MAX_FRAME_LEN`];超限视为协议违例。
//! - TCP 数据连接在收到 `START_FORWARD` 后退化为裸字节流,不再出现帧。

use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::io;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite};

pub const MAGIC: &[u8; 4] = b"RHTP";
pub const PROTOCOL_FRAME_VERSION: u8 = 1;
pub const HEADER_LEN: usize = 11;
/// 帧上限 1 MiB(UDP_PACKET 最大合法报文 64KiB + 头;控制消息远小于此)。
pub const MAX_FRAME_LEN: usize = 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FrameType(pub u16);

impl FrameType {
    // C→S
    pub const HELLO: FrameType = FrameType(0x0001);
    pub const PING: FrameType = FrameType(0x0003);
    pub const SYNC_REQUEST: FrameType = FrameType(0x0004);
    pub const DATA_CHANNEL_HELLO: FrameType = FrameType(0x0005);
    pub const UDP_PACKET: FrameType = FrameType(0x0007);
    pub const APPLY_RESULT: FrameType = FrameType(0x0008);
    pub const AUTH_RESPONSE: FrameType = FrameType(0x000A);
    pub const USER_CONNECT: FrameType = FrameType(0x0010);
    pub const HEALTH_REPORT: FrameType = FrameType(0x0011);
    pub const NAT_HOLE_REPORT: FrameType = FrameType(0x0013);
    // S→C
    pub const AUTH_CHALLENGE: FrameType = FrameType(0x0009);
    pub const HELLO_ACK: FrameType = FrameType(0x0101);
    pub const SERVICE_SYNC: FrameType = FrameType(0x0102);
    pub const SERVICE_UPDATE: FrameType = FrameType(0x0103);
    pub const SERVICE_REMOVE: FrameType = FrameType(0x0104);
    pub const CREATE_DATA_CHANNEL: FrameType = FrameType(0x0105);
    pub const PONG: FrameType = FrameType(0x0106);
    pub const START_FORWARD: FrameType = FrameType(0x0107);
    pub const START_VISITOR_RELAY: FrameType = FrameType(0x0108);
    /// 附加载体报到应答(round-10):仅出现在报到流上,旧客户端不会读到,
    /// 旧服务端不会发送——线格式对既有两个方向零影响。
    pub const CARRIER_ATTACH_ACK: FrameType = FrameType(0x0109);
    /// 证书下发(见 [`crate::certprov`]):认证成功后,服务端用 secret 派生的
    /// 密钥加密自己的 TLS 证书发来,客户端解密后与 TLS 对端证书比对。
    /// 仅在加密协议(tls/wss/quic)上发送。
    pub const SERVER_CERT: FrameType = FrameType(0x010A);
    pub const NAT_HOLE_PREPARE: FrameType = FrameType(0x0110);
    pub const NAT_HOLE_CONNECT: FrameType = FrameType(0x0112);
    pub const ERROR: FrameType = FrameType(0x0199);
}

#[derive(Debug, thiserror::Error)]
pub enum FrameError {
    #[error("bad magic")]
    BadMagic,
    #[error("unsupported frame version {0}")]
    BadVersion(u8),
    #[error("frame length {0} exceeds limit {1}")]
    TooLarge(usize, usize),
    #[error("io error: {0}")]
    Io(#[from] io::Error),
}

/// 编码一帧到缓冲区。
pub fn encode_frame(out: &mut BytesMut, ty: FrameType, payload: &[u8]) {
    out.reserve(HEADER_LEN + payload.len());
    out.put_slice(MAGIC);
    out.put_u8(PROTOCOL_FRAME_VERSION);
    out.put_u16(ty.0);
    out.put_u32(payload.len() as u32);
    out.put_slice(payload);
}

/// 只编码帧头(payload 由调用方自行追加)。
///
/// B1:UDP 上行热路径按 `HEADER + 1 + addr + data` 预留一个缓冲一次成帧,
/// 消除「udp_packet::encode 的中间 Vec + write_frame 的二次拷贝」。
#[inline]
pub fn encode_frame_header(out: &mut BytesMut, ty: FrameType, payload_len: usize) {
    out.reserve(HEADER_LEN + payload_len);
    out.put_slice(MAGIC);
    out.put_u8(PROTOCOL_FRAME_VERSION);
    out.put_u16(ty.0);
    out.put_u32(payload_len as u32);
}

/// 编码一帧并返回(便于直接写出)。
pub fn encode_frame_bytes(ty: FrameType, payload: &[u8]) -> Bytes {
    let mut buf = BytesMut::with_capacity(HEADER_LEN + payload.len());
    encode_frame(&mut buf, ty, payload);
    buf.freeze()
}

/// 尝试从 `buf` 解出一帧。`Ok(None)` 表示数据不足;成功时消费对应字节。
pub fn decode_frame(buf: &mut BytesMut) -> Result<Option<(FrameType, Bytes)>, FrameError> {
    if buf.len() < HEADER_LEN {
        return Ok(None);
    }
    if &buf[0..4] != MAGIC {
        return Err(FrameError::BadMagic);
    }
    if buf[4] != PROTOCOL_FRAME_VERSION {
        return Err(FrameError::BadVersion(buf[4]));
    }
    let ty = FrameType(u16::from_be_bytes([buf[5], buf[6]]));
    let len = u32::from_be_bytes([buf[7], buf[8], buf[9], buf[10]]) as usize;
    if len > MAX_FRAME_LEN {
        return Err(FrameError::TooLarge(len, MAX_FRAME_LEN));
    }
    if buf.len() < HEADER_LEN + len {
        return Ok(None);
    }
    buf.advance(HEADER_LEN);
    let payload = buf.split_to(len).freeze();
    Ok(Some((ty, payload)))
}

/// 异步读取一帧(控制连接/UDP 通道使用)。
pub async fn read_frame<R: AsyncRead + Unpin>(
    r: &mut R,
    buf: &mut BytesMut,
) -> Result<(FrameType, Bytes), FrameError> {
    loop {
        if let Some(frame) = decode_frame(buf)? {
            return Ok(frame);
        }
        if buf.len() > HEADER_LEN + MAX_FRAME_LEN {
            return Err(FrameError::TooLarge(buf.len(), MAX_FRAME_LEN));
        }
        let n = r.read_buf(buf).await?;
        if n == 0 {
            return Err(FrameError::Io(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "connection closed",
            )));
        }
    }
}

/// 异步写入一帧。超限载荷直接报错:对端会以 TooLarge 关闭整条连接,
/// 不如在本端拒绝编码(长度字段截断会产生腐化帧)。
///
/// L-H c(coreProtocol-1,首轮漏收):修前 `encode_frame_bytes` 一次
/// 分配 + 整帧 memcpy(payload 双拷)再 write_all。改为栈上帧头 +
/// `write_vectored` 两段写:支持向量写的底层(裸 TCP/Unix stream)单次
/// syscall;不支持的包装走默认实现(逐段写,首段优先)—— 循环消费
/// 保证语义与 write_all 一致。**不**拆成两次 write_all(裸 TCP 会变
/// 2 syscall,任务书红线)。
pub async fn write_frame<W: AsyncWrite + Unpin>(
    w: &mut W,
    ty: FrameType,
    payload: &[u8],
) -> io::Result<()> {
    use tokio::io::AsyncWriteExt as _;
    if payload.len() > MAX_FRAME_LEN {
        return Err(io::Error::other(format!(
            "frame payload {} exceeds limit {MAX_FRAME_LEN}",
            payload.len()
        )));
    }
    let mut header = [0u8; HEADER_LEN];
    header[..4].copy_from_slice(MAGIC);
    header[4] = PROTOCOL_FRAME_VERSION;
    header[5..7].copy_from_slice(&ty.0.to_be_bytes());
    header[7..11].copy_from_slice(&(payload.len() as u32).to_be_bytes());
    let (mut h_done, mut p_done) = (0usize, 0usize);
    while h_done < HEADER_LEN || p_done < payload.len() {
        let n = w
            .write_vectored(&[
                std::io::IoSlice::new(&header[h_done..]),
                std::io::IoSlice::new(&payload[p_done..]),
            ])
            .await?;
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "failed to write whole frame",
            ));
        }
        let hn = n.min(HEADER_LEN - h_done);
        h_done += hn;
        p_done += n - hn;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let mut buf = BytesMut::new();
        encode_frame(&mut buf, FrameType::PING, b"hello");
        let (ty, payload) = decode_frame(&mut buf).unwrap().unwrap();
        assert_eq!(ty, FrameType::PING);
        assert_eq!(&payload[..], b"hello");
        assert!(buf.is_empty());
    }

    #[test]
    fn partial_then_complete() {
        let mut src = BytesMut::new();
        encode_frame(&mut src, FrameType::SERVICE_SYNC, &[0u8; 3000]);
        let mut buf = BytesMut::new();
        buf.extend_from_slice(&src[..5]);
        assert!(decode_frame(&mut buf).unwrap().is_none());
        buf.extend_from_slice(&src[5..]);
        let (ty, p) = decode_frame(&mut buf).unwrap().unwrap();
        assert_eq!(ty, FrameType::SERVICE_SYNC);
        assert_eq!(p.len(), 3000);
    }

    #[test]
    fn bad_magic_rejected() {
        let mut buf = BytesMut::from(&b"XXXX\x01\x00\x01\x00\x00\x00\x00"[..]);
        assert!(matches!(decode_frame(&mut buf), Err(FrameError::BadMagic)));
    }

    #[test]
    fn oversize_rejected() {
        let mut buf = BytesMut::new();
        encode_frame(&mut buf, FrameType::UDP_PACKET, &[0u8; 16]);
        // 篡改长度字段为超限值
        buf[7] = 0xFF;
        assert!(matches!(
            decode_frame(&mut buf),
            Err(FrameError::TooLarge(_, _))
        ));
    }
}

#[cfg(test)]
mod buffered_residue_tests {
    use super::*;

    /// `read_frame` 必须能消费**已缓冲**的字节,而不是只读新数据。
    ///
    /// 这条锁的是 rh-client 控制面的一处真实 bug:握手阶段用 `buf` 读帧,
    /// 一次 `read_buf` 可能读进多帧(服务端正是背靠背写 HELLO_ACK →
    /// SERVER_CERT → ServiceSync)。切到 `run_loop` 时若新建缓冲而丢弃
    /// 残留,后续帧就永久丢失 —— 客户端从不发 SYNC_REQUEST、服务端也不重发,
    /// 表现为"连上了但所有规则不启动"。局域网(NODELAY 下帧常被合并)最易触发。
    ///
    /// 修法是把这个 `buf` 传进 `run_loop`;本测试证明只要传进来,
    /// `read_frame` 就会先吐缓冲里的帧。
    #[tokio::test]
    async fn read_frame_consumes_preseeded_buffer_before_reading() {
        // 预填:两个完整帧,模拟"握手时一次读进多帧"的残留
        let mut buf = BytesMut::new();
        encode_frame(&mut buf, FrameType::SERVICE_SYNC, b"first");
        encode_frame(&mut buf, FrameType::PING, b"second");

        // 读端给一个立刻 EOF 的空流:只要还要读数据就会返回 UnexpectedEof
        let mut empty: &[u8] = &[];

        let (ty1, p1) = read_frame(&mut empty, &mut buf)
            .await
            .expect("第一帧应从预填缓冲取出,不得去流里读");
        assert_eq!(ty1, FrameType::SERVICE_SYNC);
        assert_eq!(&p1[..], b"first");

        let (ty2, p2) = read_frame(&mut empty, &mut buf)
            .await
            .expect("第二帧同样应从缓冲取出");
        assert_eq!(ty2, FrameType::PING);
        assert_eq!(&p2[..], b"second");

        // 缓冲耗尽后才去读,此时空流报 EOF
        assert!(read_frame(&mut empty, &mut buf).await.is_err());
    }
}
