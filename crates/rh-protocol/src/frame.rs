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
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

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
pub async fn write_frame<W: AsyncWrite + Unpin>(
    w: &mut W,
    ty: FrameType,
    payload: &[u8],
) -> io::Result<()> {
    if payload.len() > MAX_FRAME_LEN {
        return Err(io::Error::other(format!(
            "frame payload {} exceeds limit {MAX_FRAME_LEN}",
            payload.len()
        )));
    }
    let frame = encode_frame_bytes(ty, payload);
    w.write_all(&frame).await
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
