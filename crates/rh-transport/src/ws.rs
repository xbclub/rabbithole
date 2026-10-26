//! WebSocket 载体:在任意字节流上做 HTTP Upgrade 握手,再把 WS 二进制消息
//! 适配为 AsyncRead/AsyncWrite 字节流(消息边界视作连续字节)。

use crate::{base64_encode, BoxedIo};
use futures::{Sink, Stream};
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio_tungstenite::tungstenite::protocol::{Message, Role};

/// B16:WebSocket 帧/消息上限。tungstenite 默认 64MiB —— 一条巨帧就能让
/// 单个隧道连接吃掉数倍于 up_chunk_cap 的内存。RHTP 自身的帧体上限是
/// 1MiB,这里给 2MiB 留出控制帧与压缩余量。
const WS_MAX_MESSAGE: usize = 2 * 1024 * 1024;
const WS_MAX_FRAME: usize = 2 * 1024 * 1024;

fn ws_config() -> tokio_tungstenite::tungstenite::protocol::WebSocketConfig {
    let mut c = tokio_tungstenite::tungstenite::protocol::WebSocketConfig::default();
    c.max_message_size = Some(WS_MAX_MESSAGE);
    c.max_frame_size = Some(WS_MAX_FRAME);
    c
}

/// WebSocket 升级路径。
///
/// **为什么不是 `/rhtp`**:该路径以明文出现在 `GET <path> HTTP/1.1` 请求行,
/// 是比 ALPN 更显眼的指纹——任何反代、WAF、DPI 看一眼请求行就知道是这个
/// 项目。改用常见的路径形态,混在同机其它 WS 服务里不显眼。
///
/// 与 ALPN 同理,改动需要服务端与客户端一致(两端同版本即一致)。
pub const WS_PATH: &str = "/ws";

/// 服务端:完成 WS 升级握手,返回字节流。
pub async fn upgrade_server(mut io: BoxedIo, expect_path: &str) -> io::Result<BoxedIo> {
    // 读到请求头结尾
    let mut buf = Vec::with_capacity(1024);
    let mut chunk = [0u8; 1024];
    loop {
        let n = tokio::time::timeout(std::time::Duration::from_secs(5), io.read(&mut chunk))
            .await
            .map_err(|_| io::Error::other("ws handshake timeout"))??;
        if n == 0 {
            return Err(io::Error::other("ws handshake: client closed"));
        }
        buf.extend_from_slice(&chunk[..n]);
        if buf.windows(4).any(|w| w == b"\r\n\r\n") {
            break;
        }
        if buf.len() > 16 * 1024 {
            return Err(io::Error::other("ws handshake request too large"));
        }
    }
    let req = String::from_utf8_lossy(&buf).into_owned();
    let first_line = req.lines().next().unwrap_or_default().to_string();
    let mut path_ok = false;
    if let Some(path) = first_line.split_whitespace().nth(1) {
        let p = path.split('?').next().unwrap_or("");
        path_ok = p == expect_path || (expect_path == "/" && p == "/");
    }
    if !path_ok {
        let _ = io
            .write_all(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n")
            .await;
        return Err(io::Error::other("ws: unexpected path"));
    }
    // 校验升级头部(RFC 6455 §4.2.1)
    let header = |name: &str| {
        req.lines().find_map(|l| {
            let (k, v) = l.split_once(':')?;
            k.trim()
                .eq_ignore_ascii_case(name)
                .then(|| v.trim().to_ascii_lowercase())
        })
    };
    let upgrade_ws =
        header("upgrade").is_some_and(|v| v.split(',').any(|t| t.trim() == "websocket"));
    let connection_upgrade =
        header("connection").is_some_and(|v| v.split(',').any(|t| t.trim() == "upgrade"));
    let version_ok = header("sec-websocket-version").as_deref() == Some("13");
    if !(upgrade_ws && connection_upgrade && version_ok) {
        let _ = io
            .write_all(b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\n\r\n")
            .await;
        return Err(io::Error::other("ws: not a websocket upgrade request"));
    }
    // 生成 Sec-WebSocket-Accept
    let key = req
        .lines()
        .find_map(|l| {
            let (k, v) = l.split_once(':')?;
            if k.trim().eq_ignore_ascii_case("sec-websocket-key") {
                Some(v.trim().to_string())
            } else {
                None
            }
        })
        .ok_or_else(|| io::Error::other("ws: missing Sec-WebSocket-Key"))?;
    let accept = ws_accept(&key);
    let resp = format!(
        "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: {accept}\r\n\r\n"
    );
    io.write_all(resp.as_bytes()).await?;
    // 同一 TCP 读可能带出头部之后的早期 WS 帧,必须保留给帧解析器,
    // 否则丢字节 → 帧界错位断连
    let term = buf
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .unwrap_or(buf.len());
    let part = buf[term + 4..].to_vec();
    let ws = tokio_tungstenite::WebSocketStream::from_partially_read(
        io,
        part,
        Role::Server,
        Some(ws_config()),
    )
    .await;
    Ok(Box::new(WsIo::new(ws)))
}

/// 客户端:发起 WS 升级握手,返回字节流。
pub async fn client_handshake(mut io: BoxedIo, host: &str, path: &str) -> io::Result<BoxedIo> {
    use rand::RngCore;
    let mut key_bytes = [0u8; 16];
    rand::rng().fill_bytes(&mut key_bytes);
    let key = base64_encode(&key_bytes);
    let req = format!(
        "GET {path} HTTP/1.1\r\nHost: {host}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n\r\n"
    );
    io.write_all(req.as_bytes()).await?;
    let mut buf = Vec::with_capacity(1024);
    let mut chunk = [0u8; 1024];
    loop {
        let n = tokio::time::timeout(std::time::Duration::from_secs(5), io.read(&mut chunk))
            .await
            .map_err(|_| io::Error::other("ws handshake timeout"))??;
        if n == 0 {
            return Err(io::Error::other("ws handshake: server closed"));
        }
        buf.extend_from_slice(&chunk[..n]);
        if buf.windows(4).any(|w| w == b"\r\n\r\n") {
            break;
        }
        if buf.len() > 16 * 1024 {
            return Err(io::Error::other("ws handshake response too large"));
        }
    }
    let resp = String::from_utf8_lossy(&buf).into_owned();
    let status_ok = resp
        .lines()
        .next()
        .map(|l| l.contains("101"))
        .unwrap_or(false);
    if !status_ok {
        return Err(io::Error::other(format!(
            "ws upgrade failed: {}",
            resp.lines().next().unwrap_or("")
        )));
    }
    // 校验 accept
    let want = ws_accept(&key);
    let got = resp.lines().find_map(|l| {
        let (k, v) = l.split_once(':')?;
        if k.trim().eq_ignore_ascii_case("sec-websocket-accept") {
            Some(v.trim().to_string())
        } else {
            None
        }
    });
    if got.as_deref() != Some(want.as_str()) {
        return Err(io::Error::other("ws: Sec-WebSocket-Accept mismatch"));
    }
    // 同 upgrade_server:保留头部终止符之后已读到的字节
    let term = buf
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .unwrap_or(buf.len());
    let part = buf[term + 4..].to_vec();
    let ws = tokio_tungstenite::WebSocketStream::from_partially_read(
        io,
        part,
        Role::Client,
        Some(ws_config()),
    )
    .await;
    Ok(Box::new(WsIo::new(ws)))
}

// RFC 6455:Sec-WebSocket-Accept = base64(SHA-1(key + GUID))
fn ws_accept(key: &str) -> String {
    use sha1::Digest;
    const GUID: &str = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
    let mut h = sha1::Sha1::new();
    h.update(key.as_bytes());
    h.update(GUID.as_bytes());
    base64_encode(&h.finalize())
}

// WS 消息流 → 字节流适配。
pin_project_lite::pin_project! {
struct WsIo {
    #[pin]
    ws: tokio_tungstenite::WebSocketStream<BoxedIo>,
    // B2:tungstenite 0.30 的 Message::Binary 载荷就是 `Bytes`,
    // 直接持有共享引用(修前 `data.to_vec()` 多拷一次),消费完置空
    // 即释放;不再需要"容量 > 64KiB 丢弃缓冲"特判。
    rd_buf: bytes::Bytes,
    rd_pos: usize,
    eof: bool,
    // start_send 已接受一条消息、尚未 flush 完毕。
    // Sink 契约:start_send 成功即"已入队",即使后续 flush 返回 Pending;
    // 若此时向上层报 Pending,上层重试同一缓冲时不得再次 start_send,
    // 否则消息重复入队 → 对端字节流错位 → 协议错误断连。
    send_queued: bool,
    // B-6:写路径复用 scratch。消息载荷经 `split().freeze()` 以共享视图
    // 移交 tungstenite(send_queued 保证至多一条在途),scratch 前移追加;
    // 余量不足时 reserve 触发 bytes 1.12 的前缀回收(在途消息已 flush 即
    // 唯一持有),稳态不随写次数线性分配(修前每 ≤64KiB 块/每批一次
    // with_capacity + 整载荷拷贝)。
    scratch: bytes::BytesMut,
}
}

impl WsIo {
    fn new(ws: tokio_tungstenite::WebSocketStream<BoxedIo>) -> Self {
        Self {
            ws,
            rd_buf: bytes::Bytes::new(),
            rd_pos: 0,
            eof: false,
            send_queued: false,
            scratch: bytes::BytesMut::new(),
        }
    }
}

impl AsyncRead for WsIo {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let mut this = self.project();
        loop {
            if *this.rd_pos < this.rd_buf.len() {
                let avail = &this.rd_buf[*this.rd_pos..];
                let n = avail.len().min(buf.remaining());
                buf.put_slice(&avail[..n]);
                *this.rd_pos += n;
                if *this.rd_pos == this.rd_buf.len() {
                    // B2:Bytes 引用计数,置空即释放(大帧不再长期占内存,
                    // 也不需要 Vec 时代的容量特判)。
                    *this.rd_buf = bytes::Bytes::new();
                    *this.rd_pos = 0;
                }
                return Poll::Ready(Ok(()));
            }
            if *this.eof {
                return Poll::Ready(Ok(()));
            }
            match futures::ready!(this.ws.as_mut().poll_next(cx)) {
                None => {
                    *this.eof = true;
                    return Poll::Ready(Ok(()));
                }
                Some(Ok(Message::Binary(data))) => {
                    // B2:零拷贝交付(载荷本身是 Bytes,修前 to_vec 整帧再拷)
                    *this.rd_buf = data;
                    *this.rd_pos = 0;
                }
                Some(Ok(Message::Close(_))) => {
                    *this.eof = true;
                    return Poll::Ready(Ok(()));
                }
                Some(Ok(_)) => continue, // 文本/ping/pong 等忽略
                Some(Err(e)) => return Poll::Ready(Err(io::Error::other(format!("ws: {e}")))),
            }
        }
    }
}

impl AsyncWrite for WsIo {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let mut this = self.project();
        if !*this.send_queued {
            this.scratch.clear();
            this.scratch.extend_from_slice(buf);
            let msg = Message::Binary(this.scratch.split().freeze());
            match futures::ready!(this.ws.as_mut().poll_ready(cx)) {
                Ok(()) => match this.ws.as_mut().start_send(msg) {
                    // start_send 返回 Ok 即消息已被 tungstenite 接受并入队
                    //(WouldBlock 内部已被映射为 Ok),绝不重发
                    Ok(()) => *this.send_queued = true,
                    Err(e) => return Poll::Ready(Err(io::Error::other(format!("ws: {e}")))),
                },
                Err(e) => return Poll::Ready(Err(io::Error::other(format!("ws: {e}")))),
            }
        }
        // 冲刷直到完成;Pending 时上层重试会跳过 start_send 直接续冲
        match futures::ready!(this.ws.as_mut().poll_flush(cx)) {
            Ok(()) => {
                *this.send_queued = false;
                Poll::Ready(Ok(buf.len()))
            }
            Err(e) => Poll::Ready(Err(io::Error::other(format!("ws flush: {e}")))),
        }
    }
    /// B2:向量写透传。修前未覆盖,落到 tokio 默认实现——只取第一个
    /// 非空 slice;YamuxCarrier 的批量写(poll_writev,≤33 段)在
    /// ws/wss 载体上整批失效,每个 yamux 帧退化成 ≥2 条 WS 消息
    /// (12B 头一条、帧体一条,各带 WS 帧头与一次 flush)。与
    /// PrependedIo 的同型缺陷修复(lib.rs 回归测试)对齐。
    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[std::io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let mut this = self.project();
        let total: usize = bufs.iter().map(|b| b.len()).sum();
        if !*this.send_queued {
            this.scratch.clear();
            for b in bufs {
                this.scratch.extend_from_slice(b);
            }
            let msg = Message::Binary(this.scratch.split().freeze());
            match futures::ready!(this.ws.as_mut().poll_ready(cx)) {
                Ok(()) => match this.ws.as_mut().start_send(msg) {
                    // start_send 返回 Ok 即消息已被 tungstenite 接受并入队,
                    // 绝不重发(与标量写同契约)
                    Ok(()) => *this.send_queued = true,
                    Err(e) => return Poll::Ready(Err(io::Error::other(format!("ws: {e}")))),
                },
                Err(e) => return Poll::Ready(Err(io::Error::other(format!("ws: {e}")))),
            }
        }
        match futures::ready!(this.ws.as_mut().poll_flush(cx)) {
            Ok(()) => {
                *this.send_queued = false;
                Poll::Ready(Ok(total))
            }
            Err(e) => Poll::Ready(Err(io::Error::other(format!("ws flush: {e}")))),
        }
    }

    fn is_write_vectored(&self) -> bool {
        // 多段在 poll_write_vectored 里拼成一条消息,能力位如实上报
        true
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let mut this = self.project();
        Poll::Ready(
            futures::ready!(this.ws.as_mut().poll_flush(cx))
                .map_err(|e| io::Error::other(format!("ws flush: {e}"))),
        )
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let mut this = self.project();
        Poll::Ready(
            futures::ready!(this.ws.as_mut().poll_close(cx))
                .map_err(|e| io::Error::other(format!("ws close: {e}"))),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 解析客户端→方向的 WS 帧(tungstenite Role::Client 写出的 masked 帧;
    /// 可能分片),返回按序拼接的各 binary 帧载荷。R5 不变量锁的判读器。
    fn parse_client_frames(mut wire: &[u8]) -> Vec<Vec<u8>> {
        let mut out = Vec::new();
        while !wire.is_empty() {
            let b0 = wire[0];
            let b1 = wire[1];
            let opcode = b0 & 0x0F;
            let masked = b1 & 0x80 != 0;
            let len7 = (b1 & 0x7F) as usize;
            let mut off = 2;
            let len = match len7 {
                126 => {
                    let l = u16::from_be_bytes([wire[off], wire[off + 1]]) as usize;
                    off += 2;
                    l
                }
                127 => {
                    let l = u64::from_be_bytes(wire[off..off + 8].try_into().unwrap()) as usize;
                    off += 8;
                    l
                }
                n => n,
            };
            let mask = if masked {
                let m: [u8; 4] = wire[off..off + 4].try_into().unwrap();
                off += 4;
                m
            } else {
                [0u8; 4]
            };
            let payload: Vec<u8> = wire[off..off + len]
                .iter()
                .enumerate()
                .map(|(i, b)| b ^ mask[i % 4])
                .collect();
            assert_eq!(opcode, 2, "只有 binary 帧应出现在测试线路,opcode={opcode}");
            wire = &wire[off + len..];
            out.push(payload);
        }
        out
    }

    /// R5 不变量锁(修前即绿,禁止生产代码回退):小 duplex(64B)强制
    /// 背压——8KiB 载荷首帧 start_send 成功(send_queued=true)后 flush
    /// 反复 Pending;上层按 AsyncWrite 契约以**同一缓冲**重试,重试必须
    /// 跳过 start_send 只续冲,线上恰好一份载荷(不重);随后第二个载荷
    /// 独立成帧(不丢、不并帧错位)。
    #[tokio::test]
    async fn ws_send_queued_backpressure_no_dup_no_loss() {
        use std::time::Duration;
        use tokio::io::AsyncWriteExt;
        let (a, b) = tokio::io::duplex(64);
        let ws = tokio_tungstenite::WebSocketStream::from_partially_read(
            Box::new(a) as BoxedIo,
            vec![],
            Role::Client,
            None,
        )
        .await;
        let mut c = WsIo::new(ws);
        let reader = tokio::spawn(async move {
            let mut b = b;
            let mut out = Vec::new();
            let _ = tokio::time::timeout(Duration::from_secs(5), b.read_to_end(&mut out)).await;
            out
        });

        let p1 = vec![0xA5u8; 8 * 1024];
        let p2 = vec![0x3Cu8; 1024];
        let n1 = tokio::time::timeout(Duration::from_secs(5), c.write(&p1))
            .await
            .expect("deadline: write p1")
            .expect("write p1");
        assert_eq!(n1, p1.len());
        let n2 = tokio::time::timeout(Duration::from_secs(5), c.write(&p2))
            .await
            .expect("deadline: write p2")
            .expect("write p2");
        assert_eq!(n2, p2.len());
        drop(c);
        let wire = tokio::time::timeout(Duration::from_secs(5), reader)
            .await
            .expect("deadline: reader join")
            .unwrap();
        let frames = parse_client_frames(&wire);
        let joined: Vec<u8> = frames.concat();
        let mut want = p1.clone();
        want.extend_from_slice(&p2);
        assert_eq!(
            joined,
            want,
            "背压 Pending 重试不得重复入队、不得丢载荷;帧数={}",
            frames.len()
        );
        // 8KiB 恰好一份(重复入队会使字节数翻倍)
        assert_eq!(joined.len(), 9 * 1024);
    }

    /// B2(红→绿):向量写透传。修前未实现 poll_write_vectored,落到
    /// tokio 默认实现只取第一个非空 slice —— YamuxCarrier 的批量写在
    /// ws/wss 载体上整批失效。断言:能力位为 true;一次双段向量写
    /// 在线上恰好一条 binary 帧,载荷为两段拼接(与 PrependedIo 的
    /// 同型回归测试对齐)。
    #[tokio::test]
    async fn ws_vectored_write_single_frame() {
        use std::time::Duration;
        let (a, b) = tokio::io::duplex(64);
        let ws = tokio_tungstenite::WebSocketStream::from_partially_read(
            Box::new(a) as BoxedIo,
            vec![],
            Role::Client,
            None,
        )
        .await;
        let mut c = WsIo::new(ws);
        assert!(c.is_write_vectored(), "能力位必须如实上报(B2)");

        let reader = tokio::spawn(async move {
            let mut b = b;
            let mut out = Vec::new();
            let _ = tokio::time::timeout(Duration::from_secs(5), b.read_to_end(&mut out)).await;
            out
        });

        let header = [0u8; 12];
        let body = vec![7u8; 4096];
        // poll_fn 提供真实任务 waker:首次 poll_ready 可能 Pending
        //(sink 需先注册对 duplex 的可写兴趣),就绪后自动重试。
        let n = tokio::time::timeout(
            Duration::from_secs(5),
            std::future::poll_fn(|cx| {
                std::pin::Pin::new(&mut c).poll_write_vectored(
                    cx,
                    &[std::io::IoSlice::new(&header), std::io::IoSlice::new(&body)],
                )
            }),
        )
        .await
        .expect("deadline: vectored write")
        .expect("vectored write");

        drop(c);
        let wire = tokio::time::timeout(Duration::from_secs(5), reader)
            .await
            .expect("deadline: reader join")
            .unwrap();
        let frames = parse_client_frames(&wire);
        assert_eq!(n, header.len() + body.len(), "全部段字节应一次带走");
        assert_eq!(
            frames.len(),
            1,
            "双段向量写必须合并为一条 WS 消息(修前只写首段,B2)"
        );
        let mut want = header.to_vec();
        want.extend_from_slice(&body);
        assert_eq!(frames.concat(), want);
    }

    /// R5 不变量锁(修前即绿):手动 poll 级观察——poll_write 在 flush
    /// 未完时返回 Pending 且 start_send 已消费;同缓冲重试不得再次
    /// start_send(以"恰好一帧"判读),完成后 send_queued 复位,
    /// 下一个载荷开始新帧。
    #[tokio::test]
    async fn ws_send_queued_manual_poll_single_frame() {
        use std::time::Duration;
        let (a, b) = tokio::io::duplex(8);
        let ws = tokio_tungstenite::WebSocketStream::from_partially_read(
            Box::new(a) as BoxedIo,
            vec![],
            Role::Client,
            None,
        )
        .await;
        let reader = tokio::spawn(async move {
            let mut b = b;
            let mut out = Vec::new();
            let _ = tokio::time::timeout(Duration::from_secs(5), b.read_to_end(&mut out)).await;
            out
        });

        let payload = b"manual-poll-payload".to_vec();
        // 手动驱动:同缓冲重试,直到 Ready(模拟 write_all 的上层契约)。
        // 整体放独立块:块结束 drop WsIo(含 duplex 半)→ reader 侧 EOF。
        let n = {
            let c = WsIo::new(ws);
            let mut pinned = std::pin::pin!(c);
            tokio::time::timeout(
                Duration::from_secs(5),
                std::future::poll_fn(|cx| {
                    tokio::io::AsyncWrite::poll_write(pinned.as_mut(), cx, &payload)
                }),
            )
            .await
            .expect("deadline: manual write")
            .expect("manual write")
        };
        assert_eq!(n, payload.len());
        let wire = tokio::time::timeout(Duration::from_secs(5), reader)
            .await
            .expect("deadline: reader join")
            .unwrap();
        let frames = parse_client_frames(&wire);
        assert_eq!(frames.len(), 1, "同缓冲重试期间恰好一帧: {frames:?}");
        assert_eq!(frames[0], payload);
    }

    /// R5 不变量锁(修前即绿):服务端升级——HTTP 头与其后**同一批读到**的
    /// 早期 WS 帧必须整批保留给帧解析器(from_partially_read 前缀),
    /// 升级完成后按序先交付首帧;跨批的拆分帧尾也能续上。
    #[tokio::test]
    async fn ws_upgrade_server_preserves_same_batch_first_frames() {
        use std::time::Duration;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let (mut a, b) = tokio::io::duplex(8192);

        // masked(零掩码)客户端 binary 帧
        let cli_frame = |p: &[u8]| -> Vec<u8> {
            let mut f = vec![0x82u8];
            if p.len() < 126 {
                f.push(0x80 | p.len() as u8);
            } else {
                f.push(0x80 | 126);
                f.extend_from_slice(&(p.len() as u16).to_be_bytes());
            }
            f.extend_from_slice(&[0u8; 4]); // 掩码全零:异或后载荷不变
            f.extend_from_slice(p);
            f
        };
        let mut early = cli_frame(b"early-1");
        early.extend_from_slice(&cli_frame(b"early-2"));
        // 第三个帧拆两半:前半在同批前缀,后半稍后送达
        let f3 = cli_frame(b"tail-frame-3");
        let (f3_head, f3_tail) = f3.split_at(6);
        early.extend_from_slice(f3_head);

        let req = format!(
            "GET {WS_PATH} HTTP/1.1\r\nHost: h\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n"
        );
        let mut batch = req.into_bytes();
        batch.extend_from_slice(&early);
        a.write_all(&batch).await.unwrap(); // 单次写:严格同批

        let mut s = tokio::time::timeout(
            Duration::from_secs(5),
            upgrade_server(Box::new(b) as BoxedIo, WS_PATH),
        )
        .await
        .expect("deadline: upgrade")
        .expect("upgrade ok");

        let mut got = [0u8; 7];
        tokio::time::timeout(Duration::from_secs(5), s.read_exact(&mut got))
            .await
            .expect("deadline: read early-1")
            .expect("read early-1");
        assert_eq!(&got, b"early-1", "同批首帧必须保留并最先交付");
        tokio::time::timeout(Duration::from_secs(5), s.read_exact(&mut got))
            .await
            .expect("deadline: read early-2")
            .expect("read early-2");
        assert_eq!(&got, b"early-2");

        // 补上拆分帧尾:跨批字节与前缀拼接成完整帧
        let mut tail = f3_tail.to_vec();
        tail.extend_from_slice(&cli_frame(b"after"));
        a.write_all(&tail).await.unwrap();
        let mut got3 = [0u8; 12];
        tokio::time::timeout(Duration::from_secs(5), s.read_exact(&mut got3))
            .await
            .expect("deadline: read f3")
            .expect("read f3");
        assert_eq!(&got3, b"tail-frame-3", "拆分帧跨批拼接不得错位");
        let mut got4 = [0u8; 5];
        tokio::time::timeout(Duration::from_secs(5), s.read_exact(&mut got4))
            .await
            .expect("deadline: read after")
            .expect("read after");
        assert_eq!(&got4, b"after");
    }

    /// R5 不变量锁(修前即绿):客户端握手——101 响应与其后同批的早期
    /// 服务端帧(未掩码)必须保留,握手完成后先交付。
    #[tokio::test]
    async fn ws_client_handshake_preserves_same_batch_first_frames() {
        use std::time::Duration;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let (a, mut b) = tokio::io::duplex(8192);

        let server = tokio::spawn(async move {
            // 读请求头,回带正确 Accept 的 101 + 同批早期帧(服务端方向不掩码)
            let mut buf = Vec::new();
            let mut ch = [0u8; 512];
            loop {
                let n = tokio::time::timeout(Duration::from_secs(5), b.read(&mut ch))
                    .await
                    .expect("deadline: server read req")
                    .expect("server read req");
                assert!(n > 0, "client closed before request");
                buf.extend_from_slice(&ch[..n]);
                if buf.windows(4).any(|w| w == b"\r\n\r\n") {
                    break;
                }
            }
            let req = String::from_utf8_lossy(&buf).into_owned();
            let key = req
                .lines()
                .find_map(|l| {
                    let (k, v) = l.split_once(':')?;
                    k.trim()
                        .eq_ignore_ascii_case("sec-websocket-key")
                        .then(|| v.trim().to_string())
                })
                .expect("request carries key");
            let srv_frame = |p: &[u8]| -> Vec<u8> {
                let mut f = vec![0x82u8];
                f.push(p.len() as u8);
                f.extend_from_slice(p);
                f
            };
            let mut batch = format!(
                "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: {}\r\n\r\n",
                ws_accept(&key)
            )
            .into_bytes();
            batch.extend_from_slice(&srv_frame(b"down-1"));
            batch.extend_from_slice(&srv_frame(b"down-2"));
            b.write_all(&batch).await.unwrap();
        });

        let mut c = tokio::time::timeout(
            Duration::from_secs(5),
            client_handshake(Box::new(a) as BoxedIo, "127.0.0.1", WS_PATH),
        )
        .await
        .expect("deadline: handshake")
        .expect("handshake ok");

        let mut got = [0u8; 6];
        tokio::time::timeout(Duration::from_secs(5), c.read_exact(&mut got))
            .await
            .expect("deadline: read down-1")
            .expect("read down-1");
        assert_eq!(&got, b"down-1", "同批首帧必须保留并最先交付");
        tokio::time::timeout(Duration::from_secs(5), c.read_exact(&mut got))
            .await
            .expect("deadline: read down-2")
            .expect("read down-2");
        assert_eq!(&got, b"down-2");
        tokio::time::timeout(Duration::from_secs(5), server)
            .await
            .expect("deadline: server join")
            .unwrap();
    }

    #[tokio::test]
    async fn ws_handshake_and_stream() {
        let (a, b) = tokio::io::duplex(8192);
        let server = tokio::spawn(async move {
            let io: BoxedIo = Box::new(b);
            let s = upgrade_server(io, WS_PATH).await.unwrap();
            s
        });
        let client_io: BoxedIo = Box::new(a);
        let mut c = client_handshake(client_io, "127.0.0.1", WS_PATH)
            .await
            .unwrap();
        let mut s = server.await.unwrap();
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        c.write_all(b"hello-ws").await.unwrap();
        let mut buf = [0u8; 8];
        s.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"hello-ws");
        s.write_all(b"ok").await.unwrap();
        let mut back = [0u8; 2];
        c.read_exact(&mut back).await.unwrap();
        assert_eq!(&back, b"ok");
    }

    #[tokio::test]
    async fn ws_wrong_path_rejected() {
        let (a, b) = tokio::io::duplex(1024);
        tokio::spawn(async move {
            let io: BoxedIo = Box::new(b);
            let _ = upgrade_server(io, WS_PATH).await;
        });
        let mut raw = a;
        raw.write_all(b"GET /other HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: abc\r\nSec-WebSocket-Version: 13\r\n\r\n")
            .await
            .unwrap();
        let mut resp = Vec::new();
        let mut ch = [0u8; 128];
        let n = raw.read(&mut ch).await.unwrap();
        resp.extend_from_slice(&ch[..n]);
        let text = String::from_utf8_lossy(&resp);
        assert!(text.contains("404"));
    }
}
