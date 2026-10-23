//! rh-protocol: RHTP 帧编解码、RHCP 控制消息与共享配置类型。
//!
//! 本 crate 零 IO、零业务状态,可被服务端、客户端与测试共用。
//! 帧格式见 `frame` 模块文档;消息定义见 `message` 模块。

pub mod certprov;
pub mod frame;
pub mod message;
pub mod proxy;
pub mod visitor;

pub use frame::{
    decode_frame, encode_frame, encode_frame_bytes, encode_frame_header, read_frame, write_frame,
    FrameError, FrameType, MAGIC, MAX_FRAME_LEN,
};
pub use message::{forward_flags, ClientMessage, ServerMessage};
pub use proxy::{
    BandwidthMode, CacheConfig, HeaderOp, HealthCheckKind, HealthCheckSpec, HttpProxyConfig,
    HttpsProxyConfig, PluginKind, PluginSpec, ProxyKind, ProxyProtoVersion, ProxySpec,
    ProxyTransport, StcpProxyConfig, SudpProxyConfig, TcpMuxProxyConfig, UdpProxyConfig,
};
pub use visitor::{VisitorKind, VisitorSpec};

/// 协议版本号(语义化,主版本不一致时服务端拒绝)。
pub const PROTOCOL_VERSION: &str = "0.1.0";
