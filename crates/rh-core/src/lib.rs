//! rh-core:跨端共享的实用组件 —— PROXY protocol 编解码、令牌桶限速、
//! 带宽数量解析、STUN 客户端(RFC 5780 binding)、地址工具。

pub mod bandwidth;
pub mod net;
pub mod proxyproto;
pub mod ratelimit;
pub mod stun;

pub use bandwidth::BandwidthQuantity;
pub use proxyproto::{encode_v1, encode_v2, ProxyProtoInfo};
pub use ratelimit::TokenBucket;
pub use stun::stun_binding;
