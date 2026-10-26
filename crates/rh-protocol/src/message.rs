//! RHCP 控制消息定义(JSON payload)。
//!
//! 语义遵循 docs/design.md §3:配置同步(全量/增量/确认/重放)、
//! nonce 挑战认证、服务端指令发起的数据通道、visitor 中继、NAT 打洞协调。

use crate::proxy::ProxySpec;
use crate::visitor::VisitorSpec;
use serde::{Deserialize, Serialize};

/// 配置同步条目:代理或 visitor。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum SyncEntity {
    Proxy(Box<ProxySpec>),
    Visitor(VisitorSpec),
}

/// 数据通道实体定位:代理或 visitor 侧。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "side", rename_all = "lowercase")]
pub enum ChannelEntity {
    /// 代理持有方(服务端→代理客户端的转发通道)。
    Proxy { proxy_id: u64 },
    /// visitor 持有方(访问端客户端→服务端的中继通道)。
    /// `user_conn_id` 由客户端生成,用于配对本地等待中的连接/UDP 源。
    Visitor { visitor_id: u64, user_conn_id: u64 },
}

/// START_FORWARD flags(bit 位)。
pub mod forward_flags {
    /// 数据通道端到端 snappy 压缩。
    pub const COMPRESSED: u8 = 0x01;
    /// tcpmux passthrough:客户端侧自行运行 yamux 接受多路流。
    pub const TCPMUX_PASSTHROUGH: u8 = 0x02;
}

/// PROXY protocol 版本(START_FORWARD 显式携带,客户端不查本地配置)。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum ProxyProto {
    #[default]
    None,
    V1,
    V2,
}

// ============ C → S ============

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Hello {
    pub client_id: String,
    pub version: String,
    #[serde(default)]
    pub hostname: String,
    #[serde(default)]
    pub os: String,
    #[serde(default)]
    pub arch: String,
    /// 支持的代理能力集。
    #[serde(default)]
    pub capabilities: Vec<String>,
    /// 附加载体报到(round-9 多载体池):多载体会话的后续物理连接在首流
    /// HELLO 中携带所属会话 id,服务端查活跃 Session 聚合进池。None/缺省 =
    /// 首条连接(完整挑战应答认证);skip_serializing_if 保证 None 形态的
    /// 线格式与旧版本逐字节一致,现有字段语义不变。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    /// B9:附加载体报到凭据 —— HMAC(secret, "rh3:attach:" + session_id)。
    /// 修前仅凭明文 session_id 聚合,id 一旦泄漏(日志/抓包)即可把第三方
    /// 连接混入会话池。None/缺省 = 首载体(走完整挑战应答),线格式不变。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attach_auth: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AuthResponse {
    pub client_id: String,
    /// HMAC-SHA256(secret, "rh2:" + client_id + ":" + nonce_hex) 的 hex。
    pub auth: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Ping {
    #[serde(default)]
    pub ts_ms: i64,
    /// 通道层探活(UDP 数据连接上使用)。
    #[serde(default)]
    pub probe: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DataChannelHello {
    pub session_id: String,
    /// 回显触发本次拨号的请求号。
    pub request_id: u64,
}

/// visitor 本地新连接(stcp/xtcp TCP);sudp 用 user_conn_id=0 的 UDP 通道。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UserConnect {
    pub visitor_id: u64,
    pub user_conn_id: u64,
    /// 本地用户来源 ip:port(日志/透传)。
    #[serde(default)]
    pub src_addr: String,
    /// udp = 建立常驻 UDP 通道(首包触发)。
    #[serde(default)]
    pub udp: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ApplyResult {
    /// 确认语义:客户端当前已应用到的 revision。
    pub revision: u64,
    pub ok: bool,
    /// ok 路径定点推进的实体 id:仅 proxy 实体的 ServiceUpdate ACK 携带
    /// (服务端据此 O(1) 推进该规则,不再全量扫本客户端全部规则);
    /// ServiceSync/ServiceRemove/visitor 实体的 ACK 为 None → 服务端走
    /// 全量推进。与 ApplyError.service_id 同语义。
    /// 兼容性:字段缺省即 None,旧客户端报文无需迁移;旧服务端 serde
    /// 默认忽略未知字段,双向混布安全。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service_id: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<ApplyError>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ApplyError {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service_id: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entity: Option<String>,
    pub reason: String,
}

/// UDP_PACKET 二进制 payload:`[src_addr_len u8][src_addr ASCII][data...]`。
pub mod udp_packet {
    /// 编码一个 UDP 包帧 payload。
    pub fn encode(src_addr: &str, data: &[u8]) -> Vec<u8> {
        let mut out = Vec::with_capacity(1 + src_addr.len() + data.len());
        out.push(src_addr.len() as u8);
        out.extend_from_slice(src_addr.as_bytes());
        out.extend_from_slice(data);
        out
    }
    /// 解码;地址超长或截断返回 None。
    pub fn decode(payload: &[u8]) -> Option<(&str, &[u8])> {
        if payload.is_empty() {
            return None;
        }
        let alen = payload[0] as usize;
        if payload.len() < 1 + alen {
            return None;
        }
        let addr = std::str::from_utf8(&payload[1..1 + alen]).ok()?;
        Some((addr, &payload[1 + alen..]))
    }

    #[cfg(test)]
    mod tests {
        #[test]
        fn roundtrip() {
            let p = super::encode("1.2.3.4:56", b"hello");
            let (addr, data) = super::decode(&p).unwrap();
            assert_eq!(addr, "1.2.3.4:56");
            assert_eq!(data, b"hello");
            assert!(super::decode(&[5, b'a']).is_none());
        }
    }
}

/// 健康检查状态上报(状态变化时发送)。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HealthReport {
    pub proxy_id: u64,
    pub healthy: bool,
    #[serde(default)]
    pub reason: String,
}

/// NAT 打洞:双端候选上报(C→S)。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NatHoleReport {
    /// 会话内服务端生成的打洞事务 id。
    pub transaction_id: String,
    /// 本端候选(公网映射地址,由 STUN 探测)。
    pub candidates: Vec<String>,
    /// 本端 NAT 行为描述(诊断用)。
    #[serde(default)]
    pub nat_behavior: String,
}

// ============ S → C ============

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AuthChallenge {
    pub nonce: String,
}

/// 证书下发(见 [`crate::certprov`])。认证成功后服务端立即发送。
///
/// 只在**加密协议**(tls/wss/quic)上发送:明文协议链路本身可被窃听,
/// 下发证书不提供任何额外保证,只会制造虚假安全感。
///
/// `cert_der_b64` 是密文(含 GCM tag)的 base64;客户端解密后与 TLS 握手
/// 实际看到的对端叶子证书逐字节比对,不一致必须断开。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ServerCert {
    /// AEAD nonce,base64(12 字节)。
    pub nonce_b64: String,
    /// 密文 + tag,base64。
    pub cert_der_b64: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HelloAck {
    pub ok: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_code: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    pub heartbeat_interval_s: u32,
    #[serde(default)]
    pub server_version: String,
}

/// 附加载体报到应答(round-10):服务端在报到流上回写,客户端据此定容。
/// 仅走 [`FrameType::CARRIER_ATTACH_ACK`](crate::FrameType::CARRIER_ATTACH_ACK);
/// `Hello`/`HelloAck` 字段不变(红线:旧线格式不动)。客户端读应答有 2s
/// 超时,失败只损失该载体,不阻断主会话。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CarrierAttachAck {
    pub ok: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_code: Option<String>,
    /// 服务端允许的最大载体数(含首载体)。
    pub max_carriers: usize,
}

/// 全量配置同步:该客户端全部 enabled 的代理与 visitor,幂等全量替换。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ServiceSync {
    pub revision: u64,
    pub entities: Vec<SyncEntity>,
}

/// 增量 upsert(单实体)。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ServiceUpdate {
    pub revision: u64,
    pub entity: SyncEntity,
}

/// 增量移除(对不存在的实体幂等)。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ServiceRemove {
    pub revision: u64,
    #[serde(rename = "type")]
    pub entity_type: String,
    pub entity_id: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CreateDataChannel {
    pub request_id: u64,
    pub entity: ChannelEntity,
}

/// 指派信号(数据连接最后一帧):目标自包含。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StartForward {
    pub proxy_id: u64,
    #[serde(default)]
    pub user_src: String,
    pub target_host: String,
    pub target_port: u16,
    #[serde(default)]
    pub flags: u8,
    #[serde(default)]
    pub proxy_proto: ProxyProto,
    /// 供路由/健康检查关联的代理名。
    #[serde(default)]
    pub proxy_name: String,
    /// A7:XTCP 直连首帧的一次性凭据(HMAC(secret_key, nonce),见
    /// [`NatHoleConnect::auth`])。中继路径不填;直连路径必须在发送侧写入、
    /// 接收侧恒定时间比对,缺失或不符即断(旧端缺字段 = 拒绝,不静默降级)。
    #[serde(default)]
    pub xtcp_auth: String,
}

/// visitor 侧中继启动(数据连接最后一帧):桥接到本地等待连接。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StartVisitorRelay {
    pub visitor_id: u64,
    pub user_conn_id: u64,
    #[serde(default)]
    pub flags: u8,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Pong {
    #[serde(default)]
    pub ts_ms: i64,
    #[serde(default)]
    pub probe: bool,
}

/// NAT 打洞准备(S→C,双端):开始 STUN 探测并回报候选。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NatHolePrepare {
    pub transaction_id: String,
    /// 打洞超时(秒);超时未回报则服务端判定失败转中继。
    pub timeout_seconds: u64,
    /// A7:本事务一次性随机数。直连首帧必须回填它,第三方无法凭空构造。
    #[serde(default)]
    pub nonce: String,
}

/// NAT 打洞连接指令(S→C,双端):携带对端候选与选定角色。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NatHoleConnect {
    pub transaction_id: String,
    /// visitor 侧本地连接配对号(服务端回填;proxy 侧忽略)。
    #[serde(default)]
    pub user_conn_id: u64,
    /// 对端候选列表。
    pub candidates: Vec<String>,
    /// dial = 发起打洞包并作为 kcp 客户端;listen = 被动响应并作为 kcp 服务端。
    pub role: String,
    /// 打洞尝试时长(秒)。
    pub punch_seconds: u64,
    /// 直接连接失败时回退中继。
    #[serde(default = "default_true_fn")]
    pub fallback_relay: bool,
    /// 中继模式(stcp)标识:xtcp 直连失败后本事务转中继。
    #[serde(default)]
    pub relay_mode: bool,
    /// A7:与 PREPARE 相同的一次性随机数。
    #[serde(default)]
    pub nonce: String,
    /// A7:向 dial 侧下发的凭据 —— HMAC(目标规则 secret_key, nonce)。
    /// dial 侧原样写入直连首帧,listen 侧用本机规则密钥重算并恒定时间比对。
    #[serde(default)]
    pub auth: String,
}

fn default_true_fn() -> bool {
    true
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ErrorMessage {
    pub code: String,
    pub message: String,
}

// ============ 顶层枚举 ============

/// 客户端→服务端消息。
///
/// 注意 `UdpPacket` 不在此列:UDP_PACKET 帧始终使用 `udp_packet` 模块的
/// 二进制格式,不经过 JSON。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub enum ClientMessage {
    Hello(Hello),
    AuthResponse(AuthResponse),
    Ping(Ping),
    SyncRequest {},
    DataChannelHello(DataChannelHello),
    ApplyResult(ApplyResult),
    UserConnect(UserConnect),
    HealthReport(HealthReport),
    NatHoleReport(NatHoleReport),
}

/// 服务端→客户端消息。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub enum ServerMessage {
    AuthChallenge(AuthChallenge),
    HelloAck(HelloAck),
    ServiceSync(ServiceSync),
    ServiceUpdate(Box<ServiceUpdate>),
    ServiceRemove(ServiceRemove),
    CreateDataChannel(CreateDataChannel),
    StartForward(StartForward),
    StartVisitorRelay(StartVisitorRelay),
    Pong(Pong),
    NatHolePrepare(NatHolePrepare),
    NatHoleConnect(NatHoleConnect),
    Error(ErrorMessage),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn server_message_roundtrip() {
        let m = ServerMessage::ServiceSync(ServiceSync {
            revision: 5,
            entities: vec![SyncEntity::Proxy(Box::new(ProxySpec {
                id: 1,
                client_id: String::new(),
                name: "t".into(),
                kind: crate::proxy::ProxyKind::Tcp,
                enabled: true,
                bind_ip: "0.0.0.0".into(),
                bind_port: 5200,
                target_host: "127.0.0.1".into(),
                target_port: 80,
                pool_size: 2,
                transport: Default::default(),
                group: None,
                group_key: String::new(),
                health_check: None,
                plugin: None,
                udp: None,
                http: None,
                https: None,
                stcp: None,
                sudp: None,
                xtcp: None,
                tcpmux: None,
            }))],
        });
        let s = serde_json::to_string(&m).unwrap();
        assert!(s.contains("\"ServiceSync\""));
        let back: ServerMessage = serde_json::from_str(&s).unwrap();
        assert_eq!(m, back);
    }

    #[test]
    fn client_message_error_roundtrip() {
        let m = ServerMessage::Error(ErrorMessage {
            code: "AUTH_FAILED".into(),
            message: "bad secret".into(),
        });
        let s = serde_json::to_string(&m).unwrap();
        let back: ServerMessage = serde_json::from_str(&s).unwrap();
        assert!(matches!(back, ServerMessage::Error(_)));
    }

    #[test]
    fn udp_packet_codec() {
        let p = udp_packet::encode("[::1]:999", &[1, 2, 3]);
        let (a, d) = udp_packet::decode(&p).unwrap();
        assert_eq!(a, "[::1]:999");
        assert_eq!(d, &[1, 2, 3]);
    }

    // T-C6(round-10):报到应答两态序列化。成功态 error_code 必须被省略
    // (旧解析器/日志不受空字段干扰);失败态回传错误码且可原样往返。
    #[test]
    fn carrier_attach_ack_roundtrip() {
        let ok = CarrierAttachAck {
            ok: true,
            error_code: None,
            max_carriers: 8,
        };
        let s = serde_json::to_string(&ok).unwrap();
        assert_eq!(
            s, r#"{"ok":true,"max_carriers":8}"#,
            "成功态省略 error_code"
        );
        assert_eq!(serde_json::from_str::<CarrierAttachAck>(&s).unwrap(), ok);

        let err = CarrierAttachAck {
            ok: false,
            error_code: Some("SESSION_MISMATCH".into()),
            max_carriers: 2,
        };
        let s = serde_json::to_string(&err).unwrap();
        assert!(s.contains("SESSION_MISMATCH"));
        assert_eq!(serde_json::from_str::<CarrierAttachAck>(&s).unwrap(), err);
        // 缺省 error_code(旧/精简报文)也可反序列化
        let parsed: CarrierAttachAck =
            serde_json::from_str(r#"{"ok":true,"max_carriers":4}"#).unwrap();
        assert_eq!(parsed.error_code, None);
    }

    // round-10 红线:新帧类型不得与既有 S→C 段冲突。
    #[test]
    fn carrier_attach_ack_frame_type_is_free() {
        use crate::FrameType;
        for ty in [
            FrameType::HELLO,
            FrameType::PING,
            FrameType::SYNC_REQUEST,
            FrameType::DATA_CHANNEL_HELLO,
            FrameType::UDP_PACKET,
            FrameType::APPLY_RESULT,
            FrameType::AUTH_RESPONSE,
            FrameType::USER_CONNECT,
            FrameType::HEALTH_REPORT,
            FrameType::NAT_HOLE_REPORT,
            FrameType::AUTH_CHALLENGE,
            FrameType::HELLO_ACK,
            FrameType::SERVICE_SYNC,
            FrameType::SERVICE_UPDATE,
            FrameType::SERVICE_REMOVE,
            FrameType::CREATE_DATA_CHANNEL,
            FrameType::PONG,
            FrameType::START_FORWARD,
            FrameType::START_VISITOR_RELAY,
            FrameType::NAT_HOLE_PREPARE,
            FrameType::NAT_HOLE_CONNECT,
            FrameType::ERROR,
        ] {
            assert_ne!(ty, FrameType::CARRIER_ATTACH_ACK);
        }
        assert_eq!(FrameType::CARRIER_ATTACH_ACK, FrameType(0x0109));
    }
}
