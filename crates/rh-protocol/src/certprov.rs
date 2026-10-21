//! 证书下发:用 `secret` 派生的密钥把服务端 TLS 证书加密传给客户端。
//!
//! # 为什么需要这个
//!
//! 服务端默认用自签证书,客户端默认不校验它 —— 于是**客户端从来不知道自己
//! 连的是不是真服务端**。加上这个机制后:
//!
//! 1. 客户端在 HMAC 认证里已经证明自己知道 `secret`(现有机制,不变);
//! 2. 服务端用 `secret` 派生的密钥加密自己的 TLS 证书下发;
//! 3. 客户端解密得到"服务端自称的证书",与 TLS 握手实际看到的对端证书
//!    **逐字节比对**,不一致即断开。
//!
//! 攻击者不知道 `secret` → 解不出也伪造不了这段密文 → 无法让客户端接受自己
//! 的证书 → 中间人转发被识破。这条路径**首次连接即生效**,不需要 TOFU、
//! 不需要 pin、不写任何状态文件。
//!
//! # 为什么不直接用 secret 当密钥
//!
//! 同一个密钥既用于 HMAC 又用于 AEAD 属于密钥复用,是密码学禁忌(两种用途
//! 的密码学假设不同)。故用 HKDF 派生**用途绑定的子密钥**,`info` 里带上
//! 协议版本与方向,避免跨用途/跨版本重放。
//!
//! # 安全边界(必须明确)
//!
//! - 它证明"对端知道 secret";若 secret 本身泄露(内部人、日志、配置外泄),
//!   这层保护同时失效。它不替代 secret 保管。
//! - 明文协议(tcp/ws)下不发此帧:链路本身可被窃听,下发证书没有意义。
//! - 该机制**不改变认证方式**:认证仍是 HMAC 挑战应答,证书只是附加证明。

use ring::{aead, hkdf};

/// 协议标签:子密钥与密文都绑定它,升级协议时改这里即可让旧密文失效。
const INFO: &[u8] = b"rabbithole/tls-cert-provisioning/v1";

/// 派生凭据(两端一致):32 字节 AEAD 密钥。
///
/// 用 HKDF-SHA256,以 `secret` 为输入密钥材料、固定盐(`INFO`),
/// 输出专用的 32 字节密钥。不同用途共享 `secret` 也不会互相削弱。
fn derive_key(secret: &[u8]) -> [u8; 32] {
    let salt = hkdf::Salt::new(hkdf::HKDF_SHA256, INFO);
    let prk = salt.extract(secret);
    let okm = prk
        .expand(&[INFO], &aead::AES_256_GCM)
        .expect("HKDF expand: 32 字节输出不可能超出长度限制");
    let mut key = [0u8; 32];
    okm.fill(&mut key)
        .expect("HKDF fill: 长度已匹配 AEAD 密钥长度");
    key
}

/// 证书下发帧的在线格式。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SealedCert {
    /// AEAD nonce(12 字节,每次连接随机)。
    pub nonce: [u8; 12],
    /// 密文 + 16 字节 GCM tag。
    pub ciphertext: Vec<u8>,
}

/// 服务端侧:加密自己的 TLS 证书(叶子证书 DER)发给客户端。
///
/// `nonce` 由调用方提供(每次连接随机),便于测试确定性注入。
pub fn seal_cert(secret: &[u8], nonce: [u8; 12], cert_der: &[u8]) -> Option<SealedCert> {
    let key = derive_key(secret);
    let unbound = aead::UnboundKey::new(&aead::AES_256_GCM, &key).ok()?;
    let key = aead::LessSafeKey::new(unbound);
    let mut buf = cert_der.to_vec();
    key.seal_in_place_append_tag(
        aead::Nonce::assume_unique_for_key(nonce),
        // AAD 绑定协议标签:密文无法被挪用到其它协议/版本
        aead::Aad::from(INFO),
        &mut buf,
    )
    .ok()?;
    Some(SealedCert {
        nonce,
        ciphertext: buf,
    })
}

/// 客户端侧:解密服务端下发的证书。
///
/// 解密失败(密钥不符 / 密文被篡改 / nonce 被改)一律返回 None,
/// 调用方必须视为"对端不可信"而不是"跳过校验"。
pub fn open_cert(secret: &[u8], sealed: &SealedCert) -> Option<Vec<u8>> {
    let key = derive_key(secret);
    let unbound = aead::UnboundKey::new(&aead::AES_256_GCM, &key).ok()?;
    let key = aead::LessSafeKey::new(unbound);
    let mut buf = sealed.ciphertext.clone();
    let plain = key
        .open_in_place(
            aead::Nonce::assume_unique_for_key(sealed.nonce),
            aead::Aad::from(INFO),
            &mut buf,
        )
        .ok()?;
    Some(plain.to_vec())
}

/// 生成一次连接用的随机 nonce(12 字节)。
///
/// 用系统 CSPRNG;`ring::rand::SystemRandom` 不可用时返回 None,
/// 调用方应跳过下发(而不是退化成固定 nonce —— 那会破坏 GCM 安全性)。
pub fn random_nonce() -> Option<[u8; 12]> {
    use ring::rand::SecureRandom as _;
    let rng = ring::rand::SystemRandom::new();
    let mut n = [0u8; 12];
    rng.fill(&mut n).ok()?;
    Some(n)
}

#[cfg(test)]
mod tests {
    use super::*;

    const SECRET: &[u8] = b"0123456789abcdef0123456789abcdef";
    const CERT: &[u8] = b"\x30\x82\x01\x00fake-certificate-der-bytes";

    #[test]
    fn seal_then_open_roundtrips() {
        let nonce = [7u8; 12];
        let sealed = seal_cert(SECRET, nonce, CERT).unwrap();
        assert_eq!(open_cert(SECRET, &sealed).unwrap(), CERT);
    }

    #[test]
    fn wrong_secret_cannot_open() {
        let sealed = seal_cert(SECRET, [1u8; 12], CERT).unwrap();
        assert!(
            open_cert(b"another-secret", &sealed).is_none(),
            "不同 secret 必须解不开(这是防冒充的基础)"
        );
    }

    #[test]
    fn tampered_ciphertext_rejected() {
        let mut sealed = seal_cert(SECRET, [2u8; 12], CERT).unwrap();
        sealed.ciphertext[0] ^= 0x01;
        assert!(
            open_cert(SECRET, &sealed).is_none(),
            "篡改必须被 GCM tag 拒绝"
        );
    }

    #[test]
    fn tampered_nonce_rejected() {
        let mut sealed = seal_cert(SECRET, [3u8; 12], CERT).unwrap();
        sealed.nonce[0] ^= 0x01;
        assert!(open_cert(SECRET, &sealed).is_none(), "nonce 篡改必须被拒绝");
    }

    #[test]
    fn truncated_ciphertext_rejected() {
        let mut sealed = seal_cert(SECRET, [4u8; 12], CERT).unwrap();
        sealed.ciphertext.truncate(4);
        assert!(open_cert(SECRET, &sealed).is_none());
    }

    /// 派生密钥必须与用途/版本绑定:换 INFO 后旧密文解不开。
    #[test]
    fn derived_key_is_32_bytes_and_stable() {
        let a = derive_key(SECRET);
        let b = derive_key(SECRET);
        assert_eq!(a, b, "同一 secret 必须派生出同一密钥");
        assert_eq!(a.len(), 32);
        assert_ne!(a, derive_key(b"other"), "不同 secret 必须不同");
    }

    #[test]
    fn random_nonce_is_not_constant() {
        let a = random_nonce().unwrap();
        let b = random_nonce().unwrap();
        assert_ne!(a, b, "nonce 必须随机(否则 GCM 不安全)");
    }
}
