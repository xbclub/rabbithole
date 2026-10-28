//! API Key 凭据原语:纯函数,不碰 IO,便于单测。
//!
//! 凭据形态:`rhk_<8 hex prefix>_<64 hex secret>`,总长 77。
//! ★ 落库的 key_hash = sha256(**完整 bearer 串**),不是只有 secret 部分 ——
//!   generate 与 parse/verify 两条链路口径必须一致,否则所有 key 永远 401。

// ★ `use rand::RngCore;` 只在文件顶部出现一次。
//   generate() 内部不要再写一遍 —— 重复 use 触发 unused import,
//   在 clippy -D warnings 下直接编译失败(任务书 §0 第 5 条,已实测)。
use rand::RngCore;
use sha2::{Digest, Sha256};

pub const TAG: &str = "rhk_";

#[derive(Clone)]
pub struct Generated {
    pub plaintext: String,
    pub prefix: String,
    pub hash: String,
}

/// 生成一枚 key:8 hex 前缀 + 64 hex secret,共用一份随机源。
pub fn generate() -> Generated {
    // 4 字节前缀 + 32 字节 secret
    let mut buf = [0u8; 36];
    rand::rng().fill_bytes(&mut buf);
    let prefix = hex::encode(&buf[..4]); // 8 hex
    let secret = hex::encode(&buf[4..]); // 64 hex
    let plaintext = format!("rhk_{prefix}_{secret}");

    Generated {
        // ★ hash 对**完整 plaintext** 取,不是对 secret 取 —— 与 parse 返回的
        //   full 口径严格一致,两者不一致会导致所有 key 永远 401。
        hash: hash_hex(&plaintext),
        prefix,
        plaintext,
    }
}

/// 从整个 bearer 值剥出 (prefix, full)。
///
/// ★ `full` = **完整的 bearer 串**(含 `rhk_` 前缀),不是 secret 部分:
///   落库的 key_hash 就是 sha256(完整 bearer),校验时直接对 `full` 哈希即可。
///
/// 边界语义(刻意为之):
/// - 大小写都接受(`is_ascii_hexdigit`),但 hash 对**原始输入串**计算,
///   所以大写 key 一定校验失败 —— 不做 to_lowercase,那会让哈希口径变复杂。
/// - 不做 trim():带前后空格的 key 视为形态错误(401)。
pub fn parse(bearer: &str) -> Option<(&str, &str)> {
    let rest = bearer.strip_prefix(TAG)?;
    let (p, f) = rest.split_once('_')?;
    if p.len() != 8 || !p.chars().all(|c| c.is_ascii_hexdigit()) {
        return None;
    }
    if f.len() != 64 || !f.chars().all(|c| c.is_ascii_hexdigit()) {
        return None;
    }
    Some((p, bearer)) // ★ 返回 (prefix, 完整 bearer)
}

/// hex(sha256(s))。
pub fn hash_hex(s: &str) -> String {
    hex::encode(Sha256::digest(s.as_bytes()))
}

/// 零依赖常量时间相等。长度不等直接 false(长度本身不是秘密)。
///
/// 不为这个加 `ring` 依赖 —— rh-server 的 Cargo.toml 里没有 ring,
/// 手写 XOR 折叠即可满足"不因比较时机泄露匹配进度"。
pub fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_rejects_malformed() {
        assert!(parse("rhk_").is_none(), "空尾");
        assert!(parse("rhk_9f2c1a4b").is_none(), "缺 _");
        assert!(
            parse(&format!("rhk_9f2c1a4zz_{}", "a".repeat(64))).is_none(),
            "前缀非 hex"
        );
        assert!(
            parse(&format!("rhk_9f2c1a4b_{}", "g".repeat(64))).is_none(),
            "secret 非 hex"
        );
        assert!(
            parse(&format!("rhk_9f2c1a4b_{}", "a".repeat(63))).is_none(),
            "secret 超短"
        );
        assert!(
            parse(&format!("rhk_9f2c1a4b_{}", "a".repeat(65))).is_none(),
            "secret 超长"
        );
        assert!(
            parse(&format!("Bearer rhk_9f2c1a4b_{}", "a".repeat(64))).is_none(),
            "带 Bearer 前缀(未剥)"
        );
        assert!(parse("").is_none(), "空串");
    }

    #[test]
    fn parse_accepts_valid() {
        let full = format!("rhk_9f2c1a4b_{}", "ab".repeat(32));
        let (p, f) = parse(&full).unwrap();
        assert_eq!(p, "9f2c1a4b");
        assert_eq!(f, full, "full 必须含 rhk_ 前缀(哈希口径)");
        // B-1 验收:大写形态能 parse 成功(形态合法),但 hash 对原始
        // 输入串计算 → 与建 key 时(小写)必然不同,校验必失败。
        // 这是刻意的安全行为,不为"大小写不敏感"做 to_lowercase。
        let upper = format!("rhk_9F2C1A4B_{}", "AB".repeat(32));
        let (up, uf) = parse(&upper).unwrap();
        assert_eq!(up, "9F2C1A4B");
        assert_eq!(uf, upper);
        assert_ne!(
            hash_hex(uf),
            hash_hex(&full),
            "大写串的哈希必须与小写原串不同(同字符异 case)"
        );
    }

    #[test]
    fn ct_eq_semantics() {
        assert!(ct_eq(b"abc", b"abc"));
        assert!(!ct_eq(b"abc", b"abd"), "等长不等 → false");
        assert!(!ct_eq(b"abc", b"ab"), "长度不等 → false 且不 panic");
        assert!(!ct_eq(b"", b"x"));
        assert!(ct_eq(b"", b""));
    }

    #[test]
    fn generate_unique() {
        let mut seen = std::collections::HashSet::new();
        for _ in 0..1000 {
            let g = generate();
            assert!(g.plaintext.starts_with("rhk_"));
            assert_eq!(g.plaintext.len(), 77, "4+8+1+64");
            assert_eq!(g.hash.len(), 64);
            assert_eq!(g.prefix.len(), 8);
            // generate→parse→hash 全链一致:parse 的 full 再哈希必须等于存的 hash
            let (p, f) = parse(&g.plaintext).unwrap();
            assert_eq!(p, g.prefix);
            assert_eq!(hash_hex(f), g.hash);
            assert!(seen.insert(g.plaintext), "1000 次无重复");
        }
    }
}
