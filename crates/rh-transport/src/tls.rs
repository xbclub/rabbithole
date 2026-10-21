//! rustls 配置构建:服务端证书(含自动自签)、客户端 CA/pin/校验策略。
//!
//! pin 模式:握手采用 NoVerify 完成后立刻比对叶子证书 SHA-256 指纹,
//! 不匹配则断开(任何应用数据发送前完成),等价于固定证书校验且无需
//! 自定义 ServerCertVerifier。

use crate::{TlsClientOptions, TlsServerOptions};
use rustls_pki_types::pem::PemObject;
use rustls_pki_types::{CertificateDer, PrivateKeyDer};
use std::io;
use std::sync::Arc;

/// 客户端校验策略(握手后需要执行的附加检查)。
#[derive(Debug, Clone)]
pub enum ClientVerify {
    /// 固定叶子证书 SHA-256 指纹。
    Pin([u8; 32]),
    /// 无附加检查(CA 链已在握手内校验)。
    Standard,
    /// 无任何校验(仅测试)。
    Insecure,
    /// 握手不校验,但**上层必须**用 secret 加密下发的方式核对对端证书
    /// (见 [`crate::TlsClientOptions::secret_verified`])。
    SecretVerified,
}

/// 服务端 TLS 配置。cert/key 或自动自签(可持久化)。
pub fn server_config(opts: &TlsServerOptions) -> io::Result<Arc<rustls::ServerConfig>> {
    server_config_with_alpn(opts, &[crate::DEFAULT_ALPN.to_string()])
}

pub fn server_config_with_alpn(
    opts: &TlsServerOptions,
    alpn: &[String],
) -> io::Result<Arc<rustls::ServerConfig>> {
    let (cert, key) = match (&opts.cert_pem, &opts.key_pem) {
        (Some(c), Some(k)) => (c.clone(), k.clone()),
        (None, None) => auto_cert(opts)?,
        _ => {
            return Err(io::Error::other(
                "tls: cert_pem and key_pem must be provided together",
            ))
        }
    };
    let certs: Vec<CertificateDer> = CertificateDer::pem_slice_iter(&cert)
        .collect::<Result<_, _>>()
        .map_err(|e| io::Error::other(format!("tls: parse cert: {e}")))?;
    let key = PrivateKeyDer::from_pem_slice(&key)
        .map_err(|e| io::Error::other(format!("tls: parse key: {e}")))?;
    let mut cfg = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| io::Error::other(format!("tls: build server config: {e}")))?;
    cfg.alpn_protocols = alpn.iter().map(|s| s.as_bytes().to_vec()).collect();
    // 0-RTT 永久禁用(B12 / docs/0rtt-remediation.md §3.1):early data 无
    // 防重放保护。依赖库默认值即为 0,这里显式置零以把"依赖默认"升级为
    // "代码承诺",并由 `tests::server_disables_early_data` 锁定。
    cfg.max_early_data_size = 0;
    Ok(Arc::new(cfg))
}

/// 生成或复用自签证书;persist_prefix 为 Some 时落盘 `<prefix>.crt/<prefix>.key`。
pub fn auto_cert(opts: &TlsServerOptions) -> io::Result<(Vec<u8>, Vec<u8>)> {
    if let Some(prefix) = &opts.persist_prefix {
        let crt = prefix.with_extension("crt");
        let key = prefix.with_extension("key");
        if crt.exists() && key.exists() {
            let c = std::fs::read(&crt)?;
            let k = std::fs::read(&key)?;
            return Ok((c, k));
        }
        let (c, k) = generate_self_signed()?;
        // 落盘前确保证书目录存在(persist_prefix 通常指向 <配置目录>/certs/ 子目录)
        if let Some(dir) = crt.parent() {
            std::fs::create_dir_all(dir)?;
        }
        std::fs::write(&crt, &c)?;
        // 私钥以 0600 直接创建(先写后 chmod 存在世界可读窗口)
        #[cfg(unix)]
        {
            use std::io::Write as _;
            use std::os::unix::fs::OpenOptionsExt as _;
            let mut f = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(&key)?;
            f.write_all(&k)?;
        }
        #[cfg(not(unix))]
        {
            std::fs::write(&key, &k)?;
            set_key_perm(&key)?;
        }
        return Ok((c, k));
    }
    generate_self_signed()
}

/// 非 unix 平台的私钥权限收紧(unix 上直接以 0600 创建,不走此函数)。
#[cfg(not(unix))]
fn set_key_perm(_path: &std::path::Path) -> io::Result<()> {
    Ok(())
}

/// 生成自签证书。
///
/// **不用 `rcgen::generate_simple_self_signed`**:它的 CN 固定为
/// `rcgen self signed cert`,在任何抓包里都是显眼的指纹(证书是明文传输的,
/// 中间人/DPI 直接可读)。这里显式构造参数,CN 用 SAN 的首项(默认
/// `localhost`),与普通自签证书无异。
fn generate_self_signed() -> io::Result<(Vec<u8>, Vec<u8>)> {
    let mut names = vec![
        "localhost".to_string(),
        "127.0.0.1".to_string(),
        "::1".to_string(),
    ];
    if let Ok(host) = std::env::var("RHTP_TLS_EXTRA_SAN") {
        if !host.is_empty() {
            names.push(host);
        }
    }
    let mut params = rcgen::CertificateParams::new(names.clone())
        .map_err(|e| io::Error::other(format!("tls: cert params: {e}")))?;
    // CN 取首个标识(通常 localhost),避免出现 rcgen 的默认字样。
    params.distinguished_name = {
        let mut dn = rcgen::DistinguishedName::new();
        dn.push(rcgen::DnType::CommonName, names[0].clone());
        dn
    };
    let key_pair = rcgen::KeyPair::generate()
        .map_err(|e| io::Error::other(format!("tls: self-signed key: {e}")))?;
    let cert = params
        .self_signed(&key_pair)
        .map_err(|e| io::Error::other(format!("tls: self-signed gen: {e}")))?;
    Ok((
        cert.pem().into_bytes(),
        key_pair.serialize_pem().into_bytes(),
    ))
}

/// 免校验器(配合握手后 pin 比对或显式 insecure 测试配置)。
#[derive(Debug)]
struct NoVerify;

impl rustls::client::danger::ServerCertVerifier for NoVerify {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &rustls_pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls_pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }
    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }
    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }
    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        vec![
            rustls::SignatureScheme::RSA_PKCS1_SHA256,
            rustls::SignatureScheme::RSA_PKCS1_SHA384,
            rustls::SignatureScheme::RSA_PKCS1_SHA512,
            rustls::SignatureScheme::ECDSA_NISTP256_SHA256,
            rustls::SignatureScheme::ECDSA_NISTP384_SHA384,
            rustls::SignatureScheme::ED25519,
            rustls::SignatureScheme::RSA_PSS_SHA256,
            rustls::SignatureScheme::RSA_PSS_SHA384,
            rustls::SignatureScheme::RSA_PSS_SHA512,
        ]
    }
}

/// 客户端 TLS 配置 + 附加校验策略。
/// - pin_sha256:握手后比对指纹;
/// - trusted_ca:标准链校验;
/// - insecure_skip_verify:无校验(测试)。
pub fn client_config(
    opts: &TlsClientOptions,
) -> io::Result<(Arc<rustls::ClientConfig>, ClientVerify)> {
    client_config_with_alpn(opts, &[crate::DEFAULT_ALPN.to_string()])
}

pub fn client_config_with_alpn(
    opts: &TlsClientOptions,
    alpn: &[String],
) -> io::Result<(Arc<rustls::ClientConfig>, ClientVerify)> {
    if let Some(pin) = &opts.pin_sha256 {
        let bytes = hex_to_32(pin)?;
        let cfg = rustls::ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(NoVerify))
            .with_no_client_auth();
        return Ok((with_alpn(cfg, alpn), ClientVerify::Pin(bytes)));
    }
    if opts.insecure_skip_verify {
        let cfg = rustls::ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(NoVerify))
            .with_no_client_auth();
        return Ok((with_alpn(cfg, alpn), ClientVerify::Insecure));
    }
    if let Some(ca) = &opts.trusted_ca {
        let mut roots = rustls::RootCertStore::empty();
        for cert in CertificateDer::pem_slice_iter(ca) {
            let cert = cert.map_err(|e| io::Error::other(format!("tls: parse ca: {e}")))?;
            roots
                .add(cert)
                .map_err(|e| io::Error::other(format!("tls: add ca: {e}")))?;
        }
        let cfg = rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        return Ok((with_alpn(cfg, alpn), ClientVerify::Standard));
    }
    if opts.secret_verified {
        // 握手阶段不校验对端证书:此刻还没有任何先验信任锚。
        // 信任由**应用层**建立 —— 服务端用 secret 派生的密钥加密下发它的
        // 证书,客户端解出后与本次握手实际看到的对端证书比对。
        // 见 `rh_protocol::certprov` 与客户端 `verify_server_cert`。
        //
        // 安全性前提:必须真的执行那次比对。`SecretVerified` 只表示
        // "本端允许先握手再验证",不含任何保证;若上层不比对,等价于 insecure。
        let cfg = rustls::ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(NoVerify))
            .with_no_client_auth();
        return Ok((with_alpn(cfg, alpn), ClientVerify::SecretVerified));
    }
    Err(io::Error::other(
        "tls client: need trusted_ca, pin_sha256, insecure_skip_verify or secret_verified",
    ))
}

fn with_alpn(mut cfg: rustls::ClientConfig, alpn: &[String]) -> Arc<rustls::ClientConfig> {
    cfg.alpn_protocols = alpn.iter().map(|s| s.as_bytes().to_vec()).collect();
    // 0-RTT 禁用(B12 / docs/0rtt-remediation.md §3.2):会话恢复仅 1-RTT
    // PSK,无 early data 面。三个校验分支(pin/insecure/CA/secret_verified)
    // 统一经过这里。
    cfg.enable_early_data = false;
    cfg.resumption = rustls::client::Resumption::in_memory_sessions(256);
    Arc::new(cfg)
}

fn hex_to_32(s: &str) -> io::Result<[u8; 32]> {
    let s = s.trim().trim_start_matches("sha256:").replace(':', "");
    if s.len() != 64 || !s.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err(io::Error::other("pin_sha256 must be 64 hex chars"));
    }
    let mut out = [0u8; 32];
    for i in 0..32 {
        out[i] = u8::from_str_radix(&s[i * 2..i * 2 + 2], 16)
            .map_err(|e| io::Error::other(format!("bad pin hex: {e}")))?;
    }
    Ok(out)
}

/// SHA-256 指纹(sha2 crate,用于证书 pin)。
pub fn sha256(data: &[u8]) -> [u8; 32] {
    use sha2::Digest;
    let mut h = sha2::Sha256::new();
    h.update(data);
    let out = h.finalize();
    let mut arr = [0u8; 32];
    arr.copy_from_slice(&out);
    arr
}

/// 对握手完成后的连接执行 pin 比对。
pub fn verify_pin(expected: &[u8; 32], leaf_cert: &[u8]) -> io::Result<()> {
    let digest = sha256(leaf_cert);
    if crate::constant_time_eq(&digest, expected) {
        Ok(())
    } else {
        Err(io::Error::other("tls: certificate fingerprint mismatch"))
    }
}

/// 从 PEM 证书链取**叶子证书** SHA-256 指纹(hex,小写 64 字符)。
///
/// 与客户端 `pin_sha256` 记录的是同一个值,故服务端启动时打印这个字符串,
/// 运维可直接复制进客户端配置。
pub fn leaf_fingerprint_pem(cert_pem: &[u8]) -> io::Result<String> {
    let leaf = CertificateDer::pem_slice_iter(cert_pem)
        .next()
        .ok_or_else(|| io::Error::other("tls: empty certificate chain"))?
        .map_err(|e| io::Error::other(format!("tls: parse cert: {e}")))?;
    Ok(hex32(&sha256(leaf.as_ref())))
}

/// 从 PEM 证书链取叶子证书的 DER 字节(证书下发的载荷)。
pub fn leaf_der_pem(cert_pem: &[u8]) -> io::Result<Vec<u8>> {
    let leaf = CertificateDer::pem_slice_iter(cert_pem)
        .next()
        .ok_or_else(|| io::Error::other("tls: empty certificate chain"))?
        .map_err(|e| io::Error::other(format!("tls: parse cert: {e}")))?;
    Ok(leaf.as_ref().to_vec())
}

/// 从服务端 TLS 选项取叶子证书 DER(证书下发的载荷)。
///
/// 与 [`server_config_with_alpn`] 同源:显式 cert 优先,否则读
/// `persist_prefix` 落盘的 `.crt`。返回 `None` 表示该监听器没有可用证书
/// (例如纯明文控制面),调用方据此跳过下发。
///
/// **必须按监听器各自解析,不能全局只取一份**:多监听器
/// (`[[extra_listeners]]`)各有独立的自签证书,下发错的那份会让客户端
/// 的逐字节比对必然失败,并被误报成"可能存在中间人"。
pub fn leaf_der_of(opts: &TlsServerOptions) -> Option<Vec<u8>> {
    let pem = match (&opts.cert_pem, &opts.persist_prefix) {
        (Some(p), _) => p.clone(),
        (None, Some(prefix)) => std::fs::read(prefix.with_extension("crt")).ok()?,
        (None, None) => return None,
    };
    leaf_der_pem(&pem).ok()
}

/// 32 字节指纹 → 小写 hex(64 字符,与 `pin_sha256` 同格式)。
pub fn hex32(v: &[u8; 32]) -> String {
    v.iter().map(|x| format!("{x:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hex(b: &[u8]) -> String {
        b.iter().map(|x| format!("{x:02x}")).collect()
    }

    #[test]
    fn sha256_known_vectors() {
        assert_eq!(
            hex(&sha256(b"")),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        assert_eq!(
            hex(&sha256(b"abc")),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        let million_a = vec![b'a'; 1_000_000];
        assert_eq!(
            hex(&sha256(&million_a)),
            "cdc76e5c9914fb9281a1c7e284d73e67f1809a48a497200e046d39ccc7112cd0"
        );
    }

    /// ALPN 必须真的进入 rustls 配置。
    ///
    /// 这条测试的由来:服务端 `listen_stream` 曾误用 `server_config()`
    /// (内部固定取 DEFAULT_ALPN),导致 `alpn` 配置项被静默忽略 ——
    /// 配了 ["h2","http/1.1"] 实际只协商 h2,且客户端只提 http/1.1 时
    /// 服务端回 no_application_protocol。配置项形同虚设却无任何报错,
    /// 只能靠实测发现。此处从配置对象反查,锁死"传进去就生效"。
    #[test]
    fn alpn_list_reaches_rustls_config() {
        let (cert, key) = generate_self_signed().unwrap();
        let opts = TlsServerOptions {
            cert_pem: Some(cert),
            key_pem: Some(key),
            ..Default::default()
        };
        // 服务端:多值必须按顺序保留
        let want = vec!["h2".to_string(), "http/1.1".to_string()];
        let cfg = server_config_with_alpn(&opts, &want).unwrap();
        assert_eq!(
            cfg.alpn_protocols,
            want.iter()
                .map(|s| s.as_bytes().to_vec())
                .collect::<Vec<_>>(),
            "服务端 ALPN 列表必须完整进入配置且顺序不变"
        );

        // 客户端:同一份列表也要进入配置
        let c = client_config_with_alpn(
            &TlsClientOptions {
                insecure_skip_verify: true,
                ..Default::default()
            },
            &want,
        )
        .unwrap()
        .0;
        assert_eq!(
            c.alpn_protocols,
            want.iter()
                .map(|s| s.as_bytes().to_vec())
                .collect::<Vec<_>>(),
            "客户端 ALPN 列表必须完整进入配置"
        );

        // 默认路径用 DEFAULT_ALPN(不是写死的旧值)
        let def = server_config(&opts).unwrap();
        assert_eq!(
            def.alpn_protocols,
            vec![crate::DEFAULT_ALPN.as_bytes().to_vec()]
        );
        assert_eq!(crate::DEFAULT_ALPN, "h2");
    }

    #[test]
    fn self_signed_then_config() {
        let (cert, key) = generate_self_signed().unwrap();
        let opts = TlsServerOptions {
            cert_pem: Some(cert),
            key_pem: Some(key),
            ..Default::default()
        };
        server_config(&opts).unwrap();
    }

    #[test]
    fn pin_roundtrip() {
        let (cert, _) = generate_self_signed().unwrap();
        let leaf = CertificateDer::pem_slice_iter(&cert)
            .next()
            .unwrap()
            .unwrap();
        let digest = sha256(leaf.as_ref());
        assert!(verify_pin(&digest, leaf.as_ref()).is_ok());
        let mut bad = digest;
        bad[0] ^= 1;
        assert!(verify_pin(&bad, leaf.as_ref()).is_err());
    }

    // ---- B12 0-RTT 回归(docs/0rtt-remediation.md §3.4) ----

    fn server_opts() -> TlsServerOptions {
        TlsServerOptions::default()
    }

    fn client_opts() -> TlsClientOptions {
        TlsClientOptions {
            trusted_ca: None,
            server_name: None,
            insecure_skip_verify: true,
            pin_sha256: None,
            secret_verified: false,
        }
    }

    /// B12 / docs/0rtt-remediation.md §3.4-1:服务端必须显式禁用 early data。
    /// 不能只依赖 rustls 默认值 —— 依赖升级可能无声改变它。
    #[test]
    fn server_disables_early_data() {
        let cfg = server_config(&server_opts()).expect("server cfg");
        assert_eq!(
            cfg.max_early_data_size, 0,
            "服务端 max_early_data_size 必须为 0(0-RTT 永久禁用)"
        );
    }

    /// B12 §3.4-1:客户端必须显式关闭 early data,并保留 1-RTT PSK 恢复。
    #[test]
    fn client_disables_early_data() {
        let (cfg, verify) = client_config(&client_opts()).expect("client cfg");
        assert!(matches!(verify, ClientVerify::Insecure));
        assert!(
            !cfg.enable_early_data,
            "客户端 enable_early_data 必须为 false"
        );
    }

    /// B12 §3.4-1:secret_verified / pin / CA 三个分支共用 with_alpn,
    /// 全部必须关闭 early data(防止只改一个分支)。
    #[test]
    fn all_client_branches_disable_early_data() {
        // pin 分支
        let pin = "00".repeat(32);
        let (cfg, _) = client_config(&TlsClientOptions {
            pin_sha256: Some(pin),
            ..client_opts()
        })
        .expect("pin cfg");
        assert!(!cfg.enable_early_data, "pin 分支必须关闭 early data");

        // secret_verified 分支
        let (cfg, _) = client_config(&TlsClientOptions {
            secret_verified: true,
            insecure_skip_verify: false,
            ..client_opts()
        })
        .expect("secret_verified cfg");
        assert!(
            !cfg.enable_early_data,
            "secret_verified 分支必须关闭 early data"
        );

        // CA 分支:用自签证书当根(无需真的能验证成功,只检查配置字段)
        let (crt, _key) = generate_self_signed().expect("self signed");
        let (cfg, _) = client_config(&TlsClientOptions {
            trusted_ca: Some(crt),
            insecure_skip_verify: false,
            ..client_opts()
        })
        .expect("ca cfg");
        assert!(!cfg.enable_early_data, "CA 分支必须关闭 early data");
    }
}
