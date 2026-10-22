//! SSH Tunnel Gateway:任意 OpenSSH 客户端 `ssh -R` 直接注册远程转发。
//!
//! - 认证:用户名 = 客户端 ID;密码 = 客户端密钥(常量时间比较);
//!   或 authorized_keys 公钥(用户名仍须为已启用客户端)。
//! - `ssh -R [addr:]port:host:port user@gw` → 服务端绑定监听;用户连接到来时
//!   经 SSH `forwarded-tcpip` 通道送回 ssh 客户端,由其对 host:port 发起真实连接。
//! - 端口 0 → 服务端自动分配;SSH 连接断开即释放该连接注册的全部监听。

use crate::state::AppState;
use russh::server::Handle as SshHandle;
use russh::server::{Auth, Handler, Session};
use russh::ChannelMsg;
use ssh_key::{LineEnding, PrivateKey, PublicKey};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_util::sync::CancellationToken;

pub struct SshGateway {
    state: Arc<AppState>,
    host_key_dir: PathBuf,
}

/// SSH 网关监督任务:跟随热配置启停监听(网页开关/换端口/换公钥即时生效)。
/// 每次变更取消当前 serve(在途会话随连接自然结束),按最新配置重建。
pub async fn supervisor(
    state: Arc<AppState>,
    mut watch: tokio::sync::watch::Receiver<u64>,
    shutdown: CancellationToken,
) {
    loop {
        let cfg = state.hot.read().await.ssh_gateway.clone();
        if cfg.enabled {
            let key_dir = cfg.auto_gen_key_dir.clone().unwrap_or_else(|| {
                state
                    .cfg
                    .config_path
                    .parent()
                    .map(|p| p.to_path_buf().join("certs"))
                    .unwrap_or_else(|| std::path::PathBuf::from("certs"))
            });
            let token = CancellationToken::new();
            let gw = SshGateway::new(state.clone(), key_dir);
            let t2 = token.clone();
            let handle = tokio::spawn(async move {
                if let Err(e) = gw.serve(t2).await {
                    tracing::warn!(error = %e, "ssh gateway serve error");
                }
            });
            tokio::select! {
                _ = watch.changed() => {
                    token.cancel();
                    let _ = handle.await;
                }
                _ = shutdown.cancelled() => {
                    token.cancel();
                    let _ = handle.await;
                    return;
                }
            }
        } else {
            tokio::select! {
                _ = watch.changed() => {}
                _ = shutdown.cancelled() => return,
            }
        }
    }
}

impl SshGateway {
    pub fn new(state: Arc<AppState>, host_key_dir: PathBuf) -> Self {
        Self {
            state,
            host_key_dir,
        }
    }

    pub async fn serve(self, stop: CancellationToken) -> anyhow::Result<()> {
        let cfg = self.state.hot.read().await.ssh_gateway.clone();
        let authorized_keys = load_authorized_keys(cfg.authorized_keys_file.as_ref());
        let host_key = load_or_gen_host_key(&self.host_key_dir)?;
        let scfg = Arc::new(russh::server::Config {
            auth_rejection_time: std::time::Duration::from_secs(1),
            keys: vec![host_key],
            ..Default::default()
        });

        let bind_ip = if cfg.bind_addr.is_empty() {
            "0.0.0.0".to_string()
        } else {
            cfg.bind_addr.clone()
        };
        let addr = format!("{bind_ip}:{}", cfg.bind_port);
        let l = tokio::net::TcpListener::bind(&addr).await?;
        tracing::info!(%addr, "ssh gateway up");
        loop {
            let (sock, _peer) = tokio::select! {
                a = l.accept() => match a { Ok(v) => v, Err(_) => continue },
                _ = stop.cancelled() => {
                    tracing::info!("ssh gateway stopped (config change)");
                    return Ok(());
                }
            };
            let state = self.state.clone();
            let keys = authorized_keys.clone();
            let cfg2 = scfg.clone();
            tokio::spawn(async move {
                let forwards: ForwardMap = Arc::new(tokio::sync::Mutex::new(HashMap::new()));
                let peer_ip = sock
                    .peer_addr()
                    .map(|a| a.ip())
                    .unwrap_or(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST));
                let session = SshSession {
                    state,
                    peer_ip,
                    client_id: String::new(),
                    authorized_keys: keys,
                    forwards: forwards.clone(),
                };
                // 会话结束(连接断开)即释放该连接注册的全部监听
                let running = match russh::server::run_stream(cfg2, sock, session).await {
                    Ok(r) => r,
                    Err(_) => return,
                };
                let _ = running.await;
                for (_, t) in forwards.lock().await.drain() {
                    t.cancel();
                }
                tracing::info!("ssh gateway session closed");
            });
        }
    }
}

fn load_authorized_keys(path: Option<&PathBuf>) -> Arc<Vec<PublicKey>> {
    let Some(p) = path else {
        return Arc::new(Vec::new());
    };
    let mut out = Vec::new();
    if let Ok(text) = std::fs::read_to_string(p) {
        for line in text.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            // ★容忍带 options 的行(标准 authorized_keys 允许前置选项,
            // 如 `command="..." ssh-ed25519 AAAA...`)。
            //
            // `PublicKey::from_openssh` 把第一个空格段当算法名,带 options 时
            // 会解析失败并整行丢弃 —— 运维把现成的 authorized_keys 直接拷
            // 过来时,带选项的密钥会静默失效。这里按 SSH 规范跳过前置 options
            // (以逗号分隔、整体通常含 `=`,且出现在算法名之前)。
            let key_part = strip_authorized_keys_options(line);
            match PublicKey::from_openssh(key_part) {
                Ok(k) => out.push(k),
                Err(e) => tracing::warn!(%e, line, "ssh gateway: bad authorized_keys line"),
            }
        }
    } else {
        tracing::warn!(path = %p.display(), "ssh gateway: authorized_keys unreadable");
    }
    Arc::new(out)
}

/// 跳过 `authorized_keys` 行首的可选 options,返回纯 `type base64 [comment]` 部分。
///
/// 标准格式:`[options] keytype base64 [comment]`。options 可能本身含空格
/// (引号内),所以按**引号是否闭合**扫描而不是简单 split:找到第一个不在
/// 引号内、且其后紧接已知密钥类型的空格位置。
fn strip_authorized_keys_options(line: &str) -> &str {
    const KEY_TYPES: [&str; 6] = ["ssh-", "ecdsa-", "sk-", "rsa-", "dsa-", "pkcs11:"];
    let bytes = line.as_bytes();
    let mut in_quote = false;
    let mut i = 0;
    while i < bytes.len() {
        let c = bytes[i];
        if c == b'"' {
            in_quote = !in_quote;
        } else if c == b' ' && !in_quote {
            let rest = &line[i + 1..];
            if KEY_TYPES.iter().any(|t| rest.starts_with(t)) {
                return rest;
            }
        }
        i += 1;
    }
    // 没有前置 options(或不是已知类型):原样返回,交给 from_openssh 报错
    line
}

fn load_or_gen_host_key(dir: &PathBuf) -> anyhow::Result<PrivateKey> {
    std::fs::create_dir_all(dir)?;
    let path = dir.join("ssh_host_ed25519");
    if let Ok(text) = std::fs::read_to_string(&path) {
        return PrivateKey::from_openssh(&text)
            .map_err(|e| anyhow::anyhow!("ssh host key parse: {e}"));
    }
    let mut rng = rand_010::rng();
    let key = PrivateKey::random(&mut rng, ssh_key::Algorithm::Ed25519)
        .map_err(|e| anyhow::anyhow!("ssh host key gen: {e}"))?;
    let pem = key.to_openssh(LineEnding::LF)?;
    // 私钥文件直接以 0600 创建(先写后 chmod 存在世界可读窗口)
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&path)?;
        use std::io::Write as _;
        f.write_all(pem.as_bytes())?;
    }
    #[cfg(not(unix))]
    {
        std::fs::write(&path, pem.as_bytes())?;
    }
    tracing::info!(path = %path.display(), "ssh gateway: host key generated");
    Ok(key)
}

type ForwardMap = Arc<tokio::sync::Mutex<HashMap<u16, CancellationToken>>>;

/// 每条 SSH 连接可注册的转发监听上限。
const MAX_FORWARDS_PER_SESSION: usize = 16;

struct SshSession {
    state: Arc<AppState>,
    /// 对端 IP(认证失败限速用)。
    peer_ip: std::net::IpAddr,
    client_id: String,
    authorized_keys: Arc<Vec<PublicKey>>,
    /// 本连接注册的监听:port → 取消器(断开级联释放)。
    forwards: ForwardMap,
}

impl Handler for SshSession {
    type Error = anyhow::Error;

    async fn auth_password(&mut self, user: &str, password: &str) -> Result<Auth, Self::Error> {
        let ok = match self.state.db.get_client(user).await {
            Ok(Some(c)) => {
                c.enabled
                    && rh_transport::constant_time_eq(c.secret.as_bytes(), password.as_bytes())
            }
            _ => false,
        };
        if ok {
            self.client_id = user.to_string();
            tracing::info!(client = user, "ssh gateway auth ok (password)");
            Ok(Auth::Accept)
        } else {
            // ★失败计数限速:SSH 网关此前只有 1s 的 `auth_rejection_time`,
            // 没有次数上限 —— 单个 IP 可以无限次尝试(每次都是一次 HMAC
            // 比较 + 一次 DB 查询)。复用登录限速器(按 IP,30 次/分钟),
            // 超限直接拒绝。
            if self
                .state
                .login_limiter
                .check_key(&format!("ssh|{}", self.peer_ip))
                .is_err()
            {
                tracing::warn!(ip = %self.peer_ip, user, "ssh gateway auth rate limited");
                // 限速期间退避更久,压住爆破速率
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            }
            Ok(Auth::reject())
        }
    }

    async fn auth_publickey(
        &mut self,
        user: &str,
        public_key: &PublicKey,
    ) -> Result<Auth, Self::Error> {
        // PublicKey 的派生相等含注释字段; authorized_keys 行注释与线上无关,只比密钥本体。
        // 绑定约定:行尾注释恰为客户端 ID(UUID 形态)时,该密钥只能登录该客户端;
        // 其余注释(如 ssh-keygen 默认的 user@host)或无注释视为全局密钥。
        let listed = self.authorized_keys.iter().any(|k| {
            if k.key_data() != public_key.key_data() {
                return false;
            }
            let cmt: &str = k.comment().as_ref();
            let cmt = cmt.trim();
            cmt.is_empty() || cmt == user || uuid::Uuid::parse_str(cmt).is_err()
        });
        let client_ok = match self.state.db.get_client(user).await {
            Ok(Some(c)) => c.enabled,
            _ => false,
        };
        if listed && client_ok {
            self.client_id = user.to_string();
            tracing::info!(client = user, "ssh gateway auth ok (publickey)");
            Ok(Auth::Accept)
        } else {
            Ok(Auth::reject())
        }
    }

    /// 接受会话通道并"空跑"命令:隧道场景仅作会话保持,不真正执行。
    async fn channel_open_session(
        &mut self,
        _channel: russh::Channel<russh::server::Msg>,
        reply: russh::server::ChannelOpenHandle,
        _session: &mut Session,
    ) -> Result<(), Self::Error> {
        reply.accept().await;
        Ok(())
    }

    async fn exec_request(
        &mut self,
        channel: russh::ChannelId,
        _data: &[u8],
        session: &mut Session,
    ) -> Result<(), Self::Error> {
        let _ = session.channel_success(channel);
        Ok(())
    }

    async fn shell_request(
        &mut self,
        channel: russh::ChannelId,
        session: &mut Session,
    ) -> Result<(), Self::Error> {
        let _ = session.channel_success(channel);
        Ok(())
    }

    async fn tcpip_forward(
        &mut self,
        address: &str,
        port: &mut u32,
        session: &mut Session,
    ) -> Result<bool, Self::Error> {
        if self.client_id.is_empty() {
            return Ok(false);
        }
        // 每连接转发数量上限(防单连接注册大量监听)
        {
            let fw = self.forwards.lock().await;
            if fw.len() >= MAX_FORWARDS_PER_SESSION {
                tracing::warn!(client = %self.client_id, "ssh gateway: forward limit reached");
                return Ok(false);
            }
        }
        // 仅允许环回/显式配置地址:客户端不得在服务器上任意网卡开洞
        let bind_ip: std::net::IpAddr =
            if address.is_empty() || address.eq_ignore_ascii_case("localhost") {
                "127.0.0.1".parse().unwrap()
            } else {
                match address.parse::<std::net::IpAddr>() {
                    Ok(ip) if ip.is_loopback() => ip,
                    _ => {
                        tracing::warn!(
                            client = %self.client_id,
                            addr = address,
                            "ssh gateway: non-loopback bind rejected"
                        );
                        return Ok(false);
                    }
                }
            };
        let requested = *port as u16;
        let bind_port = if requested == 0 {
            match self.state.proxies.allocate_port("tcp", bind_ip).await {
                Some(p) => p,
                None => return Ok(false),
            }
        } else {
            // 客户端指定端口必须过白名单与占用检查(与常规规则同策略,
            // 不得绕过 allow_ports 在服务器上抢占任意端口)
            if !self.state.proxies.port_allowed(requested).await {
                tracing::warn!(
                    client = %self.client_id,
                    port = requested,
                    "ssh gateway: port not in allow_ports"
                );
                return Ok(false);
            }
            if self
                .state
                .proxies
                .port_in_use("tcp", bind_ip, requested, None, None)
                .await
            {
                tracing::warn!(
                    client = %self.client_id,
                    port = requested,
                    "ssh gateway: port in use"
                );
                return Ok(false);
            }
            requested
        };
        let l = match tokio::net::TcpListener::bind((bind_ip, bind_port)).await {
            Ok(l) => l,
            Err(_) => return Ok(false),
        };
        let allocated = l.local_addr()?.port();
        *port = allocated as u32;
        let cancel = CancellationToken::new();
        self.forwards.lock().await.insert(allocated, cancel.clone());
        let handle = session.handle();
        let st = self.state.clone();
        let client_id = self.client_id.clone();
        // 通道回程的 connected_address 必须与注册串一致(OpenSSH 按其匹配转发)
        let registered = if address.is_empty() {
            "localhost".to_string()
        } else {
            address.to_string()
        };
        tokio::spawn(forward_accept(
            st, handle, l, cancel, client_id, allocated, registered,
        ));
        tracing::info!(client = %self.client_id, port = allocated, "ssh gateway forward registered");
        Ok(true)
    }

    async fn cancel_tcpip_forward(
        &mut self,
        _address: &str,
        port: u32,
        _session: &mut Session,
    ) -> Result<bool, Self::Error> {
        if let Some(t) = self.forwards.lock().await.remove(&(port as u16)) {
            t.cancel();
        }
        Ok(true)
    }
}

/// 监听接受循环:每个用户连接开一条 forwarded-tcpip 通道回 ssh 客户端。
async fn forward_accept(
    state: Arc<AppState>,
    handle: SshHandle,
    l: tokio::net::TcpListener,
    cancel: CancellationToken,
    client_id: String,
    port: u16,
    registered_addr: String,
) {
    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                tracing::info!(client = %client_id, port, "ssh gateway forward released");
                return;
            }
            acc = l.accept() => {
                let Ok((sock, peer)) = acc else { return };
                let h = handle.clone();
                let st2 = state.clone();
                let reg = registered_addr.clone();
                tokio::spawn(async move {
                    st2.metrics.conns_current.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    let ch = match h
                        .channel_open_forwarded_tcpip(
                            reg,
                            port as u32,
                            peer.ip().to_string(),
                            peer.port() as u32,
                        )
                        .await
                    {
                        Ok(c) => c,
                        Err(_) => {
                            st2.metrics.conns_current.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
                            return;
                        }
                    };
                    pump(sock, ch).await;
                    st2.metrics.conns_current.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
                });
            }
        }
    }
}

/// 用户 TCP ↔ SSH 通道双向搬运(含半关闭)。
async fn pump(sock: tokio::net::TcpStream, mut channel: russh::Channel<russh::server::Msg>) {
    let (mut sr, mut sw) = sock.into_split();
    let mut writer = channel.make_writer();
    let t1 = tokio::spawn(async move {
        let mut buf = vec![0u8; 32 * 1024];
        loop {
            match sr.read(&mut buf).await {
                Ok(0) | Err(_) => break,
                Ok(n) => {
                    if writer.write_all(&buf[..n]).await.is_err() {
                        break;
                    }
                }
            }
        }
        let _ = writer.shutdown().await;
    });
    let mut pending = Vec::new();
    loop {
        let msg = channel.wait().await;
        match msg {
            Some(ChannelMsg::Data { data }) => pending.extend_from_slice(&data),
            Some(ChannelMsg::Eof) | Some(ChannelMsg::Close) | None => break,
            _ => {}
        }
        while !pending.is_empty() {
            match sw.write_buf(&mut &pending[..]).await {
                Ok(0) | Err(_) => break,
                Ok(n) => {
                    let _ = pending.drain(..n);
                }
            }
        }
    }
    // 对端结束发送:向用户侧传播半关闭,否则 t1 会一直等用户主动断开
    let _ = sw.shutdown().await;
    let _ = t1.await;
    let _ = channel.close().await;
}

#[cfg(test)]
mod authorized_keys_tests {
    use super::*;

    /// 带 options 的行必须能取出密钥本体。
    #[test]
    fn strips_leading_options() {
        let line = r#"command="echo hi",no-pty ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAI user@host"#;
        assert_eq!(
            strip_authorized_keys_options(line),
            "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAI user@host"
        );
    }

    /// 无 options 的普通行原样返回。
    #[test]
    fn plain_line_unchanged() {
        let line = "ssh-rsa AAAAB3NzaC1yc2EAAA user@host";
        assert_eq!(strip_authorized_keys_options(line), line);
    }

    /// options 内含空格(引号内)时不得从引号中间切断。
    #[test]
    fn options_with_spaces_in_quotes() {
        let line = r#"command="echo hello world" ssh-ed25519 AAAA test"#;
        assert_eq!(strip_authorized_keys_options(line), "ssh-ed25519 AAAA test");
    }

    /// 没有任何密钥类型时原样返回(让上层报错,不静默吞掉)。
    #[test]
    fn unknown_type_returned_as_is() {
        let line = "garbage line here";
        assert_eq!(strip_authorized_keys_options(line), line);
    }
}
