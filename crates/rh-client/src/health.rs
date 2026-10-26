//! 健康检查:客户端周期探测本地目标,状态变化时上报。

use crate::session::ClientSession;
use rh_protocol::message::{ClientMessage, HealthReport};
use rh_protocol::{HealthCheckKind, HealthCheckSpec, ProxySpec};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_util::sync::CancellationToken;
use tracing::info;

pub async fn run(
    sess: Arc<ClientSession>,
    proxy_id: u64,
    spec: Arc<ProxySpec>,
    hc: HealthCheckSpec,
    cancel: CancellationToken,
) {
    let mut healthy = true;
    let mut fails: u32 = 0;
    let interval = std::time::Duration::from_secs(hc.interval_seconds.max(1));
    let mut tk = tokio::time::interval(interval);
    tk.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let _ = tk.tick().await;
    // 启动首报
    report(&sess, proxy_id, healthy, "").await;
    loop {
        tokio::select! {
            _ = cancel.cancelled() => return,
            _ = sess.cancel.cancelled() => return,
            _ = tk.tick() => {
                let ok = probe(&spec, &hc).await;
                if ok {
                    if !healthy {
                        healthy = true;
                        fails = 0;
                        info!(proxy = %spec.name, "health recovered");
                        report(&sess, proxy_id, true, "").await;
                    } else {
                        fails = 0;
                    }
                } else {
                    fails += 1;
                    if healthy && fails >= hc.max_failed.max(1) {
                        healthy = false;
                        info!(proxy = %spec.name, fails, "health check failing");
                        report(&sess, proxy_id, false, "probe failed").await;
                    }
                }
            }
        }
    }
}

async fn report(sess: &Arc<ClientSession>, proxy_id: u64, healthy: bool, reason: &str) {
    let _ = sess
        .send_msg(ClientMessage::HealthReport(HealthReport {
            proxy_id,
            healthy,
            reason: reason.to_string(),
        }))
        .await;
}

async fn probe(spec: &ProxySpec, hc: &HealthCheckSpec) -> bool {
    // 插件:仅 unix_domain_socket/tcp 类可探测;带本地语义的插件探测其本地地址
    let (host, port) = match &spec.plugin {
        Some(p) => match p {
            rh_protocol::PluginSpec::UnixDomainSocket { .. } => {
                // unix socket:TCP 探测不可用;视为健康
                return true;
            }
            rh_protocol::PluginSpec::HttpProxy { .. }
            | rh_protocol::PluginSpec::Socks5 { .. }
            | rh_protocol::PluginSpec::StaticFile { .. } => {
                return true; // 本地代理/文件服务无固定探测点
            }
            _ => plugin_local(p),
        },
        None => (spec.target_host.clone(), spec.target_port),
    };
    let timeout = std::time::Duration::from_secs(hc.timeout_seconds.max(1));
    match hc.kind {
        HealthCheckKind::Tcp => {
            let Ok(addr) = crate::forward::resolve(&host, port).await else {
                return false;
            };
            matches!(
                tokio::time::timeout(timeout, tokio::net::TcpStream::connect(addr)).await,
                Ok(Ok(_))
            )
        }
        HealthCheckKind::Http => {
            let Ok(addr) = crate::forward::resolve(&host, port).await else {
                return false;
            };
            let Ok(s) = tokio::time::timeout(timeout, tokio::net::TcpStream::connect(addr)).await
            else {
                return false;
            };
            let Ok(mut s) = s else { return false };
            let path = if hc.path.is_empty() { "/" } else { &hc.path };
            let mut req = format!("GET {path} HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n");
            for h in &hc.http_headers {
                req.push_str(&format!("{}: {}\r\n", h.name, h.value));
            }
            req.push_str("\r\n");
            if s.write_all(req.as_bytes()).await.is_err() {
                return false;
            }
            let mut buf = [0u8; 128];
            let n = tokio::time::timeout(timeout, s.read(&mut buf))
                .await
                .ok()
                .and_then(|r| r.ok());
            match n {
                Some(n) if n >= 12 => {
                    let status = &buf[..12.min(n)];
                    // 2xx/3xx
                    status.len() >= 12 && (status[9] == b'2' || status[9] == b'3')
                }
                _ => false,
            }
        }
    }
}

fn plugin_local(p: &rh_protocol::PluginSpec) -> (String, u16) {
    let addr_of = |s: &str| -> (String, u16) {
        match s.rsplit_once(':') {
            Some((h, p)) => (h.to_string(), p.parse().unwrap_or(0)),
            None => (s.to_string(), 0),
        }
    };
    match p {
        rh_protocol::PluginSpec::Https2Http { local_addr, .. }
        | rh_protocol::PluginSpec::Https2Https { local_addr, .. }
        | rh_protocol::PluginSpec::Http2Https { local_addr, .. }
        | rh_protocol::PluginSpec::Http2Http { local_addr, .. }
        | rh_protocol::PluginSpec::Tls2Raw { local_addr, .. } => addr_of(local_addr),
        _ => (String::new(), 0),
    }
}
