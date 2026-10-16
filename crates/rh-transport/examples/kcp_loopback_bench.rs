//! KCP loopback 微基准(诊断用,不替代三机验收):
//! - latency:N 次 64B 同步往返,统计 mean/p50/p99(写→flush 调度延迟敏感)
//! - throughput-1 / throughput-4:单流/4 流并发 16KiB 块 echo 吞吐
//!
//! 用法:cargo run --release -p rh-transport --example kcp_loopback_bench [-- <MiB> <reps>]
//! 输出 RESULT 行可直接留存;中位数在 summary 汇总。

use rh_transport::kcp;
use rh_transport::{ClientTransportConfig, Protocol, ServerTransportConfig};
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn mbps(bytes: u64, elapsed: Duration) -> f64 {
    bytes as f64 * 8.0 / elapsed.as_secs_f64() / 1e6
}

fn percentile_u64(mut v: Vec<u64>, p: f64) -> u64 {
    v.sort_unstable();
    v[((v.len() as f64 - 1.0) * p) as usize]
}

fn percentile_f64(mut v: Vec<f64>, p: f64) -> f64 {
    v.sort_unstable_by(|a, b| a.partial_cmp(b).unwrap());
    v[((v.len() as f64 - 1.0) * p) as usize]
}

async fn spawn_echo_server(mut listener: kcp::KcpListener, streams: usize) {
    let inc = listener.accept().await.expect("accept mux");
    for _ in 0..streams {
        let mut s = inc.mux.accept().await.expect("accept stream");
        tokio::spawn(async move {
            let mut buf = vec![0u8; 16 * 1024];
            loop {
                match s.read(&mut buf).await {
                    Ok(0) | Err(_) => break,
                    Ok(n) => {
                        if s.write_all(&buf[..n]).await.is_err() {
                            break;
                        }
                    }
                }
            }
        });
    }
}

async fn bench_latency(addr: std::net::SocketAddr, iters: usize) -> Vec<u64> {
    let cfg = ClientTransportConfig {
        protocol: Protocol::Kcp,
        server_addr: addr,
        ..Default::default()
    };
    let mux = kcp::connect(&cfg).await.expect("connect");
    let mut s = mux.open_stream().await.expect("open");
    let payload = vec![0x5Au8; 64];
    let mut back = [0u8; 64];
    // 预热(排除首包握手/窗口初始化)
    s.write_all(&payload).await.unwrap();
    s.read_exact(&mut back).await.unwrap();
    let mut samples = Vec::with_capacity(iters);
    for _ in 0..iters {
        let t = Instant::now();
        s.write_all(&payload).await.unwrap();
        s.read_exact(&mut back).await.unwrap();
        samples.push(t.elapsed().as_micros() as u64);
    }
    samples
}

async fn bench_throughput(addr: std::net::SocketAddr, streams: usize, total: u64) -> Duration {
    let cfg = ClientTransportConfig {
        protocol: Protocol::Kcp,
        server_addr: addr,
        ..Default::default()
    };
    let mux = Arc::new(kcp::connect(&cfg).await.expect("connect"));
    let t0 = Instant::now();
    let mut tasks = Vec::new();
    for _ in 0..streams {
        let mux = mux.clone();
        tasks.push(tokio::spawn(async move {
            let s = mux.open_stream().await.expect("open");
            let chunk = vec![0xA7u8; 16 * 1024];
            let (mut r, mut w) = tokio::io::split(s);
            let reader = tokio::spawn(async move {
                let mut remaining = total;
                let mut buf = vec![0u8; 16 * 1024];
                while remaining > 0 {
                    let want = buf.len().min(remaining as usize);
                    r.read_exact(&mut buf[..want]).await.expect("echo read");
                    remaining -= want as u64;
                }
            });
            let mut written = 0u64;
            while written < total {
                let n = chunk.len().min((total - written) as usize);
                w.write_all(&chunk[..n]).await.expect("write");
                written += n as u64;
            }
            reader.await.expect("reader join");
        }));
    }
    for t in tasks {
        t.await.expect("stream task");
    }
    t0.elapsed()
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mib: u64 = args.get(1).and_then(|s| s.parse().ok()).unwrap_or(64);
    let reps: usize = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(3);
    let total = mib * 1024 * 1024;

    let mut lat_p50 = Vec::new();
    let mut lat_p99 = Vec::new();
    let mut lat_mean = Vec::new();
    for r in 0..reps {
        kcp::counters_reset();
        let server_cfg = ServerTransportConfig {
            protocol: Protocol::Kcp,
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            ..Default::default()
        };
        let listener = kcp::listen(&server_cfg, Arc::new(AtomicUsize::new(0)))
            .await
            .expect("listen");
        let addr = listener.local_addr();
        tokio::spawn(spawn_echo_server(listener, 1));
        let s = tokio::time::timeout(Duration::from_secs(60), bench_latency(addr, 300))
            .await
            .expect("latency timeout");
        let mean = s.iter().sum::<u64>() / s.len() as u64;
        println!(
            "RESULT rep={} mode=latency mean_us={} p50_us={} p99_us={}",
            r,
            mean,
            percentile_u64(s.clone(), 0.5),
            percentile_u64(s.clone(), 0.99)
        );
        let c = kcp::counters();
        println!(
            "COUNTERS rep={} mode=latency bytes_moved={} wakeups={} packets_consumed={} input_calls={} update_calls={} flush_calls={} send_attempts={} recvfrom_calls={}",
            r,
            300 * 64 * 2,
            c.wakeups,
            c.packets_consumed,
            c.input_calls,
            c.update_calls,
            c.flush_calls,
            c.send_attempts,
            c.recvfrom_calls
        );
        lat_mean.push(mean);
        lat_p50.push(percentile_u64(s.clone(), 0.5));
        lat_p99.push(percentile_u64(s, 0.99));
    }
    println!(
        "SUMMARY mode=latency median_mean_us={} median_p50_us={} median_p99_us={}",
        percentile_u64(lat_mean.clone(), 0.5),
        percentile_u64(lat_p50.clone(), 0.5),
        percentile_u64(lat_p99.clone(), 0.5)
    );

    for streams in [1usize, 4] {
        let mut mbs = Vec::new();
        for r in 0..reps {
            kcp::counters_reset();
            let server_cfg = ServerTransportConfig {
                protocol: Protocol::Kcp,
                bind_addr: "127.0.0.1:0".parse().unwrap(),
                ..Default::default()
            };
            let listener = kcp::listen(&server_cfg, Arc::new(AtomicUsize::new(0)))
                .await
                .expect("listen");
            let addr = listener.local_addr();
            tokio::spawn(spawn_echo_server(listener, streams));
            let elapsed = tokio::time::timeout(
                Duration::from_secs(300),
                bench_throughput(addr, streams, total),
            )
            .await
            .expect("throughput timeout");
            let bytes = total * streams as u64;
            println!(
                "RESULT rep={} mode=throughput-{} mib={} elapsed_ms={} mbps_per_direction={:.1}",
                r,
                streams,
                mib,
                elapsed.as_millis(),
                mbps(bytes, elapsed)
            );
            let c = kcp::counters();
            println!(
                "COUNTERS rep={} mode=throughput-{} bytes_moved={} wakeups={} packets_consumed={} input_calls={} update_calls={} flush_calls={} send_attempts={} recvfrom_calls={}",
                r,
                streams,
                bytes * 2,
                c.wakeups,
                c.packets_consumed,
                c.input_calls,
                c.update_calls,
                c.flush_calls,
                c.send_attempts,
                c.recvfrom_calls
            );
            mbs.push(mbps(bytes, elapsed));
        }
        println!(
            "SUMMARY mode=throughput-{} median_mbps_per_direction={:.1}",
            streams,
            percentile_f64(mbs, 0.5)
        );
    }
}
