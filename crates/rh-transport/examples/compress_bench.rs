//! snappy 压缩泵微基准(诊断用,不替代三机验收)。
//!
//! 直接驱动 `compress_stream` 的两端(压缩泵 + 解压泵),测得的是
//! 「固定每块开销」而非网络吞吐。数据量固定、块型可切换,便于改前改后对比。
//!
//! 用法:
//! ```text
//! cargo run --release -p rh-transport --example compress_bench -- [MiB] [reps] [noise|random|zero|mixed]
//! ```
//! 输出 `RESULT ...` 行可直接留存;RSS 行只作量级参考(RSS 只增不减,
//! 分配次数的严肃判定在 `compress.rs` 单测的计数探针里)。

use rh_transport::compress_stream;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn pct(mut v: Vec<f64>, p: f64) -> f64 {
    v.sort_unstable_by(|a, b| a.partial_cmp(b).unwrap());
    v[((v.len() as f64 - 1.0) * p) as usize]
}

fn gen_data(total: usize, kind: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(total);
    let mut x: u64 = 0x2545F4914F6CDD1D;
    while out.len() < total {
        match kind {
            // 不可压缩:每块都是新的 xorshift 序列(模拟已压缩/加密流量)
            "random" => {
                x = x.wrapping_mul(6364136223846793005).wrapping_add(1);
                for _ in 0..64 * 1024 {
                    x ^= x << 13;
                    x ^= x >> 7;
                    x ^= x << 17;
                    out.push((x >> 24) as u8);
                }
            }
            // 全零:snappy 最省力的一档
            "zero" => out.extend(std::iter::repeat_n(0u8, 64 * 1024)),
            // 混合:一半 0xAB 长游程 + 一半伪随机,接近真实业务负载
            "mixed" => {
                for i in 0..64 * 1024 {
                    if i % 2 == 0 {
                        out.push(0xAB);
                    } else {
                        x ^= x << 13;
                        x ^= x >> 7;
                        x ^= x << 17;
                        out.push((x >> 24) as u8);
                    }
                }
            }
            // 默认:64KiB 周期噪声(半压缩,常见于文本/日志)
            _ => {
                for _ in 0..64 * 1024 {
                    x ^= x << 13;
                    x ^= x >> 7;
                    x ^= x << 17;
                    out.push((x >> 24) as u8);
                }
            }
        }
        if out.len() > total {
            out.truncate(total);
        }
    }
    out
}

/// 跑一整轮:压 → 解,校验长度。返回墙钟耗时。
async fn one_round(data: &[u8]) -> Duration {
    let (net_a, net_b) = tokio::io::duplex(64 * 1024);
    let client = compress_stream(Box::new(net_a)).await;
    let server = compress_stream(Box::new(net_b)).await;
    let expect = data.len() as u64;

    let t0 = Instant::now();
    let payload = data.to_vec();
    let writer = tokio::spawn(async move {
        let mut w = client;
        w.write_all(&payload).await.unwrap();
        w.shutdown().await.unwrap();
    });
    let reader = tokio::spawn(async move {
        let mut r = server;
        let mut buf = vec![0u8; 32 * 1024];
        let mut got = 0u64;
        loop {
            let n = r.read(&mut buf).await.unwrap();
            if n == 0 {
                break;
            }
            got += n as u64;
        }
        got
    });
    writer.await.unwrap();
    let got = reader.await.unwrap();
    let el = t0.elapsed();
    assert_eq!(got, expect, "decoded length mismatch");
    el
}

fn rss_kb() -> u64 {
    // macOS: rusage 的 maxrss 单位是字节;Linux: KB。统一折成 KB。
    #[cfg(unix)]
    {
        let mut ru = std::mem::MaybeUninit::<libc::rusage>::uninit();
        // SAFETY: getrusage 只写 ru。
        let rc = unsafe { libc::getrusage(libc::RUSAGE_SELF, ru.as_mut_ptr()) };
        if rc == 0 {
            // SAFETY: rc==0 时结构体已初始化。
            let ru = unsafe { ru.assume_init() };
            #[cfg(target_os = "macos")]
            {
                return (ru.ru_maxrss as u64) / 1024;
            }
            #[cfg(not(target_os = "macos"))]
            {
                return ru.ru_maxrss as u64;
            }
        }
        0
    }
    #[cfg(not(unix))]
    {
        0
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mib: usize = args.first().and_then(|s| s.parse().ok()).unwrap_or(64);
    let reps: usize = args.get(1).and_then(|s| s.parse().ok()).unwrap_or(7);
    let kind = args.get(2).map(String::as_str).unwrap_or("noise").to_string();
    let total = mib * 1024 * 1024;

    println!(
        "compress_bench: {} MiB x {} reps, block=64KiB, pattern={}",
        mib, reps, kind
    );
    // 预热一轮(首轮含线程/缓冲冷启动)
    let data = gen_data(total, &kind);
    let _ = one_round(&data).await;

    let mut times = Vec::with_capacity(reps);
    let rss_before = rss_kb();
    let mut rss_peak = rss_before;
    for _ in 0..reps {
        times.push(one_round(&data).await);
        rss_peak = rss_peak.max(rss_kb());
    }
    let best = times.iter().copied().min().unwrap();
    let med = pct(times.iter().map(|d| d.as_secs_f64()).collect(), 0.5);
    let p90 = pct(times.iter().map(|d| d.as_secs_f64()).collect(), 0.9);
    for (i, t) in times.iter().enumerate() {
        println!(
            "RESULT round={} kind={} mib={} secs={:.4} mibps={:.1}",
            i,
            kind,
            mib,
            t.as_secs_f64(),
            mib as f64 / t.as_secs_f64()
        );
    }
    println!(
        "RESULT summary kind={} mib={} reps={} best_secs={:.4} best_mibps={:.1} med_secs={:.4} med_mibps={:.1} p90_secs={:.4}",
        kind,
        mib,
        reps,
        best.as_secs_f64(),
        mib as f64 / best.as_secs_f64(),
        med,
        mib as f64 / med,
        p90
    );
    println!(
        "RESULT rss kind={} before_kb={} peak_kb={} delta_kb={}",
        kind,
        rss_before,
        rss_peak,
        rss_peak.saturating_sub(rss_before)
    );
}
