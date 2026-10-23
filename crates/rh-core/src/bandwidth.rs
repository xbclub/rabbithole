//! 带宽数量解析("1MB"/"512KB"/"1048576" → 字节/秒),对齐 frp 语义。

use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BandwidthQuantity {
    #[serde(rename = "bps")]
    bytes_per_sec: u64,
}

impl BandwidthQuantity {
    pub fn bytes_per_sec(&self) -> u64 {
        self.bytes_per_sec
    }

    /// 解析 "1MB"/"1.5MB"/"512KB"/"1048576"。
    pub fn parse(s: &str) -> Option<Self> {
        let s = s.trim();
        if s.is_empty() {
            return None;
        }
        let (num_part, unit) =
            s.split_at(s.find(|c: char| c.is_ascii_alphabetic()).unwrap_or(s.len()));
        let num: f64 = num_part.trim().parse().ok()?;
        let mult = match unit.trim().to_ascii_uppercase().as_str() {
            "" | "B" => 1.0,
            "KB" => 1024.0,
            "MB" => 1024.0 * 1024.0,
            "GB" => 1024.0 * 1024.0 * 1024.0,
            _ => return None,
        };
        let v = num * mult;
        if v.is_finite() && v >= 0.0 && v <= u64::MAX as f64 {
            Some(Self {
                bytes_per_sec: v as u64,
            })
        } else {
            None
        }
    }
}

impl fmt::Display for BandwidthQuantity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}B/s", self.bytes_per_sec)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_variants() {
        assert_eq!(
            BandwidthQuantity::parse("1MB").unwrap().bytes_per_sec(),
            1048576
        );
        assert_eq!(
            BandwidthQuantity::parse("512KB").unwrap().bytes_per_sec(),
            512 * 1024
        );
        assert_eq!(
            BandwidthQuantity::parse("2048").unwrap().bytes_per_sec(),
            2048
        );
        assert_eq!(
            BandwidthQuantity::parse("1.5MB").unwrap().bytes_per_sec(),
            1_572_864
        );
        assert_eq!(BandwidthQuantity::parse("0").unwrap().bytes_per_sec(), 0);
        assert!(BandwidthQuantity::parse("").is_none());
        assert!(BandwidthQuantity::parse("1XB").is_none());
        assert!(BandwidthQuantity::parse("abc").is_none());
    }
}
