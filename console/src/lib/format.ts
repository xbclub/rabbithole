/**
 * 展示格式化(自旧版 console 的 utils/format.ts 与各页内联实现平移,口径不变)。
 * 端口/IP/UUID/字节/速率/时间戳一律 mono + tabular-nums,见 .rh-mono。
 */

/** 字节数 → 人类可读。旧版口径:>1GiB 才升 GB,1024 边界严格大于。 */
export function fmtBytes(n: number | null | undefined): string {
  const v = n ?? 0;
  if (v > 1024 * 1024 * 1024) return `${(v / 1024 ** 3).toFixed(2)} GB`;
  if (v > 1024 * 1024) return `${(v / 1024 ** 2).toFixed(2)} MB`;
  if (v > 1024) return `${(v / 1024).toFixed(1)} KB`;
  return `${v} B`;
}

/** unix 毫秒 → 本地时间字符串(zh-CN,24 小时制);空值显示 `-`。 */
export function fmtTime(ts: number | null | undefined): string {
  if (!ts) return '-';
  return new Date(ts).toLocaleString('zh-CN', { hour12: false });
}

/** 仅时间部分(HH:mm:ss),图表 tooltip 用。 */
export function fmtClock(ts: number | null | undefined): string {
  if (!ts) return '-';
  return new Date(ts).toLocaleTimeString('zh-CN', { hour12: false });
}

/** 相对时间:"刚刚" / "2 分钟前" / "3 小时前" / "5 天前"。 */
export function fmtRelative(ts: null | number | undefined): string {
  if (!ts) return '-';
  const diff = Date.now() - ts;
  if (diff < 60_000) return '刚刚';
  if (diff < 3_600_000) return `${Math.floor(diff / 60_000)} 分钟前`;
  if (diff < 86_400_000) return `${Math.floor(diff / 3_600_000)} 小时前`;
  return `${Math.floor(diff / 86_400_000)} 天前`;
}

/** 剩余天数(证书到期倒计时);已过期为负。 */
export function daysUntil(ts: null | number | undefined): null | number {
  if (!ts) return null;
  return Math.floor((ts - Date.now()) / 86_400_000);
}

/** MB → 字节(客户端流量配额表单用)。 */
export function mbToBytes(mb: number | null | undefined): number | undefined {
  if (mb === null || mb === undefined || Number.isNaN(mb)) return undefined;
  return Math.round(mb * 1024 * 1024);
}

/** 字节 → MB(编辑回填用,保留 2 位)。 */
export function bytesToMb(bytes: number | null | undefined): number | null {
  if (!bytes) return null;
  return Math.round((bytes / (1024 * 1024)) * 100) / 100;
}
