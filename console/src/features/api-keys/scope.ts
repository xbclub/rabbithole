/**
 * API Key 的 scope 位定义 —— 后端 `crates/rh-server/src/api.rs` 的
 * `pub mod scope` 逐位镜像(见任务书 §2.3)。
 *
 * ★没有编译期保证:后端改位定义必须同步改这里,单测把三个预设值
 * 与每个位写死成字面量,一漂移就红。
 */

/** 与后端 scope::* 常量逐位对齐。位 7/9 后端保留不用,故无对应键。 */
export const SCOPE_BITS = {
  clients_r: 1 << 0,
  clients_w: 1 << 1,
  services_r: 1 << 2,
  services_w: 1 << 3,
  visitors_r: 1 << 4,
  visitors_w: 1 << 5,
  stats_r: 1 << 6,
  events_r: 1 << 8,
  config_r: 1 << 10,
  config_w: 1 << 11,
  certs_r: 1 << 12,
  certs_w: 1 << 13,
} as const;

export const READONLY = 0x1555;
export const OPS = 0x157f;
export const FULL = 0x3fff;

/** 徽标文字:命中预设就显示预设名,否则按位列出。 */
export function scopeBadges(scopes: number): string[] {
  if (scopes === FULL) return ['全部权限'];
  if (scopes === OPS) return ['运维'];
  if (scopes === READONLY) return ['只读'];
  return Object.entries(SCOPE_BITS)
    .filter(([, bit]) => (scopes & bit) !== 0)
    .map(([name]) => name);
}
