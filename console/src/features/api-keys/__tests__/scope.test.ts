/**
 * scope 常量与后端对齐的钉子单测(期望值全部写死字面量)。
 * 后端 `pub mod scope` 的位定义一漂移,这里立刻红。
 */
import { describe, expect, it } from 'vitest';
import { FULL, OPS, READONLY, SCOPE_BITS, scopeBadges } from '../scope';

describe('scope 常量与后端对齐', () => {
  // ★ 这三个数必须与后端 scope 模块逐字相同。写死具体值(不用
  // READ_MASK|WRITE_MASK 表达式),后端改位定义、前端漏改时会红。
  it('三个预设值钉死', () => {
    expect(READONLY).toBe(5461);
    expect(OPS).toBe(5503);
    expect(FULL).toBe(16383);
  });

  it('每个位常量等于 1<<n', () => {
    expect(SCOPE_BITS.clients_r).toBe(1);
    expect(SCOPE_BITS.clients_w).toBe(2);
    expect(SCOPE_BITS.services_r).toBe(4);
    expect(SCOPE_BITS.services_w).toBe(8);
    expect(SCOPE_BITS.visitors_r).toBe(16);
    expect(SCOPE_BITS.visitors_w).toBe(32);
    expect(SCOPE_BITS.stats_r).toBe(64);
    expect(SCOPE_BITS.events_r).toBe(256); // 位 8,不是 128
    expect(SCOPE_BITS.config_r).toBe(1024);
    expect(SCOPE_BITS.config_w).toBe(2048);
    expect(SCOPE_BITS.certs_r).toBe(4096);
    expect(SCOPE_BITS.certs_w).toBe(8192);
  });

  it('scopeBadges 命中预设', () => {
    expect(scopeBadges(READONLY)).toEqual(['只读']);
    expect(scopeBadges(OPS)).toEqual(['运维']);
    expect(scopeBadges(FULL)).toEqual(['全部权限']);
  });

  it('scopeBadges 非预设按位展开', () => {
    expect(scopeBadges(0)).toEqual([]);
    expect(scopeBadges(1)).toEqual(['clients_r']);
    expect(scopeBadges(1 | 2)).toEqual(['clients_r', 'clients_w']);
  });
});
