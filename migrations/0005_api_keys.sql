-- API Key 认证(2026-10):控制台 REST API 的第二凭据。
-- key_hash = sha256(完整 bearer 串 "rhk_<prefix>_<secret>") 的 hex,
-- ★哈希对象是完整串而不是 secret 部分 —— 两种口径的哈希完全不同,
-- 全链路(generate/verify)必须一致,否则所有 key 永远 401。
CREATE TABLE api_keys (
  id           TEXT PRIMARY KEY,              -- uuid v4
  name         TEXT NOT NULL UNIQUE,          -- 人类可读,如 "ci-terraform"
  prefix       TEXT NOT NULL,                 -- 8 hex,展示 + 唯一索引定位
  key_hash     TEXT NOT NULL,                 -- sha256(完整 bearer) 的 hex
  scopes       INTEGER NOT NULL DEFAULT 0,
  enabled      INTEGER NOT NULL DEFAULT 1 CHECK (enabled IN (0,1)),
  created_at   INTEGER NOT NULL,
  created_by   TEXT NOT NULL DEFAULT '',
  last_used_at INTEGER,
  expires_at   INTEGER                        -- NULL = 永不过期
);
-- prefix 用独立索引而非列级 UNIQUE:便于将来改部分索引(软删回收 prefix)
CREATE UNIQUE INDEX idx_api_keys_prefix ON api_keys(prefix);
