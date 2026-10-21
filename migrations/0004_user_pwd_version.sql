-- B11:登出/改密必须能让既有 JWT 失效。
-- pwd_version 随每次登出、改密 +1;JWT claims 携带签发时的版本,
-- 校验时与服务端当前值比对,不等即 401。
ALTER TABLE users ADD COLUMN pwd_version INTEGER NOT NULL DEFAULT 0;
