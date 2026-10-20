#!/bin/sh
# rabbithole 容器入口:数据卷无配置时写入默认配置,再启动 rh-server。
# 数据库(rabbithole.db)与自动生成的 TLS 证书都落在配置同目录(即数据卷)。
set -e

DATA_DIR=/var/lib/rabbithole
mkdir -p "$DATA_DIR"

if [ ! -f "$DATA_DIR/server.toml" ]; then
  cp /etc/rabbithole/server.toml "$DATA_DIR/server.toml"
  echo "[entrypoint] 已初始化默认配置:$DATA_DIR/server.toml"
fi

# 控制面 TLS 自签证书目录(<数据卷>/certs/)。显式创建并收紧权限,
# 与 install-server.sh 同一口径:
#   * 0700 让目录不可列举(私钥文件本身是 0600);
#   * 容器内以 root 运行,chmod 失败只告警不阻断启动 —— 有些挂载
#     (如 NFS / 只读卷)不支持改权限,不能因此让服务起不来。
# 目录已在数据卷内,重建容器只要卷还在,证书与指纹都不变。
mkdir -p "$DATA_DIR/certs"
chmod 0700 "$DATA_DIR/certs" 2>/dev/null \
  || echo "[entrypoint] 警告:无法把 $DATA_DIR/certs 权限设为 0700"

exec rh-server --config "$DATA_DIR/server.toml"
