#!/usr/bin/env bash
# rabbithole 服务端一键部署脚本(Linux systemd;其他环境安装二进制与配置后打印手动运行指引)。
#
# 用法:
#   curl -fsSL https://raw.githubusercontent.com/xbclub/rabbithole/main/deploy/install-server.sh | sudo bash
#   RH_VERSION=v0.2.1 ... | sudo bash          # 安装指定版本(默认 latest)
#   ... | sudo bash -s -- --prefix /opt/rh     # 自定义安装前缀(默认 /usr/local)
#
# 行为:下载 Release 静态二进制(musl,含 sha256 校验)→ 安装 rh-server →
# 首次写入 /etc/rabbithole/server.toml(已存在则保留,升级不覆盖配置)→
# 注册 systemd 服务并启动。重复执行 = 升级(配置与数据保留)。
# 注意:数据库与自动生成的 TLS 证书与配置同目录(实现如此),均在 /etc/rabbithole/。
set -euo pipefail

REPO="xbclub/rabbithole"
VERSION="${RH_VERSION:-latest}"
PREFIX="/usr/local"
CONF_DIR="${RH_CONF_DIR:-/etc/rabbithole}"
SERVICE="rabbithole-server"

while [ $# -gt 0 ]; do
  case "$1" in
    --prefix) PREFIX="$2"; shift 2 ;;
    --version) VERSION="$2"; shift 2 ;;
    -h|--help) grep '^#' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "未知参数:$1(--help 查看)" >&2; exit 1 ;;
  esac
done

[ "$(id -u)" -eq 0 ] || { echo "请以 root 运行(sudo bash)"; exit 1; }

# ---------- 架构与目标 ----------
MACHINE="$(uname -m)"
case "$MACHINE" in
  x86_64|amd64)   ARCH="x86_64" ;;
  aarch64|arm64)  ARCH="aarch64" ;;
  *) echo "不支持的架构:$MACHINE(发布包仅覆盖 amd64/arm64)" >&2; exit 1 ;;
esac
case "$(uname -s)" in
  Linux)  TARGET="$ARCH-unknown-linux-musl" ;;
  Darwin) TARGET="$ARCH-apple-darwin"; echo "注意:macOS 无 systemd,仅安装二进制与配置,末尾打印手动运行指引" >&2 ;;
  *) echo "不支持的系统:$(uname -s),请到 Releases 手动下载" >&2; exit 1 ;;
esac

# ---------- 解析版本 ----------
if [ "$VERSION" = "latest" ]; then
  VERSION="$(curl -fsSL "https://api.github.com/repos/$REPO/releases/latest" \
    | grep -o '"tag_name": *"[^"]*"' | head -1 | sed 's/.*"v\{0,1\}\([^"]*\)"/\1/')"
  [ -n "$VERSION" ] || { echo "无法解析最新版本号" >&2; exit 1; }
fi
echo ">> 安装 rabbithole-server v$VERSION ($TARGET)"

# ---------- 下载 + 校验 ----------
TMP="$(mktemp -d)"; trap 'rm -rf "$TMP"' EXIT
BASE="https://github.com/$REPO/releases/download/v$VERSION/rabbithole-v$VERSION-$TARGET"
curl -fsSL -o "$TMP/pkg.tar.gz" "$BASE.tar.gz"
curl -fsSL -o "$TMP/pkg.tar.gz.sha256" "$BASE.tar.gz.sha256"
# 校验文件记录的是原始包名,直接比对哈希值(兼容 sha256sum / shasum)
EXPECT_SHA="$(awk '{print $1}' "$TMP/pkg.tar.gz.sha256")"
if command -v sha256sum >/dev/null 2>&1; then
  ACTUAL_SHA="$(sha256sum "$TMP/pkg.tar.gz" | awk '{print $1}')"
else
  ACTUAL_SHA="$(shasum -a 256 "$TMP/pkg.tar.gz" | awk '{print $1}')"
fi
[ "$EXPECT_SHA" = "$ACTUAL_SHA" ] || { echo "sha256 校验失败" >&2; exit 1; }
echo ">> sha256 校验通过"
tar -xzf "$TMP/pkg.tar.gz" -C "$TMP"
mkdir -p "$PREFIX/bin"
mkdir -p "$PREFIX/bin"
install -m 0755 "$TMP/rabbithole-v$VERSION-$TARGET/rh-server" "$PREFIX/bin/rh-server"
echo ">> rh-server → $PREFIX/bin/rh-server"

# ---------- 配置(升级不覆盖) ----------
if [ -f "$CONF_DIR/server.toml" ]; then
  echo ">> 已存在 $CONF_DIR/server.toml,保留(升级路径)"
else
  mkdir -p "$CONF_DIR"
  cat > "$CONF_DIR/server.toml" <<'EOF'
# rabbithole 服务端配置(一键部署默认值;管理面暴露公网,首登后请立即改密,
# 生产建议置于反向代理 + HTTPS 之后)
bind_addr = "0.0.0.0"
bind_protocol = "tls"          # 客户端入口;控制面证书自动生成于本目录
bind_port = 7100
multiplex = true

[web]
bind_addr = "0.0.0.0"          # Web 控制台;如仅本机访问可改 127.0.0.1 + SSH 隧道
port = 8080

[vhost]
http_port = 8081
https_port = 8443
tcpmux_port = 8082

[ssh_gateway]
enabled = false
bind_port = 2222

[ports]
auto_range = "20000-21000"
allow_ports = ["10000-40000"]

[limits]
# 并发上限默认 0 = 不限;控制台「设置 → 并发与限流」可热改
default_pool_size = 2

[log]
level = "info"
EOF
  chmod 0644 "$CONF_DIR/server.toml"
  echo ">> 已写入默认配置 $CONF_DIR/server.toml"
fi

# ---------- 证书目录 ----------
# 控制面 TLS 自签证书落在 <配置目录>/certs/(rh-server 按 config 路径推导)。
# 目录在这里显式创建并收紧权限,而不是留给运行时自动 mkdir:
#   * 安装期就能暴露"目录不可写"这类问题,而不是等首次启动生成证书失败
#     —— 那种失败是文件系统错误,看不出与证书有关,服务端会直接起不来;
#   * 0700 让目录本身不可列举。私钥文件虽是 0600,但目录可读会泄露
#     "有哪些证书、何时生成"这类元信息。
# 已存在则只补权限,不覆盖内容(升级路径保留既有证书,指纹不变)。
# 目录建不出来只告警不中断:此时服务端首次启动仍会自己尝试创建
# (auto_cert 里有 create_dir_all),安装脚本不该因此半途而废;但如果
# 那里也失败,服务端会直接起不来 —— 故把原因明确告诉操作者。
if mkdir -p "$CONF_DIR/certs" 2>/dev/null; then
  if chmod 0700 "$CONF_DIR/certs" 2>/dev/null; then
    echo ">> 证书目录 $CONF_DIR/certs (0700)"
  else
    echo "!! 无法把 $CONF_DIR/certs 权限设为 0700,请手动检查属主" >&2
  fi
else
  echo "!! 无法创建 $CONF_DIR/certs(服务端首次启动可能失败,请手动检查)" >&2
fi

# ---------- systemd ----------
if command -v systemctl >/dev/null 2>&1 && [ -d /run/systemd/system ]; then
  cat > "/etc/systemd/system/$SERVICE.service" <<EOF
[Unit]
Description=rabbithole server (port forwarding gateway)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
ExecStart=$PREFIX/bin/rh-server --config $CONF_DIR/server.toml
Restart=on-failure
RestartSec=3
# 需内核 fs.nr_open >= 1000000(默认 1048576)
LimitNOFILE=1000000
# 数据库与证书在配置同目录:/etc/rabbithole/

[Install]
WantedBy=multi-user.target
EOF
  systemctl daemon-reload
  systemctl enable "$SERVICE.service" >/dev/null
  systemctl restart "$SERVICE.service"
  echo ">> systemd 服务已启动:$SERVICE"
  echo ""
  echo "完成。初始 admin 密码(仅首次启动展示一次):"
  echo "  journalctl -u $SERVICE | grep -A3 'password:'"
  echo "控制台: http://<本机地址>:8080/  (默认账号 admin)"
else
  echo ""
  echo "完成(无 systemd,未注册服务)。手动运行:"
  echo "  $PREFIX/bin/rh-server --config $CONF_DIR/server.toml"
  echo "初始 admin 密码在进程 stdout 横幅,仅首次启动展示一次。"
fi
