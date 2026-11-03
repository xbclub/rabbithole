#!/usr/bin/env bash
# rabbithole 客户端一键部署脚本(Linux systemd;其他环境安装二进制与凭据后打印手动运行指引)。
#
# 用法(凭据在服务端控制台「客户端」页创建,仅展示一次;控制台可直接复制本命令):
#   curl -fsSL https://raw.githubusercontent.com/xbclub/rabbithole/main/deploy/install-client.sh \
#     | sudo bash -s -- --server <服务端地址:端口> --id <客户端ID> --secret <密钥>
# 可选:--protocol tcp|tls|ws|wss|quic|kcp(与服务端 bind_protocol 一致,默认 tls)
#      --insecure   服务端自签证书且未配置信任时跳过校验(测试环境)
#      --version v0.2.1 / --prefix /opt/rh
#   --id/--secret 缺省时交互式询问。
#
# 注意:客户端脚本以环境变量接入,需 rabbithole v0.2.2 及以上(默认安装最新版)。
# 行为:下载 Release 静态二进制(musl,含 sha256 校验)→ 安装 rh-client →
# 写 /etc/rabbithole/client.env(0600,凭据所在;重跑覆盖,可用于轮换密钥)→
# 注册 systemd 服务(EnvironmentFile 接入,零配置文件)并启动。
set -euo pipefail

REPO="xbclub/rabbithole"
VERSION="${RH_VERSION:-latest}"
PREFIX="/usr/local"
CONF_DIR="${RH_CONF_DIR:-/etc/rabbithole}"
SERVICE="rabbithole-client"
SERVER=""; CLIENT_ID=""; SECRET=""; PROTOCOL=""; INSECURE=""

while [ $# -gt 0 ]; do
  case "$1" in
    --server)  SERVER="$2"; shift 2 ;;
    --id)      CLIENT_ID="$2"; shift 2 ;;
    --secret)  SECRET="$2"; shift 2 ;;
    --protocol) PROTOCOL="$2"; shift 2 ;;
    --insecure) INSECURE="1"; shift ;;
    --prefix)  PREFIX="$2"; shift 2 ;;
    --version) VERSION="$2"; shift 2 ;;
    -h|--help) grep '^#' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "未知参数:$1(--help 查看)" >&2; exit 1 ;;
  esac
done

[ "$(id -u)" -eq 0 ] || { echo "请以 root 运行(sudo bash)"; exit 1; }

# 交互补齐缺省凭据
if [ -z "$SERVER" ]; then
  read -r -p "服务端地址(IP 或域名:port,如 203.0.113.10:7100):" SERVER
fi
if [ -z "$CLIENT_ID" ]; then
  read -r -p "客户端 ID(控制台「客户端」页创建):" CLIENT_ID
fi
if [ -z "$SECRET" ]; then
  read -r -s -p "客户端密钥(输入不回显):" SECRET; echo
fi
[ -n "$SERVER" ] && [ -n "$CLIENT_ID" ] && [ -n "$SECRET" ] || { echo "缺少连接信息" >&2; exit 1; }
case "$SERVER" in
  *:*) ;;
  *) echo "服务端地址需含端口,如 203.0.113.10:7100" >&2; exit 1 ;;
esac

# ---------- 架构与目标 ----------
MACHINE="$(uname -m)"
case "$MACHINE" in
  x86_64|amd64)   ARCH="x86_64" ;;
  aarch64|arm64)  ARCH="aarch64" ;;
  *) echo "不支持的架构:$MACHINE(发布包仅覆盖 amd64/arm64)" >&2; exit 1 ;;
esac
case "$(uname -s)" in
  Linux)  TARGET="$ARCH-unknown-linux-musl" ;;
  Darwin) TARGET="$ARCH-apple-darwin"; echo "注意:macOS 无 systemd,仅安装二进制与凭据,末尾打印手动运行指引" >&2 ;;
  *) echo "不支持的系统:$(uname -s),请到 Releases 手动下载" >&2; exit 1 ;;
esac

# ---------- 解析版本 ----------
if [ "$VERSION" = "latest" ]; then
  VERSION="$(curl -fsSL "https://api.github.com/repos/$REPO/releases/latest" \
    | grep -o '"tag_name": *"[^"]*"' | head -1 | sed 's/.*"v\{0,1\}\([^"]*\)"/\1/')"
  [ -n "$VERSION" ] || { echo "无法解析最新版本号" >&2; exit 1; }
fi
echo ">> 安装 rabbithole-client v$VERSION ($TARGET)"

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
install -m 0755 "$TMP/rabbithole-v$VERSION-$TARGET/rh-client" "$PREFIX/bin/rh-client"
echo ">> rh-client → $PREFIX/bin/rh-client"

# ---------- 凭据(0600;重跑覆盖 = 轮换密钥) ----------
mkdir -p "$CONF_DIR"
if [ -f "$CONF_DIR/client.env" ]; then
  cp -f "$CONF_DIR/client.env" "$CONF_DIR/client.env.bak"
  echo ">> 已备份原 client.env → client.env.bak"
fi
{
  echo "RH_SERVER=\"$SERVER\""
  echo "RH_CLIENT_ID=\"$CLIENT_ID\""
  echo "RH_SECRET=\"$SECRET\""
  [ -n "$PROTOCOL" ] && echo "RH_PROTOCOL=\"$PROTOCOL\""
  [ -n "$INSECURE" ] && echo "RH_TLS_INSECURE=true"
} > "$CONF_DIR/client.env"
chmod 0600 "$CONF_DIR/client.env"
echo ">> 凭据已写入 $CONF_DIR/client.env (0600)"

# ---------- systemd ----------
if command -v systemctl >/dev/null 2>&1 && [ -d /run/systemd/system ]; then
  cat > "/etc/systemd/system/$SERVICE.service" <<EOF
[Unit]
Description=rabbithole client (edge tunnel agent)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
EnvironmentFile=$CONF_DIR/client.env
ExecStart=$PREFIX/bin/rh-client
Restart=always
RestartSec=3
# 需内核 fs.nr_open >= 1000000(默认 1048576)
LimitNOFILE=1000000

[Install]
WantedBy=multi-user.target
EOF
  systemctl daemon-reload
  systemctl enable "$SERVICE.service" >/dev/null
  systemctl restart "$SERVICE.service"
  echo ">> systemd 服务已启动:$SERVICE"
  echo ""
  echo "完成。状态:systemctl status $SERVICE ;日志:journalctl -u $SERVICE -f"
else
  echo ""
  echo "完成(无 systemd,未注册服务)。手动运行:"
  echo "  set -a; . $CONF_DIR/client.env; set +a; $PREFIX/bin/rh-client"
fi
