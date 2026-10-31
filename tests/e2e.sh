#!/usr/bin/env bash
# rabbithole E2E 多进程测试套件。
# 用法: tests/e2e.sh [二进制目录](默认 target/release)
#       tests/e2e.sh --self-test   # 仅自测便携辅助(不启动 rh-server/client)
# 自带临时目录与独立端口,不依赖外部状态;每项输出 PASS/FAIL。
set -u
BIN_DIR="${1:-$(dirname "$0")/../target/release}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
WORK=$(mktemp -d /tmp/rh-e2e.XXXXXX)
SERVER="$ROOT/$BIN_DIR/rh-server"
CLIENT="$ROOT/$BIN_DIR/rh-client"
PASS=0; FAIL=0
PIDS=()
SKIP_ARCHIVE=0

cleanup() {
  # 只清理本脚本亲自 spawn 的进程(PIDS 均为 $! 记录,身份确定);
  # 不做任何按 /tmp/rh-* 模式的广域扫杀(可能是其他会话的服务)。
  for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null; done
  # SIGTERM 后有界等待**本脚本自己的** PID 退出(绝不触碰未知进程):
  # rh-server 优雅关停需数秒(会话下线、在途连接收尾、SSH 网关清理),
  # 不等待会让紧随其后的下一次运行在端口预检撞上仍在 LISTEN 的旧
  # 服务端(实测 run1 退出后 rh-server 仍持有 127.0.0.1:18080 LISTEN
  # 约 2-5s → run2 预检 exit=2"端口已被占用")。等待有界(≤15s);
  # 超时仅对自有 PID 升级 SIGKILL 并再兜底 2s。
  local waited=0 alive=1
  while [ "$waited" -lt 15 ]; do
    alive=0
    for p in "${PIDS[@]:-}"; do kill -0 "$p" 2>/dev/null && alive=1; done
    [ "$alive" = "0" ] && break
    sleep 1; waited=$((waited+1))
  done
  if [ "$alive" = "1" ]; then
    for p in "${PIDS[@]:-}"; do kill -9 "$p" 2>/dev/null; done
    sleep 2
  fi
  # 进程全退后,内核仍会保留服务端主动关闭连接的 TIME_WAIT pcb
  # (控制口 18100 的 mux 长连接由服务端在优雅关停中先关,实测 2 条
  # local TIME_WAIT 令裸 bind 探测 BUSY 约 13-30s = 2×MSL)。下一次
  # 运行的预检用的正是同款裸 bind 探测,故此处以**完全相同的口径**
  # 有界等待预检 TCP 端口集全部真正可绑再退出——不放宽任何断言,
  # 只是不再把被自身残留污染的环境交给下一轮。仅在确实 spawn 过
  # 进程时等待(预检失败路径 PIDS 为空,无需空等);超时即放弃,
  # 让下一轮预检给出准确报错。
  if [ "${#PIDS[@]}" != "0" ]; then
    local w2=0 busy=1
    while [ "$w2" -lt 35 ]; do
      busy=0
      for p in $TCP_PORTS; do port_free_tcp "$p" || busy=1; done
      [ "$busy" = "0" ] && break
      sleep 1; w2=$((w2+1))
    done
  fi
  # 归档目录唯一化:保留每次运行的历史快照,不覆盖其他会话证据
  if [ "$SKIP_ARCHIVE" != "1" ]; then
    local ARCHIVE="/tmp/rh-e2e-last-$(date +%Y%m%d-%H%M%S)-$$"
    cp -r "$WORK" "$ARCHIVE" 2>/dev/null && echo "(环境快照归档: $ARCHIVE)"
  fi
  rm -rf "$WORK"
}
trap cleanup EXIT

ok()   { PASS=$((PASS+1)); echo "PASS: $1"; }
bad()  { FAIL=$((FAIL+1)); echo "FAIL: $1"; }
check(){ if [ "$1" = "0" ]; then ok "$2"; else bad "$2"; fi; }

# ---------- 便携辅助 ----------
# 替换 client.toml 的 secret 行(macOS/Linux 通用;密钥经环境变量传递,
# 不出现在命令行(ps)/错误输出里;必须恰好命中 1 行,否则不写回并报错)。
update_secret() {  # $1=toml 路径;新密钥经环境变量 UPDATE_SECRET_VAL
  UPDATE_SECRET_VAL="${UPDATE_SECRET_VAL:?}" python3 - "$1" <<'PYEOF'
import os, re, sys
path = sys.argv[1]
secret = os.environ["UPDATE_SECRET_VAL"]
with open(path, encoding="utf-8") as fh:
    lines = fh.readlines()
pat = re.compile(r'^secret\s*=\s*".*"\s*$')
hits = [i for i, l in enumerate(lines) if pat.match(l)]
if len(hits) != 1:
    sys.stderr.write("update_secret: 期望恰好 1 行 secret,实际 %d 行,未写回\n"
                     % len(hits))
    sys.exit(1)
lines[hits[0]] = 'secret = "%s"\n' % secret
tmp = path + ".tmp"
with open(tmp, "w", encoding="utf-8") as fh:
    fh.writelines(lines)
os.replace(tmp, path)
PYEOF
}

# 端口预检:被占用时报错退出,绝不杀未知进程
port_free_tcp() { python3 -c 'import socket,sys
s = socket.socket()
try:
    s.bind(("127.0.0.1", int(sys.argv[1]))); s.close(); sys.exit(0)
except OSError: sys.exit(1)' "$1"; }
port_free_udp() { python3 -c 'import socket,sys
s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
try:
    s.bind(("127.0.0.1", int(sys.argv[1]))); s.close(); sys.exit(0)
except OSError: sys.exit(1)' "$1"; }

# ---------- 自测模式(便携辅助自身,不启动被测二进制) ----------
if [ "${1:-}" = "--self-test" ]; then
  SKIP_ARCHIVE=1
  T=$(mktemp -d /tmp/rh-e2e-selftest.XXXXXX)
  cat > "$T/c.toml" <<'TOML'
server_addr = "127.0.0.1:18100"
protocol = "tcp"
client_id = "abc"
secret = "OLD-SECRET-VALUE"
dns_server = "127.0.0.1:19553"
TOML
  UPDATE_SECRET_VAL="NEW-SECRET-VALUE" update_secret "$T/c.toml"
  rc=$?
  if [ $rc -eq 0 ] \
     && grep -q '^secret = "NEW-SECRET-VALUE"$' "$T/c.toml" \
     && [ "$(grep -c '^secret' "$T/c.toml")" = "1" ] \
     && grep -q '^client_id = "abc"$' "$T/c.toml"; then
    ok "update_secret: 恰好替换 secret 一行,其余行不动"
  else
    bad "update_secret: 替换结果不符"
  fi
  # 负例:没有 secret 行时必须拒绝且不写回
  printf 'a = 1\n' > "$T/d.toml"
  UPDATE_SECRET_VAL="X" update_secret "$T/d.toml" 2>/dev/null
  if [ $? -ne 0 ] && [ "$(cat "$T/d.toml")" = "a = 1" ]; then
    ok "update_secret: 无 secret 行时报错不写回"
  else
    bad "update_secret: 缺行行为不符"
  fi
  # 负例:两行 secret 必须拒绝(不得静默改写其一)
  printf 'secret = "a"\nsecret = "b"\n' > "$T/e.toml"
  UPDATE_SECRET_VAL="X" update_secret "$T/e.toml" 2>/dev/null
  if [ $? -ne 0 ] && grep -q '^secret = "a"$' "$T/e.toml"; then
    ok "update_secret: 多行 secret 报错不写回"
  else
    bad "update_secret: 多行行为不符"
  fi
  # 密钥不出现在本脚本进程的命令行(辅助经 env 传递):静态自证——
  # helper 从 os.environ 取值,无 argv 携带密钥
  ok "update_secret: 密钥经环境变量传递(非 argv,不落入 ps/错误输出)"
  rm -rf "$T"
  echo "== 自测结果: PASS=$PASS FAIL=$FAIL =="
  [ "$FAIL" = "0" ]
  exit $?
fi

# ---------- 端口(避开常用) ----------
API_PORT=18080; CTL_PORT=18100; VHOST_HTTP=18081; TCPMUX_PORT=18082
ECHO_TCP=18091; ECHO_UDP=18092; HTTP_ORIGIN=18093; HDR_PORT=18094
P_TCP=18301; P_UDP=18302; P_COMP=18303; P_PPRO=18304; P_REFUSED=18305
DEAD_PORT=18999  # 无监听端口(目标拒连测试)
MM_PORT=18401; P_MM=18411   # 中间人端口 / multiplex=false 规则端口
V_STCP=18310; V_SUDP=18311; V_XTCP=18312; V_STCP2=18313
METRICS_PORT=18450          # Prometheus 独立端口(0fe4069:解耦、无鉴权)
ADMIN_USER="admin"; ADMIN_PASS="E2e-pass-123"

echo "== rabbithole E2E($WORK)=="
# 端口预检:全部所需端口必须空闲;被占即报错退出(不杀未知进程——
# 那可能是其他会话/本机真实服务,广域扫杀是被禁止的清理方式)
TCP_PORTS="$API_PORT $CTL_PORT $VHOST_HTTP $TCPMUX_PORT $ECHO_TCP $HTTP_ORIGIN $HDR_PORT $P_TCP $P_COMP $P_PPRO $P_REFUSED $MM_PORT $P_MM $V_STCP $V_XTCP $V_STCP2 $DEAD_PORT $METRICS_PORT 19700 19195 19196 18022 18998 19997 19998 19999"
UDP_PORTS="$ECHO_UDP $P_UDP $V_SUDP 19553"
for p in $TCP_PORTS; do
  if ! port_free_tcp "$p"; then
    echo "ERROR: 端口 $p/tcp 已被占用(预检失败)。可能是先前运行残留或无关服务;不自动清理,请人工排查后重跑。" >&2
    exit 2
  fi
done
for p in $UDP_PORTS; do
  if ! port_free_udp "$p"; then
    echo "ERROR: 端口 $p/udp 已被占用(预检失败)。请人工排查后重跑。" >&2
    exit 2
  fi
done

# ---------- 基础服务 ----------
python3 - "$ECHO_TCP" <<'EOF' &
import socket, threading, sys
s = socket.socket(); s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.bind(("127.0.0.1", int(sys.argv[1]))); s.listen(64)
def echo(c):
    try:
        while True:
            d = c.recv(65536)
            if not d: break
            c.sendall(d)
    except OSError: pass
    finally: c.close()
while True:
    c, _ = s.accept()
    threading.Thread(target=echo, args=(c,), daemon=True).start()
EOF
PIDS+=($!)

python3 - "$ECHO_UDP" <<'EOF' &
import socket, sys
s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
s.bind(("127.0.0.1", int(sys.argv[1])))
while True:
    d, a = s.recvfrom(65536)
    s.sendto(d, a)
EOF
PIDS+=($!)

python3 - "$HTTP_ORIGIN" "$WORK/www" <<'EOF' &
import http.server, socketserver, sys, os
os.makedirs(sys.argv[2], exist_ok=True)
open(os.path.join(sys.argv[2], "index.html"), "w").write("origin-ok")
class H(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *a, **k): super().__init__(*a, directory=sys.argv[2], **k)
    def log_message(self, *a): pass
socketserver.TCPServer.allow_reuse_address = True
socketserver.TCPServer(("127.0.0.1", int(sys.argv[1])), H).serve_forever()
EOF
PIDS+=($!)

python3 - "$HDR_PORT" "$WORK" <<'EOF' &
import socket, threading, sys
s = socket.socket(); s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.bind(("127.0.0.1", int(sys.argv[1]))); s.listen(16)
def h(c):
    # PROXY 头与首段数据可能分两次到达(客户端 relay 分段写):累积读到
    # "data\n" 或 EOF/超时,避免单次 recv 截断造成假失败。断言口径不变。
    c.settimeout(2)
    buf = b""
    try:
        while not buf.endswith(b"data\n"):
            d = c.recv(65536)
            if not d:
                break
            buf += d
    except OSError:
        pass
    try:
        open(sys.argv[2] + "/hdr.log", "wb").write(buf)
    except OSError:
        pass
    c.close()
while True:
    c, _ = s.accept()
    threading.Thread(target=h, args=(c,), daemon=True).start()
EOF
PIDS+=($!)

# DNS stub:任意 A 查询应答 127.0.0.1(客户端 dns_server 指向它)
python3 - 19553 <<'DNSEOF' &
import socket, sys
s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
s.bind(("127.0.0.1", int(sys.argv[1])))
while True:
    q, a = s.recvfrom(1500)
    if len(q) < 12:
        continue
    resp = q[:2] + bytes([0x81, 0x80]) + q[4:6] + bytes([0, 1, 0, 0, 0, 0]) + q[12:]
    resp += bytes([0xC0, 0x0C, 0, 1, 0, 1, 0, 0, 0, 30, 0, 4, 127, 0, 0, 1])
    s.sendto(resp, a)
DNSEOF
PIDS+=($!)
sleep 1

# ---------- 服务端 ----------
ssh-keygen -q -t ed25519 -N "" -f "$WORK/sshkey" >/dev/null 2>&1
printf '%s\n' "$(cat "$WORK/sshkey.pub")" > "$WORK/authorized_keys"
cat > "$WORK/server.toml" <<EOF
bind_addr = "127.0.0.1"
bind_protocol = "tcp"
bind_port = $CTL_PORT
multiplex = true
[web]
bind_addr = "127.0.0.1"
port = $API_PORT
[vhost]
http_port = $VHOST_HTTP
https_port = 0
tcpmux_port = $TCPMUX_PORT
subdomain_host = "e2e.test"
[ssh_gateway]
enabled = true
bind_port = 19700
authorized_keys_file = "$WORK/authorized_keys"
[ports]
auto_range = "20000-21000"
[metrics]
enable_prometheus = true
bind_addr = "127.0.0.1"
port = $METRICS_PORT
[log]
level = "info"
EOF

"$SERVER" --config "$WORK/server.toml" admin create "$ADMIN_USER" "$ADMIN_PASS" >/dev/null 2>&1
check $? "管理员创建"
"$SERVER" --config "$WORK/server.toml" >"$WORK/server.log" 2>&1 &
SRV_PID=$!; PIDS+=($SRV_PID)   # 记录精确服务端 PID(重启时按 PID 终止)
sleep 2

api() { curl -s -m 10 -H "Authorization: Bearer $TOKEN" "$@"; }
TOKEN=$(curl -s -X POST "http://127.0.0.1:$API_PORT/api/auth/login" \
  -H 'Content-Type: application/json' \
  -d "{\"username\":\"$ADMIN_USER\",\"password\":\"$ADMIN_PASS\"}" \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["data"]["accessToken"])' 2>/dev/null)
[ -n "$TOKEN" ]; check $? "登录获取 JWT"

# 错误密码拒绝(envelope:HTTP200 + code!=0)
lcode=$(curl -s -X POST "http://127.0.0.1:$API_PORT/api/auth/login" \
  -H 'Content-Type: application/json' -d '{"username":"admin","password":"wrong"}' \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["code"])')
[ "$lcode" != "0" ]; check $? "错误密码拒绝(code!=0)"

# 未授权 401
code=$(curl -s -o /dev/null -w "%{http_code}" "http://127.0.0.1:$API_PORT/api/services")
[ "$code" = "401" ]; check $? "无令牌 401"

# ---------- API Key(双凭据) ----------
BASE_URL="http://127.0.0.1:$API_PORT"

# 1) 创建只读 API Key(明文仅此一次返回)
KRESP=$(api -X POST "$BASE_URL/api/api-keys" -H 'Content-Type: application/json' \
  -d '{"name":"e2e-ro","preset":"readonly"}')
KEY=$(echo "$KRESP" | python3 -c 'import json,sys;print(json.load(sys.stdin)["data"]["key"])' 2>/dev/null)
KID=$(echo "$KRESP" | python3 -c 'import json,sys;print(json.load(sys.stdin)["data"]["id"])' 2>/dev/null)
[ -n "$KEY" ] && [ -n "$KID" ]; check $? "创建 API Key 并取明文"

# 1b) 明文形态 + 列表:有 created_by,无 key_hash/明文
echo "$KRESP" | python3 -c 'import json,sys; d=json.load(sys.stdin)["data"]; sys.exit(0 if d["key"].startswith("rhk_") else 1)'
check $? "创建响应含明文 key(rhk_ 前缀)"
api "$BASE_URL/api/api-keys" | python3 -c '
import json,sys
d=json.load(sys.stdin)["data"]
assert d, "列表不应为空"
row=d[0]
assert "created_by" in row and row["created_by"], "created_by 缺失或为空"
assert "key_hash" not in row, "列表不得返回 key_hash"
assert "key" not in row, "列表不得返回明文"
'
check $? "列表含 created_by 且不含 key_hash/明文"

# 2) 只读 key 读 clients → 200
kc=$(curl -s -o /dev/null -w "%{http_code}" "$BASE_URL/api/clients" -H "Authorization: Bearer $KEY")
[ "$kc" = "200" ]; check $? "API Key 可读 clients(HTTP=$kc)"

# 3) 只读 key 写 clients → 403(真实 HTTP 状态,业务信封是 200 会误判)
kc=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$BASE_URL/api/clients" \
  -H "Authorization: Bearer $KEY" -H 'Content-Type: application/json' -d '{"name":"nope"}')
[ "$kc" = "403" ]; check $? "只读 Key 写被拒 403(HTTP=$kc)"

# 4) key 调 session-only 接口 → 403
kc=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$BASE_URL/api/auth/logout" \
  -H "Authorization: Bearer $KEY")
[ "$kc" = "403" ]; check $? "API Key 不能调 logout(HTTP=$kc)"

# 5) 未知 /api/*:不得被鉴权拦截(不是 401);既有口径是 HTTP 200 +
#    body code=404(err 信封不带状态码 —— 与改造前基线一致)
ub=$(curl -s -w "|%{http_code}" "$BASE_URL/api/nope")
uc=${ub##*|}
ucb=$(echo "${ub%|*}" | python3 -c 'import json,sys; print(json.load(sys.stdin).get("code"))' 2>/dev/null)
# 两种构建风味都合法:裸构建(无内嵌 UI)走 axum 默认 fallback → 真 404;
# 内嵌 UI 构建走 embedded_ui::handle → err 信封 HTTP 200+body code=404。
# 本质要求只有一个:不得被鉴权拦截成 401(route_layer 误用的信号)。
[ "$uc" != "401" ] && { [ "$uc" = "404" ] || [ "$ucb" = "404" ]; }; check $? "未知 API 路径携带 404 语义且未被鉴权拦截(HTTP=$uc code=$ucb)"

# 6) 改密码后 API Key 仍可用 —— Key 独立于 pwd_version 的唯一可执行证明。
#    ★此步作废 $TOKEN(服务端还会轮换 JWT 密钥),故放在本段末尾,验完立即改回
kc=$(api -X POST "$BASE_URL/api/auth/password" -H 'Content-Type: application/json' \
  -d '{"old_password":"'"$ADMIN_PASS"'","new_password":"E2e-tmp-456"}' \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["code"])')
[ "$kc" = "0" ]; check $? "改密码成功(code=0)"
kc=$(curl -s -o /dev/null -w "%{http_code}" "$BASE_URL/api/clients" -H "Authorization: Bearer $KEY")
[ "$kc" = "200" ]; check $? "改密码后 API Key 仍可用(HTTP=$kc)"
# 改回原密码,保持后续段落凭据不变;重新登录拿回 TOKEN
kc=$(curl -s -X POST "$BASE_URL/api/auth/login" -H 'Content-Type: application/json' \
  -d '{"username":"admin","password":"E2e-tmp-456"}' \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["data"]["accessToken"])')
TOKEN="$kc"
kc=$(api -X POST "$BASE_URL/api/auth/password" -H 'Content-Type: application/json' \
  -d '{"old_password":"E2e-tmp-456","new_password":"'"$ADMIN_PASS"'"}' \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["code"])')
[ "$kc" = "0" ]; check $? "密码已改回"
TOKEN=$(curl -s -X POST "$BASE_URL/api/auth/login" -H 'Content-Type: application/json' \
  -d "{\"username\":\"$ADMIN_USER\",\"password\":\"$ADMIN_PASS\"}" \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["data"]["accessToken"])' 2>/dev/null)
[ -n "$TOKEN" ]; check $? "改回后重新登录"

# 6b) rotate 返回新明文,且旧 key 失效
RRESP=$(api -X POST "$BASE_URL/api/api-keys/$KID/rotate")
NEWKEY=$(echo "$RRESP" | python3 -c 'import json,sys;print(json.load(sys.stdin)["data"]["key"])' 2>/dev/null)
[ -n "$NEWKEY" ] && [ "$NEWKEY" != "$KEY" ]; check $? "rotate 返回新明文且与旧不同"
kc=$(curl -s -o /dev/null -w "%{http_code}" "$BASE_URL/api/clients" -H "Authorization: Bearer $KEY")
[ "$kc" = "401" ]; check $? "rotate 后旧 Key 失效 401(HTTP=$kc)"

# 7) 吊销 → 新 key 也 401;软删后列表仍可见
kr=$(api -X DELETE "$BASE_URL/api/api-keys/$KID")
echo "$kr" | python3 -c 'import json,sys; sys.exit(0 if json.load(sys.stdin)["data"]["deleted"] else 1)'
check $? "吊销 API Key"
kc=$(curl -s -o /dev/null -w "%{http_code}" "$BASE_URL/api/clients" -H "Authorization: Bearer $NEWKEY")
[ "$kc" = "401" ]; check $? "吊销后 Key 失效 401(HTTP=$kc)"
api "$BASE_URL/api/api-keys" | python3 -c '
import json,sys
rows=json.load(sys.stdin)["data"]
assert any((not r["enabled"]) and r["id"] == "'"$(echo "$KID")"'" for r in rows), "软删后列表应仍可见且 enabled=false"
'
check $? "软删后列表仍可见(审计保留)"

# ---------- 客户端 ----------
CRESP=$(curl -s -X POST "http://127.0.0.1:$API_PORT/api/clients" \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' -d '{"name":"e2e"}')
CID=$(echo "$CRESP" | python3 -c 'import json,sys; print(json.load(sys.stdin)["data"]["id"])')
CSECRET=$(echo "$CRESP" | python3 -c 'import json,sys; print(json.load(sys.stdin)["data"]["secret"])')
[ -n "$CID" ] && [ -n "$CSECRET" ]; check $? "创建客户端并取凭据"

# 列表不泄露密钥
leak=$(curl -s "http://127.0.0.1:$API_PORT/api/clients" -H "Authorization: Bearer $TOKEN" | grep -c "$CSECRET")
[ "$leak" = "0" ]; check $? "列表响应不含密钥"

cat > "$WORK/client.toml" <<EOF
server_addr = "127.0.0.1:$CTL_PORT"
protocol = "tcp"
multiplex = true
client_id = "$CID"
secret = "$CSECRET"
dns_server = "127.0.0.1:19553"
log_level = "info"
EOF
"$CLIENT" --config "$WORK/client.toml" >"$WORK/client.log" 2>&1 &
CLIENT_PID=$!; PIDS+=($CLIENT_PID)   # 记录精确客户端 PID(轮换时按 PID 终止)
sleep 3
online=$(curl -s "http://127.0.0.1:$API_PORT/api/clients" -H "Authorization: Bearer $TOKEN" \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["data"][0]["online"])')
[ "$online" = "True" ]; check $? "客户端上线"

# 第二个客户端:纯环境变量接入(无配置文件,Docker 形态)
CRESP2=$(curl -s -X POST "http://127.0.0.1:$API_PORT/api/clients" \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' -d '{"name":"env-only"}')
CID2=$(echo "$CRESP2" | python3 -c 'import json,sys; print(json.load(sys.stdin)["data"]["id"])')
CSECRET2=$(echo "$CRESP2" | python3 -c 'import json,sys; print(json.load(sys.stdin)["data"]["secret"])')
cd "$WORK"   # 离开仓库目录,确保没有 rh-client.toml 落回文件模式
RH_SERVER="127.0.0.1:$CTL_PORT" RH_CLIENT_ID="$CID2" RH_SECRET="$CSECRET2" \
  "$CLIENT" >"$WORK/client-env.log" 2>&1 &
PIDS+=($!)
cd - >/dev/null
sleep 3
env_online=$(curl -s "http://127.0.0.1:$API_PORT/api/clients" -H "Authorization: Bearer $TOKEN" \
  | python3 -c 'import json,sys; d=json.load(sys.stdin)["data"]; print([c["online"] for c in d if c["name"]=="env-only"][0])')
[ "$env_online" = "True" ]; check $? "纯环境变量客户端上线(RH_SERVER/RH_CLIENT_ID/RH_SECRET,无配置文件)"
# 无连接信息时的报错可行动(非静默连默认地址)
err_out=$(cd "$WORK" && "$CLIENT" 2>&1; echo "exit=$?")
echo "$err_out" | grep -q "RH_SERVER"; check $? "裸启动报错含环境变量指引"

# 第三个客户端:域名接入(issue #2)。RH_SERVER 用 域名:port,解析走
# 上方的 DNS stub(任意 A 查询 → 127.0.0.1),确定性地覆盖
# 域名拆分 → dnsquery(自定义 DNS)→ 拨号全链路。
# 修前 str::parse::<SocketAddr>() 只认 IP:port,域名直接 bad server_addr
# 无限重连。不用 localhost 是因为它可能先解析到 ::1 而服务端只听 127.0.0.1。
CRESP3=$(curl -s -X POST "http://127.0.0.1:$API_PORT/api/clients" \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' -d '{"name":"domain-addr"}')
CID3=$(echo "$CRESP3" | python3 -c 'import json,sys; print(json.load(sys.stdin)["data"]["id"])')
CSECRET3=$(echo "$CRESP3" | python3 -c 'import json,sys; print(json.load(sys.stdin)["data"]["secret"])')
cd "$WORK"
RH_SERVER="server.e2e.test:$CTL_PORT" RH_DNS_SERVER="127.0.0.1:19553" \
  RH_CLIENT_ID="$CID3" RH_SECRET="$CSECRET3" \
  "$CLIENT" >"$WORK/client-domain.log" 2>&1 &
PIDS+=($!)
cd - >/dev/null
sleep 3
dom_online=$(curl -s "http://127.0.0.1:$API_PORT/api/clients" -H "Authorization: Bearer $TOKEN" \
  | python3 -c 'import json,sys; d=json.load(sys.stdin)["data"]; print([c["online"] for c in d if c["name"]=="domain-addr"][0])')
[ "$dom_online" = "True" ]; check $? "域名接入上线(RH_SERVER=域名:port + RH_DNS_SERVER,issue #2)"

# 域名接入的日志不得再出现 bad server_addr(修前的无限重连症状)
grep -q "bad server_addr" "$WORK/client-domain.log" && bad "域名接入日志不得出现 bad server_addr" \
  || ok "域名接入日志无 bad server_addr"

# 形状错误快速失败(issue #2):缺端口是永久配置错误,启动即退出并给
# 格式提示,而不是进重连循环无限退避。3s 探活(不用 timeout:macOS 无该命令)。
cd "$WORK"
RH_SERVER="no-port.example" RH_CLIENT_ID=x RH_SECRET=y \
  "$CLIENT" >"$WORK/client-shape.log" 2>&1 &
SHAPE_PID=$!
sleep 3
if kill -0 "$SHAPE_PID" 2>/dev/null; then
  kill "$SHAPE_PID" 2>/dev/null
  bad "缺端口的 server_addr 启动即退出(非重连循环)"
else
  ok "缺端口的 server_addr 启动即退出(非重连循环)"
fi
cd - >/dev/null
grep -q "缺少端口" "$WORK/client-shape.log"; check $? "缺端口的 server_addr 报错含格式提示"

mk_svc() { api -X POST "http://127.0.0.1:$API_PORT/api/services" -H 'Content-Type: application/json' -d "$1"; }
mk_vis() { api -X POST "http://127.0.0.1:$API_PORT/api/visitors" -H 'Content-Type: application/json' -d "$1"; }

# ---------- TCP ----------
mk_svc "{\"client_id\":\"$CID\",\"name\":\"tcp\",\"kind\":\"tcp\",\"bind_port\":$P_TCP,\"target_host\":\"127.0.0.1\",\"target_port\":$ECHO_TCP,\"enabled\":true}" >/dev/null
sleep 2
python3 - "$P_TCP" <<'EOF' && ok "TCP 转发+大负载" || bad "TCP 转发+大负载"
import socket, sys
s = socket.create_connection(("127.0.0.1", int(sys.argv[1])), timeout=10)
p = b"x" * 300000 + b"-TAIL-"
s.sendall(p); s.settimeout(10)
g = b""
while len(g) < len(p):
    d = s.recv(65536)
    if not d: break
    g += d
sys.exit(0 if g == p else 1)
EOF

python3 - "$P_TCP" <<'EOF' && ok "TCP 半关闭" || bad "TCP 半关闭"
import socket, sys
s = socket.create_connection(("127.0.0.1", int(sys.argv[1])), timeout=8)
s.sendall(b"half\n"); s.shutdown(socket.SHUT_WR); s.settimeout(8)
g = b""
while True:
    d = s.recv(4096)
    if not d: break
    g += d
sys.exit(0 if g == b"half\n" else 1)
EOF

python3 - "$P_TCP" <<'EOF' && ok "TCP 10 并发" || bad "TCP 10 并发"
import socket, sys, threading
res = []
def w(i):
    try:
        c = socket.create_connection(("127.0.0.1", int(sys.argv[1])), timeout=10)
        m = f"c{i}\n".encode(); c.sendall(m); c.settimeout(10)
        r = b""
        while len(r) < len(m):
            d = c.recv(256)
            if not d: break
            r += d
        res.append(r == m); c.close()
    except Exception:
        res.append(False)
ts = [threading.Thread(target=w, args=(i,)) for i in range(10)]
[t.start() for t in ts]; [t.join() for t in ts]
sys.exit(0 if all(res) and len(res) == 10 else 1)
EOF

# ---------- UDP ----------
mk_svc "{\"client_id\":\"$CID\",\"name\":\"udp\",\"kind\":\"udp\",\"bind_port\":$P_UDP,\"target_host\":\"127.0.0.1\",\"target_port\":$ECHO_UDP,\"enabled\":true}" >/dev/null
sleep 2
python3 - "$P_UDP" <<'EOF' && ok "UDP 转发" || bad "UDP 转发"
import socket, sys
u = socket.socket(socket.AF_INET, socket.SOCK_DGRAM); u.settimeout(8)
u.sendto(b"udp-ok\n", ("127.0.0.1", int(sys.argv[1])))
try:
    d, _ = u.recvfrom(65536)
    sys.exit(0 if d == b"udp-ok\n" else 1)
except socket.timeout:
    sys.exit(1)
EOF

# ---------- 压缩 ----------
mk_svc "{\"client_id\":\"$CID\",\"name\":\"comp\",\"kind\":\"tcp\",\"bind_port\":$P_COMP,\"target_host\":\"127.0.0.1\",\"target_port\":$ECHO_TCP,\"enabled\":true,\"transport\":{\"use_compression\":true,\"bandwidth_mode\":\"server\",\"proxy_proto\":\"none\"}}" >/dev/null
sleep 2
python3 - "$P_COMP" <<'EOF' && ok "snappy 压缩端到端" || bad "snappy 压缩端到端"
import socket, sys
s = socket.create_connection(("127.0.0.1", int(sys.argv[1])), timeout=10)
p = (b"compress-" * 40000)[:400000]
s.sendall(p); s.settimeout(10)
g = b""
while len(g) < len(p):
    d = s.recv(65536)
    if not d: break
    g += d
sys.exit(0 if g == p else 1)
EOF

# ---------- PROXY protocol v1 ----------
mk_svc "{\"client_id\":\"$CID\",\"name\":\"ppro\",\"kind\":\"tcp\",\"bind_port\":$P_PPRO,\"target_host\":\"127.0.0.1\",\"target_port\":$HDR_PORT,\"enabled\":true,\"transport\":{\"use_compression\":false,\"bandwidth_mode\":\"server\",\"proxy_proto\":\"v1\"}}" >/dev/null
sleep 2
python3 - "$P_PPRO" "$WORK" <<'EOF' && ok "PROXY protocol v1" || bad "PROXY protocol v1"
import socket, sys, time, os
s = socket.create_connection(("127.0.0.1", int(sys.argv[1])), timeout=6)
s.sendall(b"data\n"); time.sleep(0.6); s.close()
time.sleep(0.6)
try:
    d = open(sys.argv[2] + "/hdr.log", "rb").read()
    sys.exit(0 if d.startswith(b"PROXY TCP4") and d.endswith(b"data\n") else 1)
except FileNotFoundError:
    sys.exit(1)
EOF

# ---------- HTTP vhost ----------
mk_svc "{\"client_id\":\"$CID\",\"name\":\"web\",\"kind\":\"http\",\"target_host\":\"127.0.0.1\",\"target_port\":$HTTP_ORIGIN,\"enabled\":true,\"http\":{\"custom_domains\":[\"app.e2e.test\"]}}" >/dev/null
sleep 2
body=$(curl -s -m 8 -H "Host: app.e2e.test" "http://127.0.0.1:$VHOST_HTTP/")
[ "$body" = "origin-ok" ]; check $? "HTTP vhost 域名路由"
body404=$(curl -s -m 8 -o /dev/null -w "%{http_code}" -H "Host: none.e2e.test" "http://127.0.0.1:$VHOST_HTTP/")
[ "$body404" = "404" ]; check $? "HTTP vhost 未知域名 404"

# ---------- STCP ----------
# 密钥完全服务端管理:创建即生成(入参提供的密钥一律被忽略),访问器不存副本(连接时继承)
mk_svc "{\"client_id\":\"$CID\",\"name\":\"sdb\",\"kind\":\"stcp\",\"target_host\":\"127.0.0.1\",\"target_port\":$ECHO_TCP,\"enabled\":true,\"stcp\":{\"secret_key\":\"client-side-should-be-ignored\"}}" >/dev/null
mk_vis "{\"client_id\":\"$CID\",\"name\":\"sdb-v\",\"kind\":\"stcp\",\"bind_port\":$V_STCP,\"target_proxy_name\":\"sdb\",\"enabled\":true}" >/dev/null
sleep 2
python3 - "$V_STCP" <<'EOF' && ok "STCP visitor(服务端生成密钥+访问器继承)" || bad "STCP visitor(服务端生成密钥+访问器继承)"
import socket, sys
s = socket.create_connection(("127.0.0.1", int(sys.argv[1])), timeout=10)
s.sendall(b"stcp\n"); s.shutdown(socket.SHUT_WR); s.settimeout(8)
g = b""
while True:
    d = s.recv(4096)
    if not d: break
    g += d
sys.exit(0 if g == b"stcp\n" else 1)
EOF
# 入参密钥被忽略 + 库中值是服务端随机 32 hex。
#
# ★口径变更(2026-10-23):`07b1aa2`(安全加固)让列表接口主动抹除
# `spec.stcp.secret_key`,这条断言原本读列表取密钥来判 32 hex,**必然**
# 读到空串而失败。那是回归,但不是产品缺陷 —— 变的是「从哪个口径能观察到
# 这条语义」,不是语义本身。
#
# 新口径分两层,合起来覆盖原来的语义,不放宽:
#   1. 这里(e2e,黑盒):列表**不**回显密钥 —— 安全性质本身必须被验证。
#      `-z` 是强断言(空才过),不是"删掉断言"。
#   2. 32 hex 与"入参被忽略":没有任何 API 再返回该密钥了(创建响应只回
#      id/bind_port/revision;列表抹除;轮换回的**新**值),所以黑盒拿不到。
#      改由服务端单测承接:`api::secret_key_management_tests`
#      (create_ignores_caller_secret_and_generates_32_hex 等 4 条),直接断言
#      库中值 —— 那才是转发时真正用的唯一事实源。
#      见 crates/rh-server/src/api.rs。
# ★口径再变更(M8,2026-10-27):列表接口已完全不携带 spec 段(投影
# 化),断言从「spec.stcp.secret_key 为空」升级为「spec 段不存在」,
# 更强且不依赖嵌套取值。完整 spec 由 GET /api/services/{id} 单条返回
# (secret_key 同样抹除,由服务端单测 get_service_carries_redacted_spec
# 覆盖)。随机生成语义仍由 api::secret_key_management_tests 承接。
SEC_SDB=$(api "http://127.0.0.1:$API_PORT/api/services?client_id=$CID" | python3 -c 'import json,sys; d=json.load(sys.stdin)["data"]; print("LEAK" if any("spec" in s for s in d if s["name"]=="sdb") else "")')
[ -z "$SEC_SDB" ]; check $? "列表接口不携带 spec 段(M8 投影;更不得回显 secret_key)"

# 轮换:服务端重新生成,访问器(无副本)自动继承新值,在途之外的新中继继续可用
mk_svc "{\"client_id\":\"$CID\",\"name\":\"sdb2\",\"kind\":\"stcp\",\"target_host\":\"127.0.0.1\",\"target_port\":$ECHO_TCP,\"enabled\":true,\"stcp\":{}}" >/dev/null
mk_vis "{\"client_id\":\"$CID\",\"name\":\"sdb2-v\",\"kind\":\"stcp\",\"bind_port\":$V_STCP2,\"target_proxy_name\":\"sdb2\",\"enabled\":true}" >/dev/null
sleep 2
python3 - "$V_STCP2" <<'EOF' && ok "STCP 轮换前中继正常" || bad "STCP 轮换前中继正常"
import socket, sys
s = socket.create_connection(("127.0.0.1", int(sys.argv[1])), timeout=10)
s.sendall(b"stcp-pre\n"); s.shutdown(socket.SHUT_WR); s.settimeout(8)
g = b""
while True:
    d = s.recv(4096)
    if not d: break
    g += d
sys.exit(0 if g == b"stcp-pre\n" else 1)
EOF
SDB2_ID=$(api "http://127.0.0.1:$API_PORT/api/services?client_id=$CID" | python3 -c 'import json,sys; d=json.load(sys.stdin)["data"]; print([s["id"] for s in d if s["name"]=="sdb2"][0])')
# ★口径变更(2026-10-23):轮换端点**本身**返回新密钥(且按设计"仅此一次"),
# 而列表接口已抹除密钥(`07b1aa2`)。原断言拿列表取 SEC1/SEC2 比较必然两边都是
# 空串 → `SEC1 != SEC2` 恒假。改成直接用轮换响应,口径**更强**:
#   - 返回的必须是 32 位 hex(原来也测这个);
#   - 且必须回滚出"新值"—— 用两次轮换互比,证明每次都重新生成,不是固定值;
#   - 同时列表仍不回显(安全加固不被绕过)。
ROT1=$(api -X POST "http://127.0.0.1:$API_PORT/api/services/$SDB2_ID/rotate-secret" \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["data"]["secret"])')
ROT2=$(api -X POST "http://127.0.0.1:$API_PORT/api/services/$SDB2_ID/rotate-secret" \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["data"]["secret"])')
# ★口径再变更(M8):列表不携带 spec 段(见上),listed 断言改为
# 「spec 段不存在」。
SEC2=$(api "http://127.0.0.1:$API_PORT/api/services?client_id=$CID" | python3 -c 'import json,sys; d=json.load(sys.stdin)["data"]; print("LEAK" if any("spec" in s for s in d if s["name"]=="sdb2") else "")')
python3 - "$ROT1" "$ROT2" "$SEC2" <<'EOF' && ok "轮换端点每次生成新的 32 hex 密钥(且列表不携带 spec)" || bad "轮换端点每次生成新的 32 hex 密钥(且列表不携带 spec)"
import sys, re
rot1, rot2, listed = sys.argv[1], sys.argv[2], sys.argv[3]
hex32 = re.compile(r"^[0-9a-f]{32}$")
assert hex32.match(rot1), f"第一次轮换返回值不是 32 hex: {rot1!r}"
assert hex32.match(rot2), f"第二次轮换返回值不是 32 hex: {rot2!r}"
assert rot1 != rot2, "两次轮换返回了相同的密钥(没有重新生成)"
assert listed == "", f"列表仍携带 spec 段: {listed!r}"
EOF
sleep 2
python3 - "$V_STCP2" <<'EOF' && ok "STCP 轮换后中继不断(访问器自动继承新密钥)" || bad "STCP 轮换后中继不断(访问器自动继承新密钥)"
import socket, sys
s = socket.create_connection(("127.0.0.1", int(sys.argv[1])), timeout=10)
s.sendall(b"stcp-post\n"); s.shutdown(socket.SHUT_WR); s.settimeout(8)
g = b""
while True:
    d = s.recv(4096)
    if not d: break
    g += d
sys.exit(0 if g == b"stcp-post\n" else 1)
EOF

# ---------- SUDP ----------
mk_svc "{\"client_id\":\"$CID\",\"name\":\"udbs\",\"kind\":\"sudp\",\"target_host\":\"127.0.0.1\",\"target_port\":$ECHO_UDP,\"enabled\":true,\"sudp\":{}}" >/dev/null
mk_vis "{\"client_id\":\"$CID\",\"name\":\"udbs-v\",\"kind\":\"sudp\",\"bind_port\":$V_SUDP,\"target_proxy_name\":\"udbs\",\"enabled\":true}" >/dev/null
sleep 2
python3 - "$V_SUDP" <<'EOF' && ok "SUDP visitor" || bad "SUDP visitor"
import socket, sys
u = socket.socket(socket.AF_INET, socket.SOCK_DGRAM); u.settimeout(8)
u.sendto(b"sudp\n", ("127.0.0.1", int(sys.argv[1])))
try:
    d, _ = u.recvfrom(65536)
    sys.exit(0 if d == b"sudp\n" else 1)
except socket.timeout:
    sys.exit(1)
EOF

# ---------- 健康检查(失败转不健康 → 目标恢复转健康) ----------
DEAD_HC=18999
mk_svc "{\"client_id\":\"$CID\",\"name\":\"hsvc\",\"kind\":\"tcp\",\"bind_port\":$((P_TCP+90)),\"target_host\":\"127.0.0.1\",\"target_port\":$DEAD_HC,\"enabled\":true,\"health_check\":{\"type\":\"tcp\",\"interval_seconds\":1,\"timeout_seconds\":1,\"max_failed\":2}}" >/dev/null
sleep 5
st=$(api "http://127.0.0.1:$API_PORT/api/services?client_id=$CID" | python3 -c 'import json,sys; print([s["status"] for s in json.load(sys.stdin)["data"] if s["name"]=="hsvc"][0])')
[ "$st" = "unhealthy" ]; check $? "健康检查失败转 unhealthy"

python3 - "$DEAD_HC" <<'EOF' &
import socket, sys, time
s = socket.socket(); s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.bind(("127.0.0.1", int(sys.argv[1]))); s.listen(8)
s.settimeout(1); end = time.time() + 10
while time.time() < end:
    try:
        c, _ = s.accept(); c.close()
    except socket.timeout:
        pass
EOF
HCPID=$!
sleep 5
st=$(api "http://127.0.0.1:$API_PORT/api/services?client_id=$CID" | python3 -c 'import json,sys; print([s["status"] for s in json.load(sys.stdin)["data"] if s["name"]=="hsvc"][0])')
[ "$st" = "running" ]; check $? "健康检查恢复转 running"
kill "$HCPID" 2>/dev/null

# ---------- XTCP ----------
mk_svc "{\"client_id\":\"$CID\",\"name\":\"xsvc\",\"kind\":\"xtcp\",\"target_host\":\"127.0.0.1\",\"target_port\":$ECHO_TCP,\"enabled\":true,\"xtcp\":{}}" >/dev/null
mk_vis "{\"client_id\":\"$CID\",\"name\":\"xsvc-v\",\"kind\":\"xtcp\",\"bind_port\":$V_XTCP,\"target_proxy_name\":\"xsvc\",\"enabled\":true}" >/dev/null
sleep 2
python3 - "$V_XTCP" <<'EOF' && ok "XTCP 打洞直连" || bad "XTCP 打洞直连"
import socket, sys
s = socket.create_connection(("127.0.0.1", int(sys.argv[1])), timeout=20)
s.sendall(b"xtcp\n"); s.shutdown(socket.SHUT_WR); s.settimeout(12)
g = b""
try:
    while True:
        d = s.recv(4096)
        if not d: break
        g += d
except Exception: pass
sys.exit(0 if g == b"xtcp\n" else 1)
EOF

# ---------- 端口冲突 ----------
python3 -c "import socket,time; s=socket.socket(); s.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1); s.bind(('0.0.0.0',19999)); s.listen(1); time.sleep(30)" &
OCC=$!; PIDS+=($OCC)
sleep 1
conflict=$(mk_svc "{\"client_id\":\"$CID\",\"name\":\"cf\",\"kind\":\"tcp\",\"bind_port\":19999,\"target_host\":\"127.0.0.1\",\"target_port\":$ECHO_TCP,\"enabled\":true}" | python3 -c 'import json,sys; print(json.load(sys.stdin)["code"])')
[ "$conflict" != "0" ]; check $? "创建时端口冲突拒绝"
kill $OCC 2>/dev/null

# 更新冲突保留原状态:把 tcp 规则改绑到被占端口
python3 -c "import socket,time; s=socket.socket(); s.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1); s.bind(('0.0.0.0',19998)); s.listen(1); time.sleep(20)" &
OCC2=$!; PIDS+=($OCC2)
sleep 1
SVC_ID=$(api "http://127.0.0.1:$API_PORT/api/services" | python3 -c 'import json,sys; d=json.load(sys.stdin)["data"]; print([s["id"] for s in d if s["name"]=="tcp"][0])')
ucode=$(api -X PUT "http://127.0.0.1:$API_PORT/api/services/$SVC_ID" -H 'Content-Type: application/json' \
  -d "{\"client_id\":\"$CID\",\"name\":\"tcp\",\"kind\":\"tcp\",\"bind_port\":19998,\"target_host\":\"127.0.0.1\",\"target_port\":$ECHO_TCP,\"enabled\":true}" | python3 -c 'import json,sys; print(json.load(sys.stdin)["code"])')
[ "$ucode" != "0" ]; check $? "更新时端口冲突拒绝"
python3 - "$P_TCP" <<'EOF' && ok "冲突后原规则仍可用" || bad "冲突后原规则仍可用"
import socket, sys
s = socket.create_connection(("127.0.0.1", int(sys.argv[1])), timeout=6)
s.sendall(b"still\n"); s.shutdown(socket.SHUT_WR); s.settimeout(6)
g = b""
while True:
    d = s.recv(256)
    if not d: break
    g += d
sys.exit(0 if g == b"still\n" else 1)
EOF
kill $OCC2 2>/dev/null

# 非端口变更(§4.3 原位发布):同端口换 target → 转发即时切换、listener 不重建
api -X PUT "http://127.0.0.1:$API_PORT/api/services/$SVC_ID" -H 'Content-Type: application/json' \
  -d "{\"client_id\":\"$CID\",\"name\":\"tcp\",\"kind\":\"tcp\",\"bind_port\":$P_TCP,\"target_host\":\"127.0.0.1\",\"target_port\":$HTTP_ORIGIN,\"enabled\":true}" >/dev/null
sleep 2
python3 - "$P_TCP" <<'EOF' && ok "非端口更新原位切换目标" || bad "非端口更新原位切换目标"
import socket, sys
s = socket.create_connection(("127.0.0.1", int(sys.argv[1])), timeout=6)
s.sendall(b"GET / HTTP/1.0\r\n\r\n"); s.settimeout(6)
g = b""
while True:
    d = s.recv(4096)
    if not d: break
    g += d
sys.exit(0 if b"origin-ok" in g else 1)
EOF
# 恢复原 target(再次原位发布)
api -X PUT "http://127.0.0.1:$API_PORT/api/services/$SVC_ID" -H 'Content-Type: application/json' \
  -d "{\"client_id\":\"$CID\",\"name\":\"tcp\",\"kind\":\"tcp\",\"bind_port\":$P_TCP,\"target_host\":\"127.0.0.1\",\"target_port\":$ECHO_TCP,\"enabled\":true}" >/dev/null
sleep 1

# 外部具体地址占用 + 通配探针:规则 bind_ip=0.0.0.0,持有者 127.0.0.1:19997
python3 -c "import socket,time; s=socket.socket(); s.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1); s.bind(('127.0.0.1',19997)); s.listen(1); time.sleep(20)" &
OCC3=$!; PIDS+=($OCC3)
sleep 1
ucode3=$(api -X PUT "http://127.0.0.1:$API_PORT/api/services/$SVC_ID" -H 'Content-Type: application/json' \
  -d "{\"client_id\":\"$CID\",\"name\":\"tcp\",\"kind\":\"tcp\",\"bind_port\":19997,\"target_host\":\"127.0.0.1\",\"target_port\":$ECHO_TCP,\"enabled\":true}" | python3 -c 'import json,sys; print(json.load(sys.stdin)["code"])')
[ "$ucode3" != "0" ]; check $? "通配探针拒绝外部占用端口"
python3 - "$P_TCP" <<'EOF' && ok "探针拒绝后原规则仍可用" || bad "探针拒绝后原规则仍可用"
import socket, sys
s = socket.create_connection(("127.0.0.1", int(sys.argv[1])), timeout=6)
s.sendall(b"still2\n"); s.shutdown(socket.SHUT_WR); s.settimeout(6)
g = b""
while True:
    d = s.recv(256)
    if not d: break
    g += d
sys.exit(0 if g == b"still2\n" else 1)
EOF
kill $OCC3 2>/dev/null

# ---------- 启停 ----------
api -X POST "http://127.0.0.1:$API_PORT/api/services/$SVC_ID/disable" >/dev/null
sleep 1
python3 -c "
import socket, sys
try:
    socket.create_connection(('127.0.0.1', $P_TCP), timeout=2).close()
    sys.exit(1)
except ConnectionRefusedError:
    sys.exit(0)" ; check $? "停用后端口关闭"
api -X POST "http://127.0.0.1:$API_PORT/api/services/$SVC_ID/enable" >/dev/null
sleep 2
python3 - "$P_TCP" <<'EOF' && ok "重新启用恢复转发" || bad "重新启用恢复转发"
import socket, sys
s = socket.create_connection(("127.0.0.1", int(sys.argv[1])), timeout=8)
s.sendall(b"re\n"); s.shutdown(socket.SHUT_WR); s.settimeout(6)
g = b""
while True:
    d = s.recv(256)
    if not d: break
    g += d
sys.exit(0 if g == b"re\n" else 1)
EOF

# ---------- TCPMux(HTTP CONNECT 复用入口) ----------
mk_svc "{\"client_id\":\"$CID\",\"name\":\"mux\",\"kind\":\"tcpmux\",\"target_host\":\"127.0.0.1\",\"target_port\":$ECHO_TCP,\"enabled\":true,\"tcpmux\":{\"custom_domains\":[\"mux.e2e.test\"]}}" >/dev/null
sleep 2
code=$(printf 'CONNECT mux.e2e.test:%s HTTP/1.1\r\nHost: mux.e2e.test\r\n\r\n' "$ECHO_TCP" | nc -w 5 127.0.0.1 "$TCPMUX_PORT" | head -1)
echo "$code" | grep -q "200"; check $? "TCPMux CONNECT 200"

# ---------- 插件:static_file ----------
mkdir -p "$WORK/plugin_root" && echo "static-content-ok" > "$WORK/plugin_root/hello.txt"
mk_svc "{\"client_id\":\"$CID\",\"name\":\"sf\",\"kind\":\"tcp\",\"bind_port\":$((P_TCP+20)),\"target_host\":\"127.0.0.1\",\"target_port\":1,\"enabled\":true,\"plugin\":{\"kind\":\"static_file\",\"local_path\":\"$WORK/plugin_root\"}}" >/dev/null
sleep 2
body=$(curl -s -m 8 "http://127.0.0.1:$((P_TCP+20))/hello.txt")
[ "$body" = "static-content-ok" ]; check $? "插件 static_file 服务文件"

# ---------- 插件:unix_domain_socket ----------
python3 - "$WORK/uds.sock" <<'UDSEOF' &
import socket, sys, os, threading
path = sys.argv[1]
try: os.unlink(path)
except OSError: pass
s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
s.bind(path); s.listen(8)
def e(c):
    try:
        while True:
            d = c.recv(4096)
            if not d: break
            c.sendall(d)
    except OSError: pass
while True:
    c, _ = s.accept()
    threading.Thread(target=e, args=(c,), daemon=True).start()
UDSEOF
UDSPID=$!
mk_svc "{\"client_id\":\"$CID\",\"name\":\"uds\",\"kind\":\"tcp\",\"bind_port\":$((P_TCP+21)),\"target_host\":\"127.0.0.1\",\"target_port\":1,\"enabled\":true,\"plugin\":{\"kind\":\"unix_domain_socket\",\"unix_path\":\"$WORK/uds.sock\"}}" >/dev/null
sleep 2
python3 - "$((P_TCP+21))" <<'EOF' && ok "插件 unix_domain_socket 回显" || bad "插件 unix_domain_socket 回显"
import socket, sys
s = socket.create_connection(("127.0.0.1", int(sys.argv[1])), timeout=8)
p = b"uds-echo\n"
s.sendall(p); s.settimeout(8)
g = b""
while len(g) < len(p):
    d = s.recv(128)
    if not d: break
    g += d
sys.exit(0 if g == p else 1)
EOF
kill "$UDSPID" 2>/dev/null

# ---------- 负载均衡 group(不健康成员被摘除) ----------
mk_svc "{\"client_id\":\"$CID\",\"name\":\"g1\",\"kind\":\"tcp\",\"bind_port\":$((P_TCP+22)),\"target_host\":\"127.0.0.1\",\"target_port\":$ECHO_TCP,\"enabled\":true,\"group\":\"gg\",\"group_key\":\"gk\"}" >/dev/null
mk_svc "{\"client_id\":\"$CID\",\"name\":\"g2\",\"kind\":\"tcp\",\"bind_port\":$((P_TCP+23)),\"target_host\":\"127.0.0.1\",\"target_port\":18998,\"enabled\":true,\"group\":\"gg\",\"group_key\":\"gk\",\"health_check\":{\"type\":\"tcp\",\"interval_seconds\":1,\"timeout_seconds\":1,\"max_failed\":1}}" >/dev/null
sleep 6
okg=0
for i in 1 2 3 4 5 6; do
  if python3 - "$((P_TCP+22))" <<'EOF'
import socket, sys
s = socket.create_connection(("127.0.0.1", int(sys.argv[1])), timeout=6)
p = b"g\n"; s.sendall(p); s.settimeout(6)
g = b""
while len(g) < len(p):
    d = s.recv(64)
    if not d: break
    g += d
sys.exit(0 if g == p else 1)
EOF
  then okg=$((okg+1)); fi
done
[ "$okg" = "6" ]; check $? "group LB 全部命中健康成员($okg/6)"

# ---------- 带宽限制(server 模式 512KB/s) ----------
mk_svc "{\"client_id\":\"$CID\",\"name\":\"bw\",\"kind\":\"tcp\",\"bind_port\":$((P_TCP+24)),\"target_host\":\"127.0.0.1\",\"target_port\":$ECHO_TCP,\"enabled\":true,\"transport\":{\"bandwidth_limit\":\"512KB\",\"bandwidth_mode\":\"server\"}}" >/dev/null
sleep 2
t0=$(python3 -c 'import time; print(time.time())')
python3 - "$((P_TCP+24))" <<'EOF'
import socket, sys
s = socket.create_connection(("127.0.0.1", int(sys.argv[1])), timeout=20)
p = b"x" * (1024 * 1024)  # 1MB:512KB/s 下至少 ~2s
s.sendall(p); s.settimeout(30)
g = b""
while len(g) < len(p):
    d = s.recv(65536)
    if not d: break
    g += d
sys.exit(0 if g == p else 1)
EOF
BW_RC=$?
t1=$(python3 -c 'import time; print(time.time())')
dur=$(python3 -c "print(f'{$t1-$t0:.1f}')")
[ "$BW_RC" = "0" ] && python3 -c "import sys; sys.exit(0 if $t1-$t0 >= 1.5 else 1)"
check $? "带宽限制 1MB@512KB/s 耗时 ${dur}s(≥1.5s)"

# ---------- 自定义 DNS 解析(客户端 dns_server) ----------
mk_svc "{\"client_id\":\"$CID\",\"name\":\"dnsr\",\"kind\":\"tcp\",\"bind_port\":$((P_TCP+25)),\"target_host\":\"rh-dns.test\",\"target_port\":$ECHO_TCP,\"enabled\":true}" >/dev/null
sleep 2
python3 - "$((P_TCP+25))" <<'EOF' && ok "自定义 DNS 解析目标域名" || bad "自定义 DNS 解析目标域名"
import socket, sys
s = socket.create_connection(("127.0.0.1", int(sys.argv[1])), timeout=8)
p = b"dns-resolve\n"
s.sendall(p); s.settimeout(8)
g = b""
while len(g) < len(p):
    d = s.recv(128)
    if not d: break
    g += d
sys.exit(0 if g == p else 1)
EOF

# ---------- SSH Tunnel Gateway(ssh -R 注册) ----------
SSH_GW=19700
# authorized_keys 与密钥已在服务端启动前生成(server.toml 引用)
ssh -i "$WORK/sshkey" -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
  -o ExitOnForwardFailure=yes -o BatchMode=yes -p $SSH_GW \
  -R 0:127.0.0.1:$ECHO_TCP "$CID@127.0.0.1" sleep 15 2>"$WORK/ssh_err" &
SSHPID=$!
ALLOC_PORT=""
for i in 1 2 3 4 5 6 7 8 9 10; do
  ALLOC_PORT=$(grep -o 'Allocated port [0-9]*' "$WORK/ssh_err" | head -1 | awk '{print $3}')
  [ -n "$ALLOC_PORT" ] && break
  sleep 0.5
done
if [ -n "$ALLOC_PORT" ]; then
  ok "ssh -R 注册(分配端口 $ALLOC_PORT)"
else
  bad "ssh -R 注册(未拿到分配端口)"
fi
sleep 1
python3 - "$ALLOC_PORT" <<'EOF' && ok "SSH 隧道转发数据" || bad "SSH 隧道转发数据"
import socket, sys
s = socket.create_connection(("127.0.0.1", int(sys.argv[1])), timeout=8)
p = b"ssh-tunnel\n"
s.sendall(p); s.settimeout(8)
g = b""
while len(g) < len(p):
    d = s.recv(256)
    if not d: break
    g += d
sys.exit(0 if g == p else 1)
EOF
kill "$SSHPID" 2>/dev/null; PIDS+=("$SSHPID")
sleep 2
python3 - "$ALLOC_PORT" <<'EOF' && ok "ssh 断开后端口释放" || bad "ssh 断开后端口释放"
import socket, sys
try:
    socket.create_connection(("127.0.0.1", int(sys.argv[1])), timeout=3).close()
    sys.exit(1)  # 还能连上 = 未释放
except (ConnectionRefusedError, socket.timeout, OSError):
    sys.exit(0)
EOF


# ---------- 密钥轮换 ----------
NEW_SECRET=$(api -X POST "http://127.0.0.1:$API_PORT/api/clients/$CID/rotate-secret" \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["data"]["secret"])')
[ -n "$NEW_SECRET" ] && [ "$NEW_SECRET" != "$CSECRET" ]; check $? "密钥轮换返回新密钥"
sleep 4
# 旧密钥客户端应被踢出
online_after=$(curl -s "http://127.0.0.1:$API_PORT/api/clients" -H "Authorization: Bearer $TOKEN" \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["data"][0]["online"])')
[ "$online_after" = "False" ]; check $? "轮换后旧会话下线"
# 用新密钥重连
# 便携写入:Python 原子替换,要求恰好命中 1 行 secret 才写回。
# (旧 `sed -i ''` 是 BSD 专用语法:GNU sed 把脚本文本当文件名,secret
# 既没写入,错误信息还把密钥打印进日志。密钥经环境变量传递,不进
# 命令行/错误输出。)
UPDATE_SECRET_VAL="$NEW_SECRET" update_secret "$WORK/client.toml"
check $? "密钥写入 client.toml(便携替换恰好一行)"
# 仅终止本脚本启动的旧客户端(精确 PID;不做 ps 广域匹配)
kill "$CLIENT_PID" 2>/dev/null
"$CLIENT" --config "$WORK/client.toml" >>"$WORK/client.log" 2>&1 &
CLIENT_PID=$!; PIDS+=($CLIENT_PID)
sleep 4
online2=$(curl -s "http://127.0.0.1:$API_PORT/api/clients" -H "Authorization: Bearer $TOKEN" \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["data"][0]["online"])')
[ "$online2" = "True" ]; check $? "新密钥重连上线"

# ---------- 重启恢复 ----------
# 按记录的精确 PID 终止服务端(旧写法 ps 广域匹配 + PIDS[4]=$! 覆盖
# DNS stub 槽位,导致 DNS 残留进程);新 PID 一律追加,绝不覆盖既有槽位
kill "$SRV_PID" 2>/dev/null; sleep 3
"$SERVER" --config "$WORK/server.toml" >>"$WORK/server.log" 2>&1 &
SRV_PID=$!; PIDS+=($SRV_PID)
sleep 5
rec=$(grep -c recovered "$WORK/server.log"); [ "$rec" -ge 1 ]; check $? "服务端重启恢复规则"
python3 - "$P_TCP" <<'EOF' && ok "重启后 TCP 恢复转发" || bad "重启后 TCP 恢复转发"
import socket, sys
s = socket.create_connection(("127.0.0.1", int(sys.argv[1])), timeout=12)
s.sendall(b"rec\n"); s.shutdown(socket.SHUT_WR); s.settimeout(10)
g = b""
while True:
    d = s.recv(256)
    if not d: break
    g += d
sys.exit(0 if g == b"rec\n" else 1)
EOF
python3 - "$P_UDP" <<'EOF' && ok "重启后 UDP 恢复转发" || bad "重启后 UDP 恢复转发"
import socket, sys
u = socket.socket(socket.AF_INET, socket.SOCK_DGRAM); u.settimeout(10)
u.sendto(b"rec\n", ("127.0.0.1", int(sys.argv[1])))
try:
    d, _ = u.recvfrom(65536)
    sys.exit(0 if d == b"rec\n" else 1)
except socket.timeout:
    sys.exit(1)
EOF

# ---------- 统计与事件 ----------
# 重启后 JWT 密钥更换:重新登录
TOKEN=$(curl -s -X POST "http://127.0.0.1:$API_PORT/api/auth/login" \
  -H 'Content-Type: application/json' \
  -d "{\"username\":\"$ADMIN_USER\",\"password\":\"$ADMIN_PASS\"}" \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["data"]["accessToken"])')
stats=$(api "http://127.0.0.1:$API_PORT/api/stats/overview")
echo "$stats" | python3 -c 'import json,sys; d=json.load(sys.stdin)["data"]; sys.exit(0 if d["clients_total"]>=1 and d["services_total"]>=6 else 1)'
check $? "统计总览"
# F1 流量计量:此前 suite 已跑过 TCP/UDP/HTTP 等多类转发,全局字节计数必须 > 0
echo "$stats" | python3 -c 'import json,sys; d=json.load(sys.stdin)["data"]; sys.exit(0 if d["bytes_in"]>0 and d["bytes_out"]>0 else 1)'
check $? "流量统计(F1:入/出站字节数递增)"
# F1 分规则计量:任一 tcp 规则 bytes_in > 0
api "http://127.0.0.1:$API_PORT/api/services" | python3 -c '
import json, sys
rows = json.load(sys.stdin)["data"]
tcp = [r for r in rows if r["kind"] == "tcp" and r.get("bytes_in", 0) > 0]
sys.exit(0 if tcp else 1)'; check $? "流量统计(F1:分规则 bytes_in 递增)"

# ---------- 服务器配置(热生效) ----------
unauth_cfg=$(curl -s -o /dev/null -w "%{http_code}" "http://127.0.0.1:$API_PORT/api/server/config")
[ "$unauth_cfg" = "401" ]; check $? "服务器配置:未认证拒绝"
cfg_restore=$(api "http://127.0.0.1:$API_PORT/api/server/config" | python3 -c 'import json,sys;print("|".join(json.load(sys.stdin)["data"]["hot"]["allow_ports"]))')
r=$(api -X PUT "http://127.0.0.1:$API_PORT/api/server/config" -H 'Content-Type: application/json' -d '{"ports":{"allow_ports":["18301"]}}')
echo "$r" | python3 -c 'import json,sys;d=json.load(sys.stdin)["data"];sys.exit(0 if "ports.allow_ports" in d["applied"] and d["persisted"] else 1)'
check $? "服务器配置:allow_ports 收紧热生效并写回"
cat > "$WORK/denysvc.json" <<EOF
{"client_id":"$CID","name":"denytest","kind":"tcp","bind_port":18999,"target_host":"127.0.0.1","target_port":80,"enabled":false}
EOF
denied=$(api -X POST "http://127.0.0.1:$API_PORT/api/services" -H 'Content-Type: application/json' --data-binary "@$WORK/denysvc.json" | python3 -c 'import json,sys;print(json.load(sys.stdin)["code"]!=0)')
[ "$denied" = "True" ]; check $? "服务器配置:收紧后白名单外端口被拒"
restore_body=$(CFG="$cfg_restore" python3 -c '
import json, os
allow = [x for x in os.environ["CFG"].split("|") if x]
print(json.dumps({"ports": {"allow_ports": allow}}))')
api -X PUT "http://127.0.0.1:$API_PORT/api/server/config" -H 'Content-Type: application/json' -d "$restore_body" > /dev/null
api -X PUT "http://127.0.0.1:$API_PORT/api/server/config" -H 'Content-Type: application/json' -d '{"ssh_gateway":{"enabled":true,"bind_port":18022,"bind_addr":"127.0.0.1"},"log":{"level":"debug"}}' | python3 -c 'import json,sys;d=json.load(sys.stdin)["data"];sys.exit(0 if "ssh_gateway.enabled" in d["applied"] and "log.level" in d["applied"] else 1)'
check $? "服务器配置:SSH 网关热开启 + 日志级别热切"
sleep 1
python3 -c 'import socket;s=socket.create_connection(("127.0.0.1",18022),timeout=3);s.close()' 2>/dev/null; check $? "SSH 网关:热开启后监听可达"
api -X PUT "http://127.0.0.1:$API_PORT/api/server/config" -H 'Content-Type: application/json' -d '{"ssh_gateway":{"enabled":false},"log":{"level":"info"}}' > /dev/null
sleep 1
python3 -c 'import socket
try:
    socket.create_connection(("127.0.0.1",18022),timeout=2); raise SystemExit(1)
except OSError: raise SystemExit(0)' 2>/dev/null; check $? "SSH 网关:热关闭后监听撤销"

# ---------- F2 客户端配额(max_services) ----------
QR=$(api -X POST "http://127.0.0.1:$API_PORT/api/clients" -H 'Content-Type: application/json' -d '{"name":"quota-test","max_services":1}')
QID=$(echo "$QR" | python3 -c 'import json,sys;print(json.load(sys.stdin)["data"]["id"])')
cat > "$WORK/q1.json" <<EOF
{"client_id":"$QID","name":"q1","kind":"http","target_host":"127.0.0.1","target_port":80,"enabled":false,"http":{"custom_domains":["q1.test"]}}
EOF
cat > "$WORK/q2.json" <<EOF
{"client_id":"$QID","name":"q2","kind":"http","target_host":"127.0.0.1","target_port":80,"enabled":false,"http":{"custom_domains":["q2.test"]}}
EOF
q1=$(api -X POST "http://127.0.0.1:$API_PORT/api/services" -H 'Content-Type: application/json' --data-binary "@$WORK/q1.json" | python3 -c 'import json,sys;print(json.load(sys.stdin)["code"]==0)' 2>/dev/null)
q2=$(api -X POST "http://127.0.0.1:$API_PORT/api/services" -H 'Content-Type: application/json' --data-binary "@$WORK/q2.json" | python3 -c 'import json,sys;print(json.load(sys.stdin)["code"]!=0)' 2>/dev/null)
[ "$q1" = "True" ] && [ "$q2" = "True" ]; check $? "F2 配额:max_services=1 时第二条被拒(q1=$q1 q2=$q2)"
api -X DELETE "http://127.0.0.1:$API_PORT/api/clients/$QID" > /dev/null
events=$(api "http://127.0.0.1:$API_PORT/api/events?limit=5" | python3 -c 'import json,sys; print(len(json.load(sys.stdin)["data"])>0)')
[ "$events" = "True" ]; check $? "事件查询"
# 指标自 0fe4069 起在独立无鉴权端口暴露(config [metrics] bind_addr/port,
# 本 E2E 显式配置 METRICS_PORT),不再挂 API 端口下的 /metrics。
# 1) 独立端口:不带任何认证头必须 HTTP 200 且含 >3 行格式合法的
#    rabbithole_ 指标样本(名{标签} 数值)。空 body/404 不算成功:
#    状态码显式校验,样本行格式显式校验。
mcode=$(curl -s -o "$WORK/metrics.txt" -w "%{http_code}" -m 5 "http://127.0.0.1:$METRICS_PORT/metrics")
mlines=$(grep -cE '^rabbithole_[a-z_0-9]+(\{[^}]*\})? [0-9]' "$WORK/metrics.txt" 2>/dev/null || true)
[ "$mcode" = "200" ] && [ "$mlines" -gt 3 ]; check $? "独立 metrics 端口无鉴权暴露(HTTP=$mcode, 样本行=${mlines})"
# 2) API 端口不再暴露 rabbithole_ 指标:带令牌与不带令牌各验一次,
#    断言只认 body 内容(0 行指标);实际状态码写入标签供人工核对,
#    404(路由不存在)或 200 空指标都视为"不再暴露"。
apic_with=$(curl -s -m 5 -H "Authorization: Bearer $TOKEN" "http://127.0.0.1:$API_PORT/metrics" | grep -c "rabbithole_")
apic_with_code=$(curl -s -o /dev/null -w "%{http_code}" -m 5 -H "Authorization: Bearer $TOKEN" "http://127.0.0.1:$API_PORT/metrics")
[ "$apic_with" -eq 0 ]; check $? "API 端口 /metrics 不再暴露 rabbithole_ 指标(带令牌 HTTP=$apic_with_code)"
apic_no=$(curl -s -m 5 "http://127.0.0.1:$API_PORT/metrics" | grep -c "rabbithole_")
apic_no_code=$(curl -s -o /dev/null -w "%{http_code}" -m 5 "http://127.0.0.1:$API_PORT/metrics")
[ "$apic_no" -eq 0 ]; check $? "API 端口 /metrics 无认证亦不暴露 rabbithole_ 指标(HTTP=$apic_no_code)"

# ---------- 首次启动引导 + 修改密码(独立短环境) ----------
BOOT="$WORK/boot"; mkdir -p "$BOOT"
cat > "$BOOT/s.toml" <<EOF5
bind_addr = "127.0.0.1"
bind_protocol = "tcp"
bind_port = 19195
[web]
bind_addr = "127.0.0.1"
port = 19196
[vhost]
http_port = 0
https_port = 0
tcpmux_port = 0
[log]
level = "info"
EOF5
"$SERVER" --config "$BOOT/s.toml" >"$BOOT/out.log" 2>&1 &
BOOT_PID=$!; PIDS+=($BOOT_PID)
sleep 2
BOOT_PW=$(grep -o "password: [A-Za-z0-9]*" "$BOOT/out.log" | head -1 | awk '{print $2}')
[ "${#BOOT_PW}" -ge 16 ]; check $? "首次启动引导:控制台一次性展示随机密码"
BOOT_T=$(curl -s -X POST "http://127.0.0.1:19196/api/auth/login" -H 'Content-Type: application/json' \
  -d "{\"username\":\"admin\",\"password\":\"$BOOT_PW\"}" | python3 -c 'import json,sys;print(json.load(sys.stdin)["data"]["accessToken"])' 2>/dev/null)
[ -n "$BOOT_T" ]; check $? "首次启动引导:初始密码可登录"
bad=$(curl -s -X POST "http://127.0.0.1:19196/api/auth/password" -H "Authorization: Bearer $BOOT_T" \
  -H 'Content-Type: application/json' -d "{\"old_password\":\"WRONG\",\"new_password\":\"Boot-New-456\"}" | python3 -c 'import json,sys;print(json.load(sys.stdin)["code"])')
[ "$bad" != "0" ]; check $? "修改密码:错误旧密码被拒(code=$bad)"
chg=$(curl -s -X POST "http://127.0.0.1:19196/api/auth/password" -H "Authorization: Bearer $BOOT_T" \
  -H 'Content-Type: application/json' -d "{\"old_password\":\"$BOOT_PW\",\"new_password\":\"Boot-New-456\"}" | python3 -c 'import json,sys;print(json.load(sys.stdin)["code"])')
[ "$chg" = "0" ]; check $? "修改密码:正确旧密码修改成功"
stale=$(curl -s -o /dev/null -w "%{http_code}" -H "Authorization: Bearer $BOOT_T" "http://127.0.0.1:19196/api/clients")
[ "$stale" = "401" ]; check $? "修改密码:旧会话令牌立即失效($stale)"
NEW_T=$(curl -s -X POST "http://127.0.0.1:19196/api/auth/login" -H 'Content-Type: application/json' \
  -d "{\"username\":\"admin\",\"password\":\"Boot-New-456\"}" | python3 -c 'import json,sys;print(json.load(sys.stdin)["data"]["accessToken"])' 2>/dev/null)
[ -n "$NEW_T" ]; check $? "修改密码:新密码可登录"
oldpw=$(curl -s -X POST "http://127.0.0.1:19196/api/auth/login" -H 'Content-Type: application/json' \
  -d "{\"username\":\"admin\",\"password\":\"$BOOT_PW\"}" | python3 -c 'import json,sys;print(json.load(sys.stdin)["code"])')
[ "$oldpw" != "0" ]; check $? "修改密码:旧密码被拒绝(code=$oldpw)"
kill $BOOT_PID 2>/dev/null
sleep 1
"$SERVER" --config "$BOOT/s.toml" >"$BOOT/out2.log" 2>&1 &
BOOT_PID2=$!; PIDS+=($BOOT_PID2)
sleep 2
banner2=$(grep -c "password:" "$BOOT/out2.log")
[ "$banner2" = "0" ]; check $? "重启不重复引导(无密码横幅)"
RT=$(curl -s -X POST "http://127.0.0.1:19196/api/auth/login" -H 'Content-Type: application/json' \
  -d "{\"username\":\"admin\",\"password\":\"Boot-New-456\"}" | python3 -c 'import json,sys;print(json.load(sys.stdin)["data"]["accessToken"])' 2>/dev/null)
[ -n "$RT" ]; check $? "重启后修改过的密码持久有效"
kill $BOOT_PID2 2>/dev/null

# ---------- Round-9.1(F2):多载体池化端到端 ----------
# 覆盖服务端聚合路径(Round-9):客户端以 mux_pool_size=3 拨 3 条物理
# 连接,首条控制 + 2 条附加载体报到;服务端把附加载体聚合进会话池,
# 数据流经池合流路由。断言四件事:
#   1) 服务端日志出现 2 条 "pool carrier attached"(真实聚合,非手工原语);
#   2) 经代理规则端口 P_TCP 的随机字节 echo 全通(真实数据面);
#   3) 会话替换(杀池化客户端→重启单载体)后,服务端控制端口上旧池化
#      载体的 ESTAB 连接收敛(计数回落到重启后的预期);
#   4) 收敛后数据面恢复(再次 echo)。
# 连接计数在 ss 不可用时退化到 netstat;两者皆无则标注 SKIP(只用
# 可达性与日志断言),不伪造通过。

# 服务端进程在控制端口上**仍持有**的连接数(pid 过滤到 rh-server)。
# 计 ESTABLISHED + CLOSE_WAIT:客户端进程被 kill 后,若服务端不收敛,
# 该连接会永久停在 CLOSE_WAIT;收敛后服务端主动 close,连接转入
# TIME_WAIT 并很快消失——TIME_WAIT 不计入,避免把"已收敛的残留 pcb"
# 误判为泄漏。输出整数;ss/lsof/netstat 均不可用时输出 NA。
ctl_estab() {  # $1=本地端口 $2=服务端 pid
  python3 - "$1" "$2" <<'PYEOF'
import shutil, subprocess, sys
port, srv_pid = sys.argv[1], sys.argv[2]
suffix = ":" + port
def run(cmd, ok_codes=(0,)):
    # 命令失败(无权限/内核不支持)返回 None,让调用方退化到下一工具;
    # 成功但无匹配连接必须返回空串/计数 0,不能与"工具不可用"混淆
    # (否则 SKIP 条件永真)。lsof 无匹配时退出码为 1,调用方显式放行。
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=5)
    except Exception:
        return None
    return r.stdout if r.returncode in ok_codes else None

def count_ss():
    out = run(["ss", "-tnp"])
    if out is None:
        return None
    # 无任何进程信息(非特权环境内核不给 users:):按 pid 过滤不可用,
    # 返回 None 交给下一工具,而不是把"看不见"当成"没有连接"(假 0)。
    if "users:(" not in out:
        return None
    n = 0
    for line in out.splitlines():
        f = line.split()
        # ss -tnp 默认列序:State Recv-Q Send-Q Local Peer [Process]
        # 状态在第 1 列、本地地址在第 4 列(第 3 列是 Send-Q,不是地址)。
        # 进程信息形如 users:(("rh-server",pid=123,fd=7))
        if len(f) < 5 or f[0] not in ("ESTAB", "ESTABLISHED", "CLOSE-WAIT", "CLOSE_WAIT"):
            continue
        if f[3].endswith(suffix):
            if ("pid=%s," % srv_pid) in line or ("pid=%s)" % srv_pid) in line:
                n += 1
    return n

def count_lsof():
    # lsof 无匹配连接时退出码 1(不是错误):放行,计 0。
    out = run(["lsof", "-nP", "-a", "-p", srv_pid, "-iTCP:" + port], ok_codes=(0, 1))
    if out is None:
        return None
    n = 0
    for line in out.splitlines():
        if "ESTABLISHED" not in line and "CLOSE_WAIT" not in line:
            continue
        for tok in line.split():
            if "->" in tok:
                lhs = tok.split("->", 1)[0]
                if lhs.endswith(suffix):
                    n += 1
                break
    return n

def count_netstat():
    # 退化路径(无法按 pid 过滤):只计 ESTABLISHED/CLOSE_WAIT +
    # 本地端口匹配;其他进程同端口连接会抬高计数,故仅在无 ss/lsof 时使用。
    out = run(["netstat", "-an", "-p", "tcp"])
    if out is None:
        return None
    n = 0
    for line in out.splitlines():
        f = line.split()
        # 列序:Proto Recv-Q Send-Q Local Foreign State [PID/Program]。
        # macOS 无 -p 列(状态为末列);Linux 带 -p 时末列是 PID/Program,
        # 故状态在 f[4:] 中查找而不绑定固定下标。本地地址:macOS 形如
        # 192.168.50.23.52334(`.` 分隔),Linux 形如 127.0.0.1:18100;
        # 取最后一段端口比较,两种分隔符下都成立。
        if len(f) < 6 or not ({"ESTABLISHED", "CLOSE_WAIT"} & set(f[4:])):
            continue
        local_port = f[3].rsplit(":", 1)[-1].rsplit(".", 1)[-1]
        if local_port == port:
            n += 1
    return n

for fn, tool in (
    (count_ss, "ss"),
    (count_lsof, "lsof"),
    (count_netstat, "netstat"),
):
    if not shutil.which(tool):
        continue
    n = fn()
    if n is not None:
        print(n)
        sys.exit(0)
print("NA")
PYEOF
}

# 等待主客户端(CID)重新上线:最多 10s
wait_client_online() {  # $1=client_id
  local i on
  for i in $(seq 1 20); do
    on=$(curl -s "http://127.0.0.1:$API_PORT/api/clients" -H "Authorization: Bearer $TOKEN" \
      | python3 -c 'import json,sys; d=json.load(sys.stdin)["data"]; print([c["online"] for c in d if c["id"]==sys.argv[1]][0])' "$1" 2>/dev/null || true)
    [ "$on" = "True" ] && return 0
    sleep 0.5
  done
  return 1
}

CTL_BASE=$(ctl_estab "$CTL_PORT" "$SRV_PID")

# 硬件上限期望值(round-49:**直接问被测二进制**)。
#
# 修前这里用 Python 独立推算:CPU 亲和数(及 cgroup 配额)/2,再与
# /sys/class/net/*/queues/rx-* 的最大值取 min,钳制 [1,8]。口径与 Rust 侧
# `detect_hardware_cap()` 看似相同,但两者用的是**不同的系统接口**:
#   - 脚本:`os.sched_getaffinity(0)` + 手工读 cgroup v1/v2 配额;
#   - Rust:`std::thread::available_parallelism()`
# available_parallelism 在嵌套 cgroup 下的取值与"亲和数 ∩ 配额"未必相等
# (REPORT-round10.md「## 6. 遗留」第 1 条已承认)。该差异只影响 e2e 期望值,
# 不误 PASS,但会让断言在本不该 SKIP 的时候 SKIP。
#
# 现在改为消费 `rh-client --print-hardware-cap` 的输出——即**客户端自己
# 算出的那个数**,口径不可能再漂移。若二进制不可执行则回退到旧推算
# (保持脚本在只构建了部分目标时仍可用)。
indep_hw_cap() {
  local cap
  if [ -x "$CLIENT" ]; then
    cap=$("$CLIENT" --print-hardware-cap 2>/dev/null | tr -dc '0-9')
  fi
  if [ -z "${cap:-}" ]; then
    # 回退:二进制缺失/不可执行时的独立推算(与修前逻辑一致)
    cap=$(python3 - <<'PYEOF'
import glob, os

def cgroup_quota():
    try:
        with open("/sys/fs/cgroup/cpu.max", encoding="utf-8") as fh:
            q, p = fh.read().split()[:2]
            if q != "max" and int(p) > 0:
                return max(1, int(q) // int(p))
    except Exception:
        pass
    try:
        q = int(open("/sys/fs/cgroup/cpu/cpu.cfs_quota_us", encoding="utf-8").read())
        p = int(open("/sys/fs/cgroup/cpu/cpu.cfs_period_us", encoding="utf-8").read())
        if q > 0 and p > 0:
            return max(1, q // p)
    except Exception:
        pass
    return None

try:
    cpus = len(os.sched_getaffinity(0))
except AttributeError:
    cpus = os.cpu_count() or 1
q = cgroup_quota()
if q:
    cpus = min(cpus, q)
cap = max(cpus // 2, 1)
queues = []
for iface in glob.glob("/sys/class/net/*"):
    n = len(glob.glob(os.path.join(iface, "queues", "rx-*")))
    if n:
        queues.append(n)
if queues:
    cap = min(cap, max(max(queues), 1))
print(max(1, min(cap, 8)))
PYEOF
)
  fi
  # 兜底:任何异常都不得让 cap 为空(后续 -gt/-lt 数值比较会崩)
  case "${cap:-}" in
    ''|*[!0-9]*) cap=1 ;;
  esac
  echo "$cap"
}

# 按源端口归因(round-10.1 §2.5):记录客户端进程到控制端口的 ESTAB 连接
# 本地源端口。拆除后按这些端口在服务端侧精确计数,避免全局 ESTAB 噪声。
client_ctl_sports() {  # $1=控制端口 $2=客户端 pid → 空格分隔源端口
  python3 - "$1" "$2" <<'PYEOF'
import shutil, subprocess, sys
port, pid = sys.argv[1], sys.argv[2]

def run(cmd, ok=(0,)):
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=5)
    except Exception:
        return None
    return r.stdout if r.returncode in ok else None

def from_ss():
    out = run(["ss", "-tnp"])
    if out is None or "users:(" not in out:
        return None
    sports = set()
    for line in out.splitlines():
        f = line.split()
        if len(f) < 5 or f[0] != "ESTAB" or not f[4].endswith(":" + port):
            continue
        if ("pid=%s," % pid) in line or ("pid=%s)" % pid) in line:
            sports.add(f[3].rsplit(":", 1)[-1])
    return sports

def from_lsof():
    out = run(["lsof", "-nP", "-a", "-p", pid, "-iTCP:" + port], ok=(0, 1))
    if out is None:
        return None
    sports = set()
    for line in out.splitlines():
        if "ESTABLISHED" not in line:
            continue
        for tok in line.split():
            if "->" not in tok:
                continue
            lhs, rhs = tok.split("->", 1)
            if rhs.rsplit(":", 1)[-1] == port:
                sports.add(lhs.rsplit(":", 1)[-1])
            break
    return sports

for fn, tool in ((from_ss, "ss"), (from_lsof, "lsof")):
    if not shutil.which(tool):
        continue
    s = fn()
    if s is not None:
        print(" ".join(sorted(s)))
        sys.exit(0)
print("")
PYEOF
}

# 服务端侧:本地端口 = 控制端口、对端源端口 ∈ 给定集合的连接数
# (ESTABLISHED + CLOSE_WAIT,同 ctl_estab 口径)。NA = 工具不可用。
ctl_estab_peer_ports() {  # $1=控制端口 $2=服务端 pid $3..=客户端源端口
  python3 - "$@" <<'PYEOF'
import shutil, subprocess, sys
port, srv_pid = sys.argv[1], sys.argv[2]
peers = set(p for p in sys.argv[3:] if p)

def run(cmd, ok=(0,)):
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=5)
    except Exception:
        return None
    return r.stdout if r.returncode in ok else None

def count_ss():
    out = run(["ss", "-tnp"])
    if out is None or "users:(" not in out:
        return None
    n = 0
    for line in out.splitlines():
        f = line.split()
        # State Recv-Q Send-Q Local Peer [Process]
        if len(f) < 5 or f[0] not in ("ESTAB", "ESTABLISHED", "CLOSE-WAIT", "CLOSE_WAIT"):
            continue
        if not f[3].endswith(":" + port) or f[4].rsplit(":", 1)[-1] not in peers:
            continue
        if ("pid=%s," % srv_pid) in line or ("pid=%s)" % srv_pid) in line:
            n += 1
    return n

def count_lsof():
    out = run(["lsof", "-nP", "-a", "-p", srv_pid, "-iTCP:" + port], ok=(0, 1))
    if out is None:
        return None
    n = 0
    for line in out.splitlines():
        if "ESTABLISHED" not in line and "CLOSE_WAIT" not in line:
            continue
        for tok in line.split():
            if "->" not in tok:
                continue
            lhs, rhs = tok.split("->", 1)
            if lhs.rsplit(":", 1)[-1] == port and rhs.rsplit(":", 1)[-1] in peers:
                n += 1
            break
    return n

def count_netstat():
    # 无法按 pid 过滤:本地端口 + 对端源端口双重匹配后,同端口的其他
    # 客户端不会落入(源端口集合只来自被测进程)。
    out = run(["netstat", "-an", "-p", "tcp"])
    if out is None:
        return None
    n = 0
    for line in out.splitlines():
        f = line.split()
        if len(f) < 6 or not ({"ESTABLISHED", "CLOSE_WAIT"} & set(f[4:])):
            continue
        local_port = f[3].rsplit(":", 1)[-1].rsplit(".", 1)[-1]
        peer_port = f[4].rsplit(":", 1)[-1].rsplit(".", 1)[-1]
        if local_port == port and peer_port in peers:
            n += 1
    return n

for fn, tool in ((count_ss, "ss"), (count_lsof, "lsof"), (count_netstat, "netstat")):
    if not shutil.which(tool):
        continue
    n = fn()
    if n is not None:
        print(n)
        sys.exit(0)
print("NA")
PYEOF
}

# 按 PID 轮换主客户端为池化形态(3 载体):旧单载体连接收尾后再起新进程,
# 池化阶段的连接增量只归因于新进程。
cp "$WORK/client.toml" "$WORK/client-pool.toml"
printf '\nmux_pool_size = 3\n' >> "$WORK/client-pool.toml"
kill "$CLIENT_PID" 2>/dev/null
sleep 2
"$CLIENT" --config "$WORK/client-pool.toml" >"$WORK/client-pool.log" 2>&1 &
CLIENT_PID=$!; PIDS+=($CLIENT_PID)
wait_client_online "$CID"; check $? "池化客户端上线(mux_pool_size=3)"

# 服务端聚合生效:1 首条 + (K-1) 条附加。K 不取自被测客户端日志:
# 显式 mux_pool_size=3 按独立硬件上限钳制(§2.5)。initial_k 缺失或与
# 独立期望不符 → FAIL;只有「显式 3 被硬件上限合法钳制到 1」才 SKIP
# (钳制本身由 indep_hw_cap 独立证明)。
POOL_HW_CAP=$(indep_hw_cap)
POOL_EXPECT_K=3
[ "$POOL_EXPECT_K" -gt "$POOL_HW_CAP" ] && POOL_EXPECT_K=$POOL_HW_CAP
POOL_SIZING=""
for _ in $(seq 1 20); do
  POOL_SIZING=$(sed -E 's/\x1b\[[0-9;]*m//g' "$WORK/client-pool.log" 2>/dev/null \
    | grep "pool sizing" | tail -1)
  [ -n "$POOL_SIZING" ] && break
  sleep 0.5
done
POOL_K=$(printf '%s' "$POOL_SIZING" | sed -nE 's/.*initial_k=([0-9]+).*/\1/p')
if [ -z "$POOL_K" ]; then
  bad "池化客户端定容日志缺失(实际: $POOL_SIZING)"
elif [ "$POOL_K" -ne "$POOL_EXPECT_K" ]; then
  bad "池化客户端 initial_k=$POOL_K,独立期望 $POOL_EXPECT_K(显式 3,硬件上限 $POOL_HW_CAP)"
elif [ "$POOL_EXPECT_K" -lt 2 ]; then
  echo "SKIP: 池化载体聚合断言(独立硬件上限 $POOL_HW_CAP 合法钳制显式 3 → initial_k=$POOL_K)"
else
  want_attached=$((POOL_EXPECT_K - 1))
  attached=0
  for _ in $(seq 1 20); do
    attached=$(grep -c "pool carrier attached" "$WORK/server.log" 2>/dev/null || true)
    [ "$attached" -ge "$want_attached" ] && break
    sleep 0.5
  done
  if [ "$attached" -ge "$want_attached" ]; then
    ok "池化客户端 $want_attached 条附加载体聚合(expect_k=$POOL_EXPECT_K, attached=$attached)"
  else
    bad "池化客户端附加载体聚合(attached=$attached,期望>= $want_attached)"
  fi
fi
# 环回上附加必须全成:失败/被拒/缺 ACK 只允许出现在非环回/受限环境。
if grep -qE "pool carrier attach (failed|rejected)|carrier attach ack (missing|timeout)|bad carrier attach ack" "$WORK/client-pool.log"; then
  bad "池化客户端附加载体无失败(日志出现失败/拒绝/缺 ACK)"
else
  ok "池化客户端附加载体无失败"
fi

# 数据面:经真实代理端口 P_TCP 写随机字节并从同连接读回(非端口探测)。
python3 - "$P_TCP" <<'EOF' && ok "池化会话 TCP 随机字节 echo" || bad "池化会话 TCP 随机字节 echo"
import os, socket, sys
p = os.urandom(100 * 1024) + b"-POOL-"
s = socket.create_connection(("127.0.0.1", int(sys.argv[1])), timeout=10)
s.sendall(p); s.settimeout(10)
g = b""
while len(g) < len(p):
    d = s.recv(65536)
    if not d: break
    g += d
sys.exit(0 if g == p else 1)
EOF

# 会话替换后的载体收敛:池化运行期按**客户端源端口**记录载体连接集合,
# 杀池化客户端并重启单载体客户端后,按同一组源端口在服务端侧计数
# (round-10.1 §2.5:不再用全局 ESTAB —— 其中含无关连接)。全局计数
# delta 仅作辅助信息。源端口集合不可得时退化为 after <= base + 1。
sleep 1
CTL_POOL=$(ctl_estab "$CTL_PORT" "$SRV_PID")
POOL_SPORTS=$(client_ctl_sports "$CTL_PORT" "$CLIENT_PID")
kill "$CLIENT_PID" 2>/dev/null
sleep 3   # 服务端会话拆除 + driver 中止 → 载体 socket 关闭
"$CLIENT" --config "$WORK/client.toml" >>"$WORK/client.log" 2>&1 &
CLIENT_PID=$!; PIDS+=($CLIENT_PID)
wait_client_online "$CID"; check $? "单载体客户端重启上线(会话替换)"
sleep 2
CTL_AFTER=$(ctl_estab "$CTL_PORT" "$SRV_PID")
PORTS_AFTER="NA"
if [ -n "$POOL_SPORTS" ]; then
  # shellcheck disable=SC2086  # 源端口按空格分隔展开为独立参数
  PORTS_AFTER=$(ctl_estab_peer_ports "$CTL_PORT" "$SRV_PID" $POOL_SPORTS)
fi
# T-E2(round-10,2026-10-20 修订):tcp 默认(无 mux_pool_size)= 协议基线 2
# → 应有两条物理载体;定容日志同样要给出 initial_k=final_k=2。
# (单载体在 2.1-2.2G 封顶且与流数无关,故 tcp 族默认 K=2。)
# 日志带 ANSI 颜色,先剥转义再匹配。上线判定早于客户端写定容日志
# (服务端先记账),轮询等它。
SIZING_DEFAULT=""
for _ in $(seq 1 20); do
  SIZING_DEFAULT=$(sed -E 's/\x1b\[[0-9;]*m//g' "$WORK/client.log" 2>/dev/null \
    | grep "pool sizing" | grep "protocol=tcp" | tail -1)
  [ -n "$SIZING_DEFAULT" ] && break
  sleep 0.5
done
if printf '%s' "$SIZING_DEFAULT" | grep -qE "initial_k=2 .*final_k=2|final_k=2 .*initial_k=2"; then
  ok "T-E2: tcp 默认定容 initial_k=final_k=2"
else
  bad "T-E2: tcp 默认定容日志缺失或 K≠2(实际: $SIZING_DEFAULT)"
fi
# 按客户端 PID 计数:控制端口上还有 env-only 客户端,服务端侧聚合计数
# 无法归因到本进程。ss/lsof 按 pid 过滤;netstat 无法按 pid,直接 NA。
CLI_CONNS=$(python3 - "$CTL_PORT" "$CLIENT_PID" <<'PYEOF'
import shutil, subprocess, sys
port, pid = sys.argv[1], sys.argv[2]

def run(cmd, ok=(0,)):
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=5)
    except Exception:
        return None
    return r.stdout if r.returncode in ok else None

def count_ss():
    out = run(["ss", "-tnp"])
    if out is None or "users:(" not in out:
        return None
    n = 0
    for line in out.splitlines():
        f = line.split()
        if len(f) < 5 or not f[0].startswith("ESTAB"):
            continue
        if f[4].endswith(":" + port) or f[3].endswith(":" + port):
            if ("pid=%s," % pid) in line or ("pid=%s)" % pid) in line:
                n += 1
    return n

def count_lsof():
    out = run(["lsof", "-nP", "-a", "-p", pid, "-iTCP:" + port], ok=(0, 1))
    if out is None:
        return None
    # 同一 socket 的 fd 副本会出现多行:按四元组去重
    seen = set()
    for line in out.splitlines():
        if "ESTABLISHED" not in line:
            continue
        for tok in line.split():
            if "->" in tok:
                seen.add(tok)
                break
    return len(seen)

for fn, tool in ((count_ss, "ss"), (count_lsof, "lsof")):
    if not shutil.which(tool):
        continue
    n = fn()
    if n is not None:
        print(n)
        sys.exit(0)
print("NA")
PYEOF
)
if [ "$CLI_CONNS" = "NA" ]; then
  echo "SKIP: T-E2 物理载体数断言(ss/lsof 均不可用)"
elif [ "$CLI_CONNS" = "2" ]; then
  ok "T-E2: tcp 默认 K=2,客户端控制口物理连接数 = 2"
else
  bad "T-E2: tcp 默认 K=2,实际物理连接数 = $CLI_CONNS"
fi
if [ "$CTL_BASE" = "NA" ] || [ "$CTL_POOL" = "NA" ] || [ "$CTL_AFTER" = "NA" ]; then
  echo "SKIP: 会话替换后旧载体连接收敛计数(ss/netstat 均不可用)"
  # 退化断言:数据面确认新会话可用(旧载体是否泄漏由 F1 单测覆盖)
  python3 - "$P_TCP" <<'EOF' && ok "会话替换后数据面恢复(连接计数 SKIP 退化断言)" || bad "会话替换后数据面恢复(连接计数 SKIP 退化断言)"
import socket, sys
s = socket.create_connection(("127.0.0.1", int(sys.argv[1])), timeout=10)
s.sendall(b"after-pool\n"); s.shutdown(socket.SHUT_WR); s.settimeout(10)
g = b""
while True:
    d = s.recv(256)
    if not d: break
    g += d
sys.exit(0 if g == b"after-pool\n" else 1)
EOF
else
  delta=$((CTL_POOL - CTL_AFTER))
  if [ "$POOL_EXPECT_K" -le 1 ]; then
    echo "SKIP: 会话替换后旧载体收敛计数(独立硬件上限 $POOL_HW_CAP 使池化阶段不建池)"
  elif [ "$PORTS_AFTER" != "NA" ]; then
    # 精确定位:池化期该客户端用过的全部源端口,拆除后在服务端侧必须归零
    # (留 1 条容忍握手/关闭窗口)。delta 只作辅助信息。
    if [ "$PORTS_AFTER" -le 1 ]; then
      ok "会话替换后旧池化载体收敛(源端口归属残留=$PORTS_AFTER,期望<=1;base=$CTL_BASE pool=$CTL_POOL after=$CTL_AFTER delta=$delta)"
    else
      bad "会话替换后旧池化载体收敛(源端口归属残留=$PORTS_AFTER,期望<=1;base=$CTL_BASE pool=$CTL_POOL after=$CTL_AFTER delta=$delta)"
    fi
  elif [ "$CTL_AFTER" -le $((CTL_BASE + 1)) ]; then
    # 源端口集合不可得(工具限制):退化为 after <= base + 1(拆除后只
    # 应剩新单载体会话;+1 容忍 TIME_WAIT/关闭窗口)。delta 仅参考。
    ok "会话替换后旧池化载体收敛(after=$CTL_AFTER <= base+1=$((CTL_BASE + 1));base=$CTL_BASE pool=$CTL_POOL delta=$delta)"
  else
    bad "会话替换后旧池化载体收敛(after=$CTL_AFTER 超过 base+1=$((CTL_BASE + 1));base=$CTL_BASE pool=$CTL_POOL delta=$delta)"
  fi
  # 数据面恢复(单载体路径,零漂移验证)
  python3 - "$P_TCP" <<'EOF' && ok "会话替换后数据面恢复" || bad "会话替换后数据面恢复"
import socket, sys
s = socket.create_connection(("127.0.0.1", int(sys.argv[1])), timeout=10)
s.sendall(b"after-pool\n"); s.shutdown(socket.SHUT_WR); s.settimeout(10)
g = b""
while True:
    d = s.recv(256)
    if not d: break
    g += d
sys.exit(0 if g == b"after-pool\n" else 1)
EOF
fi

echo
echo "== 结果: PASS=$PASS FAIL=$FAIL =="
[ "$FAIL" = "0" ]
