#!/usr/bin/env bash
# rabbithole 客户端插件 E2E(七个无外部依赖插件):
#   http_proxy / socks5 / https2http / https2https / http2https / http2http / tls2raw
# 每插件一条规则,经 服务端→客户端→插件→本地辅助服务 全链路验证语义:
#   - 改写类(http2http/http2https/https2http/https2https):上游记录收到的请求头,
#     断言 Host 重写 / HeaderOp(set 后 del)/ 响应回传
#   - tls2raw:TLS 入 → 裸 echo 上游回显
#   - http_proxy/socks5:插件在客户端进程内扮演**代理服务端**。断言用户的
#     CONNECT / SOCKS5 请求里写的目的地被真正拨通并回显(配置的 target 故意
#     填不可达地址,任何"拨配置目标"的实现都会失败),并覆盖 407 / RFC1929
#     认证失败 / 502 / 非 CONNECT 命令 四个负向分支
# 用法: tests/e2e-plugins.sh [二进制目录(默认 target/release)]
set -u
BIN_DIR="${1:-$(dirname "$0")/../target/release}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SERVER="$ROOT/$BIN_DIR/rh-server"
CLIENT="$ROOT/$BIN_DIR/rh-client"
WORK=$(mktemp -d /tmp/rh-plugins.XXXXXX)
API=30580; CTL=30500; ECHO=30501; HDR=30502; HDR_TLS=30503
PASS=0; FAIL=0; PIDS=()
cleanup(){
  for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null; done
  rm -rf /tmp/rh-plugins-last; cp -r "$WORK" /tmp/rh-plugins-last 2>/dev/null
  rm -rf "$WORK"
}
trap cleanup EXIT
ok()   { PASS=$((PASS+1)); echo "PASS: $1"; }
bad()  { FAIL=$((FAIL+1)); echo "FAIL: $1"; }
check(){ [ "$1" = "0" ] && ok "$2" || bad "$2"; }

echo "== rabbithole 插件 E2E($WORK)=="
for pid in $(ps -eo pid,args | grep -E "rh-(client|server) --config /tmp/rh-plugins" | grep -v grep | awk '{print $1}'); do kill "$pid" 2>/dev/null; done

# ---------- 证书(插件 TLS 侧 + TLS 上游辅助) ----------
openssl req -x509 -newkey rsa:2048 -nodes -keyout "$WORK/plugin.key" -out "$WORK/plugin.crt" \
  -days 2 -subj "/CN=local.test" -addext "subjectAltName=DNS:local.test,IP:127.0.0.1" >/dev/null 2>&1
openssl req -x509 -newkey rsa:2048 -nodes -keyout "$WORK/up.key" -out "$WORK/up.crt" \
  -days 2 -subj "/CN=up.test" -addext "subjectAltName=DNS:up.test,IP:127.0.0.1" >/dev/null 2>&1

# ---------- 辅助服务(全部显式 close,避免依赖 GC 释放 fd) ----------
python3 - "$ECHO" <<'EOF' &
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
    c, _ = s.accept(); threading.Thread(target=echo, args=(c,), daemon=True).start()
EOF
PIDS+=($!)

# 请求头记录型 HTTP 上游(明文/TLS 同一实现;记录完整请求头到 $LOG)
python3 - "$WORK/hdr-plain.log" "$HDR" "plain" <<'EOF' &
import socket, ssl, threading, sys
LOG, PORT, MODE = sys.argv[1], int(sys.argv[2]), sys.argv[3]
s = socket.socket(); s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.bind(("127.0.0.1", PORT)); s.listen(64)
if MODE == "tls":
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.load_cert_chain(sys.argv[4], sys.argv[5])
    s = ctx.wrap_socket(s, server_side=True)
def serve(c):
    try:
        buf = b""
        while b"\r\n\r\n" not in buf:
            d = c.recv(65536)
            if not d: return
            buf += d
        with open(LOG, "ab") as f:
            f.write(b"---REQ---\n" + buf.split(b"\r\n\r\n", 1)[0] + b"\n")
        c.sendall(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nOK")
    except OSError: pass
    finally: c.close()
while True:
    c, _ = s.accept(); threading.Thread(target=serve, args=(c,), daemon=True).start()
EOF
PIDS+=($!)
python3 - "$WORK/hdr-tls.log" "$HDR_TLS" "tls" "$WORK/up.crt" "$WORK/up.key" <<'EOF' &
import socket, ssl, threading, sys
LOG, PORT, MODE = sys.argv[1], int(sys.argv[2]), sys.argv[3]
s = socket.socket(); s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.bind(("127.0.0.1", PORT)); s.listen(64)
if MODE == "tls":
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.load_cert_chain(sys.argv[4], sys.argv[5])
    s = ctx.wrap_socket(s, server_side=True)
def serve(c):
    try:
        buf = b""
        while b"\r\n\r\n" not in buf:
            d = c.recv(65536)
            if not d: return
            buf += d
        with open(LOG, "ab") as f:
            f.write(b"---REQ---\n" + buf.split(b"\r\n\r\n", 1)[0] + b"\n")
        c.sendall(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nOK")
    except OSError: pass
    finally: c.close()
while True:
    c, _ = s.accept(); threading.Thread(target=serve, args=(c,), daemon=True).start()
EOF
PIDS+=($!)

# ---------- server / client ----------
cat > "$WORK/server.toml" <<EOF
bind_addr = "127.0.0.1"
bind_protocol = "tcp"
bind_port = $CTL
multiplex = true
[web]
bind_addr = "127.0.0.1"
port = $API
[vhost]
http_port = 0
https_port = 0
tcpmux_port = 0
[ports]
auto_range = "30600-30700"
[log]
level = "info"
EOF
"$SERVER" --config "$WORK/server.toml" admin create admin Pl-123-456 >/dev/null 2>&1
"$SERVER" --config "$WORK/server.toml" >"$WORK/server.log" 2>&1 &
PIDS+=($!)
sleep 2
TOKEN=$(curl -s -X POST "http://127.0.0.1:$API/api/auth/login" -H 'Content-Type: application/json' -d '{"username":"admin","password":"Pl-123-456"}' | python3 -c 'import json,sys;print(json.load(sys.stdin)["data"]["accessToken"])')
api(){ curl -s -m 15 -H "Authorization: Bearer $TOKEN" "$@"; }
CR=$(api -X POST "http://127.0.0.1:$API/api/clients" -H 'Content-Type: application/json' -d '{"name":"main"}')
CID=$(echo "$CR"|python3 -c 'import json,sys;print(json.load(sys.stdin)["data"]["id"])')
SEC=$(echo "$CR"|python3 -c 'import json,sys;print(json.load(sys.stdin)["data"]["secret"])')
cat > "$WORK/client.toml" <<EOF
server_addr = "127.0.0.1:$CTL"
protocol = "tcp"
multiplex = true
client_id = "$CID"
secret = "$SEC"
log_level = "info"
EOF
"$CLIENT" --config "$WORK/client.toml" >"$WORK/client.log" 2>&1 &
PIDS+=($!)
for i in $(seq 1 20); do
  on=$(api "http://127.0.0.1:$API/api/clients" | python3 -c 'import json,sys;print(json.load(sys.stdin)["data"][0]["online"])' 2>/dev/null)
  [ "$on" = "True" ] && break; sleep 0.5
done
[ "$on" = "True" ]; check $? "客户端上线"

mk_svc(){ r=$(api -X POST "http://127.0.0.1:$API/api/services" -H 'Content-Type: application/json' -d "$1"); code=$(echo "$r"|python3 -c 'import json,sys;d=json.load(sys.stdin);print(d.get("code"))' 2>/dev/null); [ "$code" = "0" ] || echo "MK_SVC_FAIL: $r" >&2; }
wait_rule(){ # $1 name → 等 running
  for i in $(seq 1 12); do
    st=$(api "http://127.0.0.1:$API/api/services?client_id=$CID" | python3 -c "import json,sys;print([s['status'] for s in json.load(sys.stdin)['data'] if s['name']=='$1'][0])" 2>/dev/null)
    [ "$st" = "running" ] && return 0; sleep 0.5
  done
  return 1
}
HDRASSERT='import sys
log = open(sys.argv[1], "rb").read().decode("latin1")
last = log.rsplit("---REQ---\n", 1)[1] if "---REQ---" in log else ""
checks = sys.argv[2:]
ok_all = True
for i in range(0, len(checks), 2):
    key, want = checks[i], checks[i+1]
    if want.startswith("!"):
        found = key in last
        if key == "X-Del" or key == "Connection":
            # 头存在性检查:精确按行匹配
            found = any(l.startswith(key + ":") for l in last.split("\r\n"))
        if found: print("FAIL: %s should be absent/gone" % key); ok_all = False
    else:
        if key + ": " + want not in last:
            print("FAIL: %s: %s not found; head=%r" % (key, want, last)); ok_all = False
sys.exit(0 if ok_all else 1)'

# ---------- 1) http2http:头改写(Host 重写 + set + del) ----------
mk_svc "{\"client_id\":\"$CID\",\"name\":\"h2h\",\"kind\":\"tcp\",\"bind_port\":30511,\"target_host\":\"127.0.0.1\",\"target_port\":1,\"enabled\":true,\"plugin\":{\"kind\":\"http2_http\",\"local_addr\":\"127.0.0.1:$HDR\",\"host_rewrite\":\"rewritten.example\",\"request_headers\":{\"set\":[{\"name\":\"X-Rew\",\"value\":\"on\"}],\"del\":[\"X-Del\"]}}}"
wait_rule h2h
body=$(curl -s -m 8 -H "X-Del: yes" "http://127.0.0.1:30511/first")
[ "$body" = "OK" ]; check $? "http2http:响应回传($body)"
python3 -c "$HDRASSERT" "$WORK/hdr-plain.log" Host "rewritten.example" X-Rew "on" X-Del "!"; check $? "http2http:Host 重写+set+del 生效"

# ---------- 2) http2https:明文入 → TLS 上游 ----------
mk_svc "{\"client_id\":\"$CID\",\"name\":\"h2s\",\"kind\":\"tcp\",\"bind_port\":30512,\"target_host\":\"127.0.0.1\",\"target_port\":1,\"enabled\":true,\"plugin\":{\"kind\":\"http2_https\",\"local_addr\":\"127.0.0.1:$HDR_TLS\",\"host_rewrite\":\"secure.up.test\"}}"
wait_rule h2s
body=$(curl -s -m 8 "http://127.0.0.1:30512/second")
[ "$body" = "OK" ]; check $? "http2https:TLS 上游响应回传($body)"
python3 -c "$HDRASSERT" "$WORK/hdr-tls.log" Host "secure.up.test"; check $? "http2https:经 TLS 到达上游且 Host 已重写"

# ---------- 3) https2http:TLS 终结(插件证书)→ 明文上游 ----------
mk_svc "{\"client_id\":\"$CID\",\"name\":\"s2h\",\"kind\":\"tcp\",\"bind_port\":30513,\"target_host\":\"127.0.0.1\",\"target_port\":1,\"enabled\":true,\"plugin\":{\"kind\":\"https2_http\",\"local_addr\":\"127.0.0.1:$HDR\",\"crt_path\":\"$WORK/plugin.crt\",\"key_path\":\"$WORK/plugin.key\",\"host_rewrite\":\"term.example\",\"request_headers\":{\"set\":[{\"name\":\"X-Tls\",\"value\":\"terminated\"}]}}}"
wait_rule s2h
body=$(curl -sk -m 8 "https://127.0.0.1:30513/third")
[ "$body" = "OK" ]; check $? "https2http:TLS 入响应回传($body)"
python3 -c "$HDRASSERT" "$WORK/hdr-plain.log" Host "term.example" X-Tls "terminated"; check $? "https2http:TLS 终结+Host 重写+注入头"

# ---------- 4) https2https:TLS 终结 → TLS 上游 ----------
mk_svc "{\"client_id\":\"$CID\",\"name\":\"s2s\",\"kind\":\"tcp\",\"bind_port\":30514,\"target_host\":\"127.0.0.1\",\"target_port\":1,\"enabled\":true,\"plugin\":{\"kind\":\"https2_https\",\"local_addr\":\"127.0.0.1:$HDR_TLS\",\"crt_path\":\"$WORK/plugin.crt\",\"key_path\":\"$WORK/plugin.key\",\"host_rewrite\":\"both.example\"}}"
wait_rule s2s
body=$(curl -sk -m 8 "https://127.0.0.1:30514/fourth")
[ "$body" = "OK" ]; check $? "https2https:双 TLS 链路响应回传($body)"
python3 -c "$HDRASSERT" "$WORK/hdr-tls.log" Host "both.example"; check $? "https2https:上游收到重写后请求"

# ---------- 5) tls2raw:TLS 入 → 裸 echo 上游 ----------
mk_svc "{\"client_id\":\"$CID\",\"name\":\"t2r\",\"kind\":\"tcp\",\"bind_port\":30515,\"target_host\":\"127.0.0.1\",\"target_port\":1,\"enabled\":true,\"plugin\":{\"kind\":\"tls2_raw\",\"local_addr\":\"127.0.0.1:$ECHO\",\"crt_path\":\"$WORK/plugin.crt\",\"key_path\":\"$WORK/plugin.key\"}}"
wait_rule t2r
out=$(python3 - <<'EOF2'
import socket, ssl
try:
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT); ctx.check_hostname = False; ctx.verify_mode = ssl.CERT_NONE
    s = socket.create_connection(("127.0.0.1", 30515), timeout=10)
    t = ctx.wrap_socket(s)
    t.sendall(b"tls2raw-ping\n"); t.settimeout(8)
    print(t.recv(64).decode())
except Exception as e:
    print("ERR", e)
EOF2
)
[ "$out" = "tls2raw-ping" ]; check $? "tls2raw:TLS 解密后裸转发回显($out)"

# ---------- 6) http_proxy:插件自己扮演代理服务端,目的地来自用户请求 ----------
#
# 语义要点(见 docs/pending-plugin-proxy-semantics.md 的结论):
# 插件不是「拨号器」,而是「客户端进程内的代理服务端」。用户经隧道发来的
# CONNECT 里写的才是目的地,插件解析后自己去拨。
#
# 因此规则**不再需要 target 指向代理**:target_host/target_port 对这两个
# 插件无意义,这里故意填一个不可达地址,任何「拨配置目标」的实现都会失败。
HPX_DEAD_TARGET='"target_host":"127.0.0.1","target_port":1'
mk_svc "{\"client_id\":\"$CID\",\"name\":\"hpx\",\"kind\":\"tcp\",\"bind_port\":30516,$HPX_DEAD_TARGET,\"enabled\":true,\"plugin\":{\"kind\":\"http_proxy\",\"http_user\":\"pu\",\"http_password\":\"pp\"}}"
wait_rule hpx
hpx_out=$(python3 - "$ECHO" <<'EOF'
import socket, sys, base64
echo = int(sys.argv[1])
try:
    s = socket.create_connection(("127.0.0.1", 30516), timeout=10)
    cred = base64.b64encode(b"pu:pp").decode()
    # 目的地是 echo(由用户请求指定),不是配置里的 target
    s.sendall(("CONNECT 127.0.0.1:%d HTTP/1.1\r\nHost: ignored\r\n"
               "Proxy-Authorization: Basic %s\r\n\r\n" % (echo, cred)).encode())
    s.settimeout(8)
    buf = b""
    while b"\r\n\r\n" not in buf:
        d = s.recv(4096)
        if not d: break
        buf += d
    status = buf.split(b"\r\n", 1)[0].decode()
    if " 200" not in status:
        print("STATUS:" + status); sys.exit(0)
    s.sendall(b"hpx-payload\n")
    print(s.recv(64).decode())
except Exception as e:
    print("ERR", e)
EOF
)
[ "$hpx_out" = "hpx-payload" ]; check $? "http_proxy:CONNECT 到用户请求的目的地并回显($hpx_out)"

# 负向:错误凭据 → 407 + Proxy-Authenticate(而不是静默断开)
mk_svc "{\"client_id\":\"$CID\",\"name\":\"hpxbad\",\"kind\":\"tcp\",\"bind_port\":30517,$HPX_DEAD_TARGET,\"enabled\":true,\"plugin\":{\"kind\":\"http_proxy\",\"http_user\":\"pu\",\"http_password\":\"pp\"}}"
wait_rule hpxbad
hpx_neg=$(python3 - "$ECHO" <<'EOF'
import socket, sys, base64
echo = int(sys.argv[1])
try:
    s = socket.create_connection(("127.0.0.1", 30517), timeout=10)
    cred = base64.b64encode(b"pu:WRONG").decode()
    s.sendall(("CONNECT 127.0.0.1:%d HTTP/1.1\r\nProxy-Authorization: Basic %s\r\n\r\n"
               % (echo, cred)).encode())
    s.settimeout(8)
    buf = b""
    while b"\r\n\r\n" not in buf:
        d = s.recv(4096)
        if not d: break
        buf += d
    head = buf.decode("latin1")
    ok = " 407 " in head.split("\r\n", 1)[0] and "proxy-authenticate:" in head.lower()
    print("407-OK" if ok else "BAD:" + head.replace("\r\n", "|"))
except Exception as e:
    print("ERR", e)
EOF
)
[ "$hpx_neg" = "407-OK" ]; check $? "http_proxy:错误凭据回 407 且带 Proxy-Authenticate($hpx_neg)"

# 负向:目的地不可达 → 502(不能把用户连接挂死)
hpx_502=$(python3 - <<'EOF'
import socket, base64
try:
    s = socket.create_connection(("127.0.0.1", 30516), timeout=10)
    cred = base64.b64encode(b"pu:pp").decode()
    # 端口 1 几乎必然无人监听;凭据必须正确,否则先被 407 挡下
    s.sendall(("CONNECT 127.0.0.1:1 HTTP/1.1\r\nProxy-Authorization: Basic %s\r\n\r\n" % cred).encode())
    s.settimeout(8)
    d = s.recv(256)
    print("502" if b" 502 " in d.split(b"\r\n", 1)[0] else "BAD:" + d.decode("latin1", "replace"))
except Exception as e:
    print("ERR", e)
EOF
)
[ "$hpx_502" = "502" ]; check $? "http_proxy:目的地不可达回 502($hpx_502)"

# ---------- 7) socks5:方法协商 + 用户密码 + 用户指定的目的地 + 隧道 ----------
mk_svc "{\"client_id\":\"$CID\",\"name\":\"s5\",\"kind\":\"tcp\",\"bind_port\":30518,$HPX_DEAD_TARGET,\"enabled\":true,\"plugin\":{\"kind\":\"socks5\",\"user\":\"su\",\"password\":\"sp\"}}"
wait_rule s5
s5_out=$(python3 - "$ECHO" <<'EOF'
import socket, sys
echo = int(sys.argv[1])
try:
    s = socket.create_connection(("127.0.0.1", 30518), timeout=10)
    s.settimeout(8)
    s.sendall(b"\x05\x01\x02")            # 只提供用户名密码
    assert s.recv(2) == b"\x05\x02", "应选 0x02"
    s.sendall(b"\x01\x02su\x02sp")        # RFC1929
    assert s.recv(2) == b"\x01\x00", "认证应成功"
    # CONNECT 到 echo(用户指定),不是配置里的 target
    s.sendall(b"\x05\x01\x00\x01" + socket.inet_aton("127.0.0.1") + echo.to_bytes(2, "big"))
    rep = s.recv(10)
    if rep[1] != 0:
        print("REP:%d" % rep[1]); sys.exit(0)
    s.sendall(b"socks-payload\n")
    print(s.recv(64).decode())
except Exception as e:
    print("ERR", e)
EOF
)
[ "$s5_out" = "socks-payload" ]; check $? "socks5:CONNECT 到用户请求的目的地并回显($s5_out)"

# 负向:错误凭据 → RFC1929 的 0x01 0x01,且不再接受请求
s5_neg=$(python3 - <<'EOF'
import socket
try:
    s = socket.create_connection(("127.0.0.1", 30518), timeout=10)
    s.settimeout(8)
    s.sendall(b"\x05\x01\x02")
    s.recv(2)
    s.sendall(b"\x01\x02su\x03BAD")
    print("AUTHFAIL" if s.recv(2) == b"\x01\x01" else "BAD")
except Exception as e:
    print("ERR", e)
EOF
)
[ "$s5_neg" = "AUTHFAIL" ]; check $? "socks5:错误凭据回 0x01 0x01($s5_neg)"

# 负向:非 CONNECT 命令(BIND)→ REP=0x07
s5_cmd=$(python3 - "$ECHO" <<'EOF'
import socket, sys
echo = int(sys.argv[1])
try:
    s = socket.create_connection(("127.0.0.1", 30518), timeout=10)
    s.settimeout(8)
    s.sendall(b"\x05\x01\x02")
    s.recv(2)
    s.sendall(b"\x01\x02su\x02sp")
    s.recv(2)
    s.sendall(b"\x05\x02\x00\x01" + socket.inet_aton("127.0.0.1") + echo.to_bytes(2, "big"))
    print("REP=%d" % s.recv(10)[1])
except Exception as e:
    print("ERR", e)
EOF
)
[ "$s5_cmd" = "REP=7" ]; check $? "socks5:非 CONNECT 命令回 REP=0x07($s5_cmd)"

echo
echo "== 结果: PASS=$PASS FAIL=$FAIL =="
[ "$FAIL" = "0" ]
