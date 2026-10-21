#!/usr/bin/env bash
# rabbithole benchmark —— vhost 专项基准(rh vs frp,控制面 tcp / tls 两档)
#
# 与 peer.sh / peer-frp.sh 的关系:
#   peer.sh      测 TCP/UDP 转发面 + 16 连接明文 HTTP QPS(覆盖率低)
#   peer-frp.sh  同一套场景,被测物换成 frp
#   本脚本       只测 vhost,但把两个维度**正交拆开**:
#
#     维度 1(控制面):PROTO = tcp | tls
#     维度 2(vhost):  HTTP vhost(明文) | HTTPS vhost(SNI 透传)
#
# 为什么必须单开:既有 http 场景是 16 连接 × 8s 的明文 QPS 一档,
#   - 测不到大响应体吞吐(源站 target.py 固定 4 KiB)
#   - 完全没有 HTTPS vhost(path)
#   - 无法把「控制面 tls 开销」与「vhost https 开销」分开归因
# 见 benchmark/vhostbench/main.go 顶部注释。
#
# 拓扑与端口沿用 peer.sh(同机、同网段、同目标机),保证与历史轮可比:
#
#   [test3: 压测端]         [test1: 服务端]           [test2: 客户端+源站]
#   vhostbench qps/tput ──► A:20004 (http vhost)  ──隧道──► B:19104 (明文源站)
#                          A:20005 (https vhost) ──隧道──► B:19105 (TLS 源站)
#                          A:17100 控制面(tcp/tls)
#
# 角色(与 peer.sh 一致,由运行者用自己的通道投放):
#   vhost-peer.sh server                        # A(test1)
#   vhost-peer.sh client <A_ip:ctl> <id> <secret>   # B(test2)
#   vhost-peer.sh drive  <A_ip> <B_ip> <api> <token> <cid> [label]   # test3
#
# frp 侧同角色,只需 VHOST_IMPL=frp:
#   VHOST_IMPL=frp vhost-peer.sh server | client <A_IP> | drive <A_IP> <B_IP> x
#
# 环境变量(除 label 外,drive 侧都必须显式):
#   VHOST_IMPL  rh | frp           被测实现,决定 server/client 起什么进程
#   PROTO       tcp | tls          控制面协议
#   THP_TOOL    loadgen(固定用 Go 压测器)
#   VHOST_RUN_ID                   本轮 run id(证据溯源)
#
# 判定口径(与 peer.sh 一致):
#   直连(B:19104/19105)= 标尺,隧道(A:20004/20005)= 被测。
#   唯一有意义的是**保留率 = 隧道 / 直连**;绝对值跨环境不可比。

set -uo pipefail
BENCH_SELF_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$BENCH_SELF_DIR/lib/common.sh"

PEER_DIR="${PEER_DIR:-/tmp/vhost-peer}"
VHOST_IMPL="${VHOST_IMPL:-rh}"
PROTO="${PROTO:-tcp}"
VHOST_RUN_ID="${VHOST_RUN_ID:-}"
RELAY="${RELAY:-splice}"
MUX="${MUX:-1}"
PEER_RUN_ID="${VHOST_RUN_ID:-$PEER_RUN_ID}"

VHOST_BIN="${VHOST_BIN:-/tmp/rh-bin/vhostbench}"

CTL_PORT="${CTL_PORT:-17100}"
API_PORT="${API_PORT:-18080}"
KCP_PORT="${KCP_PORT:-17101}"
QUIC_PORT="${QUIC_PORT:-17102}"
PROXY_BASE="${PROXY_BASE:-20001}"
# vhost 端口:紧接 peer.sh 的 20004 之后另开 20005 给 HTTPS,避免抢占用
P_VHOST_HTTP="${P_VHOST_HTTP:-20004}"
P_VHOST_HTTPS="${P_VHOST_HTTPS:-20005}"
T_HTTP="${T_HTTP:-19104}"
T_HTTPS="${T_HTTPS:-19105}"
# TLS 大响应体源站:tput-tls 必须打它。
# 踩过:一开始 tput-tls 打的是 T_HTTPS(4 KiB 源站),直连只有 0.3~0.45 Gbps,
# 而明文 tput 打 4 MiB 源站有 2.9 Gbps —— 两个数根本不可比,若照此出数
# 会得出「HTTPS vhost 吞吐只有明文 1/8」的错误结论。
T_HTTPS_BIG="${T_HTTPS_BIG:-19107}"

VHOST_DOMAIN="${VHOST_DOMAIN:-bench.test}"
# 压测参数(可覆盖)。默认值按「样本量足够/单轮可接受耗时」折中选定:
#   QPS:   16 连接 × 10s(与 peer.sh 的 16×8s 同量级,便于对照历史)
#   tput:   8 连接 × 10s,响应体 4 MiB(足够大,受带宽而非 QPS 约束)
#   lat:   5000 往返(与 peer.sh 一致)
#   conc:  64/256/1024/4096(与 peer.sh 的档位一致)
VHOST_QPS_CONNS="${VHOST_QPS_CONNS:-16}"
VHOST_QPS_DUR="${VHOST_QPS_DUR:-10}"
VHOST_TPUT_CONNS="${VHOST_TPUT_CONNS:-8}"
VHOST_TPUT_DUR="${VHOST_TPUT_DUR:-10}"
VHOST_TPUT_BODY="${VHOST_TPUT_BODY:-4194304}"
VHOST_LAT_COUNT="${VHOST_LAT_COUNT:-5000}"
VHOST_CONC_LEVELS="${VHOST_CONC_LEVELS:-64 256 1024 4096}"
VHOST_CONC_HOLD="${VHOST_CONC_HOLD:-5}"
VHOST_CONC_REPEAT="${VHOST_CONC_REPEAT:-3}"

export VHOST_IMPL PROTO RELAY MUX PEER_RUN_ID

conc_fd_need() {
  local max=0 n
  for n in $VHOST_CONC_LEVELS; do [ "$n" -gt "$max" ] && max="$n"; done
  echo $((max + 512))
}

# ---------- 从 peer.sh 借用的两个助手(common.sh 里没有,必须自带) ----------
# start_cpu_sampler <pid> <out> —— /proc CPU+RSS 采样器(仅 Linux)。
# 采样失败是环境错误,必须可见并中止角色:CPU 证据缺失时数据无法复核。
start_cpu_sampler() {
  local pid="$1" out="$2"
  if [ ! -d /proc ]; then
    # 非 Linux(如 macOS 本机冒烟):CPU 证据不可能产生。只有显式声明
    # 冒烟模式才放行,且必须把结论的不可用性喊出来 —— 这种轮次绝不可
    # 用于任何性能结论。
    if [ "${VHOST_SMOKE:-0}" = "1" ]; then
      warn "VHOST_SMOKE=1 且无 /proc:跳过 CPU 采样(本轮仅供连通性冒烟,不可用于性能结论)"
      return 0
    fi
    err "本平台无 /proc,CPU 证据只能产于 Linux 验收机(fail closed)"
    return 1
  fi
  python3 "$BENCH_PY/cpu_sampler.py" --pid "$pid" --out "$out" >"$out.boot" 2>&1 &
  echo $! > "$out.pid"
  sleep 1
  if ! kill -0 "$(cat "$out.pid")" 2>/dev/null; then
    err "CPU 采样器启动失败(CPU 证据是本基准的硬性要求):"
    cat "$out.boot" >&2
    return 1
  fi
  info "CPU 采样器已启动: pid=$(cat "$out.pid") out=$out"
}

# drive 侧记录助手(与 peer.sh 同名同语义)
FAILS=0
RAW_DIR=""
REC_OUT_FILE=""
# FAILS_FILE:run_one 由 $(...) 调用,**运行在子 shell 里** —— 子 shell 对
# FAILS 的递增在父 shell 不可见(踩过:整轮大量无效样本,末尾仍打印
# "无效样本: 0")。改用文件累加,父 shell 事后读取。
FAILS_FILE=""

# rec 只接受非空 JSON 行:组装失败必须可见并计数,不能静默丢样本
rec() {
  if [ -n "$1" ]; then
    echo "$1" >> "$REC_OUT_FILE"
  else
    err "rec: 空 JSON 行(记录组装失败,计入 FAILS)"
    FAILS=$((FAILS+1))
  fi
}

# iso_now —— 跨平台 ISO-8601(GNU date 的 -I 在 macOS 不可用)
iso_now() { date -u +%Y-%m-%dT%H:%M:%SZ; }


vhost_server_config() {
  local dir="$1"
  gen_server_config "$dir" "$CTL_PORT" "$API_PORT" "$PROTO" "relay_mode = \"$RELAY\""
  python3 - "$dir/server.toml" "$P_VHOST_HTTP" "$P_VHOST_HTTPS" "$VHOST_DOMAIN" <<'PY'
import sys
path, http_p, https_p, domain = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]
txt = open(path).read()
txt = txt.replace("http_port = 0", f"http_port = {http_p}")
txt = txt.replace("https_port = 0", f"https_port = {https_p}")
if "[ports]" in txt:
    txt = txt.replace("[ports]", f'subdomain_host = "{domain}"\n\n[ports]')
open(path, "w").write(txt)
PY
}

# ============================================================
# 角色 1:server
# ============================================================
vhost_server() {
  require_run_env server
  raise_nofile 65535
  check_nofile server "$(conc_fd_need)" || return 1
  [ "$VHOST_IMPL" = "rh" ] && resolve_binaries
  mkdir -p "$PEER_DIR"

  if [ "$VHOST_IMPL" = "frp" ]; then
    vhost_frp_server
    return
  fi

  vhost_server_config "$PEER_DIR"
  "$BENCH_BIN/rh-server" --config "$PEER_DIR/server.toml" admin create admin peer-Pass-1 >/dev/null 2>&1
  "$BENCH_BIN/rh-server" --config "$PEER_DIR/server.toml" >"$PEER_DIR/server.log" 2>&1 &
  echo $! > "$PEER_DIR/server.pid"
  record_identity rh-server "$(cat "$PEER_DIR/server.pid")" "$BENCH_BIN/rh-server" >/dev/null
  start_cpu_sampler "$(cat "$PEER_DIR/server.pid")" "$PEER_DIR/cpu.jsonl" || return 1
  wait_port 127.0.0.1 "$API_PORT" 20 || { err "server 未就绪"; cat "$PEER_DIR/server.log"; return 1; }
  wait_port 127.0.0.1 "$P_VHOST_HTTP" 10 || { err "http vhost 未就绪"; return 1; }
  wait_port 127.0.0.1 "$P_VHOST_HTTPS" 10 || { err "https vhost 未就绪"; return 1; }

  local token
  token="$(api_login "http://127.0.0.1:$API_PORT" admin peer-Pass-1)"
  [ -n "$token" ] || { err "登录失败"; return 1; }
  read -r cid secret < <(create_client "http://127.0.0.1:$API_PORT" "$token" peer)
  printf '%s\n' "$token"  > "$PEER_DIR/token"
  printf '%s\n' "$cid"    > "$PEER_DIR/client_id"
  printf '%s\n' "$secret" > "$PEER_DIR/client_secret"

  write_endpoint_evidence server "$PEER_DIR/server.toml" "$PEER_DIR/identity.jsonl" >/dev/null

  hdr "A 侧就绪 (impl=rh proto=$PROTO)"
  log "  http vhost :$P_VHOST_HTTP   https vhost :$P_VHOST_HTTPS"
  log "  ── 在 B 上执行 ──"
  log "  VHOST_IMPL=rh PROTO=$PROTO VHOST_RUN_ID=$PEER_RUN_ID vhost-peer.sh client <A_IP>:$CTL_PORT $cid $secret"
}

vhost_frp_server() {
  resolve_frp_bin
  cat > "$PEER_DIR/frps.toml" <<EOF
bindAddr = "0.0.0.0"
bindPort = $CTL_PORT
kcpBindPort = $KCP_PORT
quicBindPort = $QUIC_PORT
vhostHTTPPort = $P_VHOST_HTTP
vhostHTTPSPort = $P_VHOST_HTTPS

auth.token = "$BENCH_TOKEN"

log.level = "warn"
EOF
  "$FRP_BIN/frps" -c "$PEER_DIR/frps.toml" >"$PEER_DIR/frps.log" 2>&1 &
  echo $! > "$PEER_DIR/frps.pid"
  wait_port 127.0.0.1 "$CTL_PORT" 20 || { err "frps 未就绪"; cat "$PEER_DIR/frps.log"; return 1; }
  wait_port 127.0.0.1 "$P_VHOST_HTTP" 10 || { err "frps http vhost 未就绪"; return 1; }
  wait_port 127.0.0.1 "$P_VHOST_HTTPS" 10 || { err "frps https vhost 未就绪"; return 1; }

  hdr "A 侧就绪 (impl=frp proto=$PROTO)"
  log "  http vhost :$P_VHOST_HTTP   https vhost :$P_VHOST_HTTPS"
  log "  ── 在 B 上执行 ──"
  log "  VHOST_IMPL=frp PROTO=$PROTO vhost-peer.sh client <A_IP>"
}

# ============================================================
# 角色 2:client + 源站
# ============================================================
vhost_client() {  # <a_addr> <cid> <secret>
  local a_addr="${1:-}" cid="${2:-}" secret="${3:-}"
  require_run_env client
  raise_nofile 65535
  check_nofile client "$(conc_fd_need)" || return 1
  # 只有 rh 需要本仓库二进制;frp 侧自己解析 FRP_BIN(否则会强制要求
  # BENCH_BIN 下的 rh-server/rh-client,frp 轮根本用不到)
  [ "$VHOST_IMPL" = "rh" ] && resolve_binaries
  mkdir -p "$PEER_DIR"

  [ -x "$VHOST_BIN" ] || die "vhostbench 不可执行: $VHOST_BIN"

  # 两个源站:明文 4 KiB(QPS 场景)与明文 4 MiB(吞吐场景)共用 19104,
  # 这里按场景切换太麻烦 —— 直接让源站按 Content-Length 之外的路径区分:
  #   /        → 4 KiB(默认,测 QPS)
  #   /big     → 4 MiB(测吞吐)
  # serve 不支持路径区分,故起**两个**源站进程,端口不同。
  #   19104 = 4 KiB 源站(明文,QPS/lat/conc 用)
  #   19106 = 4 MiB 源站(明文,tput 用)
  #   19105 = 4 KiB TLS 源站(HTTPS vhost 用)
  # 端口 19106 在 README 的规划之外,但只在 B 本机,tunnel 侧不暴露。
  "$VHOST_BIN" serve --port "$T_HTTP"  --body 4096      >"$PEER_DIR/srv-http.log" 2>&1 &
  echo $! > "$PEER_DIR/srv-http.pid"
  "$VHOST_BIN" serve --port "$T_HTTPS" --body 4096 --tls >"$PEER_DIR/srv-https.log" 2>&1 &
  echo $! > "$PEER_DIR/srv-https.pid"
  local t_big="${T_BIG:-19106}"
  "$VHOST_BIN" serve --port "$t_big"   --body "$VHOST_TPUT_BODY" >"$PEER_DIR/srv-big.log" 2>&1 &
  echo $! > "$PEER_DIR/srv-big.pid"
  # TLS + 大响应体:tput-tls 的直连基线必须是它,否则与明文 tput 不可比
  "$VHOST_BIN" serve --port "$T_HTTPS_BIG" --body "$VHOST_TPUT_BODY" --tls >"$PEER_DIR/srv-https-big.log" 2>&1 &
  echo $! > "$PEER_DIR/srv-https-big.pid"
  for p in "$T_HTTP" "$T_HTTPS" "$t_big" "$T_HTTPS_BIG"; do
    wait_port 127.0.0.1 "$p" 15 || { err "源站 $p 未就绪"; ls "$PEER_DIR"/srv-*.log | xargs -r tail -5; return 1; }
  done
  # ★端口归属校验:wait_port 只证明"有人在这个端口上听",不证明是**本次**
  # 启动的 vhostbench。踩过两次:上一轮遗留的 python3 target.py / 旧版
  # vhostbench 占着 19104,新源站 bind 失败静默退出,于是 QPS 档实际测的是
  # 一个 Python HTTP 服务(p50 8ms,把明文吞吐压低十倍),而日志里的
  # "address already in use" 没人看。绑定失败必须当场失败,不能带病出数。
  local port pid errlog
  for port in "$T_HTTP" "$T_HTTPS" "$t_big" "$T_HTTPS_BIG"; do
    case "$port" in
      "$T_HTTP")      pid="$(cat "$PEER_DIR/srv-http.pid")";       errlog="$PEER_DIR/srv-http.log" ;;
      "$T_HTTPS")     pid="$(cat "$PEER_DIR/srv-https.pid")";      errlog="$PEER_DIR/srv-https.log" ;;
      "$t_big")       pid="$(cat "$PEER_DIR/srv-big.pid")";        errlog="$PEER_DIR/srv-big.log" ;;
      *)              pid="$(cat "$PEER_DIR/srv-https-big.pid")";  errlog="$PEER_DIR/srv-https-big.log" ;;
    esac
    kill -0 "$pid" 2>/dev/null || {
      err "源站 $port 进程已退出(bind 失败?)"; tail -5 "$errlog" >&2; return 1; }
  done
  # 监听者必须是 vhostbench:用 ss 交叉确认,防止"端口被别的进程占着"
  if command -v ss >/dev/null 2>&1; then
    for port in "$T_HTTP" "$T_HTTPS" "$t_big" "$T_HTTPS_BIG"; do
      local owner
      owner="$(ss -ltnp 2>/dev/null | awk -v p=":$port" '$4 ~ (p"$"){print $0}' | grep -o 'vhostbench' | head -1)"
      [ "$owner" = "vhostbench" ] || {
        err "端口 $port 的监听者不是 vhostbench,拒绝用这个源站出数:"
        ss -ltnp 2>/dev/null | awk -v p=":$port" '$4 ~ (p"$")' >&2
        return 1; }
    done
  fi
  info "源站就绪: $T_HTTP(4KiB) $T_HTTPS(4KiB+TLS) $t_big($VHOST_TPUT_BODY B) $T_HTTPS_BIG($VHOST_TPUT_BODY B+TLS)"

  if [ "$VHOST_IMPL" = "frp" ]; then
    vhost_frp_client "$a_addr"
    return
  fi

  gen_client_config "$PEER_DIR" "$a_addr" "$PROTO" "$cid" "$secret" "relay_mode = \"$RELAY\""
  "$BENCH_BIN/rh-client" --config "$PEER_DIR/client.toml" >"$PEER_DIR/client.log" 2>&1 &
  echo $! > "$PEER_DIR/client.pid"
  record_identity rh-client "$(cat "$PEER_DIR/client.pid")" "$BENCH_BIN/rh-client" >/dev/null
  start_cpu_sampler "$(cat "$PEER_DIR/client.pid")" "$PEER_DIR/cpu.jsonl" || return 1
  sleep 3
  write_endpoint_evidence client "$PEER_DIR/client.toml" "$PEER_DIR/identity.jsonl" >/dev/null

  hdr "B 侧就绪 (impl=rh proto=$PROTO)"
  log "  ── 在 test3 上执行 ──"
  log "  VHOST_IMPL=rh PROTO=$PROTO VHOST_RUN_ID=$PEER_RUN_ID vhost-peer.sh drive <A_IP> <B_IP> $API_PORT <token> $cid <label>"
}

vhost_frp_client() {  # <a_ip>
  local a_ip="$1"
  resolve_frp_bin
  local transport port="$CTL_PORT"
  local proto_line='transport.protocol = "tcp"'
  case "$PROTO" in
    tcp) ;;
    tls) proto_line="$proto_line"$'\n''transport.tls.enable = true' ;;
    quic) port="$QUIC_PORT"; proto_line='transport.protocol = "quic"' ;;
    kcp) port="$KCP_PORT"; proto_line='transport.protocol = "kcp"' ;;
    *) die "frp 侧不支持 PROTO=$PROTO(tcp/tls/quic/kcp)" ;;
  esac
  local t_big="${T_BIG:-19106}"
  cat > "$PEER_DIR/frpc.toml" <<EOF
serverAddr = "$a_ip"
serverPort = $port
auth.token = "$BENCH_TOKEN"

$proto_line
transport.poolCount = 4
transport.tcpMux = true

log.level = "warn"

[[proxies]]
name = "web-http"
type = "http"
localIP = "127.0.0.1"
localPort = $T_HTTP
customDomains = ["$VHOST_DOMAIN"]

[[proxies]]
name = "web-https"
type = "https"
localIP = "127.0.0.1"
localPort = $T_HTTPS
customDomains = ["$VHOST_DOMAIN"]

[[proxies]]
name = "web-https-big"
type = "https"
localIP = "127.0.0.1"
localPort = $T_HTTPS_BIG
customDomains = ["big.$VHOST_DOMAIN"]

[[proxies]]
name = "web-big"
type = "http"
localIP = "127.0.0.1"
localPort = $t_big
customDomains = ["big.$VHOST_DOMAIN"]
EOF
  "$FRP_BIN/frpc" -c "$PEER_DIR/frpc.toml" >"$PEER_DIR/frpc.log" 2>&1 &
  echo $! > "$PEER_DIR/frpc.pid"
  sleep 3
  hdr "B 侧就绪 (impl=frp proto=$PROTO)"
  log "  ── 在 test3 上执行 ──"
  log "  VHOST_IMPL=frp PROTO=$PROTO vhost-peer.sh drive <A_IP> <B_IP> x <label>"
}

# ============================================================
# 角色 3:drive
# ============================================================
vhost_drive() {  # <a_ip> <b_ip> <api_port> <token> <cid> [label]
  local a_ip="$1" b_ip="$2" api_port="$3" token="${4:-x}" cid="${5:-x}" label="${6:-vhost}"
  require_run_env drive
  case "$label" in *[!a-zA-Z0-9._-]*) die "label 含非法字符: $label";; esac
  raise_nofile 65535
  check_nofile drive "$(conc_fd_need)" || return 1
  [ -x "$VHOST_BIN" ] || die "vhostbench 不可执行: $VHOST_BIN"
  mkdir -p "$PEER_DIR"

  local API="http://$a_ip:$api_port"
  local stamp out_file
  stamp="$(date +%Y%m%d-%H%M%S)"
  out_file="$PEER_DIR/results-$label-$stamp.jsonl"
  : > "$out_file"
  REC_OUT_FILE="$out_file"
  RAW_DIR="$PEER_DIR/raw"
  mkdir -p "$RAW_DIR"
  FAILS=0
  FAILS_FILE="$PEER_DIR/fails-$label-$stamp.txt"
  : > "$FAILS_FILE"

  printf '{"schema":"vhost-evidence","version":1,"impl":"%s","proto":"%s","run_id":"%s"}\n' \
    "$VHOST_IMPL" "$PROTO" "$PEER_RUN_ID" > "$PEER_DIR/schema.json"

  # ---------- 元数据 ----------
  local impl_ver="unknown"
  if [ "$VHOST_IMPL" = "rh" ]; then
    impl_ver="$(curl -s -m 10 -H "Authorization: Bearer $token" "$API/api/clients" \
      | jq -r --arg id "$cid" '.data[] | select(.id==$id) | .client_version // ""' 2>/dev/null)"
    if [ -z "$impl_ver" ]; then
      rec "$(jq -cn --arg run_id "$PEER_RUN_ID" \
        '{metric:"meta",env_error:"client_version 为空(客户端未上报/不在线),中止本轮"}')"
      err "client_version 为空,中止本轮(token 可能已过期)"
      return 1
    fi
  else
    # frp 侧解析二进制只为取版本号;取不到就如实记 unknown,不要静默编造
    if resolve_frp_bin 2>/dev/null; then
      impl_ver="$("$FRP_BIN/frpc" --version 2>/dev/null | head -1)"
    fi
    [ -n "$impl_ver" ] || impl_ver="frp(version unknown)"
  fi
  rec "$(jq -cn --arg run_id "$PEER_RUN_ID" --arg lbl "$label" --arg impl "$VHOST_IMPL" \
    --arg proto "$PROTO" --arg ver "$impl_ver" --arg started "$(iso_now)" \
    --argjson qc "$VHOST_QPS_CONNS" --argjson qd "$VHOST_QPS_DUR" \
    --argjson tc "$VHOST_TPUT_CONNS" --argjson td "$VHOST_TPUT_DUR" \
    --argjson tb "$VHOST_TPUT_BODY" --argjson lc "$VHOST_LAT_COUNT" \
    --arg conc "$VHOST_CONC_LEVELS" --argjson chr "$VHOST_CONC_REPEAT" \
    '{metric:"meta",schema_version:1,impl:$impl,proto:$proto,relay:env.RELAY,mux:env.MUX,
      run_id:$run_id,"label":$lbl,impl_version:$ver,
      qps_conns:$qc,qps_dur:$qd,tput_conns:$tc,tput_dur:$td,tput_body:$tb,
      lat_count:$lc,conc_levels:($conc|split(" ")|map(tonumber)),conc_repeat:$chr,
      started_at:$started}')"
  info "impl=$VHOST_IMPL ver=$impl_ver proto=$PROTO"

  # ---------- rh:下发 vhost 规则 ----------
  if [ "$VHOST_IMPL" = "rh" ]; then
    hdr "下发 vhost 规则"
    local old_ids
    old_ids="$(curl -s -m 10 -H "Authorization: Bearer $token" "$API/api/services" \
      | jq -r --arg cid "$cid" '.data[] | select(.client_id==$cid) | .id' 2>/dev/null)"
    for sid in $old_ids; do
      curl -s -m 10 -X DELETE -H "Authorization: Bearer $token" "$API/api/services/$sid" >/dev/null
    done
    [ -n "$old_ids" ] && sleep 2

    local t_big="${T_BIG:-19106}"
    # http vhost:QPS/延迟/并发都打这个
    create_service "$API" "$token" \
      "{\"client_id\":\"$cid\",\"name\":\"vh-http\",\"kind\":\"http\",\"target_host\":\"127.0.0.1\",\"target_port\":$T_HTTP,\"http\":{\"custom_domains\":[\"$VHOST_DOMAIN\"]},\"enabled\":true}" >/dev/null \
      || { err "http 规则失败"; return 1; }
    # http vhost:大响应体吞吐(独立域名,指向 4 MiB 源站)
    create_service "$API" "$token" \
      "{\"client_id\":\"$cid\",\"name\":\"vh-big\",\"kind\":\"http\",\"target_host\":\"127.0.0.1\",\"target_port\":$t_big,\"http\":{\"custom_domains\":[\"big.$VHOST_DOMAIN\"]},\"enabled\":true}" >/dev/null \
      || { err "big 规则失败"; return 1; }
    # https vhost:SNI 透传到 B 的 TLS 源站(rh 未上传证书 → passthrough,
    # 与 frp 的 https 代理语义一致)
    create_service "$API" "$token" \
      "{\"client_id\":\"$cid\",\"name\":\"vh-https\",\"kind\":\"https\",\"target_host\":\"127.0.0.1\",\"target_port\":$T_HTTPS,\"https\":{\"custom_domains\":[\"$VHOST_DOMAIN\"]},\"enabled\":true}" >/dev/null \
      || { err "https 规则失败"; return 1; }
    # https vhost 的吞吐口径:SNI 透传到 TLS + 4 MiB 源站。
    # 域名必须与明文 tput 一致(big.$VHOST_DOMAIN),否则 vhost 路由找不到。
    create_service "$API" "$token" \
      "{\"client_id\":\"$cid\",\"name\":\"vh-https-big\",\"kind\":\"https\",\"target_host\":\"127.0.0.1\",\"target_port\":$T_HTTPS_BIG,\"https\":{\"custom_domains\":[\"big.$VHOST_DOMAIN\"]},\"enabled\":true}" >/dev/null \
      || { err "https big 规则失败"; return 1; }
  fi

  # ---------- 就绪门控 ----------
  hdr "就绪门控"
  local gate_ok=0 i
  for i in $(seq 1 40); do
    if "$VHOST_BIN" lat --addr "$a_ip:$P_VHOST_HTTP" --host "$VHOST_DOMAIN" \
         --count 3 --warmup 1 --timeout 3s >/dev/null 2>&1; then
      gate_ok=1; break
    fi
    sleep 1
  done
  [ "$gate_ok" = "1" ] || { err "http vhost 40s 内未就绪"; return 1; }
  info "http vhost 就绪"

  local gate_https=0
  for i in $(seq 1 40); do
    if "$VHOST_BIN" lat --addr "$a_ip:$P_VHOST_HTTPS" --tls --sni "$VHOST_DOMAIN" \
         --host "$VHOST_DOMAIN" --count 3 --warmup 1 --timeout 5s >/dev/null 2>&1; then
      gate_https=1; break
    fi
    sleep 1
  done
  [ "$gate_https" = "1" ] && info "https vhost 就绪" || warn "https vhost 未就绪,该档将记 INVALID"

  # ---------- 场景执行 ----------
  # 全局序号:同一场景(lat)会在多个变体(串行/newconn)与多档并发下重复跑,
  # 若 raw 文件名只看 <label>-<path>-<scenario> 就会互相覆盖 —— 事后无法
  # 复核「哪个样本来自哪次执行」。序号进文件名与记录,保证一一对应。
  SEQ=0

  # run_one <scenario> <tag> <path> <kind> <domain> <host_port> <extra args...>
  #   tag        → 变体标签(如 lat / lat-newconn / conc-256),进文件名
  #   kind=http  → 明文;kind=https → TLS + SNI
  #   domain     → vhost 路由依据(tput 用 big.* 指向 4 MiB 源站)
  #   host_port  → 直连时的 B 侧端口(隧道侧端口由 kind 决定)
  run_one() {
    local name="$1" tag="$2" path="$3" kind="$4" domain="$5" hostport="$6"; shift 6
    SEQ=$((SEQ+1))
    local seqfmt base addr errout
    # 期望样本量:有效性判定要拿它做下限(lat 的 --count / conc 的 --connections),
    # 从 $@ 里解析,避免再往签名里堆参数。
    local latwant=0 concwant=0 a prev=""
    for a in "$@"; do
      case "$prev" in
        --count) latwant="$a" ;;
        --connections) concwant="$a" ;;
      esac
      prev="$a"
    done
    seqfmt="$(printf '%03d' "$SEQ")"
    base="$label-$seqfmt-$tag-$path"
    if [ "$path" = "direct" ]; then
      addr="$b_ip:$hostport"
    else
      if [ "$kind" = "https" ]; then addr="$a_ip:$P_VHOST_HTTPS"; else addr="$a_ip:$P_VHOST_HTTP"; fi
    fi
    errout="$RAW_DIR/$base.err"
    local out rc
    if [ "${VHOST_DEBUG:-0}" = "1" ]; then
      printf 'CMD: %s %s --addr %s%s\n' "$VHOST_BIN" "$name" "$addr" "$*" >&2
    fi
    if [ "$kind" = "https" ]; then
      out="$("$VHOST_BIN" "$name" --addr "$addr" --tls --sni "$domain" --host "$domain" \
        "$@" 2>"$errout")"
    else
      out="$("$VHOST_BIN" "$name" --addr "$addr" --host "$domain" "$@" 2>"$errout")"
    fi
    rc=$?
    printf '%s\n' "$out" > "$RAW_DIR/$base.json"
    # ★有效性判定必须按场景校验**关键字段**,不能只看「有没有 env_error」:
    # 一次全部超时/全部建连失败时程序仍会正常输出 JSON(exit=0),
    # 只看 exit 与 env_error 会把「0 QPS」「0/0 建连」当成有效样本,
    # 于是整轮全红却报「无效样本: 0」—— 这正是本轮踩过的假绿。
    local valid=true why=""
    if [ "$rc" -ne 0 ]; then
      valid=false; why="exit=$rc"
    elif printf '%s' "$out" | jq -e 'has("env_error")' >/dev/null 2>&1; then
      valid=false; why="env_error"
    elif ! printf '%s' "$out" | jq -e . >/dev/null 2>&1; then
      valid=false; why="bad_json"
    else
      case "$name" in
        qps)
          # 至少要有请求成功且 QPS>0
          if ! printf '%s' "$out" | jq -e '(.requests // 0) > 0 and (.qps // 0) > 0' >/dev/null 2>&1; then
            valid=false; why="no_successful_request"
          fi ;;
        tput)
          if ! printf '%s' "$out" | jq -e '(.bytes // 0) > 0 and (.gbps // 0) > 0' >/dev/null 2>&1; then
            valid=false; why="no_bytes"
          elif ! printf '%s' "$out" | jq -e --argjson body "$VHOST_TPUT_BODY" \
               '(.bytes / ((.requests // 1)|if . < 1 then 1 else . end)) >= ($body * 0.5)' >/dev/null 2>&1; then
            # ★平均响应体必须接近 --body:这条闸专门抓「打错源站」。
            # 实测踩过 tput-tls 隧道打到了 4 KiB 源站,requests=40436、
            # bytes=165MB(平均 4 KiB),读数 0.13 Gbps,而直连同档 3.35 Gbps ——
            # 若不校验,会被读成"HTTPS vhost 吞吐只有 1/25"。
            valid=false; why="wrong_body_size"
          fi ;;
        lat)
          # 采样数必须达到请求数的 99%:新建连接场景下少量失败可容忍,
          # 大面积失败(如 500 次只成 5 次)必须判无效,不能拿 5 个样本的分位当结论
          if ! printf '%s' "$out" | jq -e --argjson want "$latwant" \
               '(.count // 0) >= ($want * 0.99)' >/dev/null 2>&1; then
            valid=false; why="insufficient_samples"
          fi ;;
        conc)
          # 建连成功数与存活数都必须达到请求数:并发能力不成立时要报红
          if ! printf '%s' "$out" | jq -e --argjson want "$concwant" \
               '(.established // 0) >= ($want * 0.99) and (.hold_alive // 0) >= ($want * 0.99)' >/dev/null 2>&1; then
            valid=false; why="concurrent_shortfall"
          fi ;;
      esac
    fi
    if [ "$valid" = "false" ]; then
      # 追加一行(子 shell 里改不了父 shell 的 FAILS,故走文件)
      printf '%s %s\n' "$base" "$why" >> "$FAILS_FILE"
      err "  无效样本 $base: $why"
    fi
    # 无论有效与否都落一条记录(带 why),绝不丢样本
    if printf '%s' "$out" | jq -e . >/dev/null 2>&1; then
      rec "$(printf '%s' "$out" | jq -c --arg sc "$name" --arg tag "$tag" --arg path "$path" \
        --arg impl "$VHOST_IMPL" --arg proto "$PROTO" --arg run_id "$PEER_RUN_ID" \
        --arg lbl "$label" --arg domain "$domain" --arg raw "$base" --arg why "$why" \
        --argjson valid "$valid" \
        '{metric:"vhost",scenario:$sc,tag:$tag,path:$path,impl:$impl,proto:$proto,run_id:$run_id,
          "label":$lbl,domain:$domain,valid:$valid,invalid_reason:(if $valid then null else $why end),
          raw:$raw,data:.}')"
    else
      rec "$(jq -cn --arg sc "$name" --arg tag "$tag" --arg path "$path" --arg impl "$VHOST_IMPL" \
        --arg proto "$PROTO" --arg run_id "$PEER_RUN_ID" --arg lbl "$label" \
        --arg domain "$domain" --arg raw "$base" --arg why "$why" --argjson rc "$rc" \
        '{metric:"vhost",scenario:$sc,tag:$tag,path:$path,impl:$impl,proto:$proto,run_id:$run_id,
          "label":$lbl,domain:$domain,valid:false,invalid_reason:$why,raw:$raw,exit:$rc}')"
    fi
    printf '%s' "$out"
  }

  # run_pair <scenario> <tag> <kind> <domain> <direct_port> <extra args...>
  run_pair() {
    local name="$1" tag="$2" kind="$3" domain="$4" dport="$5"; shift 5
    local d_out t_out
    d_out="$(run_one "$name" "$tag" direct "$kind" "$domain" "$dport" "$@")"
    t_out="$(run_one "$name" "$tag" tunnel "$kind" "$domain" "$dport" "$@")"
    case "$name" in
      qps)  log "  $(printf '%-16s' "$tag"): 直连 $(printf '%s' "$d_out" | jq -r '.qps // "INVALID"')QPS  隧道 $(printf '%s' "$t_out" | jq -r '.qps // "INVALID"')QPS";;
      tput) log "  $(printf '%-16s' "$tag"): 直连 $(printf '%s' "$d_out" | jq -r '.gbps // "INVALID"')Gbps 隧道 $(printf '%s' "$t_out" | jq -r '.gbps // "INVALID"')Gbps";;
      lat)  log "  $(printf '%-16s' "$tag"): 直连 p50=$(printf '%s' "$d_out" | jq -r '.p50_ms // "INVALID"')ms  隧道 p50=$(printf '%s' "$t_out" | jq -r '.p50_ms // "INVALID"')ms  样本=$(printf '%s' "$t_out" | jq -r '.count // 0')";;
      conc) log "  $(printf '%-16s' "$tag"): 直连 alive=$(printf '%s' "$d_out" | jq -r '.hold_alive // "INVALID"')/$(printf '%s' "$d_out" | jq -r '.established // 0')  隧道 alive=$(printf '%s' "$t_out" | jq -r '.hold_alive // "INVALID"')/$(printf '%s' "$t_out" | jq -r '.established // 0')";;
    esac
  }

  local t_big="${T_BIG:-19106}"

  # 1) HTTP vhost(明文全链路)
  hdr "HTTP vhost(明文)"
  run_pair qps  qps         http  "$VHOST_DOMAIN"     "$T_HTTP"  --conns "$VHOST_QPS_CONNS" --dur "$VHOST_QPS_DUR"
  run_pair lat  lat         http  "$VHOST_DOMAIN"     "$T_HTTP"  --count "$VHOST_LAT_COUNT" --warmup 200
  run_pair lat  lat-newconn http  "$VHOST_DOMAIN"     "$T_HTTP"  --count 500 --newconn
  # 吞吐必须打 big 域名 → 4 MiB 源站;打 4 KiB 源站测的是 QPS 不是带宽
  run_pair tput tput        http  "big.$VHOST_DOMAIN" "$t_big"   --conns "$VHOST_TPUT_CONNS" --dur "$VHOST_TPUT_DUR"

  # 并发:多档 × 多轮(与 peer.sh 的高水位语义一致)
  local n s
  for s in $(seq 1 "$VHOST_CONC_REPEAT"); do
    for n in $VHOST_CONC_LEVELS; do
      hdr "HTTP 并发 ${n} (第 ${s} 轮)"
      run_pair conc "conc${n}s${s}" http "$VHOST_DOMAIN" "$T_HTTP" \
        --connections "$n" --hold "$VHOST_CONC_HOLD" --parallel 64
    done
  done

  # 2) HTTPS vhost(SNI 透传,TLS 在源站终结)
  hdr "HTTPS vhost(SNI 透传)"
  run_pair qps  qps-tls         https "$VHOST_DOMAIN" "$T_HTTPS" --conns "$VHOST_QPS_CONNS" --dur "$VHOST_QPS_DUR"
  run_pair lat  lat-tls         https "$VHOST_DOMAIN" "$T_HTTPS" --count "$VHOST_LAT_COUNT" --warmup 200
  run_pair lat  lat-tls-newconn https "$VHOST_DOMAIN" "$T_HTTPS" --count 500 --newconn
  # 吞吐必须同时满足两点,否则与明文 tput 不可比:
  #   1. 打 TLS + 4 MiB 源站(19107),不是 4 KiB 的 19105
  #   2. 域名用 big.$VHOST_DOMAIN —— **隧道侧靠 SNI 选路**,
  #      用 bench.test 会被路由到 4 KiB 源站(实测:隧道 requests 40436
  #      × 4 KiB = 165 MB,而直连 1002 × 4 MiB = 4.2 GB,相差 40 倍)。
  run_pair tput tput-tls        https "big.$VHOST_DOMAIN" "$T_HTTPS_BIG" --conns "$VHOST_TPUT_CONNS" --dur "$VHOST_TPUT_DUR"

  # HTTPS 并发(单档 1024:交叉验证 TLS 握手在高并发下的表现)
  hdr "HTTPS 并发 1024"
  run_pair conc conc1024-tls https "$VHOST_DOMAIN" "$T_HTTPS" \
    --connections 1024 --hold "$VHOST_CONC_HOLD" --parallel 64

  hdr "本轮完成"
  FAILS="$(wc -l < "$FAILS_FILE" 2>/dev/null | tr -d ' ')"
  [ -n "$FAILS" ] || FAILS=0
  log "  证据: $REC_OUT_FILE"
  log "  无效样本: $FAILS"$([ "$FAILS" -gt 0 ] && echo " (明细: $FAILS_FILE)")
  [ "$FAILS" -eq 0 ] || return 1
}

# ============================================================
# frp 辅助
# ============================================================
resolve_frp_bin() {
  local cache="$BENCH_ROOT/.bin/frp-v0.71.0"
  if [ -x "$cache/frps" ] && [ -x "$cache/frpc" ]; then
    FRP_BIN="$cache"
    return 0
  fi
  mkdir -p "$cache"
  local url="https://github.com/fatedier/frp/releases/download/v0.71.0/frp_0.71.0_linux_amd64.tar.gz"
  local tmp="$cache/dl.tar.gz"
  info "下载 frp v0.71.0 …"
  curl -fsSL -o "$tmp" "$url" || die "frp 下载失败: $url"
  ( cd "$cache" && sha256sum "$(basename "$tmp")" > frp.sha256 )
  tar xzf "$tmp" -C "$cache" --strip-components=1
  rm -f "$tmp"
  chmod +x "$cache/frps" "$cache/frpc"
  FRP_BIN="$cache"
}

BENCH_TOKEN="rh-bench-token"

# ============================================================
# 入口
# ============================================================
case "${1:-}" in
  server) shift; vhost_server "$@" ;;
  client) shift; vhost_client "$@" ;;
  drive)  shift; vhost_drive "$@" ;;
  *)
    cat >&2 <<EOF
用法:
  vhost-peer.sh server
  vhost-peer.sh client <A_ip:ctl> <client_id> <secret>   # rh
  vhost-peer.sh client <A_IP>                            # frp
  vhost-peer.sh drive <A_IP> <B_IP> <api_port> <token> <cid> [label]

环境变量:
  VHOST_IMPL=rh|frp   PROTO=tcp|tls   VHOST_RUN_ID=<id>
  VHOST_BIN=<vhostbench 路径>  PEER_DIR=<证据目录>
EOF
    exit 2 ;;
esac
