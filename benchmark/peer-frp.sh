#!/usr/bin/env bash
# rabbithole benchmark —— frp 对照基准(与 peer.sh 同拓扑/同场景/同记录格式)。
#
# 与 peer.sh 的差异仅两点:
#   1. 被测软件换成 frp v0.71.0(官方 release,固定 tag + 实测 sha256 记录);
#   2. frp 的规则在 frpc.toml 静态声明,无需服务端 API 下发,drive 无需凭据。
#
# 角色(与 peer.sh 一致,由运行者用自己的通道投放):
#   peer-frp.sh server              # A(test1):起 frps
#   peer-frp.sh client <A_IP>       # B(test2):起 frpc + 目标服务
#   peer-frp.sh drive  <A_IP> <B_IP> [label]   # test3:门控 + 全场景(带直连基线)
#
# 环境变量:
#   FRP_VERSION   frp 版本(默认 v0.71.0,即 rabbithole README 的对标版本)
#   PROTO         控制面协议 tcp|tls|ws|wss|quic|kcp(默认 tcp)
#   PEER_DIR      工作目录(默认 /tmp/frp-peer,与 rabbithole 轮隔离)
#   PEER_DURATION iperf3 每档秒数(默认 10)
#   PEER_CONCURRENT  是否跑高水位并发(默认 1)
#
# 端口规划与 rabbithole 轮完全一致:
#   A: frps 控制面 17100(kcp 17101 / quic 17102) 转发口 20001-20004 vhost 20004
#   B: iperf=19101 tcp=19102 udp=19103 http=19104

set -uo pipefail
BENCH_SELF_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$BENCH_SELF_DIR/lib/common.sh"

require_cmd python3 curl jq iperf3

FRP_VERSION="${FRP_VERSION:-v0.71.0}"
FRP_ASSET="frp_${FRP_VERSION#v}_linux_amd64.tar.gz"   # tag 带 v,包名不带
FRP_URL="https://github.com/fatedier/frp/releases/download/${FRP_VERSION}/${FRP_ASSET}"
PROTO="${PROTO:-tcp}"
PEER_DIR="${PEER_DIR:-/tmp/frp-peer}"
PEER_DURATION="${PEER_DURATION:-10}"

# 吞吐压测工具(与 peer.sh 同一开关,保证两侧可比):
#   THP_TOOL=iperf3(默认) | loadgen(仓库自带 Go 压测器)
THP_TOOL="${THP_TOOL:-iperf3}"
LOADGEN_BIN="${LOADGEN_BIN:-/tmp/rh-bin/loadgen}"
if [ "$THP_TOOL" = "loadgen" ]; then
  [ -x "$LOADGEN_BIN" ] || die "THP_TOOL=loadgen 但 LOADGEN_BIN 不可执行: $LOADGEN_BIN"
fi
export THP_TOOL LOADGEN_BIN

CTL_PORT=17100; KCP_PORT=17101; QUIC_PORT=17102
PROXY_BASE=20001
T_IPERF=19101; T_TCP=19102; T_UDP=19103; T_HTTP=19104
BENCH_TOKEN="rh-bench-token"

raise_nofile 65535

resolve_frp() {
  local cache="$BENCH_ROOT/.bin/frp-$FRP_VERSION"
  if [ ! -x "$cache/frps" ]; then
    info "下载 frp $FRP_VERSION …"
    mkdir -p "$cache"
    local tmp="$cache/dl.tar.gz"
    curl -fsSL -o "$tmp" "$FRP_URL" || die "frp 下载失败: $FRP_URL"
    # frp 不提供 sidecar 校验文件:记录实测 sha256 保证可复现
    ( cd "$cache" && sha256sum "$(basename "$tmp")" > frp.sha256 )
    info "frp sha256: $(cut -c1-16 "$cache/frp.sha256" | head -1)…"
    tar xzf "$tmp" -C "$cache" --strip-components=1
    rm -f "$tmp"
    chmod +x "$cache/frps" "$cache/frpc"
  fi
  FRP_BIN="$cache"
  info "frp 就绪: $FRP_BIN ($("$cache/frps" --version 2>/dev/null | head -1))"
}

# frp 客户端传输配置(按 PROTO 生成)
# frp 规定:kcp/quic 模式下 serverPort 必须指向对应的 kcpBindPort/quicBindPort
frp_client_transport() {
  local port="$CTL_PORT"
  local proto_line='transport.protocol = "tcp"'
  case "$PROTO" in
    tcp)  ;;
    tls)  proto_line="$proto_line"$'\n''transport.tls.enable = true' ;;
    ws)   proto_line='transport.protocol = "websocket"' ;;
    wss)  die "frp 的 wss 需外部 TLS 终结(如 nginx),frps 不原生支持;ws 轮已覆盖 websocket 开销,跳过并记录 N/A" ;;
    quic) port="$QUIC_PORT"; proto_line='transport.protocol = "quic"' ;;
    kcp)  port="$KCP_PORT";  proto_line='transport.protocol = "kcp"' ;;
    *) die "未知 PROTO: $PROTO" ;;
  esac
  printf 'serverPort = %s\n%s\n' "$port" "$proto_line"
}

# ============================================================
# 角色 1:A(test1)frps
# ============================================================
frp_server() {
  resolve_frp
  mkdir -p "$PEER_DIR"
  cat > "$PEER_DIR/frps.toml" <<EOF
bindAddr = "0.0.0.0"
bindPort = $CTL_PORT
kcpBindPort = $KCP_PORT
quicBindPort = $QUIC_PORT
vhostHTTPPort = 20004

auth.token = "$BENCH_TOKEN"

log.level = "warn"
EOF
  "$FRP_BIN/frps" -c "$PEER_DIR/frps.toml" >"$PEER_DIR/frps.log" 2>&1 &
  echo $! > "$PEER_DIR/frps.pid"
  wait_port 127.0.0.1 "$CTL_PORT" 20 || { err "frps 未就绪"; cat "$PEER_DIR/frps.log"; return 1; }

  hdr "frps 就绪 (A, proto=$PROTO, $( "$FRP_BIN/frps" --version 2>/dev/null | head -1 ))"
  log "  控制面 :$CTL_PORT (kcp :$KCP_PORT quic :$QUIC_PORT)  vhost :20004"
  log "  ── 在 B(test2) 上执行 ──"
  log "  peer-frp.sh client <A_IP>"
}

# ============================================================
# 角色 2:B(test2)frpc + 目标服务
# ============================================================
frp_client() {  # <a_ip>
  local a_ip="$1"
  resolve_frp
  mkdir -p "$PEER_DIR"

  python3 "$BENCH_PY/target.py" \
    --tcp-port "$T_TCP" --udp-port "$T_UDP" --http-port "$T_HTTP" \
    >"$PEER_DIR/target.log" 2>&1 &
  echo $! > "$PEER_DIR/target.pid"
  wait_port 127.0.0.1 "$T_TCP" 15 || { err "目标服务未就绪"; cat "$PEER_DIR/target.log"; return 1; }

  if [ "$THP_TOOL" = "loadgen" ]; then
    "$LOADGEN_BIN" sink "$T_IPERF" 7200 200000 >"$PEER_DIR/iperf.log" 2>&1 &
  else
    iperf3 -s -p "$T_IPERF" >"$PEER_DIR/iperf.log" 2>&1 &
  fi
  echo $! > "$PEER_DIR/iperf.pid"
  wait_port 127.0.0.1 "$T_IPERF" 10 || { err "iperf3 未就绪"; return 1; }

  local transport; transport="$(frp_client_transport)"
  cat > "$PEER_DIR/frpc.toml" <<EOF
serverAddr = "$a_ip"
auth.token = "$BENCH_TOKEN"

$transport
transport.poolCount = 4
transport.tcpMux = true

log.level = "warn"

[[proxies]]
name = "tcp-echo"
type = "tcp"
localIP = "127.0.0.1"
localPort = $T_TCP
remotePort = $PROXY_BASE

[[proxies]]
name = "iperf"
type = "tcp"
localIP = "127.0.0.1"
localPort = $T_IPERF
remotePort = $((PROXY_BASE + 1))

[[proxies]]
name = "udp-echo"
type = "udp"
localIP = "127.0.0.1"
localPort = $T_UDP
remotePort = $((PROXY_BASE + 2))

[[proxies]]
name = "web"
type = "http"
localIP = "127.0.0.1"
localPort = $T_HTTP
customDomains = ["bench.test"]
EOF
  "$FRP_BIN/frpc" -c "$PEER_DIR/frpc.toml" >"$PEER_DIR/frpc.log" 2>&1 &
  echo $! > "$PEER_DIR/frpc.pid"
  sleep 3

  hdr "frpc 就绪 (B, proto=$PROTO)"
  log "  已连向 $a_ip:$CTL_PORT;转发口与 rabbithole 轮一致(20001-20004)"
}

# ============================================================
# 角色 3:test3 驱动(场景与 peer.sh drive 完全一致)
# ============================================================
frp_drive() {  # <a_ip> <b_ip> [label]
  local a_ip="$1" b_ip="$2" label="${3:-$PROTO}"
  local P_TCP=$PROXY_BASE P_IPERF=$((PROXY_BASE+1)) P_UDP=$((PROXY_BASE+2)) P_VHOST=$((PROXY_BASE+3))
  mkdir -p "$PEER_DIR"

  # 就绪门控:与 rabbithole 轮相同,避免建连竞态污染
  local gate_ok=0
  for _ in $(seq 1 30); do
    if python3 - "$a_ip" "$P_TCP" <<'PYEOF'
import socket, sys
try:
    s = socket.create_connection((sys.argv[1], int(sys.argv[2])), timeout=2)
    s.sendall(b"PING\n")
    ok = s.recv(16).startswith(b"PONG")
    s.close()
    sys.exit(0 if ok else 1)
except OSError:
    sys.exit(1)
PYEOF
    then gate_ok=1; break; fi
    sleep 1
  done
  [ "$gate_ok" = "1" ] || { err "隧道 30s 内未就绪(检查 frpc 日志)"; return 1; }
  info "隧道就绪,开始压测 (proto=$PROTO)"

  local out_file="$PEER_DIR/results-$label.jsonl"
  : > "$out_file"
  rec() { echo "$1" >> "$out_file"; }

  # ---------- 吞吐 ----------
  # thp <host> <port> <streams> <rev> → Mbps 整数(0 = 无效,如实记录不造数)
  thp() {
    local host="$1" port="$2" streams="$3" rev="$4" out
    if [ "$THP_TOOL" = "loadgen" ]; then
      # loadgen 仅单向发送,rev(下行)无对应能力 → 与 peer.sh 同口径
      out="$("$LOADGEN_BIN" src "$host:$port" "$PEER_DURATION" "$streams" 2>/dev/null \
        | jq -r '(.gbps // 0)*1000|floor' 2>/dev/null)"
    else
      out="$(iperf3 -c "$host" -p "$port" -t "$PEER_DURATION" -P "$streams" $rev -J 2>/dev/null \
        | jq -r '.end.sum_received.bits_per_second/1e6|floor // 0' 2>/dev/null)"
    fi
    [ -n "$out" ] && [ "$out" != "null" ] || out=0
    printf '%s' "$out"
  }

  # 重复配对次数,与 peer.sh 的 PEER_REPEAT 同语义、同默认值 —— 两侧轮数
  # 不一致会让"取中位数"与"单次采样"混比,对比失效。
  local repeat="${PEER_REPEAT:-3}" sample
  hdr "吞吐 (${THP_TOOL}, 每档 ${PEER_DURATION}s × ${repeat} 次配对)"
  for sample in $(seq 1 "$repeat"); do
  for spec in "1:upload:" "1:download:-R" "4:upload:" "4:download:-R" "16:upload:" "16:download:-R"; do
    local streams dir rev mbps
    streams="${spec%%:*}"; rest="${spec#*:}"; dir="${rest%%:*}"; rev="${rest#*:}"
    mbps="$(thp "$b_ip" "$T_IPERF" "$streams" "$rev")"
    rec "{\"metric\":\"throughput\",\"path\":\"direct\",\"dir\":\"$dir\",\"streams\":$streams,\"mbps\":$mbps,\"sample\":$sample,\"proto\":\"frp-$PROTO\"}"
    log "  直连  ${streams}s $dir 第${sample}次: ${mbps} Mbps"
    mbps="$(thp "$a_ip" "$P_IPERF" "$streams" "$rev")"
    rec "{\"metric\":\"throughput\",\"path\":\"tunnel\",\"dir\":\"$dir\",\"streams\":$streams,\"mbps\":$mbps,\"sample\":$sample,\"proto\":\"frp-$PROTO\"}"
    log "  隧道  ${streams}s $dir 第${sample}次: ${mbps} Mbps"
  done
  done

  # ---------- 延迟 ----------
  hdr "延迟 (5000 往返)"
  local d_lat t_lat d_nc
  d_lat="$(python3 "$BENCH_PY/loadgen.py" --host "$b_ip" --port "$T_TCP" latency --count 5000 --warmup 200)"
  t_lat="$(python3 "$BENCH_PY/loadgen.py" --host "$a_ip" --port "$P_TCP" latency --count 5000 --warmup 200)"
  d_nc="$(python3 "$BENCH_PY/loadgen.py" --host "$a_ip" --port "$P_TCP" latency --count 500 --newconn)"
  rec "{\"metric\":\"latency\",\"path\":\"direct\",\"data\":$d_lat,\"proto\":\"frp-$PROTO\"}"
  rec "{\"metric\":\"latency\",\"path\":\"tunnel\",\"data\":$t_lat,\"proto\":\"frp-$PROTO\"}"
  rec "{\"metric\":\"latency_newconn\",\"path\":\"tunnel\",\"data\":$d_nc,\"proto\":\"frp-$PROTO\"}"
  python3 -c "
import json
d,t,n=(json.loads(x) for x in '''$d_lat|$t_lat|$d_nc'''.split('|'))
print(f'  直连  p50={d[\"p50_ms\"]}ms p99={d[\"p99_ms\"]}ms')
print(f'  隧道  p50={t[\"p50_ms\"]}ms p99={t[\"p99_ms\"]}ms')
print(f'  开销  p50={round(t[\"p50_ms\"]-d[\"p50_ms\"],3)}ms p99={round(t[\"p99_ms\"]-d[\"p99_ms\"],3)}ms')
print(f'  新建连接 p50={n[\"p50_ms\"]}ms p99={n[\"p99_ms\"]}ms')"

  # ---------- 建连速率 ----------
  hdr "连接建立速率"
  local d_cr t_cr
  d_cr="$(python3 "$BENCH_PY/loadgen.py" --host "$b_ip" --port "$T_TCP" connect --connections 3000 --parallel 64)"
  t_cr="$(python3 "$BENCH_PY/loadgen.py" --host "$a_ip" --port "$P_TCP" connect --connections 3000 --parallel 64)"
  rec "{\"metric\":\"connect_rate\",\"direct\":$d_cr,\"tunnel\":$t_cr,\"proto\":\"frp-$PROTO\"}"
  log "  直连 $(echo "$d_cr"|jq -r .rate_per_s)/s  隧道 $(echo "$t_cr"|jq -r .rate_per_s)/s"

  # ---------- UDP ----------
  hdr "UDP (1200B)"
  local d_u t_u
  d_u="$(python3 "$BENCH_PY/loadgen.py" --host "$b_ip" --port "$T_UDP" udp --size 1200 --duration 8 --window 32)"
  t_u="$(python3 "$BENCH_PY/loadgen.py" --host "$a_ip" --port "$P_UDP" udp --size 1200 --duration 8 --window 32)"
  rec "{\"metric\":\"udp\",\"direct\":$d_u,\"tunnel\":$t_u,\"proto\":\"frp-$PROTO\"}"
  log "  直连 $(echo "$d_u"|jq -r .mbps)Mbps 丢=$(echo "$d_u"|jq -r .loss_pct)%  隧道 $(echo "$t_u"|jq -r .mbps)Mbps 丢=$(echo "$t_u"|jq -r .loss_pct)%"

  # ---------- HTTP vhost ----------
  hdr "HTTP vhost QPS"
  local d_h t_h
  d_h="$(python3 "$BENCH_PY/loadgen.py" --host "$b_ip" --port "$T_HTTP" http --connections 16 --duration 8)"
  t_h="$(python3 "$BENCH_PY/loadgen.py" --host "$a_ip" --port "$P_VHOST" http --connections 16 --duration 8 --host-header bench.test)"
  rec "{\"metric\":\"http\",\"direct\":$d_h,\"tunnel\":$t_h,\"proto\":\"frp-$PROTO\"}"
  log "  直连 $(echo "$d_h"|jq -r .qps)QPS  隧道 $(echo "$t_h"|jq -r .qps)QPS"

  # ---------- 并发(高水位放最后;非 tcp 轮可跳过) ----------
  if [ "${PEER_CONCURRENT:-1}" = "1" ]; then
    hdr "并发连接 (直连 vs 隧道)"
    for n in 64 256 1024 4096; do
      local d_c t_c
      d_c="$(python3 "$BENCH_PY/loadgen.py" --host "$b_ip" --port "$T_TCP" concurrent --connections "$n" --hold 3)"
      t_c="$(python3 "$BENCH_PY/loadgen.py" --host "$a_ip" --port "$P_TCP" concurrent --connections "$n" --hold 3)"
      rec "{\"metric\":\"concurrent\",\"n\":$n,\"direct\":$d_c,\"tunnel\":$t_c,\"proto\":\"frp-$PROTO\"}"
      log "  $n: 直连成功=$(echo "$d_c"|jq -r .established) 隧道成功=$(echo "$t_c"|jq -r .established) 建连p99=$(echo "$t_c"|jq -r .connect_p99_ms)ms"
    done
  fi

  hdr "完成"
  info "结果: $out_file"
}

main() {
  local cmd="${1:-help}"
  case "$cmd" in
    server) shift; frp_server ;;
    client) shift; frp_client "$@" ;;
    drive)  shift; frp_drive "$@" ;;
    *) sed -n '2,40p' "$0" ;;
  esac
}

main "$@"
