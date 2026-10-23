#!/usr/bin/env bash
# rabbithole benchmark —— 跨机三角色基准(真实链路)。goal-repair P3 修复版。
#
# 拓扑:
#
#   [test3: driver]        [test1: server]            [test2: client]
#   iperf3 -c  ──10GbE──►  rh-server :CTL    ──隧道──►  rh-client
#   loadgen.py             :API 管理面                 ├ iperf3 -s :T_IPERF
#   (独立压测机,           :20001+ 转发口              ├ target.py :T_TCP/UDP/HTTP
#    不与被测抢 CPU)              │                      (目标都在 B 本机)
#                                 └── 规则由 drive 经 API 下发
#
#   基线:  driver ──直连──► B:T_IPERF       (不经 rabbithole)
#   隧道:  driver ──► A:PROXY ──隧道──► B:T_IPERF
#   开销 = 隧道 / 直连
#
# 三个角色,同一份脚本,由运行者用自己的通道(termark/ssh/手动)分别投放:
#
#   peer.sh server                              # 在 A(test1) 上
#   peer.sh client <A_ip:ctl> <id> <secret>     # 在 B(test2) 上
#   peer.sh drive  <A_ip> <B_ip> <api> <token> <cid> [label]  # 在 test3 上
#
# 固定端口规划(可用环境变量覆盖):
#   A: ctl=17100 api=18080 转发口 20001..20004
#   B: iperf=19101 tcp=19102 udp=19103 http=19104
#
# ★goal-repair 必读(相对旧版的行为变化):
#   * 三个角色都要求显式环境变量:PROTO / RELAY / MUX / PEER_RUN_ID
#     (不再有隐式默认值暗中生效 —— A9:元数据必须与端点实况一致)。
#   * 三个角色都 raise_nofile 并把实际软限写入 $PEER_DIR/nofile.json;
#     不足(并发档 + 512)即以环境错误中止,EMFILE 绝不折算成产品失败。
#   * server/client 落 endpoint.json(实际配置回读 + /proc/PID/exe SHA-256
#     + RH_NOTSENT_LOWAT/RH_QUIC_CC 实际值 + iperf3 版本)+ cpu.jsonl
#     (/proc 采样器,≥10Hz)。
#   * drive 每条 iperf3 原始 JSON/stderr/退出码落 $PEER_DIR/raw/,
#     失败如实记 valid:false,绝不 //0;任何无效样本使本轮退出码非 0。
#   * 结果文件名带时间戳,永不覆盖历史。

set -uo pipefail
BENCH_SELF_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$BENCH_SELF_DIR/lib/common.sh"

require_cmd python3 curl jq

# ★RELAY 决定生成的 relay_mode(splice=最终版本默认;copy=拷贝路径对照轮,
# 对照轮放独立证据目录,不进门槛)。PROTO/RELAY/MUX/PEER_RUN_ID 由
# require_run_env 强制显式(见 lib/common.sh),无隐式默认值。
PEER_DIR="${PEER_DIR:-/tmp/rh-peer}"
PEER_DURATION="${PEER_DURATION:-10}"
# 配对重复次数:G7 要求 ≥3 次独立配对测量的中位数,默认 3。
PEER_REPEAT="${PEER_REPEAT:-3}"
# 并发档与保持时长(秒);G5 = 4096 verified。
PEER_CONC_LEVELS="${PEER_CONC_LEVELS:-64 256 1024 4096}"
PEER_CONC_HOLD="${PEER_CONC_HOLD:-5}"
# 并发档重复次数(默认与 PEER_REPEAT 一致)
PEER_CONC_REPEAT="${PEER_CONC_REPEAT:-$PEER_REPEAT}"
# UDP 排空期秒数(A8:到时后继续收在途回包,固定上限)
PEER_UDP_DRAIN="${PEER_UDP_DRAIN:-5}"

CTL_PORT="${CTL_PORT:-17100}"
API_PORT="${API_PORT:-18080}"
PROXY_BASE="${PROXY_BASE:-20001}"
T_IPERF="${T_IPERF:-19101}"
T_TCP="${T_TCP:-19102}"
T_UDP="${T_UDP:-19103}"
T_HTTP="${T_HTTP:-19104}"

# 吞吐压测工具。默认 iperf3(保持既有行为与历史数据可比)。
#   THP_TOOL=iperf3   系统 iperf3 单线程,-P 不扩展吞吐;多线程版见 IPERF3_BIN
#   THP_TOOL=loadgen  仓库自带 Go 压测器(benchmark/loadgen)
#
# ★为什么需要 loadgen:TLS 下的吞吐量级会显著高于明文,
# iperf3 3.9 单线程与 Python loadgen 的天花板都在 2 Gbps 量级;
# 用它去测 TLS 会把两边都压到同一数字,测到的是压测器瓶颈而不是产品差异
# (见 benchmark/loadgen/main.go 顶部注释,该坑在明文场景已踩过一次)。
# loadgen 与 iperf3 的字节口径一致(只计有效载荷),结果可对照。
# 数据通道是否开启端到端 snappy 压缩(use_compression)。
#   PEER_COMPRESS=0  默认,规则不开压缩(走 splice / chunk 快路径)
#   PEER_COMPRESS=1  规则开压缩 → 客户端恒回落 forward::splice 的 copy 路径
#
# ★为什么需要这个开关:压缩规则走的是 `forward.rs::splice()`(服务端压缩流
# 包着通道,客户端必须解压,故不能 splice 零拷贝),而这条路径承接了
# 所有 use_compression=true 的规则、relay_mode=copy 的 A/B 对照、全部插件
# 规则与全部 visitor 流量。要度量这条路径的改动(如 round-69 的无锁拆分)
# 就必须能在同一套 peer 拓扑上把它打开 —— 否则只能另写压测脚本,那会丢掉
# 证据采集(CPU 采样/端点实况/nofile/无效样本留痕)。
PEER_COMPRESS="${PEER_COMPRESS:-0}"

# UDP 并发源地址数(每个源 = 隧道侧一个用户会话)。
#   1(默认)= 单会话,旧行为
#   >1      = 多会话,用于压测"每源一个回写任务"的写路径(锁争用/队头阻塞)
# 代理侧按用户源地址建会话,单源压测永远测不到多会话争用。
PEER_UDP_SOURCES="${PEER_UDP_SOURCES:-1}"

THP_TOOL="${THP_TOOL:-iperf3}"
IPERF3_BIN="${IPERF3_BIN:-iperf3}"
LOADGEN_BIN="${LOADGEN_BIN:-/tmp/rh-bin/loadgen}"
# 必须 export:run_iperf 在 `$(...)` 子 shell 里执行,未导出的普通变量
# 在那里不可见 —— 会导致 THP_TOOL 回落成 iperf3,而解析分支仍按 loadgen
# 判断(空 JSON → 全部 INVALID(exit=0))。这个不一致极难从输出看出来。
# PEER_COMPRESS 同样必须 export(role_* 由 `$(...)`/子 shell 分派,
# 未导出时预算里的压缩开关在子进程看不到 → 规则静默不开压缩,
# 与 THP_TOOL 曾踩的坑同型)。
export THP_TOOL LOADGEN_BIN PEER_COMPRESS PEER_UDP_SOURCES
if [ "$THP_TOOL" = "loadgen" ]; then
  [ -x "$LOADGEN_BIN" ] || die "THP_TOOL=loadgen 但 LOADGEN_BIN 不可执行: $LOADGEN_BIN"
else
  command -v "$IPERF3_BIN" >/dev/null 2>&1 \
    || die "iperf3 不可用: IPERF3_BIN=$IPERF3_BIN (三机上 /tmp/rh-bin/iperf3 可用)"
fi

# 并发最高档 → 本轮 fd 需求(每连接 1 fd + 余量 512;4096 档即 ≥4608,
# 统一推荐 65535。依据见 docs/performance-2026-10-history.md §4)
conc_fd_need() {
  local max=0 n
  for n in $PEER_CONC_LEVELS; do [ "$n" -gt "$max" ] && max="$n"; done
  echo $((max + 512))
}

# 启动 /proc CPU+RSS 采样器(仅 Linux;采样失败=环境错误,直接中止本角色)
start_cpu_sampler() {  # <pid> <out>
  local pid="$1" out="$2"
  python3 "$BENCH_PY/cpu_sampler.py" --pid "$pid" --out "$out" >"$out.boot" 2>&1 &
  echo $! > "$out.pid"
  sleep 1
  if ! kill -0 "$(cat "$out.pid")" 2>/dev/null; then
    err "CPU 采样器启动失败(G1 的 CPU 证据是硬性要求):"
    cat "$out.boot" >&2
    return 1
  fi
  info "CPU 采样器已启动: pid=$(cat "$out.pid") out=$out"
}

# ---------- drive 侧共享助手(顶层定义,供 peer_drive 与本地回归测试共用) ----------
FAILS=0
LG_SEQ=0
RAW_DIR=""
REC_OUT_FILE=""

# rec 只接受非空 JSON 行:组装失败(jq 报错等)必须可见并计数
rec() {
  if [ -n "$1" ]; then
    echo "$1" >> "$REC_OUT_FILE"
  else
    err "rec: 空 JSON 行(记录组装失败,计入 FAILS)"
    FAILS=$((FAILS+1))
  fi
}

# run_iperf <dst> <port> <streams> <rev> <id> → stdout 单行 JSON(A11:留痕)
#   有效: {"valid":true,"mbps":N,"exit":0,"window_start_us":..,"window_end_us":..,"raw_*":"raw/.."}
#   失败: {"valid":false,"mbps":null,"error":{...}} —— 绝不输出 0 冒充
run_iperf() {
  local dst="$1" port="$2" streams="$3" rev="$4" id="$5"
  local j="$RAW_DIR/$id.json" e="$RAW_DIR/$id.stderr" x="$RAW_DIR/$id.exit"
  local t0 t1 rc mbps
  t0="$(epoch_us)"
  if [ "$THP_TOOL" = "loadgen" ]; then
    # loadgen src <host:port> <seconds> <conns>:与 iperf3 -c/-P 语义对齐。
    # 它只有单向发送(sink 读空),故忽略 $rev —— 下行样本会如实记为同口径
    # 的单向数据,而不是伪装成双向。
    "$LOADGEN_BIN" src "$dst:$port" "$PEER_DURATION" "$streams" >"$j" 2>"$e"
    rc=$?
  else
    "$IPERF3_BIN" -c "$dst" -p "$port" -t "$PEER_DURATION" -P "$streams" $rev -J >"$j" 2>"$e"
    rc=$?
  fi
  t1="$(epoch_us)"
  printf '%s\n' "$rc" >"$x"
  local ok_expr='(.end.sum_received.bits_per_second // 0) > 0'
  local mbps_expr='.end.sum_received.bits_per_second/1e6|floor'
  if [ "$THP_TOOL" = "loadgen" ]; then
    ok_expr='(.gbps // 0) > 0'
    mbps_expr='.gbps*1000|floor'
  fi
  if [ "$rc" -eq 0 ] && jq -e "$ok_expr" "$j" >/dev/null 2>&1; then
    mbps="$(jq -r "$mbps_expr" "$j")"
    jq -cn --argjson mbps "$mbps" --argjson t0 "$t0" --argjson t1 "$t1" --arg id "$id" \
      '{valid:true,mbps:$mbps,exit:0,window_start_us:$t0,window_end_us:$t1,
        raw_json:("raw/"+$id+".json"),raw_stderr:("raw/"+$id+".stderr"),raw_exit:("raw/"+$id+".exit")}'
  else
    jq -cn --argjson exit "$rc" --argjson t0 "$t0" --argjson t1 "$t1" --arg id "$id" \
      --arg stderr_tail "$(tail -c 300 "$e" 2>/dev/null)" \
      '{valid:false,mbps:null,exit:$exit,window_start_us:$t0,window_end_us:$t1,
        raw_json:("raw/"+$id+".json"),raw_stderr:("raw/"+$id+".stderr"),raw_exit:("raw/"+$id+".exit"),
        error:{iperf_exit:$exit,stderr_tail:$stderr_tail}}'
  fi
}

# run_loadgen <tag> <args...> → stdout loadgen JSON;失败/含 error 时
# 返回 {"ok":false,...}(无效样本如实留痕,不造 0)。
# 环境错误(EMFILE 等)返回 {"ok":false,"env_error":{...}} —— 由调用方中止本轮。
# 注意:run_loadgen 通常在 $(...) 里执行(子 shell),所以失败计数必须由
# 调用方用 loadgen_failed 在父 shell 里补记,否则失败不会传导到退出码。
run_loadgen() {
  local tag="$1"; shift
  LG_SEQ=$((LG_SEQ+1))
  local out rc errf="$RAW_DIR/$tag.stderr"
  out="$(python3 "$BENCH_PY/loadgen.py" "$@" 2>"$errf")"; rc=$?
  if [ "$rc" -eq 0 ] && printf '%s' "$out" | jq -e 'type=="object" and (has("error")|not) and (has("env_error")|not)' >/dev/null 2>&1; then
    printf '%s' "$out"
  else
    if printf '%s' "$out" | jq -e 'has("env_error")' >/dev/null 2>&1; then
      jq -cn --argjson exit "$rc" --arg tag "$tag" \
        --arg env "$(printf '%s' "$out" | jq -c '.env_error')" \
        '{ok:false,loadgen_exit:$exit,tag:$tag,env_error:($env|fromjson)}'
    else
      jq -cn --argjson exit "$rc" --arg tag "$tag" \
        --arg out "$(printf '%s' "$out" | head -c 400)" \
        '{ok:false,loadgen_exit:$exit,tag:$tag,output:$out}'
    fi
  fi
}

# loadgen_failed <json> → 结果是 ok:false 时在父 shell 计 FAILS 并返回 0
loadgen_failed() {
  if printf '%s' "$1" | jq -e '.ok == false' >/dev/null 2>&1; then
    FAILS=$((FAILS+1))
    return 0
  fi
  return 1
}

# ============================================================
# 角色 1:A(test1)服务端
# ============================================================
peer_server() {
  require_run_env server
  raise_nofile 65535
  check_nofile server "$(conc_fd_need)" || return 1
  resolve_binaries
  mkdir -p "$PEER_DIR"

  gen_server_config "$PEER_DIR" "$CTL_PORT" "$API_PORT" "$PROTO" "relay_mode = \"$RELAY\""
  python3 - "$PEER_DIR/server.toml" <<'PY'
import sys
path = sys.argv[1]
txt = open(path).read()
txt = txt.replace("http_port = 0", "http_port = 20004")   # vhost
txt = txt.replace("[ports]", 'subdomain_host = "bench.test"\n\n[ports]')
open(path, "w").write(txt)
PY
  "$BENCH_BIN/rh-server" --config "$PEER_DIR/server.toml" admin create admin peer-Pass-1 >/dev/null 2>&1
  "$BENCH_BIN/rh-server" --config "$PEER_DIR/server.toml" >"$PEER_DIR/server.log" 2>&1 &
  echo $! > "$PEER_DIR/server.pid"
  record_identity rh-server "$(cat "$PEER_DIR/server.pid")" "$BENCH_BIN/rh-server" >/dev/null
  start_cpu_sampler "$(cat "$PEER_DIR/server.pid")" "$PEER_DIR/cpu.jsonl" || return 1
  wait_port 127.0.0.1 "$API_PORT" 20 || { err "server 未就绪"; cat "$PEER_DIR/server.log"; return 1; }

  local token
  token="$(api_login "http://127.0.0.1:$API_PORT" admin peer-Pass-1)"
  [ -n "$token" ] || { err "登录失败"; return 1; }
  read -r cid secret < <(create_client "http://127.0.0.1:$API_PORT" "$token" peer)
  printf '%s\n' "$token"  > "$PEER_DIR/token"
  printf '%s\n' "$cid"    > "$PEER_DIR/client_id"
  printf '%s\n' "$secret" > "$PEER_DIR/client_secret"

  # ★A9:端点实况证据(实际配置回读 + 身份 + env 旋钮 + nofile)
  write_endpoint_evidence server "$PEER_DIR/server.toml" "$PEER_DIR/identity.jsonl" >/dev/null

  hdr "A 侧就绪 (proto=$PROTO relay=$RELAY mux=$(mux_bool) run_id=$PEER_RUN_ID)"
  log "  控制面 :$CTL_PORT   管理面 :$API_PORT"
  log "  证据文件: $PEER_DIR/{endpoint.json,identity.jsonl,cpu.jsonl,nofile.json}"
  log "  ── 在 B(test2) 上执行 ──"
  log "  PEER_RUN_ID=$PEER_RUN_ID PROTO=$PROTO RELAY=$RELAY MUX=$MUX peer.sh client <A_IP>:$CTL_PORT $cid $secret"
}

# ============================================================
# 角色 2:B(test2)客户端 + 目标服务
# ============================================================
peer_client() {  # <a_addr> <client_id> <secret>
  local a_addr="$1" cid="$2" secret="$3"
  require_run_env client
  raise_nofile 65535
  check_nofile client "$(conc_fd_need)" || return 1
  resolve_binaries
  mkdir -p "$PEER_DIR"

  # 目标服务(绑定 0.0.0.0,直连基线要从别的机器访问)
  python3 "$BENCH_PY/target.py" \
    --tcp-port "$T_TCP" --udp-port "$T_UDP" --http-port "$T_HTTP" \
    >"$PEER_DIR/target.log" 2>&1 &
  echo $! > "$PEER_DIR/target.pid"
  wait_port 127.0.0.1 "$T_TCP" 15 || { err "目标服务未就绪"; cat "$PEER_DIR/target.log"; return 1; }

  if [ "$THP_TOOL" = "loadgen" ]; then
    # loadgen sink 只读不答(推送式压测),与 iperf3 的请求/响应协议不兼容,
    # 故换工具时 B 侧必须成对切换。端口沿用 T_IPERF,drive 无需感知。
    # 秒数给足:过期后 sink 静默退出会让后续样本全为 0。
    "$LOADGEN_BIN" sink "$T_IPERF" 7200 200000 >"$PEER_DIR/iperf.log" 2>&1 &
    echo $! > "$PEER_DIR/iperf.pid"
    wait_port 127.0.0.1 "$T_IPERF" 15 || { err "loadgen sink 未就绪"; cat "$PEER_DIR/iperf.log"; return 1; }
  else
    "$IPERF3_BIN" -s -p "$T_IPERF" >"$PEER_DIR/iperf.log" 2>&1 &
    echo $! > "$PEER_DIR/iperf.pid"
    wait_port 127.0.0.1 "$T_IPERF" 10 || { err "iperf3 未就绪"; cat "$PEER_DIR/iperf.log"; return 1; }
  fi

  gen_client_config "$PEER_DIR" "$a_addr" "$PROTO" "$cid" "$secret" "relay_mode = \"$RELAY\""
  "$BENCH_BIN/rh-client" --config "$PEER_DIR/client.toml" >"$PEER_DIR/client.log" 2>&1 &
  echo $! > "$PEER_DIR/client.pid"
  record_identity rh-client "$(cat "$PEER_DIR/client.pid")" "$BENCH_BIN/rh-client" >/dev/null
  start_cpu_sampler "$(cat "$PEER_DIR/client.pid")" "$PEER_DIR/cpu.jsonl" || return 1
  sleep 3

  # ★A9:端点实况证据;B 侧另记本机 iperf3(server 端)版本 —— 工具双端版本
  write_endpoint_evidence client "$PEER_DIR/client.toml" "$PEER_DIR/identity.jsonl" "$IPERF3_BIN" >/dev/null

  hdr "B 侧就绪 (proto=$PROTO relay=$RELAY mux=$(mux_bool) run_id=$PEER_RUN_ID)"
  log "  已连向 $a_addr;目标: iperf=$T_IPERF tcp=$T_TCP udp=$T_UDP http=$T_HTTP"
  log "  证据文件: $PEER_DIR/{endpoint.json,identity.jsonl,cpu.jsonl,nofile.json}"
  log "  ── 在 test3 上执行(token/cid 取 server 角色输出) ──"
  log "  PEER_RUN_ID=$PEER_RUN_ID PROTO=$PROTO RELAY=$RELAY MUX=$MUX peer.sh drive <A_IP> <B_IP> $API_PORT <token> $cid <label>"
}

# ============================================================
# 角色 3:test3 驱动
# ============================================================
peer_drive() {  # <a_ip> <b_ip> <api_port> <token> <client_id> [label]
  local a_ip="$1" b_ip="$2" api_port="$3" token="$4" cid="$5" label="${6:-peer}"
  require_run_env drive
  # label 只允许安全字符(要进 raw 文件名)
  case "$label" in *[!a-zA-Z0-9._-]*) die "label 含非法字符: $label";; esac
  [ -n "$token" ] || die "缺少 token(server 角色打印)"
  [ -n "$cid" ] || die "缺少 client_id"

  # ★F-Drive-Nofile:drive 自己也要持有 4096+ 个 socket(G5 由本机发起)
  raise_nofile 65535
  check_nofile drive "$(conc_fd_need)" || return 1
  mkdir -p "$PEER_DIR"

  local API="http://$a_ip:$api_port"
  local P_TCP=$PROXY_BASE P_IPERF=$((PROXY_BASE+1)) P_UDP=$((PROXY_BASE+2)) P_VHOST=$((PROXY_BASE+3))

  # ★结果文件名带时间戳:永不覆盖任何历史证据
  local stamp out_file
  stamp="$(date +%Y%m%d-%H%M%S)"
  out_file="$PEER_DIR/results-$label-$stamp.jsonl"
  : > "$out_file"
  RAW_DIR="$PEER_DIR/raw"
  REC_OUT_FILE="$out_file"
  mkdir -p "$RAW_DIR"
  FAILS=0
  LG_SEQ=0

  # ---------- 工具与被测版本(元数据,跨机可信的部分) ----------
  local cli_ver iperf_ver
  cli_ver="$(curl -s -m 10 -H "Authorization: Bearer $token" "$API/api/clients" \
    | jq -r --arg id "$cid" '.data[] | select(.id==$id) | .client_version // ""' 2>/dev/null)"
  # 证据里记录**实际使用的**压测器版本(换工具后仍可追溯)
  if [ "$THP_TOOL" = "loadgen" ]; then
    iperf_ver="loadgen($LOADGEN_BIN)"
  else
    iperf_ver="$("$IPERF3_BIN" --version 2>/dev/null | head -1)"
  fi
  # ★A9:client_version 为空即中止本轮(版本不可信 → 全部数据不可信)
  if [ -z "$cli_ver" ]; then
    rec "$(jq -cn --arg run_id "$PEER_RUN_ID" --arg lbl "$label" \
      '{metric:"meta",env_error:"client_version 为空(客户端未上报/不在线),中止本轮"}')"
    err "client_version 为空,中止本轮(检查 client 是否在线)"
    return 1
  fi

  # ★schema 标记:verify_goal 靠它识别证据目录
  printf '{"schema":"goal-repair-evidence","version":1,"run_id":"%s"}\n' "$PEER_RUN_ID" \
    > "$PEER_DIR/schema.json"

  local nofile_soft
  nofile_soft="$(nofile_soft)"
  rec "$(jq -cn \
    --arg run_id "$PEER_RUN_ID" --arg lbl "$label" --arg proto "$PROTO" \
    --arg relay "$RELAY" --arg mux "$(mux_bool)" --arg cli_ver "$cli_ver" \
    --arg iperf3 "$iperf_ver" --arg soft "$nofile_soft" \
    --arg duration "$PEER_DURATION" --arg repeat "$PEER_REPEAT" \
    --arg conc_levels "$PEER_CONC_LEVELS" --arg started_at "$(date -Is)" \
    '{metric:"meta",schema_version:1,run_id:$run_id,"label":$lbl,proto:$proto,relay:$relay,mux:$mux,
      client_version:$cli_ver,drive_iperf3:$iperf3,drive_nofile_soft:$soft,
      peer_duration_s:($duration|tonumber),repeat:($repeat|tonumber),
      conc_levels:($conc_levels|split(" ")|map(tonumber)),
      cc_source:"endpoints",lowat_source:"endpoints",
      config_evidence:{server:"endpoints/server/endpoint.json",client:"endpoints/client/endpoint.json"},
      started_at:$started_at}')"
  info "client_version=$cli_ver  drive iperf3=[$iperf_ver]  fd软限=$nofile_soft"

  hdr "下发规则 (proto=$PROTO relay=$RELAY mux=$(mux_bool))"
  # 清掉本客户端旧规则,保证 drive 可重入(端口冲突 2006)
  local old_ids
  old_ids="$(curl -s -m 10 -H "Authorization: Bearer $token" "$API/api/services" \
    | jq -r --arg cid "$cid" '.data[] | select(.client_id==$cid) | .id' 2>/dev/null)"
  for sid in $old_ids; do
    curl -s -m 10 -X DELETE -H "Authorization: Bearer $token" "$API/api/services/$sid" >/dev/null
  done
  [ -n "$old_ids" ] && sleep 2

  # 压缩开关:打开时规则带 transport.use_compression=true,客户端该通道
  # 回落 forward::splice 的 copy 路径(压缩流无法零拷贝)。
  local COMPRESS_JSON=""
  if [ "$PEER_COMPRESS" = "1" ]; then COMPRESS_JSON=',"transport":{"use_compression":true}'; fi
  create_service "$API" "$token" \
    "{\"client_id\":\"$cid\",\"name\":\"tcp\",\"kind\":\"tcp\",\"bind_port\":$P_TCP,\"target_host\":\"127.0.0.1\",\"target_port\":$T_TCP,\"pool_size\":4${COMPRESS_JSON},\"enabled\":true}" >/dev/null \
    || { err "tcp 规则失败"; return 1; }
  create_service "$API" "$token" \
    "{\"client_id\":\"$cid\",\"name\":\"iperf\",\"kind\":\"tcp\",\"bind_port\":$P_IPERF,\"target_host\":\"127.0.0.1\",\"target_port\":$T_IPERF,\"pool_size\":4${COMPRESS_JSON},\"enabled\":true}" >/dev/null \
    || { err "iperf 规则失败"; return 1; }
  create_service "$API" "$token" \
    "{\"client_id\":\"$cid\",\"name\":\"udp\",\"kind\":\"udp\",\"bind_port\":$P_UDP,\"target_host\":\"127.0.0.1\",\"target_port\":$T_UDP,\"enabled\":true}" >/dev/null \
    || { err "udp 规则失败"; return 1; }
  create_service "$API" "$token" \
    "{\"client_id\":\"$cid\",\"name\":\"web\",\"kind\":\"http\",\"target_host\":\"127.0.0.1\",\"target_port\":$T_HTTP,\"http\":{\"custom_domains\":[\"bench.test\"]},\"enabled\":true}" >/dev/null \
    || { err "http 规则失败"; return 1; }
  # 隧道就绪门控:规则下发后等数据面真正可用,避免把建连竞态测进吞吐
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
  [ "$gate_ok" = "1" ] || { err "隧道 30s 内未就绪,放弃(检查 client 日志)"; return 1; }
  info "隧道就绪,开始压测"

  # emit_thp <path> <host> <port> <streams> <rev> <dir> <sample>
  emit_thp() {
    local path="$1" host="$2" port="$3" streams="$4" rev="$5" dir="$6" sample="$7"
    local id="thp-$label-s$sample-$path-$dir-${streams}s" res mbps
    res="$(run_iperf "$host" "$port" "$streams" "$rev" "$id")"
    if [ "$(printf '%s' "$res" | jq -r '.valid')" != "true" ]; then
      FAILS=$((FAILS+1))
      mbps="INVALID(exit=$(printf '%s' "$res" | jq -r '.exit'))"
    else
      mbps="$(printf '%s' "$res" | jq -r '.mbps')"
    fi
    rec "$(printf '%s' "$res" | jq -c \
      --arg path "$path" --arg dir "$dir" --arg proto "$PROTO" --arg relay "$RELAY" \
      --arg mux "$(mux_bool)" --arg run_id "$PEER_RUN_ID" --arg lbl "$label" \
      --arg dst "$host" --argjson dst_port "$port" \
      --argjson streams "$streams" --argjson sample "$sample" \
      '. + {metric:"throughput",path:$path,dir:$dir,proto:$proto,relay:$relay,mux:$mux,
            run_id:$run_id,"label":$lbl,streams:$streams,sample:$sample,
            dst:$dst,dst_port:$dst_port,
            pair:("s"+($sample|tostring)+"-"+$dir+"-"+($streams|tostring)+"s")}')"
    log "  [$path] ${streams}流 $dir 第${sample}次: ${mbps} Mbps"
  }

  # ---------- 吞吐(RFC 6349;direct/tunnel 同 spec 紧邻成对) ----------
  # 标题里的工具名按 THP_TOOL 实际值渲染:写死 "iperf3" 时换 loadgen 会
  # 打印错误工具名,看输出的人会据此误判压测器(证据里 drive 行是对的,
  # 只有这个阶段标题会骗人)。
  hdr "吞吐 (${THP_TOOL}, 每档 ${PEER_DURATION}s × ${PEER_REPEAT} 次配对)"
  local sample spec streams dir rev rest
  for sample in $(seq 1 "$PEER_REPEAT"); do
    for spec in "1:upload:" "1:download:-R" "4:upload:" "4:download:-R" "16:upload:" "16:download:-R"; do
      streams="${spec%%:*}"; rest="${spec#*:}"; dir="${rest%%:*}"; rev="${rest#*:}"
      emit_thp direct "$b_ip" "$T_IPERF" "$streams" "$rev" "$dir" "$sample"
      emit_thp tunnel "$a_ip" "$P_IPERF" "$streams" "$rev" "$dir" "$sample"
    done
  done

  # ---------- UDP(1200B;A8:序号去重 + 到时排空,损失口径=排空后) ----------
  hdr "UDP (1200B, 序号去重+${PEER_UDP_DRAIN}s 排空)"
  local path host port d_u
  for sample in $(seq 1 "$PEER_REPEAT"); do
    for path in direct tunnel; do
      if [ "$path" = direct ]; then host="$b_ip" port="$T_UDP"; else host="$a_ip" port="$P_UDP"; fi
      d_u="$(run_loadgen "udp-$label-s${sample}-$path" --host "$host" --port "$port" udp \
        --sources "$PEER_UDP_SOURCES" \
        --size 1200 --duration 8 --window 32 --drain "$PEER_UDP_DRAIN")"
      loadgen_failed "$d_u" || true
      rec "$(printf '%s' "$d_u" | jq -c \
        --arg path "$path" --arg proto "$PROTO" --arg relay "$RELAY" --arg mux "$(mux_bool)" \
        --arg run_id "$PEER_RUN_ID" --arg lbl "$label" --argjson sample "$sample" \
        '{metric:"udp",path:$path,proto:$proto,relay:$relay,mux:$mux,run_id:$run_id,"label":$lbl,sample:$sample,data:.}')"
      log "  [$path] 第${sample}次: $(printf '%s' "$d_u" | jq -r '.mbps // "INVALID"')Mbps 丢=$(printf '%s' "$d_u" | jq -r '.loss_pct // "INVALID"')% late=$(printf '%s' "$d_u" | jq -r '.late_recv // "INVALID"')"
    done
  done

  # ---------- 延迟 ----------
  hdr "延迟 (python, 5000 往返)"
  local d_lat t_lat d_nc
  d_lat="$(run_loadgen "lat-$label-direct" --host "$b_ip" --port "$T_TCP" latency --count 5000 --warmup 200)"
  t_lat="$(run_loadgen "lat-$label-tunnel" --host "$a_ip" --port "$P_TCP" latency --count 5000 --warmup 200)"
  d_nc="$(run_loadgen "latnc-$label-tunnel" --host "$a_ip" --port "$P_TCP" latency --count 500 --newconn)"
  loadgen_failed "$d_lat" || true; loadgen_failed "$t_lat" || true; loadgen_failed "$d_nc" || true
  for pair in "direct:$d_lat" "tunnel:$t_lat"; do
    rec "$(printf '%s' "${pair#*:}" | jq -c --arg path "${pair%%:*}" --arg proto "$PROTO" \
      --arg run_id "$PEER_RUN_ID" --arg lbl "$label" \
      '{metric:"latency",path:$path,proto:$proto,run_id:$run_id,"label":$lbl,data:.}')"
  done
  rec "$(printf '%s' "$d_nc" | jq -c --arg proto "$PROTO" \
    --arg run_id "$PEER_RUN_ID" --arg lbl "$label" \
    '{metric:"latency_newconn",path:"tunnel",proto:$proto,run_id:$run_id,"label":$lbl,data:.}')"
  if [ "$(printf '%s' "$d_lat" | jq -r 'has("ok") and .ok==false')" != "true" ] \
     && [ "$(printf '%s' "$t_lat" | jq -r 'has("ok") and .ok==false')" != "true" ] \
     && [ "$(printf '%s' "$d_nc" | jq -r 'has("ok") and .ok==false')" != "true" ]; then
    python3 -c "
import json
d,t,n=(json.loads(x) for x in '''$d_lat|$t_lat|$d_nc'''.split('|'))
print(f'  直连  p50={d[\"p50_ms\"]}ms p99={d[\"p99_ms\"]}ms p999={d[\"p999_ms\"]}ms')
print(f'  隧道  p50={t[\"p50_ms\"]}ms p99={t[\"p99_ms\"]}ms p999={t[\"p999_ms\"]}ms')
print(f'  开销  p50={round(t[\"p50_ms\"]-d[\"p50_ms\"],3)}ms p99={round(t[\"p99_ms\"]-d[\"p99_ms\"],3)}ms')
print(f'  新建连接 p50={n[\"p50_ms\"]}ms p99={n[\"p99_ms\"]}ms')"
  fi

  # ---------- 连接建立速率 ----------
  hdr "连接建立速率"
  local d_cr t_cr
  d_cr="$(run_loadgen "conn-$label-direct" --host "$b_ip" --port "$T_TCP" connect --connections 3000 --parallel 64)"
  t_cr="$(run_loadgen "conn-$label-tunnel" --host "$a_ip" --port "$P_TCP" connect --connections 3000 --parallel 64)"
  loadgen_failed "$d_cr" || true; loadgen_failed "$t_cr" || true
  rec "$(printf '%s' "$d_cr" | jq -c --arg proto "$PROTO" --arg run_id "$PEER_RUN_ID" --arg lbl "$label" \
    '{metric:"connect_rate",path:"direct",proto:$proto,run_id:$run_id,"label":$lbl,data:.}')"
  rec "$(printf '%s' "$t_cr" | jq -c --arg proto "$PROTO" --arg run_id "$PEER_RUN_ID" --arg lbl "$label" \
    '{metric:"connect_rate",path:"tunnel",proto:$proto,run_id:$run_id,"label":$lbl,data:.}')"
  log "  直连 $(printf '%s' "$d_cr" | jq -r '.rate_per_s // "INVALID"')/s  隧道 $(printf '%s' "$t_cr" | jq -r '.rate_per_s // "INVALID"')/s"

  # ---------- HTTP vhost ----------
  hdr "HTTP vhost QPS"
  local d_h t_h
  d_h="$(run_loadgen "http-$label-direct" --host "$b_ip" --port "$T_HTTP" http --connections 16 --duration 8)"
  t_h="$(run_loadgen "http-$label-tunnel" --host "$a_ip" --port "$P_VHOST" http --connections 16 --duration 8 --host-header bench.test)"
  loadgen_failed "$d_h" || true; loadgen_failed "$t_h" || true
  rec "$(printf '%s' "$d_h" | jq -c --arg proto "$PROTO" --arg run_id "$PEER_RUN_ID" --arg lbl "$label" \
    '{metric:"http",path:"direct",proto:$proto,run_id:$run_id,"label":$lbl,data:.}')"
  rec "$(printf '%s' "$t_h" | jq -c --arg proto "$PROTO" --arg run_id "$PEER_RUN_ID" --arg lbl "$label" \
    '{metric:"http",path:"tunnel",proto:$proto,run_id:$run_id,"label":$lbl,data:.}')"
  log "  直连 $(printf '%s' "$d_h" | jq -r '.qps // "INVALID"')QPS  隧道 $(printf '%s' "$t_h" | jq -r '.qps // "INVALID"')QPS"

  # ---------- 并发(A12:verified = 应用层 PING/PONG 往返 + 保持期存活) ----------
  hdr "并发连接 (直连 vs 隧道; verified=应用层往返校验)"
  local sample2 n path host port out
  for sample2 in $(seq 1 "$PEER_CONC_REPEAT"); do
    for n in $PEER_CONC_LEVELS; do
      for path in direct tunnel; do
        if [ "$path" = direct ]; then host="$b_ip" port="$T_TCP"; else host="$a_ip" port="$P_TCP"; fi
        out="$(run_loadgen "conc-$label-s${sample2}-$path-${n}c" \
          --host "$host" --port "$port" concurrent \
          --connections "$n" --hold "$PEER_CONC_HOLD")"
        loadgen_failed "$out" || true
        # 环境错误(EMFILE/fd 不足)→ 记 env_error meta 并中止本轮:
        # 环境问题绝不折算成产品并发失败(§5 P3.0)
        if printf '%s' "$out" | jq -e 'has("env_error")' >/dev/null 2>&1; then
          rec "$(printf '%s' "$out" | jq -c --arg run_id "$PEER_RUN_ID" --arg lbl "$label" \
            --argjson n "$n" \
            '{metric:"meta",env_error:.env_error,tag:.tag,run_id:$run_id,"label":$lbl,n:$n}')"
          err "环境错误,中止本轮(see meta env_error record)"
          return 1
        fi
        rec "$(printf '%s' "$out" | jq -c \
          --arg path "$path" --arg proto "$PROTO" --arg relay "$RELAY" --arg mux "$(mux_bool)" \
          --arg run_id "$PEER_RUN_ID" --arg lbl "$label" --argjson n "$n" --argjson sample "$sample2" \
          '. + {metric:"concurrent",path:$path,proto:$proto,relay:$relay,mux:$mux,
                run_id:$run_id,"label":$lbl,n:$n,sample:$sample}')"
        log "  ${n}c [$path] 第${sample2}次: established=$(printf '%s' "$out" | jq -r '.established // "INVALID"') verified=$(printf '%s' "$out" | jq -r '.verified // "INVALID"') hold_alive=$(printf '%s' "$out" | jq -r '.hold_alive // "INVALID"')"
      done
    done
  done

  # ---------- 收尾 ----------
  hdr "完成"
  info "结果:   $out_file"
  info "原始:   $RAW_DIR/(iperf JSON/stderr/退出码 + loadgen stderr)"
  info "回收证据(verify_goal 需要,详见 benchmark/README.md):"
  info "  A 机: $PEER_DIR/{endpoint.json,identity.jsonl,cpu.jsonl,nofile.json} → <ev>/endpoints/server/"
  info "  B 机: 同名四件 → <ev>/endpoints/client/"
  info "  A/B 机跑完本轮后停采样器: kill \"\$(cat $PEER_DIR/cpu.jsonl.pid)\""
  info "  build 清单: python3 $BENCH_PY/build_manifest.py <repo根> <target/release> → <ev>/build/"
  if [ "$FAILS" -gt 0 ]; then
    err "本轮存在 $FAILS 个无效样本/失败子项(结果文件中 valid:false 或 ok:false;原始输出在 raw/)—— 退出码非 0"
    return 1
  fi
  ok "本轮全部子项有效(逐项判定仍以 py/verify_goal.py --evidence 为准)"
}

main() {
  local cmd="${1:-help}"
  case "$cmd" in
    server) shift; peer_server ;;
    client) shift; peer_client "$@" ;;
    drive)  shift; peer_drive "$@" ;;
    *) sed -n '2,45p' "$0" ;;
  esac
}

# 仅作为脚本执行时进入 main;被 source(本地回归测试)时只提供函数
if [ "${BASH_SOURCE[0]}" = "$0" ]; then
  main "$@"
fi
