#!/usr/bin/env bash
# rabbithole benchmark —— 共享函数库。
# 被 bench.sh / lib 下各场景脚本 source。
#
# 设计约束:
#   * 本套件只做「测试」,不安装系统环境、不修改服务器配置、不碰 SSH 配置。
#   * 依赖(二进制、iperf3、jq、python3)由运行者自备,setup 只校验不代劳。
#   * 编排(把脚本送到机器上、启停进程)由运行者用自己的通道完成
#     (termark / ssh / ansible 均可),本库提供的是「在单机上要跑什么」。

set -uo pipefail

# ---------- 路径与常量 ----------
BENCH_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BENCH_LIB="$BENCH_ROOT/lib"
BENCH_PY="$BENCH_ROOT/py"
BENCH_RUN="${BENCH_RUN:-$BENCH_ROOT/runs}"

# 固定的基准版本:默认取最新公开 release,可用 RH_VERSION 覆盖。
RH_VERSION="${RH_VERSION:-v0.8.3}"
RH_REPO="${RH_REPO:-xbclub/rabbithole}"
RH_ASSET="rabbithole-${RH_VERSION}-x86_64-unknown-linux-musl.tar.gz"
RH_URL="https://github.com/${RH_REPO}/releases/download/${RH_VERSION}/${RH_ASSET}"

# ---------- 日志 ----------
c_reset=$'\033[0m'; c_red=$'\033[31m'; c_grn=$'\033[32m'
c_yel=$'\033[33m'; c_blu=$'\033[36m'; c_bold=$'\033[1m'

log()  { printf '%s\n' "$*"; }
info() { printf '%s[bench]%s %s\n' "$c_blu" "$c_reset" "$*"; }
ok()   { printf '%s[ ok ]%s %s\n' "$c_grn" "$c_reset" "$*"; }
warn() { printf '%s[warn]%s %s\n' "$c_yel" "$c_reset" "$*"; }
err()  { printf '%s[fail]%s %s\n' "$c_red" "$c_reset" "$*" >&2; }
hdr()  { printf '\n%s== %s ==%s\n' "$c_bold" "$*" "$c_reset"; }

die() { err "$*"; exit 1; }

# ---------- 依赖校验(只校验,不安装) ----------
require_cmd() {
  local missing=0
  for c in "$@"; do
    command -v "$c" >/dev/null 2>&1 || { err "缺少命令: $c"; missing=1; }
  done
  [ "$missing" = "0" ] || die "请先自行安装缺失依赖(本套件不代为安装)"
}

# ---------- fd 上限(仅影响本套件启动的子进程,不改系统配置) ----------
# 高并发下 rh-server/rh-client/压测端都需要大量 socket;默认 1024 会让
# 被测系统先于测试目标失败(os error 24),污染后续所有场景。
# ★goal-repair P3.0(F-Drive-Nofile):不再"尽力而为"。raise_nofile 返回后
# 由 record_nofile 把**实际软限**写盘(三个角色都要落 nofile.json),
# check_nofile 在软限不足时以环境错误中止 —— EMFILE 属环境问题,
# 绝不折算成产品吞吐/并发失败(loadgen 已把 EMFILE 归类 env_error)。
raise_nofile() {
  local want="${1:-65535}"
  ulimit -n "$want" 2>/dev/null || ulimit -n hard 2>/dev/null || true
}

nofile_soft() { ulimit -Sn 2>/dev/null || echo 0; }
nofile_hard() { ulimit -Hn 2>/dev/null || echo unlimited; }

# record_nofile <role> <want> —— 记录实际软/硬限到 $PEER_DIR/nofile.json
record_nofile() {
  local role="$1" want="${2:-65535}" dir="${PEER_DIR:-/tmp/rh-peer}"
  local soft hard ok
  soft="$(nofile_soft)"; hard="$(nofile_hard)"
  if [ "$soft" = "unlimited" ]; then ok=true; else
    [ "$soft" -ge "$want" ] 2>/dev/null && ok=true || ok=false
  fi
  mkdir -p "$dir"
  printf '{"role":"%s","want":%s,"soft":"%s","hard":"%s","ok":%s,"recorded_at":"%s"}\n' \
    "$role" "$want" "$soft" "$hard" "$ok" "$(date -Is)" > "$dir/nofile.json"
  printf '%s' "$soft"
}

# check_nofile <role> <need> —— 实际软限 < need 时打 env_error 并失败返回。
check_nofile() {
  local role="$1" need="$2" soft
  soft="$(record_nofile "$role" 65535)"
  if [ "$soft" = "unlimited" ]; then return 0; fi
  if ! [ "$soft" -ge "$need" ] 2>/dev/null; then
    err "环境错误:$role 实际 fd 软限=${soft},需要 ≥${need}(EMFILE 不得计入产品吞吐)"
    return 1
  fi
  info "$role fd 软限=$soft (需要 ≥$need)"
}

# ---------- 二进制获取(bench.sh / peer.sh 共用) ----------
# 默认从公开 release 下载并校验 sha256;BENCH_BIN 指向已解压目录时跳过下载。
# 任何人复跑拿到的是同一份二进制,无需 Rust 工具链。
# BENCH_BIN 显式指定时不再回落下载:曾因上传通道丢执行位而静默换成
# release 旧版,导致两端版本不一致、数据全部作废(教训固化于此)。
resolve_binaries() {
  if [ -n "${BENCH_BIN:-}" ]; then
    local miss=0 b
    for b in rh-server rh-client; do
      if [ ! -f "$BENCH_BIN/$b" ]; then
        err "BENCH_BIN=$BENCH_BIN 缺少 $b"; miss=1
      elif [ ! -x "$BENCH_BIN/$b" ]; then
        err "BENCH_BIN/$BENCH_BIN/$b 无执行位(上传通道常见),先 chmod +x"; miss=1
      fi
    done
    [ "$miss" = "0" ] || die "BENCH_BIN 已显式指定,缺失/不可执行直接失败(不回落 release)"
    info "使用本地二进制: $BENCH_BIN ($("$BENCH_BIN/rh-server" --version))"
    return 0
  fi
  local cache="$BENCH_ROOT/.bin/$RH_VERSION"
  if [ ! -x "$cache/rh-server" ]; then
    info "下载 release $RH_VERSION …"
    mkdir -p "$cache"
    local tmp="$cache/dl.tar.gz"
    curl -fsSL -o "$tmp" "$RH_URL" || die "下载失败: $RH_URL"
    local want got
    want="$(curl -fsSL "$RH_URL.sha256" 2>/dev/null | awk '{print $1}')"
    if [ -n "$want" ]; then
      got="$(sha256sum "$tmp" 2>/dev/null | awk '{print $1}')"
      [ -n "$got" ] || got="$(shasum -a 256 "$tmp" | awk '{print $1}')"
      [ "$want" = "$got" ] || die "sha256 不匹配: 期望 $want 实得 $got"
      info "sha256 校验通过"
    fi
    tar xzf "$tmp" -C "$cache" --strip-components=1
    rm -f "$tmp"
    chmod +x "$cache/rh-server" "$cache/rh-client"
  fi
  BENCH_BIN="$cache"
  info "二进制就绪: $BENCH_BIN ($("$cache/rh-server" --version))"
}

# ---------- 进程身份(结果可信度:跑的到底是哪个二进制) ----------
# record_identity <name> <pid> [exe_path] —— 记录实际运行二进制的 SHA-256 与版本到
# $PEER_DIR/identity.jsonl,并打印单行 JSON 供调用方嵌入结果。
# ★goal-repair A9:优先取 /proc/PID/exe(运行真身);无 /proc(如本地 macOS
# 语法自检)时回落到显式传入的 exe_path,并在 note 里如实标注来源。
# 之前用 md5 且失败静默留空 —— 均已改为 SHA-256 + 失败必须可见。
record_identity() {
  local name="$1" pid="$2" exe_path="${3:-}" dir="${PEER_DIR:-/tmp/rh-peer}"
  local exe_sha256="" ver="" note="" target=""
  if [ -e "/proc/$pid/exe" ]; then
    target="/proc/$pid/exe"; note="proc_exe"
  elif [ -n "$exe_path" ] && [ -f "$exe_path" ]; then
    target="$exe_path"; note="fallback_path"
  else
    note="unresolved"
  fi
  if [ -n "$target" ]; then
    exe_sha256="$(sha256sum "$target" 2>/dev/null | awk '{print $1}')"
    [ -n "$exe_sha256" ] || exe_sha256="$(shasum -a 256 "$target" 2>/dev/null | awk '{print $1}')"
    ver="$("$target" --version 2>/dev/null | head -1)"
  fi
  mkdir -p "$dir"
  printf '{"name":"%s","pid":%s,"exe_sha256":"%s","version":"%s","source":"%s","recorded_at":"%s"}\n' \
    "$name" "$pid" "$exe_sha256" "$ver" "$note" "$(date -Is)" | tee -a "$dir/identity.jsonl"
}

# ---------- 运行环境显式化(★goal-repair A9:无默认值暗中生效) ----------
# require_run_env <role> —— PROTO/RELAY/MUX/PEER_RUN_ID 必须显式设置;
# 靠 shell 默认值暗中生效正是 A9 的病根(QUIC 标签轮 proto=tcp、copy 轮
# relay=splice 都源于"驱动环境变量"冒充"端点实况")。
require_run_env() {
  local role="$1" miss=0 kv
  for kv in "PROTO=控制面协议" "RELAY=relay_mode(splice/copy)" "MUX=multiplex(0/1)" "PEER_RUN_ID=本轮 run id"; do
    local name="${kv%%=*}" desc="${kv#*=}"
    if [ -z "${!name:-}" ]; then
      err "$role 缺少显式环境变量 $name($desc);不再提供隐式默认值"
      miss=1
    fi
  done
  [ "$miss" = "0" ] || die "请显式设置 PROTO/RELAY/MUX/PEER_RUN_ID 后重试(README 有完整示例)"
  info "$role 运行参数(显式):PROTO=$PROTO RELAY=$RELAY MUX=$MUX PEER_RUN_ID=$PEER_RUN_ID"
}

# epoch_us —— 微秒级 Unix 时间戳(GNU date 的 %N 在 macOS 不可用,统一走 python)
epoch_us() { python3 -c 'import time; print(int(time.time()*1_000_000))'; }

# write_endpoint_evidence <role> <config_path> <identity.jsonl> [iperf3_bin]
# ★goal-repair A9:端点实况证据。proto/multiplex/relay_mode 从**实际生成的
# 配置文件**读回(不是驱动端环境变量);RH_NOTSENT_LOWAT/RH_QUIC_CC 记录
# rh-server/rh-client 进程真实继承到的值(未设置也显式记 "unset");
# 身份取 identity.jsonl 末条(/proc/PID/exe SHA-256);nofile 取本角色
# 刚记录的实际软限。结果落 $PEER_DIR/endpoint.json,由运行者随证据目录回收。
write_endpoint_evidence() {
  local role="$1" cfg="$2" idfile="$3" iperf="${4:-}"
  local dir="${PEER_DIR:-/tmp/rh-peer}"
  RH_ROLE="$role" RH_CFG="$cfg" RH_IDFILE="$idfile" RH_IPERF3="$iperf" \
  RH_RUN_ID="${PEER_RUN_ID:-}" RH_DIR="$dir" \
  RH_LOWAT="${RH_NOTSENT_LOWAT:-}" RH_CC="${RH_QUIC_CC:-}" \
  python3 <<'PY'
import datetime, json, os, platform, subprocess

def cfg_val(path, key):
    try:
        for line in open(path, encoding="utf-8"):
            s = line.strip()
            if s.startswith(key) and "=" in s:
                return s.split("=", 1)[1].strip().strip('"')
    except OSError:
        return None
    return None

d = os.environ["RH_DIR"]
# 取 identity.jsonl 末条带 exe_sha256 的记录(即本角色刚写入的运行真身)
ident = {}
try:
    for line in open(os.environ["RH_IDFILE"], encoding="utf-8").read().splitlines():
        try:
            rec = json.loads(line)
        except ValueError:
            continue
        if rec.get("exe_sha256"):
            ident = rec
except OSError:
    pass

iperf_ver = ""
if os.environ["RH_IPERF3"]:
    try:
        out = subprocess.run([os.environ["RH_IPERF3"], "--version"],
                             capture_output=True, text=True, timeout=10)
        iperf_ver = (out.stdout or out.stderr or "").splitlines()[0] if (out.stdout or out.stderr) else ""
    except (OSError, IndexError, subprocess.TimeoutExpired):
        iperf_ver = ""

nofile = {}
try:
    nofile = json.load(open(os.path.join(d, "nofile.json"), encoding="utf-8"))
except (OSError, ValueError):
    pass

cfg = os.environ["RH_CFG"]
doc = {
    "role": os.environ["RH_ROLE"],
    "run_id": os.environ.get("RH_RUN_ID", ""),
    "recorded_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    "host": platform.node(),
    "os": platform.platform(),
    "config": {
        "source": os.path.abspath(cfg),
        "proto": cfg_val(cfg, "bind_protocol") or cfg_val(cfg, "protocol"),
        "multiplex": cfg_val(cfg, "multiplex"),
        "relay_mode": cfg_val(cfg, "relay_mode"),
    },
    "env": {
        # 未设置必须显式为 "unset":来源一致性由 verify_goal 核对,不留空白
        "RH_NOTSENT_LOWAT": os.environ["RH_LOWAT"] or "unset",
        "RH_QUIC_CC": os.environ["RH_CC"] or "unset",
    },
    "identity": {
        "pid": ident.get("pid"),
        "exe_sha256": ident.get("exe_sha256", ""),
        "version": ident.get("version", ""),
        "source": ident.get("source", ""),
    },
    "nofile": nofile,
}
if iperf_ver:
    doc["iperf3"] = iperf_ver
out = os.path.join(d, "endpoint.json")
with open(out, "w", encoding="utf-8") as fh:
    json.dump(doc, fh, ensure_ascii=False, indent=2)
print(out)
PY
}

# ---------- 结果收集 ----------
# 每次运行落一个 JSON 结果目录,便于汇总与对比。
RESULT_DIR=""
result_init() {
  local name="$1"
  local stamp; stamp="$(date +%Y%m%d-%H%M%S)"
  RESULT_DIR="$BENCH_RUN/$stamp-$name"
  mkdir -p "$RESULT_DIR"
  printf '%s\n' "$RESULT_DIR"
}

# record <scenario> <json>
record() {
  local scenario="$1"; shift
  local payload="$1"
  [ -n "$RESULT_DIR" ] || die "result_init 未调用"
  printf '%s\n' "$payload" > "$RESULT_DIR/$scenario.json"
}

# emit <scenario> <key=value...> —— 组装 JSON 并落盘
emit() {
  local scenario="$1"; shift
  local json="{}"
  if command -v jq >/dev/null 2>&1; then
    local args=()
    for kv in "$@"; do
      args+=(--arg "${kv%%=*}" "${kv#*=}")
    done
    json="$(jq -n "${args[@]}" '$ARGS.named')"
  else
    json="$(python3 - "$@" <<'PY'
import json, sys
out = {}
for kv in sys.argv[1:]:
    k, _, v = kv.partition("=")
    out[k] = v
print(json.dumps(out))
PY
)"
  fi
  record "$scenario" "$json"
}

# ---------- 配置生成 ----------
# ★goal-repair A9:MUX 不再有隐式默认值 —— 必须由运行者显式设置
# (peer.sh 三角色经 require_run_env 强制;值域 0/1)。
MUX="${MUX:-}"
mux_bool() {
  [ -n "$MUX" ] || die "MUX 未显式设置(不再提供隐式默认;A9:不允许默认值暗中生效)"
  case "$MUX" in
    0) echo false ;;
    1) echo true ;;
    *) die "MUX 只允许 0/1,得到: $MUX" ;;
  esac
}

# gen_server_config <dir> <ctl_port> <api_port> <proto> [extra...]
# extra 追加在顶层键区(必须在首个 [table] 之前,否则会被归入上一张表)
gen_server_config() {
  local dir="$1" ctl="$2" api="$3" proto="$4"; shift 4
  cat > "$dir/server.toml" <<EOF
bind_addr = "0.0.0.0"
bind_protocol = "$proto"
bind_port = $ctl
multiplex = $(mux_bool)
$*

[web]
bind_addr = "0.0.0.0"
port = $api

[vhost]
http_port = 0
https_port = 0
tcpmux_port = 0

[ports]
auto_range = "20000-21000"
allow_ports = ["10000-40000"]
max_services_per_client = 256

[limits]
# 并发上限走默认(0=不限):基准要测的是系统层瓶颈,应用层不得先卡
default_pool_size = 4

[metrics]
enable_prometheus = true
bind_addr = "127.0.0.1"
port = $((api + 1000))

[log]
level = "warn"
EOF
}

# gen_client_config <dir> <server_addr> <proto> <id> <secret> [extra...]
gen_client_config() {
  local dir="$1" addr="$2" proto="$3" id="$4" secret="$5"; shift 5
  local tls_extra=""
  case "$proto" in
    tls|wss|quic) tls_extra="tls_insecure_skip_verify = true" ;;
  esac
  cat > "$dir/client.toml" <<EOF
server_addr = "$addr"
protocol = "$proto"
multiplex = $(mux_bool)
client_id = "$id"
secret = "$secret"
$tls_extra
log_level = "warn"
$*
EOF
}

# ---------- 控制面 API ----------
api_login() {  # <api_base> <user> <pass> → token
  curl -s -m 15 -X POST "$1/api/auth/login" \
    -H 'Content-Type: application/json' \
    -d "{\"username\":\"$2\",\"password\":\"$3\"}" \
    | jq -r '.data.accessToken // empty'
}

api_post() {  # <api_base> <token> <path> <json>
  curl -s -m 20 -X POST "$1$3" -H "Authorization: Bearer $2" \
    -H 'Content-Type: application/json' -d "$4"
}

# create_client <api_base> <token> <name> → "id secret"
create_client() {
  local resp
  resp="$(api_post "$1" "$2" "/api/clients" "{\"name\":\"$3\"}")"
  local id secret
  id="$(echo "$resp" | jq -r '.data.id // empty')"
  secret="$(echo "$resp" | jq -r '.data.secret // empty')"
  [ -n "$id" ] || { err "创建客户端失败: $resp"; return 1; }
  printf '%s %s\n' "$id" "$secret"
}

# create_service <api_base> <token> <json> → service id
create_service() {
  local resp
  resp="$(api_post "$1" "$2" "/api/services" "$3")"
  local id
  id="$(echo "$resp" | jq -r '.data.id // empty')"
  [ -n "$id" ] || { err "创建规则失败: $resp"; return 1; }
  printf '%s\n' "$id"
}

# wait_online <api_base> <token> <client_id> <timeout_s>
wait_online() {
  local api="$1" tok="$2" cid="$3" tmo="${4:-30}"
  local deadline=$(( $(date +%s) + tmo ))
  while [ "$(date +%s)" -lt "$deadline" ]; do
    local on
    on="$(curl -s -m 10 -H "Authorization: Bearer $tok" "$api/api/clients" \
      | jq -r --arg id "$cid" '.data[] | select(.id==$id) | .online' 2>/dev/null)"
    [ "$on" = "true" ] && return 0
    sleep 0.5
  done
  return 1
}

# ---------- 进程管理(仅管理本套件自己启动的进程) ----------
BENCH_PIDS=()
track() { BENCH_PIDS+=("$1"); }

stop_tracked() {
  local alive=()
  for p in "${BENCH_PIDS[@]:-}"; do
    [ -n "$p" ] || continue
    kill "$p" 2>/dev/null && alive+=("$p")
  done
  # 等进程真正退出(最多 5s),否则下一个场景绑端口会 Address already in use
  local waited=0
  while [ ${#alive[@]} -gt 0 ] && [ $waited -lt 50 ]; do
    local rest=()
    for p in "${alive[@]}"; do
      kill -0 "$p" 2>/dev/null && rest+=("$p")
    done
    alive=("${rest[@]:-}")
    [ ${#alive[@]} -gt 0 ] || break
    sleep 0.1; waited=$((waited + 1))
  done
  for p in "${alive[@]:-}"; do
    kill -9 "$p" 2>/dev/null
  done
  BENCH_PIDS=()
}

# 等待端口可连(用 python,避免依赖 nc 语义差异)
wait_port() {  # <host> <port> <timeout_s>
  python3 - "$1" "$2" "${3:-20}" <<'PY'
import socket, sys, time
host, port, tmo = sys.argv[1], int(sys.argv[2]), float(sys.argv[3])
end = time.time() + tmo
while time.time() < end:
    try:
        s = socket.create_connection((host, port), timeout=1)
        s.close(); sys.exit(0)
    except OSError:
        time.sleep(0.2)
sys.exit(1)
PY
}
