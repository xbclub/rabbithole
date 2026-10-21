#!/usr/bin/env bash
# rabbithole 传输层变体 E2E:tls / ws / wss / quic / kcp 主控通道各起独立环境,
# 验证 登录+SYNC+池通道+TCP 转发+半关闭 全链路(tcp 已由 e2e.sh 覆盖,此处作对照)。
# 用法: tests/e2e-transports.sh [二进制目录(默认 target/release)]
set -u
BIN_DIR="${1:-$(dirname "$0")/../target/release}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SERVER="$ROOT/$BIN_DIR/rh-server"
CLIENT="$ROOT/$BIN_DIR/rh-client"
PASS=0; FAIL=0
ok()  { PASS=$((PASS+1)); echo "PASS: $1"; }
bad() { FAIL=$((FAIL+1)); echo "FAIL: $1"; }

# 客户端进程到控制端口的 ESTABLISHED 连接数(round-10:载体数=物理连接数)。
# 优先 ss(Linux,-tnp 带 pid),退化 lsof(macOS/Linux),最后 netstat
# (无法按 pid 过滤,只在无前两者时使用)。全部不可用 → NA,调用方 SKIP。
client_conn_count() {  # $1=控制端口 $2=客户端 pid
  python3 - "$1" "$2" <<'PYEOF'
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
    # 同一 socket 可能因 fd 副本(try_clone/dup)出现多行:按四元组去重,
    # 否则物理载体数会被重复计数。
    seen = set()
    for line in out.splitlines():
        if "ESTABLISHED" not in line:
            continue
        for tok in line.split():
            if "->" in tok:
                seen.add(tok)
                break
    return len(seen)

def count_netstat():
    out = run(["netstat", "-an", "-p", "tcp"])
    if out is None:
        return None
    # 无法按 pid 过滤:只数"本地端口 == 控制端口"的服务端侧行,并只计
    # ESTABLISHED(同一连接在内核里可能同时出现两行,本地端口过滤后仅剩
    # 服务端侧一行,不会重复)。
    n = 0
    for line in out.splitlines():
        f = line.split()
        if len(f) < 6 or "ESTABLISHED" not in f[4:]:
            continue
        local = f[3]
        if local.rsplit(":", 1)[-1].rsplit(".", 1)[-1] == port:
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

# 独立硬件上限(round-10.1 §2.5,round-10.2 §2.6):与客户端同口径但不读
# 被测日志——`max(CPU 亲和数/cgroup 配额 - 1, 1)`,再被
# /sys/class/net/*/queues/rx-* 最大值钳制,最终 [1,8]。非 Linux/无队列信息
# 时仅由 CPU 决定。
#
# 口径必须与 config.rs 的 detect_hardware_cap_with 一致。round-54 把客户端
# 从 `cpus/2` 放宽到 `cpus-1`(实测每载体仅占约 1.7 核的 21%),本脚本当时
# 漏改,一直按 `cpus/2` 算——8 核机器上给出 4 而客户端是 7。默认单载体时
# 两者都能把 K 钳到 1 故没暴露;一旦显式配 mux_pool_size>4 就会假红。
# 额外在 stderr 打印一行 cgroup/nproc 诊断:两者不一致时只 WARN(嵌套
# cgroup 下口径可能与 available_parallelism 不同),不影响判定。
indep_hw_cap() {
  python3 - <<'PYEOF'
import glob, os, sys

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
    nproc = len(os.sched_getaffinity(0))
except AttributeError:
    nproc = os.cpu_count() or 1
q = cgroup_quota()
if q:
    cpus = min(nproc, q)
else:
    cpus = nproc
cap = max(cpus - 1, 1)
queues = [len(glob.glob(os.path.join(i, "queues", "rx-*")))
          for i in glob.glob("/sys/class/net/*")]
queues = [n for n in queues if n]
if queues:
    cap = min(cap, max(queues))
cap = max(1, min(cap, 8))
print(f"[e2e-transports] hw cap diagnostic: nproc={nproc} "
      f"cgroup_quota={q if q else 'NA'} cap={cap}", file=sys.stderr)
if q and q != nproc:
    print(f"WARN: cgroup 配额 {q} 与 nproc {nproc} 不一致"
          f"(独立期望取 min,仅提示,不 FAIL)", file=sys.stderr)
print(cap)
PYEOF
}
INDEP_HW=$(indep_hw_cap)

echo "== rabbithole 传输变体 E2E =="
for pid in $(ps -eo pid,args | grep -E "rh-(client|server) --config /tmp/rh-tr-" | grep -v grep | awk '{print $1}'); do kill "$pid" 2>/dev/null; done
sleep 0.5

run_one() {  # $1=proto $2=ctl_port $3=api_port $4=bind_port $5=echo_port [6=max_carriers] [7=no_insecure] [8=label] [9=pool_size]
  local PROTO="$1" CTL="$2" API="$3" BIND="$4" ECHO="$5" MAXC="${6:-}"
  # $7=no_insecure:客户端**不写** tls_insecure_skip_verify,即走默认的
  #   "无 CA / 无 pin / 非 insecure → secret_verified" 路径。QUIC 曾经的致命
  #   缺陷正在这里:握手成功但传输层没写入对端证书 → 上层取不到 → 拒绝连接
  #   → 无限重连。其余用例都写了 insecure,永远绕开该路径,故单独加一组。
  # $8=label:同一协议跑多组配置时用于区分断言前缀与临时目录。
  # $9=pool_size:客户端显式 `mux_pool_size`。默认载体数自 de49db8 起为 1
  #   (iperf3 接收端饱和造成的假收益被证伪,默认不再建池),故想验证池化
  #   链路的用例**必须显式配**;这也正是 de49db8 保留的可选路径。
  #   不配 = 走默认(auto_carriers,环回 <5ms 恒为 1)。
  local NO_INSECURE="${7:-}" TAG="${8:-$1}" POOL="${9:-}"
  # secret_verified 组把日志开到 debug:正向断言要看到
  # "服务端证书比对通过",那是一条 tracing::debug!。
  local LOGLEVEL="info"
  if [ -n "$NO_INSECURE" ]; then LOGLEVEL="debug"; fi
  local W; W=$(mktemp -d /tmp/rh-tr-$TAG.XXXXXX)
  local PIDS=()
  cleanup() { for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null; done; sleep 0.3; rm -rf "$W"; }
  trap cleanup EXIT

  python3 - "$ECHO" <<'EOF' &
import socket, threading, sys
s = socket.socket(); s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.bind(("127.0.0.1", int(sys.argv[1]))); s.listen(16)
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

  cat > "$W/server.toml" <<EOF
bind_addr = "127.0.0.1"
bind_protocol = "$PROTO"
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
auto_range = "27500-27600"
[log]
level = "info"
EOF
  # round-10:显式服务端上限(tls 收敛场景 T-E3)。必须写在首个 [table]
  # 之前,否则会被 TOML 归入上一张表(segfault:unknown field)。
  if [ -n "$MAXC" ]; then
    printf 'max_carriers = %s\n' "$MAXC" | cat - "$W/server.toml" > "$W/server.toml.new"
    mv "$W/server.toml.new" "$W/server.toml"
  fi
  "$SERVER" --config "$W/server.toml" admin create admin Tr-123-456 >/dev/null 2>&1
  "$SERVER" --config "$W/server.toml" >"$W/server.log" 2>&1 &
  PIDS+=($!)
  sleep 2
  local TOKEN
  TOKEN=$(curl -s -X POST "http://127.0.0.1:$API/api/auth/login" -H 'Content-Type: application/json' -d '{"username":"admin","password":"Tr-123-456"}' | python3 -c 'import json,sys;print(json.load(sys.stdin)["data"]["accessToken"])' 2>/dev/null)
  if [ -z "$TOKEN" ]; then
    echo "login failed(server.log 尾部):"
    tail -n 20 "$W/server.log" 2>/dev/null
    bad "$TAG${MAXC:+ (max_carriers=$MAXC)}: API 登录"
    cleanup; trap - EXIT; return 1
  fi
  api(){ curl -s -m 15 -H "Authorization: Bearer $TOKEN" "$@"; }
  local CR CID SEC
  CR=$(api -X POST "http://127.0.0.1:$API/api/clients" -H 'Content-Type: application/json' -d '{"name":"main"}')
  CID=$(echo "$CR"|python3 -c 'import json,sys;print(json.load(sys.stdin)["data"]["id"])')
  SEC=$(echo "$CR"|python3 -c 'import json,sys;print(json.load(sys.stdin)["data"]["secret"])')

  local EXTRA=""
  case "$PROTO" in
    tls|wss|quic)
      if [ -z "$NO_INSECURE" ]; then EXTRA=$'tls_insecure_skip_verify = true'; fi
      ;;
  esac
  cat > "$W/client.toml" <<EOF
server_addr = "127.0.0.1:$CTL"
protocol = "$PROTO"
multiplex = true
client_id = "$CID"
secret = "$SEC"
$EXTRA
log_level = "$LOGLEVEL"
EOF
  if [ -n "$POOL" ]; then
    printf 'mux_pool_size = %s\n' "$POOL" >> "$W/client.toml"
  fi
  "$CLIENT" --config "$W/client.toml" >"$W/client.log" 2>&1 &
  local CLIENT_PID=$!
  PIDS+=($CLIENT_PID)
  local online=""
  for i in $(seq 1 20); do
    online=$(api "http://127.0.0.1:$API/api/clients" | python3 -c 'import json,sys;print(json.load(sys.stdin)["data"][0]["online"])' 2>/dev/null)
    [ "$online" = "True" ] && break; sleep 0.5
  done
  if [ "$online" != "True" ]; then
    bad "$TAG: 客户端上线"
    cp "$W/server.log" /tmp/tr-$TAG-server.log 2>/dev/null; cp "$W/client.log" /tmp/tr-$TAG-client.log 2>/dev/null
    cleanup; trap - EXIT; return 1
  fi
  ok "$TAG: 客户端上线"

  api -X POST "http://127.0.0.1:$API/api/services" -H 'Content-Type: application/json' \
    -d "{\"client_id\":\"$CID\",\"name\":\"tcp\",\"kind\":\"tcp\",\"bind_port\":$BIND,\"target_host\":\"127.0.0.1\",\"target_port\":$ECHO,\"enabled\":true}" >/dev/null
  sleep 2
  python3 - "$BIND" <<'EOF'
import socket, sys, time
t0=time.time()
s = socket.create_connection(("127.0.0.1", int(sys.argv[1])), timeout=10)
s.sendall(b"ping\n"); s.shutdown(socket.SHUT_WR); s.settimeout(8)
g=b""
while True:
    d=s.recv(256)
    if not d: break
    g+=d
sys.exit(0 if g==b"ping\n" and time.time()-t0<8 else 1)
EOF
  rc=$?
  if [ $rc -ne 0 ]; then
    bad "$TAG: 转发+半关闭"
    cp "$W/server.log" /tmp/tr-$TAG-server.log 2>/dev/null; cp "$W/client.log" /tmp/tr-$TAG-client.log 2>/dev/null
  else
    ok "$TAG: 转发+半关闭"
  fi

  # secret_verified 路径的正向断言(仅 $7=no_insecure 组)。
  # 光"能上线"不足以证明走的是下发比对:万一客户端退化成 insecure 也能上线。
  # 必须看到服务端确实下发了 SERVER_CERT、且客户端完成了逐字节比对。
  if [ -n "$NO_INSECURE" ]; then
    local clog
    clog=$(sed -E 's/\x1b\[[0-9;]*m//g' "$W/client.log" 2>/dev/null)
    if printf '%s' "$clog" | grep -q "服务端证书比对通过"; then
      ok "$TAG: 客户端完成证书下发比对(secret_verified 路径)"
    else
      bad "$TAG: 未见\"服务端证书比对通过\"(未真正走 secret 校验路径)"
      cp "$W/client.log" /tmp/tr-$TAG-client.log 2>/dev/null
    fi
    if printf '%s' "$clog" | grep -qE "可能存在中间人|无法完成比对|缺失该帧"; then
      bad "$TAG: 出现证书校验失败日志(应通过)"
      cp "$W/client.log" /tmp/tr-$TAG-client.log 2>/dev/null
    else
      ok "$TAG: 无证书校验失败日志"
    fi
  fi

  # round-10 定容断言(§4 T-E1/T-E2/T-E3):
  #   - 期望 K 不能硬编码:tls/wss 基线 2 仍要经硬件上限钳制。期望值来自
  #     **独立来源**(协议基线 + 本脚本的 indep_hw_cap),不取被测日志的
  #     hardware_cap(round-10.1 §2.5:期望值不得自指)。服务端显式
  #     max_carriers 更小时再压到该值。
  #   - 收敛(T-E3):服务端 max_carriers=1 → 最终物理连接 = 1;
  #     initial_k>1 时日志必须有 "pool carriers converged" 与 detached 计数。
  #   - log_initial 必须等于独立期望;不一致即 FAIL(不再只比 final)。
  # 计数不可用(无 ss/lsof/netstat)时 SKIP 并标注,不伪造通过。
  local proto_base="" sizing="" hw="" log_initial="" log_final="" expect_k="" i
  local expect_initial=""
  # 协议基线期望值,**独立**于被测日志:显式配了 mux_pool_size 就是该值,
  # 否则是 default_carriers(=1,tcp/tls/ws/wss 全部)。quic/kcp 恒单载体,
  # 不做定容断言(不参与下面的 proto_base 分支)。
  case "$PROTO" in
    tcp|tls|ws|wss) proto_base=${POOL:-1} ;;
  esac
  if [ -n "$proto_base" ]; then
    # 日志带 tracing 的 ANSI 颜色,先剥转义再匹配。定容日志在报到收敛后
    # 才写出,轮询等它落地(与上线判定存在毫秒级窗口)。
    for i in $(seq 1 20); do
      sizing=$(sed -E 's/\x1b\[[0-9;]*m//g' "$W/client.log" 2>/dev/null | grep "pool sizing" | tail -1)
      [ -n "$sizing" ] && break
      sleep 0.5
    done
    hw=$INDEP_HW
    log_initial=$(printf '%s' "$sizing" | sed -nE 's/.*initial_k=([0-9]+).*/\1/p')
    log_final=$(printf '%s' "$sizing" | sed -nE 's/.*final_k=([0-9]+).*/\1/p')
    if [ -z "$sizing" ] || [ -z "$log_initial" ] || [ -z "$log_final" ]; then
      bad "$TAG: 缺少 pool sizing 日志(实际: $sizing)"
      cp "$W/client.log" /tmp/tr-$TAG-client.log 2>/dev/null
    else
      ok "$TAG: pool sizing 日志(${sizing#*pool sizing})"
      expect_k=$proto_base
      [ "$expect_k" -gt "$hw" ] && expect_k=$hw
      [ "$expect_k" -lt 1 ] && expect_k=1
      if [ -n "$MAXC" ] && [ "$MAXC" -lt "$expect_k" ]; then
        expect_k=$MAXC
      fi
      # initial_k 必须等于独立期望(硬件钳制后)。max_carriers 只作用于
      # 报到后的收敛,不改变 initial_k,故此处不并入 MAXC。
      local expect_initial=$proto_base
      [ "$expect_initial" -gt "$hw" ] && expect_initial=$hw
      [ "$expect_initial" -lt 1 ] && expect_initial=1
      if [ "$log_initial" = "$expect_initial" ]; then
        ok "$TAG: initial_k=$log_initial(独立期望 $expect_initial,硬件上限 $hw)"
      else
        bad "$TAG: initial_k=$log_initial,独立期望 $expect_initial(硬件上限 $hw)"
        cp "$W/client.log" /tmp/tr-$TAG-client.log 2>/dev/null
      fi
      if [ "$log_final" = "$expect_k" ]; then
        ok "$TAG: 定容 final_k=$log_final(期望 $expect_k,硬件上限 $hw)"
      else
        bad "$TAG: 定容 final_k=$log_final,期望 $expect_k(硬件上限 $hw)"
        cp "$W/client.log" /tmp/tr-$TAG-client.log 2>/dev/null
      fi
      local got="NA"
      for i in $(seq 1 20); do
        got=$(client_conn_count "$CTL" "$CLIENT_PID")
        [ "$got" = "$expect_k" ] && break
        sleep 0.5
      done
      if [ "$got" = "NA" ]; then
        echo "SKIP: $TAG 载体数断言(ss/lsof/netstat 均不可用)"
      elif [ "$got" = "$expect_k" ]; then
        ok "$TAG: 载体数 = $expect_k(实际 $got)"
      else
        bad "$TAG: 载体数期望 $expect_k,实际 $got"
        cp "$W/client.log" /tmp/tr-$TAG-client.log 2>/dev/null
      fi
    fi
  fi
  if [ -n "$MAXC" ] && [ "$MAXC" = "1" ] && [ "$PROTO" = "tls" ]; then
    if [ -z "$log_initial" ]; then
      : # 上面的缺日志分支已记 bad
    elif [ "$log_initial" -le 1 ]; then
      # round-10.2 §2.6:SKIP 不得自指——仅当**独立计算**的期望(与
      # log_initial 同口径的 expect_initial,硬件钳制后)也 <=1(本机确实
      # 不建池)才 SKIP;否则日志与独立期望矛盾,FAIL 并打印两者。
      # 注:expect_k 已被 MAXC=1 压到 1,不能用于判断"是否建池"。
      if [ -n "$expect_initial" ] && [ "$expect_initial" -le 1 ]; then
        echo "SKIP: tls 收敛断言(独立期望 initial_k=$expect_initial<=1,log_initial=$log_initial,本机不建池)"
      else
        bad "tls: log_initial=$log_initial<=1 但独立期望 initial_k=${expect_initial:-NA}>1,本机本应建池"
        cp "$W/client.log" /tmp/tr-$TAG-client.log 2>/dev/null
      fi
    elif grep -q "pool carriers converged" "$W/client.log"; then
      ok "tls: max_carriers=1 收敛日志(initial_k=$log_initial → final_k=$log_final)"
      # T-R5(round-10.1 §2.3):服务端上限强制生效——附加载体必须收到
      # ok:false + POOL_FULL,且服务端日志留下拒绝记录(而非 send_error)。
      if grep -qE "POOL_FULL" "$W/server.log"; then
        ok "tls: max_carriers=1 时服务端回 POOL_FULL 拒绝(日志)"
      else
        bad "tls: max_carriers=1 时服务端缺 POOL_FULL 拒绝日志"
        cp "$W/server.log" /tmp/tr-$TAG-server.log 2>/dev/null
      fi
      # T-S3:收敛后不得被既有重拨逻辑补回——跨过 REDIAL_BACKOFF_START
      # (200ms)与健康监视周期(500ms)后物理连接仍为 1。这是"先清 redial
      # 再 shutdown"顺序的直接证据(顺序颠倒会形成 5s 重拨环)。
      sleep 2
      local stable
      stable=$(client_conn_count "$CTL" "$CLIENT_PID")
      if [ "$stable" = "NA" ]; then
        echo "SKIP: tls 收敛稳定性(连接计数不可用)"
      elif [ "$stable" = "1" ]; then
        ok "tls: max_carriers=1 收敛后无重拨回弹(连接数仍为 1)"
      else
        bad "tls: 收敛后连接数回弹为 $stable"
        cp "$W/client.log" /tmp/tr-$TAG-client.log 2>/dev/null
      fi
      # 客户端侧 detach 的证据:收敛日志里的 detached 计数 >= 1。日志带
      # tracing 的 ANSI 颜色,先剥转义再匹配(否则 field=value 被颜色码割裂)。
      local conv
      conv=$(sed -E 's/\x1b\[[0-9;]*m//g' "$W/client.log" 2>/dev/null | grep "pool carriers converged" | tail -1)
      if printf '%s' "$conv" | grep -qE "detached=[1-9]"; then
        ok "tls: 收敛日志含 detached 计数"
      else
        bad "tls: 收敛日志缺 detached 计数(实际: $conv)"
        cp "$W/client.log" /tmp/tr-$TAG-client.log 2>/dev/null
      fi
    else
      bad "tls: 缺少收敛日志(pool carriers converged)"
      cp "$W/client.log" /tmp/tr-$TAG-client.log 2>/dev/null
    fi
  fi
  if [ "$PROTO" = "tls" ] && { [ -z "$MAXC" ] || [ "$MAXC" != "1" ]; } \
     && [ -n "$expect_k" ] && [ "$expect_k" -gt 1 ]; then
    # T-S1:tls 默认多载体 → 服务端必须记录至少 1 条 "pool carrier attached"
    if [ "$(grep -c "pool carrier attached" "$W/server.log" 2>/dev/null || true)" -ge 1 ]; then
      ok "tls: 服务端 carrier attach ACK 路径(attached 日志)"
    else
      bad "tls: 服务端缺 pool carrier attached 日志"
      cp "$W/server.log" /tmp/tr-$TAG-server.log 2>/dev/null
    fi
    # T-R6(round-10.1 §2.5):环回上不得出现报到失败/被拒/缺 ACK;出现
    # 任一即 FAIL(失败载体已由客户端按身份摘除,但它本不该出现)。
    if grep -qE "pool carrier attach (failed|rejected)|carrier attach ack (missing|timeout)|bad carrier attach ack" "$W/client.log"; then
      bad "tls: 附加载体出现失败/拒绝/缺 ACK 日志"
      cp "$W/client.log" /tmp/tr-$TAG-client.log 2>/dev/null
    else
      ok "tls: 附加载体全程无失败/拒绝/缺 ACK"
    fi
  fi
  cleanup; trap - EXIT
  return $rc
}

check(){ [ "$1" = "0" ] && ok "$2" || bad "$2"; }

# ---------------------------------------------------------------------------
# 反向用例(降级防护):模拟"旧服务端"——完成 HELLO/挑战应答认证后**不下发**
# SERVER_CERT。新客户端在 secret_verified 路径下必须**拒绝连接**,而不是
# 静默放行(fail-open)。若这里被改成 fail-open,中间人丢弃该帧即可绕过
# 全部证书比对,本用例会立刻变红。
#
# 为什么必须用假服务端:真服务端按设计总会下发该帧,无法制造"缺失"。
# 假服务端用同一份自签证书起 TLS(客户端握手不校验证书,信任完全来自
# secret),故 TLS 层能通、认证也能过,缺的只有应用层那一帧 —— 这样拒绝
# 才确实归因于"缺帧",而不是连接没建立。
#
# 必须 multiplex=false:多路复用下 RHTP 帧在 yamux 流内,裸 socket 读不到。
# ---------------------------------------------------------------------------
run_oldserver_no_frame() {
  local CTL="$1"
  local W; W=$(mktemp -d /tmp/rh-tr-oldnodown.XXXXXX)
  local PIDS=()
  cleanup() { for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null; done; sleep 0.3; rm -rf "$W"; }
  trap cleanup EXIT

  # 起一个真服务端,只为拿 admin/客户端凭据(假服务端不实现 DB/API)
  cat > "$W/real.toml" <<EOF
bind_addr = "127.0.0.1"
bind_protocol = "tcp"
bind_port = 1
[web]
bind_addr = "127.0.0.1"
port = $((CTL + 1))
[vhost]
http_port = 0
https_port = 0
tcpmux_port = 0
[ports]
auto_range = "28800-28810"
[log]
level = "warn"
EOF
  "$SERVER" --config "$W/real.toml" admin create admin Od-123-456 >/dev/null 2>&1
  "$SERVER" --config "$W/real.toml" >"$W/real.log" 2>&1 &
  PIDS+=($!)
  sleep 2
  local TOKEN
  TOKEN=$(curl -s -X POST "http://127.0.0.1:$((CTL + 1))/api/auth/login" \
    -H 'Content-Type: application/json' \
    -d '{"username":"admin","password":"Od-123-456"}' \
    | python3 -c 'import json,sys;print(json.load(sys.stdin)["data"]["accessToken"])' 2>/dev/null)
  if [ -z "$TOKEN" ]; then
    bad "oldnodown: 无法取得凭据(真服务端未起来)"
    cleanup; trap - EXIT; return 1
  fi
  local CR CID SEC
  CR=$(curl -s -X POST "http://127.0.0.1:$((CTL + 1))/api/clients" \
    -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
    -d '{"name":"oldnodown"}')
  CID=$(echo "$CR"|python3 -c 'import json,sys;print(json.load(sys.stdin)["data"]["id"])' 2>/dev/null)
  SEC=$(echo "$CR"|python3 -c 'import json,sys;print(json.load(sys.stdin)["data"]["secret"])' 2>/dev/null)
  if [ -z "$CID" ] || [ -z "$SEC" ]; then
    bad "oldnodown: 无法创建客户端"
    cleanup; trap - EXIT; return 1
  fi

  # 假服务端:同一份自签证书 + 完整认证 + 故意不发 SERVER_CERT
  "$SERVER" --config "$W/real.toml" gencert "$W/certs" >/dev/null 2>&1
  python3 - "$CTL" "$W/certs/cert.pem" "$W/certs/key.pem" "$SEC" >"$W/fake.log" 2>&1 <<'PYFAKE' &
import hashlib, hmac, json, socket, ssl, struct, sys, threading, time

PORT, CERT, KEY, SECRET = int(sys.argv[1]), sys.argv[2], sys.argv[3], sys.argv[4].encode()
MAGIC = b"RHTP"
HELLO, AUTH_CHALLENGE, AUTH_RESPONSE, HELLO_ACK, SERVICE_SYNC = (
    0x0001, 0x0009, 0x000A, 0x0101, 0x0102)

def recv_exact(s, n):
    b = b""
    while len(b) < n:
        d = s.recv(n - len(b))
        if not d:
            raise EOFError
        b += d
    return b

def read_frame(s):
    h = recv_exact(s, 11)
    if h[:4] != MAGIC:
        raise ValueError("bad magic %r" % h[:4])
    return struct.unpack(">H", h[5:7])[0], recv_exact(s, struct.unpack(">I", h[7:11])[0])

def write_frame(s, ty, payload):
    s.sendall(MAGIC + bytes([1]) + struct.pack(">H", ty)
              + struct.pack(">I", len(payload)) + payload)

def handle(raw):
    try:
        s = ctx.wrap_socket(raw, server_side=True)
    except Exception:
        return
    try:
        ty, p = read_frame(s)
        if ty != HELLO:
            return
        cid = json.loads(p)["client_id"]
        nonce = "old-server-nonce-0123456789abcdef"
        write_frame(s, AUTH_CHALLENGE, json.dumps({"nonce": nonce}).encode())
        ty, p = read_frame(s)
        if ty != AUTH_RESPONSE:
            return
        want = hmac.new(SECRET, ("rh2:%s:%s" % (cid, nonce)).encode(),
                        hashlib.sha256).hexdigest()
        if json.loads(p).get("auth") != want:
            print("BAD_AUTH", flush=True)
            return
        print("AUTH_OK_NO_CERT_FRAME", flush=True)
        write_frame(s, HELLO_ACK, json.dumps({
            "ok": True, "session_id": "old-session", "heartbeat_interval_s": 30,
            "server_version": "0.4.0"}).encode())
        write_frame(s, SERVICE_SYNC, json.dumps({"revision": 0, "entities": []}).encode())
        time.sleep(15)
    except Exception:
        pass
    finally:
        try: s.close()
        except Exception: pass

ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
ctx.load_cert_chain(CERT, KEY)
ctx.set_alpn_protocols(["h2", "http/1.1"])
srv = socket.socket()
srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
srv.bind(("127.0.0.1", PORT)); srv.listen(16)
print("FAKE_OLD_SERVER_UP", flush=True)
while True:
    c, _ = srv.accept()
    threading.Thread(target=handle, args=(c,), daemon=True).start()
PYFAKE
  PIDS+=($!)
  sleep 1.5
  if ! grep -q "FAKE_OLD_SERVER_UP" "$W/fake.log" 2>/dev/null; then
    bad "oldnodown: 假服务端未启动"
    head -5 "$W/fake.log" 2>/dev/null
    cleanup; trap - EXIT; return 1
  fi

  # 被测客户端:不写 insecure / CA / pin → 走 secret_verified
  cat > "$W/client.toml" <<EOF
server_addr = "127.0.0.1:$CTL"
protocol = "tls"
multiplex = false
client_id = "$CID"
secret = "$SEC"
log_level = "debug"
EOF
  "$CLIENT" --config "$W/client.toml" >"$W/client.log" 2>&1 &
  PIDS+=($!)
  sleep 6
  local clog
  clog=$(sed -E 's/\x1b\[[0-9;]*m//g' "$W/client.log" 2>/dev/null)

  # 核心断言:必须明确因"缺帧"而拒绝,且绝不能出现"比对通过"
  if printf '%s' "$clog" | grep -qE "缺失该帧|无法验证对端身份|期望服务端证书下发帧"; then
    ok "oldnodown: 客户端因缺 SERVER_CERT 帧拒绝连接(fail-closed)"
  else
    bad "oldnodown: 客户端未因缺帧拒绝(可能已 fail-open 成静默放行)"
    cp "$W/client.log" /tmp/tr-oldnodown-client.log 2>/dev/null
  fi
  if printf '%s' "$clog" | grep -q "服务端证书比对通过"; then
    bad "oldnodown: 缺帧却报告比对通过(严重:fail-open)"
    cp "$W/client.log" /tmp/tr-oldnodown-client.log 2>/dev/null
  else
    ok "oldnodown: 缺帧时未伪装成比对通过"
  fi
  # 假服务端确实认证成功过 —— 证明拒绝发生在认证之后、证书校验处
  if grep -q "AUTH_OK_NO_CERT_FRAME" "$W/fake.log" 2>/dev/null; then
    ok "oldnodown: 假服务端已完成认证(拒绝确由缺帧导致)"
  else
    bad "oldnodown: 假服务端未完成认证(用例未命中目标路径)"
    head -5 "$W/fake.log" 2>/dev/null
  fi

  cleanup; trap - EXIT
  return 0
}

# 载体数期望(自 de49db8 起默认全部为 1):
#   - tcp/ws:不配 mux_pool_size,走默认 → 恒单载体,只验连通与转发;
#   - tls/wss:显式 mux_pool_size=2,保留池化链路(报到/收敛/载体数)的覆盖
#     —— 这些断言不能用默认配置测,默认已不建池。
run_one tcp  19310 19311 19312 19319
run_one tls  19330 19331 19332 19339 "" "" "" 2
# T-E3:同一 tls 配置 + max_carriers=1 → initial_k=2 收敛到 final_k=1
run_one tls  19430 19431 19432 19439 1 "" "" 2
run_one ws   19350 19351 19352 19359
run_one wss  19370 19371 19372 19379 "" "" "" 2
run_one quic 19390 19391 19392 19399
# 红→绿证据(secret_verified 路径):quic 客户端**不写** tls_insecure_skip_verify、
# 不写 trusted_ca、不写 pin → 走"握手不校验、靠 secret 加密下发证书比对"。
# 修复前 quic::connect 从不写 peer_cert_sink → 上层取不到对端证书 → 拒绝连接
# → 无限重连(本组会卡在"客户端上线"FAIL)。
run_one quic 19450 19451 19452 19459 "" no_insecure quic-secret
run_one kcp  19410 19411 19412 19419
# 降级防护反向用例:旧服务端不下发 SERVER_CERT → 客户端必须拒绝,不得放行
run_oldserver_no_frame 19470

echo
echo "== 结果: PASS=$PASS FAIL=$FAIL =="
[ "$FAIL" = "0" ]
