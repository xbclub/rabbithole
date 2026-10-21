#!/usr/bin/env bash
# 三机配对跑批:同一 commit 上跑 rh(TCP/TLS)与 frp(TCP/TLS)四轮。
#
# 本脚本只是**编排**(起停进程 + 调驱动),不实现任何测量:所有场景、
# 证据采集、无效样本判定都由 peer.sh / peer-frp.sh 完成。这是 AGENTS.md
# 「不要自己写临时压测脚本」红线的合规形态。
#
# 用法(在本地执行,通过 termark 遥控):
#   BENCH_REPO=/tmp/rh-bench ./benchmark/run-pair.sh <label> proto...
#
# 典型:
#   ./benchmark/run-pair.sh r1 tcp tls
set -uo pipefail

HOST_A="cizwDkKnGqY47eBL"   # test1 172.16.1.6   服务端
HOST_B="Du9x3tHGGDhcZJNx"   # test2 172.16.1.17  客户端
HOST_C="tN76t9aSo3zaCwJJ"   # test3 172.16.1.18  压测端
A_IP=172.16.1.6
B_IP=172.16.1.17
BENCH_REPO="${BENCH_REPO:-/tmp/rh-bench}"
BENCH_BIN="${BENCH_BIN:-/tmp/rh-bin}"
PEER_DIR="${PEER_DIR:-/tmp/peer}"
DURATION="${DURATION:-15}"
REPEAT="${REPEAT:-5}"
CONC_LEVELS="${CONC_LEVELS:-64 256 1024}"
CONC_HOLD="${CONC_HOLD:-5}"

label="${1:?usage: run-pair.sh <label> <proto>...}"
shift
protos=("$@")
[ ${#protos[@]} -gt 0 ] || { echo "need at least one proto" >&2; exit 2; }

tex() { termark exec "$1" --timeout "${3:-600}" "$2"; }
kill_role() {  # <host> <pattern> <dir>
  tex "$1" "ps -eo pid,comm | awk '\$2 ~ /^($2)\$/{print \$1}' | xargs -r kill 2>/dev/null; rm -rf $3; true" 60 >/dev/null
}
json_field() { python3 -c "import json,sys;print(json.load(sys.stdin).get('output',''))"; }

# wait_drive <label> —— 轮询 drive.out 直到出现 "== 完成 ==" 或进程消失。
# drive 是 setsid 常驻(本地 termark exec 超时会切断客户端连接但不会杀远端
# 进程),因此必须显式等待,不能依赖 exec 返回。
wait_drive() {
  local lbl="$1" i
  for i in $(seq 1 240); do   # 240 × 20s = 80min 上限
    local st
    st="$(tex "$HOST_C" "grep -q '== 完成 ==' /tmp/peer-results-$lbl/drive.out 2>/dev/null && echo DONE || (pgrep -f 'peer.* drive ' >/dev/null && echo RUN || echo GONE)" 30 | json_field)"
    case "$st" in
      DONE) echo "  drive $lbl 完成"; return 0 ;;
      GONE) echo "  drive $lbl 进程已退出(未见完成标记)"; return 1 ;;
    esac
    sleep 20
  done
  echo "  drive $lbl 轮询超时"; return 1
}

echo "== 同步仓库/二进制就绪检查"
for h in "$HOST_A" "$HOST_B" "$HOST_C"; do
  tex "$h" "cd $BENCH_REPO && git rev-parse --short HEAD && ls -l $BENCH_BIN/rh-server $BENCH_BIN/rh-client $BENCH_BIN/loadgen | wc -l" 60 \
    | json_field
done

for proto in "${protos[@]}"; do
  for impl in rh frp; do
    lbl="$impl-$proto"
    echo "== [$lbl] 清理"
    kill_role "$HOST_A" "rh-server|frps" "$PEER_DIR"
    kill_role "$HOST_B" "rh-client|frpc|target.py|loadgen|iperf3" "$PEER_DIR"
    kill_role "$HOST_C" "loadgen|iperf3" "/tmp/peer-results-$lbl"
    # 证据/工作目录必须在起进程前存在(脚本只写文件不建父目录)
    tex "$HOST_A" "mkdir -p $PEER_DIR" 30 >/dev/null
    tex "$HOST_B" "mkdir -p $PEER_DIR" 30 >/dev/null
    tex "$HOST_C" "mkdir -p /tmp/peer-results-$lbl" 30 >/dev/null

    script="$BENCH_REPO/benchmark/peer.sh"
    [ "$impl" = frp ] && script="$BENCH_REPO/benchmark/peer-frp.sh"
    common="PEER_DIR=$PEER_DIR PEER_DURATION=$DURATION PEER_REPEAT=$REPEAT ${EXTRA_ENV:-} \
PEER_CONC_LEVELS='$CONC_LEVELS' PEER_CONC_HOLD=$CONC_HOLD THP_TOOL=loadgen \
LOADGEN_BIN=$BENCH_BIN/loadgen PEER_RUN_ID=$label-$lbl PROTO=$proto"

    if [ "$impl" = rh ]; then
      echo "== [$lbl] 起 server"
      tex "$HOST_A" "cd $BENCH_REPO/benchmark && BENCH_BIN=$BENCH_BIN $common RELAY=splice MUX=1 \
        setsid bash $script server > $PEER_DIR/srv.out 2>&1 < /dev/null & sleep 2; tail -5 $PEER_DIR/srv.out" 60 | json_field
      sleep 3
      srvout="$(tex "$HOST_A" "grep 'peer.sh client' $PEER_DIR/srv.out | tail -1" 30 | json_field)"
      cid="$(printf '%s' "$srvout" | awk '{print $(NF-1)}')"
      secret="$(printf '%s' "$srvout" | awk '{print $NF}')"
      if [ -z "$cid" ] || [ -z "$secret" ]; then
        echo "!! 未能从 server 输出解析客户端凭据:"; printf '%s\n' "$srvout"
        tex "$HOST_A" "cat $PEER_DIR/srv.out" 30 | json_field
        continue
      fi
      echo "== [$lbl] 起 client cid=$cid"
      tex "$HOST_B" "cd $BENCH_REPO/benchmark && BENCH_BIN=$BENCH_BIN $common RELAY=splice MUX=1 \
        setsid bash $script client $A_IP:17100 $cid $secret > $PEER_DIR/cli.out 2>&1 < /dev/null & sleep 2; tail -5 $PEER_DIR/cli.out" 60 | json_field
      sleep 3
      token="$(tex "$HOST_C" "curl -s -m 10 -X POST http://$A_IP:18080/api/auth/login -H 'Content-Type: application/json' -d '{\"username\":\"admin\",\"password\":\"peer-Pass-1\"}'" 30 | json_field | python3 -c "import json,sys;print(json.loads(sys.stdin.read()).get('data',{}).get('accessToken',''))")"
      [ -n "$token" ] || { echo "!! 登录失败"; continue; }
      echo "== [$lbl] drive"
      tex "$HOST_C" "cd $BENCH_REPO/benchmark && PEER_DIR=/tmp/peer-results-$lbl PEER_DURATION=$DURATION PEER_REPEAT=$REPEAT PEER_CONC_LEVELS='$CONC_LEVELS' PEER_CONC_HOLD=$CONC_HOLD THP_TOOL=loadgen LOADGEN_BIN=$BENCH_BIN/loadgen PEER_RUN_ID=$label-$lbl PROTO=$proto RELAY=splice MUX=1 setsid bash $script drive $A_IP $B_IP 18080 $token $cid $lbl > /tmp/peer-results-$lbl/drive.out 2>&1 < /dev/null; echo STARTED" 120 | json_field
      wait_drive "$lbl" || true
    else
      echo "== [$lbl] 起 frps"
      tex "$HOST_A" "cd $BENCH_REPO/benchmark && $common setsid bash $script server > $PEER_DIR/srv.out 2>&1 < /dev/null & sleep 2; tail -5 $PEER_DIR/srv.out" 60 | json_field
      sleep 3
      echo "== [$lbl] 起 frpc"
      tex "$HOST_B" "cd $BENCH_REPO/benchmark && $common setsid bash $script client $A_IP > $PEER_DIR/cli.out 2>&1 < /dev/null & sleep 2; tail -5 $PEER_DIR/cli.out" 60 | json_field
      sleep 3
      echo "== [$lbl] drive"
      tex "$HOST_C" "cd $BENCH_REPO/benchmark && PEER_DIR=/tmp/peer-results-$lbl PEER_DURATION=$DURATION PEER_REPEAT=$REPEAT THP_TOOL=loadgen LOADGEN_BIN=$BENCH_BIN/loadgen PROTO=$proto setsid bash $script drive $A_IP $B_IP $lbl > /tmp/peer-results-$lbl/drive.out 2>&1 < /dev/null; echo STARTED" 120 | json_field
      wait_drive "$lbl" || true
    fi

    echo "== [$lbl] 回收结果"
    for role in A B C; do
      case $role in A) h=$HOST_A;; B) h=$HOST_B;; C) h=$HOST_C;; esac
      tex "$h" "tar czf /tmp/ev-$label-$lbl-$role.tgz -C /tmp \$(ls -d peer-results-$lbl peer 2>/dev/null) 2>/dev/null; ls -l /tmp/ev-$label-$lbl-$role.tgz 2>/dev/null || echo NONE" 120 | json_field
    done
  done
done
echo "== 完成"
