#!/usr/bin/env bash
# Control for the tunnel diagnostic: same back-to-back cadence against the DIRECT target (no tunnel),
# i.e. exactly peer.sh's own direct-concurrency command, while sampling the driver's TIME_WAIT sockets.
set -u
ulimit -n 65535
N="${1:-30}"
D=/tmp/peer-v089-diag-direct
rm -rf "$D"; mkdir -p "$D"
echo "start $(date -u -Is) iterations=$N port_range=$(cat /proc/sys/net/ipv4/ip_local_port_range | tr '\t' '-')"
( while true; do
    echo "$(date -u +%H:%M:%S) tw_all=$(ss -H -tan state time-wait | wc -l) tw_19102=$(ss -H -tan state time-wait '( dport = :19102 )' | wc -l)"
    sleep 1
  done ) >> "$D/tw.log" 2>&1 &
SAMPLER=$!
for i in $(seq 1 "$N"); do
  "$LOADGEN_BIN" --host 172.16.1.17 --port 19102 concurrent --connections 4096 --hold 5 > "$D/conc-$i.json" 2> "$D/conc-$i.stderr"
  rc=$?
  jq -c --arg i "$i" --argjson rc "$rc" --arg t "$(date -u +%H:%M:%S)" --arg tw "$(ss -H -tan state time-wait '( dport = :19102 )' | wc -l)" '{i:$i,t:$t,tw_19102:($tw|tonumber),rc:$rc,established:.established,verified:.verified,connect_failed:.connect_failed,verify_failed:.verify_failed,failed:.failed,connect_p99_ms:.connect_p99_ms}' "$D/conc-$i.json"
done
kill "$SAMPLER" 2>/dev/null
echo "DIAG DIRECT DONE $(date -u -Is)"
