#!/usr/bin/env bash
# Diagnostic only: repeat the harness's own tunnel-concurrency command (same flags peer.sh uses for
# `concurrent --connections 4096 --hold 5`) to measure how often PING/PONG verification fails.
# Not a benchmark; no performance figure is taken from this run.
set -u
ulimit -n 65535
N="${1:-30}"
D=/tmp/peer-v089-diag
A=172.16.1.6
CID=316560e1-ea0c-4857-9447-ad718bbb47b2
mkdir -p "$D"
echo "start $(date -u -Is) iterations=$N loadgen_sha256=$(sha256sum "$LOADGEN_BIN" | cut -c1-16) nofile=$(ulimit -Sn)"
TOKEN=$(curl -s -m 15 -X POST "http://$A:18080/api/auth/login" -H 'Content-Type: application/json' -d '{"username":"admin","password":"peer-Pass-1"}' | jq -r '.data.accessToken // empty')
[ -n "$TOKEN" ] || { echo "login failed"; exit 1; }
RESP=$(curl -s -m 20 -X POST "http://$A:18080/api/services" -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d "{\"client_id\":\"$CID\",\"name\":\"tcp\",\"kind\":\"tcp\",\"bind_port\":20001,\"target_host\":\"127.0.0.1\",\"target_port\":19102,\"pool_size\":4,\"enabled\":true}")
echo "service: $(echo "$RESP" | jq -c '{code:.code, id:.data.id, name:.data.name, bind:.data.bind_port, pool:.data.pool_size}')"
ON=$(curl -s -m 10 -H "Authorization: Bearer $TOKEN" "http://$A:18080/api/clients" | jq -r --arg id "$CID" '.data[] | select(.id==$id) | "online=\(.online) client_version=\(.client_version)"')
echo "client: $ON"
gate=0
for i in $(seq 1 30); do
  if python3 - "$A" 20001 <<'PY'
import socket, sys
try:
    s = socket.create_connection((sys.argv[1], int(sys.argv[2])), timeout=2)
    s.sendall(b"PING\n")
    ok = s.recv(16).startswith(b"PONG")
    s.close()
    sys.exit(0 if ok else 1)
except OSError:
    sys.exit(1)
PY
  then gate=1; break; fi
  sleep 1
done
[ "$gate" = 1 ] || { echo "gate failed"; exit 1; }
echo "tunnel ready $(date -u -Is)"
for i in $(seq 1 "$N"); do
  "$LOADGEN_BIN" --host "$A" --port 20001 concurrent --connections 4096 --hold 5 > "$D/conc-$i.json" 2> "$D/conc-$i.stderr"
  rc=$?
  jq -c --arg i "$i" --argjson rc "$rc" --arg t "$(date -u +%H:%M:%S.%3N)" '{i:$i,t:$t,rc:$rc,established:.established,verified:.verified,hold_alive:.hold_alive,failed:.failed,failures:.failures,connect_p99_ms:.connect_p99_ms,verify_p99_ms:.verify_rtt_ms.p99_ms}' "$D/conc-$i.json"
done
echo "DIAG DONE $(date -u -Is)"
