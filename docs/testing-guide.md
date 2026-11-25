# rabbithole 测试与基准操作手册（Agent Runbook）

> **状态**：现役（2026-11-25）。面向接手本仓库的 Agent，提供可直接复制执行的四层测试与基准流程、远端 `termark` 编排模板、早期异常门控与常见踩坑清单。

---

## 1. 测试分层总览（什么时候跑哪一层）

| 改动范围 | 必跑层 | 触发说明与入口 |
|---|---|---|
| 任意 Rust 代码（`crates/`、`Cargo.*`） | 第一层：Rust 三道门 | 提交前全绿，macOS 需加 `ulimit -n 10240` |
| `cli/rhctl/`（Go 控制端） | 第一层：Go 三道门 | `gofmt` / `go vet` / `go test` + 三平台交叉编译 |
| `benchmark/loadgen/`（Go 压测器） | 第一层：Go 校验 + 静态构建 | `go vet` / `go test` + `GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build` |
| `console/`（Web 控制台） | 第一层：前端门禁 | `pnpm typecheck` / `pnpm lint` / `pnpm test` / `pnpm build` |
| 协议、数据面、插件、vhost、XTCP、韧性、内嵌 UI 改动 | 第二层：本机多进程 E2E | `cargo build --release` 后跑 `tests/` 对应脚本 |
| 数据面吞吐/延迟/并发改动、或发版前后与 frp 对标 | 第三层：三机 10GbE 性能基准 | 必须通过 `benchmark/peer.sh` 与 `benchmark/peer-frp.sh` |
| HTTP/HTTPS vhost 吞吐、多协议批跑、单点剖析 | 第四层：专项基准与诊断 | `benchmark/vhost-peer.sh`、`benchmark/run-pair.sh`、`RH_*` 环境变量 |

---

## 2. 第一层：本地快速质量门（提交前必跑）

### 2.1 Rust 三道门与平台/脚本自查

```bash
# 1) Rust 格式化与静态检查（CI 严格 -D warnings）
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings

# 2) 全量单测与集成测试
# 注：macOS 默认 nofile(256/2560) 会让 thousand_entries_delivered_within_1s_no_residual_tasks
# 在万级句柄阶段失败，跑全量测试前务必提限：
ulimit -n 10240 && cargo test --workspace

# 3) Windows 编译防线：macOS 本地无法交叉编译 x86_64-pc-windows-msvc，
#    任何新增 std::os::fd / std::os::unix（含 #[cfg(test)]）都必须包在 #[cfg(unix)] 内。
#    zsh 下务必给 --include 引号，避免通配符报错：
grep -rn "std::os::fd\|std::os::unix" "--include=*.rs" crates/ vendor/

# 4) 若改动 bash 基准/测试脚本：
bash -n benchmark/lib/common.sh benchmark/peer.sh benchmark/peer-frp.sh
```

> **CI 注意**：`.github/workflows/ci.yml` 包含 `test + clippy (linux)`（含依赖 `ss` 检查内核 accept 队列的 `pool_full_keeps_connections_in_kernel_accept_queue`）、`编译检查 (windows)`（`cargo check --workspace --all-targets`）与 `cli (go)`。推送 `main` 后务必用 `gh run list` / `gh run view` 确认三者全绿。

### 2.2 Go 侧检查（`cli/rhctl` 与 `benchmark/loadgen`）

```bash
# 1) cli/rhctl（同 CI cli-go job + 本地三平台交叉编译）
(
  cd cli/rhctl
  test -z "$(gofmt -l .)"
  go vet ./... && go test ./...
  GOOS=windows GOARCH=amd64 go build ./...
  GOOS=linux   GOARCH=amd64 go build ./...
  GOOS=linux   GOARCH=arm64 go build ./...
)

# 2) benchmark/loadgen（改压测器时校验并重编 Linux 静态二进制）
(
  cd benchmark/loadgen
  test -z "$(gofmt -l .)"
  go vet ./... && go test ./...
  GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o /tmp/loadgen-linux .
)
```

### 2.3 前端 `console/`

```bash
cd console
pnpm typecheck && pnpm lint && pnpm test && pnpm build
```

> 若仅测纯 Rust CI 路径（不测内嵌 UI），注意不要留无意构建出的占位 `console/dist` 污染排查；需要验证单二进制内嵌控制台时，先 `cd console && pnpm build` 再在仓库根目录执行 `cargo build --release`（`build.rs` 检测到 `console/dist/index.html` 时自动开启 `embed_ui`）。

---

## 3. 第二层：本机多进程功能 E2E（`tests/`）

全部脚本默认从 `target/release/{rh-server,rh-client}` 取二进制，运行前先 `cargo build --release`。脚本使用独立临时目录 `/tmp/rh-*.XXXXXX` 并在退出时保留快照 `/tmp/rh-*-last*`，**避免并发同时跑多个 `e2e.sh`**（优雅关停会 wait 最多 35s 释放 2×MSL `TIME_WAIT` 端口）。

| 脚本 | 覆盖场景 | 前置与命令 |
|---|---|---|
| `tests/e2e.sh` | 登录、规则 SYNC、TCP/UDP/HTTP vhost、池通道、半关闭、健康检查、基础配额等全量主链路 | `bash tests/e2e.sh target/release`（自测脚本辅助：`bash tests/e2e.sh --self-test`） |
| `tests/e2e-transports.sh` | 控制面变体：`tls` / `ws` / `wss` / `quic` / `kcp` 全链路与载体数校验 | `bash tests/e2e-transports.sh target/release` |
| `tests/e2e-relay-modes.sh` | `relay_mode=splice` vs `copy`、Prometheus `rabbithole_relay_path_total`（`chunk`/`copy`/`splice`）断言 | `bash tests/e2e-relay-modes.sh target/release` |
| `tests/e2e-plugins.sh` | 客户端插件：`http_proxy`、`socks5`、`https2http`、`https2https`、`http2https`、`http2http`、`tls2raw`（含负向分支） | `bash tests/e2e-plugins.sh target/release` |
| `tests/e2e-vhost.sh` | HTTPS SNI 透传与未知 SNI 拒绝、HTTP 头改写、Basic Auth、自定义 404 | `bash tests/e2e-vhost.sh target/release` |
| `tests/e2e-resilience.sh` | 目标拒连、DB 故障与回滚、数据通道断开、迟到连接、重复上线 | `bash tests/e2e-resilience.sh target/release` |
| `tests/e2e-embed.sh` | `rust-embed` 内嵌控制台单二进制托管（SPA 回退、`/api/*` JSON 404、`static_dir` 覆盖） | 需先 `cd console && pnpm build && cd .. && cargo build --release`，再 `bash tests/e2e-embed.sh target/release` |
| `tests/xtcp-isolated.sh` | XTCP 用户态双 NAT 隔离拓扑（`tests/natgw.py` STUN 打洞直连 vs 拦截回退中继） | `bash tests/xtcp-isolated.sh target/release` |
| `tests/load.sh` | 本机 100 并发 TCP 长稳压测（默认 600s） | `bash tests/load.sh target/release 600` |

---

## 4. 第三层：三机 10GbE 性能基准（`benchmark/peer.sh` 与 `benchmark/peer-frp.sh`）

所有性能结论必须来自 `benchmark/peer.sh`（rh）与 `benchmark/peer-frp.sh`（frp 对照），**禁止自写临时脚本替代**（会丢失 20Hz `/proc` CPU/RSS 采样、端点实况 `endpoint.json`/`identity.jsonl`、`nofile.json` 与 `raw/` 失败留痕）。

### 4.1 拓扑、主机与固定端口

```text
[test3: 压测端]        [test1: 服务端]           [test2: 客户端+目标]
loadgen src ──10GbE──► 转发口 20001..  ──隧道──► loadgen sink   :19101
                       控制面 17100              loadgen target :19102(tcp)
                       管理面 18080                             :19103(udp)
                                                                :19104(http)
直连基线：test3 ───────► test2 :19101..19104（不经任何隧道）
```

| 角色 | `termark` asset id | 内网 IP | 说明 |
|---|---|---|---|
| `test1` 服务端 A | `cizwDkKnGqY47eBL` | `172.16.1.6` | 8 核 Xeon / 10GbE，运行 `rh-server` / `frps` |
| `test2` 客户端 B + 目标 | `Du9x3tHGGDhcZJNx` | `172.16.1.17` | 运行 `rh-client` / `frpc` + `loadgen target/sink` |
| `test3` 压测端 C | `tN76t9aSo3zaCwJJ` | `172.16.1.18` | 运行 `peer.sh drive` / `peer-frp.sh drive` |

> **压测器现状（重要）**：旧 Python `loadgen.py` / `target.py` 及 `sink` `SetReadBuffer(4MiB)` cwnd 塌陷伪影均已彻底移除与修复（commits `2babafb` / `0e75adf`）。两脚本现已默认全部使用纯 Go `/tmp/rh-bin/loadgen`（`THP_TOOL=loadgen`），支持真双向 `-R` `download`、`loadgen sink` 与 Go `loadgen target`（`SO_REUSEPORT` 多协程 UDP + `net/http`）。为避免子 shell 歧义，仍必须在三角色显式导出 `THP_TOOL=loadgen`。

### 4.2 `termark` 远程执行规范（必须遵守）

本地操作三机统一使用 `termark`（禁用裸 `ssh`/`scp`），确保环境 `PATH` 包含 CLI：

```bash
export PATH="/usr/local/bin:/opt/homebrew/bin:$PATH"
```

1. **拉起后台守护进程或长耗时 `drive` 时，严禁裸写 `setsid ... &`**：`termark exec` 经 SSH 管道收集 stdout/stderr，裸 `&` 会继承远端 fd 导致 SSH 阻塞直到 60s RPC 超时。必须用 Python `subprocess.Popen` 彻底脱离 stdio 与会话，并通过单引号 `'EOF'` 的 `--stdin` 注入（绝不在 `--stdin << 'EOF'` 内嵌套另一个 heredoc）：

   ```bash
   termark exec <asset-id> --stdin << 'EOF'
   python3 - << 'PYEOF'
   import os, subprocess, time
   env = os.environ.copy()
   env.update({
       "BENCH_BIN": "/tmp/rh-bin",
       "LOADGEN_BIN": "/tmp/rh-bin/loadgen",
       "THP_TOOL": "loadgen",
       "PROTO": "tcp",
       "RELAY": "splice",
       "MUX": "1",
       "PEER_RUN_ID": "go-vX.Y.Z",
       "PEER_DIR": "/tmp/peer-run",
       "PEER_REPEAT": "5",
       "PEER_DURATION": "15",
       "PEER_CONC_REPEAT": "3",
   })
   logf = open("/tmp/role.log", "wb")
   p = subprocess.Popen(
       ["bash", "/tmp/rh-bench/benchmark/peer.sh", "server"],
       cwd="/tmp/rh-bench/benchmark",
       env=env,
       stdin=subprocess.DEVNULL,
       stdout=logf,
       stderr=subprocess.STDOUT,
       start_new_session=True,
       close_fds=True,
   )
   logf.close()
   PYEOF
   EOF
   ```

2. **`git fetch` 必须带超时，且严禁在无界网络调用后拼接 `rm -rf`**：`termark exec` 在 60s 超时后**仅断开本地等待，不杀远端 shell**。若把 `rm -rf /tmp/peer-...` 接在卡住的 `git fetch` 之后，旧任务超时恢复后会把你刚起的新压测目录删光。始终将 `git fetch` 单独执行并加有界超时：

   ```bash
   cd /tmp/rh-bench && timeout 20 env GIT_HTTP_LOW_SPEED_LIMIT=1000 GIT_HTTP_LOW_SPEED_TIME=8 git fetch -q origin main && git reset -q --hard origin/main && git rev-parse --short HEAD
   ```

3. **精确清理进程（严禁 `pkill -f`）**：`pkill -f` 会误杀包含关键字的当前 shell。清理前先停 CPU 采样器，再用精确进程名匹配，然后核对端口已释放：

   ```bash
   for p in /tmp/peer*/cpu.jsonl.pid; do [ -f "$p" ] && kill "$(cat "$p")" 2>/dev/null || true; done
   ps -eo pid,comm | awk '$2 ~ /^(rh-server|rh-client|frps|frpc|loadgen|iperf3)$/{print $1}' | xargs -r kill 2>/dev/null || true
   sleep 0.3
   ps -eo pid,comm | awk '$2 ~ /^(rh-server|rh-client|frps|frpc|loadgen|iperf3)$/{print $1}' | xargs -r kill -9 2>/dev/null || true
   ss -ltnp | grep -E ':(17100|18080|19101|19102|19103|19104|20001|20002|20003|20004)\b' || echo "TCP free"
   ss -lunp | grep -E ':(17100|17101|17102|19103|20003)\b' || echo "UDP free"
   ```

### 4.3 被测产物与工具准备

1. **已发版版本（铁律：直接用 GitHub 官方 Release `x86_64-unknown-linux-musl`，禁止自行重编）**：

   ```bash
   rm -rf /tmp/rel-vX.Y.Z
   gh release download vX.Y.Z -R xbclub/rabbithole -D /tmp/rel-vX.Y.Z -p "*x86_64-unknown-linux-musl*"
   ( cd /tmp/rel-vX.Y.Z && shasum -a 256 -c *.sha256 && tar xzf rabbithole-vX.Y.Z-x86_64-unknown-linux-musl.tar.gz )
   shasum -a 256 /tmp/rel-vX.Y.Z/rabbithole-vX.Y.Z-x86_64-unknown-linux-musl.tar.gz \
     /tmp/rel-vX.Y.Z/rabbithole-vX.Y.Z-x86_64-unknown-linux-musl/rh-server \
     /tmp/rel-vX.Y.Z/rabbithole-vX.Y.Z-x86_64-unknown-linux-musl/rh-client
   ```

   分发到 `test1` 与 `test2` 前必须**先 `rm -f` 再 `termark upload`**（防止覆盖历史硬链接 inode），上传后 `chmod +x` 并核对 `--version` 与 `sha256sum` 与本地完全一致（注意 `common.sh` 会检查 `$BENCH_BIN` 下同时有 `rh-server` 与 `rh-client`）：

   ```bash
   for id in cizwDkKnGqY47eBL Du9x3tHGGDhcZJNx; do
     termark exec "$id" 'rm -f /tmp/rh-bin/rh-server /tmp/rh-bin/rh-client'
     termark upload "$id" /tmp/rel-vX.Y.Z/rabbithole-vX.Y.Z-x86_64-unknown-linux-musl/rh-server /tmp/rh-bin/rh-server
     termark upload "$id" /tmp/rel-vX.Y.Z/rabbithole-vX.Y.Z-x86_64-unknown-linux-musl/rh-client /tmp/rh-bin/rh-client
     termark exec "$id" 'chmod +x /tmp/rh-bin/rh-server /tmp/rh-bin/rh-client && sha256sum /tmp/rh-bin/rh-server /tmp/rh-bin/rh-client && /tmp/rh-bin/rh-server --version && /tmp/rh-bin/rh-client --version'
   done
   ```

2. **仅未发版提交或代码 A/B 对照**：三机通过 `git fetch` + `git reset --hard origin/<branch>` 同步 `/tmp/rh-bench`，在 `test1` 上执行：

   ```bash
   cd /tmp/rh-bench && sed -i 's/^strip = true/strip = false/' Cargo.toml
   ~/.cargo/bin/cargo build --release -p rh-server -p rh-client
   ```

   要编特定 tag 时用 `git worktree add /tmp/rh-vX.Y.Z vX.Y.Z`，产物替换 `/tmp/rh-bin/` 前同样先 `rm -f`，并在所有输出注明「非 Release 产物」。

3. **`loadgen` 与 `frp` 校验**：三台 `/tmp/rh-bin/loadgen` 哈希必须一致（否则用 `cd benchmark/loadgen && GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build` 重新 upload）；`test1`/`test2` 上 `/tmp/rh-bench/benchmark/.bin/frp-v0.72.0/{frps,frpc}` 需核对 `sha256sum` 与 `--version`。

### 4.4 背靠背完整实测流程（rabbithole → frp）

两轮必须在连续相邻时间窗执行（`v0.8.9` 约 `20–21` 分钟，`frp v0.72.0` 约 `17–18` 分钟），三角色环境变量完全一致：

| 变量 | 正式推荐值 | 说明 |
|---|---|---|
| `BENCH_BIN` | `/tmp/rh-bin` | 被测二进制目录（A/B 时可用 `/tmp/rel-v088` 等隔离目录） |
| `LOADGEN_BIN` | `/tmp/rh-bin/loadgen` | 纯 Go 压测器 |
| `THP_TOOL` | `loadgen` | 三机必须显式设置 |
| `PROTO` | `tcp` | 控制面协议（对照六协议时切换 `tls`/`ws`/`wss`/`quic`/`kcp`） |
| `RELAY` | `splice` | 数据面转发模式 |
| `MUX` | `1` | 启用多路复用 |
| `PEER_DIR` | `/tmp/peer-vX.Y.Z` / `/tmp/frp-peer-v072` | 与历史轮隔离，重跑前务必 `rm -rf` |
| `PEER_RUN_ID` | `<run_id>` | 三机一致 |
| `PEER_REPEAT` / `PEER_DURATION` / `PEER_CONC_REPEAT` | `5` / `15` / `3` | 定量对比要求 `15s × 5` 配对中位数；并发每档 3 轮 |

**执行顺序**：

1. **rabbithole 轮**：
   - `test1` 启 `peer.sh server`，等 `/tmp/peer-.../{client_id,client_secret,endpoint.json,cpu.jsonl.pid}` 就绪，记录 `CID` 与 `SECRET`。
   - `test2` 启 `peer.sh client 172.16.1.6:17100 <CID> <SECRET>`（由脚本自起 `loadgen sink :19101` 与 `loadgen target :19102-19104`，不要手动起）。
   - `test3` 现场登录 `http://172.16.1.6:18080/api/auth/login`（`admin` / `peer-Pass-1`，**不要复用旧 token**）拿新 `accessToken`，启 `peer.sh drive 172.16.1.6 172.16.1.17 18080 <TOKEN> <CID> <label>` 并保存 PID。
   - **等待进度请用 `schedule` 定时通知（勿用 shell `sleep` 死等）**。
2. **+3 分钟 Sample 1 早停门控（必查）**：
   - 启动 `drive` 后约 200 秒查 `/tmp/drv.log` 第 1 次配对结果：
     - 正常形态：`16s` 隧道应达 `3.0+ Gbps`、`4s` 上行 `2.8+ Gbps`。
     - 若发现 `direct 1s` 高达 `3.7–4.8 Gbps` 而**隧道全部档位（含 `4s`/`16s`）被平顶压死在 `~2.2–2.3 Gbps`**，说明 `test1` 正处于 VMware ESXi 宿主机转发限速/抖动窗。立即在 `test1` 上查 `ss -tin "sport = :17100"` 确认是否 `K=2` 且双载体均衡，并做一轮 90 秒 2 臂（上个 Release vs 当前 Release）快速 A/B 验证；确认属环境波动后，在干净时间窗重跑完整 `peer.sh`。
3. **收尾与 frp 背靠背轮**：
   - `drive` 退出且打印 `[ ok ] 本轮全部子项有效` 后，先查记录数与零值/无效样本（`rh` 为 102 条），再停 `test1`/`test2` 的 `cpu_sampler`、`rh-server`/`rh-client`/`loadgen` 并压缩 `gzip -kf $PEER_DIR/cpu.jsonl`。
   - 确认所有端口释放后，立即在相邻窗口用相同参数（`FRP_VERSION=v0.72.0`、`PROTO=tcp`、`PEER_REPEAT=5`、`PEER_DURATION=15`、`PEER_DIR=/tmp/frp-peer-v072`）在 `test1` → `test2` → `test3` 依次启动 `peer-frp.sh server`、`peer-frp.sh client 172.16.1.6`、`peer-frp.sh drive 172.16.1.6 172.16.1.17 frp-go-v072`。完成后查全 70 条记录无 0 值/错误并停进程。

### 4.5 证据回收、核验与归档规范

1. 按既有归档目录结构用 `termark download` 回收证据到本地 `benchmark/results/<pair-name>/`（例如 `benchmark/results/v0.8.9-vs-frp-v0.72.0/`）：

   ```text
   benchmark/results/<pair-name>/
   ├── REPORT.md
   ├── SHA256SUMS.txt
   ├── rh-vX.Y.Z/
   │   ├── results-<label>-<stamp>.jsonl    # 102 条（含 15s×5 吞吐、5×UDP、延迟/建连/HTTP、3×并发）
   │   ├── schema.json
   │   ├── nofile.json
   │   ├── raw/                             # 每条压测的 .json/.stderr/.exit
   │   └── endpoints/
   │       ├── server/{endpoint.json,identity.jsonl,nofile.json,cpu.jsonl.gz}
   │       └── client/{endpoint.json,identity.jsonl,nofile.json,cpu.jsonl.gz}
   └── frp-v0.72.0/
       ├── results-frp-go-v072.jsonl        # 70 条
       ├── frps.toml
       ├── frpc.toml
       ├── frp.sha256
       └── frp_sha256_checksums.txt
   ```

   > 注意：`cpu.jsonl` 原始文本约 1.6 MB，入库前务必只保留 `cpu.jsonl.gz`（约 180 KB）。

2. **验收门槛核查清单**：
   - 运行 `python3 benchmark/py/summarize.py benchmark/results/<pair-name>/rh-vX.Y.Z --out /tmp/sum`，确认 `无效样本/失败: 0 条`；若跑了六协议完整 `goal-repair` 集，再跑 `python3 benchmark/py/verify_goal.py --evidence <runs_dir>`。
   - 核对 `endpoints/{server,client}/identity.jsonl` 中 `/proc/PID/exe` SHA-256 与官方 Release 一致，`nofile.ok == true`（`65535`），`proto=tcp` / `relay_mode=splice` / `multiplex=true`。
   - **双轮直连基线对齐校验**：比较 `rh` 轮与 `frp` 轮全 6 档 `direct` 中位数偏差；偏差均应在 `±15%` 以内，若因宿主机漂移超过 `15%–20%`，需同窗重跑对齐，严禁拿严重错位的两轮硬比。
3. **同步更新文档与默认值**：撰写 `REPORT.md` 与 `SHA256SUMS.txt` 后，移除已被取代的旧 `benchmark/results/` 目录，并同步更新 `README.md`、`benchmark/README.md`、`benchmark/COMPARE-FRP.md` 及 `benchmark/lib/common.sh` 中的默认 `RH_VERSION`。提交前检查 `git status`（仅提交目标文件）并确认 GitHub Actions CI 全绿。

---

## 5. 第四层：专项基准与性能诊断旋钮

- **HTTP/HTTPS vhost 专项基准**（拆分控制面 `tcp`/`tls` 与 vhost 明文/SNI TLS）：使用 `benchmark/vhost-peer.sh`（支持 `VHOST_IMPL=rh|frp`、`PROTO=tcp|tls`、`THP_TOOL=loadgen`、`VHOST_RUN_ID=...`）。
- **多协议自动 paired 编排**：`benchmark/run-pair.sh <label> tcp tls`（内部按 `peer.sh` / `peer-frp.sh` 编排 `termark`，受超长跑批时留意 4096 并发参数 `CONC_LEVELS` 默认为 `64 256 1024`）。
- **常用诊断旋钮（定位载体不均/锁争用/预取开销时用）**：
  - 查看物理载体分布、Send-Q、重传与 RTT：`ss -tin "sport = :17100"`（`test1`）或 `ss -tin "dport = :17100"`（`test2`）。
  - `RH_MUX_PREFETCH=0|1`（关闭/开启下行全批 `pump_down` 并发预取 A/B）。
  - `RH_MUX_POOL_SIZE=N`（强制客户端物理载体数 `K=N`，跳过 RTT 探测）。
  - `RH_UP_CHUNK_CAP=<bytes>`（调整上行读块上限，默认 `256 KiB`；注意此前 A/B + `strace` 已证明保持 `256 KiB` 优于 `64 KiB`/`128 KiB`，`Staging` 无 `mmap`/`brk` 抖动）。
  - `RH_PUMP_STATS=1`（向 stderr 输出 `pump_up` / `pump_down` 读/写耗时分解）。
  - `PEER_COMPRESS=1` / `PEER_UDP_SOURCES=N`（在 `peer.sh` 拓扑上压测压缩回落 copy 路径与多源 UDP 写锁路径）。

---

## 6. 常见踩坑速查

| 现象 | 根因 | 正确处理 |
|---|---|---|
| 本地 `cargo test` 万级队列用例挂失败 | macOS 默认 `ulimit -n` 过小（256/2560） | `ulimit -n 10240 && cargo test --workspace` |
| macOS 绿但发版 Windows msvc 报错 | 测试或业务代码无条件引入 `std::os::fd`/`std::os::unix` | 加 `#[cfg(unix)]`，推送 `main` 后等 `编译检查 (windows)` 绿 |
| `termark exec` 启后台进程必卡满 60s | `setsid ... &` 继承了远端 SSH stdio fd | 用 Python `subprocess.Popen(..., start_new_session=True, close_fds=True)` |
| 重启 `drive` 几分钟后目录莫名消失 | 上一次超时断开的 `termark exec` 在 `git fetch` 恢复后继续执行了后面的 `rm -rf` | `git fetch` 必须独立执行且加 `timeout`，勿与清理命令串联 |
| `test1` 隧道全档被锁死 ~2.25 Gbps 而直连很高 | ESXi 宿主机时段性转发限速；`K=2` 均衡仍跑不满时可 A/B 验证 | 跑 +3m Sample 1 门控；A/B 确认无退化后换干净窗口整轮重跑 |
| `drive` 报 `client_version 为空` 或样本全 0 | `token` 过期（401）、旧 `iperf3 -s` 占住 `:19101` 或 `loadgen sink` 到时退出 | 登录取新 `accessToken`；跑前清干净端口；依赖 `peer.sh` 自管 `sink` |
| `RH/frp` 无法分辨，直连基线低或错位 >20% | 压测时段宿主机抖动 | 两轮背靠背相邻执行；比较全 6 档 `direct` 对齐度（`<=15%`） |
