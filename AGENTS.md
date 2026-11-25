# Agent 工作约定(rabbithole)

## 视觉理解

- **禁用 `analyze image` 工具**(`mcp__4_5v_mcp__analyze_image`):本项目的一切图像/截图判读都不得调用该工具。
- 视觉理解需求(截图复核、UI 浏览器验收看图、图表分析等)统一交由 **glm-5.3-flash** 完成。

## 发版流程

发版由**推送 `v*` 标签**触发,流水线定义在 `.github/workflows/release.yml`。
不要手写发布脚本,也不要手动上传产物。

### 步骤

1. **合并到 main**。工作分支合入 `main`(用 `--no-ff` 保留历史),合并后**在 main 上跑全量检查**:

   ```bash
   git checkout main
   git merge --no-ff <branch> -m "merge: <说明>"
   ulimit -n 10240 && cargo test --workspace              # macOS 需先提 fd 上限;必须全绿
   cargo clippy --workspace --all-targets -- -D warnings
   ```

2. **升版本号——两处都要改,漏一处就会发错版**:

   | 文件 | 字段 |
   |---|---|
   | `Cargo.toml` | `[workspace.package].version` |
   | `console/package.json` | `version` |

   各 crate 的 `Cargo.toml` 都是 `version.workspace = true`,**只改根 `Cargo.toml`** 即可。

   `console/package.json` 的版本经 `console/vite.config.ts` 注入 `__APP_VERSION__`,
   直接显示在侧边栏(`components/layout/app-sidebar.tsx`)。**只改 Cargo.toml 会导致
   界面显示旧版本号**——v0.4.0 踩过,界面显示 v0.3.2 而实际是 0.4.0。

   改完执行 `cargo update --workspace` 同步 `Cargo.lock`。

3. **提交并推送 main**:

   ```bash
   git add -A && git commit -m "chore: release vX.Y.Z"
   git push origin main
   ```

4. **打标签并推送**(标签是唯一触发器):

   ```bash
   git tag -a vX.Y.Z -m "vX.Y.Z: <一句话主题>"
   git push origin vX.Y.Z
   ```

5. **跟进流水线**:

   ```bash
   gh run list --limit 3
   gh run view <run-id>                     # 各 job 状态
   gh run view --log-failed --job=<job-id>  # 失败时看日志
   ```

   拿到日志若报 "the response contains terminal escape sequences",
   加 `--allow-escape-sequences` 再用 `sed 's/\x1b\[[0-9;]*m//g'` 清洗。

6. **补充 Release 说明**。流水线只生成一行 changelog 链接
   (`generate_release_notes: true`),**必须手工补写实质内容**:

   ```bash
   gh release edit vX.Y.Z --notes-file /tmp/relnotes.md
   ```

   应包含:亮点、新增/变更、修复项、**兼容性与行为变化**、产物清单。

### 版本号规则

语义化版本:`feat` 走 minor(`0.3.2` → `0.4.0`),纯 `fix`/`docs` 走 patch。

### 流水线会做什么

`console`(前端产物,供 rust-embed 内嵌)→ `build`(6 平台矩阵并行)→
`cli`(rhctl 六平台,Go 交叉编译,单 ubuntu runner,不碰 Rust 矩阵)→
`docker`(ghcr.io 多架构镜像)→ `release`(GitHub Release + 附件)。

平台:`x86_64`/`aarch64` × `linux-gnu`/`apple-darwin`/`pc-windows-msvc`
(2026-11-04 定论回 musl:cross-gnu/zig 与 cross-musl 实测等价或更差,原生构建钉 GLIBC_2.38 不可发,完整证据链见 benchmark/README;基准拉产物用 `-p "*x86_64-unknown-linux-musl*"`),
每个带 `.sha256`。Windows 目标编译较慢(约 10 分钟,其余 2~3 分钟);
rhctl 归档命名是 `rhctl-<版本>-<os>-<arch>`(Go 生态惯例,非 Rust 三元组),
版本号由流水线 `-ldflags` 从 git 标签注入 —— **不需要第三处版本登记位**
(只改 Cargo.toml 与 console/package.json 的约定不变)。

### 踩过的坑

- **发版前必须确认全平台能编译**。本地 `cargo test` 绿不代表 Windows 能过:
  v0.4.0 首次发版时两个 msvc 目标同时红,原因是 `std::os::fd` 是 Unix 专属
  模块,而 `crates/rh-transport/src/tcp.rs` 的 `try_dup_tcp` 无条件 import 了它。

  **任何新增的 `std::os::fd` / `std::os::unix` 用法都必须放在 `#[cfg(unix)]`
  内**,否则 Windows 编译失败。自查:

  ```bash
  grep -rn "std::os::fd\|std::os::unix" "--include=*.rs" crates/ vendor/
  ```

  逐个确认有 `cfg` 门控。注意 `#[cfg(test)]` 里的代码同样会参与编译,
  依赖 Unix fd 的测试要用 `#[cfg(unix)]` 关掉,不能只靠 `cfg(test)`。

- **失败后重发要先删旧标签**。流水线失败不会创建 Release,但标签还在。
  先 `git push --delete origin vX.Y.Z` + `git tag -d vX.Y.Z`,修完在**新提交**
  上重新打标签。不要 force-push 标签。

- **`release` job 依赖 `build`**(`needs: [build, docker]`),任一平台失败就
  不会发 Release。看到"没有 Release"先查是不是某个平台红了。

- **`workflow_dispatch` 手动触发不发 Release**,只构建并推 `ci-<sha>` 临时
  镜像标签,用于验证流水线本身。

### 不要做的事

- 不要手动 `gh release create` —— 让流水线建,手建会缺产物。
- 不要在标签上直接改代码(标签指向的提交一变,产物就与源码不一致)。
- 不要跳过第 6 步 —— 自动生成的说明只有一行链接,对使用者没有信息量。

## 提交与 CI 工作流

### 提交前:三道门必须全绿

> 完整测试分层与 E2E / 三机压测手顺见 **[`docs/testing-guide.md`](docs/testing-guide.md)**。

```bash
cargo fmt --all -- --check                          # 格式
cargo clippy --workspace --all-targets -- -D warnings
ulimit -n 10240 && cargo test --workspace           # macOS 需先提 nofile 上限
```

三者都在 `.github/workflows/ci.yml` 里跑,本地先过一遍能省一个来回。
格式化提交已登记在 `.git-blame-ignore-revs`,`git blame` 会跳过它。

改 `cli/`(Go,rhctl)时另有 **Go 三道门**(与 CI 的 `cli (go)` job 同款;
三条交叉编译本地零成本,不进 CI):

```bash
cd cli/rhctl
test -z "$(gofmt -l .)"        # 格式:无输出即通过
go vet ./... && go test ./...
GOOS=windows GOARCH=amd64 go build ./...   # 另跑 linux/amd64、linux/arm64
```

### 提交:推送前先推 main,再追 CI

**`main` 是唯一长期分支**,直接推,不建特性分支(本项目单人维护)。
推送后**必须**确认 CI 绿了再走下一步:

```bash
git push origin main
gh run list --limit 3                 # 找到刚才那次 run 的 id
gh run view <run-id> --json jobs \
  --jq '{status, conclusion, jobs: [.jobs[] | {name, conclusion}]}'
```

CI 有三个 job:

| job | 做什么 | 能抓到什么 |
|---|---|---|
| `test + clippy (linux)` | fmt / clippy / 全量测试 | 逻辑错误;**只有 Linux 上能跑的那条背压测试**(靠 `ss` 读内核 accept 队列) |
| `编译检查 (windows)` | `cargo check --all-targets` | **v0.4.0 那类 msvc 编译失败**(`std::os::fd` 等 Unix 专属用法漏了 `cfg`) |
| `cli (go)` | `cli/rhctl` 的 gofmt / go vet / go build / go test | rhctl 的 Go 侧质量门(本地同款命令见上方 Go 三道门);交叉编译不进 CI |

### 看失败日志

`gh run view --log-failed` 可能因为终端转义序列被拒,用 API 直取:

```bash
JOB=$(gh api "repos/<owner>/<repo>/actions/runs/<run-id>/jobs" \
  --jq '.jobs[] | select(.conclusion=="failure") | .id')
gh api --allow-escape-sequences "repos/<owner>/<repo>/actions/jobs/$JOB/logs" \
  | sed 's/\x1b\[[0-9;]*m//g' | grep -E "FAILED|panicked|thread '" -A 5
```

(`--allow-escape-sequences` 是必需的,否则 `gh` 直接拒绝输出。)

### 本地绿 ≠ CI 绿:两类踩过的坑

1. **内核参数差异**。`sndbuf_explicit_set_takes_effect` 曾硬编码 4 MiB,
   而 Linux 的 `SO_SNDBUF` 受 `net.core.wmem_max` 约束、超出部分**静默截断**:
   开发机 16 MiB(绿),GitHub Actions 容器 212992(红,
   `target=4194304 got=425984`)。**断言不要依赖本机内核调参**,先读上限
   再定目标。
2. **平台条件编译**。macOS 上 `cargo test` 绿不代表 Windows 能编 ——
   `std::os::fd` / `std::os::unix` 必须放在 `cfg(unix)` 内,`#[cfg(test)]`
   里的代码同样参与编译。自查:

   ```bash
   grep -rn "std::os::fd\|std::os::unix" "--include=*.rs" crates/ vendor/
   ```

   逐个确认有 `cfg` 门控。这条**只能靠 `windows-check` job 验证** ——
   开发机是 macOS,`rustup target add x86_64-pc-windows-msvc` 装不上,
   本地无法交叉验证。

### 什么时候不要加门禁

**红的门禁比没有门禁更糟** —— 它很快会被无视。给 CI 加检查前先确认
当前代码能过。反例:仓库曾有 94 处 `cargo fmt` 差异,当时就没加 fmt 检查;
先把格式化单独做成一次提交(登记进 `.git-blame-ignore-revs`),再开门禁。

### 提交信息

写**为什么**,不写**做了什么**(diff 自己会说)。涉及判断的要写清依据:

- 判定"不是缺陷"的,给出反证(哪段源码、哪次实测);
- 判定"不做"的,写进 `docs/not-supported.md` 并给 frp 源码位置 ——
  否则会被反复当成遗漏重新提起;
- 承认修正的,直接写"我之前判错了,原因是什么",不要悄悄改掉。

### 不要做的事

- 不要 `git add -A` 后不看 `git status` 就提交 —— 工作区可能有别人的改动。
  只 `git add` 自己动过的文件。
- 不要把纯格式化与业务改动混在一个提交里(review 时真正要看的东西会被淹没)。
- 不要用 tar 传源码给测试机(见下方"性能基准"第 1 步)—— 走 git,
  保证结果能绑定到 commit。
- 不要手动 `gh release create`。

## 测试与性能基准

> **完整可复制操作手册见 [`docs/testing-guide.md`](docs/testing-guide.md)**（涵盖：一、Rust/Go/前端本地门禁；二、`tests/` 全部 8+1 个本机多进程 E2E 脚本；三、三机 10GbE `termark` + Python `Popen` 编排模板、`+3 min` Sample 1 门控、证据回收核验与文档归档步骤；四、vhost 专项基准与 `RH_*` 性能诊断旋钮）。下文仅保留快速索引与硬性红线。

### 四层测试路由

1. **第一层 · 提交前质量门**：Rust 三道门（`cargo fmt --check`、`cargo clippy --all-targets -- -D warnings`、`ulimit -n 10240 && cargo test --workspace`）+ Go 三道门（`cli/rhctl` 含 windows/linux 交叉编译、`benchmark/loadgen` 校验）+ 前端 `console/`（`typecheck`/`lint`/`test`/`build`）+ Windows Unix-fd 门控检查。详细命令见 [`docs/testing-guide.md`](docs/testing-guide.md) §2。
2. **第二层 · 本机多进程功能 E2E**：先 `cargo build --release`（跑 `e2e-embed.sh` 前需先在 `console/` 执行 `pnpm build` 再重编服务端），按需执行 `tests/` 下的 `e2e.sh`、`e2e-transports.sh`、`e2e-relay-modes.sh`、`e2e-plugins.sh`、`e2e-vhost.sh`、`e2e-resilience.sh`、`e2e-embed.sh`、`xtcp-isolated.sh`、`load.sh`。避免并发执行多个 `e2e.sh`。用法列表见 [`docs/testing-guide.md`](docs/testing-guide.md) §3。
3. **第三层 · 三机 10GbE 性能基准（`peer.sh` 与 `peer-frp.sh`）**：所有性能结论必须来自这两套对等脚本，**不要另写临时压测脚本**（会丢 20Hz CPU/RSS、端点实况 `endpoint.json`/`identity.jsonl`、`nofile.json` 与 `raw/` 失败留痕）。定量结论要求相邻同窗背靠背实测、`PEER_REPEAT=5`、`PEER_DURATION=15`、`PEER_CONC_REPEAT=3`。
4. **第四层 · 专项与诊断**：HTTP/HTTPS vhost 拆分对照用 `benchmark/vhost-peer.sh`，多协议批跑用 `benchmark/run-pair.sh`；内核与载体排查常用 `ss -tin "sport = :17100"`、`RH_MUX_PREFETCH`、`RH_MUX_POOL_SIZE`、`RH_PUMP_STATS=1`。详见 [`docs/testing-guide.md`](docs/testing-guide.md) §5。

### 三机拓扑与主机凭据

```text
[test3: 压测端]        [test1: 服务端]           [test2: 客户端+目标]
loadgen src ──10GbE──► 转发口 20001..  ──隧道──► loadgen sink   :19101
                       控制面 17100 / API 18080  loadgen target :19102..19104
```

| 角色 | termark id | 地址 |
|---|---|---|
| test1 服务端 | `cizwDkKnGqY47eBL` | 172.16.1.6 |
| test2 客户端+目标 | `Du9x3tHGGDhcZJNx` | 172.16.1.17 |
| test3 压测端 | `tN76t9aSo3zaCwJJ` | 172.16.1.18 |

### 压测工具现状（纯 Go `loadgen`）

- Python `loadgen.py` / `target.py` 及 `sink` `SetReadBuffer(4MiB)` 导致的 cwnd 塌陷伪影**均已移除与修复**（`2babafb` / `0e75adf`）。当前 `benchmark/peer.sh` 与 `benchmark/peer-frp.sh` 默认使用纯 Go `/tmp/rh-bin/loadgen`（`THP_TOOL=loadgen`），支持真双向 `-R` `download`、`loadgen sink` 与 Go `loadgen target`。
- 三角色仍必须**显式导出**一致环境变量（`BENCH_BIN=/tmp/rh-bin LOADGEN_BIN=/tmp/rh-bin/loadgen THP_TOOL=loadgen PROTO=tcp RELAY=splice MUX=1 PEER_RUN_ID=... PEER_DIR=... PEER_REPEAT=5 PEER_DURATION=15`），test2 的 `sink` 与 `target` 由脚本自行启停，不要手工起 `iperf3 -s`。

### 硬性红线（不得违反）

1. **已发版版本必须用官方 Release 产物，禁止自编 `cargo build`**：用 `gh release download vX.Y.Z -R xbclub/rabbithole -p "*x86_64-unknown-linux-musl*"` 并校验 `.sha256`；上传/替换 `/tmp/rh-bin/` 前**先 `rm -f` 再写**避免硬链接污染，且核对 `--version` 与 `sha256sum`。只有未发版提交或代码 A/B 对照才在 `test1` 源码构建并标明「非 Release 产物」。
2. **源码走 git，不传源码 tar**：三机 `/tmp/rh-bench` 用有界 `timeout 20 env GIT_HTTP_LOW_SPEED_LIMIT=1000 GIT_HTTP_LOW_SPEED_TIME=8 git fetch -q origin main && git reset -q --hard` 同步；唯一允许本地编译上传的是 Go `benchmark/loadgen` 静态二进制。
3. **远端操作走 `termark` + Python `Popen`，禁止 `setsid ... &` 与 `pkill -f`**：`termark exec` 裸跑 `&` 会因继承 fd 卡满 60s；超时断连不会杀远端 shell，故 `git fetch` 绝不能与 `rm -rf` 串联在同一命令内。启后台与删目录标准模板、精确 `awk` 杀进程命令见 [`docs/testing-guide.md`](docs/testing-guide.md) §4.2。
4. **+3 分钟 Sample 1 门控与基线对齐**：跑 full `peer.sh` 时先设 ~200s 定时检查第 1 组样品；若直连飙高而隧道全档平顶 ~2.2–2.3G，先做短时 A/B 区分 ESXi 限速与代码回退。两轮 `rh` vs `frp` 全 6 档 `direct` 中位数偏差应落在 `±15%` 内，禁止错位硬比或只跑一遍下结论。
5. **证据回收与归档**：确认 `0` 无效/零值样本、`endpoints/*/identity.jsonl` `/proc/PID/exe` 哈希一致、`cpu.jsonl` 压缩为 `cpu.jsonl.gz` 后，按 [`docs/testing-guide.md`](docs/testing-guide.md) §4.5 归档入 `benchmark/results/` 并更新首页/基准文档，推送后必须追到 CI 全绿。

## Workflow 守则

### 核心原则:禁止给 workflow 子 agent 过重的任务

禁止向单个 workflow 子 agent 分配过重的任务。凡判定为过重的任务,必须拆散
成多个子 agent 分别执行。

主 agent(编排方)的职责是**拆分、派发与汇总**,而不是把整条流水线压给
一个子 agent。

### 过重任务的判定标准

满足以下任一条件,即视为过重,必须拆分:

1. **多阶段流程**:一个任务内包含 3 个及以上性质不同的阶段
   (如"分析 → 修改 → 测试 → 写报告")。
2. **大范围文件读写**:需要通读或修改大量文件/目录,超出单一明确目标所需。
3. **长链路依赖**:前一步的输出决定后一步的做法,需要反复决策而非按既定
   指令机械执行。
4. **单一 prompt 难以说清**:任务描述需要大量上下文铺垫,或子 agent 需要
   自行做多次架构/方案决策。

### 拆分方法

- **按阶段拆**:每个子 agent 只负责流水线中的一环(如调研、实现、验证、
  评审各归一个子 agent)。
- **按模块/文件拆**:互相独立的模块或文件各归一个子 agent。
- **每个子 agent 的 prompt 要求**:目标单一、输入路径/上下文明确、期望
  输出明确(写到哪里、返回什么结论)。

### 编排规则

- 子 agent 之间不直接通信;由主 agent 收集中间结果并派发下一环。
- 无依赖的子任务并行派发,有依赖的严格串行。
- 主 agent 阻塞等待子 agent / 后台任务结果时,一律用 **TaskOutput**
  (`block=true`)阻塞取结果,不要 `sleep` 轮询或空转重试。
- 每个子 agent 返回后,主 agent 负责校验其产出是否符合该环节的验收标准,
  再进入下一环节。

### 运行巡检

主 agent 每间隔 **45 分钟**必须探查一次 workflow 是否工作正常
(查看运行日志、进度与中间产出)。

巡检时重点识别以下异常,一经发现必须立即纠正,不得等待其自行恢复:

- **工具循环调用**:子 agent 反复调用同一工具、以相同参数反复重试。
- **循环查相同代码**:子 agent 反复读取/搜索同一批文件或同一段代码,
  无明显进展。

纠正手段包括:

1. 向对应子 agent 发送纠偏指令(明确指出当前陷入循环、给出下一步具体动作);
2. 纠偏无效时终止该子 agent,将其剩余任务重新拆分后派发给新的子 agent。
