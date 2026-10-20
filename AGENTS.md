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
   cargo test --workspace                              # 必须全绿
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
`docker`(ghcr.io 多架构镜像)→ `release`(GitHub Release + 附件)。

平台:`x86_64`/`aarch64` × `linux-musl`/`apple-darwin`/`pc-windows-msvc`,
每个带 `.sha256`。Windows 目标编译较慢(约 10 分钟,其余 2~3 分钟)。

### 踩过的坑

- **发版前必须确认全平台能编译**。本地 `cargo test` 绿不代表 Windows 能过:
  v0.4.0 首次发版时两个 msvc 目标同时红,原因是 `std::os::fd` 是 Unix 专属
  模块,而 `crates/rh-transport/src/tcp.rs` 的 `try_dup_tcp` 无条件 import 了它。

  **任何新增的 `std::os::fd` / `std::os::unix` 用法都必须放在 `#[cfg(unix)]`
  内**,否则 Windows 编译失败。自查:

  ```bash
  grep -rn "std::os::fd\|std::os::unix" --include=*.rs crates/ vendor/
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

## 性能基准(三机 peer 基准)

所有性能结论必须来自 `benchmark/peer.sh`(rh)与 `benchmark/peer-frp.sh`(frp
对照),**不要自己另写压测脚本**。两脚本同拓扑、同端口、同记录格式,结果可比。

### 拓扑

```
[test3: 压测端]        [test1: 服务端]           [test2: 客户端+目标]
loadgen/iperf3 -c ──►  转发口 20001..  ──隧道──►  目标服务 19101..19104
                      控制面 17100 / API 18080
```

- **直连**:压测端 → test2 目标服务,不经隧道。这是**标尺**,反映机器与网络上限。
- **隧道**:压测端 → test1 转发口 → 隧道 → test2 目标服务。
- 唯一有意义的指标是**保留率 = 隧道 / 直连**。绝对值随机器浮动,跨环境不可比。

### 主机与凭据

| 角色 | termark id | 地址 |
|---|---|---|
| test1 服务端 | `cizwDkKnGqY47eBL` | 172.16.1.6 |
| test2 客户端 | `Du9x3tHGGDhcZJNx` | 172.16.1.17 |
| test3 压测端 | `tN76t9aSo3zaCwJJ` | 172.16.1.18 |

三台均 8 核、10GbE、`sudo -n` 可用、`iperf3 3.9`+`python3` 就位。

### 流程

1. **代码进 git,机器拉取**。**rh 源码与 rh 二进制都不要用 tar 传**:

   ```bash
   # 三台都执行
   cd /tmp/rh-bench && git fetch -q origin <branch> \
     && git reset -q --hard origin/<branch>
   ```

   基准结果必须能绑定到某个 commit。tar 传递无法追溯,事后无法复核。
   (唯一例外是 `benchmark/loadgen` 的**静态二进制**:三台没装 Go,只能本地
   `GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build` 后 upload 到
   `/tmp/rh-bin/loadgen`。它是工具不是被测对象,与 commit 无关。)

2. **构建在 test1 上做**,不要本地交叉编译 rh:

   ```bash
   cd /tmp/rh-bench && sed -i 's/^strip = true/strip = false/' Cargo.toml
   ~/.cargo/bin/cargo build --release -p rh-server -p rh-client   # 约 4-5 分钟
   ```

   编译产物放 `/tmp/rh-bin/`。**注意硬链接**:`/tmp/rh-bin/rh-server` 可能是
   多个历史版本的硬链接(count>1),`cp` 覆盖会改到同一 inode。换版本要先
   `rm` 再 `cp`:

   ```bash
   rm -f /tmp/rh-bin/rh-server && cp target/release/rh-server /tmp/rh-bin/rh-server
   /tmp/rh-bin/rh-server --version    # 必须核对版本,别拿旧二进制测
   ```

3. **三台同步脚本**,然后按角色启动(每个角色一个 termark 会话,`setsid` 后台):

   ```bash
   # test1
   cd /tmp/rh-bench/benchmark && BENCH_BIN=/tmp/rh-bin \
     PEER_RUN_ID=<id> PROTO=<proto> RELAY=splice MUX=1 PEER_DIR=/tmp/peer \
     setsid bash peer.sh server > /tmp/srv.out 2>&1 < /dev/null &

   # test2(凭据取 test1 输出)
   ... setsid bash peer.sh client <A_IP>:17100 <cid> <secret> > /tmp/cli.out 2>&1 &

   # test3(先登录拿 token)
   TOKEN=$(curl -s -X POST http://<A_IP>:18080/api/auth/login \
     -H 'Content-Type: application/json' \
     -d '{"username":"admin","password":"peer-Pass-1"}' | jq -r .data.accessToken)
   ... setsid bash peer.sh drive <A_IP> <B_IP> 18080 $TOKEN <cid> <label>
   ```

   `PROTO`/`RELAY`/`MUX`/`PEER_RUN_ID` **必须显式给**,脚本不再有隐式默认。

4. **等 drive 自己跑完**,别中途重跑。吞吐只是第一个场景,后面还有 UDP /
   延迟 / 并发 / 建连速率 / HTTP vhost。

### 压测器:默认 iperf3,量大时切 loadgen

`THP_TOOL=loadgen` 用仓库自带 Go 压测器(`benchmark/loadgen`),`THP_TOOL`
默认为 `iperf3`。

**什么时候必须切**:被测吞吐接近 2 Gbps 量级时。系统 iperf3 3.9 单线程、
`-P` 不扩展吞吐,与 Python loadgen 的天花板都在 2 Gbps 附近;用它测高吞吐
会把两边都压到同一数字,测到的是压测器瓶颈而非产品差异(`loadgen/main.go`
顶部记录了同一坑在明文场景的教训)。

切换是**成对**的:drive 侧改用 loadgen src,**B 侧同时**把 `iperf3 -s` 换成
`loadgen sink`(两者协议不兼容)。脚本已按 `THP_TOOL` 自动处理。

loadgen 只有单向发送,B 侧 sink 读空;`download` 档实际与 `upload` 同向。

### 结果在哪

- test3: `/tmp/peer*/results-<label>-<时间戳>.jsonl` 与 `raw/`
- 三台的 `endpoint.json` / `identity.jsonl` / `cpu.jsonl` / `nofile.json`

判定:`python3 benchmark/py/summarize.py <证据目录>`、
`python3 benchmark/py/verify_goal.py --evidence <证据目录>`。

### 踩过的坑

- **token 会过期**。drive 用过期 token 查 `/api/clients` 得 401,报
  `client_version 为空,中止本轮`,**吞吐循环根本不会执行**,后面的 UDP 是
  残留进程刷出来的。重跑前先重新登录拿 token。

- **`THP_TOOL` 必须 export**。`run_iperf` 由 `$(...)` 子 shell 调用,未导出
  的变量在那里不可见 → 实际跑 iperf3(空 JSON)而解析仍按 loadgen 判断,
  结果全成 `INVALID(exit=0)`,极难从输出看出。

- **B 侧端口被上一轮 iperf3 占住**。换 loadgen 后若 `iperf3 -s` 仍在监听
  19101,sink 绑定失败**静默退出**,直连/隧道样本全变成 0~2 Mbps。跑前先
  `ss -ltnp | grep 19101` 确认占用者是不是 `loadgen`。

- **sink 有寿命**。`loadgen sink <port> <seconds>` 到点自行退出,后续样本
  静默变 0。脚本给的是 7200s,手工起时别给短了。历史上还遇到过 sink 长跑
  数小时后吞吐退化,重启即恢复 —— 怀疑有退化就重启 sink。

- **`pkill -f` 会杀掉自己的 shell**。命令行里含 `rh-client` 等字样时,
  `pkill -f rh-client` 会匹配到执行该命令的 shell 本身。用精确匹配:

  ```bash
  ps -eo pid,comm | awk '$2 ~ /^(rh-server|rh-client|loadgen)$/{print $1}' | xargs -r kill
  ```

- **`PEER_DIR` 残留会让创建客户端失败**。DB 在 `PEER_DIR` 内,同名客户端
  冲突报 `UNIQUE constraint failed: clients.name`。重跑前 `rm -rf $PEER_DIR`。

- **样本量不足时不要下结论**。`PEER_REPEAT=2` + 8s 时单档波动可达 ±70%,
  会出现「隧道比直连还快」这种物理上不可能的读数。定量结论至少
  `PEER_REPEAT=5` + 每档 15s。

- **先看直连基线**。若直连只有 2.5 Gbps 而隧道也接近它,说明两边一起被
  压测端或目标服务卡住,此时 rh 与 frp 的差异不可分辨,别硬解。

### 不要做的事

- 不要自己写临时压测脚本代替 `peer.sh` / `peer-frp.sh` —— 会丢掉证据采集
  (CPU 采样、端点实况、nofile、无效样本留痕),事后无法复核。
- 不要用 tar 传 rh 源码或 rh 二进制到测试机 —— 走 git,保证结果可绑定 commit。
  (loadgen 这类压测工具是例外,见流程第 1 步。)
- 不要在 `PROTO`/`RELAY`/`MUX` 不一致的两端之间比较结果。
- 不要只跑一遍就下结论,也不要拿单向样本冒充双向。
