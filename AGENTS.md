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
`cli`(rhctl 六平台,Go 交叉编译,单 ubuntu runner,不碰 Rust 矩阵)→
`docker`(ghcr.io 多架构镜像)→ `release`(GitHub Release + 附件)。

平台:`x86_64`/`aarch64` × `linux-musl`/`apple-darwin`/`pc-windows-msvc`,
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

## 提交与 CI 工作流

### 提交前:三道门必须全绿

```bash
cargo fmt --all -- --check                          # 格式
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

三者都在 `.github/workflows/ci.yml` 里跑,本地先过一遍能省一个来回。
格式化提交已登记在 `.git-blame-ignore-revs`,`git blame` 会跳过它。

### 提交:推送前先推 main,再追 CI

**`main` 是唯一长期分支**,直接推,不建特性分支(本项目单人维护)。
推送后**必须**确认 CI 绿了再走下一步:

```bash
git push origin main
gh run list --limit 3                 # 找到刚才那次 run 的 id
gh run view <run-id> --json jobs \
  --jq '{status, conclusion, jobs: [.jobs[] | {name, conclusion}]}'
```

CI 有两个 job:

| job | 做什么 | 能抓到什么 |
|---|---|---|
| `test + clippy (linux)` | fmt / clippy / 全量测试 | 逻辑错误;**只有 Linux 上能跑的那条背压测试**(靠 `ss` 读内核 accept 队列) |
| `编译检查 (windows)` | `cargo check --all-targets` | **v0.4.0 那类 msvc 编译失败**(`std::os::fd` 等 Unix 专属用法漏了 `cfg`) |

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
   grep -rn "std::os::fd\|std::os::unix" --include=*.rs crates/ vendor/
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

## 性能基准(三机 peer 基准)

所有性能结论必须来自 `benchmark/peer.sh`(rh)与 `benchmark/peer-frp.sh`(frp
对照),**不要自己另写压测脚本**。两脚本同拓扑、同端口、同记录格式,结果可比。

### 拓扑

```
[test3: 压测端]        [test1: 服务端]           [test2: 客户端+目标]
loadgen src ───────►  转发口 20001..  ──隧道──►  目标服务 19101..19104
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

三台均 8 核、10GbE、`sudo -n` 可用、`python3` 就位;`/tmp/rh-bin/loadgen`
(Go 压测器)必须提前放好 —— 三台没装 Go,只能本地交叉编译后上传。

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

   > **⚠ 先问一句:被测版本有 Release 吗?**
   >
   > 只要被测版本**已经发过版**(`gh release view vX.Y.Z` 能查到),
   > **直接下载 Release 产物,不要自己编译**:
   >
   > ```bash
   > gh release download vX.Y.Z -R xbclub/rabbithole -D /tmp/rel-vX.Y.Z \
   >   -p "*x86_64-unknown-linux-musl*"
   > cd /tmp/rel-vX.Y.Z && shasum -a 256 -c *.sha256 && tar xzf *.tar.gz
   > ```
   >
   > **为什么这是必须的**(2026-10-24 踩过):
   > - **产物即事实**。用户拿到的是 Release 里那个二进制。自己编译出来的
   >   即使 commit 相同,也可能因工具链/feature/依赖解析差异而不同 ——
   >   测出来的数字不能代表用户实际跑的东西。
   > - **能省一次构建**。`cargo build` 两套版本约 8-10 分钟,Release 下载
   >   只要几秒,且带 `.sha256` 可校验完整性。
   > - **省掉一类坑**。自己编译要 `sed` 关 `strip`、要处理 worktree、
   >   要小心"版本号对得上但 commit 不对"(本次实测:test1 上那个
   >   自报 `0.5.1` 的二进制其实是 `9fcce2f`,不是 v0.5.1 的 `a5d4276`)。
   >   Release 产物由流水线从 tag 构建,产物与 commit 天然绑定。
   >
   > **只在下面两种情况下才自己编译**:
   > - 被测的是**未发版的提交**(修完想立刻验证,还没打标签);
   > - 需要**改代码做对照实验**(如临时关掉某个优化)。
   >   这时才走下面的构建流程,并且要在结果里注明"非 Release 产物"。

   构建流程(仅用于未发版的提交):

   ```bash
   cd /tmp/rh-bench && sed -i 's/^strip = true/strip = false/' Cargo.toml
   ~/.cargo/bin/cargo build --release -p rh-server -p rh-client   # 约 4-5 分钟
   ```

   **`--version` 只能证明版本号,不能证明 commit**。要测某个 tag,用
   `git worktree add /tmp/rh-vX.Y.Z vX.Y.Z` 检出该 tag 再构建,不要依赖
   工作区当前状态(本次踩过:工作区在 `9fcce2f`,自报 `0.5.1`,与
   `v0.5.1` 的 `a5d4276` 不是同一个东西)。

   编译产物放 `/tmp/rh-bin/`。**注意硬链接**:`/tmp/rh-bin/rh-server` 可能是
   多个历史版本的硬链接(count>1),`cp` 覆盖会改到同一 inode。换版本要先
   `rm` 再 `cp`:

   ```bash
   rm -f /tmp/rh-bin/rh-server && cp target/release/rh-server /tmp/rh-bin/rh-server
   /tmp/rh-bin/rh-server --version    # 必须核对版本,别拿旧二进制测
   ```

3. **三台同步脚本**,然后按角色启动(每个角色一个 termark 会话,`setsid` 后台)。
   **三个角色的环境变量必须完全一致**,尤其 `THP_TOOL=loadgen`:

   ```bash
   # test1
   cd /tmp/rh-bench/benchmark && BENCH_BIN=/tmp/rh-bin THP_TOOL=loadgen \
     PEER_RUN_ID=<id> PROTO=<proto> RELAY=splice MUX=1 PEER_DIR=/tmp/peer \
     setsid bash peer.sh server > /tmp/srv.out 2>&1 < /dev/null &

   # test2(凭据取 test1 输出)
   ... THP_TOOL=loadgen setsid bash peer.sh client <A_IP>:17100 <cid> <secret> \
     > /tmp/cli.out 2>&1 &

   # test3(先登录拿 token)
   TOKEN=$(curl -s -X POST http://<A_IP>:18080/api/auth/login \
     -H 'Content-Type: application/json' \
     -d '{"username":"admin","password":"peer-Pass-1"}' | jq -r .data.accessToken)
   ... THP_TOOL=loadgen setsid bash peer.sh drive <A_IP> <B_IP> 18080 \
     $TOKEN <cid> <label>
   ```

   `PROTO`/`RELAY`/`MUX`/`PEER_RUN_ID`/`THP_TOOL` **必须显式给**,脚本的
   隐式默认值(`iperf3`)不能依赖。test2 由脚本自己起 `loadgen sink`,
   **不要手工起 `iperf3 -s`**。

4. **等 drive 自己跑完**,别中途重跑。吞吐只是第一个场景,后面还有 UDP /
   延迟 / 并发 / 建连速率 / HTTP vhost。

### 压测器:一律用 loadgen,不用 iperf3

**所有吞吐压测都必须 `THP_TOOL=loadgen`**,用仓库自带 Go 压测器
(`benchmark/loadgen`)。三个角色都要显式带上这个环境变量:

```bash
THP_TOOL=loadgen PROTO=... RELAY=... MUX=1 PEER_RUN_ID=... ...
```

脚本里 `THP_TOOL` 的默认值仍是 `iperf3`(`benchmark/peer.sh:95`,为兼容
历史数据保留),**不要依赖这个默认值** —— 不显式给就等于在用过时的压测器。

**为什么不再用 iperf3**:系统 iperf3 3.9 是单线程,`-P` 不扩展吞吐,天花板
就在 2 Gbps 量级。本机三台是 10GbE,直连基线动辄 2.5~3 Gbps,用 iperf3
测会把**压测器瓶颈当成产品瓶颈**,rh 与 frp 被压到同一数字而不可分辨。
`loadgen/main.go` 顶部记录了 Python 版压测器踩过的同一个坑:曾据此得出
「rh 与 frp 都卡在 2.0~2.2G、都不随并发扩展」——那个结论是压测器造成的。

Go 版每连接一个 goroutine、收发走 `io.Copy` 大缓冲快路径,可稳定压满 10GbE。

切换是**成对**的:drive 侧用 `loadgen src`,**B 侧同时**把 `iperf3 -s` 换成
`loadgen sink`(两者协议不兼容,混用会让样本静默变 0)。脚本已按
`THP_TOOL` 自动处理,B 侧不要手工起 `iperf3 -s`。

loadgen 只有单向发送,B 侧 sink 读空;`download` 档实际与 `upload` 同向,
这是工具的已知局限,**不要把它当成"双向都测过了"**。

### 结果在哪

- test3: `/tmp/peer*/results-<label>-<时间戳>.jsonl` 与 `raw/`
- 三台的 `endpoint.json` / `identity.jsonl` / `cpu.jsonl` / `nofile.json`

判定:`python3 benchmark/py/summarize.py <证据目录>`、
`python3 benchmark/py/verify_goal.py --evidence <证据目录>`。

### 踩过的坑

- **`loadgen sink` 的 `SetReadBuffer(4MiB)` 会把单流吞吐腰斩**。2026-10-24
  实测:同一个 `loadgen src` 打 `loadgen sink` 只有 1.58 G,打 `nc -l` 能到
  2.67 G,打 iperf3 自己的 sink 是 2.73–3.19 G。二分确认是 sink 侧的
  `SetReadBuffer` 干的 —— 只要显式设 `SO_RCVBUF`(不管 256K/1M/2M/4M),
  吞吐就掉到 1.6 G;不设是 3.01 G。

  后果:直连基线被压到 1.6 G,而隧道读数是 2.1 G,于是出现
  **「保留率 > 100%」的反物理读数**(实测 121%–132%,3 次重复极差 < 5%)。
  **看到保留率 > 100% 直接判为伪影,不要当产品特性写进结论。**
  机制用 `ss -tin` 可验:设了 `SO_RCVBUF` 时 `cwnd` 从 1571 塌到 94、
  `delivery_rate` 从 2.68 G 掉到 0.98 G(注意 `rcv_space` 两种情况下都是
  14480,所以**不是**窗口被钉死,别按那个方向解释)。
  隧道为何没被同一个 sink 拖住,尚未查清(已排除「瓶颈分摊到两段」与
  「小块读更平滑」两个猜测),但「>100% 是伪影」的判断不依赖它。
  详见 `benchmark/README.md` 的「测量伪影」节。**尚未修复**(改压测器会让
  历史数据不可比,需先决定是否重跑基线)。

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

  **⚠ 这条有两种成因,别混用同一个解释**:
  (a) **波动大**(单档极差占中位数几十%)= 样本量不足,加大 REPEAT/时长;
  (b) **读数稳定但仍然 >100%**(如 3 次重复极差 < 5%)= **不是噪声**,
  是上面那条 `loadgen sink` 伪影。2026-10-24 实测就是 (b):REPEAT=3 +
  10s,保留率 121%–132%,极差 < 5%。判据:先看 **极差/中位数**,小于
  5% 还 >100% 的,直接去查 sink,不要再加样本量。

- **先看直连基线**。若直连只有 2.5 Gbps 而隧道也接近它,说明两边一起被
  压测端或目标服务卡住,此时 rh 与 frp 的差异不可分辨,别硬解。

### 不要做的事

- 不要自己写临时压测脚本代替 `peer.sh` / `peer-frp.sh` —— 会丢掉证据采集
  (CPU 采样、端点实况、nofile、无效样本留痕),事后无法复核。
- 不要用 tar 传 rh 源码或 rh 二进制到测试机 —— 走 git,保证结果可绑定 commit。
  (loadgen 这类压测工具是例外,见流程第 1 步。)
- **被测版本已发版时,不要自己 `cargo build`** —— 直接 `gh release download`
  拉产物。产物即用户实际拿到的东西,且与 commit 天然绑定。详见流程第 2 步
  的⚠ 框。
- 不要在 `PROTO`/`RELAY`/`MUX` 不一致的两端之间比较结果。
- 不要只跑一遍就下结论,也不要拿单向样本冒充双向。
