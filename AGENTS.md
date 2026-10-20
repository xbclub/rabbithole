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
