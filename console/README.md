# rabbithole 控制台(console/)

rabbithole 管理台前端:**Vite 7 + React 19 + TypeScript(strict)+ Tailwind CSS v4 + shadcn/ui**,
数据层 TanStack Query + axios + zustand,表格 TanStack Table,表单 react-hook-form + zod,图表 recharts。

界面规范沿用 shadcn/ui 的约定(neutral 基色、CSS 变量 token、亮/暗双主题、lucide 图标);
术语与状态语义见仓库根目录的 [CONTEXT.md](../CONTEXT.md)。

## 开发

```bash
pnpm install          # Node ^20.19 || >=22.12,pnpm 由 packageManager 声明(corepack 自动切换)
pnpm dev              # http://localhost:8000,/api 原样代理到 127.0.0.1:8080(vite.config.ts)
pnpm typecheck        # tsc --noEmit
pnpm lint             # eslint(flat config)
pnpm test             # vitest run(覆盖抽出来的业务纯逻辑)
pnpm build            # tsc --noEmit && vite build → console/dist
pnpm preview          # 本地预览生产构建
pnpm format           # prettier --write
```

后端联调:`target/debug/rh-server --config <你的 server.toml>`(web 端口默认 8080)。

## 被 rh-server 托管的两种方式

1. **内嵌(推荐,单二进制)**:`pnpm build` 后重编 `cargo build -p rh-server`。
   `crates/rh-server/build.rs` 检测 `console/dist` 存在即置 `embed_ui`,
   构建日志出现 `embedding web console from .../console/dist` 即内嵌成功。
   运行时未配置 `[web].static_dir` 时自动启用内嵌 UI(SPA 深链回退 200、未知 `/api/*` JSON 404)。
2. **static_dir 显式指定**:`server.toml` 中 `[web] static_dir = "/path/to/console/dist"`,
   显式配置优先于内嵌。

注意:debug 构建的 rust-embed 从 `console/dist` **运行时读盘**,改 dist 无需重编;
release 构建编译期嵌入,改 dist 后需重编。

## 目录结构

```
index.html           首屏占位(避免白屏)+ 首帧前定 dark class(避免亮暗闪烁)
vite.config.ts       别名 @/、__APP_VERSION__ 注入、dev 代理、产物落 console/dist
components.json      shadcn/ui 配置(new-york / neutral / CSS 变量 / lucide)
src/
  api/               request.ts(envelope code===0 取 data、业务错弹中文 message、
                     HTTP 401 清 token 回跳登录、safeRedirect 防开放重定向)
                     + rabbithole.ts(业务接口与类型,契约与后端零变更)+ auth.ts
  stores/auth.ts     zustand:token/username,localStorage 持久化
  lib/               theme.ts(亮/暗/跟随系统)、format.ts(字节/时间/相对时间)、
                     feedback.ts(sonner 出口)、query-keys.ts、utils.ts(cn)
  features/
    field-info.ts    「名词 ℹ 说明」文案字典(87 key,旧版 89 条文案逐字平移)
    services/        转发规则的纯逻辑:types / matrix(8 kind 字段矩阵 + 9 插件子表单)
                     / spec(表单值 ⇄ 提交载荷 ⇄ 编辑回读)+ __tests__/spec.test.ts
  components/
    ui/              shadcn/ui 生成组件(38 个)
    layout/          ConsoleLayout(sidebar 外壳)+ AppSidebar(两组 + 设置 + 版本号)
    proxy-list-page  参数化类型页(tcp/udp/domains/secret 共用)
    proxy-form-modal 新建/编辑转发规则(按 matrix 泛型渲染,不手写字段)
    visitors-panel   访问器面板、secret-modal 一次性客户端凭据 + 三档接入指令
    data-table       列表封装(排序/分页/列可见性/行选择/骨架/空态插槽)
    info-label       名词 + ℹ(悬停/点按,触屏可用)、status-badge、mono、
    confirm-dialog、empty-state、page-header、header-pairs-input、command-palette(⌘K)
  pages/             overview / clients / tcp / udp / domains / secret(双 tab)
                     / certs / settings / login / error-page(403/404/500)
  router/            路径表 + createBrowserRouter(含旧路径重定向与登录守卫)
```

## 已知约定

- **弹窗表单以 `key` 强制重挂载**(新建/编辑不同目标各一份),避免表单状态串味。
- **数值字段必须是 number**:后端 serde 不接受字符串,数值输入统一经 `valueAsNumber`
  转数字,空值转 `null`。
- **密钥零输入**:stcp/sudp/xtcp 的 `secret_key` 完全由服务端生成与轮换,表单与列表都
  不出现;`allow_users` 已移除(访问器记录本身即授权)。
- **术语**:用户可见文案统一叫「转发规则」;「客户端密钥」(接入凭据)与「访问密钥」
  (stcp/sudp/xtcp 共享密钥)分开写,不单用「密钥」二字。
- **ℹ 说明**:每个配置项的名词旁有 ℹ(悬停/点按弹说明),文案来自
  `src/features/field-info.ts`,不要写成括号补充说明。
- 依赖后端未落地字段的 UI 按容错降级;证书页对 404 给出可读提示而不是白屏。
- 内嵌产物**零外域请求**:字体走系统字体栈,不打包也不外链字体文件。
