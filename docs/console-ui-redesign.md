# console UI 重设计文档(全面版)

> **执行状态(2026-10-12 更新)。**
>
> - **§1–§9 已由 R22 全量落地**(antd 体系下):去模板脸、token 与 mono、字体自托管、
>   IA 分组、总览重排、ProxyListPage 参数化、空态三件套、弹窗改造、文案与 i18n 决策 A。
>   验收记录见 `docs/review-log.md` R22。
> - **后续已演进的条目**:§2.6 的字体自托管在 2026-10-12 的整体重写中**改为纯系统字体栈**
>   (不再打包字体文件);§4.2"本次不做配色调整"随之失效——新体系用 shadcn/ui 的
>   neutral 单色主色 + 语义状态色。
> - **2026-10-12 起前端整体重写为 Vite + Tailwind v4 + shadcn/ui**,§4 的形状/层次/动效/
>   响应式/组件模式规范继续有效,但具体实现与色值以 `console/src/index.css` 的
>   token 与 shadcn/ui 约定为准。
> - 本文档保留为设计依据与诊断记录,不再逐条对应实现文件行号(旧行号基于 v0.1.0)。

> 证据来源:线上 v0.1.0 控制台(172.16.1.2:8080)10 个视图实拍截图(登录/总览/客户端/
> TCP 隧道/UDP 隧道/域名服务/密钥访问/证书/设置/新建规则弹窗)+ console/ 全源码走查。
> 方法论:[Anthropic frontend-design](https://github.com/anthropics/claude-code/tree/main/plugins/frontend-design)
> (设计方向与自检)与 [Vercel web-design-guidelines](https://github.com/vercel-labs/agent-skills)
> (全量规则审查)。本文档只做分析与方案,未改动任何代码。
> 行号基于 v0.1.0(01e924a),可点击跳转。

---

## 1. 总体判断

**这是模板的皮,不是产品的脸。** 视觉层全部停留在 ant-design-admin-v6 模板出厂状态:
默认蓝 `#1890ff`(antd v4 时代的默认值)、"Ant Design Pro" 侧栏标题、admin 水印、
开发设置抽屉漏进生产包。没有任何一个视觉决定是为"端口转发与内网穿透控制台"这个主题做的。
此外存在一条贯穿构建链的真 bug:开发面板被打进生产包(§2.1 V3)。

---

## 2. 现状全量诊断

### 2.1 模板残留与构建链缺陷(实锤,截图可见)

| # | 现象 | 根因 | 严重度 |
|---|------|------|--------|
| V1 | 侧栏标题写 **"Ant Design Pro"** | `console/src/components/Logo/index.tsx:14` `title = 'Ant Design Pro'` 默认 prop,渲染进侧栏头部 | P0 |
| V2 | 内容区满屏 **"admin" 平铺水印** | `console/src/app.tsx:129` `waterMarkProps: { content: 用户名 }` | P0 |
| V3 | 生产环境右侧悬挂**开发用 SettingDrawer 齿轮**(登录页除外,所有页面可见) | 根因链三环:`console/src/app.tsx:4` `isDev = NODE_ENV==='development' \|\| process.env.CI` → `console/config/config.ts:72` `define: { 'process.env.CI': process.env.CI }` 把构建机环境变量内联进产物 → `release.yml`(GitHub Actions 恒有 `CI=true`)构建前端 → 生产包 `isDev===true`,`app.tsx:150` 的 SettingDrawer 全量渲染 | P0(功能泄漏 + 视觉缺陷) |
| V4 | 全站默认蓝、淡蓝渐变登录页 | `console/config/defaultSettings.ts:8` `colorPrimary: '#1890ff'`;无任何主题 token 定制(配色调整不在本方案范围,见 §4.2) | P1 |

> V3 与 GHCR 镜像二进制丢可执行位同属"CI 阶段默认值污染产物"一类缺陷,修复时应一并排查
> 所有 `process.env.*` 内联。

### 2.2 逐页问题清单(按截图)

**登录页**(`/user/login`)
- 淡蓝渐变背景 + 居中小表单 + 默认蓝按钮,模板构图;品牌信息只有 logo + 一行副标题。
- 页面下半 60% 空白;无任何产品语境(它是唯一能做品牌表达的页面,现在最平淡)。
- 表单:无 `autoComplete`(`pages/user/login/index.tsx:90-102`);placeholder"用户名/密码"
  不含示例格式;错误提示依赖接口回显,无行内校验焦点管理。

**总览**(`/rabbithole/overview`)
- 四张同质白卡(客户端/在线/运行服务/当前连接)+ 两张流量卡,数字大标签小,卡片间无层级;
  下半屏图表 + 事件表,信息密度极低,一屏只承载 6 个数字。
- 数字为比例字体,5s 轮询刷新时宽度抖动(`pages/overview/index.tsx:88-117`,无
  `tabular-nums`);流量图无填充、无当前值标注、Y 轴 `B/s` 量级不适配真实流量。
- 水印铺满整个内容区。

**客户端**(`/rabbithole/clients`)
- 表格仅 6 列,UUID 列占半张表宽且截断(`084dd60b-8627-4bc3-bbc2-59…`)无一键复制;
  在线状态为浅绿文字标签,与"启用"开关的语义权重混淆。
- 空表区域无任何占位引导;单行数据 + 半屏空白。
- 操作列:编辑/轮换密钥/删除,链接式 OK;`pagination={false}`(`pages/clients/index.tsx:274`)。

**TCP 隧道 / UDP 隧道 / 域名服务**(`/rabbithole/tcp|udp|domains`,共用 `ProxyListPage`)
- 三个页面几乎同一模板:页头 + 筛选下拉 + 三个按钮(新建/批量导入/刷新)+ 表格。
- UDP/域名页空态为 antd 默认"暂无数据"信箱图标,无下一步引导(新用户到这里就卡住:
  没有客户端时应该先去哪、规则长什么样,全靠猜)。
- 状态列 `running applied…` 绿色 pill 文字截断,healthy/pool 等服务端已有信息未呈现。
- 端口、目标地址用比例字体,列对不齐。

**密钥访问**(`/rabbithole/secret`)
- 页头已有"密钥访问"标题,卡片内又重复一个"密钥访问"卡标题,同屏两次。
- 规则/访问器双 Tab 结构 OK;空态同样无引导。

**证书**(`/rabbithole/certs`)
- 有"服务端尚未实现(M3)"提示条(好),但整页只有一个上传按钮和空表,UI 先行于后端。
- 到期时间列无相对时间/倒计时设计(证书页的核心信息是"还有几天到期")。

**设置**(`/rabbithole/settings`)
- 修改密码卡:居中窄卡、红色星号左置,密度低;卡片标题"服务器配置(保存即热生效)"
  用括号补充说明是文案坏味道(`pages/settings/index.tsx:95`)。
- 监控指标卡:Prometheus URL + 复制图标(icon-only,可访问性待查),说明文案整段
  中文长句无结构。
- 密码三个输入框均无 `autoComplete`(`current-password`/`new-password`)。

**新建规则弹窗**(`ProxyFormModal.tsx`,795 行)
- 结构完整(所属客户端/名称/监听/目标/插件/高级折叠),但:
  - 名称字段预填了示例值 "web"(截图中可见),用户会误以为是必填校验通过的真实值;
  - "监听端口 0 = 自动分配" 的说明藏在字段下方小字,数值 0 是魔法值,应改为
    "自动分配"开关 + 端口输入框二选一;
  - 高级折叠区(传输与限速/负载均衡组/健康检查)层级正确,但表单整体无分组视觉;
  - 弹窗遮罩过浅,弹窗与背后表格对比不足(截图可见背后内容清晰穿透)。

### 2.3 组件层问题

| 组件 | 问题 |
|---|---|
| `TopHeader/index.tsx:112` | 折叠按钮是 `<div onClick>`,无 role/aria-label/键盘可达 |
| `TopHeader/index.tsx:123` | 头像 `<img>` 无 width/height(CLS) |
| `HeaderPairsInput.tsx:42` | 图标删除按钮无 aria-label(**全 src 零 aria-label**) |
| `TagsView/index.tsx`(200 行) | 多标签 chrome 常驻占一行;8 个页面逛一圈会堆出 8 个 tab(截图实证);对 8 页小控制台是模板税 |
| `Logo/index.tsx:14` | 模板标题硬编码 |
| `feedback.ts` | 全局 toast 出口设计正确(antd App 上下文);但错误 toast 文案直接透传后端 message,无"下一步"信息 |
| `Footer` | 独立页脚行"© rabbithole"悬在大片空白下方,无信息量 |
| `ProxyListPage.tsx` | 三页共用的骨架设计 OK,但列定义/空态/状态徽章未参数化,导致各页观感不一 |

### 2.4 信息架构(IA)

`config/routes.ts`:扁平 8 项(总览/客户端/域名服务/TCP 隧道/UDP 隧道/密钥访问/证书/设置)。
问题:

1. **命名维度不一**:"TCP 隧道/UDP 隧道"按协议命名,"域名服务/密钥访问"按功能命名,
   "证书"按资源命名——三套命名法混在一个侧栏。
2. **分组缺失**:8 项平铺,但业务上天然两组:转发面(TCP/UDP/域名/密钥/证书——都是
   "把流量送进内网的方式")与平台面(总览/客户端/设置)。
3. **旧路由兼容重定向**(`routes.ts` 内 vben 路径)说明产品经历过一次迁移,URL 已稳定,
   IA 调整需保路径兼容。

### 2.5 文案与 i18n

- 页面级文案**全部硬编码中文**(`label="压缩"`、`title="确认轮换密钥?"` 等),
  `src/locales/{zh-CN,en-US}/` 有 7 个翻译文件但只覆盖菜单/框架层;
  顶栏 `SelectLang` 语言切换器存在但切到 English 后页面主体不变——**名存实亡**。
  `config/config.ts:44-49` 开了 `locale.antd: true`。
- 文案坏味道:卡片标题用括号补充说明("服务器配置(保存即热生效)")、
  说明混入技术细节("需在服务端启用 metrics(enable_prometheus)后可用;抓取时携带
  管理台 Bearer 令牌"整段无结构)、错误 toast 直透后端 message。

### 2.6 性能与资产

- **字体走第三方 CDN**:`src/global.less:1-31` 加载 5 个 AlibabaSans woff2 文件,
  全部来自 `mdn.alipayobjects.com`。控制台是 rust-embed **内嵌离线产物**,在无外网/
  内网隔离环境字体全部加载失败(有 fallback 不至于裸奔,但首屏会闪);且向第三方
  泄漏使用者 IP 与访问时间。应自托管进 `public/`(随二进制分发,零额外请求域)。
- 首屏 loading.js(`config/config.ts:61-64`)是个好实践,保留。
- 表格无虚拟化/无分页:`pages/clients/index.tsx:274`、`components/ProxyListPage.tsx:286`
  均一次渲染全量行。
- `cssVar: true` 已开(`config/config.ts:53`),token 化改造的前置条件已具备。

### 2.7 响应式与触控

- `global.less:83-96` 有一个 768px 断点的表格 hack(`white-space: pre` 强制换行),
  属模板遗留,实际小屏体验未设计;侧栏 208px 固定,无自动折叠策略。
- 无 `theme-color` meta、无 `color-scheme` 声明(暗色模式前提缺失)。
- 触控目标未验证(链接型操作列在小屏密集)。

### 2.8 暗色模式

SettingDrawer 里有 `enableDarkTheme` 开关(还是那个泄漏进生产的开发抽屉),
但无任何暗色 token 设计——目前开暗色只有 antd 组件反色,自绘样式/图表/状态色全部失效。

---

## 3. Vercel Web Interface Guidelines 审查报告

全量规则逐条比对结果。格式:`file:line - 问题`。✓ 为通过项。

```text
## console/src/app.tsx

app.tsx:150   SettingDrawer 进入生产包(根因链见 §2.1 V3)
app.tsx:129   waterMarkProps 水印:对真实用户是噪音,应移除
✓ onPageChange 登录守卫、FeedbackBinder 避免静态 message

## console/src/components/TopHeader/index.tsx

TopHeader/index.tsx:112   div onClick 折叠按钮:改 <button>,需 aria-label +
                          焦点样式(guideline:<button> for actions)
TopHeader/index.tsx:123   头像 img 无显式 width/height(CLS)
✓ 布局用 flex,无 JS 测量

## console/src/components/HeaderPairsInput.tsx

HeaderPairsInput.tsx:42   icon-only 删除按钮无 aria-label
                          (guideline anti-pattern 清单原文项)

## console/src/components/Logo/index.tsx

Logo/index.tsx:14   模板默认标题 'Ant Design Pro' 泄漏

## console/src/pages/user/login/index.tsx

login/index.tsx:90-102   用户名/密码缺 autocomplete=username /
                         current-password;placeholder 无示例格式
✓ 无 onPaste 阻断、密码可见性切换齐备

## console/src/config/defaultSettings.ts

defaultSettings.ts:8   colorPrimary '#1890ff'(v4 弃用默认);无暗色 token;
                       无字体/圆角/密度 token

## console/src/pages/clients/index.tsx + console/src/components/ProxyListPage.tsx

clients/index.tsx:274       pagination={false} 无虚拟化,数据增长后一次性渲染
ProxyListPage.tsx:286       同上
✓ 删除/轮换密钥均有 Popconfirm 二次确认(破坏性操作合规)

## console/src/pages/overview/index.tsx

overview/index.tsx:88-117   Statistic/表格数字无 tabular-nums(5s 刷新抖动);
                            字节/速率未用等宽数字
✓ react-query 轮询,无布局抖动的 JS 测量

## console/src/pages/settings/index.tsx

settings/index.tsx:95       卡片标题括号补充说明(文案问题,§2.5)
settings/index.tsx          三个密码输入缺 autocomplete=new-password /
                            current-password;Prometheus URL 复制按钮
                            为 icon-only(需 aria-label)
✓ 保存/提交按钮有 loading 态(guideline: spinner during request)

## console/src/utils/dateHelper.ts

dateHelper.ts:22-24   dayjs 硬编码格式(可接受;若做 i18n 需切 Intl 或按 locale
                      选择格式)

## 全局通过项

✓ 无 transition: all;无 outline:none;无 autoFocus 滥用;无 onPaste 阻断
✓ 交互元素均为原生 button/link;无 div onClick 导航(TopHeader 一处除外)
✓ 图表/表格无大数组 .map() 裸渲染(当前数据量小)
✓ react-query 统一数据层,无手写轮询竞态
```

---

## 4. 设计系统规范 v1 —— "洞层(Strata)"

### 4.1 概念与原则

rabbithole 的本体是**洞**:公网一侧的入口、一条隧道、通往内网里一台台具体的机器。
运营者打开控制台是在"看隧道里的灯":哪条链路活着、洞口开在哪个端口、流量多大。

1. **状态语义一致**——在线/离线/警告/未启用四态,全站用同一组件模式表达(信号点+文字),不引入装饰性用色。
2. **数字皆等宽**——端口/IP/UUID/字节/速率/时间戳,一律 mono + `tabular-nums`。
3. **分区靠结构与留白**——导航与内容的分层不依赖装饰性底色。
4. **空态即引导**——每个空列表都回答"我现在该干什么"。
5. **品牌只放一次**——登录页是唯一做品牌表达的地方,业务页面全部纪律化。

### 4.2 配色

**本次不做配色调整**:沿用现有 antd 主题(现状 `colorPrimary: '#1890ff'` 保持不变)。
品牌色、状态色体系与暗色模式如需调整,另行专项立项;本方案的其余规范(字体/形状/
布局/组件模式)不与具体色值绑定。

### 4.3 字体

- 正文/标题:**AlibabaSans** 保留(300–700 五档),但 **woff2 自托管进 `console/public/fonts/`**
  并加 preload(修 §2.6 外链问题;内嵌离线产物不应依赖第三方 CDN)。
- 新增 **mono 栈**(零下载):`ui-monospace, 'SF Mono', 'Cascadia Mono', Menlo, Consolas, monospace`。
  适用:端口号、IP/CIDR、UUID、流量、速率、版本号、证书域名、指纹、时间戳。
- 数字列一律 `font-variant-numeric: tabular-nums`(cssVar 已开,直接进 token)。
- 字阶:`12 / 13 / 14(base) / 16 / 20 / 28`,行高 1.5(正文)/1.3(标题)。

### 4.4 形状与层次

- 圆角分层:容器 8px / 控件 6px / 信号点全圆——圆角跟层级走。
- 阴影只给浮层(modal/popover/dropdown):`0 8px 24px rgba(0,0,0,.16)`;
  平面卡片用 1px 中性描边,不用阴影。
- 间距:8px 网格;页面内边距 24;卡片内边距 20;表格行高 ≥40(密度舒适档)。

### 4.5 动效

- 只允许两类:①响应操作的过渡(弹窗 200ms/ ease-out、开关、折叠);②登录页一个
  页面加载的品牌瞬间(唯一)。
- 全站遵守 `prefers-reduced-motion`(motion-reduce 时登录页动画静止)。
- 只动 `transform/opacity`;禁止 `transition: all`(现状已合规,保持)。
- 不做:每卡 hover 浮起、逐段上滑入场、循环光效。

### 4.6 响应式与触控

- 断点:`≥1280` 全量;`768–1279` 侧栏自动收起为图标态;`<768` 侧栏抽屉化、
  表格降级为卡片列表(替换现有 `white-space:pre` hack)。
- 触控目标 ≥44px(操作列链接加大点击区);弹窗内 `overscroll-behavior: contain`。

### 4.7 组件模式规范

| 模式 | 规范 |
|---|---|
| 页头 | 单行:标题(20)+ 一句说明(ink-2)+ 主操作右对齐;禁止卡片内重复页标题 |
| 状态徽章 | 信号点 8px + 文字(语义状态色),四态:在线/离线/警告/未启用;禁止大色块底 |
| 表格列 | 名称(粗)/ 关键标识(mono,如端口、域名、指纹前 8 位 + 复制按钮)/ 状态(徽章)/ 开关 / 操作(链接组,删除用危险语义);数字列右对齐 |
| 表单 | 标签上置(替代左置星号);魔法值改控件(0=自动分配 → 开关+输入框);必填用"必填"徽标而非仅红星;提交按钮 loading 态(已合规) |
| 弹窗 | 遮罩加深至 `rgba(22,35,43,.45)`;标题=动作动词;底部右对齐 [取消][具体动词] |
| 空态 | 图 + 一句话 + 主按钮(+ 可复制的最小配置示例);禁止裸"暂无数据" |
| 复制交互 | UUID/密钥/URL:mono 展示 + 截断 + 悬停展开 + 一键复制(统一 `Copyable` 封装,补 aria-label) |
| 危险操作 | Popconfirm(已合规)+ 危险语义按钮样式 |
| Toast | 成功/失败均带"下一步"信息;容器确保 aria-live(antd App message 默认带 role,验证即可) |

---

## 5. 信息架构重构

侧栏从 8 项平铺改为 **2 组 + 底部版本号**(路径全部不变,只动呈现与分组):

```text
侧栏(分组示意,路径不变)
├── 总览            /rabbithole/overview
├── 客户端          /rabbithole/clients
├── 转发
│   ├── TCP 隧道    /rabbithole/tcp
│   ├── UDP 隧道    /rabbithole/udp
│   ├── 域名服务    /rabbithole/domains
│   ├── 密钥访问    /rabbithole/secret
│   └── 证书        /rabbithole/certs
├── 设置            /rabbithole/settings
└── (底部)v0.1.0 · server commit
```

- TagsView 默认关闭(`multiTabs: false`),省一行 chrome;配置项保留。
- 独立 Footer 移除,版本信息并入侧栏底部。

---

## 6. 页面级方案

### 6.1 登录页(唯一品牌位)

左 2/3 品牌区:产品名、一句价值描述与一个克制的"洞"主题图形(品牌表达仅此一处);
右 1/3 表单。表单:autocomplete 补齐、行内校验、
错误聚焦首个错误字段、按钮文案具体化("进入控制台")。移动端品牌区收成顶部窄条。
动画仅此一处,reduced-motion 下静止。

### 6.2 总览(信息密度重排)

```text
┌ 指标条(单卡分格,mono + tabular-nums,值刷新不抖动)──────────────┐
│ 在线客户端 1/1 ●    运行服务 1    当前连接 3    入站速率 12 B/s    │
├──────────────────────────────┬────────────────────────────────────┤
│ 流量速率图(渐变填充;       │ 隧道健康(每条转发规则一行:        │
│ 右上角当前值 mono 标注;     │ 名称 端口(mono) 信号点 池状态)   │
│ Y 轴自适应量级)             │                                    │
├──────────────────────────────┴────────────────────────────────────┤
│ 最近事件(信号点级别,时间 mono,默认 5 条,展开全部)             │
└───────────────────────────────────────────────────────────────────┘
```

### 6.3 列表页族(TCP/UDP/域名/密钥/证书)

统一收敛进参数化的 `ProxyListPage`:§4.7 表格列模式 + 每页差异列:

| 页面 | 关键列(mono) | 特有信息 |
|---|---|---|
| TCP/UDP 隧道 | 监听 `0.0.0.0:20000` | 目标 `host:port`、池、健康 |
| 域名服务 | 域名 + SNI 证书标记 | 回落目标 |
| 密钥访问 | 密钥前 8 位 + 复制 | 允许访问方 |
| 证书 | 域名(SAN 列表截断) | **到期倒计时**(<30 天 warn、<7 天 err)、指纹前 8 位 |

空态三件套:发生了什么 / 该做什么 / 主按钮。
例(UDP 空):"还没有 UDP 转发规则。规则把公网 UDP 端口的流量送进内网。→ [新建规则]。

### 6.4 客户端

- UUID 列:后 8 位 + 复制;悬停展开全量。
- 在线徽章 + 最近在线相对时间("2 分钟前",mono)。
- 行展开:该客户端的连接数/流量(数据已有)。

### 6.5 新建/编辑规则弹窗

- 名称不预填示例值;placeholder 给示例(`placeholder="web-nginx…"` 按 guideline 加示例)。
- 监听端口:"自动分配"开关替代魔法值 0;开关关闭时端口必填。
- 分组:连接(客户端/名称/监听)→ 目标 → 行为(启用/插件)→ 高级(折叠,现状保留)。
- 遮罩加深;底部 [取消][创建规则]。

### 6.6 设置

- 三个分节卡:修改密码(autocomplete 补齐)/ 服务器配置(标题去括号,说明移到
  卡片副标题)/ 监控指标(URL Copyable 补 aria-label,说明改步骤列表)。

### 6.7 证书页占位态

M3 未实现期间:保留提示条,空态改"证书功能将在服务端 M3 提供" + 文档链接,
不渲染可交互的空表。

---

## 7. 文案规范(示例改写)

| 现状 | 建议 | 理由 |
|---|---|---|
| `登 录` | `进入控制台` | 按钮说清楚动作结果 |
| `服务器配置(保存即热生效)` | 标题 `服务器配置` + 副标题 `保存后立即生效,无需重启` | 括号补充说明是坏味道 |
| `暂无数据` | 见 §6.3 空态三件套 | 空态要给下一步 |
| `确认删除该证书?` | `删除证书 example.com?此操作不可恢复。` | 具体对象 + 后果 |
| toast:`操作失败` | `保存失败:端口 20000 已被占用` | 错误信息含下一步 |
| `0 = 自动分配` | 自动分配开关 | 魔法值改控件 |
| 密钥展示页标题重复两次 | 页头一次,卡标题 `规则` / `访问器` | 去重复 |
| 全站 `…` 使用 `...` | `…` | Typography 规范 |

i18n 决策(二选一,不要现状):**A. 收缩**——撤掉 SelectLang,明确中文单语,删 7 个
locale 文件(推荐,省维护);**B. 做全**——页面级文案全部进 locale,补 en-US。
当前"切换器存在但只翻菜单"是最差状态。

---

## 8. 实施路线(文件级)

| 阶段 | 改动 | 涉及文件 |
|---|---|---|
| **P0 去模板脸**(~半天) | ① `isDev` 去掉 `process.env.CI` 并从 `define` 移除 CI 内联;② 移除 waterMarkProps;③ Logo 默认 title 改 `rabbithole`;④ SettingsDrawer 与镜像 exec-bit 一并重新发版 | `app.tsx:4,129`、`config/config.ts:72`、`components/Logo/index.tsx:14` |
| **P1 Token 化**(1 天) | ConfigProvider 接入形状/字体 token(圆角/密度/mono 栈,不含配色);mono 工具类 + `tabular-nums`;字体自托管 + preload;TopHeader/HeaderPairsInput a11y;登录 autocomplete;IA 分组 + TagsView 默认关 | `app.tsx`、`global.less`、`components/TopHeader|HeaderPairsInput`、`pages/user/login`、`config/routes.ts`(仅呈现) |
| **P2 页面重排**(2–3 天) | 总览重排;ProxyListPage 参数化(列模式/状态徽章/空态/复制封装);弹窗改造;设置/证书/密钥页按 §6 | `pages/*`、`components/ProxyListPage|ProxyFormModal|SecretModal` |
| **P3 文案与 i18n**(半天) | §7 改写表落地;i18n 决策 A/B 执行 | `pages/*`、`locales/*` |
| **P4 审查验收**(半天) | web-design-guidelines 拉最新规则复审;§9 清单核对;暗色/移动端截图走查 | — |

每个阶段独立可合入;P0 完成即可重新发版。

## 9. 验收清单

- [ ] 生产包:无 SettingDrawer、无水印、无 "Ant Design Pro" 字样;`grep -r "Ant Design Pro\|SettingDrawer\|waterMark" console/src` 为空(或仅存在但被环境正确门控)
- [ ] 构建产物中不再含 `process.env.CI` 内联(发布后远程实测齿轮按钮消失)
- [ ] 端口/IP/UUID/字节/时间全部 mono + tabular-nums;总览数字刷新不抖动
- [ ] 字体资源全部同域(self-host),离线环境无第三方请求
- [ ] 键盘走查:Tab 可达侧栏/表格操作/弹窗;focus-visible 可见;图标按钮 100% aria-label
- [ ] 登录表单 autocomplete;错误行内 + 聚焦
- [ ] 每个列表空态有引导 + 主按钮;>50 行启用分页/虚拟化
- [ ] `prefers-reduced-motion` 下仅剩必要过渡;登录页动画静止
- [ ] 768px 下侧栏收起、表格可用;触控目标 ≥44px
- [ ] i18n 决策已执行(A 撤切换器 / B 全量翻译),无中间态
