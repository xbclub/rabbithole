# console 走查截图(shadcn/ui 重写)

2026-10-12,`console/` 从 Umi Max + antd v6 重写为 Vite + Tailwind v4 + shadcn/ui 后的
浏览器走查记录。全部截图取自 **生产构建 + 真实 rh-server 内嵌托管**
(`pnpm build` → `cargo build -p rh-server` → 服务端日志 `web api up with embedded ui`)。

## 走查方式

Playwright(Chromium)驱动真实浏览器,对**空数据库**的新实例跑完整流程:

1. 登录页 → 用首启横幅里的随机管理员密码登录;
2. 八页逐页访问(总览/客户端/TCP/UDP/域名/密钥/访问器/证书/设置);
   空库阶段顺带断言「新建规则」禁用且悬停给出 Tooltip;
3. 新建客户端 → 一次性客户端凭据弹窗(三档接入指令切换);
4. 新建 TCP 转发规则(选客户端、自动分配端口、目标地址/端口、展开高级配置)→ 列表出现该规则;
5. 域名服务页新建弹窗(多 kind 选择器)+ 选 socks5 插件展开子表单;
6. ⌘K 命令面板;
7. 切暗色主题,重走总览/客户端/TCP/设置 + 规则弹窗;
8. 390px 小屏(表格卡片降级 + 侧栏抽屉:断言顶栏入口可见、点击后抽屉打开);
9. 登出(二次确认)→ 回登录页 → 重新登录并核对后端审计事件里出现「已登出」;
10. 深链直接刷新、旧路径重定向(`/rabbithole/services`、`/rabbithole/visitors`)、404 兜底;
11. 侧栏收起态与顶栏:断言三个侧栏分组都有二级标题(平台 / 转发 / 系统,
    无标题会留空白)、断言顶栏不含当前页面名、桌面端不出现侧栏切换按钮、
    搜索框 ≥240px 且位于右侧、右侧顺序为 搜索 → 主题 → 账户;
    页脚含版本号与折叠开关,开/关图标不同;收起后品牌文字块与版本号不可见、
    header 内无元素越出 48px 轨道;
14. 弹窗焦点与表单分组:断言四个弹窗(证书 / 客户端 / 访问器 / 规则)打开后
   不出现自动弹出的 Tooltip;规则弹窗的分组数 ≥4 且每组都有可见边框。
13. 证书表单:灌入 120 行证书内容断言 PEM 框高度仍 ≤200px 且转为内部滚动;
    PKCS#1(RSA PRIVATE KEY)私钥必须通过结构校验;加密私钥必须给出针对性的
   「先解密」提示。
12. 行操作与列名:断言「⋯」**常显**(未悬停该行时 opacity=1)、悬停「⋯」直接展开菜单、
   主操作「编辑」留在主位;并做**抗闪烁采样**(悬停期间连续 15 次 × 100ms 检查菜单
   可见性,要求 0 次翻转、末态可见),鼠标移开后自动关闭;
   ≤2 个动作的页面(TCP 隧道 / 证书)不出现「⋯」;密钥访问页(4 个动作)「⋯」含
   创建访问器/轮换密钥/删除;并逐页断言「列」下拉里的列名都是中文。

结果:**功能问题 0,控制台错误 0**。

## 截图索引

| 文件 | 内容 |
|---|---|
| `01-login-light.png` | 登录页(唯一品牌位,分栏) |
| `02-overview-light.png` | 总览:单卡分格指标条 + 流量速率图 + 转发规则健康 + 最近事件 |
| `03-overview-info-hover.png` | 指标条「名词 ℹ 说明」悬停弹出 |
| `04-clients-empty.png` | 客户端页空态三件套 |
| `05-tcp-empty.png` / `06-udp-empty.png` | TCP / UDP 隧道空态 |
| `07-domains-empty.png` | 域名服务空态 |
| `08-secret-rules.png` | 密钥访问 · 规则 Tab |
| `09-secret-visitors.png` | 密钥访问 · 访问器 Tab |
| `10-certs.png` | 证书页 |
| `11-settings.png` | 设置页(改密 / 服务器配置 / 监控指标) |
| `12-clients-secret-modal.png` | 一次性客户端凭据弹窗 |
| `13-clients-secret-Docker.png` | 接入指令 · Docker 一键命令 |
| `14-clients-secret-rhclienttoml.png` | 接入指令 · rh-client.toml 模板 |
| `15-clients-with-row.png` | 客户端列表(含行展开) |
| `16-rule-form-basic.png` | 新建转发规则 · 基础区 |
| `17-rule-form-advanced.png` | 新建转发规则 · 展开「高级配置」 |
| `18-tcp-with-rule.png` | 创建成功后的 TCP 列表 |
| `19-rule-form-http-kind-selector.png` | 域名服务页的多 kind 选择器 |
| `20-rule-form-plugin-socks5.png` | 插件子表单(socks5) |
| `21-command-palette.png` | ⌘K 命令面板 |
| `22-overview-dark.png` | 暗色主题 · 总览 |
| `23-clients-dark.png` | 暗色主题 · 客户端 |
| `24-tcp-dark.png` | 暗色主题 · TCP 隧道 |
| `25-settings-dark.png` | 暗色主题 · 设置 |
| `26-rule-form-dark.png` | 暗色主题 · 规则弹窗 |
| `27-mobile-tcp.png` / `28-mobile-clients.png` | 390px 小屏表格降级为卡片 |
| `29-mobile-sidebar.png` | 小屏侧栏抽屉(顶栏有仅小屏可见的入口) |
| `30-logout-confirm.png` / `31-login-after-logout.png` | 登出二次确认与回登录页 |
| `32-overview-events.png` | 审计事件(含「已登出」) |
| `33-settings-deeplink-refresh.png` | 深链直接刷新 |
| `34-not-found.png` | 404 兜底页 |
| `35-sidebar-collapsed.png` | 侧栏收起为图标态(品牌区只留标记;页脚开关换成展开图标) |
| `36-clients-row-actions.png` | 客户端行操作:主操作「编辑」+ 常显的「⋯」 |
| `37-clients-row-actions-menu.png` | 悬停「⋯」直接展开的操作列表(轮换客户端密钥 / 删除) |
| `38-clients-column-menu.png` | 「列」下拉:中文列名 |
| `39-certs-pkcs1-key.png` | 证书表单:PKCS#1 私钥被接受、PEM 框高度受限 |
| `40-rule-form-groups.png` | 规则弹窗:分组改为带边框容器 + 标题分隔线 |

## 可编程视觉审计

除截图外,另在页面上下文里跑了客观测量(亮/暗 × 桌面 1440 / 小屏 390 × 10 个页面 = 40 个组合):

| 指标 | 结果 |
|---|---|
| 文档横向溢出 | 40/40 组合 **0** |
| 元素越出视口右边界 | **0** |
| 文字被裁切(排除刻意的 `sr-only`) | **0** |
| 文字对比度不足(WCAG AA:正文 4.5:1、大字 3:1) | **0** |
| 交互元素小于 24×24(WCAG 2.2 AA 目标尺寸) | **0**(修复前:ℹ 按钮 14×14、开关 18.4×32) |

> 说明:本次会话的模型不支持图像输入,无法按 `AGENTS.md` 的约定把截图交给
> glm-5.3-flash 判读(子代理继承同一模型,workflow 的模型覆盖在本会话亦不可用)。
> 因此这里给出的是**可编程的客观测量**加**原图**,视觉观感请人工过目。
