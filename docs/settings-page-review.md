# 设置页设计评估与整改意见(settings-page-review)

> **⚠ 已被取代(2026-10-22 补注)**。本文针对的是 **antd 版** `console/`:
> 其引用的 `components/ui.tsx` 与 `global.less` 在 2026-10-12 的
> Vite + Tailwind v4 + shadcn/ui 重写中已删除,文中行号全部失效。
> 所提的 P0(保存配置静默清空端口策略)等缺陷**已随重写消除**。
> 保留本文是因为它记录的**表单取值陷阱与无障碍检查方法**仍可复用。

- 对象(历史):`console/src/pages/settings/index.tsx` 及依赖组件(`components/ui.tsx`、`global.less`)
- 方法:按 Vercel Web Interface Guidelines 逐条代码审查 + 真实渲染走查
  (本地构建 rh-server + `pnpm dev` + 浏览器,桌面 1280px / 窄屏 390px),
  关键问题均在真实页面复现实证。
- 日期:2026-10-12

## P0:点「保存配置」会静默清空端口策略(已实测复现)

位置:`console/src/pages/settings/index.tsx:129`、`:132`

「允许端口」「每客户端最大规则数」两个 `Form.Item` 没有 `name` 属性,也不受控,
而 `saveAll()`(:71)用 `portsForm.getFieldsValue()` 取值。rc-field-form 的
`getFieldsValue()` 只返回已注册(带 `name`)字段,导致:

- **回显失效**:服务端配置 `allow_ports=["10000-40000"]`、
  `max_services_per_client=32`,页面上这两个字段永远显示空 placeholder
  (实测:SSH 表单的地址/端口正常回显,这两个字段为空);
- **保存即清空**:实测点击一次「保存配置」(未做任何修改),服务端
  `allow_ports` 变 `[]`、`max_services_per_client` 变 `0`——端口限制被静默
  重置为"不限制/不限"。对内网穿透服务等于一次误点就放开全部端口。

**整改**:两个 `Form.Item` 补 `name="allow_ports"` / `name="max_services_per_client"`
(`saveAll` 取值逻辑已写好,加 name 同时修复回显与保存)。
修复后回归三条路径:加载回显 → 不改直接保存 → 改完保存。

## P1:结构性问题

1. **配置加载失败永远显示"加载中…"** — `settings/index.tsx:92-93`。
   `cfgQ` 出错时 `data` 为空,`restartNote` 恒为"加载中…",用户无从知道失败,
   也无法重试。整改:区分 `isLoading` / `isError`,`isError` 时渲染 Alert
   (含重试按钮),符合"错误信息要给出下一步"。
2. **同页卡片宽度不一致** — 实测 DOM:「修改密码」「监控指标」560px
   (`:186`、`:233` 有 `maxWidth: 560`),「服务器配置」992px 撑满剩余宽度。
   整改:统一宽度(抽 `CARD_MAX_WIDTH = 560` 常量三处共用);若嫌窄就三张卡
   都用 Fluid,但必须一致。

## P2:体验整改

3. **修改密码强登出无预警** — `settings/index.tsx:172-180`。成功后清 token
   跳登录是正确设计,但用户事前不知道。整改:卡片顶部加 Alert
   "修改成功后所有会话将立即失效,需重新登录"。
4. **服务器配置无任何校验** — `bind_addr`、`allow_ports` 均无 `rules`;
   `allow_ports` 是自由文本,格式全靠后端兜底。整改:加正则校验
   (`^\d+(-\d+)?(,\s*\d+(-\d+)?)*$`),inline 报错并聚焦首个错误字段。
5. **离开页面不提醒未保存修改** — 服务器配置填一半切走直接丢失。
   整改:dirty 检测 + 拦截路由跳转弹确认。
6. **硬编码黑色透明度,暗色主题下不可读** — `settings/index.tsx:240`
   `rgba(0,0,0,0.55)`、`ui.tsx:117` `rgba(0,0,0,0.45)`。dev 的 SettingDrawer
   带 `enableDarkTheme`,一旦启用暗色这些文字/图标直接消失。
   整改:换 antd token(`token.colorTextTertiary` 等)。

## P3:细节打磨

7. **一个「保存配置」按钮管三块内容且离端口策略/日志很远** — `:145-153`。
   建议拆成每节自己的保存,或按钮文案改"保存以上全部配置";「级别」Select
   用局部 state 而非表单字段,与其它字段模式不统一(本次 P0 即"半绑定"出错)。
8. **placeholder `AAAA...` 应为 `AAAA…`** — `:121`。
9. **监控指标 URL 长域名可能溢出** — `:236`。本机 localhost 没问题,部署在
   `https://console.example.com` 下会顶破卡片,建议 `wordBreak: 'break-all'`
   或 Typography `ellipsis` + copyable;另该元素 `aria-label="复制 Prometheus
   地址"` 会让读屏听不到 URL 本身,建议去掉(antd copyable 按钮自带标签)。
10. **560px 卡内 `labelCol span=6`(~140px)长标签换行** — "每客户端最大规则数 ℹ"
    实测折两行。服务器配置卡可改 `layout="vertical"`,或 labelCol 加响应式断点。

## 做得好的(无需动)

- 密码表单 `autocomplete` 新旧密码用法正确;
- 全局 `:focus-visible` 轮廓 + `prefers-reduced-motion` 降级(`global.less:133-149`);
- 端口/URL 用 `rh-mono` + `tabular-nums`;
- 提交按钮保持可用 + loading 态;
- 写回配置文件失败有专门 Alert 降级文案。

## 不采纳项(评估后决定)

- ℹ 说明图标键盘/读屏可达性(`ui.tsx:114-119`,tabindex=-1 的 role="img"):
  单管理员内部工具、鼠标操作为主,投入产出比不足,不做。

## 整改顺序

1. 立即:P0(两行 `name` 属性 + 回归);
2. 本迭代:P1 两项(错误态、卡片宽度统一);
3. 顺手:P2 #3、#6 改动小收益大;#4、#5 次之;
4. P3 按需排入日常打磨。
