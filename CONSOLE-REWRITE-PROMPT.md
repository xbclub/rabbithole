> **已废止(历史存档,2026-10-12)。**
>
> 本文档描述的是 2026-10 早期"用 React + Ant Design v6 重写 console"的一次性任务书,
> 其中多项要求后来已被用户拍板推翻或已被实现超越,例如:
>
> - 表单内填写 `secret_key` → **R24 定稿:密钥完全服务端托管,表单零输入**
> - `allow_users` 多值白名单 → **R32:字段已从后端与前端一并删除**
> - 登录路径 `/auth/login` → 现役为 `/user/login`(R33 修复的正是这条路上的报错)
> - 以 `web/`(vue-vben-admin)为行为参照 → `web/` 已删除
> - antd 组件体系 → **2026-10-12 起整体改为 Vite + Tailwind v4 + shadcn/ui**
> - "M1 计量 / M3 证书未实现,需容错降级" → 二者均已落地
>
> **现行契约 = `console/` 现役代码行为 + `docs/review-log.md` R20 之后的全部条目。**
> 本文档仅作历史追溯保留,不要再作为实现依据。

---

# 任务:用 React + Ant Design v6 重写 rabbithole 管理台(新 console/ 工程,R0→R3)

你是一名前端工程师。仓库根目录:`/Users/yii/Documents/mycode/rabbithole`。后端 Rust(`crates/rh-server`,axum,静态托管/内嵌 UI 机制已就绪),现有前端是 `web/`(vue-vben-admin 裁剪版,**本次任务冻结不动,仅作行为参照**)。你的任务:按 `docs/design-parity.md` §10(F9)新建顶层 `console/` 独立工程,用 React 19 + Ant Design v6 重写管理台,完成 R0→R3 四个阶段。**CUT(删除 web/、切换默认产物路径)不在本任务内**,见文末。

## 0. 开工前必读与现状核对(不要跳过)

权威依据(先读,冲突时以 design-parity.md 为准):

1. `docs/design-parity.md` §8.0(八菜单信息架构)、§8.1(按 kind 字段矩阵)、§8.2(插件子表单)、§10(F9 全部决策);
2. `docs/design.md` §7(Web API 契约)、§8.2(登录链路对接契约);
3. 参照实现(只读不改):`web/apps/web-antd/src/api/rabbithole.ts`(接口与类型,原样平移)、`web/apps/web-antd/src/views/rabbithole/*`(五个业务页的行为基线)、`web/apps/web-antd/src/views/_core/authentication/login.vue`。

核对命令(结果不符则停下报告,不要硬做):

```bash
# 1) 参照文件存在
ls docs/design-parity.md web/apps/web-antd/src/api/rabbithole.ts
# 2) build.rs 当前只检测旧路径(你要改为双路径)
grep -n 'web-antd/dist' crates/rh-server/build.rs      # 预期:命中 1 处(join("../../web/apps/web-antd/dist"))
# 3) console/ 尚不存在
ls console/ 2>/dev/null                                  # 预期:No such file or directory
# 4) node/pnpm 版本(仓库 web/ 用 pnpm 11.16.0,console/ 沿用)
node -v && pnpm -v
```

## 1. 硬性约束(违反即失败)

1. **Rust 侧只允许改一个文件**:`crates/rh-server/build.rs`(内嵌检测改为先 `console/dist` 后 `web/apps/web-antd/dist`,双路径择一)。其余 `crates/**`、`migrations/**`、`tests/**`、`deploy/*.toml` 一律不动。
2. **`web/` 整目录冻结**:不删、不改、不重构。它是切换前的现役 UI。
3. **API 契约零变更**:envelope 语义(`code===0` 取 `data`;业务失败 `code!==0` 弹 `message`;认证失效是 **HTTP 401 状态码**)、登录链路、全部业务端点。
4. UI 文案中文直出;不引入 i18n 框架、主题/多标签系统、redux。
5. 技术栈锁定:React 19 + TypeScript(strict)+ Vite + @vitejs/plugin-react + react-router v7(library mode)+ TanStack Query v5 + zustand + axios + antd v6 + recharts。antd 用**确定的小版本**并写入 package.json,不用 `latest`/`^` 大跨度。
6. 每阶段结束跑文末门禁,全绿才进下一阶段;不要跳过、不要 `--force`。
7. 后端 M1(F2 配额)/M3(F6 证书)尚未实现:相关 UI 按 §3/§5 的"容错降级"规则做,**不要为了让字段生效去改后端**。

## 2. R0:脚手架、认证链路、布局、构建接线

### 2.1 工程

- `console/`:独立 npm 工程(不进任何 workspace),`packageManager` 写 pnpm 11.16.0,Node engines `^22.18.0 || ^24.12.0`。
- 目录:`src/api`(axios 封装 + rabbithole.ts)、`src/stores/auth.ts`、`src/router`、`src/layouts/Console.tsx`、`src/components`、`src/pages`。
- `.env.development`:dev server proxy `/api → http://127.0.0.1:8080`(无 rewrite);`.env.production`:`VITE_API_BASE=/api`。
- vite:`base: '/'`,产物 `console/dist`;`index.html` 标题「rabbithole 控制台」,favicon 用现有 `web/apps/web-antd/public/favicon.ico` 拷贝。

### 2.2 axios 封装(`src/api/request.ts`)

- 拦截器:请求头注入 `Authorization: Bearer <token>`(zustand 取);响应解包——`code===0` 返回 `data`,否则 `message.error(error ?? message)` 并 reject;**HTTP 401** → 清 token → 跳 `/auth/login?redirect=<当前路径>`,登录成功后回跳。
- **logout 请求必须显式带 Bearer**(用同一个带拦截器的实例发,不要用裸 axios)。这是旧版踩过的坑:后端 `POST /api/auth/logout` 校验 Bearer,无 Bearer 回 401,审计事件不会落库。

### 2.3 认证链路(契约逐项实现)

| 请求 | 说明 |
|---|---|
| POST `/api/auth/login` `{username,password}` | → `{accessToken}`;存 zustand(持久化 localStorage) |
| GET `/api/user/info` + GET `/api/auth/codes` | 登录后**并行**请求;`homePath` 固定 `/rabbithole/overview`;codes 预期 `[]` |
| POST `/api/auth/logout`(带 Bearer) | 成功(204)后清 token 回登录页;401 也清 token 回登录页 |
| POST `/api/auth/password` `{old_password,new_password}` | 前端校验新密码 ≥8、两次一致;成功 → `{relogin:true}` → 清 token 回登录页(后端已轮换 JWT 密钥,旧 token 必然失效) |

登录页:账号/密码/登录按钮,居中卡片即可,无第三方/注册/忘记密码入口。AuthGuard:未登录访问 `/rabbithole/*` → 登录页;已登录访问登录页 → homePath。

### 2.4 路由与侧边栏(§8.0,八菜单)

```
/auth/login                  登录页
/rabbithole/overview         总览         R0 占位
/rabbithole/clients          客户端       R0 占位
/rabbithole/domains          域名服务     R0 占位(http/https/tcpmux)
/rabbithole/tcp              TCP 隧道     R0 占位
/rabbithole/udp              UDP 隧道     R0 占位
/rabbithole/secret           密钥访问     R0 占位(stcp/sudp/xtcp + 访问器 tab)
/rabbithole/certs            证书         R0 占位
/rabbithole/settings         设置         R0 占位
```

重定向(`/rabbithole/services` → `/rabbithole/tcp`;`/rabbithole/visitors` → `/rabbithole/secret?tab=visitors`)现在就建好。403/404 兜底页各一。侧边栏:antd `Layout.Sider` + `Menu`,顶栏右侧放用户名与退出按钮(Popconfirm)。

### 2.5 构建接线

- `pnpm build` 产出 `console/dist`。
- 改 `crates/rh-server/build.rs`:检测顺序 `../../console/dist` → `../../web/apps/web-antd/dist`,**任一存在即置 `embed_ui` cfg**(两个路径都打 rerun-if-changed;console 优先)。改完 `cargo build` 应出现 `embedding web console from .../console/dist` warning。
- `deploy/rh-server.toml` 不改;本地验证用 `[web].static_dir` 指向 `console/dist` 或直接依赖内嵌。

### R0 门禁

```bash
cd console && pnpm install && pnpm typecheck && pnpm build   # 全部 exit 0
cd .. && cargo build -p rh-server                             # embed warning 指向 console/dist
./target/debug/rh-server(或现有启动方式,配置 static_dir)
```

浏览器冒烟(R0):首启 stdout 横幅拿 admin 密码 → 登录 → 落在总览 → 八菜单全部可点(占位页)→ 深链(直接刷 `/rabbithole/tcp`)不 404 → 退出 → 回登录页且后端 events 出现 `logout` 审计 → 旧路径 `/rabbithole/services` 重定向到 `/rabbithole/tcp`。

## 3. R1:对等迁移(客户端 / 设置 / 总览)

行为基线 = 对应 vue 页,先读再写;组件 1:1 换成 antd React 版(Table/Modal/Form/Switch/Tag/Popconfirm/Statistic)。

### 3.1 客户端页

- 列表列:名称、ID、在线(Tag)、启用(Switch 即时生效)、版本/主机、备注、操作(编辑/轮换密钥/删除确认)。
- 新建:名称 + 备注 → 创建 → **一次性 secret 弹窗**(readonly Input + 复制按钮,关闭即不可再看)。
- 编辑:名称 + 备注 + enabled。轮换:确认后弹一次性 secret。
- **F2 配额容错降级**(后端 M1 未做):表单加"高级"折叠区(rate_limit 字符串、max_conns/max_services/flow_limit 数字,空/0=不限),提交时随 PUT 发送——后端当前会忽略未知字段,属预期;列表的「流量 used/limit」「连接 n/limit」列**仅当 API 响应含这些字段时渲染**,字段缺失就整列不出现。类型定义把这些字段标 optional。

### 3.2 设置页

改密表单(当前密码/新密码/确认),规则与成功后的强制重登流程同 §2.3;另加一块只读卡片:Prometheus `/metrics` 地址展示(取 `location.origin + '/metrics'`,说明文字"需在服务端启用 metrics")。

### 3.3 总览页

- 6 张统计卡片(客户端总数/在线、运行中服务、当前连接、入/出流量)+ 最近事件表(时间/级别 Tag/来源/消息,limit 20)。
- 数据源 `GET /api/stats/overview` + `GET /api/events?limit=20`,TanStack Query `refetchInterval: 5000`,失败保持原值不清空。
- ⚠️ 已知现状:`bytes_in/bytes_out` 后端从未递增,当前恒 0(F1/M1 修复)。卡片照常渲染,不要在前端造假数据。

### R1 门禁

typecheck/build 全绿 + 浏览器冒烟:与旧 UI 并排打开,逐页核对操作集是否对等(建客户端→拿密钥→建/停/删、改密后强制重登、总览 5s 自刷)。事件 toast 文案与旧版一致口径(中文)。

## 4. R2:类型页拆分 + 字段矩阵(吸收 F7,工作量最大)

### 4.1 ProxyListPage 参数化组件

一个组件承载四个页面:`props: { kinds: ProxyKind[], defaultKind, title, description }`。

- 单 kind 页(tcp/udp):表单**无** kind 选择器;多 kind 页(domains: http/https/tcpmux;secret: stcp/sudp/xtcp):表单顶部 kind 选择(编辑时锁定)。
- 列表按 `kinds` 过滤 `GET /api/services` 结果(客户端下拉筛选可选做)。
- 列:单 kind 隧道页含「监听」列(`bind_ip:bind_port`,0 显示"共享端口");domains 页以「域名」列替换(首个域名 + `+n`);secret 页含密钥/目标代理列。通用列:名称/状态徽标(7 态:running green、pending blue、offline/disabled default、bind_error/apply_error red、unhealthy orange,附 status_reason 截断展示)/目标(有 plugin 时加紫色 Tag)/池/启用 Switch/操作。
- 「高级」区默认折叠;基础区必填项 ≤5(客户端、名称、类型、目标地址、目标端口)。

### 4.2 表单字段(严格按 design-parity.md §8.1 矩阵)

字段按 kind 显隐,完整矩阵以文档为准,速记:

- **tcp/udp**:bind_ip/bind_port(0=自动)、target、pool_size(0-16)、高级:bandwidth_limit("1MB"式字符串,空=不限)+mode、proxy_proto(none/v1/v2,仅 tcp)、group/group_key、health_check 子表单(type tcp/http、interval/timeout/max_failed、http 加 path)、plugin 选择器。
- **http**:custom_domains(逗号分隔,支持 `*.example.com` 通配)、subdomain、locations 多值、host_rewrite、http_user/password、request/response headers(增/改/删行编辑器)、route_by_http_user、not_found_html(多行文本,≤8KiB)、cache 子表单(enabled/ttl/max_size_mb/extensions/max_item_size_kb)、高级同上(无 bind)。
- **https**:custom_domains/subdomain + 高级。
- **tcpmux**:custom_domains + 高级。
- **stcp/sudp/xtcp**:secret_key + allow_users 多值 + 高级。
- **plugin**(9 种,按 `PluginSpec` 动态子表单,启用后隐藏 target 区):unix_domain_socket(unix_path)、http_proxy(user/password)、socks5(user/password)、static_file(local_path 等)、https2http/https2https(crt/key/local_addr/host_rewrite)、http2https/http2http(local_addr/host_rewrite/headers)、tls2raw(local_addr/crt/key)。提交结构按后端 `ProxySpec` JSON 形状组装(spec.http / spec.transport / spec.plugin 等嵌套),编辑回读时从 `spec` 反解——对照 `web/apps/web-antd/src/views/rabbithole/services/index.vue` 的 `openEdit` 现有做法。

### 4.3 密钥访问页双 tab

「规则」(stcp/sudp/xtcp 的 ProxyListPage 实例)+「访问器」(平移 `web/.../visitors/index.vue` 全部行为:CRUD、启停、目标代理、密钥留空=不变、压缩)。tab 状态写 query 参数 `?tab=visitors`,刷新保持。

### 4.4 批量导入

各类型页工具区「批量导入」按钮:Modal 内粘贴 JSON 数组 → `POST /api/services/batch` → 逐条回报成功/失败(message 列表),完成后刷新。

### R2 门禁

typecheck/build 全绿 + 冒烟(每 kind 至少一条全生命周期):

- tcp:建(自动端口)→ 状态 running → `curl` 打通 → 编辑改 pool_size/限速 → 停/启/删;
- udp:建 → `nc -u` 打通;
- http:建(带 domain+locations+basic auth)→ 域名+路径分流、401、host 改写抓包验证;
- https/tcpmux:各建一条,回读一致;
- secret:建 stcp + 访问器 → 双端连通;
- 插件抽查 3 种(socks5/unix_domain_socket/static_file);
- 旧路由重定向、tab 刷新持久、高级区折叠态不影响最短创建路径。

## 5. R3:证书页、图表、接入引导

### 5.1 证书页(按 design-parity.md §7 契约,后端 M3 未落地时容错)

- CRUD:`GET/POST/PUT/DELETE /api/certs`;表单 name + cert_pem/key_pem(粘贴框或文件上传)+ domains(缺省留空,文案注明"留空从证书 SAN/CN 自动提取");列表显示 name/domains/到期时间(≤30 天橙色徽标)/指纹;**key_pem 任何响应都不回显**。
- 后端未实现时(M3 之前):`GET /api/certs` 404 → 页面空态 + 顶部提示"需要服务端 M3";前端表单仍按契约实现完整,M3 落地后无需改前端即可用。

### 5.2 总览图表(F8)

- recharts 双折线(入/出速率),数据 = 每 5s 轮询的 `bytes_in/out` **差分**,前端维护 60 点滚动窗口(5 分钟);断网/失败期间断线显示,不外推、不补零。
- 注意当前后端计数恒 0(F1/M1 修复前),图表会是平线——预期行为,不造假。

### 5.3 接入引导弹窗(F8)

- 客户端「新建成功」与「轮换成功」的 secret 弹窗内加折叠区"rh-client.toml 模板":`server_addr`(可编辑,默认 `location.hostname + :7000`,旁注"以实际控制端口为准")、transport 下拉(noise/tls,默认 noise)、client_id(已填)、secret(已填);一键复制。
- 服务列表如响应含 `conns_current` 则加「连接」列(后端 F1 未落地前不出现,同 §3.1 容错规则)。

### R3 门禁

typecheck/build 全绿 + 冒烟:证书页空态与表单校验(非法 PEM 前端拦截提示);图表在打流时呈趋势线(若 F1 已合入则用 `iperf3` 验证量级 ±10%,未合入则验证平线+断线行为);引导模板复制后内容完整可读。

## 6. 交付要求

- 分阶段提交(R0/R1/R2/R3 各一段清晰的变更集),每阶段的门禁命令输出结论(exit code + 关键结果)写进最终报告。
- `console/README.md`:开发(dev server + 代理)、构建、被 rh-server 托管的两种方式(static_dir / 内嵌)、目录结构说明。
- 最终报告附:bundle 体积(`dist` gzip 后估算,对照 ≤500KB gz 目标)、与旧 UI 的功能对等清单(逐页打勾)、已知依赖后端未落地的降级项清单(F1 计数、F2 配额、F6 证书)。
- **明确不做**:删除 `web/`、把 build.rs 旧路径检测移除、改 deploy 配置默认值——这是 CUT 阶段的事,前置条件是本任务全部验收通过 + 人工确认 `tests/e2e*.sh` 浏览器用例在新 UI 重跑通过。
