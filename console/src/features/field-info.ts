/**
 * 「名词 ℹ 说明」文案字典
 *
 * 来源:旧版 console(Umi Max + antd)的 `InfoLabel` 组件——「名词 + ℹ 图标,悬停/点按弹出该项的功能说明」。
 * 说明文案均照后端真实行为撰写,重写为 Vite + React 19 时按 key 逐条平移,文案逐字保留(中文标点、空格、`—`/`-` 一律原样)。
 * key 采用点号分层的稳定标识:`<域>.<对象>.<字段>`,例如 `service.target_host`、`client.rate_limit`、`settings.ssh_gateway.enabled`、`overview.clients_online`。
 *
 * 本模块为纯数据,不含 UI、不含 React import。
 *
 * 关于动态文案:已穷尽检索旧代码全部 InfoLabel 用法,`info` 均为字符串字面量,没有拼接/变量/JSX 参与;
 * `pages/overview/index.tsx` 的 `metricCell(label, value, extra, info)` 只是把 `info` 透传给 InfoLabel,
 * 但 6 个调用点全部传入字面量,因此无需导出 `fieldInfoDynamicXxx` 函数。
 *
 * 同一名词在不同页面文案不同时(如「名称」「类型」),按域拆成不同 key,不合并;文案完全相同的重复项(如规则弹窗中的「监听端口」)
 * 共用一个 key。
 */

export const FIELD_INFO = {
  // ── 规则弹窗(连接) ──
  'service.client_id': '承载并执行这条规则的边缘客户端,转发流量经它的隧道进出内网',
  'service.name': '规则唯一标识,1-64 位字母/数字/下划线/连字符;访问器按名称+类型匹配目标',
  'service.kind':
    '转发协议,决定公网入口形态:端口直通(TCP/UDP)、域名路由(HTTP/HTTPS/TCPMux)或密钥访问(STCP/SUDP/XTCP)',
  'service.bind_ip': '服务端公网侧绑定地址,留空 0.0.0.0 监听全部网卡',
  'service.bind_port': '服务端公网入口端口,进入的流量经隧道送往目标地址',
  'service.bind_auto': '开启后由服务端在允许端口范围内挑选空闲端口,免人工规划端口',

  // ── 规则弹窗(目标) ──
  'service.target_host': '内网服务的真实地址,由客户端侧发起连接(须在客户端所在网络可达)',
  'service.target_port': '内网服务的真实端口',
  'service.custom_domains': '逗号分隔,支持 *.example.com 单层通配;请求按域名路由到本规则',
  'service.subdomain': '与服务端 vhost.subdomain_host 拼接生成完整域名',

  // ── 规则弹窗(行为) ──
  'service.enabled': '创建后立即生效并开始监听;关闭则仅保存、不承载流量',
  'service.advanced': '传输压缩、限速、负载均衡分组、健康检查与 HTTP 增强等低频配置',

  // ── 规则弹窗(插件) ──
  'service.plugin_kind': '在客户端本地处理流量(静态文件/SOCKS5/https2http 等),替代直连目标服务',
  'service.plugin_params': '按所选插件类型展开对应参数',

  // ── 规则弹窗(传输与限速) ──
  'service.use_compression': '隧道传输启用 snappy 压缩,节省公网带宽',
  'service.bandwidth_limit': '该规则单连接限速,如 1MB / 512KB;留空不限',
  'service.bandwidth_mode': '在服务端出口还是客户端入口执行限速',
  'service.proxy_proto': '向目标服务传递真实客户端地址:v1 明文头 / v2 二进制头',
  'service.pool_size': '预热连接数,降低首包延迟;0 = 使用服务端默认',

  // ── 规则弹窗(负载均衡组) ──
  'service.group': '组内相同类型+监听端口的规则共享监听并轮询分发,实现负载均衡',

  // ── 规则弹窗(健康检查) ──
  'service.hc_enabled': '客户端定期探测目标服务,连续失败自动摘除流量',
  'service.hc_type': 'TCP 端口连通或 HTTP 请求判定健康状态',
  'service.hc_interval': '探测周期',
  'service.hc_timeout': '单次探测的超时时间',
  'service.hc_max_failed': '连续失败达到该次数标记 unhealthy 并从池中摘除',
  'service.hc_path': '仅 HTTP 检查类型有效',

  // ── 规则弹窗(HTTP 高级) ──
  'service.locations': '逗号分隔,如 /api,/static;空 = 匹配全部路径',
  'service.host_rewrite': '改写转发到目标的 Host 头',
  'service.http_user': '访问本规则时要求基础认证,与密码成对填写',
  'service.http_password': '与用户名成对填写的认证密码',
  'service.req_headers': '追加或覆盖发往目标的请求头',
  'service.resp_headers': '追加或覆盖返回客户端的响应头',
  'service.route_by_http_user': '按经 PROXY protocol 传入的终端用户分流,留空不启用',

  // ── 规则弹窗(域名增强) ──
  'service.not_found_html': '域名命中但路径未命中时返回的 HTML,最多 8KiB',
  'service.cache_enabled': '缓存 GET 静态资源响应,命中直接由服务端返回(响应头 X-Cache: HIT/MISS)',
  'service.cache_ttl': '缓存条目存活时间',
  'service.cache_max_size_mb': '规则级缓存总容量,满后逐出旧条目',
  'service.cache_extensions': '逗号分隔;仅这些扩展名的 GET 响应入缓存',
  'service.cache_max_item_size_kb': '超过该大小的响应不缓存',

  // ── 访问器 ──
  'visitor.name': '访问器标识',
  'visitor.kind': 'STCP/SUDP/XTCP,创建后不可改',
  'visitor.bind': '访问端本地绑定,连接它即经隧道进入目标规则',
  'visitor.target_proxy_name': '要访问的密钥访问规则;凭据自动继承',
  'visitor.compression': '中继通道启用 snappy 压缩',
  'visitor.form.client_id': '在这台客户端上打开本地监听端口,访问器随它运行',
  'visitor.form.name': '访问器标识,同一客户端内唯一',
  'visitor.form.kind': '须与目标规则类型一致',
  'visitor.form.bind_addr': '访问端本地绑定地址,访问方连接它进入隧道',
  'visitor.form.bind_port': '本地入口端口,流量经服务端中继或打洞直达目标规则',
  'visitor.form.target_proxy_name':
    '选择要访问的密钥访问规则,类型须一致;中继凭据自动继承目标规则,无需填写',

  // ── 客户端 ──
  'client.id': '客户端 UUID,末 8 位展示,可复制完整值',
  'client.online': '客户端会话是否连着服务端;离线规则不再承载流量',
  'client.version_host': '客户端上报的版本与主机信息',
  'client.bytes': '该客户端名下全部规则的入站/出站累计字节(进程内计量,重启归零)',
  'client.conns': '当前并发连接数',
  'client.detail.bytes': '进程内累计,重启归零',
  'client.form.name': '客户端显示名;创建后凭据仅展示一次',
  'client.form.remark': '任意说明文字,便于识别机器',
  'client.form.advanced':
    '配额与限速;保存即生效,并发/流量上限在接入时执行,限速在 TCP 隧道转发中执行',
  'client.rate_limit': '客户端整体限速,如 1MB / 512KB;留空不限',
  'client.max_conns': '该客户端全部规则的并发连接上限',
  'client.max_services': '可创建规则数上限;0 = 用全局默认',
  'client.flow_limit_mb': '入+出累计流量达到上限后拒绝新连接',

  // ── 设置 ──
  'settings.ssh_gateway.enabled': '开启后可用 ssh -R 在服务端开临时转发端口,凭据与客户端一致',
  'settings.ssh_gateway.bind_addr': 'SSH 网关绑定地址,留空 = 0.0.0.0',
  'settings.ssh_gateway.bind_port': 'SSH 网关端口',
  'settings.ssh_gateway.public_keys':
    '每行一个公钥,用户名须为已启用客户端 ID;密码认证(客户端密钥)始终可用',
  'settings.allow_ports': '逗号分隔,支持范围如 10000-20000,30000;空 = 不限制',
  'settings.max_services_per_client': '全局默认上限;单个客户端可在「客户端」页覆盖,0 = 不限',
  'settings.log_level': '服务端日志级别,保存立即生效',
  'settings.prometheus.enabled':
    '开启后启动独立端口的 /metrics 监听(无鉴权,默认仅本机可达),即时生效并写回配置文件;关闭即停止监听',
  'settings.prometheus.bind_addr':
    '监听网卡地址:127.0.0.1 仅本机抓取;0.0.0.0 对外暴露(端口无鉴权,注意防火墙)。保存即热重启监听器,绑定失败自动回滚',
  'settings.prometheus.port': '独立监听端口,不与管理面共口',

  // ── 设置(并发与限流,0 = 不限) ──
  'settings.limits.max_mux_streams':
    '单条复用连接(yamux/QUIC)上的并发流硬顶,数据通道即流;0 = 不限。新会话生效',
  'settings.limits.max_data_conns_per_client':
    '每客户端并发数据通道(池+在途+转发中);0 = 不限。立即生效;实际值自动钳制到复用流上限-1024',
  'settings.limits.max_udp_sessions_per_proxy':
    '每条 UDP 规则的会话映射数(按源地址);0 = 不限。立即生效,超限丢新会话包、既有会话不受影响',
  'settings.limits.unauth_conns_per_ip':
    '每 IP 同时处于认证窗口的连接数,防匿名洪水;0 = 不限(不建议)。立即生效',
  'settings.limits.max_pool_size': '每规则预热池深度上限;0 = 不限。仅对新建/更新的规则生效',
  'settings.limits.default_pool_size': '规则未指定池深时使用的默认值(0 = 不预热)',
  'settings.limits.control_send_queue':
    '控制连接发送队列深度,满则背压等待(不拒连);须 ≥ 16。新会话生效',
  'settings.limits.udp_packet_size':
    'UDP 报文缓冲字节上限;0 = 用默认 64KB;范围 1500-65536。立即生效',
  'settings.limits.pool_wait_timeout_ms':
    '用户连接等待池补拨的总时长(毫秒);范围 100-600000。立即生效',

  // ── 证书 ──
  'cert.name': '证书条目标识,便于识别',
  'cert.domains': '留空自动从证书 SAN/CN 提取;HTTPS 请求 SNI 命中(含单层通配)即由服务端终结 TLS',
  'cert.cert_pem': 'PEM 格式证书链,粘贴或用下方文件选择',
  'cert.key_pem': '仅上传时提交,任何响应不回显;编辑时需重新粘贴',
  'cert.upload': '按文件名或 PEM 头自动识别证书/私钥并填充',

  // ── 规则列表列头 ──
  'service.list.status':
    'running=监听中;offline=承载客户端离线;pending=等待发布;bind/apply_error=发布失败;未启用=仅保存',
  'service.list.pool': '预热连接池 当前/目标',
  'service.list.conns': '当前并发连接数',

  // ── 总览指标 ──
  'overview.clients_online': '已连接服务端的客户端数 / 注册总数',
  'overview.services_running': '正在监听公网入口的规则数 / 启用总数',
  'overview.conns_current': '全部规则当前承载的并发连接',
  'overview.rate_in': '公网侧流入服务端的流量速率(按最近采样)',
  'overview.rate_out': '服务端流向公网侧的流量速率(按最近采样)',
  'overview.bytes_total': '服务端启动以来的入+出累计(进程内计量,重启归零)',
} as const satisfies Record<string, string>;

export type FieldInfoKey = keyof typeof FIELD_INFO;

/** 按 key 取说明文案;key 为编译期受限的 FieldInfoKey。 */
export function fieldInfo(key: FieldInfoKey): string {
  return FIELD_INFO[key];
}
