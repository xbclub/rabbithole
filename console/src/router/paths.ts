/**
 * 路由路径表(与旧版逐字一致,URL 100% 稳定兼容)。
 * 旧 vben 路径 /rabbithole/services、/rabbithole/visitors 保留重定向。
 */
export const ROUTES = {
  login: '/user/login',
  overview: '/rabbithole/overview',
  clients: '/rabbithole/clients',
  tcp: '/rabbithole/tcp',
  udp: '/rabbithole/udp',
  domains: '/rabbithole/domains',
  secret: '/rabbithole/secret',
  certs: '/rabbithole/certs',
  settings: '/rabbithole/settings',
} as const;

/** 隧道中枢涵盖的所有子路径集合(用于顶部胶囊导航高亮判定)。 */
export const TUNNEL_HUB_PATHS: readonly string[] = [
  ROUTES.tcp,
  ROUTES.udp,
  ROUTES.domains,
  ROUTES.secret,
];

/**
 * 顶部悬浮毛玻璃导航舱五大核心工作域。
 * 将原分散的 4 个协议菜单收敛进统一的「隧道中枢」,内部通过协议透镜无感切页。
 */
export const PRIMARY_NAV_ITEMS = [
  {
    key: 'overview',
    title: '态势总览',
    to: ROUTES.overview,
    icon: 'dashboard',
    matchPaths: [ROUTES.overview],
  },
  {
    key: 'clients',
    title: '边缘节点',
    to: ROUTES.clients,
    icon: 'clients',
    matchPaths: [ROUTES.clients],
  },
  {
    key: 'tunnels',
    title: '隧道中枢',
    to: ROUTES.tcp,
    icon: 'tcp',
    matchPaths: TUNNEL_HUB_PATHS,
  },
  {
    key: 'certs',
    title: 'TLS 证书',
    to: ROUTES.certs,
    icon: 'certs',
    matchPaths: [ROUTES.certs],
  },
  {
    key: 'settings',
    title: '系统控制',
    to: ROUTES.settings,
    icon: 'settings',
    matchPaths: [ROUTES.settings],
  },
] as const;

/**
 * ⌘K 命令面板与完整深链索引分组(保留全部细分协议直达入口)。
 */
export const NAV_GROUPS = [
  {
    label: '核心工作台',
    items: [
      { title: '态势总览', to: ROUTES.overview, icon: 'dashboard' },
      { title: '边缘节点 (客户端)', to: ROUTES.clients, icon: 'clients' },
    ],
  },
  {
    label: '隧道与路由中枢',
    items: [
      { title: 'TCP 隧道透镜', to: ROUTES.tcp, icon: 'tcp' },
      { title: 'UDP 隧道透镜', to: ROUTES.udp, icon: 'udp' },
      { title: '域名服务 (HTTP / HTTPS / TCPMux)', to: ROUTES.domains, icon: 'domains' },
      { title: '私网密钥访问 (STCP / SUDP / XTCP)', to: ROUTES.secret, icon: 'secret' },
      { title: 'P2P 访问器 (Visitors)', to: `${ROUTES.secret}?tab=visitors`, icon: 'secret' },
      { title: 'TLS 证书金库', to: ROUTES.certs, icon: 'certs' },
    ],
  },
  {
    label: '系统控制面',
    items: [
      { title: '服务器热配置与限流', to: ROUTES.settings, icon: 'settings' },
      { title: 'Prometheus 监控指标', to: `${ROUTES.settings}?tab=metrics`, icon: 'settings' },
      { title: 'API 密钥管理', to: `${ROUTES.settings}?tab=apikeys`, icon: 'secret' },
      { title: '修改管理员密码', to: `${ROUTES.settings}?tab=password`, icon: 'settings' },
    ],
  },
] as const;
