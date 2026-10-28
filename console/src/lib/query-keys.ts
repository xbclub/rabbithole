/** 统一的 TanStack Query key,避免各页与 ⌘K 面板各写一份导致缓存不共享。 */
export const qk = {
  clients: ['clients'] as const,
  services: (clientId?: string) => ['services', clientId ?? 'all'] as const,
  /** 单条规则(M8:编辑弹窗回读完整 spec)。列表失效时前缀 ['services'] 一并失效。 */
  service: (id: number) => ['services', 'detail', id] as const,
  visitors: (clientId?: string) => ['visitors', clientId ?? 'all'] as const,
  certs: ['certs'] as const,
  apiKeys: ['api-keys'] as const,
  stats: ['stats'] as const,
  events: (limit: number) => ['events', limit] as const,
  serverConfig: ['server-config'] as const,
};
