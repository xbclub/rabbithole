/**
 * rabbithole 管理面业务 API(后端 envelope: {code:0,data,message})。
 * 接口与类型自旧版 console 原样平移,契约零变更;计量/配额字段由后端 M1/M2
 * 提供,此处保持 optional 以兼容旧库数据。
 */
import { httpDelete, httpGet, httpPost, httpPut } from './request';

export interface ClientRow {
  id: string;
  name: string;
  enabled: boolean;
  online: boolean;
  remark: string;
  created_at: number;
  last_online_at: null | number;
  client_version: string;
  hostname: string;
  os: string;
  expected_revision: number;
  /** F2 配额(0/空 = 不限)。 */
  rate_limit?: string;
  max_conns?: number;
  flow_limit?: number;
  max_services?: number;
  /** F1 计量(M1 后端落地前不返回)。 */
  bytes_in?: number;
  bytes_out?: number;
  conns_current?: number;
}

export interface ServiceRow {
  id: number;
  client_id: string;
  name: string;
  kind: string;
  enabled: boolean;
  bind_ip: string;
  bind_port: number;
  target_host: string;
  target_port: number;
  group: null | string;
  status: string;
  status_reason: string;
  healthy: boolean;
  pool_ready: number;
  plugin: null | string;
  spec: Record<string, unknown>;
  /** F1 计量(M1 后端落地前不返回)。 */
  bytes_in?: number;
  bytes_out?: number;
  conns_current?: number;
}

export interface VisitorRow {
  id: number;
  client_id: string;
  target_client_id: null | string;
  name: string;
  kind: string;
  enabled: boolean;
  bind_addr: string;
  bind_port: number;
  target_proxy_name: string;
  /** 完整 visitor spec(use_compression / secret_key 等在此读取)。 */
  spec: Record<string, unknown>;
}

export interface StatsOverview {
  clients_total: number;
  clients_online: number;
  services_total: number;
  services_running: number;
  conns_current: number;
  bytes_in: number;
  bytes_out: number;
}

export interface EventRow {
  id: number;
  ts: number;
  level: string;
  source: string;
  message: string;
  detail: string;
}

// ---------- clients ----------
export async function listClients(): Promise<ClientRow[]> {
  return httpGet<ClientRow[]>('/clients');
}
export async function createClient(data: {
  name: string;
  remark?: string;
  /** F2 配额字段:后端 M1 前忽略未知字段,属预期。 */
  rate_limit?: string;
  max_conns?: number;
  flow_limit?: number;
  max_services?: number;
}): Promise<{ id: string; secret: string }> {
  return httpPost('/clients', data);
}
export async function updateClient(
  id: string,
  data: {
    name?: string;
    remark?: string;
    enabled?: boolean;
    rate_limit?: string;
    max_conns?: number;
    flow_limit?: number;
    max_services?: number;
  },
): Promise<unknown> {
  return httpPut(`/clients/${id}`, data);
}
export async function deleteClient(id: string): Promise<unknown> {
  return httpDelete(`/clients/${id}`);
}
export async function rotateClientSecret(id: string): Promise<{ secret: string }> {
  return httpPost<{ secret: string }>(`/clients/${id}/rotate-secret`);
}

// ---------- services ----------
export async function listServices(params?: { client_id?: string }): Promise<ServiceRow[]> {
  return httpGet<ServiceRow[]>('/services', { params });
}
export async function createService(spec: Record<string, unknown>): Promise<unknown> {
  return httpPost('/services', spec);
}
export async function updateService(id: number, spec: Record<string, unknown>): Promise<unknown> {
  return httpPut(`/services/${id}`, spec);
}
export async function deleteService(id: number): Promise<unknown> {
  return httpDelete(`/services/${id}`);
}
export async function enableService(id: number): Promise<unknown> {
  return httpPost(`/services/${id}/enable`);
}
export async function disableService(id: number): Promise<unknown> {
  return httpPost(`/services/${id}/disable`);
}
/** 轮换密钥访问规则凭据:服务端生成新随机值,访问器连接时自动继承,无需任何同步。 */
export async function rotateServiceSecret(id: number): Promise<{ secret: string }> {
  return httpPost(`/services/${id}/rotate-secret`);
}
/** 批量启停删(后端语义:按 ids + action,非批量导入)。 */
export async function batchServices(
  ids: number[],
  action: 'delete' | 'disable' | 'enable',
): Promise<Array<{ code: number; id: number; message: string }>> {
  return httpPost('/services/batch', { ids, action });
}

// ---------- visitors ----------
export async function listVisitors(params?: { client_id?: string }): Promise<VisitorRow[]> {
  return httpGet<VisitorRow[]>('/visitors', { params });
}
export async function createVisitor(spec: Record<string, unknown>): Promise<unknown> {
  return httpPost('/visitors', spec);
}
export async function updateVisitor(id: number, spec: Record<string, unknown>): Promise<unknown> {
  return httpPut(`/visitors/${id}`, spec);
}
export async function deleteVisitor(id: number): Promise<unknown> {
  return httpDelete(`/visitors/${id}`);
}
export async function enableVisitor(id: number): Promise<unknown> {
  return httpPost(`/visitors/${id}/enable`);
}
export async function disableVisitor(id: number): Promise<unknown> {
  return httpPost(`/visitors/${id}/disable`);
}

// ---------- certs(F6 契约;后端 M3 落地前 404,页面容错) ----------
export interface CertRow {
  id: number;
  name: string;
  domains: string[];
  /** 到期时间(unix ms);命名按契约字段 not_after。 */
  not_after: null | number;
  /** 指纹摘要(key_pem 任何响应不回显)。 */
  fingerprint: string;
  created_at: number;
}

export async function listCerts(): Promise<CertRow[]> {
  return httpGet<CertRow[]>('/certs');
}
export async function createCert(data: {
  cert_pem: string;
  domains?: string[];
  key_pem: string;
  name: string;
}): Promise<unknown> {
  return httpPost('/certs', data);
}
export async function updateCert(
  id: number,
  data: { cert_pem: string; domains?: string[]; key_pem: string; name: string },
): Promise<unknown> {
  return httpPut(`/certs/${id}`, data);
}
export async function deleteCert(id: number): Promise<unknown> {
  return httpDelete(`/certs/${id}`);
}

// ---------- stats / events ----------
export async function statsOverview(): Promise<StatsOverview> {
  return httpGet<StatsOverview>('/stats/overview');
}
export async function listEvents(params?: { limit?: number; level?: string }): Promise<EventRow[]> {
  return httpGet<EventRow[]>('/events', { params });
}

// ---------- 服务器配置(热生效子集) ----------

/** [limits] 全九项;0 = 不限(max_pool_size 语义见字段注释)。 */
export interface LimitsConfig {
  /** 单条复用连接(yamux/QUIC)并发流硬顶;0 = 不限;新会话生效 */
  max_mux_streams: number;
  /** 每客户端并发数据通道上限;0 = 不限;立即生效 */
  max_data_conns_per_client: number;
  /** 每代理 UDP 会话上限;0 = 不限;立即生效 */
  max_udp_sessions_per_proxy: number;
  /** 每规则池深上限;0 = 不限;仅对新建/更新规则生效 */
  max_pool_size: number;
  /** 规则未指定池深时的默认值 */
  default_pool_size: number;
  /** 控制帧发送队列深度;新会话生效 */
  control_send_queue: number;
  /** 每 IP 认证窗口并发;0 = 不限;立即生效 */
  unauth_conns_per_ip: number;
  /** UDP 报文缓冲;0 = 用默认(64KB) */
  udp_packet_size: number;
  /** 用户连接等池补拨超时(毫秒) */
  pool_wait_timeout_ms: number;
}

export interface ServerConfigView {
  config: Record<string, unknown>;
  hot: {
    allow_ports: string[];
    log_level: string;
    max_services_per_client: number;
    ssh_gateway: {
      enabled: boolean;
      bind_addr: string;
      bind_port: number;
    };
    metrics: { enable_prometheus: boolean; bind_addr: string; port: number };
    limits: LimitsConfig;
  };
  authorized_keys: null | string;
}

export interface ServerConfigUpdate {
  ports?: { allow_ports?: string[]; max_services_per_client?: number };
  log?: { level?: string };
  ssh_gateway?: { enabled?: boolean; bind_addr?: string; bind_port?: number };
  metrics?: { enable_prometheus?: boolean; bind_addr?: string; port?: number };
  limits?: Partial<LimitsConfig>;
  authorized_keys?: string;
}

export interface ServerConfigResult {
  applied: string[];
  error?: string;
  persisted: boolean;
  restart_required: string[];
}

export async function getServerConfig(): Promise<ServerConfigView> {
  return httpGet<ServerConfigView>('/server/config');
}

export async function putServerConfig(update: ServerConfigUpdate): Promise<ServerConfigResult> {
  return httpPut<ServerConfigResult>('/server/config', update);
}

/** 修改自身密码:成功后所有会话失效(后端轮换 JWT 密钥),需重新登录。 */
export async function changePassword(
  oldPassword: string,
  newPassword: string,
): Promise<{ relogin: boolean }> {
  return httpPost('/auth/password', {
    new_password: newPassword,
    old_password: oldPassword,
  });
}
