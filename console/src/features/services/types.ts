/**
 * 服务规则表单的纯类型定义。
 *
 * 这是从旧版 `components/ProxyFormModal.tsx` 平移出来的纯逻辑(剥离 React / antd / DOM),
 * 后端 `ProxySpec` 契约零变更:键名、嵌套层级、默认值处理与省略规则均与旧实现逐字段等价。
 * 本文件不 import 任何 UI 库,只描述形状。
 */

/** 代理类型(kind),取值与后端 `ProxyKind` 一致。 */
export type ProxyKind = 'http' | 'https' | 'sudp' | 'stcp' | 'tcp' | 'tcpmux' | 'udp' | 'xtcp';

/** 后端支持的 9 种插件名(不含"不启用")。 */
export type PluginName =
  | 'unix_domain_socket'
  | 'http_proxy'
  | 'socks5'
  | 'static_file'
  | 'https2http'
  | 'https2https'
  | 'http2https'
  | 'http2http'
  | 'tls2raw';

/** 表单插件选择值:`none` = 不启用插件(直连目标)。 */
export type PluginKindOption = PluginName | 'none';

/** HTTP 头键值对(与旧版 `HeaderPairsInput` 的值形状一致)。 */
export interface HeaderPairValue {
  name: string;
  value: string;
}

/**
 * 插件子表单的扁平字段名。
 * 提交时去掉 `plugin_` 前缀即为 `spec.plugin` 的键名(与旧版 switch 逐字段一致)。
 */
export type PluginFormField =
  | 'plugin_unix_path'
  | 'plugin_http_user'
  | 'plugin_http_password'
  | 'plugin_user'
  | 'plugin_password'
  | 'plugin_local_path'
  | 'plugin_strip_prefix'
  | 'plugin_local_addr'
  | 'plugin_crt_path'
  | 'plugin_key_path'
  | 'plugin_host_rewrite'
  | 'plugin_request_headers';

/** 带宽限速执行位置。 */
export type BandwidthMode = 'client' | 'server';

/** PROXY protocol 版本。 */
export type ProxyProtoVersion = 'none' | 'v1' | 'v2';

/** 健康检查类型。 */
export type HealthCheckType = 'http' | 'tcp';

/**
 * 表单值形状(与旧版 `FormValues` 完全一致)。
 *
 * - `bind_port` / `target_port` / 各数值项沿用 antd InputNumber 的 `number | null`;
 * - `bind_auto` 是"自动分配监听端口"开关(编辑态禁用),提交时映射为魔法值 0;
 * - `plugin_*` 为扁平字段,提交时按插件类型挑用。
 *
 * 注意:stcp/sudp/xtcp 的 `secret_key` 由服务端生成与轮换(R24),`allow_users` 已在 R32 删除,
 * 二者都不在表单值里,表单零输入。
 */
export interface ProxyFormValues {
  client_id: string;
  name: string;
  kind: ProxyKind;
  enabled: boolean;
  bind_ip: string;
  bind_auto: boolean;
  bind_port: number | null;
  target_host: string;
  target_port: number | null;
  custom_domains: string;
  subdomain: string;
  use_compression: boolean;
  pool_size: number | null;
  bandwidth_limit: string;
  bandwidth_mode: BandwidthMode;
  proxy_proto: ProxyProtoVersion;
  group: string;
  hc_enabled: boolean;
  hc_type: HealthCheckType;
  hc_interval: number | null;
  hc_timeout: number | null;
  hc_max_failed: number | null;
  hc_path: string;
  locations: string;
  host_rewrite: string;
  http_user: string;
  http_password: string;
  route_by_http_user: string;
  req_headers: HeaderPairValue[];
  resp_headers: HeaderPairValue[];
  not_found_html: string;
  cache_enabled: boolean;
  cache_ttl: number | null;
  cache_max_size_mb: number | null;
  cache_extensions: string;
  cache_max_item_size_kb: number | null;
  plugin_kind: PluginKindOption;
  plugin_unix_path: string;
  plugin_http_user: string;
  plugin_http_password: string;
  plugin_user: string;
  plugin_password: string;
  plugin_local_path: string;
  plugin_strip_prefix: string;
  plugin_local_addr: string;
  plugin_crt_path: string;
  plugin_key_path: string;
  plugin_host_rewrite: string;
  plugin_request_headers: HeaderPairValue[];
}

/**
 * 编辑回读所需的服务行字段(旧版 `ServiceRow` 的子集)。
 * 只声明 `serviceToFormValues` 真正读取的字段;`spec` 承载按 kind 分叉的嵌套配置。
 */
export interface ServiceRowLike {
  id?: number;
  client_id: string;
  name: string;
  kind: string;
  enabled: boolean;
  bind_ip: string;
  bind_port: number;
  target_host: string;
  target_port: number;
  group?: null | string;
  plugin?: null | string;
  spec?: Record<string, unknown>;
}

/** 提交给 `POST /api/services` 或 `PUT /api/services/{id}` 的组装后 spec JSON。 */
export type ServiceSpecPayload = Record<string, unknown>;

/** 表单字段校验错误(对应旧版 `form.setFields([{ name, errors }])`)。 */
export interface FormFieldError {
  field: keyof ProxyFormValues;
  message: string;
}
