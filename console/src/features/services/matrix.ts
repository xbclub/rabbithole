/**
 * kind → 字段可见性矩阵,以及 9 种插件的子表单字段定义。
 *
 * 这是从旧版 `components/ProxyFormModal.tsx` 平移出来的纯逻辑(剥离 React / antd / DOM),
 * 后端 `ProxySpec` 契约零变更:字段显隐、必填、魔法值语义与插件参数均与旧实现一致。
 * `spec.ts` 直接消费本文件的 traits / PLUGIN_FIELDS,保证"表单怎么显示"与"提交什么"同源,
 * 不会出现矩阵改了而载荷没改的漂移。
 */
import type {
  HeaderPairValue,
  PluginFormField,
  PluginKindOption,
  PluginName,
  ProxyFormValues,
  ProxyKind,
} from './types';

/** spec 中按 kind 分叉的段落键(旧版 handleFinish 末尾循环的 6 段)。 */
export type KindSectionKey = 'http' | 'https' | 'tcpmux' | 'stcp' | 'sudp' | 'xtcp';

/** 与旧版 `KIND_SECTION` 一致:kind → spec 段落键(tcp/udp 无段落)。 */
export const KIND_SECTION: Partial<Record<ProxyKind, KindSectionKey>> = {
  http: 'http',
  https: 'https',
  tcpmux: 'tcpmux',
  stcp: 'stcp',
  sudp: 'sudp',
  xtcp: 'xtcp',
};

/** 旧版 handleFinish 末尾遍历的段落键顺序(逐一 delete 非当前 kind 的段落)。 */
export const KIND_SECTION_KEYS: readonly KindSectionKey[] = [
  'http',
  'https',
  'tcpmux',
  'stcp',
  'sudp',
  'xtcp',
];

/** kind 选择项(与旧版 `KIND_OPTIONS` 一致)。 */
export const KIND_OPTIONS: ReadonlyArray<{ label: string; value: ProxyKind }> = [
  { label: 'TCP', value: 'tcp' },
  { label: 'UDP', value: 'udp' },
  { label: 'HTTP', value: 'http' },
  { label: 'HTTPS', value: 'https' },
  { label: 'STCP', value: 'stcp' },
  { label: 'SUDP', value: 'sudp' },
  { label: 'XTCP', value: 'xtcp' },
  { label: 'TCPMux', value: 'tcpmux' },
];

/** 9 种插件名(旧版 `PLUGIN_KINDS` 去掉 'none')。 */
export const PLUGIN_NAMES: readonly PluginName[] = [
  'unix_domain_socket',
  'http_proxy',
  'socks5',
  'static_file',
  'https2http',
  'https2https',
  'http2https',
  'http2http',
  'tls2raw',
];

/** 插件选择项('none' 的文案与旧版一致)。 */
export const PLUGIN_KIND_OPTIONS: ReadonlyArray<{ label: string; value: PluginKindOption }> = [
  { label: '不启用(直连目标)', value: 'none' },
  ...PLUGIN_NAMES.map((k): { label: string; value: PluginKindOption } => ({ label: k, value: k })),
];

/** 表单字段分组(与旧版 Modal 的分区一致)。 */
export type FieldGroupKey =
  | 'connection'
  | 'target'
  | 'domain'
  | 'behavior'
  | 'plugin'
  | 'transport'
  | 'group'
  | 'health_check'
  | 'http_advanced'
  | 'domain_enhance';

/** 分组标题(新 UI 分节渲染用)。 */
export const GROUP_LABELS: Record<FieldGroupKey, string> = {
  connection: '连接',
  target: '目标(内网服务)',
  domain: '域名',
  behavior: '行为',
  plugin: '插件',
  transport: '传输与限速',
  group: '负载均衡组',
  health_check: '健康检查',
  http_advanced: 'HTTP 高级',
  domain_enhance: '域名增强',
};

/** 输入控件形态(由新 UI 决定渲染成什么,本文件不含任何 UI 依赖)。 */
export type FieldInputType =
  'text' | 'number' | 'checkbox' | 'select' | 'textarea' | 'header-pairs';

/** 下拉选项。 */
export interface SelectOption {
  label: string;
  value: string;
}

/** 单字段的可见性/必填/魔法值规则。 */
export interface FieldRule {
  /** 表单值字段名。 */
  name: keyof ProxyFormValues;
  label: string;
  group: FieldGroupKey;
  input: FieldInputType;
  required: boolean;
  /** 仅新建态可见(编辑态由后端回读且锁定,如 bind_auto)。 */
  createOnly?: boolean;
  /** 仅当新建态未勾选"自动分配"时可见(bind_port)。 */
  createOnlyWhenManualBind?: boolean;
  /** 仅当未启用插件时可见(target 区;插件接管目标)。 */
  hiddenWhenPlugin?: boolean;
  /** 仅当健康检查类型为 http 时可见(hc_path)。 */
  visibleWhenHcType?: 'http';
  min?: number;
  max?: number;
  precision?: number;
  placeholder?: string;
  options?: readonly SelectOption[];
  /** 魔法值语义(0 / 空串 / 缺省回退等);旧版用开关或省略表达,这里显式记录。 */
  magic?: string;
}

const CONNECTION_FIELDS: FieldRule[] = [
  {
    name: 'client_id',
    label: '所属客户端',
    group: 'connection',
    input: 'select',
    required: true,
    magic: '编辑态锁定,不可更换客户端',
  },
  {
    name: 'name',
    label: '名称',
    group: 'connection',
    input: 'text',
    required: true,
    placeholder: 'web-nginx',
    magic: '1-64 位字母/数字/下划线/连字符',
  },
  {
    name: 'kind',
    label: '类型',
    group: 'connection',
    input: 'select',
    required: true,
    options: KIND_OPTIONS,
    magic: '仅页内多 kind 时可见;编辑态锁定',
  },
];

const BIND_FIELDS: FieldRule[] = [
  {
    name: 'bind_ip',
    label: '监听 IP',
    group: 'connection',
    input: 'text',
    required: false,
    placeholder: '0.0.0.0',
    magic: '默认 0.0.0.0 = 监听全部网卡;提交时原样写入 bind_ip',
  },
  {
    name: 'bind_auto',
    label: '自动分配',
    group: 'connection',
    input: 'checkbox',
    required: false,
    createOnly: true,
    magic: '勾选 → 提交 bind_port = 0(服务端在允许端口范围内挑选空闲端口)',
  },
  {
    name: 'bind_port',
    label: '监听端口',
    group: 'connection',
    input: 'number',
    required: true,
    createOnlyWhenManualBind: true,
    min: 1,
    max: 65535,
    precision: 0,
    magic: '0 = 自动分配(新建勾选"自动分配"时提交 0);编辑态禁用自动分配、按真实端口提交',
  },
];

const TARGET_FIELDS: FieldRule[] = [
  {
    name: 'target_host',
    label: '目标地址',
    group: 'target',
    input: 'text',
    required: true,
    placeholder: '127.0.0.1',
    hiddenWhenPlugin: true,
  },
  {
    name: 'target_port',
    label: '目标端口',
    group: 'target',
    input: 'number',
    required: true,
    min: 1,
    max: 65535,
    precision: 0,
    hiddenWhenPlugin: true,
  },
];

const DOMAIN_FIELDS: FieldRule[] = [
  {
    name: 'custom_domains',
    label: '自定义域名',
    group: 'domain',
    input: 'text',
    required: false,
    placeholder: 'a.example.com,*.example.com',
    magic:
      '逗号分隔;仅最左标签单通配;http/https 需域名或子域名(校验),tcpmux 必须填域名;提交为小写去重后的数组',
  },
  {
    name: 'subdomain',
    label: '子域名',
    group: 'domain',
    input: 'text',
    required: false,
    placeholder: 'app',
    magic: '提交时 trim;空串也会写入段落(tcpmux 无此字段)',
  },
];

const BEHAVIOR_FIELDS: FieldRule[] = [
  { name: 'enabled', label: '启用', group: 'behavior', input: 'checkbox', required: false },
];

const PLUGIN_SELECTOR_FIELD: FieldRule = {
  name: 'plugin_kind',
  label: '插件',
  group: 'plugin',
  input: 'select',
  required: false,
  options: PLUGIN_KIND_OPTIONS,
  magic: 'none = 直连目标;启用后隐藏 target_host/target_port 并改写 spec.plugin',
};

const TRANSPORT_FIELDS: FieldRule[] = [
  {
    name: 'use_compression',
    label: '启用 snappy 压缩',
    group: 'transport',
    input: 'checkbox',
    required: false,
  },
  {
    name: 'bandwidth_limit',
    label: '带宽限制',
    group: 'transport',
    input: 'text',
    required: false,
    placeholder: '留空不限',
    magic: '空串 = 不限速(提交时省略 bandwidth_limit 键)',
  },
  {
    name: 'bandwidth_mode',
    label: '限速位置',
    group: 'transport',
    input: 'select',
    required: false,
    options: [
      { label: '服务端(默认)', value: 'server' },
      { label: '客户端', value: 'client' },
    ],
    magic: "缺省 'server'",
  },
];

const POOL_SIZE_FIELD: FieldRule = {
  name: 'pool_size',
  label: '连接池大小',
  group: 'transport',
  input: 'number',
  required: false,
  min: 0,
  max: 16,
  precision: 0,
  magic: '0 = 使用服务端默认预热连接数;仅 tcp 可见且仅 tcp 提交',
};

const PROXY_PROTO_FIELD: FieldRule = {
  name: 'proxy_proto',
  label: 'PROXY protocol',
  group: 'transport',
  input: 'select',
  required: false,
  options: [
    { label: '不启用', value: 'none' },
    { label: 'v1', value: 'v1' },
    { label: 'v2', value: 'v2' },
  ],
  magic: "缺省 'none';仅 tcp/http 可见,但所有 kind 都会写入 transport.proxy_proto",
};

const GROUP_FIELDS: FieldRule[] = [
  {
    name: 'group',
    label: '分组名',
    group: 'group',
    input: 'text',
    required: false,
    placeholder: '留空不分组',
    magic: '空串 = 不分组(提交时省略 group 键);仅 tcp/udp/http/https 可见',
  },
];

const HEALTH_CHECK_FIELDS: FieldRule[] = [
  {
    name: 'hc_enabled',
    label: '启用',
    group: 'health_check',
    input: 'checkbox',
    required: false,
    magic: '未启用时省略 spec.health_check;仅 tcp/udp/http/https 可见',
  },
  {
    name: 'hc_type',
    label: '检查类型',
    group: 'health_check',
    input: 'select',
    required: false,
    options: [
      { label: 'TCP 端口连通', value: 'tcp' },
      { label: 'HTTP 请求', value: 'http' },
    ],
    magic: "缺省 'tcp'",
  },
  {
    name: 'hc_interval',
    label: '间隔(秒)',
    group: 'health_check',
    input: 'number',
    required: false,
    min: 1,
    precision: 0,
    magic: '缺省 10',
  },
  {
    name: 'hc_timeout',
    label: '超时(秒)',
    group: 'health_check',
    input: 'number',
    required: false,
    min: 1,
    max: 60,
    precision: 0,
    magic: '缺省 3',
  },
  {
    name: 'hc_max_failed',
    label: '连续失败阈值',
    group: 'health_check',
    input: 'number',
    required: false,
    min: 1,
    precision: 0,
    magic: '缺省 3',
  },
  {
    name: 'hc_path',
    label: 'HTTP 路径',
    group: 'health_check',
    input: 'text',
    required: false,
    placeholder: '/healthz(仅 http 检查)',
    visibleWhenHcType: 'http',
    magic: '仅 hc_type=http 且非空时写入 health_check.path',
  },
];

const HTTP_ADVANCED_FIELDS: FieldRule[] = [
  {
    name: 'locations',
    label: '路由路径',
    group: 'http_advanced',
    input: 'text',
    required: false,
    placeholder: '/api,/static',
    magic: '逗号分隔;空 = 匹配全部路径(提交为空数组)',
  },
  {
    name: 'host_rewrite',
    label: 'Host 改写',
    group: 'http_advanced',
    input: 'text',
    required: false,
    placeholder: '留空不改写',
    magic: '空串原样写入 spec.http.host_rewrite',
  },
  {
    name: 'http_user',
    label: 'Basic Auth 用户',
    group: 'http_advanced',
    input: 'text',
    required: false,
    placeholder: '留空不启用',
  },
  {
    name: 'http_password',
    label: 'Basic Auth 密码',
    group: 'http_advanced',
    input: 'text',
    required: false,
    placeholder: '与用户名成对填写',
  },
  {
    name: 'req_headers',
    label: '请求头改写',
    group: 'http_advanced',
    input: 'header-pairs',
    required: false,
    magic: '提交为 { set: [...] },仅过滤 name 为空的行(保留 value 为空的行)',
  },
  {
    name: 'resp_headers',
    label: '响应头改写',
    group: 'http_advanced',
    input: 'header-pairs',
    required: false,
    magic: '提交为 { set: [...] },仅过滤 name 为空的行(保留 value 为空的行)',
  },
  {
    name: 'route_by_http_user',
    label: '按用户路由',
    group: 'http_advanced',
    input: 'text',
    required: false,
    placeholder: '留空不启用',
  },
];

const DOMAIN_ENHANCE_FIELDS: FieldRule[] = [
  {
    name: 'not_found_html',
    label: '自定义 404 页',
    group: 'domain_enhance',
    input: 'textarea',
    required: false,
    placeholder: 'HTML 内容,请求未命中 locations 时返回(≤8KiB)',
    magic: 'trim 后为空则省略 not_found_html 键;写入的是未 trim 的原值',
  },
  {
    name: 'cache_enabled',
    label: '静态缓存',
    group: 'domain_enhance',
    input: 'checkbox',
    required: false,
    magic: '未勾选时不写 cache;编辑态不勾选不会删除已存在的 cache(旧版语义,见报告)',
  },
  {
    name: 'cache_ttl',
    label: 'TTL(秒)',
    group: 'domain_enhance',
    input: 'number',
    required: false,
    min: 10,
    max: 86400,
    precision: 0,
    magic: '缺省 300',
  },
  {
    name: 'cache_max_size_mb',
    label: '缓存容量(MB)',
    group: 'domain_enhance',
    input: 'number',
    required: false,
    min: 1,
    max: 256,
    precision: 0,
    magic: '缺省 16',
  },
  {
    name: 'cache_extensions',
    label: '缓存扩展名',
    group: 'domain_enhance',
    input: 'text',
    required: false,
    placeholder: 'js,css,png',
    magic: "缺省回退 'js,css'(表单默认值是一长串静态资源扩展名)",
  },
  {
    name: 'cache_max_item_size_kb',
    label: '单条上限(KB)',
    group: 'domain_enhance',
    input: 'number',
    required: false,
    min: 1,
    precision: 0,
    magic: '缺省 1024',
  },
];

/** 单个 kind 的能力布尔(旧版组件内联推导的等价物)。 */
export interface KindTraits {
  /** tcp/udp:有公网监听(bind_ip/bind_port)。 */
  usesBind: boolean;
  /** http/https/tcpmux:有域名区。 */
  usesDomain: boolean;
  /** 是否显示 subdomain(tcpmux 只填 domains)。 */
  showSubdomain: boolean;
  /** 是否有负载均衡组(旧版:usesBind || http || https)。 */
  supportsGroup: boolean;
  /** 是否有健康检查(旧版:恒等于 supportsGroup)。 */
  supportsHealthCheck: boolean;
  /** 是否支持插件(9 种插件对所有 kind 开放)。 */
  supportsPlugin: boolean;
  /** 是否显示 pool_size(仅 tcp)。 */
  supportsPoolSize: boolean;
  /** 是否显示 proxy_proto(仅 tcp/http)。 */
  supportsProxyProto: boolean;
  /** 是否显示 HTTP 高级 + 域名增强(仅 http)。 */
  supportsHttpExtras: boolean;
  /** spec 中该 kind 的段落键(tcp/udp 为 null)。 */
  section: KindSectionKey | null;
}

/** 单 kind 的完整矩阵。 */
export interface KindMatrix extends KindTraits {
  label: string;
  /**
   * 该 kind 可见的字段(不含插件子表单字段 —— 那由 `PLUGIN_FIELDS` 按 plugin_kind 决定)。
   * `hiddenWhenPlugin` 标注 target 区在插件启用时的隐藏。
   */
  fields: readonly FieldRule[];
}

/**
 * 由 traits 组装字段清单,保证"能力布尔"与"可见字段"不会互相漂移。
 * 顺序与旧版 Modal 的渲染顺序一致。
 */
function buildFields(traits: KindTraits): FieldRule[] {
  const fields: FieldRule[] = [...CONNECTION_FIELDS];
  if (traits.usesBind) fields.push(...BIND_FIELDS);
  fields.push(...TARGET_FIELDS);
  if (traits.usesDomain) {
    fields.push(...DOMAIN_FIELDS.filter((f) => f.name !== 'subdomain' || traits.showSubdomain));
  }
  fields.push(...BEHAVIOR_FIELDS, PLUGIN_SELECTOR_FIELD, ...TRANSPORT_FIELDS);
  if (traits.supportsPoolSize) fields.push(POOL_SIZE_FIELD);
  if (traits.supportsProxyProto) fields.push(PROXY_PROTO_FIELD);
  if (traits.supportsGroup) fields.push(...GROUP_FIELDS);
  if (traits.supportsHealthCheck) fields.push(...HEALTH_CHECK_FIELDS);
  if (traits.supportsHttpExtras) fields.push(...HTTP_ADVANCED_FIELDS, ...DOMAIN_ENHANCE_FIELDS);
  return fields;
}

function defineKind(label: string, traits: KindTraits): KindMatrix {
  return { ...traits, label, fields: buildFields(traits) };
}

/** kind → 字段可见性矩阵(8 个 kind 全量)。 */
export const KIND_MATRIX: Record<ProxyKind, KindMatrix> = {
  tcp: defineKind('TCP', {
    usesBind: true,
    usesDomain: false,
    showSubdomain: false,
    supportsGroup: true,
    supportsHealthCheck: true,
    supportsPlugin: true,
    supportsPoolSize: true,
    supportsProxyProto: true,
    supportsHttpExtras: false,
    section: null,
  }),
  udp: defineKind('UDP', {
    usesBind: true,
    usesDomain: false,
    showSubdomain: false,
    supportsGroup: true,
    supportsHealthCheck: true,
    supportsPlugin: true,
    supportsPoolSize: false,
    supportsProxyProto: false,
    supportsHttpExtras: false,
    section: null,
  }),
  http: defineKind('HTTP', {
    usesBind: false,
    usesDomain: true,
    showSubdomain: true,
    supportsGroup: true,
    supportsHealthCheck: true,
    supportsPlugin: true,
    supportsPoolSize: false,
    supportsProxyProto: true,
    supportsHttpExtras: true,
    section: 'http',
  }),
  https: defineKind('HTTPS', {
    usesBind: false,
    usesDomain: true,
    showSubdomain: true,
    supportsGroup: true,
    supportsHealthCheck: true,
    supportsPlugin: true,
    supportsPoolSize: false,
    supportsProxyProto: false,
    supportsHttpExtras: false,
    section: 'https',
  }),
  tcpmux: defineKind('TCPMux', {
    usesBind: false,
    usesDomain: true,
    showSubdomain: false,
    supportsGroup: false,
    supportsHealthCheck: false,
    supportsPlugin: true,
    supportsPoolSize: false,
    supportsProxyProto: false,
    supportsHttpExtras: false,
    section: 'tcpmux',
  }),
  stcp: defineKind('STCP', {
    usesBind: false,
    usesDomain: false,
    showSubdomain: false,
    supportsGroup: false,
    supportsHealthCheck: false,
    supportsPlugin: true,
    supportsPoolSize: false,
    supportsProxyProto: false,
    supportsHttpExtras: false,
    section: 'stcp',
  }),
  sudp: defineKind('SUDP', {
    usesBind: false,
    usesDomain: false,
    showSubdomain: false,
    supportsGroup: false,
    supportsHealthCheck: false,
    supportsPlugin: true,
    supportsPoolSize: false,
    supportsProxyProto: false,
    supportsHttpExtras: false,
    section: 'sudp',
  }),
  xtcp: defineKind('XTCP', {
    usesBind: false,
    usesDomain: false,
    showSubdomain: false,
    supportsGroup: false,
    supportsHealthCheck: false,
    supportsPlugin: true,
    supportsPoolSize: false,
    supportsProxyProto: false,
    supportsHttpExtras: false,
    section: 'xtcp',
  }),
};

/**
 * 未知 kind 的兜底 traits(旧版对未知 kind 只会把所有布尔判为 false,不抛错)。
 * 契约内 kind 恒为 8 种之一,这里只是保持旧版的宽松行为。
 */
const UNKNOWN_KIND_TRAITS: KindTraits = {
  usesBind: false,
  usesDomain: false,
  showSubdomain: false,
  supportsGroup: false,
  supportsHealthCheck: false,
  supportsPlugin: true,
  supportsPoolSize: false,
  supportsProxyProto: false,
  supportsHttpExtras: false,
  section: null,
};

/** 取某 kind 的能力布尔(未知 kind 走兜底)。 */
export function kindTraits(kind: ProxyKind): KindTraits {
  return KIND_MATRIX[kind] ?? UNKNOWN_KIND_TRAITS;
}

/** 某 kind 下按出现顺序去重的字段分组(供 UI 分节渲染)。 */
export function fieldGroupsOf(kind: ProxyKind): FieldGroupKey[] {
  const seen = new Set<FieldGroupKey>();
  const order: FieldGroupKey[] = [];
  for (const field of KIND_MATRIX[kind].fields) {
    if (!seen.has(field.group)) {
      seen.add(field.group);
      order.push(field.group);
    }
  }
  return order;
}

/** 插件子表单字段类型:文本 / HTTP 头键值对(多值)。 */
export type PluginFieldType = 'text' | 'pairs';

/** 插件子表单字段定义(旧版 `PluginFields` 的声明式等价物)。 */
export interface PluginFieldDef {
  /** 表单字段名(扁平 plugin_* 命名)。 */
  name: PluginFormField;
  /** 提交到 spec.plugin 的键名(去掉 `plugin_` 前缀)。 */
  specKey: string;
  label: string;
  type: PluginFieldType;
  required: boolean;
  /** 新建表单默认值(旧版 initialValuesOf 中全部为 '' 或 [])。 */
  defaultValue: string | HeaderPairValue[];
  /** 是否多值/键值对(type === 'pairs' 时恒为 true)。 */
  multi: boolean;
  /**
   * 文本字段值为 undefined 时的回退方式,逐字段与旧版等价:
   * - 'raw':旧版直接赋值(必填字段,如 `p.unix_path = v.plugin_unix_path`);
   * - 'empty-string':旧版回退空串(如 `p.http_user = v.plugin_http_user ?? ''`)。
   */
  emptyFallback: 'raw' | 'empty-string';
  placeholder?: string;
}

function pluginText(
  name: PluginFormField,
  specKey: string,
  label: string,
  required: boolean,
  placeholder?: string,
): PluginFieldDef {
  return {
    name,
    specKey,
    label,
    type: 'text',
    required,
    defaultValue: '',
    multi: false,
    // 旧版必填文本字段直接赋值、可选文本字段回退空串(逐字段核对过)
    emptyFallback: required ? 'raw' : 'empty-string',
    placeholder,
  };
}

function pluginPairs(name: PluginFormField, specKey: string, label: string): PluginFieldDef {
  return {
    name,
    specKey,
    label,
    type: 'pairs',
    required: false,
    defaultValue: [],
    multi: true,
    emptyFallback: 'empty-string',
  };
}

/** https2http / https2https 共用同一组字段。 */
const HTTPS_TLS_PLUGIN_FIELDS: readonly PluginFieldDef[] = [
  pluginText('plugin_local_addr', 'local_addr', '本地地址', true, '127.0.0.1:8080'),
  pluginText('plugin_crt_path', 'crt_path', '证书路径', true, '/path/tls.crt'),
  pluginText('plugin_key_path', 'key_path', '私钥路径', true, '/path/tls.key'),
  pluginText('plugin_host_rewrite', 'host_rewrite', 'Host 改写', false),
  pluginPairs('plugin_request_headers', 'request_headers', '请求头改写'),
];

/** http2https / http2http 共用同一组字段。 */
const HTTP2_PLUGIN_FIELDS: readonly PluginFieldDef[] = [
  pluginText('plugin_local_addr', 'local_addr', '本地地址', true, '127.0.0.1:8080'),
  pluginText('plugin_host_rewrite', 'host_rewrite', 'Host 改写', false),
  pluginPairs('plugin_request_headers', 'request_headers', '请求头改写'),
];

/**
 * 9 种插件的子表单字段定义(顺序与旧版 `PluginFields` 的 switch 分支一致)。
 *
 * 密钥相关字段(`secret_key`)与白名单(`allow_users`)按 R24/R32 定稿不在此列:
 * secret_key 由服务端生成与轮换、表单零输入,allow_users 已删除。
 */
export const PLUGIN_FIELDS: Record<PluginName, readonly PluginFieldDef[]> = {
  unix_domain_socket: [
    pluginText('plugin_unix_path', 'unix_path', 'Unix socket 路径', true, '/var/run/app.sock'),
  ],
  http_proxy: [
    pluginText('plugin_http_user', 'http_user', '代理用户名', false),
    pluginText('plugin_http_password', 'http_password', '代理密码', false),
  ],
  socks5: [
    pluginText('plugin_user', 'user', '用户名', false),
    pluginText('plugin_password', 'password', '密码', false),
  ],
  static_file: [
    pluginText('plugin_local_path', 'local_path', '本地目录', true, '/var/www'),
    pluginText('plugin_strip_prefix', 'strip_prefix', '剥离前缀', false, '/static'),
    pluginText('plugin_http_user', 'http_user', 'Basic Auth 用户', false),
    pluginText('plugin_http_password', 'http_password', 'Basic Auth 密码', false),
  ],
  https2http: HTTPS_TLS_PLUGIN_FIELDS,
  https2https: HTTPS_TLS_PLUGIN_FIELDS,
  http2https: HTTP2_PLUGIN_FIELDS,
  http2http: HTTP2_PLUGIN_FIELDS,
  tls2raw: [
    pluginText('plugin_local_addr', 'local_addr', '上游地址', true, '127.0.0.1:9000'),
    pluginText('plugin_crt_path', 'crt_path', '证书路径', true, '/path/tls.crt'),
    pluginText('plugin_key_path', 'key_path', '私钥路径', true, '/path/tls.key'),
  ],
};
