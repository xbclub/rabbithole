/**
 * 表单值 ⇄ 后端 `ProxySpec` 的纯转换逻辑。
 *
 * 从旧版 `components/ProxyFormModal.tsx` 平移(剥离 React / antd / DOM),后端契约零变更:
 * 键名、嵌套层级、默认值处理与省略规则逐字段与旧实现等价。所有函数均为纯函数。
 *
 * 密钥相关(R24/R32 定稿):
 * - stcp/sudp/xtcp 的 `secret_key` 完全由服务端生成与轮换,表单零输入 —— 编辑时以原 spec
 *   深拷贝为底本,因此服务端下发的 secret_key 原样保留,既不被清空也不被覆盖;
 * - `allow_users` 已删除,规则侧无白名单字段,不进表单值、不进载荷。
 */
import { KIND_SECTION, KIND_SECTION_KEYS, PLUGIN_FIELDS, kindTraits } from './matrix';
import type { KindSectionKey } from './matrix';
import type {
  BandwidthMode,
  FormFieldError,
  HeaderPairValue,
  HealthCheckType,
  PluginKindOption,
  PluginName,
  ProxyFormValues,
  ProxyKind,
  ProxyProtoVersion,
  ServiceRowLike,
  ServiceSpecPayload,
} from './types';

/** spec 的宽松读取视图(旧版用 `Record<string, any>`,这里保持宽松但不用 any)。 */
type SpecObject = Record<string, unknown>;

/**
 * 取值助手:仅在 null/undefined 时回退,与旧版 `??` 语义完全一致
 * (不做类型校验、不做数字强转 —— 旧版也没有)。
 */
function pick<T>(value: unknown, fallback: T): T {
  return (value ?? fallback) as T;
}

/** 旧版 pairsOf:读取 `HeaderOp` 结构 `{ set: [...] }` 并过滤掉 name 为空的行。 */
export function headerPairsOf(v: unknown): HeaderPairValue[] {
  const set = (v as { set?: HeaderPairValue[] } | undefined)?.set;
  return Array.isArray(set) ? set.filter((p) => p && p.name) : [];
}

/** 旧版 csv:数组 → 逗号串;非数组一律返回空串。 */
export function csvToList(v: unknown): string {
  return Array.isArray(v) ? (v as string[]).join(',') : '';
}

/** 逗号串 → 去空白、去空的字符串数组(旧版 locations / cache_extensions 的处理)。 */
function splitCsv(input: string | null | undefined): string[] {
  return (input ?? '')
    .split(',')
    .map((x) => x.trim())
    .filter(Boolean);
}

/** 旧版提交时对头键值对的处理:仅过滤 name 为空的行,不做拷贝、不校验 value。 */
function filterHeaderPairs(pairs: HeaderPairValue[] | null | undefined): HeaderPairValue[] {
  return (pairs ?? []).filter((r) => r.name);
}

/** 域名语法校验结果。 */
export interface DomainParseResult {
  error?: string;
  list: string[];
}

/**
 * 域名解析/校验(与旧版一致):逗号分隔、trim、小写化,最左标签单个通配(如 *.example.com)。
 * 任一项非法即整体返回 error 且 list 为空。
 */
export function parseDomains(input: string): DomainParseResult {
  const list = input
    .split(',')
    .map((s) => s.trim().toLowerCase())
    .filter(Boolean);
  for (const d of list) {
    if (d === '*') return { error: '通配仅支持最左标签(如 *.example.com)', list: [] };
    if (d.startsWith('*.') && d.includes('*', 2)) {
      return { error: `仅支持单个通配标签:${d}`, list: [] };
    }
    if (d.includes('*') && !d.startsWith('*.')) return { error: `通配符位置非法:${d}`, list: [] };
    if (!/^(\*\.)?([a-z0-9-]+\.)*[a-z0-9-]+$/.test(d))
      return { error: `域名格式非法:${d}`, list: [] };
  }
  return { list };
}

/** 新建表单的整表默认值(旧版 `initialValuesOf` 的无 editing 分支)。 */
export function defaultProxyFormValues(defaultKind: ProxyKind, clientId = ''): ProxyFormValues {
  return {
    client_id: clientId,
    name: '',
    kind: defaultKind,
    enabled: true,
    bind_ip: '0.0.0.0',
    bind_auto: true,
    bind_port: null,
    target_host: '127.0.0.1',
    target_port: 80,
    custom_domains: '',
    subdomain: '',
    use_compression: false,
    pool_size: 0,
    bandwidth_limit: '',
    bandwidth_mode: 'server',
    proxy_proto: 'none',
    group: '',
    hc_enabled: false,
    hc_type: 'tcp',
    hc_interval: 10,
    hc_timeout: 3,
    hc_max_failed: 3,
    hc_path: '',
    locations: '',
    host_rewrite: '',
    http_user: '',
    http_password: '',
    route_by_http_user: '',
    req_headers: [],
    resp_headers: [],
    not_found_html: '',
    cache_enabled: false,
    cache_ttl: 300,
    cache_max_size_mb: 16,
    cache_extensions: 'js,css,png,jpg,jpeg,gif,svg,webp,ico,woff,woff2,ttf,html',
    cache_max_item_size_kb: 1024,
    plugin_kind: 'none',
    plugin_unix_path: '',
    plugin_http_user: '',
    plugin_http_password: '',
    plugin_user: '',
    plugin_password: '',
    plugin_local_path: '',
    plugin_strip_prefix: '',
    plugin_local_addr: '',
    plugin_crt_path: '',
    plugin_key_path: '',
    plugin_host_rewrite: '',
    plugin_request_headers: [],
  };
}

/**
 * 编辑回读:ServiceRow → 表单值(旧版 `initialValuesOf` 的 editing 分支)。
 *
 * 旧版从 `spec` 反解嵌套字段(段落键由 kind 决定、health_check/cache/plugin 子对象),
 * `custom_domains` / `locations` / `cache.extensions` 由数组拼回逗号串。
 * 编辑态恒 `bind_auto = false`(旧版:编辑禁用自动分配)。
 */
export function serviceToFormValues(row: ServiceRowLike): ProxyFormValues {
  const s: SpecObject = row.spec ?? {};
  const sectionKey = KIND_SECTION[row.kind as ProxyKind];
  // 旧版对 section 全程用可选链(`section?.xxx`),段落为 null 时不抛错 —— 这里保持等价
  const section = (sectionKey ? s[sectionKey] : {}) as SpecObject | null | undefined;
  const transport = (s.transport ?? {}) as SpecObject;
  const hc = s.health_check as SpecObject | undefined;
  const cache = section?.cache as SpecObject | undefined;
  const plugin = s.plugin as SpecObject | undefined;
  return {
    client_id: row.client_id,
    name: row.name,
    kind: row.kind as ProxyKind,
    enabled: row.enabled,
    bind_ip: row.bind_ip || '0.0.0.0',
    bind_auto: false,
    bind_port: row.bind_port,
    target_host: row.target_host,
    target_port: row.target_port,
    custom_domains: csvToList(section?.custom_domains),
    subdomain: pick(section?.subdomain, ''),
    use_compression: !!transport.use_compression,
    pool_size: pick(s.pool_size, 0),
    bandwidth_limit: pick(transport.bandwidth_limit, ''),
    bandwidth_mode: pick<BandwidthMode>(transport.bandwidth_mode, 'server'),
    proxy_proto: pick<ProxyProtoVersion>(transport.proxy_proto, 'none'),
    group: pick(s.group, ''),
    hc_enabled: !!hc,
    hc_type: pick<HealthCheckType>(hc?.type, 'tcp'),
    hc_interval: pick(hc?.interval_seconds, 10),
    hc_timeout: pick(hc?.timeout_seconds, 3),
    hc_max_failed: pick(hc?.max_failed, 3),
    hc_path: pick(hc?.path, ''),
    locations: csvToList(section?.locations),
    host_rewrite: pick(section?.host_rewrite, ''),
    http_user: pick(section?.http_user, ''),
    http_password: pick(section?.http_password, ''),
    route_by_http_user: pick(section?.route_by_http_user, ''),
    req_headers: headerPairsOf(section?.request_headers),
    resp_headers: headerPairsOf(section?.response_headers),
    not_found_html: pick(section?.not_found_html, ''),
    cache_enabled: !!cache,
    cache_ttl: pick(cache?.ttl_seconds, 300),
    cache_max_size_mb: pick(cache?.max_size_mb, 16),
    cache_extensions: csvToList(cache?.extensions),
    cache_max_item_size_kb: pick(cache?.max_item_size_kb, 1024),
    plugin_kind: pick<PluginKindOption>(plugin?.kind, 'none'),
    plugin_unix_path: pick(plugin?.unix_path, ''),
    plugin_http_user: pick(plugin?.http_user, ''),
    plugin_http_password: pick(plugin?.http_password, ''),
    plugin_user: pick(plugin?.user, ''),
    plugin_password: pick(plugin?.password, ''),
    plugin_local_path: pick(plugin?.local_path, ''),
    plugin_strip_prefix: pick(plugin?.strip_prefix, ''),
    plugin_local_addr: pick(plugin?.local_addr, ''),
    plugin_crt_path: pick(plugin?.crt_path, ''),
    plugin_key_path: pick(plugin?.key_path, ''),
    plugin_host_rewrite: pick(plugin?.host_rewrite, ''),
    plugin_request_headers: headerPairsOf(plugin?.request_headers),
  };
}

/** `buildServiceSpec` 的可选项。 */
export interface BuildServiceSpecOptions {
  /** 编辑目标(旧版 `editing`):非空时以原 spec 深拷贝为底本,并禁用"自动分配"。 */
  editing?: ServiceRowLike | null;
}

/**
 * 由插件字段定义组装 `spec.plugin`(等价于旧版 handleFinish 里的 switch)。
 * 键名、顺序与空值回退逐字段一致;`request_headers` 恒为 `{ set: [...] }`。
 */
function buildPluginSpec(pluginKind: PluginName, values: ProxyFormValues): SpecObject {
  const p: SpecObject = { kind: pluginKind };
  for (const def of PLUGIN_FIELDS[pluginKind]) {
    if (def.type === 'pairs') {
      p[def.specKey] = { set: filterHeaderPairs(values[def.name] as HeaderPairValue[]) };
    } else if (def.emptyFallback === 'raw') {
      // 必填文本字段:旧版直接赋值(undefined 也原样写入)
      p[def.specKey] = values[def.name];
    } else {
      p[def.specKey] = values[def.name] ?? '';
    }
  }
  return p;
}

/**
 * 组装当前 kind 的 spec 段落(等价于旧版 handleFinish 末尾的 6 段分支)。
 * http 以原段落为底本展开,只覆盖表单管理的字段。
 */
function applyKindSection(
  spec: SpecObject,
  sectionKey: KindSectionKey,
  values: ProxyFormValues,
  domains: string[],
): void {
  if (sectionKey === 'http') {
    const http: SpecObject = {
      ...((spec.http ?? {}) as SpecObject),
      custom_domains: domains,
      subdomain: values.subdomain?.trim() ?? '',
      locations: splitCsv(values.locations),
      host_rewrite: values.host_rewrite ?? '',
      http_user: values.http_user ?? '',
      http_password: values.http_password ?? '',
      route_by_http_user: values.route_by_http_user ?? '',
      request_headers: { set: filterHeaderPairs(values.req_headers) },
      response_headers: { set: filterHeaderPairs(values.resp_headers) },
    };
    // M2 增强字段(后端落地前 serde 忽略,属预期降级)
    if (values.not_found_html?.trim()) http.not_found_html = values.not_found_html;
    if (values.cache_enabled) {
      http.cache = {
        ttl_seconds: values.cache_ttl ?? 300,
        max_size_mb: values.cache_max_size_mb ?? 16,
        extensions: splitCsv(values.cache_extensions ?? 'js,css'),
        max_item_size_kb: values.cache_max_item_size_kb ?? 1024,
      };
    }
    spec.http = http;
    return;
  }
  if (sectionKey === 'https') {
    spec.https = {
      ...((spec.https ?? {}) as SpecObject),
      custom_domains: domains,
      subdomain: values.subdomain?.trim() ?? '',
    };
    return;
  }
  if (sectionKey === 'tcpmux') {
    spec.tcpmux = { ...((spec.tcpmux ?? {}) as SpecObject), custom_domains: domains };
    return;
  }
  // secret_key 服务端管理:创建时生成、编辑时保留原值,表单不触碰;
  // 访问授权由访问器记录本身保证,规则侧无白名单字段(R32 已删除 allow_users)
  spec[sectionKey] = spec[sectionKey] ?? {};
}

/**
 * 表单值 → 提交载荷(旧版 handleFinish 的组装部分,剔除 UI 侧的 setFields 报错)。
 *
 * 调用前应先跑 `validateServiceForm`;旧版在校验失败时不会走到组装,所以这里不重复校验。
 *
 * @param values 表单值
 * @param opts   `editing` 非空表示编辑:以原 spec 深拷贝为底本,仅覆盖表单管理的字段
 */
export function buildServiceSpec(
  values: ProxyFormValues,
  opts: BuildServiceSpecOptions = {},
): ServiceSpecPayload {
  const editing = opts.editing ?? null;
  const kind = values.kind;
  const traits = kindTraits(kind);
  const pluginOn = values.plugin_kind !== 'none';
  const domains = parseDomains(values.custom_domains ?? '').list;

  // 编辑:以原 spec 深拷贝为底本(JSON 往返,与旧版一致,可丢弃 undefined 值)
  const spec: SpecObject = editing
    ? (JSON.parse(JSON.stringify(editing.spec ?? {})) as SpecObject)
    : {};

  spec.client_id = values.client_id;
  spec.name = values.name;
  spec.kind = kind;
  spec.enabled = values.enabled;

  if (!pluginOn) {
    spec.target_host = values.target_host;
    spec.target_port = values.target_port;
  } else {
    // 插件接管目标:后端语义如此,旧版显式删除两个键
    delete spec.target_host;
    delete spec.target_port;
  }

  if (traits.usesBind) {
    spec.bind_ip = values.bind_ip;
    // "自动分配"开关替代魔法值 0(§6.5);编辑禁用自动分配
    spec.bind_port = !editing && values.bind_auto ? 0 : (values.bind_port ?? 0);
  }
  if (kind === 'tcp') spec.pool_size = values.pool_size ?? 0;

  const transport: SpecObject = {
    ...((spec.transport ?? {}) as SpecObject),
    use_compression: !!values.use_compression,
    bandwidth_mode: values.bandwidth_mode ?? 'server',
    proxy_proto: values.proxy_proto ?? 'none',
  };
  if (values.bandwidth_limit?.trim()) transport.bandwidth_limit = values.bandwidth_limit.trim();
  else delete transport.bandwidth_limit;
  spec.transport = transport;

  if (traits.supportsGroup && values.group?.trim()) spec.group = values.group.trim();
  else delete spec.group;

  if (values.hc_enabled && traits.supportsHealthCheck) {
    spec.health_check = {
      type: values.hc_type,
      interval_seconds: values.hc_interval ?? 10,
      timeout_seconds: values.hc_timeout ?? 3,
      max_failed: values.hc_max_failed ?? 3,
      ...(values.hc_type === 'http' && values.hc_path ? { path: values.hc_path } : {}),
    };
  } else {
    delete spec.health_check;
  }

  if (pluginOn) {
    spec.plugin = buildPluginSpec(values.plugin_kind as PluginName, values);
  } else {
    delete spec.plugin;
  }

  for (const k of KIND_SECTION_KEYS) {
    if (k === kind) applyKindSection(spec, k, values, domains);
    else delete spec[k];
  }

  return spec;
}

/** `validateServiceForm` 的可选项。 */
export interface ValidateServiceFormOptions {
  /** 编辑目标:非空时不再要求"新建态"的监听端口必填。 */
  editing?: ServiceRowLike | null;
}

/**
 * 表单校验(旧版 handleFinish 开头的四段校验,剔除 `form.setFields` 的 UI 调用)。
 *
 * 语义与旧版一致:
 * - 域名解析出错时**只**返回该错误(旧版直接 return,不再叠加后续必填提示);
 * - http/https 需自定义域名或子域名;tcpmux 必须有自定义域名;
 * - 新建的 tcp/udp 未勾"自动分配"且端口为空时报错(编辑态不校验)。
 *
 * 返回空数组表示可以组装提交。
 */
export function validateServiceForm(
  values: ProxyFormValues,
  opts: ValidateServiceFormOptions = {},
): FormFieldError[] {
  const editing = opts.editing ?? null;
  const kind = values.kind;
  const traits = kindTraits(kind);
  const domains = parseDomains(values.custom_domains ?? '');
  if (traits.usesDomain && domains.error) {
    return [{ field: 'custom_domains', message: domains.error }];
  }
  const errors: FormFieldError[] = [];
  if (
    (kind === 'http' || kind === 'https') &&
    domains.list.length === 0 &&
    !values.subdomain?.trim()
  ) {
    errors.push({
      field: 'custom_domains',
      message:
        kind === 'http'
          ? 'HTTP 规则需填写自定义域名或子域名'
          : 'HTTPS 规则需填写自定义域名或子域名',
    });
  }
  if (kind === 'tcpmux' && domains.list.length === 0) {
    errors.push({ field: 'custom_domains', message: 'TCPMux 规则需填写自定义域名' });
  }
  if (traits.usesBind && !editing && !values.bind_auto && !values.bind_port) {
    errors.push({ field: 'bind_port', message: '请填写监听端口,或改用自动分配' });
  }
  return errors;
}
