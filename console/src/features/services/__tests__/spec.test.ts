/**
 * `ProxyFormModal` 纯逻辑平移的等价性单测。
 *
 * 覆盖:8 个 kind 的「表单值 → 载荷」、9 种插件的「插件字段 → 载荷」、
 * ServiceRow → 表单值 → 载荷 往返一致性、以及 bind_port=0 / 空值省略 / 数值透传等边界。
 * 期望值全部写死具体值,后端 ProxySpec 形状一旦漂移即失败。
 */
import { describe, expect, it } from 'vitest';
import { KIND_MATRIX, PLUGIN_FIELDS, PLUGIN_NAMES, fieldGroupsOf, kindTraits } from '../matrix';
import {
  buildServiceSpec,
  defaultProxyFormValues,
  parseDomains,
  serviceToFormValues,
  validateServiceForm,
} from '../spec';
import type { PluginName, ProxyFormValues, ProxyKind, ServiceRowLike } from '../types';

/** 造一份新建表单值(默认 tcp + 客户端 client-1)。 */
function makeValues(overrides: Partial<ProxyFormValues> = {}): ProxyFormValues {
  return { ...defaultProxyFormValues('tcp', 'client-1'), ...overrides };
}

/** 造一行 ServiceRow(只含编辑回读需要的字段)。 */
function makeRow(overrides: Partial<ServiceRowLike> = {}): ServiceRowLike {
  return {
    id: 1,
    client_id: 'client-1',
    name: 'svc',
    kind: 'tcp',
    enabled: true,
    bind_ip: '0.0.0.0',
    bind_port: 0,
    target_host: '127.0.0.1',
    target_port: 80,
    spec: {},
    ...overrides,
  };
}

/** 插件字段 → spec 键名清单。 */
function specKeys(plugin: PluginName): string[] {
  return PLUGIN_FIELDS[plugin].map((f) => f.specKey);
}

describe('buildServiceSpec:每个 kind 的表单值 → 载荷', () => {
  it('tcp:监听/目标/传输/分组/健康检查落到顶层与嵌套键', () => {
    const values = makeValues({
      kind: 'tcp',
      name: 'tcp-svc',
      bind_ip: '10.0.0.1',
      bind_auto: false,
      bind_port: 8443,
      target_host: '10.1.1.1',
      target_port: 443,
      pool_size: 8,
      use_compression: true,
      bandwidth_limit: '1MB',
      bandwidth_mode: 'client',
      proxy_proto: 'v1',
      group: 'pool-a',
      hc_enabled: true,
      hc_type: 'http',
      hc_interval: 5,
      hc_timeout: 2,
      hc_max_failed: 4,
      hc_path: '/healthz',
    });
    expect(buildServiceSpec(values)).toEqual({
      client_id: 'client-1',
      name: 'tcp-svc',
      kind: 'tcp',
      enabled: true,
      target_host: '10.1.1.1',
      target_port: 443,
      bind_ip: '10.0.0.1',
      bind_port: 8443,
      pool_size: 8,
      transport: {
        use_compression: true,
        bandwidth_mode: 'client',
        proxy_proto: 'v1',
        bandwidth_limit: '1MB',
      },
      group: 'pool-a',
      health_check: {
        type: 'http',
        interval_seconds: 5,
        timeout_seconds: 2,
        max_failed: 4,
        path: '/healthz',
      },
    });
  });

  it('udp:有监听与分组,不写 pool_size;transport.proxy_proto 仍按缺省 none 写入', () => {
    const values = makeValues({
      kind: 'udp',
      bind_auto: false,
      bind_port: 5353,
      target_host: '10.2.2.2',
      target_port: 53,
      group: 'dns',
    });
    const spec = buildServiceSpec(values);
    expect(spec).toEqual({
      client_id: 'client-1',
      name: '',
      kind: 'udp',
      enabled: true,
      target_host: '10.2.2.2',
      target_port: 53,
      bind_ip: '0.0.0.0',
      bind_port: 5353,
      transport: { use_compression: false, bandwidth_mode: 'server', proxy_proto: 'none' },
      group: 'dns',
    });
    expect(spec).not.toHaveProperty('pool_size');
    expect(spec).not.toHaveProperty('health_check');
  });

  it('http:域名/locations 数组化、请求响应头 { set }、cache 与 404 页按开关写入', () => {
    const values = makeValues({
      kind: 'http',
      name: 'web',
      custom_domains: 'A.Example.com, *.Example.com ,',
      subdomain: ' app ',
      locations: '/api, /static ,',
      host_rewrite: 'backend.internal',
      http_user: 'u',
      http_password: 'p',
      route_by_http_user: 'alice',
      req_headers: [
        { name: 'X-Req', value: '1' },
        { name: '', value: 'drop' },
      ],
      resp_headers: [{ name: 'X-Resp', value: '2' }],
      not_found_html: '<h1>404</h1>',
      cache_enabled: true,
      cache_ttl: 600,
      cache_max_size_mb: 32,
      cache_extensions: 'js, css ,',
      cache_max_item_size_kb: 2048,
      group: 'web-pool',
      hc_enabled: true,
      hc_type: 'tcp',
    });
    const spec = buildServiceSpec(values);
    expect(spec.http).toEqual({
      custom_domains: ['a.example.com', '*.example.com'],
      subdomain: 'app',
      locations: ['/api', '/static'],
      host_rewrite: 'backend.internal',
      http_user: 'u',
      http_password: 'p',
      route_by_http_user: 'alice',
      request_headers: { set: [{ name: 'X-Req', value: '1' }] },
      response_headers: { set: [{ name: 'X-Resp', value: '2' }] },
      not_found_html: '<h1>404</h1>',
      cache: {
        ttl_seconds: 600,
        max_size_mb: 32,
        extensions: ['js', 'css'],
        max_item_size_kb: 2048,
      },
    });
    expect(spec.health_check).toEqual({
      type: 'tcp',
      interval_seconds: 10,
      timeout_seconds: 3,
      max_failed: 3,
    });
    expect(spec).not.toHaveProperty('https');
    expect(spec).not.toHaveProperty('plugin');
  });

  it('https:仅 https 段落,含 custom_domains 与 subdomain(空子域名也写入空串)', () => {
    const values = makeValues({
      kind: 'https',
      name: 'tls',
      custom_domains: 'secure.example.com',
      target_host: '10.3.3.3',
      target_port: 8443,
    });
    expect(buildServiceSpec(values)).toEqual({
      client_id: 'client-1',
      name: 'tls',
      kind: 'https',
      enabled: true,
      target_host: '10.3.3.3',
      target_port: 8443,
      transport: { use_compression: false, bandwidth_mode: 'server', proxy_proto: 'none' },
      https: { custom_domains: ['secure.example.com'], subdomain: '' },
    });
  });

  it('tcpmux:仅 custom_domains,无 subdomain/group/health_check 键', () => {
    const values = makeValues({ kind: 'tcpmux', name: 'mux', custom_domains: 'mux.example.com' });
    const spec = buildServiceSpec(values);
    expect(spec.tcpmux).toEqual({ custom_domains: ['mux.example.com'] });
    expect(spec.tcpmux as Record<string, unknown>).not.toHaveProperty('subdomain');
    expect(spec).not.toHaveProperty('group');
    expect(spec).not.toHaveProperty('health_check');
  });

  it.each(['stcp', 'sudp', 'xtcp'] as const)(
    '%s:段落仅占位空对象,secret_key / allow_users 不进表单载荷',
    (kind) => {
      const values = makeValues({
        kind,
        name: `${kind}-svc`,
        target_host: '10.4.4.4',
        target_port: 22,
      });
      const spec = buildServiceSpec(values);
      expect(spec[kind]).toEqual({});
      expect(spec).not.toHaveProperty('http');
      expect(spec).not.toHaveProperty('plugin');
      expect(JSON.stringify(spec)).not.toContain('secret_key');
      expect(JSON.stringify(spec)).not.toContain('allow_users');
    },
  );
});

describe('buildServiceSpec:9 种插件的插件字段 → 载荷', () => {
  it('unix_domain_socket:unix_path,并删除 target_host/target_port', () => {
    const spec = buildServiceSpec(
      makeValues({
        kind: 'tcp',
        plugin_kind: 'unix_domain_socket',
        plugin_unix_path: '/var/run/app.sock',
        target_host: '10.0.0.9',
        target_port: 9999,
      }),
    );
    expect(spec.plugin).toEqual({ kind: 'unix_domain_socket', unix_path: '/var/run/app.sock' });
    expect(spec).not.toHaveProperty('target_host');
    expect(spec).not.toHaveProperty('target_port');
  });

  it('http_proxy:http_user + http_password', () => {
    const spec = buildServiceSpec(
      makeValues({
        kind: 'tcp',
        plugin_kind: 'http_proxy',
        plugin_http_user: 'proxy-u',
        plugin_http_password: 'proxy-p',
      }),
    );
    expect(spec.plugin).toEqual({
      kind: 'http_proxy',
      http_user: 'proxy-u',
      http_password: 'proxy-p',
    });
  });

  it('socks5:user + password', () => {
    const spec = buildServiceSpec(
      makeValues({
        kind: 'tcp',
        plugin_kind: 'socks5',
        plugin_user: 'socks-u',
        plugin_password: 'socks-p',
      }),
    );
    expect(spec.plugin).toEqual({ kind: 'socks5', user: 'socks-u', password: 'socks-p' });
  });

  it('static_file:local_path + strip_prefix + Basic Auth', () => {
    const spec = buildServiceSpec(
      makeValues({
        kind: 'tcp',
        plugin_kind: 'static_file',
        plugin_local_path: '/var/www',
        plugin_strip_prefix: '/static',
        plugin_http_user: 'su',
        plugin_http_password: 'sp',
      }),
    );
    expect(spec.plugin).toEqual({
      kind: 'static_file',
      local_path: '/var/www',
      strip_prefix: '/static',
      http_user: 'su',
      http_password: 'sp',
    });
  });

  it('https2http:local_addr + 证书 + host_rewrite + request_headers(过滤空 name 行)', () => {
    const spec = buildServiceSpec(
      makeValues({
        kind: 'tcp',
        plugin_kind: 'https2http',
        plugin_local_addr: '127.0.0.1:8080',
        plugin_crt_path: '/path/tls.crt',
        plugin_key_path: '/path/tls.key',
        plugin_host_rewrite: 'example.com',
        plugin_request_headers: [
          { name: 'X-Fwd', value: '1' },
          { name: '', value: 'drop' },
        ],
      }),
    );
    expect(spec.plugin).toEqual({
      kind: 'https2http',
      local_addr: '127.0.0.1:8080',
      crt_path: '/path/tls.crt',
      key_path: '/path/tls.key',
      host_rewrite: 'example.com',
      request_headers: { set: [{ name: 'X-Fwd', value: '1' }] },
    });
  });

  it('https2https:字段与 https2http 相同,仅 kind 不同', () => {
    const spec = buildServiceSpec(
      makeValues({
        kind: 'tcp',
        plugin_kind: 'https2https',
        plugin_local_addr: '127.0.0.1:8443',
        plugin_crt_path: '/path/a.crt',
        plugin_key_path: '/path/a.key',
        plugin_host_rewrite: 'inner.example.com',
        plugin_request_headers: [{ name: 'X-Fwd', value: '1' }],
      }),
    );
    expect(spec.plugin).toEqual({
      kind: 'https2https',
      local_addr: '127.0.0.1:8443',
      crt_path: '/path/a.crt',
      key_path: '/path/a.key',
      host_rewrite: 'inner.example.com',
      request_headers: { set: [{ name: 'X-Fwd', value: '1' }] },
    });
  });

  it('http2https:无证书字段', () => {
    const spec = buildServiceSpec(
      makeValues({
        kind: 'tcp',
        plugin_kind: 'http2https',
        plugin_local_addr: '127.0.0.1:8080',
        plugin_host_rewrite: 'example.com',
        plugin_request_headers: [{ name: 'X-Fwd', value: '1' }],
      }),
    );
    expect(spec.plugin).toEqual({
      kind: 'http2https',
      local_addr: '127.0.0.1:8080',
      host_rewrite: 'example.com',
      request_headers: { set: [{ name: 'X-Fwd', value: '1' }] },
    });
    expect(spec.plugin as Record<string, unknown>).not.toHaveProperty('crt_path');
    expect(spec.plugin as Record<string, unknown>).not.toHaveProperty('key_path');
  });

  it('http2http:无证书字段', () => {
    const spec = buildServiceSpec(
      makeValues({
        kind: 'tcp',
        plugin_kind: 'http2http',
        plugin_local_addr: '127.0.0.1:8081',
        plugin_host_rewrite: 'example.com',
        plugin_request_headers: [],
      }),
    );
    expect(spec.plugin).toEqual({
      kind: 'http2http',
      local_addr: '127.0.0.1:8081',
      host_rewrite: 'example.com',
      request_headers: { set: [] },
    });
  });

  it('tls2raw:local_addr + 证书,无 host_rewrite/request_headers', () => {
    const spec = buildServiceSpec(
      makeValues({
        kind: 'tcp',
        plugin_kind: 'tls2raw',
        plugin_local_addr: '127.0.0.1:9000',
        plugin_crt_path: '/path/tls.crt',
        plugin_key_path: '/path/tls.key',
      }),
    );
    expect(spec.plugin).toEqual({
      kind: 'tls2raw',
      local_addr: '127.0.0.1:9000',
      crt_path: '/path/tls.crt',
      key_path: '/path/tls.key',
    });
    expect(spec.plugin as Record<string, unknown>).not.toHaveProperty('host_rewrite');
    expect(spec.plugin as Record<string, unknown>).not.toHaveProperty('request_headers');
  });

  it('可选插件字段留空 → 回退空串(与旧版 ?? "" 一致)', () => {
    const spec = buildServiceSpec(makeValues({ kind: 'tcp', plugin_kind: 'http_proxy' }));
    expect(spec.plugin).toEqual({ kind: 'http_proxy', http_user: '', http_password: '' });
  });

  it('必填插件字段为 undefined 时按旧版原样写入(raw),可选字段回退空串', () => {
    const rawValues = {
      ...makeValues({ kind: 'tcp', plugin_kind: 'unix_domain_socket' }),
      plugin_unix_path: undefined as unknown as string,
    };
    expect(buildServiceSpec(rawValues).plugin).toStrictEqual({
      kind: 'unix_domain_socket',
      unix_path: undefined,
    });
    const optionalValues = {
      ...makeValues({ kind: 'tcp', plugin_kind: 'socks5' }),
      plugin_user: undefined as unknown as string,
    };
    expect(buildServiceSpec(optionalValues).plugin).toStrictEqual({
      kind: 'socks5',
      user: '',
      password: '',
    });
  });
});

describe('ServiceRow → 表单值 → 载荷 往返一致性(编辑回读不丢字段)', () => {
  it('http 全字段往返:回读后重提交与首次载荷逐键相同', () => {
    const values = makeValues({
      kind: 'http',
      name: 'web',
      custom_domains: 'A.Example.com, *.Example.com',
      subdomain: 'app',
      locations: '/api,/static',
      host_rewrite: 'backend.internal',
      http_user: 'u',
      http_password: 'p',
      route_by_http_user: 'alice',
      req_headers: [{ name: 'X-Req', value: '1' }],
      resp_headers: [{ name: 'X-Resp', value: '2' }],
      not_found_html: '<h1>404</h1>',
      cache_enabled: true,
      cache_ttl: 600,
      cache_max_size_mb: 32,
      cache_extensions: 'js,css',
      cache_max_item_size_kb: 2048,
      group: 'web-pool',
      hc_enabled: true,
      hc_type: 'http',
      hc_interval: 5,
      hc_timeout: 2,
      hc_max_failed: 4,
      hc_path: '/healthz',
      bandwidth_limit: '2MB',
      use_compression: true,
      bandwidth_mode: 'client',
      proxy_proto: 'v2',
    });
    const payload = buildServiceSpec(values);
    const row = makeRow({
      kind: 'http',
      name: 'web',
      bind_port: 0,
      target_host: '127.0.0.1',
      target_port: 80,
      spec: payload,
    });
    const readback = serviceToFormValues(row);
    expect(readback.custom_domains).toBe('a.example.com,*.example.com');
    expect(readback.locations).toBe('/api,/static');
    expect(readback.req_headers).toEqual([{ name: 'X-Req', value: '1' }]);
    expect(buildServiceSpec(readback, { editing: row })).toEqual(payload);
  });

  it('tcp 往返:bind_ip/bind_port/pool_size 与 transport 均不丢', () => {
    const values = makeValues({
      kind: 'tcp',
      name: 'tcp-svc',
      bind_ip: '10.0.0.1',
      bind_auto: false,
      bind_port: 8443,
      target_host: '10.1.1.1',
      target_port: 443,
      pool_size: 8,
      use_compression: true,
      bandwidth_limit: '512KB',
      bandwidth_mode: 'client',
      proxy_proto: 'v2',
      group: 'pool-a',
    });
    const payload = buildServiceSpec(values);
    const row = makeRow({
      kind: 'tcp',
      name: 'tcp-svc',
      bind_ip: '10.0.0.1',
      bind_port: 8443,
      target_host: '10.1.1.1',
      target_port: 443,
      spec: payload,
    });
    const readback = serviceToFormValues(row);
    expect(readback.bind_auto).toBe(false);
    expect(readback.bind_port).toBe(8443);
    expect(readback.pool_size).toBe(8);
    expect(buildServiceSpec(readback, { editing: row })).toEqual(payload);
  });

  it('stcp 往返:服务端下发的 secret_key 原样保留,且不被表单覆盖', () => {
    const row = makeRow({
      kind: 'stcp',
      name: 'sec',
      target_host: '127.0.0.1',
      target_port: 22,
      spec: {
        client_id: 'client-1',
        name: 'sec',
        kind: 'stcp',
        enabled: true,
        target_host: '127.0.0.1',
        target_port: 22,
        transport: { use_compression: true, bandwidth_mode: 'client', proxy_proto: 'none' },
        stcp: { secret_key: 'SERVER_GENERATED_SECRET' },
      },
    });
    const readback = serviceToFormValues(row);
    expect(readback.plugin_kind).toBe('none');
    const payload = buildServiceSpec(readback, { editing: row });
    expect(payload.stcp).toEqual({ secret_key: 'SERVER_GENERATED_SECRET' });
    expect(payload).toMatchObject({
      client_id: 'client-1',
      name: 'sec',
      kind: 'stcp',
      enabled: true,
      target_host: '127.0.0.1',
      target_port: 22,
      transport: { use_compression: true, bandwidth_mode: 'client', proxy_proto: 'none' },
    });
    expect(payload).not.toHaveProperty('http');
    expect(payload).not.toHaveProperty('plugin');
  });

  it('编辑态 bind_port=0(服务端自动分配)原样保留', () => {
    const row = makeRow({ kind: 'tcp', name: 'auto', bind_port: 0, spec: {} });
    const payload = buildServiceSpec(serviceToFormValues(row), { editing: row });
    expect(payload.bind_port).toBe(0);
  });
});

describe('边界:0 = 自动分配、空值省略、数值透传', () => {
  it('新建勾选"自动分配" → bind_port 提交 0', () => {
    const spec = buildServiceSpec(makeValues({ kind: 'tcp', bind_auto: true, bind_port: null }));
    expect(spec.bind_port).toBe(0);
  });

  it('新建未勾自动分配但端口留空 → 仍按旧版回退 0,且校验会拦下', () => {
    const values = makeValues({ kind: 'tcp', bind_auto: false, bind_port: null });
    expect(buildServiceSpec(values).bind_port).toBe(0);
    expect(validateServiceForm(values)).toEqual([
      { field: 'bind_port', message: '请填写监听端口,或改用自动分配' },
    ]);
  });

  it('编辑态忽略 bind_auto,按真实端口提交', () => {
    const row = makeRow({ kind: 'tcp', bind_port: 12345, spec: {} });
    const values = serviceToFormValues(row);
    const payload = buildServiceSpec(
      { ...values, bind_auto: true, bind_port: 12345 },
      { editing: row },
    );
    expect(payload.bind_port).toBe(12345);
  });

  it('可选字段留空 → 省略键(bandwidth_limit/group/health_check/plugin/not_found_html/cache)', () => {
    const spec = buildServiceSpec(makeValues({ kind: 'http', name: 'empty' }));
    expect(spec).not.toHaveProperty('group');
    expect(spec).not.toHaveProperty('health_check');
    expect(spec).not.toHaveProperty('plugin');
    expect(spec.transport).toEqual({
      use_compression: false,
      bandwidth_mode: 'server',
      proxy_proto: 'none',
    });
    // http 段落里的固定键即使为空也照写,只有增强字段按开关省略
    expect(spec.http).toEqual({
      custom_domains: [],
      subdomain: '',
      locations: [],
      host_rewrite: '',
      http_user: '',
      http_password: '',
      route_by_http_user: '',
      request_headers: { set: [] },
      response_headers: { set: [] },
    });
  });

  it('数字保持数字类型,旧版不做字符串→数字强转(依赖 InputNumber 给 number)', () => {
    const numeric = buildServiceSpec(
      makeValues({
        kind: 'tcp',
        bind_auto: false,
        bind_port: 8443,
        target_port: 443,
        pool_size: 8,
      }),
    );
    expect(numeric.bind_port).toBe(8443);
    expect(numeric.target_port).toBe(443);
    expect(numeric.pool_size).toBe(8);
    expect(typeof numeric.bind_port).toBe('number');

    // 旧实现没有 Number() 强转:传入数字字符串会原样透传(新 UI 必须继续用数值输入)
    const stringy = {
      ...makeValues({ kind: 'tcp', bind_auto: false }),
      bind_port: '8443' as unknown as number,
      target_port: '443' as unknown as number,
      pool_size: '8' as unknown as number,
    };
    const spec = buildServiceSpec(stringy);
    expect(spec.bind_port).toBe('8443');
    expect(spec.target_port).toBe('443');
    expect(spec.pool_size).toBe('8');
  });
});

describe('serviceToFormValues:默认值与反解', () => {
  it('空 spec 全量回退默认值', () => {
    const row = makeRow({
      kind: 'tcp',
      bind_ip: '',
      bind_port: 0,
      target_host: '',
      target_port: 0,
      spec: {},
    });
    const v = serviceToFormValues(row);
    expect(v.bind_ip).toBe('0.0.0.0');
    expect(v.bind_auto).toBe(false);
    expect(v.pool_size).toBe(0);
    expect(v.bandwidth_limit).toBe('');
    expect(v.bandwidth_mode).toBe('server');
    expect(v.proxy_proto).toBe('none');
    expect(v.group).toBe('');
    expect(v.hc_enabled).toBe(false);
    expect(v.hc_type).toBe('tcp');
    expect(v.hc_interval).toBe(10);
    expect(v.hc_timeout).toBe(3);
    expect(v.hc_max_failed).toBe(3);
    expect(v.hc_path).toBe('');
    expect(v.cache_enabled).toBe(false);
    expect(v.cache_ttl).toBe(300);
    expect(v.cache_max_size_mb).toBe(16);
    expect(v.cache_max_item_size_kb).toBe(1024);
    expect(v.plugin_kind).toBe('none');
    expect(v.req_headers).toEqual([]);
    expect(v.resp_headers).toEqual([]);
    expect(v.plugin_request_headers).toEqual([]);
  });

  it('反解嵌套:段落由 kind 决定,health_check/cache/plugin 子对象按存在与否开关', () => {
    const row = makeRow({
      kind: 'https',
      spec: {
        transport: {
          use_compression: true,
          bandwidth_limit: '1MB',
          bandwidth_mode: 'client',
          proxy_proto: 'v1',
        },
        health_check: {
          type: 'http',
          interval_seconds: 7,
          timeout_seconds: 4,
          max_failed: 2,
          path: '/ready',
        },
        https: { custom_domains: ['a.example.com'], subdomain: 'app' },
        plugin: { kind: 'socks5', user: 'u', password: 'p' },
      },
    });
    const v = serviceToFormValues(row);
    expect(v.use_compression).toBe(true);
    expect(v.bandwidth_limit).toBe('1MB');
    expect(v.bandwidth_mode).toBe('client');
    expect(v.proxy_proto).toBe('v1');
    expect(v.hc_enabled).toBe(true);
    expect(v.hc_type).toBe('http');
    expect(v.hc_interval).toBe(7);
    expect(v.hc_timeout).toBe(4);
    expect(v.hc_max_failed).toBe(2);
    expect(v.hc_path).toBe('/ready');
    expect(v.custom_domains).toBe('a.example.com');
    expect(v.subdomain).toBe('app');
    expect(v.plugin_kind).toBe('socks5');
    expect(v.plugin_user).toBe('u');
    expect(v.plugin_password).toBe('p');
  });

  it('段落为 null 时不抛错,按空段落回退(与旧版 section?. 可选链语义一致)', () => {
    const row = makeRow({ kind: 'http', spec: { http: null, transport: null } });
    const v = serviceToFormValues(row);
    expect(v.custom_domains).toBe('');
    expect(v.locations).toBe('');
    expect(v.host_rewrite).toBe('');
    expect(v.req_headers).toEqual([]);
    expect(v.use_compression).toBe(false);
    expect(v.bandwidth_mode).toBe('server');
    expect(v.proxy_proto).toBe('none');
  });

  it('段落值非数组时按旧版 csv() 回退空串(后端若返回字符串会丢值,见报告)', () => {
    const row = makeRow({
      kind: 'http',
      spec: { http: { custom_domains: 'a.example.com', locations: '/api' } },
    });
    const v = serviceToFormValues(row);
    expect(v.custom_domains).toBe('');
    expect(v.locations).toBe('');
  });
});

describe('parseDomains', () => {
  it('小写化/去空白/去空项,保留最左单通配', () => {
    expect(parseDomains(' A.Example.com , *.Example.com ,, ')).toEqual({
      list: ['a.example.com', '*.example.com'],
    });
  });

  it('拒绝裸 *、多通配、非最左通配与非法域名', () => {
    expect(parseDomains('*')).toEqual({ error: '通配仅支持最左标签(如 *.example.com)', list: [] });
    expect(parseDomains('*.a*.example.com')).toEqual({
      error: '仅支持单个通配标签:*.a*.example.com',
      list: [],
    });
    expect(parseDomains('a.*.example.com')).toEqual({
      error: '通配符位置非法:a.*.example.com',
      list: [],
    });
    expect(parseDomains('bad_domain!')).toEqual({ error: '域名格式非法:bad_domain!', list: [] });
  });
});

describe('validateServiceForm', () => {
  it('http 无域名且无子域名 → 报错', () => {
    expect(validateServiceForm(makeValues({ kind: 'http' }))).toEqual([
      { field: 'custom_domains', message: 'HTTP 规则需填写自定义域名或子域名' },
    ]);
  });

  it('http 仅有子域名 → 通过', () => {
    expect(validateServiceForm(makeValues({ kind: 'http', subdomain: 'app' }))).toEqual([]);
  });

  it('https 无域名且无子域名 → 报错', () => {
    expect(validateServiceForm(makeValues({ kind: 'https' }))).toEqual([
      { field: 'custom_domains', message: 'HTTPS 规则需填写自定义域名或子域名' },
    ]);
  });

  it('tcpmux 无域名 → 报错(子域名不豁免)', () => {
    expect(validateServiceForm(makeValues({ kind: 'tcpmux', subdomain: 'app' }))).toEqual([
      { field: 'custom_domains', message: 'TCPMux 规则需填写自定义域名' },
    ]);
  });

  it('域名语法错误优先返回,不再叠加"必填"错误', () => {
    expect(validateServiceForm(makeValues({ kind: 'http', custom_domains: '*' }))).toEqual([
      { field: 'custom_domains', message: '通配仅支持最左标签(如 *.example.com)' },
    ]);
  });

  it('新建 tcp 未勾自动分配且端口为空 → 报错;编辑态不报', () => {
    const values = makeValues({ kind: 'tcp', bind_auto: false, bind_port: null });
    expect(validateServiceForm(values)).toEqual([
      { field: 'bind_port', message: '请填写监听端口,或改用自动分配' },
    ]);
    expect(validateServiceForm(values, { editing: makeRow({ kind: 'tcp' }) })).toEqual([]);
  });
});

describe('matrix:字段可见性与插件定义', () => {
  it('kind traits 与旧版布尔推导一致', () => {
    expect(kindTraits('tcp')).toMatchObject({
      usesBind: true,
      usesDomain: false,
      supportsGroup: true,
      supportsHealthCheck: true,
      supportsPoolSize: true,
      supportsProxyProto: true,
      supportsHttpExtras: false,
      section: null,
    });
    expect(kindTraits('udp')).toMatchObject({
      usesBind: true,
      supportsGroup: true,
      supportsPoolSize: false,
      supportsProxyProto: false,
    });
    expect(kindTraits('http')).toMatchObject({
      usesBind: false,
      usesDomain: true,
      showSubdomain: true,
      supportsGroup: true,
      supportsProxyProto: true,
      supportsHttpExtras: true,
      section: 'http',
    });
    expect(kindTraits('https')).toMatchObject({
      usesDomain: true,
      showSubdomain: true,
      supportsGroup: true,
      supportsProxyProto: false,
      supportsHttpExtras: false,
      section: 'https',
    });
    expect(kindTraits('tcpmux')).toMatchObject({
      usesDomain: true,
      showSubdomain: false,
      supportsGroup: false,
      supportsHealthCheck: false,
      section: 'tcpmux',
    });
    expect(kindTraits('stcp')).toMatchObject({
      usesBind: false,
      usesDomain: false,
      section: 'stcp',
    });
    expect(kindTraits('sudp')).toMatchObject({ section: 'sudp' });
    expect(kindTraits('xtcp')).toMatchObject({ section: 'xtcp' });
  });

  it('未知 kind 走兜底 traits(不抛错,保持旧版宽松行为)', () => {
    expect(kindTraits('bogus' as ProxyKind)).toEqual({
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
    });
    // 未知 kind:所有段落被删、transport 照写、不写 bind/pool_size
    const spec = buildServiceSpec(makeValues({ kind: 'bogus' as ProxyKind, name: 'x' }));
    expect(spec).toEqual({
      client_id: 'client-1',
      name: 'x',
      kind: 'bogus',
      enabled: true,
      target_host: '127.0.0.1',
      target_port: 80,
      transport: { use_compression: false, bandwidth_mode: 'server', proxy_proto: 'none' },
    });
  });

  it('bind_port 的"0 = 自动分配"魔法值在矩阵中显式声明', () => {
    const bindPort = KIND_MATRIX.tcp.fields.find((f) => f.name === 'bind_port');
    expect(bindPort?.required).toBe(true);
    expect(bindPort?.magic).toContain('0 = 自动分配');
    expect(bindPort?.createOnlyWhenManualBind).toBe(true);
    const bindAuto = KIND_MATRIX.tcp.fields.find((f) => f.name === 'bind_auto');
    expect(bindAuto?.createOnly).toBe(true);
    expect(bindAuto?.magic).toContain('bind_port = 0');
  });

  it('pool_size 仅 tcp 可见;subdomain 仅 http/https 可见', () => {
    const names = (k: keyof typeof KIND_MATRIX) => KIND_MATRIX[k].fields.map((f) => f.name);
    expect(names('tcp')).toContain('pool_size');
    expect(names('udp')).not.toContain('pool_size');
    expect(names('http')).toContain('subdomain');
    expect(names('https')).toContain('subdomain');
    expect(names('tcpmux')).not.toContain('subdomain');
    expect(names('tcp')).not.toContain('custom_domains');
    expect(names('stcp')).not.toContain('group');
    expect(names('stcp')).not.toContain('health_check');
  });

  it('target 区标注插件启用时隐藏', () => {
    const target = KIND_MATRIX.tcp.fields.filter((f) => f.group === 'target');
    expect(target.map((f) => f.name)).toEqual(['target_host', 'target_port']);
    expect(target.every((f) => f.hiddenWhenPlugin)).toBe(true);
    expect(target.every((f) => f.required)).toBe(true);
  });

  it('9 种插件的 specKey 与旧版 switch 逐字段一致', () => {
    expect(Object.keys(PLUGIN_FIELDS).sort()).toEqual([...PLUGIN_NAMES].sort());
    expect(specKeys('unix_domain_socket')).toEqual(['unix_path']);
    expect(specKeys('http_proxy')).toEqual(['http_user', 'http_password']);
    expect(specKeys('socks5')).toEqual(['user', 'password']);
    expect(specKeys('static_file')).toEqual([
      'local_path',
      'strip_prefix',
      'http_user',
      'http_password',
    ]);
    expect(specKeys('https2http')).toEqual([
      'local_addr',
      'crt_path',
      'key_path',
      'host_rewrite',
      'request_headers',
    ]);
    expect(specKeys('https2https')).toEqual([
      'local_addr',
      'crt_path',
      'key_path',
      'host_rewrite',
      'request_headers',
    ]);
    expect(specKeys('http2https')).toEqual(['local_addr', 'host_rewrite', 'request_headers']);
    expect(specKeys('http2http')).toEqual(['local_addr', 'host_rewrite', 'request_headers']);
    expect(specKeys('tls2raw')).toEqual(['local_addr', 'crt_path', 'key_path']);
  });

  it('插件必填字段与旧版 rules 一致', () => {
    const required = (p: PluginName) =>
      PLUGIN_FIELDS[p].filter((f) => f.required).map((f) => f.name);
    expect(required('unix_domain_socket')).toEqual(['plugin_unix_path']);
    expect(required('http_proxy')).toEqual([]);
    expect(required('socks5')).toEqual([]);
    expect(required('static_file')).toEqual(['plugin_local_path']);
    expect(required('https2http')).toEqual([
      'plugin_local_addr',
      'plugin_crt_path',
      'plugin_key_path',
    ]);
    expect(required('http2https')).toEqual(['plugin_local_addr']);
    expect(required('tls2raw')).toEqual([
      'plugin_local_addr',
      'plugin_crt_path',
      'plugin_key_path',
    ]);
  });

  it('request_headers 声明为多值键值对,默认空数组', () => {
    const headers = PLUGIN_FIELDS.https2http.find((f) => f.name === 'plugin_request_headers');
    expect(headers).toMatchObject({
      type: 'pairs',
      multi: true,
      required: false,
      defaultValue: [],
      specKey: 'request_headers',
    });
  });

  it('字段分组按渲染顺序去重(http 含域名增强与 HTTP 高级,stcp 只有基础组)', () => {
    expect(fieldGroupsOf('http')).toEqual([
      'connection',
      'target',
      'domain',
      'behavior',
      'plugin',
      'transport',
      'group',
      'health_check',
      'http_advanced',
      'domain_enhance',
    ]);
    expect(fieldGroupsOf('stcp')).toEqual([
      'connection',
      'target',
      'behavior',
      'plugin',
      'transport',
    ]);
  });

  it('secret_key / allow_users 不在任何表单字段矩阵中(R24 / R32)', () => {
    const all = Object.values(KIND_MATRIX).flatMap((m) => m.fields.map((f) => String(f.name)));
    expect(all.some((n) => n.includes('secret') || n.includes('allow_users'))).toBe(false);
  });
});
