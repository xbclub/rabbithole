import { zodResolver } from '@hookform/resolvers/zod';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { Activity, CircleCheck, KeyRound, RefreshCw, Server, TriangleAlert } from 'lucide-react';
import { useEffect } from 'react';
import { useForm } from 'react-hook-form';
import { useNavigate, useSearchParams } from 'react-router';
import { z } from 'zod';

import { changePassword, getServerConfig, putServerConfig } from '@/api/rabbithole';
import type { ServerConfigResult } from '@/api/rabbithole';
import { InfoLabel } from '@/components/info-label';
import { MonoCopy } from '@/components/mono';
import { PageHeader } from '@/components/page-header';
import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import {
  Form,
  FormControl,
  FormDescription,
  FormField,
  FormItem,
  FormLabel,
  FormMessage,
} from '@/components/ui/form';
import { Input } from '@/components/ui/input';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';
import { Skeleton } from '@/components/ui/skeleton';
import { Switch } from '@/components/ui/switch';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { Textarea } from '@/components/ui/textarea';
import { feedbackSuccess, feedbackWarning } from '@/lib/feedback';
import { qk } from '@/lib/query-keys';
import { ROUTES } from '@/router/paths';
import { useAuthStore } from '@/stores/auth';

const LOG_LEVELS = ['trace', 'debug', 'info', 'warn', 'error'] as const;

/**
 * 整数输入以字符串承载(便于空值/非法值行内提示),提交前统一转 number ——
 * 后端 serde 不接受字符串。
 */
function intInRange(min: number, max: number, label: string) {
  return z
    .string()
    .trim()
    .refine(
      (v) => /^\d+$/.test(v) && Number(v) >= min && Number(v) <= max,
      `${label}须为 ${min}-${max} 的整数`,
    );
}

function nonNegativeInt(label: string) {
  return z
    .string()
    .trim()
    .refine((v) => /^\d+$/.test(v), `${label}须为非负整数`);
}

/** 宽松 IP 校验:IPv4 四段 0-255;IPv6 支持 :: 缩写与内嵌 IPv4(最终以服务端解析为准)。 */
function isValidIp(value: string): boolean {
  const v = value.trim();
  if (!v) return false;
  if (/^\d{1,3}(\.\d{1,3}){3}$/.test(v)) {
    return v.split('.').every((seg) => Number(seg) <= 255);
  }
  if (!v.includes(':')) return false;
  const parts = v.split('::');
  if (parts.length > 2) return false;
  const groups = parts.flatMap((part) => (part ? part.split(':') : []));
  if (groups.length > 8) return false;
  if (parts.length === 1 && groups.length !== 8) return false;
  if (parts.length === 2 && groups.length > 7) return false;
  return groups.every((group) => /^[0-9a-fA-F]{1,4}$/.test(group) || isValidIp(group));
}

/** 服务器配置(热生效子集):SSH 网关 / 端口策略 / 日志级别 / 公钥白名单。 */
const serverSchema = z.object({
  ssh_enabled: z.boolean(),
  ssh_bind_addr: z
    .string()
    .trim()
    .refine((v) => v === '' || isValidIp(v), '监听地址须为合法 IP(留空 = 0.0.0.0)'),
  ssh_bind_port: intInRange(1, 65535, 'SSH 网关监听端口'),
  allow_ports: z.string(),
  max_services_per_client: intInRange(0, 10000, '每客户端最大转发规则数'),
  log_level: z.enum(LOG_LEVELS),
  authorized_keys: z.string(),
  // 并发与限流(0 = 不限;范围与后端一致)
  limit_max_mux_streams: intInRange(0, 16777216, '复用流上限'),
  limit_max_data_conns_per_client: nonNegativeInt('每客户端并发通道'),
  limit_max_udp_sessions_per_proxy: nonNegativeInt('每规则 UDP 会话'),
  limit_unauth_conns_per_ip: nonNegativeInt('认证窗口并发/IP'),
  limit_max_pool_size: intInRange(0, 255, '池深上限'),
  limit_default_pool_size: intInRange(0, 255, '默认池深'),
  limit_control_send_queue: intInRange(16, 65536, '控制帧队列'),
  limit_udp_packet_size: intInRange(0, 65536, 'UDP 包缓冲'),
  limit_pool_wait_timeout_ms: intInRange(100, 600000, '等池超时'),
});

type ServerFormValues = z.infer<typeof serverSchema>;

/** 「并发与限流」九项:标签与说明与后端字段一一对应(0 = 不限)。 */
const LIMIT_FIELDS = [
  {
    name: 'limit_max_mux_streams',
    label: '复用流上限',
    infoKey: 'settings.limits.max_mux_streams',
  },
  {
    name: 'limit_max_data_conns_per_client',
    label: '每客户端并发通道',
    infoKey: 'settings.limits.max_data_conns_per_client',
  },
  {
    name: 'limit_max_udp_sessions_per_proxy',
    label: '每规则 UDP 会话',
    infoKey: 'settings.limits.max_udp_sessions_per_proxy',
  },
  {
    name: 'limit_unauth_conns_per_ip',
    label: '认证窗口并发/IP',
    infoKey: 'settings.limits.unauth_conns_per_ip',
  },
  { name: 'limit_max_pool_size', label: '池深上限', infoKey: 'settings.limits.max_pool_size' },
  {
    name: 'limit_default_pool_size',
    label: '默认池深',
    infoKey: 'settings.limits.default_pool_size',
  },
  {
    name: 'limit_control_send_queue',
    label: '控制帧队列',
    infoKey: 'settings.limits.control_send_queue',
  },
  {
    name: 'limit_udp_packet_size',
    label: 'UDP 包缓冲',
    infoKey: 'settings.limits.udp_packet_size',
  },
  {
    name: 'limit_pool_wait_timeout_ms',
    label: '等池超时(ms)',
    infoKey: 'settings.limits.pool_wait_timeout_ms',
  },
] as const;

/** 监控指标(独立监听端口)表单。 */
const metricsSchema = z.object({
  bind_addr: z
    .string()
    .trim()
    .refine((v) => v !== '' && isValidIp(v), '监听地址须为合法 IP(如 127.0.0.1)'),
  port: intInRange(1, 65535, '监听端口'),
});

type MetricsFormValues = z.infer<typeof metricsSchema>;

/** 修改密码:新密码 ≥8 位且两次一致。 */
const passwordSchema = z
  .object({
    oldPassword: z.string().min(1, '请输入当前密码'),
    newPassword: z.string().min(8, '新密码至少 8 位'),
    confirmPassword: z.string().min(1, '请再次输入新密码'),
  })
  .refine((v) => v.newPassword === v.confirmPassword, {
    message: '两次输入的新密码不一致',
    path: ['confirmPassword'],
  });

type PasswordFormValues = z.infer<typeof passwordSchema>;

const EMPTY_SERVER_FORM: ServerFormValues = {
  ssh_enabled: false,
  ssh_bind_addr: '',
  ssh_bind_port: '2222',
  allow_ports: '',
  max_services_per_client: '0',
  log_level: 'info',
  authorized_keys: '',
  // 与服务端默认一致(上游回填/提交用的同一组兜底值)
  limit_max_mux_streams: '0',
  limit_max_data_conns_per_client: '0',
  limit_max_udp_sessions_per_proxy: '0',
  limit_unauth_conns_per_ip: '64',
  limit_max_pool_size: '0',
  limit_default_pool_size: '2',
  limit_control_send_queue: '256',
  limit_udp_packet_size: '0',
  limit_pool_wait_timeout_ms: '10000',
};

/** 保存结果统一提示:写回失败时运行时仍已生效(与旧版口径一致)。 */
function notifyResult(result: ServerConfigResult) {
  if (result.persisted) {
    feedbackSuccess(result.applied.length ? `已生效:${result.applied.join(', ')}` : '配置未变更');
  } else {
    feedbackWarning(`已生效但写回文件失败:${result.error ?? '未知错误'}`);
  }
}

/** 保存返回结果展示:applied / restart_required / error 三项都要可见。 */
function ConfigResultAlert({ result }: { result: ServerConfigResult }) {
  return (
    <div className="space-y-2">
      <Alert variant={result.persisted ? 'default' : 'destructive'}>
        {result.persisted ? <CircleCheck /> : <TriangleAlert />}
        <AlertTitle>
          {result.persisted ? '已生效' : '写回配置文件失败(运行时已生效,重启后会回退)'}
        </AlertTitle>
        <AlertDescription>
          <p>{result.applied.length ? `生效项:${result.applied.join(', ')}` : '无字段变更'}</p>
          {result.error ? <p>错误:{result.error}</p> : null}
        </AlertDescription>
      </Alert>
      {result.restart_required.length ? (
        <Alert>
          <RefreshCw />
          <AlertTitle>以下配置需重启服务端后生效</AlertTitle>
          <AlertDescription>{result.restart_required.join(', ')}</AlertDescription>
        </Alert>
      ) : null}
    </div>
  );
}

/** 服务器配置卡:保存后立即生效并写回配置文件。 */
function ServerConfigCard() {
  const queryClient = useQueryClient();
  const cfgQ = useQuery({ queryKey: qk.serverConfig, queryFn: getServerConfig });

  const form = useForm<ServerFormValues>({
    resolver: zodResolver(serverSchema),
    defaultValues: EMPTY_SERVER_FORM,
  });

  // 加载后回填(字段常驻渲染,无 setFieldsValue 挂起风险)
  useEffect(() => {
    if (!cfgQ.data) return;
    const hot = cfgQ.data.hot;
    const level: ServerFormValues['log_level'] = (LOG_LEVELS as readonly string[]).includes(
      hot.log_level,
    )
      ? (hot.log_level as ServerFormValues['log_level'])
      : 'info';
    form.reset({
      ssh_enabled: hot.ssh_gateway.enabled,
      ssh_bind_addr: hot.ssh_gateway.bind_addr || '0.0.0.0',
      ssh_bind_port: String(hot.ssh_gateway.bind_port || 2222),
      allow_ports: hot.allow_ports.join(','),
      max_services_per_client: String(hot.max_services_per_client),
      log_level: level,
      authorized_keys: cfgQ.data.authorized_keys ?? '',
      limit_max_mux_streams: String(hot.limits?.max_mux_streams ?? 0),
      limit_max_data_conns_per_client: String(hot.limits?.max_data_conns_per_client ?? 0),
      limit_max_udp_sessions_per_proxy: String(hot.limits?.max_udp_sessions_per_proxy ?? 0),
      limit_unauth_conns_per_ip: String(hot.limits?.unauth_conns_per_ip ?? 64),
      limit_max_pool_size: String(hot.limits?.max_pool_size ?? 0),
      limit_default_pool_size: String(hot.limits?.default_pool_size ?? 2),
      limit_control_send_queue: String(hot.limits?.control_send_queue ?? 256),
      limit_udp_packet_size: String(hot.limits?.udp_packet_size ?? 0),
      limit_pool_wait_timeout_ms: String(hot.limits?.pool_wait_timeout_ms ?? 10000),
    });
  }, [cfgQ.data, form]);

  const saveMut = useMutation({
    mutationFn: (values: ServerFormValues) =>
      putServerConfig({
        ssh_gateway: {
          enabled: values.ssh_enabled,
          bind_addr: values.ssh_bind_addr.trim(),
          bind_port: Number(values.ssh_bind_port),
        },
        ports: {
          allow_ports: values.allow_ports
            .split(',')
            .map((x) => x.trim())
            .filter(Boolean),
          max_services_per_client: Number(values.max_services_per_client),
        },
        log: { level: values.log_level },
        limits: {
          max_mux_streams: Number(values.limit_max_mux_streams),
          max_data_conns_per_client: Number(values.limit_max_data_conns_per_client),
          max_udp_sessions_per_proxy: Number(values.limit_max_udp_sessions_per_proxy),
          unauth_conns_per_ip: Number(values.limit_unauth_conns_per_ip),
          max_pool_size: Number(values.limit_max_pool_size),
          default_pool_size: Number(values.limit_default_pool_size),
          control_send_queue: Number(values.limit_control_send_queue),
          udp_packet_size: Number(values.limit_udp_packet_size),
          pool_wait_timeout_ms: Number(values.limit_pool_wait_timeout_ms),
        },
        authorized_keys: values.authorized_keys,
      }),
    onSuccess: (result, values) => {
      notifyResult(result);
      // 以已提交值作为新基准:react-query 结构共享可能保留旧 data 引用,
      // 不能只依赖回填 effect 把「无改动时禁用保存」恢复成未改动态
      form.reset(values);
      void queryClient.invalidateQueries({ queryKey: qk.serverConfig });
    },
  });

  if (cfgQ.isLoading) {
    return (
      <Card>
        <CardContent className="space-y-2">
          <Skeleton className="h-9 w-full" />
          <Skeleton className="h-9 w-full" />
          <Skeleton className="h-9 w-full" />
        </CardContent>
      </Card>
    );
  }

  // 加载失败不渲染表单:空表单点「保存配置」会把端口策略清空(P0 同源风险)
  if (cfgQ.isError) {
    return (
      <Card>
        <CardContent>
          <Alert variant="destructive">
            <TriangleAlert />
            <AlertTitle>加载服务器配置失败</AlertTitle>
            <AlertDescription>
              <p>{cfgQ.error?.message ?? '未知错误'}</p>
              <Button
                variant="outline"
                size="sm"
                disabled={cfgQ.isFetching}
                onClick={() => void cfgQ.refetch()}
              >
                {cfgQ.isFetching ? '重试中…' : '重试'}
              </Button>
            </AlertDescription>
          </Alert>
        </CardContent>
      </Card>
    );
  }

  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <Server className="size-4" />
          服务器配置
        </CardTitle>
        <CardDescription>
          以下配置保存后立即生效并写回配置文件(原文件备份为 *.toml.bak,注释会丢失)。vhost
          端口、主监听等其余配置仍需修改文件并重启。
        </CardDescription>
      </CardHeader>
      <CardContent className="space-y-5">
        <Form {...form}>
          <form
            className="space-y-5"
            onSubmit={form.handleSubmit((values) => {
              saveMut.mutate(values);
            })}
          >
            <section className="space-y-4">
              <h3 className="text-sm font-semibold">SSH 网关</h3>
              <FormField
                control={form.control}
                name="ssh_enabled"
                render={({ field }) => (
                  <FormItem className="flex flex-row items-center justify-between gap-4 rounded-md border p-3">
                    <div className="space-y-1">
                      <FormLabel>
                        <InfoLabel title="启用" infoKey="settings.ssh_gateway.enabled" />
                      </FormLabel>
                      <FormDescription>开启后可用 ssh -R 在服务端开临时转发端口。</FormDescription>
                    </div>
                    <FormControl>
                      <Switch checked={field.value} onCheckedChange={field.onChange} />
                    </FormControl>
                  </FormItem>
                )}
              />
              <FormField
                control={form.control}
                name="ssh_bind_addr"
                render={({ field }) => (
                  <FormItem>
                    <FormLabel>
                      <InfoLabel title="监听地址" infoKey="settings.ssh_gateway.bind_addr" />
                    </FormLabel>
                    <FormControl>
                      <Input className="w-64" placeholder="0.0.0.0" {...field} />
                    </FormControl>
                    <FormMessage />
                  </FormItem>
                )}
              />
              <FormField
                control={form.control}
                name="ssh_bind_port"
                render={({ field }) => (
                  <FormItem>
                    <FormLabel>
                      <InfoLabel title="监听端口" infoKey="settings.ssh_gateway.bind_port" />
                    </FormLabel>
                    <FormControl>
                      <Input className="w-32" min={1} max={65535} type="number" {...field} />
                    </FormControl>
                    <FormMessage />
                  </FormItem>
                )}
              />
              <FormField
                control={form.control}
                name="authorized_keys"
                render={({ field }) => (
                  <FormItem>
                    <FormLabel>
                      <InfoLabel title="公钥白名单" infoKey="settings.ssh_gateway.public_keys" />
                    </FormLabel>
                    <FormControl>
                      <Textarea
                        className="min-h-24 font-mono text-xs"
                        placeholder={'ssh-ed25519 AAAA... client-id\nssh-ed25519 AAAA... edge-2'}
                        rows={4}
                        {...field}
                      />
                    </FormControl>
                    <FormMessage />
                  </FormItem>
                )}
              />
            </section>

            <section className="space-y-4">
              <h3 className="text-sm font-semibold">端口策略</h3>
              <FormField
                control={form.control}
                name="allow_ports"
                render={({ field }) => (
                  <FormItem>
                    <FormLabel>
                      <InfoLabel title="允许端口" infoKey="settings.allow_ports" />
                    </FormLabel>
                    <FormControl>
                      <Input className="w-64" placeholder="10000-40000" {...field} />
                    </FormControl>
                    <FormDescription>
                      逗号分隔,支持范围如 10000-20000,30000;空 = 不限制。
                    </FormDescription>
                    <FormMessage />
                  </FormItem>
                )}
              />
              <FormField
                control={form.control}
                name="max_services_per_client"
                render={({ field }) => (
                  <FormItem>
                    <FormLabel>
                      <InfoLabel
                        title="每客户端最大转发规则数"
                        infoKey="settings.max_services_per_client"
                      />
                    </FormLabel>
                    <FormControl>
                      <Input className="w-32" min={0} max={10000} type="number" {...field} />
                    </FormControl>
                    <FormMessage />
                  </FormItem>
                )}
              />
            </section>

            <section className="space-y-4">
              <h3 className="text-sm font-semibold">日志</h3>
              <FormField
                control={form.control}
                name="log_level"
                render={({ field }) => (
                  <FormItem>
                    <FormLabel>
                      <InfoLabel title="级别" infoKey="settings.log_level" />
                    </FormLabel>
                    <Select value={field.value} onValueChange={field.onChange}>
                      <FormControl>
                        <SelectTrigger className="w-40">
                          <SelectValue />
                        </SelectTrigger>
                      </FormControl>
                      <SelectContent>
                        {LOG_LEVELS.map((level) => (
                          <SelectItem key={level} value={level}>
                            {level}
                          </SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                    <FormMessage />
                  </FormItem>
                )}
              />
            </section>

            <section className="space-y-4">
              <div className="space-y-1">
                <h3 className="text-sm font-semibold">并发与限流</h3>
                <p className="text-xs leading-relaxed text-muted-foreground">
                  默认全部不设限(0 = 不限),并发上限交由系统层(fd / 内存 / 端口)决定。
                  标「新会话生效」的项对已接入客户端在下一次重连后应用;QUIC
                  传输的流上限需重启服务端。
                </p>
              </div>
              <div className="grid gap-4 sm:grid-cols-2">
                {LIMIT_FIELDS.map((item) => (
                  <FormField
                    key={item.name}
                    control={form.control}
                    name={item.name}
                    render={({ field }) => (
                      <FormItem>
                        <FormLabel>
                          <InfoLabel title={item.label} infoKey={item.infoKey} />
                        </FormLabel>
                        <FormControl>
                          <Input inputMode="numeric" className="w-40" {...field} />
                        </FormControl>
                        <FormMessage />
                      </FormItem>
                    )}
                  />
                ))}
              </div>
            </section>

            <div className="flex items-center gap-3">
              <Button type="submit" disabled={!form.formState.isDirty || saveMut.isPending}>
                {saveMut.isPending ? '保存中…' : '保存配置'}
              </Button>
              {!form.formState.isDirty ? (
                <span className="text-xs text-muted-foreground">尚无改动</span>
              ) : null}
            </div>
          </form>
        </Form>

        {saveMut.data ? <ConfigResultAlert result={saveMut.data} /> : null}
      </CardContent>
    </Card>
  );
}

/** 监控指标卡:Prometheus 开关(热启停)与监听地址/端口(保存热重启),均写回配置文件。 */
function MetricsCard() {
  const queryClient = useQueryClient();
  // 与 ServerConfigCard 共用 query key:同页切 tab 不重复请求
  const cfgQ = useQuery({ queryKey: qk.serverConfig, queryFn: getServerConfig });

  const metrics = cfgQ.data?.hot.metrics;
  const metricsEnabled = !!metrics?.enable_prometheus;
  const metricsBindAddr = metrics?.bind_addr;
  const metricsPort = metrics?.port;

  const form = useForm<MetricsFormValues>({
    resolver: zodResolver(metricsSchema),
    defaultValues: { bind_addr: '127.0.0.1', port: '9090' },
  });

  // 仅在服务端值变化时回填,避免打字过程中被刷新覆盖
  useEffect(() => {
    if (metricsBindAddr === undefined || metricsPort === undefined) return;
    form.reset({ bind_addr: metricsBindAddr, port: String(metricsPort) });
  }, [metricsBindAddr, metricsPort, form]);

  const onSaved = (result: ServerConfigResult) => {
    notifyResult(result);
    void queryClient.invalidateQueries({ queryKey: qk.serverConfig });
  };

  const toggleMut = useMutation({
    mutationFn: (enabled: boolean) => putServerConfig({ metrics: { enable_prometheus: enabled } }),
    onSuccess: onSaved,
  });

  const addrMut = useMutation({
    mutationFn: (values: MetricsFormValues) =>
      putServerConfig({
        metrics: { bind_addr: values.bind_addr.trim(), port: Number(values.port) },
      }),
    onSuccess: (result, values) => {
      // 以已提交值作为新基准,保证「无改动时禁用保存」
      form.reset(values);
      onSaved(result);
    },
  });

  // 抓取地址:独立端口。绑定 0.0.0.0 时展示当前主机名(实际可达地址),其余原样
  const bindHost = metrics
    ? metrics.bind_addr === '0.0.0.0' || metrics.bind_addr === '::'
      ? window.location.hostname
      : metrics.bind_addr
    : '';
  const metricsUrl = metrics ? `http://${bindHost}:${metrics.port}/metrics` : '';

  if (cfgQ.isLoading) {
    return (
      <Card>
        <CardContent className="space-y-2">
          <Skeleton className="h-9 w-full" />
          <Skeleton className="h-9 w-full" />
          <Skeleton className="h-9 w-full" />
        </CardContent>
      </Card>
    );
  }

  if (cfgQ.isError) {
    return (
      <Card>
        <CardContent>
          <Alert variant="destructive">
            <TriangleAlert />
            <AlertTitle>加载服务器配置失败</AlertTitle>
            <AlertDescription>
              <p>{cfgQ.error?.message ?? '未知错误'}</p>
              <Button
                variant="outline"
                size="sm"
                disabled={cfgQ.isFetching}
                onClick={() => void cfgQ.refetch()}
              >
                {cfgQ.isFetching ? '重试中…' : '重试'}
              </Button>
            </AlertDescription>
          </Alert>
        </CardContent>
      </Card>
    );
  }

  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <Activity className="size-4" />
          监控指标
        </CardTitle>
        <CardDescription>
          Prometheus 抓取端点与监听配置;保存后立即热生效并写回配置文件。
        </CardDescription>
      </CardHeader>
      <CardContent className="space-y-5">
        <div className="flex flex-row items-center justify-between gap-4 rounded-md border p-3">
          <div className="space-y-1">
            <div className="text-sm leading-none font-medium">
              <InfoLabel title="启用 Prometheus" infoKey="settings.prometheus.enabled" />
            </div>
            <p className="text-xs text-muted-foreground">关闭即停止监听。</p>
          </div>
          <Switch
            checked={metricsEnabled}
            disabled={toggleMut.isPending || cfgQ.isFetching}
            onCheckedChange={(checked) => toggleMut.mutate(checked)}
          />
        </div>

        <Form {...form}>
          <form
            className="space-y-5"
            onSubmit={form.handleSubmit((values) => {
              addrMut.mutate(values);
            })}
          >
            <FormField
              control={form.control}
              name="bind_addr"
              render={({ field }) => (
                <FormItem>
                  <FormLabel>
                    <InfoLabel title="监听地址" infoKey="settings.prometheus.bind_addr" />
                  </FormLabel>
                  <FormControl>
                    <Input className="w-64" placeholder="127.0.0.1" {...field} />
                  </FormControl>
                  <FormMessage />
                </FormItem>
              )}
            />
            <FormField
              control={form.control}
              name="port"
              render={({ field }) => (
                <FormItem>
                  <FormLabel>
                    <InfoLabel title="监听端口" infoKey="settings.prometheus.port" />
                  </FormLabel>
                  <FormControl>
                    <Input className="w-32" min={1} max={65535} type="number" {...field} />
                  </FormControl>
                  <FormMessage />
                </FormItem>
              )}
            />
            <Button
              type="submit"
              disabled={!metrics || !form.formState.isDirty || addrMut.isPending}
            >
              {addrMut.isPending ? '保存中…' : '保存监听配置'}
            </Button>
          </form>
        </Form>

        <div className="flex flex-wrap items-center gap-2 text-sm">
          <span className="text-muted-foreground">Prometheus 抓取地址:</span>
          <MonoCopy ariaLabel="复制 Prometheus 地址" full={metricsUrl} />
        </div>

        <ol className="list-decimal space-y-1 pl-5 text-[13px] leading-relaxed text-muted-foreground">
          <li>默认仅监听 127.0.0.1:Prometheus 与服务端同机部署,或经内网 / SSH 隧道抓取;</li>
          <li>远程直抓把上方监听地址改为 0.0.0.0 保存即可(端口无鉴权,注意防火墙);</li>
          <li>可用指标:连接数、在线客户端、入/出站累计字节数。</li>
        </ol>

        {toggleMut.data ? <ConfigResultAlert result={toggleMut.data} /> : null}
        {addrMut.data ? <ConfigResultAlert result={addrMut.data} /> : null}
      </CardContent>
    </Card>
  );
}

/** 修改密码:成功后服务端轮换 JWT 密钥,所有会话失效需重登。 */
function PasswordCard() {
  const navigate = useNavigate();

  const form = useForm<PasswordFormValues>({
    resolver: zodResolver(passwordSchema),
    defaultValues: { oldPassword: '', newPassword: '', confirmPassword: '' },
  });

  const changeMut = useMutation({
    mutationFn: (values: PasswordFormValues) =>
      changePassword(values.oldPassword, values.newPassword),
    onSuccess: () => {
      feedbackSuccess('密码已修改,所有会话已失效,请重新登录');
      // 后端已轮换 JWT 密钥:当前令牌必然失效,清 token 并强制回登录页
      useAuthStore.getState().clear();
      navigate(ROUTES.login, { replace: true });
    },
  });

  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <KeyRound className="size-4" />
          修改密码
        </CardTitle>
        <CardDescription>修改成功后服务端轮换 JWT 密钥,所有会话立即失效。</CardDescription>
      </CardHeader>
      <CardContent className="space-y-5">
        <Alert>
          <TriangleAlert />
          <AlertTitle>修改成功后所有会话将立即失效,需重新登录</AlertTitle>
          <AlertDescription>
            服务端会轮换 JWT 签名密钥,当前令牌立即作废,必须重新登录后才能继续使用控制台。
          </AlertDescription>
        </Alert>

        <Form {...form}>
          <form
            className="space-y-4"
            onSubmit={form.handleSubmit((values) => {
              changeMut.mutate(values);
            })}
          >
            <FormField
              control={form.control}
              name="oldPassword"
              render={({ field }) => (
                <FormItem>
                  <FormLabel>当前密码</FormLabel>
                  <FormControl>
                    <Input
                      autoComplete="current-password"
                      placeholder="输入当前密码"
                      type="password"
                      {...field}
                    />
                  </FormControl>
                  <FormMessage />
                </FormItem>
              )}
            />
            <FormField
              control={form.control}
              name="newPassword"
              render={({ field }) => (
                <FormItem>
                  <FormLabel>新密码</FormLabel>
                  <FormControl>
                    <Input
                      autoComplete="new-password"
                      placeholder="至少 8 位"
                      type="password"
                      {...field}
                    />
                  </FormControl>
                  <FormMessage />
                </FormItem>
              )}
            />
            <FormField
              control={form.control}
              name="confirmPassword"
              render={({ field }) => (
                <FormItem>
                  <FormLabel>确认新密码</FormLabel>
                  <FormControl>
                    <Input
                      autoComplete="new-password"
                      placeholder="再次输入新密码"
                      type="password"
                      {...field}
                    />
                  </FormControl>
                  <FormMessage />
                </FormItem>
              )}
            />
            <Button type="submit" disabled={changeMut.isPending}>
              {changeMut.isPending ? '提交中…' : '修改密码'}
            </Button>
          </form>
        </Form>
      </CardContent>
    </Card>
  );
}

const SETTINGS_TABS = ['server', 'metrics', 'password'] as const;
type SettingsTab = (typeof SETTINGS_TABS)[number];

/** 设置页:服务器配置 / 监控指标 / 修改密码 三 tab,tab 状态写 query 持久化。 */
export default function SettingsPage() {
  const [searchParams, setSearchParams] = useSearchParams();
  const tabParam = searchParams.get('tab');
  const tab: SettingsTab = (SETTINGS_TABS as readonly string[]).includes(tabParam ?? '')
    ? (tabParam as SettingsTab)
    : 'server';

  const onTabChange = (next: string) => {
    const params = new URLSearchParams(searchParams);
    params.set('tab', next);
    setSearchParams(params, { replace: true });
  };

  return (
    <div className="space-y-6">
      <PageHeader title="设置" description="服务器配置、监控指标与修改密码" />
      <Tabs value={tab} onValueChange={onTabChange}>
        <TabsList>
          <TabsTrigger value="server">服务器配置</TabsTrigger>
          <TabsTrigger value="metrics">监控指标</TabsTrigger>
          <TabsTrigger value="password">修改密码</TabsTrigger>
        </TabsList>
        <TabsContent value="server">
          <ServerConfigCard />
        </TabsContent>
        <TabsContent value="metrics">
          <MetricsCard />
        </TabsContent>
        <TabsContent value="password">
          <PasswordCard />
        </TabsContent>
      </Tabs>
    </div>
  );
}
