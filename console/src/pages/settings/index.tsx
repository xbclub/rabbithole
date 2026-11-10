import { zodResolver } from '@hookform/resolvers/zod';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import {
  Activity,
  CircleCheck,
  Gauge,
  KeyRound,
  Lock,
  Network,
  Plus,
  RefreshCw,
  Server,
  Shield,
  Sparkles,
  Terminal,
  TriangleAlert,
} from 'lucide-react';
import { useEffect, useState } from 'react';
import { useForm, useWatch } from 'react-hook-form';
import { useNavigate, useSearchParams } from 'react-router';
import { z } from 'zod';

import {
  changePassword,
  createApiKey,
  deleteApiKey,
  getServerConfig,
  listApiKeys,
  putServerConfig,
  rotateApiKey,
} from '@/api/rabbithole';
import type { ApiKeyRow, ServerConfigResult } from '@/api/rabbithole';
import { ConfirmDialog } from '@/components/confirm-dialog';
import { EmptyState } from '@/components/empty-state';
import { InfoLabel } from '@/components/info-label';
import { Mono, MonoCopy } from '@/components/mono';
import { PageHeader } from '@/components/page-header';
import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Checkbox } from '@/components/ui/checkbox';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog';
import {
  Form,
  FormControl,
  FormDescription,
  FormField,
  FormItem,
  FormLabel,
  FormMessage,
} from '@/components/ui/form';
import { Input, PasswordInput } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';
import { Skeleton } from '@/components/ui/skeleton';
import { Switch } from '@/components/ui/switch';
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table';
import { Textarea } from '@/components/ui/textarea';
import { SCOPE_BITS, scopeBadges } from '@/features/api-keys/scope';
import { feedbackError, feedbackSuccess, feedbackWarning } from '@/lib/feedback';
import { fmtRelative, fmtTime } from '@/lib/format';
import { qk } from '@/lib/query-keys';
import { cn } from '@/lib/utils';
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

/**
 * ALPN 列表校验:逗号分隔的可见 ASCII,每项 1-255 字节,最多 8 项。
 */
const ALPN_INVALID_MSG = 'ALPN 须为逗号分隔的可见 ASCII,每项 1-255 字符,最多 8 项';

function alpnListValid(raw: string): boolean {
  const items = raw
    .split(',')
    .map((x) => x.trim())
    .filter(Boolean);
  if (items.length > 8) return false;
  return items.every((it) => it.length >= 1 && it.length <= 255 && /^[\x21-\x7e]+$/.test(it));
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
  // TLS/ALPN(重启生效)。逗号分隔;留空 = 用默认值 "h2"。
  alpn: z.string().trim().refine(alpnListValid, ALPN_INVALID_MSG),
});

type ServerFormValues = z.infer<typeof serverSchema>;

/** 「并发与限流」九项:标签与说明与后端字段一一对应(0 = 不限)。 */
const LIMIT_FIELDS = [
  {
    name: 'limit_max_mux_streams',
    label: '复用流上限',
    hint: '0 = 不限',
    infoKey: 'settings.limits.max_mux_streams',
  },
  {
    name: 'limit_max_data_conns_per_client',
    label: '每客户端并发通道',
    hint: '0 = 不限',
    infoKey: 'settings.limits.max_data_conns_per_client',
  },
  {
    name: 'limit_max_udp_sessions_per_proxy',
    label: '每规则 UDP 会话',
    hint: '0 = 不限',
    infoKey: 'settings.limits.max_udp_sessions_per_proxy',
  },
  {
    name: 'limit_unauth_conns_per_ip',
    label: '认证窗口并发/IP',
    hint: '默认 64',
    infoKey: 'settings.limits.unauth_conns_per_ip',
  },
  {
    name: 'limit_max_pool_size',
    label: '池深上限',
    hint: '0-255 (0=不限)',
    infoKey: 'settings.limits.max_pool_size',
  },
  {
    name: 'limit_default_pool_size',
    label: '默认池深',
    hint: '默认 2',
    infoKey: 'settings.limits.default_pool_size',
  },
  {
    name: 'limit_control_send_queue',
    label: '控制帧队列',
    hint: '16-65536',
    infoKey: 'settings.limits.control_send_queue',
  },
  {
    name: 'limit_udp_packet_size',
    label: 'UDP 包缓冲',
    hint: '0 = 默认',
    infoKey: 'settings.limits.udp_packet_size',
  },
  {
    name: 'limit_pool_wait_timeout_ms',
    label: '等池超时(ms)',
    hint: '100-600000',
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
  limit_max_mux_streams: '0',
  limit_max_data_conns_per_client: '0',
  limit_max_udp_sessions_per_proxy: '0',
  limit_unauth_conns_per_ip: '64',
  limit_max_pool_size: '0',
  limit_default_pool_size: '2',
  limit_control_send_queue: '256',
  limit_udp_packet_size: '0',
  limit_pool_wait_timeout_ms: '10000',
  alpn: '',
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

/** 服务器配置矩阵:模块化分组 + 3×3 并发与限流网格,保存后立即生效并写回配置文件。 */
function ServerConfigCard() {
  const queryClient = useQueryClient();
  const cfgQ = useQuery({ queryKey: qk.serverConfig, queryFn: getServerConfig });

  const form = useForm<ServerFormValues>({
    resolver: zodResolver(serverSchema),
    defaultValues: EMPTY_SERVER_FORM,
  });

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
      alpn: (cfgQ.data.alpn ?? []).join(','),
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
        tls: {
          alpn: values.alpn
            .split(',')
            .map((x) => x.trim())
            .filter(Boolean),
        },
      }),
    onSuccess: (result, values) => {
      notifyResult(result);
      form.reset(values);
      void queryClient.invalidateQueries({ queryKey: qk.serverConfig });
    },
  });

  if (cfgQ.isLoading) {
    return (
      <div className="rh-telemetry-card space-y-3 rounded-2xl border bg-card p-6">
        <Skeleton className="h-9 w-full" />
        <Skeleton className="h-28 w-full" />
        <Skeleton className="h-40 w-full" />
      </div>
    );
  }

  if (cfgQ.isError) {
    return (
      <div className="rh-telemetry-card rounded-2xl border bg-card p-6">
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
      </div>
    );
  }

  return (
    <Form {...form}>
      <form
        className="space-y-5"
        onSubmit={form.handleSubmit((values) => {
          saveMut.mutate(values);
        })}
      >
        {/* 模块 1: SSH 网关 & 公钥白名单 */}
        <section className="rh-telemetry-card space-y-4 rounded-2xl border border-border/80 bg-card p-5 shadow-2xs">
          <div className="flex flex-wrap items-center justify-between gap-2 border-b border-border/60 pb-3">
            <div className="flex items-center gap-2.5">
              <div className="flex size-8 items-center justify-center rounded-lg border border-primary/20 bg-primary/10 text-primary">
                <Terminal className="size-4" />
              </div>
              <div>
                <h3 className="text-sm font-semibold">SSH 网关</h3>
                <p className="text-xs text-muted-foreground">
                  免安装客户端通过 <code className="font-mono">ssh -R</code> 创建临时转发隧道
                </p>
              </div>
            </div>
            <Badge variant="outline" className="font-mono text-[11px]">
              热生效 · 写回 TOML
            </Badge>
          </div>

          <FormField
            control={form.control}
            name="ssh_enabled"
            render={({ field }) => (
              <FormItem className="flex flex-row items-center justify-between gap-4 rounded-xl border border-border/70 bg-muted/25 p-3.5">
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

          <div className="grid gap-4 sm:grid-cols-2">
            <FormField
              control={form.control}
              name="ssh_bind_addr"
              render={({ field }) => (
                <FormItem>
                  <FormLabel>
                    <InfoLabel title="监听地址" infoKey="settings.ssh_gateway.bind_addr" />
                  </FormLabel>
                  <FormControl>
                    <Input className="font-mono" placeholder="0.0.0.0" {...field} />
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
                    <Input className="font-mono" min={1} max={65535} type="number" {...field} />
                  </FormControl>
                  <FormMessage />
                </FormItem>
              )}
            />
          </div>

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

        {/* 模块 2: 端口策略 + TLS ALPN + 日志级别 */}
        <div className="grid gap-5 lg:grid-cols-2">
          <section className="rh-telemetry-card space-y-4 rounded-2xl border border-border/80 bg-card p-5 shadow-2xs">
            <div className="flex items-center gap-2.5 border-b border-border/60 pb-3">
              <div className="flex size-8 items-center justify-center rounded-lg border border-primary/20 bg-primary/10 text-primary">
                <Network className="size-4" />
              </div>
              <div>
                <h3 className="text-sm font-semibold">端口策略</h3>
                <p className="text-xs text-muted-foreground">公网暴露端口范围与单节点规则配额</p>
              </div>
            </div>

            <FormField
              control={form.control}
              name="allow_ports"
              render={({ field }) => (
                <FormItem>
                  <FormLabel>
                    <InfoLabel title="允许端口" infoKey="settings.allow_ports" />
                  </FormLabel>
                  <FormControl>
                    <Input className="font-mono" placeholder="10000-40000" {...field} />
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
                    <Input
                      className="w-40 font-mono"
                      min={0}
                      max={10000}
                      type="number"
                      {...field}
                    />
                  </FormControl>
                  <FormMessage />
                </FormItem>
              )}
            />
          </section>

          <section className="rh-telemetry-card space-y-4 rounded-2xl border border-border/80 bg-card p-5 shadow-2xs">
            <div className="flex items-center justify-between gap-2 border-b border-border/60 pb-3">
              <div className="flex items-center gap-2.5">
                <div className="flex size-8 items-center justify-center rounded-lg border border-primary/20 bg-primary/10 text-primary">
                  <Shield className="size-4" />
                </div>
                <div>
                  <h3 className="text-sm font-semibold">TLS & 日志</h3>
                  <p className="text-xs text-muted-foreground">TLS 握手 ALPN 标识与运行日志级别</p>
                </div>
              </div>
              <Badge variant="outline" className="font-mono text-[10px]">
                ALPN 重启生效
              </Badge>
            </div>

            <FormField
              control={form.control}
              name="alpn"
              render={({ field }) => (
                <FormItem>
                  <FormLabel>
                    <InfoLabel title="ALPN 标识" infoKey="settings.alpn" />
                  </FormLabel>
                  <FormControl>
                    <Input className="font-mono" placeholder="h2" {...field} />
                  </FormControl>
                  <FormDescription>
                    逗号分隔,最多 8 项;留空 = 使用默认值 <code className="font-mono">h2</code>
                    。改动必须与客户端一致,且需<strong>重启服务端</strong>生效。
                  </FormDescription>
                  <FormMessage />
                </FormItem>
              )}
            />

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
                      <SelectTrigger className="w-44 font-mono">
                        <SelectValue />
                      </SelectTrigger>
                    </FormControl>
                    <SelectContent>
                      {LOG_LEVELS.map((level) => (
                        <SelectItem key={level} value={level} className="font-mono">
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
        </div>

        {/* 模块 3: 并发与限流 (3×3 矩阵) */}
        <section className="rh-telemetry-card space-y-4 rounded-2xl border border-border/80 bg-card p-5 shadow-2xs">
          <div className="flex flex-wrap items-center justify-between gap-2 border-b border-border/60 pb-3">
            <div className="flex items-center gap-2.5">
              <div className="flex size-8 items-center justify-center rounded-lg border border-primary/20 bg-primary/10 text-primary">
                <Gauge className="size-4" />
              </div>
              <div>
                <h3 className="text-sm font-semibold">并发与限流</h3>
                <p className="text-xs text-muted-foreground">
                  默认全部不设限(0 = 不限),并发上限交由系统层(fd / 内存 / 端口)决定
                </p>
              </div>
            </div>
            <Badge variant="outline" className="font-mono text-[11px]">
              3×3 限流矩阵
            </Badge>
          </div>

          <div className="grid gap-3.5 sm:grid-cols-2 lg:grid-cols-3">
            {LIMIT_FIELDS.map((item) => (
              <FormField
                key={item.name}
                control={form.control}
                name={item.name}
                render={({ field }) => (
                  <FormItem className="rounded-xl border border-border/60 bg-muted/20 p-3">
                    <div className="flex items-center justify-between gap-2">
                      <FormLabel className="text-xs">
                        <InfoLabel title={item.label} infoKey={item.infoKey} />
                      </FormLabel>
                      <span className="font-mono text-[10px] text-muted-foreground">
                        {item.hint}
                      </span>
                    </div>
                    <FormControl>
                      <Input inputMode="numeric" className="h-8 font-mono text-xs" {...field} />
                    </FormControl>
                    <FormMessage />
                  </FormItem>
                )}
              />
            ))}
          </div>
        </section>

        {/* 底部保存操作栏 (跟随表单流式排列,不悬浮固定) */}
        <div className="rh-telemetry-card flex flex-wrap items-center justify-between gap-3 rounded-2xl border border-border/80 bg-card px-5 py-3.5 shadow-2xs">
          <div className="text-xs text-muted-foreground">
            {!form.formState.isDirty ? (
              <span>尚无改动 · 所有热更新配置与服务端保持同步</span>
            ) : (
              <span className="font-medium text-primary">
                检测到未保存的配置变更，点击右侧按钮立即应用并写回配置文件
              </span>
            )}
          </div>
          <div className="flex items-center gap-2">
            {form.formState.isDirty ? (
              <Button
                type="button"
                variant="ghost"
                size="sm"
                disabled={saveMut.isPending}
                onClick={() => form.reset()}
              >
                放弃修改
              </Button>
            ) : null}
            <Button type="submit" disabled={!form.formState.isDirty || saveMut.isPending}>
              {saveMut.isPending ? '保存中…' : '保存配置'}
            </Button>
          </div>
        </div>

        {saveMut.data ? <ConfigResultAlert result={saveMut.data} /> : null}
      </form>
    </Form>
  );
}

/** 监控指标卡:Prometheus 开关(热启停)与监听地址/端口(保存热重启),均写回配置文件。 */
function MetricsCard() {
  const queryClient = useQueryClient();
  const cfgQ = useQuery({ queryKey: qk.serverConfig, queryFn: getServerConfig });

  const metrics = cfgQ.data?.hot.metrics;
  const metricsEnabled = !!metrics?.enable_prometheus;
  const metricsBindAddr = metrics?.bind_addr;
  const metricsPort = metrics?.port;

  const form = useForm<MetricsFormValues>({
    resolver: zodResolver(metricsSchema),
    defaultValues: { bind_addr: '127.0.0.1', port: '9090' },
  });

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
      form.reset(values);
      onSaved(result);
    },
  });

  const bindHost = metrics
    ? metrics.bind_addr === '0.0.0.0' || metrics.bind_addr === '::'
      ? window.location.hostname
      : metrics.bind_addr
    : '';
  const metricsUrl = metrics ? `http://${bindHost}:${metrics.port}/metrics` : '';

  if (cfgQ.isLoading) {
    return (
      <div className="rh-telemetry-card space-y-2 rounded-2xl border bg-card p-6">
        <Skeleton className="h-9 w-full" />
        <Skeleton className="h-9 w-full" />
        <Skeleton className="h-9 w-full" />
      </div>
    );
  }

  if (cfgQ.isError) {
    return (
      <div className="rh-telemetry-card rounded-2xl border bg-card p-6">
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
      </div>
    );
  }

  return (
    <div className="rh-telemetry-card space-y-5 rounded-2xl border border-border/80 bg-card p-5 shadow-2xs sm:p-6">
      <div className="flex flex-wrap items-center justify-between gap-2 border-b border-border/60 pb-4">
        <div className="flex items-center gap-2.5">
          <div className="flex size-9 items-center justify-center rounded-xl border border-primary/20 bg-primary/10 text-primary">
            <Activity className="size-4" />
          </div>
          <div>
            <h3 className="text-base font-semibold">监控指标</h3>
            <p className="text-xs text-muted-foreground">
              Prometheus 抓取端点与监听配置;保存后立即热生效并写回配置文件
            </p>
          </div>
        </div>
        <Badge
          variant="outline"
          className={cn(
            'rounded-full px-2.5 py-0.5 text-xs',
            metricsEnabled
              ? 'border-emerald-500/35 bg-emerald-500/10 text-emerald-700 dark:text-emerald-300'
              : 'text-muted-foreground',
          )}
        >
          {metricsEnabled ? 'Prometheus 已启用' : 'Prometheus 已停用'}
        </Badge>
      </div>

      <div className="flex flex-row items-center justify-between gap-4 rounded-xl border border-border/70 bg-muted/25 p-4">
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

      {/* Prometheus 抓取地址展示卡 */}
      <div className="rounded-xl border border-primary/20 bg-primary/5 p-4">
        <div className="text-[11px] font-medium tracking-wider text-muted-foreground uppercase">
          PROMETHEUS SCRAPE ENDPOINT · 抓取地址
        </div>
        <div className="mt-1.5 flex flex-wrap items-center gap-2 text-sm">
          <span className="text-xs text-muted-foreground">Prometheus 抓取地址:</span>
          <MonoCopy ariaLabel="复制 Prometheus 地址" full={metricsUrl} />
        </div>
      </div>

      <Form {...form}>
        <form
          className="space-y-4"
          onSubmit={form.handleSubmit((values) => {
            addrMut.mutate(values);
          })}
        >
          <div className="grid gap-4 sm:grid-cols-2">
            <FormField
              control={form.control}
              name="bind_addr"
              render={({ field }) => (
                <FormItem>
                  <FormLabel>
                    <InfoLabel title="监听地址" infoKey="settings.prometheus.bind_addr" />
                  </FormLabel>
                  <FormControl>
                    <Input className="font-mono" placeholder="127.0.0.1" {...field} />
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
                    <Input className="font-mono" min={1} max={65535} type="number" {...field} />
                  </FormControl>
                  <FormMessage />
                </FormItem>
              )}
            />
          </div>
          <Button type="submit" disabled={!metrics || !form.formState.isDirty || addrMut.isPending}>
            {addrMut.isPending ? '保存中…' : '保存监听配置'}
          </Button>
        </form>
      </Form>

      <ol className="list-decimal space-y-1 rounded-xl border border-border/60 bg-muted/20 p-4 pl-8 text-[12px] leading-relaxed text-muted-foreground">
        <li>默认仅监听 127.0.0.1:Prometheus 与服务端同机部署,或经内网 / SSH 隧道抓取;</li>
        <li>远程直抓把上方监听地址改为 0.0.0.0 保存即可(端口无鉴权,注意防火墙);</li>
        <li>可用指标:连接数、在线客户端、入/出站累计字节数。</li>
      </ol>

      {toggleMut.data ? <ConfigResultAlert result={toggleMut.data} /> : null}
      {addrMut.data ? <ConfigResultAlert result={addrMut.data} /> : null}
    </div>
  );
}

/** 计算密码强度评分(0-4)与文案。 */
function calcPasswordStrength(pw: string): { score: number; label: string; color: string } {
  if (!pw) return { score: 0, label: '未输入', color: 'bg-muted' };
  let score = 0;
  if (pw.length >= 8) score += 1;
  if (pw.length >= 12) score += 1;
  if (/[A-Z]/.test(pw) && /[a-z]/.test(pw)) score += 1;
  if (/\d/.test(pw) && /[^A-Za-z0-9]/.test(pw)) score += 1;
  if (score <= 1) return { score: 1, label: '弱', color: 'bg-destructive' };
  if (score === 2) return { score: 2, label: '中等', color: 'bg-amber-500' };
  if (score === 3) return { score: 3, label: '强', color: 'bg-emerald-500' };
  return { score: 4, label: '极强', color: 'bg-emerald-500' };
}

function generateStrongPassword(): string {
  const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz23456789!@#$%^&*_-+=';
  const arr = new Uint32Array(16);
  window.crypto.getRandomValues(arr);
  return Array.from(arr, (n) => chars[n % chars.length]).join('');
}

/** 修改密码:支持明文眼睛图标查看、随机强密码生成与强度指示。 */
function PasswordCard() {
  const navigate = useNavigate();

  const form = useForm<PasswordFormValues>({
    resolver: zodResolver(passwordSchema),
    defaultValues: { oldPassword: '', newPassword: '', confirmPassword: '' },
  });

  const watchedNewPassword = useWatch({ control: form.control, name: 'newPassword' });
  const strength = calcPasswordStrength(watchedNewPassword || '');

  const changeMut = useMutation({
    mutationFn: (values: PasswordFormValues) =>
      changePassword(values.oldPassword, values.newPassword),
    onSuccess: () => {
      feedbackSuccess('密码已修改,所有会话已失效,请重新登录');
      useAuthStore.getState().clear();
      navigate(ROUTES.login, { replace: true });
    },
  });

  const handleGenerate = () => {
    const pwd = generateStrongPassword();
    form.setValue('newPassword', pwd, { shouldValidate: true, shouldDirty: true });
    form.setValue('confirmPassword', pwd, { shouldValidate: true, shouldDirty: true });
    if (navigator.clipboard && window.isSecureContext) {
      void navigator.clipboard.writeText(pwd).then(() => {
        feedbackSuccess('已生成 16 位强密码并复制到剪贴板');
      });
    } else {
      feedbackSuccess('已自动填入随机强密码，可点击眼睛图标查看');
    }
  };

  return (
    <div className="rh-telemetry-card space-y-5 rounded-2xl border border-border/80 bg-card p-5 shadow-2xs sm:p-6">
      <div className="flex items-center gap-2.5 border-b border-border/60 pb-4">
        <div className="flex size-9 items-center justify-center rounded-xl border border-primary/20 bg-primary/10 text-primary">
          <Lock className="size-4" />
        </div>
        <div>
          <h3 className="text-base font-semibold">修改密码</h3>
          <p className="text-xs text-muted-foreground">
            更新管理员登录口令，修改成功后所有已登录设备将强制下线
          </p>
        </div>
      </div>

      <Alert>
        <TriangleAlert />
        <AlertTitle>修改成功后所有会话将立即失效,需重新登录</AlertTitle>
        <AlertDescription>
          密码更新后服务端将立即吊销全部历史登录会话，您需要使用新密码重新登录控制台。
        </AlertDescription>
      </Alert>

      <Form {...form}>
        <form
          className="max-w-md space-y-4"
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
                  <PasswordInput
                    autoComplete="current-password"
                    placeholder="输入当前密码"
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
                <div className="flex items-center justify-between">
                  <FormLabel>新密码</FormLabel>
                  <button
                    type="button"
                    onClick={handleGenerate}
                    className="inline-flex items-center gap-1 text-xs font-medium text-primary transition-opacity hover:opacity-80"
                  >
                    <Sparkles className="size-3" />
                    随机生成强密码
                  </button>
                </div>
                <FormControl>
                  <PasswordInput autoComplete="new-password" placeholder="至少 8 位" {...field} />
                </FormControl>
                {watchedNewPassword ? (
                  <div className="space-y-1 pt-0.5">
                    <div className="grid grid-cols-4 gap-1.5">
                      {[1, 2, 3, 4].map((idx) => (
                        <div
                          key={idx}
                          className={cn(
                            'h-1 rounded-full transition-colors',
                            idx <= strength.score ? strength.color : 'bg-muted',
                          )}
                        />
                      ))}
                    </div>
                    <div className="flex items-center justify-between text-[11px] text-muted-foreground">
                      <span>密码强度: {strength.label}</span>
                      <Mono>{watchedNewPassword.length} 字符</Mono>
                    </div>
                  </div>
                ) : null}
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
                  <PasswordInput
                    autoComplete="new-password"
                    placeholder="再次输入新密码"
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
    </div>
  );
}

/** 一次性明文展示弹窗:关闭即清空。 */
function PlaintextDialog({
  open,
  onOpenChange,
  plaintext,
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  plaintext: string;
}) {
  return (
    <Dialog
      open={open}
      onOpenChange={(next) => {
        if (!next) onOpenChange(false);
      }}
    >
      <DialogContent className="sm:max-w-lg">
        <DialogHeader>
          <DialogTitle>API 密钥(仅此一次展示)</DialogTitle>
          <DialogDescription>
            服务端只保存哈希,关闭后无法再次查看,只能重新生成(rotate)。
          </DialogDescription>
        </DialogHeader>
        <div className="rounded-xl border border-slate-800 bg-slate-950 p-3 text-slate-100">
          <MonoCopy
            full={plaintext}
            className="w-full text-emerald-300 hover:bg-slate-900"
            ariaLabel="复制 API 密钥"
          />
        </div>
        <Alert>
          <TriangleAlert />
          <AlertTitle>请立即复制保存</AlertTitle>
          <AlertDescription>明文不会再次出现;泄露时先吊销再重建,不要试图找回。</AlertDescription>
        </Alert>
        <DialogFooter>
          <Button
            variant="outline"
            onClick={() => {
              onOpenChange(false);
            }}
          >
            我已保存,关闭
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

/** scope 勾选分组:每资源一行(读/写),自定义模式用。 */
const SCOPE_GROUPS: {
  label: string;
  read: keyof typeof SCOPE_BITS;
  write: keyof typeof SCOPE_BITS;
}[] = [
  { label: '客户端', read: 'clients_r', write: 'clients_w' },
  { label: '服务规则', read: 'services_r', write: 'services_w' },
  { label: '访问器', read: 'visitors_r', write: 'visitors_w' },
  { label: '统计', read: 'stats_r', write: null as never },
  { label: '事件', read: 'events_r', write: null as never },
  { label: '服务端配置', read: 'config_r', write: 'config_w' },
  { label: '证书', read: 'certs_r', write: 'certs_w' },
];

/** API 密钥管理:列表 / 创建(预设+自定义勾选+有效期) / 轮换 / 吊销。 */
function ApiKeysCard() {
  const queryClient = useQueryClient();
  const listQuery = useQuery({ queryKey: qk.apiKeys, queryFn: listApiKeys });

  const [createOpen, setCreateOpen] = useState(false);
  const [name, setName] = useState('');
  const [preset, setPreset] = useState('readonly');
  const [custom, setCustom] = useState(0);
  const [days, setDays] = useState('0');
  const [plaintext, setPlaintext] = useState('');

  const invalidate = () => queryClient.invalidateQueries({ queryKey: qk.apiKeys });

  const createMut = useMutation({
    mutationFn: () =>
      createApiKey({
        name: name.trim(),
        ...(preset === 'custom' ? { scopes: custom } : { preset }),
        ...(days !== '0' ? { expires_in_days: Number(days) } : {}),
      }),
    onSuccess: (d) => {
      setCreateOpen(false);
      setName('');
      setPreset('readonly');
      setCustom(0);
      setDays('0');
      setPlaintext(d.key);
      void invalidate();
    },
    onError: (e) => feedbackError(`创建失败:${e.message}`),
  });

  const rotateMut = useMutation({
    mutationFn: (id: string) => rotateApiKey(id),
    onSuccess: (d) => {
      feedbackSuccess('已轮换,旧密钥立即失效');
      setPlaintext(d.key);
      void invalidate();
    },
    onError: (e) => feedbackError(`轮换失败:${e.message}`),
  });

  const revokeMut = useMutation({
    mutationFn: (id: string) => deleteApiKey(id),
    onSuccess: () => {
      feedbackSuccess('已吊销');
      void invalidate();
    },
    onError: (e) => feedbackError(`吊销失败:${e.message}`),
  });

  return (
    <div className="rh-telemetry-card space-y-4 rounded-2xl border border-border/80 bg-card p-5 shadow-2xs sm:p-6">
      <div className="flex flex-wrap items-center justify-between gap-3 border-b border-border/60 pb-4">
        <div className="flex items-center gap-2.5">
          <div className="flex size-9 items-center justify-center rounded-xl border border-primary/20 bg-primary/10 text-primary">
            <KeyRound className="size-4" />
          </div>
          <div>
            <h3 className="text-base font-semibold">API 密钥</h3>
            <p className="text-xs text-muted-foreground">
              服务端调用的 Bearer 凭据(rhk_ 前缀);明文仅创建/轮换时展示一次
            </p>
          </div>
        </div>
        <Button onClick={() => setCreateOpen(true)}>
          <Plus className="size-4" />
          新建密钥
        </Button>
      </div>

      {listQuery.isLoading ? (
        <Skeleton className="h-32 w-full" />
      ) : listQuery.isError ? (
        <Alert variant="destructive">
          <TriangleAlert />
          <AlertTitle>加载失败</AlertTitle>
          <AlertDescription>{(listQuery.error as Error).message}</AlertDescription>
        </Alert>
      ) : (listQuery.data ?? []).length === 0 ? (
        <div className="space-y-4">
          <div className="rounded-xl border border-dashed border-border/80 bg-muted/20 py-6">
            <EmptyState
              icon={<KeyRound className="size-6" />}
              title="还没有 API 密钥"
              description="创建以 rhk_ 为前缀的 Bearer 访问密钥，供 CI/CD 流水线、Terraform 或自动化运维脚本安全调用服务端接口。"
              action={
                <Button onClick={() => setCreateOpen(true)}>
                  <Plus className="size-4" />
                  新建首个密钥
                </Button>
              }
            />
          </div>
          <div className="flex flex-wrap items-center justify-between gap-2 rounded-xl border border-border/70 bg-muted/25 px-4 py-3 text-xs text-muted-foreground">
            <span>自动化请求认证头示例：</span>
            <Mono className="rounded-md border border-border/70 bg-background px-2 py-1 text-foreground">
              Authorization: Bearer rhk_xxxxxxxx...
            </Mono>
          </div>
        </div>
      ) : (
        <div className="space-y-4">
          {/* 移动端卡片列表 (<768px) */}
          <div className="space-y-2.5 md:hidden">
            {(listQuery.data ?? []).map((row) => (
              <div
                key={row.id}
                className="space-y-2.5 rounded-xl border border-border/70 bg-muted/20 p-3.5 text-xs"
              >
                <div className="flex items-start justify-between gap-2">
                  <div>
                    <div className="text-sm font-semibold">{row.name}</div>
                    <Mono className="text-muted-foreground">rhk_{row.prefix}…</Mono>
                  </div>
                  {row.enabled ? <Badge>正常</Badge> : <Badge variant="destructive">已吊销</Badge>}
                </div>
                <div className="flex flex-wrap gap-1">
                  {scopeBadges(row.scopes).map((b) => (
                    <Badge key={b} variant="secondary" className="font-mono text-[11px]">
                      {b}
                    </Badge>
                  ))}
                </div>
                <div className="flex flex-wrap items-center justify-between gap-2 border-t border-border/60 pt-2 text-muted-foreground">
                  <span>
                    最近: {row.last_used_at ? fmtRelative(row.last_used_at) : '从未使用'} · 过期:{' '}
                    {row.expires_at ? fmtTime(row.expires_at) : '永不'}
                  </span>
                  {row.enabled ? (
                    <div className="flex items-center gap-1.5">
                      <ConfirmDialog
                        title={`轮换密钥「${row.name}」`}
                        description="旧密钥立即失效,新明文仅展示一次。"
                        confirmText="轮换"
                        onConfirm={async () => rotateMut.mutate(row.id)}
                        trigger={
                          <Button variant="outline" size="sm">
                            轮换
                          </Button>
                        }
                      />
                      <ConfirmDialog
                        title={`吊销密钥「${row.name}」`}
                        description="吊销后该密钥立即失效;记录保留用于审计,名称不可复用。"
                        confirmText="吊销"
                        destructive
                        onConfirm={async () => revokeMut.mutate(row.id)}
                        trigger={
                          <Button variant="outline" size="sm" disabled={revokeMut.isPending}>
                            吊销
                          </Button>
                        }
                      />
                    </div>
                  ) : null}
                </div>
              </div>
            ))}
          </div>

          {/* 桌面端完整表格 (≥768px) */}
          <div className="hidden overflow-x-auto rounded-xl border border-border/70 md:block">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>名称</TableHead>
                  <TableHead>前缀</TableHead>
                  <TableHead>权限</TableHead>
                  <TableHead>创建者</TableHead>
                  <TableHead>最近使用</TableHead>
                  <TableHead>过期</TableHead>
                  <TableHead>状态</TableHead>
                  <TableHead className="text-right">操作</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {(listQuery.data ?? []).map((row) => (
                  <ApiKeyRowView
                    key={row.id}
                    row={row}
                    onRotate={() => rotateMut.mutate(row.id)}
                    onRevoke={() => revokeMut.mutate(row.id)}
                    revokePending={revokeMut.isPending}
                  />
                ))}
              </TableBody>
            </Table>
          </div>

          <div className="flex flex-wrap items-center justify-between gap-2 rounded-xl border border-border/70 bg-muted/25 px-4 py-2.5 text-xs text-muted-foreground">
            <span>在 HTTP 请求头中携带 Bearer 令牌即可调用授权范围内的 API：</span>
            <Mono className="rounded-md border border-border/70 bg-background px-2 py-0.5 text-foreground">
              Authorization: Bearer rhk_...
            </Mono>
          </div>
        </div>
      )}

      <Dialog open={createOpen} onOpenChange={setCreateOpen}>
        <DialogContent className="sm:max-w-lg">
          <DialogHeader>
            <DialogTitle>新建 API 密钥</DialogTitle>
            <DialogDescription>创建后明文仅展示一次,请立即保存。</DialogDescription>
          </DialogHeader>
          <div className="space-y-4">
            <div className="space-y-2">
              <Label htmlFor="apikey-name">名称</Label>
              <Input
                id="apikey-name"
                value={name}
                onChange={(e) => setName(e.target.value)}
                placeholder="如 ci-terraform(1-64 字符)"
              />
            </div>
            <div className="space-y-2">
              <Label>权限预设</Label>
              <Select value={preset} onValueChange={setPreset}>
                <SelectTrigger className="w-full">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="readonly">只读(全部读权限)</SelectItem>
                  <SelectItem value="ops">运维(读 + 客户端/规则/访问器写)</SelectItem>
                  <SelectItem value="full">全部权限</SelectItem>
                  <SelectItem value="custom">自定义</SelectItem>
                </SelectContent>
              </Select>
            </div>
            {preset === 'custom' && (
              <div className="grid grid-cols-2 gap-2 rounded-xl border border-border/70 bg-muted/20 p-3">
                {SCOPE_GROUPS.map((g) => (
                  <div key={g.label} className="flex items-center gap-3 text-[13px]">
                    <span className="w-20 text-muted-foreground">{g.label}</span>
                    <label className="flex items-center gap-1">
                      <Checkbox
                        checked={(custom & SCOPE_BITS[g.read]) !== 0}
                        onCheckedChange={(c) =>
                          setCustom((v) => (c ? v | SCOPE_BITS[g.read] : v & ~SCOPE_BITS[g.read]))
                        }
                      />
                      读
                    </label>
                    {g.write !== null && (
                      <label className="flex items-center gap-1">
                        <Checkbox
                          checked={(custom & SCOPE_BITS[g.write]) !== 0}
                          onCheckedChange={(c) =>
                            setCustom((v) =>
                              c ? v | SCOPE_BITS[g.write] : v & ~SCOPE_BITS[g.write],
                            )
                          }
                        />
                        写
                      </label>
                    )}
                  </div>
                ))}
                <Mono className="col-span-2">scopes = {custom}</Mono>
              </div>
            )}
            <div className="space-y-2">
              <Label>有效期</Label>
              <Select value={days} onValueChange={setDays}>
                <SelectTrigger className="w-full">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="0">永不过期</SelectItem>
                  <SelectItem value="1">1 天</SelectItem>
                  <SelectItem value="7">7 天</SelectItem>
                  <SelectItem value="30">30 天</SelectItem>
                  <SelectItem value="90">90 天</SelectItem>
                  <SelectItem value="365">1 年</SelectItem>
                </SelectContent>
              </Select>
            </div>
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => setCreateOpen(false)}>
              取消
            </Button>
            <Button
              disabled={createMut.isPending || name.trim().length === 0}
              onClick={() => createMut.mutate()}
            >
              {createMut.isPending ? '创建中…' : '创建'}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <PlaintextDialog
        open={plaintext !== ''}
        onOpenChange={(o) => {
          if (!o) setPlaintext('');
        }}
        plaintext={plaintext}
      />
    </div>
  );
}

/** 单行:名称/前缀/权限徽标/创建者/最近使用/过期/状态/操作。 */
function ApiKeyRowView({
  row,
  onRotate,
  onRevoke,
  revokePending,
}: {
  row: ApiKeyRow;
  onRotate: () => void;
  onRevoke: () => void;
  revokePending: boolean;
}) {
  return (
    <TableRow>
      <TableCell className="font-medium">{row.name}</TableCell>
      <TableCell>
        <Mono>rhk_{row.prefix}…</Mono>
      </TableCell>
      <TableCell>
        <div className="flex flex-wrap gap-1">
          {scopeBadges(row.scopes).map((b) => (
            <Badge key={b} variant="secondary" className="font-mono text-[11px]">
              {b}
            </Badge>
          ))}
        </div>
      </TableCell>
      <TableCell>{row.created_by || '-'}</TableCell>
      <TableCell className="text-muted-foreground">
        {row.last_used_at ? fmtRelative(row.last_used_at) : '从未使用'}
      </TableCell>
      <TableCell className="text-muted-foreground">
        {row.expires_at ? fmtTime(row.expires_at) : '永不'}
      </TableCell>
      <TableCell>
        {row.enabled ? <Badge>正常</Badge> : <Badge variant="destructive">已吊销</Badge>}
      </TableCell>
      <TableCell className="text-right">
        {row.enabled && (
          <div className="flex justify-end gap-2">
            <ConfirmDialog
              title={`轮换密钥「${row.name}」`}
              description="旧密钥立即失效,新明文仅展示一次。"
              confirmText="轮换"
              onConfirm={async () => onRotate()}
              trigger={
                <Button variant="outline" size="sm">
                  轮换
                </Button>
              }
            />
            <ConfirmDialog
              title={`吊销密钥「${row.name}」`}
              description="吊销后该密钥立即失效;记录保留用于审计,名称不可复用。"
              confirmText="吊销"
              destructive
              onConfirm={async () => onRevoke()}
              trigger={
                <Button variant="outline" size="sm" disabled={revokePending}>
                  吊销
                </Button>
              }
            />
          </div>
        )}
      </TableCell>
    </TableRow>
  );
}

const SETTINGS_TABS = ['server', 'metrics', 'password', 'apikeys'] as const;
type SettingsTab = (typeof SETTINGS_TABS)[number];

const CONTROL_MODULES: {
  id: SettingsTab;
  label: string;
  desc: string;
  badge: string;
  icon: typeof Server;
}[] = [
  {
    id: 'server',
    label: '服务器配置',
    desc: 'SSH 网关、端口策略、TLS ALPN 与 3×3 限流矩阵',
    badge: '热生效',
    icon: Server,
  },
  {
    id: 'metrics',
    label: '监控指标',
    desc: 'Prometheus 独立抓取端点与热启停开关',
    badge: 'Metrics',
    icon: Activity,
  },
  {
    id: 'apikeys',
    label: 'API 密钥',
    desc: 'CI / Terraform 自动化调用的 rhk_ Bearer 凭据',
    badge: 'Bearer',
    icon: KeyRound,
  },
  {
    id: 'password',
    label: '修改密码',
    desc: '更新管理员登录密码与强制注销历史登录会话',
    badge: '安全',
    icon: Lock,
  },
];

/**
 * 系统控制矩阵:
 * 桌面端左侧导览栏(含标题与四个控制模块卡片)常驻固定不动,右侧表单区独立平滑滚动;
 * 移动端顶部展示 2×2 模块切换器,下方直接跟当前表单。
 */
export default function SettingsPage() {
  const [searchParams, setSearchParams] = useSearchParams();
  const tabParam = searchParams.get('tab');
  const tab: SettingsTab = (SETTINGS_TABS as readonly string[]).includes(tabParam ?? '')
    ? (tabParam as SettingsTab)
    : 'server';

  const onTabChange = (next: SettingsTab) => {
    const params = new URLSearchParams(searchParams);
    params.set('tab', next);
    setSearchParams(params, { replace: true });
  };

  return (
    <div className="space-y-5">
      <PageHeader
        eyebrow="CONTROL PLANE MATRIX · 系统控制与安全凭据"
        title="系统控制矩阵"
        description="服务器热更新配置、Prometheus 遥测指标、自动化 API 密钥与管理员凭据管理"
      />

      <div className="grid gap-4 sm:gap-5 lg:grid-cols-12 lg:items-start">
        {/* 左侧控制平面分类导览轨 (桌面端与右侧卡片顶边平齐,滚动时 sticky 固定不动) */}
        <aside className="lg:sticky lg:top-20 lg:col-span-3">
          <div
            role="tablist"
            aria-label="系统控制分类"
            className="grid grid-cols-2 gap-2 lg:grid-cols-1 lg:gap-2.5"
          >
            {CONTROL_MODULES.map((m) => {
              const Icon = m.icon;
              const active = tab === m.id;
              return (
                <button
                  key={m.id}
                  type="button"
                  role="tab"
                  aria-selected={active}
                  onClick={() => onTabChange(m.id)}
                  className={cn(
                    'group flex flex-col gap-1.5 rounded-xl border p-2.5 text-left transition-all sm:rounded-2xl sm:p-3.5',
                    active
                      ? 'border-primary/45 bg-primary/10 shadow-xs ring-1 ring-primary/20'
                      : 'rh-telemetry-card bg-card/85 hover:border-primary/30 hover:bg-card',
                  )}
                >
                  <div className="flex items-center justify-between gap-1.5 sm:gap-2">
                    <div className="flex min-w-0 items-center gap-1.5 sm:gap-2.5">
                      <div
                        className={cn(
                          'flex size-6 shrink-0 items-center justify-center rounded-lg border transition-colors sm:size-7',
                          active
                            ? 'border-primary/30 bg-primary text-primary-foreground shadow-2xs'
                            : 'border-border/70 bg-muted/40 text-muted-foreground group-hover:text-foreground',
                        )}
                      >
                        <Icon className="size-3.5" />
                      </div>
                      <span
                        className={cn(
                          'truncate text-xs font-semibold sm:text-sm',
                          active ? 'text-primary' : 'text-foreground',
                        )}
                      >
                        {m.label}
                      </span>
                    </div>
                    <Badge
                      variant="outline"
                      className={cn(
                        'shrink-0 rounded-full px-1.5 py-0 font-mono text-[10px] max-xs:hidden sm:px-2',
                        active
                          ? 'border-primary/35 bg-primary/15 text-primary'
                          : 'text-muted-foreground',
                      )}
                    >
                      {m.badge}
                    </Badge>
                  </div>
                  <p className="pl-9 text-xs leading-relaxed text-muted-foreground max-sm:hidden">
                    {m.desc}
                  </p>
                </button>
              );
            })}
          </div>
        </aside>

        {/* 右侧配置内容区 (顶边与左侧导览平齐,随页面自然滚动) */}
        <div className="min-w-0 lg:col-span-9">
          {tab === 'server' ? <ServerConfigCard /> : null}
          {tab === 'metrics' ? <MetricsCard /> : null}
          {tab === 'apikeys' ? <ApiKeysCard /> : null}
          {tab === 'password' ? <PasswordCard /> : null}
        </div>
      </div>
    </div>
  );
}
