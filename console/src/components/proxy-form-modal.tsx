import { zodResolver } from '@hookform/resolvers/zod';
import { useQuery } from '@tanstack/react-query';
import { ArrowRight, ChevronDown, Loader2, Server, Waypoints } from 'lucide-react';
import { useEffect, useState } from 'react';
import { useForm } from 'react-hook-form';
import { z } from 'zod';

import { getService } from '@/api/rabbithole';
import type { ClientRow, ServiceRow } from '@/api/rabbithole';
import { HeaderPairsInput } from '@/components/header-pairs-input';
import { InfoLabel } from '@/components/info-label';
import { Mono } from '@/components/mono';
import { Button } from '@/components/ui/button';
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from '@/components/ui/collapsible';
import {
  Form,
  FormControl,
  FormField,
  FormItem,
  FormLabel,
  FormMessage,
} from '@/components/ui/form';
import { Input, PasswordInput } from '@/components/ui/input';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';
import { Separator } from '@/components/ui/separator';
import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetFooter,
  SheetHeader,
  SheetTitle,
} from '@/components/ui/sheet';
import { Switch } from '@/components/ui/switch';
import { Textarea } from '@/components/ui/textarea';
import { FIELD_INFO } from '@/features/field-info';
import type { FieldInfoKey } from '@/features/field-info';
import {
  GROUP_LABELS,
  KIND_MATRIX,
  PLUGIN_FIELDS,
  fieldGroupsOf,
  kindTraits,
} from '@/features/services/matrix';
import type { FieldGroupKey, FieldRule, PluginFieldDef } from '@/features/services/matrix';
import {
  buildServiceSpec,
  defaultProxyFormValues,
  serviceToFormValues,
  validateServiceForm,
} from '@/features/services/spec';
import type { ProxyFormValues, ProxyKind, ServiceSpecPayload } from '@/features/services/types';
import { focusFirstControl } from '@/lib/dialog-focus';
import { qk } from '@/lib/query-keys';

export type { ProxyKind } from '@/features/services/types';

/** 折叠在「高级配置」里的低频分组。 */
const ADVANCED_GROUPS: readonly FieldGroupKey[] = [
  'transport',
  'group',
  'health_check',
  'http_advanced',
  'domain_enhance',
];

const headerPairSchema = z.object({ name: z.string(), value: z.string() });

const formSchema = z
  .object({
    client_id: z.string().min(1, '请选择所属客户端'),
    name: z.string().regex(/^[a-zA-Z0-9_-]{1,64}$/, '1-64 位字母/数字/下划线/连字符'),
    kind: z.enum(['tcp', 'udp', 'http', 'https', 'tcpmux', 'stcp', 'sudp', 'xtcp']),
    enabled: z.boolean(),
    bind_ip: z.string(),
    bind_auto: z.boolean(),
    bind_port: z.number().nullable(),
    target_host: z.string(),
    target_port: z.number().nullable(),
    custom_domains: z.string(),
    subdomain: z.string(),
    use_compression: z.boolean(),
    pool_size: z.number().nullable(),
    bandwidth_limit: z.string(),
    bandwidth_mode: z.enum(['client', 'server']),
    proxy_proto: z.enum(['none', 'v1', 'v2']),
    group: z.string(),
    hc_enabled: z.boolean(),
    hc_type: z.enum(['http', 'tcp']),
    hc_interval: z.number().nullable(),
    hc_timeout: z.number().nullable(),
    hc_max_failed: z.number().nullable(),
    hc_path: z.string(),
    locations: z.string(),
    host_rewrite: z.string(),
    http_user: z.string(),
    http_password: z.string(),
    route_by_http_user: z.string(),
    req_headers: z.array(headerPairSchema),
    resp_headers: z.array(headerPairSchema),
    not_found_html: z.string(),
    cache_enabled: z.boolean(),
    cache_ttl: z.number().nullable(),
    cache_max_size_mb: z.number().nullable(),
    cache_extensions: z.string(),
    cache_max_item_size_kb: z.number().nullable(),
    plugin_kind: z.enum([
      'none',
      'unix_domain_socket',
      'http_proxy',
      'socks5',
      'static_file',
      'https2http',
      'https2https',
      'http2https',
      'http2http',
      'tls2raw',
    ]),
    plugin_unix_path: z.string(),
    plugin_http_user: z.string(),
    plugin_http_password: z.string(),
    plugin_user: z.string(),
    plugin_password: z.string(),
    plugin_local_path: z.string(),
    plugin_strip_prefix: z.string(),
    plugin_local_addr: z.string(),
    plugin_crt_path: z.string(),
    plugin_key_path: z.string(),
    plugin_host_rewrite: z.string(),
    plugin_request_headers: z.array(headerPairSchema),
  })
  .superRefine((values, ctx) => {
    const traits = kindTraits(values.kind);
    const pluginOn = values.plugin_kind !== 'none';

    if (!pluginOn) {
      if (!values.target_host.trim()) {
        ctx.addIssue({ code: 'custom', path: ['target_host'], message: '请填写目标地址' });
      }
      if (values.target_port === null) {
        ctx.addIssue({ code: 'custom', path: ['target_port'], message: '请填写目标端口' });
      }
    }
    if (traits.usesBind && !values.bind_auto) {
      if (values.bind_port === null) {
        ctx.addIssue({ code: 'custom', path: ['bind_port'], message: '请填写监听端口' });
      }
    }
    if (values.kind === 'tcpmux' && !values.custom_domains.trim()) {
      ctx.addIssue({
        code: 'custom',
        path: ['custom_domains'],
        message: 'TCPMux 至少需要一个域名',
      });
    }
    if (values.kind === 'http' || values.kind === 'https') {
      if (!values.custom_domains.trim() && !values.subdomain.trim()) {
        ctx.addIssue({
          code: 'custom',
          path: ['custom_domains'],
          message: '自定义域名与子域名至少填一个',
        });
      }
    }
  });

type FormSchemaValues = z.infer<typeof formSchema>;

function infoKeyFor(name: string): FieldInfoKey | undefined {
  const key = `service.${name}`;
  return key in FIELD_INFO ? (key as FieldInfoKey) : undefined;
}

function readbackValues(row: ServiceRow): ProxyFormValues {
  const values = serviceToFormValues(row);
  return { ...values, bind_auto: row.bind_port === 0 };
}

function NumberControl({
  value,
  onChange,
  disabled,
  min,
  max,
  placeholder,
  ...rest
}: {
  value: number | null;
  onChange: (next: number | null) => void;
  disabled?: boolean;
  min?: number;
  max?: number;
  placeholder?: string;
} & Omit<React.ComponentProps<'input'>, 'value' | 'onChange' | 'min' | 'max'>) {
  return (
    <Input
      {...rest}
      type="number"
      inputMode="numeric"
      disabled={disabled}
      min={min}
      max={max}
      placeholder={placeholder}
      value={value === null || value === undefined ? '' : String(value)}
      onChange={(event) => {
        const raw = event.target.value;
        if (raw === '') {
          onChange(null);
          return;
        }
        const parsed = event.target.valueAsNumber;
        onChange(Number.isNaN(parsed) ? null : parsed);
      }}
    />
  );
}

export interface ProxyFormModalProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  kinds: ProxyKind[];
  defaultKind: ProxyKind;
  defaultClientId?: string;
  editing: ServiceRow | null;
  clients: ClientRow[];
  submitting?: boolean;
  onSubmit: (spec: ServiceSpecPayload) => Promise<void> | void;
}

/**
 * 右侧滑出式隧道配置抽屉 (Slide-over Pipeline Drawer)。
 * 顶部常驻「实时链路拓扑预览卡」,随表单输入动态呈现 [公网入口 ──▶ 承载客户端 ──▶ 内网目标/插件]。
 */
export function ProxyFormModal({
  open,
  onOpenChange,
  kinds,
  defaultKind,
  defaultClientId,
  editing,
  clients,
  submitting = false,
  onSubmit,
}: ProxyFormModalProps) {
  const multiKind = kinds.length > 1;
  const fullRowQuery = useQuery({
    queryKey: qk.service(editing?.id ?? -1),
    queryFn: () => getService(editing!.id),
    enabled: open && editing !== null,
    staleTime: 5_000,
  });
  const editingRow = editing ? (fullRowQuery.data ?? null) : null;
  const fallbackClientId =
    defaultClientId && clients.some((c) => c.id === defaultClientId)
      ? defaultClientId
      : (clients[0]?.id ?? '');
  const defaultValues: ProxyFormValues = editingRow
    ? readbackValues(editingRow)
    : defaultProxyFormValues(defaultKind, fallbackClientId);

  const form = useForm<FormSchemaValues>({
    resolver: zodResolver(formSchema),
    defaultValues,
  });
  const [advancedOpen, setAdvancedOpen] = useState(false);

  useEffect(() => {
    if (editingRow) form.reset(readbackValues(editingRow));
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [editingRow]);

  const kind = form.watch('kind') as ProxyKind;
  const clientId = form.watch('client_id');
  const bindIp = form.watch('bind_ip');
  const bindAuto = form.watch('bind_auto');
  const bindPort = form.watch('bind_port');
  const customDomains = form.watch('custom_domains');
  const subdomain = form.watch('subdomain');
  const targetHost = form.watch('target_host');
  const targetPort = form.watch('target_port');
  const pluginKind = form.watch('plugin_kind');
  const hcType = form.watch('hc_type');
  const groups = fieldGroupsOf(kind);

  const selectedClient = clients.find((c) => c.id === clientId);

  // 计算实时链路预览文案
  const previewIngress = (() => {
    if (kind === 'tcp' || kind === 'udp') {
      const ip = bindIp?.trim() || '0.0.0.0';
      if (bindAuto) return `${ip}:自动端口`;
      return `${ip}:${bindPort ?? '?'}`;
    }
    if (kind === 'http' || kind === 'https' || kind === 'tcpmux') {
      const firstDom = customDomains
        ?.split(',')
        .map((s) => s.trim())
        .find(Boolean);
      if (firstDom) return firstDom;
      if (subdomain?.trim()) return `${subdomain.trim()}.*`;
      return '待填写域名';
    }
    return '密钥鉴权访问器';
  })();

  const previewTarget = (() => {
    if (pluginKind !== 'none') {
      return `插件 · ${pluginKind}`;
    }
    const host = targetHost?.trim() || '127.0.0.1';
    return `${host}:${targetPort ?? '?'}`;
  })();

  const visible = (rule: FieldRule): boolean => {
    if (rule.name === 'kind' && !multiKind) return false;
    if (rule.createOnly && editing) return false;
    if (rule.createOnlyWhenManualBind && !editing && bindAuto) return false;
    if (rule.hiddenWhenPlugin && pluginKind !== 'none') return false;
    if (rule.visibleWhenHcType && hcType !== rule.visibleWhenHcType) return false;
    return true;
  };

  const disabledFor = (rule: FieldRule): boolean => {
    if (!editing) return false;
    if (rule.name === 'client_id' || rule.name === 'kind' || rule.name === 'bind_auto') return true;
    return false;
  };

  const handleSubmit = form.handleSubmit(async (values) => {
    const errors = validateServiceForm(values as ProxyFormValues, { editing });
    let applied = false;
    for (const error of errors) {
      if (form.getFieldState(error.field).error) continue;
      form.setError(error.field, { type: 'validate', message: error.message });
      applied = true;
    }
    if (applied) return;

    const spec = buildServiceSpec(values as ProxyFormValues, { editing: editingRow ?? editing });
    await onSubmit(spec);
  });

  const renderField = (rule: FieldRule) => {
    if (!visible(rule)) return null;
    const disabled = disabledFor(rule);
    const wide = rule.input === 'textarea' || rule.input === 'header-pairs';

    if (rule.input === 'checkbox') {
      return (
        <FormField
          key={rule.name}
          control={form.control}
          name={rule.name}
          render={({ field }) => (
            <FormItem>
              <div className="flex items-center gap-1">
                <FormLabel>{rule.label}</FormLabel>
                <InfoLabel iconOnly title={rule.label} infoKey={infoKeyFor(rule.name)} />
              </div>
              <div className="flex h-9 items-center">
                <FormControl>
                  <Switch
                    checked={Boolean(field.value)}
                    disabled={disabled}
                    onCheckedChange={(checked) => field.onChange(checked)}
                    aria-label={rule.label}
                  />
                </FormControl>
              </div>
              {rule.magic ? <p className="text-xs text-muted-foreground">{rule.magic}</p> : null}
              <FormMessage />
            </FormItem>
          )}
        />
      );
    }

    if (rule.input === 'select') {
      const options =
        rule.name === 'client_id'
          ? clients.map((client) => ({ label: client.name, value: client.id }))
          : rule.name === 'kind'
            ? (rule.options ?? []).filter((opt) => kinds.includes(opt.value as ProxyKind))
            : (rule.options ?? []);
      return (
        <FormField
          key={rule.name}
          control={form.control}
          name={rule.name}
          render={({ field }) => (
            <FormItem>
              <div className="flex items-center gap-1">
                <FormLabel>{rule.label}</FormLabel>
                <InfoLabel iconOnly title={rule.label} infoKey={infoKeyFor(rule.name)} />
              </div>
              <Select
                value={String(field.value ?? '')}
                disabled={disabled}
                onValueChange={(next) => field.onChange(next)}
              >
                <FormControl>
                  <SelectTrigger className="w-full">
                    <SelectValue placeholder={rule.placeholder ?? '请选择'} />
                  </SelectTrigger>
                </FormControl>
                <SelectContent>
                  {options.map((option) => (
                    <SelectItem key={option.value} value={option.value}>
                      {option.label}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
              {rule.magic ? <p className="text-xs text-muted-foreground">{rule.magic}</p> : null}
              <FormMessage />
            </FormItem>
          )}
        />
      );
    }

    if (rule.input === 'header-pairs') {
      return (
        <FormField
          key={rule.name}
          control={form.control}
          name={rule.name}
          render={({ field }) => (
            <FormItem className="sm:col-span-2">
              <div className="flex items-center gap-1 text-sm font-medium">
                <span>{rule.label}</span>
                <InfoLabel iconOnly title={rule.label} infoKey={infoKeyFor(rule.name)} />
              </div>
              <HeaderPairsInput
                value={(field.value ?? []) as Array<{ name: string; value: string }>}
                disabled={disabled}
                onChange={(next) => field.onChange(next)}
              />
              {rule.magic ? <p className="text-xs text-muted-foreground">{rule.magic}</p> : null}
              <FormMessage />
            </FormItem>
          )}
        />
      );
    }

    const isPasswordField = rule.name === 'http_password';

    return (
      <FormField
        key={rule.name}
        control={form.control}
        name={rule.name}
        render={({ field }) => (
          <FormItem className={wide ? 'sm:col-span-2' : undefined}>
            <div className="flex items-center gap-1">
              <FormLabel>{rule.label}</FormLabel>
              <InfoLabel iconOnly title={rule.label} infoKey={infoKeyFor(rule.name)} />
            </div>
            <FormControl>
              {rule.input === 'number' ? (
                <NumberControl
                  value={field.value as number | null}
                  onChange={(next) => field.onChange(next)}
                  disabled={disabled}
                  min={rule.min}
                  max={rule.max}
                  placeholder={rule.placeholder}
                />
              ) : rule.input === 'textarea' ? (
                <Textarea
                  value={String(field.value ?? '')}
                  disabled={disabled}
                  placeholder={rule.placeholder}
                  onChange={(event) => field.onChange(event.target.value)}
                  rows={4}
                />
              ) : isPasswordField ? (
                <PasswordInput
                  value={String(field.value ?? '')}
                  disabled={disabled}
                  placeholder={rule.placeholder}
                  onChange={(event) => field.onChange(event.target.value)}
                />
              ) : (
                <Input
                  value={String(field.value ?? '')}
                  disabled={disabled}
                  placeholder={rule.placeholder}
                  onChange={(event) => field.onChange(event.target.value)}
                />
              )}
            </FormControl>
            {rule.name === 'target_port' && !disabled ? (
              <div className="flex flex-wrap items-center gap-1 pt-0.5">
                {[
                  { label: '22 SSH', port: 22 },
                  { label: '80 Web', port: 80 },
                  { label: '443 TLS', port: 443 },
                  { label: '3306 MySQL', port: 3306 },
                  { label: '5432 PG', port: 5432 },
                  { label: '6379 Redis', port: 6379 },
                  { label: '8080 App', port: 8080 },
                ].map((preset) => (
                  <button
                    key={preset.port}
                    type="button"
                    onClick={() => field.onChange(preset.port)}
                    className="rh-mono rounded-md border border-border/70 bg-muted/40 px-1.5 py-0.5 text-[10px] text-muted-foreground transition-colors hover:border-primary/40 hover:bg-primary/10 hover:text-primary"
                  >
                    {preset.label}
                  </button>
                ))}
              </div>
            ) : null}
            {rule.name === 'bandwidth_limit' && !disabled ? (
              <div className="flex flex-wrap items-center gap-1 pt-0.5">
                {[
                  { label: '不限', val: '' },
                  { label: '1MB', val: '1MB' },
                  { label: '5MB', val: '5MB' },
                  { label: '10MB', val: '10MB' },
                  { label: '50MB', val: '50MB' },
                ].map((preset) => (
                  <button
                    key={preset.label}
                    type="button"
                    onClick={() => field.onChange(preset.val)}
                    className="rh-mono rounded-md border border-border/70 bg-muted/40 px-1.5 py-0.5 text-[10px] text-muted-foreground transition-colors hover:border-primary/40 hover:bg-primary/10 hover:text-primary"
                  >
                    {preset.label}
                  </button>
                ))}
              </div>
            ) : null}
            {rule.magic ? <p className="text-xs text-muted-foreground">{rule.magic}</p> : null}
            <FormMessage />
          </FormItem>
        )}
      />
    );
  };

  const renderPluginField = (def: PluginFieldDef) => {
    const isPassword = def.name.endsWith('password');
    return (
      <FormField
        key={def.name}
        control={form.control}
        name={def.name}
        render={({ field }) => (
          <FormItem className={def.type === 'pairs' ? 'sm:col-span-2' : undefined}>
            <FormLabel>
              {def.label}
              {def.required ? <span className="ml-1 text-destructive">*</span> : null}
            </FormLabel>
            <FormControl>
              {def.type === 'pairs' ? (
                <HeaderPairsInput
                  value={(field.value ?? []) as Array<{ name: string; value: string }>}
                  onChange={(next) => field.onChange(next)}
                />
              ) : isPassword ? (
                <PasswordInput
                  value={String(field.value ?? '')}
                  placeholder={def.placeholder}
                  onChange={(event) => field.onChange(event.target.value)}
                />
              ) : (
                <Input
                  value={String(field.value ?? '')}
                  placeholder={def.placeholder}
                  onChange={(event) => field.onChange(event.target.value)}
                />
              )}
            </FormControl>
            <FormMessage />
          </FormItem>
        )}
      />
    );
  };

  const section = (group: FieldGroupKey) => {
    const rules = KIND_MATRIX[kind].fields.filter((rule) => rule.group === group && visible(rule));
    if (rules.length === 0) return null;
    return (
      <section key={group} className="space-y-4 rounded-xl border border-border/80 bg-card/60 p-4">
        <div className="flex items-center gap-3">
          <h3 className="text-sm font-semibold text-foreground">{GROUP_LABELS[group]}</h3>
          <Separator className="flex-1" />
        </div>
        <div className="grid gap-4 sm:grid-cols-2">{rules.map(renderField)}</div>
      </section>
    );
  };

  const pluginFields = pluginKind !== 'none' ? PLUGIN_FIELDS[pluginKind] : null;

  return (
    <Sheet open={open} onOpenChange={onOpenChange}>
      <SheetContent
        side="right"
        className="flex w-full flex-col gap-0 p-0 sm:max-w-2xl"
        onOpenAutoFocus={focusFirstControl}
      >
        <SheetHeader className="border-b border-border/80 bg-muted/30 px-4 py-3.5 pr-11 sm:px-6 sm:py-4">
          <div className="flex items-center gap-2 text-xs font-medium text-primary">
            <Waypoints className="size-3.5" />
            <span>TUNNEL TOPOLOGY INSPECTOR</span>
          </div>
          <SheetTitle className="text-base sm:text-lg">
            {editing
              ? `编辑转发规则: ${editing.name}`
              : `新建 ${KIND_MATRIX[kind]?.label ?? KIND_MATRIX[defaultKind].label} 转发规则`}
          </SheetTitle>
          <SheetDescription className="text-xs sm:text-sm">
            配置公网监听入口、承载客户端节点与内网转发终点，保存后实时下发同步。
          </SheetDescription>

          {/* 实时链路拓扑预览条 */}
          <div className="mt-2 flex flex-wrap items-center justify-between gap-2 rounded-xl border border-primary/20 bg-background/90 px-3 py-2 text-xs shadow-2xs sm:grid sm:grid-cols-[1fr_auto_1fr_auto_1fr] sm:py-2.5">
            <div className="min-w-0">
              <div className="text-[10px] text-muted-foreground">公网入口</div>
              <Mono className="mt-0.5 block truncate font-semibold text-foreground">
                {previewIngress}
              </Mono>
            </div>
            <div className="flex items-center gap-1 px-1 text-primary sm:flex-col sm:gap-0">
              <span className="rh-mono rounded bg-primary/10 px-1.5 py-0.5 text-[10px] font-bold uppercase">
                {kind}
              </span>
              <ArrowRight className="size-3.5 sm:mt-0.5" />
            </div>
            <div className="min-w-0 rounded-lg border border-border/80 bg-muted/40 px-2.5 py-1">
              <div className="flex items-center gap-1 text-[10px] text-muted-foreground">
                <Server className="size-3 text-primary" />
                <span>承载节点</span>
              </div>
              <div className="truncate font-medium text-foreground">
                {selectedClient ? selectedClient.name : '未选择节点'}
              </div>
            </div>
            <ArrowRight className="size-3.5 text-muted-foreground max-sm:hidden" />
            <div className="min-w-0 text-right max-sm:w-full max-sm:border-t max-sm:border-border/50 max-sm:pt-1.5 max-sm:text-left">
              <div className="text-[10px] text-muted-foreground">内网终点</div>
              <Mono className="mt-0.5 block truncate font-semibold text-emerald-600 dark:text-emerald-400">
                {previewTarget}
              </Mono>
            </div>
          </div>
        </SheetHeader>

        <div className="flex-1 overflow-y-auto px-4 py-4 sm:px-6 sm:py-5">
          {editing && !editingRow ? (
            <div className="flex items-center justify-center gap-2 py-16 text-sm text-muted-foreground">
              <Loader2 className="size-4 animate-spin" />
              正在读取规则完整配置…
            </div>
          ) : (
            <Form {...form}>
              <form onSubmit={handleSubmit} className="space-y-4" id="proxy-form">
                {groups
                  .filter((group) => !ADVANCED_GROUPS.includes(group) && group !== 'plugin')
                  .map(section)}

                <section className="space-y-4 rounded-xl border border-border/80 bg-card/60 p-3.5 sm:p-4">
                  <div className="flex items-center gap-3">
                    <h3 className="text-sm font-semibold">{GROUP_LABELS.plugin}</h3>
                    <Separator className="flex-1" />
                  </div>
                  <div className="grid gap-4 sm:grid-cols-2">
                    {KIND_MATRIX[kind].fields
                      .filter((rule) => rule.group === 'plugin')
                      .map(renderField)}
                  </div>
                  {pluginFields ? (
                    <div className="space-y-4 rounded-lg border border-dashed border-primary/30 bg-primary/5 p-3.5">
                      <p className="text-xs font-medium text-primary">
                        <InfoLabel title="插件参数" infoKey="service.plugin_params" />
                      </p>
                      <div className="grid gap-4 sm:grid-cols-2">
                        {pluginFields.map(renderPluginField)}
                      </div>
                    </div>
                  ) : null}
                </section>

                <Collapsible
                  open={advancedOpen}
                  onOpenChange={setAdvancedOpen}
                  className="rounded-xl border border-border/80 bg-muted/20 p-3"
                >
                  <div className="flex items-center justify-between gap-2">
                    <div className="flex min-w-0 items-center gap-1">
                      <CollapsibleTrigger asChild>
                        <Button
                          type="button"
                          variant="ghost"
                          size="sm"
                          className="h-auto min-w-0 gap-1.5 px-2 py-1 text-left font-medium"
                        >
                          <span className="truncate">
                            高级配置
                            <span className="max-sm:hidden">
                              {' '}
                              (连接池 / 限速 / 健康检查 / 增强策略)
                            </span>
                          </span>
                          <ChevronDown
                            className={`size-4 shrink-0 transition-transform ${advancedOpen ? 'rotate-180' : ''}`}
                          />
                        </Button>
                      </CollapsibleTrigger>
                      <InfoLabel iconOnly title="高级配置" infoKey="service.advanced" />
                    </div>
                    <span className="shrink-0 text-xs text-muted-foreground">
                      {advancedOpen ? '已展开' : '按需配置'}
                    </span>
                  </div>
                  <CollapsibleContent className="space-y-4 pt-4">
                    {groups.filter((group) => ADVANCED_GROUPS.includes(group)).map(section)}
                  </CollapsibleContent>
                </Collapsible>
              </form>
            </Form>
          )}
        </div>

        <SheetFooter className="flex-row items-center justify-end gap-2 border-t border-border/80 bg-card/90 px-4 py-3.5 sm:px-6 sm:py-4">
          <Button type="button" variant="outline" onClick={() => onOpenChange(false)}>
            取消
          </Button>
          <Button
            type="submit"
            form="proxy-form"
            disabled={submitting || (editing !== null && editingRow === null)}
          >
            {submitting ? <Loader2 className="size-4 animate-spin" /> : null}
            {editing ? '保存变更' : '创建规则'}
          </Button>
        </SheetFooter>
      </SheetContent>
    </Sheet>
  );
}
