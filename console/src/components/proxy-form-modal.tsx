import { zodResolver } from '@hookform/resolvers/zod';
import { ChevronDown, Loader2 } from 'lucide-react';
import { useState } from 'react';
import { useForm } from 'react-hook-form';
import { z } from 'zod';

import type { ClientRow, ServiceRow } from '@/api/rabbithole';
import { HeaderPairsInput } from '@/components/header-pairs-input';
import { InfoLabel } from '@/components/info-label';
import { Button } from '@/components/ui/button';
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from '@/components/ui/collapsible';
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
import { Separator } from '@/components/ui/separator';
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

/**
 * 表单形状与格式校验。可见性相关的必填检查放在 superRefine —— 它只看表单值,
 * 不依赖编辑态,因此 resolver 在整个弹窗生命周期内稳定。
 * 依赖编辑态的业务校验(域名/端口)由 validateServiceForm 在提交时补充。
 */
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

/** 编辑回读:自动分配的端口(0)在界面上表达为开关打开,而不是魔法值 0。 */
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
      // FormControl 通过 Slot 注入 id/aria-*;自定义组件必须继续透传,否则
      // <label htmlFor> 指不到输入框(与 Select 那次是同一类问题的不同形态)
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
  /** 页内允许的 kind;多于一个时表单顶部显示类型选择器。 */
  kinds: ProxyKind[];
  defaultKind: ProxyKind;
  editing: ServiceRow | null;
  clients: ClientRow[];
  submitting?: boolean;
  onSubmit: (spec: ServiceSpecPayload) => Promise<void> | void;
}

/**
 * 新建/编辑转发规则弹窗。
 *
 * 字段矩阵不写死在这里,而是由 @/features/services/matrix 的声明式矩阵驱动渲染
 * (可见性/必填/魔法值/分组全部来自矩阵);组装与回读走 @/features/services/spec,
 * 后端契约零变更。
 *
 * 密钥(stcp/sudp/xtcp 的 secret_key)完全由服务端托管,表单零输入;
 * allow_users 已在 R32 删除,不出现。
 */
export function ProxyFormModal({
  open,
  onOpenChange,
  kinds,
  defaultKind,
  editing,
  clients,
  submitting = false,
  onSubmit,
}: ProxyFormModalProps) {
  const multiKind = kinds.length > 1;
  const defaultValues: ProxyFormValues = editing
    ? readbackValues(editing)
    : defaultProxyFormValues(defaultKind, clients[0]?.id ?? '');

  const form = useForm<FormSchemaValues>({
    resolver: zodResolver(formSchema),
    defaultValues,
  });
  const [advancedOpen, setAdvancedOpen] = useState(false);

  const kind = form.watch('kind') as ProxyKind;
  const pluginKind = form.watch('plugin_kind');
  const bindAuto = form.watch('bind_auto');
  const hcType = form.watch('hc_type');
  const groups = fieldGroupsOf(kind);

  const visible = (rule: FieldRule): boolean => {
    // 单 kind 页不显示类型选择器(多 kind 页才有意义)
    if (rule.name === 'kind' && !multiKind) return false;
    if (rule.createOnly && editing) return false;
    if (rule.createOnlyWhenManualBind && !editing && bindAuto) return false;
    if (rule.hiddenWhenPlugin && pluginKind !== 'none') return false;
    if (rule.visibleWhenHcType && hcType !== rule.visibleWhenHcType) return false;
    return true;
  };

  const disabledFor = (rule: FieldRule): boolean => {
    if (!editing) return false;
    // 编辑态锁定:所属客户端、类型、自动分配
    if (rule.name === 'client_id' || rule.name === 'kind' || rule.name === 'bind_auto') return true;
    return false;
  };

  const handleSubmit = form.handleSubmit(async (values) => {
    // 依赖编辑态的业务校验(域名语法 / 端口 / 域名必填),错误按字段回填
    const errors = validateServiceForm(values as ProxyFormValues, { editing });
    let applied = false;
    for (const error of errors) {
      if (form.getFieldState(error.field).error) continue;
      form.setError(error.field, { type: 'validate', message: error.message });
      applied = true;
    }
    if (applied) return;

    const spec = buildServiceSpec(values as ProxyFormValues, { editing });
    await onSubmit(spec);
  });

  const renderField = (rule: FieldRule) => {
    if (!visible(rule)) return null;
    const disabled = disabledFor(rule);
    const wide = rule.input === 'textarea' || rule.input === 'header-pairs';

    // 开关单独一支:FormControl 的直接子元素必须是控件本身,
    // 否则它的 Slot 会把 id/aria 挂到包装 div 上,<label htmlFor> 指不到开关。
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

    // 下拉单独一支:FormControl 必须放在 Select 内部包住 SelectTrigger。
    // Radix 的 Select.Root 不渲染 DOM,把它当 FormControl 的子元素会让 id 丢失,
    // <label htmlFor> 便指不到触发器(shadcn 文档的写法正是下面这样)。
    if (rule.input === 'select') {
      // client_id 的选项来自运行时客户端列表(矩阵里没有静态 options);
      // 其余下拉的选项都在矩阵里声明。
      const options =
        rule.name === 'client_id'
          ? clients.map((client) => ({ label: client.name, value: client.id }))
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

    // 键值对是复合编辑器,没有单一可关联的控件:不套 FormControl/FormLabel,
    // 否则 htmlFor 会指向不存在的 id(标签点不动任何东西)。
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

    return (
      <FormField
        key={rule.name}
        control={form.control}
        name={rule.name}
        render={({ field }) => (
          <FormItem className={wide ? 'sm:col-span-2' : undefined}>
            {/* ℹ 按钮放在 label 之外:label 内嵌 button 会让点按 ℹ 同时聚焦输入框 */}
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
              ) : (
                <Input
                  value={String(field.value ?? '')}
                  disabled={disabled}
                  placeholder={rule.placeholder}
                  onChange={(event) => field.onChange(event.target.value)}
                />
              )}
            </FormControl>
            {rule.magic ? <p className="text-xs text-muted-foreground">{rule.magic}</p> : null}
            <FormMessage />
          </FormItem>
        )}
      />
    );
  };

  const renderPluginField = (def: PluginFieldDef) => (
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

  const section = (group: FieldGroupKey) => {
    const rules = KIND_MATRIX[kind].fields.filter((rule) => rule.group === group && visible(rule));
    if (rules.length === 0) return null;
    return (
      // 分组用带边框的容器 + 标题分隔线,否则一长串字段看不出哪些属于同一组
      <section key={group} className="space-y-4 rounded-lg border p-4">
        <div className="flex items-center gap-3">
          <h3 className="text-sm font-semibold">{GROUP_LABELS[group]}</h3>
          <Separator className="flex-1" />
        </div>
        <div className="grid gap-4 sm:grid-cols-2">{rules.map(renderField)}</div>
      </section>
    );
  };

  const pluginFields = pluginKind !== 'none' ? PLUGIN_FIELDS[pluginKind] : null;

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent
        className="max-h-[90svh] overflow-y-auto sm:max-w-3xl"
        // 打开时聚焦第一个真实控件,而不是首个可聚焦元素(那通常是字段名旁的 ℹ 按钮)
        onOpenAutoFocus={focusFirstControl}
      >
        <DialogHeader>
          <DialogTitle>
            {editing
              ? `编辑转发规则:${editing.name}`
              : `新建${KIND_MATRIX[defaultKind].label}转发规则`}
          </DialogTitle>
          <DialogDescription>
            基础区只保留必填项;传输压缩、限速、负载均衡、健康检查与 HTTP 增强在「高级配置」里。
          </DialogDescription>
        </DialogHeader>

        <Form {...form}>
          <form onSubmit={handleSubmit} className="space-y-4" id="proxy-form">
            {groups
              .filter((group) => !ADVANCED_GROUPS.includes(group) && group !== 'plugin')
              .map(section)}

            <section className="space-y-4 rounded-lg border p-4">
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
                <div className="space-y-4 rounded-md border border-dashed p-3">
                  <p className="text-xs font-medium text-muted-foreground">
                    <InfoLabel title="插件参数" infoKey="service.plugin_params" />
                  </p>
                  <div className="grid gap-4 sm:grid-cols-2">
                    {pluginFields.map(renderPluginField)}
                  </div>
                </div>
              ) : null}
            </section>

            <Collapsible open={advancedOpen} onOpenChange={setAdvancedOpen}>
              <Separator />
              <div className="mt-2 flex items-center gap-1">
                <CollapsibleTrigger asChild>
                  <Button type="button" variant="ghost" className="gap-2">
                    高级配置
                    <ChevronDown
                      className={`size-4 transition-transform ${advancedOpen ? 'rotate-180' : ''}`}
                    />
                  </Button>
                </CollapsibleTrigger>
                <InfoLabel iconOnly title="高级配置" infoKey="service.advanced" />
              </div>
              <CollapsibleContent className="space-y-4 pt-4">
                {groups.filter((group) => ADVANCED_GROUPS.includes(group)).map(section)}
              </CollapsibleContent>
            </Collapsible>
          </form>
        </Form>

        <DialogFooter>
          <Button type="button" variant="outline" onClick={() => onOpenChange(false)}>
            取消
          </Button>
          <Button type="submit" form="proxy-form" disabled={submitting}>
            {submitting ? <Loader2 className="size-4 animate-spin" /> : null}
            {editing ? '保存' : '创建规则'}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
