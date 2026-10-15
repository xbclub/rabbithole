import { zodResolver } from '@hookform/resolvers/zod';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { Cable, Plus } from 'lucide-react';
import { useEffect, useRef, useState } from 'react';
import type { ReactElement } from 'react';
import { useForm } from 'react-hook-form';
import { z } from 'zod';

import {
  createVisitor,
  deleteVisitor,
  disableVisitor,
  enableVisitor,
  listClients,
  listServices,
  listVisitors,
  updateVisitor,
} from '@/api/rabbithole';
import type { ClientRow, VisitorRow } from '@/api/rabbithole';
import { ConfirmDialog } from '@/components/confirm-dialog';
import { DataTable } from '@/components/data-table';
import type { RhColumnDef } from '@/components/data-table';
import { EmptyState } from '@/components/empty-state';
import { InfoLabel } from '@/components/info-label';
import { Mono } from '@/components/mono';
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
import { Switch } from '@/components/ui/switch';
import { focusFirstControl } from '@/lib/dialog-focus';
import { feedbackSuccess } from '@/lib/feedback';
import { qk } from '@/lib/query-keys';

/** 从规则列表「创建访问器」带入的预填(目标规则 + 类型)。 */
export interface VisitorSeed {
  kind: 'stcp' | 'sudp' | 'xtcp';
  target: string;
}

/** 访问器类型:密钥访问的三种子协议。 */
type VisitorKind = VisitorSeed['kind'];

/** 可作为访问器目标的规则类型(与服务端「名称 + 类型」的匹配口径一致)。 */
const VISITOR_KINDS: readonly string[] = ['stcp', 'sudp', 'xtcp'];

/** 类型下拉选项(创建后不可改,故仅在新建时可选)。 */
const KIND_OPTIONS: Array<{ label: string; value: VisitorKind }> = [
  { label: 'STCP', value: 'stcp' },
  { label: 'SUDP', value: 'sudp' },
  { label: 'XTCP', value: 'xtcp' },
];

/**
 * 表单校验:口径与旧版 antd rules 一致(必填 + 名称字符集 + 端口范围)。
 * 密钥字段不存在 —— 访问器密钥留空即由服务端在连接时继承目标规则的当前密钥(R23/R24)。
 */
const visitorSchema = z.object({
  client_id: z.string().min(1, '请选择客户端'),
  name: z
    .string()
    .min(1, '请输入名称')
    .regex(/^[a-zA-Z0-9_-]{1,64}$/, '1-64 位字母/数字/下划线/连字符'),
  kind: z.enum(['stcp', 'sudp', 'xtcp']),
  bind_addr: z.string(),
  bind_port: z
    .number({ error: '请输入端口' })
    .int('端口须为整数')
    .min(1, '端口范围 1-65535')
    .max(65535, '端口范围 1-65535'),
  target_proxy_name: z.string().min(1, '请选择目标代理'),
  use_compression: z.boolean(),
});

type VisitorFormValues = z.infer<typeof visitorSchema>;

/** 新建表单初值(等价旧版 setFieldsValue;seed 带入时预填类型与目标)。 */
function createDefaults(seed: VisitorSeed | null, clients: ClientRow[]): VisitorFormValues {
  return {
    client_id: clients[0]?.id ?? '',
    name: '',
    kind: seed?.kind ?? 'stcp',
    bind_addr: '127.0.0.1',
    bind_port: 6000,
    target_proxy_name: seed?.target ?? '',
    use_compression: false,
  };
}

/** 编辑回填(逐字段等价旧版 setFieldsValue;密钥不在表单,故不读取 spec.secret_key)。 */
function rowToValues(row: VisitorRow): VisitorFormValues {
  return {
    client_id: row.client_id,
    name: row.name,
    kind: row.kind as VisitorKind,
    bind_addr: row.bind_addr,
    bind_port: row.bind_port,
    target_proxy_name: row.target_proxy_name,
    use_compression: Boolean(row.spec?.use_compression),
  };
}

/**
 * 密钥访问页「访问器」面板:STCP/SUDP/XTCP 访问端规则的 CRUD 与启停。
 *
 * 密钥不在表单出现:访问器密钥留空 = 连接时继承目标转发规则当前密钥(服务端判定),
 * 因此表单没有密钥字段、列表没有密钥列。`seed` 由规则列表行内「创建访问器」带入,
 * 经 ref 一次性消费后回调 `onSeedUsed`,避免父级清空 seed 触发表单重置。
 */
export function VisitorsPanel({
  seed,
  onSeedUsed,
}: {
  seed?: VisitorSeed | null;
  onSeedUsed?: () => void;
}): ReactElement {
  const queryClient = useQueryClient();
  const [modalOpen, setModalOpen] = useState(false);
  const [editId, setEditId] = useState<null | number>(null);
  const form = useForm<VisitorFormValues>({
    resolver: zodResolver(visitorSchema),
    defaultValues: createDefaults(null, []),
  });
  // 经 ref 读取一次性 seed:父级清空 seed 时不应触发表单重置
  const seedRef = useRef<VisitorSeed | null>(null);

  const visitorsQ = useQuery({ queryKey: qk.visitors(), queryFn: () => listVisitors() });
  const clientsQ = useQuery({ queryKey: qk.clients, queryFn: listClients });
  const servicesQ = useQuery({ queryKey: qk.services(), queryFn: () => listServices() });

  const clients: ClientRow[] = clientsQ.data ?? [];
  const rows: VisitorRow[] = visitorsQ.data ?? [];

  // 目标代理候选:同类型(stcp/sudp/xtcp)规则名去重后按名称排序
  const targetOptions = Array.from(
    new Set(
      (servicesQ.data ?? [])
        .filter((rule) => VISITOR_KINDS.includes(rule.kind))
        .map((rule) => rule.name),
    ),
  )
    .sort()
    .map((name) => ({ label: name, value: name }));

  const editRow = editId === null ? null : (rows.find((row) => row.id === editId) ?? null);
  // 编辑中的目标规则若已不在候选里(规则被删或改名),仍作为选项展示,避免回填值不可见
  const targetSelectOptions =
    editRow &&
    editRow.target_proxy_name &&
    !targetOptions.some((o) => o.value === editRow.target_proxy_name)
      ? [{ label: editRow.target_proxy_name, value: editRow.target_proxy_name }, ...targetOptions]
      : targetOptions;

  const invalidate = () => {
    void queryClient.invalidateQueries({ queryKey: qk.visitors() });
    void queryClient.invalidateQueries({ queryKey: qk.clients });
  };

  const saveMut = useMutation({
    mutationFn: ({ id, values }: { id: null | number; values: VisitorFormValues }) => {
      // bind_port 必须是 number(后端 serde 拒绝字符串);留空按旧版回退 0
      const spec = {
        name: values.name,
        kind: values.kind,
        enabled: id !== null ? (rows.find((row) => row.id === id)?.enabled ?? true) : true,
        bind_addr: values.bind_addr,
        bind_port: values.bind_port ?? 0,
        target_proxy_name: values.target_proxy_name,
        use_compression: Boolean(values.use_compression),
        ...(id !== null ? {} : { client_id: values.client_id }),
      };
      return id !== null ? updateVisitor(id, spec) : createVisitor(spec);
    },
    onSuccess: (_data, variables) => {
      feedbackSuccess(variables.id !== null ? '已保存' : '已创建');
      setModalOpen(false);
      invalidate();
    },
  });

  const toggleMut = useMutation({
    mutationFn: ({ enabled, row }: { enabled: boolean; row: VisitorRow }) =>
      enabled ? enableVisitor(row.id) : disableVisitor(row.id),
    onSuccess: invalidate,
  });

  const deleteMut = useMutation({
    mutationFn: (id: number) => deleteVisitor(id),
    onSuccess: () => {
      feedbackSuccess('已删除');
      invalidate();
    },
  });

  // 规则页「创建访问器」跳入:记下目标与类型,打开新建弹窗。
  // 这是 React 官方的「响应 props 变化调整状态」场景,只能放在 effect 里;
  // set-state-in-effect 在此为误报,按旧版一次性 seed 语义显式放行。
  /* eslint-disable react-hooks/set-state-in-effect -- 响应 seed prop 变化打开弹窗,与旧版 effect 语义一致 */
  useEffect(() => {
    if (!seed) return;
    seedRef.current = seed;
    setEditId(null);
    setModalOpen(true);
  }, [seed]);
  /* eslint-enable react-hooks/set-state-in-effect */

  // 弹窗打开时重置表单并按模式回填;新建模式在此消费一次性 seed(故依赖里不含 seed)
  useEffect(() => {
    if (!modalOpen) return;
    if (editId !== null) {
      const row = rows.find((item) => item.id === editId);
      form.reset(row ? rowToValues(row) : createDefaults(null, clients));
    } else {
      const pending = seedRef.current;
      form.reset(createDefaults(pending, clients));
      if (pending) {
        seedRef.current = null;
        onSeedUsed?.();
      }
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [modalOpen, editId]);

  const openCreate = () => {
    setEditId(null);
    setModalOpen(true);
  };

  const openEdit = (id: number) => {
    setEditId(id);
    setModalOpen(true);
  };

  const onSubmit = (values: VisitorFormValues) => {
    saveMut.mutate({ id: editId, values });
  };

  /** 启停开关:请求未落定期间保持乐观值并禁用,落定后回到服务端值。 */
  const renderEnabledSwitch = (row: VisitorRow) => {
    const pending = toggleMut.isPending && toggleMut.variables?.row.id === row.id;
    const checked = pending ? (toggleMut.variables?.enabled ?? row.enabled) : row.enabled;
    return (
      <Switch
        size="sm"
        checked={checked}
        disabled={pending}
        aria-label={`启用访问器 ${row.name}`}
        onCheckedChange={(next) => toggleMut.mutate({ row, enabled: next })}
      />
    );
  };

  /** 行内操作:编辑 + 删除(删除走二次确认,危险语义)。 */
  const renderActions = (row: VisitorRow) => (
    <div className="flex items-center gap-1">
      <Button type="button" variant="ghost" size="sm" onClick={() => openEdit(row.id)}>
        编辑
      </Button>
      <ConfirmDialog
        trigger={
          <Button
            type="button"
            variant="ghost"
            size="sm"
            className="text-destructive hover:text-destructive"
          >
            删除
          </Button>
        }
        title={`删除访问器 ${row.name}?`}
        description="此操作不可恢复。"
        confirmText="删除"
        destructive
        onConfirm={async () => {
          try {
            await deleteMut.mutateAsync(row.id);
          } catch {
            // 失败提示由响应拦截器统一弹出,此处仅避免确认浮层误关
          }
        }}
      />
    </div>
  );

  // 表头带 InfoLabel(名词 + ℹ 说明),故不使用可排序表头:DataTableColumnHeader 的
  // 排序按钮会与 InfoLabel 的浮层触发按钮形成嵌套 button 并互相抢点击。
  const columns: RhColumnDef<VisitorRow>[] = [
    {
      accessorKey: 'name',
      meta: { label: '名称' },
      enableHiding: false,
      header: () => <InfoLabel title="名称" infoKey="visitor.name" />,
      cell: ({ row }) => <span className="font-medium">{row.original.name}</span>,
    },
    {
      accessorKey: 'kind',
      meta: { label: '类型' },
      enableHiding: false,
      header: () => <InfoLabel title="类型" infoKey="visitor.kind" />,
      cell: ({ row }) => <Badge variant="outline">{row.original.kind.toUpperCase()}</Badge>,
    },
    {
      id: 'bind',
      meta: { label: '本地监听' },
      enableHiding: false,
      header: () => <InfoLabel title="本地监听" infoKey="visitor.bind" />,
      cell: ({ row }) => <Mono>{`${row.original.bind_addr}:${row.original.bind_port}`}</Mono>,
    },
    {
      accessorKey: 'target_proxy_name',
      meta: { label: '目标代理' },
      enableHiding: false,
      header: () => <InfoLabel title="目标代理" infoKey="visitor.target_proxy_name" />,
    },
    {
      id: 'compress',
      meta: { label: '压缩' },
      enableHiding: false,
      header: () => <InfoLabel title="压缩" infoKey="visitor.compression" />,
      cell: ({ row }) => (
        <Badge variant={row.original.spec?.use_compression ? 'secondary' : 'outline'}>
          {row.original.spec?.use_compression ? '是' : '否'}
        </Badge>
      ),
    },
    {
      id: 'enabled',
      meta: { label: '启用' },
      enableHiding: false,
      header: '启用',
      cell: ({ row }) => renderEnabledSwitch(row.original),
    },
    {
      id: 'action',
      meta: { label: '操作', headerClassName: 'w-[1%]' },
      enableHiding: false,
      header: '操作',
      cell: ({ row }) => renderActions(row.original),
    },
  ];

  /** <768px 表格降级为卡片列表(设计规范 §4.6)。 */
  const mobileRow = (row: VisitorRow) => (
    <div className="space-y-2">
      <div className="flex items-center justify-between gap-2">
        <span className="font-medium">{row.name}</span>
        <Badge variant="outline">{row.kind.toUpperCase()}</Badge>
      </div>
      <div className="flex flex-wrap items-center gap-x-4 gap-y-1 text-sm text-muted-foreground">
        <span>
          本地监听 <Mono>{`${row.bind_addr}:${row.bind_port}`}</Mono>
        </span>
        <span>
          目标代理 <span className="text-foreground">{row.target_proxy_name}</span>
        </span>
        <span>
          压缩 <span className="text-foreground">{row.spec?.use_compression ? '是' : '否'}</span>
        </span>
      </div>
      <div className="flex items-center justify-between gap-2">
        {renderEnabledSwitch(row)}
        {renderActions(row)}
      </div>
    </div>
  );

  return (
    <div className="space-y-4">
      <DataTable
        columns={columns}
        data={rows}
        loading={visitorsQ.isLoading}
        initialPageSize={50}
        getRowId={(row) => String(row.id)}
        toolbar={
          <Button type="button" onClick={openCreate}>
            <Plus className="size-4" aria-hidden="true" />
            新建访问器
          </Button>
        }
        mobileRow={mobileRow}
        empty={
          <EmptyState
            icon={<Cable className="size-6" aria-hidden="true" />}
            title="还没有访问器"
            description="访问器在另一台机器上打开本地端口,经服务端中继或打洞直达密钥规则的目标。"
            action={
              <Button type="button" onClick={openCreate}>
                <Plus className="size-4" aria-hidden="true" />
                新建访问器
              </Button>
            }
          />
        }
      />

      <Dialog open={modalOpen} onOpenChange={setModalOpen}>
        <DialogContent className="sm:max-w-lg" onOpenAutoFocus={focusFirstControl}>
          <DialogHeader>
            <DialogTitle>{editId !== null ? '编辑访问器' : '新建访问器'}</DialogTitle>
            <DialogDescription>
              访问器密钥不在这里填写:留空即由服务端在连接时继承目标转发规则的当前密钥。
            </DialogDescription>
          </DialogHeader>
          <Form {...form}>
            <form onSubmit={form.handleSubmit(onSubmit)} className="space-y-4">
              <FormField
                control={form.control}
                name="client_id"
                render={({ field }) => (
                  <FormItem>
                    <FormLabel>
                      <InfoLabel title="所属客户端" infoKey="visitor.form.client_id" />
                    </FormLabel>
                    <Select
                      value={field.value}
                      onValueChange={field.onChange}
                      disabled={editId !== null}
                    >
                      <FormControl>
                        <SelectTrigger className="w-full">
                          <SelectValue placeholder="选择客户端" />
                        </SelectTrigger>
                      </FormControl>
                      <SelectContent>
                        {clients.map((client) => (
                          <SelectItem key={client.id} value={client.id}>
                            {client.name}
                          </SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                    <FormMessage />
                  </FormItem>
                )}
              />

              <FormField
                control={form.control}
                name="name"
                render={({ field }) => (
                  <FormItem>
                    <FormLabel>
                      <InfoLabel title="名称" infoKey="visitor.form.name" />
                    </FormLabel>
                    <FormControl>
                      <Input {...field} />
                    </FormControl>
                    <FormMessage />
                  </FormItem>
                )}
              />

              <FormField
                control={form.control}
                name="kind"
                render={({ field }) => (
                  <FormItem>
                    <FormLabel>
                      <InfoLabel title="类型" infoKey="visitor.form.kind" />
                    </FormLabel>
                    <Select
                      value={field.value}
                      onValueChange={field.onChange}
                      disabled={editId !== null}
                    >
                      <FormControl>
                        <SelectTrigger className="w-full">
                          <SelectValue />
                        </SelectTrigger>
                      </FormControl>
                      <SelectContent>
                        {KIND_OPTIONS.map((option) => (
                          <SelectItem key={option.value} value={option.value}>
                            {option.label}
                          </SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                    <FormMessage />
                  </FormItem>
                )}
              />

              <FormField
                control={form.control}
                name="bind_addr"
                render={({ field }) => (
                  <FormItem>
                    <FormLabel>
                      <InfoLabel title="本地监听地址" infoKey="visitor.form.bind_addr" />
                    </FormLabel>
                    <FormControl>
                      <Input {...field} placeholder="127.0.0.1" />
                    </FormControl>
                    <FormMessage />
                  </FormItem>
                )}
              />

              <FormField
                control={form.control}
                name="bind_port"
                render={({ field }) => (
                  <FormItem>
                    <FormLabel>
                      <InfoLabel title="本地监听端口" infoKey="visitor.form.bind_port" />
                    </FormLabel>
                    <FormControl>
                      {/* 数值字段:提交给后端的必须是 number,空输入保持 undefined 交给校验 */}
                      <Input
                        type="number"
                        min={1}
                        max={65535}
                        step={1}
                        inputMode="numeric"
                        name={field.name}
                        ref={field.ref}
                        onBlur={field.onBlur}
                        value={Number.isFinite(field.value) ? String(field.value) : ''}
                        onChange={(event) => {
                          const raw = event.target.value;
                          field.onChange(raw === '' ? undefined : Number(raw));
                        }}
                      />
                    </FormControl>
                    <FormMessage />
                  </FormItem>
                )}
              />

              <FormField
                control={form.control}
                name="target_proxy_name"
                render={({ field }) => (
                  <FormItem>
                    <FormLabel>
                      <InfoLabel title="目标代理名" infoKey="visitor.form.target_proxy_name" />
                    </FormLabel>
                    <Select
                      value={field.value}
                      onValueChange={field.onChange}
                      disabled={targetSelectOptions.length === 0}
                    >
                      <FormControl>
                        <SelectTrigger className="w-full">
                          <SelectValue placeholder="选择目标规则" />
                        </SelectTrigger>
                      </FormControl>
                      <SelectContent>
                        {targetSelectOptions.map((option) => (
                          <SelectItem key={option.value} value={option.value}>
                            {option.label}
                          </SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                    {targetOptions.length === 0 ? (
                      <p className="text-xs text-muted-foreground">
                        暂无 STCP/SUDP/XTCP 规则,请先在「规则」页创建
                      </p>
                    ) : null}
                    <FormMessage />
                  </FormItem>
                )}
              />

              <FormField
                control={form.control}
                name="use_compression"
                render={({ field }) => (
                  <FormItem>
                    <FormLabel>
                      <InfoLabel title="压缩" infoKey="visitor.compression" />
                    </FormLabel>
                    <div className="flex items-center gap-2">
                      <FormControl>
                        <Checkbox
                          checked={field.value}
                          onCheckedChange={(checked) => field.onChange(checked === true)}
                        />
                      </FormControl>
                      <span className="text-sm">启用 snappy 压缩</span>
                    </div>
                    <FormMessage />
                  </FormItem>
                )}
              />

              <DialogFooter>
                <Button
                  type="button"
                  variant="outline"
                  disabled={saveMut.isPending}
                  onClick={() => setModalOpen(false)}
                >
                  取消
                </Button>
                <Button type="submit" disabled={saveMut.isPending}>
                  {saveMut.isPending ? '保存中…' : '保存'}
                </Button>
              </DialogFooter>
            </form>
          </Form>
        </DialogContent>
      </Dialog>
    </div>
  );
}
