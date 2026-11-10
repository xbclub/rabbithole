import { zodResolver } from '@hookform/resolvers/zod';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { ArrowRight, Cable, Plus, Server } from 'lucide-react';
import { useEffect, useRef, useState } from 'react';
import type { ReactElement } from 'react';
import { useForm, useWatch } from 'react-hook-form';
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
import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetFooter,
  SheetHeader,
  SheetTitle,
} from '@/components/ui/sheet';
import { Switch } from '@/components/ui/switch';
import { focusFirstControl } from '@/lib/dialog-focus';
import { feedbackSuccess } from '@/lib/feedback';
import { qk } from '@/lib/query-keys';

export interface VisitorSeed {
  kind: 'stcp' | 'sudp' | 'xtcp';
  target: string;
}

type VisitorKind = VisitorSeed['kind'];

const VISITOR_KINDS: readonly string[] = ['stcp', 'sudp', 'xtcp'];

const KIND_OPTIONS: Array<{ label: string; value: VisitorKind }> = [
  { label: 'STCP (TCP 密钥中继)', value: 'stcp' },
  { label: 'SUDP (UDP 密钥中继)', value: 'sudp' },
  { label: 'XTCP (P2P NAT 打洞直连)', value: 'xtcp' },
];

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
  const seedRef = useRef<VisitorSeed | null>(null);

  const visitorsQ = useQuery({ queryKey: qk.visitors(), queryFn: () => listVisitors() });
  const clientsQ = useQuery({ queryKey: qk.clients, queryFn: listClients });
  const servicesQ = useQuery({ queryKey: qk.services(), queryFn: () => listServices() });

  const clients: ClientRow[] = clientsQ.data ?? [];
  const rows: VisitorRow[] = visitorsQ.data ?? [];
  const clientMap = new Map(clients.map((c) => [c.id, c.name]));

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

  useEffect(() => {
    if (!seed) return;
    seedRef.current = seed;
    // eslint-disable-next-line react-hooks/set-state-in-effect
    setEditId(null);
    setModalOpen(true);
  }, [seed]);

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

  const watchKind = useWatch({ control: form.control, name: 'kind' });
  const watchClientId = useWatch({ control: form.control, name: 'client_id' });
  const watchBindAddr = useWatch({ control: form.control, name: 'bind_addr' });
  const watchBindPort = useWatch({ control: form.control, name: 'bind_port' });
  const watchTarget = useWatch({ control: form.control, name: 'target_proxy_name' });

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
            // 错误由拦截器弹出
          }
        }}
      />
    </div>
  );

  const columns: RhColumnDef<VisitorRow>[] = [
    {
      accessorKey: 'name',
      meta: { label: '名称' },
      enableHiding: false,
      header: () => <InfoLabel title="名称" infoKey="visitor.name" />,
      cell: ({ row }) => (
        <div className="flex items-center gap-2">
          <span className="rh-mono rounded bg-primary/10 px-1.5 py-0.5 text-[10px] font-bold text-primary uppercase">
            {row.original.kind}
          </span>
          <span className="font-medium">{row.original.name}</span>
        </div>
      ),
    },
    {
      id: 'client',
      meta: { label: '发起节点' },
      header: '发起节点',
      cell: ({ row }) => (
        <span className="inline-flex items-center gap-1.5 text-xs text-muted-foreground">
          <Server className="size-3.5 text-primary/70" />
          <span className="font-medium text-foreground">
            {clientMap.get(row.original.client_id) ?? row.original.client_id.slice(0, 8)}
          </span>
        </span>
      ),
    },
    {
      id: 'pipeline',
      meta: { label: '本地入口 ──▶ 目标代理' },
      enableHiding: false,
      header: () => <InfoLabel title="本地监听 ──▶ 目标代理" infoKey="visitor.bind" />,
      cell: ({ row }) => (
        <div className="inline-flex items-center gap-2 rounded-lg border border-border/70 bg-muted/30 px-2.5 py-1 text-xs">
          <Mono className="font-medium">{`${row.original.bind_addr}:${row.original.bind_port}`}</Mono>
          <ArrowRight className="size-3.5 text-primary" />
          <Mono className="font-semibold text-emerald-600 dark:text-emerald-400">
            {row.original.target_proxy_name}
          </Mono>
        </div>
      ),
    },
    {
      id: 'compress',
      meta: { label: '压缩' },
      enableHiding: false,
      header: () => <InfoLabel title="压缩" infoKey="visitor.compression" />,
      cell: ({ row }) => (
        <Badge variant={row.original.spec?.use_compression ? 'secondary' : 'outline'}>
          {row.original.spec?.use_compression ? 'Snappy' : '否'}
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
          <Button type="button" size="sm" onClick={openCreate}>
            <Plus className="size-4" aria-hidden="true" />
            新建访问器
          </Button>
        }
        mobileRow={mobileRow}
        empty={
          <EmptyState
            icon={<Cable className="size-6" aria-hidden="true" />}
            title="还没有访问器"
            description="访问器在访问方节点打开本地监听端口，经服务端中继或 P2P 打洞直达私网密钥规则目标。"
            action={
              <Button type="button" onClick={openCreate}>
                <Plus className="size-4" aria-hidden="true" />
                新建访问器
              </Button>
            }
          />
        }
      />

      <Sheet open={modalOpen} onOpenChange={setModalOpen}>
        <SheetContent
          side="right"
          className="flex w-full flex-col gap-0 p-0 sm:max-w-lg"
          onOpenAutoFocus={focusFirstControl}
        >
          <SheetHeader className="border-b border-border/80 bg-muted/30 px-4 py-3.5 pr-11 sm:px-6 sm:py-4">
            <div className="flex items-center gap-2 text-xs font-medium text-primary">
              <Cable className="size-3.5" />
              <span>VISITOR ENDPOINT INSPECTOR</span>
            </div>
            <SheetTitle>{editId !== null ? '编辑访问器' : '新建访问器'}</SheetTitle>
            <SheetDescription className="text-xs sm:text-sm">
              访问器密钥无需手动填写：连接时自动从服务端继承目标私网规则的当前密钥。
            </SheetDescription>

            {/* 实时访问器拓扑预览 */}
            <div className="mt-2 flex flex-wrap items-center justify-between gap-2 rounded-xl border border-primary/20 bg-background/90 px-3 py-2 text-xs sm:grid sm:grid-cols-[1fr_auto_1fr_auto_1fr] sm:py-2.5">
              <div className="min-w-0">
                <div className="text-[10px] text-muted-foreground">本地绑定</div>
                <Mono className="mt-0.5 block truncate font-semibold">
                  {`${watchBindAddr || '127.0.0.1'}:${watchBindPort ?? '?'}`}
                </Mono>
              </div>
              <div className="flex items-center gap-1 px-1 text-primary sm:flex-col sm:gap-0">
                <span className="rh-mono rounded bg-primary/10 px-1.5 py-0.5 text-[10px] font-bold uppercase">
                  {watchKind}
                </span>
                <ArrowRight className="size-3.5 sm:mt-0.5" />
              </div>
              <div className="min-w-0 rounded-lg border border-border/80 bg-muted/40 px-2 py-1">
                <div className="text-[10px] text-muted-foreground">发起节点</div>
                <div className="truncate font-medium">
                  {clientMap.get(watchClientId) ?? '未选择'}
                </div>
              </div>
              <ArrowRight className="size-3.5 text-muted-foreground max-sm:hidden" />
              <div className="min-w-0 text-right max-sm:w-full max-sm:border-t max-sm:border-border/50 max-sm:pt-1.5 max-sm:text-left">
                <div className="text-[10px] text-muted-foreground">目标私网规则</div>
                <Mono className="mt-0.5 block truncate font-semibold text-emerald-600 dark:text-emerald-400">
                  {watchTarget || '待选择'}
                </Mono>
              </div>
            </div>
          </SheetHeader>

          <div className="flex-1 overflow-y-auto px-4 py-4 sm:px-6 sm:py-5">
            <Form {...form}>
              <form id="visitor-form" onSubmit={form.handleSubmit(onSubmit)} className="space-y-4">
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

                <div className="grid gap-4 sm:grid-cols-2">
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
                </div>

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
                          暂无 STCP/SUDP/XTCP 规则,请先在「私网密钥规则」页创建
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
                      <div className="flex items-center gap-2 rounded-lg border border-border/70 p-3">
                        <FormControl>
                          <Checkbox
                            checked={field.value}
                            onCheckedChange={(checked) => field.onChange(checked === true)}
                          />
                        </FormControl>
                        <span className="text-sm">启用 snappy 数据流压缩</span>
                      </div>
                      <FormMessage />
                    </FormItem>
                  )}
                />
              </form>
            </Form>
          </div>

          <SheetFooter className="flex-row items-center justify-end gap-2 border-t border-border/80 bg-card/90 px-6 py-4">
            <Button
              type="button"
              variant="outline"
              disabled={saveMut.isPending}
              onClick={() => setModalOpen(false)}
            >
              取消
            </Button>
            <Button type="submit" form="visitor-form" disabled={saveMut.isPending}>
              {saveMut.isPending ? '保存中…' : '保存'}
            </Button>
          </SheetFooter>
        </SheetContent>
      </Sheet>
    </div>
  );
}
