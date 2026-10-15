import { zodResolver } from '@hookform/resolvers/zod';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { ChevronDown, KeyRound, Loader2, Plus, RefreshCw, Trash2, Users } from 'lucide-react';
import { useState } from 'react';
import { useForm } from 'react-hook-form';
import { z } from 'zod';

import {
  createClient,
  deleteClient,
  listClients,
  rotateClientSecret,
  updateClient,
} from '@/api/rabbithole';
import type { ClientRow } from '@/api/rabbithole';
import { ConfirmDialog } from '@/components/confirm-dialog';
import { DataTable, DataTableColumnHeader } from '@/components/data-table';
import type { RhColumnDef } from '@/components/data-table';
import { EmptyState } from '@/components/empty-state';
import { InfoLabel } from '@/components/info-label';
import { Mono, MonoCopy } from '@/components/mono';
import { PageHeader } from '@/components/page-header';
import { RowActionsMenu } from '@/components/row-actions';
import { SecretModal } from '@/components/secret-modal';
import { StatusBadge } from '@/components/status-badge';
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
import { MenubarItem } from '@/components/ui/menubar';
import {
  Form,
  FormControl,
  FormField,
  FormItem,
  FormLabel,
  FormMessage,
} from '@/components/ui/form';
import { Input } from '@/components/ui/input';
import { Switch } from '@/components/ui/switch';
import { Textarea } from '@/components/ui/textarea';
import { Tooltip, TooltipContent, TooltipTrigger } from '@/components/ui/tooltip';
import type { FieldInfoKey } from '@/features/field-info';
import { focusFirstControl } from '@/lib/dialog-focus';
import { feedbackSuccess } from '@/lib/feedback';
import { bytesToMb, fmtBytes, fmtRelative, mbToBytes } from '@/lib/format';
import { qk } from '@/lib/query-keys';
import { cn } from '@/lib/utils';

/** 表单字段名与实际 payload 的对应:flow_limit_mb(界面 MB)→ flow_limit(后端字节)。 */
const clientSchema = z.object({
  name: z.string().min(1, '请输入名称').max(64, '最多 64 字符'),
  remark: z.string(),
  enabled: z.boolean(),
  rate_limit: z.string(),
  max_conns: z.number().min(0, '不能为负数').nullable(),
  max_services: z.number().min(0, '不能为负数').nullable(),
  flow_limit_mb: z.number().min(0, '不能为负数').nullable(),
});

type ClientForm = z.infer<typeof clientSchema>;

/** 配额输入字段(空 / 0 = 不限;max_services 空 / 0 = 用全局默认)。 */
type QuotaField = 'flow_limit_mb' | 'max_conns' | 'max_services';

/**
 * 客户端管理:列表 / 新建(一次性客户端凭据)/ 编辑 / 轮换客户端密钥 / 启停 / 删除。
 * 行为基线 = 旧版 console 客户端页,计量与配额字段(M1/M2 已上线)直接渲染,不做整列降级。
 */
export default function ClientsPage() {
  const queryClient = useQueryClient();
  const [formOpen, setFormOpen] = useState(false);
  const [editing, setEditing] = useState<ClientRow | null>(null);
  const [expandedId, setExpandedId] = useState<string | null>(null);
  const [advancedOpen, setAdvancedOpen] = useState(false);
  const [secretState, setSecretState] = useState<{
    clientId?: string;
    open: boolean;
    secret: string;
  }>({ open: false, secret: '' });
  // 行操作收进 ⋯ 下拉后,确认框改为受控:由菜单项设置目标行
  const [rotateTarget, setRotateTarget] = useState<ClientRow | null>(null);
  const [deleteTarget, setDeleteTarget] = useState<ClientRow | null>(null);

  const form = useForm<ClientForm>({
    resolver: zodResolver(clientSchema),
    defaultValues: {
      name: '',
      remark: '',
      enabled: true,
      rate_limit: '',
      max_conns: null,
      max_services: null,
      flow_limit_mb: null,
    },
  });

  const clientsQuery = useQuery({ queryKey: qk.clients, queryFn: listClients });
  const rows = clientsQuery.data ?? [];

  const invalidateClients = () => {
    void queryClient.invalidateQueries({ queryKey: qk.clients });
  };

  const createMut = useMutation({
    mutationFn: (values: ClientForm) =>
      createClient({
        name: values.name,
        remark: values.remark,
        rate_limit: values.rate_limit.trim() || undefined,
        max_conns: values.max_conns || undefined,
        max_services: values.max_services || undefined,
        flow_limit: values.flow_limit_mb ? mbToBytes(values.flow_limit_mb) : undefined,
      }),
    onSuccess: (result) => {
      feedbackSuccess('客户端已创建');
      setFormOpen(false);
      // secret 仅此一次返回:先弹一次性凭据弹窗,关闭即不可再看
      setSecretState({ clientId: result.id, open: true, secret: result.secret });
      invalidateClients();
    },
  });

  const updateMut = useMutation({
    mutationFn: ({ id, values }: { id: string; values: ClientForm }) =>
      updateClient(id, {
        name: values.name,
        remark: values.remark,
        enabled: values.enabled,
        rate_limit: values.rate_limit,
        max_conns: values.max_conns || undefined,
        max_services: values.max_services || undefined,
        flow_limit: values.flow_limit_mb ? mbToBytes(values.flow_limit_mb) : undefined,
      }),
    onSuccess: () => {
      feedbackSuccess('已保存');
      setFormOpen(false);
      invalidateClients();
    },
  });

  const toggleMut = useMutation({
    mutationFn: ({ row, enabled }: { enabled: boolean; row: ClientRow }) =>
      updateClient(row.id, { enabled }),
    onSuccess: invalidateClients,
  });

  const rotateMut = useMutation({
    mutationFn: (id: string) => rotateClientSecret(id),
    onSuccess: (result, id) => {
      setSecretState({ clientId: id, open: true, secret: result.secret });
      feedbackSuccess('客户端密钥已轮换,新客户端密钥由服务端生成,无需其他操作');
      invalidateClients();
    },
  });

  const deleteMut = useMutation({
    mutationFn: (id: string) => deleteClient(id),
    onSuccess: () => {
      feedbackSuccess('已删除');
      invalidateClients();
    },
  });

  const saving = createMut.isPending || updateMut.isPending;

  function openCreate() {
    setEditing(null);
    setAdvancedOpen(false);
    form.reset({
      name: '',
      remark: '',
      enabled: true,
      rate_limit: '',
      max_conns: null,
      max_services: null,
      flow_limit_mb: null,
    });
    setFormOpen(true);
  }

  function openEdit(row: ClientRow) {
    setEditing(row);
    setAdvancedOpen(false);
    form.reset({
      name: row.name,
      remark: row.remark,
      enabled: row.enabled,
      rate_limit: row.rate_limit ?? '',
      max_conns: row.max_conns ?? null,
      max_services: row.max_services ?? null,
      // 后端存字节,界面按 MB 回填
      flow_limit_mb: bytesToMb(row.flow_limit),
    });
    setFormOpen(true);
  }

  const onSubmit = form.handleSubmit((values) => {
    if (editing) {
      updateMut.mutate({ id: editing.id, values });
    } else {
      createMut.mutate(values);
    }
  });

  /** 在线徽章:离线附最近在线相对时间(悬停给全量口径)。 */
  function renderStatus(row: ClientRow) {
    if (row.online) {
      return <StatusBadge signal="ok" text="在线" />;
    }
    return (
      <Tooltip>
        <TooltipTrigger asChild>
          <span>
            <StatusBadge signal="off" text={`离线 · ${fmtRelative(row.last_online_at)}`} />
          </span>
        </TooltipTrigger>
        <TooltipContent>
          {row.last_online_at ? `最近在线 ${fmtRelative(row.last_online_at)}` : '从未上线'}
        </TooltipContent>
      </Tooltip>
    );
  }

  /**
   * 启用开关(即时生效)。乐观受控:PUT 完成到列表重取完成之间开关即反映目标态,
   * 防快速双击发出重复反向请求。
   */
  function renderEnabledSwitch(row: ClientRow) {
    const pending = toggleMut.isPending && toggleMut.variables?.row.id === row.id;
    const checked = pending ? (toggleMut.variables?.enabled ?? row.enabled) : row.enabled;
    return (
      <Switch
        size="sm"
        aria-label={`${row.name} 启用`}
        checked={checked}
        disabled={pending}
        onCheckedChange={(next) => toggleMut.mutate({ row, enabled: next })}
      />
    );
  }

  /** 行详情:连接 / 流量 / 规则上限 / 限速(旧版行展开内容)。 */
  function renderRowDetail(row: ClientRow) {
    return (
      <div className="space-y-1.5">
        <div className="flex items-center justify-between gap-3">
          <span className="text-muted-foreground">连接 / 并发上限</span>
          <Mono>{`${row.conns_current ?? 0}${row.max_conns ? ` / ${row.max_conns}` : ''}`}</Mono>
        </div>
        <div className="flex items-center justify-between gap-3">
          <InfoLabel title="流量(入 / 出)" infoKey="client.detail.bytes" />
          <Mono>{`${fmtBytes(row.bytes_in)} / ${fmtBytes(row.bytes_out)}`}</Mono>
        </div>
        <div className="flex items-center justify-between gap-3">
          <span className="text-muted-foreground">规则数上限</span>
          <Mono>{row.max_services || '不限'}</Mono>
        </div>
        <div className="flex items-center justify-between gap-3">
          <span className="text-muted-foreground">限速</span>
          <Mono>{row.rate_limit || '不限'}</Mono>
        </div>
      </div>
    );
  }

  /**
   * 行操作:编辑 / 轮换客户端密钥 / 删除(桌面列与移动卡片共用)。
   * 收敛为「⋯」下拉:触发器悬停该行才显现(小屏常显),确认框由菜单项受控打开。
   */
  function renderRowActions(row: ClientRow) {
    return (
      <div className="flex items-center justify-end gap-1">
        <Button type="button" variant="ghost" size="sm" onClick={() => openEdit(row)}>
          编辑
        </Button>
        <RowActionsMenu label={`${row.name} 的操作`}>
          <MenubarItem onSelect={() => setRotateTarget(row)}>
            <KeyRound className="size-4" aria-hidden="true" />
            轮换客户端密钥
          </MenubarItem>
          <MenubarItem
            className="text-destructive focus:bg-destructive/10 focus:text-destructive"
            onSelect={() => setDeleteTarget(row)}
          >
            <Trash2 className="size-4" aria-hidden="true" />
            删除
          </MenubarItem>
        </RowActionsMenu>
      </div>
    );
  }

  const columns: RhColumnDef<ClientRow>[] = [
    {
      accessorKey: 'name',
      meta: { label: '名称' },
      header: ({ column }) => <DataTableColumnHeader column={column} title="名称" />,
      cell: ({ row }) => {
        const client = row.original;
        const open = expandedId === client.id;
        return (
          <div className="space-y-2">
            <div className="flex items-center gap-1">
              <Button
                type="button"
                variant="ghost"
                size="icon-xs"
                aria-expanded={open}
                aria-label={open ? `收起 ${client.name} 详情` : `展开 ${client.name} 详情`}
                onClick={() => setExpandedId(open ? null : client.id)}
              >
                <ChevronDown
                  aria-hidden="true"
                  className={cn('size-3.5 transition-transform', !open && '-rotate-90')}
                />
              </Button>
              <span className="font-medium whitespace-nowrap">{client.name}</span>
            </div>
            <Collapsible open={open}>
              <CollapsibleContent className="ml-6 w-64 rounded-md border bg-muted/30 p-2.5 text-xs">
                {renderRowDetail(client)}
              </CollapsibleContent>
            </Collapsible>
          </div>
        );
      },
    },
    {
      accessorKey: 'id',
      meta: { label: '客户端 ID' },
      header: ({ column }) => (
        <DataTableColumnHeader
          column={column}
          title={<InfoLabel title="ID" infoKey="client.id" />}
        />
      ),
      cell: ({ row }) => (
        <MonoCopy
          full={row.original.id}
          display={`…${row.original.id.slice(-8)}`}
          ariaLabel="复制客户端 ID"
        />
      ),
    },
    {
      accessorKey: 'online',
      meta: { label: '在线' },
      header: ({ column }) => (
        <DataTableColumnHeader
          column={column}
          title={<InfoLabel title="在线" infoKey="client.online" />}
        />
      ),
      cell: ({ row }) => renderStatus(row.original),
    },
    {
      accessorKey: 'enabled',
      meta: { label: '启用' },
      header: ({ column }) => <DataTableColumnHeader column={column} title="启用" />,
      cell: ({ row }) => renderEnabledSwitch(row.original),
    },
    {
      id: 'meta',
      meta: { label: '版本 / 主机' },
      accessorFn: (row) => `${row.client_version || '-'} ${row.hostname || '-'}`,
      header: ({ column }) => (
        <DataTableColumnHeader
          column={column}
          title={<InfoLabel title="版本 / 主机" infoKey="client.version_host" />}
        />
      ),
      cell: ({ row }) => (
        <Mono>{`${row.original.client_version || '-'} / ${row.original.hostname || '-'}`}</Mono>
      ),
    },
    {
      id: 'bytes',
      meta: { label: '流量(入/出)' },
      accessorFn: (row) => row.bytes_in ?? 0,
      header: ({ column }) => (
        <DataTableColumnHeader
          column={column}
          title={<InfoLabel title="流量(入/出)" infoKey="client.bytes" />}
        />
      ),
      cell: ({ row }) => (
        <Mono>{`${fmtBytes(row.original.bytes_in)} / ${fmtBytes(row.original.bytes_out)}`}</Mono>
      ),
    },
    {
      id: 'conns',
      meta: { label: '连接' },
      accessorFn: (row) => row.conns_current ?? 0,
      header: ({ column }) => (
        <DataTableColumnHeader
          column={column}
          title={<InfoLabel title="连接" infoKey="client.conns" />}
        />
      ),
      cell: ({ row }) => {
        const client = row.original;
        return (
          <Mono>
            {client.max_conns
              ? `${client.conns_current ?? 0}/${client.max_conns}`
              : `${client.conns_current ?? 0}`}
          </Mono>
        );
      },
    },
    {
      accessorKey: 'remark',
      meta: { label: '备注' },
      header: ({ column }) => <DataTableColumnHeader column={column} title="备注" />,
      cell: ({ row }) =>
        row.original.remark ? (
          <Tooltip>
            <TooltipTrigger asChild>
              <span className="block max-w-64 truncate">{row.original.remark}</span>
            </TooltipTrigger>
            <TooltipContent className="max-w-96 break-all">{row.original.remark}</TooltipContent>
          </Tooltip>
        ) : (
          <span className="text-muted-foreground">-</span>
        ),
    },
    {
      id: 'action',
      meta: { label: '操作', headerClassName: 'w-[1%]' },
      enableHiding: false,
      header: '操作',
      cell: ({ row }) => renderRowActions(row.original),
    },
  ];

  /** 配额数字输入(空 = 不限,不用魔法值)。 */
  function renderQuotaField(name: QuotaField, label: string, infoKey: FieldInfoKey) {
    return (
      <FormField
        control={form.control}
        name={name}
        render={({ field }) => (
          <FormItem>
            <FormLabel>
              <InfoLabel title={label} infoKey={infoKey} />
            </FormLabel>
            <FormControl>
              <Input
                ref={field.ref}
                name={field.name}
                type="number"
                inputMode="numeric"
                min={0}
                step={1}
                placeholder="0 = 不限"
                value={field.value ?? ''}
                onBlur={field.onBlur}
                onChange={(event) => {
                  const raw = event.target.value;
                  field.onChange(raw === '' ? null : Number(raw));
                }}
              />
            </FormControl>
            <FormMessage />
          </FormItem>
        )}
      />
    );
  }

  return (
    <div className="space-y-4">
      <PageHeader title="客户端" description="接入 rabbithole 的边缘客户端" />

      <DataTable
        columns={columns}
        data={rows}
        loading={clientsQuery.isLoading}
        getRowId={(row) => row.id}
        initialPageSize={50}
        toolbar={
          <>
            <Button type="button" variant="outline" size="sm" onClick={invalidateClients}>
              <RefreshCw className="size-4" aria-hidden="true" />
              刷新
            </Button>
            <Button type="button" size="sm" onClick={openCreate}>
              <Plus className="size-4" aria-hidden="true" />
              新建客户端
            </Button>
          </>
        }
        empty={
          <EmptyState
            icon={<Users className="size-6" aria-hidden="true" />}
            title="还没有客户端"
            description="创建客户端后,控制台会给出接入配置,客户端进程上线即可承载转发规则。"
            action={
              <Button type="button" onClick={openCreate}>
                <Plus className="size-4" aria-hidden="true" />
                新建客户端
              </Button>
            }
          />
        }
        mobileRow={(row) => (
          <div className="space-y-3">
            <div className="flex items-start justify-between gap-3">
              <div className="min-w-0 space-y-1">
                <div className="truncate font-medium">{row.name}</div>
                <MonoCopy
                  full={row.id}
                  display={`…${row.id.slice(-8)}`}
                  ariaLabel="复制客户端 ID"
                />
              </div>
              {renderEnabledSwitch(row)}
            </div>
            <div className="flex flex-wrap items-center gap-x-4 gap-y-1 text-xs text-muted-foreground">
              {renderStatus(row)}
              <Mono>{`${row.client_version || '-'} / ${row.hostname || '-'}`}</Mono>
            </div>
            {renderRowDetail(row)}
            {row.remark ? (
              <p className="text-xs leading-relaxed text-muted-foreground">{row.remark}</p>
            ) : null}
            {renderRowActions(row)}
          </div>
        )}
      />

      <Dialog
        open={formOpen}
        onOpenChange={(next) => {
          if (!next && !saving) {
            setFormOpen(false);
          }
        }}
      >
        <DialogContent className="sm:max-w-xl" onOpenAutoFocus={focusFirstControl}>
          <DialogHeader>
            <DialogTitle>{editing ? '编辑客户端' : '新建客户端'}</DialogTitle>
            <DialogDescription>
              {editing
                ? '修改名称、备注、启用状态与配额;保存即生效。'
                : '创建后服务端一次性返回客户端凭据,请立即复制保存。'}
            </DialogDescription>
          </DialogHeader>

          <Form {...form}>
            <form onSubmit={onSubmit} className="space-y-4">
              <FormField
                control={form.control}
                name="name"
                render={({ field }) => (
                  <FormItem>
                    <FormLabel>
                      <InfoLabel title="名称" infoKey="client.form.name" />
                    </FormLabel>
                    <FormControl>
                      <Input placeholder="edge-1" {...field} />
                    </FormControl>
                    <FormMessage />
                  </FormItem>
                )}
              />

              <FormField
                control={form.control}
                name="remark"
                render={({ field }) => (
                  <FormItem>
                    <FormLabel>
                      <InfoLabel title="备注" infoKey="client.form.remark" />
                    </FormLabel>
                    <FormControl>
                      <Textarea rows={2} placeholder="可选" {...field} />
                    </FormControl>
                    <FormMessage />
                  </FormItem>
                )}
              />

              {editing ? (
                <FormField
                  control={form.control}
                  name="enabled"
                  render={({ field }) => (
                    <FormItem className="flex flex-row items-center justify-between gap-4 rounded-md border p-3">
                      <div className="space-y-1">
                        <FormLabel>启用</FormLabel>
                        <p className="text-xs text-muted-foreground">
                          关闭后客户端会话断开,其转发规则不再承载流量。
                        </p>
                      </div>
                      <FormControl>
                        <Switch
                          checked={field.value}
                          onCheckedChange={field.onChange}
                          aria-label="启用"
                        />
                      </FormControl>
                    </FormItem>
                  )}
                />
              ) : null}

              <Collapsible
                open={advancedOpen}
                onOpenChange={setAdvancedOpen}
                className="rounded-md border"
              >
                <div className="flex items-center justify-between gap-2 p-2">
                  <InfoLabel
                    title="高级配置"
                    infoKey="client.form.advanced"
                    className="px-1 text-sm font-medium"
                  />
                  <CollapsibleTrigger asChild>
                    <Button type="button" variant="ghost" size="sm" aria-expanded={advancedOpen}>
                      {advancedOpen ? '收起' : '展开'}
                      <ChevronDown
                        aria-hidden="true"
                        className={cn('size-4 transition-transform', advancedOpen && 'rotate-180')}
                      />
                    </Button>
                  </CollapsibleTrigger>
                </div>
                <CollapsibleContent className="space-y-4 border-t p-3">
                  <FormField
                    control={form.control}
                    name="rate_limit"
                    render={({ field }) => (
                      <FormItem>
                        <FormLabel>
                          <InfoLabel title="限速" infoKey="client.rate_limit" />
                        </FormLabel>
                        <FormControl>
                          <Input placeholder="如 1MB / 512KB,留空不限" {...field} />
                        </FormControl>
                        <FormMessage />
                      </FormItem>
                    )}
                  />
                  {renderQuotaField('max_conns', '最大连接', 'client.max_conns')}
                  {renderQuotaField('max_services', '最大规则数', 'client.max_services')}
                  {renderQuotaField('flow_limit_mb', '流量上限(MB)', 'client.flow_limit_mb')}
                </CollapsibleContent>
              </Collapsible>

              <DialogFooter>
                <Button
                  type="button"
                  variant="outline"
                  disabled={saving}
                  onClick={() => setFormOpen(false)}
                >
                  取消
                </Button>
                <Button type="submit" disabled={saving}>
                  {saving ? <Loader2 className="size-4 animate-spin" aria-hidden="true" /> : null}
                  保存
                </Button>
              </DialogFooter>
            </form>
          </Form>
        </DialogContent>
      </Dialog>

      <SecretModal
        open={secretState.open}
        clientId={secretState.clientId}
        secret={secretState.secret}
        onOpenChange={(open) => {
          // 关闭即不可再看:连同 secret 一起清掉,不存在重新打开的路径
          if (!open) {
            setSecretState({ open: false, secret: '' });
          }
        }}
      />

      {/* 行操作的二次确认:全页各一份,由 ⋯ 菜单项设置目标行后受控打开 */}
      <ConfirmDialog
        destructive
        open={rotateTarget !== null}
        onOpenChange={(open) => {
          if (!open) setRotateTarget(null);
        }}
        confirmText="确认轮换"
        title={`轮换 ${rotateTarget?.name ?? ''} 的客户端密钥?`}
        description="服务端将生成新的随机客户端密钥;旧客户端密钥立即失效,客户端需更新配置后重连。"
        onConfirm={async () => {
          if (!rotateTarget) return;
          try {
            await rotateMut.mutateAsync(rotateTarget.id);
          } catch {
            // 失败提示由响应拦截器统一弹出
          }
        }}
      />
      <ConfirmDialog
        destructive
        open={deleteTarget !== null}
        onOpenChange={(open) => {
          if (!open) setDeleteTarget(null);
        }}
        confirmText="删除"
        title={`删除客户端 ${deleteTarget?.name ?? ''}?`}
        description="此操作不可恢复,该客户端下的转发规则将失效。"
        onConfirm={async () => {
          if (!deleteTarget) return;
          try {
            await deleteMut.mutateAsync(deleteTarget.id);
          } catch {
            // 失败提示由响应拦截器统一弹出
          }
        }}
      />
    </div>
  );
}
