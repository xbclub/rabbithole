import { zodResolver } from '@hookform/resolvers/zod';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import {
  ArrowRight,
  ChevronDown,
  KeyRound,
  LayoutGrid,
  LayoutList,
  Loader2,
  Plus,
  RefreshCw,
  Search,
  Server,
  Trash2,
  Users,
  Waypoints,
} from 'lucide-react';
import { useMemo, useState } from 'react';
import { useForm } from 'react-hook-form';
import { Link, useNavigate } from 'react-router';
import { z } from 'zod';

import {
  createClient,
  deleteClient,
  listClients,
  listServices,
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
import { Card, CardContent, CardHeader } from '@/components/ui/card';
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from '@/components/ui/collapsible';
import {
  Form,
  FormControl,
  FormField,
  FormItem,
  FormLabel,
  FormMessage,
} from '@/components/ui/form';
import { Input } from '@/components/ui/input';
import { MenubarItem } from '@/components/ui/menubar';
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
import { Tooltip, TooltipContent, TooltipTrigger } from '@/components/ui/tooltip';
import type { FieldInfoKey } from '@/features/field-info';
import { focusFirstControl } from '@/lib/dialog-focus';
import { feedbackSuccess } from '@/lib/feedback';
import { bytesToMb, fmtBytes, fmtRelative, mbToBytes } from '@/lib/format';
import { qk } from '@/lib/query-keys';
import { cn } from '@/lib/utils';
import { ROUTES } from '@/router/paths';

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
type QuotaField = 'flow_limit_mb' | 'max_conns' | 'max_services';

export default function ClientsPage() {
  const queryClient = useQueryClient();
  const navigate = useNavigate();

  const [formOpen, setFormOpen] = useState(false);
  const [editing, setEditing] = useState<ClientRow | null>(null);
  const [expandedId, setExpandedId] = useState<string | null>(null);
  const [advancedOpen, setAdvancedOpen] = useState(false);
  const [viewMode, setViewMode] = useState<'cards' | 'table'>('cards');
  const [onlineFilter, setOnlineFilter] = useState<'all' | 'online' | 'offline'>('all');
  const [searchQuery, setSearchQuery] = useState('');
  const [secretState, setSecretState] = useState<{
    clientId?: string;
    open: boolean;
    secret: string;
  }>({ open: false, secret: '' });
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
  const servicesQuery = useQuery({ queryKey: qk.services(), queryFn: () => listServices() });

  const rows = useMemo(() => clientsQuery.data ?? [], [clientsQuery.data]);
  const allServices = useMemo(() => servicesQuery.data ?? [], [servicesQuery.data]);

  // 统计每个客户端关联的隧道数与运行中隧道数
  const clientServiceStats = useMemo(() => {
    const map = new Map<string, { total: number; running: number }>();
    for (const s of allServices) {
      const cur = map.get(s.client_id) ?? { total: 0, running: 0 };
      cur.total += 1;
      if (s.status === 'running' && s.healthy) cur.running += 1;
      map.set(s.client_id, cur);
    }
    return map;
  }, [allServices]);

  const filteredRows = useMemo(() => {
    return rows.filter((r) => {
      if (onlineFilter === 'online' && !r.online) return false;
      if (onlineFilter === 'offline' && r.online) return false;
      if (searchQuery.trim()) {
        const q = searchQuery.trim().toLowerCase();
        return (
          r.name.toLowerCase().includes(q) ||
          r.id.toLowerCase().includes(q) ||
          (r.hostname ?? '').toLowerCase().includes(q) ||
          (r.remark ?? '').toLowerCase().includes(q)
        );
      }
      return true;
    });
  }, [rows, onlineFilter, searchQuery]);

  const onlineCount = useMemo(() => rows.filter((r) => r.online).length, [rows]);

  const invalidateClients = () => {
    void queryClient.invalidateQueries({ queryKey: qk.clients });
    void queryClient.invalidateQueries({ queryKey: ['services'] });
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

  function renderRowActions(row: ClientRow) {
    return (
      <div className="flex items-center justify-end gap-1">
        <Button type="button" variant="ghost" size="sm" onClick={() => openEdit(row)}>
          编辑
        </Button>
        <RowActionsMenu label={`${row.name} 的操作`}>
          <MenubarItem
            onSelect={() => navigate(`${ROUTES.tcp}?client=${encodeURIComponent(row.id)}`)}
          >
            <Waypoints className="size-4" aria-hidden="true" />
            管理该节点隧道
          </MenubarItem>
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
      id: 'tunnels',
      meta: { label: '关联隧道' },
      header: '关联隧道',
      cell: ({ row }) => {
        const st = clientServiceStats.get(row.original.id) ?? { total: 0, running: 0 };
        return (
          <Link
            to={`${ROUTES.tcp}?client=${encodeURIComponent(row.original.id)}`}
            className="inline-flex items-center gap-1.5 rounded-full border border-primary/25 bg-primary/8 px-2.5 py-0.5 text-xs font-medium text-primary hover:bg-primary/15"
          >
            <Mono>{`${st.running}/${st.total}`}</Mono>
            <span>条规则</span>
            <ArrowRight className="size-3" />
          </Link>
        );
      },
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

  const toolbarControls = (
    <div className="flex w-full flex-wrap items-center gap-2">
      <div className="relative w-full sm:w-64">
        <Search className="pointer-events-none absolute top-1/2 left-2.5 size-3.5 -translate-y-1/2 text-muted-foreground" />
        <Input
          value={searchQuery}
          onChange={(e) => setSearchQuery(e.target.value)}
          placeholder="搜索节点名、主机名或 ID…"
          className="h-8 pl-8 text-xs"
          aria-label="搜索客户端节点"
        />
      </div>

      <div className="grid min-w-48 flex-1 grid-cols-3 rounded-lg border border-border/80 bg-muted/40 p-0.5 text-xs sm:inline-flex sm:flex-initial">
        {(
          [
            { key: 'all', label: `全部 (${rows.length})` },
            { key: 'online', label: `在线 (${onlineCount})` },
            { key: 'offline', label: `离线 (${Math.max(0, rows.length - onlineCount)})` },
          ] as const
        ).map((item) => (
          <button
            key={item.key}
            type="button"
            onClick={() => setOnlineFilter(item.key)}
            className={cn(
              'rounded-md px-2 py-1 text-center text-xs font-medium transition-colors sm:px-2.5',
              onlineFilter === item.key
                ? 'bg-background text-foreground shadow-2xs'
                : 'text-muted-foreground hover:text-foreground',
            )}
          >
            {item.label}
          </button>
        ))}
      </div>

      <div className="inline-flex rounded-lg border border-border/80 bg-muted/40 p-0.5">
        <button
          type="button"
          onClick={() => setViewMode('cards')}
          title="节点拓扑卡片视图"
          className={cn(
            'inline-flex items-center gap-1 rounded-md px-2 py-1 text-xs font-medium transition-colors',
            viewMode === 'cards'
              ? 'bg-background text-foreground shadow-2xs'
              : 'text-muted-foreground hover:text-foreground',
          )}
        >
          <LayoutGrid className="size-3.5" />
          <span className="max-sm:hidden">节点卡</span>
        </button>
        <button
          type="button"
          onClick={() => setViewMode('table')}
          title="高密度表格视图"
          className={cn(
            'inline-flex items-center gap-1 rounded-md px-2 py-1 text-xs font-medium transition-colors',
            viewMode === 'table'
              ? 'bg-background text-foreground shadow-2xs'
              : 'text-muted-foreground hover:text-foreground',
          )}
        >
          <LayoutList className="size-3.5" />
          <span className="max-sm:hidden">表格</span>
        </button>
      </div>

      <Button type="button" variant="outline" size="sm" onClick={invalidateClients}>
        <RefreshCw className="size-4" aria-hidden="true" />
        <span className="max-sm:hidden">刷新</span>
      </Button>
      <Button type="button" size="sm" onClick={openCreate} className="ml-auto sm:ml-0">
        <Plus className="size-4" aria-hidden="true" />
        新建客户端
      </Button>
    </div>
  );

  const emptyStateNode = (
    <EmptyState
      icon={<Users className="size-6" aria-hidden="true" />}
      title={rows.length === 0 ? '还没有客户端' : '无匹配的客户端节点'}
      description={
        rows.length === 0
          ? '创建客户端后，控制台会生成一键安装脚本与 Docker 接入指令，客户端上线即可承载转发规则。'
          : '请尝试清空搜索词或切换在线状态筛选。'
      }
      action={
        <Button type="button" onClick={openCreate}>
          <Plus className="size-4" aria-hidden="true" />
          新建客户端
        </Button>
      }
    />
  );

  return (
    <div className="space-y-5">
      <PageHeader
        eyebrow="EDGE CLIENT NODES WORKBENCH"
        title="边缘节点管理"
        description="管理接入 rabbithole 控制面的内网客户端节点、连接配额、访问密钥轮换与挂载隧道"
      />

      {viewMode === 'cards' ? (
        <div className="space-y-4">
          <div className="flex flex-wrap items-center justify-between gap-2">{toolbarControls}</div>
          {filteredRows.length === 0 ? (
            <div className="rounded-2xl border border-border/80 bg-card p-8">{emptyStateNode}</div>
          ) : (
            <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
              {filteredRows.map((client) => {
                const st = clientServiceStats.get(client.id) ?? { total: 0, running: 0 };
                return (
                  <Card
                    key={client.id}
                    className="rh-telemetry-card flex flex-col justify-between gap-4 py-5"
                  >
                    <CardHeader className="pb-0">
                      <div className="flex items-start justify-between gap-3">
                        <div className="flex min-w-0 items-center gap-3">
                          <span
                            className={cn(
                              'flex size-10 shrink-0 items-center justify-center rounded-xl ring-1',
                              client.online
                                ? 'bg-emerald-500/10 text-emerald-600 ring-emerald-500/25 dark:text-emerald-400'
                                : 'bg-muted text-muted-foreground ring-border',
                            )}
                          >
                            <Server className="size-5" />
                          </span>
                          <div className="min-w-0">
                            <div className="flex items-center gap-2">
                              <span className="truncate text-base font-bold">{client.name}</span>
                              {renderStatus(client)}
                            </div>
                            <div className="mt-0.5 flex items-center gap-2 text-xs text-muted-foreground">
                              <span>ID:</span>
                              <MonoCopy
                                full={client.id}
                                display={`…${client.id.slice(-8)}`}
                                ariaLabel="复制客户端 ID"
                              />
                            </div>
                          </div>
                        </div>
                        <div className="flex items-center gap-1">{renderEnabledSwitch(client)}</div>
                      </div>
                    </CardHeader>

                    <CardContent className="space-y-3 pt-0">
                      {/* 主机与流量连接遥测 */}
                      <div className="grid grid-cols-2 gap-2 rounded-xl border border-border/70 bg-muted/30 p-3 text-xs">
                        <div>
                          <div className="text-[11px] text-muted-foreground">主机 / 版本</div>
                          <Mono className="mt-0.5 block truncate font-medium">
                            {client.hostname || '-'} · {client.client_version || '-'}
                          </Mono>
                        </div>
                        <div>
                          <div className="text-[11px] text-muted-foreground">活跃连接 / 上限</div>
                          <Mono className="mt-0.5 block font-medium">
                            {client.conns_current ?? 0}
                            {client.max_conns ? ` / ${client.max_conns}` : ' (不限)'}
                          </Mono>
                        </div>
                        <div>
                          <div className="text-[11px] text-muted-foreground">累计入站 / 出站</div>
                          <Mono className="mt-0.5 block truncate font-medium">
                            {fmtBytes(client.bytes_in)} / {fmtBytes(client.bytes_out)}
                          </Mono>
                        </div>
                        <div>
                          <div className="text-[11px] text-muted-foreground">限速 / 规则配额</div>
                          <Mono className="mt-0.5 block truncate font-medium">
                            {client.rate_limit || '不限速'} ·{' '}
                            {client.max_services ? `≤${client.max_services}条` : '不限'}
                          </Mono>
                        </div>
                      </div>

                      {client.remark ? (
                        <p className="line-clamp-2 text-xs text-muted-foreground">
                          备注: {client.remark}
                        </p>
                      ) : null}

                      {/* 底部操作与隧道联动条 */}
                      <div className="flex items-center justify-between gap-2 border-t border-border/60 pt-3">
                        <Link
                          to={`${ROUTES.tcp}?client=${encodeURIComponent(client.id)}`}
                          className="inline-flex items-center gap-1.5 rounded-lg border border-primary/25 bg-primary/8 px-2.5 py-1.5 text-xs font-medium text-primary transition-colors hover:bg-primary/15"
                        >
                          <Waypoints className="size-3.5" />
                          <span>
                            隧道{' '}
                            <Mono>
                              {st.running}/{st.total}
                            </Mono>
                          </span>
                          <ArrowRight className="size-3" />
                        </Link>
                        {renderRowActions(client)}
                      </div>
                    </CardContent>
                  </Card>
                );
              })}
            </div>
          )}
        </div>
      ) : (
        <DataTable
          columns={columns}
          data={filteredRows}
          loading={clientsQuery.isLoading}
          getRowId={(row) => row.id}
          initialPageSize={50}
          toolbar={toolbarControls}
          empty={emptyStateNode}
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
      )}

      {/* 右侧滑出式客户端配置抽屉 */}
      <Sheet
        open={formOpen}
        onOpenChange={(next) => {
          if (!next && !saving) {
            setFormOpen(false);
          }
        }}
      >
        <SheetContent
          side="right"
          className="flex w-full flex-col gap-0 p-0 sm:max-w-lg"
          onOpenAutoFocus={focusFirstControl}
        >
          <SheetHeader className="border-b border-border/80 bg-muted/30 px-4 py-3.5 pr-11 sm:px-6 sm:py-4">
            <div className="flex items-center gap-2 text-xs font-medium text-primary">
              <Server className="size-3.5" />
              <span>EDGE CLIENT NODE INSPECTOR</span>
            </div>
            <SheetTitle>{editing ? '编辑客户端' : '新建客户端'}</SheetTitle>
            <SheetDescription className="text-xs sm:text-sm">
              {editing
                ? '修改客户端名称、备注、启用状态与资源配额；保存后立即热生效。'
                : '创建后服务端将一次性生成客户端密钥与一键接入指令，请立即保存。'}
            </SheetDescription>
          </SheetHeader>

          <div className="flex-1 overflow-y-auto px-4 py-4 sm:px-6 sm:py-5">
            <Form {...form}>
              <form id="client-form" onSubmit={onSubmit} className="space-y-4">
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
                        <Textarea rows={2} placeholder="可选，如机房位置或负责人" {...field} />
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
                      <FormItem className="flex flex-row items-center justify-between gap-4 rounded-xl border border-border/80 bg-muted/20 p-3.5">
                        <div className="space-y-1">
                          <FormLabel>启用</FormLabel>
                          <p className="text-xs text-muted-foreground">
                            关闭后客户端会话立即断开，其名下转发规则不再承载流量。
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
                  className="rounded-xl border border-border/80 bg-muted/20"
                >
                  <div className="flex items-center justify-between gap-2 p-3">
                    <InfoLabel
                      title="高级配置 (资源与流量配额)"
                      infoKey="client.form.advanced"
                      className="px-1 text-sm font-medium"
                    />
                    <CollapsibleTrigger asChild>
                      <Button type="button" variant="ghost" size="sm" aria-expanded={advancedOpen}>
                        {advancedOpen ? '收起' : '展开'}
                        <ChevronDown
                          aria-hidden="true"
                          className={cn(
                            'size-4 transition-transform',
                            advancedOpen && 'rotate-180',
                          )}
                        />
                      </Button>
                    </CollapsibleTrigger>
                  </div>
                  <CollapsibleContent className="grid gap-4 border-t border-border/70 p-4 sm:grid-cols-2">
                    <div className="sm:col-span-2">
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
                            <div className="flex flex-wrap items-center gap-1 pt-0.5">
                              {[
                                { label: '不限', val: '' },
                                { label: '1MB', val: '1MB' },
                                { label: '5MB', val: '5MB' },
                                { label: '10MB', val: '10MB' },
                                { label: '50MB', val: '50MB' },
                                { label: '100MB', val: '100MB' },
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
                            <FormMessage />
                          </FormItem>
                        )}
                      />
                    </div>
                    {renderQuotaField('max_conns', '最大连接', 'client.max_conns')}
                    {renderQuotaField('max_services', '最大规则数', 'client.max_services')}
                    <div className="sm:col-span-2">
                      {renderQuotaField('flow_limit_mb', '流量上限(MB)', 'client.flow_limit_mb')}
                    </div>
                  </CollapsibleContent>
                </Collapsible>
              </form>
            </Form>
          </div>

          <SheetFooter className="flex-row items-center justify-end gap-2 border-t border-border/80 bg-card/90 px-6 py-4">
            <Button
              type="button"
              variant="outline"
              disabled={saving}
              onClick={() => setFormOpen(false)}
            >
              取消
            </Button>
            <Button type="submit" form="client-form" disabled={saving}>
              {saving ? <Loader2 className="size-4 animate-spin" aria-hidden="true" /> : null}
              保存
            </Button>
          </SheetFooter>
        </SheetContent>
      </Sheet>

      <SecretModal
        open={secretState.open}
        clientId={secretState.clientId}
        secret={secretState.secret}
        onOpenChange={(open) => {
          if (!open) {
            setSecretState({ open: false, secret: '' });
          }
        }}
      />

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
