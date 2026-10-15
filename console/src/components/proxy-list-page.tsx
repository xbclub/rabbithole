import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { KeyRound, Plus, RefreshCw, Trash2, Upload } from 'lucide-react';
import { Suspense, lazy, useMemo, useState } from 'react';
import { Link } from 'react-router';

import {
  batchServices,
  createService,
  deleteService,
  disableService,
  enableService,
  listClients,
  listServices,
  rotateServiceSecret,
  updateService,
} from '@/api/rabbithole';
import type { ClientRow, ServiceRow } from '@/api/rabbithole';
import { ConfirmDialog } from '@/components/confirm-dialog';
import { DataTable, DataTableColumnHeader } from '@/components/data-table';
import type { RhColumnDef } from '@/components/data-table';
import { EmptyState } from '@/components/empty-state';
import { InfoLabel } from '@/components/info-label';
import { Mono } from '@/components/mono';
import { PageHeader } from '@/components/page-header';
import { RowActionsMenu } from '@/components/row-actions';
import { StatusBadge, proxyStatusSignal } from '@/components/status-badge';
import { Badge } from '@/components/ui/badge';
import { MenubarItem } from '@/components/ui/menubar';
import { Button } from '@/components/ui/button';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';
import { Switch } from '@/components/ui/switch';
import { Textarea } from '@/components/ui/textarea';
import { Tooltip, TooltipContent, TooltipTrigger } from '@/components/ui/tooltip';
import type { ProxyKind, ServiceSpecPayload } from '@/features/services/types';
import { focusFirstControl } from '@/lib/dialog-focus';
import { feedbackError, feedbackSuccess } from '@/lib/feedback';
import { qk } from '@/lib/query-keys';
import { ROUTES } from '@/router/paths';

const ALL_KINDS_FILTER = '__all__';

/**
 * 规则表单弹窗按需加载。
 *
 * 它静态引入会拖进 react-hook-form + zod + Form/Textarea/Switch 等(约 130 KB 未压缩),
 * 而这些只有用户点「新建/编辑」时才需要 —— 列表页首屏不该为它买单。
 */
const LazyProxyFormModal = lazy(() =>
  import('@/components/proxy-form-modal').then((module) => ({
    default: module.ProxyFormModal,
  })),
);

export interface ProxyListPageProps {
  kinds: ProxyKind[];
  defaultKind: ProxyKind;
  title?: string;
  description?: string;
  /** 空态文案里的名词(默认「转发规则」)。 */
  emptyNoun?: string;
  /** 作为 Tab 面板嵌入时不渲染页头(避免与外层页头重复)。 */
  bare?: boolean;
  /** 密钥访问页传入:行内「创建访问器」入口。 */
  onCreateVisitor?: (row: ServiceRow) => void;
}

/**
 * 参数化类型页:tcp/udp(单 kind)、domains(http/https/tcpmux)、
 * secret(stcp/sudp/xtcp)共用;列模式/空态按页参数化,数据行为不变。
 */
export function ProxyListPage({
  kinds,
  defaultKind,
  title,
  description,
  emptyNoun,
  bare = false,
  onCreateVisitor,
}: ProxyListPageProps) {
  const queryClient = useQueryClient();
  const [modal, setModal] = useState<{ editing: null | ServiceRow; open: boolean }>({
    open: false,
    editing: null,
  });
  const [clientFilter, setClientFilter] = useState<string>(ALL_KINDS_FILTER);
  const [selected, setSelected] = useState<ServiceRow[]>([]);
  // 动作超过 2 个时收进 ⋯,确认框由菜单项受控打开
  const [rotateTarget, setRotateTarget] = useState<ServiceRow | null>(null);
  const [deleteTarget, setDeleteTarget] = useState<ServiceRow | null>(null);
  // 打开过一次后再常驻挂载:既能延后加载表单模块,又不丢关闭动画
  const [modalEverOpened, setModalEverOpened] = useState(false);
  const [batchOpen, setBatchOpen] = useState(false);
  const [batchText, setBatchText] = useState('');
  const [batchResults, setBatchResults] = useState<null | Array<{ name: string; msg: string }>>(
    null,
  );
  const [batchRunning, setBatchRunning] = useState(false);

  const servicesQuery = useQuery({ queryKey: qk.services(), queryFn: () => listServices() });
  const clientsQuery = useQuery({ queryKey: qk.clients, queryFn: listClients });
  const clients: ClientRow[] = clientsQuery.data ?? [];

  const rows = useMemo(
    () => (servicesQuery.data ?? []).filter((row) => kinds.includes(row.kind as ProxyKind)),
    [servicesQuery.data, kinds],
  );
  const filtered = useMemo(
    () =>
      clientFilter === ALL_KINDS_FILTER
        ? rows
        : rows.filter((row) => row.client_id === clientFilter),
    [rows, clientFilter],
  );

  const invalidate = () => queryClient.invalidateQueries({ queryKey: ['services'] });

  /** 打开新建/编辑弹窗。顺带记录「已打开过」,之后弹窗常驻挂载以保留关闭动画。 */
  const openModal = (editing: ServiceRow | null) => {
    setModalEverOpened(true);
    setModal({ open: true, editing });
  };

  const saveMutation = useMutation({
    mutationFn: ({ editing, spec }: { editing: null | ServiceRow; spec: ServiceSpecPayload }) =>
      editing ? updateService(editing.id, spec) : createService(spec),
    onSuccess: (_data, variables) => {
      feedbackSuccess(variables.editing ? '已保存' : '已创建');
      setModal({ open: false, editing: null });
      invalidate();
    },
  });

  const toggleMutation = useMutation({
    mutationFn: ({ row, enabled }: { row: ServiceRow; enabled: boolean }) =>
      enabled ? enableService(row.id) : disableService(row.id),
    onSuccess: (_data, variables) => {
      feedbackSuccess(
        variables.enabled ? `已启用 ${variables.row.name}` : `已停用 ${variables.row.name}`,
      );
      invalidate();
    },
  });

  const rotateMutation = useMutation({
    mutationFn: (row: ServiceRow) => rotateServiceSecret(row.id),
    onSuccess: () => {
      feedbackSuccess('访问密钥已轮换,访问器自动继承新密钥,无需其他操作');
      invalidate();
    },
  });

  const deleteMutation = useMutation({
    mutationFn: (row: ServiceRow) => deleteService(row.id),
    onSuccess: () => {
      feedbackSuccess('已删除');
      invalidate();
    },
  });

  const isTunnel = kinds.every((kind) => kind === 'tcp' || kind === 'udp');
  const isDomain = kinds.every((kind) => kind === 'http' || kind === 'https' || kind === 'tcpmux');
  const isSecret = kinds.every((kind) => kind === 'stcp' || kind === 'sudp' || kind === 'xtcp');

  const noun = emptyNoun ?? title ?? '转发规则';

  const columns = useMemo<RhColumnDef<ServiceRow>[]>(() => {
    const list: RhColumnDef<ServiceRow>[] = [
      {
        id: 'name',
        meta: { label: '名称' },
        accessorKey: 'name',
        header: ({ column }) => <DataTableColumnHeader column={column} title="名称" />,
        cell: ({ row }) => <span className="font-medium">{row.original.name}</span>,
      },
    ];

    if (isTunnel) {
      list.push({
        id: 'bind',
        meta: { label: '监听' },
        header: '监听',
        cell: ({ row }) =>
          row.original.bind_port > 0 ? (
            <Mono>{`${row.original.bind_ip}:${row.original.bind_port}`}</Mono>
          ) : (
            <Mono>共享端口</Mono>
          ),
      });
    }

    if (isDomain) {
      list.push({
        id: 'domains',
        meta: { label: '域名' },
        header: '域名',
        cell: ({ row }) => {
          const spec = row.original.spec ?? {};
          const http = spec.http as Record<string, unknown> | undefined;
          const https = spec.https as Record<string, unknown> | undefined;
          const tcpmux = spec.tcpmux as Record<string, unknown> | undefined;
          const domains =
            (http?.custom_domains as string[] | undefined) ??
            (https?.custom_domains as string[] | undefined) ??
            (tcpmux?.custom_domains as string[] | undefined) ??
            [];
          const subdomain = (http?.subdomain ?? https?.subdomain) as string | undefined;
          if (domains.length === 0 && !subdomain) return '-';
          return (
            <span className="flex flex-wrap items-center gap-1.5">
              {domains[0] ? <Mono>{domains[0]}</Mono> : null}
              {domains.length > 1 ? <Badge variant="secondary">+{domains.length - 1}</Badge> : null}
              {subdomain ? <Mono>{`${subdomain}.*`}</Mono> : null}
            </span>
          );
        },
      });
    }

    list.push(
      {
        id: 'status',
        meta: { label: '状态' },
        accessorKey: 'status',
        header: () => <InfoLabel title="状态" infoKey="service.list.status" />,
        cell: ({ row }) => {
          const signal = proxyStatusSignal(row.original.status, row.original.healthy);
          const badge = <StatusBadge signal={signal.signal} text={signal.text} />;
          if (!row.original.status_reason) return badge;
          return (
            <Tooltip>
              <TooltipTrigger asChild>{badge}</TooltipTrigger>
              <TooltipContent className="max-w-80">{row.original.status_reason}</TooltipContent>
            </Tooltip>
          );
        },
      },
      {
        id: 'target',
        meta: { label: '目标' },
        header: '目标',
        cell: ({ row }) =>
          row.original.plugin ? (
            <span className="flex items-center gap-1.5">
              <Mono>{row.original.plugin}</Mono>
              <Badge
                variant="outline"
                className="border-violet-500/40 text-violet-600 dark:text-violet-400"
              >
                插件
              </Badge>
            </span>
          ) : (
            <Mono>{`${row.original.target_host}:${row.original.target_port}`}</Mono>
          ),
      },
      {
        id: 'pool',
        meta: { label: '池' },
        accessorKey: 'pool_ready',
        header: () => <InfoLabel title="池" infoKey="service.list.pool" />,
        cell: ({ row }) => <Mono>{row.original.pool_ready}</Mono>,
      },
      {
        id: 'conns',
        meta: { label: '连接' },
        accessorKey: 'conns_current',
        header: () => <InfoLabel title="连接" infoKey="service.list.conns" />,
        cell: ({ row }) => <Mono>{row.original.conns_current ?? 0}</Mono>,
      },
      {
        id: 'enabled',
        meta: { label: '启用' },
        accessorKey: 'enabled',
        header: '启用',
        cell: ({ row }) => {
          const pending =
            toggleMutation.isPending && toggleMutation.variables?.row.id === row.original.id;
          const checked = pending
            ? (toggleMutation.variables?.enabled ?? row.original.enabled)
            : row.original.enabled;
          return (
            <Switch
              checked={checked}
              disabled={pending}
              aria-label={`${row.original.name} 启用`}
              onCheckedChange={(next) =>
                toggleMutation.mutate({ row: row.original, enabled: next })
              }
            />
          );
        },
      },
      {
        id: 'actions',
        meta: { label: '操作', headerClassName: 'w-[1%]' },
        enableHiding: false,
        header: '操作',
        cell: ({ row }) => {
          const service = row.original;
          const visitorAction = isSecret && onCreateVisitor;
          // 动作数 = 编辑 + 删除 + 创建访问器(密钥页) + 轮换密钥(密钥页);
          // 规则:超过 2 个才收进 ⋯,≤2 个全部平铺,不牺牲快捷操作。
          const actionCount = 2 + (visitorAction ? 1 : 0) + (isSecret ? 1 : 0);
          const overflow = actionCount > 2;
          return (
            <div className="flex items-center justify-end gap-1">
              <Button variant="ghost" size="sm" onClick={() => openModal(service)}>
                编辑
              </Button>
              {!overflow && visitorAction ? (
                <Button variant="ghost" size="sm" onClick={() => onCreateVisitor(service)}>
                  创建访问器
                </Button>
              ) : null}
              {!overflow && isSecret ? (
                <ConfirmDialog
                  trigger={
                    <Button variant="ghost" size="sm" disabled={rotateMutation.isPending}>
                      轮换密钥
                    </Button>
                  }
                  title={`轮换 ${service.name} 的访问密钥?`}
                  description="服务端生成新随机密钥,访问器自动继承,现有中继不受影响。"
                  confirmText="轮换"
                  onConfirm={async () => {
                    await rotateMutation.mutateAsync(service);
                  }}
                />
              ) : null}
              {!overflow ? (
                <ConfirmDialog
                  trigger={
                    <Button
                      variant="ghost"
                      size="sm"
                      className="text-destructive hover:text-destructive"
                      disabled={deleteMutation.isPending}
                    >
                      删除
                    </Button>
                  }
                  title={`删除转发规则 ${service.name}?`}
                  description="此操作不可恢复,该规则的公网入口立即关闭。"
                  confirmText="删除"
                  destructive
                  onConfirm={async () => {
                    await deleteMutation.mutateAsync(service);
                  }}
                />
              ) : null}
              {overflow ? (
                <RowActionsMenu label={`${service.name} 的操作`}>
                  {visitorAction ? (
                    <MenubarItem onSelect={() => onCreateVisitor(service)}>
                      <KeyRound className="size-4" aria-hidden="true" />
                      创建访问器
                    </MenubarItem>
                  ) : null}
                  {isSecret ? (
                    <MenubarItem onSelect={() => setRotateTarget(service)}>
                      <RefreshCw className="size-4" aria-hidden="true" />
                      轮换密钥
                    </MenubarItem>
                  ) : null}
                  <MenubarItem
                    className="text-destructive focus:bg-destructive/10 focus:text-destructive"
                    onSelect={() => setDeleteTarget(service)}
                  >
                    <Trash2 className="size-4" aria-hidden="true" />
                    删除
                  </MenubarItem>
                </RowActionsMenu>
              ) : null}
            </div>
          );
        },
      },
    );

    return list;
  }, [
    isTunnel,
    isDomain,
    isSecret,
    onCreateVisitor,
    toggleMutation,
    rotateMutation,
    deleteMutation,
  ]);

  async function runBatchImport() {
    let parsed: unknown;
    try {
      parsed = JSON.parse(batchText);
    } catch {
      feedbackError('不是合法的 JSON 数组');
      return;
    }
    if (!Array.isArray(parsed)) {
      feedbackError('不是合法的 JSON 数组');
      return;
    }
    setBatchRunning(true);
    const results: Array<{ name: string; msg: string }> = [];
    for (const item of parsed as Array<Record<string, unknown>>) {
      const name = typeof item.name === 'string' ? item.name : '?';
      try {
        await createService(item);
        results.push({ name, msg: '成功' });
      } catch (error) {
        results.push({ name, msg: error instanceof Error ? error.message : '失败' });
      }
    }
    setBatchResults(results);
    setBatchRunning(false);
    invalidate();
  }

  async function runBatchOp(action: 'delete' | 'disable' | 'enable') {
    if (selected.length === 0) return;
    const results = await batchServices(
      selected.map((row) => row.id),
      action,
    );
    const ok = results.filter((item) => item.code === 0).length;
    const bad = results.filter((item) => item.code !== 0);
    const verb = action === 'enable' ? '启用' : action === 'disable' ? '停用' : '删除';
    feedbackSuccess(`批量${verb}:${ok} 成功${bad.length ? `,${bad.length} 失败` : ''}`);
    bad.slice(0, 3).forEach((item) => feedbackError(`#${item.id}:${item.message}`));
    setSelected([]);
    invalidate();
  }

  const emptyNode = (
    <EmptyState
      icon={<Upload className="size-5" />}
      title={clients.length === 0 ? '还没有客户端' : `还没有${noun}`}
      description={
        clients.length === 0
          ? `${noun}需要一个在线客户端承载,先创建客户端并接入服务端。`
          : `${noun}把公网入口的流量送进内网。创建后由承载客户端立即生效。`
      }
      action={
        clients.length === 0 ? (
          <Button asChild>
            <Link to={ROUTES.clients}>去创建客户端</Link>
          </Button>
        ) : (
          <Button onClick={() => openModal(null)}>
            <Plus className="size-4" />
            新建规则
          </Button>
        )
      }
    />
  );

  return (
    <div className="flex min-w-0 flex-col gap-6">
      {bare ? null : <PageHeader title={title ?? noun} description={description} />}

      <DataTable
        columns={columns}
        data={filtered}
        loading={servicesQuery.isLoading}
        empty={emptyNode}
        getRowId={(row) => String(row.id)}
        enableRowSelection
        onSelectedRowsChange={setSelected}
        mobileRow={(row) => (
          <div className="space-y-2">
            <div className="flex items-center justify-between gap-2">
              <span className="font-medium">{row.name}</span>
              <StatusBadge {...proxyStatusSignal(row.status, row.healthy)} />
            </div>
            <div className="rh-mono text-xs text-muted-foreground">
              {row.plugin ? row.plugin : `${row.target_host}:${row.target_port}`}
            </div>
            {row.bind_port > 0 ? (
              <div className="rh-mono text-xs text-muted-foreground">
                {row.bind_ip}:{row.bind_port}
              </div>
            ) : null}
          </div>
        )}
        toolbar={
          <>
            <Select value={clientFilter} onValueChange={setClientFilter}>
              <SelectTrigger size="sm" className="w-44" aria-label="按客户端筛选">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value={ALL_KINDS_FILTER}>全部客户端</SelectItem>
                {clients.map((client) => (
                  <SelectItem key={client.id} value={client.id}>
                    {client.name}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
            <Button
              variant="outline"
              size="sm"
              onClick={() => {
                setBatchResults(null);
                setBatchOpen(true);
              }}
            >
              <Upload className="size-4" />
              批量导入
            </Button>
            <Button
              variant="outline"
              size="sm"
              onClick={() => invalidate()}
              disabled={servicesQuery.isFetching}
            >
              <RefreshCw className={`size-4 ${servicesQuery.isFetching ? 'animate-spin' : ''}`} />
              刷新
            </Button>
            {clients.length === 0 ? (
              // 禁用按钮不触发指针事件,故把 Tooltip 挂在外层 span 上
              <Tooltip>
                <TooltipTrigger asChild>
                  <span className="inline-flex">
                    <Button size="sm" disabled>
                      <Plus className="size-4" />
                      新建规则
                    </Button>
                  </span>
                </TooltipTrigger>
                <TooltipContent>先创建并接入一个客户端</TooltipContent>
              </Tooltip>
            ) : (
              <Button size="sm" onClick={() => openModal(null)}>
                <Plus className="size-4" />
                新建规则
              </Button>
            )}
          </>
        }
      />

      {selected.length > 0 ? (
        <div className="flex flex-wrap items-center gap-2 rounded-lg border bg-muted/40 px-3 py-2 text-sm">
          <span className="text-muted-foreground">已选 {selected.length} 条</span>
          <Button variant="outline" size="sm" onClick={() => runBatchOp('enable')}>
            批量启用
          </Button>
          <Button variant="outline" size="sm" onClick={() => runBatchOp('disable')}>
            批量停用
          </Button>
          <ConfirmDialog
            trigger={
              <Button variant="outline" size="sm" className="text-destructive">
                批量删除
              </Button>
            }
            title={`删除选中的 ${selected.length} 条转发规则?`}
            description="此操作不可恢复。"
            confirmText="删除"
            destructive
            onConfirm={async () => {
              await runBatchOp('delete');
            }}
          />
        </div>
      ) : null}

      {modalEverOpened ? (
        <Suspense fallback={null}>
          <LazyProxyFormModal
            key={modal.editing ? `edit-${modal.editing.id}` : 'new'}
            open={modal.open}
            onOpenChange={(open) => setModal({ open, editing: open ? modal.editing : null })}
            kinds={kinds}
            defaultKind={defaultKind}
            editing={modal.editing}
            clients={clients}
            submitting={saveMutation.isPending}
            onSubmit={async (spec) => {
              await saveMutation.mutateAsync({ editing: modal.editing, spec });
            }}
          />
        </Suspense>
      ) : null}

      <Dialog open={batchOpen} onOpenChange={setBatchOpen}>
        <DialogContent className="sm:max-w-2xl" onOpenAutoFocus={focusFirstControl}>
          <DialogHeader>
            <DialogTitle>批量导入规则</DialogTitle>
            <DialogDescription>
              粘贴规则 JSON 数组(与单条创建载荷一致),逐条创建并回报结果。
            </DialogDescription>
          </DialogHeader>
          <Textarea
            value={batchText}
            onChange={(event) => setBatchText(event.target.value)}
            rows={10}
            className="rh-mono text-xs"
            placeholder='[{"client_id":"…","name":"web","kind":"tcp","bind_port":0,"target_host":"10.0.0.1","target_port":80}]'
            aria-label="规则 JSON 数组"
          />
          {batchResults ? (
            <div className="max-h-40 space-y-1 overflow-y-auto rounded-md border p-2 text-xs">
              {batchResults.map((item, index) => (
                <div key={index} className="flex items-center gap-2">
                  <span className="rh-mono">{item.name}</span>
                  <span className={item.msg === '成功' ? 'text-emerald-600' : 'text-destructive'}>
                    {item.msg}
                  </span>
                </div>
              ))}
            </div>
          ) : null}
          <DialogFooter>
            <Button variant="outline" onClick={() => setBatchOpen(false)}>
              关闭
            </Button>
            <Button
              onClick={() => void runBatchImport()}
              disabled={batchRunning || !batchText.trim()}
            >
              {batchRunning ? '导入中…' : '开始导入'}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* 密钥页动作超过 2 个,轮换/删除收进 ⋯:确认框全页各一份,受控打开 */}
      <ConfirmDialog
        open={rotateTarget !== null}
        onOpenChange={(open) => {
          if (!open) setRotateTarget(null);
        }}
        title={`轮换 ${rotateTarget?.name ?? ''} 的访问密钥?`}
        description="服务端生成新随机密钥,访问器自动继承,现有中继不受影响。"
        confirmText="轮换"
        onConfirm={async () => {
          if (!rotateTarget) return;
          await rotateMutation.mutateAsync(rotateTarget);
        }}
      />
      <ConfirmDialog
        destructive
        open={deleteTarget !== null}
        onOpenChange={(open) => {
          if (!open) setDeleteTarget(null);
        }}
        title={`删除转发规则 ${deleteTarget?.name ?? ''}?`}
        description="此操作不可恢复,该规则的公网入口立即关闭。"
        confirmText="删除"
        onConfirm={async () => {
          if (!deleteTarget) return;
          await deleteMutation.mutateAsync(deleteTarget);
        }}
      />
    </div>
  );
}
