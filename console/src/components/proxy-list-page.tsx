import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import {
  ArrowRight,
  ArrowRightLeft,
  Cable,
  Globe,
  KeyRound,
  Layers,
  LayoutList,
  Plus,
  Radio,
  RefreshCw,
  Search,
  Server,
  Trash2,
  Upload,
} from 'lucide-react';
import { Suspense, lazy, useEffect, useMemo, useState } from 'react';
import { Link, useLocation, useNavigate, useSearchParams } from 'react-router';

import {
  batchServices,
  createService,
  deleteService,
  disableService,
  enableService,
  listClients,
  listServices,
  listVisitors,
  rotateServiceSecret,
  updateService,
} from '@/api/rabbithole';
import type { ClientRow, ServiceRow } from '@/api/rabbithole';
import { BatchImportDialog } from '@/components/batch-import-dialog';
import { ConfirmDialog } from '@/components/confirm-dialog';
import { DataTable, DataTableColumnHeader } from '@/components/data-table';
import type { RhColumnDef } from '@/components/data-table';
import { EmptyState } from '@/components/empty-state';
import { InfoLabel } from '@/components/info-label';
import { Mono, MonoCopy } from '@/components/mono';
import { PageHeader } from '@/components/page-header';
import { RowActionsMenu } from '@/components/row-actions';
import { StatusBadge, proxyStatusSignal } from '@/components/status-badge';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardHeader } from '@/components/ui/card';
import { Input } from '@/components/ui/input';
import { MenubarItem } from '@/components/ui/menubar';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';
import { Switch } from '@/components/ui/switch';
import { Tooltip, TooltipContent, TooltipTrigger } from '@/components/ui/tooltip';
import type { ProxyKind, ServiceSpecPayload } from '@/features/services/types';
import { feedbackError, feedbackSuccess } from '@/lib/feedback';
import { qk } from '@/lib/query-keys';
import { cn } from '@/lib/utils';
import { ROUTES } from '@/router/paths';

const ALL_CLIENTS_FILTER = '__all__';
const ALL_STATUS_FILTER = '__all__';
const ALL_PROXY_KINDS: ProxyKind[] = [
  'tcp',
  'udp',
  'http',
  'https',
  'tcpmux',
  'stcp',
  'sudp',
  'xtcp',
];

const LazyProxyFormModal = lazy(() =>
  import('@/components/proxy-form-modal').then((module) => ({
    default: module.ProxyFormModal,
  })),
);

/** 计算规则的公网入口描述 */
function formatIngress(row: ServiceRow): {
  primary: string;
  extraCount: number;
  copyable: boolean;
} {
  if (row.kind === 'http' || row.kind === 'https' || row.kind === 'tcpmux') {
    const domains = row.custom_domains ?? [];
    const sub = row.subdomain;
    if (domains.length > 0) {
      return { primary: domains[0], extraCount: domains.length - 1, copyable: true };
    }
    if (sub) {
      return { primary: `${sub}.*`, extraCount: 0, copyable: false };
    }
    return { primary: '未绑定域名', extraCount: 0, copyable: false };
  }
  if (row.kind === 'stcp' || row.kind === 'sudp' || row.kind === 'xtcp') {
    return {
      primary: row.kind === 'xtcp' ? 'P2P 密钥打洞' : '密钥鉴权中继',
      extraCount: 0,
      copyable: false,
    };
  }
  if (row.bind_port > 0) {
    return { primary: `${row.bind_ip}:${row.bind_port}`, extraCount: 0, copyable: true };
  }
  return { primary: '自动分配端口', extraCount: 0, copyable: false };
}

/**
 * 隧道与路由中枢顶部「协议透镜导航条」。
 * 将原本割裂在侧边栏的 TCP / UDP / 域名 / 密钥 / 访问器整合为带实时计数的水平透镜切页。
 */
export function TunnelHubLensBar() {
  const { pathname } = useLocation();
  const [searchParams] = useSearchParams();
  const navigate = useNavigate();

  const servicesQ = useQuery({ queryKey: qk.services(), queryFn: () => listServices() });
  const visitorsQ = useQuery({ queryKey: qk.visitors(), queryFn: () => listVisitors() });

  const counts = useMemo(() => {
    const allServices = servicesQ.data ?? [];
    const allVisitors = visitorsQ.data ?? [];
    let tcp = 0;
    let udp = 0;
    let domains = 0;
    let secret = 0;
    for (const s of allServices) {
      if (s.kind === 'tcp') tcp++;
      else if (s.kind === 'udp') udp++;
      else if (s.kind === 'http' || s.kind === 'https' || s.kind === 'tcpmux') domains++;
      else if (s.kind === 'stcp' || s.kind === 'sudp' || s.kind === 'xtcp') secret++;
    }
    return {
      all: allServices.length,
      tcp,
      udp,
      domains,
      secret,
      visitors: allVisitors.length,
    };
  }, [servicesQ.data, visitorsQ.data]);

  const isVisitorsTab = pathname === ROUTES.secret && searchParams.get('tab') === 'visitors';
  const isAllLens = pathname === ROUTES.tcp && searchParams.get('lens') === 'all';

  const lenses = [
    {
      key: 'all',
      label: '全部隧道',
      count: counts.all,
      icon: Layers,
      active: isAllLens,
      to: `${ROUTES.tcp}?lens=all`,
    },
    {
      key: 'tcp',
      label: 'TCP 隧道',
      count: counts.tcp,
      icon: ArrowRightLeft,
      active: pathname === ROUTES.tcp && !isAllLens,
      to: ROUTES.tcp,
    },
    {
      key: 'udp',
      label: 'UDP 隧道',
      count: counts.udp,
      icon: Radio,
      active: pathname === ROUTES.udp,
      to: ROUTES.udp,
    },
    {
      key: 'domains',
      label: '域名服务 (HTTP/S)',
      count: counts.domains,
      icon: Globe,
      active: pathname === ROUTES.domains,
      to: ROUTES.domains,
    },
    {
      key: 'secret',
      label: '私网密钥规则',
      count: counts.secret,
      icon: KeyRound,
      active: pathname === ROUTES.secret && !isVisitorsTab,
      to: ROUTES.secret,
    },
    {
      key: 'visitors',
      label: 'P2P 访问器',
      count: counts.visitors,
      icon: Cable,
      active: isVisitorsTab,
      to: `${ROUTES.secret}?tab=visitors`,
    },
  ];

  // 保留跨透镜的 client 筛选参数
  const clientParam = searchParams.get('client');
  const buildUrl = (baseTo: string) => {
    if (!clientParam) return baseTo;
    const sep = baseTo.includes('?') ? '&' : '?';
    return `${baseTo}${sep}client=${encodeURIComponent(clientParam)}`;
  };

  return (
    <div className="grid grid-cols-2 gap-1.5 rounded-2xl border border-border/80 bg-card/80 p-1.5 shadow-2xs sm:flex sm:flex-wrap sm:items-center">
      {lenses.map((lens) => {
        const Icon = lens.icon;
        return (
          <button
            key={lens.key}
            type="button"
            onClick={() => navigate(buildUrl(lens.to))}
            className={cn(
              'flex min-w-0 items-center justify-between gap-1.5 rounded-xl px-2.5 py-2 text-xs font-medium transition-all sm:justify-start sm:gap-2 sm:px-3',
              lens.active
                ? 'bg-primary text-primary-foreground shadow-xs'
                : 'text-muted-foreground hover:bg-muted/70 hover:text-foreground',
            )}
          >
            <span className="flex min-w-0 items-center gap-1.5">
              <Icon className="size-3.5 shrink-0" />
              <span className="truncate">{lens.label}</span>
            </span>
            <span
              className={cn(
                'rh-mono shrink-0 rounded-full px-1.5 py-0.2 text-[11px]',
                lens.active
                  ? 'bg-primary-foreground/20 text-primary-foreground'
                  : 'bg-muted text-muted-foreground',
              )}
            >
              {lens.count}
            </span>
          </button>
        );
      })}
    </div>
  );
}

export interface ProxyListPageProps {
  kinds: ProxyKind[];
  defaultKind: ProxyKind;
  title?: string;
  description?: string;
  emptyNoun?: string;
  bare?: boolean;
  onCreateVisitor?: (row: ServiceRow) => void;
}

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
  const [searchParams, setSearchParams] = useSearchParams();

  const isAllLens = searchParams.get('lens') === 'all';
  const effectiveKinds = isAllLens ? ALL_PROXY_KINDS : kinds;

  const [modal, setModal] = useState<{
    editing: null | ServiceRow;
    open: boolean;
    presetClientId?: string;
  }>({
    open: false,
    editing: null,
  });

  const urlClient = searchParams.get('client');
  const urlQuery = searchParams.get('q');

  const [clientFilter, setClientFilter] = useState<string>(urlClient || ALL_CLIENTS_FILTER);
  const [statusFilter, setStatusFilter] = useState<string>(ALL_STATUS_FILTER);
  const [searchQuery, setSearchQuery] = useState<string>(urlQuery ?? '');
  const [viewMode, setViewMode] = useState<'table' | 'grouped'>('table');
  const [selected, setSelected] = useState<ServiceRow[]>([]);
  const [rotateTarget, setRotateTarget] = useState<ServiceRow | null>(null);
  const [deleteTarget, setDeleteTarget] = useState<ServiceRow | null>(null);
  const [modalEverOpened, setModalEverOpened] = useState(false);
  const [batchOpen, setBatchOpen] = useState(false);

  // 同步外部 URL ?client= 或 ?q= 参数变化(例如从 ⌘K 或节点页跳入)
  useEffect(() => {
    // eslint-disable-next-line react-hooks/set-state-in-effect
    if (urlClient) setClientFilter(urlClient);
  }, [urlClient]);

  useEffect(() => {
    // eslint-disable-next-line react-hooks/set-state-in-effect
    if (urlQuery !== null) setSearchQuery(urlQuery);
  }, [urlQuery]);

  const handleClientFilterChange = (next: string) => {
    setClientFilter(next);
    const params = new URLSearchParams(searchParams);
    if (next === ALL_CLIENTS_FILTER) {
      params.delete('client');
    } else {
      params.set('client', next);
    }
    setSearchParams(params, { replace: true });
  };

  const servicesQuery = useQuery({ queryKey: qk.services(), queryFn: () => listServices() });
  const clientsQuery = useQuery({ queryKey: qk.clients, queryFn: listClients });
  const clients: ClientRow[] = useMemo(() => clientsQuery.data ?? [], [clientsQuery.data]);
  const clientMap = useMemo(() => new Map(clients.map((c) => [c.id, c])), [clients]);

  const rows = useMemo(
    () =>
      (servicesQuery.data ?? []).filter((row) => effectiveKinds.includes(row.kind as ProxyKind)),
    [servicesQuery.data, effectiveKinds],
  );

  const filtered = useMemo(() => {
    return rows.filter((row) => {
      if (clientFilter !== ALL_CLIENTS_FILTER && row.client_id !== clientFilter) {
        return false;
      }
      if (statusFilter === 'running' && (row.status !== 'running' || !row.healthy)) {
        return false;
      }
      if (
        statusFilter === 'issues' &&
        !(
          row.status === 'bind_error' ||
          row.status === 'apply_error' ||
          row.status === 'offline' ||
          (row.status === 'running' && !row.healthy)
        )
      ) {
        return false;
      }
      if (statusFilter === 'disabled' && row.enabled && row.status !== 'disabled') {
        return false;
      }
      if (searchQuery.trim()) {
        const q = searchQuery.trim().toLowerCase();
        const domainStr = (row.custom_domains ?? []).join(' ').toLowerCase();
        const subStr = (row.subdomain ?? '').toLowerCase();
        const bindStr = `${row.bind_ip}:${row.bind_port}`.toLowerCase();
        const targetStr = `${row.target_host}:${row.target_port}`.toLowerCase();
        const pluginStr = (row.plugin ?? '').toLowerCase();
        const clientName = (clientMap.get(row.client_id)?.name ?? '').toLowerCase();
        return (
          row.name.toLowerCase().includes(q) ||
          row.kind.toLowerCase().includes(q) ||
          domainStr.includes(q) ||
          subStr.includes(q) ||
          bindStr.includes(q) ||
          targetStr.includes(q) ||
          pluginStr.includes(q) ||
          clientName.includes(q)
        );
      }
      return true;
    });
  }, [rows, clientFilter, statusFilter, searchQuery, clientMap]);

  const groupedByClient = useMemo(() => {
    const groups = new Map<string, { client: ClientRow | undefined; rules: ServiceRow[] }>();
    for (const rule of filtered) {
      const existing = groups.get(rule.client_id);
      if (existing) {
        existing.rules.push(rule);
      } else {
        groups.set(rule.client_id, {
          client: clientMap.get(rule.client_id),
          rules: [rule],
        });
      }
    }
    return Array.from(groups.entries());
  }, [filtered, clientMap]);

  const invalidate = () => queryClient.invalidateQueries({ queryKey: ['services'] });

  const openModal = (editing: ServiceRow | null, presetClientId?: string) => {
    setModalEverOpened(true);
    setModal({
      open: true,
      editing,
      presetClientId:
        presetClientId ?? (clientFilter !== ALL_CLIENTS_FILTER ? clientFilter : undefined),
    });
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

  const noun = isAllLens ? '转发规则' : (emptyNoun ?? title ?? '转发规则');

  const renderRowActions = (service: ServiceRow) => {
    const rowIsSecret =
      service.kind === 'stcp' || service.kind === 'sudp' || service.kind === 'xtcp';
    const visitorAction = rowIsSecret && onCreateVisitor;
    const actionCount = 2 + (visitorAction ? 1 : 0) + (rowIsSecret ? 1 : 0);
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
        {!overflow && rowIsSecret ? (
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
            {rowIsSecret ? (
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
  };

  const columns: RhColumnDef<ServiceRow>[] = (() => {
    const list: RhColumnDef<ServiceRow>[] = [
      {
        id: 'name',
        meta: { label: '名称与协议' },
        accessorKey: 'name',
        header: ({ column }) => <DataTableColumnHeader column={column} title="规则名称" />,
        cell: ({ row }) => {
          const s = row.original;
          const client = clientMap.get(s.client_id);
          return (
            <div className="flex flex-col gap-0.5">
              <div className="flex items-center gap-2">
                <span className="rh-mono rounded bg-primary/10 px-1.5 py-0.5 text-[10px] font-bold text-primary uppercase">
                  {s.kind}
                </span>
                <span className="font-semibold text-foreground">{s.name}</span>
                {s.group ? (
                  <Badge variant="secondary" className="text-[10px]">
                    组:{s.group}
                  </Badge>
                ) : null}
              </div>
              <div className="flex items-center gap-1.5 text-xs text-muted-foreground">
                <Server className="size-3 text-primary/70" />
                <span>{client ? client.name : s.client_id.slice(0, 8)}</span>
              </div>
            </div>
          );
        },
      },
      {
        id: 'pipeline',
        meta: { label: '链路拓扑 (入口 ──▶ 目标)' },
        header: '链路拓扑 (公网入口 ──▶ 内网终点)',
        cell: ({ row }) => {
          const s = row.original;
          const ingress = formatIngress(s);
          return (
            <div className="inline-flex flex-wrap items-center gap-2 rounded-xl border border-border/70 bg-muted/30 px-3 py-1.5 text-xs">
              <span className="flex items-center gap-1.5">
                {ingress.copyable ? (
                  <MonoCopy full={ingress.primary} className="font-medium text-foreground" />
                ) : (
                  <Mono className="text-muted-foreground">{ingress.primary}</Mono>
                )}
                {ingress.extraCount > 0 ? (
                  <Badge variant="secondary" className="px-1.5 py-0 text-[10px]">
                    +{ingress.extraCount}
                  </Badge>
                ) : null}
              </span>
              <ArrowRight className="size-3.5 shrink-0 text-primary" />
              {s.plugin ? (
                <span className="flex items-center gap-1.5">
                  <Mono className="font-semibold text-violet-600 dark:text-violet-400">
                    {s.plugin}
                  </Mono>
                  <Badge
                    variant="outline"
                    className="border-violet-500/40 px-1.5 py-0 text-[10px] text-violet-600 dark:text-violet-400"
                  >
                    插件
                  </Badge>
                </span>
              ) : (
                <Mono className="font-semibold text-emerald-600 dark:text-emerald-400">
                  {`${s.target_host}:${s.target_port}`}
                </Mono>
              )}
            </div>
          );
        },
      },
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
        cell: ({ row }) => renderRowActions(row.original),
      },
    ];

    return list;
  })();

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
      title={
        clients.length === 0
          ? '还没有边缘客户端'
          : searchQuery || statusFilter !== ALL_STATUS_FILTER || clientFilter !== ALL_CLIENTS_FILTER
            ? '没有匹配当前筛选条件的规则'
            : `还没有${noun}`
      }
      description={
        clients.length === 0
          ? `${noun}需要一个在线客户端承载,先创建客户端并接入服务端。`
          : searchQuery || statusFilter !== ALL_STATUS_FILTER || clientFilter !== ALL_CLIENTS_FILTER
            ? '可尝试清空搜索关键词或切换客户端/状态筛选器。'
            : `${noun}把公网入口的流量安全送进内网，创建后由承载客户端立即热生效。`
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

  const filterToolbar = (
    <div className="flex w-full flex-wrap items-center gap-2">
      <div className="relative w-full sm:w-64">
        <Search className="pointer-events-none absolute top-1/2 left-2.5 size-3.5 -translate-y-1/2 text-muted-foreground" />
        <Input
          value={searchQuery}
          onChange={(e) => setSearchQuery(e.target.value)}
          placeholder="搜索名称、端口、域名或目标…"
          className="h-8 pl-8 text-xs"
          aria-label="搜索转发规则"
        />
      </div>

      <Select value={clientFilter} onValueChange={handleClientFilterChange}>
        <SelectTrigger
          size="sm"
          className="min-w-32 flex-1 text-xs sm:w-40 sm:flex-initial"
          aria-label="按客户端筛选"
        >
          <SelectValue />
        </SelectTrigger>
        <SelectContent>
          <SelectItem value={ALL_CLIENTS_FILTER}>全部客户端</SelectItem>
          {clients.map((client) => (
            <SelectItem key={client.id} value={client.id}>
              {client.name}
            </SelectItem>
          ))}
        </SelectContent>
      </Select>

      <Select value={statusFilter} onValueChange={setStatusFilter}>
        <SelectTrigger
          size="sm"
          className="min-w-28 flex-1 text-xs sm:w-32 sm:flex-initial"
          aria-label="按状态筛选"
        >
          <SelectValue />
        </SelectTrigger>
        <SelectContent>
          <SelectItem value={ALL_STATUS_FILTER}>全部状态</SelectItem>
          <SelectItem value="running">运行中</SelectItem>
          <SelectItem value="issues">异常 / 告警</SelectItem>
          <SelectItem value="disabled">已停用</SelectItem>
        </SelectContent>
      </Select>

      {/* 视图模式切换:拓扑表格 vs 按客户端分组 */}
      <div className="inline-flex rounded-lg border border-border/80 bg-muted/40 p-0.5">
        <button
          type="button"
          onClick={() => setViewMode('table')}
          title="拓扑列表视图"
          className={cn(
            'inline-flex items-center gap-1 rounded-md px-2 py-1 text-xs font-medium transition-colors',
            viewMode === 'table'
              ? 'bg-background text-foreground shadow-2xs'
              : 'text-muted-foreground hover:text-foreground',
          )}
        >
          <LayoutList className="size-3.5" />
          <span className="max-sm:hidden">列表</span>
        </button>
        <button
          type="button"
          onClick={() => setViewMode('grouped')}
          title="按客户端节点分组视图"
          className={cn(
            'inline-flex items-center gap-1 rounded-md px-2 py-1 text-xs font-medium transition-colors',
            viewMode === 'grouped'
              ? 'bg-background text-foreground shadow-2xs'
              : 'text-muted-foreground hover:text-foreground',
          )}
        >
          <Server className="size-3.5" />
          <span className="max-sm:hidden">按节点</span>
        </button>
      </div>

      <Button
        variant="outline"
        size="sm"
        onClick={() => {
          setBatchOpen(true);
        }}
      >
        <Upload className="size-4" />
        <span className="max-xs:hidden">批量导入</span>
      </Button>
      <Button
        variant="outline"
        size="sm"
        onClick={() => invalidate()}
        disabled={servicesQuery.isFetching}
      >
        <RefreshCw className={`size-4 ${servicesQuery.isFetching ? 'animate-spin' : ''}`} />
        <span className="max-sm:hidden">刷新</span>
      </Button>
      {clients.length === 0 ? (
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
        <Button size="sm" onClick={() => openModal(null)} className="ml-auto sm:ml-0">
          <Plus className="size-4" />
          新建规则
        </Button>
      )}
    </div>
  );

  return (
    <div className="flex min-w-0 flex-col gap-4 sm:gap-5">
      {bare ? null : (
        <>
          <PageHeader
            eyebrow="TUNNEL & ROUTING FABRIC HUB"
            title={isAllLens ? '全部转发隧道' : (title ?? noun)}
            description={
              isAllLens
                ? '集中编排所有 TCP / UDP / HTTP(S) / TCPMux / STCP / SUDP / XTCP 转发链路'
                : description
            }
          />
          <TunnelHubLensBar />
        </>
      )}

      {viewMode === 'table' ? (
        <DataTable
          columns={columns}
          data={filtered}
          loading={servicesQuery.isLoading}
          empty={emptyNode}
          getRowId={(row) => String(row.id)}
          enableRowSelection
          onSelectedRowsChange={setSelected}
          mobileRow={(row) => {
            const pending = toggleMutation.isPending && toggleMutation.variables?.row.id === row.id;
            const checked = pending
              ? (toggleMutation.variables?.enabled ?? row.enabled)
              : row.enabled;
            const ingress = formatIngress(row);
            return (
              <div className="space-y-2.5">
                <div className="flex items-start justify-between gap-2">
                  <div className="flex min-w-0 flex-wrap items-center gap-1.5">
                    <span className="rh-mono shrink-0 rounded bg-primary/10 px-1.5 py-0.5 text-[10px] font-bold text-primary uppercase">
                      {row.kind}
                    </span>
                    <span className="truncate font-semibold">{row.name}</span>
                    <StatusBadge {...proxyStatusSignal(row.status, row.healthy)} />
                  </div>
                  <Switch
                    checked={checked}
                    disabled={pending}
                    aria-label={`${row.name} 启用`}
                    onCheckedChange={(next) => toggleMutation.mutate({ row, enabled: next })}
                  />
                </div>
                <div className="flex flex-wrap items-center gap-1.5 rounded-lg border border-border/70 bg-muted/30 px-2.5 py-1.5 text-xs">
                  <Mono className="break-all font-medium">{ingress.primary}</Mono>
                  <ArrowRight className="size-3 shrink-0 text-primary" />
                  <Mono className="break-all font-semibold text-emerald-600 dark:text-emerald-400">
                    {row.plugin ? `插件:${row.plugin}` : `${row.target_host}:${row.target_port}`}
                  </Mono>
                </div>
                <div className="flex flex-wrap items-center justify-between gap-2 border-t border-border/60 pt-2 text-xs text-muted-foreground">
                  <div className="flex items-center gap-2">
                    <span className="inline-flex items-center gap-1">
                      <Server className="size-3 text-primary/70" />
                      {clientMap.get(row.client_id)?.name ?? row.client_id.slice(0, 8)}
                    </span>
                    <span>·</span>
                    <span className="rh-mono">
                      池 {row.pool_ready} / 连接 {row.conns_current ?? 0}
                    </span>
                  </div>
                  {renderRowActions(row)}
                </div>
              </div>
            );
          }}
          toolbar={filterToolbar}
        />
      ) : (
        /* 按客户端节点分组拓扑视图 */
        <div className="space-y-4">
          <div className="flex flex-wrap items-center justify-between gap-2">{filterToolbar}</div>
          {groupedByClient.length === 0 ? (
            <div className="rounded-xl border border-border/80 bg-card p-6">{emptyNode}</div>
          ) : (
            <div className="space-y-4">
              {groupedByClient.map(([cId, group]) => {
                const client = group.client;
                return (
                  <Card key={cId} className="rh-telemetry-card gap-0 overflow-hidden py-0">
                    <CardHeader className="flex flex-wrap items-center justify-between gap-2.5 border-b border-border/70 bg-muted/30 px-3.5 py-3 sm:px-5 sm:py-3.5">
                      <div className="flex min-w-0 items-center gap-2.5 sm:gap-3">
                        <span className="flex size-8 shrink-0 items-center justify-center rounded-lg bg-primary/10 text-primary">
                          <Server className="size-4" />
                        </span>
                        <div className="min-w-0">
                          <div className="flex flex-wrap items-center gap-1.5 sm:gap-2">
                            <span className="truncate font-semibold">
                              {client ? client.name : cId}
                            </span>
                            <StatusBadge
                              signal={client?.online ? 'ok' : 'off'}
                              text={client?.online ? '在线' : '离线'}
                            />
                            <Badge variant="secondary" className="rh-mono text-[11px]">
                              {group.rules.length} 条隧道
                            </Badge>
                          </div>
                          {client?.hostname ? (
                            <p className="rh-mono truncate text-xs text-muted-foreground">
                              {client.hostname} · {client.os} · {client.client_version}
                            </p>
                          ) : null}
                        </div>
                      </div>
                      <Button
                        size="sm"
                        variant="outline"
                        onClick={() => openModal(null, cId)}
                        className="shrink-0 gap-1.5 text-xs"
                      >
                        <Plus className="size-3.5" />
                        在此节点新建
                      </Button>
                    </CardHeader>
                    <CardContent className="divide-y divide-border/60 p-0">
                      {group.rules.map((rule) => {
                        const ingress = formatIngress(rule);
                        const signal = proxyStatusSignal(rule.status, rule.healthy);
                        const pending =
                          toggleMutation.isPending && toggleMutation.variables?.row.id === rule.id;
                        const checked = pending
                          ? (toggleMutation.variables?.enabled ?? rule.enabled)
                          : rule.enabled;

                        return (
                          <div
                            key={rule.id}
                            className="flex flex-wrap items-center justify-between gap-2.5 px-3.5 py-3 text-sm transition-colors hover:bg-muted/20 sm:gap-3 sm:px-5"
                          >
                            <div className="flex min-w-0 items-center gap-2">
                              <span className="rh-mono shrink-0 rounded bg-primary/10 px-1.5 py-0.5 text-[10px] font-bold text-primary uppercase">
                                {rule.kind}
                              </span>
                              <span className="truncate font-semibold">{rule.name}</span>
                            </div>

                            <div className="inline-flex max-w-full flex-wrap items-center gap-1.5 rounded-xl border border-border/70 bg-muted/30 px-2.5 py-1 text-xs sm:gap-2 sm:px-3">
                              <Mono className="break-all font-medium">{ingress.primary}</Mono>
                              <ArrowRight className="size-3.5 shrink-0 text-primary" />
                              <Mono className="break-all font-semibold text-emerald-600 dark:text-emerald-400">
                                {rule.plugin
                                  ? `插件 · ${rule.plugin}`
                                  : `${rule.target_host}:${rule.target_port}`}
                              </Mono>
                            </div>

                            <div className="flex w-full flex-wrap items-center justify-between gap-3 text-xs sm:w-auto sm:justify-end sm:gap-4">
                              <div className="flex items-center gap-2">
                                <StatusBadge signal={signal.signal} text={signal.text} />
                                <span className="rh-mono text-muted-foreground">
                                  池 {rule.pool_ready} · 连接 {rule.conns_current ?? 0}
                                </span>
                              </div>
                              <div className="flex items-center gap-2">
                                <Switch
                                  checked={checked}
                                  disabled={pending}
                                  aria-label={`${rule.name} 启用`}
                                  onCheckedChange={(next) =>
                                    toggleMutation.mutate({ row: rule, enabled: next })
                                  }
                                />
                                {renderRowActions(rule)}
                              </div>
                            </div>
                          </div>
                        );
                      })}
                    </CardContent>
                  </Card>
                );
              })}
            </div>
          )}
        </div>
      )}

      {selected.length > 0 ? (
        <div className="rh-glass-dock sticky bottom-20 z-30 flex flex-wrap items-center justify-between gap-2 rounded-xl border border-primary/30 px-3.5 py-2.5 text-sm md:bottom-4 sm:px-4">
          <span className="font-medium text-foreground">
            已选中 <Mono className="text-primary">{selected.length}</Mono> 条转发规则
          </span>
          <div className="flex flex-wrap items-center gap-2">
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
        </div>
      ) : null}

      {modalEverOpened ? (
        <Suspense fallback={null}>
          <LazyProxyFormModal
            key={
              modal.editing
                ? `edit-${modal.editing.id}`
                : `new-${isAllLens ? 'all' : defaultKind}-${modal.presetClientId ?? ''}`
            }
            open={modal.open}
            onOpenChange={(open) => setModal({ open, editing: open ? modal.editing : null })}
            kinds={effectiveKinds}
            defaultKind={defaultKind}
            defaultClientId={modal.presetClientId}
            editing={modal.editing}
            clients={clients}
            submitting={saveMutation.isPending}
            onSubmit={async (spec) => {
              await saveMutation.mutateAsync({ editing: modal.editing, spec });
            }}
          />
        </Suspense>
      ) : null}

      <BatchImportDialog open={batchOpen} onOpenChange={setBatchOpen} />

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
