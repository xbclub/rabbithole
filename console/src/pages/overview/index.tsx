import { useQuery } from '@tanstack/react-query';
import {
  Activity,
  ArrowRight,
  ArrowRightLeft,
  ChevronDown,
  ChevronUp,
  Cpu,
  Inbox,
  Network,
  RefreshCw,
  Search,
  Server,
  Waypoints,
} from 'lucide-react';
import { Suspense, lazy, useMemo, useState } from 'react';
import { Link, useNavigate } from 'react-router';

import { listEvents, listServices, statsOverview } from '@/api/rabbithole';
import type { ServiceRow } from '@/api/rabbithole';
import { EmptyState } from '@/components/empty-state';
import { InfoLabel } from '@/components/info-label';
import { Mono } from '@/components/mono';
import { PageHeader } from '@/components/page-header';
import { StatusBadge, eventLevelSignal, proxyStatusSignal } from '@/components/status-badge';
import { useTrafficRateWindow } from '@/components/traffic-rate';
import { Button } from '@/components/ui/button';
import {
  Card,
  CardAction,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from '@/components/ui/card';
import { Input } from '@/components/ui/input';
import { Skeleton } from '@/components/ui/skeleton';
import { Tooltip, TooltipContent, TooltipTrigger } from '@/components/ui/tooltip';
import { fmtBytes, fmtTime } from '@/lib/format';
import { qk } from '@/lib/query-keys';
import { cn } from '@/lib/utils';
import { ROUTES } from '@/router/paths';

const TrafficRateChart = lazy(() =>
  import('@/components/traffic-chart').then((module) => ({
    default: module.TrafficRateChart,
  })),
);

const POLL_MS = 5000;
const EVENTS_LIMIT = 20;
const EVENTS_COLLAPSED = 6;

const KIND_ROUTE: Record<string, string> = {
  tcp: ROUTES.tcp,
  udp: ROUTES.udp,
  http: ROUTES.domains,
  https: ROUTES.domains,
  tcpmux: ROUTES.domains,
  stcp: ROUTES.secret,
  sudp: ROUTES.secret,
  xtcp: ROUTES.secret,
};

function listenAddress(rule: ServiceRow): string {
  const bind = rule.bind_port === 0 ? `${rule.bind_ip}:自动` : `${rule.bind_ip}:${rule.bind_port}`;
  if (rule.kind === 'tcp' || rule.kind === 'udp') return bind;

  const first = rule.custom_domains?.find((d) => d.length > 0);
  if (first) return first;
  if (rule.subdomain) return rule.subdomain;
  return bind;
}

type TunnelHealthFilter = 'all' | 'issues' | 'ok';
type EventLevelFilter = 'all' | 'info' | 'warn' | 'error';

export default function OverviewPage() {
  const navigate = useNavigate();

  const statsQ = useQuery({
    queryKey: qk.stats,
    queryFn: statsOverview,
    refetchInterval: POLL_MS,
  });
  const eventsQ = useQuery({
    queryKey: qk.events(EVENTS_LIMIT),
    queryFn: () => listEvents({ limit: EVENTS_LIMIT }),
    refetchInterval: POLL_MS,
  });
  const servicesQ = useQuery({
    queryKey: qk.services(),
    queryFn: () => listServices(),
    refetchInterval: POLL_MS,
  });

  const stats = statsQ.data;
  const rateFailed = statsQ.errorUpdatedAt > statsQ.dataUpdatedAt;
  const { currentIn, currentOut, points } = useTrafficRateWindow(
    stats,
    statsQ.dataUpdatedAt,
    rateFailed,
  );
  const hasRate = points.some((point) => point.in !== null);
  const rateIn = hasRate ? `${fmtBytes(currentIn)}/s` : '-';
  const rateOut = hasRate ? `${fmtBytes(currentOut)}/s` : '-';

  const peakIn = useMemo(() => {
    const vals = points.map((p) => p.in).filter((v): v is number => v !== null);
    return vals.length > 0 ? Math.max(...vals) : null;
  }, [points]);
  const peakOut = useMemo(() => {
    const vals = points.map((p) => p.out).filter((v): v is number => v !== null);
    return vals.length > 0 ? Math.max(...vals) : null;
  }, [points]);

  const allServices = useMemo(() => servicesQ.data ?? [], [servicesQ.data]);
  const enabledTunnels = useMemo(
    () => allServices.filter((rule) => rule.enabled && rule.status !== 'disabled'),
    [allServices],
  );

  const issueCount = useMemo(
    () =>
      enabledTunnels.filter(
        (r) =>
          r.status === 'bind_error' ||
          r.status === 'apply_error' ||
          r.status === 'offline' ||
          (r.status === 'running' && !r.healthy),
      ).length,
    [enabledTunnels],
  );

  const totalPoolReady = useMemo(
    () => enabledTunnels.reduce((acc, r) => acc + (r.pool_ready || 0), 0),
    [enabledTunnels],
  );

  const [healthFilter, setHealthFilter] = useState<TunnelHealthFilter>('all');
  const filteredTunnels = useMemo(() => {
    if (healthFilter === 'issues') {
      return enabledTunnels.filter((r) => r.status !== 'running' || !r.healthy);
    }
    if (healthFilter === 'ok') {
      return enabledTunnels.filter((r) => r.status === 'running' && r.healthy);
    }
    return enabledTunnels;
  }, [enabledTunnels, healthFilter]);

  const events = useMemo(() => eventsQ.data ?? [], [eventsQ.data]);
  const [eventLevel, setEventLevel] = useState<EventLevelFilter>('all');
  const [eventQuery, setEventQuery] = useState('');
  const [eventsExpanded, setEventsExpanded] = useState(false);

  const filteredEvents = useMemo(() => {
    return events.filter((ev) => {
      if (eventLevel !== 'all') {
        const lv = ev.level.toLowerCase();
        if (eventLevel === 'warn' && lv !== 'warn' && lv !== 'warning') return false;
        if (eventLevel !== 'warn' && lv !== eventLevel) return false;
      }
      if (eventQuery.trim()) {
        const q = eventQuery.trim().toLowerCase();
        return ev.message.toLowerCase().includes(q) || ev.source.toLowerCase().includes(q);
      }
      return true;
    });
  }, [events, eventLevel, eventQuery]);

  const shownEvents = eventsExpanded ? filteredEvents : filteredEvents.slice(0, EVENTS_COLLAPSED);

  const statsLoading = statsQ.isPending;
  const onlineRatio =
    stats && stats.clients_total > 0
      ? Math.round((stats.clients_online / stats.clients_total) * 100)
      : 0;
  const runningRatio =
    stats && stats.services_total > 0
      ? Math.round((stats.services_running / stats.services_total) * 100)
      : 0;

  const manualRefresh = () => {
    void statsQ.refetch();
    void servicesQ.refetch();
    void eventsQ.refetch();
  };

  return (
    <div className="space-y-6">
      <PageHeader
        eyebrow={
          <>
            <span className="size-1.5 rounded-full bg-emerald-500 rh-pulse-dot" />
            <span>REAL-TIME TELEMETRY & FABRIC MATRIX</span>
          </>
        }
        title="网络态势总览"
        description="实时监控边缘客户端在线率、隧道数据面健康度、双通道吞吐速率与控制面事件流"
        actions={
          <div className="flex flex-wrap items-center gap-2">
            {issueCount > 0 ? (
              <StatusBadge signal="warn" text={`${issueCount} 条隧道需关注`} />
            ) : (
              <StatusBadge signal="ok" text="数据面运行健康" />
            )}
            <Button
              variant="outline"
              size="sm"
              onClick={manualRefresh}
              className="gap-1.5"
              disabled={statsQ.isFetching || servicesQ.isFetching}
            >
              <RefreshCw
                className={cn(
                  'size-3.5',
                  (statsQ.isFetching || servicesQ.isFetching) && 'animate-spin',
                )}
              />
              刷新遥测
            </Button>
            <Button size="sm" asChild className="gap-1.5">
              <Link to={ROUTES.tcp}>
                <ArrowRightLeft className="size-3.5" />
                进入隧道中枢
              </Link>
            </Button>
          </div>
        }
      />

      {/* 第一层:Bento 4 维核心遥测矩阵 */}
      <div className="grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
        {/* 卡片 1:边缘客户端节点 */}
        <Card className="rh-telemetry-card justify-between gap-4 py-5">
          <CardHeader className="pb-0">
            <div className="flex items-center justify-between">
              <span className="text-xs font-medium text-muted-foreground">
                <InfoLabel infoKey="overview.clients_online" title="边缘节点态势" />
              </span>
              <span className="flex size-8 items-center justify-center rounded-lg bg-primary/10 text-primary">
                <Server className="size-4" />
              </span>
            </div>
          </CardHeader>
          <CardContent className="space-y-3">
            {statsLoading ? (
              <Skeleton className="h-9 w-32" />
            ) : (
              <div className="flex items-baseline justify-between">
                <div className="flex items-baseline gap-1.5">
                  <Mono className="text-3xl font-bold tracking-tight">
                    {stats ? stats.clients_online : '-'}
                  </Mono>
                  <Mono className="text-sm text-muted-foreground">
                    / {stats ? stats.clients_total : '-'} 在线
                  </Mono>
                </div>
                <Mono className="text-xs font-medium text-emerald-600 dark:text-emerald-400">
                  {onlineRatio}%
                </Mono>
              </div>
            )}
            <div className="h-1.5 w-full overflow-hidden rounded-full bg-muted">
              <div
                className="h-full rounded-full bg-emerald-500 transition-all duration-500"
                style={{ width: `${onlineRatio}%` }}
              />
            </div>
            <div className="flex items-center justify-between pt-0.5 text-xs text-muted-foreground">
              <span>
                离线节点:{' '}
                <Mono className="text-foreground">
                  {stats ? Math.max(0, stats.clients_total - stats.clients_online) : '-'}
                </Mono>
              </span>
              <Link
                to={ROUTES.clients}
                className="inline-flex items-center gap-1 font-medium text-primary hover:underline"
              >
                管理节点 <ArrowRight className="size-3" />
              </Link>
            </div>
          </CardContent>
        </Card>

        {/* 卡片 2:隧道运行健康度 */}
        <Card className="rh-telemetry-card justify-between gap-4 py-5">
          <CardHeader className="pb-0">
            <div className="flex items-center justify-between">
              <span className="text-xs font-medium text-muted-foreground">
                <InfoLabel infoKey="overview.services_running" title="隧道转发健康" />
              </span>
              <span className="flex size-8 items-center justify-center rounded-lg bg-emerald-500/10 text-emerald-600 dark:text-emerald-400">
                <Waypoints className="size-4" />
              </span>
            </div>
          </CardHeader>
          <CardContent className="space-y-3">
            {statsLoading ? (
              <Skeleton className="h-9 w-32" />
            ) : (
              <div className="flex items-baseline justify-between">
                <div className="flex items-baseline gap-1.5">
                  <Mono className="text-3xl font-bold tracking-tight">
                    {stats ? stats.services_running : '-'}
                  </Mono>
                  <Mono className="text-sm text-muted-foreground">
                    / {stats ? stats.services_total : '-'} 运行中
                  </Mono>
                </div>
                <Mono className="text-xs font-medium text-primary">{runningRatio}%</Mono>
              </div>
            )}
            <div className="h-1.5 w-full overflow-hidden rounded-full bg-muted">
              <div
                className="h-full rounded-full bg-primary transition-all duration-500"
                style={{ width: `${runningRatio}%` }}
              />
            </div>
            <div className="flex items-center justify-between pt-0.5 text-xs text-muted-foreground">
              <span>
                异常/待就绪: <Mono className="text-foreground">{issueCount}</Mono>
              </span>
              <Link
                to={ROUTES.tcp}
                className="inline-flex items-center gap-1 font-medium text-primary hover:underline"
              >
                编排规则 <ArrowRight className="size-3" />
              </Link>
            </div>
          </CardContent>
        </Card>

        {/* 卡片 3:并发流与预热连接池 */}
        <Card className="rh-telemetry-card justify-between gap-4 py-5">
          <CardHeader className="pb-0">
            <div className="flex items-center justify-between">
              <span className="text-xs font-medium text-muted-foreground">
                <InfoLabel infoKey="overview.conns_current" title="活跃连接与预热池" />
              </span>
              <span className="flex size-8 items-center justify-center rounded-lg bg-primary/10 text-primary">
                <Network className="size-4" />
              </span>
            </div>
          </CardHeader>
          <CardContent className="space-y-3">
            {statsLoading ? (
              <Skeleton className="h-9 w-28" />
            ) : (
              <div className="flex items-baseline justify-between">
                <div className="flex items-baseline gap-1.5">
                  <Mono className="text-3xl font-bold tracking-tight">
                    {stats ? stats.conns_current : '-'}
                  </Mono>
                  <span className="text-xs text-muted-foreground">并发通道</span>
                </div>
                <span className="rounded-md bg-muted px-2 py-0.5 text-xs text-muted-foreground">
                  就绪池 <Mono className="font-semibold text-foreground">{totalPoolReady}</Mono>
                </span>
              </div>
            )}
            <div className="grid grid-cols-2 gap-2 rounded-lg border border-border/60 bg-muted/30 px-2.5 py-1.5 text-xs">
              <div className="flex items-center justify-between">
                <span className="text-muted-foreground">启用规则</span>
                <Mono className="font-medium">{enabledTunnels.length}</Mono>
              </div>
              <div className="flex items-center justify-between">
                <span className="text-muted-foreground">总计规则</span>
                <Mono className="font-medium">{allServices.length}</Mono>
              </div>
            </div>
          </CardContent>
        </Card>

        {/* 卡片 4:实时吞吐与累计流量 */}
        <Card className="rh-telemetry-card justify-between gap-4 py-5">
          <CardHeader className="pb-0">
            <div className="flex items-center justify-between">
              <span className="text-xs font-medium text-muted-foreground">
                <InfoLabel infoKey="overview.bytes_total" title="实时吞吐与累计流量" />
              </span>
              <span className="flex size-8 items-center justify-center rounded-lg bg-emerald-500/10 text-emerald-600 dark:text-emerald-400">
                <Cpu className="size-4" />
              </span>
            </div>
          </CardHeader>
          <CardContent className="space-y-3">
            {statsLoading ? (
              <Skeleton className="h-9 w-36" />
            ) : (
              <div className="flex items-baseline justify-between">
                <div>
                  <div className="text-[11px] text-muted-foreground">累计总流量</div>
                  <Mono className="text-2xl font-bold tracking-tight">
                    {stats ? fmtBytes(stats.bytes_in + stats.bytes_out) : '-'}
                  </Mono>
                </div>
                <div className="text-right text-xs">
                  <div className="text-muted-foreground">
                    入 <Mono className="font-medium text-foreground">{rateIn}</Mono>
                  </div>
                  <div className="text-muted-foreground">
                    出 <Mono className="font-medium text-foreground">{rateOut}</Mono>
                  </div>
                </div>
              </div>
            )}
            <div className="grid grid-cols-2 gap-2 rounded-lg border border-border/60 bg-muted/30 px-2.5 py-1.5 text-xs">
              <div className="flex items-center justify-between gap-1">
                <span className="text-muted-foreground">累计入</span>
                <Mono className="truncate font-medium">
                  {stats ? fmtBytes(stats.bytes_in) : '-'}
                </Mono>
              </div>
              <div className="flex items-center justify-between gap-1">
                <span className="text-muted-foreground">累计出</span>
                <Mono className="truncate font-medium">
                  {stats ? fmtBytes(stats.bytes_out) : '-'}
                </Mono>
              </div>
            </div>
          </CardContent>
        </Card>
      </div>

      {/* 第二层:左 7 右 5 双面板 (吞吐示波器 + 转发链路实时巡检) */}
      <div className="grid gap-4 xl:grid-cols-12">
        {/* 左侧 7 列:双通道实时吞吐示波器 */}
        <Card className="rh-telemetry-card xl:col-span-7">
          <CardHeader>
            <div className="flex items-center gap-2">
              <Activity className="size-4 text-primary" />
              <CardTitle>双通道实时流量速率</CardTitle>
            </div>
            <CardDescription>近 5 分钟滚动窗口 · 每 5 秒差分采样</CardDescription>
            <CardAction>
              <div className="flex flex-wrap items-center justify-start gap-2 text-xs sm:justify-end sm:gap-3">
                <div className="flex items-center gap-1.5 rounded-lg border border-border/70 bg-muted/30 px-2.5 py-1">
                  <span
                    aria-hidden="true"
                    className="size-2 shrink-0 rounded-full bg-(--chart-1)"
                  />
                  <span className="text-muted-foreground">
                    <InfoLabel infoKey="overview.rate_in" title="入站" />
                  </span>
                  <Mono className="font-semibold text-foreground">{rateIn}</Mono>
                  {peakIn !== null ? (
                    <span className="text-[11px] text-muted-foreground">
                      (峰 {fmtBytes(peakIn)}/s)
                    </span>
                  ) : null}
                </div>
                <div className="flex items-center gap-1.5 rounded-lg border border-border/70 bg-muted/30 px-2.5 py-1">
                  <span
                    aria-hidden="true"
                    className="size-2 shrink-0 rounded-full bg-(--chart-2)"
                  />
                  <span className="text-muted-foreground">
                    <InfoLabel infoKey="overview.rate_out" title="出站" />
                  </span>
                  <Mono className="font-semibold text-foreground">{rateOut}</Mono>
                  {peakOut !== null ? (
                    <span className="text-[11px] text-muted-foreground">
                      (峰 {fmtBytes(peakOut)}/s)
                    </span>
                  ) : null}
                </div>
              </div>
            </CardAction>
          </CardHeader>
          <CardContent>
            {statsLoading ? (
              <Skeleton className="h-[240px] w-full" />
            ) : (
              <Suspense fallback={<Skeleton className="h-[240px] w-full" />}>
                <TrafficRateChart points={points} className="h-[240px]" />
              </Suspense>
            )}
          </CardContent>
        </Card>

        {/* 右侧 5 列:转发规则健康巡检舱 */}
        <Card className="rh-telemetry-card flex flex-col xl:col-span-5">
          <CardHeader className="pb-3">
            <CardTitle className="flex items-center gap-2">
              <Waypoints className="size-4 text-primary" />
              转发规则健康巡检
            </CardTitle>
            <CardDescription>
              {servicesQ.isPending
                ? '加载中…'
                : `共 ${enabledTunnels.length} 条启用规则 · 点击规则可直达配置`}
            </CardDescription>
            <CardAction>
              <div className="grid w-full grid-cols-3 rounded-lg border border-border/70 bg-muted/40 p-0.5 text-xs sm:inline-flex sm:w-auto">
                {(
                  [
                    { key: 'all', label: `全部 (${enabledTunnels.length})` },
                    { key: 'issues', label: `告警 (${issueCount})` },
                    {
                      key: 'ok',
                      label: `正常 (${Math.max(0, enabledTunnels.length - issueCount)})`,
                    },
                  ] as const
                ).map((item) => (
                  <button
                    key={item.key}
                    type="button"
                    onClick={() => setHealthFilter(item.key)}
                    className={cn(
                      'rounded-md px-2 py-1 text-center text-[11px] font-medium transition-colors',
                      healthFilter === item.key
                        ? 'bg-background text-foreground shadow-2xs'
                        : 'text-muted-foreground hover:text-foreground',
                    )}
                  >
                    {item.label}
                  </button>
                ))}
              </div>
            </CardAction>
          </CardHeader>
          <CardContent className="max-h-[260px] flex-1 overflow-y-auto pt-0">
            {servicesQ.isPending ? (
              <div className="space-y-2">
                {[0, 1, 2, 3].map((index) => (
                  <Skeleton className="h-12 w-full" key={index} />
                ))}
              </div>
            ) : filteredTunnels.length === 0 ? (
              <EmptyState
                description={
                  enabledTunnels.length === 0
                    ? '先到「边缘节点」接入客户端,再创建转发隧道;启用后这里实时展示链路健康与连接池。'
                    : '当前筛选条件下没有转发规则。'
                }
                icon={<Waypoints aria-hidden="true" className="size-4" />}
                title={enabledTunnels.length === 0 ? '暂无启用中的转发规则' : '无匹配规则'}
              />
            ) : (
              <div className="space-y-2">
                {filteredTunnels.map((rule) => {
                  const signal = proxyStatusSignal(rule.status, rule.healthy);
                  const targetRoute = KIND_ROUTE[rule.kind] ?? ROUTES.tcp;
                  return (
                    <div
                      key={rule.id}
                      onClick={() => navigate(`${targetRoute}?q=${encodeURIComponent(rule.name)}`)}
                      role="button"
                      tabIndex={0}
                      onKeyDown={(e) => {
                        if (e.key === 'Enter' || e.key === ' ') {
                          e.preventDefault();
                          navigate(`${targetRoute}?q=${encodeURIComponent(rule.name)}`);
                        }
                      }}
                      className="group flex flex-col gap-1.5 rounded-xl border border-border/70 bg-background/60 px-3 py-2.5 text-xs transition-colors hover:border-primary/40 hover:bg-accent/30"
                    >
                      <div className="flex items-center justify-between gap-2">
                        <div className="flex min-w-0 items-center gap-2">
                          <span className="rh-mono rounded bg-primary/10 px-1.5 py-0.5 text-[10px] font-semibold text-primary uppercase">
                            {rule.kind}
                          </span>
                          <span className="truncate font-semibold text-foreground group-hover:text-primary">
                            {rule.name}
                          </span>
                        </div>
                        {rule.status_reason ? (
                          <Tooltip>
                            <TooltipTrigger asChild>
                              <span
                                className="inline-flex cursor-help"
                                onClick={(e) => e.stopPropagation()}
                              >
                                <StatusBadge signal={signal.signal} text={signal.text} />
                              </span>
                            </TooltipTrigger>
                            <TooltipContent>{rule.status_reason}</TooltipContent>
                          </Tooltip>
                        ) : (
                          <StatusBadge signal={signal.signal} text={signal.text} />
                        )}
                      </div>

                      <div className="flex flex-wrap items-center justify-between gap-2 text-muted-foreground">
                        <div className="flex min-w-0 items-center gap-1.5">
                          <Mono className="truncate text-xs">{listenAddress(rule)}</Mono>
                          <ArrowRight className="size-3 shrink-0 text-primary/70" />
                          <Mono className="truncate text-xs">
                            {rule.plugin
                              ? `插件:${rule.plugin}`
                              : `${rule.target_host}:${rule.target_port}`}
                          </Mono>
                        </div>
                        <div className="flex shrink-0 items-center gap-2.5 text-[11px]">
                          <span>
                            池 <Mono className="text-foreground">{rule.pool_ready}</Mono>
                          </span>
                          <span>
                            连接{' '}
                            <Mono className="text-foreground">{rule.conns_current ?? '-'}</Mono>
                          </span>
                        </div>
                      </div>
                    </div>
                  );
                })}
              </div>
            )}
          </CardContent>
        </Card>
      </div>

      {/* 第三层:实时事件审计时间轴 (支持级别过滤与搜索) */}
      <Card className="rh-telemetry-card">
        <CardHeader>
          <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
            <div>
              <CardTitle>控制面事件流</CardTitle>
              <CardDescription className="mt-1">
                客户端上下线、隧道下发与认证审计实时日志 · 最新 {EVENTS_LIMIT} 条
              </CardDescription>
            </div>
            <div className="flex w-full flex-col gap-2 sm:w-auto sm:flex-row sm:flex-wrap sm:items-center">
              <div className="grid w-full grid-cols-4 rounded-lg border border-border/70 bg-muted/40 p-0.5 text-xs sm:inline-flex sm:w-auto">
                {(
                  [
                    { key: 'all', label: '全部' },
                    { key: 'info', label: 'INFO' },
                    { key: 'warn', label: 'WARN' },
                    { key: 'error', label: 'ERROR' },
                  ] as const
                ).map((item) => (
                  <button
                    key={item.key}
                    type="button"
                    onClick={() => setEventLevel(item.key)}
                    className={cn(
                      'rounded-md px-2.5 py-1 text-center text-[11px] font-medium transition-colors',
                      eventLevel === item.key
                        ? 'bg-background text-foreground shadow-2xs'
                        : 'text-muted-foreground hover:text-foreground',
                    )}
                  >
                    {item.label}
                  </button>
                ))}
              </div>
              <div className="relative w-full sm:w-48">
                <Search className="pointer-events-none absolute top-1/2 left-2.5 size-3.5 -translate-y-1/2 text-muted-foreground" />
                <Input
                  value={eventQuery}
                  onChange={(e) => setEventQuery(e.target.value)}
                  placeholder="过滤来源或消息…"
                  className="h-8 pl-8 text-xs"
                  aria-label="过滤事件日志"
                />
              </div>
              {filteredEvents.length > EVENTS_COLLAPSED ? (
                <Button
                  aria-expanded={eventsExpanded}
                  onClick={() => setEventsExpanded((value) => !value)}
                  size="sm"
                  variant="ghost"
                  className="h-8 text-xs"
                >
                  {eventsExpanded ? '收起' : `展开全部 (${filteredEvents.length})`}
                  {eventsExpanded ? (
                    <ChevronUp aria-hidden="true" className="size-3.5" />
                  ) : (
                    <ChevronDown aria-hidden="true" className="size-3.5" />
                  )}
                </Button>
              ) : null}
            </div>
          </div>
        </CardHeader>
        <CardContent>
          {eventsQ.isPending ? (
            <div className="space-y-2">
              {[0, 1, 2, 3].map((index) => (
                <Skeleton className="h-10 w-full" key={index} />
              ))}
            </div>
          ) : filteredEvents.length === 0 ? (
            <EmptyState
              description={
                events.length === 0
                  ? '客户端上下线、转发规则发布结果都会记录在这里;当前还没有任何事件。'
                  : '没有符合当前筛选级别的事件记录。'
              }
              icon={<Inbox aria-hidden="true" className="size-4" />}
              title={events.length === 0 ? '暂无事件' : '无匹配事件'}
            />
          ) : (
            <div className="divide-y divide-border/60">
              {shownEvents.map((event) => {
                const sig = eventLevelSignal(event.level);
                return (
                  <div
                    key={event.id}
                    className="flex flex-col gap-1.5 py-2.5 text-sm sm:grid sm:grid-cols-[165px_88px_110px_1fr] sm:items-center sm:gap-2"
                  >
                    <div className="flex flex-wrap items-center justify-between gap-2 sm:contents">
                      <Mono className="text-xs text-muted-foreground">{fmtTime(event.ts)}</Mono>
                      <div className="flex items-center gap-1.5 sm:contents">
                        <div>
                          <StatusBadge signal={sig} text={event.level.toUpperCase()} />
                        </div>
                        <div>
                          <Mono className="rounded bg-muted px-2 py-0.5 text-xs text-muted-foreground">
                            {event.source}
                          </Mono>
                        </div>
                      </div>
                    </div>
                    <div className="min-w-0 text-xs leading-relaxed break-words text-foreground sm:text-sm">
                      {event.message}
                    </div>
                  </div>
                );
              })}
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  );
}
