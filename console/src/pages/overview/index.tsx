import { useQuery } from '@tanstack/react-query';
import { ChevronDown, ChevronUp, Inbox, Waypoints } from 'lucide-react';
import { Suspense, lazy, useState } from 'react';
import type { ReactNode } from 'react';

import type { ServiceRow } from '@/api/rabbithole';

/**
 * 图表按需加载:recharts 约 367 KB,是总览页最大的一块。
 * 指标条与事件先渲染,图表随后到(期间显示骨架),避免整页等图表库。
 */
const TrafficRateChart = lazy(() =>
  import('@/components/traffic-chart').then((module) => ({
    default: module.TrafficRateChart,
  })),
);
import { listEvents, listServices, statsOverview } from '@/api/rabbithole';
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
import { Skeleton } from '@/components/ui/skeleton';
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table';
import { Tooltip, TooltipContent, TooltipTrigger } from '@/components/ui/tooltip';
import type { FieldInfoKey } from '@/features/field-info';
import { fmtBytes, fmtTime } from '@/lib/format';
import { qk } from '@/lib/query-keys';

/** 轮询间隔:指标条 / 速率窗口 / 最近事件统一 5 秒。 */
const POLL_MS = 5000;
/** 最近事件拉取条数(与 qk.events 的 key 一一对应)。 */
const EVENTS_LIMIT = 20;
/** 事件表默认展示条数,其余点「展开全部」查看。 */
const EVENTS_COLLAPSED = 5;

/**
 * 转发规则的监听标识:TCP/UDP 是 bind_ip:bind_port;HTTP(S) 走域名/子域;
 * 其余 kind 的 spec 段里没有域名,回落到 bind 地址(旧版此处显示占位符「—」)。
 */
function listenAddress(rule: ServiceRow): string {
  const bind = `${rule.bind_ip}:${rule.bind_port}`;
  if (rule.kind === 'tcp' || rule.kind === 'udp') return bind;

  const section = rule.spec?.[rule.kind];
  if (section !== null && typeof section === 'object') {
    const spec = section as Record<string, unknown>;
    const domains = spec.custom_domains;
    const first = Array.isArray(domains) ? domains.find((d) => typeof d === 'string') : undefined;
    if (typeof first === 'string' && first.length > 0) return first;
    if (typeof spec.subdomain === 'string' && spec.subdomain.length > 0) return spec.subdomain;
  }
  return bind;
}

/** 指标条单格:名词 + 说明浮层 + mono 数值(tabular-nums,轮询刷新不抖动)。 */
function MetricCell({
  infoKey,
  loading,
  suffix,
  title,
  value,
}: {
  infoKey: FieldInfoKey;
  loading: boolean;
  suffix?: ReactNode;
  title: string;
  value: ReactNode;
}) {
  return (
    <div className="bg-card px-4 py-3.5">
      <div className="text-xs text-muted-foreground">
        <InfoLabel infoKey={infoKey} title={title} />
      </div>
      {loading ? (
        <Skeleton className="mt-2 h-7 w-24" />
      ) : (
        <div className="mt-2 flex items-baseline gap-1.5">
          <Mono className="text-2xl leading-none font-semibold">{value}</Mono>
          {suffix === undefined ? null : <Mono className="text-muted-foreground">{suffix}</Mono>}
        </div>
      )}
    </div>
  );
}

/**
 * 总览页:指标条 / 流量速率 + 转发规则健康 / 最近事件。
 * 三个查询都是 5 秒轮询;queryKey 稳定,失败时 TanStack Query 保留上一次成功的 data
 * (不写 placeholderData:key 不变时它不会带来额外语义,失败清空只可能由换 key 引起)。
 */
export default function OverviewPage() {
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
  });

  const stats = statsQ.data;
  // 失败判定:最近一次失败晚于最近一次成功 → 速率窗口在该处断线(不补零、不外推)
  const rateFailed = statsQ.errorUpdatedAt > statsQ.dataUpdatedAt;
  const { currentIn, currentOut, points } = useTrafficRateWindow(
    stats,
    statsQ.dataUpdatedAt,
    rateFailed,
  );
  // 至少有一个真实速率点才显示速率,否则显示 `-`(不把「还没采到」伪装成 0 B/s)
  const hasRate = points.some((point) => point.in !== null);
  const rateIn = hasRate ? `${fmtBytes(currentIn)}/s` : '-';
  const rateOut = hasRate ? `${fmtBytes(currentOut)}/s` : '-';

  const events = eventsQ.data ?? [];
  const [eventsExpanded, setEventsExpanded] = useState(false);
  const shownEvents = eventsExpanded ? events : events.slice(0, EVENTS_COLLAPSED);

  // 转发规则健康面板:启用中的规则(enabled 且状态不是 disabled)
  const tunnels = (servicesQ.data ?? []).filter(
    (rule) => rule.enabled && rule.status !== 'disabled',
  );

  const statsLoading = statsQ.isPending;

  return (
    <div className="space-y-4">
      <PageHeader description="服务端运行状态与转发面健康" title="总览" />

      {/* 指标条:单卡内部分格(gap-px + bg-border 画分隔线),6 个指标 */}
      <Card className="gap-0 overflow-hidden py-0">
        <div className="grid grid-cols-2 gap-px bg-border sm:grid-cols-3 xl:grid-cols-6">
          <MetricCell
            infoKey="overview.clients_online"
            loading={statsLoading}
            suffix={stats ? `/ ${stats.clients_total}` : undefined}
            title="在线客户端"
            value={stats ? stats.clients_online : '-'}
          />
          <MetricCell
            infoKey="overview.services_running"
            loading={statsLoading}
            suffix={stats ? `/ ${stats.services_total}` : undefined}
            title="运行规则"
            value={stats ? stats.services_running : '-'}
          />
          <MetricCell
            infoKey="overview.conns_current"
            loading={statsLoading}
            title="当前连接"
            value={stats ? stats.conns_current : '-'}
          />
          <MetricCell
            infoKey="overview.rate_in"
            loading={statsLoading}
            title="入站速率"
            value={rateIn}
          />
          <MetricCell
            infoKey="overview.rate_out"
            loading={statsLoading}
            title="出站速率"
            value={rateOut}
          />
          <MetricCell
            infoKey="overview.bytes_total"
            loading={statsLoading}
            title="累计流量"
            value={stats ? fmtBytes(stats.bytes_in + stats.bytes_out) : '-'}
          />
        </div>
      </Card>

      <div className="grid gap-4 xl:grid-cols-[minmax(0,3fr)_minmax(0,2fr)]">
        {/* 流量速率图:渐变填充 + 右上角当前值 mono 标注 */}
        <Card>
          <CardHeader>
            <CardTitle>流量速率</CardTitle>
            <CardDescription>近 5 分钟 · 每 5 秒采样</CardDescription>
            <CardAction>
              <div className="flex flex-wrap items-center justify-end gap-x-3 gap-y-1 text-xs text-muted-foreground">
                <span className="flex items-center gap-1.5">
                  <span
                    aria-hidden="true"
                    className="size-2 shrink-0 rounded-[2px] bg-(--chart-1)"
                  />
                  入<Mono className="text-sm font-medium text-foreground">{rateIn}</Mono>
                </span>
                <span className="flex items-center gap-1.5">
                  <span
                    aria-hidden="true"
                    className="size-2 shrink-0 rounded-[2px] bg-(--chart-2)"
                  />
                  出<Mono className="text-sm font-medium text-foreground">{rateOut}</Mono>
                </span>
              </div>
            </CardAction>
          </CardHeader>
          <CardContent>
            {statsLoading ? (
              <Skeleton className="h-[220px] w-full" />
            ) : (
              <Suspense fallback={<Skeleton className="h-[220px] w-full" />}>
                <TrafficRateChart points={points} />
              </Suspense>
            )}
          </CardContent>
        </Card>

        {/* 转发规则健康:每条启用中的规则一行(名称 / 监听 / 状态 / 池 / 连接) */}
        <Card>
          <CardHeader>
            <CardTitle>转发规则健康</CardTitle>
            <CardDescription>
              {servicesQ.isPending ? '加载中…' : `${tunnels.length} 条启用中的转发规则`}
            </CardDescription>
          </CardHeader>
          <CardContent className="max-h-[320px] overflow-y-auto">
            {servicesQ.isPending ? (
              <div className="space-y-2">
                {[0, 1, 2, 3].map((index) => (
                  <Skeleton className="h-9 w-full" key={index} />
                ))}
              </div>
            ) : tunnels.length === 0 ? (
              <EmptyState
                description="先到「客户端」页接入客户端,再创建转发规则;启用后这里逐条显示监听地址与运行状态。"
                icon={<Waypoints aria-hidden="true" className="size-4" />}
                title="还没有启用中的转发规则"
              />
            ) : (
              <>
                <div className="hidden gap-x-3 border-b border-border/60 pb-2 text-xs text-muted-foreground sm:grid sm:grid-cols-[minmax(0,1.2fr)_minmax(0,1.1fr)_auto_auto_auto]">
                  <span>规则</span>
                  <span>监听</span>
                  <span>
                    <InfoLabel infoKey="service.list.status" title="状态" />
                  </span>
                  <span>
                    <InfoLabel infoKey="service.list.pool" title="池" />
                  </span>
                  <span>
                    <InfoLabel infoKey="service.list.conns" title="连接" />
                  </span>
                </div>
                {tunnels.map((rule) => {
                  const signal = proxyStatusSignal(rule.status, rule.healthy);
                  return (
                    <div
                      className="grid grid-cols-[minmax(0,1fr)_auto] items-center gap-x-3 gap-y-1 border-b border-border/60 py-2.5 text-sm last:border-b-0 sm:grid-cols-[minmax(0,1.2fr)_minmax(0,1.1fr)_auto_auto_auto]"
                      key={rule.id}
                    >
                      <span className="min-w-0 truncate font-medium">{rule.name}</span>
                      <Mono className="col-span-2 col-start-1 row-start-2 min-w-0 truncate text-muted-foreground sm:col-span-1 sm:col-start-2 sm:row-start-1">
                        {listenAddress(rule)}
                      </Mono>
                      <span className="col-start-2 row-start-1 justify-self-end sm:col-start-3 sm:justify-self-start">
                        {rule.status_reason ? (
                          <Tooltip>
                            <TooltipTrigger asChild>
                              <span className="inline-flex cursor-help">
                                <StatusBadge signal={signal.signal} text={signal.text} />
                              </span>
                            </TooltipTrigger>
                            <TooltipContent>{rule.status_reason}</TooltipContent>
                          </Tooltip>
                        ) : (
                          <StatusBadge signal={signal.signal} text={signal.text} />
                        )}
                      </span>
                      <Mono className="col-start-1 row-start-3 text-muted-foreground sm:col-start-4 sm:row-start-1">
                        池 {rule.pool_ready}
                      </Mono>
                      <Mono className="col-start-2 row-start-3 justify-self-end text-muted-foreground sm:col-start-5 sm:row-start-1 sm:justify-self-start">
                        连接 {rule.conns_current ?? '-'}
                      </Mono>
                    </div>
                  );
                })}
              </>
            )}
          </CardContent>
        </Card>
      </div>

      {/* 最近事件:默认 5 条,可展开全部 20 条 */}
      <Card>
        <CardHeader>
          <CardTitle>最近事件</CardTitle>
          <CardDescription>最新 {EVENTS_LIMIT} 条 · 每 5 秒刷新</CardDescription>
          <CardAction>
            {events.length > EVENTS_COLLAPSED ? (
              <Button
                aria-expanded={eventsExpanded}
                onClick={() => setEventsExpanded((value) => !value)}
                size="sm"
                variant="ghost"
              >
                {eventsExpanded ? '收起' : `展开全部(${events.length})`}
                {eventsExpanded ? (
                  <ChevronUp aria-hidden="true" className="size-4" />
                ) : (
                  <ChevronDown aria-hidden="true" className="size-4" />
                )}
              </Button>
            ) : null}
          </CardAction>
        </CardHeader>
        <CardContent>
          {eventsQ.isPending ? (
            <div className="space-y-2">
              {[0, 1, 2, 3].map((index) => (
                <Skeleton className="h-8 w-full" key={index} />
              ))}
            </div>
          ) : events.length === 0 ? (
            <EmptyState
              description="客户端上下线、转发规则发布结果都会记录在这里;当前还没有任何事件。"
              icon={<Inbox aria-hidden="true" className="size-4" />}
              title="暂无事件"
            />
          ) : (
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead className="w-[170px]">时间</TableHead>
                  <TableHead className="w-[92px]">级别</TableHead>
                  <TableHead className="hidden w-[92px] sm:table-cell">来源</TableHead>
                  <TableHead>消息</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {shownEvents.map((event) => (
                  <TableRow key={event.id}>
                    <TableCell className="text-muted-foreground">
                      <Mono>{fmtTime(event.ts)}</Mono>
                    </TableCell>
                    <TableCell>
                      <StatusBadge signal={eventLevelSignal(event.level)} text={event.level} />
                    </TableCell>
                    <TableCell className="hidden text-muted-foreground sm:table-cell">
                      <Mono>{event.source}</Mono>
                    </TableCell>
                    <TableCell className="whitespace-normal">{event.message}</TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          )}
        </CardContent>
      </Card>
    </div>
  );
}
