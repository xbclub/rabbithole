import { QueryClientProvider, useQuery } from '@tanstack/react-query';
import {
  Activity,
  ArrowRightLeft,
  Cable,
  ChevronRight,
  Globe,
  KeyRound,
  Layers,
  LayoutDashboard,
  Lock,
  LogOut,
  PanelLeft,
  PanelLeftClose,
  PanelLeftOpen,
  PanelTop,
  Radio,
  Server,
  Settings,
  ShieldCheck,
} from 'lucide-react';
import type { LucideIcon } from 'lucide-react';
import { useEffect, useMemo, useState } from 'react';
import { Link, NavLink, Outlet, useLocation, useNavigate, useSearchParams } from 'react-router';

import { logoutApi } from '@/api/auth';
import { listServices, listVisitors, statsOverview } from '@/api/rabbithole';
import { BrandMark } from '@/components/brand-mark';
import { CommandPalette, CommandPaletteTrigger } from '@/components/command-palette';
import { Mono } from '@/components/mono';
import { ThemeToggle } from '@/components/theme-toggle';
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from '@/components/ui/alert-dialog';
import { Avatar, AvatarFallback } from '@/components/ui/avatar';
import { Button } from '@/components/ui/button';
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu';
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/components/ui/tooltip';
import { feedbackSuccess } from '@/lib/feedback';
import { queryClient } from '@/lib/query-client';
import { qk } from '@/lib/query-keys';
import { cn } from '@/lib/utils';
import { PRIMARY_NAV_ITEMS, ROUTES } from '@/router/paths';
import { useAuthStore } from '@/stores/auth';

export type LayoutMode = 'dock' | 'sidebar';

const LAYOUT_MODE_KEY = 'rh_layout_mode';
const SIDEBAR_COLLAPSED_KEY = 'rh_sidebar_collapsed';

function readLayoutMode(): LayoutMode {
  try {
    const raw = localStorage.getItem(LAYOUT_MODE_KEY);
    return raw === 'sidebar' ? 'sidebar' : 'dock';
  } catch {
    return 'dock';
  }
}

function readSidebarCollapsed(): boolean {
  try {
    return localStorage.getItem(SIDEBAR_COLLAPSED_KEY) === '1';
  } catch {
    return false;
  }
}

const NAV_ICONS: Record<string, LucideIcon> = {
  dashboard: LayoutDashboard,
  clients: Server,
  tcp: ArrowRightLeft,
  certs: ShieldCheck,
  settings: Settings,
};

const MOBILE_SHORT_LABELS: Record<string, string> = {
  overview: '总览',
  clients: '边缘节点',
  tunnels: '隧道中枢',
  certs: 'TLS 证书',
  settings: '系统控制',
};

/** 顶栏实时遥测脉冲胶囊:常驻展示节点在线率与运行中隧道数。 */
function LiveTelemetryTicker() {
  const { data } = useQuery({
    queryKey: qk.stats,
    queryFn: statsOverview,
    refetchInterval: 5000,
  });

  if (!data) {
    return (
      <Link
        to={ROUTES.overview}
        className="hidden shrink-0 items-center gap-1.5 rounded-full border border-border/70 bg-muted/40 px-2.5 py-1 text-xs text-muted-foreground transition-colors hover:bg-accent/50 xl:inline-flex"
      >
        <Activity className="size-3.5 text-primary" />
        <span>同步中…</span>
      </Link>
    );
  }

  const hasOnline = data.clients_online > 0;

  return (
    <Link
      to={ROUTES.overview}
      title="实时网络遥测 · 点击返回态势总览"
      className="hidden shrink-0 items-center gap-2 rounded-full border border-border/80 bg-background/70 px-2.5 py-1 text-xs transition-colors hover:border-primary/40 hover:bg-accent/40 xl:inline-flex"
    >
      <span
        aria-hidden="true"
        className={cn(
          'inline-block size-2 rounded-full',
          hasOnline ? 'bg-emerald-500 rh-pulse-dot' : 'bg-zinc-400',
        )}
      />
      <span className="text-muted-foreground">节点</span>
      <Mono className="font-semibold text-foreground">
        {data.clients_online}/{data.clients_total}
      </Mono>
      <span className="text-border">·</span>
      <span className="text-muted-foreground">隧道</span>
      <Mono className="font-semibold text-foreground">
        {data.services_running}/{data.services_total}
      </Mono>
      <span className="text-border">·</span>
      <span className="text-muted-foreground">连接</span>
      <Mono className="font-semibold text-primary">{data.conns_current}</Mono>
    </Link>
  );
}

/** 桌面端布局模式微型切换器:精致双图标胶囊,不占顶栏横向空间。 */
function LayoutModeSwitch({
  mode,
  onChange,
}: {
  mode: LayoutMode;
  onChange: (next: LayoutMode) => void;
}) {
  return (
    <div
      role="group"
      aria-label="桌面端导航布局切换"
      className="hidden h-8 shrink-0 items-center gap-0.5 rounded-full border border-border/80 bg-muted/45 p-0.5 md:inline-flex"
    >
      <Tooltip>
        <TooltipTrigger asChild>
          <button
            type="button"
            onClick={() => onChange('dock')}
            aria-pressed={mode === 'dock'}
            aria-label="顶栏导航布局"
            className={cn(
              'flex size-6.5 items-center justify-center rounded-full transition-all',
              mode === 'dock'
                ? 'bg-primary text-primary-foreground shadow-2xs'
                : 'text-muted-foreground hover:text-foreground',
            )}
          >
            <PanelTop className="size-3.5" />
          </button>
        </TooltipTrigger>
        <TooltipContent>顶栏悬浮坞布局</TooltipContent>
      </Tooltip>

      <Tooltip>
        <TooltipTrigger asChild>
          <button
            type="button"
            onClick={() => onChange('sidebar')}
            aria-pressed={mode === 'sidebar'}
            aria-label="侧边栏导航布局"
            className={cn(
              'flex size-6.5 items-center justify-center rounded-full transition-all',
              mode === 'sidebar'
                ? 'bg-primary text-primary-foreground shadow-2xs'
                : 'text-muted-foreground hover:text-foreground',
            )}
          >
            <PanelLeft className="size-3.5" />
          </button>
        </TooltipTrigger>
        <TooltipContent>左侧导航栏布局</TooltipContent>
      </Tooltip>
    </div>
  );
}

function UserMenu({
  layoutMode,
  onToggleLayoutMode,
}: {
  layoutMode: LayoutMode;
  onToggleLayoutMode: () => void;
}) {
  const username = useAuthStore((state) => state.username);
  const navigate = useNavigate();
  const [confirmOpen, setConfirmOpen] = useState(false);
  const [pending, setPending] = useState(false);

  const doLogout = async () => {
    setPending(true);
    try {
      await logoutApi().catch(() => undefined);
    } finally {
      useAuthStore.getState().clear();
      setPending(false);
      setConfirmOpen(false);
      feedbackSuccess('已退出登录');
      navigate(ROUTES.login, { replace: true });
    }
  };

  const name = username ?? 'admin';

  return (
    <>
      <DropdownMenu>
        <DropdownMenuTrigger asChild>
          <Button
            variant="ghost"
            size="sm"
            aria-label={`账户:${name}`}
            className="h-8 shrink-0 gap-1.5 rounded-full border border-transparent px-1.5 hover:border-border/70 hover:bg-accent/70 sm:px-2"
          >
            <Avatar className="size-6 border border-primary/25">
              <AvatarFallback className="bg-primary/12 text-xs font-semibold text-primary">
                {name.slice(0, 1).toUpperCase()}
              </AvatarFallback>
            </Avatar>
            <span className="max-w-20 truncate text-xs font-medium max-lg:hidden">{name}</span>
          </Button>
        </DropdownMenuTrigger>
        <DropdownMenuContent align="end" className="w-56 rounded-xl p-1.5 shadow-xl">
          <DropdownMenuLabel className="flex flex-col gap-0.5 px-2.5 py-2">
            <span className="truncate text-sm font-semibold">{name}</span>
            <span className="rh-mono text-[11px] font-normal text-muted-foreground">
              rabbithole · v{__APP_VERSION__}
            </span>
          </DropdownMenuLabel>
          <DropdownMenuSeparator />
          <DropdownMenuItem
            className="rounded-lg max-md:hidden"
            onSelect={() => {
              onToggleLayoutMode();
            }}
          >
            {layoutMode === 'dock' ? (
              <>
                <PanelLeft className="size-4" />
                切换为侧边栏布局
              </>
            ) : (
              <>
                <PanelTop className="size-4" />
                切换为顶栏悬浮布局
              </>
            )}
          </DropdownMenuItem>
          <DropdownMenuItem
            className="rounded-lg"
            onSelect={() => navigate(`${ROUTES.settings}?tab=apikeys`)}
          >
            <KeyRound className="size-4" />
            API 密钥管理
          </DropdownMenuItem>
          <DropdownMenuItem
            className="rounded-lg"
            onSelect={() => navigate(`${ROUTES.settings}?tab=password`)}
          >
            <Lock className="size-4" />
            修改管理员密码
          </DropdownMenuItem>
          <DropdownMenuSeparator />
          <DropdownMenuItem
            onSelect={() => setConfirmOpen(true)}
            className="rounded-lg text-destructive focus:text-destructive"
          >
            <LogOut className="size-4" />
            退出登录
          </DropdownMenuItem>
        </DropdownMenuContent>
      </DropdownMenu>

      <AlertDialog open={confirmOpen} onOpenChange={(next) => !pending && setConfirmOpen(next)}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>退出登录?</AlertDialogTitle>
            <AlertDialogDescription>
              退出后当前管理会话立即失效,需要重新验证管理员凭据。
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel disabled={pending}>取消</AlertDialogCancel>
            <AlertDialogAction
              disabled={pending}
              onClick={(event) => {
                event.preventDefault();
                void doLogout();
              }}
            >
              {pending ? '退出中…' : '退出登录'}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </>
  );
}

/** 移动端底部悬浮导航坞 (<768px,两种桌面模式在手机端均统一使用该响应式导航)。 */
function MobileBottomDock() {
  const { pathname } = useLocation();

  return (
    <nav
      aria-label="移动端主导航"
      className="rh-glass-dock fixed right-2.5 bottom-2.5 left-2.5 z-40 grid grid-cols-5 gap-1 rounded-2xl border border-border/80 p-1 pb-[max(0.25rem,env(safe-area-inset-bottom))] shadow-lg md:hidden"
    >
      {PRIMARY_NAV_ITEMS.map((item) => {
        const Icon = NAV_ICONS[item.icon] ?? LayoutDashboard;
        const active = item.matchPaths.some((p) => pathname === p || pathname.startsWith(`${p}/`));
        const targetTo = item.key === 'tunnels' && active ? pathname : item.to;

        return (
          <NavLink
            key={item.key}
            to={targetTo}
            className={cn(
              'flex min-w-0 flex-col items-center justify-center gap-0.5 rounded-xl px-1 py-1.5 text-[11px] font-medium transition-colors',
              active
                ? 'bg-primary font-semibold text-primary-foreground shadow-2xs'
                : 'text-muted-foreground hover:text-foreground',
            )}
          >
            <Icon className="size-4 shrink-0" />
            <span className="max-w-full truncate">
              {MOBILE_SHORT_LABELS[item.key] ?? item.title}
            </span>
          </NavLink>
        );
      })}
    </nav>
  );
}

/** 模式 A: 顶部悬浮毛玻璃导航舱 (Top Dock Mode)。 */
function TopCommandDock({
  layoutMode,
  onLayoutModeChange,
}: {
  layoutMode: LayoutMode;
  onLayoutModeChange: (next: LayoutMode) => void;
}) {
  const { pathname } = useLocation();

  return (
    <header className="sticky top-0 z-40 w-full px-2.5 pt-2 sm:px-6 sm:pt-3">
      <div className="rh-glass-dock mx-auto flex h-12 max-w-[1600px] items-center justify-between gap-2 rounded-2xl border border-border/80 px-3 sm:h-14 sm:gap-3 sm:px-4">
        {/* 左侧:品牌徽标 + 版本号置于名称下方 (shrink-0 保证任何宽度下都不被挤压截断) */}
        <NavLink
          to={ROUTES.overview}
          className="group flex shrink-0 items-center gap-2.5 rounded-xl py-1 pr-1 transition-opacity hover:opacity-90"
        >
          <span className="flex size-8 shrink-0 items-center justify-center rounded-xl bg-gradient-to-br from-primary/20 via-primary/10 to-transparent ring-1 ring-primary/30 shadow-2xs transition-transform group-hover:scale-105">
            <BrandMark className="size-5" />
          </span>
          <div className="flex flex-col leading-none whitespace-nowrap">
            <span className="text-sm font-bold tracking-tight text-foreground">rabbithole</span>
            <span className="rh-mono mt-1 text-[10px] font-medium text-primary">
              v{__APP_VERSION__}
            </span>
          </div>
        </NavLink>

        {/* 居中:五大核心域胶囊导航 (桌面端显示) */}
        <nav
          aria-label="主导航"
          className="hidden items-center gap-1 rounded-full border border-border/75 bg-muted/50 p-1 md:flex"
        >
          {PRIMARY_NAV_ITEMS.map((item) => {
            const Icon = NAV_ICONS[item.icon] ?? LayoutDashboard;
            const active = item.matchPaths.some(
              (p) => pathname === p || pathname.startsWith(`${p}/`),
            );
            const targetTo = item.key === 'tunnels' && active ? pathname : item.to;

            return (
              <NavLink
                key={item.key}
                to={targetTo}
                className={cn(
                  'flex items-center gap-1.5 rounded-full px-3 py-1.5 text-xs font-medium whitespace-nowrap transition-all lg:px-3.5',
                  active
                    ? 'bg-primary text-primary-foreground shadow-xs'
                    : 'text-muted-foreground hover:bg-background/70 hover:text-foreground',
                )}
              >
                <Icon className="size-3.5 shrink-0" />
                <span>{item.title}</span>
              </NavLink>
            );
          })}
        </nav>

        {/* 右侧:实时遥测脉冲胶囊 + ⌘K + 布局切换 + 主题 + 账户 */}
        <div className="flex shrink-0 items-center gap-1 sm:gap-1.5">
          <LiveTelemetryTicker />
          <CommandPaletteTrigger />
          <LayoutModeSwitch mode={layoutMode} onChange={onLayoutModeChange} />
          <ThemeToggle />
          <UserMenu
            layoutMode={layoutMode}
            onToggleLayoutMode={() =>
              onLayoutModeChange(layoutMode === 'dock' ? 'sidebar' : 'dock')
            }
          />
        </div>
      </div>
    </header>
  );
}

interface SidebarNavItem {
  key: string;
  title: string;
  to: string;
  icon: LucideIcon;
  active: boolean;
  badge?: string | number;
  badgeHighlight?: boolean;
}

/** 模式 B: 精密遥测侧边栏壳层 (Sidebar Telemetry Rail Mode, 支持展开 w-64 与图标窄轨 w-[68px])。 */
function SidebarShell({
  layoutMode,
  onLayoutModeChange,
  collapsed,
  onToggleCollapsed,
}: {
  layoutMode: LayoutMode;
  onLayoutModeChange: (next: LayoutMode) => void;
  collapsed: boolean;
  onToggleCollapsed: () => void;
}) {
  const { pathname } = useLocation();
  const [searchParams] = useSearchParams();
  const navigate = useNavigate();

  const statsQ = useQuery({
    queryKey: qk.stats,
    queryFn: statsOverview,
    refetchInterval: 5000,
  });
  const servicesQ = useQuery({ queryKey: qk.services(), queryFn: () => listServices() });
  const visitorsQ = useQuery({ queryKey: qk.visitors(), queryFn: () => listVisitors() });

  const tunnelCounts = useMemo(() => {
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
  const clientParam = searchParams.get('client');

  const withClientParam = (baseTo: string) => {
    if (!clientParam) return baseTo;
    const sep = baseTo.includes('?') ? '&' : '?';
    return `${baseTo}${sep}client=${encodeURIComponent(clientParam)}`;
  };

  const stats = statsQ.data;

  const sections: { label: string; items: SidebarNavItem[] }[] = [
    {
      label: '态势与边缘节点',
      items: [
        {
          key: 'overview',
          title: '态势总览',
          to: ROUTES.overview,
          icon: LayoutDashboard,
          active: pathname === ROUTES.overview,
        },
        {
          key: 'clients',
          title: '边缘节点',
          to: ROUTES.clients,
          icon: Server,
          active: pathname === ROUTES.clients,
          badge: stats ? `${stats.clients_online}/${stats.clients_total}` : undefined,
          badgeHighlight: (stats?.clients_online ?? 0) > 0,
        },
      ],
    },
    {
      label: '隧道与路由中枢',
      items: [
        {
          key: 'tunnel-all',
          title: '全部隧道',
          to: withClientParam(`${ROUTES.tcp}?lens=all`),
          icon: Layers,
          active: isAllLens,
          badge: tunnelCounts.all,
        },
        {
          key: 'tunnel-tcp',
          title: 'TCP 隧道',
          to: withClientParam(ROUTES.tcp),
          icon: ArrowRightLeft,
          active: pathname === ROUTES.tcp && !isAllLens,
          badge: tunnelCounts.tcp,
        },
        {
          key: 'tunnel-udp',
          title: 'UDP 隧道',
          to: withClientParam(ROUTES.udp),
          icon: Radio,
          active: pathname === ROUTES.udp,
          badge: tunnelCounts.udp,
        },
        {
          key: 'tunnel-domains',
          title: '域名服务 (HTTP/S)',
          to: withClientParam(ROUTES.domains),
          icon: Globe,
          active: pathname === ROUTES.domains,
          badge: tunnelCounts.domains,
        },
        {
          key: 'tunnel-secret',
          title: '私网密钥规则',
          to: withClientParam(ROUTES.secret),
          icon: KeyRound,
          active: pathname === ROUTES.secret && !isVisitorsTab,
          badge: tunnelCounts.secret,
        },
        {
          key: 'tunnel-visitors',
          title: 'P2P 访问器',
          to: withClientParam(`${ROUTES.secret}?tab=visitors`),
          icon: Cable,
          active: isVisitorsTab,
          badge: tunnelCounts.visitors,
        },
      ],
    },
    {
      label: '安全与系统控制',
      items: [
        {
          key: 'certs',
          title: 'TLS 证书保险箱',
          to: ROUTES.certs,
          icon: ShieldCheck,
          active: pathname === ROUTES.certs,
        },
        {
          key: 'settings',
          title: '系统控制矩阵',
          to: ROUTES.settings,
          icon: Settings,
          active: pathname === ROUTES.settings,
        },
      ],
    },
  ];

  // 计算当前激活项的面包屑标题
  const activeCrumb = useMemo(() => {
    for (const sec of sections) {
      for (const item of sec.items) {
        if (item.active) {
          return { section: sec.label, title: item.title };
        }
      }
    }
    return { section: '控制台', title: '态势总览' };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [pathname, isVisitorsTab, isAllLens]);

  return (
    <div className="flex min-h-svh w-full">
      {/* 左侧桌面端专属遥测边栏 (≥768px) */}
      <aside
        aria-label="侧边栏主导航"
        className={cn(
          'rh-glass-dock sticky top-0 z-30 hidden h-svh shrink-0 flex-col border-r border-border/80 transition-[width] duration-200 md:flex',
          collapsed ? 'w-[68px]' : 'w-64',
        )}
      >
        {/* 边栏顶栏:品牌徽标 + 折叠切换 */}
        <div
          className={cn(
            'flex h-14 items-center border-b border-border/70 px-3.5',
            collapsed ? 'justify-center' : 'justify-between',
          )}
        >
          <NavLink
            to={ROUTES.overview}
            className="group flex shrink-0 items-center gap-2.5 rounded-xl py-1 transition-opacity hover:opacity-90"
          >
            <span className="flex size-8 shrink-0 items-center justify-center rounded-xl bg-gradient-to-br from-primary/20 via-primary/10 to-transparent ring-1 ring-primary/30 shadow-2xs transition-transform group-hover:scale-105">
              <BrandMark className="size-5" />
            </span>
            {!collapsed ? (
              <div className="flex flex-col leading-none whitespace-nowrap">
                <span className="text-sm font-bold tracking-tight text-foreground">rabbithole</span>
                <span className="rh-mono mt-1 text-[10px] font-medium text-primary">
                  v{__APP_VERSION__}
                </span>
              </div>
            ) : null}
          </NavLink>

          {!collapsed ? (
            <Tooltip>
              <TooltipTrigger asChild>
                <Button
                  type="button"
                  variant="ghost"
                  size="icon-xs"
                  onClick={onToggleCollapsed}
                  aria-label="收起侧边栏为图标窄轨"
                  className="text-muted-foreground hover:text-foreground"
                >
                  <PanelLeftClose className="size-4" />
                </Button>
              </TooltipTrigger>
              <TooltipContent side="right">收起为图标窄轨</TooltipContent>
            </Tooltip>
          ) : null}
        </div>

        {/* 边栏中部:三段式分组导航 */}
        <div className="flex-1 space-y-5 overflow-y-auto px-2.5 py-4">
          {sections.map((section) => (
            <div key={section.label} className="space-y-1">
              {!collapsed ? (
                <div className="px-2.5 pb-1 text-[10px] font-semibold tracking-wider text-muted-foreground/80 uppercase">
                  {section.label}
                </div>
              ) : (
                <div className="mx-auto my-1 h-px w-6 bg-border/70" />
              )}
              <div className="space-y-0.5">
                {section.items.map((item) => {
                  const Icon = item.icon;
                  const buttonNode = (
                    <button
                      key={item.key}
                      type="button"
                      onClick={() => navigate(item.to)}
                      className={cn(
                        'group flex w-full items-center rounded-xl text-xs font-medium transition-all',
                        collapsed ? 'justify-center p-2.5' : 'justify-between gap-2 px-2.5 py-2',
                        item.active
                          ? 'bg-primary text-primary-foreground shadow-xs'
                          : 'text-muted-foreground hover:bg-muted/70 hover:text-foreground',
                      )}
                    >
                      <span className="flex min-w-0 items-center gap-2.5">
                        <Icon className="size-4 shrink-0" />
                        {!collapsed ? <span className="truncate">{item.title}</span> : null}
                      </span>
                      {!collapsed && item.badge !== undefined ? (
                        <span
                          className={cn(
                            'rh-mono shrink-0 rounded-full px-1.5 py-0.2 text-[10px]',
                            item.active
                              ? 'bg-primary-foreground/20 text-primary-foreground'
                              : item.badgeHighlight
                                ? 'bg-emerald-500/15 text-emerald-700 dark:text-emerald-300'
                                : 'bg-muted text-muted-foreground',
                          )}
                        >
                          {item.badge}
                        </span>
                      ) : null}
                    </button>
                  );

                  if (collapsed) {
                    return (
                      <Tooltip key={item.key}>
                        <TooltipTrigger asChild>{buttonNode}</TooltipTrigger>
                        <TooltipContent side="right" className="flex items-center gap-2">
                          <span>{item.title}</span>
                          {item.badge !== undefined ? (
                            <Mono className="text-[11px] opacity-80">({item.badge})</Mono>
                          ) : null}
                        </TooltipContent>
                      </Tooltip>
                    );
                  }
                  return buttonNode;
                })}
              </div>
            </div>
          ))}
        </div>

        {/* 边栏底部:实时遥测状态舱 */}
        <div className="border-t border-border/70 p-2.5">
          {collapsed ? (
            <Tooltip>
              <TooltipTrigger asChild>
                <button
                  type="button"
                  onClick={onToggleCollapsed}
                  aria-label="展开侧边栏"
                  className="flex w-full flex-col items-center justify-center gap-1.5 rounded-xl border border-border/70 bg-muted/30 py-2.5 transition-colors hover:bg-accent/50"
                >
                  <span
                    className={cn(
                      'size-2 rounded-full',
                      (stats?.clients_online ?? 0) > 0
                        ? 'bg-emerald-500 rh-pulse-dot'
                        : 'bg-zinc-400',
                    )}
                  />
                  <PanelLeftOpen className="size-4 text-muted-foreground" />
                </button>
              </TooltipTrigger>
              <TooltipContent side="right">
                展开侧边栏 · 节点 {stats?.clients_online ?? 0}/{stats?.clients_total ?? 0} · 隧道{' '}
                {stats?.services_running ?? 0}/{stats?.services_total ?? 0}
              </TooltipContent>
            </Tooltip>
          ) : (
            <div className="space-y-2 rounded-xl border border-border/80 bg-muted/30 p-3 text-xs">
              <div className="flex items-center justify-between">
                <span className="inline-flex items-center gap-1.5 font-medium text-foreground">
                  <span
                    className={cn(
                      'size-2 rounded-full',
                      (stats?.clients_online ?? 0) > 0
                        ? 'bg-emerald-500 rh-pulse-dot'
                        : 'bg-zinc-400',
                    )}
                  />
                  RHTP 控制面
                </span>
                <Mono className="text-[11px] text-primary">
                  {stats?.conns_current ?? 0} 活跃连接
                </Mono>
              </div>
              <div className="grid grid-cols-2 gap-1.5 pt-0.5 text-[11px] text-muted-foreground">
                <div className="rounded-lg bg-background/70 px-2 py-1">
                  节点{' '}
                  <Mono className="font-semibold text-foreground">
                    {stats ? `${stats.clients_online}/${stats.clients_total}` : '-'}
                  </Mono>
                </div>
                <div className="rounded-lg bg-background/70 px-2 py-1">
                  隧道{' '}
                  <Mono className="font-semibold text-foreground">
                    {stats ? `${stats.services_running}/${stats.services_total}` : '-'}
                  </Mono>
                </div>
              </div>
            </div>
          )}
        </div>
      </aside>

      {/* 右侧主画布区 */}
      <div className="flex min-w-0 flex-1 flex-col">
        {/* 侧栏模式下的上下文顶栏 (桌面端展示面包屑+折叠+工具栏;移动端展示品牌+工具栏) */}
        <header className="rh-glass-dock sticky top-0 z-30 flex h-12 items-center justify-between gap-2 border-b border-border/80 px-3 sm:h-14 sm:px-6">
          {/* 左侧:移动端显示品牌,桌面端显示折叠按钮 + 动态面包屑 */}
          <div className="flex min-w-0 items-center gap-2">
            {/* 移动端品牌标识 (<768px) */}
            <NavLink
              to={ROUTES.overview}
              className="flex shrink-0 items-center gap-2 rounded-xl py-1 md:hidden"
            >
              <span className="flex size-7 shrink-0 items-center justify-center rounded-lg bg-primary/10 ring-1 ring-primary/20">
                <BrandMark className="size-5" />
              </span>
              <div className="flex flex-col leading-none whitespace-nowrap">
                <span className="text-xs font-bold tracking-tight">rabbithole</span>
                <span className="rh-mono mt-0.5 text-[9px] font-medium text-primary">
                  v{__APP_VERSION__}
                </span>
              </div>
            </NavLink>

            {/* 桌面端侧边栏展开/收起按钮 + 面包屑 (≥768px) */}
            <div className="hidden items-center gap-2 md:flex">
              <Button
                type="button"
                variant="ghost"
                size="icon-xs"
                onClick={onToggleCollapsed}
                title={collapsed ? '展开侧边栏' : '收起侧边栏'}
                aria-label={collapsed ? '展开侧边栏' : '收起侧边栏'}
                className="text-muted-foreground hover:text-foreground"
              >
                {collapsed ? (
                  <PanelLeftOpen className="size-4" />
                ) : (
                  <PanelLeftClose className="size-4" />
                )}
              </Button>
              <span className="h-4 w-px bg-border" />
              <div className="flex items-center gap-1.5 text-xs">
                <span className="text-muted-foreground">{activeCrumb.section}</span>
                <ChevronRight className="size-3.5 text-muted-foreground/60" />
                <span className="font-semibold text-foreground">{activeCrumb.title}</span>
              </div>
            </div>
          </div>

          {/* 右侧:遥测胶囊 + ⌘K + 布局切换器 + 主题 + 用户菜单 */}
          <div className="flex shrink-0 items-center gap-1 sm:gap-1.5">
            <LiveTelemetryTicker />
            <CommandPaletteTrigger />
            <LayoutModeSwitch mode={layoutMode} onChange={onLayoutModeChange} />
            <ThemeToggle />
            <UserMenu
              layoutMode={layoutMode}
              onToggleLayoutMode={() =>
                onLayoutModeChange(layoutMode === 'dock' ? 'sidebar' : 'dock')
              }
            />
          </div>
        </header>

        <main className="mx-auto w-full max-w-[1640px] flex-1 px-3 pt-4 pb-24 sm:px-6 sm:pt-5 md:pb-12">
          <Outlet />
        </main>
      </div>
    </div>
  );
}

/**
 * 控制台双模自适应主壳层:
 * - 模式 A (`dock`): 顶栏悬浮毛玻璃指令舱,释放 100% 横向视野;
 * - 模式 B (`sidebar`): 经典左侧遥测导航轨(支持展开全部分类与折叠为极简图标轨) + 顶部面包屑上下文栏;
 * - 移动端 (`<768px`): 自动收敛为顶部紧凑栏 + 底部 5 宫格毛玻璃导航坞。
 */
export function ConsoleLayout() {
  const [layoutMode, setLayoutMode] = useState<LayoutMode>(readLayoutMode);
  const [sidebarCollapsed, setSidebarCollapsed] = useState<boolean>(readSidebarCollapsed);

  useEffect(() => {
    try {
      localStorage.setItem(LAYOUT_MODE_KEY, layoutMode);
    } catch {
      // ignore storage errors
    }
  }, [layoutMode]);

  useEffect(() => {
    try {
      localStorage.setItem(SIDEBAR_COLLAPSED_KEY, sidebarCollapsed ? '1' : '0');
    } catch {
      // ignore storage errors
    }
  }, [sidebarCollapsed]);

  return (
    <QueryClientProvider client={queryClient}>
      <TooltipProvider delayDuration={180}>
        <div className="rh-bg-canvas flex min-h-svh flex-col">
          {layoutMode === 'dock' ? (
            <>
              <TopCommandDock layoutMode={layoutMode} onLayoutModeChange={setLayoutMode} />
              <main className="mx-auto w-full max-w-[1600px] flex-1 px-3 pt-4 pb-24 sm:px-6 sm:pt-6 md:pb-12">
                <Outlet />
              </main>
            </>
          ) : (
            <SidebarShell
              layoutMode={layoutMode}
              onLayoutModeChange={setLayoutMode}
              collapsed={sidebarCollapsed}
              onToggleCollapsed={() => setSidebarCollapsed((prev) => !prev)}
            />
          )}
          <MobileBottomDock />
        </div>
        <CommandPalette />
      </TooltipProvider>
    </QueryClientProvider>
  );
}
