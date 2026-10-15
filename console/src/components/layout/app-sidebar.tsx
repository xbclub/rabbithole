import {
  ArrowRightLeft,
  Globe,
  KeyRound,
  LayoutDashboard,
  PanelLeftClose,
  PanelLeftOpen,
  Radio,
  Server,
  Settings,
  ShieldCheck,
} from 'lucide-react';
import type { LucideIcon } from 'lucide-react';
import { NavLink, useLocation } from 'react-router';

import { BrandMark } from '@/components/brand-mark';
import {
  Sidebar,
  SidebarContent,
  SidebarFooter,
  SidebarGroup,
  SidebarGroupContent,
  SidebarGroupLabel,
  SidebarHeader,
  SidebarMenu,
  SidebarMenuButton,
  SidebarMenuItem,
  SidebarRail,
  useSidebar,
} from '@/components/ui/sidebar';
import { Button } from '@/components/ui/button';
import { NAV_GROUPS, ROUTES } from '@/router/paths';

const ICONS: Record<string, LucideIcon> = {
  dashboard: LayoutDashboard,
  clients: Server,
  tcp: ArrowRightLeft,
  udp: Radio,
  domains: Globe,
  secret: KeyRound,
  certs: ShieldCheck,
  settings: Settings,
};

/**
 * 侧栏折叠开关。放在页脚版本号那一行(用户要求),收起/展开用不同图标。
 *
 * 不用生成组件里的 SidebarTrigger:它把 PanelLeftIcon 写死在内部、不接受
 * children,拿不到状态相关图标;这里用 useSidebar() 自己实现,也不改动
 * shadcn 生成文件(便于 CLI 覆盖升级)。
 */
function SidebarCollapseToggle() {
  const { state, toggleSidebar } = useSidebar();
  const collapsed = state === 'collapsed';
  return (
    <Button
      type="button"
      variant="ghost"
      size="icon-sm"
      onClick={toggleSidebar}
      aria-label={collapsed ? '展开侧栏' : '收起侧栏'}
      title={collapsed ? '展开侧栏' : '收起侧栏'}
      className="shrink-0 text-muted-foreground"
    >
      {collapsed ? (
        <PanelLeftOpen className="size-4" aria-hidden="true" />
      ) : (
        <PanelLeftClose className="size-4" aria-hidden="true" />
      )}
    </Button>
  );
}

/** 侧栏:两组 + 设置,底部版本号 + 折叠开关;路径与旧版一致。 */
export function AppSidebar() {
  const { pathname } = useLocation();

  return (
    <Sidebar collapsible="icon">
      <SidebarHeader>
        <SidebarMenu>
          <SidebarMenuItem>
            <SidebarMenuButton size="lg" asChild tooltip="rabbithole 控制台">
              {/*
                收起为图标态时只留品牌标记:文字块隐藏、按钮内容居中。
                size=lg 在收起态是 h-12 + p-0!,不隐藏文字会被 48px 的轨道裁切。
              */}
              <NavLink
                to={ROUTES.overview}
                className="group-data-[collapsible=icon]:justify-center"
              >
                {/*
                  尺寸容器:SidebarMenuButton 基础样式里的 [&>svg]:size-4 只作用于**直接子** svg,
                  直接放 BrandMark 会被压成 16px(展开态也偏小)。套一层 span 后按状态给尺寸:
                  展开态 28px;收起态按钮内框只有 16px(32 − p-2×2),故同步收到 16px。
                */}
                <span className="flex size-7 shrink-0 items-center justify-center group-data-[collapsible=icon]:size-4">
                  <BrandMark className="size-full" />
                </span>
                <div className="grid flex-1 text-left leading-tight group-data-[collapsible=icon]:hidden">
                  <span className="truncate font-semibold">rabbithole</span>
                  <span className="truncate text-xs text-muted-foreground">控制台</span>
                </div>
              </NavLink>
            </SidebarMenuButton>
          </SidebarMenuItem>
        </SidebarMenu>
      </SidebarHeader>

      <SidebarContent>
        {NAV_GROUPS.map((group, index) => (
          <SidebarGroup key={group.label ?? `group-${index}`}>
            {group.label ? <SidebarGroupLabel>{group.label}</SidebarGroupLabel> : null}
            <SidebarGroupContent>
              <SidebarMenu>
                {group.items.map((item) => {
                  const Icon = ICONS[item.icon] ?? LayoutDashboard;
                  const active = pathname === item.to || pathname.startsWith(`${item.to}/`);
                  return (
                    <SidebarMenuItem key={item.to}>
                      <SidebarMenuButton asChild isActive={active} tooltip={item.title}>
                        <NavLink to={item.to}>
                          <Icon className="size-4" />
                          <span>{item.title}</span>
                        </NavLink>
                      </SidebarMenuButton>
                    </SidebarMenuItem>
                  );
                })}
              </SidebarMenu>
            </SidebarGroupContent>
          </SidebarGroup>
        ))}
      </SidebarContent>

      <SidebarFooter>
        <div className="flex items-center justify-between gap-1 group-data-[collapsible=icon]:justify-center">
          <span className="truncate px-1 text-xs text-muted-foreground group-data-[collapsible=icon]:hidden">
            rabbithole v{__APP_VERSION__}
          </span>
          <SidebarCollapseToggle />
        </div>
      </SidebarFooter>
      <SidebarRail />
    </Sidebar>
  );
}
