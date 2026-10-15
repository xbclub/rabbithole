import { LogOut } from 'lucide-react';
import { useState } from 'react';
import { Outlet, useNavigate } from 'react-router';

import { QueryClientProvider } from '@tanstack/react-query';

import { CommandPalette, CommandPaletteTrigger } from '@/components/command-palette';
import { AppSidebar } from '@/components/layout/app-sidebar';
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
import { SidebarInset, SidebarProvider, SidebarTrigger } from '@/components/ui/sidebar';
import { TooltipProvider } from '@/components/ui/tooltip';
import { logoutApi } from '@/api/auth';
import { feedbackSuccess } from '@/lib/feedback';
import { queryClient } from '@/lib/query-client';
import { ROUTES } from '@/router/paths';
import { useAuthStore } from '@/stores/auth';

function UserMenu() {
  const username = useAuthStore((state) => state.username);
  const navigate = useNavigate();
  const [confirmOpen, setConfirmOpen] = useState(false);
  const [pending, setPending] = useState(false);

  const doLogout = async () => {
    setPending(true);
    try {
      // 必须经带拦截器的实例:后端校验 Bearer 通过才落 logout 审计事件
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
          <Button variant="ghost" size="icon" aria-label={`账户:${name}`}>
            <Avatar className="size-7">
              <AvatarFallback className="text-xs">{name.slice(0, 1).toUpperCase()}</AvatarFallback>
            </Avatar>
          </Button>
        </DropdownMenuTrigger>
        <DropdownMenuContent align="end" className="w-44">
          <DropdownMenuLabel className="truncate">{name}</DropdownMenuLabel>
          <DropdownMenuSeparator />
          <DropdownMenuItem onSelect={() => setConfirmOpen(true)}>
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
              退出后当前会话立即失效,需要重新输入账号密码。
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

/** 应用外壳:shadcn sidebar(collapsible=icon)+ 顶栏;移动端自动抽屉。 */
export function ConsoleLayout() {
  return (
    // QueryClientProvider 挂在控制台外壳上:登录页不用 react-query,
    // 放根部会让首屏白白加载 @tanstack/react-query(约 93 KB 未压缩)
    <QueryClientProvider client={queryClient}>
      {/* TooltipProvider 同样只服务控制台:登录页没有 Tooltip,放根部会让首屏多带一份 */}
      <TooltipProvider delayDuration={200}>
        <SidebarProvider>
          <AppSidebar />
          <SidebarInset>
            <header className="flex h-14 shrink-0 items-center gap-2 border-b px-4">
              {/*
            顶栏不再显示当前页面名(用户要求):面包屑去掉后左侧留给搜索。
            桌面端的折叠开关移到了侧栏页脚(版本号那一行)。
            但小屏侧栏是 Sheet,页脚在侧栏内部、够不到,所以这里保留一个
            仅小屏可见的入口,否则 <768px 无法打开侧栏。
          */}
              <SidebarTrigger className="-ml-1 md:hidden" aria-label="打开侧栏菜单" />
              {/* 搜索框保持在右侧、紧挨主题切换(用户要求) */}
              <div className="ml-auto flex items-center gap-1.5">
                <CommandPaletteTrigger />
                <ThemeToggle />
                <UserMenu />
              </div>
            </header>
            <div className="flex min-w-0 flex-1 flex-col gap-6 p-4 md:p-6">
              <Outlet />
            </div>
          </SidebarInset>
          <CommandPalette />
        </SidebarProvider>
      </TooltipProvider>
    </QueryClientProvider>
  );
}
