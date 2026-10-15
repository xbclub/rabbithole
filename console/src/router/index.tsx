import { Suspense, lazy } from 'react';
import type { ComponentType } from 'react';
import { Navigate, createBrowserRouter } from 'react-router';

import { Spinner } from '@/components/ui/spinner';
import { ForbiddenPage, NotFoundPage, RouteErrorPage } from '@/pages/error-page';
import LoginPage from '@/pages/login';
import { ROUTES } from '@/router/paths';
import { RequireAuth } from '@/router/require-auth';

/**
 * 懒加载期间的占位:给出可见反馈,而不是空白页。
 * `full` 用于整壳加载(侧栏也还没到),否则只在内容区里显示。
 */
function RouteFallback({ full = false }: { full?: boolean }) {
  return (
    <div
      className={
        full
          ? 'flex min-h-svh items-center justify-center'
          : 'flex min-h-40 items-center justify-center'
      }
    >
      <Spinner className="size-5 text-muted-foreground" aria-label="正在载入" />
    </div>
  );
}

/**
 * 路由级懒加载:首屏只下载当前路由需要的代码。
 *
 * 之前所有页面都是静态 import,登录页会连带下载 recharts(约 357 KB)与
 * react-hook-form + zod(约 122 KB)—— 这是首屏白屏的主因。
 */
function lazyPage(loader: () => Promise<{ default: ComponentType }>) {
  const Component = lazy(loader);
  return (
    <Suspense fallback={<RouteFallback />}>
      <Component />
    </Suspense>
  );
}

/** 控制台外壳(侧栏 / 顶栏 / ⌘K 面板)本身也懒加载,登录页不需要它们。 */
const LazyConsoleLayout = lazy(() =>
  import('@/components/layout/console-layout').then((module) => ({
    default: module.ConsoleLayout,
  })),
);

export const router = createBrowserRouter([
  {
    path: '/',
    element: <Navigate to={ROUTES.overview} replace />,
    errorElement: <RouteErrorPage />,
  },
  // 登录页保持静态引入:它是未登录用户的首屏,不该再走一次懒加载往返
  { path: ROUTES.login, element: <LoginPage /> },
  { path: '/user', element: <Navigate to={ROUTES.login} replace /> },
  { path: '/403', element: <ForbiddenPage /> },
  {
    path: '/rabbithole',
    element: (
      <RequireAuth>
        <Suspense fallback={<RouteFallback full />}>
          <LazyConsoleLayout />
        </Suspense>
      </RequireAuth>
    ),
    errorElement: <RouteErrorPage />,
    children: [
      { index: true, element: <Navigate to={ROUTES.overview} replace /> },
      { path: 'overview', element: lazyPage(() => import('@/pages/overview')) },
      { path: 'clients', element: lazyPage(() => import('@/pages/clients')) },
      { path: 'tcp', element: lazyPage(() => import('@/pages/tcp')) },
      { path: 'udp', element: lazyPage(() => import('@/pages/udp')) },
      { path: 'domains', element: lazyPage(() => import('@/pages/domains')) },
      { path: 'secret', element: lazyPage(() => import('@/pages/secret')) },
      { path: 'certs', element: lazyPage(() => import('@/pages/certs')) },
      { path: 'settings', element: lazyPage(() => import('@/pages/settings')) },
      // 旧 vben 路径兼容
      { path: 'services', element: <Navigate to={ROUTES.tcp} replace /> },
      { path: 'visitors', element: <Navigate to={`${ROUTES.secret}?tab=visitors`} replace /> },
    ],
  },
  { path: '*', element: <NotFoundPage /> },
]);
