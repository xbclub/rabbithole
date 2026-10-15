import { Link, useRouteError } from 'react-router';

import { Button } from '@/components/ui/button';
import { ROUTES } from '@/router/paths';

/** 兜底错误页(403 / 404 / 路由渲染异常共用)。 */
export function ErrorPage({
  code,
  title,
  description,
}: {
  code: string;
  title: string;
  description: React.ReactNode;
}) {
  return (
    <div className="flex min-h-svh flex-col items-center justify-center gap-4 p-6 text-center">
      <span className="rh-mono text-5xl font-semibold text-muted-foreground">{code}</span>
      <h1 className="text-xl font-semibold tracking-tight">{title}</h1>
      <p className="max-w-md text-sm leading-relaxed text-muted-foreground">{description}</p>
      <Button asChild>
        <Link to={ROUTES.overview}>回到总览</Link>
      </Button>
    </div>
  );
}

export function NotFoundPage() {
  return (
    <ErrorPage
      code="404"
      title="页面不存在"
      description="地址可能已变更或输入有误。控制台的八个页面都能从左侧导航进入。"
    />
  );
}

export function ForbiddenPage() {
  return (
    <ErrorPage
      code="403"
      title="没有访问权限"
      description="当前账号无权访问该页面。控制台为单管理员模式,如需权限请联系部署者。"
    />
  );
}

/** 路由级渲染异常兜底:把错误显示成页面,而不是白屏。 */
export function RouteErrorPage() {
  const error = useRouteError();
  const message =
    error instanceof Error ? error.message : typeof error === 'string' ? error : '未知错误';
  return (
    <ErrorPage
      code="500"
      title="界面出错了"
      description={
        <span>
          渲染该页面时发生异常。刷新可重试;若持续出现,请把下面的信息反馈给维护者。
          <span className="rh-mono mt-2 block text-xs break-all">{message}</span>
        </span>
      }
    />
  );
}
