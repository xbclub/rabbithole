import { ArrowRight, HelpCircle, Loader2, Lock, ShieldCheck, Terminal, User } from 'lucide-react';
import { useEffect, useRef, useState } from 'react';
import { useNavigate, useSearchParams } from 'react-router';

import { codesApi, loginApi, userInfoApi } from '@/api/auth';
import { safeRedirect } from '@/api/request';
import { BrandMark } from '@/components/brand-mark';
import { ThemeToggle } from '@/components/theme-toggle';
import { Button } from '@/components/ui/button';
import { Input, PasswordInput } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { feedbackSuccess } from '@/lib/feedback';
import { HOME_PATH, useAuthStore } from '@/stores/auth';

/**
 * 登录页:rabbithole 网络隧道控制面门户。
 * 保持首屏轻量(不静态引入 react-hook-form + zod),左侧为定制的三层网络拓扑蓝图,
 * 右侧为悬浮毛玻璃认证舱。
 */
export default function LoginPage() {
  const navigate = useNavigate();
  const [searchParams] = useSearchParams();
  const token = useAuthStore((state) => state.token);

  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [errors, setErrors] = useState<{ password?: string; username?: string }>({});
  const [submitting, setSubmitting] = useState(false);
  const [showBootHelp, setShowBootHelp] = useState(false);
  const passwordRef = useRef<HTMLInputElement | null>(null);

  useEffect(() => {
    if (token) {
      navigate(HOME_PATH, { replace: true });
    }
  }, [token, navigate]);

  const onSubmit = async (event: React.FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    const nextErrors: { password?: string; username?: string } = {};
    if (!username.trim()) nextErrors.username = '请输入用户名';
    if (!password) nextErrors.password = '请输入密码';
    setErrors(nextErrors);
    if (Object.keys(nextErrors).length > 0) return;

    setSubmitting(true);
    try {
      const { accessToken } = await loginApi({ username, password });
      useAuthStore.getState().setAuth(accessToken, username);
      const [info] = await Promise.all([
        userInfoApi().catch(() => null),
        codesApi().catch(() => []),
      ]);
      if (info?.username) {
        useAuthStore.getState().setAuth(accessToken, info.username);
      }
      feedbackSuccess('登录成功');
      navigate(safeRedirect(searchParams.get('redirect')) || HOME_PATH, { replace: true });
    } catch {
      passwordRef.current?.focus();
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <div className="rh-bg-canvas relative flex min-h-svh flex-col justify-between">
      {/* 顶栏:品牌标识 + 主题切换 */}
      <header className="mx-auto flex w-full max-w-[1380px] items-center justify-between px-4 pt-4 sm:px-6 sm:pt-6">
        <div className="flex items-center gap-2.5 sm:gap-3">
          <span className="flex size-8 items-center justify-center rounded-xl bg-gradient-to-br from-primary/20 via-primary/10 to-transparent ring-1 ring-primary/30 shadow-2xs sm:size-9">
            <BrandMark className="size-5 sm:size-6" />
          </span>
          <div className="flex flex-col leading-none">
            <span className="text-sm font-bold tracking-tight sm:text-base">rabbithole</span>
            <span className="rh-mono mt-1 text-[10px] font-medium text-primary">
              v{__APP_VERSION__}
            </span>
          </div>
        </div>
        <div className="flex items-center gap-2">
          <span className="rh-mono hidden text-xs text-muted-foreground sm:inline">
            RHTP CONTROL PLANE
          </span>
          <ThemeToggle />
        </div>
      </header>

      {/* 主体双栏:左侧拓扑架构蓝图 + 右侧悬浮认证舱 */}
      <div className="mx-auto grid w-full max-w-[1380px] flex-1 items-center gap-8 px-4 py-6 sm:gap-12 sm:px-6 sm:py-8 lg:grid-cols-[1.25fr_430px]">
        {/* 左侧:网络隧道拓扑可视化面板 */}
        <div className="hidden flex-col gap-8 lg:flex">
          <div className="space-y-4">
            <div className="inline-flex items-center gap-2 rounded-full border border-emerald-500/25 bg-emerald-500/10 px-3 py-1 text-xs font-medium text-emerald-700 dark:text-emerald-300">
              <span className="size-1.5 rounded-full bg-emerald-500 rh-pulse-dot" />
              自托管高性能端口转发与内网服务穿透平台
            </div>
            <h1 className="text-4xl leading-[1.15] font-bold tracking-tight xl:text-5xl">
              把公网的单一入口，
              <br />
              <span className="text-primary">化作直达内网集群的精密隧道。</span>
            </h1>
            <p className="max-w-xl text-base leading-relaxed text-muted-foreground">
              服务端集中编排规则、实时热更新下发、零客户端本地配置，统一承载端口转发、SNI
              域名路由与私网点对点直连。
            </p>
          </div>

          {/* 三层实时拓扑流向蓝图卡 */}
          <div className="rh-telemetry-card rounded-2xl border border-border/80 bg-card/85 p-6 shadow-sm backdrop-blur-xs">
            <div className="mb-4 flex items-center justify-between border-b border-border/60 pb-3">
              <div className="flex items-center gap-2 text-xs font-medium text-muted-foreground">
                <Terminal className="size-3.5 text-primary" />
                <span>TUNNEL TOPOLOGY ARCHITECTURE</span>
              </div>
              <div className="rh-mono flex items-center gap-2 text-[11px] text-muted-foreground">
                <span>TLS / QUIC / KCP</span>
                <span>·</span>
                <span>多路复用传输</span>
              </div>
            </div>

            <div className="space-y-3">
              {/* 链路 1:TCP/UDP 端口映射 */}
              <div className="grid grid-cols-[1fr_auto_1fr_auto_1fr] items-center gap-2 rounded-xl border border-border/60 bg-background/70 p-3 text-xs">
                <div className="min-w-0">
                  <div className="text-[11px] text-muted-foreground">公网入口 (Ingress)</div>
                  <div className="rh-mono mt-0.5 truncate font-semibold">0.0.0.0:20000-21000</div>
                </div>
                <div className="flex flex-col items-center px-1 text-primary">
                  <span className="rh-mono rounded bg-primary/10 px-1.5 py-0.5 text-[10px] font-semibold">
                    TCP / UDP
                  </span>
                  <ArrowRight className="mt-0.5 size-3.5" />
                </div>
                <div className="min-w-0 rounded-lg border border-primary/20 bg-primary/5 px-2.5 py-1.5">
                  <div className="flex items-center gap-1.5 text-[11px] font-medium text-primary">
                    <span className="size-1.5 rounded-full bg-emerald-500 rh-pulse-dot" />
                    边缘节点连接池
                  </div>
                  <div className="rh-mono truncate text-[11px] text-muted-foreground">
                    预热池 · 带宽限速
                  </div>
                </div>
                <ArrowRight className="size-3.5 text-muted-foreground" />
                <div className="min-w-0 text-right">
                  <div className="text-[11px] text-muted-foreground">内网服务 (Target)</div>
                  <div className="rh-mono mt-0.5 truncate font-semibold text-emerald-600 dark:text-emerald-400">
                    10.0.0.12:22 / :3306
                  </div>
                </div>
              </div>

              {/* 链路 2:HTTP/HTTPS SNI 域名路由 */}
              <div className="grid grid-cols-[1fr_auto_1fr_auto_1fr] items-center gap-2 rounded-xl border border-border/60 bg-background/70 p-3 text-xs">
                <div className="min-w-0">
                  <div className="text-[11px] text-muted-foreground">域名路由 (Vhost SNI)</div>
                  <div className="rh-mono mt-0.5 truncate font-semibold">*.app.example.com</div>
                </div>
                <div className="flex flex-col items-center px-1 text-primary">
                  <span className="rh-mono rounded bg-primary/10 px-1.5 py-0.5 text-[10px] font-semibold">
                    HTTP / HTTPS
                  </span>
                  <ArrowRight className="mt-0.5 size-3.5" />
                </div>
                <div className="min-w-0 rounded-lg border border-primary/20 bg-primary/5 px-2.5 py-1.5">
                  <div className="flex items-center gap-1.5 text-[11px] font-medium text-primary">
                    <span className="size-1.5 rounded-full bg-emerald-500 rh-pulse-dot" />
                    多证书终结 & 缓存
                  </div>
                  <div className="rh-mono truncate text-[11px] text-muted-foreground">
                    路径分流 · Header 改写
                  </div>
                </div>
                <ArrowRight className="size-3.5 text-muted-foreground" />
                <div className="min-w-0 text-right">
                  <div className="text-[11px] text-muted-foreground">内网 Web / 插件</div>
                  <div className="rh-mono mt-0.5 truncate font-semibold text-emerald-600 dark:text-emerald-400">
                    10.0.1.8:8080
                  </div>
                </div>
              </div>

              {/* 链路 3:STCP/XTCP 零暴露私网打洞 */}
              <div className="grid grid-cols-[1fr_auto_1fr_auto_1fr] items-center gap-2 rounded-xl border border-border/60 bg-background/70 p-3 text-xs">
                <div className="min-w-0">
                  <div className="text-[11px] text-muted-foreground">访问器端 (Visitor)</div>
                  <div className="rh-mono mt-0.5 truncate font-semibold">127.0.0.1:6000</div>
                </div>
                <div className="flex flex-col items-center px-1 text-primary">
                  <span className="rh-mono rounded bg-primary/10 px-1.5 py-0.5 text-[10px] font-semibold">
                    STCP / XTCP
                  </span>
                  <ArrowRight className="mt-0.5 size-3.5" />
                </div>
                <div className="min-w-0 rounded-lg border border-primary/20 bg-primary/5 px-2.5 py-1.5">
                  <div className="flex items-center gap-1.5 text-[11px] font-medium text-primary">
                    <span className="size-1.5 rounded-full bg-emerald-500 rh-pulse-dot" />
                    密钥鉴权 & NAT 打洞
                  </div>
                  <div className="rh-mono truncate text-[11px] text-muted-foreground">
                    点对点直连 · 中继回退
                  </div>
                </div>
                <ArrowRight className="size-3.5 text-muted-foreground" />
                <div className="min-w-0 text-right">
                  <div className="text-[11px] text-muted-foreground">私有目标节点</div>
                  <div className="rh-mono mt-0.5 truncate font-semibold text-emerald-600 dark:text-emerald-400">
                    10.0.2.50:5432
                  </div>
                </div>
              </div>
            </div>
          </div>
        </div>

        {/* 右侧:管理端身份验证舱 */}
        <div className="mx-auto w-full max-w-md">
          <div className="rh-glass-dock rounded-2xl border border-border/85 p-6 shadow-xl sm:p-8">
            <div className="mb-6 space-y-2">
              <span className="inline-flex items-center gap-1.5 rounded-full border border-primary/25 bg-primary/10 px-2.5 py-1 text-xs font-medium text-primary">
                <ShieldCheck className="size-3.5" />
                管理员身份验证
              </span>
              <h2 className="pt-1 text-2xl font-bold tracking-tight">登录控制台</h2>
              <p className="text-sm text-muted-foreground">
                输入管理员账号与密码以编排边缘节点、转发规则与系统配置
              </p>
            </div>

            <form onSubmit={onSubmit} className="space-y-4" noValidate>
              <div className="space-y-2">
                <Label htmlFor="login-username">管理员账号</Label>
                <div className="relative">
                  <User
                    className="pointer-events-none absolute top-1/2 left-3 size-4 -translate-y-1/2 text-muted-foreground"
                    aria-hidden="true"
                  />
                  <Input
                    id="login-username"
                    name="username"
                    value={username}
                    onChange={(event) => setUsername(event.target.value)}
                    autoComplete="username"
                    placeholder="用户名(默认 admin)"
                    className="h-10 pl-9"
                    aria-invalid={errors.username ? true : undefined}
                    aria-describedby={errors.username ? 'login-username-error' : undefined}
                  />
                </div>
                {errors.username ? (
                  <p id="login-username-error" className="text-xs font-medium text-destructive">
                    {errors.username}
                  </p>
                ) : null}
              </div>

              <div className="space-y-2">
                <Label htmlFor="login-password">管理密码</Label>
                <PasswordInput
                  id="login-password"
                  name="password"
                  ref={passwordRef}
                  value={password}
                  onChange={(event) => setPassword(event.target.value)}
                  autoComplete="current-password"
                  placeholder="输入管理员密码"
                  className="h-10"
                  leftIcon={<Lock className="size-4" />}
                  aria-invalid={errors.password ? true : undefined}
                  aria-describedby={errors.password ? 'login-password-error' : undefined}
                />
                {errors.password ? (
                  <p id="login-password-error" className="text-xs font-medium text-destructive">
                    {errors.password}
                  </p>
                ) : null}
              </div>

              <Button
                type="submit"
                className="mt-1 h-10 w-full gap-2 font-medium shadow-sm"
                disabled={submitting}
              >
                {submitting ? <Loader2 className="size-4 animate-spin" aria-hidden="true" /> : null}
                进入控制台
                {!submitting ? <ArrowRight className="size-4" aria-hidden="true" /> : null}
              </Button>
            </form>

            {/* 首启密码引导与重置说明折叠区 */}
            <div className="mt-6 border-t border-border/60 pt-4">
              <button
                type="button"
                onClick={() => setShowBootHelp((prev) => !prev)}
                className="flex w-full items-center justify-between text-left text-xs text-muted-foreground transition-colors hover:text-foreground"
              >
                <span className="inline-flex items-center gap-1.5">
                  <HelpCircle className="size-3.5 text-primary" />
                  首次部署如何查看初始密码 / 忘记密码？
                </span>
                <span className="rh-mono text-[11px]">{showBootHelp ? '收起' : '展开'}</span>
              </button>

              {showBootHelp ? (
                <div className="mt-3 space-y-2 rounded-xl border border-border/70 bg-muted/40 p-3 text-xs leading-relaxed text-muted-foreground">
                  <p>
                    1. 首次启动时自动创建 <span className="rh-mono text-foreground">admin</span>{' '}
                    账号，随机初始密码仅在服务端启动日志横幅中打印一次：
                  </p>
                  <pre className="rh-mono overflow-x-auto rounded-lg border bg-background/80 p-2 text-[11px] text-foreground">
                    journalctl -u rabbithole-server | grep -A3 &apos;password:&apos;
                  </pre>
                  <p>
                    2. 如遗忘密码，可停止服务端并删除数据目录中的 SQLite
                    数据库文件后重启重新引导（注意：原有数据将清空）。
                  </p>
                </div>
              ) : null}
            </div>
          </div>
        </div>
      </div>

      {/* 底栏铭牌 */}
      <footer className="mx-auto flex w-full max-w-[1380px] items-center justify-between px-4 pb-5 text-xs text-muted-foreground sm:px-6 sm:pb-6">
        <span>rabbithole · Apache-2.0 License</span>
        <span className="rh-mono">v{__APP_VERSION__}</span>
      </footer>
    </div>
  );
}
