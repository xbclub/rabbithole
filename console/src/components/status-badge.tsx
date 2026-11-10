import { cn } from '@/lib/utils';

/**
 * 精密遥测状态徽章:脉冲信号点 + 胶囊边框,五态语义(在线/离线/警告/未启用/错误)。
 */
export type RhSignal = 'ok' | 'off' | 'warn' | 'idle' | 'error';

const SIGNAL_DOT_CLASS: Record<RhSignal, string> = {
  ok: 'bg-emerald-500 rh-pulse-dot',
  off: 'bg-zinc-400 dark:bg-zinc-500',
  warn: 'bg-amber-500',
  idle: 'bg-zinc-400 dark:bg-zinc-500',
  error: 'bg-rose-500',
};

const SIGNAL_PILL_CLASS: Record<RhSignal, string> = {
  ok: 'border-emerald-500/25 bg-emerald-500/10 text-emerald-700 dark:text-emerald-300',
  off: 'border-border bg-muted/60 text-muted-foreground',
  warn: 'border-amber-500/30 bg-amber-500/10 text-amber-700 dark:text-amber-300',
  idle: 'border-border bg-muted/40 text-muted-foreground',
  error: 'border-rose-500/30 bg-rose-500/10 text-rose-700 dark:text-rose-300',
};

export function StatusBadge({
  signal,
  text,
  className,
}: {
  signal: RhSignal;
  text?: React.ReactNode;
  className?: string;
}) {
  return (
    <span
      className={cn(
        'inline-flex items-center gap-1.5 rounded-full border px-2.5 py-0.5 text-xs font-medium whitespace-nowrap transition-colors',
        SIGNAL_PILL_CLASS[signal],
        className,
      )}
    >
      <span
        aria-hidden="true"
        className={cn('inline-block size-1.5 shrink-0 rounded-full', SIGNAL_DOT_CLASS[signal])}
      />
      <span>{text}</span>
    </span>
  );
}

/** 转发规则状态 → 徽章信号(七态收敛为五态语义)。 */
export function proxyStatusSignal(
  status: string,
  healthy?: boolean,
): { signal: RhSignal; text: string } {
  if (!healthy && status === 'running') {
    return { signal: 'warn', text: '运行中 · 探针异常' };
  }
  switch (status) {
    case 'running':
      return { signal: 'ok', text: '运行中' };
    case 'pending':
      return { signal: 'warn', text: '待同步' };
    case 'bind_error':
      return { signal: 'error', text: '端口冲突' };
    case 'apply_error':
      return { signal: 'error', text: '下发异常' };
    case 'offline':
      return { signal: 'off', text: '节点离线' };
    default:
      return { signal: 'idle', text: '已停用' };
  }
}

/** 事件级别 → 徽章信号。 */
export function eventLevelSignal(level: string): RhSignal {
  switch (level) {
    case 'error':
      return 'error';
    case 'warn':
    case 'warning':
      return 'warn';
    case 'info':
      return 'ok';
    default:
      return 'idle';
  }
}
