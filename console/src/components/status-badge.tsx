import { cn } from '@/lib/utils';

/**
 * 状态徽章:8px 信号点 + 文字,四态语义(在线/离线/警告/未启用),不做大色块底。
 * 色值走 Tailwind 语义色并配 dark 变体,保证暗色主题下可读。
 */
export type RhSignal = 'ok' | 'off' | 'warn' | 'idle' | 'error';

const SIGNAL_CLASS: Record<RhSignal, string> = {
  ok: 'bg-emerald-500',
  off: 'bg-zinc-300 dark:bg-zinc-600',
  warn: 'bg-amber-500',
  idle: 'bg-zinc-400 dark:bg-zinc-500',
  error: 'bg-red-500',
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
    <span className={cn('inline-flex items-center gap-1.5 whitespace-nowrap', className)}>
      <span
        aria-hidden="true"
        className={cn('inline-block size-2 shrink-0 rounded-full', SIGNAL_CLASS[signal])}
      />
      <span>{text}</span>
    </span>
  );
}

/** 转发规则状态 → 徽章信号(七态收敛为四态语义)。 */
export function proxyStatusSignal(
  status: string,
  healthy?: boolean,
): { signal: RhSignal; text: string } {
  if (!healthy && status === 'running') {
    return { signal: 'warn', text: 'running(不健康)' };
  }
  switch (status) {
    case 'running':
      return { signal: 'ok', text: 'running' };
    case 'pending':
      return { signal: 'warn', text: 'pending' };
    case 'bind_error':
      return { signal: 'error', text: 'bind_error' };
    case 'apply_error':
      return { signal: 'error', text: 'apply_error' };
    case 'offline':
      return { signal: 'off', text: 'offline' };
    default:
      return { signal: 'idle', text: '未启用' };
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
