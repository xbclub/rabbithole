import { cn } from '@/lib/utils';

/**
 * 工作台页头:支持顶部协议/模块铭牌(eyebrow)、主标题、说明文字与右侧快捷操作区。
 */
export function PageHeader({
  title,
  description,
  actions,
  eyebrow,
  className,
}: {
  title: React.ReactNode;
  description?: React.ReactNode;
  actions?: React.ReactNode;
  eyebrow?: React.ReactNode;
  className?: string;
}) {
  return (
    <div
      className={cn(
        'flex flex-col gap-2.5 sm:flex-row sm:items-end sm:justify-between sm:gap-3',
        className,
      )}
    >
      <div className="min-w-0 space-y-1">
        {eyebrow ? (
          <div className="flex flex-wrap items-center gap-1.5 text-[11px] font-medium tracking-wider text-primary uppercase sm:gap-2 sm:text-xs">
            {eyebrow}
          </div>
        ) : null}
        <h1 className="text-xl font-semibold tracking-tight text-foreground sm:text-2xl">
          {title}
        </h1>
        {description ? (
          <p className="max-w-3xl text-xs leading-relaxed text-muted-foreground sm:text-sm">
            {description}
          </p>
        ) : null}
      </div>
      {actions ? <div className="flex shrink-0 flex-wrap items-center gap-2">{actions}</div> : null}
    </div>
  );
}
