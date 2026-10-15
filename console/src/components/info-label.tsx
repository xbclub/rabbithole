import { Info } from 'lucide-react';
import { useRef, useState } from 'react';

import { Tooltip, TooltipContent, TooltipTrigger } from '@/components/ui/tooltip';
import { fieldInfo } from '@/features/field-info';
import type { FieldInfoKey } from '@/features/field-info';
import { cn } from '@/lib/utils';

/**
 * 带说明的名词:名词 + ℹ 图标,悬停(桌面)或点按(触屏)弹出该项的功能说明,
 * 替代「括号补充说明」式文案 —— 界面只留名词本身。
 *
 * 说明文案不在本组件里,而是按 key 取自 @/features/field-info(旧版 89 条文案逐字平移)。
 *
 * 用 shadcn 的 Tooltip(Radix Tooltip)。Radix Tooltip 会**忽略 touch 指针**,
 * 因此触屏点按由这里受控 open 手动开合(记录最后一次指针类型,只对触屏生效);
 * 桌面仍由 Radix 的 hover/focus 逻辑驱动。
 */
export function InfoLabel({
  title,
  infoKey,
  info,
  iconOnly = false,
  className,
}: {
  title: React.ReactNode;
  infoKey?: FieldInfoKey;
  info?: React.ReactNode;
  /** 只渲染 ℹ 按钮(名词文字由外部已有元素承担时用,避免重复与非法嵌套)。 */
  iconOnly?: boolean;
  className?: string;
}) {
  const [open, setOpen] = useState(false);
  const lastPointerTypeRef = useRef('');
  /** 触屏:记录「本次按下之前是否已展开」——Radix 会在 pointerdown 时先行关闭。 */
  const wasOpenAtPointerDownRef = useRef(false);

  const content = infoKey ? fieldInfo(infoKey) : info;
  const label = typeof title === 'string' ? title : '该项';

  if (!content) {
    return iconOnly ? null : <span className={className}>{title}</span>;
  }

  return (
    <span className={cn('inline-flex items-center gap-1', className)}>
      {iconOnly ? null : <span>{title}</span>}
      <Tooltip open={open} onOpenChange={setOpen}>
        <TooltipTrigger asChild>
          <button
            type="button"
            aria-label={`${label} 说明`}
            // p-1.5 + -m-1.5:图标视觉尺寸不变,但命中区从 14×14 撑到 26×26,
            // 满足 WCAG 2.2 AA 的 24×24 最小目标
            className="-m-1.5 inline-flex shrink-0 cursor-help items-center rounded-sm p-1.5 text-muted-foreground transition-colors hover:text-foreground focus-visible:ring-[3px] focus-visible:ring-ring/50 focus-visible:outline-none"
            onPointerDown={(event) => {
              lastPointerTypeRef.current = event.pointerType;
              if (event.pointerType === 'touch') {
                // 我的 handler 先于 Radix 执行,此刻拿到的才是「按下前」的真实状态
                wasOpenAtPointerDownRef.current = open;
              }
            }}
            onClick={() => {
              if (lastPointerTypeRef.current !== 'touch') return;
              /*
               * Radix Tooltip 的 trigger 对 touch 的处理(见 @radix-ui/react-tooltip):
               *   pointerdown → 若已展开则 onClose();focus → 指针按下期间跳过打开;
               *   click       → 无条件 onClose()
               * 也就是「点按」这条路 Radix 自己不会打开,还会把我同步设置的 open 关掉。
               * 因此这里按「按下前的状态」取反,并延到下一个宏任务再定态 ——
               * 此时 Radix 的同步 onClose 已经跑完,不会被覆盖。
               */
              const next = !wasOpenAtPointerDownRef.current;
              window.setTimeout(() => setOpen(next), 0);
            }}
          >
            <Info className="size-3.5" aria-hidden="true" />
          </button>
        </TooltipTrigger>
        <TooltipContent side="top" align="start" className="max-w-72 text-xs leading-relaxed">
          {content}
        </TooltipContent>
      </Tooltip>
    </span>
  );
}
