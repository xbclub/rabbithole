import { MoreHorizontal } from 'lucide-react';
import { useEffect, useRef, useState } from 'react';

import { Menubar, MenubarContent, MenubarMenu, MenubarTrigger } from '@/components/ui/menubar';
import { cn } from '@/lib/utils';

/** 鼠标离开触发器/菜单后延迟关闭,给「从 ⋯ 移向菜单内容」留出时间。 */
const CLOSE_DELAY_MS = 180;

/**
 * 行操作的「⋯」:列表行超过 2 个动作时,主操作留在主位,其余收进这里。
 *
 * 用 shadcn 的 Menubar(底层 Radix Menubar)而不是 DropdownMenu,原因是后者
 * 默认 modal:打开时会给页面其余部分加 `pointer-events: none`,导致触发器收到
 * pointerleave → 关 → 再开,悬停展开时反复闪烁。Radix Menubar 的菜单根是
 * `modal: false`,结构上不会出现这个循环。
 *
 * 交互:
 * - 触发器**常显**(不再随行悬停隐现),键盘可 Tab 到后按 Enter/Space;
 * - **悬停触发器即展开**菜单(Radix 只在「已有菜单打开」时响应悬停切换,
 *   从关闭态展开由这里受控 value 完成);
 * - 鼠标移进菜单内容不会关(内容也参与「离开才关」判定,并带 180ms 宽限)。
 */
export function RowActionsMenu({
  label,
  children,
  className,
}: {
  /** 可访问名,例如「smoke-client 的操作」。 */
  label: string;
  children: React.ReactNode;
  className?: string;
}) {
  const [value, setValue] = useState('');
  const closeTimerRef = useRef<number | null>(null);

  const cancelClose = () => {
    if (closeTimerRef.current !== null) {
      window.clearTimeout(closeTimerRef.current);
      closeTimerRef.current = null;
    }
  };

  const scheduleClose = () => {
    cancelClose();
    closeTimerRef.current = window.setTimeout(() => setValue(''), CLOSE_DELAY_MS);
  };

  useEffect(() => cancelClose, []);

  return (
    <Menubar
      aria-label={label}
      value={value}
      onValueChange={setValue}
      className={cn('h-auto gap-0 rounded-none border-0 bg-transparent p-0 shadow-none', className)}
    >
      <MenubarMenu value="actions">
        <MenubarTrigger
          aria-label={label}
          className="flex size-9 items-center justify-center rounded-md p-0 text-muted-foreground transition-colors hover:bg-accent hover:text-accent-foreground focus-visible:ring-[3px] focus-visible:ring-ring/50 focus-visible:outline-none data-[state=open]:bg-accent data-[state=open]:text-accent-foreground"
          onPointerEnter={(event) => {
            if (event.pointerType === 'touch') return;
            cancelClose();
            setValue('actions');
          }}
          onPointerLeave={(event) => {
            if (event.pointerType === 'touch') return;
            scheduleClose();
          }}
        >
          <MoreHorizontal className="size-5 shrink-0" aria-hidden="true" />
        </MenubarTrigger>
        <MenubarContent
          align="end"
          className="w-48"
          onPointerEnter={cancelClose}
          onPointerLeave={scheduleClose}
        >
          {children}
        </MenubarContent>
      </MenubarMenu>
    </Menubar>
  );
}
