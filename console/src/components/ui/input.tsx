import { Eye, EyeOff } from 'lucide-react';
import * as React from 'react';

import { cn } from '@/lib/utils';

function Input({ className, type, ...props }: React.ComponentProps<'input'>) {
  return (
    <input
      type={type}
      data-slot="input"
      className={cn(
        'h-9 w-full min-w-0 rounded-lg border border-border/85 bg-background/75 px-3 py-1 text-base shadow-2xs transition-all duration-150 outline-none selection:bg-primary selection:text-primary-foreground file:inline-flex file:h-7 file:border-0 file:bg-transparent file:text-sm file:font-medium file:text-foreground placeholder:text-muted-foreground/75 hover:border-primary/35 disabled:pointer-events-none disabled:cursor-not-allowed disabled:opacity-50 md:text-sm dark:bg-muted/30 dark:hover:border-primary/40 dark:hover:bg-muted/45',
        'focus-visible:border-primary focus-visible:bg-background focus-visible:ring-[3px] focus-visible:ring-primary/20 dark:focus-visible:bg-background/95',
        'aria-invalid:border-destructive aria-invalid:ring-destructive/20 dark:aria-invalid:ring-destructive/40',
        className,
      )}
      {...props}
    />
  );
}

export interface PasswordInputProps extends Omit<React.ComponentProps<'input'>, 'type'> {
  leftIcon?: React.ReactNode;
  defaultVisible?: boolean;
}

/** 带明文/密文眼睛切换按钮的密码输入框。 */
function PasswordInput({
  className,
  leftIcon,
  defaultVisible = false,
  disabled,
  ...props
}: PasswordInputProps) {
  const [visible, setVisible] = React.useState(defaultVisible);

  return (
    <div className="relative flex items-center">
      {leftIcon ? (
        <span
          className="pointer-events-none absolute top-1/2 left-3 -translate-y-1/2 text-muted-foreground"
          aria-hidden="true"
        >
          {leftIcon}
        </span>
      ) : null}
      <Input
        {...props}
        disabled={disabled}
        type={visible ? 'text' : 'password'}
        className={cn('pr-9', leftIcon ? 'pl-9' : '', className)}
      />
      <button
        type="button"
        tabIndex={-1}
        disabled={disabled}
        onClick={() => setVisible((prev) => !prev)}
        aria-label={visible ? '隐藏密码' : '显示密码'}
        title={visible ? '隐藏明文密码' : '查看明文密码'}
        className="absolute top-1/2 right-1.5 flex size-6.5 -translate-y-1/2 items-center justify-center rounded-md text-muted-foreground transition-colors hover:bg-muted hover:text-foreground focus-visible:ring-2 focus-visible:ring-ring focus-visible:outline-none disabled:pointer-events-none disabled:opacity-40"
      >
        {visible ? <EyeOff className="size-3.5" /> : <Eye className="size-3.5" />}
      </button>
    </div>
  );
}

export { Input, PasswordInput };
