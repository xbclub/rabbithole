import { Check, Copy } from 'lucide-react';
import { useState } from 'react';

import { Button } from '@/components/ui/button';
import { Tooltip, TooltipContent, TooltipTrigger } from '@/components/ui/tooltip';
import { cn } from '@/lib/utils';

/** 纯 mono 展示:端口/IP/UUID/字节/速率/时间戳等。 */
export function Mono({ children, className }: { children: React.ReactNode; className?: string }) {
  return <span className={cn('rh-mono text-[13px]', className)}>{children}</span>;
}

async function copyText(text: string): Promise<boolean> {
  try {
    if (navigator.clipboard && window.isSecureContext) {
      await navigator.clipboard.writeText(text);
      return true;
    }
    // 纯 HTTP 非安全上下文降级(旧版 R23 踩过)
    const el = document.createElement('textarea');
    el.value = text;
    el.style.position = 'fixed';
    el.style.opacity = '0';
    document.body.appendChild(el);
    el.select();
    const ok = document.execCommand('copy');
    document.body.removeChild(el);
    return ok;
  } catch {
    return false;
  }
}

/**
 * mono 展示 + 截断 + 一键复制。
 * display 为界面上的截断展示,full 为复制的完整值(两者不同时必传 display)。
 */
export function MonoCopy({
  full,
  display,
  ariaLabel,
  className,
}: {
  full: string;
  display?: React.ReactNode;
  ariaLabel?: string;
  className?: string;
}) {
  const [copied, setCopied] = useState(false);

  const onCopy = async () => {
    const ok = await copyText(full);
    if (!ok) return;
    setCopied(true);
    window.setTimeout(() => setCopied(false), 1600);
  };

  return (
    <span className={cn('group inline-flex min-w-0 items-center gap-1', className)}>
      {display !== undefined && display !== full ? (
        <Tooltip>
          <TooltipTrigger asChild>
            <span className="rh-mono truncate text-[13px]">{display}</span>
          </TooltipTrigger>
          <TooltipContent className="rh-mono max-w-96 text-xs break-all">{full}</TooltipContent>
        </Tooltip>
      ) : (
        <span className="rh-mono truncate text-[13px]">{full}</span>
      )}
      <Button
        type="button"
        variant="ghost"
        size="icon"
        onClick={onCopy}
        aria-label={ariaLabel ?? `复制 ${full}`}
        className="size-6 shrink-0 text-muted-foreground opacity-0 transition-opacity group-hover:opacity-100 focus-visible:opacity-100 max-md:opacity-100"
      >
        {copied ? <Check className="size-3.5" /> : <Copy className="size-3.5" />}
      </Button>
    </span>
  );
}
