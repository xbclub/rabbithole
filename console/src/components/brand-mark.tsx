import { cn } from '@/lib/utils';

/**
 * 品牌标记:融合「加密隧道虫洞 (RHTP Portal)」与「兔耳脉冲天线」的几何工程徽标。
 * 自适应亮暗色与 primary 品牌色。
 */
export function BrandMark({ className }: { className?: string }) {
  return (
    <svg
      viewBox="0 0 48 48"
      role="img"
      aria-label="rabbithole"
      className={cn('text-primary', className)}
      fill="none"
    >
      {/* 外层隧道折射环 */}
      <rect
        x="4"
        y="4"
        width="40"
        height="40"
        rx="12"
        fill="currentColor"
        fillOpacity="0.12"
        stroke="currentColor"
        strokeWidth="1.75"
        strokeOpacity="0.35"
      />
      {/* 双脉冲兔耳天线 */}
      <path
        d="M17.5 22V12.5C17.5 10.567 19.067 9 21 9C22.933 9 24.5 10.567 24.5 12.5V20"
        stroke="currentColor"
        strokeWidth="2.8"
        strokeLinecap="round"
      />
      <path
        d="M25.5 20V13.5C25.5 11.567 27.067 10 29 10C30.933 10 32.5 11.567 32.5 13.5V22"
        stroke="currentColor"
        strokeWidth="2.8"
        strokeLinecap="round"
      />
      {/* 核心隧道节点舱 */}
      <circle cx="24" cy="28.5" r="10.5" fill="currentColor" />
      {/* 隧道内环与双信号眼 */}
      <circle cx="20.5" cy="27.5" r="1.8" fill="var(--background)" />
      <circle cx="27.5" cy="27.5" r="1.8" fill="var(--background)" />
      <path
        d="M21 32.5C22.2 33.8 25.8 33.8 27 32.5"
        stroke="var(--background)"
        strokeWidth="1.8"
        strokeLinecap="round"
      />
    </svg>
  );
}
