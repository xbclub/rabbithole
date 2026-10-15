/**
 * 品牌标记(洞/兔主题图形,沿用旧版 logo.svg 的路径数据)。
 * 旧文件把填充色写死为 antd 蓝 #1890ff;这里改走 currentColor,
 * 由外层文字色决定,暗色主题下自动反相;眼与嘴用页面背景色挖空。
 */
export function BrandMark({ className }: { className?: string }) {
  return (
    <svg viewBox="0 0 48 48" role="img" aria-label="rabbithole" className={className} fill="none">
      <circle cx="24" cy="26" r="14" fill="currentColor" />
      <ellipse cx="17" cy="12" rx="4" ry="9" fill="currentColor" transform="rotate(-14 17 12)" />
      <ellipse cx="30" cy="11" rx="4" ry="9" fill="currentColor" transform="rotate(12 30 11)" />
      <circle cx="19" cy="24" r="2" fill="var(--background)" />
      <circle cx="29" cy="24" r="2" fill="var(--background)" />
      <path
        d="M22 31 q2 2 4 0"
        stroke="var(--background)"
        strokeWidth="1.6"
        strokeLinecap="round"
      />
    </svg>
  );
}
