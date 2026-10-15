import { Activity } from 'lucide-react';
import { Area, AreaChart, CartesianGrid, XAxis, YAxis } from 'recharts';

import { EmptyState } from '@/components/empty-state';
import type { TrafficRatePoint } from '@/components/traffic-rate';
import { ChartContainer, ChartTooltip, ChartTooltipContent } from '@/components/ui/chart';
import type { ChartConfig } from '@/components/ui/chart';
import { fmtBytes } from '@/lib/format';
import { cn } from '@/lib/utils';

/** 流量速率面积图:渐变填充、Y 轴自适应量级、tooltip 与 Y 轴刻度都走 fmtBytes。 */

const RATE_CONFIG = {
  in: { label: '入站', color: 'var(--chart-1)' },
  out: { label: '出站', color: 'var(--chart-2)' },
} satisfies ChartConfig;

/** 流量速率面积图:渐变填充、Y 轴自适应量级、tooltip 与 Y 轴刻度都走 fmtBytes。 */
export function TrafficRateChart({
  className,
  points,
}: {
  className?: string;
  points: TrafficRatePoint[];
}) {
  if (points.length === 0) {
    return (
      <EmptyState
        icon={<Activity aria-hidden="true" className="size-4" />}
        title="暂无速率采样"
        description="每 5 秒采样一次,两个成功采样点后开始绘制;请求失败期间曲线断开,不补零、不外推。"
      />
    );
  }

  return (
    <ChartContainer className={cn('aspect-auto h-[220px] w-full', className)} config={RATE_CONFIG}>
      <AreaChart data={points} margin={{ bottom: 0, left: 0, right: 8, top: 8 }}>
        <defs>
          <linearGradient id="rh-rate-in" x1="0" x2="0" y1="0" y2="1">
            <stop offset="0%" stopColor="var(--color-in)" stopOpacity={0.3} />
            <stop offset="100%" stopColor="var(--color-in)" stopOpacity={0.02} />
          </linearGradient>
          <linearGradient id="rh-rate-out" x1="0" x2="0" y1="0" y2="1">
            <stop offset="0%" stopColor="var(--color-out)" stopOpacity={0.26} />
            <stop offset="100%" stopColor="var(--color-out)" stopOpacity={0.02} />
          </linearGradient>
        </defs>
        <CartesianGrid strokeDasharray="3 3" vertical={false} />
        <XAxis axisLine={false} dataKey="label" minTickGap={28} tickLine={false} tickMargin={8} />
        <YAxis
          axisLine={false}
          tickFormatter={(value: number) => fmtBytes(value)}
          tickLine={false}
          tickMargin={4}
          width={64}
        />
        <ChartTooltip
          content={
            <ChartTooltipContent
              // 自定义 formatter 会整行替换 shadcn 的默认行,因此这里自己带上序列名;
              // 断点没有速率,显示 `-` 而不是 0 B/s(不补零)
              formatter={(value: unknown, name: unknown) => (
                <span className="flex w-full items-center justify-between gap-3">
                  <span className="text-muted-foreground">{String(name)}</span>
                  <span className="rh-mono font-medium tabular-nums">
                    {value === null || value === undefined ? '-' : `${fmtBytes(Number(value))}/s`}
                  </span>
                </span>
              )}
            />
          }
        />
        <Area
          connectNulls={false}
          dataKey="in"
          fill="url(#rh-rate-in)"
          isAnimationActive={false}
          name="入站"
          stroke="var(--color-in)"
          strokeWidth={2}
          type="monotone"
        />
        <Area
          connectNulls={false}
          dataKey="out"
          fill="url(#rh-rate-out)"
          isAnimationActive={false}
          name="出站"
          stroke="var(--color-out)"
          strokeWidth={2}
          type="monotone"
        />
      </AreaChart>
    </ChartContainer>
  );
}
