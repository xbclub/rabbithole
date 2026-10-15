import { useEffect, useState } from 'react';

import type { StatsOverview } from '@/api/rabbithole';
import { fmtClock } from '@/lib/format';

/**
 * 流量速率的差分滚动窗口(纯数据,不含图表库)。
 *
 * 与图表组件分开是为了让总览页**先渲染指标条与事件**,recharts(约 367 KB)
 * 随后按需加载 —— 否则整页要等图表库到齐才出内容。
 *
 * 数据:5 秒轮询的 `GET /api/stats/overview` 里的 `bytes_in` / `bytes_out`(进程内累计)
 * 做差分得到 B/s,前端维护 60 点(5s × 60 = 5 分钟)滚动窗口。
 *
 * 断线语义(纯前端呈现约定,不改后端契约):
 * - 只有「成功采样」入列;失败/断网期间在窗口尾部插入 null 断点,曲线断开显示;
 * - 不补零、不跨断线区间外推:失败即丢弃差分基线,恢复后以首个成功采样重建基线;
 * - 失败时保留上一次的当前值,指标条数字不清零。
 */

/** 轮询间隔(ms),与页面的 refetchInterval 一致。 */
const POLL_MS = 5000;
/** 滚动窗口点数:5s × 60 = 5 分钟。 */
const WINDOW_POINTS = 60;
/**
 * 两次成功采样的最大间隔;超过即视为中间漏采(标签页休眠、请求失败未被感知),
 * 断开而不是跨区间平均。留 2.5 倍余量,避免正常抖动被误判为断线。
 */
const MAX_GAP_MS = POLL_MS * 2.5;

/** 速率采样点;`in`/`out` 为 null 表示该处断线(recharts connectNulls=false 即断开)。 */
export interface TrafficRatePoint {
  ts: number;
  label: string;
  in: null | number;
  out: null | number;
}

/** 差分基线:上一次成功采样的累计字节与时间戳。 */
interface RateBaseline {
  bytesIn: number;
  bytesOut: number;
  ts: number;
}

/** 滚动窗口(对外只暴露曲线与当前值)。 */
export interface RateWindow {
  points: TrafficRatePoint[];
  /** 当前入站速率(B/s);请求失败期间保持上一次的值。 */
  currentIn: number;
  /** 当前出站速率(B/s);请求失败期间保持上一次的值。 */
  currentOut: number;
}

/** 内部状态 = 对外窗口 + 差分基线(基线失败即丢弃,展示值保留)。 */
interface RateState extends RateWindow {
  baseline: RateBaseline | null;
}

const INITIAL_STATE: RateState = {
  baseline: null,
  currentIn: 0,
  currentOut: 0,
  points: [],
};

/** 断点:值全为 null,只用于把折线断开(不是 0)。 */
function breakPoint(): TrafficRatePoint {
  return { ts: 0, label: '', in: null, out: null };
}

function isBreak(point: TrafficRatePoint | undefined): boolean {
  return point !== undefined && point.in === null && point.out === null;
}

/** 追加断点并裁剪到窗口长度。 */
function withBreak(state: RateState): RateState {
  return {
    ...state,
    baseline: null,
    points: [...state.points, breakPoint()].slice(-WINDOW_POINTS),
  };
}

/**
 * 维护速率差分滚动窗口。
 *
 * @param stats         最近一次成功的总览数据;undefined 表示从未成功(窗口保持空,不做任何推测)
 * @param dataUpdatedAt 最近一次成功的时间戳(TanStack Query)
 * @param failed        是否处于「最近一次请求失败」状态(errorUpdatedAt > dataUpdatedAt)
 */
export function useTrafficRateWindow(
  stats: StatsOverview | undefined,
  dataUpdatedAt: number,
  failed: boolean,
): RateWindow {
  const [state, setState] = useState<RateState>(INITIAL_STATE);

  useEffect(() => {
    // 从未成功过:没有可断的线,也不做任何推测
    if (!stats) return;

    // 滚动窗口是「外部轮询数据 → React 状态」的同步,effect 正是这里的正确工具;
    // updater 为纯函数且幂等(StrictMode 双调用结果一致),不会产生级联渲染。
    // eslint-disable-next-line react-hooks/set-state-in-effect -- 轮询驱动的滚动窗口,updater 幂等
    setState((prev) => {
      if (failed) {
        // 已处于断线状态:只需确保基线已丢弃(identity 返回可跳过重渲染)
        if (isBreak(prev.points[prev.points.length - 1])) {
          return prev.baseline === null ? prev : { ...prev, baseline: null };
        }
        return withBreak(prev);
      }

      const next: RateBaseline = {
        bytesIn: stats.bytes_in,
        bytesOut: stats.bytes_out,
        ts: dataUpdatedAt,
      };
      // 首次采样 / 断线后恢复 / 同一次采样的重复触发:只记基线,不出点
      if (prev.baseline === null || dataUpdatedAt <= prev.baseline.ts) {
        return { ...prev, baseline: next };
      }

      const dtMs = dataUpdatedAt - prev.baseline.ts;
      // 中间漏采:断开后重建基线,不把跨越漏采区间的累计差值摊成平均速率
      if (dtMs > MAX_GAP_MS) {
        return { ...withBreak(prev), baseline: next };
      }

      const dt = dtMs / 1000;
      // 累计值单调递增;回绕/重置时钳到 0,不出现负速率
      const currentIn = Math.max(0, (stats.bytes_in - prev.baseline.bytesIn) / dt);
      const currentOut = Math.max(0, (stats.bytes_out - prev.baseline.bytesOut) / dt);
      return {
        baseline: next,
        currentIn,
        currentOut,
        points: [
          ...prev.points,
          { ts: dataUpdatedAt, label: fmtClock(dataUpdatedAt), in: currentIn, out: currentOut },
        ].slice(-WINDOW_POINTS),
      };
    });
  }, [stats, dataUpdatedAt, failed]);

  return state;
}
