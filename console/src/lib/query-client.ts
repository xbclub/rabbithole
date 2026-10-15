import { QueryClient } from '@tanstack/react-query';

/**
 * 全局 QueryClient 单例。
 *
 * 放在独立模块里,而不是在组件里 useMemo:Provider 现在挂在控制台外壳上
 * (见 @/components/layout/console-layout),外壳重挂载时缓存不应被清空。
 */
export const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      retry: 1,
      refetchOnWindowFocus: false,
      staleTime: 10_000,
    },
  },
});
