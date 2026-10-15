import { useEffect } from 'react';
import { RouterProvider } from 'react-router';

import { Toaster } from '@/components/ui/sonner';
import { watchSystemTheme } from '@/lib/theme';
import { router } from '@/router';

export function App() {
  // 主题模式为「跟随系统」时,系统切换要跟着变
  useEffect(() => watchSystemTheme(), []);

  // 首屏占位(#rh-boot)在 React 首次提交后才移除:render() 是异步的,
  // 在它返回后立刻移除会露出一帧空白(main.tsx 里原先就是这么做的)
  useEffect(() => {
    document.getElementById('rh-boot')?.remove();
  }, []);

  return (
    <>
      <RouterProvider router={router} />
      {/* Toaster 挂在根部、与路由无关:登录页等无布局页面也能收到拦截器报错(R33 的根因) */}
      <Toaster />
    </>
  );
}
