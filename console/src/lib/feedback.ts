/**
 * 模块级提示出口。
 *
 * 旧版用 antd 的 App message,需要组件树里的绑定者(FeedbackBinder),导致
 * 「直接落地登录页」时拦截器报错无人接收(R33 的根因)。新版改用 sonner:
 * `<Toaster />` 挂在应用根部、与路由无关,拦截器等 React 树外代码可直接调用,
 * 结构上消除该类缺陷。
 */
import { toast } from 'sonner';

export function feedbackError(content: string): void {
  toast.error(content);
}

export function feedbackSuccess(content: string): void {
  toast.success(content);
}

export function feedbackWarning(content: string): void {
  toast.warning(content);
}

export function feedbackInfo(content: string): void {
  toast.info(content);
}
