import axios from 'axios';
import type { AxiosError, AxiosRequestConfig } from 'axios';

import { feedbackError } from '@/lib/feedback';
import { useAuthStore } from '@/stores/auth';

/** 后端 envelope:HTTP 200 + {code, data, message},code===0 取 data。 */
interface Envelope<T = unknown> {
  code: number;
  data: T;
  message: string;
}

/** 生产由 rh-server 同源托管 /api;开发走 vite dev proxy(见 vite.config.ts)。 */
const API_BASE = '/api';

export const LOGIN_PATH = '/user/login';

/** 回跳地址仅接受站内路径(以单个 / 开头),防开放重定向。 */
export function safeRedirect(raw: null | string): string {
  if (raw && raw.startsWith('/') && !raw.startsWith('//')) {
    return raw;
  }
  return '';
}

function redirectToLogin(): void {
  const { pathname, search } = window.location;
  // 已在登录页则不再跳转,避免循环
  if (!pathname.startsWith(LOGIN_PATH)) {
    const redirect = encodeURIComponent(pathname + search);
    window.location.href = `${LOGIN_PATH}?redirect=${redirect}`;
  }
}

export const requestClient = axios.create({
  baseURL: API_BASE,
  timeout: 15_000,
});

requestClient.interceptors.request.use((config) => {
  const token = useAuthStore.getState().token;
  if (token) {
    config.headers.Authorization = `Bearer ${token}`;
  }
  return config;
});

requestClient.interceptors.response.use(
  (res) => {
    // 204 / 空 body(logout 等)直接透传
    if (res.status === 204 || res.data == null || typeof res.data !== 'object') {
      return res.data;
    }
    const env = res.data as Envelope;
    if (env.code === 0) {
      return env.data;
    }
    // 业务失败:弹中文 message(拦截器统一出口,页面无需重复弹)
    feedbackError(env.message || '操作失败');
    return Promise.reject(new Error(env.message || `业务错误 code=${env.code}`));
  },
  (error: AxiosError<Partial<Envelope>>) => {
    // 认证失效是 HTTP 401 状态码(非业务 code):清 token 并带回跳地址
    if (error.response?.status === 401) {
      useAuthStore.getState().clear();
      redirectToLogin();
      return Promise.reject(new Error('登录已失效'));
    }
    let msg: string;
    if (error.response?.data?.message) {
      msg = error.response.data.message;
    } else if (error.message === 'Network Error') {
      msg = '网络错误,请检查与服务端的连接';
    } else {
      msg = error.message || '请求失败';
    }
    feedbackError(msg);
    return Promise.reject(new Error(msg));
  },
);

/** 类型化封装:拦截器已解包 envelope,await 结果即 data 本身。 */
export async function httpGet<T>(url: string, config?: AxiosRequestConfig): Promise<T> {
  return (await requestClient.get(url, config)) as unknown as T;
}

export async function httpPost<T>(
  url: string,
  body?: unknown,
  config?: AxiosRequestConfig,
): Promise<T> {
  return (await requestClient.post(url, body, config)) as unknown as T;
}

export async function httpPut<T>(
  url: string,
  body?: unknown,
  config?: AxiosRequestConfig,
): Promise<T> {
  return (await requestClient.put(url, body, config)) as unknown as T;
}

export async function httpDelete<T>(url: string, config?: AxiosRequestConfig): Promise<T> {
  return (await requestClient.delete(url, config)) as unknown as T;
}
