import { Navigate, useLocation } from 'react-router';

import { ROUTES } from '@/router/paths';
import { useAuthStore } from '@/stores/auth';

/** 未登录访问业务页 → 登录页并带回跳地址(登录成功后由登录页消费 redirect)。 */
export function RequireAuth({ children }: { children: React.ReactNode }) {
  const token = useAuthStore((state) => state.token);
  const location = useLocation();

  if (!token) {
    const redirect = encodeURIComponent(location.pathname + location.search);
    return <Navigate to={`${ROUTES.login}?redirect=${redirect}`} replace />;
  }
  return <>{children}</>;
}
