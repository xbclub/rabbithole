import { ProxyListPage } from '@/components/proxy-list-page';
import type { ProxyKind } from '@/features/services/types';

const KINDS: ProxyKind[] = ['tcp'];

export default function TcpPage() {
  return (
    <ProxyListPage
      kinds={KINDS}
      defaultKind="tcp"
      title="TCP 隧道"
      description="TCP 端口转发规则"
      emptyNoun="TCP 转发规则"
    />
  );
}
