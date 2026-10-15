import { ProxyListPage } from '@/components/proxy-list-page';
import type { ProxyKind } from '@/features/services/types';

const KINDS: ProxyKind[] = ['udp'];

export default function UdpPage() {
  return (
    <ProxyListPage
      kinds={KINDS}
      defaultKind="udp"
      title="UDP 隧道"
      description="UDP 端口转发规则"
      emptyNoun="UDP 转发规则"
    />
  );
}
