import { ProxyListPage } from '@/components/proxy-list-page';
import type { ProxyKind } from '@/features/services/types';

const KINDS: ProxyKind[] = ['http', 'https', 'tcpmux'];

export default function DomainsPage() {
  return (
    <ProxyListPage
      kinds={KINDS}
      defaultKind="http"
      title="域名服务"
      description="HTTP / HTTPS / TCPMux 域名规则"
      emptyNoun="域名转发规则"
    />
  );
}
