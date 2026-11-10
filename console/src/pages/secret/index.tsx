import { useState } from 'react';
import { useSearchParams } from 'react-router';

import type { ServiceRow } from '@/api/rabbithole';
import { PageHeader } from '@/components/page-header';
import { ProxyListPage, TunnelHubLensBar } from '@/components/proxy-list-page';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { VisitorsPanel } from '@/components/visitors-panel';
import type { VisitorSeed } from '@/components/visitors-panel';
import type { ProxyKind } from '@/features/services/types';

const KINDS: ProxyKind[] = ['stcp', 'sudp', 'xtcp'];

/**
 * 密钥访问与 P2P 访问器页:无缝集成在隧道中枢透镜体系内。
 * 保持 ?tab=visitors 深链与切换能力。
 */
export default function SecretPage() {
  const [searchParams, setSearchParams] = useSearchParams();
  const tab = searchParams.get('tab') === 'visitors' ? 'visitors' : 'rules';
  const [visitorSeed, setVisitorSeed] = useState<VisitorSeed | null>(null);

  const onTabChange = (next: string) => {
    const params = new URLSearchParams(searchParams);
    if (next === 'visitors') {
      params.set('tab', 'visitors');
    } else {
      params.delete('tab');
    }
    setSearchParams(params, { replace: true });
  };

  const handleCreateVisitor = (row: ServiceRow) => {
    setVisitorSeed({ kind: row.kind as VisitorSeed['kind'], target: row.name });
    onTabChange('visitors');
  };

  return (
    <div className="flex min-w-0 flex-col gap-5">
      <PageHeader
        eyebrow="TUNNEL & ROUTING FABRIC HUB"
        title={tab === 'visitors' ? 'P2P 访问器 (Visitors)' : '私网密钥访问'}
        description="STCP / SUDP / XTCP 零公网暴露私网穿透规则与 P2P 打洞访问端管理"
      />
      <TunnelHubLensBar />
      <Tabs value={tab} onValueChange={onTabChange}>
        <TabsList className="hidden">
          <TabsTrigger value="rules">规则</TabsTrigger>
          <TabsTrigger value="visitors">访问器</TabsTrigger>
        </TabsList>
        <TabsContent value="rules" className="mt-0">
          <ProxyListPage
            bare
            kinds={KINDS}
            defaultKind="stcp"
            emptyNoun="密钥转发规则"
            onCreateVisitor={handleCreateVisitor}
          />
        </TabsContent>
        <TabsContent value="visitors" className="mt-0">
          <VisitorsPanel seed={visitorSeed} onSeedUsed={() => setVisitorSeed(null)} />
        </TabsContent>
      </Tabs>
    </div>
  );
}
