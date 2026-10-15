import { useState } from 'react';
import { useSearchParams } from 'react-router';

import type { ServiceRow } from '@/api/rabbithole';
import { PageHeader } from '@/components/page-header';
import { ProxyListPage } from '@/components/proxy-list-page';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { VisitorsPanel } from '@/components/visitors-panel';
import type { VisitorSeed } from '@/components/visitors-panel';
import type { ProxyKind } from '@/features/services/types';

const KINDS: ProxyKind[] = ['stcp', 'sudp', 'xtcp'];

/**
 * 密钥访问:规则(stcp/sudp/xtcp)+ 访问器双 Tab。
 * Tab 状态写 query 参数 `?tab=visitors`,刷新后保持(旧路由 /rabbithole/visitors 重定向到此)。
 */
export default function SecretPage() {
  const [searchParams, setSearchParams] = useSearchParams();
  const tab = searchParams.get('tab') === 'visitors' ? 'visitors' : 'rules';
  // 规则行「创建访问器」带入的目标(访问密钥由服务端从目标规则继承,不手工传递)
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
    <div className="flex min-w-0 flex-col gap-6">
      <PageHeader title="密钥访问" description="STCP / SUDP / XTCP 密钥访问规则与访问器" />
      <Tabs value={tab} onValueChange={onTabChange}>
        <TabsList>
          <TabsTrigger value="rules">规则</TabsTrigger>
          <TabsTrigger value="visitors">访问器</TabsTrigger>
        </TabsList>
        <TabsContent value="rules" className="pt-4">
          <ProxyListPage
            bare
            kinds={KINDS}
            defaultKind="stcp"
            emptyNoun="密钥转发规则"
            onCreateVisitor={handleCreateVisitor}
          />
        </TabsContent>
        <TabsContent value="visitors" className="pt-4">
          <VisitorsPanel seed={visitorSeed} onSeedUsed={() => setVisitorSeed(null)} />
        </TabsContent>
      </Tabs>
    </div>
  );
}
