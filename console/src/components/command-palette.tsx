import { useQuery } from '@tanstack/react-query';
import {
  ArrowRightLeft,
  Globe,
  KeyRound,
  LayoutDashboard,
  Radio,
  Search,
  Server,
  Settings,
  ShieldCheck,
} from 'lucide-react';
import type { LucideIcon } from 'lucide-react';
import { useEffect, useState } from 'react';
import { useNavigate } from 'react-router';

import { listClients, listServices } from '@/api/rabbithole';
import { StatusBadge, proxyStatusSignal } from '@/components/status-badge';
import { Button } from '@/components/ui/button';
import {
  CommandDialog,
  CommandEmpty,
  CommandGroup,
  CommandInput,
  CommandItem,
  CommandList,
  CommandSeparator,
} from '@/components/ui/command';
import { Kbd, KbdGroup } from '@/components/ui/kbd';
import { qk } from '@/lib/query-keys';
import { NAV_GROUPS, ROUTES } from '@/router/paths';

const ICONS: Record<string, LucideIcon> = {
  dashboard: LayoutDashboard,
  clients: Server,
  tcp: ArrowRightLeft,
  udp: Radio,
  domains: Globe,
  secret: KeyRound,
  certs: ShieldCheck,
  settings: Settings,
};

/** 转发规则 kind → 对应页面(⌘K 里点一条规则直接跳到它所属的协议透镜)。 */
const KIND_ROUTE: Record<string, string> = {
  tcp: ROUTES.tcp,
  udp: ROUTES.udp,
  http: ROUTES.domains,
  https: ROUTES.domains,
  tcpmux: ROUTES.domains,
  stcp: ROUTES.secret,
  sudp: ROUTES.secret,
  xtcp: ROUTES.secret,
};

function isMac(): boolean {
  return typeof navigator !== 'undefined' && /Mac|iPhone|iPad/.test(navigator.platform);
}

/** 顶部导航舱的 ⌘K 搜索胶囊触发器(精简紧凑态,不挤压导航栏)。 */
export function CommandPaletteTrigger() {
  return (
    <Button
      variant="outline"
      onClick={() => window.dispatchEvent(new CustomEvent('rh:toggle-command'))}
      aria-label="打开命令面板"
      title="全局命令与拓扑检索 (⌘K)"
      className="h-8 shrink-0 gap-1.5 rounded-full border-border/80 bg-background/60 px-2.5 text-xs font-normal text-muted-foreground shadow-none hover:border-primary/35 hover:bg-accent/60 hover:text-foreground max-sm:size-8 max-sm:px-0"
    >
      <Search className="size-3.5 shrink-0 text-primary" />
      <span className="max-sm:hidden">搜索…</span>
      <KbdGroup className="ml-0.5 shrink-0 max-md:hidden">
        <Kbd>{isMac() ? '⌘' : 'Ctrl'}</Kbd>
        <Kbd>K</Kbd>
      </KbdGroup>
    </Button>
  );
}

/** ⌘K 全局命令中枢:工作台跳转 + 边缘节点检索 + 隧道规则直达。 */
export function CommandPalette() {
  const [open, setOpen] = useState(false);
  const navigate = useNavigate();

  useEffect(() => {
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key.toLowerCase() === 'k' && (event.metaKey || event.ctrlKey)) {
        event.preventDefault();
        setOpen((prev) => !prev);
      }
    };
    const onToggle = () => setOpen((prev) => !prev);
    document.addEventListener('keydown', onKeyDown);
    window.addEventListener('rh:toggle-command', onToggle);
    return () => {
      document.removeEventListener('keydown', onKeyDown);
      window.removeEventListener('rh:toggle-command', onToggle);
    };
  }, []);

  const clientsQuery = useQuery({
    queryKey: qk.clients,
    queryFn: listClients,
    enabled: open,
    staleTime: 30_000,
  });
  const servicesQuery = useQuery({
    queryKey: qk.services(),
    queryFn: () => listServices(),
    enabled: open,
    staleTime: 30_000,
  });

  const go = (to: string) => {
    setOpen(false);
    navigate(to);
  };

  return (
    <CommandDialog
      open={open}
      onOpenChange={setOpen}
      title="全局命令与拓扑检索"
      description="快速切换工作台、检索边缘节点或定位转发隧道"
    >
      <CommandInput placeholder="输入工作台名称、节点主机名、隧道名称或协议类型…" />
      <CommandList>
        <CommandEmpty>未找到匹配的节点、隧道或命令</CommandEmpty>
        {NAV_GROUPS.map((group, index) => (
          <CommandGroup key={group.label ?? `g-${index}`} heading={group.label ?? '导航'}>
            {group.items.map((item) => {
              const Icon = ICONS[item.icon] ?? LayoutDashboard;
              return (
                <CommandItem
                  key={item.to}
                  value={`${item.title} ${item.to}`}
                  onSelect={() => go(item.to)}
                >
                  <Icon className="size-4 text-primary" />
                  <span>{item.title}</span>
                </CommandItem>
              );
            })}
          </CommandGroup>
        ))}
        {clientsQuery.data && clientsQuery.data.length > 0 ? (
          <>
            <CommandSeparator />
            <CommandGroup heading="边缘节点 (点击查看该节点隧道)">
              {clientsQuery.data.slice(0, 20).map((client) => (
                <CommandItem
                  key={client.id}
                  value={`${client.name} ${client.id} ${client.hostname ?? ''}`}
                  onSelect={() => go(`${ROUTES.tcp}?client=${encodeURIComponent(client.id)}`)}
                >
                  <Server className="size-4 text-muted-foreground" />
                  <span className="truncate font-medium">{client.name}</span>
                  {client.hostname ? (
                    <span className="rh-mono truncate text-xs text-muted-foreground">
                      ({client.hostname})
                    </span>
                  ) : null}
                  <span className="ml-auto">
                    <StatusBadge
                      signal={client.online ? 'ok' : 'off'}
                      text={client.online ? '在线' : '离线'}
                    />
                  </span>
                </CommandItem>
              ))}
            </CommandGroup>
          </>
        ) : null}
        {servicesQuery.data && servicesQuery.data.length > 0 ? (
          <>
            <CommandSeparator />
            <CommandGroup heading="转发隧道规则">
              {servicesQuery.data.slice(0, 30).map((service) => {
                const sig = proxyStatusSignal(service.status, service.healthy);
                const route = KIND_ROUTE[service.kind] ?? ROUTES.tcp;
                return (
                  <CommandItem
                    key={service.id}
                    value={`${service.name} ${service.kind} ${service.target_host}:${service.target_port}`}
                    onSelect={() => go(`${route}?q=${encodeURIComponent(service.name)}`)}
                  >
                    <ArrowRightLeft className="size-4 text-muted-foreground" />
                    <span className="truncate font-medium">{service.name}</span>
                    <span className="rh-mono rounded bg-muted px-1.5 py-0.5 text-[11px] uppercase">
                      {service.kind}
                    </span>
                    <span className="ml-auto">
                      <StatusBadge signal={sig.signal} text={sig.text} />
                    </span>
                  </CommandItem>
                );
              })}
            </CommandGroup>
          </>
        ) : null}
      </CommandList>
    </CommandDialog>
  );
}
