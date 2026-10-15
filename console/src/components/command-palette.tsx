import { useQuery } from '@tanstack/react-query';
import {
  ArrowRightLeft,
  Globe,
  KeyRound,
  LayoutDashboard,
  Search,
  Server,
  Settings,
  ShieldCheck,
  Radio,
} from 'lucide-react';
import type { LucideIcon } from 'lucide-react';
import { useEffect, useState } from 'react';
import { useNavigate } from 'react-router';

import { listClients, listServices } from '@/api/rabbithole';
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

/** 转发规则 kind → 对应页面(⌘K 里点一条规则直接跳到它所属的类型页)。 */
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

/** 顶栏的 ⌘K 触发钮(快捷键监听统一在 CommandPalette 里,这里只派发开关事件)。 */
export function CommandPaletteTrigger() {
  return (
    <Button
      variant="outline"
      onClick={() => window.dispatchEvent(new CustomEvent('rh:toggle-command'))}
      aria-label="打开命令面板"
      className="h-9 w-72 max-w-[45vw] justify-start gap-2 font-normal text-muted-foreground max-sm:w-9 max-sm:px-0"
    >
      <Search className="size-4 shrink-0" />
      <span className="truncate max-sm:hidden">搜索页面、客户端或转发规则</span>
      <KbdGroup className="ml-auto shrink-0 max-sm:hidden">
        <Kbd>{isMac() ? '⌘' : 'Ctrl'}</Kbd>
        <Kbd>K</Kbd>
      </KbdGroup>
    </Button>
  );
}

/** ⌘K 命令面板:页面跳转 + 客户端/转发规则按名称搜索。 */
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
      title="命令面板"
      description="跳转页面或搜索客户端与转发规则"
    >
      <CommandInput placeholder="跳转页面、搜索客户端或转发规则…" />
      <CommandList>
        <CommandEmpty>没有匹配结果</CommandEmpty>
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
                  <Icon className="size-4" />
                  {item.title}
                </CommandItem>
              );
            })}
          </CommandGroup>
        ))}
        {clientsQuery.data && clientsQuery.data.length > 0 ? (
          <>
            <CommandSeparator />
            <CommandGroup heading="客户端">
              {clientsQuery.data.slice(0, 20).map((client) => (
                <CommandItem
                  key={client.id}
                  value={`${client.name} ${client.id}`}
                  onSelect={() => go(ROUTES.clients)}
                >
                  <Server className="size-4" />
                  <span className="truncate">{client.name}</span>
                  <span className="rh-mono ml-auto text-xs text-muted-foreground">
                    {client.online ? '在线' : '离线'}
                  </span>
                </CommandItem>
              ))}
            </CommandGroup>
          </>
        ) : null}
        {servicesQuery.data && servicesQuery.data.length > 0 ? (
          <>
            <CommandSeparator />
            <CommandGroup heading="转发规则">
              {servicesQuery.data.slice(0, 30).map((service) => (
                <CommandItem
                  key={service.id}
                  value={`${service.name} ${service.kind}`}
                  onSelect={() => go(KIND_ROUTE[service.kind] ?? ROUTES.tcp)}
                >
                  <span className="truncate">{service.name}</span>
                  <span className="rh-mono ml-auto text-xs text-muted-foreground">
                    {service.kind}
                  </span>
                </CommandItem>
              ))}
            </CommandGroup>
          </>
        ) : null}
      </CommandList>
    </CommandDialog>
  );
}
