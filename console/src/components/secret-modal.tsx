import { ChevronDown, Copy, KeyRound, ShieldAlert, Terminal } from 'lucide-react';
import { useMemo, useState } from 'react';

import { MonoCopy } from '@/components/mono';
import { Button } from '@/components/ui/button';
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from '@/components/ui/collapsible';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';
import { Switch } from '@/components/ui/switch';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { focusFirstControl } from '@/lib/dialog-focus';
import { feedbackSuccess, feedbackWarning } from '@/lib/feedback';
import { cn } from '@/lib/utils';

const PROTOCOLS = ['tls', 'tcp', 'ws', 'wss', 'quic', 'kcp'] as const;
const SCRIPT_URL =
  'https://raw.githubusercontent.com/xbclub/rabbithole/main/deploy/install-client.sh';

type SnippetTab = 'docker' | 'script' | 'toml';

async function copyToClipboard(text: string): Promise<void> {
  try {
    if (navigator.clipboard && window.isSecureContext) {
      await navigator.clipboard.writeText(text);
      feedbackSuccess('已复制');
      return;
    }
    const el = document.createElement('textarea');
    el.value = text;
    el.style.position = 'fixed';
    el.style.opacity = '0';
    document.body.appendChild(el);
    el.select();
    const ok = document.execCommand('copy');
    document.body.removeChild(el);
    if (ok) {
      feedbackSuccess('已复制');
    } else {
      feedbackWarning('复制失败,请手动选中复制');
    }
  } catch {
    feedbackWarning('复制失败,请手动选中复制');
  }
}

/** 仿真终端命令块:带终端顶栏状态灯与一键复制。 */
function CommandBlock({
  label,
  text,
  shellTitle = 'bash',
}: {
  label: string;
  text: string;
  shellTitle?: string;
}) {
  return (
    <div className="space-y-2">
      <div className="overflow-hidden rounded-xl border border-zinc-800 bg-zinc-950 text-zinc-100 shadow-xs">
        <div className="flex items-center justify-between border-b border-zinc-800/80 bg-zinc-900/90 px-3 py-1.5 text-[11px] text-zinc-400">
          <div className="flex items-center gap-1.5">
            <span className="size-2.5 rounded-full bg-rose-500/80" />
            <span className="size-2.5 rounded-full bg-amber-500/80" />
            <span className="size-2.5 rounded-full bg-emerald-500/80" />
            <span className="rh-mono ml-2 text-zinc-400">{shellTitle}</span>
          </div>
          <button
            type="button"
            onClick={() => void copyToClipboard(text)}
            className="inline-flex items-center gap-1 rounded px-1.5 py-0.5 text-[11px] text-zinc-300 transition-colors hover:bg-zinc-800 hover:text-white"
          >
            <Copy className="size-3" />
            复制
          </button>
        </div>
        <pre className="rh-mono max-h-52 overflow-auto p-3.5 text-xs leading-relaxed break-words whitespace-pre-wrap text-emerald-300">
          {text}
        </pre>
      </div>
      <Button
        type="button"
        variant="outline"
        size="sm"
        className="w-full"
        onClick={() => void copyToClipboard(text)}
      >
        <Copy className="size-3.5" aria-hidden="true" />
        {label}
      </Button>
    </div>
  );
}

export function SecretModal({
  open,
  onOpenChange,
  clientId,
  secret,
  title = '客户端凭据(仅此一次展示)',
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  clientId?: string;
  secret: string;
  title?: string;
}) {
  const [serverAddr, setServerAddr] = useState(() => `${window.location.hostname}:7100`);
  const [protocol, setProtocol] = useState('tls');
  const [tlsInsecure, setTlsInsecure] = useState(true);
  const [tab, setTab] = useState<SnippetTab>('script');
  const [tomlOpen, setTomlOpen] = useState(false);

  const id = clientId ?? '<客户端ID>';
  const scriptCmd = useMemo(
    () =>
      `curl -fsSL ${SCRIPT_URL} | sudo bash -s -- \\\n  --server ${serverAddr} --protocol ${protocol} \\\n  --id ${id} --secret ${secret}${tlsInsecure ? ' \\\n  --insecure' : ''}`,
    [serverAddr, protocol, id, secret, tlsInsecure],
  );
  const dockerCmd = useMemo(
    () =>
      [
        'docker run -d --name rh-client --restart unless-stopped \\',
        `  -e RH_SERVER=${serverAddr} \\`,
        `  -e RH_PROTOCOL=${protocol} \\`,
        `  -e RH_CLIENT_ID=${id} \\`,
        `  -e RH_SECRET=${secret} \\`,
        ...(tlsInsecure ? ['  -e RH_TLS_INSECURE=true \\'] : []),
        '  ghcr.io/xbclub/rabbithole-client:latest',
      ].join('\n'),
    [serverAddr, protocol, id, secret, tlsInsecure],
  );
  const toml = useMemo(
    () =>
      [
        `server_addr = "${serverAddr}"`,
        `protocol = "${protocol}"`,
        'multiplex = true',
        `client_id = "${id}"`,
        `secret = "${secret}"`,
        `tls_insecure_skip_verify = ${tlsInsecure}`,
        'log_level = "info"',
      ].join('\n'),
    [serverAddr, protocol, id, secret, tlsInsecure],
  );

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent
        className="max-h-[90svh] overflow-y-auto p-4 sm:max-w-2xl sm:p-6"
        onOpenAutoFocus={focusFirstControl}
      >
        <DialogHeader>
          <div className="flex items-center gap-2 text-xs font-medium text-primary">
            <Terminal className="size-3.5" />
            <span>EDGE NODE ONBOARDING WIZARD</span>
          </div>
          <DialogTitle>{title}</DialogTitle>
          <DialogDescription className="flex items-center gap-1.5 text-amber-600 dark:text-amber-400">
            <ShieldAlert className="size-4 shrink-0" />
            请立即复制客户端密钥并部署到内网机器，关闭本窗口后服务端不再回显明文密钥。
          </DialogDescription>
        </DialogHeader>

        {/* 凭据明文卡片 */}
        <div className="space-y-2 rounded-xl border border-amber-500/30 bg-amber-500/5 p-3.5">
          <div className="flex items-center justify-between text-xs text-muted-foreground">
            <span className="inline-flex items-center gap-1.5 font-medium text-foreground">
              <KeyRound className="size-3.5 text-amber-500" />
              一次性客户端密钥 (Secret)
            </span>
            {clientId ? <span className="rh-mono text-[11px]">节点 ID: {clientId}</span> : null}
          </div>
          <div className="flex items-center gap-2 rounded-lg border border-border/80 bg-background px-3 py-2">
            {secret ? (
              <MonoCopy full={secret} className="min-w-0 flex-1 font-semibold" />
            ) : (
              <span className="text-xs text-muted-foreground">服务端未返回客户端密钥</span>
            )}
          </div>
          <Button
            type="button"
            variant="outline"
            size="sm"
            className="w-full"
            disabled={!secret}
            onClick={() => void copyToClipboard(secret)}
          >
            <Copy className="size-3.5" aria-hidden="true" />
            复制客户端密钥
          </Button>
        </div>

        {/* 接入参数配置条 */}
        <div className="space-y-3 rounded-xl border border-border/80 bg-muted/20 p-3.5">
          <div className="flex flex-wrap items-center gap-2">
            <span className="rh-mono shrink-0 text-xs font-medium text-muted-foreground">
              server_addr
            </span>
            <Input
              aria-label="server_addr"
              className="h-8 min-w-44 flex-1 bg-background"
              value={serverAddr}
              onChange={(event) => setServerAddr(event.target.value)}
            />
            <Select value={protocol} onValueChange={setProtocol}>
              <SelectTrigger size="sm" className="w-28 bg-background" aria-label="协议">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                {PROTOCOLS.map((item) => (
                  <SelectItem key={item} value={item}>
                    {item.toUpperCase()}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
          <p className="text-xs leading-relaxed text-muted-foreground">
            默认地址按当前访问主机 + 控制端口 7100
            推测，请以实际服务端控制端口为准；传输协议需与服务端{' '}
            <span className="rh-mono">bind_protocol</span> 一致。
          </p>

          <div className="flex items-center justify-between gap-4 rounded-lg border border-border/70 bg-background/80 px-3 py-2">
            <div className="min-w-0 space-y-0.5">
              <Label htmlFor="tls-insecure" className="text-xs font-medium">
                跳过 TLS 证书校验 (自签证书服务端)
              </Label>
              <p className="text-xs leading-relaxed text-muted-foreground">
                服务端一键部署默认自动生成自签证书，需开启才能握手成功；配置可信证书后请关闭。
              </p>
            </div>
            <Switch
              id="tls-insecure"
              checked={tlsInsecure}
              onCheckedChange={setTlsInsecure}
              aria-label="跳过 TLS 证书校验"
            />
          </div>
        </div>

        <Tabs value={tab} onValueChange={(value) => setTab(value as SnippetTab)} className="gap-3">
          <TabsList className="grid w-full grid-cols-3">
            <TabsTrigger value="script" className="text-xs">
              <span className="sm:hidden">安装脚本</span>
              <span className="max-sm:hidden">一键安装脚本(推荐)</span>
            </TabsTrigger>
            <TabsTrigger value="docker" className="text-xs">
              <span className="sm:hidden">Docker</span>
              <span className="max-sm:hidden">Docker 一键命令</span>
            </TabsTrigger>
            <TabsTrigger value="toml" className="text-xs">
              <span className="sm:hidden">TOML 模板</span>
              <span className="max-sm:hidden">rh-client.toml 模板</span>
            </TabsTrigger>
          </TabsList>
          <TabsContent value="script" className="space-y-2">
            <CommandBlock label="复制安装命令" text={scriptCmd} shellTitle="linux-systemd.sh" />
            <p className="text-xs leading-relaxed text-muted-foreground">
              在内网 Linux 机器执行；自动下载对应架构二进制并注册 systemd 服务，凭据安全写入{' '}
              <span className="rh-mono">/etc/rabbithole/client.env (0600)</span>。
            </p>
          </TabsContent>
          <TabsContent value="docker">
            <CommandBlock label="复制 Docker 命令" text={dockerCmd} shellTitle="docker-cli" />
          </TabsContent>
          <TabsContent value="toml">
            <Collapsible open={tomlOpen} onOpenChange={setTomlOpen} className="space-y-2">
              <div className="flex items-center gap-2">
                <CollapsibleTrigger asChild>
                  <Button
                    type="button"
                    variant="outline"
                    size="sm"
                    className="min-w-0 flex-1 justify-between"
                  >
                    <span className="truncate">rh-client.toml 模板(裸机手动)</span>
                    <ChevronDown
                      aria-hidden="true"
                      className={cn(
                        'size-4 shrink-0 transition-transform',
                        tomlOpen && 'rotate-180',
                      )}
                    />
                  </Button>
                </CollapsibleTrigger>
                <Button
                  type="button"
                  variant="outline"
                  size="sm"
                  className="shrink-0"
                  onClick={() => void copyToClipboard(toml)}
                >
                  <Copy className="size-3.5" aria-hidden="true" />
                  复制完整配置
                </Button>
              </div>
              <CollapsibleContent>
                <pre className="rh-mono max-h-52 overflow-auto rounded-xl border border-zinc-800 bg-zinc-950 p-3.5 text-xs leading-relaxed break-words whitespace-pre-wrap text-emerald-300">
                  {toml}
                </pre>
              </CollapsibleContent>
            </Collapsible>
          </TabsContent>
        </Tabs>

        <DialogFooter>
          <Button type="button" onClick={() => onOpenChange(false)}>
            我已保存,关闭
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
