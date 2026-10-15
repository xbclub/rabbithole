import { ChevronDown, Copy } from 'lucide-react';
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
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { focusFirstControl } from '@/lib/dialog-focus';
import { feedbackSuccess, feedbackWarning } from '@/lib/feedback';
import { cn } from '@/lib/utils';

/** env / toml 模板共用的协议列表(与服务端 bind_protocol 一致)。 */
const PROTOCOLS = ['tls', 'tcp', 'ws', 'wss', 'quic', 'kcp'] as const;
const SCRIPT_URL =
  'https://raw.githubusercontent.com/xbclub/rabbithole/main/deploy/install-client.sh';

type SnippetTab = 'docker' | 'script' | 'toml';

/**
 * 剪贴板写入:安全上下文走 Clipboard API,纯 HTTP 下降级 execCommand(旧版 R23 踩过)。
 * 结果统一走 sonner 出口,不依赖组件树内的 message 绑定。
 */
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

/** 命令 / 模板块:等宽可滚动区 + 一键复制。 */
function CommandBlock({ label, text }: { label: string; text: string }) {
  return (
    <div className="space-y-2">
      <pre className="rh-mono max-h-52 overflow-auto rounded-md border bg-muted/40 p-3 text-xs leading-relaxed break-words whitespace-pre-wrap">
        {text}
      </pre>
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

/**
 * 一次性客户端凭据弹窗(新建 / 轮换客户端密钥后弹出,secret 仅此一次返回)。
 * 三档接入凭据全填好:一键脚本(推荐,Linux systemd)/ Docker 一键命令 /
 * rh-client.toml 模板(裸机手动,折叠在 Collapsible 内)。server_addr 按当前访问主机推测,可编辑。
 *
 * 注意:不生成 `transport` 字段 —— ClientConfig 是 deny_unknown_fields,
 * 旧版照抄 transport="noise" 必解析失败(R25 踩过)。
 */
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
  // 默认 tls:与服务端一键部署脚本/生产建议一致;纯内网 tcp 部署自行切换
  const [serverAddr, setServerAddr] = useState(() => `${window.location.hostname}:7100`);
  const [protocol, setProtocol] = useState('tls');
  const [tab, setTab] = useState<SnippetTab>('script');
  const [tomlOpen, setTomlOpen] = useState(false);

  const id = clientId ?? '<客户端ID>';
  // 一键脚本:下载 + 校验 + systemd 服务(凭据经 /etc/rabbithole/client.env 接入)
  const scriptCmd = useMemo(
    () =>
      `curl -fsSL ${SCRIPT_URL} | sudo bash -s -- \\\n  --server ${serverAddr} --protocol ${protocol} \\\n  --id ${id} --secret ${secret}`,
    [serverAddr, protocol, id, secret],
  );
  // Docker:纯环境变量接入
  const dockerCmd = useMemo(
    () =>
      [
        'docker run -d --name rh-client --restart unless-stopped \\',
        `  -e RH_SERVER=${serverAddr} \\`,
        `  -e RH_PROTOCOL=${protocol} \\`,
        `  -e RH_CLIENT_ID=${id} \\`,
        `  -e RH_SECRET=${secret} \\`,
        '  ghcr.io/xbclub/rabbithole-client:latest',
      ].join('\n'),
    [serverAddr, protocol, id, secret],
  );
  const toml = useMemo(
    () =>
      [
        `server_addr = "${serverAddr}"`,
        `protocol = "${protocol}"`,
        'multiplex = true',
        `client_id = "${id}"`,
        `secret = "${secret}"`,
        'log_level = "info"',
        // 高级项(可选):trusted_ca_file / tls_pin_sha256 / proxy_url / stun_server / dns_server
      ].join('\n'),
    [serverAddr, protocol, id, secret],
  );

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-2xl" onOpenAutoFocus={focusFirstControl}>
        <DialogHeader>
          <DialogTitle>{title}</DialogTitle>
          <DialogDescription className="text-amber-600 dark:text-amber-500">
            请立即复制客户端密钥并配置到客户端,关闭后无法再次查看。
          </DialogDescription>
        </DialogHeader>

        <div className="space-y-2">
          <div className="flex items-center gap-2 rounded-md border bg-muted/40 px-3 py-2">
            {secret ? (
              <MonoCopy full={secret} className="min-w-0 flex-1" />
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

        <div className="space-y-2">
          <div className="flex items-center gap-2">
            <span className="rh-mono shrink-0 text-xs text-muted-foreground">server_addr</span>
            <Input
              aria-label="server_addr"
              className="h-8 flex-1"
              value={serverAddr}
              onChange={(event) => setServerAddr(event.target.value)}
            />
            <Select value={protocol} onValueChange={setProtocol}>
              <SelectTrigger size="sm" className="w-24" aria-label="协议">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                {PROTOCOLS.map((item) => (
                  <SelectItem key={item} value={item}>
                    {item}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
          <p className="text-xs leading-relaxed text-muted-foreground">
            默认地址按当前访问主机 + 控制端口 7100 推测,以实际控制端口为准;协议与服务端
            bind_protocol 一致。
          </p>
        </div>

        <Tabs value={tab} onValueChange={(value) => setTab(value as SnippetTab)} className="gap-3">
          <TabsList className="w-full">
            <TabsTrigger value="script">一键安装脚本(推荐)</TabsTrigger>
            <TabsTrigger value="docker">Docker 一键命令</TabsTrigger>
            <TabsTrigger value="toml">rh-client.toml 模板</TabsTrigger>
          </TabsList>
          <TabsContent value="script" className="space-y-2">
            <CommandBlock label="复制安装命令" text={scriptCmd} />
            <p className="text-xs leading-relaxed text-muted-foreground">
              在内网 Linux 机器执行;自动下载二进制并注册 systemd 服务,凭据写入
              /etc/rabbithole/client.env(0600)。轮换客户端密钥后重跑同一命令即可。需 v0.2.2+。
            </p>
          </TabsContent>
          <TabsContent value="docker">
            <CommandBlock label="复制 Docker 命令" text={dockerCmd} />
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
                <pre className="rh-mono max-h-52 overflow-auto rounded-md border bg-muted/40 p-3 text-xs leading-relaxed break-words whitespace-pre-wrap">
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
