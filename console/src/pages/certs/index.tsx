import { zodResolver } from '@hookform/resolvers/zod';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { Eye, EyeOff, Plus, ShieldCheck, TriangleAlert, Upload } from 'lucide-react';
import { useRef, useState } from 'react';
import type { ChangeEvent } from 'react';
import { useForm } from 'react-hook-form';
import { z } from 'zod';

import { createCert, deleteCert, listCerts, updateCert } from '@/api/rabbithole';
import type { CertRow } from '@/api/rabbithole';
import { ConfirmDialog } from '@/components/confirm-dialog';
import { DataTable, DataTableColumnHeader } from '@/components/data-table';
import type { RhColumnDef } from '@/components/data-table';
import { EmptyState } from '@/components/empty-state';
import { InfoLabel } from '@/components/info-label';
import { Mono, MonoCopy } from '@/components/mono';
import { PageHeader } from '@/components/page-header';
import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert';
import { Badge } from '@/components/ui/badge';
import { Tooltip, TooltipContent, TooltipTrigger } from '@/components/ui/tooltip';
import { Button } from '@/components/ui/button';
import { Card, CardContent } from '@/components/ui/card';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog';
import {
  Form,
  FormControl,
  FormDescription,
  FormField,
  FormItem,
  FormLabel,
  FormMessage,
} from '@/components/ui/form';
import { Input } from '@/components/ui/input';
import { Textarea } from '@/components/ui/textarea';
import { focusFirstControl } from '@/lib/dialog-focus';
import { feedbackSuccess } from '@/lib/feedback';
import { daysUntil, fmtTime } from '@/lib/format';
import { cn } from '@/lib/utils';
import { qk } from '@/lib/query-keys';

/**
 * 私钥 PEM 的合法头。除 PKCS#8 的 `PRIVATE KEY`,还必须接受实际最常见的
 * PKCS#1(`RSA PRIVATE KEY`)与 SEC1(`EC PRIVATE KEY`)—— 旧版只认 PKCS#8,
 * 会把 openssl/nginx 导出的 `*.key` 直接判非法。
 */
const PRIVATE_KEY_KINDS = ['PRIVATE KEY', 'RSA PRIVATE KEY', 'EC PRIVATE KEY', 'DSA PRIVATE KEY'];

function hasPemBlock(text: string, kind: string): boolean {
  return text.includes(`-----BEGIN ${kind}-----`) && text.includes(`-----END ${kind}-----`);
}

/** PEM 结构粗校验(拦截明显误贴,最终以服务端解析为准)。 */
function looksLikeCertPem(text: string): boolean {
  return hasPemBlock(text, 'CERTIFICATE');
}

function looksLikePrivateKeyPem(text: string): boolean {
  return PRIVATE_KEY_KINDS.some((kind) => hasPemBlock(text, kind));
}

/** 加密私钥前端解不开,单独给出可执行的下一步。 */
function isEncryptedPrivateKey(text: string): boolean {
  return text.includes('-----BEGIN ENCRYPTED PRIVATE KEY-----');
}

/**
 * -webkit-text-security 只有 Chromium/WebKit 支持。不支持时(如 Firefox)不渲染
 * 「显示/隐藏」开关 —— 否则开关点了没用、反而让人以为已遮住。
 */
const supportsTextSecurity =
  typeof CSS !== 'undefined' && typeof CSS.supports === 'function'
    ? CSS.supports('-webkit-text-security', 'disc')
    : false;

const certSchema = z.object({
  name: z.string().trim().min(1, '请输入名称'),
  domains: z.string(),
  cert_pem: z
    .string()
    .trim()
    .min(1, '请粘贴或上传证书 PEM')
    .refine(looksLikeCertPem, '证书 PEM 结构非法(缺 BEGIN/END CERTIFICATE)'),
  key_pem: z
    .string()
    .trim()
    .min(1, '请粘贴或上传私钥 PEM')
    .refine(
      (v) => !isEncryptedPrivateKey(v),
      '不支持加密私钥:请先解密后再粘贴(openssl rsa -in 原私钥 -out 明文私钥)',
    )
    .refine(
      looksLikePrivateKeyPem,
      '私钥 PEM 结构非法:支持 PKCS#8(PRIVATE KEY)与 PKCS#1 / SEC1(RSA / EC PRIVATE KEY)',
    ),
});

type CertFormValues = z.infer<typeof certSchema>;

const EMPTY_FORM: CertFormValues = { name: '', domains: '', cert_pem: '', key_pem: '' };

/** 到期倒计时:<7 天红、≤30 天橙(证书页核心信息,过期单独提示)。 */
function ExpiryCell({ notAfter }: { notAfter: null | number }) {
  const days = daysUntil(notAfter);
  if (days === null || notAfter === null) {
    return <span className="text-muted-foreground">-</span>;
  }
  const expired = days < 0;
  const tone =
    expired || days < 7
      ? 'border-transparent bg-destructive text-white'
      : days <= 30
        ? 'border-amber-500/40 bg-amber-500/10 text-amber-700 dark:text-amber-400'
        : 'border-border text-muted-foreground';
  return (
    <div className="flex flex-col items-start gap-0.5">
      <Badge className={tone}>{expired ? '已过期' : `${days} 天`}</Badge>
      <Mono className="text-muted-foreground">{fmtTime(notAfter)}</Mono>
    </div>
  );
}

/** 域名列:SAN 列表截断展示(最多 2 个 + 其余数量,悬停看全量)。 */
function DomainsCell({ domains }: { domains: string[] }) {
  if (!domains?.length) {
    return <span className="text-muted-foreground">-</span>;
  }
  return (
    <div className="flex flex-wrap items-center gap-1">
      {domains.slice(0, 2).map((d) => (
        <Badge key={d} variant="secondary" className="font-mono text-[11px]">
          {d}
        </Badge>
      ))}
      {domains.length > 2 ? (
        <Tooltip>
          <TooltipTrigger asChild>
            <Badge variant="outline">+{domains.length - 2}</Badge>
          </TooltipTrigger>
          <TooltipContent className="max-w-96">{domains.slice(2).join(', ')}</TooltipContent>
        </Tooltip>
      ) : null}
    </div>
  );
}

/**
 * 证书页:按 SNI 自动匹配、服务端终结 TLS 的 HTTPS 证书。
 * 后端 M3 已上线,CRUD 真实可用;仍保留旧服务端(无 /api/certs)的 404 兜底提示。
 */
export default function CertsPage() {
  const queryClient = useQueryClient();
  const [open, setOpen] = useState(false);
  const [editing, setEditing] = useState<CertRow | null>(null);
  const [keyVisible, setKeyVisible] = useState(false);
  const fileInputRef = useRef<HTMLInputElement | null>(null);

  const form = useForm<CertFormValues>({
    resolver: zodResolver(certSchema),
    defaultValues: EMPTY_FORM,
  });

  const certsQ = useQuery({ queryKey: qk.certs, queryFn: listCerts, retry: false });

  const errorMessage = certsQ.error instanceof Error ? certsQ.error.message : '';
  // 旧版本服务端无 /api/certs 端点:请求失败给可读 Alert,而不是白屏
  const missingEndpoint = certsQ.isError && /404|not found/i.test(errorMessage);

  const invalidate = () => queryClient.invalidateQueries({ queryKey: qk.certs });

  const saveMut = useMutation({
    mutationFn: ({ id, values }: { id: null | number; values: CertFormValues }) => {
      const domains = values.domains
        .split(',')
        .map((s) => s.trim())
        .filter(Boolean);
      const payload: { cert_pem: string; domains?: string[]; key_pem: string; name: string } = {
        name: values.name.trim(),
        cert_pem: values.cert_pem.trim(),
        key_pem: values.key_pem.trim(),
        // 留空则不提交 domains,由服务端从证书 SAN/CN 自动提取
        ...(domains.length ? { domains } : {}),
      };
      return id === null ? createCert(payload) : updateCert(id, payload);
    },
    onSuccess: (_data, variables) => {
      feedbackSuccess(variables.id === null ? '证书已上传' : '证书已更新');
      setOpen(false);
      setEditing(null);
      void invalidate();
    },
  });

  const deleteMut = useMutation({
    mutationFn: (id: number) => deleteCert(id),
    onSuccess: () => {
      feedbackSuccess('证书已删除');
      void invalidate();
    },
  });

  function openCreate() {
    setEditing(null);
    setKeyVisible(false);
    form.reset(EMPTY_FORM);
    setOpen(true);
  }

  function openEdit(row: CertRow) {
    setEditing(row);
    setKeyVisible(false);
    // key_pem 任何响应都不回显:编辑时私钥框必须留空,需重新粘贴
    form.reset({
      name: row.name,
      domains: (row.domains ?? []).join(','),
      cert_pem: '',
      key_pem: '',
    });
    setOpen(true);
  }

  /** 选择文件后按文件名或 PEM 头自动识别证书/私钥并填入对应框。 */
  async function handleFile(event: ChangeEvent<HTMLInputElement>) {
    const file = event.target.files?.[0];
    if (!file) return;
    const text = await file.text();
    if (file.name.includes('key') || looksLikePrivateKeyPem(text)) {
      form.setValue('key_pem', text.trim(), { shouldValidate: true, shouldDirty: true });
    } else {
      form.setValue('cert_pem', text.trim(), { shouldValidate: true, shouldDirty: true });
    }
    // 清空 value,允许连续选择同一个文件
    event.target.value = '';
  }

  const columns: RhColumnDef<CertRow>[] = [
    {
      accessorKey: 'name',
      meta: { label: '名称' },
      header: ({ column }) => <DataTableColumnHeader column={column} title="名称" />,
      cell: ({ row }) => <span className="font-medium">{row.original.name}</span>,
    },
    {
      id: 'domains',
      meta: { label: '域名' },
      header: '域名',
      enableSorting: false,
      cell: ({ row }) => <DomainsCell domains={row.original.domains ?? []} />,
    },
    {
      id: 'not_after',
      meta: { label: '到期' },
      accessorFn: (row) => row.not_after ?? 0,
      header: ({ column }) => <DataTableColumnHeader column={column} title="到期" />,
      cell: ({ row }) => <ExpiryCell notAfter={row.original.not_after} />,
    },
    {
      accessorKey: 'fingerprint',
      meta: { label: '指纹' },
      header: ({ column }) => <DataTableColumnHeader column={column} title="指纹" />,
      cell: ({ row }) =>
        row.original.fingerprint ? (
          <MonoCopy
            ariaLabel="复制证书指纹"
            display={`${row.original.fingerprint.slice(0, 8)}…`}
            full={row.original.fingerprint}
          />
        ) : (
          <span className="text-muted-foreground">-</span>
        ),
    },
    {
      id: 'actions',
      meta: { label: '操作', headerClassName: 'w-[1%]' },
      header: '操作',
      enableHiding: false,
      enableSorting: false,
      cell: ({ row }) => (
        <div className="flex items-center gap-1">
          <Button variant="ghost" size="sm" onClick={() => openEdit(row.original)}>
            编辑
          </Button>
          <ConfirmDialog
            destructive
            confirmText="删除"
            title={`删除证书 ${row.original.name}?`}
            description="绑定该域名的 HTTPS 转发规则将回退为端到端透传。"
            trigger={
              <Button variant="ghost" size="sm" className="text-destructive hover:text-destructive">
                删除
              </Button>
            }
            onConfirm={async () => {
              try {
                await deleteMut.mutateAsync(row.original.id);
              } catch {
                // 失败已由请求拦截器统一提示,这里仅保持确认框关闭语义
              }
            }}
          />
        </div>
      ),
    },
  ];

  return (
    <div className="space-y-6">
      <PageHeader
        title="证书"
        description="按 SNI 自动匹配、服务端终结 TLS 的 HTTPS 证书"
        actions={
          <Button onClick={openCreate}>
            <Plus />
            上传证书
          </Button>
        }
      />

      {certsQ.isError ? (
        <Alert variant="destructive">
          <TriangleAlert />
          <AlertTitle>{missingEndpoint ? '服务端尚未实现证书管理' : '加载证书列表失败'}</AlertTitle>
          <AlertDescription>
            <p>
              {missingEndpoint
                ? '当前服务端无 /api/certs 端点(旧版本);升级服务端后证书管理即可使用。'
                : errorMessage || '未知错误'}
            </p>
            <Button
              variant="outline"
              size="sm"
              disabled={certsQ.isFetching}
              onClick={() => void certsQ.refetch()}
            >
              {certsQ.isFetching ? '重试中…' : '重试'}
            </Button>
          </AlertDescription>
        </Alert>
      ) : null}

      <Card>
        <CardContent>
          <DataTable
            columns={columns}
            data={certsQ.data ?? []}
            loading={certsQ.isLoading}
            getRowId={(row) => String(row.id)}
            initialPageSize={50}
            empty={
              <EmptyState
                icon={<ShieldCheck />}
                title="还没有证书"
                description="上传 PEM 证书后,HTTPS 转发规则将按 SNI 自动匹配并由服务端终结 TLS。"
                action={<Button onClick={openCreate}>上传证书</Button>}
              />
            }
          />
        </CardContent>
      </Card>

      <Dialog
        open={open}
        onOpenChange={(next) => {
          setOpen(next);
          if (!next) setEditing(null);
        }}
      >
        <DialogContent
          className="max-h-[90vh] overflow-y-auto sm:max-w-2xl"
          onOpenAutoFocus={focusFirstControl}
        >
          <DialogHeader>
            <DialogTitle>{editing ? `编辑证书 #${editing.id}` : '上传证书'}</DialogTitle>
            <DialogDescription>
              证书与服务端终结 TLS 的 HTTPS 转发规则配套;私钥仅在提交时上传,任何响应都不回显。
            </DialogDescription>
          </DialogHeader>
          <Form {...form}>
            <form
              className="space-y-4"
              onSubmit={form.handleSubmit((values) => {
                saveMut.mutate({ id: editing?.id ?? null, values });
              })}
            >
              <FormField
                control={form.control}
                name="name"
                render={({ field }) => (
                  <FormItem>
                    <FormLabel>
                      <InfoLabel title="名称" infoKey="cert.name" />
                    </FormLabel>
                    <FormControl>
                      <Input placeholder="wildcard-example" {...field} />
                    </FormControl>
                    <FormMessage />
                  </FormItem>
                )}
              />

              <FormField
                control={form.control}
                name="domains"
                render={({ field }) => (
                  <FormItem>
                    <FormLabel>
                      <InfoLabel title="域名" infoKey="cert.domains" />
                    </FormLabel>
                    <FormControl>
                      <Input placeholder="留空从证书 SAN/CN 自动提取" {...field} />
                    </FormControl>
                    <FormDescription>
                      逗号分隔;留空时由服务端从证书 SAN/CN 自动提取。HTTPS 请求 SNI
                      命中(含单层通配)即由服务端终结 TLS。
                    </FormDescription>
                    <FormMessage />
                  </FormItem>
                )}
              />

              <FormField
                control={form.control}
                name="cert_pem"
                render={({ field }) => (
                  <FormItem>
                    <FormLabel>
                      <InfoLabel title="证书 PEM" infoKey="cert.cert_pem" />
                    </FormLabel>
                    <FormControl>
                      <Textarea
                        // field-sizing-fixed + 固定高度:否则 shadcn Textarea 默认
                        // field-sizing-content 会被上百行的证书链撑得很高
                        className="field-sizing-fixed h-32 max-h-40 overflow-y-auto font-mono text-xs"
                        placeholder="-----BEGIN CERTIFICATE-----…"
                        rows={5}
                        spellCheck={false}
                        {...field}
                      />
                    </FormControl>
                    <FormMessage />
                  </FormItem>
                )}
              />

              <FormField
                control={form.control}
                name="key_pem"
                render={({ field }) => (
                  <FormItem>
                    <div className="flex items-center justify-between gap-2">
                      <FormLabel>
                        <InfoLabel title="私钥 PEM" infoKey="cert.key_pem" />
                      </FormLabel>
                      {/* 私钥是多行 PEM,用 Textarea 而不是单行 Input;
                          遮罩走 -webkit-text-security(仅 Chromium/WebKit),
                          不支持就不显示开关,避免「开关点了没用」 */}
                      {supportsTextSecurity ? (
                        <Button
                          type="button"
                          variant="ghost"
                          size="xs"
                          className="text-muted-foreground"
                          aria-label={keyVisible ? '隐藏私钥' : '显示私钥'}
                          onClick={() => setKeyVisible((v) => !v)}
                        >
                          {keyVisible ? (
                            <EyeOff className="size-3.5" />
                          ) : (
                            <Eye className="size-3.5" />
                          )}
                          {keyVisible ? '隐藏' : '显示'}
                        </Button>
                      ) : null}
                    </div>
                    <FormControl>
                      <Textarea
                        autoComplete="off"
                        className={cn(
                          'field-sizing-fixed h-32 max-h-40 overflow-y-auto font-mono text-xs',
                          !keyVisible && supportsTextSecurity && '[-webkit-text-security:disc]',
                        )}
                        placeholder="-----BEGIN RSA PRIVATE KEY-----…"
                        rows={5}
                        spellCheck={false}
                        {...field}
                      />
                    </FormControl>
                    <FormDescription>
                      {editing
                        ? '编辑时服务端不回显已存私钥,需重新粘贴私钥 PEM。'
                        : '仅上传时提交,任何响应不回显。'}{' '}
                      支持 PKCS#8(PRIVATE KEY)与 PKCS#1 / SEC1(RSA / EC PRIVATE KEY)。
                    </FormDescription>
                    <FormMessage />
                  </FormItem>
                )}
              />

              <div className="space-y-2">
                <div className="text-sm leading-none font-medium">
                  <InfoLabel title="文件上传" infoKey="cert.upload" />
                </div>
                <div className="flex flex-wrap items-center gap-2">
                  <input
                    ref={fileInputRef}
                    type="file"
                    accept=".pem,.crt,.key,.txt"
                    className="hidden"
                    onChange={(event) => {
                      void handleFile(event);
                    }}
                  />
                  <Button
                    type="button"
                    variant="outline"
                    size="sm"
                    onClick={() => fileInputRef.current?.click()}
                  >
                    <Upload />
                    选择证书/私钥文件自动填充
                  </Button>
                  <span className="text-xs text-muted-foreground">
                    按文件名或 PEM 头识别,分别填入证书 / 私钥框
                  </span>
                </div>
              </div>

              <DialogFooter>
                <Button type="button" variant="outline" onClick={() => setOpen(false)}>
                  取消
                </Button>
                <Button type="submit" disabled={saveMut.isPending}>
                  {saveMut.isPending ? '保存中…' : '保存'}
                </Button>
              </DialogFooter>
            </form>
          </Form>
        </DialogContent>
      </Dialog>
    </div>
  );
}
