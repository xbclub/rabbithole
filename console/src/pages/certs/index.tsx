import { zodResolver } from '@hookform/resolvers/zod';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import {
  CheckCircle2,
  Clock,
  Eye,
  EyeOff,
  FileKey2,
  Lock,
  Plus,
  Search,
  ShieldAlert,
  ShieldCheck,
  TriangleAlert,
  Upload,
} from 'lucide-react';
import { useMemo, useRef, useState } from 'react';
import type { ChangeEvent, DragEvent } from 'react';
import { useForm, useWatch } from 'react-hook-form';
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
import { Button } from '@/components/ui/button';
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
import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetFooter,
  SheetHeader,
  SheetTitle,
} from '@/components/ui/sheet';
import { Textarea } from '@/components/ui/textarea';
import { Tooltip, TooltipContent, TooltipTrigger } from '@/components/ui/tooltip';
import { focusFirstControl } from '@/lib/dialog-focus';
import { feedbackSuccess } from '@/lib/feedback';
import { daysUntil, fmtTime } from '@/lib/format';
import { qk } from '@/lib/query-keys';
import { cn } from '@/lib/utils';

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

/** 到期倒计时:<7 天红、≤30 天橙(证书页核心信息,过期单独提示) + 进度条可视化。 */
function ExpiryCell({ notAfter }: { notAfter: null | number }) {
  const days = daysUntil(notAfter);
  if (days === null || notAfter === null) {
    return <span className="text-muted-foreground">-</span>;
  }
  const expired = days < 0;
  const critical = expired || days < 7;
  const warning = !critical && days <= 30;
  const tone = critical
    ? 'border-destructive/40 bg-destructive/15 text-destructive'
    : warning
      ? 'border-amber-500/40 bg-amber-500/15 text-amber-700 dark:text-amber-300'
      : 'border-emerald-500/35 bg-emerald-500/10 text-emerald-700 dark:text-emerald-300';
  const barColor = critical ? 'bg-destructive' : warning ? 'bg-amber-500' : 'bg-emerald-500';
  const pct = expired ? 100 : Math.max(6, Math.min(100, Math.round((days / 90) * 100)));

  return (
    <div className="flex min-w-36 flex-col items-start gap-1">
      <div className="flex items-center gap-2">
        <Badge variant="outline" className={cn('rounded-full px-2 py-0 text-[11px]', tone)}>
          {expired ? '已过期' : `剩余 ${days} 天`}
        </Badge>
        <Mono className="text-[11px] text-muted-foreground">{fmtTime(notAfter)}</Mono>
      </div>
      <div className="h-1 w-28 overflow-hidden rounded-full bg-muted">
        <div
          className={cn('h-full rounded-full transition-all', barColor)}
          style={{ width: `${pct}%` }}
        />
      </div>
    </div>
  );
}

/** 域名列:SAN 列表截断展示(最多 3 个 + 其余数量,悬停看全量)。 */
function DomainsCell({ domains }: { domains: string[] }) {
  if (!domains?.length) {
    return <span className="text-muted-foreground">-</span>;
  }
  return (
    <div className="flex flex-wrap items-center gap-1">
      {domains.slice(0, 3).map((d) => (
        <Badge
          key={d}
          variant="outline"
          className="border-primary/25 bg-primary/8 font-mono text-[11px] text-primary"
        >
          {d}
        </Badge>
      ))}
      {domains.length > 3 ? (
        <Tooltip>
          <TooltipTrigger asChild>
            <Badge variant="outline" className="text-[11px]">
              +{domains.length - 3}
            </Badge>
          </TooltipTrigger>
          <TooltipContent className="max-w-96 font-mono text-xs">
            {domains.slice(3).join(', ')}
          </TooltipContent>
        </Tooltip>
      ) : null}
    </div>
  );
}

/**
 * 证书保险箱(TLS Certificates Vault):按 SNI 自动匹配、服务端终结 TLS 的 HTTPS 证书。
 */
export default function CertsPage() {
  const queryClient = useQueryClient();
  const [open, setOpen] = useState(false);
  const [editing, setEditing] = useState<CertRow | null>(null);
  const [keyVisible, setKeyVisible] = useState(false);
  const [searchQuery, setSearchQuery] = useState('');
  const [dragOver, setDragOver] = useState(false);
  const fileInputRef = useRef<HTMLInputElement | null>(null);

  const form = useForm<CertFormValues>({
    resolver: zodResolver(certSchema),
    defaultValues: EMPTY_FORM,
  });

  const watchedCertPem = useWatch({ control: form.control, name: 'cert_pem' });
  const watchedKeyPem = useWatch({ control: form.control, name: 'key_pem' });
  const certPemValid = looksLikeCertPem(watchedCertPem || '');
  const keyPemValid = looksLikePrivateKeyPem(watchedKeyPem || '');

  const certsQ = useQuery({ queryKey: qk.certs, queryFn: listCerts, retry: false });

  const errorMessage = certsQ.error instanceof Error ? certsQ.error.message : '';
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
    form.reset({
      name: row.name,
      domains: (row.domains ?? []).join(','),
      cert_pem: '',
      key_pem: '',
    });
    setOpen(true);
  }

  async function ingestFileList(files: FileList | null) {
    if (!files || files.length === 0) return;
    for (const file of Array.from(files)) {
      const text = await file.text();
      if (file.name.includes('key') || looksLikePrivateKeyPem(text)) {
        form.setValue('key_pem', text.trim(), { shouldValidate: true, shouldDirty: true });
      } else {
        form.setValue('cert_pem', text.trim(), { shouldValidate: true, shouldDirty: true });
      }
    }
  }

  async function handleFile(event: ChangeEvent<HTMLInputElement>) {
    await ingestFileList(event.target.files);
    event.target.value = '';
  }

  async function handleDrop(event: DragEvent<HTMLDivElement>) {
    event.preventDefault();
    setDragOver(false);
    await ingestFileList(event.dataTransfer.files);
  }

  const allCerts = useMemo(() => certsQ.data ?? [], [certsQ.data]);
  const summaryStats = useMemo(() => {
    let valid = 0;
    let expiring = 0;
    for (const c of allCerts) {
      const d = daysUntil(c.not_after);
      if (d !== null && d < 0) {
        expiring += 1;
      } else if (d !== null && d <= 30) {
        expiring += 1;
        valid += 1;
      } else {
        valid += 1;
      }
    }
    return { total: allCerts.length, valid, expiring };
  }, [allCerts]);

  const filteredCerts = useMemo(() => {
    const q = searchQuery.trim().toLowerCase();
    if (!q) return allCerts;
    return allCerts.filter(
      (c) =>
        c.name.toLowerCase().includes(q) ||
        (c.domains ?? []).some((d) => d.toLowerCase().includes(q)) ||
        (c.fingerprint ?? '').toLowerCase().includes(q),
    );
  }, [allCerts, searchQuery]);

  const columns: RhColumnDef<CertRow>[] = [
    {
      accessorKey: 'name',
      meta: { label: '证书名称' },
      header: ({ column }) => <DataTableColumnHeader column={column} title="证书名称" />,
      cell: ({ row }) => (
        <div className="flex items-center gap-2">
          <div className="flex size-7 items-center justify-center rounded-lg border border-primary/20 bg-primary/10 text-primary">
            <Lock className="size-3.5" />
          </div>
          <span className="font-semibold">{row.original.name}</span>
        </div>
      ),
    },
    {
      id: 'domains',
      meta: { label: 'SNI 域名 (SAN)' },
      header: 'SNI 域名 (SAN)',
      enableSorting: false,
      cell: ({ row }) => <DomainsCell domains={row.original.domains ?? []} />,
    },
    {
      id: 'not_after',
      meta: { label: '有效期与倒计时' },
      accessorFn: (row) => row.not_after ?? 0,
      header: ({ column }) => <DataTableColumnHeader column={column} title="有效期与倒计时" />,
      cell: ({ row }) => <ExpiryCell notAfter={row.original.not_after} />,
    },
    {
      accessorKey: 'fingerprint',
      meta: { label: 'SHA-256 指纹' },
      header: ({ column }) => <DataTableColumnHeader column={column} title="SHA-256 指纹" />,
      cell: ({ row }) =>
        row.original.fingerprint ? (
          <MonoCopy
            ariaLabel="复制证书指纹"
            display={`${row.original.fingerprint.slice(0, 12)}…`}
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
            更新凭据
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
                // 失败已由请求拦截器统一提示
              }
            }}
          />
        </div>
      ),
    },
  ];

  return (
    <div className="space-y-5">
      <PageHeader
        eyebrow="SNI TLS TERMINATION VAULT · HTTPS 证书保险箱"
        title="TLS 证书保险箱"
        description="按 SNI 自动匹配、由服务端统一终结 TLS 的 HTTPS 证书与私钥管理"
        actions={
          <Button onClick={openCreate}>
            <Plus />
            上传证书
          </Button>
        }
      />

      {/* 顶部证书健康摘要卡 */}
      <div className="grid gap-3.5 sm:grid-cols-3">
        <div className="rh-telemetry-card flex items-center justify-between rounded-2xl p-4 sm:p-5">
          <div className="space-y-1">
            <div className="text-xs font-medium text-muted-foreground">托管证书总数</div>
            <div className="font-mono text-2xl font-bold tracking-tight tabular-nums">
              {summaryStats.total}
            </div>
            <div className="text-[11px] text-muted-foreground">按 SNI 域名自动匹配终结 TLS</div>
          </div>
          <div className="flex size-10 shrink-0 items-center justify-center rounded-xl border border-primary/20 bg-primary/10 text-primary">
            <ShieldCheck className="size-5" />
          </div>
        </div>
        <div className="rh-telemetry-card flex items-center justify-between rounded-2xl p-4 sm:p-5">
          <div className="space-y-1">
            <div className="text-xs font-medium text-muted-foreground">有效期内证书</div>
            <div className="font-mono text-2xl font-bold tracking-tight text-emerald-600 tabular-nums dark:text-emerald-400">
              {summaryStats.valid}
            </div>
            <div className="text-[11px] text-muted-foreground">证书链与私钥校验正常</div>
          </div>
          <div className="flex size-10 shrink-0 items-center justify-center rounded-xl border border-emerald-500/25 bg-emerald-500/10 text-emerald-500">
            <CheckCircle2 className="size-5" />
          </div>
        </div>
        <div className="rh-telemetry-card flex items-center justify-between rounded-2xl p-4 sm:p-5">
          <div className="space-y-1">
            <div className="text-xs font-medium text-muted-foreground">30天内到期 / 已过期</div>
            <div
              className={cn(
                'font-mono text-2xl font-bold tracking-tight tabular-nums',
                summaryStats.expiring > 0
                  ? 'text-amber-600 dark:text-amber-400'
                  : 'text-foreground',
              )}
            >
              {summaryStats.expiring}
            </div>
            <div className="text-[11px] text-muted-foreground">
              {summaryStats.expiring > 0 ? '请及时轮换即将到期证书' : '暂无近期到期风险'}
            </div>
          </div>
          <div
            className={cn(
              'flex size-10 shrink-0 items-center justify-center rounded-xl border',
              summaryStats.expiring > 0
                ? 'border-amber-500/30 bg-amber-500/10 text-amber-500'
                : 'border-border/70 bg-muted/50 text-muted-foreground',
            )}
          >
            {summaryStats.expiring > 0 ? (
              <ShieldAlert className="size-5" />
            ) : (
              <Clock className="size-5" />
            )}
          </div>
        </div>
      </div>

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

      <div className="rh-telemetry-card rounded-2xl p-4 sm:p-5">
        <div className="mb-4 flex flex-wrap items-center justify-between gap-3">
          <div className="relative min-w-60 flex-1 sm:max-w-xs">
            <Search className="pointer-events-none absolute top-1/2 left-2.5 size-3.5 -translate-y-1/2 text-muted-foreground" />
            <Input
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              placeholder="搜索证书名称、域名或指纹…"
              className="h-8 pl-8 text-xs"
            />
          </div>
          <div className="text-xs text-muted-foreground">
            支持通配符域名（如 <code className="font-mono text-foreground">*.example.com</code>
            ），HTTPS 入站请求按 SNI 自动匹配
          </div>
        </div>

        <DataTable
          columns={columns}
          data={filteredCerts}
          loading={certsQ.isLoading}
          getRowId={(row) => String(row.id)}
          initialPageSize={50}
          mobileRow={(row) => (
            <div className="space-y-2.5">
              <div className="flex items-start justify-between gap-2">
                <div className="flex min-w-0 items-center gap-2">
                  <div className="flex size-7 shrink-0 items-center justify-center rounded-lg border border-primary/20 bg-primary/10 text-primary">
                    <Lock className="size-3.5" />
                  </div>
                  <span className="truncate font-semibold">{row.name}</span>
                </div>
                <ExpiryCell notAfter={row.not_after} />
              </div>
              <DomainsCell domains={row.domains ?? []} />
              <div className="flex flex-wrap items-center justify-between gap-2 border-t border-border/60 pt-2 text-xs">
                <div className="flex items-center gap-1 text-muted-foreground">
                  <span>指纹:</span>
                  {row.fingerprint ? (
                    <MonoCopy
                      ariaLabel="复制证书指纹"
                      display={`${row.fingerprint.slice(0, 10)}…`}
                      full={row.fingerprint}
                    />
                  ) : (
                    <span>-</span>
                  )}
                </div>
                <div className="flex items-center gap-1">
                  <Button variant="ghost" size="sm" onClick={() => openEdit(row)}>
                    更新凭据
                  </Button>
                  <ConfirmDialog
                    destructive
                    confirmText="删除"
                    title={`删除证书 ${row.name}?`}
                    description="绑定该域名的 HTTPS 转发规则将回退为端到端透传。"
                    trigger={
                      <Button
                        variant="ghost"
                        size="sm"
                        className="text-destructive hover:text-destructive"
                      >
                        删除
                      </Button>
                    }
                    onConfirm={async () => {
                      try {
                        await deleteMut.mutateAsync(row.id);
                      } catch {
                        // 失败已由请求拦截器统一提示
                      }
                    }}
                  />
                </div>
              </div>
            </div>
          )}
          empty={
            <EmptyState
              icon={<ShieldCheck />}
              title={searchQuery ? '未找到匹配的证书' : '还没有证书'}
              description="上传 PEM 证书后,HTTPS 转发规则将按 SNI 自动匹配并由服务端终结 TLS。"
              action={<Button onClick={openCreate}>上传证书</Button>}
            />
          }
        />
      </div>

      <Sheet
        open={open}
        onOpenChange={(next) => {
          setOpen(next);
          if (!next) setEditing(null);
        }}
      >
        <SheetContent
          side="right"
          className="flex w-full flex-col gap-0 p-0 sm:max-w-xl"
          onOpenAutoFocus={focusFirstControl}
        >
          <SheetHeader className="border-b border-border/80 bg-muted/30 px-4 py-3.5 pr-11 sm:px-6 sm:py-4">
            <div className="flex items-center gap-2">
              <Badge
                variant="outline"
                className="border-primary/30 bg-primary/10 font-mono text-[11px] text-primary"
              >
                SNI TLS VAULT
              </Badge>
              <SheetTitle>{editing ? `编辑证书 #${editing.id}` : '上传证书'}</SheetTitle>
            </div>
            <SheetDescription className="text-xs sm:text-sm">
              证书与服务端终结 TLS 的 HTTPS 转发规则配套;私钥仅在提交时上传,任何响应都不回显。
            </SheetDescription>
          </SheetHeader>

          <div className="flex-1 overflow-y-auto px-4 py-4 sm:px-6 sm:py-5">
            {/* 拖拽/选择文件快速导入区 */}
            <div
              onDragOver={(e) => {
                e.preventDefault();
                setDragOver(true);
              }}
              onDragLeave={() => setDragOver(false)}
              onDrop={(e) => void handleDrop(e)}
              onClick={() => fileInputRef.current?.click()}
              className={cn(
                'mb-4 flex cursor-pointer flex-col items-start justify-between gap-3 rounded-xl border border-dashed p-3.5 transition-colors sm:flex-row sm:items-center',
                dragOver
                  ? 'border-primary bg-primary/10'
                  : 'border-border/80 bg-muted/30 hover:border-primary/40 hover:bg-muted/50',
              )}
            >
              <div className="flex items-center gap-3">
                <div className="flex size-9 shrink-0 items-center justify-center rounded-lg border border-primary/20 bg-primary/10 text-primary">
                  <FileKey2 className="size-4" />
                </div>
                <div>
                  <div className="text-xs font-semibold">
                    拖拽或点击选择 <code className="font-mono">.pem / .crt / .key</code>{' '}
                    文件自动填充
                  </div>
                  <div className="mt-0.5 text-[11px] text-muted-foreground">
                    支持一次选择多个文件，按文件名或 PEM 头自动识别证书链与私钥
                  </div>
                </div>
              </div>
              <input
                ref={fileInputRef}
                type="file"
                multiple
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
                onClick={(e) => {
                  e.stopPropagation();
                  fileInputRef.current?.click();
                }}
              >
                <Upload className="size-3.5" />
                选择文件
              </Button>
            </div>

            <Form {...form}>
              <form
                id="cert-form"
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
                      <div className="flex items-center justify-between gap-2">
                        <FormLabel>
                          <InfoLabel title="证书 PEM" infoKey="cert.cert_pem" />
                        </FormLabel>
                        {certPemValid ? (
                          <span className="inline-flex items-center gap-1 text-[11px] font-medium text-emerald-600 dark:text-emerald-400">
                            <CheckCircle2 className="size-3" />
                            已识别 CERTIFICATE
                          </span>
                        ) : null}
                      </div>
                      <FormControl>
                        <Textarea
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
                        <div className="flex items-center gap-2">
                          {keyPemValid ? (
                            <span className="inline-flex items-center gap-1 text-[11px] font-medium text-emerald-600 dark:text-emerald-400">
                              <CheckCircle2 className="size-3" />
                              已识别 PRIVATE KEY
                            </span>
                          ) : null}
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
              </form>
            </Form>
          </div>

          <SheetFooter className="flex-row items-center justify-end gap-2 border-t border-border/80 bg-card/90 px-4 py-3.5 sm:px-6 sm:py-4">
            <Button type="button" variant="outline" onClick={() => setOpen(false)}>
              取消
            </Button>
            <Button type="submit" form="cert-form" disabled={saveMut.isPending}>
              {saveMut.isPending ? '保存中…' : '保存'}
            </Button>
          </SheetFooter>
        </SheetContent>
      </Sheet>
    </div>
  );
}
