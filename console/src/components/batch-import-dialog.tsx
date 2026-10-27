import { useQueryClient } from '@tanstack/react-query';
import { useEffect, useState } from 'react';

import { createService } from '@/api/rabbithole';
import { Button } from '@/components/ui/button';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog';
import { Textarea } from '@/components/ui/textarea';
import { focusFirstControl } from '@/lib/dialog-focus';
import { feedbackError } from '@/lib/feedback';

/**
 * 批量导入规则弹窗(M7:从 proxy-list-page 抽出)。
 *
 * batchText/batchResults/batchRunning 三态内聚在此:修前它们挂在列表页
 * 顶层,每键入一个字符整页(含全量表格)重渲染。抽出后打字只重渲染
 * 本弹窗;父组件仅在 open 布尔翻转时重渲染。
 */
export function BatchImportDialog({
  open,
  onOpenChange,
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
}) {
  const queryClient = useQueryClient();
  const [batchText, setBatchText] = useState('');
  const [batchResults, setBatchResults] = useState<null | Array<{ name: string; msg: string }>>(
    null,
  );
  const [batchRunning, setBatchRunning] = useState(false);

  // 打开时清掉上一轮结果(文本保留,与抽出前行为一致)
  useEffect(() => {
    if (open) setBatchResults(null);
  }, [open]);

  async function runBatchImport() {
    let parsed: unknown;
    try {
      parsed = JSON.parse(batchText);
    } catch {
      feedbackError('不是合法的 JSON 数组');
      return;
    }
    if (!Array.isArray(parsed)) {
      feedbackError('不是合法的 JSON 数组');
      return;
    }
    setBatchRunning(true);
    const results: Array<{ name: string; msg: string }> = [];
    for (const item of parsed as Array<Record<string, unknown>>) {
      const name = typeof item.name === 'string' ? item.name : '?';
      try {
        await createService(item);
        results.push({ name, msg: '成功' });
      } catch (error) {
        results.push({ name, msg: error instanceof Error ? error.message : '失败' });
      }
    }
    setBatchResults(results);
    setBatchRunning(false);
    // ['services'] 前缀失效:与抽出前父页的 invalidate 口径一致
    // (覆盖按 client_id 参数化的各 services 查询,不只本页列表)
    queryClient.invalidateQueries({ queryKey: ['services'] });
  }

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-2xl" onOpenAutoFocus={focusFirstControl}>
        <DialogHeader>
          <DialogTitle>批量导入规则</DialogTitle>
          <DialogDescription>
            粘贴规则 JSON 数组(与单条创建载荷一致),逐条创建并回报结果。
          </DialogDescription>
        </DialogHeader>
        <Textarea
          value={batchText}
          onChange={(event) => setBatchText(event.target.value)}
          rows={10}
          className="rh-mono text-xs"
          placeholder='[{"client_id":"…","name":"web","kind":"tcp","bind_port":0,"target_host":"10.0.0.1","target_port":80}]'
          aria-label="规则 JSON 数组"
        />
        {batchResults ? (
          <div className="max-h-40 space-y-1 overflow-y-auto rounded-md border p-2 text-xs">
            {batchResults.map((item, index) => (
              <div key={index} className="flex items-center gap-2">
                <span className="rh-mono">{item.name}</span>
                <span className={item.msg === '成功' ? 'text-emerald-600' : 'text-destructive'}>
                  {item.msg}
                </span>
              </div>
            ))}
          </div>
        ) : null}
        <DialogFooter>
          <Button variant="outline" onClick={() => onOpenChange(false)}>
            关闭
          </Button>
          <Button
            onClick={() => void runBatchImport()}
            disabled={batchRunning || !batchText.trim()}
          >
            {batchRunning ? '导入中…' : '开始导入'}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
