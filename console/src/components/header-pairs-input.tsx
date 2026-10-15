import { Plus, Trash2 } from 'lucide-react';

import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import type { HeaderPairValue } from '@/features/services/types';

/**
 * HTTP 头键值对行编辑器(旧版 HeaderPairsInput 的 shadcn 版)。
 * 每一行的删除按钮都带 aria-label(旧版漏了,属可访问性缺陷)。
 */
export function HeaderPairsInput({
  value,
  onChange,
  disabled = false,
  namePlaceholder = 'Header 名',
  valuePlaceholder = 'Header 值',
}: {
  value: HeaderPairValue[];
  onChange: (next: HeaderPairValue[]) => void;
  disabled?: boolean;
  namePlaceholder?: string;
  valuePlaceholder?: string;
}) {
  const rows = Array.isArray(value) ? value : [];

  const update = (index: number, patch: Partial<HeaderPairValue>) => {
    onChange(rows.map((row, i) => (i === index ? { ...row, ...patch } : row)));
  };

  return (
    <div className="space-y-2">
      {rows.length === 0 ? (
        <p className="text-xs text-muted-foreground">还没有条目。</p>
      ) : (
        rows.map((row, index) => (
          <div key={index} className="flex items-center gap-2">
            <Input
              value={row.name}
              disabled={disabled}
              placeholder={namePlaceholder}
              aria-label={`第 ${index + 1} 行 Header 名`}
              onChange={(event) => update(index, { name: event.target.value })}
            />
            <Input
              value={row.value}
              disabled={disabled}
              placeholder={valuePlaceholder}
              aria-label={`第 ${index + 1} 行 Header 值`}
              onChange={(event) => update(index, { value: event.target.value })}
            />
            <Button
              type="button"
              variant="ghost"
              size="icon"
              disabled={disabled}
              aria-label={`删除第 ${index + 1} 行`}
              onClick={() => onChange(rows.filter((_, i) => i !== index))}
            >
              <Trash2 className="size-4" />
            </Button>
          </div>
        ))
      )}
      <Button
        type="button"
        variant="outline"
        size="sm"
        disabled={disabled}
        onClick={() => onChange([...rows, { name: '', value: '' }])}
      >
        <Plus className="size-4" />
        添加一行
      </Button>
    </div>
  );
}
