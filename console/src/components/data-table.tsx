import {
  flexRender,
  getCoreRowModel,
  getPaginationRowModel,
  getSortedRowModel,
  useReactTable,
} from '@tanstack/react-table';
import type {
  Column,
  ColumnDef,
  RowSelectionState,
  SortingState,
  VisibilityState,
} from '@tanstack/react-table';
import { ArrowDown, ArrowUp, ChevronsUpDown, Columns3 } from 'lucide-react';
import { useEffect, useMemo, useState } from 'react';

/**
 * 本表格封装在 TanStack `ColumnDef.meta` 上追加的字段。
 *
 * 不能用模块增强:`ColumnMeta` 是空接口且声明在 `@tanstack/table-core`
 * (react-table 只是再导出,该包在本工程不可直接解析),增强 react-table 不会
 * 作用到 `ColumnDef.meta`。因此这里显式声明自己的 meta 类型与列定义类型,
 * 调用点用 `RhColumnDef<T>` 声明列表即可获得类型检查,无需断言。
 */
export interface RhColumnMeta {
  /** 「列」下拉里显示的中文名;不填则退回字符串 header,再退回列 id。 */
  label?: string;
  /**
   * 加到该列表头 th 上的类名。自动布局下 `w-[1%]` 可让该列按内容收紧,
   * 避免表格 w-full 把富余宽度摊给操作列(按钮右对齐后左边会留大片空白)。
   */
  headerClassName?: string;
}

/**
 * 分配式 Omit:ColumnDef 是联合类型,直接 `Omit<ColumnDef, 'meta'>` 会把联合塌成
 * 单个对象类型,`accessorKey` / `accessorFn` 等成员随之丢失。
 */
type DistributiveOmit<T, K extends PropertyKey> = T extends unknown ? Omit<T, K> : never;

/** 列表页使用的列定义类型(在 ColumnDef 上补 RhColumnMeta)。 */
export type RhColumnDef<TData> = DistributiveOmit<ColumnDef<TData, unknown>, 'meta'> & {
  meta?: RhColumnMeta;
};

/** 读取列上的 meta(列定义来自 TanStack,meta 为未知形状,这里收口一次转换)。 */
function rhMeta<TData>(column: Column<TData, unknown>): RhColumnMeta {
  return (column.columnDef.meta ?? {}) as RhColumnMeta;
}

import { Button } from '@/components/ui/button';
import { Checkbox } from '@/components/ui/checkbox';
import {
  DropdownMenu,
  DropdownMenuCheckboxItem,
  DropdownMenuContent,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu';
import { Skeleton } from '@/components/ui/skeleton';
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table';
import { useIsMobile } from '@/hooks/use-mobile';
import { cn } from '@/lib/utils';

/** 「列」下拉里的列名:优先 meta.label(中文),否则字符串 header,最后退回列 id。 */
function columnLabel<TData>(column: Column<TData, unknown>): string {
  const label = rhMeta(column).label;
  if (label) {
    return label;
  }
  if (typeof column.columnDef.header === 'string') {
    return column.columnDef.header;
  }
  return column.id;
}

/** 可排序列头(升/降/取消三态)。 */
export function DataTableColumnHeader<TData, TValue>({
  column,
  title,
  className,
}: {
  column: Column<TData, TValue>;
  title: React.ReactNode;
  className?: string;
}) {
  if (!column.getCanSort()) {
    return <div className={className}>{title}</div>;
  }
  const sorted = column.getIsSorted();
  return (
    <Button
      type="button"
      variant="ghost"
      size="sm"
      onClick={() => column.toggleSorting(sorted === 'asc')}
      className={cn('-ml-2 h-8 gap-1 px-2 font-medium data-[state=open]:bg-accent', className)}
      aria-label={typeof title === 'string' ? `按${title}排序` : '排序'}
    >
      {title}
      {sorted === 'asc' ? (
        <ArrowUp className="size-3.5" />
      ) : sorted === 'desc' ? (
        <ArrowDown className="size-3.5" />
      ) : (
        <ChevronsUpDown className="size-3.5 text-muted-foreground" />
      )}
    </Button>
  );
}

export interface DataTableProps<TData> {
  columns: RhColumnDef<TData>[];
  data: TData[];
  loading?: boolean;
  /** 无数据时展示(空态三件套);缺省给一句朴素占位。 */
  empty?: React.ReactNode;
  /** 工具栏(新建/刷新/批量等),渲染在表格上方右侧。 */
  toolbar?: React.ReactNode;
  /**
   * <768px 时把每行渲染成卡片(文档 §4.6 表格降级为卡片列表)。
   * 未提供时小屏改为横向滚动。
   */
  mobileRow?: (row: TData) => React.ReactNode;
  initialPageSize?: number;
  getRowId?: (row: TData) => string;
  /** 开启行选择(批量启停删用),选中行经 onSelectedRowsChange 回调。 */
  enableRowSelection?: boolean;
  onSelectedRowsChange?: (rows: TData[]) => void;
  className?: string;
}

/**
 * 列表页通用表格:客户端排序 + 分页 + 列可见性 + 加载骨架 + 空态插槽。
 * 数据源是后端一次性返回的全量列表,故分页/排序都在客户端做。
 */
export function DataTable<TData>({
  columns,
  data,
  loading = false,
  empty,
  toolbar,
  mobileRow,
  initialPageSize = 20,
  getRowId,
  enableRowSelection = false,
  onSelectedRowsChange,
  className,
}: DataTableProps<TData>) {
  const isMobile = useIsMobile();
  const [sorting, setSorting] = useState<SortingState>([]);
  const [columnVisibility, setColumnVisibility] = useState<VisibilityState>({});
  const [rowSelection, setRowSelection] = useState<RowSelectionState>({});

  const selectionColumn = useMemo<ColumnDef<TData, unknown>>(
    () => ({
      id: 'select',
      header: ({ table }) => (
        <Checkbox
          checked={
            table.getIsAllPageRowsSelected() ||
            (table.getIsSomePageRowsSelected() && 'indeterminate')
          }
          onCheckedChange={(value) => table.toggleAllPageRowsSelected(!!value)}
          aria-label="全选本页"
        />
      ),
      cell: ({ row }) => (
        <Checkbox
          checked={row.getIsSelected()}
          disabled={!row.getCanSelect()}
          onCheckedChange={(value) => row.toggleSelected(!!value)}
          aria-label="选择该行"
        />
      ),
      enableSorting: false,
      enableHiding: false,
      meta: { headerClassName: 'w-[1%]' },
    }),
    [],
  );

  const table = useReactTable({
    data,
    // 收口一次转换:RhColumnDef 只是给 meta 补了类型,运行时形状就是 ColumnDef
    columns: (enableRowSelection ? [selectionColumn, ...columns] : columns) as ColumnDef<
      TData,
      unknown
    >[],
    state: { sorting, columnVisibility, rowSelection },
    onSortingChange: setSorting,
    onColumnVisibilityChange: setColumnVisibility,
    onRowSelectionChange: setRowSelection,
    enableRowSelection,
    getCoreRowModel: getCoreRowModel(),
    getSortedRowModel: getSortedRowModel(),
    getPaginationRowModel: getPaginationRowModel(),
    initialState: { pagination: { pageSize: initialPageSize } },
    ...(getRowId ? { getRowId } : {}),
  });

  /* eslint-disable react-hooks/exhaustive-deps -- 只在选中集变化时通知外层:
     回调常是内联箭头函数、table 实例每次渲染都是新的,纳入依赖会导致每渲染都回调。 */
  useEffect(() => {
    onSelectedRowsChange?.(table.getSelectedRowModel().rows.map((row) => row.original));
  }, [rowSelection]);
  /* eslint-enable react-hooks/exhaustive-deps */

  const rows = table.getRowModel().rows;
  const hideableColumns = table.getAllColumns().filter((column) => column.getCanHide());

  if (loading) {
    return (
      <div className={cn('space-y-2', className)}>
        <Skeleton className="h-9 w-full" />
        <Skeleton className="h-14 w-full" />
        <Skeleton className="h-14 w-full" />
        <Skeleton className="h-14 w-full" />
      </div>
    );
  }

  const toolbarNode =
    toolbar || hideableColumns.length > 0 ? (
      <div className="flex flex-wrap items-center justify-end gap-2">
        {toolbar}
        {hideableColumns.length > 0 ? (
          <DropdownMenu>
            <DropdownMenuTrigger asChild>
              <Button variant="outline" size="sm" className="gap-1.5">
                <Columns3 className="size-4" />列
              </Button>
            </DropdownMenuTrigger>
            <DropdownMenuContent align="end" className="w-44">
              <DropdownMenuLabel>显示的列</DropdownMenuLabel>
              <DropdownMenuSeparator />
              {hideableColumns.map((column) => (
                <DropdownMenuCheckboxItem
                  key={column.id}
                  checked={column.getIsVisible()}
                  onCheckedChange={(value) => column.toggleVisibility(!!value)}
                  onSelect={(event) => event.preventDefault()}
                >
                  {columnLabel(column)}
                </DropdownMenuCheckboxItem>
              ))}
            </DropdownMenuContent>
          </DropdownMenu>
        ) : null}
      </div>
    ) : null;

  const emptyNode = empty ?? (
    <div className="py-12 text-center text-sm text-muted-foreground">暂无数据</div>
  );

  return (
    <div className={cn('space-y-3', className)}>
      {toolbarNode}
      {rows.length === 0 ? (
        emptyNode
      ) : isMobile && mobileRow ? (
        <div className="space-y-3">
          {rows.map((row) => (
            <div key={row.id} className="rounded-lg border p-4">
              {mobileRow(row.original)}
            </div>
          ))}
        </div>
      ) : (
        <div className="overflow-x-auto rounded-lg border">
          <Table>
            <TableHeader>
              {table.getHeaderGroups().map((headerGroup) => (
                <TableRow key={headerGroup.id}>
                  {headerGroup.headers.map((header) => (
                    <TableHead
                      key={header.id}
                      className={cn('whitespace-nowrap', rhMeta(header.column).headerClassName)}
                    >
                      {header.isPlaceholder
                        ? null
                        : flexRender(header.column.columnDef.header, header.getContext())}
                    </TableHead>
                  ))}
                </TableRow>
              ))}
            </TableHeader>
            <TableBody>
              {rows.map((row) => (
                <TableRow key={row.id}>
                  {row.getVisibleCells().map((cell) => (
                    <TableCell key={cell.id}>
                      {flexRender(cell.column.columnDef.cell, cell.getContext())}
                    </TableCell>
                  ))}
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </div>
      )}
      {table.getPageCount() > 1 ? (
        <div className="flex items-center justify-between gap-2">
          <p className="text-xs text-muted-foreground">
            第 {table.getState().pagination.pageIndex + 1} / {table.getPageCount()} 页,共{' '}
            {data.length} 条
          </p>
          <div className="flex items-center gap-2">
            <Button
              variant="outline"
              size="sm"
              onClick={() => table.previousPage()}
              disabled={!table.getCanPreviousPage()}
            >
              上一页
            </Button>
            <Button
              variant="outline"
              size="sm"
              onClick={() => table.nextPage()}
              disabled={!table.getCanNextPage()}
            >
              下一页
            </Button>
          </div>
        </div>
      ) : null}
    </div>
  );
}
