/**
 * 弹窗打开时的焦点落点。
 *
 * Radix Dialog 默认聚焦内容里**第一个可聚焦元素**;表单里那个元素通常是字段名
 * 旁边的 ℹ 按钮,于是弹窗一打开就弹出第一个字段的说明浮层(Radix Tooltip 会在
 * focus 时打开)。改为聚焦第一个真实控件,顺带也更符合"打开表单直接开填"的预期。
 *
 * 用法:`<DialogContent onOpenAutoFocus={focusFirstControl}>`
 */
/**
 * 按优先级依次尝试的候选控件。
 *
 * 注意不能把它们拼成一个选择器列表交给 querySelector:那返回的是**文档顺序**里
 * 第一个匹配项,而字段名旁的 ℹ 按钮排在输入框之前,会把焦点抢走(于是 Tooltip
 * 又被 focus 打开)。必须按优先级分次查询。
 */
const CONTROL_SELECTORS = [
  'input:not([type="hidden"]):not([disabled])',
  'textarea:not([disabled])',
  '[data-slot="select-trigger"]:not([disabled])',
];

export function focusFirstControl(event: Event): void {
  event.preventDefault();
  // onOpenAutoFocus 的事件目标是内容元素本身;取不到时退回整个文档
  const root = (event.target as HTMLElement | null) ?? document;
  for (const selector of CONTROL_SELECTORS) {
    const control = root.querySelector<HTMLElement>(selector);
    if (control) {
      control.focus();
      return;
    }
  }
}
