/**
 * 主题模式(亮色 / 暗色 / 跟随系统)。
 *
 * 刻意不用 zustand persist:index.html 的首帧内联脚本需要在 React 之前读到
 * 裸字符串值来决定是否加 `.dark`(避免亮暗闪烁),persist 的 JSON 包装会破坏它。
 * 因此 localStorage 里存的就是 `light` / `dark` / `system` 原值。
 */
import { useSyncExternalStore } from 'react';

export type ThemeMode = 'light' | 'dark' | 'system';

const STORAGE_KEY = 'rabbithole-console-theme';

function readStored(): ThemeMode {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    if (raw === 'light' || raw === 'dark' || raw === 'system') {
      return raw;
    }
  } catch {
    /* localStorage 不可用(隐私模式)时按跟随系统 */
  }
  return 'system';
}

function systemPrefersDark(): boolean {
  return window.matchMedia('(prefers-color-scheme: dark)').matches;
}

export function resolveDark(mode: ThemeMode): boolean {
  return mode === 'dark' || (mode === 'system' && systemPrefersDark());
}

let mode: ThemeMode = readStored();
const listeners = new Set<() => void>();

function applyToDocument(): void {
  document.documentElement.classList.toggle('dark', resolveDark(mode));
}

function emit(): void {
  for (const listener of listeners) {
    listener();
  }
}

function subscribe(listener: () => void): () => void {
  listeners.add(listener);
  return () => {
    listeners.delete(listener);
  };
}

export function setTheme(next: ThemeMode): void {
  mode = next;
  try {
    localStorage.setItem(STORAGE_KEY, next);
  } catch {
    /* 存不下不影响本次会话 */
  }
  applyToDocument();
  emit();
}

/** 在系统主题变化时同步(仅 mode === 'system' 时影响实际观感)。 */
export function watchSystemTheme(): () => void {
  const mql = window.matchMedia('(prefers-color-scheme: dark)');
  const onChange = () => {
    if (mode === 'system') {
      applyToDocument();
      emit();
    }
  };
  mql.addEventListener('change', onChange);
  return () => mql.removeEventListener('change', onChange);
}

export function useThemeMode(): ThemeMode {
  return useSyncExternalStore(
    subscribe,
    () => mode,
    () => 'system' as ThemeMode,
  );
}

export function useIsDark(): boolean {
  const current = useThemeMode();
  return resolveDark(current);
}
