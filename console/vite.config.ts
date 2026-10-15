import { readFileSync } from 'node:fs';
import path from 'node:path';
import tailwindcss from '@tailwindcss/vite';
import react from '@vitejs/plugin-react';
import { defineConfig } from 'vite';

const pkg = JSON.parse(readFileSync(new URL('./package.json', import.meta.url), 'utf8')) as {
  version: string;
};

/**
 * 控制台构建配置。
 *
 * - 产物固定落在 `console/dist`:`crates/rh-server/build.rs` 只检测该路径,
 *   release.yml 亦按此目录上传工件。
 * - 不产出 sourcemap(内嵌分发,体积与源码暴露都不需要)。
 * - 开发期 `/api` 原样代理到本机 rh-server(无 rewrite);生产同源托管。
 */
export default defineConfig({
  plugins: [react(), tailwindcss()],
  resolve: {
    alias: {
      '@': path.resolve(import.meta.dirname, 'src'),
    },
  },
  define: {
    __APP_VERSION__: JSON.stringify(pkg.version),
  },
  server: {
    port: 8000,
    proxy: {
      '/api': {
        target: 'http://127.0.0.1:8080',
        changeOrigin: false,
      },
    },
  },
  build: {
    outDir: 'dist',
    emptyOutDir: true,
    sourcemap: false,
    chunkSizeWarningLimit: 900,
    rollupOptions: {
      /**
       * zod 4.6.6 的产物里有两处 `@__PURE__` 注释的位置 Rollup 无法解析(第三方依赖,
       * 与本工程代码无关;Rollup 会去掉这些注释后继续,不影响产物)。
       * 只静音这一种来源为依赖的警告,其余警告照常输出。
       */
      onwarn(warning, defaultHandler) {
        const fromDependency = warning.id?.includes('node_modules') ?? false;
        if (warning.code === 'INVALID_ANNOTATION' && fromDependency) {
          return;
        }
        defaultHandler(warning);
      },
    },
  },
});
