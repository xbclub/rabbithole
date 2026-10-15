import { StrictMode } from 'react';
import { createRoot } from 'react-dom/client';

import { App } from '@/App';
import './index.css';

const container = document.getElementById('root');
if (!container) {
  throw new Error('#root 容器缺失');
}

createRoot(container).render(
  <StrictMode>
    <App />
  </StrictMode>,
);

// #rh-boot 的移除在 App 的 effect 里做(React 首次提交之后),
// 这里立刻移除会露出一帧空白
