# Data Workspace

数据分析工作空间 - 支持工作流编辑和报告生成的数据平台。

## 项目结构

```
src/
├── api/                    # 后端 API 层 (插拔式设计)
│   ├── index.ts           # 统一导出
│   ├── client.ts          # API 客户端
│   ├── types.ts           # API 类型定义
│   ├── tables.ts          # 表格 CRUD API
│   ├── artifacts.ts       # 产物 API
│   ├── workflows.ts       # 工作流 API
│   └── ai.ts              # AI 功能 API
├── components/
│   ├── ui/                # Shadcn UI 组件
│   ├── workspace/         # 工作空间组件
│   └── workflow/          # 工作流组件
├── store/                 # Zustand 状态管理
└── pages/                 # 页面组件
```

## API 端点

| 模块 | 端点 | 描述 |
|------|------|------|
| Tables | `/tables` | 表格 CRUD |
| Artifacts | `/artifacts` | 洞察/图表产物 |
| Workflows | `/workflows/:id/execute` | 工作流执行 |
| AI | `/ai/insights` | AI 洞察生成 |

## 开发

```bash
npm install
npm run dev
```

## 技术栈

React 18 + TypeScript + Vite + Tailwind CSS + Zustand + Supabase
