/**
 * Workflow Types - 可视化 AI 工作流的类型定义
 * 支持可插拔节点类型和后端集成
 */

// 节点类型 - 可扩展
export type WorkflowNodeType = 
  | "data_source"      // 数据源节点
  | "transform"        // 数据转换
  | "ai_analysis"      // AI 分析
  | "chart_generator"  // 图表生成
  | "insight_extractor" // 洞察提取
  | "output"           // 输出节点
  | "custom";          // 自定义节点

// 节点执行状态
export type NodeExecutionStatus = 
  | "idle"       // 未执行
  | "pending"    // 等待执行
  | "running"    // 执行中
  | "success"    // 成功
  | "error"      // 失败
  | "skipped";   // 跳过

// 工作流整体状态
export type WorkflowStatus = 
  | "draft"      // 草稿
  | "ready"      // 就绪
  | "running"    // 运行中
  | "completed"  // 已完成
  | "failed";    // 失败

// 端口定义
export interface WorkflowPort {
  id: string;
  name: string;
  type: "input" | "output";
  dataType?: string; // 数据类型约束
  required?: boolean;
}

// 节点配置 - 可扩展的表单配置
export interface NodeConfig {
  [key: string]: unknown;
}

// 工作流节点
export interface WorkflowNode {
  id: string;
  type: WorkflowNodeType;
  name: string;
  description?: string;
  position: { x: number; y: number };
  ports: WorkflowPort[];
  config: NodeConfig;
  status: NodeExecutionStatus;
  error?: string;
  output?: unknown; // 节点输出数据
  executedAt?: string;
  duration?: number; // 执行耗时 ms
}

// 连接边
export interface WorkflowEdge {
  id: string;
  sourceNodeId: string;
  sourcePortId: string;
  targetNodeId: string;
  targetPortId: string;
}

// 工作流定义
export interface Workflow {
  id: string;
  name: string;
  description?: string;
  tableId: string; // 关联的表
  nodes: WorkflowNode[];
  edges: WorkflowEdge[];
  status: WorkflowStatus;
  createdAt: string;
  updatedAt: string;
  lastRunAt?: string;
  version: number;
}

// 工作流执行上下文
export interface WorkflowExecutionContext {
  workflowId: string;
  startedAt: string;
  currentNodeId?: string;
  nodeOutputs: Record<string, unknown>;
  errors: Array<{ nodeId: string; error: string; timestamp: string }>;
}

// 节点注册表 - 用于插件系统
export interface NodeDefinition {
  type: WorkflowNodeType;
  name: string;
  description: string;
  icon: string; // lucide icon name
  category: "source" | "transform" | "ai" | "output";
  defaultConfig: NodeConfig;
  defaultPorts: WorkflowPort[];
  // 后端执行器标识
  executorId?: string;
}

// 预定义节点模板
export const NODE_DEFINITIONS: Record<string, NodeDefinition> = {
  data_source: {
    type: "data_source",
    name: "Data Source",
    description: "Load data from table",
    icon: "Database",
    category: "source",
    defaultConfig: { columns: [], limit: 1000 },
    defaultPorts: [
      { id: "out", name: "Data", type: "output", dataType: "table" }
    ],
    executorId: "core.data_source"
  },
  ai_analysis: {
    type: "ai_analysis",
    name: "AI Analysis",
    description: "Analyze data with AI",
    icon: "Sparkles",
    category: "ai",
    defaultConfig: { prompt: "", model: "default" },
    defaultPorts: [
      { id: "in", name: "Input", type: "input", dataType: "any", required: true },
      { id: "out", name: "Result", type: "output", dataType: "any" }
    ],
    executorId: "ai.analyze"
  },
  chart_generator: {
    type: "chart_generator",
    name: "Chart Generator",
    description: "Generate visualization",
    icon: "BarChart3",
    category: "output",
    defaultConfig: { chartType: "bar", xAxis: "", yAxis: "" },
    defaultPorts: [
      { id: "in", name: "Data", type: "input", dataType: "table", required: true },
      { id: "out", name: "Chart", type: "output", dataType: "chart" }
    ],
    executorId: "viz.chart"
  },
  insight_extractor: {
    type: "insight_extractor",
    name: "Insight Extractor",
    description: "Extract insights from analysis",
    icon: "Lightbulb",
    category: "ai",
    defaultConfig: { maxInsights: 5 },
    defaultPorts: [
      { id: "in", name: "Analysis", type: "input", dataType: "any", required: true },
      { id: "out", name: "Insights", type: "output", dataType: "insights" }
    ],
    executorId: "ai.insights"
  },
  transform: {
    type: "transform",
    name: "Transform",
    description: "Transform data",
    icon: "Shuffle",
    category: "transform",
    defaultConfig: { operation: "filter", expression: "" },
    defaultPorts: [
      { id: "in", name: "Input", type: "input", dataType: "any", required: true },
      { id: "out", name: "Output", type: "output", dataType: "any" }
    ],
    executorId: "core.transform"
  },
  output: {
    type: "output",
    name: "Output",
    description: "Final output node",
    icon: "Download",
    category: "output",
    defaultConfig: { format: "json" },
    defaultPorts: [
      { id: "in", name: "Result", type: "input", dataType: "any", required: true }
    ],
    executorId: "core.output"
  }
};

// 创建新节点的工厂函数
export function createWorkflowNode(
  type: WorkflowNodeType,
  position: { x: number; y: number },
  overrides?: Partial<WorkflowNode>
): WorkflowNode {
  const definition = NODE_DEFINITIONS[type];
  if (!definition) {
    throw new Error(`Unknown node type: ${type}`);
  }

  return {
    id: `node_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`,
    type,
    name: definition.name,
    description: definition.description,
    position,
    ports: [...definition.defaultPorts],
    config: { ...definition.defaultConfig },
    status: "idle",
    ...overrides
  };
}

// 创建默认工作流
export function createDefaultWorkflow(tableId: string): Workflow {
  const dataSourceNode = createWorkflowNode("data_source", { x: 100, y: 200 });
  const analysisNode = createWorkflowNode("ai_analysis", { x: 400, y: 150 });
  const chartNode = createWorkflowNode("chart_generator", { x: 700, y: 100 });
  const insightNode = createWorkflowNode("insight_extractor", { x: 700, y: 300 });

  return {
    id: `workflow_${tableId}`,
    name: "Default Analysis Pipeline",
    description: "Analyze table data and generate insights",
    tableId,
    nodes: [dataSourceNode, analysisNode, chartNode, insightNode],
    edges: [
      {
        id: "edge_1",
        sourceNodeId: dataSourceNode.id,
        sourcePortId: "out",
        targetNodeId: analysisNode.id,
        targetPortId: "in"
      },
      {
        id: "edge_2",
        sourceNodeId: analysisNode.id,
        sourcePortId: "out",
        targetNodeId: chartNode.id,
        targetPortId: "in"
      },
      {
        id: "edge_3",
        sourceNodeId: analysisNode.id,
        sourcePortId: "out",
        targetNodeId: insightNode.id,
        targetPortId: "in"
      }
    ],
    status: "draft",
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
    version: 1
  };
}
