/**
 * Workflows API Service
 * 工作流的 CRUD 和执行操作
 * 
 * 后端 Endpoints:
 * - GET    /workflows?tableId=xxx        - 获取表格的工作流
 * - GET    /workflows/:id                - 获取单个工作流
 * - POST   /workflows                    - 创建工作流
 * - PATCH  /workflows/:id                - 更新工作流
 * - DELETE /workflows/:id                - 删除工作流
 * - POST   /workflows/:id/execute        - 执行工作流
 * - POST   /workflows/:id/nodes/:nodeId  - 执行单个节点
 */

import { Workflow, WorkflowNode, WorkflowNodeType, createDefaultWorkflow, createWorkflowNode } from '@/types/workflow';
import { 
  ApiResponse, 
  ExecuteWorkflowRequest, 
  ExecuteWorkflowResponse,
  NodeExecutionRequest,
  NodeExecutionResponse 
} from './types';
import { apiRequest, invokeFunction, simulateDelay } from './client';

// ============= Node Execution Simulator =============

/**
 * 模拟节点执行
 * 后续可替换为真实的 Edge Function 调用
 */
export async function simulateNodeExecution(
  node: WorkflowNode,
  inputs: Record<string, unknown>
): Promise<unknown> {
  await simulateDelay(800 + Math.random() * 1200);

  switch (node.type) {
    case 'data_source':
      return { 
        rows: 1000, 
        columns: ['id', 'name', 'value', 'date'],
        sampleData: [] 
      };
    case 'ai_analysis':
      return { 
        summary: 'Data shows positive trend',
        patterns: ['seasonal variation', 'growth trend'],
        anomalies: 3,
        confidence: 0.92 
      };
    case 'chart_generator':
      return { 
        chartId: `chart_${Date.now()}`, 
        chartType: 'bar',
        title: 'Generated Chart' 
      };
    case 'insight_extractor':
      return { 
        insights: [
          { title: 'Revenue Growth', confidence: 0.92 },
          { title: 'Customer Churn Risk', confidence: 0.78 }
        ] 
      };
    case 'transform':
      return { 
        transformedRows: 800, 
        operations: ['filter', 'aggregate'] 
      };
    case 'output':
      return { 
        exportPath: '/exports/result.csv', 
        format: 'csv' 
      };
    default:
      return { processed: true, inputs };
  }
}

/**
 * 获取节点执行顺序（拓扑排序）
 */
export function getExecutionOrder(workflow: Workflow): string[] {
  const { nodes, edges } = workflow;
  const inDegree: Record<string, number> = {};
  const adjacency: Record<string, string[]> = {};

  nodes.forEach(n => {
    inDegree[n.id] = 0;
    adjacency[n.id] = [];
  });

  edges.forEach(e => {
    inDegree[e.targetNodeId]++;
    adjacency[e.sourceNodeId].push(e.targetNodeId);
  });

  const queue = nodes.filter(n => inDegree[n.id] === 0).map(n => n.id);
  const order: string[] = [];

  while (queue.length > 0) {
    const nodeId = queue.shift()!;
    order.push(nodeId);

    for (const neighbor of adjacency[nodeId]) {
      inDegree[neighbor]--;
      if (inDegree[neighbor] === 0) {
        queue.push(neighbor);
      }
    }
  }

  return order;
}

// ============= API Methods =============

export const workflowsApi = {
  /**
   * 获取表格的工作流
   * GET /workflows?tableId=xxx
   */
  async getByTableId(tableId: string): Promise<ApiResponse<Workflow | null>> {
    return apiRequest(async () => {
      await simulateDelay(200);
      // TODO: 从数据库获取
      return null;
    });
  },

  /**
   * 创建默认工作流
   * POST /workflows
   */
  async create(tableId: string): Promise<ApiResponse<Workflow>> {
    return apiRequest(async () => {
      await simulateDelay(300);
      return createDefaultWorkflow(tableId);
    });
  },

  /**
   * 添加节点
   * POST /workflows/:id/nodes
   */
  async addNode(
    workflowId: string,
    type: WorkflowNodeType,
    position: { x: number; y: number }
  ): Promise<ApiResponse<WorkflowNode>> {
    return apiRequest(async () => {
      await simulateDelay(100);
      return createWorkflowNode(type, position);
    });
  },

  /**
   * 执行工作流
   * POST /workflows/:id/execute
   * 
   * 未来可调用 Edge Function:
   * return invokeFunction<ExecuteWorkflowResponse>('execute-workflow', request);
   */
  async execute(request: ExecuteWorkflowRequest): Promise<ApiResponse<ExecuteWorkflowResponse>> {
    return apiRequest(async () => {
      await simulateDelay(1000);
      
      return {
        success: true,
        outputs: { processed: true },
        executionTime: 2500,
      };
    });
  },

  /**
   * 执行单个节点
   * POST /workflows/:id/nodes/:nodeId/execute
   */
  async executeNode(request: NodeExecutionRequest): Promise<ApiResponse<NodeExecutionResponse>> {
    return apiRequest(async () => {
      const startTime = Date.now();
      
      // 模拟执行
      const output = await simulateNodeExecution(
        { id: request.nodeId, type: request.nodeType as WorkflowNodeType } as WorkflowNode,
        request.inputs
      );
      
      return {
        output,
        metadata: {
          executionTime: Date.now() - startTime,
        },
      };
    });
  },
};
