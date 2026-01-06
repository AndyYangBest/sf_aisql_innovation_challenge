/**
 * Workflow Store - 工作流状态管理
 * 支持后端执行器集成
 */

import { create } from "zustand";
import { persist } from "zustand/middleware";
import {
  Workflow,
  WorkflowNode,
  WorkflowEdge,
  WorkflowStatus,
  NodeExecutionStatus,
  WorkflowExecutionContext,
  createDefaultWorkflow,
  createWorkflowNode,
  WorkflowNodeType,
} from "@/types/workflow";

interface WorkflowStore {
  // State
  workflows: Record<string, Workflow>;
  executionContexts: Record<string, WorkflowExecutionContext>;
  
  // Workflow CRUD
  getWorkflow: (tableId: string) => Workflow | undefined;
  createWorkflow: (tableId: string) => Workflow;
  updateWorkflow: (workflowId: string, updates: Partial<Workflow>) => void;
  deleteWorkflow: (workflowId: string) => void;
  
  // Node operations
  addNode: (workflowId: string, type: WorkflowNodeType, position: { x: number; y: number }) => WorkflowNode;
  updateNode: (workflowId: string, nodeId: string, updates: Partial<WorkflowNode>) => void;
  removeNode: (workflowId: string, nodeId: string) => void;
  
  // Edge operations
  addEdge: (workflowId: string, edge: Omit<WorkflowEdge, "id">) => void;
  removeEdge: (workflowId: string, edgeId: string) => void;
  
  // Execution
  startExecution: (workflowId: string) => void;
  updateNodeStatus: (workflowId: string, nodeId: string, status: NodeExecutionStatus, output?: unknown, error?: string) => void;
  completeExecution: (workflowId: string, success: boolean) => void;
  resetExecution: (workflowId: string) => void;
  
  // Execution context
  getExecutionContext: (workflowId: string) => WorkflowExecutionContext | undefined;
}

export const useWorkflowStore = create<WorkflowStore>()(
  persist(
    (set, get) => ({
      workflows: {},
      executionContexts: {},

      getWorkflow: (tableId) => {
        const workflows = get().workflows;
        return Object.values(workflows).find((w) => w.tableId === tableId);
      },

      createWorkflow: (tableId) => {
        const existing = get().getWorkflow(tableId);
        if (existing) return existing;

        const workflow = createDefaultWorkflow(tableId);
        set((state) => ({
          workflows: { ...state.workflows, [workflow.id]: workflow }
        }));
        return workflow;
      },

      updateWorkflow: (workflowId, updates) => {
        set((state) => {
          const workflow = state.workflows[workflowId];
          if (!workflow) return state;
          
          return {
            workflows: {
              ...state.workflows,
              [workflowId]: {
                ...workflow,
                ...updates,
                updatedAt: new Date().toISOString()
              }
            }
          };
        });
      },

      deleteWorkflow: (workflowId) => {
        set((state) => {
          const { [workflowId]: _, ...rest } = state.workflows;
          return { workflows: rest };
        });
      },

      addNode: (workflowId, type, position) => {
        const node = createWorkflowNode(type, position);
        
        set((state) => {
          const workflow = state.workflows[workflowId];
          if (!workflow) return state;

          return {
            workflows: {
              ...state.workflows,
              [workflowId]: {
                ...workflow,
                nodes: [...workflow.nodes, node],
                updatedAt: new Date().toISOString()
              }
            }
          };
        });

        return node;
      },

      updateNode: (workflowId, nodeId, updates) => {
        set((state) => {
          const workflow = state.workflows[workflowId];
          if (!workflow) return state;

          return {
            workflows: {
              ...state.workflows,
              [workflowId]: {
                ...workflow,
                nodes: workflow.nodes.map((n) =>
                  n.id === nodeId ? { ...n, ...updates } : n
                ),
                updatedAt: new Date().toISOString()
              }
            }
          };
        });
      },

      removeNode: (workflowId, nodeId) => {
        set((state) => {
          const workflow = state.workflows[workflowId];
          if (!workflow) return state;

          return {
            workflows: {
              ...state.workflows,
              [workflowId]: {
                ...workflow,
                nodes: workflow.nodes.filter((n) => n.id !== nodeId),
                // Also remove connected edges
                edges: workflow.edges.filter(
                  (e) => e.sourceNodeId !== nodeId && e.targetNodeId !== nodeId
                ),
                updatedAt: new Date().toISOString()
              }
            }
          };
        });
      },

      addEdge: (workflowId, edge) => {
        const newEdge: WorkflowEdge = {
          ...edge,
          id: `edge_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`
        };

        set((state) => {
          const workflow = state.workflows[workflowId];
          if (!workflow) return state;

          // Prevent duplicate edges
          const exists = workflow.edges.some(
            (e) =>
              e.sourceNodeId === edge.sourceNodeId &&
              e.sourcePortId === edge.sourcePortId &&
              e.targetNodeId === edge.targetNodeId &&
              e.targetPortId === edge.targetPortId
          );
          if (exists) return state;

          return {
            workflows: {
              ...state.workflows,
              [workflowId]: {
                ...workflow,
                edges: [...workflow.edges, newEdge],
                updatedAt: new Date().toISOString()
              }
            }
          };
        });
      },

      removeEdge: (workflowId, edgeId) => {
        set((state) => {
          const workflow = state.workflows[workflowId];
          if (!workflow) return state;

          return {
            workflows: {
              ...state.workflows,
              [workflowId]: {
                ...workflow,
                edges: workflow.edges.filter((e) => e.id !== edgeId),
                updatedAt: new Date().toISOString()
              }
            }
          };
        });
      },

      startExecution: (workflowId) => {
        const workflow = get().workflows[workflowId];
        if (!workflow) return;

        // Create execution context
        const context: WorkflowExecutionContext = {
          workflowId,
          startedAt: new Date().toISOString(),
          nodeOutputs: {},
          errors: []
        };

        set((state) => ({
          executionContexts: { ...state.executionContexts, [workflowId]: context },
          workflows: {
            ...state.workflows,
            [workflowId]: {
              ...workflow,
              status: "running" as WorkflowStatus,
              lastRunAt: new Date().toISOString(),
              // Reset all nodes to pending
              nodes: workflow.nodes.map((n) => ({
                ...n,
                status: "pending" as NodeExecutionStatus,
                error: undefined,
                output: undefined
              }))
            }
          }
        }));
      },

      updateNodeStatus: (workflowId, nodeId, status, output, error) => {
        set((state) => {
          const workflow = state.workflows[workflowId];
          const context = state.executionContexts[workflowId];
          if (!workflow) return state;

          const newContext = context
            ? {
                ...context,
                currentNodeId: status === "running" ? nodeId : context.currentNodeId,
                nodeOutputs: output
                  ? { ...context.nodeOutputs, [nodeId]: output }
                  : context.nodeOutputs,
                errors: error
                  ? [
                      ...context.errors,
                      { nodeId, error, timestamp: new Date().toISOString() }
                    ]
                  : context.errors
              }
            : undefined;

          return {
            executionContexts: newContext
              ? { ...state.executionContexts, [workflowId]: newContext }
              : state.executionContexts,
            workflows: {
              ...state.workflows,
              [workflowId]: {
                ...workflow,
                nodes: workflow.nodes.map((n) =>
                  n.id === nodeId
                    ? {
                        ...n,
                        status,
                        output,
                        error,
                        executedAt: status === "success" || status === "error"
                          ? new Date().toISOString()
                          : n.executedAt
                      }
                    : n
                )
              }
            }
          };
        });
      },

      completeExecution: (workflowId, success) => {
        set((state) => {
          const workflow = state.workflows[workflowId];
          if (!workflow) return state;

          return {
            workflows: {
              ...state.workflows,
              [workflowId]: {
                ...workflow,
                status: success ? "completed" : "failed"
              }
            }
          };
        });
      },

      resetExecution: (workflowId) => {
        set((state) => {
          const workflow = state.workflows[workflowId];
          if (!workflow) return state;

          const { [workflowId]: _, ...restContexts } = state.executionContexts;

          return {
            executionContexts: restContexts,
            workflows: {
              ...state.workflows,
              [workflowId]: {
                ...workflow,
                status: "ready",
                nodes: workflow.nodes.map((n) => ({
                  ...n,
                  status: "idle",
                  error: undefined,
                  output: undefined,
                  executedAt: undefined
                }))
              }
            }
          };
        });
      },

      getExecutionContext: (workflowId) => {
        return get().executionContexts[workflowId];
      }
    }),
    {
      name: "workflow-storage",
      partialize: (state) => ({
        workflows: state.workflows
      })
    }
  )
);

// 模拟执行器 - 后续可替换为真实后端调用
export async function simulateNodeExecution(
  node: WorkflowNode,
  inputs: Record<string, unknown>
): Promise<unknown> {
  // 模拟网络延迟
  await new Promise((resolve) => setTimeout(resolve, 800 + Math.random() * 1200));

  // 模拟不同节点类型的输出
  switch (node.type) {
    case "data_source":
      return { rows: 1000, columns: ["id", "name", "value", "date"] };
    case "ai_analysis":
      return {
        summary: "Data shows positive trend",
        patterns: ["seasonal variation", "growth trend"],
        anomalies: 3
      };
    case "chart_generator":
      return { chartId: `chart_${Date.now()}`, type: "bar" };
    case "insight_extractor":
      return {
        insights: [
          { title: "Revenue Growth", confidence: 0.92 },
          { title: "Customer Churn Risk", confidence: 0.78 }
        ]
      };
    default:
      return inputs;
  }
}

// 拓扑排序获取执行顺序
export function getExecutionOrder(workflow: Workflow): string[] {
  const { nodes, edges } = workflow;
  const inDegree: Record<string, number> = {};
  const adjacency: Record<string, string[]> = {};

  // Initialize
  nodes.forEach((n) => {
    inDegree[n.id] = 0;
    adjacency[n.id] = [];
  });

  // Build graph
  edges.forEach((e) => {
    inDegree[e.targetNodeId]++;
    adjacency[e.sourceNodeId].push(e.targetNodeId);
  });

  // Kahn's algorithm
  const queue = nodes.filter((n) => inDegree[n.id] === 0).map((n) => n.id);
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
