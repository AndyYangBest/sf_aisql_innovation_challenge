/**
 * useWorkflowExecution - 工作流执行 hook
 * 管理工作流运行状态和节点执行
 */

import { useState, useCallback } from 'react';
import { Workflow, WorkflowNode, NodeExecutionStatus } from '@/types/workflow';
import { simulateNodeExecution, getExecutionOrder } from '@/api/workflows';
import { useToast } from '@/hooks/use-toast';

export interface NodeStatus {
  status: NodeExecutionStatus;
  output?: unknown;
  error?: string;
}

export function useWorkflowExecution(workflow: Workflow | null) {
  const [isRunning, setIsRunning] = useState(false);
  const [nodeStatuses, setNodeStatuses] = useState<Record<string, NodeStatus>>({});
  const { toast } = useToast();

  const updateNodeStatus = useCallback((
    nodeId: string, 
    status: NodeExecutionStatus, 
    output?: unknown, 
    error?: string
  ) => {
    setNodeStatuses(prev => ({
      ...prev,
      [nodeId]: { status, output, error },
    }));
  }, []);

  const resetExecution = useCallback(() => {
    setNodeStatuses({});
  }, []);

  const execute = useCallback(async (onComplete?: () => void) => {
    if (!workflow || isRunning) return;

    setIsRunning(true);
    resetExecution();

    try {
      const order = getExecutionOrder(workflow);
      const outputs: Record<string, unknown> = {};

      for (const nodeId of order) {
        const node = workflow.nodes.find(n => n.id === nodeId);
        if (!node) continue;

        updateNodeStatus(nodeId, 'running');

        try {
          // 收集输入
          const inputs: Record<string, unknown> = {};
          workflow.edges
            .filter(e => e.targetNodeId === nodeId)
            .forEach(e => {
              inputs[e.targetPortId] = outputs[e.sourceNodeId];
            });

          // 执行节点
          const output = await simulateNodeExecution(node, inputs);
          outputs[nodeId] = output;
          updateNodeStatus(nodeId, 'success', output);
        } catch (error) {
          updateNodeStatus(
            nodeId,
            'error',
            undefined,
            error instanceof Error ? error.message : 'Unknown error'
          );
        }
      }

      toast({ title: 'Workflow completed' });
      onComplete?.();
    } catch (error) {
      console.error('Workflow execution failed:', error);
      toast({ title: 'Workflow failed', variant: 'destructive' });
    } finally {
      setIsRunning(false);
    }
  }, [workflow, isRunning, updateNodeStatus, resetExecution, toast]);

  return {
    isRunning,
    nodeStatuses,
    execute,
    resetExecution,
    updateNodeStatus,
  };
}
