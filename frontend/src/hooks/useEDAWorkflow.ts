/**
 * useEDAWorkflow Hook
 * 管理 EDA workflow 的执行、节点状态和日志
 */

import { useState, useCallback, useRef } from 'react';
import { edaApi, WorkflowLogEvent, EDARunResponse, WorkflowType } from '@/api/eda';
import { EDA_WORKFLOW_TEMPLATES, EDANodeType } from '@/types/eda-workflow';
import { useToast } from './use-toast';

export interface WorkflowNode {
  id: string;
  type: EDANodeType;
  position: { x: number; y: number };
  data: {
    title: string;
    status: 'idle' | 'running' | 'success' | 'error' | 'skipped';
    progress?: number;
    error?: string;
    [key: string]: any;
  };
}

export interface WorkflowEdge {
  sourceNodeID: string;
  targetNodeID: string;
}

export interface EDAWorkflowState {
  nodes: WorkflowNode[];
  edges: WorkflowEdge[];
  logs: WorkflowLogEvent[];
  isRunning: boolean;
  currentNodeId: string | null;
  result: EDARunResponse | null;
}

export function useEDAWorkflow(tableAssetId: number, tableName: string) {
  const { toast } = useToast();
  const [state, setState] = useState<EDAWorkflowState>({
    nodes: [],
    edges: [],
    logs: [],
    isRunning: false,
    currentNodeId: null,
    result: null,
  });

  const eventSourceRef = useRef<EventSource | null>(null);

  /**
   * Initialize workflow with template
   */
  const initializeWorkflow = useCallback((workflowType: WorkflowType = 'EDA_OVERVIEW') => {
    const templateKey = workflowType === 'EDA_OVERVIEW' ? 'overview' :
                        workflowType === 'EDA_TIME_SERIES' ? 'time_series' :
                        'data_quality';

    const template = EDA_WORKFLOW_TEMPLATES[templateKey];

    // Convert template to workflow nodes
    const nodes: WorkflowNode[] = template.nodes.map((node) => ({
      ...node,
      data: {
        ...node.data,
        title: node.data?.title || node.type,
        status: 'idle',
        ...(node.type === 'data_source' && {
          table_asset_id: tableAssetId,
          table_name: tableName,
        }),
      },
    }));

    setState((prev) => ({
      ...prev,
      nodes,
      edges: template.edges,
      logs: [],
      result: null,
    }));

    return { nodes, edges: template.edges };
  }, [tableAssetId, tableName]);

  /**
   * Update node status
   */
  const updateNodeStatus = useCallback((
    nodeId: string,
    status: 'idle' | 'running' | 'success' | 'error' | 'skipped',
    progress?: number,
    error?: string
  ) => {
    setState((prev) => ({
      ...prev,
      nodes: prev.nodes.map((node) =>
        node.id === nodeId
          ? {
              ...node,
              data: {
                ...node.data,
                status,
                progress,
                error,
              },
            }
          : node
      ),
      currentNodeId: status === 'running' ? nodeId : prev.currentNodeId,
    }));
  }, []);

  /**
   * Add log entry
   */
  const addLog = useCallback((log: WorkflowLogEvent) => {
    setState((prev) => ({
      ...prev,
      logs: [...prev.logs, log],
    }));
  }, []);

  /**
   * Parse log message to determine which node is running
   */
  const parseNodeFromLog = useCallback((message: string, nodes: WorkflowNode[]): string | null => {
    // Match patterns like "profile_table", "generate_insights", etc.
    const nodePatterns = [
      'profile_table',
      'generate_insights',
      'generate_charts',
      'generate_documentation',
    ];

    for (const pattern of nodePatterns) {
      if (message.toLowerCase().includes(pattern)) {
        // Find the node with this type
        const node = nodes.find((n) => n.type === pattern);
        return node?.id || null;
      }
    }

    return null;
  }, []);

  /**
   * Run workflow with streaming
   */
  const runWorkflow = useCallback(async (
    userIntent?: string,
    workflowType?: WorkflowType
  ) => {
    if (eventSourceRef.current) {
      eventSourceRef.current.close();
      eventSourceRef.current = null;
    }
    // Initialize workflow first
    const { nodes } = initializeWorkflow(workflowType);

    setState((prev) => ({ ...prev, isRunning: true }));

    // Mark data_source as running
    const dataSourceNode = nodes.find((n) => n.type === 'data_source');
    if (dataSourceNode) {
      updateNodeStatus(dataSourceNode.id, 'running');
    }

    let currentNodeId: string | null = dataSourceNode?.id || null;

    try {
      const eventSource = await edaApi.runWorkflowWithStreaming(
        {
          table_asset_id: tableAssetId,
          user_intent: userIntent,
          workflow_type: workflowType,
        },
        // onLog
        (event) => {
          addLog(event);

          // Try to determine which node is running from the log message
          if (event.message) {
            const nodeId = parseNodeFromLog(event.message, nodes);
            if (nodeId) {
              // Mark previous node as success
              if (currentNodeId) {
                updateNodeStatus(currentNodeId, 'success');
              }
              // Mark current node as running
              updateNodeStatus(nodeId, 'running');
              currentNodeId = nodeId;
            }
          }

          // Handle progress updates
          if (event.type === 'progress' && event.data?.progress) {
            if (currentNodeId) {
              updateNodeStatus(
                currentNodeId,
                'running',
                event.data.progress
              );
            }
          }
        },
        // onComplete
        (result) => {
          eventSourceRef.current = null;
          // Mark all nodes as success
          setState((prev) => ({
            ...prev,
            nodes: prev.nodes.map((node) => ({
              ...node,
              data: { ...node.data, status: 'success' },
            })),
            isRunning: false,
            result,
          }));

          toast({
            title: 'Workflow Completed',
            description: `Successfully analyzed ${tableName}`,
          });
        },
        // onError
        (error) => {
          eventSourceRef.current = null;
          // Mark current node as error
          if (currentNodeId) {
            updateNodeStatus(currentNodeId, 'error', undefined, error);
          }

          setState((prev) => ({ ...prev, isRunning: false }));

          toast({
            title: 'Workflow Failed',
            description: error,
            variant: 'destructive',
          });
        }
      );
      eventSourceRef.current = eventSource;
    } catch (error) {
      setState((prev) => ({ ...prev, isRunning: false }));
      toast({
        title: 'Failed to start workflow',
        description: error instanceof Error ? error.message : 'Unknown error',
        variant: 'destructive',
      });
    }
  }, [
    tableAssetId,
    tableName,
    eventSourceRef,
    initializeWorkflow,
    updateNodeStatus,
    addLog,
    parseNodeFromLog,
    toast,
  ]);

  /**
   * Stop workflow
   */
  const stopWorkflow = useCallback(() => {
    if (eventSourceRef.current) {
      eventSourceRef.current.close();
      eventSourceRef.current = null;
    }

    setState((prev) => ({
      ...prev,
      isRunning: false,
      nodes: prev.nodes.map((node) => ({
        ...node,
        data: {
          ...node.data,
          status: node.data.status === 'running' ? 'skipped' : node.data.status,
        },
      })),
    }));

    toast({
      title: 'Workflow Stopped',
      description: 'Workflow execution was cancelled',
    });
  }, [toast]);

  /**
   * Clear workflow
   */
  const clearWorkflow = useCallback(() => {
    setState({
      nodes: [],
      edges: [],
      logs: [],
      isRunning: false,
      currentNodeId: null,
      result: null,
    });
  }, []);

  return {
    ...state,
    initializeWorkflow,
    runWorkflow,
    stopWorkflow,
    clearWorkflow,
    updateNodeStatus,
  };
}
