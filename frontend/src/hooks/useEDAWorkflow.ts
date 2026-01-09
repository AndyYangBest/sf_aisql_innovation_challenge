/**
 * useEDAWorkflow Hook
 * 管理 EDA workflow 的执行、节点状态和日志
 */

import { useState, useCallback, useRef } from 'react';
import type { WorkflowJSON } from '@flowgram.ai/free-layout-editor';
import { edaApi, WorkflowLogEvent, EDARunResponse, WorkflowType } from '@/api/eda';
import { EDA_WORKFLOW_TEMPLATES, EDANodeType, EDA_NODE_DEFINITIONS } from '@/types/eda-workflow';
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
  workflowId: string | null;
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
    workflowId: null,
  });

  const eventSourceRef = useRef<EventSource | null>(null);
  const stopRequestedRef = useRef(false);
  const latestWorkflowRef = useRef<WorkflowJSON | null>(null);

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
        ...EDA_NODE_DEFINITIONS[node.type]?.defaultData,
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
      workflowId: null,
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
  }, [tableAssetId, tableName]);

  /**
   * Sync workflow layout changes from Flowgram editor
   */
  const updateWorkflowFromEditor = useCallback((workflowData: WorkflowJSON) => {
    latestWorkflowRef.current = workflowData;
    setState((prev) => {
      const prevById = new Map(prev.nodes.map((node) => [node.id, node]));
      const nextNodes: WorkflowNode[] = workflowData.nodes.map((node) => {
        const prevNode = prevById.get(node.id);
        const nodeType = node.type as EDANodeType;
        const definition = EDA_NODE_DEFINITIONS[nodeType];
        const baseData = {
          ...definition?.defaultData,
          ...node.data,
        };
        if (nodeType === 'data_source') {
          baseData.table_asset_id = tableAssetId;
          baseData.table_name = tableName;
        }
        const status = (baseData.status ??
          prevNode?.data.status ??
          'idle') as WorkflowNode['data']['status'];

        return {
          id: node.id,
          type: nodeType,
          position: node.meta?.position ?? prevNode?.position ?? { x: 0, y: 0 },
          data: {
            ...baseData,
            title: baseData.title ?? definition?.name ?? node.type,
            status,
          },
        };
      });

      const nextEdges: WorkflowEdge[] = workflowData.edges.map((edge) => ({
        sourceNodeID: edge.sourceNodeID,
        targetNodeID: edge.targetNodeID,
      }));

      return {
        ...prev,
        nodes: nextNodes,
        edges: nextEdges,
      };
    });
  }, [tableAssetId, tableName]);

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
      'export',
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
   * Parse task status updates from log messages
   */
  const parseTaskStatusFromMessage = useCallback((message: string) => {
    const patterns: Array<{ regex: RegExp; state: 'running' | 'success' | 'error' }> = [
      { regex: /Task Started: (\S+)/i, state: 'running' },
      { regex: /Task Completed: (\S+)/i, state: 'success' },
      { regex: /Task Failed: (\S+)/i, state: 'error' },
      { regex: /Task '([^']+)' completed/i, state: 'success' },
      { regex: /Task '([^']+)' failed/i, state: 'error' },
    ];

    for (const pattern of patterns) {
      const match = message.match(pattern.regex);
      if (match?.[1]) {
        return { taskId: match[1], state: pattern.state };
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
    stopRequestedRef.current = false;
    if (eventSourceRef.current) {
      eventSourceRef.current.close();
      eventSourceRef.current = null;
    }
    let workflowNodes = state.nodes;
    let workflowEdges = state.edges;

    if (workflowNodes.length === 0) {
      const initialized = initializeWorkflow(workflowType);
      workflowNodes = initialized.nodes;
      workflowEdges = initialized.edges;
    }

    const resetNodes = workflowNodes.map((node) => ({
      ...node,
      data: {
        ...node.data,
        ...(node.type === 'data_source'
          ? { table_asset_id: tableAssetId, table_name: tableName }
          : {}),
        status: 'idle',
        progress: undefined,
        error: undefined,
      },
    }));

    setState((prev) => ({
      ...prev,
      nodes: resetNodes,
      edges: workflowEdges,
      logs: [],
      result: null,
      isRunning: true,
      currentNodeId: null,
      workflowId: null,
    }));

    // Mark data_source as running
    const dataSourceNode = resetNodes.find((n) => n.type === 'data_source');
    if (dataSourceNode) {
      updateNodeStatus(dataSourceNode.id, 'running');
    }

    const nodeIdByType = new Map(resetNodes.map((node) => [node.type, node.id]));
    const nodeIdSet = new Set(resetNodes.map((node) => node.id));
    const incomingByTarget = new Map<string, string[]>();
    const outgoingBySource = new Map<string, string[]>();

    workflowEdges.forEach((edge) => {
      const incoming = incomingByTarget.get(edge.targetNodeID) ?? [];
      incoming.push(edge.sourceNodeID);
      incomingByTarget.set(edge.targetNodeID, incoming);

      const outgoing = outgoingBySource.get(edge.sourceNodeID) ?? [];
      outgoing.push(edge.targetNodeID);
      outgoingBySource.set(edge.sourceNodeID, outgoing);
    });

    let currentNodeId: string | null = dataSourceNode?.id || null;
    let dataSourceCompleted = false;

    const getNodeIdForTask = (taskId?: string | null): string | null => {
      if (!taskId) {
        return null;
      }
      const normalized = taskId.toLowerCase();
      if (nodeIdSet.has(taskId)) {
        return taskId;
      }
      return nodeIdByType.get(normalized as EDANodeType) ?? null;
    };

    const advanceDownstreamNodes = (
      updatedNodes: WorkflowNode[],
      completedNodeId: string
    ) => {
      const statusById = new Map(
        updatedNodes.map((node) => [node.id, node.data.status])
      );

      const downstream = outgoingBySource.get(completedNodeId) ?? [];
      let nextNodes = updatedNodes;

      downstream.forEach((targetId) => {
        const currentStatus = statusById.get(targetId);
        if (currentStatus && currentStatus !== 'idle' && currentStatus !== 'skipped') {
          return;
        }

        const incoming = incomingByTarget.get(targetId) ?? [];
        const allDepsComplete = incoming.every(
          (sourceId) => statusById.get(sourceId) === 'success'
        );
        if (!allDepsComplete) {
          return;
        }

        nextNodes = nextNodes.map((node) =>
          node.id === targetId
            ? {
                ...node,
                data: {
                  ...node.data,
                  status: 'running',
                },
              }
            : node
        );
        statusById.set(targetId, 'running');
      });

      return nextNodes;
    };

    const applyTaskStatus = (
      taskId: string,
      state: 'running' | 'success' | 'error',
      errorMessage?: string
    ) => {
      const nodeId = getNodeIdForTask(taskId);
      if (!nodeId) {
        return;
      }

      if (state === 'running') {
        if (dataSourceNode && !dataSourceCompleted) {
          updateNodeStatus(dataSourceNode.id, 'success', 100);
          dataSourceCompleted = true;
        }
        updateNodeStatus(nodeId, 'running');
        currentNodeId = nodeId;
        return;
      }

      if (state === 'success') {
        setState((prev) => {
          let updatedNodes = prev.nodes.map((node) =>
            node.id === nodeId
              ? {
                  ...node,
                  data: {
                    ...node.data,
                    status: 'success',
                    progress: 100,
                    error: undefined,
                  },
                }
              : node
          );
          updatedNodes = advanceDownstreamNodes(updatedNodes, nodeId);
          return {
            ...prev,
            nodes: updatedNodes,
            currentNodeId: prev.currentNodeId === nodeId ? null : prev.currentNodeId,
          };
        });
        return;
      }

      updateNodeStatus(nodeId, 'error', undefined, errorMessage);
      if (currentNodeId === nodeId) {
        currentNodeId = null;
      }
    };

    const fallbackJson: WorkflowJSON = {
      nodes: resetNodes.map((node) => ({
        id: node.id,
        type: node.type,
        meta: { position: node.position },
        data: node.data,
      })),
      edges: workflowEdges.map((edge) => ({
        sourceNodeID: edge.sourceNodeID,
        targetNodeID: edge.targetNodeID,
      })),
    };
    const latestJson = latestWorkflowRef.current;
    const workflowJson: WorkflowJSON = latestJson
      ? {
          ...latestJson,
          nodes: latestJson.nodes.map((node) =>
            node.type === 'data_source'
              ? {
                  ...node,
                  data: {
                    ...node.data,
                    table_asset_id: tableAssetId,
                    table_name: tableName,
                  },
                }
              : node
          ),
        }
      : fallbackJson;

    try {
      const eventSource = await edaApi.runWorkflowWithStreaming(
        {
          table_asset_id: tableAssetId,
          user_intent: userIntent,
          workflow_type: workflowType,
        },
        // onLog
        (event) => {
          if (event.type === 'status' && event.data?.workflow_id) {
            setState((prev) => ({
              ...prev,
              workflowId: event.data.workflow_id,
            }));
            if (stopRequestedRef.current) {
              void edaApi.cancelWorkflow(event.data.workflow_id);
            }
          }

          if (stopRequestedRef.current) {
            return;
          }
          addLog(event);

          if (event.type === 'status' && event.data?.task_id && event.data?.state) {
            applyTaskStatus(event.data.task_id, event.data.state, event.message);
            return;
          }

          if (event.message) {
            const parsedStatus = parseTaskStatusFromMessage(event.message);
            if (parsedStatus) {
              applyTaskStatus(parsedStatus.taskId, parsedStatus.state, event.message);
              return;
            }

            const nodeId = parseNodeFromLog(event.message, resetNodes);
            if (nodeId) {
              updateNodeStatus(nodeId, 'running');
              currentNodeId = nodeId;
            }
          }

          if (event.type === 'progress' && event.data?.progress !== undefined) {
            if (currentNodeId) {
              updateNodeStatus(currentNodeId, 'running', event.data.progress);
            }
          }
        },
        // onComplete
        (result) => {
          eventSourceRef.current = null;
          if (stopRequestedRef.current) {
            setState((prev) => ({
              ...prev,
              isRunning: false,
              currentNodeId: null,
            }));
            return;
          }
          if (result && result.success === false) {
            setState((prev) => ({
              ...prev,
              isRunning: false,
              currentNodeId: null,
              result: null,
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
            return;
          }
          // Mark all nodes as success
          setState((prev) => ({
            ...prev,
            nodes: prev.nodes.map((node) => ({
              ...node,
              data: { ...node.data, status: node.data.status === 'error' ? 'error' : 'success' },
            })),
            isRunning: false,
            result,
            workflowId: result?.workflow_id ?? prev.workflowId,
          }));

          toast({
            title: 'Workflow Completed',
            description: `Successfully analyzed ${tableName}`,
          });
        },
        // onError
        (error) => {
          eventSourceRef.current = null;
          if (stopRequestedRef.current) {
            setState((prev) => ({ ...prev, isRunning: false }));
            return;
          }
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
        },
        {
          workflowData: workflowJson,
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
    parseTaskStatusFromMessage,
    toast,
    state.nodes,
    state.edges,
  ]);

  /**
   * Stop workflow
   */
  const stopWorkflow = useCallback(() => {
    stopRequestedRef.current = true;
    if (eventSourceRef.current) {
      eventSourceRef.current.close();
      eventSourceRef.current = null;
    }

    if (state.workflowId) {
      void edaApi.cancelWorkflow(state.workflowId);
    }

    setState((prev) => ({
      ...prev,
      isRunning: false,
      currentNodeId: null,
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
  }, [state.workflowId, toast]);

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
      workflowId: null,
    });
  }, []);

  return {
    ...state,
    initializeWorkflow,
    runWorkflow,
    stopWorkflow,
    clearWorkflow,
    updateNodeStatus,
    updateWorkflowFromEditor,
  };
}
