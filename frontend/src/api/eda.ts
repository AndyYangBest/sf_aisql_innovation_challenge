/**
 * EDA Workflow API Service
 * EDA 工作流相关的 API 操作
 */

import { ApiResponse } from './types';
import { apiRequest } from './client';

// ============================================================================
// Types
// ============================================================================

export type WorkflowType = 'EDA_OVERVIEW' | 'EDA_TIME_SERIES' | 'EDA_DATA_QUALITY';

export type WorkflowStatus = 'pending' | 'running' | 'completed' | 'failed';

export type NodeStatus = 'idle' | 'running' | 'success' | 'error' | 'skipped';

export interface WorkflowExecution {
  id: number;
  workflow_id: string;
  workflow_type: WorkflowType;
  status: WorkflowStatus;
  progress: number;
  tasks_total: number;
  tasks_completed: number;
  tasks_failed: number;
  data_structure_type?: string;
  started_at?: string;
  completed_at?: string;
  duration_seconds?: number;
  user_intent?: string;
  error_message?: string;
}

export interface WorkflowDetails extends WorkflowExecution {
  table_asset_id: number;
  artifacts?: Record<string, any>;
  summary?: Record<string, any>;
  column_type_inferences?: any[];
}

export interface WorkflowStats {
  table_asset_id: number;
  total_executions: number;
  successful_executions: number;
  failed_executions: number;
  success_rate: number;
  avg_duration_seconds?: number;
  last_execution?: string;
}

export interface EDARunRequest {
  table_asset_id: number;
  user_intent?: string;
  workflow_type?: WorkflowType;
}

export interface EDARunResponse {
  success: boolean;
  workflow: string;
  workflow_id: string;
  table_asset_id: number;
  table_name: string;
  artifacts: Record<string, any>;
  summary: Record<string, any>;
  error?: string;
}

export interface WorkflowLogEvent {
  type: 'log' | 'status' | 'progress' | 'complete' | 'error';
  timestamp: string;
  message?: string;
  data?: any;
}

// ============================================================================
// API Methods
// ============================================================================

export const edaApi = {
  /**
   * Run EDA workflow on a table asset
   */
  async runWorkflow(request: EDARunRequest): Promise<ApiResponse<EDARunResponse>> {
    return apiRequest(async () => {
      const response = await fetch('/api/v1/eda/run', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(request),
      });

      if (!response.ok) {
        throw new Error('Failed to run EDA workflow');
      }

      return await response.json();
    });
  },

  /**
   * Run EDA workflow with streaming logs (SSE)
   */
  async runWorkflowWithStreaming(
    request: EDARunRequest,
    onLog: (event: WorkflowLogEvent) => void,
    onComplete: (result: EDARunResponse) => void,
    onError: (error: string) => void
  ): Promise<EventSource> {
    try {
      const params = new URLSearchParams({
        table_asset_id: request.table_asset_id.toString(),
        ...(request.user_intent && { user_intent: request.user_intent }),
        ...(request.workflow_type && { workflow_type: request.workflow_type }),
      });

      const eventSource = new EventSource(`/api/v1/eda/run-stream?${params}`);
      let hasCompleted = false;
      let hasErrored = false;

      const handleLog = (raw: string | undefined) => {
        if (!raw) {
          return;
        }
        try {
          const event: WorkflowLogEvent = JSON.parse(raw);
          onLog(event);
        } catch {
          // Ignore malformed payloads.
        }
      };

      eventSource.addEventListener('log', (e) => {
        handleLog((e as MessageEvent).data);
      });

      eventSource.addEventListener('status', (e) => {
        handleLog((e as MessageEvent).data);
      });

      eventSource.addEventListener('progress', (e) => {
        handleLog((e as MessageEvent).data);
      });

      eventSource.addEventListener('complete', (e) => {
        hasCompleted = true;
        const result: EDARunResponse = JSON.parse((e as MessageEvent).data);
        onComplete(result);
        eventSource.close();
      });

      eventSource.addEventListener('workflow-error', (e) => {
        const raw = (e as MessageEvent).data;
        if (!raw) {
          return;
        }
        try {
          const errorData = JSON.parse(raw);
          hasErrored = true;
          onError(errorData.message || 'Workflow execution failed');
          eventSource.close();
        } catch {
          // Ignore non-JSON errors; onerror will handle connection issues.
        }
      });

      eventSource.onmessage = (e) => {
        handleLog(e.data);
      };

      eventSource.onerror = () => {
        if (hasCompleted || hasErrored) {
          return;
        }
        onError('Connection to server lost');
        eventSource.close();
      };
      return eventSource;
    } catch (error) {
      onError(error instanceof Error ? error.message : 'Unknown error');
      throw error;
    }
  },

  /**
   * Get workflow execution history for a table
   */
  async getHistory(
    tableAssetId: number,
    limit: number = 10
  ): Promise<ApiResponse<{ table_asset_id: number; total: number; executions: WorkflowExecution[] }>> {
    return apiRequest(async () => {
      const response = await fetch(`/api/v1/eda/history/${tableAssetId}?limit=${limit}`);

      if (!response.ok) {
        throw new Error('Failed to fetch workflow history');
      }

      return await response.json();
    });
  },

  /**
   * Get detailed information about a specific workflow
   */
  async getWorkflowDetails(workflowId: string): Promise<ApiResponse<WorkflowDetails>> {
    return apiRequest(async () => {
      const response = await fetch(`/api/v1/eda/workflow/${workflowId}`);

      if (!response.ok) {
        throw new Error('Failed to fetch workflow details');
      }

      return await response.json();
    });
  },

  /**
   * Get workflow execution statistics
   */
  async getStats(tableAssetId: number): Promise<ApiResponse<WorkflowStats>> {
    return apiRequest(async () => {
      const response = await fetch(`/api/v1/eda/stats/${tableAssetId}`);

      if (!response.ok) {
        throw new Error('Failed to fetch workflow stats');
      }

      return await response.json();
    });
  },

  /**
   * Get available workflow types
   */
  async getWorkflowTypes(): Promise<ApiResponse<{ workflows: Array<{ type: string; name: string; description: string }> }>> {
    return apiRequest(async () => {
      const response = await fetch('/api/v1/eda/workflows');

      if (!response.ok) {
        throw new Error('Failed to fetch workflow types');
      }

      return await response.json();
    });
  },
};
