/**
 * Column Workflow API Service
 */

import { apiRequest } from './client';
import { ApiResponse } from './types';
import type { WorkflowLogEvent } from './eda';

export interface ColumnWorkflowEstimate {
  column: string;
  semantic_type: string;
  total_tokens: number;
  estimates: Array<{
    task: string;
    token_count: number;
    row_count?: number;
  }>;
}

export interface ColumnWorkflowToolCall {
  tool_use_id?: string | null;
  tool_name?: string | null;
  agent_name?: string | null;
  input?: Record<string, any>;
  status?: string | null;
  timestamp?: string | null;
  started_at?: string | null;
  ended_at?: string | null;
  sequence?: number | null;
  error?: string | null;
}

export interface ColumnWorkflowRunResponse {
  workflow_id: string;
  status: any;
  workflow_state?: string | null;
  column: string;
  semantic_type: string;
  workflow_logs?: WorkflowLogEvent[];
  workflow_tool_calls?: ColumnWorkflowToolCall[];
}

export interface ColumnWorkflowSelectedRunRequest {
  tool_calls: Array<{
    tool_name: string;
    input?: Record<string, any>;
  }>;
  focus?: string;
}

export const columnWorkflowsApi = {
  async estimate(tableAssetId: number, columnName: string): Promise<ApiResponse<ColumnWorkflowEstimate>> {
    return apiRequest(async () => {
      const response = await fetch(`/api/v1/column-workflows/${tableAssetId}/${encodeURIComponent(columnName)}/estimate`, {
        method: 'POST',
      });
      if (!response.ok) {
        throw new Error('Failed to estimate workflow tokens');
      }
      return await response.json();
    });
  },

  async run(
    tableAssetId: number,
    columnName: string,
    options?: { focus?: string }
  ): Promise<ApiResponse<ColumnWorkflowRunResponse>> {
    return apiRequest(async () => {
      const params = new URLSearchParams();
      if (options?.focus) {
        params.set('focus', options.focus);
      }
      const query = params.toString();
      const response = await fetch(`/api/v1/column-workflows/${tableAssetId}/${encodeURIComponent(columnName)}/run${query ? `?${query}` : ''}`, {
        method: 'POST',
      });
      if (!response.ok) {
        throw new Error('Failed to run column workflow');
      }
      return await response.json();
    });
  },

  async runSelected(
    tableAssetId: number,
    columnName: string,
    payload: ColumnWorkflowSelectedRunRequest
  ): Promise<ApiResponse<ColumnWorkflowRunResponse>> {
    return apiRequest(async () => {
      const response = await fetch(
        `/api/v1/column-workflows/${tableAssetId}/${encodeURIComponent(columnName)}/run-selected`,
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload),
        }
      );
      if (!response.ok) {
        throw new Error("Failed to run selected workflow nodes");
      }
      return await response.json();
    });
  },
};
