/**
 * Column Workflow API Service
 */

import { apiRequest } from './client';
import { ApiResponse } from './types';

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

export interface ColumnWorkflowRunResponse {
  workflow_id: string;
  status: any;
  workflow_state?: string | null;
  column: string;
  semantic_type: string;
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

  async run(tableAssetId: number, columnName: string): Promise<ApiResponse<ColumnWorkflowRunResponse>> {
    return apiRequest(async () => {
      const response = await fetch(`/api/v1/column-workflows/${tableAssetId}/${encodeURIComponent(columnName)}/run`, {
        method: 'POST',
      });
      if (!response.ok) {
        throw new Error('Failed to run column workflow');
      }
      return await response.json();
    });
  },
};
