/**
 * Column Metadata API Service
 */

import { apiRequest } from './client';
import { ApiResponse } from './types';

export interface ColumnMetadataRecord {
  id: number;
  table_asset_id: number;
  column_name: string;
  semantic_type: string;
  confidence: number;
  metadata?: Record<string, any> | null;
  provenance?: Record<string, any> | null;
  examples?: any[] | null;
  overrides?: Record<string, any> | null;
  last_updated?: string | null;
  created_at: string;
  updated_at: string;
}

export interface TableAssetMetadataRecord {
  id: number;
  table_asset_id: number;
  structure_type: string;
  sampling_strategy: string;
  metadata?: Record<string, any> | null;
  overrides?: Record<string, any> | null;
  last_updated?: string | null;
  created_at: string;
  updated_at: string;
}

export interface ColumnMetadataListResponse {
  table: TableAssetMetadataRecord | null;
  columns: ColumnMetadataRecord[];
}

export const columnMetadataApi = {
  async get(tableAssetId: number): Promise<ApiResponse<ColumnMetadataListResponse>> {
    return apiRequest(async () => {
      const response = await fetch(`/api/v1/column-metadata/${tableAssetId}`, {
        cache: 'no-store',
        headers: {
          'Cache-Control': 'no-store',
        },
      });
      if (!response.ok) {
        throw new Error('Failed to fetch column metadata');
      }
      return await response.json();
    });
  },

  async initialize(tableAssetId: number, force: boolean = false): Promise<ApiResponse<ColumnMetadataListResponse>> {
    return apiRequest(async () => {
      const response = await fetch(`/api/v1/column-metadata/${tableAssetId}/initialize?force=${force}`, {
        method: 'POST',
        cache: 'no-store',
        headers: {
          'Cache-Control': 'no-store',
        },
      });
      if (!response.ok) {
        throw new Error('Failed to initialize column metadata');
      }
      return await response.json();
    });
  },

  async override(tableAssetId: number, columnName: string, overrides: Record<string, any>): Promise<ApiResponse<ColumnMetadataListResponse>> {
    return apiRequest(async () => {
      const response = await fetch(`/api/v1/column-metadata/${tableAssetId}/override`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ column_name: columnName, overrides }),
      });
      if (!response.ok) {
        throw new Error('Failed to override column metadata');
      }
      return await response.json();
    });
  },

  async bulkOverride(
    tableAssetId: number,
    overrides: Array<{ column_name: string; overrides: Record<string, any> }>,
  ): Promise<ApiResponse<ColumnMetadataListResponse>> {
    return apiRequest(async () => {
      const response = await fetch(`/api/v1/column-metadata/${tableAssetId}/bulk-override`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(overrides),
      });
      if (!response.ok) {
        throw new Error('Failed to override column metadata');
      }
      return await response.json();
    });
  },

  async overrideTable(tableAssetId: number, overrides: Record<string, any>): Promise<ApiResponse<ColumnMetadataListResponse>> {
    return apiRequest(async () => {
      const response = await fetch(`/api/v1/column-metadata/${tableAssetId}/table-override`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ overrides }),
      });
      if (!response.ok) {
        throw new Error('Failed to override table metadata');
      }
      return await response.json();
    });
  },
};
