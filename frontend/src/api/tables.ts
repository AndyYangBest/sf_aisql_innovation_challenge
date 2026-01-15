/**
 * Tables API Service
 * 表格资产相关的 API 操作
 */

import { TableAsset, TableResult } from '@/types';
import { ApiResponse } from './types';
import { apiRequest } from './client';

// Snowflake table interface
export interface SnowflakeTable {
  DATABASE_NAME: string;
  SCHEMA_NAME: string;
  TABLE_NAME: string;
  TABLE_TYPE: string;
  ROW_COUNT: number;
  BYTES: number;
  CREATED: string;
  LAST_ALTERED: string;
  COMMENT: string;
}

// API 方法
export const tablesApi = {
  // 获取所有表格
  async getAll(): Promise<ApiResponse<TableAsset[]>> {
    return apiRequest(async () => {
      const response = await fetch('/api/v1/table-assets?page=1&page_size=50');
      if (!response.ok) {
        throw new Error('Failed to fetch table assets');
      }
      const result = await response.json();
      return (result.items || []).map((item: any) => ({
        id: item.id.toString(),
        name: item.name,
        sourceSql: item.source_sql,
        database: item.database,
        schema: item.schema,
        createdAt: item.created_at,
        updatedAt: item.updated_at,
        tags: item.tags,
        owner: item.owner,
        aiSummary: item.ai_summary,
        useCases: item.use_cases,
      }));
    });
  },

  // 获取单个表格
  async getById(id: string): Promise<ApiResponse<TableAsset | null>> {
    return apiRequest(async () => {
      const response = await fetch(`/api/v1/table-assets/${id}`);
      if (!response.ok) {
        if (response.status === 404) {
          return null;
        }
        throw new Error('Failed to fetch table asset');
      }
      const result = await response.json();
      return {
        id: result.id.toString(),
        name: result.name,
        sourceSql: result.source_sql,
        database: result.database,
        schema: result.schema,
        createdAt: result.created_at,
        updatedAt: result.updated_at,
        tags: result.tags,
        owner: result.owner,
        aiSummary: result.ai_summary,
        useCases: result.use_cases,
      };
    });
  },

  // Get table asset by id from database
  async getTableAssetById(id: string): Promise<ApiResponse<TableAsset | null>> {
    return apiRequest(async () => {
      const response = await fetch(`/api/v1/table-assets/${id}`);
      if (!response.ok) {
        if (response.status === 404) {
          return null;
        }
        throw new Error('Failed to fetch table asset');
      }

      const result = await response.json();
      return {
        id: result.id.toString(),
        name: result.name,
        sourceSql: result.source_sql,
        database: result.database,
        schema: result.schema,
        createdAt: result.created_at,
        updatedAt: result.updated_at,
        tags: result.tags,
        owner: result.owner,
        aiSummary: result.ai_summary,
        useCases: result.use_cases,
      };
    });
  },

  // 获取表格结果数据
  async getResult(id: string): Promise<ApiResponse<TableResult | null>> {
    return apiRequest(async () => {
      const response = await fetch(`/api/v1/table-assets/${id}/preview`, {
        cache: 'no-store',
        headers: {
          'Cache-Control': 'no-store',
        },
      });
      if (!response.ok) {
        if (response.status === 404) {
          return null;
        }
        throw new Error('Failed to fetch table preview');
      }
      const result = await response.json();
      return {
        columns: Array.isArray(result.columns)
          ? result.columns.map((column: any) => ({
              name: column.name,
              type: column.type,
              role: column.role,
            }))
          : [],
        rows: Array.isArray(result.rows) ? result.rows : [],
        rowCount: result.row_count ?? result.rowCount ?? (Array.isArray(result.rows) ? result.rows.length : 0),
      };
    });
  },

  // 创建表格
  async create(asset: Omit<TableAsset, 'id' | 'createdAt' | 'updatedAt'>): Promise<ApiResponse<TableAsset>> {
    return apiRequest(async () => {
      const response = await fetch('/api/v1/table-assets', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          name: asset.name,
          source_sql: asset.sourceSql,
          database: asset.database,
          schema: asset.schema,
          tags: asset.tags,
          owner: asset.owner,
          ai_summary: asset.aiSummary,
          use_cases: asset.useCases || [],
        }),
      });
      if (!response.ok) {
        throw new Error('Failed to create table asset');
      }
      const result = await response.json();
      return {
        id: result.id.toString(),
        name: result.name,
        sourceSql: result.source_sql,
        database: result.database,
        schema: result.schema,
        createdAt: result.created_at,
        updatedAt: result.updated_at,
        tags: result.tags,
        owner: result.owner,
        aiSummary: result.ai_summary,
        useCases: result.use_cases,
      };
    });
  },

  // 更新表格
  async update(id: string, updates: Partial<TableAsset>): Promise<ApiResponse<TableAsset | null>> {
    return apiRequest(async () => {
      const response = await fetch(`/api/v1/table-assets/${id}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          name: updates.name,
          source_sql: updates.sourceSql,
          database: updates.database,
          schema: updates.schema,
          tags: updates.tags,
          ai_summary: updates.aiSummary,
          use_cases: updates.useCases,
        }),
      });
      if (!response.ok) {
        if (response.status === 404) {
          return null;
        }
        throw new Error('Failed to update table asset');
      }
      const result = await response.json();
      return {
        id: result.id.toString(),
        name: result.name,
        sourceSql: result.source_sql,
        database: result.database,
        schema: result.schema,
        createdAt: result.created_at,
        updatedAt: result.updated_at,
        tags: result.tags,
        owner: result.owner,
        aiSummary: result.ai_summary,
        useCases: result.use_cases,
      };
    });
  },

  // 删除表格
  async delete(id: string): Promise<ApiResponse<boolean>> {
    return apiRequest(async () => {
      const response = await fetch(`/api/v1/table-assets/${id}`, {
        method: 'DELETE',
      });
      if (!response.ok) {
        if (response.status === 404) {
          return false;
        }
        throw new Error('Failed to delete table asset');
      }
      return true;
    });
  },

  // Get Snowflake tables from backend
  async getSnowflakeTables(database?: string, schema?: string): Promise<ApiResponse<SnowflakeTable[]>> {
    return apiRequest(async () => {
      const params = new URLSearchParams();
      if (database) params.append('database', database);
      if (schema) params.append('schema', schema);

      const response = await fetch(`/api/v1/tables?${params.toString()}`);
      let result: any = null;
      try {
        result = await response.json();
      } catch {
        result = null;
      }
      if (!response.ok) {
        throw new Error(result?.detail || result?.error || 'Failed to fetch Snowflake tables');
      }
      return result?.data ?? [];
    });
  },

  // Execute SQL query on Snowflake
  async executeSql(sql: string, limit: number = 50): Promise<ApiResponse<{
    success: boolean;
    columns: Array<{ name: string; type: string }>;
    rows: Array<Record<string, any>>;
    row_count: number;
    error: string | null;
  }>> {
    return apiRequest(async () => {
      const response = await fetch('/api/v1/ai-sql/execute', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ sql, limit }),
      });

      if (!response.ok) {
        throw new Error('Failed to execute SQL query');
      }

      return await response.json();
    });
  },

  // Get AI metadata suggestions for SQL
  async suggestMetadata(
    sql: string,
    tableName?: string,
    columns?: Array<{ name: string; type: string }>,
    sampleRows?: Array<Record<string, any>>
  ): Promise<ApiResponse<{
    success: boolean;
    suggested_name: string;
    suggested_tags: string[];
    ai_summary: string | null;
    use_cases: string[];
    error: string | null;
  }>> {
    return apiRequest(async () => {
      const response = await fetch('/api/v1/ai-sql/suggest-metadata', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          sql,
          table_name: tableName,
          columns: columns,
          sample_rows: sampleRows,
        }),
      });

      if (!response.ok) {
        throw new Error('Failed to get AI suggestions');
      }

      return await response.json();
    });
  },

  // Save table asset to database
  async saveTableAsset(asset: {
    name: string;
    source_sql: string;
    database?: string;
    schema?: string;
    tags: string[];
    owner?: string;
    ai_summary?: string;
    use_cases?: string[];
  }): Promise<ApiResponse<TableAsset>> {
    return apiRequest(async () => {
      const response = await fetch('/api/v1/table-assets', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          name: asset.name,
          source_sql: asset.source_sql,
          database: asset.database || 'ANALYTICS',
          schema: asset.schema || 'PUBLIC',
          tags: asset.tags,
          owner: asset.owner || 'current_user',
          ai_summary: asset.ai_summary,
          use_cases: asset.use_cases || [],
        }),
      });

      if (!response.ok) {
        throw new Error('Failed to save table asset');
      }

      const result = await response.json();
      // Convert backend format to frontend format
      return {
        id: result.id.toString(),
        name: result.name,
        sourceSql: result.source_sql,
        database: result.database,
        schema: result.schema,
        createdAt: result.created_at,
        updatedAt: result.updated_at,
        tags: result.tags,
        owner: result.owner,
        aiSummary: result.ai_summary,
        useCases: result.use_cases,
      };
    });
  },

  // Get all table assets from database
  async getAllTableAssets(page: number = 1, pageSize: number = 50): Promise<ApiResponse<{
    items: TableAsset[];
    total: number;
    page: number;
    page_size: number;
  }>> {
    return apiRequest(async () => {
      const params = new URLSearchParams();
      params.append('page', page.toString());
      params.append('page_size', pageSize.toString());

      const response = await fetch(`/api/v1/table-assets?${params.toString()}`);

      if (!response.ok) {
        throw new Error('Failed to fetch table assets');
      }

      const result = await response.json();

      // Convert backend format to frontend format
      const items = result.items.map((item: any) => ({
        id: item.id.toString(),
        name: item.name,
        sourceSql: item.source_sql,
        database: item.database,
        schema: item.schema,
        createdAt: item.created_at,
        updatedAt: item.updated_at,
        tags: item.tags,
        owner: item.owner,
        aiSummary: item.ai_summary,
        useCases: item.use_cases,
      }));

      return {
        items,
        total: result.total,
        page: result.page,
        page_size: result.page_size,
      };
    });
  },
};
