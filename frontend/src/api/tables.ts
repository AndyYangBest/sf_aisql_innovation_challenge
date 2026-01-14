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

// Mock 数据 - 后续可替换为真实 API
export const mockTableAssets: TableAsset[] = [
  {
    id: "1",
    name: "Sales Analytics Q4",
    sourceSql: "SELECT * FROM sales WHERE quarter = 'Q4' AND year = 2024",
    database: "ANALYTICS",
    schema: "PUBLIC",
    createdAt: "2024-12-20T10:00:00Z",
    updatedAt: "2024-12-22T15:30:00Z",
    tags: ["sales", "quarterly", "finance"],
    owner: "john.doe",
    aiSummary: "This table captures quarterly sales transactions across product categories and regions.",
    useCases: ["Sales Analytics", "Revenue Tracking", "Quarterly Reporting"],
  },
  {
    id: "2",
    name: "User Engagement Metrics",
    sourceSql: "SELECT user_id, session_count, avg_session_duration FROM user_metrics",
    database: "PRODUCT",
    schema: "ANALYTICS",
    createdAt: "2024-12-18T09:00:00Z",
    updatedAt: "2024-12-21T12:00:00Z",
    tags: ["users", "engagement", "product"],
    owner: "jane.smith",
    aiSummary: "User engagement data capturing session metrics and activity patterns.",
    useCases: ["Product Analytics", "User Behavior", "Retention Analysis"],
  },
  {
    id: "3",
    name: "Revenue by Region",
    sourceSql: "SELECT region, SUM(revenue) as total_revenue FROM transactions GROUP BY region",
    database: "FINANCE",
    schema: "REPORTING",
    createdAt: "2024-12-15T08:00:00Z",
    updatedAt: "2024-12-19T18:00:00Z",
    tags: ["revenue", "regional", "finance"],
    owner: "mike.wilson",
    aiSummary: "Aggregated revenue metrics by geographic region.",
    useCases: ["Regional Analysis", "Executive Reporting", "Market Planning"],
  },
];

export const mockTableResults: Record<string, TableResult> = {
  "1": {
    columns: [
      { name: "id", type: "INTEGER", role: "id" },
      { name: "product_name", type: "VARCHAR", role: "dimension" },
      { name: "category", type: "VARCHAR", role: "dimension" },
      { name: "quantity", type: "INTEGER", role: "metric" },
      { name: "unit_price", type: "DECIMAL", role: "metric" },
      { name: "total_revenue", type: "DECIMAL", role: "metric" },
      { name: "sale_date", type: "DATE", role: "time" },
      { name: "region", type: "VARCHAR", role: "dimension" },
    ],
    rows: [
      { id: 1, product_name: "Pro Dashboard", category: "Software", quantity: 150, unit_price: 299, total_revenue: 44850, sale_date: "2024-10-15", region: "North America" },
      { id: 2, product_name: "Analytics Suite", category: "Software", quantity: 89, unit_price: 499, total_revenue: 44411, sale_date: "2024-10-18", region: "Europe" },
      { id: 3, product_name: "Data Connector", category: "Integration", quantity: 245, unit_price: 149, total_revenue: 36505, sale_date: "2024-11-02", region: "Asia Pacific" },
      { id: 4, product_name: "Report Builder", category: "Software", quantity: 312, unit_price: 199, total_revenue: 62088, sale_date: "2024-11-10", region: "North America" },
    ],
    rowCount: 4,
  },
  "2": {
    columns: [
      { name: "user_id", type: "VARCHAR", role: "id" },
      { name: "username", type: "VARCHAR", role: "dimension" },
      { name: "session_count", type: "INTEGER", role: "metric" },
      { name: "avg_session_duration", type: "DECIMAL", role: "metric" },
    ],
    rows: [
      { user_id: "u001", username: "alex_dev", session_count: 45, avg_session_duration: 12.5 },
      { user_id: "u002", username: "sarah_pm", session_count: 38, avg_session_duration: 18.2 },
    ],
    rowCount: 2,
  },
  "3": {
    columns: [
      { name: "region", type: "VARCHAR", role: "dimension" },
      { name: "total_revenue", type: "DECIMAL", role: "metric" },
      { name: "transaction_count", type: "INTEGER", role: "metric" },
    ],
    rows: [
      { region: "North America", total_revenue: 2450000, transaction_count: 12500 },
      { region: "Europe", total_revenue: 1890000, transaction_count: 9800 },
    ],
    rowCount: 2,
  },
};

// API 方法
export const tablesApi = {
  // 获取所有表格
  async getAll(): Promise<ApiResponse<TableAsset[]>> {
    return apiRequest(async () => {
      // TODO: 替换为真实 API 调用
      // const { data, error } = await supabase.from('tables').select('*');
      return [...mockTableAssets];
    });
  },

  // 获取单个表格
  async getById(id: string): Promise<ApiResponse<TableAsset | null>> {
    return apiRequest(async () => {
      const asset = mockTableAssets.find(t => t.id === id);
      return asset || null;
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
      return mockTableResults[id] || null;
    });
  },

  // 创建表格
  async create(asset: Omit<TableAsset, 'id' | 'createdAt' | 'updatedAt'>): Promise<ApiResponse<TableAsset>> {
    return apiRequest(async () => {
      const newAsset: TableAsset = {
        ...asset,
        id: `${Date.now()}`,
        createdAt: new Date().toISOString(),
        updatedAt: new Date().toISOString(),
      };
      mockTableAssets.push(newAsset);
      return newAsset;
    });
  },

  // 更新表格
  async update(id: string, updates: Partial<TableAsset>): Promise<ApiResponse<TableAsset | null>> {
    return apiRequest(async () => {
      const index = mockTableAssets.findIndex(t => t.id === id);
      if (index === -1) return null;
      mockTableAssets[index] = {
        ...mockTableAssets[index],
        ...updates,
        updatedAt: new Date().toISOString(),
      };
      return mockTableAssets[index];
    });
  },

  // 删除表格
  async delete(id: string): Promise<ApiResponse<boolean>> {
    return apiRequest(async () => {
      const index = mockTableAssets.findIndex(t => t.id === id);
      if (index === -1) return false;
      mockTableAssets.splice(index, 1);
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
      if (!response.ok) {
        throw new Error('Failed to fetch Snowflake tables');
      }
      const result = await response.json();
      return result.data;
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
