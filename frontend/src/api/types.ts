/**
 * API Types - 统一的 API 类型定义
 * 所有后端 API 的响应和请求类型都在此定义
 */

// ============= 通用 API 类型 =============

export interface ApiResponse<T> {
  data: T | null;
  error: string | null;
  status: 'success' | 'error' | 'loading' | 'unauthorized';
}

export interface PaginationParams {
  page?: number;
  limit?: number;
  offset?: number;
}

export interface PaginatedResponse<T> {
  items: T[];
  total: number;
  page: number;
  limit: number;
  hasMore: boolean;
}

export interface ApiConfig {
  baseUrl: string;
  timeout?: number;
  headers?: Record<string, string>;
}

export interface QueryParams {
  select?: string;
  filters?: Record<string, unknown>;
  order?: { column: string; ascending?: boolean };
  limit?: number;
  offset?: number;
}

// ============= Tables API =============

export interface GetTablesParams extends PaginationParams {
  search?: string;
  tags?: string[];
  database?: string;
  owner?: string;
}

export interface CreateTableRequest {
  name: string;
  sourceSql: string;
  database?: string;
  schema?: string;
  tags?: string[];
}

export interface UpdateTableRequest {
  name?: string;
  sourceSql?: string;
  tags?: string[];
  aiSummary?: string;
  useCases?: string[];
}

// ============= Artifacts API =============

export interface GetArtifactsParams {
  tableId: string;
  type?: 'insight' | 'chart' | 'doc' | 'annotation';
  pinned?: boolean;
}

export interface CreateArtifactRequest {
  tableId: string;
  type: 'insight' | 'chart' | 'doc' | 'annotation';
  content: unknown;
  author?: string;
}

// ============= Workflows API =============

export interface ExecuteWorkflowRequest {
  workflowId: string;
  nodeId?: string;
  inputs?: Record<string, unknown>;
}

export interface ExecuteWorkflowResponse {
  success: boolean;
  outputs: Record<string, unknown>;
  errors?: Array<{
    nodeId: string;
    error: string;
    timestamp: string;
  }>;
  executionTime?: number;
}

export interface NodeExecutionRequest {
  nodeId: string;
  nodeType: string;
  inputs: Record<string, unknown>;
  config?: Record<string, unknown>;
}

export interface NodeExecutionResponse {
  output: unknown;
  metadata?: {
    executionTime: number;
    tokensUsed?: number;
  };
}

// ============= AI API =============

export interface GenerateInsightsRequest {
  tableId: string;
  columns: string[];
  sampleData: Record<string, unknown>[];
  prompt?: string;
}

export interface GenerateInsightsResponse {
  title: string;
  summary: string;
  bullets: string[];
  confidence: number;
  sourceColumns: string[];
}

export interface RecommendChartsRequest {
  tableId: string;
  columns: Array<{
    name: string;
    type: string;
    role?: string;
  }>;
  sampleData: Record<string, unknown>[];
}

export interface RecommendChartsResponse {
  charts: Array<{
    chartType: string;
    title: string;
    xKey: string;
    yKey: string;
    reasoning: string;
  }>;
}

export interface GenerateDocRequest {
  tableId: string;
  tableName: string;
  columns: Array<{
    name: string;
    type: string;
    role?: string;
  }>;
  sampleData: Record<string, unknown>[];
}

export interface GenerateDocResponse {
  title: string;
  content: string;
  sections: Array<{
    heading: string;
    content: string;
  }>;
}

export interface ExplainColumnRequest {
  tableId: string;
  columnName: string;
  columnType: string;
  sampleValues: unknown[];
}

export interface ExplainColumnResponse {
  explanation: string;
  dataType: string;
  role: string;
  suggestions: string[];
}

// ============= Collaboration API =============

export interface Collaborator {
  id: string;
  name: string;
  email: string;
  status: 'online' | 'idle' | 'offline';
  color: string;
  avatar?: string;
}

export interface InviteCollaboratorRequest {
  email: string;
  tableId: string;
  role: 'viewer' | 'editor' | 'admin';
}

// ============= Analytics API =============

export interface TokenUsage {
  context: number;
  output: number;
  total: number;
}

export interface UsageStats {
  totalQueries: number;
  totalInsights: number;
  totalCharts: number;
  tokenUsage: TokenUsage;
}
