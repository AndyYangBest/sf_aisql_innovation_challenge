/**
 * API Module - 统一导出
 * 
 * 所有后端 API 服务的入口点
 * 
 * 使用方式:
 * import { tablesApi, artifactsApi, workflowsApi, aiApi } from '@/api';
 * 
 * const tables = await tablesApi.getAll();
 * const insights = await aiApi.generateInsights({ ... });
 */

// 客户端
export { supabase, apiRequest, invokeFunction, simulateDelay } from './client';

// 类型
export type { 
  ApiResponse, 
  PaginationParams, 
  PaginatedResponse, 
  ApiConfig, 
  QueryParams,
  // Tables
  GetTablesParams,
  CreateTableRequest,
  UpdateTableRequest,
  // Artifacts
  GetArtifactsParams,
  CreateArtifactRequest,
  // Workflows
  ExecuteWorkflowRequest,
  ExecuteWorkflowResponse,
  NodeExecutionRequest,
  NodeExecutionResponse,
  // AI
  GenerateInsightsRequest,
  GenerateInsightsResponse,
  RecommendChartsRequest,
  RecommendChartsResponse,
  GenerateDocRequest,
  GenerateDocResponse,
  ExplainColumnRequest,
  ExplainColumnResponse,
  // Collaboration
  Collaborator,
  InviteCollaboratorRequest,
  TokenUsage,
  UsageStats,
} from './types';

// 服务
export { tablesApi } from './tables';
export { artifactsApi, mockArtifacts } from './artifacts';
export { workflowsApi, simulateNodeExecution, getExecutionOrder } from './workflows';
export { aiApi } from './ai';
export { columnMetadataApi } from './columnMetadata';
export { columnWorkflowsApi } from './columnWorkflows';
