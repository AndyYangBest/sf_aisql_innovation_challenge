/**
 * API Client - 统一的 API 请求客户端
 * 可轻松切换到真实后端 (Supabase Edge Functions / REST API)
 */

import { supabase } from '@/integrations/supabase/client';
import { ApiResponse, ApiConfig } from './types';

// 默认配置
const defaultConfig: ApiConfig = {
  baseUrl: import.meta.env.VITE_SUPABASE_URL || '',
  timeout: 30000,
};

/**
 * Custom error class for authentication errors
 */
export class AuthenticationError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'AuthenticationError';
  }
}

/**
 * 统一的请求处理包装器
 * 将任意异步函数转换为标准 API 响应格式
 */
export async function apiRequest<T>(
  fn: () => Promise<T>,
  errorMessage = 'Request failed'
): Promise<ApiResponse<T>> {
  try {
    const data = await fn();
    return { data, error: null, status: 'success' };
  } catch (error) {
    console.error(errorMessage, error);

    // Check if this is an authentication error
    if (error instanceof AuthenticationError) {
      return {
        data: null,
        error: error.message,
        status: 'unauthorized',
      };
    }

    return {
      data: null,
      error: error instanceof Error ? error.message : errorMessage,
      status: 'error',
    };
  }
}

/**
 * Edge Function 调用封装
 * 使用 Supabase Edge Functions 作为后端
 * 
 * @example
 * const response = await invokeFunction<InsightsResponse>('generate-insights', { tableId: '1' });
 */
export async function invokeFunction<T, P = unknown>(
  functionName: string,
  payload?: P
): Promise<ApiResponse<T>> {
  return apiRequest(async () => {
    const { data, error } = await supabase.functions.invoke(functionName, {
      body: payload,
    });
    if (error) throw error;
    return data as T;
  }, `Failed to invoke ${functionName}`);
}

/**
 * 模拟延迟 - 用于开发环境模拟网络延迟
 */
export function simulateDelay(ms: number = 500): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// 导出 supabase client 供直接使用
export { supabase };
export { defaultConfig };
