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

const LOCAL_HOSTS = new Set(['localhost', '127.0.0.1', '::1']);
let cachedApiBase: string | null = null;
const FETCH_PATCH_FLAG = '__sf_ai_sql_fetch_patched__';

function isBrowser(): boolean {
  return typeof window !== 'undefined' && typeof window.location !== 'undefined';
}

function normalizeApiBase(raw: string): string {
  const trimmed = raw.trim().replace(/\/$/, '');
  if (!trimmed) {
    return '';
  }
  try {
    const url = new URL(trimmed);
    const isLocalTarget = LOCAL_HOSTS.has(url.hostname.toLowerCase());
    if (isLocalTarget && isBrowser()) {
      const currentHost = window.location.hostname.toLowerCase();
      const currentProtocol = window.location.protocol;
      // 在 HTTPS / 远程域名场景，强制走相对路径让 Vite proxy 转发，避免 localhost 被浏览器升级为 HTTPS。
      if (currentProtocol === 'https:' || !LOCAL_HOSTS.has(currentHost)) {
        return '';
      }
    }
    if (isLocalTarget) {
      url.hostname = '127.0.0.1';
      if (url.protocol === 'https:') {
        url.protocol = 'http:';
      }
    }
    const normalizedPath = url.pathname.replace(/\/$/, '');
    return `${url.origin}${normalizedPath}`;
  } catch {
    return trimmed;
  }
}

function normalizeLocalHttpsUrl(rawUrl: string): string {
  const trimmed = rawUrl.trim();
  if (!trimmed) {
    return rawUrl;
  }
  if (!/^https?:\/\//i.test(trimmed)) {
    return rawUrl;
  }
  try {
    const parsed = new URL(trimmed, globalThis.location?.origin);
    if (LOCAL_HOSTS.has(parsed.hostname.toLowerCase())) {
      parsed.hostname = '127.0.0.1';
      if (parsed.protocol === 'https:') {
        parsed.protocol = 'http:';
      }
      return parsed.toString();
    }
    return rawUrl;
  } catch {
    return rawUrl;
  }
}

function installLocalhostFetchPatch(): void {
  const state = globalThis as typeof globalThis & Record<string, unknown>;
  if (state[FETCH_PATCH_FLAG] || typeof globalThis.fetch !== 'function') {
    return;
  }
  const originalFetch = globalThis.fetch.bind(globalThis);
  const patchedFetch: typeof fetch = (input, init) => {
    const logFetchUrl = (url: string) => {
      if (import.meta.env.DEV) {
        console.info(`[api] outgoing fetch: ${url}`);
      }
    };
    if (typeof input === 'string') {
      const normalized = normalizeLocalHttpsUrl(input);
      logFetchUrl(normalized);
      return originalFetch(normalized, init);
    }
    if (input instanceof URL) {
      const normalized = normalizeLocalHttpsUrl(input.toString());
      logFetchUrl(normalized);
      return originalFetch(new URL(normalized), init);
    }
    if (input instanceof Request) {
      const normalized = normalizeLocalHttpsUrl(input.url);
      if (normalized !== input.url) {
        const rewritten = new Request(normalized, input);
        logFetchUrl(rewritten.url);
        return originalFetch(rewritten, init);
      }
      logFetchUrl(input.url);
    }
    return originalFetch(input, init);
  };
  globalThis.fetch = patchedFetch;
  state[FETCH_PATCH_FLAG] = true;
}

installLocalhostFetchPatch();

/**
 * API 请求的 base URL。开发时用相对路径走 Vite proxy；若前后端分离（例如后端单独 ngrok），
 * 在 .env 设置 VITE_API_BASE 为后端地址（如 https://xxx.ngrok-free.app）可避免 ERR_SSL_PROTOCOL_ERROR。
 */
export function getApiBase(): string {
  if (cachedApiBase !== null) {
    return cachedApiBase;
  }
  const base = import.meta.env.VITE_API_BASE;
  cachedApiBase = typeof base === 'string' ? normalizeApiBase(base) : '';
  return cachedApiBase;
}

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
