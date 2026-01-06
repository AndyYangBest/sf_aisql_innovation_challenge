/**
 * useApiQuery - 通用 API 查询 hook
 * 处理加载状态、错误、缓存
 */

import { useState, useEffect, useCallback } from 'react';
import { ApiResponse } from '@/api/types';

interface UseApiQueryOptions<T> {
  enabled?: boolean;
  initialData?: T;
  onSuccess?: (data: T) => void;
  onError?: (error: string) => void;
}

export function useApiQuery<T>(
  queryFn: () => Promise<ApiResponse<T>>,
  deps: unknown[] = [],
  options: UseApiQueryOptions<T> = {}
) {
  const { enabled = true, initialData, onSuccess, onError } = options;
  
  const [data, setData] = useState<T | null>(initialData ?? null);
  const [error, setError] = useState<string | null>(null);
  const [isLoading, setIsLoading] = useState(enabled);

  const refetch = useCallback(async () => {
    setIsLoading(true);
    setError(null);

    const response = await queryFn();
    
    if (response.status === 'success' && response.data !== null) {
      setData(response.data);
      onSuccess?.(response.data);
    } else if (response.error) {
      setError(response.error);
      onError?.(response.error);
    }
    
    setIsLoading(false);
  }, [queryFn, onSuccess, onError]);

  useEffect(() => {
    if (enabled) {
      refetch();
    }
  }, [...deps, enabled]);

  return {
    data,
    error,
    isLoading,
    refetch,
  };
}

/**
 * useApiMutation - 通用 API 变更 hook
 */
export function useApiMutation<T, P = unknown>(
  mutationFn: (params: P) => Promise<ApiResponse<T>>,
  options: {
    onSuccess?: (data: T) => void;
    onError?: (error: string) => void;
  } = {}
) {
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const mutate = useCallback(async (params: P): Promise<T | null> => {
    setIsLoading(true);
    setError(null);

    const response = await mutationFn(params);
    
    setIsLoading(false);

    if (response.status === 'success' && response.data !== null) {
      options.onSuccess?.(response.data);
      return response.data;
    } else if (response.error) {
      setError(response.error);
      options.onError?.(response.error);
    }
    
    return null;
  }, [mutationFn, options]);

  return {
    mutate,
    isLoading,
    error,
  };
}
