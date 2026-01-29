/**
 * Usage API Service
 * Snowflake credits usage
 */

import { apiRequest } from "./client";
import type { ApiResponse } from "./types";

export interface CreditUsageDay {
  day: string;
  credits_used: number;
}

export interface CreditUsageResponse {
  days: number;
  total_credits: number;
  by_day: CreditUsageDay[];
}

export const usageApi = {
  async getCreditUsage(days: number = 7): Promise<ApiResponse<CreditUsageResponse>> {
    return apiRequest(async () => {
      const response = await fetch(`/api/v1/usage/credits?days=${encodeURIComponent(days)}`);
      if (!response.ok) {
        const text = await response.text().catch(() => "");
        throw new Error(text || "Failed to fetch credit usage");
      }
      const result = await response.json();
      return result.data as CreditUsageResponse;
    });
  },
};

