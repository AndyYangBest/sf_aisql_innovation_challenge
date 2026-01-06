/**
 * Artifacts API Service
 * Insights, Charts, Docs 等产出物的 API 操作
 */

import { Artifact, InsightArtifact, ChartArtifact } from '@/types';
import { ApiResponse } from './types';
import { apiRequest } from './client';

// Mock 数据
export const mockArtifacts: Artifact[] = [
  {
    type: "insight",
    id: "ins1",
    tableId: "1",
    content: {
      title: "Q4 Sales Performance Overview",
      summary: "Key findings from Q4 2024 sales analysis",
      bullets: [
        "North America leads with 43% of total revenue",
        "Software category dominates sales with $151K revenue",
        "Average order value increased by 12% compared to Q3",
        "Report Builder is the top-selling product with 312 units",
      ],
      sourceColumns: ["total_revenue", "region", "category"],
    },
    author: "AI Assistant",
    createdAt: "2024-12-21T10:00:00Z",
    pinned: true,
  },
  {
    type: "chart",
    id: "chart1",
    tableId: "1",
    content: {
      chartType: "bar",
      title: "Revenue by Region",
      xKey: "region",
      yKey: "total_revenue",
      data: [
        { region: "North America", total_revenue: 146004 },
        { region: "Europe", total_revenue: 105611 },
        { region: "Asia Pacific", total_revenue: 83227 },
      ],
      narrative: ["North America accounts for the majority of Q4 revenue"],
      sourceColumns: ["region", "total_revenue"],
    },
    createdAt: "2024-12-21T11:00:00Z",
  },
];

export const artifactsApi = {
  // 获取表格的所有 artifacts
  async getByTableId(tableId: string): Promise<ApiResponse<Artifact[]>> {
    return apiRequest(async () => {
      return mockArtifacts.filter(a => a.tableId === tableId);
    });
  },

  // 创建 artifact
  async create(artifact: Artifact): Promise<ApiResponse<Artifact>> {
    return apiRequest(async () => {
      mockArtifacts.push(artifact);
      return artifact;
    });
  },

  // 删除 artifact
  async delete(id: string): Promise<ApiResponse<boolean>> {
    return apiRequest(async () => {
      const index = mockArtifacts.findIndex(a => a.id === id);
      if (index === -1) return false;
      mockArtifacts.splice(index, 1);
      return true;
    });
  },

  // 切换 pin 状态
  async togglePin(id: string): Promise<ApiResponse<Artifact | null>> {
    return apiRequest(async () => {
      const artifact = mockArtifacts.find(a => a.id === id);
      if (!artifact) return null;
      artifact.pinned = !artifact.pinned;
      return artifact;
    });
  },
};
