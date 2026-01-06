/**
 * AI API Service
 * AI 功能相关的 API 操作
 * 
 * 后端 Endpoints:
 * - POST /ai/insights          - 生成数据洞察
 * - POST /ai/charts            - 推荐图表类型
 * - POST /ai/documentation     - 生成文档
 * - POST /ai/explain-column    - 解释列含义
 * - POST /ai/chat              - AI 对话
 */

import { 
  ApiResponse,
  GenerateInsightsRequest,
  GenerateInsightsResponse,
  RecommendChartsRequest,
  RecommendChartsResponse,
  GenerateDocRequest,
  GenerateDocResponse,
  ExplainColumnRequest,
  ExplainColumnResponse,
} from './types';
import { apiRequest, invokeFunction, simulateDelay } from './client';

// ============= API Methods =============

export const aiApi = {
  /**
   * 生成数据洞察
   * POST /ai/insights
   * 
   * 可使用 Edge Function 调用 AI 服务:
   * return invokeFunction<GenerateInsightsResponse>('generate-insights', request);
   */
  async generateInsights(request: GenerateInsightsRequest): Promise<ApiResponse<GenerateInsightsResponse>> {
    return apiRequest(async () => {
      await simulateDelay(1500);
      
      // Mock response - 替换为真实 AI 调用
      return {
        title: 'Data Analysis Insights',
        summary: 'Key findings from your data analysis',
        bullets: [
          'Strong correlation found between key metrics',
          'Seasonal patterns detected in the data',
          'Outliers identified in specific regions',
          'Growth trend observed over the analysis period',
        ],
        confidence: 0.85,
        sourceColumns: request.columns,
      };
    });
  },

  /**
   * 推荐图表类型
   * POST /ai/charts
   */
  async recommendCharts(request: RecommendChartsRequest): Promise<ApiResponse<RecommendChartsResponse>> {
    return apiRequest(async () => {
      await simulateDelay(1000);
      
      const metrics = request.columns.filter(c => c.role === 'metric');
      const dimensions = request.columns.filter(c => c.role === 'dimension');
      
      const charts: RecommendChartsResponse['charts'] = [];
      
      if (dimensions.length > 0 && metrics.length > 0) {
        charts.push({
          chartType: 'bar',
          title: `${metrics[0].name} by ${dimensions[0].name}`,
          xKey: dimensions[0].name,
          yKey: metrics[0].name,
          reasoning: 'Bar chart is ideal for comparing values across categories',
        });
      }
      
      if (metrics.length >= 2) {
        charts.push({
          chartType: 'scatter',
          title: `${metrics[0].name} vs ${metrics[1].name}`,
          xKey: metrics[0].name,
          yKey: metrics[1].name,
          reasoning: 'Scatter plot shows correlation between two numeric variables',
        });
      }
      
      const timeCol = request.columns.find(c => c.role === 'time');
      if (timeCol && metrics.length > 0) {
        charts.push({
          chartType: 'line',
          title: `${metrics[0].name} Over Time`,
          xKey: timeCol.name,
          yKey: metrics[0].name,
          reasoning: 'Line chart is best for showing trends over time',
        });
      }
      
      return { charts };
    });
  },

  /**
   * 生成文档
   * POST /ai/documentation
   */
  async generateDocumentation(request: GenerateDocRequest): Promise<ApiResponse<GenerateDocResponse>> {
    return apiRequest(async () => {
      await simulateDelay(2000);
      
      return {
        title: `${request.tableName} Documentation`,
        content: `Comprehensive documentation for ${request.tableName}`,
        sections: [
          {
            heading: 'Overview',
            content: `This table contains ${request.columns.length} columns and provides data for analysis.`,
          },
          {
            heading: 'Column Definitions',
            content: request.columns.map(c => `- **${c.name}** (${c.type}): ${c.role || 'general'} field`).join('\n'),
          },
          {
            heading: 'Usage Guidelines',
            content: 'This data can be used for various analytics and reporting purposes.',
          },
        ],
      };
    });
  },

  /**
   * 解释列含义
   * POST /ai/explain-column
   */
  async explainColumn(request: ExplainColumnRequest): Promise<ApiResponse<ExplainColumnResponse>> {
    return apiRequest(async () => {
      await simulateDelay(800);
      
      return {
        explanation: `The ${request.columnName} column appears to contain ${request.columnType.toLowerCase()} values.`,
        dataType: request.columnType,
        role: 'dimension',
        suggestions: [
          'Consider using this column for grouping',
          'May be useful for filtering',
          'Could be a good candidate for visualization',
        ],
      };
    });
  },

  /**
   * AI 对话
   * POST /ai/chat
   */
  async chat(message: string, context?: Record<string, unknown>): Promise<ApiResponse<string>> {
    return apiRequest(async () => {
      await simulateDelay(1000);
      
      // Mock response - 替换为真实 AI 调用
      return `I understand you want to know about: "${message}". Based on the data context, here are my observations...`;
    });
  },
};
