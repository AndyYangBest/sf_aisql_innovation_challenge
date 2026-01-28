/**
 * EDA Workflow Types
 * EDA 工作流节点类型定义
 */

import { WorkflowNodeRegistry } from '@flowgram.ai/free-layout-editor';

// ============================================================================
// Node Types
// ============================================================================

export type EDANodeType =
  | 'data_source'
  | 'profile_table'
  | 'numeric_distribution'
  | 'numeric_correlations'
  | 'numeric_periodicity'
  | 'categorical_groups'
  | 'scan_nulls'
  | 'scan_conflicts'
  | 'plan_data_repairs'
  | 'approval_gate'
  | 'apply_data_repairs'
  | 'generate_summary'
  | 'generate_insights'
  | 'generate_charts'
  | 'generate_documentation'
  | 'generate_visuals'
  | 'agent_step'
  | 'summarize_text'
  | 'row_level_extract'
  | 'describe_images'
  | 'basic_stats'
  | 'column_hint'
  | 'export'
  | 'comment';

export type NodeStatus = 'idle' | 'running' | 'success' | 'error' | 'skipped';

// ============================================================================
// Node Definitions
// ============================================================================

export interface EDANodeDefinition {
  type: EDANodeType;
  name: string;
  description: string;
  icon: string;
  category: 'source' | 'analysis' | 'feature' | 'output';
  defaultData?: Record<string, any>;
  createsColumn?: boolean;
}

export const EDA_NODE_DEFINITIONS: Record<EDANodeType, EDANodeDefinition> = {
  data_source: {
    type: 'data_source',
    name: 'Data Source',
    description: 'Table asset to analyze',
    icon: 'Database',
    category: 'source',
    defaultData: {
      title: 'Data Source',
      table_asset_id: null,
      table_name: '',
    },
  },
  profile_table: {
    type: 'profile_table',
    name: 'Profile Table',
    description: 'Extract schema, statistics, and semantic type inference',
    icon: 'Sparkles',
    category: 'analysis',
    defaultData: {
      title: 'Profile Table',
      sample_size: 100,
      include_type_inference: true,
    },
  },
  generate_insights: {
    type: 'generate_insights',
    name: 'Generate Insights',
    description: 'Analyze data patterns and quality issues',
    icon: 'Lightbulb',
    category: 'analysis',
    defaultData: {
      title: 'Generate Insights',
      focus: 'general', // general, quality, patterns
    },
  },
  generate_summary: {
    type: 'generate_summary',
    name: 'Generate Summary',
    description: 'Create a concise AI column summary',
    icon: 'Sparkles',
    category: 'analysis',
    defaultData: {
      title: 'Generate Summary',
      focus: 'overview',
    },
  },
  generate_charts: {
    type: 'generate_charts',
    name: 'Generate Charts',
    description: 'Create visualization specifications',
    icon: 'BarChart3',
    category: 'analysis',
    defaultData: {
      title: 'Generate Charts',
      chart_count: 3,
      use_semantic_types: true,
    },
  },
  generate_documentation: {
    type: 'generate_documentation',
    name: 'Generate Documentation',
    description: 'Create comprehensive documentation',
    icon: 'FileText',
    category: 'analysis',
    defaultData: {
      title: 'Generate Documentation',
      include_summary: true,
      include_use_cases: true,
      include_recommendations: true,
    },
  },
  numeric_distribution: {
    type: 'numeric_distribution',
    name: 'Numeric Distribution',
    description: 'Compute percentiles and distribution stats',
    icon: 'Sigma',
    category: 'analysis',
    defaultData: {
      title: 'Numeric Distribution',
      sample_size: 10000,
      window_days: null,
      time_column: '',
    },
  },
  numeric_correlations: {
    type: 'numeric_correlations',
    name: 'Correlation Scan',
    description: 'Detect positive/negative correlations',
    icon: 'BarChart3',
    category: 'analysis',
    defaultData: {
      title: 'Correlation Scan',
      sample_size: 5000,
      max_columns: 12,
      window_days: null,
    },
  },
  numeric_periodicity: {
    type: 'numeric_periodicity',
    name: 'Periodicity Scan',
    description: 'Check seasonality against time buckets',
    icon: 'Clock',
    category: 'analysis',
    defaultData: {
      title: 'Periodicity Scan',
      bucket: 'day',
      sample_size: 10000,
      window_days: 180,
      time_column: '',
    },
  },
  categorical_groups: {
    type: 'categorical_groups',
    name: 'Category Groups',
    description: 'Head/tail grouping for categorical values',
    icon: 'ListTree',
    category: 'analysis',
    defaultData: {
      title: 'Category Groups',
      top_n: 10,
      sample_size: 20000,
    },
  },
  scan_nulls: {
    type: 'scan_nulls',
    name: 'Null Scan',
    description: 'Measure null counts and rates',
    icon: 'AlertTriangle',
    category: 'analysis',
    defaultData: {
      title: 'Null Scan',
      sample_size: 20000,
      window_days: null,
      time_column: '',
    },
  },
  scan_conflicts: {
    type: 'scan_conflicts',
    name: 'Conflict Scan',
    description: 'Detect conflicting values within groups',
    icon: 'GitMerge',
    category: 'analysis',
    defaultData: {
      title: 'Conflict Scan',
      group_by_columns: '',
      sample_size: 20000,
      window_days: null,
      time_column: '',
    },
  },
  plan_data_repairs: {
    type: 'plan_data_repairs',
    name: 'Repair Plan',
    description: 'Plan null/conflict fixes',
    icon: 'ClipboardList',
    category: 'analysis',
    defaultData: {
      title: 'Repair Plan',
      null_strategy: '',
      conflict_strategy: '',
      plan_id: '',
      plan_hash: '',
      snapshot_signature: '',
      plan_steps: [],
      sql_previews: {},
      rollback: {},
      row_id_column: '',
      audit_table: '',
      apply_ready: false,
    },
  },
  approval_gate: {
    type: 'approval_gate',
    name: 'Approval Gate',
    description: 'Require user approval before applying fixes',
    icon: 'ShieldCheck',
    category: 'output',
    defaultData: {
      title: 'Approval Gate',
      approved: false,
      approval_key: 'data_fix_approved',
      note: '',
      plan_id: '',
      plan_hash: '',
      snapshot_signature: '',
      plan_steps: [],
      sql_previews: {},
      rollback: {},
      row_id_column: '',
      audit_table: '',
      apply_ready: false,
    },
  },
  apply_data_repairs: {
    type: 'apply_data_repairs',
    name: 'Apply Repairs',
    description: 'Apply approved null/conflict fixes',
    icon: 'ShieldCheck',
    category: 'output',
    defaultData: {
      title: 'Apply Repairs',
      null_strategy: '',
      conflict_strategy: '',
      apply_mode: 'fixing_table',
      approval_key: 'data_fix_approved',
      plan_id: '',
      plan_hash: '',
      snapshot_signature: '',
      plan_steps: [],
      sql_previews: {},
      rollback: {},
      row_id_column: '',
      audit_table: '',
      apply_ready: false,
    },
  },
  generate_visuals: {
    type: 'generate_visuals',
    name: 'Generate Visuals',
    description: 'Create column-specific visualizations',
    icon: 'BarChart3',
    category: 'analysis',
    defaultData: {
      title: 'Generate Visuals',
      chart_type: '',
      x_column: '',
      y_column: '',
      category_limit: 'all',
      time_limit: 'all',
      time_bucket: 'day',
    },
  },
  agent_step: {
    type: 'agent_step',
    name: 'Agent Step',
    description: 'Strands agent tool call',
    icon: 'Sparkles',
    category: 'analysis',
    defaultData: {
      title: 'Agent Step',
      tool_name: '',
      tool_input: {},
    },
  },
  summarize_text: {
    type: 'summarize_text',
    name: 'Summarize Text',
    description: 'Summarize text column content',
    icon: 'FileText',
    category: 'analysis',
    defaultData: {
      title: 'Summarize Text',
    },
  },
  row_level_extract: {
    type: 'row_level_extract',
    name: 'Row-level Extract',
    description: 'AI_COMPLETE extraction per row',
    icon: 'Sparkles',
    category: 'feature',
    createsColumn: true,
    defaultData: {
      title: 'Row-level Extract',
      instruction: '',
      output_column: '',
      response_schema: '',
    },
  },
  describe_images: {
    type: 'describe_images',
    name: 'Describe Images',
    description: 'Generate per-row image descriptions',
    icon: 'Image',
    category: 'feature',
    createsColumn: true,
    defaultData: {
      title: 'Describe Images',
      output_column: '',
      image_stage: '',
      image_path_prefix: '',
      image_path_suffix: '',
      image_model: '',
    },
  },
  basic_stats: {
    type: 'basic_stats',
    name: 'Basic Stats',
    description: 'Quick stats for ID/binary/spatial columns',
    icon: 'Sigma',
    category: 'analysis',
    defaultData: {
      title: 'Basic Stats',
    },
  },
  column_hint: {
    type: 'column_hint',
    name: 'Column Hint',
    description: 'Provide a short semantic hint',
    icon: 'PencilLine',
    category: 'analysis',
    defaultData: {
      title: 'Column Hint',
      hint: '',
    },
  },
  export: {
    type: 'export',
    name: 'Export Results',
    description: 'Export analysis results',
    icon: 'Download',
    category: 'output',
    defaultData: {
      title: 'Export Results',
      format: 'json', // json, markdown, pdf
    },
  },
  comment: {
    type: 'comment',
    name: 'Comment',
    description: 'Add a note to the canvas',
    icon: 'MessageSquare',
    category: 'analysis',
    defaultData: {
      title: 'Comment',
      note: '',
    },
  },
};

// ============================================================================
// Workflow Templates
// ============================================================================

export interface WorkflowTemplate {
  id: string;
  name: string;
  description: string;
  nodes: Array<{
    id: string;
    type: EDANodeType;
    position: { x: number; y: number };
    data?: Record<string, any>;
  }>;
  edges: Array<{
    sourceNodeID: string;
    targetNodeID: string;
  }>;
}

export const EDA_WORKFLOW_TEMPLATES: Record<string, WorkflowTemplate> = {
  overview: {
    id: 'eda_overview',
    name: 'EDA Overview',
    description: 'Comprehensive analysis with profiling, insights, charts, and documentation',
    nodes: [
      {
        id: 'data_source_0',
        type: 'data_source',
        position: { x: 100, y: 200 },
      },
      {
        id: 'profile_table_0',
        type: 'profile_table',
        position: { x: 350, y: 100 },
      },
      {
        id: 'generate_insights_0',
        type: 'generate_insights',
        position: { x: 600, y: 200 },
      },
      {
        id: 'generate_charts_0',
        type: 'generate_charts',
        position: { x: 600, y: 350 },
      },
      {
        id: 'generate_documentation_0',
        type: 'generate_documentation',
        position: { x: 850, y: 275 },
      },
      {
        id: 'export_0',
        type: 'export',
        position: { x: 1100, y: 275 },
      },
    ],
    edges: [
      { sourceNodeID: 'data_source_0', targetNodeID: 'profile_table_0' },
      { sourceNodeID: 'profile_table_0', targetNodeID: 'generate_insights_0' },
      { sourceNodeID: 'profile_table_0', targetNodeID: 'generate_charts_0' },
      { sourceNodeID: 'generate_insights_0', targetNodeID: 'generate_documentation_0' },
      { sourceNodeID: 'generate_charts_0', targetNodeID: 'generate_documentation_0' },
      { sourceNodeID: 'generate_documentation_0', targetNodeID: 'export_0' },
    ],
  },
  time_series: {
    id: 'eda_time_series',
    name: 'Time Series Analysis',
    description: 'Focused on temporal patterns and trends',
    nodes: [
      {
        id: 'data_source_0',
        type: 'data_source',
        position: { x: 100, y: 200 },
      },
      {
        id: 'profile_table_0',
        type: 'profile_table',
        position: { x: 350, y: 200 },
      },
      {
        id: 'generate_charts_0',
        type: 'generate_charts',
        position: { x: 600, y: 150 },
        data: { title: 'Time Series Charts', focus: 'temporal' },
      },
      {
        id: 'generate_insights_0',
        type: 'generate_insights',
        position: { x: 600, y: 300 },
        data: { title: 'Temporal Insights', focus: 'temporal' },
      },
      {
        id: 'generate_documentation_0',
        type: 'generate_documentation',
        position: { x: 850, y: 225 },
      },
      {
        id: 'export_0',
        type: 'export',
        position: { x: 1100, y: 225 },
      },
    ],
    edges: [
      { sourceNodeID: 'data_source_0', targetNodeID: 'profile_table_0' },
      { sourceNodeID: 'profile_table_0', targetNodeID: 'generate_charts_0' },
      { sourceNodeID: 'profile_table_0', targetNodeID: 'generate_insights_0' },
      { sourceNodeID: 'generate_charts_0', targetNodeID: 'generate_documentation_0' },
      { sourceNodeID: 'generate_insights_0', targetNodeID: 'generate_documentation_0' },
      { sourceNodeID: 'generate_documentation_0', targetNodeID: 'export_0' },
    ],
  },
  data_quality: {
    id: 'eda_data_quality',
    name: 'Data Quality Check',
    description: 'Focused on validation and quality issues',
    nodes: [
      {
        id: 'data_source_0',
        type: 'data_source',
        position: { x: 100, y: 200 },
      },
      {
        id: 'profile_table_0',
        type: 'profile_table',
        position: { x: 350, y: 200 },
      },
      {
        id: 'generate_insights_0',
        type: 'generate_insights',
        position: { x: 600, y: 200 },
        data: { title: 'Quality Insights', focus: 'quality' },
      },
      {
        id: 'generate_documentation_0',
        type: 'generate_documentation',
        position: { x: 850, y: 200 },
        data: { title: 'Quality Report' },
      },
      {
        id: 'export_0',
        type: 'export',
        position: { x: 1100, y: 200 },
      },
    ],
    edges: [
      { sourceNodeID: 'data_source_0', targetNodeID: 'profile_table_0' },
      { sourceNodeID: 'profile_table_0', targetNodeID: 'generate_insights_0' },
      { sourceNodeID: 'generate_insights_0', targetNodeID: 'generate_documentation_0' },
      { sourceNodeID: 'generate_documentation_0', targetNodeID: 'export_0' },
    ],
  },
};
