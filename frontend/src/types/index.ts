export type TableAsset = {
  id: string;
  name: string;
  sourceSql: string;
  database?: string;
  schema?: string;
  tableRef?: string;
  createdAt: string;
  updatedAt: string;
  tags: string[];
  owner?: string;
  rowCount?: number;
  aiSummary?: string;
  useCases?: string[];
};

export type ColumnInfo = {
  name: string;
  type?: string;
  nullable?: boolean;
  description?: string;
  role?: "id" | "time" | "metric" | "dimension" | "foreign_key";
  aiExplanation?: string;
};

export type TableResult = {
  columns: ColumnInfo[];
  rows: Record<string, any>[];
  rowCount?: number;
};

export type InsightArtifact = {
  type: "insight";
  id: string;
  tableId: string;
  content: { 
    title: string;
    bullets: string[]; 
    summary?: string;
    sourceColumns?: string[];
    sourceCharts?: string[];
  };
  author?: string;
  createdAt: string;
  pinned?: boolean;
};

export type ChartArtifact = {
  type: "chart";
  id: string;
  tableId: string;
  content: {
    chartType: "bar" | "line" | "pie" | "area";
    title: string;
    xKey: string;
    yKey: string;
    data: any[];
    narrative: string[];
    sourceColumns: string[];
  };
  createdAt: string;
  pinned?: boolean;
};

export type DocArtifact = {
  type: "doc";
  id: string;
  tableId: string;
  content: { markdown: string; title?: string };
  createdAt: string;
  pinned?: boolean;
};

export type AnnotationArtifact = {
  type: "annotation";
  id: string;
  tableId: string;
  content: { target: string; targetType: "column" | "chart"; note: string };
  createdAt: string;
  pinned?: boolean;
};

export type Artifact = InsightArtifact | ChartArtifact | DocArtifact | AnnotationArtifact;

export type WorkspaceTab = "overview" | "data" | "profile" | "columnmap" | "charts" | "insights" | "notes" | "lineage" | "workflow";

export type AIAction = 
  | "generate_summary"
  | "generate_insights" 
  | "recommend_charts"
  | "generate_doc"
  | "explain_column"
  | "suggest_next_steps";

export type ChangelogEntry = {
  id: string;
  action: "save_insight" | "save_chart" | "save_doc" | "delete" | "pin" | "unpin";
  artifactType: Artifact["type"];
  artifactTitle: string;
  tableId: string;
  timestamp: string;
};
