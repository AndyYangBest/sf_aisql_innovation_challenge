import { create } from 'zustand';
import { persist } from 'zustand/middleware';
import { TableAsset, TableResult, Artifact, ChangelogEntry } from '@/types';

// Mock data
const mockTableAssets: TableAsset[] = [
  {
    id: "1",
    name: "Sales Analytics Q4",
    sourceSql: "SELECT * FROM sales WHERE quarter = 'Q4' AND year = 2024",
    database: "ANALYTICS",
    schema: "PUBLIC",
    createdAt: "2024-12-20T10:00:00Z",
    updatedAt: "2024-12-22T15:30:00Z",
    tags: ["sales", "quarterly", "finance"],
    owner: "john.doe",
    aiSummary: "This table captures quarterly sales transactions across product categories and regions. It's commonly used for revenue analysis, trend identification, and regional performance comparisons.",
    useCases: ["Sales Analytics", "Revenue Tracking", "Quarterly Reporting"],
  },
  {
    id: "2",
    name: "User Engagement Metrics",
    sourceSql: "SELECT user_id, session_count, avg_session_duration FROM user_metrics",
    database: "PRODUCT",
    schema: "ANALYTICS",
    createdAt: "2024-12-18T09:00:00Z",
    updatedAt: "2024-12-21T12:00:00Z",
    tags: ["users", "engagement", "product"],
    owner: "jane.smith",
    aiSummary: "User engagement data capturing session metrics, activity patterns, and retention indicators. Essential for product analytics and user behavior analysis.",
    useCases: ["Product Analytics", "User Behavior", "Retention Analysis"],
  },
  {
    id: "3",
    name: "Revenue by Region",
    sourceSql: "SELECT region, SUM(revenue) as total_revenue FROM transactions GROUP BY region",
    database: "FINANCE",
    schema: "REPORTING",
    createdAt: "2024-12-15T08:00:00Z",
    updatedAt: "2024-12-19T18:00:00Z",
    tags: ["revenue", "regional", "finance"],
    owner: "mike.wilson",
    aiSummary: "Aggregated revenue metrics by geographic region. Used for territory planning, market analysis, and executive reporting.",
    useCases: ["Regional Analysis", "Executive Reporting", "Market Planning"],
  },
];

const mockTableResults: Record<string, TableResult> = {
  "1": {
    columns: [
      { name: "id", type: "INTEGER", role: "id" },
      { name: "product_name", type: "VARCHAR", role: "dimension" },
      { name: "category", type: "VARCHAR", role: "dimension" },
      { name: "quantity", type: "INTEGER", role: "metric" },
      { name: "unit_price", type: "DECIMAL", role: "metric" },
      { name: "total_revenue", type: "DECIMAL", role: "metric" },
      { name: "sale_date", type: "DATE", role: "time" },
      { name: "region", type: "VARCHAR", role: "dimension" },
    ],
    rows: [
      { id: 1, product_name: "Pro Dashboard", category: "Software", quantity: 150, unit_price: 299, total_revenue: 44850, sale_date: "2024-10-15", region: "North America" },
      { id: 2, product_name: "Analytics Suite", category: "Software", quantity: 89, unit_price: 499, total_revenue: 44411, sale_date: "2024-10-18", region: "Europe" },
      { id: 3, product_name: "Data Connector", category: "Integration", quantity: 245, unit_price: 149, total_revenue: 36505, sale_date: "2024-11-02", region: "Asia Pacific" },
      { id: 4, product_name: "Report Builder", category: "Software", quantity: 312, unit_price: 199, total_revenue: 62088, sale_date: "2024-11-10", region: "North America" },
      { id: 5, product_name: "Cloud Storage", category: "Infrastructure", quantity: 567, unit_price: 49, total_revenue: 27783, sale_date: "2024-11-15", region: "Europe" },
      { id: 6, product_name: "API Gateway", category: "Integration", quantity: 134, unit_price: 299, total_revenue: 40066, sale_date: "2024-11-22", region: "North America" },
      { id: 7, product_name: "Security Pack", category: "Security", quantity: 78, unit_price: 599, total_revenue: 46722, sale_date: "2024-12-01", region: "Asia Pacific" },
      { id: 8, product_name: "Mobile SDK", category: "Development", quantity: 423, unit_price: 79, total_revenue: 33417, sale_date: "2024-12-05", region: "Europe" },
    ],
    rowCount: 8,
  },
  "2": {
    columns: [
      { name: "user_id", type: "VARCHAR", role: "id" },
      { name: "username", type: "VARCHAR", role: "dimension" },
      { name: "session_count", type: "INTEGER", role: "metric" },
      { name: "avg_session_duration", type: "DECIMAL", role: "metric" },
      { name: "pages_viewed", type: "INTEGER", role: "metric" },
      { name: "last_active", type: "TIMESTAMP", role: "time" },
    ],
    rows: [
      { user_id: "u001", username: "alex_dev", session_count: 45, avg_session_duration: 12.5, pages_viewed: 234, last_active: "2024-12-22T14:30:00Z" },
      { user_id: "u002", username: "sarah_pm", session_count: 38, avg_session_duration: 18.2, pages_viewed: 189, last_active: "2024-12-22T11:15:00Z" },
      { user_id: "u003", username: "mike_eng", session_count: 62, avg_session_duration: 8.7, pages_viewed: 312, last_active: "2024-12-21T22:45:00Z" },
      { user_id: "u004", username: "lisa_design", session_count: 29, avg_session_duration: 25.4, pages_viewed: 156, last_active: "2024-12-22T09:00:00Z" },
      { user_id: "u005", username: "tom_data", session_count: 51, avg_session_duration: 15.8, pages_viewed: 278, last_active: "2024-12-22T16:20:00Z" },
    ],
    rowCount: 5,
  },
  "3": {
    columns: [
      { name: "region", type: "VARCHAR", role: "dimension" },
      { name: "total_revenue", type: "DECIMAL", role: "metric" },
      { name: "transaction_count", type: "INTEGER", role: "metric" },
      { name: "avg_order_value", type: "DECIMAL", role: "metric" },
      { name: "growth_rate", type: "DECIMAL", role: "metric" },
    ],
    rows: [
      { region: "North America", total_revenue: 2450000, transaction_count: 12500, avg_order_value: 196, growth_rate: 12.5 },
      { region: "Europe", total_revenue: 1890000, transaction_count: 9800, avg_order_value: 193, growth_rate: 8.2 },
      { region: "Asia Pacific", total_revenue: 1650000, transaction_count: 11200, avg_order_value: 147, growth_rate: 22.1 },
      { region: "Latin America", total_revenue: 680000, transaction_count: 4500, avg_order_value: 151, growth_rate: 15.8 },
      { region: "Middle East", total_revenue: 420000, transaction_count: 2100, avg_order_value: 200, growth_rate: 18.4 },
    ],
    rowCount: 5,
  },
};

const mockArtifacts: Artifact[] = [
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
      narrative: ["North America accounts for the majority of Q4 revenue", "Asia Pacific shows strong growth potential"],
      sourceColumns: ["region", "total_revenue"],
    },
    createdAt: "2024-12-21T11:00:00Z",
  },
];

interface TableStore {
  tableAssets: TableAsset[];
  tableResults: Record<string, TableResult>;
  artifacts: Artifact[];
  changelog: ChangelogEntry[];
  selectedColumn: string | null;

  // Actions
  addTableAsset: (asset: TableAsset) => void;
  setTableAssets: (assets: TableAsset[]) => void;
  updateTableAsset: (id: string, updates: Partial<TableAsset>) => void;
  deleteTableAsset: (id: string) => void;
  getTableResult: (id: string) => TableResult | undefined;
  addArtifact: (artifact: Artifact, title?: string) => void;
  deleteArtifact: (id: string) => void;
  toggleArtifactPin: (id: string) => void;
  setSelectedColumn: (column: string | null) => void;
  getArtifactsByTable: (tableId: string) => Artifact[];
  getChangelog: (tableId?: string) => ChangelogEntry[];
}

export const useTableStore = create<TableStore>()(
  persist(
    (set, get) => ({
      tableAssets: mockTableAssets,
      tableResults: mockTableResults,
      artifacts: mockArtifacts,
      changelog: [],
      selectedColumn: null,

      addTableAsset: (asset) =>
        set((state) => ({
          tableAssets: [...state.tableAssets, asset],
        })),

      setTableAssets: (assets) =>
        set(() => ({
          tableAssets: assets,
        })),

      updateTableAsset: (id, updates) =>
        set((state) => ({
          tableAssets: state.tableAssets.map((asset) =>
            asset.id === id ? { ...asset, ...updates, updatedAt: new Date().toISOString() } : asset
          ),
        })),

      deleteTableAsset: (id) =>
        set((state) => ({
          tableAssets: state.tableAssets.filter((asset) => asset.id !== id),
          artifacts: state.artifacts.filter((artifact) => artifact.tableId !== id),
        })),

      getTableResult: (id) => get().tableResults[id],

      addArtifact: (artifact, title) => {
        const actionMap: Record<Artifact["type"], ChangelogEntry["action"]> = {
          insight: "save_insight",
          chart: "save_chart",
          doc: "save_doc",
          annotation: "save_insight",
        };
        const getTitle = (): string => {
          if (title) return title;
          if (artifact.type === "insight") return artifact.content.title;
          if (artifact.type === "chart") return artifact.content.title;
          if (artifact.type === "doc") return artifact.content.title || "Documentation";
          if (artifact.type === "annotation") return artifact.content.note.slice(0, 30);
          return "Untitled";
        };
        const entry: ChangelogEntry = {
          id: `log-${Date.now()}`,
          action: actionMap[artifact.type],
          artifactType: artifact.type,
          artifactTitle: getTitle(),
          tableId: artifact.tableId,
          timestamp: new Date().toISOString(),
        };
        set((state) => ({
          artifacts: [...state.artifacts, artifact],
          changelog: [entry, ...state.changelog].slice(0, 50), // Keep last 50
        }));
      },

      deleteArtifact: (id) => {
        const artifact = get().artifacts.find((a) => a.id === id);
        if (artifact) {
          const getTitle = (): string => {
            if (artifact.type === "insight") return artifact.content.title;
            if (artifact.type === "chart") return artifact.content.title;
            if (artifact.type === "doc") return artifact.content.title || "Documentation";
            if (artifact.type === "annotation") return artifact.content.note.slice(0, 30);
            return "Untitled";
          };
          const entry: ChangelogEntry = {
            id: `log-${Date.now()}`,
            action: "delete",
            artifactType: artifact.type,
            artifactTitle: getTitle(),
            tableId: artifact.tableId,
            timestamp: new Date().toISOString(),
          };
          set((state) => ({
            artifacts: state.artifacts.filter((a) => a.id !== id),
            changelog: [entry, ...state.changelog].slice(0, 50),
          }));
        } else {
          set((state) => ({
            artifacts: state.artifacts.filter((a) => a.id !== id),
          }));
        }
      },

      toggleArtifactPin: (id) => {
        const artifact = get().artifacts.find((a) => a.id === id);
        if (artifact) {
          const getTitle = (): string => {
            if (artifact.type === "insight") return artifact.content.title;
            if (artifact.type === "chart") return artifact.content.title;
            if (artifact.type === "doc") return artifact.content.title || "Documentation";
            if (artifact.type === "annotation") return artifact.content.note.slice(0, 30);
            return "Untitled";
          };
          const entry: ChangelogEntry = {
            id: `log-${Date.now()}`,
            action: artifact.pinned ? "unpin" : "pin",
            artifactType: artifact.type,
            artifactTitle: getTitle(),
            tableId: artifact.tableId,
            timestamp: new Date().toISOString(),
          };
          set((state) => ({
            artifacts: state.artifacts.map((a) =>
              a.id === id ? { ...a, pinned: !a.pinned } : a
            ),
            changelog: [entry, ...state.changelog].slice(0, 50),
          }));
        }
      },

      setSelectedColumn: (column) => set({ selectedColumn: column }),

      getArtifactsByTable: (tableId) =>
        get().artifacts.filter((artifact) => artifact.tableId === tableId),

      getChangelog: (tableId) => {
        const logs = get().changelog;
        return tableId ? logs.filter((l) => l.tableId === tableId) : logs;
      },
    }),
    {
      name: "table-workspace-storage",
    }
  )
);
