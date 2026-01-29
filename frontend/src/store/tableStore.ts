import { create } from 'zustand';
import { persist } from 'zustand/middleware';
import { columnMetadataApi, ColumnMetadataRecord } from '@/api/columnMetadata';
import { tablesApi } from '@/api/tables';
import { TableAsset, TableResult, Artifact, ChangelogEntry } from '@/types';

type ReportOverrides = {
  notes?: string;
  manual_artifacts?: Artifact[];
  pinned_artifact_ids?: string[];
  hidden_artifact_ids?: string[];
};

type ReportStatus = {
  loaded: boolean;
  hasReport: boolean;
};

const NOTES_DOC_ID_PREFIX = "notes-";

const buildNotesArtifact = (tableId: string, notes: string, createdAt?: string): Artifact => ({
  type: "doc",
  id: `${NOTES_DOC_ID_PREFIX}${tableId}`,
  tableId,
  content: { markdown: notes, title: "Notes" },
  createdAt: createdAt || new Date().toISOString(),
});

const mergeArtifactsById = (artifacts: Artifact[]): Artifact[] => {
  const byId = new Map<string, Artifact>();
  artifacts.forEach((artifact) => {
    byId.set(artifact.id, artifact);
  });
  return Array.from(byId.values());
};

const buildReportArtifacts = (
  columns: ColumnMetadataRecord[],
  tableId: string,
  overrides: ReportOverrides,
): Artifact[] => {
  const manualArtifacts = Array.isArray(overrides.manual_artifacts)
    ? overrides.manual_artifacts.map((artifact) => ({
        ...artifact,
        tableId: artifact.tableId || tableId,
      }))
    : [];
  const noteArtifacts = overrides.notes ? [buildNotesArtifact(tableId, overrides.notes)] : [];
  const combined = mergeArtifactsById([...manualArtifacts, ...noteArtifacts]);
  const hidden = new Set(overrides.hidden_artifact_ids || []);
  const pinned = new Set(overrides.pinned_artifact_ids || []);

  return combined
    .filter((artifact) => !hidden.has(artifact.id))
    .map((artifact) => ({
      ...artifact,
      pinned: pinned.has(artifact.id) || artifact.pinned,
    }));
};

const extractReportOverrides = (rawOverrides: any): ReportOverrides => {
  if (!rawOverrides || typeof rawOverrides !== "object") {
    return {};
  }
  const report = rawOverrides.report;
  if (!report || typeof report !== "object") {
    return {};
  }
  return {
    notes: typeof report.notes === "string" ? report.notes : undefined,
    manual_artifacts: Array.isArray(report.manual_artifacts) ? report.manual_artifacts : undefined,
    pinned_artifact_ids: Array.isArray(report.pinned_artifact_ids)
      ? report.pinned_artifact_ids
      : undefined,
    hidden_artifact_ids: Array.isArray(report.hidden_artifact_ids)
      ? report.hidden_artifact_ids
      : undefined,
  };
};

interface TableStore {
  tableAssets: TableAsset[];
  tableResults: Record<string, TableResult>;
  artifacts: Artifact[];
  changelog: ChangelogEntry[];
  selectedColumn: string | null;
  reportOverrides: Record<string, ReportOverrides>;
  reportStatus: Record<string, ReportStatus>;
  approvedPlansByTable: Record<string, number>;

  // Actions
  addTableAsset: (asset: TableAsset) => void;
  setTableAssets: (assets: TableAsset[]) => void;
  updateTableAsset: (id: string, updates: Partial<TableAsset>) => void;
  deleteTableAsset: (id: string) => void;
  getTableResult: (id: string) => TableResult | undefined;
  setTableResult: (id: string, result: TableResult) => void;
  loadTableResult: (id: string) => Promise<void>;
  addArtifact: (artifact: Artifact, title?: string) => void;
  deleteArtifact: (id: string) => void;
  toggleArtifactPin: (id: string) => void;
  loadReport: (tableId: string) => Promise<void>;
  updateReportNotes: (tableId: string, notes: string) => Promise<void>;
  setSelectedColumn: (column: string | null) => void;
  getArtifactsByTable: (tableId: string) => Artifact[];
  getReportStatus: (tableId: string) => ReportStatus | undefined;
  getChangelog: (tableId?: string) => ChangelogEntry[];
  getApprovedPlansCount: (tableId?: string) => number;
}

export const useTableStore = create<TableStore>()(
  persist(
    (set, get) => {
      const persistReportOverrides = async (tableId: string, overrides: ReportOverrides) => {
        const tableAssetId = Number.parseInt(tableId, 10);
        if (Number.isNaN(tableAssetId)) {
          return;
        }
        const response = await columnMetadataApi.overrideTable(tableAssetId, { report: overrides });
        if (response.status === "error") {
          throw new Error(response.error || "Failed to update report");
        }
      };

      return {
        tableAssets: [],
        tableResults: {},
        artifacts: [],
        changelog: [],
        selectedColumn: null,
        reportOverrides: {},
        reportStatus: {},
        approvedPlansByTable: {},

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
          set((state) => {
            const { [id]: _unusedOverrides, ...nextOverrides } = state.reportOverrides;
            const { [id]: _unusedStatus, ...nextStatus } = state.reportStatus;
            const { [id]: _unusedPlans, ...nextApproved } = state.approvedPlansByTable;
            return {
              tableAssets: state.tableAssets.filter((asset) => asset.id !== id),
              artifacts: state.artifacts.filter((artifact) => artifact.tableId !== id),
              reportOverrides: nextOverrides,
              reportStatus: nextStatus,
              approvedPlansByTable: nextApproved,
            };
          }),

        getTableResult: (id) => get().tableResults[id],

        setTableResult: (id, result) =>
          set((state) => ({
            tableResults: { ...state.tableResults, [id]: result },
          })),

        loadTableResult: async (id) => {
          set((state) => {
            const next = { ...state.tableResults };
            delete next[id];
            return { tableResults: next };
          });
          const response = await tablesApi.getResult(id);
          if (response.status === "success" && response.data) {
            set((state) => ({
              tableResults: { ...state.tableResults, [id]: response.data },
            }));
            return;
          }
          set((state) => {
            const next = { ...state.tableResults };
            delete next[id];
            return { tableResults: next };
          });
        },

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

          const tableId = artifact.tableId;
          const currentOverrides = get().reportOverrides[tableId] || {};
          const manualArtifacts = mergeArtifactsById([
            ...(currentOverrides.manual_artifacts || []),
            artifact,
          ]);
          const pinnedIds = new Set(currentOverrides.pinned_artifact_ids || []);
          if (artifact.pinned) {
            pinnedIds.add(artifact.id);
          }

          const nextOverrides: ReportOverrides = {
            ...currentOverrides,
            manual_artifacts: manualArtifacts,
            pinned_artifact_ids: Array.from(pinnedIds),
          };

          set((state) => ({
            artifacts: [...state.artifacts, artifact],
            changelog: [entry, ...state.changelog].slice(0, 50),
            reportOverrides: { ...state.reportOverrides, [tableId]: nextOverrides },
            reportStatus: {
              ...state.reportStatus,
              [tableId]: { loaded: true, hasReport: true },
            },
          }));

          void persistReportOverrides(tableId, nextOverrides).catch((error) => {
            console.error("Failed to persist report overrides", error);
          });
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

            const tableId = artifact.tableId;
            const currentOverrides = get().reportOverrides[tableId] || {};
            const manualArtifacts = (currentOverrides.manual_artifacts || []).filter(
              (item) => item.id !== id
            );
            const hiddenIds = new Set(currentOverrides.hidden_artifact_ids || []);
            if (manualArtifacts.length === (currentOverrides.manual_artifacts || []).length) {
              hiddenIds.add(id);
            }
            const pinnedIds = new Set(currentOverrides.pinned_artifact_ids || []);
            pinnedIds.delete(id);

            const nextOverrides: ReportOverrides = {
              ...currentOverrides,
              manual_artifacts: manualArtifacts,
              hidden_artifact_ids: Array.from(hiddenIds),
              pinned_artifact_ids: Array.from(pinnedIds),
            };

            const nextArtifacts = get().artifacts.filter((item) => item.id !== id);
            const hasReport =
              nextArtifacts.some((item) => item.tableId === tableId) ||
              !!nextOverrides.notes;

            set((state) => ({
              artifacts: nextArtifacts,
              changelog: [entry, ...state.changelog].slice(0, 50),
              reportOverrides: { ...state.reportOverrides, [tableId]: nextOverrides },
              reportStatus: {
                ...state.reportStatus,
                [tableId]: { loaded: true, hasReport },
              },
            }));

            void persistReportOverrides(tableId, nextOverrides).catch((error) => {
              console.error("Failed to persist report overrides", error);
            });
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

            const tableId = artifact.tableId;
            const currentOverrides = get().reportOverrides[tableId] || {};
            const pinnedIds = new Set(currentOverrides.pinned_artifact_ids || []);
            if (artifact.pinned) {
              pinnedIds.delete(id);
            } else {
              pinnedIds.add(id);
            }

            const nextOverrides: ReportOverrides = {
              ...currentOverrides,
              pinned_artifact_ids: Array.from(pinnedIds),
            };

            set((state) => ({
              artifacts: state.artifacts.map((item) =>
                item.id === id ? { ...item, pinned: !item.pinned } : item
              ),
              changelog: [entry, ...state.changelog].slice(0, 50),
              reportOverrides: { ...state.reportOverrides, [tableId]: nextOverrides },
            }));

            void persistReportOverrides(tableId, nextOverrides).catch((error) => {
              console.error("Failed to persist report overrides", error);
            });
          }
        },

        loadReport: async (tableId: string) => {
          const tableAssetId = Number.parseInt(tableId, 10);
          if (Number.isNaN(tableAssetId)) {
            return;
          }

          let response = await columnMetadataApi.get(tableAssetId);
          if (response.status === "error") {
            throw new Error(response.error || "Failed to load report metadata");
          }
          let data = response.data;
          if (!data || data.columns.length === 0) {
            response = await columnMetadataApi.initialize(tableAssetId);
            if (response.status === "error") {
              throw new Error(response.error || "Failed to initialize metadata");
            }
            data = response.data;
          }
          if (!data) {
            return;
          }

          const overrides = extractReportOverrides(data.table?.overrides);
          const artifacts = buildReportArtifacts(data.columns, tableId, overrides);
          const hasReport = artifacts.length > 0 || !!overrides.notes;
          const approvedPlansCount = data.columns.reduce((count, column) => {
            const analysis = column.metadata?.analysis;
            if (!analysis) {
              return count;
            }
            let plan = analysis.repair_plan;
            if (typeof plan === "string") {
              try {
                plan = JSON.parse(plan);
              } catch {
                plan = null;
              }
            }
            if (!plan || typeof plan !== "object") {
              return count;
            }
            const approved = (plan as Record<string, any>).approved;
            const status = (plan as Record<string, any>).approval_status;
            return approved || status === "approved" ? count + 1 : count;
          }, 0);

          set((state) => ({
            artifacts: [
              ...state.artifacts.filter((artifact) => artifact.tableId !== tableId),
              ...artifacts,
            ],
            reportOverrides: { ...state.reportOverrides, [tableId]: overrides },
            reportStatus: {
              ...state.reportStatus,
              [tableId]: { loaded: true, hasReport },
            },
            approvedPlansByTable: {
              ...state.approvedPlansByTable,
              [tableId]: approvedPlansCount,
            },
          }));
        },

        updateReportNotes: async (tableId: string, notes: string) => {
          const trimmed = notes.trim();
          const currentOverrides = get().reportOverrides[tableId] || {};
          const nextOverrides: ReportOverrides = {
            ...currentOverrides,
            notes: trimmed || undefined,
          };

          const nextArtifacts = get()
            .artifacts
            .filter((artifact) => artifact.tableId !== tableId || !artifact.id.startsWith(NOTES_DOC_ID_PREFIX));

          if (trimmed) {
            nextArtifacts.push(buildNotesArtifact(tableId, trimmed));
          }

          const hasReport = nextArtifacts.some((artifact) => artifact.tableId === tableId);

          set((state) => ({
            artifacts: [
              ...state.artifacts.filter((artifact) => artifact.tableId !== tableId),
              ...nextArtifacts.filter((artifact) => artifact.tableId === tableId),
            ],
            reportOverrides: { ...state.reportOverrides, [tableId]: nextOverrides },
            reportStatus: {
              ...state.reportStatus,
              [tableId]: { loaded: true, hasReport },
            },
          }));

          await persistReportOverrides(tableId, nextOverrides);
        },

        setSelectedColumn: (column) => set({ selectedColumn: column }),

        getArtifactsByTable: (tableId) =>
          get().artifacts.filter((artifact) => artifact.tableId === tableId),

        getReportStatus: (tableId) => get().reportStatus[tableId],

        getChangelog: (tableId) => {
          const logs = get().changelog;
          return tableId ? logs.filter((l) => l.tableId === tableId) : logs;
        },

        getApprovedPlansCount: (tableId) => {
          const approvedPlans = get().approvedPlansByTable;
          if (tableId) {
            return approvedPlans[tableId] || 0;
          }
          return Object.values(approvedPlans).reduce((sum, value) => sum + value, 0);
        },
      };
    },
    {
      name: "table-workspace-storage",
      version: 4,
      migrate: (state: any, version: number) => {
        if (version < 3) {
          state = {
            ...state,
            tableResults: {},
            reportStatus: {},
          };
        }
        if (version < 4) {
          return {
            ...state,
            approvedPlansByTable: {},
          };
        }
        return state;
      },
    }
  )
);
