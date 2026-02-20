import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { BarChart3, Lightbulb, Loader2, Save, Sparkles, X, ShieldCheck } from "lucide-react";
import { Button } from "@/components/ui/button";
import {
  Drawer,
  DrawerContent,
  DrawerHeader,
  DrawerTitle,
} from "@/components/ui/drawer";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Badge } from "@/components/ui/badge";
import { useTableStore } from "@/store/tableStore";
import { useToast } from "@/hooks/use-toast";
import { useBreakpoint } from "@/hooks/useBreakpoint";
import { WorkspaceTab, Artifact, ChartArtifact, InsightArtifact } from "@/types";
import { columnMetadataApi, ColumnMetadataRecord } from "@/api/columnMetadata";
import {
  AIFloatingButton,
  AIChangelogPopover,
} from "./ai-panel";
import { cn } from "@/lib/utils";
import RepairApprovalDialog, {
  RepairPlanItem,
} from "@/components/workspace/RepairApprovalDialog";

interface AIActionsPanelProps {
  tableId: string;
  activeTab: WorkspaceTab;
  isOpen: boolean;
  onToggle: () => void;
}

type OutputGroup = {
  columnName: string;
  charts: ChartArtifact[];
  insights: InsightArtifact[];
  features: FeatureOutput[];
  repairs: RepairPlanOutput[];
  runKey?: string;
  visualSelection?: {
    selectedCount?: number;
    total?: number;
    rationale?: string;
  };
};

type FeatureOutput = {
  id: string;
  outputColumn: string;
  sourceColumn: string;
  kind: string;
  instruction?: string;
  model?: string;
};

type RepairPlanOutput = {
  id: string;
  columnName: string;
  tableId: string;
  plan: Record<string, any>;
  conflicts?: Record<string, any>;
};

const buildRepairPlanArtifact = (repair: RepairPlanOutput, runKey?: string): Artifact => {
  const plan = repair.plan || {};
  const planKey =
    plan.plan_hash || plan.snapshot?.signature || plan.plan_id || "latest";
  const suffix = runKey ? `::${runKey}` : "";
  const steps = Array.isArray(plan.steps) ? plan.steps : [];
  const sqlPreviews = plan.sql_previews || {};
  const rollback = plan.rollback || {};
  const snapshot = plan.snapshot || {};
  const lines = [
    `**Column:** ${repair.columnName}`,
    plan.summary ? `**Summary:** ${plan.summary}` : null,
    plan.plan_id ? `**Plan ID:** ${plan.plan_id}` : null,
    plan.plan_hash ? `**Plan Hash:** ${plan.plan_hash}` : null,
    snapshot.signature ? `**Snapshot:** ${snapshot.signature}` : null,
    snapshot.total_count ? `**Snapshot Rows:** ${snapshot.total_count}` : null,
    plan.token_estimate?.token_count
      ? `**Token Estimate:** ${plan.token_estimate.token_count}`
      : null,
  ].filter(Boolean) as string[];

  if (steps.length > 0) {
    lines.push("**Steps:**");
    steps.forEach((step: any) => {
      if (step.type === "null_repair") {
        lines.push(
          `- Null repair (${step.strategy}) · ~${step.estimated_rows ?? 0} rows`
        );
      } else if (step.type === "conflict_repair") {
        lines.push(
          `- Conflict repair (${step.strategy}) · ${step.estimated_groups ?? 0} groups`
        );
      } else {
        lines.push(`- ${step.type}`);
      }
    });
  }

  if (sqlPreviews?.null_repair?.update_sql) {
    lines.push("**Null Repair SQL Preview:**");
    lines.push("```sql");
    lines.push(sqlPreviews.null_repair.update_sql);
    lines.push("```");
  }
  if (sqlPreviews?.conflict_repair?.update_sql) {
    lines.push("**Conflict Repair SQL Preview:**");
    lines.push("```sql");
    lines.push(sqlPreviews.conflict_repair.update_sql);
    lines.push("```");
  }
  if (rollback?.strategy) {
    lines.push(`**Rollback Strategy:** ${rollback.strategy}`);
    if (rollback.audit_table) {
      lines.push(`**Audit Table:** ${rollback.audit_table}`);
    }
  }

  return {
    type: "doc",
    id: `repair_plan_${repair.tableId}_${repair.columnName}_${planKey}${suffix}`,
    tableId: repair.tableId,
    content: {
      title: `${repair.columnName} repair plan`,
      markdown: lines.join("\n"),
    },
    createdAt: new Date().toISOString(),
  };
};

const normalizeChartType = (value?: string): ChartArtifact["content"]["chartType"] => {
  if (!value) {
    return "bar";
  }
  const normalized = value.toLowerCase().replace(/[^a-z]/g, "");
  if (normalized.includes("heatmap") || normalized.includes("matrix")) {
    return "heatmap";
  }
  if (normalized.includes("pie") || normalized.includes("donut")) {
    return "pie";
  }
  if (normalized.includes("line")) {
    return "line";
  }
  if (normalized.includes("area")) {
    return "area";
  }
  if (normalized.includes("bar") || normalized.includes("hist")) {
    return "bar";
  }
  return "bar";
};

const stripBullet = (text: string) =>
  text.replace(/^\s*[-•*]\s+/, "").trim();

const isJunkInsight = (text: string) => {
  const cleaned = stripBullet(text).trim();
  if (!cleaned) return true;
  if (/^\{/.test(cleaned) || /^\[/.test(cleaned)) return true;
  if (/\"insights\"\s*:/.test(cleaned)) return true;
  if (/^\]$/.test(cleaned) || /^\}$/.test(cleaned)) return true;
  return false;
};

const normalizeInsightItems = (items: string[]) =>
  items
    .map((item) => stripBullet(String(item)))
    .filter((item) => !isJunkInsight(item));

const dedupeInsights = (items: string[]) => {
  const seen = new Set<string>();
  const result: string[] = [];
  items.forEach((item) => {
    const normalized = stripBullet(String(item || "")).trim();
    if (!normalized) return;
    const key = normalized.toLowerCase().replace(/\s+/g, " ");
    if (seen.has(key)) return;
    seen.add(key);
    result.push(normalized);
  });
  return result;
};

const parseInsightsFromString = (text: string): string[] => {
  let cleaned = text.trim();
  if (cleaned.startsWith("```")) {
    cleaned = cleaned.replace(/^```[a-zA-Z]*\s*/, "");
    cleaned = cleaned.replace(/```$/, "");
  }
  cleaned = cleaned.replace(/^\s*[-•*]\s+/, "").trim();
  const tryParse = (value: string) => {
    try {
      return JSON.parse(value);
    } catch {
      return null;
    }
  };
  const parsed = tryParse(cleaned);
  if (parsed) {
    if (Array.isArray(parsed)) return parsed.map(String);
    if (typeof parsed === "string") return [parsed];
    if (parsed.insights) return extractInsightBullets(parsed.insights);
    if (Array.isArray(parsed.key_findings)) return parsed.key_findings.map(String);
  }
  const start = cleaned.indexOf("{");
  const end = cleaned.lastIndexOf("}");
  if (start >= 0 && end > start) {
    const sliced = cleaned.slice(start, end + 1);
    const slicedParsed = tryParse(sliced);
    if (slicedParsed) {
      if (Array.isArray(slicedParsed)) return slicedParsed.map(String);
      if (typeof slicedParsed === "string") return [slicedParsed];
      if (slicedParsed.insights) return extractInsightBullets(slicedParsed.insights);
      if (Array.isArray(slicedParsed.key_findings))
        return slicedParsed.key_findings.map(String);
    }
  }
  return [];
};

const extractInsightBullets = (payload: any): string[] => {
  if (!payload) return [];
  if (Array.isArray(payload)) return normalizeInsightItems(payload.map(String));
  if (typeof payload === "string") {
    const parsed = parseInsightsFromString(payload);
    if (parsed.length > 0) return normalizeInsightItems(parsed);
    return normalizeInsightItems([payload]);
  }
  if (payload.insights) return extractInsightBullets(payload.insights);
  if (Array.isArray(payload.key_findings))
    return normalizeInsightItems(payload.key_findings.map(String));
  return [];
};

const extractSeriesKeys = (visual: any): string[] => {
  const series = Array.isArray(visual?.series) ? visual.series : [];
  const highlight = series
    .filter((item: any) => item?.key && item?.highlight)
    .map((item: any) => String(item.key));
  if (highlight.length) return highlight;
  const keys = series
    .filter((item: any) => item?.key)
    .map((item: any) => String(item.key));
  if (keys.length) return keys;
  if (visual?.yKey) return [String(visual.yKey)];
  if (visual?.y_key) return [String(visual.y_key)];
  return [];
};

const extractNumericSeries = (data: any[], key: string): number[] => {
  if (!Array.isArray(data)) return [];
  const values: number[] = [];
  data.forEach((row) => {
    if (!row || typeof row !== "object") return;
    const raw = row[key];
    if (raw === null || raw === undefined || typeof raw === "boolean") return;
    if (typeof raw === "number") {
      values.push(raw);
      return;
    }
    const parsed = Number(raw);
    if (!Number.isNaN(parsed)) values.push(parsed);
  });
  return values;
};

const computeTrend = (values: number[]): "upward" | "downward" | "flat" => {
  if (values.length < 2) return "flat";
  const start = values[0];
  const end = values[values.length - 1];
  if (start === end) return "flat";
  const delta = end - start;
  if (Math.abs(delta) < Math.max(Math.abs(start), 1) * 0.02) return "flat";
  return delta > 0 ? "upward" : "downward";
};

const buildVisualInsightFallback = (visual: any, columnName: string): string | undefined => {
  const title = String(visual?.title || columnName || "Chart");
  const chartType = String(visual?.chartType || visual?.chart_type || "bar").toLowerCase();
  const data = Array.isArray(visual?.data) ? visual.data : [];
  const xKey = visual?.xKey || visual?.x_key;
  if (chartType === "heatmap") {
    const valueKey =
      visual?.valueKey ||
      visual?.value_key ||
      (Array.isArray(visual?.series) && visual.series[0]?.key) ||
      "correlation";
    const values = extractNumericSeries(data, String(valueKey));
    if (values.length === 0) {
      return `${title} summarizes pairwise relationships but has no valid correlation values.`;
    }
    const minVal = Math.min(...values);
    const maxVal = Math.max(...values);
    return `${title} shows pairwise correlations ranging from ${minVal.toFixed(3)} to ${maxVal.toFixed(3)}.`;
  }
  const seriesKeys = extractSeriesKeys(visual);
  const primaryKey = seriesKeys[0];
  if (!primaryKey || data.length === 0) {
    return `${title} summarizes ${columnName} but lacks enough numeric points for detail.`;
  }
  const values = extractNumericSeries(data, primaryKey);
  if (values.length === 0) {
    return `${title} summarizes ${columnName} but lacks enough numeric points for detail.`;
  }
  const minVal = Math.min(...values);
  const maxVal = Math.max(...values);
  if (title.toLowerCase().includes("correlation") || String(primaryKey).toLowerCase() === "correlation") {
    if (xKey && data.length === values.length) {
      const maxIdx = values.indexOf(maxVal);
      const minIdx = values.indexOf(minVal);
      const maxLabel = data[maxIdx]?.[xKey];
      const minLabel = data[minIdx]?.[xKey];
      if (maxLabel !== undefined && minLabel !== undefined) {
        return `Correlations span ${minVal.toFixed(3)} to ${maxVal.toFixed(3)}; strongest positive is ${maxLabel}, strongest negative is ${minLabel}.`;
      }
    }
    return `Correlations span ${minVal.toFixed(3)} to ${maxVal.toFixed(3)}, highlighting the most related peers.`;
  }
  if (chartType === "line" || chartType === "area") {
    const trend = computeTrend(values);
    return `${title} ranges from ${minVal.toFixed(2)} to ${maxVal.toFixed(2)} across ${values.length} points, with an overall ${trend} trend.`;
  }
  if (chartType === "bar") {
    if (xKey && data.length === values.length) {
      const maxIdx = values.indexOf(maxVal);
      const maxLabel = data[maxIdx]?.[xKey];
      if (maxLabel !== undefined) {
        return `${title} spans ${minVal.toFixed(2)}-${maxVal.toFixed(2)}; highest category is ${maxLabel}.`;
      }
    }
    return `${title} spans ${minVal.toFixed(2)}-${maxVal.toFixed(2)} across categories.`;
  }
  return `${title} ranges from ${minVal.toFixed(2)} to ${maxVal.toFixed(2)} across ${values.length} points.`;
};

const buildOutputGroups = (columns: ColumnMetadataRecord[], tableId: string): OutputGroup[] => {
  return columns
    .map((column) => {
      const analysis = column.metadata?.analysis as Record<string, any> | undefined;
      if (!analysis) return null;

      const createdAt = column.last_updated || column.updated_at || new Date().toISOString();
      const workflowMeta = column.metadata?.workflow as Record<string, any> | undefined;
      const runKeyParts = [
        workflowMeta?.workflow_id,
        workflowMeta?.last_run_at,
        column.last_updated,
        column.updated_at,
        createdAt,
      ].filter((value) => Boolean(value));
      const runKey = runKeyParts.length > 0 ? runKeyParts.join("::") : "";
      const withRunKey = (id: string) => (runKey ? `${id}::${runKey}` : id);
      const visuals = Array.isArray(analysis.visuals) ? analysis.visuals : [];
      const visualIdSet = new Set<string>(
        visuals
          .map((visual: any) => String(visual?.id || ""))
          .filter((id: string) => id.length > 0)
      );
      const selectedIdsRaw = Array.isArray((analysis as any)?.visual_selection?.selected_ids)
        ? (analysis as any).visual_selection.selected_ids
        : [];
      const selectionIds = new Set<string>(
        selectedIdsRaw
          .map((id: any) => String(id))
          .filter((id: string) => visualIdSet.has(id))
      );
      const visualInsights = Array.isArray((analysis as any)?.visual_insights)
        ? (analysis as any).visual_insights
        : [];
      const visualInsightMap = new Map<string, string>();
      visualInsights.forEach((item: any) => {
        if (!item || typeof item !== "object") return;
        const visualId = item.visual_id || item.visualId || item.id || item.chart_id;
        const insight = item.insight;
        if (visualId && insight) {
          visualInsightMap.set(String(visualId), String(insight));
        }
      });
      const selectedVisuals = visuals.filter((visual: any) => {
        const visualId = String(visual?.id || "");
        return visual?.selected || selectionIds.has(visualId);
      });
      const aiSelectedIdSet = new Set<string>(
        selectedVisuals
          .map((visual: any) => String(visual?.id || ""))
          .filter((id: string) => id.length > 0)
      );
      // Always keep all generated visuals available to avoid hiding non-selected charts.
      // If AI made selections, surface them first but do not drop the remaining charts.
      const effectiveVisuals =
        aiSelectedIdSet.size > 0
          ? [...visuals].sort((left: any, right: any) => {
              const leftPicked =
                left?.selected || aiSelectedIdSet.has(String(left?.id || ""));
              const rightPicked =
                right?.selected || aiSelectedIdSet.has(String(right?.id || ""));
              if (leftPicked === rightPicked) return 0;
              return leftPicked ? -1 : 1;
            })
          : visuals;
      const charts: ChartArtifact[] = effectiveVisuals.map((visual: any, idx: number) => {
        const visualInsight =
          visual?.insight ??
          visualInsightMap.get(String(visual.id)) ??
          buildVisualInsightFallback(visual, column.column_name);
        return {
          type: "chart",
          id: withRunKey(visual.id || `chart_${tableId}_${column.column_name}_${idx}`),
          tableId,
          content: {
            chartType: normalizeChartType(visual.chartType || visual.chart_type),
            title: visual.title || `${column.column_name} chart`,
            xKey: visual.xKey || visual.x_key || "x",
            yKey: visual.yKey || visual.y_key || "y",
            valueKey: visual.valueKey || visual.value_key,
            xTitle: visual.xTitle || visual.x_title || visual.xKey || visual.x_key,
            yTitle: visual.yTitle || visual.y_title || visual.yKey || visual.y_key,
            yScale: visual.yScale || visual.y_scale,
            data: Array.isArray(visual.data) ? visual.data : [],
            narrative: Array.isArray(visual.narrative) ? visual.narrative : [],
            series: Array.isArray(visual.series) ? visual.series : undefined,
            sourceColumns: Array.isArray(visual.sourceColumns)
              ? visual.sourceColumns
              : [column.column_name],
            insight: visualInsight,
            aiSelected:
              visual?.selected || selectionIds.has(String(visual?.id || "")),
          },
          createdAt,
        };
      });

      const columnSummaryText =
        typeof analysis.summary === "string" ? stripBullet(analysis.summary) : "";
      const summaryKeyPoints = Array.isArray(analysis.summary_key_points)
        ? normalizeInsightItems(analysis.summary_key_points.map(String))
        : [];
      const summaryRisks = Array.isArray(analysis.summary_risks)
        ? normalizeInsightItems(analysis.summary_risks.map(String))
        : [];
      let bullets = extractInsightBullets(analysis.insights);
      if (
        !bullets.length &&
        typeof analysis.agent_summary === "string" &&
        analysis.agent_summary.trim()
      ) {
        bullets = [analysis.agent_summary.trim()];
      }
      bullets = dedupeInsights(bullets);
      const caveats = Array.isArray(analysis.caveats)
        ? analysis.caveats
        : Array.isArray((analysis.insights as any)?.caveats)
          ? (analysis.insights as any).caveats
          : [];

      const insights: InsightArtifact[] = [];
      const overviewBullets = dedupeInsights(
        normalizeInsightItems(
          [columnSummaryText, ...summaryKeyPoints].filter((item) => String(item).trim())
        )
      ).slice(0, 4);
      if (overviewBullets.length > 0) {
        insights.push({
          type: "insight",
          id: withRunKey(`insight_${tableId}_${column.column_name}_overview`),
          tableId,
          content: {
            title: `${column.column_name} overview`,
            bullets: overviewBullets,
            summary:
              summaryRisks.length > 0
                ? summaryRisks.join(" ")
                : caveats.length > 0
                  ? caveats.join(" ")
                  : undefined,
            sourceColumns: [column.column_name],
          },
          createdAt,
        });
      }
      const overviewSet = new Set(
        overviewBullets.map((item) => item.toLowerCase().replace(/\s+/g, " "))
      );
      const detailBullets = bullets.filter(
        (item) => !overviewSet.has(item.toLowerCase().replace(/\s+/g, " "))
      );
      detailBullets.forEach((bullet: string, idx: number) => {
        insights.push({
          type: "insight",
          id: withRunKey(`insight_${tableId}_${column.column_name}_${idx}`),
          tableId,
          content: {
            title: `${column.column_name} insight ${idx + 1}`,
            bullets: [bullet],
            sourceColumns: [column.column_name],
          },
          createdAt,
        });
      });

      const featureOutputs: FeatureOutput[] = [];
      if (Array.isArray(analysis.feature_outputs)) {
        analysis.feature_outputs.forEach((item: any, idx: number) => {
          if (!item?.output_column) return;
          featureOutputs.push({
            id: item.id || `feature_${tableId}_${column.column_name}_${idx}`,
            outputColumn: item.output_column,
            sourceColumn: item.source_column || column.column_name,
            kind: item.type || 'feature',
            instruction: item.instruction,
            model: item.model,
          });
        });
      } else if (analysis.row_level_output) {
        featureOutputs.push({
          id: `feature_${tableId}_${column.column_name}_row_extract`,
          outputColumn: analysis.row_level_output,
          sourceColumn: column.column_name,
          kind: 'row_level_extract',
          instruction: column.overrides?.row_level_instruction,
        });
      } else if (analysis.image_descriptions_column) {
        featureOutputs.push({
          id: `feature_${tableId}_${column.column_name}_image_desc`,
          outputColumn: analysis.image_descriptions_column,
          sourceColumn: column.column_name,
          kind: 'image_description',
        });
      }

      const repairs: RepairPlanOutput[] = [];
      const plan = analysis.repair_plan;
      const steps = Array.isArray(plan?.steps) ? plan.steps : [];
      const summaryText = typeof plan?.summary === "string" ? plan.summary : "";
      const hasActions =
        steps.length > 0 ||
        (summaryText && !summaryText.toLowerCase().includes("no repair actions"));
      if (plan && hasActions) {
        repairs.push({
          id: plan.plan_id || `repair_${tableId}_${column.column_name}`,
          columnName: column.column_name,
          tableId,
          plan,
          conflicts: analysis.conflicts,
        });
      }

      if (
        charts.length === 0 &&
        insights.length === 0 &&
        featureOutputs.length === 0 &&
        repairs.length === 0
      ) {
        return null;
      }

      return {
        columnName: column.column_name,
        charts,
        insights,
        features: featureOutputs,
        repairs,
        runKey,
        visualSelection: analysis?.visual_selection
          ? {
              selectedCount: selectionIds.size || undefined,
              total: visuals.length || analysis.visual_selection.total,
              rationale: analysis.visual_selection.rationale,
            }
          : undefined,
      };
    })
    .filter(Boolean) as OutputGroup[];
};

const featureToArtifact = (feature: FeatureOutput, tableId: string, runKey?: string): Artifact => {
  const suffix = runKey ? `::${runKey}` : "";
  const title = `${feature.outputColumn} (derived)`;
  const lines = [
    `**Output column:** ${feature.outputColumn}`,
    `**Source column:** ${feature.sourceColumn}`,
    `**Type:** ${feature.kind}`,
  ];
  if (feature.instruction) {
    lines.push(`**Instruction:** ${feature.instruction}`);
  }
  if (feature.model) {
    lines.push(`**Model:** ${feature.model}`);
  }
  return {
    type: "doc",
    id: `${feature.id}${suffix}`,
    tableId,
    content: {
      title,
      markdown: lines.join("\n\n"),
    },
    createdAt: new Date().toISOString(),
  };
};

const AIActionsPanel = ({ tableId, activeTab, isOpen, onToggle }: AIActionsPanelProps) => {
  const [outputGroups, setOutputGroups] = useState<OutputGroup[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [repairDialogOpen, setRepairDialogOpen] = useState(false);
  const [activeRepair, setActiveRepair] = useState<RepairPlanItem | null>(null);
  const { addArtifact, getChangelog } = useTableStore();
  const artifacts = useTableStore((s) => s.artifacts);
  const { toast } = useToast();
  const { isMobile } = useBreakpoint();
  const changelog = getChangelog(tableId);
  const prevHasPendingRef = useRef<boolean | null>(null);

  const savedIds = useMemo(() => {
    return new Set(artifacts.filter((a) => a.tableId === tableId).map((a) => a.id));
  }, [artifacts, tableId]);

  const toRepairPlanItem = useCallback(
    (repair: RepairPlanOutput): RepairPlanItem => ({
      columnName: repair.columnName,
      plan: repair.plan || {},
      conflicts: repair.conflicts,
    }),
    []
  );

  const refreshOutputs = useCallback(
    async (force = false) => {
      if (!tableId || (!isOpen && !force)) return;
      setIsLoading(true);
      setError(null);

      try {
        const response = await columnMetadataApi.get(Number(tableId));
        if (response.status === "success" && response.data) {
          const groups = buildOutputGroups(response.data.columns, tableId);
          setOutputGroups(groups);
        } else {
          setError(response.error || "Failed to load workflow outputs");
        }
      } catch (err) {
        setError(err instanceof Error ? err.message : "Failed to load workflow outputs");
      } finally {
        setIsLoading(false);
      }
    },
    [isOpen, tableId]
  );

  const hasPendingOutputs = useMemo(() => {
    if (outputGroups.length === 0) {
      return false;
    }
    return outputGroups.some((group) => {
      const pendingCharts = group.charts.filter((item) => !savedIds.has(item.id)).length;
      const pendingInsights = group.insights.filter((item) => !savedIds.has(item.id)).length;
      const pendingRepairs = group.repairs.filter(
        (repair) => !savedIds.has(buildRepairPlanArtifact(repair, group.runKey).id)
      ).length;
      const pendingFeatures = group.features.filter(
        (feature) => !savedIds.has(featureToArtifact(feature, tableId, group.runKey).id)
      ).length;
      return pendingCharts + pendingInsights + pendingRepairs + pendingFeatures > 0;
    });
  }, [outputGroups, savedIds, tableId]);

  useEffect(() => {
    void refreshOutputs();
  }, [activeTab, refreshOutputs]);

  useEffect(() => {
    if (isOpen) {
      void refreshOutputs(true);
    }
  }, [isOpen, refreshOutputs]);

  useEffect(() => {
    if (isOpen) {
      prevHasPendingRef.current = null;
    }
  }, [isOpen]);

  useEffect(() => {
    if (!isOpen || repairDialogOpen) return;
    if (isLoading || error || outputGroups.length === 0) {
      prevHasPendingRef.current = hasPendingOutputs;
      return;
    }
    const prev = prevHasPendingRef.current;
    prevHasPendingRef.current = hasPendingOutputs;
    if (prev === null) return;
    if (prev && !hasPendingOutputs) {
      onToggle();
    }
  }, [error, hasPendingOutputs, isLoading, isOpen, onToggle, outputGroups.length, repairDialogOpen]);

  useEffect(() => {
    if (typeof window === "undefined") return;
    const handler = (event: Event) => {
      const detail = (event as CustomEvent<{ tableAssetId?: number }>).detail;
      if (!detail?.tableAssetId) return;
      if (Number(tableId) !== detail.tableAssetId) return;
      void refreshOutputs(true);
    };
    window.addEventListener("workflow-outputs-refresh", handler as EventListener);
    return () => {
      window.removeEventListener("workflow-outputs-refresh", handler as EventListener);
    };
  }, [refreshOutputs, tableId]);

  const handleSave = (artifact: Artifact) => {
    if (savedIds.has(artifact.id)) return;
    addArtifact(artifact);
    toast({ title: "Saved to report" });
    if (artifact.type === "repair_plan" && typeof window !== "undefined") {
      window.dispatchEvent(
        new CustomEvent("workflow-outputs-refresh", {
          detail: { tableAssetId: Number(tableId) },
        })
      );
    }
  };

  const handleSaveGroup = (group: OutputGroup) => {
    const featureArtifacts = group.features.map((feature) =>
      featureToArtifact(feature, tableId, group.runKey)
    );
    const repairArtifacts = group.repairs.map((repair) =>
      buildRepairPlanArtifact(repair, group.runKey)
    );
    const pending = [...group.charts, ...group.insights, ...featureArtifacts, ...repairArtifacts].filter(
      (item) => !savedIds.has(item.id)
    );
    if (pending.length === 0) {
      toast({ title: "Already saved" });
      return;
    }
    pending.forEach((item) => addArtifact(item));
    toast({ title: `Saved ${pending.length} outputs` });
    if (group.repairs.length > 0 && typeof window !== "undefined") {
      window.dispatchEvent(
        new CustomEvent("workflow-outputs-refresh", {
          detail: { tableAssetId: Number(tableId) },
        })
      );
    }
  };

  const PanelBody = () => (
    <div className="flex flex-col gap-4 min-w-0 max-w-full">
      <div className="flex items-center justify-between rounded-lg border border-border bg-secondary/30 px-3 py-2 text-xs text-muted-foreground min-w-0 max-w-full gap-2">
        <span className="truncate">{outputGroups.length} columns with outputs</span>
        <span className="shrink-0">
          {outputGroups.reduce(
            (sum, group) =>
              sum + group.charts.length + group.insights.length + group.repairs.length,
            0
          )} items
        </span>
      </div>

      {isLoading && (
        <div className="flex items-center justify-center py-8 text-muted-foreground">
          <Loader2 className="h-5 w-5 animate-spin" />
        </div>
      )}

      {!isLoading && error && (
        <div className="rounded-lg border border-destructive/50 bg-destructive/10 px-3 py-2 text-xs text-destructive max-w-full">
          {error}
        </div>
      )}

      {!isLoading && !error && outputGroups.length === 0 && (
        <div className="rounded-lg border border-dashed border-border px-3 py-6 text-center text-xs text-muted-foreground max-w-full">
          No workflow outputs yet. Run column workflows to generate visuals and insights.
        </div>
      )}

      {!isLoading && !error && outputGroups.length > 0 && (
        <div className="space-y-3 max-w-full">
          {(() => {
            if (!hasPendingOutputs) {
              return (
                <div className="rounded-lg border border-dashed border-border px-3 py-6 text-center text-xs text-muted-foreground">
                  All outputs are saved. New runs will show here automatically.
                </div>
              );
            }

            return outputGroups.map((group) => {
              const pendingCharts = group.charts.filter((item) => !savedIds.has(item.id));
              const pendingInsights = group.insights.filter((item) => !savedIds.has(item.id));
              const pendingFeatures = group.features.filter(
                (feature) => !savedIds.has(featureToArtifact(feature, tableId, group.runKey).id)
              );
              const pendingRepairs = group.repairs.filter(
                (repair) => !savedIds.has(buildRepairPlanArtifact(repair, group.runKey).id)
              );
              const pendingCount =
                pendingCharts.length +
                pendingInsights.length +
                pendingFeatures.length +
                pendingRepairs.length;
              if (pendingCount === 0) {
                return null;
              }
              return (
                <div key={group.columnName} className="rounded-lg border border-border bg-card/50 p-3 space-y-3 min-w-0 max-w-full">
                  <div className="flex items-center justify-between gap-2 min-w-0">
                    <div className="min-w-0 flex-1">
                      <div className="text-sm font-medium truncate">{group.columnName}</div>
                      <div className="text-[11px] text-muted-foreground truncate">
                      {pendingCharts.length} charts · {pendingInsights.length} insights · {pendingFeatures.length} features · {pendingRepairs.length} repair plans
                      </div>
                      {group.visualSelection?.selectedCount && (
                        <div className="mt-1 text-[10px] text-muted-foreground">
                          AI recommended {group.visualSelection.selectedCount}
                          {group.visualSelection.total
                            ? ` of ${group.visualSelection.total}`
                            : ""}{" "}
                          charts for review.
                          {group.visualSelection.rationale
                            ? ` ${group.visualSelection.rationale}`
                            : ""}
                        </div>
                      )}
                    </div>
                    <Button
                      size="sm"
                      variant="surface"
                    disabled={pendingCount === 0}
                    onClick={() => handleSaveGroup(group)}
                  >
                    Save all
                  </Button>
                </div>

                {pendingCharts.length > 0 && (
                  <div className="space-y-2 max-w-full">
                    {pendingCharts.map((chart) => {
                      const isSaved = savedIds.has(chart.id);
                      return (
                        <div
                          key={chart.id}
                          className={cn(
                            "rounded-md border border-border/60 px-2.5 py-2 text-xs min-w-0 max-w-full",
                            isSaved && "bg-secondary/30"
                          )}
                        >
                          <div className="flex items-start justify-between gap-2 min-w-0">
                            <div className="min-w-0 flex-1">
                              <div className="flex items-center gap-1.5 text-foreground">
                                <BarChart3 className="h-3.5 w-3.5 text-[hsl(var(--viz-cyan))]" />
                                <span className="truncate">{chart.content.title}</span>
                                {chart.content.aiSelected && (
                                  <Badge variant="outline" className="text-[10px] border-emerald-500/40 text-emerald-600">
                                    AI pick
                                  </Badge>
                                )}
                                {isSaved && (
                                  <Badge variant="secondary" className="text-[10px]">Saved</Badge>
                                )}
                              </div>
                              <div className="text-[10px] text-muted-foreground">
                                {chart.content.chartType} · {chart.content.data.length} rows
                              </div>
                              {chart.content.insight && (
                                <div className="mt-1 text-[11px] text-muted-foreground line-clamp-2">
                                  {chart.content.insight}
                                </div>
                              )}
                            </div>
                            <Button
                              size="icon-sm"
                              variant="ghost"
                              disabled={isSaved}
                              onClick={() => handleSave(chart)}
                            >
                              <Save className="h-3.5 w-3.5" />
                            </Button>
                          </div>
                        </div>
                      );
                    })}
                  </div>
                )}

                {pendingInsights.length > 0 && (
                  <div className="space-y-2 max-w-full">
                    {pendingInsights.map((insight) => {
                      const isSaved = savedIds.has(insight.id);
                      return (
                        <div
                          key={insight.id}
                          className={cn(
                            "rounded-md border border-border/60 px-2.5 py-2 text-xs min-w-0 max-w-full",
                            isSaved && "bg-secondary/30"
                          )}
                        >
                          <div className="flex items-start justify-between gap-2 min-w-0">
                            <div className="min-w-0 flex-1">
                              <div className="flex items-center gap-1.5 text-foreground">
                                <Lightbulb className="h-3.5 w-3.5 text-[hsl(var(--viz-yellow))]" />
                                <span className="truncate">{insight.content.title}</span>
                                {isSaved && (
                                  <Badge variant="secondary" className="text-[10px]">Saved</Badge>
                                )}
                              </div>
                              <ul className="mt-1 space-y-1 text-[11px] text-muted-foreground">
                                {insight.content.bullets.slice(0, 3).map((bullet, idx) => (
                                  <li key={idx} className="flex items-start gap-1.5">
                                    <span className="text-primary">•</span>
                                    <span className="line-clamp-2">{bullet}</span>
                                  </li>
                                ))}
                              </ul>
                            </div>
                            <Button
                              size="icon-sm"
                              variant="ghost"
                              disabled={isSaved}
                              onClick={() => handleSave(insight)}
                            >
                              <Save className="h-3.5 w-3.5" />
                            </Button>
                          </div>
                        </div>
                      );
                    })}
                  </div>
                )}

                {pendingFeatures.length > 0 && (
                  <div className="space-y-2 max-w-full">
                    {pendingFeatures.map((feature) => {
                      const featureArtifact = featureToArtifact(feature, tableId, group.runKey);
                      const isSaved = savedIds.has(featureArtifact.id);
                      return (
                        <div
                          key={feature.id}
                          className={cn(
                            "rounded-md border border-border/60 px-2.5 py-2 text-xs min-w-0 max-w-full",
                            isSaved && "bg-secondary/30"
                          )}
                        >
                          <div className="flex items-start justify-between gap-2 min-w-0">
                            <div className="min-w-0 flex-1">
                              <div className="flex items-center gap-1.5 text-foreground">
                                <Sparkles className="h-3.5 w-3.5 text-[hsl(var(--viz-green))]" />
                                <span className="truncate">{feature.outputColumn}</span>
                              </div>
                              <div className="text-[11px] text-muted-foreground">
                                Derived from {feature.sourceColumn}
                              </div>
                            </div>
                            <Button
                              size="xs"
                              variant="outline"
                              onClick={() => handleSave(featureArtifact)}
                              disabled={isSaved}
                            >
                              {isSaved ? "Saved" : "Save"}
                            </Button>
                          </div>
                        </div>
                      );
                    })}
                  </div>
                )}

                {pendingRepairs.length > 0 && (
                  <div className="space-y-2 max-w-full">
                    {pendingRepairs.map((repair) => {
                      const plan = repair.plan || {};
                      const tokenCount = plan.token_estimate?.token_count ?? 0;
                      const repairArtifact = buildRepairPlanArtifact(repair, group.runKey);
                      const isSaved = savedIds.has(repairArtifact.id);
                      return (
                        <div
                          key={repair.id}
                          className="rounded-md border border-border/60 bg-card/60 px-2.5 py-2 text-xs"
                        >
                          <div className="flex items-start justify-between gap-2">
                            <div className="min-w-0 flex-1 space-y-1">
                              <div className="text-[12px] font-medium text-foreground">
                                Repair plan ready
                              </div>
                              {plan.summary && (
                                <div className="text-[11px] text-muted-foreground">
                                  {plan.summary}
                                </div>
                              )}
                              {plan.snapshot?.total_count && (
                                <div className="text-[10px] text-muted-foreground">
                                  Snapshot rows: {plan.snapshot.total_count}
                                </div>
                              )}
                              {tokenCount > 0 && (
                                <div className="text-[10px] text-muted-foreground">
                                  Token estimate: {tokenCount}
                                </div>
                              )}
                            </div>
                            <div className="flex flex-col gap-2">
                              <Button
                                size="icon-sm"
                                variant="ghost"
                                onClick={() => {
                                  setActiveRepair(toRepairPlanItem(repair));
                                  setRepairDialogOpen(true);
                                }}
                                aria-label="Review & apply repair plan"
                              >
                                <ShieldCheck className="h-3.5 w-3.5 text-emerald-600" />
                              </Button>
                              <Button
                                size="icon-sm"
                                variant="ghost"
                                disabled={isSaved}
                                onClick={() => handleSave(repairArtifact)}
                              >
                                <Save className="h-3.5 w-3.5" />
                              </Button>
                            </div>
                          </div>
                        </div>
                      );
                    })}
                  </div>
                )}
              </div>
              );
            });
          })()}
        </div>
      )}
    </div>
  );

  if (isMobile) {
    return (
      <>
        <RepairApprovalDialog
          open={repairDialogOpen}
          onOpenChange={setRepairDialogOpen}
          tableId={Number(tableId)}
          activeRepair={activeRepair}
          onApplied={async () => {
            await refreshOutputs(true);
          }}
        />
        {!isOpen && <AIFloatingButton onClick={onToggle} />}
        <Drawer open={isOpen} onOpenChange={(open) => !open && onToggle()}>
          <DrawerContent className="max-h-[85vh]">
            <DrawerHeader className="flex flex-row items-center justify-between pb-2">
              <div className="flex items-center gap-2">
                <Sparkles className="w-5 h-5 text-primary" />
                <DrawerTitle>Workflow Outputs</DrawerTitle>
              </div>
              <div className="flex items-center gap-1">
                <AIChangelogPopover changelog={changelog} />
                <Button variant="ghost" size="icon-sm" onClick={onToggle}>
                  <X className="w-4 h-4" />
                </Button>
              </div>
            </DrawerHeader>
            <div className="px-4 pb-6 overflow-auto flex-1">
              <PanelBody />
            </div>
          </DrawerContent>
        </Drawer>
      </>
    );
  }

  if (!isOpen) {
    return <AIFloatingButton onClick={onToggle} />;
  }

  return (
    <div className="w-72 flex-shrink-0 border-l border-border bg-card flex flex-col h-full overflow-hidden">
      <RepairApprovalDialog
        open={repairDialogOpen}
        onOpenChange={setRepairDialogOpen}
        tableId={Number(tableId)}
        activeRepair={activeRepair}
        onApplied={async () => {
          await refreshOutputs(true);
        }}
      />
      <div className="p-3 border-b border-border flex items-center justify-between flex-shrink-0">
        <div className="flex items-center gap-2">
          <Sparkles className="w-4 h-4 text-primary" />
          <span className="text-sm font-medium">Workflow Outputs</span>
        </div>
        <div className="flex items-center gap-1">
          <AIChangelogPopover changelog={changelog} />
          <Button variant="ghost" size="icon-sm" onClick={onToggle}>
            <X className="w-4 h-4" />
          </Button>
        </div>
      </div>

      <ScrollArea className="flex-1 min-w-0 overflow-x-hidden">
        <div className="p-3 min-w-0" style={{ width: 'calc(18rem - 0.25rem)', maxWidth: 'calc(18rem - 0.25rem)' }}>
          <PanelBody />
        </div>
      </ScrollArea>
    </div>
  );
};

export default AIActionsPanel;
