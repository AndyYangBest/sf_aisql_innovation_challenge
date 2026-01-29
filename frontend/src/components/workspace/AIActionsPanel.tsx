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
};

const buildRepairPlanArtifact = (repair: RepairPlanOutput): Artifact => {
  const plan = repair.plan || {};
  const planKey =
    plan.plan_hash || plan.snapshot?.signature || plan.plan_id || "latest";
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
    id: `repair_plan_${repair.tableId}_${repair.columnName}_${planKey}`,
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

const extractInsightBullets = (payload: any): string[] => {
  if (!payload) return [];
  if (Array.isArray(payload)) return payload.map((item) => String(item));
  if (typeof payload === "string") return [payload];
  if (payload.insights) return extractInsightBullets(payload.insights);
  if (Array.isArray(payload.key_findings)) return payload.key_findings.map(String);
  return [];
};

const buildOutputGroups = (columns: ColumnMetadataRecord[], tableId: string): OutputGroup[] => {
  return columns
    .map((column) => {
      const analysis = column.metadata?.analysis as Record<string, any> | undefined;
      if (!analysis) return null;

      const createdAt = column.last_updated || column.updated_at || new Date().toISOString();
      const visuals = Array.isArray(analysis.visuals) ? analysis.visuals : [];
      const charts: ChartArtifact[] = visuals.map((visual: any, idx: number) => ({
        type: "chart",
        id: visual.id || `chart_${tableId}_${column.column_name}_${idx}`,
        tableId,
        content: {
          chartType: normalizeChartType(visual.chartType || visual.chart_type),
          title: visual.title || `${column.column_name} chart`,
          xKey: visual.xKey || visual.x_key || "x",
          yKey: visual.yKey || visual.y_key || "y",
          xTitle: visual.xTitle || visual.x_title || visual.xKey || visual.x_key,
          yTitle: visual.yTitle || visual.y_title || visual.yKey || visual.y_key,
          yScale: visual.yScale || visual.y_scale,
          data: Array.isArray(visual.data) ? visual.data : [],
          narrative: Array.isArray(visual.narrative) ? visual.narrative : [],
          sourceColumns: Array.isArray(visual.sourceColumns)
            ? visual.sourceColumns
            : [column.column_name],
        },
        createdAt,
      }));

      let bullets = extractInsightBullets(analysis.insights);
      if (!bullets.length && typeof analysis.summary === "string" && analysis.summary.trim()) {
        bullets = [analysis.summary.trim()];
      }
      if (
        !bullets.length &&
        typeof analysis.agent_summary === "string" &&
        analysis.agent_summary.trim()
      ) {
        bullets = [analysis.agent_summary.trim()];
      }
      const caveats = Array.isArray(analysis.caveats)
        ? analysis.caveats
        : Array.isArray((analysis.insights as any)?.caveats)
          ? (analysis.insights as any).caveats
          : [];

      const insights: InsightArtifact[] = bullets.length
        ? [
            {
              type: "insight",
              id: `insight_${tableId}_${column.column_name}`,
              tableId,
              content: {
                title: `${column.column_name} insights`,
                bullets,
                summary: caveats.length ? caveats.join(" ") : undefined,
                sourceColumns: [column.column_name],
              },
              createdAt,
            },
          ]
        : [];

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
      };
    })
    .filter(Boolean) as OutputGroup[];
};

const featureToArtifact = (feature: FeatureOutput, tableId: string): Artifact => {
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
    id: feature.id,
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
        (repair) => !savedIds.has(buildRepairPlanArtifact(repair).id)
      ).length;
      const pendingFeatures = group.features.filter(
        (feature) => !savedIds.has(featureToArtifact(feature, tableId).id)
      ).length;
      return pendingCharts + pendingInsights + pendingRepairs + pendingFeatures > 0;
    });
  }, [outputGroups, savedIds, tableId]);

  useEffect(() => {
    void refreshOutputs();
  }, [activeTab, refreshOutputs]);

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
    const featureArtifacts = group.features.map((feature) => featureToArtifact(feature, tableId));
    const repairArtifacts = group.repairs.map((repair) => buildRepairPlanArtifact(repair));
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
                (feature) => !savedIds.has(featureToArtifact(feature, tableId).id)
              );
              const pendingRepairs = group.repairs.filter(
                (repair) => !savedIds.has(buildRepairPlanArtifact(repair).id)
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
                                {isSaved && (
                                  <Badge variant="secondary" className="text-[10px]">Saved</Badge>
                                )}
                              </div>
                              <div className="text-[10px] text-muted-foreground">
                                {chart.content.chartType} · {chart.content.data.length} rows
                              </div>
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
                      const featureArtifact = featureToArtifact(feature, tableId);
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
                      const repairArtifact = buildRepairPlanArtifact(repair);
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
