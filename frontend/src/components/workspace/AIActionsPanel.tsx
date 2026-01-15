import { useEffect, useMemo, useState } from "react";
import { BarChart3, Lightbulb, Loader2, Save, Sparkles, X } from "lucide-react";
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
};

type FeatureOutput = {
  id: string;
  outputColumn: string;
  sourceColumn: string;
  kind: string;
  instruction?: string;
  model?: string;
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

      if (charts.length === 0 && insights.length === 0 && featureOutputs.length === 0) {
        return null;
      }

      return {
        columnName: column.column_name,
        charts,
        insights,
        features: featureOutputs,
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
  const { addArtifact, getChangelog } = useTableStore();
  const artifacts = useTableStore((s) => s.artifacts);
  const { toast } = useToast();
  const { isMobile } = useBreakpoint();
  const changelog = getChangelog(tableId);

  const savedIds = useMemo(() => {
    return new Set(artifacts.filter((a) => a.tableId === tableId).map((a) => a.id));
  }, [artifacts, tableId]);

  useEffect(() => {
    if (!tableId || !isOpen) return;
    let isActive = true;
    setIsLoading(true);
    setError(null);

    columnMetadataApi.get(Number(tableId))
      .then((response) => {
        if (!isActive) return;
        if (response.status === "success" && response.data) {
          const groups = buildOutputGroups(response.data.columns, tableId);
          setOutputGroups(groups);
        } else {
          setError(response.error || "Failed to load workflow outputs");
        }
      })
      .catch((err) => {
        if (!isActive) return;
        setError(err instanceof Error ? err.message : "Failed to load workflow outputs");
      })
      .finally(() => {
        if (isActive) setIsLoading(false);
      });

    return () => {
      isActive = false;
    };
  }, [tableId, activeTab, isOpen]);

  const handleSave = (artifact: Artifact) => {
    if (savedIds.has(artifact.id)) return;
    addArtifact(artifact);
    toast({ title: "Saved to report" });
  };

  const handleSaveGroup = (group: OutputGroup) => {
    const featureArtifacts = group.features.map((feature) => featureToArtifact(feature, tableId));
    const pending = [...group.charts, ...group.insights, ...featureArtifacts].filter(
      (item) => !savedIds.has(item.id)
    );
    if (pending.length === 0) {
      toast({ title: "Already saved" });
      return;
    }
    pending.forEach((item) => addArtifact(item));
    toast({ title: `Saved ${pending.length} outputs` });
  };

  const PanelBody = () => (
    <div className="flex flex-col gap-4 min-w-0 max-w-full">
      <div className="flex items-center justify-between rounded-lg border border-border bg-secondary/30 px-3 py-2 text-xs text-muted-foreground min-w-0 max-w-full gap-2">
        <span className="truncate">{outputGroups.length} columns with outputs</span>
        <span className="shrink-0">
          {outputGroups.reduce((sum, group) => sum + group.charts.length + group.insights.length, 0)} items
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
          {outputGroups.map((group) => {
            const pendingCount = [...group.charts, ...group.insights].filter((item) => !savedIds.has(item.id)).length;
            return (
              <div key={group.columnName} className="rounded-lg border border-border bg-card/50 p-3 space-y-3 min-w-0 max-w-full">
                <div className="flex items-center justify-between gap-2 min-w-0">
                  <div className="min-w-0 flex-1">
                    <div className="text-sm font-medium truncate">{group.columnName}</div>
                    <div className="text-[11px] text-muted-foreground truncate">
                      {group.charts.length} charts · {group.insights.length} insights · {group.features.length} features
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

                {group.charts.length > 0 && (
                  <div className="space-y-2 max-w-full">
                    {group.charts.map((chart) => {
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

                {group.insights.length > 0 && (
                  <div className="space-y-2 max-w-full">
                    {group.insights.map((insight) => {
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

                {group.features.length > 0 && (
                  <div className="space-y-2 max-w-full">
                    {group.features.map((feature) => {
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
              </div>
            );
          })}
        </div>
      )}
    </div>
  );

  if (isMobile) {
    return (
      <>
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
