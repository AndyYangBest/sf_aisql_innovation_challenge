import { memo, useMemo, useState, useEffect, useRef, useCallback } from "react";
import {
  Pin,
  Trash2,
  GripVertical,
  Database,
  SlidersHorizontal,
  Loader2,
  AlertTriangle,
} from "lucide-react";
import { Button } from "@/components/ui/button";
import { ChartSpec } from "@/lib/chartUtils";
import ChartRenderer from "./ChartRenderer";
import { cn } from "@/lib/utils";
import { AlertDialog, AlertDialogContent, AlertDialogHeader, AlertDialogTitle } from "@/components/ui/alert-dialog";
import DataTab from "@/components/workspace/tabs/DataTab";
import { TableResult } from "@/types";
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover";
import { Checkbox } from "@/components/ui/checkbox";
import { columnMetadataApi } from "@/api/columnMetadata";
import { columnWorkflowsApi } from "@/api/columnWorkflows";

interface ChartCardProps {
  spec: ChartSpec;
  tableId?: string;
  tableResult?: TableResult;
  onRequestTableResult?: () => void;
  isPinned?: boolean;
  onPin?: () => void;
  onDelete?: () => void;
  isDragging?: boolean;
}

const parseCorrelationTargetColumn = (spec: ChartSpec): string | null => {
  const title = String(spec.title || "").trim();
  const match = title.match(/correlation heatmap for (.+)$/i);
  if (match?.[1]) {
    return match[1].trim();
  }
  const sourceColumns = Array.isArray(spec.sourceColumns) ? spec.sourceColumns : [];
  if (sourceColumns.length > 0) {
    return String(sourceColumns[0] || "").trim() || null;
  }
  return null;
};

const normalizeChartSpec = (chart: Record<string, any>, fallback: ChartSpec): ChartSpec => ({
  id: String(chart.id || fallback.id),
  chartType: (chart.chartType || chart.chart_type || fallback.chartType) as ChartSpec["chartType"],
  title: String(chart.title || fallback.title),
  xKey: String(chart.xKey || chart.x_key || fallback.xKey),
  yKey: String(chart.yKey || chart.y_key || fallback.yKey),
  valueKey: chart.valueKey || chart.value_key || fallback.valueKey,
  xTitle: chart.xTitle || chart.x_title || fallback.xTitle,
  yTitle: chart.yTitle || chart.y_title || fallback.yTitle,
  yScale: chart.yScale || chart.y_scale || fallback.yScale,
  data: Array.isArray(chart.data) ? chart.data : fallback.data,
  narrative: Array.isArray(chart.narrative) ? chart.narrative : fallback.narrative,
  sourceColumns: Array.isArray(chart.sourceColumns)
    ? chart.sourceColumns
    : Array.isArray(chart.source_columns)
      ? chart.source_columns
      : fallback.sourceColumns,
  series: Array.isArray(chart.series) ? chart.series : fallback.series,
  insight: typeof chart.insight === "string" ? chart.insight : fallback.insight,
  warnings: Array.isArray(chart.warnings)
    ? chart.warnings
    : chart.warning
      ? [chart.warning]
      : fallback.warnings,
});

const ChartCard = memo(
  ({
    spec,
    tableId,
    tableResult,
    onRequestTableResult,
    isPinned,
    onPin,
    onDelete,
    isDragging,
  }: ChartCardProps) => {
    const [columnDialogOpen, setColumnDialogOpen] = useState(false);
    const [activeColumn, setActiveColumn] = useState<string | null>(null);
    const [displaySpec, setDisplaySpec] = useState<ChartSpec>(spec);
    const [correlationPickerOpen, setCorrelationPickerOpen] = useState(false);
    const [numericColumns, setNumericColumns] = useState<string[]>([]);
    const [selectedCorrelationColumns, setSelectedCorrelationColumns] = useState<string[]>([]);
    const [isRecomputingCorrelation, setIsRecomputingCorrelation] = useState(false);
    const [correlationError, setCorrelationError] = useState<string | null>(null);
    const correlationReqSeq = useRef(0);

    useEffect(() => {
      if (columnDialogOpen && !tableResult && onRequestTableResult) {
        onRequestTableResult();
      }
    }, [columnDialogOpen, tableResult, onRequestTableResult]);

    useEffect(() => {
      setDisplaySpec(spec);
    }, [spec]);

    const tableAssetId = useMemo(() => {
      const parsed = Number.parseInt(String(tableId || ""), 10);
      return Number.isFinite(parsed) ? parsed : null;
    }, [tableId]);

    const isCorrelationHeatmap = useMemo(() => {
      if (displaySpec.chartType !== "heatmap") {
        return false;
      }
      const title = String(displaySpec.title || "").toLowerCase();
      const valueKey = String(displaySpec.valueKey || "").toLowerCase();
      return title.includes("correlation") || valueKey.includes("correlation");
    }, [displaySpec.chartType, displaySpec.title, displaySpec.valueKey]);

    const correlationTargetColumn = useMemo(
      () => (isCorrelationHeatmap ? parseCorrelationTargetColumn(displaySpec) : null),
      [displaySpec, isCorrelationHeatmap]
    );

    const sourceColumns = useMemo(
      () => (Array.isArray(displaySpec.sourceColumns) ? displaySpec.sourceColumns : []),
      [displaySpec.sourceColumns]
    );
    const chartWarnings = useMemo(
      () => (Array.isArray(displaySpec.warnings) ? displaySpec.warnings : []),
      [displaySpec.warnings]
    );
    const yearWarningSummary = useMemo(() => {
      let anomalyCount = 0;
      let hasYearWarning = false;
      for (const warning of chartWarnings) {
        if (!warning || typeof warning !== "object") {
          continue;
        }
        const code = String((warning as any).code || "").toLowerCase();
        const message = String((warning as any).message || "").toLowerCase();
        const isYearWarning =
          code.includes("year") ||
          code.includes("epoch") ||
          message.includes("year") ||
          message.includes("epoch");
        if (!isYearWarning) {
          continue;
        }
        hasYearWarning = true;
        const countRaw = (warning as any).anomaly_count;
        const count = Number.isFinite(Number(countRaw)) ? Number(countRaw) : 0;
        anomalyCount += Math.max(0, count);
      }
      if (!hasYearWarning) {
        return null;
      }
      return {
        anomalyCount,
      };
    }, [chartWarnings]);
    const pillColumns = sourceColumns.slice(0, 2);

    useEffect(() => {
      if (!isCorrelationHeatmap || !correlationTargetColumn) {
        setSelectedCorrelationColumns([]);
        setCorrelationError(null);
        return;
      }
      const fromChart = sourceColumns.filter(
        (name) => name && name.toLowerCase() !== correlationTargetColumn.toLowerCase()
      );
      setSelectedCorrelationColumns(Array.from(new Set(fromChart)));
      setCorrelationError(null);
    }, [isCorrelationHeatmap, correlationTargetColumn, sourceColumns]);

    useEffect(() => {
      if (!isCorrelationHeatmap || !tableAssetId) {
        setNumericColumns([]);
        return;
      }
      let cancelled = false;
      void (async () => {
        const response = await columnMetadataApi.get(tableAssetId);
        if (cancelled || response.status !== "success" || !response.data) {
          return;
        }
        const numeric = response.data.columns
          .filter((column) => String(column.semantic_type || "").toLowerCase() === "numeric")
          .map((column) => column.column_name)
          .filter((name) => Boolean(name))
          .sort((left, right) => left.localeCompare(right));
        setNumericColumns(numeric);
      })();
      return () => {
        cancelled = true;
      };
    }, [isCorrelationHeatmap, tableAssetId]);

    const recomputeCorrelationHeatmap = useCallback(
      async (columns: string[]) => {
        if (!tableAssetId || !correlationTargetColumn || columns.length < 1) {
          return;
        }
        const seq = ++correlationReqSeq.current;
        setIsRecomputingCorrelation(true);
        setCorrelationError(null);
        const response = await columnWorkflowsApi.recomputeCorrelationHeatmap(
          tableAssetId,
          correlationTargetColumn,
          { columns }
        );
        if (correlationReqSeq.current !== seq) {
          return;
        }
        if (response.status !== "success" || !response.data?.chart) {
          setCorrelationError(response.error || response.data?.error || "Recompute failed");
          setIsRecomputingCorrelation(false);
          return;
        }
        const nextSpec = normalizeChartSpec(response.data.chart, displaySpec);
        if (response.data.insight && !nextSpec.insight) {
          nextSpec.insight = response.data.insight;
        }
        setDisplaySpec(nextSpec);
        if (Array.isArray(response.data.source_columns)) {
          const nextColumns = response.data.source_columns.filter(
            (name) =>
              name &&
              name.toLowerCase() !== correlationTargetColumn.toLowerCase()
          );
          setSelectedCorrelationColumns(Array.from(new Set(nextColumns)));
        }
        setIsRecomputingCorrelation(false);
      },
      [tableAssetId, correlationTargetColumn, displaySpec]
    );

    const selectableCorrelationColumns = useMemo(() => {
      if (!correlationTargetColumn) {
        return [];
      }
      const candidates = new Set<string>(numericColumns);
      selectedCorrelationColumns.forEach((name) => candidates.add(name));
      return Array.from(candidates)
        .filter((name) => name.toLowerCase() !== correlationTargetColumn.toLowerCase())
        .sort((left, right) => left.localeCompare(right));
    }, [correlationTargetColumn, numericColumns, selectedCorrelationColumns]);

    const titleRemainder = useMemo(() => {
      if (!displaySpec.title) return "";
      let remainder = displaySpec.title;
      pillColumns.forEach((col) => {
        const safe = col.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
        remainder = remainder.replace(new RegExp(`\\b${safe}\\b`, "gi"), "").trim();
      });
      return remainder.replace(/\s{2,}/g, " ").trim() || "Chart";
    }, [displaySpec.title, pillColumns]);

    const columnTableResult = useMemo(() => {
      if (!tableResult || !activeColumn) return undefined;
      const colInfo = tableResult.columns.find((c) => c.name === activeColumn);
      if (!colInfo) return undefined;
      return {
        ...tableResult,
        columns: [colInfo],
        rows: tableResult.rows.map((row) => ({ [activeColumn]: row[activeColumn] })),
      } as TableResult;
    }, [tableResult, activeColumn]);

    const columnStats = useMemo(() => {
      if (!tableResult || !activeColumn) return null;
      const values = tableResult.rows.map((row) => row[activeColumn]);
      const nonNull = values.filter((v) => v !== null && v !== undefined);
      const unique = new Set(nonNull.map((v) => String(v))).size;
      const nullCount = values.length - nonNull.length;
      const nullRate = values.length ? Math.round((nullCount / values.length) * 100) : 0;
      const numericValues = nonNull.filter((v) => typeof v === "number") as number[];
      const min = numericValues.length ? Math.min(...numericValues) : null;
      const max = numericValues.length ? Math.max(...numericValues) : null;
      const mean = numericValues.length
        ? numericValues.reduce((sum, v) => sum + v, 0) / numericValues.length
        : null;
      return { unique, nullCount, nullRate, min, max, mean };
    }, [tableResult, activeColumn]);

    const handleColumnClick = (columnName: string) => {
      setActiveColumn(columnName);
      setColumnDialogOpen(true);
    };

    const handleCorrelationColumnToggle = (columnName: string, checked: boolean) => {
      if (!isCorrelationHeatmap) {
        return;
      }
      const current = selectedCorrelationColumns;
      const next = checked
        ? Array.from(new Set([...current, columnName]))
        : current.filter((name) => name !== columnName);
      if (next.length < 1) {
        setCorrelationError("At least one comparison column is required.");
        return;
      }
      setSelectedCorrelationColumns(next);
      void recomputeCorrelationHeatmap(next);
    };

  return (
    <div
      className={cn(
        "h-full flex flex-col bg-card border border-border rounded-lg overflow-hidden transition-shadow",
        isDragging && "shadow-xl ring-2 ring-primary/50"
      )}
    >
      {/* Header */}
      <div className="flex items-center justify-between px-3 py-2 border-b border-border bg-secondary/30">
        <div className="flex items-center gap-2 min-w-0">
          <div className="chart-drag-handle cursor-grab active:cursor-grabbing p-1 -ml-1 hover:bg-muted rounded">
            <GripVertical className="w-4 h-4 text-muted-foreground" />
          </div>
          <div className="flex items-center gap-2 min-w-0 flex-wrap">
            {pillColumns.map((col) => (
              <button
                key={col}
                className="inline-flex max-w-[160px] items-center gap-1 rounded-full border border-primary/40 bg-primary/10 px-2 py-0.5 text-[11px] font-semibold text-primary hover:bg-primary/20"
                onClick={(e) => {
                  e.stopPropagation();
                  handleColumnClick(col);
                }}
                title={`Open ${col} column data`}
              >
                <span className="truncate">{col}</span>
              </button>
            ))}
            {isCorrelationHeatmap && (
              <Popover open={correlationPickerOpen} onOpenChange={setCorrelationPickerOpen}>
                <PopoverTrigger asChild>
                  <Button
                    variant="outline"
                    size="sm"
                    className="h-6 gap-1 px-2 text-[11px]"
                    onClick={(event) => event.stopPropagation()}
                  >
                    {isRecomputingCorrelation ? (
                      <Loader2 className="h-3 w-3 animate-spin" />
                    ) : (
                      <SlidersHorizontal className="h-3 w-3" />
                    )}
                    Columns ({selectedCorrelationColumns.length + 1})
                  </Button>
                </PopoverTrigger>
                <PopoverContent
                  className="w-[320px] border-slate-700/80 bg-slate-950/95 p-3 text-slate-100"
                  align="start"
                >
                  <div className="space-y-2">
                    <div className="text-[11px] text-slate-300">
                      Base column:{" "}
                      <span className="font-semibold text-primary">
                        {correlationTargetColumn || "—"}
                      </span>
                    </div>
                    <div className="text-[10px] uppercase tracking-wide text-slate-400">
                      Comparison columns
                    </div>
                    <div className="max-h-52 space-y-1 overflow-auto pr-1">
                      {selectableCorrelationColumns.map((name) => {
                        const checked = selectedCorrelationColumns.includes(name);
                        return (
                          <label
                            key={name}
                            className="flex cursor-pointer items-center gap-2 rounded px-2 py-1 hover:bg-slate-900/70"
                          >
                            <Checkbox
                              checked={checked}
                              onCheckedChange={(value) =>
                                handleCorrelationColumnToggle(name, value === true)
                              }
                            />
                            <span className="truncate text-xs">{name}</span>
                          </label>
                        );
                      })}
                      {selectableCorrelationColumns.length === 0 && (
                        <div className="rounded bg-slate-900/60 px-2 py-1 text-xs text-slate-400">
                          No numeric columns available.
                        </div>
                      )}
                    </div>
                    {correlationError && (
                      <div className="rounded border border-rose-500/40 bg-rose-500/10 px-2 py-1 text-[11px] text-rose-200">
                        {correlationError}
                      </div>
                    )}
                  </div>
                </PopoverContent>
              </Popover>
            )}
            <h3 className="text-sm font-medium text-foreground/90 truncate min-w-0">
              {titleRemainder}
            </h3>
          </div>
        </div>
        <div className="flex items-center gap-0.5">
          {chartWarnings.length > 0 && (
            <Popover>
              <PopoverTrigger asChild>
                <Button
                  variant="ghost"
                  size="icon-sm"
                  className="text-warning hover:text-warning hover:bg-warning/10"
                  onClick={(event) => event.stopPropagation()}
                  title={
                    yearWarningSummary
                      ? `${yearWarningSummary.anomalyCount} out-of-range year values filtered`
                      : `${chartWarnings.length} chart warnings`
                  }
                >
                  <AlertTriangle className="w-3.5 h-3.5" />
                </Button>
              </PopoverTrigger>
              <PopoverContent
                className="w-[340px] border-warning/40 bg-slate-950/95 p-3 text-slate-100"
                align="end"
              >
                <div className="space-y-2">
                  <div className="flex items-center gap-2 text-xs font-semibold text-warning">
                    <AlertTriangle className="h-3.5 w-3.5" />
                    Chart warnings
                  </div>
                  {chartWarnings.map((warning: any, idx: number) => {
                    const title = String(
                      warning?.title || warning?.code || `Warning ${idx + 1}`
                    );
                    const message = String(
                      warning?.message || "Potential data-quality issue detected."
                    );
                    const anomalyCount = Number.isFinite(Number(warning?.anomaly_count))
                      ? Number(warning.anomaly_count)
                      : null;
                    const rangeMin = warning?.valid_range?.min;
                    const rangeMax = warning?.valid_range?.max;
                    const samples = Array.isArray(warning?.sample_values)
                      ? warning.sample_values
                      : [];
                    return (
                      <div
                        key={`${title}-${idx}`}
                        className="rounded border border-warning/30 bg-warning/10 px-2 py-2 text-[11px]"
                      >
                        <div className="font-semibold text-warning">{title}</div>
                        <div className="mt-1 text-slate-200">{message}</div>
                        {anomalyCount !== null && (
                          <div className="mt-1 text-slate-300">
                            Filtered rows: {anomalyCount.toLocaleString()}
                          </div>
                        )}
                        {(rangeMin !== undefined || rangeMax !== undefined) && (
                          <div className="text-slate-400">
                            Valid year range: {String(rangeMin ?? "—")} - {String(rangeMax ?? "—")}
                          </div>
                        )}
                        {samples.length > 0 && (
                          <div className="mt-1 flex flex-wrap gap-1">
                            {samples.slice(0, 4).map((sample: any, sampleIdx: number) => (
                              <span
                                key={`${idx}-sample-${sampleIdx}`}
                                className="rounded-full border border-warning/30 bg-slate-900/70 px-2 py-0.5 text-[10px] text-slate-200"
                              >
                                {String(sample?.value)}
                                {Number.isFinite(Number(sample?.count)) && (
                                  <span className="ml-1 text-slate-400">
                                    ×{Number(sample.count)}
                                  </span>
                                )}
                              </span>
                            ))}
                          </div>
                        )}
                      </div>
                    );
                  })}
                </div>
              </PopoverContent>
            </Popover>
          )}
          {onPin && (
            <Button
              variant="ghost"
              size="icon-sm"
              onClick={(e) => {
                e.stopPropagation();
                onPin();
              }}
              className={cn(isPinned && "text-primary")}
            >
              <Pin className="w-3.5 h-3.5" />
            </Button>
          )}
          {onDelete && (
            <Button
              variant="ghost"
              size="icon-sm"
              onClick={(e) => {
                e.stopPropagation();
                onDelete();
              }}
              className="text-muted-foreground hover:text-destructive"
            >
              <Trash2 className="w-3.5 h-3.5" />
            </Button>
          )}
        </div>
      </div>

      {/* Chart Area */}
      <div className="flex-1 p-3 min-h-0">
        <ChartRenderer spec={displaySpec} />
      </div>

      {displaySpec.insight && (
        <div className="px-3 py-2 border-t border-border bg-secondary/20">
          <div className="text-[10px] uppercase tracking-wide text-muted-foreground">
            Insight
          </div>
          <p className="text-xs text-foreground/80">{displaySpec.insight}</p>
        </div>
      )}

      {/* Narrative Footer */}
      {displaySpec.narrative && displaySpec.narrative.length > 0 && (
        <div className="px-3 py-2 border-t border-border bg-secondary/20">
          <ul className="space-y-0.5">
            {displaySpec.narrative.slice(0, 2).map((n, i) => (
              <li key={i} className="text-[10px] text-muted-foreground flex items-start gap-1.5">
                <span className="text-primary mt-px">•</span>
                <span className="truncate">{n}</span>
              </li>
            ))}
          </ul>
        </div>
      )}

      <AlertDialog open={columnDialogOpen} onOpenChange={setColumnDialogOpen}>
        <AlertDialogContent className="max-h-[85vh] overflow-auto border border-slate-800/70 bg-slate-950/95 text-slate-100">
          <AlertDialogHeader>
            <AlertDialogTitle className="text-slate-100 flex items-center gap-2">
              <Database className="h-4 w-4 text-primary" />
              {activeColumn ? `${activeColumn} column data` : "Column data"}
            </AlertDialogTitle>
          </AlertDialogHeader>
          {!tableResult && (
            <div className="text-sm text-slate-400">
              {onRequestTableResult
                ? "Loading data preview..."
                : "No data preview available."}
            </div>
          )}
          {tableResult && activeColumn && (
            <div className="space-y-4">
              {columnStats && (
                <div className="grid gap-3 sm:grid-cols-3 text-xs">
                  <div className="rounded-lg border border-slate-800 bg-slate-900/50 px-3 py-2">
                    <div className="text-slate-400">Nulls</div>
                    <div className="text-slate-100 font-semibold">
                      {columnStats.nullCount} ({columnStats.nullRate}%)
                    </div>
                  </div>
                  <div className="rounded-lg border border-slate-800 bg-slate-900/50 px-3 py-2">
                    <div className="text-slate-400">Unique</div>
                    <div className="text-slate-100 font-semibold">{columnStats.unique}</div>
                  </div>
                  <div className="rounded-lg border border-slate-800 bg-slate-900/50 px-3 py-2">
                    <div className="text-slate-400">Mean</div>
                    <div className="text-slate-100 font-semibold">
                      {columnStats.mean !== null
                        ? columnStats.mean.toLocaleString(undefined, {
                            maximumFractionDigits: 2,
                          })
                        : "—"}
                    </div>
                  </div>
                </div>
              )}
              <DataTab tableResult={columnTableResult} rowLimit={12} showHeader={false} />
            </div>
          )}
        </AlertDialogContent>
      </AlertDialog>
    </div>
  );
  }
);

ChartCard.displayName = "ChartCard";

export default ChartCard;
