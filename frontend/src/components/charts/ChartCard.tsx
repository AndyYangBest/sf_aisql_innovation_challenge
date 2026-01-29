import { memo, useMemo, useState, useEffect } from "react";
import { Pin, Trash2, GripVertical, Database } from "lucide-react";
import { Button } from "@/components/ui/button";
import { ChartSpec } from "@/lib/chartUtils";
import ChartRenderer from "./ChartRenderer";
import { cn } from "@/lib/utils";
import { AlertDialog, AlertDialogContent, AlertDialogHeader, AlertDialogTitle } from "@/components/ui/alert-dialog";
import DataTab from "@/components/workspace/tabs/DataTab";
import { TableResult } from "@/types";

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

    useEffect(() => {
      if (columnDialogOpen && !tableResult && onRequestTableResult) {
        onRequestTableResult();
      }
    }, [columnDialogOpen, tableResult, onRequestTableResult]);

    const sourceColumns = useMemo(
      () => (Array.isArray(spec.sourceColumns) ? spec.sourceColumns : []),
      [spec.sourceColumns]
    );
    const pillColumns = sourceColumns.slice(0, 2);

    const titleRemainder = useMemo(() => {
      if (!spec.title) return "";
      let remainder = spec.title;
      pillColumns.forEach((col) => {
        const safe = col.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
        remainder = remainder.replace(new RegExp(`\\b${safe}\\b`, "gi"), "").trim();
      });
      return remainder.replace(/\s{2,}/g, " ").trim() || "Chart";
    }, [spec.title, pillColumns]);

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
            <h3 className="text-sm font-medium text-foreground/90 truncate min-w-0">
              {titleRemainder}
            </h3>
          </div>
        </div>
        <div className="flex items-center gap-0.5">
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
        <ChartRenderer spec={spec} />
      </div>

      {/* Narrative Footer */}
      {spec.narrative && spec.narrative.length > 0 && (
        <div className="px-3 py-2 border-t border-border bg-secondary/20">
          <ul className="space-y-0.5">
            {spec.narrative.slice(0, 2).map((n, i) => (
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
