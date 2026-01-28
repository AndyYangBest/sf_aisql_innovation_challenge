import { useState } from "react";
import { Columns, Hash, Type, Calendar, ToggleLeft, BarChart3 } from "lucide-react";
import { TableResult } from "@/types";
import DataTab from "./DataTab";
import { useTableStore } from "@/store/tableStore";
import { cn } from "@/lib/utils";
import { Progress } from "@/components/ui/progress";

interface ProfileTabProps {
  tableResult?: TableResult;
}

const getTypeIcon = (type?: string) => {
  if (!type) return Hash;
  if (type.includes("VARCHAR") || type.includes("TEXT")) return Type;
  if (type.includes("DATE") || type.includes("TIMESTAMP")) return Calendar;
  if (type.includes("BOOL")) return ToggleLeft;
  return Hash;
};

const ProfileTab = ({ tableResult }: ProfileTabProps) => {
  const { selectedColumn, setSelectedColumn } = useTableStore();

  if (!tableResult) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="text-center text-muted-foreground">
          <Columns className="h-12 w-12 mx-auto mb-3 opacity-50" />
          <p>No data available</p>
        </div>
      </div>
    );
  }

  // Calculate stats for each column
  const getColumnStats = (colName: string) => {
    const values = tableResult.rows.map((row) => row[colName]);
    const nonNull = values.filter((v) => v !== null && v !== undefined);
    const unique = new Set(nonNull).size;
    const nullCount = values.length - nonNull.length;
    const nullPercent = values.length > 0 ? Math.round((nullCount / values.length) * 100) : 0;
    const uniquePercent = nonNull.length > 0 ? Math.round((unique / nonNull.length) * 100) : 0;

    // For numeric columns
    const numericValues = nonNull.filter((v) => typeof v === "number");
    const hasNumeric = numericValues.length > 0;
    const min = hasNumeric ? Math.min(...numericValues) : null;
    const max = hasNumeric ? Math.max(...numericValues) : null;
    const mean = hasNumeric
      ? numericValues.reduce((a, b) => a + b, 0) / numericValues.length
      : null;

    // Top values
    const valueCounts = nonNull.reduce((acc: Record<string, number>, v) => {
      const key = String(v);
      acc[key] = (acc[key] || 0) + 1;
      return acc;
    }, {});
    const topValues = Object.entries(valueCounts)
      .sort((a, b) => (b[1] as number) - (a[1] as number))
      .slice(0, 5)
      .map(([value, count]) => ({ value, count: count as number }));

    return { unique, uniquePercent, nullCount, nullPercent, min, max, mean, topValues };
  };

  const selectedStats = selectedColumn ? getColumnStats(selectedColumn) : null;
  const selectedColInfo = tableResult.columns.find((c) => c.name === selectedColumn);

  return (
    <div className="h-full flex gap-4">
      {/* Column List */}
      <div className="w-80 flex-shrink-0 flex flex-col">
        <div className="mb-3">
          <h2 className="text-lg font-semibold">Column Profile</h2>
          <p className="text-sm text-muted-foreground">{tableResult.columns.length} columns</p>
        </div>
        <div className="flex-1 space-y-2 overflow-auto pr-2 scrollbar-thin">
          {tableResult.columns.map((col) => {
            const Icon = getTypeIcon(col.type);
            const stats = getColumnStats(col.name);
            const isSelected = selectedColumn === col.name;

            return (
              <button
                key={col.name}
                onClick={() => setSelectedColumn(isSelected ? null : col.name)}
                className={cn(
                  "w-full text-left p-3 rounded-lg border transition-all duration-200",
                  isSelected
                    ? "bg-primary/10 border-primary/30"
                    : "bg-secondary/30 border-border hover:border-primary/20 hover:bg-secondary/50"
                )}
              >
                <div className="flex items-center justify-between mb-2">
                  <div className="flex items-center gap-2">
                    <Icon className={cn("w-4 h-4", isSelected ? "text-primary" : "text-muted-foreground")} />
                    <span className="font-medium text-sm">{col.name}</span>
                  </div>
                  <span className="text-xs font-mono text-muted-foreground">{col.type}</span>
                </div>
                <div className="grid grid-cols-2 gap-2 text-xs">
                  <div>
                    <span className="text-muted-foreground">Nulls: </span>
                    <span className={stats.nullPercent > 5 ? "text-[hsl(var(--viz-orange))]" : "text-[hsl(var(--viz-green))]"}>
                      {stats.nullPercent}%
                    </span>
                  </div>
                  <div>
                    <span className="text-muted-foreground">Unique: </span>
                    <span className="text-foreground">{stats.unique.toLocaleString()}</span>
                  </div>
                </div>
              </button>
            );
          })}
        </div>
      </div>

      {/* Column Details */}
      <div className="flex-1 bg-secondary/30 border border-border rounded-lg p-4 overflow-auto">
        {selectedColumn && selectedStats && selectedColInfo ? (
          <div className="fade-in">
            <div className="flex items-center gap-3 mb-4">
              <div className="w-10 h-10 rounded-lg bg-primary/10 border border-primary/30 flex items-center justify-center">
                {(() => {
                  const Icon = getTypeIcon(selectedColInfo.type);
                  return <Icon className="w-5 h-5 text-primary" />;
                })()}
              </div>
              <div>
                <h3 className="text-lg font-semibold">{selectedColumn}</h3>
                <p className="text-sm text-muted-foreground font-mono">{selectedColInfo.type}</p>
              </div>
            </div>

            <div className="grid grid-cols-2 gap-4 mb-6">
              <div className="bg-background rounded-lg p-3">
                <p className="text-xs text-muted-foreground mb-1">Null Values</p>
                <p className="text-2xl font-semibold">{selectedStats.nullCount.toLocaleString()}</p>
                <Progress value={selectedStats.nullPercent} className="h-1.5 mt-2" />
                <p className="text-xs text-muted-foreground mt-1">{selectedStats.nullPercent}% of rows</p>
              </div>
              <div className="bg-background rounded-lg p-3">
                <p className="text-xs text-muted-foreground mb-1">Unique Values</p>
                <p className="text-2xl font-semibold">{selectedStats.unique.toLocaleString()}</p>
                <Progress value={selectedStats.uniquePercent} className="h-1.5 mt-2" />
                <p className="text-xs text-muted-foreground mt-1">{selectedStats.uniquePercent}% cardinality</p>
              </div>
            </div>

            {selectedStats.min !== null && selectedStats.mean !== null && (
              <div className="mb-6">
                <h4 className="text-sm font-medium mb-2">Statistics</h4>
                <div className="grid grid-cols-3 gap-3">
                  <div className="bg-background rounded-lg p-3 text-center">
                    <p className="text-xs text-muted-foreground">Min</p>
                    <p className="font-mono text-sm">{selectedStats.min.toLocaleString()}</p>
                  </div>
                  <div className="bg-background rounded-lg p-3 text-center">
                    <p className="text-xs text-muted-foreground">Mean</p>
                    <p className="font-mono text-sm">{selectedStats.mean.toLocaleString(undefined, { maximumFractionDigits: 2 })}</p>
                  </div>
                  <div className="bg-background rounded-lg p-3 text-center">
                    <p className="text-xs text-muted-foreground">Max</p>
                    <p className="font-mono text-sm">{selectedStats.max?.toLocaleString()}</p>
                  </div>
                </div>
              </div>
            )}

            {selectedStats.topValues.length > 0 && (
              <div>
                <h4 className="text-sm font-medium mb-2">Top Values</h4>
                <div className="space-y-2">
                  {selectedStats.topValues.map((v, i) => (
                    <div key={i} className="flex items-center justify-between bg-background rounded-lg px-3 py-2">
                      <span className="font-mono text-sm truncate">{v.value}</span>
                      <span className="text-xs text-muted-foreground">{v.count.toLocaleString()} rows</span>
                    </div>
                  ))}
                </div>
              </div>
            )}

            <div className="mt-6 space-y-4">
              <div>
                <h4 className="text-sm font-medium mb-2">Schema</h4>
                <div className="grid gap-1.5">
                  {tableResult.columns.map((col) => (
                    <div
                      key={col.name}
                      className="flex items-center justify-between py-1.5 px-2 rounded bg-background text-xs"
                    >
                      <span className="font-mono">{col.name}</span>
                      <span className="text-[11px] text-muted-foreground font-mono">
                        {col.type}
                      </span>
                    </div>
                  ))}
                </div>
              </div>

              <div>
                <h4 className="text-sm font-medium mb-2">Data Preview</h4>
                <DataTab tableResult={tableResult} rowLimit={12} showHeader={false} />
              </div>
            </div>
          </div>
        ) : (
          <div className="h-full flex items-center justify-center text-muted-foreground">
            <div className="text-center">
              <BarChart3 className="w-12 h-12 mx-auto mb-3 opacity-50" />
              <p className="text-sm">Select a column to view its profile</p>
            </div>
          </div>
        )}
      </div>
    </div>
  );
};

export default ProfileTab;
