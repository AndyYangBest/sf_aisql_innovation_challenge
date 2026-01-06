import { useState } from "react";
import { Clock, Play, GitCompare, ChevronLeft, ChevronRight, RotateCcw, Check, X, TrendingUp, TrendingDown, Minus } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Slider } from "@/components/ui/slider";
import { cn } from "@/lib/utils";
import { TableResult } from "@/types";

interface RunSnapshot {
  id: string;
  timestamp: string;
  rowCount: number;
  columns: number;
  profile: {
    [column: string]: {
      unique: number;
      nullCount: number;
      min?: number;
      max?: number;
    };
  };
}

// Mock run history
const mockRunHistory: RunSnapshot[] = [
  {
    id: "run-1",
    timestamp: "2024-12-15T10:00:00Z",
    rowCount: 5,
    columns: 8,
    profile: {
      total_revenue: { unique: 5, nullCount: 0, min: 27783, max: 44850 },
      quantity: { unique: 5, nullCount: 0, min: 89, max: 245 },
      region: { unique: 3, nullCount: 0 },
    },
  },
  {
    id: "run-2",
    timestamp: "2024-12-18T14:30:00Z",
    rowCount: 6,
    columns: 8,
    profile: {
      total_revenue: { unique: 6, nullCount: 0, min: 27783, max: 62088 },
      quantity: { unique: 6, nullCount: 0, min: 89, max: 312 },
      region: { unique: 3, nullCount: 0 },
    },
  },
  {
    id: "run-3",
    timestamp: "2024-12-20T09:15:00Z",
    rowCount: 7,
    columns: 8,
    profile: {
      total_revenue: { unique: 7, nullCount: 1, min: 27783, max: 62088 },
      quantity: { unique: 7, nullCount: 0, min: 78, max: 312 },
      region: { unique: 3, nullCount: 0 },
    },
  },
  {
    id: "run-4",
    timestamp: "2024-12-22T16:45:00Z",
    rowCount: 8,
    columns: 8,
    profile: {
      total_revenue: { unique: 8, nullCount: 0, min: 27783, max: 62088 },
      quantity: { unique: 8, nullCount: 0, min: 78, max: 567 },
      region: { unique: 3, nullCount: 0 },
    },
  },
];

interface TimeTravelProps {
  tableId: string;
}

const TimeTravel = ({ tableId }: TimeTravelProps) => {
  const [selectedRun, setSelectedRun] = useState<number>(mockRunHistory.length - 1);
  const [compareMode, setCompareMode] = useState(false);
  const [compareRun, setCompareRun] = useState<number>(0);

  const currentSnapshot = mockRunHistory[selectedRun];
  const compareSnapshot = compareMode ? mockRunHistory[compareRun] : null;

  const formatDate = (timestamp: string) => {
    return new Date(timestamp).toLocaleDateString("en-US", {
      month: "short",
      day: "numeric",
      hour: "2-digit",
      minute: "2-digit",
    });
  };

  const getDiff = (current: number, previous: number) => {
    const diff = current - previous;
    if (diff > 0) return { icon: TrendingUp, color: "text-success", value: `+${diff}` };
    if (diff < 0) return { icon: TrendingDown, color: "text-destructive", value: `${diff}` };
    return { icon: Minus, color: "text-muted-foreground", value: "0" };
  };

  return (
    <div className="p-4 border-b border-border/50 bg-muted/30">
      <div className="flex items-center justify-between mb-4">
        <div className="flex items-center gap-2">
          <Clock className="h-4 w-4 text-primary" />
          <span className="text-sm font-medium">Run History</span>
          <span className="text-xs text-muted-foreground">
            ({mockRunHistory.length} snapshots)
          </span>
        </div>

        <div className="flex items-center gap-2">
          <Button
            variant={compareMode ? "default" : "outline"}
            size="sm"
            onClick={() => setCompareMode(!compareMode)}
          >
            <GitCompare className="h-4 w-4 mr-1" />
            Compare
          </Button>
        </div>
      </div>

      {/* Timeline Slider */}
      <div className="space-y-3">
        <div className="flex items-center gap-3">
          <Button
            variant="ghost"
            size="icon"
            className="h-8 w-8"
            disabled={selectedRun === 0}
            onClick={() => setSelectedRun((prev) => Math.max(0, prev - 1))}
          >
            <ChevronLeft className="h-4 w-4" />
          </Button>

          <div className="flex-1">
            <Slider
              value={[selectedRun]}
              min={0}
              max={mockRunHistory.length - 1}
              step={1}
              onValueChange={([value]) => setSelectedRun(value)}
              className="cursor-pointer"
            />
          </div>

          <Button
            variant="ghost"
            size="icon"
            className="h-8 w-8"
            disabled={selectedRun === mockRunHistory.length - 1}
            onClick={() => setSelectedRun((prev) => Math.min(mockRunHistory.length - 1, prev + 1))}
          >
            <ChevronRight className="h-4 w-4" />
          </Button>
        </div>

        {/* Timeline markers */}
        <div className="flex justify-between px-10 text-xs text-muted-foreground">
          {mockRunHistory.map((run, index) => (
            <button
              key={run.id}
              onClick={() => setSelectedRun(index)}
              className={cn(
                "flex flex-col items-center gap-1 transition-colors",
                selectedRun === index && "text-primary font-medium"
              )}
            >
              <div
                className={cn(
                  "w-2 h-2 rounded-full",
                  selectedRun === index ? "bg-primary" : "bg-muted-foreground/50"
                )}
              />
              <span className="whitespace-nowrap">{formatDate(run.timestamp)}</span>
            </button>
          ))}
        </div>
      </div>

      {/* Compare Mode Selector */}
      {compareMode && (
        <div className="mt-4 p-3 rounded-lg border border-primary/30 bg-primary/5">
          <div className="flex items-center gap-2 mb-2">
            <GitCompare className="h-4 w-4 text-primary" />
            <span className="text-sm font-medium">Compare with:</span>
          </div>
          <div className="flex gap-2">
            {mockRunHistory.map((run, index) => (
              <Button
                key={run.id}
                variant={compareRun === index ? "default" : "outline"}
                size="sm"
                onClick={() => setCompareRun(index)}
                disabled={index === selectedRun}
                className="text-xs"
              >
                {formatDate(run.timestamp)}
              </Button>
            ))}
          </div>
        </div>
      )}

      {/* Snapshot Details */}
      <div className={cn(
        "mt-4 grid gap-4",
        compareMode ? "grid-cols-2" : "grid-cols-1"
      )}>
        {/* Current Snapshot */}
        <div className={cn(
          "p-4 rounded-lg border",
          compareMode ? "border-primary/30 bg-primary/5" : "border-border bg-card"
        )}>
          <div className="flex items-center justify-between mb-3">
            <div className="flex items-center gap-2">
              <Play className="h-4 w-4 text-primary" />
              <span className="text-sm font-medium">
                {formatDate(currentSnapshot.timestamp)}
              </span>
            </div>
            {selectedRun === mockRunHistory.length - 1 && (
              <span className="px-2 py-0.5 rounded-full bg-success/20 text-success text-xs">
                Latest
              </span>
            )}
          </div>

          <div className="grid grid-cols-2 gap-3 text-sm">
            <div className="p-2 rounded bg-muted/50">
              <div className="text-lg font-bold">{currentSnapshot.rowCount}</div>
              <div className="text-xs text-muted-foreground">Rows</div>
            </div>
            <div className="p-2 rounded bg-muted/50">
              <div className="text-lg font-bold">{currentSnapshot.columns}</div>
              <div className="text-xs text-muted-foreground">Columns</div>
            </div>
          </div>

          {/* Profile Changes */}
          <div className="mt-3 space-y-2">
            <p className="text-xs text-muted-foreground font-medium uppercase tracking-wider">Profile</p>
            {Object.entries(currentSnapshot.profile).map(([column, stats]) => (
              <div key={column} className="flex items-center justify-between text-xs">
                <span className="font-mono">{column}</span>
                <div className="flex items-center gap-2">
                  <span className="text-muted-foreground">{stats.unique} unique</span>
                  {stats.nullCount > 0 && (
                    <span className="text-warning">{stats.nullCount} null</span>
                  )}
                </div>
              </div>
            ))}
          </div>
        </div>

        {/* Compare Snapshot */}
        {compareMode && compareSnapshot && (
          <div className="p-4 rounded-lg border border-muted bg-muted/30">
            <div className="flex items-center justify-between mb-3">
              <div className="flex items-center gap-2">
                <RotateCcw className="h-4 w-4 text-muted-foreground" />
                <span className="text-sm font-medium">
                  {formatDate(compareSnapshot.timestamp)}
                </span>
              </div>
            </div>

            <div className="grid grid-cols-2 gap-3 text-sm">
              <div className="p-2 rounded bg-background">
                <div className="flex items-center gap-1">
                  <span className="text-lg font-bold">{compareSnapshot.rowCount}</span>
                  {(() => {
                    const diff = getDiff(currentSnapshot.rowCount, compareSnapshot.rowCount);
                    return (
                      <span className={cn("text-xs flex items-center", diff.color)}>
                        <diff.icon className="h-3 w-3" />
                        {diff.value}
                      </span>
                    );
                  })()}
                </div>
                <div className="text-xs text-muted-foreground">Rows</div>
              </div>
              <div className="p-2 rounded bg-background">
                <div className="text-lg font-bold">{compareSnapshot.columns}</div>
                <div className="text-xs text-muted-foreground">Columns</div>
              </div>
            </div>

            {/* Profile Changes with Diff */}
            <div className="mt-3 space-y-2">
              <p className="text-xs text-muted-foreground font-medium uppercase tracking-wider">Changes</p>
              {Object.entries(currentSnapshot.profile).map(([column, currentStats]) => {
                const prevStats = compareSnapshot.profile[column];
                if (!prevStats) return null;
                
                const uniqueDiff = getDiff(currentStats.unique, prevStats.unique);
                const nullDiff = getDiff(currentStats.nullCount, prevStats.nullCount);
                
                return (
                  <div key={column} className="flex items-center justify-between text-xs">
                    <span className="font-mono">{column}</span>
                    <div className="flex items-center gap-2">
                      <span className={cn("flex items-center gap-0.5", uniqueDiff.color)}>
                        <uniqueDiff.icon className="h-3 w-3" />
                        {uniqueDiff.value}
                      </span>
                      {(currentStats.nullCount > 0 || prevStats.nullCount > 0) && (
                        <span className={cn("flex items-center gap-0.5", nullDiff.color)}>
                          null: {nullDiff.value}
                        </span>
                      )}
                    </div>
                  </div>
                );
              })}
            </div>
          </div>
        )}
      </div>
    </div>
  );
};

export default TimeTravel;
