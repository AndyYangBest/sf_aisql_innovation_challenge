import { useState } from "react";
import { Eye, EyeOff, Sparkles, AlertTriangle, Key, Calendar, TrendingUp, Layers, Link2, Info } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import { cn } from "@/lib/utils";
import { ColumnInfo } from "@/types";

interface ExplainModeProps {
  enabled: boolean;
  onToggle: () => void;
}

// Toggle component for the header
export const ExplainModeToggle = ({ enabled, onToggle }: ExplainModeProps) => (
  <Button
    variant={enabled ? "default" : "outline"}
    size="sm"
    onClick={onToggle}
    className={cn(
      "gap-2 transition-all",
      enabled && "glow-primary"
    )}
  >
    {enabled ? <Eye className="h-4 w-4" /> : <EyeOff className="h-4 w-4" />}
    Explain Mode
    {enabled && <Sparkles className="h-3 w-3 animate-pulse" />}
  </Button>
);

// Overlay labels for columns when Explain Mode is ON
interface ExplainOverlayProps {
  column: ColumnInfo;
  stats: {
    unique: number;
    nullCount: number;
    total: number;
  };
}

export const ExplainOverlay = ({ column, stats }: ExplainOverlayProps) => {
  const getRoleLabel = () => {
    switch (column.role) {
      case "id": return { icon: Key, label: "ID", color: "bg-primary/20 text-primary border-primary/30" };
      case "time": return { icon: Calendar, label: "Time", color: "bg-info/20 text-info border-info/30" };
      case "metric": return { icon: TrendingUp, label: "Metric", color: "bg-warning/20 text-warning border-warning/30" };
      case "dimension": return { icon: Layers, label: "Dimension", color: "bg-accent/20 text-accent border-accent/30" };
      case "foreign_key": return { icon: Link2, label: "FK", color: "bg-success/20 text-success border-success/30" };
      default: return { icon: Info, label: "Unknown", color: "bg-muted text-muted-foreground border-muted" };
    }
  };

  const getRiskLevel = () => {
    const nullPercent = (stats.nullCount / stats.total) * 100;
    const uniquePercent = (stats.unique / stats.total) * 100;

    const risks: { label: string; severity: "low" | "medium" | "high" }[] = [];

    if (nullPercent > 20) {
      risks.push({ label: "High null risk", severity: "high" });
    } else if (nullPercent > 5) {
      risks.push({ label: "Some nulls", severity: "medium" });
    }

    if (uniquePercent === 100 && column.role !== "id") {
      risks.push({ label: "All unique", severity: "low" });
    }

    if (column.role === "metric" && stats.unique < 3) {
      risks.push({ label: "Low cardinality", severity: "medium" });
    }

    return risks;
  };

  const role = getRoleLabel();
  const risks = getRiskLevel();
  const Icon = role.icon;

  return (
    <div className="absolute inset-0 pointer-events-none flex flex-col items-start justify-start p-1 gap-1 animate-fade-in">
      {/* Role Badge */}
      <Tooltip>
        <TooltipTrigger asChild>
          <div
            className={cn(
              "inline-flex items-center gap-1 px-1.5 py-0.5 rounded text-[10px] font-medium border",
              role.color
            )}
          >
            <Icon className="h-2.5 w-2.5" />
            {role.label}
          </div>
        </TooltipTrigger>
        <TooltipContent side="top" className="text-xs">
          <p>AI-detected role: {role.label}</p>
          <p className="text-muted-foreground">{column.aiExplanation || "Column classification based on name and data patterns"}</p>
        </TooltipContent>
      </Tooltip>

      {/* Risk Badges */}
      {risks.map((risk, index) => (
        <Tooltip key={index}>
          <TooltipTrigger asChild>
            <div
              className={cn(
                "inline-flex items-center gap-1 px-1.5 py-0.5 rounded text-[10px] font-medium border",
                risk.severity === "high" && "bg-destructive/20 text-destructive border-destructive/30",
                risk.severity === "medium" && "bg-warning/20 text-warning border-warning/30",
                risk.severity === "low" && "bg-muted text-muted-foreground border-muted"
              )}
            >
              <AlertTriangle className="h-2.5 w-2.5" />
              {risk.label}
            </div>
          </TooltipTrigger>
          <TooltipContent side="top" className="text-xs">
            <p>{risk.label}</p>
            <p className="text-muted-foreground">
              {risk.severity === "high" && "This may affect analysis quality"}
              {risk.severity === "medium" && "Worth investigating"}
              {risk.severity === "low" && "Informational"}
            </p>
          </TooltipContent>
        </Tooltip>
      ))}

      {/* Quality indicator */}
      <div className="absolute bottom-1 right-1">
        <div
          className={cn(
            "w-2 h-2 rounded-full",
            stats.nullCount === 0 ? "bg-success" : stats.nullCount > stats.total * 0.1 ? "bg-destructive" : "bg-warning"
          )}
        />
      </div>
    </div>
  );
};

// Summary banner when Explain Mode is ON
interface ExplainSummaryProps {
  columns: ColumnInfo[];
  getStats: (colName: string) => { unique: number; nullCount: number; total: number };
}

export const ExplainSummary = ({ columns, getStats }: ExplainSummaryProps) => {
  const roleCount = {
    id: columns.filter((c) => c.role === "id").length,
    time: columns.filter((c) => c.role === "time").length,
    metric: columns.filter((c) => c.role === "metric").length,
    dimension: columns.filter((c) => c.role === "dimension").length,
  };

  const columnsWithNulls = columns.filter((c) => getStats(c.name).nullCount > 0).length;
  const highCardColumns = columns.filter((c) => {
    const stats = getStats(c.name);
    return stats.unique === stats.total;
  }).length;

  return (
    <div className="mb-4 p-3 rounded-lg border border-primary/30 bg-primary/5 animate-fade-in">
      <div className="flex items-center gap-2 mb-2">
        <Sparkles className="h-4 w-4 text-primary" />
        <span className="font-medium text-sm">AI Explain Mode Active</span>
      </div>
      
      <div className="flex flex-wrap gap-4 text-xs">
        <div className="flex items-center gap-4">
          <span className="text-muted-foreground">Roles:</span>
          {roleCount.id > 0 && <Badge variant="outline" className="bg-primary/10 text-primary border-primary/30"><Key className="h-3 w-3 mr-1" /> {roleCount.id} ID</Badge>}
          {roleCount.time > 0 && <Badge variant="outline" className="bg-info/10 text-info border-info/30"><Calendar className="h-3 w-3 mr-1" /> {roleCount.time} Time</Badge>}
          {roleCount.metric > 0 && <Badge variant="outline" className="bg-warning/10 text-warning border-warning/30"><TrendingUp className="h-3 w-3 mr-1" /> {roleCount.metric} Metric</Badge>}
          {roleCount.dimension > 0 && <Badge variant="outline" className="bg-accent/10 text-accent border-accent/30"><Layers className="h-3 w-3 mr-1" /> {roleCount.dimension} Dimension</Badge>}
        </div>
        
        <div className="flex items-center gap-2 text-muted-foreground">
          <span>|</span>
          {columnsWithNulls > 0 && <span className="text-warning">{columnsWithNulls} with nulls</span>}
          {highCardColumns > 0 && <span>{highCardColumns} unique per row</span>}
        </div>
      </div>
    </div>
  );
};

export default ExplainModeToggle;
