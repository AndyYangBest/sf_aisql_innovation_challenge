import { memo } from "react";
import { Pin, Trash2, GripVertical } from "lucide-react";
import { Button } from "@/components/ui/button";
import { ChartSpec } from "@/lib/chartUtils";
import ChartRenderer from "./ChartRenderer";
import { cn } from "@/lib/utils";

interface ChartCardProps {
  spec: ChartSpec;
  isPinned?: boolean;
  onPin?: () => void;
  onDelete?: () => void;
  isDragging?: boolean;
}

const ChartCard = memo(({ spec, isPinned, onPin, onDelete, isDragging }: ChartCardProps) => {
  return (
    <div
      className={cn(
        "h-full flex flex-col bg-card border border-border rounded-lg overflow-hidden transition-shadow",
        isDragging && "shadow-xl ring-2 ring-primary/50"
      )}
    >
      {/* Header */}
      <div className="flex items-center justify-between px-3 py-2 border-b border-border bg-secondary/30">
        <div className="flex items-center gap-2">
          <div className="chart-drag-handle cursor-grab active:cursor-grabbing p-1 -ml-1 hover:bg-muted rounded">
            <GripVertical className="w-4 h-4 text-muted-foreground" />
          </div>
          <h3 className="text-sm font-medium truncate">{spec.title}</h3>
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
    </div>
  );
});

ChartCard.displayName = "ChartCard";

export default ChartCard;
