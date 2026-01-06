import { Lightbulb, PieChart, FileText, MessageSquare, Loader2 } from "lucide-react";
import { Button } from "@/components/ui/button";

type ActionType = "insights" | "charts" | "doc" | "explain";

interface AIActionButtonsProps {
  isGenerating: boolean;
  currentAction: ActionType | null;
  selectedColumn: string | null;
  onGenerateInsights: () => void;
  onRecommendCharts: () => void;
  onGenerateDoc: () => void;
  onExplainColumn: () => void;
}

const AIActionButtons = ({
  isGenerating,
  currentAction,
  selectedColumn,
  onGenerateInsights,
  onRecommendCharts,
  onGenerateDoc,
  onExplainColumn,
}: AIActionButtonsProps) => {
  return (
    <div className="space-y-4">
      <div>
        <p className="text-[10px] uppercase tracking-wider text-muted-foreground mb-2">
          Generate
        </p>
        <div className="grid grid-cols-1 sm:grid-cols-3 gap-2">
          <Button
            variant="surface"
            size="sm"
            className="w-full justify-start gap-2 h-10 sm:h-9"
            onClick={onGenerateInsights}
            disabled={isGenerating}
          >
            {currentAction === "insights" ? (
              <Loader2 className="w-4 h-4 sm:w-3.5 sm:h-3.5 animate-spin" />
            ) : (
              <Lightbulb className="w-4 h-4 sm:w-3.5 sm:h-3.5 text-[hsl(var(--viz-yellow))]" />
            )}
            <span className="text-sm sm:text-xs">Insights</span>
          </Button>
          <Button
            variant="surface"
            size="sm"
            className="w-full justify-start gap-2 h-10 sm:h-9"
            onClick={onRecommendCharts}
            disabled={isGenerating}
          >
            {currentAction === "charts" ? (
              <Loader2 className="w-4 h-4 sm:w-3.5 sm:h-3.5 animate-spin" />
            ) : (
              <PieChart className="w-4 h-4 sm:w-3.5 sm:h-3.5 text-[hsl(var(--viz-cyan))]" />
            )}
            <span className="text-sm sm:text-xs">Charts</span>
          </Button>
          <Button
            variant="surface"
            size="sm"
            className="w-full justify-start gap-2 h-10 sm:h-9"
            onClick={onGenerateDoc}
            disabled={isGenerating}
          >
            {currentAction === "doc" ? (
              <Loader2 className="w-4 h-4 sm:w-3.5 sm:h-3.5 animate-spin" />
            ) : (
              <FileText className="w-4 h-4 sm:w-3.5 sm:h-3.5 text-[hsl(var(--viz-green))]" />
            )}
            <span className="text-sm sm:text-xs">Doc</span>
          </Button>
        </div>
      </div>

      <div>
        <p className="text-[10px] uppercase tracking-wider text-muted-foreground mb-2">
          Explain
        </p>
        <Button
          variant="surface"
          size="sm"
          className="w-full justify-start gap-2 h-10 sm:h-9 text-left"
          onClick={onExplainColumn}
          disabled={isGenerating || !selectedColumn}
        >
          {currentAction === "explain" ? (
            <Loader2 className="w-4 h-4 sm:w-3.5 sm:h-3.5 animate-spin" />
          ) : (
            <MessageSquare className="w-4 h-4 sm:w-3.5 sm:h-3.5 text-[hsl(var(--viz-purple))]" />
          )}
          <span className="truncate text-sm sm:text-xs">
            {selectedColumn ? `Explain: ${selectedColumn}` : "Select a column"}
          </span>
        </Button>
      </div>
    </div>
  );
};

export default AIActionButtons;
