import { Sparkles, Save, Loader2, BarChart3 } from "lucide-react";
import { Button } from "@/components/ui/button";

type ActionType = "insights" | "charts" | "doc" | "explain";

interface AIOutputDisplayProps {
  isGenerating: boolean;
  output: { type: ActionType; content: any } | null;
  onSaveInsight: (insight: { title: string; bullets: string[] }) => void;
  onSaveChart: (chart: any) => void;
  onSaveDoc: () => void;
}

const AIOutputDisplay = ({
  isGenerating,
  output,
  onSaveInsight,
  onSaveChart,
  onSaveDoc,
}: AIOutputDisplayProps) => {
  if (isGenerating) {
    return (
      <div className="flex items-center justify-center py-8">
        <div className="text-center">
          <Loader2 className="w-6 h-6 animate-spin text-primary mx-auto mb-2" />
          <p className="text-xs text-muted-foreground">Generating...</p>
        </div>
      </div>
    );
  }

  if (output?.type === "insights") {
    return (
      <div className="space-y-3 fade-in">
        {output.content.map((insight: { title: string; bullets: string[] }, idx: number) => (
          <div key={idx} className="bg-secondary/50 rounded-lg p-3 border border-border">
            <div className="flex items-center justify-between mb-2">
              <h4 className="text-sm sm:text-xs font-medium">{insight.title}</h4>
              <Button variant="ghost" size="icon-sm" onClick={() => onSaveInsight(insight)}>
                <Save className="w-4 h-4 sm:w-3.5 sm:h-3.5" />
              </Button>
            </div>
            <ul className="space-y-1.5 sm:space-y-1">
              {insight.bullets.map((b: string, i: number) => (
                <li key={i} className="flex items-start gap-2 sm:gap-1.5 text-sm sm:text-xs text-muted-foreground">
                  <span className="text-primary mt-0.5">•</span>
                  {b}
                </li>
              ))}
            </ul>
          </div>
        ))}
      </div>
    );
  }

  if (output?.type === "charts") {
    return (
      <div className="grid grid-cols-1 sm:grid-cols-1 gap-2 fade-in">
        {output.content.map((chart: any, i: number) => (
          <div key={i} className="bg-secondary/50 rounded-lg p-3 border border-border">
            <div className="flex items-center justify-between mb-1">
              <span className="text-sm sm:text-xs font-medium">{chart.title}</span>
              <Button variant="ghost" size="icon-sm" onClick={() => onSaveChart(chart)}>
                <Save className="w-4 h-4 sm:w-3.5 sm:h-3.5" />
              </Button>
            </div>
            <div className="flex items-center gap-1.5 text-xs sm:text-[10px] text-muted-foreground">
              <BarChart3 className="w-3.5 h-3.5 sm:w-3 sm:h-3" />
              {chart.chartType} chart
            </div>
          </div>
        ))}
      </div>
    );
  }

  if (output?.type === "doc") {
    return (
      <div className="space-y-2 fade-in">
        <div className="flex items-center justify-between">
          <span className="text-sm sm:text-xs font-medium">Documentation</span>
          <Button variant="ghost" size="sm" onClick={onSaveDoc}>
            <Save className="w-4 h-4 sm:w-3.5 sm:h-3.5 mr-1" />
            Save
          </Button>
        </div>
        <div className="bg-secondary/50 rounded-lg p-3 sm:p-2 border border-border max-h-48 overflow-auto scrollbar-thin">
          <pre className="text-xs sm:text-[10px] whitespace-pre-wrap font-mono text-muted-foreground">
            {output.content}
          </pre>
        </div>
      </div>
    );
  }

  if (output?.type === "explain") {
    return (
      <div className="space-y-2 fade-in">
        <span className="text-sm sm:text-xs font-medium">Column Explanation</span>
        <div className="bg-secondary/50 rounded-lg p-3 sm:p-2 border border-border max-h-64 overflow-auto scrollbar-thin">
          <p className="text-sm sm:text-xs font-mono text-primary mb-1">{output.content.column}</p>
          <p className="text-sm sm:text-xs text-muted-foreground">{output.content.explanation}</p>
        </div>
      </div>
    );
  }

  return (
    <div className="flex items-center justify-center py-8 text-muted-foreground">
      <div className="text-center">
        <Sparkles className="w-6 h-6 mx-auto mb-2 opacity-40" />
        <p className="text-sm sm:text-xs">Click an action above</p>
      </div>
    </div>
  );
};

export default AIOutputDisplay;
