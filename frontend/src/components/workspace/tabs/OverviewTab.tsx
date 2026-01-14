import { Sparkles, Tag, Lightbulb, BarChart3, FileText, Clock } from "lucide-react";
import { TableAsset, TableResult, InsightArtifact } from "@/types";
import { useTableStore } from "@/store/tableStore";
import { Badge } from "@/components/ui/badge";

interface OverviewTabProps {
  tableAsset: TableAsset;
  tableResult?: TableResult;
}

const OverviewTab = ({ tableAsset, tableResult }: OverviewTabProps) => {
  const { getArtifactsByTable, getReportStatus } = useTableStore();
  const artifacts = getArtifactsByTable(tableAsset.id);
  const reportStatus = getReportStatus(tableAsset.id);
  const hasReport = reportStatus?.hasReport ?? false;

  const insightCount = artifacts.filter((a) => a.type === "insight").length;
  const chartCount = artifacts.filter((a) => a.type === "chart").length;
  const pinnedInsights = artifacts.filter((a): a is InsightArtifact => a.type === "insight" && !!a.pinned);

  const aiSummary = tableAsset.aiSummary || "No summary available yet.";

  const useCases = tableAsset.useCases || [];


  const formatDate = (date: string) =>
    new Date(date).toLocaleDateString("en-US", {
      month: "short",
      day: "numeric",
      year: "numeric",
    });

  if (!hasReport) {
    return (
      <div className="flex items-center justify-center h-64 text-muted-foreground">
        <div className="text-center">
          <Sparkles className="h-12 w-12 mx-auto mb-3 opacity-50" />
          <p className="mb-2">Report is empty</p>
          <p className="text-sm">Run workflows and save outputs from the sidebar</p>
        </div>
      </div>
    );
  }

  return (
    <div className="space-y-6 max-w-4xl">
      {/* AI Summary */}
      <div className="bg-gradient-to-br from-primary/5 to-[hsl(var(--viz-cyan))]/5 border border-primary/20 rounded-lg p-5">
        <div className="flex items-start gap-3">
          <div className="w-8 h-8 rounded-md bg-primary/20 flex items-center justify-center flex-shrink-0">
            <Sparkles className="w-4 h-4 text-primary" />
          </div>
          <div>
            <p className="text-sm text-muted-foreground mb-1">AI Summary</p>
            <p className="text-foreground leading-relaxed">{aiSummary}</p>
          </div>
        </div>
      </div>

      {/* Use Case Tags */}
      {useCases.length > 0 && (
        <div>
          <div className="flex items-center gap-2 mb-3">
            <Tag className="w-4 h-4 text-muted-foreground" />
            <span className="text-sm font-medium text-muted-foreground">Use Cases</span>
          </div>
          <div className="flex flex-wrap gap-2">
            {useCases.map((tag) => (
              <Badge key={tag} variant="secondary" className="px-3 py-1 text-sm">
                {tag}
              </Badge>
            ))}
          </div>
        </div>
      )}

      {/* Quick Stats */}
      <div className="grid grid-cols-4 gap-3">
        <div className="bg-secondary/30 border border-border rounded-lg p-3">
          <p className="text-xs text-muted-foreground mb-1">Columns</p>
          <p className="text-xl font-semibold">{tableResult?.columns.length || 0}</p>
        </div>
        <div className="bg-secondary/30 border border-border rounded-lg p-3">
          <p className="text-xs text-muted-foreground mb-1">Rows</p>
          <p className="text-xl font-semibold">{tableResult?.rowCount?.toLocaleString() || "-"}</p>
        </div>
        <div className="bg-secondary/30 border border-border rounded-lg p-3">
          <div className="flex items-center gap-1.5">
            <Lightbulb className="w-3 h-3 text-[hsl(var(--viz-yellow))]" />
            <p className="text-xs text-muted-foreground">Insights</p>
          </div>
          <p className="text-xl font-semibold">{insightCount}</p>
        </div>
        <div className="bg-secondary/30 border border-border rounded-lg p-3">
          <div className="flex items-center gap-1.5">
            <BarChart3 className="w-3 h-3 text-[hsl(var(--viz-cyan))]" />
            <p className="text-xs text-muted-foreground">Charts</p>
          </div>
          <p className="text-xl font-semibold">{chartCount}</p>
        </div>
      </div>

      {/* Pinned Insights */}
      {pinnedInsights.length > 0 && (
        <div>
          <div className="flex items-center gap-2 mb-3">
            <Lightbulb className="w-4 h-4 text-[hsl(var(--viz-yellow))]" />
            <span className="text-sm font-medium">Pinned Insights</span>
          </div>
          <div className="space-y-3">
            {pinnedInsights.map((insight) => (
              <div key={insight.id} className="bg-secondary/30 border border-border rounded-lg p-4">
                <h4 className="font-medium mb-2">{insight.content.title}</h4>
                <ul className="space-y-1.5">
                  {insight.content.bullets.map((bullet, i) => (
                    <li key={i} className="flex items-start gap-2 text-sm text-muted-foreground">
                      <span className="text-primary mt-0.5">•</span>
                      {bullet}
                    </li>
                  ))}
                </ul>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Recent Activity */}
      {artifacts.length > 0 && (
        <div className="bg-secondary/30 border border-border rounded-lg p-4">
          <h4 className="text-sm font-medium mb-3">Recent Activity</h4>
          <div className="space-y-2">
            {artifacts.slice(0, 4).map((artifact) => (
              <div
                key={artifact.id}
                className="flex items-center gap-3 p-2 rounded-md hover:bg-muted/30 transition-colors"
              >
                {artifact.type === "insight" && <Lightbulb className="h-4 w-4 text-[hsl(var(--viz-yellow))]" />}
                {artifact.type === "chart" && <BarChart3 className="h-4 w-4 text-[hsl(var(--viz-cyan))]" />}
                {artifact.type === "doc" && <FileText className="h-4 w-4 text-[hsl(var(--viz-green))]" />}
                {artifact.type === "annotation" && <Tag className="h-4 w-4 text-muted-foreground" />}
                <div className="flex-1 min-w-0">
                  <p className="text-sm font-medium truncate">
                    {artifact.type === "insight" && artifact.content.title}
                    {artifact.type === "chart" && artifact.content.title}
                    {artifact.type === "doc" && (artifact.content.title || "Documentation")}
                    {artifact.type === "annotation" && "Annotation"}
                  </p>
                  <p className="text-xs text-muted-foreground flex items-center gap-1">
                    <Clock className="h-3 w-3" />
                    {formatDate(artifact.createdAt)}
                  </p>
                </div>
                {artifact.pinned && (
                  <Badge variant="outline" className="text-xs text-primary border-primary/30">
                    Pinned
                  </Badge>
                )}
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Schema Preview */}
      {tableResult && (
        <div className="bg-secondary/30 border border-border rounded-lg p-4">
          <h4 className="text-sm font-medium mb-3">Schema</h4>
          <div className="grid gap-1.5">
            {tableResult.columns.map((col) => (
              <div
                key={col.name}
                className="flex items-center justify-between py-1.5 px-2 rounded bg-background text-sm"
              >
                <span className="font-mono">{col.name}</span>
                <span className="text-xs text-muted-foreground font-mono">{col.type}</span>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
};

export default OverviewTab;
