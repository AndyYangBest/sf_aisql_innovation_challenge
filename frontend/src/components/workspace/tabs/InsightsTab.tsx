import { Lightbulb, Pin, Trash2, MessageSquare, Clock, MoreHorizontal } from "lucide-react";
import { useTableStore } from "@/store/tableStore";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";

interface InsightsTabProps {
  tableId: string;
}

const InsightsTab = ({ tableId }: InsightsTabProps) => {
  const { getArtifactsByTable, deleteArtifact, toggleArtifactPin } = useTableStore();
  const artifacts = getArtifactsByTable(tableId);
  const insightArtifacts = artifacts.filter((a) => a.type === "insight");

  const formatDate = (date: string) =>
    new Date(date).toLocaleDateString("en-US", {
      month: "short",
      day: "numeric",
      year: "numeric",
    });

  return (
    <div className="space-y-6 animate-fade-in max-w-4xl">
      <div>
        <h2 className="text-lg font-semibold mb-1">Insights</h2>
        <p className="text-sm text-muted-foreground">
          Reusable findings and conclusions from analysis. These can be pinned, commented on, and shared.
        </p>
      </div>

      {insightArtifacts.length === 0 ? (
        <div className="flex items-center justify-center h-64 text-muted-foreground">
          <div className="text-center">
            <Lightbulb className="h-12 w-12 mx-auto mb-3 opacity-50" />
            <p className="mb-2">No insights yet</p>
            <p className="text-sm">Use "Generate Insights" in the AI Actions panel to discover key findings</p>
          </div>
        </div>
      ) : (
        <div className="space-y-4">
          {/* Pinned Insights */}
          {insightArtifacts.filter((a) => a.pinned).length > 0 && (
            <div className="space-y-3">
              <h3 className="text-sm font-medium text-muted-foreground uppercase tracking-wider">Pinned</h3>
              {insightArtifacts
                .filter((a) => a.pinned)
                .map((artifact) => (
                  <InsightCard
                    key={artifact.id}
                    artifact={artifact}
                    onPin={() => toggleArtifactPin(artifact.id)}
                    onDelete={() => deleteArtifact(artifact.id)}
                    formatDate={formatDate}
                  />
                ))}
            </div>
          )}

          {/* All Insights */}
          <div className="space-y-3">
            {insightArtifacts.filter((a) => a.pinned).length > 0 && (
              <h3 className="text-sm font-medium text-muted-foreground uppercase tracking-wider">All Insights</h3>
            )}
            {insightArtifacts
              .filter((a) => !a.pinned)
              .map((artifact) => (
                <InsightCard
                  key={artifact.id}
                  artifact={artifact}
                  onPin={() => toggleArtifactPin(artifact.id)}
                  onDelete={() => deleteArtifact(artifact.id)}
                  formatDate={formatDate}
                />
              ))}
          </div>
        </div>
      )}
    </div>
  );
};

interface InsightCardProps {
  artifact: any;
  onPin: () => void;
  onDelete: () => void;
  formatDate: (date: string) => string;
}

const InsightCard = ({ artifact, onPin, onDelete, formatDate }: InsightCardProps) => {
  if (artifact.type !== "insight") return null;

  return (
    <div className="p-5 rounded-xl glass animate-slide-up">
      <div className="flex items-start justify-between mb-3">
        <div className="flex items-center gap-2">
          <div className="p-1.5 rounded bg-warning/10">
            <Lightbulb className="h-4 w-4 text-warning" />
          </div>
          <h3 className="font-medium">{artifact.content.title}</h3>
          {artifact.pinned && (
            <Badge variant="outline" className="text-xs text-primary border-primary/30">
              Pinned
            </Badge>
          )}
        </div>
        <DropdownMenu>
          <DropdownMenuTrigger asChild>
            <Button variant="ghost" size="icon" className="h-8 w-8">
              <MoreHorizontal className="h-4 w-4" />
            </Button>
          </DropdownMenuTrigger>
          <DropdownMenuContent align="end">
            <DropdownMenuItem onClick={onPin}>
              <Pin className="h-4 w-4 mr-2" />
              {artifact.pinned ? "Unpin" : "Pin"}
            </DropdownMenuItem>
            <DropdownMenuItem>
              <MessageSquare className="h-4 w-4 mr-2" />
              Comment
            </DropdownMenuItem>
            <DropdownMenuItem onClick={onDelete} className="text-destructive">
              <Trash2 className="h-4 w-4 mr-2" />
              Delete
            </DropdownMenuItem>
          </DropdownMenuContent>
        </DropdownMenu>
      </div>

      {artifact.content.summary && (
        <p className="text-sm text-muted-foreground mb-3">{artifact.content.summary}</p>
      )}

      <ul className="space-y-2 mb-4">
        {artifact.content.bullets.map((bullet: string, i: number) => (
          <li key={i} className="flex items-start gap-2 text-sm">
            <span className="text-primary mt-1">•</span>
            <span>{bullet}</span>
          </li>
        ))}
      </ul>

      <div className="flex items-center gap-4 text-xs text-muted-foreground pt-3 border-t border-border/50">
        <span className="flex items-center gap-1">
          <Clock className="h-3 w-3" />
          {formatDate(artifact.createdAt)}
        </span>
        {artifact.author && <span>by {artifact.author}</span>}
        {artifact.content.sourceColumns && artifact.content.sourceColumns.length > 0 && (
          <span>from {artifact.content.sourceColumns.join(", ")}</span>
        )}
      </div>
    </div>
  );
};

export default InsightsTab;
