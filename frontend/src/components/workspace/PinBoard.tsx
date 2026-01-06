import { useState } from "react";
import { useDraggable, useDroppable, DndContext, DragEndEvent, DragOverlay, DragStartEvent } from "@dnd-kit/core";
import { Pin, X, GripVertical, Lightbulb, BarChart3, FileText, MessageSquare, ChevronDown, ChevronUp, Trash2 } from "lucide-react";
import { Button } from "@/components/ui/button";
import { cn } from "@/lib/utils";
import { Artifact } from "@/types";
import { useTableStore } from "@/store/tableStore";

interface PinBoardProps {
  tableId: string;
  isOpen: boolean;
  onToggle: () => void;
}

// Draggable Artifact Card
const DraggableCard = ({ artifact, onUnpin, onDelete }: { artifact: Artifact; onUnpin: () => void; onDelete: () => void }) => {
  const { attributes, listeners, setNodeRef, transform, isDragging } = useDraggable({
    id: artifact.id,
    data: artifact,
  });

  const style = transform
    ? {
        transform: `translate3d(${transform.x}px, ${transform.y}px, 0)`,
        zIndex: isDragging ? 50 : 1,
      }
    : undefined;

  const getIcon = () => {
    switch (artifact.type) {
      case "insight": return Lightbulb;
      case "chart": return BarChart3;
      case "doc": return FileText;
      case "annotation": return MessageSquare;
    }
  };

  const getColor = () => {
    switch (artifact.type) {
      case "insight": return "text-warning border-warning/30 bg-warning/5";
      case "chart": return "text-info border-info/30 bg-info/5";
      case "doc": return "text-accent border-accent/30 bg-accent/5";
      case "annotation": return "text-success border-success/30 bg-success/5";
    }
  };

  const Icon = getIcon();

  return (
    <div
      ref={setNodeRef}
      style={style}
      className={cn(
        "p-3 rounded-lg border transition-all duration-200",
        getColor(),
        isDragging && "opacity-50 shadow-lg"
      )}
    >
      <div className="flex items-start gap-2">
        <button
          {...attributes}
          {...listeners}
          className="p-1 rounded hover:bg-muted/50 cursor-grab active:cursor-grabbing"
        >
          <GripVertical className="h-3 w-3 text-muted-foreground" />
        </button>
        
        <Icon className="h-4 w-4 mt-0.5 shrink-0" />
        
        <div className="flex-1 min-w-0">
          <p className="text-xs font-medium truncate">
            {artifact.type === "insight" && artifact.content.title}
            {artifact.type === "chart" && artifact.content.title}
            {artifact.type === "doc" && (artifact.content.title || "Documentation")}
            {artifact.type === "annotation" && `Note: ${artifact.content.target}`}
          </p>
          <p className="text-xs text-muted-foreground mt-0.5">
            {new Date(artifact.createdAt).toLocaleDateString()}
          </p>
        </div>

        <div className="flex items-center gap-1">
          <button
            onClick={onUnpin}
            className="p-1 rounded hover:bg-muted/50 text-muted-foreground hover:text-foreground"
          >
            <Pin className="h-3 w-3" />
          </button>
          <button
            onClick={onDelete}
            className="p-1 rounded hover:bg-destructive/20 text-muted-foreground hover:text-destructive"
          >
            <Trash2 className="h-3 w-3" />
          </button>
        </div>
      </div>

      {/* Content Preview */}
      {artifact.type === "insight" && (
        <ul className="mt-2 space-y-0.5">
          {artifact.content.bullets.slice(0, 2).map((b, i) => (
            <li key={i} className="text-xs text-muted-foreground flex items-start gap-1">
              <span className="shrink-0">•</span>
              <span className="line-clamp-1">{b}</span>
            </li>
          ))}
        </ul>
      )}

      {artifact.type === "chart" && (
        <div className="mt-2 h-8 rounded bg-muted/30 flex items-center justify-center">
          <span className="text-xs text-muted-foreground">{artifact.content.chartType} chart</span>
        </div>
      )}
    </div>
  );
};

// Drop Zone for Storyline/Dashboard
const DropZone = ({ id, label, children, isOver }: { id: string; label: string; children?: React.ReactNode; isOver?: boolean }) => {
  const { setNodeRef, isOver: dropIsOver } = useDroppable({ id });

  return (
    <div
      ref={setNodeRef}
      className={cn(
        "p-2 rounded-lg border-2 border-dashed transition-all duration-200",
        dropIsOver || isOver
          ? "border-primary bg-primary/10"
          : "border-border/50 hover:border-border"
      )}
    >
      <p className="text-xs text-muted-foreground font-medium mb-2">{label}</p>
      <div className="min-h-[40px] space-y-2">
        {children}
      </div>
    </div>
  );
};

const PinBoard = ({ tableId, isOpen, onToggle }: PinBoardProps) => {
  const { getArtifactsByTable, toggleArtifactPin, deleteArtifact } = useTableStore();
  const [storyline, setStoryline] = useState<Artifact[]>([]);
  const [dashboardStrip, setDashboardStrip] = useState<Artifact[]>([]);
  const [activeId, setActiveId] = useState<string | null>(null);

  const artifacts = getArtifactsByTable(tableId);
  const pinnedArtifacts = artifacts.filter((a) => a.pinned);

  const handleDragStart = (event: DragStartEvent) => {
    setActiveId(event.active.id as string);
  };

  const handleDragEnd = (event: DragEndEvent) => {
    const { active, over } = event;
    setActiveId(null);

    if (over) {
      const artifact = artifacts.find((a) => a.id === active.id);
      if (!artifact) return;

      if (over.id === "storyline") {
        if (!storyline.find((a) => a.id === artifact.id)) {
          setStoryline((prev) => [...prev, artifact]);
        }
      } else if (over.id === "dashboard") {
        if (!dashboardStrip.find((a) => a.id === artifact.id)) {
          setDashboardStrip((prev) => [...prev, artifact]);
        }
      }
    }
  };

  const removeFromStoryline = (id: string) => {
    setStoryline((prev) => prev.filter((a) => a.id !== id));
  };

  const removeFromDashboard = (id: string) => {
    setDashboardStrip((prev) => prev.filter((a) => a.id !== id));
  };

  const activeArtifact = activeId ? artifacts.find((a) => a.id === activeId) : null;

  return (
    <DndContext onDragStart={handleDragStart} onDragEnd={handleDragEnd}>
      <div className={cn(
        "border-t border-border/50 bg-sidebar transition-all duration-300",
        isOpen ? "h-auto" : "h-10"
      )}>
        {/* Header */}
        <button
          onClick={onToggle}
          className="w-full px-4 py-2 flex items-center justify-between hover:bg-muted/30 transition-colors"
        >
          <div className="flex items-center gap-2">
            <Pin className="h-4 w-4 text-primary" />
            <span className="text-sm font-medium">Pin Board</span>
            {pinnedArtifacts.length > 0 && (
              <span className="px-1.5 py-0.5 rounded-full bg-primary/20 text-primary text-xs">
                {pinnedArtifacts.length}
              </span>
            )}
          </div>
          {isOpen ? <ChevronDown className="h-4 w-4" /> : <ChevronUp className="h-4 w-4" />}
        </button>

        {isOpen && (
          <div className="p-4 space-y-4 animate-fade-in">
            {/* Pinned Artifacts */}
            <div>
              <p className="text-xs text-muted-foreground font-medium uppercase tracking-wider mb-2">
                Pinned ({pinnedArtifacts.length})
              </p>
              {pinnedArtifacts.length === 0 ? (
                <div className="p-4 rounded-lg border border-dashed border-border/50 text-center">
                  <Pin className="h-5 w-5 mx-auto mb-1 text-muted-foreground/50" />
                  <p className="text-xs text-muted-foreground">Pin insights, charts, or notes here</p>
                </div>
              ) : (
                <div className="space-y-2">
                  {pinnedArtifacts.map((artifact) => (
                    <DraggableCard
                      key={artifact.id}
                      artifact={artifact}
                      onUnpin={() => toggleArtifactPin(artifact.id)}
                      onDelete={() => deleteArtifact(artifact.id)}
                    />
                  ))}
                </div>
              )}
            </div>

            {/* Drop Zones */}
            <div className="grid grid-cols-2 gap-3">
              <DropZone id="storyline" label="📖 Storyline">
                {storyline.map((artifact) => (
                  <div key={artifact.id} className="flex items-center gap-2 p-2 rounded bg-muted/30 text-xs">
                    <span className="flex-1 truncate">
                      {artifact.type === "insight" && artifact.content.title}
                      {artifact.type === "chart" && artifact.content.title}
                      {artifact.type === "doc" && "Doc"}
                    </span>
                    <button onClick={() => removeFromStoryline(artifact.id)}>
                      <X className="h-3 w-3 text-muted-foreground hover:text-foreground" />
                    </button>
                  </div>
                ))}
              </DropZone>
              
              <DropZone id="dashboard" label="📊 Dashboard Strip">
                {dashboardStrip.map((artifact) => (
                  <div key={artifact.id} className="flex items-center gap-2 p-2 rounded bg-muted/30 text-xs">
                    <span className="flex-1 truncate">
                      {artifact.type === "insight" && artifact.content.title}
                      {artifact.type === "chart" && artifact.content.title}
                      {artifact.type === "doc" && "Doc"}
                    </span>
                    <button onClick={() => removeFromDashboard(artifact.id)}>
                      <X className="h-3 w-3 text-muted-foreground hover:text-foreground" />
                    </button>
                  </div>
                ))}
              </DropZone>
            </div>

            {/* Export Button */}
            {(storyline.length > 0 || dashboardStrip.length > 0) && (
              <Button variant="outline" size="sm" className="w-full">
                <FileText className="h-4 w-4 mr-2" />
                Export as Summary
              </Button>
            )}
          </div>
        )}
      </div>

      {/* Drag Overlay */}
      <DragOverlay>
        {activeArtifact && (
          <div className="p-3 rounded-lg border bg-card shadow-lg opacity-90">
            <p className="text-xs font-medium">
              {activeArtifact.type === "insight" && activeArtifact.content.title}
              {activeArtifact.type === "chart" && activeArtifact.content.title}
              {activeArtifact.type === "doc" && "Documentation"}
            </p>
          </div>
        )}
      </DragOverlay>
    </DndContext>
  );
};

export default PinBoard;
