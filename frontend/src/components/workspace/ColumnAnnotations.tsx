import { useState } from "react";
import { MessageSquare, Plus, X, Check, Pin } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Textarea } from "@/components/ui/textarea";
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover";
import { cn } from "@/lib/utils";

interface ColumnAnnotation {
  id: string;
  column: string;
  note: string;
  author: string;
  timestamp: string;
  pinned?: boolean;
}

interface ColumnAnnotationsProps {
  column: string;
  annotations: ColumnAnnotation[];
  onAddAnnotation: (column: string, note: string) => void;
  onDeleteAnnotation: (id: string) => void;
  onTogglePin: (id: string) => void;
}

export const AnnotationBadge = ({ 
  column, 
  annotations, 
  onAddAnnotation, 
  onDeleteAnnotation,
  onTogglePin 
}: ColumnAnnotationsProps) => {
  const [isOpen, setIsOpen] = useState(false);
  const [isAdding, setIsAdding] = useState(false);
  const [newNote, setNewNote] = useState("");

  const columnAnnotations = annotations.filter((a) => a.column === column);
  const hasAnnotations = columnAnnotations.length > 0;
  const hasPinned = columnAnnotations.some((a) => a.pinned);

  const handleSubmit = () => {
    if (newNote.trim()) {
      onAddAnnotation(column, newNote.trim());
      setNewNote("");
      setIsAdding(false);
    }
  };

  return (
    <Popover open={isOpen} onOpenChange={setIsOpen}>
      <PopoverTrigger asChild>
        <button
          className={cn(
            "relative p-1 rounded transition-all duration-200",
            hasAnnotations
              ? "text-warning hover:bg-warning/20"
              : "text-muted-foreground/50 hover:text-muted-foreground hover:bg-muted"
          )}
        >
          <MessageSquare className="h-3.5 w-3.5" />
          {hasAnnotations && (
            <span className="absolute -top-1 -right-1 w-3.5 h-3.5 rounded-full bg-warning text-warning-foreground text-[10px] flex items-center justify-center font-bold">
              {columnAnnotations.length}
            </span>
          )}
          {hasPinned && (
            <Pin className="absolute -bottom-0.5 -right-0.5 h-2.5 w-2.5 text-primary" />
          )}
        </button>
      </PopoverTrigger>
      <PopoverContent className="w-72 p-0" align="start">
        <div className="p-3 border-b border-border">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-2">
              <MessageSquare className="h-4 w-4 text-warning" />
              <span className="font-medium text-sm">{column}</span>
            </div>
            <Button
              variant="ghost"
              size="sm"
              onClick={() => setIsAdding(true)}
              className="h-7 px-2"
            >
              <Plus className="h-3.5 w-3.5" />
            </Button>
          </div>
        </div>

        <div className="max-h-64 overflow-y-auto">
          {columnAnnotations.length === 0 && !isAdding && (
            <div className="p-4 text-center text-sm text-muted-foreground">
              No annotations yet
            </div>
          )}

          {columnAnnotations.map((annotation) => (
            <div
              key={annotation.id}
              className={cn(
                "p-3 border-b border-border last:border-b-0 group",
                annotation.pinned && "bg-primary/5"
              )}
            >
              <div className="flex items-start justify-between gap-2">
                <p className="text-sm flex-1">{annotation.note}</p>
                <div className="flex items-center gap-1 opacity-0 group-hover:opacity-100 transition-opacity">
                  <button
                    onClick={() => onTogglePin(annotation.id)}
                    className={cn(
                      "p-1 rounded hover:bg-muted",
                      annotation.pinned ? "text-primary" : "text-muted-foreground"
                    )}
                  >
                    <Pin className="h-3 w-3" />
                  </button>
                  <button
                    onClick={() => onDeleteAnnotation(annotation.id)}
                    className="p-1 rounded hover:bg-destructive/20 text-muted-foreground hover:text-destructive"
                  >
                    <X className="h-3 w-3" />
                  </button>
                </div>
              </div>
              <div className="mt-1 flex items-center gap-2 text-xs text-muted-foreground">
                <span>@{annotation.author}</span>
                <span>•</span>
                <span>{new Date(annotation.timestamp).toLocaleDateString()}</span>
              </div>
            </div>
          ))}

          {isAdding && (
            <div className="p-3 space-y-2">
              <Textarea
                value={newNote}
                onChange={(e) => setNewNote(e.target.value)}
                placeholder="Add a note..."
                className="min-h-[60px] text-sm resize-none"
                autoFocus
              />
              <div className="flex items-center justify-between">
                <span className="text-xs text-muted-foreground">@you</span>
                <div className="flex items-center gap-1">
                  <Button
                    variant="ghost"
                    size="sm"
                    onClick={() => {
                      setIsAdding(false);
                      setNewNote("");
                    }}
                    className="h-7 px-2"
                  >
                    <X className="h-3.5 w-3.5" />
                  </Button>
                  <Button
                    size="sm"
                    onClick={handleSubmit}
                    disabled={!newNote.trim()}
                    className="h-7 px-2"
                  >
                    <Check className="h-3.5 w-3.5" />
                  </Button>
                </div>
              </div>
            </div>
          )}
        </div>
      </PopoverContent>
    </Popover>
  );
};

// Hook to manage annotations
export const useColumnAnnotations = () => {
  const [annotations, setAnnotations] = useState<ColumnAnnotation[]>([
    {
      id: "ann-1",
      column: "total_revenue",
      note: "This column shows outliers in December data. Need to verify source.",
      author: "john.doe",
      timestamp: "2024-12-20T10:00:00Z",
      pinned: true,
    },
    {
      id: "ann-2",
      column: "region",
      note: "APAC includes Australia and New Zealand as of Q4.",
      author: "jane.smith",
      timestamp: "2024-12-21T14:30:00Z",
    },
  ]);

  const addAnnotation = (column: string, note: string) => {
    const newAnnotation: ColumnAnnotation = {
      id: `ann-${Date.now()}`,
      column,
      note,
      author: "you",
      timestamp: new Date().toISOString(),
    };
    setAnnotations((prev) => [...prev, newAnnotation]);
  };

  const deleteAnnotation = (id: string) => {
    setAnnotations((prev) => prev.filter((a) => a.id !== id));
  };

  const togglePin = (id: string) => {
    setAnnotations((prev) =>
      prev.map((a) => (a.id === id ? { ...a, pinned: !a.pinned } : a))
    );
  };

  return { annotations, addAnnotation, deleteAnnotation, togglePin };
};

export default AnnotationBadge;
