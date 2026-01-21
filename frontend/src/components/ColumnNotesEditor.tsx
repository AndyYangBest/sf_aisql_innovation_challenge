import { useEffect, useMemo, useState } from "react";
import { Checkbox } from "@/components/ui/checkbox";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Textarea } from "@/components/ui/textarea";
import { cn } from "@/lib/utils";

export type ColumnNotesMap = Record<string, string>;

interface ColumnNotesEditorProps {
  columns: string[];
  value: ColumnNotesMap;
  onChange: (next: ColumnNotesMap) => void;
  className?: string;
  label?: string;
}

const ColumnNotesEditor = ({
  columns,
  value,
  onChange,
  className,
  label = "Column Notes",
}: ColumnNotesEditorProps) => {
  const [filter, setFilter] = useState("");
  const [activeColumn, setActiveColumn] = useState<string | null>(null);

  const filteredColumns = useMemo(() => {
    const needle = filter.trim().toLowerCase();
    if (!needle) return columns;
    return columns.filter((col) => col.toLowerCase().includes(needle));
  }, [columns, filter]);

  const selectedColumns = useMemo(
    () => columns.filter((col) => Object.prototype.hasOwnProperty.call(value, col)),
    [columns, value],
  );

  useEffect(() => {
    if (activeColumn && selectedColumns.includes(activeColumn)) {
      return;
    }
    setActiveColumn(selectedColumns[0] ?? null);
  }, [activeColumn, selectedColumns]);

  const applySelection = (next: ColumnNotesMap) => {
    const nextSelected = columns.filter((col) => Object.prototype.hasOwnProperty.call(next, col));
    onChange(next);
    if (!nextSelected.length) {
      setActiveColumn(null);
      return;
    }
    if (!activeColumn || !nextSelected.includes(activeColumn)) {
      setActiveColumn(nextSelected[0]);
    }
  };

  const toggleColumn = (column: string) => {
    const wasSelected = Object.prototype.hasOwnProperty.call(value, column);
    const next = { ...value };
    if (wasSelected) {
      delete next[column];
    } else {
      next[column] = "";
    }
    applySelection(next);
    if (!wasSelected) {
      setActiveColumn(column);
    }
  };

  const handleRowClick = (column: string) => {
    if (Object.prototype.hasOwnProperty.call(value, column)) {
      setActiveColumn(column);
      return;
    }
    const next = { ...value, [column]: "" };
    applySelection(next);
    setActiveColumn(column);
  };

  const handleNoteChange = (text: string) => {
    if (!activeColumn) return;
    onChange({ ...value, [activeColumn]: text });
  };

  return (
    <div className={cn("space-y-3 rounded-lg border border-border bg-muted/20 p-3", className)}>
      <div className="flex items-center justify-between">
        <Label>{label}</Label>
        <span className="text-xs text-muted-foreground">{selectedColumns.length} selected</span>
      </div>

      <Input
        value={filter}
        onChange={(e) => setFilter(e.target.value)}
        placeholder="Filter columns..."
        className="h-9 bg-background"
      />

      <ScrollArea className="h-28 rounded-md border border-border bg-background">
        <div className="space-y-1 p-2">
          {filteredColumns.length === 0 ? (
            <div className="py-6 text-center text-xs text-muted-foreground">No matching columns</div>
          ) : (
            filteredColumns.map((column) => {
              const isSelected = Object.prototype.hasOwnProperty.call(value, column);
              const isActive = activeColumn === column;
              return (
                <div
                  key={column}
                  className={cn(
                    "flex items-center gap-2 rounded-md px-2 py-1 text-sm transition-colors",
                    isActive && "bg-accent/60",
                  )}
                  onClick={() => handleRowClick(column)}
                >
                  <Checkbox
                    checked={isSelected}
                    onCheckedChange={() => toggleColumn(column)}
                    onClick={(event) => event.stopPropagation()}
                  />
                  <span className="truncate">{column}</span>
                </div>
              );
            })
          )}
        </div>
      </ScrollArea>

      {activeColumn ? (
        <div className="space-y-2">
          <Label className="text-xs text-muted-foreground">Note for {activeColumn}</Label>
          <Textarea
            value={value[activeColumn] ?? ""}
            onChange={(e) => handleNoteChange(e.target.value)}
            className="min-h-[90px] bg-background text-sm"
            placeholder="Add context, definitions, or usage notes..."
          />
        </div>
      ) : (
        <div className="text-xs text-muted-foreground">Select columns to add notes.</div>
      )}
    </div>
  );
};

export default ColumnNotesEditor;
