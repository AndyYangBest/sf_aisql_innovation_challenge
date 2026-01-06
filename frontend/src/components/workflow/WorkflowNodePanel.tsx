/**
 * WorkflowNodePanel - 节点选择面板
 */

import { Database, Sparkles, BarChart3, Lightbulb, Shuffle, Download } from "lucide-react";
import { NODE_DEFINITIONS, WorkflowNodeType } from "@/types/workflow";
import { cn } from "@/lib/utils";

interface WorkflowNodePanelProps {
  onAddNode: (type: WorkflowNodeType) => void;
}

const iconMap: Record<string, React.ComponentType<{ className?: string }>> = {
  Database,
  Sparkles,
  BarChart3,
  Lightbulb,
  Shuffle,
  Download,
};

const categoryLabels: Record<string, string> = {
  source: "Sources",
  transform: "Transform",
  ai: "AI",
  output: "Output",
};

const WorkflowNodePanel = ({ onAddNode }: WorkflowNodePanelProps) => {
  // Group nodes by category
  const groupedNodes = Object.entries(NODE_DEFINITIONS).reduce(
    (acc, [key, def]) => {
      if (!acc[def.category]) acc[def.category] = [];
      acc[def.category].push({ key, ...def });
      return acc;
    },
    {} as Record<string, Array<typeof NODE_DEFINITIONS[string] & { key: string }>>
  );

  return (
    <div className="w-48 border-r border-border bg-muted/30 flex flex-col">
      <div className="p-3 border-b border-border">
        <h3 className="text-xs font-medium text-muted-foreground uppercase tracking-wider">
          Nodes
        </h3>
      </div>

      <div className="flex-1 overflow-y-auto p-2 space-y-4">
        {Object.entries(categoryLabels).map(([category, label]) => {
          const nodes = groupedNodes[category];
          if (!nodes?.length) return null;

          return (
            <div key={category}>
              <p className="text-[10px] font-medium text-muted-foreground uppercase tracking-wider px-2 mb-2">
                {label}
              </p>
              <div className="space-y-1">
                {nodes.map((node) => {
                  const Icon = iconMap[node.icon] || Database;
                  return (
                    <button
                      key={node.key}
                      onClick={() => onAddNode(node.key as WorkflowNodeType)}
                      className={cn(
                        "w-full flex items-center gap-2 px-2 py-1.5 rounded-md",
                        "text-xs text-left",
                        "hover:bg-accent hover:text-accent-foreground",
                        "transition-colors cursor-grab active:cursor-grabbing"
                      )}
                      draggable
                      onDragStart={(e) => {
                        e.dataTransfer.setData("nodeType", node.key);
                      }}
                    >
                      <Icon className="h-3.5 w-3.5 text-muted-foreground" />
                      <span>{node.name}</span>
                    </button>
                  );
                })}
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
};

export default WorkflowNodePanel;
