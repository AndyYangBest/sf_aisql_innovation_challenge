/**
 * WorkflowCustomNode - Flowgram 自定义节点渲染
 */

import { useNodeRender, WorkflowNodeRenderer } from "@flowgram.ai/free-layout-editor";
import { Database, Sparkles, BarChart3, Lightbulb, Shuffle, Download, CheckCircle2, XCircle, Loader2, Clock } from "lucide-react";
import { cn } from "@/lib/utils";
import { NodeExecutionStatus } from "@/types/workflow";

// Icon mapping
const iconMap: Record<string, React.ComponentType<{ className?: string }>> = {
  Database,
  Sparkles,
  BarChart3,
  Lightbulb,
  Shuffle,
  Download,
};

// Status colors
const statusBg: Record<NodeExecutionStatus, string> = {
  idle: "bg-muted",
  pending: "bg-muted",
  running: "bg-amber-500",
  success: "bg-emerald-500",
  error: "bg-destructive",
  skipped: "bg-muted",
};

const statusRing: Record<NodeExecutionStatus, string> = {
  idle: "",
  pending: "",
  running: "ring-2 ring-amber-500 ring-offset-2 ring-offset-background",
  success: "ring-2 ring-emerald-500 ring-offset-2 ring-offset-background",
  error: "ring-2 ring-destructive ring-offset-2 ring-offset-background",
  skipped: "",
};

interface WorkflowNodeData {
  title: string;
  type: string;
  status: NodeExecutionStatus;
  definition?: {
    icon: string;
    category: string;
  };
}

export const WorkflowCustomNode = ({ node }: { node: any }) => {
  const { startDrag } = useNodeRender(node);
  const data = node?.getData?.() as WorkflowNodeData | undefined;
  
  const iconName = data?.definition?.icon || "Database";
  const Icon = iconMap[iconName] || Database;
  const status = data?.status || "idle";
  const category = data?.definition?.category || "source";

  const StatusIcon = status === "running" 
    ? Loader2 
    : status === "success" 
    ? CheckCircle2 
    : status === "error"
    ? XCircle
    : Clock;

  return (
    <WorkflowNodeRenderer node={node}>
      <div 
        className={cn(
          "px-4 py-3 rounded-xl border bg-card shadow-lg min-w-[180px] transition-all duration-200 cursor-grab active:cursor-grabbing",
          statusRing[status]
        )}
        onMouseDown={startDrag}
      >
        <div className="flex items-center gap-3">
          {/* Status/Icon indicator */}
          <div className={cn(
            "p-2 rounded-lg transition-colors",
            statusBg[status],
            status === "running" && "animate-pulse"
          )}>
            {status === "idle" || status === "pending" ? (
              <Icon className="h-5 w-5 text-foreground" />
            ) : (
              <StatusIcon className={cn(
                "h-5 w-5 text-white",
                status === "running" && "animate-spin"
              )} />
            )}
          </div>

          {/* Node info */}
          <div className="flex-1 min-w-0">
            <p className="text-sm font-medium truncate">{data?.title || "Node"}</p>
            <p className="text-[10px] text-muted-foreground capitalize">{category}</p>
          </div>
        </div>
      </div>
    </WorkflowNodeRenderer>
  );
};

export default WorkflowCustomNode;
