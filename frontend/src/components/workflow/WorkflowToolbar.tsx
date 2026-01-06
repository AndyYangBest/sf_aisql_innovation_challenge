/**
 * WorkflowToolbar - 工作流编辑器工具栏
 */

import { Badge } from "@/components/ui/badge";
import { WorkflowNodeType, WorkflowStatus } from "@/types/workflow";

interface WorkflowToolbarProps {
  workflowStatus: WorkflowStatus;
  onAddNode: (type: WorkflowNodeType) => void;
}

const statusColors: Record<WorkflowStatus, string> = {
  draft: "bg-muted text-muted-foreground",
  ready: "bg-primary/10 text-primary",
  running: "bg-amber-500/10 text-amber-500",
  completed: "bg-emerald-500/10 text-emerald-500",
  failed: "bg-destructive/10 text-destructive",
};

const statusLabels: Record<WorkflowStatus, string> = {
  draft: "Draft",
  ready: "Ready",
  running: "Running",
  completed: "Completed",
  failed: "Failed",
};

const WorkflowToolbar = ({
  workflowStatus,
  onAddNode,
}: WorkflowToolbarProps) => {
  return (
    <div className="h-10 border-b border-border bg-card/50 px-4 flex items-center">
      <div className="flex items-center gap-3">
        {/* Status Badge */}
        <Badge variant="secondary" className={statusColors[workflowStatus]}>
          {statusLabels[workflowStatus]}
        </Badge>
        
        <span className="text-xs text-muted-foreground">
          Drag nodes from panel to canvas
        </span>
      </div>
    </div>
  );
};

export default WorkflowToolbar;
