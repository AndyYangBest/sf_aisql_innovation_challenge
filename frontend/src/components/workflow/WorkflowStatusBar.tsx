/**
 * WorkflowStatusBar - 工作流状态栏
 */

import { CheckCircle2, XCircle, Clock, Loader2, AlertCircle } from "lucide-react";
import { Workflow, WorkflowExecutionContext, NodeExecutionStatus } from "@/types/workflow";
import { cn } from "@/lib/utils";

interface WorkflowStatusBarProps {
  workflow: Workflow;
  executionContext?: WorkflowExecutionContext;
  isRunning: boolean;
}

const statusIcons: Record<NodeExecutionStatus, React.ComponentType<{ className?: string }>> = {
  idle: Clock,
  pending: Clock,
  running: Loader2,
  success: CheckCircle2,
  error: XCircle,
  skipped: AlertCircle,
};

const statusColors: Record<NodeExecutionStatus, string> = {
  idle: "text-muted-foreground",
  pending: "text-muted-foreground",
  running: "text-amber-500 animate-spin",
  success: "text-emerald-500",
  error: "text-destructive",
  skipped: "text-muted-foreground",
};

const WorkflowStatusBar = ({
  workflow,
  executionContext,
  isRunning,
}: WorkflowStatusBarProps) => {
  const completedCount = workflow.nodes.filter((n) => n.status === "success").length;
  const errorCount = workflow.nodes.filter((n) => n.status === "error").length;
  const totalCount = workflow.nodes.length;

  return (
    <div className="h-8 border-t border-border bg-card px-4 flex items-center justify-between text-xs">
      <div className="flex items-center gap-4">
        {/* Node status summary */}
        <div className="flex items-center gap-3">
          {workflow.nodes.slice(0, 6).map((node) => {
            const Icon = statusIcons[node.status];
            return (
              <div
                key={node.id}
                className="flex items-center gap-1"
                title={`${node.name}: ${node.status}`}
              >
                <Icon className={cn("h-3 w-3", statusColors[node.status])} />
                <span className="text-muted-foreground truncate max-w-20">
                  {node.name}
                </span>
              </div>
            );
          })}
          {workflow.nodes.length > 6 && (
            <span className="text-muted-foreground">
              +{workflow.nodes.length - 6} more
            </span>
          )}
        </div>
      </div>

      <div className="flex items-center gap-4">
        {/* Progress */}
        {isRunning && (
          <span className="text-muted-foreground">
            Running: {completedCount}/{totalCount}
          </span>
        )}

        {/* Completed stats */}
        {!isRunning && workflow.status === "completed" && (
          <span className="text-emerald-500">
            Completed: {completedCount} nodes
          </span>
        )}

        {/* Error count */}
        {errorCount > 0 && (
          <span className="text-destructive">
            {errorCount} error{errorCount > 1 ? "s" : ""}
          </span>
        )}

        {/* Last run time */}
        {workflow.lastRunAt && !isRunning && (
          <span className="text-muted-foreground">
            Last run: {new Date(workflow.lastRunAt).toLocaleTimeString()}
          </span>
        )}
      </div>
    </div>
  );
};

export default WorkflowStatusBar;
