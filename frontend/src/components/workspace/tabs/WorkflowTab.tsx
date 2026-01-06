/**
 * WorkflowTab - 工作流标签页
 * 集成 WorkflowEditor 到 Workspace
 */

import { WorkflowEditor } from "@/components/workflow";

interface WorkflowTabProps {
  tableId: string;
  onRunComplete?: () => void;
}

const WorkflowTab = ({ tableId, onRunComplete }: WorkflowTabProps) => {
  return (
    <div className="h-full">
      <WorkflowEditor tableId={tableId} onRunComplete={onRunComplete} />
    </div>
  );
};

export default WorkflowTab;
