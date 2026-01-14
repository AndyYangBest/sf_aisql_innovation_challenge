/**
 * WorkflowTab - 工作流标签页
 * 集成 EDA Workflow Editor 到 Workspace
 */

import { useTableStore } from '@/store/tableStore';
import ColumnWorkflowPanel from '@/components/workflow/ColumnWorkflowPanel';

interface WorkflowTabProps {
  tableId: string;
}

const WorkflowTab = ({ tableId }: WorkflowTabProps) => {
  const { tableAssets } = useTableStore();
  const tableAsset = tableAssets.find((t) => t.id === tableId);
  if (!tableAsset) {
    return (
      <div className="flex items-center justify-center h-full">
        <p className="text-muted-foreground">Table not found</p>
      </div>
    );
  }

  return (
    <div className="h-full overflow-auto bg-background p-4">
      <ColumnWorkflowPanel
        tableAssetId={parseInt(tableAsset.id)}
        tableName={tableAsset.name}
      />
    </div>
  );
};

export default WorkflowTab;
