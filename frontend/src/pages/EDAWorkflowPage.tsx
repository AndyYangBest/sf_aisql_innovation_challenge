/**
 * EDA Workflow Page
 * EDA 工作流执行页面 - 显示 Flowgram 编辑器和流式日志
 */

import { useParams, useNavigate } from 'react-router-dom';
import { useTableStore } from '@/store/tableStore';
import { Button } from '@/components/ui/button';
import ColumnWorkflowPanel from '@/components/workflow/ColumnWorkflowPanel';
import {
  ArrowLeft,
} from 'lucide-react';

const EDAWorkflowPage = () => {
  const { id } = useParams<{ id: string }>();
  const navigate = useNavigate();
  const { tableAssets } = useTableStore();

  const tableAsset = tableAssets.find((t) => t.id === id);

  if (!tableAsset) {
    return (
      <div className="min-h-screen bg-background flex items-center justify-center">
        <div className="text-center">
          <h2 className="text-lg font-medium mb-2">Table not found</h2>
          <Button variant="outline" size="sm" onClick={() => navigate('/tables')}>
            Back to Tables
          </Button>
        </div>
      </div>
    );
  }

  return (
    <div className="h-screen bg-background flex flex-col">
      {/* Header */}
      <div className="border-b bg-card">
        <div className="flex items-center justify-between px-6 py-4">
          {/* Left: Back button and title */}
          <div className="flex items-center gap-4">
            <Button
              variant="ghost"
              size="sm"
              onClick={() => navigate(`/tables/${id}`)}
              className="gap-2"
            >
              <ArrowLeft className="h-4 w-4" />
              Back
            </Button>

            <div>
              <h1 className="text-xl font-semibold">{tableAsset.name}</h1>
              <p className="text-sm text-muted-foreground">
                EDA Workflow Analysis
              </p>
            </div>
          </div>

          <div />
        </div>
      </div>

      {/* Main content */}
      <div className="flex-1 min-h-0 overflow-hidden">
        <div className="h-full overflow-auto bg-white p-4">
          <ColumnWorkflowPanel
            tableAssetId={parseInt(tableAsset.id)}
            tableName={tableAsset.name}
          />
        </div>
      </div>
    </div>
  );
};

export default EDAWorkflowPage;
