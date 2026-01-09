/**
 * WorkflowTab - 工作流标签页
 * 集成 EDA Workflow Editor 到 Workspace
 */

import { useEffect, useState } from 'react';
import { useTableStore } from '@/store/tableStore';
import { useEDAWorkflow } from '@/hooks/useEDAWorkflow';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { EDAWorkflowEditor } from '@/components/workflow/EDAWorkflowEditor';
import { WorkflowLogPanel } from '@/components/workflow/WorkflowLogPanel';
import {
  Play,
  Square,
  RotateCcw,
  Loader2,
  CheckCircle2,
  XCircle,
  Sparkles,
} from 'lucide-react';

interface WorkflowTabProps {
  tableId: string;
}

const WorkflowTab = ({ tableId }: WorkflowTabProps) => {
  const { tableAssets } = useTableStore();
  const tableAsset = tableAssets.find((t) => t.id === tableId);
  const [showLogs, setShowLogs] = useState(false);

  const {
    nodes,
    edges,
    logs,
    isRunning,
    result,
    initializeWorkflow,
    runWorkflow,
    stopWorkflow,
    clearWorkflow,
    updateWorkflowFromEditor,
  } = useEDAWorkflow(
    tableAsset ? parseInt(tableAsset.id) : 0,
    tableAsset?.name || ''
  );

  // Initialize workflow on mount
  useEffect(() => {
    if (tableAsset && nodes.length === 0) {
      initializeWorkflow('EDA_OVERVIEW');
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [tableAsset?.id]);

  // Show logs when workflow starts
  useEffect(() => {
    if (isRunning) {
      setShowLogs(true);
    }
  }, [isRunning]);

  if (!tableAsset) {
    return (
      <div className="flex items-center justify-center h-full">
        <p className="text-muted-foreground">Table not found</p>
      </div>
    );
  }

  // Calculate workflow status
  const completedNodes = nodes.filter((n) => n.data.status === 'success').length;
  const errorNodes = nodes.filter((n) => n.data.status === 'error').length;
  const totalNodes = nodes.length;
  const progress = totalNodes > 0 ? Math.round((completedNodes / totalNodes) * 100) : 0;

  const handleRunWorkflow = () => {
    runWorkflow('Comprehensive EDA analysis', 'EDA_OVERVIEW');
  };

  const handleReset = () => {
    clearWorkflow();
    initializeWorkflow('EDA_OVERVIEW');
    setShowLogs(false);
  };

  return (
    <div className="h-full flex flex-col relative bg-white">
      {/* Toolbar */}
      <div className="border-b border-slate-200 bg-white px-4 py-3 shadow-sm">
        <div className="flex items-center justify-between">
          {/* Left: Status */}
          <div className="flex items-center gap-3">
            {isRunning && (
              <>
                <Loader2 className="h-4 w-4 animate-spin text-amber-500" />
                <span className="text-sm text-amber-500">Running workflow...</span>
                <Badge variant="outline" className="text-xs">
                  {progress}%
                </Badge>
              </>
            )}

            {!isRunning && result && (
              <>
                {errorNodes > 0 ? (
                  <>
                    <XCircle className="h-4 w-4 text-destructive" />
                    <span className="text-sm text-destructive">
                      Failed ({errorNodes} errors)
                    </span>
                  </>
                ) : (
                  <>
                    <CheckCircle2 className="h-4 w-4 text-emerald-500" />
                    <span className="text-sm text-emerald-500">Completed</span>
                  </>
                )}
              </>
            )}

            {!isRunning && !result && nodes.length > 0 && (
              <span className="text-sm text-slate-600 font-medium">
                Ready to run
              </span>
            )}

            {nodes.length > 0 && (
              <Badge variant="secondary" className="text-xs">
                {completedNodes}/{totalNodes} tasks
              </Badge>
            )}
          </div>

          {/* Right: Actions */}
          <div className="flex items-center gap-2">
            {!isRunning && !result && (
              <Button
                onClick={handleRunWorkflow}
                disabled={nodes.length === 0}
                className="gap-2"
                size="sm"
              >
                <Sparkles className="h-4 w-4" />
                Run EDA Workflow
              </Button>
            )}

            {isRunning && (
              <Button
                onClick={stopWorkflow}
                variant="destructive"
                size="sm"
                className="gap-2"
              >
                <Square className="h-4 w-4" />
                Stop
              </Button>
            )}

            {!isRunning && result && (
              <Button
                onClick={handleReset}
                variant="outline"
                size="sm"
                className="gap-2"
              >
                <RotateCcw className="h-4 w-4" />
                Reset
              </Button>
            )}

            {logs.length > 0 && (
              <Button
                onClick={() => setShowLogs(!showLogs)}
                variant="ghost"
                size="sm"
              >
                {showLogs ? 'Hide' : 'Show'} Logs ({logs.length})
              </Button>
            )}
          </div>
        </div>

        {/* Progress bar */}
        {isRunning && (
          <div className="mt-2 h-1 bg-muted rounded-full overflow-hidden">
            <div
              className="h-full bg-amber-500 transition-all duration-300"
              style={{ width: `${progress}%` }}
            />
          </div>
        )}
      </div>

      {/* Workflow Editor */}
      <div className="flex-1 overflow-hidden">
        {nodes.length === 0 ? (
          <div className="flex items-center justify-center h-full">
            <div className="text-center">
              <Sparkles className="h-12 w-12 text-muted-foreground mx-auto mb-4" />
              <p className="text-muted-foreground mb-4">
                Initialize EDA workflow to get started
              </p>
              <Button onClick={() => initializeWorkflow('EDA_OVERVIEW')}>
                Initialize Workflow
              </Button>
            </div>
          </div>
        ) : (
          <EDAWorkflowEditor
            nodes={nodes}
            edges={edges}
            isRunning={isRunning}
            onWorkflowDataChange={updateWorkflowFromEditor}
          />
        )}
      </div>

      {/* Log Panel */}
      {showLogs && logs.length > 0 && (
        <WorkflowLogPanel
          logs={logs}
          isRunning={isRunning}
          onClose={() => setShowLogs(false)}
        />
      )}
    </div>
  );
};

export default WorkflowTab;
