/**
 * EDA Workflow Page
 * EDA 工作流执行页面 - 显示 Flowgram 编辑器和流式日志
 */

import { useEffect } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
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
  ArrowLeft,
  Loader2,
  CheckCircle2,
  XCircle,
} from 'lucide-react';
import { cn } from '@/lib/utils';

const EDAWorkflowPage = () => {
  const { id } = useParams<{ id: string }>();
  const navigate = useNavigate();
  const { tableAssets } = useTableStore();

  const tableAsset = tableAssets.find((t) => t.id === id);

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
    if (tableAsset) {
      initializeWorkflow('EDA_OVERVIEW');
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [tableAsset?.id]);

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
  };

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

          {/* Center: Status */}
          <div className="flex items-center gap-4">
            {isRunning && (
              <div className="flex items-center gap-2">
                <Loader2 className="h-4 w-4 animate-spin text-amber-500" />
                <span className="text-sm text-amber-500">Running...</span>
                <Badge variant="outline" className="text-xs">
                  {progress}%
                </Badge>
              </div>
            )}

            {!isRunning && result && (
              <div className="flex items-center gap-2">
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
              </div>
            )}

            {nodes.length > 0 && (
              <Badge variant="secondary" className="text-xs">
                {completedNodes}/{totalNodes} nodes
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
              >
                <Play className="h-4 w-4" />
                Run Workflow
              </Button>
            )}

            {isRunning && (
              <Button
                onClick={stopWorkflow}
                variant="destructive"
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
                className="gap-2"
              >
                <RotateCcw className="h-4 w-4" />
                Reset
              </Button>
            )}
          </div>
        </div>

        {/* Progress bar */}
        {isRunning && (
          <div className="h-1 bg-muted">
            <div
              className="h-full bg-amber-500 transition-all duration-300"
              style={{ width: `${progress}%` }}
            />
          </div>
        )}
      </div>

      {/* Main content */}
      <div className="flex-1 min-h-0 overflow-hidden">
        {nodes.length === 0 ? (
          <div className="flex items-center justify-center h-full">
            <div className="text-center">
              <p className="text-muted-foreground mb-4">
                No workflow initialized
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
            className="h-full"
            onWorkflowDataChange={updateWorkflowFromEditor}
          />
        )}
      </div>

      {/* Log panel */}
      {logs.length > 0 && (
        <WorkflowLogPanel
          logs={logs}
          isRunning={isRunning}
        />
      )}
    </div>
  );
};

export default EDAWorkflowPage;
