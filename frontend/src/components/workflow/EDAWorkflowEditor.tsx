/**
 * EDA Workflow Editor
 * 专门用于 EDA workflow 的 Flowgram 编辑器
 */

import { useCallback, useEffect, useMemo, useRef, useState, type MouseEvent as ReactMouseEvent } from 'react';
import {
  FreeLayoutEditorProvider,
  EditorRenderer,
  FreeLayoutPluginContext,
  WorkflowDragService,
  WorkflowSelectService,
  delay,
  useClientContext,
  usePlaygroundTools,
  useService,
  WorkflowJSON,
} from '@flowgram.ai/free-layout-editor';
import { MinimapRender, createMinimapPlugin } from '@flowgram.ai/minimap-plugin';
import { Button } from '@/components/ui/button';
import {
  LayoutGrid,
  MessageSquarePlus,
  Redo2,
  Scan,
  Undo2,
  ZoomIn,
  ZoomOut,
} from 'lucide-react';
import { createEDANodeRegistries } from './EDANodeRegistries';
import { EDANodeRenderer } from './EDANodeRenderer';
import { WorkflowNode, WorkflowEdge } from '@/hooks/useEDAWorkflow';
import { cn } from '@/lib/utils';

interface EDAWorkflowEditorProps {
  nodes: WorkflowNode[];
  edges: WorkflowEdge[];
  className?: string;
}

const EDAWorkflowToolbar = () => {
  const { history, document, playground } = useClientContext();
  const dragService = useService(WorkflowDragService);
  const selectService = useService(WorkflowSelectService);
  const tools = usePlaygroundTools();
  const zoomPercent = Math.round(tools.zoom * 100);
  const [canUndo, setCanUndo] = useState(false);
  const [canRedo, setCanRedo] = useState(false);

  useEffect(() => {
    const disposable = history.undoRedoService.onChange(() => {
      setCanUndo(history.canUndo());
      setCanRedo(history.canRedo());
    });
    return () => disposable.dispose();
  }, [history]);

  const handleAutoLayout = useCallback(async () => {
    await tools.autoLayout({
      enableAnimation: true,
      animationDuration: 600,
      layoutConfig: {
        rankdir: 'LR',
        nodesep: 80,
        ranksep: 120,
      },
    });
  }, [tools]);

  const handleAddComment = useCallback(
    async (event: ReactMouseEvent<HTMLButtonElement>) => {
      const nativeEvent = event.nativeEvent as MouseEvent;
      const position = playground.config.getPosFromMouseEvent(nativeEvent);
      const node = document.createWorkflowNodeByType('comment', position, {
        data: {
          title: 'Comment',
          note: '',
        },
      });
      await delay(16);
      selectService.selectNode(node);
      if (nativeEvent.detail !== 0) {
        dragService.startDragSelectedNodes(nativeEvent);
      }
    },
    [document, playground, dragService, selectService]
  );

  const iconClassName = 'h-4 w-4 text-slate-900';
  const buttonClassName = 'h-8 w-8 text-slate-900 hover:text-slate-900';

  return (
    <div
      className="absolute bottom-4 left-4 z-30 flex items-center gap-1 rounded-lg border border-slate-200 bg-white/95 px-2 py-1 shadow-md backdrop-blur pointer-events-auto"
      onMouseDown={(event) => event.stopPropagation()}
    >
      <Button
        variant="ghost"
        size="icon"
        className={buttonClassName}
        onClick={handleAutoLayout}
        aria-label="Auto layout"
      >
        <LayoutGrid className={iconClassName} strokeWidth={2.5} />
      </Button>
      <Button
        variant="ghost"
        size="icon"
        className={buttonClassName}
        onClick={handleAddComment}
        aria-label="Add comment"
      >
        <MessageSquarePlus className={iconClassName} strokeWidth={2.5} />
      </Button>
      <div className="h-5 w-px bg-slate-200 mx-1" aria-hidden="true" />
      <Button
        variant="ghost"
        size="icon"
        className={buttonClassName}
        onClick={() => history.undo()}
        aria-label="Undo"
        disabled={!canUndo}
      >
        <Undo2 className={iconClassName} strokeWidth={2.5} />
      </Button>
      <Button
        variant="ghost"
        size="icon"
        className={buttonClassName}
        onClick={() => history.redo()}
        aria-label="Redo"
        disabled={!canRedo}
      >
        <Redo2 className={iconClassName} strokeWidth={2.5} />
      </Button>
      <div className="h-5 w-px bg-slate-200 mx-1" aria-hidden="true" />
      <Button
        variant="ghost"
        size="icon"
        className={buttonClassName}
        onClick={() => tools.zoomout()}
        aria-label="Zoom out"
      >
        <ZoomOut className={iconClassName} strokeWidth={2.5} />
      </Button>
      <div className="w-12 text-center text-xs font-medium text-slate-700">
        {zoomPercent}%
      </div>
      <Button
        variant="ghost"
        size="icon"
        className={buttonClassName}
        onClick={() => tools.zoomin()}
        aria-label="Zoom in"
      >
        <ZoomIn className={iconClassName} strokeWidth={2.5} />
      </Button>
      <Button
        variant="ghost"
        size="icon"
        className={buttonClassName}
        onClick={() => tools.fitView()}
        aria-label="Fit view"
      >
        <Scan className={iconClassName} strokeWidth={2.5} />
      </Button>
    </div>
  );
};

const EDAMinimap = () => (
  <div className="absolute bottom-4 right-4 z-20 w-[200px] rounded-lg border border-slate-200 bg-white/95 shadow-md backdrop-blur pointer-events-auto">
    <MinimapRender
      panelStyles={{}}
      containerStyles={{
        pointerEvents: 'auto',
        position: 'relative',
        top: 'unset',
        right: 'unset',
        bottom: 'unset',
        left: 'unset',
      }}
      inactiveStyle={{
        opacity: 1,
        scale: 1,
        translateX: 0,
        translateY: 0,
      }}
    />
  </div>
);

export const EDAWorkflowEditor = ({
  nodes,
  edges,
  className,
}: EDAWorkflowEditorProps) => {
  const editorRef = useRef<FreeLayoutPluginContext | undefined>();

  // Node registries
  const nodeRegistries = useMemo(() => createEDANodeRegistries(), []);

  // Convert nodes and edges to Flowgram format
  const workflowData: WorkflowJSON = useMemo(
    () => ({
      nodes: nodes.map((node) => ({
        id: node.id,
        type: node.type,
        meta: {
          position: node.position,
        },
        data: node.data,
      })),
      edges: edges.map((edge) => ({
        sourceNodeID: edge.sourceNodeID,
        targetNodeID: edge.targetNodeID,
      })),
    }),
    [nodes, edges]
  );

  const handleAllLayersRendered = () => {
    if (nodes.length > 0) {
      void editorRef.current?.tools?.fitView(false);
    }
  };

  if (nodes.length === 0) {
    return (
      <div className={cn('flex items-center justify-center h-full', className)}>
        <p className="text-muted-foreground">No workflow data</p>
      </div>
    );
  }

  return (
    <div className={cn('flex flex-col h-full bg-slate-50', className)}>
      <div className="relative flex-1 min-h-0">
        <FreeLayoutEditorProvider
          key={`workflow-${nodes.length}`}
          ref={editorRef}
          initialData={workflowData}
          nodeRegistries={nodeRegistries}
          materials={{
            renderDefaultNode: EDANodeRenderer,
          }}
          nodeEngine={{
            enable: true,
          }}
          history={{
            enable: true,
            enableChangeNode: true,
          }}
          plugins={() => [
            createMinimapPlugin({
              disableLayer: true,
              canvasStyle: {
                canvasWidth: 182,
                canvasHeight: 102,
                canvasPadding: 50,
                canvasBackground: 'rgba(248, 250, 252, 1)',
                canvasBorderRadius: 10,
                viewportBackground: 'rgba(255, 255, 255, 1)',
                viewportBorderRadius: 4,
                viewportBorderColor: 'rgba(15, 23, 42, 0.18)',
                viewportBorderWidth: 1,
                nodeColor: 'rgba(15, 23, 42, 0.12)',
                nodeBorderRadius: 2,
                nodeBorderWidth: 0.15,
                nodeBorderColor: 'rgba(15, 23, 42, 0.2)',
                overlayColor: 'rgba(255, 255, 255, 0.6)',
              },
            }),
          ]}
          playground={{ autoResize: true }}
          onAllLayersRendered={handleAllLayersRendered}
          // Disable editing during workflow execution
          canAddLine={() => false}
          canDeleteLine={() => true}
          canDeleteNode={() => true}
          // Line colors
          lineColor={{
            default: '#4d53e8',
            drawing: '#5DD6E3',
            hovered: '#37d0ff',
            selected: '#37d0ff',
            error: '#ef4444',
          }}
        >
          <EditorRenderer className="w-full h-full" />
          <EDAWorkflowToolbar />
          <EDAMinimap />
        </FreeLayoutEditorProvider>
      </div>
    </div>
  );
};
