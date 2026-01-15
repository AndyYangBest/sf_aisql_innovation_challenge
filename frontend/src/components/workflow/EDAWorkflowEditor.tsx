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
  SelectionService,
  delay,
  useClientContext,
  usePlaygroundTools,
  useService,
  type WorkflowJSON,
} from '@flowgram.ai/free-layout-editor';
import { MinimapRender, createMinimapPlugin } from '@flowgram.ai/minimap-plugin';
import { Button } from '@/components/ui/button';
import { Popover, PopoverContent, PopoverTrigger } from '@/components/ui/popover';
import {
  LayoutGrid,
  MessageSquarePlus,
  Plus,
  Redo2,
  Scan,
  Undo2,
  ZoomIn,
  ZoomOut,
  Play,
} from 'lucide-react';
import { createEDANodeRegistries } from './EDANodeRegistries';
import { EDANodeRenderer } from './EDANodeRenderer';
import { WorkflowNode, WorkflowEdge } from '@/hooks/useEDAWorkflow';
import { EDA_NODE_DEFINITIONS, EDANodeType, type EDANodeDefinition } from '@/types/eda-workflow';
import { cn } from '@/lib/utils';

interface EDAWorkflowEditorProps {
  nodes: WorkflowNode[];
  edges: WorkflowEdge[];
  isRunning?: boolean;
  selectedNodeIds?: string[];
  onWorkflowDataChange?: (data: WorkflowJSON) => void;
  onSelectionChange?: (nodeIds: string[]) => void;
  onRun?: () => void;
  runLabel?: string;
  runDisabled?: boolean;
  className?: string;
}

const EDAWorkflowToolbar = ({
  isRunning,
  onRun,
  runLabel,
  runDisabled,
  onPlaceComment,
  isPlacingComment,
}: {
  isRunning?: boolean;
  onRun?: () => void;
  runLabel?: string;
  runDisabled?: boolean;
  onPlaceComment?: () => void;
  isPlacingComment?: boolean;
}) => {
  const { history, document, playground } = useClientContext();
  const selectService = useService(SelectionService);
  const tools = usePlaygroundTools();
  const zoomPercent = Math.round(tools.zoom * 100);
  const [canUndo, setCanUndo] = useState(false);
  const [canRedo, setCanRedo] = useState(false);
  const [addNodeOpen, setAddNodeOpen] = useState(false);

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
    (event: ReactMouseEvent<HTMLButtonElement>) => {
      event.preventDefault();
      event.stopPropagation();
      onPlaceComment?.();
    },
    [onPlaceComment]
  );

  const handleAddNode = useCallback(
    (type: EDANodeType) => {
      const definition = EDA_NODE_DEFINITIONS[type];
      const container = playground.node.getBoundingClientRect();
      const position = playground.config.getPosFromMouseEvent({
        clientX: container.left + container.width * 0.5,
        clientY: container.top + container.height * 0.5,
      });
      const data = {
        ...definition.defaultData,
        title: definition.name,
        status: 'idle',
      };
      const node = document.createWorkflowNodeByType(type, position, { data });
      if (selectService) {
        selectService.selection = [node];
      }
      setAddNodeOpen(false);
    },
    [document, playground, selectService]
  );

  const nodeGroups = useMemo(() => {
    const grouped: Record<'source' | 'analysis' | 'feature' | 'output', EDANodeDefinition[]> = {
      source: [],
      analysis: [],
      feature: [],
      output: [],
    };

    Object.values(EDA_NODE_DEFINITIONS).forEach((definition) => {
      grouped[definition.category].push(definition);
    });

    return grouped;
  }, []);

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
        disabled={isRunning}
      >
        <LayoutGrid className={iconClassName} strokeWidth={2.5} />
      </Button>
      {onRun && (
        <Button
          variant="ghost"
          size="icon"
          className={buttonClassName}
          onClick={onRun}
          aria-label={runLabel || 'Run workflow'}
          disabled={runDisabled || isRunning}
        >
          <Play className={iconClassName} strokeWidth={2.5} />
        </Button>
      )}
      <Popover open={addNodeOpen} onOpenChange={setAddNodeOpen}>
        <PopoverTrigger asChild>
          <Button
            variant="ghost"
            size="icon"
            className={buttonClassName}
            aria-label="Add node"
            disabled={isRunning}
          >
            <Plus className={iconClassName} strokeWidth={2.5} />
          </Button>
        </PopoverTrigger>
        <PopoverContent align="start" className="w-72 p-3" onMouseDown={(event) => event.stopPropagation()}>
          <div className="text-xs font-semibold uppercase text-slate-500 tracking-wide mb-2">
            Add Node
          </div>
          <div className="space-y-3">
            {(['source', 'analysis', 'feature', 'output'] as const).map((category) => (
              <div key={category} className="space-y-1">
                <div className="text-[11px] uppercase text-slate-400 tracking-wide">
                  {category}
                </div>
                <div className="space-y-1">
                  {nodeGroups[category].map((definition) => (
                    <button
                      key={definition.type}
                      className="w-full rounded-md border border-slate-200 bg-white px-2 py-2 text-left text-xs text-slate-700 hover:border-slate-300 hover:bg-slate-50"
                      onClick={() => handleAddNode(definition.type)}
                    >
                      <div className="font-medium text-slate-900">{definition.name}</div>
                      <div className="text-[11px] text-slate-500">{definition.description}</div>
                    </button>
                  ))}
                </div>
              </div>
            ))}
          </div>
        </PopoverContent>
      </Popover>
      <Button
        variant="ghost"
        size="icon"
        className={buttonClassName}
        onMouseDown={handleAddComment}
        aria-label="Add comment"
        disabled={isRunning}
      >
        <MessageSquarePlus
          className={cn(iconClassName, isPlacingComment && 'text-blue-600')}
          strokeWidth={2.5}
        />
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
  isRunning,
  selectedNodeIds,
  onWorkflowDataChange,
  onSelectionChange,
  onRun,
  runLabel,
  runDisabled,
  className,
}: EDAWorkflowEditorProps) => {
  const editorRef = useRef<FreeLayoutPluginContext | undefined>();
  const [editorReady, setEditorReady] = useState(false);
  const [placingComment, setPlacingComment] = useState(false);
  const applyingRef = useRef(false);
  const lastEditorJsonRef = useRef<string | null>(null);

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

  const runningNodeIds = useMemo(() => {
    return new Set(nodes.filter((node) => node.data.status === 'running').map((node) => node.id));
  }, [nodes]);

  const handleAllLayersRendered = () => {
    setEditorReady(true);
    if (nodes.length > 0) {
      void editorRef.current?.tools?.fitView(false);
    }
  };

  useEffect(() => {
    if (!editorReady) {
      return;
    }
    const ctx = editorRef.current;
    if (!ctx) {
      return;
    }
    const nextJson = JSON.stringify(workflowData);
    if (lastEditorJsonRef.current === nextJson) {
      return;
    }

    applyingRef.current = true;
    if (typeof ctx.operation?.fromJSON === 'function') {
      ctx.operation.fromJSON(workflowData);
    } else if (typeof ctx.document?.fromJSON === 'function') {
      ctx.document.fromJSON(workflowData);
    }
    setTimeout(() => {
      applyingRef.current = false;
    }, 0);
  }, [editorReady, workflowData]);

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
          ref={editorRef}
          initialData={workflowData}
          readonly={isRunning}
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
          onContentChange={(ctx) => {
            if (isRunning || applyingRef.current) {
              return;
            }
            if (onWorkflowDataChange) {
              const json = ctx.document.toJSON() as WorkflowJSON;
              lastEditorJsonRef.current = JSON.stringify(json);
              onWorkflowDataChange(json);
            }
          }}
          onAllLayersRendered={handleAllLayersRendered}
          // Disable editing during workflow execution
          canAddLine={() => !isRunning}
          canDeleteLine={() => !isRunning}
          canDeleteNode={() => !isRunning}
          // Line colors
          lineColor={{
            default: '#4d53e8',
            drawing: '#5DD6E3',
            hovered: '#37d0ff',
            selected: '#37d0ff',
            flowing: '#f59e0b',
            error: '#ef4444',
          }}
          isFlowingLine={(_, line) => {
            const fromId = line.from?.id;
            const toId = line.to?.id;
            return (
              (fromId ? runningNodeIds.has(fromId) : false) ||
              (toId ? runningNodeIds.has(toId) : false)
            );
          }}
        >
          <EditorRenderer className="w-full h-full" />
          <CommentPlacementLayer
            active={placingComment}
            onPlaced={() => setPlacingComment(false)}
            onCancel={() => setPlacingComment(false)}
          />
          {selectedNodeIds && selectedNodeIds.length > 0 && !placingComment && (
            <SelectionSync selectedNodeIds={selectedNodeIds} />
          )}
          {onSelectionChange && !placingComment && (
            <SelectionWatcher onSelectionChange={onSelectionChange} />
          )}
          <EDAWorkflowToolbar
            isRunning={isRunning}
            onRun={onRun}
            runLabel={runLabel}
            runDisabled={runDisabled}
            onPlaceComment={() => setPlacingComment((prev) => !prev)}
            isPlacingComment={placingComment}
          />
          <EDAMinimap />
        </FreeLayoutEditorProvider>
      </div>
    </div>
  );
};

const CommentPlacementLayer = ({
  active,
  onPlaced,
  onCancel,
}: {
  active: boolean;
  onPlaced: () => void;
  onCancel: () => void;
}) => {
  const { document, playground } = useClientContext();
  const dragService = useService(WorkflowDragService);
  const selectService = useService(SelectionService);
  const [cursor, setCursor] = useState<{ x: number; y: number } | null>(null);
  const rafRef = useRef<number | null>(null);
  const pendingRef = useRef<{ x: number; y: number } | null>(null);

  useEffect(() => {
    if (!active) return;
    const handleKey = (event: KeyboardEvent) => {
      if (event.key === 'Escape') {
        onCancel();
      }
    };
    window.addEventListener('keydown', handleKey);
    return () => {
      window.removeEventListener('keydown', handleKey);
    };
  }, [active, onCancel]);

  useEffect(() => {
    return () => {
      if (rafRef.current) {
        cancelAnimationFrame(rafRef.current);
      }
    };
  }, []);

  const handleMouseMove = useCallback(
    (event: ReactMouseEvent<HTMLDivElement>) => {
      if (!active) return;
      const rect = event.currentTarget.getBoundingClientRect();
      pendingRef.current = {
        x: event.clientX - rect.left,
        y: event.clientY - rect.top,
      };
      if (rafRef.current) {
        return;
      }
      rafRef.current = requestAnimationFrame(() => {
        setCursor(pendingRef.current);
        rafRef.current = null;
      });
    },
    [active]
  );

  const handleMouseDown = useCallback(
    async (event: ReactMouseEvent<HTMLDivElement>) => {
      if (!active) return;
      const nativeEvent = event.nativeEvent as MouseEvent;
      if (nativeEvent.button !== 0) {
        return;
      }
      event.preventDefault();
      event.stopPropagation();
      const position = playground.config.getPosFromMouseEvent(nativeEvent);
      const node = document.createWorkflowNodeByType('comment', position, {
        data: {
          title: 'Comment',
          note: '',
        },
      });
      await delay(16);
      if (selectService) {
        selectService.selection = [node];
      }
      if (dragService && typeof dragService.startDragSelectedNodes === 'function') {
        try {
          dragService.startDragSelectedNodes(nativeEvent);
        } catch {
          // Fallback to static placement if drag is unavailable.
        }
      }
      onPlaced();
    },
    [active, document, dragService, onPlaced, playground, selectService]
  );

  if (!active) return null;

  return (
    <div
      className="absolute inset-0 z-50 cursor-crosshair pointer-events-auto"
      onMouseMove={handleMouseMove}
      onMouseDown={handleMouseDown}
      onMouseLeave={() => setCursor(null)}
    >
      {cursor && (
        <div
          className="pointer-events-none absolute z-50 -translate-y-1/2 translate-x-3 rounded-md border border-amber-200 bg-amber-50 px-2 py-1 text-[11px] text-amber-900 shadow-sm"
          style={{ left: cursor.x, top: cursor.y }}
        >
          Comment
        </div>
      )}
    </div>
  );
};

const SelectionWatcher = ({ onSelectionChange }: { onSelectionChange: (nodeIds: string[]) => void }) => {
  const selectService = useService(SelectionService);

  useEffect(() => {
    if (!selectService) {
      return;
    }
    const emitSelection = () => {
      const selectedNodes = selectService.selection || [];
      onSelectionChange(selectedNodes.map((node) => node.id));
    };

    const disposable = selectService.onSelectionChanged?.(emitSelection);
    return () => {
      disposable?.dispose?.();
    };
  }, [selectService, onSelectionChange]);

  return null;
};

const SelectionSync = ({ selectedNodeIds }: { selectedNodeIds: string[] }) => {
  const selectService = useService(SelectionService);
  const { document } = useClientContext();
  const lastAppliedRef = useRef<string>('');

  useEffect(() => {
    if (!selectService) {
      return;
    }
    const desired = new Set(selectedNodeIds);
    const desiredKey = Array.from(desired).sort().join('|');
    if (lastAppliedRef.current === desiredKey) {
      return;
    }
    const selectedNodes = selectService.selection || [];
    const current = new Set(selectedNodes.map((node) => node.id));
    if (desired.size === current.size && Array.from(desired).every((id) => current.has(id))) {
      lastAppliedRef.current = desiredKey;
      return;
    }

    const docAny = document as any;
    const nodes: any[] =
      docAny?.getAllNodes?.() ??
      docAny?.getNodes?.() ??
      docAny?.nodes ??
      [];
    const nextSelection = nodes.filter((node) => desired.has(node.id));
    selectService.selection = nextSelection;
    lastAppliedRef.current = desiredKey;
  }, [document, selectService, selectedNodeIds]);

  return null;
};
