/**
 * EDA Node Renderer
 * EDA 节点渲染组件 - 支持状态动画和流式日志
 */

import { WorkflowNodeProps, WorkflowNodeRenderer, useNodeRender } from '@flowgram.ai/free-layout-editor';
import { Database, Sparkles, BarChart3, Lightbulb, FileText, Download, MessageSquare, CheckCircle2, XCircle, Loader2, AlertCircle } from 'lucide-react';
import { cn } from '@/lib/utils';
import { EDANodeType, EDA_NODE_DEFINITIONS } from '@/types/eda-workflow';
import { NodeStatus } from '@/api/eda';

// Icon mapping
const iconMap: Record<string, React.ComponentType<{ className?: string }>> = {
  Database,
  Sparkles,
  BarChart3,
  Lightbulb,
  FileText,
  Download,
  MessageSquare,
};

// Status styles
const statusStyles: Record<NodeStatus, string> = {
  idle: 'border-slate-300 bg-white shadow-sm',
  running: 'border-amber-500 bg-amber-50 shadow-lg shadow-amber-500/20',
  success: 'border-emerald-500 bg-emerald-50 shadow-lg shadow-emerald-500/20',
  error: 'border-red-500 bg-red-50 shadow-lg shadow-red-500/20',
  skipped: 'border-slate-200 bg-slate-50 opacity-60',
};

// Status icon component
const StatusIcon = ({ status }: { status: NodeStatus }) => {
  switch (status) {
    case 'running':
      return <Loader2 className="h-3.5 w-3.5 animate-spin text-amber-500" />;
    case 'success':
      return <CheckCircle2 className="h-3.5 w-3.5 text-emerald-500" />;
    case 'error':
      return <XCircle className="h-3.5 w-3.5 text-destructive" />;
    case 'skipped':
      return <AlertCircle className="h-3.5 w-3.5 text-muted-foreground" />;
    default:
      return null;
  }
};

// Progress bar component
const ProgressBar = ({ progress }: { progress?: number }) => {
  if (progress === undefined || progress === 0) return null;

  return (
    <div className="absolute bottom-0 left-0 right-0 h-1 bg-muted rounded-b-lg overflow-hidden">
      <div
        className="h-full bg-amber-500 transition-all duration-300 ease-out"
        style={{ width: `${progress}%` }}
      />
    </div>
  );
};

// Pulse animation for running state
const PulseRing = ({ show }: { show: boolean }) => {
  if (!show) return null;

  return (
    <div className="absolute inset-0 rounded-lg pointer-events-none">
      <div className="absolute inset-0 rounded-lg border-2 border-amber-500 animate-ping opacity-75" />
      <div className="absolute inset-0 rounded-lg border-2 border-amber-500" />
    </div>
  );
};

/**
 * EDA Node Renderer Component
 */
export const EDANodeRenderer = (props: WorkflowNodeProps) => {
  const {
    node,
    data,
    form,
    type,
    selected,
    activated,
  } = useNodeRender(props.node);

  // Get node data
  const nodeData = data ?? {};
  const nodeType = (type ?? node?.flowNodeType) as EDANodeType;
  const definition = EDA_NODE_DEFINITIONS[nodeType];

  // Get node state
  const title = nodeData?.title ?? definition?.name ?? 'Node';
  const status = (nodeData?.status ?? 'idle') as NodeStatus;
  const progress = nodeData?.progress as number | undefined;
  const iconName = definition?.icon ?? 'Database';
  const Icon = iconMap[iconName] ?? Database;
  const isComment = nodeType === 'comment';

  // Determine if node should be dimmed (not yet executed)
  const isDimmed = status === 'skipped';

  const nodeClassName = cn(
    props.className,
    'relative rounded-lg border-2 shadow-md transition-all duration-300 min-w-[180px] text-slate-900',
    isComment ? 'border-amber-200 bg-amber-50 shadow-sm' : statusStyles[status],
    isDimmed && 'opacity-70',
    status === 'running' && !isComment && 'scale-105',
    selected && 'selected',
    activated && 'activated'
  );
  const portPrimaryColor = props.portPrimaryColor ?? '#00b4d8';
  const portSecondaryColor = props.portSecondaryColor ?? '#48cae4';
  const portErrorColor = props.portErrorColor ?? '#ef4444';
  const portBackgroundColor = props.portBackgroundColor ?? '#0f1419';

  return (
    <WorkflowNodeRenderer
      {...props}
      className={nodeClassName}
      portPrimaryColor={portPrimaryColor}
      portSecondaryColor={portSecondaryColor}
      portErrorColor={portErrorColor}
      portBackgroundColor={portBackgroundColor}
    >
      {/* Pulse animation for running state */}
        <PulseRing show={status === 'running' && !isComment} />

      {/* Node content */}
      <div className="relative px-3 py-2.5">
        <div className="flex items-center gap-2.5">
          {/* Icon */}
          <div
            className={cn(
              'p-1.5 rounded-md transition-colors',
              status === 'running'
                ? 'bg-amber-500/20'
                : status === 'success'
                ? 'bg-emerald-500/20'
                : status === 'error'
                ? 'bg-destructive/20'
                : 'bg-primary/10'
            )}
          >
            <Icon
              className={cn(
                'h-4 w-4 transition-colors',
                status === 'running'
                  ? 'text-amber-500'
                  : status === 'success'
                  ? 'text-emerald-500'
                  : status === 'error'
                  ? 'text-destructive'
                  : 'text-primary'
              )}
            />
          </div>

          {/* Title */}
          <span className="font-medium text-sm text-slate-900 flex-1 truncate">
            {title}
          </span>

          {/* Status icon */}
          {!isComment && <StatusIcon status={status} />}
        </div>

        {/* Form content */}
        {form && (
          <div className={cn('mt-2', isComment ? 'text-sm text-slate-700' : 'text-xs text-slate-600')}>
            {form.render()}
          </div>
        )}

        {/* Progress indicator for running state */}
        {status === 'running' && progress !== undefined && !isComment && (
          <div className="mt-2 text-xs text-amber-600 font-medium">
            {progress}% complete
          </div>
        )}

        {/* Error message */}
        {status === 'error' && nodeData?.error && (
          <div className="mt-2 text-xs text-red-600">
            {nodeData.error}
          </div>
        )}
      </div>

      {/* Progress bar */}
      {!isComment && <ProgressBar progress={progress} />}
    </WorkflowNodeRenderer>
  );
};
