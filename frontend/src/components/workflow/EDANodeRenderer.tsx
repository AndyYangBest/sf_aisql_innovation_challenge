/**
 * EDA Node Renderer
 * EDA 节点渲染组件 - 支持状态动画和流式日志
 */

import type { ComponentType, ReactNode } from 'react';
import { WorkflowNodeProps, WorkflowNodeRenderer, useNodeRender } from '@flowgram.ai/free-layout-editor';
import {
  Database,
  Sparkles,
  BarChart3,
  Lightbulb,
  FileText,
  Download,
  MessageSquare,
  Image,
  Sigma,
  PencilLine,
  CheckCircle2,
  XCircle,
  Loader2,
  AlertCircle,
  ChevronDown,
  ChevronUp,
} from 'lucide-react';
import { cn } from '@/lib/utils';
import { EDANodeType, EDA_NODE_DEFINITIONS } from '@/types/eda-workflow';
import { NodeStatus } from '@/api/eda';

// Icon mapping
const iconMap: Record<string, ComponentType<{ className?: string }>> = {
  Database,
  Sparkles,
  BarChart3,
  Lightbulb,
  FileText,
  Download,
  MessageSquare,
  Image,
  Sigma,
  PencilLine,
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
const ProgressBar = ({ progress, status }: { progress?: number; status: NodeStatus }) => {
  if (progress === undefined || progress === 0) return null;

  const barColor =
    status === 'success'
      ? 'bg-emerald-500'
      : status === 'error'
      ? 'bg-destructive'
      : 'bg-amber-500';

  return (
    <div className="absolute bottom-0 left-0 right-0 h-1 bg-muted rounded-b-lg overflow-hidden">
      <div
        className={cn('h-full transition-all duration-300 ease-out', barColor)}
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
    readonly,
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
  const isExpanded = Boolean(nodeData?.expanded);

  // Determine if node should be dimmed (not yet executed)
  const isDimmed = status === 'skipped';
  const canExpand = !isComment;

  const setNodeValue = (key: string, value: unknown) => {
    if (typeof form?.setValueIn === 'function') {
      form.setValueIn(key, value);
      return;
    }
    if (typeof node?.updateData === 'function') {
      node.updateData({ [key]: value });
    }
  };

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

  const renderDetailField = (label: string, input: ReactNode) => (
    <div className="space-y-1">
      <div className="text-[11px] font-medium text-slate-500 uppercase tracking-wide">
        {label}
      </div>
      {input}
    </div>
  );

  const renderDetails = () => {
    if (isComment) {
      return (
        <div className="mt-2 text-sm text-slate-800">
          {form?.render()}
        </div>
      );
    }

    if (!isExpanded) {
      return (
        <div className="mt-2 text-xs text-slate-700">
          {form?.render()}
        </div>
      );
    }

    switch (nodeType) {
      case 'data_source':
        return (
          <div className="mt-3 space-y-2 text-xs text-slate-700">
            {renderDetailField(
              'Table Asset ID',
              <input
                className="w-full rounded-md border border-slate-200 bg-white px-2 py-1 text-xs text-slate-900 focus:outline-none focus:ring-2 focus:ring-slate-200"
                value={nodeData.table_asset_id ?? ''}
                onChange={(event) => setNodeValue('table_asset_id', Number(event.target.value))}
                disabled={readonly}
              />
            )}
            {renderDetailField(
              'Table Name',
              <input
                className="w-full rounded-md border border-slate-200 bg-white px-2 py-1 text-xs text-slate-900 focus:outline-none focus:ring-2 focus:ring-slate-200"
                value={nodeData.table_name ?? ''}
                onChange={(event) => setNodeValue('table_name', event.target.value)}
                disabled={readonly}
              />
            )}
          </div>
        );
      case 'profile_table':
        return (
          <div className="mt-3 space-y-2 text-xs text-slate-700">
            {renderDetailField(
              'Sample Size',
              <input
                type="number"
                min={10}
                className="w-full rounded-md border border-slate-200 bg-white px-2 py-1 text-xs text-slate-900 focus:outline-none focus:ring-2 focus:ring-slate-200"
                value={nodeData.sample_size ?? 100}
                onChange={(event) => setNodeValue('sample_size', Number(event.target.value))}
                disabled={readonly}
              />
            )}
            <label className="flex items-center gap-2 text-xs text-slate-700">
              <input
                type="checkbox"
                className="h-3 w-3 rounded border-slate-300 text-amber-500 focus:ring-amber-400"
                checked={Boolean(nodeData.include_type_inference ?? true)}
                onChange={(event) => setNodeValue('include_type_inference', event.target.checked)}
                disabled={readonly}
              />
              Include semantic type inference
            </label>
          </div>
        );
      case 'generate_insights':
        return (
          <div className="mt-3 space-y-2 text-xs text-slate-700">
            {renderDetailField(
              'Focus',
              <select
                className="w-full rounded-md border border-slate-200 bg-white px-2 py-1 text-xs text-slate-900 focus:outline-none focus:ring-2 focus:ring-slate-200"
                value={nodeData.focus ?? 'general'}
                onChange={(event) => setNodeValue('focus', event.target.value)}
                disabled={readonly}
              >
                <option value="general">General</option>
                <option value="quality">Data Quality</option>
                <option value="patterns">Patterns</option>
                <option value="temporal">Temporal</option>
              </select>
            )}
            {renderDetailField(
              'User Notes',
              <input
                className="w-full rounded-md border border-slate-200 bg-white px-2 py-1 text-xs text-slate-900 focus:outline-none focus:ring-2 focus:ring-slate-200"
                value={nodeData.user_notes ?? ''}
                onChange={(event) => setNodeValue('user_notes', event.target.value)}
                disabled={readonly}
              />
            )}
          </div>
        );
      case 'generate_charts':
        return (
          <div className="mt-3 space-y-2 text-xs text-slate-700">
            {renderDetailField(
              'Chart Count',
              <input
                type="number"
                min={1}
                className="w-full rounded-md border border-slate-200 bg-white px-2 py-1 text-xs text-slate-900 focus:outline-none focus:ring-2 focus:ring-slate-200"
                value={nodeData.chart_count ?? 3}
                onChange={(event) => setNodeValue('chart_count', Number(event.target.value))}
                disabled={readonly}
              />
            )}
            <label className="flex items-center gap-2 text-xs text-slate-700">
              <input
                type="checkbox"
                className="h-3 w-3 rounded border-slate-300 text-amber-500 focus:ring-amber-400"
                checked={Boolean(nodeData.use_semantic_types ?? true)}
                onChange={(event) => setNodeValue('use_semantic_types', event.target.checked)}
                disabled={readonly}
              />
              Use semantic type hints
            </label>
          </div>
        );
      case 'generate_documentation':
        return (
          <div className="mt-3 space-y-2 text-xs text-slate-700">
            <label className="flex items-center gap-2 text-xs text-slate-700">
              <input
                type="checkbox"
                className="h-3 w-3 rounded border-slate-300 text-amber-500 focus:ring-amber-400"
                checked={Boolean(nodeData.include_summary ?? true)}
                onChange={(event) => setNodeValue('include_summary', event.target.checked)}
                disabled={readonly}
              />
              Include summary
            </label>
            <label className="flex items-center gap-2 text-xs text-slate-700">
              <input
                type="checkbox"
                className="h-3 w-3 rounded border-slate-300 text-amber-500 focus:ring-amber-400"
                checked={Boolean(nodeData.include_use_cases ?? true)}
                onChange={(event) => setNodeValue('include_use_cases', event.target.checked)}
                disabled={readonly}
              />
              Include use cases
            </label>
            <label className="flex items-center gap-2 text-xs text-slate-700">
              <input
                type="checkbox"
                className="h-3 w-3 rounded border-slate-300 text-amber-500 focus:ring-amber-400"
                checked={Boolean(nodeData.include_recommendations ?? true)}
                onChange={(event) => setNodeValue('include_recommendations', event.target.checked)}
                disabled={readonly}
              />
              Include recommendations
            </label>
          </div>
        );
      case 'export':
        return (
          <div className="mt-3 space-y-2 text-xs text-slate-700">
            {renderDetailField(
              'Export Format',
              <select
                className="w-full rounded-md border border-slate-200 bg-white px-2 py-1 text-xs text-slate-900 focus:outline-none focus:ring-2 focus:ring-slate-200"
                value={nodeData.format ?? 'json'}
                onChange={(event) => setNodeValue('format', event.target.value)}
                disabled={readonly}
              >
                <option value="json">JSON</option>
                <option value="markdown">Markdown</option>
                <option value="pdf">PDF</option>
              </select>
            )}
          </div>
        );
      default:
        return (
          <div className="mt-2 text-xs text-slate-700">
            {definition?.description ?? 'Configure this step'}
          </div>
        );
    }
  };

  const showProgressBar =
    !isComment && progress !== undefined && status !== 'idle' && status !== 'skipped';

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
          {canExpand && (
            <button
              className="ml-1 rounded-md p-1 text-slate-500 hover:bg-slate-100 hover:text-slate-700"
              onMouseDown={(event) => event.stopPropagation()}
              onClick={(event) => {
                event.stopPropagation();
                setNodeValue('expanded', !isExpanded);
              }}
              aria-label={isExpanded ? 'Collapse details' : 'Expand details'}
            >
              {isExpanded ? (
                <ChevronUp className="h-3.5 w-3.5" />
              ) : (
                <ChevronDown className="h-3.5 w-3.5" />
              )}
            </button>
          )}
        </div>

        {!isComment && (
          <div className="mt-2 text-[11px] text-slate-500">
            {definition?.description}
          </div>
        )}

        {renderDetails()}

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
      {showProgressBar && <ProgressBar progress={progress} status={status} />}
    </WorkflowNodeRenderer>
  );
};
