/**
 * EDA Node Renderer
 */

import { useState, type ComponentType, type ReactNode } from 'react';
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
  Clock,
  ListTree,
  AlertTriangle,
  GitMerge,
  ClipboardList,
  ShieldCheck,
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
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from '@/components/ui/alert-dialog';

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
  Clock,
  ListTree,
  AlertTriangle,
  GitMerge,
  ClipboardList,
  ShieldCheck,
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
  const columnName = nodeData?.column_name as string | undefined;
  const columnType = nodeData?.column_type as string | undefined;
  const columnNullRate = nodeData?.column_null_rate as number | undefined;

  // Get node state
  const title = nodeData?.title ?? definition?.name ?? 'Node';
  const status = (nodeData?.status ?? 'idle') as NodeStatus;
  const progress = nodeData?.progress as number | undefined;
  const iconName = definition?.icon ?? 'Database';
  const Icon = iconMap[iconName] ?? Database;
  const isComment = nodeType === 'comment';
  const isExpanded = Boolean(nodeData?.expanded);
  const [approvalDialogOpen, setApprovalDialogOpen] = useState(false);

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
    definition?.createsColumn && 'ring-1 ring-emerald-200',
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
      case 'numeric_distribution':
        return (
          <div className="mt-3 space-y-2 text-xs text-slate-700">
            {renderDetailField(
              'Sample Size',
              <input
                type="number"
                min={100}
                className="w-full rounded-md border border-slate-200 bg-white px-2 py-1 text-xs text-slate-900 focus:outline-none focus:ring-2 focus:ring-slate-200"
                value={nodeData.sample_size ?? 10000}
                onChange={(event) => setNodeValue('sample_size', Number(event.target.value))}
                disabled={readonly}
              />
            )}
            {renderDetailField(
              'Window Days',
              <input
                type="number"
                min={1}
                className="w-full rounded-md border border-slate-200 bg-white px-2 py-1 text-xs text-slate-900 focus:outline-none focus:ring-2 focus:ring-slate-200"
                value={nodeData.window_days ?? ''}
                onChange={(event) => setNodeValue('window_days', event.target.value ? Number(event.target.value) : null)}
                disabled={readonly}
              />
            )}
          </div>
        );
      case 'numeric_correlations':
        return (
          <div className="mt-3 space-y-2 text-xs text-slate-700">
            {renderDetailField(
              'Sample Size',
              <input
                type="number"
                min={100}
                className="w-full rounded-md border border-slate-200 bg-white px-2 py-1 text-xs text-slate-900 focus:outline-none focus:ring-2 focus:ring-slate-200"
                value={nodeData.sample_size ?? 5000}
                onChange={(event) => setNodeValue('sample_size', Number(event.target.value))}
                disabled={readonly}
              />
            )}
            {renderDetailField(
              'Max Columns',
              <input
                type="number"
                min={2}
                className="w-full rounded-md border border-slate-200 bg-white px-2 py-1 text-xs text-slate-900 focus:outline-none focus:ring-2 focus:ring-slate-200"
                value={nodeData.max_columns ?? 12}
                onChange={(event) => setNodeValue('max_columns', Number(event.target.value))}
                disabled={readonly}
              />
            )}
          </div>
        );
      case 'numeric_periodicity':
        return (
          <div className="mt-3 space-y-2 text-xs text-slate-700">
            {renderDetailField(
              'Bucket',
              <input
                className="w-full rounded-md border border-slate-200 bg-white px-2 py-1 text-xs text-slate-900 focus:outline-none focus:ring-2 focus:ring-slate-200"
                value={nodeData.bucket ?? 'day'}
                onChange={(event) => setNodeValue('bucket', event.target.value)}
                disabled={readonly}
              />
            )}
            {renderDetailField(
              'Window Days',
              <input
                type="number"
                min={1}
                className="w-full rounded-md border border-slate-200 bg-white px-2 py-1 text-xs text-slate-900 focus:outline-none focus:ring-2 focus:ring-slate-200"
                value={nodeData.window_days ?? 180}
                onChange={(event) => setNodeValue('window_days', Number(event.target.value))}
                disabled={readonly}
              />
            )}
          </div>
        );
      case 'categorical_groups':
        return (
          <div className="mt-3 space-y-2 text-xs text-slate-700">
            {renderDetailField(
              'Top N',
              <input
                type="number"
                min={3}
                className="w-full rounded-md border border-slate-200 bg-white px-2 py-1 text-xs text-slate-900 focus:outline-none focus:ring-2 focus:ring-slate-200"
                value={nodeData.top_n ?? 10}
                onChange={(event) => setNodeValue('top_n', Number(event.target.value))}
                disabled={readonly}
              />
            )}
          </div>
        );
      case 'scan_nulls':
        return (
          <div className="mt-3 space-y-2 text-xs text-slate-700">
            {nodeData.null_rate !== undefined && (
              <div className="rounded-md border border-slate-200 bg-slate-50 px-2 py-1 text-[11px]">
                Null rate: {Math.round((nodeData.null_rate ?? 0) * 100)}%
              </div>
            )}
            {renderDetailField(
              'Sample Size',
              <input
                type="number"
                min={100}
                className="w-full rounded-md border border-slate-200 bg-white px-2 py-1 text-xs text-slate-900 focus:outline-none focus:ring-2 focus:ring-slate-200"
                value={nodeData.sample_size ?? 20000}
                onChange={(event) => setNodeValue('sample_size', Number(event.target.value))}
                disabled={readonly}
              />
            )}
          </div>
        );
      case 'scan_conflicts':
        return (
          <div className="mt-3 space-y-2 text-xs text-slate-700">
            {renderDetailField(
              'Group By Columns',
              <input
                className="w-full rounded-md border border-slate-200 bg-white px-2 py-1 text-xs text-slate-900 focus:outline-none focus:ring-2 focus:ring-slate-200"
                value={nodeData.group_by_columns ?? ''}
                onChange={(event) => setNodeValue('group_by_columns', event.target.value)}
                disabled={readonly}
                placeholder="column_a, column_b"
              />
            )}
            {nodeData.conflict_rate !== undefined && (
              <div className="rounded-md border border-slate-200 bg-slate-50 px-2 py-1 text-[11px]">
                Conflict rate: {Math.round((nodeData.conflict_rate ?? 0) * 100)}%
              </div>
            )}
          </div>
        );
      case 'plan_data_repairs':
        return (
          <div className="mt-3 space-y-2 text-xs text-slate-700">
            {renderDetailField(
              'Null Strategy',
              <input
                className="w-full rounded-md border border-slate-200 bg-white px-2 py-1 text-xs text-slate-900 focus:outline-none focus:ring-2 focus:ring-slate-200"
                value={nodeData.null_strategy ?? ''}
                onChange={(event) => setNodeValue('null_strategy', event.target.value)}
                disabled={readonly}
              />
            )}
            {renderDetailField(
              'Conflict Strategy',
              <input
                className="w-full rounded-md border border-slate-200 bg-white px-2 py-1 text-xs text-slate-900 focus:outline-none focus:ring-2 focus:ring-slate-200"
                value={nodeData.conflict_strategy ?? ''}
                onChange={(event) => setNodeValue('conflict_strategy', event.target.value)}
                disabled={readonly}
              />
            )}
            {renderDetailField(
              'Row ID Column',
              <input
                className="w-full rounded-md border border-slate-200 bg-white px-2 py-1 text-xs text-slate-900 focus:outline-none focus:ring-2 focus:ring-slate-200"
                value={nodeData.row_id_column ?? ''}
                onChange={(event) => setNodeValue('row_id_column', event.target.value)}
                disabled={readonly}
                placeholder="primary_key_column"
              />
            )}
            {renderDetailField(
              'Audit Table',
              <input
                className="w-full rounded-md border border-slate-200 bg-white px-2 py-1 text-xs text-slate-900 focus:outline-none focus:ring-2 focus:ring-slate-200"
                value={nodeData.audit_table ?? ''}
                onChange={(event) => setNodeValue('audit_table', event.target.value)}
                disabled={readonly}
                placeholder="optional_audit_table"
              />
            )}
            {nodeData.apply_ready === false && (
              <div className="rounded-md border border-amber-200 bg-amber-50 px-2 py-1 text-[11px] text-amber-700">
                Missing row ID column or table reference for apply.
              </div>
            )}
            {nodeData.plan_summary && (
              <div className="rounded-md border border-slate-200 bg-slate-50 px-2 py-1 text-[11px]">
                {nodeData.plan_summary}
              </div>
            )}
          </div>
        );
      case 'approval_gate':
        return (
          <div className="mt-3 space-y-2 text-xs text-slate-700">
            {nodeData.plan_summary && (
              <div className="rounded-md border border-emerald-200 bg-emerald-50 px-2 py-1 text-[11px] text-emerald-800">
                {nodeData.plan_summary}
              </div>
            )}
            {nodeData.token_estimate && (
              <div className="rounded-md border border-slate-200 bg-slate-50 px-2 py-1 text-[11px]">
                Token estimate: {nodeData.token_estimate.token_count ?? 0}
              </div>
            )}
            <label className="flex items-center gap-2 text-xs text-slate-700">
              <input
                type="checkbox"
                className="h-3 w-3 rounded border-slate-300 text-emerald-600 focus:ring-emerald-400"
                checked={Boolean(nodeData.approved)}
                onChange={(event) => {
                  const checked = event.target.checked;
                  if (checked && !readonly) {
                    setApprovalDialogOpen(true);
                    return;
                  }
                  setNodeValue('approved', false);
                }}
                disabled={readonly}
              />
              Approved to apply fixes
            </label>
            {renderDetailField(
              'Note',
              <input
                className="w-full rounded-md border border-slate-200 bg-white px-2 py-1 text-xs text-slate-900 focus:outline-none focus:ring-2 focus:ring-slate-200"
                value={nodeData.note ?? ''}
                onChange={(event) => setNodeValue('note', event.target.value)}
                disabled={readonly}
                placeholder="Reason for approval"
              />
            )}
            <AlertDialog open={approvalDialogOpen} onOpenChange={setApprovalDialogOpen}>
              <AlertDialogContent className="bg-slate-950 text-slate-100 border-slate-800">
                <AlertDialogHeader>
                  <AlertDialogTitle>Approve Data Repairs</AlertDialogTitle>
                  <AlertDialogDescription className="text-slate-300">
                    Review the plan and token estimate before applying changes.
                  </AlertDialogDescription>
                </AlertDialogHeader>
                <div className="space-y-3 text-sm text-slate-200">
                  {nodeData.plan_summary && (
                    <div className="rounded-md border border-slate-800 bg-slate-900/60 px-3 py-2">
                      {nodeData.plan_summary}
                    </div>
                  )}
                  {nodeData.apply_ready === false && (
                    <div className="rounded-md border border-amber-500/60 bg-amber-900/30 px-3 py-2 text-amber-200">
                      Repairs require a row identifier and a writable table.
                    </div>
                  )}
                  {nodeData.row_id_column && (
                    <div className="rounded-md border border-slate-800 bg-slate-900/60 px-3 py-2">
                      Row ID column: {nodeData.row_id_column}
                    </div>
                  )}
                  {nodeData.plan_id && (
                    <div className="rounded-md border border-slate-800 bg-slate-900/60 px-3 py-2">
                      Plan ID: {nodeData.plan_id}
                    </div>
                  )}
                  {nodeData.snapshot_signature && (
                    <div className="rounded-md border border-slate-800 bg-slate-900/60 px-3 py-2">
                      Snapshot signature: {nodeData.snapshot_signature}
                    </div>
                  )}
                  {Array.isArray(nodeData.plan_steps) && nodeData.plan_steps.length > 0 && (
                    <div className="rounded-md border border-slate-800 bg-slate-900/60 px-3 py-2">
                      {nodeData.plan_steps.map((step: any, index: number) => (
                        <div key={index} className="text-[12px] text-slate-200">
                          {step.type === "null_repair" && (
                            <span>
                              Null repair ({step.strategy}) - ~{step.estimated_rows ?? 0} rows
                              {nodeData.snapshot?.total_count
                                ? ` (${Math.round((step.estimated_rows || 0) / nodeData.snapshot.total_count * 100)}%)`
                                : ""}
                            </span>
                          )}
                          {step.type === "conflict_repair" && (
                            <span>
                              Conflict repair ({step.strategy}) - {step.estimated_groups ?? 0} groups
                            </span>
                          )}
                        </div>
                      ))}
                    </div>
                  )}
                  {nodeData.sql_previews?.null_repair?.update_sql && (
                    <div className="rounded-md border border-slate-800 bg-slate-900/60 px-3 py-2">
                      <div className="mb-1 text-[11px] uppercase tracking-wide text-slate-400">
                        Null Repair SQL
                      </div>
                      <pre className="max-h-32 overflow-auto text-[11px] text-slate-200">
                        {nodeData.sql_previews.null_repair.update_sql}
                      </pre>
                    </div>
                  )}
                  {nodeData.sql_previews?.conflict_repair?.update_sql && (
                    <div className="rounded-md border border-slate-800 bg-slate-900/60 px-3 py-2">
                      <div className="mb-1 text-[11px] uppercase tracking-wide text-slate-400">
                        Conflict Repair SQL
                      </div>
                      <pre className="max-h-32 overflow-auto text-[11px] text-slate-200">
                        {nodeData.sql_previews.conflict_repair.update_sql}
                      </pre>
                    </div>
                  )}
                  {nodeData.rollback?.strategy && (
                    <div className="rounded-md border border-slate-800 bg-slate-900/60 px-3 py-2">
                      Rollback: {nodeData.rollback.strategy}
                      {nodeData.rollback.audit_table
                        ? ` (${nodeData.rollback.audit_table})`
                        : ""}
                    </div>
                  )}
                  {nodeData.token_estimate && (
                    <div className="rounded-md border border-slate-800 bg-slate-900/60 px-3 py-2">
                      Estimated tokens: {nodeData.token_estimate.token_count ?? 0}
                    </div>
                  )}
                </div>
                <AlertDialogFooter>
                  <AlertDialogCancel
                    onClick={() => {
                      setNodeValue('approved', false);
                      setApprovalDialogOpen(false);
                    }}
                  >
                    Cancel
                  </AlertDialogCancel>
                  <AlertDialogAction
                    onClick={() => {
                      setNodeValue('approved', true);
                      setApprovalDialogOpen(false);
                      if (typeof window !== 'undefined' && columnName && nodeData.table_asset_id) {
                        window.dispatchEvent(
                          new CustomEvent('column-workflow-approval', {
                            detail: {
                              tableAssetId: nodeData.table_asset_id,
                              columnName,
                              note: nodeData.note ?? '',
                              planId: nodeData.plan_id,
                              planHash: nodeData.plan_hash,
                              snapshotSignature: nodeData.snapshot_signature,
                            },
                          })
                        );
                      }
                    }}
                  >
                    Confirm & Run Repairs
                  </AlertDialogAction>
                </AlertDialogFooter>
              </AlertDialogContent>
            </AlertDialog>
          </div>
        );
      case 'apply_data_repairs':
        return (
          <div className="mt-3 space-y-2 text-xs text-slate-700">
            {nodeData.plan_summary && (
              <div className="rounded-md border border-slate-200 bg-slate-50 px-2 py-1 text-[11px]">
                {nodeData.plan_summary}
              </div>
            )}
            {nodeData.apply_skipped_reason && (
              <div className="rounded-md border border-amber-200 bg-amber-50 px-2 py-1 text-[11px] text-amber-700">
                Skipped: {nodeData.apply_skipped_reason}
              </div>
            )}
            {nodeData.token_estimate && (
              <div className="rounded-md border border-slate-200 bg-slate-50 px-2 py-1 text-[11px]">
                Estimated tokens: {nodeData.token_estimate.token_count ?? 0}
              </div>
            )}
            {renderDetailField(
              'Null Strategy',
              <input
                className="w-full rounded-md border border-slate-200 bg-white px-2 py-1 text-xs text-slate-900 focus:outline-none focus:ring-2 focus:ring-slate-200"
                value={nodeData.null_strategy ?? ''}
                onChange={(event) => setNodeValue('null_strategy', event.target.value)}
                disabled={readonly}
              />
            )}
            {renderDetailField(
              'Conflict Strategy',
              <input
                className="w-full rounded-md border border-slate-200 bg-white px-2 py-1 text-xs text-slate-900 focus:outline-none focus:ring-2 focus:ring-slate-200"
                value={nodeData.conflict_strategy ?? ''}
                onChange={(event) => setNodeValue('conflict_strategy', event.target.value)}
                disabled={readonly}
              />
            )}
            {renderDetailField(
              'Approval Key',
              <input
                className="w-full rounded-md border border-slate-200 bg-white px-2 py-1 text-xs text-slate-900 focus:outline-none focus:ring-2 focus:ring-slate-200"
                value={nodeData.approval_key ?? 'data_fix_approved'}
                onChange={(event) => setNodeValue('approval_key', event.target.value)}
                disabled={readonly}
              />
            )}
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
      case 'generate_visuals':
        return (
          <div className="mt-3 space-y-2 text-xs text-slate-700">
            {renderDetailField(
              'Chart Type',
              <select
                className="w-full rounded-md border border-slate-200 bg-white px-2 py-1 text-xs text-slate-900 focus:outline-none focus:ring-2 focus:ring-slate-200"
                value={nodeData.chart_type ?? ''}
                onChange={(event) => setNodeValue('chart_type', event.target.value)}
                disabled={readonly}
              >
                <option value="">Auto (AI will choose)</option>
                <option value="line">Line</option>
                <option value="bar">Bar</option>
                <option value="area">Area</option>
                <option value="pie">Pie</option>
              </select>
            )}
            {renderDetailField(
              'X Column',
              <input
                className="w-full rounded-md border border-slate-200 bg-white px-2 py-1 text-xs text-slate-900 focus:outline-none focus:ring-2 focus:ring-slate-200"
                placeholder="Auto (leave empty for AI selection)"
                value={nodeData.x_column ?? ''}
                onChange={(event) => setNodeValue('x_column', event.target.value)}
                disabled={readonly}
              />
            )}
            {renderDetailField(
              'Y Column',
              <input
                className="w-full rounded-md border border-slate-200 bg-white px-2 py-1 text-xs text-slate-900 focus:outline-none focus:ring-2 focus:ring-slate-200"
                placeholder="Auto (leave empty for AI selection)"
                value={nodeData.y_column ?? ''}
                onChange={(event) => setNodeValue('y_column', event.target.value)}
                disabled={readonly}
              />
            )}
            <div className="flex items-start gap-1.5 rounded-md bg-indigo-50 p-2 text-[10px] text-indigo-700">
              <Sparkles className="h-3 w-3 shrink-0 mt-0.5" />
              <span>
                Leave fields empty for AI auto-selection. Or specify columns for custom charts.
              </span>
            </div>
          </div>
        );
      case 'agent_step':
        return (
          <div className="mt-3 space-y-2 text-xs text-slate-700">
            {renderDetailField(
              'Tool Name',
              <input
                className="w-full rounded-md border border-slate-200 bg-white px-2 py-1 text-xs text-slate-900 focus:outline-none focus:ring-2 focus:ring-slate-200"
                value={nodeData.tool_name ?? ''}
                onChange={(event) => setNodeValue('tool_name', event.target.value)}
                disabled={readonly}
              />
            )}
            {nodeData.tool_input && (
              <div className="rounded-md border border-slate-200 bg-slate-50 px-2 py-1 text-[11px] text-slate-600">
                Input: {JSON.stringify(nodeData.tool_input)}
              </div>
            )}
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

        {!isComment && columnName && (
          <div className="mt-1.5 flex flex-wrap items-center gap-1.5">
            <span className="inline-flex max-w-full truncate rounded-md border-2 border-indigo-300 bg-indigo-100 px-2.5 py-1 text-xs font-bold uppercase tracking-wide text-indigo-900 shadow-sm">
              {columnName}
            </span>
            {columnType && (
              <span className="rounded-md border border-indigo-200 bg-indigo-50 px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wide text-indigo-700">
                {columnType}
              </span>
            )}
            {typeof columnNullRate === 'number' && (
              <span className="rounded-md border border-slate-200 bg-white px-2 py-0.5 text-[10px] text-slate-500">
                Nulls {(columnNullRate * 100).toFixed(0)}%
              </span>
            )}
          </div>
        )}

        {!isComment && definition?.createsColumn && (
          <div className="mt-1 flex flex-wrap items-center gap-1.5 text-[10px] text-slate-600">
            <span className="rounded-full border border-emerald-200 bg-emerald-50 px-2 py-0.5 font-semibold uppercase tracking-wide text-emerald-700">
              Feature
            </span>
            {nodeData?.output_column && (
              <span className="truncate rounded-full border border-slate-200 bg-white px-2 py-0.5">
                Output: {nodeData.output_column}
              </span>
            )}
          </div>
        )}

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
