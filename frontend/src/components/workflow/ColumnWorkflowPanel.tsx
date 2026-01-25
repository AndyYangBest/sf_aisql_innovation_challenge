/**
 * ColumnWorkflowPanel - renders multi-column Flowgram workflows with selection.
 */

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { columnMetadataApi, ColumnMetadataRecord } from "@/api/columnMetadata";
import {
  columnWorkflowsApi,
  ColumnWorkflowEstimate,
} from "@/api/columnWorkflows";
import { WorkflowLogPanel } from "@/components/workflow/WorkflowLogPanel";
import { EDAWorkflowEditor } from "@/components/workflow/EDAWorkflowEditor";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogFooter,
} from "@/components/ui/dialog";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import { Checkbox } from "@/components/ui/checkbox";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Tabs, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { useToast } from "@/hooks/use-toast";
import { useTableStore } from "@/store/tableStore";
import type { WorkflowNode, WorkflowEdge } from "@/hooks/useEDAWorkflow";
import { EDA_NODE_DEFINITIONS, EDANodeType } from "@/types/eda-workflow";
import type { WorkflowLogEvent } from "@/api/eda";
import { cn } from "@/lib/utils";

interface ColumnWorkflowState {
  column: ColumnMetadataRecord;
  nodes: WorkflowNode[];
  edges: WorkflowEdge[];
  isRunning: boolean;
}

interface BoardState {
  id: string;
  name: string;
  columnNames: string[];
  selectedColumns: string[];
}

interface ColumnWorkflowPanelProps {
  tableAssetId: number;
  tableName: string;
}

type WorkflowGraphPayload = {
  nodes: WorkflowNode[];
  edges: WorkflowEdge[];
};

const COLUMN_NODE_WIDTH = 240;
const WORKFLOW_COLUMNS = 2;
const WORKFLOW_SPAN_X = COLUMN_NODE_WIDTH * 4 + 240;
const WORKFLOW_SPAN_Y = 360;
const PARALLEL_STACK_GAP = 120;

function normalizeId(value: string) {
  return value.replace(/[^a-z0-9]/gi, "_").toLowerCase();
}

function getNullRate(column: ColumnMetadataRecord): number | null {
  const rate = column.metadata?.null_rate;
  if (typeof rate === "number" && Number.isFinite(rate)) {
    return rate;
  }
  return null;
}

function getNullRateSortValue(column: ColumnMetadataRecord): number {
  const rate = getNullRate(column);
  return typeof rate === "number" ? rate : 1;
}

function collectOverrides(nodes: WorkflowNode[]) {
  const overrides: Record<string, any> = {};
  const hintNode = nodes.find((node) => node.type === "column_hint");
  if (hintNode && hintNode.data?.hint) {
    overrides.hint = hintNode.data.hint;
  }
  const rowLevelNode = nodes.find((node) => node.type === "row_level_extract");
  if (rowLevelNode && rowLevelNode.data?.instruction) {
    overrides.row_level_instruction = rowLevelNode.data.instruction;
    if (rowLevelNode.data?.output_column) {
      overrides.row_level_output_column = rowLevelNode.data.output_column;
    }
    if (rowLevelNode.data?.response_schema) {
      overrides.row_level_schema = rowLevelNode.data.response_schema;
    }
  }
  const imageNode = nodes.find((node) => node.type === "describe_images");
  if (imageNode) {
    if (imageNode.data?.output_column) {
      overrides.image_output_column = imageNode.data.output_column;
    }
    if (imageNode.data?.image_stage) {
      overrides.image_stage = imageNode.data.image_stage;
    }
    if (imageNode.data?.image_path_prefix) {
      overrides.image_path_prefix = imageNode.data.image_path_prefix;
    }
    if (imageNode.data?.image_path_suffix) {
      overrides.image_path_suffix = imageNode.data.image_path_suffix;
    }
    if (imageNode.data?.image_model) {
      overrides.image_model = imageNode.data.image_model;
    }
  }
  const conflictNode = nodes.find((node) => node.type === "scan_conflicts");
  if (conflictNode && conflictNode.data?.group_by_columns) {
    overrides.conflict_group_columns = conflictNode.data.group_by_columns;
  }
  const nullsNode = nodes.find((node) => node.type === "scan_nulls");
  if (nullsNode) {
    if (nullsNode.data?.sample_size !== undefined && nullsNode.data?.sample_size !== null) {
      overrides.scan_nulls_sample_size = nullsNode.data.sample_size;
    }
  }
  const distributionNode = nodes.find((node) => node.type === "numeric_distribution");
  if (distributionNode) {
    if (
      distributionNode.data?.sample_size !== undefined &&
      distributionNode.data?.sample_size !== null
    ) {
      overrides.numeric_distribution_sample_size = distributionNode.data.sample_size;
    }
    if (
      distributionNode.data?.window_days !== undefined &&
      distributionNode.data?.window_days !== null
    ) {
      overrides.numeric_distribution_window_days = distributionNode.data.window_days;
    }
  }
  const correlationsNode = nodes.find((node) => node.type === "numeric_correlations");
  if (correlationsNode) {
    if (
      correlationsNode.data?.sample_size !== undefined &&
      correlationsNode.data?.sample_size !== null
    ) {
      overrides.numeric_correlations_sample_size = correlationsNode.data.sample_size;
    }
    if (
      correlationsNode.data?.max_columns !== undefined &&
      correlationsNode.data?.max_columns !== null
    ) {
      overrides.numeric_correlations_max_columns = correlationsNode.data.max_columns;
    }
  }
  const periodicityNode = nodes.find((node) => node.type === "numeric_periodicity");
  if (periodicityNode) {
    if (periodicityNode.data?.bucket) {
      overrides.numeric_periodicity_bucket = periodicityNode.data.bucket;
    }
    if (
      periodicityNode.data?.window_days !== undefined &&
      periodicityNode.data?.window_days !== null
    ) {
      overrides.numeric_periodicity_window_days = periodicityNode.data.window_days;
    }
  }
  const categoricalNode = nodes.find((node) => node.type === "categorical_groups");
  if (categoricalNode) {
    if (categoricalNode.data?.top_n !== undefined && categoricalNode.data?.top_n !== null) {
      overrides.categorical_groups_top_n = categoricalNode.data.top_n;
    }
  }
  const visualsNode = nodes.find((node) => node.type === "generate_visuals");
  if (visualsNode) {
    if (visualsNode.data?.chart_type) {
      overrides.visual_chart_type = visualsNode.data.chart_type;
    }
    if (visualsNode.data?.x_column) {
      overrides.visual_x_column = visualsNode.data.x_column;
    }
    if (visualsNode.data?.y_column) {
      overrides.visual_y_column = visualsNode.data.y_column;
    }
  }
  const insightsNode = nodes.find((node) => node.type === "generate_insights");
  if (insightsNode) {
    if (insightsNode.data?.focus) {
      overrides.insights_focus = insightsNode.data.focus;
    }
    if (insightsNode.data?.user_notes) {
      overrides.insights_user_notes = insightsNode.data.user_notes;
    }
  }
  const planNode = nodes.find((node) => node.type === "plan_data_repairs");
  if (planNode) {
    if (planNode.data?.null_strategy) {
      overrides.null_strategy = planNode.data.null_strategy;
    }
    if (planNode.data?.conflict_strategy) {
      overrides.conflict_strategy = planNode.data.conflict_strategy;
    }
    if (planNode.data?.row_id_column) {
      overrides.row_id_column = planNode.data.row_id_column;
    }
    if (planNode.data?.audit_table) {
      overrides.repair_audit_table = planNode.data.audit_table;
    }
  }
  const approvalNode = nodes.find((node) => node.type === "approval_gate");
  if (approvalNode) {
    overrides.data_fix_approved = Boolean(approvalNode.data?.approved);
    if (approvalNode.data?.note) {
      overrides.data_fix_note = approvalNode.data.note;
    }
    if (approvalNode.data?.plan_id) {
      overrides.data_fix_plan_id = approvalNode.data.plan_id;
    }
    if (approvalNode.data?.plan_hash) {
      overrides.data_fix_plan_hash = approvalNode.data.plan_hash;
    }
    if (approvalNode.data?.snapshot_signature) {
      overrides.data_fix_snapshot_signature = approvalNode.data.snapshot_signature;
    }
  }
  const applyNode = nodes.find((node) => node.type === "apply_data_repairs");
  if (applyNode) {
    if (!overrides.null_strategy && applyNode.data?.null_strategy) {
      overrides.null_strategy = applyNode.data.null_strategy;
    }
    if (!overrides.conflict_strategy && applyNode.data?.conflict_strategy) {
      overrides.conflict_strategy = applyNode.data.conflict_strategy;
    }
  }
  return overrides;
}

function getWorkflowBasePosition(index: number) {
  const colIndex = index % WORKFLOW_COLUMNS;
  const rowIndex = Math.floor(index / WORKFLOW_COLUMNS);
  return {
    x: 120 + colIndex * WORKFLOW_SPAN_X,
    y: 120 + rowIndex * WORKFLOW_SPAN_Y,
  };
}

const TOOL_CALL_NODE_MAP: Record<string, EDANodeType> = {
  analyze_numeric_distribution: "numeric_distribution",
  analyze_numeric_correlations: "numeric_correlations",
  analyze_numeric_periodicity: "numeric_periodicity",
  analyze_categorical_groups: "categorical_groups",
  scan_nulls: "scan_nulls",
  scan_conflicts: "scan_conflicts",
  plan_data_repairs: "plan_data_repairs",
  require_user_approval: "approval_gate",
  apply_data_repairs: "apply_data_repairs",
  generate_numeric_visuals: "generate_visuals",
  generate_categorical_visuals: "generate_visuals",
  generate_numeric_insights: "generate_insights",
  generate_categorical_insights: "generate_insights",
  summarize_text_column: "summarize_text",
  row_level_extract_text: "row_level_extract",
  describe_image_column: "describe_images",
  basic_column_stats: "basic_stats",
};

const NODE_TYPE_TOOL_MAP: Record<string, string> = {
  numeric_distribution: "analyze_numeric_distribution",
  numeric_correlations: "analyze_numeric_correlations",
  numeric_periodicity: "analyze_numeric_periodicity",
  categorical_groups: "analyze_categorical_groups",
  scan_nulls: "scan_nulls",
  scan_conflicts: "scan_conflicts",
  plan_data_repairs: "plan_data_repairs",
  approval_gate: "require_user_approval",
  apply_data_repairs: "apply_data_repairs",
  generate_visuals: "generate_numeric_visuals",
  generate_insights: "generate_numeric_insights",
  summarize_text: "summarize_text_column",
  row_level_extract: "row_level_extract_text",
  describe_images: "describe_image_column",
  basic_stats: "basic_column_stats",
};

function mapToolStatus(status?: string | null): WorkflowNode["data"]["status"] {
  if (status === "success") return "success";
  if (status === "error") return "error";
  if (status === "running") return "running";
  return "idle";
}

function buildGraphFromToolCalls(
  toolCalls: Array<Record<string, any>>,
  column: ColumnMetadataRecord,
  tableAssetId: number,
  tableName: string,
  layoutIndex: number
): WorkflowGraphPayload {
  const nodes: WorkflowNode[] = [];
  const edges: WorkflowEdge[] = [];
  const colId = normalizeId(column.column_name);
  const basePosition = getWorkflowBasePosition(layoutIndex);
  const sortedCalls = [...toolCalls].sort((a, b) => {
    const aSeq = typeof a.sequence === "number" ? a.sequence : 0;
    const bSeq = typeof b.sequence === "number" ? b.sequence : 0;
    if (aSeq !== bSeq) return aSeq - bSeq;
    return String(a.timestamp || "").localeCompare(String(b.timestamp || ""));
  });

  const batches: Array<{ id: string; calls: Array<Record<string, any>> }> = [];
  const batchMap = new Map<string, Array<Record<string, any>>>();
  sortedCalls.forEach((call, index) => {
    const batchId = String(call.batch_id || call.tool_use_id || call.sequence || index);
    if (!batchMap.has(batchId)) {
      batchMap.set(batchId, []);
      batches.push({ id: batchId, calls: batchMap.get(batchId)! });
    }
    batchMap.get(batchId)!.push(call);
  });

  let x = basePosition.x;
  let prevGroupIds: string[] = [];
  batches.forEach((batch, batchIndex) => {
    const groupIds: string[] = [];
    batch.calls.forEach((call, index) => {
      const toolName = String(call.tool_name || "");
      if (!toolName) return;
      let nodeType = TOOL_CALL_NODE_MAP[toolName];
      if (!nodeType && toolName.endsWith("_agent")) {
        nodeType = "agent_step";
      }
      if (!nodeType) return;
      const definition = EDA_NODE_DEFINITIONS[nodeType];
      const nodeId = `${nodeType}_${colId}_${call.sequence ?? batchIndex}_${index}`;
      const title = nodeType === "agent_step" ? toolName : definition?.name || nodeType;
      const data = {
        ...definition?.defaultData,
        ...(call.input ?? {}),
        title,
        status: mapToolStatus(call.status),
        column_name: column.column_name,
        column_type: column.semantic_type,
        column_null_rate: getNullRate(column),
        table_asset_id: tableAssetId,
        tool_name: toolName,
        tool_input: call.input ?? {},
      };
      const y = basePosition.y + index * PARALLEL_STACK_GAP;
      nodes.push({
        id: nodeId,
        type: nodeType,
        position: { x, y },
        data,
      });
      groupIds.push(nodeId);
    });

    if (prevGroupIds.length > 0 && groupIds.length > 0) {
      if (prevGroupIds.length * groupIds.length <= 9) {
        prevGroupIds.forEach((sourceId) => {
          groupIds.forEach((targetId) => {
            edges.push({ sourceNodeID: sourceId, targetNodeID: targetId });
          });
        });
      } else {
        const sourceId = prevGroupIds[prevGroupIds.length - 1];
        groupIds.forEach((targetId) => {
          edges.push({ sourceNodeID: sourceId, targetNodeID: targetId });
        });
      }
    }

    if (groupIds.length > 0) {
      prevGroupIds = groupIds;
    }
    x += COLUMN_NODE_WIDTH + 80;
  });

  if (nodes.length === 0) {
    nodes.push({
      id: `data_source_${colId}`,
      type: "data_source",
      position: { x: basePosition.x, y: basePosition.y },
      data: {
        ...EDA_NODE_DEFINITIONS.data_source.defaultData,
        title: "Data Source",
        status: "idle",
        table_name: tableName,
        column_name: column.column_name,
        column_type: column.semantic_type,
        column_null_rate: getNullRate(column),
        table_asset_id: tableAssetId,
      },
    });
  }

  return { nodes, edges };
}

function hydrateColumnWorkflowGraph(
  column: ColumnMetadataRecord,
  tableAssetId: number,
  tableName: string,
  layoutIndex: number
): WorkflowGraphPayload {
  const toolCalls =
    ((column.metadata as any)?.workflow?.tool_calls as Array<Record<string, any>>) ??
    [];
  const graph = buildGraphFromToolCalls(
    toolCalls,
    column,
    tableAssetId,
    tableName,
    layoutIndex
  );
  const basePosition = getWorkflowBasePosition(layoutIndex);
  return ensureApprovalGate(graph, column, basePosition);
}

function ensureApprovalGate(
  graph: WorkflowGraphPayload,
  column: ColumnMetadataRecord,
  basePosition?: { x: number; y: number }
): WorkflowGraphPayload {
  const analysis = column.metadata?.analysis ?? {};
  const repairPlan = analysis.repair_plan ?? {};
  const nullRate =
    analysis.nulls?.null_rate ??
    column.metadata?.null_rate ??
    column.metadata?.analysis?.null_rate;
  const conflictRate =
    analysis.conflicts?.conflict_rate ??
    column.metadata?.analysis?.conflicts?.conflict_rate ??
    column.metadata?.conflicts?.conflict_rate;
  const hasPlanCall = graph.nodes.some(
    (node) => node.type === "plan_data_repairs"
  );
  const hasIssues =
    hasPlanCall ||
    (Array.isArray(repairPlan.steps) && repairPlan.steps.length > 0) ||
    (typeof nullRate === "number" && nullRate > 0) ||
    (typeof conflictRate === "number" && conflictRate > 0);

  if (!hasIssues) {
    return graph;
  }

  const hasPlan = graph.nodes.some((node) => node.type === "plan_data_repairs");
  const hasApproval = graph.nodes.some((node) => node.type === "approval_gate");
  const planNode = graph.nodes.find((node) => node.type === "plan_data_repairs");
  const maxX = graph.nodes.reduce(
    (acc, node) => Math.max(acc, node.position.x),
    basePosition?.x ?? 0
  );
  const maxY = graph.nodes.reduce(
    (acc, node) => Math.max(acc, node.position.y),
    basePosition?.y ?? 0
  );
  const anchorNode =
    planNode ||
    graph.nodes.find((node) => node.type === "scan_conflicts") ||
    graph.nodes.find((node) => node.type === "scan_nulls") ||
    graph.nodes[graph.nodes.length - 1];
  const anchorY = anchorNode?.position.y ?? maxY;
  const colId = normalizeId(column.column_name);
  const edges = [...graph.edges];
  const nextNodes = [...graph.nodes];
  let planNodeId = planNode?.id;
  if (!hasPlan) {
    planNodeId = `plan_data_repairs_${colId}`;
    nextNodes.push({
      id: planNodeId,
      type: "plan_data_repairs",
      position: { x: maxX + COLUMN_NODE_WIDTH, y: anchorY },
      data: {
        ...EDA_NODE_DEFINITIONS.plan_data_repairs.defaultData,
        title: "Repair Plan",
        status: "idle",
        column_name: column.column_name,
        column_type: column.semantic_type,
        column_null_rate: getNullRate(column),
        table_asset_id: column.table_asset_id,
      },
    });

    if (anchorNode) {
      edges.push({ sourceNodeID: anchorNode.id, targetNodeID: planNodeId });
    }
  }

  const approvalNode: WorkflowNode = {
    id: `approval_gate_${colId}`,
    type: "approval_gate",
    position: { x: maxX + COLUMN_NODE_WIDTH * 2, y: anchorY },
    data: {
      ...EDA_NODE_DEFINITIONS.approval_gate.defaultData,
      title: "Approval Gate",
      status: "idle",
      approved: Boolean(repairPlan.approved ?? column.overrides?.data_fix_approved),
      column_name: column.column_name,
      column_type: column.semantic_type,
      column_null_rate: getNullRate(column),
      table_asset_id: column.table_asset_id,
    },
  };
  if (!hasApproval) {
    if (planNodeId) {
      edges.push({ sourceNodeID: planNodeId, targetNodeID: approvalNode.id });
    }
    nextNodes.push(approvalNode);
  }

  const hasApply = nextNodes.some((node) => node.type === "apply_data_repairs");
  if (!hasApply) {
    const applyNode: WorkflowNode = {
      id: `apply_data_repairs_${colId}`,
      type: "apply_data_repairs",
      position: { x: maxX + COLUMN_NODE_WIDTH * 3, y: anchorY },
      data: {
        ...EDA_NODE_DEFINITIONS.apply_data_repairs.defaultData,
        title: "Apply Repairs",
        status: "idle",
        column_name: column.column_name,
        column_type: column.semantic_type,
        column_null_rate: getNullRate(column),
        table_asset_id: column.table_asset_id,
      },
    };
    const approvalId = hasApproval
      ? nextNodes.find((node) => node.type === "approval_gate")?.id
      : approvalNode.id;
    if (approvalId) {
      edges.push({ sourceNodeID: approvalId, targetNodeID: applyNode.id });
    }
    nextNodes.push(applyNode);
  }

  return {
    nodes: nextNodes,
    edges,
  };
}

function applyWorkflowMetadataToNodes(
  column: ColumnMetadataRecord,
  nodes: WorkflowNode[]
): WorkflowNode[] {
  const workflowMeta = column.metadata?.workflow ?? {};
  const taskResults = workflowMeta.task_results ?? {};
  const analysis = column.metadata?.analysis ?? {};
  const repairPlan = analysis.repair_plan ?? {};
  const repairResults = analysis.repair_results ?? [];
  const planSteps = Array.isArray(repairPlan.steps) ? repairPlan.steps : [];
  const planSnapshot = repairPlan.snapshot ?? {};
  const sqlPreviews = repairPlan.sql_previews ?? {};
  const rollback = repairPlan.rollback ?? {};
  const nullRate = analysis.nulls?.null_rate;
  const conflictRate = analysis.conflicts?.conflict_rate;

  return nodes.map((node) => {
    const taskResult = taskResults[node.id];
    const taskStatus = taskResult?.status;
    let status = node.data?.status ?? "idle";
    if (taskStatus === "completed") {
      status = "success";
    } else if (taskStatus === "failed" || taskStatus === "error") {
      status = "error";
    } else if (taskStatus === "running") {
      status = "running";
    }
    const nextData = { ...node.data, status };
    if (node.type === "scan_nulls" && nullRate !== undefined) {
      nextData.null_rate = nullRate;
    }
    if (node.type === "scan_conflicts" && conflictRate !== undefined) {
      nextData.conflict_rate = conflictRate;
    }
    if (
      node.type === "plan_data_repairs" ||
      node.type === "approval_gate" ||
      node.type === "apply_data_repairs"
    ) {
      if (repairPlan.summary) {
        nextData.plan_summary = repairPlan.summary;
      }
      if (repairPlan.token_estimate) {
        nextData.token_estimate = repairPlan.token_estimate;
      }
      if (repairPlan.plan_id) {
        nextData.plan_id = repairPlan.plan_id;
      }
      if (repairPlan.plan_hash) {
        nextData.plan_hash = repairPlan.plan_hash;
      }
      if (planSnapshot?.signature) {
        nextData.snapshot_signature = planSnapshot.signature;
      }
      if (planSnapshot && Object.keys(planSnapshot).length > 0) {
        nextData.snapshot = planSnapshot;
      }
      if (planSteps.length > 0) {
        nextData.plan_steps = planSteps;
      }
      if (Object.keys(sqlPreviews).length > 0) {
        nextData.sql_previews = sqlPreviews;
      }
      if (Object.keys(rollback).length > 0) {
        nextData.rollback = rollback;
      }
      if (repairPlan.row_id_column) {
        nextData.row_id_column = repairPlan.row_id_column;
      }
      if (rollback?.audit_table) {
        nextData.audit_table = rollback.audit_table;
      }
      if (repairPlan.apply_ready !== undefined) {
        nextData.apply_ready = repairPlan.apply_ready;
      }
      if (repairPlan.approval_match !== undefined) {
        nextData.approval_match = repairPlan.approval_match;
      }
      if (repairPlan.apply_skipped_reason) {
        nextData.apply_skipped_reason = repairPlan.apply_skipped_reason;
      }
      const nullStep = planSteps.find((step) => step.type === "null_repair");
      const conflictStep = planSteps.find((step) => step.type === "conflict_repair");
      if (nullStep?.strategy) {
        nextData.null_strategy = nullStep.strategy;
      }
      if (conflictStep?.strategy) {
        nextData.conflict_strategy = conflictStep.strategy;
      }
      if (repairPlan.approved !== undefined && node.type === "approval_gate") {
        nextData.approved = repairPlan.approved;
        if (repairPlan.approved && status === "idle") {
          nextData.status = "success";
        }
      }
      if (node.type === "plan_data_repairs" && repairPlan.plan_id) {
        nextData.status = nextData.status === "idle" ? "success" : nextData.status;
      }
    }
    if (node.type === "apply_data_repairs" && Array.isArray(repairResults)) {
      if (repairResults.length > 0) {
        const applied = repairResults.some((item: any) => item.status === "applied");
        const failed = repairResults.some((item: any) => item.status === "failed");
        const skipped = repairResults.some((item: any) => item.status === "skipped");
        const dryRun = repairResults.some((item: any) => item.status === "dry_run");
        nextData.status = applied
          ? "success"
          : failed
          ? "error"
          : skipped || dryRun
          ? "skipped"
          : status;
      }
    }
    return {
      ...node,
      data: nextData,
    };
  });
}

function buildColumnWorkflowState(
  column: ColumnMetadataRecord,
  tableAssetId: number,
  tableName: string,
  layoutIndex: number,
  isRunning: boolean
): ColumnWorkflowState {
  const graph = hydrateColumnWorkflowGraph(
    column,
    tableAssetId,
    tableName,
    layoutIndex
  );
  const nodes = applyWorkflowMetadataToNodes(column, graph.nodes);
  return {
    column,
    nodes,
    edges: graph.edges,
    isRunning,
  };
}

const ColumnWorkflowPanel = ({
  tableAssetId,
  tableName,
}: ColumnWorkflowPanelProps) => {
  const { toast } = useToast();
  const { loadReport } = useTableStore();
  const [loading, setLoading] = useState(true);
  const [workflows, setWorkflows] = useState<
    Record<string, ColumnWorkflowState>
  >({});
  const [boards, setBoards] = useState<BoardState[]>([]);
  const [activeBoardId, setActiveBoardId] = useState<string | null>(null);
  const [boardExtras, setBoardExtras] = useState<
    Record<string, { nodes: WorkflowNode[]; edges: WorkflowEdge[] }>
  >({});
  const lastBoardsPayloadRef = useRef<string | null>(null);
  const persistTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const activeBoardIdRef = useRef<string | null>(null);
  const hydratingRef = useRef(false);
  const [estimateResults, setEstimateResults] = useState<
    ColumnWorkflowEstimate[] | null
  >(null);
  const [estimateTargets, setEstimateTargets] = useState<string[]>([]);
  const [isEstimating, setIsEstimating] = useState(false);
  const [filter, setFilter] = useState("");
  const [logs, setLogs] = useState<WorkflowLogEvent[]>([]);
  const [isLogExpanded, setIsLogExpanded] = useState(true);
  const [expandedGroups, setExpandedGroups] = useState<Record<string, boolean>>(
    {}
  );
  const [canvasSelectionIds, setCanvasSelectionIds] = useState<string[]>([]);
  const [selectionMode, setSelectionMode] = useState<"list" | "canvas" | "none">("none");
  const selectionSourceRef = useRef<"list" | "canvas" | null>(null);
  const selectionResetRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const logCursorRef = useRef<Record<string, number>>({});

  const appendLog = useCallback(
    (type: WorkflowLogEvent["type"], message: string, data?: any) => {
      setLogs((prev) => {
        const next = [
          ...prev,
          { type, timestamp: new Date().toISOString(), message, data },
        ];
        return next.slice(-200);
      });
    },
    []
  );

  const appendWorkflowLogsDelta = useCallback(
    (columnName: string, events?: WorkflowLogEvent[]) => {
      if (!events || events.length === 0) return;
      const prevCount = logCursorRef.current[columnName] || 0;
      if (events.length <= prevCount) return;
      const nextBatch = events.slice(prevCount);
      logCursorRef.current[columnName] = events.length;
      setLogs((prev) => {
        const next = [...prev, ...nextBatch];
        return next.slice(-200);
      });
    },
    []
  );

  const createBoard = useCallback(
    (
      name: string,
      columnNames: string[],
      selectedColumns?: string[]
    ): BoardState => {
      const id =
        typeof crypto !== "undefined" && crypto.randomUUID
          ? crypto.randomUUID()
          : `board-${Date.now()}-${Math.random().toString(16).slice(2, 8)}`;
      const initialSelection =
        selectedColumns && selectedColumns.length > 0
          ? selectedColumns
          : columnNames;
      return {
        id,
        name,
        columnNames,
        selectedColumns: columnNames.length > 0 ? initialSelection : [],
      };
    },
    []
  );

  const buildBoardsPayload = useCallback((nextBoards: BoardState[]) => {
    return nextBoards.map((board) => ({
      id: board.id,
      name: board.name,
      columnNames: board.columnNames,
      selectedColumns: board.selectedColumns,
    }));
  }, []);

  const persistBoards = useCallback(
    async (nextBoards: BoardState[]) => {
      const payload = buildBoardsPayload(nextBoards);
      const extrasPayload = Object.entries(boardExtras).reduce<
        Record<string, any>
      >((acc, [boardId, extra]) => {
        acc[boardId] = {
          nodes: extra.nodes,
          edges: extra.edges,
        };
        return acc;
      }, {});
      const activeId = activeBoardIdRef.current ?? nextBoards[0]?.id ?? null;
      try {
        await columnMetadataApi.overrideTable(tableAssetId, {
          workflow_boards: payload,
          workflow_board_extras: extrasPayload,
          workflow_active_board_id: activeId,
        });
      } catch (error) {
        appendLog("error", "Failed to save board configuration", { error });
      }
    },
    [appendLog, boardExtras, buildBoardsPayload, tableAssetId]
  );

  const loadMetadata = useCallback(async () => {
    setLoading(true);
    try {
      const response = await columnMetadataApi.get(tableAssetId);
      let data = response.data;
      if (!data || data.columns.length === 0) {
        const initResponse = await columnMetadataApi.initialize(tableAssetId);
        data = initResponse.data;
      }
      if (!data) {
        throw new Error("No metadata returned");
      }

      const columns = data.columns;
      setWorkflows((prev) => {
        const nextWorkflows: Record<string, ColumnWorkflowState> = {};
        columns.forEach((column, index) => {
          nextWorkflows[column.column_name] = buildColumnWorkflowState(
            column,
            tableAssetId,
            tableName,
            index,
            prev[column.column_name]?.isRunning ?? false
          );
        });
        return nextWorkflows;
      });

      const persistedBoards = Array.isArray(
        data.table?.overrides?.workflow_boards
      )
        ? data.table?.overrides?.workflow_boards
        : null;
      const persistedExtras =
        data.table?.overrides?.workflow_board_extras &&
        typeof data.table?.overrides?.workflow_board_extras === "object"
          ? data.table?.overrides?.workflow_board_extras
          : null;
      const persistedActiveBoardId =
        typeof data.table?.overrides?.workflow_active_board_id === "string"
          ? data.table?.overrides?.workflow_active_board_id
          : null;

      let sanitizedExtras: Record<
        string,
        { nodes: WorkflowNode[]; edges: WorkflowEdge[] }
      > | null = null;
      if (persistedExtras) {
        sanitizedExtras = Object.entries(persistedExtras).reduce<
          Record<string, { nodes: WorkflowNode[]; edges: WorkflowEdge[] }>
        >((acc, [boardId, extra]) => {
          if (!extra || typeof extra !== "object") return acc;
          const nodes = Array.isArray((extra as any).nodes)
            ? (extra as any).nodes
            : [];
          const edges = Array.isArray((extra as any).edges)
            ? (extra as any).edges
            : [];
          acc[boardId] = { nodes, edges };
          return acc;
        }, {});
        setBoardExtras((prev) => ({ ...prev, ...sanitizedExtras }));
      }

      setBoards((prev) => {
        hydratingRef.current = true;
        const columnNames = columns.map((column) => column.column_name);
        const preferredActiveId = activeBoardIdRef.current ?? activeBoardId ?? null;
        if (persistedBoards) {
          const prevSelections = new Map(
            prev.map((board) => [board.id, board.selectedColumns])
          );
          const normalized = persistedBoards
            .map((board: any) => {
              const name =
                typeof board?.name === "string" ? board.name : "Board";
              const id =
                typeof board?.id === "string"
                  ? board.id
                  : `board-${Date.now()}-${Math.random()}`;
              const names = Array.isArray(board?.columnNames)
                ? board.columnNames
                : [];
              const filtered = names.filter((name: string) =>
                columnNames.includes(name)
              );
              const selected =
                prevSelections.get(id) ||
                (Array.isArray(board?.selectedColumns)
                  ? board.selectedColumns
                  : []);
              const nextSelected = selected.filter((name: string) =>
                filtered.includes(name)
              );
              return {
                id,
                name,
                columnNames: filtered,
                selectedColumns:
                  filtered.length > 0
                    ? nextSelected.length > 0
                      ? nextSelected
                      : filtered
                    : [],
              } satisfies BoardState;
            })
            .filter((board: BoardState) => board.columnNames.length > 0);

          const ensured =
            normalized.length > 0
              ? normalized
              : [createBoard("Board 1", columnNames)];
          const assigned = new Set(
            ensured.flatMap((board) => board.columnNames)
          );
          const unassigned = columnNames.filter((name) => !assigned.has(name));
          if (unassigned.length > 0) {
            ensured[0] = {
              ...ensured[0],
              columnNames: [...ensured[0].columnNames, ...unassigned],
              selectedColumns: Array.from(
                new Set([...(ensured[0].selectedColumns || []), ...unassigned])
              ),
            };
          }
          if (preferredActiveId && ensured.some((board) => board.id === preferredActiveId)) {
            setActiveBoardId(preferredActiveId);
          } else if (
            persistedActiveBoardId &&
            ensured.some((board) => board.id === persistedActiveBoardId)
          ) {
            setActiveBoardId(persistedActiveBoardId);
          } else if (!activeBoardId && ensured.length > 0) {
            setActiveBoardId(ensured[0].id);
          }
          lastBoardsPayloadRef.current = JSON.stringify({
            boards: buildBoardsPayload(ensured),
            extras: sanitizedExtras || {},
            activeBoardId:
              (preferredActiveId && ensured.some((board) => board.id === preferredActiveId))
                ? preferredActiveId
                : persistedActiveBoardId,
          });
          setTimeout(() => {
            hydratingRef.current = false;
          }, 0);
          return ensured;
        }

        if (prev.length === 0) {
          const board = createBoard("Board 1", columnNames);
          setActiveBoardId(board.id);
          lastBoardsPayloadRef.current = JSON.stringify({
            boards: buildBoardsPayload([board]),
            extras: sanitizedExtras || {},
            activeBoardId: board.id,
          });
          setTimeout(() => {
            hydratingRef.current = false;
          }, 0);
          return [board];
        }

        const nextBoards = prev.map((board) => {
          const nextColumnNames = board.columnNames.filter((name) =>
            columnNames.includes(name)
          );
          const nextSelection = board.selectedColumns.filter((name) =>
            nextColumnNames.includes(name)
          );
          return {
            ...board,
            columnNames: nextColumnNames,
            selectedColumns:
              nextColumnNames.length > 0
                ? nextSelection.length > 0
                  ? nextSelection
                  : nextColumnNames
                : [],
          };
        });

        const assigned = new Set(
          nextBoards.flatMap((board) => board.columnNames)
        );
        const unassigned = columnNames.filter((name) => !assigned.has(name));
        if (unassigned.length === 0) {
          return nextBoards;
        }
        const targetId = activeBoardIdRef.current || activeBoardId || nextBoards[0]?.id;
        if (!targetId) {
          return nextBoards;
        }
        const updatedBoards = nextBoards.map((board) =>
          board.id === targetId
            ? {
                ...board,
                columnNames: [...board.columnNames, ...unassigned],
                selectedColumns: Array.from(
                  new Set([...board.selectedColumns, ...unassigned])
                ),
              }
            : board
        );
        lastBoardsPayloadRef.current = JSON.stringify({
          boards: buildBoardsPayload(updatedBoards),
          extras: sanitizedExtras || {},
          activeBoardId: targetId,
        });
        setTimeout(() => {
          hydratingRef.current = false;
        }, 0);
        return updatedBoards;
      });

      columns.forEach((column) => {
        const columnLogs = (column.metadata as any)?.workflow?.logs ?? [];
        logCursorRef.current[column.column_name] = columnLogs.length;
      });
      const storedLogs = columns.flatMap(
        (column) => (column.metadata as any)?.workflow?.logs ?? []
      );
      if (storedLogs.length > 0) {
        setLogs((prev) => (prev.length > 0 ? prev : storedLogs.slice(-200)));
      }
    } catch (error) {
      toast({
        title: "Failed to load column metadata",
        description: error instanceof Error ? error.message : "Unknown error",
        variant: "destructive",
      });
    } finally {
      setLoading(false);
    }
  }, [createBoard, tableAssetId, tableName, toast]);

  const refreshWorkflowSnapshots = useCallback(async () => {
    try {
      const response = await columnMetadataApi.get(tableAssetId);
      if (response.status !== "success" || !response.data) {
        return;
      }
      const columns = response.data.columns;
      setWorkflows((prev) => {
        const nextWorkflows: Record<string, ColumnWorkflowState> = {};
        columns.forEach((column, index) => {
          const previous = prev[column.column_name];
          nextWorkflows[column.column_name] = buildColumnWorkflowState(
            column,
            tableAssetId,
            tableName,
            index,
            previous?.isRunning ?? false
          );
        });
        return nextWorkflows;
      });

      columns.forEach((column) => {
        const columnLogs = (column.metadata as any)?.workflow?.logs ?? [];
        appendWorkflowLogsDelta(column.column_name, columnLogs);
      });
    } catch (error) {
      appendLog("error", "Failed to refresh workflow logs", { error });
    }
  }, [appendLog, appendWorkflowLogsDelta, tableAssetId, tableName]);

  useEffect(() => {
    if (tableAssetId) {
      void loadMetadata();
    }
  }, [tableAssetId, loadMetadata]);

  useEffect(() => {
    activeBoardIdRef.current = activeBoardId ?? boards[0]?.id ?? null;
  }, [activeBoardId, boards]);

  useEffect(() => {
    if (boards.length === 0) return;
    if (hydratingRef.current) return;
    const payload = {
      boards: buildBoardsPayload(boards),
      extras: boardExtras,
      activeBoardId: activeBoardIdRef.current,
    };
    const serialized = JSON.stringify(payload);
    if (serialized === lastBoardsPayloadRef.current) return;
    lastBoardsPayloadRef.current = serialized;
    if (persistTimerRef.current) {
      clearTimeout(persistTimerRef.current);
    }
    persistTimerRef.current = setTimeout(() => {
      void persistBoards(boards);
    }, 600);
  }, [boards, boardExtras, buildBoardsPayload, persistBoards]);

  useEffect(() => {
    if (boards.length === 0) return;
    if (activeBoardId && boards.some((board) => board.id === activeBoardId)) {
      return;
    }
    setActiveBoardId(boards[0].id);
  }, [activeBoardId, boards]);

  const workflowList = useMemo(() => Object.values(workflows), [workflows]);
  const columns = useMemo(
    () => workflowList.map((workflow) => workflow.column),
    [workflowList]
  );

  const activeBoardIdValue = activeBoardId ?? boards[0]?.id ?? null;
  const activeBoard = useMemo(
    () => boards.find((board) => board.id === activeBoardIdValue) ?? boards[0],
    [boards, activeBoardIdValue]
  );

  const updateActiveBoard = useCallback(
    (updater: (board: BoardState) => BoardState, targetBoardId?: string) => {
      setBoards((prev) => {
        const activeId =
          targetBoardId ?? activeBoardIdRef.current ?? prev[0]?.id;
        if (!activeId) {
          return prev;
        }
        return prev.map((board) =>
          board.id === activeId ? updater(board) : board
        );
      });
    },
    []
  );

  const selectedColumns = activeBoard?.selectedColumns ?? [];

  useEffect(() => {
    if (!activeBoard) return;
    if (activeBoard.columnNames.length === 0) return;
    const validSelection = activeBoard.selectedColumns.filter((name) =>
      activeBoard.columnNames.includes(name)
    );
    if (validSelection.length !== activeBoard.selectedColumns.length) {
      updateActiveBoard(
        (board) => ({ ...board, selectedColumns: validSelection }),
        activeBoard.id
      );
    }
  }, [activeBoard, updateActiveBoard]);

  const activeBoardColumns = useMemo(() => {
    if (!activeBoard) return [];
    const names = new Set(activeBoard.columnNames);
    return columns.filter((column) => names.has(column.column_name));
  }, [activeBoard, columns]);

  const filteredColumns = useMemo(() => {
    const query = filter.trim().toLowerCase();
    if (!query) return activeBoardColumns;
    return activeBoardColumns.filter(
      (column) =>
        column.column_name.toLowerCase().includes(query) ||
        column.semantic_type.toLowerCase().includes(query)
    );
  }, [activeBoardColumns, filter]);
  const isFiltering = filter.trim().length > 0;

  const groupedColumns = useMemo(() => {
    const order = [
      "numeric",
      "temporal",
      "categorical",
      "text",
      "spatial",
      "binary",
      "image",
      "unknown",
      "id",
    ];
    const labels: Record<string, string> = {
      numeric: "Numeric",
      temporal: "Temporal",
      categorical: "Categorical",
      text: "Text",
      spatial: "Spatial",
      binary: "Binary",
      image: "Image",
      id: "Identifiers",
      unknown: "Unknown",
    };
    const map = new Map<string, ColumnMetadataRecord[]>();
    filteredColumns.forEach((column) => {
      const key = column.semantic_type || "unknown";
      const list = map.get(key) ?? [];
      list.push(column);
      map.set(key, list);
    });
    const sortColumns = (columns: ColumnMetadataRecord[]) =>
      [...columns].sort((a, b) => {
        const aRate = getNullRateSortValue(a);
        const bRate = getNullRateSortValue(b);
        if (aRate !== bRate) {
          return aRate - bRate;
        }
        return a.column_name.localeCompare(b.column_name);
      });
    const groups = order
      .filter((key) => map.has(key))
      .map((key) => ({
        type: key,
        label: labels[key] ?? key,
        columns: sortColumns(map.get(key) ?? []),
      }));
    const extraGroups = Array.from(map.keys())
      .filter((key) => !order.includes(key))
      .map((key) => ({
        type: key,
        label: labels[key] ?? key,
        columns: sortColumns(map.get(key) ?? []),
      }));
    return [...groups, ...extraGroups];
  }, [filteredColumns]);

  const defaultGroupsExpanded = activeBoardColumns.length <= 12;

  useEffect(() => {
    if (groupedColumns.length === 0) return;
    setExpandedGroups((prev) => {
      const next = { ...prev };
      groupedColumns.forEach((group) => {
        if (next[group.type] === undefined) {
          next[group.type] = defaultGroupsExpanded;
        }
      });
      return next;
    });
  }, [defaultGroupsExpanded, groupedColumns]);

  const boardGraph = useMemo(() => {
    const nodes: WorkflowNode[] = [];
    const edges: WorkflowEdge[] = [];
    if (!activeBoard) {
      return { nodes, edges };
    }
    activeBoard.columnNames.forEach((columnName) => {
      const workflow = workflows[columnName];
      if (!workflow) return;
      nodes.push(...workflow.nodes);
      edges.push(...workflow.edges);
    });
    const extras = boardExtras[activeBoard.id];
    if (extras) {
      nodes.push(...extras.nodes);
      edges.push(...extras.edges);
    }
    return { nodes, edges };
  }, [activeBoard, boardExtras, workflows]);

  const nodeIdToColumn = useMemo(() => {
    const mapping = new Map<string, string>();
    if (!activeBoard) return mapping;
    activeBoard.columnNames.forEach((columnName) => {
      const workflow = workflows[columnName];
      if (!workflow) return;
      workflow.nodes.forEach((node) => {
        mapping.set(node.id, columnName);
      });
    });
    return mapping;
  }, [activeBoard, workflows]);

  const selectedNodeIds = canvasSelectionIds;

  const updateWorkflowData = useCallback(
    (data: { nodes: any[]; edges: any[] }) => {
      if (!activeBoard) return;

      const nodeColumnMap = new Map<string, string>();
      const extraNodes: WorkflowNode[] = [];
      const extraNodeIds = new Set<string>();
      const nodesByColumn = new Map<string, WorkflowNode[]>();
      data.nodes.forEach((node) => {
        const columnName = node.data?.column_name;
        if (columnName) {
          nodeColumnMap.set(node.id, columnName);
          const list = nodesByColumn.get(columnName) ?? [];
          list.push({
            id: node.id,
            type: node.type as EDANodeType,
            position: node.meta?.position ?? { x: 0, y: 0 },
            data: node.data,
          });
          nodesByColumn.set(columnName, list);
        } else {
          extraNodeIds.add(node.id);
          extraNodes.push({
            id: node.id,
            type: node.type as EDANodeType,
            position: node.meta?.position ?? { x: 0, y: 0 },
            data: node.data,
          });
        }
      });

      const extraEdges = data.edges.filter(
        (edge) =>
          extraNodeIds.has(edge.sourceNodeID) &&
          extraNodeIds.has(edge.targetNodeID)
      );
      const edgesByColumn = new Map<string, WorkflowEdge[]>();
      data.edges.forEach((edge) => {
        const sourceColumn = nodeColumnMap.get(edge.sourceNodeID);
        const targetColumn = nodeColumnMap.get(edge.targetNodeID);
        if (!sourceColumn || sourceColumn !== targetColumn) return;
        const list = edgesByColumn.get(sourceColumn) ?? [];
        list.push({
          sourceNodeID: edge.sourceNodeID,
          targetNodeID: edge.targetNodeID,
        });
        edgesByColumn.set(sourceColumn, list);
      });
      setBoardExtras((prevExtras) => ({
        ...prevExtras,
        [activeBoard.id]: {
          nodes: extraNodes,
          edges: extraEdges.map((edge) => ({
            sourceNodeID: edge.sourceNodeID,
            targetNodeID: edge.targetNodeID,
          })),
        },
      }));

      setWorkflows((prev) => {
        const next = { ...prev };
        nodesByColumn.forEach((nodes, columnName) => {
          next[columnName] = {
            ...next[columnName],
            nodes,
            edges: edgesByColumn.get(columnName) ?? next[columnName].edges,
          };
        });
        return next;
      });

    },
    [activeBoard]
  );

  const handleCanvasSelection = useCallback(
    (nodeIds: string[]) => {
      if (!activeBoard) return;
      if (nodeIds.length === 0) {
        setCanvasSelectionIds([]);
        setSelectionMode("none");
        return;
      }
      if (selectionSourceRef.current === "list") {
        selectionSourceRef.current = null;
        return;
      }
      selectionSourceRef.current = "canvas";
      setSelectionMode("canvas");
      setCanvasSelectionIds(nodeIds);
      const selected = new Set<string>();
      nodeIds.forEach((id) => {
        const columnName = nodeIdToColumn.get(id);
        if (columnName) {
          selected.add(columnName);
        }
      });
      if (selected.size === 0) {
        return;
      }
      updateActiveBoard(
        (board) => ({ ...board, selectedColumns: Array.from(selected) }),
        activeBoard.id
      );
    },
    [activeBoard, nodeIdToColumn, updateActiveBoard]
  );

  const handleToggleColumn = useCallback(
    (columnName: string, checked: boolean) => {
      selectionSourceRef.current = "list";
      setSelectionMode("list");
      if (selectionResetRef.current) {
        clearTimeout(selectionResetRef.current);
      }
      selectionResetRef.current = setTimeout(() => {
        if (selectionSourceRef.current === "list") {
          selectionSourceRef.current = null;
        }
      }, 150);
      updateActiveBoard((board) => {
        const selected = new Set(board.selectedColumns);
        if (checked) {
          selected.add(columnName);
        } else {
          selected.delete(columnName);
        }
        return { ...board, selectedColumns: Array.from(selected) };
      }, activeBoard?.id);
    },
    [activeBoard, updateActiveBoard]
  );

  const handleClearSelection = useCallback(() => {
    if (!activeBoard) return;
    setCanvasSelectionIds([]);
    setSelectionMode("none");
    updateActiveBoard(
      (board) => ({ ...board, selectedColumns: [] }),
      activeBoard.id
    );
  }, [activeBoard, updateActiveBoard]);

  useEffect(() => {
    if (selectionMode !== "list") {
      return;
    }
    if (!activeBoard) {
      setCanvasSelectionIds([]);
      return;
    }
    const ids: string[] = [];
    activeBoard.selectedColumns.forEach((columnName) => {
      const workflow = workflows[columnName];
      if (!workflow) return;
      workflow.nodes.forEach((node) => ids.push(node.id));
    });
    setCanvasSelectionIds(ids);
  }, [activeBoard, selectionMode, workflows]);

  useEffect(() => {
    if (selectionMode !== "canvas") {
      return;
    }
    const nodeIdSet = new Set(boardGraph.nodes.map((node) => node.id));
    setCanvasSelectionIds((prev) => prev.filter((id) => nodeIdSet.has(id)));
  }, [boardGraph.nodes, selectionMode]);

  const buildToolCallsFromNodes = useCallback(
    (nodes: WorkflowNode[]) => {
      return nodes
        .map((node) => {
          const nodeType = node.type as string;
          let toolName = NODE_TYPE_TOOL_MAP[nodeType];
          if (nodeType === "generate_visuals") {
            toolName =
              node.data?.column_type === "categorical"
                ? "generate_categorical_visuals"
                : "generate_numeric_visuals";
          }
          if (nodeType === "generate_insights") {
            toolName =
              node.data?.column_type === "categorical"
                ? "generate_categorical_insights"
                : "generate_numeric_insights";
          }
          if (!toolName) {
            return null;
          }
          return {
            tool_name: toolName,
            input: {
              ...(node.data ?? {}),
              table_asset_id: node.data?.table_asset_id,
              column_name: node.data?.column_name,
              approved: node.data?.approved,
            },
          };
        })
        .filter(Boolean) as Array<{ tool_name: string; input: Record<string, any> }>;
    },
    []
  );

  const runSelectedNodes = useCallback(async () => {
    if (!activeBoard || canvasSelectionIds.length === 0) {
      return;
    }
    const selectedNodes = boardGraph.nodes.filter((node) =>
      canvasSelectionIds.includes(node.id)
    );
    const toolNodes = selectedNodes.filter((node) =>
      NODE_TYPE_TOOL_MAP[node.type as string] ||
      node.type === "generate_visuals" ||
      node.type === "generate_insights"
    );
    if (toolNodes.length === 0) {
      toast({ title: "Select tool nodes to run." });
      return;
    }
    appendLog("status", `Running ${toolNodes.length} selected nodes...`);

    const nodesByColumn = new Map<string, WorkflowNode[]>();
    toolNodes.forEach((node) => {
      const columnName = node.data?.column_name;
      if (!columnName) return;
      const list = nodesByColumn.get(columnName) ?? [];
      list.push(node);
      nodesByColumn.set(columnName, list);
    });

    setWorkflows((prev) => {
      const next = { ...prev };
      nodesByColumn.forEach((nodes, columnName) => {
        const workflow = next[columnName];
        if (!workflow) return;
        const selectedIds = new Set(nodes.map((node) => node.id));
        next[columnName] = {
          ...workflow,
          isRunning: true,
          nodes: workflow.nodes.map((node) => ({
            ...node,
            data: {
              ...node.data,
              status: selectedIds.has(node.id) ? "running" : node.data.status,
            },
          })),
        };
      });
      return next;
    });

    const results = await Promise.allSettled(
      Array.from(nodesByColumn.entries()).map(async ([columnName, nodes]) => {
        const workflow = workflows[columnName];
        if (workflow) {
          const overrides = collectOverrides(workflow.nodes);
          await columnMetadataApi.override(tableAssetId, columnName, {
            ...overrides,
          });
        }
        const toolCalls = buildToolCallsFromNodes(nodes);
        const response = await columnWorkflowsApi.runSelected(
          tableAssetId,
          columnName,
          {
            tool_calls: toolCalls,
          }
        );
        return { columnName, response };
      })
    );

    setWorkflows((prev) => {
      const next = { ...prev };
      results.forEach((result) => {
        if (result.status === "fulfilled") {
          const workflow = next[result.value.columnName];
          if (!workflow) return;
          next[result.value.columnName] = {
            ...workflow,
            isRunning: false,
            nodes: workflow.nodes.map((node) => ({
              ...node,
              data: { ...node.data, status: node.data.status === "running" ? "success" : node.data.status },
            })),
          };
        } else {
          const columnName = (result as PromiseRejectedResult).reason
            ?.columnName;
          if (columnName && next[columnName]) {
            next[columnName] = {
              ...next[columnName],
              isRunning: false,
              nodes: next[columnName].nodes.map((node) => ({
                ...node,
                data: {
                  ...node.data,
                  status: node.data.status === "running" ? "error" : node.data.status,
                },
              })),
            };
          }
        }
      });
      return next;
    });

    results.forEach((result) => {
      if (result.status === "fulfilled") {
        const payload = result.value.response.data;
        appendWorkflowLogsDelta(result.value.columnName, payload?.workflow_logs);
        appendLog(
          "complete",
          `Workflow completed for ${result.value.columnName}`,
          payload
        );
      } else {
        const columnName = (result as PromiseRejectedResult).reason?.columnName;
        appendLog(
          "error",
          `Workflow failed${columnName ? ` for ${columnName}` : ""}`,
          (result as PromiseRejectedResult).reason
        );
      }
    });

    await loadMetadata();
    await loadReport(String(tableAssetId));
  }, [
    activeBoard,
    appendLog,
    appendWorkflowLogsDelta,
    boardGraph.nodes,
    buildToolCallsFromNodes,
    canvasSelectionIds,
    loadMetadata,
    loadReport,
    tableAssetId,
    toast,
    workflows,
  ]);

  const handleAddBoard = useCallback(() => {
    if (!activeBoard) return;
    const unselected = activeBoard.columnNames.filter(
      (name) => !selectedColumns.includes(name)
    );
    if (unselected.length === 0) {
      toast({
        title: "Select columns to keep on this board before splitting.",
      });
      return;
    }
    const board = createBoard(`Board ${boards.length + 1}`, unselected);
    const nextBoards = boards
      .map((item) => {
        if (item.id !== activeBoard.id) return item;
        const remaining = item.columnNames.filter(
          (name) => !unselected.includes(name)
        );
        const remainingSelected = item.selectedColumns.filter((name) =>
          remaining.includes(name)
        );
        return {
          ...item,
          columnNames: remaining,
          selectedColumns:
            remaining.length > 0
              ? remainingSelected.length > 0
                ? remainingSelected
                : remaining
              : [],
        };
      })
      .concat(board);
    setBoards(nextBoards);
    setActiveBoardId(board.id);
    activeBoardIdRef.current = board.id;
    lastBoardsPayloadRef.current = JSON.stringify({
      boards: buildBoardsPayload(nextBoards),
      extras: boardExtras,
      activeBoardId: board.id,
    });
    void persistBoards(nextBoards);
  }, [
    activeBoard,
    boardExtras,
    boards,
    buildBoardsPayload,
    createBoard,
    persistBoards,
    selectedColumns,
    toast,
  ]);

  const isRunningAny = useMemo(
    () => Object.values(workflows).some((wf) => wf.isRunning),
    [workflows]
  );

  useEffect(() => {
    if (!isRunningAny) return;
    let active = true;
    const poll = async () => {
      if (!active) return;
      await refreshWorkflowSnapshots();
    };
    void poll();
    const intervalId = setInterval(poll, 1200);
    return () => {
      active = false;
      clearInterval(intervalId);
    };
  }, [isRunningAny, refreshWorkflowSnapshots]);

  const handleEstimateSelected = useCallback(async () => {
    if (!activeBoard) return;
    if (selectionMode === "canvas" && canvasSelectionIds.length > 0) {
      await runSelectedNodes();
      return;
    }
    if (selectedColumns.length === 0) {
      toast({ title: "Select at least one workflow to run." });
      return;
    }

    setIsEstimating(true);
    appendLog(
      "status",
      `Estimating tokens for ${selectedColumns.length} columns...`
    );

    const results = await Promise.allSettled(
      selectedColumns.map(async (columnName) => {
        const workflow = workflows[columnName];
        if (workflow) {
          const overrides = collectOverrides(workflow.nodes);
          await columnMetadataApi.override(tableAssetId, columnName, {
            ...overrides,
          });
        }
        const response = await columnWorkflowsApi.estimate(
          tableAssetId,
          columnName
        );
        return response.data as ColumnWorkflowEstimate;
      })
    );

    const estimates: ColumnWorkflowEstimate[] = [];
    results.forEach((result) => {
      if (result.status === "fulfilled" && result.value) {
        estimates.push(result.value);
      } else if (result.status === "rejected") {
        appendLog("error", "Token estimate failed", { error: result.reason });
      }
    });

    if (estimates.length === 0) {
      toast({
        title: "Failed to estimate tokens",
        description: "No estimates returned from server",
        variant: "destructive",
      });
      setIsEstimating(false);
      return;
    }

    appendLog(
      "complete",
      `Token estimate ready for ${estimates.length} columns.`
    );
    setEstimateResults(estimates);
    setEstimateTargets(estimates.map((item) => item.column));
    setIsEstimating(false);
  }, [
    activeBoard,
    appendLog,
    canvasSelectionIds,
    runSelectedNodes,
    selectedColumns,
    selectionMode,
    tableAssetId,
    toast,
    workflows,
  ]);

  const handleRunSelected = useCallback(async () => {
    const targets =
      estimateTargets.length > 0 ? estimateTargets : selectedColumns;
    if (targets.length === 0) {
      setEstimateResults(null);
      return;
    }

    appendLog("status", `Running workflows for ${targets.length} columns...`);
    setEstimateResults(null);

    setWorkflows((prev) => {
      const next = { ...prev };
      targets.forEach((columnName) => {
        const workflow = next[columnName];
        if (!workflow) return;
        next[columnName] = {
          ...workflow,
          isRunning: true,
          nodes: workflow.nodes.map((node) => ({
            ...node,
            data: { ...node.data, status: "running" },
          })),
        };
      });
      return next;
    });

    const results = await Promise.allSettled(
      targets.map(async (columnName) => {
        try {
          const workflow = workflows[columnName];
          if (workflow) {
            const overrides = collectOverrides(workflow.nodes);
            await columnMetadataApi.override(tableAssetId, columnName, {
              ...overrides,
            });
          }
          const response = await columnWorkflowsApi.run(
            tableAssetId,
            columnName
          );
          return { columnName, response };
        } catch (error) {
          throw { columnName, error };
        }
      })
    );

    setWorkflows((prev) => {
      const next = { ...prev };
      results.forEach((result) => {
        if (result.status === "fulfilled") {
          const workflow = next[result.value.columnName];
          if (!workflow) return;
          next[result.value.columnName] = {
            ...workflow,
            isRunning: false,
            nodes: workflow.nodes.map((node) => ({
              ...node,
              data: { ...node.data, status: "success" },
            })),
          };
        } else {
          const columnName = (result as PromiseRejectedResult).reason
            ?.columnName;
          if (columnName && next[columnName]) {
            next[columnName] = {
              ...next[columnName],
              isRunning: false,
              nodes: next[columnName].nodes.map((node) => ({
                ...node,
                data: {
                  ...node.data,
                  status:
                    node.data.status === "running" ? "error" : node.data.status,
                },
              })),
            };
          }
        }
      });
      return next;
    });

    results.forEach((result) => {
      if (result.status === "fulfilled") {
        const payload = result.value.response.data;
        appendWorkflowLogsDelta(result.value.columnName, payload?.workflow_logs);
        appendLog(
          "complete",
          `Workflow completed for ${result.value.columnName}`,
          payload
        );
      } else {
        const columnName = (result as PromiseRejectedResult).reason?.columnName;
        appendLog(
          "error",
          `Workflow failed${columnName ? ` for ${columnName}` : ""}`,
          (result as PromiseRejectedResult).reason
        );
      }
    });

    await loadMetadata();
    await loadReport(String(tableAssetId));
    setEstimateTargets([]);
  }, [
    appendLog,
    appendWorkflowLogsDelta,
    estimateTargets,
    loadMetadata,
    loadReport,
    selectedColumns,
    tableAssetId,
  ]);

  const runSingleColumn = useCallback(
    async (
      columnName: string,
      focus?: string,
      overridePatch?: Record<string, any>
    ) => {
      appendLog("status", `Running workflow for ${columnName}...`);
      setWorkflows((prev) => {
        const next = { ...prev };
        const workflow = next[columnName];
        if (!workflow) return next;
        next[columnName] = {
          ...workflow,
          isRunning: true,
          nodes: workflow.nodes.map((node) => ({
            ...node,
            data: { ...node.data, status: "running" },
          })),
        };
        return next;
      });

      try {
        const workflow = workflows[columnName];
        if (workflow) {
          const overrides = {
            ...collectOverrides(workflow.nodes),
            ...(overridePatch || {}),
          };
          await columnMetadataApi.override(tableAssetId, columnName, {
            ...overrides,
          });
        }
        const response = await columnWorkflowsApi.run(tableAssetId, columnName, {
          focus,
        });
        appendWorkflowLogsDelta(columnName, response.data.workflow_logs);
        appendLog(
          "complete",
          `Workflow completed for ${columnName}`,
          response.data
        );
        setWorkflows((prev) => {
          const next = { ...prev };
          const current = next[columnName];
          if (!current) return next;
          next[columnName] = {
            ...current,
            isRunning: false,
            nodes: current.nodes.map((node) => ({
              ...node,
              data: { ...node.data, status: "success" },
            })),
          };
          return next;
        });
      } catch (error) {
        appendLog("error", `Workflow failed for ${columnName}`, error);
        setWorkflows((prev) => {
          const next = { ...prev };
          const current = next[columnName];
          if (!current) return next;
          next[columnName] = {
            ...current,
            isRunning: false,
            nodes: current.nodes.map((node) => ({
              ...node,
              data: {
                ...node.data,
                status: node.data.status === "running" ? "error" : node.data.status,
              },
            })),
          };
          return next;
        });
      }

      await loadMetadata();
      await loadReport(String(tableAssetId));
    },
    [
      appendLog,
      appendWorkflowLogsDelta,
      loadMetadata,
      loadReport,
      tableAssetId,
      workflows,
    ]
  );

  useEffect(() => {
    const handler = (event: Event) => {
      const detail = (event as CustomEvent<{
        tableAssetId: number;
        columnName: string;
        note?: string;
        planId?: string;
        planHash?: string;
        snapshotSignature?: string;
      }>).detail;
      if (!detail || detail.tableAssetId !== tableAssetId) return;
      appendLog(
        "status",
        `Approval received. Running repairs for ${detail.columnName}...`
      );
      void runSingleColumn(detail.columnName, "repairs", {
        data_fix_approved: true,
        data_fix_note: detail.note || "",
        ...(detail.planId ? { data_fix_plan_id: detail.planId } : {}),
        ...(detail.planHash ? { data_fix_plan_hash: detail.planHash } : {}),
        ...(detail.snapshotSignature
          ? { data_fix_snapshot_signature: detail.snapshotSignature }
          : {}),
      });
    };
    if (typeof window !== "undefined") {
      window.addEventListener("column-workflow-approval", handler as EventListener);
    }
    return () => {
      if (typeof window !== "undefined") {
        window.removeEventListener("column-workflow-approval", handler as EventListener);
      }
    };
  }, [appendLog, runSingleColumn, tableAssetId]);

  const handleCancelEstimate = useCallback(() => {
    setEstimateResults(null);
    setEstimateTargets([]);
  }, []);

  const estimateTotals = useMemo(() => {
    if (!estimateResults) return { totalTokens: 0, columns: 0 };
    return {
      totalTokens: estimateResults.reduce(
        (sum, item) => sum + (item.total_tokens || 0),
        0
      ),
      columns: estimateResults.length,
    };
  }, [estimateResults]);

  if (loading) {
    return (
      <div className="flex items-center justify-center h-full text-sm text-muted-foreground">
        Loading column metadata...
      </div>
    );
  }

  if (workflowList.length === 0) {
    return (
      <div className="flex items-center justify-center h-full text-sm text-muted-foreground">
        No column metadata available.
      </div>
    );
  }

  return (
    <div className="flex h-full min-h-0 gap-4">
      <div className="w-72 shrink-0 rounded-lg border border-border bg-card flex flex-col min-h-0">
        <div className="border-b border-border p-4 space-y-3">
          <div className="flex items-center justify-between">
            <div className="text-sm font-semibold">Columns</div>
            <Button
              variant="ghost"
              size="sm"
              className="h-7 px-2 text-xs"
              onClick={handleClearSelection}
              disabled={selectedColumns.length === 0}
            >
              Clear
            </Button>
          </div>
          <Input
            value={filter}
            onChange={(event) => setFilter(event.target.value)}
            placeholder="Search column or type"
          />
          <div className="text-xs text-muted-foreground">
            Use Add Board to move unselected columns into a new board.
          </div>
        </div>
        <ScrollArea className="flex-1">
          <div className="p-3 space-y-2">
            {groupedColumns.length === 0 && (
              <div className="rounded-lg border border-dashed border-border px-3 py-4 text-xs text-muted-foreground text-center">
                No columns on this board.
              </div>
            )}
            {groupedColumns.map((group) => {
              const expanded = isFiltering
                ? true
                : expandedGroups[group.type] ?? defaultGroupsExpanded;
              return (
                <div key={group.type} className="space-y-2">
                  <button
                    type="button"
                    className="flex w-full items-center justify-between rounded-md border border-border bg-muted/40 px-2.5 py-1.5 text-xs font-semibold uppercase tracking-wide text-muted-foreground"
                    onClick={() =>
                      setExpandedGroups((prev) => ({
                        ...prev,
                        [group.type]: !(prev[group.type] ?? true),
                      }))
                    }
                    disabled={isFiltering}
                  >
                    <span>{group.label}</span>
                    <span>{group.columns.length}</span>
                  </button>
                  {expanded && (
                    <div className="space-y-2">
                      {group.columns.map((column) => {
                        const checked = selectedColumns.includes(
                          column.column_name
                        );
                        const hasFeature =
                          column.semantic_type === "image" ||
                          Boolean(column.overrides?.row_level_instruction);
                        const nullRate = getNullRate(column);
                        return (
                          <div
                            key={column.column_name}
                            role="button"
                            tabIndex={0}
                            onClick={() =>
                              handleToggleColumn(column.column_name, !checked)
                            }
                            onKeyDown={(event) => {
                              if (event.key === "Enter" || event.key === " ") {
                                event.preventDefault();
                                handleToggleColumn(
                                  column.column_name,
                                  !checked
                                );
                              }
                            }}
                            className={cn(
                              "flex items-start gap-3 rounded-lg border border-transparent px-2 py-2 hover:bg-muted/50 cursor-pointer select-none",
                              checked && "border-primary/40 bg-primary/5"
                            )}
                          >
                            <Checkbox
                              checked={checked}
                              onCheckedChange={(value) =>
                                handleToggleColumn(
                                  column.column_name,
                                  Boolean(value)
                                )
                              }
                              onClick={(event) => event.stopPropagation()}
                              className="mt-0.5"
                            />
                            <div className="flex-1 space-y-1">
                              <div className="text-sm font-medium text-foreground">
                                {column.column_name}
                              </div>
                              <div className="flex flex-wrap items-center gap-2 text-xs text-muted-foreground">
                                <Badge
                                  variant="secondary"
                                  className="text-[10px] uppercase tracking-wide"
                                >
                                  {column.semantic_type}
                                </Badge>
                                {typeof nullRate === "number" && (
                                  <Badge
                                    variant="outline"
                                    className="text-[10px]"
                                    title="Null rate for this column"
                                  >
                                    Nulls {Math.round(nullRate * 100)}%
                                  </Badge>
                                )}
                                {hasFeature && (
                                  <Badge
                                    variant="outline"
                                    className="text-[10px] border-emerald-200 text-emerald-700"
                                  >
                                    Feature
                                  </Badge>
                                )}
                              </div>
                            </div>
                          </div>
                        );
                      })}
                    </div>
                  )}
                </div>
              );
            })}
          </div>
        </ScrollArea>
      </div>

      <div className="flex flex-1 flex-col min-h-0">
        <div className="flex items-center justify-between border-b border-border pb-2">
          <Tabs
            value={activeBoardIdValue ?? undefined}
            onValueChange={setActiveBoardId}
          >
            <TabsList>
              {boards.map((board) => (
                <TabsTrigger key={board.id} value={board.id}>
                  {board.name} ({board.columnNames.length})
                </TabsTrigger>
              ))}
            </TabsList>
          </Tabs>
          <div className="flex items-center gap-2">
            <Button size="sm" variant="surface" onClick={handleAddBoard}>
              Scrat!
            </Button>
            <Badge variant="outline" className="text-xs">
              {selectedColumns.length} selected
            </Badge>
          </div>
        </div>

        <div
          className={cn(
            "min-h-0 rounded-lg border border-border bg-muted/20 mt-3 overflow-hidden",
            isLogExpanded ? "flex-[0.7]" : "flex-1"
          )}
        >
          {activeBoard && activeBoard.columnNames.length > 0 ? (
            <div className="relative h-full">
              <EDAWorkflowEditor
                nodes={boardGraph.nodes}
                edges={boardGraph.edges}
                selectedNodeIds={selectedNodeIds}
                onWorkflowDataChange={updateWorkflowData}
                onSelectionChange={handleCanvasSelection}
                onRun={handleEstimateSelected}
                runLabel={
                  selectionMode === "canvas" && canvasSelectionIds.length > 0
                    ? "Run Selected Nodes"
                    : "Estimate & Run Selected"
                }
                runDisabled={
                  isRunningAny ||
                  isEstimating ||
                  (selectionMode === "canvas"
                    ? canvasSelectionIds.length === 0
                    : selectedColumns.length === 0)
                }
                isRunning={isRunningAny || isEstimating}
                className="h-full"
              />
            </div>
          ) : (
            <div className="flex h-full items-center justify-center text-sm text-muted-foreground">
              Add columns to this board to build workflows.
            </div>
          )}
        </div>

        <WorkflowLogPanel
          logs={logs}
          isRunning={isRunningAny || isEstimating}
          className="mt-4"
          useFlexLayout={true}
          onExpandedChange={setIsLogExpanded}
        />
      </div>

      <Dialog
        open={!!estimateResults}
        onOpenChange={(open) => !open && handleCancelEstimate()}
      >
        <DialogContent className="bg-slate-950 text-slate-100 border-slate-800">
          <DialogHeader className="text-slate-100">
            <DialogTitle className="text-slate-100">Token Estimate</DialogTitle>
          </DialogHeader>
          {estimateResults && (
            <div className="space-y-4 text-sm text-slate-100">
              <div className="flex items-center justify-between rounded-lg border border-slate-800 px-3 py-2">
                <span className="text-slate-300">Columns</span>
                <span className="font-medium">{estimateTotals.columns}</span>
              </div>
              <div className="flex items-center justify-between rounded-lg border border-slate-800 px-3 py-2">
                <span className="text-slate-300">Total tokens</span>
                <span className="font-medium">
                  {estimateTotals.totalTokens}
                </span>
              </div>
              <div className="space-y-3">
                {estimateResults.map((estimate) => (
                  <div
                    key={estimate.column}
                    className="rounded-lg border border-slate-800 px-3 py-2"
                  >
                    <div className="flex items-center justify-between">
                      <span className="font-medium text-slate-100">
                        {estimate.column}
                      </span>
                      <span className="text-xs text-slate-300">
                        {estimate.total_tokens} tokens
                      </span>
                    </div>
                    <div className="mt-2 space-y-1">
                      {estimate.estimates.map((item, idx) => (
                        <div
                          key={`${estimate.column}-${item.task}-${idx}`}
                          className="flex items-center justify-between"
                        >
                          <span className="text-xs text-slate-300">
                            {item.task}
                          </span>
                          <span className="text-xs text-slate-300">
                            {item.row_count ? `${item.row_count} rows · ` : ""}
                            {item.token_count} tokens
                          </span>
                        </div>
                      ))}
                    </div>
                  </div>
                ))}
              </div>
            </div>
          )}
          <DialogFooter>
            <Button
              variant="outline"
              onClick={handleCancelEstimate}
              className="border-slate-700 text-slate-100 hover:bg-slate-900"
            >
              Cancel
            </Button>
            <Button
              onClick={handleRunSelected}
              className="bg-slate-100 text-slate-900 hover:bg-slate-200"
            >
              Run Workflows
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
};

export default ColumnWorkflowPanel;
