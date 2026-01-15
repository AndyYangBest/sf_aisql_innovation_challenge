/**
 * ColumnWorkflowPanel - renders multi-column Flowgram workflows with selection.
 */

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { columnMetadataApi, ColumnMetadataRecord } from "@/api/columnMetadata";
import {
  columnWorkflowsApi,
  ColumnWorkflowEstimate,
} from "@/api/columnWorkflows";
import { EDAWorkflowEditor } from "@/components/workflow/EDAWorkflowEditor";
import { WorkflowLogPanel } from "@/components/workflow/WorkflowLogPanel";
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
const FEATURE_LANE_OFFSET = 170;
const WORKFLOW_SPAN_Y = 360;

function normalizeId(value: string) {
  return value.replace(/[^a-z0-9]/gi, "_").toLowerCase();
}

function collectOverrides(nodes: WorkflowNode[]) {
  const overrides: Record<string, any> = {};
  const hintNode = nodes.find((node) => node.type === "column_hint");
  if (hintNode) {
    overrides.hint = hintNode.data?.hint ?? "";
  }
  const rowLevelNode = nodes.find((node) => node.type === "row_level_extract");
  if (rowLevelNode) {
    overrides.row_level_instruction = rowLevelNode.data?.instruction ?? "";
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

function coercePosition(position?: { x?: number; y?: number }) {
  return {
    x: typeof position?.x === "number" ? position.x : 0,
    y: typeof position?.y === "number" ? position.y : 0,
  };
}

function sanitizeWorkflowGraph(
  graph: WorkflowGraphPayload
): WorkflowGraphPayload {
  return {
    nodes: graph.nodes.map((node) => ({
      ...node,
      position: coercePosition(node.position),
      data: {
        ...node.data,
        status: node.data?.status ?? "idle",
      },
    })),
    edges: graph.edges.map((edge) => ({
      sourceNodeID: edge.sourceNodeID,
      targetNodeID: edge.targetNodeID,
    })),
  };
}

function buildColumnWorkflowGraph(
  column: ColumnMetadataRecord,
  tableAssetId: number,
  tableName: string,
  layoutIndex: number
) {
  const nodes: WorkflowNode[] = [];
  const edges: WorkflowEdge[] = [];
  const colId = normalizeId(column.column_name);
  const basePosition = getWorkflowBasePosition(layoutIndex);
  let analysisX = basePosition.x;
  let featureX = basePosition.x;
  const analysisY = basePosition.y;
  const featureY = basePosition.y + FEATURE_LANE_OFFSET;

  const addNode = (
    type: EDANodeType,
    data?: Record<string, any>,
    lane: "analysis" | "feature" = "analysis"
  ) => {
    const definition = EDA_NODE_DEFINITIONS[type];
    const nodeId = `${type}_${colId}`;
    const positionX = lane === "feature" ? featureX : analysisX;
    const positionY = lane === "feature" ? featureY : analysisY;
    nodes.push({
      id: nodeId,
      type,
      position: { x: positionX, y: positionY },
      data: {
        ...definition?.defaultData,
        ...(data || {}),
        title: data?.title || definition?.name || type,
        status: "idle",
        column_type: column.semantic_type,
        column_confidence: column.confidence,
        column_name: column.column_name,
        table_asset_id: tableAssetId,
      },
    });
    if (lane === "feature") {
      featureX += COLUMN_NODE_WIDTH;
    } else {
      analysisX += COLUMN_NODE_WIDTH;
    }
    return nodeId;
  };

  const dataNodeId = addNode("data_source", {
    table_asset_id: tableAssetId,
    table_name: tableName,
    column_name: column.column_name,
  });

  let previousId = dataNodeId;

  if (column.confidence < 0.6) {
    const hintNodeId = addNode("column_hint", {
      hint: column.overrides?.hint || "",
    });
    edges.push({ sourceNodeID: previousId, targetNodeID: hintNodeId });
    previousId = hintNodeId;
  }

  const semantic = column.semantic_type;
  if (semantic === "numeric" || semantic === "temporal") {
    const visualsNode = addNode("generate_visuals");
    edges.push({ sourceNodeID: previousId, targetNodeID: visualsNode });
    const insightsNode = addNode("generate_insights", { focus: "column" });
    edges.push({ sourceNodeID: visualsNode, targetNodeID: insightsNode });
  } else if (semantic === "categorical") {
    const visualsNode = addNode("generate_visuals");
    edges.push({ sourceNodeID: previousId, targetNodeID: visualsNode });
    const insightsNode = addNode("generate_insights", { focus: "column" });
    edges.push({ sourceNodeID: visualsNode, targetNodeID: insightsNode });
  } else if (semantic === "text") {
    const summaryNode = addNode("summarize_text");
    edges.push({ sourceNodeID: previousId, targetNodeID: summaryNode });
    if (column.overrides?.row_level_instruction) {
      const extractNode = addNode(
        "row_level_extract",
        {
          instruction: column.overrides?.row_level_instruction,
          output_column: column.overrides?.row_level_output_column,
          response_schema: column.overrides?.row_level_schema,
        },
        "feature"
      );
      edges.push({ sourceNodeID: summaryNode, targetNodeID: extractNode });
    }
  } else if (semantic === "image") {
    const imageNode = addNode(
      "describe_images",
      {
        output_column: column.overrides?.image_output_column,
        image_stage: column.overrides?.image_stage,
        image_path_prefix: column.overrides?.image_path_prefix,
        image_path_suffix: column.overrides?.image_path_suffix,
        image_model: column.overrides?.image_model,
      },
      "feature"
    );
    edges.push({ sourceNodeID: previousId, targetNodeID: imageNode });
  } else {
    const statsNode = addNode("basic_stats");
    edges.push({ sourceNodeID: previousId, targetNodeID: statsNode });
  }

  return { nodes, edges };
}

function hydrateColumnWorkflowGraph(
  column: ColumnMetadataRecord,
  tableAssetId: number,
  tableName: string,
  layoutIndex: number
): WorkflowGraphPayload {
  const baseGraph = buildColumnWorkflowGraph(
    column,
    tableAssetId,
    tableName,
    layoutIndex
  );
  const storedGraph = (column.overrides as any)?.workflow_graph as
    | WorkflowGraphPayload
    | undefined;
  if (
    !storedGraph ||
    !Array.isArray(storedGraph.nodes) ||
    !Array.isArray(storedGraph.edges)
  ) {
    return baseGraph;
  }

  const baseById = new Map(baseGraph.nodes.map((node) => [node.id, node]));
  const storedById = new Map(storedGraph.nodes.map((node) => [node.id, node]));
  const mergedNodes: WorkflowNode[] = baseGraph.nodes.map((node) => {
    const storedNode = storedById.get(node.id);
    const definition = EDA_NODE_DEFINITIONS[node.type];
    const data = {
      ...definition?.defaultData,
      ...node.data,
      ...(storedNode?.data ?? {}),
      title:
        storedNode?.data?.title ||
        node.data.title ||
        definition?.name ||
        node.type,
      status: storedNode?.data?.status || node.data.status || "idle",
      column_name: column.column_name,
      column_type: column.semantic_type,
      column_confidence: column.confidence,
      table_asset_id: tableAssetId,
      ...(node.type === "data_source"
        ? {
            table_name: tableName,
            column_name: column.column_name,
            table_asset_id: tableAssetId,
          }
        : {}),
    };
    return {
      ...node,
      position: storedNode?.position ?? node.position,
      data,
    };
  });

  const extraNodes: WorkflowNode[] = storedGraph.nodes
    .filter((node) => !baseById.has(node.id))
    .map((node) => {
      const definition = EDA_NODE_DEFINITIONS[node.type as EDANodeType];
      return {
        id: node.id,
        type: node.type as EDANodeType,
        position: coercePosition(node.position),
        data: {
          ...definition?.defaultData,
          ...node.data,
          title: node.data?.title || definition?.name || node.type,
          status: node.data?.status || "idle",
          column_name: column.column_name,
          column_type: column.semantic_type,
          column_confidence: column.confidence,
          table_asset_id: tableAssetId,
        },
      };
    });

  const nodeIds = new Set(
    [...mergedNodes, ...extraNodes].map((node) => node.id)
  );
  const mergedEdges = storedGraph.edges.filter(
    (edge) => nodeIds.has(edge.sourceNodeID) && nodeIds.has(edge.targetNodeID)
  );

  return {
    nodes: [...mergedNodes, ...extraNodes],
    edges: mergedEdges.length > 0 ? mergedEdges : baseGraph.edges,
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
  const selectionSourceRef = useRef<"list" | "canvas" | null>(null);
  const selectionResetRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const workflowPersistTimerRef = useRef<ReturnType<typeof setTimeout> | null>(
    null
  );
  const pendingWorkflowPersistRef = useRef<
    Record<string, WorkflowGraphPayload>
  >({});

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

  const persistWorkflowGraphs = useCallback(async () => {
    const pending = { ...pendingWorkflowPersistRef.current };
    pendingWorkflowPersistRef.current = {};
    const entries = Object.entries(pending);
    if (entries.length === 0) return;

    const results = await Promise.allSettled(
      entries.map(async ([columnName, graph]) => {
        const sanitized = sanitizeWorkflowGraph(graph);
        await columnMetadataApi.override(tableAssetId, columnName, {
          workflow_graph: sanitized,
        });
      })
    );

    results.forEach((result, index) => {
      if (result.status === "rejected") {
        const [columnName] = entries[index];
        appendLog("error", "Failed to save workflow layout", {
          column: columnName,
          error: result.reason,
        });
      }
    });
  }, [appendLog, tableAssetId]);

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
          const existing = prev[column.column_name];
          if (existing) {
            const nodes = existing.nodes.map((node) => ({
              ...node,
              data: {
                ...node.data,
                column_name: column.column_name,
                table_asset_id: tableAssetId,
                ...(node.type === "data_source"
                  ? {
                      table_name: tableName,
                      column_name: column.column_name,
                      table_asset_id: tableAssetId,
                    }
                  : {}),
              },
            }));
            nextWorkflows[column.column_name] = {
              ...existing,
              column,
              nodes,
              edges: existing.edges,
            };
            return;
          }
          const graph = hydrateColumnWorkflowGraph(
            column,
            tableAssetId,
            tableName,
            index
          );
          nextWorkflows[column.column_name] = {
            column,
            nodes: graph.nodes,
            edges: graph.edges,
            isRunning: false,
          };
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
          if (
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
            activeBoardId: persistedActiveBoardId,
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
        const targetId = activeBoardIdRef.current || nextBoards[0]?.id;
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
      "id",
      "unknown",
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
    const groups = order
      .filter((key) => map.has(key))
      .map((key) => ({
        type: key,
        label: labels[key] ?? key,
        columns: map.get(key) ?? [],
      }));
    const extraGroups = Array.from(map.keys())
      .filter((key) => !order.includes(key))
      .map((key) => ({
        type: key,
        label: labels[key] ?? key,
        columns: map.get(key) ?? [],
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

  const selectedNodeIds = useMemo(() => {
    if (!activeBoard) return [];
    const ids: string[] = [];
    activeBoard.selectedColumns.forEach((columnName) => {
      const workflow = workflows[columnName];
      if (!workflow) return;
      workflow.nodes.forEach((node) => ids.push(node.id));
    });
    return ids;
  }, [activeBoard, workflows]);

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

      if (nodesByColumn.size > 0) {
        nodesByColumn.forEach((nodes, columnName) => {
          pendingWorkflowPersistRef.current[columnName] = {
            nodes,
            edges: edgesByColumn.get(columnName) ?? [],
          };
        });
        if (workflowPersistTimerRef.current) {
          clearTimeout(workflowPersistTimerRef.current);
        }
        workflowPersistTimerRef.current = setTimeout(() => {
          void persistWorkflowGraphs();
        }, 800);
      }
    },
    [activeBoard, persistWorkflowGraphs]
  );

  const handleCanvasSelection = useCallback(
    (nodeIds: string[]) => {
      if (!activeBoard) return;
      if (nodeIds.length === 0) {
        return;
      }
      if (selectionSourceRef.current === "list") {
        selectionSourceRef.current = null;
        return;
      }
      selectionSourceRef.current = "canvas";
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
    updateActiveBoard(
      (board) => ({ ...board, selectedColumns: [] }),
      activeBoard.id
    );
  }, [activeBoard, updateActiveBoard]);

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

  const handleEstimateSelected = useCallback(async () => {
    if (!activeBoard) return;
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
          if (Object.keys(overrides).length > 0) {
            await columnMetadataApi.override(
              tableAssetId,
              columnName,
              overrides
            );
          }
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
  }, [activeBoard, appendLog, selectedColumns, tableAssetId, toast, workflows]);

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
        appendLog(
          "complete",
          `Workflow completed for ${result.value.columnName}`,
          result.value.response.data
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
    estimateTargets,
    loadMetadata,
    loadReport,
    selectedColumns,
    tableAssetId,
  ]);

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
                                <Badge
                                  variant="outline"
                                  className="text-[10px]"
                                >
                                  {(column.confidence * 100).toFixed(0)}%
                                </Badge>
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
            <EDAWorkflowEditor
              nodes={boardGraph.nodes}
              edges={boardGraph.edges}
              isRunning={isRunningAny}
              selectedNodeIds={selectedNodeIds}
              onSelectionChange={handleCanvasSelection}
              onRun={handleEstimateSelected}
              runDisabled={
                isRunningAny || isEstimating || selectedColumns.length === 0
              }
              runLabel="Estimate & Run Selected"
              onWorkflowDataChange={(data) =>
                updateWorkflowData({
                  nodes: data.nodes,
                  edges: data.edges,
                })
              }
            />
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
