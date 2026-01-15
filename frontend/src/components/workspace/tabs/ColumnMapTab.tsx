import { useCallback, useEffect, useMemo } from "react";
import {
  ReactFlow,
  Background,
  Controls,
  MiniMap,
  Node,
  Edge,
  useNodesState,
  useEdgesState,
  Handle,
  Position,
  ConnectionMode,
} from "@xyflow/react";
import "@xyflow/react/dist/style.css";
import { TableResult, ColumnInfo, ChartArtifact } from "@/types";
import { useTableStore } from "@/store/tableStore";
import { cn } from "@/lib/utils";
import { Hash, Type, Calendar, ToggleLeft, Key, TrendingUp, Layers, Link2, Network } from "lucide-react";

interface ColumnMapTabProps {
  tableId: string;
  tableResult?: TableResult;
}

// Get icon for column role/type
const getRoleIcon = (role?: string) => {
  switch (role) {
    case "id": return Key;
    case "time": return Calendar;
    case "metric": return TrendingUp;
    case "dimension": return Layers;
    case "foreign_key": return Link2;
    default: return Hash;
  }
};

const getRoleColor = (role?: string) => {
  switch (role) {
    case "id": return "border-viz-purple bg-viz-purple/15 text-viz-purple";
    case "time": return "border-viz-blue bg-viz-blue/15 text-viz-blue";
    case "metric": return "border-viz-green bg-viz-green/15 text-viz-green";
    case "dimension": return "border-viz-cyan bg-viz-cyan/15 text-viz-cyan";
    case "foreign_key": return "border-viz-orange bg-viz-orange/15 text-viz-orange";
    default: return "border-viz-indigo bg-viz-indigo/15 text-viz-indigo";
  }
};

// Custom Column Node
const ColumnNode = ({ data }: { data: { column: ColumnInfo; stats: any; isSelected: boolean; onClick: () => void } }) => {
  const Icon = getRoleIcon(data.column.role);
  const colorClass = getRoleColor(data.column.role);
  const importance = Math.min(100, (data.stats.unique / data.stats.total) * 100 + (data.stats.nullCount === 0 ? 20 : 0));
  
  return (
    <div
      onClick={data.onClick}
      className={cn(
        "relative px-4 py-3 rounded-xl border-2 cursor-pointer transition-all duration-300 min-w-[140px]",
        colorClass,
        data.isSelected && "ring-2 ring-primary ring-offset-2 ring-offset-background scale-110"
      )}
      style={{
        transform: `scale(${0.9 + importance * 0.003})`,
      }}
    >
      <Handle type="target" position={Position.Left} className="!bg-primary !w-2 !h-2" />
      <Handle type="source" position={Position.Right} className="!bg-primary !w-2 !h-2" />
      
      <div className="flex items-center gap-2">
        <Icon className="h-4 w-4" />
        <span className="font-semibold text-sm">{data.column.name}</span>
      </div>
      
      <div className="mt-1 flex items-center gap-2 text-xs opacity-70">
        <span className="uppercase tracking-wider">{data.column.role || "unknown"}</span>
        <span>•</span>
        <span>{data.stats.unique} unique</span>
      </div>

      {/* Importance indicator */}
      <div className="absolute -bottom-1 left-1/2 -translate-x-1/2 w-8 h-1 rounded-full bg-background overflow-hidden">
        <div 
          className="h-full bg-current opacity-50 rounded-full" 
          style={{ width: `${importance}%` }}
        />
      </div>
    </div>
  );
};

const nodeTypes = { columnNode: ColumnNode };

const buildChartEdges = (columns: ColumnInfo[], charts: ChartArtifact[]): Edge[] => {
  const columnSet = new Set(columns.map((column) => column.name));
  const edgeCounts = new Map<string, { source: string; target: string; count: number }>();

  charts.forEach((chart) => {
    const sourceColumns = Array.isArray(chart.content.sourceColumns) ? chart.content.sourceColumns : [];
    const uniqueColumns = Array.from(new Set(sourceColumns.filter((column) => columnSet.has(column))));
    if (uniqueColumns.length !== 2) return;
    const [source, target] = uniqueColumns;
    const key = source < target ? `${source}__${target}` : `${target}__${source}`;
    const existing = edgeCounts.get(key);
    if (existing) {
      existing.count += 1;
    } else {
      edgeCounts.set(key, { source, target, count: 1 });
    }
  });

  return Array.from(edgeCounts.values()).map((edge) => {
    const strength = Math.min(1, 0.4 + edge.count * 0.2);
    return {
      id: `edge_${edge.source}_${edge.target}`,
      source: edge.source,
      target: edge.target,
      animated: edge.count > 1,
      style: {
        strokeWidth: Math.max(1, strength * 3),
        stroke: `hsl(var(--primary) / ${strength * 0.6})`,
      },
      type: "default",
    };
  });
};

const ColumnMapTab = ({ tableId, tableResult }: ColumnMapTabProps) => {
  const { selectedColumn, setSelectedColumn } = useTableStore();
  const getArtifactsByTable = useTableStore((state) => state.getArtifactsByTable);
  const chartArtifacts = useMemo(() => {
    if (!tableId) return [];
    return getArtifactsByTable(tableId).filter((artifact) => artifact.type === "chart") as ChartArtifact[];
  }, [getArtifactsByTable, tableId]);

  // Calculate stats for each column
  const getColumnStats = useCallback((colName: string) => {
    if (!tableResult) return { unique: 0, nullCount: 0, total: 0 };
    const values = tableResult.rows.map((row) => row[colName]);
    const nonNull = values.filter((v) => v !== null && v !== undefined);
    return { 
      unique: new Set(nonNull).size, 
      nullCount: values.length - nonNull.length,
      total: values.length 
    };
  }, [tableResult]);

  // Create nodes from columns
  const initialNodes: Node[] = useMemo(() => {
    if (!tableResult) return [];
    
    const centerX = 400;
    const centerY = 300;
    const radius = 250;
    
    return tableResult.columns.map((col, index) => {
      const angle = (index / tableResult.columns.length) * 2 * Math.PI - Math.PI / 2;
      const x = centerX + radius * Math.cos(angle);
      const y = centerY + radius * Math.sin(angle);
      
      return {
        id: col.name,
        type: "columnNode",
        position: { x, y },
        data: {
          column: col,
          stats: getColumnStats(col.name),
          isSelected: selectedColumn === col.name,
          onClick: () => setSelectedColumn(selectedColumn === col.name ? null : col.name),
        },
      };
    });
  }, [tableResult, getColumnStats, selectedColumn, setSelectedColumn]);

  // Create edges from co-occurrence
  const initialEdges: Edge[] = useMemo(() => {
    if (!tableResult) return [];
    return buildChartEdges(tableResult.columns, chartArtifacts);
  }, [chartArtifacts, tableResult]);

  const [nodes, setNodes, onNodesChange] = useNodesState(initialNodes);
  const [edges, setEdges, onEdgesChange] = useEdgesState(initialEdges);

  useEffect(() => {
    setNodes(initialNodes);
  }, [initialNodes, setNodes]);

  useEffect(() => {
    setEdges(initialEdges);
  }, [initialEdges, setEdges]);

  if (!tableResult) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="text-center text-muted-foreground">
          <Network className="h-12 w-12 mx-auto mb-3 opacity-50" />
          <p>No data available</p>
        </div>
      </div>
    );
  }

  return (
    <div className="space-y-4 animate-fade-in">
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-lg font-semibold mb-1 flex items-center gap-2">
            <Network className="h-5 w-5 text-primary" />
            Column Map
          </h2>
          <p className="text-sm text-muted-foreground">
            Visualize column relationships and dependencies
          </p>
        </div>
        
        {/* Legend */}
        <div className="flex items-center gap-3 text-xs">
          <div className="flex items-center gap-1.5">
            <div className="w-3 h-3 rounded-full bg-viz-purple" />
            <span>ID</span>
          </div>
          <div className="flex items-center gap-1.5">
            <div className="w-3 h-3 rounded-full bg-viz-blue" />
            <span>Time</span>
          </div>
          <div className="flex items-center gap-1.5">
            <div className="w-3 h-3 rounded-full bg-viz-green" />
            <span>Metric</span>
          </div>
          <div className="flex items-center gap-1.5">
            <div className="w-3 h-3 rounded-full bg-viz-cyan" />
            <span>Dimension</span>
          </div>
        </div>
      </div>

      <div className="h-[600px] rounded-xl border border-border bg-card overflow-hidden">
        <ReactFlow
          nodes={nodes}
          edges={edges}
          onNodesChange={onNodesChange}
          onEdgesChange={onEdgesChange}
          nodeTypes={nodeTypes}
          connectionMode={ConnectionMode.Loose}
          fitView
          minZoom={0.5}
          maxZoom={2}
          className="bg-background"
        >
          <Background color="hsl(var(--muted-foreground) / 0.1)" gap={20} />
          <Controls className="!bg-card !border-border !shadow-lg [&_button]:!bg-card [&_button]:!border-border [&_button]:!text-foreground [&_button:hover]:!bg-secondary [&_svg]:!fill-foreground" />
          <MiniMap 
            className="!bg-card !border-border" 
            nodeColor="hsl(var(--primary))"
            maskColor="hsl(var(--background) / 0.8)"
          />
        </ReactFlow>
      </div>

      {/* Selected Column Details */}
      {selectedColumn && (
        <div className="p-4 rounded-xl border border-primary/30 bg-primary/5 animate-fade-in">
          <div className="flex items-center gap-2 mb-2">
            <Key className="h-4 w-4 text-primary" />
            <span className="font-semibold">{selectedColumn}</span>
          </div>
          <p className="text-sm text-muted-foreground">
            Click on the column node to see details in the AI Actions panel, or use "Explain Column" for AI-powered insights.
          </p>
        </div>
      )}
    </div>
  );
};

export default ColumnMapTab;
