import { useMemo, useState, useCallback, useEffect, useRef } from "react";
import { BarChart3 } from "lucide-react";
import { useTableStore } from "@/store/tableStore";
import { ChartSpec } from "@/lib/chartUtils";
import ChartCard from "@/components/charts/ChartCard";
import GridLayout from "react-grid-layout";
import "react-grid-layout/css/styles.css";
import "react-resizable/css/styles.css";

interface LayoutItem {
  i: string;
  x: number;
  y: number;
  w: number;
  h: number;
  minW?: number;
  minH?: number;
}

interface ChartsTabProps {
  tableId: string;
}

const RGL = GridLayout as any;

// Normalize layout item values to integers to avoid floating-point issues
const normalizeLayout = (items: LayoutItem[]): LayoutItem[] =>
  items.map((item) => ({
    i: item.i,
    x: Math.round(item.x),
    y: Math.round(item.y),
    w: Math.round(item.w),
    h: Math.round(item.h),
    minW: item.minW,
    minH: item.minH,
  }));

const ChartsTab = ({ tableId }: ChartsTabProps) => {
  const artifacts = useTableStore((s) => s.artifacts);
  const deleteArtifact = useTableStore((s) => s.deleteArtifact);
  const toggleArtifactPin = useTableStore((s) => s.toggleArtifactPin);

  const [layout, setLayout] = useState<LayoutItem[]>([]);
  const containerRef = useRef<HTMLDivElement>(null);
  const [containerWidth, setContainerWidth] = useState(800);

  const chartArtifacts = useMemo(
    () => artifacts.filter((a) => a.tableId === tableId && a.type === "chart"),
    [artifacts, tableId]
  );

  // Convert artifacts to ChartSpec format
  const chartSpecs: ChartSpec[] = useMemo(
    () =>
      chartArtifacts.map((artifact) => {
        if (artifact.type !== "chart") return null;
        return {
          id: artifact.id,
          chartType: artifact.content.chartType || "bar",
          title: artifact.content.title || "Untitled",
          xKey: artifact.content.xKey || "x",
          yKey: artifact.content.yKey || "y",
          xTitle: artifact.content.xTitle,
          yTitle: artifact.content.yTitle,
          yScale: artifact.content.yScale,
          data: artifact.content.data || [],
          narrative: artifact.content.narrative || [],
          sourceColumns: artifact.content.sourceColumns || [],
        };
      }).filter(Boolean) as ChartSpec[],
    [chartArtifacts]
  );

  // Use chart IDs as stable dependency to avoid re-running on content changes
  const chartIdsKey = useMemo(
    () => chartSpecs.map((s) => s.id).join("|"),
    [chartSpecs]
  );

  // Generate initial layout for new charts - only when chart IDs change
  useEffect(() => {
    const ids = chartIdsKey.split("|").filter(Boolean);
    
    if (ids.length === 0) {
      setLayout((prev) => (prev.length === 0 ? prev : []));
      return;
    }

    setLayout((prevLayout) => {
      const existingById = new Map(prevLayout.map((l) => [l.i, l] as const));
      const currentIds = new Set(ids);

      // Keep existing layout for still-present charts
      const keptLayout = prevLayout.filter((l) => currentIds.has(l.i));

      // Add layout for new charts (default: 2 per row with 30 cols => 15+15)
      const newItems: LayoutItem[] = [];
      let newIndex = keptLayout.length;
      ids.forEach((id) => {
        if (!existingById.has(id)) {
          const col = newIndex % 2; // 2 charts per row
          const row = Math.floor(newIndex / 2);
          newItems.push({
            i: id,
            x: col * 15,
            y: row * 3,
            w: 6,
            h: 3,
            minW: 3,
            minH: 2,
          });
          newIndex++;
        }
      });

      if (newItems.length === 0 && keptLayout.length === prevLayout.length) {
        return prevLayout; // No changes needed
      }

      return [...keptLayout, ...newItems];
    });
  }, [chartIdsKey]);

  // Track container width for responsive layout
  useEffect(() => {
    const updateWidth = () => {
      if (containerRef.current) {
        setContainerWidth(containerRef.current.clientWidth);
      }
    };

    updateWidth();
    window.addEventListener('resize', updateWidth);
    
    const resizeObserver = new ResizeObserver(updateWidth);
    if (containerRef.current) {
      resizeObserver.observe(containerRef.current);
    }

    return () => {
      window.removeEventListener('resize', updateWidth);
      resizeObserver.disconnect();
    };
  }, []);

  // Handle layout changes only on drag/resize end to avoid infinite loops
  const handleDragStop = useCallback((_layout: LayoutItem[], _oldItem: LayoutItem, newItem: LayoutItem) => {
    setLayout((prev) => {
      const normalized = normalizeLayout(prev.map((item) =>
        item.i === newItem.i ? { ...item, x: newItem.x, y: newItem.y } : item
      ));
      return normalized;
    });
  }, []);

  const handleResizeStop = useCallback((_layout: LayoutItem[], _oldItem: LayoutItem, newItem: LayoutItem) => {
    setLayout((prev) => {
      const normalized = normalizeLayout(prev.map((item) =>
        item.i === newItem.i ? { ...item, w: newItem.w, h: newItem.h, x: newItem.x, y: newItem.y } : item
      ));
      return normalized;
    });
  }, []);

  return (
    <div className="space-y-4">
      <div>
        <h2 className="text-lg font-semibold">Charts</h2>
        <p className="text-sm text-muted-foreground">
          {chartSpecs.length} chart{chartSpecs.length !== 1 && "s"} - Drag to reorder, resize from corners
        </p>
      </div>

      {chartSpecs.length === 0 ? (
        <div className="flex items-center justify-center h-64 text-muted-foreground border border-dashed border-border rounded-lg">
          <div className="text-center">
            <BarChart3 className="h-12 w-12 mx-auto mb-3 opacity-50" />
            <p className="mb-2">No charts yet</p>
            <p className="text-sm">Save charts from Workflow Outputs to add them to the report</p>
          </div>
        </div>
      ) : (
        <div ref={containerRef} className="chart-grid-container">
          <RGL
            className="layout"
            layout={layout}
            cols={30}
            rowHeight={8}
            width={containerWidth || 800}
            onDragStop={handleDragStop}
            onResizeStop={handleResizeStop}
            draggableHandle=".chart-drag-handle"
            isResizable={true}
            isDraggable={true}
            compactType="horizontal"
            preventCollision={false}
            margin={[16, 16]}
            containerPadding={[0, 0]}
            useCSSTransforms={true}
          >
            {chartSpecs.map((spec) => {
              const artifact = chartArtifacts.find((a) => a.id === spec.id);
              return (
                <div key={spec.id} style={{ width: '100%', height: '100%' }}>
                  <ChartCard
                    spec={spec}
                    isPinned={artifact?.pinned}
                    onPin={() => toggleArtifactPin(spec.id)}
                    onDelete={() => deleteArtifact(spec.id)}
                  />
                </div>
              );
            })}
          </RGL>
        </div>
      )}
    </div>
  );
};

export default ChartsTab;
