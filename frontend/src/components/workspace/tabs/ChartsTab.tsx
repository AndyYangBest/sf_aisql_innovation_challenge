import { useMemo, useState, useCallback, useEffect, useRef } from "react";
import {
  BarChart3,
  Lightbulb,
  GripVertical,
  MoreHorizontal,
  Pin,
  Trash2,
  MessageSquare,
  List,
  Clock,
} from "lucide-react";
import { useTableStore } from "@/store/tableStore";
import { ChartSpec } from "@/lib/chartUtils";
import ChartCard from "@/components/charts/ChartCard";
import { InsightArtifact } from "@/types";
import { Tabs, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
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

type DisplayCard =
  | {
      id: string;
      kind: "chart";
      sourceColumns: string[];
      chartSpec: ChartSpec;
      chartPinned?: boolean;
    }
  | {
      id: string;
      kind: "insight";
      sourceColumns: string[];
      insightArtifact: InsightArtifact;
    };

const RGL = GridLayout;
const CHART_MIN_W = 2;
const CHART_MIN_H = 1;
type ReportLayoutItem = {
  x: number;
  y: number;
  w: number;
  h: number;
  kind?: "chart" | "insight";
  artifactId?: string;
};

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

const parseCardIdentity = (cardId: string): { kind: "chart" | "insight" | undefined; artifactId: string } => {
  const [prefix, ...rest] = cardId.split(":");
  const artifactId = rest.join(":") || cardId;
  if (prefix === "chart" || prefix === "insight") {
    return { kind: prefix, artifactId };
  }
  return { kind: undefined, artifactId };
};

const mergeVisibleLayout = (previous: LayoutItem[], visible: LayoutItem[]): LayoutItem[] => {
  const visibleById = new Map(
    normalizeLayout(
      visible.map((item) => ({
        ...item,
        minW: CHART_MIN_W,
        minH: CHART_MIN_H,
      }))
    ).map((item) => [item.i, item] as const)
  );
  const merged = previous.map((item) => visibleById.get(item.i) || item);
  const existingIds = new Set(merged.map((item) => item.i));
  visibleById.forEach((item, id) => {
    if (!existingIds.has(id)) {
      merged.push(item);
    }
  });
  return normalizeLayout(merged);
};

const ChartsTab = ({ tableId }: ChartsTabProps) => {
  const artifacts = useTableStore((s) => s.artifacts);
  const deleteArtifact = useTableStore((s) => s.deleteArtifact);
  const toggleArtifactPin = useTableStore((s) => s.toggleArtifactPin);
  const setInsightDisplayInCharts = useTableStore((s) => s.setInsightDisplayInCharts);
  const updateReportLayout = useTableStore((s) => s.updateReportLayout);
  const reportLayout = useTableStore((s) => s.reportOverrides[tableId]?.layout);
  const tableResult = useTableStore((s) => s.getTableResult(tableId));
  const loadTableResult = useTableStore((s) => s.loadTableResult);

  const [layout, setLayout] = useState<LayoutItem[]>([]);
  const layoutRef = useRef<LayoutItem[]>([]);
  const containerRef = useRef<HTMLDivElement>(null);
  const [containerWidth, setContainerWidth] = useState(800);
  const [activeView, setActiveView] = useState("all");

  useEffect(() => {
    layoutRef.current = layout;
  }, [layout]);

  const persistedLayout = useMemo(() => reportLayout || {}, [reportLayout]);

  useEffect(() => {
    setActiveView("all");
  }, [tableId]);

  const chartArtifacts = useMemo(
    () => artifacts.filter((a) => a.tableId === tableId && a.type === "chart"),
    [artifacts, tableId]
  );

  const movedInsightArtifacts = useMemo(
    () =>
      artifacts.filter(
        (artifact): artifact is InsightArtifact =>
          artifact.tableId === tableId &&
          artifact.type === "insight" &&
          Boolean(artifact.content?.displayInCharts)
      ),
    [artifacts, tableId]
  );

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
          valueKey: artifact.content.valueKey,
          xTitle: artifact.content.xTitle,
          yTitle: artifact.content.yTitle,
          yScale: artifact.content.yScale,
          data: artifact.content.data || [],
          narrative: artifact.content.narrative || [],
          series: artifact.content.series,
          sourceColumns: artifact.content.sourceColumns || [],
          insight: artifact.content.insight,
          warnings: artifact.content.warnings,
        };
      }).filter(Boolean) as ChartSpec[],
    [chartArtifacts]
  );

  const displayCards: DisplayCard[] = useMemo(() => {
    const chartCards: DisplayCard[] = chartSpecs.map((spec) => {
      const chartArtifact = chartArtifacts.find((artifact) => artifact.id === spec.id);
      return {
        id: `chart:${spec.id}`,
        kind: "chart",
        sourceColumns: Array.isArray(spec.sourceColumns) ? spec.sourceColumns : [],
        chartSpec: spec,
        chartPinned: chartArtifact?.pinned,
      };
    });
    const insightCards: DisplayCard[] = movedInsightArtifacts.map((artifact) => ({
      id: `insight:${artifact.id}`,
      kind: "insight",
      sourceColumns: Array.isArray(artifact.content?.sourceColumns)
        ? artifact.content.sourceColumns
        : [],
      insightArtifact: artifact,
    }));
    return [...chartCards, ...insightCards];
  }, [chartArtifacts, chartSpecs, movedInsightArtifacts]);

  const cardIdsKey = useMemo(
    () => displayCards.map((card) => card.id).join("|"),
    [displayCards]
  );

  const cardKindById = useMemo(
    () => new Map(displayCards.map((card) => [card.id, card.kind] as const)),
    [displayCards]
  );

  const cardsByColumn = useMemo(() => {
    const counts = new Map<string, number>();
    displayCards.forEach((card) => {
      const uniqueColumns = Array.from(new Set(card.sourceColumns.filter(Boolean)));
      uniqueColumns.forEach((column) => {
        counts.set(column, (counts.get(column) || 0) + 1);
      });
    });
    return Array.from(counts.entries()).sort((left, right) => {
      if (right[1] !== left[1]) {
        return right[1] - left[1];
      }
      return left[0].localeCompare(right[0]);
    });
  }, [displayCards]);

  useEffect(() => {
    if (activeView === "all") {
      return;
    }
    const exists = cardsByColumn.some(([column]) => column === activeView);
    if (!exists) {
      setActiveView("all");
    }
  }, [activeView, cardsByColumn]);

  const visibleCards = useMemo(() => {
    if (activeView === "all") {
      return displayCards;
    }
    return displayCards.filter((card) => card.sourceColumns.includes(activeView));
  }, [activeView, displayCards]);

  const visibleCardIds = useMemo(
    () => new Set(visibleCards.map((card) => card.id)),
    [visibleCards]
  );

  const visibleLayout = useMemo(
    () => layout.filter((item) => visibleCardIds.has(item.i)),
    [layout, visibleCardIds]
  );

  useEffect(() => {
    const ids = cardIdsKey.split("|").filter(Boolean);

    if (ids.length === 0) {
      setLayout((prev) => (prev.length === 0 ? prev : []));
      return;
    }

    setLayout((prevLayout) => {
      const existingById = new Map(prevLayout.map((item) => [item.i, item] as const));
      const currentIds = new Set(ids);

      const keptLayout = prevLayout
        .filter((item) => currentIds.has(item.i))
        .map((item) => ({
          ...(persistedLayout[item.i] || item),
          minW: CHART_MIN_W,
          minH: CHART_MIN_H,
        }));

      const newItems: LayoutItem[] = [];
      let newIndex = keptLayout.length;
      ids.forEach((id) => {
        if (existingById.has(id)) {
          return;
        }
        const kind = cardKindById.get(id);
        const persisted = persistedLayout[id];
        const col = newIndex % 2;
        const row = Math.floor(newIndex / 2);
        const defaultW = kind === "insight" ? 8 : 6;
        const defaultH = kind === "insight" ? 5 : 3;
        newItems.push({
          i: id,
          x: persisted?.x ?? col * 15,
          y: persisted?.y ?? row * 5,
          w: persisted?.w ?? defaultW,
          h: persisted?.h ?? defaultH,
          minW: CHART_MIN_W,
          minH: CHART_MIN_H,
        });
        newIndex++;
      });

      if (newItems.length === 0 && keptLayout.length === prevLayout.length) {
        return prevLayout;
      }

      return normalizeLayout([...keptLayout, ...newItems]);
    });
  }, [cardIdsKey, cardKindById, persistedLayout]);

  useEffect(() => {
    const updateWidth = () => {
      if (containerRef.current) {
        setContainerWidth(containerRef.current.clientWidth);
      }
    };

    updateWidth();
    window.addEventListener("resize", updateWidth);

    const resizeObserver = new ResizeObserver(updateWidth);
    if (containerRef.current) {
      resizeObserver.observe(containerRef.current);
    }

    return () => {
      window.removeEventListener("resize", updateWidth);
      resizeObserver.disconnect();
    };
  }, []);

  const persistLayout = useCallback(
    (nextLayout: LayoutItem[]) => {
      const payload = nextLayout.reduce<Record<string, ReportLayoutItem>>((acc, item) => {
        const kindFromMap = cardKindById.get(item.i);
        const identity = parseCardIdentity(item.i);
        if (!kindFromMap && !identity.kind) {
          return acc;
        }
        acc[item.i] = {
          x: item.x,
          y: item.y,
          w: item.w,
          h: item.h,
          kind: kindFromMap || identity.kind,
          artifactId: identity.artifactId,
        };
        return acc;
      }, {});
      updateReportLayout(tableId, payload);
    },
    [cardKindById, tableId, updateReportLayout]
  );

  const syncVisibleLayout = useCallback(
    (nextVisibleLayout: LayoutItem[]) => {
      const mergedLayout = mergeVisibleLayout(layoutRef.current, nextVisibleLayout);
      layoutRef.current = mergedLayout;
      setLayout(mergedLayout);
      persistLayout(mergedLayout);
    },
    [persistLayout]
  );

  const handleDragStop = useCallback(
    (nextVisibleLayout: LayoutItem[]) => {
      syncVisibleLayout(nextVisibleLayout);
    },
    [syncVisibleLayout]
  );

  const handleResizeStop = useCallback(
    (nextVisibleLayout: LayoutItem[]) => {
      syncVisibleLayout(nextVisibleLayout);
    },
    [syncVisibleLayout]
  );

  return (
    <div className="space-y-4">
      <div>
        <h2 className="text-lg font-semibold">Insight Canvas</h2>
        <p className="text-sm text-muted-foreground">
          {visibleCards.length}
          {activeView !== "all" ? ` / ${displayCards.length}` : ""} card
          {visibleCards.length !== 1 && "s"} - Drag to reorder, resize from corners
        </p>
      </div>

      {cardsByColumn.length > 0 && (
        <Tabs value={activeView} onValueChange={setActiveView}>
          <TabsList className="h-auto w-full justify-start gap-1 overflow-x-auto whitespace-nowrap">
            <TabsTrigger value="all">All ({displayCards.length})</TabsTrigger>
            {cardsByColumn.map(([column, count]) => (
              <TabsTrigger key={column} value={column} title={column}>
                {column} ({count})
              </TabsTrigger>
            ))}
          </TabsList>
        </Tabs>
      )}

      {visibleCards.length === 0 ? (
        <div className="flex items-center justify-center h-64 text-muted-foreground border border-dashed border-border rounded-lg">
          <div className="text-center">
            <BarChart3 className="h-12 w-12 mx-auto mb-3 opacity-50" />
            {displayCards.length === 0 ? (
              <>
                <p className="mb-2">No canvas cards yet</p>
                <p className="text-sm">Charts and moved insight cards will appear in the Insight Canvas</p>
              </>
            ) : (
              <>
                <p className="mb-2">No cards for this column</p>
                <p className="text-sm">Switch tab to view another column’s cards</p>
              </>
            )}
          </div>
        </div>
      ) : (
        <div ref={containerRef} className="chart-grid-container">
          <RGL
            className="layout"
            layout={visibleLayout}
            cols={30}
            rowHeight={8}
            width={containerWidth || 800}
            onDragStop={handleDragStop}
            onResizeStop={handleResizeStop}
            draggableHandle=".chart-drag-handle"
            isResizable={true}
            isDraggable={true}
            compactType={null}
            preventCollision={false}
            margin={[16, 16]}
            containerPadding={[0, 0]}
            useCSSTransforms={true}
          >
            {visibleCards.map((card) => {
              if (card.kind === "chart") {
                return (
                  <div key={card.id} style={{ width: "100%", height: "100%" }}>
                    <ChartCard
                      spec={card.chartSpec}
                      tableId={tableId}
                      tableResult={tableResult}
                      onRequestTableResult={() => loadTableResult(tableId)}
                      isPinned={card.chartPinned}
                      onPin={() => toggleArtifactPin(card.chartSpec.id)}
                      onDelete={() => deleteArtifact(card.chartSpec.id)}
                    />
                  </div>
                );
              }

              const insightArtifact = card.insightArtifact;
              return (
                <div key={card.id} style={{ width: "100%", height: "100%" }}>
                  <InsightInChartsCard
                    artifact={insightArtifact}
                    onPin={() => toggleArtifactPin(insightArtifact.id)}
                    onDelete={() => deleteArtifact(insightArtifact.id)}
                    onMoveBack={() => setInsightDisplayInCharts(insightArtifact.id, false)}
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

interface InsightInChartsCardProps {
  artifact: InsightArtifact;
  onPin: () => void;
  onDelete: () => void;
  onMoveBack: () => void;
}

const InsightInChartsCard = ({
  artifact,
  onPin,
  onDelete,
  onMoveBack,
}: InsightInChartsCardProps) => {
  const stripBullet = (text: string) => text.replace(/^\s*[-•*]\s+/, "").trim();
  const bullets = Array.isArray(artifact.content?.bullets)
    ? artifact.content.bullets.map((bullet: string) => stripBullet(String(bullet))).filter(Boolean)
    : [];

  const dateText = new Date(artifact.createdAt).toLocaleDateString("en-US", {
    month: "short",
    day: "numeric",
    year: "numeric",
  });

  return (
    <div className="h-full flex flex-col bg-card border border-border rounded-lg overflow-hidden transition-shadow">
      <div className="flex items-center justify-between px-3 py-2 border-b border-border bg-secondary/30">
        <div className="flex items-center gap-2 min-w-0">
          <div className="chart-drag-handle cursor-grab active:cursor-grabbing p-1 -ml-1 hover:bg-muted rounded">
            <GripVertical className="w-4 h-4 text-muted-foreground" />
          </div>
          <div className="p-1.5 rounded bg-warning/10 flex-shrink-0">
            <Lightbulb className="h-4 w-4 text-warning" />
          </div>
          <h3 className="text-sm font-medium text-foreground/90 truncate min-w-0">
            {artifact.content?.title || "Insight"}
          </h3>
          {artifact.pinned && (
            <Badge variant="outline" className="text-xs text-primary border-primary/30">
              Pinned
            </Badge>
          )}
        </div>
        <DropdownMenu>
          <DropdownMenuTrigger asChild>
            <Button variant="ghost" size="icon-sm">
              <MoreHorizontal className="w-3.5 h-3.5" />
            </Button>
          </DropdownMenuTrigger>
          <DropdownMenuContent align="end">
            <DropdownMenuItem onClick={onPin}>
              <Pin className="h-4 w-4 mr-2" />
              {artifact.pinned ? "Unpin" : "Pin"}
            </DropdownMenuItem>
            <DropdownMenuItem onClick={onMoveBack}>
              <List className="h-4 w-4 mr-2" />
              Move back to Insights
            </DropdownMenuItem>
            <DropdownMenuItem>
              <MessageSquare className="h-4 w-4 mr-2" />
              Comment
            </DropdownMenuItem>
            <DropdownMenuItem onClick={onDelete} className="text-destructive">
              <Trash2 className="h-4 w-4 mr-2" />
              Delete
            </DropdownMenuItem>
          </DropdownMenuContent>
        </DropdownMenu>
      </div>

      <div className="flex-1 p-3 overflow-auto">
        {artifact.content?.summary && (
          <p className="text-xs text-muted-foreground mb-2">{artifact.content.summary}</p>
        )}
        <ul className="space-y-1.5">
          {bullets.slice(0, 8).map((bullet: string, index: number) => (
            <li key={index} className="text-xs text-foreground/90 flex items-start gap-1.5">
              <span className="text-primary mt-px">•</span>
              <span className="break-words">{bullet}</span>
            </li>
          ))}
        </ul>
      </div>

      <div className="px-3 py-2 border-t border-border bg-secondary/20">
        <div className="text-[10px] text-muted-foreground flex items-center gap-1">
          <Clock className="h-3 w-3" />
          {dateText}
        </div>
      </div>
    </div>
  );
};

export default ChartsTab;
