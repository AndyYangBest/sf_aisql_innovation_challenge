import { useRef, useEffect, useState, useCallback } from "react";
import { BarChart3, FileText, Lightbulb, ShieldCheck } from "lucide-react";
import { cn } from "@/lib/utils";
import { TableAsset, TableResult } from "@/types";
import OverviewTab from "./tabs/OverviewTab";
import ChartsTab from "./tabs/ChartsTab";
import InsightsTab from "./tabs/InsightsTab";
import NotesTab from "./tabs/NotesTab";

interface ScrollableWorkspaceProps {
  tableAsset: TableAsset;
  tableResult?: TableResult;
}

const sections = [
  {
    id: "approvals",
    label: "Approve Plans",
    icon: ShieldCheck,
    color: "text-[hsl(var(--viz-green))]",
  },
  {
    id: "visuals",
    label: "Visuals",
    icon: BarChart3,
    color: "text-[hsl(var(--viz-orange))]",
  },
  {
    id: "insights-notes",
    label: "Insights + Notes",
    icon: Lightbulb,
    color: "text-[hsl(var(--viz-yellow))]",
  },
];

const ScrollableWorkspace = ({
  tableAsset,
  tableResult,
}: ScrollableWorkspaceProps) => {
  const containerRef = useRef<HTMLDivElement>(null);
  const sectionRefs = useRef<Record<string, HTMLDivElement | null>>({});
  const [activeSection, setActiveSection] = useState("approvals");

  // 滚动到指定 section
  const scrollToSection = useCallback((sectionId: string) => {
    const element = sectionRefs.current[sectionId];
    if (element && containerRef.current) {
      const containerTop = containerRef.current.getBoundingClientRect().top;
      const elementTop = element.getBoundingClientRect().top;
      const offset =
        elementTop - containerTop + containerRef.current.scrollTop - 20;

      containerRef.current.scrollTo({
        top: offset,
        behavior: "smooth",
      });
    }
  }, []);

  // 监听滚动，更新当前 section
  useEffect(() => {
    const container = containerRef.current;
    if (!container) return;

    const handleScroll = () => {
      const containerRect = container.getBoundingClientRect();
      const scrollTop = container.scrollTop;

      let currentSection = "overview";

      for (const section of sections) {
        const element = sectionRefs.current[section.id];
        if (element) {
          const rect = element.getBoundingClientRect();
          const relativeTop = rect.top - containerRect.top;

          // 当 section 顶部进入视口上半部分时激活
          if (relativeTop <= containerRect.height * 0.3) {
            currentSection = section.id;
          }
        }
      }

      setActiveSection(currentSection);
    };

    container.addEventListener("scroll", handleScroll);
    return () => container.removeEventListener("scroll", handleScroll);
  }, []);

  return (
    <div className="flex flex-1 h-full overflow-hidden">
      {/* Left Navigation Rail - Fixed */}
      <div className="w-14 border-r border-border bg-card/50 flex flex-col items-center py-4 gap-1 flex-shrink-0">
        {sections.map((section) => (
          <button
            key={section.id}
            onClick={() => scrollToSection(section.id)}
            className={cn(
              "p-3 rounded-lg transition-all duration-200 group relative",
              activeSection === section.id
                ? "bg-primary text-primary-foreground shadow-md"
                : "text-muted-foreground hover:text-foreground hover:bg-muted"
            )}
          >
            <section.icon
              className={cn(
                "h-5 w-5",
                activeSection !== section.id && section.color
              )}
            />
            <span className="absolute left-full ml-2 px-2 py-1 rounded bg-popover text-popover-foreground text-xs font-medium opacity-0 group-hover:opacity-100 pointer-events-none whitespace-nowrap transition-opacity z-50 shadow-lg border border-border">
              {section.label}
            </span>
          </button>
        ))}
      </div>

      {/* Main Scrollable Content */}
      <div
        ref={containerRef}
        className="flex-1 overflow-y-auto p-6 scrollbar-thin scroll-smooth min-w-0"
      >
        <div className="space-y-10">
          {/* Approvals Section */}
          <div
            ref={(el) => {
              sectionRefs.current["approvals"] = el;
            }}
            id="approvals"
          >
            <h2 className="text-lg font-semibold mb-4 flex items-center gap-2">
              <ShieldCheck className="h-5 w-5 text-[hsl(var(--viz-green))]" />
              Approve Plans
            </h2>
            <OverviewTab tableAsset={tableAsset} tableResult={tableResult} variant="approvals" />
          </div>

          {/* Visuals Section */}
          <div
            ref={(el) => {
              sectionRefs.current["visuals"] = el;
            }}
            id="visuals"
            className="pt-6 border-t border-border"
          >
            <h2 className="text-lg font-semibold mb-4 flex items-center gap-2">
              <BarChart3 className="h-5 w-5 text-[hsl(var(--viz-orange))]" />
              Visuals
            </h2>
            <ChartsTab tableId={tableAsset.id} />
          </div>

          {/* Insights + Notes Section */}
          <div
            ref={(el) => {
              sectionRefs.current["insights-notes"] = el;
            }}
            id="insights-notes"
            className="pt-6 border-t border-border"
          >
            <h2 className="text-lg font-semibold mb-4 flex items-center gap-2">
              <Lightbulb className="h-5 w-5 text-[hsl(var(--viz-yellow))]" />
              Insights + Notes
            </h2>
            <div className="grid gap-6 lg:grid-cols-2">
              <div className="rounded-lg border border-border bg-card/40 p-4">
                <div className="flex items-center gap-2 mb-3">
                  <Lightbulb className="h-4 w-4 text-[hsl(var(--viz-yellow))]" />
                  <span className="text-sm font-medium">Insights</span>
                </div>
                <InsightsTab tableId={tableAsset.id} />
              </div>
              <div className="rounded-lg border border-border bg-card/40 p-4">
                <div className="flex items-center gap-2 mb-3">
                  <FileText className="h-4 w-4 text-[hsl(var(--viz-pink))]" />
                  <span className="text-sm font-medium">Notes</span>
                </div>
                <NotesTab tableId={tableAsset.id} />
              </div>
            </div>
          </div>

          {/* Bottom padding */}
          <div className="h-24" />
        </div>
      </div>
    </div>
  );
};

export default ScrollableWorkspace;
