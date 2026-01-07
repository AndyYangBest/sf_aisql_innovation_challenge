import { useRef, useEffect, useState, useCallback } from "react";
import {
  LayoutGrid,
  Table,
  BarChart3,
  FileText,
  Columns,
  Lightbulb,
  Network,
  GitBranch,
} from "lucide-react";
import { cn } from "@/lib/utils";
import { TableAsset, TableResult } from "@/types";
import OverviewTab from "./tabs/OverviewTab";
import DataTab from "./tabs/DataTab";
import ProfileTab from "./tabs/ProfileTab";
import ChartsTab from "./tabs/ChartsTab";
import InsightsTab from "./tabs/InsightsTab";
import NotesTab from "./tabs/NotesTab";
import LineageTab from "./tabs/LineageTab";
import ColumnMapTab from "./tabs/ColumnMapTab";

interface ScrollableWorkspaceProps {
  tableAsset: TableAsset;
  tableResult?: TableResult;
}

const sections = [
  {
    id: "overview",
    label: "Overview",
    icon: LayoutGrid,
    color: "text-[hsl(var(--viz-blue))]",
  },
  {
    id: "data",
    label: "Data",
    icon: Table,
    color: "text-[hsl(var(--viz-cyan))]",
  },
  {
    id: "profile",
    label: "Profile",
    icon: Columns,
    color: "text-[hsl(var(--viz-green))]",
  },
  {
    id: "columnmap",
    label: "Column Map",
    icon: Network,
    color: "text-[hsl(var(--viz-purple))]",
  },
  {
    id: "charts",
    label: "Charts",
    icon: BarChart3,
    color: "text-[hsl(var(--viz-orange))]",
  },
  {
    id: "insights",
    label: "Insights",
    icon: Lightbulb,
    color: "text-[hsl(var(--viz-yellow))]",
  },
  {
    id: "notes",
    label: "Notes",
    icon: FileText,
    color: "text-[hsl(var(--viz-pink))]",
  },
  {
    id: "lineage",
    label: "Lineage",
    icon: GitBranch,
    color: "text-muted-foreground",
  },
];

const ScrollableWorkspace = ({
  tableAsset,
  tableResult,
}: ScrollableWorkspaceProps) => {
  const containerRef = useRef<HTMLDivElement>(null);
  const sectionRefs = useRef<Record<string, HTMLDivElement | null>>({});
  const [activeSection, setActiveSection] = useState("overview");

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
        className="flex-1 overflow-y-auto p-6 scrollbar-thin scroll-smooth"
      >
        <div className="space-y-12">
          {/* Overview Section */}
          <div
            ref={(el) => {
              sectionRefs.current["overview"] = el;
            }}
            id="overview"
          >
            <OverviewTab tableAsset={tableAsset} tableResult={tableResult} />
          </div>

          {/* Data Section */}
          <div
            ref={(el) => {
              sectionRefs.current["data"] = el;
            }}
            id="data"
            className="pt-6 border-t border-border"
          >
            <h2 className="text-lg font-semibold mb-4 flex items-center gap-2">
              <Table className="h-5 w-5 text-[hsl(var(--viz-cyan))]" />
              Data Preview
            </h2>
            <DataTab tableResult={tableResult} />
          </div>

          {/* Profile Section */}
          <div
            ref={(el) => {
              sectionRefs.current["profile"] = el;
            }}
            id="profile"
            className="pt-6 border-t border-border"
          >
            <h2 className="text-lg font-semibold mb-4 flex items-center gap-2">
              <Columns className="h-5 w-5 text-[hsl(var(--viz-green))]" />
              Column Profile
            </h2>
            <ProfileTab tableResult={tableResult} />
          </div>

          {/* Column Map Section */}
          <div
            ref={(el) => {
              sectionRefs.current["columnmap"] = el;
            }}
            id="columnmap"
            className="pt-6 border-t border-border"
          >
            <h2 className="text-lg font-semibold mb-4 flex items-center gap-2">
              <Network className="h-5 w-5 text-[hsl(var(--viz-purple))]" />
              Column Map
            </h2>
            <ColumnMapTab tableResult={tableResult} />
          </div>

          {/* Charts Section */}
          <div
            ref={(el) => {
              sectionRefs.current["charts"] = el;
            }}
            id="charts"
            className="pt-6 border-t border-border"
          >
            <h2 className="text-lg font-semibold mb-4 flex items-center gap-2">
              <BarChart3 className="h-5 w-5 text-[hsl(var(--viz-orange))]" />
              Charts
            </h2>
            <ChartsTab tableId={tableAsset.id} />
          </div>

          {/* Insights Section */}
          <div
            ref={(el) => {
              sectionRefs.current["insights"] = el;
            }}
            id="insights"
            className="pt-6 border-t border-border"
          >
            <h2 className="text-lg font-semibold mb-4 flex items-center gap-2">
              <Lightbulb className="h-5 w-5 text-[hsl(var(--viz-yellow))]" />
              Insights
            </h2>
            <InsightsTab tableId={tableAsset.id} />
          </div>

          {/* Notes Section */}
          <div
            ref={(el) => {
              sectionRefs.current["notes"] = el;
            }}
            id="notes"
            className="pt-6 border-t border-border"
          >
            <h2 className="text-lg font-semibold mb-4 flex items-center gap-2">
              <FileText className="h-5 w-5 text-[hsl(var(--viz-pink))]" />
              Notes
            </h2>
            <NotesTab tableId={tableAsset.id} />
          </div>

          {/* Lineage Section */}
          <div
            ref={(el) => {
              sectionRefs.current["lineage"] = el;
            }}
            id="lineage"
            className="pt-6 border-t border-border"
          >
            <h2 className="text-lg font-semibold mb-4 flex items-center gap-2">
              <GitBranch className="h-5 w-5 text-muted-foreground" />
              Lineage
            </h2>
            <LineageTab tableAsset={tableAsset} tableResult={tableResult} />
          </div>

          {/* Bottom padding */}
          <div className="h-24" />
        </div>
      </div>
    </div>
  );
};

export default ScrollableWorkspace;
