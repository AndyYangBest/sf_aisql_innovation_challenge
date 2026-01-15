import { LayoutGrid, Table, BarChart3, FileText, Columns, Lightbulb, GitBranch, Network, Workflow } from "lucide-react";
import { cn } from "@/lib/utils";
import { TableAsset, TableResult, WorkspaceTab } from "@/types";
import OverviewTab from "./tabs/OverviewTab";
import DataTab from "./tabs/DataTab";
import ProfileTab from "./tabs/ProfileTab";
import ChartsTab from "./tabs/ChartsTab";
import InsightsTab from "./tabs/InsightsTab";
import NotesTab from "./tabs/NotesTab";
import LineageTab from "./tabs/LineageTab";
import ColumnMapTab from "./tabs/ColumnMapTab";
import WorkflowTab from "./tabs/WorkflowTab";

interface WorkspaceTabsProps {
  activeTab: WorkspaceTab;
  onTabChange: (tab: WorkspaceTab) => void;
  tableAsset: TableAsset;
  tableResult?: TableResult;
  hideWorkflowTab?: boolean;
}

const allTabs: { id: WorkspaceTab; label: string; icon: React.ElementType; color: string }[] = [
  { id: "workflow", label: "Workflow", icon: Workflow, color: "text-[hsl(var(--viz-purple))]" },
  { id: "overview", label: "Overview", icon: LayoutGrid, color: "text-[hsl(var(--viz-blue))]" },
  { id: "data", label: "Data", icon: Table, color: "text-[hsl(var(--viz-cyan))]" },
  { id: "profile", label: "Profile", icon: Columns, color: "text-[hsl(var(--viz-green))]" },
  { id: "columnmap", label: "Column Map", icon: Network, color: "text-[hsl(var(--viz-purple))]" },
  { id: "charts", label: "Charts", icon: BarChart3, color: "text-[hsl(var(--viz-orange))]" },
  { id: "insights", label: "Insights", icon: Lightbulb, color: "text-[hsl(var(--viz-yellow))]" },
  { id: "notes", label: "Notes", icon: FileText, color: "text-[hsl(var(--viz-pink))]" },
  { id: "lineage", label: "Lineage", icon: GitBranch, color: "text-muted-foreground" },
];

const WorkspaceTabs = ({ activeTab, onTabChange, tableAsset, tableResult, hideWorkflowTab = false }: WorkspaceTabsProps) => {
  const tabs = hideWorkflowTab ? allTabs.filter(t => t.id !== "workflow") : allTabs;
  return (
    <div className="flex flex-1 h-full overflow-hidden">
      {/* Left Tab Rail - Fixed */}
      <div className="w-14 border-r border-border bg-card/50 flex flex-col items-center py-4 gap-1 flex-shrink-0 overflow-hidden">
        {tabs.map((tab) => (
          <button
            key={tab.id}
            onClick={() => onTabChange(tab.id)}
            className={cn(
              "p-3 rounded-lg transition-all duration-200 group relative",
              activeTab === tab.id
                ? "bg-primary text-primary-foreground shadow-md"
                : "text-muted-foreground hover:text-foreground hover:bg-muted"
            )}
          >
            <tab.icon className={cn("h-5 w-5", activeTab !== tab.id && tab.color)} />
            <span className="absolute left-full ml-2 px-2 py-1 rounded bg-popover text-popover-foreground text-xs font-medium opacity-0 group-hover:opacity-100 pointer-events-none whitespace-nowrap transition-opacity z-50 shadow-lg border border-border">
              {tab.label}
            </span>
          </button>
        ))}
      </div>

      {/* Main Content Area - Independent Scroll */}
      <div className={cn(
        "flex-1 h-full",
        activeTab === "workflow" ? "overflow-hidden" : "overflow-y-auto p-6 scrollbar-thin overflow-x-hidden"
      )}>
        {activeTab === "workflow" && <WorkflowTab tableId={tableAsset.id} />}
        {activeTab === "overview" && <OverviewTab tableAsset={tableAsset} tableResult={tableResult} />}
        {activeTab === "data" && <DataTab tableResult={tableResult} />}
        {activeTab === "profile" && <ProfileTab tableResult={tableResult} />}
        {activeTab === "columnmap" && (
          <ColumnMapTab tableId={tableAsset.id} tableResult={tableResult} />
        )}
        {activeTab === "charts" && <ChartsTab tableId={tableAsset.id} />}
        {activeTab === "insights" && <InsightsTab tableId={tableAsset.id} />}
        {activeTab === "notes" && <NotesTab tableId={tableAsset.id} />}
        {activeTab === "lineage" && <LineageTab tableAsset={tableAsset} tableResult={tableResult} />}
      </div>
    </div>
  );
};

export default WorkspaceTabs;
