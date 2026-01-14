import { useEffect, useState } from "react";
import { useParams, useNavigate } from "react-router-dom";
import { useTableStore } from "@/store/tableStore";
import { Button } from "@/components/ui/button";
import ScrollableWorkspace from "@/components/workspace/ScrollableWorkspace";
import AIActionsPanel from "@/components/workspace/AIActionsPanel";
import WorkflowTab from "@/components/workspace/tabs/WorkflowTab";
import WorkspaceHeader, { Collaborator, TokenUsage } from "@/components/workspace/WorkspaceHeader";
import { useToast } from "@/hooks/use-toast";
import { tablesApi } from "@/api/tables";

// Mock 协作者数据 - 未来从后端获取
const mockCollaborators: Collaborator[] = [
  { id: "1", name: "Alice Chen", email: "alice@example.com", status: "online", color: "#10B981" },
  { id: "2", name: "Bob Wang", email: "bob@example.com", status: "idle", color: "#F59E0B" },
];

// Mock Token 使用数据 - 未来从后端获取
const mockTokenUsage: TokenUsage = {
  context: 8200,
  output: 4250,
  total: 12450,
};

const TableWorkspace = () => {
  const { id } = useParams<{ id: string }>();
  const navigate = useNavigate();
  const {
    tableAssets,
    getTableResult,
    updateTableAsset,
    addTableAsset,
    loadReport,
  } = useTableStore();
  const [activeTab, setActiveTab] = useState<"workflow" | "report">("workflow");
  const [aiPanelOpen, setAiPanelOpen] = useState(true);
  const [isLoading, setIsLoading] = useState(false);
  const { toast } = useToast();

  const tableAsset = tableAssets.find((t) => t.id === id);
  const tableResult = id ? getTableResult(id) : undefined;
  useEffect(() => {
    if (!id || tableAsset) return;
    setIsLoading(true);
    tablesApi.getTableAssetById(id)
      .then((response) => {
        if (response.status === "success" && response.data) {
          addTableAsset(response.data);
        } else {
          toast({
            title: "Failed to load table",
            description: response.error || "Could not fetch table asset",
            variant: "destructive",
          });
        }
      })
      .catch(() => {
        toast({
          title: "Failed to load table",
          description: "Could not connect to server",
          variant: "destructive",
        });
      })
      .finally(() => setIsLoading(false));
  }, [addTableAsset, id, tableAsset, toast]);

  useEffect(() => {
    if (!id || !tableAsset || activeTab !== "report") return;
    void loadReport(id).catch((error) => {
      toast({
        title: "Failed to load report",
        description: error instanceof Error ? error.message : "Unknown error",
        variant: "destructive",
      });
    });
  }, [activeTab, id, loadReport, tableAsset, toast]);

  if (isLoading) {
    return (
      <div className="min-h-screen bg-background flex items-center justify-center">
        <div className="text-center text-muted-foreground">Loading table...</div>
      </div>
    );
  }

  if (!tableAsset) {
    return (
      <div className="min-h-screen bg-background flex items-center justify-center">
        <div className="text-center">
          <h2 className="text-lg font-medium mb-2">Table not found</h2>
          <Button variant="outline" size="sm" onClick={() => navigate("/")}>
            Back to Tables
          </Button>
        </div>
      </div>
    );
  }

  // 处理标题更新
  const handleTitleChange = (newTitle: string) => {
    if (id) {
      updateTableAsset(id, { name: newTitle });
    }
  };

  // 处理模式切换
  const handleModeChange = (mode: "workflow" | "report") => {
    setActiveTab(mode);
  };

  // 处理邀请协作者
  const handleInvite = () => {
    // TODO: 打开邀请协作者的对话框
    toast({ title: "Invite collaborators coming soon" });
  };

  return (
    <div className="h-screen bg-background flex flex-col overflow-hidden">
      <WorkspaceHeader
        title={tableAsset.name}
        subtitle={`${tableAsset.database?.toLowerCase()}.${tableAsset.schema?.toLowerCase()}.${tableAsset.name.toLowerCase().replace(/\s+/g, "_")}`}
        onTitleChange={handleTitleChange}
        mode={activeTab}
        onModeChange={handleModeChange}
        collaborators={mockCollaborators}
        onInvite={handleInvite}
        tokenUsage={mockTokenUsage}
      />

      <div className="flex-1 flex overflow-hidden">
        <div className="flex-1 overflow-hidden">
          <div className={activeTab === "workflow" ? "h-full" : "hidden"} aria-hidden={activeTab !== "workflow"}>
            <WorkflowTab tableId={tableAsset.id} />
          </div>
          <div className={activeTab === "report" ? "h-full" : "hidden"} aria-hidden={activeTab !== "report"}>
            <ScrollableWorkspace tableAsset={tableAsset} tableResult={tableResult} />
          </div>
        </div>

        {activeTab === "report" && (
          <div className="flex-shrink-0 sticky top-0 h-full">
            <AIActionsPanel
              tableId={tableAsset.id}
              activeTab="overview"
              isOpen={aiPanelOpen}
              onToggle={() => setAiPanelOpen(!aiPanelOpen)}
            />
          </div>
        )}
      </div>
    </div>
  );
};

export default TableWorkspace;
