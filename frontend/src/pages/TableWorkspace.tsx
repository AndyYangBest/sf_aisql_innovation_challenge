import { useState, useCallback, useMemo } from "react";
import { useParams, useNavigate } from "react-router-dom";
import { useTableStore } from "@/store/tableStore";
import { useWorkflowStore } from "@/store/workflowStore";
import { Button } from "@/components/ui/button";
import ScrollableWorkspace from "@/components/workspace/ScrollableWorkspace";
import AIActionsPanel from "@/components/workspace/AIActionsPanel";
import WorkflowTab from "@/components/workspace/tabs/WorkflowTab";
import WorkspaceHeader, { Collaborator, TokenUsage } from "@/components/workspace/WorkspaceHeader";
import { useToast } from "@/hooks/use-toast";
import { getExecutionOrder, simulateNodeExecution } from "@/store/workflowStore";

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
  const { tableAssets, getTableResult, updateTableAsset } = useTableStore();
  const { getWorkflow, createWorkflow, startExecution, updateNodeStatus, completeExecution } = useWorkflowStore();
  const [activeTab, setActiveTab] = useState<"workflow" | "report">("workflow");
  const [aiPanelOpen, setAiPanelOpen] = useState(true);
  const [isRunning, setIsRunning] = useState(false);
  const [workflowCompleted, setWorkflowCompleted] = useState(false);
  const { toast } = useToast();

  const tableAsset = tableAssets.find((t) => t.id === id);
  const tableResult = id ? getTableResult(id) : undefined;

  // 检查工作流是否已完成
  const workflow = useMemo(() => {
    if (!id) return null;
    return getWorkflow(id);
  }, [id, getWorkflow]);

  // 工作流模式：未完成时全屏显示工作流编辑器
  const isWorkflowMode = activeTab === "workflow" && !workflowCompleted;

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
    if (mode === "workflow") {
      setWorkflowCompleted(false);
    } else {
      setWorkflowCompleted(true);
    }
    setActiveTab(mode);
  };

  // 运行工作流
  const handleRunWorkflow = useCallback(async () => {
    if (!id || isRunning) return;
    
    setIsRunning(true);
    const wf = getWorkflow(id) ?? createWorkflow(id);
    
    try {
      startExecution(wf.id);
      const order = getExecutionOrder(wf);
      const outputs: Record<string, unknown> = {};

      for (const nodeId of order) {
        const node = wf.nodes.find((n) => n.id === nodeId);
        if (!node) continue;

        updateNodeStatus(wf.id, nodeId, "running");

        try {
          const inputs: Record<string, unknown> = {};
          wf.edges
            .filter((e) => e.targetNodeId === nodeId)
            .forEach((e) => {
              inputs[e.targetPortId] = outputs[e.sourceNodeId];
            });

          const output = await simulateNodeExecution(node, inputs);
          outputs[nodeId] = output;
          updateNodeStatus(wf.id, nodeId, "success", output);
        } catch (error) {
          updateNodeStatus(
            wf.id,
            nodeId,
            "error",
            undefined,
            error instanceof Error ? error.message : "Unknown error"
          );
        }
      }

      completeExecution(wf.id, true);
      toast({ title: "Workflow completed" });
      
      setWorkflowCompleted(true);
      setActiveTab("report");
    } catch (error) {
      console.error("Workflow execution failed:", error);
      completeExecution(wf.id, false);
      toast({ title: "Workflow failed", variant: "destructive" });
    } finally {
      setIsRunning(false);
    }
  }, [id, isRunning, getWorkflow, createWorkflow, startExecution, updateNodeStatus, completeExecution, toast]);

  // 处理邀请协作者
  const handleInvite = () => {
    // TODO: 打开邀请协作者的对话框
    toast({ title: "Invite collaborators coming soon" });
  };

  // 工作流画布模式
  if (isWorkflowMode) {
    return (
      <div className="h-screen bg-background flex flex-col overflow-hidden">
        <WorkspaceHeader
          title={tableAsset.name}
          subtitle={`${tableAsset.database?.toLowerCase()}.${tableAsset.schema?.toLowerCase()}.${tableAsset.name.toLowerCase().replace(/\s+/g, "_")}`}
          onTitleChange={handleTitleChange}
          mode="workflow"
          onModeChange={handleModeChange}
          collaborators={mockCollaborators}
          onInvite={handleInvite}
          tokenUsage={mockTokenUsage}
        />

        <div className="flex-1 flex overflow-hidden">
          <div className="flex-1 overflow-hidden">
            <WorkflowTab 
              tableId={tableAsset.id} 
              onRunComplete={() => {
                setWorkflowCompleted(true);
                setActiveTab("report");
              }} 
            />
          </div>
          
          <div className="flex-shrink-0 sticky top-0 h-full">
            <AIActionsPanel 
              tableId={tableAsset.id} 
              activeTab="workflow"
              isOpen={aiPanelOpen}
              onToggle={() => setAiPanelOpen(!aiPanelOpen)}
            />
          </div>
        </div>
      </div>
    );
  }

  // 报告模式
  return (
    <div className="h-screen bg-background flex flex-col overflow-hidden">
      <WorkspaceHeader
        title={tableAsset.name}
        subtitle={`${tableAsset.database?.toLowerCase()}.${tableAsset.schema?.toLowerCase()}.${tableAsset.name.toLowerCase().replace(/\s+/g, "_")}`}
        onTitleChange={handleTitleChange}
        mode="report"
        onModeChange={handleModeChange}
        collaborators={mockCollaborators}
        onInvite={handleInvite}
        tokenUsage={mockTokenUsage}
      />

      <div className="flex-1 flex overflow-hidden">
        <div className="flex-1 flex overflow-hidden">
          <ScrollableWorkspace
            tableAsset={tableAsset}
            tableResult={tableResult}
          />
        </div>

        <div className="flex-shrink-0 sticky top-0 h-full">
          <AIActionsPanel 
            tableId={tableAsset.id} 
            activeTab="overview"
            isOpen={aiPanelOpen}
            onToggle={() => setAiPanelOpen(!aiPanelOpen)}
          />
        </div>
      </div>
    </div>
  );
};

export default TableWorkspace;
