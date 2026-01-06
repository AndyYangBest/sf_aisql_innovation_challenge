/**
 * WorkflowEditor - 基于 Flowgram 的工作流可视化编辑器
 */

import { useCallback, useRef, useState, useMemo } from "react";
import {
  FreeLayoutEditorProvider,
  EditorRenderer,
  FreeLayoutPluginContext,
  WorkflowJSON,
  WorkflowNodeRegistry,
  WorkflowNodeRenderer,
} from "@flowgram.ai/free-layout-editor";
import { useWorkflowStore, getExecutionOrder, simulateNodeExecution } from "@/store/workflowStore";
import { NODE_DEFINITIONS, WorkflowNodeType, Workflow } from "@/types/workflow";
import { cn } from "@/lib/utils";
import WorkflowToolbar from "./WorkflowToolbar";
import WorkflowStatusBar from "./WorkflowStatusBar";
import { Database, Sparkles, BarChart3, Lightbulb, Shuffle, Download, CheckCircle2, XCircle, Loader2 } from "lucide-react";

interface WorkflowEditorProps {
  tableId: string;
  onRunComplete?: () => void;
  className?: string;
}

const iconMap: Record<string, React.ComponentType<{ className?: string }>> = {
  Database,
  Sparkles,
  BarChart3,
  Lightbulb,
  Shuffle,
  Download,
};

const statusStyles: Record<string, string> = {
  idle: "border-border bg-card",
  running: "border-amber-500 bg-amber-500/10",
  success: "border-emerald-500 bg-emerald-500/10",
  error: "border-destructive bg-destructive/10",
  skipped: "border-muted bg-muted/50",
};

// 自定义节点渲染组件
const BaseNode = (props: { node: any }) => {
  const node = props.node;
  const nodeData = (node?.getData?.() ?? {}) as Record<string, any>;
  const title = nodeData?.title ?? node?.id ?? "Node";
  const status = (nodeData?.status ?? "idle") as string;
  const iconName = nodeData?.icon ?? "Database";
  const Icon = iconMap[iconName] ?? Database;

  const StatusIcon = () => {
    switch (status) {
      case "running":
        return <Loader2 className="h-3 w-3 animate-spin text-amber-500" />;
      case "success":
        return <CheckCircle2 className="h-3 w-3 text-emerald-500" />;
      case "error":
        return <XCircle className="h-3 w-3 text-destructive" />;
      default:
        return null;
    }
  };

  return (
    <WorkflowNodeRenderer
      node={node}
      className={cn(
        "rounded-lg border-2 shadow-md transition-all duration-200 min-w-[160px]",
        statusStyles[status] ?? statusStyles.idle
      )}
      portPrimaryColor="#00b4d8"
      portSecondaryColor="#48cae4"
      portErrorColor="#ef4444"
      portBackgroundColor="#0f1419"
    >
      <div className="px-3 py-2">
        <div className="flex items-center gap-2">
          <div className="p-1.5 rounded-md bg-primary/10">
            <Icon className="h-4 w-4 text-primary" />
          </div>
          <span className="font-medium text-sm text-foreground flex-1 truncate">
            {title}
          </span>
          <StatusIcon />
        </div>
      </div>
    </WorkflowNodeRenderer>
  );
};

// 转换为 Flowgram WorkflowJSON 格式
function toFlowgramData(workflow: Workflow): WorkflowJSON {
  return {
    nodes: workflow.nodes.map((node) => ({
      id: node.id,
      type: node.type,
      meta: {
        position: node.position,
      },
      data: {
        title: node.name,
        type: node.type,
        status: node.status,
        icon: NODE_DEFINITIONS[node.type]?.icon || "Database",
      },
    })),
    edges: workflow.edges.map((edge) => ({
      sourceNodeID: edge.sourceNodeId,
      targetNodeID: edge.targetNodeId,
      sourcePortID: edge.sourcePortId,
      targetPortID: edge.targetPortId,
    })),
  };
}

// 创建节点注册配置
function createNodeRegistries(): WorkflowNodeRegistry[] {
  return Object.entries(NODE_DEFINITIONS).map(([type, def]) => ({
    type,
    meta: {
      isStart: type === "data_source",
      deleteDisable: type === "data_source",
      copyDisable: type === "data_source",
      defaultPorts: type === "data_source" 
        ? [{ type: "output" as const }]
        : type === "export"
        ? [{ type: "input" as const }]
        : [{ type: "input" as const }, { type: "output" as const }],
    },
    formMeta: {
      render: () => <div className="text-xs text-muted-foreground">{def.name}</div>,
    },
  }));
}

const WorkflowEditor = ({ tableId, onRunComplete, className }: WorkflowEditorProps) => {
  const editorRef = useRef<FreeLayoutPluginContext | undefined>();
  const [isRunning, setIsRunning] = useState(false);

  const {
    getWorkflow,
    createWorkflow,
    addNode,
    startExecution,
    updateNodeStatus,
    completeExecution,
    resetExecution,
    getExecutionContext,
  } = useWorkflowStore();

  // 确保工作流存在
  const workflow = useMemo(() => {
    return getWorkflow(tableId) ?? createWorkflow(tableId);
  }, [tableId, getWorkflow, createWorkflow]);
  
  const executionContext = getExecutionContext(workflow.id);

  // Flowgram 初始数据
  const initialData = useMemo(() => toFlowgramData(workflow), [workflow]);
  
  // 节点注册
  const nodeRegistries = useMemo(() => createNodeRegistries(), []);

  // 添加节点
  const handleAddNode = useCallback(
    (type: WorkflowNodeType) => {
      const position = {
        x: 200 + Math.random() * 200,
        y: 150 + Math.random() * 100,
      };
      addNode(workflow.id, type, position);
    },
    [workflow.id, addNode]
  );

  // 运行工作流
  const handleRun = useCallback(async () => {
    if (isRunning) return;
    setIsRunning(true);

    try {
      startExecution(workflow.id);
      
      const currentWorkflow = getWorkflow(tableId);
      if (!currentWorkflow) {
        throw new Error("Workflow not found");
      }
      
      const order = getExecutionOrder(currentWorkflow);
      const outputs: Record<string, unknown> = {};

      for (const nodeId of order) {
        const node = currentWorkflow.nodes.find((n) => n.id === nodeId);
        if (!node) continue;

        updateNodeStatus(workflow.id, nodeId, "running");

        try {
          const inputs: Record<string, unknown> = {};
          currentWorkflow.edges
            .filter((e) => e.targetNodeId === nodeId)
            .forEach((e) => {
              inputs[e.targetPortId] = outputs[e.sourceNodeId];
            });

          const output = await simulateNodeExecution(node, inputs);
          outputs[nodeId] = output;
          updateNodeStatus(workflow.id, nodeId, "success", output);
        } catch (error) {
          updateNodeStatus(
            workflow.id,
            nodeId,
            "error",
            undefined,
            error instanceof Error ? error.message : "Unknown error"
          );
        }
      }

      completeExecution(workflow.id, true);
      onRunComplete?.();
    } catch (error) {
      console.error("Workflow execution failed:", error);
      completeExecution(workflow.id, false);
    } finally {
      setIsRunning(false);
    }
  }, [
    workflow.id,
    tableId,
    isRunning,
    getWorkflow,
    startExecution,
    updateNodeStatus,
    completeExecution,
    onRunComplete,
  ]);

  // 重置工作流
  const handleReset = useCallback(() => {
    resetExecution(workflow.id);
  }, [workflow.id, resetExecution]);

  return (
    <div className={cn("flex flex-col h-full bg-background", className)}>
      {/* 工具栏 */}
      <WorkflowToolbar
        workflowStatus={workflow.status}
        onAddNode={handleAddNode}
      />

      {/* Flowgram 画布 */}
      <div className="flex-1 relative bg-muted/20">
        <FreeLayoutEditorProvider 
          initialData={initialData}
          nodeRegistries={nodeRegistries}
          ref={editorRef}
          materials={{
            renderNodes: {
              default: BaseNode,
            },
          }}
        >
          <EditorRenderer className="w-full h-full" />
        </FreeLayoutEditorProvider>
      </div>

      {/* 状态栏 */}
      <WorkflowStatusBar
        workflow={workflow}
        executionContext={executionContext}
        isRunning={isRunning}
      />
    </div>
  );
};

export default WorkflowEditor;
