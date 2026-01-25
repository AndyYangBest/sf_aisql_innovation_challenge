import { useEffect, useMemo, useState } from "react";
import { Sparkles, Tag, Lightbulb, BarChart3, FileText, Clock, ShieldCheck } from "lucide-react";
import { TableAsset, TableResult, InsightArtifact } from "@/types";
import { useTableStore } from "@/store/tableStore";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { columnMetadataApi } from "@/api/columnMetadata";
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from "@/components/ui/alert-dialog";

type RepairPlanItem = {
  columnName: string;
  plan: Record<string, any>;
  nullRate?: number;
  conflictRate?: number;
};

interface OverviewTabProps {
  tableAsset: TableAsset;
  tableResult?: TableResult;
}

const OverviewTab = ({ tableAsset, tableResult }: OverviewTabProps) => {
  const { getArtifactsByTable, getReportStatus } = useTableStore();
  const artifacts = getArtifactsByTable(tableAsset.id);
  const reportStatus = getReportStatus(tableAsset.id);
  const hasReport = reportStatus?.hasReport ?? false;
  const [repairPlans, setRepairPlans] = useState<RepairPlanItem[]>([]);
  const [repairDialogOpen, setRepairDialogOpen] = useState(false);
  const [activeRepair, setActiveRepair] = useState<RepairPlanItem | null>(null);
  const [approvalNote, setApprovalNote] = useState("");

  const insightCount = artifacts.filter((a) => a.type === "insight").length;
  const chartCount = artifacts.filter((a) => a.type === "chart").length;
  const pinnedInsights = artifacts.filter((a): a is InsightArtifact => a.type === "insight" && !!a.pinned);

  const aiSummary = tableAsset.aiSummary || "No summary available yet.";

  const useCases = tableAsset.useCases || [];

  useEffect(() => {
    let active = true;
    const loadRepairs = async () => {
      const tableId = Number.parseInt(tableAsset.id, 10);
      if (Number.isNaN(tableId)) {
        return;
      }
      const response = await columnMetadataApi.get(tableId);
      if (response.status !== "success" || !response.data) {
        return;
      }
      if (!active) return;
      const plans = response.data.columns
        .map((column) => {
          const analysis = column.metadata?.analysis ?? {};
          const plan = analysis.repair_plan ?? {};
          const steps = Array.isArray(plan.steps) ? plan.steps : [];
          if (steps.length === 0) {
            return null;
          }
          return {
            columnName: column.column_name,
            plan,
            nullRate: analysis.nulls?.null_rate,
            conflictRate: analysis.conflicts?.conflict_rate,
          } satisfies RepairPlanItem;
        })
        .filter(Boolean) as RepairPlanItem[];
      setRepairPlans(plans);
    };
    void loadRepairs();
    return () => {
      active = false;
    };
  }, [tableAsset.id]);

  const pendingRepairs = useMemo(
    () => repairPlans.filter((item) => item.plan && !item.plan.approved),
    [repairPlans]
  );

  const formatDate = (date: string) =>
    new Date(date).toLocaleDateString("en-US", {
      month: "short",
      day: "numeric",
      year: "numeric",
    });

  if (!hasReport) {
    return (
      <div className="flex items-center justify-center h-64 text-muted-foreground">
        <div className="text-center">
          <Sparkles className="h-12 w-12 mx-auto mb-3 opacity-50" />
          <p className="mb-2">Report is empty</p>
          <p className="text-sm">Run workflows and save outputs from the sidebar</p>
        </div>
      </div>
    );
  }

  return (
    <div className="space-y-6 max-w-4xl">
      {/* AI Summary */}
      <div className="bg-gradient-to-br from-primary/5 to-[hsl(var(--viz-cyan))]/5 border border-primary/20 rounded-lg p-5">
        <div className="flex items-start gap-3">
          <div className="w-8 h-8 rounded-md bg-primary/20 flex items-center justify-center flex-shrink-0">
            <Sparkles className="w-4 h-4 text-primary" />
          </div>
          <div>
            <p className="text-sm text-muted-foreground mb-1">AI Summary</p>
            <p className="text-foreground leading-relaxed">{aiSummary}</p>
          </div>
        </div>
      </div>

      {/* Use Case Tags */}
      {useCases.length > 0 && (
        <div>
          <div className="flex items-center gap-2 mb-3">
            <Tag className="w-4 h-4 text-muted-foreground" />
            <span className="text-sm font-medium text-muted-foreground">Use Cases</span>
          </div>
          <div className="flex flex-wrap gap-2">
            {useCases.map((tag) => (
              <Badge key={tag} variant="secondary" className="px-3 py-1 text-sm">
                {tag}
              </Badge>
            ))}
          </div>
        </div>
      )}

      {/* Quick Stats */}
      <div className="grid grid-cols-4 gap-3">
        <div className="bg-secondary/30 border border-border rounded-lg p-3">
          <p className="text-xs text-muted-foreground mb-1">Columns</p>
          <p className="text-xl font-semibold">{tableResult?.columns.length || 0}</p>
        </div>
        <div className="bg-secondary/30 border border-border rounded-lg p-3">
          <p className="text-xs text-muted-foreground mb-1">Rows</p>
          <p className="text-xl font-semibold">{tableResult?.rowCount?.toLocaleString() || "-"}</p>
        </div>
        <div className="bg-secondary/30 border border-border rounded-lg p-3">
          <div className="flex items-center gap-1.5">
            <Lightbulb className="w-3 h-3 text-[hsl(var(--viz-yellow))]" />
            <p className="text-xs text-muted-foreground">Insights</p>
          </div>
          <p className="text-xl font-semibold">{insightCount}</p>
        </div>
        <div className="bg-secondary/30 border border-border rounded-lg p-3">
          <div className="flex items-center gap-1.5">
            <BarChart3 className="w-3 h-3 text-[hsl(var(--viz-cyan))]" />
            <p className="text-xs text-muted-foreground">Charts</p>
          </div>
          <p className="text-xl font-semibold">{chartCount}</p>
        </div>
      </div>

      {pendingRepairs.length > 0 && (
        <div className="bg-secondary/30 border border-border rounded-lg p-4 space-y-3">
          <div className="flex items-center gap-2">
            <ShieldCheck className="w-4 h-4 text-emerald-500" />
            <span className="text-sm font-medium">Repair Plans Pending Approval</span>
          </div>
          <div className="space-y-2">
            {pendingRepairs.map((plan) => (
              <div
                key={plan.columnName}
                className="flex flex-col gap-2 rounded-md border border-border bg-background px-3 py-2"
              >
                <div className="flex items-center justify-between gap-3">
                  <div>
                    <div className="text-sm font-medium">{plan.columnName}</div>
                    <div className="text-xs text-muted-foreground">
                      {plan.plan?.summary || "Repair plan available"}
                    </div>
                  </div>
                  <Button
                    size="sm"
                    variant="surface"
                    onClick={() => {
                      setActiveRepair(plan);
                      setApprovalNote("");
                      setRepairDialogOpen(true);
                    }}
                  >
                    Review & Approve
                  </Button>
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Pinned Insights */}
      {pinnedInsights.length > 0 && (
        <div>
          <div className="flex items-center gap-2 mb-3">
            <Lightbulb className="w-4 h-4 text-[hsl(var(--viz-yellow))]" />
            <span className="text-sm font-medium">Pinned Insights</span>
          </div>
          <div className="space-y-3">
            {pinnedInsights.map((insight) => (
              <div key={insight.id} className="bg-secondary/30 border border-border rounded-lg p-4">
                <h4 className="font-medium mb-2">{insight.content.title}</h4>
                <ul className="space-y-1.5">
                  {insight.content.bullets.map((bullet, i) => (
                    <li key={i} className="flex items-start gap-2 text-sm text-muted-foreground">
                      <span className="text-primary mt-0.5">•</span>
                      {bullet}
                    </li>
                  ))}
                </ul>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Recent Activity */}
      {artifacts.length > 0 && (
        <div className="bg-secondary/30 border border-border rounded-lg p-4">
          <h4 className="text-sm font-medium mb-3">Recent Activity</h4>
          <div className="space-y-2">
            {artifacts.slice(0, 4).map((artifact) => (
              <div
                key={artifact.id}
                className="flex items-center gap-3 p-2 rounded-md hover:bg-muted/30 transition-colors"
              >
                {artifact.type === "insight" && <Lightbulb className="h-4 w-4 text-[hsl(var(--viz-yellow))]" />}
                {artifact.type === "chart" && <BarChart3 className="h-4 w-4 text-[hsl(var(--viz-cyan))]" />}
                {artifact.type === "doc" && <FileText className="h-4 w-4 text-[hsl(var(--viz-green))]" />}
                {artifact.type === "annotation" && <Tag className="h-4 w-4 text-muted-foreground" />}
                <div className="flex-1 min-w-0">
                  <p className="text-sm font-medium truncate">
                    {artifact.type === "insight" && artifact.content.title}
                    {artifact.type === "chart" && artifact.content.title}
                    {artifact.type === "doc" && (artifact.content.title || "Documentation")}
                    {artifact.type === "annotation" && "Annotation"}
                  </p>
                  <p className="text-xs text-muted-foreground flex items-center gap-1">
                    <Clock className="h-3 w-3" />
                    {formatDate(artifact.createdAt)}
                  </p>
                </div>
                {artifact.pinned && (
                  <Badge variant="outline" className="text-xs text-primary border-primary/30">
                    Pinned
                  </Badge>
                )}
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Schema Preview */}
      {tableResult && (
        <div className="bg-secondary/30 border border-border rounded-lg p-4">
          <h4 className="text-sm font-medium mb-3">Schema</h4>
          <div className="grid gap-1.5">
            {tableResult.columns.map((col) => (
              <div
                key={col.name}
                className="flex items-center justify-between py-1.5 px-2 rounded bg-background text-sm"
              >
                <span className="font-mono">{col.name}</span>
                <span className="text-xs text-muted-foreground font-mono">{col.type}</span>
              </div>
            ))}
          </div>
        </div>
      )}

      <AlertDialog open={repairDialogOpen} onOpenChange={setRepairDialogOpen}>
        <AlertDialogContent className="bg-slate-950 text-slate-100 border-slate-800">
          <AlertDialogHeader>
            <AlertDialogTitle>Approve Data Repairs</AlertDialogTitle>
            <AlertDialogDescription className="text-slate-300">
              Review the plan details before applying changes.
            </AlertDialogDescription>
          </AlertDialogHeader>
          {activeRepair && (
            <div className="space-y-3 text-sm text-slate-200">
              <div className="rounded-md border border-slate-800 bg-slate-900/60 px-3 py-2">
                Column: {activeRepair.columnName}
              </div>
              {activeRepair.plan?.summary && (
                <div className="rounded-md border border-slate-800 bg-slate-900/60 px-3 py-2">
                  {activeRepair.plan.summary}
                </div>
              )}
              {activeRepair.plan?.row_id_column && (
                <div className="rounded-md border border-slate-800 bg-slate-900/60 px-3 py-2">
                  Row ID column: {activeRepair.plan.row_id_column}
                </div>
              )}
              {activeRepair.plan?.plan_id && (
                <div className="rounded-md border border-slate-800 bg-slate-900/60 px-3 py-2">
                  Plan ID: {activeRepair.plan.plan_id}
                </div>
              )}
              {activeRepair.plan?.snapshot?.signature && (
                <div className="rounded-md border border-slate-800 bg-slate-900/60 px-3 py-2">
                  Snapshot signature: {activeRepair.plan.snapshot.signature}
                </div>
              )}
              {Array.isArray(activeRepair.plan?.steps) && activeRepair.plan.steps.length > 0 && (
                <div className="rounded-md border border-slate-800 bg-slate-900/60 px-3 py-2 space-y-1">
                  {activeRepair.plan.steps.map((step: any, index: number) => (
                    <div key={index} className="text-[12px] text-slate-200">
                      {step.type === "null_repair" && (
                        <span>
                          Null repair ({step.strategy}) - ~{step.estimated_rows ?? 0} rows
                        </span>
                      )}
                      {step.type === "conflict_repair" && (
                        <span>
                          Conflict repair ({step.strategy}) - {step.estimated_groups ?? 0} groups
                        </span>
                      )}
                    </div>
                  ))}
                </div>
              )}
              {activeRepair.plan?.sql_previews?.null_repair?.update_sql && (
                <div className="rounded-md border border-slate-800 bg-slate-900/60 px-3 py-2">
                  <div className="mb-1 text-[11px] uppercase tracking-wide text-slate-400">
                    Null Repair SQL
                  </div>
                  <pre className="max-h-32 overflow-auto text-[11px] text-slate-200">
                    {activeRepair.plan.sql_previews.null_repair.update_sql}
                  </pre>
                </div>
              )}
              {activeRepair.plan?.sql_previews?.conflict_repair?.update_sql && (
                <div className="rounded-md border border-slate-800 bg-slate-900/60 px-3 py-2">
                  <div className="mb-1 text-[11px] uppercase tracking-wide text-slate-400">
                    Conflict Repair SQL
                  </div>
                  <pre className="max-h-32 overflow-auto text-[11px] text-slate-200">
                    {activeRepair.plan.sql_previews.conflict_repair.update_sql}
                  </pre>
                </div>
              )}
              {activeRepair.plan?.token_estimate && (
                <div className="rounded-md border border-slate-800 bg-slate-900/60 px-3 py-2">
                  Estimated tokens: {activeRepair.plan.token_estimate.token_count ?? 0}
                </div>
              )}
              <div className="space-y-1">
                <div className="text-xs text-slate-400">Approval note</div>
                <input
                  className="w-full rounded-md border border-slate-800 bg-slate-900/60 px-2 py-1 text-xs text-slate-100 focus:outline-none focus:ring-2 focus:ring-slate-700"
                  value={approvalNote}
                  onChange={(event) => setApprovalNote(event.target.value)}
                  placeholder="Reason for approval"
                />
              </div>
            </div>
          )}
          <AlertDialogFooter>
            <AlertDialogCancel
              onClick={() => {
                setRepairDialogOpen(false);
              }}
            >
              Cancel
            </AlertDialogCancel>
            <AlertDialogAction
              onClick={() => {
                if (!activeRepair) return;
                setRepairDialogOpen(false);
                if (typeof window !== "undefined") {
                  window.dispatchEvent(
                    new CustomEvent("column-workflow-approval", {
                      detail: {
                        tableAssetId: Number.parseInt(tableAsset.id, 10),
                        columnName: activeRepair.columnName,
                        note: approvalNote,
                        planId: activeRepair.plan?.plan_id,
                        planHash: activeRepair.plan?.plan_hash,
                        snapshotSignature: activeRepair.plan?.snapshot?.signature,
                      },
                    })
                  );
                }
              }}
            >
              Confirm & Run Repairs
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </div>
  );
};

export default OverviewTab;
