import { useEffect, useState } from "react";
import { columnMetadataApi } from "@/api/columnMetadata";
import { columnWorkflowsApi } from "@/api/columnWorkflows";
import { useToast } from "@/hooks/use-toast";
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

export type RepairPlanItem = {
  columnName: string;
  plan: Record<string, any>;
  nullRate?: number;
  conflictRate?: number;
};

type RepairApprovalDialogProps = {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  tableId: number;
  activeRepair: RepairPlanItem | null;
  onApplied?: () => void | Promise<void>;
};

const RepairApprovalDialog = ({
  open,
  onOpenChange,
  tableId,
  activeRepair,
  onApplied,
}: RepairApprovalDialogProps) => {
  const { toast } = useToast();
  const [approvalNote, setApprovalNote] = useState("");
  const [applyTarget, setApplyTarget] = useState<"fixing_table" | "source_table">(
    "fixing_table"
  );
  const [isApplying, setIsApplying] = useState(false);
  const [localPlan, setLocalPlan] = useState<Record<string, any> | null>(null);

  useEffect(() => {
    if (!activeRepair?.plan) {
      setLocalPlan(null);
      return;
    }
    setLocalPlan({ ...activeRepair.plan });
    if (activeRepair.plan.apply_mode === "source_table") {
      setApplyTarget("source_table");
    } else {
      setApplyTarget("fixing_table");
    }
  }, [activeRepair]);

  const effectivePlan = localPlan ?? activeRepair?.plan ?? {};

  return (
    <AlertDialog open={open} onOpenChange={onOpenChange}>
      <AlertDialogContent className="max-h-[85vh] overflow-auto border border-slate-800/70 bg-slate-950/95 text-slate-100 shadow-2xl">
        <AlertDialogHeader>
          <AlertDialogTitle className="text-slate-100">
            Approve Data Repairs
          </AlertDialogTitle>
          <AlertDialogDescription className="text-slate-400">
            Review the plan details before applying changes.
          </AlertDialogDescription>
        </AlertDialogHeader>
        {activeRepair && (
          <div className="space-y-3 text-sm text-slate-300">
            <div className="font-medium text-slate-100">
              Column: {activeRepair.columnName}
            </div>
            {effectivePlan?.summary && (
              <div className="rounded-md border border-slate-800 bg-slate-900/60 px-3 py-2 text-slate-100">
                {effectivePlan.summary}
              </div>
            )}
            <div className="grid gap-2 text-xs">
              {effectivePlan?.plan_id && (
                <div>Plan ID: {effectivePlan.plan_id}</div>
              )}
              {effectivePlan?.plan_hash && (
                <div>Plan Hash: {effectivePlan.plan_hash}</div>
              )}
              {effectivePlan?.snapshot?.signature && (
                <div>Snapshot signature: {effectivePlan.snapshot.signature}</div>
              )}
              {effectivePlan?.token_estimate && (
                <div>
                  Estimated tokens: {effectivePlan.token_estimate.token_count ?? 0}
                </div>
              )}
            </div>
            {Array.isArray(effectivePlan?.steps) &&
              effectivePlan.steps.length > 0 && (
                <div className="space-y-1 text-xs">
                  {effectivePlan.steps.map((step: any, index: number) => (
                    <div
                      key={index}
                      className="rounded-md border border-slate-800 bg-slate-900/50 px-2 py-1 space-y-1 text-slate-200"
                    >
                      {step.type === "null_repair" && (
                        <>
                          <div className="font-medium text-slate-100">
                            Null repair ({step.strategy}) · ~{step.estimated_rows ?? 0} rows
                          </div>
                          {step.fill_value !== undefined && step.fill_value !== null && (
                            <div>Fill value: {String(step.fill_value)}</div>
                          )}
                          {step.reason && <div>Reason: {step.reason}</div>}
                          {step.basis?.method && (
                            <div>Basis: {step.basis.method}</div>
                          )}
                        </>
                      )}
                      {step.type === "conflict_repair" && (
                        <>
                          <div className="font-medium text-slate-100">
                            Conflict repair ({step.strategy}) · {step.estimated_groups ?? 0} groups
                          </div>
                          {Array.isArray(step.group_by_columns) && step.group_by_columns.length > 0 && (
                            <div>Group by: {step.group_by_columns.join(", ")}</div>
                          )}
                        </>
                      )}
                    </div>
                  ))}
                </div>
              )}
            {effectivePlan?.rationale?.nulls?.reason && (
              <div className="rounded-md border border-slate-800 bg-slate-900/60 px-3 py-2 text-xs text-slate-200">
                <div className="mb-1 text-[11px] uppercase tracking-wide text-slate-400">
                  Why this repair?
                </div>
                <div>{effectivePlan.rationale.nulls.reason}</div>
              </div>
            )}
            {effectivePlan?.rationale_report && (
              <div className="rounded-md border border-slate-800 bg-slate-900/60 px-3 py-2 text-xs space-y-2 text-slate-200">
                <div className="text-[11px] uppercase tracking-wide text-slate-400">
                  Repair rationale
                </div>
                {effectivePlan.rationale_report.summary && (
                  <div className="text-slate-100">{effectivePlan.rationale_report.summary}</div>
                )}
                {effectivePlan.rationale_report.why_this_value && (
                  <div>
                    <span className="font-medium text-slate-100">Why this value:</span>{" "}
                    {effectivePlan.rationale_report.why_this_value}
                  </div>
                )}
                {Array.isArray(effectivePlan.rationale_report.row_level_rules) &&
                  effectivePlan.rationale_report.row_level_rules.length > 0 && (
                    <div>
                      <div className="font-medium text-slate-100">Row-level rules</div>
                      <ul className="mt-1 list-disc space-y-1 pl-4">
                        {effectivePlan.rationale_report.row_level_rules.map(
                          (rule: string, idx: number) => (
                            <li key={idx}>{rule}</li>
                          )
                        )}
                      </ul>
                    </div>
                  )}
                {Array.isArray(effectivePlan.rationale_report.alternatives) &&
                  effectivePlan.rationale_report.alternatives.length > 0 && (
                    <div>
                      <div className="font-medium text-slate-100">Alternatives</div>
                      <ul className="mt-1 list-disc space-y-1 pl-4">
                        {effectivePlan.rationale_report.alternatives.map(
                          (rule: string, idx: number) => (
                            <li key={idx}>{rule}</li>
                          )
                        )}
                      </ul>
                    </div>
                  )}
              </div>
            )}
            {effectivePlan?.sql_previews?.null_repair?.update_sql && (
              <div className="rounded-md border border-slate-800 bg-slate-900/60 px-3 py-2">
                <div className="mb-1 text-[11px] uppercase tracking-wide text-slate-400">
                  Null Repair SQL
                </div>
                <pre className="max-h-32 overflow-auto rounded-md bg-slate-950 px-2 py-2 text-[11px] text-slate-100">
                  {effectivePlan.sql_previews.null_repair.update_sql}
                </pre>
              </div>
            )}
            {effectivePlan?.sql_previews?.conflict_repair?.update_sql && (
              <div className="rounded-md border border-slate-800 bg-slate-900/60 px-3 py-2">
                <div className="mb-1 text-[11px] uppercase tracking-wide text-slate-400">
                  Conflict Repair SQL
                </div>
                <pre className="max-h-32 overflow-auto rounded-md bg-slate-950 px-2 py-2 text-[11px] text-slate-100">
                  {effectivePlan.sql_previews.conflict_repair.update_sql}
                </pre>
              </div>
            )}
            <div className="space-y-1">
              <div className="text-xs text-slate-400">Apply target</div>
              <select
                className="w-full rounded-md border border-slate-800 bg-slate-950/80 px-2 py-1 text-xs text-slate-100 focus:outline-none focus:ring-2 focus:ring-slate-600"
                value={applyTarget}
                onChange={(event) =>
                  setApplyTarget(event.target.value as "fixing_table" | "source_table")
                }
                disabled={isApplying}
              >
                <option value="fixing_table">Write to fixing table (recommended)</option>
                <option value="source_table">Write to original table</option>
              </select>
            </div>
            <div className="space-y-1">
              <div className="text-xs text-slate-400">Approval note</div>
              <input
                className="w-full rounded-md border border-slate-800 bg-slate-950/80 px-2 py-1 text-xs text-slate-100 focus:outline-none focus:ring-2 focus:ring-slate-600"
                value={approvalNote}
                onChange={(event) => setApprovalNote(event.target.value)}
                placeholder="Reason for approval"
                disabled={isApplying}
              />
            </div>
          </div>
        )}
        <AlertDialogFooter>
          <AlertDialogCancel
            onClick={() => {
              onOpenChange(false);
            }}
            disabled={isApplying}
          >
            Cancel
          </AlertDialogCancel>
          <AlertDialogAction
            disabled={isApplying || !activeRepair || Number.isNaN(tableId)}
            onClick={() => {
              if (!activeRepair || Number.isNaN(tableId)) return;
              void (async () => {
                setIsApplying(true);
                try {
                  const latest = await columnMetadataApi.get(tableId);
                  const latestColumn = latest.data?.columns?.find(
                    (col) => col.column_name === activeRepair.columnName
                  );
                  const latestPlan =
                    (latestColumn?.metadata as Record<string, any> | undefined)
                      ?.analysis?.repair_plan ?? {};
                  const latestPlanId = latestPlan?.plan_id as string | undefined;
                  const latestPlanHash = latestPlan?.plan_hash as string | undefined;
                  const latestSnapshotSignature = latestPlan?.snapshot?.signature as
                    | string
                    | undefined;

                  if (
                    (effectivePlan?.plan_id &&
                      latestPlanId &&
                      effectivePlan.plan_id !== latestPlanId) ||
                    (effectivePlan?.plan_hash &&
                      latestPlanHash &&
                      effectivePlan.plan_hash !== latestPlanHash) ||
                    (effectivePlan?.snapshot?.signature &&
                      latestSnapshotSignature &&
                      effectivePlan.snapshot.signature !== latestSnapshotSignature)
                  ) {
                    setLocalPlan(latestPlan);
                    toast({
                      title: "Plan updated",
                      description:
                        "The repair plan changed. Review the updated plan and confirm again.",
                    });
                    return;
                  }

                  const planId = latestPlanId ?? effectivePlan?.plan_id;
                  const planHash = latestPlanHash ?? effectivePlan?.plan_hash;
                  const snapshotSignature =
                    latestSnapshotSignature ?? effectivePlan?.snapshot?.signature;

                  if (!planId || !planHash || !snapshotSignature) {
                    toast({
                      title: "Approval failed",
                      description:
                        "Repair plan details are missing. Re-run the repair plan and try again.",
                      variant: "destructive",
                    });
                    return;
                  }

                  await columnMetadataApi.override(tableId, activeRepair.columnName, {
                    data_fix_approved: true,
                    data_fix_note: approvalNote,
                    data_fix_target: applyTarget,
                    data_fix_plan_id: planId,
                    data_fix_plan_hash: planHash,
                    data_fix_snapshot_signature: snapshotSignature,
                  });
                  const applyTool =
                    applyTarget === "fixing_table"
                      ? "apply_data_repairs_to_fixing_table"
                      : "apply_data_repairs";
                  const response = await columnWorkflowsApi.runSelected(
                    tableId,
                    activeRepair.columnName,
                    {
                      focus: "repairs",
                      tool_calls: [
                        {
                          tool_name: applyTool,
                          input: {
                            plan_id: planId,
                            plan_hash: planHash,
                            snapshot_signature: snapshotSignature,
                          },
                        },
                      ],
                    }
                  );
                  const applyResult = response.data?.results?.find(
                    (item: any) => item?.tool_name === applyTool
                  );
                  if (applyResult?.result?.skipped) {
                    if (applyResult.result.reason === "snapshot_mismatch") {
                      const refreshed = await columnMetadataApi.get(tableId);
                      const refreshedColumn = refreshed.data?.columns?.find(
                        (col) => col.column_name === activeRepair.columnName
                      );
                      const refreshedPlan =
                        (refreshedColumn?.metadata as Record<string, any> | undefined)
                          ?.analysis?.repair_plan ?? null;
                      setLocalPlan(refreshedPlan ? { ...refreshedPlan } : null);
                      toast({
                        title: "Plan updated",
                        description:
                          "The underlying data changed. Review the updated plan and confirm again.",
                        variant: "destructive",
                      });
                      return;
                    }
                    toast({
                      title: "Repairs not applied",
                      description:
                        applyResult.result.reason ||
                        "Approval or plan validation failed.",
                      variant: "destructive",
                    });
                  } else {
                    const targetTable = applyResult?.result?.target_table;
                    toast({
                      title: "Repairs applied",
                      description: targetTable
                        ? `Applied fixes for ${activeRepair.columnName}. Created ${targetTable}.`
                        : `Applied fixes for ${activeRepair.columnName}.`,
                    });
                  }
                  if (onApplied) {
                    await onApplied();
                  }
                  if (typeof window !== "undefined") {
                    window.dispatchEvent(
                      new CustomEvent("workflow-outputs-refresh", {
                        detail: { tableAssetId: tableId },
                      })
                    );
                  }
                  onOpenChange(false);
                } catch (error) {
                  toast({
                    title: "Failed to apply repairs",
                    description:
                      error instanceof Error ? error.message : "Unknown error",
                    variant: "destructive",
                  });
                } finally {
                  setIsApplying(false);
                }
              })();
            }}
          >
            Confirm & Apply Repairs
          </AlertDialogAction>
        </AlertDialogFooter>
      </AlertDialogContent>
    </AlertDialog>
  );
};

export default RepairApprovalDialog;
