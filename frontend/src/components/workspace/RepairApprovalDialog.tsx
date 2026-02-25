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
import {
  Accordion,
  AccordionContent,
  AccordionItem,
  AccordionTrigger,
} from "@/components/ui/accordion";

export type RepairPlanItem = {
  columnName: string;
  plan: Record<string, any>;
  nullRate?: number;
  conflictRate?: number;
  conflicts?: Record<string, any>;
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
  const [showAllConflicts, setShowAllConflicts] = useState(false);

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
    setShowAllConflicts(false);
  }, [activeRepair]);


  const effectivePlan = localPlan ?? activeRepair?.plan ?? {};
  const conflictInfo =
    activeRepair?.conflicts ?? (effectivePlan?.conflicts as Record<string, any>) ?? null;
  const conflictSamples = Array.isArray(conflictInfo?.sample_groups)
    ? conflictInfo.sample_groups
    : [];
  const conflictSampleLimit = conflictInfo?.sample_group_limit ?? conflictSamples.length;
  const conflictValueLimit = conflictInfo?.sample_value_limit ?? 0;
  const valueConflicts = Array.isArray(conflictInfo?.value_conflicts)
    ? conflictInfo.value_conflicts
    : [];
  const totalConflictGroups = conflictInfo?.conflict_groups ?? 0;
  const conflictGroupCount = conflictInfo?.group_count ?? 0;
  const sampledRows =
    typeof conflictInfo?.sampled_rows === "number"
      ? conflictInfo.sampled_rows
      : typeof conflictInfo?.sample_size === "number"
        ? conflictInfo.sample_size
        : null;
  const sampleSizeRequested =
    typeof conflictInfo?.sample_size_requested === "number"
      ? conflictInfo.sample_size_requested
      : null;
  const sampleError =
    typeof conflictInfo?.sample_error === "string" && conflictInfo.sample_error.trim()
      ? conflictInfo.sample_error.trim()
      : null;
  const conflictRate =
    typeof conflictInfo?.conflict_rate === "number"
      ? Math.round(conflictInfo.conflict_rate * 10000) / 100
      : null;
  const maxDistinct = conflictInfo?.max_distinct ?? null;
  const groupByColumns = Array.isArray(conflictInfo?.group_by_columns)
    ? conflictInfo.group_by_columns
    : [];
  const likelyIdGroupBy = groupByColumns.some((col: string) =>
    /(^|_)(id|key|code)$/i.test(String(col || ""))
  );
  const likelyGroupingIssue =
    conflictRate !== null &&
    conflictRate >= 95 &&
    totalConflictGroups > 0 &&
    totalConflictGroups === conflictGroupCount &&
    (maxDistinct ?? 0) >= 5;
  const groupingHint = likelyGroupingIssue
    ? likelyIdGroupBy
      ? "Every sampled group conflicts, and group-by columns look ID-like. Consider grouping by a higher-level business key (e.g. model/segment/year) before auto repair."
      : "Every sampled group conflicts. Consider revisiting `group_by_columns` so groups reflect stable business entities before auto repair."
    : null;
  const visibleConflicts = showAllConflicts
    ? conflictSamples
    : conflictSamples.slice(0, 6);
  const conflictTypeLabel =
    conflictInfo?.type === "group_by_inconsistency"
      ? "Group consistency conflict"
      : conflictInfo?.type
        ? String(conflictInfo.type)
        : "Conflict scan";
  const nullStep = Array.isArray(effectivePlan?.steps)
    ? effectivePlan.steps.find((step: any) => step?.type === "null_repair")
    : null;
  const nullReason =
    nullStep?.reason || effectivePlan?.rationale?.nulls?.reason || null;
  const nullFillValue =
    nullStep?.fill_value ??
    effectivePlan?.rationale?.nulls?.fill_value ??
    null;

  return (
    <AlertDialog open={open} onOpenChange={onOpenChange}>
      <AlertDialogContent className="w-[min(1200px,95vw)] max-w-[1200px] max-h-[90vh] overflow-auto border border-slate-800/70 bg-slate-950/95 text-slate-100 shadow-2xl p-6">
        <AlertDialogHeader>
          <AlertDialogTitle className="text-slate-100 text-xl font-semibold">
            Approve Data Repairs
          </AlertDialogTitle>
          <AlertDialogDescription className="text-slate-400 text-sm">
            Review the plan details before applying changes.
          </AlertDialogDescription>
        </AlertDialogHeader>
        {activeRepair && (
          <div className="grid gap-6 text-sm text-slate-300 lg:grid-cols-[minmax(0,1.1fr)_minmax(0,0.9fr)]">
            <div className="space-y-4">
              <section className="rounded-xl border border-slate-800/80 bg-slate-900/50 px-3 py-3">
                <div className="flex items-start justify-between gap-4">
                  <div>
                    <div className="text-[11px] uppercase tracking-wide text-slate-400">
                      Plan summary
                    </div>
                    <div className="text-base font-semibold text-slate-100">
                      {activeRepair.columnName}
                    </div>
                  </div>
                  <span className="rounded-full border border-slate-700/70 bg-slate-950/60 px-2 py-1 text-[10px] text-slate-300">
                    {effectivePlan?.summary ? "Ready to review" : "Plan pending"}
                  </span>
                </div>
                {effectivePlan?.summary && (
                  <p className="mt-2 text-sm text-slate-200 break-words">
                    {effectivePlan.summary}
                  </p>
                )}
                {nullReason && (
                  <div className="mt-2 rounded-md border border-slate-800/80 bg-slate-950/50 px-2.5 py-2 text-xs text-slate-200">
                    <div className="text-[10px] uppercase tracking-wide text-slate-400">
                      Why this repair
                    </div>
                    <div className="mt-1 break-words whitespace-pre-wrap">
                      {nullReason}
                    </div>
                    {nullFillValue !== null && nullFillValue !== undefined && (
                      <div className="mt-1 text-[11px] text-slate-400">
                        Fill value: {String(nullFillValue)}
                      </div>
                    )}
                  </div>
                )}
                {Array.isArray(effectivePlan?.steps) &&
                  effectivePlan.steps.length > 0 && (
                    <div className="mt-3 grid gap-2 text-xs">
                      {effectivePlan.steps.map((step: any, index: number) => (
                        <div
                          key={index}
                          className="rounded-lg border border-slate-800 bg-slate-950/40 px-3 py-2 space-y-1 text-slate-200"
                        >
                          {step.type === "null_repair" && (
                            <>
                              <div className="font-medium text-slate-100">
                                Null repair ({step.strategy}) · ~{step.estimated_rows ?? 0} rows
                              </div>
                              {step.fill_value !== undefined && step.fill_value !== null && (
                                <div className="break-words">
                                  Fill value: {String(step.fill_value)}
                                </div>
                              )}
                              {step.reason && (
                                <div className="break-words text-slate-300">
                                  Reason: {step.reason}
                                </div>
                              )}
                              {step.basis?.method && (
                                <div className="text-slate-400">
                                  Basis: {step.basis.method}
                                </div>
                              )}
                            </>
                          )}
                          {step.type === "conflict_repair" && (
                            <>
                              <div className="font-medium text-slate-100">
                                Conflict repair ({step.strategy}) · {step.estimated_groups ?? 0} groups
                              </div>
                              {Array.isArray(step.group_by_columns) && step.group_by_columns.length > 0 && (
                                <div className="text-slate-300">
                                  Group by: {step.group_by_columns.join(", ")}
                                </div>
                              )}
                            </>
                          )}
                        </div>
                      ))}
                    </div>
                  )}
              </section>

              <section className="rounded-xl border border-slate-800/80 bg-gradient-to-br from-slate-900/70 via-slate-900/40 to-slate-950/80 px-3 py-3 text-xs text-slate-200 space-y-3">
              <div className="flex items-center justify-between">
                <div className="text-[11px] uppercase tracking-wide text-slate-400">
                  Conflict diagnostics
                </div>
                {conflictInfo?.skipped && (
                  <span className="rounded-full border border-amber-500/40 bg-amber-500/10 px-2 py-0.5 text-[10px] text-amber-200">
                    Scan skipped
                  </span>
                )}
                {conflictInfo?.error && (
                  <span className="rounded-full border border-rose-500/40 bg-rose-500/10 px-2 py-0.5 text-[10px] text-rose-200">
                    Scan error
                  </span>
                )}
                {!conflictInfo && (
                  <span className="rounded-full border border-slate-700/70 bg-slate-900/60 px-2 py-0.5 text-[10px] text-slate-300">
                    Not scanned
                  </span>
                )}
              </div>

              {!conflictInfo && (
                <div className="rounded-md border border-slate-800/60 bg-slate-900/40 px-2.5 py-2 text-[11px] text-slate-400">
                  No conflict scan results yet. Run `scan_conflicts` or set
                  `conflict_group_columns` to capture conflict diagnostics.
                </div>
              )}

              {conflictInfo?.skipped && (
                <div className="rounded-md border border-amber-500/30 bg-amber-500/5 px-2.5 py-2 text-[11px] text-amber-100">
                  Reason: {String(conflictInfo.reason || "missing_group_by_columns")}
                </div>
              )}

              {conflictInfo?.error && (
                <div className="rounded-md border border-rose-500/30 bg-rose-500/5 px-2.5 py-2 text-[11px] text-rose-100">
                  {String(conflictInfo.error)}
                </div>
              )}

              {conflictInfo && !conflictInfo?.skipped && !conflictInfo?.error && (
                <>
                  <div className="grid gap-2 sm:grid-cols-2">
                    <div className="rounded-lg border border-slate-800 bg-slate-950/40 px-2.5 py-2">
                      <div className="text-[10px] uppercase tracking-wide text-slate-500">
                        Conflict rate
                      </div>
                      <div className="text-sm text-slate-100">
                        {conflictRate !== null ? `${conflictRate}%` : "—"}
                      </div>
                      <div className="text-[11px] text-slate-500">
                        {totalConflictGroups} of {conflictGroupCount} groups
                      </div>
                    </div>
                    <div className="rounded-lg border border-slate-800 bg-slate-950/40 px-2.5 py-2">
                      <div className="text-[10px] uppercase tracking-wide text-slate-500">
                        Max distinct values
                      </div>
                      <div className="text-sm text-slate-100">
                        {maxDistinct ?? "—"}
                      </div>
                      <div className="text-[11px] text-slate-500">
                        Type: {conflictTypeLabel}
                      </div>
                    </div>
                  </div>

                  {Array.isArray(conflictInfo?.group_by_columns) &&
                    conflictInfo.group_by_columns.length > 0 && (
                      <div className="space-y-1">
                        <div className="text-[10px] uppercase tracking-wide text-slate-500">
                          Group by columns
                        </div>
                        <div className="flex flex-wrap gap-1">
                          {conflictInfo.group_by_columns.map((col: string) => (
                            <span
                              key={col}
                              className="rounded-full border border-slate-700/70 bg-slate-900/60 px-2 py-0.5 text-[11px] text-slate-200"
                            >
                              {col}
                            </span>
                          ))}
                        </div>
                      </div>
                    )}

                  {conflictInfo?.definition && (
                    <div className="rounded-md border border-slate-800 bg-slate-900/50 px-2.5 py-2 text-[11px] text-slate-300">
                      Diagnosis: {String(conflictInfo.definition)}
                    </div>
                  )}
                  {groupingHint && (
                    <div className="rounded-md border border-amber-500/40 bg-amber-500/10 px-2.5 py-2 text-[11px] text-amber-100 break-words">
                      {groupingHint}
                    </div>
                  )}
                  {valueConflicts.length > 0 && (
                    <div className="space-y-2">
                      <div className="text-[10px] uppercase tracking-wide text-slate-500">
                        Potential value conflicts
                      </div>
                      <div className="max-h-36 overflow-auto space-y-1 pr-1">
                        {valueConflicts.slice(0, 10).map((item: any, idx: number) => (
                          <div
                            key={`value-conflict-${idx}`}
                            className="rounded-md border border-slate-800/70 bg-slate-950/40 px-2 py-1 text-[11px] text-slate-200"
                          >
                            <span className="font-medium">{String(item?.value ?? "—")}</span>
                            {item?.likely_canonical ? (
                              <>
                                {" -> "}
                                <span className="font-medium">
                                  {String(item?.likely_canonical)}
                                </span>
                              </>
                            ) : null}
                            <span className="text-slate-400">
                              {" "}
                              ({String(item?.value_count ?? "—")}
                              {item?.canonical_count !== undefined &&
                              item?.canonical_count !== null
                                ? ` vs ${String(item?.canonical_count)}`
                                : ""}
                              )
                            </span>
                            {(item?.reason || item?.parsed_year !== undefined) && (
                              <div className="mt-1 text-[10px] text-slate-400">
                                {item?.reason
                                  ? String(item.reason).replace(/_/g, " ")
                                  : ""}
                                {item?.parsed_year !== undefined && item?.parsed_year !== null
                                  ? ` · parsed year: ${String(item.parsed_year)}`
                                  : ""}
                              </div>
                            )}
                          </div>
                        ))}
                      </div>
                    </div>
                  )}
                  {(sampledRows !== null || sampleSizeRequested !== null || conflictInfo?.sample_window_days) && (
                    <div className="text-[10px] text-slate-500">
                      Scanned {sampledRows ?? "—"} rows
                      {sampleSizeRequested !== null &&
                      sampledRows !== null &&
                      sampleSizeRequested > sampledRows
                        ? ` (requested up to ${sampleSizeRequested})`
                        : ""}
                      {conflictInfo.sample_window_days
                        ? ` in last ${conflictInfo.sample_window_days} days`
                        : ""}
                      .
                    </div>
                  )}
                  {sampleError && (
                    <div className="rounded-md border border-amber-500/30 bg-amber-500/5 px-2.5 py-2 text-[11px] text-amber-100 break-words">
                      Sample extraction error: {sampleError}
                    </div>
                  )}

                  {conflictSamples.length > 0 ? (
                    <div className="space-y-2">
                      <div className="flex items-center justify-between text-[10px] uppercase tracking-wide text-slate-500">
                        <span>Conflict samples</span>
                        <span>
                          Showing {visibleConflicts.length} of {conflictSamples.length}
                          {totalConflictGroups && totalConflictGroups > conflictSamples.length
                            ? ` (top ${conflictSampleLimit} of ${totalConflictGroups})`
                            : ""}
                        </span>
                      </div>
                      <div className="max-h-48 overflow-auto space-y-2 pr-1">
                        {visibleConflicts.map((sample: any, index: number) => {
                          const group = sample?.group || {};
                          const values = Array.isArray(sample?.values) ? sample.values : [];
                          return (
                            <div
                              key={`${index}-${JSON.stringify(group)}`}
                              className="rounded-lg border border-slate-800/70 bg-slate-950/40 px-2.5 py-2"
                            >
                              <div className="flex items-center justify-between text-[11px] text-slate-200">
                                <div className="flex flex-wrap gap-1">
                                  {Object.entries(group).map(([key, value]) => (
                                    <span
                                      key={key}
                                      className="rounded-md border border-slate-800 bg-slate-900/60 px-1.5 py-0.5 text-[10px] text-slate-200"
                                    >
                                      {key}: {String(value)}
                                    </span>
                                  ))}
                                </div>
                                <span className="text-[10px] text-slate-400">
                                  {sample?.distinct_values ?? "—"} distinct
                                </span>
                              </div>
                              {values.length > 0 && (
                                <div className="mt-2 flex flex-wrap gap-1">
                                  {values.map((entry: any, idx: number) => (
                                    <span
                                      key={idx}
                                      className="rounded-full border border-slate-700/60 bg-slate-900/70 px-2 py-0.5 text-[10px] text-slate-200"
                                    >
                                      {String(entry?.value ?? entry)}
                                      {entry?.count !== undefined && (
                                        <span className="ml-1 text-[10px] text-slate-400">
                                          ×{entry.count}
                                        </span>
                                      )}
                                    </span>
                                  ))}
                                </div>
                              )}
                              {values.length === 0 && (
                                <div className="mt-2 text-[10px] text-slate-500">
                                  No value samples available for this group.
                                  {conflictValueLimit
                                    ? ` Current value sample limit: ${conflictValueLimit}.`
                                    : ""}
                                </div>
                              )}
                            </div>
                          );
                        })}
                      </div>
                      {conflictSamples.length > 6 && (
                        <button
                          type="button"
                          className="text-[11px] text-slate-300 hover:text-slate-100"
                          onClick={() => setShowAllConflicts((prev) => !prev)}
                        >
                          {showAllConflicts ? "Show less" : "Show all samples"}
                        </button>
                      )}
                      {conflictValueLimit ? (
                        <div className="text-[10px] text-slate-500">
                          Showing up to {conflictValueLimit} values per group. Increase
                          `scan_conflicts_value_limit` to see more.
                        </div>
                      ) : null}
                    </div>
                  ) : (
                    <div className="rounded-md border border-slate-800/60 bg-slate-900/40 px-2.5 py-2 text-[11px] text-slate-400">
                      {totalConflictGroups
                        ? sampleError
                          ? "Conflicts detected, but sample extraction failed."
                          : "Conflicts detected, but no sample groups were returned."
                        : "No conflicting groups detected in the scan."}
                    </div>
                  )}
                </>
              )}
            </section>

            </div>

            <div className="space-y-4">
              <section className="rounded-xl border border-slate-800/80 bg-slate-900/60 px-3 py-3 text-xs text-slate-300">
                <div className="text-[11px] uppercase tracking-wide text-slate-400">
                  Plan metadata
                </div>
                <div className="mt-2 space-y-1 font-mono text-[11px] text-slate-200 break-all">
                  {effectivePlan?.plan_id && <div>Plan ID: {effectivePlan.plan_id}</div>}
                  {effectivePlan?.plan_hash && <div>Plan Hash: {effectivePlan.plan_hash}</div>}
                  {effectivePlan?.snapshot?.signature && (
                    <div>Snapshot: {effectivePlan.snapshot.signature}</div>
                  )}
                  {effectivePlan?.token_estimate && (
                    <div>
                      Estimated tokens: {effectivePlan.token_estimate.token_count ?? 0}
                    </div>
                  )}
                </div>
              </section>

              {effectivePlan?.rationale_report && (
                <section className="rounded-xl border border-slate-800 bg-slate-900/60 px-3 py-3 text-xs space-y-2 text-slate-200">
                  <div className="text-[11px] uppercase tracking-wide text-slate-400">
                    Repair rationale
                  </div>
                  {effectivePlan.rationale_report.summary && (
                    <div className="text-slate-100 break-words whitespace-pre-wrap">
                      {effectivePlan.rationale_report.summary}
                    </div>
                  )}
                  {effectivePlan.rationale_report.why_this_value && (
                    <div className="break-words whitespace-pre-wrap">
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
                </section>
              )}

              {(effectivePlan?.sql_previews?.null_repair?.update_sql ||
                effectivePlan?.sql_previews?.conflict_repair?.update_sql) && (
                <section className="rounded-xl border border-slate-800 bg-slate-900/60 px-3 py-2 text-xs text-slate-200">
                  <Accordion type="single" collapsible>
                    <AccordionItem value="sql">
                      <AccordionTrigger className="text-[11px] uppercase tracking-wide text-slate-400">
                        SQL preview
                      </AccordionTrigger>
                      <AccordionContent className="space-y-3">
                        {effectivePlan?.sql_previews?.null_repair?.update_sql && (
                          <div>
                            <div className="mb-1 text-[11px] uppercase tracking-wide text-slate-500">
                              Null repair
                            </div>
                            <pre className="max-h-48 overflow-auto whitespace-pre-wrap break-all rounded-md bg-slate-950 px-2 py-2 text-[11px] text-slate-100">
                              {effectivePlan.sql_previews.null_repair.update_sql}
                            </pre>
                          </div>
                        )}
                        {effectivePlan?.sql_previews?.conflict_repair?.update_sql && (
                          <div>
                            <div className="mb-1 text-[11px] uppercase tracking-wide text-slate-500">
                              Conflict repair
                            </div>
                            <pre className="max-h-48 overflow-auto whitespace-pre-wrap break-all rounded-md bg-slate-950 px-2 py-2 text-[11px] text-slate-100">
                              {effectivePlan.sql_previews.conflict_repair.update_sql}
                            </pre>
                          </div>
                        )}
                      </AccordionContent>
                    </AccordionItem>
                  </Accordion>
                </section>
              )}

              <section className="rounded-xl border border-slate-800 bg-slate-900/60 px-3 py-3 text-xs text-slate-200 space-y-2">
                <div className="text-[11px] uppercase tracking-wide text-slate-400">
                  Apply target
                </div>
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
                <div className="text-[11px] uppercase tracking-wide text-slate-400">
                  Approval note
                </div>
                <input
                  className="w-full rounded-md border border-slate-800 bg-slate-950/80 px-2 py-1 text-xs text-slate-100 focus:outline-none focus:ring-2 focus:ring-slate-600"
                  value={approvalNote}
                  onChange={(event) => setApprovalNote(event.target.value)}
                  placeholder="Reason for approval"
                  disabled={isApplying}
                />
              </section>
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
