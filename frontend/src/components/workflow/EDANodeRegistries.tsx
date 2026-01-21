/**
 * EDA Node Registries
 */

import { WorkflowNodeRegistry, ValidateTrigger, Field } from '@flowgram.ai/free-layout-editor';
import { Input } from '@/components/ui/input';
import { Textarea } from '@/components/ui/textarea';
import { EDANodeType, EDA_NODE_DEFINITIONS } from '@/types/eda-workflow';

/**
 * Create EDA node registries for Flowgram
 */
export function createEDANodeRegistries(): WorkflowNodeRegistry[] {
  return [
    // Data Source Node (Start)
    {
      type: 'data_source',
      meta: {
        isStart: true,
        deleteDisable: true,
        copyDisable: true,
        defaultPorts: [{ type: 'output', location: 'right' }],
      },
      formMeta: {
        validateTrigger: ValidateTrigger.onChange,
        validate: {
          table_asset_id: ({ value }) => (value ? undefined : 'Table asset is required'),
        },
        render: () => (
          <>
            <Field name="title">
              {({ field }) => (
                <div className="text-sm font-medium text-slate-900 mb-2">
                  {field.value || 'Data Source'}
                </div>
              )}
            </Field>
            <Field name="table_name">
              {({ field }) => (
                <div className="text-xs text-slate-700">
                  {field.value || 'No table selected'}
                </div>
              )}
            </Field>
          </>
        ),
      },
    },

    // Profile Table Node
    {
      type: 'profile_table',
      meta: {
        defaultPorts: [
          { type: 'input', location: 'left' },
          { type: 'output', location: 'right' },
        ],
      },
      formMeta: {
        render: () => (
          <>
            <Field name="title">
              {({ field }) => (
                <div className="text-sm font-medium text-slate-900 mb-2">
                  {field.value || 'Profile Table'}
                </div>
              )}
            </Field>
            <Field name="sample_size">
              {({ field }) => (
                <div className="text-xs text-slate-700">
                  Sample: {field.value || 100} rows
                </div>
              )}
            </Field>
            <Field name="include_type_inference">
              {({ field }) => (
                <div className="text-xs text-slate-700">
                  {field.value ? '✓ Type inference enabled' : '✗ Type inference disabled'}
                </div>
              )}
            </Field>
          </>
        ),
      },
    },

    // Generate Insights Node
    {
      type: 'generate_insights',
      meta: {
        defaultPorts: [
          { type: 'input', location: 'left' },
          { type: 'output', location: 'right' },
        ],
      },
      formMeta: {
        render: () => (
          <>
            <Field name="title">
              {({ field }) => (
                <div className="text-sm font-medium text-slate-900 mb-2">
                  {field.value || 'Generate Insights'}
                </div>
              )}
            </Field>
            <Field name="focus">
              {({ field }) => (
                <div className="text-xs text-slate-700">
                  Focus: {field.value || 'general'}
                </div>
              )}
            </Field>
          </>
        ),
      },
    },

    // Generate Charts Node
    {
      type: 'generate_charts',
      meta: {
        defaultPorts: [
          { type: 'input', location: 'left' },
          { type: 'output', location: 'right' },
        ],
      },
      formMeta: {
        render: () => (
          <>
            <Field name="title">
              {({ field }) => (
                <div className="text-sm font-medium text-slate-900 mb-2">
                  {field.value || 'Generate Charts'}
                </div>
              )}
            </Field>
            <Field name="chart_count">
              {({ field }) => (
                <div className="text-xs text-slate-700">
                  Charts: {field.value || 3}
                </div>
              )}
            </Field>
          </>
        ),
      },
    },

    // Generate Documentation Node
    {
      type: 'generate_documentation',
      meta: {
        defaultPorts: [
          { type: 'input', location: 'left' },
          { type: 'output', location: 'right' },
        ],
      },
      formMeta: {
        render: () => (
          <>
            <Field name="title">
              {({ field }) => (
                <div className="text-sm font-medium text-slate-900 mb-2">
                  {field.value || 'Generate Documentation'}
                </div>
              )}
            </Field>
            <div className="text-xs text-slate-700">
              Complete documentation
            </div>
          </>
        ),
      },
    },

    // Numeric Distribution
    {
      type: 'numeric_distribution',
      meta: {
        defaultPorts: [
          { type: 'input', location: 'left' },
          { type: 'output', location: 'right' },
        ],
      },
      formMeta: {
        render: () => (
          <>
            <Field name="title">
              {({ field }) => (
                <div className="text-sm font-medium text-slate-900 mb-2">
                  {field.value || 'Numeric Distribution'}
                </div>
              )}
            </Field>
            <Field name="sample_size">
              {({ field }) => (
                <div className="text-xs text-slate-700">
                  Sample: {field.value || 10000} rows
                </div>
              )}
            </Field>
          </>
        ),
      },
    },

    // Numeric Correlations
    {
      type: 'numeric_correlations',
      meta: {
        defaultPorts: [
          { type: 'input', location: 'left' },
          { type: 'output', location: 'right' },
        ],
      },
      formMeta: {
        render: () => (
          <>
            <Field name="title">
              {({ field }) => (
                <div className="text-sm font-medium text-slate-900 mb-2">
                  {field.value || 'Correlation Scan'}
                </div>
              )}
            </Field>
            <Field name="max_columns">
              {({ field }) => (
                <div className="text-xs text-slate-700">
                  Max columns: {field.value || 12}
                </div>
              )}
            </Field>
          </>
        ),
      },
    },

    // Numeric Periodicity
    {
      type: 'numeric_periodicity',
      meta: {
        defaultPorts: [
          { type: 'input', location: 'left' },
          { type: 'output', location: 'right' },
        ],
      },
      formMeta: {
        render: () => (
          <>
            <Field name="title">
              {({ field }) => (
                <div className="text-sm font-medium text-slate-900 mb-2">
                  {field.value || 'Periodicity Scan'}
                </div>
              )}
            </Field>
            <Field name="bucket">
              {({ field }) => (
                <div className="text-xs text-slate-700">
                  Bucket: {field.value || 'day'}
                </div>
              )}
            </Field>
          </>
        ),
      },
    },

    // Categorical Groups
    {
      type: 'categorical_groups',
      meta: {
        defaultPorts: [
          { type: 'input', location: 'left' },
          { type: 'output', location: 'right' },
        ],
      },
      formMeta: {
        render: () => (
          <>
            <Field name="title">
              {({ field }) => (
                <div className="text-sm font-medium text-slate-900 mb-2">
                  {field.value || 'Category Groups'}
                </div>
              )}
            </Field>
            <Field name="top_n">
              {({ field }) => (
                <div className="text-xs text-slate-700">
                  Top N: {field.value || 10}
                </div>
              )}
            </Field>
          </>
        ),
      },
    },

    // Null Scan
    {
      type: 'scan_nulls',
      meta: {
        defaultPorts: [
          { type: 'input', location: 'left' },
          { type: 'output', location: 'right' },
        ],
      },
      formMeta: {
        render: () => (
          <>
            <Field name="title">
              {({ field }) => (
                <div className="text-sm font-medium text-slate-900 mb-2">
                  {field.value || 'Null Scan'}
                </div>
              )}
            </Field>
            <Field name="sample_size">
              {({ field }) => (
                <div className="text-xs text-slate-700">
                  Sample: {field.value || 20000} rows
                </div>
              )}
            </Field>
          </>
        ),
      },
    },

    // Conflict Scan
    {
      type: 'scan_conflicts',
      meta: {
        defaultPorts: [
          { type: 'input', location: 'left' },
          { type: 'output', location: 'right' },
        ],
      },
      formMeta: {
        render: () => (
          <>
            <Field name="title">
              {({ field }) => (
                <div className="text-sm font-medium text-slate-900 mb-2">
                  {field.value || 'Conflict Scan'}
                </div>
              )}
            </Field>
            <Field name="group_by_columns">
              {({ field }) => (
                <div className="text-xs text-slate-700">
                  Groups: {field.value || 'Not set'}
                </div>
              )}
            </Field>
          </>
        ),
      },
    },

    // Plan Data Repairs
    {
      type: 'plan_data_repairs',
      meta: {
        defaultPorts: [
          { type: 'input', location: 'left' },
          { type: 'output', location: 'right' },
        ],
      },
      formMeta: {
        render: () => (
          <>
            <Field name="title">
              {({ field }) => (
                <div className="text-sm font-medium text-slate-900 mb-2">
                  {field.value || 'Repair Plan'}
                </div>
              )}
            </Field>
            <div className="text-xs text-slate-700">
              Generates a preview plan before applying fixes.
            </div>
          </>
        ),
      },
    },

    // Approval Gate
    {
      type: 'approval_gate',
      meta: {
        defaultPorts: [
          { type: 'input', location: 'left' },
        ],
      },
      formMeta: {
        render: () => (
          <>
            <Field name="title">
              {({ field }) => (
                <div className="text-sm font-medium text-slate-900 mb-2">
                  {field.value || 'Approval Gate'}
                </div>
              )}
            </Field>
            <Field name="approved">
              {({ field }) => (
                <div className="text-xs text-slate-700">
                  {field.value ? '✓ Approved' : 'Pending approval'}
                </div>
              )}
            </Field>
          </>
        ),
      },
    },

    // Apply Data Repairs
    {
      type: 'apply_data_repairs',
      meta: {
        defaultPorts: [
          { type: 'input', location: 'left' },
        ],
      },
      formMeta: {
        render: () => (
          <>
            <Field name="title">
              {({ field }) => (
                <div className="text-sm font-medium text-slate-900 mb-2">
                  {field.value || 'Apply Repairs'}
                </div>
              )}
            </Field>
            <div className="text-xs text-slate-700">
              Applies approved null/conflict fixes.
            </div>
          </>
        ),
      },
    },

    // Generate Visuals Node (column-level)
    {
      type: 'generate_visuals',
      meta: {
        defaultPorts: [
          { type: 'input', location: 'left' },
          { type: 'output', location: 'right' },
        ],
      },
      formMeta: {
        render: () => (
          <>
            <Field name="title">
              {({ field }) => (
                <div className="text-sm font-medium text-slate-900 mb-2">
                  {field.value || 'Generate Visuals'}
                </div>
              )}
            </Field>
            <Field name="chart_type">
              {({ field }) => (
                <div className="text-[11px] text-slate-700 uppercase tracking-wide">
                  {field.value ? `${field.value} chart` : 'Chart'}
                </div>
              )}
            </Field>
            <Field name="x_column">
              {({ field }) => (
                <div className="text-xs text-slate-700">
                  X: {field.value || 'auto'}
                </div>
              )}
            </Field>
            <Field name="y_column">
              {({ field }) => (
                <div className="text-xs text-slate-700">
                  Y: {field.value || 'auto'}
                </div>
              )}
            </Field>
            <Field name="expanded">
              {({ field: expandedField }) =>
                expandedField.value ? (
                  <div className="mt-2 space-y-2">
                    <Field name="column_name">
                      {({ field }) => (
                        <Input
                          className="text-xs"
                          placeholder="Attach to column"
                          value={field.value || ''}
                          onChange={(event) => field.onChange(event.target.value)}
                        />
                      )}
                    </Field>
                    <Field name="chart_type">
                      {({ field }) => (
                        <select
                          className="w-full rounded-md border border-slate-200 bg-white px-2 py-1 text-xs text-slate-900"
                          value={field.value || 'bar'}
                          onChange={(event) => field.onChange(event.target.value)}
                        >
                          <option value="bar">bar</option>
                          <option value="line">line</option>
                          <option value="area">area</option>
                          <option value="pie">pie</option>
                        </select>
                      )}
                    </Field>
                    <Field name="x_column">
                      {({ field }) => (
                        <Input
                          className="text-xs"
                          placeholder="X column"
                          value={field.value || ''}
                          onChange={(event) => field.onChange(event.target.value)}
                        />
                      )}
                    </Field>
                    <Field name="y_column">
                      {({ field }) => (
                        <Input
                          className="text-xs"
                          placeholder="Y column or count"
                          value={field.value || ''}
                          onChange={(event) => field.onChange(event.target.value)}
                        />
                      )}
                    </Field>
                  </div>
                ) : null
              }
            </Field>
          </>
        ),
      },
    },

    // Summarize Text Node
    {
      type: 'summarize_text',
      meta: {
        defaultPorts: [
          { type: 'input', location: 'left' },
          { type: 'output', location: 'right' },
        ],
      },
      formMeta: {
        render: () => (
          <>
            <Field name="title">
              {({ field }) => (
                <div className="text-sm font-medium text-slate-900 mb-2">
                  {field.value || 'Summarize Text'}
                </div>
              )}
            </Field>
            <div className="text-xs text-slate-600">AI_SUMMARIZE_AGG</div>
          </>
        ),
      },
    },

    // Row-level Extract Node
    {
      type: 'row_level_extract',
      meta: {
        defaultPorts: [
          { type: 'input', location: 'left' },
          { type: 'output', location: 'right' },
        ],
      },
      formMeta: {
        render: () => (
          <>
            <Field name="title">
              {({ field }) => (
                <div className="text-sm font-medium text-slate-900 mb-2">
                  {field.value || 'Row-level Extract'}
                </div>
              )}
            </Field>
            <Field name="output_column">
              {({ field }) => (
                <Input
                  className="text-xs"
                  placeholder="Output column (optional)"
                  value={field.value || ''}
                  onChange={(event) => field.onChange(event.target.value)}
                />
              )}
            </Field>
            <Field name="instruction">
              {({ field }) => (
                <Textarea
                  className="text-xs"
                  placeholder="Extraction instruction"
                  value={field.value || ''}
                  onChange={(event) => field.onChange(event.target.value)}
                />
              )}
            </Field>
            <Field name="response_schema">
              {({ field }) => (
                <Textarea
                  className="text-xs"
                  placeholder="Optional JSON schema for structured output"
                  value={field.value || ''}
                  onChange={(event) => field.onChange(event.target.value)}
                />
              )}
            </Field>
          </>
        ),
      },
    },

    // Describe Images Node
    {
      type: 'describe_images',
      meta: {
        defaultPorts: [
          { type: 'input', location: 'left' },
          { type: 'output', location: 'right' },
        ],
      },
      formMeta: {
        render: () => (
          <>
            <Field name="title">
              {({ field }) => (
                <div className="text-sm font-medium text-slate-900 mb-2">
                  {field.value || 'Describe Images'}
                </div>
              )}
            </Field>
            <Field name="output_column">
              {({ field }) => (
                <Input
                  className="text-xs"
                  placeholder="Output column (optional)"
                  value={field.value || ''}
                  onChange={(event) => field.onChange(event.target.value)}
                />
              )}
            </Field>
            <Field name="image_stage">
              {({ field }) => (
                <Input
                  className="text-xs"
                  placeholder="Image stage (e.g. @my_stage)"
                  value={field.value || ''}
                  onChange={(event) => field.onChange(event.target.value)}
                />
              )}
            </Field>
            <Field name="image_path_prefix">
              {({ field }) => (
                <Input
                  className="text-xs"
                  placeholder="Path prefix (optional)"
                  value={field.value || ''}
                  onChange={(event) => field.onChange(event.target.value)}
                />
              )}
            </Field>
            <Field name="image_path_suffix">
              {({ field }) => (
                <Input
                  className="text-xs"
                  placeholder="Path suffix (optional)"
                  value={field.value || ''}
                  onChange={(event) => field.onChange(event.target.value)}
                />
              )}
            </Field>
            <Field name="image_model">
              {({ field }) => (
                <Input
                  className="text-xs"
                  placeholder="Image model (optional)"
                  value={field.value || ''}
                  onChange={(event) => field.onChange(event.target.value)}
                />
              )}
            </Field>
          </>
        ),
      },
    },

    // Basic Stats Node
    {
      type: 'basic_stats',
      meta: {
        defaultPorts: [
          { type: 'input', location: 'left' },
          { type: 'output', location: 'right' },
        ],
      },
      formMeta: {
        render: () => (
          <>
            <Field name="title">
              {({ field }) => (
                <div className="text-sm font-medium text-slate-900 mb-2">
                  {field.value || 'Basic Stats'}
                </div>
              )}
            </Field>
            <div className="text-xs text-slate-600">Counts & nulls</div>
          </>
        ),
      },
    },

    // Column Hint Node
    {
      type: 'column_hint',
      meta: {
        defaultPorts: [
          { type: 'input', location: 'left' },
          { type: 'output', location: 'right' },
        ],
      },
      formMeta: {
        render: () => (
          <>
            <Field name="title">
              {({ field }) => (
                <div className="text-sm font-medium text-slate-900 mb-2">
                  {field.value || 'Column Hint'}
                </div>
              )}
            </Field>
            <Field name="hint">
              {({ field }) => (
                <Textarea
                  className="text-xs"
                  placeholder="Optional semantic hint (e.g., user tier)"
                  value={field.value || ''}
                  onChange={(event) => field.onChange(event.target.value)}
                />
              )}
            </Field>
          </>
        ),
      },
    },

    // Export Node (End)
    {
      type: 'export',
      meta: {
        deleteDisable: true,
        copyDisable: true,
        defaultPorts: [{ type: 'input', location: 'left' }],
      },
      formMeta: {
        render: () => (
          <>
            <Field name="title">
              {({ field }) => (
                <div className="text-sm font-medium text-slate-900 mb-2">
                  {field.value || 'Export Results'}
                </div>
              )}
            </Field>
            <Field name="format">
              {({ field }) => (
                <div className="text-xs text-slate-700">
                  Format: {field.value || 'json'}
                </div>
              )}
            </Field>
          </>
        ),
      },
    },
    // Comment Node
    {
      type: 'comment',
      meta: {
        defaultPorts: [],
        size: {
          width: 260,
          height: 160,
        },
      },
      formMeta: {
        render: () => (
          <Field name="note">
            {({ field }) => (
              <textarea
                className="w-full min-h-[96px] rounded-md border border-amber-200 bg-amber-50/70 p-2 text-sm text-slate-700 focus:outline-none focus:ring-2 focus:ring-amber-200"
                placeholder="Add a note..."
                value={field.value || ''}
                onChange={(event) => field.onChange(event.target.value)}
              />
            )}
          </Field>
        ),
      },
    },
    // Agent Step
    {
      type: 'agent_step',
      meta: {
        defaultPorts: [
          { type: 'input', location: 'left' },
          { type: 'output', location: 'right' },
        ],
      },
      formMeta: {
        render: () => (
          <>
            <Field name="tool_name">
              {({ field }) => (
                <div className="text-xs text-slate-700">
                  Tool: {field.value || 'agent_step'}
                </div>
              )}
            </Field>
            <div className="text-[11px] text-slate-500">
              Strands agent tool invocation.
            </div>
          </>
        ),
      },
    },
  ];
}
