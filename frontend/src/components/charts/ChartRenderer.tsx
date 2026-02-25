import { Fragment, memo, useMemo } from "react";
import {
  BarChart,
  Bar,
  LineChart,
  Line,
  PieChart,
  Pie,
  AreaChart,
  Area,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Legend,
  Cell,
} from "recharts";
import { CHART_COLORS, ChartSpec } from "@/lib/chartUtils";

interface ChartRendererProps {
  spec: ChartSpec;
  showAxes?: boolean;
}

const ChartRenderer = memo(({ spec, showAxes = true }: ChartRendererProps) => {
  const { chartType, xKey, yKey } = spec;
  const data = Array.isArray(spec.data) ? spec.data : [];
  const series = Array.isArray(spec.series) && spec.series.length > 0 ? spec.series : null;
  const seriesKeys = series ? series.map((entry) => entry.key) : [yKey];
  const formatAxisTitle = (value: string) =>
    value
      .replace(/_/g, " ")
      .replace(/\b\w/g, (char) => char.toUpperCase());

  const xTitle = spec.xTitle || formatAxisTitle(xKey);
  const yTitle = spec.yTitle || formatAxisTitle(yKey);
  const barData = useMemo(() => {
    if (chartType !== "bar") {
      return data;
    }
    const xKeyLower = String(xKey || "").toLowerCase();
    const titleLower = String(spec.title || "").toLowerCase();
    const isOrderedAxis =
      xKeyLower.includes("bin") ||
      xKeyLower.includes("time") ||
      xKeyLower.includes("date") ||
      xKeyLower.includes("bucket") ||
      titleLower.includes("distribution");
    if (isOrderedAxis || data.length <= 1) {
      return data;
    }
    const parsed = data
      .map((row) => ({ row, value: Number(row?.[yKey]) }))
      .filter((entry) => Number.isFinite(entry.value));
    if (parsed.length !== data.length) {
      return data;
    }
    const sortByAbs =
      String(yKey || "").toLowerCase().includes("correlation") ||
      titleLower.includes("correlation");
    const sorted = [...parsed].sort((left, right) =>
      sortByAbs
        ? Math.abs(right.value) - Math.abs(left.value)
        : right.value - left.value
    );
    return sorted.map((entry) => entry.row);
  }, [chartType, data, xKey, yKey, spec.title]);
  const renderedData = chartType === "bar" ? barData : data;

  const numericValues = renderedData
    .flatMap((row) =>
      seriesKeys.map((key) => {
        const raw = row?.[key];
        if (raw === null || raw === undefined || typeof raw === "boolean") {
          return null;
        }
        const parsed = Number(raw);
        return Number.isFinite(parsed) ? parsed : null;
      })
    )
    .filter((value): value is number => value !== null);
  const minValue = numericValues.length ? Math.min(...numericValues) : null;
  const maxValue = numericValues.length ? Math.max(...numericValues) : null;
  const shouldUseLog =
    spec.yScale === "log" ||
    (spec.yScale !== "linear" &&
      minValue !== null &&
      maxValue !== null &&
      minValue > 0 &&
      maxValue / minValue >= 1000);

  const commonProps = {
    data: renderedData,
    margin: { top: 12, right: 12, left: 18, bottom: 28 },
  };

  const axisStyle = {
    fill: "#b8c4d9",
    fontSize: 11,
  };

  const tooltipStyle = {
    backgroundColor: "#0f172a",
    border: "1px solid #334155",
    borderRadius: "8px",
    color: "#f1f5f9",
    padding: "10px 14px",
    boxShadow: "0 10px 40px rgba(0, 0, 0, 0.5)",
  };

  const axisLabelStyle = {
    fill: "#aebad2",
    fontSize: 10,
  };

  const formatTick = (value: unknown) => {
    if (value === null || value === undefined || value === "") {
      return "";
    }
    const numeric = Number(value);
    if (!Number.isFinite(numeric)) {
      return String(value);
    }
    const absValue = Math.abs(numeric);
    const fractionDigits = absValue >= 1000 ? 0 : 2;
    return new Intl.NumberFormat("en-US", {
      maximumFractionDigits: fractionDigits,
      minimumFractionDigits: 0,
    }).format(numeric);
  };

  const formatSeriesName = (value: string) =>
    value
      .replace(/_/g, " ")
      .replace(/\b\w/g, (char) => char.toUpperCase());

  const formatTooltipItem = (value: unknown, name: unknown) => {
    const rawName = String(name ?? "").trim();
    const genericNames = new Set([
      String(yKey || ""),
      "avg_value",
      "count",
      "value",
      "y",
    ]);
    const displayName = rawName && !genericNames.has(rawName)
      ? formatSeriesName(rawName)
      : yTitle;
    return [formatTick(value), displayName];
  };

  const formatXAxisTick = (value: unknown) => {
    if (value === null || value === undefined || value === "") {
      return "";
    }
    const raw = String(value).trim();
    if (!raw) {
      return "";
    }
    if (/^[12]\d{3}$/.test(raw)) {
      return raw;
    }
    const yearStartMatch = raw.match(
      /^([12]\d{3})-01-01(?:[ T]00:00(?::00(?:\.\d+)?)?(?:Z)?)?$/
    );
    if (yearStartMatch) {
      return yearStartMatch[1];
    }
    const epochYearMatch = raw.match(/^1970-01-01[ T]00:(\d{2}):(\d{2})(?:\.\d+)?$/);
    if (epochYearMatch) {
      const minute = Number(epochYearMatch[1]);
      const second = Number(epochYearMatch[2]);
      const possibleYear = minute * 60 + second;
      if (Number.isInteger(possibleYear) && possibleYear >= 1000 && possibleYear <= 2999) {
        return String(possibleYear);
      }
    }
    if (/^[12]\d{3}-\d{2}$/.test(raw)) {
      return raw;
    }
    const dateOnlyMatch = raw.match(/^([12]\d{3})-(\d{2})-(\d{2})$/);
    if (dateOnlyMatch) {
      const [, year, month, day] = dateOnlyMatch;
      if (month === "01" && day === "01") {
        return year;
      }
      return `${month}-${day}`;
    }
    const numeric = Number(raw);
    if (
      Number.isFinite(numeric) &&
      Number.isInteger(numeric) &&
      numeric >= 1800 &&
      numeric <= 2200
    ) {
      return String(numeric);
    }
    return raw;
  };

  const yAxisProps =
    shouldUseLog && minValue !== null
      ? { scale: "log" as const, domain: [Math.max(minValue, 1e-6), "auto"] as const }
      : {};

  const getLinearDomain = () => {
    if (minValue === null || maxValue === null) {
      return undefined;
    }
    const range = maxValue - minValue;
    const pad = range === 0 ? Math.max(Math.abs(maxValue) * 0.05, 1) : range * 0.12;
    return [minValue - pad, maxValue + pad] as const;
  };

  const lineAreaYAxisProps = !shouldUseLog
    ? {
        domain: getLinearDomain(),
        allowDataOverflow: true,
      }
    : yAxisProps;

  switch (chartType) {
    case "bar":
      return (
        <ResponsiveContainer width="100%" height="100%">
          <BarChart {...commonProps}>
            <CartesianGrid strokeDasharray="3 3" stroke="hsl(var(--border))" />
            {showAxes && (
              <XAxis
                dataKey={xKey}
                tick={axisStyle}
                axisLine={false}
                tickLine={false}
                tickFormatter={formatXAxisTick}
                label={{ value: xTitle, position: "insideBottom", offset: -12, ...axisLabelStyle }}
              />
            )}
            {showAxes && (
              <YAxis
                tick={axisStyle}
                axisLine={false}
                tickLine={false}
                tickFormatter={formatTick}
                {...lineAreaYAxisProps}
                label={{ value: yTitle, angle: -90, position: "insideLeft", ...axisLabelStyle }}
              />
            )}
            <Tooltip
              contentStyle={tooltipStyle}
              labelStyle={{ color: "#f1f5f9" }}
              itemStyle={{ color: "#f1f5f9" }}
              labelFormatter={formatXAxisTick}
              formatter={formatTooltipItem}
            />
            <Bar
              dataKey={yKey}
              fill="hsl(191 91% 45%)"
              stroke="hsl(191 91% 62%)"
              strokeWidth={1}
              radius={[4, 4, 0, 0]}
            />
          </BarChart>
        </ResponsiveContainer>
      );

    case "line":
      return (
        <ResponsiveContainer width="100%" height="100%">
          <LineChart {...commonProps}>
            <CartesianGrid strokeDasharray="3 3" stroke="hsl(var(--border))" />
            {showAxes && (
              <XAxis
                dataKey={xKey}
                tick={axisStyle}
                axisLine={false}
                tickLine={false}
                tickFormatter={formatXAxisTick}
                label={{ value: xTitle, position: "insideBottom", offset: -12, ...axisLabelStyle }}
              />
            )}
            {showAxes && (
              <YAxis
                tick={axisStyle}
                axisLine={false}
                tickLine={false}
                tickFormatter={formatTick}
                {...lineAreaYAxisProps}
                label={{ value: yTitle, angle: -90, position: "insideLeft", ...axisLabelStyle }}
              />
            )}
            <Tooltip
              contentStyle={tooltipStyle}
              labelStyle={{ color: "#f1f5f9" }}
              itemStyle={{ color: "#f1f5f9" }}
              labelFormatter={formatXAxisTick}
              formatter={formatTooltipItem}
            />
            {series ? (
              <>
                {series.length > 1 && (
                  <Legend
                    verticalAlign="top"
                    height={24}
                    wrapperStyle={{ fontSize: 11, color: "#94a3b8" }}
                  />
                )}
                {series.map((entry, index) => (
                  <Line
                    key={entry.key}
                    type="monotone"
                    dataKey={entry.key}
                    name={entry.label || entry.key}
                    stroke={CHART_COLORS[index % CHART_COLORS.length]}
                    strokeWidth={entry.highlight ? 3 : 2}
                    opacity={entry.highlight ? 1 : 0.55}
                    dot={false}
                    activeDot={false}
                  />
                ))}
              </>
            ) : (
              <Line
                type="monotone"
                dataKey={yKey}
                stroke={CHART_COLORS[0]}
                strokeWidth={2}
                dot={false}
                activeDot={false}
              />
            )}
          </LineChart>
        </ResponsiveContainer>
      );

    case "area":
      return (
        <ResponsiveContainer width="100%" height="100%">
          <AreaChart {...commonProps}>
            <defs>
              <linearGradient id={`gradient-${spec.id}`} x1="0" y1="0" x2="0" y2="1">
                <stop offset="5%" stopColor={CHART_COLORS[0]} stopOpacity={0.3} />
                <stop offset="95%" stopColor={CHART_COLORS[0]} stopOpacity={0} />
              </linearGradient>
            </defs>
            <CartesianGrid strokeDasharray="3 3" stroke="hsl(var(--border))" />
            {showAxes && (
              <XAxis
                dataKey={xKey}
                tick={axisStyle}
                axisLine={false}
                tickLine={false}
                tickFormatter={formatXAxisTick}
                label={{ value: xTitle, position: "insideBottom", offset: -12, ...axisLabelStyle }}
              />
            )}
            {showAxes && (
              <YAxis
                tick={axisStyle}
                axisLine={false}
                tickLine={false}
                tickFormatter={formatTick}
                {...lineAreaYAxisProps}
                label={{ value: yTitle, angle: -90, position: "insideLeft", ...axisLabelStyle }}
              />
            )}
            <Tooltip
              contentStyle={tooltipStyle}
              labelStyle={{ color: "#f1f5f9" }}
              itemStyle={{ color: "#f1f5f9" }}
              labelFormatter={formatXAxisTick}
              formatter={formatTooltipItem}
            />
            <Area
              type="monotone"
              dataKey={yKey}
              stroke={CHART_COLORS[0]}
              fill={`url(#gradient-${spec.id})`}
              strokeWidth={2}
            />
          </AreaChart>
        </ResponsiveContainer>
      );

    case "heatmap": {
      const valueKey = spec.valueKey || (series?.[0]?.key ?? "correlation");
      const xLabelsRaw = Array.from(
        new Set(
          data
            .map((row) => row?.[xKey])
            .filter((value) => value !== null && value !== undefined && value !== "")
            .map((value) => String(value))
        )
      );
      const yLabelsRaw = Array.from(
        new Set(
          data
            .map((row) => row?.[yKey])
            .filter((value) => value !== null && value !== undefined && value !== "")
            .map((value) => String(value))
        )
      );
      const labels = Array.from(new Set([...xLabelsRaw, ...yLabelsRaw]));
      const xLabels = labels;
      const yLabels = labels;
      const valueMap = new Map<string, number>();
      data.forEach((row) => {
        const xVal = row?.[xKey];
        const yVal = row?.[yKey];
        const rawValue = row?.[valueKey];
        if (xVal === null || xVal === undefined || yVal === null || yVal === undefined) {
          return;
        }
        const numeric = Number(rawValue);
        if (!Number.isFinite(numeric)) {
          return;
        }
        const xLabel = String(xVal);
        const yLabel = String(yVal);
        valueMap.set(`${xLabel}|||${yLabel}`, numeric);
        if (!valueMap.has(`${yLabel}|||${xLabel}`)) {
          valueMap.set(`${yLabel}|||${xLabel}`, numeric);
        }
      });
      const isCorrelationMatrix =
        String(valueKey).toLowerCase().includes("correlation") ||
        String(spec.title || "").toLowerCase().includes("correlation");
      if (isCorrelationMatrix) {
        labels.forEach((label) => {
          const key = `${label}|||${label}`;
          if (!valueMap.has(key)) {
            valueMap.set(key, 1);
          }
        });
      }

      const colorFor = (value: number | undefined) => {
        if (value === undefined || Number.isNaN(value)) {
          return "hsl(220 18% 18%)";
        }
        const clamped = Math.max(-1, Math.min(1, value));
        const intensity = Math.abs(clamped);
        if (intensity < 0.02) {
          return "hsl(220 16% 24%)";
        }
        const lightness = 24 + intensity * 34;
        if (clamped >= 0) {
          return `hsl(168 ${58 + intensity * 30}% ${lightness}%)`;
        }
        return `hsl(8 ${64 + intensity * 28}% ${lightness}%)`;
      };

      const textColorFor = (value: number | undefined) => {
        if (value === undefined || Number.isNaN(value)) {
          return "text-slate-300";
        }
        return Math.abs(value) >= 0.62 ? "text-slate-950" : "text-slate-100";
      };

      if (xLabels.length === 0 || yLabels.length === 0) {
        return (
          <div className="h-full w-full flex items-center justify-center text-xs text-muted-foreground">
            No matrix cells available.
          </div>
        );
      }

      return (
        <div className="h-full w-full overflow-auto">
          <div className="min-w-max pr-2">
            <div
              className="grid gap-1"
              style={{
                gridTemplateColumns: `minmax(120px, 180px) repeat(${xLabels.length}, minmax(52px, 1fr))`,
              }}
            >
              <div />
              {xLabels.map((label) => (
                <div
                  key={`x-${label}`}
                  className="truncate text-[11px] font-medium text-slate-300 text-center px-1 py-1"
                  title={label}
                >
                  {label}
                </div>
              ))}
              {yLabels.map((yLabel) => (
                <Fragment key={`row-${yLabel}`}>
                  <div
                    className="truncate text-[11px] font-medium text-slate-300 px-1 py-1"
                    title={yLabel}
                  >
                    {yLabel}
                  </div>
                  {xLabels.map((xLabel) => {
                    const value = valueMap.get(`${xLabel}|||${yLabel}`);
                    return (
                      <div
                        key={`${xLabel}-${yLabel}`}
                        className={`h-9 rounded-sm border border-slate-800/90 flex items-center justify-center text-[11px] font-semibold ${textColorFor(value)}`}
                        style={{ backgroundColor: colorFor(value) }}
                        title={
                          value === undefined
                            ? `${xLabel} × ${yLabel}: N/A`
                            : `${xLabel} × ${yLabel}: ${value.toFixed(3)}`
                        }
                      >
                        {value === undefined ? "—" : value.toFixed(2)}
                      </div>
                    );
                  })}
                </Fragment>
              ))}
            </div>
            <div className="mt-2 flex items-center gap-2 text-[10px] text-slate-400">
              <span>-1</span>
              <div className="h-2 w-28 rounded-full bg-gradient-to-r from-[#ef4444] via-[#334155] to-[#14b8a6]" />
              <span>+1</span>
            </div>
          </div>
        </div>
      );
    }

    case "pie": {
      const pieData = data.filter((row) => {
        const value = Number(row?.[yKey]);
        return Number.isFinite(value) && value > 0;
      });
      const resolvedData = pieData.length > 0 ? pieData : data;
      return (
        <ResponsiveContainer width="100%" height="100%">
          <PieChart>
            <Pie
              data={resolvedData}
              cx="50%"
              cy="50%"
              innerRadius="40%"
              outerRadius="70%"
              dataKey={yKey}
              nameKey={xKey}
              paddingAngle={2}
              label={({ name, percent, payload }) => {
                const labelName = name ?? payload?.[xKey] ?? "Category";
                if (!Number.isFinite(percent)) {
                  return String(labelName);
                }
                return `${labelName} ${(percent * 100).toFixed(0)}%`;
              }}
              labelLine={false}
            >
              {resolvedData.map((_, index) => (
                <Cell key={`cell-${index}`} fill={CHART_COLORS[index % CHART_COLORS.length]} />
              ))}
            </Pie>
            <Tooltip
              contentStyle={tooltipStyle}
              labelStyle={{ color: "#f1f5f9" }}
              itemStyle={{ color: "#f1f5f9" }}
              labelFormatter={formatXAxisTick}
              formatter={formatTooltipItem}
            />
          </PieChart>
        </ResponsiveContainer>
      );
    }

    default:
      return null;
  }
});

ChartRenderer.displayName = "ChartRenderer";

export default ChartRenderer;
