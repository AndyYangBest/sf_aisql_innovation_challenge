import { memo } from "react";
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
  Cell,
} from "recharts";
import { CHART_COLORS, ChartSpec } from "@/lib/chartUtils";

interface ChartRendererProps {
  spec: ChartSpec;
  showAxes?: boolean;
}

const ChartRenderer = memo(({ spec, showAxes = true }: ChartRendererProps) => {
  const { chartType, data, xKey, yKey } = spec;
  const formatAxisTitle = (value: string) =>
    value
      .replace(/_/g, " ")
      .replace(/\b\w/g, (char) => char.toUpperCase());

  const xTitle = spec.xTitle || formatAxisTitle(xKey);
  const yTitle = spec.yTitle || formatAxisTitle(yKey);

  const numericValues = data
    .map((row) => {
      const raw = row?.[yKey];
      if (raw === null || raw === undefined || typeof raw === "boolean") {
        return null;
      }
      const parsed = Number(raw);
      return Number.isFinite(parsed) ? parsed : null;
    })
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
    data,
    margin: { top: 12, right: 12, left: 18, bottom: 28 },
  };

  const axisStyle = {
    fill: "#94a3b8",
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
    fill: "#94a3b8",
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
            <Tooltip contentStyle={tooltipStyle} labelStyle={{ color: "#f1f5f9" }} itemStyle={{ color: "#f1f5f9" }} />
            <Bar dataKey={yKey} fill={CHART_COLORS[0]} radius={[4, 4, 0, 0]}>
              {data.map((_, index) => (
                <Cell key={`cell-${index}`} fill={CHART_COLORS[index % CHART_COLORS.length]} />
              ))}
            </Bar>
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
            <Tooltip contentStyle={tooltipStyle} labelStyle={{ color: "#f1f5f9" }} itemStyle={{ color: "#f1f5f9" }} />
            <Line
              type="monotone"
              dataKey={yKey}
              stroke={CHART_COLORS[0]}
              strokeWidth={2}
              dot={false}
              activeDot={false}
            />
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
            <Tooltip contentStyle={tooltipStyle} labelStyle={{ color: "#f1f5f9" }} itemStyle={{ color: "#f1f5f9" }} />
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
            <Tooltip contentStyle={tooltipStyle} labelStyle={{ color: "#f1f5f9" }} itemStyle={{ color: "#f1f5f9" }} />
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
