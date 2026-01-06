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

  const commonProps = {
    data,
    margin: { top: 10, right: 10, left: 0, bottom: 0 },
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

  switch (chartType) {
    case "bar":
      return (
        <ResponsiveContainer width="100%" height="100%">
          <BarChart {...commonProps}>
            <CartesianGrid strokeDasharray="3 3" stroke="hsl(var(--border))" />
            {showAxes && <XAxis dataKey={xKey} tick={axisStyle} axisLine={false} tickLine={false} />}
            {showAxes && <YAxis tick={axisStyle} axisLine={false} tickLine={false} />}
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
            {showAxes && <XAxis dataKey={xKey} tick={axisStyle} axisLine={false} tickLine={false} />}
            {showAxes && <YAxis tick={axisStyle} axisLine={false} tickLine={false} />}
             <Tooltip contentStyle={tooltipStyle} labelStyle={{ color: "#f1f5f9" }} itemStyle={{ color: "#f1f5f9" }} />
            <Line
              type="monotone"
              dataKey={yKey}
              stroke={CHART_COLORS[0]}
              strokeWidth={2}
              dot={{ fill: CHART_COLORS[0], strokeWidth: 0, r: 4 }}
              activeDot={{ r: 6, fill: CHART_COLORS[0] }}
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
            {showAxes && <XAxis dataKey={xKey} tick={axisStyle} axisLine={false} tickLine={false} />}
            {showAxes && <YAxis tick={axisStyle} axisLine={false} tickLine={false} />}
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

    case "pie":
      return (
        <ResponsiveContainer width="100%" height="100%">
          <PieChart>
            <Pie
              data={data}
              cx="50%"
              cy="50%"
              innerRadius="40%"
              outerRadius="70%"
              dataKey={yKey}
              nameKey={xKey}
              paddingAngle={2}
              label={({ name, percent }) => `${name} ${(percent * 100).toFixed(0)}%`}
              labelLine={false}
            >
              {data.map((_, index) => (
                <Cell key={`cell-${index}`} fill={CHART_COLORS[index % CHART_COLORS.length]} />
              ))}
            </Pie>
            <Tooltip contentStyle={tooltipStyle} labelStyle={{ color: "#f1f5f9" }} itemStyle={{ color: "#f1f5f9" }} />
          </PieChart>
        </ResponsiveContainer>
      );

    default:
      return null;
  }
});

ChartRenderer.displayName = "ChartRenderer";

export default ChartRenderer;
