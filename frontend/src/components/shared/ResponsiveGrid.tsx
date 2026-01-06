/**
 * ResponsiveGrid - 可复用的响应式网格组件
 * 根据断点自动调整列数
 */

import { cn } from "@/lib/utils";
import { ReactNode } from "react";

interface ResponsiveGridProps {
  children: ReactNode;
  /** 各断点的列数配置 */
  cols?: {
    xs?: number;
    sm?: number;
    md?: number;
    lg?: number;
    xl?: number;
  };
  /** 网格间距 */
  gap?: "sm" | "md" | "lg";
  className?: string;
}

const gapClasses = {
  sm: "gap-2",
  md: "gap-4",
  lg: "gap-6",
};

export function ResponsiveGrid({
  children,
  cols = { xs: 1, sm: 2, md: 2, lg: 4 },
  gap = "md",
  className,
}: ResponsiveGridProps) {
  // 生成响应式网格类名
  const gridCols = cn(
    cols.xs && `grid-cols-${cols.xs}`,
    cols.sm && `sm:grid-cols-${cols.sm}`,
    cols.md && `md:grid-cols-${cols.md}`,
    cols.lg && `lg:grid-cols-${cols.lg}`,
    cols.xl && `xl:grid-cols-${cols.xl}`
  );

  return (
    <div className={cn("grid", gridCols, gapClasses[gap], className)}>
      {children}
    </div>
  );
}
