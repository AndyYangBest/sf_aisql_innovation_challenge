/**
 * StatCard - 可复用的统计卡片组件
 * 支持响应式布局和多种变体
 */

import { LucideIcon } from "lucide-react";
import { cn } from "@/lib/utils";

interface StatCardProps {
  icon: LucideIcon;
  value: number | string;
  label: string;
  /** 图标背景色 - 使用 Tailwind 变体 */
  variant?: "primary" | "success" | "warning" | "info";
  className?: string;
}

const variantClasses = {
  primary: "bg-primary/10 text-primary",
  success: "bg-success/10 text-success",
  warning: "bg-warning/10 text-warning",
  info: "bg-info/10 text-info",
};

export function StatCard({
  icon: Icon,
  value,
  label,
  variant = "primary",
  className,
}: StatCardProps) {
  return (
    <div
      className={cn(
        "p-3 sm:p-4 rounded-xl bg-card border border-border",
        className
      )}
    >
      <div className="flex items-center gap-2 sm:gap-3">
        <div className={cn("p-1.5 sm:p-2 rounded-lg", variantClasses[variant])}>
          <Icon className="h-4 w-4 sm:h-5 sm:w-5" />
        </div>
        <div className="min-w-0">
          <div className="text-lg sm:text-2xl font-bold truncate">{value}</div>
          <div className="text-xs sm:text-sm text-muted-foreground truncate">
            {label}
          </div>
        </div>
      </div>
    </div>
  );
}
