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
  primary: "bg-primary/15 text-primary",
  success: "bg-success/15 text-success",
  warning: "bg-warning/15 text-warning",
  info: "bg-info/15 text-info",
};

const glowClasses = {
  primary: "from-primary/25 via-primary/10 to-transparent",
  success: "from-success/25 via-success/10 to-transparent",
  warning: "from-warning/25 via-warning/10 to-transparent",
  info: "from-info/25 via-info/10 to-transparent",
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
        "relative overflow-hidden rounded-2xl border border-border/60 bg-card/70 p-4 sm:p-5 shadow-sm transition hover:-translate-y-0.5 hover:shadow-md",
        className
      )}
    >
      <div
        className={cn(
          "pointer-events-none absolute -right-10 -top-10 h-24 w-24 rounded-full bg-gradient-to-br opacity-60 blur-2xl",
          glowClasses[variant]
        )}
      />
      <div className="flex items-start justify-between gap-3">
        <div className="min-w-0">
          <div className="text-xs uppercase tracking-[0.2em] text-muted-foreground">
            {label}
          </div>
          <div className="mt-2 text-2xl sm:text-3xl font-semibold leading-none truncate">
            {value}
          </div>
        </div>
        <div className={cn("rounded-xl p-2 sm:p-2.5", variantClasses[variant])}>
          <Icon className="h-4 w-4 sm:h-5 sm:w-5" />
        </div>
      </div>
    </div>
  );
}
