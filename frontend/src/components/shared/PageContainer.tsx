/**
 * PageContainer - 可复用的页面容器组件
 * 提供统一的页面布局和响应式内边距
 */

import { cn } from "@/lib/utils";
import { ReactNode } from "react";

interface PageContainerProps {
  children: ReactNode;
  /** 最大宽度 */
  maxWidth?: "sm" | "md" | "lg" | "xl" | "2xl" | "full";
  className?: string;
}

const maxWidthClasses = {
  sm: "max-w-2xl",
  md: "max-w-4xl",
  lg: "max-w-5xl",
  xl: "max-w-6xl",
  "2xl": "max-w-7xl",
  full: "max-w-full",
};

export function PageContainer({
  children,
  maxWidth = "xl",
  className,
}: PageContainerProps) {
  return (
    <div
      className={cn(
        "container mx-auto px-4 sm:px-6 py-4 sm:py-6 md:py-8",
        maxWidthClasses[maxWidth],
        className
      )}
    >
      {children}
    </div>
  );
}

interface PageHeaderProps {
  children: ReactNode;
  className?: string;
  sticky?: boolean;
}

export function PageHeader({
  children,
  className,
  sticky = true,
}: PageHeaderProps) {
  return (
    <header
      className={cn(
        "border-b border-border/50 bg-card/50 backdrop-blur-sm",
        sticky && "sticky top-0 z-50",
        className
      )}
    >
      <div className="container mx-auto px-4 sm:px-6 py-3 sm:py-4 max-w-6xl">
        {children}
      </div>
    </header>
  );
}
