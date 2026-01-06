/**
 * useBreakpoint - 响应式断点 Hook
 * 提供统一的断点检测，支持 mobile/tablet/desktop
 */

import { useState, useEffect, useMemo } from "react";

export const BREAKPOINTS = {
  sm: 640,
  md: 768,
  lg: 1024,
  xl: 1280,
  "2xl": 1536,
} as const;

export type Breakpoint = keyof typeof BREAKPOINTS;

export function useBreakpoint() {
  const [width, setWidth] = useState<number>(
    typeof window !== "undefined" ? window.innerWidth : 1024
  );

  useEffect(() => {
    const handleResize = () => setWidth(window.innerWidth);
    window.addEventListener("resize", handleResize);
    return () => window.removeEventListener("resize", handleResize);
  }, []);

  const breakpoint = useMemo(() => {
    if (width < BREAKPOINTS.sm) return "xs";
    if (width < BREAKPOINTS.md) return "sm";
    if (width < BREAKPOINTS.lg) return "md";
    if (width < BREAKPOINTS.xl) return "lg";
    if (width < BREAKPOINTS["2xl"]) return "xl";
    return "2xl";
  }, [width]);

  return {
    width,
    breakpoint,
    isMobile: width < BREAKPOINTS.md,
    isTablet: width >= BREAKPOINTS.md && width < BREAKPOINTS.lg,
    isDesktop: width >= BREAKPOINTS.lg,
    // 便捷方法
    isAbove: (bp: Breakpoint) => width >= BREAKPOINTS[bp],
    isBelow: (bp: Breakpoint) => width < BREAKPOINTS[bp],
  };
}
