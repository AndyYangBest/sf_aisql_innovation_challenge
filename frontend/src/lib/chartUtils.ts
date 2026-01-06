// Chart utility functions for generating varied chart data

export type ChartType = "bar" | "line" | "pie" | "area";

export interface ChartData {
  [key: string]: string | number;
}

export interface ChartSpec {
  id: string;
  chartType: ChartType;
  title: string;
  xKey: string;
  yKey: string;
  data: ChartData[];
  narrative: string[];
  sourceColumns: string[];
}

// Color palette for charts - using direct HSL values for recharts compatibility
export const CHART_COLORS = [
  "#22d3ee", // cyan
  "#a855f7", // purple
  "#facc15", // yellow
  "#34d399", // green
  "#fb923c", // orange
  "#f472b6", // pink
  "#60a5fa", // blue
  "#f87171", // red
  "#2dd4bf", // teal
];

// Sample data generators
const categories = ["Software", "Integration", "Infrastructure", "Analytics", "Support", "Consulting"];
const regions = ["North America", "Europe", "Asia Pacific", "Latin America", "Middle East"];
const products = ["Pro Dashboard", "Analytics Suite", "Data Connector", "Report Builder", "API Gateway"];
const months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
const quarters = ["Q1", "Q2", "Q3", "Q4"];

const chartTemplates = [
  {
    title: "Revenue by Category",
    xKey: "category",
    yKey: "revenue",
    types: ["bar", "pie"] as ChartType[],
    generateData: () => shuffleArray(categories).slice(0, 4 + Math.floor(Math.random() * 3)).map(cat => ({
      category: cat,
      revenue: Math.floor(Math.random() * 150000) + 20000,
    })),
    narrative: ["Top category accounts for {top}% of revenue", "Growth rate varies by {var}% across categories"],
  },
  {
    title: "Sales by Region",
    xKey: "region",
    yKey: "sales",
    types: ["bar", "pie"] as ChartType[],
    generateData: () => shuffleArray(regions).slice(0, 3 + Math.floor(Math.random() * 3)).map(region => ({
      region,
      sales: Math.floor(Math.random() * 200000) + 30000,
    })),
    narrative: ["Regional distribution shows {pattern} pattern", "Highest performing region leads by {lead}%"],
  },
  {
    title: "Monthly Trend",
    xKey: "month",
    yKey: "value",
    types: ["line", "area"] as ChartType[],
    generateData: () => {
      let base = Math.floor(Math.random() * 50000) + 30000;
      return months.slice(0, 6 + Math.floor(Math.random() * 6)).map(month => {
        base = base + (Math.random() - 0.4) * 10000;
        return { month, value: Math.max(10000, Math.floor(base)) };
      });
    },
    narrative: ["Trend shows {trend} trajectory", "Peak month outperforms average by {peak}%"],
  },
  {
    title: "Quarterly Performance",
    xKey: "quarter",
    yKey: "performance",
    types: ["bar", "line", "area"] as ChartType[],
    generateData: () => quarters.map(quarter => ({
      quarter,
      performance: Math.floor(Math.random() * 100000) + 50000,
    })),
    narrative: ["Quarter-over-quarter growth of {growth}%", "Seasonal patterns detected"],
  },
  {
    title: "Product Distribution",
    xKey: "product",
    yKey: "units",
    types: ["bar", "pie"] as ChartType[],
    generateData: () => shuffleArray(products).slice(0, 3 + Math.floor(Math.random() * 3)).map(product => ({
      product,
      units: Math.floor(Math.random() * 500) + 50,
    })),
    narrative: ["Top product represents {top}% of units", "Product mix is {mix}"],
  },
  {
    title: "Cost Analysis",
    xKey: "category",
    yKey: "cost",
    types: ["bar", "area"] as ChartType[],
    generateData: () => shuffleArray(["Operations", "Marketing", "R&D", "Sales", "Admin"]).slice(0, 4).map(cat => ({
      category: cat,
      cost: Math.floor(Math.random() * 80000) + 10000,
    })),
    narrative: ["Cost distribution across departments", "Optimization potential in {area}"],
  },
  {
    title: "Growth Metrics",
    xKey: "metric",
    yKey: "value",
    types: ["bar"] as ChartType[],
    generateData: () => shuffleArray(["Users", "Revenue", "Orders", "Sessions", "Conversions"]).slice(0, 4).map(metric => ({
      metric,
      value: Math.floor(Math.random() * 1000) + 100,
    })),
    narrative: ["Key metrics overview", "Growth opportunities identified"],
  },
];

function shuffleArray<T>(array: T[]): T[] {
  const shuffled = [...array];
  for (let i = shuffled.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [shuffled[i], shuffled[j]] = [shuffled[j], shuffled[i]];
  }
  return shuffled;
}

function fillNarrative(narrative: string[]): string[] {
  return narrative.map(n => 
    n.replace("{top}", String(Math.floor(Math.random() * 30) + 25))
      .replace("{var}", String(Math.floor(Math.random() * 20) + 5))
      .replace("{pattern}", ["balanced", "concentrated", "distributed"][Math.floor(Math.random() * 3)])
      .replace("{lead}", String(Math.floor(Math.random() * 25) + 10))
      .replace("{trend}", ["upward", "stable", "fluctuating"][Math.floor(Math.random() * 3)])
      .replace("{peak}", String(Math.floor(Math.random() * 40) + 15))
      .replace("{growth}", String(Math.floor(Math.random() * 20) + 5))
      .replace("{mix}", ["diverse", "concentrated", "evolving"][Math.floor(Math.random() * 3)])
      .replace("{area}", ["operations", "marketing", "logistics"][Math.floor(Math.random() * 3)])
  );
}

export function generateRandomCharts(count: number = 3): ChartSpec[] {
  const selectedTemplates = shuffleArray(chartTemplates).slice(0, count);
  
  return selectedTemplates.map((template, index) => {
    const chartType = template.types[Math.floor(Math.random() * template.types.length)];
    
    return {
      id: `chart-${Date.now()}-${index}`,
      chartType,
      title: template.title,
      xKey: template.xKey,
      yKey: template.yKey,
      data: template.generateData(),
      narrative: fillNarrative(template.narrative),
      sourceColumns: [template.xKey, template.yKey],
    };
  });
}

// Grid layout utilities
export interface GridLayoutItem {
  i: string;
  x: number;
  y: number;
  w: number;
  h: number;
  minW?: number;
  minH?: number;
}

export function generateGridLayout(chartIds: string[], existingLayout: GridLayoutItem[] = []): GridLayoutItem[] {
  const existingMap = new Map(existingLayout.map(item => [item.i, item]));
  const cols = 12;
  const defaultW = 6;
  const defaultH = 4;
  
  // Find the next available position
  const getNextPosition = (usedPositions: GridLayoutItem[]): { x: number; y: number } => {
    if (usedPositions.length === 0) return { x: 0, y: 0 };
    
    // Find the maximum y and calculate next position
    let maxY = 0;
    let maxYEndX = 0;
    
    usedPositions.forEach(pos => {
      const endY = pos.y + pos.h;
      if (endY > maxY) {
        maxY = endY;
        maxYEndX = 0;
      }
      if (pos.y + pos.h === maxY) {
        maxYEndX = Math.max(maxYEndX, pos.x + pos.w);
      }
    });
    
    // Try to fit in the same row
    if (maxYEndX + defaultW <= cols) {
      return { x: maxYEndX, y: maxY - defaultH };
    }
    
    // Start new row
    return { x: 0, y: maxY };
  };
  
  const layout: GridLayoutItem[] = [];
  
  chartIds.forEach((id) => {
    if (existingMap.has(id)) {
      layout.push(existingMap.get(id)!);
    } else {
      const pos = getNextPosition(layout);
      layout.push({
        i: id,
        x: pos.x,
        y: pos.y,
        w: defaultW,
        h: defaultH,
        minW: 4,
        minH: 3,
      });
    }
  });
  
  return layout;
}
