import { GitBranch, ArrowRight, Table, Columns } from "lucide-react";
import { TableAsset, TableResult } from "@/types";

interface LineageTabProps {
  tableAsset: TableAsset;
  tableResult?: TableResult;
}

const LineageTab = ({ tableAsset, tableResult }: LineageTabProps) => {
  // Mock related tables
  const relatedTables = [
    { name: "customers", relationship: "frequently joined", columns: ["customer_id"] },
    { name: "products", relationship: "frequently joined", columns: ["product_id", "category"] },
    { name: "regions", relationship: "dimension lookup", columns: ["region_id"] },
  ];

  // Mock column co-usage patterns
  const columnPatterns = tableResult?.columns.slice(0, 5).map((col) => ({
    column: col.name,
    usedWith: tableResult.columns
      .filter((c) => c.name !== col.name)
      .slice(0, 2)
      .map((c) => c.name),
  })) || [];

  return (
    <div className="space-y-6 animate-fade-in max-w-4xl">
      <div>
        <h2 className="text-lg font-semibold mb-1">Lineage & Relations</h2>
        <p className="text-sm text-muted-foreground">
          Understand how this table relates to other tables and how columns are typically used together.
        </p>
      </div>

      {/* Related Tables */}
      <div className="p-5 rounded-xl glass">
        <h3 className="font-medium mb-4 flex items-center gap-2">
          <Table className="h-4 w-4 text-muted-foreground" />
          Related Tables
        </h3>
        <div className="space-y-3">
          {relatedTables.map((table, i) => (
            <div
              key={i}
              className="flex items-center justify-between p-3 rounded-lg bg-muted/30 hover:bg-muted/50 transition-colors cursor-pointer group"
            >
              <div className="flex items-center gap-3">
                <div className="p-1.5 rounded bg-primary/10">
                  <Table className="h-4 w-4 text-primary" />
                </div>
                <div>
                  <p className="font-medium group-hover:text-primary transition-colors">{table.name}</p>
                  <p className="text-xs text-muted-foreground">{table.relationship}</p>
                </div>
              </div>
              <div className="flex items-center gap-2">
                <span className="text-xs text-muted-foreground">
                  via {table.columns.join(", ")}
                </span>
                <ArrowRight className="h-4 w-4 text-muted-foreground group-hover:text-primary transition-colors" />
              </div>
            </div>
          ))}
        </div>
      </div>

      {/* Column Co-usage Patterns */}
      <div className="p-5 rounded-xl glass">
        <h3 className="font-medium mb-4 flex items-center gap-2">
          <Columns className="h-4 w-4 text-muted-foreground" />
          Column Co-usage Patterns
        </h3>
        <p className="text-sm text-muted-foreground mb-4">
          Columns that are frequently analyzed together based on usage patterns.
        </p>
        <div className="space-y-3">
          {columnPatterns.map((pattern, i) => (
            <div key={i} className="flex items-center gap-3 p-3 rounded-lg bg-muted/30">
              <span className="font-mono text-sm text-primary">{pattern.column}</span>
              <ArrowRight className="h-4 w-4 text-muted-foreground" />
              <span className="text-sm text-muted-foreground">often used with</span>
              <div className="flex gap-2">
                {pattern.usedWith.map((col) => (
                  <span key={col} className="font-mono text-sm bg-muted px-2 py-0.5 rounded">
                    {col}
                  </span>
                ))}
              </div>
            </div>
          ))}
        </div>
      </div>

      {/* Visual Lineage Placeholder */}
      <div className="p-5 rounded-xl glass border border-dashed border-border">
        <div className="flex items-center justify-center h-32 text-muted-foreground">
          <div className="text-center">
            <GitBranch className="h-8 w-8 mx-auto mb-2 opacity-50" />
            <p className="text-sm">Visual lineage graph coming soon</p>
          </div>
        </div>
      </div>
    </div>
  );
};

export default LineageTab;
