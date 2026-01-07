import { cn } from "@/lib/utils";

interface PreviewTableProps {
  columns: { name: string; type?: string }[];
  rows: any[];
  maxHeight?: number;
  colWidth?: number;
  rowLimit?: number;
  numericColumns?: string[];
}

const PreviewTable = ({
  columns,
  rows,
  maxHeight = 320,
  colWidth = 200,
  rowLimit = 10,
  numericColumns = [],
}: PreviewTableProps) => {
  const minWidth = Math.max(columns.length * colWidth, colWidth * 6);
  const numericSet = new Set(numericColumns);

  return (
    <div
      className="w-full max-w-full border border-border rounded-lg overflow-auto scrollbar-thin"
      style={{ maxHeight: `${maxHeight}px`, WebkitOverflowScrolling: "touch" as const }}
    >
      <table
        className="text-sm border-collapse table-fixed"
        style={{ minWidth: `${minWidth}px`, width: "max-content" }}
      >
        <thead className="bg-muted/50 sticky top-0 z-10">
          <tr>
            {columns.map((col) => (
              <th
                key={col.name}
                className={cn(
                  "px-3 py-1.5 text-left font-medium text-muted-foreground text-xs whitespace-nowrap border-b border-border",
                  numericSet.has(col.name) &&
                    "bg-sky-200 text-sky-950 border-sky-400 dark:bg-sky-900 dark:text-sky-50 dark:border-sky-500/70"
                )}
                style={{ width: `${colWidth}px`, minWidth: `${colWidth}px`, maxWidth: `${colWidth}px` }}
              >
                <div className="flex items-center gap-1">
                  <span>{col.name}</span>
                  {col.type && <span className="text-xs opacity-60">({col.type})</span>}
                </div>
              </th>
            ))}
          </tr>
        </thead>
        <tbody className="divide-y divide-border">
          {rows.slice(0, rowLimit).map((row: any, i: number) => (
            <tr key={i} className="hover:bg-muted/30">
              {columns.map((col) => (
                <td
                  key={col.name}
                  className="px-3 py-1.5 font-mono text-xs align-top"
                  style={{ width: `${colWidth}px`, minWidth: `${colWidth}px`, maxWidth: `${colWidth}px` }}
                >
                  <div className="max-w-full overflow-x-auto whitespace-nowrap scrollbar-thin" title={String(row[col.name])}>
                    {row[col.name] === null || row[col.name] === undefined ? (
                      <span className="text-muted-foreground italic">null</span>
                    ) : (
                      String(row[col.name])
                    )}
                  </div>
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
};

export default PreviewTable;
