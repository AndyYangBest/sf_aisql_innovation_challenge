import { TableResult } from "@/types";
import { Table } from "lucide-react";

interface DataTabProps {
  tableResult?: TableResult;
}

const DataTab = ({ tableResult }: DataTabProps) => {
  if (!tableResult) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="text-center text-muted-foreground">
          <Table className="h-12 w-12 mx-auto mb-3 opacity-50" />
          <p>No data available</p>
        </div>
      </div>
    );
  }

  return (
    <div className="space-y-4 animate-fade-in">
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-lg font-semibold mb-1">Data</h2>
          <p className="text-sm text-muted-foreground">
            {tableResult.rowCount} rows × {tableResult.columns.length} columns
          </p>
        </div>
      </div>

      <div className="border border-border rounded-xl overflow-hidden bg-card">
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead className="bg-muted/50 sticky top-0">
              <tr>
                {tableResult.columns.map((col) => (
                  <th
                    key={col.name}
                    className="px-4 py-3 text-left font-medium text-foreground border-b border-border"
                  >
                    <div className="flex flex-col">
                      <span>{col.name}</span>
                      {col.type && (
                        <span className="text-xs font-normal text-muted-foreground">
                          {col.type}
                        </span>
                      )}
                    </div>
                  </th>
                ))}
              </tr>
            </thead>
            <tbody className="divide-y divide-border">
              {tableResult.rows.map((row, rowIndex) => (
                <tr
                  key={rowIndex}
                  className="hover:bg-muted/30 transition-colors"
                >
                  {tableResult.columns.map((col) => (
                    <td
                      key={col.name}
                      className="px-4 py-3 font-mono text-sm whitespace-nowrap"
                    >
                      {formatValue(row[col.name], col.type)}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
};

const formatValue = (value: any, type?: string): string => {
  if (value === null || value === undefined) return "—";
  if (type?.includes("DATE") || type?.includes("TIMESTAMP")) {
    return new Date(value).toLocaleDateString();
  }
  if (type?.includes("DECIMAL") || type?.includes("FLOAT")) {
    return typeof value === "number" ? value.toLocaleString() : String(value);
  }
  return String(value);
};

export default DataTab;
