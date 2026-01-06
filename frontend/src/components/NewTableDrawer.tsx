import { useState } from "react";
import { useNavigate } from "react-router-dom";
import { X, Play, Save, Loader2 } from "lucide-react";
import { Sheet, SheetContent, SheetHeader, SheetTitle } from "@/components/ui/sheet";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Textarea } from "@/components/ui/textarea";
import { useTableStore } from "@/store/tableStore";
import { useToast } from "@/hooks/use-toast";

interface NewTableDrawerProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
}

const mockRunQuery = async (sql: string) => {
  await new Promise((resolve) => setTimeout(resolve, 1500));
  return {
    columns: [
      { name: "id", type: "INTEGER" },
      { name: "name", type: "VARCHAR" },
      { name: "value", type: "DECIMAL" },
      { name: "created_at", type: "TIMESTAMP" },
    ],
    rows: [
      { id: 1, name: "Sample A", value: 1250.5, created_at: "2024-12-01T10:00:00Z" },
      { id: 2, name: "Sample B", value: 890.25, created_at: "2024-12-02T11:30:00Z" },
      { id: 3, name: "Sample C", value: 2100.0, created_at: "2024-12-03T09:15:00Z" },
      { id: 4, name: "Sample D", value: 567.8, created_at: "2024-12-04T14:45:00Z" },
      { id: 5, name: "Sample E", value: 1890.15, created_at: "2024-12-05T16:20:00Z" },
    ],
    rowCount: 5,
  };
};

const NewTableDrawer = ({ open, onOpenChange }: NewTableDrawerProps) => {
  const [sql, setSql] = useState("SELECT * FROM your_table LIMIT 100");
  const [tableName, setTableName] = useState("");
  const [tags, setTags] = useState("");
  const [isRunning, setIsRunning] = useState(false);
  const [previewData, setPreviewData] = useState<any>(null);
  const { addTableAsset, tableResults } = useTableStore();
  const { toast } = useToast();
  const navigate = useNavigate();

  const handleRun = async () => {
    setIsRunning(true);
    try {
      const result = await mockRunQuery(sql);
      setPreviewData(result);
      toast({
        title: "Query executed",
        description: `Returned ${result.rowCount} rows`,
      });
    } catch (error) {
      toast({
        title: "Query failed",
        description: "Please check your SQL syntax",
        variant: "destructive",
      });
    } finally {
      setIsRunning(false);
    }
  };

  const handleSave = () => {
    if (!tableName.trim()) {
      toast({
        title: "Name required",
        description: "Please enter a name for this table",
        variant: "destructive",
      });
      return;
    }

    const newId = Date.now().toString();
    const newAsset = {
      id: newId,
      name: tableName,
      sourceSql: sql,
      database: "ANALYTICS",
      schema: "PUBLIC",
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString(),
      tags: tags
        .split(",")
        .map((t) => t.trim())
        .filter(Boolean),
      owner: "current_user",
    };

    addTableAsset(newAsset);

    // Store the result
    if (previewData) {
      const store = useTableStore.getState();
      store.tableResults[newId] = previewData;
    }

    toast({
      title: "Table saved",
      description: `"${tableName}" has been created`,
    });

    onOpenChange(false);
    navigate(`/tables/${newId}`);
  };

  const handleClose = () => {
    setSql("SELECT * FROM your_table LIMIT 100");
    setTableName("");
    setTags("");
    setPreviewData(null);
    onOpenChange(false);
  };

  return (
    <Sheet open={open} onOpenChange={handleClose}>
      <SheetContent className="w-full sm:max-w-2xl glass border-border overflow-y-auto">
        <SheetHeader className="mb-6">
          <SheetTitle className="flex items-center justify-between">
            <span>New Table from SQL</span>
          </SheetTitle>
        </SheetHeader>

        <div className="space-y-6">
          {/* SQL Editor */}
          <div className="space-y-2">
            <Label>SQL Query</Label>
            <Textarea
              value={sql}
              onChange={(e) => setSql(e.target.value)}
              className="font-mono text-sm min-h-[150px] bg-background border-border resize-none"
              placeholder="Enter your SQL query..."
            />
            <Button onClick={handleRun} disabled={isRunning} className="w-full gap-2">
              {isRunning ? (
                <>
                  <Loader2 className="h-4 w-4 animate-spin" />
                  Running...
                </>
              ) : (
                <>
                  <Play className="h-4 w-4" />
                  Run Query
                </>
              )}
            </Button>
          </div>

          {/* Preview */}
          {previewData && (
            <div className="space-y-2 animate-slide-up">
              <Label>Preview ({previewData.rowCount} rows)</Label>
              <div className="border border-border rounded-lg overflow-hidden">
                <div className="overflow-x-auto">
                  <table className="w-full text-sm">
                    <thead className="bg-muted/50">
                      <tr>
                        {previewData.columns.map((col: any) => (
                          <th key={col.name} className="px-3 py-2 text-left font-medium text-muted-foreground">
                            {col.name}
                            <span className="ml-1 text-xs opacity-60">{col.type}</span>
                          </th>
                        ))}
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-border">
                      {previewData.rows.slice(0, 5).map((row: any, i: number) => (
                        <tr key={i} className="hover:bg-muted/30">
                          {previewData.columns.map((col: any) => (
                            <td key={col.name} className="px-3 py-2 font-mono text-xs">
                              {String(row[col.name])}
                            </td>
                          ))}
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            </div>
          )}

          {/* Save Form */}
          {previewData && (
            <div className="space-y-4 pt-4 border-t border-border animate-slide-up">
              <div className="space-y-2">
                <Label>Table Name</Label>
                <Input
                  value={tableName}
                  onChange={(e) => setTableName(e.target.value)}
                  placeholder="My Sales Data"
                  className="bg-background"
                />
              </div>
              <div className="space-y-2">
                <Label>Tags (comma-separated)</Label>
                <Input
                  value={tags}
                  onChange={(e) => setTags(e.target.value)}
                  placeholder="sales, q4, revenue"
                  className="bg-background"
                />
              </div>
              <Button onClick={handleSave} variant="default" className="w-full gap-2">
                <Save className="h-4 w-4" />
                Save as Table Asset
              </Button>
            </div>
          )}
        </div>
      </SheetContent>
    </Sheet>
  );
};

export default NewTableDrawer;
