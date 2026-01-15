import { useState, useEffect } from "react";
import { useNavigate } from "react-router-dom";
import { X, Play, Save, Loader2, Database, Table as TableIcon, RefreshCw, Sparkles, Minus, Plus } from "lucide-react";
import { Sheet, SheetContent, SheetHeader, SheetTitle } from "@/components/ui/sheet";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Textarea } from "@/components/ui/textarea";
import { useTableStore } from "@/store/tableStore";
import { useToast } from "@/hooks/use-toast";
import { tablesApi, SnowflakeTable } from "@/api/tables";
import { Badge } from "@/components/ui/badge";
import { ScrollArea } from "@/components/ui/scroll-area";
import PreviewTable from "@/components/PreviewTable";

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
  const [isSuggesting, setIsSuggesting] = useState(false);
  const [previewData, setPreviewData] = useState<any>(null);
  const [snowflakeTables, setSnowflakeTables] = useState<SnowflakeTable[]>([]);
  const [isLoadingTables, setIsLoadingTables] = useState(false);
  const [showTableList, setShowTableList] = useState(true);
  const [searchTable, setSearchTable] = useState("");
  const [aiSummary, setAiSummary] = useState("");
  const [useCases, setUseCases] = useState<string[]>([]);
  const [numericColumns, setNumericColumns] = useState<string[]>([]);
  const [isProfilingColumns, setIsProfilingColumns] = useState(false);
  const [colWidth, setColWidth] = useState(200);
  const { addTableAsset, setTableResult } = useTableStore();
  const { toast } = useToast();
  const navigate = useNavigate();

  // Fetch Snowflake tables when drawer opens
  useEffect(() => {
    if (open) {
      fetchSnowflakeTables();
    }
  }, [open]);

  const fetchSnowflakeTables = async () => {
    setIsLoadingTables(true);
    try {
      const response = await tablesApi.getSnowflakeTables();
      if (response.status === 'success' && response.data) {
        setSnowflakeTables(response.data);
      } else {
        toast({
          title: "Failed to load tables",
          description: response.error || "Unknown error",
          variant: "destructive",
        });
      }
    } catch (error) {
      toast({
        title: "Failed to load tables",
        description: "Could not connect to Snowflake",
        variant: "destructive",
      });
    } finally {
      setIsLoadingTables(false);
    }
  };

  const handleTableSelect = (table: SnowflakeTable) => {
    const fullTableName = `${table.DATABASE_NAME}.${table.SCHEMA_NAME}.${table.TABLE_NAME}`;
    setSql(`SELECT * FROM ${fullTableName} LIMIT 100`);

    // 清除之前的数据
    setPreviewData(null);
    setTableName("");
    setTags("");
    setAiSummary("");
    setUseCases([]);

    // 不关闭侧边栏，让用户可以继续浏览其他表
    toast({
      title: "Table selected",
      description: `Selected ${table.TABLE_NAME}`,
    });
  };

  const filteredTables = snowflakeTables.filter((table) => {
    // 只显示 PUBLIC schema 的表
    if (table.SCHEMA_NAME !== 'PUBLIC') {
      return false;
    }
    // 搜索过滤
    const searchLower = searchTable.toLowerCase();
    return (
      table.TABLE_NAME.toLowerCase().includes(searchLower) ||
      table.SCHEMA_NAME.toLowerCase().includes(searchLower) ||
      table.DATABASE_NAME.toLowerCase().includes(searchLower)
    );
  });

  const handleRun = async () => {
    setIsRunning(true);
    try {
      const response = await tablesApi.executeSql(sql, 50);
      if (response.status === 'success' && response.data && response.data.success) {
        const initialNumeric = inferNumericFromTypes(response.data.columns, response.data.rows);
        setPreviewData({
          columns: response.data.columns,
          rows: response.data.rows,
          rowCount: response.data.row_count,
        });
        setNumericColumns(initialNumeric);

        // Fire column profiling to find numeric-only columns
        profileNumericColumns(sql, response.data.columns, initialNumeric);

        toast({
          title: "Query executed",
          description: `Returned ${response.data.row_count} rows`,
        });

        // 自动触发 AI 建议
        handleSuggestMetadata();
      } else {
        toast({
          title: "Query failed",
          description: response.data?.error || "Please check your SQL syntax",
          variant: "destructive",
        });
      }
    } catch (error) {
      toast({
        title: "Query failed",
        description: "Could not execute query",
        variant: "destructive",
      });
    } finally {
      setIsRunning(false);
    }
  };

  const inferNumericFromTypes = (
    columns: Array<{ name: string; type?: string }>,
    rows: Array<Record<string, any>> = []
  ) => {
    const numericRegex = /(number|int|decimal|float|double|real|byte|long)/i;
    const byType = columns
      .filter((col) => col.type && numericRegex.test(col.type))
      .map((col) => col.name);

    // Value-based inference as fallback when types are missing
    const sample = rows.slice(0, 20);
    const byValue = columns
      .filter((col) => {
        const values = sample.map((r) => r[col.name]);
        if (!values.length) return false;
        return values.every((v) => {
          if (v === null || v === undefined) return true;
          if (typeof v === "number") return Number.isFinite(v);
          const parsed = Number(v);
          return Number.isFinite(parsed);
        });
      })
      .map((col) => col.name);

    return Array.from(new Set([...byType, ...byValue]));
  };

  const sanitizeSql = (query: string) => query.trim().replace(/;+\s*$/, "");

  const buildNumericProfileSql = (baseSql: string, columns: Array<{ name: string }>) => {
    const cleanSql = sanitizeSql(baseSql);
    const checks = columns.map((col) => {
      const colName = col.name.replace(/"/g, '""');
      return `SUM(CASE WHEN TRY_TO_NUMBER("${colName}") IS NULL AND "${colName}" IS NOT NULL THEN 1 ELSE 0 END) AS "${colName}"`;
    });

    return `
WITH src AS (
  SELECT * FROM (
    ${cleanSql}
  ) LIMIT 500
)
SELECT
  ${checks.join(",\n  ")}
FROM src
LIMIT 1;
`.trim();
  };

  const profileNumericColumns = async (baseSql: string, columns: Array<{ name: string }>, seedNumeric: string[]) => {
    if (!columns.length) return;
    setIsProfilingColumns(true);
    try {
      const profileSql = buildNumericProfileSql(baseSql, columns);
      const profileResponse = await tablesApi.executeSql(profileSql, 1);
      if (profileResponse.status === 'success' && profileResponse.data?.success && profileResponse.data.rows?.[0]) {
        const row = profileResponse.data.rows[0] as Record<string, any>;
        const numericCols = columns
          .map((col) => {
            const val = row[col.name];
            const nonNumericCount = typeof val === 'number' ? val : parseInt(val, 10);
            return { name: col.name, isNumeric: Number.isFinite(nonNumericCount) ? nonNumericCount === 0 : false };
          })
          .filter((c) => c.isNumeric)
          .map((c) => c.name);
        const merged = Array.from(new Set([...seedNumeric, ...numericCols]));
        setNumericColumns(merged);
      } else {
        setNumericColumns(seedNumeric);
      }
    } catch (error) {
      console.error("Column profiling failed", error);
      setNumericColumns(seedNumeric);
    } finally {
      setIsProfilingColumns(false);
    }
  };

  const handleSuggestMetadata = async () => {
    setIsSuggesting(true);
    try {
      const response = await tablesApi.suggestMetadata(
        sql,
        tableName,
        previewData?.columns,
        previewData?.rows.slice(0, 5) // Send first 5 rows as samples
      );
      console.log('AI Suggestion Response:', response);

      if (response.status === 'success' && response.data) {
        const metadata = response.data;
        console.log('Metadata:', metadata);

        // 直接使用返回的数据，不检查 success 字段
        setTableName(metadata.suggested_name || '');
        setTags((metadata.suggested_tags || []).join(", "));
        setAiSummary(metadata.ai_summary || "");
        setUseCases(metadata.use_cases || []);

        toast({
          title: "AI suggestions generated",
          description: "Table metadata auto-filled with AI suggestions",
        });
      } else {
        toast({
          title: "Suggestion failed",
          description: response.error || "Could not generate suggestions",
          variant: "destructive",
        });
      }
    } catch (error) {
      console.error('AI Suggestion Error:', error);
      toast({
        title: "Suggestion failed",
        description: "Could not connect to AI service",
        variant: "destructive",
      });
    } finally {
      setIsSuggesting(false);
    }
  };

  const handleSave = async () => {
    if (!tableName.trim()) {
      toast({
        title: "Name required",
        description: "Please enter a name for this table",
        variant: "destructive",
      });
      return;
    }

    try {
      const response = await tablesApi.saveTableAsset({
        name: tableName,
        source_sql: sql,
        database: "AI_SQL_COMP",
        schema: "PUBLIC",
        tags: tags.split(",").map((t) => t.trim()).filter(Boolean),
        owner: "current_user",
        ai_summary: aiSummary,
        use_cases: useCases,
      });

      if (response.status === 'success' && response.data) {
        addTableAsset(response.data);

        // Store the result if we have preview data
        if (previewData) {
          setTableResult(response.data.id, previewData);
        }

        toast({
          title: "Table saved",
          description: `"${tableName}" has been created`,
        });

        navigate(`/tables/${response.data.id}`);
        handleClose();
      } else {
        toast({
          title: "Failed to save",
          description: response.error || "Could not save table asset",
          variant: "destructive",
        });
      }
    } catch (error) {
      toast({
        title: "Failed to save",
        description: "Could not save table asset",
        variant: "destructive",
      });
    }
  };

  const handleClose = () => {
    setSql("SELECT * FROM your_table LIMIT 100");
    setTableName("");
    setTags("");
    setAiSummary("");
    setUseCases([]);
    setPreviewData(null);
    setNumericColumns([]);
    setIsProfilingColumns(false);
    setColWidth(200);
    setShowTableList(true);
    setSearchTable("");
    onOpenChange(false);
  };

  return (
    <Sheet open={open} onOpenChange={handleClose}>
      <SheetContent className="w-full sm:max-w-4xl glass border-border overflow-hidden flex flex-col p-0">
        <SheetHeader className="p-6 pb-4">
          <SheetTitle className="flex items-center justify-between">
            <span>New Table from SQL</span>
            <Button
              variant="ghost"
              size="sm"
              onClick={() => setShowTableList(!showTableList)}
              className="gap-2"
            >
              <Database className="h-4 w-4" />
              {showTableList ? "Hide Tables" : "Show Tables"}
            </Button>
          </SheetTitle>
        </SheetHeader>

        <div className="flex-1 overflow-hidden flex">
          {/* Table List Panel */}
          {showTableList && (
            <div className="w-80 flex flex-col border-r border-border p-6 pt-0">
              <div className="flex items-center justify-between mb-3">
                <Label className="flex items-center gap-2">
                  <TableIcon className="h-4 w-4" />
                  Snowflake Tables
                </Label>
                <Button
                  variant="ghost"
                  size="sm"
                  onClick={fetchSnowflakeTables}
                  disabled={isLoadingTables}
                >
                  <RefreshCw className={`h-4 w-4 ${isLoadingTables ? 'animate-spin' : ''}`} />
                </Button>
              </div>

              <Input
                placeholder="Search tables..."
                value={searchTable}
                onChange={(e) => setSearchTable(e.target.value)}
                className="mb-3 h-9"
              />

              {isLoadingTables ? (
                <div className="flex items-center justify-center py-8">
                  <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
                </div>
              ) : (
                <ScrollArea className="flex-1 -mx-2 px-2">
                  <div className="space-y-2">
                    {filteredTables.length === 0 ? (
                      <div className="text-center py-8 text-sm text-muted-foreground">
                        No tables found
                      </div>
                    ) : (
                      filteredTables.map((table) => (
                        <div
                          key={`${table.DATABASE_NAME}.${table.SCHEMA_NAME}.${table.TABLE_NAME}`}
                          onClick={() => handleTableSelect(table)}
                          className="p-3 rounded-lg border border-border hover:border-primary/50 hover:bg-accent cursor-pointer transition-all group"
                        >
                          <div className="font-medium text-sm group-hover:text-primary transition-colors break-words">
                            {table.TABLE_NAME}
                          </div>
                          <div className="text-xs text-muted-foreground font-mono mt-1 break-words">
                            {table.DATABASE_NAME}.{table.SCHEMA_NAME}
                          </div>
                          <div className="flex items-center gap-2 mt-2">
                            <Badge variant="outline" className="text-xs shrink-0">
                              {table.TABLE_TYPE}
                            </Badge>
                            {table.ROW_COUNT > 0 && (
                              <span className="text-xs text-muted-foreground shrink-0">
                                {table.ROW_COUNT.toLocaleString()} rows
                              </span>
                            )}
                          </div>
                        </div>
                      ))
                    )}
                  </div>
                </ScrollArea>
              )}
            </div>
          )}

          {/* SQL Editor Panel */}
          <div className="flex-1 flex flex-col overflow-hidden">
            <div className="flex-1 overflow-auto">
              <div className="space-y-6 p-6 pt-0">
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
                    <div className="flex items-center justify-between gap-3">
                      <Label>Preview ({previewData.rowCount} rows)</Label>
                      <div className="flex items-center gap-2 text-xs text-muted-foreground">
                        <span>Col width</span>
                        <div className="flex items-center gap-1">
                          <Button
                            type="button"
                            variant="outline"
                            size="icon"
                            className="h-7 w-7"
                            onClick={() => setColWidth((w) => Math.max(40, w - 20))}
                          >
                            <Minus className="h-3 w-3" />
                          </Button>
                          <span className="w-10 text-center tabular-nums">{colWidth}</span>
                          <Button
                            type="button"
                            variant="outline"
                            size="icon"
                            className="h-7 w-7"
                            onClick={() => setColWidth((w) => Math.min(320, w + 20))}
                          >
                            <Plus className="h-3 w-3" />
                          </Button>
                        </div>
                      </div>
                    </div>
                    <div className="text-xs text-muted-foreground flex items-center gap-2">
                      {isProfilingColumns ? (
                        <span>Profiling columns for numeric-only data…</span>
                      ) : numericColumns.length > 0 ? (
                        <span>Numeric columns: {numericColumns.join(", ")}</span>
                      ) : (
                        <span>No numeric columns detected</span>
                      )}
                    </div>
                    <PreviewTable
                      columns={previewData.columns}
                      rows={previewData.rows}
                      rowLimit={10}
                      numericColumns={numericColumns}
                      colWidth={colWidth}
                    />
                  </div>
                )}

                {/* Save Form */}
                {previewData && (
                  <form autoComplete="off" onSubmit={(e) => { e.preventDefault(); handleSave(); }} className="space-y-4 pt-4 border-t border-border animate-slide-up">
                    <div className="space-y-2">
                      <Label>Table Name</Label>
                      <Input
                        value={tableName}
                        onChange={(e) => setTableName(e.target.value)}
                        placeholder="My Sales Data"
                        className="bg-background"
                        autoComplete="off"
                        name="table-name-unique"
                        id="table-name-unique"
                      />
                    </div>
                    <div className="space-y-2">
                      <Label>Tags (comma-separated)</Label>
                      <Input
                        value={tags}
                        onChange={(e) => setTags(e.target.value)}
                        placeholder="sales, q4, revenue"
                        className="bg-background"
                        autoComplete="off"
                        name="table-tags-unique"
                        id="table-tags-unique"
                      />
                    </div>

                    {/* AI-generated summary and use cases - read only display */}
                    {(aiSummary || useCases.length > 0) && (
                      <div className="space-y-3 p-4 rounded-lg bg-muted/30 border border-border">
                        <div className="flex items-center gap-2 text-sm font-medium text-muted-foreground">
                          <Sparkles className="h-4 w-4" />
                          AI-Generated Insights
                        </div>

                        {aiSummary && (
                          <div className="space-y-1">
                            <Label className="text-xs text-muted-foreground">Summary</Label>
                            <p className="text-sm">{aiSummary}</p>
                          </div>
                        )}

                        {useCases.length > 0 && (
                          <div className="space-y-1">
                            <Label className="text-xs text-muted-foreground">Use Cases</Label>
                            <ul className="text-sm space-y-1">
                              {useCases.map((useCase, i) => (
                                <li key={i} className="flex items-start gap-2">
                                  <span className="text-primary mt-0.5">•</span>
                                  <span>{useCase}</span>
                                </li>
                              ))}
                            </ul>
                          </div>
                        )}
                      </div>
                    )}

                    <Button type="submit" variant="default" className="w-full gap-2">
                      <Save className="h-4 w-4" />
                      Save as Table Asset
                    </Button>
                  </form>
                )}
              </div>
            </div>
          </div>
        </div>
      </SheetContent>
    </Sheet>
  );
};

export default NewTableDrawer;
