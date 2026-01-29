import { useState, useMemo, useEffect } from "react";
import { useNavigate } from "react-router-dom";
import {
  Clock,
  Tag,
  ChevronRight,
  Sparkles,
  TrendingUp,
  ShieldCheck,
  Search,
  Plus,
  Table,
  Loader2,
} from "lucide-react";
import { useTableStore } from "@/store/tableStore";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import NewTableDrawer from "@/components/NewTableDrawer";
import { StatCard } from "@/components/shared/StatCard";
import { PageContainer, PageHeader } from "@/components/shared/PageContainer";
import StatusBar from "@/components/StatusBar";
import { tablesApi } from "@/api/tables";
import { useToast } from "@/hooks/use-toast";

const TablesPage = () => {
  const [drawerOpen, setDrawerOpen] = useState(false);
  const [searchQuery, setSearchQuery] = useState("");
  const [isLoading, setIsLoading] = useState(true);
  const { tableAssets, artifacts, setTableAssets, loadReport, reportStatus, getApprovedPlansCount } = useTableStore();
  const navigate = useNavigate();
  const { toast } = useToast();

  // Load tables from database on mount
  useEffect(() => {
    loadTablesFromDatabase();
  }, []);

  useEffect(() => {
    if (!tableAssets.length) return;
    tableAssets.forEach((table) => {
      const status = reportStatus[table.id];
      if (!status?.loaded) {
        loadReport(table.id).catch((error) => {
          console.error("Failed to load report metadata", error);
        });
      }
    });
  }, [tableAssets, loadReport, reportStatus]);

  const loadTablesFromDatabase = async () => {
    setIsLoading(true);
    try {
      const response = await tablesApi.getAllTableAssets();
      if (response.status === "success" && response.data) {
        setTableAssets(response.data.items);
      } else if (response.status === "unauthorized") {
        // Handle authentication error - show specific message
        toast({
          title: "Authentication Required",
          description: "Your Snowflake session has expired. Please refresh the page and re-authenticate.",
          variant: "destructive",
          duration: 10000, // Show for 10 seconds
        });
        // Optionally, redirect to login page or trigger re-authentication
        // For now, we'll just show the error message
        // In a full implementation, you might want to:
        // 1. Clear any stored tokens
        // 2. Redirect to a login page
        // 3. Show a re-authentication modal
      } else {
        toast({
          title: "Failed to load tables",
          description: response.error || "Could not fetch table assets",
          variant: "destructive",
        });
      }
    } catch (error) {
      toast({
        title: "Failed to load tables",
        description: "Could not connect to server",
        variant: "destructive",
      });
    } finally {
      setIsLoading(false);
    }
  };

  // Filter tables by search query
  const filteredTables = useMemo(() => {
    if (!searchQuery.trim()) return tableAssets;
    const query = searchQuery.toLowerCase();
    return tableAssets.filter(
      (table) =>
        table.name.toLowerCase().includes(query) ||
        table.tags.some((tag) => tag.toLowerCase().includes(query)) ||
        table.database?.toLowerCase().includes(query) ||
        table.schema?.toLowerCase().includes(query),
    );
  }, [tableAssets, searchQuery]);

  const formatDate = (dateString: string) => {
    const date = new Date(dateString);
    return date.toLocaleDateString("en-US", {
      month: "short",
      day: "numeric",
      hour: "2-digit",
      minute: "2-digit",
    });
  };

  const getTableInsightCount = (tableId: string) => {
    return artifacts.filter(
      (a) => a.tableId === tableId && a.type === "insight",
    ).length;
  };

  const chartCount = useMemo(
    () => new Set(artifacts.filter((a) => a.type === "chart").map((a) => a.id)).size,
    [artifacts]
  );
  const insightCount = useMemo(
    () => new Set(artifacts.filter((a) => a.type === "insight").map((a) => a.id)).size,
    [artifacts]
  );
  const approvedPlansCount = getApprovedPlansCount();

  return (
    <div className="min-h-screen bg-background pb-10">
      {/* Header */}
      <PageHeader>
        <div className="flex items-center justify-between gap-3">
          <div className="flex items-center gap-2 sm:gap-3 min-w-0">
            <div className="p-1 sm:p-1.5 rounded-lg bg-primary/10 flex-shrink-0">
              <img
                src="/black-theme.svg"
                alt="Scrat"
                className="h-12 w-12 sm:h-14 sm:w-14 dark:hidden"
              />
              <img
                src="/white-theme.svg"
                alt="Scrat"
                className="h-12 w-12 sm:h-14 sm:w-14 hidden dark:block"
              />
            </div>
            <div className="min-w-0">
              <h1 className="text-lg sm:text-xl font-bold truncate" style={{ fontFamily: "'Orbitron', sans-serif", letterSpacing: '0.05em' }}>Scrat</h1>
              <p className="text-xs text-muted-foreground hidden sm:block" style={{ fontFamily: "'Fredoka One', cursive", fontSize: '0.85rem' }}>
                Digesting Yummy Snowflake large Data
              </p>
            </div>
          </div>
          <Button
            onClick={() => setDrawerOpen(true)}
            size="sm"
            className="flex-shrink-0"
          >
            <Plus className="h-4 w-4 sm:mr-2" />
            <span className="hidden sm:inline">New Table</span>
          </Button>
        </div>
      </PageHeader>

      <PageContainer>
        {/* Search Bar */}
        <div className="mb-6 sm:mb-8">
          <div className="relative w-full sm:max-w-md">
            <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
            <Input
              placeholder="Search tables..."
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              className="pl-10 h-10 sm:h-11 bg-card border-border"
            />
          </div>
          {searchQuery && (
            <p className="text-sm text-muted-foreground mt-2">
              Found {filteredTables.length} table
              {filteredTables.length !== 1 && "s"}
            </p>
          )}
        </div>

        {/* Quick Stats - 响应式网格 */}
        <div className="grid grid-cols-2 lg:grid-cols-4 gap-3 sm:gap-4 mb-6 sm:mb-8">
          <StatCard
            icon={Table}
            value={tableAssets.length}
            label="Tables"
            variant="primary"
          />
          <StatCard
            icon={TrendingUp}
            value={chartCount}
            label="Charts"
            variant="success"
          />
          <StatCard
            icon={Sparkles}
            value={insightCount}
            label="Insights"
            variant="warning"
          />
          <StatCard
            icon={ShieldCheck}
            value={approvedPlansCount}
            label="Approved Plans"
            variant="info"
          />
        </div>

        {/* Table List */}
        <div className="space-y-2 sm:space-y-3">
          <h2 className="text-base sm:text-lg font-semibold mb-3 sm:mb-4">
            All Tables
          </h2>

          {isLoading ? (
            <div className="text-center py-8 sm:py-12">
              <Loader2 className="h-10 w-10 sm:h-12 sm:w-12 mx-auto mb-3 animate-spin text-primary" />
              <p className="text-sm sm:text-base text-muted-foreground">
                Loading tables...
              </p>
            </div>
          ) : filteredTables.length === 0 ? (
            <div className="text-center py-8 sm:py-12 text-muted-foreground">
              <Table className="h-10 w-10 sm:h-12 sm:w-12 mx-auto mb-3 opacity-30" />
              <p className="text-sm sm:text-base">
                {searchQuery
                  ? `No tables found matching "${searchQuery}"`
                  : "No tables yet. Create your first table!"}
              </p>
            </div>
          ) : (
            filteredTables.map((table) => {
              const tableInsightCount = getTableInsightCount(table.id);
              return (
                <div
                  key={table.id}
                  onClick={() => navigate(`/tables/${table.id}`)}
                  className="p-3 sm:p-5 rounded-xl bg-card border border-border hover:border-primary/50 hover:shadow-lg hover:shadow-primary/5 cursor-pointer transition-all duration-200 group"
                >
                  <div className="flex items-start justify-between gap-2">
                    <div className="flex-1 min-w-0">
                      {/* 标题和洞察徽章 */}
                      <div className="flex flex-wrap items-center gap-2 mb-1 sm:mb-2">
                        <h3 className="text-base sm:text-lg font-semibold group-hover:text-primary transition-colors truncate">
                          {table.name}
                        </h3>
                        {tableInsightCount > 0 && (
                          <Badge
                            variant="secondary"
                            className="bg-warning/10 text-warning border-warning/20 text-xs flex-shrink-0"
                          >
                            <Sparkles className="h-3 w-3 mr-1" />
                            {tableInsightCount}
                          </Badge>
                        )}
                      </div>

                      {/* 数据库路径 - 移动端隐藏或截断 */}
                      <div className="font-mono text-xs text-muted-foreground mb-2 sm:mb-3 truncate">
                        {table.database?.toLowerCase()}.
                        {table.schema?.toLowerCase()}.
                        {table.name.toLowerCase().replace(/\s+/g, "_")}
                      </div>

                      {/* 元信息 */}
                      <div className="flex flex-wrap items-center gap-x-3 gap-y-1 text-xs sm:text-sm text-muted-foreground">
                        <span className="flex items-center gap-1">
                          <Clock className="h-3 w-3 sm:h-3.5 sm:w-3.5" />
                          {formatDate(table.updatedAt)}
                        </span>
                        {table.owner && (
                          <span className="hidden sm:inline">
                            by {table.owner}
                          </span>
                        )}
                      </div>

                      {/* 标签 */}
                      {table.tags.length > 0 && (
                        <div className="flex flex-wrap gap-1.5 sm:gap-2 mt-2 sm:mt-3">
                          {table.tags.slice(0, 3).map((tag) => (
                            <Badge
                              key={tag}
                              variant="outline"
                              className="text-xs px-1.5 sm:px-2"
                            >
                              <Tag className="h-2.5 w-2.5 sm:h-3 sm:w-3 mr-0.5 sm:mr-1" />
                              {tag}
                            </Badge>
                          ))}
                          {table.tags.length > 3 && (
                            <Badge
                              variant="outline"
                              className="text-xs px-1.5 sm:px-2"
                            >
                              +{table.tags.length - 3}
                            </Badge>
                          )}
                        </div>
                      )}
                    </div>

                    <ChevronRight className="h-4 w-4 sm:h-5 sm:w-5 text-muted-foreground group-hover:text-primary group-hover:translate-x-1 transition-all mt-1 flex-shrink-0" />
                  </div>
                </div>
              );
            })
          )}
        </div>
      </PageContainer>

      <StatusBar />
      <NewTableDrawer open={drawerOpen} onOpenChange={setDrawerOpen} />
    </div>
  );
};

export default TablesPage;
