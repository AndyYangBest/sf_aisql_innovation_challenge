import { useState } from "react";
import { useNavigate, useLocation } from "react-router-dom";
import { Database, Layout, Lightbulb, Search, Menu } from "lucide-react";
import { cn } from "@/lib/utils";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";

interface TopNavProps {
  onNewTable?: () => void;
}

const navItems = [
  { id: "tables", label: "Tables", icon: Database, path: "/" },
  { id: "workspaces", label: "Workspaces", icon: Layout, path: "/workspaces" },
  { id: "insights", label: "Insights", icon: Lightbulb, path: "/insights" },
];

const TopNav = ({ onNewTable }: TopNavProps) => {
  const navigate = useNavigate();
  const location = useLocation();
  const [searchOpen, setSearchOpen] = useState(false);

  const isActive = (path: string) => {
    if (path === "/") return location.pathname === "/" || location.pathname.startsWith("/tables");
    return location.pathname.startsWith(path);
  };

  return (
    <header className="h-14 border-b border-border/50 glass sticky top-0 z-50">
      <div className="h-full px-4 flex items-center justify-between">
        {/* Logo & Nav */}
        <div className="flex items-center gap-6">
          <div className="flex items-center gap-2 cursor-pointer" onClick={() => navigate("/")}>
            <div className="p-1.5 rounded-lg bg-primary/10">
              <Database className="h-5 w-5 text-primary" />
            </div>
            <span className="font-semibold text-lg hidden sm:block">TableSpace</span>
          </div>

          <nav className="flex items-center gap-1">
            {navItems.map((item) => (
              <button
                key={item.id}
                onClick={() => navigate(item.path)}
                className={cn(
                  "flex items-center gap-2 px-3 py-1.5 rounded-lg text-sm font-medium transition-all",
                  isActive(item.path)
                    ? "bg-primary/10 text-primary"
                    : "text-muted-foreground hover:text-foreground hover:bg-muted/50"
                )}
              >
                <item.icon className="h-4 w-4" />
                <span className="hidden md:inline">{item.label}</span>
              </button>
            ))}
          </nav>
        </div>

        {/* Search & Actions */}
        <div className="flex items-center gap-3">
          <div className={cn(
            "transition-all duration-200",
            searchOpen ? "w-64" : "w-8"
          )}>
            {searchOpen ? (
              <Input
                placeholder="Search tables, insights..."
                className="h-8 bg-muted/50"
                autoFocus
                onBlur={() => setSearchOpen(false)}
              />
            ) : (
              <Button
                variant="ghost"
                size="icon"
                className="h-8 w-8"
                onClick={() => setSearchOpen(true)}
              >
                <Search className="h-4 w-4" />
              </Button>
            )}
          </div>
          {onNewTable && (
            <Button size="sm" onClick={onNewTable} className="hidden sm:flex">
              + New Table
            </Button>
          )}
        </div>
      </div>
    </header>
  );
};

export default TopNav;
