import { Users, FolderOpen, Database, Cloud, Clock } from "lucide-react";

const StatusBar = () => {
  return (
    <div className="fixed bottom-0 left-0 right-0 z-40 border-t border-border bg-card/90 backdrop-blur">
      <div className="h-8 px-4 flex items-center justify-between text-xs text-muted-foreground">
        <div className="flex items-center gap-3">
          <div className="flex items-center gap-1.5">
            <Users className="h-3.5 w-3.5 text-[hsl(var(--viz-blue))]" />
            <span>Members: 3</span>
          </div>
          <span className="opacity-40">•</span>
          <div className="flex items-center gap-1.5">
            <FolderOpen className="h-3.5 w-3.5 text-[hsl(var(--viz-green))]" />
            <span>Project: Scrat Labs</span>
          </div>
          <span className="opacity-40">•</span>
          <div className="flex items-center gap-1.5">
            <Database className="h-3.5 w-3.5 text-[hsl(var(--viz-orange))]" />
            <span>Warehouse: X-Small</span>
          </div>
        </div>

        <div className="flex items-center gap-3">
          <div className="flex items-center gap-1.5">
            <Cloud className="h-3.5 w-3.5 text-[hsl(var(--viz-purple))]" />
            <span>Env: Staging</span>
          </div>
          <span className="opacity-40">•</span>
          <div className="flex items-center gap-1.5">
            <Clock className="h-3.5 w-3.5 text-[hsl(var(--viz-yellow))]" />
            <span>Last sync: 2m ago</span>
          </div>
        </div>
      </div>
    </div>
  );
};

export default StatusBar;
