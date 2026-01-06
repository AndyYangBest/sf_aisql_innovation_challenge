import { History } from "lucide-react";
import { Button } from "@/components/ui/button";
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from "@/components/ui/popover";
import { ChangelogEntry } from "@/types";

const formatTimeAgo = (timestamp: string): string => {
  const now = new Date();
  const then = new Date(timestamp);
  const diff = Math.floor((now.getTime() - then.getTime()) / 1000);
  if (diff < 60) return "just now";
  if (diff < 3600) return `${Math.floor(diff / 60)}m ago`;
  if (diff < 86400) return `${Math.floor(diff / 3600)}h ago`;
  return `${Math.floor(diff / 86400)}d ago`;
};

const actionLabels: Record<ChangelogEntry["action"], string> = {
  save_insight: "Saved insight",
  save_chart: "Saved chart",
  save_doc: "Saved doc",
  delete: "Deleted",
  pin: "Pinned",
  unpin: "Unpinned",
};

const ChangelogItem = ({ entry }: { entry: ChangelogEntry }) => (
  <div className="px-3 py-2 hover:bg-muted/50 transition-colors">
    <div className="flex items-center justify-between gap-2">
      <span className="text-xs text-foreground truncate flex-1">
        {actionLabels[entry.action]}
      </span>
      <span className="text-[10px] text-muted-foreground whitespace-nowrap">
        {formatTimeAgo(entry.timestamp)}
      </span>
    </div>
    <p className="text-[10px] text-muted-foreground truncate">{entry.artifactTitle}</p>
  </div>
);

interface AIChangelogPopoverProps {
  changelog: ChangelogEntry[];
}

const AIChangelogPopover = ({ changelog }: AIChangelogPopoverProps) => {
  return (
    <Popover>
      <PopoverTrigger asChild>
        <Button variant="ghost" size="icon-sm" className="relative">
          <History className="w-4 h-4" />
          {changelog.length > 0 && (
            <span className="absolute -top-0.5 -right-0.5 w-3.5 h-3.5 bg-primary text-primary-foreground text-[9px] rounded-full flex items-center justify-center">
              {changelog.length > 9 ? "9+" : changelog.length}
            </span>
          )}
        </Button>
      </PopoverTrigger>
      <PopoverContent align="end" className="w-64 p-0">
        <div className="p-2 border-b border-border">
          <span className="text-xs font-medium">Activity Log</span>
        </div>
        <div className="h-64 overflow-y-auto scrollbar-thin">
          {changelog.length === 0 ? (
            <p className="text-xs text-muted-foreground p-3 text-center">No activity yet</p>
          ) : (
            <div className="divide-y divide-border">
              {changelog.slice(0, 20).map((entry) => (
                <ChangelogItem key={entry.id} entry={entry} />
              ))}
            </div>
          )}
        </div>
      </PopoverContent>
    </Popover>
  );
};

export default AIChangelogPopover;
