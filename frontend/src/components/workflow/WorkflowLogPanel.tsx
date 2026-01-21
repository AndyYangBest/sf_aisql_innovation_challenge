/**
 * Workflow Log Panel
 * 工作流日志面板 - 显示流式日志输出
 */

import { useEffect, useRef, useState } from 'react';
import { ScrollArea } from '@/components/ui/scroll-area';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { X, ChevronDown, ChevronUp, Terminal, Loader2 } from 'lucide-react';
import { cn } from '@/lib/utils';
import { WorkflowLogEvent } from '@/api/eda';

interface WorkflowLogPanelProps {
  logs: WorkflowLogEvent[];
  isRunning: boolean;
  onClose?: () => void;
  className?: string;
  onExpandedChange?: (expanded: boolean) => void;
  useFlexLayout?: boolean;
}

export const WorkflowLogPanel = ({
  logs,
  isRunning,
  onClose,
  className,
  onExpandedChange,
  useFlexLayout = false,
}: WorkflowLogPanelProps) => {
  const [isExpanded, setIsExpanded] = useState(true);

  const handleToggleExpand = () => {
    const newExpanded = !isExpanded;
    setIsExpanded(newExpanded);
    onExpandedChange?.(newExpanded);
  };
  const [autoScroll, setAutoScroll] = useState(true);
  const scrollRef = useRef<HTMLDivElement>(null);
  const bottomRef = useRef<HTMLDivElement>(null);

  // Auto-scroll to bottom when new logs arrive
  useEffect(() => {
    if (autoScroll && bottomRef.current) {
      bottomRef.current.scrollIntoView({ behavior: 'smooth' });
    }
  }, [logs, autoScroll]);

  // Format timestamp
  const formatTime = (timestamp: string) => {
    const date = new Date(timestamp);
    return date.toLocaleTimeString('en-US', {
      hour12: false,
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit',
      fractionalSecondDigits: 3,
    });
  };

  // Get log type badge
  const getLogTypeBadge = (type: string) => {
    switch (type) {
      case 'log':
        return <Badge variant="outline" className="text-xs">LOG</Badge>;
      case 'status':
        return <Badge variant="default" className="text-xs bg-blue-500">STATUS</Badge>;
      case 'progress':
        return <Badge variant="default" className="text-xs bg-amber-500">PROGRESS</Badge>;
      case 'complete':
        return <Badge variant="default" className="text-xs bg-emerald-500">COMPLETE</Badge>;
      case 'error':
        return <Badge variant="destructive" className="text-xs">ERROR</Badge>;
      case 'strands_log':
        return <Badge variant="outline" className="text-xs bg-purple-50 text-purple-700 border-purple-200">STRANDS</Badge>;
      case 'workflow_log':
        return <Badge variant="outline" className="text-xs bg-indigo-50 text-indigo-700 border-indigo-200">WORKFLOW</Badge>;
      case 'telemetry_log':
        return <Badge variant="outline" className="text-xs bg-slate-50 text-slate-600 border-slate-200">TELEMETRY</Badge>;
      default:
        return <Badge variant="outline" className="text-xs">{type.toUpperCase()}</Badge>;
    }
  };

  // Get log color
  const getLogColor = (type: string) => {
    switch (type) {
      case 'error':
        return 'text-destructive';
      case 'complete':
        return 'text-emerald-500';
      case 'progress':
        return 'text-amber-500';
      case 'status':
        return 'text-blue-500';
      case 'strands_log':
        return 'text-purple-700';
      case 'workflow_log':
        return 'text-indigo-700';
      case 'telemetry_log':
        return 'text-slate-600';
      default:
        return 'text-foreground';
    }
  };

  return (
    <div
      className={cn(
        'w-full bg-background border-t shadow-lg transition-all duration-300',
        useFlexLayout
          ? isExpanded
            ? 'flex-[0.3] min-h-0 flex flex-col'
            : 'h-12 shrink-0'
          : isExpanded
            ? 'h-80 shrink-0'
            : 'h-12 shrink-0',
        className
      )}
    >
      {/* Header */}
      <div className="flex items-center justify-between px-4 py-2 border-b bg-muted/50 shrink-0">
        <div className="flex items-center gap-2">
          <Terminal className="h-4 w-4 text-muted-foreground" />
          <span className="text-sm font-medium">Workflow Logs</span>
          {isRunning && (
            <>
              <Loader2 className="h-3.5 w-3.5 animate-spin text-amber-500" />
              <span className="text-xs text-amber-500">Running...</span>
            </>
          )}
          <Badge variant="outline" className="text-xs">
            {logs.length} {logs.length === 1 ? 'entry' : 'entries'}
          </Badge>
        </div>

        <div className="flex items-center gap-2">
          {/* Auto-scroll toggle */}
          <Button
            variant="ghost"
            size="sm"
            onClick={() => setAutoScroll(!autoScroll)}
            className="h-7 text-xs"
          >
            {autoScroll ? '🔒 Auto-scroll' : '🔓 Manual'}
          </Button>

          {/* Expand/Collapse */}
          <Button
            variant="ghost"
            size="sm"
            onClick={handleToggleExpand}
            className="h-7 w-7 p-0"
          >
            {isExpanded ? (
              <ChevronDown className="h-4 w-4" />
            ) : (
              <ChevronUp className="h-4 w-4" />
            )}
          </Button>

          {/* Close */}
          {onClose && (
            <Button
              variant="ghost"
              size="sm"
              onClick={onClose}
              className="h-7 w-7 p-0"
            >
              <X className="h-4 w-4" />
            </Button>
          )}
        </div>
      </div>

      {/* Log content */}
      {isExpanded && (
        <ScrollArea className={cn(useFlexLayout ? 'flex-1 min-h-0 overflow-hidden' : 'h-[calc(100%-3rem)]')} ref={scrollRef}>
          <div className="p-4 space-y-1 font-mono text-xs">
            {logs.length === 0 ? (
              <div className="text-muted-foreground text-center py-8">
                No logs yet. Waiting for workflow to start...
              </div>
            ) : (
              logs.map((log, index) => (
                <div
                  key={index}
                  className={cn(
                    'flex items-start gap-2 py-1 px-2 rounded hover:bg-muted/50 transition-colors',
                    log.type === 'error' && 'bg-destructive/5'
                  )}
                >
                  {/* Timestamp */}
                  <span className="text-muted-foreground shrink-0">
                    {formatTime(log.timestamp)}
                  </span>

                  {/* Type badge */}
                  <div className="shrink-0">
                    {getLogTypeBadge(log.type)}
                  </div>

                  {/* Message */}
                  <span
                    className={cn('flex-1 whitespace-pre-wrap break-words', getLogColor(log.type))}
                  >
                    {log.message || JSON.stringify(log.data)}
                  </span>
                </div>
              ))
            )}
            <div ref={bottomRef} />
          </div>
        </ScrollArea>
      )}
    </div>
  );
};
