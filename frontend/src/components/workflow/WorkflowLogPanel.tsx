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
}

export const WorkflowLogPanel = ({
  logs,
  isRunning,
  onClose,
  className,
}: WorkflowLogPanelProps) => {
  const [isExpanded, setIsExpanded] = useState(true);
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
      default:
        return 'text-foreground';
    }
  };

  return (
    <div
      className={cn(
        'w-full shrink-0 bg-background border-t shadow-lg transition-all duration-300',
        isExpanded ? 'h-80' : 'h-12',
        className
      )}
    >
      {/* Header */}
      <div className="flex items-center justify-between px-4 py-2 border-b bg-muted/50">
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
            onClick={() => setIsExpanded(!isExpanded)}
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
        <ScrollArea className="h-[calc(100%-3rem)]" ref={scrollRef}>
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
                  <span className={cn('flex-1', getLogColor(log.type))}>
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
