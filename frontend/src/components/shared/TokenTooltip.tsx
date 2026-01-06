/**
 * TokenTooltip - Token 使用量提示组件
 * 可复用的 token 计数显示
 */

import { Coins } from 'lucide-react';
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from '@/components/ui/tooltip';

export interface TokenUsage {
  context: number;
  output: number;
  total: number;
}

interface TokenTooltipProps {
  usage?: TokenUsage;
  className?: string;
}

const defaultUsage: TokenUsage = {
  context: 8200,
  output: 4250,
  total: 12450,
};

export function TokenTooltip({ usage = defaultUsage, className }: TokenTooltipProps) {
  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <div className={`flex items-center gap-1.5 text-xs text-muted-foreground cursor-default hover:text-foreground transition-colors ${className}`}>
          <Coins className="h-3.5 w-3.5" />
          <span>{usage.total.toLocaleString()}</span>
        </div>
      </TooltipTrigger>
      <TooltipContent side="bottom" className="text-xs">
        <div className="space-y-1">
          <div className="flex justify-between gap-4">
            <span className="text-muted-foreground">Context:</span>
            <span>{usage.context.toLocaleString()}</span>
          </div>
          <div className="flex justify-between gap-4">
            <span className="text-muted-foreground">Output:</span>
            <span>{usage.output.toLocaleString()}</span>
          </div>
          <div className="border-t border-border pt-1 flex justify-between gap-4 font-medium">
            <span>Total:</span>
            <span>{usage.total.toLocaleString()}</span>
          </div>
        </div>
      </TooltipContent>
    </Tooltip>
  );
}
