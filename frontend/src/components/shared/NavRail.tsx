/**
 * NavRail - 垂直导航栏组件
 * 支持 icon + tooltip 的侧边导航
 */

import { LucideIcon } from 'lucide-react';
import { cn } from '@/lib/utils';

export interface NavItem {
  id: string;
  label: string;
  icon: LucideIcon;
  color?: string;
}

interface NavRailProps {
  items: NavItem[];
  activeId: string;
  onSelect: (id: string) => void;
  className?: string;
}

export function NavRail({ items, activeId, onSelect, className }: NavRailProps) {
  return (
    <div className={cn(
      'w-14 border-r border-border bg-card/50 flex flex-col items-center py-4 gap-1 flex-shrink-0',
      className
    )}>
      {items.map((item) => (
        <button
          key={item.id}
          onClick={() => onSelect(item.id)}
          className={cn(
            'p-3 rounded-lg transition-all duration-200 group relative',
            activeId === item.id
              ? 'bg-primary text-primary-foreground shadow-md'
              : 'text-muted-foreground hover:text-foreground hover:bg-muted'
          )}
        >
          <item.icon className={cn('h-5 w-5', activeId !== item.id && item.color)} />
          <span className="absolute left-full ml-2 px-2 py-1 rounded bg-popover text-popover-foreground text-xs font-medium opacity-0 group-hover:opacity-100 pointer-events-none whitespace-nowrap transition-opacity z-50 shadow-lg border border-border">
            {item.label}
          </span>
        </button>
      ))}
    </div>
  );
}
