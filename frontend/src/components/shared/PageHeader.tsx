/**
 * PageHeader - 通用的页面头部组件
 */

import { ArrowLeft } from 'lucide-react';
import { useNavigate } from 'react-router-dom';
import { Button } from '@/components/ui/button';
import { cn } from '@/lib/utils';

interface PageHeaderProps {
  title: string;
  subtitle?: string;
  backTo?: string;
  actions?: React.ReactNode;
  className?: string;
}

export function PageHeader({
  title,
  subtitle,
  backTo = '/',
  actions,
  className,
}: PageHeaderProps) {
  const navigate = useNavigate();

  return (
    <header className={cn(
      'h-12 border-b border-border bg-card px-4 flex items-center justify-between flex-shrink-0',
      className
    )}>
      <div className="flex items-center gap-3">
        <Button 
          variant="ghost" 
          size="icon" 
          className="h-8 w-8" 
          onClick={() => navigate(backTo)}
        >
          <ArrowLeft className="h-4 w-4" />
        </Button>
        <div className="h-5 w-px bg-border" />
        <div>
          <h1 className="text-sm font-medium">{title}</h1>
          {subtitle && (
            <p className="text-[10px] text-muted-foreground">{subtitle}</p>
          )}
        </div>
      </div>
      {actions && (
        <div className="flex items-center gap-3">
          {actions}
        </div>
      )}
    </header>
  );
}
