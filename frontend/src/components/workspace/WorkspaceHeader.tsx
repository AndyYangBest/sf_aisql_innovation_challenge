import { useState } from "react";
import { useNavigate } from "react-router-dom";
import { 
  ArrowLeft, 
  Save, 
  Share2, 
  MoreHorizontal, 
  Coins, 
  Tag, 
  User, 
  FileText, 
  GitBranch,
  Users,
  Circle
} from "lucide-react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Avatar, AvatarFallback, AvatarImage } from "@/components/ui/avatar";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import { useToast } from "@/hooks/use-toast";
import { useBreakpoint } from "@/hooks/useBreakpoint";

// 协作者类型
export interface Collaborator {
  id: string;
  name: string;
  email: string;
  avatar?: string;
  status: "online" | "idle" | "offline";
  color: string; // 用于光标颜色
}

// Token 使用信息
export interface TokenUsage {
  context: number;
  output: number;
  total: number;
}

export interface CreditUsageDay {
  day: string;
  credits_used: number;
}

export interface CreditUsage {
  days: number;
  total_credits: number;
  by_day: CreditUsageDay[];
}

interface WorkspaceHeaderProps {
  // 基础信息
  title: string;
  subtitle?: string;
  onTitleChange?: (newTitle: string) => void;
  editable?: boolean;
  
  // 模式控制
  mode: "workflow" | "report";
  onModeChange?: (mode: "workflow" | "report") => void;
  
  // 协作者
  collaborators?: Collaborator[];
  currentUserId?: string;
  onInvite?: () => void;
  
  // Token 使用
  tokenUsage?: TokenUsage;

  // Snowflake credits usage
  creditUsage?: CreditUsage;
  
  // 操作回调
  onBack?: () => void;
  onSave?: () => void;
  onShare?: () => void;
  onDelete?: () => void;
  
  // 自定义操作
  extraActions?: React.ReactNode;
}

// 状态颜色映射
const statusColors: Record<Collaborator["status"], string> = {
  online: "bg-success",
  idle: "bg-warning",
  offline: "bg-muted-foreground",
};

export function WorkspaceHeader({
  title,
  subtitle,
  onTitleChange,
  editable = true,
  mode,
  onModeChange,
  collaborators = [],
  currentUserId,
  onInvite,
  tokenUsage,
  creditUsage,
  onBack,
  onSave,
  onShare,
  onDelete,
  extraActions,
}: WorkspaceHeaderProps) {
  const navigate = useNavigate();
  const { toast } = useToast();
  const { isMobile } = useBreakpoint();
  const [isEditing, setIsEditing] = useState(false);
  const [editValue, setEditValue] = useState(title);

  const handleBack = () => {
    if (onBack) {
      onBack();
    } else {
      navigate("/");
    }
  };

  const handleSave = () => {
    if (editValue.trim() && onTitleChange) {
      onTitleChange(editValue.trim());
      setIsEditing(false);
      toast({ title: "Renamed successfully" });
    }
  };

  const handleShare = () => {
    if (onShare) {
      onShare();
    } else {
      navigator.clipboard.writeText(window.location.href);
      toast({ title: "Link copied" });
    }
  };

  const handleModeToggle = () => {
    if (onModeChange) {
      onModeChange(mode === "workflow" ? "report" : "workflow");
    }
  };

  // 在线协作者（移动端最多显示2个，桌面端3个）
  const onlineCollaborators = collaborators.filter(c => c.status !== "offline");
  const maxDisplay = isMobile ? 2 : 3;
  const displayCollaborators = onlineCollaborators.slice(0, maxDisplay);
  const moreCount = onlineCollaborators.length - maxDisplay;

  return (
    <header className="h-12 sm:h-14 border-b border-border bg-card px-2 sm:px-4 flex items-center justify-between flex-shrink-0 gap-2">
      {/* 左侧：返回 + 标题 */}
      <div className="flex items-center gap-1.5 sm:gap-3 min-w-0 flex-1">
        <Button 
          variant="ghost" 
          size="icon" 
          className="h-8 w-8 flex-shrink-0" 
          onClick={handleBack}
        >
          <ArrowLeft className="h-4 w-4" />
        </Button>
        
        <div className="h-4 sm:h-5 w-px bg-border hidden sm:block" />
        
        <div className="min-w-0 flex-1">
          {isEditing && editable ? (
            <div className="flex items-center gap-2">
              <Input
                value={editValue}
                onChange={(e) => setEditValue(e.target.value)}
                className="h-7 text-sm font-medium w-full max-w-[200px]"
                autoFocus
                onKeyDown={(e) => {
                  if (e.key === "Enter") handleSave();
                  if (e.key === "Escape") setIsEditing(false);
                }}
              />
              <Button size="sm" variant="secondary" className="h-7 text-xs flex-shrink-0" onClick={handleSave}>
                Save
              </Button>
            </div>
          ) : (
            <div
              onClick={() => {
                if (editable) {
                  setEditValue(title);
                  setIsEditing(true);
                }
              }}
              className={editable ? "cursor-pointer" : ""}
            >
              <h1 className="text-xs sm:text-sm font-medium hover:text-primary transition-colors truncate">
                {title}
              </h1>
              {subtitle && (
                <p className="text-[9px] sm:text-[10px] text-muted-foreground truncate hidden sm:block">
                  {subtitle}
                </p>
              )}
            </div>
          )}
        </div>
      </div>

      {/* 右侧：协作者 + Token + 操作 */}
      <div className="flex items-center gap-1.5 sm:gap-3 flex-shrink-0">
        {/* 协作者头像组 - 移动端隐藏或精简 */}
        {collaborators.length > 0 && (
          <div className="hidden xs:flex items-center">
            <div className="flex -space-x-2">
              {displayCollaborators.map((collaborator) => (
                <Tooltip key={collaborator.id}>
                  <TooltipTrigger asChild>
                    <div className="relative">
                      <Avatar className="h-6 w-6 sm:h-7 sm:w-7 border-2 border-card">
                        <AvatarImage src={collaborator.avatar} alt={collaborator.name} />
                        <AvatarFallback 
                          className="text-[9px] sm:text-[10px]"
                          style={{ backgroundColor: collaborator.color }}
                        >
                          {collaborator.name.slice(0, 2).toUpperCase()}
                        </AvatarFallback>
                      </Avatar>
                      {/* 在线状态指示器 */}
                      <Circle 
                        className={`absolute -bottom-0.5 -right-0.5 h-2 w-2 sm:h-2.5 sm:w-2.5 ${statusColors[collaborator.status]} rounded-full border border-card fill-current`}
                      />
                    </div>
                  </TooltipTrigger>
                  <TooltipContent side="bottom" className="text-xs">
                    <div>
                      <div className="font-medium">{collaborator.name}</div>
                      <div className="text-muted-foreground capitalize">{collaborator.status}</div>
                    </div>
                  </TooltipContent>
                </Tooltip>
              ))}
              
              {moreCount > 0 && (
                <Tooltip>
                  <TooltipTrigger asChild>
                    <Avatar className="h-6 w-6 sm:h-7 sm:w-7 border-2 border-card">
                      <AvatarFallback className="text-[9px] sm:text-[10px] bg-muted">
                        +{moreCount}
                      </AvatarFallback>
                    </Avatar>
                  </TooltipTrigger>
                  <TooltipContent side="bottom" className="text-xs">
                    {moreCount} more collaborator{moreCount > 1 ? "s" : ""} online
                  </TooltipContent>
                </Tooltip>
              )}
            </div>
            
            {/* 邀请按钮 - 移动端隐藏 */}
            {onInvite && !isMobile && (
              <Button 
                variant="ghost" 
                size="icon" 
                className="h-7 w-7 ml-1"
                onClick={onInvite}
              >
                <Users className="h-3.5 w-3.5" />
              </Button>
            )}
          </div>
        )}

        {/* Snowflake credits（优先显示真实 credits，其次才显示 token） */}
        {creditUsage ? (
          <Tooltip>
            <TooltipTrigger asChild>
              <div className="flex items-center gap-1 sm:gap-1.5 text-[10px] sm:text-xs text-muted-foreground cursor-default hover:text-foreground transition-colors">
                <Coins className="h-3 w-3 sm:h-3.5 sm:w-3.5" />
                <span className="tabular-nums">
                  {isMobile
                    ? (creditUsage.total_credits >= 1000
                        ? `${(creditUsage.total_credits / 1000).toFixed(1)}k`
                        : creditUsage.total_credits.toFixed(2))
                    : creditUsage.total_credits.toLocaleString(undefined, { maximumFractionDigits: 2 })}
                </span>
              </div>
            </TooltipTrigger>
            <TooltipContent side="bottom" className="text-xs">
              <div className="space-y-1">
                <div className="flex justify-between gap-4 font-medium">
                  <span>Credits (last {creditUsage.days}d):</span>
                  <span>{creditUsage.total_credits.toLocaleString(undefined, { maximumFractionDigits: 6 })}</span>
                </div>
                {creditUsage.by_day?.length > 0 && (
                  <div className="border-t border-border pt-1 space-y-1">
                    {creditUsage.by_day.slice(0, 7).map((d) => (
                      <div key={d.day} className="flex justify-between gap-4">
                        <span className="text-muted-foreground">{d.day}</span>
                        <span>{d.credits_used.toLocaleString(undefined, { maximumFractionDigits: 6 })}</span>
                      </div>
                    ))}
                  </div>
                )}
              </div>
            </TooltipContent>
          </Tooltip>
        ) : tokenUsage && (
          <Tooltip>
            <TooltipTrigger asChild>
              <div className="flex items-center gap-1 sm:gap-1.5 text-[10px] sm:text-xs text-muted-foreground cursor-default hover:text-foreground transition-colors">
                <Coins className="h-3 w-3 sm:h-3.5 sm:w-3.5" />
                <span className="tabular-nums">
                  {isMobile 
                    ? (tokenUsage.total >= 1000 
                        ? `${(tokenUsage.total / 1000).toFixed(1)}k` 
                        : tokenUsage.total)
                    : tokenUsage.total.toLocaleString()
                  }
                </span>
              </div>
            </TooltipTrigger>
            <TooltipContent side="bottom" className="text-xs">
              <div className="space-y-1">
                <div className="flex justify-between gap-4">
                  <span className="text-muted-foreground">Context:</span>
                  <span>{tokenUsage.context.toLocaleString()}</span>
                </div>
                <div className="flex justify-between gap-4">
                  <span className="text-muted-foreground">Output:</span>
                  <span>{tokenUsage.output.toLocaleString()}</span>
                </div>
                <div className="border-t border-border pt-1 flex justify-between gap-4 font-medium">
                  <span>Total:</span>
                  <span>{tokenUsage.total.toLocaleString()}</span>
                </div>
              </div>
            </TooltipContent>
          </Tooltip>
        )}

        {/* 模式切换按钮 */}
        {onModeChange && (
          <Button 
            variant="outline" 
            size="sm" 
            className="h-7 sm:h-8 text-[10px] sm:text-xs px-2 sm:px-3" 
            onClick={handleModeToggle}
          >
            {mode === "workflow" ? (
              <>
                <FileText className="h-3 w-3 sm:h-3.5 sm:w-3.5 mr-1 sm:mr-1.5" />
                <span className="text-[9px] sm:text-xs">Report</span>
              </>
            ) : (
              <>
                <GitBranch className="h-3 w-3 sm:h-3.5 sm:w-3.5 mr-1 sm:mr-1.5" />
                <span className="text-[9px] sm:text-xs">Workflow</span>
              </>
            )}
          </Button>
        )}

        {/* 自定义操作 - 移动端可能需要隐藏 */}
        <div className="hidden sm:flex items-center">
          {extraActions}
        </div>

        {/* 更多操作菜单 */}
        <DropdownMenu>
          <DropdownMenuTrigger asChild>
            <Button variant="ghost" size="icon" className="h-7 w-7 sm:h-8 sm:w-8">
              <MoreHorizontal className="h-4 w-4" />
            </Button>
          </DropdownMenuTrigger>
          <DropdownMenuContent align="end">
            {/* 移动端显示额外操作 */}
            {isMobile && onInvite && (
              <DropdownMenuItem onClick={onInvite}>
                <Users className="h-3.5 w-3.5 mr-2" />
                Invite
              </DropdownMenuItem>
            )}
            <DropdownMenuItem>
              <Tag className="h-3.5 w-3.5 mr-2" />
              Tags
            </DropdownMenuItem>
            <DropdownMenuItem>
              <User className="h-3.5 w-3.5 mr-2" />
              Author
            </DropdownMenuItem>
            <DropdownMenuItem onClick={handleShare}>
              <Share2 className="h-3.5 w-3.5 mr-2" />
              Share Link
            </DropdownMenuItem>
            {onSave && (
              <DropdownMenuItem onClick={onSave}>
                <Save className="h-3.5 w-3.5 mr-2" />
                Save
              </DropdownMenuItem>
            )}
            {onDelete && (
              <DropdownMenuItem className="text-destructive" onClick={onDelete}>
                Delete
              </DropdownMenuItem>
            )}
          </DropdownMenuContent>
        </DropdownMenu>
      </div>
    </header>
  );
}

export default WorkspaceHeader;
