import { useState, useRef, useEffect, useCallback } from "react";
import { Sparkles } from "lucide-react";
import { Button } from "@/components/ui/button";
import { cn } from "@/lib/utils";

interface AIFloatingButtonProps {
  onClick: () => void;
  className?: string;
}

const AIFloatingButton = ({ onClick, className }: AIFloatingButtonProps) => {
  const [position, setPosition] = useState({ x: 24, y: window.innerHeight / 2 });
  const [isDragging, setIsDragging] = useState(false);
  const [hasMoved, setHasMoved] = useState(false);
  const [isNearEdge, setIsNearEdge] = useState(false);
  const [showHint, setShowHint] = useState(true);
  const buttonRef = useRef<HTMLButtonElement>(null);
  const dragStart = useRef({ x: 0, y: 0 });
  const initialPos = useRef({ x: 0, y: 0 });
  const edgeThreshold = 100; // Distance from right edge to trigger follow

  // Initialize position on mount - right side, vertically centered
  useEffect(() => {
    const updatePosition = () => {
      setPosition({ 
        x: 24, // 24px from right edge
        y: Math.max(100, window.innerHeight / 2 - 24) 
      });
    };
    updatePosition();
    window.addEventListener("resize", updatePosition);
    return () => window.removeEventListener("resize", updatePosition);
  }, []);

  // Hide hint after 3 seconds
  useEffect(() => {
    const timer = setTimeout(() => setShowHint(false), 3000);
    return () => clearTimeout(timer);
  }, []);

  // Track mouse position for edge detection
  const handleGlobalMouseMove = useCallback((e: MouseEvent) => {
    if (isDragging) return;
    
    const distanceFromRight = window.innerWidth - e.clientX;
    const nearEdge = distanceFromRight < edgeThreshold;
    
    setIsNearEdge(nearEdge);
    
    // If near right edge, make button follow mouse Y position smoothly
    if (nearEdge) {
      setPosition(prev => ({
        x: prev.x,
        y: Math.max(60, Math.min(window.innerHeight - 60, e.clientY))
      }));
    }
  }, [isDragging]);

  useEffect(() => {
    window.addEventListener("mousemove", handleGlobalMouseMove);
    return () => window.removeEventListener("mousemove", handleGlobalMouseMove);
  }, [handleGlobalMouseMove]);

  const handleMouseDown = (e: React.MouseEvent) => {
    setIsDragging(true);
    setHasMoved(false);
    setShowHint(false);
    dragStart.current = { x: e.clientX, y: e.clientY };
    initialPos.current = { ...position };
    e.preventDefault();
  };

  const handleMouseMove = useCallback((e: MouseEvent) => {
    if (!isDragging) return;
    
    const deltaX = dragStart.current.x - e.clientX;
    const deltaY = e.clientY - dragStart.current.y;
    
    // Check if actually moved
    if (Math.abs(deltaX) > 5 || Math.abs(deltaY) > 5) {
      setHasMoved(true);
    }
    
    // Keep button within viewport bounds
    const newX = Math.max(24, Math.min(window.innerWidth - 72, initialPos.current.x + deltaX));
    const newY = Math.max(60, Math.min(window.innerHeight - 60, initialPos.current.y + deltaY));
    
    setPosition({ x: newX, y: newY });
  }, [isDragging]);

  const handleMouseUp = useCallback(() => {
    if (isDragging && !hasMoved) {
      onClick();
    }
    setIsDragging(false);
  }, [isDragging, hasMoved, onClick]);

  useEffect(() => {
    if (isDragging) {
      window.addEventListener("mousemove", handleMouseMove);
      window.addEventListener("mouseup", handleMouseUp);
      return () => {
        window.removeEventListener("mousemove", handleMouseMove);
        window.removeEventListener("mouseup", handleMouseUp);
      };
    }
  }, [isDragging, handleMouseMove, handleMouseUp]);

  return (
    <div 
      className="fixed z-50"
      style={{
        right: `${position.x}px`,
        top: `${position.y}px`,
        transform: "translateY(-50%)",
      }}
    >
      {/* Pulse hint ring */}
      <div 
        className={cn(
          "absolute inset-0 rounded-full bg-primary/30 animate-ping",
          "pointer-events-none",
          !showHint && "hidden"
        )}
        style={{ animationDuration: "1.5s" }}
      />
      
      {/* Edge proximity indicator */}
      <div 
        className={cn(
          "absolute -inset-2 rounded-full transition-all duration-300",
          "border-2 border-primary/40 pointer-events-none",
          isNearEdge ? "opacity-100 scale-100" : "opacity-0 scale-90"
        )}
      />
      
      <Button
        ref={buttonRef}
        variant="default"
        size="icon"
        className={cn(
          "relative w-12 h-12 rounded-full shadow-lg",
          "bg-primary hover:bg-primary/90",
          "transition-all duration-200",
          "hover:shadow-xl hover:scale-105",
          "cursor-grab active:cursor-grabbing",
          isDragging && "opacity-90 scale-95 shadow-md",
          isNearEdge && !isDragging && "ring-2 ring-primary/50 ring-offset-2 ring-offset-background",
          className
        )}
        onMouseDown={handleMouseDown}
        onClick={(e) => {
          e.stopPropagation();
          if (!hasMoved) {
            onClick();
          }
        }}
      >
        <Sparkles className={cn(
          "w-5 h-5 text-primary-foreground transition-transform",
          isNearEdge && "animate-pulse"
        )} />
      </Button>
      
      {/* Tooltip hint */}
      <div 
        className={cn(
          "absolute right-full mr-3 top-1/2 -translate-y-1/2",
          "px-3 py-1.5 rounded-lg bg-popover border border-border shadow-md",
          "text-xs text-popover-foreground whitespace-nowrap",
          "transition-all duration-300",
          showHint ? "opacity-100 translate-x-0" : "opacity-0 translate-x-2 pointer-events-none"
        )}
      >
        <div className="flex items-center gap-1.5">
          <Sparkles className="w-3 h-3 text-primary" />
          <span>AI Assistant</span>
        </div>
        {/* Arrow */}
        <div className="absolute right-0 top-1/2 -translate-y-1/2 translate-x-1/2 rotate-45 w-2 h-2 bg-popover border-r border-t border-border" />
      </div>
    </div>
  );
};

export default AIFloatingButton;
