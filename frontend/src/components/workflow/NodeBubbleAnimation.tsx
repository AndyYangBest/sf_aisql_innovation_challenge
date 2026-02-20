/**
 * NodeBubbleAnimation - Enhanced bubble animation effect for node creation
 * Following Vercel React best practices:
 * - rendering-hoist-jsx: Static JSX hoisted outside component
 * - rerender-memo: Memoized for performance
 * - rendering-animate-svg-wrapper: Animating wrapper div, not SVG
 */

import { memo, useEffect, useState } from 'react';
import { cn } from '@/lib/utils';

interface BubbleAnimationProps {
  /** Trigger animation when this changes */
  trigger?: number;
  /** Duration of animation in ms */
  duration?: number;
  /** Number of bubbles to generate */
  bubbleCount?: number;
}

// Hoist static bubble generation outside component (rendering-hoist-jsx)
const generateBubbleStyles = (count: number) => {
  const bubbles = [];

  // Generate multiple layers of bubbles for depth
  for (let layer = 0; layer < 3; layer++) {
    const layerCount = Math.floor(count / 3);
    const layerDistance = 60 + layer * 40;

    for (let i = 0; i < layerCount; i++) {
      const angle = (Math.PI * 2 * i) / layerCount + (layer * Math.PI / 6);
      const distance = layerDistance + Math.random() * 20;
      const size = 6 + Math.random() * 12 - layer * 2;
      const delay = Math.random() * 150 + layer * 50;
      const duration = 600 + Math.random() * 400 + layer * 100;

      // Random color variations
      const colorVariant = Math.floor(Math.random() * 5);
      const colors = [
        'from-blue-400 via-cyan-400 to-blue-300',
        'from-cyan-400 via-teal-400 to-cyan-300',
        'from-indigo-400 via-blue-400 to-indigo-300',
        'from-purple-400 via-indigo-400 to-purple-300',
        'from-teal-400 via-cyan-400 to-teal-300',
      ];

      bubbles.push({
        '--tx': `${Math.cos(angle) * distance}px`,
        '--ty': `${Math.sin(angle) * distance}px`,
        '--size': `${size}px`,
        '--delay': `${delay}ms`,
        '--duration': `${duration}ms`,
        '--rotation': `${Math.random() * 360}deg`,
        color: colors[colorVariant],
        layer,
      });
    }
  }

  return bubbles;
};

/**
 * Enhanced bubble animation component for node creation
 * Memoized to prevent unnecessary re-renders (rerender-memo)
 */
export const NodeBubbleAnimation = memo<BubbleAnimationProps>(({
  trigger = 0,
  duration = 1000,
  bubbleCount = 24,
}) => {
  const [isAnimating, setIsAnimating] = useState(false);
  const [bubbleStyles] = useState(() => generateBubbleStyles(bubbleCount));

  useEffect(() => {
    if (trigger === 0) return;

    setIsAnimating(true);
    const timer = setTimeout(() => {
      setIsAnimating(false);
    }, duration);

    return () => clearTimeout(timer);
  }, [trigger, duration]);

  if (!isAnimating) return null;

  // Animate wrapper div, not individual elements (rendering-animate-svg-wrapper pattern)
  return (
    <div className="absolute inset-0 pointer-events-none overflow-visible z-50">
      {/* Central burst effect */}
      <div className="absolute inset-0 flex items-center justify-center">
        <div className="absolute w-16 h-16 rounded-full bg-gradient-to-br from-cyan-400/40 to-blue-500/40 animate-burst-scale blur-md" />
        <div className="absolute w-12 h-12 rounded-full bg-gradient-to-br from-white/60 to-cyan-300/60 animate-burst-scale-delayed blur-sm" />
      </div>

      {/* Particle bubbles */}
      <div className="absolute inset-0 flex items-center justify-center">
        {bubbleStyles.map((style, i) => (
          <div
            key={i}
            className={cn(
              "absolute rounded-full bg-gradient-to-br shadow-lg",
              "animate-bubble-burst",
              style.color
            )}
            style={{
              ...style,
              width: style['--size'],
              height: style['--size'],
              animationDelay: style['--delay'],
              animationDuration: style['--duration'],
              filter: `blur(${style.layer === 0 ? '0px' : style.layer === 1 ? '1px' : '2px'})`,
              boxShadow: style.layer === 0
                ? '0 0 8px rgba(56, 189, 248, 0.6), 0 0 16px rgba(56, 189, 248, 0.3)'
                : '0 0 4px rgba(56, 189, 248, 0.4)',
            }}
          />
        ))}
      </div>

      {/* Ring waves */}
      <div className="absolute inset-0 flex items-center justify-center">
        <div className="absolute w-8 h-8 rounded-full border-2 border-cyan-400/60 animate-ring-expand" />
        <div className="absolute w-8 h-8 rounded-full border-2 border-blue-400/60 animate-ring-expand-delayed" />
        <div className="absolute w-8 h-8 rounded-full border border-indigo-400/40 animate-ring-expand-delayed-2" />
      </div>

      {/* Sparkle effects */}
      <div className="absolute inset-0 flex items-center justify-center">
        {[0, 1, 2, 3, 4, 5].map((i) => {
          const angle = (Math.PI * 2 * i) / 6;
          const distance = 35;
          return (
            <div
              key={`sparkle-${i}`}
              className="absolute w-1 h-1 bg-white rounded-full animate-sparkle"
              style={{
                left: `calc(50% + ${Math.cos(angle) * distance}px)`,
                top: `calc(50% + ${Math.sin(angle) * distance}px)`,
                animationDelay: `${i * 80}ms`,
                boxShadow: '0 0 4px rgba(255, 255, 255, 0.8), 0 0 8px rgba(56, 189, 248, 0.6)',
              }}
            />
          );
        })}
      </div>
    </div>
  );
});

NodeBubbleAnimation.displayName = 'NodeBubbleAnimation';

/**
 * Hook to trigger bubble animation
 * Returns a trigger function that increments the animation counter
 */
export const useBubbleAnimation = () => {
  const [trigger, setTrigger] = useState(0);

  const triggerAnimation = () => {
    setTrigger((prev) => prev + 1);
  };

  return { trigger, triggerAnimation };
};
