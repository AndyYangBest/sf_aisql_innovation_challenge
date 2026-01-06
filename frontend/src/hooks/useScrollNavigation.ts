/**
 * useScrollNavigation - 滚动导航 hook
 * 支持点击跳转和滚动时自动高亮
 */

import { useRef, useEffect, useState, useCallback } from 'react';

export interface Section {
  id: string;
  label: string;
}

export function useScrollNavigation(sections: Section[]) {
  const containerRef = useRef<HTMLDivElement>(null);
  const sectionRefs = useRef<Record<string, HTMLDivElement | null>>({});
  const [activeSection, setActiveSection] = useState(sections[0]?.id || '');

  // 滚动到指定 section
  const scrollToSection = useCallback((sectionId: string) => {
    const element = sectionRefs.current[sectionId];
    if (element && containerRef.current) {
      const containerTop = containerRef.current.getBoundingClientRect().top;
      const elementTop = element.getBoundingClientRect().top;
      const offset = elementTop - containerTop + containerRef.current.scrollTop - 20;
      
      containerRef.current.scrollTo({
        top: offset,
        behavior: 'smooth',
      });
    }
  }, []);

  // 注册 section ref
  const registerSection = useCallback((id: string) => {
    return (el: HTMLDivElement | null) => {
      sectionRefs.current[id] = el;
    };
  }, []);

  // 监听滚动，更新当前 section
  useEffect(() => {
    const container = containerRef.current;
    if (!container) return;

    const handleScroll = () => {
      const containerRect = container.getBoundingClientRect();
      let currentSection = sections[0]?.id || '';
      
      for (const section of sections) {
        const element = sectionRefs.current[section.id];
        if (element) {
          const rect = element.getBoundingClientRect();
          const relativeTop = rect.top - containerRect.top;
          
          if (relativeTop <= containerRect.height * 0.3) {
            currentSection = section.id;
          }
        }
      }
      
      setActiveSection(currentSection);
    };

    container.addEventListener('scroll', handleScroll);
    return () => container.removeEventListener('scroll', handleScroll);
  }, [sections]);

  return {
    containerRef,
    activeSection,
    scrollToSection,
    registerSection,
  };
}
