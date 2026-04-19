"use client";

import { X } from "lucide-react";
import { type CSSProperties, type ReactNode, useState } from "react";

import { cn } from "@/lib/cn";
import { EditorWindow } from "./EditorWindow";

export type EditorTab = { name: string; content: ReactNode };

function Tab({
  name,
  active,
  onSelect,
  onClose,
}: {
  name: string;
  active: boolean;
  onSelect: () => void;
  onClose?: () => void;
}) {
  return (
    <div
      aria-selected={active}
      className={cn(
        "group/tab relative flex h-full cursor-pointer items-center gap-1.5 border-[var(--l-border)] border-r px-3",
        active
          ? "bg-[var(--l-bg)] pb-px text-[var(--l-ink)] after:absolute after:inset-x-0 after:bottom-0 after:h-px after:bg-[var(--l-bg)]"
          : "border-b bg-[var(--l-surface)] text-[var(--l-ink-muted)] hover:text-[var(--l-ink)]",
      )}
      onClick={onSelect}
      onKeyDown={(e) => {
        if (e.key === "Enter" || e.key === " ") {
          e.preventDefault();
          onSelect();
        }
      }}
      role="tab"
      tabIndex={0}
    >
      <span className="truncate text-xs">{name}</span>
      {onClose && (
        <button
          aria-label={`Close ${name}`}
          className="inline-flex cursor-pointer items-center text-[var(--l-ink-muted)] opacity-0 transition-opacity duration-150 hover:text-[var(--l-ink)] group-hover/tab:opacity-100"
          onClick={(e) => {
            e.stopPropagation();
            onClose();
          }}
          type="button"
        >
          <X aria-hidden="true" size={10} strokeWidth={2} />
        </button>
      )}
    </div>
  );
}

export function TabbedEditor({
  title,
  tabs: initialTabs,
  className,
  style,
  contentBg,
  onClose,
  onMinimize,
  onMaximize,
}: {
  title?: string;
  tabs: EditorTab[];
  className?: string;
  style?: CSSProperties;
  contentBg?: string;
  onClose?: () => void;
  onMinimize?: () => void;
  onMaximize?: () => void;
}) {
  const [tabs, setTabs] = useState(initialTabs);
  const [activeIndex, setActiveIndex] = useState(0);
  const active = tabs[activeIndex];

  function closeTab(index: number) {
    setTabs((prev) => prev.filter((_, i) => i !== index));
    setActiveIndex((current) => {
      if (index < current) return current - 1;
      if (index === current) return Math.min(current, tabs.length - 2);
      return current;
    });
  }

  const mergedStyle: CSSProperties | undefined = contentBg
    ? { ...style, ["--tab-content-bg" as string]: contentBg }
    : style;

  return (
    <EditorWindow
      className={className}
      onClose={onClose}
      onMaximize={onMaximize}
      onMinimize={onMinimize}
      style={mergedStyle}
      title={title}
    >
      <div
        className="flex h-7.5 shrink-0 items-center bg-[var(--l-surface)]"
        role="tablist"
      >
        {tabs.map((tab, i) => (
          <Tab
            active={i === activeIndex}
            key={tab.name}
            name={tab.name}
            onClose={() => closeTab(i)}
            onSelect={() => setActiveIndex(i)}
          />
        ))}
        <div className="h-full flex-1 border-[var(--l-border)] border-b" />
      </div>
      <div className="min-h-0 flex-1 overflow-hidden bg-[var(--tab-content-bg,var(--l-bg))]">
        {active?.content}
      </div>
    </EditorWindow>
  );
}
