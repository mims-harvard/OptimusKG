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
        "group/tab relative flex h-full cursor-pointer items-center gap-[0.375rem] border-[var(--l-border)] border-r px-[0.75rem]",
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
      <span className="truncate text-[0.69375rem]">{name}</span>
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
}: {
  title?: string;
  tabs: EditorTab[];
  className?: string;
  style?: CSSProperties;
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

  return (
    <EditorWindow className={className} style={style} title={title}>
      <div
        className="flex shrink-0 items-center bg-[var(--l-surface)]"
        role="tablist"
        style={{ height: "1.887rem" }}
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
      <div className="min-h-0 flex-1 overflow-hidden bg-[var(--l-bg)]">
        {active?.content}
      </div>
    </EditorWindow>
  );
}
