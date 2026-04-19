"use client";

import { type CSSProperties, type ReactNode, useState } from "react";

import { X } from "lucide-react";

import { cn } from "@/lib/cn";

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
        "group/tab relative flex h-full shrink-0 cursor-pointer items-center gap-1.5 border-[var(--l-border)] border-r px-3",
        active
          ? "bg-[var(--l-bg)] pb-px text-[var(--l-ink)] after:absolute after:inset-x-0 after:bottom-0 after:h-px after:bg-[var(--l-bg)]"
          : "border-b bg-[var(--l-surface)] text-[var(--l-ink-muted)] hover:text-[var(--l-ink)]"
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
          className="-my-1 inline-flex cursor-pointer items-center justify-center rounded-[4px] p-0.5 text-[var(--l-ink-muted)] opacity-0 transition-[opacity,background-color,color] duration-150 hover:bg-[color-mix(in_srgb,var(--l-ink)_12%,transparent)] hover:text-[var(--l-ink)] group-hover/tab:opacity-100"
          onClick={(e) => {
            e.stopPropagation();
            onClose();
          }}
          type="button"
        >
          <X aria-hidden="true" size={12} strokeWidth={1.75} />
        </button>
      )}
    </div>
  );
}

export function TabbedEditor({
  tabs: initialTabs,
  contentBg,
  onLastTabClose,
}: {
  tabs: EditorTab[];
  contentBg?: string;
  onLastTabClose?: () => void;
}) {
  const [tabs, setTabs] = useState(initialTabs);
  const [activeIndex, setActiveIndex] = useState(0);
  const active = tabs[activeIndex];

  function closeTab(index: number) {
    if (tabs.length === 1) {
      onLastTabClose?.();
      return;
    }
    setTabs((prev) => prev.filter((_, i) => i !== index));
    setActiveIndex((current) => {
      if (index < current) {
        return current - 1;
      }
      if (index === current) {
        return Math.min(current, tabs.length - 2);
      }
      return current;
    });
  }

  const rootStyle: CSSProperties | undefined = contentBg
    ? { ["--tab-content-bg" as string]: contentBg }
    : undefined;

  return (
    <div className="flex h-full w-full flex-col" style={rootStyle}>
      <div
        className="l-scrollbar-hide flex h-7.5 shrink-0 items-center overflow-x-auto bg-[var(--l-surface)]"
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
    </div>
  );
}
