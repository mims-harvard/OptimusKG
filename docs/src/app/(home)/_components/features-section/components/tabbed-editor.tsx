"use client";

import {
  type CSSProperties,
  forwardRef,
  type ReactNode,
  useImperativeHandle,
  useState,
} from "react";

import { X } from "lucide-react";

import { cn } from "@/lib/cn";

export type EditorTab = { name: string; content: ReactNode };

export type TabbedEditorHandle = {
  openTab: (tab: EditorTab) => void;
};

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
        "group/tab relative flex h-full shrink-0 cursor-pointer items-center gap-1.5 border-fd-border border-r ps-3 pe-1.5",
        active
          ? "bg-fd-background pb-px text-fd-foreground after:absolute after:inset-x-0 after:bottom-0 after:h-px after:bg-fd-background"
          : "border-b bg-fd-card text-fd-muted-foreground hover:text-fd-foreground"
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
          className={cn(
            "-my-1 inline-flex cursor-pointer items-center justify-center rounded-[4px] p-0.5 text-fd-muted-foreground transition-[opacity,background-color,color] duration-150 hover:bg-[color-mix(in_srgb,var(--color-fd-foreground)_12%,transparent)] hover:text-fd-foreground",
            active ? "opacity-100" : "opacity-0 group-hover/tab:opacity-100"
          )}
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

export const TabbedEditor = forwardRef<
  TabbedEditorHandle,
  {
    tabs: EditorTab[];
    contentBg?: string;
    onLastTabClose?: () => void;
  }
>(function TabbedEditor({ tabs: initialTabs, contentBg, onLastTabClose }, ref) {
  const [tabs, setTabs] = useState(initialTabs);
  const [activeIndex, setActiveIndex] = useState(0);
  const active = tabs[activeIndex];

  useImperativeHandle(
    ref,
    () => ({
      openTab(tab) {
        setTabs((prev) => {
          const existing = prev.findIndex((t) => t.name === tab.name);
          if (existing >= 0) {
            setActiveIndex(existing);
            return prev;
          }
          setActiveIndex(prev.length);
          return [...prev, tab];
        });
      },
    }),
    []
  );

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
        className="l-scrollbar-hide flex h-7.5 shrink-0 items-center overflow-x-auto bg-fd-card"
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
        <div className="h-full flex-1 border-fd-border border-b" />
      </div>
      <div className="min-h-0 flex-1 overflow-hidden bg-[var(--tab-content-bg,var(--color-fd-background))]">
        {active?.content}
      </div>
    </div>
  );
});
