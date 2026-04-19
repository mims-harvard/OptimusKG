"use client";

import type { CSSProperties, ReactNode } from "react";

import { cn } from "@/lib/cn";

type ChromeCallbacks = {
  onClose?: () => void;
  onMinimize?: () => void;
  onMaximize?: () => void;
};

function WinChrome({
  title,
  onClose,
  onMinimize,
  onMaximize,
  isMaximized = false,
  canMinimize = true,
  overlay = false,
}: {
  title?: string;
  isMaximized?: boolean;
  canMinimize?: boolean;
  overlay?: boolean;
} & ChromeCallbacks) {
  return (
    <div
      className={cn(
        "relative flex h-7 shrink-0 items-center border-(--l-border) border-b bg-(--l-surface) px-2",
        overlay && "border-t border-r border-l"
      )}
    >
      <div className="group/winctl flex gap-1.5">
        <button
          aria-label="Close window"
          className="relative flex size-2.5 cursor-pointer items-center justify-center rounded-full border-0 bg-(--l-ink-muted) p-0 opacity-40 transition-[background-color,opacity] duration-150 group-hover/winctl:bg-[#ff5f57] group-hover/winctl:opacity-100"
          onClick={(e) => {
            e.stopPropagation();
            onClose?.();
          }}
          type="button"
        >
          <svg
            aria-hidden="true"
            className="size-2 opacity-0 transition-opacity duration-150 group-hover/winctl:opacity-70"
            fill="none"
            stroke="#4d0000"
            strokeLinecap="round"
            strokeWidth="1.5"
            viewBox="0 0 10 10"
          >
            <path d="M3 3l4 4M7 3l-4 4" />
          </svg>
        </button>
        <button
          aria-disabled={!canMinimize}
          aria-label="Minimize window"
          className={cn(
            "relative flex size-2.5 items-center justify-center rounded-full border-0 bg-(--l-ink-muted) p-0 opacity-40 transition-[background-color,opacity] duration-150 group-hover/winctl:opacity-100",
            canMinimize
              ? "cursor-pointer group-hover/winctl:bg-[#febc2e]"
              : "cursor-default"
          )}
          disabled={!canMinimize}
          onClick={(e) => {
            e.stopPropagation();
            if (canMinimize) {
              onMinimize?.();
            }
          }}
          type="button"
        >
          {canMinimize && (
            <svg
              aria-hidden="true"
              className="size-2 opacity-0 transition-opacity duration-150 group-hover/winctl:opacity-70"
              fill="none"
              stroke="#5b3300"
              strokeLinecap="round"
              strokeWidth="1.5"
              viewBox="0 0 10 10"
            >
              <path d="M2.5 5h5" />
            </svg>
          )}
        </button>
        <button
          aria-label="Maximize window"
          className="relative flex size-2.5 cursor-pointer items-center justify-center rounded-full border-0 bg-(--l-ink-muted) p-0 opacity-40 transition-[background-color,opacity] duration-150 group-hover/winctl:bg-[#28c840] group-hover/winctl:opacity-100"
          onClick={(e) => {
            e.stopPropagation();
            onMaximize?.();
          }}
          type="button"
        >
          <svg
            aria-hidden="true"
            className="size-2 opacity-0 transition-opacity duration-150 group-hover/winctl:opacity-70"
            fill="#0b3d04"
            viewBox="0 0 10 10"
          >
            <path
              d={
                isMaximized
                  ? "M5 5 L1 5 L5 1 Z M5 5 L9 5 L5 9 Z"
                  : "M2 2 L6 2 L2 6 Z M8 8 L4 8 L8 4 Z"
              }
            />
          </svg>
        </button>
      </div>
      {title && (
        <span className="absolute left-1/2 -translate-x-1/2 whitespace-nowrap text-(--l-ink-muted) text-xs">
          {title}
        </span>
      )}
    </div>
  );
}

export function EditorWindow({
  title,
  className,
  style,
  children,
  onClose,
  onMinimize,
  onMaximize,
  chromeOverlay = false,
  isMaximized = false,
  canMinimize = true,
}: {
  title?: string;
  className?: string;
  style?: CSSProperties;
  children: ReactNode;
  chromeOverlay?: boolean;
  isMaximized?: boolean;
  canMinimize?: boolean;
} & ChromeCallbacks) {
  return (
    <div
      className={cn(
        "relative flex flex-col overflow-hidden rounded-[0.625rem] bg-(--l-surface) shadow-[0px_28px_70px_0px_rgba(0,0,0,0.14),0px_14px_32px_0px_rgba(0,0,0,0.1),0px_0px_0px_1px_rgba(38,37,30,0.1)]",
        isMaximized &&
          "shadow-[0px_28px_70px_0px_rgba(0,0,0,0.14),0px_14px_32px_0px_rgba(0,0,0,0.1),0px_0px_0px_1px_rgba(38,37,30,0.1),inset_0px_0px_0px_1px_rgba(38,37,30,0.1)]",
        className
      )}
      style={style}
    >
      {chromeOverlay ? (
        <div className="group pointer-events-none absolute inset-x-0 top-0 z-20 h-8">
          <div className="pointer-events-auto absolute inset-x-0 top-0 -translate-y-full opacity-0 transition-[translate,opacity] duration-200 ease-out group-hover:translate-y-0 group-hover:opacity-100">
            <WinChrome
              canMinimize={canMinimize}
              isMaximized={isMaximized}
              onClose={onClose}
              onMaximize={onMaximize}
              onMinimize={onMinimize}
              overlay
              title={title}
            />
          </div>
        </div>
      ) : (
        <WinChrome
          canMinimize={canMinimize}
          isMaximized={isMaximized}
          onClose={onClose}
          onMaximize={onMaximize}
          onMinimize={onMinimize}
          title={title}
        />
      )}
      {children}
    </div>
  );
}
