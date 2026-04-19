import type { CSSProperties, ReactNode } from "react";

import { cn } from "@/lib/cn";

function WinChrome({ title }: { title?: string }) {
  return (
    <div
      className="relative flex shrink-0 items-center border-[var(--l-border)] border-b bg-[var(--l-surface)] px-[0.5rem]"
      style={{ height: "1.75rem" }}
    >
      <div className="group/winctl flex gap-[0.375rem]">
        <span className="relative flex size-[0.625rem] cursor-pointer items-center justify-center rounded-full bg-[var(--l-ink-muted)] opacity-40 transition-[background-color,opacity] duration-150 group-hover/winctl:bg-[#ff5f57] group-hover/winctl:opacity-100">
          <svg
            aria-hidden="true"
            className="size-[0.5rem] opacity-0 transition-opacity duration-150 group-hover/winctl:opacity-70"
            fill="none"
            stroke="#4d0000"
            strokeLinecap="round"
            strokeWidth="1.5"
            viewBox="0 0 10 10"
          >
            <path d="M3 3l4 4M7 3l-4 4" />
          </svg>
        </span>
        <span className="relative flex size-[0.625rem] cursor-pointer items-center justify-center rounded-full bg-[var(--l-ink-muted)] opacity-40 transition-[background-color,opacity] duration-150 group-hover/winctl:bg-[#febc2e] group-hover/winctl:opacity-100">
          <svg
            aria-hidden="true"
            className="size-[0.5rem] opacity-0 transition-opacity duration-150 group-hover/winctl:opacity-70"
            fill="none"
            stroke="#5b3300"
            strokeLinecap="round"
            strokeWidth="1.5"
            viewBox="0 0 10 10"
          >
            <path d="M2.5 5h5" />
          </svg>
        </span>
        <span className="relative flex size-[0.625rem] cursor-pointer items-center justify-center rounded-full bg-[var(--l-ink-muted)] opacity-40 transition-[background-color,opacity] duration-150 group-hover/winctl:bg-[#28c840] group-hover/winctl:opacity-100">
          <svg
            aria-hidden="true"
            className="size-[0.5rem] opacity-0 transition-opacity duration-150 group-hover/winctl:opacity-70"
            fill="#0b3d04"
            viewBox="0 0 10 10"
          >
            <path d="M2 2 L6 2 L2 6 Z M8 8 L4 8 L8 4 Z" />
          </svg>
        </span>
      </div>
      {title && (
        <span className="absolute left-1/2 -translate-x-1/2 whitespace-nowrap text-[0.7125rem] text-[var(--l-ink-muted)]">
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
}: {
  title?: string;
  className?: string;
  style?: CSSProperties;
  children: ReactNode;
}) {
  return (
    <div
      className={cn(
        "relative flex flex-col overflow-hidden rounded-[0.625rem] bg-[var(--l-surface)] shadow-[0px_28px_70px_0px_rgba(0,0,0,0.14),0px_14px_32px_0px_rgba(0,0,0,0.1),0px_0px_0px_1px_rgba(38,37,30,0.1)]",
        className,
      )}
      style={style}
    >
      <WinChrome title={title} />
      {children}
    </div>
  );
}
