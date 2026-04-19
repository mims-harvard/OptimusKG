"use client";

import { useRef, useState } from "react";

import { cn } from "@/lib/cn";

type TSnippetType = "success" | "warning" | "error";

type SnippetProps = {
  text: string | string[];
  width?: string;
  onCopy?: () => void;
  prompt?: boolean;
  dark?: boolean;
  type?: TSnippetType;
  className?: string;
};

const variant = {
  default: {
    background: "bg-[var(--l-bg)]",
    text: "text-[var(--l-ink)]",
  },
  inverted: {
    background: "bg-[var(--l-ink)]",
    text: "text-[var(--l-bg)]",
  },
  success: {
    background: "bg-blue-100 dark:bg-blue-950",
    text: "text-blue-900 dark:text-blue-200",
  },
  warning: {
    background: "bg-amber-100 dark:bg-amber-950",
    text: "text-amber-900 dark:text-amber-200",
  },
  error: {
    background: "bg-red-100 dark:bg-red-950",
    text: "text-red-900 dark:text-red-200",
  },
} as const;

function getVariant(inverted: boolean, type?: TSnippetType) {
  if (inverted) {
    return variant.inverted;
  }
  switch (type) {
    case "success":
      return variant.success;
    case "warning":
      return variant.warning;
    case "error":
      return variant.error;
    default:
      return variant.default;
  }
}

export function Snippet({
  text,
  width = "100%",
  onCopy,
  prompt = true,
  dark = false,
  type,
  className,
}: SnippetProps) {
  const [animation, setAnimation] = useState(false);
  const animationTimeout = useRef<ReturnType<typeof setTimeout> | null>(null);
  const lines = typeof text === "string" ? [text] : text;
  const colors = getVariant(dark, type);

  const onClick = (event: React.MouseEvent) => {
    event.preventDefault();
    event.stopPropagation();
    if (animationTimeout.current) {
      clearTimeout(animationTimeout.current);
    }
    setAnimation(true);
    animationTimeout.current = setTimeout(() => setAnimation(false), 2000);
    navigator.clipboard.writeText(lines.join("\n"));
    onCopy?.();
  };

  return (
    <div
      className={cn(
        "flex items-center gap-3 rounded-[1px] border border-[var(--l-border)] px-3 py-2.5",
        colors.background,
        className
      )}
      style={{ width }}
    >
      <div className="min-w-0 flex-1">
        {lines.map((line) => (
          <div
            className={cn(
              "truncate font-mono text-[13px]",
              prompt && "before:content-['$_']",
              colors.text
            )}
            key={line}
          >
            {line}
          </div>
        ))}
      </div>
      <button
        aria-label="Copy to clipboard"
        className="relative inline-flex shrink-0 cursor-pointer items-center justify-center rounded-none border border-[var(--l-ink)] bg-[var(--l-ink)] text-[var(--l-bg)] transition-opacity hover:opacity-90"
        onClick={onClick}
        style={{
          width: "1.75rem",
          height: "1.75rem",
        }}
        type="button"
      >
        <svg
          aria-hidden="true"
          className={cn(
            "absolute fill-[var(--l-bg)] transition-opacity duration-150",
            animation ? "opacity-0" : "opacity-100"
          )}
          height="14"
          strokeLinejoin="round"
          viewBox="0 0 16 16"
          width="14"
        >
          <path
            clipRule="evenodd"
            d="M2.75 0.5C1.7835 0.5 1 1.2835 1 2.25V9.75C1 10.7165 1.7835 11.5 2.75 11.5H3.75H4.5V10H3.75H2.75C2.61193 10 2.5 9.88807 2.5 9.75V2.25C2.5 2.11193 2.61193 2 2.75 2H8.25C8.38807 2 8.5 2.11193 8.5 2.25V3H10V2.25C10 1.2835 9.2165 0.5 8.25 0.5H2.75ZM7.75 4.5C6.7835 4.5 6 5.2835 6 6.25V13.75C6 14.7165 6.7835 15.5 7.75 15.5H13.25C14.2165 15.5 15 14.7165 15 13.75V6.25C15 5.2835 14.2165 4.5 13.25 4.5H7.75ZM7.5 6.25C7.5 6.11193 7.61193 6 7.75 6H13.25C13.3881 6 13.5 6.11193 13.5 6.25V13.75C13.5 13.8881 13.3881 14 13.25 14H7.75C7.61193 14 7.5 13.8881 7.5 13.75V6.25Z"
            fillRule="evenodd"
          />
        </svg>
        <svg
          aria-hidden="true"
          className={cn(
            "absolute fill-[var(--l-bg)] transition-opacity duration-150",
            animation ? "opacity-100" : "opacity-0"
          )}
          height="14"
          strokeLinejoin="round"
          viewBox="0 0 16 16"
          width="14"
        >
          <path
            clipRule="evenodd"
            d="M15.5607 3.99999L15.0303 4.53032L6.23744 13.3232C5.55403 14.0066 4.44599 14.0066 3.76257 13.3232L4.2929 12.7929L3.76257 13.3232L0.969676 10.5303L0.439346 9.99999L1.50001 8.93933L2.03034 9.46966L4.82323 12.2626C4.92086 12.3602 5.07915 12.3602 5.17678 12.2626L13.9697 3.46966L14.5 2.93933L15.5607 3.99999Z"
            fillRule="evenodd"
          />
        </svg>
      </button>
    </div>
  );
}
