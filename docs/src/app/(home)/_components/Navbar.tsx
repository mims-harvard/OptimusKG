"use client";

import { useState } from "react";
import { DownloadButton } from "./DownloadButton";

const GET_STARTED_HREF = "/docs/getting-started";

const outlineButton =
  "inline-flex items-center justify-center whitespace-nowrap rounded-full border border-[var(--l-border)] text-[var(--l-ink)]";
const filledButton =
  "inline-flex items-center justify-center whitespace-nowrap rounded-full border border-[var(--l-ink)] bg-[var(--l-ink)] text-[var(--l-bg)]";

function OptimusKGLogo() {
  return (
    <a href="/" className="flex items-center gap-1 text-[var(--l-ink)]">
      <svg
        width="22"
        height="22"
        viewBox="0 0 256 256"
        fill="none"
        xmlns="http://www.w3.org/2000/svg"
        aria-hidden="true"
      >
        <circle cx="128" cy="128" r="24" stroke="currentColor" strokeLinecap="round" strokeLinejoin="round" strokeWidth="16" />
        <circle cx="96" cy="56" r="24" stroke="currentColor" strokeLinecap="round" strokeLinejoin="round" strokeWidth="16" />
        <circle cx="200" cy="104" r="24" stroke="currentColor" strokeLinecap="round" strokeLinejoin="round" strokeWidth="16" />
        <circle cx="200" cy="184" r="24" stroke="currentColor" strokeLinecap="round" strokeLinejoin="round" strokeWidth="16" />
        <circle cx="56" cy="192" r="24" stroke="currentColor" strokeLinecap="round" strokeLinejoin="round" strokeWidth="16" />
        <line x1="118.25" y1="106.07" x2="105.75" y2="77.93" stroke="currentColor" strokeLinecap="round" strokeLinejoin="round" strokeWidth="16" />
        <line x1="177.23" y1="111.59" x2="150.77" y2="120.41" stroke="currentColor" strokeLinecap="round" strokeLinejoin="round" strokeWidth="16" />
        <line x1="181.06" y1="169.27" x2="146.94" y2="142.73" stroke="currentColor" strokeLinecap="round" strokeLinejoin="round" strokeWidth="16" />
        <line x1="110.06" y1="143.94" x2="73.94" y2="176.06" stroke="currentColor" strokeLinecap="round" strokeLinejoin="round" strokeWidth="16" />
      </svg>
      <span
        className="font-normal whitespace-nowrap"
        style={{ fontSize: "1rem", lineHeight: "1", letterSpacing: "-0.015em" }}
      >
        OptimusKG
      </span>
    </a>
  );
}

export function Navbar() {
  const [open, setOpen] = useState(false);

  return (
    <header className="sticky top-0 z-50 bg-[var(--l-bg)]">
      <div className="mx-auto max-w-[81.25rem] px-6">
        <div className="flex h-[3.25rem] items-center justify-between">
          <OptimusKGLogo />

          <div className="hidden items-center gap-[0.469rem] lg:flex">
            <a
              href={GET_STARTED_HREF}
              className={outlineButton}
              style={{
                paddingTop: "0.4125rem",
                paddingBottom: "0.43rem",
                paddingInline: "0.719rem",
                fontSize: "0.844rem",
                lineHeight: "0.875rem",
              }}
            >
              Get started
            </a>
            <DownloadButton
              showIcon={false}
              className={filledButton}
              style={{
                paddingTop: "0.4125rem",
                paddingBottom: "0.43rem",
                paddingInline: "0.719rem",
                fontSize: "0.856rem",
                lineHeight: "0.875rem",
              }}
            />
          </div>

          <button
            type="button"
            aria-label={open ? "Close menu" : "Open menu"}
            onClick={() => setOpen((v) => !v)}
            className="flex h-6 w-6 flex-col justify-center gap-[0.3rem] text-[var(--l-ink)] lg:hidden"
          >
            {open ? (
              <>
                <span className="block h-px w-full translate-y-[0.34rem] rotate-45 bg-[var(--l-ink)]" />
                <span className="block h-px w-full -translate-y-[0.22rem] -rotate-45 bg-[var(--l-ink)]" />
              </>
            ) : (
              <>
                <span className="block h-px w-full bg-[var(--l-ink)]" />
                <span className="block h-px w-full bg-[var(--l-ink)]" />
                <span className="block h-px w-full bg-[var(--l-ink)]" />
              </>
            )}
          </button>
        </div>
      </div>

      {open && (
        <div className="border-t border-[var(--l-border)] bg-[var(--l-bg)] px-6 pb-5 lg:hidden">
          <div className="mt-4 flex flex-col gap-3">
            <a
              href={GET_STARTED_HREF}
              className={`${outlineButton} text-[0.9375rem]`}
              style={{ paddingBlock: "0.625rem", paddingInline: "1.25rem" }}
            >
              Get started
            </a>
            <DownloadButton
              showIcon={false}
              className={`${filledButton} text-[0.9375rem]`}
              style={{ paddingBlock: "0.625rem", paddingInline: "1.25rem" }}
            />
          </div>
        </div>
      )}
    </header>
  );
}
