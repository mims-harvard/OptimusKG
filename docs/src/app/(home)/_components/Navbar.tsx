"use client";

import { useState } from "react";
import { gitConfig } from "@/lib/shared";
import { Logo } from "@/components/Logo";
import { DownloadButton } from "./DownloadButton";

function GitHubIcon({ size = 22 }: { size?: number }) {
  return (
    <svg
      width={size}
      height={size}
      viewBox="0 0 24 24"
      fill="currentColor"
      aria-hidden="true"
    >
      <path
        fillRule="evenodd"
        clipRule="evenodd"
        d="M12 2C6.477 2 2 6.484 2 12.017c0 4.425 2.865 8.18 6.839 9.504.5.092.682-.217.682-.483 0-.237-.008-.868-.013-1.703-2.782.605-3.369-1.343-3.369-1.343-.454-1.158-1.11-1.466-1.11-1.466-.908-.62.069-.608.069-.608 1.003.07 1.531 1.032 1.531 1.032.892 1.53 2.341 1.088 2.91.832.092-.647.35-1.088.636-1.338-2.22-.253-4.555-1.113-4.555-4.951 0-1.093.39-1.988 1.029-2.688-.103-.253-.446-1.272.098-2.65 0 0 .84-.27 2.75 1.026A9.564 9.564 0 0112 6.844c.85.004 1.705.115 2.504.337 1.909-1.296 2.747-1.027 2.747-1.027.546 1.379.202 2.398.1 2.651.64.7 1.028 1.595 1.028 2.688 0 3.848-2.339 4.695-4.566 4.943.359.309.678.92.678 1.855 0 1.338-.012 2.419-.012 2.747 0 .268.18.58.688.482A10.02 10.02 0 0022 12.017C22 6.484 17.522 2 12 2z"
      />
    </svg>
  );
}

const GET_STARTED_HREF = "/docs";
const GITHUB_HREF = `https://github.com/${gitConfig.user}/${gitConfig.repo}`;

const outlineButton =
  "inline-flex items-center justify-center whitespace-nowrap rounded-full border border-[var(--l-border)] text-[var(--l-ink)]";
const filledButton =
  "inline-flex items-center justify-center whitespace-nowrap rounded-full border border-[var(--l-ink)] bg-[var(--l-ink)] text-[var(--l-bg)]";

function OptimusKGLogo() {
  return (
    <a href="/" className="text-[var(--l-ink)]">
      <Logo />
    </a>
  );
}

export function Navbar() {
  const [open, setOpen] = useState(false);

  return (
    <header className="sticky top-0 z-50 bg-[var(--l-bg)]">
      <div className="l-container">
        <div className="flex h-[var(--l-header-h)] items-center justify-between">
          <OptimusKGLogo />

          <div className="hidden items-center gap-[0.469rem] md:flex">
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
            <a
              href={GITHUB_HREF}
              target="_blank"
              rel="noopener noreferrer"
              aria-label="GitHub repository"
              className="flex h-8 w-8 items-center justify-center rounded-full text-[var(--l-ink)] transition-opacity hover:opacity-70"
            >
              <GitHubIcon />
            </a>
          </div>

          <button
            type="button"
            aria-label={open ? "Close menu" : "Open menu"}
            onClick={() => setOpen((v) => !v)}
            className="flex h-6 w-6 flex-col justify-center gap-[0.3rem] text-[var(--l-ink)] md:hidden"
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
        <div className="border-t border-[var(--l-border)] bg-[var(--l-bg)] px-[var(--l-g2)] pb-5 md:hidden">
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
            <a
              href={GITHUB_HREF}
              target="_blank"
              rel="noopener noreferrer"
              className={`${outlineButton} gap-2 text-[0.9375rem]`}
              style={{ paddingBlock: "0.625rem", paddingInline: "1.25rem" }}
            >
              <GitHubIcon />
              GitHub
            </a>
          </div>
        </div>
      )}
    </header>
  );
}
