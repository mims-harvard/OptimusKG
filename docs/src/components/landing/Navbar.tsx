"use client";

import { useState } from "react";

const navLinks = [
  { label: "Product", href: "https://cursor.com/product" },
  { label: "Enterprise", href: "https://cursor.com/enterprise" },
  { label: "Pricing", href: "https://cursor.com/pricing" },
  { label: "Resources", href: "https://cursor.com/changelog" },
];


function OptimusKGLogo() {
  return (
    <a href="/" className="flex items-center gap-[0.5rem]">
      {/* Knowledge-graph icon: hub + 3 satellites */}
      <svg
        width="22"
        height="22"
        viewBox="0 0 18 18"
        fill="none"
        xmlns="http://www.w3.org/2000/svg"
        aria-hidden="true"
      >
        {/* Edges */}
        <line x1="9" y1="9" x2="15" y2="3.5" stroke="#26251e" strokeWidth="1.7" strokeLinecap="round" />
        <line x1="9" y1="9" x2="15" y2="14.5" stroke="#26251e" strokeWidth="1.7" strokeLinecap="round" />
        <line x1="9" y1="9" x2="2.5"  y2="9"   stroke="#26251e" strokeWidth="1.7" strokeLinecap="round" />
        <line x1="15" y1="3.5" x2="15" y2="14.5" stroke="#26251e" strokeWidth="1.4" strokeLinecap="round" strokeOpacity="0.35" />
        {/* Nodes */}
        <circle cx="9"    cy="9"    r="2.75" fill="#26251e" />
        <circle cx="15"   cy="3.5"  r="1.75" fill="#26251e" />
        <circle cx="15"   cy="14.5" r="1.75" fill="#26251e" />
        <circle cx="2.5"  cy="9"    r="1.75" fill="#26251e" />
      </svg>
      {/* Wordmark */}
      <span
        className="text-[#26251e] font-normal whitespace-nowrap"
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
    <header className="sticky top-0 z-50 bg-[#f7f7f4]">
      <div className="max-w-[81.25rem] mx-auto px-6">

        {/* ── Desktop (lg+) ── */}
        <div
          className="hidden lg:grid items-center w-full"
          style={{
            gridTemplateColumns: "auto minmax(0,1fr) 16.664rem",
            height: "3.25rem",
          }}
        >
          {/* Col 1 — logo */}
          <div
            className="col-start-1 self-center"
            style={{ paddingTop: "0.199rem", paddingBottom: "0.238rem" }}
          >
            <OptimusKGLogo />
          </div>

          {/* Col 2 — nav links */}
          <div className="col-start-2 flex items-center justify-center">
            {navLinks.map((link) => (
              <a
                key={link.label}
                href={link.href}
                target="_blank"
                rel="noopener noreferrer"
                className="border border-transparent text-[#26251e] whitespace-nowrap"
                style={{
                  paddingInline: "1rem",
                  paddingBlock: "0.4125rem",
                  fontSize: "0.85rem",
                  lineHeight: "1.3125rem",
                }}
              >
                {link.label}
              </a>
            ))}
          </div>

          {/* Col 3 — actions */}
          <div
            className="col-start-3 flex items-center justify-end"
            style={{ gap: "0.469rem" }}
          >

            {/* Get started */}
            <a
              href="https://cursor.com/contact-sales"
              target="_blank"
              rel="noopener noreferrer"
              className="border border-[rgba(38,37,30,0.2)] rounded-full text-[#26251e] whitespace-nowrap"
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

            {/* Download */}
            <a
              href="https://cursor.com/download"
              target="_blank"
              rel="noopener noreferrer"
              className="bg-[#26251e] border border-[#26251e] rounded-full text-[#f7f7f4] whitespace-nowrap"
              style={{
                paddingTop: "0.4125rem",
                paddingBottom: "0.43rem",
                paddingInline: "0.719rem",
                fontSize: "0.856rem",
                lineHeight: "0.875rem",
              }}
            >
              Download
            </a>
          </div>
        </div>

        {/* ── Mobile (< lg) ── */}
        <div className="flex lg:hidden items-center justify-between h-[3.25rem]">
          <OptimusKGLogo />
          <button
            type="button"
            aria-label={open ? "Close menu" : "Open menu"}
            onClick={() => setOpen((v) => !v)}
            className="flex flex-col justify-center gap-[0.3rem] w-6 h-6 text-[#26251e]"
          >
            {open ? (
              <>
                <span className="block h-px bg-[#26251e] w-full rotate-45 translate-y-[0.34rem]" />
                <span className="block h-px bg-[#26251e] w-full -rotate-45 -translate-y-[0.22rem]" />
              </>
            ) : (
              <>
                <span className="block h-px bg-[#26251e] w-full" />
                <span className="block h-px bg-[#26251e] w-full" />
                <span className="block h-px bg-[#26251e] w-full" />
              </>
            )}
          </button>
        </div>
      </div>

      {/* ── Mobile menu panel ── */}
      {open && (
        <div className="lg:hidden bg-[#f7f7f4] border-t border-[rgba(38,37,30,0.1)] px-6 pb-5">
          <nav className="flex flex-col">
            {navLinks.map((link) => (
              <a
                key={link.label}
                href={link.href}
                target="_blank"
                rel="noopener noreferrer"
                className="py-3 text-[0.9375rem] leading-[1.3125rem] text-[#26251e] border-b border-[rgba(38,37,30,0.06)]"
              >
                {link.label}
              </a>
            ))}
          </nav>
          <div className="mt-4 flex flex-col gap-3">
            <a
              href="https://cursor.com/dashboard"
              target="_blank"
              rel="noopener noreferrer"
              className="text-[0.9375rem] leading-[1.3125rem] text-[#26251e]"
            >
              Sign in
            </a>
            <a
              href="https://cursor.com/contact-sales"
              target="_blank"
              rel="noopener noreferrer"
              className="inline-flex items-center justify-center border border-[rgba(38,37,30,0.2)] rounded-full text-[0.9375rem] text-[#26251e]"
              style={{ paddingBlock: "0.625rem", paddingInline: "1.25rem" }}
            >
              Contact sales
            </a>
            <a
              href="https://cursor.com/download"
              target="_blank"
              rel="noopener noreferrer"
              className="inline-flex items-center justify-center bg-[#26251e] border border-[#26251e] rounded-full text-[0.9375rem] text-[#f7f7f4]"
              style={{ paddingBlock: "0.625rem", paddingInline: "1.25rem" }}
            >
              Download
            </a>
          </div>
        </div>
      )}
    </header>
  );
}
