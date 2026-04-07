import { FlaskConical } from "lucide-react";

const NAV_COLUMNS = [
  {
    heading: "Product",
    links: [
      { label: "Agents",        href: "https://cursor.com/en-US/product",    external: false },
      { label: "Enterprise",    href: "https://cursor.com/en-US/enterprise",  external: false },
      { label: "Pricing",       href: "https://cursor.com/en-US/pricing",     external: false },
      { label: "Code Review",   href: "https://cursor.com/en-US/bugbot",      external: false },
      { label: "Tab",           href: "https://cursor.com/en-US/product/tab", external: false },
      { label: "CLI",           href: "https://cursor.com/en-US/cli",         external: false },
      { label: "Cloud Agents",  href: "https://cursor.com/agents",            external: false },
      { label: "Marketplace",   href: "https://cursor.com/marketplace",       external: true  },
    ],
  },
  {
    heading: "Resources",
    links: [
      { label: "Download",   href: "https://cursor.com/en-US/download",   external: false },
      { label: "Changelog",  href: "https://cursor.com/en-US/changelog",  external: false },
      { label: "Docs",       href: "https://cursor.com/docs",             external: true  },
      { label: "Learn",      href: "https://cursor.com/learn",            external: true  },
      { label: "Forum",      href: "https://forum.cursor.com/",           external: true  },
      { label: "Help",       href: "https://cursor.com/help",             external: true  },
      { label: "Workshops",  href: "https://cursor.com/en-US/workshops",  external: false },
      { label: "Status",     href: "https://status.cursor.com/",          external: true  },
    ],
  },
  {
    heading: "Company",
    links: [
      { label: "Careers",    href: "https://cursor.com/en-US/careers",    external: false },
      { label: "Blog",       href: "https://cursor.com/en-US/blog",       external: false },
      { label: "Community",  href: "https://cursor.com/en-US/community",  external: false },
      { label: "Students",   href: "https://cursor.com/en-US/students",   external: false },
      { label: "Brand",      href: "https://cursor.com/en-US/brand",      external: false },
      { label: "Future",     href: "https://cursor.com/en-US/future",     external: false },
      { label: "Anysphere",  href: "https://anysphere.inc/",              external: true  },
    ],
  },
  {
    heading: "Legal",
    links: [
      { label: "Terms of Service",  href: "https://cursor.com/en-US/terms-of-service",  external: false },
      { label: "Privacy Policy",    href: "https://cursor.com/en-US/privacy",            external: false },
      { label: "Data Use",          href: "https://cursor.com/en-US/data-use",           external: false },
      { label: "Security",          href: "https://cursor.com/en-US/security",           external: false },
    ],
  },
  {
    heading: "Connect",
    links: [
      { label: "X",         href: "https://x.com/cursor_ai",                          external: true },
      { label: "LinkedIn",  href: "https://linkedin.com/company/cursorai",            external: true },
      { label: "YouTube",   href: "https://youtube.com/@cursor_ai",                   external: true },
    ],
  },
];

function NavColumn({ heading, links }: (typeof NAV_COLUMNS)[number]) {
  return (
    <div className="flex flex-col">
      {/* heading */}
      <div style={{ paddingBottom: "0.292rem" }}>
        <p
          className="text-[rgba(38,37,30,0.6)]"
          style={{
            fontSize: "0.85625rem",
            lineHeight: "1.3125rem",
            letterSpacing: "0.00875rem",
          }}
        >
          {heading}
        </p>
      </div>
      {/* links */}
      {links.map(({ label, href, external }) => (
        <div key={label} style={{ paddingTop: "0.26rem", paddingBottom: "0.322rem" }}>
          <a
            href={href}
            target={external ? "_blank" : undefined}
            rel={external ? "noopener noreferrer" : undefined}
            className="text-[#26251e] hover:opacity-70 transition-opacity"
            style={{
              fontSize: "0.85625rem",
              lineHeight: "1.3125rem",
              letterSpacing: "0.00875rem",
            }}
          >
            {label}
            {external && (
              <span className="ml-[0.2em] text-[0.7em] align-super opacity-60">↗</span>
            )}
          </a>
        </div>
      ))}
    </div>
  );
}

export function Footer() {
  return (
    <footer className="bg-[#f2f1ed]">
      {/* ── Desktop (≥lg): full layout, max-w-[81.25rem] centered ── */}
      <div className="hidden lg:block px-[1.25rem]">
        <div className="max-w-[81.25rem] mx-auto">
          <div
            className="flex flex-col"
            style={{ paddingTop: "4.2rem", paddingBottom: "1.875rem" }}
          >
            {/* 5-col nav grid */}
            <div
              className="grid w-full"
              style={{
                gridTemplateColumns: "repeat(5, minmax(0, 1fr))",
                gap: "0 0.625rem",
                paddingBottom: "6.3rem",
              }}
            >
              {NAV_COLUMNS.map((col) => (
                <NavColumn key={col.heading} {...col} />
              ))}
            </div>

            {/* bottom bar */}
            <BottomBar />
          </div>
        </div>
      </div>

      {/* ── Medium tablet (≥md, <lg): same layout, smaller px ── */}
      <div className="hidden md:block lg:hidden px-[1.172rem]">
        <div
          className="flex flex-col"
          style={{ paddingTop: "3.9375rem", paddingBottom: "1.758rem" }}
        >
          {/* 5-col nav grid */}
          <div
            className="grid w-full"
            style={{
              gridTemplateColumns: "repeat(5, minmax(0, 1fr))",
              gap: "0 0.625rem",
              paddingBottom: "5.906rem",
            }}
          >
            {NAV_COLUMNS.map((col) => (
              <NavColumn key={col.heading} {...col} />
            ))}
          </div>

          {/* bottom bar */}
          <BottomBar />
        </div>
      </div>

      {/* ── Mobile (<md): 2-col grid, stacked bottom bar ── */}
      <div className="block md:hidden px-[1.172rem]">
        <div
          className="flex flex-col"
          style={{ paddingTop: "3.9375rem", paddingBottom: "3.9375rem" }}
        >
          {/* 2-col nav grid */}
          <div
            className="grid grid-cols-2 w-full"
            style={{ gap: "0 0.625rem", paddingBottom: "5.906rem" }}
          >
            {NAV_COLUMNS.map((col) => (
              <NavColumn key={col.heading} {...col} />
            ))}
          </div>

          {/* bottom bar — stacked */}
          <BottomBar stacked />
        </div>
      </div>
    </footer>
  );
}

function BottomBar({ stacked = false }: { stacked?: boolean }) {
  return (
    <div
      className={
        stacked
          ? "flex flex-col gap-[0.75rem]"
          : "flex flex-row items-center justify-between"
      }
    >
      {/* left: copyright + open science badge */}
      <div className="flex flex-col gap-[0.35rem] sm:flex-row sm:items-center sm:gap-[1rem]">
        <a
          href="https://zitniklab.hms.harvard.edu/"
          target="_blank"
          rel="noopener noreferrer"
          className="text-[rgba(38,37,30,0.6)] hover:opacity-80 transition-opacity"
          style={{ fontSize: "0.825rem", lineHeight: "1.3125rem", letterSpacing: "0.00875rem" }}
        >
          © {new Date().getFullYear()} Zitnik Lab
        </a>
        <span
          className="flex items-center gap-[0.3rem] text-[rgba(38,37,30,0.6)]"
          style={{ fontSize: "0.825rem", lineHeight: "1.3125rem", letterSpacing: "0.00875rem" }}
        >
          <FlaskConical size={13} strokeWidth={1.75} />
          <span>An open science, academic research project.</span>
        </span>
      </div>

    </div>
  );
}
