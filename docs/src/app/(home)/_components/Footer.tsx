import { FlaskConical } from "lucide-react";
import { ThemeSwitch } from "fumadocs-ui/layouts/shared/slots/theme-switch";

const smallText = "text-sm/5.25 tracking-[0.00875rem] text-[var(--l-ink-muted)]";

export function Footer() {
  return (
    <footer className="l-gutter">
      <div className="l-container flex flex-col pt-15.75 pb-15.75 md:pb-7 min-[900px]:pt-16.75 min-[900px]:pb-7.5">
        <div className="flex flex-col gap-4 sm:flex-row sm:items-center sm:justify-between">
          <div className="flex flex-col gap-1.5 sm:flex-row sm:items-center sm:gap-4">
            <a
              className={`${smallText} transition-opacity hover:opacity-80`}
              href="https://zitniklab.hms.harvard.edu/"
              rel="noopener noreferrer"
              target="_blank"
            >
              © {new Date().getFullYear()} Zitnik Lab
            </a>
            <span className={`${smallText} flex items-center gap-1.25`}>
              <FlaskConical size={13} strokeWidth={1.75} />
              <span>An open science, academic research project.</span>
            </span>
          </div>
          <ThemeSwitch mode="light-dark-system" className="self-start sm:self-auto" />
        </div>
      </div>
    </footer>
  );
}
