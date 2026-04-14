import { FlaskConical } from "lucide-react";

const smallText = "text-[0.825rem]/[1.3125rem] tracking-[0.00875rem] text-[rgba(38,37,30,0.6)]";

export function Footer() {
  return (
    <footer>
      <div className="mx-auto flex max-w-325 flex-col px-[1.172rem] pt-15.75 pb-15.75 md:pb-[1.758rem] lg:px-5 lg:pt-[4.2rem] lg:pb-7.5">
        <div className="flex flex-col gap-[0.35rem] sm:flex-row sm:items-center sm:gap-4">
          <a
            className={`${smallText} transition-opacity hover:opacity-80`}
            href="https://zitniklab.hms.harvard.edu/"
            rel="noopener noreferrer"
            target="_blank"
          >
            © {new Date().getFullYear()} Zitnik Lab
          </a>
          <span className={`${smallText} flex items-center gap-[0.3rem]`}>
            <FlaskConical size={13} strokeWidth={1.75} />
            <span>An open science, academic research project.</span>
          </span>
        </div>
      </div>
    </footer>
  );
}
