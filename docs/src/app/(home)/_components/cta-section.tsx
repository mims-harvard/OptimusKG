import { DownloadButton } from "./download-button";

export function CtaSection() {
  return (
    <section
      className="l-section l-section--cta border-[var(--l-border)] border-t bg-[var(--l-bg)]"
      id="download"
    >
      <div className="mx-auto flex max-w-[810px] flex-col items-center gap-5.5 text-center">
        <h2 className="text-balance font-normal text-4xl text-[var(--l-ink)] leading-[1.18] tracking-tight sm:text-5xl min-[900px]:text-7xl">
          Try OptimusKG now.
        </h2>
        <DownloadButton className="inline-flex items-center gap-1.5 rounded-none bg-[var(--l-ink)] px-5.75 py-3.5 text-[var(--l-bg)] text-base transition-opacity hover:opacity-80" />
      </div>
    </section>
  );
}
