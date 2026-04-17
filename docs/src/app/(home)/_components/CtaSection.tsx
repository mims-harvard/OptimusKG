import { DownloadButton } from "./DownloadButton";

export function CtaSection() {
  return (
    <section
      id="download"
      className="l-section l-section--cta border-t border-[var(--l-border)] bg-[var(--l-bg)]"
    >
      <div className="mx-auto flex max-w-[810px] flex-col items-center gap-[1.4rem] text-center">
        <h2 className="text-balance text-[2.5rem] leading-[1.18] font-normal tracking-[-0.032em] text-[var(--l-ink)] sm:text-[3.25rem] min-[900px]:text-[4.1875rem]">
          Try OptimusKG now.
        </h2>
        <DownloadButton
          className="inline-flex items-center gap-1.5 rounded-full bg-[var(--l-ink)] px-[22.6px] pt-[13.48px] pb-[13.8px] text-[0.95625rem] text-[var(--l-bg)] transition-opacity hover:opacity-80"
        />
      </div>
    </section>
  );
}
