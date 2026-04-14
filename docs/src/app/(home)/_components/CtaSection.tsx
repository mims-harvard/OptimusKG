import { DownloadButton } from "./DownloadButton";

export function CtaSection() {
  return (
    <section
      id="download"
      className="border-t border-[var(--l-border)] bg-[var(--l-bg)] py-[5.25rem] pb-[8.4rem]"
    >
      <div className="mx-auto flex max-w-[810px] flex-col items-center gap-[1.4rem] px-6 text-center">
        <h2 className="text-[4.1875rem] leading-[1.18] font-normal tracking-[-0.032em] text-[var(--l-ink)] whitespace-nowrap">
          Try OptimusKG now.
        </h2>
        <DownloadButton
          className="inline-flex items-center gap-1.5 rounded-full bg-[var(--l-ink)] px-[22.6px] pt-[13.48px] pb-[13.8px] text-[0.95625rem] text-[var(--l-bg)] transition-opacity hover:opacity-80"
        />
      </div>
    </section>
  );
}
