import { DownloadButton } from "./download-button";

export function CtaSection() {
  return (
    <section
      className="l-section l-section--cta border-(--l-border) border-t bg-(--l-bg)"
      id="download"
    >
      <div className="mx-auto flex max-w-202.5 flex-col items-center gap-5.5 text-center">
        <h2 className="text-balance font-normal text-(--l-ink) text-4xl leading-[1.18] tracking-tight sm:text-5xl min-[900px]:text-7xl">
          Try OptimusKG now.
        </h2>
        <DownloadButton className="inline-flex items-center gap-1.5 rounded-none bg-(--l-ink) px-5.75 py-3.5 text-(--l-bg) text-base transition-opacity hover:opacity-80" />
      </div>
    </section>
  );
}
