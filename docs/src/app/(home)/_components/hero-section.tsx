import { DownloadButton } from "./download-button";

const MEDIA_BG =
  "linear-gradient(rgba(38,37,30,0.05),rgba(38,37,30,0.05)), linear-gradient(#f2f1ed,#f2f1ed)";

export function HeroSection() {
  return (
    <section className="l-section l-section--first l-section--flush-bottom bg-[var(--l-bg)]">
      <div className="l-container">
        <div className="flex flex-col gap-13 min-[900px]:gap-14">
          <div className="flex flex-col gap-5.25 min-[900px]:gap-5.5">
            <h1 className="font-normal text-[var(--l-ink)] text-balance text-2xl leading-8">
              <span className="block">Unifying biomedical knowledge</span>
              <span className="block">in a modern multimodal graph</span>
            </h1>

            <div className="flex flex-wrap items-center gap-2.5">
              <DownloadButton
                className="rounded-full bg-[var(--l-ink)] py-3.5 px-5.75 text-sm leading-4 font-normal text-[var(--l-bg)]"
              />
              <a
                href="/docs"
                className="inline-flex items-center gap-1.5 whitespace-nowrap rounded-full border border-[var(--l-border)] py-3.5 px-5.75 text-sm leading-4 font-normal text-[var(--l-ink)] transition-opacity hover:opacity-70"
              >
                Read the docs
              </a>
            </div>
          </div>

          <div
            className="relative aspect-[16/10] max-h-195 min-h-96 overflow-hidden rounded-sm sm:aspect-[16/9] min-[900px]:aspect-auto min-[900px]:h-170 2xl:h-195"
            style={{ backgroundImage: MEDIA_BG }}
          >
            <img
              src="/hero/valley-stream.png"
              alt=""
              className="pointer-events-none absolute inset-0 h-full w-full scale-[1.1] object-cover"
            />
            <div
              className="pointer-events-none absolute inset-0"
              style={{ background: "linear-gradient(90deg,rgba(0,0,0,0.12) 0%,rgba(0,0,0,0.22) 100%)" }}
            />

            <img
              src="/features/figure.webp"
              alt="Schema figure"
              className="absolute left-1/2 top-1/2 -translate-x-1/2 -translate-y-1/2 max-h-[calc(100%-2rem)] max-w-[calc(100%-2rem)] rounded-[0.625rem] border border-[var(--l-border)] shadow-[0px_28px_70px_0px_rgba(0,0,0,0.14),0px_14px_32px_0px_rgba(0,0,0,0.1)] min-[900px]:max-h-[calc(100%-4rem)] min-[900px]:max-w-[calc(100%-4rem)]"
            />

            <div className="pointer-events-none absolute inset-0 rounded-sm border border-[var(--l-border-subtle)]" />
          </div>
        </div>
      </div>
    </section>
  );
}
