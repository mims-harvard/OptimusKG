import { DownloadButton } from "./DownloadButton";

const MEDIA_BG =
  "linear-gradient(rgba(38,37,30,0.05),rgba(38,37,30,0.05)), linear-gradient(#f2f1ed,#f2f1ed)";

export function HeroSection() {
  return (
    <section className="l-section l-section--first l-section--flush-bottom bg-[var(--l-bg)]">
      <div className="l-container">
        <div className="flex flex-col gap-[3.281rem] min-[900px]:gap-[3.5rem]">
          <div className="flex flex-col gap-[1.3125rem] min-[900px]:gap-[1.399rem]">
            <h1
              className="font-normal text-[var(--l-ink)] text-balance min-[900px]:text-[1.55rem] min-[900px]:leading-[2.031rem] min-[900px]:tracking-[-0.020rem]"
              style={{ fontSize: "1.456rem", lineHeight: "1.904rem", letterSpacing: "-0.019rem" }}
            >
              <span className="block">Unifying biomedical knowledge</span>
              <span className="block">in a modern multimodal graph</span>
            </h1>

            <DownloadButton
              className="self-start rounded-full bg-[var(--l-ink)] font-normal text-[var(--l-bg)]"
              style={{
                fontSize: "0.894rem",
                lineHeight: "1rem",
                paddingBlock: "0.864rem",
                paddingInline: "1.4125rem",
              }}
            />
          </div>

          <div
            className="relative aspect-[16/10] max-h-[48.75rem] min-h-[24rem] overflow-hidden rounded-[0.25rem] sm:aspect-[16/9] min-[900px]:aspect-auto min-[900px]:h-[42.5rem] 2xl:h-[48.75rem]"
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

            <div className="pointer-events-none absolute inset-0 rounded-[0.25rem] border border-[var(--l-border-subtle)]" />
          </div>
        </div>
      </div>
    </section>
  );
}
