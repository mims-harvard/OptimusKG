import { DownloadButton } from "./DownloadButton";

const MEDIA_BG =
  "linear-gradient(rgba(38,37,30,0.05),rgba(38,37,30,0.05)), linear-gradient(#f2f1ed,#f2f1ed)";

export function HeroSection() {
  return (
    <section className="bg-[var(--l-bg)] pt-[6.498rem] pb-[3.9375rem] lg:pt-[6.953rem] lg:pb-[4.199rem]">
      <div className="mx-auto max-w-[81.25rem] px-[1.172rem] lg:px-[1.25rem]">
        <div className="flex flex-col gap-[3.281rem] lg:gap-[3.5rem]">
          <div className="flex flex-col gap-[1.3125rem] lg:gap-[1.399rem]">
            <h1
              className="font-normal text-[var(--l-ink)] lg:text-[1.55rem] lg:leading-[2.031rem] lg:tracking-[-0.020rem]"
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
            className="relative h-[42.5rem] overflow-hidden rounded-[0.25rem] md:h-[48.75rem] lg:h-[42.5rem] 2xl:h-[48.75rem]"
            style={{ backgroundImage: MEDIA_BG }}
          >
            <img
              src="/hero/bg.webp"
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
              className="hidden lg:block absolute left-1/2 -translate-x-1/2 rounded-[0.625rem] object-contain shadow-[0px_28px_70px_0px_rgba(0,0,0,0.14),0px_14px_32px_0px_rgba(0,0,0,0.1),0px_0px_0px_1px_rgba(38,37,30,0.1)]"
              style={{ top: "2rem", height: "38.5rem", aspectRatio: "3840 / 2808" }}
            />

            <img
              src="/features/figure.webp"
              alt="Schema figure"
              className="lg:hidden absolute rounded-[0.625rem] object-contain shadow-[0px_28px_70px_0px_rgba(0,0,0,0.14),0px_14px_32px_0px_rgba(0,0,0,0.1),0px_0px_0px_1px_rgba(38,37,30,0.1)]"
              style={{ left: "2rem", top: "2rem", height: "calc(100% - 4rem)", aspectRatio: "3840 / 2808" }}
            />

            <div className="pointer-events-none absolute inset-0 rounded-[0.25rem] border border-[var(--l-border-subtle)]" />
          </div>
        </div>
      </div>
    </section>
  );
}
