// ─── Feature 3 assets (In every tool, at every step) ───
const F3_BG = "https://www.figma.com/api/mcp/asset/a2cb3a47-401b-4acf-bef4-86d91965ea40";

// ─── Feature 4 assets ───
const F4_BG = "https://www.figma.com/api/mcp/asset/59fc802f-4555-4c95-a7c1-44c5f90616e1";

// Shared window chrome (3 dots + optional title)
function WinChrome({ title, bg = "#f2f1ed" }: { title?: string; bg?: string }) {
  return (
    <div
      className="flex items-center shrink-0 relative px-[0.5rem]"
      style={{ height: "1.75rem", borderBottom: "1px solid rgba(38,37,30,0.1)", background: bg }}
    >
      <div className="flex gap-[0.375rem]">
        <span className="block size-[0.625rem] rounded-full bg-[rgba(38,37,30,0.2)]" />
        <span className="block size-[0.625rem] rounded-full bg-[rgba(38,37,30,0.2)]" />
        <span className="block size-[0.625rem] rounded-full bg-[rgba(38,37,30,0.2)]" />
      </div>
      {title && (
        <span className="absolute left-1/2 -translate-x-1/2 text-[0.7125rem] text-[#26251e] opacity-70 whitespace-nowrap">
          {title}
        </span>
      )}
    </div>
  );
}

// ─── Feature 1 media: figure.svg ─────────────────────────────────────────────
function Feature1Media() {
  return (
    <div className="absolute inset-0 bg-[#d9d5cf] overflow-hidden">
      {/* Desktop: centered, fully contained */}
      <img
        src="/figure.webp"
        alt="figure"
        className="hidden lg:block absolute inset-0 w-full h-full object-contain p-[2rem]"
      />
      {/* Mobile/tablet: anchored top-left, scaled to fill height, right side crops out */}
      <img
        src="/figure.webp"
        alt="figure"
        className="lg:hidden absolute top-[2rem] left-[2rem] h-[calc(100%-4rem)] w-auto max-w-none"
      />
    </div>
  );
}

// ─── Feature 2 media: data pipeline diagram ──────────────────────────────────
function Feature2Media() {
  return (
    <div className="absolute inset-0 bg-[#b6b9be] overflow-hidden">
      {/* Desktop: centered, fully contained */}
      <img
        src="/data-pipeline.webp"
        alt="Data pipeline"
        className="hidden lg:block absolute inset-0 w-full h-full object-contain p-[2rem]"
      />
      {/* Mobile: anchored top-left, fills height, right side crops */}
      <img
        src="/data-pipeline.webp"
        alt="Data pipeline"
        className="lg:hidden absolute top-[2rem] left-[2rem] h-[calc(100%-4rem)] w-auto max-w-none"
      />
    </div>
  );
}

// ─── Feature 3 media: two chart windows on background photo ──────────────────
function Feature3Media() {
  return (
    <div className="absolute inset-0 overflow-hidden rounded-[0.25rem]">
      {/* Background photo */}
      <div className="absolute inset-0 overflow-hidden">
        <img alt="" className="absolute max-w-none" style={{ height: "100%", left: "-45.96%", top: 0, width: "191.91%" }} src={F3_BG} />
      </div>
      {/* Subtle gradient overlay */}
      <div className="absolute inset-0" style={{ background: "linear-gradient(90deg,rgba(38,37,30,0.05) 0%,rgba(38,37,30,0.05) 100%)" }} />

      {/* ── Desktop (≥ lg): two windows ── */}

      {/* Window 1: Molecular Function — top-left, desktop only */}
      <div
        className="hidden lg:flex absolute bg-white rounded-[0.625rem] overflow-hidden flex-col shadow-[0px_28px_70px_0px_rgba(0,0,0,0.14),0px_14px_32px_0px_rgba(0,0,0,0.1),0px_0px_0px_1px_rgba(38,37,30,0.1)]"
        style={{ left: "4.75rem", top: "3.625rem", width: "30.75rem", height: "19.25rem" }}
      >
        <WinChrome title="Molecular Function" bg="white" />
        <div className="flex-1 min-h-0 flex items-center justify-center p-[1.25rem]">
          <img src="/molecular-function.webp" alt="Molecular Function" className="w-full h-full object-contain" />
        </div>
      </div>

      {/* Window 2: Phenotype — desktop: bottom-right overlapping, moved up */}
      <div
        className="hidden lg:flex absolute bg-white rounded-[0.625rem] overflow-hidden flex-col shadow-[0px_28px_70px_0px_rgba(0,0,0,0.14),0px_14px_32px_0px_rgba(0,0,0,0.1),0px_0px_0px_1px_rgba(38,37,30,0.1)]"
        style={{ left: "13.375rem", top: "14rem", width: "30.75rem", height: "24rem" }}
      >
        <WinChrome title="Phenotype" bg="white" />
        <div className="flex-1 min-h-0 flex items-center justify-center p-[1.25rem]">
          <img src="/phenotype.webp" alt="Phenotype" className="w-full h-full object-contain" />
        </div>
      </div>

      {/* ── Mobile (< lg): Phenotype only, anchored left, bleeds right ── */}
      <div
        className="lg:hidden absolute flex bg-white rounded-[0.625rem] overflow-hidden flex-col shadow-[0px_28px_70px_0px_rgba(0,0,0,0.14),0px_14px_32px_0px_rgba(0,0,0,0.1),0px_0px_0px_1px_rgba(38,37,30,0.1)]"
        style={{ left: "1.5rem", top: "2rem", width: "51rem", height: "36rem" }}
      >
        <WinChrome title="Phenotype" bg="white" />
        <div className="flex-1 min-h-0 flex items-center justify-center p-[1.25rem]">
          <img src="/phenotype.webp" alt="Phenotype" className="w-full h-full object-contain" />
        </div>
      </div>

      {/* Border overlay */}
      <div className="absolute inset-0 rounded-[0.25rem] border border-[rgba(38,37,30,0.03)]" />
    </div>
  );
}

// ─── Feature 4 media: Cursor IDE on landscape photo ──────────────────────────
function Feature4Media() {
  return (
    <div className="absolute inset-0 overflow-hidden rounded-[0.25rem]">
      {/* Mountains background */}
      <div className="absolute inset-0 overflow-hidden">
        <img alt="" className="absolute max-w-none" style={{ height: "100%", left: "-27.46%", top: 0, width: "154.93%" }} src={F4_BG} />
      </div>
      <div className="absolute inset-0" style={{ background: "linear-gradient(90deg,rgba(38,37,30,0.05) 0%,rgba(38,37,30,0.05) 100%)" }} />

      {/* Cursor IDE window */}
      <div
        className="absolute bg-[#f2f1ed] rounded-[0.625rem] overflow-hidden flex flex-col shadow-[0px_28px_70px_0px_rgba(0,0,0,0.14),0px_14px_32px_0px_rgba(0,0,0,0.1),0px_0px_0px_1px_rgba(38,37,30,0.1)]"
        style={{ left: "2rem", top: "2.8125rem", width: "42.5rem", height: "35rem" }}
      >
        <WinChrome title="Cursor" />
        {/* Tab bar */}
        <div className="flex items-end shrink-0 overflow-hidden" style={{ height: "1.887rem", borderBottom: "1px solid rgba(38,37,30,0.1)" }}>
          {/* Active tab: Dashboard.tsx */}
          <div
            className="flex items-center gap-[0.5rem] px-[0.75rem] bg-[#f7f7f4] h-full"
            style={{ borderRight: "1px solid rgba(38,37,30,0.1)" }}
          >
            <span className="text-[0.69375rem] text-[#26251e]">Dashboard.tsx</span>
            <span className="text-[0.5rem] text-[rgba(38,37,30,0.6)]">×</span>
          </div>
          {/* Inactive tab: SupportChat.tsx */}
          <div className="flex items-center gap-[0.5rem] px-[0.75rem] bg-[#f2f1ed] h-full">
            <span className="text-[0.69375rem] text-[rgba(38,37,30,0.6)]">SupportChat.tsx</span>
          </div>
          <div className="flex-1 border-b border-[rgba(38,37,30,0.1)]" />
          {/* Active tab bottom line */}
          <div className="absolute bottom-0 left-0 h-px bg-[#f7f7f4]" style={{ width: "7.8125rem" }} />
        </div>
        {/* Code editor */}
        <div className="flex-1 min-h-0 bg-[#f7f7f4] overflow-hidden pl-[0.5rem] pt-[0.5rem]">
          <pre className="text-[0.75rem] leading-[1.25rem] pl-[1.75rem]">
            <div><span className="text-[#9e94d5]">&quot;</span><span className="text-[#aa52a2]">use client</span><span className="text-[#9e94d5]">&quot;</span><span className="text-[rgba(20,20,20,0.92)]">;</span></div>
            <div className="mt-[1.25rem]">
              <div><span className="text-[#b3003f]">import</span><span className="text-[rgba(20,20,20,0.92)]"> React, {"{ useState }"} </span><span className="text-[#b3003f]">from</span><span className="text-[#9e94d5]"> &quot;</span><span className="text-[#aa52a2]">react</span><span className="text-[#9e94d5]">&quot;</span><span className="text-[rgba(20,20,20,0.92)]">;</span></div>
              <div><span className="text-[#b3003f]">import</span><span className="text-[rgba(20,20,20,0.92)]"> Navigation </span><span className="text-[#b3003f]">from</span><span className="text-[#9e94d5]"> &quot;</span><span className="text-[#aa52a2]">./Navigation</span><span className="text-[#9e94d5]">&quot;</span><span className="text-[rgba(20,20,20,0.92)]">;</span></div>
              <div><span className="text-[#b3003f]">import</span><span className="text-[rgba(20,20,20,0.92)]"> SupportChat </span><span className="text-[#b3003f]">from</span><span className="text-[#9e94d5]"> &quot;</span><span className="text-[#aa52a2]">./SupportChat</span><span className="text-[#9e94d5]">&quot;</span><span className="text-[rgba(20,20,20,0.92)]">;</span></div>
            </div>
            <div className="mt-[1.25rem]">
              <div><span className="text-[#b3003f]">export default function</span><span className="text-[#db704b]"> Dashboard() {"{"}</span></div>
            </div>
            <div className="mt-[1.25rem] whitespace-pre">
              <div><span className="text-[#b3003f]">  return</span><span className="text-[#db704b]"> (</span></div>
              <div><span className="text-[rgba(20,20,20,0.68)]">    &lt;</span><span className="text-[#1f8a65]">div</span><span className="text-[#6049b3]"> className</span><span className="text-[rgba(20,20,20,0.92)]">=</span><span className="text-[#9e94d5]">&quot;</span><span className="text-[#aa52a2]">flex h-[600px] border rounded-lg overflow-hidden</span><span className="text-[#9e94d5]">&quot;</span><span className="text-[rgba(20,20,20,0.68)]">&gt;</span></div>
              <div><span className="text-[rgba(20,20,20,0.68)]">      &lt;</span><span className="text-[#1f8a65]">div</span><span className="text-[#6049b3]"> className</span><span className="text-[rgba(20,20,20,0.92)]">=</span><span className="text-[#9e94d5]">&quot;</span><span className="text-[#aa52a2]">w-64 border-r</span><span className="text-[#9e94d5]">&quot;</span><span className="text-[rgba(20,20,20,0.68)]">&gt;</span></div>
              <div><span className="text-[rgba(20,20,20,0.68)]">      &lt;/</span><span className="text-[#1f8a65]">div</span><span className="text-[rgba(20,20,20,0.68)]">&gt;</span></div>
              <div><span className="text-[rgba(20,20,20,0.68)]">      &lt;</span><span className="text-[#1f8a65]">div</span><span className="text-[#6049b3]"> className</span><span className="text-[rgba(20,20,20,0.92)]">=</span><span className="text-[#9e94d5]">&quot;</span><span className="text-[#aa52a2]">w-80 border-l</span><span className="text-[#9e94d5]">&quot;</span><span className="text-[rgba(20,20,20,0.68)]">&gt;</span></div>
              <div><span className="text-[rgba(20,20,20,0.68)]">        &lt;</span><span className="text-[#1f8a65]">SupportChat</span><span className="text-[rgba(20,20,20,0.68)]"> /&gt;</span></div>
              <div><span className="text-[rgba(20,20,20,0.68)]">      &lt;/</span><span className="text-[#1f8a65]">div</span><span className="text-[rgba(20,20,20,0.68)]">&gt;</span></div>
              <div><span className="text-[rgba(20,20,20,0.68)]">    &lt;/</span><span className="text-[#1f8a65]">div</span><span className="text-[rgba(20,20,20,0.68)]">&gt;</span></div>
              <div><span className="text-[#db704b]">  );</span></div>
              <div><span className="text-[#db704b]">{"}"}</span></div>
            </div>
          </pre>
          {/* Cursor blink */}
          <div className="absolute bg-[#26251e]" style={{ left: "calc(0.5rem + 1.75rem + 18.9rem)", top: "calc(0.5rem + 8.125rem)", width: "0.125rem", height: "0.875rem" }} />
        </div>
      </div>

      {/* Border overlay */}
      <div className="absolute inset-0 rounded-[0.25rem] border border-[rgba(38,37,30,0.03)]" />
    </div>
  );
}

// ─── Text content block ───────────────────────────────────────────────────────
function FeatureText({
  title,
  description,
  ctaText,
  ctaHref,
  isLink,
}: {
  title: string;
  description: string;
  ctaText: string;
  ctaHref?: string;
  isLink: boolean;
}) {
  const cta = isLink ? (
    <span style={{ fontSize: "0.95625rem", lineHeight: "1.5rem" }} className="text-[#f54e00]">
      {ctaText}
    </span>
  ) : (
    <a href={ctaHref} target="_blank" rel="noopener noreferrer" style={{ fontSize: "0.95625rem", lineHeight: "1.5rem" }} className="text-[#f54e00]">
      {ctaText}
    </a>
  );

  return (
    <div className="flex flex-col" style={{ gap: "0.9325rem" }}>
      <div className="flex flex-col">
        <h3
          className="text-[#26251e] font-normal"
          style={{ fontSize: "1.31875rem", lineHeight: "1.7875rem", letterSpacing: "-0.006875rem" }}
        >
          {title}
        </h3>
        <p
          className="text-[rgba(38,37,30,0.6)] font-normal"
          style={{ fontSize: "1.31875rem", lineHeight: "1.7875rem", letterSpacing: "-0.006875rem" }}
        >
          {description}
        </p>
      </div>
      {cta}
    </div>
  );
}

// ─── Desktop card: text-left / image-right (Features 1 & 3) ─────────────────
function CardTextLeft({
  href,
  cardH,
  mediaH,
  text,
  Media,
}: {
  href: string;
  cardH: string;
  mediaH: string;
  text: React.ReactNode;
  Media: React.ComponentType;
}) {
  return (
    <div className="relative" style={{ height: cardH }}>
      {/* bg + text layer */}
      <a
        href={href}
        target="_blank"
        rel="noopener noreferrer"
        className="absolute inset-0 bg-[#f2f1ed] rounded-[0.25rem] p-[1.09375rem] grid grid-cols-[repeat(24,minmax(0,1fr))] gap-x-[0.625rem]"
      >
        <div className="col-[1/span_8] row-[1/span_2] flex flex-col justify-center pl-[0.15625rem] pr-[1.875rem]">
          {text}
        </div>
        <div className="absolute inset-0 rounded-[0.25rem] border border-[rgba(38,37,30,0.03)]" />
      </a>
      {/* image overlay */}
      <div className="absolute inset-0 p-[1.09375rem] grid grid-cols-[repeat(24,minmax(0,1fr))] gap-x-[0.625rem] pointer-events-none">
        <div className="col-[9/span_16] overflow-hidden rounded-[0.25rem] relative" style={{ height: mediaH }}>
          <Media />
        </div>
      </div>
    </div>
  );
}

// ─── Desktop card: image-left / text-right (Features 2 & 4) ─────────────────
function CardImageLeft({
  CardTag,
  href,
  cardH,
  mediaH,
  text,
  Media,
}: {
  CardTag: "div" | "a";
  href?: string;
  cardH: string;
  mediaH: string;
  text: React.ReactNode;
  Media: React.ComponentType;
}) {
  const bgProps =
    CardTag === "a"
      ? { href, target: "_blank", rel: "noopener noreferrer" }
      : {};

  const BgEl = CardTag as React.ElementType;

  return (
    <div className="relative" style={{ height: cardH }}>
      {/* bg + text layer */}
      <BgEl
        {...bgProps}
        className="absolute inset-0 bg-[#f2f1ed] rounded-[0.25rem] p-[1.09375rem] grid grid-cols-[repeat(24,minmax(0,1fr))] gap-x-[0.625rem]"
      >
        <div className="col-[17/span_8] row-[1/span_2] flex flex-col justify-center pl-[1.875rem] pr-[0.15625rem]">
          {text}
        </div>
        <div className="absolute inset-0 rounded-[0.25rem] border border-[rgba(38,37,30,0.03)]" />
      </BgEl>
      {/* image overlay */}
      <div className="absolute inset-0 p-[1.09375rem] grid grid-cols-[repeat(24,minmax(0,1fr))] gap-x-[0.625rem] pointer-events-none">
        <div className="col-[1/span_16] overflow-hidden rounded-[0.25rem] relative" style={{ height: mediaH }}>
          <Media />
        </div>
      </div>
    </div>
  );
}

// ─── Mobile card: stacked (text top, image bottom) ───────────────────────────
function MobileCard({
  CardTag,
  href,
  text,
  Media,
}: {
  CardTag: "div" | "a";
  href?: string;
  text: React.ReactNode;
  Media: React.ComponentType;
}) {
  const props =
    CardTag === "a"
      ? { href, target: "_blank", rel: "noopener noreferrer" }
      : {};

  const El = CardTag as React.ElementType;

  return (
    <El
      {...props}
      className="relative bg-[#f2f1ed] rounded-[0.25rem] flex flex-col overflow-hidden"
    >
      <div className="p-[1.025rem]">{text}</div>
      <div className="relative shrink-0 overflow-hidden" style={{ height: "42.5rem" }}>
        <Media />
      </div>
      <div className="absolute inset-0 rounded-[0.25rem] border border-[rgba(38,37,30,0.03)]" />
    </El>
  );
}

// ─── Main export ──────────────────────────────────────────────────────────────
export function FeaturesSection() {
  const f1text = (
    <FeatureText
      title="Rich strong-typed properties"
      description="Every entity is enriched with structured properties for fine-grained analysis."
      ctaText="Learn about the schema →"
      isLink
    />
  );

  const f2text = (
    <FeatureText
      title="Works autonomously, runs in parallel"
      description="Agents use their own computers to build, test, and demo features end to end for you to review."
      ctaText="Learn about cloud agents →"
      ctaHref="https://cursor.com/docs/cloud-agent"
      isLink={false}
    />
  );

  const f3text = (
    <FeatureText
      title="In every tool, at every step"
      description="Cursor reviews your PRs in GitHub, collaborates in Slack, and runs in your terminal."
      ctaText="Learn about Cursor's surfaces →"
      isLink
    />
  );

  const f4text = (
    <FeatureText
      title="Magically accurate autocomplete"
      description="Our specialized Tab model predicts your next action with striking speed and precision."
      ctaText="Learn about Tab →"
      isLink
    />
  );

  return (
    <section className="bg-[#f7f7f4]" style={{ paddingBlock: "4.2rem" }}>
      {/* ── Desktop (≥ lg): side-by-side 24-col grid ── */}
      <div className="hidden lg:flex flex-col px-[1.25rem] max-w-[81.25rem] mx-auto w-full" style={{ gap: "5.6rem" }}>
        {/* Feature 1: text-left, image-right — card height 715px = 44.6875rem */}
        <CardTextLeft
          href="https://cursor.com/product"
          cardH="44.6875rem"
          mediaH="42.5rem"
          text={f1text}
          Media={Feature1Media}
        />

        {/* Feature 2: image-left, text-right */}
        <CardImageLeft
          CardTag="div"
          cardH="44.6875rem"
          mediaH="42.5rem"
          text={f2text}
          Media={Feature2Media}
        />

        {/* Feature 3: text-left, image-right — card height 685px = 42.8125rem */}
        <CardTextLeft
          href="https://cursor.com/product"
          cardH="42.8125rem"
          mediaH="40.625rem"
          text={f3text}
          Media={Feature3Media}
        />

        {/* Feature 4: image-left, text-right */}
        <CardImageLeft
          CardTag="a"
          href="https://cursor.com/product/tab"
          cardH="42.8125rem"
          mediaH="40.625rem"
          text={f4text}
          Media={Feature4Media}
        />
      </div>

      {/* ── Mobile / tablet (< lg): stacked ── */}
      <div
        className="lg:hidden flex flex-col px-[1.172rem]"
        style={{ gap: "5.25rem" }}
      >
        <MobileCard CardTag="a" href="https://cursor.com/product" text={f1text} Media={Feature1Media} />
        <MobileCard CardTag="div" text={f2text} Media={Feature2Media} />
        <MobileCard CardTag="a" href="https://cursor.com/product" text={f3text} Media={Feature3Media} />
        <MobileCard CardTag="a" href="https://cursor.com/product/tab" text={f4text} Media={Feature4Media} />
      </div>
    </section>
  );
}
