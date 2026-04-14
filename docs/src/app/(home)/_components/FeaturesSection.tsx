import { cn } from "@/lib/cn";

const F3_BG = "https://www.figma.com/api/mcp/asset/a2cb3a47-401b-4acf-bef4-86d91965ea40";
const F4_BG = "https://www.figma.com/api/mcp/asset/59fc802f-4555-4c95-a7c1-44c5f90616e1";

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
        <span className="absolute left-1/2 -translate-x-1/2 text-[0.7125rem] text-[var(--l-ink)] opacity-70 whitespace-nowrap">
          {title}
        </span>
      )}
    </div>
  );
}

function Feature1Media() {
  return (
    <div className="absolute inset-0 bg-[#d9d5cf] overflow-hidden">
      <img
        src="/figure.webp"
        alt="figure"
        className="hidden lg:block absolute inset-0 w-full h-full object-contain p-[2rem]"
      />
      <img
        src="/figure.webp"
        alt="figure"
        className="lg:hidden absolute top-[2rem] left-[2rem] h-[calc(100%-4rem)] w-auto max-w-none"
      />
    </div>
  );
}

function Feature2Media() {
  return (
    <div className="absolute inset-0 bg-[#b6b9be] overflow-hidden">
      <img
        src="/data-pipeline.webp"
        alt="Data pipeline"
        className="hidden lg:block absolute inset-0 w-full h-full object-contain p-[2rem]"
      />
      <img
        src="/data-pipeline.webp"
        alt="Data pipeline"
        className="lg:hidden absolute top-[2rem] left-[2rem] h-[calc(100%-4rem)] w-auto max-w-none"
      />
    </div>
  );
}

function Feature3Media() {
  return (
    <div className="absolute inset-0 overflow-hidden rounded-[0.25rem]">
      <div className="absolute inset-0 overflow-hidden">
        <img alt="" className="absolute max-w-none" style={{ height: "100%", left: "-45.96%", top: 0, width: "191.91%" }} src={F3_BG} />
      </div>
      <div className="absolute inset-0" style={{ background: "linear-gradient(90deg,rgba(38,37,30,0.05) 0%,rgba(38,37,30,0.05) 100%)" }} />

      <div
        className="hidden lg:flex absolute bg-white rounded-[0.625rem] overflow-hidden flex-col shadow-[0px_28px_70px_0px_rgba(0,0,0,0.14),0px_14px_32px_0px_rgba(0,0,0,0.1),0px_0px_0px_1px_rgba(38,37,30,0.1)]"
        style={{ left: "4.75rem", top: "3.625rem", width: "30.75rem", height: "19.25rem" }}
      >
        <WinChrome title="Molecular Function" bg="white" />
        <div className="flex-1 min-h-0 flex items-center justify-center p-[1.25rem]">
          <img src="/molecular-function.webp" alt="Molecular Function" className="w-full h-full object-contain" />
        </div>
      </div>

      <div
        className="hidden lg:flex absolute bg-white rounded-[0.625rem] overflow-hidden flex-col shadow-[0px_28px_70px_0px_rgba(0,0,0,0.14),0px_14px_32px_0px_rgba(0,0,0,0.1),0px_0px_0px_1px_rgba(38,37,30,0.1)]"
        style={{ left: "13.375rem", top: "14rem", width: "30.75rem", height: "24rem" }}
      >
        <WinChrome title="Phenotype" bg="white" />
        <div className="flex-1 min-h-0 flex items-center justify-center p-[1.25rem]">
          <img src="/phenotype.webp" alt="Phenotype" className="w-full h-full object-contain" />
        </div>
      </div>

      <div
        className="lg:hidden absolute flex bg-white rounded-[0.625rem] overflow-hidden flex-col shadow-[0px_28px_70px_0px_rgba(0,0,0,0.14),0px_14px_32px_0px_rgba(0,0,0,0.1),0px_0px_0px_1px_rgba(38,37,30,0.1)]"
        style={{ left: "1.5rem", top: "2rem", width: "51rem", height: "36rem" }}
      >
        <WinChrome title="Phenotype" bg="white" />
        <div className="flex-1 min-h-0 flex items-center justify-center p-[1.25rem]">
          <img src="/phenotype.webp" alt="Phenotype" className="w-full h-full object-contain" />
        </div>
      </div>

      <div className="absolute inset-0 rounded-[0.25rem] border border-[var(--l-border-subtle)]" />
    </div>
  );
}

function Feature4Media() {
  return (
    <div className="absolute inset-0 overflow-hidden rounded-[0.25rem]">
      <div className="absolute inset-0 overflow-hidden">
        <img alt="" className="absolute max-w-none" style={{ height: "100%", left: "-27.46%", top: 0, width: "154.93%" }} src={F4_BG} />
      </div>
      <div className="absolute inset-0" style={{ background: "linear-gradient(90deg,rgba(38,37,30,0.05) 0%,rgba(38,37,30,0.05) 100%)" }} />

      <div
        className="absolute bg-[var(--l-surface)] rounded-[0.625rem] overflow-hidden flex flex-col shadow-[0px_28px_70px_0px_rgba(0,0,0,0.14),0px_14px_32px_0px_rgba(0,0,0,0.1),0px_0px_0px_1px_rgba(38,37,30,0.1)]"
        style={{ left: "2rem", top: "2.8125rem", width: "42.5rem", height: "35rem" }}
      >
        <WinChrome title="Cursor" />
        <div className="flex items-end shrink-0 overflow-hidden" style={{ height: "1.887rem", borderBottom: "1px solid rgba(38,37,30,0.1)" }}>
          <div
            className="flex items-center gap-[0.5rem] px-[0.75rem] bg-[var(--l-bg)] h-full"
            style={{ borderRight: "1px solid rgba(38,37,30,0.1)" }}
          >
            <span className="text-[0.69375rem] text-[var(--l-ink)]">Dashboard.tsx</span>
            <span className="text-[0.5rem] text-[var(--l-ink-muted)]">×</span>
          </div>
          <div className="flex items-center gap-[0.5rem] px-[0.75rem] bg-[var(--l-surface)] h-full">
            <span className="text-[0.69375rem] text-[var(--l-ink-muted)]">SupportChat.tsx</span>
          </div>
          <div className="flex-1 border-b border-[var(--l-border)]" />
          <div className="absolute bottom-0 left-0 h-px bg-[var(--l-bg)]" style={{ width: "7.8125rem" }} />
        </div>
        <div className="flex-1 min-h-0 bg-[var(--l-bg)] overflow-hidden pl-[0.5rem] pt-[0.5rem]">
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
          <div className="absolute bg-[#26251e]" style={{ left: "calc(0.5rem + 1.75rem + 18.9rem)", top: "calc(0.5rem + 8.125rem)", width: "0.125rem", height: "0.875rem" }} />
        </div>
      </div>

      <div className="absolute inset-0 rounded-[0.25rem] border border-[var(--l-border-subtle)]" />
    </div>
  );
}

const HEADING_STYLE = { fontSize: "1.31875rem", lineHeight: "1.7875rem", letterSpacing: "-0.006875rem" };
const CTA_STYLE = { fontSize: "0.95625rem", lineHeight: "1.5rem" };

function FeatureText({
  title,
  description,
  ctaText,
  ctaHref,
}: {
  title: string;
  description: string;
  ctaText: string;
  ctaHref?: string;
}) {
  const cta = ctaHref ? (
    <a href={ctaHref} target="_blank" rel="noopener noreferrer" style={CTA_STYLE} className="text-[var(--l-accent)]">
      {ctaText}
    </a>
  ) : (
    <span style={CTA_STYLE} className="text-[var(--l-accent)]">
      {ctaText}
    </span>
  );
  return (
    <div className="flex flex-col" style={{ gap: "0.9325rem" }}>
      <div className="flex flex-col">
        <h3 className="font-normal text-[var(--l-ink)]" style={HEADING_STYLE}>{title}</h3>
        <p className="font-normal text-[var(--l-ink-muted)]" style={HEADING_STYLE}>{description}</p>
      </div>
      {cta}
    </div>
  );
}

type Feature = {
  title: string;
  description: string;
  ctaText: string;
  ctaHref?: string;
  Media: React.ComponentType;
  href?: string;
  imageSide: "left" | "right";
  cardH: string;
  mediaH: string;
};

const FEATURES: Feature[] = [
  {
    title: "Rich strong-typed properties",
    description: "Every entity is enriched with structured properties for fine-grained analysis.",
    ctaText: "Learn about the schema →",
    Media: Feature1Media,
    href: "https://cursor.com/product",
    imageSide: "right",
    cardH: "44.6875rem",
    mediaH: "42.5rem",
  },
  {
    title: "Works autonomously, runs in parallel",
    description: "Agents use their own computers to build, test, and demo features end to end for you to review.",
    ctaText: "Learn about cloud agents →",
    ctaHref: "https://cursor.com/docs/cloud-agent",
    Media: Feature2Media,
    imageSide: "left",
    cardH: "44.6875rem",
    mediaH: "42.5rem",
  },
  {
    title: "In every tool, at every step",
    description: "Cursor reviews your PRs in GitHub, collaborates in Slack, and runs in your terminal.",
    ctaText: "Learn about Cursor's surfaces →",
    Media: Feature3Media,
    href: "https://cursor.com/product",
    imageSide: "right",
    cardH: "42.8125rem",
    mediaH: "40.625rem",
  },
  {
    title: "Magically accurate autocomplete",
    description: "Our specialized Tab model predicts your next action with striking speed and precision.",
    ctaText: "Learn about Tab →",
    Media: Feature4Media,
    href: "https://cursor.com/product/tab",
    imageSide: "left",
    cardH: "42.8125rem",
    mediaH: "40.625rem",
  },
];

function DesktopCard({ feature }: { feature: Feature }) {
  const { href, imageSide, cardH, mediaH, Media } = feature;
  const linkProps = href ? { href, target: "_blank", rel: "noopener noreferrer" as const } : {};
  const Tag = href ? "a" : "div";
  const textCol = imageSide === "right"
    ? "col-[1/span_8] pl-[0.15625rem] pr-[1.875rem]"
    : "col-[17/span_8] pl-[1.875rem] pr-[0.15625rem]";
  const imageCol = imageSide === "right" ? "col-[9/span_16]" : "col-[1/span_16]";

  return (
    <div className="relative" style={{ height: cardH }}>
      <Tag
        {...linkProps}
        className="absolute inset-0 grid grid-cols-[repeat(24,minmax(0,1fr))] gap-x-[0.625rem] rounded-[0.25rem] bg-[var(--l-surface)] p-[1.09375rem]"
      >
        <div className={cn("row-[1/span_2] flex flex-col justify-center", textCol)}>
          <FeatureText {...feature} />
        </div>
        <div className="pointer-events-none absolute inset-0 rounded-[0.25rem] border border-[var(--l-border-subtle)]" />
      </Tag>
      <div className="pointer-events-none absolute inset-0 grid grid-cols-[repeat(24,minmax(0,1fr))] gap-x-[0.625rem] p-[1.09375rem]">
        <div className={cn("relative overflow-hidden rounded-[0.25rem]", imageCol)} style={{ height: mediaH }}>
          <Media />
        </div>
      </div>
    </div>
  );
}

function MobileCard({ feature }: { feature: Feature }) {
  const { href, Media } = feature;
  const linkProps = href ? { href, target: "_blank", rel: "noopener noreferrer" as const } : {};
  const Tag = href ? "a" : "div";

  return (
    <Tag
      {...linkProps}
      className="relative flex flex-col overflow-hidden rounded-[0.25rem] bg-[var(--l-surface)]"
    >
      <div className="p-[1.025rem]">
        <FeatureText {...feature} />
      </div>
      <div className="relative h-[42.5rem] shrink-0 overflow-hidden">
        <Media />
      </div>
      <div className="pointer-events-none absolute inset-0 rounded-[0.25rem] border border-[var(--l-border-subtle)]" />
    </Tag>
  );
}

export function FeaturesSection() {
  return (
    <section className="bg-[var(--l-bg)]" style={{ paddingBlock: "4.2rem" }}>
      <div className="mx-auto hidden w-full max-w-[81.25rem] flex-col px-[1.25rem] lg:flex" style={{ gap: "5.6rem" }}>
        {FEATURES.map((f) => (
          <DesktopCard key={f.title} feature={f} />
        ))}
      </div>
      <div className="flex flex-col px-[1.172rem] lg:hidden" style={{ gap: "5.25rem" }}>
        {FEATURES.map((f) => (
          <MobileCard key={f.title} feature={f} />
        ))}
      </div>
    </section>
  );
}
