import { toJsxRuntime } from "hast-util-to-jsx-runtime";
import { ArrowRight } from "lucide-react";
import { Fragment } from "react";
import { jsx, jsxs } from "react/jsx-runtime";
import { type BundledLanguage, codeToHast } from "shiki";

import { DisGenEdge } from "@/components/disease-assoc-edge-schemas";
import { GeneSchema } from "@/components/gene-schema";
import { cn } from "@/lib/cn";

import { EditorWindow } from "./EditorWindow";
import { Snippet } from "./Snippet";
import { TabbedEditor } from "./TabbedEditor";

async function renderShiki(code: string, lang: BundledLanguage) {
  const hast = await codeToHast(code, { lang, themes: SHIKI_THEMES });
  return toJsxRuntime(hast, { Fragment, jsx, jsxs });
}

const SHIKI_THEMES = {
  light: "catppuccin-latte",
  dark: "catppuccin-mocha",
} as const;

const F3_BG = "/hero/mountain-overlook.png";
const F4_BG = "/hero/hillside-village.png";

const FEATURE1_SNIPPET = `import optimuskg

# Download a parquet file and cache it locally
path = optimuskg.get_file("nodes/gene.parquet")

# Load a single table as a Polars DataFrame
drugs = optimuskg.load_parquet("nodes/drug.parquet")

# Load the largest connected component
nodes, edges = optimuskg.load_graph(lcc=True)

# Or load it as a NetworkX MultiDiGraph
G = optimuskg.load_networkx(lcc=True)
`;

function ShikiBlock({ children }: { children: React.ReactNode }) {
  return (
    <div className="l-shiki-block h-full w-full overflow-auto p-[0.5rem]">
      {children}
    </div>
  );
}

function ImageTabContent({ src, alt }: { src: string; alt: string }) {
  return (
    <div className="flex h-full w-full items-center justify-center p-[1.25rem]">
      <img alt={alt} className="h-full w-full object-contain" src={src} />
    </div>
  );
}

function SchemaTabContent({ children }: { children: React.ReactNode }) {
  return (
    <div className="h-full w-full overflow-auto p-[1rem] [&>*:first-child]:!mt-0 [&>*:last-child]:!mb-0">
      {children}
    </div>
  );
}

function Feature1Media() {
  return (
    <div className="absolute inset-0 flex items-center justify-center overflow-hidden rounded-[0.25rem]">
      <img
        alt=""
        className="pointer-events-none absolute inset-0 h-full w-full scale-[1.1] object-cover"
        src="/hero/lakeside-village.png"
      />
      <div
        className="pointer-events-none absolute inset-0"
        style={{
          background: "linear-gradient(90deg,rgba(0,0,0,0.12) 0%,rgba(0,0,0,0.22) 100%)",
        }}
      />

      <TabbedEditor
        style={{
          width: "min(48rem, calc(100% - 4rem))",
          height: "min(36rem, calc(100% - 4rem))",
        }}
        tabs={[
          {
            name: "Gene Nodes Schema",
            content: (
              <SchemaTabContent>
                <GeneSchema />
              </SchemaTabContent>
            ),
          },
          {
            name: "Disease-Gene Edges Schema",
            content: (
              <SchemaTabContent>
                <DisGenEdge />
              </SchemaTabContent>
            ),
          },
        ]}
        title="Graph Schema"
      />

      <div className="pointer-events-none absolute inset-0 rounded-[0.25rem] border border-[var(--l-border-subtle)]" />
    </div>
  );
}

function Feature2Media() {
  return (
    <div className="absolute inset-0 overflow-hidden bg-[#b6b9be]">
      <img
        alt="Data pipeline"
        className="absolute inset-0 hidden h-full w-full object-contain p-[2rem] min-[900px]:block"
        src="/features/data-pipeline.webp"
      />
      <img
        alt="Data pipeline"
        className="absolute top-[2rem] left-[2rem] h-[calc(100%-4rem)] w-auto max-w-none min-[900px]:hidden"
        src="/features/data-pipeline.webp"
      />
    </div>
  );
}

function Feature3Media() {
  return (
    <div className="absolute inset-0 flex items-center justify-center overflow-hidden rounded-[0.25rem]">
      <div className="pointer-events-none absolute inset-0 overflow-hidden">
        <img
          alt=""
          className="absolute max-w-none"
          src={F3_BG}
          style={{ height: "100%", left: "-45.96%", top: 0, width: "191.91%" }}
        />
      </div>
      <div
        className="pointer-events-none absolute inset-0"
        style={{
          background: "linear-gradient(90deg,rgba(38,37,30,0.05) 0%,rgba(38,37,30,0.05) 100%)",
        }}
      />

      <TabbedEditor
        style={{
          width: "min(42.5rem, calc(100% - 4rem))",
          height: "min(35rem, calc(100% - 4rem))",
        }}
        tabs={[
          {
            name: "Molecular Function Validation",
            content: (
              <ImageTabContent alt="Molecular Function" src="/features/molecular-function.webp" />
            ),
          },
          {
            name: "Phenotype Validation",
            content: <ImageTabContent alt="Phenotype" src="/features/phenotype.webp" />,
          },
        ]}
        title="PaperQA3 Analysis"
      />

      <div className="pointer-events-none absolute inset-0 rounded-[0.25rem] border border-[var(--l-border-subtle)]" />
    </div>
  );
}

async function Feature4Media() {
  const loadGraphJsx = await renderShiki(FEATURE1_SNIPPET, "python");
  return (
    <div className="absolute inset-0 flex items-center justify-center overflow-hidden rounded-[0.25rem]">
      <div className="pointer-events-none absolute inset-0 overflow-hidden">
        <img
          alt=""
          className="absolute max-w-none"
          src={F4_BG}
          style={{ height: "100%", left: "-27.46%", top: 0, width: "154.93%" }}
        />
      </div>
      <div
        className="pointer-events-none absolute inset-0"
        style={{
          background: "linear-gradient(90deg,rgba(38,37,30,0.05) 0%,rgba(38,37,30,0.05) 100%)",
        }}
      />

      <TabbedEditor
        style={{
          width: "min(42.5rem, calc(100% - 4rem))",
          height: "min(35rem, calc(100% - 4rem))",
        }}
        tabs={[
          {
            name: "load_graph.py",
            content: <ShikiBlock>{loadGraphJsx}</ShikiBlock>,
          },
        ]}
        title="Python Client"
      />

      <div className="pointer-events-none absolute inset-0 rounded-[0.25rem] border border-[var(--l-border-subtle)]" />
    </div>
  );
}

const HEADING_STYLE = {
  fontSize: "1.31875rem",
  lineHeight: "1.7875rem",
  letterSpacing: "-0.006875rem",
};
const CTA_STYLE = { fontSize: "0.95625rem", lineHeight: "1.5rem" };

function FeatureText({
  title,
  description,
  ctaText,
  ctaHref,
  ctaVariant = "link",
}: {
  title: string;
  description: string;
  ctaText: string;
  ctaHref?: string;
  ctaVariant?: "link" | "snippet";
}) {
  let cta: React.ReactNode;
  if (ctaVariant === "snippet") {
    cta = <Snippet text={ctaText} />;
  } else {
    const ctaContent = (
      <>
        {ctaText}
        <ArrowRight
          aria-hidden="true"
          className="transition-transform duration-300 ease-out group-hover/card:translate-x-1 group-hover/cta:translate-x-1"
          size={16}
          strokeWidth={2}
        />
      </>
    );
    const ctaClass = "group/cta inline-flex items-center gap-[0.15rem] text-[var(--l-accent)]";
    cta = ctaHref ? (
      <a
        className={ctaClass}
        href={ctaHref}
        rel="noopener noreferrer"
        style={CTA_STYLE}
        target="_blank"
      >
        {ctaContent}
      </a>
    ) : (
      <span className={ctaClass} style={CTA_STYLE}>
        {ctaContent}
      </span>
    );
  }
  return (
    <div className="flex flex-col" style={{ gap: "0.9325rem" }}>
      <div className="flex flex-col">
        <h3 className="font-normal text-[var(--l-ink)]" style={HEADING_STYLE}>
          {title}
        </h3>
        <p className="font-normal text-[var(--l-ink-muted)]" style={HEADING_STYLE}>
          {description}
        </p>
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
  ctaVariant?: "link" | "snippet";
  Media: React.ComponentType;
  href?: string;
  imageSide: "left" | "right";
  cardH: string;
  mediaH: string;
};

const FEATURES: Feature[] = [
  {
    title: "Rich strongly-typed properties",
    description: "Every entity is enriched with structured properties for fine-grained analysis.",
    ctaText: "Learn about the schema",
    Media: Feature1Media,
    href: "/docs/graph-schema/nodes",
    imageSide: "right",
    cardH: "44.6875rem",
    mediaH: "42.5rem",
  },
  {
    title: "Delightfully simple Python client",
    description:
      "Install with one command and load the graph as Polars data frames or a NetworkX graph in a single line.",
    ctaText: "uv add optimuskg",
    ctaVariant: "snippet",
    Media: Feature4Media,
    imageSide: "left",
    cardH: "42.8125rem",
    mediaH: "40.625rem",
  },
  {
    title: "Rigorously validated associations",
    description:
      "Every edge is cross-validated against millions of research papers by PaperQA3, a deep research agent.",
    ctaText: "Learn about our methodology",
    Media: Feature3Media,
    href: "https://arxiv.org", // TODO: Update once we have the link
    imageSide: "right",
    cardH: "44.6875rem",
    mediaH: "42.5rem",
  },
  // {
  //   title: "Works autonomously, runs in parallel",
  //   description:
  //     "Agents use their own computers to build, test, and demo features end to end for you to review.",
  //   ctaText: "Learn about cloud agents",
  //   ctaHref: "https://cursor.com/docs/cloud-agent",
  //   Media: Feature2Media,
  //   imageSide: "left",
  //   cardH: "42.8125rem",
  //   mediaH: "40.625rem",
  // },
];

function DesktopCard({ feature }: { feature: Feature }) {
  const { href, imageSide, cardH, mediaH, Media } = feature;
  const linkProps = href ? { href, target: "_blank", rel: "noopener noreferrer" as const } : {};
  const Tag = href ? "a" : "div";
  const textCol =
    imageSide === "right"
      ? "col-[1/span_8] pl-[0.15625rem] pr-[1.875rem]"
      : "col-[17/span_8] pl-[1.875rem] pr-[0.15625rem]";
  const imageCol = imageSide === "right" ? "col-[9/span_16]" : "col-[1/span_16]";

  return (
    <div
      className="relative grid grid-cols-[repeat(24,minmax(0,1fr))] gap-x-[0.625rem] rounded-[0.25rem] bg-[var(--l-surface)] p-[1.09375rem]"
      style={{ height: cardH }}
    >
      <Tag
        {...linkProps}
        className={cn("group/card row-start-1 flex flex-col justify-center", textCol)}
      >
        <FeatureText {...feature} />
      </Tag>
      <div
        className={cn("relative row-start-1 overflow-hidden rounded-[0.25rem]", imageCol)}
        style={{ height: mediaH }}
      >
        <Media />
      </div>
      <div className="pointer-events-none absolute inset-0 rounded-[0.25rem] border border-[var(--l-border-subtle)]" />
    </div>
  );
}

function MobileCard({ feature }: { feature: Feature }) {
  const { href, Media } = feature;
  const linkProps = href ? { href, target: "_blank", rel: "noopener noreferrer" as const } : {};
  const Tag = href ? "a" : "div";

  return (
    <div className="relative flex flex-col overflow-hidden rounded-[0.25rem] bg-[var(--l-surface)]">
      <Tag {...linkProps} className="group/card block p-[1.025rem] md:p-[1.5rem]">
        <FeatureText {...feature} />
      </Tag>
      <div className="relative h-[32rem] shrink-0 overflow-hidden sm:h-[36rem] md:h-[40rem]">
        <Media />
      </div>
      <div className="pointer-events-none absolute inset-0 rounded-[0.25rem] border border-[var(--l-border-subtle)]" />
    </div>
  );
}

export function FeaturesSection() {
  return (
    <section className="l-section bg-[var(--l-bg)]">
      <div className="l-container hidden flex-col min-[900px]:flex" style={{ gap: "5.6rem" }}>
        {FEATURES.map((f) => (
          <DesktopCard feature={f} key={f.title} />
        ))}
      </div>
      <div className="l-container flex flex-col min-[900px]:hidden" style={{ gap: "5.25rem" }}>
        {FEATURES.map((f) => (
          <MobileCard feature={f} key={f.title} />
        ))}
      </div>
    </section>
  );
}
