import { Fragment } from "react";

import Image from "next/image";

import { toJsxRuntime } from "hast-util-to-jsx-runtime";
import { ArrowRight } from "lucide-react";
import { jsx, jsxs } from "react/jsx-runtime";
import { type BundledLanguage, codeToHast } from "shiki";

import { disGenFields } from "@/components/disease-assoc-edge-schemas";
import { geneFields } from "@/components/gene-schema";
import { cn } from "@/lib/cn";

import { MaximizableWindow } from "./components/maximizable-window";
import { SchemaTreeView } from "./components/schema-tree-view";
import { Snippet } from "./components/snippet";
import { WindowTabbedEditor } from "./components/window-tabbed-editor";

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
    <div className="l-shiki-block h-full w-full overflow-auto p-2">
      {children}
    </div>
  );
}

function ImageTabContent({ src, alt }: { src: string; alt: string }) {
  return (
    <div className="relative flex h-full w-full items-center justify-center p-5">
      <Image
        alt={alt}
        className="object-contain p-5"
        fill
        sizes="(min-width: 900px) 680px, 100vw"
        src={src}
      />
    </div>
  );
}

function SchemaTabContent({ children }: { children: React.ReactNode }) {
  return <div className="h-full w-full overflow-auto p-2">{children}</div>;
}

function Feature1Media() {
  return (
    <div className="absolute inset-0 flex items-center justify-center overflow-hidden rounded-[1px]">
      <Image
        alt=""
        className="pointer-events-none scale-[1.1] object-cover"
        fill
        sizes="(min-width: 900px) 1200px, 100vw"
        src="/hero/lakeside-village.png"
      />
      <div
        className="pointer-events-none absolute inset-0"
        style={{
          background:
            "linear-gradient(90deg,rgba(0,0,0,0.12) 0%,rgba(0,0,0,0.22) 100%)",
        }}
      />

      <MaximizableWindow
        appIcon="/dock/editor.svg"
        appId="graph-schema"
        appName="Graph Schema"
        normalStyle={{
          width: "min(48rem, calc(100% - 4rem))",
          height: "min(36rem, calc(100% - 4rem))",
        }}
        title="Graph Schema"
      >
        <WindowTabbedEditor
          tabs={[
            {
              name: "Gene Nodes Schema",
              content: (
                <SchemaTabContent>
                  <SchemaTreeView fields={geneFields} />
                </SchemaTabContent>
              ),
            },
            {
              name: "Disease-Gene Edges Schema",
              content: (
                <SchemaTabContent>
                  <SchemaTreeView fields={disGenFields} />
                </SchemaTabContent>
              ),
            },
          ]}
        />
      </MaximizableWindow>

      <div className="pointer-events-none absolute inset-0 rounded-[1px] border border-[var(--l-border-subtle)]" />
    </div>
  );
}

function Feature3Media() {
  return (
    <div className="absolute inset-0 flex items-center justify-center overflow-hidden rounded-[1px]">
      <div className="pointer-events-none absolute inset-0 overflow-hidden">
        {/* biome-ignore lint/performance/noImgElement: intentionally overscaled panoramic background, next/image fill cannot reproduce the percentage stretch */}
        {/* biome-ignore lint/correctness/useImageSize: size is expressed as a percentage of the container, not intrinsic pixels */}
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
          background:
            "linear-gradient(90deg,rgba(38,37,30,0.05) 0%,rgba(38,37,30,0.05) 100%)",
        }}
      />

      <MaximizableWindow
        appIcon="/dock/editor.svg"
        appId="paperqa3"
        appName="PaperQA3 Analysis"
        normalStyle={{
          width: "min(42.5rem, calc(100% - 4rem))",
          height: "min(35rem, calc(100% - 4rem))",
        }}
        title="PaperQA3 Analysis"
      >
        {/* TODO: Remove `contentBg` once we have light/dark versions of the figures. */}
        <WindowTabbedEditor
          contentBg="#ffffff"
          tabs={[
            {
              name: "Molecular Function Validation",
              content: (
                <ImageTabContent
                  alt="Molecular Function"
                  src="/features/molecular-function.webp"
                />
              ),
            },
            {
              name: "Phenotype Validation",
              content: (
                <ImageTabContent
                  alt="Phenotype"
                  src="/features/phenotype.webp"
                />
              ),
            },
          ]}
        />
      </MaximizableWindow>

      <div className="pointer-events-none absolute inset-0 rounded-[1px] border border-[var(--l-border-subtle)]" />
    </div>
  );
}

async function Feature4Media() {
  const loadGraphJsx = await renderShiki(FEATURE1_SNIPPET, "python");
  return (
    <div className="absolute inset-0 flex items-center justify-center overflow-hidden rounded-[1px]">
      <div className="pointer-events-none absolute inset-0 overflow-hidden">
        {/* biome-ignore lint/performance/noImgElement: intentionally overscaled panoramic background, next/image fill cannot reproduce the percentage stretch */}
        {/* biome-ignore lint/correctness/useImageSize: size is expressed as a percentage of the container, not intrinsic pixels */}
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
          background:
            "linear-gradient(90deg,rgba(38,37,30,0.05) 0%,rgba(38,37,30,0.05) 100%)",
        }}
      />

      <MaximizableWindow
        appIcon="/dock/editor.svg"
        appId="python-client"
        appName="Python Client"
        normalStyle={{
          width: "min(42.5rem, calc(100% - 4rem))",
          height: "min(35rem, calc(100% - 4rem))",
        }}
        title="Python Client"
      >
        <WindowTabbedEditor
          tabs={[
            {
              name: "load_graph.py",
              content: <ShikiBlock>{loadGraphJsx}</ShikiBlock>,
            },
          ]}
        />
      </MaximizableWindow>

      <div className="pointer-events-none absolute inset-0 rounded-[1px] border border-[var(--l-border-subtle)]" />
    </div>
  );
}

const HEADING_CLASSES = "text-xl leading-7";
const DESCRIPTION_CLASSES = "text-md leading-6";
const CTA_CLASSES = "text-base leading-6";

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
    const ctaClass =
      "group/cta inline-flex items-center gap-0.5 text-[var(--l-accent)]";
    cta = ctaHref ? (
      <a
        className={`${ctaClass} ${CTA_CLASSES}`}
        href={ctaHref}
        rel="noopener noreferrer"
        target="_blank"
      >
        {ctaContent}
      </a>
    ) : (
      <span className={`${ctaClass} ${CTA_CLASSES}`}>{ctaContent}</span>
    );
  }
  return (
    <div className="flex flex-col gap-3.75">
      <div className="flex flex-col">
        <h3 className={`font-normal text-[var(--l-ink)] ${HEADING_CLASSES}`}>
          {title}
        </h3>
        <p
          className={`font-normal text-[var(--l-ink-muted)] ${DESCRIPTION_CLASSES}`}
        >
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
  cardHeightClass: string;
  mediaHeightClass: string;
};

const FEATURES: Feature[] = [
  {
    title: "Rich strongly-typed properties",
    description:
      "Every entity is enriched with structured properties for fine-grained analysis.",
    ctaText: "Learn about the schema",
    Media: Feature1Media,
    href: "/docs/graph-schema/nodes",
    imageSide: "right",
    cardHeightClass: "h-178.75",
    mediaHeightClass: "h-170",
  },
  {
    title: "Delightfully simple Python client",
    description:
      "Install with one command and load the graph as Polars data frames or a NetworkX graph in a single line.",
    ctaText: "uv add optimuskg",
    ctaVariant: "snippet",
    Media: Feature4Media,
    imageSide: "left",
    cardHeightClass: "h-171.25",
    mediaHeightClass: "h-162.5",
  },
  {
    title: "Rigorously validated",
    description:
      "Every edge is cross-validated against millions of research papers by PaperQA3, a deep research agent.",
    ctaText: "Learn about our methodology",
    Media: Feature3Media,
    href: "https://arxiv.org", // TODO: Update once we have the link
    imageSide: "right",
    cardHeightClass: "h-178.75",
    mediaHeightClass: "h-170",
  },
];

function DesktopCard({ feature }: { feature: Feature }) {
  const { href, imageSide, cardHeightClass, mediaHeightClass, Media } = feature;
  const linkProps = href
    ? { href, target: "_blank", rel: "noopener noreferrer" as const }
    : {};
  const Tag = href ? "a" : "div";
  const textCol =
    imageSide === "right"
      ? "col-[1/span_8] pl-0.5 pr-7.5"
      : "col-[17/span_8] pl-7.5 pr-0.5";
  const imageCol =
    imageSide === "right" ? "col-[9/span_16]" : "col-[1/span_16]";

  return (
    <div
      className={cn(
        "relative grid grid-cols-[repeat(24,minmax(0,1fr))] gap-x-2.5 rounded-[1px] bg-[var(--l-surface)] p-4.5",
        cardHeightClass
      )}
    >
      <Tag
        {...linkProps}
        className={cn(
          "group/card row-start-1 flex flex-col justify-center",
          textCol
        )}
      >
        <FeatureText {...feature} />
      </Tag>
      <div
        className={cn(
          "relative row-start-1 overflow-hidden rounded-[1px]",
          imageCol,
          mediaHeightClass
        )}
      >
        <Media />
      </div>
      <div className="pointer-events-none absolute inset-0 rounded-[1px] border border-[var(--l-border-subtle)]" />
    </div>
  );
}

function MobileCard({ feature }: { feature: Feature }) {
  const { href, Media } = feature;
  const linkProps = href
    ? { href, target: "_blank", rel: "noopener noreferrer" as const }
    : {};
  const Tag = href ? "a" : "div";

  return (
    <div className="relative flex flex-col overflow-hidden rounded-[1px] bg-[var(--l-surface)]">
      <Tag {...linkProps} className="group/card block p-4 md:p-6">
        <FeatureText {...feature} />
      </Tag>
      <div className="relative h-128 shrink-0 overflow-hidden sm:h-144 md:h-160">
        <Media />
      </div>
      <div className="pointer-events-none absolute inset-0 rounded-[1px] border border-[var(--l-border-subtle)]" />
    </div>
  );
}

export function FeaturesSection() {
  return (
    <section className="l-section bg-[var(--l-bg)]">
      <div className="l-container hidden flex-col gap-22.5 min-[900px]:flex">
        {FEATURES.map((f) => (
          <DesktopCard feature={f} key={f.title} />
        ))}
      </div>
      <div className="l-container flex flex-col gap-21 min-[900px]:hidden">
        {FEATURES.map((f) => (
          <MobileCard feature={f} key={f.title} />
        ))}
      </div>
    </section>
  );
}
