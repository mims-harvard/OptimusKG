import type { CSSProperties, ReactNode } from "react";
import { interpolate, spring, useCurrentFrame, useVideoConfig } from "remotion";

export const EASE_OUT = [0.16, 1, 0.3, 1] as const;

export const ArrowRightIcon: React.FC<{ size?: number }> = ({ size = 16 }) => (
  <svg
    aria-hidden="true"
    fill="none"
    height={size}
    stroke="currentColor"
    strokeLinecap="round"
    strokeLinejoin="round"
    strokeWidth={2}
    viewBox="0 0 24 24"
    width={size}
  >
    <path d="M5 12h14" />
    <path d="m12 5 7 7-7 7" />
  </svg>
);

export const ChevronRightIcon: React.FC<{ size?: number; open?: boolean }> = ({
  size = 12,
  open = false,
}) => (
  <svg
    aria-hidden="true"
    fill="none"
    height={size}
    stroke="currentColor"
    strokeLinecap="round"
    strokeLinejoin="round"
    strokeWidth={1.75}
    style={{ transform: open ? "rotate(90deg)" : "none" }}
    viewBox="0 0 24 24"
    width={size}
  >
    <path d="m9 18 6-6-6-6" />
  </svg>
);

export const ChartColumnIcon: React.FC<{ size?: number }> = ({ size = 14 }) => (
  <svg
    aria-hidden="true"
    fill="none"
    height={size}
    stroke="currentColor"
    strokeLinecap="round"
    strokeLinejoin="round"
    strokeWidth={1.75}
    viewBox="0 0 24 24"
    width={size}
  >
    <path d="M3 3v18h18" />
    <path d="M18 17V9" />
    <path d="M13 17V5" />
    <path d="M8 17v-3" />
  </svg>
);

export type ValidationEntity = { id: string; label: string };

export const VALIDATION_ENTITIES: ValidationEntity[] = [
  { id: "anatomy", label: "Anatomy" },
  { id: "biological-process", label: "Biological Process" },
  { id: "cellular-component", label: "Cellular Component" },
  { id: "disease", label: "Disease" },
  { id: "drug", label: "Drug" },
  { id: "exposure", label: "Exposure" },
  { id: "gene", label: "Gene" },
  { id: "molecular-function", label: "Molecular Function" },
  { id: "pathway", label: "Pathway" },
  { id: "phenotype", label: "Phenotype" },
];

export const ValidationsSidebar: React.FC<{
  items: ValidationEntity[];
  activeId: string;
}> = ({ items, activeId }) => (
  <aside
    className="flex shrink-0 flex-col border-fd-border border-r bg-fd-card"
    style={{ width: "17rem" }}
  >
    <div className="min-h-0 flex-1 overflow-y-auto py-2">
      <div
        className="flex items-center gap-1 px-3 py-1.5 font-semibold text-fd-muted-foreground uppercase"
        style={{ fontSize: 13, letterSpacing: "0.04em" }}
      >
        <ChevronRightIcon open size={10} />
        <span>validations</span>
      </div>
      <ul>
        {items.map((item) => {
          const active = item.id === activeId;
          return (
            <li
              className={`flex items-center gap-2 py-1 pe-2 ps-8 ${
                active
                  ? "bg-fd-accent text-fd-accent-foreground"
                  : "text-fd-foreground"
              }`}
              key={item.id}
              style={{ fontSize: 17 }}
            >
              <ChartColumnIcon size={16} />
              <span className="truncate">{item.label}</span>
            </li>
          );
        })}
      </ul>
    </div>
  </aside>
);

export const Logo: React.FC<{ size?: number }> = ({ size = 22 }) => (
  <span className="flex items-center gap-1">
    <svg
      aria-hidden="true"
      fill="none"
      height={size}
      viewBox="0 0 256 256"
      width={size}
      xmlns="http://www.w3.org/2000/svg"
    >
      <g
        stroke="currentColor"
        strokeLinecap="round"
        strokeLinejoin="round"
        strokeWidth="16"
      >
        <circle cx="128" cy="128" r="24" />
        <circle cx="96" cy="56" r="24" />
        <circle cx="200" cy="104" r="24" />
        <circle cx="200" cy="184" r="24" />
        <circle cx="56" cy="192" r="24" />
        <line x1="118.25" x2="105.75" y1="106.07" y2="77.93" />
        <line x1="177.23" x2="150.77" y1="111.59" y2="120.41" />
        <line x1="181.06" x2="146.94" y1="169.27" y2="142.73" />
        <line x1="110.06" x2="73.94" y1="143.94" y2="176.06" />
      </g>
    </svg>
    <span
      className="whitespace-nowrap font-normal"
      style={{ fontSize: size * 0.8, lineHeight: "1", letterSpacing: "-0.015em" }}
    >
      OptimusKG
    </span>
  </span>
);

export const DownloadButton: React.FC<{
  className?: string;
  showIcon?: boolean;
  label?: string;
}> = ({ className, showIcon = true, label = "Download" }) => (
  <span className={`inline-flex items-center gap-1.5 ${className ?? ""}`}>
    {label}
    {showIcon && (
      <svg
        aria-hidden="true"
        fill="none"
        height={14}
        stroke="currentColor"
        strokeLinecap="round"
        strokeLinejoin="round"
        strokeWidth={1.75}
        viewBox="0 0 24 24"
        width={14}
      >
        <path d="M12 17V3" />
        <path d="m6 11 6 6 6-6" />
        <path d="M19 21H5" />
      </svg>
    )}
  </span>
);

const WinControl: React.FC<{ color: string }> = ({ color }) => (
  <div
    style={{
      width: "0.625rem",
      height: "0.625rem",
      borderRadius: 999,
      background: color,
      opacity: 0.55,
    }}
  />
);

export const EditorWindow: React.FC<{
  title?: string;
  style?: CSSProperties;
  className?: string;
  children: ReactNode;
}> = ({ title, style, className, children }) => (
  <div
    className={`relative flex flex-col overflow-hidden bg-fd-card ${className ?? ""}`}
    style={{
      borderRadius: "0.625rem",
      boxShadow:
        "0px 28px 70px 0px rgba(0,0,0,0.14), 0px 14px 32px 0px rgba(0,0,0,0.1), 0px 0px 0px 1px rgba(38,37,30,0.1)",
      ...style,
    }}
  >
    <div className="relative flex h-7 shrink-0 items-center border-fd-border border-b bg-fd-card px-2">
      <div className="flex gap-1.5">
        <WinControl color="var(--color-fd-muted-foreground)" />
        <WinControl color="var(--color-fd-muted-foreground)" />
        <WinControl color="var(--color-fd-muted-foreground)" />
      </div>
      {title && (
        <span className="-translate-x-1/2 absolute left-1/2 whitespace-nowrap text-fd-muted-foreground text-xs">
          {title}
        </span>
      )}
    </div>
    {children}
  </div>
);

export type StaticTab = { name: string };

export const TabbedEditorShell: React.FC<{
  tabs: StaticTab[];
  activeIndex?: number;
  children: ReactNode;
}> = ({ tabs, activeIndex = 0, children }) => (
  <div className="flex h-full w-full flex-col">
    <div className="flex h-10 shrink-0 items-center bg-fd-card" role="tablist">
      {tabs.map((tab, i) => {
        const active = i === activeIndex;
        return (
          <div
            aria-selected={active}
            className={`relative flex h-full shrink-0 items-center gap-1.5 border-fd-border border-r ps-3 pe-3 ${
              active
                ? "bg-fd-background pb-px text-fd-foreground after:absolute after:inset-x-0 after:bottom-0 after:h-px after:bg-fd-background"
                : "border-b bg-fd-card text-fd-muted-foreground"
            }`}
            key={tab.name}
            role="tab"
          >
            <span className="truncate text-base">{tab.name}</span>
          </div>
        );
      })}
      <div className="h-full flex-1 border-fd-border border-b" />
    </div>
    <div className="min-h-0 flex-1 overflow-hidden bg-fd-background">
      {children}
    </div>
  </div>
);

export const Snippet: React.FC<{ text: string; style?: CSSProperties; size?: "sm" | "lg" }> = ({
  text,
  style,
  size = "sm",
}) => {
  const padding = size === "lg" ? "0.875rem 1.25rem" : "0.625rem 0.75rem";
  const fontSize = size === "lg" ? 22 : 13;
  const copySize = size === "lg" ? "2.25rem" : "1.75rem";
  return (
    <div
      className="inline-flex items-center gap-3 border border-fd-border bg-fd-background"
      style={{ borderRadius: 1, padding, ...style }}
    >
      <div
        className="font-mono text-fd-foreground"
        style={{ fontSize, fontVariantLigatures: "none" }}
      >
        <span style={{ opacity: 0.7 }}>$ </span>
        {text}
      </div>
      <span
        aria-label="Copy to clipboard"
        className="relative inline-flex shrink-0 items-center justify-center border border-fd-foreground bg-fd-foreground text-fd-background"
        style={{ width: copySize, height: copySize, borderRadius: 0 }}
      >
        <svg
          aria-hidden="true"
          fill="currentColor"
          height={size === "lg" ? 18 : 14}
          viewBox="0 0 16 16"
          width={size === "lg" ? 18 : 14}
        >
          <path
            clipRule="evenodd"
            d="M2.75 0.5C1.7835 0.5 1 1.2835 1 2.25V9.75C1 10.7165 1.7835 11.5 2.75 11.5H3.75H4.5V10H3.75H2.75C2.61193 10 2.5 9.88807 2.5 9.75V2.25C2.5 2.11193 2.61193 2 2.75 2H8.25C8.38807 2 8.5 2.11193 8.5 2.25V3H10V2.25C10 1.2835 9.2165 0.5 8.25 0.5H2.75ZM7.75 4.5C6.7835 4.5 6 5.2835 6 6.25V13.75C6 14.7165 6.7835 15.5 7.75 15.5H13.25C14.2165 15.5 15 14.7165 15 13.75V6.25C15 5.2835 14.2165 4.5 13.25 4.5H7.75ZM7.5 6.25C7.5 6.11193 7.61193 6 7.75 6H13.25C13.3881 6 13.5 6.11193 13.5 6.25V13.75C13.5 13.8881 13.3881 14 13.25 14H7.75C7.61193 14 7.5 13.8881 7.5 13.75V6.25Z"
            fillRule="evenodd"
          />
        </svg>
      </span>
    </div>
  );
};

export const WordHighlight: React.FC<{
  children: ReactNode;
  enter: number;
  color?: string;
}> = ({ children, enter, color = "var(--l-accent)" }) => {
  const frame = useCurrentFrame();
  const { fps } = useVideoConfig();
  const progress = spring({
    fps,
    frame: frame - enter,
    config: { damping: 200 },
    durationInFrames: 18,
  });
  const scaleX = Math.max(0, Math.min(1, progress));
  return (
    <span
      className="relative inline-block"
      style={{ whiteSpace: "nowrap", color: "var(--color-fd-background)" }}
    >
      <span
        aria-hidden
        style={{
          position: "absolute",
          left: "-0.1em",
          right: "-0.1em",
          top: "50%",
          height: "1.05em",
          transform: `translateY(-50%) scaleX(${scaleX})`,
          transformOrigin: "left center",
          backgroundColor: color,
          borderRadius: "0.08em",
          zIndex: 0,
        }}
      />
      <span style={{ position: "relative", zIndex: 1 }}>{children}</span>
    </span>
  );
};

export type RevealToken = string | ReactNode;

export const RevealLine: React.FC<{
  tokens: RevealToken[];
  startFrame: number;
  perWordFrames?: number;
  lift?: number;
  className?: string;
  style?: CSSProperties;
}> = ({ tokens, startFrame, perWordFrames = 12, lift = 18, className, style }) => {
  const frame = useCurrentFrame();
  return (
    <div className={className} style={style}>
      {tokens.map((tok, i) => {
        const delay = startFrame + i * perWordFrames;
        const opacity = interpolate(frame, [delay, delay + 44], [0, 1], {
          extrapolateLeft: "clamp",
          extrapolateRight: "clamp",
          easing: (t) => cubicBezier(t, EASE_OUT),
        });
        const y = interpolate(frame, [delay, delay + 56], [lift, 0], {
          extrapolateLeft: "clamp",
          extrapolateRight: "clamp",
          easing: (t) => cubicBezier(t, EASE_OUT),
        });
        return (
          <span
            key={i}
            style={{
              display: "inline-block",
              opacity,
              transform: `translateY(${y}px)`,
              whiteSpace: "pre",
            }}
          >
            {tok}
            {i < tokens.length - 1 ? " " : ""}
          </span>
        );
      })}
    </div>
  );
};

function cubicBezier(t: number, curve: readonly [number, number, number, number]) {
  const [, y1, , y2] = curve;
  const u = 1 - t;
  return 3 * u * u * t * y1 + 3 * u * t * t * y2 + t * t * t;
}

export const StatBig: React.FC<{
  value: string;
  label: string;
  startFrame: number;
  className?: string;
}> = ({ value, label, startFrame, className }) => {
  const frame = useCurrentFrame();
  const opacity = interpolate(frame, [startFrame, startFrame + 20], [0, 1], {
    extrapolateLeft: "clamp",
    extrapolateRight: "clamp",
  });
  const y = interpolate(frame, [startFrame, startFrame + 28], [24, 0], {
    extrapolateLeft: "clamp",
    extrapolateRight: "clamp",
    easing: (t) => cubicBezier(t, EASE_OUT),
  });

  const numeric = Number(value.replace(/[^0-9]/g, ""));
  const ramp = interpolate(frame, [startFrame, startFrame + 26], [0, 1], {
    extrapolateLeft: "clamp",
    extrapolateRight: "clamp",
    easing: (t) => cubicBezier(t, EASE_OUT),
  });
  const display = Number.isFinite(numeric) && numeric > 0
    ? Math.round(numeric * ramp).toLocaleString("en-US")
    : value;

  return (
    <div
      className={`flex items-baseline gap-4 ${className ?? ""}`}
      style={{ opacity, transform: `translateY(${y}px)` }}
    >
      <span
        className="font-mono text-fd-foreground tabular-nums"
        style={{
          fontSize: "8rem",
          lineHeight: 1,
          letterSpacing: "-0.04em",
          fontVariantNumeric: "tabular-nums",
          fontWeight: 700,
        }}
      >
        {display}
      </span>
      <span
        className="text-fd-muted-foreground"
        style={{ fontSize: "1.75rem" }}
      >
        {label}
      </span>
    </div>
  );
};

type SchemaField = {
  name: string;
  type: string;
  description: string;
  children?: SchemaField[];
};

const nodeSourcesField: SchemaField = {
  name: "sources",
  type: "Struct",
  description: "Provenance of this node",
  children: [
    {
      name: "direct",
      type: "List[String]",
      description: "Datasets that directly contributed this entity",
    },
    {
      name: "indirect",
      type: "List[String]",
      description: "Datasets that referenced this entity",
    },
  ],
};

// Mirrors docs/graph-schema/gene — the full Gene-node schema.
export const GENE_FIELDS: SchemaField[] = [
  {
    name: "id",
    type: "String",
    description: "Node identifier in CURIE format (e.g. ENSG00000141510)",
  },
  {
    name: "label",
    type: "String",
    description: "Node type abbreviation (GEN)",
  },
  {
    name: "properties",
    type: "Struct",
    description: "Gene-specific properties",
    children: [
      {
        name: "symbol",
        type: "String",
        description: "Official HGNC gene symbol (e.g. TP53)",
      },
      { name: "name", type: "String", description: "Full gene name" },
      {
        name: "biotype",
        type: "String",
        description: "Gene biotype (e.g. protein_coding, lncRNA)",
      },
      {
        name: "genomic_location",
        type: "Struct",
        description: "Chromosomal coordinates",
        children: [
          { name: "chromosome", type: "String", description: "Chromosome name" },
          { name: "start", type: "Int64", description: "Start position (0-based)" },
          { name: "end", type: "Int64", description: "End position" },
          { name: "strand", type: "Int32", description: "Strand (+1 forward, -1 reverse)" },
        ],
      },
      {
        name: "transcription_start_site",
        type: "Int64",
        description: "Transcription start site position",
      },
      {
        name: "canonical_transcript",
        type: "Struct",
        description: "Canonical transcript details",
        children: [
          { name: "id", type: "String", description: "Ensembl transcript ID" },
          { name: "chromosome", type: "String", description: "Chromosome name" },
          { name: "start", type: "Int64", description: "Start position" },
          { name: "end", type: "Int64", description: "End position" },
          { name: "strand", type: "String", description: "Strand" },
        ],
      },
      { name: "canonical_exons", type: "List[String]", description: "Canonical exon coordinates" },
      { name: "transcript_ids", type: "List[String]", description: "All associated Ensembl transcript IDs" },
      { name: "alternative_genes", type: "List[String]", description: "Alternative gene entries at the same locus" },
      { name: "function_descriptions", type: "List[String]", description: "Functional descriptions" },
      {
        name: "synonyms",
        type: "List[Struct]",
        description: "General gene synonyms",
        children: [
          { name: "label", type: "String", description: "Synonym label" },
          { name: "source", type: "String", description: "Source database" },
        ],
      },
      {
        name: "symbol_synonyms",
        type: "List[Struct]",
        description: "Alternative gene symbols",
        children: [
          { name: "label", type: "String", description: "Symbol label" },
          { name: "source", type: "String", description: "Source database" },
        ],
      },
      {
        name: "name_synonyms",
        type: "List[Struct]",
        description: "Alternative gene names",
        children: [
          { name: "label", type: "String", description: "Name label" },
          { name: "source", type: "String", description: "Source database" },
        ],
      },
      {
        name: "obsolete_symbols",
        type: "List[Struct]",
        description: "Deprecated gene symbols",
        children: [
          { name: "label", type: "String", description: "Symbol label" },
          { name: "source", type: "String", description: "Source database" },
        ],
      },
      {
        name: "obsolete_names",
        type: "List[Struct]",
        description: "Deprecated gene names",
        children: [
          { name: "label", type: "String", description: "Name label" },
          { name: "source", type: "String", description: "Source database" },
        ],
      },
      {
        name: "subcellular_locations",
        type: "List[Struct]",
        description: "Subcellular localization annotations",
        children: [
          { name: "location", type: "String", description: "Location name" },
          { name: "source", type: "String", description: "Source database" },
          { name: "term_sl", type: "String", description: "Subcellular location ontology term" },
          { name: "label_sl", type: "String", description: "Subcellular location label" },
        ],
      },
      {
        name: "target_class",
        type: "List[Struct]",
        description: "Drug target class classification",
        children: [
          { name: "id", type: "Int64", description: "Target class ID" },
          { name: "label", type: "String", description: "Target class label" },
          { name: "level", type: "String", description: "Hierarchy level" },
        ],
      },
      {
        name: "target_enabling_package",
        type: "Struct",
        description: "Target enabling package annotation",
        children: [
          { name: "target_from_source_id", type: "String", description: "Source target ID" },
          { name: "description", type: "String", description: "Package description" },
          { name: "therapeutic_area", type: "String", description: "Therapeutic area" },
          { name: "url", type: "String", description: "Reference URL" },
        ],
      },
      {
        name: "tractability",
        type: "List[Struct]",
        description: "Drug tractability assessments per modality",
        children: [
          { name: "modality", type: "String", description: "Drug modality (e.g. sm, ab, pr)" },
          { name: "id", type: "String", description: "Tractability category ID" },
          { name: "value", type: "Boolean", description: "Tractability assessment value" },
        ],
      },
      {
        name: "constraint_scores",
        type: "List[Struct]",
        description: "Evolutionary constraint scores (e.g. pLI, LOEUF)",
        children: [
          { name: "constraint_type", type: "String", description: "Score type (e.g. lof, mis)" },
          { name: "score", type: "Float32", description: "Constraint score" },
          { name: "exp", type: "Float32", description: "Expected variant count" },
          { name: "obs", type: "Int32", description: "Observed variant count" },
          { name: "oe", type: "Float32", description: "Observed/expected ratio" },
          { name: "oe_lower", type: "Float32", description: "O/E 90% CI lower bound" },
          { name: "oe_upper", type: "Float32", description: "O/E 90% CI upper bound" },
          { name: "upper_rank", type: "Int32", description: "Upper rank (gnomAD)" },
          { name: "upper_bin", type: "Int32", description: "Upper bin (10-bin)" },
          { name: "upper_bin6", type: "Int32", description: "Upper bin (6-bin)" },
        ],
      },
      {
        name: "hallmarks_attributes",
        type: "List[Struct]",
        description: "Cancer hallmark attributes (Cancer Gene Census)",
        children: [
          { name: "pmid", type: "Int64", description: "PubMed ID of supporting reference" },
          { name: "description", type: "String", description: "Hallmark description" },
          { name: "attribute_name", type: "String", description: "Attribute name" },
        ],
      },
      {
        name: "cancer_hallmarks",
        type: "List[Struct]",
        description: "Associated cancer hallmarks",
        children: [
          { name: "pmid", type: "Int64", description: "PubMed ID of supporting reference" },
          { name: "description", type: "String", description: "Hallmark description" },
          { name: "impact", type: "String", description: "Functional impact (promotes/suppresses)" },
          { name: "label", type: "String", description: "Hallmark label" },
        ],
      },
      {
        name: "associated_proteins",
        type: "List[Struct]",
        description: "Associated UniProt protein entries",
        children: [
          { name: "id", type: "String", description: "UniProt accession" },
          { name: "source", type: "String", description: "Source database" },
        ],
      },
      {
        name: "xrefs",
        type: "List[Struct]",
        description: "Cross-references to external databases",
        children: [
          { name: "id", type: "String", description: "External identifier" },
          { name: "source", type: "String", description: "Database name" },
        ],
      },
      {
        name: "chemical_probes",
        type: "List[Struct]",
        description: "Chemical probe annotations (Probes & Drugs)",
      },
      {
        name: "homologues",
        type: "List[Struct]",
        description: "Ortholog and paralog information",
        children: [
          { name: "species_id", type: "String", description: "NCBI taxonomy ID" },
          { name: "species_name", type: "String", description: "Species name" },
          { name: "homology_type", type: "String", description: "Homology type (ortholog/paralog)" },
          { name: "target_gene_id", type: "String", description: "Target gene identifier" },
          { name: "is_high_confidence", type: "String", description: "High-confidence flag" },
          { name: "target_gene_symbol", type: "String", description: "Target gene symbol" },
          { name: "query_percentage_identity", type: "Float64", description: "Query % sequence identity" },
          { name: "target_percentage_identity", type: "Float64", description: "Target % sequence identity" },
          { name: "priority", type: "Int32", description: "Priority rank" },
        ],
      },
      {
        name: "safety_liabilities",
        type: "List[Struct]",
        description: "Safety liability annotations (OpenTargets)",
      },
      nodeSourcesField,
    ],
  },
];

export const SchemaTreeStatic: React.FC<{
  fields: SchemaField[];
  fontSize?: number;
}> = ({ fields, fontSize = 18 }) => (
  <div
    className="w-full"
    style={{
      fontFamily: "var(--font-sans)",
      fontSize,
      lineHeight: 1.55,
    }}
  >
    {fields.map((field, i) => (
      <SchemaRow depth={0} field={field} key={`${field.name}-${i}`} />
    ))}
  </div>
);

const SchemaRow: React.FC<{ field: SchemaField; depth: number }> = ({
  field,
  depth,
}) => (
  <>
    <div
      className="flex items-baseline gap-2.5 py-0.5"
      style={{ paddingLeft: `${depth * 22 + 8}px` }}
    >
      <span
        aria-hidden="true"
        className="inline-flex h-3 w-3 shrink-0 items-center justify-center text-fd-muted-foreground"
        style={{ alignSelf: "center" }}
      >
        {field.children ? (
          <ChevronRightIcon open size={12} />
        ) : (
          <span className="block h-0.5 w-2 bg-fd-muted-foreground/40" />
        )}
      </span>
      <span
        className="font-medium font-mono text-fd-foreground"
        style={{ fontSize: "0.95em" }}
      >
        {field.name}
      </span>
      <span
        className="font-mono text-fd-muted-foreground"
        style={{ fontSize: "0.78em" }}
      >
        {field.type}
      </span>
      <span
        className="text-fd-muted-foreground"
        style={{ fontSize: "0.85em" }}
      >
        {field.description}
      </span>
    </div>
    {field.children
      ? field.children.map((c, i) => (
          <SchemaRow depth={depth + 1} field={c} key={`${c.name}-${i}`} />
        ))
      : null}
  </>
);
