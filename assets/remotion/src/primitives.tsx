import type { CSSProperties, ReactNode } from "react";

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

export const Logo: React.FC = () => (
  <span className="flex items-center gap-1">
    <svg
      aria-hidden="true"
      fill="none"
      height="22"
      viewBox="0 0 256 256"
      width="22"
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
      style={{ fontSize: "1rem", lineHeight: "1", letterSpacing: "-0.015em" }}
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
  <span className={`group inline-flex items-center gap-1.5 ${className ?? ""}`}>
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
    <div className="flex h-7.5 shrink-0 items-center bg-fd-card" role="tablist">
      {tabs.map((tab, i) => {
        const active = i === activeIndex;
        return (
          <div
            aria-selected={active}
            className={`group/tab relative flex h-full shrink-0 items-center gap-1.5 border-fd-border border-r ps-3 pe-1.5 ${
              active
                ? "bg-fd-background pb-px text-fd-foreground after:absolute after:inset-x-0 after:bottom-0 after:h-px after:bg-fd-background"
                : "border-b bg-fd-card text-fd-muted-foreground"
            }`}
            key={tab.name}
            role="tab"
          >
            <span className="truncate text-xs">{tab.name}</span>
            <span
              aria-hidden="true"
              className={`inline-flex items-center justify-center p-0.5 text-fd-muted-foreground ${
                active ? "opacity-100" : "opacity-0"
              }`}
            >
              <svg
                fill="none"
                height={12}
                stroke="currentColor"
                strokeLinecap="round"
                strokeWidth={1.75}
                viewBox="0 0 24 24"
                width={12}
              >
                <path d="M18 6 6 18M6 6l12 12" />
              </svg>
            </span>
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

export type FeatureTextProps = {
  eyebrow: string;
  title: string;
  description: ReactNode;
  ctaText: string;
  ctaVariant?: "link" | "snippet";
  titleOpacity?: number;
  titleY?: number;
  descOpacity?: number;
  ctaOpacity?: number;
};

export const FeatureText: React.FC<FeatureTextProps> = ({
  eyebrow,
  title,
  description,
  ctaText,
  ctaVariant = "link",
  titleOpacity = 1,
  titleY = 0,
  descOpacity = 1,
  ctaOpacity = 1,
}) => (
  <div className="flex flex-col gap-3.75">
    <div className="flex flex-col gap-4" style={{ opacity: titleOpacity }}>
      <div
        className="font-mono text-fd-muted-foreground text-xs uppercase"
        style={{ letterSpacing: "0.15em" }}
      >
        — {eyebrow}
      </div>
      <h3
        className="font-normal text-fd-foreground text-xl leading-7"
        style={{ transform: `translateY(${titleY}px)` }}
      >
        {title}
      </h3>
    </div>
    <p
      className="font-normal text-fd-muted-foreground text-base leading-6"
      style={{ opacity: descOpacity }}
    >
      {description}
    </p>
    {ctaVariant === "link" ? (
      <span
        className="inline-flex items-center gap-0.5 text-base leading-6"
        style={{ color: "var(--l-accent)", opacity: ctaOpacity }}
      >
        {ctaText}
        <ArrowRightIcon size={16} />
      </span>
    ) : (
      <Snippet text={ctaText} style={{ opacity: ctaOpacity }} />
    )}
  </div>
);

export const Snippet: React.FC<{ text: string; style?: CSSProperties }> = ({
  text,
  style,
}) => (
  <div
    className="flex items-center gap-3 border border-fd-border bg-fd-background px-3 py-2.5"
    style={{ borderRadius: 1, ...style }}
  >
    <div className="min-w-0 flex-1">
      <div
        className="truncate font-mono text-[13px] text-fd-foreground"
        style={{ fontVariantLigatures: "none" }}
      >
        <span style={{ opacity: 0.8 }}>$ </span>
        {text}
      </div>
    </div>
    <button
      aria-label="Copy to clipboard"
      className="relative inline-flex shrink-0 items-center justify-center border border-fd-foreground bg-fd-foreground text-fd-background"
      style={{ width: "1.75rem", height: "1.75rem", borderRadius: 0 }}
      type="button"
    >
      <svg
        aria-hidden="true"
        fill="currentColor"
        height={14}
        viewBox="0 0 16 16"
        width={14}
      >
        <path
          clipRule="evenodd"
          d="M2.75 0.5C1.7835 0.5 1 1.2835 1 2.25V9.75C1 10.7165 1.7835 11.5 2.75 11.5H3.75H4.5V10H3.75H2.75C2.61193 10 2.5 9.88807 2.5 9.75V2.25C2.5 2.11193 2.61193 2 2.75 2H8.25C8.38807 2 8.5 2.11193 8.5 2.25V3H10V2.25C10 1.2835 9.2165 0.5 8.25 0.5H2.75ZM7.75 4.5C6.7835 4.5 6 5.2835 6 6.25V13.75C6 14.7165 6.7835 15.5 7.75 15.5H13.25C14.2165 15.5 15 14.7165 15 13.75V6.25C15 5.2835 14.2165 4.5 13.25 4.5H7.75ZM7.5 6.25C7.5 6.11193 7.61193 6 7.75 6H13.25C13.3881 6 13.5 6.11193 13.5 6.25V13.75C13.5 13.8881 13.3881 14 13.25 14H7.75C7.61193 14 7.5 13.8881 7.5 13.75V6.25Z"
          fillRule="evenodd"
        />
      </svg>
    </button>
  </div>
);

type SchemaField = {
  name: string;
  type: string;
  description: string;
  children?: SchemaField[];
  open?: boolean;
};

export const GENE_FIELDS: SchemaField[] = [
  { name: "id", type: "String", description: "Node identifier (CURIE)" },
  { name: "label", type: "String", description: "Node type abbreviation" },
  {
    name: "properties",
    type: "Struct",
    description: "Gene-specific properties",
    open: true,
    children: [
      { name: "symbol", type: "String", description: "HGNC gene symbol" },
      { name: "name", type: "String", description: "Full gene name" },
      { name: "biotype", type: "String", description: "Gene biotype" },
      {
        name: "genomic_location",
        type: "Struct",
        description: "Chromosomal coordinates",
        open: true,
        children: [
          { name: "chromosome", type: "String", description: "Chromosome" },
          { name: "start", type: "Int64", description: "Start position" },
          { name: "end", type: "Int64", description: "End position" },
          { name: "strand", type: "Int32", description: "Strand direction" },
        ],
      },
      {
        name: "transcription_start_site",
        type: "Int64",
        description: "TSS position",
      },
      {
        name: "transcript_ids",
        type: "List[String]",
        description: "Ensembl transcript IDs",
      },
      {
        name: "function_descriptions",
        type: "List[String]",
        description: "Functional descriptions",
      },
    ],
  },
];

export const SchemaTreeStatic: React.FC<{ fields: SchemaField[] }> = ({
  fields,
}) => (
  <div className="h-full w-full overflow-hidden px-2 py-1.5">
    <div className="font-sans text-[13px] leading-6">
      {fields.map((field, i) => (
        <SchemaRow depth={0} field={field} key={`${field.name}-${i}`} />
      ))}
    </div>
  </div>
);

const SchemaRow: React.FC<{ field: SchemaField; depth: number }> = ({
  field,
  depth,
}) => (
  <>
    <div
      className="group flex items-center gap-1.5 py-0.5"
      style={{ paddingLeft: `${depth * 16 + 6}px` }}
    >
      <span
        aria-hidden="true"
        className="inline-flex h-3 w-3 items-center justify-center text-fd-muted-foreground"
      >
        {field.children ? (
          <ChevronRightIcon open={!!field.open} size={10} />
        ) : (
          <span className="block h-0.5 w-2 bg-fd-muted-foreground/40" />
        )}
      </span>
      <span className="font-medium font-mono text-[12.5px] text-fd-foreground">
        {field.name}
      </span>
      <span className="font-mono text-[11px] text-fd-muted-foreground">
        {field.type}
      </span>
      <span className="truncate text-fd-muted-foreground text-xs">
        {field.description}
      </span>
    </div>
    {field.children && field.open
      ? field.children.map((c, i) => (
          <SchemaRow depth={depth + 1} field={c} key={`${c.name}-${i}`} />
        ))
      : null}
  </>
);

export const ValidationsSidebarStatic: React.FC<{ activeId: string }> = ({
  activeId,
}) => {
  const items = [
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
  return (
    <aside className="flex w-48 shrink-0 flex-col border-fd-border border-r bg-fd-card">
      <div className="min-h-0 flex-1 overflow-y-auto py-1.5">
        <div className="flex items-center gap-1 px-2 py-1 font-semibold text-[11px] text-fd-muted-foreground uppercase tracking-[0.04em]">
          <ChevronRightIcon open size={10} />
          <span>validations</span>
        </div>
        <ul>
          {items.map((item) => (
            <li
              className={`flex items-center gap-2 py-0.5 pe-2 ps-7 text-sm ${
                item.id === activeId
                  ? "bg-fd-accent text-fd-accent-foreground"
                  : "text-fd-foreground"
              }`}
              key={item.id}
            >
              <ChartColumnIcon size={13} />
              <span className="truncate">{item.label}</span>
            </li>
          ))}
        </ul>
      </div>
    </aside>
  );
};
