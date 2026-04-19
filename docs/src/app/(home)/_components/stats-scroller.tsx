import type { CSSProperties } from "react";

const STATS: ReadonlyArray<{ value: string; label: string }> = [
  { value: "65", label: "sources" },
  { value: "18", label: "ontologies" },
  { value: "10", label: "entity types" },
  { value: "190,531", label: "nodes" },
  { value: "26", label: "relation types" },
  { value: "21,813,816", label: "edges" },
  { value: "150", label: "property keys" },
  { value: "67,249,863", label: "property instances" },
  { value: "110,276,843", label: "property values" },
];

const COPIES = ["primary", "mirror"] as const;

const MARQUEE_CSS = `
.stats-marquee-fade {
  -webkit-mask-image: linear-gradient(to right, transparent, black 5%, black 95%, transparent);
  mask-image: linear-gradient(to right, transparent, black 5%, black 95%, transparent);
}
.stats-marquee-track {
  animation: stats-marquee var(--stats-marquee-duration, 40s) linear infinite;
}
@keyframes stats-marquee {
  from { transform: translateX(0); }
  to { transform: translateX(calc(-100% - var(--stats-marquee-gap, 2rem))); }
}
@media (prefers-reduced-motion: reduce) {
  .stats-marquee-track { animation: none; }
}
`;

export function StatsScroller() {
  const style = {
    ["--stats-marquee-gap" as string]: "3rem",
    ["--stats-marquee-duration" as string]: "60s",
  } as CSSProperties;

  return (
    <section
      aria-label="OptimusKG statistics"
      className="l-section l-section--flush-bottom bg-[var(--l-bg)]"
      style={style}
    >
      <style href="stats-scroller" precedence="medium">
        {MARQUEE_CSS}
      </style>
      <div className="l-container overflow-hidden">
        <div className="stats-marquee-fade flex gap-[var(--stats-marquee-gap)]">
          {COPIES.map((kind) => (
            <ul
              aria-hidden={kind !== "primary"}
              className="stats-marquee-track flex shrink-0 items-center gap-[var(--stats-marquee-gap)]"
              key={kind}
            >
              {STATS.map(({ value, label }) => (
                <li
                  className="flex items-baseline gap-2 whitespace-nowrap"
                  key={label}
                >
                  <span className="font-mono text-[var(--l-ink)] text-lg tabular-nums">
                    {value}
                  </span>
                  <span className="text-[var(--l-ink-muted)] text-sm">
                    {label}
                  </span>
                </li>
              ))}
            </ul>
          ))}
        </div>
      </div>
    </section>
  );
}
