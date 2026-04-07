// ─── Card data ────────────────────────────────────────────────────────────────
const CARDS = [
  {
    title: "Use the best model for every task",
    description:
      "Choose between every cutting-edge model from OpenAI, Anthropic, Gemini, xAI, and Cursor.",
    ctaText: "Explore models ↗",
    ctaHref: "https://cursor.com/docs/models",
    ctaExternal: true,
    imageSrc: "/metagraph.svg",
    imageAlt: "Knowledge graph",
    // desktop media height (rem), mobile media height (rem)
    desktopMediaH: "24.481rem",
    mobileMediaH: "14.988rem",
  },
  {
    title: "Complete codebase understanding",
    description:
      "Cursor learns how your codebase works, no matter the scale or complexity.",
    ctaText: "Learn about codebase indexing ↗",
    ctaHref: "https://cursor.com/docs/context/codebase-indexing",
    ctaExternal: true,
    imageSrc: "/metapath.svg",
    imageAlt: "Metapath matrix",
    desktopMediaH: "24.481rem",
    mobileMediaH: "14.988rem",
  },
  {
    title: "Develop enduring software",
    description:
      "Trusted by over half of the Fortune 500 to accelerate development, securely and at scale.",
    ctaText: "Explore enterprise →",
    ctaHref: "https://cursor.com/en-US/enterprise",
    ctaExternal: false,
    imageSrc: "/lines.svg",
    imageAlt: "Distribution chart",
    desktopMediaH: "24.481rem",
    mobileMediaH: "19.981rem",
  },
];

// ─── Card media background (matches Figma: #f2f1ed + 5% dark overlay) ─────────
const MEDIA_BG =
  "linear-gradient(rgba(38,37,30,0.05),rgba(38,37,30,0.05)), linear-gradient(#f2f1ed,#f2f1ed)";

// ─── Single card ──────────────────────────────────────────────────────────────
function FrontierCard({
  card,
  mediaH,
}: {
  card: (typeof CARDS)[number];
  mediaH: string;
}) {
  return (
    <div
      className="relative bg-[#f2f1ed] rounded-[0.25rem] flex flex-col"
      style={{
        paddingTop: "0.994rem",
        paddingBottom: "1.094rem",
        paddingInline: "1.094rem",
      }}
    >
      {/* ── Text block ── */}
      <div className="flex flex-col">
        <div className="flex flex-col">
          <h3
            className="text-[#26251e] font-normal"
            style={{
              fontSize: "0.944rem",
              lineHeight: "1.5rem",
              letterSpacing: "0.005rem",
            }}
          >
            {card.title}
          </h3>
          <p
            className="text-[rgba(38,37,30,0.6)] font-normal"
            style={{
              fontSize: "0.944rem",
              lineHeight: "1.5rem",
              letterSpacing: "0.005rem",
            }}
          >
            {card.description}
          </p>
        </div>
        <div style={{ paddingTop: "0.901rem" }}>
          <a
            href={card.ctaHref}
            target={card.ctaExternal ? "_blank" : undefined}
            rel={card.ctaExternal ? "noopener noreferrer" : undefined}
            className="text-[#f54e00] font-normal"
            style={{
              fontSize: "0.944rem",
              lineHeight: "1.5rem",
              letterSpacing: "0.005rem",
            }}
          >
            {card.ctaText}
          </a>
        </div>
      </div>

      {/* ── Media area ── */}
      <div style={{ paddingTop: "1.094rem" }}>
        <div
          className="rounded-[0.25rem] overflow-hidden relative"
          style={{ height: mediaH, backgroundImage: MEDIA_BG }}
        >
          <img
            src={card.imageSrc}
            alt={card.imageAlt}
            className="absolute inset-0 w-full h-full object-cover"
          />
        </div>
      </div>

      {/* ── Inset border overlay ── */}
      <div className="absolute inset-0 rounded-[0.25rem] border border-[rgba(38,37,30,0.03)] pointer-events-none" />
    </div>
  );
}

// ─── Main export ──────────────────────────────────────────────────────────────
export function FrontierSection() {
  return (
    <section className="bg-[#f7f7f4]" style={{ paddingBlock: "4.2rem" }}>

      {/* ── Desktop (≥ lg): 3-column grid ── */}
      <div className="hidden lg:block px-[1.25rem] max-w-[81.25rem] mx-auto">
        <h2
          className="text-[#26251e] font-normal"
          style={{
            fontSize: "0.944rem",
            lineHeight: "1.8125rem",
            letterSpacing: "0.005rem",
            marginBottom: "1.4rem",
          }}
        >
          Stay on the frontier
        </h2>
        <div
          className="grid grid-cols-3"
          style={{ gap: "0.625rem" }}
        >
          {CARDS.map((card) => (
            <FrontierCard key={card.title} card={card} mediaH={card.desktopMediaH} />
          ))}
        </div>
      </div>

      {/* ── Mobile / tablet (< lg): 1-column stacked ── */}
      <div className="lg:hidden px-[1.172rem]">
        <h2
          className="text-[#26251e] font-normal"
          style={{
            fontSize: "0.944rem",
            lineHeight: "1.8125rem",
            letterSpacing: "0.005rem",
            marginBottom: "1.3125rem",
          }}
        >
          Stay on the frontier
        </h2>
        <div className="flex flex-col" style={{ gap: "0.5rem" }}>
          {CARDS.map((card) => (
            <FrontierCard key={card.title} card={card} mediaH={card.mobileMediaH} />
          ))}
        </div>
      </div>

    </section>
  );
}
