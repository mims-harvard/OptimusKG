import { MoveUpRight } from "lucide-react";
import { cn } from "@/lib/cn";

const CARDS = [
  {
    title: "Use the best model for every task",
    description:
      "Choose between every cutting-edge model from OpenAI, Anthropic, Gemini, xAI, and Cursor.",
    ctaText: "Explore models",
    ctaHref: "https://cursor.com/docs/models",
    ctaExternal: true,
    imageSrc: "/metagraph.svg",
    imageAlt: "Knowledge graph",
    mobileHeightClass: "h-[14.988rem]",
  },
  {
    title: "Complete codebase understanding",
    description:
      "Cursor learns how your codebase works, no matter the scale or complexity.",
    ctaText: "Learn about codebase indexing",
    ctaHref: "https://cursor.com/docs/context/codebase-indexing",
    ctaExternal: true,
    imageSrc: "/metapath.svg",
    imageAlt: "Metapath matrix",
    mobileHeightClass: "h-[14.988rem]",
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
    mobileHeightClass: "h-[19.981rem]",
  },
];

const MEDIA_BG =
  "linear-gradient(rgba(38,37,30,0.05),rgba(38,37,30,0.05)), linear-gradient(#f2f1ed,#f2f1ed)";

const BODY_TEXT = { fontSize: "0.944rem", lineHeight: "1.5rem", letterSpacing: "0.005rem" };

function FrontierCard({ card }: { card: (typeof CARDS)[number] }) {
  return (
    <div
      className="relative flex flex-col rounded-[0.25rem] bg-[var(--l-surface)]"
      style={{ paddingTop: "0.994rem", paddingBottom: "1.094rem", paddingInline: "1.094rem" }}
    >
      <div className="flex flex-col">
        <h3 className="font-normal text-[var(--l-ink)]" style={BODY_TEXT}>{card.title}</h3>
        <p className="font-normal text-[var(--l-ink-muted)]" style={BODY_TEXT}>{card.description}</p>
        <div style={{ paddingTop: "0.901rem" }}>
          <a
            href={card.ctaHref}
            target={card.ctaExternal ? "_blank" : undefined}
            rel={card.ctaExternal ? "noopener noreferrer" : undefined}
            className="inline-flex items-center gap-[0.3rem] font-normal text-[var(--l-accent)]"
            style={BODY_TEXT}
          >
            {card.ctaText}
            {card.ctaExternal && <MoveUpRight size={14} strokeWidth={2} />}
          </a>
        </div>
      </div>
      <div style={{ paddingTop: "1.094rem" }}>
        <div
          className={cn("relative overflow-hidden rounded-[0.25rem] lg:h-[24.481rem]", card.mobileHeightClass)}
          style={{ backgroundImage: MEDIA_BG }}
        >
          <img
            src={card.imageSrc}
            alt={card.imageAlt}
            className="absolute inset-0 h-full w-full object-cover"
          />
        </div>
      </div>
      <div className="pointer-events-none absolute inset-0 rounded-[0.25rem] border border-[var(--l-border-subtle)]" />
    </div>
  );
}

export function FrontierSection() {
  return (
    <section className="bg-[var(--l-bg)]" style={{ paddingBlock: "4.2rem" }}>
      <div className="mx-auto max-w-[81.25rem] px-[1.172rem] lg:px-[1.25rem]">
        <h2
          className="mb-[1.3125rem] font-normal text-[var(--l-ink)] lg:mb-[1.4rem]"
          style={{ fontSize: "0.944rem", lineHeight: "1.8125rem", letterSpacing: "0.005rem" }}
        >
          Stay on the frontier
        </h2>
        <div className="grid grid-cols-1 gap-[0.5rem] lg:grid-cols-3 lg:gap-[0.625rem]">
          {CARDS.map((card) => (
            <FrontierCard key={card.title} card={card} />
          ))}
        </div>
      </div>
    </section>
  );
}
