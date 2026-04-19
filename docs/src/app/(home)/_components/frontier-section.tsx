import { ArrowUpRight } from "lucide-react";
import { cn } from "@/lib/cn";

const CARDS = [
  {
    title: "Use the best model for every task",
    description:
      "Choose between every cutting-edge model from OpenAI, Anthropic, Gemini, xAI, and Cursor.",
    ctaText: "Explore models",
    ctaHref: "https://cursor.com/docs/models",
    ctaExternal: true,
    imageSrc: "/frontier/metagraph.svg",
    imageAlt: "Knowledge graph",
    mobileHeightClass: "h-60",
  },
  {
    title: "Complete codebase understanding",
    description:
      "Cursor learns how your codebase works, no matter the scale or complexity.",
    ctaText: "Learn about codebase indexing",
    ctaHref: "https://cursor.com/docs/context/codebase-indexing",
    ctaExternal: true,
    imageSrc: "/frontier/metapath.svg",
    imageAlt: "Metapath matrix",
    mobileHeightClass: "h-60",
  },
  {
    title: "Develop enduring software",
    description:
      "Trusted by over half of the Fortune 500 to accelerate development, securely and at scale.",
    ctaText: "Explore enterprise",
    ctaHref: "https://cursor.com/en-US/enterprise",
    ctaExternal: false,
    imageSrc: "/frontier/lines.svg",
    imageAlt: "Distribution chart",
    mobileHeightClass: "h-80",
  },
];

const MEDIA_BG =
  "linear-gradient(rgba(38,37,30,0.05),rgba(38,37,30,0.05)), linear-gradient(#f2f1ed,#f2f1ed)";

const BODY_TEXT = "text-base leading-6 tracking-[0.005rem]";

function FrontierCard({ card }: { card: (typeof CARDS)[number] }) {
  return (
    <div className="relative flex flex-col rounded-sm bg-[var(--l-surface)] pt-4 pb-4.5 px-4.5">
      <div className="flex flex-col">
        <h3 className={`font-normal text-[var(--l-ink)] ${BODY_TEXT}`}>{card.title}</h3>
        <p className={`font-normal text-[var(--l-ink-muted)] ${BODY_TEXT}`}>{card.description}</p>
        <div className="pt-3.5">
          <a
            href={card.ctaHref}
            target={card.ctaExternal ? "_blank" : undefined}
            rel={card.ctaExternal ? "noopener noreferrer" : undefined}
            className={`group inline-flex items-center gap-0.5 font-normal text-[var(--l-accent)] ${BODY_TEXT}`}
          >
            {card.ctaText}
            <ArrowUpRight
              size={14}
              strokeWidth={2}
              aria-hidden="true"
              className="transition-transform duration-300 ease-out group-hover:translate-x-0.5 group-hover:-translate-y-0.5"
            />
          </a>
        </div>
      </div>
      <div className="pt-4.5">
        <div
          className={cn("relative overflow-hidden rounded-sm md:h-80 min-[900px]:h-98", card.mobileHeightClass)}
          style={{ backgroundImage: MEDIA_BG }}
        >
          <img
            src={card.imageSrc}
            alt={card.imageAlt}
            className="absolute inset-0 h-full w-full object-cover"
          />
        </div>
      </div>
      <div className="pointer-events-none absolute inset-0 rounded-sm border border-[var(--l-border-subtle)]" />
    </div>
  );
}

export function FrontierSection() {
  return (
    <section className="l-section bg-[var(--l-bg)]">
      <div className="l-container">
        <h2 className="mb-5.25 font-normal text-[var(--l-ink)] text-base leading-7.25 tracking-[0.005rem] min-[900px]:mb-5.5">
          Stay on the frontier
        </h2>
        <div className="grid grid-cols-1 gap-2 sm:grid-cols-2 sm:gap-2.25 min-[900px]:grid-cols-3 min-[900px]:gap-2.5">
          {CARDS.map((card) => (
            <FrontierCard key={card.title} card={card} />
          ))}
        </div>
      </div>
    </section>
  );
}
