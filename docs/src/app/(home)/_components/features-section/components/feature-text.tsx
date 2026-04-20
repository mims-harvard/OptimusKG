import { ArrowRight } from "lucide-react";

import { Snippet } from "./snippet";

const HEADING_CLASSES = "text-xl leading-7";
const DESCRIPTION_CLASSES = "text-md leading-6";
const CTA_CLASSES = "text-base leading-6";

export type FeatureTextProps = {
  title: string;
  description: string;
  ctaText: string;
  ctaHref?: string;
  ctaVariant?: "link" | "snippet";
};

export function FeatureText({
  title,
  description,
  ctaText,
  ctaHref,
  ctaVariant = "link",
}: FeatureTextProps) {
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
      "group/cta inline-flex items-center gap-0.5 text-(--l-accent)";
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
        <h3 className={`font-normal text-fd-foreground ${HEADING_CLASSES}`}>
          {title}
        </h3>
        <p
          className={`font-normal text-fd-muted-foreground ${DESCRIPTION_CLASSES}`}
        >
          {description}
        </p>
      </div>
      {cta}
    </div>
  );
}
