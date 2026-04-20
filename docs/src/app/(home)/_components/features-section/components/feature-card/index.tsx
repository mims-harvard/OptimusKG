import { cn } from "@/lib/cn";

import { FeatureText, type FeatureTextProps } from "./components/feature-text";

export type Feature = FeatureTextProps & {
  Media: React.ComponentType;
  href?: string;
  imageSide: "left" | "right";
  cardHeightClass: string;
  mediaHeightClass: string;
};

export function DesktopCard({ feature }: { feature: Feature }) {
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
        "relative grid grid-cols-[repeat(24,minmax(0,1fr))] gap-x-2.5 rounded-[1px] bg-fd-card p-4.5",
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
      <div className="pointer-events-none absolute inset-0 rounded-[1px] border border-(--l-border-subtle)" />
    </div>
  );
}

export function MobileCard({ feature }: { feature: Feature }) {
  const { href, Media } = feature;
  const linkProps = href
    ? { href, target: "_blank", rel: "noopener noreferrer" as const }
    : {};
  const Tag = href ? "a" : "div";

  return (
    <div className="relative flex flex-col overflow-hidden rounded-[1px] bg-fd-card">
      <Tag {...linkProps} className="group/card block p-4 md:p-6">
        <FeatureText {...feature} />
      </Tag>
      <div className="relative h-128 shrink-0 overflow-hidden sm:h-144 md:h-160">
        <Media />
      </div>
      <div className="pointer-events-none absolute inset-0 rounded-[1px] border border-(--l-border-subtle)" />
    </div>
  );
}
