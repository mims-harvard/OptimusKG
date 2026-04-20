import {
  DesktopCard,
  type Feature,
  MobileCard,
} from "./components/feature-card";
import { Feature1Media } from "./components/feature-1-media";
import { Feature3Media } from "./components/feature-3-media";
import { Feature4Media } from "./components/feature-4-media";

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

export function FeaturesSection() {
  return (
    <section className="l-section bg-fd-background">
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
