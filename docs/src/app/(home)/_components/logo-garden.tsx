const LOGOS = [
  { src: "/sources/bgee.svg", alt: "Bgee" },
  { src: "/sources/biolink-model.png", alt: "Biolink Model" },
  { src: "/sources/ctd.png", alt: "Comparative Toxicogenomics Database" },
  { src: "/sources/disease-ontology.png", alt: "Disease Ontology" },
  { src: "/sources/disgenet.png", alt: "DisGeNET" },
  { src: "/sources/drugbank.png", alt: "DrugBank" },
  { src: "/sources/drugcentral.png", alt: "DrugCentral" },
  { src: "/sources/gene-ontology.png", alt: "Gene Ontology" },
  { src: "/sources/hgnc.png", alt: "HGNC" },
  {
    src: "/sources/human-phenotype-ontology.png",
    alt: "Human Phenotype Ontology",
  },
  { src: "/sources/mondo.png", alt: "Mondo" },
  { src: "/sources/onsides.png", alt: "OnSIDES" },
  { src: "/sources/open-targets.png", alt: "Open Targets" },
  { src: "/sources/orphanet.png", alt: "Orphanet" },
  // { src: "/sources/ppi.svg", alt: "PPI" },
  { src: "/sources/reactome.png", alt: "Reactome" },
  { src: "/sources/uberon.png", alt: "Uberon" },
];

function LogoCard({ children }: { children: React.ReactNode }) {
  return (
    <div className="relative flex items-center justify-center">
      <div className="relative flex h-16 w-full items-center justify-center rounded-[1px] bg-[var(--l-surface)] px-2 md:h-18 min-[900px]:h-25">
        {children}
        <div className="pointer-events-none absolute inset-0 rounded-[1px] border border-[var(--l-border-subtle)]" />
      </div>
    </div>
  );
}

export function LogoGarden() {
  return (
    <section className="l-section l-section--flush-y l-section--compact bg-[var(--l-bg)]">
      <div className="l-container">
        <div className="flex flex-col gap-5.25 min-[900px]:gap-5.5">
          <p className="text-balance text-center text-[var(--l-ink)] text-xs leading-5 tracking-[0.0082rem] md:text-sm md:leading-5.25 md:tracking-[0.00875rem]">
            Built on trusted biomedical data sources
          </p>
          <div className="grid grid-cols-4 gap-2 min-[900px]:grid-cols-8 min-[420px]:gap-2.5">
            {LOGOS.map(({ src, alt }) => (
              <LogoCard key={alt}>
                {/* biome-ignore lint/performance/noImgElement: decorative brand logos with varying intrinsic aspect ratios. */}
                {/* biome-ignore lint/correctness/useImageSize: width/height are set via CSS max-h and w-auto to preserve each logo's native ratio. */}
                <img
                  alt={alt}
                  className="l-logo-neutralize max-h-8 w-auto object-contain md:max-h-9 min-[900px]:max-h-10"
                  src={src}
                />
              </LogoCard>
            ))}
          </div>
        </div>
      </div>
    </section>
  );
}
