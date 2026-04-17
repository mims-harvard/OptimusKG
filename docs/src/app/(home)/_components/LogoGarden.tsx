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
  { src: "/sources/human-phenotype-ontology.png", alt: "Human Phenotype Ontology" },
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
      <div className="relative flex h-[4rem] w-full items-center justify-center rounded-[0.25rem] bg-[var(--l-surface)] px-[0.46875rem] md:h-[4.5rem] min-[900px]:h-[6.25rem]">
        {children}
        <div className="pointer-events-none absolute inset-0 rounded-[0.25rem] border border-[var(--l-border-subtle)]" />
      </div>
    </div>
  );
}

export function LogoGarden() {
  return (
    <section className="l-section l-section--flush-y l-section--compact bg-[var(--l-bg)]">
      <div className="l-container">
        <div className="flex flex-col gap-[1.3125rem] min-[900px]:gap-[1.399rem]">
          <p className="text-center text-balance text-[var(--l-ink)] text-[0.775rem] leading-[1.231rem] tracking-[0.0082rem] md:text-[0.825rem] md:leading-[1.3125rem] md:tracking-[0.00875rem]">
            Built on trusted biomedical data sources
          </p>
          <div className="grid grid-cols-4 gap-[0.46875rem] min-[420px]:gap-[0.625rem] min-[900px]:grid-cols-8">
            {LOGOS.map(({ src, alt }) => (
              <LogoCard key={alt}>
                <img
                  src={src}
                  alt={alt}
                  className="l-logo-neutralize max-h-[2rem] w-auto object-contain md:max-h-[2.25rem] min-[900px]:max-h-[2.5rem]"
                />
              </LogoCard>
            ))}
          </div>
        </div>
      </div>
    </section>
  );
}
