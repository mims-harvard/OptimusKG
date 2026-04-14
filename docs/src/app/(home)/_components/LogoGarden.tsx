const LOGOS = [
  { src: "https://aiscientist.tools/logos/mit.png", alt: "MIT" },
  { src: "/institutions/kempner.svg", alt: "Kempner Institute" },
  { src: "https://aiscientist.tools/logos/broad.png", alt: "Broad Institute" },
  { src: "https://aiscientist.tools/logos/HDSI.png", alt: "HDSI" },
  { src: "/partners/amazon.svg", alt: "Amazon" },
  { src: "/partners/arpa-h.svg", alt: "ARPA-H" },
  { src: "/partners/astrazeneca.svg", alt: "AstraZeneca" },
  { src: "/partners/boehringer-ingelheim.svg", alt: "Boehringer Ingelheim" },
  { src: "/partners/chan-zuckerberg-initiative.svg", alt: "Chan Zuckerberg Initiative" },
  { src: "/partners/roche.svg", alt: "Roche" },
  { src: "/partners/gates-foundation.svg", alt: "Gates Foundation" },
  { src: "/partners/google.svg", alt: "Google" },
  { src: "/partners/gsk.svg", alt: "GSK" },
  { src: "/partners/harvard.svg", alt: "Harvard University" },
  { src: "/partners/merck.svg", alt: "Merck" },
  { src: "/partners/nsf.svg", alt: "National Science Foundation" },
  { src: "/partners/nih.svg", alt: "NIH" },
  { src: "/partners/optum.svg", alt: "Optum" },
  { src: "/partners/pfizer.svg", alt: "Pfizer" },
  { src: "/partners/sanofi.svg", alt: "Sanofi" },
  { src: "/partners/dod.svg", alt: "U.S. Department of Defense" },
];

function LogoCard({ children }: { children: React.ReactNode }) {
  return (
    <div className="flex h-[3.75rem] items-center justify-center lg:h-[5rem]">
      <div className="relative flex min-h-0 min-w-0 flex-1 items-center justify-center rounded-[0.25rem] bg-[var(--l-surface)] px-[0.75rem] lg:px-[1rem]">
        {children}
        <div className="absolute inset-0 rounded-[0.25rem] border border-[var(--l-border-subtle)]" />
      </div>
    </div>
  );
}

export function LogoGarden() {
  return (
    <section className="bg-[var(--l-bg)] pb-[2.1rem]">
      <div className="mx-auto max-w-[81.25rem] px-[1.172rem] lg:px-[1.25rem]">
        <div className="flex flex-col gap-[1.3125rem] lg:gap-[1.399rem]">
          <p className="text-center whitespace-nowrap text-[var(--l-ink)] text-[0.775rem] leading-[1.231rem] tracking-[0.0082rem] lg:text-[0.825rem] lg:leading-[1.3125rem] lg:tracking-[0.00875rem]">
            Trusted every day by researchers at world-class{" "}
            <span className="block lg:inline">institutions</span>
          </p>
          <div className="grid grid-cols-3 gap-[0.4395rem] lg:grid-cols-7 lg:gap-[0.625rem]">
            {LOGOS.map(({ src, alt }) => (
              <LogoCard key={alt}>
                <img
                  src={src}
                  alt={alt}
                  className="max-h-[1.875rem] w-auto object-contain grayscale lg:max-h-[2.5rem]"
                />
              </LogoCard>
            ))}
          </div>
        </div>
      </div>
    </section>
  );
}
