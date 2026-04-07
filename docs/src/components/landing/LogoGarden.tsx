const LOGOS = [
  // ── Institutions ──────────────────────────────────────────────────────────
  { src: "https://aiscientist.tools/logos/mit.png",     alt: "MIT" },
  { src: "/institutions/kempner.svg", alt: "Kempner Institute" },
  { src: "https://aiscientist.tools/logos/broad.png",   alt: "Broad Institute" },
  { src: "https://aiscientist.tools/logos/HDSI.png",    alt: "HDSI" },
  // ── Partners ──────────────────────────────────────────────────────────────
  { src: "/partners/amazon.svg",                   alt: "Amazon" },
  { src: "/partners/arpa-h.svg",                   alt: "ARPA-H" },
  { src: "/partners/astrazeneca.svg",              alt: "AstraZeneca" },
  { src: "/partners/boehringer-ingelheim.svg",     alt: "Boehringer Ingelheim" },
  { src: "/partners/chan-zuckerberg-initiative.svg", alt: "Chan Zuckerberg Initiative" },
  { src: "/partners/roche.svg",                    alt: "Roche" },
  { src: "/partners/gates-foundation.svg",         alt: "Gates Foundation" },
  { src: "/partners/google.svg",                   alt: "Google" },
  { src: "/partners/gsk.svg",                      alt: "GSK" },
  { src: "/partners/harvard.svg",                  alt: "Harvard University" },
  { src: "/partners/merck.svg",                    alt: "Merck" },
  { src: "/partners/nsf.svg",                      alt: "National Science Foundation" },
  { src: "/partners/nih.svg",                      alt: "NIH" },
  { src: "/partners/optum.svg",                    alt: "Optum" },
  { src: "/partners/pfizer.svg",                   alt: "Pfizer" },
  { src: "/partners/sanofi.svg",                   alt: "Sanofi" },
  { src: "/partners/dod.svg",                      alt: "U.S. Department of Defense" },
];

function LogoCard({
  children,
  height,
  px,
}: {
  children: React.ReactNode;
  height: string;
  px: string;
}) {
  return (
    <div className="flex items-center justify-center" style={{ height }}>
      <div
        className="relative flex flex-1 items-center justify-center bg-[#f2f1ed] rounded-[0.25rem] min-w-0 min-h-0"
        style={{ height, paddingInline: px }}
      >
        {children}
        <div className="absolute inset-0 rounded-[0.25rem] border border-[rgba(38,37,30,0.03)]" />
      </div>
    </div>
  );
}

export function LogoGarden() {
  return (
    <section className="bg-[#f7f7f4] pb-[2.1rem]">

      {/* ── Mobile / tablet (< lg): 3-col grid ── */}
      <div className="lg:hidden px-[1.172rem]">
        <div className="flex flex-col gap-[1.3125rem] max-w-[81.25rem]">
          {/* heading */}
          <div className="flex flex-col items-center w-full pb-[0.033rem]">
            <p
              className="text-[#26251e] text-center whitespace-nowrap"
              style={{ fontSize: "0.775rem", lineHeight: "1.231rem", letterSpacing: "0.0082rem" }}
            >
              Trusted every day by researchers at world-class
            </p>
            <p
              className="text-[#26251e] text-center whitespace-nowrap"
              style={{ fontSize: "0.775rem", lineHeight: "1.231rem", letterSpacing: "0.0082rem" }}
            >
              institutions
            </p>
          </div>

          {/* 3-col grid */}
          <div
            className="grid grid-cols-3 w-full"
            style={{ gap: "0.4395rem" }}
          >
            {LOGOS.map(({ src, alt }) => (
              <LogoCard key={alt} height="3.75rem" px="0.75rem">
                <img
                  src={src}
                  alt={alt}
                  className="max-h-[1.875rem] w-auto object-contain grayscale"
                />
              </LogoCard>
            ))}
          </div>
        </div>
      </div>

      {/* ── Desktop (≥ lg): 7-col grid ── */}
      <div className="hidden lg:block px-[1.25rem] max-w-[81.25rem] mx-auto">
        <div className="flex flex-col gap-[1.399rem]">
          {/* heading */}
          <div className="flex flex-col items-center w-full">
            <p
              className="text-[#26251e] text-center whitespace-nowrap"
              style={{ fontSize: "0.825rem", lineHeight: "1.3125rem", letterSpacing: "0.00875rem" }}
            >
              Trusted every day by researchers at world-class institutions
            </p>
          </div>

          {/* 7-col grid */}
          <div
            className="grid grid-cols-7 w-full"
            style={{ gap: "0.625rem" }}
          >
            {LOGOS.map(({ src, alt }) => (
              <LogoCard key={alt} height="5rem" px="1rem">
                <img
                  src={src}
                  alt={alt}
                  className="max-h-[2.5rem] w-auto object-contain grayscale"
                />
              </LogoCard>
            ))}
          </div>
        </div>
      </div>

    </section>
  );
}
