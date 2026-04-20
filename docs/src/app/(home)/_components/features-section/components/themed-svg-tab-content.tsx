type ThemedSvgTabContentProps = {
  lightSrc: string;
  darkSrc: string;
  alt: string;
};

export function ThemedSvgTabContent({
  lightSrc,
  darkSrc,
  alt,
}: ThemedSvgTabContentProps) {
  return (
    <div className="relative flex h-full w-full items-center justify-center p-5">
      {/* biome-ignore lint/performance/noImgElement: next/image doesn't composite light/dark SVG pairs via CSS, and these are inlined per-theme, not responsively sized */}
      {/* biome-ignore lint/correctness/useImageSize: SVG sized via parent layout, not intrinsic pixels */}
      <img
        alt={alt}
        className="h-full w-full object-contain p-5 dark:hidden"
        src={lightSrc}
      />
      {/* biome-ignore lint/performance/noImgElement: see above */}
      {/* biome-ignore lint/correctness/useImageSize: see above */}
      <img
        alt=""
        aria-hidden="true"
        className="hidden h-full w-full object-contain p-5 dark:block"
        src={darkSrc}
      />
    </div>
  );
}
