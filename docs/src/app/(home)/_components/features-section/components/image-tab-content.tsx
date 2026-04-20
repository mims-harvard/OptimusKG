import Image from "next/image";

export function ImageTabContent({ src, alt }: { src: string; alt: string }) {
  return (
    <div className="relative flex h-full w-full items-center justify-center p-5">
      <Image
        alt={alt}
        className="object-contain p-5"
        fill
        sizes="(min-width: 900px) 680px, 100vw"
        src={src}
      />
    </div>
  );
}
