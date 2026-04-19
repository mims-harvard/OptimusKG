import { cn } from "@/lib/cn";

const PYPI_URL = "https://pypi.org/project/optimuskg/";

export function DownloadButton({
  className,
  style,
  showIcon = true,
}: {
  className?: string;
  style?: React.CSSProperties;
  showIcon?: boolean;
}) {
  return (
    <a
      className={cn("group inline-flex items-center gap-1.5", className)}
      href={PYPI_URL}
      rel="noopener noreferrer"
      style={style}
      target="_blank"
    >
      Download
      {showIcon && (
        <svg
          aria-hidden="true"
          fill="none"
          height={14}
          stroke="currentColor"
          strokeLinecap="round"
          strokeLinejoin="round"
          strokeWidth={1.75}
          viewBox="0 0 24 24"
          width={14}
          xmlns="http://www.w3.org/2000/svg"
        >
          <g className="transition-transform duration-300 ease-out group-hover:translate-y-[2px]">
            <path d="M12 17V3" />
            <path d="m6 11 6 6 6-6" />
          </g>
          <path d="M19 21H5" />
        </svg>
      )}
    </a>
  );
}
