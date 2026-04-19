import { ArrowDownToLine } from "lucide-react";

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
      className={cn("inline-flex items-center gap-1.5", className)}
      href={PYPI_URL}
      rel="noopener noreferrer"
      style={style}
      target="_blank"
    >
      Download
      {showIcon && <ArrowDownToLine size={14} strokeWidth={1.75} />}
    </a>
  );
}
