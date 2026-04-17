import { ArrowDownToLine } from "lucide-react";
import { cn } from "@/lib/cn";

const DATAVERSE_URL =
  "https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/IYNGEV";

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
      href={DATAVERSE_URL}
      target="_blank"
      rel="noopener noreferrer"
      className={cn("inline-flex items-center gap-1.5", className)}
      style={style}
    >
      Download
      {showIcon && <ArrowDownToLine size={14} strokeWidth={1.75} />}
    </a>
  );
}
