import { codeToHtml } from "shiki";

import { cn } from "@/lib/cn";

export async function CodeBlock({
  code,
  lang = "python",
  className,
}: {
  code: string;
  lang?: string;
  className?: string;
}) {
  const html = await codeToHtml(code, {
    lang,
    themes: {
      light: "catppuccin-latte",
      dark: "catppuccin-mocha",
    },
  });
  return (
    <div
      className={cn("l-shiki-block h-full w-full overflow-auto", className)}
      // Server-rendered Shiki HTML, escaped at build time.
      dangerouslySetInnerHTML={{ __html: html }}
    />
  );
}
