"use client";

import { useState } from "react";

import { Check, Clipboard } from "lucide-react";
import { buttonVariants } from "fumadocs-ui/components/ui/button";

import { cn } from "@/lib/cn";

const BIBTEX = `@article{vittor2026optimuskg,
      title={OptimusKG: Unifying biomedical knowledge in a modern multimodal graph},
      author={Vittor, Lucas and Noori, Ayush and Arango, I{\\~n}aki and Polonuer, Joaqu{\\'\\i}n and Rodriques, Sam and White, Andrew and Clifton, David A. and Zitnik, Marinka},
      journal={Nature Scientific Data},
      year={2026}
}`;

const TEXT =
  "Vittor, L. et al. OptimusKG: Unifying biomedical knowledge in a modern multimodal graph. Nature Scientific Data (2026).";

const TABS = [
  { id: "bibtex", label: "BibTeX", content: BIBTEX },
  { id: "text", label: "Text", content: TEXT },
] as const;

export function CitationBlock() {
  const [activeTab, setActiveTab] = useState<"bibtex" | "text">("bibtex");
  const [copied, setCopied] = useState(false);

  const active = TABS.find((t) => t.id === activeTab)!;

  function handleCopy() {
    navigator.clipboard.writeText(active.content);
    setCopied(true);
    setTimeout(() => setCopied(false), 1500);
  }

  return (
    <div className="not-prose my-4 overflow-hidden rounded-xl border border-fd-border bg-fd-card shadow-sm">
      <div className="flex items-center justify-between border-fd-border border-b px-4 pt-1">
        <div className="flex gap-4">
          {TABS.map((tab) => (
            <button
              className={cn(
                "relative cursor-pointer border-0 bg-transparent pb-2 font-medium text-sm transition-colors",
                tab.id === activeTab
                  ? "text-fd-foreground"
                  : "text-fd-muted-foreground hover:text-fd-foreground"
              )}
              key={tab.id}
              onClick={() => setActiveTab(tab.id)}
              type="button"
            >
              {tab.label}
              {tab.id === activeTab && (
                <span className="absolute inset-x-0 bottom-0 h-px bg-fd-primary" />
              )}
            </button>
          ))}
        </div>
        <button
          aria-label={copied ? "Copied Text" : "Copy Text"}
          className={cn(
            buttonVariants({
              className:
                "hover:text-fd-accent-foreground data-checked:text-fd-accent-foreground",
              size: "icon-xs",
            })
          )}
          data-checked={copied || undefined}
          onClick={handleCopy}
          type="button"
        >
          {copied ? <Check /> : <Clipboard />}
        </button>
      </div>
      <pre className="m-0 overflow-x-auto whitespace-pre-wrap p-4 font-mono text-fd-muted-foreground text-[0.8125rem] leading-relaxed">
        {active.content}
      </pre>
    </div>
  );
}
