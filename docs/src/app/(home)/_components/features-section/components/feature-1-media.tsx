import Image from "next/image";

import { disGenFields } from "@/components/disease-assoc-edge-schemas";
import { geneFields } from "@/components/gene-schema";
import { SchemaTreeView } from "./schema-tree-view";

import { MaximizableWindow } from "./maximizable-window";
import { SchemaTabContent } from "./schema-tab-content";
import { WindowTabbedEditor } from "./window-tabbed-editor";

export function Feature1Media() {
  return (
    <div className="absolute inset-0 flex items-center justify-center overflow-hidden rounded-[1px]">
      <Image
        alt=""
        className="pointer-events-none scale-[1.1] object-cover"
        fill
        sizes="(min-width: 900px) 1200px, 100vw"
        src="/hero/lakeside-village.png"
      />
      <div
        className="pointer-events-none absolute inset-0"
        style={{
          background: "linear-gradient(90deg,rgba(0,0,0,0.12) 0%,rgba(0,0,0,0.22) 100%)",
        }}
      />

      <MaximizableWindow
        appIcon="/dock/editor.svg"
        appId="graph-schema"
        appName="Graph Schema"
        normalStyle={{
          width: "min(42.5rem, calc(100% - var(--l-window-inset, 4rem)))",
          height: "min(35rem, calc(100% - var(--l-window-inset, 4rem)))",
        }}
        title="Graph Schema"
      >
        <WindowTabbedEditor
          tabs={[
            {
              name: "Gene Nodes Schema",
              content: (
                <SchemaTabContent>
                  <SchemaTreeView fields={geneFields} />
                </SchemaTabContent>
              ),
            },
            {
              name: "Disease-Gene Edges Schema",
              content: (
                <SchemaTabContent>
                  <SchemaTreeView fields={disGenFields} />
                </SchemaTabContent>
              ),
            },
          ]}
        />
      </MaximizableWindow>

      <div className="pointer-events-none absolute inset-0 rounded-[1px] border border-(--l-border-subtle)" />
    </div>
  );
}
