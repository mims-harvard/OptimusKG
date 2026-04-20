import { MaximizableWindow } from "./maximizable-window";
import { ThemedSvgTabContent } from "./themed-svg-tab-content";
import { WindowTabbedEditor } from "./window-tabbed-editor";

const BG_SRC = "/hero/mountain-overlook.png";

export function Feature3Media() {
  return (
    <div className="absolute inset-0 flex items-center justify-center overflow-hidden rounded-[1px]">
      <div className="pointer-events-none absolute inset-0 overflow-hidden">
        {/* biome-ignore lint/performance/noImgElement: intentionally overscaled panoramic background, next/image fill cannot reproduce the percentage stretch */}
        {/* biome-ignore lint/correctness/useImageSize: size is expressed as a percentage of the container, not intrinsic pixels */}
        <img
          alt=""
          className="absolute max-w-none"
          src={BG_SRC}
          style={{ height: "100%", left: "-45.96%", top: 0, width: "191.91%" }}
        />
      </div>
      <div
        className="pointer-events-none absolute inset-0"
        style={{
          background: "linear-gradient(90deg,rgba(38,37,30,0.05) 0%,rgba(38,37,30,0.05) 100%)",
        }}
      />

      <MaximizableWindow
        appIcon="/dock/editor.svg"
        appId="paperqa3"
        appName="PaperQA3 Analysis"
        normalStyle={{
          width: "min(42.5rem, calc(100% - var(--l-window-inset, 4rem)))",
          height: "min(35rem, calc(100% - var(--l-window-inset, 4rem)))",
        }}
        title="PaperQA3 Analysis"
      >
        <WindowTabbedEditor
          tabs={[
            {
              name: "Molecular Function Validation",
              content: (
                <ThemedSvgTabContent
                  alt="Molecular Function validation bar chart"
                  darkSrc="/features/molecular-function-dark.svg"
                  lightSrc="/features/molecular-function-light.svg"
                />
              ),
            },
            {
              name: "Phenotype Validation",
              content: (
                <ThemedSvgTabContent
                  alt="Phenotype validation bar chart"
                  darkSrc="/features/phenotype-dark.svg"
                  lightSrc="/features/phenotype-light.svg"
                />
              ),
            },
          ]}
        />
      </MaximizableWindow>

      <div className="pointer-events-none absolute inset-0 rounded-[1px] border border-(--l-border-subtle)" />
    </div>
  );
}
