import {
  AbsoluteFill,
  Img,
  interpolate,
  staticFile,
  useCurrentFrame,
} from "remotion";
import { HERO_HEADING } from "../Beat";
import {
  EditorWindow,
  RevealLine,
  TabbedEditorShell,
  VALIDATION_ENTITIES,
  ValidationsSidebar,
} from "../primitives";
import type { BeatRenderProps } from "../scenes";

// Beat 9 (150 frames): window slides in, then 4 ticks cycling
// Anatomy → Gene → Molecular Function → Phenotype. Chart crossfades
// between pre-rendered SVGs. Final Phenotype state holds for ~15 frames.

const CYCLE_ORDER = [
  "anatomy",
  "gene",
  "molecular-function",
  "phenotype",
] as const;
type CycleId = (typeof CYCLE_ORDER)[number];

const INTRO_FRAMES = 15;
const CYCLE_FRAMES = 30; // per tick
const CYCLES = CYCLE_ORDER.length;

function currentCycleIndex(frame: number): number {
  const progress = frame - INTRO_FRAMES;
  if (progress <= 0) return 0;
  const idx = Math.floor(progress / CYCLE_FRAMES);
  return Math.min(idx, CYCLES - 1);
}

function chartCrossfade(frame: number, index: number): number {
  // Each tick: first 12 frames are the crossfade window.
  const cycleStart = INTRO_FRAMES + index * CYCLE_FRAMES;
  const local = frame - cycleStart;
  if (index === 0) return 1; // anatomy is active from t=0
  return interpolate(local, [0, 12], [0, 1], {
    extrapolateLeft: "clamp",
    extrapolateRight: "clamp",
  });
}

const ENTITY_LABEL: Record<CycleId, string> = {
  anatomy: "Anatomy",
  gene: "Gene",
  "molecular-function": "Molecular Function",
  phenotype: "Phenotype",
};

export const PaperQA3AnalysisWindow: React.FC<BeatRenderProps> = ({
  heroText,
}) => {
  const frame = useCurrentFrame();

  const windowOpacity = interpolate(frame, [0, 15], [0, 1], {
    extrapolateLeft: "clamp",
    extrapolateRight: "clamp",
  });
  const windowY = interpolate(frame, [0, 15], [30, 0], {
    extrapolateLeft: "clamp",
    extrapolateRight: "clamp",
  });

  const activeIndex = currentCycleIndex(frame);
  const activeId = CYCLE_ORDER[activeIndex];
  const previousId =
    activeIndex > 0 ? CYCLE_ORDER[activeIndex - 1] : CYCLE_ORDER[0];

  const crossfadeAmt = chartCrossfade(frame, activeIndex);

  // Build tab list: show current + previous (as a secondary tab, like the landing).
  const tabs =
    activeIndex === 0
      ? [{ name: ENTITY_LABEL[activeId] }]
      : [{ name: ENTITY_LABEL[previousId] }, { name: ENTITY_LABEL[activeId] }];
  const activeTabIndex = activeIndex === 0 ? 0 : 1;

  return (
    <AbsoluteFill
      className="bg-fd-background"
      style={{
        display: "flex",
        flexDirection: "column",
        alignItems: "center",
        justifyContent: "center",
        gap: "2rem",
        padding: "2rem 1.25rem",
      }}
    >
      <h2
        className="text-center text-fd-foreground"
        style={{ ...HERO_HEADING, fontSize: "5rem" }}
      >
        <RevealLine
          perWordFrames={4}
          startFrame={2}
          style={{ display: "block" }}
          tokens={(heroText as string).split(" ")}
        />
      </h2>
      <div
        style={{
          width: "min(92%, 1280px)",
          height: "32rem",
          opacity: windowOpacity,
          transform: `translateY(${windowY}px)`,
        }}
      >
        <EditorWindow className="h-full w-full" title="PaperQA3 Analysis">
          <div className="flex h-full min-h-0 w-full">
            <ValidationsSidebar
              activeId={activeId}
              items={VALIDATION_ENTITIES}
            />
            <div className="min-w-0 flex-1">
              <TabbedEditorShell activeIndex={activeTabIndex} tabs={tabs}>
                <div className="relative h-full w-full">
                  {activeIndex > 0 && (
                    <Img
                      src={staticFile(`features/${previousId}-light.svg`)}
                      style={{
                        position: "absolute",
                        inset: 0,
                        width: "100%",
                        height: "100%",
                        objectFit: "contain",
                        opacity: 1 - crossfadeAmt,
                      }}
                    />
                  )}
                  <Img
                    src={staticFile(`features/${activeId}-light.svg`)}
                    style={{
                      position: "absolute",
                      inset: 0,
                      width: "100%",
                      height: "100%",
                      objectFit: "contain",
                      opacity: crossfadeAmt,
                    }}
                  />
                </div>
              </TabbedEditorShell>
            </div>
          </div>
        </EditorWindow>
      </div>
    </AbsoluteFill>
  );
};
