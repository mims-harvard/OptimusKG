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

// Beat 9 (150 frames):
//   0–24    Hero text "Rigorously validated." reveals, centered, alone.
//   24–36   Hero fades out and window fades in (no overlap of both visible).
//   36–150  Window visible; sidebar auto-cycles Anatomy → Gene →
//           Molecular Function → Phenotype with chart crossfades.

const HERO_FADE_OUT: [number, number] = [64, 76];
const WINDOW_FADE_IN: [number, number] = [68, 82];
const CYCLE_BEGIN = 82;
const CYCLE_FRAMES = 24;

// Window aspect ratio mirrors the landing's `42.5rem × 35rem` (≈ 17:14 / 1.214).
const WINDOW_W = 900;
const WINDOW_H = Math.round(WINDOW_W * 0.62); // ~1.61 aspect — chart fills the window snugly, no bottom whitespace

const CYCLE_ORDER = [
  "anatomy",
  "gene",
  "molecular-function",
  "phenotype",
] as const;
type CycleId = (typeof CYCLE_ORDER)[number];

const ENTITY_LABEL: Record<CycleId, string> = {
  anatomy: "Anatomy",
  gene: "Gene",
  "molecular-function": "Molecular Function",
  phenotype: "Phenotype",
};

function currentCycleIndex(frame: number): number {
  const progress = frame - CYCLE_BEGIN;
  if (progress <= 0) return 0;
  const idx = Math.floor(progress / CYCLE_FRAMES);
  return Math.min(idx, CYCLE_ORDER.length - 1);
}

function chartCrossfade(frame: number, index: number): number {
  if (index === 0) return 1;
  const cycleStart = CYCLE_BEGIN + index * CYCLE_FRAMES;
  return interpolate(frame - cycleStart, [0, 12], [0, 1], {
    extrapolateLeft: "clamp",
    extrapolateRight: "clamp",
  });
}

export const PaperQA3AnalysisWindow: React.FC<BeatRenderProps> = ({
  heroText,
}) => {
  const frame = useCurrentFrame();

  const heroOpacity = interpolate(frame, HERO_FADE_OUT, [1, 0], {
    extrapolateLeft: "clamp",
    extrapolateRight: "clamp",
  });
  const windowOpacity = interpolate(frame, WINDOW_FADE_IN, [0, 1], {
    extrapolateLeft: "clamp",
    extrapolateRight: "clamp",
  });
  const windowY = interpolate(frame, WINDOW_FADE_IN, [24, 0], {
    extrapolateLeft: "clamp",
    extrapolateRight: "clamp",
  });

  const activeIndex = currentCycleIndex(frame);
  const activeId = CYCLE_ORDER[activeIndex];
  const previousId =
    activeIndex > 0 ? CYCLE_ORDER[activeIndex - 1] : CYCLE_ORDER[0];
  const crossfadeAmt = chartCrossfade(frame, activeIndex);

  // Tabs accumulate as cycles fire: each new entity appends a tab.
  // [Anatomy] → [Anatomy, Gene] → [Anatomy, Gene, Molecular Function] → …
  const tabs = CYCLE_ORDER.slice(0, activeIndex + 1).map((id) => ({
    name: ENTITY_LABEL[id],
  }));
  const activeTabIndex = activeIndex;

  return (
    <AbsoluteFill className="bg-fd-background">
      {/* Hero text — visible alone, then fades out before the window arrives. */}
      {heroOpacity > 0 && (
        <div
          style={{
            position: "absolute",
            top: "50%",
            left: 0,
            right: 0,
            transform: "translateY(-50%)",
            textAlign: "center",
            opacity: heroOpacity,
          }}
        >
          <h2 className="text-fd-foreground" style={{ ...HERO_HEADING }}>
            <RevealLine
              perWordFrames={2}
              startFrame={0}
              tokens={(heroText as string).split(" ")}
            />
          </h2>
        </div>
      )}

      {/* PaperQA3 window — landing-aspect, vertically centered. */}
      {windowOpacity > 0 && (
        <div
          style={{
            position: "absolute",
            top: "50%",
            left: "50%",
            width: WINDOW_W,
            height: WINDOW_H,
            transform: `translate(-50%, calc(-50% + ${windowY}px))`,
            opacity: windowOpacity,
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
                          top: 28, left: 0, right: 0, bottom: 0,
                          
                          
                          objectFit: "contain", objectPosition: "center top",
                          opacity: 1 - crossfadeAmt,
                        }}
                      />
                    )}
                    <Img
                      src={staticFile(`features/${activeId}-light.svg`)}
                      style={{
                        position: "absolute",
                        top: 28, left: 0, right: 0, bottom: 0,
                        
                        
                        objectFit: "contain", objectPosition: "center top",
                        opacity: crossfadeAmt,
                      }}
                    />
                  </div>
                </TabbedEditorShell>
              </div>
            </div>
          </EditorWindow>
        </div>
      )}
    </AbsoluteFill>
  );
};
