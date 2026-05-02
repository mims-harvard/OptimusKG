import type { CSSProperties, ReactNode } from "react";
import {
  AbsoluteFill,
  Img,
  interpolate,
  staticFile,
  useCurrentFrame,
} from "remotion";
import { fontWeight } from "../../tokens";

// Beat: PaperQA3 analysis window. Adapted from
//   https://github.com/mims-harvard/OptimusKG/blob/remotion/assets/remotion/src/beats/PaperQA3AnalysisWindow.tsx
// — only the editor-window/sidebar/cycle part. Self-contained: the
// EditorWindow, TabbedEditorShell and ValidationsSidebar primitives are
// inlined here as private helpers.

// ─── Public timings (frames are scene-7-local) ───────────────────────────
const WINDOW_FADE_IN: [number, number] = [168, 198];
const CYCLE_BEGIN = 198;
const CYCLE_FRAMES = 60;
const CHART_CROSSFADE_FRAMES = 24;

// ─── Window geometry ─────────────────────────────────────────────────────
const WINDOW_W = 900;
const WINDOW_H = Math.round(WINDOW_W * 0.62); // ≈ 558

// ─── Validation entities (sidebar list) ──────────────────────────────────
type ValidationEntity = { id: string; label: string };

const VALIDATION_ENTITIES: ValidationEntity[] = [
  { id: "anatomy", label: "Anatomy" },
  { id: "biological-process", label: "Biological Process" },
  { id: "cellular-component", label: "Cellular Component" },
  { id: "disease", label: "Disease" },
  { id: "drug", label: "Drug" },
  { id: "exposure", label: "Exposure" },
  { id: "gene", label: "Gene" },
  { id: "molecular-function", label: "Molecular Function" },
  { id: "pathway", label: "Pathway" },
  { id: "phenotype", label: "Phenotype" },
];

// Order the sidebar cycles through; each id must have a matching SVG
// at public/features/{id}-light.svg. Gene is intentionally excluded from
// the cycle (it still appears in the sidebar list, just never goes active).
const CYCLE_ORDER = [
  "anatomy",
  "molecular-function",
  "phenotype",
] as const;
type CycleId = (typeof CYCLE_ORDER)[number];

const ENTITY_LABEL: Record<CycleId, string> = {
  anatomy: "Anatomy",
  "molecular-function": "Molecular Function",
  phenotype: "Phenotype",
};

// ─── Theme palette (kept inline to avoid Tailwind theme dependency) ──────
const COLOR_BG = "#f5f5f5";
const COLOR_CARD = "#f1f1f1";
const COLOR_BORDER = "rgba(204, 204, 204, 0.6)";
const COLOR_FG = "#0a0a0a";
const COLOR_MUTED_FG = "#737373";
const COLOR_ACCENT_BG = "#e2e8f0";

// ─── Helpers ─────────────────────────────────────────────────────────────
function currentCycleIndex(frame: number): number {
  const progress = frame - CYCLE_BEGIN;
  if (progress <= 0) return 0;
  const idx = Math.floor(progress / CYCLE_FRAMES);
  return Math.min(idx, CYCLE_ORDER.length - 1);
}

function chartCrossfade(frame: number, index: number): number {
  if (index === 0) return 1;
  const cycleStart = CYCLE_BEGIN + index * CYCLE_FRAMES;
  return interpolate(
    frame - cycleStart,
    [0, CHART_CROSSFADE_FRAMES],
    [0, 1],
    {
      extrapolateLeft: "clamp",
      extrapolateRight: "clamp",
    },
  );
}

// ─── Inlined window primitives ───────────────────────────────────────────
const WinControl: React.FC = () => (
  <div
    style={{
      background: COLOR_MUTED_FG,
      borderRadius: 999,
      height: "0.625rem",
      opacity: 0.55,
      width: "0.625rem",
    }}
  />
);

const EditorWindow: React.FC<{ title?: string; children: ReactNode }> = ({
  title,
  children,
}) => (
  <div
    style={{
      background: COLOR_CARD,
      borderRadius: "0.625rem",
      boxShadow:
        "0px 28px 70px 0px rgba(0,0,0,0.14), 0px 14px 32px 0px rgba(0,0,0,0.1), 0px 0px 0px 1px rgba(38,37,30,0.1)",
      display: "flex",
      flexDirection: "column",
      height: "100%",
      overflow: "hidden",
      width: "100%",
    }}
  >
    <div
      style={{
        alignItems: "center",
        background: COLOR_CARD,
        borderBottom: `1px solid ${COLOR_BORDER}`,
        display: "flex",
        flexShrink: 0,
        height: "1.75rem",
        padding: "0 0.5rem",
        position: "relative",
      }}
    >
      <div style={{ display: "flex", gap: "0.375rem" }}>
        <WinControl />
        <WinControl />
        <WinControl />
      </div>
      {title ? (
        <span
          style={{
            color: COLOR_MUTED_FG,
            fontSize: 12,
            left: "50%",
            position: "absolute",
            transform: "translateX(-50%)",
            whiteSpace: "nowrap",
          }}
        >
          {title}
        </span>
      ) : null}
    </div>
    {children}
  </div>
);

const ChevronRightIcon: React.FC<{ size?: number }> = ({ size = 10 }) => (
  <svg
    aria-hidden="true"
    fill="none"
    height={size}
    stroke="currentColor"
    strokeLinecap="round"
    strokeLinejoin="round"
    strokeWidth={1.75}
    style={{ transform: "rotate(90deg)" }}
    viewBox="0 0 24 24"
    width={size}
  >
    <path d="m9 18 6-6-6-6" />
  </svg>
);

const ChartColumnIcon: React.FC<{ size?: number }> = ({ size = 16 }) => (
  <svg
    aria-hidden="true"
    fill="none"
    height={size}
    stroke="currentColor"
    strokeLinecap="round"
    strokeLinejoin="round"
    strokeWidth={1.75}
    viewBox="0 0 24 24"
    width={size}
  >
    <path d="M3 3v18h18" />
    <path d="M18 17V9" />
    <path d="M13 17V5" />
    <path d="M8 17v-3" />
  </svg>
);

const ValidationsSidebar: React.FC<{ activeId: string }> = ({ activeId }) => (
  <aside
    style={{
      background: COLOR_CARD,
      borderRight: `1px solid ${COLOR_BORDER}`,
      color: COLOR_FG,
      display: "flex",
      flexDirection: "column",
      flexShrink: 0,
      width: "17rem",
    }}
  >
    <div style={{ flex: 1, minHeight: 0, overflowY: "auto", padding: "0.5rem 0" }}>
      <div
        style={{
          alignItems: "center",
          color: COLOR_MUTED_FG,
          display: "flex",
          fontSize: 13,
          fontWeight: fontWeight.semibold,
          gap: "0.25rem",
          letterSpacing: "0.04em",
          padding: "0.375rem 0.75rem",
          textTransform: "uppercase",
        }}
      >
        <ChevronRightIcon size={10} />
        <span>validations</span>
      </div>
      <ul style={{ listStyle: "none", margin: 0, padding: 0 }}>
        {VALIDATION_ENTITIES.map((item) => {
          const active = item.id === activeId;
          return (
            <li
              key={item.id}
              style={{
                alignItems: "center",
                background: active ? COLOR_ACCENT_BG : "transparent",
                color: COLOR_FG,
                display: "flex",
                fontSize: 17,
                gap: "0.5rem",
                padding: "0.25rem 0.5rem 0.25rem 2rem",
              }}
            >
              <ChartColumnIcon size={16} />
              <span
                style={{
                  overflow: "hidden",
                  textOverflow: "ellipsis",
                  whiteSpace: "nowrap",
                }}
              >
                {item.label}
              </span>
            </li>
          );
        })}
      </ul>
    </div>
  </aside>
);

const TabbedEditorShell: React.FC<{
  tabs: { name: string }[];
  activeIndex: number;
  children: ReactNode;
}> = ({ tabs, activeIndex, children }) => (
  <div
    style={{
      display: "flex",
      flexDirection: "column",
      height: "100%",
      width: "100%",
    }}
  >
    <div
      style={{
        alignItems: "center",
        background: COLOR_CARD,
        display: "flex",
        flexShrink: 0,
        height: "2.5rem",
      }}
    >
      {tabs.map((tab, i) => {
        const active = i === activeIndex;
        const baseStyle: CSSProperties = {
          alignItems: "center",
          borderRight: `1px solid ${COLOR_BORDER}`,
          display: "flex",
          flexShrink: 0,
          gap: "0.375rem",
          height: "100%",
          padding: "0 0.75rem",
        };
        return (
          <div
            key={tab.name}
            style={
              active
                ? {
                    ...baseStyle,
                    background: COLOR_BG,
                    color: COLOR_FG,
                    paddingBottom: 1,
                  }
                : {
                    ...baseStyle,
                    background: COLOR_CARD,
                    borderBottom: `1px solid ${COLOR_BORDER}`,
                    color: COLOR_MUTED_FG,
                  }
            }
          >
            <span
              style={{
                fontSize: 16,
                overflow: "hidden",
                textOverflow: "ellipsis",
                whiteSpace: "nowrap",
              }}
            >
              {tab.name}
            </span>
          </div>
        );
      })}
      <div
        style={{
          borderBottom: `1px solid ${COLOR_BORDER}`,
          flex: 1,
          height: "100%",
        }}
      />
    </div>
    <div
      style={{
        background: COLOR_BG,
        flex: 1,
        minHeight: 0,
        overflow: "hidden",
      }}
    >
      {children}
    </div>
  </div>
);

// ─── Beat ────────────────────────────────────────────────────────────────
export const PaperQA: React.FC = () => {
  const frame = useCurrentFrame();

  const windowOpacity = interpolate(frame, WINDOW_FADE_IN, [0, 1], {
    extrapolateLeft: "clamp",
    extrapolateRight: "clamp",
  });
  const windowY = interpolate(frame, WINDOW_FADE_IN, [24, 0], {
    extrapolateLeft: "clamp",
    extrapolateRight: "clamp",
  });

  if (windowOpacity <= 0) return null;

  const activeIndex = currentCycleIndex(frame);
  const activeId = CYCLE_ORDER[activeIndex];
  const previousId =
    activeIndex > 0 ? CYCLE_ORDER[activeIndex - 1] : CYCLE_ORDER[0];
  const crossfadeAmt = chartCrossfade(frame, activeIndex);

  const tabs = CYCLE_ORDER.slice(0, activeIndex + 1).map((id) => ({
    name: ENTITY_LABEL[id],
  }));

  return (
    <AbsoluteFill>
      <div
        style={{
          height: WINDOW_H,
          left: "50%",
          opacity: windowOpacity,
          position: "absolute",
          top: "50%",
          transform: `translate(-50%, calc(-50% + ${windowY}px))`,
          width: WINDOW_W,
        }}
      >
        <EditorWindow title="PaperQA3 Analysis">
          <div
            style={{
              display: "flex",
              height: "100%",
              minHeight: 0,
              width: "100%",
            }}
          >
            <ValidationsSidebar activeId={activeId} />
            <div style={{ flex: 1, minWidth: 0 }}>
              <TabbedEditorShell activeIndex={activeIndex} tabs={tabs}>
                <div style={{ height: "100%", position: "relative", width: "100%" }}>
                  {activeIndex > 0 ? (
                    <Img
                      src={staticFile(`features/${previousId}-light.svg`)}
                      style={{
                        bottom: 0,
                        left: 0,
                        objectFit: "contain",
                        objectPosition: "center top",
                        opacity: 1 - crossfadeAmt,
                        position: "absolute",
                        right: 0,
                        top: 28,
                      }}
                    />
                  ) : null}
                  <Img
                    src={staticFile(`features/${activeId}-light.svg`)}
                    style={{
                      bottom: 0,
                      left: 0,
                      objectFit: "contain",
                      objectPosition: "center top",
                      opacity: crossfadeAmt,
                      position: "absolute",
                      right: 0,
                      top: 28,
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
