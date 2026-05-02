import {
  AbsoluteFill,
  interpolate,
  spring,
  useCurrentFrame,
  useVideoConfig,
} from "remotion";
import { heroHeading } from "../../tokens";

// Beat: OptimusKG logo + wordmark.
// Phase 1 (0–~12):    "OptimusKG" wordmark fades in at its final position
//                     on the right of the (centred) logo+wordmark row.
// Phase 2 (30–~80):   the five logo circles spring in from scattered
//                     positions to assemble the logo at the LEFT of the
//                     wordmark, then the four connecting lines fade in.
//                     Both elements are at their final layout positions
//                     from frame 0 — no horizontal sliding.
// Phase 3 (80+):      hold.
//
// Logo geometry copied from
//   https://github.com/mims-harvard/OptimusKG/blob/remotion/assets/remotion/src/beats/Wordmark.tsx
// (FINAL_CIRCLES / LINES / SCATTER_OFFSETS).

type Point = { x: number; y: number };

const FINAL_CIRCLES: Point[] = [
  { x: 128, y: 128 },
  { x: 96, y: 56 },
  { x: 200, y: 104 },
  { x: 200, y: 184 },
  { x: 56, y: 192 },
];

const LINES: Array<[Point, Point]> = [
  [{ x: 118.25, y: 106.07 }, { x: 105.75, y: 77.93 }],
  [{ x: 177.23, y: 111.59 }, { x: 150.77, y: 120.41 }],
  [{ x: 181.06, y: 169.27 }, { x: 146.94, y: 142.73 }],
  [{ x: 110.06, y: 143.94 }, { x: 73.94, y: 176.06 }],
];

const SCATTER_OFFSETS: Point[] = [
  { x: 0, y: -90 },
  { x: -120, y: -30 },
  { x: 140, y: -70 },
  { x: 110, y: 120 },
  { x: -150, y: 80 },
];

const STROKE = "#0f172a";
const STROKE_WIDTH = 16;
const CIRCLE_RADIUS = 24;
const LOGO_PX = 240;

const WORDMARK_FONT_SIZE = "7rem";
const GAP_REM = 2;

// Wordmark fades in first.
const WORDMARK_FADE_DURATION = 12;
// Logo assembly starts a few frames after the wordmark settles, so the
// viewer reads the text first.
const LOGO_START = 24;

export const LogoMark: React.FC = () => {
  const frame = useCurrentFrame();
  const { fps } = useVideoConfig();

  const wordmarkOpacity = interpolate(
    frame,
    [0, WORDMARK_FADE_DURATION],
    [0, 1],
    { extrapolateLeft: "clamp", extrapolateRight: "clamp" },
  );

  return (
    <AbsoluteFill
      style={{
        alignItems: "center",
        display: "flex",
        flexDirection: "row",
        gap: `${GAP_REM}rem`,
        justifyContent: "center",
      }}
    >
      <svg
        fill="none"
        height={LOGO_PX}
        style={{ flexShrink: 0, overflow: "visible" }}
        viewBox="0 0 256 256"
        width={LOGO_PX}
      >
        {LINES.map((line, i) => {
          const lineOpacity = interpolate(
            frame,
            [LOGO_START + 28 + i * 2, LOGO_START + 42 + i * 2],
            [0, 1],
            { extrapolateLeft: "clamp", extrapolateRight: "clamp" },
          );
          return (
            <line
              key={i}
              opacity={lineOpacity}
              stroke={STROKE}
              strokeLinecap="round"
              strokeLinejoin="round"
              strokeWidth={STROKE_WIDTH}
              x1={line[0].x}
              x2={line[1].x}
              y1={line[0].y}
              y2={line[1].y}
            />
          );
        })}
        {FINAL_CIRCLES.map((target, i) => {
          const delay = i * 2;
          const progress = spring({
            fps,
            frame: frame - LOGO_START - delay,
            config: { damping: 16, stiffness: 120 },
            durationInFrames: 30,
          });
          const dx = interpolate(progress, [0, 1], [SCATTER_OFFSETS[i].x, 0]);
          const dy = interpolate(progress, [0, 1], [SCATTER_OFFSETS[i].y, 0]);
          const opacity = interpolate(progress, [0, 0.4], [0, 1], {
            extrapolateRight: "clamp",
          });
          return (
            <circle
              cx={target.x + dx}
              cy={target.y + dy}
              fill="none"
              key={i}
              opacity={opacity}
              r={CIRCLE_RADIUS}
              stroke={STROKE}
              strokeLinecap="round"
              strokeLinejoin="round"
              strokeWidth={STROKE_WIDTH}
            />
          );
        })}
      </svg>

      {/* Wordmark — fades in first, at its final layout position. */}
      <span
        style={{
          ...heroHeading,
          color: STROKE,
          display: "inline-block",
          flexShrink: 0,
          fontSize: WORDMARK_FONT_SIZE,
          opacity: wordmarkOpacity,
          whiteSpace: "nowrap",
        }}
      >
        OptimusKG
      </span>
    </AbsoluteFill>
  );
};
