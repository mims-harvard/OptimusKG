import {
  AbsoluteFill,
  interpolate,
  spring,
  useCurrentFrame,
  useVideoConfig,
} from "remotion";
import { heroHeading } from "../../tokens";

// Beat: OptimusKG logo + wordmark.
// Phase 1 (0–~50):   five circles spring in from scattered positions to
//                    assemble the logo, then the four connecting lines fade
//                    in. Logo is on its own, vertically + horizontally
//                    centred.
// Phase 2 (90–120):  the wordmark "OptimusKG" grows in beside the logo.
//                    Because the logo + wordmark live in a centred flex row,
//                    the logo automatically shifts leftward as the wordmark
//                    claims layout space — final composition stays centred.
// Phase 3 (120+):    hold.
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

// Logo slides leftward into its final position over this window. The
// wordmark snaps to visible at the end (no per-character transition).
const SHIFT_START = 90;
const SHIFT_DURATION = 30;
const WORDMARK_FONT_SIZE = "7rem";
// Estimated natural width of the wordmark text at WORDMARK_FONT_SIZE. Used
// to compute the logo's compensating translateX so it appears centred when
// alone (phase 1) and shifts back to its layout-natural position (phase 2).
// Tune by eye if the final composition reads off-centre.
const WORDMARK_WIDTH_REM = 33;
const GAP_REM = 2;
// How far the logo needs to shift right so that, while alone, it sits at
// canvas centre — equal to half the (gap + wordmark) layout space the
// invisible wordmark already reserves on the right.
const SHIFT_PER_REM = (GAP_REM + WORDMARK_WIDTH_REM) / 2;

export const LogoMark: React.FC = () => {
  const frame = useCurrentFrame();
  const { fps } = useVideoConfig();

  // Logo slides from "centred alone" (+SHIFT_PER_REM) to "centred with
  // wordmark" (0) over the shift window.
  const logoTranslateX = interpolate(
    frame,
    [SHIFT_START, SHIFT_START + SHIFT_DURATION],
    [SHIFT_PER_REM, 0],
    { extrapolateLeft: "clamp", extrapolateRight: "clamp" },
  );
  // Wordmark snaps to visible once the logo has finished sliding into place.
  const wordmarkVisible = frame >= SHIFT_START + SHIFT_DURATION;

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
        style={{
          flexShrink: 0,
          overflow: "visible",
          transform: `translateX(${logoTranslateX}rem)`,
        }}
        viewBox="0 0 256 256"
        width={LOGO_PX}
      >
        {LINES.map((line, i) => {
          const lineOpacity = interpolate(
            frame,
            [28 + i * 2, 42 + i * 2],
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
            frame: frame - delay,
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

      {/* Wordmark — always in layout (so the centred flex row reserves the
          right amount of space from frame 0); snaps to visible after the
          logo finishes sliding into its left position. */}
      <span
        style={{
          ...heroHeading,
          color: STROKE,
          display: "inline-block",
          flexShrink: 0,
          fontSize: WORDMARK_FONT_SIZE,
          opacity: wordmarkVisible ? 1 : 0,
          whiteSpace: "nowrap",
        }}
      >
        OptimusKG
      </span>
    </AbsoluteFill>
  );
};
