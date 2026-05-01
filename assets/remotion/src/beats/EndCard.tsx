import {
  AbsoluteFill,
  interpolate,
  spring,
  useCurrentFrame,
  useVideoConfig,
} from "remotion";
import { HERO_HEADING } from "../Beat";
import { Snippet } from "../primitives";
import type { BeatRenderProps } from "../scenes";

// Beat 13 (198 frames — 48f animation + 150f frozen hold = 2.5s after animation):
//   0–36    Logo's 5 circles assemble from scattered positions.
//   24–44   Lines connecting the circles fade in.
//   28–44   "OptimusKG" wordmark fades in next to the logo (same row, centered).
//   36–56   "uv add optimuskg" snippet fades in beneath the row at 70% opacity.
//   48–96   Frozen — Twitter autoplay thumbnail.

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

const LOGO_SIZE = 140; // px

export const EndCard: React.FC<BeatRenderProps> = () => {
  const frame = useCurrentFrame();
  const { fps } = useVideoConfig();

  // Freeze everything from frame 48 onward (Twitter autoplay thumbnail).
  const t = Math.min(frame, 48);

  const wordmarkOpacity = interpolate(t, [28, 44], [0, 1], {
    extrapolateLeft: "clamp",
    extrapolateRight: "clamp",
  });
  const snippetOpacity = interpolate(t, [32, 48], [0, 1], {
    extrapolateLeft: "clamp",
    extrapolateRight: "clamp",
  });

  return (
    <AbsoluteFill
      className="bg-fd-background"
      style={{
        display: "flex",
        flexDirection: "column",
        alignItems: "center",
        justifyContent: "center",
      }}
    >
      {/* Group is shifted slightly upward to leave room for the snippet. */}
      <div
        style={{
          display: "flex",
          flexDirection: "column",
          alignItems: "center",
          gap: "3rem",
          transform: "translateY(-40px)",
        }}
      >
        {/* Logo + wordmark on a single row, centered. */}
        <div
          style={{
            display: "flex",
            alignItems: "center",
            gap: "1rem",
          }}
        >
          <svg
            fill="none"
            height={LOGO_SIZE}
            viewBox="0 0 256 256" style={{ overflow: "visible" }}
            width={LOGO_SIZE}
            xmlns="http://www.w3.org/2000/svg"
          >
            {LINES.map((line, i) => {
              const lineOpacity = interpolate(
                t,
                [24 + 2 * i, 44 + 2 * i],
                [0, 1],
                { extrapolateLeft: "clamp", extrapolateRight: "clamp" },
              );
              return (
                <line
                  key={i}
                  opacity={lineOpacity}
                  stroke="var(--color-fd-foreground)"
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  strokeWidth={16}
                  x1={line[0].x}
                  x2={line[1].x}
                  y1={line[0].y}
                  y2={line[1].y}
                />
              );
            })}
            {FINAL_CIRCLES.map((target, i) => {
              const delay = i * 3;
              const progress = spring({
                fps,
                frame: t - delay,
                config: { damping: 16, stiffness: 140 },
                durationInFrames: 36,
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
                  r={24}
                  stroke="var(--color-fd-foreground)"
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  strokeWidth={16}
                />
              );
            })}
          </svg>
          <span
            className="text-fd-foreground"
            style={{
              ...HERO_HEADING,
              fontSize: "5rem",
              opacity: wordmarkOpacity,
            }}
          >
            OptimusKG
          </span>
        </div>

        {/* `uv add optimuskg` snippet beneath the wordmark row. */}
        <div style={{ opacity: snippetOpacity }}>
          <Snippet size="lg" text="uv add optimuskg" />
        </div>
      </div>
    </AbsoluteFill>
  );
};
