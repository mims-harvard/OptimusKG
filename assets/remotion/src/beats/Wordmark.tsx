import {
  AbsoluteFill,
  interpolate,
  spring,
  useCurrentFrame,
  useVideoConfig,
} from "remotion";
import { HERO_HEADING } from "../Beat";
import { RevealLine } from "../primitives";
import type { BeatRenderProps } from "../scenes";

// Beat 3: the 5 logo circles assemble from scattered positions, then the 4
// lines fade in. The word "OptimusKG" appears via RevealLine.

type Point = { x: number; y: number };

const FINAL_CIRCLES: Point[] = [
  { x: 128, y: 128 },
  { x: 96, y: 56 },
  { x: 200, y: 104 },
  { x: 200, y: 184 },
  { x: 56, y: 192 },
];

const LINES: Array<[Point, Point]> = [
  [
    { x: 118.25, y: 106.07 },
    { x: 105.75, y: 77.93 },
  ],
  [
    { x: 177.23, y: 111.59 },
    { x: 150.77, y: 120.41 },
  ],
  [
    { x: 181.06, y: 169.27 },
    { x: 146.94, y: 142.73 },
  ],
  [
    { x: 110.06, y: 143.94 },
    { x: 73.94, y: 176.06 },
  ],
];

// Deterministic scatter — radial offsets per circle index.
const SCATTER_OFFSETS: Point[] = [
  { x: 0, y: -90 },
  { x: -120, y: -30 },
  { x: 140, y: -70 },
  { x: 110, y: 120 },
  { x: -150, y: 80 },
];

export const Wordmark: React.FC<BeatRenderProps> = ({ heroText }) => {
  const frame = useCurrentFrame();
  const { fps } = useVideoConfig();

  return (
    <AbsoluteFill
      className="bg-fd-background"
      style={{
        display: "flex",
        flexDirection: "column",
        alignItems: "center",
        justifyContent: "center",
        gap: "1.5rem",
      }}
    >
      <svg
        fill="none"
        height={320}
        viewBox="0 0 256 256"
        width={320}
        xmlns="http://www.w3.org/2000/svg"
      >
        {LINES.map((line, i) => {
          const lineOpacity = interpolate(frame, [28 + i * 2, 42 + i * 2], [0, 1], {
            extrapolateLeft: "clamp",
            extrapolateRight: "clamp",
          });
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
              r={24}
              stroke="var(--color-fd-foreground)"
              strokeLinecap="round"
              strokeLinejoin="round"
              strokeWidth={16}
            />
          );
        })}
      </svg>
      <h1
        className="text-center text-fd-foreground"
        style={{ ...HERO_HEADING, fontSize: "5rem" }}
      >
        <RevealLine
          perWordFrames={6}
          startFrame={36}
          style={{ display: "block" }}
          tokens={[heroText ?? "OptimusKG"]}
        />
      </h1>
    </AbsoluteFill>
  );
};
