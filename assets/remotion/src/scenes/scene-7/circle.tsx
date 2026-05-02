import {
  AbsoluteFill,
  interpolate,
  useCurrentFrame,
  useVideoConfig,
} from "remotion";
import { springIn } from "../../animations";

// Beat: thin circle behind the heading. The circle scales in from a small
// radius to its final size, then dots spring in around the circumference.
// Once the dots are visible the whole group rotates slowly clockwise.

const RADIUS = 460;
const STROKE_WIDTH = 2.5;
const STROKE_COLOR = "#0f172a";

const DOT_RADIUS = 14;
const DOT_STROKE = 3;
// Equidistant dots around the circle (count = 5 → 72° apart). Starts at the
// top (-90°) so the first dot sits at 12 o'clock.
const DOT_COUNT = 5;
const DOT_ANGLES = Array.from(
  { length: DOT_COUNT },
  (_, i) => -90 + (360 / DOT_COUNT) * i,
);

const CIRCLE_ENTER_START = 0;
const DOT_ENTER_START = 28;
const DOT_STAGGER = 5;

// Frames for one full clockwise rotation. Slower = more contemplative.
const ROTATION_PERIOD_FRAMES = 600;

// Whole circle (and dots) fades out before the PaperQA3 window arrives.
const FADE_OUT_START = 100;
const FADE_OUT_DURATION = 24;

export const Circle: React.FC = () => {
  const frame = useCurrentFrame();
  const { fps, width, height } = useVideoConfig();

  const cx = width / 2;
  const cy = height / 2;

  // Circle scales from tiny → full size, snappy spring.
  const circleT = springIn({
    frame,
    start: CIRCLE_ENTER_START,
    fps,
    config: { damping: 14, stiffness: 110, mass: 1 },
  });
  const r = RADIUS * circleT;

  const rotation = (frame / ROTATION_PERIOD_FRAMES) * 360;

  const groupOpacity = interpolate(
    frame,
    [FADE_OUT_START, FADE_OUT_START + FADE_OUT_DURATION],
    [1, 0],
    { extrapolateLeft: "clamp", extrapolateRight: "clamp" },
  );

  if (groupOpacity <= 0) return null;

  return (
    <AbsoluteFill style={{ opacity: groupOpacity, pointerEvents: "none" }}>
      <svg height={height} viewBox={`0 0 ${width} ${height}`} width={width}>
        <g transform={`rotate(${rotation} ${cx} ${cy})`}>
          <circle
            cx={cx}
            cy={cy}
            fill="none"
            r={r}
            stroke={STROKE_COLOR}
            strokeWidth={STROKE_WIDTH}
          />
          {DOT_ANGLES.map((deg, i) => {
            const dotT = springIn({
              frame,
              start: DOT_ENTER_START + i * DOT_STAGGER,
              fps,
              config: { damping: 12, stiffness: 180, mass: 1 },
            });
            if (dotT <= 0) return null;
            const a = (deg * Math.PI) / 180;
            return (
              <circle
                cx={cx + Math.cos(a) * r}
                cy={cy + Math.sin(a) * r}
                fill="white"
                key={i}
                opacity={dotT}
                r={DOT_RADIUS * dotT}
                stroke={STROKE_COLOR}
                strokeWidth={DOT_STROKE}
              />
            );
          })}
        </g>
      </svg>
    </AbsoluteFill>
  );
};
