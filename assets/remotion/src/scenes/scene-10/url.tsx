import { AbsoluteFill, interpolate, useCurrentFrame } from "remotion";
import { heroHeading } from "../../tokens";

// Beat: URL "optimuskg.ai/docs" centred. Each segment slides in from the
// right (translateX +SLIDE_REM → 0) with an opacity ramp. All three are in
// inline layout from frame 0 so the final phrase reads as one centred
// block with normal letter spacing.

const COLOR = "#0f172a";
const SLIDE_REM = 2;
const FADE_FRAMES = 16;

const SEGMENTS = [
  { text: "optimuskg", startFrame: 0 },
  { text: ".ai", startFrame: 12 },
  { text: "/docs", startFrame: 24 },
];

export const Url: React.FC = () => {
  const frame = useCurrentFrame();

  return (
    <AbsoluteFill
      style={{
        alignItems: "center",
        display: "flex",
        flexDirection: "row",
        justifyContent: "center",
        paddingInline: "8rem",
      }}
    >
      <h1 style={{ ...heroHeading, color: COLOR, whiteSpace: "nowrap" }}>
        {SEGMENTS.map((seg) => {
          const t = interpolate(
            frame,
            [seg.startFrame, seg.startFrame + FADE_FRAMES],
            [0, 1],
            { extrapolateLeft: "clamp", extrapolateRight: "clamp" },
          );
          return (
            <span
              key={seg.text}
              style={{
                display: "inline-block",
                opacity: t,
                transform: `translateX(${SLIDE_REM * (1 - t)}rem)`,
              }}
            >
              {seg.text}
            </span>
          );
        })}
      </h1>
    </AbsoluteFill>
  );
};
