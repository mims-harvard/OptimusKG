import { AbsoluteFill, interpolate, useCurrentFrame } from "remotion";
import { fadeRamp } from "../../animations";

// Beat: "uv add optimuskg" snippet card anchored at vertical 60%.
// Slides up + fades in after the logo + wordmark have settled.

const ENTRY_START = 100;
const ENTRY_DURATION = 24;
const RISE_REM = 1.5;

export const Snippet: React.FC = () => {
  const frame = useCurrentFrame();

  const opacity = fadeRamp({
    frame,
    start: ENTRY_START,
    duration: ENTRY_DURATION,
    from: 0,
    to: 1,
  });
  const ty = interpolate(
    frame,
    [ENTRY_START, ENTRY_START + ENTRY_DURATION],
    [RISE_REM, 0],
    { extrapolateLeft: "clamp", extrapolateRight: "clamp" },
  );

  return (
    <AbsoluteFill
      style={{
        alignItems: "center",
        display: "flex",
        flexDirection: "row",
        justifyContent: "center",
        paddingTop: "60vh",
      }}
    >
      <div
        style={{
          alignItems: "center",
          background: "#ffffff",
          border: "1px solid rgba(15, 23, 42, 0.15)",
          borderRadius: 4,
          color: "#0f172a",
          display: "inline-flex",
          fontFamily:
            "ui-monospace, SFMono-Regular, 'SF Mono', Menlo, Consolas, monospace",
          fontSize: "2.5rem",
          fontVariantLigatures: "none",
          opacity,
          padding: "1.25rem 2rem",
          transform: `translateY(${ty}rem)`,
        }}
      >
        <span style={{ opacity: 0.7 }}>$&nbsp;</span>
        <span>uv add optimuskg</span>
      </div>
    </AbsoluteFill>
  );
};
