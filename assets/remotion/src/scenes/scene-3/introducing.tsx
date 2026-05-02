import { AbsoluteFill, interpolate, useCurrentFrame } from "remotion";
import { letterSpacingCollapse } from "../../animations";
import { heroHeading } from "../../tokens";

// Beat: "Introducing" appears at canvas centre, holds, then fades out so
// the wordmark can take the same spot.

const ENTRY_DURATION = 18;
const FADE_OUT_START = 28;
const FADE_OUT_DURATION = 10;

export const Introducing: React.FC = () => {
  const frame = useCurrentFrame();
  const collapse = letterSpacingCollapse({
    frame,
    start: 0,
    duration: ENTRY_DURATION,
  });
  const fadeOut = interpolate(
    frame,
    [FADE_OUT_START, FADE_OUT_START + FADE_OUT_DURATION],
    [1, 0],
    { extrapolateLeft: "clamp", extrapolateRight: "clamp" },
  );

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
      <h1
        style={{ ...heroHeading, ...collapse, opacity: collapse.opacity * fadeOut }}
      >
        Introducing
      </h1>
    </AbsoluteFill>
  );
};
