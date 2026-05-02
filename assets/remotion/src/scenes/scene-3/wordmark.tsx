import { AbsoluteFill, useCurrentFrame } from "remotion";
import { letterSpacingCollapse } from "../../animations";
import { fontSize, heroHeading } from "../../tokens";

// Beat: "OptimusKG" wordmark appears centred (taking the same spot the
// faded-out "Introducing" label vacated). Uses the heroHeading h1 token,
// overridden to display size for extra visual weight.

const ENTRY_START = 38;
const ENTRY_DURATION = 18;
const RISE = "1.5rem";

export const Wordmark: React.FC = () => {
  const frame = useCurrentFrame();
  const collapse = letterSpacingCollapse({
    frame,
    start: ENTRY_START,
    duration: ENTRY_DURATION,
    rise: RISE,
  });

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
        style={{
          ...heroHeading,
          ...collapse,
          fontSize: fontSize.display,
        }}
      >
        OptimusKG
      </h1>
    </AbsoluteFill>
  );
};
