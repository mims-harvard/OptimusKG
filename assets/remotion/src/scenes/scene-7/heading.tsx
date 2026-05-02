import { AbsoluteFill, interpolate, useCurrentFrame } from "remotion";
import { heroHeading } from "../../tokens";

// Beat: heading centred, full opacity. Fades out at frame 100 to make way
// for the PaperQA3 analysis window.

const FADE_OUT_START = 100;
const FADE_OUT_DURATION = 24;

export const Heading: React.FC = () => {
  const frame = useCurrentFrame();
  const opacity = interpolate(
    frame,
    [FADE_OUT_START, FADE_OUT_START + FADE_OUT_DURATION],
    [1, 0],
    {
      extrapolateLeft: "clamp",
      extrapolateRight: "clamp",
    },
  );

  return (
    <AbsoluteFill
      style={{
        alignItems: "center",
        display: "flex",
        justifyContent: "center",
        paddingInline: "8rem",
      }}
    >
      <h1
        style={{
          ...heroHeading,
          fontSize: "2.9rem",
          maxWidth: "45rem",
          opacity,
        }}
      >
        Independently validated with a multimodal agent that reasons over
        millions of scientific articles
      </h1>
    </AbsoluteFill>
  );
};
