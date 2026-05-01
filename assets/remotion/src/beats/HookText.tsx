import { interpolate, useCurrentFrame, useVideoConfig } from "remotion";
import { Beat, HERO_HEADING } from "../Beat";
import { RevealLine } from "../primitives";
import type { BeatRenderProps } from "../scenes";

// Beats 1, 2, 5, 11, 12: hero-only copy, centered.
// Layouts:
//   "default"           — standard word-by-word reveal
//   "exit-up"           — last ~12 frames slide up -120px and fade out
//   "enter-from-below"  — starts +60px below baseline, slides up in first 6 frames

export const HookText: React.FC<BeatRenderProps> = ({
  heroText,
  layout = "default",
}) => {
  const frame = useCurrentFrame();
  const { durationInFrames } = useVideoConfig();

  const lines = Array.isArray(heroText) ? heroText : [heroText ?? ""];

  let wrapperY = 0;
  let wrapperOpacity = 1;

  if (layout === "exit-up") {
    const exitStart = durationInFrames - 24;
    wrapperY = interpolate(frame, [exitStart, durationInFrames], [0, -120], {
      extrapolateLeft: "clamp",
      extrapolateRight: "clamp",
    });
    wrapperOpacity = interpolate(
      frame,
      [exitStart, durationInFrames - 4],
      [1, 0],
      { extrapolateLeft: "clamp", extrapolateRight: "clamp" },
    );
  }

  if (layout === "enter-from-below") {
    wrapperY = interpolate(frame, [0, 20], [60, 0], {
      extrapolateLeft: "clamp",
      extrapolateRight: "clamp",
    });
    wrapperOpacity = interpolate(frame, [0, 16], [0, 1], {
      extrapolateLeft: "clamp",
      extrapolateRight: "clamp",
    });
  }

  const hero = (
    <h1
      className="text-center text-fd-foreground"
      style={{
        ...HERO_HEADING,
        transform: `translateY(${wrapperY}px)`,
        opacity: wrapperOpacity,
      }}
    >
      {lines.map((line, i) => (
        <RevealLine
          key={i}
          perWordFrames={6}
          startFrame={4 + i * 16}
          style={{ display: "block" }}
          tokens={line.split(" ")}
        />
      ))}
    </h1>
  );

  return <Beat hero={hero} layout={layout} />;
};
