import {
  AbsoluteFill,
  Img,
  interpolate,
  spring,
  staticFile,
  useCurrentFrame,
  useVideoConfig,
} from "remotion";
import { HERO_HEADING } from "../Beat";
import type { BeatRenderProps } from "../scenes";

// Beat 4: hero text wipes in left-to-right, hold a beat, THEN push-up.
// Hero top:   50% → 28%
// Figure top: 110% → 42% (tight gap under hero)

const LIFT_START = 32;
const WIPE_START = 2;
const WIPE_DURATION = 26;

export const UnifiedSchema: React.FC<BeatRenderProps> = ({ heroText }) => {
  const frame = useCurrentFrame();
  const { fps } = useVideoConfig();

  const lift = spring({
    fps,
    frame: frame - LIFT_START,
    config: { damping: 18 },
    durationInFrames: 28,
  });

  const heroTopPct = interpolate(lift, [0, 1], [50, 28]);
  const figureTopPct = interpolate(lift, [0, 1], [110, 42]);

  // Left-to-right wipe: a linear-gradient mask whose reveal edge slides
  // past 100% so the soft edge clears the last character and the final
  // state is completely unmasked.
  const softEdge = 8;
  const wipeDone = frame >= WIPE_START + WIPE_DURATION;
  const wipe = interpolate(
    frame,
    [WIPE_START, WIPE_START + WIPE_DURATION],
    [0, 100 + softEdge],
    { extrapolateLeft: "clamp", extrapolateRight: "clamp" },
  );
  const maskImage = wipeDone
    ? undefined
    : `linear-gradient(to right, black ${wipe - softEdge}%, transparent ${wipe}%)`;

  return (
    <AbsoluteFill className="bg-fd-background">
      <div
        style={{
          position: "absolute",
          top: `${heroTopPct}%`,
          left: 0,
          right: 0,
          transform: "translateY(-50%)",
          textAlign: "center",
          padding: "0 1.25rem",
        }}
      >
        <h1
          className="text-fd-foreground"
          style={{
            ...HERO_HEADING,
            margin: 0,
            display: "inline-block",
            whiteSpace: "nowrap",
            WebkitMaskImage: maskImage,
            maskImage,
          }}
        >
          {heroText as string}
        </h1>
      </div>

      <Img
        src={staticFile("features/figure.webp")}
        style={{
          position: "absolute",
          top: `${figureTopPct}%`,
          left: "50%",
          transform: "translate(-50%, 0)",
          width: "62%",
          maxWidth: 1200,
          borderRadius: "0.625rem",
          border: "1px solid var(--color-fd-border)",
          boxShadow:
            "0px 28px 70px rgba(0,0,0,0.14), 0px 14px 32px rgba(0,0,0,0.1)",
        }}
      />
    </AbsoluteFill>
  );
};
