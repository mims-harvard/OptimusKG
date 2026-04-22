import {
  AbsoluteFill,
  interpolate,
  spring,
  useCurrentFrame,
  useVideoConfig,
} from "remotion";
import { Logo, Snippet } from "../primitives";
import type { BeatRenderProps } from "../scenes";

// Beat 13 (48 frames): logo springs in, snippet fades in beneath at 70% opacity.
// Last 24 frames: zero animation (Twitter autoplay thumbnail).

export const EndCard: React.FC<BeatRenderProps> = () => {
  const frame = useCurrentFrame();
  const { fps } = useVideoConfig();

  // Freeze everything from frame 24 onward.
  const t = Math.min(frame, 24);

  const scaleSpring = spring({
    fps,
    frame: t,
    config: { damping: 18 },
    durationInFrames: 12,
  });
  const scale = interpolate(scaleSpring, [0, 1], [0.94, 1]);
  const logoOpacity = interpolate(t, [0, 8], [0, 1], {
    extrapolateLeft: "clamp",
    extrapolateRight: "clamp",
  });
  const snippetOpacity = interpolate(t, [18, 30], [0, 0.7], {
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
        gap: "2.5rem",
      }}
    >
      <div
        style={{
          transform: `scale(${scale})`,
          opacity: logoOpacity,
          transformOrigin: "center",
        }}
      >
        <Logo size={140} />
      </div>
      <div style={{ opacity: snippetOpacity }}>
        <Snippet size="lg" text="uv add optimuskg" />
      </div>
    </AbsoluteFill>
  );
};
