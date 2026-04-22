import { AbsoluteFill, Easing, interpolate, useCurrentFrame } from "remotion";
import { HERO_HEADING } from "../Beat";
import type { BeatRenderProps } from "../scenes";

// URL reveal: segments fade in left-to-right, staggered with slight overlap.
// The full string occupies its natural inline width from frame 0, so the
// final URL ("optimuskg.ai/research") reads seamlessly with no gaps between
// segments — the entire string is centered as one block from the start.

interface Segment {
  text: string;
  startFrame: number;
  fadeFrames: number;
}

const SEGMENTS: Segment[] = [
  { text: "optimuskg", startFrame: 0, fadeFrames: 14 },
  { text: ".ai", startFrame: 7, fadeFrames: 12 },
  { text: "/", startFrame: 13, fadeFrames: 10 },
  { text: "research", startFrame: 18, fadeFrames: 14 },
];

const EASE = Easing.out(Easing.cubic);

export const ResearchUrl: React.FC<BeatRenderProps> = () => {
  const frame = useCurrentFrame();
  return (
    <AbsoluteFill
      className="bg-fd-background"
      style={{
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
      }}
    >
      <div
        className="text-fd-foreground"
        style={{
          ...HERO_HEADING,
          whiteSpace: "nowrap",
        }}
      >
        {SEGMENTS.map((seg) => {
          const opacity = interpolate(
            frame,
            [seg.startFrame, seg.startFrame + seg.fadeFrames],
            [0, 1],
            {
              extrapolateLeft: "clamp",
              extrapolateRight: "clamp",
              easing: EASE,
            },
          );
          return (
            <span key={seg.text} style={{ opacity }}>
              {seg.text}
            </span>
          );
        })}
      </div>
    </AbsoluteFill>
  );
};
