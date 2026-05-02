import type { CSSProperties } from "react";
import { useCurrentFrame } from "remotion";
import { fadeRamp, useStaggered } from "../animations";

// Per-word entry animation. All modes also fade opacity from `fadeFrom` → 1.
//   "fade"        — opacity only.
//   "rise"        — translateY rises from +distance to 0 (bottom-up).
//   "fall"        — translateY falls from -distance to 0 (top-down).
//   "slide-left"  — translateX slides from +distance to 0 (enters from right).
//   "slide-right" — translateX slides from -distance to 0 (enters from left).
//   "scale"       — scales from `scaleFrom` to 1.
export type WordMotion =
  | "fade"
  | "rise"
  | "fall"
  | "slide-left"
  | "slide-right"
  | "scale";

interface WordByWordTextProps {
  text: string;
  startFrame: number;
  wordDelay?: number;
  // Initial opacity per word before its ramp begins. Defaults to 0 so
  // unrevealed words are absent rather than pre-dimmed.
  fadeFrom?: number;
  // Per-word entry animation. Same mode applied to every word, or pass an
  // array indexed by word position for different motions per word.
  motion?: WordMotion | WordMotion[];
  // Distance for translate-based motions. Accepts any CSS length.
  distance?: string;
  // Starting scale for the "scale" motion.
  scaleFrom?: number;
  // Frames over which each word's opacity ramp completes.
  duration?: number;
  style?: CSSProperties;
}

function wordTransform(
  motion: WordMotion,
  distance: string,
  scaleFrom: number,
  progress: number,
): string | undefined {
  const inverse = 1 - progress;
  switch (motion) {
    case "fade":
      return undefined;
    case "rise":
      return `translateY(calc(${distance} * ${inverse}))`;
    case "fall":
      return `translateY(calc(-${distance} * ${inverse}))`;
    case "slide-left":
      return `translateX(calc(${distance} * ${inverse}))`;
    case "slide-right":
      return `translateX(calc(-${distance} * ${inverse}))`;
    case "scale": {
      const s = scaleFrom + (1 - scaleFrom) * progress;
      return `scale(${s})`;
    }
  }
}

export const WordByWordText: React.FC<WordByWordTextProps> = ({
  text,
  startFrame,
  wordDelay = 6,
  fadeFrom = 0,
  motion = "fade",
  distance = "0.5rem",
  scaleFrom = 0.85,
  duration,
  style,
}) => {
  const frame = useCurrentFrame();
  const elapsed = frame - startFrame;
  const words = text.split(" ");
  const lastIdx = words.length - 1;

  const motionAt = (i: number): WordMotion =>
    Array.isArray(motion) ? (motion[i] ?? motion[motion.length - 1]) : motion;

  return (
    <span style={style}>
      {words.map((word, i) => {
        // useStaggered is a pure helper, not a React hook — safe in a loop.
        // eslint-disable-next-line react-hooks/rules-of-hooks
        const local = useStaggered(elapsed, i, wordDelay);
        const progress = fadeRamp({
          frame: local,
          start: 0,
          duration,
          from: 0,
          to: 1,
        });
        const opacity = fadeFrom + (1 - fadeFrom) * progress;
        const transform = wordTransform(
          motionAt(i),
          distance,
          scaleFrom,
          progress,
        );
        return (
          <span
            key={i}
            style={{
              display: "inline-block",
              opacity,
              transform,
              whiteSpace: "pre",
            }}
          >
            {word}
            {i < lastIdx ? " " : ""}
          </span>
        );
      })}
    </span>
  );
};
