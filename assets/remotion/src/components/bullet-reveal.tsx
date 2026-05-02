import { useCurrentFrame, useVideoConfig } from "remotion";
import { fadeRamp, springIn } from "../animations";
import { fontSize, fontWeight, spacing } from "../tokens";

type FontSizeToken = keyof typeof fontSize;

interface BulletRevealProps {
  items: string[];
  // Frame at which the first item begins revealing.
  startFrame?: number;
  // Frames between consecutive item starts.
  interval?: number;
  // Frames over which each item's opacity ramp completes.
  itemDuration?: number;
  // Initial translateY offset that springs to 0.
  rise?: string;
  size?: FontSizeToken;
  // Spring damping for the per-item rise. Spec calls for 16.
  damping?: number;
}

export const BulletReveal: React.FC<BulletRevealProps> = ({
  items,
  startFrame = 0,
  interval = 30,
  itemDuration = 15,
  rise = "0.5rem",
  size = "body",
  damping = 16,
}) => {
  const frame = useCurrentFrame();
  const { fps } = useVideoConfig();

  return (
    <ul
      style={{
        display: "flex",
        flexDirection: "column",
        gap: spacing[1],
        listStyle: "none",
        margin: 0,
        padding: 0,
        textAlign: "center",
      }}
    >
      {items.map((item, i) => {
        const itemStart = startFrame + i * interval;
        const opacity = fadeRamp({
          frame,
          start: itemStart,
          duration: itemDuration,
          from: 0,
        });
        const t = springIn({
          frame,
          start: itemStart,
          fps,
          config: { damping },
        });
        return (
          <li
            key={i}
            style={{
              fontSize: fontSize[size],
              fontWeight: fontWeight.regular,
              lineHeight: 1.2,
              opacity,
              transform: `translateY(calc(${rise} * ${1 - t}))`,
            }}
          >
            {item}
          </li>
        );
      })}
    </ul>
  );
};
