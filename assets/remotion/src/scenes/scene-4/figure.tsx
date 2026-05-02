import {
  AbsoluteFill,
  Img,
  interpolate,
  staticFile,
  useCurrentFrame,
  useVideoConfig,
} from "remotion";
import { springIn } from "../../animations";
import { Sfx } from "../../sounds/sfx";

// Beat: schema figure (public/images/schema.webp). Final position is
// vertically centred. Rises from off-screen below into centre, pushing the
// heading up as it goes.
//
// Phase 1 (0–90):     off-screen below, invisible.
// Phase 2 (90–150):   spring-rises from +75vh → 0vh, opacity 0 → 1.
// Phase 3 (150+):     hold centred until the scene dissolves.

const ENTER_START = 90;
const ENTER_FROM_VH = 75;

export const Figure: React.FC = () => {
  const frame = useCurrentFrame();
  const { fps } = useVideoConfig();

  const t = springIn({
    frame,
    start: ENTER_START,
    fps,
    config: { damping: 18, stiffness: 90, mass: 1 },
  });
  const y = ENTER_FROM_VH * (1 - t);
  const opacity = interpolate(t, [0, 0.4], [0, 1], {
    extrapolateLeft: "clamp",
    extrapolateRight: "clamp",
  });

  return (
    <AbsoluteFill
      style={{
        alignItems: "center",
        display: "flex",
        justifyContent: "center",
        paddingInline: "8rem",
      }}
    >
      <Sfx at={ENTER_START} sound="swoosh" />
      {/* Fan-out swoosh — fires when the scene wrapper begins its
          18-frame dissolve (scene-4 duration 210 → exit starts at 192). */}
      <Sfx at={192} sound="swoosh" />
      <Img
        src={staticFile("images/schema.webp")}
        style={{
          borderRadius: "1rem",
          boxShadow:
            "0 24px 60px rgba(15, 23, 42, 0.1), 0 8px 18px rgba(15, 23, 42, 0.05)",
          maxWidth: "60rem",
          opacity,
          transform: `translateY(${y}vh)`,
          width: "100%",
        }}
      />
    </AbsoluteFill>
  );
};
