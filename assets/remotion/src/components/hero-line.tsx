import { useCurrentFrame } from "remotion";
import { letterSpacingCollapse } from "../animations";
import { fontSize, fontWeight } from "../tokens";

type FontSizeToken = keyof typeof fontSize;

interface HeroLineProps {
  text: string;
  startFrame: number;
  // Type-scale token. Defaults to `hero`.
  size?: FontSizeToken;
  // Initial translateY offset that collapses to 0. Defaults to "1.5rem".
  rise?: string;
  // Frames over which the collapse runs. Defaults to the primitive's 18.
  duration?: number;
}

export const HeroLine: React.FC<HeroLineProps> = ({
  text,
  startFrame,
  size = "hero",
  rise,
  duration,
}) => {
  const frame = useCurrentFrame();
  const collapse = letterSpacingCollapse({
    frame,
    start: startFrame,
    rise,
    duration,
  });

  return (
    <span
      style={{
        ...collapse,
        display: "inline-block",
        fontSize: fontSize[size],
        fontWeight: fontWeight.medium,
        lineHeight: 1.08,
      }}
    >
      {text}
    </span>
  );
};
