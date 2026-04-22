import type { ReactNode } from "react";
import { AbsoluteFill } from "remotion";
import type { BeatLayout } from "./scenes";

export const CONTAINER_MAX = 1300;
export const GUTTER = 20;

export const HERO_HEADING: React.CSSProperties = {
  fontSize: "3rem",
  lineHeight: 1.08,
  letterSpacing: "-0.03em",
  fontWeight: 500,
  margin: 0,
};

export const Beat: React.FC<{
  hero?: ReactNode;
  visual?: ReactNode;
  layout?: BeatLayout;
  containerStyle?: React.CSSProperties;
  fill?: ReactNode;
}> = ({ hero, visual, layout = "default", containerStyle, fill }) => {
  if (fill) {
    return <AbsoluteFill className="bg-fd-background">{fill}</AbsoluteFill>;
  }

  // Exit-up / enter-from-below still center the hero — the component applies
  // its own translateY on top of the centered baseline.
  const verticalAlign = layout === "push-up" ? "flex-start" : "center";

  return (
    <AbsoluteFill
      className="bg-fd-background"
      style={{
        display: "flex",
        flexDirection: "column",
        justifyContent: verticalAlign,
        paddingLeft: GUTTER,
        paddingRight: GUTTER,
      }}
    >
      <div
        style={{
          width: "100%",
          maxWidth: CONTAINER_MAX,
          marginInline: "auto",
          display: "flex",
          flexDirection: "column",
          alignItems: "center",
          gap: "2.5rem",
          ...containerStyle,
        }}
      >
        {hero}
        {visual}
      </div>
    </AbsoluteFill>
  );
};
