import { AbsoluteFill, Easing, interpolate, useCurrentFrame } from "remotion";
import { HERO_HEADING } from "../Beat";
import type { BeatRenderProps } from "../scenes";

// 102-frame beat (tighter than before):
//   0–10     Nodes fades in centered.
//   10–26    Hold nodes alone.
//   26–44    Nodes → -L; edges enters from below → +L (2-line stack).
//   44–58    Hold 2-line stack.
//   58–76    Nodes → -2L; edges → 0; properties enters → +2L (3-line stack).
//   76–102   Hold 3-line stack.

interface NodesEdgesProps extends BeatRenderProps {
  nodesText?: string;
  edgesText?: string;
  propertiesText?: string;
}

const EASE = Easing.bezier(0.16, 1, 0.3, 1);
const L = 56;

const NODES_FADE_IN: [number, number] = [0, 10];
const TO_2_STACK: [number, number] = [26, 44];
const EDGES_FADE_IN: [number, number] = [30, 44];
const TO_3_STACK: [number, number] = [58, 76];
const PROPS_FADE_IN: [number, number] = [62, 76];
const ENTER_FROM_BELOW = 56;

export const NodesEdges: React.FC<NodesEdgesProps> = ({
  nodesText = "190,531 nodes",
  edgesText = "21,813,816 edges",
  propertiesText,
}) => {
  const frame = useCurrentFrame();

  const nodesOpacity = interpolate(frame, NODES_FADE_IN, [0, 1], {
    extrapolateLeft: "clamp",
    extrapolateRight: "clamp",
    easing: EASE,
  });
  const nodesY1 = interpolate(frame, TO_2_STACK, [0, -L], {
    extrapolateLeft: "clamp",
    extrapolateRight: "clamp",
    easing: EASE,
  });
  const nodesY2 = interpolate(frame, TO_3_STACK, [0, -L], {
    extrapolateLeft: "clamp",
    extrapolateRight: "clamp",
    easing: EASE,
  });
  const nodesY = nodesY1 + nodesY2;

  const edgesOpacity = interpolate(frame, EDGES_FADE_IN, [0, 1], {
    extrapolateLeft: "clamp",
    extrapolateRight: "clamp",
    easing: EASE,
  });
  const edgesY1 = interpolate(
    frame,
    TO_2_STACK,
    [L + ENTER_FROM_BELOW, L],
    {
      extrapolateLeft: "clamp",
      extrapolateRight: "clamp",
      easing: EASE,
    },
  );
  const edgesY2 = interpolate(frame, TO_3_STACK, [0, -L], {
    extrapolateLeft: "clamp",
    extrapolateRight: "clamp",
    easing: EASE,
  });
  const edgesY = edgesY1 + edgesY2;

  const propertiesOpacity = interpolate(frame, PROPS_FADE_IN, [0, 1], {
    extrapolateLeft: "clamp",
    extrapolateRight: "clamp",
    easing: EASE,
  });
  const propertiesY = interpolate(
    frame,
    TO_3_STACK,
    [2 * L + ENTER_FROM_BELOW, 2 * L],
    {
      extrapolateLeft: "clamp",
      extrapolateRight: "clamp",
      easing: EASE,
    },
  );

  return (
    <AbsoluteFill className="bg-fd-background">
      <div
        style={{
          position: "absolute",
          top: "50%",
          left: 0,
          right: 0,
          transform: "translateY(-50%)",
          textAlign: "center",
        }}
      >
        <StackLine opacity={nodesOpacity} text={nodesText} y={nodesY} />
        <StackLine opacity={edgesOpacity} text={edgesText} y={edgesY} />
        {propertiesText && (
          <StackLine
            opacity={propertiesOpacity}
            text={propertiesText}
            y={propertiesY}
          />
        )}
      </div>
    </AbsoluteFill>
  );
};

const StackLine: React.FC<{ text: string; y: number; opacity: number }> = ({
  text,
  y,
  opacity,
}) => (
  <div
    className="text-fd-foreground"
    style={{
      ...HERO_HEADING,
      position: "absolute",
      left: 0,
      right: 0,
      top: 0,
      transform: `translateY(calc(-50% + ${y}px))`,
      opacity,
      whiteSpace: "nowrap",
    }}
  >
    {text}
  </div>
);
