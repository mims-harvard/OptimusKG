import { AbsoluteFill, Easing, interpolate, useCurrentFrame } from "remotion";
import { HERO_HEADING } from "../Beat";
import { GENE_FIELDS, SchemaTreeStatic } from "../primitives";
import type { BeatRenderProps } from "../scenes";

// Bottom-up scroll with simultaneous zoom-in. Stats stack rides the same
// scroll and exits top; the Gene-node SchemaTree rises from the bottom while
// scaling toward the frame width, ending filling the screen.

interface SchemaScrollProps extends BeatRenderProps {
  nodesText?: string;
  edgesText?: string;
  propertiesText?: string;
}

const HOLD = 0; // minimal hold — scroll starts almost immediately
const SCROLL = 120; // 4s @ 30fps
const TOTAL = HOLD + SCROLL;

const FRAME_W = 1920;
const FRAME_H = 1080;
const TREE_BASE_W = 1000; // at ZOOM_TO this becomes 1900, ≈ frame width
const STAT_OFFSET = 112;

const SCROLL_DISTANCE = 10000;
const ZOOM_FROM = 1;
const ZOOM_TO = 3;

const EASE = Easing.inOut(Easing.cubic);

export const SchemaScroll: React.FC<SchemaScrollProps> = ({
  nodesText = "190,531 nodes",
  edgesText = "21,813,816 edges",
  propertiesText = "110,276,843 properties",
}) => {
  const frame = useCurrentFrame();

  const scrollY = interpolate(frame, [HOLD, TOTAL], [0, SCROLL_DISTANCE], {
    extrapolateLeft: "clamp",
    extrapolateRight: "clamp",
    easing: EASE,
  });

  const zoom = interpolate(frame, [HOLD, TOTAL], [ZOOM_FROM, ZOOM_TO], {
    extrapolateLeft: "clamp",
    extrapolateRight: "clamp",
    easing: EASE,
  });

  const stats = [
    { text: nodesText, offsetY: -STAT_OFFSET },
    { text: edgesText, offsetY: 0 },
    { text: propertiesText, offsetY: STAT_OFFSET },
  ];

  // Tree top starts just below the frame; rides the same scroll upward.
  const treeTop = FRAME_H - scrollY;
  // Compute left so the scaled tree stays horizontally centered at every zoom.
  const scaledWidth = TREE_BASE_W * zoom;
  const treeLeft = (FRAME_W - scaledWidth) / 2;

  return (
    <AbsoluteFill className="bg-fd-background" style={{ overflow: "hidden" }}>
      {stats.map((s) => (
        <div
          key={s.text}
          className="text-fd-foreground"
          style={{
            ...HERO_HEADING,
            position: "absolute",
            left: 0,
            right: 0,
            top: FRAME_H / 2 + s.offsetY - scrollY,
            transform: "translateY(-50%)",
            textAlign: "center",
            whiteSpace: "nowrap",
          }}
        >
          {s.text}
        </div>
      ))}

      <div
        style={{
          position: "absolute",
          top: treeTop,
          left: treeLeft,
          width: TREE_BASE_W,
          transform: `scale(${zoom})`,
          transformOrigin: "0 0",
        }}
      >
        <SchemaTreeStatic fields={GENE_FIELDS} fontSize={18} />
      </div>
    </AbsoluteFill>
  );
};
