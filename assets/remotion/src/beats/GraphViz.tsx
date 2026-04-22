import { AbsoluteFill, interpolate, useCurrentFrame } from "remotion";
import { HERO_HEADING } from "../Beat";
import { RevealLine } from "../primitives";
import type { BeatRenderProps } from "../scenes";

// Beats 6 & 7: the same deterministic graph.
// phase="nodes" — 200 nodes clustered by entity type, fading in.
// phase="edges" — nodes present, ~40 inter-cluster edges draw in varied colors.

const SEED = 42;
const CLUSTERS = 10;
const NODES_PER_CLUSTER = 20;
const TOTAL_NODES = CLUSTERS * NODES_PER_CLUSTER;
const EDGES_COUNT = 40;

// Canvas used for precomputed positions (center of 1920×1080).
const VIEWBOX_W = 1400;
const VIEWBOX_H = 660;

function mulberry32(seed: number) {
  let s = seed >>> 0;
  return () => {
    s = (s + 0x6d2b79f5) >>> 0;
    let t = s;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

type Node = {
  x: number;
  y: number;
  cluster: number;
  r: number;
};

type Edge = {
  a: number;
  b: number;
  hue: number;
};

const NODES: Node[] = (() => {
  const rand = mulberry32(SEED);
  const result: Node[] = [];
  // Lay out 10 cluster centers on two rows of 5.
  const centers: Array<{ cx: number; cy: number }> = [];
  for (let i = 0; i < CLUSTERS; i++) {
    const col = i % 5;
    const row = Math.floor(i / 5);
    const cx = VIEWBOX_W * (0.12 + col * 0.19) + (rand() - 0.5) * 40;
    const cy = VIEWBOX_H * (0.28 + row * 0.44) + (rand() - 0.5) * 30;
    centers.push({ cx, cy });
  }
  for (let c = 0; c < CLUSTERS; c++) {
    for (let i = 0; i < NODES_PER_CLUSTER; i++) {
      const angle = rand() * Math.PI * 2;
      const dist = Math.pow(rand(), 0.55) * 78;
      result.push({
        x: centers[c].cx + Math.cos(angle) * dist,
        y: centers[c].cy + Math.sin(angle) * dist,
        cluster: c,
        r: 4 + rand() * 4,
      });
    }
  }
  return result;
})();

const EDGES: Edge[] = (() => {
  const rand = mulberry32(SEED + 1);
  const result: Edge[] = [];
  for (let i = 0; i < EDGES_COUNT; i++) {
    // Bias: most edges connect different clusters.
    const a = Math.floor(rand() * TOTAL_NODES);
    let b = Math.floor(rand() * TOTAL_NODES);
    if (NODES[a].cluster === NODES[b].cluster && rand() > 0.2) {
      b = Math.floor(rand() * TOTAL_NODES);
    }
    if (a === b) b = (a + 7) % TOTAL_NODES;
    const hue = Math.floor(rand() * 360);
    result.push({ a, b, hue });
  }
  return result;
})();

const CLUSTER_HUES = [5, 40, 80, 120, 165, 200, 235, 275, 310, 345];

export const GraphViz: React.FC<BeatRenderProps> = ({ heroText, ...rest }) => {
  const phase = (rest as { phase?: "nodes" | "edges" }).phase ?? "nodes";
  const frame = useCurrentFrame();

  return (
    <AbsoluteFill
      className="bg-fd-background"
      style={{
        display: "flex",
        flexDirection: "column",
        alignItems: "center",
        justifyContent: "center",
        gap: "2rem",
        padding: "3rem 1.25rem",
      }}
    >
      <h2
        className="text-center text-fd-foreground"
        style={{ ...HERO_HEADING, fontSize: "5.5rem" }}
      >
        <RevealLine
          perWordFrames={3}
          startFrame={2}
          style={{ display: "block" }}
          tokens={(heroText as string).split(" ")}
        />
      </h2>

      <svg
        height={660}
        style={{ maxWidth: "100%" }}
        viewBox={`0 0 ${VIEWBOX_W} ${VIEWBOX_H}`}
        width={1400}
      >
        {phase === "edges" &&
          EDGES.map((e, i) => {
            const a = NODES[e.a];
            const b = NODES[e.b];
            const draw = interpolate(frame - (2 + i * 0.6), [0, 18], [0, 1], {
              extrapolateLeft: "clamp",
              extrapolateRight: "clamp",
            });
            const dx = b.x - a.x;
            const dy = b.y - a.y;
            return (
              <line
                key={i}
                opacity={0.7}
                stroke={`hsl(${e.hue} 70% 45%)`}
                strokeLinecap="round"
                strokeWidth={1.4}
                x1={a.x}
                x2={a.x + dx * draw}
                y1={a.y}
                y2={a.y + dy * draw}
              />
            );
          })}

        {NODES.map((n, i) => {
          const nodeAppear =
            phase === "nodes"
              ? interpolate(frame - (i * 0.15), [0, 14], [0, 1], {
                  extrapolateLeft: "clamp",
                  extrapolateRight: "clamp",
                })
              : 1;
          const color = `hsl(${CLUSTER_HUES[n.cluster]} 70% 45%)`;
          return (
            <circle
              cx={n.x}
              cy={n.y}
              fill={color}
              fillOpacity={0.85}
              key={i}
              opacity={nodeAppear}
              r={n.r * nodeAppear}
              stroke={color}
              strokeOpacity={0.3}
              strokeWidth={1}
            />
          );
        })}
      </svg>
    </AbsoluteFill>
  );
};
