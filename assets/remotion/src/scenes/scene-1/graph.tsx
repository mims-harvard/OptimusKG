import { useMemo } from "react";
import { AbsoluteFill, useCurrentFrame, useVideoConfig } from "remotion";
import { lineDraw, springIn } from "../../animations";
import { mulberry32 } from "../../animations/internal";

// Beat: knowledge-graph behind the hero text. Big nodes pop in around the
// canvas (centre kept clear for the text); edges between them draw in once
// both endpoints have landed. Force-connected so there are no orphan
// clusters; positions extend past the canvas so the graph reads as
// continuing offscreen.

// ─── Tunables ────────────────────────────────────────────────────────────
const START_FRAME = 60;
const COUNT = 100;
const SEED = 2;
const NODE_STAGGER = 1;
const NODE_DURATION = 24;
const EDGE_DELAY = 4;
const EDGE_DURATION = 24;
const EDGE_DEGREE = 4;
const NODE_RADIUS = 18;
// Negative inset → nodes can extend past the canvas edges.
const INSET = -80;
// Keep-out rectangle behind the hero text (canvas 1920×1080).
const TEXT_EXCLUSION = { x: 360, y: 360, width: 1200, height: 360 };
const MAX_OPACITY = 0.22;
const NODE_COLOR = "#0f172a";
const EDGE_COLOR = "#0f172a";
const EDGE_WIDTH = 3;

// ─── Graph generation ────────────────────────────────────────────────────

interface Rect {
  x: number;
  y: number;
  width: number;
  height: number;
}

interface NodePos {
  x: number;
  y: number;
}

interface Edge {
  from: number;
  to: number;
  length: number;
}

class UnionFind {
  private parent: number[];
  constructor(n: number) {
    this.parent = Array.from({ length: n }, (_, i) => i);
  }
  find(x: number): number {
    if (this.parent[x] !== x) this.parent[x] = this.find(this.parent[x]);
    return this.parent[x];
  }
  union(a: number, b: number): boolean {
    const ra = this.find(a);
    const rb = this.find(b);
    if (ra === rb) return false;
    this.parent[ra] = rb;
    return true;
  }
}

function inExclusion(x: number, y: number, ex: Rect): boolean {
  return x >= ex.x && x <= ex.x + ex.width && y >= ex.y && y <= ex.y + ex.height;
}

function generateGraph(
  width: number,
  height: number,
): { nodes: NodePos[]; edges: Edge[] } {
  const rand = mulberry32(SEED);
  const nodes: NodePos[] = [];
  const minDist = NODE_RADIUS * 3;

  // Rejection-sample positions: avoid overlap, avoid the exclusion rect.
  let attempts = 0;
  while (nodes.length < COUNT && attempts < COUNT * 200) {
    attempts++;
    const x = INSET + rand() * (width - 2 * INSET);
    const y = INSET + rand() * (height - 2 * INSET);
    if (inExclusion(x, y, TEXT_EXCLUSION)) continue;
    const ok = nodes.every((n) => Math.hypot(n.x - x, n.y - y) >= minDist);
    if (ok) nodes.push({ x, y });
  }

  // For each node, connect to its k nearest neighbours; dedupe pairs.
  const seen = new Set<string>();
  const edges: Edge[] = [];
  const addEdge = (i: number, j: number, d: number) => {
    const a = Math.min(i, j);
    const b = Math.max(i, j);
    const key = `${a}-${b}`;
    if (seen.has(key)) return;
    seen.add(key);
    edges.push({ from: a, to: b, length: d });
  };

  for (let i = 0; i < nodes.length; i++) {
    const distances = nodes
      .map((n, j) => ({ j, d: Math.hypot(n.x - nodes[i].x, n.y - nodes[i].y) }))
      .filter((p) => p.j !== i)
      .sort((a, b) => a.d - b.d)
      .slice(0, EDGE_DEGREE);
    for (const { j, d } of distances) addEdge(i, j, d);
  }

  // Force connectedness: bridge separate components by their closest pair
  // until everything is one component.
  const uf = new UnionFind(nodes.length);
  for (const e of edges) uf.union(e.from, e.to);

  const groupsByRoot = () => {
    const g = new Map<number, number[]>();
    for (let i = 0; i < nodes.length; i++) {
      const r = uf.find(i);
      const list = g.get(r);
      if (list) list.push(i);
      else g.set(r, [i]);
    }
    return [...g.values()].sort((a, b) => b.length - a.length);
  };

  let groups = groupsByRoot();
  while (groups.length > 1) {
    const [main, other] = groups;
    let best = { i: main[0], j: other[0], d: Infinity };
    for (const i of main) {
      for (const j of other) {
        const d = Math.hypot(nodes[i].x - nodes[j].x, nodes[i].y - nodes[j].y);
        if (d < best.d) best = { i, j, d };
      }
    }
    addEdge(best.i, best.j, best.d);
    uf.union(best.i, best.j);
    groups = groupsByRoot();
  }

  return { nodes, edges };
}

// ─── Beat ────────────────────────────────────────────────────────────────

export const Graph: React.FC = () => {
  const frame = useCurrentFrame();
  const { fps, width, height } = useVideoConfig();

  const { nodes, edges } = useMemo(
    () => generateGraph(width, height),
    [width, height],
  );

  const nodeStart = (i: number) => START_FRAME + i * NODE_STAGGER;
  const edgeStart = (e: Edge) =>
    Math.max(nodeStart(e.from), nodeStart(e.to)) + NODE_DURATION + EDGE_DELAY;

  return (
    <AbsoluteFill style={{ opacity: MAX_OPACITY, pointerEvents: "none" }}>
      <svg height={height} viewBox={`0 0 ${width} ${height}`} width={width}>
        {edges.map((e, i) => {
          const a = nodes[e.from];
          const b = nodes[e.to];
          const draw = lineDraw({
            frame,
            start: edgeStart(e),
            length: e.length,
            duration: EDGE_DURATION,
          });
          return (
            <line
              key={`e-${i}`}
              stroke={EDGE_COLOR}
              strokeDasharray={draw.strokeDasharray}
              strokeDashoffset={draw.strokeDashoffset}
              strokeLinecap="round"
              strokeWidth={EDGE_WIDTH}
              x1={a.x}
              x2={b.x}
              y1={a.y}
              y2={b.y}
            />
          );
        })}

        {nodes.map((n, i) => {
          const t = springIn({
            frame,
            start: nodeStart(i),
            fps,
            config: { damping: 12, stiffness: 140, mass: 1 },
          });
          return (
            <circle
              cx={n.x}
              cy={n.y}
              fill={NODE_COLOR}
              key={`n-${i}`}
              opacity={t}
              r={NODE_RADIUS * t}
            />
          );
        })}
      </svg>
    </AbsoluteFill>
  );
};
