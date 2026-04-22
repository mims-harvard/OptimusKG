import { AbsoluteFill, interpolate, useCurrentFrame } from "remotion";
import { HERO_HEADING } from "../Beat";
import { EditorWindow, RevealLine, TabbedEditorShell } from "../primitives";
import type { BeatRenderProps } from "../scenes";

// Beat 10 (138 frames): load_graph.py with type-on animation.
// Last char should land ~frame 132 (6 before the 138-frame cut).

const CODE: string[] = [
  "import optimuskg",
  "",
  "# Load a single Parquet file as a Polars DataFrame",
  'drugs = optimuskg.load_parquet("nodes/drug.parquet")',
  "",
  "# Load nodes and edges as Polars DataFrames",
  "# Set lcc=True to load only the largest connected component",
  "nodes, edges = optimuskg.load_graph(lcc=True)",
  "",
  "# Load the graph as a NetworkX MultiDiGraph with metadata",
  "# Set lcc=True to load only the largest connected component",
  "G = optimuskg.load_networkx(lcc=True)",
];

// Allocate 108 typing frames starting at frame 24 → finishes by ~132.
const TYPE_START = 24;
const TYPE_END = 132;
const TOTAL_CHARS = CODE.reduce((acc, line) => acc + line.length, 0) || 1;

// Compute cumulative character offset per line so each line gets a proportional slice.
const LINE_OFFSETS: number[] = (() => {
  const arr: number[] = [];
  let acc = 0;
  for (const line of CODE) {
    arr.push(acc);
    acc += line.length;
  }
  return arr;
})();

const FRAMES_PER_CHAR = (TYPE_END - TYPE_START) / TOTAL_CHARS;

export const PythonClientWindow: React.FC<BeatRenderProps> = ({ heroText }) => {
  const frame = useCurrentFrame();
  const windowOpacity = interpolate(frame, [0, 18], [0, 1], {
    extrapolateLeft: "clamp",
    extrapolateRight: "clamp",
  });
  const windowY = interpolate(frame, [0, 22], [30, 0], {
    extrapolateLeft: "clamp",
    extrapolateRight: "clamp",
  });

  return (
    <AbsoluteFill
      className="bg-fd-background"
      style={{
        display: "flex",
        flexDirection: "column",
        alignItems: "center",
        justifyContent: "center",
        gap: "2rem",
        padding: "2rem 1.25rem",
      }}
    >
      <h2
        className="text-center text-fd-foreground"
        style={{ ...HERO_HEADING, fontSize: "4.5rem" }}
      >
        <RevealLine
          perWordFrames={3}
          startFrame={2}
          style={{ display: "block" }}
          tokens={(heroText as string).split(" ")}
        />
      </h2>
      <div
        style={{
          width: "min(92%, 1200px)",
          height: "32rem",
          opacity: windowOpacity,
          transform: `translateY(${windowY}px)`,
        }}
      >
        <EditorWindow className="h-full w-full" title="Python Client">
          <TabbedEditorShell tabs={[{ name: "load_graph.py" }]}>
            <div className="h-full w-full overflow-hidden px-8 py-6">
              <div
                className="font-mono"
                style={{ fontSize: 22, lineHeight: 1.65 }}
              >
                {CODE.map((line, i) => (
                  <TypewriterLine
                    charDelayFrames={FRAMES_PER_CHAR}
                    key={i}
                    startOffset={TYPE_START + LINE_OFFSETS[i] * FRAMES_PER_CHAR}
                    text={line}
                  />
                ))}
              </div>
            </div>
          </TabbedEditorShell>
        </EditorWindow>
      </div>
    </AbsoluteFill>
  );
};

const TypewriterLine: React.FC<{
  text: string;
  startOffset: number;
  charDelayFrames: number;
}> = ({ text, startOffset, charDelayFrames }) => {
  const frame = useCurrentFrame();
  if (!text) return <div style={{ height: "1em" }} />;
  const progress = interpolate(
    frame - startOffset,
    [0, Math.max(1, text.length * charDelayFrames)],
    [0, text.length],
    { extrapolateLeft: "clamp", extrapolateRight: "clamp" },
  );
  const visible = text.slice(0, Math.floor(progress));
  const isComment = visible.startsWith("#");
  if (isComment) return <div style={{ color: "#8c8fa1" }}>{visible}</div>;
  const parts = visible.split(
    /("[^"]*"|\b(?:import|as|True|False|None)\b|[()=,.])/,
  );
  return (
    <div style={{ color: "#4c4f69" }}>
      {parts.map((tok, i) => {
        if (!tok) return null;
        if (/^"[^"]*"$/.test(tok))
          return (
            <span key={i} style={{ color: "#40a02b" }}>
              {tok}
            </span>
          );
        if (["import", "as", "True", "False", "None"].includes(tok))
          return (
            <span key={i} style={{ color: "#8839ef" }}>
              {tok}
            </span>
          );
        if (tok === "optimuskg")
          return (
            <span key={i} style={{ color: "#1e66f5" }}>
              {tok}
            </span>
          );
        if (/^(load_parquet|load_graph|load_networkx|get_file)$/.test(tok))
          return (
            <span key={i} style={{ color: "#1e66f5" }}>
              {tok}
            </span>
          );
        if (tok === "lcc")
          return (
            <span key={i} style={{ color: "#df8e1d" }}>
              {tok}
            </span>
          );
        return <span key={i}>{tok}</span>;
      })}
    </div>
  );
};
