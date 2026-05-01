import { AbsoluteFill, interpolate, useCurrentFrame } from "remotion";
import { HERO_HEADING } from "../Beat";
import { EditorWindow, RevealLine, TabbedEditorShell } from "../primitives";
import type { BeatRenderProps } from "../scenes";

// Beat 10 (348 frames):
//   0–36     Hero text reveals, centered, alone.
//   36–96    Hold hero for reading (~1 s).
//   96–120   Hero fades out.
//   104–132  Window fades + slides in.
//   132–348  Typewriter types out the code (~216 f); last char ~12 f before cut.

const HERO_FADE_OUT: [number, number] = [96, 120];
const WINDOW_FADE_IN: [number, number] = [104, 132];
const TYPE_START = 132;
const TYPE_END = 336;

// Window aspect mirrors the landing's `42.5rem × 35rem` (≈ 17:14 / 1.214).
const WINDOW_W = 760;
const WINDOW_H = 490; // height fits 12-line code + small buffer

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

const TOTAL_CHARS = CODE.reduce((acc, line) => acc + line.length, 0) || 1;
const FRAMES_PER_CHAR = (TYPE_END - TYPE_START) / TOTAL_CHARS;
const LINE_OFFSETS: number[] = (() => {
  const arr: number[] = [];
  let acc = 0;
  for (const line of CODE) {
    arr.push(acc);
    acc += line.length;
  }
  return arr;
})();

export const PythonClientWindow: React.FC<BeatRenderProps> = ({ heroText }) => {
  const frame = useCurrentFrame();

  const heroOpacity = interpolate(frame, HERO_FADE_OUT, [1, 0], {
    extrapolateLeft: "clamp",
    extrapolateRight: "clamp",
  });
  const windowOpacity = interpolate(frame, WINDOW_FADE_IN, [0, 1], {
    extrapolateLeft: "clamp",
    extrapolateRight: "clamp",
  });
  const windowY = interpolate(frame, WINDOW_FADE_IN, [24, 0], {
    extrapolateLeft: "clamp",
    extrapolateRight: "clamp",
  });

  return (
    <AbsoluteFill className="bg-fd-background">
      {heroOpacity > 0 && (
        <div
          style={{
            position: "absolute",
            top: "50%",
            left: 0,
            right: 0,
            transform: "translateY(-50%)",
            textAlign: "center",
            opacity: heroOpacity,
          }}
        >
          <h2 className="text-fd-foreground" style={{ ...HERO_HEADING }}>
            <RevealLine
              perWordFrames={6}
              startFrame={4}
              tokens={(heroText as string).split(" ")}
            />
          </h2>
        </div>
      )}

      {windowOpacity > 0 && (
        <div
          style={{
            position: "absolute",
            top: "50%",
            left: "50%",
            width: WINDOW_W,
            height: WINDOW_H,
            transform: `translate(-50%, calc(-50% + ${windowY}px))`,
            opacity: windowOpacity,
          }}
        >
          <EditorWindow className="h-full w-full" title="Python Client">
            <TabbedEditorShell tabs={[{ name: "load_graph.py" }]}>
              <div className="h-full w-full overflow-hidden px-6 py-4">
                <div
                  className="font-mono"
                  style={{ fontSize: 18, lineHeight: 1.65 }}
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
      )}
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
