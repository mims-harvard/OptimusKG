import { AbsoluteFill, useCurrentFrame, useVideoConfig } from "remotion";
import { fadeRamp, springIn } from "../../animations";
import { Sfx } from "../../sounds/sfx";

// Beat: editor window. Springs in from the right edge after the heading
// settles, then reveals the Python snippet line by line once it lands.

export const WINDOW_START = 90;
export const WINDOW_RISE_REM = 50; // travel distance for the slide-in

const CODE_START = 150; // window settles ~140; start typing just after
const LINE_STAGGER = 6;
const LINE_FADE = 12;

const COLOR_BG = "#fafafa";
const COLOR_HEADER = "#f1f1f1";
const COLOR_BORDER = "rgba(204, 204, 204, 0.6)";
const COLOR_MUTED_FG = "#737373";
const FILENAME = "load_graph.py";

// Token colours (Catppuccin Latte-ish).
const C_DEFAULT = "#4c4f69";
const C_KEYWORD = "#8839ef"; // import, True
const C_IDENT = "#1e66f5"; // optimuskg, function names
const C_KWARG = "#df8e1d"; // keyword args (lcc=...)
const C_STRING = "#40a02b";
const C_COMMENT = "#8c8fa1";

interface Tok {
  text: string;
  color?: string;
}
type Line = Tok[];

// Snippet drawn from https://optimuskg.ai/docs/optimuskg-client — covers
// configuration, parquet loading (single file + column subset), graph
// loading (Polars + NetworkX), and file-path resolution.
const LINES: Line[] = [
  [
    { text: "import", color: C_KEYWORD },
    { text: " " },
    { text: "optimuskg", color: C_IDENT },
  ],
  [],
  [{ text: "# Override the cache directory", color: C_COMMENT }],
  [
    { text: "optimuskg", color: C_IDENT },
    { text: "." },
    { text: "set_cache_dir", color: C_IDENT },
    { text: "(" },
    { text: '"/data/optimuskg-cache"', color: C_STRING },
    { text: ")" },
  ],
  [],
  [{ text: "# Load a single Parquet file as a Polars DataFrame", color: C_COMMENT }],
  [
    { text: "drugs = " },
    { text: "optimuskg", color: C_IDENT },
    { text: "." },
    { text: "load_parquet", color: C_IDENT },
    { text: "(" },
    { text: '"nodes/drug.parquet"', color: C_STRING },
    { text: ")" },
  ],
  [],
  [{ text: "# Load only specific columns from a Parquet file", color: C_COMMENT }],
  [
    { text: "genes = " },
    { text: "optimuskg", color: C_IDENT },
    { text: "." },
    { text: "load_parquet", color: C_IDENT },
    { text: "(" },
  ],
  [
    { text: "    " },
    { text: '"nodes/gene.parquet"', color: C_STRING },
    { text: "," },
  ],
  [
    { text: "    " },
    { text: "columns", color: C_KWARG },
    { text: "=[" },
    { text: '"id"', color: C_STRING },
    { text: "]" },
  ],
  [{ text: ")" }],
  [],
  [{ text: "# Load nodes and edges as Polars DataFrames", color: C_COMMENT }],
  [
    { text: "# Set lcc=True to load only the largest connected component", color: C_COMMENT },
  ],
  [
    { text: "nodes, edges = " },
    { text: "optimuskg", color: C_IDENT },
    { text: "." },
    { text: "load_graph", color: C_IDENT },
    { text: "(" },
    { text: "lcc", color: C_KWARG },
    { text: "=" },
    { text: "True", color: C_KEYWORD },
    { text: ")" },
  ],
  [],
  [{ text: "# Load the graph as a NetworkX MultiDiGraph", color: C_COMMENT }],
  [
    { text: "G = " },
    { text: "optimuskg", color: C_IDENT },
    { text: "." },
    { text: "load_networkx", color: C_IDENT },
    { text: "(" },
    { text: "lcc", color: C_KWARG },
    { text: "=" },
    { text: "True", color: C_KEYWORD },
    { text: ")" },
  ],
  [],
  [{ text: "# Resolve a file path without loading it", color: C_COMMENT }],
  [
    { text: "path = " },
    { text: "optimuskg", color: C_IDENT },
    { text: "." },
    { text: "get_file", color: C_IDENT },
    { text: "(" },
    { text: '"edges/disease_gene.parquet"', color: C_STRING },
    { text: ")" },
  ],
];

const WinControl: React.FC<{ color: string }> = ({ color }) => (
  <div
    style={{
      background: color,
      borderRadius: 999,
      height: "0.625rem",
      width: "0.625rem",
    }}
  />
);

export const Window: React.FC = () => {
  const frame = useCurrentFrame();
  const { fps } = useVideoConfig();

  // Stiff damping → lands cleanly without overshoot past the right edge.
  const t = springIn({
    frame,
    start: WINDOW_START,
    fps,
    config: { damping: 22, stiffness: 110, mass: 1 },
  });
  const tx = WINDOW_RISE_REM * (1 - t);
  const opacity = t;

  if (opacity <= 0) return null;

  return (
    <AbsoluteFill
      style={{
        alignItems: "center",
        display: "flex",
        // AbsoluteFill defaults to column direction — set row so that
        // `justifyContent: flex-end` pushes horizontally to the right and
        // `alignItems: center` centres vertically.
        flexDirection: "row",
        justifyContent: "flex-end",
        overflow: "hidden",
      }}
    >
      <Sfx at={WINDOW_START} sound="swoosh" />
      {/* Fan-out swoosh — scene-8 duration 360 → exit starts at 342. */}
      <Sfx at={342} sound="swoosh" />
      <div
        style={{
          background: COLOR_BG,
          borderRadius: "0.625rem",
          boxShadow:
            "0px 28px 70px 0px rgba(0,0,0,0.14), 0px 14px 32px 0px rgba(0,0,0,0.1), 0px 0px 0px 1px rgba(38,37,30,0.1)",
          display: "flex",
          flexDirection: "column",
          height: "55rem",
          // Negative end-margin pushes the window past the canvas's right
          // edge so it appears cropped — matches the reference design.
          marginInlineEnd: "-14rem",
          opacity,
          overflow: "hidden",
          transform: `translateX(${tx}rem)`,
          transformOrigin: "right center",
          width: "64rem",
        }}
      >
        <div
          style={{
            alignItems: "center",
            background: COLOR_HEADER,
            borderBottom: `1px solid ${COLOR_BORDER}`,
            display: "flex",
            flexShrink: 0,
            height: "2rem",
            padding: "0 0.625rem",
            position: "relative",
          }}
        >
          <div style={{ display: "flex", gap: "0.5rem" }}>
            <WinControl color="#ff5f57" />
            <WinControl color="#febc2e" />
            <WinControl color="#28c840" />
          </div>
          <span
            style={{
              color: COLOR_MUTED_FG,
              fontFamily:
                "ui-monospace, SFMono-Regular, 'SF Mono', Menlo, Consolas, monospace",
              fontSize: 12,
              left: "50%",
              position: "absolute",
              transform: "translateX(-50%)",
              whiteSpace: "nowrap",
            }}
          >
            {FILENAME}
          </span>
        </div>
        <div
          style={{
            color: C_DEFAULT,
            flex: 1,
            fontFamily:
              "ui-monospace, SFMono-Regular, 'SF Mono', Menlo, Consolas, monospace",
            fontSize: "1.375rem",
            lineHeight: 1.55,
            overflow: "hidden",
            padding: "1.5rem 1.75rem",
          }}
        >
          {LINES.map((line, i) => {
            const lineOpacity = fadeRamp({
              frame,
              start: CODE_START + i * LINE_STAGGER,
              duration: LINE_FADE,
              from: 0,
              to: 1,
            });
            return (
              <div
                key={i}
                style={{
                  minHeight: "1.55em",
                  opacity: lineOpacity,
                  whiteSpace: "pre",
                }}
              >
                {line.length === 0
                  ? " "
                  : line.map((tok, j) => (
                      <span key={j} style={{ color: tok.color }}>
                        {tok.text}
                      </span>
                    ))}
              </div>
            );
          })}
        </div>
      </div>
    </AbsoluteFill>
  );
};
