import {
  AbsoluteFill,
  Easing,
  Img,
  Series,
  interpolate,
  staticFile,
  useCurrentFrame,
} from "remotion";

import {
  ArrowRightIcon,
  DownloadButton,
  EditorWindow,
  FeatureText,
  GENE_FIELDS,
  SchemaTreeStatic,
  TabbedEditorShell,
  ValidationsSidebarStatic,
} from "./primitives";

export const FPS = 30;
export const WIDTH = 1920;
export const HEIGHT = 1080;

const SCENES = {
  intro: 4 * FPS,
  stats: 5 * FPS,
  feature1: 6 * FPS,
  feature2: 6 * FPS,
  feature3: 5 * FPS,
  outro: 4 * FPS,
};

export const DURATION_IN_FRAMES = Object.values(SCENES).reduce(
  (a, b) => a + b,
  0,
);

const EASE_OUT = Easing.bezier(0.16, 1, 0.3, 1);

const CONTAINER_MAX = 1300;
const GUTTER = 20;
const SCENE_V_PADDING = "5.6rem";
const SCENE_V_PADDING_FIRST = "7rem";
const WINDOW_STYLE: React.CSSProperties = {
  width: "min(42.5rem, calc(100% - var(--l-window-inset, 4rem)))",
  height: "min(35rem, calc(100% - var(--l-window-inset, 4rem)))",
};

const SceneShell: React.FC<{
  firstOfPage?: boolean;
  flush?: "bottom";
  centerVertically?: boolean;
  bordered?: boolean;
  children: React.ReactNode;
}> = ({ firstOfPage, flush, centerVertically, bordered, children }) => (
  <AbsoluteFill
    className="bg-fd-background"
    style={{
      paddingTop: firstOfPage ? SCENE_V_PADDING_FIRST : SCENE_V_PADDING,
      paddingBottom: flush === "bottom" ? 0 : SCENE_V_PADDING,
      paddingLeft: GUTTER,
      paddingRight: GUTTER,
      display: "flex",
      flexDirection: "column",
      justifyContent: centerVertically ? "center" : "flex-start",
      borderTop: bordered ? "1px solid var(--color-fd-border)" : undefined,
    }}
  >
    <div
      style={{
        width: "100%",
        maxWidth: CONTAINER_MAX,
        marginInline: "auto",
      }}
    >
      {children}
    </div>
  </AbsoluteFill>
);

export const OptimusKGVideo: React.FC = () => (
  <AbsoluteFill className="bg-fd-background text-fd-foreground">
    <Series>
      <Series.Sequence durationInFrames={SCENES.intro}>
        <IntroScene />
      </Series.Sequence>
      <Series.Sequence durationInFrames={SCENES.stats}>
        <StatsScene />
      </Series.Sequence>
      <Series.Sequence durationInFrames={SCENES.feature1}>
        <Feature1Scene />
      </Series.Sequence>
      <Series.Sequence durationInFrames={SCENES.feature2}>
        <Feature2Scene />
      </Series.Sequence>
      <Series.Sequence durationInFrames={SCENES.feature3}>
        <Feature3Scene />
      </Series.Sequence>
      <Series.Sequence durationInFrames={SCENES.outro}>
        <OutroScene />
      </Series.Sequence>
    </Series>
  </AbsoluteFill>
);

const IntroScene: React.FC = () => {
  const frame = useCurrentFrame();
  const lineAOpacity = interpolate(frame, [5, 30], [0, 1], {
    extrapolateRight: "clamp",
    easing: EASE_OUT,
  });
  const lineAY = interpolate(frame, [5, 35], [18, 0], {
    extrapolateRight: "clamp",
    easing: EASE_OUT,
  });
  const lineBOpacity = interpolate(frame, [20, 45], [0, 1], {
    extrapolateRight: "clamp",
    easing: EASE_OUT,
  });
  const lineBY = interpolate(frame, [20, 50], [18, 0], {
    extrapolateRight: "clamp",
    easing: EASE_OUT,
  });
  const btnOpacity = interpolate(frame, [40, 65], [0, 1], {
    extrapolateRight: "clamp",
    easing: EASE_OUT,
  });
  const mediaOpacity = interpolate(frame, [25, 60], [0, 1], {
    extrapolateRight: "clamp",
    easing: EASE_OUT,
  });
  const figureOpacity = interpolate(frame, [45, 80], [0, 1], {
    extrapolateRight: "clamp",
    easing: EASE_OUT,
  });
  const figureY = interpolate(frame, [45, 85], [20, 0], {
    extrapolateRight: "clamp",
    easing: EASE_OUT,
  });

  return (
    <SceneShell firstOfPage flush="bottom">
      <div className="flex flex-col gap-14">
        <div className="flex flex-col gap-5.5">
          <h1 className="text-balance font-normal text-2xl text-fd-foreground leading-8">
            <span
              className="block"
              style={{
                opacity: lineAOpacity,
                transform: `translateY(${lineAY}px)`,
              }}
            >
              Unifying biomedical knowledge
            </span>
            <span
              className="block"
              style={{
                opacity: lineBOpacity,
                transform: `translateY(${lineBY}px)`,
              }}
            >
              in a modern multimodal graph
            </span>
          </h1>

          <div
            className="flex flex-wrap items-center gap-2.5"
            style={{ opacity: btnOpacity }}
          >
            <span className="inline-flex items-center gap-1.5 bg-fd-foreground px-5.75 py-3.5 font-normal text-fd-background text-sm leading-4">
              <DownloadButton />
            </span>
            <span className="inline-flex items-center gap-1.5 whitespace-nowrap border border-fd-border px-5.75 py-3.5 font-normal text-fd-foreground text-sm leading-4">
              Read the docs
            </span>
          </div>
        </div>

        <div
          className="relative overflow-hidden"
          style={{
            borderRadius: 1,
            height: "42.5rem",
            background:
              "linear-gradient(rgba(38,37,30,0.05),rgba(38,37,30,0.05)), linear-gradient(#f2f1ed,#f2f1ed)",
            opacity: mediaOpacity,
          }}
        >
          <Img
            src={staticFile("hero/valley-stream.png")}
            style={{
              position: "absolute",
              inset: 0,
              width: "100%",
              height: "100%",
              objectFit: "cover",
              transform: "scale(1.1)",
            }}
          />
          <div
            className="pointer-events-none absolute inset-0"
            style={{
              background:
                "linear-gradient(90deg,rgba(0,0,0,0.12) 0%,rgba(0,0,0,0.22) 100%)",
            }}
          />
          <Img
            src={staticFile("features/figure.webp")}
            style={{
              position: "absolute",
              top: "50%",
              left: "50%",
              transform: `translate(-50%, calc(-50% + ${figureY}px))`,
              maxWidth: "calc(100% - 4rem)",
              maxHeight: "calc(100% - 4rem)",
              borderRadius: "0.625rem",
              border: "1px solid var(--color-fd-border)",
              boxShadow:
                "0px 28px 70px rgba(0,0,0,0.14), 0px 14px 32px rgba(0,0,0,0.1)",
              background: "var(--color-fd-background)",
              opacity: figureOpacity,
            }}
          />
          <div
            className="pointer-events-none absolute inset-0"
            style={{
              borderRadius: 1,
              border: "1px solid var(--l-border-subtle)",
            }}
          />
        </div>
      </div>
    </SceneShell>
  );
};

const STATS = [
  { value: "65", label: "sources" },
  { value: "18", label: "ontologies" },
  { value: "10", label: "entity types" },
  { value: "190,531", label: "nodes" },
  { value: "26", label: "relation types" },
  { value: "21,813,816", label: "edges" },
  { value: "110,276,843", label: "properties" },
];

const StatsScene: React.FC = () => {
  const frame = useCurrentFrame();
  const marqueeOpacity = interpolate(frame, [0, 25], [0, 1], {
    extrapolateRight: "clamp",
    easing: EASE_OUT,
  });

  const shift = interpolate(frame, [0, SCENES.stats], [0, -1800]);
  const items = [...STATS, ...STATS, ...STATS];

  return (
    <SceneShell centerVertically>
      <div
        className="overflow-hidden"
        style={{
          maskImage:
            "linear-gradient(to right, transparent, black 5%, black 95%, transparent)",
          WebkitMaskImage:
            "linear-gradient(to right, transparent, black 5%, black 95%, transparent)",
          opacity: marqueeOpacity,
        }}
      >
        <ul
          className="flex shrink-0 items-center gap-12"
          style={{
            transform: `translateX(${shift}px)`,
            whiteSpace: "nowrap",
          }}
        >
          {items.map((s, i) => (
            <li
              className="flex items-baseline gap-2 whitespace-nowrap"
              key={`${s.label}-${i}`}
            >
              <span className="font-mono text-fd-foreground text-lg tabular-nums">
                {s.value}
              </span>
              <span className="text-fd-muted-foreground text-sm">
                {s.label}
              </span>
            </li>
          ))}
        </ul>
      </div>
    </SceneShell>
  );
};

const FeatureCardShell: React.FC<{
  imageSide: "left" | "right";
  height: string;
  mediaHeight: string;
  text: React.ReactNode;
  media: React.ReactNode;
  cardOpacity: number;
  cardY: number;
}> = ({ imageSide, height, mediaHeight, text, media, cardOpacity, cardY }) => {
  const textCol =
    imageSide === "right"
      ? "col-[1/span_8] pl-0.5 pr-7.5"
      : "col-[17/span_8] pl-7.5 pr-0.5";
  const imageCol =
    imageSide === "right" ? "col-[9/span_16]" : "col-[1/span_16]";
  return (
    <SceneShell centerVertically>
      <div
        className="relative grid gap-x-2.5 bg-fd-card p-4.5"
        style={{
          gridTemplateColumns: "repeat(24, minmax(0, 1fr))",
          borderRadius: 1,
          height,
          opacity: cardOpacity,
          transform: `translateY(${cardY}px)`,
        }}
      >
        <div
          className={`group/card row-start-1 flex flex-col justify-center ${textCol}`}
        >
          {text}
        </div>
        <div
          className={`relative row-start-1 overflow-hidden ${imageCol}`}
          style={{ borderRadius: 1, height: mediaHeight }}
        >
          {media}
        </div>
        <div
          className="pointer-events-none absolute inset-0"
          style={{
            borderRadius: 1,
            border: "1px solid var(--l-border-subtle)",
          }}
        />
      </div>
    </SceneShell>
  );
};

const MediaBackdrop: React.FC<{ src: string; overlay?: string }> = ({
  src,
  overlay,
}) => (
  <>
    <Img
      src={staticFile(src)}
      style={{
        position: "absolute",
        inset: 0,
        width: "100%",
        height: "100%",
        objectFit: "cover",
        transform: "scale(1.1)",
      }}
    />
    <div
      className="pointer-events-none absolute inset-0"
      style={{
        background:
          overlay ??
          "linear-gradient(90deg,rgba(0,0,0,0.12) 0%,rgba(0,0,0,0.22) 100%)",
      }}
    />
  </>
);

const Feature1Scene: React.FC = () => {
  const frame = useCurrentFrame();
  const textOpacity = interpolate(frame, [0, 25], [0, 1], {
    extrapolateRight: "clamp",
    easing: EASE_OUT,
  });
  const textY = interpolate(frame, [0, 30], [12, 0], {
    extrapolateRight: "clamp",
    easing: EASE_OUT,
  });
  const descOpacity = interpolate(frame, [15, 35], [0, 1], {
    extrapolateRight: "clamp",
    easing: EASE_OUT,
  });
  const ctaOpacity = interpolate(frame, [25, 45], [0, 1], {
    extrapolateRight: "clamp",
    easing: EASE_OUT,
  });
  const cardOpacity = interpolate(frame, [10, 40], [0, 1], {
    extrapolateRight: "clamp",
    easing: EASE_OUT,
  });
  const cardY = interpolate(frame, [10, 40], [24, 0], {
    extrapolateRight: "clamp",
    easing: EASE_OUT,
  });

  return (
    <FeatureCardShell
      cardOpacity={cardOpacity}
      cardY={cardY}
      height="44.6875rem"
      imageSide="right"
      mediaHeight="42.5rem"
      text={
        <FeatureText
          ctaOpacity={ctaOpacity}
          ctaText="Learn about the schema"
          descOpacity={descOpacity}
          description="Every entity is enriched with structured properties for fine-grained analysis."
          eyebrow="Feature 01"
          title="Rich strongly-typed properties"
          titleOpacity={textOpacity}
          titleY={textY}
        />
      }
      media={
        <div className="absolute inset-0 flex items-center justify-center overflow-hidden">
          <MediaBackdrop src="hero/lakeside-village.png" />
          <div className="pointer-events-none absolute inset-0 flex items-center justify-center">
            <div className="pointer-events-auto" style={WINDOW_STYLE}>
              <EditorWindow className="h-full w-full" title="Graph Schema">
                <TabbedEditorShell
                  activeIndex={0}
                  tabs={[
                    { name: "Gene Nodes Schema" },
                    { name: "Disease-Gene Edges Schema" },
                  ]}
                >
                  <SchemaTreeStatic fields={GENE_FIELDS} />
                </TabbedEditorShell>
              </EditorWindow>
            </div>
          </div>
          <div
            className="pointer-events-none absolute inset-0"
            style={{
              borderRadius: 1,
              border: "1px solid var(--l-border-subtle)",
            }}
          />
        </div>
      }
    />
  );
};

const CODE_LINES = [
  { text: "import optimuskg", delay: 20 },
  { text: "", delay: 0 },
  { text: "# Load a single Parquet file as a Polars DataFrame", delay: 40 },
  { text: "drugs = optimuskg.load_parquet('nodes/drug.parquet')", delay: 65 },
  { text: "", delay: 0 },
  { text: "# Load nodes and edges as Polars DataFrames", delay: 95 },
  { text: "nodes, edges = optimuskg.load_graph(lcc=True)", delay: 118 },
  { text: "", delay: 0 },
  { text: "# Or load the graph as a NetworkX MultiDiGraph", delay: 142 },
  { text: "G = optimuskg.load_networkx(lcc=True)", delay: 162 },
];

const Feature2Scene: React.FC = () => {
  const frame = useCurrentFrame();
  const textOpacity = interpolate(frame, [0, 25], [0, 1], {
    extrapolateRight: "clamp",
    easing: EASE_OUT,
  });
  const textY = interpolate(frame, [0, 30], [12, 0], {
    extrapolateRight: "clamp",
    easing: EASE_OUT,
  });
  const descOpacity = interpolate(frame, [15, 35], [0, 1], {
    extrapolateRight: "clamp",
    easing: EASE_OUT,
  });
  const ctaOpacity = interpolate(frame, [25, 45], [0, 1], {
    extrapolateRight: "clamp",
    easing: EASE_OUT,
  });
  const cardOpacity = interpolate(frame, [10, 40], [0, 1], {
    extrapolateRight: "clamp",
    easing: EASE_OUT,
  });
  const cardY = interpolate(frame, [10, 40], [24, 0], {
    extrapolateRight: "clamp",
    easing: EASE_OUT,
  });

  return (
    <FeatureCardShell
      cardOpacity={cardOpacity}
      cardY={cardY}
      height="42.8125rem"
      imageSide="left"
      mediaHeight="40.625rem"
      text={
        <FeatureText
          ctaOpacity={ctaOpacity}
          ctaText="uv add optimuskg"
          ctaVariant="snippet"
          descOpacity={descOpacity}
          description="Install with one command and load the graph as Polars data frames or a NetworkX graph in a single line."
          eyebrow="Feature 02"
          title="Delightfully simple Python client"
          titleOpacity={textOpacity}
          titleY={textY}
        />
      }
      media={
        <div className="absolute inset-0 flex items-center justify-center overflow-hidden">
          <MediaBackdrop
            overlay="linear-gradient(90deg,rgba(38,37,30,0.05) 0%,rgba(38,37,30,0.05) 100%)"
            src="hero/hillside-village.png"
          />
          <div className="pointer-events-none absolute inset-0 flex items-center justify-center">
            <div className="pointer-events-auto" style={WINDOW_STYLE}>
              <EditorWindow className="h-full w-full" title="Python Client">
                <TabbedEditorShell tabs={[{ name: "load_graph.py" }]}>
                  <div className="h-full w-full overflow-hidden p-4">
                    <div className="font-mono text-[13px] leading-relaxed">
                      {CODE_LINES.map((l, i) => (
                        <TypewriterLine
                          delay={l.delay}
                          key={i}
                          text={l.text}
                        />
                      ))}
                    </div>
                  </div>
                </TabbedEditorShell>
              </EditorWindow>
            </div>
          </div>
          <div
            className="pointer-events-none absolute inset-0"
            style={{
              borderRadius: 1,
              border: "1px solid var(--l-border-subtle)",
            }}
          />
        </div>
      }
    />
  );
};

const TypewriterLine: React.FC<{ text: string; delay: number }> = ({
  text,
  delay,
}) => {
  const frame = useCurrentFrame();
  if (!text) return <div style={{ height: "1em" }} />;
  const progress = interpolate(
    frame - delay,
    [0, Math.max(1, text.length * 0.9)],
    [0, text.length],
    {
      extrapolateLeft: "clamp",
      extrapolateRight: "clamp",
    },
  );
  const visible = text.slice(0, Math.floor(progress));
  const isComment = visible.startsWith("#");
  if (isComment) {
    return <div style={{ color: "#8c8fa1" }}>{visible}</div>;
  }
  const parts = visible.split(
    /('[^']*'|\b(?:import|as|True|False|None)\b|[()=,.])/,
  );
  return (
    <div style={{ color: "#4c4f69" }}>
      {parts.map((tok, i) => {
        if (!tok) return null;
        if (/^'[^']*'$/.test(tok))
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
        if (/^(load_parquet|load_graph|load_networkx)$/.test(tok))
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

const Feature3Scene: React.FC = () => {
  const frame = useCurrentFrame();
  const textOpacity = interpolate(frame, [0, 25], [0, 1], {
    extrapolateRight: "clamp",
    easing: EASE_OUT,
  });
  const textY = interpolate(frame, [0, 30], [12, 0], {
    extrapolateRight: "clamp",
    easing: EASE_OUT,
  });
  const descOpacity = interpolate(frame, [15, 35], [0, 1], {
    extrapolateRight: "clamp",
    easing: EASE_OUT,
  });
  const ctaOpacity = interpolate(frame, [25, 45], [0, 1], {
    extrapolateRight: "clamp",
    easing: EASE_OUT,
  });
  const cardOpacity = interpolate(frame, [10, 40], [0, 1], {
    extrapolateRight: "clamp",
    easing: EASE_OUT,
  });
  const cardY = interpolate(frame, [10, 40], [24, 0], {
    extrapolateRight: "clamp",
    easing: EASE_OUT,
  });

  return (
    <FeatureCardShell
      cardOpacity={cardOpacity}
      cardY={cardY}
      height="44.6875rem"
      imageSide="right"
      mediaHeight="42.5rem"
      text={
        <FeatureText
          ctaOpacity={ctaOpacity}
          ctaText="Learn about our methodology"
          descOpacity={descOpacity}
          description={
            <>
              Every edge is cross-validated against millions of research papers
              by <span className="text-fd-foreground">PaperQA3</span>, a deep
              research agent.
            </>
          }
          eyebrow="Feature 03"
          title="Rigorously validated"
          titleOpacity={textOpacity}
          titleY={textY}
        />
      }
      media={
        <div className="absolute inset-0 flex items-center justify-center overflow-hidden">
          <MediaBackdrop
            overlay="linear-gradient(90deg,rgba(38,37,30,0.05) 0%,rgba(38,37,30,0.05) 100%)"
            src="hero/mountain-overlook.png"
          />
          <div className="pointer-events-none absolute inset-0 flex items-center justify-center">
            <div className="pointer-events-auto" style={WINDOW_STYLE}>
              <EditorWindow className="h-full w-full" title="PaperQA3 Analysis">
                <div className="flex h-full min-h-0 w-full">
                  <ValidationsSidebarStatic activeId="molecular-function" />
                  <div className="min-w-0 flex-1">
                    <TabbedEditorShell
                      activeIndex={1}
                      tabs={[
                        { name: "Molecular Function" },
                        { name: "Phenotype" },
                      ]}
                    >
                      <ValidationChart />
                    </TabbedEditorShell>
                  </div>
                </div>
              </EditorWindow>
            </div>
          </div>
          <div
            className="pointer-events-none absolute inset-0"
            style={{
              borderRadius: 1,
              border: "1px solid var(--l-border-subtle)",
            }}
          />
        </div>
      }
    />
  );
};

const ValidationChart: React.FC = () => {
  const frame = useCurrentFrame();
  const bars = [
    { label: "Supported", value: 0.72, color: "hsl(145, 60%, 45%)" },
    { label: "Partial", value: 0.18, color: "hsl(35, 80%, 55%)" },
    { label: "Refuted", value: 0.06, color: "hsl(355, 70%, 55%)" },
    { label: "Uncertain", value: 0.04, color: "hsl(215, 15%, 55%)" },
  ];
  return (
    <div className="flex h-full w-full flex-col px-6 py-5">
      <div className="mb-4 flex items-baseline justify-between">
        <div className="font-normal text-fd-foreground text-sm">
          Validation distribution
        </div>
        <div className="font-mono text-fd-muted-foreground text-xs">
          n = 142,817 edges
        </div>
      </div>
      <div className="flex flex-1 items-end gap-6 border-fd-border border-b pb-4">
        {bars.map((b, i) => {
          const grow = interpolate(frame - i * 6, [15, 45], [0, 1], {
            extrapolateLeft: "clamp",
            extrapolateRight: "clamp",
            easing: EASE_OUT,
          });
          const h = `${b.value * 100 * grow}%`;
          return (
            <div
              className="flex h-full flex-1 flex-col items-end justify-end"
              key={b.label}
            >
              <div
                className="w-full text-center font-mono text-[10px] text-fd-muted-foreground"
                style={{ marginBottom: 4, opacity: grow }}
              >
                {Math.round(b.value * 100)}%
              </div>
              <div
                style={{
                  width: "100%",
                  height: h,
                  background: b.color,
                  borderRadius: 1,
                }}
              />
            </div>
          );
        })}
      </div>
      <div className="mt-2 flex justify-between">
        {bars.map((b) => (
          <div
            className="flex flex-1 justify-center text-fd-muted-foreground text-xs"
            key={b.label}
          >
            {b.label}
          </div>
        ))}
      </div>
    </div>
  );
};

const OutroScene: React.FC = () => {
  const frame = useCurrentFrame();
  const headingOpacity = interpolate(frame, [5, 35], [0, 1], {
    extrapolateRight: "clamp",
    easing: EASE_OUT,
  });
  const headingY = interpolate(frame, [5, 35], [20, 0], {
    extrapolateRight: "clamp",
    easing: EASE_OUT,
  });
  const btnOpacity = interpolate(frame, [25, 55], [0, 1], {
    extrapolateRight: "clamp",
    easing: EASE_OUT,
  });
  const footerOpacity = interpolate(frame, [40, 70], [0, 1], {
    extrapolateRight: "clamp",
    easing: EASE_OUT,
  });

  return (
    <SceneShell bordered centerVertically>
      <div
        className="flex flex-col items-center gap-5.5 text-center"
        style={{ maxWidth: "50.625rem", marginInline: "auto" }}
      >
        <h2
          className="text-balance font-normal text-7xl text-fd-foreground tracking-tight"
          style={{
            lineHeight: 1.18,
            opacity: headingOpacity,
            transform: `translateY(${headingY}px)`,
          }}
        >
          Try OptimusKG now.
        </h2>
        <span
          className="inline-flex items-center gap-1.5 bg-fd-foreground px-5.75 py-3.5 text-base text-fd-background"
          style={{ opacity: btnOpacity }}
        >
          <DownloadButton />
        </span>
        <div
          className="flex items-center gap-2 text-fd-muted-foreground text-sm"
          style={{
            marginTop: "2.5rem",
            opacity: footerOpacity,
            letterSpacing: "0.00875rem",
          }}
        >
          <span>© {new Date().getFullYear()} Zitnik Lab</span>
          <span>·</span>
          <span>An open science, academic research project.</span>
          <ArrowRightIcon size={14} />
          <span className="text-fd-foreground">optimuskg.ai</span>
        </div>
      </div>
    </SceneShell>
  );
};
