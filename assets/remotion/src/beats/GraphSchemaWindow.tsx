import { AbsoluteFill, interpolate, useCurrentFrame } from "remotion";
import { HERO_HEADING } from "../Beat";
import {
  EditorWindow,
  GENE_FIELDS,
  RevealLine,
  SchemaTreeStatic,
  TabbedEditorShell,
} from "../primitives";
import type { BeatRenderProps } from "../scenes";

// Beat 8: reveal the Graph Schema window (tabs: Gene Nodes / Disease-Gene Edges).

export const GraphSchemaWindow: React.FC<BeatRenderProps> = ({ heroText }) => {
  const frame = useCurrentFrame();
  const windowOpacity = interpolate(frame, [4, 32], [0, 1], {
    extrapolateLeft: "clamp",
    extrapolateRight: "clamp",
  });
  const windowY = interpolate(frame, [4, 36], [30, 0], {
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
        gap: "2.5rem",
        padding: "2rem 1.25rem",
      }}
    >
      <h2
        className="text-center text-fd-foreground"
        style={{ ...HERO_HEADING, fontSize: "5rem" }}
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
          width: "min(90%, 1200px)",
          height: "28rem",
          opacity: windowOpacity,
          transform: `translateY(${windowY}px)`,
        }}
      >
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
    </AbsoluteFill>
  );
};
