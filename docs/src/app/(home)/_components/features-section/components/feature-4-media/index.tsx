import { MaximizableWindow } from "../maximizable-window";
import { WindowTabbedEditor } from "../window-tabbed-editor";
import { ShikiBlock } from "./components/shiki-block";
import { renderShiki } from "./components/shiki-themes";

const BG_SRC = "/hero/hillside-village.png";

const LOAD_GRAPH_SNIPPET = `import optimuskg

# Download a specific file and store it locally
local_path = optimuskg.get_file("nodes/gene.parquet")

# Load a single Parquet file as a Polars DataFrame
drugs = optimuskg.load_parquet("nodes/drug.parquet")

# Load nodes and edges as Polars DataFrames
# Set lcc=True to load only the largest connected component
nodes, edges = optimuskg.load_graph(lcc=True)

# Load the graph as a NetworkX MultiDiGraph with metadata
# Set lcc=True to load only the largest connected component
G = optimuskg.load_networkx(lcc=True)
`;

export async function Feature4Media() {
  const loadGraphJsx = await renderShiki(LOAD_GRAPH_SNIPPET, "python");
  return (
    <div className="absolute inset-0 flex items-center justify-center overflow-hidden rounded-[1px]">
      <div className="pointer-events-none absolute inset-0 overflow-hidden">
        {/* biome-ignore lint/performance/noImgElement: intentionally overscaled panoramic background, next/image fill cannot reproduce the percentage stretch */}
        {/* biome-ignore lint/correctness/useImageSize: size is expressed as a percentage of the container, not intrinsic pixels */}
        <img
          alt=""
          className="absolute max-w-none"
          src={BG_SRC}
          style={{ height: "100%", left: "-27.46%", top: 0, width: "154.93%" }}
        />
      </div>
      <div
        className="pointer-events-none absolute inset-0"
        style={{
          background:
            "linear-gradient(90deg,rgba(38,37,30,0.05) 0%,rgba(38,37,30,0.05) 100%)",
        }}
      />

      <MaximizableWindow
        appIcon="/dock/editor.svg"
        appId="python-client"
        appName="Python Client"
        normalStyle={{
          width: "min(42.5rem, calc(100% - var(--l-window-inset, 4rem)))",
          height: "min(35rem, calc(100% - var(--l-window-inset, 4rem)))",
        }}
        title="Python Client"
      >
        <WindowTabbedEditor
          tabs={[
            {
              name: "load_graph.py",
              content: <ShikiBlock>{loadGraphJsx}</ShikiBlock>,
            },
          ]}
        />
      </MaximizableWindow>

      <div className="pointer-events-none absolute inset-0 rounded-[1px] border border-(--l-border-subtle)" />
    </div>
  );
}
