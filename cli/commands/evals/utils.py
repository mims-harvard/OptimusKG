"""Shared utilities for evals commands."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Literal

import networkx as nx
import polars as pl

logger = logging.getLogger("cli")

# Centrality metrics supported by compute_centrality
CentralityMetric = Literal[
    "pagerank", "degree", "betweenness", "closeness", "eigenvector"
]

# Graph construction modes
GraphMode = Literal["directed", "undirected"]

# Human-readable labels for compact node-type codes
NODE_TYPE_LABELS: dict[str, str] = {
    "ANA": "Anatomy",
    "BPO": "Biological process",
    "CCO": "Cellular component",
    "DIS": "Disease",
    "DRG": "Drug",
    "EXP": "Exposure",
    "GEN": "Gene",
    "MFN": "Molecular function",
    "PHE": "Phenotype",
    "PWY": "Pathway",
}

PALETTE = [
    "#516FD9",  # Royal Blue
    "#7EACF5",  # Sky Blue
    "#69C39C",  # Mint
    "#6FA430",  # Green
    "#E7C454",  # Yellow
    "#EDB453",  # Amber
    "#ED9353",  # Orange
    "#DA3546",  # Red
    "#9B7DF1",  # Purple
    "#838E9F",  # Gray
]

# Per-panel SVG themes. Data colors stay fixed; only axes/text/spines flip.
THEMES: dict[str, dict[str, str]] = {
    "light": {"ink": "#26251e", "muted": "#57534e"},
    "dark": {"ink": "#ebebeb", "muted": "#a8a29e"},
}

# Whole-token PRO → GEN (Unicode word boundaries: hyphens/pipes/ends ok; not "PROGRAM")
_RELATION_PRO_TO_GEN_PATTERN = r"\bPRO\b"


def load_polled_edges(input_path: Path) -> pl.DataFrame:
    """Load and normalize the polled-edges CSV produced by ``cli evals paperqa``.

    Rows whose ``rating`` or ``is_true_edge`` could not be parsed are dropped;
    these correspond to queries for which the agent returned no usable answer.

    Args:
        input_path: Path to the polled-edges CSV.

    Returns:
        DataFrame with ``rating`` cast to Int32 and ``is_true_edge`` cast to
        Boolean, filtered to rows where both are non-null.

    Raises:
        FileNotFoundError: If ``input_path`` does not exist.
        ValueError: If required columns are missing.
    """
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    df = pl.read_csv(input_path, infer_schema_length=100000)

    required = {"seed_node_type", "is_true_edge", "rating"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Input CSV is missing required columns: {missing}")

    return df.with_columns(
        pl.col("rating").cast(pl.Int32, strict=False),
        pl.col("is_true_edge").cast(pl.Boolean, strict=False),
    ).filter(pl.col("rating").is_not_null() & pl.col("is_true_edge").is_not_null())


def normalize_relation_types(df: pl.DataFrame) -> pl.DataFrame:
    """Replace whole-token ``PRO`` with ``GEN`` in ``relation_type``.

    OptimusKG is gene-centric: protein endpoints are represented as gene nodes,
    so relation labels recorded as ``PRO`` during sampling are reported as
    ``GEN``. Returns the frame unchanged when ``relation_type`` is absent.

    Args:
        df: DataFrame that may contain a ``relation_type`` column.

    Returns:
        DataFrame with normalized relation-type labels.
    """
    if "relation_type" not in df.columns:
        return df
    return df.with_columns(
        pl.when(pl.col("relation_type").is_null())
        .then(None)
        .otherwise(
            pl.col("relation_type").str.replace_all(_RELATION_PRO_TO_GEN_PATTERN, "GEN")
        )
        .alias("relation_type"),
    )


def load_graph(
    nodes_path: Path,
    edges_path: Path,
    graph_mode: GraphMode = "undirected",
) -> tuple[nx.MultiDiGraph, list[str], list[str]]:
    """Build a MultiDiGraph from node and edge parquet files.

    Reads nodes first to populate the graph, then reads edges. Each edge is
    stored with its relation ``label`` as an edge attribute. Because a
    ``MultiDiGraph`` supports parallel edges, multiple relation types between
    the same pair of nodes are kept as distinct edges rather than collapsed
    into a single entry.

    Args:
        nodes_path: Path to nodes.parquet file.
        edges_path: Path to edges.parquet file.
        graph_mode: How to treat edge directionality when building the graph.
            - ``"undirected"`` (default): every edge gets a reverse arc added,
              regardless of the ``undirected`` column. This makes all
              centrality and neighbor queries treat the graph as undirected
              while preserving the MultiDiGraph structure.
            - ``"directed"``: only edges marked ``undirected=true`` in the
              parquet file get a reverse arc added; all other edges are
              one-directional.

    Returns:
        Tuple of:
        - G: Directed MultiDiGraph with ``label`` stored on every edge
        - node_types: Sorted list of unique node type labels
        - edge_types: Sorted list of unique edge type labels
    """
    G = nx.MultiDiGraph()

    # 1. Read nodes first
    nodes_df = pl.read_parquet(nodes_path)
    node_ids = nodes_df["id"].to_list()
    node_types_list = sorted(nodes_df["label"].unique().to_list())
    G.add_nodes_from(node_ids)

    logger.info(
        "Loaded %s nodes with %s node types: %s",
        G.number_of_nodes(),
        len(node_types_list),
        node_types_list,
    )

    # 2. Read edges
    edges_df = pl.read_parquet(edges_path)
    edge_types_list = sorted(edges_df["label"].unique().to_list())

    force_undirected = graph_mode == "undirected"

    for row in edges_df.select("from", "to", "label", "undirected").iter_rows():
        from_id, to_id, label, is_undirected = row

        # Add forward edge, storing relation type as an attribute
        G.add_edge(from_id, to_id, label=label)

        # Add reverse edge when the edge is undirected in the data, or when
        # graph_mode="undirected" forces all edges to be treated as such
        if is_undirected or force_undirected:
            G.add_edge(to_id, from_id, label=label)

    logger.info(
        "Loaded %s edges with %s edge types: %s (graph_mode=%s)",
        G.number_of_edges(),
        len(edge_types_list),
        edge_types_list,
        graph_mode,
    )

    return G, node_types_list, edge_types_list


def load_node_metadata(nodes_path: Path) -> pl.DataFrame:
    """Load node id, label, and name from nodes parquet file.

    Args:
        nodes_path: Path to nodes.parquet file.

    Returns:
        DataFrame with columns: id, label, name
    """
    df = pl.read_parquet(nodes_path)

    # Handle both struct and JSON string properties
    if df.schema["properties"] == pl.String:
        # Properties is JSON string - parse it
        result = df.select(
            "id",
            "label",
            pl.col("properties").str.json_path_match("$.name").alias("name"),
        )
    else:
        # Properties is struct - access field directly
        result = df.select(
            "id",
            "label",
            pl.col("properties").struct.field("name").alias("name"),
        )

    logger.info("Loaded metadata for %s nodes", result.height)
    return result


def compute_centrality(
    G: nx.MultiDiGraph,
    metric: CentralityMetric = "pagerank",
    alpha: float = 0.85,
) -> dict[str, float]:
    """Compute a centrality score for every node in the graph.

    Args:
        G: NetworkX MultiDiGraph.
        metric: Which centrality measure to compute.
            - ``"pagerank"``: PageRank (directed, uses damping factor ``alpha``)
            - ``"degree"``: Normalised total degree (in + out), i.e.
              ``(in_degree + out_degree) / (2 * (N-1))``
            - ``"betweenness"``: Betweenness centrality
            - ``"closeness"``: Closeness centrality
            - ``"eigenvector"``: Eigenvector centrality
        alpha: Damping factor, used only for ``metric="pagerank"``.

    Returns:
        Dictionary mapping node ID to centrality score.
    """
    logger.info("Computing %s centrality...", metric)

    if metric == "pagerank":
        scores = nx.pagerank(G, alpha=alpha)
    elif metric == "degree":
        n = G.number_of_nodes()
        denom = 2 * (n - 1) if n > 1 else 1
        scores = {
            node: (G.in_degree(node) + G.out_degree(node)) / denom for node in G.nodes()
        }
    elif metric == "betweenness":
        scores = nx.betweenness_centrality(G)
    elif metric == "closeness":
        scores = nx.closeness_centrality(G)
    elif metric == "eigenvector":
        scores = nx.eigenvector_centrality(G, max_iter=1000)
    else:
        msg = f"Unknown centrality metric: {metric!r}"
        raise ValueError(msg)

    logger.info("Computed %s centrality for %s nodes", metric, len(scores))
    return scores


def centrality_to_dataframe(
    scores: dict[str, float],
    node_metadata: pl.DataFrame,
) -> pl.DataFrame:
    """Convert centrality scores to a DataFrame joined with node metadata.

    Args:
        scores: Dictionary mapping node ID to centrality score.
        node_metadata: DataFrame with id, label, name columns.

    Returns:
        DataFrame with columns: id, label, name, centrality
        (sorted by centrality descending)
    """
    df = pl.DataFrame({"id": list(scores.keys()), "centrality": list(scores.values())})
    return df.join(node_metadata, on="id", how="left").sort(
        "centrality", descending=True
    )
