"""Edge evaluation dataset generator.

Generates evaluation datasets for edge prediction models by:
1. Ranking nodes by PageRank within each node type
2. Sampling nodes from a specific percentile range
3. Creating true/false edge pairs for evaluation
"""

from __future__ import annotations

import json
import logging
import random
import warnings
from pathlib import Path

warnings.filterwarnings(
    "ignore",
    message="Dataset name '.*' contains '.' characters.*",
    category=UserWarning,
)

import matplotlib.pyplot as plt
import networkx as nx
import polars as pl

logger = logging.getLogger(__name__)

# Default hyperparameters
DEFAULT_PAGERANK_UPPER = 5  # Top 5% cutoff
DEFAULT_PAGERANK_LOWER = 15  # Top 15% cutoff
DEFAULT_NODES_PER_TYPE = 100
DEFAULT_TRUE_NEIGHBORS = 10
DEFAULT_FALSE_NEIGHBORS = 5
DEFAULT_SEED = 42


def load_graph(edges_dir: Path) -> nx.Graph:
    """Build an undirected NetworkX graph from edge parquet files."""
    G = nx.Graph()
    for path in sorted(edges_dir.glob("*.parquet")):
        df = pl.read_parquet(path)
        if df.height > 0:
            edge_pairs = df.select("from", "to").iter_rows()
            G.add_edges_from(edge_pairs)
    logger.info(
        "Built graph with %s nodes and %s edges",
        G.number_of_nodes(),
        G.number_of_edges(),
    )
    return G


def load_node_metadata(nodes_dir: Path) -> pl.DataFrame:
    """Load node id, label, and name from all node parquet files."""
    frames: list[pl.DataFrame] = []
    for path in sorted(nodes_dir.glob("*.parquet")):
        df = pl.read_parquet(path)
        if df.height > 0:
            frames.append(
                df.select(
                    "id",
                    "label",
                    pl.col("properties").struct.field("name").alias("name"),
                )
            )
    result = pl.concat(frames)
    logger.info("Loaded metadata for %s nodes", result.height)
    return result


def compute_pagerank_by_type(
    G: nx.Graph, node_metadata: pl.DataFrame
) -> dict[str, pl.DataFrame]:
    """Compute PageRank and return DataFrames stratified by node type.

    Returns a dict mapping node_type -> DataFrame with columns:
    [node_id, node_type, node_name, pagerank, rank_within_type, percentile]
    """
    logger.info("Computing PageRank...")
    pagerank_scores = nx.pagerank(G, alpha=0.85)

    # Create base DataFrame
    pagerank_df = pl.DataFrame(
        {
            "node_id": list(pagerank_scores.keys()),
            "pagerank": list(pagerank_scores.values()),
        }
    )

    # Join with metadata
    pagerank_df = pagerank_df.join(
        node_metadata.rename(
            {"id": "node_id", "label": "node_type", "name": "node_name"}
        ),
        on="node_id",
        how="left",
    )

    # Stratify by node type
    stratified: dict[str, pl.DataFrame] = {}
    node_types = pagerank_df["node_type"].unique().drop_nulls().to_list()

    for node_type in node_types:
        type_df = (
            pagerank_df.filter(pl.col("node_type") == node_type)
            .sort("pagerank", descending=True)
            .with_row_index("rank_within_type", offset=1)
        )
        # Add percentile (rank 1 = 0th percentile, i.e., top node)
        n_nodes = type_df.height
        type_df = type_df.with_columns(
            ((pl.col("rank_within_type") - 1) / n_nodes * 100).alias("percentile")
        )
        stratified[node_type] = type_df

    logger.info("Stratified nodes into %s types", len(stratified))
    return stratified


def plot_pagerank_distributions(
    stratified_data: dict[str, pl.DataFrame], out_path: Path
) -> None:
    """Create a multi-panel figure showing PageRank vs Rank for each node type."""
    n_types = len(stratified_data)
    n_cols = 5
    n_rows = (n_types + n_cols - 1) // n_cols

    fig, axes = plt.subplots(n_rows, n_cols, figsize=(15, 3 * n_rows))
    axes = axes.flatten() if n_types > 1 else [axes]

    sorted_types = sorted(stratified_data.keys())

    for idx, node_type in enumerate(sorted_types):
        ax = axes[idx]
        df = stratified_data[node_type]

        ranks = df["rank_within_type"].to_numpy()
        pageranks = df["pagerank"].to_numpy()

        ax.plot(ranks, pageranks, linewidth=0.8, color=plt.cm.tab10(idx % 10))
        ax.set_yscale("log")
        ax.set_xscale("log")
        ax.set_xlabel("Rank", fontsize=8)
        ax.set_ylabel("PageRank", fontsize=8)
        ax.set_title(f"{node_type}\n(n={len(ranks):,})", fontsize=9, fontweight="bold")
        ax.tick_params(axis="both", labelsize=7)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)

    # Hide unused subplots
    for idx in range(n_types, len(axes)):
        axes[idx].set_visible(False)

    plt.tight_layout()
    plt.savefig(out_path, dpi=300, bbox_inches="tight")
    plt.close(fig)
    logger.info("Saved PageRank distribution plot to %s", out_path)


def sample_nodes_in_percentile(
    stratified_data: dict[str, pl.DataFrame],
    upper_percentile: float,
    lower_percentile: float,
    nodes_per_type: int,
    seed: int,
) -> tuple[pl.DataFrame, dict]:
    """Sample nodes from the specified percentile range for each node type.

    Args:
        stratified_data: Dict mapping node_type -> DataFrame with pagerank data
        upper_percentile: Top X% cutoff (e.g., 5 means top 5%)
        lower_percentile: Bottom cutoff (e.g., 15 means top 15%)
        nodes_per_type: Number of nodes to sample per type
        seed: Random seed

    Returns:
        Tuple of (sampled DataFrame, sampling stats dict)
    """
    random.seed(seed)
    sampled_frames: list[pl.DataFrame] = []
    sampling_stats: dict[str, dict] = {}

    for node_type, df in stratified_data.items():
        # Filter to percentile range
        eligible = df.filter(
            (pl.col("percentile") >= upper_percentile)
            & (pl.col("percentile") <= lower_percentile)
        )

        n_eligible = eligible.height
        n_to_sample = min(nodes_per_type, n_eligible)

        if n_eligible > 0:
            # Sample randomly
            sampled = eligible.sample(n=n_to_sample, seed=seed)
            sampled_frames.append(sampled)

        sampling_stats[node_type] = {
            "total_nodes": df.height,
            "eligible_in_range": n_eligible,
            "sampled": n_to_sample if n_eligible > 0 else 0,
            "percentile_range": [upper_percentile, lower_percentile],
        }

        logger.info(
            "Type '%s': %d eligible, sampled %d",
            node_type,
            n_eligible,
            n_to_sample if n_eligible > 0 else 0,
        )

    if not sampled_frames:
        raise ValueError("No nodes were sampled. Check percentile parameters.")

    result = pl.concat(sampled_frames)
    logger.info("Total sampled nodes: %d", result.height)
    return result, sampling_stats


def sample_edges_for_nodes(
    sampled_nodes: pl.DataFrame,
    G: nx.Graph,
    node_metadata: pl.DataFrame,
    true_neighbors_per_node: int,
    false_neighbors_per_node: int,
    seed: int,
) -> tuple[pl.DataFrame, dict]:
    """Sample true and false edges for each sampled node.

    Args:
        sampled_nodes: DataFrame of sampled seed nodes
        G: NetworkX graph
        node_metadata: DataFrame with node id, label, name
        true_neighbors_per_node: Max true neighbors to sample per node
        false_neighbors_per_node: False neighbors to sample per node
        seed: Random seed

    Returns:
        Tuple of (edges DataFrame, edge sampling stats dict)
    """
    random.seed(seed)

    # Create lookup for node metadata
    metadata_dict = {
        row["id"]: {"type": row["label"], "name": row["name"]}
        for row in node_metadata.iter_rows(named=True)
    }

    all_nodes = set(G.nodes())
    edge_records: list[dict] = []
    edge_stats: dict[str, dict] = {}

    # Track stats by seed node type
    type_stats: dict[str, dict] = {}

    for row in sampled_nodes.iter_rows(named=True):
        seed_id = row["node_id"]
        seed_type = row["node_type"]
        seed_name = row["node_name"]

        # Initialize type stats
        if seed_type not in type_stats:
            type_stats[seed_type] = {
                "true_edges": 0,
                "false_edges": 0,
                "total_neighbors": 0,
                "nodes_with_few_neighbors": 0,
            }

        # Get neighbors
        if seed_id in G:
            neighbors = set(G.neighbors(seed_id))
        else:
            neighbors = set()

        type_stats[seed_type]["total_neighbors"] += len(neighbors)

        # Sample true neighbors
        neighbors_list = list(neighbors)
        n_true = min(true_neighbors_per_node, len(neighbors_list))
        if n_true < true_neighbors_per_node:
            type_stats[seed_type]["nodes_with_few_neighbors"] += 1

        true_sample = random.sample(neighbors_list, n_true) if n_true > 0 else []

        for target_id in true_sample:
            target_meta = metadata_dict.get(target_id, {"type": None, "name": None})
            edge_records.append(
                {
                    "seed_node_id": seed_id,
                    "seed_node_type": seed_type,
                    "seed_node_name": seed_name,
                    "target_node_id": target_id,
                    "target_node_type": target_meta["type"],
                    "target_node_name": target_meta["name"],
                    "is_true_edge": True,
                    "edge_type": "true_neighbor",
                }
            )
            type_stats[seed_type]["true_edges"] += 1

        # Sample false neighbors (non-edges)
        non_neighbors = all_nodes - neighbors - {seed_id}
        non_neighbors_list = list(non_neighbors)
        n_false = min(false_neighbors_per_node, len(non_neighbors_list))
        false_sample = random.sample(non_neighbors_list, n_false) if n_false > 0 else []

        for target_id in false_sample:
            target_meta = metadata_dict.get(target_id, {"type": None, "name": None})
            edge_records.append(
                {
                    "seed_node_id": seed_id,
                    "seed_node_type": seed_type,
                    "seed_node_name": seed_name,
                    "target_node_id": target_id,
                    "target_node_type": target_meta["type"],
                    "target_node_name": target_meta["name"],
                    "is_true_edge": False,
                    "edge_type": "false_neighbor",
                }
            )
            type_stats[seed_type]["false_edges"] += 1

    edges_df = pl.DataFrame(edge_records)

    # Compute summary stats
    total_true = sum(s["true_edges"] for s in type_stats.values())
    total_false = sum(s["false_edges"] for s in type_stats.values())
    total_neighbors = sum(s["total_neighbors"] for s in type_stats.values())
    nodes_with_few = sum(s["nodes_with_few_neighbors"] for s in type_stats.values())

    edge_stats = {
        "total_true_edges": total_true,
        "total_false_edges": total_false,
        "total_edges": total_true + total_false,
        "nodes_with_fewer_than_max_neighbors": nodes_with_few,
        "average_neighbors_per_seed": total_neighbors / sampled_nodes.height
        if sampled_nodes.height > 0
        else 0,
        "by_seed_type": {
            k: {
                "true_edges": v["true_edges"],
                "false_edges": v["false_edges"],
                "avg_neighbors": v["total_neighbors"]
                / sampled_nodes.filter(pl.col("node_type") == k).height
                if sampled_nodes.filter(pl.col("node_type") == k).height > 0
                else 0,
            }
            for k, v in type_stats.items()
        },
    }

    logger.info(
        "Sampled %d true edges and %d false edges",
        total_true,
        total_false,
    )
    return edges_df, edge_stats


def run(
    nodes_dir: Path,
    edges_dir: Path,
    out_dir: Path,
    pagerank_upper: int = DEFAULT_PAGERANK_UPPER,
    pagerank_lower: int = DEFAULT_PAGERANK_LOWER,
    nodes_per_type: int = DEFAULT_NODES_PER_TYPE,
    true_neighbors: int = DEFAULT_TRUE_NEIGHBORS,
    false_neighbors: int = DEFAULT_FALSE_NEIGHBORS,
    seed: int = DEFAULT_SEED,
) -> None:
    """Run edge evaluation dataset generation.

    Args:
        nodes_dir: Directory containing node parquet files
        edges_dir: Directory containing edge parquet files
        out_dir: Output directory for generated files
        pagerank_upper: Upper percentile cutoff (top X%)
        pagerank_lower: Lower percentile cutoff (top X%)
        nodes_per_type: Nodes to sample per type
        true_neighbors: True neighbors per node
        false_neighbors: False neighbors per node
        seed: Random seed
    """
    out_dir.mkdir(parents=True, exist_ok=True)

    # Load data
    logger.info("Loading graph and node metadata...")
    G = load_graph(edges_dir)
    node_metadata = load_node_metadata(nodes_dir)

    # Compute PageRank stratified by type
    stratified_data = compute_pagerank_by_type(G, node_metadata)

    # Plot PageRank distributions
    logger.info("Generating PageRank distribution plots...")
    plot_pagerank_distributions(
        stratified_data, out_dir / "pagerank_distribution_by_type.pdf"
    )

    # Sample nodes
    logger.info(
        "Sampling nodes in percentile range [%d%%, %d%%]...",
        pagerank_upper,
        pagerank_lower,
    )
    sampled_nodes, node_sampling_stats = sample_nodes_in_percentile(
        stratified_data,
        upper_percentile=pagerank_upper,
        lower_percentile=pagerank_lower,
        nodes_per_type=nodes_per_type,
        seed=seed,
    )

    # Save sampled nodes
    sampled_nodes_path = out_dir / "sampled_nodes.csv"
    sampled_nodes.write_csv(sampled_nodes_path)
    logger.info("Saved sampled nodes to %s", sampled_nodes_path)

    # Sample edges
    logger.info("Sampling edges for %d seed nodes...", sampled_nodes.height)
    sampled_edges, edge_sampling_stats = sample_edges_for_nodes(
        sampled_nodes,
        G,
        node_metadata,
        true_neighbors_per_node=true_neighbors,
        false_neighbors_per_node=false_neighbors,
        seed=seed,
    )

    # Save sampled edges
    sampled_edges_path = out_dir / "sampled_edges.csv"
    sampled_edges.write_csv(sampled_edges_path)
    logger.info("Saved sampled edges to %s", sampled_edges_path)

    # Compile and save summary stats
    summary_stats = {
        "parameters": {
            "pagerank_upper_percentile": pagerank_upper,
            "pagerank_lower_percentile": pagerank_lower,
            "nodes_per_type": nodes_per_type,
            "true_neighbors_per_node": true_neighbors,
            "false_neighbors_per_node": false_neighbors,
            "random_seed": seed,
        },
        "graph": {
            "total_nodes": G.number_of_nodes(),
            "total_edges": G.number_of_edges(),
        },
        "node_sampling": {
            "total_sampled_nodes": sampled_nodes.height,
            "by_type": node_sampling_stats,
        },
        "edge_sampling": edge_sampling_stats,
    }

    summary_path = out_dir / "summary_stats.json"
    with open(summary_path, "w") as f:
        json.dump(summary_stats, f, indent=2)
    logger.info("Saved summary stats to %s", summary_path)

    # Print summary to console
    print("\n" + "=" * 60)
    print("EDGE EVALUATION DATASET GENERATION COMPLETE")
    print("=" * 60)
    print(f"\nParameters:")
    print(f"  - PageRank percentile range: [{pagerank_upper}%, {pagerank_lower}%]")
    print(f"  - Nodes per type: {nodes_per_type}")
    print(f"  - True neighbors per node: {true_neighbors}")
    print(f"  - False neighbors per node: {false_neighbors}")
    print(f"  - Random seed: {seed}")
    print(f"\nResults:")
    print(f"  - Sampled nodes: {sampled_nodes.height:,}")
    print(f"  - True edges: {edge_sampling_stats['total_true_edges']:,}")
    print(f"  - False edges: {edge_sampling_stats['total_false_edges']:,}")
    print(f"  - Total edge pairs: {edge_sampling_stats['total_edges']:,}")
    print(f"\nOutput files:")
    print(f"  - {out_dir / 'pagerank_distribution_by_type.pdf'}")
    print(f"  - {out_dir / 'sampled_nodes.csv'}")
    print(f"  - {out_dir / 'sampled_edges.csv'}")
    print(f"  - {out_dir / 'summary_stats.json'}")
    print("=" * 60 + "\n")
