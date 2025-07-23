import polars as pl

from optimuskg.pipelines.gold.nodes.edges.utils import normalize_edge_topology


def export_to_csv(
    edges: list[pl.DataFrame], nodes: list[pl.DataFrame]
) -> tuple[pl.DataFrame, pl.DataFrame]:
    kg_edges = normalize_edge_topology(pl.concat(edges, how="diagonal"))
    kg_nodes = pl.concat(nodes, how="diagonal").unique()

    return kg_edges, kg_nodes
