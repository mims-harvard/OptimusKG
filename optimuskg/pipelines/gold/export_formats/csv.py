import logging

import polars as pl

logger = logging.getLogger(__name__)


def csv_export(
    nodes_dict: dict[str, pl.DataFrame],
    edges_dict: dict[str, pl.DataFrame],
    include_properties: bool = True,
) -> dict[str, pl.DataFrame]:
    """Export knowledge graph to CSV format.

    Args:
        nodes_dict: Dictionary of node type name to DataFrame.
        edges_dict: Dictionary of edge type name to DataFrame.
        include_properties: If True, include properties column (JSON encoded).
                           If False, exclude properties column.

    Returns:
        Dictionary with keys like:
        - "nodes" (consolidated all nodes)
        - "edges" (consolidated all edges)
        - "nodes/gene", "nodes/disease", etc. (individual node files)
        - "edges/anatomy_protein", etc. (individual edge files)
    """
    result: dict[str, pl.DataFrame] = {}

    # Process nodes
    processed_nodes: dict[str, pl.DataFrame] = {}
    for name, df in nodes_dict.items():
        if include_properties:
            processed = (
                df.with_columns(pl.col("properties").struct.json_encode())
                if "properties" in df.columns
                else df
            )
        else:
            processed = df.drop("properties") if "properties" in df.columns else df
        processed_nodes[name] = processed
        result[f"nodes/{name}"] = processed

    # Process edges
    processed_edges: dict[str, pl.DataFrame] = {}
    for name, df in edges_dict.items():
        if include_properties:
            processed = (
                df.with_columns(pl.col("properties").struct.json_encode())
                if "properties" in df.columns
                else df
            )
        else:
            processed = df.drop("properties") if "properties" in df.columns else df
        processed_edges[name] = processed
        result[f"edges/{name}"] = processed

    # Consolidated files
    if processed_nodes:
        result["nodes"] = pl.concat(list(processed_nodes.values()))
    if processed_edges:
        result["edges"] = pl.concat(list(processed_edges.values()))

    return result
