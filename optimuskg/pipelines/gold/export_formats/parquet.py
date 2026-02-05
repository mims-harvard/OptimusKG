import logging

import polars as pl

logger = logging.getLogger(__name__)


def parquet_export(
    nodes_dict: dict[str, pl.DataFrame],
    edges_dict: dict[str, pl.DataFrame],
    include_properties: bool = True,
) -> dict[str, pl.DataFrame]:
    """Export knowledge graph to Parquet format.

    Args:
        nodes_dict: Dictionary of node type name to DataFrame.
        edges_dict: Dictionary of edge type name to DataFrame.
        include_properties: If True, include properties column (as native struct for
                           individual files, JSON-encoded for consolidated files).
                           If False, exclude properties column.

    Returns:
        Dictionary with keys like:
        - "nodes" (consolidated all nodes, properties JSON-encoded if included)
        - "edges" (consolidated all edges, properties JSON-encoded if included)
        - "nodes/gene", "nodes/disease", etc. (individual files, properties as struct)
        - "edges/anatomy_protein", etc. (individual files, properties as struct)
    """
    result: dict[str, pl.DataFrame] = {}

    processed_nodes: dict[str, pl.DataFrame] = {}
    for name, df in nodes_dict.items():
        if include_properties:
            processed = df
        else:
            processed = df.drop("properties") if "properties" in df.columns else df
        processed_nodes[name] = processed
        result[f"nodes/{name}"] = processed

    processed_edges: dict[str, pl.DataFrame] = {}
    for name, df in edges_dict.items():
        if include_properties:
            processed = df
        else:
            processed = df.drop("properties") if "properties" in df.columns else df
        processed_edges[name] = processed
        result[f"edges/{name}"] = processed

    # Consolidated files - JSON encode properties for concatenation
    # (different types have different property schemas)
    if processed_nodes:
        if include_properties:
            consolidated_nodes = [
                df.with_columns(pl.col("properties").struct.json_encode())
                if "properties" in df.columns
                else df
                for df in processed_nodes.values()
            ]
            result["nodes"] = pl.concat(consolidated_nodes)
        else:
            result["nodes"] = pl.concat(list(processed_nodes.values()))

    if processed_edges:
        if include_properties:
            consolidated_edges = [
                df.with_columns(pl.col("properties").struct.json_encode())
                if "properties" in df.columns
                else df
                for df in processed_edges.values()
            ]
            result["edges"] = pl.concat(consolidated_edges)
        else:
            result["edges"] = pl.concat(list(processed_edges.values()))

    return result
