import json
import logging
from pathlib import Path

import polars as pl
from pydantic import BaseModel, Field, model_serializer

logger = logging.getLogger("cli")


class Statistics(BaseModel):
    avg: float | None
    std: float | None
    total: int


class DataOverview(BaseModel):
    total_elements: int
    total_properties: int
    num_nodes: int
    num_edges: int


class NodeMetrics(BaseModel):
    label: str
    count: int
    percentage: float
    properties: Statistics
    degree: Statistics
    sources: dict[str, int]
    ontologies: dict[str, int]
    degree_counts: list[int]


class EdgeMetrics(BaseModel):
    label: str
    count: int
    percentage: float
    properties: Statistics
    sources: dict[str, int]
    ontologies: dict[str, int]
    directed_count: int
    undirected_count: int


class TopologyMetrics(BaseModel):
    directed: int = Field(description="Number of edges that have a direction (X->Y).")
    undirected: int = Field(
        description="Number of edges that do not have a direction (X-Y)."
    )
    bidirectional: int = Field(
        description="Number of edges part of a symmetric pair (X->Y and Y->X, where X!=Y). This counts all participating edges."
    )
    loops: int = Field(
        description="Number of edges where the source and target node are the same (X->X)."
    )
    duplicated: int = Field(
        description="Number of extra non-self-looping edge instances that share the same source, target, and labels as another edge."
    )

    @model_serializer
    def serialize_model(self):
        output = {}
        for field_name, field_info in self.__class__.model_fields.items():
            value = getattr(self, field_name)
            description = field_info.description
            output[field_name] = {
                "value": value,
                "description": description if description else "",
            }
        return output


def _get_ontology_prefix(id: str) -> str:
    """Returns the ontology prefix of an ID."""
    # TODO: Maybe this can be less hardcoded when we switch to identifiers.org for the IDs
    if ":" in id:
        return id.split(":", 1)[0]
    elif "_" in id:
        return id.split("_", 1)[0]
    elif "CHEMBL" in id:
        return "CHEMBL"
    elif "ENSG" in id:
        return "Ensembl"
    return ""


def get_node_metrics(
    nodes: list[pl.DataFrame], edges: list[pl.DataFrame] = None
) -> list[NodeMetrics]:
    node_metrics = []

    # Calculate node degrees from edges if provided
    node_degrees = {}
    if edges:
        for edge_df in edges:
            # Count degrees for each node from "from" and "to" columns
            from_degrees = edge_df["from"].value_counts()
            to_degrees = edge_df["to"].value_counts()

            # Combine degrees (a node can appear as both from and to)
            for node_id, count in from_degrees.iter_rows():
                node_degrees[node_id] = node_degrees.get(node_id, 0) + count
            for node_id, count in to_degrees.iter_rows():
                node_degrees[node_id] = node_degrees.get(node_id, 0) + count

    for df in nodes:
        df_unnested = df.unnest("properties")

        # Calculate properties statistics based on the number of non-null fields in properties
        properties_count = df_unnested.select(
            [
                pl.sum_horizontal(
                    [
                        pl.col(col).is_not_null().cast(pl.Int32)
                        for col in df_unnested.columns
                        if col
                        in [field.name for field in df["properties"].dtype.fields]
                    ]
                )
            ]
        ).to_series()

        avg_properties = Statistics(
            avg=properties_count.mean(),
            std=properties_count.std(),
            total=properties_count.sum(),
        )

        # Collect sources from both 'sources' list and 'source' string columns
        all_sources = []

        # Handle 'sources' column (list of strings) - explode and collect
        if "sources" in df_unnested.columns:
            sources_from_list = (
                df_unnested.select("sources")
                .filter(pl.col("sources").is_not_null())
                .explode("sources")["sources"]
                .to_list()
            )
            all_sources.extend([s for s in sources_from_list if s])

        # Handle 'source' column (single string) - collect directly
        if "source" in df_unnested.columns:
            sources_from_string = (
                df_unnested.select("source")
                .filter(pl.col("source").is_not_null())["source"]
                .to_list()
            )
            all_sources.extend([s for s in sources_from_string if s])

        # Count sources
        sources_counts = {}
        for source in all_sources:
            sources_counts[source] = sources_counts.get(source, 0) + 1

        # Calculate degree statistics for nodes of this type
        node_label = df["node_type"].unique().item()
        node_ids_in_df = df["id"].to_list()
        degrees_for_label = [node_degrees.get(node_id, 0) for node_id in node_ids_in_df]

        degree_stats = Statistics(avg=None, std=None, total=0)
        if degrees_for_label and node_degrees:  # Only calculate if we have edge data
            mean_degree = sum(degrees_for_label) / len(degrees_for_label)
            if len(degrees_for_label) > 1:
                variance = sum((x - mean_degree) ** 2 for x in degrees_for_label) / len(
                    degrees_for_label
                )
                std_degree = variance**0.5
            else:
                std_degree = 0.0
            degree_stats = Statistics(
                avg=mean_degree, std=std_degree, total=sum(degrees_for_label)
            )

        node_metrics.append(
            NodeMetrics(
                label=node_label,
                count=df.height,
                percentage=df.height / sum(df.height for df in nodes),
                properties=avg_properties,
                degree=degree_stats,
                sources=sources_counts,
                ontologies=dict(
                    df["id"]
                    .map_elements(_get_ontology_prefix, return_dtype=pl.String)
                    .value_counts()
                    .iter_rows()
                ),
                degree_counts=degrees_for_label,
            )
        )
    # Sort by count in descending order (bigger to smaller)
    return sorted(node_metrics, key=lambda x: x.count, reverse=True)


def get_edge_metrics(edges: list[pl.DataFrame]) -> list[EdgeMetrics]:
    edge_metrics = []
    for df in edges:
        df_unnested = df.unnest("properties")

        # Calculate properties statistics based on the number of non-null fields in properties
        properties_count = df_unnested.select(
            [
                pl.sum_horizontal(
                    [
                        pl.col(col).is_not_null().cast(pl.Int32)
                        for col in df_unnested.columns
                        if col
                        in [field.name for field in df["properties"].dtype.fields]
                    ]
                )
            ]
        ).to_series()

        avg_properties = Statistics(
            avg=properties_count.mean(),
            std=properties_count.std(),
            total=properties_count.sum(),
        )

        # Collect sources from 'sources' list column only (edges don't have 'source' string column)
        all_sources = []

        # Handle 'sources' column (list of strings) - explode and collect
        if "sources" in df_unnested.columns:
            sources_from_list = (
                df_unnested.select("sources")
                .filter(pl.col("sources").is_not_null())
                .explode("sources")["sources"]
                .to_list()
            )
            all_sources.extend([s for s in sources_from_list if s])

        # Count sources (for edges, keep only prefix if source has ":")
        sources_counts = {}
        for source in all_sources:
            # If source has ":", only keep the prefix part
            source_key = source.split(":", 1)[0] if ":" in source else source
            sources_counts[source_key] = sources_counts.get(source_key, 0) + 1

        # Collect ontologies from both "from" and "to" IDs
        from_ontologies = (
            df["from"]
            .map_elements(_get_ontology_prefix, return_dtype=pl.String)
            .value_counts()
        )
        to_ontologies = (
            df["to"]
            .map_elements(_get_ontology_prefix, return_dtype=pl.String)
            .value_counts()
        )

        # Combine ontology counts from both from and to IDs
        combined_ontologies = {}
        for ontology, count in from_ontologies.iter_rows():
            combined_ontologies[ontology] = combined_ontologies.get(ontology, 0) + count
        for ontology, count in to_ontologies.iter_rows():
            combined_ontologies[ontology] = combined_ontologies.get(ontology, 0) + count

        # Calculate directed and undirected counts from the "undirected" property
        directed_count = (
            df_unnested.filter(~pl.col("undirected")).height
            if "undirected" in df_unnested.columns
            else df.height
        )
        undirected_count = (
            df_unnested.filter(pl.col("undirected")).height
            if "undirected" in df_unnested.columns
            else 0
        )

        edge_metrics.append(
            EdgeMetrics(
                label=df["relation"].unique().item(),
                count=df.height,
                percentage=df.height / sum(df.height for df in edges),
                properties=avg_properties,
                sources=sources_counts,
                ontologies=combined_ontologies,
                directed_count=directed_count,
                undirected_count=undirected_count,
            )
        )
    # Sort by count in descending order (bigger to smaller)
    return sorted(edge_metrics, key=lambda x: x.count, reverse=True)


def _format_dict_for_markdown(d: dict[str, int]) -> str:
    """Format a dictionary for markdown display, showing all items."""
    if not d:
        return "None"

    # Sort by count (descending) and show all items
    sorted_items = sorted(d.items(), key=lambda x: x[1], reverse=True)
    formatted = ", ".join([f"{k} ({v})" for k, v in sorted_items])

    return formatted


def _format_statistics_for_markdown(stats: Statistics) -> str:
    """Format Statistics object for markdown display."""
    if stats.avg is None or stats.std is None:
        return "N/A"
    return f"{stats.avg:.2f} ± {stats.std:.2f}"


def export_metrics_to_markdown(
    node_metrics: list[NodeMetrics],
    edge_metrics: list[EdgeMetrics],
    output_path: Path,
    nodes: list[pl.DataFrame] = None,
    edges: list[pl.DataFrame] = None,
) -> None:
    """Export metrics to a markdown file with formatted tables."""

    markdown_content = []
    markdown_content.append("# Knowledge Graph Metrics Report\n")

    # Calculate summary metrics
    total_nodes = sum(metric.count for metric in node_metrics)
    total_edges = sum(metric.count for metric in edge_metrics)
    total_elements = total_nodes + total_edges

    # Calculate actual total properties by counting non-null properties in all dataframes
    total_properties = 0

    # Count properties in nodes
    if nodes:
        for df in nodes:
            df_unnested = df.unnest("properties")
            properties_count = df_unnested.select(
                [
                    pl.sum_horizontal(
                        [
                            pl.col(col).is_not_null().cast(pl.Int32)
                            for col in df_unnested.columns
                            if col
                            in [field.name for field in df["properties"].dtype.fields]
                        ]
                    )
                ]
            ).to_series()
            total_properties += properties_count.sum()

    # Count properties in edges
    if edges:
        for df in edges:
            df_unnested = df.unnest("properties")
            properties_count = df_unnested.select(
                [
                    pl.sum_horizontal(
                        [
                            pl.col(col).is_not_null().cast(pl.Int32)
                            for col in df_unnested.columns
                            if col
                            in [field.name for field in df["properties"].dtype.fields]
                        ]
                    )
                ]
            ).to_series()
            total_properties += properties_count.sum()

    # Data Overview Table at the top
    markdown_content.append("## Data Overview\n")
    markdown_content.append("| Metric | Value |")
    markdown_content.append("|--------|-------|")
    markdown_content.append(f"| Total Elements | {total_elements:,} |")
    markdown_content.append(f"| Total Properties | {total_properties:,} |")
    markdown_content.append(f"| Number of Nodes | {total_nodes:,} |")
    markdown_content.append(f"| Number of Edges | {total_edges:,} |")
    markdown_content.append("")

    # Node Metrics Table
    markdown_content.append("## Node Metrics\n")
    markdown_content.append(
        "| Label | Count | Percentage | Avg Degree | Avg Properties | Sources | Ontologies |"
    )
    markdown_content.append(
        "|-------|-------|------------|------------|----------------|---------|------------|"
    )

    for metric in node_metrics:
        sources_str = _format_dict_for_markdown(metric.sources)
        ontologies_str = _format_dict_for_markdown(metric.ontologies)
        properties_str = _format_statistics_for_markdown(metric.properties)
        degree_str = _format_statistics_for_markdown(metric.degree)

        markdown_content.append(
            f"| {metric.label} | {metric.count:,} | {metric.percentage:.2%} | "
            f"{degree_str} | {properties_str} | {sources_str} | {ontologies_str} |"
        )

    markdown_content.append("")

    # Edge Metrics Table
    markdown_content.append("## Edge Metrics\n")
    markdown_content.append(
        "| Label | Count | Percentage | Directed | Undirected | Avg Properties | Sources | Ontologies |"
    )
    markdown_content.append(
        "|-------|-------|------------|----------|------------|----------------|---------|------------|"
    )

    for metric in edge_metrics:
        sources_str = _format_dict_for_markdown(metric.sources)
        ontologies_str = _format_dict_for_markdown(metric.ontologies)
        properties_str = _format_statistics_for_markdown(metric.properties)

        markdown_content.append(
            f"| {metric.label} | {metric.count:,} | {metric.percentage:.2%} | "
            f"{metric.directed_count:,} | {metric.undirected_count:,} | "
            f"{properties_str} | {sources_str} | {ontologies_str} |"
        )

    # Write to file
    with output_path.open("w") as f:
        f.write("\n".join(markdown_content))


def metrics_command(nodes_dir: Path, edges_dir: Path, out_dir: Path) -> None:
    """Parse node and edge parquet files and write metrics to file."""
    logger.info(
        f"Parsing node and edge parquet files from {nodes_dir} and {edges_dir}..."
    )

    nodes = []
    edges = []

    for node_file in nodes_dir.glob("*.parquet"):
        df = pl.read_parquet(node_file)
        nodes.append(df)

    for edge_file in edges_dir.glob("*.parquet"):
        df = pl.read_parquet(edge_file)
        edges.append(df)

    logger.info("Calculating node and edge metrics...")
    node_metrics = get_node_metrics(nodes, edges)
    edge_metrics = get_edge_metrics(edges)

    metrics = {
        "node_metrics": [metric.model_dump() for metric in node_metrics],
        "edge_metrics": [metric.model_dump() for metric in edge_metrics],
    }

    logger.info(f"Writing metrics to {out_dir}...")
    out_dir.mkdir(parents=True, exist_ok=True)

    # Export JSON
    with (out_dir / "metrics.json").open("w") as f:
        json.dump(metrics, f, indent=2)

    # Export Markdown
    export_metrics_to_markdown(
        node_metrics, edge_metrics, out_dir / "metrics.md", nodes, edges
    )
    logger.info(
        f"Metrics exported to {out_dir / 'metrics.json'} and {out_dir / 'metrics.md'}"
    )
