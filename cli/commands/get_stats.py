import json
import logging
from pathlib import Path

from pydantic import BaseModel, Field, model_serializer

from .types import Edge, Node

logger = logging.getLogger("cli")


class LabelMetricsEntry(BaseModel):
    type: str
    count: int
    percentage: float


class EdgeTopologyMetrics(BaseModel):
    num_bidirectional_edges: int = Field(
        description="Number of edges part of a symmetric pair (X->Y and Y->X, where X!=Y). This counts all participating edges."
    )
    num_self_loops: int = Field(
        description="Number of edges where the source and target node are the same (X->X)."
    )
    num_duplicated_edges: int = Field(
        description="Number of 'extra' non-self-looping edge instances that share the same source, target, and labels as another edge."
    )

    @model_serializer
    def serialize_model(self):
        output = {}
        for field_name, field_info in self.model_fields.items():
            value = getattr(self, field_name)
            description = field_info.description
            output[field_name] = {
                "value": value,
                "description": description if description else "",
            }
        return output


class EdgeMetrics(BaseModel):
    topology_metrics: EdgeTopologyMetrics = Field(
        description="Metrics related to the traversal of edges."
    )
    label_metrics: list[LabelMetricsEntry] = Field(
        description="Count and percentage of edges by their label"
    )


class NodeMetrics(BaseModel):
    label_metrics: list[LabelMetricsEntry] = Field(
        description="List of node label combinations and their counts. The label combination is the sorted labels concatenated with '|'."
    )


class Metrics(BaseModel):
    connected: bool = Field(
        description="Whether the graph is connected or not. An empty graph or a graph with a single node is considered connected."
    )
    total_elements: int = Field(
        description="Total number of elements (nodes + edges) in the file."
    )
    num_nodes: int = Field(description="Number of nodes in the file.")
    num_edges: int = Field(description="Number of edges in the file.")
    processed_lines: int = Field(
        description="Number of lines processed from the input file."
    )
    malformed_lines: int = Field(
        description="Number of lines that were malformed and skipped."
    )
    node_metrics: NodeMetrics
    edge_metrics: EdgeMetrics


def _join_labels(labels: list[str] | None) -> str:
    """Joins a list of labels into a single string, sorted and joined by '|'. Returns '<NO_LABEL>' if labels is None."""
    return "|".join(sorted(labels)) if labels else "<NO_LABEL>"


def _count_element_labels(labels_count: dict[str, int], element: Node | Edge) -> None:
    """Updates the labels_count dictionary based on the element's labels."""
    label_key = _join_labels(element.labels)
    labels_count[label_key] = labels_count.get(label_key, 0) + 1


def _get_element_metrics(
    element_label_metrics: dict[str, int], num_elements: int
) -> list[LabelMetricsEntry]:
    """Returns a list of LabelMetricsEntry objects sorted by label."""
    element_label_stats_list: list[LabelMetricsEntry] = []
    for labels, count in sorted(element_label_metrics.items()):
        percentage = (count / num_elements) * 100
        element_label_stats_list.append(
            LabelMetricsEntry(
                type=labels,
                count=count,
                percentage=round(percentage, 5),
            )
        )
    return element_label_stats_list


def _process_input_file(in_path: Path) -> Metrics:
    """
    Reads the input file, parses JSON objects, and categorizes them.
    """
    num_nodes = 0
    num_edges = 0
    all_edges: list[Edge] = []
    all_node_ids: set[str] = set()  # Collect unique node IDs
    malformed_lines = 0
    processed_lines = 0
    node_label_metrics: dict[str, int] = {}
    edge_label_metrics: dict[str, int] = {}

    try:
        with open(in_path, encoding="utf-8") as infile:
            for line_idx, line_content in enumerate(infile):
                processed_lines += 1
                try:
                    data = json.loads(line_content)
                    object_type = data.get("type")

                    if object_type == "node":
                        node = Node(**data)
                        num_nodes += 1
                        all_node_ids.add(node.id)  # Add node ID to the set
                        _count_element_labels(node_label_metrics, node)
                    elif object_type == "edge":
                        edge = Edge(**data)
                        num_edges += 1
                        _count_element_labels(edge_label_metrics, edge)
                        all_edges.append(edge)
                    else:
                        logger.warning(
                            f"Skipping line {line_idx + 1} with unrecognized type '{object_type}': {line_content.strip()}"
                        )
                        malformed_lines += 1
                except json.JSONDecodeError:
                    logger.error(
                        f"Error decoding JSON from line {line_idx + 1}: {line_content.strip()}"
                    )
                    malformed_lines += 1
                except Exception as e:
                    logger.error(
                        f"Error processing line {line_idx + 1} ('{line_content.strip()}'): {type(e).__name__} - {e}"
                    )
                    malformed_lines += 1
    except FileNotFoundError:
        logger.error(f"Input file not found: {in_path}")
        raise
    except Exception as e:
        logger.error(
            f"An unexpected error occurred while reading or processing {in_path}: {type(e).__name__} - {e}",
            exc_info=True,
        )
        raise

    logger.info(
        f"Finished processing {in_path}. Total lines: {processed_lines}. Nodes: {num_nodes}, Edges: {num_edges}. Malformed/Skipped: {malformed_lines}."
    )

    node_label_metrics: list[LabelMetricsEntry] = _get_element_metrics(
        node_label_metrics, num_nodes
    )
    edge_label_metrics: list[LabelMetricsEntry] = _get_element_metrics(
        edge_label_metrics, num_edges
    )

    edge_topology_metrics: EdgeTopologyMetrics = _calculate_edge_topology_metrics(
        all_edges
    )
    is_connected: bool = _is_graph_connected(all_node_ids, all_edges)

    return Metrics(
        total_elements=num_nodes + num_edges,
        num_nodes=num_nodes,
        num_edges=num_edges,
        processed_lines=processed_lines,
        malformed_lines=malformed_lines,
        node_metrics=NodeMetrics(label_metrics=node_label_metrics),
        edge_metrics=EdgeMetrics(
            topology_metrics=edge_topology_metrics,
            label_metrics=edge_label_metrics,
        ),
        connected=is_connected,
    )


def _is_graph_connected(all_node_ids: set[str], all_edges: list[Edge]) -> bool:
    """Checks if the graph is connected using DFS."""
    if not all_node_ids:
        return True  # An empty graph is considered connected
    if len(all_node_ids) == 1:
        return True  # A graph with a single node is considered connected

    adj: dict[str, list[str]] = {node_id: [] for node_id in all_node_ids}
    for edge in all_edges:
        # Ensure both source and target nodes are in the graph's node set
        if edge.from_node in all_node_ids and edge.to_node in all_node_ids:
            adj[edge.from_node].append(edge.to_node)
            adj[edge.to_node].append(edge.from_node)  # For undirected connectivity

    start_node = next(iter(all_node_ids))  # Pick an arbitrary start node
    visited: set[str] = set()
    stack: list[str] = [start_node]
    visited.add(start_node)

    while stack:
        current_node = stack.pop()
        for neighbor in adj.get(current_node, []):
            if neighbor not in visited:
                visited.add(neighbor)
                stack.append(neighbor)

    return len(visited) == len(all_node_ids)


def _calculate_edge_topology_metrics(all_edges: list[Edge]) -> EdgeTopologyMetrics:
    if not all_edges:
        return EdgeTopologyMetrics(
            num_bidirectional_edges=0, num_self_loops=0, num_duplicated_edges=0
        )

    # 1. Identify and count self-looping edges
    non_self_loop_edges = [edge for edge in all_edges if edge.from_node != edge.to_node]

    # 2. Identify and count duplicated edges from non-self-loop edges
    # An edge is duplicated if it's a non-self-loop edge with the same from_node, to_node, and labels as another.
    # We count the 'extra' instances.
    edge_key_to_first_edge: dict[str, Edge] = {}
    unique_non_self_loop_edges: list[Edge] = []

    for edge in non_self_loop_edges:
        edge_key = f"{edge.id}_{edge.from_node}_{edge.to_node}"
        if edge_key not in edge_key_to_first_edge:
            edge_key_to_first_edge[edge_key] = edge
            unique_non_self_loop_edges.append(edge)

    # 3. Calculate Bidirectional Edges (from unique_non_self_loop_edges)
    # Create a map of edge pairs to find bidirectional edges
    edge_pairs = {
        f"{edge.from_node}_{edge.to_node}": edge.id
        for edge in unique_non_self_loop_edges
    }

    bidirectional_edge_ids = set()
    for edge in unique_non_self_loop_edges:
        reverse_key = f"{edge.to_node}_{edge.from_node}"
        if reverse_key in edge_pairs:
            bidirectional_edge_ids.add(edge.id)
            bidirectional_edge_ids.add(edge_pairs[reverse_key])

    return EdgeTopologyMetrics(
        num_bidirectional_edges=len(bidirectional_edge_ids),
        num_self_loops=len(all_edges) - len(non_self_loop_edges),
        num_duplicated_edges=len(non_self_loop_edges) - len(unique_non_self_loop_edges),
    )


def _write_stats_to_file(
    out_path: Path,
    metrics: Metrics,
):
    """Writes the statistics data (Pydantic model) to a JSON file."""
    try:
        with open(out_path, "w", encoding="utf-8") as outfile:
            json.dump(metrics.model_dump(mode="json"), outfile, indent=4)
        logger.info(f"Successfully wrote statistics to {out_path}")
    except Exception as e:
        logger.error(
            f"Failed to write statistics to {out_path}: {type(e).__name__} - {e}",
            exc_info=True,
        )
        raise


def get_stats(in_path: Path, out_path: Path):
    logger.info(
        f"Attempting to process statistics for {in_path}, output will be written to {out_path}"
    )

    try:
        metrics: Metrics = _process_input_file(in_path)
        _write_stats_to_file(out_path, metrics)

    except Exception as e:
        logger.error(
            f"An unexpected error occurred during statistics generation for {in_path}: {type(e).__name__} - {e}",
            exc_info=True,
        )
