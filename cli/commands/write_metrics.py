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
    avg_degree: float | None = Field(
        default=None, description="Average degree for nodes of this type."
    )
    std_dev_degree: float | None = Field(
        default=None, description="Standard deviation of degree for nodes of this type."
    )


class EdgeTopologyMetrics(BaseModel):
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
    label_type: list[LabelMetricsEntry] = Field(
        description="Count and percentage of edges by their label"
    )


class NodeMetrics(BaseModel):
    label_type: list[LabelMetricsEntry] = Field(
        description="List of node label combinations and their counts. The label combination is the sorted labels concatenated with '|'."
    )


class Metrics(BaseModel):
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
    element_label_type: dict[str, int],
    num_elements: int,
    degrees_by_label: dict[str, list[int]] | None = None,
) -> list[LabelMetricsEntry]:
    """Returns a list of LabelMetricsEntry objects sorted by label."""
    element_label_stats_list: list[LabelMetricsEntry] = []
    for labels, count in sorted(element_label_type.items()):
        percentage = (count / num_elements) * 100 if num_elements > 0 else 0

        avg_degree, std_dev_degree = None, None
        if degrees_by_label:
            degrees = degrees_by_label.get(labels, [])
            if degrees:
                mean = sum(degrees) / len(degrees)
                variance = sum((x - mean) ** 2 for x in degrees) / len(degrees)
                std_dev = variance**0.5
                avg_degree = round(mean, 5)
                std_dev_degree = round(std_dev, 5)

        element_label_stats_list.append(
            LabelMetricsEntry(
                type=labels,
                count=count,
                percentage=round(percentage, 5),
                avg_degree=avg_degree,
                std_dev_degree=std_dev_degree,
            )
        )
    return element_label_stats_list


def _process_input_file(in_path: Path) -> Metrics:  # noqa: PLR0915
    """
    Reads the input file, parses JSON objects, and categorizes them.
    """
    num_nodes = 0
    num_edges = 0
    all_edges: list[Edge] = []
    all_node_ids: set[str] = set()  # Collect unique node IDs
    malformed_lines = 0
    processed_lines = 0
    node_label_type: dict[str, int] = {}
    edge_label_type: dict[str, int] = {}
    node_id_to_label: dict[str, str] = {}

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
                        all_node_ids.add(node.id)
                        _count_element_labels(node_label_type, node)
                        node_id_to_label[node.id] = _join_labels(node.labels)
                    elif object_type == "edge":
                        edge = Edge(**data)
                        num_edges += 1
                        _count_element_labels(edge_label_type, edge)
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

    from collections import defaultdict

    node_degrees: dict[str, int] = defaultdict(int)
    for edge in all_edges:
        node_degrees[edge.from_node] += 1
        node_degrees[edge.to_node] += 1

    degrees_by_label: dict[str, list[int]] = defaultdict(list)
    for node_id in all_node_ids:
        label = node_id_to_label.get(node_id)
        if label:
            degrees_by_label[label].append(node_degrees.get(node_id, 0))

    edge_topology_metrics: EdgeTopologyMetrics = _get_edge_topology_metrics(all_edges)

    return Metrics(
        total_elements=num_nodes + num_edges,
        num_nodes=num_nodes,
        num_edges=num_edges,
        processed_lines=processed_lines,
        malformed_lines=malformed_lines,
        node_metrics=NodeMetrics(
            label_type=_get_element_metrics(
                node_label_type, num_nodes, degrees_by_label
            )
        ),
        edge_metrics=EdgeMetrics(
            topology_metrics=edge_topology_metrics,
            label_type=_get_element_metrics(edge_label_type, num_edges),
        ),
    )


def _get_edge_topology_metrics(all_edges: list[Edge]) -> EdgeTopologyMetrics:
    """Returns the topology metrics for the edges."""
    if not all_edges:
        return EdgeTopologyMetrics(
            directed=0, undirected=0, bidirectional=0, loops=0, duplicated=0
        )

    edge_keys = set()
    reverse_keys = set()
    metrics = EdgeTopologyMetrics(
        directed=0, undirected=0, bidirectional=0, loops=0, duplicated=0
    )

    for edge in all_edges:
        key_suffix = (
            f"_{_join_labels(edge.labels)}_{edge.properties.get('relation_type', '')}"
        )
        key = f"{edge.from_node}_{edge.to_node}{key_suffix}"
        reverse_key = f"{edge.to_node}_{edge.from_node}{key_suffix}"

        if edge.from_node == edge.to_node:
            metrics.loops += 1
        elif key in edge_keys:
            metrics.duplicated += 1
        elif edge.undirected:
            metrics.undirected += 1
        else:
            metrics.directed += 1
            if reverse_key in edge_keys:
                metrics.bidirectional += 1

        edge_keys.add(key)
        reverse_keys.add(reverse_key)

    return metrics


def write_metrics(in_path: Path, out_path: Path):
    """Processes input file and writes the statistics data (Pydantic model) to a JSON file."""
    logger.info(
        f"Attempting to process statistics for {in_path}, output will be written to {out_path}"
    )

    try:
        metrics: Metrics = _process_input_file(in_path)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with open(out_path, "w", encoding="utf-8") as outfile:
            json.dump(metrics.model_dump(mode="json"), outfile, indent=4)
        logger.info(f"Successfully wrote statistics to {out_path}")

    except Exception as e:
        logger.error(
            f"An error occurred processing {in_path} or writing to {out_path}: {type(e).__name__} - {e}",
            exc_info=True,
        )
        raise
