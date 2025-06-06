import json
import logging
from pathlib import Path

from .types import Edge, Neo4jEdge, Neo4jNode, Node

logger = logging.getLogger("cli")


def _transform_node(node: Neo4jNode) -> Node:
    possible_labels = [
        "Gene",
        "AnatomicalEntity",
        "EnvironmentalExposure",
        "Drug",
        "Disease",
        "Phenotype",
        "BiologicalProcess",
        "CellularComponent",
        "MolecularFunction",
        "Pathway",
    ]
    first_matching_label = next(lbl for lbl in node.labels if lbl in possible_labels)

    return Node(
        type="node",
        id=node.properties["id"],
        labels=[first_matching_label],
        properties={
            k: [v] for k, v in node.properties.items() if k in ["name", "source"]
        },
    )


def _transform_edge(edge: Neo4jEdge) -> Edge:
    edge_data = {
        "type": "edge",
        "id": edge.properties.get("id"),
        "from": edge.start.properties.get("id"),
        "to": edge.end.properties.get("id"),
        "labels": [edge.label],
        "properties": {
            k: [v] for k, v in edge.properties.items() if k == "relation_type"
        },
        "undirected": edge.properties.get("undirected", False),
    }
    return Edge(**edge_data)


def neo4j_to_pg(in_path: Path, out_path: Path):
    try:
        with open(in_path) as infile, open(out_path, "w") as outfile:
            for line in infile:
                try:
                    data = json.loads(line)
                    object_type = data.get("type")
                    if object_type == "node":
                        neo4j_node = Neo4jNode(**data)
                        pg_node = _transform_node(neo4j_node)
                        outfile.write(pg_node.model_dump_json(by_alias=True) + "\n")
                    elif object_type == "relationship":
                        neo4j_edge = Neo4jEdge(**data)
                        pg_edge = _transform_edge(neo4j_edge)
                        outfile.write(pg_edge.model_dump_json(by_alias=True) + "\n")
                    else:
                        logger.warning(
                            f"Skipping line with unrecognized type '{object_type}': {line.strip()}"
                        )
                except json.JSONDecodeError:
                    logger.error(f"Error decoding JSON from line: {line.strip()}")
                except Exception as e:
                    logger.error(f"Error processing line: {line.strip()} - {e}")
        logger.info(f"Successfully converted {in_path} to {out_path}")
    except FileNotFoundError:
        logger.error(f"Input file not found: {in_path}")
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
