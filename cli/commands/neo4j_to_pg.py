import json
import logging
from pathlib import Path

from .types import Edge, Neo4jEdge, Neo4jNode, Node

logger = logging.getLogger("cli")


def neo4j_to_pg(in_path: Path, out_path: Path):
    try:
        with open(in_path) as infile, open(out_path, "w") as outfile:
            for line in infile:
                try:
                    data = json.loads(line)
                    object_type = data.get("type")
                    if object_type == "node":
                        node = Node.from_neo4j(node=Neo4jNode(**data))
                        outfile.write(node.model_dump_json(by_alias=True) + "\n")
                    elif object_type == "relationship":
                        edge = Edge.from_neo4j(edge=Neo4jEdge(**data))
                        outfile.write(edge.model_dump_json(by_alias=True) + "\n")
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
