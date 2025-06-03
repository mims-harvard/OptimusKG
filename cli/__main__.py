import json
import logging
from pathlib import Path
from typing import Annotated, Literal

import typer
from pydantic import BaseModel, Field

from optimuskg.utils import calculate_checksum

app = typer.Typer(help="Main entry point for the CLI.")

logger = logging.getLogger("cli")


Identifier = Annotated[str, Field(min_length=1)]
PropertyValue = str | float | bool
PropertyArray = list[PropertyValue]
Properties = dict[Identifier, PropertyArray]
Labels = list[Identifier]


class Node(BaseModel):
    type: Literal["node"]
    id: Identifier
    labels: Labels
    properties: Properties


class Edge(BaseModel):
    type: Literal["edge"]
    id: Identifier | None = None
    from_node: Identifier = Field(alias="from")
    to_node: Identifier = Field(alias="to")
    labels: Labels
    properties: Properties
    undirected: bool | None = True


class _Neo4jNode(BaseModel):
    id: Identifier
    labels: Labels
    properties: dict[Identifier, str]


class Neo4jNode(_Neo4jNode):
    type: Literal["node"]


class Neo4jEdge(BaseModel):
    type: Literal["relationship"]
    id: Identifier
    label: Identifier
    properties: dict[Identifier, str]
    start: _Neo4jNode
    end: _Neo4jNode


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
        "type": Literal["edge"],
        "id": edge.properties.get("id"),
        "from": edge.start.properties.get("id"),
        "to": edge.end.properties.get("id"),
        "labels": [edge.label],
        "properties": {
            k: [v] for k, v in edge.properties.items() if k == "display_relation"
        },
    }
    return Edge(**edge_data)


@app.command(help="Log the checksum of a file or directory.")
def checksum(  # noqa: PLR0913
    path: Path,
    checksum: str = typer.Option(
        None, "--checksum", help="The checksum to compare the file to."
    ),
    chunk_size: int = typer.Option(
        8192, "--chunk-size", help="The size of the chunks to read from the file."
    ),
    digest_size: int = typer.Option(
        16, "--digest-size", help="The size of the digest to use for the checksum."
    ),
    dir: bool = typer.Option(
        False, "--dir", help="Generate one checksum of all files in the directory."
    ),
):
    try:
        actual_checksum = calculate_checksum(
            path=path,
            chunk_size=chunk_size,
            digest_size=digest_size,
            process_directory=dir,
        )
        display_path = f"directory '{path}'" if dir else f"'{path}'"

        if not checksum:
            logger.info(f"The checksum of {display_path} is: {actual_checksum}")
        elif checksum == actual_checksum:
            logger.info(f"The checksum of {display_path} is correct")
        else:
            logger.error(
                f"The checksums do not match for {display_path}: {checksum} != {actual_checksum}"
            )
    except FileNotFoundError as e:
        logger.error(e)
    except IsADirectoryError as e:
        logger.error(e)
    except NotADirectoryError as e:
        logger.error(e)
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")


@app.command(help="Convert a Neo4j export JSONL file into a PG-JSONL representation.")
def neo4j_to_pg(
    in_path: Path = typer.Option(
        "data/neo4j/export/optimuskg.jsonl",
        "--in",
        help="The path to read the input file from.",
    ),
    out_path: Path = typer.Option(
        "data/neo4j/export/optimuskg-pg.jsonl",
        "--out",
        help="The path to write the output file to.",
    ),
):
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


if __name__ == "__main__":
    app()
