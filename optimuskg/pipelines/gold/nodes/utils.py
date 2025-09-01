import logging
import subprocess
import uuid
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import polars as pl
import yaml

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class BiocypherNode:
    """A type safe intermediate node compatible with BioCypher."""

    id: str
    label: str
    properties: Mapping[str, Any]

    def __iter__(self) -> Iterator[Any]:
        """Implement the tuple protocol for BioCypher."""
        return iter((self.id, self.label, self.properties))

    def __len__(self) -> int:
        """Implement the tuple protocol for BioCypher."""
        return 3


def yield_nodes(df: pl.DataFrame) -> Iterator[BiocypherNode]:
    for row in df.iter_rows(named=True):
        not_null_properties = {}
        for k, v in row[
            "properties"
        ].items():  # Escape double quotes since biocypher doesn't escape them
            if v is not None:
                if isinstance(v, list) and all(isinstance(x, str) for x in v):
                    not_null_properties[k] = [x.replace('"', '""') for x in v]
                elif isinstance(v, str):
                    not_null_properties[k] = v.replace('"', '""')
                else:
                    not_null_properties[k] = v
        yield BiocypherNode(
            id=row["id"],
            label=row["node_type"],
            properties=not_null_properties,
        )


@dataclass(frozen=True)
class BiocypherEdge:
    """A type safe intermediate edge compatible with BioCypher."""

    id: str
    from_id: str
    to_id: str
    label: str
    properties: Mapping[str, Any]

    def __iter__(self) -> Iterator[Any]:
        """Implement the tuple protocol for BioCypher."""
        return iter((self.id, self.from_id, self.to_id, self.label, self.properties))

    def __len__(self) -> int:
        """Implement the tuple protocol for BioCypher."""
        return 5


def yield_edges(df: pl.DataFrame) -> Iterator[BiocypherEdge]:
    for row in df.iter_rows(named=True):
        properties = {**row["properties"], "undirected": row["undirected"]}
        not_null_properties = {}
        for (
            k,
            v,
        ) in (
            properties.items()
        ):  # Escape double quotes since biocypher doesn't escape them
            if v is not None:
                if isinstance(v, list) and all(isinstance(x, str) for x in v):
                    not_null_properties[k] = [x.replace('"', '""') for x in v]
                elif isinstance(v, str):
                    not_null_properties[k] = v.replace('"', '""')
                else:
                    not_null_properties[k] = v
        yield BiocypherEdge(
            id=str(uuid.uuid4()),
            from_id=row["from"],
            to_id=row["to"],
            label=row["relation"],
            properties=not_null_properties,
        )


def csv_to_neo4j(import_path: Path, schema_config_path: Path) -> None:
    """
    Import data into Neo4j from CSV files.
    """
    logger.info(f"Importing data from {import_path} into Neo4j.")

    with open(schema_config_path) as f:
        schema_config = yaml.safe_load(f)

    node_args = []
    edge_args = []
    for name, config in schema_config.items():
        represented_as = config.get("represented_as")
        input_label = config.get("input_label")

        if not input_label:
            logger.warning(f"'{name}' is missing 'input_label' in schema, skipping.")
            continue

        pascal_case_label = "".join(
            word.capitalize() for word in input_label.split("_")
        )
        header_file = f"{pascal_case_label}-header.csv"

        if represented_as == "node":
            if (import_path / header_file).exists():
                node_args.append(
                    f"--nodes=/import/{header_file},/import/{pascal_case_label}-part.*"
                )
            else:
                logger.debug(
                    f"Header file for node '{pascal_case_label}' not found, skipping."
                )
        elif represented_as == "edge":
            if (import_path / header_file).exists():
                edge_args.append(
                    f"--relationships=/import/{header_file},/import/{pascal_case_label}-part.*"
                )
            else:
                logger.debug(
                    f"Header file for edge '{pascal_case_label}' not found, skipping."
                )

    if not node_args and not edge_args:
        logger.warning("No node or edge files found to import. Aborting.")
        return

    command = [
        "docker",
        "run",
        "--interactive",
        "--tty",
        "--rm",
        "--publish=7474:7474",
        "--publish=7687:7687",
        "--volume=./data/neo4j/data:/data",
        f"--volume=./{import_path}:/import",
        "--volume=./data/export:/export",
        "neo4j:5.26.2-community-bullseye",
        "neo4j-admin",
        "database",
        "import",
        "full",
        "neo4j",
        "--verbose",
        "--delimiter=;",
        "--array-delimiter=|",
        '--quote="',
        "--overwrite-destination=true",
    ]
    command.extend(node_args)
    command.extend(edge_args)

    logger.info("Running neo4j-admin import command...")
    logger.debug(f"Executing command: {' '.join(command)}")

    try:
        subprocess.run(command, check=True)
        logger.info("Neo4j import completed successfully.")
    except subprocess.CalledProcessError as e:
        logger.error(f"Neo4j import failed: {e}")
    except FileNotFoundError:
        logger.error(
            "Docker command not found. Please ensure Docker is installed and in your PATH."
        )
