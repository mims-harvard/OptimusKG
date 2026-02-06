import logging
import subprocess
import uuid
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import polars as pl
import yaml
from biocypher import BioCypher
from more_itertools import peekable

from optimuskg.utils import format_rich

logger = logging.getLogger(__name__)

_BIOCYPHER_CONFIG_PATH = "conf/base/biocypher/biocypher_config.yaml"


def _flatten_properties(properties: dict[str, Any], prefix: str = "") -> dict[str, Any]:
    """Flatten nested dicts into flat key-value pairs with prefixed keys.

    Neo4j does not support nested properties, so this function flattens nested
    dictionaries (e.g., ontology structs) into flat key-value pairs.

    Example:
        {"ontology": {"description": "foo", "version": "1.0"}}
        becomes:
        {"ontology_description": "foo", "ontology_version": "1.0"}

    Args:
        properties: Dictionary of properties, potentially with nested dicts.
        prefix: Prefix to prepend to keys (used for recursion).

    Returns:
        Flattened dictionary with no nested structures.
    """
    result: dict[str, Any] = {}
    for k, v in properties.items():
        key = f"{prefix}{k}" if prefix else k
        if isinstance(v, dict):
            result.update(_flatten_properties(v, f"{key}_"))
        elif v is not None:
            result[key] = v
    return result


def _escape_quotes(value: Any) -> Any:
    """Escape double quotes in strings and string lists for Neo4j CSV export."""
    if isinstance(value, list) and all(isinstance(x, str) for x in value):
        return [x.replace('"', '""') for x in value]
    elif isinstance(value, str):
        return value.replace('"', '""')
    return value


_BIOCYPHER_SCHEMA_CONFIG_PATH = Path("conf/base/biocypher/schema_config.yaml")
_NEO4J_IMPORT_PATH = Path("data/neo4j/import")


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


def _yield_nodes(
    df: pl.DataFrame, include_properties: bool = True
) -> Iterator[BiocypherNode]:
    for row in df.iter_rows(named=True):
        not_null_properties: dict[str, Any] = {}
        if include_properties:
            # Flatten nested structs (like ontology) for Neo4j compatibility
            flat_props = _flatten_properties(row["properties"])
            for k, v in flat_props.items():
                if v is not None:
                    not_null_properties[k] = _escape_quotes(v)
        yield BiocypherNode(
            id=row["id"],
            label=row["label"],
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


def _yield_edges(
    df: pl.DataFrame, include_properties: bool = True
) -> Iterator[BiocypherEdge]:
    for row in df.iter_rows(named=True):
        not_null_properties: dict[str, Any] = {"undirected": row["undirected"]}
        if include_properties:
            properties = {**row["properties"], "undirected": row["undirected"]}
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
            label=row["label"],
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


def _process_adapters(
    bc: BioCypher,
    adapters: list[Iterator[Any]],
    adapter_type: str,
    write_func: Any,
) -> None:
    """Process and write adapters to BioCypher.

    Args:
        bc: BioCypher instance.
        adapters: List of adapter iterators.
        adapter_type: Type name for logging ("node" or "edge").
        write_func: BioCypher write function (write_nodes or write_edges).
    """
    if not adapters:
        logger.error(f"There are no {adapter_type}s to process.")
        return

    for i, iterable in enumerate(adapters):
        logger.info(
            f"Processing {adapter_type} adapter "
            f"{format_rich(str(i + 1), 'dark_orange')}/"
            f"{format_rich(str(len(adapters)), 'dark_orange')}."
        )

        items = peekable(iterable)
        if items.peek(None) is not None:
            logger.info(f"Writing {adapter_type}s...")
            write_func(items)
        else:
            logger.warning(f"No {adapter_type}s found.")


def neo4j_export(
    nodes_dict: dict[str, pl.DataFrame],
    edges_dict: dict[str, pl.DataFrame],
    include_properties: bool = True,
) -> None:
    """Export knowledge graph to Neo4j via BioCypher.

    Args:
        nodes_dict: Dictionary of node type name to DataFrame.
        edges_dict: Dictionary of edge type name to DataFrame.
        include_properties: If True, include properties in exported nodes/edges.
                           If False, export only structural data (id, label, etc.).
    """
    bc = BioCypher(biocypher_config_path=_BIOCYPHER_CONFIG_PATH)

    node_adapters = [_yield_nodes(df, include_properties) for df in nodes_dict.values()]
    edge_adapters = [_yield_edges(df, include_properties) for df in edges_dict.values()]

    try:
        _process_adapters(bc, node_adapters, "node", bc.write_nodes)
        _process_adapters(bc, edge_adapters, "edge", bc.write_edges)
    except Exception as e:
        logger.exception(f"Error writing graph data to disk: {e}")
        raise

    try:
        logger.info("Bulk-importing graph data from CSV files to Neo4j database...")
        csv_to_neo4j(
            import_path=_NEO4J_IMPORT_PATH,
            schema_config_path=_BIOCYPHER_SCHEMA_CONFIG_PATH,
        )
    except Exception as e:
        logger.exception(f"Error importing graph data to Neo4j: {e}")
        raise
