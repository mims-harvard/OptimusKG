import json
import logging
import uuid
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import polars as pl
from biocypher import BioCypher
from more_itertools import peekable

from optimuskg.utils import format_rich

logger = logging.getLogger(__name__)

_BIOCYPHER_CONFIG_PATH = "conf/base/biocypher/biocypher_config.yaml"


def _encode_nested_properties(properties: dict[str, Any]) -> dict[str, Any]:
    """JSON-encode nested dicts and lists of dicts for Neo4j compatibility.

    Neo4j does not support nested properties, so this function converts nested
    dictionaries and lists of dictionaries into JSON-encoded strings.

    Example:
        {"ontology": {"description": "foo", "version": "1.0"}, "name": "bar"}
        becomes:
        {"ontology": '{"description": "foo", "version": "1.0"}', "name": "bar"}

        {"items": [{"id": "1", "label": "a"}, {"id": "2", "label": "b"}]}
        becomes:
        {"items": ['{"id": "1", "label": "a"}', '{"id": "2", "label": "b"}']}

    Args:
        properties: Dictionary of properties, potentially with nested dicts
            or lists of dicts.

    Returns:
        Dictionary with nested dicts/lists of dicts JSON-encoded as strings.
    """
    result: dict[str, Any] = {}
    for k, v in properties.items():
        if isinstance(v, dict):
            result[k] = json.dumps(v)
        elif isinstance(v, list) and v and isinstance(v[0], dict):
            result[k] = [json.dumps(item) for item in v]
        elif v is not None:
            result[k] = v
    return result


def _escape_quotes(value: Any) -> Any:
    """Escape double quotes in strings and string lists for Neo4j CSV export."""
    if isinstance(value, list) and all(isinstance(x, str) for x in value):
        return [x.replace('"', '""') for x in value]
    elif isinstance(value, str):
        return value.replace('"', '""')
    return value


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
            # JSON-encode nested structs for Neo4j compatibility
            encoded_props = _encode_nested_properties(row["properties"])
            for k, v in encoded_props.items():
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
            # JSON-encode nested structs (e.g., sources) for Neo4j compatibility
            encoded_props = _encode_nested_properties(row["properties"])
            properties = {**encoded_props, "undirected": row["undirected"]}
            for k, v in properties.items():
                if v is not None:
                    not_null_properties[k] = _escape_quotes(v)
        yield BiocypherEdge(
            id=str(uuid.uuid4()),
            from_id=row["from"],
            to_id=row["to"],
            label=row["label"],
            properties=not_null_properties,
        )


def _process_adapters(
    bc: BioCypher,
    adapters: list[Iterator[Any]],
    adapter_type: str,
    write_func: Callable[[Iterator[Any]], Any],
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


def run_biocypher(
    nodes_dict: dict[str, pl.DataFrame],
    edges_dict: dict[str, pl.DataFrame],
    include_properties: bool = True,
    output_directory: str | Path | None = None,
) -> BioCypher:
    """Run BioCypher node/edge writes; returns the BioCypher instance.

    Schema and ontology validation is performed as a side-effect of
    ``write_nodes``/``write_edges``. Passing ``output_directory`` overrides the
    path in ``biocypher_config.yaml`` (used for validation-only runs into a
    temp dir). Leave ``None`` to use the production path configured in the YAML.

    Args:
        nodes_dict: Dictionary of node type name to DataFrame.
        edges_dict: Dictionary of edge type name to DataFrame.
        include_properties: If True, include properties in exported nodes/edges.
            If False, export only structural data (id, label, etc.).
        output_directory: Optional override for BioCypher's output directory.

    Returns:
        The configured ``BioCypher`` instance after writes complete.
    """
    kwargs: dict[str, Any] = {"biocypher_config_path": _BIOCYPHER_CONFIG_PATH}
    if output_directory is not None:
        kwargs["output_directory"] = str(output_directory)

    bc = BioCypher(**kwargs)

    node_adapters = [_yield_nodes(df, include_properties) for df in nodes_dict.values()]
    edge_adapters = [_yield_edges(df, include_properties) for df in edges_dict.values()]

    _process_adapters(bc, node_adapters, "node", bc.write_nodes)
    _process_adapters(bc, edge_adapters, "edge", bc.write_edges)

    return bc
