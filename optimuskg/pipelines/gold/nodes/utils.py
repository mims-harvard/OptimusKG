import uuid
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from typing import Any

import polars as pl


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
