from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class NodeInfo:
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


@dataclass(frozen=True)
class EdgeInfo:
    """A type safe intermediate edge compatible with BioCypher."""

    id: str
    source_id: str
    target_id: str
    label: str
    properties: Mapping[str, Any]

    def __iter__(self) -> Iterator[Any]:
        """Implement the tuple protocol for BioCypher."""
        return iter(
            (self.id, self.source_id, self.target_id, self.label, self.properties)
        )

    def __len__(self) -> int:
        """Implement the tuple protocol for BioCypher."""
        return 5
