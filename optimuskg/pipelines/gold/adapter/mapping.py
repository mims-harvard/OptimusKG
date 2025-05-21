import uuid
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any


def _default_edge_id_factory() -> str:
    return str(uuid.uuid4())


def _default_node_id_factory(val: Any) -> str:
    return str(val)


@dataclass(frozen=True)
class EdgeMappingConfig:
    source_field: str
    target_field: str
    label_field: str
    properties_fields: Sequence[str] = ()
    id_factory: Callable[[], str] = _default_edge_id_factory


@dataclass(frozen=True)
class NodeMappingConfig:
    id_field: str
    default_label: str | None = None
    label_field: str | None = None
    properties_fields: Sequence[str] = ()
    id_factory: Callable[[Any], str] = _default_node_id_factory
