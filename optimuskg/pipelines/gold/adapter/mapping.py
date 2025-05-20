import uuid
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class EdgeMappingConfig:
    source_field: str
    target_field: str
    label_field: str
    properties_fields: Sequence[str] = ()
    id_factory: Callable[[], str] = lambda: str(uuid.uuid4())


@dataclass(frozen=True)
class NodeMappingConfig:
    id_field: str
    default_label: str | None = None
    label_field: str | None = None
    properties_fields: Sequence[str] = ()
    id_factory: Callable[[Any], str] = lambda val: val
