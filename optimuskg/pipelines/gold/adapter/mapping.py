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


def get_mapping_configs(
    extended_schema: dict,
) -> dict[str, NodeMappingConfig | EdgeMappingConfig]:
    """Get mapping configs for all schema elements.

    Args:
        extended_schema: Schema configuration dictionary from BioCypher

    Returns:
        Dictionary mapping schema element names to their mapping configs
    """
    mapping_configs = {}
    for k, v in extended_schema.items():
        properties = list(v["properties"].keys())

        if v["represented_as"] == "node":
            mapping_configs[k] = NodeMappingConfig(
                id_field=v["preferred_id"],
                default_label=v["input_label"],
                properties_fields=properties,
            )
        else:
            mapping_configs[k] = EdgeMappingConfig(
                source_field="x_id",
                target_field="y_id",
                label_field="relation",
                properties_fields=properties,
            )

    return mapping_configs
