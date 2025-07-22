import logging
from collections.abc import Iterable

import polars as pl

from .mapping import EdgeMappingConfig, NodeMappingConfig
from .output import EdgeInfo, NodeInfo

logger = logging.getLogger(__name__)


def yield_nodes(
    df: pl.DataFrame, mapping_config: NodeMappingConfig
) -> Iterable[NodeInfo]:
    for row in df.iter_rows(named=True):
        node_id = mapping_config.id_factory(row[mapping_config.id_field])
        node_label: str = (
            row[mapping_config.label_field]
            if mapping_config.label_field and row.get(mapping_config.label_field)
            else mapping_config.default_label or "Unknown"
        )
        node_properties = (
            {
                field: (
                    row[field].split("|")
                    if row[field] is not None
                    and "|" in row[field]  # Decompose multi-valued fields into a list
                    else row[field].replace('"', '""')
                    if row[field]
                    is not None  # Escape double quotes since biocypher doesn't escape them
                    else None
                )
                for field in mapping_config.properties_fields
                if field in row
            }
            if mapping_config.properties_fields
            else {}
        )
        yield NodeInfo(id=node_id, label=node_label, properties=node_properties)


def yield_edges(
    df: pl.DataFrame, mapping_config: EdgeMappingConfig
) -> Iterable[EdgeInfo]:
    for row in df.iter_rows(named=True):
        yield EdgeInfo(
            id=mapping_config.id_factory(),
            source_id=row[mapping_config.source_field],
            target_id=row[mapping_config.target_field],
            label=row[mapping_config.label_field],
            properties=(
                {
                    field: row[field]
                    for field in mapping_config.properties_fields
                    if field in row
                }
                if mapping_config.properties_fields
                else {}
            ),
        )
