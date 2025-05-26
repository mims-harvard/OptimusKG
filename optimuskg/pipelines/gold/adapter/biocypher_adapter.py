import logging
from collections.abc import Iterable, Sequence

import polars as pl

from .mapping import EdgeMappingConfig, NodeMappingConfig
from .output import EdgeInfo, NodeInfo

logger = logging.getLogger(__name__)


class BiocypherAdapter:
    def __init__(
        self,
        df: pl.DataFrame,
        node_configs: Sequence[NodeMappingConfig] | None = None,
        edge_configs: Sequence[EdgeMappingConfig] | None = None,
        name: str = "biocypher_adapter",
    ):
        self.df = df
        self.node_configs = node_configs
        self.edge_configs = edge_configs
        self.name = name

    def nodes(self) -> Iterable[NodeInfo]:
        if self.node_configs is None:
            return
        seen = set()
        for row in self.df.iter_rows(named=True):
            for cfg in self.node_configs:
                node_id = cfg.id_factory(row[cfg.id_field])
                if node_id in seen:
                    continue
                seen.add(node_id)

                current_label = ""  # Default to empty string
                if cfg.label_field and row.get(cfg.label_field):
                    current_label = row[cfg.label_field]
                elif cfg.default_label:
                    current_label = cfg.default_label

                props = (
                    {
                        field: row[field]
                        for field in cfg.properties_fields
                        if field in row
                    }
                    if cfg.properties_fields
                    else {}
                )
                yield NodeInfo(id=node_id, label=current_label, properties=props)

    def edges(self) -> Iterable[EdgeInfo]:
        if self.edge_configs is None:
            return
        for row in self.df.iter_rows(named=True):
            for cfg in self.edge_configs:
                try:
                    eid = cfg.id_factory()
                    sid = row[cfg.source_field]
                    tid = row[cfg.target_field]
                    lbl = row[cfg.label_field]
                    props = (
                        {
                            field: row[field]
                            for field in cfg.properties_fields
                            if field in row
                        }
                        if cfg.properties_fields
                        else {}
                    )
                    yield EdgeInfo(
                        id=str(eid),
                        source_id=str(sid),
                        target_id=str(tid),
                        label=str(lbl),
                        properties=props,
                    )
                except Exception:
                    logger.exception(f"Error generating edge for row {row}")


def adapter_factory(
    df: pl.DataFrame,
    name: str,
) -> BiocypherAdapter:
    adapter = BiocypherAdapter(
        df=df,
        name=name,
        node_configs=[
            NodeMappingConfig(
                id_field="x_id",
                label_field="x_type",
                properties_fields=["x_name", "x_source"],
            ),
            NodeMappingConfig(
                id_field="y_id",
                label_field="y_type",
                properties_fields=["y_name", "y_source"],
            ),
        ],
        edge_configs=[
            EdgeMappingConfig(
                source_field="x_id",
                target_field="y_id",
                label_field="relation",
                properties_fields=["display_relation"],
            )
        ],
    )

    return adapter
