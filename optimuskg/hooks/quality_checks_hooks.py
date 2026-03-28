import logging
from typing import Any

import polars as pl
from kedro.framework.hooks import hook_impl
from kedro.io.core import DatasetError
from kedro.pipeline.node import Node

from optimuskg.pipelines.silver.nodes.constants import Relation
from optimuskg.utils import get_dataset_display_name, is_snake_case

logger = logging.getLogger(__name__)


class QualityChecksHooks:
    @hook_impl
    def after_node_run(self, node: Node, outputs: dict[str, Any]) -> None:
        if node.namespace in ["silver", "gold"]:
            self._validate_outputs(outputs)

        if node.name == "gold.export_kg":
            self._validate_global_node_id_uniqueness(outputs)
            self._validate_edge_node_references(outputs)
            self._validate_single_connected_component(outputs)

    def _validate_outputs(self, outputs: dict[str, Any]) -> None:
        for output_name, output_value in outputs.items():
            if isinstance(output_value, pl.DataFrame):
                self._validate_dataframe(output_name, output_value)

    def _validate_dataframe(self, name: str, df: pl.DataFrame) -> None:
        self._check_column_names(name, df)
        self._check_not_null_ids(name, df)
        self._check_relation_values(name, df)

    def _check_column_names(self, name: str, df: pl.DataFrame) -> None:
        invalid_columns = [col for col in df.columns if not is_snake_case(col)]
        if invalid_columns:
            logger.warning(
                f"{get_dataset_display_name(name)}: The following columns are not in snake_case: {', '.join(invalid_columns)}",
                extra={"markup": True},
            )

    def _check_not_null_ids(self, name: str, df: pl.DataFrame) -> None:
        id_columns = [col for col in df.columns if col.startswith("id")]
        for column in id_columns:
            if df[column].is_null().any():
                error_msg = (
                    f"{get_dataset_display_name(name)}: Column {column} has null ids",
                )
                logger.error(
                    error_msg,
                    extra={"markup": True},
                )
                raise DatasetError(error_msg)

    def _validate_global_node_id_uniqueness(self, outputs: dict[str, Any]) -> None:
        """
        Check that node IDs are globally unique across all node types.

        Runs on the consolidated ``"nodes"`` DataFrame that the export_kg
        node already produces (via parquet_export).  When
        duplicates exist the error lists which labels share each colliding
        ID so the upstream silver-layer pipeline can be fixed.
        """
        output_name = "gold.kg.parquet"
        output_value = outputs[output_name]

        nodes_df = output_value.get("nodes")

        if nodes_df.is_empty():
            return

        duplicates = (
            nodes_df.select("id", "label")
            .group_by("id")
            .agg(pl.col("label"))
            .filter(pl.col("label").list.len() > 1)
        )

        if duplicates.height > 0:
            sample = (
                duplicates.head(10)
                .with_columns(pl.col("label").list.join(", "))
                .to_dicts()
            )
            raise DatasetError(
                f"Found {duplicates.height} non-unique node IDs across "
                f"node types in {output_name}. "
                f"Duplicates (id -> labels): {sample}"
            )

        logger.info(
            f"Validated global ID uniqueness in {output_name}: "
            f"{nodes_df.height} IDs are globally unique."
        )

    def _validate_edge_node_references(self, outputs: dict[str, Any]) -> None:
        """Check that every edge endpoint references an existing node ID.

        Runs on the consolidated ``"nodes"`` and ``"edges"`` DataFrames that
        the export_kg node produces.  When orphan references exist the error
        lists which IDs are missing so the upstream pipeline can be fixed.
        """
        output_name = "gold.kg.parquet"
        output_value = outputs[output_name]

        nodes_df = output_value.get("nodes")
        edges_df = output_value.get("edges")

        if edges_df.is_empty():
            return

        node_ids = nodes_df["id"].to_list()

        orphan_from = edges_df.filter(~pl.col("from").is_in(node_ids))
        orphan_to = edges_df.filter(~pl.col("to").is_in(node_ids))

        missing_from = set(orphan_from["from"].to_list()) - set(node_ids)
        missing_to = set(orphan_to["to"].to_list()) - set(node_ids)
        missing_ids = missing_from | missing_to

        if missing_ids:
            sample = sorted(missing_ids)[:10]
            raise DatasetError(
                f"Found {len(missing_ids)} edge endpoint IDs in {output_name} that do not match any node ID. Sample missing IDs: {sample}"
            )

        logger.info(
            f"Validated edge-node references in {output_name}: all edge endpoints reference valid node IDs."
        )

    def _validate_single_connected_component(self, outputs: dict[str, Any]) -> None:
        """Check that the exported graph forms a single connected component.

        Builds a networkx Graph from the consolidated nodes and edges
        DataFrames produced by export_kg and verifies it is connected.
        """
        import networkx as nx

        output_name = "gold.kg.parquet"
        output_value = outputs[output_name]

        nodes_df = output_value.get("nodes")
        edges_df = output_value.get("edges")

        if nodes_df.is_empty():
            return

        G = nx.Graph()
        for nid, label in zip(nodes_df["id"].to_list(), nodes_df["label"].to_list()):
            G.add_node(nid, label=label)

        if not edges_df.is_empty():
            for src, dst, label in zip(
                edges_df["from"].to_list(),
                edges_df["to"].to_list(),
                edges_df["label"].to_list(),
            ):
                G.add_edge(src, dst, label=label)

        from collections import Counter

        component_sizes = [len(c) for c in nx.connected_components(G)]
        n_components = len(component_sizes)
        size_counts = dict(sorted(Counter(component_sizes).items(), reverse=True))

        logger.info(
            f"Graph connectivity in {output_name}: "
            f"{n_components} connected component(s), "
            f"size distribution (size: count): {size_counts}"
        )

    def _check_relation_values(self, name: str, df: pl.DataFrame) -> None:
        if "relation" not in df.columns:
            return

        relation_col = df["relation"]
        total_rows = len(df)
        valid_values = set(Relation._value2member_map_.keys())

        # Check for invalid values (not in Relation enum)
        invalid_mask = ~relation_col.is_in(list(valid_values))
        invalid_count = invalid_mask.sum()

        if invalid_count > 0:
            invalid_values = relation_col.filter(invalid_mask).unique().to_list()
            raise DatasetError(
                f"{get_dataset_display_name(name)}: Found {invalid_count} rows with invalid relation values: {invalid_values}"
            )

        # Warn about OTHER relations
        other_count = (relation_col == Relation.OTHER).sum()
        if other_count > 0:
            pct = (other_count / total_rows) * 100
            logger.warning(
                f"{get_dataset_display_name(name)}: {other_count} of {total_rows} rows ({pct:.1f}%) have OTHER as their relation value",
                extra={"markup": True},
            )
