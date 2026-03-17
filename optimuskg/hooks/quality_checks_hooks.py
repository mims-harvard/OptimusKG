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

        if node.namespace == "gold" and node.name == "export_kg":
            self._validate_global_node_id_uniqueness(outputs)

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
                logger.error(
                    f"{get_dataset_display_name(name)}: Column {column} has null ids",
                    extra={"markup": True},
                )

    def _validate_global_node_id_uniqueness(self, outputs: dict[str, Any]) -> None:
        """Check that node IDs are globally unique across all node types.

        Runs on the consolidated ``"nodes"`` DataFrame that the export_kg
        node already produces (via csv_export / parquet_export).  When
        duplicates exist the error lists which labels share each colliding
        ID so the upstream silver-layer pipeline can be fixed.
        """
        for output_name, output_value in outputs.items():
            if not isinstance(output_value, dict):
                continue
            nodes_df = output_value.get("nodes")
            if nodes_df is None or not isinstance(nodes_df, pl.DataFrame):
                continue
            if nodes_df.is_empty():
                continue

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
            return  # Only need to validate once; all formats share the same source data.

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
