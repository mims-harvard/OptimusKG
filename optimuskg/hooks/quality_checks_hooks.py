import logging
from typing import Any

import polars as pl
from kedro.framework.hooks import hook_impl
from kedro.pipeline.node import Node

from optimuskg.utils import get_dataset_display_name, is_snake_case

logger = logging.getLogger(__name__)


class QualityChecksHooks:
    @hook_impl
    def after_node_run(self, node: Node, outputs: dict[str, Any]) -> None:
        if node.namespace in ["silver", "gold"]:
            self._validate_outputs(outputs)

    def _validate_outputs(self, outputs: dict[str, Any]) -> None:
        for output_name, output_value in outputs.items():
            if isinstance(output_value, pl.DataFrame):
                self._validate_dataframe(output_name, output_value)

    def _validate_dataframe(self, name: str, df: pl.DataFrame) -> None:
        self._check_column_names(name, df)
        self._check_not_null_ids(name, df)

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
