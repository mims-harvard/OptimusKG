import logging
from typing import Any

import polars as pl
from kedro.framework.hooks import hook_impl
from kedro.pipeline.node import Node

from optimuskg.utils import convert_columns_to_snake_case

logger = logging.getLogger(__name__)


class SilverHooks:
    @hook_impl
    def after_node_run(self, node: Node, outputs: dict[str, Any]) -> None:
        if node.namespace == "silver":
            outputs = self._convert_columns_to_snake_case(outputs)

    def _convert_columns_to_snake_case(self, outputs: dict[str, Any]) -> dict[str, Any]:
        """Convert the columns of the dataframes in the node outputs to snake case if the node belongs to the silver pipeline.

        Args:
            outputs (dict[str, Any]): The outputs produced by the node.

        Returns:
            dict[str, Any]: The outputs with the columns converted to snake case.
        """
        for output_name, output_value in outputs.items():
            if isinstance(output_value, pl.DataFrame):
                outputs[output_name] = convert_columns_to_snake_case(output_value)
        return outputs
