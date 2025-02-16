import logging
from typing import Any

import polars as pl
from kedro.framework.hooks import hook_impl
from kedro.pipeline.node import Node

from optimuskg.utils.strings import convert_columns_to_snake_case

logger = logging.getLogger(__name__)


class ConvertColumnsToSnakeCase:
    @hook_impl
    def after_node_run(self, node: Node, outputs: dict[str, Any]) -> None:
        """Convert the columns of the dataframes in the node outputs to snake case if the node belongs to the silver pipeline.

        Args:
            node (Node): The node that was executed.
            outputs (dict[str, Any]): The outputs produced by the node.
        """
        if node.namespace == "silver":
            for output_name, output_value in outputs.items():
                if isinstance(output_value, pl.DataFrame):
                    outputs[output_name] = convert_columns_to_snake_case(output_value)
