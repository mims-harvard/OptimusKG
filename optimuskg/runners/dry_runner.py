"""``DryRunner`` is an ``AbstractRunner`` implementation. It can be used to
list which nodes would be run without actually executing anything. It also
checks if all the necessary input data exists.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from kedro.runner.runner import AbstractRunner

if TYPE_CHECKING:
    from concurrent.futures import Executor

    from kedro.io import CatalogProtocol
    from kedro.pipeline import Pipeline
    from pluggy import PluginManager


class DryRunner(AbstractRunner):
    """``DryRunner`` is an ``AbstractRunner`` implementation. It can be used to
    list which nodes would be run without actually executing anything. It also
    checks if all the necessary input data exists.
    """

    def __init__(self, is_async: bool = False) -> None:
        """Instantiates the runner class.

        Args:
            is_async: If True, the node inputs and outputs are loaded and saved
                asynchronously with threads. Defaults to False.
        """
        super().__init__(is_async=is_async)

    def _get_executor(self, max_workers: int) -> Executor | None:
        """Return ``None`` since dry runs do not need an executor."""
        return None

    def _run(
        self,
        pipeline: Pipeline,
        catalog: CatalogProtocol,
        hook_manager: PluginManager | None = None,
        run_id: str | None = None,
    ) -> None:
        """The method implementing dry pipeline running.

        Example logs output using this implementation::

            kedro.runner.dry_runner - INFO - Actual run would execute 3 nodes:
            node3: identity([A]) -> [B]
            node2: identity([C]) -> [D]
            node1: identity([D]) -> [E]

        Args:
            pipeline: The ``Pipeline`` to run.
            catalog: An implemented instance of ``CatalogProtocol``
                from which to fetch data.
            hook_manager: The ``PluginManager`` to activate hooks.
            run_id: The id of the run.

        Raises:
            KeyError: If any pipeline input datasets are missing.
        """
        nodes = pipeline.nodes
        self._logger.info(
            "Actual run would execute %d nodes:\n%s",
            len(nodes),
            pipeline.describe(),
        )
        self._logger.info("Checking inputs...")
        input_names = pipeline.inputs()

        missing_inputs = [
            input_name for input_name in input_names if not catalog.exists(input_name)
        ]
        if missing_inputs:
            raise KeyError(f"Datasets {missing_inputs} not found.")
