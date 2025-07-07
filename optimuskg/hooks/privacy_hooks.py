from typing import Any

import polars as pl
from kedro.framework.hooks import hook_impl
from kedro.io import AbstractDataset, DataCatalog
from kedro.pipeline import Pipeline


class EmptyPolarsDataset(AbstractDataset):
    def _load(self):
        return pl.DataFrame()

    def _save(self, data):
        pass

    def _describe(self):
        return {}


class PrivacyHook:
    def __init__(self):
        pass

    @hook_impl
    def before_pipeline_run(
        self, run_params: dict[str, Any], pipeline: Pipeline, catalog: DataCatalog
    ) -> None:
        """Hook to access runtime parameters passed via --params"""
        # Access parameters passed via --params key=value
        private_flag_enabled = self._private_flag_enabled(run_params)
        for name, dataset in catalog._datasets.items():
            if not hasattr(dataset, "config"):
                continue
            is_private = dataset.config["metadata"].get("private", False)
            if is_private and not private_flag_enabled:
                catalog._datasets[name] = EmptyPolarsDataset()

    def _private_flag_enabled(self, run_params: dict[str, Any]):
        # Check for --private in sys.argv
        return run_params.get("extra_params", {}).get("private", False)
