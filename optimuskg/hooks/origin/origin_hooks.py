import logging
from typing import Any

from kedro.framework.hooks import hook_impl
from kedro.io import DataCatalog
from kedro.pipeline import Pipeline
from pydantic import ValidationError

from optimuskg.hooks.origin.providers import BaseProvider, OriginProviderAdapter
from optimuskg.utils import (
    format_rich,
    get_dataset_by_name,
    get_dataset_display_name,
    get_dataset_path,
)

logger = logging.getLogger(__name__)


class OriginHooks:
    def __init__(self):
        self._run_with_private_datasets = False

    @hook_impl
    def before_pipeline_run(
        self, run_params: dict[str, Any], pipeline: Pipeline, catalog: DataCatalog
    ) -> None:
        """Hook to access runtime parameters passed via --params"""
        self._run_with_private_datasets = run_params.get("extra_params", {}).get(
            "private", False
        )

    @hook_impl
    def before_dataset_loaded(self, dataset_name: str) -> None:
        if dataset_name.startswith("landing."):
            self._download_data(dataset_name)

    def _origin_display_str(self) -> str:
        return format_rich("origin", "bright_green")

    def _provider_display_str(self, provider: BaseProvider) -> str:
        return format_rich(provider.provider, "bright_blue")

    def _download_data(self, ds_name: str) -> None:
        ds_path = get_dataset_path(ds_name)
        ds = get_dataset_by_name(ds_name)

        origin_dict = getattr(ds, "metadata", {}).get("origin")
        if not origin_dict:
            logger.warning(
                f"No {self._origin_display_str()} metadata found for dataset {get_dataset_display_name(ds_name)}",
                extra={"markup": True},
            )
            return
        if not isinstance(origin_dict, dict):
            logger.error(
                f"{self._origin_display_str()} metadata for {get_dataset_display_name(ds_name)} is not a dictionary: {origin_dict}",
                extra={"markup": True},
            )
            return

        try:
            provider: BaseProvider = OriginProviderAdapter.validate_python(origin_dict)
        except ValidationError:
            logger.exception(
                f"Failed to parse {self._origin_display_str()} metadata into provider model for {get_dataset_display_name(ds_name)}",
                extra={"markup": True},
            )
            return
        except Exception:
            logger.exception(
                f"An unexpected error occurred parsing {self._origin_display_str()} metadata for {get_dataset_display_name(ds_name)}",
                extra={"markup": True},
            )
            return

        try:
            logger.info(
                f"Attempting download for {get_dataset_display_name(ds_name)} using {self._provider_display_str(provider)} provider."
            )
            provider.download(output_path=ds_path)
        except Exception:
            logger.exception(
                f"Failed during download using {self._provider_display_str(provider)} provider for {get_dataset_display_name(ds_name)}",
                extra={"markup": True},
            )
