import logging
import os
from pathlib import Path

import polars as pl
from kedro.framework.hooks import hook_impl
from lxml import etree
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
                f"{get_dataset_display_name(ds_name)} is missing Origin metadata. "
                "If you added this dataset manually, it will be used as-is. "
                "Otherwise, an empty dataset will be used. This is because the dataset is probably private.",
                extra={"markup": True},
            )

            if os.path.exists(ds_path):
                return

            ds_type = type(ds).__name__
            ds_path.parent.mkdir(parents=True, exist_ok=True)

            if ds_type == "CSVDataset":
                self._create_empty_csv(ds, ds_path)
            elif ds_type == "LXMLDataset":
                self._create_empty_xml(ds, ds_path)
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
            if os.path.exists(ds_path):
                logger.info(
                    f"Skipping download for {get_dataset_display_name(ds_name)} because it already exists.",
                    extra={"markup": True},
                )
                return

            logger.info(
                f"Attempting download for {get_dataset_display_name(ds_name)} using {self._provider_display_str(provider)} provider.",
                extra={"markup": True},
            )
            provider.download(output_path=ds_path)
        except Exception:
            logger.exception(
                f"Failed during download using {self._provider_display_str(provider)} provider for {get_dataset_display_name(ds_name)}",
                extra={"markup": True},
            )

    def _create_empty_csv(self, ds, ds_path: Path) -> None:
        """Create empty CSV/TSV file with headers based on schema."""
        load_args = getattr(ds, "_load_args", {})
        schema = load_args.get("schema", {})
        separator = load_args.get("separator", ",")

        if schema:
            headers = list(schema.keys())
            df = pl.DataFrame({col: [] for col in headers}, schema=schema)

            # Write to file
            if separator == "\t":
                df.write_csv(ds_path, separator="\t")
            else:
                df.write_csv(ds_path, separator=separator)
        else:
            # If no schema, create empty file
            ds_path.touch()

    def _create_empty_xml(self, ds, ds_path: Path) -> None:
        """Create minimal valid XML file."""
        root = etree.Element("root")
        tree = etree.ElementTree(root)

        save_args = getattr(ds, "_save_args", {})
        encoding = save_args.get("encoding", "utf-8")
        pretty_print = save_args.get("pretty_print", True)

        tree.write(str(ds_path), encoding=encoding, pretty_print=pretty_print)
