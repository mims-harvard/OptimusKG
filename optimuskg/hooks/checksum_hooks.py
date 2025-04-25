import logging
from pathlib import Path
from typing import Any

from kedro.framework.hooks import hook_impl
from kedro.io import KedroDataCatalog
from kedro.pipeline.node import Node
from kedro_datasets.partitions.partitioned_dataset import PartitionedDataset

from optimuskg.utils import calculate_checksum

logger = logging.getLogger(__name__)


class ChecksumHooks:
    @hook_impl
    def after_catalog_created(self, catalog: KedroDataCatalog) -> None:
        self.catalog = catalog

    @hook_impl
    def after_dataset_loaded(self, dataset_name: str, data: Any, node: Node) -> None:
        valid_prefixes = ("landing.", "bronze.", "silver.")
        if any(dataset_name.startswith(prefix) for prefix in valid_prefixes):
            self._validate_checksum(dataset_name, self.catalog)

    def _validate_checksum(self, ds_name: str, catalog: KedroDataCatalog) -> None:
        """Validate the checksum of a dataset against its expected value in metadata.

        Args:
            ds_name: The name of the dataset in the catalog
            catalog: The Kedro data catalog containing the dataset
        """
        ds = catalog.get(ds_name)
        expected_checksum = getattr(ds, "metadata", {}).get("checksum")
        if not expected_checksum:
            logger.warning(f"No checksum found in metadata for dataset: '{ds_name}'")
            return

        is_partitioned = isinstance(ds, PartitionedDataset)
        path_str = (
            getattr(ds, "_path", None)
            if is_partitioned
            else getattr(ds, "_filepath", None)
        )
        if path_str is None:
            logger.error(f"Could not determine path for dataset '{ds_name}'")
            return
        path = Path(path_str)
        if is_partitioned and not path.is_dir():
            logger.error(
                f"Expected directory for PartitionedDataset '{ds_name}' but found path: {path}"
            )
            return
        elif not is_partitioned and not path.is_file():
            logger.error(f"File not found for dataset '{ds_name}' at path: {path}")
            return

        try:
            actual_checksum = calculate_checksum(path, process_directory=is_partitioned)

            if expected_checksum != actual_checksum:
                logger.error(
                    f"Checksum mismatch for '{ds_name}'. "
                    f"Expected: {expected_checksum}, Got: {actual_checksum}"
                )
            else:
                logger.debug(f"Checksum validated successfully for '{ds_name}'")

        except FileNotFoundError:
            logger.error(
                f"Path not found during checksum calculation for '{ds_name}': {path}"
            )
        except IsADirectoryError:
            logger.error(
                f"Expected a file but found a directory for '{ds_name}': {path}"
            )
        except NotADirectoryError:
            logger.error(
                f"Expected a directory but found a file for '{ds_name}': {path}"
            )
        except Exception as e:
            logger.error(f"Error calculating checksum for '{ds_name}': {e}")
