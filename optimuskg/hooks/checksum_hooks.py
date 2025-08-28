import logging

from kedro.framework.hooks import hook_impl

from optimuskg.utils import (
    calculate_checksum,
    format_rich,
    get_dataset_by_name,
    get_dataset_display_name,
    get_dataset_path,
)

logger = logging.getLogger(__name__)


class ChecksumHooks:
    @hook_impl
    def before_dataset_loaded(self, dataset_name: str) -> None:
        valid_prefixes = ("landing.", "bronze.", "silver.")
        if any(dataset_name.startswith(prefix) for prefix in valid_prefixes):
            self._validate_checksum(dataset_name)

    def _checksum_display_str(self, checksum: str) -> str:
        return format_rich(checksum, "bright_blue")

    def _validate_checksum(self, ds_name: str) -> None:
        ds = get_dataset_by_name(ds_name)
        expected_checksum = getattr(ds, "metadata", {}).get("checksum")
        if not expected_checksum:
            logger.warning(
                f"No checksum found in metadata for dataset: {get_dataset_display_name(ds_name)}",
                extra={"markup": True},
            )
            return

        path = get_dataset_path(ds_name)

        try:
            actual_checksum = calculate_checksum(path)

            if expected_checksum != actual_checksum:
                logger.error(
                    f"Checksum mismatch for {get_dataset_display_name(ds_name)}. "
                    f"Expected: {self._checksum_display_str(expected_checksum)}, Got: {self._checksum_display_str(actual_checksum)}",
                    extra={"markup": True},
                )
            else:
                logger.debug(
                    f"Checksum validated successfully for {get_dataset_display_name(ds_name)}",
                    extra={"markup": True},
                )

        except FileNotFoundError:
            logger.exception(
                f"Path not found during checksum calculation for {get_dataset_display_name(ds_name)}: {path}",
                extra={"markup": True},
            )
            raise
        except Exception:
            logger.exception(
                f"Error calculating checksum for {get_dataset_display_name(ds_name)}",
                extra={"markup": True},
            )
            raise
