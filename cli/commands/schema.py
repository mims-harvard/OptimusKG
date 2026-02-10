import logging
import re
from pathlib import Path
from typing import Any

import polars as pl
import yaml

from optimuskg.utils import calculate_checksum, format_rich

logger = logging.getLogger("cli")


def _format_dataset_display(ds_name: str, ds_type: str) -> str:
    display_name = format_rich(ds_name, "dark_orange")
    type_name = ds_type.split(".")[-1]
    return f"{display_name} ({type_name})"


_SIMPLE_TYPE_MAP: dict[type, str] = {
    pl.String: "pl.String",
    pl.Int8: "pl.Int8",
    pl.Int16: "pl.Int16",
    pl.Int32: "pl.Int32",
    pl.Int64: "pl.Int64",
    pl.UInt8: "pl.UInt8",
    pl.UInt16: "pl.UInt16",
    pl.UInt32: "pl.UInt32",
    pl.UInt64: "pl.UInt64",
    pl.Float32: "pl.Float32",
    pl.Float64: "pl.Float64",
    pl.Boolean: "pl.Boolean",
    pl.Date: "pl.Date",
    pl.Time: "pl.Time",
    pl.Datetime: "pl.Datetime",
    pl.Duration: "pl.Duration",
    pl.Binary: "pl.Binary",
    pl.Null: "pl.Null",
    pl.Object: "pl.Object",
}


def polars_dtype_to_yaml(dtype: Any) -> str | dict | list:
    """Convert a polars dtype to idiomatic YAML representation."""
    dtype_class = type(dtype)
    if dtype_class in _SIMPLE_TYPE_MAP:
        return _SIMPLE_TYPE_MAP[dtype_class]

    if isinstance(dtype, pl.List):
        inner = dtype.inner
        if isinstance(inner, pl.Struct):
            return [_struct_to_yaml_dict(inner)]
        else:
            inner_yaml = polars_dtype_to_yaml(inner)
            if isinstance(inner_yaml, str):
                return f"pl.List({inner_yaml})"
            else:
                raise ValueError(f"Unexpected inner type for List: {inner}")

    if isinstance(dtype, pl.Struct):
        return _struct_to_yaml_dict(dtype)

    dtype_name = str(dtype)
    if hasattr(pl, dtype_name):
        return f"pl.{dtype_name}"

    raise ValueError(f"Unsupported polars dtype: {dtype}")


def _struct_to_yaml_dict(struct_dtype: pl.Struct) -> dict[str, Any]:
    return {
        field.name: polars_dtype_to_yaml(field.dtype) for field in struct_dtype.fields
    }


class _CustomYamlDumper(yaml.SafeDumper):
    pass


def _str_representer(dumper: yaml.Dumper, data: str) -> yaml.ScalarNode:
    if "\n" in data or ":" in data or data.startswith("{"):
        return dumper.represent_scalar("tag:yaml.org,2002:str", data, style="'")
    return dumper.represent_scalar("tag:yaml.org,2002:str", data)


_CustomYamlDumper.add_representer(str, _str_representer)


def _write_yaml(config: dict, output_path: Path) -> None:
    with open(output_path, "w") as f:
        yaml.dump(
            config,
            f,
            Dumper=_CustomYamlDumper,
            default_flow_style=False,
            sort_keys=False,
            allow_unicode=True,
            width=120,
        )


def _load_yaml(yaml_path: Path) -> dict | None:
    with open(yaml_path) as f:
        return yaml.safe_load(f)


def _generate_schema_yaml(parquet_path: Path) -> dict[str, Any]:
    df = pl.read_parquet(parquet_path)
    return {col: polars_dtype_to_yaml(dtype) for col, dtype in df.schema.items()}


def _iter_catalog_yamls(
    catalog_dir: Path,
    layers: list[str],
    dataset: str | None = None,
) -> list[Path]:
    """Collect catalog YAML files for the given layers.

    When *dataset* is provided, only the file containing that dataset key is
    returned.  Otherwise every ``.yml`` / ``.yaml`` file under the requested
    layers is returned.
    """
    yaml_files: list[Path] = []

    for layer_name in layers:
        layer_path = catalog_dir / layer_name
        if not layer_path.exists():
            continue

        for yaml_file in sorted(
            set(layer_path.rglob("*.yml")) | set(layer_path.rglob("*.yaml")),
        ):
            if dataset is not None:
                content = _load_yaml(yaml_file)
                if content and dataset in content:
                    return [yaml_file]
            else:
                yaml_files.append(yaml_file)

    return sorted(yaml_files)


def _process_schema(  # noqa: PLR0911
    yaml_path: Path,
    ds_name: str,
    ds_config: dict[str, Any],
    validate_only: bool = False,
    dry_run: bool = False,
) -> tuple[str, str | None]:
    """Process schema for a single catalog entry.

    Returns ``(status, message)`` where *status* is one of: ``skipped``,
    ``warning``, ``error``, ``valid``, ``mismatch``, ``unchanged``,
    ``updated``.
    """
    ds_type = ds_config.get("type", "")
    if "ParquetDataset" not in ds_type:
        return "skipped", "Not a ParquetDataset"

    filepath = ds_config.get("filepath", "")
    if not filepath:
        return "skipped", "No filepath specified"

    parquet_path = Path(filepath)
    if not parquet_path.exists():
        return "warning", f"Parquet file not found: {parquet_path}"

    try:
        new_schema = _generate_schema_yaml(parquet_path)
    except Exception as e:
        return "error", f"Failed to read parquet: {e}"

    existing_schema = ds_config.get("load_args", {}).get("schema", {})
    schemas_match = new_schema == existing_schema

    if validate_only:
        if schemas_match:
            return "valid", None
        return "mismatch", "Schema differs from parquet file"

    if schemas_match:
        return "unchanged", None

    if dry_run:
        return "updated", "Would update schema (dry-run)"

    new_config: dict[str, Any] = {
        ds_name: {
            "type": "optimuskg.datasets.polars.ParquetDataset",
            "filepath": filepath,
            "load_args": {"schema": new_schema},
        }
    }

    if "metadata" in ds_config:
        new_config[ds_name]["metadata"] = ds_config["metadata"]

    _write_yaml(new_config, yaml_path)
    return "updated", None


def _update_checksum_in_file(
    yaml_path: Path,
    old_checksum: str,
    new_checksum: str,
) -> bool:
    """Replace a checksum value in a YAML file using regex.

    Only the checksum hex string is touched; all other content (comments,
    quoting, OmegaConf ``${pl:...}`` syntax, etc.) is preserved byte-for-byte.
    """
    text = yaml_path.read_text()
    pattern = r"(checksum:\s*)" + re.escape(old_checksum)
    new_text, count = re.subn(pattern, r"\g<1>" + new_checksum, text, count=1)
    if count == 0:
        return False
    yaml_path.write_text(new_text)
    return True


def _process_checksum(  # noqa: PLR0911
    yaml_path: Path,
    ds_config: dict[str, Any],
    validate_only: bool = False,
    dry_run: bool = False,
) -> tuple[str, str | None]:
    """Process checksum for a single catalog entry.

    Returns ``(status, message)`` where *status* is one of: ``skipped``,
    ``error``, ``valid``, ``mismatch``, ``unchanged``, ``updated``.
    """
    metadata = ds_config.get("metadata")
    if not metadata or not isinstance(metadata, dict):
        return "skipped", "No metadata"

    expected_checksum = metadata.get("checksum")
    if not expected_checksum:
        return "skipped", "No checksum in metadata"

    # filepath for regular datasets, path for PartitionedDatasets
    filepath = ds_config.get("filepath") or ds_config.get("path")
    if not filepath:
        return "skipped", "No filepath"

    data_path = Path(filepath)
    if not data_path.exists():
        return "skipped", f"Data file not found: {data_path}"

    try:
        actual_checksum = calculate_checksum(data_path)
    except Exception as e:
        return "error", f"Failed to compute checksum: {e}"

    if expected_checksum == actual_checksum:
        if validate_only:
            return "valid", None
        return "unchanged", None

    if validate_only:
        return "mismatch", f"Expected {expected_checksum}, got {actual_checksum}"

    if dry_run:
        return "updated", f"{expected_checksum} -> {actual_checksum}"

    if _update_checksum_in_file(yaml_path, expected_checksum, actual_checksum):
        return "updated", f"{expected_checksum} -> {actual_checksum}"

    return "error", "Failed to update checksum in file (regex match failed)"


_ZERO_STATS: dict[str, int] = {
    "updated": 0,
    "unchanged": 0,
    "skipped": 0,
    "warning": 0,
    "error": 0,
    "valid": 0,
    "mismatch": 0,
}


def _log_schema_status(
    status: str,
    message: str | None,
    display: str,
    dry_run: bool,
) -> None:
    if status == "updated":
        action = "Would update" if dry_run else "Updated"
        logger.info(
            f"{action} schema for {display}",
            extra={"markup": True},
        )
    elif status == "warning":
        logger.warning(f"{display}: {message}", extra={"markup": True})
    elif status == "error":
        logger.error(f"Schema error for {display}: {message}", extra={"markup": True})
    elif status == "valid":
        logger.debug(f"Valid schema for {display}", extra={"markup": True})
    elif status == "mismatch":
        logger.warning(f"Schema mismatch for {display}", extra={"markup": True})


def _log_checksum_status(
    status: str,
    message: str | None,
    display: str,
    dry_run: bool,
) -> None:
    if status == "updated":
        action = "Would update" if dry_run else "Updated"
        logger.info(
            f"{action} checksum for {display}: {message}",
            extra={"markup": True},
        )
    elif status == "error":
        logger.error(
            f"Checksum error for {display}: {message}",
            extra={"markup": True},
        )
    elif status == "valid":
        logger.debug(f"Valid checksum for {display}", extra={"markup": True})
    elif status == "mismatch":
        logger.warning(
            f"Checksum mismatch for {display}: {message}",
            extra={"markup": True},
        )


def sync_catalog_command(  # noqa: PLR0913, PLR0912
    layer: str = "all",
    dataset: str | None = None,
    validate: bool = False,
    dry_run: bool = False,
    catalog_dir: Path = Path("conf/base/catalog"),
    data_dir: Path = Path("data"),
) -> None:
    """Synchronize or validate schema and checksum specifications for catalog datasets."""
    mode = "Validating" if validate else ("Previewing" if dry_run else "Syncing")
    logger.info(f"{mode} catalog schemas and checksums...")

    layers = ["landing", "bronze", "silver"] if layer == "all" else [layer]

    yaml_files = _iter_catalog_yamls(catalog_dir, layers, dataset)
    if dataset and not yaml_files:
        logger.error(f"Dataset not found: {dataset}")
        return

    logger.info(f"Found {len(yaml_files)} YAML files to process")

    schema_stats: dict[str, int] = {**_ZERO_STATS}
    checksum_stats: dict[str, int] = {**_ZERO_STATS}

    for yaml_path in yaml_files:
        rel_path = yaml_path.relative_to(catalog_dir)

        try:
            content = _load_yaml(yaml_path)
        except Exception as e:
            schema_stats["error"] += 1
            checksum_stats["error"] += 1
            logger.error(f"{rel_path}: {e}", extra={"markup": True})
            continue

        if not content:
            schema_stats["skipped"] += 1
            checksum_stats["skipped"] += 1
            continue

        ds_name = list(content.keys())[0]
        ds_config = content[ds_name]
        ds_type = ds_config.get("type", "")
        display = (
            _format_dataset_display(ds_name, ds_type)
            if ds_name and ds_type
            else str(rel_path)
        )

        # The schema sync must be run before the checksum change so that the `_write_yaml` preserves the existing metadata.checksum for the regex pass.
        try:
            schema_status, schema_msg = _process_schema(
                yaml_path, ds_name, ds_config, validate, dry_run
            )
        except Exception as e:
            schema_status, schema_msg = "error", str(e)
        schema_stats[schema_status] += 1
        _log_schema_status(schema_status, schema_msg, display, dry_run)

        # Checksum sync reads file from disk to it picks up schema sync changes.
        try:
            checksum_status, checksum_msg = _process_checksum(
                yaml_path, ds_config, validate, dry_run
            )
        except Exception as e:
            checksum_status, checksum_msg = "error", str(e)
        checksum_stats[checksum_status] += 1
        _log_checksum_status(checksum_status, checksum_msg, display, dry_run)

    if validate:
        logger.info(
            f"Schema summary:   {schema_stats['valid']} valid, "
            f"{schema_stats['mismatch']} mismatch, "
            f"{schema_stats['skipped']} skipped, "
            f"{schema_stats['error']} errors"
        )
        logger.info(
            f"Checksum summary: {checksum_stats['valid']} valid, "
            f"{checksum_stats['mismatch']} mismatch, "
            f"{checksum_stats['skipped']} skipped, "
            f"{checksum_stats['error']} errors"
        )
    else:
        logger.info(
            f"Schema summary:   {schema_stats['updated']} updated, "
            f"{schema_stats['unchanged']} unchanged, "
            f"{schema_stats['skipped']} skipped, "
            f"{schema_stats['error']} errors"
        )
        logger.info(
            f"Checksum summary: {checksum_stats['updated']} updated, "
            f"{checksum_stats['unchanged']} unchanged, "
            f"{checksum_stats['skipped']} skipped, "
            f"{checksum_stats['error']} errors"
        )
