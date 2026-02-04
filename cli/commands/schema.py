import logging
from pathlib import Path
from typing import Any

import polars as pl
import yaml

from optimuskg.utils import format_rich

logger = logging.getLogger("cli")


def _format_dataset_display(ds_name: str, ds_type: str) -> str:
    display_name = format_rich(ds_name, "dark_orange")
    type_name = ds_type.split(".")[-1]
    return f"{display_name} ({type_name})"


_SIMPLE_TYPE_MAP: dict[type, str] = {
    pl.String: "pl.String",
    pl.Utf8: "pl.String",
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


def _process_yaml_file(  # noqa: PLR0911
    yaml_path: Path,
    data_dir: Path,
    validate_only: bool = False,
    dry_run: bool = False,
) -> tuple[str, str | None, str | None, str | None]:
    """Process a single YAML file. Returns (status, message, ds_name, ds_type)."""
    content = _load_yaml(yaml_path)

    if not content:
        return "skipped", "Empty YAML file", None, None

    ds_name = list(content.keys())[0]
    ds_config = content[ds_name]

    ds_type = ds_config.get("type", "")
    if "ParquetDataset" not in ds_type:
        return "skipped", "Not a ParquetDataset", ds_name, ds_type

    filepath = ds_config.get("filepath", "")
    if not filepath:
        return "skipped", "No filepath specified", ds_name, ds_type

    parquet_path = Path(filepath)
    if not parquet_path.exists():
        return "warning", f"Parquet file not found: {parquet_path}", ds_name, ds_type

    try:
        new_schema = _generate_schema_yaml(parquet_path)
    except Exception as e:
        return "error", f"Failed to read parquet: {e}", ds_name, ds_type

    existing_schema = ds_config.get("load_args", {}).get("schema", {})
    schemas_match = new_schema == existing_schema

    if validate_only:
        if schemas_match:
            return "valid", None, ds_name, ds_type
        return "mismatch", "Schema differs from parquet file", ds_name, ds_type

    if schemas_match:
        return "unchanged", None, ds_name, ds_type

    if dry_run:
        return "updated", "Would update (dry-run)", ds_name, ds_type

    new_config = {
        ds_name: {
            "type": "optimuskg.datasets.polars.ParquetDataset",
            "filepath": filepath,
            "load_args": {"schema": new_schema},
        }
    }

    if "metadata" in ds_config:
        new_config[ds_name]["metadata"] = ds_config["metadata"]

    _write_yaml(new_config, yaml_path)
    return "updated", None, ds_name, ds_type


def sync_catalog_schemas_command(  # noqa: PLR0913, PLR0912
    layer: str = "all",
    dataset: str | None = None,
    validate: bool = False,
    dry_run: bool = False,
    catalog_dir: Path = Path("conf/base/catalog"),
    data_dir: Path = Path("data"),
) -> None:
    """Synchronize or validate schema specifications for parquet datasets."""
    mode = "Validating" if validate else ("Previewing" if dry_run else "Syncing")
    logger.info(f"{mode} catalog schemas...")

    layers = ["bronze", "silver"] if layer == "all" else [layer]
    yaml_files: list[Path] = []

    if dataset:
        for layer_name in layers:
            layer_path = catalog_dir / layer_name
            if layer_path.exists():
                for yaml_file in layer_path.rglob("*.yml"):
                    content = _load_yaml(yaml_file)
                    if content and dataset in content:
                        yaml_files.append(yaml_file)
                        break
        if not yaml_files:
            logger.error(f"Dataset not found: {dataset}")
            return
    else:
        for layer_name in layers:
            layer_path = catalog_dir / layer_name
            if layer_path.exists():
                yaml_files.extend(layer_path.rglob("*.yml"))

    yaml_files = sorted(yaml_files)
    logger.info(f"Found {len(yaml_files)} YAML files to process")

    stats = {
        "updated": 0,
        "unchanged": 0,
        "skipped": 0,
        "warning": 0,
        "error": 0,
        "valid": 0,
        "mismatch": 0,
    }

    for yaml_path in yaml_files:
        rel_path = yaml_path.relative_to(catalog_dir)

        try:
            status, message, ds_name, ds_type = _process_yaml_file(
                yaml_path=yaml_path,
                data_dir=data_dir,
                validate_only=validate,
                dry_run=dry_run,
            )
        except Exception as e:
            status, message, ds_name, ds_type = "error", str(e), None, None

        stats[status] += 1
        display = (
            _format_dataset_display(ds_name, ds_type)
            if ds_name and ds_type
            else str(rel_path)
        )

        if status == "updated":
            action = "Would update" if dry_run else "Updated"
            logger.info(
                f"{action} schema configuration for {display}", extra={"markup": True}
            )
        elif status == "unchanged":
            logger.debug(
                f"Unchanged schema configuration for {display}", extra={"markup": True}
            )
        elif status == "skipped":
            logger.debug(f"Skipped {display} ({message})", extra={"markup": True})
        elif status == "warning":
            logger.warning(f"{display} {message}", extra={"markup": True})
        elif status == "error":
            logger.error(f"{display} {message}", extra={"markup": True})
        elif status == "valid":
            logger.info(
                f"Valid schema configuration for {display}", extra={"markup": True}
            )
        elif status == "mismatch":
            logger.warning(f"Schema mismatch for {display}", extra={"markup": True})

    if validate:
        logger.info(
            f"Summary: {stats['valid']} valid, {stats['mismatch']} mismatch, {stats['skipped']} skipped, {stats['warning']} warnings, {stats['error']} errors"
        )
    else:
        logger.info(
            f"Summary: {stats['updated']} updated, {stats['unchanged']} unchanged, {stats['skipped']} skipped, {stats['warning']} warnings, {stats['error']} errors"
        )
