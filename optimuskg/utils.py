import hashlib
import logging
import re
from functools import lru_cache
from pathlib import Path
from typing import Any

import polars as pl
from kedro.framework.session import KedroSession
from kedro.io.core import AbstractDataset, CatalogProtocol, DatasetNotFoundError
from kedro.logging import _format_rich
from kedro.utils import _has_rich_handler
from kedro_datasets.partitions.partitioned_dataset import (
    PartitionedDataset,
)

logger = logging.getLogger(__name__)


def parse_polars_type(type_expr: str) -> Any:
    """Parse a polars type expression string into an actual polars type."""
    type_expr = " ".join(type_expr.split()).strip()
    return _parse_polars_type_internal(type_expr)


def _parse_polars_type_internal(s: str) -> Any:
    s = s.strip()

    if not s:
        raise ValueError("Empty type expression")

    if s.startswith("pl."):
        s = s[3:]

    if not any(c in s for c in "({"):
        if hasattr(pl, s):
            return getattr(pl, s)
        raise ValueError(f"Unknown polars type: {s}")

    if s.startswith("List(") and s.endswith(")"):
        inner = _extract_balanced_content(s, 5, "(", ")")
        if len(inner) == len(s) - 6:
            return pl.List(_parse_polars_type_internal(inner))

    if s.startswith("Struct({") and s.endswith("})"):
        inner = _extract_balanced_content(s, 8, "{", "}")
        if len(inner) == len(s) - 10:
            return pl.Struct(_parse_struct_fields(inner))

    raise ValueError(f"Cannot parse polars type: {s}")


def _extract_balanced_content(s: str, start: int, open_c: str, close_c: str) -> str:
    depth = 1
    i = start
    while i < len(s) and depth > 0:
        if s[i] == open_c:
            depth += 1
        elif s[i] == close_c:
            depth -= 1
        i += 1
    return s[start : i - 1]


def _parse_struct_fields(s: str) -> dict[str, Any]:
    fields: dict[str, Any] = {}
    current = ""
    depth = 0

    for char in s:
        if char in "({":
            depth += 1
            current += char
        elif char in ")}":
            depth -= 1
            current += char
        elif char == "," and depth == 0:
            _add_struct_field(current.strip(), fields)
            current = ""
        else:
            current += char

    if current.strip():
        _add_struct_field(current.strip(), fields)

    return fields


def _add_struct_field(field_str: str, fields: dict[str, Any]) -> None:
    if not field_str:
        return

    depth = 0
    for i, char in enumerate(field_str):
        if char in "({":
            depth += 1
        elif char in ")}":
            depth -= 1
        elif char == ":" and depth == 0:
            name = field_str[:i].strip().strip("'\"")
            type_str = field_str[i + 1 :].strip()
            fields[name] = _parse_polars_type_internal(type_str)
            return

    raise ValueError(f"Invalid field definition (no colon found): {field_str}")


def to_snake_case(text: str) -> str:
    """
    Convert a string to valid snake_case following Python naming conventions.

    The output will pass validation by is_snake_case:
    - Starts with lowercase letter or underscore
    - Contains only lowercase letters, numbers, or single underscores
    - No consecutive underscores
    - No trailing underscore

    Args:
        text (str): Input string that could be in any format (camelCase, PascalCase, etc.)

    Returns:
        str: The string converted to valid snake_case

    Examples:
        >>> to_snake_case("helloWorld")
        'hello_world'
        >>> to_snake_case("HelloWorld")
        'hello_world'
        >>> to_snake_case("hello-world")
        'hello_world'
        >>> to_snake_case("hello__world")
        'hello_world'
        >>> to_snake_case("_hello_world_")
        '_hello_world'
    """
    if not text:
        return text

    # Replace hyphens and spaces with underscores
    text = re.sub(r"[-\s]+", "_", text)

    # Insert underscore between camelCase
    text = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", text)

    # Convert to lowercase and remove any non-alphanumeric characters (except underscores)
    text = re.sub(r"[^a-zA-Z0-9_]", "", text)

    # Convert to lowercase
    text = text.lower()

    # Replace multiple consecutive underscores with single underscore
    text = re.sub(r"_+", "_", text)

    # Remove trailing underscore if present
    text = re.sub(r"_$", "", text)

    # If string doesn't start with a letter or underscore, prepend underscore
    if text and not re.match(r"^[a-z_]", text):
        text = "_" + text

    if not is_snake_case(text):
        raise NameError(f"{text} is not in snake_case format")

    return text


def is_snake_case(text: str) -> bool:
    """
    Check if a string follows Python snake_case naming convention using regex.

    Pattern explanation:
    ^           # Start of string
    [a-z_]      # Start with lowercase letter or underscore
    [a-z0-9_]*  # Followed by any number of lowercase letters, numbers, or underscores
    (?<!_)      # Negative lookbehind to prevent trailing underscore
    $           # End of string

    Additional negative pattern:
    .*__.*      # Matches any string containing consecutive underscores

    Args:
        text (str): The string to check

    Returns:
        bool: True if the string follows snake_case, False otherwise

    Examples:
        >>> is_snake_case("hello_world")
        True
        >>> is_snake_case("HelloWorld")
        False
        >>> is_snake_case("hello__world")
        False
    """
    # Pattern for valid snake_case
    snake_pattern = r"^[a-z_][a-z0-9_]*(?<!_)$"

    # Pattern for consecutive underscores
    double_underscore_pattern = r".*__.*"

    return bool(re.match(snake_pattern, text)) and not bool(
        re.match(double_underscore_pattern, text)
    )


def calculate_checksum(
    path: Path,
    chunk_size: int = 8192,
    digest_size: int = 16,
) -> str:
    """Calculate the checksum of a file or directory.

    Args:
        path: The path to the file or directory.
        chunk_size: The size of chunks to read from files.
        digest_size: The size of the digest for the hash.

    Returns:
        The calculated checksum as a hexadecimal string.

    Raises:
        FileNotFoundError: If the path does not exist.
    """
    if not path.exists():
        raise FileNotFoundError(f"Path does not exist: {path}")

    if path.is_dir():
        combined_hash = hashlib.blake2b(digest_size=digest_size)
        files = sorted(path.glob("**/*"))
        for file_path in files:
            if file_path.is_file():
                with open(file_path, "rb") as f:
                    while chunk := f.read(chunk_size):
                        combined_hash.update(chunk)
                # Include file path relative to the base directory in the hash
                relative_path = file_path.relative_to(path)
                combined_hash.update(str(relative_path).encode())
        return combined_hash.hexdigest()
    else:
        with open(path, "rb") as f:
            file_hash = hashlib.blake2b(digest_size=digest_size)
            while chunk := f.read(chunk_size):
                file_hash.update(chunk)
        return file_hash.hexdigest()


def format_rich(value: str, markup: str) -> str:
    if _has_rich_handler():
        return _format_rich(value, markup)
    return value


@lru_cache
def get_dataset_by_name(ds_name: str) -> AbstractDataset:
    with KedroSession.create() as session:
        catalog: CatalogProtocol = session.load_context().catalog
        try:
            dataset: AbstractDataset = catalog.get(ds_name)
        except DatasetNotFoundError:
            logger.exception(
                f"Dataset {format_rich(ds_name, 'dark_red')} not found in catalog",
                extra={"markup": True},
            )
        return dataset


def get_dataset_display_name(ds_name: str) -> str:
    display_name = format_rich(ds_name, "dark_orange")
    ds: AbstractDataset = get_dataset_by_name(ds_name)
    ds_type = type(ds).__name__
    return f"{display_name} ({ds_type})"


def get_dataset_path(ds_name: str) -> Path:
    ds: AbstractDataset = get_dataset_by_name(ds_name)

    path_attr = "_path" if isinstance(ds, PartitionedDataset) else "_filepath"
    path_str = getattr(ds, path_attr, None)

    if path_str is None:
        raise ValueError(f"Could not determine path for {ds_name}")

    path = Path(path_str)
    return path
