from __future__ import annotations

import os
import tempfile
import zipfile
from copy import deepcopy
from typing import Any
from urllib.parse import urlparse

import fsspec
import polars as pl
from cachetools import Cache
from kedro.io import AbstractDataset, DatasetError
from kedro.io.catalog_config_resolver import CREDENTIALS_KEY
from kedro.io.core import parse_dataset_definition

KEY_PROPAGATION_WARNING = (
    "Top-level %(keys)s will not propagate into the %(target)s since "
    "%(keys)s were explicitly defined in the %(target)s config."
)

S3_PROTOCOLS = ("s3", "s3a", "s3n")


class ZipDataset(AbstractDataset[zipfile.ZipFile, pl.DataFrame]):
    DEFAULT_LOAD_ARGS: dict[str, Any] = {}
    DEFAULT_FS_ARGS: dict[str, Any] = {}
    DEFAULT_CREDENTIALS: dict[str, str] = {}

    def __init__(  # noqa: PLR0913
        self,
        *,
        filepath: str,
        zipped_filename: str | None = None,
        ignored_prefixes: list[str] | None = None,
        ignored_suffixes: list[str] | None = None,
        dataset: str | type[AbstractDataset] | dict[str, Any],
        credentials: dict[str, str] | None = None,
        load_args: dict[str, Any] | None = None,
        fs_args: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
        filepath_arg: str = "filepath",
    ) -> None:
        from fsspec.utils import infer_storage_options

        super().__init__()

        self._filepath = filepath
        self._protocol = infer_storage_options(self._filepath)["protocol"]
        self._partition_cache: Cache = Cache(maxsize=1)
        self.metadata = metadata

        dataset = dataset if isinstance(dataset, dict) else {"type": dataset}
        self._dataset_type, self._dataset_config = parse_dataset_definition(dataset)

        if credentials:
            if CREDENTIALS_KEY in self._dataset_config:
                self._logger.warning(
                    KEY_PROPAGATION_WARNING,
                    {"keys": CREDENTIALS_KEY, "target": "underlying dataset"},
                )
            else:
                self._dataset_config[CREDENTIALS_KEY] = deepcopy(credentials)

        self._credentials = deepcopy(credentials) or ZipDataset.DEFAULT_CREDENTIALS

        self._fs_args = deepcopy(fs_args) or ZipDataset.DEFAULT_FS_ARGS
        if self._fs_args:
            if "fs_args" in self._dataset_config:
                self._logger.warning(
                    KEY_PROPAGATION_WARNING,
                    {"keys": "filesystem arguments", "target": "underlying dataset"},
                )
            else:
                self._dataset_config["fs_args"] = deepcopy(self._fs_args)

        self._filepath_arg = filepath_arg
        if self._filepath_arg in self._dataset_config:
            self._logger.warning(
                f"'{self._filepath_arg}' key must not be specified in the dataset "
                f"definition as it will be overwritten"
            )

        self._load_args = deepcopy(load_args) or ZipDataset.DEFAULT_LOAD_ARGS
        self._sep = self._filesystem.sep
        # since some filesystem implementations may implement a global cache
        self._invalidate_caches()

        self._zipped_filename = zipped_filename
        self._ignored_prefixes = ignored_prefixes or ["_", "."]
        self._ignored_suffixes = (ignored_suffixes or []) + ["/"]

    @property
    def _filesystem(self):
        protocol = "s3" if self._protocol in S3_PROTOCOLS else self._protocol
        return fsspec.filesystem(protocol, **self._credentials, **self._fs_args)

    @property
    def _normalized_path(self) -> str:
        if self._protocol in S3_PROTOCOLS:
            return urlparse(self._filepath)._replace(scheme="s3").geturl()
        return self._filepath

    def _is_ignored(self, name: str) -> bool:
        return any(name.startswith(prefix) for prefix in self._ignored_prefixes) or any(
            name.endswith(suffix) for suffix in self._ignored_suffixes
        )

    def _load(self) -> Any:
        with zipfile.ZipFile(self._filepath) as zipped:
            namelist = zipped.namelist()
            namelist = [name for name in namelist if not self._is_ignored(name)]

            if len(namelist) > 1 and self._zipped_filename is None:
                raise DatasetError(
                    f"Multiple files found! Please specify which file to extract: {namelist}"
                )
            if not namelist:
                raise DatasetError("No files found in the archive!")

            target_filename = self._zipped_filename or namelist[0]

            with zipped.open(target_filename) as zipped_file:
                with tempfile.NamedTemporaryFile(delete=False) as temp_file:
                    temp_file.write(zipped_file.read())
                    temp_filepath = temp_file.name

                try:
                    kwargs = deepcopy(self._dataset_config)
                    kwargs[self._filepath_arg] = temp_filepath
                    dataset = self._dataset_type(**kwargs)
                    return dataset.load()
                finally:
                    os.unlink(temp_filepath)

    def _save(self, data: Any) -> None:
        raise DatasetError("Saving is unsupported")

    def _describe(self) -> dict[str, Any]:
        return {
            "filepath": self._filepath,
            "zipped_filename": self._zipped_filename,
            "ignored_prefixes": self._ignored_prefixes,
            "ignored_suffixes": self._ignored_suffixes,
        }

    def _invalidate_caches(self) -> None:
        self._partition_cache.clear()
        self._filesystem.invalidate_cache(self._normalized_path)
