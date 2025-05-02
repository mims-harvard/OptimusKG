"""``Gene2GoReaderDataset`` loads data from a Gene Annotation File (GAF) using goatools.Gene2GoReader."""

from __future__ import annotations

import contextlib
import gzip
import logging
import os
from copy import deepcopy
from pathlib import PurePosixPath
from typing import Any, override

import fsspec
from goatools.anno.genetogo_reader import Gene2GoReader
from kedro.io.core import (
    AbstractVersionedDataset,
    DatasetError,
    Version,
    get_filepath_str,
    get_protocol_and_path,
)

logger = logging.getLogger(__name__)


class Gene2GoReaderDataset(AbstractVersionedDataset[Gene2GoReader, Gene2GoReader]):
    """``Gene2GoReaderDataset`` loads data from a Gene Annotation File (GAF) using goatools.Gene2GoReader.

    Example usage:

    .. code-block:: pycon
        >>> from my_project.datasets import Gene2GoReaderDataset
        >>>
        >>> dataset = Gene2GoReaderDataset(filepath="data/01_raw/gene2go")
        >>> reader = dataset.load()
        >>> associations = reader.get_associations()
    """

    DEFAULT_LOAD_ARGS: dict[str, Any] = {}
    DEFAULT_FS_ARGS: dict[str, Any] = {}

    def __init__(  # noqa: PLR0913
        self,
        *,
        filepath: str,
        load_args: dict[str, Any] | None = None,
        version: Version | None = None,
        credentials: dict[str, Any] | None = None,
        fs_args: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Creates a new instance of ``Gene2GoReaderDataset`` pointing to a GAF file.

        Args:
            filepath: Filepath in POSIX format to a GAF file prefixed with a protocol like `s3://`.
                If prefix is not provided, `file` protocol (local filesystem) will be used.
                The prefix should be any protocol supported by ``fsspec``.
            load_args: Additional arguments to pass to Gene2GoReader.
                All defaults are preserved.
            version: If specified, should be an instance of ``kedro.io.core.Version``.
                If its ``load`` attribute is None, the latest version will be loaded.
            credentials: Credentials required to get access to the underlying filesystem.
                E.g. for ``GCSFileSystem`` it should look like `{"token": None}`.
            fs_args: Extra arguments to pass into underlying filesystem class constructor
                (e.g. `{"project": "my-project"}` for ``GCSFileSystem``).
            metadata: Any arbitrary metadata.
                This is ignored by Kedro, but may be consumed by users or external plugins.
        """
        _fs_args = deepcopy(fs_args) or {}
        _fs_open_args_load = _fs_args.pop("open_args_load", {})
        _credentials = deepcopy(credentials) or {}

        protocol, path = get_protocol_and_path(filepath, version)
        if protocol == "file":
            _fs_args.setdefault("auto_mkdir", True)

        self._protocol = protocol
        self._storage_options = {**_credentials, **_fs_args}
        self._fs = fsspec.filesystem(self._protocol, **self._storage_options)

        self.metadata = metadata

        super().__init__(
            filepath=PurePosixPath(path),
            version=version,
            exists_function=self._fs.exists,
            glob_function=self._fs.glob,
        )

        # Handle default load arguments
        self._load_args = {**self.DEFAULT_LOAD_ARGS, **(load_args or {})}
        self._fs_open_args_load = {
            **self.DEFAULT_FS_ARGS.get("open_args_load", {}),
            **(_fs_open_args_load or {}),
        }

    @override
    def _describe(self) -> dict[str, Any]:
        return {
            "filepath": self._filepath,
            "protocol": self._protocol,
            "load_args": self._load_args,
            "version": self._version,
        }

    @override
    def load(self) -> Gene2GoReader:
        """Loads the Gene2GoReader, handling potential gzip compression.

        If a `.gz` file is specified, it checks if a decompressed version exists
        at the same path without the extension. If not, it decompresses the file
        and saves it there before loading.

        Returns:
            Gene2GoReader object initialized with the GAF file.

        Raises:
            DatasetError: If the file cannot be loaded or decompressed.
            IOError: If writing the decompressed file fails due to permissions or other issues.
        """
        load_path = get_filepath_str(self._get_load_path(), self._protocol)
        is_compressed = load_path.endswith(".gz")
        target_path = load_path  # Path to be used by Gene2GoReader

        if is_compressed:
            decompressed_path = load_path[:-3]  # Remove .gz extension
            if self._fs.exists(decompressed_path):
                logger.info(f"Using existing decompressed file: '{decompressed_path}'")
                target_path = decompressed_path
            else:
                logger.warning(
                    f"Decompressing '{load_path}' to '{decompressed_path}'. "
                    f"This requires write permissions and modifies the source directory."
                )
                try:
                    # Open compressed source, open target for writing
                    with self._fs.open(
                        load_path, mode="rb", **self._fs_open_args_load
                    ) as compressed_file_obj:
                        # Use fsspec's open for writing to handle different protocols
                        with self._fs.open(
                            decompressed_path, mode="wt", encoding="utf-8"
                        ) as outfile:
                            # Decompress and write
                            with gzip.open(
                                compressed_file_obj, "rt", encoding="utf-8"
                            ) as infile:
                                outfile.write(infile.read())

                    logger.info(f"Successfully decompressed to '{decompressed_path}'")
                    target_path = decompressed_path
                except Exception as e:
                    # Catch potential IOErrors during write or other exceptions
                    raise DatasetError(
                        f"Failed to decompress '{load_path}' to '{decompressed_path}': {e}"
                    ) from e

        # Now load using the target_path (original or decompressed)
        # Suppress Gene2GoReader stdout during initialization
        with open(os.devnull, "w") as devnull, contextlib.redirect_stdout(devnull):
            if self._protocol == "file":
                # Use target_path which is either original or the decompressed path
                try:
                    return Gene2GoReader(filename=target_path, **self._load_args)
                except Exception as e:
                    raise DatasetError(
                        f"Failed to load local file {target_path}: {e}"
                    ) from e
            else:
                # For remote files, fsspec needs to handle caching/downloading.
                # If we decompressed, target_path now points to the (potentially remote)
                # decompressed file which fsspec should handle like any other remote file.
                # If not compressed, target_path is the original remote path.
                try:
                    # Use fsspec's open which returns a file-like object with a '.name' attribute
                    # pointing to a local cache/temp file if necessary.
                    with self._fs.open(
                        target_path,
                        mode="rt",
                        encoding="utf-8",
                        **self._fs_open_args_load,
                    ) as fs_file:
                        return Gene2GoReader(filename=fs_file.name, **self._load_args)
                except Exception as e:
                    raise DatasetError(
                        f"Failed to load remote file {target_path}: {e}"
                    ) from e

    @override
    def save(self, data: Gene2GoReader) -> None:
        """Gene2GoReader is read-only, so save operation is not supported."""
        raise DatasetError("Gene2GoReaderDataset is read-only")

    @override
    def _exists(self) -> bool:
        """Checks if the path exists.

        Returns:
            True if the path exists, False otherwise.
        """
        try:
            load_path = get_filepath_str(self._get_load_path(), self._protocol)
        except DatasetError:
            return False

        return self._fs.exists(load_path)  # type: ignore

    @override
    def _release(self) -> None:
        """Release any resources that have been grabbed by the dataset."""
        super()._release()
        self._invalidate_cache()

    def _invalidate_cache(self) -> None:
        """Invalidate underlying filesystem caches."""
        filepath = get_filepath_str(self._filepath, self._protocol)
        self._fs.invalidate_cache(filepath)
