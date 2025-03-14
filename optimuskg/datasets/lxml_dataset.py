from __future__ import annotations

import logging
from copy import deepcopy
from pathlib import PurePosixPath
from typing import Any, override  # type: ignore[attr-defined]

import fsspec
from kedro.io.core import (
    AbstractVersionedDataset,
    DatasetError,
    Version,
    get_filepath_str,
    get_protocol_and_path,
)
from lxml import etree

logger = logging.getLogger(__name__)


class LXMLDataset(AbstractVersionedDataset[etree._ElementTree, etree._ElementTree]):
    """``LXMLDataset`` loads XML data using lxml.etree.

    Example usage:

    .. code-block:: pycon
        >>> from my_project.datasets import LXMLDataset
        >>>
        >>> dataset = LXMLDataset(filepath="data/01_raw/data.xml")
        >>> tree = dataset.load()
        >>> root = tree.getroot()
    """

    DEFAULT_LOAD_ARGS: dict[str, Any] = {}
    DEFAULT_SAVE_ARGS: dict[str, Any] = {"pretty_print": True, "encoding": "utf-8"}
    DEFAULT_FS_ARGS: dict[str, Any] = {}

    def __init__(  # noqa: PLR0913
        self,
        *,
        filepath: str,
        load_args: dict[str, Any] | None = None,
        save_args: dict[str, Any] | None = None,
        version: Version | None = None,
        credentials: dict[str, Any] | None = None,
        fs_args: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Creates a new instance of ``LXMLDataset``.

        Args:
            filepath: Filepath in POSIX format to an XML file prefixed with a protocol like `s3://`.
                If prefix is not provided, `file` protocol (local filesystem) will be used.
                The prefix should be any protocol supported by ``fsspec``.
            load_args: Additional arguments to pass to lxml.etree.parse.
                All defaults are preserved.
            save_args: Additional arguments to pass to lxml.etree.ElementTree.write.
                Defaults to {"pretty_print": True, "encoding": "utf-8"}.
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
        _fs_open_args_save = _fs_args.pop("open_args_save", {})
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

        # Handle default load and save arguments
        self._load_args = {**self.DEFAULT_LOAD_ARGS, **(load_args or {})}
        self._save_args = {**self.DEFAULT_SAVE_ARGS, **(save_args or {})}
        self._fs_open_args_load = {
            **self.DEFAULT_FS_ARGS.get("open_args_load", {}),
            **(_fs_open_args_load or {}),
        }
        self._fs_open_args_save = {
            **self.DEFAULT_FS_ARGS.get("open_args_save", {}),
            **(_fs_open_args_save or {}),
        }

    @override
    def _describe(self) -> dict[str, Any]:
        return {
            "filepath": self._filepath,
            "protocol": self._protocol,
            "load_args": self._load_args,
            "save_args": self._save_args,
            "version": self._version,
        }

    @override
    def load(self) -> etree._ElementTree:
        """Loads the XML file.

        Returns:
            lxml.etree._ElementTree object containing the parsed XML.
        """
        load_path = get_filepath_str(self._get_load_path(), self._protocol)

        if self._protocol == "file":
            return etree.parse(load_path, **self._load_args)

        # For remote files, we need to download to a temporary file first
        with self._fs.open(load_path, **self._fs_open_args_load) as fs_file:
            return etree.parse(fs_file, **self._load_args)

    @override
    def save(self, data: etree._ElementTree) -> None:
        """Saves the XML file.

        Args:
            data: The XML ElementTree to save.
        """
        save_path = get_filepath_str(self._get_save_path(), self._protocol)

        if self._protocol == "file":
            data.write(save_path, **self._save_args)
        else:
            # For remote files, we need to save to a temporary file first
            with self._fs.open(save_path, "wb", **self._fs_open_args_save) as fs_file:
                data.write(fs_file, **self._save_args)

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
