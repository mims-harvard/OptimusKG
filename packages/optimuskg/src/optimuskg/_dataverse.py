"""Dataverse Data Access API client: list files, download, cache locally."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import NotRequired, TypedDict, cast

import requests

from optimuskg import _config
from optimuskg._paths import to_relative

_CHUNK = 1 << 20  # 1 MiB


class _DataFilePayload(TypedDict):
    """Minimal fields we read from a Dataverse ``dataFile`` entry."""

    id: int
    filename: str


class _FilePayload(TypedDict):
    """One entry in ``latestVersion.files``."""

    dataFile: _DataFilePayload
    directoryLabel: NotRequired[str | None]


class _VersionPayload(TypedDict, total=False):
    """The subset of ``latestVersion`` we read."""

    versionNumber: int
    versionMinorNumber: int
    versionState: str
    files: list[_FilePayload]


@dataclass(frozen=True, slots=True)
class FileEntry:
    """One file in a Dataverse dataset version."""

    id: int
    filename: str
    directory_label: str
    relative_path: str


@dataclass(frozen=True, slots=True)
class DatasetMetadata:
    """The subset of dataset metadata we need: version + file table."""

    version: str
    files: dict[str, FileEntry]


_metadata_cache: dict[tuple[str, str], DatasetMetadata] = {}


def _slug(doi: str) -> str:
    return doi.replace(":", "_").replace("/", "_").replace(".", "_")


def fetch_metadata(*, force: bool = False) -> DatasetMetadata:
    """Return cached dataset metadata, fetching from Dataverse on a miss."""
    server = _config.get_server()
    doi = _config.get_doi()
    key = (server, doi)
    if not force and key in _metadata_cache:
        return _metadata_cache[key]

    url = f"{server}/api/datasets/:persistentId/"
    resp = requests.get(url, params={"persistentId": doi}, timeout=60)
    resp.raise_for_status()
    latest = cast(_VersionPayload, resp.json()["data"]["latestVersion"])

    major = latest.get("versionNumber")
    minor = latest.get("versionMinorNumber")
    if major is not None and minor is not None:
        version = f"{major}.{minor}"
    else:
        version = str(latest.get("versionState", "DRAFT")).lower()

    files: dict[str, FileEntry] = {}
    for raw in latest.get("files", []):
        data_file = raw["dataFile"]
        directory_label = raw.get("directoryLabel") or ""
        filename = data_file["filename"]
        relative = to_relative(directory_label, filename)
        files[relative] = FileEntry(
            id=int(data_file["id"]),
            filename=filename,
            directory_label=directory_label,
            relative_path=relative,
        )

    meta = DatasetMetadata(version=version, files=files)
    _metadata_cache[key] = meta
    return meta


def resolve(relative_path: str, *, force_metadata: bool = False) -> FileEntry:
    """Find the Dataverse ``FileEntry`` for a given relative path."""
    meta = fetch_metadata(force=force_metadata)
    try:
        return meta.files[relative_path.strip("/")]
    except KeyError:
        available = ", ".join(sorted(meta.files)[:10])
        raise FileNotFoundError(
            f"No file at {relative_path!r} in dataset {_config.get_doi()}. "
            f"Known paths include: {available}…"
        ) from None


def download(relative_path: str, *, force: bool = False) -> Path:
    """Download ``relative_path`` to the local cache and return its path.

    Uses ``<cache_dir>/<doi_slug>/<version>/<relative_path>`` as the on-disk layout.
    Subsequent calls reuse the cached file unless ``force=True``.
    """
    meta = fetch_metadata()
    entry = resolve(relative_path)

    cache_root = _config.get_cache_dir() / _slug(_config.get_doi()) / meta.version
    local_path = cache_root / relative_path.strip("/")
    if local_path.exists() and not force:
        return local_path

    local_path.parent.mkdir(parents=True, exist_ok=True)
    url = f"{_config.get_server()}/api/access/datafile/{entry.id}"
    with requests.get(url, stream=True, timeout=300) as resp:
        resp.raise_for_status()
        tmp = local_path.with_suffix(local_path.suffix + ".part")
        with tmp.open("wb") as fh:
            for chunk in resp.iter_content(chunk_size=_CHUNK):
                if chunk:
                    fh.write(chunk)
        tmp.replace(local_path)
    return local_path


def _reset_cache() -> None:
    """Clear the in-process metadata cache. Primarily for tests."""
    _metadata_cache.clear()
