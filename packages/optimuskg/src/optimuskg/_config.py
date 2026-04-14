"""Runtime configuration: Dataverse server, DOI, and local cache directory."""

from __future__ import annotations

import os
from pathlib import Path
from typing import TypedDict

from platformdirs import user_cache_dir

DEFAULT_SERVER = "https://dataverse.harvard.edu"
# TODO: update with the real DOI after the Dataverse release is made, and verify
# that `fetch_metadata` resolves it without a 404.
DEFAULT_DOI = "doi:10.7910/DVN/IXA7BM"


class _State(TypedDict):
    """In-process overrides for the three configuration values."""

    server: str | None
    doi: str | None
    cache_dir: str | None


_state: _State = {"server": None, "doi": None, "cache_dir": None}


def get_server() -> str:
    """Return the Dataverse server URL."""
    return _state["server"] or os.environ.get("OPTIMUSKG_SERVER") or DEFAULT_SERVER


def set_server(url: str) -> None:
    """Override the Dataverse server URL for the current process."""
    _state["server"] = url.rstrip("/")


def get_doi() -> str:
    """Return the persistent identifier for the OptimusKG dataset."""
    return _state["doi"] or os.environ.get("OPTIMUSKG_DOI") or DEFAULT_DOI


def set_doi(doi: str) -> None:
    """Override the dataset DOI for the current process."""
    _state["doi"] = doi


def get_cache_dir() -> Path:
    """Return the root directory where downloaded files are cached."""
    raw = _state["cache_dir"] or os.environ.get("OPTIMUSKG_CACHE_DIR")
    return Path(raw).expanduser() if raw else Path(user_cache_dir("optimuskg"))


def set_cache_dir(path: str | os.PathLike[str]) -> None:
    """Override the local cache directory for the current process."""
    _state["cache_dir"] = str(path)


def _reset() -> None:
    """Clear in-process overrides. Primarily for tests."""
    _state["server"] = None
    _state["doi"] = None
    _state["cache_dir"] = None
