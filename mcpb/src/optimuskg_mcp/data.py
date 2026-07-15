"""Resolve the Parquet files that back the graph."""

from __future__ import annotations

import os
from pathlib import Path


class DataError(RuntimeError):
    """Raised when the gold Parquet files cannot be located or fetched."""


def _local_override() -> tuple[str, str] | None:
    nodes = os.environ.get("OPTIMUSKG_MCP_NODES")
    edges = os.environ.get("OPTIMUSKG_MCP_EDGES")
    if nodes and edges:
        for label, p in (("nodes", nodes), ("edges", edges)):
            if not Path(p).is_file():
                raise DataError(f"OPTIMUSKG_MCP_{label.upper()} does not exist: {p}")
        return nodes, edges
    return None


def resolve_parquet_paths() -> tuple[str, str]:
    """Return absolute paths to ``(nodes.parquet, edges.parquet)``.

    Prefers the local override env vars; otherwise downloads via the
    ``optimuskg`` client. The download is cached by the client, so it happens at
    most once per machine per dataset version.
    """
    override = _local_override()
    if override is not None:
        return override

    try:
        import optimuskg
    except ImportError as exc:  # pragma: no cover - dependency guaranteed by bundle
        raise DataError(
            "The 'optimuskg' client is not installed and no local Parquet "
            "override was provided."
        ) from exc

    doi = os.environ.get("OPTIMUSKG_DOI")
    if doi:
        optimuskg.set_doi(doi)

    try:
        nodes = optimuskg.get_file("nodes.parquet")
        edges = optimuskg.get_file("edges.parquet")
    except Exception as exc:  # noqa: BLE001 - surface any client/network failure
        raise DataError(f"Failed to fetch gold Parquet from Dataverse: {exc}") from exc

    return str(nodes), str(edges)


def dataset_doi() -> str:
    """Best-effort current dataset DOI, for cache keying and provenance."""
    doi = os.environ.get("OPTIMUSKG_DOI")
    if doi:
        return doi
    try:
        import optimuskg

        getter = getattr(optimuskg, "get_doi", None)
        if callable(getter):
            return str(getter())
    except Exception:  # noqa: BLE001
        pass
    return "default"
