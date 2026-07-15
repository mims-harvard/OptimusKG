"""Build the DuckDB index once (cached, keyed by dataset identity + schema
version) and serve it over a hardened, read-only connection."""

from __future__ import annotations

import hashlib
import os
import re
import time
from pathlib import Path

import duckdb
from platformdirs import user_cache_dir

from .data import dataset_doi, resolve_parquet_paths
from .index import SCHEMA_VERSION, build_index

_STALE_LOCK_SECONDS = 1800
_SERVE_CONFIG = {
    # external access off blocks the run_sql file-read/SSRF vectors; the memory
    # ceiling stops a runaway query OOM-ing the host. threads=4 is also faster
    # here — these scans are small enough that coordination beats more parallelism.
    "enable_external_access": False,
    "memory_limit": "4GB",
    "threads": 4,
}


def _source_identity() -> str:
    """Cache key for the current data source (local signature or DOI slug)."""
    nodes = os.environ.get("OPTIMUSKG_MCP_NODES")
    edges = os.environ.get("OPTIMUSKG_MCP_EDGES")
    if nodes and edges and Path(nodes).is_file() and Path(edges).is_file():
        parts = []
        for p in (nodes, edges):
            st = Path(p).stat()
            parts.append(f"{st.st_size}-{int(st.st_mtime)}")
        digest = hashlib.sha1("|".join(parts).encode()).hexdigest()[:12]
        return f"local_{digest}"
    return re.sub(r"[^A-Za-z0-9]+", "_", dataset_doi()).strip("_") or "default"


def _cache_db_path() -> Path:
    explicit = os.environ.get("OPTIMUSKG_MCP_DB")
    if explicit:
        return Path(explicit)
    root = Path(os.environ.get("OPTIMUSKG_CACHE_DIR") or user_cache_dir("optimuskg"))
    return root / "mcp" / f"graph_v{SCHEMA_VERSION}_{_source_identity()}.duckdb"


def _is_valid(db_path: Path) -> bool:
    if not db_path.is_file():
        return False
    try:
        con = duckdb.connect(str(db_path), read_only=True)
    except duckdb.Error:
        return False
    try:
        row = con.execute(
            "SELECT value FROM meta_info WHERE key = 'schema_version'"
        ).fetchone()
        return bool(row) and row[0] == str(SCHEMA_VERSION)
    except duckdb.Error:
        return False
    finally:
        con.close()


def _build_into(db_path: Path) -> None:
    tmp = db_path.with_name(f"{db_path.stem}.building.{os.getpid()}.duckdb")
    if tmp.exists():
        tmp.unlink()
    nodes_path, edges_path = resolve_parquet_paths()
    con = duckdb.connect(str(tmp))  # external access needed to read source Parquet
    try:
        build_index(con, nodes_path, edges_path, doi=dataset_doi())
    finally:
        con.close()
    tmp.replace(db_path)


def ensure_built(db_path: Path | None = None) -> Path:
    """Build the DuckDB index if missing/stale; return its path.

    Guards the check→build→replace sequence with an exclusive lock file so two
    processes launching at once cannot corrupt each other's build. A lock left by
    a crashed builder is broken after ``_STALE_LOCK_SECONDS``.
    """
    db_path = db_path or _cache_db_path()
    if _is_valid(db_path):
        return db_path

    db_path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = db_path.with_suffix(".lock")
    while True:
        try:
            fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError:
            if _is_valid(db_path):
                return db_path
            try:
                if time.time() - os.stat(lock_path).st_mtime > _STALE_LOCK_SECONDS:
                    os.unlink(lock_path)
                    continue
            except OSError:
                pass
            time.sleep(0.5)
            continue
        try:
            if _is_valid(db_path):  # another process finished while we waited
                return db_path
            _build_into(db_path)
            return db_path
        finally:
            os.close(fd)
            try:
                os.unlink(lock_path)
            except OSError:
                pass


def connect(db_path: Path | None = None) -> duckdb.DuckDBPyConnection:
    """Return a hardened read-only connection to the built-or-cached index."""
    path = ensure_built(db_path)
    return duckdb.connect(str(path), read_only=True, config=dict(_SERVE_CONFIG))
