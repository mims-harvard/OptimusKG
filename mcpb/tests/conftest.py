"""Session fixtures. Tests run against the real graph; the suite skips (rather
than fails) if the gold Parquet is absent and no override env var is set."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_NODES = REPO_ROOT / "data" / "gold" / "kg" / "parquet" / "nodes.parquet"
DEFAULT_EDGES = REPO_ROOT / "data" / "gold" / "kg" / "parquet" / "edges.parquet"


def _resolve_parquet() -> tuple[str, str] | None:
    nodes = os.environ.get("OPTIMUSKG_MCP_NODES") or (
        str(DEFAULT_NODES) if DEFAULT_NODES.is_file() else ""
    )
    edges = os.environ.get("OPTIMUSKG_MCP_EDGES") or (
        str(DEFAULT_EDGES) if DEFAULT_EDGES.is_file() else ""
    )
    if nodes and edges and Path(nodes).is_file() and Path(edges).is_file():
        return nodes, edges
    return None


@pytest.fixture(scope="session")
def graph():
    from optimuskg_mcp.db import connect
    from optimuskg_mcp.tools import Graph

    resolved = _resolve_parquet()
    if resolved is None:
        pytest.skip(
            "Gold Parquet not found. Set OPTIMUSKG_MCP_NODES / OPTIMUSKG_MCP_EDGES "
            "or generate data/gold/kg/parquet/."
        )
    os.environ["OPTIMUSKG_MCP_NODES"], os.environ["OPTIMUSKG_MCP_EDGES"] = resolved

    # Use the same hardened, external-access-disabled connection the server uses,
    # so security tests exercise the real boundary.
    con = connect()
    try:
        yield Graph(con)
    finally:
        con.close()


@pytest.fixture(scope="session")
def ids(graph):
    """Resolve a stable set of entity ids used across tests and benchmarks."""

    def first(query, type=None):
        res = graph.search_entities(query, type=type, limit=1)["results"]
        return res[0]["id"] if res else None

    resolved = {
        "gene": first("TSPAN6", "gene"),
        "disease": first("Alzheimer disease", "disease"),
        "drug": first("aspirin", "drug"),
        "hub_gene": first("TP53", "gene"),
    }
    missing = [k for k, v in resolved.items() if v is None]
    if missing:
        pytest.skip(f"Could not resolve reference entities: {missing}")
    return resolved
