"""Session fixtures. Tests fetch the graph through the optimuskg client (the
same path the bundle uses at runtime) and skip if it cannot be fetched."""

from __future__ import annotations

import pytest


@pytest.fixture(scope="session")
def graph():
    from optimuskg_mcp.data import DataError
    from optimuskg_mcp.db import connect
    from optimuskg_mcp.tools import Graph

    # connect() downloads the graph via the optimuskg client (cached) and opens
    # the same hardened, external-access-disabled connection the server uses.
    try:
        con = connect()
    except DataError as exc:
        pytest.skip(f"OptimusKG graph could not be fetched: {exc}")

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
