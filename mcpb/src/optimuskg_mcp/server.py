"""MCP tools over the OptimusKG graph.

Each tool is a thin async wrapper that offloads the synchronous DuckDB call to a
worker thread, so a slow query never blocks the event loop. The tool docstrings
are the descriptions Claude reads to choose a tool, so they are written for it.
"""

from __future__ import annotations

import threading

import anyio
from mcp.server.fastmcp import FastMCP

from .db import connect
from .tools import Graph

mcp = FastMCP("optimuskg")

_graph: Graph | None = None
_graph_lock = threading.Lock()


def graph() -> Graph:
    """Return the shared Graph, building/opening the index once on first use."""
    global _graph
    if _graph is None:
        with _graph_lock:
            if _graph is None:
                _graph = Graph(connect())
    return _graph


@mcp.tool()
async def list_schema() -> dict:
    """Describe the graph: node types, edge types, and the relation vocabulary.

    Call this first when unsure how the graph is structured or before writing a
    `run_sql` query. Returns entity types (gene, disease, drug, ...), edge type
    codes (SOURCE-TARGET, e.g. DRG-GEN = drug→gene), and which relations
    (TARGET, INDICATION, ASSOCIATED_WITH, IS_A, ...) occur on each edge type,
    with counts.
    """
    return await anyio.to_thread.run_sync(lambda: graph().list_schema())


@mcp.tool()
async def search_entities(query: str, type: str | None = None, limit: int = 25) -> dict:
    """Find entities by name, gene symbol, synonym, or id.

    This is the entry point for almost every question: turn a mention like
    "Alzheimer disease", "TSPAN6", or "aspirin" into concrete node ids you can
    pass to the other tools. Optionally restrict to a `type` (e.g. "gene",
    "disease", "drug", "phenotype", "pathway", "anatomy", "exposure"). Results
    are ranked with exact id/name/symbol matches first.
    """
    return await anyio.to_thread.run_sync(
        lambda: graph().search_entities(query, type=type, limit=limit)
    )


@mcp.tool()
async def get_entity(id: str) -> dict:
    """Return an entity's full properties and a summary of its connections.

    Give it a node id (from `search_entities`). Returns the parsed `properties`
    (descriptions, cross-references, synonyms, etc.), the total `degree`, and a
    breakdown of neighbours by type and relation, so you can see what kinds of
    connections exist before expanding them with `get_neighbors`.
    """
    return await anyio.to_thread.run_sync(lambda: graph().get_entity(id))


@mcp.tool()
async def get_neighbors(
    id: str,
    relation: str | None = None,
    edge_type: str | None = None,
    direction: str = "both",
    min_score: float | None = None,
    limit: int = 50,
) -> dict:
    """List the entities directly connected to an entity (one hop).

    The workhorse for "what is X connected to" questions. Filter with:
    `relation` (e.g. TARGET, INDICATION, ASSOCIATED_WITH), `edge_type` (e.g.
    DRG-GEN), `direction` ("out", "in", or "both"), and `min_score` — a minimum
    numeric strength that applies to association edges (DIS-GEN / PHE-GEN carry
    evidence/DisGeNET scores; most structural edges have no score and are
    dropped when `min_score` is set). Results are ranked by score. Reports
    `total_matching` and `truncated` so you know if more exist beyond `limit`.
    """
    return await anyio.to_thread.run_sync(
        lambda: graph().get_neighbors(
            id,
            relation=relation,
            edge_type=edge_type,
            direction=direction,
            min_score=min_score,
            limit=limit,
        )
    )


@mcp.tool()
async def count_neighbors(id: str, group_by: str = "edge_type") -> dict:
    """Count an entity's neighbours, grouped for a quick profile.

    Answers "how many drugs target this gene?" or "how is this disease
    connected?" without listing every neighbour. `group_by` is one of
    "edge_type", "relation", "neighbor_type", or "direction".
    """
    return await anyio.to_thread.run_sync(
        lambda: graph().count_neighbors(id, group_by=group_by)
    )


@mcp.tool()
async def find_connection(a: str, b: str, max_hops: int = 2) -> dict:
    """Find the shortest path(s) between two entities.

    Answers "how is drug X related to disease Y?". Finds the shortest undirected
    path by intersecting the two entities' neighbourhoods (meet-in-the-middle),
    so even hub nodes resolve quickly; each node on the path is annotated with
    its type and name.

    IMPORTANT: keep `max_hops` at 2 (the default). The graph has 21.8M edges, so
    a 3-hop search joins whole neighbourhoods and is markedly slower. `max_hops`
    above 3 is rejected; 3 should be used sparingly.
    """
    return await anyio.to_thread.run_sync(
        lambda: graph().find_connection(a, b, max_hops=max_hops)
    )


@mcp.tool()
async def run_sql(query: str, max_rows: int = 1000) -> dict:
    """Run a read-only SQL SELECT for questions the other tools do not cover.

    Escape hatch for aggregations and novel joins (e.g. "top 10 diseases by
    number of associated genes"). Three tables are available:

    * `nodes(id, type, name, symbol, full_name, search_blob, properties)`
    * `edges(from_id, to_id, edge_type, relation, score)`
    * `adj(s, t, edge_type, relation, score, reverse)` — undirected, indexed on `s`

    Node `properties` is a JSON string — use DuckDB JSON functions, e.g.
    `json_extract_string(properties, '$.name')`. Edge JSON is not materialised;
    the parsed association strength is the `score` column. Only a single
    read-only SELECT/WITH statement is permitted (no writes, no file/network
    access); results are capped at `max_rows`.
    """
    return await anyio.to_thread.run_sync(
        lambda: graph().run_sql(query, max_rows=max_rows)
    )


def run_stdio() -> None:
    """Serve over stdio. The index builds lazily on the first tool call, so
    start-up and MCP initialization return immediately."""
    mcp.run()


if __name__ == "__main__":
    run_stdio()
