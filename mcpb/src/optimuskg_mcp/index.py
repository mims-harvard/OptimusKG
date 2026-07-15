"""Build a query-ready DuckDB database from the OptimusKG Parquet files.

This module materialises three tables optimised for the tool layer:

* ``nodes``: one row per entity with an extracted ``name``/``symbol`` and a
  lower-cased ``search_blob`` covering ids, names, and synonyms.
* ``edges``: the raw directed edges plus a parsed numeric ``score`` column.
* ``adj``: an *undirected* adjacency (each edge emitted in both directions)
  indexed on the source column, which powers neighbour lookups and the bounded
  breadth-first search behind ``find_connection``.

The resulting ``.duckdb`` file is cached and reused on every subsequent launch.
"""

from __future__ import annotations

import time
from pathlib import Path

import duckdb

from .schema import EDGE_SCORE_KEYS, SYNONYM_KEYS, node_type_case_sql

SCHEMA_VERSION = 4


def _score_expr(properties_col: str = "properties") -> str:
    """SQL that coalesces the first present numeric score key to a DOUBLE."""
    keys = ",\n            ".join(
        f"json_extract_string({properties_col}, '$.{k}')" for k in EDGE_SCORE_KEYS
    )
    return f"TRY_CAST(COALESCE(\n            {keys}\n        ) AS DOUBLE)"


def _search_blob_expr() -> str:
    """SQL building a lower-cased searchable text blob from a node's JSON."""
    parts = [
        "id",
        "json_extract_string(properties, '$.symbol')",
        "json_extract_string(properties, '$.name')",
    ]
    parts += [f"json_extract_string(properties, '$.{k}')" for k in SYNONYM_KEYS]
    return "lower(concat_ws(' ', " + ", ".join(parts) + "))"


def build_index(
    con: duckdb.DuckDBPyConnection,
    nodes_path: str | Path,
    edges_path: str | Path,
    *,
    doi: str | None = None,
) -> dict[str, object]:
    """Populate ``con`` with the ``nodes``/``edges``/``adj`` tables and indexes.

    Args:
        con: A writable DuckDB connection (usually to a fresh file).
        nodes_path: Path to the gold ``nodes.parquet``.
        edges_path: Path to the gold ``edges.parquet``.
        doi: Optional Dataverse DOI recorded in the provenance table.

    Returns:
        A dict of build statistics (row counts and elapsed seconds).
    """
    nodes_path = str(nodes_path)
    edges_path = str(edges_path)
    started = time.perf_counter()

    con.execute("PRAGMA enable_progress_bar=false")

    con.execute(
        f"""
        CREATE TABLE nodes AS
        SELECT
            id,
            {node_type_case_sql("label")} AS type,
            label AS type_code,
            COALESCE(
                json_extract_string(properties, '$.symbol'),
                json_extract_string(properties, '$.name'),
                id
            ) AS name,
            json_extract_string(properties, '$.symbol') AS symbol,
            json_extract_string(properties, '$.name') AS full_name,
            {_search_blob_expr()} AS search_blob,
            properties
        FROM read_parquet(?)
        """,
        [nodes_path],
    )

    # Undirected adjacency is the single edge source of truth: every edge is
    # emitted in both directions, so a single index on ``s`` powers all
    # neighbour lookups and the BFS. ``reverse`` marks rows going opposite the
    # stored direction (incoming). ``properties`` is deliberately not
    # materialised — storing 21.8M JSON blobs inflates the cache several-fold;
    # the one numeric signal the tools use is parsed into ``score``, and raw
    # edge JSON stays available through the ``optimuskg`` client.
    con.execute(
        f"""
        CREATE TABLE adj AS
        WITH e AS (
            SELECT "from" AS f, "to" AS t, label AS edge_type, relation,
                   {_score_expr()} AS score
            FROM read_parquet(?)
        )
        SELECT f AS s, t, edge_type, relation, score, FALSE AS reverse FROM e
        UNION ALL
        SELECT t AS s, f AS t, edge_type, relation, score, TRUE AS reverse FROM e
        """,
        [edges_path],
    )

    # ``edges`` is a thin view over the forward half of ``adj`` for run_sql.
    con.execute(
        """
        CREATE VIEW edges AS
        SELECT s AS from_id, t AS to_id, edge_type, relation, score
        FROM adj WHERE reverse = FALSE
        """
    )

    con.execute(
        """
        CREATE TABLE meta_relations AS
        SELECT edge_type, relation, count(*) AS n
        FROM adj WHERE reverse = FALSE GROUP BY edge_type, relation
        """
    )

    # Point lookups on node id/type; neighbour expansion on adj.s.
    con.execute("CREATE UNIQUE INDEX nodes_id ON nodes(id)")
    con.execute("CREATE INDEX nodes_type ON nodes(type)")
    con.execute("CREATE INDEX adj_s ON adj(s)")

    n_nodes = con.execute("SELECT count(*) FROM nodes").fetchone()[0]
    n_edges = con.execute("SELECT count(*) FROM edges").fetchone()[0]
    elapsed = time.perf_counter() - started

    con.execute(
        """
        CREATE TABLE meta_info (key VARCHAR, value VARCHAR)
        """
    )
    con.executemany(
        "INSERT INTO meta_info VALUES (?, ?)",
        [
            ("schema_version", str(SCHEMA_VERSION)),
            ("doi", doi or ""),
            ("n_nodes", str(n_nodes)),
            ("n_edges", str(n_edges)),
            ("build_seconds", f"{elapsed:.3f}"),
        ],
    )

    return {
        "n_nodes": n_nodes,
        "n_edges": n_edges,
        "build_seconds": round(elapsed, 3),
    }
