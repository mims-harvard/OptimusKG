"""Query tools over the OptimusKG graph.

Each method returns JSON-serialisable data and runs on its own DuckDB cursor —
the connection's thread-safe unit — so concurrent tool calls don't interfere.
"""

from __future__ import annotations

import json
import re
import threading
from typing import Any

import duckdb

# The real boundary is the connection (read_only + external access off); this
# denylist is defence-in-depth against session/attach/extension statements,
# matched on SQL with literals and comments stripped so a quoted word is safe.
_FORBIDDEN_SQL = re.compile(
    r"\b(attach|detach|copy|install|load|pragma|set|reset|call|export|vacuum|"
    r"checkpoint)\b",
    re.IGNORECASE,
)

# Comments and quoted literals, stripped before denylist/`;` checks.
_SQL_NOISE = re.compile(
    r"--[^\n]*|/\*.*?\*/|'(?:[^']|'')*'|\"(?:[^\"]|\"\")*\"|\$\$.*?\$\$",
    re.DOTALL,
)


def _strip_sql_noise(sql: str) -> str:
    """Remove comments and quoted string/identifier literals from ``sql``."""
    return _SQL_NOISE.sub(" ", sql)


def _escape_like(value: str) -> str:
    r"""Escape LIKE metacharacters so user text matches literally (ESCAPE '\')."""
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


class Graph:
    """Read-only façade over the built DuckDB database."""

    def __init__(self, con: duckdb.DuckDBPyConnection) -> None:
        self._con = con

    # -- low level helpers ------------------------------------------------
    def _rows(self, sql: str, params: list[Any] | None = None) -> list[dict]:
        cur = self._con.cursor()
        try:
            res = cur.execute(sql, params or [])
            cols = [d[0] for d in res.description]
            return [dict(zip(cols, row)) for row in res.fetchall()]
        finally:
            cur.close()

    def _rows_with_timeout(
        self, sql: str, params: list[Any], timeout_s: float
    ) -> list[dict] | None:
        """Run a query on a dedicated cursor, interrupting it past ``timeout_s``.

        Returns ``None`` if the query was interrupted for exceeding the budget.
        """
        cur = self._con.cursor()
        box: dict[str, Any] = {}

        def _run() -> None:
            try:
                res = cur.execute(sql, params)
                cols = [d[0] for d in res.description]
                box["rows"] = [dict(zip(cols, r)) for r in res.fetchall()]
            except Exception as exc:  # noqa: BLE001 - includes interrupt
                box["err"] = exc

        thread = threading.Thread(target=_run, daemon=True)
        thread.start()
        thread.join(timeout_s)
        try:
            if thread.is_alive():
                cur.interrupt()
                thread.join(5)
                if thread.is_alive():
                    raise TimeoutError("Query did not stop after interrupt.")
                return None
            if "err" in box:
                raise box["err"]
            return box["rows"]
        finally:
            cur.close()

    def _node(self, node_id: str) -> dict | None:
        rows = self._rows(
            "SELECT id, type, name, symbol, full_name, properties "
            "FROM nodes WHERE id = ?",
            [node_id],
        )
        return rows[0] if rows else None

    def _names_for(self, ids: list[str]) -> dict[str, dict]:
        if not ids:
            return {}
        placeholders = ", ".join("?" for _ in ids)
        rows = self._rows(
            f"SELECT id, type, name FROM nodes WHERE id IN ({placeholders})", ids
        )
        return {r["id"]: r for r in rows}

    # -- tools ------------------------------------------------------------
    def list_schema(self) -> dict:
        """Return the node types, edge types and relation vocabulary."""
        node_types = self._rows(
            "SELECT type, count(*) AS count FROM nodes GROUP BY type ORDER BY count DESC"
        )
        edge_types = self._rows(
            "SELECT edge_type, count(*) AS count FROM edges "
            "GROUP BY edge_type ORDER BY count DESC"
        )
        relations = self._rows(
            "SELECT relation, sum(n) AS count FROM meta_relations "
            "GROUP BY relation ORDER BY count DESC"
        )
        edge_relations = self._rows(
            "SELECT edge_type, relation, n AS count FROM meta_relations "
            "ORDER BY edge_type, count DESC"
        )
        return {
            "node_types": node_types,
            "edge_types": edge_types,
            "relations": relations,
            "edge_type_relations": edge_relations,
            "notes": (
                "Edge type codes read as SOURCE-TARGET (e.g. DRG-GEN = drug→gene). "
                "Numeric edge scores exist mainly on association edges "
                "(DIS-GEN / PHE-GEN)."
            ),
        }

    def search_entities(
        self, query: str, type: str | None = None, limit: int = 25
    ) -> dict:
        """Resolve a name/symbol/id to graph entities, best matches first."""
        q = (query or "").strip().lower()
        if not q:
            return {"query": query, "results": [], "count": 0}
        limit = max(1, min(int(limit), 200))
        ql = _escape_like(q)
        rows = self._rows(
            r"""
            SELECT id, type, name, symbol, full_name,
                CASE
                    WHEN lower(id) = ? THEN 0
                    WHEN lower(coalesce(symbol, '')) = ?
                         OR lower(name) = ?
                         OR lower(coalesce(full_name, '')) = ? THEN 1
                    WHEN lower(name) LIKE ? ESCAPE '\'
                         OR lower(coalesce(symbol, '')) LIKE ? ESCAPE '\' THEN 2
                    ELSE 3
                END AS match_rank
            FROM nodes
            WHERE (? OR type = ?)
              AND (lower(id) = ? OR search_blob LIKE ? ESCAPE '\')
            ORDER BY match_rank, length(name), name
            LIMIT ?
            """,
            [
                q,
                q,
                q,
                q,
                ql + "%",
                ql + "%",
                type is None,
                type or "",
                q,
                "%" + ql + "%",
                limit,
            ],
        )
        for r in rows:
            r.pop("match_rank", None)
        return {"query": query, "type": type, "count": len(rows), "results": rows}

    def get_entity(self, id: str) -> dict:
        """Return an entity's full properties and a summary of its connections."""
        node = self._node(id)
        if node is None:
            return {"error": f"No entity with id '{id}'.", "id": id}
        try:
            props = json.loads(node["properties"]) if node["properties"] else {}
        except (TypeError, json.JSONDecodeError):
            props = {"raw": node["properties"]}
        degree = self._rows("SELECT count(*) AS degree FROM adj WHERE s = ?", [id])[0][
            "degree"
        ]
        by_type = self._rows(
            """
            SELECT n.type AS neighbor_type, a.relation, count(*) AS count
            FROM adj a JOIN nodes n ON n.id = a.t
            WHERE a.s = ?
            GROUP BY n.type, a.relation
            ORDER BY count DESC
            """,
            [id],
        )
        return {
            "id": node["id"],
            "type": node["type"],
            "name": node["name"],
            "symbol": node["symbol"],
            "full_name": node["full_name"],
            "properties": props,
            "degree": degree,
            "connections": by_type,
        }

    def get_neighbors(
        self,
        id: str,
        relation: str | None = None,
        edge_type: str | None = None,
        direction: str = "both",
        min_score: float | None = None,
        limit: int = 50,
    ) -> dict:
        """Return one-hop neighbours of an entity, filtered and score-ranked."""
        if self._node(id) is None:
            return {"error": f"No entity with id '{id}'.", "id": id}
        limit = max(1, min(int(limit), 500))
        where = ["a.s = ?"]
        params: list[Any] = [id]
        direction = (direction or "both").lower()
        if direction == "out":
            where.append("a.reverse = FALSE")
        elif direction == "in":
            where.append("a.reverse = TRUE")
        if relation:
            where.append("a.relation = ?")
            params.append(relation)
        if edge_type:
            where.append("a.edge_type = ?")
            params.append(edge_type)
        if min_score is not None:
            where.append("a.score >= ?")
            params.append(float(min_score))
        params.append(limit)
        rows = self._rows(
            f"""
            SELECT a.t AS neighbor_id, n.type AS neighbor_type, n.name AS neighbor_name,
                   a.relation, a.edge_type, a.score,
                   CASE WHEN a.reverse THEN 'in' ELSE 'out' END AS direction
            FROM adj a JOIN nodes n ON n.id = a.t
            WHERE {" AND ".join(where)}
            ORDER BY a.score DESC NULLS LAST, neighbor_name
            LIMIT ?
            """,
            params,
        )
        total = self._rows(
            f"SELECT count(*) AS c FROM adj a WHERE {' AND '.join(w for w in where)}",
            params[:-1],
        )[0]["c"]
        return {
            "id": id,
            "count": len(rows),
            "total_matching": total,
            "truncated": total > len(rows),
            "neighbors": rows,
        }

    def count_neighbors(self, id: str, group_by: str = "edge_type") -> dict:
        """Aggregate an entity's neighbours by edge_type/relation/neighbor_type."""
        if self._node(id) is None:
            return {"error": f"No entity with id '{id}'.", "id": id}
        group_by = (group_by or "edge_type").lower()
        if group_by == "neighbor_type":
            sql = (
                "SELECT n.type AS group, count(*) AS count "
                "FROM adj a JOIN nodes n ON n.id = a.t "
                "WHERE a.s = ? GROUP BY n.type ORDER BY count DESC"
            )
        elif group_by == "direction":
            sql = (
                "SELECT CASE WHEN a.reverse THEN 'in' ELSE 'out' END AS group, "
                "count(*) AS count FROM adj a WHERE a.s = ? "
                "GROUP BY a.reverse ORDER BY count DESC"
            )
        elif group_by == "relation":
            sql = (
                "SELECT a.relation AS group, count(*) AS count FROM adj a "
                "WHERE a.s = ? GROUP BY a.relation ORDER BY count DESC"
            )
        else:
            group_by = "edge_type"
            sql = (
                "SELECT a.edge_type AS group, count(*) AS count FROM adj a "
                "WHERE a.s = ? GROUP BY a.edge_type ORDER BY count DESC"
            )
        groups = self._rows(sql, [id])
        return {
            "id": id,
            "group_by": group_by,
            "total": sum(g["count"] for g in groups),
            "groups": groups,
        }

    _MAX_PATHS = 10

    def find_connection(self, a: str, b: str, max_hops: int = 2) -> dict:
        """Find shortest connection path(s) between two entities.

        Searches for the shortest undirected path up to ``max_hops`` using a
        meet-in-the-middle strategy (intersect the two entities' neighbourhoods)
        rather than a frontier-expanding BFS, so even hub nodes resolve quickly.
        **Keep ``max_hops`` at 2**: the graph has 21.8M edges, and 3-hop search
        joins whole neighbourhoods and is markedly slower. Values above 3 are
        rejected.
        """
        max_hops = int(max_hops)
        if max_hops < 1 or max_hops > 3:
            return {
                "error": "max_hops must be between 1 and 3 (2 recommended).",
                "a": a,
                "b": b,
            }
        node_a, node_b = self._node(a), self._node(b)
        if node_a is None:
            return {"error": f"No entity with id '{a}'.", "a": a, "b": b}
        if node_b is None:
            return {"error": f"No entity with id '{b}'.", "a": a, "b": b}
        if a == b:
            return {
                "a": a,
                "b": b,
                "connected": True,
                "hops": 0,
                "max_hops": max_hops,
                "path_count": 1,
                "paths": [[_endpoint(node_a)]],
            }

        # Hop 1: a direct edge.
        if self._rows("SELECT 1 FROM adj WHERE s = ? AND t = ? LIMIT 1", [a, b]):
            return self._paths_result(a, b, max_hops, [[a, b]])

        if max_hops < 2:
            return {
                "a": a,
                "b": b,
                "connected": False,
                "max_hops": max_hops,
                "paths": [],
            }

        # Hop 2: a -- m -- b (shared neighbour). Two index lookups + hash join.
        mids = self._rows(
            """
            SELECT DISTINCT x.t AS m
            FROM adj x JOIN adj y ON x.t = y.t
            WHERE x.s = ? AND y.s = ?
            LIMIT ?
            """,
            [a, b, self._MAX_PATHS],
        )
        if mids:
            return self._paths_result(a, b, max_hops, [[a, r["m"], b] for r in mids])

        if max_hops < 3:
            return {
                "a": a,
                "b": b,
                "connected": False,
                "max_hops": max_hops,
                "paths": [],
            }

        # Hop 3: a -- m1 -- m2 -- b, an edge linking a's and b's neighbourhoods.
        rows = self._rows_with_timeout(
            """
            WITH na AS (SELECT DISTINCT t AS m FROM adj WHERE s = ?),
                 nb AS (SELECT DISTINCT t AS m FROM adj WHERE s = ?)
            SELECT DISTINCT z.s AS m1, z.t AS m2
            FROM adj z JOIN na ON na.m = z.s JOIN nb ON nb.m = z.t
            WHERE z.s <> ? AND z.t <> ?
            LIMIT ?
            """,
            [a, b, b, a, self._MAX_PATHS],
            timeout_s=20.0,
        )
        if rows is None:
            return {
                "a": a,
                "b": b,
                "connected": None,
                "max_hops": max_hops,
                "error": "3-hop search exceeded the time budget; try max_hops=2.",
            }
        if rows:
            return self._paths_result(
                a, b, max_hops, [[a, r["m1"], r["m2"], b] for r in rows]
            )
        return {"a": a, "b": b, "connected": False, "max_hops": max_hops, "paths": []}

    def _paths_result(
        self, a: str, b: str, max_hops: int, id_paths: list[list[str]]
    ) -> dict:
        all_ids = {i for path in id_paths for i in path}
        names = self._names_for(list(all_ids))
        paths = [
            [
                {
                    "id": i,
                    "type": names.get(i, {}).get("type"),
                    "name": names.get(i, {}).get("name", i),
                }
                for i in path
            ]
            for path in id_paths
        ]
        return {
            "a": a,
            "b": b,
            "connected": True,
            "hops": len(id_paths[0]) - 1,
            "max_hops": max_hops,
            "path_count": len(paths),
            "paths": paths,
        }

    def run_sql(self, query: str, max_rows: int = 1000) -> dict:
        """Run a read-only SELECT against ``nodes`` / ``edges`` / ``adj``."""
        sql = (query or "").strip().rstrip(";").strip()
        if not sql:
            return {"error": "Empty query."}
        # Validate against a copy with comments and quoted literals removed, so a
        # forbidden word or ';' inside a string value is not a false positive.
        clean = _strip_sql_noise(sql).strip()
        if ";" in clean:
            return {"error": "Only a single statement is allowed."}
        if not (clean.lower().startswith("select") or clean.lower().startswith("with")):
            return {"error": "Only SELECT / WITH queries are allowed."}
        forbidden = _FORBIDDEN_SQL.search(clean)
        if forbidden:
            return {
                "error": f"Disallowed keyword: '{forbidden.group(0)}'. "
                "Only read-only SELECT/WITH over nodes/edges/adj is allowed."
            }
        max_rows = max(1, min(int(max_rows), 5000))
        wrapped = f"SELECT * FROM (\n{sql}\n) AS _q LIMIT {max_rows}"
        try:
            rows = self._rows_with_timeout(wrapped, [], timeout_s=20.0)
        except Exception as exc:  # noqa: BLE001 - report SQL/permission errors cleanly
            return {"error": f"Query failed: {exc}"}
        if rows is None:
            return {"error": "Query exceeded the time budget."}
        return {
            "columns": list(rows[0].keys()) if rows else [],
            "row_count": len(rows),
            "truncated": len(rows) >= max_rows,
            "rows": rows,
        }


def _endpoint(node: dict) -> dict:
    return {"id": node["id"], "type": node["type"], "name": node["name"]}
