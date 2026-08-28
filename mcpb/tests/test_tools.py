"""Correctness tests for the OptimusKG MCP tools against the real graph."""

from __future__ import annotations

import pytest


def test_list_schema(graph):
    schema = graph.list_schema()
    types = {t["type"] for t in schema["node_types"]}
    assert {"gene", "disease", "drug"} <= types
    edge_types = {e["edge_type"] for e in schema["edge_types"]}
    assert "DIS-GEN" in edge_types
    relations = {r["relation"] for r in schema["relations"]}
    assert "ASSOCIATED_WITH" in relations


def test_search_by_symbol(graph):
    res = graph.search_entities("TSPAN6", type="gene")
    assert res["count"] >= 1
    top = res["results"][0]
    assert top["symbol"] == "TSPAN6"
    assert top["type"] == "gene"


def test_search_by_disease_name(graph):
    res = graph.search_entities("Alzheimer disease", type="disease")
    assert res["count"] >= 1
    assert any("alzheimer" in (r["name"] or "").lower() for r in res["results"])


def test_search_type_filter(graph):
    res = graph.search_entities("insulin", type="drug")
    assert all(r["type"] == "drug" for r in res["results"])


def test_search_empty(graph):
    assert graph.search_entities("zzzznotarealentityzzzz")["count"] == 0


def test_get_entity(graph, ids):
    ent = graph.get_entity(ids["gene"])
    assert ent["type"] == "gene"
    assert ent["degree"] > 0
    assert isinstance(ent["properties"], dict)
    assert ent["connections"]


def test_get_entity_unknown(graph):
    ent = graph.get_entity("NOT_A_REAL_ID")
    assert "error" in ent


def test_get_neighbors_basic(graph, ids):
    res = graph.get_neighbors(ids["drug"], limit=10)
    assert res["count"] <= 10
    assert res["count"] >= 1
    assert res["total_matching"] >= res["count"]
    for n in res["neighbors"]:
        assert n["neighbor_id"]
        assert n["direction"] in ("in", "out")


def test_get_neighbors_min_score(graph, ids):
    res = graph.get_neighbors(
        ids["disease"], edge_type="DIS-GEN", min_score=0.5, limit=25
    )
    assert res["count"] >= 1
    for n in res["neighbors"]:
        assert n["edge_type"] == "DIS-GEN"
        assert n["score"] is not None and n["score"] >= 0.5
        assert n["neighbor_type"] == "gene"


def test_get_neighbors_direction(graph, ids):
    out = graph.get_neighbors(ids["disease"], direction="out", limit=5)
    assert all(n["direction"] == "out" for n in out["neighbors"])


def test_count_neighbors_matches_degree(graph, ids):
    ent = graph.get_entity(ids["hub_gene"])
    counts = graph.count_neighbors(ids["hub_gene"], group_by="edge_type")
    assert counts["total"] == ent["degree"]


def test_find_connection_direct_and_two_hop(graph, ids):
    res = graph.find_connection(ids["drug"], ids["disease"], max_hops=2)
    assert res["connected"] is True
    assert res["hops"] <= 2
    for path in res["paths"]:
        assert path[0]["id"] == ids["drug"]
        assert path[-1]["id"] == ids["disease"]


def test_find_connection_self(graph, ids):
    res = graph.find_connection(ids["gene"], ids["gene"])
    assert res["connected"] is True
    assert res["hops"] == 0


def test_find_connection_max_hops_1_respected(graph, ids):
    """A 2-hop-apart pair must report unconnected when max_hops=1."""
    two_hop = graph.find_connection(ids["drug"], ids["disease"], max_hops=2)
    if two_hop.get("hops") != 2:
        pytest.skip("reference drug/disease are not exactly 2 hops apart")
    one_hop = graph.find_connection(ids["drug"], ids["disease"], max_hops=1)
    assert one_hop["connected"] is False
    assert one_hop["paths"] == []


def test_find_connection_bad_hops(graph, ids):
    res = graph.find_connection(ids["drug"], ids["disease"], max_hops=5)
    assert "error" in res


def test_find_connection_unknown(graph, ids):
    res = graph.find_connection("NOT_A_REAL_ID", ids["disease"])
    assert "error" in res


def test_run_sql_select(graph):
    res = graph.run_sql("SELECT type, count(*) c FROM nodes GROUP BY type")
    assert res["row_count"] >= 1
    assert "type" in res["columns"]


@pytest.mark.parametrize(
    "query",
    [
        "DROP TABLE nodes",
        "DELETE FROM nodes",
        "INSERT INTO nodes VALUES ('x')",
        "PRAGMA database_list",
        "SELECT 1; SELECT 2",
        "ATTACH 'x.db'",
        "UPDATE nodes SET id = 'x'",
    ],
)
def test_run_sql_blocks_unsafe(graph, query):
    assert "error" in graph.run_sql(query)


@pytest.mark.parametrize(
    "query",
    [
        "SELECT * FROM read_csv('/etc/passwd')",
        "SELECT * FROM sniff_csv('/etc/passwd')",
        "SELECT * FROM read_json_objects('/etc/hostname')",
        "SELECT * FROM parquet_metadata('/etc/hostname')",
        "SELECT * FROM '/etc/passwd'",
        "SELECT * FROM 'https://example.com/x.csv'",
    ],
)
def test_run_sql_blocks_file_and_network_access(graph, query):
    """External access is disabled on the connection, so no reads escape."""
    assert "error" in graph.run_sql(query)


def test_run_sql_allows_literal_with_keyword(graph):
    """A forbidden word inside a string literal must not trip the guard."""
    res = graph.run_sql("SELECT * FROM nodes WHERE name = 'reset load set' LIMIT 1")
    assert "error" not in res


def test_run_sql_search_wildcards_are_literal(graph):
    """LIKE metacharacters in search terms match literally, not as wildcards."""
    res = graph.search_entities("___", type="gene")
    assert res["count"] == 0


def test_run_sql_enforces_limit(graph):
    res = graph.run_sql("SELECT id FROM nodes", max_rows=5)
    assert res["row_count"] == 5
    assert res["truncated"] is True
