"""Per-query micro-benchmarks (pytest-benchmark). Run via `make mcpb-profile`."""

from __future__ import annotations

import pytest


@pytest.fixture(scope="session")
def bench_ids(graph, ids):
    """Extend the base ids with a concrete disease→gene neighbour for 1-hop."""
    extra = dict(ids)
    neigh = graph.get_neighbors(ids["disease"], edge_type="DIS-GEN", limit=1)
    extra["disease_gene"] = (
        neigh["neighbors"][0]["neighbor_id"] if neigh["neighbors"] else None
    )
    return extra


@pytest.mark.benchmark(group="schema")
def test_bench_list_schema(benchmark, graph):
    benchmark(graph.list_schema)


@pytest.mark.benchmark(group="search")
def test_bench_search_symbol(benchmark, graph):
    benchmark(graph.search_entities, "TSPAN6", "gene")


@pytest.mark.benchmark(group="search")
def test_bench_search_disease_name(benchmark, graph):
    benchmark(graph.search_entities, "Alzheimer disease", "disease")


@pytest.mark.benchmark(group="search")
def test_bench_search_substring(benchmark, graph):
    benchmark(graph.search_entities, "kinase")


@pytest.mark.benchmark(group="get_entity")
def test_bench_get_entity_gene(benchmark, graph, bench_ids):
    benchmark(graph.get_entity, bench_ids["gene"])


@pytest.mark.benchmark(group="get_entity")
def test_bench_get_entity_disease(benchmark, graph, bench_ids):
    benchmark(graph.get_entity, bench_ids["disease"])


@pytest.mark.benchmark(group="get_neighbors")
def test_bench_neighbors_drug(benchmark, graph, bench_ids):
    benchmark(graph.get_neighbors, bench_ids["drug"], None, None, "both", None, 50)


@pytest.mark.benchmark(group="get_neighbors")
def test_bench_neighbors_disease_scored(benchmark, graph, bench_ids):
    benchmark(
        lambda: graph.get_neighbors(
            bench_ids["disease"], edge_type="DIS-GEN", min_score=0.3, limit=50
        )
    )


@pytest.mark.benchmark(group="count_neighbors")
def test_bench_count_neighbors_hub(benchmark, graph, bench_ids):
    benchmark(graph.count_neighbors, bench_ids["hub_gene"], "edge_type")


@pytest.mark.benchmark(group="find_connection")
def test_bench_find_connection_direct(benchmark, graph, bench_ids):
    if bench_ids["disease_gene"] is None:
        pytest.skip("no disease_gene resolved")
    benchmark(graph.find_connection, bench_ids["disease"], bench_ids["disease_gene"], 2)


@pytest.mark.benchmark(group="find_connection")
def test_bench_find_connection_drug_disease(benchmark, graph, bench_ids):
    benchmark(graph.find_connection, bench_ids["drug"], bench_ids["disease"], 2)


@pytest.mark.benchmark(group="find_connection")
def test_bench_find_connection_gene_disease(benchmark, graph, bench_ids):
    benchmark(graph.find_connection, bench_ids["gene"], bench_ids["disease"], 2)


@pytest.mark.benchmark(group="run_sql")
def test_bench_run_sql_aggregation(benchmark, graph):
    benchmark(
        graph.run_sql,
        "SELECT n.name, count(*) c FROM edges e JOIN nodes n ON n.id = e.from_id "
        "WHERE e.edge_type = 'DIS-GEN' GROUP BY n.name ORDER BY c DESC LIMIT 10",
    )
