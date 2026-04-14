"""Tests for building a MultiDiGraph from node/edge DataFrames."""

from __future__ import annotations

import networkx as nx
import polars as pl

from optimuskg._graph import build_multidigraph


def test_build_multidigraph_parses_properties(
    tiny_nodes: pl.DataFrame, tiny_edges: pl.DataFrame
) -> None:
    g = build_multidigraph(tiny_nodes, tiny_edges)

    assert isinstance(g, nx.MultiDiGraph)
    assert g.number_of_nodes() == 3
    assert g.number_of_edges() == 2

    assert g.nodes["A"]["label"] == "GEN"
    assert g.nodes["A"]["name"] == "alpha"
    assert g.nodes["B"]["score"] == 0.5
    assert "name" not in g.nodes["C"]  # empty properties string -> no splat

    edge_ab = next(iter(g.get_edge_data("A", "B").values()))
    assert edge_ab["label"] == "GEN-DIS"
    assert edge_ab["relation"] == "ASSOCIATED"
    assert edge_ab["undirected"] is True
    assert edge_ab["source"] == "test"


def test_build_multidigraph_without_parsing_properties(
    tiny_nodes: pl.DataFrame, tiny_edges: pl.DataFrame
) -> None:
    g = build_multidigraph(tiny_nodes, tiny_edges, parse_properties=False)
    assert g.nodes["A"]["properties"] == '{"name": "alpha"}'
    assert "name" not in g.nodes["A"]


def test_build_multidigraph_preserves_edge_direction(
    tiny_nodes: pl.DataFrame, tiny_edges: pl.DataFrame
) -> None:
    g = build_multidigraph(tiny_nodes, tiny_edges)
    # MultiDiGraph: A->B exists, B->A does not (even though undirected=True)
    assert g.has_edge("A", "B")
    assert not g.has_edge("B", "A")
