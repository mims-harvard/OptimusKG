"""Build a NetworkX MultiDiGraph from the gold ``nodes`` and ``edges`` tables."""

from __future__ import annotations

import json
from typing import Literal, TypedDict, cast

import networkx as nx
import polars as pl


class _NodeRow(TypedDict):
    """One row of the gold ``nodes`` table."""

    id: str
    label: str
    properties: str


class _EdgeRow(TypedDict):
    """One row of the gold ``edges`` table."""

    # ``from`` is a Python keyword, so the alias-style construction form is required
    # when this TypedDict is built by hand. ``iter_rows(named=True)`` returns dicts
    # that already contain the ``"from"`` key, so this is only a shape declaration.
    label: str
    relation: str
    undirected: bool
    properties: str
    # plus ``"from": str`` and ``"to": str`` -- see ``_edge_endpoints``


def _edge_endpoints(row: _EdgeRow) -> tuple[str, str]:
    """Pull the ``from`` / ``to`` keys out of an edge row.

    Declared separately because ``from`` can't be a valid TypedDict field name.
    """
    # The row actually carries "from"/"to" string keys; the cast placates the
    # type checker given the TypedDict limitation above.
    raw = cast(dict[Literal["from", "to"], str], row)
    return raw["from"], raw["to"]


def build_multidigraph(
    nodes: pl.DataFrame,
    edges: pl.DataFrame,
    *,
    parse_properties: bool = True,
) -> nx.MultiDiGraph:
    """Return a ``MultiDiGraph`` populated from the gold node and edge DataFrames.

    When ``parse_properties=True`` the ``properties`` JSON string on each row is
    decoded and splatted into the node/edge attributes. Otherwise the raw string
    is stored on a single ``properties`` attribute.
    """
    graph: nx.MultiDiGraph = nx.MultiDiGraph()

    for raw_node in nodes.iter_rows(named=True):
        node = cast(_NodeRow, raw_node)
        node_attrs: dict[str, object] = {"label": node["label"]}
        props = node["properties"]
        if parse_properties and props:
            node_attrs.update(json.loads(props))
        elif props:
            node_attrs["properties"] = props
        graph.add_node(node["id"], **node_attrs)

    for raw_edge in edges.iter_rows(named=True):
        edge = cast(_EdgeRow, raw_edge)
        src, dst = _edge_endpoints(edge)
        edge_attrs: dict[str, object] = {
            "label": edge["label"],
            "relation": edge["relation"],
            "undirected": edge["undirected"],
        }
        props = edge["properties"]
        if parse_properties and props:
            edge_attrs.update(json.loads(props))
        elif props:
            edge_attrs["properties"] = props
        graph.add_edge(src, dst, **edge_attrs)

    return graph
