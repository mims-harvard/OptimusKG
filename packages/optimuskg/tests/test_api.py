"""End-to-end test of the public API using mocked Dataverse responses."""

from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import polars as pl
import pytest

import optimuskg


@pytest.fixture
def stub_dataverse(
    mocker: Any,
    tmp_path: Path,
    tiny_nodes: pl.DataFrame,
    tiny_edges: pl.DataFrame,
) -> MagicMock:
    """Return a stub that serves metadata plus four canonical files."""
    nodes_path = tmp_path / "nodes.parquet"
    edges_path = tmp_path / "edges.parquet"
    lcc_nodes_path = tmp_path / "largest_connected_component_nodes.parquet"
    lcc_edges_path = tmp_path / "largest_connected_component_edges.parquet"
    tiny_nodes.write_parquet(nodes_path)
    tiny_edges.write_parquet(edges_path)
    tiny_nodes.write_parquet(lcc_nodes_path)
    tiny_edges.write_parquet(lcc_edges_path)

    by_id: dict[int, bytes] = {
        10: nodes_path.read_bytes(),
        11: edges_path.read_bytes(),
        20: lcc_nodes_path.read_bytes(),
        21: lcc_edges_path.read_bytes(),
    }

    metadata_payload = {
        "data": {
            "latestVersion": {
                "versionNumber": 2,
                "versionMinorNumber": 3,
                "files": [
                    {
                        "directoryLabel": "",
                        "dataFile": {"id": 10, "filename": "nodes.parquet"},
                    },
                    {
                        "directoryLabel": "",
                        "dataFile": {"id": 11, "filename": "edges.parquet"},
                    },
                    {
                        "directoryLabel": "",
                        "dataFile": {"id": 20, "filename": "lcc_nodes.parquet"},
                    },
                    {
                        "directoryLabel": "",
                        "dataFile": {"id": 21, "filename": "lcc_edges.parquet"},
                    },
                ],
            }
        }
    }

    def fake_get(url: str, *_: Any, **kwargs: Any) -> MagicMock:
        resp = MagicMock()
        resp.raise_for_status.return_value = None
        if "/api/datasets/" in url:
            resp.json.return_value = metadata_payload
            return resp
        if "/api/access/datafile/" in url:
            file_id = int(url.rsplit("/", 1)[-1])
            resp.iter_content.return_value = iter([by_id[file_id]])
            resp.__enter__ = MagicMock(return_value=resp)
            resp.__exit__ = MagicMock(return_value=False)
            return resp
        raise AssertionError(f"unexpected URL {url}")

    return mocker.patch("optimuskg._dataverse.requests.get", side_effect=fake_get)


def test_get_file_returns_cached_path(stub_dataverse: MagicMock) -> None:
    path = optimuskg.get_file("nodes.parquet")
    assert path.exists()
    assert path.name == "nodes.parquet"


def test_load_parquet_reads_dataframe(stub_dataverse: MagicMock) -> None:
    df = optimuskg.load_parquet("nodes.parquet")
    assert df.columns == ["id", "label", "properties"]
    assert len(df) == 3


def test_load_graph_full(stub_dataverse: MagicMock) -> None:
    nodes, edges = optimuskg.load_graph()
    assert set(nodes.columns) == {"id", "label", "properties"}
    assert set(edges.columns) == {
        "from",
        "to",
        "label",
        "relation",
        "undirected",
        "properties",
    }


def test_load_graph_lcc_uses_lcc_files(stub_dataverse: MagicMock) -> None:
    optimuskg.load_graph(lcc=True)
    urls = [c.args[0] for c in stub_dataverse.call_args_list]
    assert any("datafile/20" in u for u in urls)
    assert any("datafile/21" in u for u in urls)


def test_load_networkx_builds_graph(stub_dataverse: MagicMock) -> None:
    g = optimuskg.load_networkx(lcc=True)
    assert g.number_of_nodes() == 3
    assert g.number_of_edges() == 2


def test_load_networkx_warns_on_full_graph(stub_dataverse: MagicMock) -> None:
    with pytest.warns(UserWarning, match="several GB"):
        optimuskg.load_networkx()
