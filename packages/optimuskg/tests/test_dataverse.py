"""Tests for Dataverse metadata lookup and file download with caching."""

from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import polars as pl
import pytest

from optimuskg import _config, _dataverse


def _metadata_payload() -> dict[str, Any]:
    return {
        "data": {
            "latestVersion": {
                "versionNumber": 1,
                "versionMinorNumber": 0,
                "files": [
                    {
                        "directoryLabel": "",
                        "dataFile": {"id": 1001, "filename": "nodes.parquet"},
                    },
                    {
                        "directoryLabel": "",
                        "dataFile": {"id": 1002, "filename": "edges.parquet"},
                    },
                    {
                        "directoryLabel": "",
                        "dataFile": {"id": 1003, "filename": "lcc_nodes.parquet"},
                    },
                    {
                        "directoryLabel": "",
                        "dataFile": {"id": 1004, "filename": "lcc_edges.parquet"},
                    },
                    {
                        "directoryLabel": "nodes",
                        "dataFile": {"id": 2001, "filename": "gene.parquet"},
                    },
                    {
                        "directoryLabel": "edges",
                        "dataFile": {"id": 3001, "filename": "anatomy_gene.parquet"},
                    },
                ],
            }
        }
    }


@pytest.fixture
def mock_requests(mocker: Any, tiny_nodes: pl.DataFrame, tmp_path: Path) -> MagicMock:
    """Stub out ``requests.get`` for metadata and file downloads."""
    parquet_bytes = tmp_path / "_src.parquet"
    tiny_nodes.write_parquet(parquet_bytes)
    payload = parquet_bytes.read_bytes()

    metadata_resp = MagicMock()
    metadata_resp.json.return_value = _metadata_payload()
    metadata_resp.raise_for_status.return_value = None

    file_resp = MagicMock()
    file_resp.raise_for_status.return_value = None
    file_resp.iter_content.return_value = iter([payload])
    file_resp.__enter__ = MagicMock(return_value=file_resp)
    file_resp.__exit__ = MagicMock(return_value=False)

    def fake_get(url: str, *_: Any, **kwargs: Any) -> MagicMock:
        if "/api/datasets/" in url:
            return metadata_resp
        if "/api/access/datafile/" in url:
            return file_resp
        raise AssertionError(f"unexpected URL {url}")

    return mocker.patch("optimuskg._dataverse.requests.get", side_effect=fake_get)


def test_fetch_metadata_builds_relative_path_index(mock_requests: MagicMock) -> None:
    meta = _dataverse.fetch_metadata()
    assert meta.version == "1.0"
    assert set(meta.files) == {
        "nodes.parquet",
        "edges.parquet",
        "lcc_nodes.parquet",
        "lcc_edges.parquet",
        "nodes/gene.parquet",
        "edges/anatomy_gene.parquet",
    }
    assert meta.files["nodes/gene.parquet"].id == 2001


def test_fetch_metadata_is_cached(mock_requests: MagicMock) -> None:
    _dataverse.fetch_metadata()
    _dataverse.fetch_metadata()
    metadata_calls = [
        c for c in mock_requests.call_args_list if "/api/datasets/" in c.args[0]
    ]
    assert len(metadata_calls) == 1


def test_resolve_unknown_path_raises(mock_requests: MagicMock) -> None:
    with pytest.raises(FileNotFoundError):
        _dataverse.resolve("does/not/exist.parquet")


def test_download_writes_and_reuses_cache(mock_requests: MagicMock) -> None:
    path_one = _dataverse.download("nodes/gene.parquet")
    assert path_one.exists()
    assert path_one.parent.name == "nodes"
    assert "1.0" in path_one.parts

    path_two = _dataverse.download("nodes/gene.parquet")
    assert path_two == path_one

    file_calls = [
        c for c in mock_requests.call_args_list if "/api/access/datafile/" in c.args[0]
    ]
    assert len(file_calls) == 1


def test_download_force_refetches(mock_requests: MagicMock) -> None:
    _dataverse.download("nodes/gene.parquet")
    _dataverse.download("nodes/gene.parquet", force=True)
    file_calls = [
        c for c in mock_requests.call_args_list if "/api/access/datafile/" in c.args[0]
    ]
    assert len(file_calls) == 2


def test_cache_path_includes_doi_and_version(mock_requests: MagicMock) -> None:
    path = _dataverse.download("nodes.parquet")
    cache_root = _config.get_cache_dir()
    assert path.is_relative_to(cache_root)
    rel = path.relative_to(cache_root)
    # <doi_slug>/<version>/<relative_path>
    assert rel.parts[-1] == "nodes.parquet"
    assert rel.parts[-2] == "1.0"
    assert "10_7910" in rel.parts[-3]
