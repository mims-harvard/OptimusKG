"""Shared test fixtures: isolate cache + in-process state for each test."""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest
from optimuskg import _config, _dataverse


@pytest.fixture(autouse=True)
def _isolate(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Redirect the cache dir to tmp and clear any in-process state before each test."""
    for var in ("OPTIMUSKG_SERVER", "OPTIMUSKG_DOI", "OPTIMUSKG_CACHE_DIR"):
        monkeypatch.delenv(var, raising=False)
    _config._reset()
    _dataverse._reset_cache()
    _config.set_cache_dir(tmp_path)


@pytest.fixture
def tiny_nodes() -> pl.DataFrame:
    """A 3-row nodes DataFrame matching the gold schema."""
    return pl.DataFrame(
        {
            "id": ["A", "B", "C"],
            "label": ["GEN", "DIS", "DRG"],
            "properties": [
                '{"name": "alpha"}',
                '{"name": "beta", "score": 0.5}',
                "",
            ],
        }
    )


@pytest.fixture
def tiny_edges() -> pl.DataFrame:
    """A 2-row edges DataFrame matching the gold schema."""
    return pl.DataFrame(
        {
            "from": ["A", "B"],
            "to": ["B", "C"],
            "label": ["GEN-DIS", "DIS-DRG"],
            "relation": ["ASSOCIATED", "INDICATED"],
            "undirected": [True, False],
            "properties": ['{"source": "test"}', ""],
        }
    )
