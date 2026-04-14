"""Tests for configuration overrides (DOI, server, cache dir)."""

from pathlib import Path

import pytest
from optimuskg import _config


def test_defaults() -> None:
    assert _config.get_doi() == _config.DEFAULT_DOI
    assert _config.get_server() == _config.DEFAULT_SERVER


def test_env_overrides(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("OPTIMUSKG_DOI", "doi:10.1234/TEST")
    monkeypatch.setenv("OPTIMUSKG_SERVER", "https://dv.example.org")
    monkeypatch.setenv("OPTIMUSKG_CACHE_DIR", str(tmp_path))
    _config._reset()
    assert _config.get_doi() == "doi:10.1234/TEST"
    assert _config.get_server() == "https://dv.example.org"
    assert _config.get_cache_dir() == tmp_path


def test_setters_take_precedence_over_env(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("OPTIMUSKG_DOI", "doi:10.9999/ENV")
    _config.set_doi("doi:10.1234/CODE")
    assert _config.get_doi() == "doi:10.1234/CODE"

    _config.set_cache_dir(tmp_path / "custom")
    assert _config.get_cache_dir() == tmp_path / "custom"
