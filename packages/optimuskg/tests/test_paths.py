"""Tests for relative-path <-> (directoryLabel, filename) conversion."""

from optimuskg._paths import split_relative, to_relative


def test_to_relative_root_file() -> None:
    assert to_relative("", "nodes.parquet") == "nodes.parquet"
    assert to_relative(None, "edges.parquet") == "edges.parquet"


def test_to_relative_nested() -> None:
    assert to_relative("nodes", "gene.parquet") == "nodes/gene.parquet"
    assert to_relative("edges", "anatomy_gene.parquet") == "edges/anatomy_gene.parquet"


def test_to_relative_strips_slashes() -> None:
    assert to_relative("/nodes/", "gene.parquet") == "nodes/gene.parquet"


def test_split_relative_root() -> None:
    assert split_relative("nodes.parquet") == ("", "nodes.parquet")


def test_split_relative_nested() -> None:
    assert split_relative("nodes/gene.parquet") == ("nodes", "gene.parquet")
    assert split_relative("/edges/disease_gene.parquet") == (
        "edges",
        "disease_gene.parquet",
    )
