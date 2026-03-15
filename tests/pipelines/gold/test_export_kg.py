import polars as pl
import pytest

from optimuskg.pipelines.gold.nodes.export_kg import (
    _validate_global_id_uniqueness,
)


def _make_node_df(ids: list[str], label: str) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "id": ids,
            "label": [label] * len(ids),
            "properties": [{"name": f"n{i}"} for i in range(len(ids))],
        }
    )


class TestValidateGlobalIdUniqueness:
    def test_passes_when_all_ids_unique(self):
        nodes = {
            "gene": _make_node_df(["ENSG1", "ENSG2"], "GEN"),
            "disease": _make_node_df(["MONDO_0005148"], "DIS"),
        }
        _validate_global_id_uniqueness(nodes)  # should not raise

    def test_raises_on_duplicate_ids_across_types(self):
        nodes = {
            "disease": _make_node_df(["HP_0001250"], "DIS"),
            "phenotype": _make_node_df(["HP_0001250"], "PHE"),
        }
        with pytest.raises(ValueError, match="non-unique"):
            _validate_global_id_uniqueness(nodes)

    def test_error_message_includes_node_types(self):
        nodes = {
            "disease": _make_node_df(["HP_0001250"], "DIS"),
            "phenotype": _make_node_df(["HP_0001250"], "PHE"),
        }
        with pytest.raises(ValueError, match="disease") as exc_info:
            _validate_global_id_uniqueness(nodes)
        assert "phenotype" in str(exc_info.value)

    def test_raises_on_duplicate_within_same_type(self):
        nodes = {
            "gene": _make_node_df(["ENSG1", "ENSG1"], "GEN"),
        }
        with pytest.raises(ValueError, match="non-unique"):
            _validate_global_id_uniqueness(nodes)

    def test_passes_with_all_empty(self):
        nodes = {"gene": _make_node_df([], "GEN")}
        _validate_global_id_uniqueness(nodes)  # should not raise

    def test_passes_with_no_overlap(self):
        nodes = {
            "gene": _make_node_df(["ENSG00000141510"], "GEN"),
            "drug": _make_node_df(["CHEMBL25"], "DRG"),
            "disease": _make_node_df(["MONDO_0005148"], "DIS"),
        }
        _validate_global_id_uniqueness(nodes)  # should not raise

    def test_raises_with_multiple_collisions(self):
        nodes = {
            "disease": _make_node_df(["HP_001", "GO_001"], "DIS"),
            "phenotype": _make_node_df(["HP_001"], "PHE"),
            "biological_process": _make_node_df(["GO_001"], "BPO"),
        }
        with pytest.raises(ValueError, match="2 non-unique"):
            _validate_global_id_uniqueness(nodes)

    def test_passes_with_mix_of_empty_and_non_empty(self):
        nodes = {
            "gene": _make_node_df(["ENSG1"], "GEN"),
            "disease": _make_node_df([], "DIS"),
        }
        _validate_global_id_uniqueness(nodes)  # should not raise
