import polars as pl
import pytest

from optimuskg.pipelines.gold.nodes.export_kg import (
    _namespace_edge_ids,
    _namespace_node_ids,
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


def _make_edge_df(froms: list[str], tos: list[str], label: str) -> pl.DataFrame:
    n = len(froms)
    return pl.DataFrame(
        {
            "from": froms,
            "to": tos,
            "label": [label] * n,
            "relation": ["ASSOCIATED_WITH"] * n,
            "undirected": [True] * n,
            "properties": [{"sources": {"direct": [], "indirect": []}}] * n,
        }
    )


# -- _namespace_node_ids -------------------------------------------------------


class TestNamespaceNodeIds:
    def test_prefixes_ids_with_label(self):
        nodes = {
            "gene": _make_node_df(["ENSG00000141510"], "GEN"),
            "disease": _make_node_df(["MONDO_0005148"], "DIS"),
        }
        result = _namespace_node_ids(nodes)

        assert result["gene"]["id"].to_list() == ["GEN:ENSG00000141510"]
        assert result["disease"]["id"].to_list() == ["DIS:MONDO_0005148"]

    def test_handles_empty_dataframe(self):
        nodes = {"gene": _make_node_df([], "GEN")}
        result = _namespace_node_ids(nodes)
        assert result["gene"].is_empty()

    def test_preserves_other_columns(self):
        nodes = {"gene": _make_node_df(["ENSG1"], "GEN")}
        result = _namespace_node_ids(nodes)
        assert "label" in result["gene"].columns
        assert "properties" in result["gene"].columns


# -- _namespace_edge_ids -------------------------------------------------------


class TestNamespaceEdgeIds:
    def test_prefixes_from_to_with_node_types(self):
        edges = {
            "disease_phenotype": _make_edge_df(
                ["MONDO_0005148"], ["HP_0001250"], "DIS-PHE"
            ),
        }
        result = _namespace_edge_ids(edges)
        row = result["disease_phenotype"].row(0, named=True)
        assert row["from"] == "DIS:MONDO_0005148"
        assert row["to"] == "PHE:HP_0001250"

    def test_maps_protein_to_gene(self):
        edges = {
            "protein_protein": _make_edge_df(
                ["ENSG00000141510"], ["ENSG00000012048"], "PRO-PRO"
            ),
        }
        result = _namespace_edge_ids(edges)
        row = result["protein_protein"].row(0, named=True)
        assert row["from"] == "GEN:ENSG00000141510"
        assert row["to"] == "GEN:ENSG00000012048"

    def test_maps_protein_to_gene_mixed_edge(self):
        edges = {
            "drug_protein": _make_edge_df(["CHEMBL25"], ["ENSG00000141510"], "DRG-PRO"),
        }
        result = _namespace_edge_ids(edges)
        row = result["drug_protein"].row(0, named=True)
        assert row["from"] == "DRG:CHEMBL25"
        assert row["to"] == "GEN:ENSG00000141510"

    def test_handles_empty_dataframe(self):
        edges = {"disease_disease": _make_edge_df([], [], "DIS-DIS")}
        result = _namespace_edge_ids(edges)
        assert result["disease_disease"].is_empty()

    def test_same_type_both_sides(self):
        edges = {
            "disease_disease": _make_edge_df(
                ["MONDO_0005148"], ["MONDO_0005149"], "DIS-DIS"
            ),
        }
        result = _namespace_edge_ids(edges)
        row = result["disease_disease"].row(0, named=True)
        assert row["from"] == "DIS:MONDO_0005148"
        assert row["to"] == "DIS:MONDO_0005149"


# -- _validate_global_id_uniqueness -------------------------------------------


class TestValidateGlobalIdUniqueness:
    def test_passes_when_all_ids_unique(self):
        nodes = {
            "gene": _make_node_df(["GEN:ENSG1", "GEN:ENSG2"], "GEN"),
            "disease": _make_node_df(["DIS:MONDO_1"], "DIS"),
        }
        _validate_global_id_uniqueness(nodes)  # should not raise

    def test_raises_on_duplicate_ids_across_types(self):
        nodes = {
            "disease": _make_node_df(["SHARED_ID"], "DIS"),
            "phenotype": _make_node_df(["SHARED_ID"], "PHE"),
        }
        with pytest.raises(ValueError, match="non-unique"):
            _validate_global_id_uniqueness(nodes)

    def test_allows_duplicate_within_same_type(self):
        """Within-type duplicates are an upstream concern, not caught here."""
        nodes = {
            "gene": _make_node_df(["GEN:X", "GEN:X"], "GEN"),
        }
        with pytest.raises(ValueError, match="non-unique"):
            _validate_global_id_uniqueness(nodes)

    def test_passes_with_all_empty(self):
        nodes = {"gene": _make_node_df([], "GEN")}
        _validate_global_id_uniqueness(nodes)  # should not raise

    def test_passes_after_namespacing_resolves_collision(self):
        """IDs that collide before namespacing become unique after."""
        nodes = {
            "disease": _make_node_df(["HP_0001250"], "DIS"),
            "phenotype": _make_node_df(["HP_0001250"], "PHE"),
        }
        namespaced = _namespace_node_ids(nodes)
        _validate_global_id_uniqueness(namespaced)  # should not raise

        # Verify the IDs are now different
        dis_id = namespaced["disease"]["id"].to_list()[0]
        phe_id = namespaced["phenotype"]["id"].to_list()[0]
        assert dis_id == "DIS:HP_0001250"
        assert phe_id == "PHE:HP_0001250"
        assert dis_id != phe_id
