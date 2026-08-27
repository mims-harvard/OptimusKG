"""Node-level tests for the remaining edge builders that collapse relations.

Each of these joins two sources into a single edge per node pair; the
``relation_assertions`` property must retain every original statement.
"""

import polars as pl

from optimuskg.pipelines.silver.nodes.edges import (
    disease_phenotype,
    drug_drug,
    drug_gene,
)


def _assertion_set(props: dict) -> set[tuple[str, str]]:
    return {(a["source"], a["relation"]) for a in props["relation_assertions"]}


class TestDrugDrug:
    """DrugBank interactions merged with the OpenTargets molecule hierarchy."""

    @staticmethod
    def _inputs(overlap: bool):
        drug_drug_df = pl.DataFrame(
            {
                "head_drug_id": ["DB1"],
                "tail_drug_id": ["DB2"],
                "description": ["Synergy with X"],
            }
        )
        mapping = pl.DataFrame(
            {"drugbank_id": ["DB1", "DB2"], "chembl_id": ["CHEMBL1", "CHEMBL2"]}
        )
        # drugbank edge is from=tail(CHEMBL2), to=head(CHEMBL1)
        child = ["CHEMBL1"] if overlap else ["CHEMBL9"]
        molecule = pl.DataFrame(
            {
                "id": ["CHEMBL2"],
                "metadata": [{"child_chembl_ids": child}],
            },
            schema={
                "id": pl.String,
                "metadata": pl.Struct({"child_chembl_ids": pl.List(pl.String)}),
            },
        )
        return drug_drug_df, molecule, mapping

    def test_overlapping_pair_keeps_both_assertions(self):
        out = drug_drug.run(*self._inputs(overlap=True))
        assert out.height == 1
        row = out.row(0, named=True)
        assert _assertion_set(row["properties"]) == {
            ("DRUG_BANK", "SYNERGISTIC_INTERACTION"),
            ("OPEN_TARGETS", "PARENT"),
        }
        # SYNERGISTIC_INTERACTION (priority 1) beats PARENT (priority 10).
        assert row["relation"] == "SYNERGISTIC_INTERACTION"
        # Not a mutually exclusive pair, so no conflict.
        assert row["properties"]["relation_conflict"] is False

    def test_disjoint_pairs_keep_their_own_assertion(self):
        out = drug_drug.run(*self._inputs(overlap=False)).sort("from", "to")
        assert out.height == 2
        by_relation = {
            r["relation"]: _assertion_set(r["properties"])
            for r in out.iter_rows(named=True)
        }
        assert by_relation == {
            "SYNERGISTIC_INTERACTION": {("DRUG_BANK", "SYNERGISTIC_INTERACTION")},
            "PARENT": {("OPEN_TARGETS", "PARENT")},
        }


class TestDrugGene:
    """DrugBank roles merged with OpenTargets pharmacological actions."""

    @staticmethod
    def _inputs(with_opentargets: bool):
        drug_gene_df = pl.DataFrame(
            {
                "drug_bank_id": ["DB1"],
                "ncbi_gene_id": ["100"],
                "relation": ["target"],
            }
        )
        mechanism = pl.DataFrame(
            {
                "targets": [["ENSG1"]] if with_opentargets else [["ENSG_OTHER"]],
                "chembl_ids": [["CHEMBL1"]],
                "mechanism_of_action": ["Inhibits the kinase"],
                "metadata": [
                    {
                        "references": [
                            {"ids": ["R1"], "urls": ["http://x"], "source": "DailyMed"}
                        ],
                        "action_type": "INHIBITOR",
                    }
                ],
            },
            schema={
                "targets": pl.List(pl.String),
                "chembl_ids": pl.List(pl.String),
                "mechanism_of_action": pl.String,
                "metadata": pl.Struct(
                    {
                        "references": pl.List(
                            pl.Struct(
                                {
                                    "ids": pl.List(pl.String),
                                    "urls": pl.List(pl.String),
                                    "source": pl.String,
                                }
                            )
                        ),
                        "action_type": pl.String,
                    }
                ),
            },
        )
        chembl_map = pl.DataFrame({"drugbank_id": ["DB1"], "chembl_id": ["CHEMBL1"]})
        ensembl_map = pl.DataFrame({"ncbi_id": ["100"], "ensembl_id": ["ENSG1"]})
        return drug_gene_df, mechanism, chembl_map, ensembl_map

    def test_role_and_action_both_preserved(self):
        out = drug_gene.run(*self._inputs(with_opentargets=True))
        assert out.height == 1
        row = out.row(0, named=True)
        assert _assertion_set(row["properties"]) == {
            ("DRUG_BANK", "TARGET"),
            ("OPEN_TARGETS", "INHIBITOR"),
        }
        # The action is more specific than the role, so it wins the collapse,
        # but the TARGET role is still recorded.
        assert row["relation"] == "INHIBITOR"

    def test_drugbank_only_edge_keeps_role_assertion(self):
        out = drug_gene.run(*self._inputs(with_opentargets=False))
        assert out.height == 1
        row = out.row(0, named=True)
        assert row["relation"] == "TARGET"
        assert _assertion_set(row["properties"]) == {("DRUG_BANK", "TARGET")}
        assert row["properties"]["relation_conflict"] is False

    def test_properties_schema_matches_across_join_branches(self):
        with_ot = drug_gene.run(*self._inputs(with_opentargets=True))
        without_ot = drug_gene.run(*self._inputs(with_opentargets=False))
        assert with_ot.schema["properties"] == without_ot.schema["properties"]


class TestDiseasePhenotype:
    """HPO present/absent annotations for the same disease-phenotype pair."""

    @staticmethod
    def _input(qualifiers: list[bool]) -> pl.DataFrame:
        evidence = [
            {
                "aspect": "P",
                "bio_curation": None,
                "evidence_type": "PCS",
                "frequency": None,
                "modifiers": [],
                "onset": [],
                "qualifier_not": q,
                "references": ["PMID:1"],
                "sex": None,
                "resource": "HPO",
            }
            for q in qualifiers
        ]
        return pl.DataFrame(
            {
                "disease": ["MONDO_1"],
                "phenotype": ["HP_1"],
                "metadata": [{"evidence": evidence}],
            },
            schema={
                "disease": pl.String,
                "phenotype": pl.String,
                "metadata": pl.Struct(
                    {
                        "evidence": pl.List(
                            pl.Struct(
                                {
                                    "aspect": pl.String,
                                    "bio_curation": pl.String,
                                    "evidence_type": pl.String,
                                    "frequency": pl.String,
                                    "modifiers": pl.List(pl.String),
                                    "onset": pl.List(pl.String),
                                    "qualifier_not": pl.Boolean,
                                    "references": pl.List(pl.String),
                                    "sex": pl.String,
                                    "resource": pl.String,
                                }
                            )
                        )
                    }
                ),
            },
        )

    def test_present_only(self):
        out = disease_phenotype.run(self._input([False]))
        row = out.row(0, named=True)
        assert row["relation"] == "PHENOTYPE_PRESENT"
        assert _assertion_set(row["properties"]) == {
            ("OPEN_TARGETS", "PHENOTYPE_PRESENT")
        }
        assert row["properties"]["relation_conflict"] is False

    def test_absent_only(self):
        out = disease_phenotype.run(self._input([True]))
        row = out.row(0, named=True)
        assert row["relation"] == "PHENOTYPE_ABSENT"
        assert _assertion_set(row["properties"]) == {
            ("OPEN_TARGETS", "PHENOTYPE_ABSENT")
        }
        assert row["properties"]["relation_conflict"] is False

    def test_one_edge_per_pair(self):
        out = disease_phenotype.run(self._input([False, True]))
        assert out.height == 1
        assert out.select(pl.struct("from", "to").n_unique()).item() == 1
