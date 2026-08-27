"""Node-level tests for the drug-phenotype edge builder.

This node merges three sources (OnSIDES adverse reactions, OpenTargets
indications, DrugCentral indication/contraindication/off-label statements).
Previously it concatenated them and de-duplicated on (from, to), which dropped
whole rows -- losing both the relation and its provenance, non-deterministically.
"""

import polars as pl

from optimuskg.pipelines.silver.nodes.edges.drug_phenotype import run

_INDICATION_SCHEMA = pl.Struct(
    {
        "indications": pl.List(
            pl.Struct(
                {
                    "disease": pl.String,
                    "max_phase_for_indication": pl.Float64,
                    "references": pl.List(
                        pl.Struct({"ids": pl.List(pl.String), "source": pl.String})
                    ),
                }
            )
        )
    }
)


def _onsides(rows: list[tuple[str, str]]) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "ingredient_id": [a for a, _ in rows],
            "effect_meddra_id": [b for _, b in rows],
        },
        schema={"ingredient_id": pl.String, "effect_meddra_id": pl.String},
    )


def _opentargets(rows: list[tuple[str, str]]) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "id": [a for a, _ in rows],
            "metadata": [
                {
                    "indications": [
                        {
                            "disease": b,
                            "max_phase_for_indication": 3.0,
                            "references": [{"ids": ["R1"], "source": "ClinicalTrials"}],
                        }
                    ]
                }
                for _, b in rows
            ],
        },
        schema={"id": pl.String, "metadata": _INDICATION_SCHEMA},
    )


def _drugcentral(rows: list[tuple[str, str, str]]) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "from": [r[0] for r in rows],
            "to": [r[1] for r in rows],
            "relationship_name": [r[2] for r in rows],
            "structure_id": ["S1"] * len(rows),
            "drug_disease_id": ["DD1"] * len(rows),
        },
        schema={
            "from": pl.String,
            "to": pl.String,
            "relationship_name": pl.String,
            "structure_id": pl.String,
            "drug_disease_id": pl.String,
        },
    )


def _assertions(row: dict) -> set[tuple[str, str]]:
    return {
        (a["source"], a["relation"]) for a in row["properties"]["relation_assertions"]
    }


class TestDrugPhenotypeProvenance:
    """All three sources must survive the collapse to one edge per pair."""

    def test_all_three_sources_on_one_pair(self):
        out = run(
            _onsides([("CHEMBL1", "HP_1")]),
            _opentargets([("CHEMBL1", "HP_1")]),
            _drugcentral([("CHEMBL1", "HP_1", "contraindication")]),
        )
        assert out.height == 1
        row = out.row(0, named=True)
        assert _assertions(row) == {
            ("ONSIDES", "ADVERSE_DRUG_REACTION"),
            ("OPEN_TARGETS", "ASSOCIATED_WITH"),
            ("DRUG_CENTRAL", "CONTRAINDICATION"),
        }
        # All three have priority 1, so the alphabetically first wins.
        assert row["relation"] == "ADVERSE_DRUG_REACTION"

    def test_contraindication_is_not_dropped(self):
        # The reviewer's case, on the phenotype side.
        out = run(
            _onsides([]),
            _opentargets([("CHEMBL1", "HP_1")]),
            _drugcentral([("CHEMBL1", "HP_1", "contraindication")]),
        )
        assert out.height == 1
        assert _assertions(out.row(0, named=True)) == {
            ("OPEN_TARGETS", "ASSOCIATED_WITH"),
            ("DRUG_CENTRAL", "CONTRAINDICATION"),
        }

    def test_indication_and_contraindication_conflict_is_flagged(self):
        out = run(
            _onsides([]),
            _opentargets([]),
            _drugcentral(
                [
                    ("CHEMBL1", "HP_1", "indication"),
                    ("CHEMBL1", "HP_1", "contraindication"),
                ]
            ),
        )
        assert out.height == 1
        row = out.row(0, named=True)
        assert row["properties"]["relation_conflict"] is True
        assert _assertions(row) == {
            ("DRUG_CENTRAL", "INDICATION"),
            ("DRUG_CENTRAL", "CONTRAINDICATION"),
        }

    def test_properties_are_merged_not_dropped(self):
        out = run(
            _onsides([("CHEMBL1", "HP_1")]),
            _opentargets([("CHEMBL1", "HP_1")]),
            _drugcentral([("CHEMBL1", "HP_1", "indication")]),
        )
        props = out.row(0, named=True)["properties"]
        # Provenance from every contributing source is retained.
        assert set(props["sources"]["direct"]) == {
            "ONSIDES",
            "OPEN_TARGETS",
            "DRUG_CENTRAL",
        }
        # Source-specific fields survive the merge.
        assert props["reference_ids"] == ["R1"]
        assert props["highest_clinical_trial_phase"] == 3.0
        assert props["structure_id"] == "S1"
        assert props["drug_disease_id"] == "DD1"

    def test_one_edge_per_node_pair(self):
        out = run(
            _onsides([("CHEMBL1", "HP_1"), ("CHEMBL2", "HP_2")]),
            _opentargets([("CHEMBL1", "HP_1"), ("CHEMBL3", "HP_3")]),
            _drugcentral([("CHEMBL1", "HP_1", "indication")]),
        )
        assert out.height == 3
        assert out.select(pl.struct("from", "to").n_unique()).item() == 3

    def test_deterministic_across_input_orderings(self):
        dc = _drugcentral(
            [
                ("CHEMBL1", "HP_1", "indication"),
                ("CHEMBL1", "HP_1", "contraindication"),
            ]
        )
        a = run(_onsides([("CHEMBL1", "HP_1")]), _opentargets([]), dc)
        b = run(
            _onsides([("CHEMBL1", "HP_1")]),
            _opentargets([]),
            dc.reverse(),
        )
        assert a.equals(b)

    def test_disjoint_pairs_keep_their_own_relation(self):
        out = run(
            _onsides([("CHEMBL1", "HP_1")]),
            _opentargets([("CHEMBL2", "HP_2")]),
            _drugcentral([("CHEMBL3", "HP_3", "off-label use")]),
        ).sort("from")
        assert out["relation"].to_list() == [
            "ADVERSE_DRUG_REACTION",
            "ASSOCIATED_WITH",
            "OFF_LABEL_USE",
        ]
        assert out["properties"].struct.field("relation_conflict").to_list() == [
            False,
            False,
            False,
        ]
