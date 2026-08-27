"""Node-level tests for the drug–disease edge builder.

The reviewer's concern is concretely about this node: an indication and a
contraindication between the same drug and disease are distinct statements.
"""

import polars as pl

from optimuskg.pipelines.silver.nodes.edges.drug_disease import run

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


def _opentargets(rows: list[tuple[str, str]]) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "id": [drug for drug, _ in rows],
            "metadata": [
                {
                    "indications": [
                        {
                            "disease": disease,
                            "max_phase_for_indication": 4.0,
                            "references": [
                                {"ids": ["REF1"], "source": "ClinicalTrials"}
                            ],
                        }
                    ]
                }
                for _, disease in rows
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


class TestDrugDiseaseRelationProvenance:
    """One edge per node pair, with every source assertion still present."""

    def test_conflicting_indication_and_contraindication(self):
        out = run(
            _opentargets([("CHEMBL1", "MONDO_1")]),
            _drugcentral([("CHEMBL1", "MONDO_1", "contraindication")]),
        )

        assert out.height == 1
        row = out.row(0, named=True)
        props = row["properties"]

        # Deterministic collapsed value is unchanged from the previous design.
        assert row["relation"] == "INDICATION"

        # But the contraindication is no longer lost.
        assert sorted(
            (a["source"], a["relation"]) for a in props["relation_assertions"]
        ) == [
            ("DRUG_CENTRAL", "CONTRAINDICATION"),
            ("OPEN_TARGETS", "INDICATION"),
        ]
        assert props["relation_conflict"] is True

    def test_agreeing_sources_are_not_flagged(self):
        out = run(
            _opentargets([("CHEMBL1", "MONDO_1")]),
            _drugcentral([("CHEMBL1", "MONDO_1", "indication")]),
        )
        props = out.row(0, named=True)["properties"]
        assert props["relation_conflict"] is False
        assert len(props["relation_assertions"]) == 2

    def test_single_source_edges_keep_their_assertion(self):
        out = run(
            _opentargets([("CHEMBL1", "MONDO_1")]),
            _drugcentral([("CHEMBL2", "MONDO_2", "off-label use")]),
        ).sort("from")

        assert out.height == 2
        first, second = (out.row(i, named=True) for i in range(2))

        assert first["relation"] == "INDICATION"
        assert first["properties"]["relation_assertions"] == [
            {"source": "OPEN_TARGETS", "relation": "INDICATION"}
        ]
        assert first["properties"]["relation_conflict"] is False

        assert second["relation"] == "OFF_LABEL_USE"
        assert second["properties"]["relation_assertions"] == [
            {"source": "DRUG_CENTRAL", "relation": "OFF_LABEL_USE"}
        ]
        assert second["properties"]["relation_conflict"] is False

    def test_one_edge_per_node_pair_invariant_holds(self):
        out = run(
            _opentargets([("CHEMBL1", "MONDO_1"), ("CHEMBL2", "MONDO_2")]),
            _drugcentral(
                [
                    ("CHEMBL1", "MONDO_1", "contraindication"),
                    ("CHEMBL2", "MONDO_2", "off-label use"),
                    ("CHEMBL3", "MONDO_3", "indication"),
                ]
            ),
        )
        assert out.height == 3
        assert out.select(pl.struct("from", "to").n_unique()).item() == out.height

    def test_output_schema_is_stable(self):
        out = run(
            _opentargets([("CHEMBL1", "MONDO_1")]),
            _drugcentral([("CHEMBL1", "MONDO_1", "contraindication")]),
        )
        assert out.columns == [
            "from",
            "to",
            "label",
            "relation",
            "undirected",
            "properties",
        ]
        props = dict(out.schema["properties"])
        assert props["relation_assertions"] == pl.List(
            pl.Struct({"source": pl.String, "relation": pl.String})
        )
        assert props["relation_conflict"] == pl.Boolean
