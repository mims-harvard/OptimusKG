"""Tests for provenance-preserving relation resolution.

These cover the invariant raised in peer review: collapsing to one edge per node
pair must not discard the source-specific relation assertions.
"""

import polars as pl
import pytest

from optimuskg.pipelines.silver.nodes.constants import (
    MUTUALLY_EXCLUSIVE_RELATIONS,
    RELATION_ASSERTIONS_DTYPE,
    RELATION_PRIORITY,
    Relation,
    Source,
    merge_relation_assertions,
    relation_assertions,
    relation_conflict_expr,
    resolve_relation,
    resolve_relation_expr,
)


def _assert_frame(rows: list[list[tuple[str, str]]]) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "assertions": [
                [{"source": s, "relation": r} for s, r in row] for row in rows
            ]
        },
        schema={"assertions": RELATION_ASSERTIONS_DTYPE},
    )


def _resolve(rows: list[list[tuple[str, str]]]) -> list[str]:
    return (
        _assert_frame(rows)
        .select(resolve_relation_expr(pl.col("assertions")).alias("relation"))[
            "relation"
        ]
        .to_list()
    )


def _conflict(rows: list[list[tuple[str, str]]]) -> list[bool]:
    return (
        _assert_frame(rows)
        .select(relation_conflict_expr(pl.col("assertions")).alias("conflict"))[
            "conflict"
        ]
        .to_list()
    )


class TestRelationAssertions:
    """Building source-tagged assertions from raw relation columns."""

    def test_from_scalar_string_column(self):
        df = pl.DataFrame(
            {"relation": [Relation.INDICATION, Relation.CONTRAINDICATION]}
        )
        out = df.select(
            relation_assertions(Source.DRUG_CENTRAL, pl.col("relation")).alias("a")
        )
        assert out.schema["a"] == RELATION_ASSERTIONS_DTYPE
        assert out["a"].to_list() == [
            [{"source": "DRUG_CENTRAL", "relation": "INDICATION"}],
            [{"source": "DRUG_CENTRAL", "relation": "CONTRAINDICATION"}],
        ]

    def test_from_list_column(self):
        df = pl.DataFrame(
            {"relation": [[Relation.TARGET, Relation.ENZYME]]},
            schema={"relation": pl.List(pl.String)},
        )
        out = df.select(
            relation_assertions(Source.DRUG_BANK, pl.col("relation")).alias("a")
        )
        assert out["a"].to_list() == [
            [
                {"source": "DRUG_BANK", "relation": "TARGET"},
                {"source": "DRUG_BANK", "relation": "ENZYME"},
            ]
        ]

    def test_nulls_become_empty_lists(self):
        df = pl.DataFrame(
            {"relation": [None, None]}, schema={"relation": pl.List(pl.String)}
        )
        out = df.select(
            relation_assertions(Source.OPEN_TARGETS, pl.col("relation")).alias("a")
        )
        assert out["a"].to_list() == [[], []]

    def test_null_elements_are_dropped(self):
        df = pl.DataFrame(
            {"relation": [[Relation.INDICATION, None]]},
            schema={"relation": pl.List(pl.String)},
        )
        out = df.select(
            relation_assertions(Source.OPEN_TARGETS, pl.col("relation")).alias("a")
        )
        assert out["a"].to_list() == [
            [{"source": "OPEN_TARGETS", "relation": "INDICATION"}]
        ]


class TestMergeRelationAssertions:
    """Merging assertion lists across sources, including outer-join nulls."""

    def test_merge_preserves_both_sources(self):
        df = pl.DataFrame(
            {
                "ot": [[{"source": "OPEN_TARGETS", "relation": "INDICATION"}]],
                "dc": [[{"source": "DRUG_CENTRAL", "relation": "CONTRAINDICATION"}]],
            },
            schema={"ot": RELATION_ASSERTIONS_DTYPE, "dc": RELATION_ASSERTIONS_DTYPE},
        )
        merged = df.select(
            merge_relation_assertions(pl.col("ot"), pl.col("dc")).alias("m")
        )["m"].to_list()
        assert merged == [
            [
                {"source": "DRUG_CENTRAL", "relation": "CONTRAINDICATION"},
                {"source": "OPEN_TARGETS", "relation": "INDICATION"},
            ]
        ]

    def test_merge_handles_null_side(self):
        df = pl.DataFrame(
            {
                "ot": [[{"source": "OPEN_TARGETS", "relation": "INDICATION"}]],
                "dc": [None],
            },
            schema={"ot": RELATION_ASSERTIONS_DTYPE, "dc": RELATION_ASSERTIONS_DTYPE},
        )
        merged = df.select(
            merge_relation_assertions(pl.col("ot"), pl.col("dc")).alias("m")
        )["m"].to_list()
        assert merged == [[{"source": "OPEN_TARGETS", "relation": "INDICATION"}]]

    def test_merge_deduplicates_identical_assertions(self):
        a = [{"source": "OPEN_TARGETS", "relation": "INDICATION"}]
        df = pl.DataFrame(
            {"x": [a], "y": [a]},
            schema={"x": RELATION_ASSERTIONS_DTYPE, "y": RELATION_ASSERTIONS_DTYPE},
        )
        merged = df.select(
            merge_relation_assertions(pl.col("x"), pl.col("y")).alias("m")
        )["m"].to_list()
        assert merged == [a]

    def test_same_relation_from_two_sources_is_kept_twice(self):
        df = pl.DataFrame(
            {
                "x": [[{"source": "OPEN_TARGETS", "relation": "INDICATION"}]],
                "y": [[{"source": "DRUG_CENTRAL", "relation": "INDICATION"}]],
            },
            schema={"x": RELATION_ASSERTIONS_DTYPE, "y": RELATION_ASSERTIONS_DTYPE},
        )
        merged = df.select(
            merge_relation_assertions(pl.col("x"), pl.col("y")).alias("m")
        )["m"].to_list()
        assert len(merged[0]) == 2


class TestResolveRelationExpr:
    """The collapsed ``relation`` value must stay deterministic and unchanged."""

    def test_indication_beats_contraindication(self):
        assert _resolve(
            [
                [
                    ("DRUG_CENTRAL", Relation.CONTRAINDICATION),
                    ("OPEN_TARGETS", Relation.INDICATION),
                ]
            ]
        ) == [Relation.INDICATION]

    def test_single_assertion_passthrough(self):
        assert _resolve([[("DRUG_CENTRAL", Relation.CONTRAINDICATION)]]) == [
            Relation.CONTRAINDICATION
        ]

    def test_empty_assertions_fall_back_to_other(self):
        assert _resolve([[]]) == [Relation.OTHER]

    def test_ties_broken_alphabetically(self):
        # AGONIST and ANTAGONIST both have priority 1.
        assert _resolve(
            [
                [
                    ("OPEN_TARGETS", Relation.ANTAGONIST),
                    ("OPEN_TARGETS", Relation.AGONIST),
                ]
            ]
        ) == [Relation.AGONIST]

    def test_action_beats_role(self):
        assert _resolve(
            [[("DRUG_BANK", Relation.TARGET), ("OPEN_TARGETS", Relation.INHIBITOR)]]
        ) == [Relation.INHIBITOR]

    def test_unknown_relation_treated_as_other(self):
        assert _resolve([[("DRUG_BANK", "NOT_A_RELATION")]]) == [Relation.OTHER]

    def test_order_independence(self):
        forward = _resolve(
            [
                [
                    ("A", Relation.OFF_LABEL_USE),
                    ("B", Relation.CONTRAINDICATION),
                    ("C", Relation.INDICATION),
                ]
            ]
        )
        reverse = _resolve(
            [
                [
                    ("C", Relation.INDICATION),
                    ("B", Relation.CONTRAINDICATION),
                    ("A", Relation.OFF_LABEL_USE),
                ]
            ]
        )
        assert forward == reverse == [Relation.INDICATION]


class TestParityWithLegacyResolver:
    """The expression must reproduce the previous ``resolve_relation`` semantics."""

    @pytest.mark.parametrize(
        "relations",
        [
            [Relation.INDICATION, Relation.CONTRAINDICATION],
            [Relation.CONTRAINDICATION, Relation.OFF_LABEL_USE],
            [Relation.TARGET, Relation.ENZYME, Relation.INHIBITOR],
            [Relation.PARENT, Relation.SYNERGISTIC_INTERACTION],
            [Relation.PHENOTYPE_PRESENT, Relation.PHENOTYPE_ABSENT],
            [Relation.OTHER, Relation.CARRIER],
            [Relation.AGONIST],
        ],
    )
    def test_parity(self, relations):
        legacy = resolve_relation(pl.Series(relations))
        new = _resolve([[("SRC", r) for r in relations]])[0]
        assert new == legacy

    def test_parity_over_all_relation_pairs(self):
        for a in Relation:
            for b in Relation:
                legacy = resolve_relation(pl.Series([a, b]))
                new = _resolve([[("X", a), ("Y", b)]])[0]
                assert new == legacy, f"mismatch for {a} + {b}"

    def test_every_relation_has_an_explicit_priority(self):
        missing = [r for r in Relation if r not in RELATION_PRIORITY]
        assert missing == []


class TestRelationConflict:
    """Conflicting biological assertions must be flagged, not silently dropped."""

    def test_indication_and_contraindication_conflict(self):
        assert _conflict(
            [
                [
                    ("OPEN_TARGETS", Relation.INDICATION),
                    ("DRUG_CENTRAL", Relation.CONTRAINDICATION),
                ]
            ]
        ) == [True]

    def test_agreeing_sources_do_not_conflict(self):
        assert _conflict(
            [
                [
                    ("OPEN_TARGETS", Relation.INDICATION),
                    ("DRUG_CENTRAL", Relation.INDICATION),
                ]
            ]
        ) == [False]

    def test_phenotype_present_absent_conflict(self):
        assert _conflict(
            [
                [
                    ("HPO", Relation.PHENOTYPE_PRESENT),
                    ("HPO", Relation.PHENOTYPE_ABSENT),
                ]
            ]
        ) == [True]

    def test_agonist_antagonist_conflict(self):
        assert _conflict([[("OT", Relation.AGONIST), ("DB", Relation.ANTAGONIST)]]) == [
            True
        ]

    def test_unrelated_relations_do_not_conflict(self):
        assert _conflict([[("DB", Relation.TARGET), ("OT", Relation.INHIBITOR)]]) == [
            False
        ]

    def test_empty_and_single_never_conflict(self):
        assert _conflict([[], [("DB", Relation.INDICATION)]]) == [False, False]

    def test_mutually_exclusive_groups_are_disjoint(self):
        seen: set[Relation] = set()
        for group in MUTUALLY_EXCLUSIVE_RELATIONS:
            assert not (group & seen), f"{group} overlaps a previous group"
            seen |= group


class TestNoInformationLoss:
    """End-to-end property: nothing asserted upstream disappears downstream."""

    def test_all_input_relations_survive_the_collapse(self):
        left = pl.DataFrame(
            {
                "from": ["D1", "D2"],
                "to": ["X1", "X2"],
                "relation": [Relation.INDICATION, Relation.INDICATION],
            }
        ).select(
            "from",
            "to",
            relation_assertions(Source.OPEN_TARGETS, pl.col("relation")).alias("a"),
        )
        right = pl.DataFrame(
            {
                "from": ["D1", "D3"],
                "to": ["X1", "X3"],
                "relation": [Relation.CONTRAINDICATION, Relation.OFF_LABEL_USE],
            }
        ).select(
            "from",
            "to",
            relation_assertions(Source.DRUG_CENTRAL, pl.col("relation")).alias("a"),
        )

        merged = (
            left.join(right, on=["from", "to"], how="full")
            .select(
                pl.coalesce(["from", "from_right"]).alias("from"),
                pl.coalesce(["to", "to_right"]).alias("to"),
                merge_relation_assertions(pl.col("a"), pl.col("a_right")).alias(
                    "relation_assertions"
                ),
            )
            .with_columns(
                resolve_relation_expr(pl.col("relation_assertions")).alias("relation"),
                relation_conflict_expr(pl.col("relation_assertions")).alias(
                    "relation_conflict"
                ),
            )
            .sort("from")
        )

        # One edge per node pair is preserved.
        assert merged.height == 3
        assert merged.select(pl.struct("from", "to").n_unique()).item() == 3

        # Every upstream assertion is still recoverable.
        flat = {
            (row["from"], row["to"], a["source"], a["relation"])
            for row in merged.iter_rows(named=True)
            for a in row["relation_assertions"]
        }
        assert flat == {
            ("D1", "X1", "OPEN_TARGETS", "INDICATION"),
            ("D1", "X1", "DRUG_CENTRAL", "CONTRAINDICATION"),
            ("D2", "X2", "OPEN_TARGETS", "INDICATION"),
            ("D3", "X3", "DRUG_CENTRAL", "OFF_LABEL_USE"),
        }

        # The conflicting pair is flagged; the agreeing ones are not.
        by_pair = dict(
            zip(merged["from"].to_list(), merged["relation_conflict"].to_list())
        )
        assert by_pair == {"D1": True, "D2": False, "D3": False}


class TestChecksDetectRegressions:
    """Negative controls: the guarantees must fail when actually violated.

    A check that cannot fail proves nothing. These assert that the two
    properties the pipeline relies on are genuinely sensitive to the bugs they
    are meant to catch.
    """

    def test_dropping_assertions_changes_the_resolved_relation(self):
        # The old lossy collapse kept only one assertion per pair. If that
        # regressed, the surviving relation would no longer be the priority
        # winner for pairs where sources disagree.
        full = [
            ("DRUG_CENTRAL", Relation.CONTRAINDICATION),
            ("OPEN_TARGETS", Relation.INDICATION),
        ]
        assert _resolve([full]) == [Relation.INDICATION]
        # Truncating to the first assertion yields a different answer.
        assert _resolve([full[:1]]) == [Relation.CONTRAINDICATION]

    def test_dropping_assertions_hides_a_conflict(self):
        full = [
            ("DRUG_CENTRAL", Relation.CONTRAINDICATION),
            ("OPEN_TARGETS", Relation.INDICATION),
        ]
        assert _conflict([full]) == [True]
        assert _conflict([full[:1]]) == [False]

    def test_conflict_detection_is_not_vacuous(self):
        # At least one real relation pair must be flagged, otherwise the
        # mutually exclusive groups could be empty and every check would pass.
        assert _conflict(
            [[("A", Relation.PHENOTYPE_PRESENT), ("B", Relation.PHENOTYPE_ABSENT)]]
        ) == [True]
