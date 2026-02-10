import logging

import polars as pl
from kedro.pipeline import node

from optimuskg.pipelines.silver.nodes.constants import (
    Edge,
    Node,
    Relation,
    Source,
    resolve_relation,
    resolve_sources,
)

logger = logging.getLogger(__name__)

# DrugCentral relationship_name values
_RELATION_MAP: dict[str, Relation] = {
    "indication": Relation.INDICATION,
    "off-label use": Relation.OFF_LABEL_USE,
    "contraindication": Relation.CONTRAINDICATION,
}


def run(
    drug_indication: pl.DataFrame,
    drug_disease: pl.DataFrame,
) -> pl.DataFrame:
    opentargets_drug_disease = (
        drug_indication.with_columns(
            pl.col("id"),
            pl.col("metadata").struct.field("indications"),
        )
        .explode("indications")
        .unnest("indications")
        .explode("references")
        .unnest("references")
        .filter(~pl.col("disease").str.contains("HP"))
        .select(
            pl.col("id").alias("from"),
            pl.col("disease").alias("to"),
            pl.lit(Edge.format_label(Node.DRUG, Node.DISEASE)).alias("label"),
            pl.lit([Relation.INDICATION]).alias(
                "relation"
            ),  # TODO: replace this with strong/weak clinical evidence derived from highest_clinical_trial_phase
            pl.lit(True).alias("undirected"),
            pl.struct(
                [
                    pl.col("ids").alias("reference_ids"),
                    pl.struct(
                        [
                            pl.lit([Source.OPEN_TARGETS])
                            .cast(pl.List(pl.String))
                            .alias("direct"),
                            pl.concat_list([pl.col("source")])
                            .map_elements(
                                resolve_sources, return_dtype=pl.List(pl.String)
                            )
                            .alias("indirect"),
                        ]
                    ).alias("sources"),
                    pl.col("max_phase_for_indication").alias(
                        "highest_clinical_trial_phase"
                    ),
                ]
            ).alias("opentargets_props"),
        )
        .unique(subset=["from", "to"])
        .sort(by=["from", "to"])
    )

    drugcentral_drug_disease = (
        drug_disease.select(
            pl.col("from"),
            pl.col("to"),
            pl.lit(Edge.format_label(Node.DRUG, Node.DISEASE)).alias("label"),
            pl.col("relationship_name")
            .replace_strict(_RELATION_MAP, default=Relation.OTHER)
            .cast(pl.List(pl.String))
            .alias("relation"),
            pl.lit(True).alias("undirected"),
            pl.struct(
                [
                    pl.struct(
                        [
                            pl.lit([Source.DRUG_CENTRAL])
                            .cast(pl.List(pl.String))
                            .alias("direct"),
                            pl.lit([]).cast(pl.List(pl.String)).alias("indirect"),
                        ]
                    ).alias("sources"),
                    pl.col("structure_id").alias("structure_id"),
                    pl.col("drug_disease_id").alias("drug_disease_id"),
                ]
            ).alias("drugcentral_props"),
        )
        .unique(subset=["from", "to"])
        .sort(by=["from", "to"])
    )

    return (
        opentargets_drug_disease.join(
            drugcentral_drug_disease, on=["from", "to"], how="full"
        )
        .select(
            [
                pl.coalesce([pl.col("from"), pl.col("from_right")]).alias("from"),
                pl.coalesce([pl.col("to"), pl.col("to_right")]).alias("to"),
                pl.coalesce([pl.col("label"), pl.col("label_right")]).alias("label"),
                pl.concat_list(
                    [
                        pl.coalesce(
                            [
                                pl.col("relation"),
                                pl.lit([], dtype=pl.List(pl.String)),
                            ]
                        ),
                        pl.coalesce(
                            [
                                pl.col("relation_right"),
                                pl.lit([], dtype=pl.List(pl.String)),
                            ]
                        ),
                    ]
                )
                .map_elements(resolve_relation, return_dtype=pl.String)
                .alias("relation"),
                pl.coalesce([pl.col("undirected"), pl.col("undirected_right")]).alias(
                    "undirected"
                ),
                pl.struct(
                    [
                        pl.struct(
                            [
                                pl.concat_list(
                                    [
                                        pl.coalesce(
                                            [
                                                pl.col("drugcentral_props")
                                                .struct.field("sources")
                                                .struct.field("direct"),
                                                pl.lit([], dtype=pl.List(pl.String)),
                                            ]
                                        ),
                                        pl.coalesce(
                                            [
                                                pl.col("opentargets_props")
                                                .struct.field("sources")
                                                .struct.field("direct"),
                                                pl.lit([], dtype=pl.List(pl.String)),
                                            ]
                                        ),
                                    ]
                                ).alias("direct"),
                                pl.concat_list(
                                    [
                                        pl.coalesce(
                                            [
                                                pl.col("drugcentral_props")
                                                .struct.field("sources")
                                                .struct.field("indirect"),
                                                pl.lit([], dtype=pl.List(pl.String)),
                                            ]
                                        ),
                                        pl.coalesce(
                                            [
                                                pl.col("opentargets_props")
                                                .struct.field("sources")
                                                .struct.field("indirect"),
                                                pl.lit([], dtype=pl.List(pl.String)),
                                            ]
                                        ),
                                    ]
                                ).alias("indirect"),
                            ]
                        ).alias("sources"),
                        pl.col("drugcentral_props").struct.field("structure_id"),
                        pl.col("drugcentral_props").struct.field("drug_disease_id"),
                        pl.col("opentargets_props").struct.field("reference_ids"),
                        pl.col("opentargets_props").struct.field(
                            "highest_clinical_trial_phase"
                        ),
                    ]
                ).alias("properties"),
            ]
        )
        .unique(subset=["from", "to"])
        .sort(by=["from", "to"])
    )


drug_disease_node = node(
    run,
    inputs={
        "drug_indication": "bronze.opentargets.drug_indication",
        "drug_disease": "bronze.drugcentral.drug_disease",
    },
    outputs="edges.drug_disease",
    name="drug_disease",
    tags=["silver"],
)
