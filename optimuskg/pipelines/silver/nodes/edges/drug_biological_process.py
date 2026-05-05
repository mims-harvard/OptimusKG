import polars as pl
from kedro.pipeline import node

from optimuskg.pipelines.silver.nodes.constants import Edge, Node, Relation, Source


def run(
    drug_indication: pl.DataFrame,
) -> pl.DataFrame:
    return (
        drug_indication.with_columns(
            pl.col("id").alias("drug_id"),
            pl.col("metadata").struct.field("indications"),
        )
        .explode("indications")
        .unnest("indications")
        .explode("references")
        .unnest("references")
        .filter(pl.col("disease").str.starts_with("GO_"))
        .select(
            pl.col("drug_id").alias("from"),
            pl.col("disease").alias("to"),
            pl.lit(Edge.format_label(Node.DRUG, Node.BIOLOGICAL_PROCESS)).alias(
                "label"
            ),
            pl.lit(Relation.INDICATION).alias("relation"),
            pl.lit(True).alias("undirected"),
            pl.struct(
                [
                    pl.struct(
                        [
                            pl.lit([Source.OPEN_TARGETS])
                            .cast(pl.List(pl.String))
                            .alias("direct"),
                            pl.concat_list([pl.col("source")])
                            .cast(pl.List(pl.String))
                            .alias("indirect"),
                        ]
                    ).alias("sources"),
                    pl.col("ids").alias("reference_ids"),
                    pl.col("max_phase_for_indication").alias(
                        "highest_clinical_trial_phase"
                    ),
                ]
            ).alias("properties"),
        )
        .unique(subset=["from", "to"])
        .sort(by=["from", "to"])
    )


drug_biological_process_node = node(
    run,
    inputs={
        "drug_indication": "bronze.opentargets.drug_indication",
    },
    outputs="edges.drug_biological_process",
    name="drug_biological_process",
    tags=["silver"],
)
