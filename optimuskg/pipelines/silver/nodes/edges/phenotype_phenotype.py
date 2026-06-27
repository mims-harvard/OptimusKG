import polars as pl
from kedro.pipeline import node

from optimuskg.pipelines.silver.nodes.constants import Edge, Node, Relation, Source


def run(
    hp_relations: pl.DataFrame,
    opentargets_disease: pl.DataFrame,
) -> pl.DataFrame:
    label = Edge.format_label(Node.PHENOTYPE, Node.PHENOTYPE)

    def _properties(source: str) -> pl.Expr:
        return pl.struct(
            [
                pl.struct(
                    [
                        pl.lit([source]).cast(pl.List(pl.String)).alias("direct"),
                        pl.lit([]).cast(pl.List(pl.String)).alias("indirect"),
                    ]
                ).alias("sources"),
            ]
        ).alias("properties")

    hpo = hp_relations.select(
        pl.col("parent").alias("from"),
        pl.col("child").alias("to"),
        pl.lit(label).alias("label"),
        pl.lit(Relation.PARENT).alias("relation"),
        pl.lit(False).alias("undirected"),
        _properties(Source.HPO),
    )

    # OpenTargets contributes HP->HP parent links absent from the HPO source.
    opentargets = (
        opentargets_disease.with_columns(pl.col("metadata").struct.field("parents"))
        .explode("parents")
        .filter(
            pl.col("parents").is_not_null(),
            pl.col("id").str.starts_with("HP_"),
            pl.col("parents").str.starts_with("HP_"),
        )
        .select(
            pl.col("parents").alias("from"),
            pl.col("id").alias("to"),
            pl.lit(label).alias("label"),
            pl.lit(Relation.PARENT).alias("relation"),
            pl.lit(False).alias("undirected"),
            _properties(Source.OPEN_TARGETS),
        )
    )

    return (
        pl.concat([hpo, opentargets])
        .unique(subset=["from", "to"], keep="first")  # HPO takes precedence on overlap
        .sort(by=["from", "to"])
    )


phenotype_phenotype_node = node(
    run,
    inputs={
        "hp_relations": "bronze.ontology.hp_relations",
        "opentargets_disease": "bronze.opentargets.disease",
    },
    outputs="edges.phenotype_phenotype",
    name="phenotype_phenotype",
    tags=["silver"],
)
