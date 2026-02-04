import polars as pl
from kedro.pipeline import node

from optimuskg.pipelines.silver.nodes.constants import Node


def run(  # noqa: PLR0913
    pathway_pathway: pl.DataFrame,
    pathway_protein: pl.DataFrame,
    reactome_pathways: pl.DataFrame,
) -> pl.DataFrame:
    return (
        pl.concat(
            [
                pathway_protein.select(pl.col("from").alias("id")),
                pathway_pathway.select(
                    pl.concat_list(["from", "to"]).explode().alias("id")
                ),
            ]
        )
        .unique(subset="id")
        .join(
            reactome_pathways.select(
                ("REACT:" + pl.col("reactome_id")).alias("id"),
                pl.col("reactome_name"),
                pl.col("species"),
            ),
            left_on="id",
            right_on="id",
            how="left",  # TODO: there are 2 ids that are not in the reactome_pathways table
        )
        .select(
            [
                pl.col("id"),
                pl.lit(Node.PATHWAY).alias("label"),
                pl.struct(
                    [
                        pl.lit(["REACTOME", "opentargets"]).alias("sources"),
                        pl.col("reactome_name").alias("name"),
                        pl.col("species"),
                    ]
                ).alias("properties"),
            ]
        )
        .sort(by="id")
    )


pathway_node = node(
    run,
    inputs={
        "pathway_pathway": "silver.edges.pathway_pathway",
        "pathway_protein": "silver.edges.pathway_protein",
        "reactome_pathways": "landing.reactome.reactome_pathways",  # TODO: this should be pre-processed in bronze
    },
    outputs="nodes.pathway",
    name="pathway",
    tags=["silver"],
)
