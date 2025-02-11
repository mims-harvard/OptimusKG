def process_bgee(
    homo_sapiens_expressions_advanced: pl.DataFrame,
) -> pl.DataFrame:
    return pl.DataFrame(), pl.DataFrame()


bgee_node = node(
    process_bgee,
    inputs={
        "gene_expressions_in_anatomy": "bronze.bgee.gene_expressions_in_anatomy"
    },
    outputs=["bgee.gene_expressions_in_anatomy.nodes", "bgee.gene_expressions_in_anatomy.edges"],
    name="bgee",
)
