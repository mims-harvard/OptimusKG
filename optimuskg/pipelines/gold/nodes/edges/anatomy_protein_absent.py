# import polars as pl
# from kedro.pipeline import node

# from optimuskg.pipelines.gold.adapter import adapter_factory


# def process_biocypher(  # noqa: PLR0913
#     gene_expressions_in_anatomy: pl.DataFrame,
# ) -> pl.DataFrame:
#     bgee_adapter = adapter_factory(gene_expressions_in_anatomy, name="bgee")


# anatomy_protein_absent_edge = node(
#     process_biocypher,
#     inputs={
#         "gene_expressions_in_anatomy": "silver.bgee.gene_expressions_in_anatomy",
#     },
#     outputs="gold.edges.anatomy_protein_absent",
#     tags=["gold"],
#     name="anatomy_protein_absent",
# )
