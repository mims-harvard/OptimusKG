import polars as pl
from kedro.pipeline import node
from goatools.obo_parser import OBOReader

def process_phenotypes(
    primekg_nodes_df: pl.DataFrame,
    human_phenotype_ontology: pl.DataFrame,
) -> pl.DataFrame:
    
    
    pheno_df = primekg_nodes_df.filter(
        pl.col("node_type") == "effect/phenotype"
    ).clone()
    pheno_df = pheno_df.with_columns(
        [pl.format("HP_{}", pl.col("node_id").str.zfill(7)).alias("id")]
    )

    # Reorder columns with id first
    all_cols = ["id"] + [col for col in pheno_df.columns if col != "id"]
    df = pheno_df.select(all_cols)
    df = df.drop(["node_index", "node_id", "node_type", "node_source"])
    df = df.sort(by=sorted(df.columns))
    return df


phenotypes_node = node(
    process_phenotypes,
    inputs={
        "primekg_nodes_df": "landing.opentargets.primekg_nodes",
        "human_phenotype_ontology": "landing.ontology.human_phenotype",
    },
    outputs="opentargets.phenotypes",
    name="phenotypes",
    tags=["bronze"],
)
