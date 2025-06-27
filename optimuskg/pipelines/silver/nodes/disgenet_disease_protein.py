import polars as pl
from kedro.pipeline import node


def run(
    disgenet_diseases: pl.DataFrame,
    umls_mondo: pl.DataFrame,
    mondo_terms: pl.DataFrame,
) -> pl.DataFrame:
    # NOTE: There is a small mismatch between the umls_mondo and monto_terms here and the ones in PrimeKG.
    df_prot_dis = disgenet_diseases.join(
        umls_mondo, left_on="disease_id", right_on="umls_id", how="inner"
    )
    df_prot_dis = df_prot_dis.join(
        mondo_terms, left_on="mondo_id", right_on="id", how="left"
    )

    df_prot_dis = df_prot_dis.rename(
        {
            "gene_id": "x_id",
            "gene_symbol": "x_name",
            "mondo_id": "y_id",
            "name": "y_name",
        }
    )

    df_prot_dis = df_prot_dis.with_columns(
        [
            pl.lit("gene").alias("x_type"),
            pl.lit("NCBI").alias("x_source"),
            pl.lit("disease").alias("y_type"),
            pl.lit("MONDO").alias("y_source"),
            pl.lit("disease_protein").alias("relation"),
            pl.lit("associated with").alias("relation_type"),
        ]
    )

    df_prot_dis = df_prot_dis.select(
        [
            "relation",
            "relation_type",
            "x_id",
            "x_type",
            "x_name",
            "x_source",
            "y_id",
            "y_type",
            "y_name",
            "y_source",
        ]
    )

    return df_prot_dis


disgenet_disease_protein_node = node(
    run,
    inputs={
        "disgenet_diseases": "bronze.disgenet.disgenet_diseases",
        "umls_mondo": "silver.umls.umls_mondo",
        "mondo_terms": "bronze.ontology.mondo_terms",
    },
    outputs="disgenet.disease_protein",
    name="disgenet_disease_protein",
    tags=["silver"],
)
