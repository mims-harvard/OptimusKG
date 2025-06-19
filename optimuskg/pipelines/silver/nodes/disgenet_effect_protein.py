import polars as pl
from kedro.pipeline import node


def process_disgenet_effect_protein(
    curated_gene_disease_associations: pl.DataFrame,
    phenotypes: pl.DataFrame,
) -> pl.DataFrame:
    df_prot_phe = curated_gene_disease_associations.query('diseaseType=="phenotype"')

    #NOTE: Esto esta roto. Hay que reemplazar esas dos lineas por solo una y ver como hacer el join.
    df_prot_phe = df_prot_phe.join(phenotypes, left_on='diseaseId', right_on='ontology_id', how='inner')
    df_prot_phe = df_prot_phe.join(phenotypes, left_on='hp_id', right_on='id', how='left')

    df_prot_phe = df_prot_phe.rename(columns={'geneId':'x_id', 'geneSymbol':'x_name', 'hp_id':'y_id', 'name':'y_name'})
    df_prot_phe['x_type'] = 'gene/protein'
    df_prot_phe['x_source'] = 'NCBI'
    df_prot_phe['y_type'] = 'effect/phenotype'
    df_prot_phe['y_source'] = 'HPO'
    df_prot_phe['relation'] = 'phenotype_protein'
    df_prot_phe['display_relation'] = 'associated with'
    
    # df_prot_phe = clean_edges(df_prot_phe)
    df_prot_phe.head(1)


disgenet_effect_protein_node = node(
    process_disgenet_effect_protein,
    inputs={
        "curated_gene_disease_associations": "landing.disgenet.curated_gene_disease_associations",
        "phenotypes": "bronze.opentargets.phenotypes",
    },
    outputs="disgenet.effect_protein",
    name="disgenet_effect_protein",
    tags=["silver"],
)
