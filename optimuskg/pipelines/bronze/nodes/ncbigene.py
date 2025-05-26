import logging

import polars as pl
from goatools.anno.genetogo_reader import Gene2GoReader
from kedro.pipeline import node

logger = logging.getLogger(__name__)


def process_gene2go(
    gene2go: Gene2GoReader,
) -> pl.DataFrame:
    ns2assc_has1 = gene2go.get_ns2assc()

    # Collect associations into a list of (gene, go_id, type) where go_term_type is one of: 'molecular_function', 'biological_process', 'cellular_component'
    namespace_mapping = {
        "MF": "molecular_function",
        "BP": "biological_process",
        "CC": "cellular_component",
    }

    associations = []
    for ns, go_type in namespace_mapping.items():
        for gene, goterms in ns2assc_has1[ns].items():
            for go_id in goterms:
                associations.append((gene, go_id, go_type))

    df = pl.DataFrame(
        associations,
        schema=["ncbi_gene_id", "go_term_id", "go_term_type"],
        orient="row",
    )

    # Replace 'GO:#######' with 'GO_#######'
    df = df.with_columns(
        pl.col("go_term_id")
        .map_elements(lambda x: x.replace("GO:", "GO_"), return_dtype=pl.Utf8)
        .alias("go_term_id")
    )

    # Add "NCBIGene:" prefix to ncbi_gene_id column to match biolink schema
    df = df.with_columns(
        pl.col("ncbi_gene_id")
        .map_elements(lambda x: f"NCBIGene:{x}")
        .alias("ncbi_gene_id")
    )

    df = df.sort(by=["ncbi_gene_id", "go_term_id"])

    return df


gene2go_node = node(
    process_gene2go,
    inputs={"gene2go": "landing.ncbigene.gene2go"},
    outputs="ncbigene.protein_go_associations",
    name="gene2go",
    tags=["bronze"],
)
