import logging
from typing import Final, final

import polars as pl
from kedro.pipeline import node
from typedframe import PolarsTypedFrame

log = logging.getLogger(__name__)


@final
class GeneNames(PolarsTypedFrame):
    schema: Final = {
        "Approved symbol": pl.String,
        "Approved name": pl.String,
        "Accession numbers": pl.String,
        "RefSeq IDs": pl.String,
        "NCBI Gene ID": pl.String,
        "NCBI Gene ID(supplied by NCBI)": pl.String,
        "UniProt ID(supplied by UniProt)": pl.String,
        "OMIM ID(supplied by OMIM)": pl.String,
    }


def process_gene_names(
    gene_names: pl.DataFrame,
) -> pl.DataFrame:
    return GeneNames(gene_names).df


gene_names_node = node(
    process_gene_names,
    inputs={"gene_names": "landing.gene_names.gene_names"},
    outputs="gene_names.gene_names",
    name="gene_names",
)
