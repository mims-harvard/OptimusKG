import logging
from typing import Final, final

import polars as pl
from kedro.pipeline import node
from typedframe import PolarsTypedFrame

log = logging.getLogger(__name__)


@final
class Gene2Go(PolarsTypedFrame):
    schema: Final = {
        "tax_id": pl.String,
        "gene_id": pl.String,
        "go_id": pl.String,
        "evidence": pl.String,
        "qualifier": pl.String,
        "go_term": pl.String,
        "pubmed": pl.String,
        "category": pl.String,
    }


def process_gene2go(
    gene2go: pl.DataFrame,
) -> pl.DataFrame:
    df = Gene2Go(gene2go).df
    return df


gene2go_node = node(
    process_gene2go,
    inputs={"gene2go": "landing.ncbigene.gene2go"},
    outputs="ncbigene.protein_go_associations",
    name="gene2go",
)
