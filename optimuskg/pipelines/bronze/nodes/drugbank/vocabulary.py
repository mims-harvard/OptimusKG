import polars as pl
from kedro.pipeline import node


def run(  # noqa: PLR0913
    vocabulary: pl.DataFrame,
) -> pl.DataFrame:
    vocabulary = vocabulary.rename(
        {
            "DrugBank ID": "drugbank_id",
            "Accession Numbers": "accession_numbers",  # TODO: accession_numbers should be a list of strings
            "Common name": "common_name",
            "CAS": "cas",
            "UNII": "unii",
            "Synonyms": "synonyms",
            "Standard InChI Key": "standard_inchi_key",
        }
    )
    vocabulary = vocabulary.with_columns(
        pl.col("drugbank_id")
        .map_elements(lambda x: f"DRUGBANK:{x}", return_dtype=pl.Utf8)
        .alias("drugbank_id"),
    )
    return vocabulary.sort(by="drugbank_id")


vocabulary_node = node(
    run,
    inputs={"vocabulary": "landing.drugbank.vocabulary"},
    outputs="drugbank.vocabulary",
    name="vocabulary",
    tags=["bronze"],
)
