import logging

import polars as pl

logger = logging.getLogger(__name__)


def type_switch(
    type_x_or_y,
    targets_df: pl.DataFrame,
    phenotypes_df: pl.DataFrame,
    diseases_df: pl.DataFrame,
    drug_mappings_df: pl.DataFrame,
):
    type_key = {
        "gene": ("targetId", targets_df),
        "disease": ("diseaseId", diseases_df),
        "drug": ("drugId", drug_mappings_df),
        "phenotype": ("phenotypeId", phenotypes_df),
    }
    return type_key[type_x_or_y]


def construct_edges(  # noqa: PLR0913
    evidence_df: pl.DataFrame,
    targets_df: pl.DataFrame,
    phenotypes_df: pl.DataFrame,
    diseases_df: pl.DataFrame,
    drug_mappings_df: pl.DataFrame,
    type_x: str = "gene",
    type_y: str = "disease",
    relation_label: str | None = "disease_protein",
    display_relation_label: str | None = "associated with",
):
    logger.debug(
        f"Constructing {type_x}-{type_y} edges with {evidence_df.shape[0]} rows"
    )

    if (type_x == "gene" and type_y == "disease") and relation_label is not None:
        pheno_count = evidence_df.filter(pl.col("diseaseId").str.contains("HP")).height
        if pheno_count > 0:
            logger.debug(
                f"Identified {pheno_count} HPO phenotypes, analyzing separately from diseases"
            )

            # Split evidence into phenotype and disease dataframes
            evidence_pheno_df = (
                evidence_df.filter(pl.col("diseaseId").is_in(phenotypes_df["id"]))
                .select(["targetId", "diseaseId"])
                .rename({"diseaseId": "phenotypeId"})
            )

            evidence_disease_df = evidence_df.filter(
                pl.col("diseaseId").is_in(diseases_df["id"])
            ).select(["targetId", "diseaseId"])

            # Construct edges recursively
            evidence_pheno_df = construct_edges(
                evidence_df=evidence_pheno_df,
                targets_df=targets_df,
                phenotypes_df=phenotypes_df,
                diseases_df=diseases_df,
                drug_mappings_df=drug_mappings_df,
                type_x="gene",
                type_y="phenotype",
                relation_label="phenotype_protein",
                display_relation_label="associated with",
            )

            evidence_disease_df = construct_edges(
                evidence_df=evidence_disease_df,
                targets_df=targets_df,
                phenotypes_df=phenotypes_df,
                diseases_df=diseases_df,
                drug_mappings_df=drug_mappings_df,
                type_x="gene",
                type_y="disease",
                relation_label="disease_protein",
                display_relation_label="associated with",
            )

            # Combine results
            edge_df = pl.concat(
                [evidence_pheno_df, evidence_disease_df], how="diagonal"
            )

            edge_types = '", "'.join(
                edge_df.select("relation").unique().to_series().to_list()
            )
            logger.debug(f'Constructed {edge_df.height} edges of types "{edge_types}"')
            return edge_df

    # Add relation label if needed
    if relation_label is not None:
        evidence_df = evidence_df.with_columns(
            [
                pl.lit(relation_label).alias("relation"),
                pl.lit(display_relation_label).alias("relation_type"),
            ]
        )
        logger.debug(
            f"Adding edge type information: {relation_label} ({display_relation_label})"
        )
    else:
        logger.debug('Using existing edge type information in column "relation"')

    # Convert arguments to mapping tables
    x_id, x_df = type_switch(
        type_x,
        targets_df=targets_df,
        phenotypes_df=phenotypes_df,
        diseases_df=diseases_df,
        drug_mappings_df=drug_mappings_df,
    )
    y_id, y_df = type_switch(
        type_y,
        targets_df=targets_df,
        phenotypes_df=phenotypes_df,
        diseases_df=diseases_df,
        drug_mappings_df=drug_mappings_df,
    )
    logger.debug(f'Mapping {type_x} to "{x_id}" and {type_y} to "{y_id}"')

    # Construct KG edges
    edge_df = (
        evidence_df.select(
            [
                pl.col(x_id).alias("x_id"),
                pl.col(y_id).alias("y_id"),
                pl.col("relation"),
                pl.col("relation_type"),
            ]
        )
        .with_columns(
            [
                pl.lit(type_x).alias("x_type"),
                pl.lit(type_y).alias("y_type"),
            ]
        )
        .unique()
        .join(
            x_df.select(["id", "name", "source"]),
            left_on="x_id",
            right_on="id",
            how="left",
        )
        .rename({"name": "x_name", "source": "x_source"})
        .join(
            y_df.select(["id", "name", "source"]),
            left_on="y_id",
            right_on="id",
            how="left",
        )
        .rename({"name": "y_name", "source": "y_source"})
        .select(
            [
                "x_id",
                "y_id",
                "relation",
                "relation_type",
                "x_type",
                "y_type",
                "x_name",
                "y_name",
                "x_source",
                "y_source",
            ]
        )
    )

    # Log concluding message
    if relation_label is not None:
        logger.debug(f'Constructed {edge_df.height} edges of type "{relation_label}"')
    else:
        edge_types = '", "'.join(
            edge_df.select("relation").unique().to_series().to_list()
        )
        logger.debug(f'Constructed {edge_df.height} edges of types "{edge_types}"')

    return edge_df
