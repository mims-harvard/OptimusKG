import logging

import polars as pl
from kedro.pipeline import node

logger = logging.getLogger(__name__)


def run(
    target: pl.DataFrame,
) -> pl.DataFrame:
    go_transformed = pl.col("go").list.eval(
        pl.struct(
            pl.element().struct.field("id"),
            pl.element().struct.field("source"),
            pl.element().struct.field("evidence"),
            pl.element().struct.field("aspect"),
            pl.element().struct.field("geneProduct").alias("gene_product"),
            pl.element().struct.field("ecoId").alias("eco_id"),
        )
    )

    hallmarks_transformed = pl.struct(
        pl.col("hallmarks").struct.field("attributes"),
        pl.col("hallmarks").struct.field("cancerHallmarks").alias("cancer_hallmarks"),
    )

    subcellular_locations_transformed = pl.col("subcellularLocations").list.eval(
        pl.struct(
            pl.element().struct.field("location"),
            pl.element().struct.field("source"),
            pl.element().struct.field("termSL").alias("term_sl"),
            pl.element().struct.field("labelSL").alias("label_sl"),
        )
    )

    constraint_transformed = pl.col("constraint").list.eval(
        pl.struct(
            pl.element().struct.field("constraintType").alias("constraint_type"),
            pl.element().struct.field("score"),
            pl.element().struct.field("exp"),
            pl.element().struct.field("obs"),
            pl.element().struct.field("oe"),
            pl.element().struct.field("oeLower").alias("oe_lower"),
            pl.element().struct.field("oeUpper").alias("oe_upper"),
            pl.element().struct.field("upperRank").alias("upper_rank"),
            pl.element().struct.field("upperBin").alias("upper_bin"),
            pl.element().struct.field("upperBin6").alias("upper_bin6"),
        )
    )

    tep_transformed = pl.struct(
        pl.col("tep").struct.field("targetFromSourceId").alias("target_from_source_id"),
        pl.col("tep").struct.field("description"),
        pl.col("tep").struct.field("therapeuticArea").alias("therapeutic_area"),
        pl.col("tep").struct.field("url"),
    )

    chemical_probes_transformed = pl.col("chemicalProbes").list.eval(
        pl.struct(
            pl.element().struct.field("control"),
            pl.element().struct.field("drugId").alias("drug_id"),
            pl.element().struct.field("id"),
            pl.element().struct.field("isHighQuality").alias("is_high_quality"),
            pl.element().struct.field("mechanismOfAction").alias("mechanism_of_action"),
            pl.element().struct.field("origin"),
            pl.element().struct.field("probeMinerScore").alias("probe_miner_score"),
            pl.element().struct.field("probesDrugsScore").alias("probes_drugs_score"),
            pl.element().struct.field("scoreInCells").alias("score_in_cells"),
            pl.element().struct.field("scoreInOrganisms").alias("score_in_organisms"),
            pl.element()
            .struct.field("targetFromSourceId")
            .alias("target_from_source_id"),
            pl.element()
            .struct.field("urls")
            .list.eval(
                pl.struct(
                    pl.element().struct.field("niceName").alias("nice_name"),
                    pl.element().struct.field("url"),
                )
            ),
        )
    )

    homologues_transformed = pl.col("homologues").list.eval(
        pl.struct(
            pl.element().struct.field("speciesId").alias("species_id"),
            pl.element().struct.field("speciesName").alias("species_name"),
            pl.element().struct.field("homologyType").alias("homology_type"),
            pl.element().struct.field("targetGeneId").alias("target_gene_id"),
            pl.element().struct.field("isHighConfidence").alias("is_high_confidence"),
            pl.element().struct.field("targetGeneSymbol").alias("target_gene_symbol"),
            pl.element()
            .struct.field("queryPercentageIdentity")
            .alias("query_percentage_identity"),
            pl.element()
            .struct.field("targetPercentageIdentity")
            .alias("target_percentage_identity"),
            pl.element().struct.field("priority"),
        )
    )

    safety_liabilities_transformed = pl.col("safetyLiabilities").list.eval(
        pl.struct(
            pl.element().struct.field("event"),
            pl.element().struct.field("eventId").alias("event_id"),
            pl.element().struct.field("effects"),
            pl.element()
            .struct.field("biosamples")
            .list.eval(
                pl.struct(
                    pl.element().struct.field("cellFormat").alias("cell_format"),
                    pl.element().struct.field("cellLabel").alias("cell_label"),
                    pl.element().struct.field("tissueId").alias("tissue_id"),
                    pl.element().struct.field("tissueLabel").alias("tissue_label"),
                )
            ),
            pl.element().struct.field("datasource"),
            pl.element().struct.field("literature"),
            pl.element().struct.field("url"),
            pl.element().struct.field("studies"),
        )
    )

    pathways_transformed = pl.col("pathways").list.eval(
        pl.struct(
            pl.element().struct.field("pathwayId").alias("pathway_id"),
            pl.element().struct.field("pathway"),
            pl.element().struct.field("topLevelTerm").alias("top_level_term"),
        )
    )

    return (
        target.select(
            pl.col("id"),
            pl.col("approvedSymbol").alias("approved_symbol"),
            pl.col("approvedName").alias("approved_name"),
            pl.struct(
                pl.col("biotype"),
                pl.col("transcriptIds").alias("transcript_ids"),
                pl.col("canonicalTranscript").alias("canonical_transcript"),
                pl.col("canonicalExons").alias("canonical_exons"),
                pl.col("genomicLocation").alias("genomic_location"),
                pl.col("alternativeGenes").alias("alternative_genes"),
                go_transformed.alias("go"),
                hallmarks_transformed.alias("hallmarks"),
                pl.col("synonyms"),
                pl.col("symbolSynonyms").alias("symbol_synonyms"),
                pl.col("nameSynonyms").alias("name_synonyms"),
                pl.col("functionDescriptions").alias("function_descriptions"),
                subcellular_locations_transformed.alias("subcellular_locations"),
                pl.col("targetClass").alias("target_class"),
                pl.col("obsoleteSymbols").alias("obsolete_symbols"),
                pl.col("obsoleteNames").alias("obsolete_names"),
                constraint_transformed.alias("constraint"),
                tep_transformed.alias("tep"),
                pl.col("proteinIds").alias("protein_ids"),
                pl.col("dbXrefs").alias("db_xrefs"),
                chemical_probes_transformed.alias("chemical_probes"),
                homologues_transformed.alias("homologues"),
                pl.col("tractability"),
                safety_liabilities_transformed.alias("safety_liabilities"),
                pathways_transformed.alias("pathways"),
                pl.col("tss"),
            ).alias("metadata"),
        )
        .unique(subset=["id", "approved_symbol"])
        .sort(by=["id", "approved_symbol"])
    )


target_node = node(
    run,
    inputs={
        "target": "landing.opentargets.target",
    },
    outputs="opentargets.target",
    name="target",
    tags=["bronze"],
)
