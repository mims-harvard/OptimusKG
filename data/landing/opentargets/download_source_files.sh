#!/bin/bash

# Base URLs and directories
BASE_URL="http://ftp.ebi.ac.uk/pub/databases/opentargets/platform/24.09/output/etl"
BASE_DIR="."

# Subdirectories
EVIDENCE_SUBDIR="evidence"
TARGETS_DIR="./targets"
MOLECULE_DIR="./molecule"

# Evidence source IDs
SOURCE_IDS=(
    "cancer_gene_census"
    "chembl"
    "clingen"
    "crispr" 
    "crispr_screen"
    "expression_atlas"
    "gene_burden"
    "gene2phenotype"
    "genomics_england"
    "intogen"
    "progeny"
    "reactome"
    "slapenrich"
    "sysbio"
    "uniprot_literature"
)

download_files() {
    local source_name="$1"
    local download_dir="$2"
    local source_url="$3"
    local file_extension="$4"

    mkdir -p "$download_dir"
    cd "$download_dir" || exit

    files=$(curl -s "$source_url" | grep -oP "(?<=href=\")[^\"]*\.$file_extension(?=\")")

    for file in $files; do
        if [ ! -f "$file" ]; then
            echo "Downloading $file from $source_name..."
            curl -O "$source_url$file"
        else
            echo "Skipping $file - already exists"
        fi
    done

    cd - > /dev/null
}

# Download evidence files into ./evidence/<source_name> (parquet)
for source in "${SOURCE_IDS[@]}"; do
    download_files "$source" "$BASE_DIR/$EVIDENCE_SUBDIR/$source" "$BASE_URL/parquet/evidence/sourceId=$source/" "parquet"
done

# Download targets files (JSON version) directly into ./targets
download_files "targets" "$TARGETS_DIR" "$BASE_URL/json/targets/" "json"

# Download molecule files (JSON version) directly into ./molecule
download_files "molecule" "$MOLECULE_DIR" "$BASE_URL/json/molecule/" "json"
