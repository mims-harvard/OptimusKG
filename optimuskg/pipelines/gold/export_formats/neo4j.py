import logging
import subprocess
from pathlib import Path

import polars as pl
import yaml

from optimuskg.pipelines.gold.utils.biocypher import run_biocypher

logger = logging.getLogger(__name__)

_BIOCYPHER_SCHEMA_CONFIG_PATH = Path("conf/base/biocypher/schema_config.yaml")
_NEO4J_IMPORT_PATH = Path("data/gold/neo4j/import")


def csv_to_neo4j(import_path: Path, schema_config_path: Path) -> None:
    """
    Import data into Neo4j from CSV files.
    """
    logger.info(f"Importing data from {import_path} into Neo4j.")

    with open(schema_config_path) as f:
        schema_config = yaml.safe_load(f)

    node_args = []
    edge_args = []
    for name, config in schema_config.items():
        represented_as = config.get("represented_as")

        if not represented_as:
            logger.debug(f"'{name}' is missing 'represented_as' in schema, skipping.")
            continue

        # BioCypher names files based on the schema key (entity name), not input_label.
        # Schema uses spaces (e.g., "biological process"), so split on whitespace.
        pascal_case_label = "".join(word.capitalize() for word in name.split())
        header_file = f"{pascal_case_label}-header.csv"

        if represented_as == "node":
            if (import_path / header_file).exists():
                node_args.append(
                    f"--nodes=/import/{header_file},/import/{pascal_case_label}-part.*"
                )
            else:
                logger.debug(
                    f"Header file for node '{pascal_case_label}' not found, skipping."
                )
        elif represented_as == "edge":
            if (import_path / header_file).exists():
                edge_args.append(
                    f"--relationships=/import/{header_file},/import/{pascal_case_label}-part.*"
                )
            else:
                logger.debug(
                    f"Header file for edge '{pascal_case_label}' not found, skipping."
                )

    if not node_args and not edge_args:
        logger.warning("No node or edge files found to import. Aborting.")
        return

    command = [
        "docker",
        "run",
        "--rm",
        "--volume=./data/gold/neo4j/data:/data",
        f"--volume=./{import_path}:/import",
        "--volume=./data/export:/export",
        "neo4j:5.26.2-community-bullseye",
        "neo4j-admin",
        "database",
        "import",
        "full",
        "neo4j",
        "--verbose",
        "--delimiter=;",
        "--array-delimiter=|",
        '--quote="',
        "--overwrite-destination=true",
    ]
    command.extend(node_args)
    command.extend(edge_args)

    logger.info("Running neo4j-admin import command...")
    logger.debug(f"Executing command: {' '.join(command)}")

    try:
        subprocess.run(command, check=True)
        logger.info("Neo4j import completed successfully.")
    except subprocess.CalledProcessError as e:
        logger.error(f"Neo4j import failed: {e}")
    except FileNotFoundError:
        logger.error(
            "Docker command not found. Please ensure Docker is installed and in your PATH."
        )


def neo4j_export(
    nodes_dict: dict[str, pl.DataFrame],
    edges_dict: dict[str, pl.DataFrame],
    include_properties: bool = True,
) -> None:
    """Export knowledge graph to Neo4j via BioCypher.

    Args:
        nodes_dict: Dictionary of node type name to DataFrame.
        edges_dict: Dictionary of edge type name to DataFrame.
        include_properties: If True, include properties in exported nodes/edges.
                           If False, export only structural data (id, label, etc.).
    """
    try:
        run_biocypher(nodes_dict, edges_dict, include_properties)
    except Exception as e:
        logger.exception(f"Error writing graph data to disk: {e}")
        raise

    # Fix permissions on import directory so host user can check file existence.
    # BioCypher writes files via Docker as uid 7474 with mode 700, which prevents
    # the host user from reading the directory for the csv_to_neo4j file checks.
    try:
        logger.info("Fixing permissions on import directory...")
        subprocess.run(
            [
                "docker",
                "run",
                "--rm",
                f"--volume=./{_NEO4J_IMPORT_PATH}:/import",
                "alpine",
                "chmod",
                "-R",
                "755",
                "/import",
            ],
            check=True,
        )
    except subprocess.CalledProcessError as e:
        logger.exception(f"Error fixing permissions on import directory: {e}")
        raise

    try:
        logger.info("Bulk-importing graph data from CSV files to Neo4j database...")
        csv_to_neo4j(
            import_path=_NEO4J_IMPORT_PATH,
            schema_config_path=_BIOCYPHER_SCHEMA_CONFIG_PATH,
        )
    except Exception as e:
        logger.exception(f"Error importing graph data to Neo4j: {e}")
        raise
