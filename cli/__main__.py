import logging
import subprocess
from pathlib import Path

import typer
from kedro.framework.project import LOGGING  # noqa: F401

from cli.utils import (
    download_drugbank_files,
    download_gene_names_files,
    download_ontologies,
    download_opentargets,
)

app = typer.Typer(help="Main entry point for the CLI.")

logger = logging.getLogger("cli")


@app.command(help="Download all landing zone files.")
def landing():
    logger.info("Downloading landing files...")

    download_opentargets()
    download_ontologies()
    download_drugbank_files()
    download_gene_names_files()

    logger.info("All landing files downloaded successfully")


@app.command(help="Spin up the Neo4j service.")
def neo4j():
    logger.info("Spinning up Neo4j service...")

    compose_file = Path(__file__).parent / "neo4j" / "docker-compose.yaml"
    subprocess.run(
        ["docker", "compose", "-f", str(compose_file), "up", "-d"], check=True
    )

    logger.info("Neo4j service started successfully.")
    logger.info("Web interface (HTTP): http://localhost:7474")
    logger.info("Web interface (HTTPS): https://localhost:7473")


if __name__ == "__main__":
    app()
