import hashlib
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


@app.command(help="Log the checksum of a file.")
def checksum(
    file: Path,
    checksum: str = typer.Option(
        None, "--checksum", help="The checksum to compare the file to."
    ),
    chunk_size: int = typer.Option(
        8192, "--chunk-size", help="The size of the chunks to read from the file."
    ),
    algorithm: str = typer.Option(
        "blake2b", "--algorithm", help="The algorithm to use for the checksum."
    ),
    digest_size: int = typer.Option(
        16, "--digest-size", help="The size of the digest to use for the checksum."
    ),
):
    with open(file, "rb") as f:
        file_hash = hashlib.new(algorithm, digest_size=digest_size)
        while chunk := f.read(chunk_size):
            file_hash.update(chunk)

    if not checksum:
        logger.info(f"The checksum of '{file}' is: {file_hash.hexdigest()}")
    elif checksum == file_hash.hexdigest():
        logger.info(f"The checksum of '{file}' is correct")
    else:
        logger.error(
            f"The checksums do not match for '{file}': {checksum} != {file_hash.hexdigest()}"
        )


if __name__ == "__main__":
    app()
