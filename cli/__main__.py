import logging

import typer
from kedro.framework.project import LOGGING  # noqa: F401

from cli.utils import download_ontologies, download_opentargets

app = typer.Typer(help="Main entry point for the CLI.")

logger = logging.getLogger("cli")


@app.command(help="Download all landing zone files.")
def landing():
    logger.info("Downloading landing files...")

    download_opentargets()
    download_ontologies()

    logger.info("All landing files downloaded successfully")


if __name__ == "__main__":
    app()
