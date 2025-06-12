import logging
from pathlib import Path

import typer

from cli.commands import neo4j_to_pg_command, write_metrics_command
from optimuskg.utils import calculate_checksum

app = typer.Typer(help="Main entry point for the CLI.")

logger = logging.getLogger("cli")


@app.command(help="Log the checksum of a file or directory.")
def checksum(  # noqa: PLR0913
    path: Path,
    checksum: str = typer.Option(
        None, "--checksum", help="The checksum to compare the file to."
    ),
    chunk_size: int = typer.Option(
        8192, "--chunk-size", help="The size of the chunks to read from the file."
    ),
    digest_size: int = typer.Option(
        16, "--digest-size", help="The size of the digest to use for the checksum."
    ),
    dir: bool = typer.Option(
        False, "--dir", help="Generate one checksum of all files in the directory."
    ),
):
    try:
        actual_checksum = calculate_checksum(
            path=path,
            chunk_size=chunk_size,
            digest_size=digest_size,
            process_directory=dir,
        )
        display_path = f"directory '{path}'" if dir else f"'{path}'"

        if not checksum:
            logger.info(f"The checksum of {display_path} is: {actual_checksum}")
        elif checksum == actual_checksum:
            logger.info(f"The checksum of {display_path} is correct")
        else:
            logger.error(
                f"The checksums do not match for {display_path}: {checksum} != {actual_checksum}"
            )
    except FileNotFoundError as e:
        logger.error(e)
    except IsADirectoryError as e:
        logger.error(e)
    except NotADirectoryError as e:
        logger.error(e)
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")


@app.command(help="Convert a Neo4j export JSONL file into a PG-JSONL representation.")
def neo4j_to_pg(
    in_path: Path = typer.Option(
        "data/neo4j/export/optimuskg.jsonl",
        "--in",
        help="The path to read the input file from.",
    ),
    out_path: Path = typer.Option(
        "data/export/optimuskg.pg.jsonl",
        "--out",
        help="The path to write the output file to.",
    ),
):
    neo4j_to_pg_command(in_path, out_path)


@app.command(help="Get statistics about a PG-JSONL knowledge graph.")
def write_metrics(
    in_path: Path = typer.Option(
        "data/export/optimuskg.pg.jsonl",
        "--in",
        help="The path to read the input file from.",
    ),
    data_out_path: Path = typer.Option(
        "data/export/metrics/metrics.json",
        "--out",
        help="The path to write the output file to.",
    ),
):
    write_metrics_command(in_path, data_out_path)


# @app.command(help="Get metadata via ID.")
# def get_metadata(
#     id: str = typer.Option(
#         None,
#         "--id",
#         help="The ID to get the metadata for.",
#     ),
# ):
#     get_metadata_command(id)

if __name__ == "__main__":
    app()
