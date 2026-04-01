from __future__ import annotations

import gzip
import logging
import shutil
import subprocess  # nosec B404
import time
from copy import deepcopy
from pathlib import Path, PurePosixPath
from typing import Any, override

import fsspec
import pandas as pd
import polars as pl
from kedro.io.core import (
    AbstractVersionedDataset,
    DatasetError,
    Version,
    get_filepath_str,
    get_protocol_and_path,
)

logger = logging.getLogger("optimuskg")


class SQLDumpQueryDataset(AbstractVersionedDataset[pl.DataFrame, pl.DataFrame]):
    """``SQLDumpQueryDataset`` loads data from a SQL dump file, restores a PostgreSQL database,
    and executes a query to retrieve the data as a Polars DataFrame."""

    DEFAULT_LOAD_ARGS: dict[str, Any] = {}
    DEFAULT_FS_ARGS: dict[str, Any] = {}
    DOCKER_COMPOSE_PATH: Path = Path(__file__).parent / "docker-compose.yaml"
    DATABASE_CONNECTION_STR: str = "postgresql://optimuskg:optimuskg@localhost:5432/"

    def __init__(  # noqa: PLR0913
        self,
        *,
        filepath: str,
        query: str,
        db_name: str,
        load_args: dict[str, Any] | None = None,
        version: Version | None = None,
        credentials: dict[str, Any] | None = None,
        fs_args: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Creates a new instance of ``SQLDumpQueryDataset``."""
        _fs_args = deepcopy(fs_args) or {}
        _fs_open_args_load = _fs_args.pop("open_args_load", {})
        _credentials = deepcopy(credentials) or {}

        protocol, path = get_protocol_and_path(filepath, version)
        if protocol == "file":
            _fs_args.setdefault("auto_mkdir", True)

        self._protocol = protocol
        self._storage_options = {**_credentials, **_fs_args}
        self._fs = fsspec.filesystem(self._protocol, **self._storage_options)

        # Validate and sanitize inputs
        if not self._is_safe_db_name(db_name):
            raise DatasetError("Invalid database name")

        self._query = query
        self._db_name = db_name
        self._conn_string = f"{self.DATABASE_CONNECTION_STR}{self._db_name}"
        self.metadata = metadata

        # Verify docker and docker compose are available
        self._verify_dependencies()

        super().__init__(
            filepath=PurePosixPath(path),
            version=version,
            exists_function=self._fs.exists,
            glob_function=self._fs.glob,
        )

        self._load_args = {**self.DEFAULT_LOAD_ARGS, **(load_args or {})}
        self._fs_open_args_load = {
            **self.DEFAULT_FS_ARGS.get("open_args_load", {}),
            **(_fs_open_args_load or {}),
        }

    @staticmethod
    def _is_safe_db_name(db_name: str) -> bool:
        """Validate database name to prevent injection attacks."""
        return bool(db_name and db_name.isalnum() and not db_name.startswith("pg_"))

    @staticmethod
    def _verify_dependencies() -> None:
        """Verify that required system dependencies are available."""
        if not shutil.which("docker"):
            raise DatasetError("Required dependency 'docker' not found in PATH")

    def _run_subprocess(
        self, cmd: list[str], **kwargs: Any
    ) -> subprocess.CompletedProcess[bytes] | None:
        """Safely execute a subprocess command."""
        try:
            # Use absolute paths for executables
            cmd[0] = shutil.which(cmd[0]) or cmd[0]
            return subprocess.run(  # nosec B603
                cmd, check=True, capture_output=True, **kwargs
            )
        except subprocess.CalledProcessError as e:
            logger.exception(f"Subprocess command failed: {e}")
            return None
        except Exception as e:
            logger.exception(f"Failed to execute command: {e}")
            return None

    @override
    def _describe(self) -> dict[str, Any]:
        return {
            "filepath": self._filepath,
            "protocol": self._protocol,
            "query": self._query,
            "db_name": self._db_name,
            "version": self._version,
        }

    @override
    def load(self) -> pl.DataFrame:
        """Restores the database from the dump file and executes the SQL query."""
        self._initialize_database()
        self._restore_database()
        return self._execute_query()

    def _initialize_database(self) -> None:
        logger.info("Initializing PostgreSQL database...")
        compose_path = self.DOCKER_COMPOSE_PATH.resolve()
        if not compose_path.is_file():
            raise DatasetError(f"Docker compose file not found: {compose_path}")

        # 1) Start the container
        self._run_subprocess(
            [
                "docker",
                "compose",
                "-f",
                str(compose_path),
                "up",
                "-d",
            ],
            text=True,
        )

        # 2) Wait until Postgres is ready
        self._wait_for_postgres(
            container="drugcentral_postgres",
            user="optimuskg",
            db=self._db_name,
            timeout=60.0,
            base_interval=1.0,
            max_interval=8.0,
        )

        # 3) Wait until the database is actually created by the init script
        self._wait_for_database(
            container="drugcentral_postgres",
            user="optimuskg",
            db=self._db_name,
            timeout=60.0,
            base_interval=1.0,
            max_interval=8.0,
        )

        logger.info("PostgreSQL database initialized and healthy.")

    def _restore_database(self) -> None:
        dump_path_str = get_filepath_str(self._filepath, self._protocol)
        logger.info(f"Restoring database from dump file: {dump_path_str}")

        try:
            dump_path = Path(dump_path_str).resolve()
            if not dump_path.is_file():
                raise DatasetError(f"Dump file not found: {dump_path}")

            # Check if the dump file is gzipped and extract it if needed
            if dump_path.suffix == ".gz":
                extracted_path = dump_path.with_suffix("")  # Remove .gz extension
                if not extracted_path.exists():
                    logger.debug("Detected gzipped SQL dump file. Extracting...")
                    with gzip.open(dump_path, "rb") as f_in:
                        with open(extracted_path, "wb") as f_out:
                            shutil.copyfileobj(f_in, f_out)
                    logger.debug(f"Extracted to: {extracted_path}")
                dump_path = extracted_path

            with open(dump_path, "rb") as dump_file:
                self._run_subprocess(
                    [
                        "docker",
                        "exec",
                        "-i",
                        "drugcentral_postgres",
                        "psql",  # TODO: Investigate using pg_restore with --jobs=number-of-jobs flag to speed up the restore process, also use --single-transaction flag to stop on error
                        self._conn_string,  # TODO: If using pg_restore, use the --dbname flag to specify the connection string (ref. https://stackoverflow.com/a/28359470)
                    ],
                    stdin=dump_file,
                )
        except Exception as e:
            self._shutdown_database()
            raise DatasetError(f"Database restore failed: {str(e)}")
        logger.info("Database restored.")

    def _execute_query(self) -> pl.DataFrame:
        logger.info("Executing query on PostgreSQL database...")
        try:
            df = pd.read_sql_query(self._query, self._conn_string)
            self._shutdown_database()
            return pl.DataFrame(df)
        except Exception as e:
            self._shutdown_database()
            raise DatasetError(f"Query execution failed: {str(e)}")

    def _shutdown_database(self) -> None:
        logger.info("Shutting down PostgreSQL database...")
        try:
            compose_path = self.DOCKER_COMPOSE_PATH.resolve()
            self._run_subprocess(
                ["docker", "compose", "-f", str(compose_path), "restart"], text=True
            )
        except Exception as e:
            logger.error(f"Failed to stop the PostgreSQL database: {e}")
        logger.info("PostgreSQL database shutdown.")

    def _wait_for_postgres(  # noqa: PLR0913
        self,
        container: str,
        user: str,
        db: str,
        *,
        timeout: float = 60.0,
        base_interval: float = 1.0,
        max_interval: float = 8.0,
    ) -> None:
        """
        Poll `pg_isready` inside the given Docker container until it returns zero (ready),
        using exponential backoff up to `max_interval`, for up to `timeout` seconds.
        Raises DatasetError if the timeout is exceeded.
        """
        start = time.monotonic()
        attempt = 0
        docker_cmd = shutil.which("docker") or "docker"

        while True:
            try:
                self._run_subprocess(
                    [
                        docker_cmd,
                        "exec",
                        container,
                        "pg_isready",
                        "-U",
                        user,
                        "-d",
                        db,
                    ],
                    text=True,
                )
                return
            except subprocess.CalledProcessError:
                elapsed = time.monotonic() - start
                if elapsed >= timeout:
                    raise DatasetError(
                        f"Postgres ({container}) did not become ready after {timeout:.1f}s"
                    )
                wait = min(base_interval * 2**attempt, max_interval)
                logger.info(f"Postgres not ready yet, retrying in {wait:.1f}s...")
                time.sleep(wait)
                attempt += 1

    def _wait_for_database(  # noqa: PLR0913
        self,
        container: str,
        user: str,
        db: str,
        *,
        timeout: float = 60.0,
        base_interval: float = 1.0,
        max_interval: float = 8.0,
    ) -> None:
        """
        Poll the container until the target database exists,
        using exponential backoff up to `max_interval`, for up to `timeout` seconds.
        Raises DatasetError if the timeout is exceeded.
        """
        start = time.monotonic()
        attempt = 0
        docker_cmd = shutil.which("docker") or "docker"

        while True:
            result = subprocess.run(  # nosec B603
                [
                    docker_cmd,
                    "exec",
                    container,
                    "psql",
                    "-U",
                    user,
                    "-d",
                    db,
                    "-c",
                    "SELECT 1",
                ],
                capture_output=True,
                text=True,
            )
            if result.returncode == 0:
                return

            elapsed = time.monotonic() - start
            if elapsed >= timeout:
                raise DatasetError(
                    f"Database '{db}' did not become available after {timeout:.1f}s"
                )
            wait = min(base_interval * 2**attempt, max_interval)
            logger.info(f"Database '{db}' not ready yet, retrying in {wait:.1f}s...")
            time.sleep(wait)
            attempt += 1

    @override
    def save(self, data: pl.DataFrame) -> None:
        """Saving is not supported for this dataset."""
        raise DatasetError("SQLDumpQueryDataset is read-only.")

    @override
    def _exists(self) -> bool:
        """Checks if the dump file exists."""
        try:
            load_path = get_filepath_str(self._get_load_path(), self._protocol)
        except DatasetError:
            self._shutdown_database()
            return False

        return self._fs.exists(load_path)  # type: ignore

    @override
    def _release(self) -> None:
        """Releases resources used by the dataset."""
        try:
            self._shutdown_database()
        except Exception as e:
            logger.error(f"Failed to stop the PostgreSQL database: {e}")
        finally:
            super()._release()
