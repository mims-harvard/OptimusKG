import logging
import os
import re
from pathlib import Path
from typing import ClassVar, Literal, override

import requests
from pydantic import Field

from optimuskg.hooks.origin.providers.base import BaseProvider

logger = logging.getLogger(__name__)


class OpenTargetsProvider(BaseProvider):
    BASE_URL: ClassVar[str] = "https://ftp.ebi.ac.uk/pub/databases/opentargets/platform"

    version: str = Field(..., description="OpenTargets version")
    file_extension: str = Field(
        ...,
        description="File extension (e.g. json, parquet)",
        pattern="^(json|parquet)$",
    )
    source_id: str = Field(..., description="Source ID for evidence data")
    is_evidence: bool = Field(
        default=False, description="Whether the data is evidence data or not"
    )
    provider: Literal["opentargets"]

    @override
    def download(self, output_path: Path) -> None:
        url = self._build_evidence_url()

        if not output_path.is_dir():
            logger.debug(
                f"Output path {output_path} does not exist or is not a directory. It will be created."
            )
        else:
            logger.debug(f"Output path {output_path} already exists.")

        try:
            files_to_download = self._ls_from_url(url)
            if not files_to_download:
                return

            output_path.mkdir(parents=True, exist_ok=True)

            for file in files_to_download:
                file_url = f"{url}{file}"
                output_file = output_path / os.path.basename(file)
                self._download_file(url=file_url, output_path=output_file)
        except requests.exceptions.RequestException:
            logger.exception(f"Error listing files from {url}")
            raise
        except Exception:
            logger.exception(f"Error during download process from {url}")
            raise

    def _build_evidence_url(self) -> str:
        base_path = f"{OpenTargetsProvider.BASE_URL}/{self.version}/output/etl/{self.file_extension}"

        if self.is_evidence:
            return f"{base_path}/evidence/sourceId={self.source_id}/"

        return f"{base_path}/{self.source_id}/"

    def _ls_from_url(self, url: str) -> list[str]:
        response = requests.get(url, timeout=self.timeout)
        response.raise_for_status()

        pattern = f'(?<=href=")[^"]*\\.{self.file_extension}(?=")'
        files = re.findall(pattern, response.text)
        if not files:
            logger.warning(
                f"No files found at URL {url} with extension '{self.file_extension}'"
            )
        logger.debug(f"Found {len(files)} files to download from {url}")
        return files
