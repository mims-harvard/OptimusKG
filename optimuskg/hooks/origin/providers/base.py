import logging
from abc import ABC, abstractmethod
from pathlib import Path

import requests
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


class BaseProvider(BaseModel, ABC):
    provider: str  # Provider name used for specifying the provider class to use in the catalog
    chunk_size: int = Field(
        default=8192,
        description="Chunk size for file downloads",
    )
    timeout: int = Field(
        default=10,
        description="Timeout for file downloads",
    )

    @abstractmethod
    def download(self, output_path: Path) -> None:
        """Downloads data to the specified output path."""
        ...

    def _download_file(self, url: str, output_path: Path) -> None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        if not output_path.exists():
            logger.info("Downloading...")
            try:
                response = requests.get(
                    url,
                    stream=True,
                    allow_redirects=True,
                    timeout=self.timeout,
                )
                response.raise_for_status()

                with open(output_path, "wb") as f:
                    for chunk in response.iter_content(chunk_size=self.chunk_size):
                        f.write(chunk)
                logger.info("Downloaded")
            except requests.exceptions.RequestException:
                logger.exception(f"Failed to download from {url}")
                if output_path.exists():
                    output_path.unlink()
            except Exception:
                logger.exception(f"Failed to write to {output_path}")
        else:
            logger.debug(f"Skipping download - already exists at {output_path}")
