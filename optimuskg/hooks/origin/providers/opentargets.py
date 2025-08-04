import logging
import subprocess
from pathlib import Path
from typing import ClassVar, Literal, override

from pydantic import Field

from optimuskg.hooks.origin.providers.base import BaseProvider

logger = logging.getLogger(__name__)


class OpenTargetsProvider(BaseProvider):
    BASE_URL: ClassVar[str] = "ftp://ftp.ebi.ac.uk/pub/databases/opentargets/platform"

    version: str = Field(..., description="OpenTargets version")
    dataset_name: str = Field(..., description="OpenTargets dataset name")
    provider: Literal["opentargets"]

    @override
    def download(self, output_path: Path) -> None:
        url = self._build_url()

        if output_path.is_dir():
            logger.info(f"Output path {output_path} already exists.")
            return

        logger.info(
            f"Output path {output_path} does not exist or is not a directory. It will be created."
        )

        try:
            output_path.mkdir(parents=True, exist_ok=True)

            cmd = [
                "wget",
                "--recursive",
                "--no-parent",
                "--no-host-directories",
                "--cut-dirs=8",
                url,
                "-P",
                str(output_path),
            ]

            subprocess.run(cmd, check=True)

        except subprocess.CalledProcessError:
            logger.exception(f"Error downloading files from {url} using wget")
            raise
        except Exception:
            logger.exception(f"Error during download process from {url}")
            raise

    def _build_url(self) -> str:
        base_path = f"{OpenTargetsProvider.BASE_URL}/{self.version}/output/"
        return f"{base_path}/{self.dataset_name}"
