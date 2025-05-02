from pathlib import Path
from typing import ClassVar, Literal, override

from pydantic import Field

from optimuskg.hooks.origin.providers.base import BaseProvider


class DrugBankProvider(BaseProvider):
    BASE_URL: ClassVar[str] = "https://go.drugbank.com/releases"

    version: str = Field(
        ..., min_length=1, description="Version of the dataset to download"
    )
    dataset: str = Field(..., min_length=1, description="Dataset to download")
    provider: Literal["drugbank"] = Field("drugbank", frozen=True)

    def _build_url(self):
        return f"{DrugBankProvider.BASE_URL}/{self.version}/downloads/{self.dataset}"

    @override
    def download(self, output_path: Path) -> None:
        self._download_file(url=self._build_url(), output_path=output_path)
