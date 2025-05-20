from pathlib import Path
from typing import ClassVar, Literal, override

from pydantic import Field

from optimuskg.hooks.origin.providers.base import BaseProvider


class BioOntologyProvider(BaseProvider):
    BASE_URL: ClassVar[str] = "https://data.bioontology.org/ontologies"
    PUBLIC_API_KEY: ClassVar[str] = (
        "8b5b7825-538d-40e0-9e9e-5ab9274a9aeb"
        # "168d52c1-bd36-4866-aa99-0aa8eb06f295"  # NOTE: is safe to use this key as default value (it's public)
    )

    acronym: str = Field(..., min_length=1, description="Ontology acronym")
    version: str = Field(
        ..., min_length=1, description="Version of the ontology to download"
    )
    api_key: str | None = Field(None, description="API key to download the data from")
    provider: Literal["bioontology"] = Field("bioontology", frozen=True)

    def _build_api_key(self):
        if self.api_key:
            return self.api_key
        return BioOntologyProvider.PUBLIC_API_KEY

    def _build_url(self):
        return f"{BioOntologyProvider.BASE_URL}/{self.acronym}/submissions/{self.version}/download?apikey={self._build_api_key()}"

    @override
    def download(self, output_path: Path) -> None:
        self._download_file(url=self._build_url(), output_path=output_path)
