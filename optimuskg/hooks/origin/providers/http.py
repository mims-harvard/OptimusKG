from pathlib import Path
from typing import Literal, override

from pydantic import Field, HttpUrl

from optimuskg.hooks.origin.providers.base import BaseProvider


class HttpProvider(BaseProvider):
    url: HttpUrl = Field(..., min_length=1, description="URL to download the data from")
    provider: Literal["http"] = Field("http", frozen=True)

    @override
    def download(self, output_path: Path) -> None:
        self._download_file(url=str(self.url), output_path=output_path)
