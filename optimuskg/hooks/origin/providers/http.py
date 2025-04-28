from pathlib import Path
from typing import Literal, override

from pydantic import Field, HttpUrl

from optimuskg.hooks.origin.providers.base import BaseProvider


class HttpProvider(BaseProvider):
    url: HttpUrl = Field(..., description="URL to download the data from")
    provider: Literal["http"]

    @override
    def download(self, output_path: Path) -> None:
        self._download_file(url=str(self.url), output_path=output_path)
