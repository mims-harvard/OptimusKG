from typing import Annotated

from pydantic import Field, TypeAdapter

from .base import BaseProvider
from .http import HttpProvider
from .opentargets import OpenTargetsProvider

OriginProviderModel = Annotated[
    HttpProvider | OpenTargetsProvider, Field(discriminator="provider")
]

OriginProviderAdapter: TypeAdapter = TypeAdapter(OriginProviderModel)

__all__ = [
    "BaseProvider",
    "OriginProviderAdapter",
]
