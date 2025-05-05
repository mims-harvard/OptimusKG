from typing import Annotated

from pydantic import Field, TypeAdapter

from .base import BaseProvider
from .bioontology import BioOntologyProvider
from .drugbank import DrugBankProvider
from .http import HttpProvider
from .opentargets import OpenTargetsProvider

OriginProviderModel = Annotated[
    HttpProvider | OpenTargetsProvider | BioOntologyProvider | DrugBankProvider,
    Field(discriminator="provider"),
]

OriginProviderAdapter: TypeAdapter = TypeAdapter(OriginProviderModel)

__all__ = [
    "BaseProvider",
    "OriginProviderAdapter",
]
