from typing import Annotated, Literal

from pydantic import BaseModel, Field

Identifier = Annotated[str, Field(min_length=1)]
PropertyValue = str | float | bool
PropertyArray = list[PropertyValue]
Properties = dict[Identifier, PropertyArray]
Labels = list[Identifier]


class Node(BaseModel):
    type: Literal["node"]
    id: Identifier
    labels: Labels
    properties: Properties


class Edge(BaseModel):
    type: Literal["edge"]
    id: Identifier | None = None
    from_node: Identifier = Field(alias="from")
    to_node: Identifier = Field(alias="to")
    labels: Labels
    properties: Properties
    undirected: bool


class _Neo4jNode(BaseModel):
    id: Identifier
    labels: Labels
    properties: dict[Identifier, str]


class Neo4jNode(_Neo4jNode):
    type: Literal["node"]


class Neo4jEdge(BaseModel):
    type: Literal["relationship"]
    id: Identifier
    label: Identifier
    properties: dict[Identifier, str | bool]
    start: _Neo4jNode
    end: _Neo4jNode
