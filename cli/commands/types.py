from typing import Annotated, Literal, Self

from pydantic import BaseModel, Field

Identifier = Annotated[str, Field(min_length=1)]
PropertyValue = str | float | bool
PropertyArray = list[PropertyValue]
Properties = dict[Identifier, PropertyArray]
Labels = list[Identifier]


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
    properties: dict[Identifier, str]
    start: _Neo4jNode
    end: _Neo4jNode


class Node(BaseModel):
    type: Literal["node"]
    id: Identifier
    labels: Labels
    properties: Properties

    @classmethod
    def from_neo4j(cls, node: Neo4jNode) -> Self:
        possible_labels = [
            "Gene",
            "Anatomy",
            "Exposure",
            "Drug",
            "Disease",
            "Phenotype",
            "BiologicalProcess",
            "CellularComponent",
            "MolecularFunction",
            "Pathway",
        ]
        first_matching_label = next(
            lbl for lbl in node.labels if lbl in possible_labels
        )

        return cls(
            type="node",
            id=node.properties.get("id", ""),
            labels=[first_matching_label],
            properties={
                k: [v] for k, v in node.properties.items() if k in ["name", "source"]
            },
        )


class Edge(BaseModel):
    type: Literal["edge"]
    id: Identifier | None = None
    from_node: Identifier = Field(alias="from")
    to_node: Identifier = Field(alias="to")
    labels: Labels
    properties: Properties
    undirected: bool

    @classmethod
    def from_neo4j(cls, edge: Neo4jEdge) -> Self:
        return cls(
            **{  # Using dict-style to avoid serialization issues with aliases
                "type": "edge",
                "id": edge.properties.get("id"),
                "from": edge.start.properties.get("id"),
                "to": edge.end.properties.get("id"),
                "labels": [edge.label],
                "properties": {
                    k: [v] for k, v in edge.properties.items() if k == "relation_type"
                },
                "undirected": edge.properties.get("undirected", "false").lower()
                == "true",
            }
        )
