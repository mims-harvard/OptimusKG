"""Data loading for knowledge-graph embeddings.

Responsible for three things:

1. **Acquiring the gold graph.** If the consolidated ``nodes.parquet`` /
   ``edges.parquet`` are not already on disk, they are downloaded from Harvard
   Dataverse via the published ``optimuskg`` Python client. That client shares
   its top-level import name with *this* repository's package, so it cannot be
   imported in-process here; instead it is invoked in an isolated
   ``uv run --no-project`` subprocess (see :func:`ensure_gold_data`).
2. **Type/relation metadata.** Maps the 3-letter node ``label`` codes and the
   ``relation`` enum values to human-readable names, semantic families, and a
   stable colourblind-friendly palette, all derived from the canonical
   :mod:`optimuskg.pipelines.silver.nodes.constants` enums.
3. **Triple construction.** Turns the node/edge tables into integer-indexed
   ``(head, relation, tail)`` triples for TransE, retaining the per-entity type
   and per-relation family labels needed for the clustering analysis.
"""

from __future__ import annotations

import logging
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import polars as pl

from optimuskg.pipelines.silver.nodes.constants import Node, Relation

logger = logging.getLogger("cli")

# Default Harvard Dataverse DOI for the published graph (see README).
DEFAULT_DOI = "doi:10.7910/DVN/IYNGEV"

# ---------------------------------------------------------------------------
# Type and relation metadata (derived from the canonical enums)
# ---------------------------------------------------------------------------

# 3-letter node code -> human-readable name, e.g. "GEN" -> "Gene".
NODE_TYPE_NAME: dict[str, str] = {
    member.value: member.name.replace("_", " ").title() for member in Node
}

# Semantic grouping of the ~40 relation types into coherent families. The
# question "do relations of the same type cluster" is only well-posed once a
# notion of relation *type* is fixed; these families mirror the section
# headings in the source ``Relation`` enum.
_R = Relation
RELATION_FAMILY_GROUPS: dict[str, list[Relation]] = {
    "Hierarchy": [_R.PARENT, _R.IS_A],
    "Association": [_R.INTERACTS_WITH, _R.ASSOCIATED_WITH, _R.LINKED_TO],
    "Expression": [_R.EXPRESSION_PRESENT, _R.EXPRESSION_ABSENT],
    "Phenotype": [_R.PHENOTYPE_PRESENT, _R.PHENOTYPE_ABSENT],
    "Drug-Disease": [_R.INDICATION, _R.OFF_LABEL_USE, _R.CONTRAINDICATION],
    "Adverse reaction": [_R.ADVERSE_DRUG_REACTION],
    "Drug-Drug": [_R.SYNERGISTIC_INTERACTION],
    "Drug-Gene role": [_R.TARGET, _R.ENZYME, _R.TRANSPORTER, _R.CARRIER],
    "Drug-Gene action": [
        _R.ACTIVATOR,
        _R.AGONIST,
        _R.ALLOSTERIC_ANTAGONIST,
        _R.ANTAGONIST,
        _R.BINDING_AGENT,
        _R.BLOCKER,
        _R.DEGRADER,
        _R.INHIBITOR,
        _R.INVERSE_AGONIST,
        _R.MODULATOR,
        _R.NEGATIVE_ALLOSTERIC_MODULATOR,
        _R.NEGATIVE_MODULATOR,
        _R.OPENER,
        _R.PARTIAL_AGONIST,
        _R.POSITIVE_ALLOSTERIC_MODULATOR,
        _R.POSITIVE_MODULATOR,
        _R.RELEASING_AGENT,
        _R.STABILISER,
        _R.SUBSTRATE,
    ],
}

# relation value -> family name, with any unmapped relation (e.g. OTHER)
# falling back to "Other".
RELATION_FAMILY: dict[str, str] = {
    rel.value: family
    for family, relations in RELATION_FAMILY_GROUPS.items()
    for rel in relations
}


def relation_family(relation: str) -> str:
    """Return the semantic family for a relation value (``"Other"`` if unknown)."""
    return RELATION_FAMILY.get(relation, "Other")


# Colourblind-friendly qualitative palette (Okabe-Ito, extended to 10). Used to
# colour the 10 node types; families reuse the same ordered palette.
PALETTE: list[str] = [
    "#0072B2",  # blue
    "#E69F00",  # orange
    "#009E73",  # bluish green
    "#D55E00",  # vermillion
    "#CC79A7",  # reddish purple
    "#56B4E9",  # sky blue
    "#117733",  # dark green
    "#882255",  # wine
    "#999999",  # grey
    "#44AA99",  # teal
]


def categorical_colors(categories: list[str]) -> dict[str, str]:
    """Map an ordered list of category labels to stable palette colours."""
    return {cat: PALETTE[i % len(PALETTE)] for i, cat in enumerate(categories)}


# ---------------------------------------------------------------------------
# Triple container
# ---------------------------------------------------------------------------


@dataclass
class Triples:
    """Integer-indexed knowledge-graph triples plus alignment metadata.

    Attributes:
        head: ``(n_triples,)`` int32 array of head-entity indices.
        relation: ``(n_triples,)`` int32 array of relation indices.
        tail: ``(n_triples,)`` int32 array of tail-entity indices.
        entity_ids: Entity string IDs, indexed by entity index.
        entity_types: Node type code (e.g. ``"GEN"``) per entity index.
        relation_names: Relation string values, indexed by relation index.
    """

    head: np.ndarray
    relation: np.ndarray
    tail: np.ndarray
    entity_ids: list[str]
    entity_types: list[str]
    relation_names: list[str]

    @property
    def n_entities(self) -> int:
        """Number of distinct entities."""
        return len(self.entity_ids)

    @property
    def n_relations(self) -> int:
        """Number of distinct relations."""
        return len(self.relation_names)

    @property
    def n_triples(self) -> int:
        """Number of triples."""
        return int(self.head.shape[0])


# ---------------------------------------------------------------------------
# Gold-graph acquisition
# ---------------------------------------------------------------------------

# Inline script run by the isolated ``optimuskg`` client subprocess. Writes the
# consolidated node/edge tables to the requested paths and prints their row
# counts. Kept dependency-free beyond the client + polars.
_DOWNLOAD_SCRIPT = """
import sys
import optimuskg as okg

cache_dir, doi, lcc, out_nodes, out_edges = sys.argv[1:6]
okg.set_cache_dir(cache_dir)
if doi:
    okg.set_doi(doi)
nodes, edges = okg.load_graph(lcc=(lcc == "1"))
nodes.write_parquet(out_nodes)
edges.write_parquet(out_edges)
print(f"DOWNLOAD_OK nodes={nodes.height} edges={edges.height}")
"""


def ensure_gold_data(
    nodes_path: Path,
    edges_path: Path,
    *,
    lcc: bool = False,
    download: bool = True,
    cache_dir: Path | None = None,
    doi: str = DEFAULT_DOI,
) -> tuple[Path, Path]:
    """Ensure the gold node/edge parquet files exist, downloading if needed.

    If both ``nodes_path`` and ``edges_path`` already exist they are returned
    unchanged. Otherwise — and only when ``download`` is set — the published
    ``optimuskg`` client is run in an isolated ``uv run --no-project`` process
    to fetch the graph from Dataverse and write it to the requested paths.

    The client is isolated because its import name (``optimuskg``) collides with
    this repository's own package; running ``--no-project`` from a scratch
    directory ensures the *published* client is imported rather than the local
    pipeline package.

    Args:
        nodes_path: Destination for the consolidated ``nodes.parquet``.
        edges_path: Destination for the consolidated ``edges.parquet``.
        lcc: Download only the largest connected component.
        download: If false, never download; raise if files are missing.
        cache_dir: Client cache directory (defaults to ``<nodes dir>/.optimuskg_cache``).
        doi: Dataverse DOI to target.

    Returns:
        The ``(nodes_path, edges_path)`` that now exist on disk.

    Raises:
        FileNotFoundError: Files are missing and ``download`` is false.
        RuntimeError: The download subprocess failed.
    """
    if nodes_path.exists() and edges_path.exists():
        logger.info("Using existing gold data: %s, %s", nodes_path, edges_path)
        return nodes_path, edges_path

    if not download:
        msg = (
            f"Gold data not found ({nodes_path}, {edges_path}). Either run the "
            "Kedro pipeline, or omit --no-download to fetch it via the optimuskg "
            "client."
        )
        raise FileNotFoundError(msg)

    nodes_path.parent.mkdir(parents=True, exist_ok=True)
    edges_path.parent.mkdir(parents=True, exist_ok=True)
    cache = cache_dir or (nodes_path.parent / ".optimuskg_cache")
    cache.mkdir(parents=True, exist_ok=True)

    logger.info(
        "Downloading gold graph (lcc=%s, doi=%s) via the optimuskg client; "
        "this is large and may take a while...",
        lcc,
        doi,
    )

    # Run from a scratch directory so the local ``optimuskg`` package (in the
    # repo root) does not shadow the published client on sys.path.
    with tempfile.TemporaryDirectory() as scratch:
        cmd = [
            "uv",
            "run",
            "--no-project",
            "--with",
            "optimuskg",
            "python",
            "-c",
            _DOWNLOAD_SCRIPT,
            str(cache),
            doi,
            "1" if lcc else "0",
            str(nodes_path),
            str(edges_path),
        ]
        result = subprocess.run(  # noqa: S603
            cmd,
            cwd=scratch,
            capture_output=True,
            text=True,
        )

    if result.returncode != 0 or not (nodes_path.exists() and edges_path.exists()):
        logger.error("Client stdout:\n%s", result.stdout)
        logger.error("Client stderr:\n%s", result.stderr)
        msg = "Failed to download gold graph via the optimuskg client."
        raise RuntimeError(msg)

    logger.info("Download complete (%s).", result.stdout.strip().splitlines()[-1])
    return nodes_path, edges_path


# ---------------------------------------------------------------------------
# Triple construction
# ---------------------------------------------------------------------------


def load_triples(
    nodes_path: Path,
    edges_path: Path,
    *,
    relation_key: str = "relation",
    max_edges: int | None = None,
    seed: int = 42,
) -> Triples:
    """Build integer-indexed triples from the gold node/edge tables.

    Args:
        nodes_path: Path to the consolidated ``nodes.parquet``.
        edges_path: Path to the consolidated ``edges.parquet``.
        relation_key: Edge column to use as the relation type — ``"relation"``
            (semantic, ~40 values) or ``"label"`` (metaedge, 27 values).
        max_edges: If set, randomly subsample to this many edges (for quick
            runs). Sampling is seeded for reproducibility.
        seed: Random seed for subsampling.

    Returns:
        A :class:`Triples` with aligned entity-type and relation metadata.
    """
    if relation_key not in ("relation", "label"):
        msg = f"relation_key must be 'relation' or 'label', got {relation_key!r}"
        raise ValueError(msg)

    nodes = pl.read_parquet(nodes_path, columns=["id", "label"])
    edges = pl.read_parquet(edges_path, columns=["from", "to", relation_key])

    if max_edges is not None and edges.height > max_edges:
        edges = edges.sample(n=max_edges, seed=seed)
        logger.info("Subsampled to %d edges (seed=%d)", max_edges, seed)

    # Restrict entities to those that actually appear in the (possibly
    # subsampled) edge set, so the embedding table has no isolated rows.
    used_ids = (
        pl.concat([edges["from"], edges["to"]]).unique().rename("id").to_frame()
    )
    nodes = nodes.join(used_ids, on="id", how="inner")

    # Stable entity index: sorted by type then id for deterministic output.
    nodes = nodes.sort(["label", "id"]).with_row_index("entity_idx")
    entity_ids = nodes["id"].to_list()
    entity_types = nodes["label"].to_list()
    id_to_idx = dict(zip(nodes["id"].to_list(), nodes["entity_idx"].to_list()))

    # Stable relation index: sorted by relation value.
    relation_names = sorted(edges[relation_key].unique().to_list())
    rel_to_idx = {name: i for i, name in enumerate(relation_names)}

    head = edges["from"].replace_strict(id_to_idx).to_numpy().astype(np.int32)
    tail = edges["to"].replace_strict(id_to_idx).to_numpy().astype(np.int32)
    rel = edges[relation_key].replace_strict(rel_to_idx).to_numpy().astype(np.int32)

    logger.info(
        "Built %d triples over %d entities (%d types) and %d relations (key=%s)",
        head.shape[0],
        len(entity_ids),
        len(set(entity_types)),
        len(relation_names),
        relation_key,
    )

    return Triples(
        head=head,
        relation=rel,
        tail=tail,
        entity_ids=entity_ids,
        entity_types=entity_types,
        relation_names=relation_names,
    )
