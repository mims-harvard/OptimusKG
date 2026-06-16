# OptimusKG graph schema reference

Full node-type and edge-type taxonomy for OptimusKG. Counts are from the
published release (`doi:10.7910/DVN/IYNGEV`); use the type codes and relation
strings below to filter the graph. For exact per-type property fields, see
https://optimuskg.ai/docs/graph-schema/nodes and
https://optimuskg.ai/docs/graph-schema/edges.

## Base table columns

**Nodes** (`nodes.parquet`, `largest_connected_component_nodes.parquet`):

| Column | Type | Notes |
|--------|------|-------|
| `id` | `str` | Stable node identifier |
| `label` | `str` | Node type code (e.g. `GEN`) |
| `properties` | JSON string | Per-type metadata; a Polars `Struct` in `nodes/<type>.parquet` |

**Edges** (`edges.parquet`, `largest_connected_component_edges.parquet`):

| Column | Type | Notes |
|--------|------|-------|
| `from` | `str` | Source node id |
| `to` | `str` | Target node id |
| `label` | `str` | Edge type code (e.g. `DIS-GEN`) |
| `relation` | `str` | Specific relation (e.g. `ASSOCIATED_WITH`) |
| `undirected` | `bool` | Whether the edge is undirected |
| `properties` | JSON string | A Polars `Struct` in `edges/<label>.parquet` |

In `load_networkx`, node `label` and edge `relation` are available on the
attribute dict; `properties` keys are merged in unless `parse_properties=False`.

## Node types (10)

| Label | Type | Count |
|-------|------|------:|
| `GEN` | Gene | 61,306 |
| `DIS` | Disease | 36,345 |
| `BPO` | Biological Process | 25,754 |
| `PHE` | Phenotype | 19,341 |
| `DRG` | Drug | 16,766 |
| `ANA` | Anatomy | 13,120 |
| `MFN` | Molecular Function | 10,161 |
| `CCO` | Cellular Component | 4,052 |
| `PWY` | Pathway | 2,805 |
| `EXP` | Exposure | 881 |

## Edge types (27)

| Label | Relation(s) | Count |
|-------|-------------|------:|
| `DIS-GEN` | `ASSOCIATED_WITH` | 9,734,774 |
| `ANA-GEN` | `EXPRESSION_PRESENT`, `EXPRESSION_ABSENT` | 8,787,955 |
| `DRG-DRG` | `SYNERGISTIC_INTERACTION`, `PARENT` | 1,345,376 |
| `PHE-GEN` | `ASSOCIATED_WITH` | 793,279 |
| `GEN-GEN` | `INTERACTS_WITH` | 327,924 |
| `BPO-GEN` | `INTERACTS_WITH` | 158,410 |
| `DIS-PHE` | `PHENOTYPE_PRESENT` | 157,144 |
| `CCO-GEN` | `INTERACTS_WITH` | 105,309 |
| `MFN-GEN` | `INTERACTS_WITH` | 90,933 |
| `DRG-DIS` | `INDICATION`, `CONTRAINDICATION`, `OFF_LABEL_USE` | 70,380 |
| `PWY-GEN` | `INTERACTS_WITH` | 46,977 |
| `BPO-BPO` | `IS_A` | 44,494 |
| `DIS-DIS` | `PARENT` | 44,215 |
| `PHE-PHE` | `PARENT` | 24,862 |
| `DRG-GEN` | `ACTIVATOR`, `AGONIST`, `ALLOSTERIC_ANTAGONIST`, `ANTAGONIST`, `BINDING_AGENT`, `BLOCKER`, `CARRIER`, `DEGRADER`, `ENZYME`, `INHIBITOR`, `INVERSE_AGONIST`, `MODULATOR`, `NEGATIVE_ALLOSTERIC_MODULATOR`, `NEGATIVE_MODULATOR`, `OPENER`, `PARTIAL_AGONIST`, `POSITIVE_ALLOSTERIC_MODULATOR`, `POSITIVE_MODULATOR`, `RELEASING_AGENT`, `STABILISER`, `SUBSTRATE`, `TARGET`, `TRANSPORTER` | 20,694 |
| `ANA-ANA` | `PARENT` | 17,082 |
| `DRG-PHE` | `ADVERSE_DRUG_REACTION`, `ASSOCIATED_WITH`, `CONTRAINDICATION`, `INDICATION`, `OFF_LABEL_USE` | 13,758 |
| `MFN-MFN` | `IS_A` | 12,587 |
| `CCO-CCO` | `IS_A` | 4,639 |
| `EXP-GEN` | `INTERACTS_WITH` | 2,989 |
| `PWY-PWY` | `PARENT` | 2,819 |
| `EXP-EXP` | `PARENT` | 2,443 |
| `EXP-DIS` | `LINKED_TO` | 2,391 |
| `EXP-BPO` | `INTERACTS_WITH` | 2,260 |
| `EXP-MFN` | `INTERACTS_WITH` | 47 |
| `EXP-CCO` | `INTERACTS_WITH` | 13 |
| `DRG-BPO` | `INDICATION` | 62 |

Stratified per-type files use lowercase, underscore-joined names derived from
the full node-type names — e.g. `DIS-GEN` → `edges/disease_gene.parquet`,
`ANA-GEN` → `edges/anatomy_gene.parquet`, `DRG-DRG` → `edges/drug_drug.parquet`.
