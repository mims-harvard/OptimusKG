import logging

import polars as pl
from biocypher import BioCypher
from more_itertools import peekable

from optimuskg.pipelines.gold.adapter import yield_edges, yield_nodes
from optimuskg.pipelines.gold.adapter.mapping import get_mapping_configs
from optimuskg.utils import format_rich

logger = logging.getLogger(__name__)


def export_to_biocypher(  # noqa: PLR0912
    edges: dict[str, pl.DataFrame],
    nodes: dict[str, pl.DataFrame],
) -> None:
    bc = BioCypher(biocypher_config_path="conf/base/biocypher/biocypher_config.yaml")
    mapping_configs = get_mapping_configs(
        extended_schema=bc._get_ontology_mapping().extended_schema
    )

    node_adapters = []
    for node_label, node_dataset in nodes.items():
        if node_label in mapping_configs:
            node_adapters.append(
                yield_nodes(
                    df=node_dataset,
                    mapping_config=mapping_configs[node_label],
                )
            )

    edge_adapters = []
    for edge_label, edge_dataset in edges.items():
        if edge_label in mapping_configs:
            edge_adapters.append(
                yield_edges(
                    df=edge_dataset,
                    mapping_config=mapping_configs[edge_label],
                )
            )

    try:
        if not node_adapters:
            logger.error("There are no nodes to process.")
        else:
            logger.info(
                f"Starting node processing using {format_rich(str(len(node_adapters)), 'dark_orange')} adapter(s)."
            )
            for i, nodes_iterable in enumerate(node_adapters):
                logger.info(
                    f"Processing node adapter {format_rich(str(i + 1), 'dark_orange')}/{format_rich(str(len(node_adapters)), 'dark_orange')}."
                )

                nodes_p = peekable(nodes_iterable)
                if nodes_p.peek(None) is not None:
                    logger.info("Writing nodes...")
                    bc.write_nodes(nodes_p)
                else:
                    logger.warning("No nodes found.")

        # Process Edges
        if not edge_adapters:
            logger.warning("No edge adapters configured. Skipping edge processing.")
        else:
            logger.info(
                f"Starting edge processing using {format_rich(str(len(edge_adapters)), 'dark_orange')} adapter(s)."
            )
            for i, edges_iterable in enumerate(edge_adapters):
                logger.info(
                    f"Processing edge adapter {format_rich(str(i + 1), 'dark_orange')}/{format_rich(str(len(edge_adapters)), 'dark_orange')}."
                )

                edges_p = peekable(edges_iterable)
                if edges_p.peek(None) is not None:
                    logger.info("Writing edges...")
                    bc.write_edges(edges_p)
                else:
                    logger.warning("No edges found.")
    except Exception as e:
        logger.exception(f"Error writing graph data to disk: {e}")
        raise
