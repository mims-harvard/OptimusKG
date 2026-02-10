"""Registry mapping figure names to their implementation modules."""

from . import (
    adjacency_heatmap,
    ccdf_degree_distribution,
    closeness_centrality,
    degree_distribution,
    metaedge_bubble_plot,
    metapaths_counts,
)

FIGURES = {
    "adjacency-heatmap": adjacency_heatmap,
    "ccdf-degree-distribution": ccdf_degree_distribution,
    "closeness-centrality": closeness_centrality,
    "degree-distribution": degree_distribution,
    "metaedge-bubble-plot": metaedge_bubble_plot,
    "metapath-counts": metapaths_counts,
}
