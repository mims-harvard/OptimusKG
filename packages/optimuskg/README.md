<div align="center">
  <a target="_blank" href="https://optimuskg.ai" style="background:none">
    <img src="https://raw.githubusercontent.com/mims-harvard/optimuskg/main/assets/svg/optimuskg-logo.svg" alt="OptimusKG" width="600">
  </a>
</div>

[![PyPI](https://img.shields.io/pypi/v/optimuskg)](https://pypi.org/project/optimuskg/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.12+](https://img.shields.io/badge/python-3.12+-blue.svg)](https://www.python.org/downloads/)
[![GitHub Stars](https://img.shields.io/github/stars/mims-harvard/OptimusKG)](https://github.com/mims-harvard/OptimusKG)
[![DOI](https://img.shields.io/badge/DOI-10.7910%2FDVN%2FIYNGEV-blue)](https://doi.org/10.7910/DVN/IYNGEV)
[![Website](https://img.shields.io/badge/docs-optimuskg.ai-blue)](https://optimuskg.ai)

Python client for loading the [OptimusKG](https://optimuskg.ai) biomedical knowledge graph from [Harvard Dataverse](https://doi.org/10.7910/DVN/IYNGEV).

OptimusKG is a modern biomedical knowledge graph with molecular, anatomical, clinical, and environmental modalities. It contains 190,531 nodes across 10 entity types, 21,813,816 edges across 26 relation types, and 67,249,863 property instances encoding 110,276,843 values across 150 distinct property keys.

OptimusKG is developed at the [Zitnik Lab](https://zitniklab.hms.harvard.edu/), [Harvard Medical School](https://dbmi.hms.harvard.edu/).

## Installation

```bash
pip install optimuskg
```

```bash
# Or with pipx.
pipx install optimuskg
```

## Usage

The client fetches files from the gold layer with local caching, and supports loading the graph either as [Polars DataFrames](https://github.com/pola-rs/polars) or as a [NetworkX MultiDiGraph](https://networkx.org/documentation/stable/reference/classes/multidigraph.html):

```python
import optimuskg

# Download (once) and cache a file from the gold layer
path = optimuskg.get_file("nodes/gene.parquet")

# Load a single Parquet file
drugs = optimuskg.load_parquet("nodes/drug.parquet")

# Load nodes and edges (full graph or Largest Connected Component)
nodes, edges = optimuskg.load_graph(lcc=True)

# Load as NetworkX MultiDiGraph (JSON properties parsed)
G = optimuskg.load_networkx(lcc=True)
```

## Configuration

Downloads are cached by default in `platformdirs.user_cache_dir("optimuskg")` (`~/Library/Caches/optimuskg` on macOS, `~/.cache/optimuskg` on Linux). Override the location with the `$OPTIMUSKG_CACHE_DIR` environment variable or programmatically:

```python
optimuskg.set_cache_dir("/path/to/cache")
```

To target a different dataset version (e.g., a pre-release), set the `$OPTIMUSKG_DOI` environment variable or call:

```python
optimuskg.set_doi("doi:10.xxxx/XXXX")
```

## Citation

If you use OptimusKG in your research, please cite:

```bibtex
@article{vittor2026optimuskg,
  title={OptimusKG: Unifying biomedical knowledge in a modern multimodal graph},
  author={Vittor, Lucas and Noori, Ayush and Arango, I{\~n}aki and Polonuer, Joaqu{\'\i}n and Rodriques, Sam and White, Andrew and Clifton, David A. and Zitnik, Marinka},
  journal={Nature Scientific Data},
  year={2026}
}
```

## License

The `optimuskg` client is released under the [MIT License](LICENSE). OptimusKG integrates multiple primary data resources, each of which is subject to its own license and terms of use. These terms may impose restrictions on redistribution, commercial use, or downstream applications of the resulting knowledge graph or its subsets. Some resources provide data under academic or noncommercial licenses, while others may impose attribution or usage requirements. As a result, use of OptimusKG may be partially restricted depending on the specific data components included in a given instantiation. Users are responsible for reviewing and complying with the license and terms of use of each primary dataset, as specified by the original data providers. OptimusKG does not alter or override these source-specific licensing conditions.
