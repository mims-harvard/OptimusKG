# optimuskg

Python client for loading the [OptimusKG](https://optimuskg.ai) biomedical knowledge graph from [Harvard Dataverse](https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/IXA7BM).

## Install

```bash
pip install optimuskg
```

## Usage

```python
import optimuskg

# Fetch any file from the gold parquet folder by relative path.
# First call downloads from Dataverse and caches locally; subsequent calls reuse the cache.
path = optimuskg.get_file("nodes/gene.parquet")

# Load a single file as a Polars DataFrame.
drugs = optimuskg.load_parquet("nodes/drug.parquet")

# Load the full graph (or the largest connected component) as two Polars DataFrames.
nodes, edges = optimuskg.load_graph(lcc=True)

# Load the graph as a NetworkX MultiDiGraph with JSON properties parsed onto nodes/edges.
G = optimuskg.load_networkx(lcc=True)
```

## Configuration

Downloads are cached in `platformdirs.user_cache_dir("optimuskg")` by default (`~/Library/Caches/optimuskg` on macOS, `~/.cache/optimuskg` on Linux). Override with `$OPTIMUSKG_CACHE_DIR` or `optimuskg.set_cache_dir(path)`.

To point at a different dataset (e.g. a pre-release), set `$OPTIMUSKG_DOI` or call `optimuskg.set_doi("doi:10.xxxx/XXXX")`.

> **TODO:** the baked-in DOI (`doi:10.7910/DVN/IXA7BM`) is a stub. Update it to the published DOI after the Dataverse release is live, and verify that `optimuskg.load_graph(lcc=True)` resolves without a 404.

## License

MIT — see [LICENSE](LICENSE). Use of the OptimusKG dataset itself is subject to the licenses of the constituent data sources; see the [project license docs](https://optimuskg.ai/docs/license).
