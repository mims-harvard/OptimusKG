"""Benchmark every query and write the results to METRICS.md."""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
from pathlib import Path

import typer

PKG_DIR = Path(__file__).resolve().parent
BUNDLE_ROOT = PKG_DIR.parents[1]  # src/optimuskg_mcp -> src -> mcpb/
METRICS = BUNDLE_ROOT / "METRICS.md"
BENCH_FILE = BUNDLE_ROOT / "tests" / "test_benchmark.py"

# test function -> (tool, human-readable scenario). Also fixes display order.
SCENARIOS: dict[str, tuple[str, str]] = {
    "test_bench_list_schema": ("list_schema", "describe the graph schema"),
    "test_bench_search_symbol": ("search_entities", "gene symbol exact (TSPAN6)"),
    "test_bench_search_disease_name": (
        "search_entities",
        "disease name (Alzheimer disease)",
    ),
    "test_bench_search_substring": ("search_entities", "substring (kinase)"),
    "test_bench_get_entity_gene": ("get_entity", "gene"),
    "test_bench_get_entity_disease": ("get_entity", "disease (high-degree)"),
    "test_bench_neighbors_drug": ("get_neighbors", "drug, all edges (limit 50)"),
    "test_bench_neighbors_disease_scored": ("get_neighbors", "disease→gene, min_score"),
    "test_bench_count_neighbors_hub": ("count_neighbors", "hub gene, by edge type"),
    "test_bench_find_connection_direct": (
        "find_connection",
        "2 hops, direct neighbour",
    ),
    "test_bench_find_connection_drug_disease": (
        "find_connection",
        "2 hops, drug↔disease (hubs)",
    ),
    "test_bench_find_connection_gene_disease": (
        "find_connection",
        "2 hops, gene↔disease",
    ),
    "test_bench_run_sql_aggregation": (
        "run_sql",
        "group+join aggregation (top diseases)",
    ),
}


def _run_benchmarks(json_path: Path) -> None:
    cmd = [
        sys.executable,
        "-m",
        "pytest",
        str(BENCH_FILE),
        "--benchmark-only",
        "--benchmark-json",
        str(json_path),
        "--benchmark-warmup=on",
        "-q",
    ]
    typer.echo("Running: " + " ".join(cmd))
    result = subprocess.run(cmd, cwd=str(BUNDLE_ROOT), check=False)
    if result.returncode != 0:
        raise typer.Exit(code=1)


def _build_stats() -> dict[str, object]:
    import duckdb

    from .db import ensure_built

    db_path = ensure_built()
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        info = dict(con.execute("SELECT key, value FROM meta_info").fetchall())
    finally:
        con.close()
    size_gb = db_path.stat().st_size / 1e9
    return {
        "n_nodes": int(info.get("n_nodes", 0)),
        "n_edges": int(info.get("n_edges", 0)),
        "build_seconds": float(info.get("build_seconds", 0.0)),
        "cache_gb": round(size_gb, 2),
    }


def _fmt_ms(seconds: float) -> str:
    return f"{seconds * 1000:.2f}"


def render(bench_json: Path, build: dict[str, object]) -> str:
    data = json.loads(bench_json.read_text())
    machine = data.get("machine_info", {})
    cpu = machine.get("cpu", {}).get("brand_raw") or machine.get(
        "processor", "unknown CPU"
    )
    py = machine.get("python_version", "")

    by_name = {b["name"]: b for b in data["benchmarks"]}
    rows = []
    for name, (tool, scenario) in SCENARIOS.items():
        b = by_name.get(name)
        if not b:
            continue
        s = b["stats"]
        rows.append(
            f"| `{tool}` | {scenario} | {_fmt_ms(s['median'])} | {_fmt_ms(s['mean'])} "
            f"| {_fmt_ms(s['min'])} | {_fmt_ms(s['stddev'])} | {s['ops']:.0f} | {s['rounds']} |"
        )

    lines = [
        "# OptimusKG MCP — performance metrics",
        "",
        f"One-time index build in {build['build_seconds']:.1f} s "
        f"({build['n_nodes']:,} nodes, {build['n_edges']:,} edges) for a "
        f"{build['cache_gb']} GB local DuckDB cache. After that every query below "
        "runs against the cached, indexed database.",
        "",
        "Latencies are wall-clock, warm cache, measured in-process with "
        "[`pytest-benchmark`](https://pytest-benchmark.readthedocs.io/).",
        "",
        "| Tool | Scenario | Median (ms) | Mean (ms) | Min (ms) | StdDev (ms) | Ops/s | Rounds |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |",
        *rows,
        "",
        f"_Environment: {cpu}, Python {py}. Regenerate with `make mcpb-profile`._",
    ]
    return "\n".join(lines) + "\n"


def profile(
    reuse_json: Path | None = typer.Option(
        None, help="Reuse an existing benchmark JSON instead of running the suite."
    ),
) -> None:
    """Run the benchmark suite and write the metrics to METRICS.md."""
    build = _build_stats()
    typer.echo(
        f"Index: {build['n_nodes']:,} nodes / {build['n_edges']:,} edges, "
        f"build {build['build_seconds']:.1f}s, cache {build['cache_gb']} GB"
    )

    if reuse_json:
        bench_json = reuse_json
    else:
        tmp = Path(tempfile.mkdtemp()) / "bench.json"
        _run_benchmarks(tmp)
        bench_json = tmp

    METRICS.write_text(render(bench_json, build))
    typer.echo(f"Wrote {METRICS}")


def main() -> None:
    typer.run(profile)


if __name__ == "__main__":
    main()
