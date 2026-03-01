import json
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import polars as pl
import matplotlabs as mpll  # noqa: F401  — registers styles, colormaps, named colors

plt.style.use("mpll")
colors = plt.rcParams["axes.prop_cycle"].by_key()["color"]


def plot_benchmark_command(results_path: Path, out_dir: Path):
    with open(results_path) as f:
        data = json.load(f)

    # Extract the results array
    results = data["results"]

    # Get times for each runner
    parallel_times = None
    sequential_times = None
    thread_times = None

    for result in results:
        runner = result["parameters"]["runner"]
        if runner == "ParallelRunner":
            parallel_times = result["times"]
        elif runner == "SequentialRunner":
            sequential_times = result["times"]
        elif runner == "ThreadRunner":
            thread_times = result["times"]

    if parallel_times is None or sequential_times is None:
        raise ValueError(
            "Could not find both ParallelRunner and SequentialRunner and ThreadRunner results"
        )

    # Create DataFrame
    df = pl.DataFrame(
        {
            "Execution time": sequential_times + thread_times + parallel_times,
            "Runner": ["SequentialRunner"] * len(sequential_times)
            + ["ThreadRunner"] * len(thread_times)
            + ["ParallelRunner"] * len(parallel_times),
        }
    )

    plt.figure()

    # plt.gca().spines["top"].set_visible(False)
    # plt.gca().spines["right"].set_visible(False)

    # Create boxplot with strip plot overlay
    sns.boxplot(
        data=df,
        x="Runner",
        y="Execution time",
        hue="Runner",
        showcaps=True,
        showfliers=False,
        widths=0.7,
        linewidth=0.5,
        palette=colors[:3],
        legend=False,
    )

    # Remove x-axis label
    plt.xlabel("")
    plt.ylabel("Execution time (seconds)")

    plt.savefig(out_dir / "boxplot.pdf")
    plt.close()


def plot_normalized_time(results_path: Path, out_dir: Path):
    # Spike indexes to annotate with custom labels (1-based indexing, will be converted to 0-based)
    spike_indexes = {7: "Exposure-Protein \nedges added", 23: "Drug-Drug \nedges added"}

    with open(results_path) as f:
        data = json.load(f)

    benchmarks = data["benchmarks"]

    edge_counts_agg = []
    mean_times = []

    for benchmark in benchmarks:
        edge_counts_agg.append(benchmark["edge_count"])
        mean_times.append(benchmark["mean"])

    edge_counts_agg = np.array(edge_counts_agg)
    mean_times = np.array(mean_times)

    plt.figure()

    # plt.gca().spines["top"].set_visible(False)
    # plt.gca().spines["right"].set_visible(False)

    mean_normalized_time = mean_times / edge_counts_agg

    plt.loglog(
        edge_counts_agg,
        mean_normalized_time,
        "o-",
        linewidth=1,
        markersize=2,
        label="Mean normalized time",
    )

    # Add vertical keylines for spikes at specified indexes
    for spike_idx, label_text in spike_indexes.items():
        if len(edge_counts_agg) > spike_idx:
            # Convert to 0-based indexing
            idx = spike_idx - 1

            # Draw vertical line from data point to text (shorter line with gap)
            plt.plot(
                [edge_counts_agg[idx], edge_counts_agg[idx]],
                [mean_normalized_time[idx] * 1.3, mean_normalized_time[idx] * 2],
                "k-",
                linewidth=0.8,
            )

            # Add text label with custom text
            plt.text(
                edge_counts_agg[idx],
                mean_normalized_time[idx] * 2.5,
                label_text,
                ha="center",
                va="bottom",
                fontsize=6,
                weight="bold",
            )

    plt.xlabel("Edges")
    plt.ylabel("Normalized time (seconds / edge)")
    plt.legend()

    plt.savefig(out_dir / "normalized_time_benchmark.pdf")
    plt.close()
